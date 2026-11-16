//! 一次完整扫描：并发取数 → 分簇 → 筛查 → 排名。
//!
//! 失败与排除**都要如实上报**。一次扫描会并发打十几家 API，任何一家都可能 429、
//! 超时、或返回结构变了；如果这些情况只是让那家从榜单里消失，用户看到的就是一份
//! 残缺的排名，而界面上没有任何迹象。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{Decimal, MarketSnapshot, Settings, Symbol, Venue};
use arb_venues::VenueApi;
use chrono::{DateTime, Utc};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::identity::{self, Cluster, Clustering};
use crate::rank::{self, Opportunity, RankConfig};
use crate::sanity;

/// 单家场所本次取数的结果。
#[derive(Debug, Clone, serde::Serialize)]
pub struct VenueReport {
    pub venue: Venue,
    pub ok: bool,
    /// 取到的读数条数（`ok = false` 时为 0）。
    pub rates: usize,
    pub error: Option<String>,
    pub elapsed_ms: u64,
    /// 这一轮没有现拉、沿用了 [`RatesCache`] 里多久以前的读数（毫秒）；现拉的为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reused_age_ms: Option<u64>,
}

/// 各场所最近一次**成功**取到的原始读数（[`scan_into`] / [`scan_reusing`] 写入）。
///
/// 给只需要现拉一两家的扫描用：实盘下单前的现扫只现拉两腿场所，其余场所的读数只用来分簇
/// （同一资产，参考价相差 5% 以内）和可信度筛查（资金费日化离群 8σ 且 ≥ 0.5%），沿用看板一分钟内
/// 那轮的就够了 —— 而现拉九家要等最慢的那家（2026-10-25 东京：Bitget 150~200ms、MEXC 100~160ms，
/// 两腿常用的几家 10~80ms）。
#[derive(Default)]
pub struct RatesCache {
    inner: std::sync::Mutex<HashMap<Venue, (Instant, Vec<MarketSnapshot>)>>,
}

impl RatesCache {
    fn put(&self, venue: Venue, rates: &[MarketSnapshot]) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(venue, (Instant::now(), rates.to_vec()));
    }

    /// `venue` 不超过 `max_age` 的读数，和它有多旧。
    fn get(&self, venue: Venue, max_age: Duration) -> Option<(Duration, Vec<MarketSnapshot>)> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&venue)
            .map(|(at, rates)| (at.elapsed(), rates))
            .filter(|(age, _)| *age <= max_age)
            .map(|(age, rates)| (age, rates.clone()))
    }
}

/// 读数从哪来：全部现拉（[`scan`]）、全部现拉并记进缓存（[`scan_into`]）、或者只现拉 `fresh` 这几家、
/// 其余沿用缓存里不超过 `max_age` 的（[`scan_reusing`]，缓存里没有或太旧的照样现拉）。
#[derive(Clone, Copy)]
enum Source<'a> {
    Fresh,
    Record(&'a RatesCache),
    Reuse {
        cache: &'a RatesCache,
        fresh: &'a [Venue],
        max_age: Duration,
    },
}

/// 被排除在配对之外的读数。
///
/// 两种原因：读数与同币种其它场所严重不一致，或缺少参考价、身份无法核实。
/// 两种都必须展示 —— 静默丢掉比标出来更危险。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExcludedReading {
    pub venue: Venue,
    pub symbol: Symbol,
    pub reason: String,
}

/// 通过身份判定、但被**入场门槛**挡下的配对。
///
/// 与 [`ExcludedReading`] 分开：那个说的是「这条读数不可信」，这个说的是
/// 「读数没问题，但按当前门槛不该进场」。混在一起会让人以为数据坏了。
#[derive(Debug, Clone, serde::Serialize)]
pub struct RejectedPair {
    pub symbol: Symbol,
    pub long: Venue,
    pub short: Venue,
    pub reason: String,
}

/// 一个合约的全部读数与它的两条机会榜。
///
/// 两条榜分开：资金费视角按费差排、价差视角按基差排，**入选条件也不同**
/// （前者要费差为正，后者要基差为正）。混在一条列表里会让「榜首」的含义取决于
/// 调用方拿它做什么。
#[derive(Debug, Clone, serde::Serialize)]
pub struct SymbolView {
    pub symbol: Symbol,
    /// 该合约的**全部**读数，含被排除的（排除原因在报告的 `excluded` 里）。
    pub rates: Vec<MarketSnapshot>,
    /// 资金费套利视角（费差为正），按摊费后年化倒序。
    pub funding: Vec<Opportunity>,
    /// 跨所价差套利视角（可成交价差为正），按一次性净价差倒序。
    pub spread: Vec<Opportunity>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Totals {
    pub venues_ok: usize,
    pub venues_failed: usize,
    pub rates: usize,
    pub symbols: usize,
    pub profitable_pairs: usize,
    pub spread_pairs: usize,
    pub excluded_rates: usize,
    pub gated_pairs: usize,
}

/// 一次扫描的完整结果。这是 API 与 CLI 的唯一输出契约。
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanReport {
    pub generated_at: DateTime<Utc>,
    pub fee_per_side: Decimal,
    pub amortize_days: Decimal,
    /// 价差套利的计划持有天数。
    pub spread_hold_days: Decimal,
    /// 不利入场基差的上限（%）。`None` = 门槛关闭。
    pub max_entry_basis_pct: Option<Decimal>,
    pub min_venues: usize,
    pub venues: Vec<VenueReport>,
    pub symbols: Vec<SymbolView>,
    /// 身份无法核实、因此不参与配对的读数。
    pub unverified: Vec<ExcludedReading>,
    /// 读数与同币种其它场所严重不一致、因此不参与配对的读数。
    pub suspicious: Vec<ExcludedReading>,
    /// 被入场门槛挡下的配对（身份与读数都没问题）。
    pub gated: Vec<RejectedPair>,
    pub totals: Totals,
}

impl ScanReport {
    /// 这一轮排名用的口径（费率回落值、摊销天数、价差持有期、入场门槛）。
    /// 事后对某一对腿重新排名（比如补上现拉的盘口）必须用同一份口径，否则两处数字对不上。
    pub fn rank_config(&self) -> rank::RankConfig {
        rank::RankConfig {
            fee_per_side: self.fee_per_side,
            amortize_days: self.amortize_days,
            spread_hold_days: self.spread_hold_days,
            measured_hold: std::collections::HashMap::new(),
            max_entry_basis_pct: self.max_entry_basis_pct,
        }
    }
}

/// 记一家场所这一轮取数的结果：**状态变化**才出声。持续失败的场所（被限频、维护）每轮都
/// 报一次 WARN 会把日志灌满；只在第一次失败、错误原因变了、恢复时各记一条。
fn note_venue_result(venue: Venue, error: Option<String>) {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<HashMap<Venue, String>>> = OnceLock::new();
    let mut last = LAST
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match error {
        None => {
            if last.remove(&venue).is_some() {
                tracing::info!(%venue, "场所取数已恢复");
            }
        }
        Some(error) => {
            if last.insert(venue, error.clone()).as_ref() == Some(&error) {
                tracing::debug!(%venue, %error, "场所取数仍然失败");
            } else {
                warn!(%venue, %error, "场所取数失败");
            }
        }
    }
}

/// 并发扫描全部场所并排名。
///
/// 单家场所失败不影响其它家：套利要的是「所有能配对的场所」，一家挂了不该让
/// 整轮扫描失败，但必须在 [`VenueReport`] 里留痕。
pub async fn scan(apis: &[Arc<dyn VenueApi>], settings: &Settings) -> ScanReport {
    scan_from(apis, settings, Source::Fresh).await
}

/// 同 [`scan`]，并把每家成功取到的读数记进 `cache`（看板的后台全量扫描用）。
pub async fn scan_into(
    apis: &[Arc<dyn VenueApi>],
    settings: &Settings,
    cache: &RatesCache,
) -> ScanReport {
    scan_from(apis, settings, Source::Record(cache)).await
}

/// 同 [`scan`]，但只现拉 `fresh` 这几家（下单的两条腿）；其余场所沿用 `cache` 里不超过 `max_age` 的读数，
/// 缓存里没有或太旧的照样现拉。现拉到的也记进缓存。分簇、筛查、排名与 [`scan`] 完全相同。
pub async fn scan_reusing(
    apis: &[Arc<dyn VenueApi>],
    settings: &Settings,
    cache: &RatesCache,
    fresh: &[Venue],
    max_age: Duration,
) -> ScanReport {
    scan_reusing_pinned(apis, settings, cache, fresh, max_age, None).await
}

/// 同 [`scan_reusing`]，给已在台账里配成一对的两条腿补仓用：分簇把这两腿分进了不同的簇
/// （别家的报价把簇均值拉偏了）、而两腿彼此的参考价仍在容差以内时，另加一个只含这两腿的簇，
/// 见 [`identity::pin_pair`]。其余分簇、筛查、排名与 [`scan`] 完全相同。
pub async fn scan_reusing_pinned(
    apis: &[Arc<dyn VenueApi>],
    settings: &Settings,
    cache: &RatesCache,
    fresh: &[Venue],
    max_age: Duration,
    pin: Option<&identity::PinnedPair>,
) -> ScanReport {
    scan_from_pinned(
        apis,
        settings,
        Source::Reuse {
            cache,
            fresh,
            max_age,
        },
        pin,
    )
    .await
}

async fn scan_from(
    apis: &[Arc<dyn VenueApi>],
    settings: &Settings,
    source: Source<'_>,
) -> ScanReport {
    scan_from_pinned(apis, settings, source, None).await
}

async fn scan_from_pinned(
    apis: &[Arc<dyn VenueApi>],
    settings: &Settings,
    source: Source<'_>,
    pin: Option<&identity::PinnedPair>,
) -> ScanReport {
    // 单家场所一轮取数的总时限：连接器内部串行发好几个请求（OKX 六个），每个都可能用满
    // HTTP 超时，一家卡住不能把整轮拖成各请求超时之和。
    let deadline = Duration::from_secs((settings.http_timeout_sec * 2).max(10));
    let (rates, venues) = fetch_all(apis, deadline, source).await;

    let pinned_raw: Vec<MarketSnapshot> = pin
        .map(|pin| {
            rates
                .iter()
                .filter(|rate| rate.symbol.base.eq_ignore_ascii_case(&pin.base))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let mut clustering = identity::cluster(rates, settings.min_venues);
    // 没成对的原因记成这条腿「身份无法核实」：补仓的报错据此说清楚两腿差了多少。
    let mut pin_refused: Option<ExcludedReading> = None;
    if let Some(pin) = pin {
        match identity::pin_pair(&mut clustering, &pinned_raw, pin) {
            identity::Pinned::AlreadyTogether => {}
            identity::Pinned::Added => info!(
                base = %pin.base,
                long = %pin.long,
                short = %pin.short,
                "两腿被分簇拆开，但彼此相差在容差以内：按台账里的这一对单独成簇（只用于补仓）"
            ),
            identity::Pinned::Refused(reason) => {
                // 记在被拆出去的那条腿上（不在任何同名簇里的那条）；都在就记空腿。
                let in_cluster = |venue: Venue| {
                    clustering.clusters.iter().any(|cluster| {
                        cluster.symbol.base.eq_ignore_ascii_case(&pin.base)
                            && cluster.rates.iter().any(|rate| rate.venue == venue)
                    })
                };
                let venue = if in_cluster(pin.short) && !in_cluster(pin.long) {
                    pin.long
                } else {
                    pin.short
                };
                let symbol = pinned_raw
                    .iter()
                    .find(|rate| rate.venue == venue)
                    .map(|rate| rate.symbol.clone())
                    .unwrap_or_else(|| Symbol::perp(&pin.base, "USDT"));
                warn!(base = %pin.base, long = %pin.long, short = %pin.short, %reason, "补仓两腿没能按台账成对");
                pin_refused = Some(ExcludedReading {
                    venue,
                    symbol,
                    reason: format!("补仓两腿没能成对：{reason}"),
                });
            }
        }
    }

    let mut unverified: Vec<ExcludedReading> = clustering
        .unverified
        .iter()
        .map(|row| ExcludedReading {
            venue: row.rate.venue,
            symbol: row.rate.symbol.clone(),
            reason: row.reason.to_string(),
        })
        .collect();
    unverified.extend(pin_refused);
    sort_excluded(&mut unverified);

    let config = rank_config(settings);
    let (symbols, suspicious, gated) = rank_clusters(&clustering, &config);

    let mut report = ScanReport {
        generated_at: Utc::now(),
        fee_per_side: settings.fee_per_side,
        amortize_days: settings.amortize_days,
        spread_hold_days: settings.spread_hold_days,
        max_entry_basis_pct: settings.max_entry_basis_pct,
        min_venues: settings.min_venues,
        venues,
        symbols,
        unverified,
        suspicious,
        gated,
        totals: Totals::default(),
    };
    recount(&mut report);

    info!(
        venues_ok = report.totals.venues_ok,
        venues_failed = report.totals.venues_failed,
        rates = report.totals.rates,
        symbols = report.totals.symbols,
        profitable = report.totals.profitable_pairs,
        "扫描完成"
    );
    report
}

/// 从运行配置取出排名参数。
pub fn rank_config(settings: &Settings) -> RankConfig {
    RankConfig {
        fee_per_side: settings.fee_per_side,
        amortize_days: settings.amortize_days,
        spread_hold_days: settings.spread_hold_days,
        measured_hold: HashMap::new(),
        max_entry_basis_pct: settings.max_entry_basis_pct,
    }
}

/// 用新的摊费口径重算排名，**不重新取数**。
///
/// 面板上的「持有天数」必须走这条路：如果让前端自己重算一遍净年化，就等于在
/// 两处各写一份排名逻辑，迟早漂开（旧项目里前端硬编码了一张费率表，四家与实测
/// 不符，而扫描器早就带回了真值）。
pub fn rerank(report: &mut ScanReport, config: &RankConfig) {
    let clustering = Clustering {
        clusters: report
            .symbols
            .iter()
            .map(|view| Cluster {
                symbol: view.symbol.clone(),
                rates: view.rates.clone(),
            })
            .collect(),
        unverified: Vec::new(),
    };
    let (symbols, suspicious, gated) = rank_clusters(&clustering, config);
    report.symbols = symbols;
    report.suspicious = suspicious;
    report.gated = gated;
    report.fee_per_side = config.fee_per_side;
    report.amortize_days = config.amortize_days;
    report.spread_hold_days = config.spread_hold_days;
    report.max_entry_basis_pct = config.max_entry_basis_pct;
    recount(report);
}

/// 对每个簇做可信度筛查、排名，再按入场门槛分流。
fn rank_clusters(
    clustering: &Clustering,
    config: &RankConfig,
) -> (Vec<SymbolView>, Vec<ExcludedReading>, Vec<RejectedPair>) {
    let mut symbols = Vec::with_capacity(clustering.clusters.len());
    let mut suspicious = Vec::new();
    let mut gated = Vec::new();

    for cluster in &clustering.clusters {
        let flagged = sanity::flag_outliers(&cluster.rates);
        for rate in &cluster.rates {
            if let Some(reason) = flagged.get(&(rate.venue, rate.symbol.to_string())) {
                suspicious.push(ExcludedReading {
                    venue: rate.venue,
                    symbol: rate.symbol.clone(),
                    reason: reason.to_string(),
                });
            }
        }
        let (trusted, _) = sanity::split_trustworthy(&cluster.rates, &flagged);

        let mut funding = Vec::new();
        let mut spread = Vec::new();
        for opportunity in rank::rank(&trusted, config) {
            // 价差视角只收**现在能成交**的正价差。标记价更高但买一卖一已经倒挂的，
            // 开仓当时就是亏的，不能因为标记价差为正就上榜。
            if opportunity
                .executable_basis_pct
                .is_some_and(|basis| basis > Decimal::ZERO)
            {
                spread.push(opportunity.clone());
            }
            // 资金费视角只收费差为正的方向；基差是那边的风险，由门槛挡。
            if !opportunity.funding_profitable() && opportunity.daily_spread <= Decimal::ZERO {
                continue;
            }
            match rank::basis_gate(&opportunity, config.max_entry_basis_pct) {
                Ok(()) => funding.push(opportunity),
                Err(reason) => gated.push(RejectedPair {
                    symbol: cluster.symbol.clone(),
                    long: opportunity.long,
                    short: opportunity.short,
                    reason,
                }),
            }
        }

        // 价差视角按基差收益排序；决胜键保证两次扫描之间顺序稳定。
        spread.sort_by(|a, b| {
            b.spread_net
                .cmp(&a.spread_net)
                .then_with(|| a.long.cmp(&b.long))
                .then_with(|| a.short.cmp(&b.short))
        });

        symbols.push(SymbolView {
            symbol: cluster.symbol.clone(),
            rates: cluster.rates.clone(),
            funding,
            spread,
        });
    }

    // 榜单按「该合约最好的资金费机会」倒序；决胜键保证两次扫描之间顺序稳定。
    symbols.sort_by(|a, b| {
        let a_best = a.funding.first().map(|o| o.funding_apr);
        let b_best = b.funding.first().map(|o| o.funding_apr);
        b_best
            .cmp(&a_best)
            .then_with(|| a.symbol.base.cmp(&b.symbol.base))
            .then_with(|| a.symbol.quote.cmp(&b.symbol.quote))
    });
    sort_excluded(&mut suspicious);
    gated.sort_by(|a, b| {
        a.symbol
            .to_string()
            .cmp(&b.symbol.to_string())
            .then_with(|| a.long.cmp(&b.long))
            .then_with(|| a.short.cmp(&b.short))
    });
    (symbols, suspicious, gated)
}

fn sort_excluded(rows: &mut [ExcludedReading]) {
    rows.sort_by(|a, b| {
        a.symbol
            .to_string()
            .cmp(&b.symbol.to_string())
            .then_with(|| a.venue.cmp(&b.venue))
    });
}

/// 重算汇总。任何改动 `symbols` 的路径都必须调它，否则面板上的总数会和表对不上。
fn recount(report: &mut ScanReport) {
    report.totals = Totals {
        venues_ok: report.venues.iter().filter(|v| v.ok).count(),
        venues_failed: report.venues.iter().filter(|v| !v.ok).count(),
        rates: report.venues.iter().map(|v| v.rates).sum(),
        symbols: report.symbols.len(),
        profitable_pairs: report
            .symbols
            .iter()
            .map(|s| s.funding.iter().filter(|o| o.funding_profitable()).count())
            .sum(),
        spread_pairs: report
            .symbols
            .iter()
            .map(|s| s.spread.iter().filter(|o| o.spread_profitable()).count())
            .sum(),
        excluded_rates: report.unverified.len() + report.suspicious.len(),
        gated_pairs: report.gated.len(),
    };
}

/// 并发取数。返回 (全部读数, 每家的成败)。
///
/// 限频中的场所本轮不请求（见 [`arb_venues::http::cooling`]）；整轮取数超过 `deadline` 的场所
/// 记为失败；错误像限频的（包括 HTTP 200 里带业务码的）让这家场所冷却一阵。
async fn fetch_all(
    apis: &[Arc<dyn VenueApi>],
    deadline: Duration,
    source: Source<'_>,
) -> (Vec<MarketSnapshot>, Vec<VenueReport>) {
    use arb_core::ArbError;
    use arb_venues::http::{cool_down, cooling, default_cooldown, looks_rate_limited};

    let mut rates = Vec::new();
    let mut venues = Vec::new();
    let mut set: JoinSet<(Venue, Result<Vec<MarketSnapshot>, ArbError>, u64)> = JoinSet::new();
    for api in apis {
        if let Source::Reuse {
            cache,
            fresh,
            max_age,
        } = source
            && !fresh.contains(&api.venue())
            && let Some((age, cached)) = cache.get(api.venue(), max_age)
        {
            venues.push(VenueReport {
                venue: api.venue(),
                ok: true,
                rates: cached.len(),
                error: None,
                elapsed_ms: 0,
                reused_age_ms: Some(age.as_millis() as u64),
            });
            rates.extend(cached);
            continue;
        }
        let api = Arc::clone(api);
        set.spawn(async move {
            let started = Instant::now();
            let venue = api.venue();
            let result = if let Some(left) = cooling(venue) {
                Err(ArbError::venue(
                    venue.as_str(),
                    format!("限频冷却中，还剩 {} 秒，本轮不请求", left.as_secs().max(1)),
                ))
            } else {
                match tokio::time::timeout(deadline, api.fetch_all()).await {
                    Ok(result) => result,
                    Err(_) => Err(ArbError::venue(
                        venue.as_str(),
                        format!("取数超过 {} 秒，本轮放弃", deadline.as_secs()),
                    )),
                }
            };
            if let Err(error) = &result
                && looks_rate_limited(&error.to_string())
            {
                cool_down(venue, default_cooldown(venue));
            }
            let elapsed_ms = started.elapsed().as_millis() as u64;
            (venue, result, elapsed_ms)
        });
    }

    let record = match source {
        Source::Fresh => None,
        Source::Record(cache) | Source::Reuse { cache, .. } => Some(cache),
    };
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((venue, Ok(fetched), elapsed_ms)) => {
                note_venue_result(venue, None);
                if let Some(cache) = record {
                    cache.put(venue, &fetched);
                }
                venues.push(VenueReport {
                    venue,
                    ok: true,
                    rates: fetched.len(),
                    error: None,
                    elapsed_ms,
                    reused_age_ms: None,
                });
                rates.extend(fetched);
            }
            Ok((venue, Err(error), elapsed_ms)) => {
                note_venue_result(venue, Some(error.to_string()));
                venues.push(VenueReport {
                    venue,
                    ok: false,
                    rates: 0,
                    error: Some(error.to_string()),
                    elapsed_ms,
                    reused_age_ms: None,
                });
            }
            // 连接器内部 panic：不能让它带走整轮扫描，但也必须留痕。
            Err(join_error) => warn!(%join_error, "场所任务异常退出"),
        }
    }

    venues.sort_by_key(|report| report.venue);
    (rates, venues)
}

/// 按 base 过滤榜单。空列表 = 全部。
pub fn filter_by_base(report: &mut ScanReport, bases: &[String]) {
    if bases.is_empty() {
        return;
    }
    let wanted: Vec<String> = bases
        .iter()
        .map(|base| base.trim().to_ascii_uppercase())
        .filter(|base| !base.is_empty())
        .collect();
    report.symbols.retain(|view| {
        wanted
            .iter()
            .any(|base| view.symbol.base.eq_ignore_ascii_case(base))
    });
    for rows in [&mut report.unverified, &mut report.suspicious] {
        rows.retain(|row| {
            wanted
                .iter()
                .any(|base| row.symbol.base.eq_ignore_ascii_case(base))
        });
    }
    report.gated.retain(|row| {
        wanted
            .iter()
            .any(|base| row.symbol.base.eq_ignore_ascii_case(base))
    });
    recount(report);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arb_core::ArbResult;
    use rust_decimal_macros::dec;

    /// 每次取数都数一下、报一个可以分簇的读数（标记价 = `price`）。
    struct Counting {
        venue: Venue,
        price: std::sync::Mutex<Decimal>,
        calls: AtomicUsize,
    }

    impl Counting {
        fn new(venue: Venue, price: Decimal) -> Arc<Self> {
            Arc::new(Self {
                venue,
                price: std::sync::Mutex::new(price),
                calls: AtomicUsize::new(0),
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl VenueApi for Counting {
        fn venue(&self) -> Venue {
            self.venue
        }
        async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let price = *self.price.lock().unwrap();
            Ok(vec![MarketSnapshot {
                venue: self.venue,
                symbol: Symbol::perp("NVDA", "USDT"),
                period_rate: dec!(0.0001),
                interval_h: 8,
                interval_assumed: false,
                next_funding_at: Utc::now(),
                next_funding_estimated: false,
                taker_fee: None,
                mark_price: Some(price),
                index_price: Some(price),
                best_bid: Some(price),
                best_ask: Some(price),
                bid_size_usdt: None,
                ask_size_usdt: None,
                open_interest_usdt: None,
                quote_volume_24h: None,
                max_leverage: None,
                maintenance_margin: None,
                oi_capped: false,
            }])
        }
    }

    fn mark(report: &ScanReport, venue: Venue) -> Option<Decimal> {
        report
            .symbols
            .iter()
            .flat_map(|view| view.rates.iter())
            .find(|rate| rate.venue == venue)
            .and_then(|rate| rate.mark_price)
    }

    #[tokio::test]
    async fn reusing_scan_fetches_only_the_legs_and_takes_the_rest_from_the_cache() {
        let settings = Settings::from_env().unwrap();
        let (a, b, c) = (
            Counting::new(Venue::Lighter, dec!(100)),
            Counting::new(Venue::HyperliquidXyz, dec!(100.1)),
            Counting::new(Venue::Bitget, dec!(100.2)),
        );
        let apis: Vec<Arc<dyn VenueApi>> = vec![a.clone(), b.clone(), c.clone()];
        let cache = RatesCache::default();

        // 缓存是空的：三家都现拉，并记进缓存。
        let first = scan_reusing(
            &apis,
            &settings,
            &cache,
            &[Venue::Lighter],
            Duration::from_secs(90),
        )
        .await;
        assert_eq!((a.calls(), b.calls(), c.calls()), (1, 1, 1));
        assert_eq!(first.totals.venues_ok, 3);

        // 之后：只现拉两条腿，第三家沿用缓存（它的新价看不到，两腿的新价看得到）。
        *a.price.lock().unwrap() = dec!(101);
        *b.price.lock().unwrap() = dec!(101.1);
        *c.price.lock().unwrap() = dec!(101.2);
        let legs = [Venue::Lighter, Venue::HyperliquidXyz];
        let report = scan_reusing(&apis, &settings, &cache, &legs, Duration::from_secs(90)).await;
        assert_eq!((a.calls(), b.calls(), c.calls()), (2, 2, 1));
        assert_eq!(mark(&report, Venue::Lighter), Some(dec!(101)));
        assert_eq!(mark(&report, Venue::HyperliquidXyz), Some(dec!(101.1)));
        assert_eq!(
            mark(&report, Venue::Bitget),
            Some(dec!(100.2)),
            "旁证沿用缓存"
        );
        let reused = report
            .venues
            .iter()
            .find(|venue| venue.venue == Venue::Bitget)
            .unwrap();
        assert!(reused.ok && reused.reused_age_ms.is_some());
        assert!(
            report
                .venues
                .iter()
                .filter(|venue| legs.contains(&venue.venue))
                .all(|venue| venue.reused_age_ms.is_none())
        );
        assert_eq!(report.totals.venues_ok, 3);

        // 缓存太旧：照样现拉。
        let stale = scan_reusing(&apis, &settings, &cache, &legs, Duration::ZERO).await;
        assert_eq!(c.calls(), 2);
        assert_eq!(mark(&stale, Venue::Bitget), Some(dec!(101.2)));
    }

    #[tokio::test]
    async fn plain_scan_never_reads_the_cache_and_scan_into_fills_it() {
        let settings = Settings::from_env().unwrap();
        let (a, c) = (
            Counting::new(Venue::Lighter, dec!(100)),
            Counting::new(Venue::Bitget, dec!(100.2)),
        );
        let apis: Vec<Arc<dyn VenueApi>> = vec![a.clone(), c.clone()];
        let cache = RatesCache::default();
        scan_into(&apis, &settings, &cache).await;
        scan(&apis, &settings).await;
        assert_eq!((a.calls(), c.calls()), (2, 2), "scan 总是全部现拉");
        // scan_into 记下的读数能被 scan_reusing 用上。
        scan_reusing(
            &apis,
            &settings,
            &cache,
            &[Venue::Lighter],
            Duration::from_secs(90),
        )
        .await;
        assert_eq!((a.calls(), c.calls()), (3, 2));
    }
}
