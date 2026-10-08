//! Lighter RH ↔ Arcus 同名合约价差监控（只读，不下单）。
//!
//! 两家都在 Robinhood Chain 上、都用 USDG 保证金，37 个同名合约（美股、指数、商品、加密币）。
//! 价格偶尔会偏离：一边比另一边贵出手续费加穿价以上。这里：
//!
//! 1. 用两家的行情 WebSocket 维护本地盘口（REST 轮询会打穿 Lighter RH 的 IP 限频，
//!    并挤占实盘下单用的额度）；
//! 2. 每秒按「这笔名义吃完深度的均价」算两个方向的可成交价差，扣掉 Arcus 两次吃单费
//!    （Lighter RH 吃单费为 0）和立即平仓的穿价；
//! 3. 每分钟落盘一行中间价基差，按合约、按时段（盘中 / 盘后 / 周末 / 加密币全天）统计「正常水平」。
//!    股票类合约在休市时常常有**系统性**偏差：现价差大不等于会收敛到 0，要看相对正常水平偏了多少；
//! 4. 偏离正常水平、且「回到正常水平」的预估净收益超过门槛时推送 Telegram（同一合约同一方向 30 分钟一条）。
//!
//! **只读**：不连任何账户、不下单。

mod book;
mod feed;
mod history;
mod session;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::alert::Alerter;
pub use book::{DirectionQuote, LocalBook, mid_basis_pct, quote};
pub use history::{Normal, Row};
pub use session::{Session, classify};

/// 监控配置（环境变量）。
#[derive(Debug, Clone)]
pub struct Config {
    pub enabled: bool,
    /// 估算用的单笔名义（USDT）。
    pub size_usdt: Decimal,
    /// 提醒门槛：回到正常水平的预估净收益（%）。
    pub alert_net_pct: Decimal,
    /// 只看股票类（股票、指数、商品），不看加密币。
    pub equities_only: bool,
    pub dir: PathBuf,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let decimal =
            |name: &str, default: &str, min: Decimal, max: Decimal| -> anyhow::Result<Decimal> {
                let raw = var(name).unwrap_or_else(|| default.to_string());
                let value = arb_core::parse_decimal(&raw)
                    .ok_or_else(|| anyhow::anyhow!("{name} 必须是十进制数，收到 {raw:?}"))?;
                anyhow::ensure!(
                    (min..=max).contains(&value),
                    "{name} 必须在 {min} 到 {max} 之间，收到 {value}"
                );
                Ok(value)
            };
        let enabled = !matches!(
            var("ARB_RH_SPREAD")
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("off" | "0" | "false")
        );
        Ok(Self {
            enabled,
            size_usdt: decimal(
                "ARB_RH_SPREAD_SIZE",
                "2000",
                Decimal::from(100),
                Decimal::from(100_000),
            )?,
            alert_net_pct: decimal(
                "ARB_RH_SPREAD_ALERT_PCT",
                "0.05",
                Decimal::new(1, 3),
                Decimal::from(5),
            )?,
            equities_only: matches!(
                var("ARB_RH_SPREAD_EQUITIES_ONLY").as_deref(),
                Some("1" | "on" | "true")
            ),
            dir: var("ARB_RH_SPREAD_DIR").map_or_else(|| PathBuf::from("rh-spread"), PathBuf::from),
        })
    }
}

/// 一个同名合约在两家的身份。
#[derive(Debug, Clone, Serialize)]
pub struct Market {
    pub base: String,
    pub lighter_id: i64,
    pub arcus_name: String,
    /// Arcus `category`：`EQUITIES` / `INDICES` / `COMMODITIES` / `CRYPTO`。
    pub category: String,
    #[serde(skip)]
    pub outside_rth: Option<bool>,
}

impl Market {
    pub fn crypto(&self) -> bool {
        self.category == "CRYPTO"
    }
}

/// Arcus 吃单费率（单边）。两家都按这一笔开、按这一笔平：Arcus 开平各一次。
#[derive(Debug, Clone, Copy)]
pub struct Fees {
    pub arcus_taker: Decimal,
    pub lighter_taker: Decimal,
}

impl Fees {
    /// 往返两腿手续费（%）：两家各开平一次。
    pub fn round_trip_pct(&self) -> Decimal {
        (self.arcus_taker + self.lighter_taker) * Decimal::TWO * Decimal::ONE_HUNDRED
    }
}

/// 页面上一行：一个合约此刻的状况。
#[derive(Debug, Clone, Serialize)]
pub struct Line {
    pub base: String,
    pub category: String,
    pub session: Session,
    /// 中间价基差（%）：(Arcus − Lighter RH) / 均值。
    pub basis_pct: Option<Decimal>,
    pub normal: Option<Normal>,
    /// 正常水平还差多少分钟样本（够了为 0）。
    pub normal_missing_minutes: usize,
    /// 偏离正常水平几个 MAD。
    pub z: Option<f64>,
    /// 两个方向的报价与净收益。
    pub long_arcus: Option<Leg>,
    pub long_lighter: Option<Leg>,
    /// 更好的那个方向。
    pub best: Option<Best>,
    /// Arcus 盘口多久没更新（秒）。
    pub age_sec: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Leg {
    #[serde(flatten)]
    pub quote: DirectionQuote,
    /// 收敛到 0 时的净收益（%）：可成交价差 − 平仓穿价 − 往返手续费。
    pub net_to_zero_pct: Decimal,
    /// 回到正常水平时的净收益（%）。没有正常水平时为 `None`。
    pub net_to_normal_pct: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Best {
    /// `long_arcus` / `long_lighter`。
    pub direction: &'static str,
    /// 达到提醒条件（回到正常水平净收益 ≥ 门槛、样本够、盘口新鲜）。
    pub signal: bool,
    /// 信号已经连续保持了多少秒（[`SIGNAL_HOLD`] 之后才推送）。
    pub signal_sec: u64,
    /// 按 [`Config::size_usdt`] 折算的回到正常水平净收益（USDT）。
    pub net_usdt: Option<Decimal>,
}

/// 给接口的整体快照。
#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub enabled: bool,
    pub size_usdt: Decimal,
    pub alert_net_pct: Decimal,
    pub fee_round_trip_pct: Option<Decimal>,
    pub min_minutes: usize,
    pub window_days: i64,
    pub history_minutes: usize,
    pub connected: Connected,
    pub updated_at: Option<DateTime<Utc>>,
    pub lines: Vec<Line>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Connected {
    pub lighter: bool,
    pub arcus: bool,
    pub reconnects: u64,
}

/// 两家盘口超过这么久没更新就不算数（WebSocket 静默断流时不会报错）。
const STALE: Duration = Duration::from_secs(15);
/// 信号要连续保持这么久才推送：一秒钟的闪烁（一档挂单被吃掉又补上）不值得叫人。
pub const SIGNAL_HOLD: Duration = Duration::from_secs(10);
/// 价差提醒自己的限频：每分钟最多这么多条。告警通道全局每分钟 6 条、与交易告警共用，
/// 一波行情同时亮十几个合约时不能把规则平仓、裸敞口这类告警挤掉。
pub const ALERTS_PER_MINUTE: usize = 2;

/// 估算一个合约此刻的状况。纯计算，单测覆盖。
#[allow(clippy::too_many_arguments)]
pub fn evaluate(
    market: &Market,
    arcus: Option<&LocalBook>,
    lighter: Option<&LocalBook>,
    fees: Option<Fees>,
    normal: Option<Normal>,
    normal_minutes: usize,
    session: Session,
    config: &Config,
    now: Instant,
) -> Line {
    let mut line = Line {
        base: market.base.clone(),
        category: market.category.clone(),
        session,
        basis_pct: None,
        normal_missing_minutes: history::MIN_MINUTES.saturating_sub(normal_minutes),
        normal: normal.clone(),
        z: None,
        long_arcus: None,
        long_lighter: None,
        best: None,
        age_sec: None,
        note: None,
    };
    let (Some(arcus), Some(lighter)) = (arcus, lighter) else {
        line.note = Some("等待两家盘口".into());
        return line;
    };
    // Arcus 每 ~1.3 秒推一份完整快照（盘口没变也推），它的年龄就是数据新鲜度。
    // Lighter 只推变化：安静的市场几秒没增量是常态，新鲜度靠连接活着保证（断线即清空盘口），
    // 所以这里只按 Arcus 判断是否过期。
    let age = now.saturating_duration_since(arcus.updated);
    line.age_sec = Some(age.as_secs());
    if age > STALE {
        line.note = Some(format!("Arcus 盘口 {} 秒没更新，不计算", age.as_secs()));
        return line;
    }
    let Some(basis) = mid_basis_pct(arcus, lighter) else {
        line.note = Some("至少一家盘口为空或交叉".into());
        return line;
    };
    line.basis_pct = Some(basis);
    let Some(fees) = fees else {
        line.note = Some("还没取到 Arcus 吃单费率".into());
        return line;
    };
    let fee = fees.round_trip_pct();
    if let Some(n) = &normal
        && let Some(b) = basis.to_f64()
    {
        // MAD 太小（几乎不动的合约）时下限 0.005%，免得一点抖动就几十个 σ。
        line.z = Some(((b - n.median) / n.mad.max(0.005) * 1000.0).round() / 1000.0);
    }
    let normal_median = normal
        .as_ref()
        .and_then(|n| Decimal::from_f64_retain(n.median))
        .map(|d| d.round_dp(5));
    // 方向：多 Arcus 空 Lighter 赚的是「Arcus 相对 Lighter 变贵」（基差上升）；反方向赚基差下降。
    // 回到正常水平的净收益 = 可成交价差 − 平仓穿价 − 手续费 −/＋ 正常水平（正常基差本身是收不回来的那部分）。
    let leg = |quote: DirectionQuote, sign: Decimal| {
        let net_to_zero_pct = (quote.entry_pct - quote.exit_cross_pct - fee).round_dp(5);
        Leg {
            net_to_normal_pct: normal_median
                .map(|median| (net_to_zero_pct + sign * median).round_dp(5)),
            net_to_zero_pct,
            quote,
        }
    };
    // 多 Arcus 空 RH：入场价差 = RH 卖 − Arcus 买，正常基差 m = Arcus − RH；回到 m 时还剩 −m 收不回 → +m。
    line.long_arcus = quote(arcus, lighter, config.size_usdt).map(|q| leg(q, Decimal::ONE));
    line.long_lighter = quote(lighter, arcus, config.size_usdt).map(|q| leg(q, -Decimal::ONE));
    if line.long_arcus.is_none() && line.long_lighter.is_none() {
        line.note = Some(format!("深度不够 {} USDT", config.size_usdt.normalize()));
        return line;
    }
    let score = |leg: &Option<Leg>| {
        leg.as_ref()
            .map(|l| l.net_to_normal_pct.unwrap_or(l.net_to_zero_pct))
    };
    let (direction, chosen) = match (score(&line.long_arcus), score(&line.long_lighter)) {
        (Some(a), Some(l)) if l > a => ("long_lighter", &line.long_lighter),
        (Some(_), _) => ("long_arcus", &line.long_arcus),
        _ => ("long_lighter", &line.long_lighter),
    };
    let chosen = chosen.as_ref().expect("至少一个方向有报价");
    let signal = chosen
        .net_to_normal_pct
        .is_some_and(|net| net >= config.alert_net_pct);
    line.best = Some(Best {
        direction,
        signal,
        signal_sec: 0,
        net_usdt: chosen
            .net_to_normal_pct
            .map(|pct| (pct / Decimal::ONE_HUNDRED * config.size_usdt).round_dp(2)),
    });
    line
}

/// 一分钟内的采样（算中位数用）。
#[derive(Default)]
struct Minute {
    start: i64,
    basis: HashMap<String, Vec<f64>>,
    entry_arcus: HashMap<String, f64>,
    entry_lighter: HashMap<String, f64>,
    sessions: HashMap<String, Session>,
}

impl Minute {
    fn rows(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = self
            .basis
            .iter()
            .filter(|(_, values)| !values.is_empty())
            .map(|(base, values)| {
                let mut sorted = values.clone();
                sorted.sort_by(f64::total_cmp);
                Row {
                    t: self.start,
                    s: base.clone(),
                    k: self.sessions.get(base).copied().unwrap_or(Session::All),
                    b: (history::percentile(&sorted, 0.5) * 1e5).round() / 1e5,
                    n: u32::try_from(values.len()).unwrap_or(u32::MAX),
                    ea: self.entry_arcus.get(base).copied(),
                    el: self.entry_lighter.get(base).copied(),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.s.cmp(&b.s));
        rows
    }
}

/// 从行情 WebSocket 读到的东西，送到计算任务。
enum Feed {
    Arcus(feed::Event),
    Lighter(feed::Event),
    /// 某家连接状态变了。
    Up(&'static str, bool),
}

pub struct Monitor {
    config: Config,
    view: RwLock<View>,
    alerts: Arc<Alerter>,
    /// 最近一分钟发出的价差提醒时刻（[`ALERTS_PER_MINUTE`]）。
    sent: std::sync::Mutex<std::collections::VecDeque<Instant>>,
}

impl Monitor {
    pub fn new(config: Config, alerts: Arc<Alerter>) -> Arc<Self> {
        let view = View {
            enabled: config.enabled,
            size_usdt: config.size_usdt,
            alert_net_pct: config.alert_net_pct,
            fee_round_trip_pct: None,
            min_minutes: history::MIN_MINUTES,
            window_days: history::WINDOW_DAYS,
            history_minutes: 0,
            connected: Connected::default(),
            updated_at: None,
            lines: Vec::new(),
            error: (!config.enabled).then(|| "已关闭（ARB_RH_SPREAD=off）".to_string()),
        };
        Arc::new(Self {
            config,
            view: RwLock::new(view),
            alerts,
            sent: std::sync::Mutex::new(std::collections::VecDeque::new()),
        })
    }

    pub async fn view(&self) -> View {
        self.view.read().await.clone()
    }

    pub fn spawn(self: &Arc<Self>, client: reqwest::Client) {
        if !self.config.enabled {
            info!("Lighter RH ↔ Arcus 价差监控已关闭");
            return;
        }
        let monitor = Arc::clone(self);
        tokio::spawn(async move { monitor.run(client).await });
    }

    async fn set_error(&self, error: Option<String>) {
        self.view.write().await.error = error;
    }

    /// 发现两家的同名市场。失败就隔一会儿重试，不让监控把整个看板拖死。
    async fn discover(&self, client: &reqwest::Client) -> anyhow::Result<(Vec<Market>, Fees)> {
        let arcus: Value = arb_venues::get_json(
            client.get("https://api.arcus.xyz/v1/markets"),
            arb_core::Venue::Arcus,
        )
        .await?;
        let arcus_taker = arb_venues::arcus::fetch_base_taker_fee(client).await?;
        let lighter: Value = arb_venues::get_json(
            client.get("https://api.rh.lighter.xyz/api/v1/orderBookDetails"),
            arb_core::Venue::LighterRh,
        )
        .await?;
        discover_from(&arcus, arcus_taker, &lighter, self.config.equities_only)
    }

    async fn run(self: Arc<Self>, client: reqwest::Client) {
        let (mut markets, mut fees) = loop {
            match self.discover(&client).await {
                Ok(found) => break found,
                Err(error) => {
                    warn!("价差监控：取两家市场列表失败，60 秒后重试：{error:#}");
                    self.set_error(Some(format!("取两家市场列表失败：{error:#}")))
                        .await;
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        };
        info!(markets = markets.len(), "Lighter RH ↔ Arcus 价差监控启动");
        let (mut history, broken) = history::load(&self.config.dir, Utc::now()).await;
        if broken > 0 {
            warn!(broken, "价差历史里有坏行，已跳过");
        }
        {
            let mut view = self.view.write().await;
            view.error = None;
            view.fee_round_trip_pct = Some(fees.round_trip_pct());
            view.history_minutes = history.coverage_minutes();
        }

        let (tx, mut rx) = mpsc::channel::<Feed>(4096);
        let lighter_ids: Vec<i64> = markets.iter().map(|m| m.lighter_id).collect();
        let arcus_names: Vec<String> = markets.iter().map(|m| m.arcus_name.clone()).collect();
        let (resub_tx, resub_rx) = mpsc::channel::<i64>(64);
        tokio::spawn(lighter_loop(lighter_ids, tx.clone(), resub_rx));
        tokio::spawn(arcus_loop(arcus_names, tx.clone()));

        let by_lighter: HashMap<String, usize> = markets
            .iter()
            .enumerate()
            .map(|(i, m)| (m.lighter_id.to_string(), i))
            .collect();
        let by_arcus: HashMap<String, usize> = markets
            .iter()
            .enumerate()
            .map(|(i, m)| (m.arcus_name.clone(), i))
            .collect();
        let mut arcus_books: Vec<Option<LocalBook>> = vec![None; markets.len()];
        let mut lighter_books: Vec<Option<LocalBook>> = vec![None; markets.len()];
        let mut connected = Connected::default();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh = tokio::time::interval(Duration::from_secs(300));
        refresh.tick().await;
        let mut minute = Minute {
            start: Utc::now().timestamp() / 60 * 60,
            ..Minute::default()
        };
        // 正常水平只在每分钟落一行之后才会变：按（合约，时段）缓存，换分钟时清空。
        let mut normals: HashMap<(String, Session), (Option<Normal>, usize)> = HashMap::new();
        // 信号从什么时候开始连续成立（合约, 方向）。
        let mut signal_since: HashMap<(String, &'static str), Instant> = HashMap::new();

        loop {
            tokio::select! {
                Some(message) = rx.recv() => match message {
                    Feed::Up(which, up) => {
                        if which == "lighter" { connected.lighter = up } else { connected.arcus = up }
                        if !up {
                            connected.reconnects += 1;
                            // 断线后旧盘口不能再用：等重连后的快照。
                            let books = if which == "lighter" { &mut lighter_books } else { &mut arcus_books };
                            books.iter_mut().for_each(|b| *b = None);
                        }
                    }
                    Feed::Arcus(event) => apply(&mut arcus_books, &by_arcus, event, None),
                    Feed::Lighter(event) => apply(&mut lighter_books, &by_lighter, event, Some(&resub_tx)),
                },
                _ = refresh.tick() => {
                    // 交易时段（isOutsideRth）与费率会变：5 分钟刷新一次（Arcus markets 权重 20，IP 预算 1500/分钟）。
                    if let Ok((fresh, fresh_fees)) = self.discover(&client).await {
                        fees = fresh_fees;
                        for market in &mut markets {
                            if let Some(found) = fresh.iter().find(|f| f.base == market.base) {
                                market.outside_rth = found.outside_rth;
                            }
                        }
                    }
                }
                _ = tick.tick() => {
                    if crate::shutdown::is_draining() {
                        // 停机：把这一分钟写掉再退出循环。
                        let _ = history::append(&self.config.dir, &minute.rows(), Utc::now()).await;
                        return;
                    }
                    let now_utc = Utc::now();
                    let now = Instant::now();
                    let start = now_utc.timestamp() / 60 * 60;
                    if start != minute.start {
                        let rows = minute.rows();
                        rows.iter().for_each(|row| history.push(row));
                        history.prune(now_utc);
                        if let Err(error) = history::append(&self.config.dir, &rows, now_utc).await {
                            warn!("价差历史写不进去：{error}");
                        }
                        minute = Minute { start, ..Minute::default() };
                        normals.clear();
                    }
                    let mut lines = Vec::with_capacity(markets.len());
                    for (i, market) in markets.iter().enumerate() {
                        let session = classify(now_utc, market.crypto(), market.outside_rth);
                        let (normal, normal_minutes) = normals
                            .entry((market.base.clone(), session))
                            .or_insert_with(|| {
                                (history.normal(&market.base, session, now_utc), history.minutes(&market.base, session, now_utc))
                            })
                            .clone();
                        let mut line = evaluate(
                            market,
                            arcus_books[i].as_ref(),
                            lighter_books[i].as_ref(),
                            Some(fees),
                            normal,
                            normal_minutes,
                            session,
                            &self.config,
                            now,
                        );
                        if let Some(best) = line.best.as_mut() {
                            let key = (market.base.clone(), best.direction);
                            if best.signal {
                                let since = *signal_since.entry(key).or_insert(now);
                                best.signal_sec = now.saturating_duration_since(since).as_secs();
                            } else {
                                signal_since.remove(&key);
                            }
                        }
                        // 方向换了或没信号：另一个方向的计时作废。
                        signal_since.retain(|(base, direction), _| {
                            base != &market.base
                                || line.best.as_ref().is_some_and(|b| b.signal && b.direction == *direction)
                        });
                        if let Some(basis) = line.basis_pct.and_then(|b| b.to_f64()) {
                            minute.basis.entry(market.base.clone()).or_default().push(basis);
                            minute.sessions.insert(market.base.clone(), session);
                            let keep_max = |map: &mut HashMap<String, f64>, value: Option<f64>| {
                                if let Some(value) = value {
                                    let slot = map.entry(market.base.clone()).or_insert(value);
                                    *slot = slot.max(value);
                                }
                            };
                            keep_max(&mut minute.entry_arcus, line.long_arcus.as_ref().and_then(|l| l.quote.entry_pct.to_f64()));
                            keep_max(&mut minute.entry_lighter, line.long_lighter.as_ref().and_then(|l| l.quote.entry_pct.to_f64()));
                        }
                        self.maybe_alert(&line);
                        lines.push(line);
                    }
                    lines.sort_by(|a, b| {
                        let key = |l: &Line| l.best.as_ref().and_then(|b| b.net_usdt).unwrap_or(Decimal::MIN);
                        key(b).cmp(&key(a)).then_with(|| a.base.cmp(&b.base))
                    });
                    let mut view = self.view.write().await;
                    view.connected = connected.clone();
                    view.fee_round_trip_pct = Some(fees.round_trip_pct());
                    view.history_minutes = history.coverage_minutes();
                    view.updated_at = Some(now_utc);
                    view.lines = lines;
                }
            }
        }
    }

    fn maybe_alert(&self, line: &Line) {
        let Some(best) = line
            .best
            .as_ref()
            .filter(|b| b.signal && b.signal_sec >= SIGNAL_HOLD.as_secs())
        else {
            return;
        };
        let Some(text) = alert_text(line, best, &self.config) else {
            return;
        };
        let now = Instant::now();
        let mut sent = self
            .sent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while sent
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(60))
        {
            sent.pop_front();
        }
        if sent.len() >= ALERTS_PER_MINUTE {
            return;
        }
        if self
            .alerts
            .notify(&format!("rh-spread:{}:{}", line.base, best.direction), text)
        {
            sent.push_back(now);
            info!(symbol = %line.base, direction = best.direction, net_usdt = ?best.net_usdt, "价差监控：推送提醒");
        }
    }
}

/// 提醒文本。
pub fn alert_text(line: &Line, best: &Best, config: &Config) -> Option<String> {
    let leg = if best.direction == "long_arcus" {
        line.long_arcus.as_ref()?
    } else {
        line.long_lighter.as_ref()?
    };
    let normal = line.normal.as_ref()?;
    let (long, short) = if best.direction == "long_arcus" {
        ("Arcus", "Lighter RH")
    } else {
        ("Lighter RH", "Arcus")
    };
    Some(format!(
        "📈 价差偏离：{} {}，多 {long} / 空 {short}\n可成交价差 {}%（{} USDT），{}正常基差 {:.3}%，当前 {}%\n回到正常水平预估净赚 {}%（≈{} USDT，已扣手续费与平仓穿价）\n只读提醒，不会自动下单；同一合约同一方向 30 分钟内不重复。",
        line.base,
        line.session.label(),
        leg.quote.entry_pct.round_dp(3),
        config.size_usdt.normalize(),
        line.session.label(),
        normal.median,
        line.basis_pct?.round_dp(3),
        leg.net_to_normal_pct?.round_dp(3),
        best.net_usdt?.normalize(),
    ))
}

fn apply(
    books: &mut [Option<LocalBook>],
    index: &HashMap<String, usize>,
    event: feed::Event,
    resub: Option<&mpsc::Sender<i64>>,
) {
    let now = Instant::now();
    match event {
        feed::Event::Snapshot {
            market,
            bids,
            asks,
            nonce,
        } => {
            let Some(&i) = index.get(&market) else { return };
            let mut book = LocalBook::new(now);
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce;
            books[i] = Some(book);
        }
        feed::Event::Delta {
            market,
            bids,
            asks,
            begin_nonce,
            nonce,
        } => {
            let Some(&i) = index.get(&market) else { return };
            let Some(book) = books[i].as_mut() else {
                return;
            };
            if let (Some(last), Some(begin)) = (book.nonce, begin_nonce)
                && last != begin
            {
                // 丢了增量：这本盘口不能再信，作废并重订阅拿新快照。
                debug!(market, last, begin, "Lighter 盘口增量不连续，重订阅");
                books[i] = None;
                if let (Some(resub), Ok(id)) = (resub, market.parse::<i64>()) {
                    let _ = resub.try_send(id);
                }
                return;
            }
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce.or(book.nonce);
            book.updated = now;
        }
        feed::Event::Error(message) => warn!("价差监控：行情服务端报错：{message}"),
        feed::Event::Other => {}
    }
}

/// 从两家的市场列表里找同名、在线的永续。纯函数，单测覆盖。
///
/// `arcus_taker`：Arcus 基础档吃单费率（小数，档位只会更低，所以是上限）。Lighter RH 的
/// `taker_fee` 与扫描器同口径直接当小数用（实测为 `"0.0000"`）。
pub fn discover_from(
    arcus: &Value,
    arcus_taker: Decimal,
    lighter: &Value,
    equities_only: bool,
) -> anyhow::Result<(Vec<Market>, Fees)> {
    let arcus_rows = arcus
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Arcus markets 格式不对"))?;
    let lighter_rows = lighter
        .get("order_book_details")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Lighter RH orderBookDetails 格式不对"))?;
    let lighter_ids: HashMap<&str, (i64, Option<Decimal>)> = lighter_rows
        .iter()
        .filter(|r| r.get("status").and_then(Value::as_str) == Some("active"))
        .filter(|r| {
            r.get("market_type")
                .and_then(Value::as_str)
                .is_none_or(|t| t == "perp")
        })
        .filter_map(|r| {
            let fee = r
                .get("taker_fee")
                .and_then(Value::as_str)
                .and_then(arb_core::parse_decimal);
            Some((
                r.get("symbol")?.as_str()?,
                (r.get("market_id")?.as_i64()?, fee),
            ))
        })
        .collect();
    // 任何一个同名市场没报 Lighter 费率：不拿 0 冒充，按 Arcus 费率保守估计。
    let mut lighter_fee = Some(Decimal::ZERO);
    let mut markets: Vec<Market> = arcus_rows
        .iter()
        .filter(|m| m.get("status").and_then(Value::as_str) == Some("ONLINE"))
        .filter(|m| m.get("type").and_then(Value::as_str) == Some("PERPETUAL"))
        .filter_map(|m| {
            let base = m.get("baseAsset")?.as_str()?;
            let name = m.get("marketDisplayName")?.as_str()?;
            if name != format!("{base}-USD") {
                return None;
            }
            let &(lighter_id, fee) = lighter_ids.get(base)?;
            let category = m
                .get("category")
                .and_then(Value::as_str)
                .unwrap_or("CRYPTO")
                .to_string();
            if equities_only && category == "CRYPTO" {
                return None;
            }
            lighter_fee = lighter_fee.zip(fee).map(|(a, b)| a.max(b));
            Some(Market {
                base: base.to_string(),
                lighter_id,
                arcus_name: name.to_string(),
                outside_rth: m.get("isOutsideRth").and_then(Value::as_bool),
                category,
            })
        })
        .collect();
    markets.sort_by(|a, b| a.base.cmp(&b.base));
    anyhow::ensure!(!markets.is_empty(), "两家没有同名在线永续");
    anyhow::ensure!(
        (Decimal::ZERO..Decimal::new(1, 2)).contains(&arcus_taker),
        "Arcus 吃单费率 {arcus_taker} 超出合理范围"
    );
    Ok((
        markets,
        Fees {
            arcus_taker,
            lighter_taker: lighter_fee.unwrap_or(arcus_taker),
        },
    ))
}

// ───────────────────────────── WebSocket 连接 ─────────────────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> anyhow::Result<Ws> {
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(15),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .map_err(|_| anyhow::anyhow!("连接超时"))??;
    Ok(ws)
}

/// 重连等待：1、2、4 … 秒，封顶 60 秒；连上并稳定 60 秒后复位。
fn backoff(failures: u32) -> Duration {
    Duration::from_secs(1u64 << failures.min(6)).min(Duration::from_secs(60))
}

async fn lighter_loop(ids: Vec<i64>, tx: mpsc::Sender<Feed>, mut resub: mpsc::Receiver<i64>) {
    let mut failures = 0u32;
    loop {
        if crate::shutdown::is_draining() {
            return;
        }
        let started = Instant::now();
        match lighter_session(&ids, &tx, &mut resub).await {
            Ok(()) => return,
            Err(error) => {
                let _ = tx.send(Feed::Up("lighter", false)).await;
                if started.elapsed() > Duration::from_secs(60) {
                    failures = 0;
                }
                let wait = backoff(failures);
                failures += 1;
                warn!(
                    "价差监控：Lighter RH 行情断开，{} 秒后重连：{error:#}",
                    wait.as_secs()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

async fn lighter_session(
    ids: &[i64],
    tx: &mpsc::Sender<Feed>,
    resub: &mut mpsc::Receiver<i64>,
) -> anyhow::Result<()> {
    let mut ws = connect(feed::LIGHTER_WS).await?;
    // 每分钟最多 200 条客户端消息：37 个订阅一次发完没问题，重订阅另有节流。
    for id in ids {
        ws.send(Message::text(feed::lighter_subscribe(*id))).await?;
    }
    let _ = tx.send(Feed::Up("lighter", true)).await;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    let mut last_resub: HashMap<i64, Instant> = HashMap::new();
    loop {
        tokio::select! {
            message = tokio::time::timeout(STALE * 2, ws.next()) => {
                let message = message.map_err(|_| anyhow::anyhow!("{} 秒没收到任何消息", (STALE * 2).as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => match feed::parse_lighter(&text) {
                        Ok(event) => { if tx.send(Feed::Lighter(event)).await.is_err() { return Ok(()); } }
                        Err(error) => debug!("Lighter 消息解析失败：{error}"),
                    },
                    Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(frame))) => anyhow::bail!("服务端关闭连接：{frame:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => anyhow::bail!("连接结束"),
                }
            }
            Some(id) = resub.recv() => {
                // 同一个市场 10 秒内最多重订阅一次。
                if last_resub.get(&id).is_none_or(|at| at.elapsed() > Duration::from_secs(10)) {
                    last_resub.insert(id, Instant::now());
                    ws.send(Message::text(feed::lighter_unsubscribe(id))).await?;
                    ws.send(Message::text(feed::lighter_subscribe(id))).await?;
                }
            }
            _ = ping.tick() => {
                if crate::shutdown::is_draining() { let _ = ws.close(None).await; return Ok(()); }
                ws.send(Message::text(r#"{"type":"ping"}"#)).await?;
            }
        }
    }
}

async fn arcus_loop(names: Vec<String>, tx: mpsc::Sender<Feed>) {
    let mut failures = 0u32;
    loop {
        if crate::shutdown::is_draining() {
            return;
        }
        let started = Instant::now();
        match arcus_session(&names, &tx).await {
            Ok(()) => return,
            Err(error) => {
                let _ = tx.send(Feed::Up("arcus", false)).await;
                if started.elapsed() > Duration::from_secs(60) {
                    failures = 0;
                }
                let wait = backoff(failures);
                failures += 1;
                warn!(
                    "价差监控：Arcus 行情断开，{} 秒后重连：{error:#}",
                    wait.as_secs()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

async fn arcus_session(names: &[String], tx: &mpsc::Sender<Feed>) -> anyhow::Result<()> {
    let mut ws = connect(feed::ARCUS_WS).await?;
    for name in names {
        ws.send(Message::text(feed::arcus_subscribe(name))).await?;
    }
    let _ = tx.send(Feed::Up("arcus", true)).await;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    loop {
        tokio::select! {
            message = tokio::time::timeout(STALE * 2, ws.next()) => {
                let message = message.map_err(|_| anyhow::anyhow!("{} 秒没收到任何消息", (STALE * 2).as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => match feed::parse_arcus(&text) {
                        Ok(event) => { if tx.send(Feed::Arcus(event)).await.is_err() { return Ok(()); } }
                        Err(error) => debug!("Arcus 消息解析失败：{error}"),
                    },
                    Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(frame))) => anyhow::bail!("服务端关闭连接：{frame:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => anyhow::bail!("连接结束"),
                }
            }
            _ = ping.tick() => {
                if crate::shutdown::is_draining() { let _ = ws.close(None).await; return Ok(()); }
                // WebSocket 协议层 ping：Arcus 连接 24 小时自动断，断了就重连。
                ws.send(Message::Ping(Vec::new().into())).await?;
            }
        }
    }
}
