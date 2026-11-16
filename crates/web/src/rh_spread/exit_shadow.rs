//! 分批平仓测算（影子模式：只观察、不下单）。
//!
//! 想法：薄盘口上整笔平仓要吃穿好几档，按盘口算不赚；先平第一档够的那一小部分，等盘口补回来再平下一部分，
//! 也许每一部分都赚。可它能省的只有「吃深度」这一项 —— 买卖价差和手续费跟平仓量成正比，切多少片都一样付；
//! 实盘规则平仓吃深度平均只有几美分。值不值得做，先在这里量：
//!
//! - 每秒对每笔双腿持仓，用价差监控的实时盘口（与开仓同一份：同样的市场名、Arcus 剔掉价格带外的档位、
//!   Gate 这类按张数的量已换算成标的数量）算一条「梯子」：两条腿各自盘口的每一档累计数量都是一个断点，
//!   平掉这个比例的预估净额（[`arb_exec::monitor::exit_quote_part`]，与整笔平仓同一套算法：两腿按吃单费率估
//!   平仓费，已实现盈亏、已付手续费按比例摊进来）。
//! - **只记数、不下结论**：部分平掉之后止盈怎么算还没定，所以记的是每一档的净额和资金费，门槛事后再套。
//! - 有一档（摊上资金费后）为正就每秒记一行，转为不正时补记一行，看机会能持续多久、档位多快补回来；
//!   其余时候每 [`BASELINE_EVERY`] 记一行，做对照。
//! - 局限：盘口被我们吃掉之后多久补回来，不真下单量不出来；这里量到的是「正的机会出现多久、多大」。
//!   Hyperliquid 的盘口推送约 5 秒一次，每行带两腿盘口的年龄，事后可以只看新鲜的。
//!
//! 记录写进 `rh-spread/exit-shadow-YYYYMMDD.jsonl`（保留 [`KEEP_DAYS`] 天）。
//!
//! **盘口触发止盈**：规则轮约 60 秒才看一次止盈，而按盘口达到止盈的窗口中位只有 2~3 秒。连续 [`TRIGGER_SAMPLES`]
//! 次采样两腿盘口都新鲜（不超过 [`TRIGGER_FRESH_MS`]）、整笔平仓按盘口的净额（含资金费）都到止盈，这一行就标
//! `would_close`，并交给实盘交易台（[`Shadow::take_fired`]，同一笔至多每 [`TRIGGER_COOLDOWN`] 一次）：它用 REST
//! 现拉盘口再核对两次，仍到止盈才平（见 `arb_exec::desk::book_trigger_exit`）。2026-10-31～11-02 只记录不下单时，
//! 只记录不下单时，测算第一次判定该平时的整笔净额比实际由规则轮平掉时更高，之后接上了实盘。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{Decimal, Level, OrderBook, Venue};
use arb_exec::PairPosition;
use arb_exec::monitor::{ExitFees, exit_quote_part};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::book::LocalBook;
use super::pairs::Catalog;

/// 盘口触发平仓测算：连续几次采样都到止盈才算触发（隔约 1 秒，相当于规则轮里 700 毫秒的复核）。
pub const TRIGGER_SAMPLES: u32 = 2;
/// 盘口触发平仓测算只认两腿盘口都不超过这么旧的采样（毫秒）。
pub const TRIGGER_FRESH_MS: u64 = 3000;
/// 只推变化的场所（Lighter、币安、Bitget、MEXC）盘口多旧以内还算数（毫秒），见 `Sample::at_take_profit`。
pub const TRIGGER_FRESH_PUSH_MS: u64 = 15_000;
/// 同一笔持仓的「会触发」日志最多多久一条（记录照常每行都标）。
const TRIGGER_LOG_EVERY: Duration = Duration::from_secs(300);
/// 同一笔持仓至多多久交给交易台一次：交易台那边要现拉两次盘口（隔 0.7 秒复核），复核没过时窗口通常还在，
/// 每秒都交过去只是在刷交易所的接口。
pub const TRIGGER_COOLDOWN: Duration = Duration::from_secs(20);

/// 不为正时多久记一行做对照。
pub const BASELINE_EVERY: Duration = Duration::from_secs(60);
/// 盘口缺了这么久才警告。价差监控每次重连（订阅列表一变、服务端断线）盘口都会空一两秒，那不是问题；
/// 2026-10-28 一天出了 1000 多条「盘口还没收到」，几乎全是这种一秒的空档。
pub const WARN_AFTER: Duration = Duration::from_secs(30);
/// 记录保留几天。
pub const KEEP_DAYS: i64 = 14;
/// 梯子最多几档（不含整笔）：再往后吃的就是整笔平仓本来就会吃的深度，没有新信息。
const MAX_RUNGS: usize = 8;
/// 盘口每侧最多换算几档。
const BOOK_LEVELS: usize = 25;
const FILE_PREFIX: &str = "exit-shadow";

/// 一笔要盯的持仓：规则轮每轮结束时交过来（见 [`super::Monitor::set_exit_source`]）。
#[derive(Debug, Clone)]
pub struct Watched {
    pub position: PairPosition,
    /// 两条腿平仓的吃单费率（与规则轮按盘口核对止盈时同一个数）。
    pub fees: ExitFees,
    /// 开仓以来已收付的资金费（正 = 收到）。没查到为 `None`。
    pub funding_usdt: Option<Decimal>,
}

/// 梯子上的一档：两腿各平 `fraction` 的数量。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rung {
    pub fraction: Decimal,
    /// 这一部分的多腿入场名义。
    pub notional_usdt: Decimal,
    /// 按盘口平掉这一部分的预估净额（不含资金费）。
    pub net_usdt: Decimal,
    /// 其中吃深度的成本（盘口缺一侧时为 `None`）。
    pub depth_usdt: Option<Decimal>,
}

/// 一行记录。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub at: DateTime<Utc>,
    pub position: String,
    pub symbol: String,
    pub long: Venue,
    pub short: Venue,
    /// 多腿现在的入场名义（整笔）。
    pub notional_usdt: Decimal,
    pub take_profit_usdt: Option<Decimal>,
    pub funding_usdt: Option<Decimal>,
    /// 两腿盘口距上次更新多久（毫秒）。
    pub long_age_ms: u64,
    pub short_age_ms: u64,
    /// 有一档摊上资金费后为正。
    pub positive: bool,
    /// 由小到大；最后一档是整笔（整笔深度不够时没有这一档）。
    pub ladder: Vec<Rung>,
    /// 盘口触发平仓测算：这一行补齐了连续 [`TRIGGER_SAMPLES`] 次「盘口新鲜、整笔含资金费到止盈」。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub would_close: bool,
}

impl Sample {
    /// 这一次采样算不算「到止盈」：两腿盘口都新鲜，整笔那一档摊上资金费后不低于止盈额。
    fn at_take_profit(&self) -> bool {
        let Some(target) = self.take_profit_usdt else {
            return false;
        };
        // 快照式盘口（Arcus、HL：盘口没变也推）3 秒没更新就是旧了；只推变化的那几家（Lighter、币安、Bitget、MEXC）
        // 安静时几秒没增量是常态（11-09 线上相邻两次更新：Lighter p99 约 2.5 秒，币安 / Bitget p99 约 10 秒），按 3 秒卡会
        // 错过一部分止盈窗口，放宽到 [`TRIGGER_FRESH_PUSH_MS`]。不完全不看：交易台每次复核都现拉两次 REST 盘口，
        // 一直按旧盘口触发会刷 Lighter 的接口（429 封 IP 60 秒）。连接停推另有「多久没收到行情数据」重连兜底。
        let fresh_leg = |venue: Venue, age_ms: u64| {
            age_ms
                <= if super::snapshot_feed(venue) {
                    TRIGGER_FRESH_MS
                } else {
                    TRIGGER_FRESH_PUSH_MS
                }
        };
        let fresh =
            fresh_leg(self.long, self.long_age_ms) && fresh_leg(self.short, self.short_age_ms);
        fresh
            && self
                .ladder
                .last()
                .filter(|rung| rung.fraction == Decimal::ONE)
                .is_some_and(|whole| self.net_with_funding(whole) >= target)
    }

    /// 平掉这一档、按比例摊上资金费后的净额。资金费不知道按 0 算（这一行的 `funding_usdt` 为空，事后可以剔掉）。
    pub fn net_with_funding(&self, rung: &Rung) -> Decimal {
        rung.net_usdt + self.funding_usdt.unwrap_or_default() * rung.fraction
    }
}

struct Unavailable {
    why: String,
    since: Instant,
    warned: bool,
}

impl Unavailable {
    fn new(why: &str, now: Instant) -> Self {
        Self {
            why: why.to_string(),
            since: now,
            warned: false,
        }
    }
}

/// 测算状态：每笔持仓上次记录的时刻、上次是否为正。
pub struct Shadow {
    dir: PathBuf,
    last: HashMap<String, (Instant, bool)>,
    /// 盘口触发平仓测算：每笔持仓连续几次到止盈、上次打日志的时刻。
    streak: HashMap<String, u32>,
    trigger_logged: HashMap<String, Instant>,
    /// 上次交给交易台的时刻（[`TRIGGER_COOLDOWN`]），和这一次采样里新交出去的持仓 id。
    trigger_fired: HashMap<String, Instant>,
    fired: Vec<String>,
    /// 算不了的持仓：原因、从什么时候起、警告过没有。同一原因持续 [`WARN_AFTER`] 才警告一次。
    unavailable: HashMap<String, Unavailable>,
    writes: Vec<tokio::task::JoinHandle<()>>,
    pruned_on: Option<chrono::NaiveDate>,
    /// 按深度分批平仓的影子（[`super::exit_slice`]），盘口与这里同一份。
    slicer: super::exit_slice::Slicer,
}

impl Shadow {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            last: HashMap::new(),
            streak: HashMap::new(),
            trigger_logged: HashMap::new(),
            trigger_fired: HashMap::new(),
            fired: Vec::new(),
            unavailable: HashMap::new(),
            writes: Vec::new(),
            pruned_on: None,
            slicer: super::exit_slice::Slicer::restore(dir, Utc::now()),
        }
    }

    /// 整秒调用一次：按现在的盘口给每笔持仓算梯子，该记的记下来（后台写盘）。返回这一次记了几行。
    pub fn sample(
        &mut self,
        watched: &[Watched],
        books: &HashMap<(Venue, String), LocalBook>,
        catalog: &Catalog,
        now: Instant,
        now_utc: DateTime<Utc>,
    ) -> usize {
        let mut rows = Vec::new();
        let mut slice_rows = Vec::new();
        for item in watched {
            let id = &item.position.id;
            let mut sample = match sample_with_books(item, books, catalog, now, now_utc) {
                Ok((sample, long_book, short_book)) => {
                    self.slicer.observe(
                        item,
                        &sample,
                        &long_book,
                        &short_book,
                        now,
                        now_utc,
                        &mut slice_rows,
                    );
                    sample
                }
                Err(why) => {
                    let entry = self
                        .unavailable
                        .entry(id.clone())
                        .or_insert_with(|| Unavailable::new(&why, now));
                    if entry.why != why {
                        *entry = Unavailable::new(&why, now);
                    }
                    if !entry.warned && now.saturating_duration_since(entry.since) >= WARN_AFTER {
                        entry.warned = true;
                        warn!(position = %id, symbol = %item.position.symbol, "分批平仓测算算不了（已持续 {} 秒）：{why}", now.saturating_duration_since(entry.since).as_secs());
                    }
                    // 算不了的一次不能夹在两次「到止盈」中间还算「连续」：断线前一次、恢复后一次不是连续两次。
                    self.streak.remove(id);
                    continue;
                }
            };
            self.unavailable.remove(id);
            let streak = self.streak.entry(id.clone()).or_insert(0);
            *streak = if sample.at_take_profit() {
                *streak + 1
            } else {
                0
            };
            if *streak >= TRIGGER_SAMPLES {
                sample.would_close = true;
                let due = self
                    .trigger_fired
                    .get(id)
                    .is_none_or(|at| now.saturating_duration_since(*at) >= TRIGGER_COOLDOWN);
                if due {
                    self.trigger_fired.insert(id.clone(), now);
                    self.fired.push(id.clone());
                }
                let logged = self.trigger_logged.get(id);
                if logged.is_none_or(|at| now.saturating_duration_since(*at) >= TRIGGER_LOG_EVERY) {
                    self.trigger_logged.insert(id.clone(), now);
                    let whole = sample
                        .ladder
                        .last()
                        .map(|rung| sample.net_with_funding(rung));
                    tracing::info!(position = %id, symbol = %item.position.symbol,
                        net_usdt = ?whole.map(|net| net.round_dp(4)),
                        take_profit_usdt = ?sample.take_profit_usdt,
                        long_age_ms = sample.long_age_ms, short_age_ms = sample.short_age_ms,
                        "盘口触发止盈：连续 {TRIGGER_SAMPLES} 次新鲜盘口整笔平仓都到止盈，交给交易台现拉盘口核对");
                }
            }
            let due = match self.last.get(id) {
                None => true,
                Some((at, was_positive)) => {
                    sample.positive
                        || *was_positive
                        || now.saturating_duration_since(*at) >= BASELINE_EVERY
                }
            };
            if due {
                self.last.insert(id.clone(), (now, sample.positive));
                rows.push(sample);
            }
        }
        // 平掉了的不再盯。
        self.unavailable
            .retain(|id, _| watched.iter().any(|item| &item.position.id == id));
        self.last
            .retain(|id, _| watched.iter().any(|item| &item.position.id == id));
        self.streak
            .retain(|id, _| watched.iter().any(|item| &item.position.id == id));
        self.trigger_logged
            .retain(|id, _| watched.iter().any(|item| &item.position.id == id));
        self.trigger_fired
            .retain(|id, _| watched.iter().any(|item| &item.position.id == id));
        self.slicer.finish(watched, now_utc, &mut slice_rows);
        if rows.is_empty() && slice_rows.is_empty() {
            return 0;
        }
        let prune = self.pruned_on != Some(now_utc.date_naive());
        if prune {
            self.pruned_on = Some(now_utc.date_naive());
        }
        self.writes.retain(|write| !write.is_finished());
        let count = rows.len();
        let dir = self.dir.clone();
        // 一个接一个写：各自起任务并发追加时，后采的行可能先落盘（测试偶发看到行序颠倒）。
        let previous = self.writes.pop();
        self.writes.push(tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            if !rows.is_empty()
                && let Err(error) = append(&dir, FILE_PREFIX, &rows, now_utc, prune).await
            {
                warn!("分批平仓测算记录写不进去：{error}");
            }
            if !slice_rows.is_empty()
                && let Err(error) = append(
                    &dir,
                    super::exit_slice::SLICE_PREFIX,
                    &slice_rows,
                    now_utc,
                    prune,
                )
                .await
            {
                warn!("分批平仓影子记录写不进去：{error}");
            }
        }));
        count
    }

    /// 上次取过之后新到止盈、该交给交易台的持仓 id（[`TRIGGER_COOLDOWN`] 内同一笔只交一次）。
    pub fn take_fired(&mut self) -> Vec<String> {
        std::mem::take(&mut self.fired)
    }

    /// 等后台写盘都做完（停机、测试）。
    pub async fn settle(&mut self) {
        for write in std::mem::take(&mut self.writes) {
            let _ = write.await;
        }
    }
}

/// 本地盘口换成执行层的盘口（买盘从高到低、卖盘从低到高，量换成名义）。
fn order_book(venue: Venue, symbol: &arb_core::Symbol, book: &LocalBook) -> OrderBook {
    let level = |(price, size): (&Decimal, &Decimal)| Level {
        price: *price,
        notional_usdt: *price * *size,
    };
    OrderBook {
        venue,
        symbol: symbol.clone(),
        bids: book
            .bids
            .iter()
            .rev()
            .take(BOOK_LEVELS)
            .map(level)
            .collect(),
        asks: book.asks.iter().take(BOOK_LEVELS).map(level).collect(),
    }
}

/// 这条腿在价差监控里的盘口：同开仓的市场名，Arcus 剔掉价格带外的档位。
fn leg_book(
    venue: Venue,
    position: &PairPosition,
    books: &HashMap<(Venue, String), LocalBook>,
    catalog: &Catalog,
) -> Result<LocalBook, String> {
    let base = &position.symbol.base;
    let market = catalog
        .get(&venue)
        .and_then(|markets| markets.get(base))
        .ok_or_else(|| format!("价差监控没有 {venue} 的 {base}"))?;
    let book = books
        .get(&(venue, market.key.clone()))
        .ok_or_else(|| format!("{venue} {} 的盘口还没收到", market.key))?;
    Ok(market
        .bounds
        .and_then(|bounds| book.within(bounds))
        .unwrap_or_else(|| book.clone()))
}

/// 梯子的断点：两条腿各自盘口每一档的累计数量占这条腿数量的比例（只取小于 1 的），由小到大，最多
/// [`MAX_RUNGS`] 个，最后补上整笔。
fn breakpoints(
    long_book: &LocalBook,
    long_qty: Decimal,
    short_book: &LocalBook,
    short_qty: Decimal,
) -> Vec<Decimal> {
    let mut points = Vec::new();
    let mut cumulative = |sizes: Vec<Decimal>, quantity: Decimal| {
        let mut total = Decimal::ZERO;
        for size in sizes {
            total += size;
            let fraction = total / quantity;
            if fraction >= Decimal::ONE {
                break;
            }
            points.push(fraction.round_dp(6));
        }
    };
    cumulative(long_book.bids.values().rev().copied().collect(), long_qty);
    cumulative(short_book.asks.values().copied().collect(), short_qty);
    points.retain(|fraction| *fraction > Decimal::ZERO);
    points.sort();
    points.dedup();
    points.truncate(MAX_RUNGS);
    points.push(Decimal::ONE);
    points
}

#[cfg(test)]
fn sample_of(
    item: &Watched,
    books: &HashMap<(Venue, String), LocalBook>,
    catalog: &Catalog,
    now: Instant,
    now_utc: DateTime<Utc>,
) -> Result<Sample, String> {
    sample_with_books(item, books, catalog, now, now_utc).map(|(sample, _, _)| sample)
}

/// 一次采样，连同算它用的两腿盘口（已换成执行层的格式，Arcus 剔过价格带外的档位）。
fn sample_with_books(
    item: &Watched,
    books: &HashMap<(Venue, String), LocalBook>,
    catalog: &Catalog,
    now: Instant,
    now_utc: DateTime<Utc>,
) -> Result<(Sample, OrderBook, OrderBook), String> {
    let position = &item.position;
    let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
    else {
        return Err("不是完整的双腿持仓".into());
    };
    // 数量为 0（名义被减到 0、台账异常）也挡掉：下面按数量算梯子的比例，Decimal 除以 0 会 panic，整个价差监控
    // （信号、盘口触发止盈）跟着停。
    let (Some(long_qty), Some(short_qty)) = (
        long_leg.quantity().filter(|q| *q > Decimal::ZERO),
        short_leg.quantity().filter(|q| *q > Decimal::ZERO),
    ) else {
        return Err("入场价非正或数量为 0，算不出梯子".into());
    };
    let long_local = leg_book(long_leg.venue, position, books, catalog)?;
    let short_local = leg_book(short_leg.venue, position, books, catalog)?;
    let long_book = order_book(long_leg.venue, &position.symbol, &long_local);
    let short_book = order_book(short_leg.venue, &position.symbol, &short_local);
    let mut ladder = Vec::new();
    for fraction in breakpoints(&long_local, long_qty, &short_local, short_qty) {
        // 深度不够的那一档（多半是整笔）没有报价：梯子停在能报价的最后一档。
        let Ok(quote) = exit_quote_part(position, &long_book, &short_book, item.fees, fraction)
        else {
            break;
        };
        ladder.push(Rung {
            fraction,
            notional_usdt: (long_leg.notional_usdt * fraction).round_dp(4),
            net_usdt: quote.net_usdt.round_dp(6),
            depth_usdt: quote.cost.map(|cost| cost.depth_usdt.round_dp(6)),
        });
    }
    if ladder.is_empty() {
        return Err("两边盘口连第一档都报不出价".into());
    }
    let age = |book: &LocalBook| now.saturating_duration_since(book.updated).as_millis() as u64;
    let mut sample = Sample {
        at: now_utc,
        position: position.id.clone(),
        symbol: position.symbol.to_string(),
        long: long_leg.venue,
        short: short_leg.venue,
        notional_usdt: long_leg.notional_usdt.round_dp(4),
        take_profit_usdt: position.rules.take_profit_usdt,
        funding_usdt: item.funding_usdt.map(|funding| funding.round_dp(6)),
        long_age_ms: age(&long_local),
        short_age_ms: age(&short_local),
        positive: false,
        ladder,
        would_close: false,
    };
    sample.positive = sample
        .ladder
        .iter()
        .any(|rung| sample.net_with_funding(rung) > Decimal::ZERO);
    Ok((sample, long_book, short_book))
}

async fn append<T: Serialize>(
    dir: &Path,
    prefix: &str,
    rows: &[T],
    now: DateTime<Utc>,
    prune: bool,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    tokio::fs::create_dir_all(dir).await?;
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(super::history::file_for(dir, prefix, now.date_naive()))
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    if !prune {
        return Ok(());
    }
    let oldest = super::history::file_for(
        dir,
        prefix,
        (now - chrono::Duration::days(KEEP_DAYS)).date_naive(),
    );
    let head = format!("{prefix}-");
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let dated = name
                .strip_prefix(&head)
                .and_then(|rest| rest.strip_suffix(".jsonl"))
                .is_some_and(|day| day.len() == 8 && day.bytes().all(|c| c.is_ascii_digit()));
            if dated && path < oldest {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
    Ok(())
}

/// 给规则轮用的提供者：每次调用返回最新一轮交过来的持仓。
pub type Source = Box<dyn Fn() -> Arc<Vec<Watched>> + Send + Sync>;

/// 盘口触发止盈交给谁（实盘交易台，见 [`Shadow::take_fired`]）：参数是持仓 id。要立刻返回（自己起任务）。
pub type Trigger = Box<dyn Fn(String) + Send + Sync>;

/// 持仓两条腿所在、但价差监控的订阅里没有的市场（场所 → 市场名）。组里不一定有持仓的合约：
/// 身份核对把它剔掉了（某次实盘：两家价差约 4%，重启后 `hyperliquid-io:lighter-rh` 组里没有它了），
/// 或者组换过。只算价差监控连着的场所（`connected`）、目录里查得到的合约。
pub fn missing_markets(
    watched: &[Watched],
    catalog: &Catalog,
    subscribed: &HashMap<Venue, Vec<String>>,
    connected: impl Fn(Venue) -> bool,
) -> BTreeMap<Venue, BTreeSet<String>> {
    let mut out: BTreeMap<Venue, BTreeSet<String>> = BTreeMap::new();
    for item in watched {
        let position = &item.position;
        for leg in [position.long.as_ref(), position.short.as_ref()]
            .into_iter()
            .flatten()
        {
            if !connected(leg.venue) {
                continue;
            }
            let Some(market) = catalog
                .get(&leg.venue)
                .and_then(|markets| markets.get(&position.symbol.base))
            else {
                continue;
            };
            let already = subscribed
                .get(&leg.venue)
                .is_some_and(|keys| keys.contains(&market.key));
            if !already {
                out.entry(leg.venue).or_default().insert(market.key.clone());
            }
        }
    }
    out
}

/// 订阅列表加上 [`missing_markets`]：原有的顺序不变，补的排在后面。
pub fn with_extra(
    subscribed: &HashMap<Venue, Vec<String>>,
    extra: &BTreeMap<Venue, BTreeSet<String>>,
) -> HashMap<Venue, Vec<String>> {
    let mut out = subscribed.clone();
    for (venue, keys) in extra {
        let list = out.entry(*venue).or_default();
        for key in keys {
            if !list.contains(key) {
                list.push(key.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {

    /// 只推变化的场所（Lighter）盘口几秒没变照样算新鲜；快照式（HL）超过 3 秒就不算。
    #[test]
    fn only_snapshot_feeds_need_a_recent_book_to_count_as_take_profit() {
        let sample = |long_age_ms: u64, short_age_ms: u64| Sample {
            at: Utc::now(),
            position: "p".into(),
            symbol: "SPY/USDT".into(),
            long: Venue::Lighter,
            // Arcus 没变也推整本快照（Hyperliquid 默认只订 bbo、只推变化，和 Lighter 一样不按年龄判）。
            short: Venue::Arcus,
            notional_usdt: Decimal::from(400),
            take_profit_usdt: Some(Decimal::new(2, 1)),
            funding_usdt: None,
            long_age_ms,
            short_age_ms,
            positive: true,
            ladder: vec![Rung {
                fraction: Decimal::ONE,
                notional_usdt: Decimal::from(400),
                net_usdt: Decimal::new(3, 1),
                depth_usdt: Some(Decimal::ZERO),
            }],
            would_close: false,
        };
        assert!(
            sample(10_000, 1_000).at_take_profit(),
            "Lighter 安静 10 秒照样算"
        );
        assert!(
            !sample(TRIGGER_FRESH_PUSH_MS + 1, 1_000).at_take_profit(),
            "太旧了也不算"
        );
        assert!(
            !sample(10_000, 4_000).at_take_profit(),
            "快照式盘口 4 秒没更新不算"
        );
        assert!(sample(0, TRIGGER_FRESH_MS).at_take_profit());
    }
    use super::*;
    use arb_core::{Side, Symbol};
    use arb_exec::{ClientOrderId, LegFill, PositionStatus, Strategy, TaskRules};
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    use super::super::pairs::VenueMarket;

    fn leg(venue: Venue, side: Side) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: dec!(1000),
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("p", side, 0),
            margin_usdt: Some(dec!(200)),
        }
    }

    fn position() -> PairPosition {
        PairPosition {
            margin_mode: arb_exec::MarginMode::Isolated,
            id: "p".into(),
            symbol: Symbol::perp("NBIS", "USDT"),
            strategy: Strategy::Spread,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: Some(leg(Venue::Arcus, Side::Sell)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules: TaskRules {
                take_profit_usdt: Some(dec!(0.25)),
                ..TaskRules::default()
            },
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        }
    }

    fn market(key: &str, bounds: Option<arb_venues::arcus::TradingBounds>) -> VenueMarket {
        VenueMarket {
            key: key.into(),
            taker_fee: Some(Decimal::ZERO),
            maker_fee: None,
            category: None,
            outside_rth: None,
            bounds,
            size_multiplier: None,
        }
    }

    fn catalog(arcus_bounds: Option<arb_venues::arcus::TradingBounds>) -> Catalog {
        HashMap::from([
            (
                Venue::Lighter,
                BTreeMap::from([("NBIS".to_string(), market("77", None))]),
            ),
            (
                Venue::Arcus,
                BTreeMap::from([("NBIS".to_string(), market("NBIS-USD", arcus_bounds))]),
            ),
        ])
    }

    fn local(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)], at: Instant) -> LocalBook {
        let mut book = LocalBook::new(at);
        for (price, size) in bids {
            book.bids.insert(*price, *size);
        }
        for (price, size) in asks {
            book.asks.insert(*price, *size);
        }
        book
    }

    /// 多腿（Lighter）买盘第一档只有 3 个（共 10 个），后面一档差很多；空腿（Arcus）卖盘很深。
    fn thin_books(now: Instant) -> HashMap<(Venue, String), LocalBook> {
        HashMap::from([
            (
                (Venue::Lighter, "77".to_string()),
                local(
                    &[(dec!(100.2), dec!(3)), (dec!(99), dec!(1000))],
                    &[(dec!(100.3), dec!(1000))],
                    now,
                ),
            ),
            (
                (Venue::Arcus, "NBIS-USD".to_string()),
                local(
                    &[(dec!(100), dec!(1000))],
                    &[(dec!(100.1), dec!(1000))],
                    now,
                ),
            ),
        ])
    }

    fn watched(funding: Option<Decimal>) -> Vec<Watched> {
        vec![Watched {
            position: position(),
            fees: ExitFees {
                long: Some(Decimal::ZERO),
                short: Some(Decimal::ZERO),
            },
            funding_usdt: funding,
        }]
    }

    /// 梯子：第一档够的那一部分赚、整笔要吃到 99 那一档亏；为正就记，算法与执行层整笔平仓一致。
    #[test]
    fn the_ladder_shows_a_profitable_first_level_on_a_thin_book() {
        let now = Instant::now();
        let books = thin_books(now);
        let sample = sample_of(&watched(None)[0], &books, &catalog(None), now, Utc::now()).unwrap();
        let fractions: Vec<Decimal> = sample.ladder.iter().map(|rung| rung.fraction).collect();
        assert_eq!(fractions, vec![dec!(0.3), Decimal::ONE]);
        let first = &sample.ladder[0];
        // 3 个：(100.2 − 100) × 3 + (100 − 100.1) × 3 = 0.3，不吃深度。
        assert_eq!(first.net_usdt, dec!(0.3));
        assert_eq!(first.notional_usdt, dec!(300));
        assert_eq!(first.depth_usdt, Some(Decimal::ZERO));
        let whole = sample.ladder.last().unwrap();
        assert!(whole.net_usdt < Decimal::ZERO, "{}", whole.net_usdt);
        assert!(whole.depth_usdt.unwrap() > Decimal::ZERO);
        assert!(sample.positive);
        assert_eq!(sample.take_profit_usdt, Some(dec!(0.25)));
        // 与执行层的整笔平仓估算同一个数。
        let exact = arb_exec::monitor::exit_quote(
            &position(),
            &order_book(
                Venue::Lighter,
                &position().symbol,
                &books[&(Venue::Lighter, "77".to_string())],
            ),
            &order_book(
                Venue::Arcus,
                &position().symbol,
                &books[&(Venue::Arcus, "NBIS-USD".to_string())],
            ),
            watched(None)[0].fees,
        )
        .unwrap();
        assert_eq!(whole.net_usdt, exact.net_usdt.round_dp(6));
    }

    /// 资金费按比例摊进每一档：付出去的资金费能把唯一为正的那一档压成负的。
    #[test]
    fn funding_is_prorated_into_each_rung() {
        let now = Instant::now();
        let sample = sample_of(
            &watched(Some(dec!(-1.5)))[0],
            &thin_books(now),
            &catalog(None),
            now,
            Utc::now(),
        )
        .unwrap();
        // 第一档 0.3 − 1.5 × 0.3 = −0.15。
        assert_eq!(sample.net_with_funding(&sample.ladder[0]), dec!(-0.15));
        assert!(!sample.positive);
    }

    /// Arcus 价格带外的档位成交不了，不算进梯子（与开仓同一份过滤）。
    #[test]
    fn arcus_levels_outside_the_trading_band_are_ignored() {
        let now = Instant::now();
        let mut books = thin_books(now);
        // 带外有一档很便宜的卖盘：算进去的话空腿买回价就是 99。
        books
            .get_mut(&(Venue::Arcus, "NBIS-USD".to_string()))
            .unwrap()
            .asks
            .insert(dec!(99), dec!(1000));
        let bounds = arb_venues::arcus::TradingBounds {
            lower: dec!(99.5),
            upper: dec!(101),
        };
        let banded = sample_of(
            &watched(None)[0],
            &books,
            &catalog(Some(bounds)),
            now,
            Utc::now(),
        )
        .unwrap();
        assert_eq!(banded.ladder[0].net_usdt, dec!(0.3), "带外那一档不能用");
        let unbanded =
            sample_of(&watched(None)[0], &books, &catalog(None), now, Utc::now()).unwrap();
        assert!(unbanded.ladder[0].net_usdt > dec!(0.3));
    }

    /// 持仓所在、但组里没有的市场要补订阅：只补没订的、只补价差监控连着的场所；原有的订阅顺序不变。
    #[test]
    fn markets_of_held_positions_are_added_to_the_subscriptions() {
        let watched = watched(None);
        let catalog = catalog(None);
        let subscribed = HashMap::from([
            (Venue::Lighter, vec!["5".to_string(), "77".to_string()]),
            (Venue::Arcus, vec!["SPY-USD".to_string()]),
        ]);
        let missing = missing_markets(&watched, &catalog, &subscribed, |_| true);
        assert_eq!(
            missing,
            BTreeMap::from([(Venue::Arcus, BTreeSet::from(["NBIS-USD".to_string()]))])
        );
        let merged = with_extra(&subscribed, &missing);
        assert_eq!(merged[&Venue::Lighter], subscribed[&Venue::Lighter]);
        assert_eq!(
            merged[&Venue::Arcus],
            vec!["SPY-USD".to_string(), "NBIS-USD".to_string()]
        );
        // 价差监控没连 Arcus：不补。
        assert!(
            missing_markets(&watched, &catalog, &subscribed, |venue| venue
                != Venue::Arcus)
            .is_empty()
        );
        // 都订了：不补，列表原样。
        assert!(missing_markets(&watched, &catalog, &merged, |_| true).is_empty());
        assert_eq!(with_extra(&merged, &BTreeMap::new()), merged);
    }

    /// 为正时每秒记；转为不正时补记一行；之后只按基线间隔记。盘口找不到不记、不报错。
    #[tokio::test]
    async fn rows_are_written_while_positive_once_after_and_then_as_a_baseline() {
        let dir = std::env::temp_dir().join(format!("arb-exit-shadow-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let mut shadow = Shadow::new(&dir);
        let start = Instant::now();
        let positive = watched(None);
        let negative = watched(Some(dec!(-1.5)));
        let at = |secs: u64| start + Duration::from_secs(secs);
        let books = thin_books(start);
        let utc = Utc::now();
        assert_eq!(
            shadow.sample(&positive, &books, &catalog(None), at(0), utc),
            1
        );
        assert_eq!(
            shadow.sample(&positive, &books, &catalog(None), at(1), utc),
            1
        );
        // 转为不正：补记这一行。
        assert_eq!(
            shadow.sample(&negative, &books, &catalog(None), at(2), utc),
            1
        );
        // 之后不正：按基线间隔。
        assert_eq!(
            shadow.sample(&negative, &books, &catalog(None), at(3), utc),
            0
        );
        assert_eq!(
            shadow.sample(&negative, &books, &catalog(None), at(62), utc),
            1
        );
        // 盘口不在（断线）：跳过。
        assert_eq!(
            shadow.sample(&positive, &HashMap::new(), &catalog(None), at(63), utc),
            0
        );
        // 平掉了：不再盯。
        assert_eq!(shadow.sample(&[], &books, &catalog(None), at(64), utc), 0);
        assert!(shadow.last.is_empty());
        shadow.settle().await;
        let text = tokio::fs::read_to_string(super::super::history::file_for(
            &dir,
            FILE_PREFIX,
            utc.date_naive(),
        ))
        .await
        .unwrap();
        let rows: Vec<Sample> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows.iter().filter(|row| row.positive).count(), 2);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// 盘口触发止盈：连续 2 次到止盈时交给交易台；之后 [`TRIGGER_COOLDOWN`] 内一直到止盈也不再交，过了再交；
    /// 平掉了的清掉。
    #[tokio::test]
    async fn a_book_at_take_profit_is_handed_to_the_desk_at_most_once_per_cooldown() {
        let dir =
            std::env::temp_dir().join(format!("arb-exit-shadow-fired-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let mut shadow = Shadow::new(&dir);
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let utc = Utc::now();
        let rich = watched(Some(dec!(10)));
        let id = rich[0].position.id.clone();
        let books = thin_books(start);
        shadow.sample(&rich, &books, &catalog(None), at(0), utc);
        assert!(shadow.take_fired().is_empty(), "第一次到止盈，还不交");
        shadow.sample(&rich, &books, &catalog(None), at(1000), utc);
        assert_eq!(shadow.take_fired(), vec![id.clone()]);
        assert!(shadow.take_fired().is_empty(), "取走了就没了");
        shadow.sample(&rich, &books, &catalog(None), at(2000), utc);
        assert!(shadow.take_fired().is_empty(), "冷却中");
        let later = thin_books(at(21_000));
        shadow.sample(&rich, &later, &catalog(None), at(21_500), utc);
        assert_eq!(shadow.take_fired(), vec![id], "冷却过了再交");
        shadow.sample(&[], &later, &catalog(None), at(22_000), utc);
        assert!(shadow.trigger_fired.is_empty(), "平掉了的清掉");
        shadow.settle().await;
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// 盘口触发平仓测算：连续 2 次新鲜盘口整笔（含资金费）都到止盈，第二次那一行标 `would_close`；
    /// 盘口变旧、没到止盈都重新计数。
    #[tokio::test]
    async fn two_fresh_samples_at_take_profit_mark_a_would_close_row() {
        let dir =
            std::env::temp_dir().join(format!("arb-exit-shadow-trigger-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let mut shadow = Shadow::new(&dir);
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let books = thin_books(start);
        let utc = Utc::now();
        // 资金费 +10 把整笔垫到止盈 0.25 以上；−10 压到止盈以下。
        let rich = watched(Some(dec!(10)));
        let poor = watched(Some(dec!(-10)));
        let id = rich[0].position.id.clone();
        shadow.sample(&rich, &books, &catalog(None), at(0), utc);
        assert_eq!(shadow.streak[&id], 1, "第一次到止盈，还不算");
        shadow.sample(&rich, &books, &catalog(None), at(1000), utc);
        assert_eq!(shadow.streak[&id], 2);
        shadow.sample(&poor, &books, &catalog(None), at(2000), utc);
        assert_eq!(shadow.streak[&id], 0, "没到止盈：重新计数");
        // 盘口超过 3 秒没更新：不认。
        shadow.sample(&rich, &books, &catalog(None), at(3500), utc);
        assert_eq!(shadow.streak[&id], 0, "盘口旧了");
        shadow.sample(&[], &books, &catalog(None), at(4000), utc);
        assert!(
            shadow.streak.is_empty() && shadow.trigger_logged.is_empty(),
            "平掉了的不再盯"
        );
        shadow.settle().await;
        let text = tokio::fs::read_to_string(super::super::history::file_for(
            &dir,
            FILE_PREFIX,
            utc.date_naive(),
        ))
        .await
        .unwrap();
        let flags: Vec<bool> = text
            .lines()
            .map(|line| serde_json::from_str::<Sample>(line).unwrap().would_close)
            .collect();
        assert_eq!(flags, vec![false, true, false, false]);
        // 没触发的行不写这个字段（旧记录照样读得进来）。
        assert_eq!(text.matches("would_close").count(), 1);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    /// 盘口短暂缺一两秒（重连）不警告；持续 30 秒才警告一次；回来之后重新计时。
    #[tokio::test]
    async fn a_missing_book_is_warned_about_only_after_it_has_stayed_missing() {
        let dir = std::env::temp_dir().join(format!("arb-exit-shadow-gap-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let mut shadow = Shadow::new(&dir);
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        let positive = watched(None);
        let id = positive[0].position.id.clone();
        let books = thin_books(start);
        let utc = Utc::now();
        let none = HashMap::new();
        shadow.sample(&positive, &none, &catalog(None), at(0), utc);
        shadow.sample(&positive, &none, &catalog(None), at(29), utc);
        assert!(!shadow.unavailable[&id].warned, "缺了 29 秒");
        shadow.sample(&positive, &none, &catalog(None), at(31), utc);
        assert!(shadow.unavailable[&id].warned, "缺了 31 秒");
        shadow.sample(&positive, &books, &catalog(None), at(32), utc);
        assert!(shadow.unavailable.is_empty(), "盘口回来就清掉");
        shadow.sample(&positive, &none, &catalog(None), at(33), utc);
        assert!(!shadow.unavailable[&id].warned, "回来过之后重新计时");
        shadow.sample(&[], &books, &catalog(None), at(34), utc);
        assert!(shadow.unavailable.is_empty(), "平掉了的不再盯");
        shadow.settle().await;
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
