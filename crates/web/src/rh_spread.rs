//! 跨场所同名合约价差监控（只读，不下单；自动交易见 `rh_auto`）。
//!
//! 最早只做 Lighter RH ↔ Arcus（都在 Robinhood Chain 上、都用 USDG 保证金）；现在按**组**监控，
//! 支持 Arcus、Lighter RH、Hyperliquid 主 dex 与 HIP-3 xyz / io 任意两家（`ARB_RH_SPREAD_PAIRS`）。
//! 价格偶尔会偏离：一边比另一边贵出手续费加穿价以上。这里：
//!
//! 1. 用各家的行情 WebSocket 维护本地盘口（REST 轮询会打穿 Lighter RH 的 IP 限频，
//!    并挤占实盘下单用的额度）；每家只连一条，几组共用；
//! 2. 每秒按「这笔名义吃完深度的均价」算两个方向的可成交价差，扣掉两家各开平一次的吃单费
//!    和立即平仓的穿价；
//! 3. 每分钟落盘一行中间价基差（每组一个文件前缀），按合约、按时段（盘中 / 盘后 / 周末 / 加密币全天）
//!    统计「正常水平」。股票类合约在休市时常常有**系统性**偏差：现价差大不等于会收敛到 0；
//! 4. 偏离正常水平、且「回到正常水平」的预估净收益超过门槛时推送 Telegram（同一组同一合约同一方向 30 分钟一条）。
//! 5. Binance 的组 2026-11-01 起和 Bitget / MEXC 一样是普通交易组（提醒、自动交易走通用规则、两腿吃单）；
//!    另外照常做挂单测算（[`maker`]，只是参考）。[`pairs::OBSERVE_ONLY`] 现在是空的。
//!
//! 基差一律是 **(a − b) / 均值**，方向叫 `long_a` / `long_b`（最早那组 a = Arcus、b = Lighter RH，与旧历史同号）。

mod book;
mod catalog;
pub mod exit_shadow;
pub mod exit_slice;
mod feed;
mod history;
pub mod hl_shadow;
pub mod latency;
#[cfg(test)]
pub use history::RecentEntry;
pub mod maker;
pub mod pairs;
mod session;
pub mod tape;

#[cfg(test)]
mod latency_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::alert::Alerter;
use arb_core::Venue;
pub use book::{DirectionQuote, LocalBook, mid_basis_pct, quote};
pub use history::{Normal, Row};
use pairs::{Catalog, Pair, PairMarket};
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
    /// 监控哪几组（`ARB_RH_SPREAD_PAIRS`）。
    pub pairs: Vec<Pair>,
    /// 只观察的组（`ARB_RH_SPREAD_WATCH` 的场所 × 交易组里的 [`pairs::WATCH_PEERS`]）：不提醒、不交易，
    /// 行单独存放（见 [`Monitor::panel_view`]）。
    pub watch_pairs: Vec<Pair>,
    /// Entropy（`hyperliquid-io`）自返佣（`ARB_ENTROPY_SELF_REBATE`，2 = 200%）。
    pub entropy_rebate: Decimal,
    /// Read-only fast/BBO A/B pool; never feeds live books.
    pub hl_shadow: hl_shadow::Config,
    /// 币安挂单测算（每行的挂单估算 + 虚拟成交影子 + 汇总）。`ARB_RH_MAKER_SHADOW=off` 关掉：币安已改成
    /// 两腿吃单后只剩统计用途，却占监控约一成的计算。关掉后自动交易的「币安挂单」候选也不会再出现。
    pub maker_shadow: bool,
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
        let pairs = match var("ARB_RH_SPREAD_PAIRS") {
            Some(raw) => Pair::parse_list(&raw)
                .map_err(|error| anyhow::anyhow!("ARB_RH_SPREAD_PAIRS：{error}"))?,
            // 没指定：用户配了 API 的场所（与实盘同一套凭据识别，不联网）两两组合。
            None => default_pairs(),
        };
        let watch = parse_watch(var("ARB_RH_SPREAD_WATCH").as_deref())
            .map_err(|error| anyhow::anyhow!("ARB_RH_SPREAD_WATCH：{error}"))?;
        let enabled = !matches!(
            var("ARB_RH_SPREAD")
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("off" | "0" | "false")
        );
        Ok(Self {
            enabled,
            hl_shadow: hl_shadow::Config::from_env()?,
            maker_shadow: !matches!(
                var("ARB_RH_MAKER_SHADOW")
                    .as_deref()
                    .map(str::to_ascii_lowercase)
                    .as_deref(),
                Some("off" | "0" | "false")
            ),
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
            watch_pairs: pairs::watch_pairs(&pairs, &watch),
            pairs,
            entropy_rebate: arb_venues::hyperliquid::entropy_self_rebate_from_env()
                .map_err(|error| anyhow::anyhow!("{error}"))?,
        })
    }
}

/// `ARB_RH_SPREAD_WATCH`：逗号分隔的场所；没设 = [`pairs::DEFAULT_WATCH`]；`off` / `none` = 不观察。
fn parse_watch(raw: Option<&str>) -> Result<Vec<Venue>, String> {
    let Some(raw) = raw else {
        return Ok(pairs::DEFAULT_WATCH.to_vec());
    };
    if matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "off" | "none" | "0" | "false"
    ) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for name in raw.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let venue = Venue::parse(name)
            .filter(|v| pairs::SUPPORTED.contains(v))
            .ok_or_else(|| format!("价差监控不支持场所 {name:?}"))?;
        if !out.contains(&venue) {
            out.push(venue);
        }
    }
    Ok(out)
}

impl Config {
    /// 交易组在前、观察组在后。
    pub fn all_pairs(&self) -> impl Iterator<Item = Pair> + '_ {
        self.pairs.iter().chain(self.watch_pairs.iter()).copied()
    }

    pub fn is_watch(&self, pair: Pair) -> bool {
        self.watch_pairs.contains(&pair)
    }
}

/// 默认的组：按凭据识别出的实盘场所（`ARB_LIVE_VENUES` 或自动识别）里价差监控支持的，两两组合。
/// 识别不出两家时退回 [`pairs::DEFAULT_PAIRS`]。
fn default_pairs() -> Vec<Pair> {
    let detected = arb_exec::live_connect::live_venues(None)
        .map(|selection| pairs::all_pairs(&selection.venues))
        .unwrap_or_default();
    if detected.is_empty() {
        Pair::parse_list(pairs::DEFAULT_PAIRS).expect("默认组合法")
    } else {
        detected
    }
}

/// 页面上一行：一组里一个合约此刻的状况。
#[derive(Debug, Clone, Serialize)]
pub struct Line {
    /// 组标识（`arcus:lighter-rh`）。
    pub pair: String,
    /// 基差的被减数 / 减数：(a − b)。
    pub a: Venue,
    pub b: Venue,
    pub base: String,
    pub category: String,
    pub session: Session,
    /// 往返两腿手续费（%）。
    pub fee_round_trip_pct: Option<Decimal>,
    /// 中间价基差（%）：(a − b) / 均值。
    pub basis_pct: Option<Decimal>,
    pub normal: Option<Normal>,
    /// 正常水平还差多少分钟样本（够了为 0）。
    pub normal_missing_minutes: usize,
    /// 偏离正常水平几个 MAD。
    pub z: Option<f64>,
    /// 两个方向的报价与净收益：多 a 空 b / 多 b 空 a。
    pub long_a: Option<Leg>,
    pub long_b: Option<Leg>,
    /// 更好的那个方向。
    pub best: Option<Best>,
    /// 两家盘口里较旧的那本多久没更新（秒）。
    pub age_sec: Option<u64>,
    pub note: Option<String>,
    /// 只观察的组（Binance）：不提醒、不自动交易。
    pub observe_only: bool,
    /// 观察组（还没有实盘账户的场所，见 [`pairs::DEFAULT_WATCH`]）：不提醒、不交易、不下单。
    #[serde(default)]
    pub watch_only: bool,
    /// 挂单测算（有挂单场所的组才有）。
    pub maker: Option<maker::MakerView>,
    /// 最近 [`tape::MOVE_WINDOW`] 两家中间价各自的波动幅度。`None` = 还没记满窗口（刚启动 / 刚重连），
    /// 自动交易对它是放行，见 [`tape::PriceTape::range_pct`]。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_move: Option<RecentMove>,
    /// 近 [`RECENT_ENTRY_HOURS`] 小时两个方向可成交价差的中位数（见 [`history::RecentEntry`]）。自动交易拿它判断
    /// 「现在的价差是不是相对近期也偏高」，而不只是相对 7 天的正常水平（价差整体换了水平时后者会滞后几天）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recent_entry: Option<history::RecentEntry>,
}

/// 近期可成交价差回看几小时（自动交易的「相对近期偏离」门槛用）。回测按这个窗口定。
pub const RECENT_ENTRY_HOURS: i64 = 12;

/// 最近一个窗口里两家中间价的波动幅度（%）：（最高 − 最低）÷ 最低。a、b 与 [`Line`] 里的一致。
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct RecentMove {
    pub a_pct: Decimal,
    pub b_pct: Decimal,
}

impl RecentMove {
    /// 某一家（`line.a` 或 `line.b`）的幅度。
    pub fn of(&self, line: &Line, venue: Venue) -> Decimal {
        if venue == line.a {
            self.a_pct
        } else {
            self.b_pct
        }
    }

    /// 两家里动得大的那个：(场所, 幅度)。
    pub fn larger(&self, line: &Line) -> (Venue, Decimal) {
        if self.a_pct >= self.b_pct {
            (line.a, self.a_pct)
        } else {
            (line.b, self.b_pct)
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Leg {
    #[serde(flatten)]
    pub quote: DirectionQuote,
    /// 收敛到 0 时的净收益（%）：可成交价差 − 平仓穿价 − 往返手续费。
    pub net_to_zero_pct: Decimal,
    /// 回到正常水平时的净收益（%）。没有正常水平时为 `None`。自动交易扣过持仓期资金费的，见 `funding_cost_pct`。
    pub net_to_normal_pct: Option<Decimal>,
    /// 自动交易按持仓期估算、已经从 `net_to_normal_pct` 里扣掉的资金费（%）。监控本身不扣，为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub funding_cost_pct: Option<Decimal>,
    /// 单笔阶梯：只在有信号的那个方向上算（见 [`ladder`]），给自动交易按盘口挑单笔。其余为空。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ladder: Vec<Rung>,
}

/// 单笔阶梯的一档：按这个单笔吃完两边深度的可成交价差与净收益，口径与 [`Leg`] 相同。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Rung {
    pub size_usdt: Decimal,
    pub entry_pct: Decimal,
    pub net_to_zero_pct: Decimal,
    /// 回到正常水平的净收益（%）；自动交易扣资金费时每档同样扣。
    pub net_to_normal_pct: Option<Decimal>,
}

/// 单笔阶梯每档加多少（USDT）。
pub const LADDER_STEP_USDT: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
/// 单笔阶梯算到多大（USDT）：自动交易的动态单笔上限不能超过它。
pub const LADDER_MAX_USDT: Decimal = Decimal::from_parts(1000, 0, 0, false, 0);

/// 从 `from` 起每 [`LADDER_STEP_USDT`] 一档、到 [`LADDER_MAX_USDT`] 为止，按每档单笔估算「多 `long`、空 `short`」
/// 的可成交价差与净收益（`leg` 与监控本身那一档用同一个闭包）。某档深度不够就停在那里。
fn ladder(
    long: &LocalBook,
    short: &LocalBook,
    from: Decimal,
    leg: impl Fn(DirectionQuote) -> Leg,
) -> Vec<Rung> {
    let mut rungs = Vec::new();
    let mut size = from;
    while size <= LADDER_MAX_USDT {
        let Some(quote) = quote(long, short, size) else {
            break;
        };
        let leg = leg(quote);
        rungs.push(Rung {
            size_usdt: size,
            entry_pct: leg.quote.entry_pct,
            net_to_zero_pct: leg.net_to_zero_pct,
            net_to_normal_pct: leg.net_to_normal_pct,
        });
        size += LADDER_STEP_USDT;
    }
    rungs
}

impl Leg {
    /// 自动开仓的既有硬条件，与「回到正常」的偏离提醒不同。
    /// 提醒可以为正，但若入场价差或收敛到 0 的净收益非正，下单路径仍会拒绝。
    pub fn auto_entry_block_reason(&self) -> Option<String> {
        if self.quote.entry_pct <= Decimal::ZERO {
            Some(format!(
                "开仓可成交价差 {}% ≤ 0（空腿卖价必须高于多腿买价）",
                self.quote.entry_pct.normalize()
            ))
        } else if self.net_to_zero_pct <= Decimal::ZERO {
            Some(format!(
                "收敛到 0 净收益 {}% ≤ 0",
                self.net_to_zero_pct.normalize()
            ))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Best {
    /// `long_a` / `long_b`。
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
    pub min_minutes: usize,
    pub window_days: i64,
    /// 各组的概况（顺序 = 配置顺序）。
    pub pairs: Vec<PairView>,
    /// 各家行情连接。
    pub connected: Connected,
    pub updated_at: Option<DateTime<Utc>>,
    /// 各行共享着放（`Arc`）：监控每轮只换盘口变过的那些行，读快照的一方（自动交易每次检查都读）
    /// 拿到的是引用计数，不是两千行的深拷贝。
    pub lines: Vec<Arc<Line>>,
    pub error: Option<String>,
    /// 挂单测算的汇总（有挂单场所的组时才有）。
    pub maker: Option<Arc<maker::Summary>>,
}

/// 一组的概况。
#[derive(Debug, Clone, Serialize)]
pub struct PairView {
    pub id: String,
    pub a: Venue,
    pub b: Venue,
    /// 同名合约数（Hyperliquid 的组要等首轮扫描核过身份才有）。
    pub markets: usize,
    pub history_minutes: usize,
    /// 这组的往返手续费范围（%）：合约之间可能不同（growth mode）。
    pub fee_min_pct: Option<Decimal>,
    pub fee_max_pct: Option<Decimal>,
    /// 还没发现市场的原因（等扫描、某家市场列表取不到）。
    pub note: Option<String>,
    /// 只观察（不提醒、不自动交易）：Binance 的组只做挂单测算。
    pub observe_only: bool,
    /// 观察组（没有实盘账户的场所）。
    pub watch_only: bool,
}

/// 观察组的快照（整秒更新一次，只给面板）。自动交易、下单、提醒读的 [`View`] 里没有这些行，
/// 从结构上保证观察场所（没有实盘账户）的行不会被拿去下单。
#[derive(Debug, Clone, Default, Serialize)]
pub struct WatchView {
    pub pairs: Vec<PairView>,
    pub lines: Vec<Arc<Line>>,
}

/// 面板接口：交易用的快照，外加观察组。
#[derive(Debug, Clone, Serialize)]
pub struct PanelView {
    #[serde(flatten)]
    pub view: View,
    pub watch: WatchView,
    /// 行情分段延迟：只有整份（[`PanelFilter::full`]）才带，页面不用。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<latency::Report>,
    /// 筛选前后的行数（页面的「合约 / 当前信号」按它显示，不靠数返回的行）。
    pub counts: PanelCounts,
    /// 挂单测算里符合筛选的虚拟成交笔数（`maker.stats` 只返回前几条时页面靠它显示总数）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_fills: Option<usize>,
}

/// 面板接口的行数：`lines` / `signals` 是符合组、合约筛选的全部行（不受「只看信号」和行数上限影响），
/// `shown` 是这次真的返回了多少行。
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct PanelCounts {
    pub lines: usize,
    pub signals: usize,
    pub shown: usize,
}

/// 面板接口的筛选（`/api/rh-spread?pair=…&base=…&signal=1&watch=1&limit=100`）。
///
/// 整份快照有两千多行、2.5MB：页面每 3 秒取一次、整表重画，浏览器和 2 核的服务器都吃不消。页面只要
/// 「净收益最高的前 N 行」或某一组、某个合约的行，所以在读锁里先筛、只克隆要返回的那些。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PanelFilter {
    /// 只要这一组（`arcus:lighter-rh`）。
    pub pair: Option<String>,
    /// 只要这个合约（不分大小写），各组都算。
    pub base: Option<String>,
    /// 只要有信号的行（只观察、观察组不算信号）。
    pub signal_only: bool,
    /// 带上观察组的行（观察组的概况总是带，很小）。
    pub watch: bool,
    /// 最多返回多少行（先交易组后观察组，各自按净收益从高到低）。`None` = 不限。
    pub limit: Option<usize>,
    /// 整份：所有行、全部挂单测算统计、行情分段延迟（脚本 / 排查用，就是不带参数时的老样子）。
    pub full: bool,
}

impl PanelFilter {
    /// 不带任何参数的请求：和以前一样返回整份。
    pub fn full() -> Self {
        Self {
            watch: true,
            full: true,
            ..Self::default()
        }
    }

    fn matches(&self, line: &Line) -> bool {
        self.pair.as_deref().is_none_or(|pair| line.pair == pair)
            && self
                .base
                .as_deref()
                .is_none_or(|base| line.base.eq_ignore_ascii_case(base))
    }
}

/// 页面上算不算信号：达到提醒条件，而且不是只观察 / 观察组。
fn panel_signal(line: &Line) -> bool {
    line.best.as_ref().is_some_and(|best| best.signal) && !line.observe_only && !line.watch_only
}

/// 页面上挂单测算的统计最多显示几条。
const PANEL_MAKER_STATS: usize = 15;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Connected {
    /// 场所 → 行情 WebSocket 是否连着。
    pub venues: std::collections::BTreeMap<Venue, bool>,
    pub reconnects: u64,
}

impl Connected {
    pub fn up(&self, venue: Venue) -> bool {
        self.venues.get(&venue).copied().unwrap_or(false)
    }
}

/// 快照式盘口（Arcus、Hyperliquid：盘口没变也推）超过这么久没更新就不算数（WebSocket 静默断流时不会报错）。
/// 连接级的空闲上限按它的两倍算（见 [`Protocol::idle`]）。逐个市场的判旧更严，见 [`book_stale_after`]。
const STALE: Duration = Duration::from_secs(15);

/// 快照式盘口逐个市场多久没更新就不算数（不出信号、不进自动交易）。2026-11-07 东京实测相邻两次更新的间隔：
/// Hyperliquid（含 xyz / io）p99 ≤ 1 秒、最长约 2 秒；Arcus p95 ≤ 2.5 秒。原来统一 15 秒：一家某个市场冻住
/// 十几秒，另一家照常动，就成了假价差（保持 0 秒时当场下单）。
pub fn book_stale_after(venue: Venue) -> Duration {
    match venue {
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => Duration::from_secs(5),
        Venue::Arcus => Duration::from_secs(6),
        _ => STALE,
    }
}
/// 信号要连续保持这么久才推送：一秒钟的闪烁（一档挂单被吃掉又补上）不值得叫人。
pub const SIGNAL_HOLD: Duration = Duration::from_secs(10);
/// 价差提醒自己的限频：每分钟最多这么多条。告警通道全局每分钟 6 条、与交易告警共用，
/// 一波行情同时亮十几个合约时不能把规则平仓、裸敞口这类告警挤掉。
pub const ALERTS_PER_MINUTE: usize = 2;

/// 这家的盘口是不是「没变也推」的快照：是的话它的年龄就是数据新鲜度。
/// Lighter 与 Binance 只推变化：安静的市场几秒没增量是常态，新鲜度靠连接活着保证（断线即清空盘口）。
pub fn snapshot_feed(venue: Venue) -> bool {
    snapshot_feed_in(venue, feed::hyperliquid_book_mode())
}

/// [`snapshot_feed`]，Hyperliquid 的盘口来源由调用方给（测试两种模式用）。
pub fn snapshot_feed_in(venue: Venue, hl_book: feed::HlBook) -> bool {
    // Binance、Aster 的部分盘口也只在有变化时推（周末冷门合约 12 秒 2 条）。观察场所（Gate、Bitget、Ondo、PopDEX）
    // 推的虽是整本，但休市、冷门时会停推，按「只推变化」对待，免得一直显示「没更新」。
    // Hyperliquid 只订 `bbo`（默认）时也只推变化：安静的市场长时间没有推送是常态，不能按年龄判过期，
    // 新鲜度靠连接心跳（2 秒一次 ping，6 秒没有任何消息就判死、重连、清空盘口）。`l2` 模式是整本快照。
    match venue {
        Venue::Arcus => true,
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            hl_book == feed::HlBook::L2
        }
        _ => false,
    }
}

/// 估算一组里一个合约此刻的状况。纯计算，单测覆盖。
#[allow(clippy::too_many_arguments)]
pub fn evaluate(
    pair: Pair,
    market: &PairMarket,
    a: Option<&LocalBook>,
    b: Option<&LocalBook>,
    normal: Option<Normal>,
    normal_minutes: usize,
    session: Session,
    config: &Config,
    now: Instant,
) -> Line {
    let fee = market.round_trip_pct();
    let mut line = Line {
        pair: pair.id(),
        a: pair.a,
        b: pair.b,
        base: market.base.clone(),
        category: market.category.clone(),
        session,
        fee_round_trip_pct: fee,
        basis_pct: None,
        normal_missing_minutes: history::MIN_MINUTES.saturating_sub(normal_minutes),
        normal: normal.clone(),
        z: None,
        long_a: None,
        long_b: None,
        best: None,
        age_sec: None,
        note: None,
        observe_only: pair.observe_only(),
        watch_only: config.is_watch(pair),
        maker: None,
        recent_move: None,
        recent_entry: None,
    };
    let (Some(book_a), Some(book_b)) = (a, b) else {
        line.note = Some("等待两家盘口".into());
        return line;
    };
    let mut oldest: Option<(Venue, Duration)> = None;
    let mut stale: Option<(Venue, Duration)> = None;
    for (venue, book) in [(pair.a, book_a), (pair.b, book_b)] {
        if snapshot_feed(venue) {
            let age = now.saturating_duration_since(book.updated);
            if oldest.is_none_or(|(_, o)| age > o) {
                oldest = Some((venue, age));
            }
            if age > book_stale_after(venue) && stale.is_none_or(|(_, o)| age > o) {
                stale = Some((venue, age));
            }
        }
    }
    if let Some((_, age)) = oldest {
        line.age_sec = Some(age.as_secs());
    }
    if let Some((venue, age)) = stale {
        line.note = Some(format!("{venue} 盘口 {} 秒没更新，不计算", age.as_secs()));
        return line;
    }
    let Some(basis) = mid_basis_pct(book_a, book_b) else {
        line.note = Some("至少一家盘口为空或交叉".into());
        return line;
    };
    line.basis_pct = Some(basis);
    let Some(fee) = fee else {
        line.note = Some("至少一家的吃单费率不知道，不估净收益".into());
        return line;
    };
    if let Some(n) = &normal
        && let Some(b) = basis.to_f64()
    {
        // MAD 太小（几乎不动的合约）时下限 0.005%，免得一点抖动就几十个 σ。
        line.z = Some(((b - n.median) / n.mad.max(0.005) * 1000.0).round() / 1000.0);
    }
    let normal_median = normal_median_of(normal.as_ref());
    // 基差照原盘口算（历史与「正常水平」不变）；能不能成交按剔掉成交价限制带之外档位的盘口算。
    let banded_a = market.a.bounds.and_then(|bounds| book_a.within(bounds));
    let banded_b = market.b.bounds.and_then(|bounds| book_b.within(bounds));
    let (book_a, book_b) = (
        banded_a.as_ref().unwrap_or(book_a),
        banded_b.as_ref().unwrap_or(book_b),
    );
    line.maker = config
        .maker_shadow
        .then(|| {
            maker::evaluate(
                pair,
                market,
                book_a,
                book_b,
                normal_median,
                config.size_usdt,
            )
        })
        .flatten();
    // 方向：多 a 空 b 赚的是「a 相对 b 变贵」（基差上升）；反方向赚基差下降。
    // 回到正常水平的净收益 = 可成交价差 − 平仓穿价 − 手续费 −/＋ 正常水平（正常基差本身是收不回来的那部分）。
    let leg = |quote: DirectionQuote, sign: Decimal| {
        let net_to_zero_pct = (quote.entry_pct - quote.exit_cross_pct - fee).round_dp(5);
        Leg {
            net_to_normal_pct: normal_median
                .map(|median| (net_to_zero_pct + sign * median).round_dp(5)),
            net_to_zero_pct,
            quote,
            funding_cost_pct: None,
            ladder: Vec::new(),
        }
    };
    // 多 a 空 b：入场价差 = b 卖 − a 买，正常基差 m = a − b；回到 m 时还剩 −m 收不回 → +m。
    if let Some((long_a, long_b)) = book::quote_both(book_a, book_b, config.size_usdt) {
        line.long_a = Some(leg(long_a, Decimal::ONE));
        line.long_b = Some(leg(long_b, -Decimal::ONE));
    }
    if line.long_a.is_none() && line.long_b.is_none() {
        let banded = [
            (pair.a, market.a.bounds, banded_a.is_some()),
            (pair.b, market.b.bounds, banded_b.is_some()),
        ]
        .into_iter()
        .find_map(|(venue, bounds, clipped)| bounds.filter(|_| clipped).map(|b| (venue, b)));
        line.note = Some(match banded {
            Some((venue, bounds)) => format!(
                "{venue} 只能在 {} ～ {} 之内成交（盘后价格带），带外的挂单成交不了",
                bounds.lower.normalize(),
                bounds.upper.normalize()
            ),
            None => format!("深度不够 {} USDT", config.size_usdt.normalize()),
        });
        return line;
    }
    let score = |leg: &Option<Leg>| {
        leg.as_ref()
            .map(|l| l.net_to_normal_pct.unwrap_or(l.net_to_zero_pct))
    };
    let (direction, chosen) = match (score(&line.long_a), score(&line.long_b)) {
        (Some(x), Some(y)) if y > x => ("long_b", &line.long_b),
        (Some(_), _) => ("long_a", &line.long_a),
        _ => ("long_b", &line.long_b),
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
    // 有信号的方向再按更大的单笔逐档算一遍（自动交易按盘口挑单笔用）；没信号的行不算，省 CPU 和接口体积。
    if signal {
        let (long, short, sign, slot) = if direction == "long_a" {
            (book_a, book_b, Decimal::ONE, &mut line.long_a)
        } else {
            (book_b, book_a, -Decimal::ONE, &mut line.long_b)
        };
        if let Some(chosen) = slot.as_mut() {
            chosen.ladder = ladder(long, short, config.size_usdt, |q| leg(q, sign));
        }
    }
    line
}

/// 一家场所的市场列表（取失败时是原因）。
type CatalogResult = Result<std::collections::BTreeMap<String, pairs::VenueMarket>, String>;

/// 合进新取到的市场列表：失败的那家保留上一次的。返回失败原因。
/// 订阅列表前后差了哪些市场（日志用）：`+新增 -去掉`，最多各列 8 个。
fn describe_change(old: &[String], new: &[String]) -> String {
    let list = |keys: Vec<&String>| {
        let shown: Vec<&str> = keys.iter().take(8).map(|key| key.as_str()).collect();
        let more = keys.len().saturating_sub(shown.len());
        format!(
            "{}{}",
            shown.join(","),
            if more > 0 {
                format!("…等{}个", keys.len())
            } else {
                String::new()
            }
        )
    };
    format!(
        "+[{}] -[{}]",
        list(new.iter().filter(|key| !old.contains(key)).collect()),
        list(old.iter().filter(|key| !new.contains(key)).collect()),
    )
}

fn merge_catalog(catalog: &mut Catalog, results: Vec<(Venue, CatalogResult)>) -> Vec<String> {
    let mut errors = Vec::new();
    for (venue, result) in results {
        match result {
            Ok(markets) => {
                catalog.insert(venue, markets);
            }
            Err(error) => {
                warn!(%venue, "价差监控：取市场列表失败：{error}");
                errors.push(format!("{venue}：{error}"));
            }
        }
    }
    errors
}

/// 这个合约两家中间价最近一个窗口的波动幅度；任何一家还没记满窗口就是 `None`。
fn recent_move(
    tape: &tape::PriceTape,
    pair: Pair,
    market: &PairMarket,
    now: Instant,
) -> (Option<RecentMove>, Option<Instant>) {
    let (a, a_until) = tape.range_until(pair.a, &market.a.key, tape::MOVE_WINDOW, now);
    let (b, b_until) = tape.range_until(pair.b, &market.b.key, tape::MOVE_WINDOW, now);
    let until = match (a_until, b_until) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    };
    (
        a.zip(b).map(|(a_pct, b_pct)| RecentMove { a_pct, b_pct }),
        until,
    )
}

/// 快路径里一行的槽位（`slots[组][合约]`，和 `states[组].markets` 一一对应）。
#[derive(Default, Clone)]
struct RowSlot {
    /// 上一次算出来的这一行。
    line: Option<Arc<Line>>,
    /// 两本盘口都没变时，这一行的「近 5 秒波动」到这个时刻之前不会变（见 [`tape::PriceTape::range_until`]）；
    /// `None` = 一直不变。
    recent_until: Option<Instant>,
    /// 上一轮之后这一行的两本盘口里有变过的。
    dirty: bool,
}

/// 盘口 →（组序号, 合约序号）：一本盘口变了要重算哪些行。只收交易组（观察组快路径不算）。
type RowIndex = HashMap<Venue, HashMap<String, Vec<(u32, u32)>>>;

fn row_index(states: &[PairState]) -> RowIndex {
    let mut index = RowIndex::new();
    for (group, state) in states.iter().enumerate().filter(|(_, s)| !s.watch) {
        for (row, market) in state.markets.iter().enumerate() {
            for (venue, key) in [(state.pair.a, &market.a.key), (state.pair.b, &market.b.key)] {
                index
                    .entry(venue)
                    .or_default()
                    .entry(key.clone())
                    .or_default()
                    .push((group as u32, row as u32));
            }
        }
    }
    index
}

/// 这组还在监控这个合约。
fn monitored(states: &[PairState], pair: &str, base: &str) -> bool {
    states
        .iter()
        .any(|state| state.pair.id() == pair && state.markets.iter().any(|m| m.base == base))
}

/// 「正常」基差（%，a − b）的十进制值。
fn normal_median_of(normal: Option<&Normal>) -> Option<Decimal> {
    normal
        .and_then(|n| Decimal::from_f64_retain(n.median))
        .map(|d| d.round_dp(5))
}

impl Line {
    /// 一个方向的两条腿：(多, 空)。
    pub fn legs(&self, direction: &str) -> Option<(Venue, Venue, &Leg)> {
        match direction {
            "long_a" => Some((self.a, self.b, self.long_a.as_ref()?)),
            "long_b" => Some((self.b, self.a, self.long_b.as_ref()?)),
            _ => None,
        }
    }
}

/// 一组一分钟内的采样（算中位数用）。
#[derive(Default)]
struct Minute {
    start: i64,
    basis: HashMap<String, Vec<f64>>,
    entry_a: HashMap<String, f64>,
    entry_b: HashMap<String, f64>,
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
                    ea: self.entry_a.get(base).copied(),
                    el: self.entry_b.get(base).copied(),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.s.cmp(&b.s));
        rows
    }
}

/// 从行情 WebSocket 读到的东西，送到计算任务。
enum Feed {
    Book(Venue, feed::Event, latency::Stamp),
    /// 某家连接状态变了。
    Up(Venue, bool),
    /// 这条连接上收到了一条不带行情的消息（Hyperliquid 的 `pong`、订阅回执）：连接还活着。
    /// `bbo` 只在顶档变化时推送，安静的市场长时间没有推送，新鲜度靠它判断（见 `arb_venues::live_book`）。
    Alive(Venue),
}

/// 一组的运行状态。
/// 一个（合约，时段）的正常水平与样本分钟数。
type NormalStat = (Option<Normal>, usize);

struct PairState {
    pair: Pair,
    markets: Vec<PairMarket>,
    history: history::History,
    minute: Minute,
    /// 正常水平只在每分钟落一行之后才会变：按（合约，时段）缓存，带算它时的分钟起点。换分钟后**不**一次清空，
    /// 每轮重算按 [`RefreshBudget`] 刷新一部分（见 [`cached_history`]）。
    normals: HashMap<(String, Session), (i64, NormalStat)>,
    /// 近期可成交价差同样每分钟才变：按合约缓存，刷新方式同上。
    recents: HashMap<String, (i64, Option<history::RecentEntry>)>,
    note: Option<String>,
    /// 观察组：只在整秒算（不走快路径），行放进 [`WatchView`]。
    watch: bool,
}

type ActiveBooks = HashMap<Venue, std::collections::HashSet<String>>;

fn active_books(states: &[PairState]) -> ActiveBooks {
    let mut active = ActiveBooks::new();
    for state in states.iter().filter(|s| !s.watch) {
        for market in &state.markets {
            active
                .entry(state.pair.a)
                .or_default()
                .insert(market.a.key.clone());
            active
                .entry(state.pair.b)
                .or_default()
                .insert(market.b.key.clone());
        }
    }
    active
}

pub struct Monitor {
    config: Config,
    view: RwLock<View>,
    /// 观察组：和 `view` 分开放（见 [`WatchView`]）。
    watch: RwLock<WatchView>,
    alerts: Arc<Alerter>,
    /// 最近一分钟发出的价差提醒时刻（[`ALERTS_PER_MINUTE`]）。
    sent: std::sync::Mutex<std::collections::VecDeque<Instant>>,
    /// 提醒下面「下单…」按钮的票据（见 [`crate::telegram::order`]）。
    tickets: std::sync::Mutex<OrderTickets>,
    /// 快照每更新一次就通知一次（自动交易据此立刻看一眼，不等下一个整秒）。
    changed: tokio::sync::Notify,
    /// 提醒里说明自动交易会不会下这一笔（[`Monitor::set_auto_note`]）。
    auto_note: std::sync::OnceLock<AutoNote>,
    /// 估算名义：跟着自动交易的单腿名义走（启动时和每次保存设置时 [`Monitor::set_size_usdt`]）；
    /// `config.size_usdt`（`ARB_RH_SPREAD_SIZE`）只剩挂单测算在用。
    size_usdt: std::sync::RwLock<Decimal>,
    /// 分批平仓测算要盯的持仓（[`Monitor::set_exit_source`]）。没设 = 不测。
    exit_source: std::sync::OnceLock<exit_shadow::Source>,
    /// 盘口触发止盈交给谁（[`Monitor::set_exit_trigger`]）。没设 = 只记录。
    exit_trigger: std::sync::OnceLock<exit_shadow::Trigger>,
}

/// 给一条要推送的提醒（行, 信号）写一句自动交易的结论；`None` = 不知道，用笼统的说法。
pub type AutoNote = Box<dyn Fn(&Line, &Best) -> Option<String> + Send + Sync>;

/// 盘口有变化时最快多久重算一次快照（不写历史、不推送提醒，那些仍按整秒做）。
/// 快路径只重算这段时间盘口变过的行（见主循环的 `slots`），没变的行共用上一轮那份，所以可以很短：
/// 2026-11-15 实测（27 组 1800 行）50ms → 10ms，「收到行情 → 快照发布」平均 76ms → 约 20ms，
/// CPU 反而比改之前低（65% → 60% 单核）。可用环境变量 `ARB_RH_FAST_RECOMPUTE_MS` 调（见 [`tuned_ms`]）。
const FAST_RECOMPUTE: Duration = Duration::from_millis(10);

/// 节拍类参数（毫秒）：环境变量里给了合法的数就用它（夹在 `min..=max` 里），否则用默认值。
/// 给线上不重新编译就能调快 / 调慢用；写错了不影响启动（记一条警告、用默认）。
pub(crate) fn tuned_ms(name: &str, default: Duration, min: u64, max: u64) -> Duration {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    match raw.trim().parse::<u64>() {
        Ok(ms) => Duration::from_millis(ms.clamp(min, max)),
        Err(_) => {
            if !raw.trim().is_empty() {
                warn!(
                    "{name}={raw:?} 不是毫秒数，用默认 {} 毫秒",
                    default.as_millis()
                );
            }
            default
        }
    }
}

type OrderTickets = crate::telegram::order::Tickets<crate::telegram::order::Intent>;

/// 订阅变化：每家场所要订阅的市场名（全量）。
type Subscriptions = HashMap<Venue, Vec<String>>;

impl Monitor {
    pub fn new(config: Config, alerts: Arc<Alerter>) -> Arc<Self> {
        let starting = |pair: &Pair| PairView {
            id: pair.id(),
            a: pair.a,
            b: pair.b,
            markets: 0,
            history_minutes: 0,
            fee_min_pct: None,
            fee_max_pct: None,
            note: Some("启动中".into()),
            observe_only: pair.observe_only(),
            watch_only: config.is_watch(*pair),
        };
        let watch = WatchView {
            pairs: config.watch_pairs.iter().map(starting).collect(),
            lines: Vec::new(),
        };
        let view = View {
            enabled: config.enabled,
            size_usdt: config.size_usdt,
            alert_net_pct: config.alert_net_pct,
            min_minutes: history::MIN_MINUTES,
            window_days: history::WINDOW_DAYS,
            pairs: config.pairs.iter().map(starting).collect(),
            connected: Connected::default(),
            updated_at: None,
            lines: Vec::new(),
            error: (!config.enabled).then(|| "已关闭（ARB_RH_SPREAD=off）".to_string()),
            maker: None,
        };
        Arc::new(Self {
            size_usdt: std::sync::RwLock::new(config.size_usdt),
            config,
            view: RwLock::new(view),
            watch: RwLock::new(watch),
            alerts,
            sent: std::sync::Mutex::new(std::collections::VecDeque::new()),
            tickets: std::sync::Mutex::new(OrderTickets::new(
                crate::telegram::order::ALERT_TICKET_TTL,
            )),
            changed: tokio::sync::Notify::new(),
            auto_note: std::sync::OnceLock::new(),
            exit_source: std::sync::OnceLock::new(),
            exit_trigger: std::sync::OnceLock::new(),
        })
    }

    /// 等快照下一次更新（盘口有变化时约每 [`FAST_RECOMPUTE`] 一次，否则每秒一次）。
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// 测试里直接签一张提醒票据（不经过推送）。
    #[cfg(test)]
    pub(crate) fn issue_order_ticket(&self, intent: crate::telegram::order::Intent) -> String {
        self.tickets.lock().unwrap().issue(intent, Instant::now())
    }

    /// 提醒上「下单…」按钮的票据：过期或不认识返回 `None`。
    pub fn order_ticket(&self, id: &str) -> Option<crate::telegram::order::Intent> {
        self.tickets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id, Instant::now())
    }

    /// 交易用的快照（自动交易、下单、提醒都读它）。不含观察组。
    pub async fn view(&self) -> View {
        self.view.read().await.clone()
    }

    /// 面板用：交易快照 + 观察组（整份）。
    #[cfg(test)]
    pub async fn panel_view(&self) -> PanelView {
        self.panel(&PanelFilter::full()).await
    }

    /// 面板用：按 `filter` 筛过的交易快照 + 观察组。在读锁里只克隆要返回的行，不整份克隆。
    pub async fn panel(&self, filter: &PanelFilter) -> PanelView {
        let mut counts = PanelCounts::default();
        let mut budget = filter.limit.unwrap_or(usize::MAX);
        let mut pick = |lines: &[Arc<Line>]| -> Vec<Arc<Line>> {
            let mut picked = Vec::new();
            for line in lines.iter().filter(|line| filter.matches(line)) {
                counts.lines += 1;
                let signal = panel_signal(line);
                counts.signals += usize::from(signal);
                if budget > 0 && (signal || !filter.signal_only) {
                    budget -= 1;
                    picked.push(Arc::clone(line));
                }
            }
            picked
        };
        let (view, maker_fills) = {
            let view = self.view.read().await;
            let lines = pick(&view.lines);
            // 只查某个合约（补仓确认时的价差对比）用不上挂单测算。
            let (maker, maker_fills) =
                match view.maker.as_ref() {
                    Some(summary) if filter.full => (Some(Arc::clone(summary)), None),
                    Some(summary) if filter.base.is_none() => {
                        let mut fills = 0;
                        let mut stats = Vec::new();
                        for stat in summary.stats.iter().filter(|stat| {
                            filter.pair.as_deref().is_none_or(|pair| stat.pair == pair)
                        }) {
                            fills += stat.fills;
                            if stats.len() < PANEL_MAKER_STATS {
                                stats.push(stat.clone());
                            }
                        }
                        let slim = maker::Summary {
                            quoting: summary.quoting,
                            fills: summary.fills,
                            since: summary.since,
                            stats,
                        };
                        (Some(Arc::new(slim)), Some(fills))
                    }
                    _ => (None, None),
                };
            let view = View {
                enabled: view.enabled,
                size_usdt: view.size_usdt,
                alert_net_pct: view.alert_net_pct,
                min_minutes: view.min_minutes,
                window_days: view.window_days,
                pairs: view.pairs.clone(),
                connected: view.connected.clone(),
                updated_at: view.updated_at,
                lines,
                error: view.error.clone(),
                maker,
            };
            (view, maker_fills)
        };
        let watch = {
            let watch = self.watch.read().await;
            WatchView {
                pairs: watch.pairs.clone(),
                lines: if filter.watch {
                    pick(&watch.lines)
                } else {
                    Vec::new()
                },
            }
        };
        counts.shown = view.lines.len() + watch.lines.len();
        PanelView {
            view,
            watch,
            latency: filter.full.then(|| latency::METRICS.report()),
            counts,
            maker_fills,
        }
    }

    /// 隔离测试注入行情，不启动 WebSocket 或访问真实交易所。
    #[cfg(test)]
    pub(crate) async fn set_test_view(&self, view: View) {
        *self.view.write().await = view;
    }

    /// `cache`：扫描缓存，Hyperliquid 的组要用它核对同名合约是不是同一资产。
    pub fn spawn(self: &Arc<Self>, client: reqwest::Client, cache: Arc<crate::cache::ScanCache>) {
        if !self.config.enabled {
            info!("价差监控已关闭");
            return;
        }
        let monitor = Arc::clone(self);
        // 守着主循环：它一 panic，信号、盘口触发止盈、价差提醒就全停了，自动交易只会因为「快照不新鲜」悄悄停下。
        // panic 了就告警并在几秒后从头再起一个（各家行情连接随旧循环的通道关闭自己退出，新循环重新订阅）。
        tokio::spawn(async move {
            loop {
                let (run, client, cache) =
                    (Arc::clone(&monitor), client.clone(), Arc::clone(&cache));
                match tokio::spawn(async move { run.run(client, cache).await }).await {
                    Ok(()) => return,
                    Err(failure) if failure.is_panic() => {
                        let payload = failure.into_panic();
                        let what = payload
                            .downcast_ref::<String>()
                            .cloned()
                            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
                            .unwrap_or_else(|| "（没有文字说明）".into());
                        tracing::error!("价差监控主循环 panic，5 秒后重启：{what}");
                        monitor.alerts.notify_always(format!(
                            "⚠️ 价差监控主循环崩溃，5 秒后自动重启（重启前自动交易没有新信号）：{what}"
                        ));
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(_) => return,
                }
            }
        });
    }

    async fn set_error(&self, error: Option<String>) {
        self.view.write().await.error = error;
    }

    /// 用到的场所（含观察组）。
    fn venues(&self) -> Vec<Venue> {
        let mut venues: Vec<Venue> = self
            .config
            .all_pairs()
            .flat_map(|pair| [pair.a, pair.b])
            .collect();
        venues.sort();
        venues.dedup();
        venues
    }

    /// 取一家场所的市场列表。
    async fn catalog_of(
        &self,
        client: &reqwest::Client,
        venue: Venue,
    ) -> anyhow::Result<std::collections::BTreeMap<String, pairs::VenueMarket>> {
        match venue {
            Venue::Arcus => {
                let markets: Value = arb_venues::get_json(
                    client.get("https://api.arcus.xyz/v1/markets"),
                    Venue::Arcus,
                )
                .await?;
                let taker = arb_venues::arcus::fetch_base_taker_fee(client).await?;
                catalog::arcus(&markets, taker)
            }
            Venue::LighterRh | Venue::Lighter => {
                let host = if venue == Venue::Lighter {
                    "https://mainnet.zklighter.elliot.ai"
                } else {
                    "https://api.rh.lighter.xyz"
                };
                let details: Value = arb_venues::get_json(
                    client.get(format!("{host}/api/v1/orderBookDetails")),
                    venue,
                )
                .await?;
                catalog::lighter(venue, &details)
            }
            Venue::Gate => {
                let contracts: Value = arb_venues::get_json(
                    client.get("https://api.gateio.ws/api/v4/futures/usdt/contracts"),
                    Venue::Gate,
                )
                .await?;
                catalog::gate(&contracts)
            }
            Venue::Bitget => {
                let contracts: Value = arb_venues::get_json(
                    client.get(
                        "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures",
                    ),
                    Venue::Bitget,
                )
                .await?;
                catalog::bitget(&contracts)
            }
            Venue::Ondo => catalog::ondo(&arb_venues::ondo::fetch_contracts(client).await?),
            Venue::Mexc => {
                let contracts = arb_venues::mexc::fetch_contracts(client).await?;
                crate::preipo::record(
                    Venue::Mexc,
                    contracts
                        .iter()
                        .filter(|c| c.pre_ipo)
                        .map(|c| arb_core::Symbol::perp(&c.base, "USDT").base)
                        .collect(),
                );
                catalog::mexc(&contracts)
            }
            Venue::PopDex => {
                let symbols: Value = arb_venues::get_json(
                    client.get(format!(
                        "{}/api/v1/config/symbols?category=Futures&limit=100",
                        arb_venues::popdex::API
                    )),
                    Venue::PopDex,
                )
                .await?;
                let fees = arb_venues::popdex::fetch_pages::<arb_venues::popdex::FeeRow>(
                    client,
                    arb_venues::popdex::FEE_PATH,
                )
                .await?;
                catalog::popdex(&symbols, &fees)
            }
            Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
                let body = match arb_venues::hyperliquid::dex_name(venue) {
                    Some(dex) => serde_json::json!({"type": "meta", "dex": dex}),
                    None => serde_json::json!({"type": "meta"}),
                };
                let meta: Value = arb_venues::get_json(
                    client.post("https://api.hyperliquid.xyz/info").json(&body),
                    venue,
                )
                .await?;
                catalog::hyperliquid(venue, &meta, self.config.entropy_rebate)
            }
            Venue::Binance => {
                let info: Value = arb_venues::get_json(
                    client.get("https://fapi.binance.com/fapi/v1/exchangeInfo"),
                    Venue::Binance,
                )
                .await?;
                crate::preipo::record(Venue::Binance, crate::preipo::binance_flagged(&info));
                catalog::binance(&info, catalog::BinanceFees::default())
            }
            Venue::Aster => {
                let info: Value = arb_venues::get_json(
                    client.get("https://fapi.asterdex.com/fapi/v1/exchangeInfo"),
                    Venue::Aster,
                )
                .await?;
                crate::preipo::record(Venue::Aster, crate::preipo::aster_flagged(&info));
                catalog::aster(&info)
            }
            other => anyhow::bail!("价差监控不支持 {other}"),
        }
    }

    /// 各家市场列表（并发取；Binance 的 `exchangeInfo` 1.15 MB 要近 2 秒）。
    async fn fetch_catalogs(&self, client: &reqwest::Client) -> Vec<(Venue, CatalogResult)> {
        let venues = self.venues();
        let results = futures_util::future::join_all(
            venues
                .iter()
                .map(|&venue| async move { self.catalog_of(client, venue).await }),
        )
        .await;
        venues
            .into_iter()
            .zip(results)
            .map(|(venue, result)| (venue, result.map_err(|error| format!("{error:#}"))))
            .collect()
    }

    /// 按市场列表与身份簇重算各组的合约；返回每家要订阅的市场名。
    fn rebuild(
        &self,
        states: &mut [PairState],
        catalog: &Catalog,
        identity: Option<&pairs::Identity>,
    ) -> Subscriptions {
        let mut subs: HashMap<Venue, std::collections::BTreeSet<String>> = HashMap::new();
        for state in states.iter_mut() {
            let pair = state.pair;
            state.markets = pairs::pair_markets(pair, catalog, identity, self.config.equities_only);
            state.note = if !catalog.contains_key(&pair.a) || !catalog.contains_key(&pair.b) {
                Some("有一家的市场列表还没取到".into())
            } else if pair.needs_identity_check() && identity.is_none() {
                Some("等首轮扫描核对同名合约是不是同一资产".into())
            } else if state.markets.is_empty() {
                Some("两家没有核实过的同名合约".into())
            } else {
                None
            };
            for market in &state.markets {
                subs.entry(pair.a).or_default().insert(market.a.key.clone());
                subs.entry(pair.b).or_default().insert(market.b.key.clone());
            }
        }
        subs.into_iter()
            .map(|(venue, keys)| (venue, keys.into_iter().collect()))
            .collect()
    }

    async fn run(self: Arc<Self>, client: reqwest::Client, cache: Arc<crate::cache::ScanCache>) {
        let mut catalog = Catalog::new();
        loop {
            let errors = merge_catalog(&mut catalog, self.fetch_catalogs(&client).await);
            if !catalog.is_empty() {
                self.set_error((!errors.is_empty()).then(|| errors.join("；")))
                    .await;
                break;
            }
            self.set_error(Some(format!(
                "取市场列表失败，60 秒后重试：{}",
                errors.join("；")
            )))
            .await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        let now = Utc::now();
        let mut states = Vec::new();
        for pair in self.config.all_pairs() {
            let watch = self.config.is_watch(pair);
            // 观察组的历史在后台读（见下面的 `history_rx`）：几十组攒满一周有几百 MB，不能让交易组的行情等它。
            let history = if watch {
                history::History::default()
            } else {
                let (mut history, broken) =
                    history::load(&self.config.dir, &pair.file_prefix(), now).await;
                if broken > 0 {
                    warn!(pair = %pair.id(), broken, "价差历史里有坏行，已跳过");
                }
                history.prune(now);
                history
            };
            states.push(PairState {
                pair,
                markets: Vec::new(),
                history,
                minute: Minute {
                    start: now.timestamp() / 60 * 60,
                    ..Minute::default()
                },
                normals: HashMap::new(),
                recents: HashMap::new(),
                note: None,
                watch,
            });
        }
        let (history_tx, mut history_rx) = mpsc::channel::<(Pair, history::History)>(4);
        let (dir, watch_pairs) = (self.config.dir.clone(), self.config.watch_pairs.clone());
        tokio::spawn(async move {
            for pair in watch_pairs {
                let dir = dir.clone();
                // 解析几十 MB 的 JSON 放到阻塞线程上，不占异步工作线程（交易组的行情与计算在那上面）。
                let loaded = tokio::task::spawn_blocking(move || {
                    tokio::runtime::Handle::current().block_on(async {
                        let now = Utc::now();
                        let (mut history, broken) =
                            history::load(&dir, &pair.file_prefix(), now).await;
                        history.prune(now);
                        (history, broken)
                    })
                })
                .await;
                let Ok((history, broken)) = loaded else {
                    continue;
                };
                if broken > 0 {
                    warn!(pair = %pair.id(), broken, "价差历史里有坏行，已跳过");
                }
                if history_tx.send((pair, history)).await.is_err() {
                    return;
                }
            }
        });
        let identity = cache
            .get()
            .await
            .map(|snap| pairs::identity_of(&snap.report));
        let mut subs = self.rebuild(&mut states, &catalog, identity.as_ref());
        let mut identity_ready = identity.is_some();
        let mut multipliers = size_multipliers(&states);
        let mut active = active_books(&states);
        info!(
            pairs = %self.config.pairs.iter().map(|p| p.id()).collect::<Vec<_>>().join(","),
            watch = %self.config.watch_pairs.iter().map(|p| p.id()).collect::<Vec<_>>().join(","),
            markets = states.iter().map(|s| s.markets.len()).sum::<usize>(),
            "价差监控启动"
        );

        let (tx, mut rx) = mpsc::channel::<Feed>(8192);
        let shadow_subs = hl_shadow::spawn(&self.config.hl_shadow, &subs);
        // 每家一个连接任务；订阅列表变化时通过 watch 通知它重连。Lighter（RH、主网各一个）另有一个
        // 「重订阅某个市场」的通道：增量断了要重拿快照。
        let mut senders: HashMap<Venue, tokio::sync::watch::Sender<Vec<String>>> = HashMap::new();
        // 各家每个市场最近一次「要订」的时刻（退订缓冲，见 [`SUB_LINGER`]）。
        let mut sub_seen: HashMap<(Venue, String), Instant> = HashMap::new();
        let started = Instant::now();
        for (venue, keys) in &subs {
            for key in keys {
                sub_seen.insert((*venue, key.clone()), started);
            }
        }
        let mut resub_txs: HashMap<Venue, mpsc::Sender<String>> = HashMap::new();
        // 盘口看门狗（见 [`BOOK_MISSING_RESUB`]）：有过盘口的交易市场，盘口从什么时候起没了。
        let mut had_book: std::collections::HashSet<(Venue, String)> = Default::default();
        let mut book_missing: HashMap<(Venue, String), Instant> = HashMap::new();
        for venue in self.venues() {
            let (sub_tx, sub_rx) =
                tokio::sync::watch::channel(subs.get(&venue).cloned().unwrap_or_default());
            senders.insert(venue, sub_tx);
            let resub = resubscribes(venue).then(|| {
                let (resub_tx, resub_rx) = mpsc::channel::<String>(64);
                resub_txs.insert(venue, resub_tx);
                resub_rx
            });
            if protocol(venue, Stream::Trades).is_some() {
                // 同一份订阅列表：盘口与成交各一条连接（Binance 按路径分流）。
                tokio::spawn(venue_loop(
                    venue,
                    Stream::Trades,
                    sub_rx.clone(),
                    None,
                    tx.clone(),
                    false,
                ));
            }
            let trading = self
                .config
                .pairs
                .iter()
                .any(|pair| pair.a == venue || pair.b == venue);
            tokio::spawn(venue_loop(
                venue,
                Stream::Books,
                sub_rx,
                resub,
                tx.clone(),
                trading,
            ));
        }
        // 挂单测算：读回最近的虚拟成交。
        let (mut shadow, broken) = maker::Shadow::load(&self.config.dir, Utc::now()).await;
        if broken > 0 {
            warn!(broken, "挂单测算记录里有坏行，已跳过");
        }
        let has_maker = self.config.maker_shadow
            && self
                .config
                .pairs
                .iter()
                .any(|pair| pair.maker_venue().is_some());
        let mut maker_summary_at: Option<Instant> = None;
        let mut exit_shadow = exit_shadow::Shadow::new(&self.config.dir);
        // 分批平仓测算要的、组里又没有的市场（持仓的合约被身份核对剔掉了等）：合进订阅一起发。
        let mut exit_extra: std::collections::BTreeMap<Venue, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();

        // 盘口：(场所, 市场名) → 本地盘口。几组共用同一家的盘口。
        let mut books: HashMap<(Venue, String), LocalBook> = HashMap::new();
        // 各盘口中间价的短历史（量最近几秒价格动得多快，给自动交易的「急行情不开仓」）。
        let mut tape = tape::PriceTape::default();
        let mut connected = Connected::default();
        for venue in self.venues() {
            connected.venues.insert(venue, false);
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // 盘口一变就重算快照（最快每 FAST_RECOMPUTE 一次），自动交易不必等到下一个整秒才看到信号。
        // 整秒那一次照旧做全部的事（写历史、信号计时、提醒）；快的这一次只算快照。
        let mut fast = tokio::time::interval(tuned_ms(
            "ARB_RH_FAST_RECOMPUTE_MS",
            FAST_RECOMPUTE,
            5,
            1000,
        ));
        fast.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut dirty = false;
        // 快路径的增量重算：上一轮之后盘口变过的（场所 → 市场），整家断线 / 重连的场所，和每行上一次算出来的结果
        // （`slots[组][合约]`，和 `states[组].markets` 一一对应；市场列表一重建就清空）。没变的行直接共用上一次
        // 那份（`Arc`，不拷贝、不重排内存），不再每轮把两千多行全算、全拷一遍。整秒那一轮照旧全算，并把结果放回去。
        let mut dirty_books: HashMap<Venue, std::collections::HashSet<String>> = HashMap::new();
        let mut dirty_venues: std::collections::HashSet<Venue> = Default::default();
        let mut slots: Vec<Vec<RowSlot>> = Vec::new();
        let mut rows = RowIndex::new();
        // 上一轮排好的次序（见 [`sort_lines`]）。
        let mut line_rank: Vec<u32> = Vec::new();
        let mut oldest_received: Option<Instant> = None;
        let mut oldest_watch_received: Option<Instant> = None;
        let mut refresh = tokio::time::interval(Duration::from_secs(300));
        refresh.tick().await;
        let (catalog_tx, mut catalog_rx) = mpsc::channel::<Vec<(Venue, CatalogResult)>>(1);
        let mut refreshing = false;
        // 每 5 分钟记一次各家各路行情的消息数（看负载）。
        let mut traffic: std::collections::BTreeMap<(Venue, &'static str), u64> =
            std::collections::BTreeMap::new();
        let mut traffic_since = Instant::now();
        // 信号从什么时候开始连续成立（组, 合约, 方向）。
        let mut signal_since: HashMap<(String, String, &'static str), Instant> = HashMap::new();
        let mut config = self.config.clone();
        // 慢轮日志（见 [`SLOW_PASS`]）：上一条的时刻；累计 (轮数, 最长, (整秒?, 刷新数, 排队数, 等面板锁毫秒))。
        let mut slow_logged: Option<Instant> = None;
        let mut slow: (u32, Duration, SlowPass) = Default::default();
        // 分钟历史交给后台写（见 [`history::Writer`]），主循环不等文件读写。
        let history_writer = history::Writer::spawn(self.config.dir.clone());

        loop {
            let full = tokio::select! {
                Some(message) = rx.recv() => match message {
                    Feed::Up(venue, up) => {
                        connected.venues.insert(venue, up);
                        dirty_venues.insert(venue);
                        if up && hl_shadow::is_hl(venue) {
                            arb_venues::live_book::heartbeat(venue, Instant::now());
                        }
                        if !up {
                            connected.reconnects += 1;
                            // 断线后旧盘口不能再用：等重连后的快照。下单报价与开仓核算读的实时顶档也一并作废。
                            if hl_shadow::is_hl(venue) {
                                arb_venues::live_book::remove_venue(venue);
                            }
                            books.retain(|(v, _), _| *v != venue);
                        }
                        dirty = true;
                        continue;
                    }
                    Feed::Alive(venue) => {
                        arb_venues::live_book::heartbeat(venue, Instant::now());
                        continue;
                    }
                    Feed::Book(venue, feed::Event::Trade { market, price, quantity, seller_initiated, .. }, stamp) => {
                        *traffic.entry((venue, "成交")).or_default() += 1;
                        shadow.on_trade(&market, price, quantity, seller_initiated, &books, venue, stamp.received, Utc::now());
                        continue;
                    }
                    Feed::Book(venue, event, stamp) => {
                        // 这条连接还活着。**在这里**打心跳而不是在连接任务里：顶档是在这个循环里应用的，
                        // 循环积压时心跳就不该照样新鲜。
                        if hl_shadow::is_hl(venue) {
                            arb_venues::live_book::heartbeat(venue, Instant::now());
                        }
                        *traffic.entry((venue, "盘口")).or_default() += 1;
                        latency::METRICS.observe(latency::Scope::Venue(venue), latency::Stage::ReceiveToApply, stamp.received.elapsed());
                        latency::METRICS.observe(latency::Scope::Venue(venue), latency::Stage::EnqueueToApply, stamp.enqueued.elapsed());
                        let resub = resub_txs.get(&venue);
                        let events = match event {
                            feed::Event::Batch(events) => events,
                            event => vec![event],
                        };
                        for event in events {
                            if let Some(event) = scaled(event, venue, &multipliers) {
                                let trading = match &event {
                                    feed::Event::Snapshot { market, .. } | feed::Event::Top { market, .. } | feed::Event::Bbo { market, .. } | feed::Event::Delta { market, .. } => active.get(&venue).is_some_and(|keys| keys.contains(market)).then(|| market.clone()),
                                    _ => None,
                                };
                                if apply_at(&mut books, venue, event, resub, stamp) {
                                    let oldest = if trading.is_some() { &mut oldest_received } else { &mut oldest_watch_received };
                                    *oldest = Some(oldest.map_or(stamp.received, |at| at.min(stamp.received)));
                                    if let Some(market) = trading {
                                        dirty = true;
                                        dirty_books.entry(venue).or_default().insert(market);
                                    }
                                }
                            }
                        }
                        continue;
                    }
                },
                _ = refresh.tick() => {
                    // 交易时段（isOutsideRth）、费率、上下架会变：5 分钟刷新一次。放在后台取，
                    // 不让主循环（每 10ms 的信号检测，自动交易也靠它）停下来等。
                    if !refreshing {
                        refreshing = true;
                        let (monitor, client, tx) = (Arc::clone(&self), client.clone(), catalog_tx.clone());
                        tokio::spawn(async move {
                            let results = monitor.fetch_catalogs(&client).await;
                            let _ = tx.send(results).await;
                        });
                    }
                    continue;
                }
                Some((pair, history)) = history_rx.recv() => {
                    // 观察组的历史读完了：换上（读的这几秒里新攒的几分钟只在文件里，下次启动照样读回来）。
                    if let Some(state) = states.iter_mut().find(|state| state.pair == pair) {
                        state.history = history;
                        state.normals.clear();
                        state.recents.clear();
                    }
                    continue;
                }
                Some(results) = catalog_rx.recv() => {
                    refreshing = false;
                    let _ = merge_catalog(&mut catalog, results);
                    let identity = cache.get().await.map(|snap| pairs::identity_of(&snap.report));
                    identity_ready |= identity.is_some();
                    let fresh = self.rebuild(&mut states, &catalog, identity.as_ref());
                    multipliers = size_multipliers(&states);
                    active = active_books(&states);
                    slots.clear();
                    shadow.retain_markets(&|pair, base| monitored(&states, pair, base));
                    if fresh != subs {
                        subs = fresh;
                        let wanted = exit_shadow::with_extra(&subs, &exit_extra);
                        shadow_subs.send_if_modified(|old| { let next = hl_shadow::plan(&self.config.hl_shadow, &wanted); if *old == next { false } else { *old = next; true } });
                    }
                    // 列表没变也要过一遍：缓冲期满的退订在这里生效。
                    let wanted = exit_shadow::with_extra(&subs, &exit_extra);
                    send_subscriptions(&senders, &wanted, &mut sub_seen, Instant::now(), "市场列表刷新");
                    continue;
                }
                _ = tick.tick() => true,
                _ = fast.tick(), if dirty => false,
            };
            dirty = false;
            if crate::shutdown::is_draining() {
                shadow.settle().await;
                exit_shadow.settle().await;
                // 停机：先等后台把已交出去的几分钟写完，再把这一分钟写掉、退出循环。
                history_writer.finish(Duration::from_secs(5)).await;
                for state in &states {
                    let _ = history::append(
                        &self.config.dir,
                        &state.pair.file_prefix(),
                        &state.minute.rows(),
                        Utc::now(),
                    )
                    .await;
                }
                return;
            }
            // 盘口看门狗：Lighter 断档后删掉盘口、申请重订阅，但这一申请可能被丢掉（通道满、同一市场 10 秒内
            // 第二次重订阅被忽略），之后的增量找不到盘口也不会再申请 —— 这个市场就一直没有盘口（几小时，直到
            // 整家重连），上面已有仓位的盘口触发止盈也跟着失效。每秒查一次：有过盘口、连接也在的交易市场，
            // 盘口没了超过 BOOK_MISSING_RESUB 就再申请一次。
            if full {
                let due = books_to_resubscribe(
                    Instant::now(),
                    resub_txs
                        .keys()
                        .copied()
                        .filter(|venue| connected.up(*venue)),
                    &active,
                    &|id| books.contains_key(id),
                    &mut had_book,
                    &mut book_missing,
                );
                for (venue, key) in due {
                    warn!(%venue, market = %key, "盘口没了 {} 秒还没补上：再申请一次重订阅", BOOK_MISSING_RESUB.as_secs());
                    if let Some(tx) = resub_txs.get(&venue) {
                        let _ = tx.try_send(key);
                    }
                }
            }
            // 首轮扫描刚出来：Hyperliquid 的组现在能核对身份了，不等 5 分钟。
            if full
                && !identity_ready
                && let Some(snap) = cache.get().await
            {
                identity_ready = true;
                let identity = pairs::identity_of(&snap.report);
                subs = self.rebuild(&mut states, &catalog, Some(&identity));
                multipliers = size_multipliers(&states);
                active = active_books(&states);
                slots.clear();
                shadow.retain_markets(&|pair, base| monitored(&states, pair, base));
                let wanted = exit_shadow::with_extra(&subs, &exit_extra);
                shadow_subs.send_if_modified(|old| {
                    let next = hl_shadow::plan(&self.config.hl_shadow, &wanted);
                    if *old == next {
                        false
                    } else {
                        *old = next;
                        true
                    }
                });
                send_subscriptions(
                    &senders,
                    &wanted,
                    &mut sub_seen,
                    Instant::now(),
                    "首轮扫描核对完身份",
                );
            }
            let recompute_started = Instant::now();
            let now_utc = Utc::now();
            let now = Instant::now();
            config.size_usdt = self.size_usdt();
            // 整秒、或有场所断线 / 重连（那家的盘口整批清掉了）：全部盘口过一遍。其余的快路径只记变过的那几本。
            if full || !dirty_venues.is_empty() {
                tape.sync(&books, now);
            } else {
                for (venue, keys) in &dirty_books {
                    tape.sync_keys(&books, *venue, keys, now);
                }
            }
            if full && let Some(source) = self.exit_source.get() {
                let watched = source();
                // 持仓的市场集合变了才重发订阅（一变那家的连接就要重连），也只发列表真的变了的那几家。
                let missing = exit_shadow::missing_markets(&watched, &catalog, &subs, |venue| {
                    senders.contains_key(&venue)
                });
                if missing != exit_extra {
                    exit_extra = missing;
                    let wanted = exit_shadow::with_extra(&subs, &exit_extra);
                    shadow_subs.send_if_modified(|old| {
                        let next = hl_shadow::plan(&self.config.hl_shadow, &wanted);
                        if *old == next {
                            false
                        } else {
                            *old = next;
                            true
                        }
                    });
                    send_subscriptions(
                        &senders,
                        &wanted,
                        &mut sub_seen,
                        Instant::now(),
                        "按持仓补订 / 退订盘口（分批平仓测算）",
                    );
                }
                exit_shadow.sample(&watched, &books, &catalog, now, now_utc);
                let fired = exit_shadow.take_fired();
                if let Some(trigger) = self.exit_trigger.get() {
                    for id in fired {
                        trigger(id);
                    }
                }
            }
            let start = now_utc.timestamp() / 60 * 60;
            // 这一轮最多重算多少个正常水平 / 近期价差（见 [`cached_history`]）。
            let mut refresh_budget = RefreshBudget::new(
                HISTORY_REFRESH_PER_PASS,
                HISTORY_REFRESH_TIME,
                Instant::now(),
            );
            // 槽位和市场列表对不上（刚启动、列表重建后清空过）：重建槽位和盘口索引，这一轮全算。
            if slots.len() != states.len()
                || slots
                    .iter()
                    .zip(&states)
                    .any(|(row, state)| row.len() != state.markets.len())
            {
                slots = states
                    .iter()
                    .map(|state| vec![RowSlot::default(); state.markets.len()])
                    .collect();
                rows = row_index(&states);
                line_rank.clear();
            }
            if !full {
                for (venue, keys) in &dirty_books {
                    let Some(index) = rows.get(venue) else {
                        continue;
                    };
                    for (group, row) in keys.iter().filter_map(|key| index.get(key)).flatten() {
                        if let Some(slot) = slots
                            .get_mut(*group as usize)
                            .and_then(|row_slots| row_slots.get_mut(*row as usize))
                        {
                            slot.dirty = true;
                        }
                    }
                }
            }
            let mut lines: Vec<Arc<Line>> = Vec::with_capacity(slots.iter().map(Vec::len).sum());
            // 和 `lines` 一一对应：这一行是不是这一轮新算 / 新换的（没换的还是上一轮那一份）。
            let mut fresh: Vec<bool> = Vec::with_capacity(lines.capacity());
            let mut watch_lines: Vec<Arc<Line>> = Vec::new();
            let mut minute_rows = Vec::new();
            for (state, slot_row) in states.iter_mut().zip(slots.iter_mut()) {
                let pair = state.pair;
                // 观察组只在整秒算：快路径是给自动交易的，它们用不上。
                if !full && state.watch {
                    continue;
                }
                if full && start != state.minute.start {
                    let rows = state.minute.rows();
                    rows.iter().for_each(|row| state.history.push(row));
                    state.history.prune(now_utc);
                    minute_rows.push((pair.file_prefix(), rows));
                    state.minute = Minute {
                        start,
                        ..Minute::default()
                    };
                }
                let id = pair.id();
                // 整家断线 / 重连 = 这一家的盘口全变。
                let all = dirty_venues.contains(&pair.a) || dirty_venues.contains(&pair.b);
                for (market, slot) in state.markets.iter().zip(slot_row.iter_mut()) {
                    let changed = all || std::mem::take(&mut slot.dirty);
                    if !full
                        && !changed
                        && let Some(cached) =
                            slot.line.as_ref().filter(|line| line.base == market.base)
                    {
                        // 两本盘口都没变。近 5 秒波动只在窗口里有样本滑出去（或刚记满窗口）时才变：没到那个
                        // 时刻就还是上次的数，不用再查。
                        let (recent, until) = if slot.recent_until.is_none_or(|at| now < at) {
                            (cached.recent_move, slot.recent_until)
                        } else {
                            recent_move(&tape, pair, market, now)
                        };
                        slot.recent_until = until;
                        let signal_sec =
                            cached.best.as_ref().filter(|best| best.signal).map(|best| {
                                signal_since
                                    .get(&(id.clone(), market.base.clone(), best.direction))
                                    .map_or(0, |since| {
                                        now.saturating_duration_since(*since).as_secs()
                                    })
                            });
                        let same = recent == cached.recent_move
                            && signal_sec.is_none_or(|sec| {
                                cached
                                    .best
                                    .as_ref()
                                    .is_some_and(|best| best.signal_sec == sec)
                            });
                        if same {
                            // 和上一轮一模一样：共用同一份。
                            lines.push(Arc::clone(cached));
                            fresh.push(false);
                        } else {
                            let mut line = Line::clone(cached);
                            line.recent_move = recent;
                            if let (Some(best), Some(sec)) = (line.best.as_mut(), signal_sec) {
                                best.signal_sec = sec;
                            }
                            let line = Arc::new(line);
                            slot.line = Some(Arc::clone(&line));
                            lines.push(line);
                            fresh.push(true);
                        }
                        continue;
                    }
                    let session = classify(now_utc, market.crypto(), market.outside_rth);
                    let history = &state.history;
                    let (normal, normal_minutes) = cached_history(
                        &mut state.normals,
                        (market.base.clone(), session),
                        start,
                        &mut refresh_budget,
                        || {
                            (
                                history.normal(&market.base, session, now_utc),
                                history.minutes(&market.base, session, now_utc),
                            )
                        },
                    )
                    .unwrap_or((None, 0));
                    let mut line = evaluate(
                        pair,
                        market,
                        books.get(&(pair.a, market.a.key.clone())),
                        books.get(&(pair.b, market.b.key.clone())),
                        normal,
                        normal_minutes,
                        session,
                        &config,
                        now,
                    );
                    // 快的（盘口一变就算）和整秒的两条路径都要有：自动交易读的多半是快的那份。
                    let (recent, recent_until) = recent_move(&tape, pair, market, now);
                    line.recent_move = recent;
                    line.recent_entry = cached_history(
                        &mut state.recents,
                        market.base.clone(),
                        start,
                        &mut refresh_budget,
                        || history.recent_entry(&market.base, RECENT_ENTRY_HOURS, now_utc),
                    )
                    .flatten();
                    if has_maker && pair.maker_venue().is_some() {
                        shadow.observe(
                            pair,
                            market,
                            line.maker.as_ref(),
                            books.get(&(pair.a, market.a.key.clone())),
                            books.get(&(pair.b, market.b.key.clone())),
                            normal_median_of(line.normal.as_ref()),
                            session,
                            self.config.size_usdt,
                            now,
                        );
                    }
                    if !full {
                        // 快的这一次只读信号计时（提醒的「保持够久」仍按整秒采样，不因为一次抖动清零）。
                        if let Some(best) = line.best.as_mut().filter(|best| best.signal) {
                            best.signal_sec = signal_since
                                .get(&(id.clone(), market.base.clone(), best.direction))
                                .map_or(0, |since| now.saturating_duration_since(*since).as_secs());
                        }
                        let line = Arc::new(line);
                        *slot = RowSlot {
                            line: Some(Arc::clone(&line)),
                            recent_until,
                            dirty: false,
                        };
                        lines.push(line);
                        fresh.push(true);
                        continue;
                    }
                    if let Some(best) = line.best.as_mut() {
                        let key = (id.clone(), market.base.clone(), best.direction);
                        if best.signal {
                            let since = *signal_since.entry(key).or_insert(now);
                            best.signal_sec = now.saturating_duration_since(since).as_secs();
                        } else {
                            signal_since.remove(&key);
                        }
                    }
                    // 方向换了或没信号：另一个方向的计时作废。
                    signal_since.retain(|(p, base, direction), _| {
                        p != &id
                            || base != &market.base
                            || line
                                .best
                                .as_ref()
                                .is_some_and(|b| b.signal && b.direction == *direction)
                    });
                    if let Some(basis) = line.basis_pct.and_then(|b| b.to_f64()) {
                        let minute = &mut state.minute;
                        minute
                            .basis
                            .entry(market.base.clone())
                            .or_default()
                            .push(basis);
                        minute.sessions.insert(market.base.clone(), session);
                        let keep_max = |map: &mut HashMap<String, f64>, value: Option<f64>| {
                            if let Some(value) = value {
                                let slot = map.entry(market.base.clone()).or_insert(value);
                                *slot = slot.max(value);
                            }
                        };
                        keep_max(
                            &mut minute.entry_a,
                            line.long_a
                                .as_ref()
                                .and_then(|l| l.quote.entry_pct.to_f64()),
                        );
                        keep_max(
                            &mut minute.entry_b,
                            line.long_b
                                .as_ref()
                                .and_then(|l| l.quote.entry_pct.to_f64()),
                        );
                    }
                    self.maybe_alert(&line, &config);
                    let line = Arc::new(line);
                    if state.watch {
                        watch_lines.push(line);
                    } else {
                        *slot = RowSlot {
                            line: Some(Arc::clone(&line)),
                            recent_until,
                            dirty: false,
                        };
                        lines.push(line);
                        fresh.push(true);
                    }
                }
            }
            dirty_books.clear();
            dirty_venues.clear();
            history_writer.send(minute_rows, now_utc);
            sort_lines(&mut lines, &fresh, &mut line_rank);
            watch_lines.sort_by(|a, b| line_order(a, b));
            // 各组的概况（合约数、历史覆盖、费率范围）只在整秒重算：两次整秒之间不会变。
            let pair_views = full.then(|| -> (Vec<PairView>, Vec<PairView>) {
                states
                    .iter()
                    .map(|state| {
                        let fees: Vec<Decimal> = state
                            .markets
                            .iter()
                            .filter_map(PairMarket::round_trip_pct)
                            .collect();
                        PairView {
                            id: state.pair.id(),
                            a: state.pair.a,
                            b: state.pair.b,
                            markets: state.markets.len(),
                            history_minutes: state.history.coverage_minutes(),
                            fee_min_pct: fees.iter().min().copied(),
                            fee_max_pct: fees.iter().max().copied(),
                            note: state.note.clone(),
                            observe_only: state.pair.observe_only(),
                            watch_only: state.watch,
                        }
                    })
                    .partition(|view| view.watch_only)
            });
            let (watch_views, pair_views) = match pair_views {
                Some((watch, trading)) => (watch, Some(trading)),
                None => (Vec::new(), None),
            };
            let written = if has_maker {
                shadow.tick(&books, Venue::Binance, now)
            } else {
                0
            };
            if full && now.saturating_duration_since(traffic_since) >= Duration::from_secs(300) {
                let seconds = now.saturating_duration_since(traffic_since).as_secs_f64();
                let rates: Vec<String> = traffic
                    .iter()
                    .map(|((venue, kind), count)| {
                        format!("{venue} {kind} {:.1}/s", *count as f64 / seconds)
                    })
                    .collect();
                info!(rates = %rates.join("，"), "价差监控：行情消息量");
                info!(summary = %serde_json::to_string(&latency::METRICS.report()).unwrap_or_default(), "行情分段延迟与 HL 影子 A/B 统计（微秒；分位为桶上界）");
                traffic.clear();
                traffic_since = now;
            }
            // 汇总每 10 秒（或有新记录时）重算一次：最多几万笔，按组分桶。
            let maker_summary = (has_maker
                && full
                && (written > 0
                    || maker_summary_at.is_none_or(|at| {
                        now.saturating_duration_since(at) >= Duration::from_secs(10)
                    })))
            .then(|| {
                maker_summary_at = Some(now);
                shadow.summary(now_utc)
            });
            let lock_started = Instant::now();
            let mut view = self.view.write().await;
            let mut lock_wait = lock_started.elapsed();
            if let Some(summary) = maker_summary {
                view.maker = Some(Arc::new(summary));
            }
            view.size_usdt = config.size_usdt;
            view.connected = connected.clone();
            if let Some(pairs) = pair_views {
                view.pairs = pairs;
            }
            view.updated_at = Some(now_utc);
            view.lines = lines;
            drop(view);
            if full {
                let lock_started = Instant::now();
                let mut watch = self.watch.write().await;
                lock_wait += lock_started.elapsed();
                *watch = WatchView {
                    pairs: watch_views,
                    lines: watch_lines,
                };
            }
            if full && let Some(received) = oldest_watch_received.take() {
                latency::METRICS.observe(
                    latency::Scope::Pipeline,
                    latency::Stage::ReceiveToWatchPublish,
                    received.elapsed(),
                );
            }
            latency::METRICS.publish(recompute_started, oldest_received.take());
            self.changed.notify_waiters();
            let spent = recompute_started.elapsed();
            if spent >= SLOW_PASS {
                // 这一轮占着主循环这么久，期间各家盘口更新都在排队（见行情分段延迟的 enqueue_to_apply）。
                // 同类最多 SLOW_PASS_LOG_EVERY 记一条，带上这段时间慢了几轮、最长多久。
                slow.0 += 1;
                if spent > slow.1 {
                    slow.1 = spent;
                    slow.2 = (
                        full,
                        refresh_budget.used(),
                        rx.len(),
                        lock_wait.as_millis() as u64,
                    );
                }
                if slow_logged
                    .is_none_or(|at| now.saturating_duration_since(at) >= SLOW_PASS_LOG_EVERY)
                {
                    let (count, longest, (was_full, refreshed, backlog, lock_ms)) =
                        std::mem::take(&mut slow);
                    warn!(
                        passes = count,
                        longest_ms = longest.as_millis() as u64,
                        full = was_full,
                        refreshed,
                        backlog,
                        lock_ms,
                        "价差监控：重算太慢，这段时间盘口更新在排队（passes = 距上一条的慢轮数，其余是最慢那一轮的；lock_ms = 等面板读锁）"
                    );
                    slow_logged = Some(now);
                }
            }
        }
    }

    /// 估算名义改成自动交易的单腿名义：下一轮（≤100ms）起报价、信号、提醒、逐档估算都按它算，
    /// 下单前的实时盘口复核也就能覆盖这么大的单。
    pub fn set_size_usdt(&self, size: Decimal) {
        let mut current = self
            .size_usdt
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *current != size {
            info!(from = %current.normalize(), to = %size.normalize(), "价差监控估算名义跟着单腿名义改了");
            *current = size;
        }
    }

    fn size_usdt(&self) -> Decimal {
        *self
            .size_usdt
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 提醒里「自动交易会不会下这一笔」由谁来说（自动交易器）。只设一次，之后再设不生效。
    pub fn set_auto_note(&self, note: AutoNote) {
        let _ = self.auto_note.set(note);
    }

    /// 分批平仓测算（[`exit_shadow`]）从哪里拿持仓（实盘规则轮）。只设一次，之后再设不生效。
    pub fn set_exit_source(&self, source: exit_shadow::Source) {
        let _ = self.exit_source.set(source);
    }

    /// 盘口触发止盈（[`exit_shadow::Shadow::take_fired`]）交给谁（实盘交易台）。只设一次，之后再设不生效。
    pub fn set_exit_trigger(&self, trigger: exit_shadow::Trigger) {
        let _ = self.exit_trigger.set(trigger);
    }

    fn maybe_alert(&self, line: &Line, config: &Config) {
        if line.observe_only
            || line.watch_only
            || self.alerts.mode() == crate::alert::NotificationMode::Trades
        {
            return;
        }
        let Some(best) = line
            .best
            .as_ref()
            .filter(|b| b.signal && b.signal_sec >= SIGNAL_HOLD.as_secs())
        else {
            return;
        };
        let auto = self.auto_note.get().and_then(|note| note(line, best));
        let Some(text) = alert_text(line, best, config, auto.as_deref()) else {
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
        // 最早那组沿用原来的告警 key；Alerter 的去重状态仅在内存，进程重启仍会重置。
        let key = if line.pair == Pair::RH.id() {
            let legacy = if best.direction == "long_a" {
                "long_arcus"
            } else {
                "long_lighter"
            };
            format!("rh-spread:{}:{legacy}", line.base)
        } else {
            format!("spread:{}:{}:{}", line.pair, line.base, best.direction)
        };
        // 告警发到管理员私聊、命令机器人在跑时，下面挂「下单…」：点了只回确认消息，再点确认才下单。
        let sent_now = if self.alerts.actions_enabled() {
            let mut tickets = self
                .tickets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let ticket = tickets.issue(
                crate::telegram::order::Intent {
                    pair: line.pair.clone(),
                    base: line.base.clone(),
                    direction: best.direction,
                },
                now,
            );
            let keyboard = vec![vec![("🛒 下单…".to_string(), format!("o:{ticket}"))]];
            let sent = self.alerts.notify_with_buttons(&key, text, keyboard);
            if !sent {
                tickets.remove(&ticket);
            }
            sent
        } else {
            self.alerts.notify(&key, text)
        };
        if sent_now {
            sent.push_back(now);
            info!(pair = %line.pair, symbol = %line.base, direction = best.direction, net_usdt = ?best.net_usdt, "价差监控：推送提醒");
        }
    }
}

/// 场所的显示名（提醒、页面用）。
pub fn venue_label(venue: Venue) -> &'static str {
    match venue {
        Venue::Arcus => "Arcus",
        Venue::LighterRh => "Lighter RH",
        Venue::Hyperliquid => "Hyperliquid",
        Venue::HyperliquidXyz => "HL-xyz",
        Venue::HyperliquidIo => "HL-io",
        Venue::Lighter => "Lighter",
        Venue::Gate => "Gate",
        Venue::Bitget => "Bitget",
        Venue::Ondo => "Ondo",
        Venue::PopDex => "PopDEX",
        Venue::Mexc => "MEXC",
        Venue::Aster => "Aster",
        other => other.as_str(),
    }
}

/// 提醒文本。
///
/// `auto` 是自动交易对这一笔的结论（[`Monitor::set_auto_note`]）；没有时只按入场硬条件笼统地说。
pub fn alert_text(line: &Line, best: &Best, config: &Config, auto: Option<&str>) -> Option<String> {
    let (long, short, leg) = line.legs(best.direction)?;
    let normal = line.normal.as_ref()?;
    let auto_note = match auto {
        Some(note) => format!("🤖 {note}\n"),
        None => leg.auto_entry_block_reason().map_or_else(
            || "仅偏离提醒；是否自动下单见面板的自动交易设置。".to_string(),
            |reason| format!("仅偏离提醒，不满足现有自动开仓规则：{reason}。"),
        ),
    };
    Some(format!(
        "📈 价差偏离：{} {}，多 {} / 空 {}\n可成交价差 {}%（{} USDT），{}正常基差 {:.3}%，当前 {}%（{} − {}）\n回到正常水平预估净赚 {}%（≈{} USDT，已扣手续费与平仓穿价）\n{auto_note}同一合约同一方向 30 分钟内不重复。",
        line.base,
        line.session.label(),
        venue_label(long),
        venue_label(short),
        leg.quote.entry_pct.round_dp(3),
        config.size_usdt.normalize(),
        line.session.label(),
        normal.median,
        line.basis_pct?.round_dp(3),
        venue_label(line.a),
        venue_label(line.b),
        leg.net_to_normal_pct?.round_dp(3),
        best.net_usdt?.normalize(),
    ))
}

/// 页面上行的顺序：回到正常的预估净额从高到低，再按组、合约。
/// 把这一轮的行按 [`line_order`] 排好。`lines` 是生成顺序（每轮相同），`fresh[i]` = 第 i 行这一轮换过，
/// `rank` 记着上一轮排完后每个位置是生成顺序里的第几行。
///
/// 没换的行和上一轮是同一份，彼此的先后不会变：只把换过的行（每轮几十到几百行）排好，再用二分插回没换的
/// 那一串里，不用每轮把 1800 行整个重排。[`line_order`] 是全序（组、合约兜底），结果与整个重排完全一样。
/// 行数对不上（刚启动、市场列表重建）就整个重排。
fn sort_lines(lines: &mut Vec<Arc<Line>>, fresh: &[bool], rank: &mut Vec<u32>) {
    let count = lines.len();
    let order = |a: &u32, b: &u32| line_order(&lines[*a as usize], &lines[*b as usize]);
    let sorted: Vec<u32> = if rank.len() == count && fresh.len() == count {
        let (mut moved, kept): (Vec<u32>, Vec<u32>) =
            rank.iter().partition(|index| fresh[**index as usize]);
        moved.sort_by(order);
        let mut merged = Vec::with_capacity(count);
        let mut rest = kept.as_slice();
        for index in moved {
            let before =
                rest.partition_point(|kept| order(kept, &index) != std::cmp::Ordering::Greater);
            merged.extend_from_slice(&rest[..before]);
            merged.push(index);
            rest = &rest[before..];
        }
        merged.extend_from_slice(rest);
        merged
    } else {
        let mut all: Vec<u32> = (0..count as u32).collect();
        all.sort_by(order);
        all
    };
    let mut taken: Vec<Option<Arc<Line>>> = std::mem::take(lines).into_iter().map(Some).collect();
    lines.extend(
        sorted
            .iter()
            .filter_map(|index| taken[*index as usize].take()),
    );
    *rank = sorted;
}

fn line_order(a: &Line, b: &Line) -> std::cmp::Ordering {
    let key = |l: &Line| {
        l.best
            .as_ref()
            .and_then(|b| b.net_usdt)
            .unwrap_or(Decimal::MIN)
    };
    key(b)
        .cmp(&key(a))
        .then_with(|| a.pair.cmp(&b.pair))
        .then_with(|| a.base.cmp(&b.base))
}

/// 推增量、要靠 nonce 查连续性的场所：断了要单独重订阅那个市场。
/// 盘口看门狗：有过盘口的交易市场，盘口没了这么久还没补上就再申请重订阅。重连后 Lighter 主网按限额逐个
/// 重新订阅，近百个市场要几十秒，所以留一分钟。
const BOOK_MISSING_RESUB: Duration = Duration::from_secs(60);

/// 盘口看门狗的一次检查：`venues` 里（会重订阅、连接在）的交易市场，有过盘口、现在没有、而且没了满
/// [`BOOK_MISSING_RESUB`] 的，返回出来去重订阅（计时从这一刻重来）。盘口回来了就清掉计时。
fn books_to_resubscribe(
    now: Instant,
    venues: impl Iterator<Item = Venue>,
    active: &ActiveBooks,
    has_book: &dyn Fn(&(Venue, String)) -> bool,
    had_book: &mut std::collections::HashSet<(Venue, String)>,
    missing: &mut HashMap<(Venue, String), Instant>,
) -> Vec<(Venue, String)> {
    let mut due = Vec::new();
    for venue in venues {
        let Some(keys) = active.get(&venue) else {
            continue;
        };
        for key in keys {
            let id = (venue, key.clone());
            if has_book(&id) {
                missing.remove(&id);
                had_book.insert(id);
                continue;
            }
            if !had_book.contains(&id) {
                continue;
            }
            let since = *missing.entry(id.clone()).or_insert(now);
            if now.saturating_duration_since(since) >= BOOK_MISSING_RESUB {
                missing.insert(id.clone(), now);
                due.push(id);
            }
        }
    }
    due
}

fn resubscribes(venue: Venue) -> bool {
    matches!(venue, Venue::LighterRh | Venue::Lighter)
}

/// （场所, 市场名）→ 盘口量要乘的数（Gate 的张数 → 标的数量）。
fn size_multipliers(states: &[PairState]) -> HashMap<(Venue, String), Decimal> {
    let mut out = HashMap::new();
    for state in states {
        for market in &state.markets {
            for (venue, side) in [(state.pair.a, &market.a), (state.pair.b, &market.b)] {
                if let Some(multiplier) = side.size_multiplier {
                    out.insert((venue, side.key.clone()), multiplier);
                }
            }
        }
    }
    out
}

/// 把盘口量换成标的数量。Gate、MEXC 的量是张数：不知道面值的市场没法换算，整条丢掉（返回 `None`），
/// 不拿张数当数量。其它场所原样返回。
fn scaled(
    event: feed::Event,
    venue: Venue,
    multipliers: &HashMap<(Venue, String), Decimal>,
) -> Option<feed::Event> {
    if !matches!(venue, Venue::Gate | Venue::Mexc) {
        return Some(event);
    }
    let scale = |levels: Vec<(Decimal, Decimal)>, m: Decimal| {
        levels.into_iter().map(|(p, q)| (p, q * m)).collect()
    };
    match event {
        feed::Event::Snapshot {
            market,
            bids,
            asks,
            nonce,
        } => {
            let m = *multipliers.get(&(venue, market.clone()))?;
            Some(feed::Event::Snapshot {
                bids: scale(bids, m),
                asks: scale(asks, m),
                market,
                nonce,
            })
        }
        feed::Event::Delta {
            market,
            bids,
            asks,
            begin_nonce,
            nonce,
        } => {
            let m = *multipliers.get(&(venue, market.clone()))?;
            Some(feed::Event::Delta {
                bids: scale(bids, m),
                asks: scale(asks, m),
                market,
                begin_nonce,
                nonce,
            })
        }
        other => Some(other),
    }
}

/// 深处档位最多沿用多久的整本（毫秒）。HL 普通盘口 5~10 秒一推（10-31 东京实测全部在 10 秒内）：
/// 超过它说明整本断了，只剩 fast 顶部，深处不再拼旧的。
const HL_DEPTH_MAX_AGE_MS: i64 = 30_000;

/// 把 fast 顶部拼到现有盘口上：顶部覆盖到的价格区间整段换成顶部（区间里顶部没有的价位 = 已经没了），
/// 更深的沿用整本。某侧不满 [`feed::HL_TOP_LEVELS`] 档 = 那侧整个就这些，深处清空。
fn merge_top(
    book: &LocalBook,
    source_ms: i64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
) -> (
    std::collections::BTreeMap<Decimal, Decimal>,
    std::collections::BTreeMap<Decimal, Decimal>,
) {
    let depth_fresh = book
        .depth_time_ms
        .is_some_and(|depth| source_ms.saturating_sub(depth) <= HL_DEPTH_MAX_AGE_MS);
    let side = |old: &std::collections::BTreeMap<Decimal, Decimal>,
                top: Vec<(Decimal, Decimal)>,
                is_bid: bool| {
        let complete = top.len() < feed::HL_TOP_LEVELS;
        // 顶部最深的一档：买盘是最低价，卖盘是最高价。
        let edge = if is_bid {
            top.iter().map(|(p, _)| *p).min()
        } else {
            top.iter().map(|(p, _)| *p).max()
        };
        let mut next = std::collections::BTreeMap::new();
        if depth_fresh
            && !complete
            && let Some(edge) = edge
        {
            next.extend(
                old.iter()
                    .filter(|(price, _)| {
                        if is_bid {
                            **price < edge
                        } else {
                            **price > edge
                        }
                    })
                    .map(|(p, q)| (*p, *q)),
            );
        }
        top.into_iter()
            .for_each(|(p, q)| LocalBook::apply(&mut next, p, q));
        next
    };
    (side(&book.bids, bids, true), side(&book.asks, asks, false))
}

#[cfg(test)]
fn apply(
    books: &mut HashMap<(Venue, String), LocalBook>,
    venue: Venue,
    event: feed::Event,
    resub: Option<&mpsc::Sender<String>>,
) {
    apply_at(books, venue, event, resub, latency::Stamp::now());
}

/// Preserve receive time through coalescing and queueing. Equal producer times never
/// refresh freshness; conflicting equal-time snapshots keep the previous safe version.
fn apply_at(
    books: &mut HashMap<(Venue, String), LocalBook>,
    venue: Venue,
    event: feed::Event,
    resub: Option<&mpsc::Sender<String>>,
    stamp: latency::Stamp,
) -> bool {
    let scope = latency::Scope::Venue(venue);
    let changed = match event {
        feed::Event::Snapshot {
            market,
            bids,
            asks,
            nonce,
        } => {
            let key = (venue, market);
            let old = books.get(&key);
            if old.is_some_and(|book| book.source_time_ms.is_some()) && stamp.source_ms.is_none() {
                latency::METRICS.count(scope, latency::Counter::TimestampConflict);
                return false; // A missing timestamp cannot erase an established version fence.
            }
            if let (Some(source), Some(previous)) =
                (stamp.source_ms, old.and_then(|b| b.source_time_ms))
                && source < previous
            {
                latency::METRICS.count(scope, latency::Counter::Older);
                return false;
            }
            let mut book = LocalBook::new(stamp.received);
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce;
            book.source_time_ms = stamp.source_ms;
            book.depth_time_ms = stamp.source_ms;
            // 同一时刻先到的是 fast 顶部（深处还是旧的）：这本整本正好把深处补上，要收。
            if let Some(old) = old
                && stamp.source_ms.is_some()
                && stamp.source_ms == old.source_time_ms
                && old.depth_time_ms == old.source_time_ms
            {
                let event = if old.bids == book.bids && old.asks == book.asks {
                    latency::Counter::Duplicate
                } else {
                    latency::Counter::TimestampConflict
                };
                latency::METRICS.count(scope, event);
                return false;
            }
            if let Some(old) = old {
                latency::METRICS.observe(
                    scope,
                    latency::Stage::BookUpdateInterval,
                    stamp.received.saturating_duration_since(old.updated),
                );
            }
            books.insert(key, book);
            true
        }
        feed::Event::Top { market, bids, asks } => {
            let key = (venue, market);
            // 还没有整本（刚连上）：等整本，不拿 5 档当整本盘口。
            let Some(book) = books.get_mut(&key) else {
                return false;
            };
            let Some(source) = stamp.source_ms else {
                latency::METRICS.count(scope, latency::Counter::TimestampConflict);
                return false;
            };
            if book
                .source_time_ms
                .is_some_and(|previous| source < previous)
            {
                latency::METRICS.count(scope, latency::Counter::Older);
                return false;
            }
            let (next_bids, next_asks) = merge_top(book, source, bids, asks);
            if book.source_time_ms == Some(source) {
                let counter = if next_bids == book.bids && next_asks == book.asks {
                    latency::Counter::Duplicate
                } else {
                    latency::Counter::TimestampConflict
                };
                latency::METRICS.count(scope, counter);
                return false;
            }
            latency::METRICS.observe(
                scope,
                latency::Stage::BookUpdateInterval,
                stamp.received.saturating_duration_since(book.updated),
            );
            book.bids = next_bids;
            book.asks = next_asks;
            book.source_time_ms = Some(source);
            book.updated = stamp.received;
            true
        }
        feed::Event::Bbo {
            market,
            bid,
            ask,
            seed,
        } => {
            let key = (venue, market);
            let Some(source) = stamp.source_ms else {
                latency::METRICS.count(scope, latency::Counter::TimestampConflict);
                return false;
            };
            let old = books.get(&key);
            // 订阅后的初值走 info 查询，可能比先到的 `bbo` 推送还旧：不比已有的新就丢弃，不能盖掉新顶档
            // （之后市场安静下来，旧顶档会被一直信任）。
            if let Some(previous) = old.and_then(|book| book.source_time_ms)
                && source < previous
            {
                latency::METRICS.count(scope, latency::Counter::Older);
                return false;
            }
            let mut next = LocalBook::new(stamp.received);
            next.set_top(bid, ask);
            if let Some(old) = old {
                if old.source_time_ms == Some(source) {
                    let counter = if old.bids == next.bids && old.asks == next.asks {
                        latency::Counter::Duplicate
                    } else {
                        latency::Counter::TimestampConflict
                    };
                    latency::METRICS.count(scope, counter);
                    return false;
                }
                // 对账用的重取：静默的市场重取一次，顶档和已有的不一样，说明中间漏了推送。
                if seed && (old.best_bid() != next.best_bid() || old.best_ask() != next.best_ask())
                {
                    warn!(%venue, market = %key.1, "顶档重取与已有的不一致：漏了推送，已按重取更新");
                }
                latency::METRICS.observe(
                    scope,
                    latency::Stage::BookUpdateInterval,
                    stamp.received.saturating_duration_since(old.updated),
                );
            }
            next.source_time_ms = Some(source);
            next.depth_time_ms = Some(source);
            // 下单报价与开仓前的深度核算读同一份（`arb_venues::live_book`）。
            if let Some(base) = arb_venues::hyperliquid::base_for(venue, &key.1) {
                arb_venues::live_book::set_top(venue, &base, bid, ask, Some(source));
            }
            books.insert(key, next);
            true
        }
        feed::Event::Delta {
            market,
            bids,
            asks,
            begin_nonce,
            nonce,
        } => {
            let key = (venue, market);
            let Some(book) = books.get_mut(&key) else {
                return false;
            };
            if let (Some(last), Some(begin)) = (book.nonce, begin_nonce)
                && last != begin
            {
                debug!(market = %key.1, last, begin, "Lighter 盘口增量不连续，重订阅");
                books.remove(&key);
                if let Some(resub) = resub {
                    let _ = resub.try_send(key.1);
                }
                return true; // Publish the invalidation immediately, not the old quote.
            }
            bids.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.bids, p, q));
            asks.into_iter()
                .for_each(|(p, q)| LocalBook::apply(&mut book.asks, p, q));
            book.nonce = nonce.or(book.nonce);
            latency::METRICS.observe(
                scope,
                latency::Stage::BookUpdateInterval,
                stamp.received.saturating_duration_since(book.updated),
            );
            book.updated = stamp.received;
            true
        }
        feed::Event::Batch(events) => {
            let mut changed = false;
            for event in events {
                changed |= apply_at(books, venue, event, resub, stamp);
            }
            changed
        }
        feed::Event::Error(message) => {
            warn!(%venue, "价差监控：行情服务端报错：{message}");
            false
        }
        feed::Event::Trade { .. } | feed::Event::Other => false,
    };
    if changed {
        latency::METRICS.count(scope, latency::Counter::Applied);
    }
    changed
}

// ───────────────────────────── WebSocket 连接 ─────────────────────────────

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(url: &str) -> anyhow::Result<Ws> {
    if url == feed::HYPERLIQUID_WS {
        hl_shadow::connection_budget().await;
    }
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

/// 一次订阅全部市场的消息（见 [`Protocol::subscribe_all`]）。
type SubscribeAll = fn(&[String]) -> Vec<String>;

/// 一家场所的协议细节。
struct Protocol {
    url: &'static str,
    subscribe: fn(&str) -> Option<String>,
    unsubscribe: fn(&str) -> Option<String>,
    /// 一次订阅全部（Binance：每条消息几十个流；连接每秒最多收 10 条客户端消息）。有它就不逐个订阅。
    subscribe_all: Option<SubscribeAll>,
    parse: fn(&str) -> Result<feed::Event, String>,
    /// 应用层心跳文本；`None` = 用 WebSocket 协议层 ping。
    ping: Option<&'static str>,
    /// 这么久一条消息都没有就当断流重连。
    idle: Duration,
    /// 每条都是整本、推得又密的观察场所（Gate、Bitget、PopDEX 单个市场每秒 2 ~ 6 条）：从原文里认出市场名，
    /// 同一个市场 [`COALESCE`] 内只解析最后一条。观察组每秒才算一次，多解析的都是白费；留最后一条，盘口不会旧。
    coalesce: Option<Coalescer>,
    /// 订阅、重订阅排队按 [`Pacer`] 的限额发（Lighter 主网）。主网「同时在途最多 50 条、每分钟最多
    /// 200 条客户端消息」：近百个市场一口气发完，后面的会被拒（30010 Too Many Inflight Messages），
    /// 被拒的市场一直没有盘口；一批市场同时断档、成批重订阅也一样。RH 到现在没被拒过，照旧一次发完。
    paced: bool,
}

/// Hyperliquid 只订 `bbo` 时的心跳间隔与「判死」上限：`bbo` 只推变化，安静的市场长时间没有推送，
/// 所以连接是否活着靠心跳判断（服务端回 `pong`，往返几毫秒），而不是靠「多久没有行情数据」。
const HL_BBO_PING: Duration = Duration::from_secs(2);
const HL_BBO_IDLE: Duration = Duration::from_secs(6);
/// `bbo` 模式下，一个市场超过这么久一条推送都没有，就重取一次当前值（见 `session`）：安静的市场重取结果
/// 和已有的一样；不一样说明这个市场中间漏了推送，补回来，单个市场的过时因此有上限。
const HL_BBO_RECHECK_QUIET: Duration = Duration::from_secs(30);
/// 每 10 秒一轮最多重取这么多个市场（最久没动静的优先）：市场多、全部静默时，单个市场的过时上限
/// 约 `30 秒 + 市场数 / 20 × 10 秒`。
const HL_BBO_RECHECK_BATCH: usize = 20;

/// 整本行情的合并窗口（见 [`Protocol::coalesce`]）。
const COALESCE: Duration = Duration::from_millis(500);
/// 交易组用的窗口。Bitget / MEXC 单个市场每秒只推 2～6 条，窗口里几乎合并不到东西，窗口多长就白等多久
/// （25ms 时这两家「收到 → 入队」平均 14ms，别家 0.02ms）：只留 5ms，挡一下重连后的成批快照。
const TRADE_COALESCE: Duration = Duration::from_millis(5);
type Coalescer = (fn(&str) -> Option<&str>, Duration);

/// 订阅消息的发送队列（见 [`Protocol::paced`]）：任意 1 秒内不超过 [`Pacer::PER_SECOND`] 条、
/// 任意 60 秒内不超过 [`Pacer::PER_MINUTE`] 条（给心跳留余量），超出的排队等下一次。
#[derive(Default)]
struct Pacer {
    queue: std::collections::VecDeque<String>,
    sent: std::collections::VecDeque<Instant>,
}

impl Pacer {
    const PER_SECOND: usize = 20;
    const PER_MINUTE: usize = 180;
    /// 排队时多久检查一次能不能接着发。
    const TICK: Duration = Duration::from_millis(250);

    /// 排队；同一条已经在队里就不重复排。
    fn push(&mut self, text: String) {
        if !self.queue.contains(&text) {
            self.queue.push_back(text);
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// 此刻按限额能发的那几条（取出即记为已发）。
    fn take(&mut self, now: Instant) -> Vec<String> {
        while self
            .sent
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(60))
        {
            self.sent.pop_front();
        }
        let mut out = Vec::new();
        while !self.queue.is_empty() {
            let last_second = self
                .sent
                .iter()
                .rev()
                .take_while(|at| now.duration_since(**at) < Duration::from_secs(1))
                .count();
            if last_second >= Self::PER_SECOND || self.sent.len() >= Self::PER_MINUTE {
                break;
            }
            out.extend(self.queue.pop_front());
            self.sent.push_back(now);
        }
        out
    }
}

/// 一家场所上的哪种行情。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stream {
    /// 盘口：驱动报价与连接状态。
    Books,
    /// 逐笔成交（只有 Binance，挂单测算用）：不影响连接状态与盘口。
    Trades,
}

fn protocol(venue: Venue, stream: Stream) -> Option<Protocol> {
    let no_unsubscribe: fn(&str) -> Option<String> = |_| None;
    if stream == Stream::Trades {
        return (venue == Venue::Binance).then(|| Protocol {
            url: feed::BINANCE_TRADE_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::binance_trade_subscribe),
            parse: feed::parse_binance,
            ping: None,
            // TradFi 合约周末可能几分钟没有一笔成交；服务端每 3 分钟发一次 ping（也算消息）。
            idle: Duration::from_secs(600),
            coalesce: None,
            paced: false,
        });
    }
    match venue {
        Venue::LighterRh | Venue::Lighter => Some(Protocol {
            url: if venue == Venue::Lighter {
                feed::LIGHTER_MAINNET_WS
            } else {
                feed::LIGHTER_WS
            },
            subscribe: |key| key.parse().ok().map(feed::lighter_subscribe),
            unsubscribe: |key| key.parse().ok().map(feed::lighter_unsubscribe),
            subscribe_all: None,
            parse: feed::parse_lighter,
            // 2 分钟内必须发一帧。
            ping: Some(r#"{"type":"ping"}"#),
            idle: STALE * 2,
            coalesce: None,
            paced: venue == Venue::Lighter,
        }),
        Venue::Arcus => Some(Protocol {
            url: feed::ARCUS_WS,
            subscribe: |key| Some(feed::arcus_subscribe(key)),
            unsubscribe: |_| None,
            subscribe_all: None,
            parse: feed::parse_arcus,
            // Arcus 连接 24 小时自动断，断了就重连。
            ping: None,
            idle: STALE * 2,
            coalesce: None,
            paced: false,
        }),
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => Some(Protocol {
            url: feed::HYPERLIQUID_WS,
            subscribe: |key| Some(feed::hyperliquid_subscribe(key)),
            unsubscribe: |_| None,
            subscribe_all: None,
            parse: feed::parse_hyperliquid,
            // 60 秒没有消息服务端会断开；订阅了就一直有推送，另发应用层 ping 兜底。
            ping: Some(r#"{"method":"ping"}"#),
            idle: STALE * 2,
            coalesce: None,
            paced: false,
        }),
        Venue::Binance => Some(Protocol {
            url: feed::BINANCE_BOOK_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::binance_depth_subscribe),
            parse: feed::parse_binance,
            // 服务端每 3 分钟发 ping、10 分钟内要回 pong（session 里回）；客户端 ping 兜底。
            ping: None,
            // 只推变化，只订 TradFi：周末几十个合约可能一两分钟都不变。服务端每 3 分钟 ping 一次（也算消息），
            // 5 分钟什么都没有就是断了。
            idle: Duration::from_secs(300),
            coalesce: None,
            paced: false,
        }),
        // 观察场所：休市时可能长时间不推，空闲判断放宽到 2 分钟（心跳回执也算消息）。
        Venue::Gate => Some(Protocol {
            url: feed::GATE_WS,
            subscribe: |key| Some(feed::gate_subscribe(key)),
            unsubscribe: no_unsubscribe,
            subscribe_all: None,
            parse: feed::parse_gate,
            ping: Some(r#"{"channel":"futures.ping"}"#),
            idle: Duration::from_secs(120),
            coalesce: Some((feed::gate_market, COALESCE)),
            paced: false,
        }),
        Venue::Bitget => Some(Protocol {
            url: feed::BITGET_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::bitget_subscribe),
            parse: feed::parse_bitget,
            ping: Some("ping"),
            idle: Duration::from_secs(120),
            coalesce: Some((feed::bitget_market, COALESCE)),
            paced: false,
        }),
        Venue::Ondo => Some(Protocol {
            url: feed::ONDO_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::ondo_subscribe),
            parse: feed::parse_ondo,
            ping: Some(r#"{"op":"ping"}"#),
            idle: Duration::from_secs(120),
            coalesce: None,
            paced: false,
        }),
        Venue::Mexc => Some(Protocol {
            url: feed::MEXC_WS,
            subscribe: |key| Some(feed::mexc_subscribe(key)),
            unsubscribe: no_unsubscribe,
            subscribe_all: None,
            parse: feed::parse_mexc,
            // 服务端 1 分钟收不到客户端 ping 就断；这里每 30 秒一次，回执 `pong` 也算消息。
            ping: Some(r#"{"method":"ping"}"#),
            idle: Duration::from_secs(120),
            coalesce: Some((feed::mexc_market, COALESCE)),
            paced: false,
        }),
        Venue::Aster => Some(Protocol {
            url: feed::ASTER_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::aster_depth_subscribe),
            parse: feed::parse_binance,
            // 服务端每 5 分钟发协议层 ping（session 里回 pong），15 分钟没回才断；不用应用层心跳。
            ping: None,
            idle: Duration::from_secs(120),
            coalesce: Some((feed::aster_market, COALESCE)),
            paced: false,
        }),
        Venue::PopDex => Some(Protocol {
            url: feed::POPDEX_WS,
            subscribe: |_| None,
            unsubscribe: no_unsubscribe,
            subscribe_all: Some(feed::popdex_subscribe),
            parse: feed::parse_popdex,
            ping: Some("ping"),
            idle: Duration::from_secs(120),
            coalesce: Some((feed::popdex_market, COALESCE)),
            paced: false,
        }),
        _ => None,
    }
}

fn protocol_for_role(venue: Venue, stream: Stream, trading: bool) -> Option<Protocol> {
    let mut protocol = protocol(venue, stream)?;
    if trading && let Some((_, period)) = protocol.coalesce.as_mut() {
        *period = TRADE_COALESCE;
    }
    Some(protocol)
}

/// 每轮重算最多刷新多少个正常水平 / 近期价差。
///
/// 以前一换分钟就把缓存全清空，下一轮一次重算全部约 2200 行（7 天中位数，每行要给近万个点排两次序）。
/// 本机实测 0.46 秒，东京机器更慢（线上重算最长 2.98 秒）。这期间主循环停着，各家盘口更新都在排队。
/// 这是 2026-11-05 实测排队 p99 2.5～5 秒、最长 7 秒的来源之一。
///
/// 正常水平是 7 天的中位数，晚几秒刷新没有影响，所以改成分摊到后面的很多轮。
///
/// 只按个数限不够：11-05 13:48 上线每轮 80 个，线上每分钟仍有一轮 350 毫秒（每个约 4 毫秒，
/// 比本机慢一个数量级），当时排队 4000～5000 条。所以再加时间上限 [`HISTORY_REFRESH_TIME`]：
/// 先到哪个算哪个，每轮至少刷新一个，保证总能刷完。
pub const HISTORY_REFRESH_PER_PASS: usize = 80;
/// 每轮重算花在刷新历史统计上的时间上限（见 [`HISTORY_REFRESH_PER_PASS`]）。
pub const HISTORY_REFRESH_TIME: Duration = Duration::from_millis(20);

/// 一轮重算里刷新历史统计的额度：个数与时间，先到哪个算哪个（第一个不看时间）。
pub struct RefreshBudget {
    left: usize,
    used: usize,
    until: Instant,
}

impl RefreshBudget {
    pub fn new(count: usize, time: Duration, now: Instant) -> Self {
        Self {
            left: count,
            used: 0,
            until: now + time,
        }
    }

    /// 还能不能再刷新一个；能就记一笔。
    fn take(&mut self) -> bool {
        if self.left == 0 || (self.used > 0 && Instant::now() >= self.until) {
            return false;
        }
        self.left -= 1;
        self.used += 1;
        true
    }

    /// 这一轮刷新了几个。
    pub fn used(&self) -> usize {
        self.used
    }
}
/// 一轮重算超过这么久算慢轮，记 WARN（带当时排队的消息数）。
const SLOW_PASS: Duration = Duration::from_millis(300);
/// 慢轮里最慢那一轮的（整秒?, 刷新数, 排队数, 等面板锁毫秒）。
type SlowPass = (bool, usize, usize, u64);
/// 慢轮 WARN 最多这么久记一条。
const SLOW_PASS_LOG_EVERY: Duration = Duration::from_secs(10);

/// 按分钟缓存的历史统计（正常水平、近期价差）。
/// - 这一分钟算过的直接用；
/// - 没算过、或是上一分钟算的：`budget` 还有就重算，没有了就先用旧的；从没算过的这一轮先给 `None`。
pub fn cached_history<K: std::hash::Hash + Eq, V: Clone>(
    cache: &mut HashMap<K, (i64, V)>,
    key: K,
    minute: i64,
    budget: &mut RefreshBudget,
    compute: impl FnOnce() -> V,
) -> Option<V> {
    let fresh = cache.get(&key).map(|(at, _)| *at == minute);
    if fresh != Some(true) && budget.take() {
        let value = compute();
        cache.insert(key, (minute, value.clone()));
        return Some(value);
    }
    cache.get(&key).map(|(_, value)| value.clone())
}

/// 从订阅列表里消失的市场多留这么久才真的退订。
///
/// 订阅列表一变，那家的行情连接就整个重连：
/// - 那家所有盘口清空、重拿快照；
/// - 近 5 秒价格历史清零，急行情闸门要重新攒满。
///
/// 市场列表每 5 分钟刷新，总有几个市场在列表里进进出出（上下架状态、身份核对、对面那家短暂缺市场……）。
/// 2026-11-04～05 一天里因此重连：gate 136 次、HL-xyz 114 次、Lighter 93 次、HL-io 90 次。
///
/// 退订多等一会：进进出出的市场一直订着，就不会反复重连。真下架的最多多订 15 分钟，无害。
/// 新增的市场照旧马上重连去订。
pub const SUB_LINGER: Duration = Duration::from_secs(15 * 60);

/// 本次要订的 ∪ 最近 [`SUB_LINGER`] 内要订过的（缓冲期满的从 `seen` 里删掉）。
/// 每家的列表：先按 `wanted` 原来的顺序，再接上还在缓冲期里的（排序）。
pub fn lingering(
    wanted: &HashMap<Venue, Vec<String>>,
    seen: &mut HashMap<(Venue, String), Instant>,
    now: Instant,
) -> HashMap<Venue, Vec<String>> {
    for (venue, keys) in wanted {
        for key in keys {
            seen.insert((*venue, key.clone()), now);
        }
    }
    seen.retain(|_, at| now.saturating_duration_since(*at) < SUB_LINGER);
    let mut out = wanted.clone();
    let mut extra: Vec<&(Venue, String)> = seen
        .keys()
        .filter(|(venue, key)| !wanted.get(venue).is_some_and(|keys| keys.contains(key)))
        .collect();
    extra.sort();
    for (venue, key) in extra {
        out.entry(*venue).or_default().push(key.clone());
    }
    out
}

/// 把各家的订阅列表发给连接任务。只发集合真的变了的那几家（顺序不同不算变）—— 发了那家就重连。
fn send_subscriptions(
    senders: &HashMap<Venue, tokio::sync::watch::Sender<Vec<String>>>,
    wanted: &HashMap<Venue, Vec<String>>,
    seen: &mut HashMap<(Venue, String), Instant>,
    now: Instant,
    why: &str,
) {
    let lists = lingering(wanted, seen, now);
    for (venue, sender) in senders {
        let list = lists.get(venue).cloned().unwrap_or_default();
        let same = {
            let old = sender.borrow();
            old.len() == list.len()
                && old.iter().collect::<std::collections::BTreeSet<_>>()
                    == list.iter().collect::<std::collections::BTreeSet<_>>()
        };
        if !same {
            info!(%venue, markets = list.len(), change = %describe_change(&sender.borrow(), &list), "价差监控：订阅列表变了（{why}），这家的行情连接会重连");
            let _ = sender.send(list);
        }
    }
}

/// 一家场所的行情连接：断线指数退避重连；订阅列表变了就重连（全量重订阅最简单，也不会漏）。
async fn venue_loop(
    venue: Venue,
    stream: Stream,
    mut subs: tokio::sync::watch::Receiver<Vec<String>>,
    mut resub: Option<mpsc::Receiver<String>>,
    tx: mpsc::Sender<Feed>,
    trading: bool,
) {
    let Some(protocol) = protocol_for_role(venue, stream, trading) else {
        return;
    };
    let books = stream == Stream::Books;
    let mut failures = 0u32;
    loop {
        if crate::shutdown::is_draining() {
            return;
        }
        let keys = subs.borrow_and_update().clone();
        if keys.is_empty() {
            // 这家暂时没有要订阅的（等首轮扫描核对身份）：等订阅列表变化。
            if subs.changed().await.is_err() {
                return;
            }
            continue;
        }
        let started = Instant::now();
        match session(
            venue,
            stream,
            &protocol,
            &keys,
            &mut subs,
            resub.as_mut(),
            &tx,
        )
        .await
        {
            Ok(Some(())) => {
                // 订阅列表变了：马上用新列表重连。
                if books {
                    let _ = tx.send(Feed::Up(venue, false)).await;
                }
                failures = 0;
            }
            Ok(None) => return,
            Err(error) => {
                if books {
                    let _ = tx.send(Feed::Up(venue, false)).await;
                }
                if started.elapsed() > Duration::from_secs(60) {
                    failures = 0;
                }
                let wait = backoff(failures);
                failures += 1;
                warn!(
                    "价差监控：{venue} {}断开，{} 秒后重连：{error:#}",
                    if books { "行情" } else { "成交流" },
                    wait.as_secs()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// 解析一条行情送去计算任务。下游没了返回 `false`。
/// 交易所给的来源时间比本机时钟快这么多（毫秒）以上就整帧丢掉（见 [`forward`]）。
const FUTURE_SOURCE_MS: i64 = 5_000;

/// [`forward`] 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Forwarded {
    /// 一条行情（盘口 / 成交）：连接的「最后一次收到数据」从这里算。
    Data,
    /// 心跳回执、订阅确认、解析不了的：不算数据。
    Other,
    /// 下游没了：停。
    Closed,
}

impl Forwarded {
    fn closed(self) -> bool {
        self == Self::Closed
    }
}

async fn forward(
    venue: Venue,
    protocol: &Protocol,
    text: &str,
    mut stamp: latency::Stamp,
    tx: &mpsc::Sender<Feed>,
) -> Forwarded {
    let parsed = if hl_shadow::is_hl(venue) {
        feed::parse_hyperliquid_timed(text)
    } else {
        (protocol.parse)(text).map(|event| (event, None))
    };
    match parsed {
        Ok((feed::Event::Other, _)) => Forwarded::Other,
        // 来源时间比本机快太多（交易所发来一条时钟坏掉的帧）：整帧丢掉。收下的话，按来源时间比新旧的栅栏就被推到
        // 未来，之后所有真实的帧都被当成「更旧的」丢掉，这个市场的盘口一直冻着（空闲计时被别的市场重置，不会自愈）。
        Ok((_, Some(source))) if source > stamp.utc_ms.saturating_add(FUTURE_SOURCE_MS) => {
            latency::METRICS.count(latency::Scope::Venue(venue), latency::Counter::FutureSource);
            Forwarded::Other
        }
        Ok((event, source_ms)) => {
            stamp.source_ms = source_ms;
            stamp.enqueued = Instant::now();
            latency::METRICS.observe(
                latency::Scope::Venue(venue),
                latency::Stage::ReceiveToEnqueue,
                stamp.enqueued.saturating_duration_since(stamp.received),
            );
            latency::METRICS.received(venue, stamp);
            if tx.send(Feed::Book(venue, event, stamp)).await.is_ok() {
                Forwarded::Data
            } else {
                Forwarded::Closed
            }
        }
        Err(error) => {
            // Hyperliquid 只订 bbo 时，解析失败的消息会让这个市场少一次更新（静默市场靠重取兜底，见 `session`）：
            // 不能只在 debug 里吞掉。
            if hl_shadow::is_hl(venue) {
                warn!(%venue, "行情消息解析失败（这个市场会少一次更新）：{error}");
            } else {
                debug!(%venue, "行情消息解析失败：{error}");
            }
            Forwarded::Other
        }
    }
}

/// 一次连接。`Ok(Some(()))` = 订阅列表变了要重连；`Ok(None)` = 停机或下游没了。
#[allow(clippy::too_many_arguments)]
async fn session(
    venue: Venue,
    stream: Stream,
    protocol: &Protocol,
    keys: &[String],
    subs: &mut tokio::sync::watch::Receiver<Vec<String>>,
    mut resub: Option<&mut mpsc::Receiver<String>>,
    tx: &mpsc::Sender<Feed>,
) -> anyhow::Result<Option<()>> {
    let mut ws = connect(protocol.url).await?;
    let hl = protocol.url == feed::HYPERLIQUID_WS;
    // 只订 `bbo`（默认，不用 `l2Book`）：见 [`HL_BBO_PING`]。
    let hl_bbo =
        hl && stream == Stream::Books && feed::hyperliquid_book_mode() == feed::HlBook::Bbo;
    let mut pacer = Pacer::default();
    // Lighter RH 每分钟最多 200 条客户端消息、Hyperliquid 每条连接最多 1000 个订阅：几十个一次发完没问题。
    // Lighter 主网市场多，排队按限额发（见 [`Protocol::paced`]）：边发边照常收消息。
    // Binance 每秒最多 10 条：几十个流合成一两条发，条与条之间隔开。
    match protocol.subscribe_all {
        Some(all) => {
            for text in all(keys) {
                hl_shadow::send(&mut ws, Message::text(text), hl).await?;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        None if protocol.paced => {
            keys.iter()
                .filter_map(|key| (protocol.subscribe)(key))
                .for_each(|text| pacer.push(text));
            for text in pacer.take(Instant::now()) {
                hl_shadow::send(&mut ws, Message::text(text), hl).await?;
            }
        }
        None => {
            for key in keys {
                if let Some(text) = (protocol.subscribe)(key) {
                    hl_shadow::send(&mut ws, Message::text(text), hl).await?;
                }
                // HL：同一条连接再订 fast 顶部（见 feed::Event::Top）。
                if hl && !hl_bbo && feed::hyperliquid_fast_top_enabled() {
                    hl_shadow::send(
                        &mut ws,
                        Message::text(feed::hyperliquid_fast_subscribe(key)),
                        hl,
                    )
                    .await?;
                }
            }
        }
    }
    let mut pace = tokio::time::interval(Pacer::TICK);
    pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if stream == Stream::Books {
        let _ = tx.send(Feed::Up(venue, true)).await;
    }
    let mut ping = tokio::time::interval(if hl_bbo {
        HL_BBO_PING
    } else {
        Duration::from_secs(30)
    });
    ping.tick().await;
    let idle = if hl_bbo { HL_BBO_IDLE } else { protocol.idle };
    // `bbo` 订阅之后不会立刻推当前值（东京实测首条 p90 约 8 秒、最长近 20 秒）：订阅之后用一次 WebSocket `post`
    // 取每个市场当前的最优一档做初值，分批发（每 50ms 最多 20 个，不顶满官方「同时在途 100 条 post」的上限）。
    // 之后每 10 秒看一遍：超过 [`HL_BBO_RECHECK_QUIET`] 一条推送都没有的市场再重取一次。
    let mut seeds: std::collections::VecDeque<String> = if hl_bbo {
        keys.iter()
            .enumerate()
            .map(|(i, key)| feed::hyperliquid_seed_request(key, i))
            .collect()
    } else {
        Default::default()
    };
    let mut seed_index = keys.len();
    let mut last_seen: HashMap<String, Instant> = HashMap::new();
    let connected_at = Instant::now();
    let mut seed_tick = tokio::time::interval(Duration::from_millis(50));
    let mut recheck = tokio::time::interval(Duration::from_secs(10));
    recheck.tick().await;
    let mut last_resub: HashMap<String, Instant> = HashMap::new();
    // 合并中的整本：市场名 → 最后一条原文（见 [`Protocol::coalesce`]）。
    let mut pending: HashMap<String, (String, latency::Stamp)> = HashMap::new();
    let mut flush = tokio::time::interval(protocol.coalesce.map_or(COALESCE, |(_, period)| period));
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // 空闲上限从「最后一次收到行情数据」算，截止时刻固定，不随循环重来：原来每进一次 select 就重新计时，
    // 30 秒一次的心跳计时器、合并刷新计时器、服务端 Ping、心跳回执都会把它重置 —— 连接活着、行情却停推时
    // （币安空闲上限 300 秒）永远发现不了，旧盘口一直当新的用。订阅列表是空的连接没有数据可等，按任何消息算。
    let mut last_data = tokio::time::Instant::now();
    loop {
        tokio::select! {
            message = tokio::time::timeout_at(last_data + idle, ws.next()) => {
                let message = message.map_err(|_| {
                    if hl_bbo {
                        anyhow::anyhow!("{} 秒没收到任何消息（含心跳应答）", idle.as_secs())
                    } else {
                        anyhow::anyhow!("{} 秒没收到行情数据（心跳、订阅回执不算）", idle.as_secs())
                    }
                })?;
                if keys.is_empty() {
                    last_data = tokio::time::Instant::now();
                }
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let stamp = latency::Stamp::now();
                        if hl_bbo {
                            // 只订 `bbo`：任何一条消息（含 `pong`）都说明连接还活着。
                            last_data = tokio::time::Instant::now();
                            if let Some(market) = feed::hyperliquid_market_hint(&text) {
                                last_seen.insert(market.to_string(), Instant::now());
                            }
                        }
                        if let Some(market) = protocol.coalesce.and_then(|(hint, _)| hint(&text)) {
                            pending.insert(market.to_string(), (text.to_string(), stamp));
                            last_data = tokio::time::Instant::now();
                        } else {
                            match forward(venue, protocol, &text, stamp, tx).await {
                                Forwarded::Closed => return Ok(None),
                                Forwarded::Data => last_data = tokio::time::Instant::now(),
                                // 不带行情的消息（`pong`、订阅回执）：连接还活着，告诉主循环（非阻塞：队列满了
                                // 说明主循环积压，心跳不更新才对）。
                                Forwarded::Other => {
                                    if hl_bbo {
                                        let _ = tx.try_send(Feed::Alive(venue));
                                    }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => hl_shadow::send(&mut ws, Message::Pong(payload), hl).await?,
                    Some(Ok(Message::Close(frame))) => anyhow::bail!("服务端关闭连接：{frame:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => anyhow::bail!("连接结束"),
                }
            }
            changed = subs.changed() => {
                if changed.is_err() { return Ok(None); }
                let _ = ws.close(None).await;
                return Ok(Some(()));
            }
            Some(key) = async {
                match resub.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // 同一个市场 10 秒内最多重订阅一次。
                if last_resub.get(&key).is_none_or(|at| at.elapsed() > Duration::from_secs(10)) {
                    last_resub.insert(key.clone(), Instant::now());
                    let texts = [(protocol.unsubscribe)(&key), (protocol.subscribe)(&key)];
                    for text in texts.into_iter().flatten() {
                        if protocol.paced {
                            pacer.push(text);
                        } else {
                            hl_shadow::send(&mut ws, Message::text(text), hl).await?;
                        }
                    }
                }
            }
            _ = recheck.tick(), if hl_bbo => {
                // 每轮最多重取 [`HL_BBO_RECHECK_BATCH`] 个、最久没动静的优先：休市时 xyz 几乎全部市场都静默，
                // 不能每 30 秒把全部市场一起重取一遍（占出站消息预算、挤住读消息的循环，6 秒没读到心跳应答就判死重连）。
                let mut quiet: Vec<(Duration, &String)> = keys
                    .iter()
                    .map(|key| {
                        let age = last_seen
                            .get(key)
                            .map_or(connected_at.elapsed(), |at| at.elapsed());
                        (age, key)
                    })
                    .filter(|(age, _)| *age > HL_BBO_RECHECK_QUIET)
                    .collect();
                quiet.sort_by_key(|(age, _)| std::cmp::Reverse(*age));
                for (_, key) in quiet.into_iter().take(HL_BBO_RECHECK_BATCH) {
                    seeds.push_back(feed::hyperliquid_seed_request(key, seed_index));
                    seed_index += 1;
                    // 排队后先记一笔，免得下一轮检查又排一次。
                    last_seen.insert(key.clone(), Instant::now());
                }
            }
            _ = async {
                if seeds.is_empty() { std::future::pending::<()>().await } else { seed_tick.tick().await; }
            } => {
                for _ in 0..20 {
                    let Some(text) = seeds.pop_front() else { break };
                    hl_shadow::send(&mut ws, Message::text(text), hl).await?;
                }
            }
            _ = pace.tick(), if !pacer.is_empty() => {
                for text in pacer.take(Instant::now()) {
                    hl_shadow::send(&mut ws, Message::text(text), hl).await?;
                }
            }
            _ = flush.tick(), if !pending.is_empty() => {
                for (_, (text, stamp)) in pending.drain() {
                    if forward(venue, protocol, &text, stamp, tx).await.closed() { return Ok(None); }
                }
            }
            _ = ping.tick() => {
                if crate::shutdown::is_draining() { let _ = ws.close(None).await; return Ok(None); }
                match protocol.ping {
                    Some(text) => hl_shadow::send(&mut ws, Message::text(text), hl).await?,
                    None => hl_shadow::send(&mut ws, Message::Ping(Vec::new().into()), hl).await?,
                }
            }
        }
    }
}
