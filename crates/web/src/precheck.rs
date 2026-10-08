//! 策略页后台预检：策略页上列出来的，就是按当前金额、杠杆、模式能下单的。
//!
//! 策略表按扫描快照排名，但「能不能下」要现拉两边盘口、走一遍和预览相同的闸门才知道
//! （仓位超过盘口可吃到的量、吃完深度价差不够、强平距离太近……）。让浏览器对每一行
//! 各发一次预览，会把上游请求数乘上行数和打开页面的人数，所以放在服务端做：
//!
//! - 策略页每次取表时登记一组「关注」（哪对场所、哪个策略、多大金额和杠杆、纸面还是
//!   实盘）。有人在看的按正常节奏查；最近用过的几组参数记在文件里，网页关着、看板重启
//!   也在后台慢一些地继续查 —— 打开页面时结论已经是现成的，不用等第一轮；
//! - 后台逐个对每组关注里**所有**看上去能做的行调用 [`desk::prepare`]（与预览同一个
//!   函数），排名靠前的和选中的那一行先查、重查得勤，其余的慢一些；
//! - 扫描快照已经能判死的（OI 上限、两腿都有买一卖一且价差扣成本不划算）不拉盘口，
//!   直接标不过；
//! - 平仓规则不进缓存键（输入框每敲一下都会变），在展示时拿结论里的强平距离、行里的
//!   入场基差现比；
//! - 拉盘口经过按场所限速的包装：Lighter RH 这类配额很紧的场所单独放慢，碰到限频就
//!   冷却一阵，不和扫描、实盘监控抢配额。冷却期间没查成的不覆盖已有结论。
//!
//! 结论之外，持仓数上限、实盘对账这类「账户级」的关由看板在整张表上单独说明。当日亏损
//! 只有下单时才知道，所以下单前的预览仍然是准的那一个。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arb_core::{
    ArbError, ArbResult, FundingPoint, MarketSnapshot, OrderBook, Settings, Symbol, Venue,
};
use arb_exec::TaskRules;
use arb_exec::cli::{is_book_unavailable, limits_from};
use arb_exec::desk::{self, LeveragePolicy, OpenRequest, Prepared};
use arb_exec::preflight::Limits;
use arb_scanner::ScanReport;
use arb_venues::VenueApi;
use arb_venues::http::looks_rate_limited;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::cache::ScanCache;
use crate::strategy::{PairBoard, PairRow, pair_board};

/// 排名前几的候选（另加选中的那一行）重查得勤一些：操作者最可能点的就是它们。
pub const PRIORITY_ROWS: usize = 12;
/// 同时关注几组（场所对 × 策略 × 金额 × 杠杆 × 模式）。
const MAX_WATCHES: usize = 3;
/// 记住最近用过的几组参数：网页关着也在后台继续查，打开就有现成的结论。
const REMEMBERED: usize = 4;
/// 没人在看的那几组：通过的多久重查一次（要短于 [`PASS_STALE`]，打开时才还算数）。
const IDLE_PASS_REFRESH: Duration = Duration::from_secs(4 * 60);
/// 没人在看的那几组：不过的、没查成的多久重查一次。
const IDLE_REFRESH: Duration = Duration::from_secs(10 * 60);
/// 多久没人取表，关注就过期。页面开着时每 30 秒取一次。
const WATCH_TTL: Duration = Duration::from_secs(4 * 60);
/// 选中的那一行多久重查一次：操作者正要下单，结论要最新。
const FOCUS_REFRESH: Duration = Duration::from_secs(30);
/// 通过的行多久重查一次：列出来的就是「能下单的」，价差薄的几分钟就会变。
const PASS_REFRESH: Duration = Duration::from_secs(90);
/// 通过的结论过了这么久还没重查成，就不再当作能下单（显示为重查中）。
const PASS_STALE: Duration = Duration::from_secs(6 * 60);
/// 靠前的、不过的候选多久重查一次。
const PRIORITY_REFRESH: Duration = Duration::from_secs(120);
/// 其余不过的候选多久重查一次。CEX 对一张表能有两三百行，全部两分钟一轮会一直占着上游。
const BACKGROUND_REFRESH: Duration = Duration::from_secs(300);
/// 没查成（盘口没拉到、超时、限频冷却）的多久重试。
const UNKNOWN_RETRY: Duration = Duration::from_secs(60);
/// 结论最多留多久：过了这个时间还没重查成功，就不再当真。要长于 [`IDLE_REFRESH`]。
const VERDICT_KEEP: Duration = Duration::from_secs(20 * 60);
/// 没活干时多久醒一次（新关注登记时会立刻唤醒）。
const IDLE_TICK: Duration = Duration::from_secs(10);
/// 两次预检之间至少隔多久：一次预检最多两次盘口请求，别连成一串打出去。
const CHECK_GAP: Duration = Duration::from_millis(300);
/// 单次预检的上限（含限速排队）。
const CHECK_TIMEOUT: Duration = Duration::from_secs(45);
/// 碰到限频后这家场所冷却多久。
const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(120);
/// 预检拉几档盘口：与预览、下单一致。
const DEPTH_LEVELS: u32 = 20;

/// 一组关注：策略页上「这对场所、这个策略、这个金额和杠杆、这个模式」。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WatchKey {
    /// `funding` 或 `spread`。
    pub view: String,
    pub a: Venue,
    pub b: Venue,
    pub size: Decimal,
    pub leverage: Decimal,
    #[serde(default)]
    pub margin_mode: arb_exec::MarginMode,
    /// 实盘：杠杆按严格口径（超过上限直接拒绝，不降档）。
    pub live: bool,
}

struct Watch {
    last_seen: Instant,
    /// 当前选中的合约：最先查。
    focus: Option<String>,
}

/// 一条结论对应的输入。方向、金额、杠杆任何一个变了都是另一条结论。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VerdictKey {
    view: String,
    symbol: String,
    long: Venue,
    short: Venue,
    size: Decimal,
    leverage: Decimal,
    margin_mode: arb_exec::MarginMode,
    live: bool,
}

impl VerdictKey {
    fn of(watch: &WatchKey, row: &PairRow) -> Option<Self> {
        Some(Self {
            view: watch.view.clone(),
            symbol: row.symbol.to_string(),
            long: row.long?,
            short: row.short?,
            size: watch.size,
            leverage: watch.leverage,
            margin_mode: watch.margin_mode,
            live: watch.live,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// 走完了预览的全部检查。
    Pass,
    /// 被闸门、校验或平仓规则拦下，或者扫描快照已经能判死。
    Blocked,
    /// 没查成：盘口没拉到、限频冷却、超时。不代表能下，也不代表不能下。
    Unknown,
    /// 在候选里，还没轮到。
    Pending,
}

#[derive(Debug, Clone)]
struct Verdict {
    status: Status,
    reason: String,
    /// 得出这条结论的时间。
    checked_at: Instant,
    /// 最近一次尝试的时间。重查没做成时结论保留，只推后这个时间。
    attempted_at: Instant,
    /// 通过时两腿里更近的开仓强平距离（%）：展示时拿它比爆仓保护门槛。
    liq_distance_pct: Option<Decimal>,
    /// 通过时（价差单）吃完深度后收敛到 0 的净收益（小数）：展示时拿它比收敛目标。
    spread_depth_net: Option<Decimal>,
    /// 最近一次重查没做成的原因（结论仍是上一次的）。
    retry_error: Option<String>,
}

/// 一行在它那组关注里的档位：决定多久重查一次。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    /// 选中的那一行。
    Focus,
    /// 排名前 [`PRIORITY_ROWS`]。
    Priority,
    Background,
}

impl Tier {
    fn of(index: usize, focus_first: bool) -> Self {
        if focus_first && index == 0 {
            Tier::Focus
        } else if index < PRIORITY_ROWS + usize::from(focus_first) {
            Tier::Priority
        } else {
            Tier::Background
        }
    }
}

impl Verdict {
    /// `idle`：这组参数最近用过、但眼下没人在看 —— 放慢，只保证打开时结论还算数。
    fn due(&self, now: Instant, tier: Tier, idle: bool) -> bool {
        let every = if idle {
            if self.status == Status::Pass && self.retry_error.is_none() {
                IDLE_PASS_REFRESH
            } else {
                IDLE_REFRESH
            }
        } else if tier == Tier::Focus {
            FOCUS_REFRESH
        } else if self.status == Status::Unknown || self.retry_error.is_some() {
            UNKNOWN_RETRY
        } else if self.status == Status::Pass {
            PASS_REFRESH
        } else if tier == Tier::Priority {
            PRIORITY_REFRESH
        } else {
            BACKGROUND_REFRESH
        };
        now.duration_since(self.attempted_at) >= every
    }
}

/// 一行的预检结论。
#[derive(Debug, Clone, Serialize)]
pub struct RowCheck {
    pub status: Status,
    /// 拦下的原因、没查成的原因，或通过时的简短说明。
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// 距离这次检查过去了多少秒。还没查、或按扫描快照判的，为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_s: Option<u64>,
    /// 最近一次重查没做成的原因：结论是上一次的。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_error: Option<String>,
}

/// 整张表的预检信息。
#[derive(Debug, Serialize)]
pub struct BoardCheck {
    pub margin_mode: arb_exec::MarginMode,
    pub size: Decimal,
    pub leverage: Decimal,
    pub live: bool,
    /// 预检没在跑的原因（实盘没连这对场所、快照过期……）：这张表上没有能下单的。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<String>,
    /// 账户级的关（持仓数到上限、实盘对账不干净）：预检照常跑，但眼下哪一行都下不了。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_block: Option<String>,
    /// 不拦单、但要让操作者知道的（实盘只读）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// 这张表里要现拉盘口预检的行数。
    pub covered: usize,
    /// 其中已经有结论的行数。
    pub checked: usize,
    /// 其中还没轮到的行数。
    pub pending: usize,
    /// 选中的那一行正在等重查（结论已经不够新）：页面该很快再取一次。
    pub focus_refreshing: bool,
    /// 合约（`BASE/QUOTE`）→ 结论。只含有结论或排队中的行。
    pub rows: BTreeMap<String, RowCheck>,
}

impl BoardCheck {
    /// 预检没在跑：只带原因，不带结论。
    pub fn paused(key: &WatchKey, reason: impl Into<String>) -> Self {
        Self {
            size: key.size,
            leverage: key.leverage,
            live: key.live,
            margin_mode: key.margin_mode,
            paused: Some(reason.into()),
            account_block: None,
            note: None,
            covered: 0,
            checked: 0,
            pending: 0,
            focus_refreshing: false,
            rows: BTreeMap::new(),
        }
    }
}

/// 看板上显示为「可以做」的行。与策略页 Top 机会同一个口径：
/// 资金费——有机会、没被入场基差门槛挡下、没被可信度筛查排除；
/// 价差——有方向、标记价基差为正、没被排除。
fn looks_tradable(board: &PairBoard, row: &PairRow) -> bool {
    row.excluded.is_none()
        && if board.view == "spread" {
            row.long.is_some()
                && row.short.is_some()
                && row.basis_pct.is_some_and(|basis| basis > Decimal::ZERO)
        } else {
            row.opportunity.is_some() && row.gated.is_none()
        }
}

/// 扫描快照已经能判死的：不必拉盘口。
///
/// - 至少一条腿触及持仓量上限：闸门会拒（只是它排在拉完盘口之后）；
/// - 价差：两腿都有买一卖一，可成交价差已经不为正，或扣掉往返成本已经不划算 ——
///   预览里是同两道检查，只是按现拉的盘口（与快照最多差一个扫描周期）。
fn snapshot_block(board: &PairBoard, row: &PairRow) -> Option<String> {
    let op = row.opportunity.as_ref();
    if op.is_some_and(|op| op.oi_capped) || row.a.oi_capped || row.b.oi_capped {
        return Some("至少一条腿已触及持仓量上限（OI 上限）：只能减仓，开不了新仓".into());
    }
    if board.view != "spread" {
        return None;
    }
    let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(3).normalize();
    if let Some(basis) = row.executable_basis_pct
        && basis <= Decimal::ZERO
    {
        return Some(format!(
            "按本轮扫描的买一卖一，可成交价差 {}% 不为正：空腿卖得出的价不高于多腿要买的价",
            basis.round_dp(3).normalize()
        ));
    }
    if let Some(op) = op
        && !op.spread_unknown
        && !op.spread_profitable()
    {
        return Some(format!(
            "按本轮扫描的买一卖一，可成交价差 {}% 扣掉平仓穿价与往返手续费后不划算（净 {}%）",
            op.executable_basis_pct
                .map_or("—".to_string(), |basis| basis
                    .round_dp(3)
                    .normalize()
                    .to_string()),
            pct(op.spread_net)
        ));
    }
    None
}

/// 通过的结论里、展示时拿来比平仓规则的数字。
#[derive(Debug, Clone, Copy, Default)]
struct PassFacts {
    liq_distance: Option<Decimal>,
    spread_depth_net: Option<Decimal>,
}

/// 平仓规则在这一行上成不成立。`facts` 只有通过的结论才有。
///
/// 与 [`desk::prepare`] 里的检查同一个口径：基差收敛目标不能不低于入场基差、收敛到目标
/// 时扣完成本还得有得赚；爆仓保护门槛要低于开仓强平距离。
fn rule_block(
    row: &PairRow,
    rules: &TaskRules,
    facts: Option<PassFacts>,
    mode: arb_exec::MarginMode,
) -> Option<String> {
    let liq_distance = facts.and_then(|facts| facts.liq_distance);
    if let Some(target) = rules.basis_exit_pct {
        // 价差视角里行上不一定有资金费机会（费差方向不利时扫描器不出这一条），
        // 这时用行上的标记价基差 —— 同一个标记价、同一个公式。
        let entry = row
            .opportunity
            .as_ref()
            .and_then(|op| op.entry_basis_pct)
            .or(row.basis_pct);
        match entry {
            Some(entry) if target >= entry => {
                return Some(format!(
                    "规则不成立：基差收敛目标 {target}% 不低于当前标记价基差 {}%，开仓就会触发平仓",
                    entry.round_dp(3)
                ));
            }
            None => return Some("规则不成立：缺两腿标记价，基差收敛无从评估".into()),
            Some(_) => {}
        }
        if let Some(depth_net) = facts.and_then(|facts| facts.spread_depth_net)
            && let Some(reason) = desk::spread_target_shortfall(depth_net, target)
        {
            return Some(reason);
        }
    }
    // 费差自动平仓门槛不低于当前毛费差年化：开仓第一轮就会被平掉。与 prepare 同一个判断。
    if let Some(min) = rules.min_funding_apr
        && let Some(op) = row.opportunity.as_ref()
        && min >= op.apr
    {
        return Some(format!(
            "规则不成立：费差自动平仓门槛 {}% 不低于当前毛费差年化 {}%，开仓第一轮就会被平掉",
            (min * Decimal::ONE_HUNDRED).round_dp(2).normalize(),
            (op.apr * Decimal::ONE_HUNDRED).round_dp(2).normalize()
        ));
    }
    if facts.is_some() && !rules.is_empty() {
        let probe = TaskRules {
            basis_exit_pct: None,
            ..rules.clone()
        };
        if let Err(reason) = arb_exec::margin::validate_open_rules(mode, &probe, liq_distance) {
            return Some(format!("规则不成立：{reason}"));
        }
    }
    None
}

/// 这张表里要现拉盘口预检的行，按先后排好：选中的那一行，然后按排名。
fn candidates<'a>(board: &'a PairBoard, focus: Option<&str>) -> Vec<&'a PairRow> {
    let eligible =
        |row: &PairRow| looks_tradable(board, row) && snapshot_block(board, row).is_none();
    let mut picked: Vec<&PairRow> = Vec::new();
    let mut seen = HashSet::new();
    if let Some(focus) = focus
        && let Some(row) = board
            .rows
            .iter()
            .find(|row| row.symbol.to_string() == focus && eligible(row))
    {
        seen.insert(focus.to_string());
        picked.push(row);
    }
    for row in board.rows.iter().filter(|row| eligible(row)) {
        if seen.insert(row.symbol.to_string()) {
            picked.push(row);
        }
    }
    picked
}

#[derive(Default)]
struct Inner {
    /// 有人在看的（最近几分钟取过表的）。
    watches: HashMap<WatchKey, Watch>,
    /// 最近用过的几组参数，最近的在前。
    remembered: Vec<WatchKey>,
    verdicts: HashMap<VerdictKey, Verdict>,
}

impl Inner {
    /// 把这组参数排到「最近用过」的最前面。名单变了返回新名单（要写回文件）。
    fn remember(&mut self, key: &WatchKey) -> Option<Vec<WatchKey>> {
        if self.remembered.first() == Some(key) {
            return None;
        }
        self.remembered.retain(|seen| seen != key);
        self.remembered.insert(0, key.clone());
        self.remembered.truncate(REMEMBERED);
        Some(self.remembered.clone())
    }

    /// 丢掉过期的关注；超出上限时丢最久没人看的。
    fn expire(&mut self, now: Instant) {
        self.watches
            .retain(|_, watch| now.duration_since(watch.last_seen) < WATCH_TTL);
        while self.watches.len() > MAX_WATCHES {
            let Some(oldest) = self
                .watches
                .iter()
                .min_by_key(|(_, watch)| watch.last_seen)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.watches.remove(&oldest);
        }
        self.verdicts
            .retain(|_, verdict| now.duration_since(verdict.attempted_at) < VERDICT_KEEP);
    }

    /// 记下一次预检。这次没查成、而上一次的结论还在有效期里：保留上一次的，只推后重试。
    /// 否则限频冷却一来，整片「能下单」的行会一起消失、过一会儿再冒出来。
    fn record(&mut self, key: VerdictKey, verdict: Verdict) {
        if verdict.status == Status::Unknown
            && let Some(previous) = self.verdicts.get_mut(&key)
            && previous.status != Status::Unknown
            && verdict.attempted_at.duration_since(previous.checked_at) < VERDICT_KEEP
        {
            previous.attempted_at = verdict.attempted_at;
            previous.retry_error = Some(verdict.reason);
            return;
        }
        self.verdicts.insert(key, verdict);
    }
}

struct Job {
    key: VerdictKey,
    symbol: Symbol,
}

/// 后台预检。
pub struct Prechecker {
    /// 限速包装过的连接器：预检自己的盘口请求走这里，不影响扫描与下单。
    by_venue: HashMap<Venue, Arc<dyn VenueApi>>,
    inner: Mutex<Inner>,
    wake: Notify,
    /// 「最近用过的参数」存在哪里。`None`（测试）不落盘。
    state_path: Option<PathBuf>,
}

impl Prechecker {
    /// `state_path` 里记着最近用过的几组参数；文件还不存在时从 `seeds`（页面默认参数）开始，
    /// 这样第一次打开页面也不用等。
    pub fn new(apis: &[Arc<dyn VenueApi>], state_path: PathBuf, seeds: Vec<WatchKey>) -> Arc<Self> {
        let by_venue = apis
            .iter()
            .map(|api| {
                let throttled: Arc<dyn VenueApi> = Arc::new(Throttled::new(Arc::clone(api)));
                (api.venue(), throttled)
            })
            .collect();
        let remembered = match std::fs::read_to_string(&state_path) {
            Ok(raw) => serde_json::from_str::<Vec<WatchKey>>(&raw).unwrap_or_else(|error| {
                warn!(path = %state_path.display(), "预检参数文件读不懂，从默认参数开始：{error}");
                seeds.clone()
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => seeds,
            Err(error) => {
                warn!(path = %state_path.display(), "预检参数文件读不了，从默认参数开始：{error}");
                seeds
            }
        };
        let mut remembered = remembered;
        remembered.truncate(REMEMBERED);
        Arc::new(Self {
            by_venue,
            inner: Mutex::new(Inner {
                remembered,
                ..Inner::default()
            }),
            wake: Notify::new(),
            state_path: Some(state_path),
        })
    }

    /// 测试用：不读不写参数文件。
    #[cfg(test)]
    fn with_apis(by_venue: HashMap<Venue, Arc<dyn VenueApi>>) -> Arc<Self> {
        Arc::new(Self {
            by_venue,
            inner: Mutex::new(Inner::default()),
            wake: Notify::new(),
            state_path: None,
        })
    }

    /// 把「最近用过的参数」写回文件。先写临时文件再改名：写到一半断电也不会留下半个文件。
    fn save(&self, remembered: &[WatchKey]) {
        let Some(path) = &self.state_path else {
            return;
        };
        let result = serde_json::to_string_pretty(remembered)
            .map_err(std::io::Error::other)
            .and_then(|body| {
                let tmp = path.with_extension("json.tmp");
                std::fs::write(&tmp, body)?;
                std::fs::rename(&tmp, path)
            });
        if let Err(error) = result {
            warn!(path = %path.display(), "预检参数没写进文件（重启后要重新预检）：{error}");
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // 锁里只做内存读写，不会在持锁时 panic；真中毒了也照样用里面的数据。
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 策略页取表时登记（或续期）一组关注。新登记或换了选中行时立刻唤醒后台。
    pub fn watch(&self, key: WatchKey, focus: Option<String>) {
        let now = Instant::now();
        let (changed, remembered) = {
            let mut inner = self.lock();
            let remembered = inner.remember(&key);
            let changed = match inner.watches.get_mut(&key) {
                Some(watch) => {
                    watch.last_seen = now;
                    let changed = watch.focus != focus;
                    watch.focus = focus;
                    changed
                }
                None => {
                    inner.watches.insert(
                        key,
                        Watch {
                            last_seen: now,
                            focus,
                        },
                    );
                    true
                }
            };
            inner.expire(now);
            (changed, remembered)
        };
        if let Some(remembered) = remembered {
            self.save(&remembered);
        }
        if changed {
            self.wake.notify_one();
        }
    }

    /// 给这张表配上预检结论：快照判死的、规则不成立的直接标不过，候选里还没查到的标排队中。
    pub fn annotate(
        &self,
        board: &PairBoard,
        key: &WatchKey,
        focus: Option<&str>,
        rules: &TaskRules,
    ) -> BoardCheck {
        let now = Instant::now();
        let queued: HashSet<String> = candidates(board, focus)
            .iter()
            .map(|row| row.symbol.to_string())
            .collect();
        let inner = self.lock();
        let mut rows = BTreeMap::new();
        let (mut checked, mut pending) = (0, 0);
        for row in board.rows.iter().filter(|row| looks_tradable(board, row)) {
            let Some(verdict_key) = VerdictKey::of(key, row) else {
                continue;
            };
            let check = if let Some(reason) = snapshot_block(board, row)
                .or_else(|| arb_exec::margin::validate_rules(key.margin_mode, rules).err())
                .or_else(|| {
                    if key.live {
                        arb_exec::margin::validate_venues(key.margin_mode, &[key.a, key.b]).err()
                    } else {
                        None
                    }
                }) {
                RowCheck {
                    status: Status::Blocked,
                    reason,
                    age_s: None,
                    retry_error: None,
                }
            } else {
                let verdict = inner.verdicts.get(&verdict_key);
                if queued.contains(&verdict_key.symbol) {
                    if verdict.is_some() {
                        checked += 1;
                    } else {
                        pending += 1;
                    }
                }
                let mut check = match verdict {
                    // 太久没重查成的「通过」不再当作能下单：行情早变了。
                    Some(verdict)
                        if verdict.status == Status::Pass
                            && now.duration_since(verdict.checked_at) >= PASS_STALE =>
                    {
                        RowCheck {
                            status: Status::Pending,
                            reason: format!(
                                "上次通过是 {} 分钟前，正在重查",
                                now.duration_since(verdict.checked_at).as_secs() / 60
                            ),
                            age_s: Some(now.duration_since(verdict.checked_at).as_secs()),
                            retry_error: verdict.retry_error.clone(),
                        }
                    }
                    Some(verdict) => RowCheck {
                        status: verdict.status,
                        reason: verdict.reason.clone(),
                        age_s: Some(now.duration_since(verdict.checked_at).as_secs()),
                        retry_error: verdict.retry_error.clone(),
                    },
                    None => RowCheck {
                        status: Status::Pending,
                        reason: String::new(),
                        age_s: None,
                        retry_error: None,
                    },
                };
                if check.status != Status::Blocked {
                    // 强平距离、价差净收益只有通过的结论里才有；排队中、没查成的只比
                    // 基差收敛目标与入场基差那一条。
                    let facts = verdict
                        .filter(|verdict| verdict.status == Status::Pass)
                        .map(|verdict| PassFacts {
                            liq_distance: verdict.liq_distance_pct,
                            spread_depth_net: verdict.spread_depth_net,
                        });
                    let applicable = match facts {
                        Some(facts) => rule_block(row, rules, Some(facts), key.margin_mode),
                        None => rule_block(
                            row,
                            &TaskRules {
                                basis_exit_pct: rules.basis_exit_pct,
                                min_funding_apr: rules.min_funding_apr,
                                ..TaskRules::default()
                            },
                            None,
                            key.margin_mode,
                        ),
                    };
                    if let Some(reason) = applicable {
                        check.status = Status::Blocked;
                        check.reason = reason;
                    }
                }
                check
            };
            rows.insert(verdict_key.symbol, check);
        }
        let focus_refreshing = focus
            .filter(|focus| queued.contains(*focus))
            .and_then(|focus| {
                board
                    .rows
                    .iter()
                    .find(|row| row.symbol.to_string() == focus)
            })
            .and_then(|row| VerdictKey::of(key, row))
            .is_some_and(|verdict_key| {
                inner
                    .verdicts
                    .get(&verdict_key)
                    .is_none_or(|verdict| now.duration_since(verdict.attempted_at) >= FOCUS_REFRESH)
            });
        BoardCheck {
            size: key.size,
            leverage: key.leverage,
            live: key.live,
            margin_mode: key.margin_mode,
            paused: None,
            account_block: None,
            note: None,
            covered: queued.len(),
            checked,
            pending,
            focus_refreshing,
            rows,
        }
    }

    /// 启动后台循环。
    pub fn spawn(
        self: &Arc<Self>,
        cache: Arc<ScanCache>,
        settings: Settings,
    ) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move { this.run(cache, settings).await })
    }

    async fn run(&self, cache: Arc<ScanCache>, settings: Settings) {
        let limits = limits_from(&settings);
        // 与下单同一个过期口径：快照卡住时不按陈旧行情下结论。
        let stale_after = Duration::from_secs(settings.scan_interval_sec * 3);
        loop {
            let worked = match cache.get().await {
                Some(snapshot) if snapshot.age() <= stale_after => {
                    self.step(&snapshot.report, &limits).await
                }
                _ => false,
            };
            if worked {
                tokio::time::sleep(CHECK_GAP).await;
                continue;
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(IDLE_TICK) => {}
            }
        }
    }

    /// 查下一个到期的候选。没有要查的返回 `false`。
    async fn step(&self, report: &ScanReport, limits: &Limits) -> bool {
        let Some(job) = self.next_job(report) else {
            return false;
        };
        let verdict = self.check(report, &job, limits).await;
        debug!(
            symbol = %job.symbol,
            long = %job.key.long,
            short = %job.key.short,
            status = ?verdict.status,
            "预检"
        );
        self.lock().record(job.key, verdict);
        true
    }

    /// 下一个该查的。最近有人看的那组关注优先；组内依次是：
    ///
    /// 1. 选中的那一行（到期就查）；
    /// 2. 还没有任何结论的行，按排名 —— 新表先查一遍，能下单的才出得来；
    /// 3. 到期要重查的行，按排名。
    ///
    /// 第 2 步排在重查前面：Lighter 这类限速紧的场所，「通过的 90 秒重查」会吃掉大半额度，
    /// 不先查新的，一张表的第一轮要拖好几倍时间。
    fn next_job(&self, report: &ScanReport) -> Option<Job> {
        let now = Instant::now();
        // 有人在看的按最近取表的先后；然后是最近用过、眼下没人看的（放慢查）。
        let watches: Vec<(WatchKey, Option<String>, bool)> = {
            let mut inner = self.lock();
            inner.expire(now);
            let mut active: Vec<(&WatchKey, &Watch)> = inner.watches.iter().collect();
            active.sort_by_key(|(_, watch)| std::cmp::Reverse(watch.last_seen));
            let mut watches: Vec<(WatchKey, Option<String>, bool)> = active
                .into_iter()
                .map(|(key, watch)| (key.clone(), watch.focus.clone(), false))
                .collect();
            for key in &inner.remembered {
                if !inner.watches.contains_key(key) {
                    watches.push((key.clone(), None, true));
                }
            }
            watches
        };
        // 排名是纯计算：在锁外做完，再一次性对照结论表。
        let boards: Vec<(WatchKey, Option<String>, bool, PairBoard)> = watches
            .into_iter()
            .map(|(key, focus, idle)| {
                let board = pair_board(report, key.a, key.b, &key.view);
                (key, focus, idle, board)
            })
            .collect();
        let inner = self.lock();
        for (key, focus, idle, board) in &boards {
            let picked = candidates(board, focus.as_deref());
            let focus_first = focus
                .as_deref()
                .zip(picked.first())
                .is_some_and(|(focus, row)| row.symbol.to_string() == focus);
            let rows: Vec<(Tier, VerdictKey, &PairRow)> = picked
                .into_iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    let verdict_key = VerdictKey::of(key, row)?;
                    Some((Tier::of(index, focus_first), verdict_key, row))
                })
                .collect();
            let due = |tier: Tier, verdict_key: &VerdictKey| {
                inner
                    .verdicts
                    .get(verdict_key)
                    .is_none_or(|verdict| verdict.due(now, tier, *idle))
            };
            let unchecked = |verdict_key: &VerdictKey| !inner.verdicts.contains_key(verdict_key);
            let next = rows
                .iter()
                .find(|(tier, verdict_key, _)| *tier == Tier::Focus && due(*tier, verdict_key))
                .or_else(|| {
                    rows.iter()
                        .find(|(_, verdict_key, _)| unchecked(verdict_key))
                })
                .or_else(|| {
                    rows.iter()
                        .find(|(tier, verdict_key, _)| due(*tier, verdict_key))
                });
            if let Some((_, verdict_key, row)) = next {
                return Some(Job {
                    key: verdict_key.clone(),
                    symbol: row.symbol.clone(),
                });
            }
        }
        None
    }

    async fn check(&self, report: &ScanReport, job: &Job, limits: &Limits) -> Verdict {
        let request = OpenRequest {
            margin_mode: job.key.margin_mode,
            base: job.symbol.base.clone(),
            quote: Some(job.symbol.quote.clone()),
            long: job.key.long,
            short: job.key.short,
            size: job.key.size,
            leverage: job.key.leverage,
            // 当日盈亏只有下单时才知道；持仓数与平仓规则由看板在展示时另行核对。
            daily_pnl: Decimal::ZERO,
            view: job.key.view.clone(),
            depth_levels: DEPTH_LEVELS,
            rules: TaskRules::default(),
        };
        let policy = if job.key.live {
            LeveragePolicy::Strict
        } else {
            LeveragePolicy::CapToPair
        };
        let outcome = tokio::time::timeout(
            CHECK_TIMEOUT,
            desk::prepare(report, &self.by_venue, &request, 0, limits, policy),
        )
        .await;
        let (mut liq_distance_pct, mut spread_depth_net) = (None, None);
        let (status, reason) = match outcome {
            Err(_) => (
                Status::Unknown,
                "预检超时：盘口请求在限速队列里排得太久".to_string(),
            ),
            Ok(Err(error)) if is_book_unavailable(&error) => {
                (Status::Unknown, format!("{error:#}"))
            }
            Ok(Err(error)) => (Status::Blocked, format!("{error:#}")),
            Ok(Ok(prepared)) => {
                liq_distance_pct = prepared.risk.liq_distance_pct;
                spread_depth_net = prepared.spread.as_ref().map(|edge| edge.depth_net);
                (Status::Pass, pass_note(&prepared))
            }
        };
        let now = Instant::now();
        Verdict {
            status,
            reason,
            checked_at: now,
            attempted_at: now,
            liq_distance_pct,
            spread_depth_net,
            retry_error: None,
        }
    }
}

/// 通过时给一句话：按什么价、多少成本通过的。
fn pass_note(prepared: &Prepared) -> String {
    let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(3).normalize();
    let mut note = format!(
        "按 {} USDT、{}x 走完预览的检查；预估成本 {}%（手续费 + 穿价 + 滑点）",
        prepared.size_usdt.normalize(),
        prepared.leverage.normalize(),
        pct(prepared.plan.expected_cost)
    );
    if prepared.leverage != prepared.requested_leverage {
        note.push_str(&format!(
            "；杠杆按两腿上限压到 {}x",
            prepared.leverage.normalize()
        ));
    }
    if let Some(stability) = &prepared.stability {
        let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(1).normalize();
        note.push_str(&format!(
            "；费差稳定：近 24 小时均值年化 {}%、{}% 的小时为正{}",
            pct(stability.mean_apr_24h),
            pct(stability.positive_share),
            stability.mean_apr_6h.map_or(String::new(), |apr| format!(
                "，近 6 小时均值 {}%",
                pct(apr)
            ))
        ));
    }
    if let Some(edge) = &prepared.spread {
        note.push_str(&format!(
            "；吃完深度后价差 {}%，扣掉平仓穿价与往返手续费净 {}%",
            edge.depth_basis_pct.round_dp(3).normalize(),
            pct(edge.depth_net)
        ));
    }
    note
}

/// 限速包装：同一家场所的盘口请求之间至少隔一段时间；碰到限频就冷却一阵。
///
/// 只管 `fetch_depth`：预检只拉盘口。
struct Throttled {
    inner: Arc<dyn VenueApi>,
    spacing: Duration,
    gate: tokio::sync::Mutex<Gate>,
}

struct Gate {
    /// 下一次请求最早什么时候发。
    next_at: Instant,
    cooldown_until: Option<Instant>,
}

impl Throttled {
    fn new(inner: Arc<dyn VenueApi>) -> Self {
        let spacing = spacing_for(inner.venue());
        Self::with_spacing(inner, spacing)
    }

    fn with_spacing(inner: Arc<dyn VenueApi>, spacing: Duration) -> Self {
        Self {
            inner,
            spacing,
            gate: tokio::sync::Mutex::new(Gate {
                next_at: Instant::now(),
                cooldown_until: None,
            }),
        }
    }
}

/// 各家盘口请求的最小间隔。预检会把一张表上所有候选都查一遍，要按「持续打几分钟」
/// 来定，而不是按突发：
/// - Lighter RH 配额最紧（实测扫描加实盘监控就会碰到 429），放得最慢；Lighter 主网其次；
/// - Aster 按 IP 计权，429 之后会直接升级成封禁（见扫描缓存的说明）；
/// - Arcus 按 IP 计权重 1,500/分钟，盘口 2 ~ 7；Hyperliquid 系共用一个 IP 额度；
/// - 其余 CEX 深度端点配额宽，但同一出口 IP 上还有别的程序在用，也不连发。
fn spacing_for(venue: Venue) -> Duration {
    Duration::from_millis(match venue {
        Venue::LighterRh => 5_000,
        Venue::Lighter => 2_000,
        Venue::Aster | Venue::Arcus | Venue::Variational => 1_000,
        Venue::Hyperliquid | Venue::HyperliquidIo | Venue::HyperliquidXyz => 500,
        _ => 300,
    })
}

#[async_trait]
impl VenueApi for Throttled {
    fn venue(&self) -> Venue {
        self.inner.venue()
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        self.inner.fetch_all().await
    }

    fn supports_funding_history(&self) -> bool {
        self.inner.supports_funding_history()
    }

    fn funding_history_cached(&self, symbol: &Symbol, hours: u32) -> bool {
        self.inner.funding_history_cached(symbol, hours)
    }

    /// 资金费历史也过同一道限速与冷却：它和盘口打的是同一家场所的同一份配额。
    /// 上游有 10 分钟缓存，命中时不打上游，也就不排队。
    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        // 缓存命中的不打上游：不排队、不占配额（否则第一轮之后的每一次重查都白等一个间隔）。
        if self.inner.funding_history_cached(symbol, hours) {
            return self.inner.fetch_funding_history(symbol, hours).await;
        }
        self.pace().await?;
        let result = self.inner.fetch_funding_history(symbol, hours).await;
        self.note_rate_limit(&result).await;
        result
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        self.pace().await?;
        let result = self.inner.fetch_depth(symbol, levels).await;
        self.note_rate_limit(&result).await;
        result
    }
}

impl Throttled {
    /// 排队等到轮到这家场所；冷却期间直接报错，不打上游。
    async fn pace(&self) -> ArbResult<()> {
        let wait = {
            let mut gate = self.gate.lock().await;
            let now = Instant::now();
            if let Some(until) = gate.cooldown_until {
                if now < until {
                    return Err(ArbError::config(format!(
                        "{} 刚碰到限频，预检暂停 {} 秒",
                        self.venue(),
                        until.duration_since(now).as_secs().max(1)
                    )));
                }
                gate.cooldown_until = None;
            }
            // 先占位再睡：并发的请求各自排到自己的时间点，不会同时醒来一起发。
            let at = gate.next_at.max(now);
            gate.next_at = at + self.spacing;
            at.duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }

    /// 碰到限频就让这家场所冷却一阵。
    async fn note_rate_limit<T>(&self, result: &ArbResult<T>) {
        if let Err(error) = result
            && looks_rate_limited(&error.to_string())
        {
            warn!(venue = %self.venue(), "预检碰到限频，冷却 {} 秒", RATE_LIMIT_COOLDOWN.as_secs());
            self.gate.lock().await.cooldown_until = Some(Instant::now() + RATE_LIMIT_COOLDOWN);
        }
    }
}

#[cfg(test)]
mod tests;
