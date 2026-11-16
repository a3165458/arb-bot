//! 价差自动交易：价差监控（全部组：用户配了 API 的场所两两组合）出信号后，按用户在面板上设定的参数自动开价差单。
//! 两条腿都必须是实盘已连接的场所（纸面模式不限）；规则与最早只做 Arcus ↔ Lighter RH 时完全相同。
//!
//! **不另起一条下单路径**：每一笔都走页面下单同一个函数（[`crate::trade::open_for_auto`] →
//! `run_open`），所以对账、按现拉盘口重算深度与净收益、单笔上限、持仓数上限、当日亏损闸门、
//! Telegram `/pause`、连续失败熔断、开仓通知一个不少。这里只多加自动交易自己的限制：
//!
//! - 默认关闭；开启实盘要在请求里显式确认（`confirm = "LIVE"`）；
//! - 同一合约已有敞口就不再新开（不论是不是自动开的）；开了「补仓」时，同合约、同方向的新信号
//!   比那笔仓位实际成交的价差好出门槛，就往那笔上补（[`pick_add`]，最多补几次、总名义有上限）；
//! - 自动开的仓位同时最多 `max_positions` 笔，每个 UTC 日最多 `daily_max_opens` 笔（0 = 不限）；
//! - 每次尝试后该合约冷却；两次开仓之间至少隔 [`MIN_OPEN_GAP`]；
//! - 必须至少带一条退出规则（回到正常基差平仓 / 止盈）：没有退出条件的自动仓位只会一直挂着；
//! - 执行中断（结果未知）立刻关闭自动交易；开仓连续以回滚收场 [`MAX_FAILURES`] 次也关闭。
//!
//! 设置与「自动开过哪些仓」写在价差数据目录的 `auto.json`（0600，原子替换），重启后照旧。

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::AppState;
use crate::rh_spread::{Line, View};
use crate::trade::Mode;

/// 同一合约尝试一次后的冷却（成功或被闸门拒绝）。
const SYMBOL_COOLDOWN: Duration = Duration::from_secs(10 * 60);
/// 挂单一点都没成交就撤了之后，这个合约歇多久再挂。
const MAKER_NOFILL_COOLDOWN: Duration = Duration::from_secs(10 * 60);
/// 每个 UTC 日最多发起几次挂单（不论结果）：没成交的尝试也会往台账里写几行，台账越长每轮对账越慢。
/// 到了就当天不再挂单（进程重启会清零）。
pub const MAX_MAKER_ATTEMPTS_PER_DAY: u32 = 40;
/// 被闸门拒绝后，下一次尝试（任何合约）至少隔这么久。
const REJECT_GAP: Duration = Duration::from_secs(15);
/// 交易台忙（规则轮或手动操作占着锁）：稍后再试。
const BUSY_RETRY: Duration = Duration::from_secs(5);
/// 中断暂停这一轮怎么办（见 [`AutoTrader::interrupted_gate`]）。
#[derive(Debug, PartialEq)]
enum InterruptDecision {
    /// 核实一致：恢复开仓。
    Resume,
    /// 还没核实一致，还在等（带上在等什么）。
    Wait(String),
    /// 等满 [`INTERRUPT_RESUME_LIMIT`] 还没核实一致：关掉自动交易。
    Disable(String),
}

fn interrupt_decision(settled: Result<(), String>, waited: Duration) -> InterruptDecision {
    match settled {
        Ok(()) => InterruptDecision::Resume,
        Err(waiting) if waited >= INTERRUPT_RESUME_LIMIT => InterruptDecision::Disable(waiting),
        Err(waiting) => InterruptDecision::Wait(waiting),
    }
}

/// 执行中断（结果不明）后最多等多久自愈（台账里没有未决单、完整对账干净）；超过就关掉自动交易等人看。
/// 某次实盘 两次都在 1～2 分钟内自愈，可当时一中断就关，没人看见，各停了几个小时。
pub const INTERRUPT_RESUME_LIMIT: Duration = Duration::from_secs(600);

/// 两次自动开仓之间的最小间隔的默认值（旧设置文件没有 [`Settings::open_gap_sec`] 时用它）；
/// 「没下单、等一会儿再试」的重试间隔也用它。
pub const MIN_OPEN_GAP: Duration = Duration::from_secs(60);
/// 开仓连续以回滚收场这么多次就关闭自动交易。
pub const MAX_FAILURES: u32 = 2;
/// 价差监控的快照超过这么久没更新就不下单。
pub(crate) const VIEW_STALE_SEC: i64 = 5;
/// 页面上保留的最近事件条数。
const EVENTS: usize = 60;
/// `auto.json` 里保留的自动开仓记录条数。
const OPENED_KEEP: usize = 300;

/// 用户在面板上设定的参数。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Settings {
    pub enabled: bool,
    pub mode: Mode,
    /// 单腿名义（USDT）。
    pub size_usdt: Decimal,
    /// 杠杆（两腿相同，整数）。
    pub leverage: Decimal,
    /// 触发门槛：回到正常基差的预估净收益（%）。
    pub min_net_pct: Decimal,
    /// 信号要连续保持多少秒。
    pub hold_sec: u64,
    /// 信号保持（毫秒）：填了就代替 `hold_sec`，可以短于 5 秒；0 = 检测到信号就下。`hold_sec` 照旧写着，
    /// 旧版本读回设置时用它；信号保持影子也拿它当「现行规则」对照。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// 两次自动开仓之间至少隔多少秒（全局，不分合约；补仓同样算）。原来写死 60 秒：一波行情同时亮好几个合约时，
    /// 开完一笔整整一分钟什么都不做（11-07 影子回放：60 → 15 秒，弱信号的日利润约多三到四成）。
    /// 同一时刻只能开一笔（交易台的锁），同一合约有仓位不再开，都另有保证。
    #[serde(default = "default_open_gap_sec")]
    pub open_gap_sec: u64,
    /// 强信号不等保持（门槛的倍数，默认关）：回到正常净收益扣掉滑点缓冲后 ≥ 门槛 × 它，就不等 `hold_sec`、
    /// 当场下单。其它条件（历史分位、冷却、限额、名单）照旧；下单前仍按现拉的盘口重算计划，盘口一闪的
    /// 假信号在那一步被挡下。例如 2：门槛 0.05% 时，扣完缓冲 ≥ 0.10% 就立刻下。
    #[serde(default)]
    pub instant_multiple: Option<Decimal>,
    /// 回到同时段正常基差就平仓（基差收敛规则，目标按方向换算）。
    pub back_to_normal: bool,
    /// 止盈（USDT，含资金费，按盘口核对）。
    pub take_profit_usdt: Option<Decimal>,
    /// 爆仓保护（强平距离 %，逐仓）。
    pub liq_protection_pct: Option<Decimal>,
    /// 自动加保证金：任一腿强平距离（%）低于它就往这条腿补保证金（逐仓）。要与上限成对设置。
    #[serde(default)]
    pub auto_margin_pct: Option<Decimal>,
    /// 自动加保证金的累计上限（USDT，每笔仓位一生最多补这么多）。
    #[serde(default)]
    pub auto_margin_max_usdt: Option<Decimal>,
    /// 超时退出：持仓超过这么多小时、且按盘口平掉的亏损（含资金费）不超过
    /// `timeout_max_loss_usdt` 就平仓。两个成对设置；默认关闭。
    #[serde(default)]
    pub timeout_hours: Option<Decimal>,
    #[serde(default)]
    pub timeout_max_loss_usdt: Option<Decimal>,
    /// 补仓：同一合约、同方向的新信号，开仓可成交价差比那笔仓位实际成交的价差至少好这么多（%）
    /// 才往上补。`None` = 不补仓。
    #[serde(default)]
    pub add_min_improve_pct: Option<Decimal>,
    /// 每笔仓位最多补几次（发过单就算，不论成败）。
    #[serde(default = "default_add_max_times")]
    pub add_max_times: u32,
    /// 补完后单腿名义最多是单笔名义（`size_usdt`）的几倍。
    #[serde(default = "default_add_max_multiple")]
    pub add_max_multiple: Decimal,
    /// 止盈按单腿名义的百分比（%）。与 `take_profit_usdt` 都设时取两者里**大的**那个金额。
    /// 固定的小额止盈（0.05 USDT）在手续费、滑点面前等于保本就走，用比例更稳。
    #[serde(default)]
    pub take_profit_pct: Option<Decimal>,
    /// 止盈的盘口核对门槛（USDT）：标记价达到止盈后，按两边盘口平掉的整笔净额（含资金费）为正且 ≥ 它就平，
    /// 不再要求盘口也到止盈金额。`0` = 为正即平；`None` = 盘口也要到止盈金额（以前的做法）。
    /// 写进每笔新仓位的规则时不超过那一笔的止盈金额。
    #[serde(default)]
    pub take_profit_book_min_usdt: Option<Decimal>,
    /// 止损（USDT）：按盘口平掉亏到 −它就整笔平仓。
    #[serde(default)]
    pub stop_loss_usdt: Option<Decimal>,
    /// 滑点缓冲（%）：回到正常净收益要先扣掉它再和门槛比。实盘统计里「预估 vs 实际」的平均差很小。
    #[serde(default)]
    pub slippage_buffer_pct: Decimal,
    /// 往返手续费最多占预估利润（回到正常净收益 + 往返费，即未扣费前的毛利）的比例（%）。`None` = 不过滤。
    /// 例如 50：手续费超过毛利一半的组合 / 合约不做。
    #[serde(default)]
    pub max_fee_share_pct: Option<Decimal>,
    /// 急行情不开仓（%，默认关）：两条腿所在场所里，任何一家的中间价最近 [`crate::rh_spread::tape::MOVE_WINDOW`]
    /// （5 秒）内的波动幅度（最高 − 最低 ÷ 最低）达到它，这个信号不算（开仓、补仓都一样；保持计时也清零，
    /// 行情稳住之后信号还在才重新计）。价差常常是「一边先动、另一边报价滞后」：等第二腿下到，滞后的一边
    /// 已经追上，价差没了，第一腿裸着被行情带走（实盘遇到过：回滚亏了手续费和滑点）。没有近 5 秒的价格历史
    /// （刚启动 / 刚重连）时放行。`None` = 不过滤。
    #[serde(default)]
    pub fast_move_pct: Option<Decimal>,
    /// 按盘口深度定单笔（%，默认关）：「单腿名义」当作**上限**，盘口在限价内吃不下时往下缩 ——
    /// 单笔不超过「第一腿限价内可吃深度」和「第二腿保本价内可吃深度」的这个比例（如 50 = 一半，给下单到
    /// 第二腿到达的两秒留余量）。只在**吃不到**时缩，不为凑出利润而缩；缩完全部闸门照旧再过一遍；缩到不足
    /// [`Self::depth_min_usdt`] 就不做。按 USDT 计的止盈 / 止损随单笔同比例缩小。`None` = 固定单笔。
    /// 见 [`arb_exec::sizing`]。
    #[serde(default)]
    pub depth_fit_pct: Option<Decimal>,
    /// 按盘口深度定单笔时的最小单笔（USDT）：缩到它以下就不做。
    #[serde(default = "default_depth_min")]
    pub depth_min_usdt: Decimal,
    /// 按盘口动态单笔的上限（USDT，默认关）：信号那一刻按监控的单笔阶梯（[`crate::rh_spread::Rung`]），
    /// 在「单腿名义 ～ 这个上限」之间挑扣滑点缓冲后**预估净赚 USDT 最多**、且每档都过现有门槛的那一档。
    /// 只给稳的组合放大：回到正常净收益不到基差日常波动（MAD）的不放大（实盘遇到过：预估 0.18%、MAD 0.5%，
    /// 补仓后浮亏不小）。放大后的单笔仍是下单路径的上限：按盘口深度缩单、全部闸门照旧。
    /// 只管两腿都吃单的新开仓；币安、补仓按原来的名义。`None` = 固定单笔。
    #[serde(default)]
    pub size_max_usdt: Option<Decimal>,
    /// 强制补腿（%，默认关）：第一腿成交后，第二腿在保本价内没吃满（一点没吃到、或只吃到一部分）时，**不回滚**，
    /// 而是把剩下的数量按「两腿锁定价差不低于 −这个数」的最差价追上（如 0.3 = 价差最差到 −0.3%）。追不上才回滚。
    /// **它不消除亏损**：追上的是一笔起点就是负价差的对冲仓位，收敛到目标不一定赚钱、又没有止损，可能长期挂着，
    /// 建议同时设超时退出。只对两腿都吃单的新开仓起作用；补仓、币安挂单开仓不变。`None` = 关（照旧回滚）。
    #[serde(default)]
    pub force_hedge_pct: Option<Decimal>,
    /// 「相对正常基差」（默认关）：不要求开仓可成交价差为正、收敛到 0 也赚钱，只要求回到正常水平
    /// 扣掉成本仍达门槛。**开启时必须同时开「回到正常基差平仓」并设止损。**
    #[serde(default)]
    pub relative_to_normal: bool,
    /// 先下费率明显低的那条腿（默认开，见 [`arb_exec::preflight::LegOrder::CheapFirst`]）：Arcus ↔ Lighter RH
    /// 先下 Lighter（费率 0、周末盘口薄、最容易吃不到）—— 它没成交什么都不花，成交了再去盘口厚的 Arcus 对冲。
    /// 关掉 = 按预估滑点大的那条先下（2026-10-11 之前的做法）。
    #[serde(default = "default_cheap_leg_first")]
    pub cheap_leg_first: bool,
    /// 历史价差分位门槛（%，默认关）：按「历史价差 K 线」（`history_interval`、`history_days`），这个方向
    /// 现在的价差要排在历史前 X% 才开（例如 20 = 只在过去这段时间价差最大的 20% 位置开）——避免开在半山腰。
    /// 历史拉不到、两家对不上时**不开**（不知道就不当成合格）。
    #[serde(default)]
    pub history_gate_pct: Option<Decimal>,
    /// 相对近期偏离门槛（%，默认 0.05）：开仓、补仓（含币安挂单）时，这个方向的可成交价差要比近
    /// [`crate::rh_spread::RECENT_ENTRY_HOURS`] 小时的中位数（[`Line::recent_entry`]）至少高这么多才做。
    /// 「回到正常」按 7 天中位算：价差整体换了水平之后它要几天才跟上，这期间每个读数都像大偏离 ——
    /// 实盘遇到过：开在近 12 小时中位上（只高 0.008%），赌 7 天中位会回来，结果价差继续走宽而亏损。
    /// 回测：0.05% 拦掉的是少数几笔、但含最大的几笔亏损，保留的合计为正。
    /// 近期样本不够（新合约、刚断过数据）时放行。`None` = 不过滤。
    #[serde(default = "default_recent_gate")]
    pub recent_gate_pct: Option<Decimal>,
    /// 往返净边际门槛（%，默认 0.10）：入场可成交价差扣掉往返手续费后至少剩这么多才开仓、补仓。
    /// 离场要付买卖价差和手续费，边际太薄的单刚入场盘口估算净值就是负的，只能靠价差随机回头，回不来就滞留。
    /// 回测：0.10% 挡掉的单合计为负，最近几天挡掉的更明显。
    /// 更高的门槛在长窗口里会挡掉赚钱的单，所以只设这一档。`None` = 不过滤；拿不到往返手续费时放行。
    #[serde(default = "default_min_edge")]
    pub min_edge_pct: Option<Decimal>,
    /// 薄盘口交易所：两条腿里有它的组合用 [`Self::thin_min_entry_pct`] 这条更严的门槛。
    /// Bitget、HL-io 的盘口常常很久不更新，标记价和盘口估算能差 1U 以上，平仓时才发现价差是假的。
    #[serde(default = "default_thin_venues")]
    pub thin_venues: Vec<Venue>,
    /// 薄盘口组合的入场可成交价差下限（%，默认 0.30）。回测薄盘口单：≥0.30% 的合计为正，
    /// 被挡掉的合计为负。`None` = 不额外要求。
    #[serde(default = "default_thin_min_entry")]
    pub thin_min_entry_pct: Option<Decimal>,
    /// 降仓位的合约（base，大写）：单笔名义乘 [`Self::cut_factor`]。这几个是预上市 / 小币，几家交易所各自定价、
    /// 价差没有拉回的锚，会单边漂移：这几个合约回测合计为负，其中有几笔仍未平。
    #[serde(default = "default_cut_symbols")]
    pub cut_symbols: Vec<String>,
    /// 降仓位的比例（默认 0.5，即减半）。
    #[serde(default = "default_cut_factor")]
    pub cut_factor: Decimal,
    /// 扣持仓期资金费（默认开）：「回到正常净收益」先扣掉按两家当前资金费率估的持仓期资金费成本，再和门槛比。
    /// 持仓期按超时退出的小时数算（没设按 24 小时）；只扣成本、不加收入（收入方向说变就变）。拿不到费率时不扣。
    #[serde(default = "default_true")]
    pub deduct_funding: bool,
    #[serde(default = "default_history_interval")]
    pub history_interval: String,
    #[serde(default = "default_history_days")]
    pub history_days: Decimal,
    /// 自动开的仓位同时最多几笔。0 = 不限（仍受账户级 `ARB_MAX_OPEN_POSITIONS` 约束）。
    pub max_positions: usize,
    /// 每个 UTC 日最多自动开几笔。0 = 不限。
    pub daily_max_opens: usize,
    /// 只做这些合约（base，大写）；空 = 全部。
    #[serde(default)]
    pub symbols: Vec<String>,
    /// 不参与自动交易的交易所：两条腿里有它的组合不自动开仓、不补仓；已有仓位的规则照常，提醒照推。
    /// 新接入的场所「正常水平」样本还少时先放这里（2026-10-21 Bitget），样本够了再从面板删掉。
    #[serde(default)]
    pub exclude_venues: Vec<Venue>,
    /// 不参与自动交易的合约（base，大写）：所有组合都不自动开仓、不补仓；已有仓位的规则照常，提醒照推。
    /// 默认 某合约、某合约（2026-11-04 用户要求：上市前合约临近上市，各家换算口径与时间不同，先停新开仓）；
    /// 旧设置文件里没有这一项时也按默认，面板上清空即恢复。
    #[serde(default = "default_exclude_symbols")]
    pub exclude_symbols: Vec<String>,
    /// 币安的组合参与**吃单**自动交易（默认关）。币安的组合平时只观察；开了也只做 [`Self::binance_symbols`]
    /// 里的合约、用币安专用的单笔名义与止损（下面几项），不用上面通用的单腿名义和止损。
    #[serde(default)]
    pub binance_taker: bool,
    /// 币安的组合参与**挂单**自动交易（默认关，只支持实盘）：币安那条腿用只挂单挂在自己的最优价上，
    /// 成交了对面才按成交量吃单对冲；挂不到成交就撤，什么都没发生。一定是「相对正常基差」的单子。
    #[serde(default)]
    pub binance_maker: bool,
    /// 只做币安（默认关）：开着时非币安的组合（Arcus / Hyperliquid / Lighter RH 之间）一律不自动交易。
    /// 自动交易的总开关 `enabled` 管着所有组合；想只跑币安、不让常规组合按通用名义（可能是几百 USDT）下单，
    /// 就开这一项。`symbols` 名单隔离不了：同一个合约（如 某合约）也在别的组合里。
    #[serde(default)]
    pub binance_only: bool,
    /// 币安允许自动交易的合约（base，大写）。**空 = 币安一笔都不做**（开了上面任一开关也一样）。
    #[serde(default)]
    pub binance_symbols: Vec<String>,
    /// 币安单子的单腿名义（USDT）。币安 TradFi 合约买一卖一只有 80～100 USDT，单子要小。
    #[serde(default = "default_binance_size")]
    pub binance_size_usdt: Decimal,
    /// 币安单子的止损（USDT，必填）：开仓价差本来就是负的（结构性基差），偏离继续拉大时要有出口。
    #[serde(default)]
    pub binance_stop_loss_usdt: Option<Decimal>,
    /// 币安自动仓位同时最多几笔。
    #[serde(default = "default_binance_max_positions")]
    pub binance_max_positions: usize,
    /// 挂单最长挂多久（秒）。币安挂出不足 5 秒就撤的单算「无效撤单」（量化规则），所以不低于 5 秒；挂着的时候
    /// 交易台的锁被占着，持仓规则轮要等，所以也不能太长。
    #[serde(default = "default_maker_rest_sec")]
    pub maker_rest_sec: u64,
    /// 挂单的触发门槛：挂单价下「回到正常」的净收益（吃单退出，扣滑点缓冲后）至少这么多（%）。
    /// 比吃单的门槛高，因为挂单成交时往往正是价格在朝不利方向走（逆向选择）。
    #[serde(default = "default_maker_min_net")]
    pub maker_min_net_pct: Decimal,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Mode::Paper,
            size_usdt: Decimal::from(500),
            leverage: Decimal::from(3),
            min_net_pct: Decimal::new(5, 2),
            hold_sec: 10,
            hold_ms: None,
            open_gap_sec: 60,
            instant_multiple: None,
            back_to_normal: true,
            take_profit_usdt: None,
            liq_protection_pct: None,
            auto_margin_pct: None,
            auto_margin_max_usdt: None,
            timeout_hours: None,
            timeout_max_loss_usdt: None,
            add_min_improve_pct: None,
            add_max_times: default_add_max_times(),
            add_max_multiple: default_add_max_multiple(),
            take_profit_pct: None,
            take_profit_book_min_usdt: None,
            stop_loss_usdt: None,
            slippage_buffer_pct: Decimal::ZERO,
            max_fee_share_pct: None,
            fast_move_pct: None,
            depth_fit_pct: None,
            depth_min_usdt: default_depth_min(),
            size_max_usdt: None,
            force_hedge_pct: None,
            relative_to_normal: false,
            cheap_leg_first: default_cheap_leg_first(),
            history_gate_pct: None,
            recent_gate_pct: default_recent_gate(),
            min_edge_pct: default_min_edge(),
            thin_venues: default_thin_venues(),
            thin_min_entry_pct: default_thin_min_entry(),
            cut_symbols: default_cut_symbols(),
            cut_factor: default_cut_factor(),
            deduct_funding: true,
            history_interval: default_history_interval(),
            history_days: default_history_days(),
            max_positions: 1,
            daily_max_opens: 3,
            symbols: Vec::new(),
            exclude_venues: Vec::new(),
            // 全新安装不排除；只有旧设置文件缺这一项时才按 `default_exclude_symbols` 补上。
            exclude_symbols: Vec::new(),
            binance_taker: false,
            binance_maker: false,
            binance_only: false,
            binance_symbols: Vec::new(),
            binance_size_usdt: default_binance_size(),
            binance_stop_loss_usdt: None,
            binance_max_positions: default_binance_max_positions(),
            maker_rest_sec: default_maker_rest_sec(),
            maker_min_net_pct: default_maker_min_net(),
        }
    }
}

fn default_recent_gate() -> Option<Decimal> {
    Some(Decimal::new(5, 2))
}

fn default_min_edge() -> Option<Decimal> {
    Some(Decimal::new(10, 2))
}

fn default_thin_venues() -> Vec<Venue> {
    vec![Venue::Bitget, Venue::HyperliquidIo]
}

fn default_thin_min_entry() -> Option<Decimal> {
    Some(Decimal::new(30, 2))
}

fn default_exclude_symbols() -> Vec<String> {
    ["ANTHROPIC", "OPENAI"]
        .into_iter()
        .map(String::from)
        .collect()
}

fn default_cut_symbols() -> Vec<String> {
    ["OPENAI", "SOFTBANK", "BOT", "OURA"]
        .into_iter()
        .map(String::from)
        .collect()
}

fn default_cut_factor() -> Decimal {
    Decimal::new(5, 1)
}

fn default_true() -> bool {
    true
}

fn default_binance_size() -> Decimal {
    Decimal::from(50)
}

fn default_depth_min() -> Decimal {
    Decimal::from(50)
}

fn default_open_gap_sec() -> u64 {
    MIN_OPEN_GAP.as_secs()
}

fn default_binance_max_positions() -> usize {
    1
}

fn default_maker_rest_sec() -> u64 {
    8
}

fn default_maker_min_net() -> Decimal {
    Decimal::new(10, 2)
}

/// 币安单子单腿名义的上限（USDT）。
pub const BINANCE_MAX_SIZE_USDT: i64 = 100_000;

fn default_cheap_leg_first() -> bool {
    true
}

fn default_history_interval() -> String {
    "1h".into()
}

fn default_history_days() -> Decimal {
    Decimal::from(24)
}

fn default_add_max_times() -> u32 {
    2
}

fn default_add_max_multiple() -> Decimal {
    Decimal::from(3)
}

impl Settings {
    /// 信号保持（毫秒）：`hold_ms` 填了用它，否则 `hold_sec` 秒。
    pub fn hold_millis(&self) -> u64 {
        self.hold_ms
            .unwrap_or_else(|| self.hold_sec.saturating_mul(1000))
    }

    /// 强信号（见 [`Settings::instant_multiple`]）：不等保持。
    pub fn is_strong(&self, candidate: &Candidate) -> bool {
        // 挂单的候选用挂单自己的门槛（比吃单高）：用吃单的门槛比，几乎每个挂单信号都会被当成强信号。
        let threshold = if candidate.maker.is_some() {
            self.maker_min_net_pct
        } else {
            self.min_net_pct
        };
        self.instant_multiple.is_some_and(|multiple| {
            candidate.net_to_normal_pct - self.slippage_buffer_pct >= threshold * multiple
        })
    }

    /// 这个候选要连续保持多少毫秒才下单：强信号不用等；保持设成 0 时一律检测到就下。
    pub fn required_hold_ms(&self, candidate: &Candidate) -> u64 {
        if self.is_strong(candidate) {
            0
        } else {
            self.hold_millis()
        }
    }

    /// 校验。`max_size` 是 `ARB_MAX_POSITION_USDT`（没设上限为 `None`）。
    pub fn validate(&self, max_size: Option<Decimal>) -> Result<(), String> {
        if self.size_usdt < Decimal::from(10) || self.size_usdt > Decimal::from(100_000) {
            return Err("单腿名义必须在 10 到 100000 USDT 之间".into());
        }
        if let Some(max) = max_size
            && self.size_usdt > max
        {
            return Err(format!(
                "单腿名义 {} 超过单笔上限 ARB_MAX_POSITION_USDT={}",
                self.size_usdt.normalize(),
                max.normalize()
            ));
        }
        if !self.leverage.fract().is_zero()
            || self.leverage < Decimal::ONE
            || self.leverage > Decimal::from(10)
        {
            return Err("杠杆必须是 1 到 10 的整数".into());
        }
        if self.min_net_pct < Decimal::new(1, 2) || self.min_net_pct > Decimal::from(5) {
            return Err("触发门槛必须在 0.01% 到 5% 之间".into());
        }
        if !(5..=600).contains(&self.hold_sec) {
            return Err("信号保持时间必须在 5 到 600 秒之间".into());
        }
        if let Some(ms) = self.hold_ms
            && ms > 600_000
        {
            return Err("信号保持时间必须在 0 到 600 秒之间（0 = 检测到信号就下）".into());
        }
        if let Some(multiple) = self.instant_multiple
            && (multiple <= Decimal::ONE || multiple > Decimal::from(20))
        {
            return Err("强信号不等保持的倍数必须大于 1、不超过 20".into());
        }
        if !self.back_to_normal && self.take_profit_usdt.is_none() && self.take_profit_pct.is_none()
        {
            return Err(
                "至少开启一条退出规则（回到正常基差平仓 / 止盈）：没有退出条件的自动仓位只会一直挂着"
                    .into(),
            );
        }
        if let Some(tp) = self.take_profit_usdt
            && (tp <= Decimal::ZERO || tp > Decimal::from(100_000))
        {
            return Err("止盈必须大于 0".into());
        }
        if let Some(pct) = self.liq_protection_pct
            && (pct <= Decimal::ZERO || pct >= Decimal::from(100))
        {
            return Err("爆仓保护必须在 0 到 100% 之间".into());
        }
        if let Some(pct) = self.take_profit_pct
            && (pct <= Decimal::ZERO || pct > Decimal::from(10))
        {
            return Err("按比例止盈必须在 0 到 10% 之间（不含 0）".into());
        }
        if let Some(min) = self.take_profit_book_min_usdt {
            if self.take_profit_usdt.is_none() && self.take_profit_pct.is_none() {
                return Err(
                    "设了止盈的盘口门槛，但没设止盈：门槛只在止盈按标记价达标后才用".into(),
                );
            }
            if min < Decimal::ZERO || min > Decimal::from(100_000) {
                return Err("止盈的盘口门槛不能为负（0 = 盘口为正就平）".into());
            }
        }
        if let Some(loss) = self.stop_loss_usdt
            && (loss <= Decimal::ZERO || loss > Decimal::from(100_000))
        {
            return Err("止损必须大于 0".into());
        }
        if self.slippage_buffer_pct < Decimal::ZERO || self.slippage_buffer_pct > Decimal::ONE {
            return Err("滑点缓冲必须在 0 到 1% 之间".into());
        }
        if let Some(pct) = self.max_fee_share_pct
            && (pct <= Decimal::ZERO || pct > Decimal::from(100))
        {
            return Err("手续费占比上限必须在 0 到 100% 之间（不含 0）".into());
        }
        if let Some(pct) = self.fast_move_pct
            && (pct <= Decimal::ZERO || pct > Decimal::from(5))
        {
            return Err(
                "急行情阈值必须在 0 到 5% 之间（不含 0），如 0.15 = 5 秒内价格动了 0.15% 就不开"
                    .into(),
            );
        }
        if let Some(pct) = self.depth_fit_pct
            && (pct <= Decimal::ZERO || pct > Decimal::ONE_HUNDRED)
        {
            return Err(
                "按盘口深度定单笔的比例必须在 0 到 100% 之间（不含 0），如 50 = 单笔最多占限价内可吃深度的一半"
                    .into(),
            );
        }
        if let Some(pct) = self.force_hedge_pct
            && (pct <= Decimal::ZERO || pct > Decimal::ONE)
        {
            return Err(
                "强制补腿的最差锁定价差必须在 0 到 1% 之间（不含 0），如 0.3 = 价差最差到 −0.3%"
                    .into(),
            );
        }
        if self.depth_min_usdt <= Decimal::ZERO || self.depth_min_usdt > Decimal::from(100_000) {
            return Err("按盘口深度定单笔的最小单笔必须大于 0".into());
        }
        if let Some(max) = self.size_max_usdt {
            if max < self.size_usdt || max > crate::rh_spread::LADDER_MAX_USDT {
                return Err(format!(
                    "动态单笔上限必须在单腿名义 {} 到 {} USDT 之间",
                    self.size_usdt.normalize(),
                    crate::rh_spread::LADDER_MAX_USDT.normalize()
                ));
            }
            if let Some(limit) = max_size
                && max > limit
            {
                return Err(format!(
                    "动态单笔上限 {} 超过单笔上限 ARB_MAX_POSITION_USDT={}",
                    max.normalize(),
                    limit.normalize()
                ));
            }
        }
        if let Some(pct) = self.history_gate_pct
            && (pct <= Decimal::ZERO || pct > Decimal::from(50))
        {
            return Err(
                "历史分位门槛必须在 0 到 50% 之间（不含 0），如 20 = 只在历史价差最大的 20% 开"
                    .into(),
            );
        }
        match crate::spread_chart::INTERVALS
            .iter()
            .find(|i| i.0 == self.history_interval)
        {
            None => return Err("历史 K 线周期只能是 1m / 5m / 15m / 30m / 1h / 4h".into()),
            Some(&(_, _, _, max_days)) => {
                let days = self.history_days.to_f64().unwrap_or(0.0);
                if !(1.0..=max_days).contains(&days) {
                    return Err(format!(
                        "历史天数必须在 1 到 {max_days} 天之间（{} 线）",
                        self.history_interval
                    ));
                }
            }
        }
        if self.relative_to_normal {
            if !self.back_to_normal {
                return Err("「相对正常基差」必须同时开启「回到同时段正常基差就平仓」：赚的就是回到正常那一段".into());
            }
            if self.stop_loss_usdt.is_none() {
                return Err(
                    "「相对正常基差」必须设止损：开仓价差可以为负，偏离继续拉大时要有出口".into(),
                );
            }
        }
        // 规则之间的约束（加保证金要成对、触发线要高于爆仓保护、超时要成对……）与开仓路径用同一个
        // 校验：两边不一致的话，开仓请求会被拒。开仓时的强平距离要到下单时才知道，这里用 100% 占位，
        // 只校验数值与规则之间的约束；真实距离由开仓路径再校验。
        arb_exec::monitor::validate_rules(&self.rules(None), Some(Decimal::ONE_HUNDRED))?;
        if let Some(pct) = self.recent_gate_pct
            && (pct < Decimal::ZERO || pct > Decimal::ONE)
        {
            return Err("相对近期偏离门槛必须在 0 到 1% 之间".into());
        }
        if let Some(pct) = self.min_edge_pct
            && (pct < Decimal::ZERO || pct > Decimal::from(2))
        {
            return Err("往返净边际门槛必须在 0 到 2% 之间".into());
        }
        if let Some(pct) = self.thin_min_entry_pct
            && (pct < Decimal::ZERO || pct > Decimal::from(5))
        {
            return Err("薄盘口入场价差下限必须在 0 到 5% 之间".into());
        }
        if self.cut_factor <= Decimal::ZERO || self.cut_factor > Decimal::ONE {
            return Err("降仓位比例必须大于 0、不超过 1".into());
        }
        if self.cut_symbols.len() > 50
            || self.cut_symbols.iter().any(|s| {
                s.is_empty() || s.len() > 20 || !s.chars().all(|c| c.is_ascii_alphanumeric())
            })
        {
            return Err("降仓位的合约名单最多 50 个，每个只能是字母数字".into());
        }
        if let Some(pct) = self.add_min_improve_pct
            && (pct <= Decimal::ZERO || pct > Decimal::from(5))
        {
            return Err("补仓门槛（价差至少好多少）必须在 0 到 5% 之间（不含 0）".into());
        }
        if !(1..=10).contains(&self.add_max_times) {
            return Err("每笔最多补仓次数必须在 1 到 10 之间".into());
        }
        if self.add_max_multiple <= Decimal::ONE || self.add_max_multiple > Decimal::from(10) {
            return Err("补仓后总名义上限必须是单笔的 1 倍以上、10 倍以内".into());
        }
        if !(5..=600).contains(&self.open_gap_sec) {
            return Err("两次开仓的最小间隔必须在 5 到 600 秒之间".into());
        }
        // 0 = 不限；填了就要在合理范围内（防手误多打几个 0）。
        if self.max_positions > 1000 {
            return Err("同时持仓数必须在 0（不限）到 1000 之间".into());
        }
        if self.daily_max_opens > 10_000 {
            return Err("每日开仓数必须在 0（不限）到 10000 之间".into());
        }
        if self.symbols.len() > 100
            || self.symbols.iter().any(|s| {
                s.is_empty() || s.len() > 20 || !s.chars().all(|c| c.is_ascii_alphanumeric())
            })
        {
            return Err("合约名单只能是字母数字，逗号分隔".into());
        }
        if self.exclude_venues.len() > 32 {
            return Err("排除的交易所最多 32 家".into());
        }
        if self.exclude_symbols.len() > 100
            || self.exclude_symbols.iter().any(|s| {
                s.is_empty() || s.len() > 20 || !s.chars().all(|c| c.is_ascii_alphanumeric())
            })
        {
            return Err("不自动交易的合约只能是字母数字，逗号分隔".into());
        }
        if (self.binance_taker || self.binance_maker)
            && let Some(max) = max_size
            && self.binance_size_usdt > max
        {
            return Err(format!(
                "币安单腿名义 {} 超过单笔上限 ARB_MAX_POSITION_USDT={}",
                self.binance_size_usdt.normalize(),
                max.normalize()
            ));
        }
        self.validate_binance()
    }

    /// 这一行两条腿里在排除名单（[`Self::exclude_venues`]）上的那家。
    /// 这一行的两条腿里有没有薄盘口交易所（见 [`Self::thin_venues`]）。
    /// 第一腿下单前实时复核要再核的入场门槛（见 [`crate::trade::EntryGate`]）：和挑信号用的同一组设置。
    /// 「相对正常基差」的单子入场价差本来可以是负的，不核；两道门槛都没设也是 `None`。
    pub(crate) fn entry_gate(&self, long: Venue, short: Venue) -> Option<crate::trade::EntryGate> {
        if self.relative_to_normal {
            return None;
        }
        let thin = self.thin_venues.contains(&long) || self.thin_venues.contains(&short);
        let gate = crate::trade::EntryGate {
            min_edge_pct: self.min_edge_pct,
            thin_min_entry_pct: self.thin_min_entry_pct.filter(|_| thin),
        };
        (gate != crate::trade::EntryGate::default()).then_some(gate)
    }

    pub(crate) fn thin_venue(&self, line: &Line) -> Option<Venue> {
        [line.a, line.b]
            .into_iter()
            .find(|venue| self.thin_venues.contains(venue))
    }

    pub(crate) fn excluded_venue(&self, line: &Line) -> Option<Venue> {
        [line.a, line.b]
            .into_iter()
            .find(|venue| self.exclude_venues.contains(venue))
    }

    /// 这一行的合约在不自动交易的名单（[`Self::exclude_symbols`]）上。
    pub(crate) fn excluded_symbol(&self, line: &Line) -> bool {
        self.exclude_symbols
            .iter()
            .any(|s| s.eq_ignore_ascii_case(&line.base))
    }

    /// 币安专用设置的校验。两个开关都关着时，名单与数值也要合法（以后开的时候不会才发现）。
    fn validate_binance(&self) -> Result<(), String> {
        if self.binance_symbols.len() > 20
            || self.binance_symbols.iter().any(|s| {
                s.is_empty() || s.len() > 20 || !s.chars().all(|c| c.is_ascii_alphanumeric())
            })
        {
            return Err("币安合约名单只能是字母数字，逗号分隔，最多 20 个".into());
        }
        if self.binance_size_usdt < Decimal::from(10)
            || self.binance_size_usdt > Decimal::from(BINANCE_MAX_SIZE_USDT)
        {
            return Err(format!(
                "币安单腿名义必须在 10 到 {BINANCE_MAX_SIZE_USDT} USDT 之间"
            ));
        }
        if let Some(loss) = self.binance_stop_loss_usdt
            && (loss <= Decimal::ZERO || loss > self.binance_size_usdt)
        {
            return Err("币安止损必须大于 0、且不超过币安单腿名义".into());
        }
        if self.binance_max_positions == 0 || self.binance_max_positions > 20 {
            return Err("币安同时持仓数必须在 1 到 20 之间".into());
        }
        if !(5..=20).contains(&self.maker_rest_sec) {
            return Err("挂单时长必须在 5 到 20 秒之间（币安挂出不足 5 秒就撤算无效撤单）".into());
        }
        if self.maker_min_net_pct < Decimal::new(1, 2) || self.maker_min_net_pct > Decimal::from(5)
        {
            return Err("挂单触发门槛必须在 0.01% 到 5% 之间".into());
        }
        if self.binance_only && !(self.binance_taker || self.binance_maker) {
            return Err(
                "「只做币安」要先开启币安吃单或挂单自动交易：否则这个自动交易什么都不会做".into(),
            );
        }
        if !(self.binance_taker || self.binance_maker) {
            return Ok(());
        }
        if self.binance_symbols.is_empty() {
            return Err("开启币安自动交易必须填写币安合约名单：名单为空 = 币安一笔都不做".into());
        }
        // 币安挂单一定是「相对正常基差」的单子（开仓价差本来就是负的），下单准备强制带止损；
        // 币安吃单和别的组合一样走通用规则，止损不设就跟通用的一样（可以没有）。
        if self.binance_maker
            && self.binance_stop_loss_usdt.is_none()
            && self.stop_loss_usdt.is_none()
        {
            return Err("开启币安挂单必须设币安止损：挂单的单子是「相对正常基差」的，偏离继续拉大时要有出口".into());
        }
        // 不要求全局的「回到同时段正常基差就平仓」：挂单候选自己带着回到正常基差的目标（`maker_candidate`），
        // 币安的单子照样按它平仓；全局那一项只管别的组合，不该因为开了币安挂单就被改掉。
        if self.binance_maker && self.mode != Mode::Live {
            return Err("币安挂单自动交易只支持实盘（纸面券商没有挂单）".into());
        }
        Ok(())
    }

    /// 2026-11-01 起币安和 Bitget / MEXC 一样是普通交易组（两腿吃单、通用规则），不再有币安专用的
    /// 吃单 / 挂单 / 只做币安开关：读进来的旧设置一律关掉。特别是「只做币安」—— 币安组不再是只观察的，
    /// 它开着会把所有组合都挡掉。其余 `binance_*` 字段留着只为读得懂旧的 auto.json，不再起作用。
    pub fn retire_binance(&mut self) {
        self.binance_taker = false;
        self.binance_maker = false;
        self.binance_only = false;
    }

    /// 币安的组合能不能参与这个合约：至少开了一个币安开关，且合约在币安名单里。
    pub fn binance_allows(&self, base: &str) -> bool {
        (self.binance_taker || self.binance_maker)
            && self
                .binance_symbols
                .iter()
                .any(|s| s.eq_ignore_ascii_case(base))
    }
}

impl Settings {
    /// 自动开仓带上的退出与风控规则。`basis_exit_pct` 随信号而定（回到正常基差的目标）。
    pub fn rules(&self, basis_exit_pct: Option<Decimal>) -> arb_exec::TaskRules {
        self.rules_sized(basis_exit_pct, self.size_usdt, self.stop_loss_usdt)
    }

    /// [`Self::rules`]，但按这笔单子自己的单腿名义和止损算（币安的单子比通用的小得多：
    /// 按比例的止盈要跟着名义走，止损用币安自己的）。
    pub fn rules_for(
        &self,
        basis_exit_pct: Option<Decimal>,
        candidate: &Candidate,
    ) -> arb_exec::TaskRules {
        let mut rules = self.rules_for_size(basis_exit_pct, candidate);
        // 币安不公开维持保证金率：带币安腿的仓位算不出强平距离，爆仓保护、自动加保证金无从执行，开仓校验会把整笔
        // 拒掉（11-07 查明币安组因此一笔都没开出来）。用户 11-08 定：带币安腿的仓位不设这两条（规则按整笔仓位，
        // 另一条腿也跟着没有）。止盈、止损等其余规则照旧。
        if candidate.long == Venue::Binance || candidate.short == Venue::Binance {
            rules.liq_protection_pct = None;
            rules.auto_margin_pct = None;
            rules.auto_margin_max_usdt = None;
        }
        rules
    }

    fn rules_for_size(
        &self,
        basis_exit_pct: Option<Decimal>,
        candidate: &Candidate,
    ) -> arb_exec::TaskRules {
        if candidate.binance {
            // 币安没单独设止损就跟通用的一样（通用的也没设就是没有）。
            self.rules_sized(
                basis_exit_pct,
                self.binance_size_usdt,
                self.binance_stop_loss_usdt.or(self.stop_loss_usdt),
            )
        } else {
            // 按盘口放大过的单子，按比例的止盈跟着放大后的名义走。
            self.rules_sized(basis_exit_pct, candidate.size(self), self.stop_loss_usdt)
        }
    }

    fn rules_sized(
        &self,
        basis_exit_pct: Option<Decimal>,
        size: Decimal,
        stop_loss_usdt: Option<Decimal>,
    ) -> arb_exec::TaskRules {
        let take_profit_usdt = self.take_profit_amount_for(size);
        arb_exec::TaskRules {
            basis_exit_pct,
            take_profit_usdt,
            // 门槛不超过这一笔的止盈金额（按比例止盈时小仓位的止盈可能比门槛还低）。
            take_profit_book_min_usdt: self
                .take_profit_book_min_usdt
                .zip(take_profit_usdt)
                .map(|(min, target)| min.min(target)),
            stop_loss_usdt,
            liq_protection_pct: self.liq_protection_pct,
            auto_margin_pct: self.auto_margin_pct,
            auto_margin_max_usdt: self.auto_margin_max_usdt,
            timeout_hours: self.timeout_hours,
            timeout_max_loss_usdt: self.timeout_max_loss_usdt,
            ..arb_exec::TaskRules::default()
        }
    }
}

impl Settings {
    /// 按盘口深度定单笔的参数；没开就是 `None`（固定单笔）。
    pub fn depth_fit(&self) -> Option<arb_exec::sizing::DepthFit> {
        Some(arb_exec::sizing::DepthFit {
            fraction: self.depth_fit_pct? / Decimal::ONE_HUNDRED,
            min_size: self.depth_min_usdt,
        })
    }

    /// 实际用的止盈金额：固定金额与「单腿名义 × 比例」都设时取大的。
    pub fn take_profit_amount(&self) -> Option<Decimal> {
        self.take_profit_amount_for(self.size_usdt)
    }

    /// [`Self::take_profit_amount`]，按给定的单腿名义算比例那一项。
    pub fn take_profit_amount_for(&self, size: Decimal) -> Option<Decimal> {
        let by_pct = self
            .take_profit_pct
            .map(|pct| (size * pct / Decimal::ONE_HUNDRED).round_dp(4));
        match (self.take_profit_usdt, by_pct) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }
}

/// 一笔自动开出来的仓位。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opened {
    pub id: String,
    pub symbol: String,
    pub mode: Mode,
    pub at: DateTime<Utc>,
    /// 这条是往已有仓位上的补仓（不占同时持仓数，但算进每日笔数）。
    #[serde(default)]
    pub add: bool,
    /// 这笔有一条腿在币安（占 [`Settings::binance_max_positions`] 的名额）。
    #[serde(default)]
    pub binance: bool,
    /// 开仓当时的单腿名义（USDT）。旧记录没有这个字段，金额按 0。
    #[serde(default)]
    pub size_usdt: Decimal,
}

/// 写在 `auto.json` 里的累计计数和名义。旧文件没有这块时从 0 开始，不回填。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Tally {
    #[serde(default)]
    opened: u64,
    #[serde(default)]
    unwound: u64,
    #[serde(default)]
    rejected: u64,
    /// 成功开仓的单腿名义合计。记录被裁掉以后这个数还在。
    #[serde(default)]
    opened_usdt: Decimal,
    /// 以回滚收场的尝试，按当时的单腿名义合计。
    #[serde(default)]
    unwound_usdt: Decimal,
}

/// 写在 `auto.json` 里的全部状态。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Persisted {
    #[serde(default)]
    settings: Settings,
    #[serde(default)]
    opened: Vec<Opened>,
    /// 连续以回滚收场的次数（成功一次清零）。
    #[serde(default)]
    failures: u32,
    /// 最近一次被自动关闭的原因。
    #[serde(default)]
    disabled_reason: Option<String>,
    /// 开仓 / 回滚 / 被拒的累计次数与名义。重启不丢。
    #[serde(default)]
    tally: Tally,
}

/// 一条事件（页面上的「最近动作」）。
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub at: DateTime<Utc>,
    pub kind: &'static str,
    pub symbol: Option<String>,
    pub text: String,
}

/// 选中的一笔：方向、目标、信号上的数字。
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub base: String,
    pub long: Venue,
    pub short: Venue,
    /// 基差收敛目标（%，持仓口径：空腿 − 多腿）。
    pub target_pct: Option<Decimal>,
    pub entry_pct: Decimal,
    pub net_to_normal_pct: Decimal,
    pub signal_sec: u64,
    /// 币安的组合（只观察的组，开了币安开关才参与）：用币安专用的单笔名义与止损。
    pub binance: bool,
    /// 币安那条腿挂单开仓（见 [`Settings::binance_maker`]）；`None` = 两腿都吃单。
    pub maker: Option<arb_exec::desk::MakerLeg>,
    /// 最近 5 秒的中间价波动幅度（%）：(多腿所在场所, 空腿所在场所)。`None` = 还没记满窗口。
    /// 不论「急行情不开仓」开没开都带上，开仓日志里记下来，好用实盘结果回头定阈值。
    pub recent_move_pct: Option<(Decimal, Decimal)>,
    /// 这个方向可成交价差近 12 小时的中位数（%），开仓日志里记下来。`None` = 样本不够。
    pub recent_entry_pct: Option<Decimal>,
    /// 已经从 `net_to_normal_pct` 里扣掉的持仓期资金费（%）。`None` = 没扣（关了或拿不到费率）。
    pub funding_cost_pct: Option<Decimal>,
    /// 按盘口放大的单笔（[`Settings::size_max_usdt`]）。`None` = 用原来的名义。
    pub upsized: Option<Upsize>,
}

/// 历史门槛只认这笔候选的（base, 多腿, 空腿）。其它组的失败不能误拦、通过也不能借用。
fn candidate_history_reason(
    candidate: &Candidate,
    enabled: bool,
    history: &HashMap<HoldKey, (Instant, Option<String>)>,
    now: Instant,
) -> Option<String> {
    if !enabled {
        return None;
    }
    match history.get(&candidate.hold_key()) {
        Some((at, verdict)) if now.saturating_duration_since(*at) < HISTORY_RECHECK => {
            verdict.clone()
        }
        _ => Some("本组合历史价差分位还没算出来".to_string()),
    }
}

/// 按盘口放大后的单笔与说明。
#[derive(Debug, Clone, PartialEq)]
pub struct Upsize {
    pub size_usdt: Decimal,
    /// 例如「按盘口放大 200→500 USDT：预估净赚 0.36→0.95 USDT」。
    pub note: String,
}

/// 计时的键：同一个合约可能出现在几组里（同一个多腿、不同的空腿），要分开计。
pub type HoldKey = (String, Venue, Venue);

impl Candidate {
    pub fn hold_key(&self) -> HoldKey {
        (self.base.clone(), self.long, self.short)
    }

    /// 这笔单子的单腿名义：币安的组合用币安专用的（小得多），按盘口放大过的用放大后的，其余用通用的。
    pub fn size(&self, settings: &Settings) -> Decimal {
        if self.binance {
            settings.binance_size_usdt
        } else {
            let size = match &self.upsized {
                Some(upsized) => upsized.size_usdt,
                None => settings.size_usdt,
            };
            // 降仓位的合约（见 [`Settings::cut_symbols`]）：开仓、补仓、规则里的名义都走这里，一处生效。
            if settings.cut_symbols.contains(&self.base) {
                size * settings.cut_factor
            } else {
                size
            }
        }
    }
}

/// 价差页基差是 (a − b)；持仓规则的基差是 (空 − 多)。多 a / 空 b 时持仓基差 = −页面基差，
/// 所以「回到正常」的目标 = −中位数；反方向就是中位数本身。超出规则允许的 ±5% 时不给目标。
pub fn target_for(direction: &str, median: f64) -> Option<Decimal> {
    let raw = if direction == "long_a" {
        -median
    } else {
        median
    };
    let value = Decimal::from_f64_retain(raw)?.round_dp(3);
    (value.abs() <= arb_exec::monitor::MAX_BASIS_EXIT_PCT).then_some(value)
}

/// 更新「达到门槛」的连续计时：这一刻达标的（合约, 方向）开始或继续计时；不达标的连续 [`HOLD_GRACE`]
/// 都没再出现才清掉（`seen` 记每个组合最近一次达标的时刻）。
/// 用自己的门槛计时，不用监控的提醒信号 —— 用户的门槛可能低于提醒门槛。
pub fn track_holds(
    view: &View,
    settings: &Settings,
    since: &mut HashMap<HoldKey, Instant>,
    seen: &mut HashMap<HoldKey, Instant>,
    now: Instant,
) {
    // 有一阵子根本没看（自动交易关着、Telegram 暂停、失败退避、实盘断线时 `step` 提前返回，不更新计时）：
    // 那段时间信号是不是一直达标不知道，恢复后从头计时 —— 不然暂停前出现过的信号恢复的第一秒就算「已保持够」。
    since.retain(|key, _| {
        seen.get(key)
            .is_none_or(|at| now.saturating_duration_since(*at) < HOLD_UNOBSERVED)
    });
    let current: Vec<HoldKey> = view
        .lines
        .iter()
        .filter_map(|line| candidate_of(line, settings).map(|c| c.hold_key()))
        .collect();
    for key in &current {
        since.entry(key.clone()).or_insert(now);
        seen.insert(key.clone(), now);
    }
    since.retain(|key, _| {
        current.contains(key)
            || seen
                .get(key)
                .is_some_and(|at| now.saturating_duration_since(*at) < HOLD_GRACE)
    });
    seen.retain(|key, _| since.contains_key(key));
}

/// 仅按币种过滤的测试入口；生产的历史门槛必须通过 [`pick_checked`] 按完整候选过滤。
#[cfg(test)]
pub fn pick(
    view: &View,
    settings: &Settings,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_sec: &dyn Fn(&Candidate) -> u64,
    busy_symbols: &dyn Fn(&str) -> Option<String>,
) -> Result<Candidate, String> {
    pick_checked(
        view,
        settings,
        now,
        tradable,
        &|c| held_sec(c).saturating_mul(1000),
        &|c| busy_symbols(&c.base),
    )
}

/// 从价差监控的快照里挑一笔。过滤器拿完整候选，不能把某组的历史失败扩大到整个币种。
/// `tradable`：实盘已连接的场所；纸面都能。两条腿都要能下单的组才做。
fn pick_checked(
    view: &View,
    settings: &Settings,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_ms: &dyn Fn(&Candidate) -> u64,
    blocked_candidate: &dyn Fn(&Candidate) -> Option<String>,
) -> Result<Candidate, String> {
    match view.updated_at {
        Some(at) if (0..=VIEW_STALE_SEC * 1000).contains(&(now - at).num_milliseconds()) => {}
        _ => return Err("价差监控的快照不新鲜".into()),
    }
    let mut waiting: Option<String> = None;
    let mut blocked: Option<String> = None;
    for line in &view.lines {
        let Some(mut candidate) = candidate_of(line, settings) else {
            continue;
        };
        // 行情断了的那家，盘口会被清空、这一行不会有报价；这里再挡一次「行情连着但账户没连」的。
        if !tradable(candidate.long) || !tradable(candidate.short) {
            continue;
        }
        if !view.connected.up(candidate.long) || !view.connected.up(candidate.short) {
            continue;
        }
        // 币安资金费结算前后不下币安单：挂单按挂多久算，吃单（和 Bitget / MEXC 一样的普通单）也一样要查订单，
        // 结算时查询会落后好几秒（10-22），所以同样挡。
        let binance_leg = candidate.long == Venue::Binance || candidate.short == Venue::Binance;
        let rest_ms = candidate.maker.as_ref().map(|maker| maker.rest_ms);
        if (binance_leg || rest_ms.is_some())
            && let Some(why) = binance_settlement_reason(now, rest_ms.unwrap_or(0))
        {
            blocked.get_or_insert(format!("{}：{why}", candidate.base));
            continue;
        }
        let held = held_ms(&candidate);
        candidate.signal_sec = held / 1000;
        if !settings.symbols.is_empty() && !settings.symbols.contains(&candidate.base) {
            continue;
        }
        let hold = settings.required_hold_ms(&candidate);
        if held < hold {
            waiting.get_or_insert(format!(
                "{} 信号已保持 {}/{} 秒",
                candidate.base,
                secs_text(held),
                secs_text(hold)
            ));
            continue;
        }
        if let Some(why) = blocked_candidate(&candidate) {
            blocked.get_or_insert(format!("{}：{why}", candidate.base));
            continue;
        }
        return Ok(candidate);
    }
    Err(waiting
        .or(blocked)
        .or_else(|| blocked_signal_reason(view, settings))
        .unwrap_or_else(|| "没有达到门槛的信号".into()))
}

/// 一笔可以往上补仓的仓位（价差套利、持仓中、两腿齐全）。
#[derive(Debug, Clone, PartialEq)]
pub struct AddTarget {
    pub id: String,
    pub base: String,
    pub long: Venue,
    pub short: Venue,
    /// 这笔仓位实际成交的开仓价差（%）：(空腿均价 − 多腿均价) / 两者均值 × 100。
    /// 与信号的开仓可成交价差同一个口径。
    pub entry_spread_pct: Decimal,
    /// 单腿名义（两腿取大）。
    pub notional_usdt: Decimal,
    /// 已经尝试过几次补仓。
    pub adds: u32,
}

/// 补仓：在已有的价差仓位里找一笔，同合约、同方向（同样的多腿、空腿场所）的新信号比它实际成交的
/// 开仓价差至少好 `add_min_improve_pct`，且补的次数、补完的总名义都没到上限。信号本身的门槛
/// （回到正常净收益、保持时间、名单）与新开仓相同。`Err` 是这一刻为什么不补（给页面看）。
///
/// 补完的单腿名义上限取「单笔 × 倍数」与单笔上限 `max_size`（`ARB_MAX_POSITION_USDT`）的较小值：
/// 下单路径也按这个数拦，在这里先挡住，免得每次冷却结束都白做一遍对账与现扫。
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub fn pick_add(
    view: &View,
    settings: &Settings,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_sec: &dyn Fn(&Candidate) -> u64,
    cooling: &dyn Fn(&str) -> Option<String>,
    targets: &[AddTarget],
    max_size: Option<Decimal>,
) -> Result<(Candidate, AddTarget), String> {
    pick_add_checked(
        view,
        settings,
        now,
        tradable,
        &|c| held_sec(c).saturating_mul(1000),
        &|c| cooling(&c.base),
        targets,
        max_size,
    )
}

/// 补仓同样按完整候选核对历史门槛，不能从「已有仓位」的分支绕过。
#[allow(clippy::too_many_arguments)]
fn pick_add_checked(
    view: &View,
    settings: &Settings,
    now: DateTime<Utc>,
    tradable: &dyn Fn(Venue) -> bool,
    held_ms: &dyn Fn(&Candidate) -> u64,
    blocked_candidate: &dyn Fn(&Candidate) -> Option<String>,
    targets: &[AddTarget],
    max_size: Option<Decimal>,
) -> Result<(Candidate, AddTarget), String> {
    let Some(min_improve) = settings.add_min_improve_pct else {
        return Err("没开补仓".into());
    };
    match view.updated_at {
        Some(at) if (0..=VIEW_STALE_SEC * 1000).contains(&(now - at).num_milliseconds()) => {}
        _ => return Err("价差监控的快照不新鲜".into()),
    }
    let mut why: Option<String> = None;
    for line in &view.lines {
        let Some(mut candidate) = candidate_of(line, settings) else {
            continue;
        };
        // 币安挂单的候选不补仓（补仓走的是吃单的路径）；币安吃单的候选和别的组合一样可以补，
        // 按币安自己的单腿名义算上限。
        if candidate.maker.is_some() {
            continue;
        }
        // 补仓不按盘口放大：按原来的单腿名义补。
        candidate.upsized = None;
        let size = candidate.size(settings);
        let multiple = size * settings.add_max_multiple;
        let max_total = max_size.map_or(multiple, |max| max.min(multiple));
        if !tradable(candidate.long) || !tradable(candidate.short) {
            continue;
        }
        if !view.connected.up(candidate.long) || !view.connected.up(candidate.short) {
            continue;
        }
        // 和开仓一样（见 [`pick_checked`]）：币安资金费结算点前后不下币安单 —— 补仓单撞上结算时订单查询
        // 会落后好几秒（10-22），执行中断、自动交易被关，或两腿数量对不上。
        if (candidate.long == Venue::Binance || candidate.short == Venue::Binance)
            && let Some(settle) = binance_settlement_reason(now, 0)
        {
            why.get_or_insert(format!("{}：{settle}", candidate.base));
            continue;
        }
        if !settings.symbols.is_empty() && !settings.symbols.contains(&candidate.base) {
            continue;
        }
        let Some(target) = targets.iter().find(|target| {
            target.base.eq_ignore_ascii_case(&candidate.base)
                && target.long == candidate.long
                && target.short == candidate.short
        }) else {
            continue;
        };
        let held = held_ms(&candidate);
        candidate.signal_sec = held / 1000;
        let hold = settings.required_hold_ms(&candidate);
        if held < hold {
            why.get_or_insert(format!(
                "{} 补仓信号已保持 {}/{} 秒",
                candidate.base,
                secs_text(held),
                secs_text(hold)
            ));
            continue;
        }
        if target.adds >= settings.add_max_times {
            why.get_or_insert(format!(
                "{} 已补 {} 次，到上限",
                candidate.base, target.adds
            ));
            continue;
        }
        if target.notional_usdt + size > max_total {
            let limit = if max_total < multiple {
                format!("单笔上限 ARB_MAX_POSITION_USDT={}", max_total.normalize())
            } else {
                format!(
                    "单笔的 {} 倍（{} USDT）",
                    settings.add_max_multiple.normalize(),
                    max_total.round_dp(0)
                )
            };
            why.get_or_insert(format!(
                "{} 补完单腿约 {} USDT，超过{limit}",
                candidate.base,
                (target.notional_usdt + size).round_dp(0),
            ));
            continue;
        }
        let improve = candidate.entry_pct - target.entry_spread_pct;
        if improve < min_improve {
            why.get_or_insert(format!(
                "{} 新信号可成交价差 {}% 只比原仓位 {}% 好 {}%，不到补仓门槛 {}%",
                candidate.base,
                candidate.entry_pct.round_dp(3),
                target.entry_spread_pct.round_dp(3),
                improve.round_dp(3),
                min_improve.normalize()
            ));
            continue;
        }
        if let Some(cool) = blocked_candidate(&candidate) {
            why.get_or_insert(format!("{}：{cool}", candidate.base));
            continue;
        }
        return Ok((candidate, target.clone()));
    }
    Err(why.unwrap_or_else(|| "已有仓位上没有达到门槛的同方向新信号".into()))
}

/// 这一轮实际用的设置：币安的自动仓位已经到上限（`open_binance` 笔在场）时，把币安的两个开关当作关着；
/// 今天挂单次数到了 [`MAX_MAKER_ATTEMPTS_PER_DAY`] 时只关挂单。币安之外的组合照旧。
pub fn effective_settings(
    settings: &Settings,
    open_binance: usize,
    maker_attempts_today: u32,
) -> std::borrow::Cow<'_, Settings> {
    let full = open_binance >= settings.binance_max_positions
        && (settings.binance_taker || settings.binance_maker);
    let maker_spent = settings.binance_maker && maker_attempts_today >= MAX_MAKER_ATTEMPTS_PER_DAY;
    if full {
        std::borrow::Cow::Owned(Settings {
            binance_taker: false,
            binance_maker: false,
            ..settings.clone()
        })
    } else if maker_spent {
        std::borrow::Cow::Owned(Settings {
            binance_maker: false,
            ..settings.clone()
        })
    } else {
        std::borrow::Cow::Borrowed(settings)
    }
}

/// 一行能不能做、怎么做。纯函数。
/// 自动交易会不会考虑这个组合（不看信号与价差）。开仓、补仓挑候选（[`candidate_of`]）和空闲时提前设
/// 杠杆（[`next_preset`]）都按它筛。
fn considered(line: &Line, settings: &Settings) -> bool {
    // 观察组（没有实盘账户的场所）：自动交易读的快照里本来就没有它们，这里再挡一道。
    if line.note.is_some() || line.watch_only {
        return false;
    }
    // 排除的交易所：开仓、补仓都从这里挑候选，一处挡住。
    if settings.excluded_venue(line).is_some() || settings.excluded_symbol(line) {
        return false;
    }
    // 上市换算口径不同的两腿、上市前 / 盘前合约（见 [`crate::preipo`]）：挂单候选也不走
    // `auto_skip_reason`，这里一起挡。
    if crate::preipo::open_block(&line.base, line.a, line.b).is_some() {
        return false;
    }
    // 只做币安：别的组合一律不做。
    if settings.binance_only && !line.observe_only {
        return false;
    }
    // 只观察的组（Binance）平时不交易。开了币安开关、合约在币安名单里才参与。
    !line.observe_only || settings.binance_allows(&line.base)
}

/// 空闲时下一个要提前设杠杆的（场所, 合约）：自动交易会考虑的组合里（[`considered`]、两家都是实盘已连接
/// 的），回到正常净收益最高的（离信号最近的）先做。同一杠杆设好过的不再做；正在做或没设成的隔
/// [`PRESET_RETRY`] 再试。
fn next_preset(
    view: &View,
    settings: &Settings,
    tradable: &dyn Fn(Venue) -> bool,
    ready: &dyn Fn(Venue) -> bool,
    preset: &HashMap<(Venue, String), PresetMark>,
    now: Instant,
) -> Option<(Venue, String)> {
    let closeness = |line: &Line| {
        let best = line.best.as_ref()?;
        line.legs(best.direction)?.2.net_to_normal_pct
    };
    let mut lines: Vec<&Line> = view
        .lines
        .iter()
        .map(|line| &**line)
        .filter(|line| considered(line, settings) && tradable(line.a) && tradable(line.b))
        .collect();
    lines.sort_by_key(|line| std::cmp::Reverse(closeness(line)));
    lines
        .into_iter()
        .flat_map(|line| [line.a, line.b].map(|venue| (venue, line.base.to_ascii_uppercase())))
        .filter(|(venue, _)| ready(*venue))
        .find(|key| match preset.get(key) {
            None => true,
            Some(mark) if mark.leverage != settings.leverage => true,
            Some(mark) => !mark.done && now.saturating_duration_since(mark.at) >= PRESET_RETRY,
        })
}

/// 这一刻轮不轮得到提前设杠杆（[`AutoTrader::maybe_preset`]）：实盘、没有正在做的、离上一次够久。
fn preset_due(inner: &Inner, settings: &Settings) -> bool {
    settings.mode == Mode::Live
        && !inner
            .arming
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        && !inner
            .preset_last
            .is_some_and(|at| at.elapsed() < PRESET_EVERY)
        && !inner
            .preset_idle
            .is_some_and(|at| at.elapsed() < PRESET_IDLE_RECHECK)
}

/// 这一行回到正常净收益是不是已经到了 `floor`（预判式准备的粗筛：一次比较挡掉绝大多数行）。
fn is_nearly(line: &Line, floor: Decimal) -> bool {
    line.best
        .as_ref()
        .and_then(|best| line.legs(best.direction))
        .and_then(|(_, _, leg)| leg.net_to_normal_pct)
        .is_some_and(|net| net >= floor)
}

/// 预判式准备挑谁（纯函数）：快到门槛（含已经达标）、吃单（不是挂单 / 币安）、两条腿都是已连接的实盘场所且
/// 行情在线、这个合约没冷却的候选里，净收益最高的**一个**。`near_settings` 是把门槛放低到
/// [`ARM_NEAR_RATIO`] 的设置：其余的过滤（名单、排除、规则……）照 [`candidate_of`] 原样过。
///
/// 故意不在这里按「刚准备过」过滤：槽里只有一份准备，最优的刚准备过就该歇着，而不是退而求其次去
/// 给第二名做、把最优的那份覆盖掉。
fn pick_near(
    lines: &[Arc<Line>],
    near_settings: &Settings,
    floor: Decimal,
    live_venues: &[Venue],
    connected: &crate::rh_spread::Connected,
    cooldown: &HashMap<String, Instant>,
    now: Instant,
) -> Option<Candidate> {
    lines
        .iter()
        .filter(|line| is_nearly(line, floor))
        .filter_map(|line| candidate_of(line, near_settings))
        .filter(|c| c.maker.is_none() && !c.binance)
        .filter(|c| {
            live_venues.contains(&c.long)
                && live_venues.contains(&c.short)
                && connected.up(c.long)
                && connected.up(c.short)
        })
        .filter(|c| !cooldown.get(&c.base).is_some_and(|until| *until > now))
        .max_by(|a, b| a.net_to_normal_pct.cmp(&b.net_to_normal_pct))
}

/// 这一行这一刻值不值得自动交易细看（按**还没扣资金费**的行判断）。不值得看的行，在自动交易每次检查的
/// 每一步里本来也都是「不算」，先筛掉就不用把 1800 行逐步各过一遍：
/// - 候选（[`candidate_of`]）要「回到正常净收益 − 滑点缓冲 ≥ 门槛」，面板说明（[`blocked_signal_reason`]）
///   要「≥ 门槛」，提醒摘要只看有信号的行；
/// - 扣资金费只会让净收益变小（成本 ≥ 0），换方向也只会换到扣后更高、但不超过原来最好那个方向的数，
///   所以没扣之前就不到的，扣了更不到。
///
/// 只观察的组（币安）还可能走挂单候选，一律留下。
fn worth_a_look(line: &Line, settings: &Settings) -> bool {
    if line.observe_only {
        return true;
    }
    let Some(best) = line.best.as_ref() else {
        return false;
    };
    if best.signal {
        return true;
    }
    let floor = settings
        .min_net_pct
        .min(settings.min_net_pct + settings.slippage_buffer_pct);
    line.legs(best.direction)
        .and_then(|(_, _, leg)| leg.net_to_normal_pct)
        .is_some_and(|net| net >= floor)
}

/// 同一家两次提前设杠杆至少隔多久：Lighter 两家 [`PRESET_LIGHTER_EVERY`]，其余只受全局 [`PRESET_EVERY`] 限制。
fn preset_gap(venue: Venue) -> Duration {
    match venue {
        Venue::Lighter | Venue::LighterRh => PRESET_LIGHTER_EVERY,
        _ => Duration::ZERO,
    }
}

pub fn candidate_of(line: &Line, settings: &Settings) -> Option<Candidate> {
    // 绝大多数行这一刻根本不到门槛：先用一次比较挡掉，不必逐行过排除名单、上市前名单，再拼一条「为什么
    // 不做」的说明（每次检查 1800 行、好几遍，以前是自动交易最大的一块开销）。结论不变：
    // 两腿都吃单的候选一定要过 `auto_skip_reason`，而不到门槛它必定给出原因（见 [`below_threshold`]）。
    // 只观察的组（币安）还可能走挂单候选，不在这里挡。
    if !line.observe_only && below_threshold(line, settings) {
        return None;
    }
    if !considered(line, settings) {
        return None;
    }
    // 只观察的组（Binance）：先看挂单（币安腿挂单，更便宜），没有挂单信号再看吃单（两腿都吃单）。
    if line.observe_only {
        if settings.binance_maker
            && let Some(candidate) = maker_candidate(line, settings)
        {
            return Some(candidate);
        }
        if !settings.binance_taker {
            return None;
        }
    }
    let best = line.best.as_ref()?;
    let normal = line.normal.as_ref()?;
    let (long, short, leg) = line.legs(best.direction)?;
    let net = leg.net_to_normal_pct?;
    if auto_skip_reason(line, leg, settings).is_some() {
        return None;
    }
    let target_pct = if settings.back_to_normal {
        Some(target_for(best.direction, normal.median)?)
    } else {
        None
    };
    Some(Candidate {
        base: line.base.clone(),
        long,
        short,
        target_pct,
        entry_pct: leg.quote.entry_pct,
        net_to_normal_pct: net,
        signal_sec: 0,
        binance: line.observe_only,
        maker: None,
        recent_move_pct: line
            .recent_move
            .map(|moved| (moved.of(line, long), moved.of(line, short))),
        recent_entry_pct: recent_median(line, best.direction),
        funding_cost_pct: leg.funding_cost_pct,
        upsized: upsize(line, leg, settings),
    })
}

/// 更好的方向没有报价、没有「回到正常净收益」，或者扣滑点缓冲后不到门槛（[`skip_reason`] 的第一条）：
/// 两腿都吃单的候选肯定出不来。不分配、不加锁，给 [`candidate_of`] 先挡一道。
fn below_threshold(line: &Line, settings: &Settings) -> bool {
    let Some(best) = line.best.as_ref() else {
        return true;
    };
    let Some((_, _, leg)) = line.legs(best.direction) else {
        return true;
    };
    leg.net_to_normal_pct
        .is_none_or(|net| net - settings.slippage_buffer_pct < settings.min_net_pct)
}

/// 按盘口动态定单笔（[`Settings::size_max_usdt`]）：在这个方向的单笔阶梯里，从单腿名义往上逐档看，每档都过
/// [`skip_reason`]（门槛扣滑点缓冲、手续费占比、入场硬条件）的前提下，挑扣滑点缓冲后预估净赚 USDT 最多的那档。
/// `None` = 不放大：没开、币安的组合、回到正常净收益不到基差 MAD（不稳）、阶梯没有更大的档、放大了也不更赚。
/// `leg` 是已经扣过资金费的（阶梯每档同样扣过）。
pub(crate) fn upsize(
    line: &Line,
    leg: &crate::rh_spread::Leg,
    settings: &Settings,
) -> Option<Upsize> {
    let max = settings.size_max_usdt?;
    let base = settings.size_usdt;
    if line.observe_only || max <= base {
        return None;
    }
    let net = leg.net_to_normal_pct?;
    let mad = Decimal::from_f64_retain(line.normal.as_ref()?.mad)?;
    if net < mad {
        return None;
    }
    let earn = |size: Decimal, net: Decimal| {
        size * (net - settings.slippage_buffer_pct) / Decimal::ONE_HUNDRED
    };
    let base_earn = earn(base, net);
    let mut best: Option<(Decimal, Decimal)> = None;
    for rung in leg.ladder.iter().filter(|rung| rung.size_usdt > base) {
        if rung.size_usdt > max {
            break;
        }
        let Some(rung_net) = rung.net_to_normal_pct else {
            break;
        };
        let at_rung = crate::rh_spread::Leg {
            quote: crate::rh_spread::DirectionQuote {
                entry_pct: rung.entry_pct,
                exit_cross_pct: leg.quote.exit_cross_pct,
            },
            net_to_zero_pct: rung.net_to_zero_pct,
            net_to_normal_pct: Some(rung_net),
            funding_cost_pct: leg.funding_cost_pct,
            ladder: Vec::new(),
        };
        // 深度越吃越差：某档不过门槛，更大的档也不看了。
        if skip_reason(line, &at_rung, settings).is_some() {
            break;
        }
        let gain = earn(rung.size_usdt, rung_net);
        if gain > best.map_or(base_earn, |(_, best)| best) {
            best = Some((rung.size_usdt, gain));
        }
    }
    let (size, gain) = best?;
    Some(Upsize {
        size_usdt: size,
        note: format!(
            "按盘口放大 {}→{} USDT：预估净赚 {}→{} USDT",
            base.normalize(),
            size.normalize(),
            base_earn.round_dp(2).normalize(),
            gain.round_dp(2).normalize()
        ),
    })
}

/// 币安腿挂单的候选：挂单价下「回到正常」的净收益（吃单退出）扣滑点缓冲后 ≥ `maker_min_net_pct`，
/// 两个方向里取好的那个。目标 = 同时段正常基差（必须有，没有就不做）。
fn maker_candidate(line: &Line, settings: &Settings) -> Option<Candidate> {
    let view = line.maker.as_ref()?;
    let normal = line.normal.as_ref()?;
    [
        ("long_a", view.long_a.as_ref()),
        ("long_b", view.long_b.as_ref()),
    ]
    .into_iter()
    .filter_map(|(direction, leg)| {
        let leg = leg?;
        let net = leg.net_to_normal_taker_exit_pct?;
        if net - settings.slippage_buffer_pct < settings.maker_min_net_pct {
            return None;
        }
        if recent_gate_reason(line, direction, leg.entry_pct, settings).is_some() {
            return None;
        }
        let (long, short) = if direction == "long_a" {
            (line.a, line.b)
        } else {
            (line.b, line.a)
        };
        Some(Candidate {
            base: line.base.clone(),
            long,
            short,
            target_pct: Some(target_for(direction, normal.median)?),
            entry_pct: leg.entry_pct,
            net_to_normal_pct: net,
            signal_sec: 0,
            binance: true,
            maker: Some(arb_exec::desk::MakerLeg {
                venue: view.venue,
                entry_fee: view.maker_fee,
                exit_fee: view.taker_fee,
                rest_ms: settings.maker_rest_sec * 1000,
            }),
            // 币安挂单现在不走「急行情不开仓」（挂单默认关），只记幅度。
            recent_move_pct: line
                .recent_move
                .map(|moved| (moved.of(line, long), moved.of(line, short))),
            recent_entry_pct: recent_median(line, direction),
            funding_cost_pct: None,
            upsized: None,
        })
    })
    .max_by(|a, b| a.net_to_normal_pct.cmp(&b.net_to_normal_pct))
}

/// 自动交易这一刻为什么不做这个方向：[`skip_reason`] 的净收益 / 手续费 / 入场条件，再加「急行情不开仓」。
/// 急行情只管自动开仓与补仓：Telegram 提醒的「下单」按钮是人点的，提示里照实说，但不拦。
pub(crate) fn auto_skip_reason(
    line: &Line,
    leg: &crate::rh_spread::Leg,
    settings: &Settings,
) -> Option<String> {
    let direction = if line
        .long_a
        .as_ref()
        .is_some_and(|own| std::ptr::eq(own, leg))
    {
        "long_a"
    } else {
        "long_b"
    };
    crate::preipo::open_block(&line.base, line.a, line.b)
        .or_else(|| venue_closed_reason(line, Utc::now()))
        .or_else(|| skip_reason(line, leg, settings))
        .or_else(|| recent_gate_reason(line, direction, leg.quote.entry_pct, settings))
        .or_else(|| fast_move_reason(line, settings))
}

/// 有一家现在休市：目前只有 Bitget 的股票 / 商品类合约（5×24，见
/// [`arb_exec::bitget_broker::rwa_session_closed`]）。这时交易所拒新单，下单路径也会挡（不计失败），
/// 在这里先挡掉，免得整个周末每 10 分钟对账、扫一轮再被拒一次。
/// 币安资金费结算点（UTC 整点，每 4 小时：全所 4 小时周期的合约最多，8 小时周期的 00/08/16 也在其中）
/// 前后各这么久不挂币安单。10-22 00:00 UTC 某合约：挂单在结算那一刻撤掉，撤单回包是终态，订单查询
/// 却落后 7 秒以上，执行中断、自动交易被关；部分成交时成交明细也可能这样晚到。
const BINANCE_SETTLEMENT_GUARD_SEC: i64 = 60;
const BINANCE_SETTLEMENT_EVERY_SEC: i64 = 4 * 3600;
/// 挂单寿命 = 挂单时长 + 撤单与读回的余量。
const MAKER_CANCEL_MARGIN_SEC: i64 = 5;

/// 币安挂单这一刻挂出去，整个寿命（挂着 + 撤单读回）会不会碰到结算点前后的窗口。
pub(crate) fn binance_settlement_reason(
    now: chrono::DateTime<Utc>,
    rest_ms: u64,
) -> Option<String> {
    let into = now.timestamp().rem_euclid(BINANCE_SETTLEMENT_EVERY_SEC);
    let lifetime =
        i64::try_from(rest_ms.div_ceil(1000)).unwrap_or(i64::MAX / 2) + MAKER_CANCEL_MARGIN_SEC;
    let guard = BINANCE_SETTLEMENT_GUARD_SEC;
    (into < guard || into + lifetime > BINANCE_SETTLEMENT_EVERY_SEC - guard)
        .then(|| format!("币安资金费结算点（UTC 00/04/08/12/16/20 点）前后 {guard} 秒不下币安单"))
}

pub(crate) fn venue_closed_reason(line: &Line, now: chrono::DateTime<Utc>) -> Option<String> {
    let bitget = line.a == Venue::Bitget || line.b == Venue::Bitget;
    if bitget
        && line.session != crate::rh_spread::Session::All
        && arb_exec::bitget_broker::rwa_session_closed(now)
    {
        return Some(arb_exec::bitget_broker::RWA_CLOSED_NOTE.to_string());
    }
    opening_hour_reason(line, now).or_else(|| lighter_maintenance_reason(line, now))
}

/// 美股开盘头一小时（纽约时间工作日 9:30–10:30）不开股票 / 指数 / 商品类的新仓、不补仓：这一小时盘口乱跳，
/// 开的仓亏损占比明显更高，平仓实际成交比盘口预估差得也更多（复盘结论，窗口定 60 分钟）。
/// 加密币（不分时段）照常。
pub(crate) fn opening_hour_reason(line: &Line, now: chrono::DateTime<Utc>) -> Option<String> {
    (line.session != crate::rh_spread::Session::All && arb_exec::monitor::us_opening_hour(now))
        .then(|| {
            "美股开盘头一小时（纽约时间 9:30–10:30）不开股票类新仓：盘口乱跳、平仓滑点大"
                .to_string()
        })
}

/// Lighter（主网与 RH 两个部署）例行升级一般在周日北京时间 19:00（UTC 11:00），停 15～20 分钟，
/// 提前 1～3 天在 TG 频道公告。升级前后下单可能只成一条腿：这段时间前后各留余量，不开、不补带 Lighter 腿的仓。
/// 平仓与规则不受影响。
const LIGHTER_MAINTENANCE_START_MIN: u32 = 10 * 60 + 45;
const LIGHTER_MAINTENANCE_END_MIN: u32 = 11 * 60 + 45;

pub(crate) fn lighter_maintenance_reason(
    line: &Line,
    now: chrono::DateTime<Utc>,
) -> Option<String> {
    use chrono::{Datelike, Timelike};
    let lighter = |venue: Venue| matches!(venue, Venue::Lighter | Venue::LighterRh);
    if !(lighter(line.a) || lighter(line.b)) {
        return None;
    }
    // 刚碰到 Lighter 429（限频，可能封 IP）：这段时间不开带 Lighter 腿的新仓，第二腿发不出去就是单腿裸露。
    if let Some(left) = arb_exec::lighter_broker::rate_limited_for(now) {
        return Some(format!(
            "Lighter 刚返回 429 限频：还要约 {left} 秒才开带 Lighter 腿的新仓"
        ));
    }
    if now.weekday() != chrono::Weekday::Sun {
        return None;
    }
    let minute = now.hour() * 60 + now.minute();
    (LIGHTER_MAINTENANCE_START_MIN..LIGHTER_MAINTENANCE_END_MIN)
        .contains(&minute)
        .then(|| {
            "Lighter 周日例行升级时段（UTC 10:45～11:45，北京时间 18:45～19:45）不开带 Lighter 腿的新仓".into()
        })
}

/// 这个方向（`long_a` / `long_b`）可成交价差近期的中位数（%）。
pub(crate) fn recent_median(line: &Line, direction: &str) -> Option<Decimal> {
    let recent = line.recent_entry?;
    let median = if direction == "long_a" {
        recent.long_a_pct
    } else {
        recent.long_b_pct
    }?;
    Decimal::from_f64_retain(median).map(|median| median.round_dp(5))
}

/// 相对近期偏离门槛（见 [`Settings::recent_gate_pct`]）：开仓可成交价差 `entry_pct` 比这个方向近期的中位数高得不够。
/// 没开、近期样本不够都放行。
pub(crate) fn recent_gate_reason(
    line: &Line,
    direction: &str,
    entry_pct: Decimal,
    settings: &Settings,
) -> Option<String> {
    let gate = settings.recent_gate_pct?;
    let median = recent_median(line, direction)?;
    let above = entry_pct - median;
    (above < gate).then(|| {
        format!(
            "可成交价差 {}% 只比近 {} 小时中位 {}% 高 {}%，不到 {}%：价差可能整体换了水平，不算偏离",
            entry_pct.round_dp(3),
            line.recent_entry.map_or(0, |recent| recent.hours),
            median.round_dp(3),
            above.round_dp(3),
            gate.normalize()
        )
    })
}

/// 本来可以做、只因为「相对近期偏离门槛」被挡下的合约：(合约, 原因)。给日志用，好看这层过滤挡了谁。
pub fn recent_gate_blocked(view: &View, settings: &Settings) -> Vec<(String, String)> {
    if settings.recent_gate_pct.is_none() {
        return Vec::new();
    }
    let off = Settings {
        recent_gate_pct: None,
        ..settings.clone()
    };
    view.lines
        .iter()
        .filter(|line| settings.symbols.is_empty() || settings.symbols.contains(&line.base))
        .filter_map(|line| {
            let candidate = candidate_of(line, &off)?;
            if candidate_of(line, settings).is_some() {
                return None;
            }
            let direction = if candidate.long == line.a {
                "long_a"
            } else {
                "long_b"
            };
            recent_gate_reason(line, direction, candidate.entry_pct, settings)
                .map(|reason| (line.base.clone(), reason))
        })
        .collect()
}

/// 扫描快照多旧就不拿来扣资金费（费率按结算周期变，十几分钟内的足够）。
const FUNDING_SNAPSHOT_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// 扫描报告里各场所、各合约（base，大写）的每小时资金费率（小数）。
fn funding_rates(report: &arb_scanner::ScanReport) -> FundingRates {
    let mut rates = FundingRates::new();
    for symbol in &report.symbols {
        for rate in symbol.rates.iter().filter(|rate| rate.interval_h > 0) {
            rates
                .entry(rate.venue)
                .or_default()
                .entry(symbol.symbol.base.to_ascii_uppercase())
                .or_insert(rate.period_rate / Decimal::from(rate.interval_h));
        }
    }
    rates
}

/// 场所 → 合约（base，大写）→ 每小时资金费率。分两层，查的时候不用为了拼键分配字符串。
type FundingRates = HashMap<Venue, HashMap<String, Decimal>>;

/// 查一家一个合约的每小时费率（合约名不分大小写）。
fn funding_rate(rates: &FundingRates, venue: Venue, base: &str) -> Option<Decimal> {
    let per_venue = rates.get(&venue)?;
    if base.bytes().any(|byte| byte.is_ascii_lowercase()) {
        per_venue.get(&base.to_ascii_uppercase()).copied()
    } else {
        per_venue.get(base).copied()
    }
}

/// 指针当键用的哈希：地址本身已经够散，不必再过一遍 SipHash。
#[derive(Default)]
struct PtrHasher(u64);

impl std::hash::Hasher for PtrHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0 << 8) | u64::from(*byte);
        }
    }
    fn write_usize(&mut self, n: usize) {
        // 堆地址低几位总是 0（对齐），乘一个奇数把高位的差别搅到低位。
        self.0 = (n as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left(29);
    }
}

/// 扣好资金费的一行：`source` 是价差监控快照里的那一行（留着它，地址就不会被别的行复用）。
struct Deducted {
    source: Arc<Line>,
    line: Arc<Line>,
    round: u64,
}

/// 扣资金费的缓存。自动交易每次检查都要扣一遍，以前是每次重建几千条的费率表、把 1800 行逐行改一遍；
/// 现在：
/// - 费率表按扫描快照建一次（快照换了、过期了、持仓小时数变了才重建）；
/// - 扣好的行按源行记着：价差监控的快路径只换盘口变过的行（没变的还是同一个 `Arc`），没变的直接复用。
///
/// 结果与逐行调 [`deduct_line_funding`] 完全一样（它只看这一行、费率表和小时数）。
#[derive(Default)]
struct FundingCache {
    snapshot: Option<Arc<crate::cache::Snapshot>>,
    /// 上次看时快照是不是还在 [`FUNDING_SNAPSHOT_MAX_AGE`] 以内。
    fresh: bool,
    hours: Decimal,
    rates: FundingRates,
    lines: HashMap<usize, Deducted, std::hash::BuildHasherDefault<PtrHasher>>,
    round: u64,
}

impl FundingCache {
    /// 把持仓期资金费从 `view` 的每一行里扣掉（见 [`deduct_line_funding`]）。没有快照、太旧都不扣。
    fn apply(
        &mut self,
        view: &mut View,
        snapshot: Option<Arc<crate::cache::Snapshot>>,
        hours: Decimal,
    ) {
        let fresh = snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.age() <= FUNDING_SNAPSHOT_MAX_AGE);
        let same = match (&self.snapshot, &snapshot) {
            (Some(old), Some(new)) => Arc::ptr_eq(old, new),
            (None, None) => true,
            _ => false,
        };
        if !same || fresh != self.fresh || hours != self.hours {
            self.rates = match &snapshot {
                Some(snapshot) if fresh => funding_rates(&snapshot.report),
                _ => HashMap::new(),
            };
            self.lines.clear();
            self.snapshot = snapshot;
            self.fresh = fresh;
            self.hours = hours;
        }
        self.deduct(view);
    }

    /// 按现有的费率表扣（表是空的就不扣）。
    fn deduct(&mut self, view: &mut View) {
        if self.rates.is_empty() {
            return;
        }
        self.round = self.round.wrapping_add(1);
        let (round, rates, hours) = (self.round, &self.rates, self.hours);
        for line in &mut view.lines {
            let key = Arc::as_ptr(line) as usize;
            let entry = self
                .lines
                .entry(key)
                .and_modify(|entry| entry.round = round)
                .or_insert_with(|| {
                    let mut deducted = Line::clone(line);
                    deduct_line_funding(
                        &mut deducted,
                        &|venue, base| funding_rate(rates, venue, base),
                        hours,
                    );
                    Deducted {
                        source: Arc::clone(line),
                        line: Arc::new(deducted),
                        round,
                    }
                });
            debug_assert!(Arc::ptr_eq(&entry.source, line));
            *line = Arc::clone(&entry.line);
        }
        // 换下来的旧行留一阵再清：每次都清要多扫一遍，攒到一倍多再清。
        if self.lines.len() > view.lines.len() * 2 + 64 {
            self.lines.retain(|_, entry| entry.round == round);
        }
    }
}

/// 资金费按多久的持仓扣（小时）：设了超时退出就按超时的小时数（最长大概拿这么久），没设按 24 小时。
pub fn funding_hold_hours(settings: &Settings) -> Decimal {
    settings.timeout_hours.unwrap_or(Decimal::from(24))
}

/// 持仓 `hours` 小时的资金费成本（%，≥ 0）：费率为正时多头付、空头收，所以净付 = (多腿每小时费率 − 空腿每小时费率)
/// × 小时。是净收入时记 0：收入不确定（费率说翻就翻），不拿它去凑门槛。
pub fn funding_cost_pct(long_hourly: Decimal, short_hourly: Decimal, hours: Decimal) -> Decimal {
    ((long_hourly - short_hourly) * hours * Decimal::ONE_HUNDRED)
        .max(Decimal::ZERO)
        .round_dp(5)
}

/// 把持仓期资金费从每个方向的「回到正常净收益」里扣掉（含币安挂单的测算），再按扣后的数重选更好的方向。
/// `hourly(场所, 合约)` 给每小时费率（小数）；两腿任何一家拿不到就这个方向不扣。
#[cfg(test)]
pub fn deduct_funding(
    view: &mut View,
    hourly: &dyn Fn(Venue, &str) -> Option<Decimal>,
    hours: Decimal,
) {
    for line in &mut view.lines {
        deduct_line_funding(Arc::make_mut(line), hourly, hours);
    }
}

/// 一行的持仓期资金费：从每个方向的「回到正常净收益」里扣掉（含币安挂单的测算），再按扣后的数重选更好的
/// 方向。`hourly(场所, 合约)` 给每小时费率（小数）；两腿任何一家拿不到就这个方向不扣。
pub fn deduct_line_funding(
    line: &mut Line,
    hourly: &dyn Fn(Venue, &str) -> Option<Decimal>,
    hours: Decimal,
) {
    {
        let cost = |long: Venue, short: Venue| {
            Some(funding_cost_pct(
                hourly(long, &line.base)?,
                hourly(short, &line.base)?,
                hours,
            ))
        };
        let (a, b) = (line.a, line.b);
        let costs = [cost(a, b), cost(b, a)];
        for (leg, cost) in [line.long_a.as_mut(), line.long_b.as_mut()]
            .into_iter()
            .zip(costs)
        {
            if let (Some(leg), Some(cost)) = (leg, cost) {
                leg.net_to_normal_pct = leg.net_to_normal_pct.map(|net| net - cost);
                leg.funding_cost_pct = Some(cost);
                for rung in &mut leg.ladder {
                    rung.net_to_normal_pct = rung.net_to_normal_pct.map(|net| net - cost);
                }
            }
        }
        if let Some(maker) = line.maker.as_mut() {
            for (leg, cost) in [maker.long_a.as_mut(), maker.long_b.as_mut()]
                .into_iter()
                .zip(costs)
            {
                if let (Some(leg), Some(cost)) = (leg, cost) {
                    leg.net_to_normal_taker_exit_pct =
                        leg.net_to_normal_taker_exit_pct.map(|net| net - cost);
                    leg.net_to_normal_maker_exit_pct =
                        leg.net_to_normal_maker_exit_pct.map(|net| net - cost);
                }
            }
        }
        // 扣完可能换了更好的方向（与监控选方向同一个口径）。
        let score = |leg: &Option<crate::rh_spread::Leg>| {
            leg.as_ref()
                .map(|leg| leg.net_to_normal_pct.unwrap_or(leg.net_to_zero_pct))
        };
        let direction = match (score(&line.long_a), score(&line.long_b)) {
            (Some(x), Some(y)) if y > x => Some("long_b"),
            (Some(_), _) => Some("long_a"),
            (None, Some(_)) => Some("long_b"),
            (None, None) => None,
        };
        if let (Some(best), Some(direction)) = (line.best.as_mut(), direction) {
            best.direction = direction;
        }
    }
}

/// 急行情不开仓：两家里任何一家最近 5 秒的中间价波动幅度达到 `fast_move_pct`。没开就放行。
///
/// 还没有近 5 秒的价格历史时**不开**（刚启动、刚重连，`recent_move` 为 `None`）。以前这种情况放行，复盘（实盘样本）：
/// - 这种情况开的单，比预估平均差得多，近三分之一差得较多；
/// - 有历史且平稳（<0.05%）的单平均差得很少，极少数差得较多；
/// - 这种情况开的单已平的净赚合计也很少。
///
/// 例：Lighter 重连 7 秒后开仓，HL-xyz 比预估差 0.227%。刚连上时恰恰是盘口最不可信的时候，
/// 等几秒攒满窗口再看。
pub(crate) fn fast_move_reason(line: &Line, settings: &Settings) -> Option<String> {
    let limit = settings.fast_move_pct?;
    let Some(moved) = line.recent_move else {
        return Some(format!(
            "行情刚连上（启动或重连），近 {} 秒的价格历史还没记满，看不出是不是急行情，先不开",
            crate::rh_spread::tape::MOVE_WINDOW.as_secs()
        ));
    };
    let (venue, pct) = moved.larger(line);
    (pct >= limit).then(|| {
        format!(
            "急行情：{venue} 的中间价近 {} 秒波动 {}%，达到 {}%，等行情稳住再开",
            crate::rh_spread::tape::MOVE_WINDOW.as_secs(),
            pct.round_dp(3),
            limit.normalize()
        )
    })
}

/// 本来可以做、只因为「急行情不开仓」被挡下的合约：(合约, 原因)。给日志用，好统计这层过滤挡了多少、
/// 挡在什么幅度上。
pub fn fast_move_blocked(view: &View, settings: &Settings) -> Vec<(String, String)> {
    if settings.fast_move_pct.is_none() {
        return Vec::new();
    }
    let off = Settings {
        fast_move_pct: None,
        ..settings.clone()
    };
    view.lines
        .iter()
        .filter(|line| settings.symbols.is_empty() || settings.symbols.contains(&line.base))
        .filter_map(|line| {
            candidate_of(line, &off)?;
            fast_move_reason(line, settings).map(|reason| (line.base.clone(), reason))
        })
        .collect()
}

/// 一个方向为什么不做（纯函数）。`None` = 可以做。顺序：门槛（扣滑点缓冲）→ 手续费占比 → 入场硬条件。
///
/// - 门槛：回到正常净收益 − 滑点缓冲 ≥ `min_net_pct`。
/// - 手续费占比：往返费 ÷（回到正常净收益 + 往返费）不超过 `max_fee_share_pct`。分母是还没扣手续费的毛利。
/// - 入场硬条件：可成交价差 > 0、收敛到 0 也赚钱。开了「相对正常基差」时不要求（由下单路径按收敛到目标核对）。
pub(crate) fn skip_reason(
    line: &Line,
    leg: &crate::rh_spread::Leg,
    settings: &Settings,
) -> Option<String> {
    let net = leg.net_to_normal_pct?;
    let after_buffer = net - settings.slippage_buffer_pct;
    if after_buffer < settings.min_net_pct {
        return Some(if settings.slippage_buffer_pct.is_zero() {
            format!("回到正常净收益 {}% 不到门槛", net.round_dp(3))
        } else {
            format!(
                "回到正常净收益 {}% 扣滑点缓冲 {}% 后 {}%，不到门槛 {}%",
                net.round_dp(3),
                settings.slippage_buffer_pct.normalize(),
                after_buffer.round_dp(3),
                settings.min_net_pct.normalize()
            )
        });
    }
    // 「相对正常基差」的单子入场价差本来可以是负的（赌偏离回到正常），入场价差门槛对它没意义。
    if !settings.relative_to_normal
        && let (Some(min_edge), Some(fee)) = (settings.min_edge_pct, line.fee_round_trip_pct)
    {
        let edge = leg.quote.entry_pct - fee;
        if edge < min_edge {
            return Some(format!(
                "可成交价差 {}% 扣往返手续费 {}% 后只剩 {}%，不到往返净边际门槛 {}%",
                leg.quote.entry_pct.round_dp(3),
                fee.round_dp(3),
                edge.round_dp(3),
                min_edge.normalize()
            ));
        }
    }
    if !settings.relative_to_normal
        && let (Some(min_entry), Some(venue)) =
            (settings.thin_min_entry_pct, settings.thin_venue(line))
        && leg.quote.entry_pct < min_entry
    {
        return Some(format!(
            "{venue} 盘口薄，可成交价差 {}% 不到薄盘口门槛 {}%",
            leg.quote.entry_pct.round_dp(3),
            min_entry.normalize()
        ));
    }
    if let (Some(limit), Some(fee)) = (settings.max_fee_share_pct, line.fee_round_trip_pct) {
        let gross = net + fee;
        if gross > Decimal::ZERO && fee / gross * Decimal::ONE_HUNDRED > limit {
            return Some(format!(
                "往返手续费 {}% 占预估毛利 {}% 的 {}%，超过上限 {}%",
                fee.round_dp(3),
                gross.round_dp(3),
                (fee / gross * Decimal::ONE_HUNDRED).round_dp(0),
                limit.normalize()
            ));
        }
    }
    if !settings.relative_to_normal {
        return leg.auto_entry_block_reason();
    }
    None
}

/// 只看这一行和设置（不看账户、仓位、冷却）：自动交易做不做这个方向（`long_a` / `long_b`）。
/// `None` = 规则上可以做。与 [`candidate_of`] 的筛选一致，给 Telegram 提醒说原因用。
pub(crate) fn rule_block(line: &Line, direction: &str, settings: &Settings) -> Option<String> {
    if line.note.is_some() || line.watch_only {
        return Some("观察组，不参与自动交易".into());
    }
    if let Some(venue) = settings.excluded_venue(line) {
        return Some(format!("{venue} 在自动交易的排除名单里"));
    }
    if settings.excluded_symbol(line) {
        return Some(format!("{} 在不自动交易的合约名单里", line.base));
    }
    if line.observe_only && !settings.binance_allows(&line.base) {
        return Some(format!("{} 不在币安自动交易的名单里", line.base));
    }
    if settings.binance_only && !line.observe_only {
        return Some("自动交易设成了只做币安".into());
    }
    if !settings.symbols.is_empty() && !settings.symbols.contains(&line.base) {
        return Some(format!("{} 不在自动交易的合约名单里", line.base));
    }
    let Some((_, _, leg)) = line.legs(direction) else {
        return Some("这个方向没有报价".into());
    };
    auto_skip_reason(line, leg, settings)
}

/// 已达到用户净收益门槛、但被其它条件挡下的偏离：给面板说明，而非笼统报「没信号」。
fn blocked_signal_reason(view: &View, settings: &Settings) -> Option<String> {
    view.lines.iter().find_map(|line| {
        // 先看够不够门槛（一次比较），够了再过名单（上市前名单要加锁、逐个比）：都是「不算」的条件，先后无关。
        let best = line.best.as_ref()?;
        let (_, _, leg) = line.legs(best.direction)?;
        if leg.net_to_normal_pct? < settings.min_net_pct {
            return None;
        }
        if line.note.is_some()
            || (line.observe_only && !settings.binance_allows(&line.base))
            || settings.excluded_venue(line).is_some()
            || settings.excluded_symbol(line)
            || crate::preipo::pre_ipo(&line.base).is_some()
            || line.normal.is_none()
            || (!settings.symbols.is_empty() && !settings.symbols.contains(&line.base))
        {
            return None;
        }
        auto_skip_reason(line, leg, settings)
            .map(|reason| format!("{}（{}）：{reason}，不自动下单", line.base, line.pair))
    })
}

/// 这一刻要做的事：新开一笔，或往已有仓位上补。
enum Choice {
    Open(Candidate),
    Add(Candidate, AddTarget),
}

/// 下单结果怎么处理。
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// 两腿都建好了。
    Opened(String),
    /// 开仓以回滚收场（计入连续失败）。
    Unwound(String),
    /// 挂单开仓：挂满了时间一点都没成交已撤（什么都没发生），或成交太碎回滚了一小块（只付一笔平仓费）。
    /// 都不计失败，合约歇一会儿再看。
    NoFill(String),
    /// 吃单开仓：第一腿就被交易所拒了 / 没吃到，一张单都没成交（什么都没发生）。不计失败，合约多歇一会儿。
    Untraded(String),
    /// 闸门拒绝（深度、净收益、规则、限额）：这个合约冷却。
    Rejected,
    /// 交易台忙：稍后再试。
    Busy,
    /// 暂停开新仓 / 实盘没连上：等。
    Paused,
    /// 执行中断、结果未知：立刻关闭自动交易。
    Interrupted,
    /// 配置类错误（请求不合法、只读模式）：关闭自动交易。
    Misconfigured,
}

/// 开仓接口因为「当日已实现盈亏算不出」拒绝（见 `trade::live_daily_default`）。
pub fn is_daily_pnl_unknown(error: &str) -> bool {
    error.contains("当日已实现盈亏") && error.contains("算不出")
}

/// 当日盈亏算不出时，多久提醒一次。
const DAILY_UNKNOWN_ALERT_EVERY: Duration = Duration::from_secs(3600);

/// 按开仓接口的状态码与返回体分类。
pub fn classify(status: StatusCode, body: &Value) -> Outcome {
    if status.is_success() {
        let id = body["position"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        return match body["position"]["status"].as_str() {
            Some("open") => Outcome::Opened(id),
            _ => Outcome::Unwound(id),
        };
    }
    match status {
        StatusCode::CONFLICT
            if body["error"]
                .as_str()
                .is_some_and(|e| e.contains("正在进行")) =>
        {
            Outcome::Busy
        }
        StatusCode::LOCKED | StatusCode::SERVICE_UNAVAILABLE => Outcome::Paused,
        // 当日已实现盈亏算不出（台账里今天结束的仓位有没记盈亏的）：等台账补上，不是配置错，不关自动交易。
        // 某次实盘 就是这样把自动交易关掉的。
        StatusCode::BAD_REQUEST if body["error"].as_str().is_some_and(is_daily_pnl_unknown) => {
            Outcome::Paused
        }
        StatusCode::UNPROCESSABLE_ENTITY | StatusCode::CONFLICT => Outcome::Rejected,
        StatusCode::INTERNAL_SERVER_ERROR => Outcome::Interrupted,
        _ => Outcome::Misconfigured,
    }
}

/// [`classify`]，再认出以 `unwound` 收场里「不是失败」的几种：
/// - 一点都没成交（开仓接口的 `nothing_traded` 为真 —— 按订单成交记录判断，**不看**仓位的腿：回滚会
///   把腿清空）：挂单是到点撤了，吃单是第一腿就被交易所拒了 / 没吃到（某次实盘：Hyperliquid 周末按
///   预言机限价拒单，用户定为不计失败）；
/// - 挂单成交太碎、凑不够对冲腿的最小额，回滚了那一小块（备注带 [`arb_exec::MAKER_DUST_NOTE`]）。
///
/// 什么也没发生 / 只付一笔平仓的吃单费，都不能计入连续失败（否则两次没成交就把整个自动交易关了）。
/// 第一腿成交后对冲失败的真回滚照旧。
pub fn classify_attempt(candidate: &Candidate, status: StatusCode, body: &Value) -> Outcome {
    let maker = candidate.maker.is_some();
    match classify(status, body) {
        Outcome::Unwound(id) if body["nothing_traded"].as_bool() == Some(true) => {
            if maker {
                Outcome::NoFill(id)
            } else {
                Outcome::Untraded(id)
            }
        }
        Outcome::Unwound(id)
            if maker
                && body["position"]["note"]
                    .as_str()
                    .is_some_and(|note| note.contains(arb_exec::MAKER_DUST_NOTE)) =>
        {
            Outcome::NoFill(id)
        }
        other => other,
    }
}

/// 自动交易器。
pub struct AutoTrader {
    path: Option<PathBuf>,
    inner: Mutex<Inner>,
    /// 给 Telegram 价差提醒的摘要（见 [`Brief`]）。
    brief: std::sync::Mutex<Brief>,
    /// 扣资金费的缓存（见 [`FundingCache`]）：只有主循环用。两份：`.0` 给每次检查都扣的「值得细看」的那几行，
    /// `.1` 给整份快照 —— 各记各的行，互不把对方的挤出去。
    funding: std::sync::Mutex<(FundingCache, FundingCache)>,
}

/// 自动交易这一刻的摘要，给 Telegram 价差提醒说明「这一笔为什么没自动下单」：主循环每一轮更新。
/// 用同步锁，价差监控推提醒时不用 await。
#[derive(Default)]
struct Brief {
    /// 挡住所有新开仓的原因（没开、已关闭、实盘没连上、暂停、到上限……）；`None` = 在逐个合约地看。
    halted: Option<String>,
    settings: Option<Settings>,
    /// 达到提醒条件的各行（组, 合约, 方向）按扣过资金费的快照算的规则结论：`None` = 规则上可以做。
    rules: HashMap<(String, String, &'static str), Option<String>>,
    /// 已有仓位的合约（大写）。
    exposed: HashSet<String>,
    /// 历史分位门槛的结论（`None` = 通过）。
    history: HashMap<HoldKey, Option<String>>,
    cooldown: HashMap<String, Instant>,
    /// 各合约（大写）最近一次自动开仓的结果。
    outcome: HashMap<String, (Instant, String)>,
}

/// 提醒里提「自动交易刚试过这个合约」的时效：和下单失败后的冷却一样长。
const OUTCOME_FRESH: Duration = SYMBOL_COOLDOWN;

struct Inner {
    persisted: Persisted,
    events: VecDeque<Event>,
    cooldown: HashMap<String, Instant>,
    retry_at: Option<Instant>,
    last_open: Option<Instant>,
    /// 达到门槛的（合约, 多腿, 空腿）从什么时候开始连续成立。
    since: HashMap<HoldKey, Instant>,
    /// 这一刻为什么没下单。
    status: String,
    attempting: Option<String>,
    /// 历史分位门槛的结论（[`HISTORY_RECHECK`] 内有效）：`None` = 通过，`Some(原因)` = 挡下。
    history: HashMap<HoldKey, (Instant, Option<String>)>,
    /// 各组合最近一次达标的时刻（保持计时的去抖，见 [`HOLD_GRACE`]）。
    seen: HashMap<HoldKey, Instant>,
    /// 正在后台算历史分位的组合。
    history_pending: HashSet<HoldKey>,
    /// 进行中的提前准备。
    arming: Option<tokio::task::JoinHandle<()>>,
    /// 各组合最近一次开始提前准备的时刻（[`ARM_EVERY`]）。
    armed_at: HashMap<HoldKey, Instant>,
    /// 最近一次预判式准备的时刻（任何组合），见 [`ARM_NEAR_GAP`]。
    last_near_arm: Option<Instant>,
    /// 今天（UTC）发起过几次挂单（[`MAX_MAKER_ATTEMPTS_PER_DAY`]）；重启清零。
    maker_attempts: Option<(chrono::NaiveDate, u32)>,
    /// 各合约最近一次记「急行情不开仓」日志的时刻（[`FAST_MOVE_LOG_EVERY`]）。
    fast_move_logged: HashMap<String, Instant>,
    /// 「相对近期偏离门槛」挡下的合约上次记日志的时刻（同一合约 [`RECENT_GATE_LOG_EVERY`] 一条）。
    recent_gate_logged: HashMap<String, Instant>,
    /// 上一次提醒「当日盈亏算不出、自动交易在等」的时刻。
    daily_unknown_alerted: Option<Instant>,
    /// 按盘口放大后保证金不够被拒的合约：到这个时刻之前按原来的名义做，不放大。
    base_size_only: HashMap<String, Instant>,
    /// 提前设杠杆的进度（[`AutoTrader::maybe_preset`]），键是（场所, 合约）；重启清零。
    preset: HashMap<(Venue, String), PresetMark>,
    /// 上一次开始提前设杠杆的时刻（[`PRESET_EVERY`]）。
    preset_last: Option<Instant>,
    /// 上次看了一圈、没有要提前设杠杆的是什么时候：隔 [`PRESET_IDLE_RECHECK`] 再看，不必每次检查都把
    /// 1800 行筛一遍、排一遍。
    preset_idle: Option<Instant>,
    /// 各场所上一次开始提前设杠杆的时刻（[`PRESET_LIGHTER_EVERY`]）。
    preset_venue_last: HashMap<Venue, Instant>,
    /// 信号保持影子（只记录不下单，见 [`hold_shadow`]）。
    hold_shadow: hold_shadow::Shadow,
    /// 各交易所的下单往返；明显变慢时暂停用它开新仓（见 [`venue_latency`]）。
    latency: venue_latency::VenueLatency,
    /// 执行中断（结果不明）后的暂停：（中断时刻, 起始, 说明）。等台账与对账核实一致后自动恢复，
    /// [`INTERRUPT_RESUME_LIMIT`] 内恢复不了才关掉自动交易（见 [`AutoTrader::interrupted_gate`]）。
    interrupted: Option<(DateTime<Utc>, Instant, String)>,
}

/// 一个（场所, 合约）提前设杠杆的结果。
#[derive(Debug, Clone, Copy)]
struct PresetMark {
    leverage: Decimal,
    at: Instant,
    /// 设好了（核对过是目标杠杆）。`false` = 正在做、或没设成（[`PRESET_RETRY`] 后再试）。
    done: bool,
}

impl Inner {
    fn track(&mut self, view: &View, settings: &Settings, now: Instant) {
        track_holds(view, settings, &mut self.since, &mut self.seen, now);
    }
}

/// 历史分位门槛的结论多久重算一次（K 线缓存另有 1 分钟）。
const HISTORY_RECHECK: Duration = Duration::from_secs(5 * 60);

/// 达标的信号连续消失多久才算中断。价差快照在盘口有变化时约 100 毫秒重算一次（以前每秒一次），
/// 一次短暂掉下门槛不该把保持计时清零 —— 以前每秒采样一次，同样看不见一秒以内的抖动。
pub const HOLD_GRACE: Duration = Duration::from_secs(1);

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `at` 到 `now` 过了多少毫秒。
fn ms_since(now: Instant, at: Instant) -> u64 {
    duration_ms(now.saturating_duration_since(at))
}

/// 毫秒写成秒：整秒不带小数（`5`），否则一位小数、舍去不进位（`499` → `0.4`）。
pub fn secs_text(ms: u64) -> String {
    if ms.is_multiple_of(1000) {
        (ms / 1000).to_string()
    } else {
        format!("{}.{}", ms / 1000, ms % 1000 / 100)
    }
}
/// 计时多久没更新（这段时间没在看）就作废、从头计时（见 [`track_holds`]）。正常每秒至少看一次；
/// 一次开仓占着检查好几秒，不能因此清掉别的组合的计时。
pub const HOLD_UNOBSERVED: Duration = Duration::from_secs(10);
/// 两次检查之间至少隔多久：快照一更新就会唤醒，太密只是白读一遍状态。一次检查平时只细看离门槛近的
/// 几行（[`worth_a_look`]），很轻，所以跟着快照的节拍走：2026-11-15 实测 100ms → 10ms，「快照发布 →
/// 自动交易看到」平均 23ms → 约 4ms。可用环境变量 `ARB_RH_AUTO_MIN_STEP_MS` 调
/// （见 [`crate::rh_spread::tuned_ms`]）。
const MIN_STEP: Duration = Duration::from_millis(10);
/// 预判式提前准备：回到正常净收益达到门槛的这个比例就算「快到了」。信号保持可以是 0（见信号就下），
/// 没有保持期可以拿来准备，只能对「快到」的预判式地做（见 [`AutoTrader::maybe_arm_near`]）。
const ARM_NEAR_RATIO: Decimal = Decimal::from_parts(5, 0, 0, false, 1);
/// 预判式准备只在保持时间不长（不超过这么久）时做：保持更久的，信号保持期内的提前准备（[`maybe_arm`]）
/// 就来得及。
const ARM_NEAR_MAX_HOLD_MS: u64 = 5_000;
/// 任意两次预判式准备之间至少隔多久（不管是哪个组合）：准备占着交易锁、打两家的私有接口。
const ARM_NEAR_GAP: Duration = Duration::from_secs(3);
/// 提前准备（[`crate::trade::prearm_for_auto`]）：信号保持到离 `hold_sec` 还差这么久时开始做……
const ARM_LEAD: Duration = Duration::from_secs(4);
/// ……但信号至少要保持这么久（一闪而过的不值得做一轮对账）。历史分位也从这时候开始在后台算。
const ARM_MIN_HOLD: Duration = Duration::from_secs(1);
/// 同一个组合两次提前准备至少隔多久：每次都要对账（打几家的私有接口，Lighter RH 的限频按 IP 算）。
const ARM_EVERY: Duration = Duration::from_secs(10);
/// 同一个合约两次记「急行情不开仓」日志至少隔多久（行情急的时候每 100 毫秒都会命中）。
const FAST_MOVE_LOG_EVERY: Duration = Duration::from_secs(30);
/// 「相对近期偏离门槛」挡下的同一合约多久记一条日志：信号可能一挂几个小时。
const RECENT_GATE_LOG_EVERY: Duration = Duration::from_secs(600);
/// 下单前最多等进行中的提前准备多久：它占着交易锁，等它做完比被判「交易台忙」再等 5 秒快。
const ARM_WAIT: Duration = Duration::from_secs(3);
/// 空闲时提前设杠杆（[`AutoTrader::maybe_preset`]）：两次之间至少隔多久。每次要读一两次私有接口
/// （Lighter RH 的限频按 IP 算），慢慢来；交易组里的合约一轮做完约半小时，之后只剩新出现的。
const PRESET_EVERY: Duration = Duration::from_secs(5);
/// 看了一圈没有要提前设杠杆的，过多久再看（自动交易一秒检查几十次，不必每次都筛一遍全部组合）。
const PRESET_IDLE_RECHECK: Duration = Duration::from_secs(1);
/// 同一家 Lighter（主网 / RH）两次提前设杠杆至少隔多久：改杠杆后要轮询账户读回（最多约 10 秒、几十次读），
/// 读请求和行情扫描、下单共用同一个出口 IP 的限频（RH 尤其紧）。
const PRESET_LIGHTER_EVERY: Duration = Duration::from_secs(30);
/// 没设成（出错、有持仓、休市）的过多久再试。
const PRESET_RETRY: Duration = Duration::from_secs(3600);
/// 提前设杠杆用了这么久以上（毫秒）才记 info：多半是真的写了一次；只核对一下的记 debug。
const PRESET_SLOW_MS: u128 = 300;

/// 历史分位门槛的判定（纯函数）：现在的分位 `pct` 排在前 `gate`% 才通过。
pub fn history_gate_passes(pct: f64, gate: f64) -> bool {
    pct >= 100.0 - gate
}

/// 历史分位门槛：这个方向（多 `long`、空 `short`）现在的价差排在历史第几分位，够不够 `gate_pct`。
/// `None` = 通过。拉不到历史 = 不通过。
async fn history_verdict(c: &Candidate, settings: &Settings) -> Option<String> {
    let gate = settings.history_gate_pct?.to_f64().unwrap_or(0.0);
    let days = settings.history_days.to_f64().unwrap_or(24.0);
    match crate::spread_chart::entry_percentile(
        &c.base,
        c.long,
        c.short,
        &settings.history_interval,
        days,
    )
    .await
    {
        Ok((pct, _)) if history_gate_passes(pct, gate) => None,
        Ok((pct, stats)) => Some(format!(
            "历史价差分位 {:.0}%（{} 线 {} 天，当前 {:+.3}%，最高 {:+.3}%、中位 {:+.3}%），不在前 {}%",
            pct,
            settings.history_interval,
            settings.history_days.normalize(),
            stats.last_pct,
            stats.max_pct,
            stats.median_pct,
            gate
        )),
        Err(error) => Some(format!("历史价差拉不到（{error:#}），按不合格处理")),
    }
}

impl AutoTrader {
    pub fn load(dir: &std::path::Path) -> Arc<Self> {
        let path = dir.join("auto.json");
        let mut persisted = match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str::<Persisted>(&raw).unwrap_or_else(|error| {
                // 读不懂就当作关闭：状态不明时不自动下单。
                warn!(path = %path.display(), "自动交易设置读不懂，按关闭处理：{error}");
                Persisted {
                    disabled_reason: Some(format!("设置文件读不懂（{error}），已按关闭处理")),
                    ..Persisted::default()
                }
            }),
            Err(_) => Persisted::default(),
        };
        persisted.settings.retire_binance();
        Arc::new(Self {
            path: Some(path),
            inner: Mutex::new(Inner {
                persisted,
                events: VecDeque::new(),
                cooldown: HashMap::new(),
                history: HashMap::new(),
                since: HashMap::new(),
                retry_at: None,
                last_open: None,
                status: "未开启".into(),
                attempting: None,
                seen: HashMap::new(),
                history_pending: HashSet::new(),
                fast_move_logged: HashMap::new(),
                recent_gate_logged: HashMap::new(),
                daily_unknown_alerted: None,
                base_size_only: HashMap::new(),
                arming: None,
                armed_at: HashMap::new(),
                last_near_arm: None,
                maker_attempts: None,
                preset: HashMap::new(),
                preset_last: None,
                preset_idle: None,
                preset_venue_last: HashMap::new(),
                hold_shadow: hold_shadow::Shadow::new(Some(dir.to_path_buf())),
                latency: venue_latency::VenueLatency::default(),
                interrupted: None,
            }),
            brief: std::sync::Mutex::new(Brief::default()),
            funding: std::sync::Mutex::new(Default::default()),
        })
    }

    #[cfg(test)]
    pub fn in_memory(settings: Settings) -> Arc<Self> {
        Arc::new(Self {
            path: None,
            inner: Mutex::new(Inner {
                persisted: Persisted {
                    settings,
                    ..Persisted::default()
                },
                events: VecDeque::new(),
                cooldown: HashMap::new(),
                history: HashMap::new(),
                since: HashMap::new(),
                retry_at: None,
                last_open: None,
                status: String::new(),
                attempting: None,
                seen: HashMap::new(),
                history_pending: HashSet::new(),
                fast_move_logged: HashMap::new(),
                recent_gate_logged: HashMap::new(),
                daily_unknown_alerted: None,
                base_size_only: HashMap::new(),
                arming: None,
                armed_at: HashMap::new(),
                last_near_arm: None,
                maker_attempts: None,
                preset: HashMap::new(),
                preset_last: None,
                preset_idle: None,
                preset_venue_last: HashMap::new(),
                hold_shadow: hold_shadow::Shadow::new(None),
                latency: venue_latency::VenueLatency::default(),
                interrupted: None,
            }),
            brief: std::sync::Mutex::new(Brief::default()),
            funding: std::sync::Mutex::new(Default::default()),
        })
    }

    pub async fn settings(&self) -> Settings {
        self.inner.lock().await.persisted.settings.clone()
    }

    fn save(&self, persisted: &Persisted) {
        let Some(path) = &self.path else { return };
        let result = serde_json::to_string_pretty(persisted)
            .map_err(std::io::Error::other)
            .and_then(|body| {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let tmp = path.with_extension("json.tmp");
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create(true).truncate(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&tmp)?;
                std::io::Write::write_all(&mut file, body.as_bytes())?;
                file.sync_all()?;
                std::fs::rename(&tmp, path)
            });
        if let Err(error) = result {
            error!(path = %path.display(), "自动交易设置写不进文件：{error}");
        }
    }

    fn push(inner: &mut Inner, kind: &'static str, symbol: Option<&str>, text: String) {
        inner.events.push_front(Event {
            at: Utc::now(),
            kind,
            symbol: symbol.map(str::to_string),
            text,
        });
        inner.events.truncate(EVENTS);
    }

    /// 关闭自动交易并记下原因。
    /// 执行中断后先暂停开新仓（不关自动交易）：交给续上建仓 / 了结补仓单 / 对账去核实，核实一致就自动恢复。
    fn pause_interrupted(inner: &mut Inner, state: &AppState, text: String) {
        inner.interrupted = Some((Utc::now(), Instant::now(), text.clone()));
        Self::push(inner, "interrupted", None, text.clone());
        inner.status = format!("执行中断，暂停开新仓、等核实：{text}");
        state.alerts.notify_always(format!(
            "⏸ 价差自动交易暂停开新仓：{text}。系统会按台账与对账核实，一致后自动恢复；{} 分钟内恢复不了就关闭自动交易、等人工处理。",
            INTERRUPT_RESUME_LIMIT.as_secs() / 60
        ));
    }

    /// 中断暂停中：核实一致就恢复（返回 `true` 可以照常往下走），超时就关掉自动交易，还在等就停在这一轮。
    async fn interrupted_gate(&self, inner: &mut Inner, state: &AppState) -> bool {
        let Some((since_utc, since, text)) = inner.interrupted.clone() else {
            return true;
        };
        match interrupt_decision(state.trade.settled_since(since_utc).await, since.elapsed()) {
            InterruptDecision::Resume => {
                inner.interrupted = None;
                let note = format!(
                    "中断后已核实一致（台账没有未决单、对账干净），{} 秒后自动恢复开仓：{text}",
                    since.elapsed().as_secs()
                );
                info!("价差自动交易：{note}");
                Self::push(inner, "resumed", None, note.clone());
                state
                    .alerts
                    .notify_always(format!("▶️ 价差自动交易已自动恢复：{note}"));
                true
            }
            InterruptDecision::Disable(waiting) => {
                inner.interrupted = None;
                let reason = format!(
                    "{text}；{} 分钟内没能核实一致（{waiting}）",
                    INTERRUPT_RESUME_LIMIT.as_secs() / 60
                );
                inner.persisted.settings.enabled = false;
                inner.persisted.disabled_reason = Some(reason.clone());
                Self::push(inner, "disabled", None, reason.clone());
                self.save(&inner.persisted);
                error!("价差自动交易已关闭：{reason}");
                state.alerts.notify_always(format!(
                    "⛔ 价差自动交易已关闭：{reason}。以台账与对账为准，去持仓页核对后再决定是否重新开启。"
                ));
                false
            }
            InterruptDecision::Wait(waiting) => {
                inner.status = format!("执行中断，暂停开新仓、等核实（{waiting}）：{text}");
                self.brief().halted = Some(inner.status.clone());
                false
            }
        }
    }

    async fn disable(&self, reason: String) {
        let mut inner = self.inner.lock().await;
        inner.persisted.settings.enabled = false;
        inner.persisted.disabled_reason = Some(reason.clone());
        Self::push(&mut inner, "disabled", None, reason);
        self.save(&inner.persisted);
    }

    /// 新设置。返回保存后的设置；启用实盘需要 `confirm_live`。
    pub async fn update(
        &self,
        settings: Settings,
        max_size: Option<Decimal>,
        confirm_live: bool,
    ) -> Result<Settings, String> {
        let mut settings = settings;
        settings.retire_binance();
        settings.validate(max_size)?;
        let mut inner = self.inner.lock().await;
        let was = inner.persisted.settings.clone();
        let turning_on_live = settings.enabled
            && settings.mode == Mode::Live
            && !(was.enabled && was.mode == Mode::Live);
        if turning_on_live && !confirm_live {
            return Err("开启实盘自动交易需要确认：confirm 必须是 LIVE".into());
        }
        if settings.enabled && !was.enabled {
            // 重新开启：连续失败清零，冷却与重试时刻清空。
            inner.persisted.failures = 0;
            inner.persisted.disabled_reason = None;
            inner.cooldown.clear();
            inner.retry_at = None;
        }
        let text = describe(&settings);
        inner.persisted.settings = settings.clone();
        Self::push(&mut inner, "settings", None, text);
        self.save(&inner.persisted);
        Ok(settings)
    }

    /// 给页面的状态。
    pub async fn snapshot(&self, exposed: Option<&[(String, String)]>) -> Value {
        let inner = self.inner.lock().await;
        let today = Utc::now().date_naive();
        let mode = inner.persisted.settings.mode;
        let opened_today = inner
            .persisted
            .opened
            .iter()
            .filter(|o| o.mode == mode && o.at.date_naive() == today)
            .count();
        let open_auto: Vec<&Opened> = exposed.map_or_else(Vec::new, |exposed| {
            inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && exposed.iter().any(|(id, _)| id == &o.id))
                .collect()
        });
        let now = Instant::now();
        let cooldown: Vec<Value> = inner
            .cooldown
            .iter()
            .filter(|(_, until)| **until > now)
            .map(|(base, until)| json!({ "symbol": base, "sec": until.duration_since(now).as_secs() }))
            .collect();
        json!({
            "settings": inner.persisted.settings,
            "status": inner.status,
            "attempting": inner.attempting,
            "failures": inner.persisted.failures,
            "max_failures": MAX_FAILURES,
            "disabled_reason": inner.persisted.disabled_reason,
            "opened_today": opened_today,
            "open_positions": open_auto,
            "cooldown": cooldown,
            "events": inner.events,
            "stats": board_stats(
                &inner.persisted.opened,
                exposed.unwrap_or(&[]),
                mode,
                &inner.persisted.tally,
                inner.persisted.failures,
                today,
            ),
        })
    }

    pub fn spawn(self: &Arc<Self>, state: Arc<AppState>) {
        let auto = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last = Instant::now();
            let min_step = crate::rh_spread::tuned_ms("ARB_RH_AUTO_MIN_STEP_MS", MIN_STEP, 5, 1000);
            loop {
                // 价差快照一更新就看一眼（盘口有变化时最快约 10 毫秒一次），不等下一个整秒；
                // 没有更新也每秒看一次（冷却、限额到期之类）。
                tokio::select! {
                    _ = tick.tick() => {}
                    () = state.rh_spread.changed() => {}
                }
                let wait = min_step.saturating_sub(last.elapsed());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                if crate::shutdown::is_draining() {
                    return;
                }
                last = Instant::now();
                crate::rh_spread::latency::METRICS.auto_check();
                auto.step(&state).await;
            }
        });
    }

    /// 自动下单时提交给开仓接口的请求体（提前准备与真正下单用同一份）。
    fn open_body(settings: &Settings, candidate: &Candidate) -> crate::trade::OpenBody {
        let rules = settings.rules_for(candidate.target_pct, candidate);
        let order = if settings.cheap_leg_first {
            arb_exec::preflight::LegOrder::CheapFirst
        } else {
            arb_exec::preflight::LegOrder::Slippage
        };
        let body = crate::trade::with_leg_order(
            crate::trade::with_relative_to_normal(
                crate::trade::auto_open_body(
                    settings.mode,
                    &format!("{}/USDT", candidate.base),
                    candidate.long,
                    candidate.short,
                    candidate.size(settings),
                    settings.leverage,
                    arb_exec::MarginMode::Isolated,
                    &rules,
                ),
                settings.relative_to_normal,
            ),
            order,
        );
        match candidate.maker {
            Some(maker) => crate::trade::with_maker(body, maker),
            // 按盘口深度定单笔、强制补腿只管两腿都吃单的单子（挂单开仓照旧）。
            None => crate::trade::with_entry_gate(
                crate::trade::with_force_hedge(
                    crate::trade::with_depth_fit(body, settings.depth_fit()),
                    settings.force_hedge_pct,
                ),
                settings.entry_gate(candidate.long, candidate.short),
            ),
        }
    }

    /// 提前准备：信号还在保持中、保持够了就会选中的 `candidate`，离保持时间还差 [`ARM_LEAD`] 时
    /// 在后台把下单前的只读准备做掉（实盘才做）。同一时刻最多一个，同一组合 [`ARM_EVERY`] 内只做一次。
    /// `held_ms`：信号已保持的毫秒数。保持时间短于 [`ARM_MIN_HOLD`] 的不做（下单时自己准备）。
    fn maybe_arm(
        inner: &mut Inner,
        state: &Arc<AppState>,
        settings: &Settings,
        candidate: &Candidate,
        held_ms: u64,
    ) {
        let hold = settings.hold_millis();
        let start_at = hold
            .saturating_sub(duration_ms(ARM_LEAD))
            .max(duration_ms(ARM_MIN_HOLD));
        if held_ms < start_at || held_ms >= hold {
            return;
        }
        if inner
            .arming
            .as_ref()
            .is_some_and(|arming| !arming.is_finished())
        {
            return;
        }
        let key = candidate.hold_key();
        inner.armed_at.retain(|_, at| at.elapsed() < ARM_EVERY);
        if inner.armed_at.contains_key(&key) {
            return;
        }
        inner.armed_at.insert(key, Instant::now());
        let body = Self::open_body(settings, candidate);
        let state = Arc::clone(state);
        let base = candidate.base.clone();
        inner.arming = Some(tokio::spawn(async move {
            match crate::trade::prearm_for_auto(state, body).await {
                Ok(ms) => info!(
                    symbol = %base,
                    ms,
                    "价差自动交易：下单前的对账、现扫、保证金核对、预热已提前做好"
                ),
                Err(why) => {
                    info!(symbol = %base, "价差自动交易：没能提前准备（下单时照常现做）：{why}")
                }
            }
        }));
    }

    /// 预判式提前准备：没有哪条信号在计时（或保持期太短、来不及用 [`Self::maybe_arm`]）时，挑一个**快到门槛**的
    /// 候选（回到正常净收益达到门槛的 [`ARM_NEAR_RATIO`]），把下单前的只读准备（两腿对账、现扫、保证金、预热）
    /// 在后台先做掉；信号一到，下单直接用，只剩按现拉的盘口算计划、发单。
    ///
    /// 只在实盘、保持不超过 [`ARM_NEAR_MAX_HOLD_MS`]、没被持仓数 / 每日 / 开仓间隔 / 冷却挡着时做。
    /// **每次只给净收益最高的那一个**做（槽里只有一份准备，最优的刚做过就歇着，不会退而求其次去覆盖它）；
    /// 全局两次至少隔 [`ARM_NEAR_GAP`]、同一个组合至少隔 [`ARM_EVERY`]。准备的读数比信号旧，下单时
    /// 若它没能通过计划，下单路径会退回现做（见 `trade::open_live`）。
    async fn maybe_arm_near(
        self: &Arc<Self>,
        state: &Arc<AppState>,
        settings: &Settings,
        lines: &[Arc<Line>],
        connected: &crate::rh_spread::Connected,
    ) {
        if settings.mode != Mode::Live
            || settings.hold_millis() > ARM_NEAR_MAX_HOLD_MS
            || !state.trade.live_can_trade()
        {
            return;
        }
        // 先用一次比较挡掉绝大多数行（快照有一千八百行，这一拍多半一条都不「快到」）。
        let near_floor = settings.min_net_pct * ARM_NEAR_RATIO;
        if !lines.iter().any(|line| is_nearly(line, near_floor)) {
            return;
        }
        let near_settings = Settings {
            min_net_pct: near_floor,
            ..settings.clone()
        };
        let live_venues: Vec<Venue> = state
            .trade
            .live_venues()
            .map(<[Venue]>::to_vec)
            .unwrap_or_default();
        // 第一步（只看内存）：挑出最优的一个；还没到时候、被间隔 / 冷却挡着就算了。
        let best = {
            let inner = self.inner.lock().await;
            let now = Instant::now();
            if inner
                .last_open
                .is_some_and(|at| at.elapsed() < Duration::from_secs(settings.open_gap_sec))
                || inner.attempting.is_some()
                || inner
                    .arming
                    .as_ref()
                    .is_some_and(|arming| !arming.is_finished())
                || inner
                    .last_near_arm
                    .is_some_and(|at| at.elapsed() < ARM_NEAR_GAP)
            {
                return;
            }
            let Some(best) = pick_near(
                lines,
                &near_settings,
                near_floor,
                &live_venues,
                connected,
                &inner.cooldown,
                now,
            ) else {
                return;
            };
            let key = best.hold_key();
            if inner
                .armed_at
                .get(&key)
                .is_some_and(|at| at.elapsed() < ARM_EVERY)
            {
                // 最优的刚准备过：歇着，不去给第二名做。
                return;
            }
            best
        };
        // 第二步：读台账里的仓位（走到这里至多每 ARM_EVERY 一次）。
        let Ok(exposed) = state.trade.exposed_positions(true).await else {
            return;
        };
        // 第三步：用仓位和限额再核一遍；通过了才记下时刻、真的去准备。没通过也记一笔，免得下一拍又来读台账。
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        inner.armed_at.retain(|_, at| at.elapsed() < ARM_EVERY);
        inner.armed_at.insert(best.hold_key(), now);
        let busy = exposed
            .iter()
            .any(|(_, symbol)| symbol.base.eq_ignore_ascii_case(&best.base));
        let today = Utc::now().date_naive();
        let open_auto = inner
            .persisted
            .opened
            .iter()
            .filter(|o| o.mode == Mode::Live && !o.add && exposed.iter().any(|(id, _)| id == &o.id))
            .count();
        let opened_today = inner
            .persisted
            .opened
            .iter()
            .filter(|o| o.mode == Mode::Live && o.at.date_naive() == today)
            .count();
        // 与 `step` 同一口径：0 = 不限。
        let positions_full = settings.max_positions > 0 && open_auto >= settings.max_positions;
        let daily_full = settings.daily_max_opens > 0 && opened_today >= settings.daily_max_opens;
        if busy || positions_full || daily_full {
            return;
        }
        inner.last_near_arm = Some(now);
        let body = Self::open_body(settings, &best);
        let state = Arc::clone(state);
        let base = best.base.clone();
        inner.arming = Some(tokio::spawn(async move {
            match crate::trade::prearm_for_auto(state, body).await {
                Ok(ms) => info!(
                    symbol = %base,
                    ms,
                    "价差自动交易：下单前的对账、现扫、保证金核对、预热已提前做好（预判）"
                ),
                Err(why) => {
                    info!(symbol = %base, "价差自动交易：没能提前准备（下单时照常现做）：{why}")
                }
            }
        }));
    }

    /// 空闲时（没有快要下单的信号）在后台提前设一个合约的杠杆（[`crate::trade::preset_leverage_for_auto`]），
    /// 好让它第一次开仓时不必在报价之后现设。和提前准备共用一个槽位：下单前会等它做完（最多
    /// [`ARM_WAIT`]），它也占交易锁、忙就让开。两次之间至少隔 [`PRESET_EVERY`]。
    fn maybe_preset(
        self: &Arc<Self>,
        inner: &mut Inner,
        state: &Arc<AppState>,
        settings: &Settings,
        view: &View,
    ) {
        if !preset_due(inner, settings) {
            return;
        }
        let live_venues = state.trade.live_venues().unwrap_or_default().to_vec();
        let now = Instant::now();
        let venue_last = &inner.preset_venue_last;
        let Some((venue, base)) = next_preset(
            view,
            settings,
            &|venue| live_venues.contains(&venue),
            &|venue| {
                venue_last
                    .get(&venue)
                    .is_none_or(|at| now.saturating_duration_since(*at) >= preset_gap(venue))
            },
            &inner.preset,
            now,
        ) else {
            inner.preset_idle = Some(now);
            return;
        };
        let leverage = settings.leverage;
        inner.preset_last = Some(now);
        inner.preset_venue_last.insert(venue, now);
        inner.preset.insert(
            (venue, base.clone()),
            PresetMark {
                leverage,
                at: now,
                done: false,
            },
        );
        let this = Arc::clone(self);
        let state = Arc::clone(state);
        inner.arming = Some(tokio::spawn(async move {
            let outcome =
                crate::trade::preset_leverage_for_auto(state, venue, base.clone(), leverage).await;
            let mut inner = this.inner.lock().await;
            match outcome {
                Ok(crate::trade::Preset::Checked(ms)) => {
                    if ms >= PRESET_SLOW_MS {
                        info!(%venue, symbol = %base, ms, "价差自动交易：已提前设好杠杆，之后在这家第一次开这个合约不用再现设");
                    } else {
                        debug!(%venue, symbol = %base, ms, "价差自动交易：杠杆核对过，已是目标值");
                    }
                    inner.preset.insert(
                        (venue, base),
                        PresetMark {
                            leverage,
                            at: Instant::now(),
                            done: true,
                        },
                    );
                }
                Ok(crate::trade::Preset::Skipped) => {
                    debug!(%venue, symbol = %base, "价差自动交易：有持仓或现在开不了，先不提前设杠杆");
                }
                Ok(crate::trade::Preset::Busy) => {
                    inner.preset.remove(&(venue, base));
                }
                Err(why) => {
                    info!(%venue, symbol = %base, "价差自动交易：提前设杠杆没成功（开仓时照常现设）：{why}");
                }
            }
        }));
    }

    /// 自动交易停看时，信号保持影子里已触发的虚拟仓位照常按行情跟下去（没有就不读快照）。
    async fn shadow_follow(self: &Arc<Self>, state: &Arc<AppState>) {
        if !self.inner.lock().await.hold_shadow.has_open() {
            return;
        }
        let view = state.rh_spread.view().await;
        let mut inner = self.inner.lock().await;
        let rows = inner.hold_shadow.follow_only(&view, Instant::now());
        inner.hold_shadow.write(rows, Utc::now());
    }

    /// 一次检查：够条件就下一单。
    async fn step(self: &Arc<Self>, state: &Arc<AppState>) {
        let settings = {
            let mut inner = self.inner.lock().await;
            let mut brief = self.brief();
            brief.settings = Some(inner.persisted.settings.clone());
            if !inner.persisted.settings.enabled {
                inner.status = match &inner.persisted.disabled_reason {
                    Some(reason) => format!("已关闭：{reason}"),
                    None => "未开启".into(),
                };
                brief.halted = Some(inner.status.clone());
                None
            } else if let Some(at) = inner.retry_at.filter(|at| *at > Instant::now()) {
                brief.halted = Some(format!(
                    "{}，{} 秒后再看",
                    inner.status,
                    at.duration_since(Instant::now()).as_secs()
                ));
                None
            } else {
                brief.halted = None;
                Some(inner.persisted.settings.clone())
            }
        };
        let Some(settings) = settings else {
            self.shadow_follow(state).await;
            return;
        };
        let live = settings.mode == Mode::Live;
        if live && !state.trade.live_can_trade() {
            self.set_status(if state.trade.live_opt().is_some() {
                "实盘是只读模式（ARB_WEB_LIVE=readonly），不能自动下单"
            } else {
                "实盘账户没连上，等重连"
            })
            .await;
            self.shadow_follow(state).await;
            return;
        }
        if live && state.trade.opens_paused() {
            self.set_status("开新仓已暂停（Telegram /pause），发 /resume 恢复")
                .await;
            self.shadow_follow(state).await;
            return;
        }
        // 快照里的 1800 行绝大多数这一刻离门槛很远。每次检查先挑出值得细看的那几行（[`worth_a_look`]），
        // 平时只在它们上面扣资金费、过规则；要整份快照的地方（已经有信号在计时、要跟虚拟仓位、轮到提前设
        // 杠杆）才把整份扣一遍。两种做法结论一样：被筛掉的行本来在下面每一步里也都是「不算」。
        let mut shell = state.rh_spread.view().await;
        let all_lines = std::mem::take(&mut shell.lines);
        let snapshot = if settings.deduct_funding {
            state.cache.get().await
        } else {
            None
        };
        let deducted = |lines: Vec<Arc<Line>>, whole: bool| {
            let mut view = View {
                lines,
                ..shell.clone()
            };
            if settings.deduct_funding {
                let mut caches = self
                    .funding
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let cache = if whole { &mut caches.1 } else { &mut caches.0 };
                cache.apply(&mut view, snapshot.clone(), funding_hold_hours(&settings));
            }
            view
        };
        let hot = deducted(
            all_lines
                .iter()
                .filter(|line| worth_a_look(line, &settings))
                .cloned()
                .collect(),
            false,
        );
        self.brief().rules = hot
            .lines
            .iter()
            .filter_map(|line| {
                let best = line.best.as_ref().filter(|best| best.signal)?;
                Some((
                    (line.pair.clone(), line.base.clone(), best.direction),
                    rule_block(line, best.direction, &settings),
                ))
            })
            .collect();
        let now = Utc::now();
        let recent = recent_gate_blocked(&hot, &settings);
        if !recent.is_empty() {
            let mut inner = self.inner.lock().await;
            let now_i = Instant::now();
            for (base, reason) in recent {
                if inner
                    .recent_gate_logged
                    .get(&base)
                    .is_none_or(|at| now_i.saturating_duration_since(*at) >= RECENT_GATE_LOG_EVERY)
                {
                    inner.recent_gate_logged.insert(base.clone(), now_i);
                    info!(symbol = %base, "价差自动交易：相对近期偏离门槛没过：{reason}");
                }
            }
        }
        let fast = fast_move_blocked(&hot, &settings);
        if !fast.is_empty() {
            let mut inner = self.inner.lock().await;
            let now_i = Instant::now();
            for (base, reason) in fast {
                if inner
                    .fast_move_logged
                    .get(&base)
                    .is_none_or(|at| now_i.saturating_duration_since(*at) >= FAST_MOVE_LOG_EVERY)
                {
                    inner.fast_move_logged.insert(base.clone(), now_i);
                    info!(symbol = %base, "价差自动交易：{reason}");
                }
            }
        }
        // 先只更新计时：没有任何一条达到门槛时不必读台账（每秒读一次台账是白费）。
        {
            let mut inner = self.inner.lock().await;
            inner.track(&hot, &settings, Instant::now());
            // 还有虚拟仓位在跟：它们的行不一定还在「值得细看」里，加上这几行（不必整份）。
            let shadow_view = inner.hold_shadow.has_open().then(|| {
                deducted(
                    all_lines
                        .iter()
                        .filter(|line| {
                            worth_a_look(line, &settings) || inner.hold_shadow.tracks(line)
                        })
                        .cloned()
                        .collect(),
                    true,
                )
            });
            let inner_ref = &mut *inner;
            let history = &inner_ref.history;
            let at = Instant::now();
            let hist = |key: &HoldKey| {
                history
                    .get(key)
                    .filter(|(checked, _)| at.saturating_duration_since(*checked) < HISTORY_RECHECK)
                    .map(|(_, verdict)| verdict.is_none())
            };
            let rows = inner_ref.hold_shadow.observe(
                shadow_view.as_ref().unwrap_or(&hot),
                &settings,
                &hist,
                at,
                Utc::now(),
            );
            inner_ref.hold_shadow.write(rows, Utc::now());
            if inner.since.is_empty() {
                if preset_due(&inner, &settings) {
                    let view = deducted(all_lines.clone(), true);
                    self.maybe_preset(&mut inner, state, &settings, &view);
                }
                inner.status = match blocked_signal_reason(&hot, &settings) {
                    Some(reason) => format!("监控中：{reason}"),
                    None => format!(
                        "监控中：没有回到正常净收益 ≥ {}% 且可成交的信号",
                        settings.min_net_pct.normalize()
                    ),
                };
                drop(inner);
                // 没有信号在计时：但可能有快到的。趁现在把下单前的只读准备先做掉，信号一来就不用再等。
                self.maybe_arm_near(state, &settings, &all_lines, &shell.connected)
                    .await;
                return;
            }
        }
        // 有信号在计时了：下面的计时、挑候选、补仓都只看候选，候选只会出在「值得细看」的行里（见
        // [`worth_a_look`]），所以接着用这几行。整份快照只有提前设杠杆要（它不看门槛），轮到时再建。
        let view = hot;
        let exposed = match state.trade.exposed_positions(live).await {
            Ok(exposed) => exposed,
            Err(error) => {
                self.set_status(&format!("读不了台账：{error}")).await;
                return;
            }
        };
        // 补仓要知道每笔仓位的实际开仓价差与补过几次：只在开了补仓时才多读一次台账。
        let targets = if settings.add_min_improve_pct.is_some() {
            match state.trade.add_targets(live).await {
                Ok(targets) => targets,
                Err(error) => {
                    self.set_status(&format!("读不了台账：{error}")).await;
                    return;
                }
            }
        } else {
            Vec::new()
        };
        // 历史分位门槛（要拉两家 K 线，结论缓存 5 分钟）：信号保持满 [`ARM_MIN_HOLD`] 就在后台开始算，
        // 保持够了的那一刻结论多半已经在了，不必那时再等 K 线；已经保持够了还没有结论的，当场算。
        if settings.history_gate_pct.is_some() {
            let (due, early): (Vec<Candidate>, Vec<Candidate>) = {
                let mut inner = self.inner.lock().await;
                let now_i = Instant::now();
                let held = |c: &Candidate| {
                    inner
                        .since
                        .get(&c.hold_key())
                        .map_or(0, |at| ms_since(now_i, *at))
                };
                let wanted: Vec<Candidate> = view
                    .lines
                    .iter()
                    .filter_map(|line| candidate_of(line, &settings))
                    .filter(|c| {
                        held(c) >= duration_ms(ARM_MIN_HOLD).min(settings.required_hold_ms(c))
                            && !inner.history_pending.contains(&c.hold_key())
                            && inner
                                .history
                                .get(&c.hold_key())
                                .is_none_or(|(at, _)| at.elapsed() >= HISTORY_RECHECK)
                    })
                    .take(3)
                    .collect();
                let (due, early): (Vec<Candidate>, Vec<Candidate>) = wanted
                    .into_iter()
                    .partition(|c| held(c) >= settings.required_hold_ms(c));
                for c in &early {
                    inner.history_pending.insert(c.hold_key());
                }
                (due, early)
            };
            for c in early {
                let auto = Arc::clone(self);
                let settings = settings.clone();
                tokio::spawn(async move {
                    let verdict = history_verdict(&c, &settings).await;
                    if let Some(why) = &verdict {
                        info!(symbol = %c.base, long = %c.long, short = %c.short, "价差自动交易：历史分位门槛没过：{why}");
                    }
                    let mut inner = auto.inner.lock().await;
                    inner.history_pending.remove(&c.hold_key());
                    inner
                        .history
                        .insert(c.hold_key(), (Instant::now(), verdict));
                });
            }
            for c in due {
                let verdict = history_verdict(&c, &settings).await;
                if let Some(why) = &verdict {
                    info!(symbol = %c.base, long = %c.long, short = %c.short, "价差自动交易：历史分位门槛没过：{why}");
                }
                self.inner
                    .lock()
                    .await
                    .history
                    .insert(c.hold_key(), (Instant::now(), verdict));
            }
            let mut inner = self.inner.lock().await;
            inner
                .history
                .retain(|_, (at, _)| at.elapsed() < HISTORY_RECHECK);
        }
        let choice = {
            let mut inner = self.inner.lock().await;
            let today = now.date_naive();
            let mode = settings.mode;
            let opened_today = inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && o.at.date_naive() == today)
                .count();
            let open_auto = inner
                .persisted
                .opened
                .iter()
                .filter(|o| o.mode == mode && !o.add && exposed.iter().any(|(id, _)| id == &o.id))
                .count();
            // 币安的自动仓位已经到上限：这一轮把币安的开关当关着（币安的组合不再被选中）。
            let open_binance = inner
                .persisted
                .opened
                .iter()
                .filter(|o| {
                    o.mode == mode
                        && !o.add
                        && o.binance
                        && exposed.iter().any(|(id, _)| id == &o.id)
                })
                .count();
            let maker_today = inner
                .maker_attempts
                .filter(|(day, _)| *day == today)
                .map_or(0, |(_, n)| n);
            let capped = effective_settings(&settings, open_binance, maker_today);
            let pick_settings: &Settings = &capped;
            // 计时每次都更新（哪怕这一刻因为限额不下单），免得限额解除时拿旧的起点当「已保持很久」。
            inner.track(&view, &settings, Instant::now());
            let positions_full = settings.max_positions > 0 && open_auto >= settings.max_positions;
            {
                let mut brief = self.brief();
                brief.exposed = exposed
                    .iter()
                    .map(|(_, symbol)| symbol.base.to_ascii_uppercase())
                    .collect();
                brief.history = inner
                    .history
                    .iter()
                    .map(|(key, (_, why))| (key.clone(), why.clone()))
                    .collect();
                brief.cooldown = inner.cooldown.clone();
                brief.halted = positions_full.then(|| {
                    format!(
                        "自动仓位已到上限（{open_auto}/{}），等平仓",
                        settings.max_positions
                    )
                });
            }
            // 执行中断后的暂停：放在所有「这一轮不下单」的出口之前，恢复 / 超时关闭照常判。
            if !self.interrupted_gate(&mut inner, state).await {
                return;
            }
            if positions_full && settings.add_min_improve_pct.is_none() {
                inner.status = format!(
                    "自动仓位已到上限（{open_auto}/{}），等平仓",
                    settings.max_positions
                );
                return;
            }
            if settings.daily_max_opens > 0 && opened_today >= settings.daily_max_opens {
                inner.status = format!("今天（UTC）已自动开 {opened_today} 笔，到上限了");
                self.brief().halted = Some(inner.status.clone());
                return;
            }
            if inner
                .last_open
                .is_some_and(|at| at.elapsed() < Duration::from_secs(settings.open_gap_sec))
            {
                inner.status = format!("刚开过一笔，间隔 {} 秒再看", settings.open_gap_sec);
                self.brief().halted = Some(inner.status.clone());
                return;
            }
            let instant = Instant::now();
            inner.track(&view, &settings, instant);
            let since = inner.since.clone();
            let held = |c: &Candidate| {
                since
                    .get(&c.hold_key())
                    .map_or(0, |at| ms_since(instant, *at))
            };
            let cooldown = inner.cooldown.clone();
            let gate_on = settings.history_gate_pct.is_some();
            let history_now = inner.history.clone();
            let busy = |candidate: &Candidate| {
                let base = &candidate.base;
                if exposed
                    .iter()
                    .any(|(_, symbol)| symbol.base.eq_ignore_ascii_case(base))
                {
                    return Some("已有这个合约的仓位".to_string());
                }
                if let Some(why) =
                    candidate_history_reason(candidate, gate_on, &history_now, instant)
                {
                    return Some(why);
                }
                cooldown
                    .get(base)
                    .filter(|until| **until > instant)
                    .map(|until| {
                        format!(
                            "冷却中（还有 {} 秒）",
                            until.duration_since(instant).as_secs()
                        )
                    })
            };
            let live_venues: Option<Vec<Venue>> = state.trade.live_venues().map(<[Venue]>::to_vec);
            // 下单明显变慢、暂停中的交易所（见 [`venue_latency`]）：这段时间不用它开新仓、补仓。
            let slow: Vec<Venue> = Venue::ALL
                .iter()
                .copied()
                .filter(|venue| inner.latency.paused(*venue, instant).is_some())
                .collect();
            let tradable = |venue: Venue| {
                !slow.contains(&venue)
                    && match &live_venues {
                        _ if !live => true,
                        Some(venues) => venues.contains(&venue),
                        None => false,
                    }
            };
            // 先看能不能新开（名额没满时）；新开不了、又开了补仓，再看已有仓位能不能补。
            // 补仓不占同时持仓的名额。
            let opened = if !positions_full {
                pick_checked(&view, pick_settings, now, &tradable, &held, &busy)
            } else {
                Err(format!(
                    "自动仓位已到上限（{open_auto}/{}），等平仓",
                    settings.max_positions
                ))
            };
            // 还在保持中、保持够了就会选中的那一笔：提前把下单前的只读准备做掉（实盘才做）。
            // 没有这样的一笔：趁空提前设杠杆。
            if live && opened.is_err() {
                match pick_checked(&view, pick_settings, now, &tradable, &|_| u64::MAX, &busy) {
                    Ok(next) if !positions_full => {
                        let held_now = held(&next);
                        Self::maybe_arm(&mut inner, state, &settings, &next, held_now);
                    }
                    Ok(_) => {}
                    Err(_) if preset_due(&inner, &settings) => {
                        let whole = deducted(all_lines.clone(), true);
                        self.maybe_preset(&mut inner, state, &settings, &whole);
                    }
                    Err(_) => {}
                }
            }
            let cooling = |candidate: &Candidate| {
                if let Some(why) =
                    candidate_history_reason(candidate, gate_on, &history_now, instant)
                {
                    return Some(why);
                }
                let base = &candidate.base;
                cooldown
                    .get(base)
                    .filter(|until| **until > instant)
                    .map(|until| {
                        format!(
                            "冷却中（还有 {} 秒）",
                            until.duration_since(instant).as_secs()
                        )
                    })
            };
            match opened {
                Ok(mut candidate) => {
                    if inner
                        .base_size_only
                        .get(&candidate.base)
                        .is_some_and(|until| *until > Instant::now())
                    {
                        candidate.upsized = None;
                    }
                    inner.attempting = Some(candidate.base.clone());
                    inner.status = format!("正在为 {} 下单…", candidate.base);
                    self.brief().outcome.insert(
                        candidate.base.to_ascii_uppercase(),
                        (Instant::now(), "正在自动下单".into()),
                    );
                    Choice::Open(candidate)
                }
                Err(why) if settings.add_min_improve_pct.is_none() => {
                    inner.status = format!("监控中：{why}");
                    return;
                }
                Err(why) => {
                    let max_size = state
                        .settings
                        .max_position_usdt
                        .and_then(Decimal::from_f64_retain);
                    match pick_add_checked(
                        &view, &settings, now, &tradable, &held, &cooling, &targets, max_size,
                    ) {
                        Ok((candidate, target)) => {
                            inner.attempting = Some(candidate.base.clone());
                            inner.status =
                                format!("正在往 {} 上补仓（{}）…", target.id, candidate.base);
                            Choice::Add(candidate, target)
                        }
                        Err(add_why) => {
                            inner.status = format!("监控中：{why}；补仓：{add_why}");
                            return;
                        }
                    }
                }
            }
        };
        match choice {
            Choice::Open(candidate) => {
                // 提前准备还在做：等它做完（它占着交易锁），下单就能直接用它的结果。
                let arming = self.inner.lock().await.arming.take();
                if let Some(arming) = arming {
                    let _ = tokio::time::timeout(ARM_WAIT, arming).await;
                }
                self.attempt(state, &settings, &candidate).await;
            }
            Choice::Add(candidate, target) => {
                // 同上：提前设杠杆也占交易锁，等它做完再补，免得被判「交易台忙」白等 5 秒。
                let arming = self.inner.lock().await.arming.take();
                if let Some(arming) = arming {
                    let _ = tokio::time::timeout(ARM_WAIT, arming).await;
                }
                self.attempt_add(state, &settings, &candidate, &target)
                    .await;
            }
        }
    }

    /// 往已有仓位上补仓（见 [`pick_add`]）。结果处理与开仓相同：中断就关掉自动交易，
    /// 没补成就让这个合约冷却。
    async fn attempt_add(
        &self,
        state: &Arc<AppState>,
        settings: &Settings,
        candidate: &Candidate,
        target: &AddTarget,
    ) {
        let symbol = format!("{}/USDT", candidate.base);
        let body = crate::trade::with_entry_gate(
            crate::trade::auto_open_body(
                settings.mode,
                &symbol,
                candidate.long,
                candidate.short,
                candidate.size(settings),
                settings.leverage,
                arb_exec::MarginMode::Isolated,
                &arb_exec::TaskRules::default(),
            ),
            settings.entry_gate(candidate.long, candidate.short),
        );
        let mode = if settings.mode == Mode::Live {
            "实盘"
        } else {
            "纸面"
        };
        let what = format!(
            "{mode} 补仓 {} {}（多 {} / 空 {}）{} USDT，新信号价差 {}% vs 原仓位 {}%，第 {} 次",
            target.id,
            candidate.base,
            candidate.long,
            candidate.short,
            candidate.size(settings).normalize(),
            candidate.entry_pct.round_dp(3),
            target.entry_spread_pct.round_dp(3),
            target.adds + 1
        );
        warn!(symbol = %candidate.base, "价差自动交易：尝试{what}");
        let max_total = candidate.size(settings) * settings.add_max_multiple;
        let (status, response) =
            crate::trade::add_for_auto(Arc::clone(state), body, target.id.clone(), max_total).await;
        let what = match sized_note(&response, candidate.size(settings)) {
            Some(note) => format!("{what}（{note}）"),
            None => what,
        };
        let reason = response["error"].as_str().unwrap_or_default().to_string();
        let now = Instant::now();
        let mut inner = self.inner.lock().await;
        inner.attempting = None;
        let base = Some(candidate.base.as_str());
        inner
            .cooldown
            .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
        if status.is_success() {
            let added = response["added"].as_bool().unwrap_or(false);
            let note = response["position"]["note"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            inner.last_open = Some(now);
            if added {
                inner.persisted.opened.push(Opened {
                    id: target.id.clone(),
                    symbol: symbol.clone(),
                    mode: settings.mode,
                    at: Utc::now(),
                    add: true,
                    binance: candidate.binance,
                    size_usdt: candidate.size(settings),
                });
                let excess = inner.persisted.opened.len().saturating_sub(OPENED_KEEP);
                inner.persisted.opened.drain(..excess);
                Self::push(&mut inner, "added", base, format!("已{what}。{note}"));
                inner.status = format!("已补仓 {}", target.id);
            } else {
                info!(symbol = %candidate.base, position = %target.id, "价差自动交易：没补成：{note}");
                Self::push(
                    &mut inner,
                    "rejected",
                    base,
                    format!("没补成：{what}。{note}"),
                );
                inner.status = format!("{} 没补成", target.id);
            }
            self.save(&inner.persisted);
            drop(inner);
            if settings.mode == Mode::Paper {
                state.alerts.notify_order(format!(
                    "🤖 自动交易（纸面）{}{what}。{note}",
                    if added { "已" } else { "没补成：" }
                ));
            }
            return;
        }
        match classify(status, &response) {
            Outcome::Busy => {
                inner.cooldown.remove(&candidate.base);
                inner.retry_at = Some(now + BUSY_RETRY);
                inner.status = "交易台忙（规则轮或手动操作），5 秒后再试".into();
            }
            Outcome::Paused => {
                inner.retry_at = Some(now + MIN_OPEN_GAP);
                info!(symbol = %candidate.base, position = %target.id, "价差自动交易：没补仓：{reason}");
                Self::push(&mut inner, "paused", base, format!("没补仓：{reason}"));
                inner.status = format!("没补仓：{reason}");
            }
            Outcome::Interrupted => {
                let text = format!("补仓中断、结果未知：{reason}（{what}）");
                error!(symbol = %candidate.base, "价差自动交易：{text}");
                Self::pause_interrupted(&mut inner, state, text);
            }
            Outcome::Misconfigured => {
                drop(inner);
                let text = format!("补仓请求被拒（{}）：{reason}", status.as_u16());
                self.disable(text.clone()).await;
                state
                    .alerts
                    .notify_always(format!("⛔ 价差自动交易已关闭：{text}"));
            }
            _ => {
                inner.retry_at = Some(now + REJECT_GAP);
                info!(
                    symbol = %candidate.base,
                    position = %target.id,
                    status = status.as_u16(),
                    "价差自动交易：补仓前检查没通过（冷却 10 分钟）：{reason}"
                );
                Self::push(
                    &mut inner,
                    "rejected",
                    base,
                    format!("补仓前检查没通过（冷却 10 分钟）：{reason}"),
                );
                inner.status = format!("{} 补仓没通过检查", candidate.base);
            }
        }
    }

    /// 这一轮不开仓的原因：页面上的状态，也记进摘要（这些都挡住所有新开仓）。
    async fn set_status(&self, status: &str) {
        self.inner.lock().await.status = status.to_string();
        self.brief().halted = Some(status.to_string());
    }

    fn brief(&self) -> std::sync::MutexGuard<'_, Brief> {
        self.brief
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Telegram 价差提醒里的一句：自动交易这一刻会不会下这一笔、不下为什么。`line` 是提醒用的那一行
    /// （没扣资金费，扣过的结论在摘要里），`best` 是它的信号方向。还没跑过一轮时返回 `None`。
    pub fn alert_note(&self, line: &Line, best: &crate::rh_spread::Best) -> Option<String> {
        let brief = self.brief();
        let settings = brief.settings.as_ref()?;
        let rule = brief
            .rules
            .get(&(line.pair.clone(), line.base.clone(), best.direction))
            .cloned()
            .unwrap_or_else(|| rule_block(line, best.direction, settings));
        if let Some(halted) = &brief.halted {
            return Some(match rule {
                Some(rule) => {
                    format!("自动交易：{halted}，不会自动下单；就算开着，这条也不下：{rule}。")
                }
                None => format!("自动交易：{halted}，不会自动下单。"),
            });
        }
        let upper = line.base.to_ascii_uppercase();
        if let Some((at, text)) = brief
            .outcome
            .get(&upper)
            .filter(|(at, _)| at.elapsed() < OUTCOME_FRESH)
        {
            return Some(format!(
                "自动交易 {} 秒前刚处理过 {}：{text}。",
                at.elapsed().as_secs(),
                line.base
            ));
        }
        if let Some(rule) = rule {
            return Some(format!("自动交易不下这一笔：{rule}。"));
        }
        if brief.exposed.contains(&upper) {
            return Some(format!(
                "自动交易不下这一笔：已有 {} 的仓位，同一合约不重复开（往上补仓另按补仓规则）。",
                line.base
            ));
        }
        let (long, short, _) = line.legs(best.direction)?;
        match brief.history.get(&(line.base.clone(), long, short)) {
            Some(Some(why)) => {
                return Some(format!("自动交易不下这一笔：历史分位门槛没过：{why}。"));
            }
            None if settings.history_gate_pct.is_some() => {
                return Some("自动交易还没下：历史价差分位还在算，算完过了门槛才下。".into());
            }
            _ => {}
        }
        if let Some(until) = brief
            .cooldown
            .get(&line.base)
            .filter(|until| **until > Instant::now())
        {
            return Some(format!(
                "自动交易不下这一笔：{} 冷却中（还有 {} 秒）。",
                line.base,
                until.duration_since(Instant::now()).as_secs()
            ));
        }
        let size = line
            .legs(best.direction)
            .and_then(|(_, _, leg)| upsize(line, leg, settings))
            .map_or_else(
                || format!("单笔 {} USDT", settings.size_usdt.normalize()),
                |upsized| upsized.note,
            );
        Some(format!(
            "满足自动开仓规则，自动交易会下这一笔（{size}；下单前还要过对账与盘口复核，结果另有消息）。"
        ))
    }

    async fn attempt(&self, state: &Arc<AppState>, settings: &Settings, candidate: &Candidate) {
        let symbol = format!("{}/USDT", candidate.base);
        let body = Self::open_body(settings, candidate);
        let mode = if settings.mode == Mode::Live {
            "实盘"
        } else {
            "纸面"
        };
        let what = format!(
            "{mode} {} 多 {} / 空 {}，{}{} USDT × {}x，信号 {}s，回到正常预估 {}%{}",
            candidate.base,
            candidate.long,
            candidate.short,
            candidate
                .maker
                .map(|m| format!("{} 挂单 {} 秒，", m.venue, m.rest_ms / 1000))
                .unwrap_or_default(),
            candidate.size(settings).normalize(),
            settings.leverage.normalize(),
            candidate.signal_sec,
            candidate.net_to_normal_pct.round_dp(3),
            candidate
                .target_pct
                .map(|t| format!("，收敛目标 {t}%"))
                .unwrap_or_default()
        );
        // 近 5 秒两条腿所在场所的中间价波动幅度：不论「急行情不开仓」开没开都记，事后好和成交结果对着看。
        let moved = candidate.recent_move_pct.map_or_else(
            || "还没记满窗口".to_string(),
            |(long, short)| {
                format!(
                    "多腿 {}% / 空腿 {}%",
                    long.round_dp(3).normalize(),
                    short.round_dp(3).normalize()
                )
            },
        );
        let context = format!(
            "近 12 小时可成交价差中位 {}，已扣持仓期资金费 {}",
            candidate
                .recent_entry_pct
                .map_or("样本不够".to_string(), |pct| format!(
                    "{}%",
                    pct.round_dp(3)
                )),
            candidate
                .funding_cost_pct
                .map_or("（没扣）".to_string(), |pct| format!(
                    "{}%",
                    pct.round_dp(4)
                ))
        );
        let what = match &candidate.upsized {
            Some(upsized) => format!("{what}（{}）", upsized.note),
            None => what,
        };
        warn!(symbol = %candidate.base, "价差自动交易：尝试开仓 {what}（近 5 秒价格波动：{moved}；{context}）");
        let (status, response) = crate::trade::open_for_auto(Arc::clone(state), body, true).await;
        // 按盘口深度缩小过单笔：事件里写实际下的，不能让「800 USDT」的上限当成实际。
        let what = match sized_note(&response, candidate.size(settings)) {
            Some(note) => format!("{what}（{note}）"),
            None => what,
        };
        let outcome = classify_attempt(candidate, status, &response);
        let reason = response["error"].as_str().unwrap_or_default().to_string();
        let now = Instant::now();
        let note = match &outcome {
            Outcome::Opened(id) => format!("已自动开仓 {id}"),
            Outcome::Unwound(id) => format!("开仓以回滚收场（{id}）"),
            Outcome::NoFill(id) => format!("币安挂单没成交或成交太碎（{id}）"),
            Outcome::Untraded(id) => format!("第一腿没成交（{id}），什么都没发生"),
            Outcome::Rejected => format!("下单前检查没通过：{reason}"),
            Outcome::Busy => "交易台忙（规则轮或手动操作），稍后再试".into(),
            Outcome::Paused => format!("没下单：{reason}"),
            Outcome::Interrupted => format!("执行中断、结果未知：{reason}"),
            Outcome::Misconfigured => format!("下单请求被拒：{reason}"),
        };
        self.brief()
            .outcome
            .insert(candidate.base.to_ascii_uppercase(), (now, note));
        let mut inner = self.inner.lock().await;
        inner.attempting = None;
        let base = Some(candidate.base.as_str());
        // 真正发过挂单的（成交 / 没成交 / 回滚 / 中断）记一次；被拒在发单之前、交易台忙、暂停的不算。
        if candidate.maker.is_some()
            && matches!(
                outcome,
                Outcome::Opened(_)
                    | Outcome::NoFill(_)
                    | Outcome::Unwound(_)
                    | Outcome::Interrupted
            )
        {
            let today = Utc::now().date_naive();
            let count = inner
                .maker_attempts
                .filter(|(day, _)| *day == today)
                .map_or(0, |(_, n)| n);
            inner.maker_attempts = Some((today, count + 1));
        }
        match outcome {
            Outcome::Opened(id) => {
                // 第二腿靠强制补腿追出来的（起点就低于保本线）不算「开成了」：不清零连续失败计数，
                // 否则一连串都靠强制补腿建仓（实际在亏）时永远到不了 MAX_FAILURES，自动交易不会自己停。
                let forced_open = response["position"]["note"]
                    .as_str()
                    .is_some_and(|note| note.contains(arb_exec::FORCED_HEDGE_NOTE));
                if !forced_open {
                    inner.persisted.failures = 0;
                }
                inner.persisted.opened.push(Opened {
                    id: id.clone(),
                    symbol: symbol.clone(),
                    mode: settings.mode,
                    at: Utc::now(),
                    add: false,
                    binance: candidate.binance,
                    size_usdt: candidate.size(settings),
                });
                inner.persisted.tally.opened += 1;
                inner.persisted.tally.opened_usdt =
                    add_usdt(inner.persisted.tally.opened_usdt, candidate.size(settings));
                let excess = inner.persisted.opened.len().saturating_sub(OPENED_KEEP);
                inner.persisted.opened.drain(..excess);
                inner.last_open = Some(now);
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
                // 两腿各自的下单往返进延迟样本：哪家明显变慢了，就暂停用它开新仓。
                let mut paused = Vec::new();
                for (venue, ms) in venue_latency::samples_of(&response["position"]["open_report"]) {
                    if let Some(why) = inner.latency.record(venue, ms, now) {
                        paused.push(why);
                    }
                }
                for why in &paused {
                    warn!("价差自动交易：{why}");
                    Self::push(&mut inner, "venue_slow", None, why.clone());
                    state.alerts.notify_always(format!("⚠️ {why}"));
                }
                // 第二腿是强制补腿追出来的：这笔起点就是负价差、没有止损，事件里必须说出来
                // （它算开仓成功，失败计数会清零，不提示就看不见）。
                let forced = forced_hedge_note(&response)
                    .map(|note| format!("。⚠️ {note}"))
                    .unwrap_or_default();
                Self::push(
                    &mut inner,
                    "opened",
                    base,
                    format!("已开仓 {id}：{what}{forced}"),
                );
                inner.status = format!("已开仓 {id}");
                self.save(&inner.persisted);
                drop(inner);
                info!(position = %id, "价差自动交易：开仓成功");
                if settings.mode == Mode::Paper {
                    state
                        .alerts
                        .notify_order(format!("🤖 自动交易（纸面）开仓 {id}：{what}"));
                }
                // 实盘详细结果已由下单路径播报，不再额外补发「上面这笔是自动开的」。
            }
            Outcome::Unwound(id) => {
                inner.persisted.failures += 1;
                inner.persisted.tally.unwound += 1;
                inner.persisted.tally.unwound_usdt =
                    add_usdt(inner.persisted.tally.unwound_usdt, candidate.size(settings));
                let failures = inner.persisted.failures;
                inner.last_open = Some(now);
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN * 3);
                Self::push(
                    &mut inner,
                    "unwound",
                    base,
                    format!("开仓以回滚收场 {id}（连续 {failures} 次）：{what}"),
                );
                self.save(&inner.persisted);
                drop(inner);
                if failures >= MAX_FAILURES {
                    let reason = format!("开仓连续 {failures} 次以回滚收场");
                    self.disable(reason.clone()).await;
                    state.alerts.notify_always(format!(
                        "⛔ 价差自动交易已关闭：{reason}。查明原因后在面板上重新开启。"
                    ));
                }
            }
            Outcome::NoFill(id) => {
                // 挂单一点都没成交就撤了（没花钱），或成交太碎回滚了一小块（只付一笔平仓费）：都不是失败。
                // 这个合约歇一会儿再挂，别连着打交易所。
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + MAKER_NOFILL_COOLDOWN);
                inner.retry_at = Some(now + REJECT_GAP);
                let note = response["position"]["note"].as_str().unwrap_or_default();
                Self::push(
                    &mut inner,
                    "maker_unfilled",
                    base,
                    format!("挂单没成交或成交太碎（{id}）：{what}。{note}"),
                );
                inner.status = format!("{} 挂单没成交，歇 10 分钟", candidate.base);
            }
            Outcome::Untraded(id) => {
                // 第一腿就被交易所拒了 / 没吃到：什么都没发生，不计连续失败也不清零。
                // 同一个原因（比如周末价格带）往往一会儿还在：这个合约多歇一会儿，别连着打交易所。
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN * 3);
                inner.retry_at = Some(now + REJECT_GAP);
                let note = response["position"]["note"].as_str().unwrap_or_default();
                Self::push(
                    &mut inner,
                    "untraded",
                    base,
                    format!(
                        "第一腿没成交（{id}），什么都没发生，不计连续失败（冷却 30 分钟）：{what}。{note}"
                    ),
                );
                inner.status = format!("{} 第一腿没成交，歇 30 分钟", candidate.base);
            }
            Outcome::Rejected if upsized_margin_short(candidate, &reason) => {
                info!(
                    symbol = %candidate.base,
                    "价差自动交易：放大后的单笔保证金不够，10 分钟内按原名义做：{reason}"
                );
                // 放大后的单笔保证金不够：这个合约一段时间内按原来的名义做，不冷却，下一轮就能再试。
                inner
                    .base_size_only
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
                inner.retry_at = Some(now + REJECT_GAP);
                Self::push(
                    &mut inner,
                    "rejected",
                    base,
                    format!(
                        "按盘口放大到 {} USDT 时保证金不够，10 分钟内这个合约按原来的 {} USDT 做：{reason}",
                        candidate.size(settings).normalize(),
                        settings.size_usdt.normalize()
                    ),
                );
                inner.status = format!("{} 放大后保证金不够，改按原名义", candidate.base);
            }
            Outcome::Rejected => {
                // 被拒的原因原来只进面板事件（重启就没了）：日志里也记一行，复盘时才查得到。
                info!(
                    symbol = %candidate.base,
                    status = status.as_u16(),
                    "价差自动交易：下单前检查没通过（冷却 10 分钟）：{reason}"
                );
                inner
                    .cooldown
                    .insert(candidate.base.clone(), now + SYMBOL_COOLDOWN);
                // 每次尝试都要对账、现扫一轮实盘场所：被拒后也歇一会儿，不连着打交易所。
                inner.retry_at = Some(now + REJECT_GAP);
                Self::push(
                    &mut inner,
                    "rejected",
                    base,
                    format!("下单前检查没通过（冷却 10 分钟）：{reason}"),
                );
                inner.status = format!("{} 没通过下单前检查", candidate.base);
                inner.persisted.tally.rejected += 1;
                self.save(&inner.persisted);
            }
            Outcome::Busy => {
                inner.retry_at = Some(now + BUSY_RETRY);
                inner.status = "交易台忙（规则轮或手动操作），5 秒后再试".into();
            }
            Outcome::Paused => {
                inner.retry_at = Some(now + MIN_OPEN_GAP);
                info!(symbol = %candidate.base, "价差自动交易：没下单：{reason}");
                Self::push(&mut inner, "paused", base, format!("没下单：{reason}"));
                inner.status = format!("没下单：{reason}");
                if is_daily_pnl_unknown(&reason)
                    && inner.daily_unknown_alerted.is_none_or(|at| {
                        now.saturating_duration_since(at) >= DAILY_UNKNOWN_ALERT_EVERY
                    })
                {
                    inner.daily_unknown_alerted = Some(now);
                    warn!("价差自动交易在等：{reason}");
                    state.alerts.notify_always(format!(
                        "⏸ 价差自动交易暂时开不了新仓（没有关闭）：{reason}。把这些仓位的盈亏补进台账后自动恢复。"
                    ));
                }
            }
            Outcome::Interrupted => {
                let text = format!("执行中断、结果未知：{reason}（{what}）");
                error!(symbol = %candidate.base, "价差自动交易：{text}");
                Self::pause_interrupted(&mut inner, state, text);
            }
            Outcome::Misconfigured => {
                drop(inner);
                let text = format!("下单请求被拒（{}）：{reason}", status.as_u16());
                self.disable(text.clone()).await;
                state
                    .alerts
                    .notify_always(format!("⛔ 价差自动交易已关闭：{text}"));
            }
        }
    }
}

/// 按盘口放大过的单子在下单前检查里因为可用保证金不够被拒（[`arb_exec::desk::check_legs_collateral`]
/// 的报错）：这个合约一段时间内改按原来的名义做，不冷却。
pub fn upsized_margin_short(candidate: &Candidate, reason: &str) -> bool {
    candidate.upsized.is_some() && reason.contains("可用保证金")
}

/// 开仓响应里仓位备注是「强制补腿追出来的」或「两腿数量对齐、没有回滚」就把它取出来（执行器写的，见
/// [`arb_exec::FORCED_HEDGE_NOTE`]、[`arb_exec::BALANCED_HEDGE_NOTE`]）：这两种仓位要提醒人看一眼。
pub fn forced_hedge_note(response: &Value) -> Option<String> {
    response["position"]["note"]
        .as_str()
        .filter(|note| {
            note.contains(arb_exec::FORCED_HEDGE_NOTE)
                || note.contains(arb_exec::BALANCED_HEDGE_NOTE)
        })
        .map(str::to_string)
}

/// 开仓响应里实际单笔比请求的上限小（按盘口深度缩小过）时的说明；没缩小 / 没有这个信息返回 `None`。
pub fn sized_note(response: &Value, cap: Decimal) -> Option<String> {
    let prepared = response.get("prepared")?;
    let size: Decimal = prepared["size_usdt"].as_str()?.parse().ok()?;
    if size >= cap {
        return None;
    }
    let depth = |key: &str| {
        prepared["fit"][key]
            .as_str()
            .and_then(|raw| raw.parse::<Decimal>().ok())
            .map(|value| value.round_dp(0).normalize().to_string())
    };
    Some(format!(
        "实际单笔 {} USDT：按盘口深度从上限 {} 缩小，第一腿限价内约 {} USDT、第二腿保本价内约 {} USDT",
        size.round_dp(2).normalize(),
        cap.normalize(),
        depth("first_depth_usdt").unwrap_or_else(|| "?".into()),
        depth("second_depth_usdt").unwrap_or_else(|| "?".into())
    ))
}

/// 单腿名义相加。溢出时保留已有合计，不把看板算崩。
fn add_usdt(total: Decimal, part: Decimal) -> Decimal {
    total.checked_add(part).unwrap_or(total)
}

/// 自动交易统计看板。
///
/// 累计开仓 / 回滚 / 被拒，以及对应的单腿名义，来自 `tally`，写在 `auto.json` 里，重启不丢，
/// 旧文件从 0 起算。近 7 个 UTC 日和按合约的笔数、名义来自还留着的开仓记录（最多 [`OPENED_KEEP`] 笔）。
/// `exposed` 是当前台账里仍敞着的仓位 `(id, symbol)`，用来数「仍持有」和持有名义。
fn board_stats(
    opened: &[Opened],
    exposed: &[(String, String)],
    mode: Mode,
    tally: &Tally,
    failures: u32,
    today: chrono::NaiveDate,
) -> Value {
    #[derive(Default)]
    struct Bucket {
        opened: u64,
        open: u64,
        usdt: Decimal,
        open_usdt: Decimal,
    }
    // 补仓记录（`add`）不是新开的仓位：不计入「今日 / 按合约 / 仍持有」，和累计开仓（`tally`）同一口径。
    let mine: Vec<&Opened> = opened
        .iter()
        .filter(|row| row.mode == mode && !row.add)
        .collect();
    let open_ids: std::collections::HashSet<&str> =
        exposed.iter().map(|(id, _)| id.as_str()).collect();
    let mut by_symbol: HashMap<String, Bucket> = HashMap::new();
    for row in &mine {
        let entry = by_symbol.entry(row.symbol.clone()).or_default();
        entry.opened += 1;
        entry.usdt = add_usdt(entry.usdt, row.size_usdt);
        if open_ids.contains(row.id.as_str()) {
            entry.open += 1;
            entry.open_usdt = add_usdt(entry.open_usdt, row.size_usdt);
        }
    }
    let mut symbols: Vec<_> = by_symbol.into_iter().collect();
    symbols.sort_by(|a, b| b.1.opened.cmp(&a.1.opened).then_with(|| a.0.cmp(&b.0)));
    let symbols: Vec<Value> = symbols
        .into_iter()
        .take(8)
        .map(|(symbol, bucket)| {
            json!({
                "symbol": symbol,
                "opened": bucket.opened,
                "open": bucket.open,
                "usdt": bucket.usdt,
                "open_usdt": bucket.open_usdt,
            })
        })
        .collect();
    let days: Vec<Value> = (0..7)
        .rev()
        .map(|ago| {
            let day = today - chrono::Duration::days(ago);
            let rows = mine.iter().filter(|row| row.at.date_naive() == day);
            let (count, usdt) = rows.fold((0u64, Decimal::ZERO), |(count, usdt), row| {
                (count + 1, add_usdt(usdt, row.size_usdt))
            });
            json!({"day": day.format("%m-%d").to_string(), "opened": count, "usdt": usdt})
        })
        .collect();
    let today_rows = mine.iter().filter(|row| row.at.date_naive() == today);
    let (opened_today, opened_today_usdt) = today_rows
        .fold((0u64, Decimal::ZERO), |(count, usdt), row| {
            (count + 1, add_usdt(usdt, row.size_usdt))
        });
    let open_rows = mine.iter().filter(|row| open_ids.contains(row.id.as_str()));
    let (open_now, open_now_usdt) = open_rows.fold((0u64, Decimal::ZERO), |(count, usdt), row| {
        (count + 1, add_usdt(usdt, row.size_usdt))
    });
    json!({
        "opened": tally.opened,
        "unwound": tally.unwound,
        "rejected": tally.rejected,
        "failures": failures,
        "recorded": mine.len(),
        "opened_today": opened_today,
        "open_now": open_now,
        "opened_usdt": tally.opened_usdt,
        "unwound_usdt": tally.unwound_usdt,
        "opened_today_usdt": opened_today_usdt,
        "open_now_usdt": open_now_usdt,
        "symbols": symbols,
        "days": days,
    })
}

/// 设置的一句话描述（事件与通知用）。
pub fn describe(settings: &Settings) -> String {
    let mut parts = vec![format!(
        "{} {}，{} USDT × {}x，门槛 ≥ {}%，保持 {} 秒",
        if settings.enabled { "开启" } else { "关闭" },
        if settings.mode == Mode::Live {
            "实盘"
        } else {
            "纸面"
        },
        settings.size_usdt.normalize(),
        settings.leverage.normalize(),
        settings.min_net_pct.normalize(),
        secs_text(settings.hold_millis())
    )];
    if let Some(multiple) = settings.instant_multiple {
        parts.push(format!(
            "扣缓冲后净收益 ≥ 门槛 {} 倍时不等保持",
            multiple.normalize()
        ));
    }
    if settings.back_to_normal {
        parts.push("回到正常基差平仓".into());
    }
    if let Some(tp) = settings.take_profit_amount() {
        parts.push(format!("止盈 {} USDT", tp.normalize()));
        match settings.take_profit_book_min_usdt {
            Some(min) if min.is_zero() => parts.push("止盈触发后盘口为正即平".into()),
            Some(min) => parts.push(format!("止盈触发后盘口 ≥ {} USDT 即平", min.normalize())),
            None => {}
        }
    }
    if let Some(loss) = settings.stop_loss_usdt {
        parts.push(format!("止损 {} USDT", loss.normalize()));
    }
    if !settings.slippage_buffer_pct.is_zero() {
        parts.push(format!(
            "滑点缓冲 {}%",
            settings.slippage_buffer_pct.normalize()
        ));
    }
    if let Some(pct) = settings.max_fee_share_pct {
        parts.push(format!("手续费占毛利 ≤ {}%", pct.normalize()));
    }
    if let Some(pct) = settings.fast_move_pct {
        parts.push(format!(
            "两家任一中间价 5 秒内波动 ≥ {}% 不开（急行情）",
            pct.normalize()
        ));
    }
    if let Some(pct) = settings.force_hedge_pct {
        parts.push(format!(
            "强制补腿：第二腿保本价内没吃满不回滚，按最差锁定价差 −{}% 追上",
            pct.normalize()
        ));
    }
    if let Some(max) = settings.size_max_usdt {
        parts.push(format!(
            "按盘口动态单笔：基差够稳时最多放大到 {} USDT（取预估净赚最多的那档）",
            max.normalize()
        ));
    }
    if let Some(pct) = settings.depth_fit_pct {
        parts.push(format!(
            "按盘口深度定单笔：最多占限价内可吃深度的 {}%，最小 {} USDT，单腿名义是上限",
            pct.normalize(),
            settings.depth_min_usdt.normalize()
        ));
    }
    if settings.relative_to_normal {
        parts.push("相对正常基差（不要求开仓价差为正）".into());
    }
    if !settings.cheap_leg_first {
        parts.push("按滑点决定先下哪条腿".into());
    }
    if let Some(pct) = settings.history_gate_pct {
        parts.push(format!(
            "历史价差分位前 {}%（{} 线 {} 天）才开",
            pct.normalize(),
            settings.history_interval,
            settings.history_days.normalize()
        ));
    }
    if let Some(pct) = settings.liq_protection_pct {
        parts.push(format!("爆仓保护 {}%", pct.normalize()));
    }
    if let (Some(hours), Some(loss)) = (settings.timeout_hours, settings.timeout_max_loss_usdt) {
        parts.push(format!(
            "持仓超过 {} 小时、按盘口亏损 ≤ {} USDT 就平",
            hours.normalize(),
            loss.normalize()
        ));
    }
    if let Some(improve) = settings.add_min_improve_pct {
        parts.push(format!(
            "补仓：同方向新信号价差好 ≥ {}% 就补，每笔最多 {} 次、总名义 ≤ 单笔 {} 倍",
            improve.normalize(),
            settings.add_max_times,
            settings.add_max_multiple.normalize()
        ));
    }
    if let (Some(pct), Some(max)) = (settings.auto_margin_pct, settings.auto_margin_max_usdt) {
        parts.push(format!(
            "强平距离 < {}% 自动加保证金（每笔累计 ≤ {} USDT）",
            pct.normalize(),
            max.normalize()
        ));
    }
    let cap = |n: usize| {
        if n == 0 {
            "不限".to_string()
        } else {
            format!("{n} 笔")
        }
    };
    parts.push(format!(
        "同时最多 {}、每日最多 {}",
        cap(settings.max_positions),
        cap(settings.daily_max_opens)
    ));
    if !settings.symbols.is_empty() {
        parts.push(format!("只做 {}", settings.symbols.join(",")));
    }
    if !settings.exclude_venues.is_empty() {
        let venues: Vec<&str> = settings.exclude_venues.iter().map(|v| v.as_str()).collect();
        parts.push(format!("不做 {} 的组合", venues.join(",")));
    }
    if !settings.exclude_symbols.is_empty() {
        parts.push(format!("不做 {}", settings.exclude_symbols.join(",")));
    }
    parts.join("；")
}

// ───────────────────────────── 接口 ─────────────────────────────

/// `GET /api/rh-spread/auto`：设置、状态、最近事件。要令牌（含仓位 id 与下单结果）。
pub async fn api_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(denied) = state.trade.authorize(&headers) {
        return denied.into_response();
    }
    let settings = state.rh_auto.settings().await;
    let exposed = state
        .trade
        .exposed_positions(settings.mode == Mode::Live)
        .await
        .ok()
        .map(|list| {
            list.into_iter()
                .map(|(id, symbol)| (id, symbol.to_string()))
                .collect::<Vec<_>>()
        });
    let mut body = state.rh_auto.snapshot(exposed.as_deref()).await;
    body["max_position_usdt"] = json!(state.settings.max_position_usdt);
    body["live_can_trade"] = json!(state.trade.live_can_trade());
    body["paper_watch_sec"] = json!(state.trade.paper_watch_sec());
    body["monitor_size_usdt"] = json!(state.rh_spread.view().await.size_usdt);
    Json(body).into_response()
}

#[derive(Debug, Deserialize)]
pub struct UpdateBody {
    #[serde(flatten)]
    settings: Settings,
    /// 开启实盘自动交易时必须是 `LIVE`。
    confirm: Option<String>,
}

/// 请求里带了的字段覆盖、没带的沿用 `current`，再按设置的规则解析。
///
/// 面板只发它有输入框的字段：以前整套替换，面板没有的设置（往返净边际、薄盘口门槛、降仓位名单……）每保存一次
/// 就悄悄回到默认值，手改过的值也被覆盖（2026-10-31 审查发现）。显式发 `null` 的照样清空。
fn merge_onto(current: &Settings, body: Value) -> Result<UpdateBody, String> {
    let Value::Object(fields) = body else {
        return Err("设置必须是一个 JSON 对象".into());
    };
    let mut merged = serde_json::to_value(current).map_err(|error| error.to_string())?;
    let Some(target) = merged.as_object_mut() else {
        return Err("当前设置序列化不成对象".into());
    };
    for (key, value) in fields {
        target.insert(key, value);
    }
    serde_json::from_value(merged).map_err(|error| format!("设置不合法：{error}"))
}

/// `POST /api/rh-spread/auto`：保存设置（带了的字段覆盖，没带的沿用当前值，见 [`merge_onto`]）。
pub async fn api_set(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Err(denied) = state.trade.authorize(&headers) {
        return denied.into_response();
    }
    let body = match merge_onto(&state.rh_auto.settings().await, body) {
        Ok(body) => body,
        Err(message) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response();
        }
    };
    let mut settings = body.settings;
    settings.symbols = settings
        .symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    settings.binance_symbols = settings
        .binance_symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    settings.exclude_venues.sort();
    settings.exclude_venues.dedup();
    settings.exclude_symbols = settings
        .exclude_symbols
        .iter()
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    settings.exclude_symbols.sort();
    settings.exclude_symbols.dedup();
    if settings.enabled && settings.mode == Mode::Live && !state.trade.live_can_trade() {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": "实盘没连上或是只读模式，不能开启实盘自动交易" })),
        )
            .into_response();
    }
    let confirm = body.confirm.as_deref().is_some_and(|c| c.trim() == "LIVE");
    let max = state
        .settings
        .max_position_usdt
        .and_then(Decimal::from_f64_retain);
    match state.rh_auto.update(settings, max, confirm).await {
        Ok(saved) => {
            state.rh_spread.set_size_usdt(saved.size_usdt);
            warn!("价差自动交易设置已更新：{}", describe(&saved));
            state
                .alerts
                .notify_always(format!("🤖 价差自动交易设置已更新：{}", describe(&saved)));
            Json(json!({ "settings": saved })).into_response()
        }
        Err(message) => {
            (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
        }
    }
}

#[path = "rh_auto/hold_shadow.rs"]
mod hold_shadow;
mod venue_latency;

#[cfg(test)]
#[path = "rh_auto/paper_tests.rs"]
mod paper_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rh_spread::{Best, Connected, DirectionQuote, Leg, Normal, Session};

    fn dec(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    pub(super) fn leg(entry: &str, zero: &str, normal: &str) -> Leg {
        Leg {
            quote: DirectionQuote {
                entry_pct: dec(entry),
                exit_cross_pct: dec("0.02"),
            },
            net_to_zero_pct: dec(zero),
            net_to_normal_pct: Some(dec(normal)),
            funding_cost_pct: None,
            ladder: Vec::new(),
        }
    }

    pub(super) fn line(base: &str, direction: &'static str, leg: Leg, median: f64) -> Line {
        let (long_a, long_b) = if direction == "long_a" {
            (Some(leg), None)
        } else {
            (None, Some(leg))
        };
        Line {
            pair: crate::rh_spread::pairs::Pair::RH.id(),
            a: Venue::Arcus,
            b: Venue::LighterRh,
            fee_round_trip_pct: Some(dec("0.045")),
            base: base.into(),
            category: "EQUITIES".into(),
            session: Session::Rth,
            basis_pct: Some(dec("-0.3")),
            normal: Some(Normal {
                session: Session::Rth,
                median,
                p10: median - 0.03,
                p90: median + 0.03,
                mad: 0.01,
                minutes: 900,
            }),
            normal_missing_minutes: 0,
            z: Some(-10.0),
            long_a,
            long_b,
            best: Some(Best {
                direction,
                signal: false,
                signal_sec: 0,
                net_usdt: None,
            }),
            age_sec: Some(0),
            note: None,
            observe_only: false,
            watch_only: false,
            maker: None,
            recent_move: None,
            recent_entry: None,
        }
    }

    pub(super) fn view(lines: Vec<Line>) -> View {
        View {
            enabled: true,
            size_usdt: Decimal::from(2000),
            alert_net_pct: dec("0.05"),
            min_minutes: 120,
            window_days: 7,
            pairs: Vec::new(),
            connected: Connected {
                venues: [(Venue::Arcus, true), (Venue::LighterRh, true)].into(),
                reconnects: 0,
            },
            updated_at: Some(Utc::now()),
            lines: lines.into_iter().map(Arc::new).collect(),
            error: None,
            maker: None,
        }
    }

    pub(super) fn settings() -> Settings {
        // 往返净边际 / 薄盘口 / 降仓位三项（2026-10-31 新增）在测试里关掉，各自有专门的测试；
        // 这里的夹具价差大多很薄，开着会让其他测试的主角被它们挡掉。
        Settings {
            enabled: true,
            min_net_pct: dec("0.05"),
            min_edge_pct: None,
            thin_min_entry_pct: None,
            cut_symbols: Vec::new(),
            ..Settings::default()
        }
    }

    #[test]
    fn stats_board_counts_today_open_positions_and_keeps_lifetime_tallies() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 10, 18).unwrap();
        let at = |day: u32, hour: u32| {
            chrono::NaiveDate::from_ymd_opt(2026, 10, day)
                .unwrap()
                .and_hms_opt(hour, 0, 0)
                .unwrap()
                .and_utc()
        };
        let row = |id: &str, symbol: &str, day: u32, mode: Mode, size: i64| Opened {
            id: id.into(),
            symbol: symbol.into(),
            mode,
            at: at(day, 1),
            add: false,
            binance: false,
            size_usdt: Decimal::from(size),
        };
        let opened = vec![
            row("a", "NVDA/USDT", 18, Mode::Paper, 500),
            row("b", "NVDA/USDT", 18, Mode::Paper, 800),
            row("c", "SPY/USDT", 17, Mode::Paper, 200),
            row("live", "NVDA/USDT", 18, Mode::Live, 9_999),
            // 往已有仓位 a 上补仓：不是新开的仓位，不进今日 / 按合约 / 仍持有，和累计开仓同一口径。
            Opened {
                add: true,
                ..row("a", "NVDA/USDT", 18, Mode::Paper, 300)
            },
        ];
        let stats = board_stats(
            &opened,
            &[("a".into(), "NVDA/USDT".into())],
            Mode::Paper,
            &Tally {
                opened: 9,
                unwound: 2,
                rejected: 4,
                opened_usdt: Decimal::from(4500),
                unwound_usdt: Decimal::from(1000),
            },
            1,
            today,
        );
        let amount = |value: &Value| value.as_str().unwrap().parse::<Decimal>().unwrap();
        assert_eq!(stats["opened"], json!(9));
        assert_eq!(stats["unwound"], json!(2));
        assert_eq!(stats["rejected"], json!(4));
        assert_eq!(stats["failures"], json!(1));
        assert_eq!(stats["recorded"], json!(3));
        assert_eq!(stats["opened_today"], json!(2));
        assert_eq!(stats["open_now"], json!(1));
        // 累计名义来自 tally，不从还留着的记录重算（记录会被裁到 300 笔）。
        assert_eq!(amount(&stats["opened_usdt"]), Decimal::from(4500));
        assert_eq!(amount(&stats["unwound_usdt"]), Decimal::from(1000));
        assert_eq!(amount(&stats["opened_today_usdt"]), Decimal::from(1300));
        assert_eq!(amount(&stats["open_now_usdt"]), Decimal::from(500));
        assert_eq!(stats["symbols"][0]["symbol"], json!("NVDA/USDT"));
        assert_eq!(stats["symbols"][0]["opened"], json!(2));
        assert_eq!(stats["symbols"][0]["open"], json!(1));
        assert_eq!(amount(&stats["symbols"][0]["usdt"]), Decimal::from(1300));
        assert_eq!(
            amount(&stats["symbols"][0]["open_usdt"]),
            Decimal::from(500)
        );
        assert_eq!(stats["symbols"][1]["symbol"], json!("SPY/USDT"));
        assert_eq!(amount(&stats["symbols"][1]["usdt"]), Decimal::from(200));
        assert_eq!(stats["days"].as_array().unwrap().len(), 7);
        assert_eq!(stats["days"][6]["day"], json!("10-18"));
        assert_eq!(stats["days"][6]["opened"], json!(2));
        assert_eq!(amount(&stats["days"][6]["usdt"]), Decimal::from(1300));
        assert_eq!(stats["days"][5]["opened"], json!(1));
        assert_eq!(amount(&stats["days"][5]["usdt"]), Decimal::from(200));
        // 同一天的实盘 NVDA（9,999）不进纸面计数，也不进纸面金额。
        let legacy: Opened = serde_json::from_str(
            r#"{"id":"old","symbol":"NVDA/USDT","mode":"paper","at":"2026-10-18T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(legacy.size_usdt, Decimal::ZERO);
        let legacy_tally: Tally =
            serde_json::from_str(r#"{"opened":1,"unwound":0,"rejected":0}"#).unwrap();
        assert_eq!(legacy_tally.opened_usdt, Decimal::ZERO);
        assert_eq!(legacy_tally.unwound_usdt, Decimal::ZERO);
    }

    /// 预判式准备只给「快到门槛」里净收益最高的一个做，且尊重冷却、场所与行情在线状态。
    #[test]
    fn near_arming_picks_the_single_best_nearly_qualifying_line() {
        let s = settings(); // 门槛 0.05%
        let floor = s.min_net_pct * ARM_NEAR_RATIO;
        let near = Settings {
            min_net_pct: floor,
            ..s.clone()
        };
        // NVDA 已达标（0.09）、AMD 快到（0.06）、SPY 刚过一半（0.03）、QQQ 还差得远（0.01）。
        let v = view(vec![
            line("QQQ", "long_a", leg("0.3", "0.2", "0.01"), -0.11),
            line("SPY", "long_a", leg("0.3", "0.2", "0.03"), -0.11),
            line("AMD", "long_a", leg("0.3", "0.2", "0.06"), -0.11),
            line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11),
        ]);
        let live = [Venue::Arcus, Venue::LighterRh];
        let now = Instant::now();
        let none = HashMap::new();
        let pick =
            |live: &[Venue], cool: &HashMap<String, Instant>, c: &crate::rh_spread::Connected| {
                pick_near(&v.lines, &near, floor, live, c, cool, now).map(|c| c.base)
            };
        // 净收益最高的先准备；差得远的（QQQ）从不准备。
        assert_eq!(pick(&live, &none, &v.connected).as_deref(), Some("NVDA"));
        assert!(is_nearly(&v.lines[1], floor) && !is_nearly(&v.lines[0], floor));
        // 冷却中的跳过，轮到下一个。
        let cool = HashMap::from([("NVDA".to_string(), now + Duration::from_secs(60))]);
        assert_eq!(pick(&live, &cool, &v.connected).as_deref(), Some("AMD"));
        // 有一条腿不是已连接的实盘场所：不准备。
        assert_eq!(pick(&[Venue::Arcus], &none, &v.connected), None);
        // 行情断了：不准备。
        let mut down = v.connected.clone();
        down.venues.insert(Venue::LighterRh, false);
        assert_eq!(pick(&live, &none, &down), None);
    }

    /// 回测后的入场门槛：往返净边际 ≥0.10%（所有组合）、薄盘口（Bitget / HL-io）可成交价差 ≥0.30%。
    /// 夹具的往返手续费是 0.045%，所以可成交价差 0.14% 恰好剩 0.095% 被挡，0.15% 剩 0.105% 放行。
    #[test]
    fn the_round_trip_edge_and_thin_venue_gates_block_thin_entries() {
        let gated = Settings {
            min_edge_pct: Some(dec("0.10")),
            thin_min_entry_pct: Some(dec("0.30")),
            ..settings()
        };
        let thin = line("HOOD", "long_a", leg("0.14", "0.10", "0.30"), -0.5);
        let why = auto_skip_reason(&thin, thin.long_a.as_ref().unwrap(), &gated).unwrap();
        assert!(
            why.contains("往返净边际门槛 0.1%") && why.contains("0.095"),
            "{why}"
        );
        assert!(candidate_of(&thin, &gated).is_none());
        let fine = line("HOOD", "long_a", leg("0.15", "0.10", "0.30"), -0.5);
        assert!(candidate_of(&fine, &gated).is_some());
        // 关掉就不管。
        let off = Settings {
            min_edge_pct: None,
            ..gated.clone()
        };
        assert!(candidate_of(&thin, &off).is_some());
        // 往返手续费拿不到：放行（不知道就不挡）。
        let mut unknown_thin = line("HOOD", "long_a", leg("0.14", "0.10", "0.30"), -0.5);
        unknown_thin.fee_round_trip_pct = None;
        assert!(candidate_of(&unknown_thin, &gated).is_some());

        // 薄盘口：两条腿里有 Bitget 或 HL-io，可成交价差要 ≥0.30%。用 skip_reason 直接看这条规则
        // （auto_skip_reason 还会先看 Bitget 股票类周末休市，周末跑测试会被它抢先）。
        let skip = |line: &Line, settings: &Settings| {
            skip_reason(line, line.long_a.as_ref().unwrap(), settings)
        };
        let mut bitget = line("AMD", "long_a", leg("0.25", "0.20", "0.30"), -0.5);
        bitget.b = Venue::Bitget;
        let why = skip(&bitget, &gated).unwrap();
        assert!(
            why.contains("bitget") && why.contains("薄盘口门槛 0.3%"),
            "{why}"
        );
        let mut strong = line("AMD", "long_a", leg("0.30", "0.25", "0.35"), -0.5);
        strong.b = Venue::HyperliquidIo;
        assert!(skip(&strong, &gated).is_none());
        let mut strong_weak = strong.clone();
        strong_weak.long_a = Some(leg("0.29", "0.24", "0.35"));
        assert!(skip(&strong_weak, &gated).is_some());
        // 不涉及薄盘口的同样价差照样放行。
        let plain = line("AMD", "long_a", leg("0.25", "0.20", "0.30"), -0.5);
        assert!(skip(&plain, &gated).is_none());
        // 「相对正常基差」的单子入场价差可以为负，这两条不适用。
        let relative = Settings {
            relative_to_normal: true,
            stop_loss_usdt: Some(dec("5")),
            ..gated
        };
        assert!(skip(&bitget, &relative).is_none());
    }

    /// 降仓位的合约（某合约 / 某合约 / 某合约 / 某合约）：单笔名义乘 0.5，别的合约不动；
    /// 按盘口放大后的单笔也一样减半。
    #[test]
    fn the_cut_symbols_trade_at_half_size_and_the_rest_do_not() {
        let s = Settings {
            cut_symbols: default_cut_symbols(),
            cut_factor: default_cut_factor(),
            size_usdt: dec("200"),
            ..settings()
        };
        let candidate = |base: &str| {
            let l = line(base, "long_a", leg("0.30", "0.25", "0.30"), -0.5);
            candidate_of(&l, &s).unwrap()
        };
        // 某合约、某合约 也在默认名单里，但它们是上市前合约，根本不做（见 `crate::preipo`）。
        assert_eq!(candidate("SOFTBANK").size(&s), dec("100"));
        assert_eq!(candidate("BOT").size(&s), dec("100"));
        assert_eq!(candidate("HOOD").size(&s), dec("200"));
        let mut up = candidate("SOFTBANK");
        up.upsized = Some(Upsize {
            size_usdt: dec("400"),
            note: String::new(),
        });
        assert_eq!(up.size(&s), dec("200"));
        // 名单清空 = 不降。
        let none = Settings {
            cut_symbols: Vec::new(),
            ..s.clone()
        };
        assert_eq!(candidate("SOFTBANK").size(&none), dec("200"));
    }

    /// 面板只发它有输入框的字段：没带的沿用当前值（不回到默认），显式 `null` 照样清空，带了的覆盖。
    #[test]
    fn a_partial_save_keeps_the_settings_the_panel_does_not_send() {
        let current = Settings {
            min_edge_pct: Some(dec("0.2")),
            cut_symbols: vec!["OPENAI".into()],
            stop_loss_usdt: Some(dec("5")),
            ..settings()
        };
        let body = merge_onto(
            &current,
            serde_json::json!({"size_usdt": "300", "stop_loss_usdt": null, "confirm": "LIVE"}),
        )
        .unwrap();
        assert_eq!(
            body.settings.min_edge_pct,
            Some(dec("0.2")),
            "面板没发：沿用"
        );
        assert_eq!(body.settings.cut_symbols, ["OPENAI"]);
        assert_eq!(body.settings.size_usdt, dec("300"), "发了：覆盖");
        assert_eq!(body.settings.stop_loss_usdt, None, "显式 null：清空");
        assert_eq!(body.confirm.as_deref(), Some("LIVE"));
        assert!(merge_onto(&current, serde_json::json!([1])).is_err());
        assert!(merge_onto(&current, serde_json::json!({"size_usdt": "abc"})).is_err());
    }

    /// 老的 auto.json 没有这几项：读进来就带着默认值（门槛开着、名单是回测挑出来的那四个）；设置不合法要被拒。
    #[test]
    fn old_settings_files_get_the_new_entry_gates_by_default_and_bad_values_are_refused() {
        let mut value = serde_json::to_value(Settings::default()).unwrap();
        for key in [
            "min_edge_pct",
            "thin_venues",
            "thin_min_entry_pct",
            "cut_symbols",
            "cut_factor",
        ] {
            value.as_object_mut().unwrap().remove(key);
        }
        let old: Settings = serde_json::from_value(value).unwrap();
        assert_eq!(old.min_edge_pct, Some(dec("0.10")));
        assert_eq!(old.thin_venues, vec![Venue::Bitget, Venue::HyperliquidIo]);
        assert_eq!(old.thin_min_entry_pct, Some(dec("0.30")));
        assert_eq!(old.cut_symbols, ["OPENAI", "SOFTBANK", "BOT", "OURA"]);
        assert_eq!(old.cut_factor, dec("0.5"));
        let bad = |change: fn(&mut Settings)| {
            let mut s = old.clone();
            change(&mut s);
            s.validate(None).is_err()
        };
        assert!(bad(|s| s.min_edge_pct = Some(dec("3"))));
        assert!(bad(|s| s.thin_min_entry_pct = Some(dec("-0.1"))));
        assert!(bad(|s| s.cut_factor = Decimal::ZERO));
        assert!(bad(|s| s.cut_factor = dec("1.5")));
        assert!(bad(|s| s.cut_symbols = vec!["a b".into()]));
    }

    /// Telegram 提醒里那句「自动交易会不会下这一笔」：没开、规则不过、已有仓位、历史分位、刚试过、
    /// 都过了，各说各的原因；没开时规则也不过的，两样都说。
    #[test]
    fn alert_note_says_why_the_auto_trader_skips_this_signal() {
        let auto = AutoTrader::in_memory(Settings {
            enabled: false,
            ..settings()
        });
        let good = line("HOOD", "long_a", leg("0.30", "0.25", "0.30"), -0.5);
        let best = good.best.clone().unwrap();
        // 主循环还没跑过：不知道，用笼统的说法。
        assert_eq!(auto.alert_note(&good, &best), None);

        let set = |f: &dyn Fn(&mut Brief)| f(&mut auto.brief());
        set(&|b| {
            b.settings = Some(settings());
            b.halted = Some("未开启".into());
        });
        assert_eq!(
            auto.alert_note(&good, &best).unwrap(),
            "自动交易：未开启，不会自动下单。"
        );
        // 负入场价差（某合约 那种）：没开 + 就算开着也不下。
        let negative = line("NFLX", "long_b", leg("-0.42", "-0.43", "1.7"), -2.15);
        let note = auto
            .alert_note(&negative, negative.best.as_ref().unwrap())
            .unwrap();
        assert!(
            note.contains("未开启") && note.contains("就算开着") && note.contains("-0.42"),
            "{note}"
        );

        set(&|b| b.halted = None);
        let note = auto
            .alert_note(&negative, negative.best.as_ref().unwrap())
            .unwrap();
        assert!(
            note.starts_with("自动交易不下这一笔：开仓可成交价差 -0.42"),
            "{note}"
        );

        set(&|b| {
            b.exposed = ["HOOD".to_string()].into();
        });
        assert!(
            auto.alert_note(&good, &best)
                .unwrap()
                .contains("已有 HOOD 的仓位")
        );

        set(&|b| {
            b.exposed.clear();
            b.settings = Some(Settings {
                history_gate_pct: Some(dec("20")),
                ..settings()
            });
        });
        assert!(
            auto.alert_note(&good, &best)
                .unwrap()
                .contains("历史价差分位还在算")
        );
        set(&|b| {
            b.history.insert(
                ("HOOD".into(), Venue::Arcus, Venue::LighterRh),
                Some("历史价差分位 37%，不在前 20%".into()),
            );
        });
        assert!(
            auto.alert_note(&good, &best)
                .unwrap()
                .contains("历史分位门槛没过：历史价差分位 37%")
        );
        set(&|b| {
            b.history
                .insert(("HOOD".into(), Venue::Arcus, Venue::LighterRh), None);
        });
        assert!(
            auto.alert_note(&good, &best)
                .unwrap()
                .starts_with("满足自动开仓规则")
        );

        // 扣过资金费的结论优先于提醒这一行自己（没扣资金费）的。
        set(&|b| {
            b.rules.insert(
                (good.pair.clone(), "HOOD".into(), "long_a"),
                Some("回到正常净收益 0.04% 不到门槛".into()),
            );
        });
        assert!(
            auto.alert_note(&good, &best)
                .unwrap()
                .contains("0.04% 不到门槛")
        );

        // 刚自动下过（开成了 / 被拒）：说结果，排在规则前面。
        set(&|b| {
            b.outcome.insert(
                "HOOD".into(),
                (Instant::now(), "下单前检查没通过：对账不干净".into()),
            );
        });
        let note = auto.alert_note(&good, &best).unwrap();
        assert!(
            note.contains("秒前刚处理过 HOOD：下单前检查没通过：对账不干净"),
            "{note}"
        );
    }

    fn rung(size: &str, entry: &str, zero: &str, normal: &str) -> crate::rh_spread::Rung {
        crate::rh_spread::Rung {
            size_usdt: dec(size),
            entry_pct: dec(entry),
            net_to_zero_pct: dec(zero),
            net_to_normal_pct: Some(dec(normal)),
        }
    }

    /// 单笔 200、上限 600、滑点缓冲 0.05%：按 USDT = 单笔 ×（净收益 − 缓冲）算，
    /// 200→0.5、300→0.63、400→0.68、500→0.6、600→0.6、700 超上限。
    fn upsizing() -> (Settings, Line) {
        let s = Settings {
            size_usdt: dec("200"),
            size_max_usdt: Some(dec("600")),
            slippage_buffer_pct: dec("0.05"),
            take_profit_usdt: Some(dec("0.25")),
            take_profit_pct: Some(dec("0.1")),
            ..settings()
        };
        let mut l = line("HOOD", "long_a", leg("0.40", "0.35", "0.30"), -0.05);
        l.long_a.as_mut().unwrap().ladder = vec![
            rung("200", "0.40", "0.35", "0.30"),
            rung("300", "0.36", "0.31", "0.26"),
            rung("400", "0.32", "0.27", "0.22"),
            rung("500", "0.27", "0.22", "0.17"),
            rung("600", "0.25", "0.20", "0.15"),
            rung("700", "0.24", "0.19", "0.14"),
        ];
        (s, l)
    }

    #[test]
    fn upsize_takes_the_size_that_earns_the_most_usdt_within_the_cap() {
        let (s, l) = upsizing();
        let leg = l.long_a.as_ref().unwrap();
        let up = upsize(&l, leg, &s).unwrap();
        // 不是百分比最高的 200，也不是最大的 600。
        assert_eq!(up.size_usdt, dec("400"));
        assert_eq!(up.note, "按盘口放大 200→400 USDT：预估净赚 0.5→0.68 USDT");
        // 上限 300：只能到 300。
        let capped = Settings {
            size_max_usdt: Some(dec("300")),
            ..s.clone()
        };
        assert_eq!(upsize(&l, leg, &capped).unwrap().size_usdt, dec("300"));
        // 没开 / 上限等于单笔：不放大，和以前一样按 200。
        let off = Settings {
            size_max_usdt: None,
            ..s.clone()
        };
        assert_eq!(upsize(&l, leg, &off), None);
        let c = candidate_of(&l, &off).unwrap();
        assert_eq!(c.size(&off), dec("200"));

        // 候选带上放大后的单笔；按比例的止盈跟着走：max(0.25, 0.1% × 400) = 0.4。
        let c = candidate_of(&l, &s).unwrap();
        assert_eq!(c.size(&s), dec("400"));
        assert_eq!(s.rules_for(None, &c).take_profit_usdt, Some(dec("0.4")));
        // 下单请求按放大后的单笔发（之后下单路径照旧按盘口深度往下缩）。
        let body = format!("{:?}", AutoTrader::open_body(&s, &c));
        assert!(body.contains("size: \"400\""), "{body}");
    }

    #[test]
    fn upsize_stops_at_the_first_rung_that_fails_the_gates_and_skips_unsteady_pairs() {
        let (s, mut l) = upsizing();
        // 300 那档扣缓冲后 0.04% 不到门槛 0.05%：后面更大的档也不看。
        l.long_a.as_mut().unwrap().ladder[1] = rung("300", "0.20", "0.15", "0.09");
        assert_eq!(upsize(&l, l.long_a.as_ref().unwrap(), &s), None);
        // 300 那档入场价差不为正（硬条件）：同样停。
        let (_, mut l) = upsizing();
        l.long_a.as_mut().unwrap().ladder[1] = rung("300", "0", "-0.05", "0.26");
        assert_eq!(upsize(&l, l.long_a.as_ref().unwrap(), &s), None);
        // 只到 300 赚得最多就选 300（400 那档虽过门槛但更少）。
        let (_, mut l) = upsizing();
        l.long_a.as_mut().unwrap().ladder[2] = rung("400", "0.25", "0.20", "0.15");
        assert_eq!(
            upsize(&l, l.long_a.as_ref().unwrap(), &s)
                .unwrap()
                .size_usdt,
            dec("300")
        );

        // 基差日常波动（MAD 0.5%）比预估净收益 0.30% 还大（某合约 那种）：不放大。
        let (_, mut l) = upsizing();
        l.normal.as_mut().unwrap().mad = 0.5;
        assert_eq!(upsize(&l, l.long_a.as_ref().unwrap(), &s), None);
        assert_eq!(candidate_of(&l, &s).unwrap().size(&s), dec("200"));

        // 币安的组合不放大。
        let (_, mut l) = upsizing();
        l.observe_only = true;
        assert_eq!(upsize(&l, l.long_a.as_ref().unwrap(), &s), None);
    }

    #[test]
    fn funding_is_deducted_from_every_rung_and_adds_keep_the_base_size() {
        let (s, l) = upsizing();
        let mut v = view(vec![l]);
        // 多 Arcus 每小时付 0.001%、空 RH 不付：24 小时 0.024%。
        deduct_funding(
            &mut v,
            &|venue, _| {
                Some(if venue == Venue::Arcus {
                    dec("0.00001")
                } else {
                    Decimal::ZERO
                })
            },
            Decimal::from(24),
        );
        let leg = v.lines[0].long_a.as_ref().unwrap();
        assert_eq!(leg.funding_cost_pct, Some(dec("0.024")));
        assert_eq!(leg.net_to_normal_pct, Some(dec("0.276")));
        let nets: Vec<Decimal> = leg
            .ladder
            .iter()
            .map(|r| r.net_to_normal_pct.unwrap())
            .collect();
        assert_eq!(
            nets,
            ["0.276", "0.236", "0.196", "0.146", "0.126", "0.116"].map(dec)
        );

        // 补仓按原来的单腿名义，不放大。
        let adds = Settings {
            add_min_improve_pct: Some(dec("0.05")),
            add_max_times: 2,
            add_max_multiple: Decimal::from(3),
            ..s.clone()
        };
        let all = |_: Venue| true;
        let held = |_: &Candidate| 10;
        let free = |_: &str| None;
        let (candidate, _) = pick_add(
            &v,
            &adds,
            Utc::now(),
            &all,
            &held,
            &free,
            &[target("HOOD", "0.10", "200", 0)],
            None,
        )
        .unwrap();
        assert_eq!(candidate.upsized, None);
        assert_eq!(candidate.size(&adds), dec("200"));
    }

    #[test]
    fn an_upsized_order_short_of_margin_falls_back_to_the_base_size() {
        let (s, l) = upsizing();
        let c = candidate_of(&l, &s).unwrap();
        // 下单路径的原话（desk::check_legs_collateral）。
        let short = "hyperliquid-xyz 账户可用保证金 100 USDT，这笔要占约 146.67 USDT（名义 400 ÷ 3x，含 10% 余量），不够。";
        assert!(upsized_margin_short(&c, short));
        assert!(!upsized_margin_short(&c, "对账不干净"));
        // 没放大过的单子保证金不够：照旧冷却，不是这条路。
        let base = Candidate { upsized: None, ..c };
        assert!(!upsized_margin_short(&base, short));
    }

    #[test]
    fn the_dynamic_size_cap_is_validated_and_old_settings_leave_it_off() {
        let ok = Settings {
            size_usdt: dec("200"),
            size_max_usdt: Some(dec("600")),
            take_profit_usdt: Some(dec("0.25")),
            ..settings()
        };
        assert!(ok.validate(None).is_ok());
        for (max, limit) in [("100", None), ("1100", None), ("600", Some(dec("500")))] {
            let bad = Settings {
                size_max_usdt: Some(dec(max)),
                ..ok.clone()
            };
            assert!(bad.validate(limit).is_err(), "{max}");
        }
        assert!(describe(&ok).contains("最多放大到 600 USDT"));
        let mut old = serde_json::to_value(&ok).unwrap();
        old.as_object_mut().unwrap().remove("size_max_usdt");
        let back: Settings = serde_json::from_value(old).unwrap();
        assert_eq!(back.size_max_usdt, None);
    }

    fn target(base: &str, entry_spread: &str, notional: &str, adds: u32) -> AddTarget {
        AddTarget {
            id: format!("live-{base}"),
            base: base.into(),
            long: Venue::Arcus,
            short: Venue::LighterRh,
            entry_spread_pct: dec(entry_spread),
            notional_usdt: dec(notional),
            adds,
        }
    }

    /// 用户选的激进参数：价差好 ≥ 0.05% 就补，每笔最多 2 次，总名义 ≤ 单笔 3 倍。
    fn adding() -> Settings {
        Settings {
            size_usdt: Decimal::from(800),
            hold_sec: 5,
            hold_ms: None,
            open_gap_sec: 60,
            add_min_improve_pct: Some(dec("0.05")),
            add_max_times: 2,
            add_max_multiple: Decimal::from(3),
            ..settings()
        }
    }

    #[test]
    fn an_interruption_resumes_once_settled_and_gives_up_after_the_limit() {
        let waiting = || Err::<(), String>("台账里还有 1 张单结局没定".into());
        assert_eq!(
            interrupt_decision(Ok(()), Duration::from_secs(90)),
            InterruptDecision::Resume
        );
        assert_eq!(
            interrupt_decision(waiting(), Duration::from_secs(90)),
            InterruptDecision::Wait("台账里还有 1 张单结局没定".into())
        );
        assert!(matches!(
            interrupt_decision(waiting(), INTERRUPT_RESUME_LIMIT),
            InterruptDecision::Disable(_)
        ));
        // 到了时限但这一刻正好核实一致：恢复，不关。
        assert_eq!(
            interrupt_decision(Ok(()), INTERRUPT_RESUME_LIMIT * 2),
            InterruptDecision::Resume
        );
    }

    #[test]
    fn the_open_gap_is_a_setting_that_old_files_read_as_sixty_seconds() {
        let s = settings();
        let mut old = serde_json::to_value(&s).unwrap();
        old.as_object_mut().unwrap().remove("open_gap_sec");
        let back: Settings = serde_json::from_value(old).unwrap();
        assert_eq!(back.open_gap_sec, 60);
        assert!(
            Settings {
                open_gap_sec: 15,
                ..s.clone()
            }
            .validate(None)
            .is_ok()
        );
        for bad in [0, 4, 601] {
            let err = Settings {
                open_gap_sec: bad,
                ..s.clone()
            }
            .validate(None)
            .unwrap_err();
            assert!(err.contains("间隔"), "{bad}: {err}");
        }
    }

    #[test]
    fn no_add_with_a_binance_leg_around_a_funding_settlement() {
        use chrono::TimeZone;
        let mut l = line("HOOD", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        l.a = Venue::Binance;
        let mut v = view(vec![l]);
        v.connected.venues.insert(Venue::Binance, true);
        let all = |_: Venue| true;
        let held = |_: &Candidate| 10;
        let free = |_: &str| None;
        let s = adding();
        let mut t = target("HOOD", "0.158", "800", 0);
        t.long = Venue::Binance;
        let targets = [t];
        // 结算点（UTC 整 4 小时）前后 60 秒：不补。
        let settling = Utc.with_ymd_and_hms(2026, 11, 7, 0, 0, 20).unwrap();
        v.updated_at = Some(settling);
        let err = pick_add(&v, &s, settling, &all, &held, &free, &targets, None).unwrap_err();
        assert!(err.contains("结算点"), "{err}");
        // 平时照补。
        let calm = Utc.with_ymd_and_hms(2026, 11, 7, 1, 30, 0).unwrap();
        v.updated_at = Some(calm);
        let (candidate, _) = pick_add(&v, &s, calm, &all, &held, &free, &targets, None).unwrap();
        assert_eq!(candidate.long, Venue::Binance);
    }

    #[test]
    fn a_better_same_direction_signal_adds_to_the_existing_position() {
        // 新信号：多 Arcus / 空 RH，可成交价差 0.30%。
        let v = view(vec![line(
            "HOOD",
            "long_a",
            leg("0.30", "0.20", "0.09"),
            -0.11,
        )]);
        let now = Utc::now();
        let all = |_: Venue| true;
        let held = |_: &Candidate| 10;
        let free = |_: &str| None;
        let s = adding();
        // 原仓位实际成交价差 0.158%：好 0.142% ≥ 0.05%，补。
        let targets = [target("HOOD", "0.158", "800", 0)];
        let (candidate, chosen) =
            pick_add(&v, &s, now, &all, &held, &free, &targets, None).unwrap();
        assert_eq!(candidate.base, "HOOD");
        assert_eq!(chosen.id, "live-HOOD");

        // 只好 0.03%：不到门槛，说清楚差多少。
        let err = pick_add(
            &v,
            &s,
            now,
            &all,
            &held,
            &free,
            &[target("HOOD", "0.27", "800", 0)],
            None,
        )
        .unwrap_err();
        assert!(err.contains("不到补仓门槛 0.05%"), "{err}");
        // 已补 2 次：到上限。
        let err = pick_add(
            &v,
            &s,
            now,
            &all,
            &held,
            &free,
            &[target("HOOD", "0.158", "1600", 2)],
            None,
        )
        .unwrap_err();
        assert!(err.contains("已补 2 次"), "{err}");
        // 补完 1600 + 800 = 2400 = 3 倍，正好可以；再大就超。
        assert!(
            pick_add(
                &v,
                &s,
                now,
                &all,
                &held,
                &free,
                &[target("HOOD", "0.158", "1600", 1)],
                None,
            )
            .is_ok()
        );
        let err = pick_add(
            &v,
            &s,
            now,
            &all,
            &held,
            &free,
            &[target("HOOD", "0.158", "1700", 1)],
            None,
        )
        .unwrap_err();
        assert!(err.contains("超过单笔的 3 倍"), "{err}");
        // 单笔上限 ARB_MAX_POSITION_USDT=1500 比 3 倍还小：在选候选时就挡住，不发请求。
        let err = pick_add(
            &v,
            &s,
            now,
            &all,
            &held,
            &free,
            &[target("HOOD", "0.158", "800", 0)],
            Some(Decimal::from(1500)),
        )
        .unwrap_err();
        assert!(err.contains("ARB_MAX_POSITION_USDT=1500"), "{err}");
        // 方向相反的仓位（多 RH / 空 Arcus）：不是同一笔对冲，不补。
        let mut reverse = target("HOOD", "0.158", "800", 0);
        (reverse.long, reverse.short) = (Venue::LighterRh, Venue::Arcus);
        assert!(pick_add(&v, &s, now, &all, &held, &free, &[reverse], None).is_err());
        // 信号没保持够、合约在冷却：不补。
        let err = pick_add(&v, &s, now, &all, &|_| 2, &free, &targets, None).unwrap_err();
        assert!(err.contains("2/5"), "{err}");
        let cooling = |_: &str| Some("冷却中（还有 300 秒）".to_string());
        assert!(
            pick_add(&v, &s, now, &all, &held, &cooling, &targets, None)
                .unwrap_err()
                .contains("冷却")
        );
        // 没开补仓：什么都不补。
        assert!(pick_add(&v, &settings(), now, &all, &held, &free, &targets, None).is_err());
    }

    #[test]
    fn a_strong_signal_does_not_wait_for_the_hold_and_a_normal_one_still_does() {
        // 门槛 0.05%、滑点缓冲 0.05%、倍数 2：扣完缓冲 ≥ 0.10%（回到正常 ≥ 0.15%）就不等。
        let s = Settings {
            hold_sec: 5,
            hold_ms: None,
            open_gap_sec: 60,
            slippage_buffer_pct: dec("0.05"),
            instant_multiple: Some(dec("2")),
            ..settings()
        };
        let now = Utc::now();
        let all = |_: Venue| true;
        let fresh = |_: &Candidate| 0;
        let free = |_: &str| None;
        let strong = view(vec![line(
            "HOOD",
            "long_a",
            leg("0.3", "0.2", "0.15"),
            -0.11,
        )]);
        let picked = pick(&strong, &s, now, &all, &fresh, &free).unwrap();
        assert_eq!(picked.base, "HOOD");
        assert_eq!(s.required_hold_ms(&picked), 0);
        // 只过门槛、不到两倍（扣完缓冲 0.09%）：照旧等满 5 秒。
        let normal = view(vec![line(
            "HOOD",
            "long_a",
            leg("0.3", "0.2", "0.14"),
            -0.11,
        )]);
        let err = pick(&normal, &s, now, &all, &fresh, &free).unwrap_err();
        assert!(err.contains("0/5"), "{err}");
        assert!(pick(&normal, &s, now, &all, &|_| 5, &free).is_ok());
        // 没开：强信号也等。
        let off = Settings {
            instant_multiple: None,
            ..s.clone()
        };
        assert!(pick(&strong, &off, now, &all, &fresh, &free).is_err());
        // 其它挡板照旧：已有仓位 / 冷却的合约，强信号也不下。
        let busy = |_: &str| Some("已有这个合约的仓位".to_string());
        assert!(
            pick(&strong, &s, now, &all, &fresh, &busy)
                .unwrap_err()
                .contains("已有这个合约的仓位")
        );
        // 倍数要大于 1、不超过 20；描述里写明。
        for bad in ["1", "0.5", "21"] {
            let bad = Settings {
                instant_multiple: Some(dec(bad)),
                ..s.clone()
            };
            assert!(bad.validate(None).unwrap_err().contains("倍数"));
        }
        assert!(
            describe(&s).contains("门槛 2 倍时不等保持"),
            "{}",
            describe(&s)
        );
    }

    fn moving(mut line: Line, a_pct: &str, b_pct: &str) -> Line {
        line.recent_move = Some(crate::rh_spread::RecentMove {
            a_pct: dec(a_pct),
            b_pct: dec(b_pct),
        });
        line
    }

    fn fast_settings(limit: &str) -> Settings {
        Settings {
            fast_move_pct: Some(dec(limit)),
            ..settings()
        }
    }

    /// Bitget 股票类合约 5×24：纽约时间周五 16:00 到周一 4:00 不选它的组（下单路径也挡，这里是不让整个
    /// 周末每 10 分钟被拒一次）；加密币合约、别家的组照常。
    #[test]
    fn a_sub_second_hold_opens_after_half_a_second_and_reads_old_settings_unchanged() {
        let s = Settings {
            hold_sec: 5,
            hold_ms: Some(500),
            ..settings()
        };
        assert!(s.validate(None).is_ok());
        let now = Utc::now();
        let all = |_: Venue| true;
        let none = |_: &Candidate| None;
        let normal = view(vec![line(
            "HOOD",
            "long_a",
            leg("0.3", "0.2", "0.14"),
            -0.11,
        )]);
        let err = pick_checked(&normal, &s, now, &all, &|_| 499, &none).unwrap_err();
        assert!(err.contains("0.4/0.5"), "{err}");
        let picked = pick_checked(&normal, &s, now, &all, &|_| 500, &none).unwrap();
        assert_eq!(picked.signal_sec, 0);
        // 没填毫秒：照旧按整秒。
        let whole = Settings {
            hold_ms: None,
            open_gap_sec: 60,
            ..s.clone()
        };
        assert!(pick_checked(&normal, &whole, now, &all, &|_| 4_999, &none).is_err());
        assert!(pick_checked(&normal, &whole, now, &all, &|_| 5_000, &none).is_ok());
        // 范围：0 ～ 600 秒；0 = 检测到就下（没保持也下），但不算强信号。
        let s600 = Settings {
            hold_ms: Some(600_001),
            ..s.clone()
        };
        assert!(s600.validate(None).is_err());
        let zero = Settings {
            hold_ms: Some(0),
            ..s.clone()
        };
        assert!(zero.validate(None).is_ok());
        let picked = pick_checked(&normal, &zero, now, &all, &|_| 0, &none).unwrap();
        assert_eq!(zero.required_hold_ms(&picked), 0);
        assert!(!zero.is_strong(&picked));
        // 旧设置文件没有 hold_ms：读回来是 None；不填时也不写出去，旧版本照样读得回。
        let mut old = serde_json::to_value(&whole).unwrap();
        assert!(old.get("hold_ms").is_none());
        old["hold_sec"] = serde_json::json!(7);
        let back: Settings = serde_json::from_value(old).unwrap();
        assert_eq!((back.hold_ms, back.hold_millis()), (None, 7_000));
        let round: Settings = serde_json::from_value(serde_json::to_value(&s).unwrap()).unwrap();
        assert_eq!(round.hold_millis(), 500);
        assert_eq!(secs_text(500), "0.5");
        assert_eq!(secs_text(5_000), "5");
    }

    #[test]
    fn stock_lines_wait_out_the_first_hour_after_the_us_open() {
        use chrono::TimeZone;
        // 2026-11-10 周二，纽约是冬令时（UTC−5）：开盘 14:30 UTC。
        let at = |h, m| Utc.with_ymd_and_hms(2026, 11, 10, h, m, 0).unwrap();
        let mut stock = line("NVDA", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        stock.session = Session::Rth;
        let mut crypto = stock.clone();
        crypto.session = Session::All;
        for blocked in [at(14, 30), at(15, 0), at(15, 29)] {
            let why = venue_closed_reason(&stock, blocked).unwrap();
            assert!(why.contains("开盘头一小时"), "{why}");
            assert!(
                venue_closed_reason(&crypto, blocked).is_none(),
                "加密币照常"
            );
        }
        for free in [at(14, 29), at(15, 30), at(18, 0)] {
            assert!(opening_hour_reason(&stock, free).is_none(), "{free}");
        }
        // 周六不算；夏令时（7 月）开盘是 13:30 UTC。
        assert!(
            opening_hour_reason(
                &stock,
                Utc.with_ymd_and_hms(2026, 11, 14, 14, 40, 0).unwrap()
            )
            .is_none()
        );
        assert!(
            opening_hour_reason(
                &stock,
                Utc.with_ymd_and_hms(2026, 7, 14, 13, 40, 0).unwrap()
            )
            .is_some()
        );
        assert!(
            opening_hour_reason(
                &stock,
                Utc.with_ymd_and_hms(2026, 7, 14, 14, 40, 0).unwrap()
            )
            .is_none()
        );
    }

    #[test]
    fn lines_with_a_lighter_leg_wait_a_minute_after_a_lighter_rate_limit() {
        use chrono::TimeZone;
        let at = Utc.with_ymd_and_hms(2026, 11, 3, 12, 0, 0).unwrap();
        let mut lighter = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        lighter.a = Venue::Lighter;
        lighter.b = Venue::HyperliquidXyz;
        let mut other = lighter.clone();
        other.a = Venue::Binance;
        arb_exec::lighter_broker::set_rate_limited_at(at.timestamp_millis());
        let why = venue_closed_reason(&lighter, at + chrono::Duration::seconds(20)).unwrap();
        assert!(why.contains("429") && why.contains("40"), "{why}");
        assert!(venue_closed_reason(&other, at).is_none(), "不带 Lighter 腿");
        assert!(venue_closed_reason(&lighter, at + chrono::Duration::seconds(61)).is_none());
        arb_exec::lighter_broker::set_rate_limited_at(0);
    }

    #[test]
    fn lines_with_a_lighter_leg_are_skipped_around_its_sunday_upgrade() {
        use chrono::TimeZone;
        let at = |d, h, m| Utc.with_ymd_and_hms(2026, 11, d, h, m, 0).unwrap();
        let mut lighter = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        lighter.a = Venue::Lighter;
        lighter.b = Venue::HyperliquidXyz;
        let mut rh = lighter.clone();
        rh.a = Venue::HyperliquidXyz;
        rh.b = Venue::LighterRh;
        let mut other = lighter.clone();
        other.a = Venue::Binance;
        // 11-08 是周日。
        for blocked in [at(8, 10, 45), at(8, 11, 0), at(8, 11, 44)] {
            for l in [&lighter, &rh] {
                let why = venue_closed_reason(l, blocked).unwrap();
                assert!(why.contains("Lighter"), "{why}");
            }
            assert!(
                venue_closed_reason(&other, blocked).is_none(),
                "不带 Lighter 腿"
            );
        }
        for free in [at(8, 10, 44), at(8, 11, 45), at(7, 11, 0), at(9, 11, 0)] {
            assert!(venue_closed_reason(&lighter, free).is_none(), "{free}");
        }
    }

    #[test]
    fn bitget_stock_lines_are_skipped_while_its_market_is_closed() {
        use chrono::TimeZone;
        let saturday = Utc.with_ymd_and_hms(2026, 10, 24, 15, 0, 0).unwrap();
        let wednesday = Utc.with_ymd_and_hms(2026, 10, 21, 15, 0, 0).unwrap();
        let mut bitget = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        bitget.a = Venue::HyperliquidXyz;
        bitget.b = Venue::Bitget;
        assert!(venue_closed_reason(&bitget, wednesday).is_none());
        let reason = venue_closed_reason(&bitget, saturday).unwrap();
        assert!(reason.contains("Bitget"), "{reason}");
        let mut crypto = bitget.clone();
        crypto.session = Session::All;
        assert!(
            venue_closed_reason(&crypto, saturday).is_none(),
            "加密币合约不休市"
        );
        let other = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        assert!(
            venue_closed_reason(&other, saturday).is_none(),
            "不是 Bitget 的组"
        );
    }

    #[test]
    fn a_fast_market_on_either_venue_blocks_the_signal_only_when_the_switch_is_on() {
        let base = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        // 开关关着：再急的行情也不管；幅度只记下来。
        let calm = moving(base.clone(), "0.9", "0.9");
        assert!(candidate_of(&calm, &settings()).is_some());
        // 开了，0.15%：任何一家（a = Arcus 或 b = Lighter RH）达到就挡。
        for (a, b) in [("0.20", "0.01"), ("0.01", "0.20"), ("0.15", "0.0")] {
            let fast = moving(base.clone(), a, b);
            assert!(
                candidate_of(&fast, &fast_settings("0.15")).is_none(),
                "{a}/{b}"
            );
        }
        // 两家都没到：照常。
        let ok = moving(base.clone(), "0.149", "0.02");
        assert!(candidate_of(&ok, &fast_settings("0.15")).is_some());
        // 还没记满 5 秒窗口（刚启动 / 刚重连）：开关开着就不开 —— 复盘这种时候执行偏差最大
        // （Lighter 重连 7 秒后开仓，HL-xyz 比预估差 0.227%）；开关关着照常。
        assert!(base.recent_move.is_none());
        assert!(candidate_of(&base, &fast_settings("0.15")).is_none());
        let reason = fast_move_reason(&base, &fast_settings("0.15")).unwrap();
        assert!(reason.contains("还没记满"), "{reason}");
        assert!(candidate_of(&base, &settings()).is_some());
        // 原因里写明是哪一家、多少、阈值多少。
        let fast = moving(base.clone(), "0.01", "0.2");
        let reason = fast_move_reason(&fast, &fast_settings("0.15")).unwrap();
        assert!(
            reason.contains("lighter-rh") && reason.contains("0.2%") && reason.contains("0.15%"),
            "{reason}"
        );
        // 面板的说明与手动下单按钮的提示都用同一个原因；净收益不够时仍先说净收益。
        let blocked =
            blocked_signal_reason(&view(vec![fast.clone()]), &fast_settings("0.15")).unwrap();
        assert!(
            blocked.contains("急行情") && blocked.contains("不自动下单"),
            "{blocked}"
        );
        let weak = moving(
            line("SOXL", "long_a", leg("0.30", "0.20", "0.04"), -0.11),
            "0.9",
            "0.9",
        );
        assert!(
            auto_skip_reason(&weak, weak.long_a.as_ref().unwrap(), &fast_settings("0.15"))
                .unwrap()
                .contains("不到门槛")
        );
        // 人点的 Telegram 下单按钮不被它拦：skip_reason 本身不看行情急不急。
        assert!(
            skip_reason(&fast, fast.long_a.as_ref().unwrap(), &fast_settings("0.15")).is_none()
        );
    }

    #[test]
    fn a_fast_market_resets_the_hold_and_the_signal_must_come_back_after_it_calms() {
        let s = Settings {
            hold_sec: 5,
            hold_ms: None,
            open_gap_sec: 60,
            ..fast_settings("0.15")
        };
        let base = moving(
            line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            "0.02",
            "0.01",
        );
        let key = candidate_of(&base, &s).unwrap().hold_key();
        let (mut since, mut seen) = (HashMap::new(), HashMap::new());
        let t0 = Instant::now();
        track_holds(&view(vec![base.clone()]), &s, &mut since, &mut seen, t0);
        assert!(since.contains_key(&key));
        // 行情急了：不算达标，超过 HOLD_GRACE 计时清掉。
        let fast = moving(base.clone(), "0.4", "0.4");
        let later = t0 + HOLD_GRACE + Duration::from_millis(500);
        track_holds(&view(vec![fast]), &s, &mut since, &mut seen, later);
        assert!(since.is_empty(), "计时应清零");
        // 稳住之后信号还在：从头计，不是接着以前的算。
        let calm = moving(base, "0.02", "0.01");
        let calm_at = later + Duration::from_secs(1);
        track_holds(&view(vec![calm]), &s, &mut since, &mut seen, calm_at);
        assert_eq!(since.get(&key), Some(&calm_at));
    }

    #[test]
    fn picking_waits_out_a_fast_market_and_a_strong_signal_goes_right_after_it_calms() {
        let s = Settings {
            hold_sec: 5,
            hold_ms: None,
            open_gap_sec: 60,
            slippage_buffer_pct: dec("0.05"),
            instant_multiple: Some(dec("2")),
            ..fast_settings("0.15")
        };
        let now = Utc::now();
        let (all, fresh, free) = (|_: Venue| true, |_: &Candidate| 0, |_: &str| None);
        let base = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        let fast = view(vec![moving(base.clone(), "0.3", "0.05")]);
        let err = pick(&fast, &s, now, &all, &fresh, &free).unwrap_err();
        assert!(err.contains("急行情"), "{err}");
        let calm = view(vec![moving(base, "0.03", "0.05")]);
        let picked = pick(&calm, &s, now, &all, &fresh, &free).unwrap();
        assert_eq!(picked.base, "SOXL");
        assert_eq!(
            s.required_hold_ms(&picked),
            0,
            "强信号：稳住就下，不再等保持"
        );
    }

    #[test]
    fn candidates_carry_the_recent_moves_in_long_and_short_order_whether_or_not_the_switch_is_on() {
        // 多 a（Arcus）空 b（Lighter RH）：多腿 = a 的幅度。
        let long_a = moving(
            line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            "0.03",
            "0.12",
        );
        assert_eq!(
            candidate_of(&long_a, &settings()).unwrap().recent_move_pct,
            Some((dec("0.03"), dec("0.12")))
        );
        // 多 b 空 a：反过来。
        let long_b = moving(
            line("SOXL", "long_b", leg("0.30", "0.20", "0.15"), 0.11),
            "0.03",
            "0.12",
        );
        assert_eq!(
            candidate_of(&long_b, &settings()).unwrap().recent_move_pct,
            Some((dec("0.12"), dec("0.03")))
        );
        let unknown = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        assert_eq!(
            candidate_of(&unknown, &settings()).unwrap().recent_move_pct,
            None
        );
    }

    #[test]
    fn only_signals_that_would_otherwise_trade_are_listed_as_blocked_by_a_fast_market() {
        let s = fast_settings("0.15");
        let good = moving(
            line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            "0.3",
            "0.0",
        );
        let weak = moving(
            line("INTC", "long_a", leg("0.30", "0.20", "0.01"), -0.11),
            "0.3",
            "0.0",
        );
        let calm = moving(
            line("NVDA", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            "0.01",
            "0.0",
        );
        let listed = fast_move_blocked(&view(vec![good, weak, calm]), &s);
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].0, "SOXL");
        assert!(listed[0].1.contains("急行情"));
        // 开关关着、或不在名单里：不记。
        let line_fast = moving(
            line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            "0.3",
            "0.0",
        );
        assert!(fast_move_blocked(&view(vec![line_fast.clone()]), &settings()).is_empty());
        let only_nvda = Settings {
            symbols: vec!["NVDA".into()],
            ..s
        };
        assert!(fast_move_blocked(&view(vec![line_fast]), &only_nvda).is_empty());
    }

    #[test]
    fn the_fast_move_limit_is_validated_described_and_loads_from_json() {
        for bad in ["0", "-0.1", "5.1"] {
            let s = Settings {
                fast_move_pct: Some(dec(bad)),
                ..settings()
            };
            assert!(s.validate(None).unwrap_err().contains("急行情"), "{bad}");
        }
        for good in ["0.05", "0.15", "5"] {
            assert!(fast_settings(good).validate(None).is_ok(), "{good}");
        }
        assert!(settings().validate(None).is_ok());
        assert!(describe(&fast_settings("0.15")).contains("5 秒内波动 ≥ 0.15% 不开"));
        assert!(!describe(&settings()).contains("急行情"));
        let saved: Settings =
            serde_json::from_value(serde_json::to_value(fast_settings("0.15")).unwrap()).unwrap();
        assert_eq!(saved.fast_move_pct, Some(dec("0.15")));
    }

    fn depth_settings(pct: &str) -> Settings {
        Settings {
            depth_fit_pct: Some(dec(pct)),
            ..settings()
        }
    }

    #[test]
    fn depth_sizing_is_validated_described_and_defaults_off_for_old_files() {
        for bad in ["0", "-5", "100.1"] {
            assert!(
                depth_settings(bad)
                    .validate(None)
                    .unwrap_err()
                    .contains("盘口深度"),
                "{bad}"
            );
        }
        for good in ["10", "50", "100"] {
            assert!(depth_settings(good).validate(None).is_ok(), "{good}");
        }
        let bad_min = Settings {
            depth_min_usdt: Decimal::ZERO,
            ..depth_settings("50")
        };
        assert!(bad_min.validate(None).unwrap_err().contains("最小单笔"));
        // 没开：固定单笔，不出现在描述里。
        assert_eq!(settings().depth_fit(), None);
        assert!(!describe(&settings()).contains("按盘口深度"));
        let on = depth_settings("50");
        let fit = on.depth_fit().unwrap();
        assert_eq!((fit.fraction, fit.min_size), (dec("0.5"), dec("50")));
        let text = describe(&on);
        assert!(
            text.contains("按盘口深度定单笔")
                && text.contains("50%")
                && text.contains("单腿名义是上限"),
            "{text}"
        );
        // 旧的 auto.json 没有这两项：关着，最小单笔取默认 50。
        let old: Persisted = serde_json::from_value(serde_json::json!({
            "settings": {
                "enabled": true, "mode": "live", "size_usdt": "800", "leverage": "3",
                "min_net_pct": "0.05", "hold_sec": 5, "back_to_normal": false,
                "take_profit_usdt": "0.05", "max_positions": 5, "daily_max_opens": 20, "symbols": []
            },
            "opened": []
        }))
        .unwrap();
        assert_eq!(old.settings.depth_fit_pct, None);
        assert_eq!(old.settings.depth_min_usdt, Decimal::from(50));
    }

    #[test]
    fn only_two_taker_orders_carry_the_depth_sizing_into_the_open_request() {
        let candidate = candidate_of(
            &line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            &settings(),
        )
        .unwrap();
        // 没开：不带。
        assert_eq!(
            AutoTrader::open_body(&settings(), &candidate).depth_fit(),
            None
        );
        // 开了：两腿都吃单的单子带上，单腿名义（size_usdt）当上限。
        let on = depth_settings("40");
        let body = AutoTrader::open_body(&on, &candidate);
        let fit = body.depth_fit().expect("吃单的单子带上");
        assert_eq!((fit.fraction, fit.min_size), (dec("0.4"), dec("50")));
        // 币安挂单的单子（一条腿只挂单）照旧固定单笔。
        let maker = Candidate {
            maker: Some(arb_exec::desk::MakerLeg {
                venue: Venue::Binance,
                entry_fee: Decimal::ZERO,
                exit_fee: dec("0.0004"),
                rest_ms: 8_000,
            }),
            ..candidate
        };
        assert_eq!(AutoTrader::open_body(&on, &maker).depth_fit(), None);
    }

    #[test]
    fn the_event_says_the_real_size_when_the_depth_shrunk_it_and_a_depth_refusal_is_not_a_failure()
    {
        let shrunk = serde_json::json!({
            "prepared": {
                "size_usdt": "118",
                "fit": { "requested": "200", "fitted": "118", "first_depth_usdt": "5000.4", "second_depth_usdt": "240" }
            }
        });
        let note = sized_note(&shrunk, dec("200")).unwrap();
        assert!(
            note.contains("实际单笔 118 USDT")
                && note.contains("上限 200")
                && note.contains("约 240 USDT"),
            "{note}"
        );
        // 没缩小（实际 = 上限）、没有这个信息（被拒的响应没有 prepared）：不说。
        let same = serde_json::json!({ "prepared": { "size_usdt": "200" } });
        assert_eq!(sized_note(&same, dec("200")), None);
        assert_eq!(
            sized_note(&serde_json::json!({ "error": "x" }), dec("200")),
            None
        );
        // 深度不够被 prepare 拒绝走 422：只冷却，不进熔断（连续回滚计数）。
        let refused = serde_json::json!({ "error": "盘口深度不够：单笔最多占限价内可吃深度的 50%，不到最小单笔 50 USDT，不下单" });
        assert_eq!(
            classify(StatusCode::UNPROCESSABLE_ENTITY, &refused),
            Outcome::Rejected
        );
        let candidate = candidate_of(
            &line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            &settings(),
        )
        .unwrap();
        assert_eq!(
            classify_attempt(&candidate, StatusCode::UNPROCESSABLE_ENTITY, &refused),
            Outcome::Rejected
        );
    }

    #[test]
    fn the_forced_hedge_limit_is_validated_described_carried_to_the_request_and_defaults_off() {
        for bad in ["0", "-0.1", "1.1"] {
            let s = Settings {
                force_hedge_pct: Some(dec(bad)),
                ..settings()
            };
            assert!(s.validate(None).unwrap_err().contains("强制补腿"), "{bad}");
        }
        for good in ["0.05", "0.3", "1"] {
            let s = Settings {
                force_hedge_pct: Some(dec(good)),
                ..settings()
            };
            assert!(s.validate(None).is_ok(), "{good}");
        }
        assert!(!describe(&settings()).contains("强制补腿"));
        let on = Settings {
            force_hedge_pct: Some(dec("0.3")),
            ..settings()
        };
        assert!(
            describe(&on).contains("强制补腿") && describe(&on).contains("−0.3%"),
            "{}",
            describe(&on)
        );
        // 两腿都吃单的单子带上，币安挂单的单子不带。
        let candidate = candidate_of(
            &line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            &settings(),
        )
        .unwrap();
        assert_eq!(
            AutoTrader::open_body(&settings(), &candidate).force_hedge_pct(),
            None
        );
        assert_eq!(
            AutoTrader::open_body(&on, &candidate).force_hedge_pct(),
            Some(dec("0.3"))
        );
        let maker = Candidate {
            maker: Some(arb_exec::desk::MakerLeg {
                venue: Venue::Binance,
                entry_fee: Decimal::ZERO,
                exit_fee: dec("0.0004"),
                rest_ms: 8_000,
            }),
            ..candidate
        };
        assert_eq!(AutoTrader::open_body(&on, &maker).force_hedge_pct(), None);
        // 旧的 auto.json 没有这一项：关着。
        let old: Persisted = serde_json::from_value(serde_json::json!({
            "settings": {
                "enabled": true, "mode": "live", "size_usdt": "800", "leverage": "3",
                "min_net_pct": "0.05", "hold_sec": 5, "back_to_normal": false,
                "take_profit_usdt": "0.05", "max_positions": 5, "daily_max_opens": 20, "symbols": []
            },
            "opened": []
        }))
        .unwrap();
        assert_eq!(old.settings.force_hedge_pct, None);
        // 事件里把「被追出来的」单子标出来；普通的单子没有。
        let forced = serde_json::json!({ "position": { "note": format!("{}：第二腿……锁定价差 -0.12%", arb_exec::FORCED_HEDGE_NOTE) } });
        assert!(
            forced_hedge_note(&forced)
                .unwrap()
                .starts_with(arb_exec::FORCED_HEDGE_NOTE)
        );
        assert_eq!(
            forced_hedge_note(&serde_json::json!({ "position": { "note": "普通" } })),
            None
        );
        assert_eq!(
            forced_hedge_note(&serde_json::json!({ "error": "x" })),
            None
        );
    }

    #[test]
    fn new_settings_validate_and_old_files_load_with_adds_and_timeouts_off() {
        let ok = Settings {
            take_profit_usdt: Some(dec("0.05")),
            auto_margin_pct: Some(dec("15")),
            auto_margin_max_usdt: Some(dec("100")),
            timeout_hours: Some(dec("4")),
            timeout_max_loss_usdt: Some(dec("0.5")),
            ..adding()
        };
        assert!(ok.validate(None).is_ok());
        let rules = ok.rules(Some(dec("0.1")));
        assert_eq!(rules.auto_margin(), Some((dec("15"), dec("100"))));
        assert_eq!(rules.timeout_exit(), Some((dec("4"), dec("0.5"))));
        assert_eq!(rules.basis_exit_pct, Some(dec("0.1")));
        // 成对的规则只给一半：与开仓路径同一个校验，存都存不进去。
        let half = Settings {
            auto_margin_max_usdt: None,
            ..ok.clone()
        };
        assert!(half.validate(None).unwrap_err().contains("累计上限"));
        let half = Settings {
            timeout_max_loss_usdt: None,
            ..ok.clone()
        };
        assert!(half.validate(None).is_err());
        // 加保证金的触发线必须高于爆仓保护。
        let crossed = Settings {
            liq_protection_pct: Some(dec("20")),
            ..ok.clone()
        };
        assert!(
            crossed
                .validate(None)
                .unwrap_err()
                .contains("必须高于爆仓保护")
        );
        let bad_multiple = Settings {
            add_max_multiple: Decimal::ONE,
            ..ok.clone()
        };
        assert!(bad_multiple.validate(None).is_err());
        // 旧的 auto.json 没有这些字段：补仓、超时、加保证金全关，补仓次数与倍数取默认。
        let old: Persisted = serde_json::from_value(serde_json::json!({
            "settings": {
                "enabled": true, "mode": "live", "size_usdt": "800", "leverage": "3",
                "min_net_pct": "0.05", "hold_sec": 5, "back_to_normal": false,
                "take_profit_usdt": "0.05", "liq_protection_pct": null,
                "max_positions": 5, "daily_max_opens": 20, "symbols": []
            },
            "opened": [{"id": "live-1", "symbol": "SLV/USDT", "mode": "live", "at": "2026-10-09T14:50:32Z"}]
        }))
        .unwrap();
        assert_eq!(old.settings.add_min_improve_pct, None);
        assert_eq!(old.settings.timeout_hours, None);
        assert_eq!(old.settings.auto_margin_pct, None);
        assert_eq!(old.settings.add_max_times, 2);
        assert_eq!(old.settings.add_max_multiple, Decimal::from(3));
        assert_eq!(
            old.settings.fast_move_pct, None,
            "旧文件：急行情不开仓默认关"
        );
        assert!(!old.opened[0].add);
    }

    #[test]
    fn a_qualifying_line_becomes_an_order_with_the_back_to_normal_target_in_position_terms() {
        // 页面基差 (Arcus − RH) 正常 −0.11%；多 Arcus / 空 RH 的持仓基差 (空 − 多) = −页面基差 → 目标 +0.11%。
        let nvda = line("NVDA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        let c = candidate_of(&nvda, &settings()).expect("够门槛");
        assert_eq!((c.long, c.short), (Venue::Arcus, Venue::LighterRh));
        assert_eq!(c.target_pct, Some(dec("0.11")));
        let reverse = line("NVDA", "long_b", leg("0.30", "0.20", "0.09"), -0.11);
        let c = candidate_of(&reverse, &settings()).unwrap();
        assert_eq!((c.long, c.short), (Venue::LighterRh, Venue::Arcus));
        assert_eq!(c.target_pct, Some(dec("-0.11")));
        // 只开止盈、不按正常基差平：不带收敛目标。
        let no_target = Settings {
            back_to_normal: false,
            take_profit_usdt: Some(Decimal::ONE),
            ..settings()
        };
        assert_eq!(candidate_of(&nvda, &no_target).unwrap().target_pct, None);
    }

    #[test]
    fn lines_the_order_path_would_reject_are_never_picked() {
        let s = settings();
        // 其它组同样适用（规则相同）：多 HL-xyz / 空 RH。
        let mut other = line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11);
        other.pair = "hyperliquid-xyz:lighter-rh".into();
        other.a = Venue::HyperliquidXyz;
        let c = candidate_of(&other, &s).expect("别的组也做");
        assert_eq!((c.long, c.short), (Venue::HyperliquidXyz, Venue::LighterRh));
        // 开仓价差为负（如 io ↔ RH 的 某合约 长期差 2%）：哪个组都不做。
        let mut io = line("ANTHROPIC", "long_b", leg("-2.17", "-2.19", "0.087"), -2.27);
        io.pair = "hyperliquid-io:lighter-rh".into();
        io.a = Venue::HyperliquidIo;
        assert!(candidate_of(&io, &s).is_none());
        // 回到正常净收益不够门槛。
        assert!(candidate_of(&line("A", "long_a", leg("0.3", "0.2", "0.04"), -0.1), &s).is_none());
        // 可成交价差不为正（价差单的硬性条件）。
        assert!(
            candidate_of(&line("A", "long_a", leg("-0.01", "0.2", "0.09"), -0.1), &s).is_none()
        );
        // 收敛到 0 也不划算（下单路径按它拒绝）。
        assert!(
            candidate_of(&line("A", "long_a", leg("0.1", "-0.01", "0.09"), -0.1), &s).is_none()
        );
        // 盘口过期等。
        let mut stale = line("A", "long_a", leg("0.3", "0.2", "0.09"), -0.1);
        stale.note = Some("Arcus 盘口 20 秒没更新".into());
        assert!(candidate_of(&stale, &s).is_none());
        // 没有正常样本。
        let mut fresh = line("A", "long_a", leg("0.3", "0.2", "0.09"), -0.1);
        fresh.normal = None;
        assert!(candidate_of(&fresh, &s).is_none());
        // 目标超出规则允许的 ±5%：不截断成另一个意思的数，直接不做。
        assert!(candidate_of(&line("A", "long_a", leg("0.3", "0.2", "0.09"), 7.0), &s).is_none());
    }

    /// 2026-11-01 起币安和 Bitget / MEXC 一样：价差监控不再把币安组标成只观察，自动交易按通用规则、通用单腿名义
    /// 两腿吃单；旧的币安专用开关（吃单 / 挂单 / 只做币安）读进来一律关掉 —— 尤其「只做币安」开着会挡掉所有组合。
    /// 资金费结算点前后照旧不下币安单（吃单也要查订单，结算时查询会落后）。
    #[test]
    fn binance_lines_trade_like_any_other_pair_with_the_general_rules() {
        use chrono::TimeZone;
        let mut old = Settings {
            binance_only: true,
            binance_taker: true,
            binance_maker: true,
            binance_symbols: vec!["QQQ".into()],
            binance_size_usdt: dec("50"),
            ..settings()
        };
        old.retire_binance();
        assert!(!old.binance_only && !old.binance_taker && !old.binance_maker);
        let mut binance = line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11);
        binance.pair = format!("{}:{}", Venue::Arcus, Venue::Binance);
        binance.b = Venue::Binance;
        let plain = line("SPY", "long_a", leg("0.3", "0.2", "0.09"), -0.11);
        assert!(
            candidate_of(&plain, &old).is_some(),
            "「只做币安」作废后，别的组合照常"
        );
        let candidate = candidate_of(&binance, &old).expect("币安组是普通候选");
        assert!(
            candidate.maker.is_none() && !candidate.binance,
            "两腿吃单，不走币安专用的那套"
        );
        assert_eq!(
            candidate.size(&old),
            old.size_usdt,
            "用通用单腿名义，不用币安单腿名义"
        );

        let at = |h: u32, m: u32, sec: u32| Utc.with_ymd_and_hms(2026, 11, 2, h, m, sec).unwrap();
        let mut v = view(vec![binance]);
        v.connected.venues.insert(Venue::Binance, true);
        let all = |_: Venue| true;
        let free = |_: &str| None;
        let pick_at = |now| {
            let mut v = v.clone();
            v.updated_at = Some(now);
            pick(&v, &old, now, &all, &|_| u64::MAX, &free)
        };
        let why = pick_at(at(23, 59, 30)).unwrap_err();
        assert!(why.contains("NVDA") && why.contains("结算点"), "{why}");
        assert_eq!(pick_at(at(2, 0, 0)).unwrap().base, "NVDA");
    }

    #[tokio::test]
    async fn saving_or_loading_settings_switches_the_old_binance_switches_off() {
        let trader = AutoTrader::in_memory(Settings::default());
        let saved = trader
            .update(
                Settings {
                    binance_only: true,
                    binance_taker: true,
                    binance_symbols: vec!["AMD".into()],
                    ..settings()
                },
                None,
                true,
            )
            .await
            .unwrap();
        assert!(!saved.binance_only && !saved.binance_taker && !saved.binance_maker);
        assert!(!trader.settings().await.binance_only);

        let dir = std::env::temp_dir().join(format!("arb-retire-binance-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut persisted = Persisted::default();
        persisted.settings.binance_only = true;
        persisted.settings.binance_maker = true;
        std::fs::write(
            dir.join("auto.json"),
            serde_json::to_string(&persisted).unwrap(),
        )
        .unwrap();
        let loaded = AutoTrader::load(&dir).settings().await;
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(!loaded.binance_only && !loaded.binance_maker);
    }

    #[test]
    fn picking_waits_for_the_hold_time_and_skips_busy_symbols() {
        let s = Settings {
            hold_sec: 10,
            hold_ms: None,
            open_gap_sec: 60,
            ..settings()
        };
        let v = view(vec![
            line("NVDA", "long_a", leg("0.3", "0.2", "0.09"), -0.11),
            line("SPY", "long_a", leg("0.3", "0.2", "0.08"), -0.11),
        ]);
        let now = Utc::now();
        let none = |_: &str| None;
        let all = |_: Venue| true;
        let err = pick(&v, &s, now, &all, &|_| 3, &none).unwrap_err();
        assert!(err.contains("3/10"), "{err}");
        let held = |c: &Candidate| if c.base == "NVDA" { 12 } else { 30 };
        assert_eq!(pick(&v, &s, now, &all, &held, &none).unwrap().base, "NVDA");
        // 已有 NVDA 仓位：跳到下一条。
        let busy = |b: &str| (b == "NVDA").then(|| "已有这个合约的仓位".to_string());
        assert_eq!(pick(&v, &s, now, &all, &held, &busy).unwrap().base, "SPY");
        // 名单只做 SPY。
        let only = Settings {
            symbols: vec!["SPY".into()],
            ..s.clone()
        };
        assert_eq!(
            pick(&v, &only, now, &all, &held, &none).unwrap().base,
            "SPY"
        );
        // 行情断了、快照旧了：不下单。
        let mut down = v.clone();
        down.connected.venues.insert(Venue::Arcus, false);
        assert!(pick(&down, &s, now, &all, &held, &none).is_err());
        let mut old = v.clone();
        old.updated_at = Some(now - chrono::Duration::seconds(30));
        assert!(pick(&old, &s, now, &all, &held, &none).is_err());
        // 实盘只连了 Arcus（RH 没连上 / 没配 API）：两腿不全，不做。
        let arcus_only = |v: Venue| v == Venue::Arcus;
        assert!(pick(&v, &s, now, &arcus_only, &held, &none).is_err());
    }

    /// 暂停（`step` 提前返回、不更新计时）期间信号是不是一直在不知道：恢复后从头计时。
    #[test]
    fn hold_timers_restart_after_a_stretch_without_observation() {
        let s = settings();
        let (mut since, mut seen) = (HashMap::new(), HashMap::new());
        let t0 = Instant::now();
        let good = view(vec![line(
            "NVDA",
            "long_a",
            leg("0.3", "0.2", "0.09"),
            -0.11,
        )]);
        let key = ("NVDA".to_string(), Venue::Arcus, Venue::LighterRh);
        track_holds(&good, &s, &mut since, &mut seen, t0);
        // 开仓占了 5 秒：照常累计。
        track_holds(
            &good,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_secs(5),
        );
        assert_eq!(since[&key], t0);
        // 暂停了 10 分钟，恢复时信号还在：从恢复的这一刻算。
        let resumed = t0 + Duration::from_secs(605);
        track_holds(&good, &s, &mut since, &mut seen, resumed);
        assert_eq!(since[&key], resumed);
    }

    /// 信号保持影子的夹具：多 Arcus / 空 Lighter RH 的弱信号，反方向（平仓）价差可调。
    fn shadow_view(net: &str, back_entry: &str) -> View {
        let mut l = line("NVDA", "long_a", leg("0.3", "0.2", net), -0.11);
        l.long_b = Some(leg(back_entry, "-0.5", "-0.5"));
        view(vec![l])
    }

    fn shadow_settings() -> Settings {
        Settings {
            hold_sec: 5,
            hold_ms: None,
            open_gap_sec: 60,
            size_usdt: Decimal::from(200),
            take_profit_usdt: Some(dec("0.05")),
            ..settings()
        }
    }

    /// 只撑了 0.8 秒的弱信号：现行规则（5 秒）不会开，0.5 秒规则会；按触发后 0.9 秒的价差虚拟成交，
    /// 平仓价差到止盈（0.05 / 200 = 0.025%）就记平仓，信号断了且有结果才写。
    #[test]
    fn hold_shadow_records_a_short_signal_only_the_quick_rule_would_trade() {
        // 实盘保持设成 0（检测到就下）时，影子照样按 0.5 秒 / 5 秒两种规则对照，不把弱信号当强信号跳过。
        let zero = Settings {
            hold_ms: Some(0),
            ..shadow_settings()
        };
        for s in [shadow_settings(), zero] {
            let mut shadow = hold_shadow::Shadow::new(None);
            let t0 = Instant::now();
            let at = |ms: u64| t0 + Duration::from_millis(ms);
            let utc = Utc::now();
            let (on, off) = (shadow_view("0.09", "-0.3"), shadow_view("0.01", "-0.3"));
            assert!(shadow.observe(&on, &s, &no_hist, at(0), utc).is_empty());
            assert!(shadow.observe(&on, &s, &no_hist, at(600), utc).is_empty());
            assert!(shadow.observe(&on, &s, &no_hist, at(800), utc).is_empty());
            // 掉下门槛；触发后 0.9 秒（1.5 秒）按那时的价差成交 —— 平仓价差还不够止盈。
            assert!(shadow.observe(&off, &s, &no_hist, at(1000), utc).is_empty());
            assert!(shadow.observe(&off, &s, &no_hist, at(1500), utc).is_empty());
            // 断满 1 秒（1.8 秒）：信号结束，但虚拟仓位还没平，不写。
            assert!(shadow.observe(&off, &s, &no_hist, at(1900), utc).is_empty());
            // 平仓价差回来：0.3 + (−0.2) − 0.045 = 0.055% ≥ 0.025%，平掉，写一行。
            let rows = shadow.observe(&shadow_view("0.01", "-0.2"), &s, &no_hist, at(2500), utc);
            assert_eq!(rows.len(), 1);
            let row = &rows[0];
            assert_eq!(
                (row.s.as_str(), row.long, row.short),
                ("NVDA", Venue::Arcus, Venue::LighterRh)
            );
            assert_eq!(row.lasted_ms, Some(800));
            assert!(row.slow.is_none(), "现行规则不会开：{row:?}");
            let quick = row.quick.as_ref().unwrap();
            assert_eq!((quick.at_ms, quick.fill_ms), (600, Some(1500)));
            assert_eq!((quick.entry, quick.net), (Some(0.3), Some(0.01)));
            assert_eq!(quick.tp, 0.025);
            assert_eq!((quick.exit, quick.exit_net), (Some("tp"), Some(0.055)));
            // 平仓每秒估一次：1.9 秒那次不估（上次在 1.0 秒），2.5 秒第一次估就到了止盈。
            assert_eq!(quick.worst, Some(0.055));
            // 写过就不再写。
            assert!(shadow.observe(&off, &s, &no_hist, at(4000), utc).is_empty());
        }
    }

    /// 撑过 5 秒的：两种规则都开，现行规则那笔在 5 秒触发。
    #[test]
    fn hold_shadow_compares_both_rules_on_a_signal_that_held() {
        let s = shadow_settings();
        let mut shadow = hold_shadow::Shadow::new(None);
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let utc = Utc::now();
        let on = shadow_view("0.09", "-0.3");
        for ms in (0..=6500).step_by(500) {
            assert!(shadow.observe(&on, &s, &no_hist, at(ms), utc).is_empty());
        }
        let off = shadow_view("0.01", "-0.3");
        assert!(shadow.observe(&off, &s, &no_hist, at(7000), utc).is_empty());
        assert!(shadow.observe(&off, &s, &no_hist, at(8000), utc).is_empty());
        let rows = shadow.observe(&shadow_view("0.01", "-0.2"), &s, &no_hist, at(9000), utc);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.lasted_ms, Some(6500));
        assert_eq!(row.quick.as_ref().unwrap().at_ms, 500);
        let slow = row.slow.as_ref().unwrap();
        assert_eq!((slow.at_ms, slow.fill_ms), (5000, Some(6000)));
        assert_eq!(slow.exit, Some("tp"));
    }

    fn no_hist(_: &HoldKey) -> Option<bool> {
        None
    }

    /// 一出现就是强信号（0 秒下）：两种规则一样，不记。
    /// 自动交易停看（开完一笔后隔 60 秒）：还在的信号按断了处理（标 `cut`），已触发的虚拟仓位照常跟到止盈。
    #[test]
    fn hold_shadow_skips_strong_signals_and_keeps_following_through_a_pause() {
        let strong = Settings {
            instant_multiple: Some(dec("2")),
            ..shadow_settings()
        };
        let mut shadow = hold_shadow::Shadow::new(None);
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let utc = Utc::now();
        let hot = shadow_view("0.2", "-0.3");
        for ms in (0..=3000).step_by(500) {
            assert!(
                shadow
                    .observe(&hot, &strong, &no_hist, at(ms), utc)
                    .is_empty()
            );
        }
        let off = shadow_view("0.01", "-0.3");
        for ms in (3500..=6000).step_by(500) {
            assert!(
                shadow
                    .observe(&off, &strong, &no_hist, at(ms), utc)
                    .is_empty()
            );
        }
        assert!(!shadow.has_open(), "强信号不记");
        // 弱信号撑过 0.5 秒、虚拟成交；历史分位结论 2 秒时才算出来（放行）。
        let on = shadow_view("0.09", "-0.3");
        let passed = |_: &HoldKey| Some(true);
        for ms in (10_000..=11_500).step_by(500) {
            assert!(
                shadow
                    .observe(&on, &strong, &no_hist, at(ms), utc)
                    .is_empty()
            );
        }
        assert!(
            shadow
                .observe(&on, &strong, &passed, at(12_000), utc)
                .is_empty()
        );
        // 停看 60 秒：期间只跟虚拟仓位，平仓价差还不够。
        assert!(shadow.follow_only(&on, at(40_000)).is_empty());
        assert!(shadow.has_open());
        // 恢复后信号按断了处理（还在也不算同一个），平仓价差到止盈就写。
        let back = shadow_view("0.09", "-0.2");
        let rows = shadow.observe(&back, &strong, &no_hist, at(72_000), utc);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert!(row.cut);
        assert_eq!(row.lasted_ms, Some(2000));
        assert!(row.slow.is_none());
        let quick = row.quick.as_ref().unwrap();
        assert_eq!((quick.at_ms, quick.fill_ms), (500, Some(1500)));
        assert_eq!((quick.exit, quick.exit_net), (Some("tp"), Some(0.055)));
        assert_eq!(quick.hist, Some(true));
        assert_eq!(quick.worst, Some(-0.045));
    }

    #[test]
    fn hold_timers_reset_when_a_line_stops_qualifying() {
        let s = settings();
        let mut since = HashMap::new();
        let mut seen = HashMap::new();
        let t0 = Instant::now();
        let good = view(vec![line(
            "NVDA",
            "long_a",
            leg("0.3", "0.2", "0.09"),
            -0.11,
        )]);
        track_holds(&good, &s, &mut since, &mut seen, t0);
        track_holds(
            &good,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_secs(5),
        );
        let key = ("NVDA".to_string(), Venue::Arcus, Venue::LighterRh);
        assert_eq!(since[&key], t0);
        let gone = view(vec![line(
            "NVDA",
            "long_a",
            leg("0.3", "0.2", "0.01"),
            -0.11,
        )]);
        // 掉下门槛不到 1 秒：快照每 100 毫秒重算一次，一次抖动不清零。
        track_holds(
            &gone,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_millis(5_400),
        );
        assert_eq!(since[&key], t0, "短暂抖动不算中断");
        track_holds(
            &good,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_millis(5_500),
        );
        assert_eq!(since[&key], t0);
        // 连续 1 秒都不达标：清零，再达标从头计时。
        track_holds(
            &gone,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_millis(5_800),
        );
        track_holds(
            &gone,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_millis(6_500),
        );
        assert!(since.is_empty());
        assert!(seen.is_empty());
        track_holds(
            &good,
            &s,
            &mut since,
            &mut seen,
            t0 + Duration::from_secs(7),
        );
        assert_eq!(since[&key], t0 + Duration::from_secs(7));
    }

    #[test]
    fn order_responses_are_classified_conservatively() {
        let ok = json!({ "position": { "id": "live-1", "status": "open" } });
        assert_eq!(
            classify(StatusCode::OK, &ok),
            Outcome::Opened("live-1".into())
        );
        let unwound = json!({ "position": { "id": "live-2", "status": "unwound" } });
        assert_eq!(
            classify(StatusCode::OK, &unwound),
            Outcome::Unwound("live-2".into())
        );
        let busy =
            json!({ "error": "这张交易台上有一笔操作（或一轮规则监控）正在进行，等它结束再试" });
        assert_eq!(classify(StatusCode::CONFLICT, &busy), Outcome::Busy);
        let dirty = json!({ "error": "对账不干净，拒绝开新仓" });
        assert_eq!(classify(StatusCode::CONFLICT, &dirty), Outcome::Rejected);
        assert_eq!(
            classify(StatusCode::UNPROCESSABLE_ENTITY, &json!({})),
            Outcome::Rejected
        );
        assert_eq!(classify(StatusCode::LOCKED, &json!({})), Outcome::Paused);
        assert_eq!(
            classify(StatusCode::SERVICE_UNAVAILABLE, &json!({})),
            Outcome::Paused
        );
        // 执行中断（可能留下一条腿）：结果未知，必须停下来。
        assert_eq!(
            classify(StatusCode::INTERNAL_SERVER_ERROR, &json!({})),
            Outcome::Interrupted
        );
        assert_eq!(
            classify(StatusCode::FORBIDDEN, &json!({})),
            Outcome::Misconfigured
        );
        assert_eq!(
            classify(StatusCode::BAD_REQUEST, &json!({})),
            Outcome::Misconfigured
        );
        // 当日盈亏算不出：等台账补上，不关自动交易。
        assert_eq!(
            classify(
                StatusCode::BAD_REQUEST,
                &json!({"error": "实盘必须填写当日已实现盈亏（亏损为负）：台账里今天结束的 3 笔仓位里有 1 笔没有盈亏记录（live-1），算不出合计，不按 0 算"})
            ),
            Outcome::Paused
        );
    }

    #[test]
    fn settings_need_an_exit_rule_sane_limits_and_live_confirmation() {
        let max = Some(Decimal::from(1000));
        assert!(settings().validate(max).is_ok());
        let bad = |s: Settings| s.validate(max).unwrap_err();
        assert!(
            bad(Settings {
                size_usdt: Decimal::from(2000),
                ..settings()
            })
            .contains("ARB_MAX_POSITION_USDT")
        );
        assert!(
            bad(Settings {
                back_to_normal: false,
                take_profit_usdt: None,
                ..settings()
            })
            .contains("退出规则")
        );
        assert!(
            bad(Settings {
                leverage: dec("2.5"),
                ..settings()
            })
            .contains("整数")
        );
        // 0 = 不限：可以保存。
        assert!(
            Settings {
                max_positions: 0,
                daily_max_opens: 0,
                ..settings()
            }
            .validate(max)
            .is_ok()
        );
        assert!(
            bad(Settings {
                max_positions: 1001,
                ..settings()
            })
            .contains("同时持仓")
        );
        assert!(
            bad(Settings {
                daily_max_opens: 10_001,
                ..settings()
            })
            .contains("每日开仓")
        );
        assert!(
            bad(Settings {
                symbols: vec!["NV DA".into()],
                ..settings()
            })
            .contains("字母数字")
        );

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let auto = AutoTrader::in_memory(Settings::default());
            let live = Settings {
                mode: Mode::Live,
                ..settings()
            };
            let err = auto.update(live.clone(), max, false).await.unwrap_err();
            assert!(err.contains("LIVE"));
            assert!(!auto.settings().await.enabled, "没确认就不开");
            auto.update(live.clone(), max, true).await.unwrap();
            assert!(auto.settings().await.enabled);
            // 已经开着实盘，改参数不必再确认。
            auto.update(
                Settings {
                    size_usdt: Decimal::from(800),
                    ..live
                },
                max,
                false,
            )
            .await
            .unwrap();
            auto.disable("测试".into()).await;
            let snapshot = auto.snapshot(Some(&[])).await;
            assert_eq!(snapshot["settings"]["enabled"], json!(false));
            assert_eq!(snapshot["disabled_reason"], json!("测试"));
        });
    }

    #[test]
    fn auto_orders_carry_the_rules_and_go_through_the_confirmed_spread_path() {
        let rules = arb_exec::TaskRules {
            basis_exit_pct: Some(dec("0.110")),
            take_profit_usdt: Some(dec("1.50")),
            ..arb_exec::TaskRules::default()
        };
        let body = crate::trade::auto_open_body(
            Mode::Live,
            "NVDA/USDT",
            Venue::Arcus,
            Venue::LighterRh,
            Decimal::from(500),
            Decimal::from(3),
            arb_exec::MarginMode::Isolated,
            &rules,
        );
        let text = format!("{body:?}");
        for part in [
            "Live",
            "NVDA/USDT",
            "\"arcus\"",
            "\"lighter-rh\"",
            "\"spread\"",
            "\"0.11\"",
            "\"1.5\"",
            "confirm: Some(\"NVDA\")",
            "daily_pnl: None",
        ] {
            assert!(text.contains(part), "{part} missing in {text}");
        }
    }

    /// 第一腿下单前的实时复核也按自动交易的入场门槛核：保本线过了，往返净边际或薄盘口门槛不够也不下单。
    #[test]
    fn the_live_entry_check_also_holds_the_auto_trade_entry_gate() {
        use crate::trade::{EntryGate, live_entry_check_with};
        let now = Utc::now();
        // 现在可成交价差 0.25%，往返手续费 0.045%。
        let fresh = view(vec![line(
            "USO",
            "long_a",
            leg("0.25", "0.1", "0.08"),
            -0.1,
        )]);
        let size = Decimal::from(800);
        let check = |gate: Option<EntryGate>| {
            live_entry_check_with(
                &fresh,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0.0024"),
                size,
                now,
                gate,
                None,
            )
        };
        assert_eq!(check(None), Ok(Some(dec("0.25"))));
        // 0.25 − 0.045 = 0.205 ≥ 0.20：过。
        let edge = |pct: &str| EntryGate {
            min_edge_pct: Some(dec(pct)),
            thin_min_entry_pct: None,
        };
        assert_eq!(check(Some(edge("0.20"))), Ok(Some(dec("0.25"))));
        // 0.205 < 0.21：保本线过了也不下单。
        let refused = check(Some(edge("0.21"))).unwrap_err();
        assert!(
            refused.contains("往返净边际门槛 0.21%") && refused.contains("0.205%"),
            "{refused}"
        );
        // 薄盘口门槛 0.30% > 0.25%：不下单。
        let thin = EntryGate {
            min_edge_pct: Some(dec("0.10")),
            thin_min_entry_pct: Some(dec("0.30")),
        };
        let refused = check(Some(thin)).unwrap_err();
        assert!(refused.contains("薄盘口门槛 0.3%"), "{refused}");
        // 价差监控的盘口旧了（只推变化的交易所推送断了）：按刚现拉的盘口算的价差更差，就按它核。
        let planned = |pct: &str| {
            live_entry_check_with(
                &fresh,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0.0024"),
                size,
                now,
                Some(edge("0.20")),
                Some(dec(pct)),
            )
        };
        let refused = planned("0.2").unwrap_err();
        assert!(
            refused.contains("按刚现拉的盘口算可成交价差 0.2%") && refused.contains("0.155%"),
            "{refused}"
        );
        // 现拉的更好：照旧按价差监控的核，返回的也是价差监控的数。
        assert_eq!(planned("0.4"), Ok(Some(dec("0.25"))));
    }

    /// 自动交易带上的门槛和挑信号时用的同一组设置：薄盘口门槛只在两腿里有薄盘口交易所时带；「相对正常基差」不带。
    #[test]
    fn auto_trade_attaches_its_entry_gate_to_the_order() {
        use crate::trade::EntryGate;
        let settings = Settings {
            min_edge_pct: Some(dec("0.10")),
            thin_min_entry_pct: Some(dec("0.30")),
            thin_venues: vec![Venue::Bitget],
            ..Settings::default()
        };
        assert_eq!(
            settings.entry_gate(Venue::Lighter, Venue::HyperliquidXyz),
            Some(EntryGate {
                min_edge_pct: Some(dec("0.10")),
                thin_min_entry_pct: None,
            })
        );
        assert_eq!(
            settings.entry_gate(Venue::Bitget, Venue::HyperliquidXyz),
            Some(EntryGate {
                min_edge_pct: Some(dec("0.10")),
                thin_min_entry_pct: Some(dec("0.30")),
            })
        );
        let relative = Settings {
            relative_to_normal: true,
            ..settings.clone()
        };
        assert_eq!(relative.entry_gate(Venue::Bitget, Venue::Lighter), None);
        let off = Settings {
            min_edge_pct: None,
            thin_min_entry_pct: None,
            ..settings
        };
        assert_eq!(off.entry_gate(Venue::Bitget, Venue::Lighter), None);
        // 开仓请求上真的带着（两腿都吃单的单子）；币安挂单的不带。
        let candidate = candidate_of(
            &line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11),
            &self::settings(),
        )
        .unwrap();
        let with_gate = Settings {
            min_edge_pct: Some(dec("0.10")),
            ..self::settings()
        };
        assert_eq!(
            AutoTrader::open_body(&with_gate, &candidate).entry_gate(),
            Some(EntryGate {
                min_edge_pct: Some(dec("0.10")),
                thin_min_entry_pct: None,
            })
        );
        let maker = Candidate {
            maker: Some(arb_exec::desk::MakerLeg {
                venue: Venue::Binance,
                entry_fee: Decimal::ZERO,
                exit_fee: dec("0.0004"),
                rest_ms: 8_000,
            }),
            ..candidate
        };
        assert_eq!(AutoTrader::open_body(&with_gate, &maker).entry_gate(), None);
    }

    #[test]
    fn the_live_entry_check_compares_the_monitor_spread_with_the_hedge_floor_in_the_same_units() {
        use crate::trade::live_entry_check;
        let now = Utc::now();
        // 多 Arcus / 空 RH（long_a）现在两腿可成交价差 0.25%（百分数）。
        let fresh = view(vec![line(
            "USO",
            "long_a",
            leg("0.25", "0.1", "0.08"),
            -0.1,
        )]);
        let size = Decimal::from(800);
        // 保本线是小数：0.0024 = 0.24% < 0.25% → 通过，返回现在的价差。
        assert_eq!(
            live_entry_check(
                &fresh,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0.0024"),
                size,
                now
            ),
            Ok(Some(dec("0.25")))
        );
        // 0.0026 = 0.26% > 0.25% → 不下单，原因写清两个数。
        let refused = live_entry_check(
            &fresh,
            "uso",
            Venue::Arcus,
            Venue::LighterRh,
            dec("0.0026"),
            size,
            now,
        )
        .unwrap_err();
        assert!(
            refused.contains("0.25%") && refused.contains("0.26%"),
            "{refused}"
        );
        // 反方向（多 RH / 空 Arcus = long_b）这一行没有报价：不下单。
        assert!(
            live_entry_check(
                &fresh,
                "USO",
                Venue::LighterRh,
                Venue::Arcus,
                dec("0.0001"),
                size,
                now
            )
            .is_err()
        );
        // 没这一行、行带备注、行情断了、快照旧了：都不下单。
        assert!(
            live_entry_check(
                &fresh,
                "SPY",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0"),
                size,
                now
            )
            .is_err()
        );
        let mut noted = fresh.clone();
        Arc::make_mut(&mut noted.lines[0]).note = Some("Arcus 盘口 20 秒没更新，不计算".into());
        assert!(
            live_entry_check(
                &noted,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0"),
                size,
                now
            )
            .is_err()
        );
        let mut down = fresh.clone();
        down.connected.venues.insert(Venue::LighterRh, false);
        assert!(
            live_entry_check(
                &down,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0"),
                size,
                now
            )
            .is_err()
        );
        let mut stale = fresh.clone();
        stale.updated_at = Some(now - chrono::Duration::seconds(5));
        let error = live_entry_check(
            &stale,
            "USO",
            Venue::Arcus,
            Venue::LighterRh,
            dec("0"),
            size,
            now,
        )
        .unwrap_err();
        assert!(error.contains("5.0 秒没更新"), "{error}");
        // 时钟回拨（快照时间在未来）也不放行；2.5 秒（整秒截断会当成 2 秒放过）不放行。
        let mut ahead = fresh.clone();
        ahead.updated_at = Some(now + chrono::Duration::seconds(3));
        assert!(
            live_entry_check(
                &ahead,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0"),
                size,
                now
            )
            .is_err()
        );
        let mut almost = fresh.clone();
        almost.updated_at = Some(now - chrono::Duration::milliseconds(2500));
        assert!(
            live_entry_check(
                &almost,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0"),
                size,
                now
            )
            .is_err()
        );
        // 监控按 2000 USDT 估；这笔 5000 USDT 时估得偏乐观：按能估的最大深度核，连它都不过就不下单
        // （以前整个跳过）；过了返回 `None`（没核全，调用方记日志）。
        assert!(
            live_entry_check(
                &fresh,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0.0026"),
                Decimal::from(5000),
                now
            )
            .is_err()
        );
        assert_eq!(
            live_entry_check(
                &fresh,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec("0.0024"),
                Decimal::from(5000),
                now
            ),
            Ok(None)
        );
    }

    #[test]
    fn a_bigger_order_than_the_monitor_size_is_checked_on_the_ladder_rung_that_covers_it() {
        use crate::trade::live_entry_check;
        let now = Utc::now();
        let mut small = view(vec![line(
            "USO",
            "long_a",
            leg("0.25", "0.1", "0.08"),
            -0.1,
        )]);
        small.size_usdt = Decimal::from(200);
        Arc::make_mut(&mut small.lines[0])
            .long_a
            .as_mut()
            .unwrap()
            .ladder = [("200", "0.25"), ("300", "0.22"), ("400", "0.18")]
            .map(|(size, entry)| rung(size, entry, "0.1", "0.1"))
            .to_vec();
        let check = |view: &crate::rh_spread::View, floor: &str, size: u32| {
            live_entry_check(
                view,
                "USO",
                Venue::Arcus,
                Venue::LighterRh,
                dec(floor),
                Decimal::from(size),
                now,
            )
        };
        // 350 USDT 按 400 那一档（0.18%）核，不是按 200 档的 0.25%。
        assert_eq!(check(&small, "0.0015", 350), Ok(Some(dec("0.18"))));
        let refused = check(&small, "0.0020", 350).unwrap_err();
        assert!(
            refused.contains("0.18%") && refused.contains("按 400 USDT 深度"),
            "{refused}"
        );
        // 刚好 300：用 300 那一档。不比估算名义大：照旧按这一行的报价。
        assert_eq!(check(&small, "0.0015", 300), Ok(Some(dec("0.22"))));
        assert_eq!(check(&small, "0.0015", 150), Ok(Some(dec("0.25"))));
        // 阶梯里没有这么大的档（或这一行没信号、没阶梯）：不判断。
        assert_eq!(check(&small, "0.0015", 600), Ok(None));
        Arc::make_mut(&mut small.lines[0])
            .long_a
            .as_mut()
            .unwrap()
            .ladder
            .clear();
        assert_eq!(check(&small, "0.0015", 350), Ok(None));
    }

    #[test]
    fn the_cheap_leg_goes_first_unless_the_panel_turns_it_off_and_old_settings_keep_it_on() {
        assert!(Settings::default().cheap_leg_first);
        // 旧的 auto.json 没有这个字段：照默认开。
        let mut value = serde_json::to_value(Settings::default()).unwrap();
        value.as_object_mut().unwrap().remove("cheap_leg_first");
        let old: Settings = serde_json::from_value(value).unwrap();
        assert!(old.cheap_leg_first);
        let off = Settings {
            cheap_leg_first: false,
            ..Settings::default()
        };
        assert!(describe(&off).contains("按滑点决定先下哪条腿"));
        assert!(!describe(&Settings::default()).contains("按滑点决定先下哪条腿"));
    }

    // ---- 币安：吃单 / 挂单自动交易的开关、名单、限额 ----

    use crate::rh_spread::maker::{MakerLeg as MonitorLeg, MakerView};

    /// 币安 ↔ Lighter RH 的一行（只观察的组）：币安腿挂单、对面吃单，两个方向的挂单测算由参数给。
    fn binance_line(base: &str, long_a_net: &str, long_b_net: &str) -> Line {
        let monitor_leg = |net: &str| MonitorLeg {
            price: dec("100"),
            entry_pct: dec("-2.40"),
            exit_cross_taker_pct: dec("0.02"),
            exit_cross_maker_pct: dec("-0.01"),
            net_to_normal_taker_exit_pct: Some(dec(net)),
            net_to_normal_maker_exit_pct: Some(dec(net)),
        };
        let mut line = line(base, "long_a", leg("-2.40", "-2.45", "0.50"), 2.5);
        line.pair = format!("{}:{}", Venue::LighterRh, Venue::Binance);
        line.a = Venue::LighterRh;
        line.b = Venue::Binance;
        line.observe_only = true;
        line.maker = Some(MakerView {
            venue: Venue::Binance,
            maker_fee: Decimal::ZERO,
            taker_fee: dec("0.0004"),
            fee_taker_exit_pct: dec("0.04"),
            fee_maker_exit_pct: Decimal::ZERO,
            long_a: Some(monitor_leg(long_a_net)),
            long_b: Some(monitor_leg(long_b_net)),
        });
        line
    }

    fn binance_settings() -> Settings {
        Settings {
            enabled: true,
            mode: Mode::Live,
            binance_maker: true,
            binance_symbols: vec!["AMD".into()],
            binance_stop_loss_usdt: Some(dec("0.5")),
            ..Settings::default()
        }
    }

    /// 近 12 小时两个方向可成交价差的中位数。
    fn recent(long_a: f64, long_b: f64) -> Option<crate::rh_spread::RecentEntry> {
        Some(crate::rh_spread::RecentEntry {
            hours: 12,
            long_a_pct: Some(long_a),
            long_b_pct: Some(long_b),
        })
    }

    /// 某次实盘：可成交价差 2.655%，近 12 小时中位 2.648%，7 天中位还停在 1.72% —— 「回到正常」看着能赚
    /// 0.77%，其实价差整体换了水平。相对近期只高 0.007%，不到 0.05%：不开。
    #[test]
    fn a_signal_sitting_on_the_recent_level_is_not_a_deviation() {
        let mut line = line("AMD", "long_a", leg("2.655", "2.60", "0.767"), -1.72);
        line.recent_entry = recent(2.648, -1.70);
        let s = settings();
        assert_eq!(s.recent_gate_pct, Some(dec("0.05")), "默认开着，0.05%");
        assert!(candidate_of(&line, &s).is_none());
        let why = auto_skip_reason(&line, line.long_a.as_ref().unwrap(), &s).unwrap();
        assert!(
            why.contains("近 12 小时中位 2.648%") && why.contains("整体换了水平"),
            "{why}"
        );
        // 门槛关掉：照旧。
        let off = Settings {
            recent_gate_pct: None,
            ..settings()
        };
        assert!(candidate_of(&line, &off).is_some());
        // 高出近期 0.05% 以上：是偏离，做；候选里带上近期中位，开仓日志用。
        line.long_a = Some(leg("2.75", "2.70", "0.86"));
        let candidate = candidate_of(&line, &s).unwrap();
        assert_eq!(candidate.recent_entry_pct, Some(dec("2.648")));
        // 近期样本不够（新合约、刚断过数据）：放行。
        line.long_a = Some(leg("2.655", "2.60", "0.767"));
        line.recent_entry = None;
        assert!(candidate_of(&line, &s).is_some());
        // 只看这个方向自己的中位数：另一个方向的中位数不算。
        line.recent_entry = Some(crate::rh_spread::RecentEntry {
            hours: 12,
            long_a_pct: None,
            long_b_pct: Some(-1.70),
        });
        assert!(candidate_of(&line, &s).is_some());
        // 日志用的「被这层挡下的」：只列门槛关掉时本来会做的。
        line.recent_entry = recent(2.648, -1.70);
        let blocked = recent_gate_blocked(&view(vec![line.clone()]), &s);
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].0, "AMD");
        assert!(recent_gate_blocked(&view(vec![line]), &off).is_empty());
    }

    /// 补仓同样要相对近期偏高：价差一路走宽时，比原仓位好不等于是偏离。
    #[test]
    fn an_add_also_needs_the_signal_to_stand_out_from_the_recent_level() {
        let mut line = line("HOOD", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        line.recent_entry = recent(0.29, -0.40);
        let v = view(vec![line.clone()]);
        let now = Utc::now();
        let targets = [target("HOOD", "0.158", "800", 0)];
        let err = pick_add(
            &v,
            &adding(),
            now,
            &|_| true,
            &|_| 10,
            &|_| None,
            &targets,
            None,
        )
        .unwrap_err();
        assert!(!err.is_empty());
        line.recent_entry = recent(0.20, -0.40);
        let v = view(vec![line]);
        assert!(
            pick_add(
                &v,
                &adding(),
                now,
                &|_| true,
                &|_| 10,
                &|_| None,
                &targets,
                None
            )
            .is_ok()
        );
    }

    /// 币安挂单的候选也过这一关（按挂单价算的开仓价差比近期中位高多少）。
    #[test]
    fn a_binance_maker_signal_also_needs_to_stand_out_from_the_recent_level() {
        let mut line = binance_line("AMD", "0.20", "0.01");
        let s = binance_settings();
        assert!(candidate_of(&line, &s).is_some());
        // 挂单价下开仓价差 −2.40%，近期中位 −2.42%：只高 0.02%。
        line.recent_entry = recent(-2.42, 2.30);
        assert!(candidate_of(&line, &s).is_none());
        line.recent_entry = recent(-2.50, 2.30);
        assert!(candidate_of(&line, &s).is_some());
    }

    #[test]
    fn the_funding_cost_of_the_holding_period_is_only_ever_a_cost() {
        // 某次实盘：多 hyperliquid-io 每小时 0.00065256%，空 lighter-rh 0：24 小时约 0.0157%。
        let cost = funding_cost_pct(dec("0.0000065256"), Decimal::ZERO, Decimal::from(24));
        assert_eq!(cost, dec("0.01566"));
        // 多腿费率为负（多头收钱）、空腿费率为正（空头也收钱）：两边都是收入，记 0，不拿收入凑门槛。
        assert_eq!(
            funding_cost_pct(dec("-0.00001"), dec("0.00001"), Decimal::from(10)),
            Decimal::ZERO
        );
        // 反过来两边都付：多头付 0.001%/h、空头付 0.001%/h，10 小时 0.02%。
        assert_eq!(
            funding_cost_pct(dec("0.00001"), dec("-0.00001"), Decimal::from(10)),
            dec("0.02")
        );
        // 持仓期：设了超时按超时，没设按一天。
        assert_eq!(funding_hold_hours(&settings()), Decimal::from(24));
        let timed = Settings {
            timeout_hours: Some(Decimal::from(6)),
            ..settings()
        };
        assert_eq!(funding_hold_hours(&timed), Decimal::from(6));
    }

    /// 扣资金费的缓存：结果与逐行现扣一样；源行没换就复用上次扣好的那一行，换了才重扣；旧行会清掉。
    #[test]
    fn the_funding_cache_reuses_rows_whose_source_did_not_change() {
        let hood = || {
            let mut line = line("HOOD", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
            line.long_b = Some(leg("0.25", "0.15", "0.08"));
            line
        };
        let source = view(vec![
            hood(),
            line("NVDA", "long_a", leg("0.4", "0.3", "0.2"), -0.2),
        ]);
        let rates: FundingRates = [
            (Venue::Arcus, [("HOOD".to_string(), dec("0.0001"))].into()),
            (
                Venue::LighterRh,
                [("HOOD".to_string(), Decimal::ZERO)].into(),
            ),
        ]
        .into();
        let mut expected = source.clone();
        deduct_funding(
            &mut expected,
            &|venue, base| funding_rate(&rates, venue, base),
            Decimal::from(24),
        );
        let json = |v: &View| serde_json::to_value(&v.lines).unwrap();
        let mut cache = FundingCache {
            rates,
            hours: Decimal::from(24),
            ..FundingCache::default()
        };
        let mut first = source.clone();
        cache.deduct(&mut first);
        assert_eq!(json(&first), json(&expected));
        assert_eq!(first.lines[0].best.as_ref().unwrap().direction, "long_b");
        // 源快照没动：同一批扣好的行（指针相同），源行本身没被改。
        let mut second = source.clone();
        cache.deduct(&mut second);
        assert!(Arc::ptr_eq(&first.lines[0], &second.lines[0]));
        assert!(Arc::ptr_eq(&first.lines[1], &second.lines[1]));
        assert_eq!(
            source.lines[0].long_a.as_ref().unwrap().funding_cost_pct,
            None
        );
        // 只换了 HOOD 那一行（盘口变了）：它重扣，NVDA 还是原来那行。
        let mut moved = source.clone();
        let mut changed = hood();
        changed.long_a = Some(leg("0.50", "0.40", "0.29"));
        moved.lines[0] = Arc::new(changed);
        let mut third = moved.clone();
        cache.deduct(&mut third);
        assert!(!Arc::ptr_eq(&first.lines[0], &third.lines[0]));
        assert!(Arc::ptr_eq(&first.lines[1], &third.lines[1]));
        assert_eq!(
            third.lines[0].long_a.as_ref().unwrap().net_to_normal_pct,
            Some(dec("0.05"))
        );
        // 换下来的旧行攒多了会清掉，不无限长。
        for _ in 0..200 {
            let mut next = source.clone();
            next.lines[0] = Arc::new(hood());
            cache.deduct(&mut next);
        }
        assert!(cache.lines.len() <= source.lines.len() * 2 + 65);
        // 没有费率表（没有扫描快照 / 太旧）：不扣，行原样。
        let mut none = FundingCache::default();
        let mut untouched = source.clone();
        none.apply(&mut untouched, None, Decimal::from(24));
        assert!(Arc::ptr_eq(&untouched.lines[0], &source.lines[0]));
    }

    /// 先筛出「值得细看」的行再过规则，和整份快照过规则，每一步的结论都一样（含扣资金费后换方向的行）。
    #[test]
    fn looking_only_at_rows_near_the_threshold_gives_the_same_answers_as_the_whole_snapshot() {
        let base_settings = settings();
        let min = base_settings.min_net_pct;
        let nets = [
            dec("-0.5"),
            Decimal::ZERO,
            min - dec("0.01"),
            min,
            min + dec("0.03"),
            min + dec("0.3"),
            dec("0.9"),
        ];
        let mut lines = Vec::new();
        for (i, net) in nets.iter().enumerate() {
            for (j, other) in nets.iter().enumerate() {
                let mut l = line(
                    &format!("S{i}{j}"),
                    if net >= other { "long_a" } else { "long_b" },
                    leg("0.60", "0.40", &net.to_string()),
                    -0.11,
                );
                l.long_b = Some(leg("0.55", "0.35", &other.to_string()));
                // 有的行带着提醒信号（哪怕不到自动交易的门槛），有的最近价格在动。
                l.best.as_mut().unwrap().signal = (i + j) % 3 == 0;
                l.recent_move = Some(crate::rh_spread::RecentMove {
                    a_pct: if (i + j) % 4 == 1 {
                        dec("0.4")
                    } else {
                        Decimal::ZERO
                    },
                    b_pct: Decimal::ZERO,
                });
                lines.push(l);
            }
        }
        let mut blank = line("NOBEST", "long_a", leg("0.6", "0.4", "0.9"), -0.11);
        blank.best = None;
        lines.push(blank);
        let source = view(lines);
        // Arcus 每小时付 0.0001：多 Arcus 的方向 24 小时要扣 0.24%，不少行因此换方向 / 掉到门槛下。
        let rates: FundingRates = [
            (Venue::Arcus, HashMap::new()),
            (Venue::LighterRh, HashMap::new()),
        ]
        .into();
        let hourly = |venue: Venue, _: &str| {
            rates.get(&venue).map(|_| {
                if venue == Venue::Arcus {
                    dec("0.0001")
                } else {
                    Decimal::ZERO
                }
            })
        };
        for s in [
            base_settings.clone(),
            Settings {
                slippage_buffer_pct: dec("0.05"),
                ..base_settings.clone()
            },
            Settings {
                fast_move_pct: Some(dec("0.2")),
                recent_gate_pct: Some(dec("0.05")),
                ..base_settings.clone()
            },
        ] {
            for deduct in [false, true] {
                let finish = |mut v: View| {
                    if deduct {
                        deduct_funding(&mut v, &hourly, Decimal::from(24));
                    }
                    v
                };
                let whole = finish(source.clone());
                let hot = finish(View {
                    lines: source
                        .lines
                        .iter()
                        .filter(|line| worth_a_look(line, &s))
                        .cloned()
                        .collect(),
                    ..source.clone()
                });
                assert!(hot.lines.len() < whole.lines.len());
                let candidates = |v: &View| -> Vec<HoldKey> {
                    v.lines
                        .iter()
                        .filter_map(|line| candidate_of(line, &s).map(|c| c.hold_key()))
                        .collect()
                };
                assert_eq!(candidates(&hot), candidates(&whole));
                // 休市 / 开盘保护 / 维护时段里本来就没有候选（看的是真实时钟），那时只比两边一致。
                if venue_closed_reason(&source.lines[0], Utc::now()).is_none() {
                    assert!(!candidates(&whole).is_empty());
                }
                assert_eq!(
                    recent_gate_blocked(&hot, &s),
                    recent_gate_blocked(&whole, &s)
                );
                assert_eq!(fast_move_blocked(&hot, &s), fast_move_blocked(&whole, &s));
                assert_eq!(
                    blocked_signal_reason(&hot, &s),
                    blocked_signal_reason(&whole, &s)
                );
                let signals = |v: &View| -> Vec<(String, Option<String>)> {
                    v.lines
                        .iter()
                        .filter_map(|line| {
                            let best = line.best.as_ref().filter(|best| best.signal)?;
                            Some((line.base.clone(), rule_block(line, best.direction, &s)))
                        })
                        .collect()
                };
                assert_eq!(signals(&hot), signals(&whole));
                // 挑开仓、挑补仓：保持够了 / 还在保持，结果（选中的那一笔或不选的原因）都一样。
                for held in [0u64, u64::MAX] {
                    let pick = |v: &View| {
                        format!(
                            "{:?}",
                            pick_checked(v, &s, Utc::now(), &|_| true, &|_| held, &|_| None)
                        )
                    };
                    assert_eq!(pick(&hot), pick(&whole));
                    let targets: Vec<AddTarget> = whole
                        .lines
                        .iter()
                        .filter_map(|line| candidate_of(line, &s))
                        .map(|c| AddTarget {
                            id: format!("p-{}", c.base),
                            base: c.base.clone(),
                            long: c.long,
                            short: c.short,
                            entry_spread_pct: c.entry_pct - dec("0.5"),
                            notional_usdt: Decimal::from(400),
                            adds: 0,
                        })
                        .collect();
                    let add_settings = Settings {
                        add_min_improve_pct: Some(dec("0.2")),
                        ..s.clone()
                    };
                    let pick_add = |v: &View| {
                        format!(
                            "{:?}",
                            pick_add_checked(
                                v,
                                &add_settings,
                                Utc::now(),
                                &|_| true,
                                &|_| held,
                                &|_| None,
                                &targets,
                                None,
                            )
                        )
                    };
                    assert_eq!(pick_add(&hot), pick_add(&whole));
                }
            }
        }
    }

    /// `candidate_of` 先用一次比较挡掉不到门槛的行：被它挡掉的，原来的完整判断也一定给得出「不做」的原因。
    #[test]
    fn the_cheap_threshold_check_never_drops_a_row_the_full_rules_would_take() {
        let s = settings();
        for net in ["-0.5", "0", "0.05", "0.1", "0.2", "0.5"] {
            let line = line("HOOD", "long_a", leg("0.30", "0.20", net), -0.11);
            let leg = line.long_a.as_ref().unwrap();
            if below_threshold(&line, &s) {
                assert!(auto_skip_reason(&line, leg, &s).is_some(), "{net}");
                assert!(candidate_of(&line, &s).is_none(), "{net}");
            } else {
                assert_eq!(
                    candidate_of(&line, &s).is_some(),
                    auto_skip_reason(&line, leg, &s).is_none(),
                    "{net}"
                );
            }
        }
        // 滑点缓冲算在门槛里。
        let buffered = Settings {
            slippage_buffer_pct: dec("0.05"),
            ..settings()
        };
        let edge = line(
            "HOOD",
            "long_a",
            leg(
                "0.30",
                "0.20",
                &(buffered.min_net_pct + dec("0.04")).to_string(),
            ),
            -0.11,
        );
        assert!(below_threshold(&edge, &buffered));
        assert!(candidate_of(&edge, &buffered).is_none());
        // 没有更好的方向 / 这个方向没有报价：一样挡。
        let mut blank = edge.clone();
        blank.best = None;
        assert!(below_threshold(&blank, &settings()));
    }

    #[test]
    fn funding_is_deducted_per_direction_before_the_threshold_and_can_flip_the_better_side() {
        // 多 Arcus / 空 RH 回到正常净赚 0.09%；反方向 0.08%。
        let mut line = line("HOOD", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        line.long_b = Some(leg("0.25", "0.15", "0.08"));
        let mut v = view(vec![line]);
        // Arcus 每小时 0.0001（多头付），RH 0：多 Arcus 的方向 24 小时要付 0.24%，反方向是收入（记 0）。
        let rates = |venue: Venue, base: &str| {
            assert_eq!(base, "HOOD");
            Some(if venue == Venue::Arcus {
                dec("0.0001")
            } else {
                Decimal::ZERO
            })
        };
        deduct_funding(&mut v, &rates, Decimal::from(24));
        let line = &v.lines[0];
        let long_a = line.long_a.as_ref().unwrap();
        assert_eq!(long_a.funding_cost_pct, Some(dec("0.24")));
        assert_eq!(long_a.net_to_normal_pct, Some(dec("-0.15")));
        let long_b = line.long_b.as_ref().unwrap();
        assert_eq!(long_b.funding_cost_pct, Some(Decimal::ZERO));
        assert_eq!(long_b.net_to_normal_pct, Some(dec("0.08")));
        // 扣完反方向更好：选它；候选里带上扣掉的资金费。
        assert_eq!(line.best.as_ref().unwrap().direction, "long_b");
        let candidate = candidate_of(line, &settings()).unwrap();
        assert_eq!(candidate.long, Venue::LighterRh);
        assert_eq!(candidate.funding_cost_pct, Some(Decimal::ZERO));
        // 门槛按扣后的数比：0.24% 的资金费把 0.09% 的收益吃没了，这个方向不做。
        let only_a = {
            let mut line = Line::clone(&v.lines[0]);
            line.long_b = None;
            line.best.as_mut().unwrap().direction = "long_a";
            line
        };
        assert!(candidate_of(&only_a, &settings()).is_none());

        // 任何一家拿不到费率：这个方向不扣。
        let mut v = view(vec![self::line(
            "HOOD",
            "long_a",
            leg("0.30", "0.20", "0.09"),
            -0.11,
        )]);
        deduct_funding(
            &mut v,
            &|venue, _| (venue == Venue::Arcus).then(|| dec("0.0001")),
            Decimal::from(24),
        );
        let long_a = v.lines[0].long_a.as_ref().unwrap();
        assert_eq!(long_a.net_to_normal_pct, Some(dec("0.09")));
        assert_eq!(long_a.funding_cost_pct, None);
    }

    /// 10-22 00:00 UTC 某合约：挂单 8 秒，撤单正好碰上结算，查询落后 7 秒以上。
    /// 挂单整个寿命（挂着 8 秒 + 撤单读回 5 秒）碰到结算点前后 60 秒的，这一刻不挂。
    #[test]
    fn no_binance_maker_order_lives_across_a_funding_settlement() {
        use chrono::TimeZone;
        let at = |h: u32, m: u32, s: u32| Utc.with_ymd_and_hms(2026, 10, 22, h, m, s).unwrap();
        let rest = 8_000;
        for blocked in [
            at(23, 58, 48),
            at(23, 59, 30),
            at(0, 0, 0),
            at(0, 0, 59),
            at(4, 0, 30),
            at(19, 59, 0),
        ] {
            assert!(
                binance_settlement_reason(blocked, rest).is_some(),
                "{blocked}"
            );
        }
        for free in [
            at(23, 58, 46),
            at(0, 1, 0),
            at(2, 0, 0),
            at(3, 30, 0),
            at(8, 1, 0),
        ] {
            assert!(binance_settlement_reason(free, rest).is_none(), "{free}");
        }
        // 挂得越久，越早停：20 秒的挂单 23:58:40 就不挂了。
        assert!(binance_settlement_reason(at(23, 58, 40), 20_000).is_some());
        assert!(binance_settlement_reason(at(23, 58, 34), 20_000).is_none());

        let mut v = view(vec![binance_line("AMD", "0.20", "0.01")]);
        v.connected.venues.insert(Venue::Binance, true);
        let s = binance_settings();
        let all = |_: Venue| true;
        let free = |_: &str| None;
        let pick_at = |now| {
            let mut v = v.clone();
            v.updated_at = Some(now);
            pick(&v, &s, now, &all, &|_| u64::MAX, &free)
        };
        let why = pick_at(at(23, 59, 30)).unwrap_err();
        assert!(why.contains("AMD") && why.contains("结算点"), "{why}");
        assert!(pick_at(at(2, 0, 0)).unwrap().maker.is_some());
    }

    #[test]
    fn funding_is_also_deducted_from_the_binance_maker_estimate() {
        let mut v = view(vec![binance_line("AMD", "0.20", "0.01")]);
        // 多 Lighter RH（a）/ 空币安：RH 每小时 0.0001（多头付）、币安 0。
        deduct_funding(
            &mut v,
            &|venue, _| {
                Some(if venue == Venue::LighterRh {
                    dec("0.0001")
                } else {
                    Decimal::ZERO
                })
            },
            Decimal::from(24),
        );
        let maker = v.lines[0].maker.as_ref().unwrap();
        assert_eq!(
            maker.long_a.as_ref().unwrap().net_to_normal_taker_exit_pct,
            Some(dec("-0.04"))
        );
        assert_eq!(
            maker.long_b.as_ref().unwrap().net_to_normal_taker_exit_pct,
            Some(dec("0.01"))
        );
        assert!(candidate_of(&v.lines[0], &binance_settings()).is_none());
    }

    #[test]
    fn the_new_settings_default_on_for_saved_files_and_can_be_turned_off() {
        // 旧的 auto.json 没有这两项：读进来是门槛 0.05%、扣资金费。
        let mut saved = serde_json::to_value(settings()).unwrap();
        let object = saved.as_object_mut().unwrap();
        object.remove("recent_gate_pct");
        object.remove("deduct_funding");
        let loaded: Settings = serde_json::from_value(saved.clone()).unwrap();
        assert_eq!(loaded.recent_gate_pct, Some(dec("0.05")));
        assert!(loaded.deduct_funding);
        // 明确关掉的保持关着。
        let object = saved.as_object_mut().unwrap();
        object.insert("recent_gate_pct".into(), serde_json::Value::Null);
        object.insert("deduct_funding".into(), serde_json::Value::Bool(false));
        let off: Settings = serde_json::from_value(saved).unwrap();
        assert_eq!(off.recent_gate_pct, None);
        assert!(!off.deduct_funding);
        // 范围：0 到 1%。
        for (value, ok) in [
            ("0", true),
            ("0.05", true),
            ("1", true),
            ("-0.01", false),
            ("1.5", false),
        ] {
            let s = Settings {
                recent_gate_pct: Some(dec(value)),
                ..settings()
            };
            assert_eq!(s.validate(None).is_ok(), ok, "{value}");
        }
    }

    #[test]
    fn binance_lines_are_never_traded_unless_a_binance_switch_is_on_and_the_symbol_is_listed() {
        let line = binance_line("AMD", "0.20", "0.01");
        // 默认：什么都不做。
        assert!(candidate_of(&line, &Settings::default()).is_none());
        // 开了开关但名单是空的：还是什么都不做。
        let empty = Settings {
            binance_symbols: vec![],
            ..binance_settings()
        };
        assert!(candidate_of(&line, &empty).is_none());
        // 名单里没有这个合约。
        let other = binance_line("QQQ", "0.20", "0.01");
        assert!(candidate_of(&other, &binance_settings()).is_none());
        // 开了、名单里有：挂单候选。
        assert!(candidate_of(&line, &binance_settings()).is_some());
        // 两个开关都关着，名单再全也不做。
        let off = Settings {
            binance_maker: false,
            binance_taker: false,
            ..binance_settings()
        };
        assert!(candidate_of(&line, &off).is_none());
    }

    #[test]
    fn a_maker_candidate_takes_the_better_direction_and_carries_the_maker_terms() {
        let line = binance_line("AMD", "0.12", "0.30");
        let candidate = candidate_of(&line, &binance_settings()).unwrap();
        // long_b = 多 b（币安）/ 空 a（Lighter RH）：净收益 0.30 的那个方向。
        assert_eq!(
            (candidate.long, candidate.short),
            (Venue::Binance, Venue::LighterRh)
        );
        assert_eq!(candidate.net_to_normal_pct, dec("0.30"));
        assert!(candidate.binance);
        let maker = candidate.maker.expect("挂单");
        assert_eq!(maker.venue, Venue::Binance);
        assert_eq!(
            (maker.entry_fee, maker.exit_fee),
            (Decimal::ZERO, dec("0.0004"))
        );
        assert_eq!(maker.rest_ms, 8_000);
        // 目标 = 同时段正常基差换算到持仓口径（long_b：中位数本身）。
        assert_eq!(candidate.target_pct, Some(dec("2.5")));
        // 币安的单子用币安自己的名义和止损。
        let settings = binance_settings();
        assert_eq!(candidate.size(&settings), dec("50"));
        let rules = settings.rules_for(candidate.target_pct, &candidate);
        assert_eq!(rules.stop_loss_usdt, Some(dec("0.5")));
        // 通用的单子仍然用通用的。
        let plain = candidate_of(&line_for_plain(), &settings).unwrap();
        assert!(!plain.binance && plain.maker.is_none());
        assert_eq!(plain.size(&settings), settings.size_usdt);
    }

    fn line_for_plain() -> Line {
        line("SPY", "long_a", leg("0.2", "0.15", "0.2"), 0.0)
    }

    #[test]
    fn a_maker_signal_needs_the_maker_threshold_after_the_slippage_buffer() {
        // 默认挂单门槛 0.10%：0.09 不够，0.11 够；缓冲 0.05 后 0.11 不够。
        let settings = binance_settings();
        assert!(candidate_of(&binance_line("AMD", "0.09", "0.09"), &settings).is_none());
        assert!(candidate_of(&binance_line("AMD", "0.11", "0.09"), &settings).is_some());
        let buffered = Settings {
            slippage_buffer_pct: dec("0.05"),
            ..settings
        };
        assert!(candidate_of(&binance_line("AMD", "0.11", "0.09"), &buffered).is_none());
        assert!(candidate_of(&binance_line("AMD", "0.16", "0.09"), &buffered).is_some());
    }

    /// 观察组（没有实盘账户的场所，如 Gate）的行：信号再好、币安开关全开、名单里有这个合约，都不是候选。
    #[test]
    fn watch_lines_are_never_candidates_even_with_every_binance_switch_on() {
        let all_on = Settings {
            binance_maker: true,
            binance_taker: true,
            ..binance_settings()
        };
        let mut observed = binance_line("AMD", "0.50", "0.50");
        assert!(
            candidate_of(&observed, &all_on).is_some(),
            "对照：币安行在这些设置下是候选"
        );
        observed.watch_only = true;
        assert!(candidate_of(&observed, &all_on).is_none());
        // 普通行（不是「只观察」）同理。
        let mut plain = line("SOXL", "long_a", leg("0.30", "0.20", "0.15"), -0.11);
        assert!(candidate_of(&plain, &settings()).is_some(), "对照");
        plain.pair = format!("{}:{}", Venue::Arcus, Venue::Gate);
        plain.b = Venue::Gate;
        plain.watch_only = true;
        assert!(candidate_of(&plain, &settings()).is_none());
        assert!(candidate_of(&plain, &all_on).is_none());
    }

    #[test]
    fn without_a_maker_signal_the_taker_switch_trades_binance_like_any_other_pair_under_the_same_rules()
     {
        // 只开吃单：挂单测算再好也不看；吃单的硬条件（开仓价差为正）不满足就没有候选。
        let taker_only = Settings {
            binance_maker: false,
            binance_taker: true,
            ..binance_settings()
        };
        let line = binance_line("AMD", "0.50", "0.50");
        assert!(
            candidate_of(&line, &taker_only).is_none(),
            "开仓价差 −2.40% ≤ 0"
        );
        // 相对正常基差打开后，吃单的候选按币安名义算，没有挂单参数。
        let relative = Settings {
            relative_to_normal: true,
            stop_loss_usdt: Some(dec("5")),
            ..taker_only
        };
        let candidate = candidate_of(&line, &relative).unwrap();
        assert!(candidate.binance && candidate.maker.is_none());
        assert_eq!(candidate.size(&relative), dec("50"));
    }

    #[test]
    fn binance_settings_are_validated_and_old_files_load_with_everything_off() {
        let ok = binance_settings();
        assert!(ok.validate(None).is_ok(), "{:?}", ok.validate(None));
        // 老的 auto.json 没有这些字段：全部默认关。
        let mut value = serde_json::to_value(Settings::default()).unwrap();
        for key in [
            "binance_taker",
            "binance_maker",
            "binance_symbols",
            "binance_size_usdt",
            "binance_stop_loss_usdt",
            "binance_max_positions",
            "maker_rest_sec",
            "maker_min_net_pct",
        ] {
            value.as_object_mut().unwrap().remove(key);
        }
        let old: Settings = serde_json::from_value(value).unwrap();
        assert!(!old.binance_taker && !old.binance_maker && old.binance_symbols.is_empty());
        assert_eq!(old.binance_size_usdt, dec("50"));
        let bad = |change: fn(&mut Settings)| {
            let mut settings = binance_settings();
            change(&mut settings);
            settings.validate(None).unwrap_err()
        };
        assert!(bad(|s| s.binance_symbols.clear()).contains("名单"));
        // 挂单一定带止损（下单准备强制）：币安和通用都没设就不能开挂单。
        assert!(bad(|s| s.binance_stop_loss_usdt = None).contains("止损"));
        assert!(bad(|s| s.binance_size_usdt = dec("200000")).contains("单腿名义"));
        assert!(bad(|s| s.binance_size_usdt = dec("5")).contains("单腿名义"));
        assert!(bad(|s| s.binance_stop_loss_usdt = Some(dec("80"))).contains("止损"));
        assert!(bad(|s| s.maker_rest_sec = 3).contains("5 到 20"));
        assert!(bad(|s| s.maker_rest_sec = 60).contains("5 到 20"));
        assert!(bad(|s| s.mode = Mode::Paper).contains("只支持实盘"));
        // 全局的「回到正常基差平仓」关着也能开币安挂单：挂单候选自己带目标，别的组合的退出方式不受影响。
        let mut other_exits = binance_settings();
        other_exits.back_to_normal = false;
        other_exits.take_profit_usdt = Some(dec("1"));
        assert!(other_exits.validate(None).is_ok());
        let candidate = candidate_of(&binance_line("AMD", "0.5", "0.5"), &other_exits).unwrap();
        assert!(
            candidate.target_pct.is_some(),
            "挂单候选自带回到正常基差的目标"
        );
        assert!(
            other_exits
                .rules_for(candidate.target_pct, &candidate)
                .basis_exit_pct
                .is_some()
        );
        assert!(bad(|s| s.binance_max_positions = 0).contains("1 到 20"));
        assert!(bad(|s| s.binance_max_positions = 21).contains("1 到 20"));
        assert!(bad(|s| s.binance_symbols = vec!["A-B".into()]).contains("字母数字"));
        // 开关都关着时，名单和止损可以不填。
        let off = Settings::default();
        assert!(off.validate(None).is_ok());
    }

    /// 「币安的功能和现在的 perp dex 类似」：只开币安吃单时，币安的组合和别的组合走同一套规则 ——
    /// 通用的退出（止盈、没设的止损）、通用的单腿名义可以直接沿用，补仓也一样；不强制单独的止损。
    #[test]
    fn binance_taker_alone_follows_the_general_rules_like_every_other_pair() {
        let general = Settings {
            enabled: true,
            mode: Mode::Live,
            size_usdt: dec("800"),
            take_profit_usdt: Some(dec("0.6")),
            take_profit_pct: Some(dec("0.05")),
            back_to_normal: false,
            relative_to_normal: false,
            stop_loss_usdt: None,
            add_min_improve_pct: Some(dec("0.2")),
            binance_taker: true,
            binance_symbols: vec!["AMD".into(), "NFLX".into()],
            binance_size_usdt: dec("800"),
            binance_stop_loss_usdt: None,
            binance_max_positions: 2,
            ..Settings::default()
        };
        // 不设止损、不开回到正常基差平仓，也能存：吃单走的是通用规则。
        assert!(
            general.validate(None).is_ok(),
            "{:?}",
            general.validate(None)
        );
        // 单腿名义不能超过单笔上限（币安也算）。
        assert!(
            general
                .validate(Some(dec("500")))
                .unwrap_err()
                .contains("ARB_MAX_POSITION_USDT")
        );
        // 币安的线有候选的方向和别的组合一样要满足「开仓价差为正、收敛到 0 净收益为正」：
        // 这里 long_b（多币安 / 空 Lighter RH）开仓价差为正，且回到正常净收益够门槛。
        let mut line = binance_line("AMD", "0.0", "0.0");
        line.long_b = Some(leg("2.40", "2.30", "0.20"));
        line.long_a = None;
        line.best = Some(Best {
            direction: "long_b",
            signal: false,
            signal_sec: 0,
            net_usdt: None,
        });
        let candidate = candidate_of(&line, &general).expect("币安吃单候选");
        assert!(candidate.binance && candidate.maker.is_none());
        assert_eq!(
            (candidate.long, candidate.short),
            (Venue::Binance, Venue::LighterRh)
        );
        assert_eq!(candidate.size(&general), dec("800"));
        // 规则：止损跟通用一致（都没设 = 没有），止盈按币安的名义算。
        let rules = general.rules_for(candidate.target_pct, &candidate);
        assert_eq!(rules.stop_loss_usdt, None);
        assert_eq!(
            rules.take_profit_usdt,
            general.take_profit_amount_for(dec("800"))
        );
        // 通用设了止损，币安没单独设：跟通用的。币安单独设了：用币安的。
        let with_stop = Settings {
            stop_loss_usdt: Some(dec("5")),
            ..general.clone()
        };
        assert_eq!(
            with_stop.rules_for(None, &candidate).stop_loss_usdt,
            Some(dec("5"))
        );
        let own_stop = Settings {
            binance_stop_loss_usdt: Some(dec("2")),
            ..with_stop
        };
        assert_eq!(
            own_stop.rules_for(None, &candidate).stop_loss_usdt,
            Some(dec("2"))
        );
        // 带币安腿：不设爆仓保护、自动加保证金（币安不公开维持保证金率，设了开仓校验会整笔拒掉）。
        let protected = Settings {
            liq_protection_pct: Some(dec("8")),
            auto_margin_pct: Some(dec("15")),
            auto_margin_max_usdt: Some(dec("100")),
            ..general.clone()
        };
        let rules = protected.rules_for(None, &candidate);
        assert_eq!(
            (
                rules.liq_protection_pct,
                rules.auto_margin_pct,
                rules.auto_margin_max_usdt
            ),
            (None, None, None)
        );
        assert_eq!(
            rules.take_profit_usdt,
            protected.take_profit_amount_for(dec("800"))
        );
        // 不带币安腿的照旧有。
        let other = Candidate {
            long: Venue::Arcus,
            binance: false,
            ..candidate.clone()
        };
        let rules = protected.rules_for(None, &other);
        assert_eq!(rules.liq_protection_pct, Some(dec("8")));
        assert_eq!(rules.auto_margin_pct, Some(dec("15")));
        // 补仓：币安吃单候选和别的组合一样可以补，上限按币安的名义算。
        let view = view(vec![line.clone()]);
        let mut with_venues = view.clone();
        with_venues.connected.venues.insert(Venue::Binance, true);
        let target = AddTarget {
            id: "p1".into(),
            base: "AMD".into(),
            long: Venue::Binance,
            short: Venue::LighterRh,
            entry_spread_pct: dec("1.0"),
            notional_usdt: dec("800"),
            adds: 0,
        };
        let picked = pick_add(
            &with_venues,
            &general,
            Utc::now(),
            &|_| true,
            &|_| 99,
            &|_| None,
            std::slice::from_ref(&target),
            None,
        );
        assert!(picked.is_ok(), "{picked:?}");
        // 挂单的候选不补仓。
        let both = Settings {
            binance_maker: true,
            binance_stop_loss_usdt: Some(dec("1")),
            ..general
        };
        let mut maker_line = binance_line("AMD", "0.5", "0.5");
        maker_line.long_b = Some(leg("2.40", "2.30", "0.20"));
        let mut maker_view = view.clone();
        maker_view.lines = vec![Arc::new(maker_line)];
        maker_view.connected.venues.insert(Venue::Binance, true);
        assert!(
            pick_add(
                &maker_view,
                &both,
                Utc::now(),
                &|_| true,
                &|_| 99,
                &|_| None,
                &[target],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn an_excluded_venue_keeps_its_pairs_out_of_auto_trading() {
        // 测试行是 arcus × lighter-rh。
        let plain = line_for_plain();
        let excluded = Settings {
            exclude_venues: vec![Venue::LighterRh],
            ..Settings::default()
        };
        assert!(candidate_of(&plain, &Settings::default()).is_some());
        assert!(
            candidate_of(&plain, &excluded).is_none(),
            "开仓、补仓都不做"
        );
        assert!(
            rule_block(&plain, "long_a", &excluded)
                .unwrap()
                .contains("lighter-rh 在自动交易的排除名单里")
        );
        // 别家在名单里：这一行照旧。
        let other = Settings {
            exclude_venues: vec![Venue::Bitget],
            ..Settings::default()
        };
        assert!(candidate_of(&plain, &other).is_some());
        assert!(describe(&other).contains("不做 bitget 的组合"));
        assert!(!describe(&Settings::default()).contains("不做"));
        // 面板存的是短名；老文件没有这个字段：不排除。
        let value = serde_json::to_value(&other).unwrap();
        assert_eq!(value["exclude_venues"], serde_json::json!(["bitget"]));
        let mut old = serde_json::to_value(Settings::default()).unwrap();
        old.as_object_mut().unwrap().remove("exclude_venues");
        assert!(
            serde_json::from_value::<Settings>(old)
                .unwrap()
                .exclude_venues
                .is_empty()
        );
        let mut unknown = value.clone();
        unknown["exclude_venues"] = serde_json::json!(["nope"]);
        assert!(serde_json::from_value::<Settings>(unknown).is_err());
    }

    #[test]
    fn an_excluded_symbol_is_out_of_auto_trading_on_every_pair() {
        let plain = line_for_plain();
        let base = plain.base.clone();
        let excluded = Settings {
            exclude_symbols: vec![base.to_ascii_lowercase()],
            ..Settings::default()
        };
        assert!(candidate_of(&plain, &Settings::default()).is_some());
        assert!(
            candidate_of(&plain, &excluded).is_none(),
            "开仓、补仓都不做"
        );
        assert!(
            rule_block(&plain, "long_a", &excluded)
                .unwrap()
                .contains("在不自动交易的合约名单里")
        );
        assert!(describe(&excluded).contains(&format!("不做 {}", base.to_ascii_lowercase())));
        // 别的合约在名单里：这一行照旧。
        let other = Settings {
            exclude_symbols: vec!["ZZZ".into()],
            ..Settings::default()
        };
        assert!(candidate_of(&plain, &other).is_some());
        // 旧设置文件没有这一项：按默认停 某合约、某合约（2026-11-04）；面板清空后存成空就是空。
        let mut old = serde_json::to_value(Settings::default()).unwrap();
        old.as_object_mut().unwrap().remove("exclude_symbols");
        assert_eq!(
            serde_json::from_value::<Settings>(old)
                .unwrap()
                .exclude_symbols,
            vec!["ANTHROPIC".to_string(), "OPENAI".to_string()]
        );
        let cleared = serde_json::to_value(Settings::default()).unwrap();
        assert!(
            serde_json::from_value::<Settings>(cleared)
                .unwrap()
                .exclude_symbols
                .is_empty()
        );
        let bad = Settings {
            exclude_symbols: vec!["A-B".into()],
            ..Settings::default()
        };
        assert!(bad.validate(None).is_err());
    }

    #[test]
    fn binance_only_blocks_every_other_pair_and_needs_a_binance_switch() {
        let only = Settings {
            binance_only: true,
            ..binance_settings()
        };
        assert!(
            candidate_of(&line_for_plain(), &only).is_none(),
            "别的组合不做"
        );
        assert!(candidate_of(&binance_line("AMD", "0.5", "0.5"), &only).is_some());
        // 没开只做币安：常规组合照旧。
        assert!(candidate_of(&line_for_plain(), &binance_settings()).is_some());
        let idle = Settings {
            binance_only: true,
            ..Settings::default()
        };
        assert!(idle.validate(None).unwrap_err().contains("只做币安"));
        assert!(!describe(&only).contains("币安"), "状态里不再单独列币安");
        // 老文件没有这个字段：关。
        let mut value = serde_json::to_value(Settings::default()).unwrap();
        value.as_object_mut().unwrap().remove("binance_only");
        assert!(
            !serde_json::from_value::<Settings>(value)
                .unwrap()
                .binance_only
        );
    }

    #[test]
    fn a_full_binance_book_switches_binance_off_for_this_round_only() {
        let settings = binance_settings();
        assert!(effective_settings(&settings, 0, 0).binance_maker);
        let capped = effective_settings(&settings, 1, 0);
        assert!(!capped.binance_maker && !capped.binance_taker);
        // 今天挂单次数用完：只关挂单；吃单开关（如果开着）不受影响。
        let both = Settings {
            binance_taker: true,
            ..binance_settings()
        };
        let spent = effective_settings(&both, 0, MAX_MAKER_ATTEMPTS_PER_DAY);
        assert!(!spent.binance_maker && spent.binance_taker);
        assert!(effective_settings(&both, 0, MAX_MAKER_ATTEMPTS_PER_DAY - 1).binance_maker);
        // 币安之外的组合不受影响。
        assert!(candidate_of(&line_for_plain(), &capped).is_some());
        assert!(candidate_of(&binance_line("AMD", "0.5", "0.5"), &capped).is_none());
    }

    #[test]
    fn an_attempt_that_never_traded_or_a_maker_that_only_left_dust_is_not_a_failure_but_a_rollback_is()
     {
        let settings = binance_settings();
        let maker = candidate_of(&binance_line("AMD", "0.5", "0.5"), &settings).unwrap();
        let plain = candidate_of(&line_for_plain(), &settings).unwrap();
        // 回滚之后仓位的腿是空的：靠 `nothing_traded`（订单成交记录）区分，不能靠腿。
        let unfilled = json!({"nothing_traded": true, "position": {"id": "p1", "status": "unwound", "long": null, "short": null}});
        let rolled_back = json!({"nothing_traded": false, "position": {"id": "p2", "status": "unwound", "long": null, "short": null, "note": "建仓未完成，已成交腿已全部回滚"}});
        let dust = json!({"nothing_traded": false, "position": {"id": "p4", "status": "unwound", "long": null, "short": null, "note": format!("建仓未完成，已成交腿已全部回滚。{}", arb_exec::MAKER_DUST_NOTE)}});
        let ok = StatusCode::OK;
        assert_eq!(
            classify_attempt(&maker, ok, &unfilled),
            Outcome::NoFill("p1".into())
        );
        assert_eq!(
            classify_attempt(&maker, ok, &dust),
            Outcome::NoFill("p4".into())
        );
        assert_eq!(
            classify_attempt(&maker, ok, &rolled_back),
            Outcome::Unwound("p2".into()),
            "成交后对冲失败的回滚是真失败"
        );
        // 吃单的单子：第一腿就被拒、一张都没成交（某次实盘）不计失败；成交后回滚照旧是失败。
        assert_eq!(
            classify_attempt(&plain, ok, &unfilled),
            Outcome::Untraded("p1".into())
        );
        assert_eq!(
            classify_attempt(&plain, ok, &rolled_back),
            Outcome::Unwound("p2".into())
        );
        // 吃单没有「成交太碎」这回事：带了那个备注也按真回滚算。
        assert_eq!(
            classify_attempt(&plain, ok, &dust),
            Outcome::Unwound("p4".into())
        );
        // 不知道有没有成交（读台账失败时 `nothing_traded` 为 null）：按回滚算。
        let unknown =
            json!({"nothing_traded": null, "position": {"id": "p5", "status": "unwound"}});
        assert_eq!(
            classify_attempt(&plain, ok, &unknown),
            Outcome::Unwound("p5".into())
        );
        let opened = json!({"position": {"id": "p3", "status": "open"}});
        assert_eq!(
            classify_attempt(&maker, ok, &opened),
            Outcome::Opened("p3".into())
        );
    }

    /// 止盈的盘口门槛：写进新仓位规则（不超过这一笔的止盈金额），校验要求先设止盈，旧设置文件读回来是不设。
    #[test]
    fn take_profit_book_threshold_reaches_new_positions() {
        let settings = Settings {
            take_profit_usdt: Some(dec("0.25")),
            take_profit_book_min_usdt: Some(Decimal::ZERO),
            ..Settings::default()
        };
        assert!(
            settings.validate(None).is_ok(),
            "{:?}",
            settings.validate(None)
        );
        let rules = settings.rules(None);
        assert_eq!(rules.take_profit_usdt, Some(dec("0.25")));
        assert_eq!(rules.take_profit_book_min_usdt, Some(Decimal::ZERO));
        assert!(arb_exec::validate_rules(&rules, None).is_ok());
        assert!(describe(&settings).contains("盘口为正即平"));
        // 门槛高于这一笔的止盈金额：压到止盈金额（等于没放宽），开仓时的规则校验照样过。
        let high = Settings {
            take_profit_book_min_usdt: Some(dec("1")),
            ..settings.clone()
        };
        assert_eq!(
            high.rules(None).take_profit_book_min_usdt,
            Some(dec("0.25"))
        );
        assert!(arb_exec::validate_rules(&high.rules(None), None).is_ok());
        // 没设止盈只设门槛：拒绝；负数：拒绝。
        let orphan = Settings {
            take_profit_usdt: None,
            take_profit_pct: None,
            back_to_normal: true,
            take_profit_book_min_usdt: Some(Decimal::ZERO),
            ..Settings::default()
        };
        assert!(orphan.validate(None).unwrap_err().contains("盘口门槛"));
        let negative = Settings {
            take_profit_book_min_usdt: Some(dec("-0.1")),
            ..settings.clone()
        };
        assert!(negative.validate(None).is_err());
        // 不设门槛：规则里也没有，行为与以前相同。
        let plain = Settings {
            take_profit_book_min_usdt: None,
            ..settings
        };
        assert_eq!(plain.rules(None).take_profit_book_min_usdt, None);
        // 旧设置文件没有这个字段。
        let mut old = serde_json::to_value(Settings::default()).unwrap();
        old.as_object_mut()
            .unwrap()
            .remove("take_profit_book_min_usdt");
        let back: Settings = serde_json::from_value(old).unwrap();
        assert_eq!(back.take_profit_book_min_usdt, None);
    }

    fn preset_done(
        marks: &mut HashMap<(Venue, String), PresetMark>,
        venue: Venue,
        base: &str,
        leverage: Decimal,
        at: Instant,
        done: bool,
    ) {
        marks.insert((venue, base.into()), PresetMark { leverage, at, done });
    }

    #[test]
    fn idle_leverage_presets_go_closest_signal_first_and_each_market_only_once() {
        let s = settings();
        let near = line("NVDA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        let far = line("TSLA", "long_b", leg("0.10", "0.05", "0.01"), -0.11);
        let mut hl = line("IBM", "long_a", leg("0.20", "0.10", "0.05"), -0.10);
        hl.pair = "hyperliquid-xyz:lighter-rh".into();
        hl.a = Venue::HyperliquidXyz;
        let v = view(vec![far, near, hl]);
        let all = |_: Venue| true;
        let now = Instant::now();
        let mut marks = HashMap::new();
        // 回到正常净收益最高的先做，一组两家依次做。
        assert_eq!(
            next_preset(&v, &s, &all, &all, &marks, now),
            Some((Venue::Arcus, "NVDA".into()))
        );
        preset_done(&mut marks, Venue::Arcus, "NVDA", s.leverage, now, true);
        assert_eq!(
            next_preset(&v, &s, &all, &all, &marks, now),
            Some((Venue::LighterRh, "NVDA".into()))
        );
        preset_done(&mut marks, Venue::LighterRh, "NVDA", s.leverage, now, true);
        assert_eq!(
            next_preset(&v, &s, &all, &all, &marks, now),
            Some((Venue::HyperliquidXyz, "IBM".into()))
        );
        // 这一家还在间隔里（Lighter RH 刚设过）：先做别家，组合不跳过。
        let rh_cooling = |venue: Venue| venue != Venue::LighterRh;
        assert_eq!(
            next_preset(&v, &s, &all, &rh_cooling, &HashMap::new(), now),
            Some((Venue::Arcus, "NVDA".into()))
        );
        let mut arcus_done = HashMap::new();
        preset_done(&mut arcus_done, Venue::Arcus, "NVDA", s.leverage, now, true);
        assert_eq!(
            next_preset(&v, &s, &all, &rh_cooling, &arcus_done, now),
            Some((Venue::HyperliquidXyz, "IBM".into()))
        );
        // 不是实盘已连接的场所、排除名单里的场所：整组跳过。
        let no_hl = |venue: Venue| venue != Venue::HyperliquidXyz;
        assert_eq!(
            next_preset(&v, &s, &no_hl, &all, &marks, now),
            Some((Venue::Arcus, "TSLA".into()))
        );
        let excluded = Settings {
            exclude_venues: vec![Venue::HyperliquidXyz],
            ..settings()
        };
        assert_eq!(
            next_preset(&v, &excluded, &all, &all, &marks, now),
            Some((Venue::Arcus, "TSLA".into()))
        );
        // 自动交易的杠杆改了：设好过的也要按新杠杆再设。
        let five = Settings {
            leverage: Decimal::from(5),
            ..settings()
        };
        assert_eq!(
            next_preset(&v, &five, &all, &all, &marks, now),
            Some((Venue::Arcus, "NVDA".into()))
        );
        // 正在做或没设成的：过 PRESET_RETRY 再试；全都设好了就没有下一个。
        preset_done(
            &mut marks,
            Venue::HyperliquidXyz,
            "IBM",
            s.leverage,
            now,
            false,
        );
        preset_done(&mut marks, Venue::LighterRh, "IBM", s.leverage, now, true);
        for venue in [Venue::Arcus, Venue::LighterRh] {
            preset_done(&mut marks, venue, "TSLA", s.leverage, now, true);
        }
        assert_eq!(next_preset(&v, &s, &all, &all, &marks, now), None);
        assert_eq!(
            next_preset(&v, &s, &all, &all, &marks, now + PRESET_RETRY),
            Some((Venue::HyperliquidXyz, "IBM".into()))
        );
    }

    #[test]
    fn leverage_presets_cover_only_lines_the_auto_trader_would_consider() {
        let s = settings();
        let mut watch = line("NVDA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        watch.watch_only = true;
        let mut noted = line("TSLA", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        noted.note = Some("还没有正常水平".into());
        let mut binance = line("AMD", "long_a", leg("0.30", "0.20", "0.09"), -0.11);
        binance.observe_only = true;
        binance.a = Venue::Binance;
        let v = view(vec![watch, noted, binance]);
        let all = |_: Venue| true;
        let now = Instant::now();
        // 币安开关关着：币安组不算，观察组与带说明的也不算。
        assert_eq!(next_preset(&v, &s, &all, &all, &HashMap::new(), now), None);
        let binance_on = Settings {
            binance_maker: true,
            binance_symbols: vec!["AMD".into()],
            ..settings()
        };
        assert_eq!(
            next_preset(&v, &binance_on, &all, &all, &HashMap::new(), now),
            Some((Venue::Binance, "AMD".into()))
        );
    }
}
