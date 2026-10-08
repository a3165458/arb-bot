//! 执行层：把「扫描出来的机会」变成「可审计的下单计划与仓位记录」。
//!
//! # 券商
//!
//! 执行器只认 [`Broker`] 接口。三个实现：
//!
//! - [`PaperBroker`]：逐档吃真实盘口算成交价与手续费，不碰资金（`arb-paper`）。
//! - [`hyperliquid_broker::HyperliquidBroker`]：Hyperliquid 主 perp dex 的签名下单（`arb-live`）。
//! - [`lighter_broker::LighterBroker`]：Lighter 主网，签名交给官方 `lighter-go` 动态库（`arb-live`）。
//!
//! 两个真实券商默认禁止一切写操作，必须显式开启；未知结果一律当成需要对账，不重发。
//!
//! # 六件事
//!
//! - [`cli`]：`arb-paper` 与 `arb-live` 共用的命令行流程，保证纸面与实盘是同一份逻辑。
//! - [`desk`]：开仓 / 平仓 / 对账 / 一轮监控的结构化流程（不打印），命令行与看板共用。
//! - [`live_connect`]：实盘券商的连接与安全参数校验，`arb-live` 与看板共用。
//! - [`preflight`]：下单前的闸门。每一条拒绝都说清是哪一条拦的。
//! - [`executor`]：双腿执行、失败回滚、等比例减仓。交易所没有原子双腿，所以回滚是必需品。
//! - [`monitor`]：持仓期间的任务规则（费差自动平仓、爆仓保护、数量失衡）。
//! - [`reconcile`]：断线/重启后把本地台账与交易所状态对齐。
//! - [`ledger`]：追加式台账。事故总是发生在状态转移的缝里，覆盖式存储会把缝抹掉。

pub mod arcus_broker;
pub mod aster_broker;
pub mod binance_broker;
pub mod bitget_broker;
pub mod broker;
pub mod bybit_broker;
pub mod cli;
pub mod desk;
pub mod executor;
pub mod gate_broker;
pub mod hyperliquid_broker;
pub mod ledger;
pub mod lighter_broker;
pub mod live_common;
pub mod live_connect;
pub mod margin;
pub mod mexc_broker;
pub mod monitor;
pub mod okx_broker;
pub mod preflight;
pub mod reconcile;
pub mod report;
pub mod settlement;
pub mod types;

pub use broker::{Broker, FundingTotal, PaperBroker, VenueLegState, VenuePosition};
pub use executor::{ExecutionOutcome, Executor, PositionSetup};
pub use ledger::{Ledger, Record, Replayed, replay_file};
pub use monitor::{
    Action, Evaluation, ExitCheck, ExitQuote, Observation, basis_exit_triggered, evaluate,
    evaluate_with_exit, exit_quote, opening_distance_pct, validate_rules,
};
pub use preflight::{LegPlan, Limits, Plan, Preflight, Rejection, plan};
pub use reconcile::{Divergence, DivergenceKind, Reconciliation, reconcile};
pub use report::{LegExecution, OpenReport, OpenTiming, open_report};
pub use types::{
    ClientOrderId, Fill, LegFill, MarginMode, NewOrder, OrderAck, OrderState, OrderStatus,
    PairPosition, PositionStatus, Strategy, TaskRules,
};
pub use types::{EntryLegs, RealizedSource};
