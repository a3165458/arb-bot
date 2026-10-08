//! 执行层的领域类型。
//!
//! 这一层的核心对象是**双腿仓位**（[`PairPosition`]），不是单腿订单。套利工具里
//! 「一笔仓位」永远是两条腿，把它们分开管理就等于把「一条腿成了、另一条腿没成」
//! 这种最危险的状态藏起来。

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use arb_core::{Side, Symbol, Venue};

/// 这笔仓位是按哪条策略开的。决定它按什么条件退出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    /// 赚资金费差：按结算周期持有，基差保持不变时净额为零。
    Funding,
    /// 赚基差收敛：基差收敛到目标即退出。
    Spread,
}

impl Strategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Strategy::Funding => "funding",
            Strategy::Spread => "spread",
        }
    }
}

/// 客户侧订单号。**幂等键**：同一个 id 重复提交不能产生第二笔订单。
///
/// 超时重试是这类系统最常见的事故来源：请求发出去了、响应没回来、于是重发 ——
/// 结果开了两条腿的仓位，而策略以为自己只开了一条。所以每笔订单在提交前就定好
/// 这个 id，重试时**复用**它，由交易所侧去重。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    /// 由仓位 id + 腿 + 序号拼出来，天然可复现：同一次执行重试拿到的 id 相同。
    pub fn for_leg(position_id: &str, leg: Side, attempt: u32) -> Self {
        Self(format!("{position_id}-{}-{attempt}", side_tag(leg)))
    }

    /// 第 `n` 次减仓的订单号。和开平仓的序号分开编，避免与 `for_leg` 撞号。
    pub fn for_trim(position_id: &str, leg: Side, n: u32) -> Self {
        Self(format!("{position_id}-{}-trim{n}", side_tag(leg)))
    }

    /// 第 `n` 次退出（平仓 / 回滚）的订单号。每次重试换号：同一个号已经有终态，
    /// 复用它只会拿回上一次的结果，永远平不掉剩下的敞口。
    pub fn for_exit(position_id: &str, leg: Side, n: u32) -> Self {
        Self(format!("{position_id}-{}-exit{n}", side_tag(leg)))
    }
}

fn side_tag(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

impl std::fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// 保证金模式。旧订单与旧台账默认逐仓，绝不因升级自动改成全仓。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginMode {
    #[default]
    Isolated,
    Cross,
}

impl MarginMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Isolated => "isolated",
            Self::Cross => "cross",
        }
    }

    pub const fn is_cross(self) -> bool {
        matches!(self, Self::Cross)
    }
}

impl std::fmt::Display for MarginMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MarginMode {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "isolated" => Ok(Self::Isolated),
            "cross" => Ok(Self::Cross),
            _ => Err("margin_mode 必须是 isolated（逐仓）或 cross（全仓）".into()),
        }
    }
}

/// 一张待下的订单。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrder {
    pub client_order_id: ClientOrderId,
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: Side,
    /// 名义额（计价币）。**不是数量** —— 各场所的数量口径不同（基础币 / 张数），
    /// 换算必须由连接器负责，执行层只表达意图。
    pub notional_usdt: Decimal,
    /// 平仓/减仓的精确标的数量；不能用入场名义额倒推当前价下的数量。
    #[serde(default)]
    pub quantity: Option<Decimal>,
    /// 限价。`None` = 市价。
    ///
    /// 双腿套利**应当尽量用限价**：市价在薄盘上会把滑点直接变成亏损，
    /// 而限价最多是不成交（可以重试或放弃）。
    pub limit_price: Option<Decimal>,
    /// 平仓、回滚和减仓只能减少仓位；真实券商必须映射为交易所 reduce-only。
    #[serde(default)]
    pub reduce_only: bool,
    /// 开仓保证金模式；退出单保留原模式，尤其 OKX / Bitget / MEXC 需要它定位持仓。
    #[serde(default)]
    pub margin_mode: MarginMode,
    /// 开仓杠杆；真实券商必须在提交订单前配置成功。
    #[serde(default)]
    pub leverage: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// 已提交，还没收到确认。
    Pending,
    /// 已确认，挂在簿上。
    Open,
    /// 全部成交。
    Filled,
    /// 已撤。
    Cancelled,
    /// 被拒。
    Rejected,
}

impl OrderStatus {
    /// 还在场上的状态。对账时要拿这些去交易所核对。
    pub const fn is_live(self) -> bool {
        matches!(self, OrderStatus::Pending | OrderStatus::Open)
    }
}

/// 交易所对提交的确认。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAck {
    pub client_order_id: ClientOrderId,
    /// 交易所侧订单号。缺了它就没法撤单，所以是必需的。
    pub venue_order_id: String,
    pub status: OrderStatus,
}

/// 一笔成交。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub client_order_id: ClientOrderId,
    pub venue: Venue,
    pub side: Side,
    pub price: Decimal,
    pub notional_usdt: Decimal,
    /// 手续费（计价币）。**必须计入**：它决定这笔交易到底赚不赚。
    pub fee_usdt: Decimal,
    /// 成交时刻。
    pub at: DateTime<Utc>,
}

impl Fill {
    /// 这一笔的现金影响（不含手续费）：买入为负、卖出为正。
    pub fn cash_flow(&self) -> Decimal {
        let gross = match self.side {
            Side::Buy => -self.notional_usdt,
            Side::Sell => self.notional_usdt,
        };
        gross - self.fee_usdt
    }
}

/// 订单的完整状态（提交时的意图 + 交易所侧的最新状态 + 累计成交）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderState {
    pub order: NewOrder,
    pub venue_order_id: Option<String>,
    pub status: OrderStatus,
    pub filled_usdt: Decimal,
    pub average_price: Option<Decimal>,
    pub fee_usdt: Decimal,
    pub reject_reason: Option<String>,
}

impl OrderState {
    pub fn new(order: NewOrder) -> Self {
        Self {
            order,
            venue_order_id: None,
            status: OrderStatus::Pending,
            filled_usdt: Decimal::ZERO,
            average_price: None,
            fee_usdt: Decimal::ZERO,
            reject_reason: None,
        }
    }

    /// 成交均价按标的数量加权；名义额加权会误算成交数量和对冲腿大小。
    pub fn apply(&mut self, fill: &Fill) {
        let total = self.filled_usdt + fill.notional_usdt;
        if total > Decimal::ZERO && fill.price > Decimal::ZERO {
            let previous_qty = self
                .average_price
                .filter(|price| *price > Decimal::ZERO)
                .map_or(Decimal::ZERO, |price| self.filled_usdt / price);
            let quantity = previous_qty + fill.notional_usdt / fill.price;
            if quantity > Decimal::ZERO {
                self.average_price = Some(total / quantity);
            }
        }
        self.filled_usdt = total;
        self.fee_usdt += fill.fee_usdt;
        if self.filled_usdt >= self.order.notional_usdt {
            self.status = OrderStatus::Filled;
        }
    }
}

/// 仓位生命周期。
///
/// `Unwinding` / `Unwound` 的存在是这个系统最重要的一条设计：双腿里有一条没成交时，
/// 仓位会处于**单腿裸奔**状态，必须显式记录并自动回滚，而不是当成「开仓失败」了事。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    /// 第一腿已提交，第二腿还没。
    Opening,
    /// 两腿都已建仓。
    Open,
    /// 有一条腿失败，正在回滚已成交的那条。
    Unwinding,
    /// 回滚完成，仓位已归零。
    Unwound,
    /// 正在平仓。
    Closing,
    /// 已平仓。
    Closed,
}

impl PositionStatus {
    /// 仓位是否还有敞口。对账与风控都按这个判断，不按「有没有记录」。
    pub const fn has_exposure(self) -> bool {
        matches!(
            self,
            PositionStatus::Opening
                | PositionStatus::Open
                | PositionStatus::Unwinding
                | PositionStatus::Closing
        )
    }
}

/// 一条腿的成交摘要。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegFill {
    pub venue: Venue,
    pub side: Side,
    /// 入场名义（数量 × 入场均价）。减仓后按比例缩小。
    pub notional_usdt: Decimal,
    pub average_price: Decimal,
    pub fee_usdt: Decimal,
    pub client_order_id: ClientOrderId,
    /// 这条腿占用的逐仓保证金。开仓时 = 名义 ÷ 杠杆；减仓时加上减掉部分的已实现
    /// 盈亏、扣掉手续费 —— 权益留在这条腿上，名义变小，保证金率变大、强平价远离，
    /// 这正是爆仓保护的原理。
    /// 旧台账没有这个字段，读回来是 `None`，强平距离就算不出来。
    #[serde(default)]
    pub margin_usdt: Option<Decimal>,
}

impl LegFill {
    /// 标的数量。两腿对冲的是数量，不是名义。
    pub fn quantity(&self) -> Option<Decimal> {
        // 名义 ÷ 均价，舍到 12 位小数：十进制除不尽时会留一条尾巴（2026-09-30 LIT 的
        // `751.99000000000000000000000001`），对账会判成不一致、平仓单的数量也不再是步长的
        // 整数倍。真实成交数量都是步长的整数倍，12 位小数的舍入伤不到它。
        (self.average_price > Decimal::ZERO).then(|| {
            (self.notional_usdt / self.average_price)
                .round_dp(12)
                .normalize()
        })
    }
}

/// 开仓后的任务规则：持仓期间自动执行。
///
/// 全部是 `Option`：`None` = 这条规则关闭。互相独立，同时触发时按
/// 「数量失衡 → 基差收敛 → 止盈 → 费差 → 自动加保证金 / 强平距离」的优先级只执行一条。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskRules {
    /// 当前费差年化（小数，0.05 = 5%）跌破它就整笔平仓。
    ///
    /// 用的是**当前毛费差**，不扣成本：开仓成本已经付了，是否继续持有只取决于
    /// 往后还能收多少。
    #[serde(default)]
    pub min_funding_apr: Option<Decimal>,
    /// 任一腿强平距离（%）低于它，就把两腿按同一比例减仓，把距离拉回它的 1.5 倍。
    #[serde(default)]
    pub liq_protection_pct: Option<Decimal>,
    /// 两腿标的数量偏差（%）超过它就整笔平仓。有效范围 0.5 ~ 100。
    #[serde(default)]
    pub size_mismatch_pct: Option<Decimal>,
    /// 基差收敛平仓（价差套利的退出条件）：当前标记价基差（%，空腿 − 多腿）收敛到
    /// 它以内就整笔平仓，兑现价差。与入场基差、平仓时的退出基差是同一个口径（标记价）。
    /// 旧台账没有这个字段，读回来是 `None`。
    #[serde(default)]
    pub basis_exit_pct: Option<Decimal>,
    /// 止盈：**含资金费的净盈利**（价格盈亏 + 已收付的资金费 − 全部手续费 − 预估平仓手续费与穿价）
    /// 达到它（USDT）就整笔平仓。触发先看标记价估算，再按两边盘口核对平掉后的真实净额，
    /// 盘口拿不到或核对后不到就继续持有；资金费流水查不到时**不评估**（不拿 0 冒充）。
    #[serde(default)]
    pub take_profit_usdt: Option<Decimal>,
    /// 自动加保证金：任一腿强平距离（%）低于它，就往这条腿的逐仓保证金里补钱，
    /// 把距离拉回它的 1.5 倍。**必须同时设置 [`auto_margin_max_usdt`](Self::auto_margin_max_usdt)**。
    #[serde(default)]
    pub auto_margin_pct: Option<Decimal>,
    /// 自动加保证金的累计上限（USDT，这笔仓位一生最多补这么多）。不设就不启用自动加保证金。
    /// 补不了（上限用完、账户可用资金不够、场所拒绝）时退回爆仓保护的减仓（如果设了）。
    #[serde(default)]
    pub auto_margin_max_usdt: Option<Decimal>,
}

impl TaskRules {
    pub fn is_empty(&self) -> bool {
        self.min_funding_apr.is_none()
            && self.liq_protection_pct.is_none()
            && self.size_mismatch_pct.is_none()
            && self.basis_exit_pct.is_none()
            && self.take_profit_usdt.is_none()
            && self.auto_margin_pct.is_none()
    }

    /// 自动加保证金是否启用：触发线与累计上限都给了。
    pub fn auto_margin(&self) -> Option<(Decimal, Decimal)> {
        self.auto_margin_pct.zip(self.auto_margin_max_usdt)
    }
}

/// 一笔双腿仓位。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairPosition {
    pub id: String,
    pub symbol: Symbol,
    pub strategy: Strategy,
    /// 多腿（低费率 / 便宜的那一边）。
    pub long: Option<LegFill>,
    /// 空腿（高费率 / 贵的那一边）。
    pub short: Option<LegFill>,
    /// 建仓时的基差（%）。退出时用它算价差盈亏。
    pub entry_basis_pct: Decimal,
    /// 开仓时算出来的往返成本（手续费 + 穿价），用于判断这笔到底赚不赚。
    pub expected_round_trip_cost: Decimal,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    /// 回滚 / 拒绝的原因。有值时必须展示出来。
    pub note: Option<String>,
    /// 两腿开仓时选择的保证金模式。持仓中不自动切换。
    #[serde(default)]
    pub margin_mode: MarginMode,
    /// 开仓杠杆（两腿相同）。旧台账没有这个字段。
    #[serde(default)]
    pub leverage: Option<Decimal>,
    #[serde(default)]
    pub rules: TaskRules,
    /// 已经发起过几次减仓（含没做完的）。用来给减仓单编号：先落盘再发单，失败的那次不能让
    /// 下一次重试复用同一个订单号 —— 券商拒绝重发记录过的订单号，查询还会返回旧订单。
    #[serde(default)]
    pub trims: u32,
    /// 已经发起过几次退出。给退出单编号；已退出的腿会从仓位上移除。
    #[serde(default)]
    pub exits: u32,
    /// 自动加保证金累计补了多少（USDT）。**按尝试额计**：结果未知的那几笔也算（可能其实到账了），
    /// 只有交易所明确拒绝的不算 —— 上限因此偏保守，不会因为超时重试把上限冲破。
    #[serde(default)]
    pub margin_added_usdt: Decimal,
    /// 已经平掉的数量（减仓 + 平仓 + 回滚）按成交价算出的价格盈亏，不含手续费。
    /// 腿完全平掉后入场价就从仓位上消失了，所以每笔退出成交时当场累计。
    #[serde(default)]
    pub realized_pnl_usdt: Decimal,
    /// 已经完全平掉的腿累计付过的手续费（开仓 + 减仓 + 平仓），每条腿只计一次。
    #[serde(default)]
    pub realized_fee_usdt: Decimal,
    /// 上面两项（价格盈亏、手续费）是怎么来的。`None` = 没有记录（旧台账，或外部平仓后还没核算出来）。
    #[serde(default)]
    pub realized_source: Option<RealizedSource>,
    /// 持仓期间交易所实际结算的资金费（正 = 收到）。没查到时为 `None`。
    #[serde(default)]
    pub realized_funding_usdt: Option<Decimal>,
    /// 这笔是在交易所被外部（手动）平掉的，台账只是事后发现并结束它。
    #[serde(default)]
    pub closed_externally: bool,
    /// 外部平仓时两腿的开仓记录：腿在结束时从仓位上清掉了，事后按成交核算盈亏还要用它。
    #[serde(default)]
    pub entry_legs: Option<EntryLegs>,
    /// 外部平仓的实际盈亏最终没能核算出来的原因（数量对不上、成交记录里有别的开仓……）。
    #[serde(default)]
    pub pnl_unattributed: Option<String>,
    /// 开仓时计划与实际成交的对照、各步耗时（见 [`crate::report`]）。旧台账、回滚掉的仓位没有。
    #[serde(default)]
    pub open_report: Option<crate::report::OpenReport>,
}

/// 已实现盈亏的出处。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RealizedSource {
    /// 看板自己的平仓单：逐笔成交当场入账。
    Executor,
    /// 外部平仓：事后按交易所的成交记录核算，数量逐腿对上才记。
    VenueFills,
}

/// 两腿的开仓记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryLegs {
    pub long: LegFill,
    pub short: LegFill,
}

impl PairPosition {
    /// 是不是在交易所被外部（手动）平掉、台账事后才发现的。更早版本识别出来的记录还没有
    /// `closed_externally` 字段，靠它写下的备注开头认。
    pub fn is_external_close(&self) -> bool {
        self.closed_externally
            || (self.status == PositionStatus::Closed
                && self
                    .note
                    .as_deref()
                    .is_some_and(|note| note.starts_with("在交易所外部平仓")))
    }

    /// 两条腿都成交了才算真正建仓。
    pub fn is_hedged(&self) -> bool {
        self.long.is_some() && self.short.is_some()
    }

    /// 当前只有一条腿成交 —— 裸敞口。
    pub fn is_naked(&self) -> bool {
        self.status.has_exposure() && !self.is_hedged()
    }

    pub fn total_notional(&self) -> Decimal {
        self.long
            .as_ref()
            .map(|leg| leg.notional_usdt)
            .unwrap_or(Decimal::ZERO)
            + self
                .short
                .as_ref()
                .map(|leg| leg.notional_usdt)
                .unwrap_or(Decimal::ZERO)
    }

    pub fn total_fee(&self) -> Decimal {
        self.long
            .as_ref()
            .map(|leg| leg.fee_usdt)
            .unwrap_or(Decimal::ZERO)
            + self
                .short
                .as_ref()
                .map(|leg| leg.fee_usdt)
                .unwrap_or(Decimal::ZERO)
    }

    /// 退出时的价差盈亏（%）：`(b_entry − b_exit)`。
    pub fn basis_pnl_pct(&self, exit_basis_pct: Decimal) -> Decimal {
        self.entry_basis_pct - exit_basis_pct
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order(notional: Decimal) -> NewOrder {
        NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId::for_leg("p1", Side::Buy, 0),
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: notional,
            limit_price: Some(dec!(100)),
            quantity: None,
            reduce_only: false,
            leverage: None,
        }
    }

    fn fill(price: Decimal, notional: Decimal) -> Fill {
        Fill {
            client_order_id: ClientOrderId::for_leg("p1", Side::Buy, 0),
            venue: Venue::Binance,
            side: Side::Buy,
            price,
            notional_usdt: notional,
            fee_usdt: notional * dec!(0.0005),
            at: Utc::now(),
        }
    }

    #[test]
    fn the_client_order_id_is_reproducible_so_retries_cannot_double_fill() {
        // 同一次执行重试必须拿到同一个 id —— 否则交易所侧无法去重
        assert_eq!(
            ClientOrderId::for_leg("pos-1", Side::Sell, 2),
            ClientOrderId::for_leg("pos-1", Side::Sell, 2)
        );
        assert_ne!(
            ClientOrderId::for_leg("pos-1", Side::Sell, 1),
            ClientOrderId::for_leg("pos-1", Side::Sell, 2)
        );
        assert_eq!(
            ClientOrderId::for_leg("p", Side::Buy, 0).to_string(),
            "p-buy-0"
        );
    }

    #[test]
    fn the_average_price_preserves_total_base_quantity() {
        let mut state = OrderState::new(order(dec!(300)));
        state.apply(&fill(dec!(100), dec!(100)));
        state.apply(&fill(dec!(200), dec!(200)));
        assert_eq!(state.filled_usdt / state.average_price.unwrap(), dec!(2));
        assert_eq!(state.filled_usdt, dec!(300));
        assert_eq!(state.status, OrderStatus::Filled);
    }

    #[test]
    fn a_partial_fill_stays_open() {
        let mut state = OrderState::new(order(dec!(300)));
        state.apply(&fill(dec!(100), dec!(100)));
        assert_eq!(state.status, OrderStatus::Pending, "只成交了一部分");
        assert!(state.status.is_live(), "场上还有剩余，对账要管它");
    }

    #[test]
    fn cash_flow_counts_fees_and_respects_side() {
        let buy = fill(dec!(100), dec!(1_000));
        // 买入付出 1000 本金 + 0.5 手续费
        assert_eq!(buy.cash_flow(), dec!(-1_000.5));
        let mut sell = fill(dec!(100), dec!(1_000));
        sell.side = Side::Sell;
        assert_eq!(sell.cash_flow(), dec!(999.5));
    }

    #[test]
    fn a_position_with_one_leg_is_reported_as_naked() {
        let mut position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "p1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: dec!(0.1),
            expected_round_trip_cost: dec!(0.002),
            status: PositionStatus::Opening,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: None,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        };
        assert!(position.is_naked(), "两腿都没成交也是裸敞口");

        position.long = Some(LegFill {
            venue: Venue::Binance,
            side: Side::Buy,
            notional_usdt: dec!(1_000),
            average_price: dec!(100),
            fee_usdt: dec!(0.5),
            client_order_id: ClientOrderId::for_leg("p1", Side::Buy, 0),
            margin_usdt: None,
        });
        assert!(position.is_naked(), "只有多腿成交 → 裸敞口");

        position.short = Some(LegFill {
            venue: Venue::Okx,
            side: Side::Sell,
            notional_usdt: dec!(1_000),
            average_price: dec!(100.1),
            fee_usdt: dec!(0.5),
            client_order_id: ClientOrderId::for_leg("p1", Side::Sell, 0),
            margin_usdt: None,
        });
        assert!(position.is_hedged());
        assert!(!position.is_naked());
        assert_eq!(position.total_fee(), dec!(1));
    }

    #[test]
    fn basis_pnl_is_entry_minus_exit() {
        let position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "p1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Spread,
            long: None,
            short: None,
            entry_basis_pct: dec!(1.0),
            expected_round_trip_cost: dec!(0.002),
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: None,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        };
        // 基差收敛到 0 → 赚 1%
        assert_eq!(position.basis_pnl_pct(dec!(0)), dec!(1.0));
        // 基差没动 → 0
        assert_eq!(position.basis_pnl_pct(dec!(1.0)), Decimal::ZERO);
        // 基差反向 → 亏
        assert_eq!(position.basis_pnl_pct(dec!(-1.0)), dec!(2.0));
    }

    #[test]
    fn a_legs_quantity_has_no_division_tail() {
        // 2026-09-30 LIT 实盘：lighter-rh 多腿 2998.203578 @ 3.9870258620460378462479554249。
        let leg = LegFill {
            venue: Venue::LighterRh,
            side: Side::Buy,
            notional_usdt: dec!(2998.203578),
            average_price: dec!(3.9870258620460378462479554249),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("lit", Side::Buy, 0),
            margin_usdt: None,
        };
        assert_eq!(leg.quantity(), Some(dec!(751.99)));
        assert_eq!(
            leg.quantity().unwrap().scale(),
            2,
            "尾巴消掉后是干净的 751.99"
        );
    }
}
