//! 券商抽象与模拟券商。
//!
//! 执行层只认 [`Broker`] 这个接口，不认具体场所。真实券商见
//! [`crate::hyperliquid_broker`] 与 [`crate::lighter_broker`]。
//!
//! 模拟券商不是占位符：它用**真实盘口**逐档吃单算成交价与手续费，是验证
//! 「这笔交易到底赚不赚、失败时怎么回滚」的安全手段。

use std::collections::HashMap;
use std::sync::Arc;

use arb_core::{
    ArbError, ArbResult, FillEstimate, Level, MarketSnapshot, OrderBook, Side, Symbol, Venue,
    estimate_fill_limited,
};
use arb_venues::VenueApi;
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use tokio::sync::Mutex;
use tracing::warn;

use crate::types::{
    ClientOrderId, Fill, NewOrder, OrderAck, OrderState, OrderStatus, PairPosition, Strategy,
};

/// 账户在某个场所的净持仓。
#[derive(Debug, Clone, serde::Serialize)]
pub struct VenuePosition {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 净数量（正 = 多，负 = 空），按标的币计。
    pub net_quantity: Decimal,
    pub average_price: Option<Decimal>,
    /// 按最新成交价折算的名义额（绝对值）。
    pub notional_usdt: Decimal,
}

/// 私有交易接口。
///
/// # 不变量（实现者必须保证）
///
/// 1. **`place` 幂等**：同一个 `client_order_id` 重复提交不能产生第二笔订单。
///    超时重试是这类系统最常见的事故来源 —— 请求发出去了、响应没回来、于是重发，
///    结果开了两条腿，而策略以为自己只开了一条。
/// 2. **`place` 返回后订单必须可查**：`OrderAck.venue_order_id` 缺了就没法撤单。
/// 3. **`open_orders` 必须包含所有场上订单**，对账要靠它发现「本地不知道的订单」。
/// 4. **`order_state` 只在确定从未提交时返回 `None`**：执行层据此把 `place` 的报错
///    记成拒单。发出去过、或无法确定（超时、历史过期）时必须返回错误。
#[async_trait]
pub trait Broker: Send + Sync + 'static {
    fn venue(&self) -> Venue;
    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck>;
    /// 查一笔订单的当前状态（含累计成交与均价）。
    ///
    /// 执行层靠它判断「这条腿到底成了多少」；对账靠它发现本地不知道的订单。
    /// 返回 `None` = 这个 id 确定从未提交过（见不变量 4）。
    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>>;
    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()>;
    async fn open_orders(&self) -> ArbResult<Vec<OrderState>>;
    async fn positions(&self) -> ArbResult<Vec<VenuePosition>>;
    /// 单边吃单费率的回落值。风控算成本时要用。
    fn fee_per_side(&self) -> Decimal;
    /// 开仓前的**只读**预热：预取下单要用的市场元数据，并核对这个合约的逐仓杠杆是不是已经是目标值
    /// （是就记下，之后 [`Broker::prepare_open`] 与 `place` 不必再往返）。**不改任何设置、不下单。**
    /// 默认什么都不做。
    ///
    /// 看板在拉盘口、算计划的同时就对两条腿调用它，把往返藏在这些步骤后面；因为是只读，计划被闸门拒绝时
    /// 也没有任何副作用。杠杆需要改的话留给 [`Broker::prepare_open`]（下单已定之后）去做。
    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let _ = (symbol, leverage);
        Ok(())
    }

    /// 开仓前的准备：核对（必要时设置）这个合约的逐仓杠杆，并预取下单要用的市场元数据，
    /// 让 `place` 自己不必再多花几次往返。**不下单。** 默认什么都不做。
    ///
    /// 执行器在第一条腿下单前对两条腿**并发**调用它：第二腿的杠杆设不上，在第一腿成交之前
    /// 就能发现，不必先成交一条腿再回滚；第二腿下单时也不用再单独往返一次。
    async fn prepare_open(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let _ = (symbol, leverage);
        Ok(())
    }

    /// 模式感知的开仓准备。默认实现拒绝全仓，避免未适配券商静默开成逐仓。
    async fn prepare_open_mode(
        &self,
        symbol: &Symbol,
        side: Side,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let _ = side;
        if mode.is_cross() {
            return Err(ArbError::venue(
                self.venue().as_str(),
                "这个券商尚未适配全仓开仓",
            ));
        }
        self.prepare_open(symbol, leverage).await
    }

    /// 这个账户在这个合约上自 `since` 起实际收付的资金费（交易所结算流水）。
    ///
    /// 场所没有接入资金费流水时返回 `Ok(None)` —— 不能拿 0 冒充「没收到」。
    /// 流水按（账户, 合约）记：同一账户在同一合约上还有别的仓位时，它们的资金费也在里面。
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let _ = (symbol, since);
        Ok(None)
    }

    /// 这条腿在交易所**实际**的保证金与强平价。
    ///
    /// 台账里记的是开仓时的保证金（名义 ÷ 杠杆）；之后在交易所补保证金、调杠杆、亏损吃掉
    /// 保证金都不会回写台账，面板和强平保护就会一直按开仓时的数算（2026-09-30 LIT 补了约 785
    /// USDT，面板的强平价纹丝不动）。所以强平价要以交易所报告的为准。
    ///
    /// 场所没接入、这个合约没持仓、或响应里没有这些字段时返回 `Ok(None)` / 对应字段 `None` ——
    /// 调用方回落到台账里的数，不能当成 0。
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<VenueLegState>> {
        let _ = symbol;
        Ok(None)
    }

    /// 这个账户现在的**可用保证金**（计价币）：能拿来开新仓的部分。开仓前用来核对账户撑不撑得住
    /// 这笔，免得盘口够、账户不够，第一条腿成交后第二条腿失败，被迫付一次回滚的手续费和滑点。
    ///
    /// 场所没接入或响应里没有这个字段时返回 `Ok(None)` —— 调用方只警告、不拦（不知道不等于不够）。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        Ok(None)
    }

    /// 是否接入了「往逐仓持仓补保证金」。自动加保证金规则只在两条腿所在场所都接入时才允许开启。
    fn supports_add_margin(&self) -> bool {
        false
    }

    /// 往这个合约**已有的逐仓持仓**补 `amount_usdt` 保证金（计价币，正数）。**写操作，动真钱。**
    ///
    /// 返回值把「没动钱」「动了」「不知道动没动」分开，调用方（[`crate::Executor::add_margin`]）据此
    /// 记账：
    /// - `Err`：**没有发出请求**（只读模式、没有这个持仓、不是逐仓、参数不合法、没接入）。没动钱。
    /// - `Ok(Applied)`：交易所已确认生效。
    /// - `Ok(Refused)`：交易所明确拒绝（可用资金不够、没有逐仓持仓、低于最小额……）。没动钱。
    /// - `Ok(Unknown)`：请求发出去了但结果不明（超时、只拿到「已受理」的 ACK）。**可能已经到账**。
    ///
    /// 实现者必须保证：**一次调用最多发一次写请求，绝不内部重试** —— 补保证金没有幂等键，
    /// 重发会补两次。结果不明时由调用方读 [`Broker::leg_state`] 的保证金核对。
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        let _ = (symbol, amount_usdt);
        Err(ArbError::venue(
            self.venue().as_str(),
            "这个场所没有接入补保证金",
        ))
    }

    /// 这个账户在这个合约上 `since` ~ `until` 之间的全部成交（交易所自己的成交记录，含别的
    /// 交易，调用方再按数量对账归因，见 [`crate::settlement`]）。
    ///
    /// 场所没接入成交记录时返回 `Ok(None)`。翻页不完整、时间对不上等无法保证完整的情况
    /// 必须返回错误，**不能**返回一份不全的记录 —— 不全的成交会算出错的盈亏。
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<Vec<crate::settlement::VenueFill>>> {
        let _ = (symbol, since, until);
        Ok(None)
    }
}

/// [`Broker::add_margin`] 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarginOutcome {
    /// 交易所已确认生效。
    Applied,
    /// 交易所明确拒绝，没有动钱。
    Refused(String),
    /// 请求发出去了但结果不明：可能已经到账。
    Unknown(String),
}

/// 一条腿在交易所实际的保证金状态。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct VenueLegState {
    /// 交易所报告的真实保证金模式；未知不猜。
    pub margin_mode: Option<crate::MarginMode>,
    /// 这个仓位在交易所占用（分配）的保证金，含补进去的部分。逐仓才有意义。
    pub margin_usdt: Option<Decimal>,
    /// 交易所报告的强平价。
    pub liquidation_price: Option<Decimal>,
}

/// 一段时间内实际收付的资金费。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct FundingTotal {
    /// 合计（正 = 收到，负 = 付出），计价币。
    pub usdt: Decimal,
    /// 结算了几次。
    pub payments: usize,
    /// 最近一次结算的时间。
    pub last_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl FundingTotal {
    /// 从（时间, 金额）流水汇总，只算 `since` 之后（含）的。
    pub fn from_rows(
        rows: impl IntoIterator<Item = (chrono::DateTime<chrono::Utc>, Decimal)>,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let mut total = Self {
            usdt: Decimal::ZERO,
            payments: 0,
            last_at: None,
        };
        for (at, amount) in rows {
            if at < since {
                continue;
            }
            total.usdt += amount;
            total.payments += 1;
            total.last_at = total.last_at.max(Some(at));
        }
        total
    }
}

#[derive(Default)]
struct PaperState {
    /// key = client_order_id。用 id 做键本身就保证了幂等。
    orders: HashMap<String, OrderState>,
    /// 交易所侧订单号 → 客户订单号。
    by_venue_id: HashMap<String, String>,
    positions: HashMap<(Venue, String), VenuePosition>,
    fills: Vec<Fill>,
    seq: u64,
}

/// 用真实盘口成交的模拟券商。
pub struct PaperBroker {
    api: Arc<dyn VenueApi>,
    venue: Venue,
    fee_per_side: Decimal,
    /// 逐合约的真实吃单费率。MEXC 的费率随合约不同，用统一值会让一部分合约算错。
    fee_table: HashMap<(Venue, String), Decimal>,
    depth_levels: u32,
    state: Mutex<PaperState>,
}

impl PaperBroker {
    pub fn new(api: Arc<dyn VenueApi>, fee_per_side: Decimal, depth_levels: u32) -> Self {
        let venue = api.venue();
        Self {
            api,
            venue,
            fee_per_side,
            fee_table: HashMap::new(),
            depth_levels: depth_levels.max(1),
            state: Mutex::new(PaperState::default()),
        }
    }

    /// 装入扫描时取到的真实费率。键是 `(场所, 合约)`。
    ///
    /// 不装的话会退回 `fee_per_side` —— 那会让 Lighter（真值 0）多付一笔不存在的
    /// 手续费、让 Gate（真值 0.00075）少付 50%，两边都会把「这笔赚不赚」算错。
    pub fn with_fee_table(mut self, table: HashMap<(Venue, String), Decimal>) -> Self {
        self.fee_table = table;
        self
    }
    /// 用台账重建纸面账户净仓位，供进程重启后的监控和平仓使用。
    pub fn with_positions(mut self, positions: &[PairPosition]) -> Self {
        let mut state = PaperState::default();
        for position in positions
            .iter()
            .filter(|position| position.status.has_exposure())
        {
            for leg in [position.long.as_ref(), position.short.as_ref()]
                .into_iter()
                .flatten()
            {
                if leg.venue != self.venue {
                    continue;
                }
                let Some(quantity) = leg.quantity() else {
                    continue;
                };
                let signed = match leg.side {
                    Side::Buy => quantity,
                    Side::Sell => -quantity,
                };
                let entry = state
                    .positions
                    .entry((self.venue, position.symbol.to_string()))
                    .or_insert_with(|| VenuePosition {
                        venue: self.venue,
                        symbol: position.symbol.clone(),
                        net_quantity: Decimal::ZERO,
                        average_price: None,
                        notional_usdt: Decimal::ZERO,
                    });
                entry.net_quantity += signed;
                entry.notional_usdt += leg.notional_usdt;
                entry.average_price = Some(leg.average_price);
            }
        }
        self.state = Mutex::new(state);
        self
    }

    fn fee_for(&self, symbol: &Symbol) -> Decimal {
        self.fee_table
            .get(&(self.venue, symbol.to_string()))
            .copied()
            .unwrap_or(self.fee_per_side)
    }

    pub async fn fills(&self) -> Vec<Fill> {
        self.state.lock().await.fills.clone()
    }

    /// 记账一笔成交：更新订单状态与场所持仓。
    fn book_fill(state: &mut PaperState, fill: Fill, order: &NewOrder, venue: Venue) {
        if let Some(existing) = state.orders.get_mut(&fill.client_order_id.0) {
            existing.apply(&fill);
        }
        let key = (venue, order.symbol.to_string());
        let entry = state.positions.entry(key).or_insert_with(|| VenuePosition {
            venue,
            symbol: order.symbol.clone(),
            net_quantity: Decimal::ZERO,
            average_price: None,
            notional_usdt: Decimal::ZERO,
        });
        let quantity = if fill.price > Decimal::ZERO {
            fill.notional_usdt / fill.price
        } else {
            Decimal::ZERO
        };
        let signed = match fill.side {
            Side::Buy => quantity,
            Side::Sell => -quantity,
        };
        // 持仓归零时清掉均价，避免下次建仓沿用旧价。
        let new_net = entry.net_quantity + signed;
        if entry.net_quantity.is_zero()
            || entry.net_quantity.is_sign_negative() != new_net.is_sign_negative()
        {
            entry.average_price = Some(fill.price);
        } else if let Some(average) = entry.average_price {
            let total = entry.net_quantity.abs() + quantity;
            if total > Decimal::ZERO {
                entry.average_price =
                    Some((average * entry.net_quantity.abs() + fill.price * quantity) / total);
            }
        }
        entry.net_quantity = new_net;
        entry.notional_usdt = new_net.abs() * fill.price;
        state.fills.push(fill);
    }
}

#[async_trait]
impl Broker for PaperBroker {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn prepare_open_mode(
        &self,
        _: &Symbol,
        _: Side,
        _: Option<Decimal>,
        _: crate::MarginMode,
    ) -> ArbResult<()> {
        Ok(())
    }

    fn fee_per_side(&self) -> Decimal {
        self.fee_per_side
    }

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// 纸面没有交易所账户：补保证金只改台账里这条腿的保证金（由执行器记），这里直接算生效。
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        let _ = symbol;
        if amount_usdt <= Decimal::ZERO {
            return Err(ArbError::config("补保证金的金额必须为正"));
        }
        Ok(MarginOutcome::Applied)
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        let mut state = self.state.lock().await;

        // 幂等：同一个 client_order_id 直接返回已有的确认。
        if let Some(existing) = state.orders.get(&order.client_order_id.0) {
            return match &existing.venue_order_id {
                Some(venue_order_id) => Ok(OrderAck {
                    client_order_id: order.client_order_id.clone(),
                    venue_order_id: venue_order_id.clone(),
                    status: existing.status,
                }),
                None => Err(ArbError::config(format!(
                    "订单 {} 已存在但还没有交易所侧单号，不能重复提交",
                    order.client_order_id
                ))),
            };
        }
        if order.reduce_only {
            let net = state
                .positions
                .get(&(self.venue, order.symbol.to_string()))
                .map_or(Decimal::ZERO, |position| position.net_quantity);
            let available = match order.side {
                Side::Buy => -net,
                Side::Sell => net,
            };
            if order
                .quantity
                .is_none_or(|quantity| quantity <= Decimal::ZERO || quantity > available)
            {
                return Err(ArbError::venue(
                    self.venue.as_str(),
                    "reduce-only 数量超出当前持仓",
                ));
            }
        }

        let book = self
            .api
            .fetch_depth(&order.symbol, self.depth_levels)
            .await?;
        if book.side(order.side).is_empty() {
            let mut rejected = OrderState::new(order.clone());
            rejected.status = OrderStatus::Rejected;
            rejected.reject_reason = Some("盘口该侧为空".into());
            state.seq += 1;
            let venue_order_id = format!("paper-{}-{}", self.venue.as_str(), state.seq);
            rejected.venue_order_id = Some(venue_order_id.clone());
            state
                .by_venue_id
                .insert(venue_order_id.clone(), order.client_order_id.0.clone());
            state
                .orders
                .insert(order.client_order_id.0.clone(), rejected);
            return Err(ArbError::venue(self.venue.as_str(), "盘口该侧为空，拒单"));
        }

        let estimate = match order.quantity {
            Some(quantity) => estimate_quantity(
                book.side(order.side),
                quantity,
                order.side,
                order.limit_price,
            ),
            None => estimate_fill_limited(
                book.side(order.side),
                order.notional_usdt,
                order.side,
                order.limit_price,
            ),
        };

        state.seq += 1;
        let venue_order_id = format!("paper-{}-{}", self.venue.as_str(), state.seq);

        let Some(estimate) = estimate else {
            let mut rejected = OrderState::new(order.clone());
            rejected.status = OrderStatus::Rejected;
            rejected.venue_order_id = Some(venue_order_id.clone());
            rejected.reject_reason = Some("盘口数据不可用".into());
            state
                .by_venue_id
                .insert(venue_order_id.clone(), order.client_order_id.0.clone());
            state
                .orders
                .insert(order.client_order_id.0.clone(), rejected);
            return Err(ArbError::venue(self.venue.as_str(), "盘口数据不可用，拒单"));
        };

        let mut placed = OrderState::new(order.clone());
        placed.venue_order_id = Some(venue_order_id.clone());
        placed.status = OrderStatus::Open;
        state
            .by_venue_id
            .insert(venue_order_id.clone(), order.client_order_id.0.clone());
        state.orders.insert(order.client_order_id.0.clone(), placed);

        if estimate.filled_usdt > Decimal::ZERO {
            let fee = estimate.filled_usdt * self.fee_for(&order.symbol);
            let fill = Fill {
                client_order_id: order.client_order_id.clone(),
                venue: self.venue,
                side: order.side,
                price: estimate.average_price,
                notional_usdt: estimate.filled_usdt,
                fee_usdt: fee,
                at: Utc::now(),
            };
            Self::book_fill(&mut state, fill, order, self.venue);
            if order.quantity.is_some() {
                let status = if estimate.exhausted {
                    OrderStatus::Open
                } else {
                    OrderStatus::Filled
                };
                if let Some(placed) = state.orders.get_mut(&order.client_order_id.0) {
                    placed.status = status;
                }
            }
        }

        let status = state
            .orders
            .get(&order.client_order_id.0)
            .map(|state| state.status)
            .unwrap_or(OrderStatus::Open);
        if estimate.exhausted && status.is_live() {
            warn!(
                venue = %self.venue,
                symbol = %order.symbol,
                filled = %estimate.filled_usdt,
                target = %order.notional_usdt,
                "盘口不足以完全成交，订单仍有剩余"
            );
        }

        Ok(OrderAck {
            client_order_id: order.client_order_id.clone(),
            venue_order_id,
            status,
        })
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let state = self.state.lock().await;
        Ok(state.orders.get(&client_order_id.0).cloned())
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        let mut state = self.state.lock().await;
        let Some(client_id) = state.by_venue_id.get(venue_order_id).cloned() else {
            // 撤一个不存在的订单不是错误：可能已经成交或已撤，重复撤单必须无害。
            return Ok(());
        };
        if let Some(order) = state.orders.get_mut(&client_id)
            && order.status.is_live()
        {
            order.status = OrderStatus::Cancelled;
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let state = self.state.lock().await;
        Ok(state
            .orders
            .values()
            .filter(|order| order.status.is_live())
            .cloned()
            .collect())
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let state = self.state.lock().await;
        Ok(state
            .positions
            .values()
            .filter(|position| !position.net_quantity.is_zero())
            .cloned()
            .collect())
    }
}

/// 纸面订单按标的数量逐档成交；不能将入场名义额拿来替代平仓数量。
fn estimate_quantity(
    levels: &[Level],
    quantity: Decimal,
    side: Side,
    limit_price: Option<Decimal>,
) -> Option<FillEstimate> {
    let best = levels.first()?.price;
    if best <= Decimal::ZERO || quantity <= Decimal::ZERO {
        return None;
    }
    let mut remaining = quantity;
    let mut paid = Decimal::ZERO;
    for level in levels {
        if level.price <= Decimal::ZERO || level.notional_usdt <= Decimal::ZERO {
            continue;
        }
        if limit_price.is_some_and(|limit| match side {
            Side::Buy => level.price > limit,
            Side::Sell => level.price < limit,
        }) {
            break;
        }
        let take = remaining.min(level.notional_usdt / level.price);
        paid += take * level.price;
        remaining -= take;
        if remaining <= Decimal::ZERO {
            break;
        }
    }
    let filled = quantity - remaining;
    if filled <= Decimal::ZERO {
        return None;
    }
    let average_price = paid / filled;
    Some(FillEstimate {
        filled_usdt: paid,
        average_price,
        slippage: match side {
            Side::Buy => (average_price - best) / best,
            Side::Sell => (best - average_price) / best,
        },
        exhausted: remaining > Decimal::ZERO,
    })
}

/// 从批量快照里取某个场所某个合约的标记价，用于给持仓估值。
pub fn mark_of(snapshots: &[MarketSnapshot], venue: Venue, symbol: &Symbol) -> Option<Decimal> {
    snapshots
        .iter()
        .find(|snapshot| snapshot.venue == venue && &snapshot.symbol == symbol)
        .and_then(|snapshot| snapshot.mark_price)
}

/// 一条腿的成交摘要，供仓位记录使用。
pub fn leg_fill(order: &NewOrder, state: &OrderState) -> Option<crate::types::LegFill> {
    if !state.filled_usdt.gt(&Decimal::ZERO) {
        return None;
    }
    Some(crate::types::LegFill {
        venue: order.venue,
        side: order.side,
        notional_usdt: state.filled_usdt,
        average_price: state.average_price.unwrap_or(Decimal::ZERO),
        fee_usdt: state.fee_usdt,
        client_order_id: order.client_order_id.clone(),
        // 保证金取决于杠杆，由执行器在建仓时填上。
        margin_usdt: None,
    })
}

/// 判断一笔成交属于哪条策略的哪条腿。日志与对账都要能追溯到仓位。
pub fn describe(strategy: Strategy, side: Side) -> &'static str {
    match (strategy, side) {
        (Strategy::Funding, Side::Buy) | (Strategy::Spread, Side::Buy) => "long",
        (Strategy::Funding, Side::Sell) | (Strategy::Spread, Side::Sell) => "short",
    }
}

/// 供测试与调用方复用的空盘口判断。
pub fn is_empty(book: &OrderBook) -> bool {
    book.bids.is_empty() && book.asks.is_empty()
}

/// 幂等键的构造集中在这里，避免调用方各自拼字符串。
pub fn order_id(position_id: &str, side: Side, attempt: u32) -> ClientOrderId {
    ClientOrderId::for_leg(position_id, side, attempt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn funding_totals_only_count_payments_from_the_start() {
        let at = |hour| Utc.with_ymd_and_hms(2026, 9, 30, hour, 0, 0).unwrap();
        let total = FundingTotal::from_rows(
            [
                (at(5), Decimal::new(-3, 2)),
                (at(7), Decimal::new(5, 2)),
                (at(8), Decimal::new(-1, 2)),
            ],
            at(6),
        );
        assert_eq!(total.usdt, Decimal::new(4, 2));
        assert_eq!(total.payments, 2);
        assert_eq!(total.last_at, Some(at(8)));
    }
}
