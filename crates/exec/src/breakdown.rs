//! 已结束仓位的逐腿明细：每条腿开仓、减仓、平仓各成交了多少、什么价、付了多少手续费，
//! 以及这条腿的价格盈亏。腿在平仓时就从仓位上清掉了，这里从台账的订单记录还原。
//!
//! 口径与执行器入账一致（[`crate::executor`] 的 `exit_legs` 与 `trim`）：
//! - 价格盈亏 = 成交数量 × (平仓均价 − 开仓均价)，空腿反号；数量 = 成交名义 ÷ 成交均价。
//! - 手续费 = 有成交的开仓、减仓、平仓单的手续费之和。
//!
//! 两腿合计与台账里的已实现数对不上时如实给出差额，不抹平。外部平仓（在交易所里手动平的）
//! 台账里没有平仓单，只有开仓侧和期间的减仓，合计以按交易所成交核算的那个数为准。
//!
//! 补仓：补仓单和开仓单一起算开仓均价（按数量加权），补仓回退单和减仓、平仓一样算价格盈亏。
//! 执行器在补仓时也是这样并进腿的；只有补仓与减仓交错发生时（先减仓、后补仓），执行器按当时的
//! 均价记减仓那一笔，这里按全部开仓成交的均价算，会有小差额，照样如实列出。

use std::collections::HashMap;

use arb_core::{Side, Venue};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{OrderRole, OrderState, PairPosition, RealizedSource};

/// 差额小于这个数就算对上（Decimal 除法的尾数）。
const GAP_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 8);

/// 一张有成交的减仓或平仓单。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LegExit {
    pub client_order_id: String,
    pub role: OrderRole,
    pub quantity: Decimal,
    pub average_price: Decimal,
    pub notional_usdt: Decimal,
    pub fee_usdt: Decimal,
    /// 这一笔的价格盈亏。开仓均价不知道时为 `None`。
    pub price_pnl_usdt: Option<Decimal>,
}

/// 一条腿从开到平。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LegBreakdown {
    pub venue: Venue,
    /// 开仓方向：买 = 多腿。
    pub side: Side,
    /// 开仓与补仓成交合并（均价按数量加权）。没有开仓成交记录时数量为 0、均价为 `None`。
    pub entry_quantity: Decimal,
    pub entry_price: Option<Decimal>,
    pub entry_notional_usdt: Decimal,
    pub entry_fee_usdt: Decimal,
    /// 其中有几笔是补仓（有成交的）。
    pub adds: usize,
    /// 补仓回退、减仓与平仓成交，各按序号。
    pub exits: Vec<LegExit>,
    pub exited_quantity: Decimal,
    /// 平仓成交均价（多笔按数量加权）。没有平仓成交时为 `None`。
    pub exit_price: Option<Decimal>,
    /// 这条腿台账里有的成交算出的价格盈亏。
    pub price_pnl_usdt: Decimal,
    /// 开仓 + 减仓 + 平仓手续费。
    pub fee_usdt: Decimal,
}

/// 最近一张有成交的平仓单（`Exit`）在一条腿上的实际成交。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ExitFill {
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee_usdt: Decimal,
}

/// 规则平仓刚成交的那一张平仓单，两条腿各自的实际成交。给「平仓对账」日志拆差额用。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct ExitFills {
    pub long: Option<ExitFill>,
    pub short: Option<ExitFill>,
}

/// 从台账订单里取 `position` 最近一次平仓（序号最大的 `Exit`）在两条腿上的实际成交；一张都没有返回 `None`。
/// 减仓、补仓回退不算：只看平仓这一张。
pub fn last_exit_fills(
    position: &PairPosition,
    orders: &HashMap<String, OrderState>,
) -> Option<ExitFills> {
    let breakdown = position_breakdown(position, orders)?;
    let last = |leg: Option<&LegBreakdown>| {
        leg?.exits
            .iter()
            .filter_map(|exit| match exit.role {
                OrderRole::Exit { n } => Some((n, exit)),
                _ => None,
            })
            .max_by_key(|(n, _)| *n)
            .map(|(_, exit)| ExitFill {
                price: exit.average_price,
                quantity: exit.quantity,
                fee_usdt: exit.fee_usdt,
            })
    };
    let fills = ExitFills {
        long: last(breakdown.long.as_ref()),
        short: last(breakdown.short.as_ref()),
    };
    (fills.long.is_some() || fills.short.is_some()).then_some(fills)
}

/// 一笔已结束仓位的双边明细。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionBreakdown {
    pub long: Option<LegBreakdown>,
    pub short: Option<LegBreakdown>,
    /// 两腿价格盈亏、手续费之和（只含台账里有的成交）。
    pub price_pnl_usdt: Decimal,
    pub fee_usdt: Decimal,
    /// 两腿之和 − 台账已实现数；对得上时为 `None`。外部平仓不比（台账里本来就没有平仓单）。
    pub pnl_gap_usdt: Option<Decimal>,
    pub fee_gap_usdt: Option<Decimal>,
    /// 外部平仓：台账里只有开仓侧（和期间的减仓），合计以按交易所成交核算的数为准。
    pub external: bool,
}

struct Filled {
    id: String,
    side: Side,
    role: OrderRole,
    venue: Venue,
    quantity: Decimal,
    price: Decimal,
    notional: Decimal,
    fee: Decimal,
}

/// 还原 `position` 的双边明细。`orders` 是台账重放出的全部订单（按客户订单号）。
/// 这笔仓位一张有成交的单都没有时返回 `None`。
pub fn position_breakdown(
    position: &PairPosition,
    orders: &HashMap<String, OrderState>,
) -> Option<PositionBreakdown> {
    let mut filled: Vec<Filled> = orders
        .iter()
        .filter_map(|(id, state)| {
            let (side, role) = state.order.client_order_id.parse_for(&position.id)?;
            let price = state.average_price.filter(|price| *price > Decimal::ZERO)?;
            (state.filled_usdt > Decimal::ZERO).then(|| Filled {
                id: id.clone(),
                side,
                role,
                venue: state.order.venue,
                quantity: state.filled_usdt / price,
                price,
                notional: state.filled_usdt,
                fee: state.fee_usdt,
            })
        })
        .collect();
    if filled.is_empty() {
        return None;
    }
    // 先开仓、补仓，再减仓、补仓回退，最后平仓，各按序号：HashMap 的顺序不可复现。
    filled.sort_by_key(|fill| match fill.role {
        OrderRole::Entry { attempt } => (0, attempt),
        OrderRole::Add { n } => (1, n),
        OrderRole::Trim { n } => (2, n),
        OrderRole::Undo { n } => (3, n),
        OrderRole::Exit { n } => (4, n),
    });
    let long = leg(&filled, true);
    let short = leg(&filled, false);
    let legs = [long.as_ref(), short.as_ref()];
    let price_pnl_usdt = legs.iter().flatten().map(|leg| leg.price_pnl_usdt).sum();
    let fee_usdt = legs.iter().flatten().map(|leg| leg.fee_usdt).sum();
    let external = position.realized_source == Some(RealizedSource::VenueFills)
        || position.is_external_close();
    let gap = |ours: Decimal, ledger: Decimal| {
        let gap = ours - ledger;
        (!external && gap.abs() >= GAP_TOLERANCE).then_some(gap)
    };
    Some(PositionBreakdown {
        pnl_gap_usdt: gap(price_pnl_usdt, position.realized_pnl_usdt),
        fee_gap_usdt: gap(fee_usdt, position.realized_fee_usdt),
        long,
        short,
        price_pnl_usdt,
        fee_usdt,
        external,
    })
}

fn leg(filled: &[Filled], is_long: bool) -> Option<LegBreakdown> {
    let mine: Vec<&Filled> = filled
        .iter()
        .filter(|fill| fill.role.is_long_leg(fill.side) == is_long)
        .collect();
    let entries: Vec<&&Filled> = mine.iter().filter(|fill| fill.role.is_entry()).collect();
    let venue = mine.first()?.venue;
    let entry_quantity: Decimal = entries.iter().map(|fill| fill.quantity).sum();
    let entry_notional_usdt: Decimal = entries.iter().map(|fill| fill.notional).sum();
    let entry_fee_usdt: Decimal = entries.iter().map(|fill| fill.fee).sum();
    // 一次提交就成交时（正常情况）直接用那笔的均价，与执行器记在腿上的完全相同。
    let entry_price = match entries.as_slice() {
        [] => None,
        [only] => Some(only.price),
        _ => (entry_quantity > Decimal::ZERO).then(|| entry_notional_usdt / entry_quantity),
    };
    let exits: Vec<LegExit> = mine
        .iter()
        .filter(|fill| !fill.role.is_entry())
        .map(|fill| LegExit {
            client_order_id: fill.id.clone(),
            role: fill.role,
            quantity: fill.quantity,
            average_price: fill.price,
            notional_usdt: fill.notional,
            fee_usdt: fill.fee,
            price_pnl_usdt: entry_price.map(|entry| {
                if is_long {
                    fill.quantity * (fill.price - entry)
                } else {
                    fill.quantity * (entry - fill.price)
                }
            }),
        })
        .collect();
    let exited_quantity: Decimal = exits.iter().map(|exit| exit.quantity).sum();
    let exited_notional: Decimal = exits.iter().map(|exit| exit.notional_usdt).sum();
    let adds = entries
        .iter()
        .filter(|fill| matches!(fill.role, OrderRole::Add { .. }))
        .count();
    Some(LegBreakdown {
        venue,
        adds,
        side: if is_long { Side::Buy } else { Side::Sell },
        entry_quantity,
        entry_price,
        entry_notional_usdt,
        entry_fee_usdt,
        exit_price: (exited_quantity > Decimal::ZERO).then(|| exited_notional / exited_quantity),
        exited_quantity,
        price_pnl_usdt: exits.iter().filter_map(|exit| exit.price_pnl_usdt).sum(),
        fee_usdt: entry_fee_usdt + exits.iter().map(|exit| exit.fee_usdt).sum::<Decimal>(),
        exits,
    })
}
