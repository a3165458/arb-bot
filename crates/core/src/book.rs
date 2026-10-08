//! 盘口深度与吃单估算。
//!
//! 与 [`crate::MarketSnapshot`] 的分工：快照给的是**一档**最优买卖价（批量端点就有），
//! 用来算穿价成本；这里的 [`OrderBook`] 给的是**多档**深度，用来估算**仓位相关的**
//! 滑点。两者的量的单位统一是**计价币名义**，否则下游算滑点时单位会错。
//!
//! 所有场所的深度端点都是**逐合约**的，没有批量版本。所以深度只在需要时对少数候选
//! 调用（见 `arb-scanner` 的深度体检），不要在整轮扫描里对近千个合约逐个拉。

use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};

/// 买还是卖。滑点的方向取决于它，不能省。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

/// 盘口的一档。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Level {
    pub price: Decimal,
    /// 该档位的**计价币名义**（不是币数量、不是张数）。
    pub notional_usdt: Decimal,
}

/// 一根 K 线的收盘价。
///
/// 只要收盘价：基差的均值回归半衰期是拿收盘序列拟合的，用开高低收里的其它值
/// 只会引入噪声，而噪声会让半衰期看起来更短。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Candle {
    /// 这根 K 线的**开盘**时刻（UTC）。用它而不是收盘时刻，是为了让相邻两点的
    /// 间隔恰好等于周期 —— 缺 K 线时那个间隔会变大，这正是我们要保留的信息。
    pub open_time: chrono::DateTime<chrono::Utc>,
    pub close: Decimal,
}

/// 一个整点结算周期的资金费率（每小时的小数，正 = 多头付给空头）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct FundingPoint {
    /// 这次结算的时刻（UTC）。
    pub at: chrono::DateTime<chrono::Utc>,
    /// 这一小时的费率。
    pub rate: Decimal,
}

/// 一个合约的盘口深度快照。
#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderBook {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 买盘，从最优买价开始**价格降序**。
    pub bids: Vec<Level>,
    /// 卖盘，从最优卖价开始**价格升序**。
    pub asks: Vec<Level>,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|level| level.price)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|level| level.price)
    }

    /// 相对买卖价差（小数）。与 [`crate::MarketSnapshot::relative_spread`] 同口径，
    /// 便于交叉核对「批量快照的一档价」与「逐合约深度的一档价」是否一致。
    pub fn relative_spread(&self) -> Option<Decimal> {
        let (bid, ask) = (self.best_bid()?, self.best_ask()?);
        if bid <= Decimal::ZERO || ask < bid {
            return None;
        }
        Some((ask - bid) / ((bid + ask) / Decimal::TWO))
    }

    /// 指定一侧的档位（已按可成交顺序排列）。
    pub fn side(&self, side: Side) -> &[Level] {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }
}

/// 按盘口逐档吃单的估算结果。
#[derive(Debug, Clone, serde::Serialize)]
pub struct FillEstimate {
    /// 实际能吃到的名义额（吃穿所有档位时小于目标）。
    pub filled_usdt: Decimal,
    /// 成交均价。
    pub average_price: Decimal,
    /// 相对**最优价**的不利滑点（小数，正数 = 比最优价差）。
    ///
    /// 这是「超出穿价成本的那部分」：穿价成本算的是从最优价成交，
    /// 这里算的是仓位大到要吃第二档、第三档之后多付的部分。
    pub slippage: Decimal,
    /// 是否吃穿了全部档位（`filled_usdt < notional_usdt`）。
    pub exhausted: bool,
}

/// 估算一笔市价单的成交均价与滑点。
///
/// `levels` 必须是**可成交顺序**的那一侧（买看 `asks` 升序、卖看 `bids` 降序），
/// 单位是计价币名义。
///
/// 返回 `None` 当：盘口为空、最优价非正、或目标名义额非正 ——
/// 「估不出来」和「滑点为零」是两回事，不能混。
pub fn estimate_fill(levels: &[Level], notional_usdt: Decimal, side: Side) -> Option<FillEstimate> {
    estimate_fill_limited(levels, notional_usdt, side, None)
}

/// 带限价的吃单估算：只吃**价格不劣于限价**的档位。
///
/// 双腿套利应当尽量用限价：市价在薄盘上会把滑点直接变成亏损，而限价最多是不成交
/// （可以重试或放弃）。所以「限价能吃到多少」是执行前必须回答的问题。
pub fn estimate_fill_limited(
    levels: &[Level],
    notional_usdt: Decimal,
    side: Side,
    limit_price: Option<Decimal>,
) -> Option<FillEstimate> {
    let best = levels.first()?.price;
    if best <= Decimal::ZERO || notional_usdt <= Decimal::ZERO {
        return None;
    }

    let mut remaining = notional_usdt;
    let mut filled_usdt = Decimal::ZERO;
    let mut filled_qty = Decimal::ZERO;
    for level in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        if level.price <= Decimal::ZERO || level.notional_usdt <= Decimal::ZERO {
            // 单档坏数据不该毁掉整次估算：跳过它，继续吃下一档。
            continue;
        }
        // 买单：价格高于限价的档位吃不到；卖单：价格低于限价的吃不到。
        if let Some(limit) = limit_price {
            let acceptable = match side {
                Side::Buy => level.price <= limit,
                Side::Sell => level.price >= limit,
            };
            if !acceptable {
                break;
            }
        }
        let take = remaining.min(level.notional_usdt);
        let quantity = take / level.price;
        filled_usdt += take;
        filled_qty += quantity;
        remaining -= take;
    }

    if filled_qty <= Decimal::ZERO {
        return None;
    }
    // 除法会产生几十位小数，直接落盘/展示会变成一串噪声。
    // 价格收到 8 位、滑点收到 10 位，都远超任何交易所的精度。
    let average_price = (filled_usdt / filled_qty).round_dp(8);
    // 买单：均价高于最优卖价就是不利；卖单：均价低于最优买价就是不利。
    let slippage = match side {
        Side::Buy => (average_price - best) / best,
        Side::Sell => (best - average_price) / best,
    }
    .round_dp(10);

    Some(FillEstimate {
        filled_usdt,
        average_price,
        slippage,
        exhausted: filled_usdt < notional_usdt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn level(price: Decimal, notional: Decimal) -> Level {
        Level {
            price,
            notional_usdt: notional,
        }
    }

    #[test]
    fn a_size_within_the_first_level_pays_the_best_price() {
        let asks = vec![
            level(dec!(100), dec!(10_000)),
            level(dec!(101), dec!(50_000)),
        ];
        let fill = estimate_fill(&asks, dec!(5_000), Side::Buy).unwrap();
        assert_eq!(fill.average_price, dec!(100));
        assert_eq!(fill.slippage, Decimal::ZERO, "没吃穿一档就不该有额外滑点");
        assert_eq!(fill.filled_usdt, dec!(5_000));
        assert!(!fill.exhausted);
    }

    #[test]
    fn eating_into_the_second_level_pays_more_on_average() {
        // 一档只有 1 万，目标 2 万 → 一半吃在一档、一半吃在二档
        let asks = vec![
            level(dec!(100), dec!(10_000)),
            level(dec!(101), dec!(50_000)),
        ];
        let fill = estimate_fill(&asks, dec!(20_000), Side::Buy).unwrap();
        assert_eq!(fill.filled_usdt, dec!(20_000));
        // 10000/100 + 10000/101 = 100 + 99.0099… = 199.0099… 份
        // 均价 = 20000 / 199.0099… ≈ 100.4975124378…，收尾到 8 位
        assert_eq!(fill.average_price, dec!(100.49751244));
        assert!(fill.slippage > Decimal::ZERO);
        assert!(!fill.exhausted);
    }

    #[test]
    fn selling_walks_the_bids_downwards() {
        let bids = vec![
            level(dec!(100), dec!(10_000)),
            level(dec!(99), dec!(50_000)),
        ];
        let fill = estimate_fill(&bids, dec!(20_000), Side::Sell).unwrap();
        assert!(fill.average_price < dec!(100), "卖单均价应当低于最优买价");
        assert!(fill.slippage > Decimal::ZERO, "卖方滑点也应当是正的不利值");
    }

    #[test]
    fn an_order_larger_than_the_whole_book_is_flagged_as_exhausted() {
        let asks = vec![level(dec!(100), dec!(1_000))];
        let fill = estimate_fill(&asks, dec!(5_000), Side::Buy).unwrap();
        assert_eq!(fill.filled_usdt, dec!(1_000), "只吃到盘口里有的部分");
        assert!(fill.exhausted, "必须标出来，否则会被当成「全部成交」");
    }

    #[test]
    fn a_broken_level_is_skipped_instead_of_killing_the_estimate() {
        let asks = vec![
            level(dec!(100), dec!(1_000)),
            level(Decimal::ZERO, dec!(999_999)), // 坏档
            level(dec!(102), dec!(50_000)),
        ];
        let fill = estimate_fill(&asks, dec!(2_000), Side::Buy).unwrap();
        assert_eq!(fill.filled_usdt, dec!(2_000));
        assert!(fill.average_price > dec!(100));
    }

    #[test]
    fn an_empty_or_invalid_book_yields_none_not_zero_slippage() {
        assert!(estimate_fill(&[], dec!(1_000), Side::Buy).is_none());
        let bad = vec![level(Decimal::ZERO, dec!(1_000))];
        assert!(estimate_fill(&bad, dec!(1_000), Side::Buy).is_none());
        let asks = vec![level(dec!(100), dec!(1_000))];
        assert!(estimate_fill(&asks, Decimal::ZERO, Side::Buy).is_none());
        assert!(estimate_fill(&asks, dec!(-5), Side::Buy).is_none());
    }

    #[test]
    fn a_limit_order_only_eats_the_levels_within_its_price() {
        let asks = vec![
            level(dec!(100), dec!(1_000)),
            level(dec!(105), dec!(50_000)),
        ];
        // 限价 101：只吃得到一档
        let fill = estimate_fill_limited(&asks, dec!(20_000), Side::Buy, Some(dec!(101))).unwrap();
        assert_eq!(fill.filled_usdt, dec!(1_000), "二档 105 超过限价，吃不到");
        assert!(fill.exhausted, "没吃满目标就必须标出来");

        // 限价 110：两档都吃得到
        let fill = estimate_fill_limited(&asks, dec!(20_000), Side::Buy, Some(dec!(110))).unwrap();
        assert_eq!(fill.filled_usdt, dec!(20_000));
        assert!(!fill.exhausted);
    }

    #[test]
    fn a_sell_limit_stops_at_the_floor() {
        let bids = vec![level(dec!(100), dec!(1_000)), level(dec!(95), dec!(50_000))];
        let fill = estimate_fill_limited(&bids, dec!(20_000), Side::Sell, Some(dec!(99))).unwrap();
        assert_eq!(fill.filled_usdt, dec!(1_000), "95 低于限价，吃不到");
    }

    #[test]
    fn relative_spread_matches_the_snapshot_convention() {
        let book = OrderBook {
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![level(dec!(99), dec!(1_000))],
            asks: vec![level(dec!(101), dec!(1_000))],
        };
        // (101 − 99) / 100 = 2%
        assert_eq!(book.relative_spread(), Some(dec!(0.02)));
        assert_eq!(book.best_bid(), Some(dec!(99)));
        assert_eq!(book.best_ask(), Some(dec!(101)));
    }
}
