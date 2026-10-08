//! 外部平仓的实际盈亏：按交易所的成交记录事后核算。
//!
//! 仓位在交易所被手动平掉时，看板没有平仓成交，台账里只有开仓。这里拿交易所自己的成交
//! 记录补上：每条腿开仓价（台账）、平仓成交（交易所）、手续费（开仓 + 平仓，每笔只计一次），
//! 再加持仓期间的资金费流水。
//!
//! **只在数量逐腿严格对得上时才归因。** 成交记录是按（账户, 合约）查的，同一账户在同一合约上
//! 别的交易（另一笔仓位、加仓、反向开仓）也在里面。所以窗口里必须：开仓方向的成交总量等于
//! 台账这条腿的数量、平仓方向的成交总量也等于它（误差 0.1%，容许取整），且没有混进反向
//! 开仓 / 同向平仓。任何一条不满足就返回原因，调用方如实记「无法核算」—— 绝不猜。

use arb_core::{Decimal, Side, Venue};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::types::{EntryLegs, LegFill};

/// 一笔成交是开仓还是平仓（场所给了就用，没给是 `Unknown`，只能靠方向和数量核对）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillEffect {
    Open,
    Close,
    Unknown,
}

/// 交易所记录的一笔成交。
#[derive(Debug, Clone)]
pub struct VenueFill {
    pub at: DateTime<Utc>,
    pub side: Side,
    /// 标的数量（正数）。
    pub quantity: Decimal,
    pub price: Decimal,
    /// 手续费（计价币，返佣为负）。
    pub fee_usdt: Decimal,
    pub effect: FillEffect,
}

/// 数量对得上的容许误差：台账数量是名义 ÷ 均价反推的，交易所有最小步长取整。
const QUANTITY_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 3);

/// 一条腿的核算结果。
#[derive(Debug, Clone, Serialize)]
pub struct LegSettlement {
    pub venue: Venue,
    pub side: Side,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    /// 平仓成交的加权均价。
    pub exit_price: Decimal,
    /// (平仓价 − 开仓价) × 数量，空腿取反。
    pub price_pnl_usdt: Decimal,
    /// 开仓手续费（台账里已付的）加平仓手续费，各计一次。
    pub fees_usdt: Decimal,
}

/// 两条腿合起来的核算结果。
#[derive(Debug, Clone, Serialize)]
pub struct Settlement {
    pub long: LegSettlement,
    pub short: LegSettlement,
}

impl Settlement {
    pub fn price_pnl_usdt(&self) -> Decimal {
        self.long.price_pnl_usdt + self.short.price_pnl_usdt
    }

    pub fn fees_usdt(&self) -> Decimal {
        self.long.fees_usdt + self.short.fees_usdt
    }

    /// 一句话：平仓价、价格盈亏、手续费、资金费与净额。
    pub fn describe(&self, funding: Option<Decimal>) -> String {
        let usdt = |value: Decimal| value.round_dp(4).normalize().to_string();
        let net = self.price_pnl_usdt() - self.fees_usdt() + funding.unwrap_or_default();
        let closing = |leg: &LegSettlement| {
            format!(
                "{} {} {}",
                leg.venue,
                match leg.side {
                    Side::Buy => "卖出",
                    Side::Sell => "买回",
                },
                leg.exit_price.round_dp(8).normalize()
            )
        };
        format!(
            "实际盈亏 {} USDT{}：价格 {}、手续费 −{}、资金费 {}；平仓成交 {}、{}（按交易所成交记录核算）",
            usdt(net),
            if funding.is_none() {
                "（不含资金费）"
            } else {
                ""
            },
            usdt(self.price_pnl_usdt()),
            usdt(self.fees_usdt()),
            funding.map_or("没查到".to_string(), usdt),
            closing(&self.long),
            closing(&self.short),
        )
    }
}

/// 核算一条腿。`fills` 是这个账户在这个合约上开仓以来的成交（可以混着别的），`from` / `until`
/// 圈出这笔仓位存在的时间。
pub fn settle_leg(
    leg: &LegFill,
    fills: &[VenueFill],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<LegSettlement, String> {
    let venue = leg.venue;
    let quantity = leg
        .quantity()
        .filter(|quantity| *quantity > Decimal::ZERO)
        .ok_or_else(|| format!("{venue} 台账里这条腿缺数量"))?;
    let closing = match leg.side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    };
    let window: Vec<&VenueFill> = fills
        .iter()
        .filter(|fill| fill.at >= from && fill.at <= until)
        .collect();
    // 反向开仓 / 同向平仓：说明这个窗口里有不属于这笔仓位的交易。
    if window
        .iter()
        .any(|fill| fill.side == closing && fill.effect == FillEffect::Open)
        || window
            .iter()
            .any(|fill| fill.side == leg.side && fill.effect == FillEffect::Close)
    {
        return Err(format!(
            "{venue} 的成交记录里有反向开仓或同向平仓，混进了别的交易，没法归到这笔仓位"
        ));
    }
    let sum = |side: Side| -> Decimal {
        window
            .iter()
            .filter(|fill| fill.side == side)
            .map(|fill| fill.quantity)
            .sum()
    };
    let close_to = |actual: Decimal| (actual - quantity).abs() <= quantity * QUANTITY_TOLERANCE;
    let entered = sum(leg.side);
    if !close_to(entered) {
        return Err(format!(
            "{venue} 成交记录里的开仓数量 {} 与台账的 {} 对不上，可能混进了别的开仓",
            entered.normalize(),
            quantity.round_dp(8).normalize()
        ));
    }
    let exited = sum(closing);
    if !close_to(exited) {
        return Err(format!(
            "{venue} 成交记录里的平仓数量 {} 与台账的 {} 对不上（只平了一部分、或多平了）",
            exited.normalize(),
            quantity.round_dp(8).normalize()
        ));
    }
    let exit_fills: Vec<&&VenueFill> = window.iter().filter(|fill| fill.side == closing).collect();
    let exit_notional: Decimal = exit_fills
        .iter()
        .map(|fill| fill.quantity * fill.price)
        .sum();
    let exit_price = exit_notional / exited;
    let price_pnl = match leg.side {
        Side::Buy => exit_notional - exited * leg.average_price,
        Side::Sell => exited * leg.average_price - exit_notional,
    };
    let exit_fees: Decimal = exit_fills.iter().map(|fill| fill.fee_usdt).sum();
    Ok(LegSettlement {
        venue,
        side: leg.side,
        quantity: exited,
        entry_price: leg.average_price,
        exit_price,
        price_pnl_usdt: price_pnl,
        fees_usdt: leg.fee_usdt + exit_fees,
    })
}

/// 核算整笔仓位：两条腿都要对得上，缺一条就整笔不记。
pub fn settle(
    entry: &EntryLegs,
    long_fills: &[VenueFill],
    short_fills: &[VenueFill],
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Result<Settlement, String> {
    Ok(Settlement {
        long: settle_leg(&entry.long, long_fills, from, until)?,
        short: settle_leg(&entry.short, short_fills, from, until)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClientOrderId;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 30, 6, 22, second).unwrap()
    }

    fn leg(venue: Venue, side: Side, price: Decimal, fee: Decimal) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: price * dec!(1797.9),
            average_price: price,
            fee_usdt: fee,
            client_order_id: ClientOrderId::for_leg("p", side, 0),
            margin_usdt: None,
        }
    }

    fn fill(
        sec: u32,
        side: Side,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
        effect: FillEffect,
    ) -> VenueFill {
        VenueFill {
            at: at(sec),
            side,
            quantity,
            price,
            fee_usdt: fee,
            effect,
        }
    }

    /// 2026-09-30 实盘 PONS：Arcus 空腿 1797.9 @ 0.5547（吃单费 0.224391404），被手动分两笔
    /// 挂单买回：35.4258 + 1762.4742 @ 0.56456，做市成交免手续费。交易所自己给的 closedPnl
    /// 是 −0.349298388 与 −17.377995612，合计 −17.727294。
    fn arcus_fills() -> Vec<VenueFill> {
        vec![
            fill(
                2,
                Side::Sell,
                dec!(1797.9),
                dec!(0.5547),
                dec!(0.224391404),
                FillEffect::Open,
            ),
            fill(
                50,
                Side::Buy,
                dec!(35.4258),
                dec!(0.56456),
                dec!(0),
                FillEffect::Close,
            ),
            fill(
                51,
                Side::Buy,
                dec!(1762.4742),
                dec!(0.56456),
                dec!(0),
                FillEffect::Close,
            ),
        ]
    }

    #[test]
    fn the_pons_arcus_leg_matches_the_venues_own_closed_pnl() {
        let short = leg(Venue::Arcus, Side::Sell, dec!(0.5547), dec!(0.224391404));
        let settled = settle_leg(&short, &arcus_fills(), at(0), at(59)).unwrap();
        // (0.5547 − 0.56456) × 1797.9 = −17.727294，与交易所 closedPnl 之和一致。
        assert_eq!(settled.price_pnl_usdt, dec!(-17.727294));
        assert_eq!(
            settled.price_pnl_usdt,
            dec!(-0.349298388) + dec!(-17.377995612)
        );
        assert_eq!(settled.exit_price, dec!(0.56456));
        // 开仓手续费只计一次（台账里的 0.224391404），平仓是做市免费。
        assert_eq!(settled.fees_usdt, dec!(0.224391404));
        assert_eq!(settled.quantity, dec!(1797.9));
    }

    #[test]
    fn a_pair_adds_up_both_legs_and_funding_into_one_sentence() {
        let entry = EntryLegs {
            long: leg(Venue::LighterRh, Side::Buy, dec!(0.55507), Decimal::ZERO),
            short: leg(Venue::Arcus, Side::Sell, dec!(0.5547), dec!(0.224391404)),
        };
        // 多腿在 Lighter 以 0.5645 卖出：赚 (0.5645 − 0.55507) × 1797.9。
        let long_fills = vec![
            fill(
                2,
                Side::Buy,
                dec!(1797.9),
                dec!(0.55507),
                dec!(0),
                FillEffect::Unknown,
            ),
            fill(
                52,
                Side::Sell,
                dec!(1797.9),
                dec!(0.5645),
                dec!(0),
                FillEffect::Unknown,
            ),
        ];
        let settled = settle(&entry, &long_fills, &arcus_fills(), at(0), at(59)).unwrap();
        // (0.5645 − 0.55507) × 1797.9 = 16.954197
        assert_eq!(settled.long.price_pnl_usdt, dec!(16.954197));
        assert_eq!(settled.price_pnl_usdt(), dec!(16.954197) + dec!(-17.727294));
        assert_eq!(settled.fees_usdt(), dec!(0.224391404));
        let text = settled.describe(Some(dec!(0.1686)));
        assert!(
            text.contains("实际盈亏") && text.contains("资金费 0.1686"),
            "{text}"
        );
        assert!(
            text.contains("lighter-rh 卖出 0.5645") && text.contains("arcus 买回 0.56456"),
            "{text}"
        );
    }

    #[test]
    fn unrelated_trades_in_the_window_make_the_leg_unattributable() {
        let short = leg(Venue::Arcus, Side::Sell, dec!(0.5547), dec!(0.224391404));
        // 窗口里多了一笔别的开空：开仓数量对不上。
        let mut extra = arcus_fills();
        extra.push(fill(
            30,
            Side::Sell,
            dec!(100),
            dec!(0.56),
            dec!(0.01),
            FillEffect::Open,
        ));
        let error = settle_leg(&short, &extra, at(0), at(59)).unwrap_err();
        assert!(
            error.contains("开仓数量") && error.contains("对不上"),
            "{error}"
        );
        // 只平了一部分。
        let partial = vec![
            fill(
                2,
                Side::Sell,
                dec!(1797.9),
                dec!(0.5547),
                dec!(0.22),
                FillEffect::Open,
            ),
            fill(
                50,
                Side::Buy,
                dec!(35.4258),
                dec!(0.56456),
                dec!(0),
                FillEffect::Close,
            ),
        ];
        let error = settle_leg(&short, &partial, at(0), at(59)).unwrap_err();
        assert!(error.contains("平仓数量"), "{error}");
        // 多平了（把别的仓位也平了）。
        let mut over = arcus_fills();
        over.push(fill(
            55,
            Side::Buy,
            dec!(500),
            dec!(0.57),
            dec!(0),
            FillEffect::Close,
        ));
        assert!(settle_leg(&short, &over, at(0), at(59)).is_err());
        // 反向开仓混进来。
        let mut flipped = arcus_fills();
        flipped.push(fill(
            56,
            Side::Buy,
            dec!(10),
            dec!(0.57),
            dec!(0),
            FillEffect::Open,
        ));
        let error = settle_leg(&short, &flipped, at(0), at(59)).unwrap_err();
        assert!(error.contains("反向开仓"), "{error}");
    }

    #[test]
    fn fills_outside_the_position_window_are_ignored() {
        let short = leg(Venue::Arcus, Side::Sell, dec!(0.5547), dec!(0.224391404));
        let mut fills = arcus_fills();
        // 开仓之前、平仓之后别的交易不算。
        fills.push(fill(
            0,
            Side::Sell,
            dec!(999),
            dec!(0.5),
            dec!(1),
            FillEffect::Open,
        ));
        let settled = settle_leg(&short, &fills, at(1), at(55)).unwrap();
        assert_eq!(settled.price_pnl_usdt, dec!(-17.727294));
    }

    #[test]
    fn rounding_dust_in_the_quantity_is_tolerated() {
        let long = leg(Venue::LighterRh, Side::Buy, dec!(0.55507), Decimal::ZERO);
        let fills = vec![
            fill(
                2,
                Side::Buy,
                dec!(1797.9),
                dec!(0.55507),
                dec!(0),
                FillEffect::Unknown,
            ),
            fill(
                50,
                Side::Sell,
                dec!(1797.85),
                dec!(0.5645),
                dec!(0),
                FillEffect::Unknown,
            ),
        ];
        assert!(settle_leg(&long, &fills, at(0), at(59)).is_ok());
    }
}
