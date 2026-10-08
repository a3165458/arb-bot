//! 杠杆与强平：资金费套利真正的死法。
//!
//! 双腿对冲掉的是**价格方向**，对冲不掉的是**保证金**：价格单边走 20%，一条腿赚 20%、
//! 另一条腿亏 20%，合起来是零 —— 但亏的那条腿在自己的场所里被强平了，赚的那条腿
//! 留下来变成裸敞口。所以杠杆决定的不是收益率，而是「价格能走多远，这笔对冲还在」。
//!
//! # 强平价（逐仓）
//!
//! 按 Hyperliquid 官方公式（`side` 多 = 1、空 = −1，`l` = 维持保证金率）：
//!
//! ```text
//! liq_price = price − side × margin_available / position_size / (1 − l × side)
//! margin_available = margin − notional × l
//! ```
//!
//! 代入 `q = notional / entry` 化简：
//!
//! ```text
//! 多：liq = (q·entry − margin) / (q·(1 − l))
//! 空：liq = (q·entry + margin) / (q·(1 + l))
//! ```
//!
//! 开仓时 `margin = notional / leverage`，强平距离只跟杠杆与维持保证金率有关：
//!
//! ```text
//! 多：1 − (1 − 1/L) / (1 − l)
//! 空：(1 + 1/L) / (1 + l) − 1
//! ```
//!
//! 只用 `1/L` 当强平距离会把强平价算远：5 倍、维持保证金 1% 时是 20% 对 18.8%。
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
//!
//! 全仓的强平价取决于整个账户的权益，而那是账户数据；这里一律按逐仓算，
//! 对全仓账户是**保守**的（全仓的其它余额只会让强平更远）。
//!
//! # 健康度
//!
//! 强平距离 ≥ 20% 健康、≥ 8% 注意、更近是危险。

use arb_core::{Decimal, MarketSnapshot, Side, Venue};
use serde::Serialize;

use crate::rank::Opportunity;

/// 强平距离达到这个百分比算健康。
pub const HEALTHY_DISTANCE_PCT: Decimal = Decimal::from_parts(20, 0, 0, false, 0);
/// 强平距离达到这个百分比算注意，更近是危险。
pub const CAUTION_DISTANCE_PCT: Decimal = Decimal::from_parts(8, 0, 0, false, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Health {
    Healthy,
    Caution,
    Danger,
}

impl Health {
    pub fn of(distance_pct: Decimal) -> Self {
        if distance_pct >= HEALTHY_DISTANCE_PCT {
            Health::Healthy
        } else if distance_pct >= CAUTION_DISTANCE_PCT {
            Health::Caution
        } else {
            Health::Danger
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Health::Healthy => "healthy",
            Health::Caution => "caution",
            Health::Danger => "danger",
        }
    }
}

/// 维持保证金率必须落在 `[0, 1)`：1 以上意味着开仓即强平，是坏数据。
fn valid_mmr(mmr: Decimal) -> bool {
    mmr >= Decimal::ZERO && mmr < Decimal::ONE
}

/// 逐仓强平价。`side` 为 `Buy` = 多腿、`Sell` = 空腿。
///
/// `notional` 是**入场名义**（数量 × 入场价），`margin` 是这条腿占用的逐仓保证金。
/// 多腿保证金足以扛到价格归零时没有强平价，返回 `None`。
pub fn liquidation_price(
    entry: Decimal,
    notional: Decimal,
    margin: Decimal,
    mmr: Decimal,
    side: Side,
) -> Option<Decimal> {
    if entry <= Decimal::ZERO || notional <= Decimal::ZERO || margin < Decimal::ZERO {
        return None;
    }
    if !valid_mmr(mmr) {
        return None;
    }
    let quantity = notional.checked_div(entry)?;
    let price = match side {
        Side::Buy => (notional - margin).checked_div(quantity * (Decimal::ONE - mmr))?,
        Side::Sell => (notional + margin).checked_div(quantity * (Decimal::ONE + mmr))?,
    };
    (price > Decimal::ZERO).then_some(price)
}

/// 开仓当时的强平距离（%）。只和杠杆、维持保证金率有关。
pub fn liquidation_distance_pct(leverage: Decimal, mmr: Decimal, side: Side) -> Option<Decimal> {
    if leverage <= Decimal::ZERO || !valid_mmr(mmr) {
        return None;
    }
    let margin_ratio = Decimal::ONE.checked_div(leverage)?;
    let distance = match side {
        Side::Buy => Decimal::ONE - (Decimal::ONE - margin_ratio) / (Decimal::ONE - mmr),
        Side::Sell => (Decimal::ONE + margin_ratio) / (Decimal::ONE + mmr) - Decimal::ONE,
    };
    Some(settle_pct(distance * Decimal::ONE_HUNDRED))
}

/// 百分比舍到 10 位小数、不低于 0。
///
/// `1/3` 这类除法在第 28 位上有末位误差：3 倍、维持保证金 1/6 的理论距离正好是 20%，
/// 不舍入就是 19.9999…，显示成 20.00% 却落进「注意」档。
fn settle_pct(value: Decimal) -> Decimal {
    value.round_dp(10).max(Decimal::ZERO)
}

/// 当前价到强平价的距离（%）。价格已经越过强平价时是 0。
pub fn distance_to_liquidation_pct(
    mark: Decimal,
    liquidation: Decimal,
    side: Side,
) -> Option<Decimal> {
    if mark <= Decimal::ZERO || liquidation <= Decimal::ZERO {
        return None;
    }
    let gap = match side {
        Side::Buy => mark - liquidation,
        Side::Sell => liquidation - mark,
    };
    Some(settle_pct(gap / mark * Decimal::ONE_HUNDRED))
}

/// 把强平距离从 `current_pct` 拉回 `target_pct` 需要减掉的仓位比例（0~1）。
///
/// 假设减仓时这条腿的**权益留在逐仓保证金里**：减掉部分的已实现盈亏计入保证金，
/// 不提走、也不追加。于是减仓只缩小名义、权益不变，按当前价算的保证金率
/// `n = 权益 / 当前名义` 变大，强平价远离。
/// 由距离反解保证金率（多：`n = l + d·(1 − l)`；空：`n = (1 + l)(1 + d) − 1`），
/// 减仓比例 `f = 1 − n_now / n_target`，整理后：
///
/// ```text
/// f = i·(t − e) / (i·t + 100·l)，i = 1 − l（多）或 1 + l（空），e/t 为百分比
/// ```
///
/// 已经达到目标时是 0；结果夹在 `[0, 1]`。
pub fn trim_fraction(
    current_pct: Decimal,
    target_pct: Decimal,
    mmr: Decimal,
    side: Side,
) -> Option<Decimal> {
    if current_pct < Decimal::ZERO || target_pct <= Decimal::ZERO || !valid_mmr(mmr) {
        return None;
    }
    if current_pct >= target_pct {
        return Some(Decimal::ZERO);
    }
    let i = match side {
        Side::Buy => Decimal::ONE - mmr,
        Side::Sell => Decimal::ONE + mmr,
    };
    let denominator = i * target_pct + Decimal::ONE_HUNDRED * mmr;
    if denominator <= Decimal::ZERO {
        return None;
    }
    let fraction = i * (target_pct - current_pct) / denominator;
    Some(fraction.clamp(Decimal::ZERO, Decimal::ONE))
}

/// 把一条腿的强平距离拉到 `target_pct`（%）需要的**逐仓保证金总额**（不是补多少）。
///
/// 是 [`liquidation_price`] 的反解：先由目标距离定出强平价
/// （多：`mark × (1 − t)`，空：`mark × (1 + t)`），再解保证金：
///
/// ```text
/// 多：M = N − liq·q·(1 − l)        空：M = liq·q·(1 + l) − N
/// ```
///
/// `N` 是入场名义，`q = N / entry`，`l` 是维持保证金率。多腿的目标强平价为负（距离 ≥ 100%）
/// 时返回 `None`；结果不会低于 0。调用方拿它减去当前保证金就是要补的钱。
pub fn margin_for_distance(
    entry: Decimal,
    notional: Decimal,
    mark: Decimal,
    mmr: Decimal,
    side: Side,
    target_pct: Decimal,
) -> Option<Decimal> {
    if entry <= Decimal::ZERO
        || notional <= Decimal::ZERO
        || mark <= Decimal::ZERO
        || target_pct <= Decimal::ZERO
        || !valid_mmr(mmr)
    {
        return None;
    }
    let quantity = notional.checked_div(entry)?;
    let t = target_pct / Decimal::ONE_HUNDRED;
    let margin = match side {
        Side::Buy => {
            let liquidation = mark.checked_mul(Decimal::ONE - t)?;
            if liquidation <= Decimal::ZERO {
                return None;
            }
            notional - liquidation * quantity * (Decimal::ONE - mmr)
        }
        Side::Sell => {
            let liquidation = mark.checked_mul(Decimal::ONE + t)?;
            liquidation * quantity * (Decimal::ONE + mmr) - notional
        }
    };
    Some(margin.max(Decimal::ZERO))
}

/// 一条腿在给定杠杆下的风险。
#[derive(Debug, Clone, Serialize)]
pub struct LegRisk {
    pub venue: Venue,
    /// 场所最低档允许的最大杠杆。`None` = 批量接口不给，**不知道请求的杠杆能不能开**。
    pub max_leverage: Option<Decimal>,
    /// 实际按多少倍算：请求值，超过场所上限时压到上限。
    pub leverage: Decimal,
    /// 请求的杠杆超过了场所上限，被压低了。
    pub leverage_capped: bool,
    pub maintenance_margin: Option<Decimal>,
    /// 开仓当时的强平距离（%）。缺维持保证金率时为 `None`，不拿典型值顶上。
    pub liq_distance_pct: Option<Decimal>,
    pub health: Option<Health>,
}

/// 一笔双腿在给定杠杆下的风险与保证金收益率。
#[derive(Debug, Clone, Serialize)]
pub struct PairRisk {
    /// 请求的杠杆。
    pub leverage: Decimal,
    pub long: LegRisk,
    pub short: LegRisk,
    /// 两腿都能开到的最大杠杆。任一腿不知道上限时为 `None`。
    pub max_pair_leverage: Option<Decimal>,
    /// 两腿里更近的那个强平距离（%）：先被强平的那条腿决定这笔对冲能扛多远。
    /// 任一腿算不出来就是 `None` —— 只看算得出的那条会把风险看轻。
    pub liq_distance_pct: Option<Decimal>,
    pub health: Option<Health>,
    /// 保证金年化：摊费后净年化 × 名义 ÷ 两腿保证金合计。
    ///
    /// 等腿名义时是 `funding_apr / (1/L多 + 1/L空)`，两腿同为 L 倍即 `funding_apr × L / 2`。
    /// 假设基差不变、两腿都不被强平、没有借贷成本 —— 它说明「同样的钱摆在保证金里
    /// 能收多少」，不是收益预测。
    pub margin_apr: Decimal,
}

fn leg_risk(snapshot: &MarketSnapshot, requested: Decimal, side: Side) -> LegRisk {
    let max_leverage = snapshot.max_leverage.filter(|max| *max > Decimal::ZERO);
    let leverage = max_leverage.map_or(requested, |max| requested.min(max));
    let liq_distance_pct = snapshot
        .maintenance_margin
        .and_then(|mmr| liquidation_distance_pct(leverage, mmr, side));
    LegRisk {
        venue: snapshot.venue,
        max_leverage,
        leverage,
        leverage_capped: leverage < requested,
        maintenance_margin: snapshot.maintenance_margin,
        liq_distance_pct,
        health: liq_distance_pct.map(Health::of),
    }
}

/// 按请求的杠杆评估一笔配对。`long` / `short` 必须是这笔机会两条腿的快照。
pub fn pair_risk(
    opportunity: &Opportunity,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    leverage: Decimal,
) -> PairRisk {
    let requested = leverage.max(Decimal::ONE);
    let long_risk = leg_risk(long, requested, Side::Buy);
    let short_risk = leg_risk(short, requested, Side::Sell);

    let liq_distance_pct = long_risk
        .liq_distance_pct
        .zip(short_risk.liq_distance_pct)
        .map(|(a, b)| a.min(b));
    let margin_per_notional =
        Decimal::ONE / long_risk.leverage + Decimal::ONE / short_risk.leverage;
    PairRisk {
        leverage: requested,
        max_pair_leverage: long_risk
            .max_leverage
            .zip(short_risk.max_leverage)
            .map(|(a, b)| a.min(b)),
        liq_distance_pct,
        health: liq_distance_pct.map(Health::of),
        margin_apr: opportunity.funding_apr / margin_per_notional,
        long: long_risk,
        short: short_risk,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn opening_distance_matches_the_official_formula_not_one_over_leverage() {
        // 5 倍、维持 1%：空腿 1.2/1.01 − 1 = 18.81%，不是 20%
        let short = liquidation_distance_pct(dec!(5), dec!(0.01), Side::Sell).unwrap();
        assert_eq!(short.round_dp(2), dec!(18.81));
        // 多腿 1 − 0.8/0.99 = 19.19%
        let long = liquidation_distance_pct(dec!(5), dec!(0.01), Side::Buy).unwrap();
        assert_eq!(long.round_dp(2), dec!(19.19));
        // 默认 3 倍仍在健康档
        let three = liquidation_distance_pct(dec!(3), dec!(0.01), Side::Sell).unwrap();
        assert_eq!(Health::of(three), Health::Healthy);
        assert_eq!(Health::of(short), Health::Caution);
        // 3 倍、维持 1/6（Hyperliquid 最高 3 倍的合约）的多腿理论距离正好是 20%
        let edge = liquidation_distance_pct(dec!(3), Decimal::ONE / dec!(6), Side::Buy).unwrap();
        assert_eq!(edge, dec!(20));
        assert_eq!(
            Health::of(edge),
            Health::Healthy,
            "末位误差不能把边界值挤进下一档"
        );
    }

    #[test]
    fn liquidation_price_agrees_with_the_opening_distance() {
        // 入场 100、名义 1000、5 倍 → 保证金 200
        let long =
            liquidation_price(dec!(100), dec!(1000), dec!(200), dec!(0.01), Side::Buy).unwrap();
        let short =
            liquidation_price(dec!(100), dec!(1000), dec!(200), dec!(0.01), Side::Sell).unwrap();
        let long_gap = distance_to_liquidation_pct(dec!(100), long, Side::Buy).unwrap();
        let short_gap = distance_to_liquidation_pct(dec!(100), short, Side::Sell).unwrap();
        assert_eq!(
            long_gap.round_dp(10),
            liquidation_distance_pct(dec!(5), dec!(0.01), Side::Buy)
                .unwrap()
                .round_dp(10)
        );
        assert_eq!(
            short_gap.round_dp(10),
            liquidation_distance_pct(dec!(5), dec!(0.01), Side::Sell)
                .unwrap()
                .round_dp(10)
        );
        // 1 倍多腿、维持保证金 0：扛到价格归零，没有强平价
        assert_eq!(
            liquidation_price(dec!(100), dec!(1000), dec!(1000), dec!(0), Side::Buy),
            None
        );
        // 价格已经穿过强平价 → 距离 0，而不是负数
        assert_eq!(
            distance_to_liquidation_pct(dec!(70), long, Side::Buy),
            Some(Decimal::ZERO)
        );
    }

    #[test]
    fn trimming_by_the_fraction_restores_the_target_distance() {
        // 价格已经朝不利方向走了 4%：只有在价格偏离入场价时，「已实现盈亏留在保证金里」
        // 这个假设才会影响结果，入场价上测不出来。
        for (side, mark) in [(Side::Buy, dec!(96)), (Side::Sell, dec!(104))] {
            let mmr = dec!(0.02);
            let (entry, notional, margin) = (dec!(100), dec!(1000), dec!(150));
            let liq = liquidation_price(entry, notional, margin, mmr, side).unwrap();
            let now = distance_to_liquidation_pct(mark, liq, side).unwrap();
            let target = now * dec!(1.5);
            let fraction = trim_fraction(now, target, mmr, side).unwrap();
            assert!(fraction > Decimal::ZERO && fraction < Decimal::ONE);

            // 名义按比例缩小，减掉部分的已实现盈亏计入保证金，重算的距离应当正好是目标
            let quantity = notional / entry;
            let sign = if side == Side::Buy {
                Decimal::ONE
            } else {
                -Decimal::ONE
            };
            let realized = quantity * fraction * (mark - entry) * sign;
            let trimmed = notional * (Decimal::ONE - fraction);
            let new_liq = liquidation_price(entry, trimmed, margin + realized, mmr, side).unwrap();
            let after = distance_to_liquidation_pct(mark, new_liq, side).unwrap();
            assert_eq!(after.round_dp(8), target.round_dp(8), "{side:?}");
        }
        assert_eq!(
            trim_fraction(dec!(30), dec!(20), dec!(0.01), Side::Sell),
            Some(Decimal::ZERO)
        );
        assert_eq!(trim_fraction(dec!(10), dec!(20), dec!(1), Side::Sell), None);
    }

    fn snapshot(venue: Venue, max: Option<Decimal>, mmr: Option<Decimal>) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: arb_core::Symbol::perp("BTC", "USDT"),
            period_rate: Decimal::ZERO,
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: chrono::Utc::now(),
            next_funding_estimated: true,
            taker_fee: None,
            mark_price: Some(dec!(100)),
            index_price: Some(dec!(100)),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: max,
            maintenance_margin: mmr,
            oi_capped: false,
        }
    }

    fn opportunity(funding_apr: Decimal) -> Opportunity {
        Opportunity {
            symbol: arb_core::Symbol::perp("BTC", "USDT"),
            long: Venue::Lighter,
            short: Venue::Hyperliquid,
            long_interval_h: 1,
            short_interval_h: 1,
            long_daily: Decimal::ZERO,
            short_daily: Decimal::ZERO,
            daily_spread: Decimal::ZERO,
            apr: funding_apr,
            round_trip_fee: Decimal::ZERO,
            round_trip_spread: None,
            round_trip_cost: Decimal::ZERO,
            spread_unknown: true,
            funding_daily: Decimal::ZERO,
            funding_apr,
            spread_net: Decimal::ZERO,
            spread_hold_days: dec!(3),
            hold_measured: false,
            entry_basis_pct: None,
            executable_basis_pct: None,
            quote_mismatch: false,
            oi_capped: false,
        }
    }

    #[test]
    fn the_pair_is_as_safe_as_its_nearest_liquidation_and_capped_by_the_venue() {
        let long = snapshot(Venue::Lighter, Some(dec!(50)), Some(dec!(0.012)));
        let short = snapshot(
            Venue::HyperliquidIo,
            Some(dec!(6)),
            Some(dec!(1) / dec!(12)),
        );
        let risk = pair_risk(&opportunity(dec!(0.2)), &long, &short, dec!(10));

        assert!(risk.short.leverage_capped, "io 最多 6 倍");
        assert_eq!(risk.short.leverage, dec!(6));
        assert_eq!(risk.long.leverage, dec!(10));
        assert_eq!(risk.max_pair_leverage, Some(dec!(6)));
        assert_eq!(
            risk.liq_distance_pct,
            risk.long.liq_distance_pct.min(risk.short.liq_distance_pct)
        );
        // 20% × 1 / (1/10 + 1/6) = 75%
        assert_eq!(risk.margin_apr.round_dp(6), dec!(0.75));
    }

    #[test]
    fn an_unknown_maintenance_margin_leaves_the_pair_distance_unknown() {
        let long = snapshot(Venue::Binance, None, None);
        let short = snapshot(Venue::Hyperliquid, Some(dec!(40)), Some(dec!(0.0125)));
        let risk = pair_risk(&opportunity(dec!(0.1)), &long, &short, dec!(3));
        assert_eq!(risk.long.liq_distance_pct, None);
        assert!(risk.short.liq_distance_pct.is_some());
        assert_eq!(
            risk.liq_distance_pct, None,
            "只看算得出的那条腿会把风险看轻"
        );
        assert_eq!(risk.health, None);
        assert_eq!(risk.max_pair_leverage, None);
        assert!(!risk.long.leverage_capped, "不知道上限就不假装压过");
        // 两腿同为 3 倍：10% × 3 / 2
        assert_eq!(risk.margin_apr.round_dp(6), dec!(0.15));
    }

    /// 反解必须是 [`liquidation_price`] / [`distance_to_liquidation_pct`] 的逆：补到算出来的保证金，
    /// 距离正好是目标（多空各一遍，价格离开入场价之后也成立）。
    #[test]
    fn margin_for_distance_inverts_the_liquidation_formula() {
        for (side, mark) in [
            (Side::Buy, dec!(92)),
            (Side::Sell, dec!(108)),
            (Side::Sell, dec!(100)),
        ] {
            let (entry, notional, mmr) = (dec!(100), dec!(1000), dec!(0.0133334));
            let target = dec!(15);
            let needed = margin_for_distance(entry, notional, mark, mmr, side, target).unwrap();
            let liquidation = liquidation_price(entry, notional, needed, mmr, side).unwrap();
            let distance = distance_to_liquidation_pct(mark, liquidation, side).unwrap();
            assert_eq!(distance.round_dp(6), target, "{side:?} mark={mark}");
        }
        // 价格已经往对自己不利的方向走了：同样的目标要补得更多。
        let at_entry = margin_for_distance(
            dec!(100),
            dec!(1000),
            dec!(100),
            dec!(0.01),
            Side::Sell,
            dec!(15),
        )
        .unwrap();
        let after_rally = margin_for_distance(
            dec!(100),
            dec!(1000),
            dec!(108),
            dec!(0.01),
            Side::Sell,
            dec!(15),
        )
        .unwrap();
        assert!(after_rally > at_entry);
        // 垃圾输入不给答案。
        let bad = |mmr, side, entry| {
            margin_for_distance(entry, dec!(1000), dec!(100), mmr, side, dec!(15))
        };
        assert!(bad(dec!(1), Side::Sell, dec!(100)).is_none());
        assert!(bad(dec!(0.01), Side::Buy, dec!(0)).is_none());
        assert!(
            margin_for_distance(
                dec!(100),
                dec!(1000),
                dec!(100),
                dec!(0.01),
                Side::Buy,
                dec!(100)
            )
            .is_none()
        );
    }
}
