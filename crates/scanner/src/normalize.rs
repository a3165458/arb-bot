//! 费率归一化。
//!
//! 交易所给的资金费是**每结算周期**的费率，周期从 1h 到 8h 不等（同一家场所内部
//! 也可能不同）。任何跨场所比较都必须先归一化到同一个时间基准，否则 1h 的场所
//! 会被系统性低估 8 倍，而排名照常算得出来。

use arb_core::{DEFAULT_FUNDING_INTERVAL_H, Decimal};

/// 把每期费率折算成日化费率。
///
/// `interval_h` 为 0 时回落到 [`DEFAULT_FUNDING_INTERVAL_H`]：0 会让除法溢出，
/// 而溢出会 panic 掉整轮扫描 —— 一个坏字段不该带走全部数据。
pub fn to_daily(period_rate: Decimal, interval_h: u32) -> Decimal {
    let hours = if interval_h == 0 {
        DEFAULT_FUNDING_INTERVAL_H
    } else {
        interval_h
    };
    period_rate * Decimal::from(24u32) / Decimal::from(hours)
}

/// 日化费率折算成年化（单利，与 coinglass / loris 的口径一致）。
pub fn to_apr(daily_rate: Decimal) -> Decimal {
    daily_rate * Decimal::from(365u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn normalizes_across_settlement_intervals() {
        // 同一个 0.01% 每期，1h 场所的日化是 8h 场所的 8 倍
        assert_eq!(to_daily(dec!(0.0001), 1), dec!(0.0024));
        assert_eq!(to_daily(dec!(0.0001), 4), dec!(0.0006));
        assert_eq!(to_daily(dec!(0.0001), 8), dec!(0.0003));
        assert_eq!(
            to_daily(dec!(0.0001), 1),
            to_daily(dec!(0.0001), 8) * dec!(8)
        );
    }

    #[test]
    fn zero_interval_falls_back_instead_of_dividing_by_zero() {
        assert_eq!(
            to_daily(dec!(0.0001), 0),
            to_daily(dec!(0.0001), DEFAULT_FUNDING_INTERVAL_H)
        );
    }

    #[test]
    fn negative_rates_survive_normalization() {
        assert_eq!(to_daily(dec!(-0.0001), 1), dec!(-0.0024));
    }

    #[test]
    fn apr_is_daily_times_365() {
        assert_eq!(to_apr(dec!(0.001)), dec!(0.365));
    }
}
