//! 读数可信度筛查。
//!
//! 交易所的数据质量会撒谎 —— 不是恶意，而是偶发返回 0、返回上一个结算周期的
//! 陈旧值、或对冷门合约返回明显不合理的极端值。这类读数会**凭空造出巨大价差**
//! 并排在榜首，而照着下单得到的是相反的结果。
//!
//! 策略是：**标记，但不隐藏**。可疑读数照常展示（带原因），只是不参与配对。
//! 悄悄丢掉比标出来更危险 —— 用户会以为自己看到的是全部数据。

use std::collections::HashMap;

use arb_core::{Decimal, MarketSnapshot, Venue};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use crate::normalize::to_daily;

/// 超过中位数多少个稳健标准差算异常。
///
/// 8σ 是很松的门槛：只有明显不属于同一分布的值才会被标记，正常的跨场所费率差异
/// 不会被误伤 —— 那正是我们要找的机会。
const SIGMA_K: f64 = 8.0;

/// 绝对下限（日化百分比）。
///
/// 单独的 σ 判据在样本少时会失效：MAD 被离群值本身撑大。加一个绝对下限后，
/// 「某家报 3%/天、其余报 0.02%/天」仍会被抓住，而正常差异（0.01~0.05%/天）不会。
const FLOOR_PCT: f64 = 0.5;

/// 少于这么多家场所时不做离群判定：中位数与 MAD 都没有统计意义。
const MIN_SAMPLES: usize = 3;

/// 可疑读数的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Suspicion {
    /// 与该币种内其它场所的读数严重不一致。
    OutlierFromPeers {
        daily_pct: Decimal,
        median_pct: Decimal,
    },
}

impl std::fmt::Display for Suspicion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Suspicion::OutlierFromPeers {
                daily_pct,
                median_pct,
            } => write!(
                f,
                "与同币种其它场所差异过大：日化 {daily_pct}% vs 中位数 {median_pct}%"
            ),
        }
    }
}

/// 键：`(场所, 合约显示名)`。
pub type SuspicionMap = HashMap<(Venue, String), Suspicion>;

/// 找出同一币种内明显离群的读数。
///
/// # 这个判据**发现不了**什么
///
/// 单次快照无法区分「场所偶发返回 0」与「该合约费率确实接近 0」—— 两者在横截面上
/// 长得一样。要抓前者需要**跨轮次**的一致性检查（同一场所对同一合约在几秒内反复
/// 返回 0 与真值），那属于 `--watch` 模式的工作。这里不假装能做到。
pub fn flag_outliers(rates: &[MarketSnapshot]) -> SuspicionMap {
    let mut flagged = SuspicionMap::new();
    if rates.len() < MIN_SAMPLES {
        return flagged;
    }

    let daily_pcts: Vec<Decimal> = rates
        .iter()
        .map(|rate| to_daily(rate.period_rate, rate.interval_h) * Decimal::from(100u32))
        .collect();

    let mut values: Vec<f64> = daily_pcts
        .iter()
        .map(|value| value.to_f64().unwrap_or(0.0))
        .collect();
    let Some(center) = median(&mut values) else {
        return flagged;
    };
    let mut deviations: Vec<f64> = values.iter().map(|value| (value - center).abs()).collect();
    // MAD → 标准差的一致估计（正态分布下 σ ≈ 1.4826 × MAD）
    let sigma = 1.4826 * median(&mut deviations).unwrap_or(0.0);
    let threshold = (SIGMA_K * sigma).max(FLOOR_PCT);

    let median_pct = Decimal::from_f64(center).unwrap_or_default().round_dp(6);

    for (rate, daily_pct) in rates.iter().zip(daily_pcts) {
        let value = daily_pct.to_f64().unwrap_or(0.0);
        if (value - center).abs() > threshold {
            flagged.insert(
                (rate.venue, rate.symbol.to_string()),
                Suspicion::OutlierFromPeers {
                    daily_pct: daily_pct.round_dp(6),
                    median_pct,
                },
            );
        }
    }
    flagged
}

/// 按是否可疑把读数分成两堆。可疑的那堆仍然要展示，只是不参与配对。
pub fn split_trustworthy<'a>(
    rates: &'a [MarketSnapshot],
    flagged: &SuspicionMap,
) -> (Vec<&'a MarketSnapshot>, Vec<&'a MarketSnapshot>) {
    rates
        .iter()
        .partition(|rate| !flagged.contains_key(&(rate.venue, rate.symbol.to_string())))
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = values.len();
    Some(if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Symbol;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn rate(venue: Venue, period_rate: Decimal, interval_h: u32) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp("TEST", "USDT"),
            period_rate,
            interval_h,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: false,
            taker_fee: None,
            mark_price: Some(dec!(1)),
            index_price: Some(dec!(1)),
            best_bid: Some(dec!(100)),
            best_ask: Some(dec!(100)),
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: None,
            maintenance_margin: None,
            oi_capped: false,
        }
    }

    #[test]
    fn a_wildly_off_reading_is_flagged() {
        let rates = vec![
            rate(Venue::Binance, dec!(0.0002), 8),
            rate(Venue::Okx, dec!(0.0002), 8),
            rate(Venue::Bybit, dec!(0.0002), 8),
            // 日化 3% —— 明显不属于同一分布
            rate(Venue::Mexc, dec!(0.01), 8),
        ];
        let flagged = flag_outliers(&rates);
        assert_eq!(flagged.len(), 1);
        assert!(flagged.contains_key(&(Venue::Mexc, "TEST/USDT".to_string())));
    }

    #[test]
    fn normal_cross_venue_dispersion_is_not_flagged() {
        let rates = vec![
            rate(Venue::Binance, dec!(0.0002), 8),
            rate(Venue::Okx, dec!(0.00025), 8),
            rate(Venue::Bybit, dec!(0.00015), 8),
            rate(Venue::Gate, dec!(0.0003), 8),
        ];
        assert!(flag_outliers(&rates).is_empty());
    }

    #[test]
    fn comparison_happens_on_daily_values_not_on_raw_period_rates() {
        // 三个 8h 场所每期 0.0001（日化 0.03%），一个 1h 场所每期 0.02（日化 4.8%）。
        //
        // 这个用例能把两种实现区分开：按**原始每期费率**比较时，0.02 与 0.0001 的
        // 差距是 0.0199，低于 0.5 的绝对下限 → 不会报警；按**日化**比较时差距是
        // 4.77 → 报警。周期没归一化的实现会在这里静默通过。
        let rates = vec![
            rate(Venue::Binance, dec!(0.0001), 8),
            rate(Venue::Okx, dec!(0.0001), 8),
            rate(Venue::Bybit, dec!(0.0001), 8),
            rate(Venue::Hyperliquid, dec!(0.02), 1),
        ];
        let flagged = flag_outliers(&rates);
        assert!(flagged.contains_key(&(Venue::Hyperliquid, "TEST/USDT".to_string())));
    }

    #[test]
    fn a_merely_high_but_plausible_rate_is_not_flagged() {
        // 归一化后的正常差异不该被误伤 —— 那正是我们要找的机会。
        // 1h 场所日化 0.24% 对 8h 场所 0.03%，是 8 倍差距但仍在可信范围内。
        let rates = vec![
            rate(Venue::Binance, dec!(0.0001), 8),
            rate(Venue::Okx, dec!(0.0001), 8),
            rate(Venue::Bybit, dec!(0.0001), 8),
            rate(Venue::Hyperliquid, dec!(0.0001), 1),
        ];
        assert!(flag_outliers(&rates).is_empty());
    }

    #[test]
    fn two_venues_are_never_flagged() {
        let rates = vec![
            rate(Venue::Binance, dec!(0.0002), 8),
            rate(Venue::Mexc, dec!(0.02), 8),
        ];
        assert!(flag_outliers(&rates).is_empty());
    }

    #[test]
    fn split_partitions_without_dropping_rows() {
        let rates = vec![
            rate(Venue::Binance, dec!(0.0002), 8),
            rate(Venue::Mexc, dec!(0.01), 8),
        ];
        let flagged = flag_outliers(&rates);
        let (trusted, dropped) = split_trustworthy(&rates, &flagged);
        assert_eq!(trusted.len() + dropped.len(), rates.len());
    }
}
