//! 资金费差稳不稳：看两腿最近的逐小时费率，而不是此刻的一个读数。
//!
//! 排名与开仓用的是「当前费率」。一个几小时前才冒出来的高费差常常是短时的，开仓后没多久
//! 就反转（多头开始付、空头开始收），这时收益归零、还白付了往返成本。所以开仓前要
//! 回看历史：过去 24 小时费差（空腿费率 − 多腿费率）的均值、为正的小时占比，以及最近
//! 6 小时的均值，三者都过关才算稳定。持仓后的「费差自动平仓」也用最近几小时的均值，
//! 免得一次瞬时波动就把仓位平掉。
//!
//! 只支持逐小时结算的场所（Hyperliquid 系、Lighter 系、Arcus）：两边的点按整点对齐，
//! 对不上的小时不参与统计。历史不足时返回原因而不是当成稳定。

use std::collections::BTreeMap;

use arb_core::{Decimal, FundingPoint};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// 评估窗口（小时）与至少要对齐上多少个点。
pub const WINDOW_HOURS: i64 = 24;
pub const MIN_POINTS: usize = 12;
/// 最近窗口（小时）与至少要对齐上多少个点。
pub const RECENT_HOURS: i64 = 6;
pub const MIN_RECENT_POINTS: usize = 3;
/// 过去 24 小时里费差为正的小时至少占多少。
pub const MIN_POSITIVE_SHARE: Decimal = Decimal::from_parts(6, 0, 0, false, 1);
/// 取历史时多取几个小时，给对齐留余量。
pub const FETCH_HOURS: u32 = 30;

/// 一次评估的结果。
#[derive(Debug, Clone, Serialize)]
pub struct FundingStability {
    /// 参与统计的整点数（24 小时内两腿都有读数的）。
    pub points: usize,
    /// 过去 24 小时费差均值的年化（小数）。
    pub mean_apr_24h: Decimal,
    /// 过去 24 小时里费差为正的小时占比（0 ~ 1）。
    pub positive_share: Decimal,
    /// 最近 6 小时费差均值的年化；点数不够时为 `None`。
    pub mean_apr_6h: Option<Decimal>,
    pub stable: bool,
    /// 不稳定的原因。
    pub reason: Option<String>,
}

fn apr_of(hourly: Decimal) -> Decimal {
    hourly * Decimal::from(24 * 365)
}

fn bucket(at: DateTime<Utc>) -> i64 {
    at.timestamp().div_euclid(3600)
}

/// 两腿按整点对齐后的费差序列（空腿 − 多腿），升序。
pub fn diff_series(long: &[FundingPoint], short: &[FundingPoint]) -> Vec<(i64, Decimal)> {
    let by_hour = |points: &[FundingPoint]| -> BTreeMap<i64, Decimal> {
        points
            .iter()
            .map(|point| (bucket(point.at), point.rate))
            .collect()
    };
    let (long, short) = (by_hour(long), by_hour(short));
    short
        .iter()
        .filter_map(|(hour, short_rate)| Some((*hour, *short_rate - *long.get(hour)?)))
        .collect()
}

fn mean(values: impl Iterator<Item = Decimal>) -> Option<Decimal> {
    let (sum, count) = values.fold((Decimal::ZERO, 0u32), |(sum, count), v| {
        (sum + v, count + 1)
    });
    (count > 0).then(|| sum / Decimal::from(count))
}

/// 最近 `hours` 小时费差均值的年化。对齐的点少于 `min_points` 时返回 `None`。
pub fn recent_diff_apr(
    long: &[FundingPoint],
    short: &[FundingPoint],
    now: DateTime<Utc>,
    hours: i64,
    min_points: usize,
) -> Option<Decimal> {
    let from = bucket(now) - hours;
    let window: Vec<Decimal> = diff_series(long, short)
        .into_iter()
        .filter(|(hour, _)| *hour > from)
        .map(|(_, diff)| diff)
        .collect();
    (window.len() >= min_points)
        .then(|| mean(window.into_iter()).map(apr_of))
        .flatten()
}

/// 评估这对腿的费差稳不稳。历史不足（对齐的点太少）返回 `Err(原因)`。
pub fn assess(
    long: &[FundingPoint],
    short: &[FundingPoint],
    now: DateTime<Utc>,
) -> Result<FundingStability, String> {
    let from = bucket(now) - WINDOW_HOURS;
    let series: Vec<(i64, Decimal)> = diff_series(long, short)
        .into_iter()
        .filter(|(hour, _)| *hour > from)
        .collect();
    if series.len() < MIN_POINTS {
        return Err(format!(
            "两腿最近 {WINDOW_HOURS} 小时只对齐上 {} 个整点的资金费读数（至少要 {MIN_POINTS} 个），历史不够判断稳不稳",
            series.len()
        ));
    }
    let mean_hourly = mean(series.iter().map(|(_, diff)| *diff)).unwrap_or_default();
    let positive = series
        .iter()
        .filter(|(_, diff)| *diff > Decimal::ZERO)
        .count();
    let positive_share = Decimal::from(positive) / Decimal::from(series.len());
    let mean_apr_24h = apr_of(mean_hourly);
    let mean_apr_6h = recent_diff_apr(long, short, now, RECENT_HOURS, MIN_RECENT_POINTS);

    let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(1).normalize();
    let mut reasons = Vec::new();
    if mean_apr_24h <= Decimal::ZERO {
        reasons.push(format!(
            "过去 24 小时费差均值年化 {}%，不为正",
            pct(mean_apr_24h)
        ));
    }
    if positive_share < MIN_POSITIVE_SHARE {
        reasons.push(format!(
            "过去 24 小时只有 {}% 的小时费差为正（至少 {}%）",
            pct(positive_share),
            pct(MIN_POSITIVE_SHARE)
        ));
    }
    if let Some(recent) = mean_apr_6h
        && recent <= Decimal::ZERO
    {
        reasons.push(format!(
            "最近 {RECENT_HOURS} 小时费差均值年化 {}%，已经反转",
            pct(recent)
        ));
    }
    Ok(FundingStability {
        points: series.len(),
        mean_apr_24h: mean_apr_24h.round_dp(4),
        positive_share: positive_share.round_dp(3),
        mean_apr_6h: mean_apr_6h.map(|apr| apr.round_dp(4)),
        stable: reasons.is_empty(),
        reason: (!reasons.is_empty()).then(|| reasons.join("；")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 30, 12, 30, 0).unwrap()
    }

    /// 从 `now` 往前每小时一个点，`rates[0]` 是最近的一小时。
    fn series(rates: &[Decimal]) -> Vec<FundingPoint> {
        let mut points: Vec<FundingPoint> = rates
            .iter()
            .enumerate()
            .map(|(back, rate)| FundingPoint {
                at: Utc.with_ymd_and_hms(2026, 9, 30, 12, 0, 0).unwrap()
                    - chrono::Duration::hours(back as i64)
                    + chrono::Duration::milliseconds(58),
                rate: *rate,
            })
            .collect();
        points.reverse();
        points
    }

    fn flat(rate: Decimal, hours: usize) -> Vec<FundingPoint> {
        series(&vec![rate; hours])
    }

    #[test]
    fn a_steady_positive_spread_is_stable() {
        let stability = assess(&flat(dec!(0.00001), 24), &flat(dec!(0.00005), 24), now()).unwrap();
        assert!(stability.stable, "{:?}", stability.reason);
        assert_eq!(stability.points, 24);
        assert_eq!(stability.positive_share, dec!(1));
        // 每小时 0.00004 × 24 × 365 = 0.3504
        assert_eq!(stability.mean_apr_24h, dec!(0.3504));
        assert_eq!(stability.mean_apr_6h, Some(dec!(0.3504)));
    }

    #[test]
    fn a_spread_that_just_reversed_is_not_stable_even_if_the_day_average_is_positive() {
        // 前 18 小时费差 +0.0001，最近 6 小时反转成 −0.00005：24 小时均值仍为正，但已经反转。
        let mut long = vec![dec!(0.00005); 6];
        long.extend(vec![dec!(0); 18]);
        let mut short = vec![dec!(0); 6];
        short.extend(vec![dec!(0.0001); 18]);
        let stability = assess(&series(&long), &series(&short), now()).unwrap();
        assert!(stability.mean_apr_24h > Decimal::ZERO);
        assert!(!stability.stable);
        assert!(stability.reason.unwrap().contains("已经反转"));
    }

    #[test]
    fn a_choppy_spread_that_is_positive_only_now_and_then_is_not_stable() {
        // 只有 1/3 的小时为正，但正的那几小时很大，均值为正。
        let long: Vec<Decimal> = (0..24).map(|_| dec!(0)).collect();
        let short: Vec<Decimal> = (0..24)
            .map(|i| {
                if i % 3 == 0 {
                    dec!(0.0004)
                } else {
                    dec!(-0.00005)
                }
            })
            .collect();
        let stability = assess(&series(&long), &series(&short), now()).unwrap();
        assert!(stability.mean_apr_24h > Decimal::ZERO);
        assert!(!stability.stable);
        assert!(stability.reason.unwrap().contains("小时费差为正"));
    }

    #[test]
    fn too_little_aligned_history_is_reported_instead_of_assumed_stable() {
        let error = assess(&flat(dec!(0.00001), 24), &flat(dec!(0.00005), 5), now()).unwrap_err();
        assert!(error.contains("历史不够"), "{error}");
    }

    #[test]
    fn only_hours_both_legs_have_are_counted() {
        // 多腿缺最近 3 小时：这 3 小时不参与统计。
        let long = series(&[dec!(0.00001); 24])[..21].to_vec();
        let series_len = diff_series(&long, &flat(dec!(0.00005), 24)).len();
        assert_eq!(series_len, 21);
    }

    #[test]
    fn recent_average_needs_enough_points() {
        let long = flat(dec!(0.00001), 24);
        let short = flat(dec!(0.00003), 24);
        // 每小时 0.00002 × 8760 = 0.1752
        assert_eq!(
            recent_diff_apr(&long, &short, now(), 6, 3),
            Some(dec!(0.1752))
        );
        assert_eq!(recent_diff_apr(&long, &short[..2], now(), 6, 3), None);
    }

    #[test]
    fn millisecond_jitter_still_lands_in_the_same_hour() {
        // Hyperliquid 的时间戳带毫秒抖动，Lighter / Arcus 在整点：按整点对齐。
        let mut jittered = flat(dec!(0.00001), 24);
        jittered[3].at += chrono::Duration::milliseconds(900);
        assert_eq!(diff_series(&jittered, &flat(dec!(0.00003), 24)).len(), 24);
    }
}
