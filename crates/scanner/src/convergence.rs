//! 基差的均值回归半衰期。
//!
//! 价差套利榜原先用「计划持有 3 天」折算年化 —— 那是拍脑袋的。这里改成**实测**：
//! 取两腿的收盘价序列，算出基差走掉一半需要多久，用它当持有期。
//!
//! # 口径
//!
//! 把基差序列当成一阶自回归（等价于离散的 OU 过程）：
//!
//! ```text
//! Δb_t = α + β · b_{t-1} + ε
//! ```
//!
//! `β < 0` 才是均值回归；`φ = 1 + β` 是每期保留的比例，
//! 半衰期 = `−ln2 / ln(φ)`（单位：采样间隔）。
//!
//! # 这里为什么可以用 f64
//!
//! 回归系数是**统计量**，不是金额也不是费率：它没有「必须精确到分」的语义，
//! 用 f64 做最小二乘是标准做法。算完之后**一次性**转成 `Decimal` 交出去 ——
//! 那之后的年化折算仍然全程 `Decimal`。
//!
//! # 什么时候拒绝给答案
//!
//! 半衰期是个很容易被算出来的数：样本太少、没有均值回归（β ≥ 0）、或者拟合
//! 质量差到没有意义时，硬给一个数会让年化看起来精确而实际是噪声。这些情况一律
//! 返回 `None`，由调用方回落到配置值并**如实标记**。

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use arb_core::{Candle, Symbol, Venue};
use arb_venues::VenueApi;

use crate::scan::ScanReport;

/// 基差序列上的一点。
#[derive(Debug, Clone, Copy)]
pub struct BasisPoint {
    pub at: DateTime<Utc>,
    /// 基差（小数，不是百分数）。
    pub basis: Decimal,
}

/// 半衰期估计。
#[derive(Debug, Clone, serde::Serialize)]
pub struct HalfLife {
    /// 半衰期（天）。
    pub days: Decimal,
    /// AR(1) 的 β。负值 = 均值回归。
    pub beta: Decimal,
    /// 拟合优度 R²。金融序列通常很低，但**太低就说明这个拟合没有解释力**。
    pub r_squared: Decimal,
    /// 参与拟合的样本数。
    pub samples: usize,
    /// 采样间隔（分钟）。
    pub interval_minutes: u32,
}

/// 最少样本数。再少的话 β 的方差大到没有意义。
const MIN_SAMPLES: usize = 30;

/// R² 下限。低于它说明「上一刻的基差」几乎解释不了「下一刻的变化」，
/// 那么由 β 推出来的半衰期也只是噪声。
const MIN_R_SQUARED: f64 = 0.02;

/// 把两腿的收盘价对齐成基差序列。
///
/// 按**时刻**对齐而不是按下标：交易所会缺 K 线（无成交的那根直接不返回），
/// 按下标对齐会把缺口两边的价格错配到一起，造出并不存在的基差跳变。
///
/// 基差 = `(空腿价 − 多腿价) / 中间价`，与 [`crate::rank`] 的口径一致。
pub fn basis_series(long: &[Candle], short: &[Candle]) -> Vec<BasisPoint> {
    let mut out = Vec::new();
    let mut short_iter = short.iter().peekable();
    for long_candle in long {
        // 两个序列都按升序，所以可以用归并式推进。
        while let Some(next) = short_iter.peek() {
            if next.open_time < long_candle.open_time {
                short_iter.next();
            } else {
                break;
            }
        }
        let Some(short_candle) = short_iter.peek() else {
            break;
        };
        if short_candle.open_time != long_candle.open_time {
            continue;
        }
        let (long_price, short_price) = (long_candle.close, short_candle.close);
        if long_price <= Decimal::ZERO || short_price <= Decimal::ZERO {
            continue;
        }
        let mid = (long_price + short_price) / Decimal::TWO;
        out.push(BasisPoint {
            at: long_candle.open_time,
            basis: (short_price - long_price) / mid,
        });
    }
    out
}

/// 从基差序列拟合半衰期。
pub fn half_life(series: &[BasisPoint], interval_minutes: u32) -> Option<HalfLife> {
    if series.len() < MIN_SAMPLES || interval_minutes == 0 {
        return None;
    }

    // Δb_t = α + β·b_{t-1}
    let xs: Vec<f64> = series[..series.len() - 1]
        .iter()
        .map(|point| point.basis.to_f64().unwrap_or(f64::NAN))
        .collect();
    let ys: Vec<f64> = series
        .windows(2)
        .map(|pair| {
            let previous = pair[0].basis.to_f64().unwrap_or(f64::NAN);
            let current = pair[1].basis.to_f64().unwrap_or(f64::NAN);
            current - previous
        })
        .collect();
    if xs.iter().chain(ys.iter()).any(|value| !value.is_finite()) {
        return None;
    }

    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;

    let mut sxx = 0.0;
    let mut sxy = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        sxx += (x - mean_x) * (x - mean_x);
        sxy += (x - mean_x) * (y - mean_y);
    }
    if sxx <= f64::EPSILON {
        // 基差在整个窗口里几乎不变 → 没有可拟合的变动。
        return None;
    }
    let beta = sxy / sxx;

    // 只有负的 β 才是均值回归。β ≥ 0 说明基差在发散或随机游走 ——
    // 「收敛时间」这个概念在这里不成立，硬算会得到一个负数或无穷。
    if beta >= 0.0 || beta <= -1.0 {
        return None;
    }
    let phi = 1.0 + beta;
    let intervals = -std::f64::consts::LN_2 / phi.ln();
    if !intervals.is_finite() || intervals <= 0.0 {
        return None;
    }

    // R²：拟合对「下一刻的变化」解释了多少。
    let alpha = mean_y - beta * mean_x;
    let mut ss_res = 0.0;
    let mut ss_tot = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        let predicted = alpha + beta * x;
        ss_res += (y - predicted) * (y - predicted);
        ss_tot += (y - mean_y) * (y - mean_y);
    }
    let r_squared = if ss_tot <= f64::EPSILON {
        0.0
    } else {
        1.0 - ss_res / ss_tot
    };
    if r_squared < MIN_R_SQUARED {
        return None;
    }

    let minutes = intervals * interval_minutes as f64;
    let days = minutes / (60.0 * 24.0);

    Some(HalfLife {
        days: Decimal::from_f64(days)?.round_dp(4),
        beta: Decimal::from_f64(beta)?.round_dp(6),
        r_squared: Decimal::from_f64(r_squared)?.round_dp(6),
        samples: series.len(),
        interval_minutes,
    })
}

/// 往下最多再看这么多条。每条入选的候选要打两次 K 线，不能为了凑满 `top` 把榜单翻完。
const MAX_CANDLE_SCAN: usize = 160;

/// 从已按价差净收益排好序的配对里，挑出两腿都有公开 K 线的前 `top` 条。
///
/// 没有 K 线的场所不占名额。价差榜前排经常是 Lighter / Variational，按榜单前 N 条
/// 去拉会把预算打在必然失败的请求上，后面真正能拟合的配对根本轮不到。
pub fn select_candle_candidates<T>(
    ranked: &[T],
    venues: impl Fn(&T) -> (Venue, Venue),
    supports: impl Fn(Venue) -> bool,
    top: usize,
    max_examined: usize,
) -> Vec<&T> {
    if top == 0 || max_examined == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for item in ranked.iter().take(max_examined) {
        let (long, short) = venues(item);
        if supports(long) && supports(short) {
            out.push(item);
            if out.len() == top {
                break;
            }
        }
    }
    out
}

fn max_candle_scan(top: usize) -> usize {
    top.saturating_mul(8).clamp(top, MAX_CANDLE_SCAN)
}

/// 对榜单里的候选配对**实测**基差半衰期。
///
/// 只对两腿都有公开 K 线的前 `top` 条做：K 线端点是逐合约的，对近千个合约拉等于上千次请求。
/// 每条候选需要两腿各一次请求（同一腿会被缓存复用）。
///
/// 拿不到历史、或拟合被拒绝的合约不会让整件事失败 —— 那些合约只是没有实测值，
/// 排名会回落到配置的持有期并**如实标记**。
pub async fn measure_holds(
    apis: &std::collections::HashMap<Venue, std::sync::Arc<dyn VenueApi>>,
    report: &ScanReport,
    top: usize,
    interval_minutes: u32,
    limit: u32,
) -> std::collections::HashMap<(Symbol, Venue, Venue), HalfLife> {
    use std::collections::HashMap;

    // 1) 价差视角按基差净收益排序，再跳过任一一腿没有公开 K 线的配对。
    let mut ranked: Vec<&crate::rank::Opportunity> = report
        .symbols
        .iter()
        .flat_map(|view| view.spread.iter())
        .collect();
    ranked.sort_by_key(|op| std::cmp::Reverse(op.spread_net));
    let supports = |venue: Venue| apis.get(&venue).is_some_and(|api| api.supports_candles());
    let candidates = select_candle_candidates(
        &ranked,
        |op| (op.long, op.short),
        supports,
        top,
        max_candle_scan(top),
    );

    // 2) 去重出需要拉取的 (场所, 合约)。
    let mut wanted: Vec<(Venue, Symbol)> = Vec::new();
    for op in &candidates {
        for venue in [op.long, op.short] {
            let key = (venue, op.symbol.clone());
            if !wanted.contains(&key) {
                wanted.push(key);
            }
        }
    }

    // 3) 并发拉取。单个失败只影响它自己。
    let mut set = tokio::task::JoinSet::new();
    for (venue, symbol) in wanted {
        let Some(api) = apis.get(&venue).cloned() else {
            continue;
        };
        set.spawn(async move {
            let result = api.fetch_candles(&symbol, interval_minutes, limit).await;
            (venue, symbol, result)
        });
    }
    let mut series: HashMap<(Venue, Symbol), Vec<Candle>> = HashMap::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((venue, symbol, Ok(candles))) => {
                series.insert((venue, symbol), candles);
            }
            Ok((venue, symbol, Err(error))) => {
                tracing::debug!(%venue, %symbol, %error, "拿不到 K 线，该合约没有实测持有期");
            }
            Err(join_error) => tracing::warn!(%join_error, "K 线任务异常退出"),
        }
    }

    // 4) 按这一对腿拟合半衰期。同一合约的另一对场所是另一个基差过程，不能共用。
    let mut holds: HashMap<(Symbol, Venue, Venue), HalfLife> = HashMap::new();
    for op in candidates {
        let key = (op.symbol.clone(), op.long, op.short);
        if holds.contains_key(&key) {
            continue;
        }
        let Some(long) = series.get(&(op.long, op.symbol.clone())) else {
            continue;
        };
        let Some(short) = series.get(&(op.short, op.symbol.clone())) else {
            continue;
        };
        let points = basis_series(long, short);
        if let Some(estimate) = half_life(&points, interval_minutes) {
            holds.insert(key, estimate);
        }
    }
    holds
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use rust_decimal_macros::dec;

    fn series(values: &[f64], interval_minutes: u32) -> Vec<BasisPoint> {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        values
            .iter()
            .enumerate()
            .map(|(index, value)| BasisPoint {
                at: start + TimeDelta::minutes((index as i64) * interval_minutes as i64),
                basis: Decimal::from_f64(*value).unwrap(),
            })
            .collect()
    }

    /// 生成一段严格按 `phi` 衰减的基差序列（无噪声）。
    fn decaying(phi: f64, n: usize) -> Vec<f64> {
        let mut out = vec![0.01];
        for _ in 1..n {
            let next = out[out.len() - 1] * phi;
            out.push(next);
        }
        out
    }

    #[test]
    fn a_known_decay_rate_produces_the_expected_half_life() {
        // phi = 0.9 → 半衰期 = −ln2/ln0.9 ≈ 6.579 期；间隔 60 分钟 → ≈ 0.274 天
        let points = series(&decaying(0.9, 200), 60);
        let estimate = half_life(&points, 60).expect("应当能拟合出半衰期");
        let expected = 6.5788 * 60.0 / (60.0 * 24.0);
        let actual = estimate.days.to_f64().unwrap();
        assert!(
            (actual - expected).abs() < 0.01,
            "期望 ≈{expected:.4} 天，得到 {actual:.4}"
        );
        assert!(estimate.beta < dec!(0), "均值回归的 β 必须是负的");
    }

    #[test]
    fn a_slower_decay_gives_a_longer_half_life() {
        let fast = half_life(&series(&decaying(0.8, 200), 60), 60).unwrap();
        let slow = half_life(&series(&decaying(0.97, 200), 60), 60).unwrap();
        assert!(slow.days > fast.days, "衰减越慢，半衰期越长");
    }

    #[test]
    fn a_random_walk_is_refused_instead_of_returning_a_number() {
        // 交替正负但没有回归结构 → 不该给出半衰期
        let mut values = Vec::new();
        for index in 0..200 {
            values.push(if index % 2 == 0 { 0.01 } else { 0.02 });
        }
        assert!(half_life(&series(&values, 60), 60).is_none());
    }

    #[test]
    fn a_series_that_never_moves_is_refused() {
        let values = vec![0.005; 200];
        assert!(
            half_life(&series(&values, 60), 60).is_none(),
            "没有变动就没有可拟合的东西"
        );
    }

    #[test]
    fn too_few_samples_is_refused() {
        let points = series(&decaying(0.9, 20), 60);
        assert!(half_life(&points, 60).is_none(), "样本不足时必须承认不知道");
    }

    #[test]
    fn the_basis_series_aligns_on_time_not_on_index() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let long = vec![
            Candle {
                open_time: start,
                close: dec!(100),
            },
            // 缺了第二根
            Candle {
                open_time: start + TimeDelta::minutes(120),
                close: dec!(100),
            },
        ];
        let short = vec![
            Candle {
                open_time: start,
                close: dec!(101),
            },
            Candle {
                open_time: start + TimeDelta::minutes(60),
                close: dec!(999),
            },
            Candle {
                open_time: start + TimeDelta::minutes(120),
                close: dec!(101),
            },
        ];
        let series = basis_series(&long, &short);
        assert_eq!(series.len(), 2, "只保留两边都有的时刻");
        // (101 − 100) / 100.5 ≈ 0.00995
        assert!(series[0].basis > dec!(0.009) && series[0].basis < dec!(0.010));
        // 第二点用的是 short 的第三根（101），不是第二根（999）
        assert!(series[1].basis < dec!(0.011), "缺 K 线时不能按下标错配");
    }

    #[test]
    fn a_non_positive_price_is_skipped() {
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let long = vec![Candle {
            open_time: start,
            close: Decimal::ZERO,
        }];
        let short = vec![Candle {
            open_time: start,
            close: dec!(100),
        }];
        assert!(basis_series(&long, &short).is_empty());
    }

    #[test]
    fn candle_candidates_skip_venues_without_klines() {
        let ranked = [
            (Venue::Lighter, Venue::Variational),
            (Venue::Mexc, Venue::Lighter),
            (Venue::Gate, Venue::Mexc),
            (Venue::Binance, Venue::Okx),
            (Venue::Bybit, Venue::Bitget),
        ];
        let supports = |venue: Venue| !matches!(venue, Venue::Lighter | Venue::Variational);
        let picked = select_candle_candidates(&ranked, |pair| *pair, supports, 2, 160);
        assert_eq!(
            picked,
            vec![&(Venue::Gate, Venue::Mexc), &(Venue::Binance, Venue::Okx)]
        );

        let capped = select_candle_candidates(&ranked, |pair| *pair, supports, 5, 2);
        assert!(
            capped.is_empty(),
            "只检查前 2 条时，能拉 K 线的配对还没出现"
        );
        assert!(select_candle_candidates(&ranked, |pair| *pair, supports, 0, 10).is_empty());
    }

    #[test]
    fn candle_scan_budget_grows_with_top_but_stays_capped() {
        assert_eq!(max_candle_scan(0), 0);
        assert_eq!(max_candle_scan(1), 8);
        assert_eq!(max_candle_scan(20), 160);
        assert_eq!(max_candle_scan(30), 160);
    }
}
