//! 开仓执行报告：下单前的报价（计划）与实际成交的对照，外加各步耗时。
//!
//! 为什么要有它：点「下单」时服务端会重新现扫、重新拉盘口、重新算计划，那份计划和你在预览里看到的
//! 不是同一份，而且以前它从不落盘 —— 事后说「预估和实际差了多少」只能从限价倒推。2026-10-01 PONS
//! 那笔就是这样：Lighter 卖一只挂了 $52，2800 的买单吃穿了好几档，比卖一差 0.10%，但当时计划里的
//! 预估均价是多少，已经没法证实。现在每笔开仓都把这份对照记进仓位（台账里随仓位一起落盘），
//! 日志、Telegram 通知、结果面板都用同一份。
//!
//! 约定：偏差都按「不利为正」—— 买腿成交价高于参考价、卖腿成交价低于参考价为正，负数是比参考价更好。

use arb_core::{Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::preflight::{LegPlan, Plan};
use crate::types::LegFill;

/// 一条腿的计划与实际。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegExecution {
    pub venue: Venue,
    pub side: Side,
    /// 计划时的盘口最优价（买腿 = 卖一，卖腿 = 买一）。
    pub best_price: Decimal,
    /// 计划按这笔名义吃深度得到的预估均价（盘口够厚时就等于最优价）。
    pub expected_price: Decimal,
    pub limit_price: Decimal,
    /// 实际成交均价。
    pub actual_price: Decimal,
    /// 实际相对预估均价的不利幅度（小数）。
    pub vs_expected: Decimal,
    /// 实际相对最优价的不利幅度（小数）。
    pub vs_best: Decimal,
    /// 实际成交名义。
    pub notional_usdt: Decimal,
    pub fee_usdt: Decimal,
    /// 从取盘口（报价）到这条腿成交，本地计时的毫秒数。拿不到报价时刻时为 `None`。
    pub quote_to_fill_ms: Option<i64>,
}

/// 一笔开仓的执行报告。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenReport {
    /// 先执行的那条腿。
    pub first: LegExecution,
    /// 后执行的那条腿。
    pub second: LegExecution,
    /// 锁定价差（小数）：(空腿价 − 多腿价) / 多腿价。按最优价、预估均价、实际成交价各算一次。
    pub best_basis: Decimal,
    pub expected_basis: Decimal,
    pub actual_basis: Decimal,
    /// 两腿合计比预估均价多花（正）/ 少花（负）的 USDT。
    pub vs_expected_usdt: Decimal,
    /// 两腿合计比最优价多花（正）/ 少花（负）的 USDT。
    pub vs_best_usdt: Decimal,
    pub fees_usdt: Decimal,
    /// 第一腿成交到第二腿成交：这段时间里手上只有一条腿，价格怎么走都和你无关地涨跌。
    pub unhedged_ms: i64,
    /// 执行器开始后，两条腿的下单前准备（杠杆核对、市场元数据）花了多久。预热过的话接近 0。
    #[serde(default)]
    pub prepare_ms: i64,
    /// 执行器从开始到两腿都记完。
    pub total_ms: i64,
}

/// 计时用的时刻。`quoted_at` 是取盘口的时刻（`prepare` 里拉完两个盘口之后）。
#[derive(Debug, Clone, Copy)]
pub struct OpenTiming {
    pub quoted_at: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    /// 两条腿的下单前准备做完的时刻。
    pub ready_at: DateTime<Utc>,
    pub first_filled_at: DateTime<Utc>,
    pub second_filled_at: DateTime<Utc>,
}

/// 买腿成交价高于参考价、卖腿低于参考价为正。
fn adverse(side: Side, actual: Decimal, reference: Decimal) -> Decimal {
    match side {
        Side::Buy => (actual - reference) / reference,
        Side::Sell => (reference - actual) / reference,
    }
}

fn basis(long: Decimal, short: Decimal) -> Option<Decimal> {
    (long > Decimal::ZERO).then(|| (short - long) / long)
}

fn millis(from: DateTime<Utc>, to: DateTime<Utc>) -> i64 {
    (to - from).num_milliseconds().max(0)
}

fn leg_execution(
    planned: &LegPlan,
    fill: &LegFill,
    quote_to_fill_ms: Option<i64>,
) -> Option<LegExecution> {
    if planned.venue != fill.venue
        || planned.side != fill.side
        || planned.best_price <= Decimal::ZERO
        || planned.expected_price <= Decimal::ZERO
        || fill.average_price <= Decimal::ZERO
    {
        return None;
    }
    Some(LegExecution {
        venue: fill.venue,
        side: fill.side,
        best_price: planned.best_price,
        expected_price: planned.expected_price,
        limit_price: planned.limit_price,
        actual_price: fill.average_price,
        vs_expected: adverse(fill.side, fill.average_price, planned.expected_price),
        vs_best: adverse(fill.side, fill.average_price, planned.best_price),
        notional_usdt: fill.notional_usdt,
        fee_usdt: fill.fee_usdt,
        quote_to_fill_ms,
    })
}

/// 把计划与两腿的实际成交对成一份报告。计划与成交对不上（场所、方向不符）或有价格不可用时
/// 返回 `None` —— 报告只是给人看的，对不上就不出，不能编。
pub fn open_report(
    plan: &Plan,
    first: &LegFill,
    second: &LegFill,
    timing: &OpenTiming,
) -> Option<OpenReport> {
    let quote_to = |at: DateTime<Utc>| timing.quoted_at.map(|quoted| millis(quoted, at));
    let first_exec = leg_execution(plan.first_leg(), first, quote_to(timing.first_filled_at))?;
    let second_exec = leg_execution(plan.second_leg(), second, quote_to(timing.second_filled_at))?;

    let (long_fill, short_fill) = if first.side == Side::Buy {
        (first, second)
    } else {
        (second, first)
    };
    let vs_expected_usdt = first_exec.vs_expected * first_exec.notional_usdt
        + second_exec.vs_expected * second_exec.notional_usdt;
    let vs_best_usdt = first_exec.vs_best * first_exec.notional_usdt
        + second_exec.vs_best * second_exec.notional_usdt;
    Some(OpenReport {
        best_basis: basis(plan.long.best_price, plan.short.best_price)?,
        expected_basis: basis(plan.long.expected_price, plan.short.expected_price)?,
        actual_basis: basis(long_fill.average_price, short_fill.average_price)?,
        vs_expected_usdt,
        vs_best_usdt,
        fees_usdt: first_exec.fee_usdt + second_exec.fee_usdt,
        unhedged_ms: millis(timing.first_filled_at, timing.second_filled_at),
        prepare_ms: millis(timing.started_at, timing.ready_at),
        total_ms: millis(timing.started_at, timing.second_filled_at),
        first: first_exec,
        second: second_exec,
    })
}

fn signed_pct(fraction: Decimal) -> String {
    let pct = (fraction * Decimal::ONE_HUNDRED).round_dp(3);
    if pct.is_sign_negative() && !pct.is_zero() {
        format!("{pct}%")
    } else {
        format!("+{}%", pct.abs())
    }
}

fn seconds(ms: i64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

fn side_label(side: Side) -> &'static str {
    match side {
        Side::Buy => "买入",
        Side::Sell => "卖出",
    }
}

impl LegExecution {
    fn line(&self) -> String {
        format!(
            "{} {}：预估 {} → 实际 {}（比预估 {}，比最优价 {}）",
            side_label(self.side),
            self.venue,
            self.expected_price.normalize(),
            self.actual_price.round_dp(6).normalize(),
            signed_pct(self.vs_expected),
            signed_pct(self.vs_best),
        )
    }
}

impl OpenReport {
    /// 给日志与 Telegram 用的多行说明。
    pub fn summary(&self) -> String {
        let usdt = |value: Decimal| {
            let value = value.round_dp(2);
            if value.is_sign_negative() && !value.is_zero() {
                format!("{value}")
            } else {
                format!("+{}", value.abs())
            }
        };
        let quote = match (self.first.quote_to_fill_ms, self.second.quote_to_fill_ms) {
            (Some(first), Some(second)) => {
                format!(
                    "报价→首腿成交 {}、→次腿成交 {}，",
                    seconds(first),
                    seconds(second)
                )
            }
            _ => String::new(),
        };
        format!(
            "开仓执行（偏差按不利为正，负 = 比参考价更好）\n\
             · {}\n\
             · {}\n\
             锁定价差：最优价 {} / 预估 {} / 实际 {}\n\
             两腿合计比预估 {} USDT、比最优价 {} USDT；手续费 {} USDT\n\
             耗时：{quote}下单前准备 {}，两腿间隔 {}（只有一条腿的窗口），执行器合计 {}",
            self.first.line(),
            self.second.line(),
            signed_pct(self.best_basis),
            signed_pct(self.expected_basis),
            signed_pct(self.actual_basis),
            usdt(self.vs_expected_usdt),
            usdt(self.vs_best_usdt),
            self.fees_usdt.round_dp(2),
            seconds(self.prepare_ms),
            seconds(self.unhedged_ms),
            seconds(self.total_ms),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClientOrderId;
    use arb_core::Symbol;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn leg_plan(
        venue: Venue,
        side: Side,
        best: Decimal,
        expected: Decimal,
        limit: Decimal,
    ) -> LegPlan {
        LegPlan {
            symbol: Symbol::perp("PONS", "USDT"),
            venue,
            side,
            notional_usdt: dec!(2800),
            limit_price: limit,
            best_price: best,
            expected_price: expected,
            slippage: (expected - best).abs() / best,
            book_notional: dec!(46000),
        }
    }

    fn fill(venue: Venue, side: Side, price: Decimal, notional: Decimal, fee: Decimal) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: notional,
            average_price: price,
            fee_usdt: fee,
            client_order_id: ClientOrderId("x".into()),
            margin_usdt: None,
        }
    }

    /// 2026-10-01 03:41 的 PONS：Lighter 卖一 0.52416 只挂了 $52，下一档 0.52465 起，2800 吃到
    /// 0.52470；Arcus 买一 0.52407，成交在 0.52456（价格在这几秒里涨了）。
    fn pons() -> (Plan, LegFill, LegFill) {
        let long = leg_plan(
            Venue::LighterRh,
            Side::Buy,
            dec!(0.52416),
            dec!(0.52470),
            dec!(0.52520832),
        );
        let short = leg_plan(
            Venue::Arcus,
            Side::Sell,
            dec!(0.52407),
            dec!(0.52407),
            dec!(0.52302186),
        );
        let plan = Plan {
            symbol: Symbol::perp("PONS", "USDT"),
            long,
            short,
            expected_cost: dec!(0.0013415),
        };
        (
            plan,
            fill(
                Venue::LighterRh,
                Side::Buy,
                dec!(0.5247018341086434573829531813),
                dec!(2797.290418),
                dec!(0),
            ),
            fill(
                Venue::Arcus,
                Side::Sell,
                dec!(0.52456),
                dec!(2796.534272),
                dec!(0.62922021),
            ),
        )
    }

    fn at(second: u32, milli: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 1, 3, 41, second)
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::milliseconds(i64::from(milli))
    }

    fn timing() -> OpenTiming {
        OpenTiming {
            quoted_at: Some(at(15, 500)),
            started_at: at(16, 388),
            ready_at: at(16, 700),
            first_filled_at: at(18, 100),
            second_filled_at: at(21, 748),
        }
    }

    #[test]
    fn a_thin_top_of_book_shows_up_as_the_leg_paying_more_than_the_best_price() {
        let (plan, long, short) = pons();
        // Lighter 是 plan.long，滑点最大，先执行。
        let report = open_report(&plan, &long, &short, &timing()).unwrap();
        assert_eq!(report.first.venue, Venue::LighterRh);
        // 比卖一差 0.103%，但和预估均价（已经算了吃穿）几乎一致。
        assert_eq!(report.first.vs_best.round_dp(5), dec!(0.00103));
        assert!(report.first.vs_expected.abs() < dec!(0.00001));
        // Arcus 卖腿：成交价比买一还高，是「更好」，所以是负的。
        assert_eq!(report.second.vs_best.round_dp(5), dec!(-0.00093));
        assert_eq!(report.second.vs_expected, report.second.vs_best);
    }

    #[test]
    fn the_two_legs_drifting_together_barely_moves_the_locked_spread() {
        let (plan, long, short) = pons();
        let report = open_report(&plan, &long, &short, &timing()).unwrap();
        assert_eq!(report.best_basis.round_dp(5), dec!(-0.00017));
        assert_eq!(report.actual_basis.round_dp(5), dec!(-0.00027));
        // 比最优价合计只差约 0.28 USDT；比（已吃穿的）预估则是好了约 2.6 USDT。
        assert_eq!(report.vs_best_usdt.round_dp(2), dec!(0.28));
        assert_eq!(report.vs_expected_usdt.round_dp(2), dec!(-2.60));
        assert_eq!(report.fees_usdt, dec!(0.62922021));
    }

    #[test]
    fn the_timings_split_quote_age_unhedged_window_and_total() {
        let (plan, long, short) = pons();
        let report = open_report(&plan, &long, &short, &timing()).unwrap();
        assert_eq!(report.first.quote_to_fill_ms, Some(2600));
        assert_eq!(report.second.quote_to_fill_ms, Some(6248));
        assert_eq!(report.unhedged_ms, 3648);
        assert_eq!(report.prepare_ms, 312);
        assert_eq!(report.total_ms, 5360);
    }

    #[test]
    fn a_missing_quote_time_leaves_the_quote_ages_empty_but_keeps_the_rest() {
        let (plan, long, short) = pons();
        let report = open_report(
            &plan,
            &long,
            &short,
            &OpenTiming {
                quoted_at: None,
                ..timing()
            },
        )
        .unwrap();
        assert_eq!(report.first.quote_to_fill_ms, None);
        assert_eq!(report.total_ms, 5360);
    }

    #[test]
    fn a_fill_that_does_not_match_the_plan_makes_no_report() {
        let (plan, long, short) = pons();
        // 场所对不上。
        let wrong_venue = fill(Venue::Lighter, Side::Buy, dec!(0.5247), dec!(2797), dec!(0));
        assert!(open_report(&plan, &wrong_venue, &short, &timing()).is_none());
        // 价格不可用。
        let zero = fill(Venue::LighterRh, Side::Buy, dec!(0), dec!(2797), dec!(0));
        assert!(open_report(&plan, &zero, &short, &timing()).is_none());
        // 计划里没有最优价（旧计划）。
        let mut old = plan;
        old.long.best_price = Decimal::ZERO;
        assert!(open_report(&old, &long, &short, &timing()).is_none());
    }

    #[test]
    fn the_summary_is_readable_and_signed() {
        let (plan, long, short) = pons();
        let text = open_report(&plan, &long, &short, &timing())
            .unwrap()
            .summary();
        assert!(
            text.contains(
                "买入 lighter-rh：预估 0.5247 → 实际 0.524702（比预估 +0.000%，比最优价 +0.103%）"
            ),
            "{text}"
        );
        assert!(text.contains("卖出 arcus"), "{text}");
        assert!(text.contains("比最优价 -0.093%"), "{text}");
        assert!(text.contains("下单前准备 0.3s"), "{text}");
        assert!(text.contains("两腿间隔 3.6s"), "{text}");
        assert!(text.contains("手续费 0.63 USDT"), "{text}");
    }
}
