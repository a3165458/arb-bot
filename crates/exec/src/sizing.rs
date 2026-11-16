//! 按盘口深度定单笔（价差单）。
//!
//! 固定单笔在薄盘口上会让第二腿吃不满：第一腿成交后，第二腿的限价是按第一腿的实际均价反推的**保本价**
//! （[`Plan::hedged_after`]），比「最优价 ± 滑点上限」紧得多；保本价以内盘口只有一点点量时，第二腿只能吃到一部分，
//! 剩下的吃不到就回滚（某次实盘：200 USDT 的单，Arcus 在保本价以内只吃到 70 USDT；某次实盘 同样）。
//!
//! 这里只在**吃不到**的时候把单笔往下缩，不为「凑出利润」去缩：
//!
//! - 从上限往下，每档 ×0.9，找第一个满足**执行条件**的单笔：
//!   1. 闸门里和大小有关的两条（限价内吃得满、预估滑点不超上限，见 [`Rejection::is_depth_limited`]）；
//!   2. 单笔不超过「第一腿限价内可吃深度」和「第二腿保本价内可吃深度」的 `fraction`（留余量：下单到第二腿到达
//!      要两秒上下，盘口会变）。
//!
//!   其余拒绝（仓位上限、当日亏损、强平距离……）和大小无关，原样拒绝，不缩。
//! - 价差够不够（吃完深度后净收益、收敛目标）**不进这个搜索**：定下单笔之后，`desk::prepare` 照旧把全部闸门
//!   在这个单笔上完整跑一遍。上限本身执行得了、只是不赚的，和以前一样被拒，不会缩成一小笔去找能赚的价位。
//! - 缩到不足 `min_size` 就拒绝，说清卡在哪条腿、限价内有多少深度。
//!
//! 逐档往下试而不是二分：第一腿选哪条会随单笔变（滑点大的先下），执行条件不一定单调；逐档试出来的
//! 单笔一定是验算通过的那个值。

use anyhow::{Result, anyhow, bail};
use arb_core::{Level, OrderBook, Side, Venue};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;

use crate::preflight::{LegOrder, LegPlan, Limits, Plan, Preflight, plan};

/// 逐档缩小的比例。
const SHRINK: Decimal = Decimal::from_parts(9, 0, 0, false, 1);

/// 按盘口深度定单笔的参数。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct DepthFit {
    /// 单笔最多占限价内可吃深度的比例（小数，0 < f ≤ 1）。
    pub fraction: Decimal,
    /// 最小单笔（USDT）：缩到它以下就不做。
    pub min_size: Decimal,
}

/// 实际定下来的单笔与依据。给日志、接口和自动交易的事件用。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SizeFit {
    /// 请求的上限。
    pub requested: Decimal,
    /// 实际单笔。
    pub fitted: Decimal,
    pub fraction: Decimal,
    /// 第一腿的场所与它在限价内的可吃深度（USDT）。
    pub first_venue: Venue,
    pub first_depth_usdt: Decimal,
    /// 第二腿的场所与它在保本价内的可吃深度（USDT）。
    pub second_venue: Venue,
    pub second_depth_usdt: Decimal,
}

impl SizeFit {
    /// 被缩小了（实际小于请求）。
    pub fn shrunk(&self) -> bool {
        self.fitted < self.requested
    }

    /// 给日志的一句话。
    pub fn describe(&self) -> String {
        format!(
            "单笔 {}{}：第一腿 {} 限价内可吃约 {} USDT，第二腿 {} 保本价内可吃约 {} USDT，最多占 {}%",
            self.fitted.normalize(),
            if self.shrunk() {
                format!("（上限 {}，按盘口深度缩小）", self.requested.normalize())
            } else {
                "（深度够，没缩小）".to_string()
            },
            self.first_venue,
            self.first_depth_usdt.round_dp(0).normalize(),
            self.second_venue,
            self.second_depth_usdt.round_dp(0).normalize(),
            (self.fraction * Decimal::ONE_HUNDRED)
                .round_dp(0)
                .normalize(),
        )
    }
}

/// `levels` 里在限价以内的档位名义合计（USDT）：买单价格不超过限价、卖单价格不低于限价。
pub fn depth_within(levels: &[Level], side: Side, limit: Decimal) -> Decimal {
    levels
        .iter()
        .take_while(|level| match side {
            Side::Buy => level.price <= limit,
            Side::Sell => level.price >= limit,
        })
        .filter(|level| level.price > Decimal::ZERO && level.notional_usdt > Decimal::ZERO)
        .map(|level| level.notional_usdt)
        .sum()
}

struct Depths {
    first_venue: Venue,
    first_usdt: Decimal,
    second_venue: Venue,
    second_usdt: Decimal,
}

impl Depths {
    fn allows(&self, size: Decimal, fraction: Decimal) -> bool {
        size <= self.first_usdt * fraction && size <= self.second_usdt * fraction
    }
}

/// 两条腿各自在限价内的可吃深度：
/// - 第一腿用实际下单的限价，即按保本线封顶后的（[`Plan::first_leg_capped`]）；
/// - 第二腿用「第一腿按预估均价成交」之后反推的保本限价。
///
/// 开了强制补腿（`force_hedge`）时，第二腿能吃到的是保本价那一档加上追单的最差限价以内的全部深度，按后者算。
fn leg_depths(
    plan: &Plan,
    long_book: &OrderBook,
    short_book: &OrderBook,
    force_hedge: Option<Decimal>,
) -> Depths {
    let capped = plan.first_leg_capped();
    let first = &capped;
    let mut hedged = plan.hedged_after(first.expected_price);
    if let Some(tolerance) = force_hedge {
        // 与执行器追单时的限价同一个算法：不比保本价那一档更紧。
        let forced = plan.forced_after(first.expected_price, tolerance);
        let (stage_one, chase) = (
            hedged.second_leg().limit_price,
            forced.second_leg().limit_price,
        );
        let limit = match hedged.second_leg().side {
            Side::Buy => chase.max(stage_one),
            Side::Sell => chase.min(stage_one),
        };
        if std::ptr::eq(hedged.second_leg(), &hedged.long) {
            hedged.long.limit_price = limit;
        } else {
            hedged.short.limit_price = limit;
        }
    }
    let second = hedged.second_leg();
    let depth = |leg: &LegPlan| {
        let book = if leg.side == Side::Buy {
            long_book
        } else {
            short_book
        };
        depth_within(book.side(leg.side), leg.side, leg.limit_price)
    };
    Depths {
        first_venue: first.venue,
        first_usdt: depth(first),
        second_venue: second.venue,
        second_usdt: depth(second),
    }
}

/// 某个单笔试下来为什么不行。
enum Block {
    /// 和大小有关：单笔再小一点也许就行。
    Size(String),
    /// 和大小无关（或根本评估不了）：原样拒绝。
    Fatal(anyhow::Error),
}

fn try_size(
    ctx: &Preflight<'_>,
    limits: &Limits,
    floor: Decimal,
    leg_order: LegOrder,
    force_hedge: Option<Decimal>,
    size: Decimal,
) -> Result<Depths, Block> {
    let sized = Preflight {
        size_usdt: size,
        opportunity: ctx.opportunity,
        long_book: ctx.long_book,
        short_book: ctx.short_book,
        open_positions: ctx.open_positions,
        daily_pnl: ctx.daily_pnl,
        risk: ctx.risk,
    };
    let mut trial = plan(&sized, limits).map_err(|rejection| {
        let message = format!("闸门拒绝：{rejection}");
        if rejection.is_depth_limited() {
            Block::Size(message)
        } else {
            Block::Fatal(anyhow!(message))
        }
    })?;
    trial.min_locked_basis = Some(floor);
    trial.leg_order = leg_order;
    Ok(leg_depths(
        &trial,
        ctx.long_book,
        ctx.short_book,
        force_hedge,
    ))
}

/// 价差单（两腿都吃单）的单笔：从 `ctx.size_usdt`（上限）往下找第一个执行得了的。
///
/// `floor` 是价差单的保本线（[`crate::desk::locked_basis_floor`]），`leg_order` 与下单时一致 ——
/// 第二腿的保本限价就是用它们和第一腿的预估均价反推的。`force_hedge` 是强制补腿的最差锁定价差（小数）：
/// 开着时第二腿的深度按追单的最差限价以内算（保本价以内的量不够时还能追），没开传 `None`。
pub fn fit_spread_size(
    ctx: &Preflight<'_>,
    limits: &Limits,
    floor: Decimal,
    leg_order: LegOrder,
    force_hedge: Option<Decimal>,
    fit: DepthFit,
) -> Result<SizeFit> {
    let cap = ctx.size_usdt;
    let min_size = fit.min_size.min(cap);
    let mut size = cap;
    let mut why;
    loop {
        match try_size(ctx, limits, floor, leg_order, force_hedge, size) {
            Ok(depths) if depths.allows(size, fit.fraction) => {
                return Ok(SizeFit {
                    requested: cap,
                    fitted: size,
                    fraction: fit.fraction,
                    first_venue: depths.first_venue,
                    first_depth_usdt: depths.first_usdt,
                    second_venue: depths.second_venue,
                    second_depth_usdt: depths.second_usdt,
                });
            }
            Ok(depths) => {
                why = format!(
                    "第一腿 {} 限价内可吃约 {} USDT，第二腿 {} 保本价内可吃约 {} USDT",
                    depths.first_venue,
                    depths.first_usdt.round_dp(0).normalize(),
                    depths.second_venue,
                    depths.second_usdt.round_dp(0).normalize()
                );
            }
            Err(Block::Size(message)) => why = message,
            Err(Block::Fatal(error)) => return Err(error),
        }
        let next = (size * SHRINK).round_dp_with_strategy(0, RoundingStrategy::ToZero);
        if next < min_size || next >= size {
            break;
        }
        size = next;
    }
    bail!(
        "盘口深度不够：单笔最多占限价内可吃深度的 {}%，从上限 {} USDT 往下试到 {} USDT 仍然不行（{why}），\
        不到最小单笔 {} USDT，不下单",
        (fit.fraction * Decimal::ONE_HUNDRED)
            .round_dp(0)
            .normalize(),
        cap.normalize(),
        size.normalize(),
        min_size.normalize()
    )
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::types::TaskRules;

    fn level(price: Decimal, notional: Decimal) -> Level {
        Level {
            price,
            notional_usdt: notional,
        }
    }

    #[test]
    fn depth_counts_only_levels_inside_the_limit_in_order() {
        let asks = [
            level(dec!(100.0), dec!(50)),
            level(dec!(100.1), dec!(70)),
            level(dec!(100.3), dec!(900)),
        ];
        // 买单：价格不超过限价的档位。
        assert_eq!(depth_within(&asks, Side::Buy, dec!(100.1)), dec!(120));
        assert_eq!(depth_within(&asks, Side::Buy, dec!(99.9)), Decimal::ZERO);
        assert_eq!(depth_within(&asks, Side::Buy, dec!(101)), dec!(1020));
        let bids = [
            level(dec!(100.0), dec!(40)),
            level(dec!(99.9), dec!(60)),
            level(dec!(99.0), dec!(500)),
        ];
        // 卖单：价格不低于限价的档位。
        assert_eq!(depth_within(&bids, Side::Sell, dec!(99.9)), dec!(100));
        assert_eq!(depth_within(&bids, Side::Sell, dec!(100.5)), Decimal::ZERO);
        // 坏数据档位（价格或名义非正）不算。
        let broken = [level(dec!(100), dec!(0)), level(dec!(100), dec!(30))];
        assert_eq!(depth_within(&broken, Side::Buy, dec!(100)), dec!(30));
    }

    #[test]
    fn the_size_must_fit_inside_the_fraction_of_both_legs() {
        let depths = Depths {
            first_venue: Venue::LighterRh,
            first_usdt: dec!(400),
            second_venue: Venue::Arcus,
            second_usdt: dec!(300),
        };
        assert!(depths.allows(dec!(150), dec!(0.5)));
        assert!(
            !depths.allows(dec!(151), dec!(0.5)),
            "第二腿 300 × 50% = 150"
        );
        assert!(depths.allows(dec!(300), dec!(1)));
        assert!(!depths.allows(dec!(301), dec!(1)));
    }

    #[test]
    fn scaled_rules_shrink_every_usdt_amount_and_keep_the_rest() {
        let rules = TaskRules {
            take_profit_usdt: Some(dec!(1)),
            stop_loss_usdt: Some(dec!(4)),
            timeout_hours: Some(dec!(6)),
            timeout_max_loss_usdt: Some(dec!(0)),
            auto_margin_pct: Some(dec!(10)),
            auto_margin_max_usdt: Some(dec!(40)),
            basis_exit_pct: Some(dec!(0.11)),
            liq_protection_pct: Some(dec!(5)),
            ..TaskRules::default()
        };
        let half = rules.scaled(dec!(0.25));
        assert_eq!(half.take_profit_usdt, Some(dec!(0.25)));
        assert_eq!(half.stop_loss_usdt, Some(dec!(1)));
        assert_eq!(
            half.timeout_max_loss_usdt,
            Some(dec!(0)),
            "0 = 保本才平，缩小后还是 0"
        );
        assert_eq!(half.auto_margin_max_usdt, Some(dec!(10)));
        // 百分比、小时数不动。
        assert_eq!(half.timeout_hours, Some(dec!(6)));
        assert_eq!(half.auto_margin_pct, Some(dec!(10)));
        assert_eq!(half.basis_exit_pct, Some(dec!(0.11)));
        assert_eq!(half.liq_protection_pct, Some(dec!(5)));
        // 没缩小（比例 ≥ 1）或比例无效：原样。
        assert_eq!(rules.scaled(dec!(1)), rules);
        assert_eq!(rules.scaled(dec!(0)), rules);
        // 没设的规则保持没设。
        assert_eq!(TaskRules::default().scaled(dec!(0.5)), TaskRules::default());
    }

    #[test]
    fn only_thin_book_rejections_are_size_dependent() {
        use crate::preflight::Rejection;
        assert!(
            Rejection::BookTooThin {
                required: dec!(1),
                available: dec!(0)
            }
            .is_depth_limited()
        );
        assert!(
            Rejection::SlippageTooHigh {
                slippage: dec!(1),
                limit: dec!(0)
            }
            .is_depth_limited()
        );
        for other in [
            Rejection::TooLarge {
                requested: dec!(1),
                limit: dec!(0),
            },
            Rejection::TooManyPositions { open: 1, limit: 1 },
            Rejection::DailyLossBreached {
                loss: dec!(1),
                limit: dec!(1),
            },
            Rejection::LimitWouldNotFill { side: Side::Buy },
            Rejection::OpenInterestCapped,
            Rejection::BookUnavailable {
                venue: "x".into(),
                reason: "y".into(),
            },
        ] {
            assert!(!other.is_depth_limited(), "{other:?}");
        }
    }
}
