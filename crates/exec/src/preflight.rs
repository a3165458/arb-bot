//! 下单前的闸门。
//!
//! 每一条拒绝都必须**说清是哪一条拦的**。只说「风控拒绝」会让用户去调错旋钮：
//! 他可能以为仓位太大，实际是盘口太薄。

use arb_core::{OrderBook, Side, estimate_fill_limited};
use arb_scanner::{Opportunity, PairRisk};
use rust_decimal::Decimal;

/// 风控限额。全部来自配置，不写死在代码里。
#[derive(Debug, Clone)]
pub struct Limits {
    /// 单笔最大名义额（计价币，按单腿算）。
    pub max_position_usdt: Decimal,
    /// 同时持有的最大仓位数。
    pub max_open_positions: usize,
    /// 单日最大亏损（计价币，正数表示允许亏这么多）。
    pub max_daily_loss_usdt: Decimal,
    /// 允许的最大不利滑点（小数，0.001 = 0.1%）。
    pub max_slippage: Decimal,
    /// 允许的最大不利入场基差（%）。与扫描器的门槛同义。
    pub max_entry_basis_pct: Decimal,
    /// 两腿里更近的开仓强平距离不得低于它（%）。
    ///
    /// 距离**未知**（场所不公开维持保证金率）时不拦：多数 CEX 的批量接口不给这个字段，
    /// 一律拦下等于只能做 Hyperliquid / Lighter。未知会在计划里如实打印出来。
    pub min_liq_distance_pct: Decimal,
    /// 资金费视角开仓前要回看两腿最近的逐小时费率，费差不稳定就拒绝。
    pub require_stable_funding: bool,
}

/// 被拒的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    TooLarge {
        requested: Decimal,
        limit: Decimal,
    },
    TooManyPositions {
        open: usize,
        limit: usize,
    },
    DailyLossBreached {
        loss: Decimal,
        limit: Decimal,
    },
    SlippageTooHigh {
        slippage: Decimal,
        limit: Decimal,
    },
    EntryBasisAdverse {
        basis: Decimal,
        limit: Decimal,
    },
    BookTooThin {
        required: Decimal,
        available: Decimal,
    },
    BookUnavailable {
        venue: String,
        reason: String,
    },
    LimitWouldNotFill {
        side: Side,
    },
    /// 至少一条腿的场所报告该合约触及持仓量上限，开不了新仓。
    OpenInterestCapped,
    LiquidationTooClose {
        distance_pct: Decimal,
        limit_pct: Decimal,
    },
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rejection::TooLarge { requested, limit } => {
                write!(f, "单笔名义额 {requested} 超过上限 {limit}")
            }
            Rejection::TooManyPositions { open, limit } => {
                write!(f, "已有 {open} 个仓位，达到上限 {limit}")
            }
            Rejection::DailyLossBreached { loss, limit } => {
                write!(f, "当日亏损 {loss} 已达上限 {limit}")
            }
            Rejection::SlippageTooHigh { slippage, limit } => {
                write!(f, "估算滑点 {} 超过上限 {limit}", slippage.round_dp(6))
            }
            Rejection::EntryBasisAdverse { basis, limit } => {
                write!(f, "入场基差 {basis}% 低于 −{limit}%")
            }
            Rejection::BookTooThin {
                required,
                available,
            } => write!(
                f,
                "仓位 {required} 超过盘口在限价（最优价 ± 滑点上限）以内可吃到的 {available}"
            ),
            Rejection::BookUnavailable { venue, reason } => {
                write!(f, "{venue} 的盘口不可用：{reason}")
            }
            Rejection::LimitWouldNotFill { side } => {
                write!(f, "{side:?} 腿在限价内吃不到任何量")
            }
            Rejection::OpenInterestCapped => {
                write!(f, "至少一条腿的场所已触及该合约的持仓量上限，只能减仓")
            }
            Rejection::LiquidationTooClose {
                distance_pct,
                limit_pct,
            } => write!(
                f,
                "开仓强平距离 {}% 低于 {limit_pct}%，降低杠杆再试",
                distance_pct.round_dp(2)
            ),
        }
    }
}

/// 一条腿的执行计划。
#[derive(Debug, Clone, serde::Serialize)]
pub struct LegPlan {
    pub symbol: arb_core::Symbol,
    pub venue: arb_core::Venue,
    pub side: Side,
    pub notional_usdt: Decimal,
    /// 限价：买单不超过它、卖单不低于它。
    pub limit_price: Decimal,
    /// 计划时的盘口最优价（买腿 = 卖一，卖腿 = 买一）。事后核对实际成交价用。
    pub best_price: Decimal,
    /// 按当前盘口估算的成交均价。
    pub expected_price: Decimal,
    /// 相对最优价的不利滑点（小数）。
    pub slippage: Decimal,
    /// 盘口里这一侧能吃到多少名义额。
    pub book_notional: Decimal,
}

/// 一次执行的完整计划。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Plan {
    pub symbol: arb_core::Symbol,
    pub long: LegPlan,
    pub short: LegPlan,
    /// 两腿合计的预期成本（手续费 + 穿价 + 滑点），占名义额的比例。
    pub expected_cost: Decimal,
}

impl Plan {
    /// 先执行哪条腿：**难成交的那条先做**。
    ///
    /// 如果先做流动性好的那条、难做的那条失败了，你得回滚一条已经建好的腿（多付一次
    /// 手续费）；反过来，难做的那条先失败时你手上**什么都没有**，直接放弃即可。
    pub fn first_leg(&self) -> &LegPlan {
        if self.long.slippage >= self.short.slippage {
            &self.long
        } else {
            &self.short
        }
    }

    pub fn second_leg(&self) -> &LegPlan {
        if std::ptr::eq(self.first_leg(), &self.long) {
            &self.short
        } else {
            &self.long
        }
    }
}

/// 闸门的输入。
pub struct Preflight<'a> {
    pub opportunity: &'a Opportunity,
    pub size_usdt: Decimal,
    pub long_book: &'a OrderBook,
    pub short_book: &'a OrderBook,
    pub open_positions: usize,
    /// 当日已实现盈亏（负数 = 亏）。
    pub daily_pnl: Decimal,
    /// 按计划杠杆算出的两腿风险。`None` = 调用方没有两腿快照。
    pub risk: Option<&'a PairRisk>,
}

/// 评估一笔交易能不能做、按什么价做。
pub fn plan(ctx: &Preflight<'_>, limits: &Limits) -> Result<Plan, Rejection> {
    if ctx.size_usdt > limits.max_position_usdt {
        return Err(Rejection::TooLarge {
            requested: ctx.size_usdt,
            limit: limits.max_position_usdt,
        });
    }
    if ctx.open_positions >= limits.max_open_positions {
        return Err(Rejection::TooManyPositions {
            open: ctx.open_positions,
            limit: limits.max_open_positions,
        });
    }
    if ctx.daily_pnl <= -limits.max_daily_loss_usdt {
        return Err(Rejection::DailyLossBreached {
            loss: -ctx.daily_pnl,
            limit: limits.max_daily_loss_usdt,
        });
    }
    if let Some(basis) = ctx.opportunity.entry_basis_pct
        && basis < -limits.max_entry_basis_pct
    {
        return Err(Rejection::EntryBasisAdverse {
            basis,
            limit: limits.max_entry_basis_pct,
        });
    }
    if ctx.opportunity.oi_capped {
        return Err(Rejection::OpenInterestCapped);
    }
    if let Some(distance) = ctx.risk.and_then(|risk| risk.liq_distance_pct)
        && distance < limits.min_liq_distance_pct
    {
        return Err(Rejection::LiquidationTooClose {
            distance_pct: distance,
            limit_pct: limits.min_liq_distance_pct,
        });
    }

    // 多腿买入（吃 ask），空腿卖出（吃 bid）。
    let long = leg_plan(
        &ctx.opportunity.symbol,
        ctx.opportunity.long,
        Side::Buy,
        ctx.long_book,
        ctx.size_usdt,
        limits,
    )?;
    let short = leg_plan(
        &ctx.opportunity.symbol,
        ctx.opportunity.short,
        Side::Sell,
        ctx.short_book,
        ctx.size_usdt,
        limits,
    )?;

    // 成本 = 两腿的实际成交价相对**标记价**的偏离。这样它同时包含了穿价与滑点，
    // 而不会重复计入已经在净年化里扣过的手续费。
    let expected_cost = cost_ratio(&long, &short, ctx);

    Ok(Plan {
        symbol: ctx.opportunity.symbol.clone(),
        long,
        short,
        expected_cost,
    })
}

fn leg_plan(
    symbol: &arb_core::Symbol,
    venue: arb_core::Venue,
    side: Side,
    book: &OrderBook,
    size_usdt: Decimal,
    limits: &Limits,
) -> Result<LegPlan, Rejection> {
    let levels = book.side(side);
    if levels.is_empty() {
        return Err(Rejection::BookUnavailable {
            venue: venue.to_string(),
            reason: "该侧没有档位".into(),
        });
    }
    let best = levels[0].price;
    if best <= Decimal::ZERO {
        return Err(Rejection::BookUnavailable {
            venue: venue.to_string(),
            reason: "最优价非正".into(),
        });
    }

    // 盘口够不够厚按深度判断：限价（最优价 ± 滑点上限）以内吃得满、预估滑点不超上限。
    // 不再看「仓位不超过第一档的几倍」：第一档常常只挂着一笔很小的单子、每秒都在变，
    // 连 BTC 这种深盘口都会被它拦下，而限价以内的累计深度才是这笔单真正能吃到的量。
    let book_notional: Decimal = levels.iter().map(|level| level.notional_usdt).sum();

    // 限价按「最优价 ± 允许滑点」定，再按这个限价估算能吃到多少。
    let limit_price = match side {
        Side::Buy => best * (Decimal::ONE + limits.max_slippage),
        Side::Sell => best * (Decimal::ONE - limits.max_slippage),
    };

    let Some(estimate) = estimate_fill_limited(levels, size_usdt, side, Some(limit_price)) else {
        return Err(Rejection::BookUnavailable {
            venue: venue.to_string(),
            reason: "盘口数据不可用".into(),
        });
    };
    if estimate.filled_usdt <= Decimal::ZERO {
        return Err(Rejection::LimitWouldNotFill { side });
    }
    if estimate.slippage > limits.max_slippage {
        return Err(Rejection::SlippageTooHigh {
            slippage: estimate.slippage,
            limit: limits.max_slippage,
        });
    }
    if estimate.exhausted {
        return Err(Rejection::BookTooThin {
            required: size_usdt,
            available: estimate.filled_usdt,
        });
    }

    Ok(LegPlan {
        symbol: symbol.clone(),
        venue,
        side,
        notional_usdt: size_usdt,
        limit_price,
        best_price: best,
        expected_price: estimate.average_price,
        slippage: estimate.slippage,
        book_notional,
    })
}

/// 两腿成交价相对标记价的偏离，占名义额的比例。
fn cost_ratio(long: &LegPlan, short: &LegPlan, ctx: &Preflight<'_>) -> Decimal {
    let long_mark = ctx
        .long_book
        .best_bid()
        .zip(ctx.long_book.best_ask())
        .map(|(bid, ask)| (bid + ask) / Decimal::TWO);
    let short_mark = ctx
        .short_book
        .best_bid()
        .zip(ctx.short_book.best_ask())
        .map(|(bid, ask)| (bid + ask) / Decimal::TWO);

    let mut total = Decimal::ZERO;
    if let Some(mark) = long_mark.filter(|mark| *mark > Decimal::ZERO) {
        total += (long.expected_price - mark) / mark;
    }
    if let Some(mark) = short_mark.filter(|mark| *mark > Decimal::ZERO) {
        total += (mark - short.expected_price) / mark;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Level, Symbol, Venue};
    use rust_decimal_macros::dec;

    fn limits() -> Limits {
        Limits {
            max_position_usdt: dec!(10_000),
            max_open_positions: 3,
            max_daily_loss_usdt: dec!(500),
            max_slippage: dec!(0.002),
            max_entry_basis_pct: dec!(0.5),
            min_liq_distance_pct: dec!(8),
            require_stable_funding: true,
        }
    }

    fn book(bid: Decimal, ask: Decimal, notional: Decimal) -> OrderBook {
        OrderBook {
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![Level {
                price: bid,
                notional_usdt: notional,
            }],
            asks: vec![Level {
                price: ask,
                notional_usdt: notional,
            }],
        }
    }

    fn opportunity(basis: Decimal) -> Opportunity {
        Opportunity {
            symbol: Symbol::perp("BTC", "USDT"),
            long: Venue::Binance,
            short: Venue::Okx,
            long_interval_h: 8,
            short_interval_h: 8,
            long_daily: dec!(0.0001),
            short_daily: dec!(0.0002),
            daily_spread: dec!(0.0001),
            apr: dec!(0.0365),
            round_trip_fee: dec!(0.002),
            round_trip_spread: Some(dec!(0.0002)),
            round_trip_cost: dec!(0.0022),
            spread_unknown: false,
            funding_daily: dec!(0.00007),
            funding_apr: dec!(0.0255),
            spread_net: dec!(0.0),
            spread_hold_days: dec!(3),
            hold_measured: false,
            entry_basis_pct: Some(basis),
            executable_basis_pct: Some(basis),
            quote_mismatch: false,
            oi_capped: false,
        }
    }

    fn preflight<'a>(
        op: &'a Opportunity,
        long: &'a OrderBook,
        short: &'a OrderBook,
        size: Decimal,
    ) -> Preflight<'a> {
        Preflight {
            opportunity: op,
            size_usdt: size,
            long_book: long,
            short_book: short,
            open_positions: 0,
            daily_pnl: Decimal::ZERO,
            risk: None,
        }
    }

    fn snapshot(venue: Venue, mmr: Option<Decimal>) -> arb_core::MarketSnapshot {
        arb_core::MarketSnapshot {
            venue,
            symbol: Symbol::perp("BTC", "USDT"),
            period_rate: Decimal::ZERO,
            interval_h: 8,
            interval_assumed: false,
            next_funding_at: chrono::Utc::now(),
            next_funding_estimated: false,
            taker_fee: None,
            mark_price: Some(dec!(100)),
            index_price: Some(dec!(100)),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: Some(dec!(50)),
            maintenance_margin: mmr,
            oi_capped: false,
        }
    }

    #[test]
    fn an_oi_capped_pair_is_rejected_before_touching_the_book() {
        let mut op = opportunity(dec!(0.1));
        op.oi_capped = true;
        let long = book(dec!(100), dec!(100.01), dec!(50_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let error = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap_err();
        assert_eq!(error, Rejection::OpenInterestCapped);
    }

    #[test]
    fn a_leverage_that_puts_liquidation_in_the_danger_zone_is_rejected() {
        let op = opportunity(dec!(0.1));
        let long_book = book(dec!(100), dec!(100.01), dec!(50_000));
        let short_book = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let long = snapshot(Venue::Binance, Some(dec!(0.01)));
        let short = snapshot(Venue::Okx, Some(dec!(0.01)));

        // 20 倍：强平距离约 4%，危险档
        let risky = arb_scanner::pair_risk(&op, &long, &short, dec!(20));
        let mut ctx = preflight(&op, &long_book, &short_book, dec!(5_000));
        ctx.risk = Some(&risky);
        let error = plan(&ctx, &limits()).unwrap_err();
        assert!(
            matches!(error, Rejection::LiquidationTooClose { .. }),
            "{error}"
        );

        // 3 倍：约 32%，放行
        let safe = arb_scanner::pair_risk(&op, &long, &short, dec!(3));
        ctx.risk = Some(&safe);
        assert!(plan(&ctx, &limits()).is_ok());

        // 维持保证金率未知：不拦（多数 CEX 不公开），由调用方打印「未知」
        let unknown_long = snapshot(Venue::Binance, None);
        let unknown = arb_scanner::pair_risk(&op, &unknown_long, &short, dec!(20));
        ctx.risk = Some(&unknown);
        assert!(plan(&ctx, &limits()).is_ok());
    }

    #[test]
    fn a_clean_pair_produces_a_two_leg_plan() {
        let op = opportunity(dec!(0.1));
        let long = book(dec!(100), dec!(100.01), dec!(50_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let plan = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap();

        assert_eq!(plan.long.side, Side::Buy);
        assert_eq!(plan.short.side, Side::Sell);
        // 买单限价 = 最优卖价 × (1 + 允许滑点)
        assert!(plan.long.limit_price > dec!(100.01));
        // 卖单限价 = 最优买价 × (1 − 允许滑点)
        assert!(plan.short.limit_price < dec!(100.05));
    }

    #[test]
    fn the_harder_leg_goes_first() {
        // 空腿一档更薄 → 它更难成交 → 它先做
        let op = opportunity(dec!(0.1));
        let long = book(dec!(100), dec!(100.01), dec!(50_000));
        let short = OrderBook {
            venue: Venue::Okx,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![
                Level {
                    price: dec!(100.05),
                    notional_usdt: dec!(3_000),
                },
                Level {
                    price: dec!(100.00),
                    notional_usdt: dec!(50_000),
                },
            ],
            asks: vec![Level {
                price: dec!(100.06),
                notional_usdt: dec!(50_000),
            }],
        };
        let plan = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap();
        assert_eq!(plan.first_leg().side, Side::Sell, "难成交的空腿必须先做");
        assert_eq!(plan.second_leg().side, Side::Buy);
    }

    #[test]
    fn a_small_first_level_backed_by_depth_within_the_limit_price_is_fine() {
        let op = opportunity(dec!(0.1));
        let mut long = book(dec!(100), dec!(100.01), dec!(50_000));
        // 卖一只挂 10 USDT，后面一档（仍在 0.2% 限价以内）很厚。
        long.asks = vec![
            Level {
                price: dec!(100.01),
                notional_usdt: dec!(10),
            },
            Level {
                price: dec!(100.02),
                notional_usdt: dec!(50_000),
            },
        ];
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let plan = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap();
        assert!(plan.long.slippage < dec!(0.002), "{}", plan.long.slippage);
    }

    #[test]
    fn a_position_the_book_cannot_fill_within_the_limit_price_is_rejected_as_thin() {
        let op = opportunity(dec!(0.1));
        let long = book(dec!(100), dec!(100.01), dec!(1_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let error = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap_err();
        assert!(matches!(error, Rejection::BookTooThin { .. }), "{error}");
    }

    #[test]
    fn with_no_position_cap_only_the_book_depth_limits_the_size() {
        // ARB_MAX_POSITION_USDT=off：单笔名义额不再有固定上限，盘口够厚就放行……
        let limits = Limits {
            max_position_usdt: Decimal::MAX,
            ..limits()
        };
        let op = opportunity(dec!(0.1));
        let deep_long = book(dec!(100), dec!(100.01), dec!(500_000_000));
        let deep_short = book(dec!(100.05), dec!(100.06), dec!(500_000_000));
        assert!(
            plan(
                &preflight(&op, &deep_long, &deep_short, dec!(2_000_000)),
                &limits
            )
            .is_ok()
        );
        // ……盘口撑不住就照样拒绝：深度检查不受影响。
        let thin = book(dec!(100), dec!(100.01), dec!(1_000));
        let error = plan(
            &preflight(&op, &thin, &deep_short, dec!(2_000_000)),
            &limits,
        )
        .unwrap_err();
        assert!(matches!(error, Rejection::BookTooThin { .. }), "{error}");
    }

    #[test]
    fn a_size_beyond_the_limits_is_rejected() {
        let op = opportunity(dec!(0.1));
        let long = book(dec!(100), dec!(100.01), dec!(500_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(500_000));
        let error = plan(&preflight(&op, &long, &short, dec!(50_000)), &limits()).unwrap_err();
        assert!(matches!(error, Rejection::TooLarge { .. }), "{error}");
    }

    #[test]
    fn an_adverse_entry_basis_is_rejected() {
        let op = opportunity(dec!(-2.0));
        let long = book(dec!(100), dec!(100.01), dec!(50_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let error = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap_err();
        assert!(
            matches!(error, Rejection::EntryBasisAdverse { .. }),
            "{error}"
        );
    }

    #[test]
    fn the_daily_loss_limit_stops_further_trading() {
        let op = opportunity(dec!(0.1));
        let long = book(dec!(100), dec!(100.01), dec!(50_000));
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let mut ctx = preflight(&op, &long, &short, dec!(5_000));
        ctx.daily_pnl = dec!(-500);
        let error = plan(&ctx, &limits()).unwrap_err();
        assert!(
            matches!(error, Rejection::DailyLossBreached { .. }),
            "{error}"
        );
    }

    #[test]
    fn an_empty_side_is_reported_with_the_venue_name() {
        let op = opportunity(dec!(0.1));
        let long = OrderBook {
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![],
            asks: vec![],
        };
        let short = book(dec!(100.05), dec!(100.06), dec!(50_000));
        let error = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap_err();
        match error {
            Rejection::BookUnavailable { venue, .. } => assert_eq!(venue, "binance"),
            other => panic!("期望 BookUnavailable，得到 {other}"),
        }
    }

    #[test]
    fn the_expected_cost_counts_both_legs_crossing_the_mid() {
        let op = opportunity(dec!(0.1));
        // 买在 ask（高于 mid）、卖在 bid（低于 mid）→ 成本为正
        let long = book(dec!(99.99), dec!(100.01), dec!(50_000));
        let short = book(dec!(100.04), dec!(100.06), dec!(50_000));
        let plan = plan(&preflight(&op, &long, &short, dec!(5_000)), &limits()).unwrap();
        assert!(plan.expected_cost > Decimal::ZERO, "{}", plan.expected_cost);
    }
}
