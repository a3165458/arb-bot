//! 下单前的闸门。
//!
//! 每一条拒绝都必须**说清是哪一条拦的**。只说「风控拒绝」会让用户去调错旋钮：
//! 他可能以为仓位太大，实际是盘口太薄。

use arb_core::{OrderBook, Side, estimate_fill_limited};
use arb_scanner::{Opportunity, PairRisk};
use rust_decimal::{Decimal, RoundingStrategy};

/// 保本价保留的小数位（见 [`Plan::hedged_after`]）。
const HEDGE_PRICE_DP: u32 = 12;

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

impl Rejection {
    /// 因为这一笔**太大**、盘口吃不下被拒的（盘口在限价内吃不满、估算滑点超上限）：单笔改小就可能过。
    /// 其余拒绝（仓位上限、当日亏损、强平距离、盘口不可用……）和单笔大小无关，缩小也没用。
    pub fn is_depth_limited(&self) -> bool {
        matches!(
            self,
            Rejection::BookTooThin { .. } | Rejection::SlippageTooHigh { .. }
        )
    }
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
    /// 这条腿所在场所的单边吃单费率（小数）。决定先下哪条腿（[`Plan::first_leg`]）、
    /// 回滚它要付多少（[`Plan::rollback_allowance`]）。
    #[serde(default)]
    pub fee: Decimal,
    /// `Some(毫秒)` = 这条腿是**只挂单**（post-only）：以 `limit_price` 挂在簿上，最多等这么久，成交多少算多少，
    /// 到点没成交的撤掉。只挂单的腿永远是第一腿（[`Plan::first_leg`]）—— 它成交了才轮到对冲腿出手。
    /// `None` = 普通 IOC 限价。
    #[serde(default)]
    pub maker_rest_ms: Option<u64>,
}

/// 价差单先下哪条腿（[`Plan::first_leg`]）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegOrder {
    /// 难成交（预估滑点大）的先下。资金费单一律如此。
    #[default]
    Slippage,
    /// 两腿单边费率差得明显（≥ [`CHEAP_FIRST_FEE_GAP`]）时**先下费率低的**：它没成交什么都不花；
    /// 它成交了、对面没吃到，回滚它也几乎不付手续费，而对面费率高的那家（Arcus）盘口通常更厚、
    /// 更容易吃到。差得不明显（Hyperliquid xyz 0.009% 对 Lighter 0）仍按 `Slippage`：
    /// 省下的那点手续费抵不上丢掉 Hyperliquid 首腿不等回读的那段速度。
    CheapFirst,
}

/// [`LegOrder::CheapFirst`] 起作用的单边费率差（小数，0.015%）。
pub const CHEAP_FIRST_FEE_GAP: Decimal = Decimal::from_parts(15, 0, 0, false, 5);

/// 一次执行的完整计划。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Plan {
    pub symbol: arb_core::Symbol,
    pub long: LegPlan,
    pub short: LegPlan,
    /// 两腿合计的预期成本（手续费 + 穿价 + 滑点），占名义额的比例。
    pub expected_cost: Decimal,
    /// 价差单的保本线（小数）：两腿实际锁定的价差 (空 − 多) / 中间价 低于它，收敛到目标（没设目标
    /// 按收敛到 0）平仓时扣掉平仓穿价与往返手续费就不赚。第一腿成交后，第二腿的限价按它反推
    /// （[`Self::hedged_after`]）。`None` = 不设（资金费单），第二腿只受「最优价 ± 滑点上限」约束。
    pub min_locked_basis: Option<Decimal>,
    /// 先下哪条腿。闸门默认 [`LegOrder::Slippage`]，价差单由 `desk::prepare` 按请求设。
    #[serde(default)]
    pub leg_order: LegOrder,
}

impl Plan {
    /// 先执行哪条腿：**难成交的那条先做**。
    ///
    /// 如果先做流动性好的那条、难做的那条失败了，你得回滚一条已经建好的腿（多付一次
    /// 手续费）；反过来，难做的那条先失败时你手上**什么都没有**，直接放弃即可。
    ///
    /// [`LegOrder::CheapFirst`] 时两腿费率差得明显，就先下费率低的那条（见 [`LegOrder`]）。
    pub fn first_leg(&self) -> &LegPlan {
        // 只挂单的腿必须先下：它是不确定的那条，对面的对冲腿等它成交了再按成交量去吃。
        if self.long.maker_rest_ms.is_some() {
            return &self.long;
        }
        if self.short.maker_rest_ms.is_some() {
            return &self.short;
        }
        // 价差单里有一条腿在 Hyperliquid xyz 就先下它（两种顺序规则都一样）。
        //
        // 复盘（实盘样本，滑点按比预估差算）：
        // - HL-xyz 当第二腿：自身滑点平均明显更大，约三分之一的单差得较多（有一笔比预估差 0.227%，强制补腿锁成 −0.05%）；
        // - HL-xyz 当第一腿：平均很小，只有极少数差得较多。
        //
        // 它和 Lighter 费率差不到 [`CHEAP_FIRST_FEE_GAP`]，原来按「预估滑点大的先下」，Lighter 零点几个基点的滑点就把它
        // 排到了后面。
        //
        // 先下它，它的价跑了就是第一腿没成交、什么都不花（限价还按保本线封顶，见 [`Self::first_leg_capped`]）；
        // 放到第二腿，已经成交的第一腿只能追价或回滚。
        if self.min_locked_basis.is_some() {
            let xyz = |leg: &LegPlan| leg.venue == arb_core::Venue::HyperliquidXyz;
            match (xyz(&self.long), xyz(&self.short)) {
                (true, false) => return &self.long,
                (false, true) => return &self.short,
                _ => {}
            }
        }
        if self.leg_order == LegOrder::CheapFirst {
            let gap = self.long.fee - self.short.fee;
            if gap.abs() >= CHEAP_FIRST_FEE_GAP {
                return if gap < Decimal::ZERO {
                    &self.long
                } else {
                    &self.short
                };
            }
        }
        if self.long.slippage >= self.short.slippage {
            &self.long
        } else {
            &self.short
        }
    }

    /// 第一腿成交、第二腿吃不到时，回滚第一腿至少要付的（小数）：它开平两次的手续费，加上开仓时
    /// 预估的滑点（当作平仓也要付一次）。**故意取下限**：不算买卖价差、不算这几秒价格往哪走 ——
    /// 用它放宽保本线时宁可放得少。
    pub fn rollback_allowance(&self) -> Decimal {
        let first = self.first_leg();
        (first.fee * Decimal::TWO + first.slippage).max(Decimal::ZERO)
    }

    /// 第二腿实际用的保本线（小数）：[`Self::min_locked_basis`] 减去 [`Self::rollback_allowance`]。
    ///
    /// 第一腿成交之后它的开仓手续费已经付了，两条路比的是：对冲上、收敛到目标平仓，亏
    /// （保本线 − 锁定价差）；回滚，确定亏第一腿的平仓手续费与穿价。锁定价差只差保本线一点点时，
    /// 对冲上比回滚亏得少（还留着收敛得更多、提前止盈的机会）。
    pub fn hedge_floor(&self) -> Option<Decimal> {
        self.min_locked_basis
            .map(|floor| floor - self.rollback_allowance())
    }

    /// 按计划时（现拉盘口）估算的成交均价，两腿锁定的价差（%）：(空 − 多) / 两价中点 × 100。
    pub fn expected_entry_pct(&self) -> Option<Decimal> {
        let (long, short) = (self.long.expected_price, self.short.expected_price);
        let mid = (long + short) / Decimal::TWO;
        (long > Decimal::ZERO && short > Decimal::ZERO)
            .then(|| (short - long) / mid * Decimal::ONE_HUNDRED)
    }

    /// 实际下单用的第一腿：限价再按保本线封顶 —— 假设第二腿按计划时的预估均价成交，第一腿再差，两腿就锁不到
    /// [`Self::hedge_floor`]（第二腿的保本价必然吃不到，只能强制补腿追价或回滚）。
    ///
    /// 原限价是「最优价 ± 滑点上限 0.2%」，比价差单的边际宽得多：第一腿所在那家的价在下单前一瞬间跑了，
    /// 照样能成交在一个注定亏的价上。封顶之后它就只是没成交、什么都不花。
    ///
    /// 只会更严不会更松；只挂单的腿、没有保本线（资金费单）的不动。
    pub fn first_leg_capped(&self) -> LegPlan {
        let mut first = self.first_leg().clone();
        let second = self.second_leg();
        let two = Decimal::TWO;
        let Some(floor) = self.hedge_floor() else {
            return first;
        };
        if first.maker_rest_ms.is_some()
            || second.expected_price <= Decimal::ZERO
            || floor.abs() >= two
        {
            return first;
        }
        // (空 − 多) / ((空 + 多) / 2) ≥ b  ⇔  多 ≤ 空 × (2 − b) / (2 + b)  ⇔  空 ≥ 多 × (2 + b) / (2 − b)。
        match first.side {
            Side::Buy => {
                let cap = (second.expected_price * (two - floor) / (two + floor))
                    .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToNegativeInfinity);
                first.limit_price = first.limit_price.min(cap);
            }
            Side::Sell => {
                let cap = (second.expected_price * (two + floor) / (two - floor))
                    .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToPositiveInfinity);
                first.limit_price = first.limit_price.max(cap);
            }
        }
        first
    }

    pub fn second_leg(&self) -> &LegPlan {
        if std::ptr::eq(self.first_leg(), &self.long) {
            &self.short
        } else {
            &self.long
        }
    }

    /// 第一腿按均价 `first_price` 成交之后的计划：第二腿的限价取原限价（计划时的最优价 ± 滑点上限）
    /// 与按 [`Self::min_locked_basis`] 反推的保本价里**更严**的那个；其余不变。
    ///
    /// 原限价只管「别比计划时的最优价差太多」，管不了第一腿成交在哪、这几秒盘口怎么动。价差单的
    /// 边际常常只有 0.1% 上下，而滑点上限是 0.2%：第二腿可以把整笔的边际吃光还倒贴（某次实盘：
    /// 第二腿比最优价差 0.134%，锁定价差从预估 +0.096% 变成 −0.048%）。保本价以内吃不到就不成交，
    /// 执行器照常回滚第一腿 —— 付一次回滚的成本，不留一笔怎么平都亏的仓位。
    pub fn hedged_after(&self, first_price: Decimal) -> Plan {
        let mut plan = self.clone();
        // 放宽后的保本线；第二腿限价仍取它和原限价里更严的那个，所以不会比原限价更松。
        let Some(floor) = self.hedge_floor() else {
            return plan;
        };
        let two = Decimal::TWO;
        if first_price <= Decimal::ZERO || floor.abs() >= two {
            return plan;
        }
        // (空 − 多) / ((空 + 多) / 2) ≥ b  ⇔  空 ≥ 多 × (2 + b) / (2 − b)。
        // 反推出的价有二十多位小数：先往严的方向截到 12 位（卖向上、买向下），券商再按 tick 往严的方向取整。
        let long_first = std::ptr::eq(self.first_leg(), &self.long);
        if long_first {
            let breakeven = (first_price * (two + floor) / (two - floor))
                .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToPositiveInfinity);
            plan.short.limit_price = plan.short.limit_price.max(breakeven);
        } else {
            let breakeven = (first_price * (two - floor) / (two + floor))
                .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToNegativeInfinity);
            plan.long.limit_price = plan.long.limit_price.min(breakeven);
        }
        plan
    }
}

impl Plan {
    /// 强制补腿（[`crate::executor::PositionSetup::force_hedge`]）用：第一腿按 `first_price` 成交之后，
    /// 第二腿愿意接受的**最差**限价 —— 两腿锁定价差 (空 − 多) / 中间价 不低于 `−tolerance`（小数，0.003 = −0.3%）。
    ///
    /// 和 [`Self::hedged_after`] 是同一个公式，只是把保本线换成 `−tolerance`，所以「锁定价差 ≥ −X%」和保本线是同一个
    /// 口径，买卖两个方向的符号一致；也同样往严的方向截位（卖单向上、买单向下），券商再按 tick 往严取整。
    /// 与 `hedged_after` 不同的是**不**再和「最优价 ± 滑点上限」的原限价取严：保本价没吃到之后才追，
    /// 追的就是比原限价更差的价，上限由 `tolerance` 一个数定。只动第二腿的限价，其余不变。
    /// `tolerance` 非正、第一腿均价不可用时原样返回。
    pub fn forced_after(&self, first_price: Decimal, tolerance: Decimal) -> Plan {
        let mut plan = self.clone();
        let first = self.first_leg().side;
        if let Some(limit) = forced_limit(first, first_price, tolerance) {
            match first {
                Side::Buy => plan.short.limit_price = limit,
                Side::Sell => plan.long.limit_price = limit,
            }
        }
        plan
    }
}

/// [`Plan::forced_after`] 的公式本身：第一腿（方向 `first`）按 `first_price` 成交后，第二腿的最差限价 ——
/// 两腿锁定价差不低于 `−tolerance`（小数）。续上中断的建仓时没有原来的 `Plan`，直接用它。
/// `tolerance` 非正（或大到没有意义）、第一腿均价不可用时返回 `None`。
pub fn forced_limit(first: Side, first_price: Decimal, tolerance: Decimal) -> Option<Decimal> {
    let two = Decimal::TWO;
    let floor = -tolerance;
    if first_price <= Decimal::ZERO || tolerance <= Decimal::ZERO || floor.abs() >= two {
        return None;
    }
    Some(match first {
        // 先买（多腿在先）：第二腿是卖，卖价不低于它。
        Side::Buy => (first_price * (two + floor) / (two - floor))
            .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToPositiveInfinity),
        Side::Sell => (first_price * (two - floor) / (two + floor))
            .round_dp_with_strategy(HEDGE_PRICE_DP, RoundingStrategy::ToNegativeInfinity),
    })
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
    let long = LegPlan {
        fee: ctx.opportunity.long_fee,
        ..leg_plan(
            &ctx.opportunity.symbol,
            ctx.opportunity.long,
            Side::Buy,
            ctx.long_book,
            ctx.size_usdt,
            limits,
        )?
    };
    let short = LegPlan {
        fee: ctx.opportunity.short_fee,
        ..leg_plan(
            &ctx.opportunity.symbol,
            ctx.opportunity.short,
            Side::Sell,
            ctx.short_book,
            ctx.size_usdt,
            limits,
        )?
    };

    // 成本 = 两腿的实际成交价相对**标记价**的偏离。这样它同时包含了穿价与滑点，
    // 而不会重复计入已经在净年化里扣过的手续费。
    let expected_cost = cost_ratio(&long, &short, ctx);

    Ok(Plan {
        symbol: ctx.opportunity.symbol.clone(),
        long,
        short,
        expected_cost,
        // 闸门不知道这是不是价差单、平仓要付多少；价差单由 `desk::prepare` 补上。
        min_locked_basis: None,
        leg_order: LegOrder::Slippage,
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
        fee: Decimal::ZERO,
        maker_rest_ms: None,
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
            long_fee: Decimal::ZERO,
            short_fee: Decimal::ZERO,
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

    fn leg(venue: Venue, side: Side, limit: Decimal, slippage: Decimal) -> LegPlan {
        LegPlan {
            symbol: Symbol::perp("NEAR", "USDT"),
            venue,
            side,
            notional_usdt: dec!(800),
            limit_price: limit,
            best_price: limit,
            expected_price: limit,
            slippage,
            book_notional: dec!(10_000),
            fee: Decimal::ZERO,
            maker_rest_ms: None,
        }
    }

    /// 某次实盘：先在 Arcus 买（预估滑点 0.007%，比 Lighter 的 0 大，所以先做），后在 Lighter 卖。
    fn near_plan(floor: Option<Decimal>) -> Plan {
        Plan {
            symbol: Symbol::perp("NEAR", "USDT"),
            long: leg(Venue::Arcus, Side::Buy, dec!(5.373726), dec!(0.00007)),
            // 最优价 5.3685 × (1 − 0.2%)。
            short: leg(Venue::LighterRh, Side::Sell, dec!(5.3577630), dec!(0)),
            expected_cost: dec!(0.0002),
            min_locked_basis: floor,
            leg_order: LegOrder::Slippage,
        }
    }

    fn locked_basis(long: Decimal, short: Decimal) -> Decimal {
        (short - long) / ((short + long) / Decimal::TWO)
    }

    /// 某次实盘：多 HL-xyz（卖一 256.06）/ 空 Lighter（买一 256.51，预估 256.505），保本线 0.0337%。
    fn ibm_plan(floor: Option<Decimal>) -> Plan {
        let mut long = leg(Venue::HyperliquidXyz, Side::Buy, dec!(256.57212), dec!(0));
        (long.best_price, long.expected_price, long.fee) =
            (dec!(256.06), dec!(256.06), dec!(0.00009));
        let mut short = leg(Venue::Lighter, Side::Sell, dec!(255.99698), dec!(0.0000186));
        (short.best_price, short.expected_price) = (dec!(256.51), dec!(256.50523641));
        Plan {
            symbol: Symbol::perp("IBM", "USDT"),
            long,
            short,
            expected_cost: dec!(0.0002),
            min_locked_basis: floor,
            leg_order: LegOrder::CheapFirst,
        }
    }

    /// 价差单里有 HL-xyz 就先下它：当第二腿时实盘平均比预估差得多，当第一腿只差一点点。
    /// 原来按「预估滑点大的先下」，Lighter 零点几个基点的滑点就把 HL-xyz 排到了后面（某合约 就是这样）。
    #[test]
    fn a_spread_plan_puts_the_hyperliquid_xyz_leg_first() {
        let plan = ibm_plan(Some(dec!(0.000337)));
        assert!(
            plan.short.slippage > plan.long.slippage,
            "按滑点规则本该先下 Lighter"
        );
        assert_eq!(plan.first_leg().venue, Venue::HyperliquidXyz);
        assert_eq!(plan.second_leg().venue, Venue::Lighter);
        let mut by_slippage = plan.clone();
        by_slippage.leg_order = LegOrder::Slippage;
        assert_eq!(by_slippage.first_leg().venue, Venue::HyperliquidXyz);
        // 空腿在 HL-xyz 也一样。
        let mut flipped = plan.clone();
        std::mem::swap(&mut flipped.long.venue, &mut flipped.short.venue);
        assert_eq!(flipped.first_leg().venue, Venue::HyperliquidXyz);
        assert_eq!(flipped.first_leg().side, Side::Sell);
        // 资金费单（没有保本线）不变：照滑点规则。
        let funding = ibm_plan(None);
        assert_eq!(funding.first_leg().venue, Venue::Lighter);
        // 只挂单的腿照样永远先下。
        let mut maker = plan.clone();
        maker.short.maker_rest_ms = Some(8_000);
        assert_eq!(maker.first_leg().venue, Venue::Lighter);
    }

    /// 第一腿的限价也按保本线封顶：假设第二腿按预估均价成交，第一腿再差就锁不住保本线。
    /// 某合约 那一笔要是先下 HL-xyz：它的卖盘已经跳到 256.64，封顶后的限价约 256.465 买不到 —— 什么都不花；
    /// 原限价（最优价 + 0.2% = 256.572）也挡不住它成交在 256.5x 的注定亏的价上。
    #[test]
    fn the_first_leg_limit_is_capped_at_the_breakeven_against_the_second_legs_expected_price() {
        let plan = ibm_plan(Some(dec!(0.000337)));
        // 保本线 0.0337% − 回滚 HL 的成本（开平两次 0.009%）= 0.0157%。
        assert_eq!(plan.hedge_floor(), Some(dec!(0.000157)));
        let first = plan.first_leg_capped();
        assert_eq!(first.venue, Venue::HyperliquidXyz);
        assert!(
            first.limit_price < plan.long.limit_price,
            "{}",
            first.limit_price
        );
        assert_eq!(
            locked_basis(first.limit_price, dec!(256.50523641)).round_dp(9),
            dec!(0.000157)
        );
        assert!(locked_basis(first.limit_price, dec!(256.50523641)) >= dec!(0.000157));
        assert!(first.limit_price.scale() <= 12);
        assert!(dec!(256.64) > first.limit_price, "IBM 当时的卖一买不到");
        assert!(dec!(256.06) < first.limit_price, "计划时的卖一照样买得到");
        // 先卖的方向：卖价不低于 多腿预估 × (2 + b) / (2 − b)。
        let mut near = near_plan(Some(dec!(0.001)));
        near.short.slippage = dec!(0.0003);
        let first = near.first_leg_capped();
        assert_eq!(first.side, Side::Sell);
        assert!(first.limit_price > near.short.limit_price);
        assert!(locked_basis(dec!(5.373726), first.limit_price) >= dec!(0.0007));
        assert_eq!(
            locked_basis(dec!(5.373726), first.limit_price).round_dp(9),
            dec!(0.0007)
        );
        // 封顶只会更严：预估价差很宽时原限价已经更严，不动。
        let mut wide = ibm_plan(Some(dec!(0.000337)));
        wide.short.expected_price = dec!(260);
        assert_eq!(wide.first_leg_capped().limit_price, wide.long.limit_price);
        // 没有保本线、只挂单的腿：不动。
        let funding = ibm_plan(None);
        assert_eq!(
            funding.first_leg_capped().limit_price,
            funding.first_leg().limit_price
        );
        let mut maker = ibm_plan(Some(dec!(0.000337)));
        maker.long.maker_rest_ms = Some(8_000);
        assert_eq!(maker.first_leg_capped().limit_price, maker.long.limit_price);
    }

    #[test]
    fn the_second_leg_cannot_sell_below_the_breakeven_implied_by_the_first_fill() {
        // 保本线 +0.0156%：收敛目标 −0.042% + 平仓穿价与往返手续费。回滚 Arcus 至少要付它的预估滑点
        // 0.007%（这里费率记 0），所以第二腿按 0.0086% 反推：再低就不如回滚。
        let plan = near_plan(Some(dec!(0.000156)));
        assert_eq!(plan.first_leg().venue, Venue::Arcus);
        assert_eq!(plan.rollback_allowance(), dec!(0.00007));
        assert_eq!(plan.hedge_floor(), Some(dec!(0.000086)));
        let hedged = plan.hedged_after(dec!(5.36387));
        let limit = hedged.short.limit_price;
        assert_eq!(
            locked_basis(dec!(5.36387), limit).round_dp(10),
            dec!(0.000086)
        );
        // 当时 Lighter 实际只卖到 5.3613：按保本价这一单不会成交，执行器回滚 Arcus。
        assert!(limit > dec!(5.3613), "{limit}");
        // 截到 12 位、往严的方向（卖单不低于精确的保本价）。
        assert!(limit.scale() <= 12, "{limit}");
        assert!(
            locked_basis(dec!(5.36387), limit) >= dec!(0.000086),
            "{limit}"
        );
        // 只动第二腿，第一腿与先后顺序不变。
        assert_eq!(hedged.long.limit_price, plan.long.limit_price);
        assert_eq!(hedged.first_leg().venue, Venue::Arcus);
    }

    #[test]
    fn a_short_first_leg_caps_the_price_the_long_leg_may_pay() {
        let mut plan = near_plan(Some(dec!(0.001)));
        plan.short.slippage = dec!(0.0003);
        assert_eq!(plan.first_leg().side, Side::Sell);
        // 保本线 0.1% − 回滚空腿的成本 0.03%。
        assert_eq!(plan.hedge_floor(), Some(dec!(0.0007)));
        let hedged = plan.hedged_after(dec!(5.37));
        assert_eq!(
            locked_basis(hedged.long.limit_price, dec!(5.37)).round_dp(10),
            dec!(0.0007)
        );
        assert!(hedged.long.limit_price < dec!(5.37));
        assert!(hedged.long.limit_price.scale() <= 12);
        assert!(locked_basis(hedged.long.limit_price, dec!(5.37)) >= dec!(0.0007));
        assert_eq!(hedged.short.limit_price, plan.short.limit_price);
    }

    #[test]
    fn the_gate_copies_each_legs_fee_from_the_opportunity_so_a_spread_plan_can_put_lighter_first() {
        // 线上快照的口径：多 Arcus（0.0225%）、空 Lighter RH（0）。
        let mut op = opportunity(dec!(0.1));
        op.long = Venue::Arcus;
        op.short = Venue::LighterRh;
        op.long_fee = dec!(0.000225);
        op.short_fee = Decimal::ZERO;
        let long = book(dec!(99.99), dec!(100.00), dec!(50_000));
        let short = book(dec!(100.20), dec!(100.21), dec!(50_000));
        let mut plan = plan(&preflight(&op, &long, &short, dec!(800)), &limits()).unwrap();
        assert_eq!(
            (plan.long.fee, plan.short.fee),
            (dec!(0.000225), Decimal::ZERO)
        );
        assert_eq!(
            plan.leg_order,
            LegOrder::Slippage,
            "闸门默认按滑点；价差单由 desk::prepare 设"
        );
        plan.leg_order = LegOrder::CheapFirst;
        assert_eq!(plan.first_leg().venue, Venue::LighterRh);
    }

    #[test]
    fn a_spread_plan_puts_the_much_cheaper_leg_first_and_otherwise_keeps_the_slippage_rule() {
        // Arcus 0.0225% / Lighter 0：差 0.0225% ≥ 0.015% —— 先下 Lighter（它没成交什么都不花），
        // 哪怕 Arcus 的预估滑点更大。
        let mut plan = near_plan(Some(dec!(0.002377)));
        plan.long.fee = dec!(0.000225);
        plan.short.fee = Decimal::ZERO;
        assert_eq!(
            plan.first_leg().venue,
            Venue::Arcus,
            "默认（资金费单）仍按滑点"
        );
        plan.leg_order = LegOrder::CheapFirst;
        assert_eq!(plan.first_leg().venue, Venue::LighterRh);
        assert_eq!(plan.second_leg().venue, Venue::Arcus);
        // 回滚的是 Lighter：费率 0、滑点 0 —— 保本线不放宽。
        assert_eq!(plan.rollback_allowance(), Decimal::ZERO);
        assert_eq!(plan.hedge_floor(), Some(dec!(0.002377)));
        // 反过来（多 Lighter / 空 Arcus）也一样先下 Lighter。
        let mut flipped = plan.clone();
        flipped.long.fee = Decimal::ZERO;
        flipped.short.fee = dec!(0.000225);
        flipped.long.venue = Venue::LighterRh;
        flipped.short.venue = Venue::Arcus;
        assert_eq!(flipped.first_leg().venue, Venue::LighterRh);
        // HL xyz 0.009% / Lighter 0：差得不明显，照滑点规则（保留 HL 首腿不等回读的速度）。
        let mut close = plan.clone();
        close.long.fee = dec!(0.00009);
        assert_eq!(close.first_leg().venue, Venue::Arcus);
        // 费率一样：照滑点规则。
        let mut equal = plan.clone();
        equal.long.fee = dec!(0.0005);
        equal.short.fee = dec!(0.0005);
        assert_eq!(equal.first_leg().venue, Venue::Arcus);
    }

    #[test]
    fn rolling_back_an_expensive_first_leg_relaxes_the_hedge_floor_but_never_past_the_original_limit()
     {
        // 按滑点先下 Arcus（费率 0.0225%、滑点 0.007%）：回滚至少 0.045% + 0.007%。
        let mut plan = near_plan(Some(dec!(0.002377)));
        plan.long.fee = dec!(0.000225);
        assert_eq!(plan.first_leg().venue, Venue::Arcus);
        assert_eq!(plan.rollback_allowance(), dec!(0.00052));
        assert_eq!(plan.hedge_floor(), Some(dec!(0.001857)));
        let strict = Plan {
            min_locked_basis: Some(dec!(0.002377)),
            long: LegPlan {
                fee: Decimal::ZERO,
                slippage: Decimal::ZERO,
                ..plan.long.clone()
            },
            ..plan.clone()
        }
        .hedged_after(dec!(5.36387));
        let relaxed = plan.hedged_after(dec!(5.36387));
        assert!(
            relaxed.short.limit_price < strict.short.limit_price,
            "放宽后第二腿能卖得更低一点"
        );
        assert_eq!(
            locked_basis(dec!(5.36387), relaxed.short.limit_price).round_dp(10),
            dec!(0.001857)
        );
        // 放宽得再多，第二腿限价也不比原限价（最优价 − 0.2%）更松。
        let mut wide = plan.clone();
        wide.long.slippage = dec!(0.004);
        assert_eq!(
            wide.hedged_after(dec!(5.36387)).short.limit_price,
            dec!(5.3577630)
        );
    }

    #[test]
    fn the_original_limit_stays_when_it_is_already_stricter_or_there_is_no_floor() {
        // 保本线是 −1%：反推出的价比「最优价 − 0.2%」还宽，保留原限价。
        let loose = near_plan(Some(dec!(-0.01))).hedged_after(dec!(5.36387));
        assert_eq!(loose.short.limit_price, dec!(5.3577630));
        // 资金费单没有保本线。
        let none = near_plan(None).hedged_after(dec!(5.36387));
        assert_eq!(none.short.limit_price, dec!(5.3577630));
        // 第一腿均价不可用时不动。
        let zero = near_plan(Some(dec!(0.000156))).hedged_after(Decimal::ZERO);
        assert_eq!(zero.short.limit_price, dec!(5.3577630));
    }

    /// 强制补腿的最差限价：两个方向都按「锁定价差 ≥ −X%」反推，且比保本价那一档宽。
    #[test]
    fn the_forced_hedge_limit_allows_a_negative_locked_spread_in_both_directions() {
        let tolerance = dec!(0.003);
        // 先买（Arcus）、后卖（Lighter）：第二腿卖单的最低价。
        let plan = near_plan(Some(dec!(0.000156)));
        assert_eq!(plan.first_leg().venue, Venue::Arcus);
        let forced = plan.forced_after(dec!(5.36387), tolerance);
        let floor_limit = forced.short.limit_price;
        let locked = locked_basis(dec!(5.36387), floor_limit);
        assert!(
            locked >= -tolerance && locked < -tolerance + dec!(0.000000001),
            "{locked}"
        );
        assert!(
            floor_limit < dec!(5.36387),
            "比第一腿买入价低：锁定价差为负"
        );
        // 比保本价宽，也比「最优价 − 0.2%」的原限价宽（这正是追单的意义）。
        assert!(floor_limit < plan.hedged_after(dec!(5.36387)).short.limit_price);
        assert!(
            floor_limit < plan.short.limit_price,
            "{floor_limit} 应比原限价 {} 更松",
            plan.short.limit_price
        );
        assert!(floor_limit.scale() <= 12, "{floor_limit}");
        // 只动第二腿。
        assert_eq!(forced.long.limit_price, plan.long.limit_price);
        assert_eq!(forced.first_leg().venue, Venue::Arcus);

        // 先卖（Lighter）、后买（Arcus）：第二腿买单的最高价。
        let mut flipped = near_plan(Some(dec!(0.001)));
        flipped.short.slippage = dec!(0.01);
        assert_eq!(flipped.first_leg().venue, Venue::LighterRh);
        let forced = flipped.forced_after(dec!(5.36387), tolerance);
        let ceiling = forced.long.limit_price;
        // 第一腿卖在 5.36387，第二腿最多买到 ceiling：锁定价差 (卖 − 买) / 中间价 恰好不低于 −X%。
        let locked = locked_basis(ceiling, dec!(5.36387));
        assert!(
            locked >= -tolerance && locked < -tolerance + dec!(0.000000001),
            "{locked}"
        );
        assert!(ceiling > dec!(5.36387), "比第一腿卖出价高：锁定价差为负");
        assert!(ceiling > flipped.hedged_after(dec!(5.36387)).long.limit_price);
        assert_eq!(forced.short.limit_price, flipped.short.limit_price);
    }

    #[test]
    fn the_forced_hedge_limit_is_left_alone_without_a_usable_tolerance_or_price() {
        let plan = near_plan(Some(dec!(0.000156)));
        for (price, tolerance) in [
            (dec!(5.36387), Decimal::ZERO),
            (dec!(5.36387), dec!(-0.001)),
            (Decimal::ZERO, dec!(0.003)),
            (dec!(5.36387), dec!(2)),
        ] {
            let same = plan.forced_after(price, tolerance);
            assert_eq!(
                same.short.limit_price, plan.short.limit_price,
                "{price} {tolerance}"
            );
            assert_eq!(same.long.limit_price, plan.long.limit_price);
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
