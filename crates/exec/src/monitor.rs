//! 持仓期间的任务规则：开仓只是开始，资金费套利是在持有期里死掉的。
//!
//! 规则（见 [`TaskRules`]）：
//!
//! | 规则 | 触发 | 动作 |
//! | --- | --- | --- |
//! | 数量失衡 | 两腿标的数量偏差 > `size_mismatch_pct` | 整笔平仓 |
//! | 基差收敛 | 标记价基差 ≤ `basis_exit_pct`，且按盘口平仓为正 | 整笔平仓 |
//! | 止盈 | **含资金费的净盈利** ≥ `take_profit_usdt`，且按盘口平仓后仍达标 | 整笔平仓 |
//! | 费差自动平仓 | 最近几小时**平均**毛费差年化 < `min_funding_apr` | 整笔平仓 |
//! | 自动加保证金 | 任一腿强平距离 < `auto_margin_pct`（要同时设累计上限） | 往这条腿补保证金，距离拉回 1.5 倍 |
//! | 爆仓保护 | 任一腿强平距离 < `liq_protection_pct` | 两腿等比例减仓，距离拉回 1.5 倍 |
//!
//! 同时触发时只执行优先级最高的一条：失衡意味着对冲已经不成立，先于一切；
//! 平仓会顺带解决强平距离，所以平仓类规则排在补保证金与减仓前面；补保证金排在减仓前面，
//! 补不了（上限用完、账户可用资金不够、场所拒绝）时由调用方退回减仓（[`Evaluation::fallback`]）。
//!
//! 评估只读快照、不下单；动作由调用方交给 [`crate::Executor`] 执行。这样同一份评估
//! 既能驱动纸面执行，也能在只看不做的模式下打印出来。
//!
//! # 基差收敛平仓：标记价只触发，盘口说了算
//!
//! 「标记价基差 ≤ 目标」只是**触发**。标记价不在买卖盘中间，平仓却要在空腿的卖盘买回、
//! 在多腿的买盘卖出（2026-09-30 NEAR：标记价基差已收敛到 0.09%，按盘口平仓的价差却是
//! 0.196%，比开仓时还宽，这笔亏了 1.29 USDT）。所以触发后由调用方现拉两边盘口，按持仓
//! 数量算出「现在平掉整笔」的预估盈亏（[`ExitQuote`]，含已付和将付的手续费）；为正才平，
//! 不为正、或者盘口拿不到，就继续持有并写明原因 —— 绝不退回按标记价平仓。

use arb_core::{Decimal, MarketSnapshot, Side, Venue};
use arb_scanner::leverage::{
    Health, distance_to_liquidation_pct, liquidation_distance_pct, liquidation_price,
    margin_for_distance, trim_fraction,
};
use arb_scanner::{to_apr, to_daily};
use serde::Serialize;

use crate::types::{LegFill, PairPosition, PositionStatus, TaskRules};

/// 按盘口「现在平掉整笔」的预估。只在基差收敛触发后才去拉盘口算。
#[derive(Debug, Clone, Serialize)]
pub struct ExitQuote {
    /// 多腿卖出的预估均价（按持仓数量吃买盘）。
    pub long_price: Decimal,
    /// 空腿买回的预估均价（按持仓数量吃卖盘）。
    pub short_price: Decimal,
    /// 按这两个价平仓的成交价差（%）：(空腿买回价 − 多腿卖出价) / 中间价 × 100。
    pub exit_basis_pct: Decimal,
    /// 预估平仓手续费（每条腿按它开仓时实际付的费率）。
    pub exit_fee_usdt: Decimal,
    /// 现在平掉整笔的预估已实现盈亏：两腿价格盈亏 + 已减仓部分的盈亏 − 全部手续费。
    /// 不含资金费（台账不记资金费流水）。
    pub net_usdt: Decimal,
    /// 占多腿入场名义的百分比。
    pub net_pct: Decimal,
}

/// 平仓前的盘口核对结果。
#[derive(Debug, Clone, Default)]
pub enum ExitCheck {
    /// 没去拉盘口（持仓页的展示、或基差收敛没触发）。
    #[default]
    NotFetched,
    /// 拉了但用不了：盘口没拉到、深度不够这笔数量。
    Unavailable(String),
    Quoted(ExitQuote),
}

/// 按数量吃一侧盘口的均价。深度不够这个数量时为 `None`。
fn walk_quantity(levels: &[arb_core::Level], quantity: Decimal) -> Option<Decimal> {
    if quantity <= Decimal::ZERO {
        return None;
    }
    let (mut left, mut cost) = (quantity, Decimal::ZERO);
    for level in levels.iter().filter(|level| level.price > Decimal::ZERO) {
        let take = left.min(level.notional_usdt / level.price);
        cost += take * level.price;
        left -= take;
        if left <= Decimal::ZERO {
            return Some(cost / quantity);
        }
    }
    None
}

/// 按两边现拉的盘口，估算现在平掉这笔双腿持仓的结果。
pub fn exit_quote(
    position: &PairPosition,
    long_book: &arb_core::OrderBook,
    short_book: &arb_core::OrderBook,
) -> Result<ExitQuote, String> {
    let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
    else {
        return Err("不是完整的双腿持仓".into());
    };
    let (Some(long_qty), Some(short_qty)) = (long_leg.quantity(), short_leg.quantity()) else {
        return Err("入场价非正，算不出数量".into());
    };
    let long_price = walk_quantity(&long_book.bids, long_qty).ok_or_else(|| {
        format!(
            "{} 买盘不够卖出这条腿的 {} 个",
            long_leg.venue,
            long_qty.round_dp(4)
        )
    })?;
    let short_price = walk_quantity(&short_book.asks, short_qty).ok_or_else(|| {
        format!(
            "{} 卖盘不够买回这条腿的 {} 个",
            short_leg.venue,
            short_qty.round_dp(4)
        )
    })?;
    // 平仓费率按这条腿开仓时实际付的（账户真实费率）；减过仓的腿会略偏高，偏保守。
    let fee_rate = |leg: &LegFill| {
        if leg.notional_usdt > Decimal::ZERO {
            leg.fee_usdt / leg.notional_usdt
        } else {
            Decimal::ZERO
        }
    };
    let exit_fee_usdt =
        fee_rate(long_leg) * long_qty * long_price + fee_rate(short_leg) * short_qty * short_price;
    let price_pnl = (long_price - long_leg.average_price) * long_qty
        + (short_leg.average_price - short_price) * short_qty;
    let net_usdt = price_pnl + position.realized_pnl_usdt
        - position.realized_fee_usdt
        - long_leg.fee_usdt
        - short_leg.fee_usdt
        - exit_fee_usdt;
    let mid = (long_price + short_price) / Decimal::TWO;
    Ok(ExitQuote {
        long_price: long_price.round_dp(8),
        short_price: short_price.round_dp(8),
        exit_basis_pct: ((short_price - long_price) / mid * Decimal::ONE_HUNDRED).round_dp(4),
        exit_fee_usdt: exit_fee_usdt.round_dp(8),
        net_usdt: net_usdt.round_dp(8),
        net_pct: if long_leg.notional_usdt > Decimal::ZERO {
            (net_usdt / long_leg.notional_usdt * Decimal::ONE_HUNDRED).round_dp(4)
        } else {
            Decimal::ZERO
        },
    })
}

/// 费差自动平仓判断用的平均窗口（小时）：单个小时的费率会跳，取几小时的均值，
/// 一次瞬时波动不会把仓位平掉，真正的反转则在几小时内就会被均值反映出来。
pub const FUNDING_AVG_HOURS: i64 = 6;

/// 费差规则这一轮用哪个数：有最近几小时的均值就用均值，否则退回当前读数。
pub fn funding_measure(current_apr: Decimal, average_apr: Option<Decimal>) -> Decimal {
    average_apr.unwrap_or(current_apr)
}

/// 一笔持仓按标记价估算的净额，**不含资金费**：价格盈亏 + 已减仓部分的已实现盈亏 − 已付手续费。
fn held_net_usdt(position: &PairPosition, pnl: &PairPnl) -> Option<Decimal> {
    pnl.price_pnl_usdt.map(|price| {
        price + position.realized_pnl_usdt - position.realized_fee_usdt - pnl.fees_usdt
    })
}

/// 止盈规则这一轮会不会触发（按标记价估算，含资金费）。触发了调用方才去拉盘口核对。
///
/// 资金费没查到时不触发：不知道的不按 0 算。
pub fn take_profit_triggered(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    funding_usdt: Option<Decimal>,
) -> bool {
    let Some(target) = position.rules.take_profit_usdt else {
        return false;
    };
    let (Some(long_leg), Some(short_leg), Some(funding)) = (
        position.long.as_ref(),
        position.short.as_ref(),
        funding_usdt,
    ) else {
        return false;
    };
    let mark = |snapshot: &MarketSnapshot| snapshot.mark_price.or(snapshot.index_price);
    let pnl = pair_pnl(long_leg, short_leg, long, short, mark(long), mark(short));
    held_net_usdt(position, &pnl).is_some_and(|net| net + funding >= target)
}

/// 费差规则这一轮会不会触发（要不要去拉盘口核对）。
pub fn funding_exit_triggered(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    average_apr: Option<Decimal>,
) -> bool {
    let Some(min) = position.rules.min_funding_apr else {
        return false;
    };
    let current = to_apr(
        to_daily(short.period_rate, short.interval_h) - to_daily(long.period_rate, long.interval_h),
    );
    funding_measure(current, average_apr) < min
}

/// 现在平掉这笔要多付多少：预估平仓手续费，加上两腿按盘口成交相对标记价的穿价。
/// 缺标记价或数量时只算手续费。
fn exit_cost_usdt(observation: &Observation, quote: &ExitQuote) -> Decimal {
    let crossing = |mark: Option<Decimal>,
                    price: Decimal,
                    quantity: Option<Decimal>,
                    sell: bool| match (mark, quantity) {
        (Some(mark), Some(quantity)) => {
            let per_unit = if sell { mark - price } else { price - mark };
            (per_unit * quantity).max(Decimal::ZERO)
        }
        _ => Decimal::ZERO,
    };
    quote.exit_fee_usdt
        + crossing(
            observation.long.mark_price,
            quote.long_price,
            observation.pnl.long.quantity,
            true,
        )
        + crossing(
            observation.short.mark_price,
            quote.short_price,
            observation.pnl.short.quantity,
            false,
        )
}

/// 基差收敛规则这一轮会不会触发（标记价基差 ≤ 目标）。触发了调用方才去拉盘口核对。
pub fn basis_exit_triggered(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
) -> bool {
    let Some(target) = position.rules.basis_exit_pct else {
        return false;
    };
    let mark = |snapshot: &MarketSnapshot| snapshot.mark_price.or(snapshot.index_price);
    mark(long)
        .zip(mark(short))
        .and_then(|(long, short)| mark_basis_pct(long, short))
        .is_some_and(|basis| basis <= target)
}

fn mark_basis_pct(long: Decimal, short: Decimal) -> Option<Decimal> {
    (long + short > Decimal::ZERO).then(|| {
        ((short - long) / ((short + long) / Decimal::TWO) * Decimal::ONE_HUNDRED).round_dp(4)
    })
}

/// 数量失衡门槛的下限（%）。再小就会被正常的下单取整误差触发。
pub const MIN_SIZE_MISMATCH_PCT: Decimal = Decimal::from_parts(5, 0, 0, false, 1);
/// 数量失衡门槛的上限（%）。
pub const MAX_SIZE_MISMATCH_PCT: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
/// 爆仓保护减仓后要回到的距离 = 门槛 × 1.5。
pub const PROTECTION_TARGET_MULTIPLE: Decimal = Decimal::from_parts(15, 0, 0, false, 1);

/// 开仓前校验规则。`opening_distance_pct` 是两腿里更近的开仓强平距离。
///
/// 爆仓保护门槛不低于开仓距离时，开仓第一轮就会触发减仓 —— 那不是保护，
/// 是把一笔新仓立刻砍掉一截，所以直接拒绝。
pub fn validate_rules(
    rules: &TaskRules,
    opening_distance_pct: Option<Decimal>,
) -> Result<(), String> {
    validate_rules_inner(rules, opening_distance_pct, true)
}

/// 已开仓位改规则：仍校验数值、上限及强平数据可用性，但允许已满足触发条件。
/// 写入前由调用方评估当前动作并要求操作者显式确认。
pub fn validate_rules_update(
    rules: &TaskRules,
    current_distance_pct: Option<Decimal>,
) -> Result<(), String> {
    validate_rules_inner(rules, current_distance_pct, false)
}

fn validate_rules_inner(
    rules: &TaskRules,
    distance_pct: Option<Decimal>,
    reject_immediate: bool,
) -> Result<(), String> {
    if let Some(pct) = rules.size_mismatch_pct
        && !(MIN_SIZE_MISMATCH_PCT..=MAX_SIZE_MISMATCH_PCT).contains(&pct)
    {
        return Err(format!(
            "数量失衡门槛 {pct}% 必须在 {MIN_SIZE_MISMATCH_PCT}% ~ {MAX_SIZE_MISMATCH_PCT}%"
        ));
    }
    // 开仓拒绝立即触发；持仓修改交给调用方的显式确认。两者都必须能评估强平风险。
    for (name, effect, threshold) in [
        ("爆仓保护", "减仓", rules.liq_protection_pct),
        ("自动加保证金", "补保证金", rules.auto_margin_pct),
    ] {
        let Some(pct) = threshold else { continue };
        if pct <= Decimal::ZERO {
            return Err(format!("{name}门槛 {pct}% 必须为正"));
        }
        match distance_pct {
            None => {
                return Err(format!(
                    "至少一条腿拿不到维持保证金率，算不出强平距离，{name}无从执行"
                ));
            }
            Some(opening) if reject_immediate && pct >= opening => {
                return Err(format!(
                    "{name}门槛 {pct}% 不低于开仓强平距离 {}%，开仓就会触发{effect}",
                    opening.round_dp(2)
                ));
            }
            Some(_) => {}
        }
    }
    if let Some(pct) = rules.basis_exit_pct
        && !(-MAX_BASIS_EXIT_PCT..=MAX_BASIS_EXIT_PCT).contains(&pct)
    {
        return Err(format!(
            "基差收敛目标 {pct}% 必须在 ±{MAX_BASIS_EXIT_PCT}% 之内"
        ));
    }
    if let Some(target) = rules.take_profit_usdt
        && target <= Decimal::ZERO
    {
        return Err(format!("止盈目标 {target} USDT 必须为正"));
    }
    match (rules.auto_margin_pct, rules.auto_margin_max_usdt) {
        (Some(_), None) => {
            return Err(
                "自动加保证金必须同时设置累计上限（USDT）：不设上限就不启用，\
                 免得一次异常把账户里的钱一笔笔补进去"
                    .into(),
            );
        }
        (None, Some(_)) => return Err("设了自动加保证金的累计上限，但没设触发它的强平距离".into()),
        (Some(_), Some(max)) if max <= Decimal::ZERO => {
            return Err(format!("自动加保证金的累计上限 {max} USDT 必须为正"));
        }
        _ => {}
    }
    if let (Some(auto), Some(protection)) = (rules.auto_margin_pct, rules.liq_protection_pct)
        && auto <= protection
    {
        return Err(format!(
            "自动加保证金的触发线 {auto}% 必须高于爆仓保护的门槛 {protection}%：\
             否则减仓会抢在加保证金之前"
        ));
    }
    Ok(())
}

/// 基差收敛目标的合理范围（%）。超出它多半是把小数当成了百分数。
pub const MAX_BASIS_EXIT_PCT: Decimal = Decimal::from_parts(5, 0, 0, false, 0);

/// 一条腿此刻的状况。
#[derive(Debug, Clone, Serialize)]
pub struct LegStatus {
    pub venue: Venue,
    pub side: Side,
    pub mark_price: Option<Decimal>,
    /// 强平价：交易所报告了就用它，否则按保证金与维持保证金率算。
    pub liquidation_price: Option<Decimal>,
    /// 强平价是交易所报告的（而不是我们按台账算的）。
    pub liquidation_from_venue: bool,
    /// 计算强平价时用的保证金：交易所实际的，缺了才用台账里开仓时的。
    pub margin_usdt: Option<Decimal>,
    /// 上面的保证金来自交易所。
    pub margin_from_venue: bool,
    /// 当前价到强平价的距离（%）。缺保证金或维持保证金率时为 `None`。
    pub distance_pct: Option<Decimal>,
    pub health: Option<Health>,
}

/// 一次评估看到的东西。无论动作是什么都完整输出，方便对着台账复盘。
#[derive(Debug, Clone, Serialize)]
pub struct Observation {
    pub long: LegStatus,
    pub short: LegStatus,
    /// 当前毛费差年化（小数）：空腿日化 − 多腿日化，× 365。
    pub funding_apr: Decimal,
    /// 最近几小时（[`FUNDING_AVG_HOURS`]）费差均值的年化。费差自动平仓按它判断；
    /// 没拉到历史时为 `None`，规则退回按当前读数。
    pub funding_avg_apr: Option<Decimal>,
    /// 两腿标的数量偏差（%）。
    pub size_mismatch_pct: Option<Decimal>,
    /// 当前标记价基差（%）：(空腿标记价 − 多腿标记价) / 中间价 × 100。与入场基差同口径。
    /// 任一腿缺标记价时为 `None`。
    pub current_basis_pct: Option<Decimal>,
    /// 按当前标记价的双边盈亏。
    pub pnl: PairPnl,
    /// 基差收敛触发后，按两边盘口现在平掉整笔的预估。没触发或没拉到盘口时为 `None`。
    pub exit: Option<ExitQuote>,
    /// 开仓以来这笔仓位已收付的资金费合计（交易所结算流水，正 = 收到）。没查到为 `None`。
    pub funding_usdt: Option<Decimal>,
    /// 含资金费的净额（按标记价）：价格盈亏 + 已减仓部分的已实现盈亏 + 资金费 − 已付手续费。
    /// 不含平仓手续费与盘口穿价；止盈触发后另按盘口核对。资金费或标记价缺失时为 `None`。
    pub net_with_funding_usdt: Option<Decimal>,
    /// 拉了盘口但用不了（深度不够、请求失败）的原因。没拉或拉到了为 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_unavailable: Option<String>,
}

/// 止盈按标记价已达标、但按盘口核对后没平仓：留给日志与告警，事后能查。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TakeProfitHold {
    pub target_usdt: Decimal,
    /// 按标记价、含资金费的净额（触发依据）。
    pub mark_net_usdt: Decimal,
    /// 按盘口现在平掉整笔、含资金费的预估净额。盘口用不了时为 `None`。
    pub book_net_usdt: Option<Decimal>,
    pub funding_usdt: Decimal,
    /// 人看的原因（与 `skipped` 里那条相同）。
    pub reason: String,
}

impl Observation {
    /// 填上资金费流水，并算出含资金费的净额：价格盈亏 + 已减仓部分的已实现盈亏 + 资金费 − 已付手续费。
    /// 评估与展示共用这一处，口径不会漂开。资金费或标记价缺失时净额是 `None`。
    pub fn set_funding(&mut self, position: &PairPosition, funding_usdt: Option<Decimal>) {
        self.funding_usdt = funding_usdt;
        self.net_with_funding_usdt = held_net_usdt(position, &self.pnl)
            .zip(funding_usdt)
            .map(|(net, funding)| (net + funding).round_dp(8));
    }
}

/// 一条腿按当前标记价的浮动盈亏（不含资金费）。
#[derive(Debug, Clone, Serialize)]
pub struct LegPnl {
    /// 当前持有的标的数量（减仓后按比例变小）。入场价非正时为 `None`。
    pub quantity: Option<Decimal>,
    pub entry_price: Decimal,
    pub mark_price: Option<Decimal>,
    /// (标记价 − 入场价) × 数量；空腿取反。缺标记价时为 `None`。
    pub price_pnl_usdt: Option<Decimal>,
    /// 价格盈亏占这条腿入场名义的百分比。
    pub price_pnl_pct: Option<Decimal>,
    /// 这条腿已付的手续费（开仓 + 历次减仓）。
    pub fee_usdt: Decimal,
}

/// 整笔仓位的盈亏。
///
/// **只含价格盈亏与已付手续费。** 台账不记资金费流水，持有至今的累计资金费算不出来，
/// 所以只给按当前费率估算的每日资金费；平仓手续费也还没发生，不计入。减过仓的仓位，
/// 减掉那部分的已实现盈亏记在了保证金里，这里只算剩余数量。
#[derive(Debug, Clone, Serialize)]
pub struct PairPnl {
    pub long: LegPnl,
    pub short: LegPnl,
    /// 两腿价格盈亏合计。对冲下接近 0，偏离来自两腿价差（基差）的变化。
    pub price_pnl_usdt: Option<Decimal>,
    /// 两腿已付手续费合计。
    pub fees_usdt: Decimal,
    /// 价格盈亏 − 已付手续费。
    pub net_usdt: Option<Decimal>,
    /// 按当前费率估算的每日资金费（正 = 净收入）：空腿日费率 × 空腿名义 − 多腿日费率 ×
    /// 多腿名义，名义按标记价。费率一变它就变，不是承诺。
    pub funding_daily_usdt: Option<Decimal>,
}

fn leg_pnl(leg: &LegFill, mark: Option<Decimal>) -> LegPnl {
    let quantity = leg.quantity();
    let price_pnl_usdt = mark.zip(quantity).map(|(mark, quantity)| {
        let raw = (mark - leg.average_price) * quantity;
        match leg.side {
            Side::Buy => raw,
            Side::Sell => -raw,
        }
    });
    let price_pnl_pct = price_pnl_usdt
        .filter(|_| leg.notional_usdt > Decimal::ZERO)
        .map(|pnl| (pnl / leg.notional_usdt * Decimal::ONE_HUNDRED).round_dp(4));
    // 展示用：金额保留 8 位小数足够（交易所手续费也只到这个量级）。
    let price_pnl_usdt = price_pnl_usdt.map(|pnl| pnl.round_dp(8));
    LegPnl {
        quantity,
        entry_price: leg.average_price,
        mark_price: mark,
        price_pnl_usdt,
        price_pnl_pct,
        fee_usdt: leg.fee_usdt,
    }
}

fn pair_pnl(
    long_leg: &LegFill,
    short_leg: &LegFill,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    long_mark: Option<Decimal>,
    short_mark: Option<Decimal>,
) -> PairPnl {
    let long_pnl = leg_pnl(long_leg, long_mark);
    let short_pnl = leg_pnl(short_leg, short_mark);
    let price_pnl_usdt = long_pnl
        .price_pnl_usdt
        .zip(short_pnl.price_pnl_usdt)
        .map(|(a, b)| a + b);
    let fees_usdt = long_leg.fee_usdt + short_leg.fee_usdt;
    let marked = |pnl: &LegPnl| pnl.quantity.zip(pnl.mark_price).map(|(q, m)| q * m);
    let funding_daily_usdt =
        marked(&long_pnl)
            .zip(marked(&short_pnl))
            .map(|(long_notional, short_notional)| {
                to_daily(short.period_rate, short.interval_h) * short_notional
                    - to_daily(long.period_rate, long.interval_h) * long_notional
            });
    PairPnl {
        long: long_pnl,
        short: short_pnl,
        price_pnl_usdt,
        net_usdt: price_pnl_usdt.map(|pnl| (pnl - fees_usdt).round_dp(8)),
        fees_usdt: fees_usdt.round_dp(8),
        funding_daily_usdt: funding_daily_usdt.map(|daily| daily.round_dp(8)),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    Hold,
    Close {
        reason: String,
    },
    Trim {
        fraction: Decimal,
        reason: String,
    },
    /// 往这条腿的逐仓保证金里补钱。金额已按上限与「至少 1 USDT」夹过。
    AddMargin {
        venue: Venue,
        /// 这条腿的方向（多 = `Buy`）。
        side: Side,
        amount_usdt: Decimal,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct Evaluation {
    pub observation: Observation,
    pub action: Action,
    /// `action` 是补保证金、而补不了时退回去做的动作（爆仓保护算出来的减仓 / 平仓）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback: Option<Action>,
    /// 有规则开着但这一轮评估不了（比如缺维持保证金率）。不评估不等于没事，要说出来。
    pub skipped: Vec<String>,
    /// 止盈按标记价达标、但盘口核对没通过（或盘口用不了）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit_hold: Option<TakeProfitHold>,
}

fn leg_status(
    leg: &LegFill,
    snapshot: &MarketSnapshot,
    state: Option<&crate::broker::VenueLegState>,
    mode: crate::MarginMode,
) -> LegStatus {
    let mode = state.and_then(|state| state.margin_mode).unwrap_or(mode);
    let mark = snapshot.mark_price.or(snapshot.index_price);
    // 交易所实际的保证金优先：补过保证金后台账里的开仓时保证金早就不对了。
    let venue_margin = state
        .and_then(|state| state.margin_usdt)
        .filter(|margin| *margin > Decimal::ZERO);
    let margin = if mode.is_cross() {
        None
    } else {
        venue_margin.or(leg.margin_usdt)
    };
    let computed = margin
        .zip(snapshot.maintenance_margin)
        .and_then(|(margin, mmr)| {
            liquidation_price(leg.average_price, leg.notional_usdt, margin, mmr, leg.side)
        });
    let venue_liquidation = state
        .and_then(|state| state.liquidation_price)
        .filter(|price| *price > Decimal::ZERO);
    let liquidation = venue_liquidation.or(computed);
    let distance = mark
        .zip(liquidation)
        .and_then(|(mark, liquidation)| distance_to_liquidation_pct(mark, liquidation, leg.side));
    LegStatus {
        venue: leg.venue,
        side: leg.side,
        mark_price: mark,
        liquidation_price: liquidation,
        liquidation_from_venue: venue_liquidation.is_some(),
        margin_usdt: margin,
        margin_from_venue: venue_margin.is_some(),
        distance_pct: distance,
        health: distance.map(Health::of),
    }
}

fn mismatch_pct(long: &LegFill, short: &LegFill) -> Option<Decimal> {
    let (a, b) = (long.quantity()?, short.quantity()?);
    let larger = a.max(b);
    (larger > Decimal::ZERO).then(|| (a - b).abs() / larger * Decimal::ONE_HUNDRED)
}

/// 评估一笔持仓。只评估**完整的双腿持仓**：裸腿与回滚中的仓位归执行器与对账管。
///
/// `long` / `short` 必须是这两条腿所在场所的最新快照。不带盘口核对：基差收敛即使触发
/// 也只是保持并说明（见模块文档），真正平不平由 [`evaluate_with_exit`] 决定。
pub fn evaluate(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
) -> Option<Evaluation> {
    evaluate_with_exit(position, long, short, ExitCheck::NotFetched, None)
}

/// 同 [`evaluate`]，附带平仓前的盘口核对结果。
pub fn evaluate_with_exit(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    exit: ExitCheck,
    funding_avg_apr: Option<Decimal>,
) -> Option<Evaluation> {
    evaluate_full(
        position,
        long,
        short,
        Inputs {
            exit,
            funding_avg_apr,
            ..Inputs::default()
        },
    )
}

/// 评估一笔持仓需要的、**不在台账与快照里**的输入（都是这一轮现查的）。
#[derive(Debug, Clone, Default)]
pub struct Inputs<'a> {
    /// 平仓前的盘口核对结果（基差收敛、费差、止盈触发后才去拉）。
    pub exit: ExitCheck,
    /// 最近几小时费差均值的年化；没拉到历史是 `None`，费差规则退回按当前读数。
    pub funding_avg_apr: Option<Decimal>,
    /// 开仓以来这笔仓位已收付的资金费合计（交易所结算流水，正 = 收到）。`None` = 没查到，
    /// 止盈不评估 —— 不拿 0 冒充。
    pub funding_usdt: Option<Decimal>,
    /// 两条腿在交易所的实际保证金状态（补过保证金后台账里的开仓时保证金早不对了）。
    pub long_state: Option<&'a crate::broker::VenueLegState>,
    pub short_state: Option<&'a crate::broker::VenueLegState>,
}

fn closing(observation: Observation, skipped: Vec<String>, reason: String) -> Option<Evaluation> {
    Some(Evaluation {
        action: Action::Close { reason },
        observation,
        fallback: None,
        skipped,
        take_profit_hold: None,
    })
}

/// 评估一笔持仓。输入见 [`Inputs`]。
pub fn evaluate_full(
    position: &PairPosition,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
    inputs: Inputs<'_>,
) -> Option<Evaluation> {
    let Inputs {
        exit,
        funding_avg_apr,
        funding_usdt,
        long_state,
        short_state,
    } = inputs;
    if position.status != PositionStatus::Open {
        return None;
    }
    let (long_leg, short_leg) = (position.long.as_ref()?, position.short.as_ref()?);
    let (long_status, short_status) = (
        leg_status(long_leg, long, long_state, position.margin_mode),
        leg_status(short_leg, short, short_state, position.margin_mode),
    );
    let pnl = pair_pnl(
        long_leg,
        short_leg,
        long,
        short,
        long_status.mark_price,
        short_status.mark_price,
    );
    let current_basis_pct = long_status
        .mark_price
        .zip(short_status.mark_price)
        .and_then(|(long, short)| mark_basis_pct(long, short));
    let mut observation = Observation {
        long: long_status,
        short: short_status,
        current_basis_pct,
        pnl,
        funding_avg_apr,
        funding_usdt: None,
        net_with_funding_usdt: None,
        exit: match &exit {
            ExitCheck::Quoted(quote) => Some(quote.clone()),
            _ => None,
        },
        exit_unavailable: match &exit {
            ExitCheck::Unavailable(why) => Some(why.clone()),
            _ => None,
        },
        funding_apr: to_apr(
            to_daily(short.period_rate, short.interval_h)
                - to_daily(long.period_rate, long.interval_h),
        ),
        size_mismatch_pct: mismatch_pct(long_leg, short_leg),
    };
    observation.set_funding(position, funding_usdt);
    let rules = &position.rules;
    let mut skipped = Vec::new();
    if [long_state, short_state]
        .into_iter()
        .flatten()
        .any(|state| {
            state
                .margin_mode
                .is_some_and(|mode| mode != position.margin_mode)
        })
    {
        skipped
            .push("交易所保证金模式与台账不符，自动规则暂停；请核对持仓，不会自动切换模式".into());
        return Some(Evaluation {
            observation,
            action: Action::Hold,
            fallback: None,
            skipped,
            take_profit_hold: None,
        });
    }

    if let Some(limit) = rules.size_mismatch_pct {
        match observation.size_mismatch_pct {
            Some(pct) if pct > limit => {
                let reason = format!(
                    "两腿数量偏差 {}% 超过 {limit}%，对冲已不成立",
                    pct.round_dp(3)
                );
                return closing(observation, skipped, reason);
            }
            Some(_) => {}
            None => skipped.push("数量失衡：入场价非正，算不出数量".into()),
        }
    }

    if let Some(target) = rules.basis_exit_pct {
        match observation.current_basis_pct {
            Some(basis) if basis <= target => {
                let triggered = format!(
                    "标记价基差已收敛到 {}%（目标 ≤ {target}%，入场 {}%）",
                    basis.round_dp(3),
                    position.entry_basis_pct.round_dp(3)
                );
                match &exit {
                    ExitCheck::Quoted(quote) if quote.net_usdt > Decimal::ZERO => {
                        let reason = format!(
                            "{triggered}；按盘口现在平仓价差 {}%，整笔预估净赚 {} USDT（{}%，含手续费），兑现价差",
                            quote.exit_basis_pct,
                            quote.net_usdt.round_dp(4),
                            quote.net_pct
                        );
                        return closing(observation, skipped, reason);
                    }
                    ExitCheck::Quoted(quote) => skipped.push(format!(
                        "{triggered}，但按盘口现在平仓价差 {}%，整笔预估 {} USDT（含手续费）不为正，继续持有",
                        quote.exit_basis_pct,
                        quote.net_usdt.round_dp(4)
                    )),
                    ExitCheck::Unavailable(why) => skipped.push(format!(
                        "{triggered}，但{why}，不按标记价平仓，继续持有"
                    )),
                    ExitCheck::NotFetched => skipped.push(format!(
                        "{triggered}；平不平要按两边盘口核对，由后台监控执行"
                    )),
                }
            }
            Some(_) => {}
            None => skipped.push("基差收敛：缺至少一条腿的标记价，评估不了".into()),
        }
    }

    let mut take_profit_hold = None;
    if let Some(target) = rules.take_profit_usdt {
        match (funding_usdt, observation.net_with_funding_usdt) {
            (None, _) => skipped.push(
                "止盈：这笔仓位的资金费流水没查到（场所没接入或查询失败），不拿 0 冒充，本轮不评估"
                    .into(),
            ),
            (Some(_), None) => skipped.push("止盈：缺标记价，评估不了".into()),
            (Some(funding), Some(estimate)) if estimate >= target => {
                let triggered = format!(
                    "含资金费的净盈利（按标记价）{} USDT 已达止盈 {target} USDT（其中资金费 {} USDT）",
                    estimate.round_dp(4),
                    funding.round_dp(4)
                );
                match &exit {
                    ExitCheck::Quoted(quote) => {
                        let net = quote.net_usdt + funding;
                        if net >= target {
                            let reason = format!(
                                "{triggered}；按盘口现在平仓预估净赚 {} USDT（含平仓手续费与穿价），兑现",
                                net.round_dp(4)
                            );
                            return closing(observation, skipped, reason);
                        }
                        let reason = format!(
                            "{triggered}，但按盘口现在平仓预估只剩 {} USDT（含平仓手续费与穿价），不到止盈，继续持有",
                            net.round_dp(4)
                        );
                        skipped.push(reason.clone());
                        take_profit_hold = Some(TakeProfitHold {
                            target_usdt: target,
                            mark_net_usdt: estimate,
                            book_net_usdt: Some(net.round_dp(8)),
                            funding_usdt: funding,
                            reason,
                        });
                    }
                    ExitCheck::Unavailable(why) => {
                        let reason = format!("{triggered}，但{why}，不按标记价平仓，继续持有");
                        skipped.push(reason.clone());
                        take_profit_hold = Some(TakeProfitHold {
                            target_usdt: target,
                            mark_net_usdt: estimate,
                            book_net_usdt: None,
                            funding_usdt: funding,
                            reason,
                        });
                    }
                    ExitCheck::NotFetched => skipped.push(format!(
                        "{triggered}；平不平要按两边盘口核对，由后台监控执行"
                    )),
                }
            }
            (Some(_), Some(_)) => {}
        }
    }

    if let Some(min) = rules.min_funding_apr {
        let measured = funding_measure(observation.funding_apr, observation.funding_avg_apr);
        if measured < min {
            let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(2);
            let basis = if observation.funding_avg_apr.is_some() {
                format!(
                    "最近 {FUNDING_AVG_HOURS} 小时平均费差年化 {}%",
                    pct(measured)
                )
            } else {
                format!("当前费差年化 {}%（没拉到历史，按当前读数）", pct(measured))
            };
            let triggered = format!("{basis} 低于 {}%", pct(min));
            // 费差已经为负：每小时都在白付，先平再说，平仓成本不是继续持有的理由。
            let bleeding = measured < Decimal::ZERO;
            match &exit {
                ExitCheck::NotFetched => {
                    return closing(observation, skipped, triggered);
                }
                _ if bleeding => {
                    let reason = format!("{triggered}，已经为负，继续持有只会白付资金费");
                    return closing(observation, skipped, reason);
                }
                ExitCheck::Unavailable(why) => skipped.push(format!(
                    "{triggered}，但{why}，算不出平仓要多付多少，费差还没转负，先继续持有"
                )),
                ExitCheck::Quoted(quote) => {
                    // 平仓成本超过「按门槛水平算一周的收益」：为一点点费差多付这么多不值，
                    // 费差还是正的就继续持有，转负了再平。
                    let cost = exit_cost_usdt(&observation, quote);
                    let notional = position
                        .long
                        .as_ref()
                        .map_or(Decimal::ZERO, |leg| leg.notional_usdt);
                    let week_at_floor = notional * min / Decimal::from(365) * Decimal::from(7);
                    if cost > week_at_floor {
                        skipped.push(format!(
                            "{triggered}，但现在平仓要多付约 {} USDT（手续费加盘口穿价），超过按门槛水平算一周的收益 {} USDT，费差还没转负，先继续持有",
                            cost.round_dp(4),
                            week_at_floor.round_dp(4)
                        ));
                    } else {
                        let reason = format!(
                            "{triggered}；现在平仓约多付 {} USDT，划得来",
                            cost.round_dp(4)
                        );
                        return closing(observation, skipped, reason);
                    }
                }
            }
        }
    }

    if position.margin_mode.is_cross() {
        if rules.auto_margin().is_some() {
            skipped.push("全仓不执行逐仓追加保证金".into());
        }
        if let Some(threshold) = rules.liq_protection_pct {
            for status in [&observation.long, &observation.short] {
                match status.distance_pct {
                    Some(distance) if distance < threshold => {
                        return Some(Evaluation {
                            observation: observation.clone(),
                            action: Action::Close {
                                reason: format!(
                                    "全仓爆仓保护：{} 交易所强平距离 {}% 低于 {}%，整笔退出；不使用逐仓减仓比例",
                                    status.venue,
                                    distance.round_dp(2),
                                    threshold
                                ),
                            },
                            fallback: None,
                            skipped,
                            take_profit_hold,
                        });
                    }
                    None => skipped.push(format!(
                        "全仓爆仓保护：{} 未返回有效强平价，无法评估账户风险；不按逐仓公式估算",
                        status.venue
                    )),
                    _ => {}
                }
            }
        }
        return Some(Evaluation {
            observation,
            action: Action::Hold,
            fallback: None,
            skipped,
            take_profit_hold,
        });
    }
    let mut trim_action = Action::Hold;
    if let Some(threshold) = rules.liq_protection_pct {
        let target = threshold * PROTECTION_TARGET_MULTIPLE;
        let mut fraction = Decimal::ZERO;
        let mut breached = Vec::new();
        for (status, snapshot) in [(&observation.long, long), (&observation.short, short)] {
            match (status.distance_pct, snapshot.maintenance_margin) {
                (Some(distance), Some(mmr)) if distance < threshold => {
                    if let Some(needed) = trim_fraction(distance, target, mmr, status.side) {
                        fraction = fraction.max(needed);
                        breached.push(format!("{} {}%", status.venue, distance.round_dp(2)));
                    }
                }
                (Some(_), Some(_)) => {}
                _ => skipped.push(format!(
                    "爆仓保护：{} 缺保证金或维持保证金率，算不出强平距离",
                    status.venue
                )),
            }
        }
        if fraction >= Decimal::ONE {
            trim_action = Action::Close {
                reason: format!(
                    "强平距离 {} 低于 {threshold}%，减仓也拉不回来",
                    breached.join("、")
                ),
            };
        } else if fraction > Decimal::ZERO {
            trim_action = Action::Trim {
                fraction,
                reason: format!(
                    "强平距离 {} 低于 {threshold}%，拉回 {}%",
                    breached.join("、"),
                    target.round_dp(2)
                ),
            };
        }
    }
    let margin_action = rules.auto_margin().and_then(|(trigger, cap)| {
        auto_margin_action(
            position,
            &observation,
            [(long_leg, long), (short_leg, short)],
            (trigger, cap),
            &mut skipped,
        )
    });
    let (action, fallback) = match margin_action {
        Some(add) => (add, (trim_action != Action::Hold).then_some(trim_action)),
        None => (trim_action, None),
    };
    Some(Evaluation {
        observation,
        action,
        fallback,
        skipped,
        take_profit_hold,
    })
}

/// 一次补保证金的最小金额（USDT）：再小的补法每一轮都要打一次交易所，不值。
pub const MIN_TOP_UP_USDT: Decimal = Decimal::ONE;

/// 自动加保证金：挑强平距离最近的那条腿，算出把距离拉回触发线 1.5 倍要补多少，按剩余上限夹住。
///
/// 一轮最多补一条腿 —— 补完要等交易所的保证金读数更新，下一轮再按真实数字重算。
/// 算不出的、上限用完的都写进 `skipped`，不静默放过。
fn auto_margin_action(
    position: &PairPosition,
    observation: &Observation,
    legs: [(&LegFill, &MarketSnapshot); 2],
    (trigger, cap): (Decimal, Decimal),
    skipped: &mut Vec<String>,
) -> Option<Action> {
    use rust_decimal::RoundingStrategy::{AwayFromZero, ToZero};
    let target = trigger * PROTECTION_TARGET_MULTIPLE;
    let mut worst: Option<(Decimal, Venue, Side, Decimal)> = None;
    for ((leg, snapshot), status) in legs
        .into_iter()
        .zip([&observation.long, &observation.short])
    {
        let mut note = |why: &str| {
            let text = format!("自动加保证金：{} {why}", status.venue);
            if !skipped.contains(&text) {
                skipped.push(text);
            }
        };
        let Some(mmr) = snapshot.maintenance_margin else {
            note("缺维持保证金率，算不出强平距离");
            continue;
        };
        let (Some(distance), Some(margin), Some(mark)) =
            (status.distance_pct, status.margin_usdt, status.mark_price)
        else {
            note("缺保证金或标记价，算不出强平距离");
            continue;
        };
        if distance >= trigger {
            continue;
        }
        let Some(needed) = margin_for_distance(
            leg.average_price,
            leg.notional_usdt,
            mark,
            mmr,
            leg.side,
            target,
        ) else {
            note("算不出把强平距离拉到目标要补多少");
            continue;
        };
        let add = (needed - margin).max(Decimal::ZERO);
        if add <= Decimal::ZERO {
            // 交易所报告的强平价比按公式算的更近：公式说够了、交易所说不够。听交易所的，
            // 但没有可靠的补法，只说明、不乱补。
            note("强平距离低于触发线，但按公式保证金已足够，补多少算不出来");
            continue;
        }
        if worst.is_none_or(|(nearest, ..)| distance < nearest) {
            worst = Some((distance, status.venue, leg.side, add));
        }
    }
    let (distance, venue, side, add) = worst?;
    let remaining = (cap - position.margin_added_usdt).round_dp_with_strategy(2, ToZero);
    if remaining < Decimal::new(1, 2) {
        let text = format!(
            "自动加保证金：{venue} 强平距离 {}% 低于 {trigger}%，但这笔仓位已补满上限 {cap} USDT（已补 {} USDT）",
            distance.round_dp(2),
            position.margin_added_usdt.round_dp(2)
        );
        if !skipped.contains(&text) {
            skipped.push(text);
        }
        return None;
    }
    let amount = add
        .round_dp_with_strategy(2, AwayFromZero)
        .max(MIN_TOP_UP_USDT)
        .min(remaining);
    Some(Action::AddMargin {
        venue,
        side,
        amount_usdt: amount,
        reason: format!(
            "{venue} 强平距离 {}% 低于 {trigger}%，补 {amount} USDT 把距离拉回 {}%（累计已补 {} / 上限 {cap}）",
            distance.round_dp(2),
            target.round_dp(2),
            position.margin_added_usdt.round_dp(2)
        ),
    })
}

/// 开仓当时两腿里更近的强平距离（%）。任一腿缺维持保证金率就是 `None`。
pub fn opening_distance_pct(
    leverage: Decimal,
    long: &MarketSnapshot,
    short: &MarketSnapshot,
) -> Option<Decimal> {
    let leg = |snapshot: &MarketSnapshot, side| {
        let effective = snapshot
            .max_leverage
            .filter(|max| *max > Decimal::ZERO)
            .map_or(leverage, |max| leverage.min(max));
        liquidation_distance_pct(effective, snapshot.maintenance_margin?, side)
    };
    Some(leg(long, Side::Buy)?.min(leg(short, Side::Sell)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClientOrderId, Strategy};
    use arb_core::Symbol;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn snapshot(venue: Venue, period_rate: Decimal, mark: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp("BTC", "USDT"),
            period_rate,
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: true,
            taker_fee: None,
            mark_price: Some(mark),
            index_price: Some(mark),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: Some(dec!(50)),
            maintenance_margin: Some(dec!(0.01)),
            oi_capped: false,
        }
    }

    fn leg(venue: Venue, side: Side, notional: Decimal, leverage: Decimal) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: notional,
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("p1", side, 0),
            margin_usdt: Some(notional / leverage),
        }
    }

    fn position(rules: TaskRules, long_notional: Decimal) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "p1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Lighter, Side::Buy, long_notional, dec!(5))),
            short: Some(leg(Venue::Hyperliquid, Side::Sell, dec!(1000), dec!(5))),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules,
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        }
    }

    /// 空腿每小时 0.00002、多腿每小时 0.00001 → 毛费差年化 8.76%
    fn market(short_mark: Decimal) -> (MarketSnapshot, MarketSnapshot) {
        (
            snapshot(Venue::Lighter, dec!(0.00001), dec!(100)),
            snapshot(Venue::Hyperliquid, dec!(0.00002), short_mark),
        )
    }

    #[test]
    fn pnl_is_per_leg_price_moves_minus_fees_with_a_daily_funding_estimate() {
        // 多腿 1000 名义、空腿 1000 名义，入场价都是 100（各 10 个）。
        let mut position = position(TaskRules::default(), dec!(1000));
        position.long.as_mut().unwrap().fee_usdt = dec!(0.5);
        position.short.as_mut().unwrap().fee_usdt = dec!(0.45);
        // 多腿标记价 100 → 102（+20），空腿标记价 100 → 101（空头 −10）。
        let long = snapshot(Venue::Lighter, dec!(0.00001), dec!(102));
        let short = snapshot(Venue::Hyperliquid, dec!(0.00002), dec!(101));
        let pnl = evaluate(&position, &long, &short).unwrap().observation.pnl;
        assert_eq!(pnl.long.price_pnl_usdt, Some(dec!(20)));
        assert_eq!(pnl.long.price_pnl_pct, Some(dec!(2)));
        assert_eq!(pnl.short.price_pnl_usdt, Some(dec!(-10)));
        assert_eq!(pnl.price_pnl_usdt, Some(dec!(10)));
        assert_eq!(pnl.fees_usdt, dec!(0.95));
        assert_eq!(pnl.net_usdt, Some(dec!(9.05)));
        // 每日资金费：空腿 0.00002 × 24 × 1010 − 多腿 0.00001 × 24 × 1020 = 0.4848 − 0.2448
        assert_eq!(pnl.funding_daily_usdt, Some(dec!(0.24)));
    }

    #[test]
    fn pnl_without_a_mark_price_is_unknown_not_zero() {
        let position = position(TaskRules::default(), dec!(1000));
        let mut long = snapshot(Venue::Lighter, dec!(0.00001), dec!(100));
        long.mark_price = None;
        long.index_price = None;
        let short = snapshot(Venue::Hyperliquid, dec!(0.00002), dec!(100));
        let pnl = evaluate(&position, &long, &short).unwrap().observation.pnl;
        assert_eq!(pnl.long.price_pnl_usdt, None);
        assert_eq!(pnl.price_pnl_usdt, None);
        assert_eq!(pnl.net_usdt, None);
        assert_eq!(pnl.funding_daily_usdt, None);
        assert_eq!(pnl.short.price_pnl_usdt, Some(Decimal::ZERO));
    }

    fn quote(net: Decimal) -> ExitQuote {
        ExitQuote {
            long_price: dec!(100),
            short_price: dec!(100.05),
            exit_basis_pct: dec!(0.05),
            exit_fee_usdt: Decimal::ZERO,
            net_usdt: net,
            net_pct: net / dec!(10),
        }
    }

    #[test]
    fn a_converged_mark_basis_only_closes_when_the_books_say_it_pays() {
        let rules = TaskRules {
            basis_exit_pct: Some(dec!(0.1)),
            ..TaskRules::default()
        };
        let mut position = position(rules, dec!(1000));
        position.strategy = Strategy::Spread;
        position.entry_basis_pct = dec!(1.0);
        // 空腿 100.05 vs 多腿 100：标记价基差 ≈ 0.05%，已收敛到目标以内 —— 这只是触发。
        let (long, short) = market(dec!(100.05));
        assert!(basis_exit_triggered(&position, &long, &short));
        let shown = evaluate(&position, &long, &short).unwrap();
        assert_eq!(shown.observation.current_basis_pct, Some(dec!(0.05)));
        assert_eq!(shown.action, Action::Hold, "没核对盘口不平");
        assert!(
            shown
                .skipped
                .iter()
                .any(|why| why.contains("按两边盘口核对"))
        );
        // 按盘口整笔为正：平。
        let paid = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Quoted(quote(dec!(2))),
            None,
        )
        .unwrap();
        match paid.action {
            Action::Close { reason } => assert!(reason.contains("预估净赚"), "{reason}"),
            other => panic!("应当平仓，实际 {other:?}"),
        }
        assert!(paid.observation.exit.is_some());
        // 按盘口整笔不为正：继续持有并说明。
        let losing = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Quoted(quote(dec!(-1))),
            None,
        )
        .unwrap();
        assert_eq!(losing.action, Action::Hold);
        assert!(losing.skipped.iter().any(|why| why.contains("不为正")));
        // 盘口没拉到：不退回按标记价平仓。
        let blind = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Unavailable("lighter 盘口没拉到".into()),
            None,
        )
        .unwrap();
        assert_eq!(blind.action, Action::Hold);
        assert!(
            blind
                .skipped
                .iter()
                .any(|why| why.contains("不按标记价平仓"))
        );
        // 空腿 101：基差 ≈ 1%，还没收敛，不去拉盘口。
        let (long, short) = market(dec!(101));
        assert!(!basis_exit_triggered(&position, &long, &short));
        assert_eq!(
            evaluate(&position, &long, &short).unwrap().action,
            Action::Hold
        );
    }

    /// 2026-09-30 实盘 NEAR：多 lighter-rh（203.33 @ 4.9204，免手续费）、空 arcus（203.3347 @
    /// 4.926，手续费 0.225）。标记价基差收敛到 0.0906%，但盘口上 lighter-rh 买一 4.9613、
    /// arcus 卖一 4.971 —— 现在平会亏约 1.29 USDT，必须继续持有。
    #[test]
    fn the_near_trade_would_have_been_held() {
        let book = |venue, bid: Decimal, ask: Decimal| arb_core::OrderBook {
            venue,
            symbol: Symbol::perp("NEAR", "USDT"),
            bids: vec![arb_core::Level {
                price: bid,
                notional_usdt: dec!(50000),
            }],
            asks: vec![arb_core::Level {
                price: ask,
                notional_usdt: dec!(50000),
            }],
        };
        let mut position = position(
            TaskRules {
                basis_exit_pct: Some(dec!(0.1)),
                ..TaskRules::default()
            },
            dec!(1000),
        );
        position.symbol = Symbol::perp("NEAR", "USDT");
        position.strategy = Strategy::Spread;
        position.entry_basis_pct = dec!(0.1442);
        position.long = Some(LegFill {
            venue: Venue::LighterRh,
            notional_usdt: dec!(1000.464932),
            average_price: dec!(4.9204),
            fee_usdt: Decimal::ZERO,
            ..leg(Venue::LighterRh, Side::Buy, dec!(1000), dec!(3))
        });
        position.short = Some(LegFill {
            venue: Venue::Arcus,
            notional_usdt: dec!(1001.626673088),
            average_price: dec!(4.926),
            fee_usdt: dec!(0.225375889),
            ..leg(Venue::Arcus, Side::Sell, dec!(1000), dec!(3))
        });
        let quote = exit_quote(
            &position,
            &book(Venue::LighterRh, dec!(4.9612594059), dec!(4.9650)),
            &book(Venue::Arcus, dec!(4.9660), dec!(4.971)),
        )
        .unwrap();
        assert!(
            (quote.net_usdt - dec!(-1.2949)).abs() < dec!(0.001),
            "{}",
            quote.net_usdt
        );
        assert!(
            quote.exit_basis_pct > dec!(0.19),
            "{}",
            quote.exit_basis_pct
        );
        let mut long = snapshot(Venue::LighterRh, dec!(0.00001), dec!(4.9640));
        long.symbol = position.symbol.clone();
        let mut short = snapshot(Venue::Arcus, dec!(0.00001), dec!(4.9685));
        short.symbol = position.symbol.clone();
        assert!(basis_exit_triggered(&position, &long, &short));
        let evaluation =
            evaluate_with_exit(&position, &long, &short, ExitCheck::Quoted(quote), None).unwrap();
        assert_eq!(evaluation.action, Action::Hold);
    }

    #[test]
    fn an_exit_quote_needs_enough_depth_for_the_whole_quantity() {
        let position = position(TaskRules::default(), dec!(1000));
        let thin = arb_core::OrderBook {
            venue: Venue::Lighter,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![arb_core::Level {
                price: dec!(100),
                notional_usdt: dec!(500),
            }],
            asks: vec![],
        };
        let deep = arb_core::OrderBook {
            venue: Venue::Hyperliquid,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![],
            asks: vec![arb_core::Level {
                price: dec!(100),
                notional_usdt: dec!(50000),
            }],
        };
        let error = exit_quote(&position, &thin, &deep).unwrap_err();
        assert!(error.contains("买盘不够"), "{error}");
    }

    /// 2026-09-30 LIT：在交易所补了保证金，台账里记的还是开仓时的。评估要按交易所实际的算。
    #[test]
    fn the_liquidation_price_follows_the_margin_actually_on_the_venue() {
        let position = position(TaskRules::default(), dec!(1000));
        let (long, short) = market(dec!(100));
        let ledger = evaluate(&position, &long, &short).unwrap().observation;
        assert!(!ledger.short.margin_from_venue && !ledger.short.liquidation_from_venue);
        // 空腿补了一倍保证金：强平价往上移（更远）、距离变大，并标明是交易所的保证金。
        let topped = crate::broker::VenueLegState {
            margin_mode: None,
            margin_usdt: position
                .short
                .as_ref()
                .unwrap()
                .margin_usdt
                .map(|m| m * dec!(2)),
            liquidation_price: None,
        };
        let observed = evaluate_full(
            &position,
            &long,
            &short,
            Inputs {
                short_state: Some(&topped),
                ..Inputs::default()
            },
        )
        .unwrap()
        .observation;
        assert!(observed.short.margin_from_venue && !observed.short.liquidation_from_venue);
        assert_eq!(observed.short.margin_usdt, topped.margin_usdt);
        assert!(
            observed.short.liquidation_price > ledger.short.liquidation_price,
            "{:?} 应远于 {:?}",
            observed.short.liquidation_price,
            ledger.short.liquidation_price
        );
        assert!(observed.short.distance_pct > ledger.short.distance_pct);
        // 另一条腿没有交易所数据：仍按台账算，不受影响。
        assert_eq!(
            observed.long.liquidation_price,
            ledger.long.liquidation_price
        );
        assert!(!observed.long.margin_from_venue);
        // 交易所直接给了强平价：用它，不再自己算。
        let reported = crate::broker::VenueLegState {
            margin_mode: None,
            margin_usdt: None,
            liquidation_price: Some(dec!(160)),
        };
        let direct = evaluate_full(
            &position,
            &long,
            &short,
            Inputs {
                short_state: Some(&reported),
                ..Inputs::default()
            },
        )
        .unwrap()
        .observation;
        assert_eq!(direct.short.liquidation_price, Some(dec!(160)));
        assert!(direct.short.liquidation_from_venue);
        // 0 表示交易所没给（全仓），回落到台账：不能拿 0 当强平价。
        let zero = crate::broker::VenueLegState {
            margin_mode: None,
            margin_usdt: Some(Decimal::ZERO),
            liquidation_price: Some(Decimal::ZERO),
        };
        let fallback = evaluate_full(
            &position,
            &long,
            &short,
            Inputs {
                short_state: Some(&zero),
                ..Inputs::default()
            },
        )
        .unwrap()
        .observation;
        assert_eq!(
            fallback.short.liquidation_price,
            ledger.short.liquidation_price
        );
        assert!(!fallback.short.margin_from_venue);
    }

    fn quote_with_fee(fee: Decimal) -> ExitQuote {
        ExitQuote {
            exit_fee_usdt: fee,
            ..quote(Decimal::ZERO)
        }
    }

    fn funding_position() -> PairPosition {
        position(
            TaskRules {
                min_funding_apr: Some(dec!(0.10)),
                ..TaskRules::default()
            },
            dec!(1000),
        )
    }

    #[test]
    fn the_funding_rule_uses_the_recent_average_not_a_single_reading() {
        // 当前读数年化 8.76% 低于门槛 10%；但最近 6 小时平均 20%：不触发，也不必去拉盘口。
        let position = funding_position();
        let (long, short) = market(dec!(100));
        assert!(funding_exit_triggered(&position, &long, &short, None));
        assert!(!funding_exit_triggered(
            &position,
            &long,
            &short,
            Some(dec!(0.20))
        ));
        let held = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::NotFetched,
            Some(dec!(0.20)),
        )
        .unwrap();
        assert_eq!(held.action, Action::Hold);
        assert_eq!(held.observation.funding_avg_apr, Some(dec!(0.20)));
        // 平均也低于门槛：触发。
        let closed = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::NotFetched,
            Some(dec!(0.05)),
        )
        .unwrap();
        match closed.action {
            Action::Close { reason } => assert!(reason.contains("最近 6 小时平均"), "{reason}"),
            other => panic!("应当平仓，实际 {other:?}"),
        }
    }

    #[test]
    fn a_reversed_funding_spread_closes_regardless_of_exit_cost() {
        let position = funding_position();
        let (long, short) = market(dec!(100));
        let evaluation = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Quoted(quote_with_fee(dec!(50))),
            Some(dec!(-0.05)),
        )
        .unwrap();
        match evaluation.action {
            Action::Close { reason } => assert!(reason.contains("已经为负"), "{reason}"),
            other => panic!("应当平仓，实际 {other:?}"),
        }
        // 盘口没拉到也一样：费差为负，白付的比平仓成本更确定。
        let blind = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Unavailable("盘口没拉到".into()),
            Some(dec!(-0.05)),
        )
        .unwrap();
        assert!(matches!(blind.action, Action::Close { .. }));
    }

    #[test]
    fn a_low_but_positive_spread_is_only_closed_when_exiting_is_cheap() {
        let position = funding_position();
        let (long, short) = market(dec!(100));
        // 1000 名义按 10% 门槛一周的收益约 1.92 USDT。平仓要多付 5：不划算，继续持有。
        let costly = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Quoted(quote_with_fee(dec!(5))),
            Some(dec!(0.05)),
        )
        .unwrap();
        assert_eq!(costly.action, Action::Hold);
        assert!(
            costly.skipped.iter().any(|why| why.contains("先继续持有")),
            "{:?}",
            costly.skipped
        );
        // 多付 0.5：划得来。
        let cheap = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Quoted(quote_with_fee(dec!(0.5))),
            Some(dec!(0.05)),
        )
        .unwrap();
        match cheap.action {
            Action::Close { reason } => assert!(reason.contains("划得来"), "{reason}"),
            other => panic!("应当平仓，实际 {other:?}"),
        }
        // 盘口拉不到、费差还是正的：不盲平。
        let blind = evaluate_with_exit(
            &position,
            &long,
            &short,
            ExitCheck::Unavailable("盘口没拉到".into()),
            Some(dec!(0.05)),
        )
        .unwrap();
        assert_eq!(blind.action, Action::Hold);
    }

    #[test]
    fn a_size_mismatch_outranks_basis_convergence() {
        let rules = TaskRules {
            size_mismatch_pct: Some(dec!(1)),
            basis_exit_pct: Some(dec!(0.1)),
            ..TaskRules::default()
        };
        let (long, short) = market(dec!(100.05));
        match evaluate(&position(rules, dec!(900)), &long, &short)
            .unwrap()
            .action
        {
            Action::Close { reason } => assert!(reason.contains("数量偏差"), "{reason}"),
            other => panic!("应当按数量失衡平仓，实际 {other:?}"),
        }
    }

    #[test]
    fn basis_exit_targets_must_be_sane_and_old_rules_still_parse() {
        let rules = |pct| TaskRules {
            basis_exit_pct: Some(pct),
            ..TaskRules::default()
        };
        assert!(validate_rules(&rules(dec!(0.1)), None).is_ok());
        assert!(validate_rules(&rules(dec!(6)), None).is_err());
        assert!(!rules(dec!(0.1)).is_empty());
        // 旧台账里的规则没有这个字段。
        let old: TaskRules = serde_json::from_str(r#"{"min_funding_apr":"0.05"}"#).unwrap();
        assert_eq!(old.basis_exit_pct, None);
    }

    #[test]
    fn a_quiet_position_holds_and_reports_what_it_saw() {
        let rules = TaskRules {
            min_funding_apr: Some(dec!(0.05)),
            liq_protection_pct: Some(dec!(10)),
            size_mismatch_pct: Some(dec!(1)),
            basis_exit_pct: None,
            ..TaskRules::default()
        };
        let (long, short) = market(dec!(100));
        let evaluation = evaluate(&position(rules, dec!(1000)), &long, &short).unwrap();
        assert_eq!(evaluation.action, Action::Hold);
        assert_eq!(evaluation.observation.funding_apr, dec!(0.0876));
        assert_eq!(
            evaluation.observation.size_mismatch_pct,
            Some(Decimal::ZERO)
        );
        // 5 倍、维持 1% 的空腿开仓距离 18.81%
        assert_eq!(
            evaluation
                .observation
                .short
                .distance_pct
                .unwrap()
                .round_dp(2),
            dec!(18.81)
        );
        assert!(evaluation.skipped.is_empty());
    }

    #[test]
    fn funding_below_the_floor_closes_the_whole_position() {
        let rules = TaskRules {
            min_funding_apr: Some(dec!(0.10)),
            ..TaskRules::default()
        };
        let (long, short) = market(dec!(100));
        let evaluation = evaluate(&position(rules, dec!(1000)), &long, &short).unwrap();
        assert!(
            matches!(evaluation.action, Action::Close { .. }),
            "{:?}",
            evaluation.action
        );
    }

    #[test]
    fn a_short_squeeze_trims_both_legs_back_to_one_and_a_half_times_the_threshold() {
        let rules = TaskRules {
            liq_protection_pct: Some(dec!(10)),
            ..TaskRules::default()
        };
        // 空腿强平价 ≈ 118.81；价格涨到 110 → 距离约 8%，低于 10%
        let (long, short) = market(dec!(110));
        let position = position(rules, dec!(1000));
        let evaluation = evaluate(&position, &long, &short).unwrap();
        let Action::Trim { fraction, .. } = evaluation.action else {
            panic!("应当减仓：{:?}", evaluation.action);
        };
        assert!(fraction > Decimal::ZERO && fraction < Decimal::ONE);

        // 按比例减掉名义、减掉部分的已实现亏损计入保证金，重算的距离应当回到 15%
        let short_leg = position.short.as_ref().unwrap();
        let trimmed = short_leg.notional_usdt * (Decimal::ONE - fraction);
        let realized =
            short_leg.quantity().unwrap() * fraction * (short_leg.average_price - dec!(110));
        let liq = liquidation_price(
            short_leg.average_price,
            trimmed,
            short_leg.margin_usdt.unwrap() + realized,
            dec!(0.01),
            Side::Sell,
        )
        .unwrap();
        let after = distance_to_liquidation_pct(dec!(110), liq, Side::Sell).unwrap();
        assert_eq!(after.round_dp(6), dec!(15));
    }

    #[test]
    fn a_size_mismatch_outranks_every_other_rule() {
        let rules = TaskRules {
            min_funding_apr: Some(dec!(0.10)),
            liq_protection_pct: Some(dec!(10)),
            size_mismatch_pct: Some(dec!(1)),
            basis_exit_pct: None,
            ..TaskRules::default()
        };
        let (long, short) = market(dec!(110));
        let evaluation = evaluate(&position(rules, dec!(900)), &long, &short).unwrap();
        match evaluation.action {
            Action::Close { reason } => assert!(reason.contains("数量偏差"), "{reason}"),
            other => panic!("失衡必须先于费差与减仓：{other:?}"),
        }
    }

    #[test]
    fn missing_margin_data_is_reported_instead_of_silently_passing() {
        let rules = TaskRules {
            liq_protection_pct: Some(dec!(10)),
            ..TaskRules::default()
        };
        let (long, mut short) = market(dec!(110));
        short.maintenance_margin = None;
        let evaluation = evaluate(&position(rules, dec!(1000)), &long, &short).unwrap();
        assert_eq!(evaluation.action, Action::Hold);
        assert_eq!(evaluation.skipped.len(), 1, "{:?}", evaluation.skipped);
    }

    #[test]
    fn only_open_hedged_positions_are_evaluated() {
        let (long, short) = market(dec!(100));
        let mut closing = position(TaskRules::default(), dec!(1000));
        closing.status = PositionStatus::Closing;
        assert!(evaluate(&closing, &long, &short).is_none());
        let mut naked = position(TaskRules::default(), dec!(1000));
        naked.short = None;
        assert!(evaluate(&naked, &long, &short).is_none());
    }

    #[test]
    fn rules_that_would_fire_at_open_are_rejected() {
        let (long, short) = market(dec!(100));
        let opening = opening_distance_pct(dec!(5), &long, &short);
        assert_eq!(opening.unwrap().round_dp(2), dec!(18.81));
        let too_high = TaskRules {
            liq_protection_pct: Some(dec!(20)),
            ..TaskRules::default()
        };
        assert!(validate_rules(&too_high, opening).is_err());
        assert!(
            validate_rules(&too_high, None).is_err(),
            "算不出距离时保护无从执行"
        );
        let fine = TaskRules {
            liq_protection_pct: Some(dec!(10)),
            size_mismatch_pct: Some(dec!(0.5)),
            ..TaskRules::default()
        };
        assert!(validate_rules(&fine, opening).is_ok());
        let tiny = TaskRules {
            size_mismatch_pct: Some(dec!(0.4)),
            ..TaskRules::default()
        };
        assert!(validate_rules(&tiny, opening).is_err());
    }

    // ───────── 止盈（含资金费）─────────

    /// 多腿 100→102（+20）、空腿 100→101（−10）：价格盈亏 +10，手续费 0.95，净 9.05。
    fn profitable_pair(rules: TaskRules) -> (PairPosition, MarketSnapshot, MarketSnapshot) {
        let mut position = position(rules, dec!(1000));
        position.long.as_mut().unwrap().fee_usdt = dec!(0.5);
        position.short.as_mut().unwrap().fee_usdt = dec!(0.45);
        let long = snapshot(Venue::Lighter, dec!(0.00001), dec!(102));
        let short = snapshot(Venue::Hyperliquid, dec!(0.00002), dec!(101));
        (position, long, short)
    }

    fn take_profit(target: Decimal) -> TaskRules {
        TaskRules {
            take_profit_usdt: Some(target),
            ..TaskRules::default()
        }
    }

    fn with_funding(funding: Option<Decimal>, exit: ExitCheck) -> Inputs<'static> {
        Inputs {
            exit,
            funding_usdt: funding,
            ..Inputs::default()
        }
    }

    /// 净盈利必须把资金费算进去：价格净 9.05 本身不到 11，加上收到的 2.00 资金费才到。
    #[test]
    fn take_profit_counts_funding_and_is_confirmed_against_the_book() {
        let (position, long, short) = profitable_pair(take_profit(dec!(11)));
        let funding = Some(dec!(2));
        assert!(take_profit_triggered(&position, &long, &short, funding));
        assert!(
            !take_profit_triggered(&position, &long, &short, Some(dec!(1))),
            "9.05 + 1 = 10.05，不到 11"
        );

        // 没核对盘口：只说明，不平。
        let shown = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(funding, ExitCheck::NotFetched),
        )
        .unwrap();
        assert_eq!(shown.action, Action::Hold);
        assert_eq!(shown.observation.net_with_funding_usdt, Some(dec!(11.05)));
        assert!(
            shown
                .skipped
                .iter()
                .any(|note| note.contains("止盈") && note.contains("盘口"))
        );

        // 盘口平掉后含资金费仍达标（10.5 + 2 = 12.5 ≥ 11）：平。
        let closed = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(funding, ExitCheck::Quoted(quote(dec!(10.5)))),
        )
        .unwrap();
        let Action::Close { reason } = closed.action else {
            panic!("应当止盈：{:?}", closed.action);
        };
        assert!(
            reason.contains("止盈") && reason.contains("12.5"),
            "{reason}"
        );

        // 标记价看着够、按盘口平掉（穿价与手续费）后不到 11（8 + 2 = 10）：不平。
        let slipped = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(funding, ExitCheck::Quoted(quote(dec!(8)))),
        )
        .unwrap();
        assert_eq!(slipped.action, Action::Hold);
        assert!(slipped.skipped.iter().any(|note| note.contains("不到止盈")));
        // 被盘口挡下要留下结构化记录（给日志与告警），数字与说明一致。
        let hold = slipped
            .take_profit_hold
            .expect("标记价达标但盘口没过，要记下来");
        assert_eq!(
            (
                hold.target_usdt,
                hold.mark_net_usdt,
                hold.book_net_usdt,
                hold.funding_usdt
            ),
            (dec!(11), dec!(11.05), Some(dec!(10)), dec!(2))
        );
        assert!(slipped.skipped.contains(&hold.reason));
        assert!(shown.take_profit_hold.is_none(), "没核对盘口不算被挡下");
        assert!(closed.take_profit_hold.is_none());

        // 盘口拿不到：绝不退回按标记价平仓。
        let blind = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(funding, ExitCheck::Unavailable("盘口深度不够".into())),
        )
        .unwrap();
        assert_eq!(blind.action, Action::Hold);
        assert!(
            blind
                .skipped
                .iter()
                .any(|note| note.contains("盘口深度不够"))
        );
        let hold = blind.take_profit_hold.expect("盘口用不了也要记下来");
        assert_eq!(hold.book_net_usdt, None);
        assert_eq!(
            blind.observation.exit_unavailable.as_deref(),
            Some("盘口深度不够")
        );

        // 标记价没到目标：即使盘口拿来展示，也不算「被挡下」。
        let below = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(Some(dec!(1)), ExitCheck::Quoted(quote(dec!(8)))),
        )
        .unwrap();
        assert_eq!(below.action, Action::Hold);
        assert!(below.take_profit_hold.is_none());
        assert!(below.observation.exit.is_some(), "盘口估算仍然留给页面");
    }

    /// 资金费流水查不到就不评估：不拿 0 冒充，也不触发。已经收到的资金费是真钱，付出去的也是 ——
    /// 不知道是哪一边时，不能凭价格盈亏就宣布「已经赚到了」。
    #[test]
    fn take_profit_is_not_evaluated_when_the_funding_ledger_is_unknown() {
        let (position, long, short) = profitable_pair(take_profit(dec!(5)));
        assert!(!take_profit_triggered(&position, &long, &short, None));
        let evaluation = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(None, ExitCheck::Quoted(quote(dec!(10)))),
        )
        .unwrap();
        assert_eq!(evaluation.action, Action::Hold);
        assert!(
            evaluation
                .skipped
                .iter()
                .any(|note| note.contains("资金费流水没查到"))
        );
        assert_eq!(evaluation.observation.net_with_funding_usdt, None);
    }

    /// 资金费为负（付出去的）同样要扣：价格净 9.05、付了 3 → 6.05，到不了 8。
    #[test]
    fn paid_funding_reduces_progress_towards_take_profit() {
        let (position, long, short) = profitable_pair(take_profit(dec!(8)));
        assert!(!take_profit_triggered(
            &position,
            &long,
            &short,
            Some(dec!(-3))
        ));
        assert!(take_profit_triggered(
            &position,
            &long,
            &short,
            Some(dec!(0))
        ));
    }

    /// 数量失衡先于止盈：对冲已经不成立时，盈利是不是真的算不清。
    #[test]
    fn a_size_mismatch_outranks_take_profit() {
        let (mut position, long, short) = profitable_pair(TaskRules {
            size_mismatch_pct: Some(dec!(1)),
            take_profit_usdt: Some(dec!(1)),
            ..TaskRules::default()
        });
        position.long.as_mut().unwrap().notional_usdt = dec!(1100);
        let evaluation = evaluate_full(
            &position,
            &long,
            &short,
            with_funding(Some(dec!(2)), ExitCheck::Quoted(quote(dec!(10)))),
        )
        .unwrap();
        let Action::Close { reason } = evaluation.action else {
            panic!("应当平仓");
        };
        assert!(reason.contains("数量偏差"), "{reason}");
    }

    // ───────── 自动加保证金 ─────────

    fn auto_margin(trigger: Decimal, cap: Decimal) -> TaskRules {
        TaskRules {
            auto_margin_pct: Some(trigger),
            auto_margin_max_usdt: Some(cap),
            ..TaskRules::default()
        }
    }

    /// 5 倍空腿、维持保证金 1%：开仓强平距离 18.81%。价格涨到 108，距离掉到约 10%。
    #[test]
    fn auto_margin_tops_up_exactly_enough_to_restore_the_target_distance() {
        let (long, short) = market(dec!(108));
        let position = position(auto_margin(dec!(12), dec!(500)), dec!(1000));
        let evaluation = evaluate(&position, &long, &short).unwrap();
        let Action::AddMargin {
            venue,
            side,
            amount_usdt,
            reason,
        } = evaluation.action
        else {
            panic!("距离约 10% 低于 12%，应当补保证金：{:?}", evaluation.action);
        };
        assert_eq!((venue, side), (Venue::Hyperliquid, Side::Sell));
        assert!(reason.contains("12") && reason.contains("18"), "{reason}");
        // 补完之后，按同一份公式重算：距离回到目标 18%（12% 的 1.5 倍），四舍五入到分的误差内。
        let topped = position.short.as_ref().unwrap().margin_usdt.unwrap() + amount_usdt;
        let liquidation =
            liquidation_price(dec!(100), dec!(1000), topped, dec!(0.01), Side::Sell).unwrap();
        let distance = distance_to_liquidation_pct(dec!(108), liquidation, Side::Sell).unwrap();
        assert!(
            (distance - dec!(18)).abs() < dec!(0.02),
            "补完距离 {distance}"
        );
        assert!(
            evaluation.fallback.is_none(),
            "没设爆仓保护，没有可退回的动作"
        );
        assert_eq!(amount_usdt, amount_usdt.round_dp(2), "金额按分");
    }

    #[test]
    fn auto_margin_is_clamped_by_the_remaining_cap_and_stops_when_it_is_used_up() {
        let (long, short) = market(dec!(108));
        let mut position = position(auto_margin(dec!(12), dec!(100)), dec!(1000));
        // 缺口约 87，上限 100、已补 50：只剩 50。
        position.margin_added_usdt = dec!(50);
        let Action::AddMargin { amount_usdt, .. } =
            evaluate(&position, &long, &short).unwrap().action
        else {
            panic!("还有额度，应当补");
        };
        assert_eq!(amount_usdt, dec!(50));
        // 上限用完：不补，写明原因（给告警用），不静默放过。
        position.margin_added_usdt = dec!(100);
        let used_up = evaluate(&position, &long, &short).unwrap();
        assert_eq!(used_up.action, Action::Hold);
        assert!(
            used_up
                .skipped
                .iter()
                .any(|note| note.starts_with("自动加保证金：") && note.contains("上限")),
            "{:?}",
            used_up.skipped
        );
    }

    /// 补不了时要有路可退：爆仓保护的减仓作为 fallback 一并给出；距离还没跌到爆仓保护的门槛时没有。
    #[test]
    fn the_liquidation_protection_trim_rides_along_as_the_fallback() {
        let rules = TaskRules {
            liq_protection_pct: Some(dec!(8)),
            ..auto_margin(dec!(12), dec!(500))
        };
        // 距离约 10%：低于补保证金线 12、还高于减仓线 8 —— 只补，不减。
        let (long, short) = market(dec!(108));
        let mild = evaluate(&position(rules.clone(), dec!(1000)), &long, &short).unwrap();
        assert!(matches!(mild.action, Action::AddMargin { .. }));
        assert!(mild.fallback.is_none());
        // 距离约 6%：两条线都破了。先补；补不了就退回减仓。
        let (long, short) = market(dec!(112));
        let deep = evaluate(&position(rules, dec!(1000)), &long, &short).unwrap();
        assert!(matches!(deep.action, Action::AddMargin { .. }));
        assert!(
            matches!(deep.fallback, Some(Action::Trim { .. })),
            "{:?}",
            deep.fallback
        );
    }

    #[test]
    fn auto_margin_leaves_a_healthy_pair_alone_and_floors_tiny_top_ups() {
        let (long, short) = market(dec!(100));
        let healthy = evaluate(
            &position(auto_margin(dec!(12), dec!(500)), dec!(1000)),
            &long,
            &short,
        )
        .unwrap();
        assert_eq!(healthy.action, Action::Hold);
        assert!(healthy.skipped.is_empty());
        // 小仓位刚跌破触发线：按公式缺口只有 0.70 USDT，也至少补 1 USDT
        // （再小的补法每轮都要打一次交易所）。名义 10、杠杆 5 → 保证金 2。
        let (long, short) = market(dec!(105.2));
        let mut small = position(auto_margin(dec!(13), dec!(500)), dec!(10));
        let leg = small.short.as_mut().unwrap();
        leg.notional_usdt = dec!(10);
        leg.margin_usdt = Some(dec!(2));
        let Action::AddMargin { amount_usdt, .. } = evaluate(&small, &long, &short).unwrap().action
        else {
            panic!("距离约 12.9% 低于 13%，应当补");
        };
        assert_eq!(amount_usdt, MIN_TOP_UP_USDT);
    }

    #[test]
    fn the_new_rules_are_validated_like_the_old_ones() {
        let opening = Some(dec!(18.81));
        let ok = TaskRules {
            take_profit_usdt: Some(dec!(1.5)),
            liq_protection_pct: Some(dec!(8)),
            ..auto_margin(dec!(12), dec!(200))
        };
        assert!(validate_rules(&ok, opening).is_ok());
        let bad = |rules: TaskRules, opening| validate_rules(&rules, opening).unwrap_err();
        assert!(bad(take_profit(dec!(0)), opening).contains("止盈"));
        assert!(bad(take_profit(dec!(-1)), opening).contains("止盈"));
        // 没有上限就不能开：不设上限的自动加保证金等于把账户一笔笔补进去。
        let no_cap = TaskRules {
            auto_margin_pct: Some(dec!(12)),
            ..TaskRules::default()
        };
        assert!(bad(no_cap, opening).contains("上限"));
        let no_trigger = TaskRules {
            auto_margin_max_usdt: Some(dec!(100)),
            ..TaskRules::default()
        };
        assert!(bad(no_trigger, opening).contains("强平距离"));
        assert!(bad(auto_margin(dec!(12), dec!(0)), opening).contains("必须为正"));
        // 一生效就触发 / 算不出距离：同爆仓保护。
        assert!(bad(auto_margin(dec!(20), dec!(100)), opening).contains("补保证金"));
        assert!(bad(auto_margin(dec!(12), dec!(100)), None).contains("无从执行"));
        // 减仓线不能高于补保证金线，否则减仓会抢在加保证金之前。
        let crossed = TaskRules {
            liq_protection_pct: Some(dec!(12)),
            ..auto_margin(dec!(10), dec!(100))
        };
        assert!(bad(crossed, opening).contains("抢在"));
        // 旧台账没有这些字段。
        let old: TaskRules = serde_json::from_str(r#"{"min_funding_apr":"0.05"}"#).unwrap();
        assert_eq!((old.take_profit_usdt, old.auto_margin()), (None, None));
    }
}
