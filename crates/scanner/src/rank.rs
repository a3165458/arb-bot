//! 机会排名：两条策略共用同一份双腿结构。
//!
//! 资金费套利与跨所价差套利是**同一笔交易**的两种期望来源：
//!
//! | | 赚什么 | 排序依据 | 主要风险 |
//! | --- | --- | --- | --- |
//! | 资金费套利 | 费差按结算周期累积 | [`Opportunity::funding_apr`] | 基差扩大 |
//! | 价差套利 | 基差收敛（一次性） | [`Opportunity::spread_net`] | 基差不收敛 |
//!
//! 两腿的组合、身份判定、成本模型完全相同，所以只枚举一次、算两套数字，
//! 而不是两条策略各扫一遍行情。
//!
//! # 成本模型（全仓唯一，面板与 CLI 必须一致）
//!
//! 一次往返只有三类现金流，别的一律不算：
//!
//! ```text
//! 资金费（按天累积）   +  daily_spread × N
//! 手续费（一次性）     −  round_trip_fee
//! 穿价（一次性）       −  round_trip_spread
//! 基差（一次性）       +  (b_entry − b_exit) × notional
//! ```
//!
//! ## 穿价成本
//!
//! 市价买要吃 `ask`、市价卖只能拿 `bid`。一次往返四条腿的净额是
//! `−(spread_short + spread_long)`，其中 `spread = (ask − bid) / mid`。
//! 这是**一次性**成本，和手续费一样摊到持有期上。
//!
//! 拿不到盘口时 `round_trip_spread` 是 `None`：不编一个 0 出来（那等于宣称
//! 「这笔交易没有价差成本」），而是把成本当**下界**、净收益当**上界**，
//! 并用 [`Opportunity::spread_unknown`] 标出来。
//!
//! ## 基差
//!
//! 基差项是 `b_entry − b_exit`，而 `b_exit` 在进场时**未知**。把它当 0（也就是把
//! `b_entry` 当成确定成本）是错的：基差保持不变时，这笔钱根本不会发生。
//!
//! 所以两条策略各自给一个明确的假设：
//!
//! - **资金费套利**：假设基差**保持不变**（`b_exit = b_entry`）→ 基差项为 0。
//!   基差扩大是无界风险，靠 [`basis_gate`] 挡住已经逆风的那一批。
//! - **价差套利**：赚的是**现在能成交**的价差，假设之后收敛到 0。
//!   做空吃买一、做多吃卖一，所以入场基差是 `(空腿买一 − 多腿卖一) / 中间价`，
//!   不是两家标记价的差。标记价差不能成交；缺一边盘口时这笔价差不存在，
//!   不能把穿价当成 0 再拿去年化。开仓穿价已经含在买一/卖一里，平仓还要再穿一次。
//!
//!   这个视角**不把持有期内的资金费算进净收益**（那部分见 `apr` 列的毛年化）。
//!   理由：两条榜必须真的不同。把 carry 算进去，价差榜的榜首会立刻被高资金费的
//!   配对占满，与资金费榜几乎重合 —— 那就没有第二条榜的意义了。资金费是附带的
//!   顺风或逆风，用户可以在毛年化那一列看到它。
//!
//! 单腿费率优先用交易所公开的真实吃单费率；拿不到才回落到配置值。

use std::collections::HashMap;

use arb_core::{Decimal, MarketSnapshot, Symbol, Venue};

use crate::normalize::{to_apr, to_daily};

/// 排名参数。
#[derive(Debug, Clone)]
pub struct RankConfig {
    /// 单边吃单费率的回落值。
    pub fee_per_side: Decimal,
    /// 资金费套利的计划持有天数：一次往返的成本摊到这些天上。
    pub amortize_days: Decimal,
    /// 价差套利的**回落**持有天数。某一对腿实测到半衰期时用实测值（见下一项）。
    pub spread_hold_days: Decimal,
    /// 按**这一对腿**实测出来的基差半衰期（天）。键是 `(合约, 做多场所, 做空场所)`。
    ///
    /// 有实测值时优先于 `spread_hold_days`。不能只按合约存：同一合约在不同场所
    /// 之间的基差不是同一个过程，把 A/B 的半衰期套到 C/D 上，年化会看起来精确
    /// 而实际是另一条腿的数字。`Opportunity::hold_measured` 会如实标记用了哪个。
    pub measured_hold: HashMap<(Symbol, Venue, Venue), Decimal>,
    /// 不利入场基差的上限（%，正数）。`None` = 不设门槛。
    pub max_entry_basis_pct: Option<Decimal>,
}

impl RankConfig {
    /// 这一对腿实际使用的价差持有期，以及它是不是实测值。
    pub fn hold_days_for(&self, symbol: &Symbol, long: Venue, short: Venue) -> (Decimal, bool) {
        match self.measured_hold.get(&(symbol.clone(), long, short)) {
            Some(measured) if *measured > Decimal::ZERO => (*measured, true),
            _ => (positive_or_one(self.spread_hold_days), false),
        }
    }
}

/// 一条 (做多, 做空) 配对，同时给出两条策略的口径。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Opportunity {
    pub symbol: Symbol,
    pub long: Venue,
    pub short: Venue,
    pub long_interval_h: u32,
    pub short_interval_h: u32,
    pub long_daily: Decimal,
    pub short_daily: Decimal,
    /// 日化资金费差（short − long）。正值 = 做空高费率腿、做多低费率腿能收到费差。
    pub daily_spread: Decimal,
    /// 未扣成本的日化费差年化。
    pub apr: Decimal,

    /// 一次往返的手续费率：两腿各自的开平共 4 笔。
    pub round_trip_fee: Decimal,
    /// 一次往返的穿价成本（两腿相对买卖价差之和）。`None` = 至少一腿拿不到盘口。
    pub round_trip_spread: Option<Decimal>,
    /// 实际用于计算的一次性成本 = 手续费 + 穿价（拿不到盘口时只算手续费）。
    pub round_trip_cost: Decimal,
    /// 穿价成本未知。
    ///
    /// 为真时 `round_trip_cost` 是**下界**，因此下面的净收益都是**上界** ——
    /// 界面必须把这个方向说清楚，不能让一个漏了成本的数字看起来像完整结论。
    pub spread_unknown: bool,

    /// 资金费视角的日化净收益（假设基差保持不变）。
    pub funding_daily: Decimal,
    pub funding_apr: Decimal,
    /// 价差视角的**一次性**净收益（小数，不是年化）。
    ///
    /// `可成交价差 − 平仓穿价 − 往返手续费`。收敛到 0 时，这一笔赚到的就是这个数，
    /// 不会再来第二笔同样大的价差，所以不能 ÷ 持有天数 × 365。
    /// 半衰期只说明衰减有多快，不改变这一笔的金额。没有盘口时为 0。
    /// **不含持有期内的资金费**（见 `apr`）。
    pub spread_net: Decimal,
    /// 价差衰减一半估计要多少天。没实测时是配置里的计划持有天数。
    pub spread_hold_days: Decimal,
    /// `spread_hold_days` 是不是两腿 K 线拟合出的半衰期。
    ///
    /// 它描述收敛速度，**不参与** [`Opportunity::spread_net`] 的计算。
    pub hold_measured: bool,

    /// 标记价基差（%）：(空腿标记价 − 多腿标记价) / 中间价 × 100。
    ///
    /// 资金费视角用它判断进场是不是已经逆风。它**不是**能锁住的价差：
    /// 标记价碰不到，两家盘口一宽，这个正数可以和真实成交方向相反。
    pub entry_basis_pct: Option<Decimal>,
    /// 可成交价差（%）：(空腿买一 − 多腿卖一) / 中间价 × 100。
    ///
    /// 做空卖在买一、做多买在卖一，这才是开仓当时锁住的价差。
    /// 任一条腿没有对应档位时为 `None`，价差榜不能把它当成 0。
    pub executable_basis_pct: Option<Decimal>,
    /// 两腿的计价资产不同（如 USDC 结算的场所配 USDT 结算的场所）。
    pub quote_mismatch: bool,
    /// 至少一条腿所在场所报告该合约已触及持仓量上限：只能减仓，这笔开不出来。
    ///
    /// 照常排名、不从榜上拿掉 —— 高费率的合约往往正是被挤到上限的那批，
    /// 藏起来会让人以为没有机会；标出来才知道「有，但现在进不去」。
    pub oi_capped: bool,
}

impl Opportunity {
    /// 资金费视角下摊费后是否为正。
    pub fn funding_profitable(&self) -> bool {
        self.funding_daily > Decimal::ZERO
    }

    /// 价差视角下是否为正。
    pub fn spread_profitable(&self) -> bool {
        self.spread_net > Decimal::ZERO
    }
}

/// 单腿实际使用的吃单费率。
///
/// `Some(0)` 是**已知为 0**（Lighter 的公开 API 实测就是 0），不能当成缺省去回落 ——
/// 那会凭空给这条腿加上一笔不存在的成本，把真实机会排下去。
pub fn venue_fee(rate: &MarketSnapshot, fee_per_side: Decimal) -> Decimal {
    rate.taker_fee.unwrap_or(fee_per_side)
}

/// 枚举同一个簇内的全部有序配对，并算好两条策略的数字。
///
/// `rates` 必须来自同一个 [`crate::identity::Cluster`]：跨簇配对等于拿两个不同
/// 资产的价格算价差。
pub fn rank(rates: &[&MarketSnapshot], config: &RankConfig) -> Vec<Opportunity> {
    // 持有期必须为正；0 会让成本除以 0。回落到 1 天而不是 panic ——
    // 一个坏配置不该带走整轮扫描。
    let funding_days = positive_or_one(config.amortize_days);

    let mut out = Vec::new();
    for short in rates {
        for long in rates {
            if short.venue == long.venue {
                continue;
            }
            let short_daily = to_daily(short.period_rate, short.interval_h);
            let long_daily = to_daily(long.period_rate, long.interval_h);
            let daily_spread = short_daily - long_daily;

            let basis_pct = entry_basis_pct(short, long);
            let executable_pct = executable_basis_pct(short, long);
            // 两条策略关心的方向不同：资金费看费差，价差看能不能成交的价差。
            // 标记价差为正但买一卖一为负时，价差方向不成立。
            let funding_side = daily_spread > Decimal::ZERO;
            let spread_side = executable_pct.is_some_and(|basis| basis > Decimal::ZERO);
            if !funding_side && !spread_side {
                continue;
            }

            let round_trip_fee = (venue_fee(short, config.fee_per_side)
                + venue_fee(long, config.fee_per_side))
                * Decimal::TWO;
            // 穿价：一次往返四条腿的净额是 −(spread_short + spread_long)。
            let round_trip_spread = crossing_cost(short, long);
            let round_trip_cost = round_trip_fee + round_trip_spread.unwrap_or(Decimal::ZERO);
            let spread_unknown = round_trip_spread.is_none();

            // 资金费视角：基差保持不变 → 基差项为 0。
            let funding_daily = daily_spread - round_trip_cost / funding_days;

            // 价差视角：可成交价差收敛到 0。开仓穿价已经在买一/卖一里，
            // 这里只再扣平仓那一次穿价和全程手续费。持有期内的资金费不算进来。
            let (spread_days, hold_measured) =
                config.hold_days_for(&short.symbol, long.venue, short.venue);
            // 一次性净额。持有天数只说明资本要绑多久，不能拿去把这一笔乘成一年。
            let spread_net = match executable_pct {
                Some(pct) if !spread_unknown => {
                    let basis = pct / Decimal::from(100u32);
                    let exit_cross = round_trip_spread.unwrap_or(Decimal::ZERO) / Decimal::TWO;
                    basis - exit_cross - round_trip_fee
                }
                _ => Decimal::ZERO,
            };

            out.push(Opportunity {
                symbol: short.symbol.clone(),
                long: long.venue,
                short: short.venue,
                long_interval_h: long.interval_h,
                short_interval_h: short.interval_h,
                long_daily,
                short_daily,
                daily_spread,
                apr: to_apr(daily_spread),
                round_trip_fee,
                round_trip_spread,
                round_trip_cost,
                spread_unknown,
                funding_daily,
                funding_apr: to_apr(funding_daily),
                spread_net,
                spread_hold_days: spread_days,
                hold_measured,
                entry_basis_pct: basis_pct,
                executable_basis_pct: executable_pct,
                quote_mismatch: short.symbol.quote != long.symbol.quote,
                oi_capped: short.oi_capped || long.oi_capped,
            });
        }
    }

    // 确定性排序：相等的值保持同一顺序，两次扫描之间不会无故跳动。
    out.sort_by(|a, b| {
        b.funding_apr
            .cmp(&a.funding_apr)
            .then_with(|| a.symbol.base.cmp(&b.symbol.base))
            .then_with(|| a.long.cmp(&b.long))
            .then_with(|| a.short.cmp(&b.short))
    });
    out
}

/// 入场基差门槛：`Ok(())` = 可以进场，`Err(原因)` = 被挡下。
///
/// 只用于**资金费视角**：那边的方向由费差决定，基差只是风险。价差视角的方向本来
/// 就是按基差挑的（`b > 0`），没有「逆风进场」这回事。
///
/// 只挡**不利**的一侧。正的基差（空腿更贵）是顺风，没有理由拦。
///
/// 缺价格时也挡下：没有基差就没法判断这一笔是不是一进场就让掉几个百分点，
/// 而「不知道」不该被当成「零成本」。被挡下的配对会在报告里逐条列出，不是静默丢弃。
pub fn basis_gate(op: &Opportunity, max_pct: Option<Decimal>) -> Result<(), String> {
    let Some(max) = max_pct else {
        return Ok(());
    };
    match op.entry_basis_pct {
        None => Err("缺少两腿价格，无法评估入场基差".to_string()),
        Some(basis) if basis < -max => Err(format!(
            "入场基差 {}% 低于门槛 −{}%",
            basis.round_dp(3),
            max
        )),
        Some(_) => Ok(()),
    }
}

/// 一次往返的穿价成本（两腿相对买卖价差之和）。任一条腿拿不到盘口就是 `None`。
fn crossing_cost(short: &MarketSnapshot, long: &MarketSnapshot) -> Option<Decimal> {
    Some(short.relative_spread()? + long.relative_spread()?)
}

/// 入场基差百分比：(空腿价 − 多腿价) / 中间价 × 100。
///
/// 用标记价（场内公允价），缺失时回落到指数价。两腿都拿不到价格时返回 `None` ——
/// **不能返回 0**，那会把「不知道」渲染成「基差为零，很理想」。
fn entry_basis_pct(short: &MarketSnapshot, long: &MarketSnapshot) -> Option<Decimal> {
    let short_price = short.mark_price.or(short.index_price)?;
    let long_price = long.mark_price.or(long.index_price)?;
    if short_price <= Decimal::ZERO || long_price <= Decimal::ZERO {
        return None;
    }
    let mid = (short_price + long_price) / Decimal::TWO;
    Some((short_price - long_price) / mid * Decimal::from(100u32))
}

/// 开仓能锁住的价差（%）。做空吃买一，做多吃卖一。
fn executable_basis_pct(short: &MarketSnapshot, long: &MarketSnapshot) -> Option<Decimal> {
    let bid = short.best_bid?;
    let ask = long.best_ask?;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO {
        return None;
    }
    let mid = (bid + ask) / Decimal::TWO;
    Some((bid - ask) / mid * Decimal::from(100u32))
}

fn positive_or_one(value: Decimal) -> Decimal {
    if value > Decimal::ZERO {
        value
    } else {
        Decimal::ONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn config(fee: Decimal, days: Decimal) -> RankConfig {
        RankConfig {
            fee_per_side: fee,
            amortize_days: days,
            spread_hold_days: days,
            measured_hold: HashMap::new(),
            // 门槛默认关闭：这些用例测的是排名本身，不是门槛
            max_entry_basis_pct: None,
        }
    }

    fn rate(
        venue: Venue,
        period_rate: Decimal,
        interval_h: u32,
        fee: Option<Decimal>,
    ) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp("BTC", "USDT"),
            period_rate,
            interval_h,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: false,
            taker_fee: fee,
            mark_price: Some(dec!(100)),
            index_price: Some(dec!(100)),
            // 默认零价差：既有用例的数字不受穿价成本影响
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

    /// 找一条特定方向的配对。
    fn find(ops: &[Opportunity], short: Venue, long: Venue) -> Opportunity {
        ops.iter()
            .find(|op| op.short == short && op.long == long)
            .unwrap_or_else(|| panic!("{short}->{long} 应该在结果里"))
            .clone()
    }

    #[test]
    fn an_oi_capped_leg_marks_the_pair_but_keeps_it_ranked() {
        let mut capped = rate(Venue::Hyperliquid, dec!(0.0002), 1, None);
        capped.oi_capped = true;
        let other = rate(Venue::Okx, dec!(0.0001), 8, Some(dec!(0.0005)));
        let ops = rank(&[&capped, &other], &config(dec!(0.0005), dec!(7)));
        let pair = find(&ops, Venue::Hyperliquid, Venue::Okx);
        assert!(pair.oi_capped, "触顶的腿不管做多做空都开不出来");
        assert!(pair.funding_apr > Decimal::ZERO, "照常排名，不藏起来");
    }

    #[test]
    fn a_one_day_amortization_needs_eight_times_the_spread_of_a_seven_day_one() {
        let high = rate(Venue::Binance, dec!(0.0002), 8, Some(dec!(0.0005)));
        let low = rate(Venue::Okx, dec!(0.0001), 8, Some(dec!(0.0005)));
        let rates = vec![&high, &low];

        let one = find(
            &rank(&rates, &config(dec!(0.0005), dec!(1))),
            Venue::Binance,
            Venue::Okx,
        );
        let seven = find(
            &rank(&rates, &config(dec!(0.0005), dec!(7))),
            Venue::Binance,
            Venue::Okx,
        );

        assert_eq!(one.daily_spread, dec!(0.0003));
        assert_eq!(one.round_trip_fee, dec!(0.002));
        assert_eq!(one.funding_daily, dec!(0.0003) - dec!(0.002));
        assert_eq!(seven.funding_daily, dec!(0.0003) - dec!(0.002) / dec!(7));
        assert!(seven.funding_daily > one.funding_daily);
    }

    #[test]
    fn settlement_intervals_are_normalized_before_pairing() {
        let hourly = rate(Venue::Hyperliquid, dec!(0.0001), 1, None);
        let eight_hourly = rate(Venue::Binance, dec!(0.0001), 8, None);
        let ops = rank(&[&hourly, &eight_hourly], &config(dec!(0.0005), dec!(7)));
        let op = find(&ops, Venue::Hyperliquid, Venue::Binance);

        assert_eq!(op.long_daily, dec!(0.0003));
        assert_eq!(op.short_daily, dec!(0.0024));
    }

    #[test]
    fn a_reported_zero_fee_is_used_as_zero_not_replaced_by_the_fallback() {
        let free = rate(Venue::Lighter, dec!(0.0002), 8, Some(Decimal::ZERO));
        let paid = rate(Venue::Binance, dec!(0.0001), 8, Some(dec!(0.0005)));
        let ops = rank(&[&free, &paid], &config(dec!(0.0005), dec!(7)));
        let op = find(&ops, Venue::Lighter, Venue::Binance);
        // 只有 Binance 那条腿付钱：(0.0005 + 0) × 2
        assert_eq!(op.round_trip_fee, dec!(0.001));
    }

    #[test]
    fn unknown_fee_falls_back_to_the_configured_value() {
        let unknown = rate(Venue::Binance, dec!(0.0002), 8, None);
        let other = rate(Venue::Okx, dec!(0.0001), 8, None);
        let ops = rank(&[&unknown, &other], &config(dec!(0.0007), dec!(7)));
        assert_eq!(
            find(&ops, Venue::Binance, Venue::Okx).round_trip_fee,
            dec!(0.0028)
        );
    }

    #[test]
    fn crossing_cost_is_the_sum_of_both_legs_spreads() {
        // 空腿 spread = (101 − 99)/100 = 2%；多腿 spread = (100.5 − 99.5)/100 = 1%
        let mut wide = rate(Venue::Binance, dec!(0.0002), 8, None);
        wide.best_bid = Some(dec!(99));
        wide.best_ask = Some(dec!(101));
        let mut narrow = rate(Venue::Okx, dec!(0.0001), 8, None);
        narrow.best_bid = Some(dec!(99.5));
        narrow.best_ask = Some(dec!(100.5));

        let ops = rank(&[&wide, &narrow], &config(dec!(0.0005), dec!(7)));
        let op = find(&ops, Venue::Binance, Venue::Okx);
        assert_eq!(op.round_trip_spread, Some(dec!(0.03)), "2% + 1%");
        assert_eq!(op.round_trip_cost, op.round_trip_fee + dec!(0.03));
        assert!(!op.spread_unknown);
    }

    #[test]
    fn a_missing_book_makes_the_cost_a_lower_bound_and_says_so() {
        let mut no_book = rate(Venue::Binance, dec!(0.0002), 8, None);
        no_book.best_bid = None;
        no_book.best_ask = None;
        let other = rate(Venue::Okx, dec!(0.0001), 8, None);

        let ops = rank(&[&no_book, &other], &config(dec!(0.0005), dec!(7)));
        let op = find(&ops, Venue::Binance, Venue::Okx);
        assert_eq!(op.round_trip_spread, None);
        assert!(
            op.spread_unknown,
            "必须标出来，不能让漏了成本的数字看起来完整"
        );
        // 只扣手续费 —— 这是下界
        assert_eq!(op.round_trip_cost, op.round_trip_fee);
    }

    #[test]
    fn a_crossed_book_is_treated_as_unknown_not_as_a_negative_cost() {
        let mut crossed = rate(Venue::Binance, dec!(0.0002), 8, None);
        crossed.best_bid = Some(dec!(101));
        crossed.best_ask = Some(dec!(99));
        let other = rate(Venue::Okx, dec!(0.0001), 8, None);

        let ops = rank(&[&crossed, &other], &config(dec!(0.0005), dec!(7)));
        assert!(find(&ops, Venue::Binance, Venue::Okx).spread_unknown);
    }

    #[test]
    fn the_funding_view_assumes_the_basis_persists() {
        // 空腿更便宜（负基差）→ 资金费视角不受基差影响（假设保持不变）
        let mut cheap_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        cheap_short.mark_price = Some(dec!(99));
        let mut expensive_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        expensive_long.mark_price = Some(dec!(100));

        let ops = rank(
            &[&cheap_short, &expensive_long],
            &config(dec!(0.0005), dec!(7)),
        );
        let op = find(&ops, Venue::Binance, Venue::Okx);
        assert!(op.entry_basis_pct.unwrap() < Decimal::ZERO);
        assert_eq!(
            op.funding_daily,
            op.daily_spread - op.round_trip_cost / dec!(7)
        );
    }

    #[test]
    fn the_spread_view_earns_the_basis_when_it_converges() {
        // 空腿更贵（正基差 1%）→ 价差视角赚这一笔。盘口贴在标记价上，可成交价差=标记价差。
        let mut expensive_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive_short.mark_price = Some(dec!(101));
        expensive_short.best_bid = Some(dec!(101));
        expensive_short.best_ask = Some(dec!(101));
        let mut cheap_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        cheap_long.mark_price = Some(dec!(100));
        cheap_long.best_bid = Some(dec!(100));
        cheap_long.best_ask = Some(dec!(100));

        let ops = rank(
            &[&expensive_short, &cheap_long],
            &config(dec!(0.0005), dec!(7)),
        );
        let op = find(&ops, Venue::Binance, Venue::Okx);
        let basis = op.executable_basis_pct.unwrap() / dec!(100);

        // 盘口贴在标记价上，开仓穿价为 0，只扣手续费。这是一次性净额，不乘 365。
        assert_eq!(op.spread_net, basis - op.round_trip_fee);
        assert!(op.spread_net > Decimal::ZERO);
        assert!(
            op.spread_net < dec!(0.02),
            "1% 量级的价差不能被折成年化：{}",
            op.spread_net
        );
    }

    #[test]
    fn the_spread_view_ignores_the_carry_so_the_two_views_stay_distinct() {
        // 基差很小（约 +0.02%）、费差很大（+2%/天）：资金费视角极好，价差视角很普通。
        // 价差视角若把 carry 算进去，这两条榜就会几乎重合 —— 那第二条榜就没意义了。
        let mut expensive_short = rate(Venue::Binance, dec!(0.005), 8, None);
        expensive_short.mark_price = Some(dec!(100.02));
        expensive_short.best_bid = Some(dec!(100.02));
        expensive_short.best_ask = Some(dec!(100.02));
        let mut cheap_long = rate(Venue::Okx, dec!(0.001), 8, None);
        cheap_long.mark_price = Some(dec!(100));
        cheap_long.best_bid = Some(dec!(100));
        cheap_long.best_ask = Some(dec!(100));

        let ops = rank(
            &[&expensive_short, &cheap_long],
            &config(dec!(0.0005), dec!(7)),
        );
        let op = find(&ops, Venue::Binance, Venue::Okx);
        assert!(
            op.funding_apr > dec!(1),
            "资金费视角很高：{}",
            op.funding_apr
        );
        // 0.02% 的基差抵不过 0.2% 的成本 → 价差视角为负
        assert!(op.spread_net < Decimal::ZERO, "价差视角：{}", op.spread_net);
        // 毛年化（carry）仍然如实展示，用户能看到顺风有多大
        assert!(op.apr > Decimal::ZERO);
    }

    #[test]
    fn a_spread_trade_below_its_cost_is_negative() {
        let mut expensive_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive_short.mark_price = Some(dec!(100.05));
        expensive_short.best_bid = Some(dec!(100.05));
        expensive_short.best_ask = Some(dec!(100.05));
        let mut cheap_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        cheap_long.mark_price = Some(dec!(100));
        cheap_long.best_bid = Some(dec!(100));
        cheap_long.best_ask = Some(dec!(100));

        let ops = rank(
            &[&expensive_short, &cheap_long],
            &config(dec!(0.0005), dec!(7)),
        );
        // 基差约 0.05%，成本 0.2% → 净额为负
        assert!(find(&ops, Venue::Binance, Venue::Okx).spread_net < Decimal::ZERO);
    }

    #[test]
    fn both_directions_are_enumerated_when_either_view_has_something() {
        let a = rate(Venue::Binance, dec!(0.0002), 8, None);
        let mut b = rate(Venue::Okx, dec!(0.0001), 8, None);
        b.mark_price = Some(dec!(99.5));
        let ops = rank(&[&a, &b], &config(dec!(0.0005), dec!(7)));

        // 费差方向：Binance 高 → short Binance
        assert!(ops.iter().any(|o| o.short == Venue::Binance));
        // 基差方向：Okx 更便宜 → long Okx（同一条）
        // 反方向费差为负、基差也为负 → 不该出现
        assert!(!ops.iter().any(|o| o.short == Venue::Okx));
    }

    #[test]
    fn a_pair_with_neither_funding_nor_basis_edge_is_dropped() {
        let mut a = rate(Venue::Binance, dec!(0.0002), 8, None);
        a.mark_price = Some(dec!(100));
        let mut b = rate(Venue::Okx, dec!(0.0001), 8, None);
        b.mark_price = Some(dec!(100));
        // b 的费率更低 → short b 的费差为负；两者同价 → 两个方向基差都是 0
        let ops = rank(&[&a, &b], &config(dec!(0.0005), dec!(7)));
        assert_eq!(ops.len(), 1, "只有 short Binance 那个方向有意义");
    }

    #[test]
    fn ranking_is_deterministic_across_runs() {
        let a = rate(Venue::Binance, dec!(0.0003), 8, None);
        let b = rate(Venue::Okx, dec!(0.0002), 8, None);
        let c = rate(Venue::Bybit, dec!(0.0001), 8, None);
        let rates = vec![&a, &b, &c];
        let key = |ops: Vec<Opportunity>| {
            ops.into_iter()
                .map(|o| format!("{}-{}", o.long, o.short))
                .collect::<Vec<_>>()
        };
        let first = key(rank(&rates, &config(dec!(0.0005), dec!(7))));
        assert!(first.len() > 2);
        for _ in 0..8 {
            assert_eq!(first, key(rank(&rates, &config(dec!(0.0005), dec!(7)))));
        }
    }

    #[test]
    fn entry_basis_is_positive_when_the_short_leg_is_more_expensive() {
        let mut expensive = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive.mark_price = Some(dec!(101));
        let mut cheap = rate(Venue::Okx, dec!(0.0001), 8, None);
        cheap.mark_price = Some(dec!(100));

        let ops = rank(&[&expensive, &cheap], &config(dec!(0.0005), dec!(7)));
        let basis = find(&ops, Venue::Binance, Venue::Okx)
            .entry_basis_pct
            .unwrap();
        // (101 − 100) / 100.5 × 100 ≈ 0.995
        assert!(basis > dec!(0.9) && basis < dec!(1.0), "{basis}");
    }

    #[test]
    fn a_positive_mark_gap_is_not_a_spread_when_the_book_is_the_other_way() {
        // 标记价说空腿贵 5%，但空腿买一已经低于多腿卖一：开仓当时就是亏的。
        let mut short = rate(Venue::Binance, dec!(0.0002), 8, None);
        short.mark_price = Some(dec!(105));
        short.best_bid = Some(dec!(100));
        short.best_ask = Some(dec!(100.1));
        let mut long = rate(Venue::Okx, dec!(0.0001), 8, None);
        long.mark_price = Some(dec!(100));
        long.best_bid = Some(dec!(100.2));
        long.best_ask = Some(dec!(100.3));

        let op = find(
            &rank(&[&short, &long], &config(dec!(0.0005), dec!(7))),
            Venue::Binance,
            Venue::Okx,
        );
        assert!(op.entry_basis_pct.unwrap() > dec!(4), "标记价差仍然是正的");
        assert!(op.executable_basis_pct.unwrap() < Decimal::ZERO);
        assert!(
            op.spread_net < Decimal::ZERO,
            "净价差必须跟着能成交的价格走，不能跟着标记价：{}",
            op.spread_net
        );
    }

    #[test]
    fn a_missing_book_does_not_collect_the_mark_basis() {
        let mut short = rate(Venue::Variational, dec!(0.0002), 8, None);
        short.mark_price = Some(dec!(100));
        short.best_bid = None;
        short.best_ask = None;
        let mut long = rate(Venue::Binance, dec!(0.0001), 8, None);
        long.mark_price = Some(dec!(95));
        long.best_bid = Some(dec!(95));
        long.best_ask = Some(dec!(95));

        let op = find(
            &rank(&[&short, &long], &config(dec!(0.0005), dec!(7))),
            Venue::Variational,
            Venue::Binance,
        );
        assert!(op.entry_basis_pct.unwrap() > dec!(4));
        assert!(op.executable_basis_pct.is_none());
        assert!(op.spread_unknown);
        assert_eq!(op.spread_net, Decimal::ZERO);
    }

    #[test]
    fn entry_basis_is_none_when_prices_are_missing() {
        let mut a = rate(Venue::Binance, dec!(0.0002), 8, None);
        a.mark_price = None;
        a.index_price = None;
        let b = rate(Venue::Okx, dec!(0.0001), 8, None);
        let ops = rank(&[&a, &b], &config(dec!(0.0005), dec!(7)));
        assert!(
            find(&ops, Venue::Binance, Venue::Okx)
                .entry_basis_pct
                .is_none()
        );
    }

    #[test]
    fn the_gate_only_blocks_the_adverse_side() {
        let mut cheap_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        cheap_short.mark_price = Some(dec!(99));
        let mut expensive_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        expensive_long.mark_price = Some(dec!(100));
        let ops = rank(
            &[&cheap_short, &expensive_long],
            &config(dec!(0.0005), dec!(7)),
        );
        let op = find(&ops, Venue::Binance, Venue::Okx);

        assert!(basis_gate(&op, None).is_ok(), "关闭门槛 → 放行");
        assert!(
            basis_gate(&op, Some(dec!(5))).is_ok(),
            "门槛比逆风宽 → 放行"
        );
        let error = basis_gate(&op, Some(dec!(0.5))).unwrap_err();
        assert!(error.contains("入场基差"), "{error}");

        // 顺风的一侧不受门槛影响
        let mut expensive_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive_short.mark_price = Some(dec!(101));
        let mut cheap_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        cheap_long.mark_price = Some(dec!(100));
        let favorable = rank(
            &[&expensive_short, &cheap_long],
            &config(dec!(0.0005), dec!(7)),
        );
        let op = find(&favorable, Venue::Binance, Venue::Okx);
        assert!(basis_gate(&op, Some(dec!(0.001))).is_ok());
    }

    #[test]
    fn the_gate_blocks_pairs_whose_basis_is_unknown() {
        // 「不知道基差」不等于「基差为零」：不能因为拿不到价格就默认没有成本
        let mut a = rate(Venue::Binance, dec!(0.0002), 8, None);
        a.mark_price = None;
        a.index_price = None;
        let b = rate(Venue::Okx, dec!(0.0001), 8, None);
        let ops = rank(&[&a, &b], &config(dec!(0.0005), dec!(7)));
        let op = find(&ops, Venue::Binance, Venue::Okx);

        let error = basis_gate(&op, Some(dec!(0.5))).unwrap_err();
        assert!(error.contains("无法评估"), "{error}");
        assert!(basis_gate(&op, None).is_ok(), "关闭门槛时仍然展示");
    }

    #[test]
    fn a_measured_half_life_overrides_the_configured_hold_period() {
        let mut expensive_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive_short.mark_price = Some(dec!(101));
        expensive_short.best_bid = Some(dec!(101));
        expensive_short.best_ask = Some(dec!(101));
        let mut cheap_long = rate(Venue::Okx, dec!(0.0001), 8, None);
        cheap_long.mark_price = Some(dec!(100));
        cheap_long.best_bid = Some(dec!(100));
        cheap_long.best_ask = Some(dec!(100));

        let mut cfg = config(dec!(0.0005), dec!(7));
        let configured = rank(&[&expensive_short, &cheap_long], &cfg);
        assert!(!configured[0].hold_measured, "没实测时必须如实标记");
        assert_eq!(configured[0].spread_hold_days, dec!(7));

        // 实测半衰期 0.5 天 → 一次性基差按 0.5 天摊 → 年化被放大 14 倍
        cfg.measured_hold.insert(
            (Symbol::perp("BTC", "USDT"), Venue::Okx, Venue::Binance),
            dec!(0.5),
        );
        let measured = rank(&[&expensive_short, &cheap_long], &cfg);
        assert!(measured[0].hold_measured);
        assert_eq!(measured[0].spread_hold_days, dec!(0.5));
        assert_eq!(
            measured[0].spread_net, configured[0].spread_net,
            "半衰期只说明收敛速度，不把这一笔价差乘成年化"
        );
    }

    #[test]
    fn a_measured_half_life_does_not_apply_to_a_different_pair() {
        let mut binance = rate(Venue::Binance, dec!(0.0002), 8, None);
        binance.mark_price = Some(dec!(101));
        binance.best_bid = Some(dec!(101));
        binance.best_ask = Some(dec!(101));
        let mut okx = rate(Venue::Okx, dec!(0.0001), 8, None);
        okx.mark_price = Some(dec!(100));
        okx.best_bid = Some(dec!(100));
        okx.best_ask = Some(dec!(100));
        let mut bybit = rate(Venue::Bybit, dec!(0.0001), 8, None);
        bybit.mark_price = Some(dec!(99));
        bybit.best_bid = Some(dec!(99));
        bybit.best_ask = Some(dec!(99));

        let mut cfg = config(dec!(0.0005), dec!(7));
        cfg.measured_hold.insert(
            (Symbol::perp("BTC", "USDT"), Venue::Okx, Venue::Binance),
            dec!(0.5),
        );
        let ops = rank(&[&binance, &okx, &bybit], &cfg);
        let measured = find(&ops, Venue::Binance, Venue::Okx);
        assert!(measured.hold_measured);
        assert_eq!(measured.spread_hold_days, dec!(0.5));

        let other = find(&ops, Venue::Binance, Venue::Bybit);
        assert!(!other.hold_measured, "另一对腿不能借用这条半衰期");
        assert_eq!(other.spread_hold_days, dec!(7));
    }

    #[test]
    fn a_measured_hold_of_zero_is_ignored_instead_of_dividing_by_it() {
        let mut expensive_short = rate(Venue::Binance, dec!(0.0002), 8, None);
        expensive_short.mark_price = Some(dec!(101));
        let mut cfg = config(dec!(0.0005), dec!(7));
        cfg.measured_hold.insert(
            (Symbol::perp("BTC", "USDT"), Venue::Okx, Venue::Binance),
            Decimal::ZERO,
        );
        let ops = rank(
            &[&expensive_short, &rate(Venue::Okx, dec!(0.0001), 8, None)],
            &cfg,
        );
        assert_eq!(
            ops[0].spread_hold_days,
            dec!(7),
            "0 天会除爆，必须回落到配置值"
        );
        assert!(!ops[0].hold_measured);
    }

    #[test]
    fn zero_hold_days_fall_back_instead_of_panicking() {
        let a = rate(Venue::Binance, dec!(0.0002), 8, None);
        let b = rate(Venue::Okx, dec!(0.0001), 8, None);
        let zero = rank(&[&a, &b], &config(dec!(0.0005), Decimal::ZERO));
        let one = rank(&[&a, &b], &config(dec!(0.0005), Decimal::ONE));
        assert_eq!(
            find(&zero, Venue::Binance, Venue::Okx).funding_daily,
            find(&one, Venue::Binance, Venue::Okx).funding_daily
        );
    }
}
