//! 交易统计：结束了的仓位有多少笔、赚了多少、两腿一共成交了多少。持仓页「交易统计」用。
//!
//! 全部从台账算，不联网；「最近结束」是分页的，所以必须在服务端按全量算，不能让页面拿一页去加。
//!
//! 口径：
//! - **净盈亏** 与「最近结束」的已实现净额、当日亏损闸门（[`arb_exec::desk::daily_realized`]）一致：
//!   价格盈亏 − 手续费 + 资金费。`realized_pnl_usdt` 只是价格盈亏（毛），不能单独拿来报盈亏。
//!   资金费没查到的按 0 计、单独数出来；盈亏没核出来的（外部平仓数量对不上、单腿仓位……）
//!   **不按 0 算**，只计笔数。
//! - **成交额** = 两条腿所有有成交订单的成交名义之和（开仓、补仓、减仓、平仓、回滚、补仓回退）。
//!   台账的订单没有时间戳：开仓侧（开仓、补仓）记在仓位的开仓时间，其余记在结束时间（还没结束的记在开仓时间）。
//!   在交易所里手动平掉的，平仓成交不在台账里，按「开仓侧减去台账里已有的退出成交」估算，单独数出来。
//! - 没成交过的仓位（第一腿被拒、没吃到）什么都没发生，不计；用户在「最近结束」里隐藏的照样计。
//! - 用户要求「不计入统计」的仓位（台账旁 `<台账>.excluded.json`，见 [`load_excluded`]）整笔不计：
//!   盈亏、笔数、成交额都不算。只影响这里的交易统计；当日亏损闸门、熔断、对账照旧按台账全量算。
//! - 时间段按结束时间：「今日」按 UTC 日，和当日亏损闸门同一个日界。
//! - **按交易所**：成交额与汇总同一归属（各家加起来 = 汇总的成交额）；外部平仓估算的那部分按腿拆回各家。
//!   手续费按各笔订单的回报加总，与盈亏口径里按交易所成交记录核出的手续费可能有出入。

use std::collections::{BTreeMap, HashMap};

use arb_core::{Decimal, Venue};
use arb_exec::{PairPosition, PositionStatus, Replayed};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;

/// 不计入交易统计的仓位：仓位 id → 条目。
pub type Excluded = BTreeMap<String, ExcludedEntry>;

/// 清单里的一条：什么时候、为什么排除（只给人看）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, Serialize)]
pub struct ExcludedEntry {
    pub at: DateTime<Utc>,
    #[serde(default)]
    pub reason: String,
}

/// 清单文件：台账旁边的 `<台账>.excluded.json`。和「最近结束」的隐藏清单（`.dismissed.json`）分开：
/// 隐藏只是不在列表里显示、统计照算；这里是用户明确要求「这笔不算」（某次实盘：
/// 两腿上市换算口径不同的仓位，平仓亏损不小，用户要求不计入历史）。
pub fn excluded_path(ledger_path: &str) -> std::path::PathBuf {
    let mut name = std::path::Path::new(ledger_path).as_os_str().to_owned();
    name.push(".excluded.json");
    std::path::PathBuf::from(name)
}

/// 读清单。文件不存在是空；读不懂也当空（宁可多算，不能因为一个坏文件把统计抹掉），但会警告。
pub fn load_excluded(ledger_path: &str) -> Excluded {
    let path = excluded_path(ledger_path);
    match std::fs::read_to_string(&path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|error| {
            tracing::warn!(path = %path.display(), %error, "不计入统计的清单读不懂，按空处理");
            Excluded::new()
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Excluded::new(),
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "不计入统计的清单读不了，按空处理");
            Excluded::new()
        }
    }
}

/// 按日明细给最近几天（UTC 日，含今天）。
const DAILY_DAYS: i64 = 14;

/// 一个时间段的汇总。
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Summary {
    /// 结束了的、有过成交的仓位笔数（平仓 + 回滚）。
    pub ended: usize,
    pub closed: usize,
    pub unwound: usize,
    /// 其中盈亏核出来了的笔数（下面几项金额都只加这些）。
    pub settled: usize,
    pub wins: usize,
    pub losses: usize,
    pub net_usdt: Decimal,
    pub price_pnl_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub funding_usdt: Decimal,
    /// 回滚（第二腿没成、退掉第一腿）的净额合计。
    pub unwound_net_usdt: Decimal,
    pub best_usdt: Option<Decimal>,
    pub worst_usdt: Option<Decimal>,
    /// 盈亏没核出来的笔数（不计入金额）。
    pub unknown: usize,
    /// 资金费没查到、按 0 计的笔数。
    pub funding_missing: usize,
    /// 这段时间里新开、有过成交的仓位笔数（含还没结束的）。
    pub opened: usize,
    /// 两腿成交额。
    pub volume_usdt: Decimal,
    /// 成交额里有几笔外部平仓的平仓侧是估算的。
    pub volume_estimated: usize,
}

/// 一个合约在某个时间段里的结果。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SymbolStats {
    pub symbol: String,
    pub ended: usize,
    pub wins: usize,
    pub losses: usize,
    pub net_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub unknown: usize,
    pub volume_usdt: Decimal,
}

/// 一家交易所在某个时间段里的成交。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VenueStats {
    pub venue: Venue,
    /// 这段时间在这家有成交的仓位笔数。
    pub positions: usize,
    /// 有成交的订单数（外部平仓估算的部分没有订单，不计）。
    pub orders: usize,
    /// 成交额，含外部平仓估算的部分。
    pub volume_usdt: Decimal,
    /// 其中挂单（post-only）成交的。
    pub maker_volume_usdt: Decimal,
    /// 其中外部平仓按开仓名义估算的。
    pub estimated_usdt: Decimal,
    /// 订单回报的手续费合计。
    pub fee_usdt: Decimal,
}

impl VenueStats {
    fn new(venue: Venue) -> Self {
        Self {
            venue,
            positions: 0,
            orders: 0,
            volume_usdt: Decimal::ZERO,
            maker_volume_usdt: Decimal::ZERO,
            estimated_usdt: Decimal::ZERO,
            fee_usdt: Decimal::ZERO,
        }
    }
}

/// 一个时间段：汇总 + 按合约、按交易所拆开。
#[derive(Debug, Clone, Serialize)]
pub struct Window {
    /// `today` / `7d` / `30d` / `all`。
    pub key: &'static str,
    pub label: &'static str,
    /// 从什么时候算起（`all` 为 `None`）。
    pub since: Option<DateTime<Utc>>,
    pub summary: Summary,
    /// 按净盈亏从高到低；结束笔数为 0、只有成交额的合约也列出来（这段时间开了还没平）。
    pub by_symbol: Vec<SymbolStats>,
    /// 按成交额从高到低。实盘连着、这段时间没成交的交易所也列出来（见 [`TradeStats::include_venues`]）。
    pub by_venue: Vec<VenueStats>,
}

/// 一个 UTC 日。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Day {
    pub date: NaiveDate,
    pub summary: Summary,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeStats {
    pub generated_at: DateTime<Utc>,
    pub windows: Vec<Window>,
    /// 最近 [`DAILY_DAYS`] 个 UTC 日，最新的在前。
    pub daily: Vec<Day>,
    /// 按用户要求不计入统计的仓位笔数（见 [`load_excluded`]）。
    pub excluded: usize,
}

impl TradeStats {
    /// 每个时间段都列出这些交易所（实盘连着的），没成交的记 0。
    pub fn include_venues(&mut self, venues: &[Venue]) {
        for window in &mut self.windows {
            for &venue in venues {
                if !window.by_venue.iter().any(|row| row.venue == venue) {
                    window.by_venue.push(VenueStats::new(venue));
                }
            }
            sort_venues(&mut window.by_venue);
        }
    }
}

fn sort_venues(rows: &mut [VenueStats]) {
    rows.sort_by(|a, b| {
        b.volume_usdt
            .cmp(&a.volume_usdt)
            .then_with(|| a.venue.as_str().cmp(b.venue.as_str()))
    });
}

/// 一笔仓位在一家交易所的成交，开仓侧（开仓、补仓）与退出侧分开记。
#[derive(Debug, Clone, Copy, Default)]
struct VenueFills {
    entry: Decimal,
    exit: Decimal,
    entry_maker: Decimal,
    exit_maker: Decimal,
    entry_fee: Decimal,
    exit_fee: Decimal,
    entry_orders: usize,
    exit_orders: usize,
    /// 外部平仓估算补上的退出侧成交额（已含在 `exit` 里）。
    exit_estimated: Decimal,
}

/// 一笔仓位里统计要用的东西，先从台账里一次性拆好。
struct Item<'a> {
    position: &'a PairPosition,
    /// 结束了（平仓 / 回滚）才有。
    ended_at: Option<DateTime<Utc>>,
    /// 净额：`None` = 没结束，或结束了但没核出来。
    net: Option<Decimal>,
    unknown: bool,
    entry_volume: Decimal,
    /// 退出侧成交额（含外部平仓的估算部分）。
    exit_volume: Decimal,
    exit_estimated: bool,
    /// 按交易所拆开的成交；各家加起来 = 上面两项。
    venues: BTreeMap<Venue, VenueFills>,
}

/// 订单号 `{仓位}-{buy|sell}-{角色}` → 仓位 id。
fn position_of(order_id: &str) -> Option<&str> {
    let cut = ["-buy-", "-sell-"]
        .iter()
        .filter_map(|marker| order_id.rfind(marker))
        .max()?;
    Some(&order_id[..cut])
}

fn items<'a>(replayed: &'a Replayed, excluded: &Excluded) -> Vec<Item<'a>> {
    // 每笔仓位在各家的成交。
    let mut fills: HashMap<&str, BTreeMap<Venue, VenueFills>> = HashMap::new();
    for (order_id, state) in &replayed.orders {
        if state.filled_usdt <= Decimal::ZERO {
            continue;
        }
        let Some(position_id) = position_of(order_id) else {
            continue;
        };
        let Some((_, role)) = state.order.client_order_id.parse_for(position_id) else {
            continue;
        };
        let slot = fills
            .entry(position_id)
            .or_default()
            .entry(state.order.venue)
            .or_default();
        let maker = if state.order.post_only {
            state.filled_usdt
        } else {
            Decimal::ZERO
        };
        if role.is_entry() {
            slot.entry += state.filled_usdt;
            slot.entry_maker += maker;
            slot.entry_fee += state.fee_usdt;
            slot.entry_orders += 1;
        } else {
            slot.exit += state.filled_usdt;
            slot.exit_maker += maker;
            slot.exit_fee += state.fee_usdt;
            slot.exit_orders += 1;
        }
    }
    let mut out = Vec::new();
    for position in replayed.positions.values() {
        if excluded.contains_key(&position.id) {
            continue;
        }
        let mut venues = fills.remove(position.id.as_str()).unwrap_or_default();
        let entry_volume: Decimal = venues.values().map(|f| f.entry).sum();
        let mut exit_volume: Decimal = venues.values().map(|f| f.exit).sum();
        if entry_volume.is_zero() && exit_volume.is_zero() {
            // 什么都没成交过：被拒、第一腿没吃到。
            continue;
        }
        let ended = matches!(
            position.status,
            PositionStatus::Closed | PositionStatus::Unwound
        );
        let mut exit_estimated = false;
        if ended && position.is_external_close() && exit_volume < entry_volume {
            // 缺的退出侧按各家「开仓多于退出」的差额比例拆回去（通常就是在交易所里手动平掉的那条腿）。
            let missing = entry_volume - exit_volume;
            let gaps: Decimal = venues
                .values()
                .map(|f| (f.entry - f.exit).max(Decimal::ZERO))
                .sum();
            if gaps > Decimal::ZERO {
                for f in venues.values_mut() {
                    let share = (f.entry - f.exit).max(Decimal::ZERO) * missing / gaps;
                    f.exit += share;
                    f.exit_estimated += share;
                }
            }
            exit_volume = entry_volume;
            exit_estimated = true;
        }
        let (net, unknown) = match (ended, position.realized_source) {
            (false, _) => (None, false),
            (true, None) => (None, true),
            (true, Some(_)) => (
                Some(
                    position.realized_pnl_usdt - position.realized_fee_usdt
                        + position.realized_funding_usdt.unwrap_or_default(),
                ),
                false,
            ),
        };
        out.push(Item {
            position,
            ended_at: if ended {
                Some(position.closed_at.unwrap_or(position.opened_at))
            } else {
                None
            },
            net,
            unknown,
            entry_volume,
            exit_volume,
            exit_estimated,
            venues,
        });
    }
    out
}

/// 一个时间段的结果。
struct Tally {
    summary: Summary,
    by_symbol: Vec<SymbolStats>,
    by_venue: Vec<VenueStats>,
}

/// 按时间段累加。`in_range` 判断一个时刻落不落在这段里。
fn summarize<'a>(items: &[Item<'a>], in_range: impl Fn(DateTime<Utc>) -> bool) -> Tally {
    let mut summary = Summary::default();
    let mut symbols: BTreeMap<String, SymbolStats> = BTreeMap::new();
    let mut venues: BTreeMap<Venue, VenueStats> = BTreeMap::new();
    for item in items {
        let position = item.position;
        let opened_in = in_range(position.opened_at);
        // 退出侧成交：结束了的记在结束时间，没结束的（减仓）记在开仓时间。
        let exit_in = in_range(item.ended_at.unwrap_or(position.opened_at));
        let ended_in = item.ended_at.is_some_and(&in_range);
        let volume = if opened_in {
            item.entry_volume
        } else {
            Decimal::ZERO
        } + if exit_in {
            item.exit_volume
        } else {
            Decimal::ZERO
        };
        if !opened_in && !ended_in && volume.is_zero() {
            continue;
        }
        let symbol = symbols
            .entry(position.symbol.to_string())
            .or_insert_with(|| SymbolStats {
                symbol: position.symbol.to_string(),
                ended: 0,
                wins: 0,
                losses: 0,
                net_usdt: Decimal::ZERO,
                fee_usdt: Decimal::ZERO,
                unknown: 0,
                volume_usdt: Decimal::ZERO,
            });
        summary.volume_usdt += volume;
        symbol.volume_usdt += volume;
        for (&venue, f) in &item.venues {
            let (volume, maker, fee, orders) = match (opened_in, exit_in) {
                (true, true) => (
                    f.entry + f.exit,
                    f.entry_maker + f.exit_maker,
                    f.entry_fee + f.exit_fee,
                    f.entry_orders + f.exit_orders,
                ),
                (true, false) => (f.entry, f.entry_maker, f.entry_fee, f.entry_orders),
                (false, true) => (f.exit, f.exit_maker, f.exit_fee, f.exit_orders),
                (false, false) => continue,
            };
            if volume.is_zero() {
                continue;
            }
            let row = venues
                .entry(venue)
                .or_insert_with(|| VenueStats::new(venue));
            row.positions += 1;
            row.orders += orders;
            row.volume_usdt += volume;
            row.maker_volume_usdt += maker;
            row.fee_usdt += fee;
            if exit_in {
                row.estimated_usdt += f.exit_estimated;
            }
        }
        if opened_in {
            summary.opened += 1;
        }
        if exit_in && item.exit_estimated {
            summary.volume_estimated += 1;
        }
        if !ended_in {
            continue;
        }
        summary.ended += 1;
        symbol.ended += 1;
        if position.status == PositionStatus::Unwound {
            summary.unwound += 1;
        } else {
            summary.closed += 1;
        }
        let Some(net) = item.net else {
            if item.unknown {
                summary.unknown += 1;
                symbol.unknown += 1;
            }
            continue;
        };
        summary.settled += 1;
        summary.net_usdt += net;
        summary.price_pnl_usdt += position.realized_pnl_usdt;
        summary.fee_usdt += position.realized_fee_usdt;
        summary.funding_usdt += position.realized_funding_usdt.unwrap_or_default();
        if position.realized_funding_usdt.is_none() {
            summary.funding_missing += 1;
        }
        if position.status == PositionStatus::Unwound {
            summary.unwound_net_usdt += net;
        }
        summary.best_usdt = Some(summary.best_usdt.map_or(net, |best| best.max(net)));
        summary.worst_usdt = Some(summary.worst_usdt.map_or(net, |worst| worst.min(net)));
        symbol.net_usdt += net;
        symbol.fee_usdt += position.realized_fee_usdt;
        if net > Decimal::ZERO {
            summary.wins += 1;
            symbol.wins += 1;
        } else if net < Decimal::ZERO {
            summary.losses += 1;
            symbol.losses += 1;
        }
    }
    let round = |value: &mut Decimal| *value = value.round_dp(4);
    for value in [
        &mut summary.net_usdt,
        &mut summary.price_pnl_usdt,
        &mut summary.fee_usdt,
        &mut summary.funding_usdt,
        &mut summary.unwound_net_usdt,
        &mut summary.volume_usdt,
    ] {
        round(value);
    }
    summary.best_usdt = summary.best_usdt.map(|best| best.round_dp(4));
    summary.worst_usdt = summary.worst_usdt.map(|worst| worst.round_dp(4));
    let mut by_symbol: Vec<SymbolStats> = symbols
        .into_values()
        .map(|mut symbol| {
            round(&mut symbol.net_usdt);
            round(&mut symbol.fee_usdt);
            round(&mut symbol.volume_usdt);
            symbol
        })
        .collect();
    by_symbol.sort_by(|a, b| {
        b.net_usdt
            .cmp(&a.net_usdt)
            .then_with(|| b.volume_usdt.cmp(&a.volume_usdt))
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    let mut by_venue: Vec<VenueStats> = venues
        .into_values()
        .map(|mut row| {
            round(&mut row.volume_usdt);
            round(&mut row.maker_volume_usdt);
            round(&mut row.estimated_usdt);
            round(&mut row.fee_usdt);
            row
        })
        .collect();
    sort_venues(&mut by_venue);
    Tally {
        summary,
        by_symbol,
        by_venue,
    }
}

/// 全量台账的交易统计。`now` 决定「今日」与滚动时间段的起点。
#[cfg(test)]
pub fn trade_stats(replayed: &Replayed, now: DateTime<Utc>) -> TradeStats {
    trade_stats_excluding(replayed, now, &Excluded::new())
}

/// [`trade_stats`]，但 `excluded` 里的仓位整笔不计（见 [`load_excluded`]）。
pub fn trade_stats_excluding(
    replayed: &Replayed,
    now: DateTime<Utc>,
    excluded: &Excluded,
) -> TradeStats {
    let excluded_count = replayed
        .positions
        .keys()
        .filter(|id| excluded.contains_key(*id))
        .count();
    let items = items(replayed, excluded);
    let today = now.date_naive();
    let window = |key, label, since, tally: Tally| Window {
        key,
        label,
        since,
        summary: tally.summary,
        by_symbol: tally.by_symbol,
        by_venue: tally.by_venue,
    };
    let mut windows = vec![window(
        "today",
        "今日（UTC）",
        today.and_hms_opt(0, 0, 0).map(|start| start.and_utc()),
        summarize(&items, |at| at.date_naive() == today),
    )];
    for (key, label, days) in [("7d", "近 7 天", 7), ("30d", "近 30 天", 30)] {
        let since = now - Duration::days(days);
        windows.push(window(
            key,
            label,
            Some(since),
            summarize(&items, |at| at >= since),
        ));
    }
    windows.push(window("all", "全部", None, summarize(&items, |_| true)));
    let daily = (0..DAILY_DAYS)
        .filter_map(|back| today.checked_sub_signed(Duration::days(back)))
        .map(|date| Day {
            date,
            summary: summarize(&items, |at| at.date_naive() == date).summary,
        })
        .collect();
    TradeStats {
        generated_at: now,
        windows,
        daily,
        excluded: excluded_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, Symbol, Venue};
    use arb_exec::{
        ClientOrderId, LegFill, NewOrder, OrderState, OrderStatus, RealizedSource, Strategy,
        TaskRules,
    };
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn at(day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, day, hour, 0, 0).unwrap()
    }

    fn position(
        id: &str,
        base: &str,
        status: PositionStatus,
        opened: DateTime<Utc>,
        closed: Option<DateTime<Utc>>,
    ) -> PairPosition {
        PairPosition {
            id: id.into(),
            symbol: Symbol::perp(base, "USDT"),
            strategy: Strategy::Spread,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: opened,
            closed_at: closed,
            note: None,
            margin_mode: Default::default(),
            leverage: None,
            rules: TaskRules::default(),
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
            planned: None,
        }
    }

    fn settled(
        mut p: PairPosition,
        price: Decimal,
        fee: Decimal,
        funding: Option<Decimal>,
    ) -> PairPosition {
        p.realized_source = Some(RealizedSource::Executor);
        p.realized_pnl_usdt = price;
        p.realized_fee_usdt = fee;
        p.realized_funding_usdt = funding;
        p
    }

    fn order(id: &str, filled: Decimal) -> (String, OrderState) {
        order_on(id, filled, Venue::Hyperliquid, Decimal::ZERO, false)
    }

    fn order_on(
        id: &str,
        filled: Decimal,
        venue: Venue,
        fee: Decimal,
        post_only: bool,
    ) -> (String, OrderState) {
        let mut state = OrderState::new(NewOrder {
            client_order_id: ClientOrderId(id.into()),
            venue,
            symbol: Symbol::perp("X", "USDT"),
            side: if id.contains("-buy-") {
                Side::Buy
            } else {
                Side::Sell
            },
            notional_usdt: filled,
            quantity: None,
            limit_price: None,
            reduce_only: false,
            margin_mode: Default::default(),
            leverage: None,
            post_only,
        });
        state.fee_usdt = fee;
        state.filled_usdt = filled;
        state.status = if filled.is_zero() {
            OrderStatus::Rejected
        } else {
            OrderStatus::Filled
        };
        (id.into(), state)
    }

    fn replayed(positions: Vec<PairPosition>, orders: Vec<(String, OrderState)>) -> Replayed {
        Replayed {
            orders: orders.into_iter().collect(),
            positions: positions.into_iter().map(|p| (p.id.clone(), p)).collect(),
        }
    }

    fn window<'a>(stats: &'a TradeStats, key: &str) -> &'a Window {
        stats.windows.iter().find(|w| w.key == key).unwrap()
    }

    /// 净额 = 价格 − 手续费 + 资金费；资金费没查到按 0 计并数出来；核不出的只计笔数不计金额；
    /// 没成交过的整笔不计；成交额按订单的成交名义。
    #[test]
    fn net_follows_the_ledger_formula_and_unknowns_are_not_zero() {
        let now = at(15, 20);
        let win = settled(
            position(
                "live-1",
                "COIN",
                PositionStatus::Closed,
                at(15, 1),
                Some(at(15, 3)),
            ),
            dec!(1.0),
            dec!(0.2),
            Some(dec!(0.1)),
        );
        let loss = settled(
            position(
                "live-2",
                "COIN",
                PositionStatus::Closed,
                at(15, 2),
                Some(at(15, 4)),
            ),
            dec!(-0.5),
            dec!(0.1),
            None,
        );
        let rollback = settled(
            position(
                "live-3",
                "SOXL",
                PositionStatus::Unwound,
                at(15, 5),
                Some(at(15, 5)),
            ),
            dec!(-0.3),
            dec!(0.05),
            Some(Decimal::ZERO),
        );
        let unknown = position(
            "live-4",
            "NVDA",
            PositionStatus::Closed,
            at(15, 6),
            Some(at(15, 7)),
        );
        let never = position(
            "live-5",
            "PONS",
            PositionStatus::Unwound,
            at(15, 8),
            Some(at(15, 8)),
        );
        let stats = trade_stats(
            &replayed(
                vec![win, loss, rollback, unknown, never],
                vec![
                    order("live-1-buy-0", dec!(100)),
                    order("live-1-sell-1", dec!(100)),
                    order("live-1-sell-exit1", dec!(101)),
                    order("live-1-buy-exit1", dec!(99)),
                    order("live-2-buy-0", dec!(50)),
                    order("live-2-sell-0", dec!(50)),
                    order("live-2-sell-exit1", dec!(49)),
                    order("live-2-buy-exit1", dec!(51)),
                    order("live-3-buy-0", dec!(30)),
                    order("live-3-sell-0", Decimal::ZERO),
                    order("live-3-sell-exit1", dec!(29.7)),
                    order("live-4-buy-0", dec!(200)),
                    order("live-5-buy-0", Decimal::ZERO),
                ],
            ),
            now,
        );
        let today = &window(&stats, "today").summary;
        assert_eq!(
            (today.ended, today.closed, today.unwound),
            (4, 3, 1),
            "没成交过的 live-5 不计"
        );
        assert_eq!((today.settled, today.unknown), (3, 1));
        assert_eq!((today.wins, today.losses), (1, 2));
        // 0.9 + (−0.6) + (−0.35)
        assert_eq!(today.net_usdt, dec!(-0.05));
        assert_eq!(today.fee_usdt, dec!(0.35));
        assert_eq!(today.funding_usdt, dec!(0.1));
        assert_eq!(today.funding_missing, 1);
        assert_eq!(today.unwound_net_usdt, dec!(-0.35));
        assert_eq!(
            (today.best_usdt, today.worst_usdt),
            (Some(dec!(0.9)), Some(dec!(-0.6)))
        );
        assert_eq!(
            today.volume_usdt,
            dec!(400) + dec!(200) + dec!(59.7) + dec!(200)
        );
        assert_eq!(today.opened, 4);
        let coin = window(&stats, "today")
            .by_symbol
            .iter()
            .find(|s| s.symbol == "COIN/USDT")
            .unwrap();
        assert_eq!(
            (coin.ended, coin.wins, coin.losses, coin.net_usdt),
            (2, 1, 1, dec!(0.3))
        );
        let nvda = window(&stats, "today")
            .by_symbol
            .iter()
            .find(|s| s.symbol == "NVDA/USDT")
            .unwrap();
        assert_eq!((nvda.unknown, nvda.net_usdt), (1, Decimal::ZERO));
    }

    /// 用户要求不计入的仓位整笔不算（盈亏、笔数、成交额、按交易所），并数出被排除了几笔；
    /// 清单文件不存在或读不懂都按空。
    #[test]
    fn excluded_positions_are_left_out_entirely() {
        let now = at(15, 20);
        let keep = settled(
            position(
                "live-1",
                "MU",
                PositionStatus::Closed,
                at(15, 1),
                Some(at(15, 2)),
            ),
            dec!(0.5),
            dec!(0.1),
            Some(Decimal::ZERO),
        );
        let drop = settled(
            position(
                "live-2",
                "ANTHROPIC",
                PositionStatus::Closed,
                at(15, 3),
                Some(at(15, 4)),
            ),
            dec!(-72.44),
            dec!(0.33),
            Some(dec!(5.69)),
        );
        let ledger = replayed(
            vec![keep, drop],
            vec![
                order("live-1-buy-0", dec!(100)),
                order("live-1-sell-exit1", dec!(100)),
                order("live-2-buy-0", dec!(1800)),
                order("live-2-sell-exit1", dec!(1800)),
            ],
        );
        let excluded: Excluded = [(
            "live-2".to_string(),
            ExcludedEntry {
                at: now,
                reason: "口径不同".into(),
            },
        )]
        .into();
        let stats = trade_stats_excluding(&ledger, now, &excluded);
        assert_eq!(stats.excluded, 1);
        for key in ["today", "all"] {
            let w = window(&stats, key);
            assert_eq!((w.summary.ended, w.summary.losses), (1, 0), "{key}");
            assert_eq!(w.summary.net_usdt, dec!(0.4), "{key}");
            assert_eq!(w.summary.volume_usdt, dec!(200), "{key}");
            assert!(w.by_symbol.iter().all(|s| s.symbol != "ANTHROPIC/USDT"));
            assert_eq!(
                w.by_venue.iter().map(|v| v.volume_usdt).sum::<Decimal>(),
                dec!(200)
            );
        }
        assert_eq!(trade_stats(&ledger, now).windows[0].summary.ended, 2);

        let dir = std::env::temp_dir().join(format!("arb-excluded-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ledger_path = dir.join("ledger.jsonl").to_string_lossy().into_owned();
        assert!(load_excluded(&ledger_path).is_empty(), "没有文件 = 空");
        std::fs::write(excluded_path(&ledger_path), "不是 json").unwrap();
        assert!(load_excluded(&ledger_path).is_empty(), "读不懂 = 空");
        std::fs::write(
            excluded_path(&ledger_path),
            r#"{"live-2":{"at":"2026-11-04T06:00:00Z","reason":"x"}}"#,
        )
        .unwrap();
        assert!(load_excluded(&ledger_path).contains_key("live-2"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 时间段按结束时间；成交额开仓侧记在开仓时间、退出侧记在结束时间；还没结束的只算开仓侧成交额。
    #[test]
    fn windows_split_by_end_time_and_volume_by_side() {
        let now = at(15, 20);
        // 昨天开、今天平：盈亏算今天，成交额今天只算平仓侧。
        let overnight = settled(
            position(
                "live-1",
                "MU",
                PositionStatus::Closed,
                at(14, 10),
                Some(at(15, 2)),
            ),
            dec!(0.5),
            dec!(0.1),
            Some(Decimal::ZERO),
        );
        // 10 天前结束：只在近 30 天与全部里。
        let old = settled(
            position(
                "live-2",
                "MU",
                PositionStatus::Closed,
                at(5, 1),
                Some(at(5, 2)),
            ),
            dec!(-1),
            dec!(0.1),
            Some(Decimal::ZERO),
        );
        // 今天开、还持有。
        let open = position("live-3", "TSLA", PositionStatus::Open, at(15, 9), None);
        let stats = trade_stats(
            &replayed(
                vec![overnight, old, open],
                vec![
                    order("live-1-buy-0", dec!(100)),
                    order("live-1-sell-0", dec!(100)),
                    order("live-1-sell-exit1", dec!(100)),
                    order("live-1-buy-exit1", dec!(100)),
                    order("live-2-buy-0", dec!(10)),
                    order("live-2-sell-exit1", dec!(10)),
                    order("live-3-buy-0", dec!(80)),
                    order("live-3-sell-0", dec!(80)),
                ],
            ),
            now,
        );
        let today = &window(&stats, "today").summary;
        assert_eq!((today.ended, today.net_usdt), (1, dec!(0.4)));
        assert_eq!(today.volume_usdt, dec!(200) + dec!(160));
        assert_eq!(today.opened, 1);
        let week = &window(&stats, "7d").summary;
        assert_eq!((week.ended, week.volume_usdt), (1, dec!(560)));
        let month = &window(&stats, "30d").summary;
        assert_eq!(
            (month.ended, month.net_usdt, month.volume_usdt),
            (2, dec!(-0.7), dec!(580))
        );
        assert_eq!(window(&stats, "all").summary, *month);
        assert_eq!(stats.daily.len(), DAILY_DAYS as usize);
        assert_eq!(stats.daily[0].date, now.date_naive());
        assert_eq!(stats.daily[0].summary.ended, 1);
        assert_eq!(
            stats.daily[1].summary.volume_usdt,
            dec!(200),
            "昨天：live-1 开仓侧"
        );
    }

    /// 交易所里手动平掉的：平仓成交不在台账里，按开仓侧补足并标为估算。
    #[test]
    fn external_close_estimates_the_missing_exit_volume() {
        let now = at(15, 20);
        let mut external = settled(
            position(
                "live-1",
                "QQQ",
                PositionStatus::Closed,
                at(15, 1),
                Some(at(15, 2)),
            ),
            dec!(0.2),
            dec!(0.05),
            Some(Decimal::ZERO),
        );
        external.realized_source = Some(RealizedSource::VenueFills);
        external.closed_externally = true;
        external.entry_legs = Some(arb_exec::EntryLegs {
            long: LegFill {
                venue: Venue::Hyperliquid,
                side: Side::Buy,
                notional_usdt: dec!(100),
                average_price: dec!(10),
                fee_usdt: Decimal::ZERO,
                client_order_id: ClientOrderId("live-1-buy-0".into()),
                margin_usdt: None,
            },
            short: LegFill {
                venue: Venue::Lighter,
                side: Side::Sell,
                notional_usdt: dec!(100),
                average_price: dec!(10),
                fee_usdt: Decimal::ZERO,
                client_order_id: ClientOrderId("live-1-sell-0".into()),
                margin_usdt: None,
            },
        });
        let stats = trade_stats(
            &replayed(
                vec![external],
                vec![
                    order("live-1-buy-0", dec!(100)),
                    order("live-1-sell-0", dec!(100)),
                    // 一条腿在看板上平了，另一条在交易所里手动平。
                    order("live-1-buy-exit1", dec!(100)),
                ],
            ),
            now,
        );
        let today = &window(&stats, "today").summary;
        assert_eq!(today.volume_usdt, dec!(400));
        assert_eq!(today.volume_estimated, 1);
        assert_eq!(today.net_usdt, dec!(0.15));
    }

    /// 按交易所：各家加起来等于汇总的成交额；外部平仓估算的那部分记到手动平掉的那条腿所在的交易所；
    /// 挂单成交额、订单数、手续费、仓位笔数按家分开；实盘连着但没成交的交易所列为 0。
    #[test]
    fn volume_splits_by_venue_and_adds_up_to_the_summary() {
        let now = at(15, 20);
        let both = settled(
            position(
                "live-1",
                "MU",
                PositionStatus::Closed,
                at(15, 1),
                Some(at(15, 2)),
            ),
            dec!(0.5),
            dec!(0.1),
            Some(Decimal::ZERO),
        );
        let mut external = settled(
            position(
                "live-2",
                "OPENAI",
                PositionStatus::Closed,
                at(15, 3),
                Some(at(15, 4)),
            ),
            dec!(0.2),
            dec!(0.02),
            Some(Decimal::ZERO),
        );
        external.realized_source = Some(RealizedSource::VenueFills);
        external.closed_externally = true;
        // 昨天开、还持有：今天什么都不算，近 7 天算开仓侧。
        let open = position("live-3", "MU", PositionStatus::Open, at(14, 9), None);
        let stats = {
            let mut stats = trade_stats(
                &replayed(
                    vec![both, external, open],
                    vec![
                        order_on(
                            "live-1-buy-0",
                            dec!(100),
                            Venue::HyperliquidXyz,
                            dec!(0.009),
                            false,
                        ),
                        order_on(
                            "live-1-sell-0",
                            dec!(100),
                            Venue::Arcus,
                            dec!(0.0225),
                            false,
                        ),
                        order_on(
                            "live-1-sell-exit1",
                            dec!(101),
                            Venue::HyperliquidXyz,
                            dec!(0.009),
                            false,
                        ),
                        order_on(
                            "live-1-buy-exit1",
                            dec!(99),
                            Venue::Arcus,
                            dec!(0.0225),
                            false,
                        ),
                        order_on(
                            "live-2-buy-0",
                            dec!(50),
                            Venue::LighterRh,
                            Decimal::ZERO,
                            false,
                        ),
                        order_on(
                            "live-2-sell-0",
                            dec!(50),
                            Venue::Binance,
                            Decimal::ZERO,
                            true,
                        ),
                        // 币安那条腿在看板上平了，Lighter RH 那条在交易所里手动平。
                        order_on(
                            "live-2-buy-exit1",
                            dec!(50),
                            Venue::Binance,
                            dec!(0.02),
                            false,
                        ),
                        order_on("live-3-buy-0", dec!(80), Venue::Arcus, dec!(0.018), false),
                        order_on(
                            "live-3-sell-0",
                            dec!(80),
                            Venue::LighterRh,
                            Decimal::ZERO,
                            false,
                        ),
                    ],
                ),
                now,
            );
            stats.include_venues(&[Venue::Arcus, Venue::Hyperliquid]);
            stats
        };
        let today = window(&stats, "today");
        let venue = |venue: Venue| {
            today
                .by_venue
                .iter()
                .find(|row| row.venue == venue)
                .unwrap()
                .clone()
        };
        let total: Decimal = today.by_venue.iter().map(|row| row.volume_usdt).sum();
        assert_eq!(total, today.summary.volume_usdt);
        assert_eq!(total, dec!(400) + dec!(200));
        let xyz = venue(Venue::HyperliquidXyz);
        assert_eq!(
            (xyz.positions, xyz.orders, xyz.volume_usdt, xyz.fee_usdt),
            (1, 2, dec!(201), dec!(0.018))
        );
        let rh = venue(Venue::LighterRh);
        assert_eq!(
            (rh.orders, rh.volume_usdt, rh.estimated_usdt),
            (1, dec!(100), dec!(50)),
            "手动平掉的平仓侧按开仓名义估算，记在 Lighter RH"
        );
        let binance = venue(Venue::Binance);
        assert_eq!(
            (
                binance.volume_usdt,
                binance.maker_volume_usdt,
                binance.estimated_usdt
            ),
            (dec!(100), dec!(50), Decimal::ZERO)
        );
        assert_eq!(venue(Venue::Arcus).volume_usdt, dec!(199));
        let hl = venue(Venue::Hyperliquid);
        assert_eq!(
            (hl.positions, hl.volume_usdt),
            (0, Decimal::ZERO),
            "连着但没成交"
        );
        assert_eq!(
            today.by_venue.last().unwrap().venue,
            Venue::Hyperliquid,
            "按成交额从高到低"
        );
        assert_eq!(today.by_venue.first().unwrap().venue, Venue::HyperliquidXyz);
        let week = window(&stats, "7d");
        let week_total: Decimal = week.by_venue.iter().map(|row| row.volume_usdt).sum();
        assert_eq!(week_total, week.summary.volume_usdt);
        assert_eq!(
            week.by_venue
                .iter()
                .find(|row| row.venue == Venue::Arcus)
                .map(|row| (row.positions, row.volume_usdt)),
            Some((2, dec!(279))),
            "还持有的那笔只算开仓侧"
        );
    }

    #[test]
    fn order_ids_map_back_to_their_position() {
        assert_eq!(
            position_of("live-1791814240325-buy-0"),
            Some("live-1791814240325")
        );
        assert_eq!(
            position_of("live-1791814240325-sell-exit2"),
            Some("live-1791814240325")
        );
        assert_eq!(position_of("arcus:oid:123"), None);
    }
}
