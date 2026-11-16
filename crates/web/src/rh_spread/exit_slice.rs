//! 按深度分批平仓的影子（只观察、不下单）。
//!
//! 想法（2026-11-04 用户定的 B 方案）：盘口上哪一段深度平掉已经够「按比例分摊的止盈」，就只平那一段的数量，
//! 剩下的继续拿着。用 10-31～11-04 的测算数据离线回放过：片净 ≥ 比例 × 止盈才切，已平的那些笔里模拟结果比实际略好。
//! 回放偏乐观的几处，这里在线量：
//!
//! - **执行延迟**：每一片在决策后 [`CHECK_DELAYS`] 秒按当时的盘口给同样的数量重新定价（`check` 行），平仓一次
//!   要 1.5～2.5 秒，看这段时间盘口会不会走掉。
//! - **自己吃掉的深度**：影子不下单，盘口上那几档不会消失。切过一片后 [`CONSUMED_FOR`] 内，从两腿盘口顶上扣掉
//!   已经虚拟成交的数量再算下一片（偏保守，但不重复算同一段深度）。
//! - **最小下单额**：每片、以及切完剩下的部分都不少于 [`MIN_SLICE_USDT`]（覆盖 Hyperliquid 10 美元下限）。
//!   各家数量步长没算。
//!
//! 已平掉多少按两条腿的**绝对数量**记：持仓会补仓，补完比例的分母就变了。剩下的部分按剩余比例缩小止盈，
//! 盘口够了就整段平掉（`rest` 行），这一笔的虚拟持仓就结束了；真实持仓平掉时补一行 `end` 汇总。
//!
//! 门槛有两套轨道（[`Track`]）：持仓实际的止盈（11-03 起多数是 0.05U），以及固定按 0.25U 的参照
//! （实际止盈就是 0.25U 时不重复记）。止盈低到 0.05U 时「比例 × 止盈」几乎就是保本线，接近回放里少赚的 A 方案，
//! 两套一起记，事后不用重新部署就能比。资金费没查到不切（不拿 0 冒充）；两腿盘口都要不超过 [`SLICE_FRESH_MS`]。
//!
//! 记录写进 `rh-spread/slice-shadow-YYYYMMDD.jsonl`；每行带着写完之后的累计状态，重启时读回最近
//! [`RESTORE_DAYS`] 天，接着上次切到哪里（还没做完的延迟复核丢掉）。

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

use arb_core::{Decimal, Level, OrderBook};
use arb_exec::monitor::exit_quote_part;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;

use super::exit_shadow::{Sample, Watched};

pub const SLICE_PREFIX: &str = "slice-shadow";
/// 只认两腿盘口都不超过这么旧的采样（毫秒），与离线回放一致。
pub const SLICE_FRESH_MS: u64 = 2000;
/// 同一笔（同一套门槛）两片之间至少隔多久。
pub const SLICE_GAP: Duration = Duration::from_secs(3);
/// 切过的深度在这段时间内当作已经被吃掉。
pub const CONSUMED_FOR: Duration = Duration::from_secs(10);
/// 决策后几秒按当时盘口复核同一片。
pub const CHECK_DELAYS: [u64; 3] = [1, 2, 5];
/// 计入「延迟后」累计的那一次复核（秒）。
pub const BANKED_DELAY: u64 = 2;
/// 重启时读回几天的记录。
pub const RESTORE_DAYS: i64 = 7;

/// 每一片、切完剩下的部分都不少于这么多（USDT，多腿入场名义）。
pub fn min_slice_usdt() -> Decimal {
    Decimal::new(20, 0)
}

/// 参照轨道的止盈（USDT）。
pub fn reference_take_profit() -> Decimal {
    Decimal::new(25, 2)
}

/// 用哪套止盈门槛。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Track {
    /// 持仓实际的止盈。
    Actual,
    /// 固定按 0.25U。
    Reference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// 切一片（之后还剩）。
    Slice,
    /// 剩下的整段平掉（没切过的就是整笔到止盈）。
    Rest,
    /// 决策后若干秒按当时盘口给同一片重新定价。
    Check,
    /// 真实持仓没了（平掉了）：汇总。
    End,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SliceRow {
    pub at: DateTime<Utc>,
    pub position: String,
    pub symbol: String,
    pub track: Track,
    pub kind: Kind,
    /// 这一套门槛下整笔的止盈（USDT）。
    pub take_profit_usdt: Decimal,
    /// 第几片（从 1 起）；`check` 行是它复核的那一片；`end` 行是 0。
    pub seq: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_qty: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_qty: Option<Decimal>,
    /// 这一片占（当时）持仓的比例。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<Decimal>,
    /// 这一片的多腿入场名义。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notional_usdt: Option<Decimal>,
    /// 这一片要达到的净额：比例 × 止盈。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_usdt: Option<Decimal>,
    /// 按盘口平掉这一片的净额（含按比例分摊的资金费）。`check` 行盘口不够时为空。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub net_usdt: Option<Decimal>,
    /// `check`：计划隔几秒、实际隔了多少毫秒。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_s: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    // 写完这一行之后的累计状态（重启读回用）。
    pub closed_long_qty: Decimal,
    pub closed_short_qty: Decimal,
    pub slices: u32,
    /// 各片决策时的净额合计。
    pub banked_usdt: Decimal,
    /// 各片隔 [`BANKED_DELAY`] 秒复核的净额合计（复核时盘口不够的那片按 0 计，另见 `missed`）。
    pub banked_delayed_usdt: Decimal,
    /// 隔 [`BANKED_DELAY`] 秒复核时盘口不够、报不出价的片数。
    #[serde(default)]
    pub missed: u32,
    /// 虚拟持仓已经平完。
    pub done: bool,
}

struct Pending {
    seq: u32,
    at: Instant,
    long_qty: Decimal,
    short_qty: Decimal,
    delays: Vec<u64>,
}

struct Consumed {
    seq: u32,
    at: Instant,
    long_qty: Decimal,
    short_qty: Decimal,
}

#[derive(Default)]
struct Virtual {
    closed_long_qty: Decimal,
    closed_short_qty: Decimal,
    slices: u32,
    banked_usdt: Decimal,
    banked_delayed_usdt: Decimal,
    missed: u32,
    done: bool,
    /// 这次运行里在持仓名单上见过（重启读回的、还没见过的，名单不空时才收尾）。
    seen: bool,
    take_profit_usdt: Decimal,
    symbol: String,
    last_slice: Option<Instant>,
    consumed: Vec<Consumed>,
    pending: Vec<Pending>,
}

/// 盘口顶上扣掉 `quantity` 个。
fn eat(levels: &[Level], mut quantity: Decimal) -> Vec<Level> {
    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        if quantity <= Decimal::ZERO || level.price <= Decimal::ZERO {
            out.push(level.clone());
            continue;
        }
        let size = level.notional_usdt / level.price;
        if size <= quantity {
            quantity -= size;
            continue;
        }
        out.push(Level {
            price: level.price,
            notional_usdt: (size - quantity) * level.price,
        });
        quantity = Decimal::ZERO;
    }
    out
}

/// 盘口按各档累计数量占 `total` 的比例（小于 1 的部分）。
fn cumulative_fractions(levels: &[Level], total: Decimal, out: &mut Vec<Decimal>) {
    if total <= Decimal::ZERO {
        return;
    }
    let mut sum = Decimal::ZERO;
    for level in levels {
        if level.price <= Decimal::ZERO {
            continue;
        }
        sum += level.notional_usdt / level.price;
        let fraction = (sum / total).round_dp(6);
        if fraction >= Decimal::ONE {
            break;
        }
        if fraction > Decimal::ZERO {
            out.push(fraction);
        }
    }
}

#[derive(Default)]
pub struct Slicer {
    states: HashMap<(String, Track), Virtual>,
}

impl Slicer {
    /// 读回最近 [`RESTORE_DAYS`] 天的记录，接着上次的累计状态。读不到就从头开始。
    pub fn restore(dir: &Path, now: DateTime<Utc>) -> Self {
        let mut slicer = Self::default();
        let mut ended: HashSet<(String, Track)> = HashSet::new();
        for back in (0..=RESTORE_DAYS).rev() {
            let day = (now - chrono::Duration::days(back)).date_naive();
            let path = super::history::file_for(dir, SLICE_PREFIX, day);
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                let Ok(row) = serde_json::from_str::<SliceRow>(line) else {
                    continue;
                };
                let key = (row.position.clone(), row.track);
                if row.kind == Kind::End {
                    slicer.states.remove(&key);
                    ended.insert(key);
                    continue;
                }
                if ended.contains(&key) {
                    continue;
                }
                let state = slicer.states.entry(key).or_default();
                state.closed_long_qty = row.closed_long_qty;
                state.closed_short_qty = row.closed_short_qty;
                state.slices = row.slices;
                state.banked_usdt = row.banked_usdt;
                state.banked_delayed_usdt = row.banked_delayed_usdt;
                state.missed = row.missed;
                state.done = row.done;
                state.take_profit_usdt = row.take_profit_usdt;
                state.symbol = row.symbol.clone();
            }
        }
        if !slicer.states.is_empty() {
            info!(
                restored = slicer.states.len(),
                "分批平仓影子：读回了上次的虚拟持仓"
            );
        }
        slicer
    }

    /// 一笔持仓的一次采样（盘口与 `sample` 同一份）。要写的行追加进 `rows`。
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        item: &Watched,
        sample: &Sample,
        long_book: &OrderBook,
        short_book: &OrderBook,
        now: Instant,
        now_utc: DateTime<Utc>,
        rows: &mut Vec<SliceRow>,
    ) {
        let position = &item.position;
        let Some(take_profit) = position.rules.take_profit_usdt else {
            return;
        };
        let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
        else {
            return;
        };
        let (Some(long_total), Some(short_total)) = (long_leg.quantity(), short_leg.quantity())
        else {
            return;
        };
        if long_total <= Decimal::ZERO || short_total <= Decimal::ZERO {
            return;
        }
        let mut tracks = vec![(Track::Actual, take_profit)];
        if take_profit != reference_take_profit() {
            tracks.push((Track::Reference, reference_take_profit()));
        }
        let funding = item.funding_usdt;
        let fresh = sample.long_age_ms <= SLICE_FRESH_MS && sample.short_age_ms <= SLICE_FRESH_MS;
        for (track, target) in tracks {
            let state = self.states.entry((position.id.clone(), track)).or_default();
            state.seen = true;
            state.take_profit_usdt = target;
            state.symbol = sample.symbol.clone();
            let base = |kind: Kind, seq: u32, state: &Virtual| SliceRow {
                at: now_utc,
                position: position.id.clone(),
                symbol: sample.symbol.clone(),
                track,
                kind,
                take_profit_usdt: target,
                seq,
                long_qty: None,
                short_qty: None,
                fraction: None,
                notional_usdt: None,
                target_usdt: None,
                net_usdt: None,
                delay_s: None,
                elapsed_ms: None,
                long_age_ms: Some(sample.long_age_ms),
                short_age_ms: Some(sample.short_age_ms),
                note: None,
                closed_long_qty: state.closed_long_qty,
                closed_short_qty: state.closed_short_qty,
                slices: state.slices,
                banked_usdt: state.banked_usdt,
                banked_delayed_usdt: state.banked_delayed_usdt,
                missed: state.missed,
                done: state.done,
            };

            // 1. 到点的延迟复核：盘口扣掉别的片吃掉的深度（不扣它自己），给同样的数量重新定价。
            //    资金费没查到时等下一次（`elapsed_ms` 记实际隔了多久）。
            if let Some(funding) = funding {
                let mut checks = Vec::new();
                for pending in &mut state.pending {
                    let elapsed = now.saturating_duration_since(pending.at);
                    let due: Vec<u64> = pending
                        .delays
                        .iter()
                        .copied()
                        .filter(|delay| elapsed >= Duration::from_secs(*delay))
                        .collect();
                    if due.is_empty() {
                        continue;
                    }
                    pending.delays.retain(|delay| !due.contains(delay));
                    let others: Vec<&Consumed> = state
                        .consumed
                        .iter()
                        .filter(|consumed| consumed.seq != pending.seq)
                        .collect();
                    let (lb, sb) = adjusted(long_book, short_book, &others);
                    let fraction = (pending.long_qty / long_total).min(Decimal::ONE);
                    let net = if fraction > Decimal::ZERO {
                        exit_quote_part(position, &lb, &sb, item.fees, fraction)
                            .ok()
                            .map(|quote| (quote.net_usdt + funding * fraction).round_dp(6))
                    } else {
                        None
                    };
                    for delay in due {
                        checks.push((
                            pending.seq,
                            pending.long_qty,
                            pending.short_qty,
                            fraction,
                            delay,
                            elapsed,
                            net,
                        ));
                    }
                }
                state.pending.retain(|pending| !pending.delays.is_empty());
                for (seq, long_qty, short_qty, fraction, delay, elapsed, net) in checks {
                    if delay == BANKED_DELAY {
                        match net {
                            Some(net) => state.banked_delayed_usdt += net,
                            None => state.missed += 1,
                        }
                    }
                    let mut row = base(Kind::Check, seq, state);
                    row.long_qty = Some(long_qty);
                    row.short_qty = Some(short_qty);
                    row.fraction = Some(fraction.round_dp(6));
                    row.net_usdt = net;
                    row.delay_s = Some(delay);
                    row.elapsed_ms = Some(elapsed.as_millis() as u64);
                    if net.is_none() {
                        row.note = Some("盘口不够，报不出价".into());
                    }
                    rows.push(row);
                }
            }

            // 2. 要不要切下一片。
            if state.done {
                continue;
            }
            let Some(funding) = funding else {
                continue;
            };
            if !fresh {
                continue;
            }
            if state
                .last_slice
                .is_some_and(|at| now.saturating_duration_since(at) < SLICE_GAP)
            {
                continue;
            }
            state
                .consumed
                .retain(|consumed| now.saturating_duration_since(consumed.at) < CONSUMED_FOR);
            let closed = (state.closed_long_qty / long_total).round_dp(6);
            if closed >= Decimal::ONE {
                // 减过仓、虚拟已平的比真实持仓还多：当平完了。
                state.done = true;
                continue;
            }
            let remaining = Decimal::ONE - closed;
            let all: Vec<&Consumed> = state.consumed.iter().collect();
            let (lb, sb) = adjusted(long_book, short_book, &all);
            let mut fractions = Vec::new();
            cumulative_fractions(&lb.bids, long_total, &mut fractions);
            cumulative_fractions(&sb.asks, short_total, &mut fractions);
            fractions.retain(|fraction| *fraction < remaining);
            fractions.push(remaining);
            fractions.sort();
            fractions.dedup();
            let notional = long_leg.notional_usdt;
            let min = min_slice_usdt();
            let mut chosen = None;
            for fraction in fractions.into_iter().rev() {
                let rest = fraction == remaining;
                if !rest && (notional * fraction < min || notional * (remaining - fraction) < min) {
                    continue;
                }
                let Ok(quote) = exit_quote_part(position, &lb, &sb, item.fees, fraction) else {
                    continue;
                };
                let net = quote.net_usdt + funding * fraction;
                if net >= target * fraction {
                    chosen = Some((fraction, net.round_dp(6), rest));
                    break;
                }
            }
            let Some((fraction, net, rest)) = chosen else {
                continue;
            };
            let long_qty = (long_total * fraction).round_dp(12);
            let short_qty = (short_total * fraction).round_dp(12);
            state.slices += 1;
            let seq = state.slices;
            state.closed_long_qty += long_qty;
            state.closed_short_qty += short_qty;
            state.banked_usdt += net;
            state.done = rest;
            state.last_slice = Some(now);
            state.consumed.push(Consumed {
                seq,
                at: now,
                long_qty,
                short_qty,
            });
            state.pending.push(Pending {
                seq,
                at: now,
                long_qty,
                short_qty,
                delays: CHECK_DELAYS.to_vec(),
            });
            let kind = if rest { Kind::Rest } else { Kind::Slice };
            let mut row = base(kind, seq, state);
            row.long_qty = Some(long_qty);
            row.short_qty = Some(short_qty);
            row.fraction = Some(fraction);
            row.notional_usdt = Some((notional * fraction).round_dp(4));
            row.target_usdt = Some((target * fraction).round_dp(6));
            row.net_usdt = Some(net);
            info!(
                position = %position.id,
                symbol = %sample.symbol,
                track = ?track,
                seq,
                fraction = %fraction,
                net_usdt = %net,
                target_usdt = %(target * fraction).round_dp(4),
                "分批平仓影子：{}（只记录，不下单）",
                if rest { "剩下的整段到了按比例的止盈，会平掉" } else { "这一段深度到了按比例的止盈，会平这一片" }
            );
            rows.push(row);
        }
    }

    /// 真实持仓没了的：补一行汇总，不再盯。重启读回、这次还没见过的，持仓名单不空时才收尾
    /// （名单空可能只是规则轮还没跑）。
    pub fn finish(
        &mut self,
        watched: &[Watched],
        now_utc: DateTime<Utc>,
        rows: &mut Vec<SliceRow>,
    ) {
        let open: HashSet<&str> = watched
            .iter()
            .map(|item| item.position.id.as_str())
            .collect();
        let listed = !watched.is_empty();
        let gone: Vec<(String, Track)> = self
            .states
            .iter()
            .filter(|((id, _), state)| !open.contains(id.as_str()) && (state.seen || listed))
            .map(|(key, _)| key.clone())
            .collect();
        for key in gone {
            let Some(state) = self.states.remove(&key) else {
                continue;
            };
            if state.slices > 0 {
                info!(
                    position = %key.0,
                    symbol = %state.symbol,
                    track = ?key.1,
                    slices = state.slices,
                    banked_usdt = %state.banked_usdt.round_dp(4),
                    banked_delayed_usdt = %state.banked_delayed_usdt.round_dp(4),
                    done = state.done,
                    "分批平仓影子：持仓已平，虚拟分批合计"
                );
            }
            rows.push(SliceRow {
                at: now_utc,
                position: key.0,
                symbol: state.symbol,
                track: key.1,
                kind: Kind::End,
                take_profit_usdt: state.take_profit_usdt,
                seq: 0,
                long_qty: None,
                short_qty: None,
                fraction: None,
                notional_usdt: None,
                target_usdt: None,
                net_usdt: None,
                delay_s: None,
                elapsed_ms: None,
                long_age_ms: None,
                short_age_ms: None,
                note: None,
                closed_long_qty: state.closed_long_qty,
                closed_short_qty: state.closed_short_qty,
                slices: state.slices,
                banked_usdt: state.banked_usdt,
                banked_delayed_usdt: state.banked_delayed_usdt,
                missed: state.missed,
                done: state.done,
            });
        }
    }
}

/// 两腿盘口扣掉这些片吃掉的深度：多腿卖进买盘，空腿从卖盘买回。
fn adjusted(
    long_book: &OrderBook,
    short_book: &OrderBook,
    consumed: &[&Consumed],
) -> (OrderBook, OrderBook) {
    let long_qty: Decimal = consumed.iter().map(|consumed| consumed.long_qty).sum();
    let short_qty: Decimal = consumed.iter().map(|consumed| consumed.short_qty).sum();
    let mut long = long_book.clone();
    let mut short = short_book.clone();
    if long_qty > Decimal::ZERO {
        long.bids = eat(&long_book.bids, long_qty);
    }
    if short_qty > Decimal::ZERO {
        short.asks = eat(&short_book.asks, short_qty);
    }
    (long, short)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, Symbol, Venue};
    use arb_exec::monitor::ExitFees;
    use arb_exec::{ClientOrderId, LegFill, PairPosition, PositionStatus, Strategy, TaskRules};
    use rust_decimal_macros::dec;

    fn leg(venue: Venue, side: Side, notional: Decimal) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: notional,
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("p", side, 0),
            margin_usdt: Some(dec!(200)),
        }
    }

    fn position(take_profit: Decimal, notional: Decimal) -> PairPosition {
        PairPosition {
            margin_mode: arb_exec::MarginMode::Isolated,
            id: "p".into(),
            symbol: Symbol::perp("NBIS", "USDT"),
            strategy: Strategy::Spread,
            long: Some(leg(Venue::Lighter, Side::Buy, notional)),
            short: Some(leg(Venue::Arcus, Side::Sell, notional)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules: TaskRules {
                take_profit_usdt: Some(take_profit),
                ..TaskRules::default()
            },
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

    fn watched(take_profit: Decimal, notional: Decimal, funding: Option<Decimal>) -> Watched {
        Watched {
            position: position(take_profit, notional),
            fees: ExitFees {
                long: Some(Decimal::ZERO),
                short: Some(Decimal::ZERO),
            },
            funding_usdt: funding,
        }
    }

    fn levels(levels: &[(Decimal, Decimal)]) -> Vec<Level> {
        levels
            .iter()
            .map(|(price, size)| Level {
                price: *price,
                notional_usdt: *price * *size,
            })
            .collect()
    }

    /// 多腿（Lighter）买盘第一档 `first` 个 @100.2、之后 1000 个 @99；空腿（Arcus）卖盘 1000 个 @100.1。
    /// 入场两腿都是 100：第一档每个赚 0.1，第二档每个亏 1.1。
    fn books(first: Decimal) -> (OrderBook, OrderBook) {
        let symbol = Symbol::perp("NBIS", "USDT");
        (
            OrderBook {
                venue: Venue::Lighter,
                symbol: symbol.clone(),
                bids: levels(&[(dec!(100.2), first), (dec!(99), dec!(1000))]),
                asks: levels(&[(dec!(100.3), dec!(1000))]),
            },
            OrderBook {
                venue: Venue::Arcus,
                symbol,
                bids: levels(&[(dec!(100), dec!(1000))]),
                asks: levels(&[(dec!(100.1), dec!(1000))]),
            },
        )
    }

    fn sample(age_ms: u64) -> Sample {
        Sample {
            at: Utc::now(),
            position: "p".into(),
            symbol: "NBIS/USDT".into(),
            long: Venue::Lighter,
            short: Venue::Arcus,
            notional_usdt: dec!(1000),
            take_profit_usdt: None,
            funding_usdt: None,
            long_age_ms: age_ms,
            short_age_ms: age_ms,
            positive: false,
            ladder: Vec::new(),
            would_close: false,
        }
    }

    fn observe_at(
        slicer: &mut Slicer,
        item: &Watched,
        books: &(OrderBook, OrderBook),
        at: Instant,
    ) -> Vec<SliceRow> {
        let mut rows = Vec::new();
        slicer.observe(
            item,
            &sample(100),
            &books.0,
            &books.1,
            at,
            Utc::now(),
            &mut rows,
        );
        rows
    }

    /// 第一档那 3 个（持仓 10 个的 30%）平掉净赚 0.3，到了 0.3 × 0.25 = 0.075：切这一片；整笔吃到 99 是亏的，不平。
    /// 止盈就是 0.25 时没有参照轨道。
    #[test]
    fn a_level_that_clears_its_share_of_take_profit_is_sliced() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let rows = observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now());
        assert_eq!(rows.len(), 1, "{rows:?}");
        let row = &rows[0];
        assert_eq!(
            (row.track, row.kind, row.seq),
            (Track::Actual, Kind::Slice, 1)
        );
        assert_eq!(row.fraction, Some(dec!(0.3)));
        assert_eq!(row.net_usdt, Some(dec!(0.3)));
        assert_eq!(row.target_usdt, Some(dec!(0.075)));
        assert_eq!(row.notional_usdt, Some(dec!(300)));
        assert_eq!(
            (row.closed_long_qty, row.closed_short_qty),
            (dec!(3), dec!(3))
        );
        assert!(!row.done);
        // 止盈 1：这一片要 0.3 × 1 = 0.3，正好够（不是拿整笔的 1 去比）。
        let mut slicer = Slicer::default();
        let item = watched(dec!(1), dec!(1000), Some(Decimal::ZERO));
        let rows = observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now());
        let actual = rows
            .iter()
            .find(|row| row.track == Track::Actual)
            .expect("actual");
        assert_eq!(actual.target_usdt, Some(dec!(0.3)));
    }

    /// 止盈 1.5：这一片要 0.45 才够，0.3 不切；参照轨道按 0.25 照切。付出去的资金费按比例摊。
    #[test]
    fn the_actual_and_the_reference_take_profit_are_tracked_separately() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(1.5), dec!(1000), Some(Decimal::ZERO));
        let rows = observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].track, Track::Reference);
        assert_eq!(rows[0].take_profit_usdt, dec!(0.25));
        // 付了 0.8 资金费：这一片摊 0.24，剩 0.06 < 0.075，参照轨道也不切。
        let mut slicer = Slicer::default();
        let item = watched(dec!(1.5), dec!(1000), Some(dec!(-0.8)));
        assert!(observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now()).is_empty());
    }

    /// 资金费没查到、盘口不新鲜：不切。
    #[test]
    fn unknown_funding_or_stale_books_do_not_slice() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), None);
        assert!(observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now()).is_empty());
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let mut rows = Vec::new();
        let (long, short) = books(dec!(3));
        slicer.observe(
            &item,
            &sample(SLICE_FRESH_MS + 1),
            &long,
            &short,
            Instant::now(),
            Utc::now(),
            &mut rows,
        );
        assert!(rows.is_empty());
    }

    /// 每片、切完剩下的都不少于 20U：第一档只有 0.1 个（10U）不切。
    #[test]
    fn slices_below_the_minimum_are_skipped() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        assert!(observe_at(&mut slicer, &item, &books(dec!(0.1)), Instant::now()).is_empty());
    }

    /// 切过的深度 10 秒内当作被吃掉：3 秒后同一份盘口不再切；复核按 1/2/5 秒给同一片重新定价（不扣它自己）；
    /// 10 秒后深度算回来，再切第二片。
    #[test]
    fn eaten_depth_is_held_back_and_each_slice_is_rechecked_later() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let book = books(dec!(3));
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        assert_eq!(
            observe_at(&mut slicer, &item, &book, at(0))[0].kind,
            Kind::Slice
        );
        let rows = observe_at(&mut slicer, &item, &book, at(1000));
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].kind, rows[0].delay_s), (Kind::Check, Some(1)));
        assert_eq!(
            rows[0].net_usdt,
            Some(dec!(0.3)),
            "复核不扣这一片自己吃的深度"
        );
        let rows = observe_at(&mut slicer, &item, &book, at(2000));
        assert_eq!(rows[0].delay_s, Some(2));
        assert_eq!(rows[0].banked_delayed_usdt, dec!(0.3));
        // 3 秒后：第一档被这一片吃掉了，剩下的 99 那档是亏的，不切。
        let rows = observe_at(&mut slicer, &item, &book, at(3000));
        assert!(rows.iter().all(|row| row.kind == Kind::Check), "{rows:?}");
        let rows = observe_at(&mut slicer, &item, &book, at(5000));
        assert_eq!(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
            vec![Kind::Check]
        );
        assert_eq!(rows[0].delay_s, Some(5));
        // 10 秒后算回来：第二片。
        let rows = observe_at(&mut slicer, &item, &book, at(10_500));
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].kind, rows[0].seq), (Kind::Slice, 2));
        assert_eq!(rows[0].closed_long_qty, dec!(6));
        assert_eq!(rows[0].banked_usdt, dec!(0.6));
    }

    /// 剩下的整段到了按比例缩小的止盈：整段平掉（`rest`），之后不再切；真实持仓没了补一行汇总。
    #[test]
    fn the_rest_closes_at_the_scaled_take_profit_and_the_position_is_summed_up() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let start = Instant::now();
        observe_at(&mut slicer, &item, &books(dec!(3)), start);
        // 买盘第一档变厚到 1000 个：剩下的 7 个（70%）平掉赚 0.7 ≥ 0.175。
        let deep = books(dec!(1000));
        let later = start + CONSUMED_FOR + Duration::from_secs(1);
        let rows = observe_at(&mut slicer, &item, &deep, later);
        let rest = rows
            .iter()
            .find(|row| row.kind == Kind::Rest)
            .expect("rest");
        assert_eq!(rest.fraction, Some(dec!(0.7)));
        assert!(rest.done);
        assert_eq!(rest.banked_usdt, dec!(1.0));
        let after = observe_at(&mut slicer, &item, &deep, later + Duration::from_secs(20));
        assert!(after.iter().all(|row| row.kind == Kind::Check));
        let mut rows = Vec::new();
        slicer.finish(&[], Utc::now(), &mut rows);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            (rows[0].kind, rows[0].slices, rows[0].done),
            (Kind::End, 2, true)
        );
        assert!(slicer.states.is_empty());
    }

    /// 补仓之后按新的数量算剩余比例：已切 3 个，持仓补到 20 个，还剩 85%。
    #[test]
    fn after_an_add_the_remaining_share_is_recomputed_from_quantities() {
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let start = Instant::now();
        observe_at(&mut slicer, &item, &books(dec!(3)), start);
        let bigger = watched(dec!(0.25), dec!(2000), Some(Decimal::ZERO));
        // 第一档 1000 个：剩下的 17 个（85%）整段平掉。
        let rows = observe_at(
            &mut slicer,
            &bigger,
            &books(dec!(1000)),
            start + CONSUMED_FOR + Duration::from_secs(1),
        );
        let rest = rows
            .iter()
            .find(|row| row.kind == Kind::Rest)
            .expect("rest");
        assert_eq!(rest.fraction, Some(dec!(0.85)));
        assert_eq!(rest.closed_long_qty, dec!(20));
    }

    /// 重启读回：接着上次的累计；名单空（规则轮还没跑）不收尾，名单里没有它才收尾。已收尾的不再读回。
    #[tokio::test]
    async fn state_is_restored_after_a_restart() {
        let dir = std::env::temp_dir().join(format!("arb-exit-slice-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let mut slicer = Slicer::default();
        let item = watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO));
        let rows = observe_at(&mut slicer, &item, &books(dec!(3)), Instant::now());
        let now = Utc::now();
        append(&dir, &rows, now).await;
        let mut restored = Slicer::restore(&dir, now);
        let state = &restored.states[&("p".to_string(), Track::Actual)];
        assert_eq!((state.closed_long_qty, state.slices), (dec!(3), 1));
        let mut ended = Vec::new();
        restored.finish(&[], now, &mut ended);
        assert!(ended.is_empty(), "名单空：不收尾");
        let other = Watched {
            position: PairPosition {
                id: "q".into(),
                ..position(dec!(0.25), dec!(1000))
            },
            ..watched(dec!(0.25), dec!(1000), Some(Decimal::ZERO))
        };
        restored.finish(std::slice::from_ref(&other), now, &mut ended);
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].kind, ended[0].slices), (Kind::End, 1));
        append(&dir, &ended, now).await;
        assert!(
            Slicer::restore(&dir, now).states.is_empty(),
            "收尾过的不再读回"
        );
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    async fn append(dir: &Path, rows: &[SliceRow], now: DateTime<Utc>) {
        tokio::fs::create_dir_all(dir).await.unwrap();
        let path = super::super::history::file_for(dir, SLICE_PREFIX, now.date_naive());
        let mut text = tokio::fs::read_to_string(&path).await.unwrap_or_default();
        for row in rows {
            text.push_str(&serde_json::to_string(row).unwrap());
            text.push('\n');
        }
        tokio::fs::write(&path, text).await.unwrap();
    }
}
