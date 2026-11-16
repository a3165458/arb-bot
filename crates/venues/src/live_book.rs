//! 进程内的 Hyperliquid 实时顶档（BBO）：行情 WebSocket 写，下单报价与开仓前的深度核算读。
//!
//! **不用 `l2Book`**：默认 `l2Book` 在东京实测约 12 秒才推一份，太慢。这里只用 `bbo` 频道 —— 买一 / 卖一变化
//! 就推（BTC 约 115ms 一条）。代价是只有一档：下单报价只需要对手方顶档，所以不受影响；按金额估算吃单均价的
//! 深度核算只能看到这一档，顶档放不下这笔金额就是「深度不够」，不会拿猜的深度凑数。
//!
//! # 新鲜度看连接心跳，不看「多久没推送」
//!
//! `bbo` 只在顶档**变化**时推送，而且订阅之后不会立刻推当前值（东京实测首条 p90 约 8 秒、最长近 20 秒，
//! 各市场最长静默 25 秒）。所以安静的市场长时间没有推送是正常的、顶档没变；能不能信它，取决于这条行情连接
//! 还活着：写入方每收到该场所连接上的任何一条消息（含 5 秒一次的 `pong`）就调用 [`heartbeat`]，
//! 断线时调用 [`remove_venue`] 把这家场所的全部顶档作废。读取方给 `max_age`，指的是**心跳**的新旧。
//!
//! 订阅之后用一次 WebSocket `post`（info `l2Book`，约 4ms）取每个市场的当前顶档做初值（[`set_top`]），
//! 之后全靠 `bbo` 推送；初值只取最优一档，不拿它的深度。
//!
//! 可信条件（缺一不可，读取方一律退回 REST）：有记录、心跳不超过 `max_age`、买一 < 卖一、两侧都有挂单。

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};

struct Entry {
    /// 买一 / 卖一：(价格, 标的数量)。`None` = 该侧没有挂单。
    bid: Option<(Decimal, Decimal)>,
    ask: Option<(Decimal, Decimal)>,
    /// 这一份的交易所时间戳（毫秒）。
    time: Option<i64>,
}

/// 最多放这么多个市场；超了不再新增（读取方照常退回 REST）。
const MAX_MARKETS: usize = 2048;

static BOOKS: LazyLock<RwLock<HashMap<(Venue, String), Entry>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static HEARTBEATS: LazyLock<RwLock<HashMap<Venue, Instant>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 测试用：这份缓存是进程内全局的，碰它的测试（心跳、`remove_venue`）并行时会互相清掉对方的数据，
/// 要串行。不属于业务接口。
#[doc(hidden)]
pub fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 这家场所的行情连接在 `at` 还活着（收到了一条消息）。
pub fn heartbeat(venue: Venue, at: Instant) {
    HEARTBEATS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(venue, at);
}

/// 写入一个市场当前的买一 / 卖一（`bbo` 推送或订阅后的初值）。**整体替换**：没有更深的档位，
/// 旧顶档被吃掉后留下的不会冒充第二档。
///
/// `time` 是交易所给的时间戳（毫秒）：比已有的更旧就丢弃并返回 `false`。订阅之后的初值走 info 查询，
/// 可能比先到的 `bbo` 推送还旧，不能把新顶档盖掉；之后市场安静下来，旧顶档就会被一直信任。
pub fn set_top(
    venue: Venue,
    base: &str,
    bid: Option<(Decimal, Decimal)>,
    ask: Option<(Decimal, Decimal)>,
    time: Option<i64>,
) -> bool {
    let positive = |level: Option<(Decimal, Decimal)>| {
        level.filter(|(price, size)| *price > Decimal::ZERO && *size > Decimal::ZERO)
    };
    let mut books = BOOKS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let key = (venue, base.to_string());
    match books.get(&key) {
        Some(old) if matches!((time, old.time), (Some(new), Some(old)) if new < old) => {
            return false;
        }
        None if books.len() >= MAX_MARKETS => return false,
        _ => {}
    }
    books.insert(
        key,
        Entry {
            bid: positive(bid),
            ask: positive(ask),
            time,
        },
    );
    true
}

/// 行情断线：这家场所的全部顶档与心跳作废。
pub fn remove_venue(venue: Venue) {
    BOOKS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|(v, _), _| *v != venue);
    HEARTBEATS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&venue);
}

fn alive(venue: Venue, max_age: Duration) -> bool {
    HEARTBEATS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&venue)
        .is_some_and(|at| at.elapsed() <= max_age)
}

/// 满足可信条件的买一 / 卖一：((买价, 量), (卖价, 量))。
pub fn top(
    venue: Venue,
    base: &str,
    max_age: Duration,
) -> Option<((Decimal, Decimal), (Decimal, Decimal))> {
    if !alive(venue, max_age) {
        return None;
    }
    let books = BOOKS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = books.get(&(venue, base.to_string()))?;
    let (bid, ask) = (entry.bid?, entry.ask?);
    (bid.0 < ask.0).then_some((bid, ask))
}

/// 一本可信的盘口：(买盘, 卖盘)，每侧只有顶档这一档 (价格, 标的数量)。
pub type Levels = (Vec<(Decimal, Decimal)>, Vec<(Decimal, Decimal)>);

/// 同 [`top`]，以档位列表的形状给出（每侧一档），供深度核算用。
pub fn fresh(venue: Venue, base: &str, max_age: Duration) -> Option<Levels> {
    let (bid, ask) = top(venue, base, max_age)?;
    Some((vec![bid], vec![ask]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const V: Venue = Venue::HyperliquidXyz;
    const AGE: Duration = Duration::from_secs(15);

    #[test]
    fn a_top_is_only_trusted_while_the_connection_heartbeat_is_fresh() {
        let _guard = test_lock();
        set_top(
            V,
            "LB-HB",
            Some((dec!(100), dec!(4))),
            Some((dec!(100.5), dec!(3))),
            None,
        );
        // 没有心跳（连接从没报过活）：不可信。
        remove_heartbeat_only(V);
        assert!(top(V, "LB-HB", AGE).is_none());
        heartbeat(V, Instant::now());
        let (bid, ask) = top(V, "LB-HB", AGE).unwrap();
        assert_eq!((bid.0, ask.0), (dec!(100), dec!(100.5)));
        // 安静的市场：条目本身多久没更新都无所谓，只要连接还活着。
        heartbeat(V, Instant::now());
        assert!(top(V, "LB-HB", AGE).is_some());
        // 心跳停了（TCP 静默断开）：不可信。
        heartbeat(V, Instant::now() - Duration::from_secs(30));
        assert!(top(V, "LB-HB", AGE).is_none());
        remove_venue(V);
    }

    #[test]
    fn a_new_top_replaces_the_old_one_and_never_leaves_phantom_depth() {
        let _guard = test_lock();
        heartbeat(V, Instant::now());
        set_top(
            V,
            "LB-REPL",
            Some((dec!(100), dec!(4))),
            Some((dec!(100.5), dec!(3))),
            None,
        );
        // 买一 100 被吃光，新买一 99.5：旧的 100 不能留成第二档。
        set_top(
            V,
            "LB-REPL",
            Some((dec!(99.5), dec!(2))),
            Some((dec!(100.5), dec!(3))),
            None,
        );
        let (bids, asks) = fresh(V, "LB-REPL", AGE).unwrap();
        assert_eq!(bids, vec![(dec!(99.5), dec!(2))]);
        assert_eq!(asks, vec![(dec!(100.5), dec!(3))]);
        remove_venue(V);
    }

    #[test]
    fn an_older_reading_never_overwrites_a_newer_one() {
        let _guard = test_lock();
        heartbeat(V, Instant::now());
        // bbo 推送（时间 2000）先到，初值查询（时间 1900，读节点落后）后到：不能盖掉。
        assert!(set_top(
            V,
            "LB-TIME",
            Some((dec!(100), dec!(1))),
            Some((dec!(101), dec!(1))),
            Some(2000)
        ));
        assert!(!set_top(
            V,
            "LB-TIME",
            Some((dec!(90), dec!(1))),
            Some((dec!(91), dec!(1))),
            Some(1900)
        ));
        let (bid, ask) = top(V, "LB-TIME", AGE).unwrap();
        assert_eq!((bid.0, ask.0), (dec!(100), dec!(101)));
        // 同一时间戳、更新的一条照收；没有时间戳的一律照收。
        assert!(set_top(
            V,
            "LB-TIME",
            Some((dec!(100.1), dec!(1))),
            Some((dec!(101), dec!(1))),
            Some(2000)
        ));
        assert!(set_top(
            V,
            "LB-TIME",
            Some((dec!(100.2), dec!(1))),
            Some((dec!(101), dec!(1))),
            None
        ));
        remove_venue(V);
    }

    #[test]
    fn one_sided_crossed_or_unknown_markets_are_not_served() {
        let _guard = test_lock();
        heartbeat(V, Instant::now());
        set_top(V, "LB-ONE", Some((dec!(1), dec!(1))), None, None);
        assert!(top(V, "LB-ONE", AGE).is_none());
        set_top(
            V,
            "LB-CROSS",
            Some((dec!(102), dec!(1))),
            Some((dec!(101), dec!(1))),
            None,
        );
        assert!(top(V, "LB-CROSS", AGE).is_none());
        assert!(top(V, "LB-NEVER", AGE).is_none());
        // 零价 / 零量的档当作没有。
        set_top(
            V,
            "LB-ZERO",
            Some((dec!(0), dec!(1))),
            Some((dec!(2), dec!(1))),
            None,
        );
        assert!(top(V, "LB-ZERO", AGE).is_none());
        remove_venue(V);
    }

    #[test]
    fn a_disconnect_withdraws_that_venue_only() {
        let _guard = test_lock();
        for venue in [Venue::HyperliquidIo, Venue::Hyperliquid] {
            heartbeat(venue, Instant::now());
            set_top(
                venue,
                "LB-DISC",
                Some((dec!(1), dec!(1))),
                Some((dec!(2), dec!(1))),
                None,
            );
        }
        remove_venue(Venue::HyperliquidIo);
        assert!(top(Venue::HyperliquidIo, "LB-DISC", AGE).is_none());
        assert!(top(Venue::Hyperliquid, "LB-DISC", AGE).is_some());
        remove_venue(Venue::Hyperliquid);
    }

    fn remove_heartbeat_only(venue: Venue) {
        HEARTBEATS
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&venue);
    }
}
