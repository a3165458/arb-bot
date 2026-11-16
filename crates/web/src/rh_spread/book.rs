//! 本地盘口与按数量吃单的报价。
//!
//! 两家都是**标的数量**（不是名义）：Lighter `size`、Arcus `[价格, 数量]`。

use std::collections::BTreeMap;
use std::time::Instant;

use rust_decimal::Decimal;
use serde::Serialize;

/// 一个市场的本地盘口。价格 → 数量。
#[derive(Debug, Clone)]
pub struct LocalBook {
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    /// 最近一次收到这本盘口的数据（快照或增量）。
    pub updated: Instant,
    /// Producer time where supplied (currently Hyperliquid), never a local apply timestamp.
    pub source_time_ms: Option<i64>,
    /// Lighter 增量的连续性：上一条的 `nonce`。
    pub nonce: Option<i64>,
    /// 最近一次整本快照的生产时间（Hyperliquid）。之后只用 [`super::feed::Event::Top`] 刷了顶部时，
    /// 它比 `source_time_ms` 旧：深处的档位就是那时的。
    pub depth_time_ms: Option<i64>,
}

impl LocalBook {
    pub fn new(now: Instant) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            updated: now,
            source_time_ms: None,
            nonce: None,
            depth_time_ms: None,
        }
    }

    /// 只有顶档的盘口（Hyperliquid `bbo` 模式，不用 `l2Book`）：整体替换买卖两侧。没有更深的档位，
    /// 旧顶档被吃掉后留下的不能冒充第二档。`None` = 该侧没有挂单。
    pub fn set_top(&mut self, bid: Option<(Decimal, Decimal)>, ask: Option<(Decimal, Decimal)>) {
        self.bids.clear();
        self.asks.clear();
        if let Some((price, size)) = bid {
            Self::apply(&mut self.bids, price, size);
        }
        if let Some((price, size)) = ask {
            Self::apply(&mut self.asks, price, size);
        }
    }

    /// 应用一档：数量 0 = 删除这一档；同一帧里同价出现多次时后写的生效。
    pub fn apply(side: &mut BTreeMap<Decimal, Decimal>, price: Decimal, size: Decimal) {
        if price <= Decimal::ZERO {
            return;
        }
        if size <= Decimal::ZERO {
            side.remove(&price);
        } else {
            side.insert(price, size);
        }
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }

    /// 两边都有、且没有交叉（买一 < 卖一）才是能用的盘口。
    pub fn top(&self) -> Option<(Decimal, Decimal)> {
        let (bid, ask) = (self.best_bid()?, self.best_ask()?);
        (bid < ask).then_some((bid, ask))
    }

    pub fn mid(&self) -> Option<Decimal> {
        self.top().map(|(bid, ask)| (bid + ask) / Decimal::TWO)
    }

    /// 剔掉成交价限制带（Arcus 盘后与周末）之外的档位后的盘口：那些挂单交易所不让成交。
    /// 没有档位在带外时为 `None`（直接用原盘口，免得每次都复制）。
    pub fn within(&self, bounds: arb_venues::arcus::TradingBounds) -> Option<LocalBook> {
        let inside = |side: &BTreeMap<Decimal, Decimal>| {
            side.keys().next().is_none_or(|low| bounds.contains(*low))
                && side
                    .keys()
                    .next_back()
                    .is_none_or(|high| bounds.contains(*high))
        };
        if inside(&self.bids) && inside(&self.asks) {
            return None;
        }
        let mut book = self.clone();
        book.bids.retain(|price, _| bounds.contains(*price));
        book.asks.retain(|price, _| bounds.contains(*price));
        Some(book)
    }

    /// 市价**买**这么多个的均价（吃卖盘）。深度不够为 `None`。
    pub fn buy_avg(&self, quantity: Decimal) -> Option<Decimal> {
        walk(self.asks.iter().map(|(p, q)| (*p, *q)), quantity)
    }

    /// 市价**卖**这么多个的均价（吃买盘）。深度不够为 `None`。
    pub fn sell_avg(&self, quantity: Decimal) -> Option<Decimal> {
        walk(self.bids.iter().rev().map(|(p, q)| (*p, *q)), quantity)
    }
}

fn walk(levels: impl Iterator<Item = (Decimal, Decimal)>, quantity: Decimal) -> Option<Decimal> {
    if quantity <= Decimal::ZERO {
        return None;
    }
    let (mut left, mut cost) = (quantity, Decimal::ZERO);
    for (price, size) in levels {
        let take = left.min(size);
        cost += take * price;
        left -= take;
        if left <= Decimal::ZERO {
            return Some(cost / quantity);
        }
    }
    None
}

/// 一个方向（多 `long` 场所、空 `short` 场所）按给定名义现在的可成交情况。百分比都是相对两家中间价均值。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DirectionQuote {
    /// 开仓可成交价差（%）：空腿卖出均价 − 多腿买入均价。已含开仓穿价。
    pub entry_pct: Decimal,
    /// 立即平仓要付的穿价（%）：两腿相对各自中间价的吃单损失。
    pub exit_cross_pct: Decimal,
}

/// 按名义 `size_usdt` 估算「多 `long`、空 `short`」的可成交价差与平仓穿价。任一边深度不够为 `None`。
pub fn quote(long: &LocalBook, short: &LocalBook, size_usdt: Decimal) -> Option<DirectionQuote> {
    let (long_mid, short_mid) = (long.mid()?, short.mid()?);
    let reference = (long_mid + short_mid) / Decimal::TWO;
    if reference <= Decimal::ZERO || size_usdt <= Decimal::ZERO {
        return None;
    }
    let quantity = size_usdt / reference;
    let long_entry = long.buy_avg(quantity)?;
    let short_entry = short.sell_avg(quantity)?;
    let long_exit = long.sell_avg(quantity)?;
    let short_exit = short.buy_avg(quantity)?;
    let pct = |value: Decimal| (value / reference * Decimal::ONE_HUNDRED).round_dp(5);
    Some(DirectionQuote {
        entry_pct: pct(short_entry - long_entry),
        exit_cross_pct: pct((long_mid - long_exit) + (short_exit - short_mid)),
    })
}

/// 两个方向一起估（多 a 空 b、多 b 空 a）：两个方向用的是同样四个吃单均价（a 买 / a 卖 / b 买 / b 卖）和同一个
/// 参考价，各算一遍 [`quote`] 等于每本盘口多走一倍。结果与分别调 [`quote`] 完全一样（四个均价缺一个，两个方向都
/// 没有报价）。
pub fn quote_both(
    a: &LocalBook,
    b: &LocalBook,
    size_usdt: Decimal,
) -> Option<(DirectionQuote, DirectionQuote)> {
    let (a_mid, b_mid) = (a.mid()?, b.mid()?);
    let reference = (a_mid + b_mid) / Decimal::TWO;
    if reference <= Decimal::ZERO || size_usdt <= Decimal::ZERO {
        return None;
    }
    let quantity = size_usdt / reference;
    let a_buy = a.buy_avg(quantity)?;
    let b_sell = b.sell_avg(quantity)?;
    let a_sell = a.sell_avg(quantity)?;
    let b_buy = b.buy_avg(quantity)?;
    let pct = |value: Decimal| (value / reference * Decimal::ONE_HUNDRED).round_dp(5);
    Some((
        DirectionQuote {
            entry_pct: pct(b_sell - a_buy),
            exit_cross_pct: pct((a_mid - a_sell) + (b_buy - b_mid)),
        },
        DirectionQuote {
            entry_pct: pct(a_sell - b_buy),
            exit_cross_pct: pct((b_mid - b_sell) + (a_buy - a_mid)),
        },
    ))
}

/// 两家中间价的有符号基差（%）：(a − b) / 均值。最早那组 a = Arcus、b = Lighter RH。
pub fn mid_basis_pct(a: &LocalBook, b: &LocalBook) -> Option<Decimal> {
    let (a, b) = (a.mid()?, b.mid()?);
    let reference = (a + b) / Decimal::TWO;
    (reference > Decimal::ZERO).then(|| ((a - b) / reference * Decimal::ONE_HUNDRED).round_dp(5))
}
