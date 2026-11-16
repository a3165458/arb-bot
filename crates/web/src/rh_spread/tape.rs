//! 各家盘口中间价的短历史：算「最近几秒价格动得多快」。
//!
//! 价差信号常常是一边先动、另一边的报价还没跟上：等第二腿到的时候，滞后的那边多半已经追上，
//! 价差没了，只剩第一腿裸着（某次实盘：Lighter 几秒内跌了 0.3%，HL 的买价滞后，
//! 第二腿两次 IOC 都吃不到，回滚亏了手续费和滑点）。这里只负责量「动了多少」，门槛在自动交易设置里。
//!
//! 按（场所, 市场名）存：同一家的盘口会出现在好几个组里，各组共用，不重复记。

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use arb_core::Venue;
use rust_decimal::Decimal;

use super::book::LocalBook;

/// 量价格波动的窗口：最近这么久。
pub const MOVE_WINDOW: Duration = Duration::from_secs(5);
/// 历史最多留这么久（窗口的两倍）。
const KEEP: Duration = Duration::from_secs(10);

/// [`PriceTape::range_until`] 的结果：（幅度, 在没有新样本的前提下这个结果到什么时候为止不会变）。
/// 第二项 `None` = 一直不变。
type Range = (Option<Decimal>, Option<Instant>);

struct Series {
    /// 这条记录从什么时候开始（窗口还没记满时不给结论）。
    since: Instant,
    /// 中间价变化的时刻与新价，时间升序。盘口没变就不写（Lighter 只推变化），所以按「阶梯」读：
    /// 每个样本的价一直有效到下一个样本。
    samples: VecDeque<(Instant, Decimal)>,
    /// 上一次 [`PriceTape::range_pct`] 的（时刻, 窗口, 结果）：同一轮里同一本盘口会被好几个组各问一遍。
    /// 记了新样本就作废。
    memo: std::cell::Cell<Option<(Instant, Duration, Range)>>,
}

#[derive(Default)]
pub struct PriceTape {
    series: HashMap<Venue, HashMap<String, Series>>,
}

impl PriceTape {
    /// 按当前所有盘口记一笔：中间价变了才写；已经没有盘口的（断线清空）整条删掉，重连后重新记。
    /// 盘口暂时是空的或交叉的（`mid()` 为 `None`）时不动这条记录。
    pub fn sync(&mut self, books: &HashMap<(Venue, String), LocalBook>, now: Instant) {
        for ((venue, key), book) in books {
            self.record(*venue, key, book, now);
        }
        self.series.retain(|venue, per_venue| {
            per_venue.retain(|key, _| books.contains_key(&(*venue, key.clone())));
            !per_venue.is_empty()
        });
    }

    /// 只记 `venue` 的这几本盘口（上一轮之后变过的那些）：效果和 [`Self::sync`] 对这几本做的一样 ——
    /// 盘口还在就记一笔，没了（增量断档删掉、等重订阅）就整条删掉。没变过的盘口中间价没变，本来也不会写样本，
    /// 所以两轮整份 `sync` 之间只同步变过的就够了，不必每 50ms 把两千多本盘口的中间价全算一遍。
    pub fn sync_keys<'a>(
        &mut self,
        books: &HashMap<(Venue, String), LocalBook>,
        venue: Venue,
        keys: impl IntoIterator<Item = &'a String>,
        now: Instant,
    ) {
        for key in keys {
            match books.get(&(venue, key.clone())) {
                Some(book) => self.record(venue, key, book, now),
                None => {
                    if let Some(per_venue) = self.series.get_mut(&venue) {
                        per_venue.remove(key);
                        if per_venue.is_empty() {
                            self.series.remove(&venue);
                        }
                    }
                }
            }
        }
    }

    fn record(&mut self, venue: Venue, key: &str, book: &LocalBook, now: Instant) {
        let Some(mid) = book.mid() else {
            return;
        };
        let per_venue = self.series.entry(venue).or_default();
        if !per_venue.contains_key(key) {
            per_venue.insert(
                key.to_string(),
                Series {
                    since: now,
                    samples: VecDeque::new(),
                    memo: std::cell::Cell::new(None),
                },
            );
        }
        let series = per_venue.get_mut(key).expect("刚插入过");
        if series.samples.back().map(|(_, price)| *price) != Some(mid) {
            series.samples.push_back((now, mid));
            series.memo.set(None);
        }
        // 留一个窗口起点之前的样本（它的价一直有效到窗口里），更早的丢掉。
        if let Some(cutoff) = now.checked_sub(KEEP) {
            while series.samples.len() >= 2 && series.samples[1].0 <= cutoff {
                series.samples.pop_front();
            }
        }
    }

    /// 最近 `window` 内中间价的波动幅度（%）：（最高 − 最低）÷ 最低。窗口起点处的价、窗口里每次变化后的价、
    /// 当前价（最后一个样本，盘口没变时就是它）都算进去。
    ///
    /// `None` = 没有结论：这条记录还不满 `window`（刚启动、刚重连）。**调用方对 `None` 是放行**，
    /// 不是挡下 —— 没有数据不该让整个自动交易停摆。
    #[cfg(test)]
    pub fn range_pct(
        &self,
        venue: Venue,
        key: &str,
        window: Duration,
        now: Instant,
    ) -> Option<Decimal> {
        self.range_until(venue, key, window, now).0
    }

    /// [`Self::range_pct`]，外加「这个结果到什么时候为止不会变」：只要这本盘口没有新样本（中间价没变），
    /// 结果只会在两种时刻变 —— 记录刚满 `window`（`None` 变成有结论），或窗口里最老的那个样本滑出窗口。
    /// 调用方据此在盘口没变时不必每轮重查。`None` = 一直不变（窗口里没有样本，或还没有这条记录）。
    pub fn range_until(
        &self,
        venue: Venue,
        key: &str,
        window: Duration,
        now: Instant,
    ) -> (Option<Decimal>, Option<Instant>) {
        let Some(series) = self.series.get(&venue).and_then(|per| per.get(key)) else {
            return (None, None);
        };
        if let Some((at, asked, range)) = series.memo.get()
            && at == now
            && asked == window
        {
            return range;
        }
        let range = Self::range_of(series, window, now);
        series.memo.set(Some((now, window, range)));
        range
    }

    fn range_of(series: &Series, window: Duration, now: Instant) -> Range {
        if now.saturating_duration_since(series.since) < window {
            return (None, Some(series.since + window));
        }
        let Some(from) = now.checked_sub(window) else {
            return (None, None);
        };
        // 从最新的往回看：窗口里每次变化后的价，加上窗口起点处有效的价（最后一个不晚于起点的样本）。
        // 没有不晚于起点的样本（理论上不会，since 早于起点）时就只有窗口里的。
        let Some((_, last)) = series.samples.back() else {
            return (None, None);
        };
        let (mut low, mut high) = (*last, *last);
        // 窗口里最老的样本：它滑出窗口的那一刻（它的时刻 + 窗口）结果才可能变。
        let mut oldest_inside = None;
        for (at, price) in series.samples.iter().rev() {
            low = low.min(*price);
            high = high.max(*price);
            if *at <= from {
                break;
            }
            oldest_inside = Some(*at);
        }
        (
            (low > Decimal::ZERO).then(|| ((high - low) / low * Decimal::ONE_HUNDRED).round_dp(5)),
            oldest_inside.map(|at| at + window),
        )
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    fn book(bid: Decimal, ask: Decimal, now: Instant) -> LocalBook {
        let mut book = LocalBook::new(now);
        LocalBook::apply(&mut book.bids, bid, Decimal::ONE);
        LocalBook::apply(&mut book.asks, ask, Decimal::ONE);
        book
    }

    fn books(
        venue: Venue,
        key: &str,
        bid: Decimal,
        ask: Decimal,
        now: Instant,
    ) -> HashMap<(Venue, String), LocalBook> {
        [((venue, key.to_string()), book(bid, ask, now))].into()
    }

    /// 在 `t0 + 偏移` 记下一个中间价。
    fn feed(tape: &mut PriceTape, t0: Instant, ms: u64, bid: Decimal, ask: Decimal) {
        let at = t0 + Duration::from_millis(ms);
        tape.sync(&books(Venue::LighterRh, "SOXL", bid, ask, at), at);
    }

    fn range(tape: &PriceTape, t0: Instant, ms: u64) -> Option<Decimal> {
        tape.range_pct(
            Venue::LighterRh,
            "SOXL",
            MOVE_WINDOW,
            t0 + Duration::from_millis(ms),
        )
    }

    #[test]
    fn no_conclusion_until_the_window_is_filled() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        feed(&mut tape, t0, 0, dec!(128.8), dec!(128.9));
        feed(&mut tape, t0, 2_000, dec!(128.3), dec!(128.4));
        assert_eq!(range(&tape, t0, 4_900), None, "不满 5 秒不给结论");
        assert!(range(&tape, t0, 5_000).is_some());
        // 没见过的市场也是 None。
        assert_eq!(
            tape.range_pct(
                Venue::Arcus,
                "SOXL",
                MOVE_WINDOW,
                t0 + Duration::from_secs(9)
            ),
            None
        );
    }

    #[test]
    fn a_fast_drop_inside_the_window_shows_as_the_high_minus_the_low() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        // 中间价 128.85，4 秒后掉到 128.50（0.27%），之后一直不动。
        feed(&mut tape, t0, 0, dec!(128.80), dec!(128.90));
        feed(&mut tape, t0, 4_000, dec!(128.45), dec!(128.55));
        let moved = range(&tape, t0, 6_000).expect("窗口满了");
        // 窗口 [1s, 6s]：起点处是 128.85，4 秒后是 128.50。
        assert_eq!(
            moved,
            ((dec!(128.85) - dec!(128.50)) / dec!(128.50) * Decimal::ONE_HUNDRED).round_dp(5)
        );
        assert!(moved > dec!(0.27) && moved < dec!(0.28));
    }

    #[test]
    fn a_quiet_book_still_answers_and_old_moves_age_out() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        feed(&mut tape, t0, 0, dec!(100.0), dec!(100.2));
        feed(&mut tape, t0, 1_000, dec!(99.0), dec!(99.2));
        // 之后盘口一直没变（Lighter 只推变化）：同一价不再写样本，但窗口里仍然要有结论。
        feed(&mut tape, t0, 3_000, dec!(99.0), dec!(99.2));
        feed(&mut tape, t0, 5_000, dec!(99.0), dec!(99.2));
        // 窗口 [0, 5s]：起点 100.1，1 秒时掉到 99.1，约 1%。
        assert!(range(&tape, t0, 5_000).unwrap() > dec!(0.9));
        feed(&mut tape, t0, 6_000, dec!(99.0), dec!(99.2));
        assert_eq!(
            range(&tape, t0, 6_500),
            Some(Decimal::ZERO),
            "窗口 [1.5s, 6.5s] 里价没动"
        );
        // 老的下跌早出了窗口，历史里也只剩下起点那一个样本。
        feed(&mut tape, t0, 9_000, dec!(99.0), dec!(99.2));
        feed(&mut tape, t0, 12_000, dec!(99.0), dec!(99.2));
        assert_eq!(range(&tape, t0, 12_000), Some(Decimal::ZERO));
    }

    #[test]
    fn a_book_that_disappears_starts_over_when_it_comes_back() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        feed(&mut tape, t0, 0, dec!(100.0), dec!(100.2));
        feed(&mut tape, t0, 6_000, dec!(100.0), dec!(100.2));
        assert!(range(&tape, t0, 6_000).is_some());
        // 断线：盘口被清空。
        tape.sync(&HashMap::new(), t0 + Duration::from_millis(6_100));
        assert_eq!(range(&tape, t0, 6_200), None);
        // 重连后价格已经变了：这条记录从头记，不拿断线前的价算波动。
        feed(&mut tape, t0, 30_000, dec!(90.0), dec!(90.2));
        assert_eq!(range(&tape, t0, 30_100), None);
        assert_eq!(range(&tape, t0, 35_000), Some(Decimal::ZERO));
    }

    /// 只同步变过的那几本：和整份 `sync` 对它们做的一样（还在就记、没了就删），别的盘口的记录不动。
    #[test]
    fn syncing_only_the_changed_books_matches_a_full_sync() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let key = |name: &str| (Venue::Lighter, name.to_string());
        let mut all: HashMap<(Venue, String), LocalBook> = [
            (key("AAA"), book(dec!(99), dec!(101), t0)),
            (key("BBB"), book(dec!(49), dec!(51), t0)),
        ]
        .into();
        let (mut full, mut partial) = (PriceTape::default(), PriceTape::default());
        full.sync(&all, t0);
        partial.sync(&all, t0);
        // AAA 动了、BBB 没动。
        all.insert(key("AAA"), book(dec!(100), dec!(102), at(6000)));
        full.sync(&all, at(6000));
        partial.sync_keys(&all, Venue::Lighter, [&"AAA".to_string()], at(6000));
        for name in ["AAA", "BBB"] {
            assert_eq!(
                partial.range_pct(Venue::Lighter, name, MOVE_WINDOW, at(6500)),
                full.range_pct(Venue::Lighter, name, MOVE_WINDOW, at(6500)),
                "{name}"
            );
        }
        assert_eq!(
            partial.range_pct(Venue::Lighter, "AAA", MOVE_WINDOW, at(6500)),
            Some(dec!(1))
        );
        assert_eq!(
            partial.range_pct(Venue::Lighter, "BBB", MOVE_WINDOW, at(6500)),
            Some(Decimal::ZERO)
        );
        // 幅度是 0 的盘口之后一直没动：窗口往后滑还是 0（快路径靠这一点跳过没变的行）。
        assert_eq!(
            partial.range_pct(Venue::Lighter, "BBB", MOVE_WINDOW, at(60_000)),
            Some(Decimal::ZERO)
        );
        // AAA 的盘口没了（增量断档删掉）：这条记录删掉，BBB 的留着；回来后重新记。
        all.remove(&key("AAA"));
        partial.sync_keys(&all, Venue::Lighter, [&"AAA".to_string()], at(7000));
        assert_eq!(
            partial.range_pct(Venue::Lighter, "AAA", MOVE_WINDOW, at(7000)),
            None
        );
        assert_eq!(
            partial.range_pct(Venue::Lighter, "BBB", MOVE_WINDOW, at(7000)),
            Some(Decimal::ZERO)
        );
        all.insert(key("AAA"), book(dec!(100), dec!(102), at(8000)));
        partial.sync_keys(&all, Venue::Lighter, [&"AAA".to_string()], at(8000));
        assert_eq!(
            partial.range_pct(Venue::Lighter, "AAA", MOVE_WINDOW, at(9000)),
            None
        );
        // 最后一本也没了：这一家整个删掉。
        all.clear();
        partial.sync_keys(
            &all,
            Venue::Lighter,
            [&"AAA".to_string(), &"BBB".to_string()],
            at(9000),
        );
        assert!(partial.series.is_empty());
    }

    /// 「结果到什么时候为止不会变」：在那之前任何时刻重查都是同一个数，到了那一刻才可能变。
    #[test]
    fn the_range_only_changes_when_a_sample_slides_out_of_the_window() {
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let mut tape = PriceTape::default();
        feed(&mut tape, t0, 0, dec!(99), dec!(101));
        // 没有这条记录：没结论，也不会自己变。
        assert_eq!(
            tape.range_until(Venue::LighterRh, "nope", MOVE_WINDOW, at(100)),
            (None, None)
        );
        // 还没记满窗口：没结论，记满的那一刻（起点 + 窗口）再看。
        let (range, until) = tape.range_until(Venue::LighterRh, "SOXL", MOVE_WINDOW, at(1000));
        assert_eq!((range, until), (None, Some(at(5000))));
        // 6 秒、7 秒各动一次。
        feed(&mut tape, t0, 6000, dec!(100), dec!(102));
        feed(&mut tape, t0, 7000, dec!(101), dec!(103));
        let ask = |ms: u64| tape.range_until(Venue::LighterRh, "SOXL", MOVE_WINDOW, at(ms));
        let (range, until) = ask(7000);
        assert_eq!(range, Some(dec!(2)));
        // 窗口里最老的样本是 6 秒那个：11 秒滑出去之前结果不变，滑出去之后变小。
        assert_eq!(until, Some(at(11_000)));
        for ms in [7001, 9000, 10_999] {
            assert_eq!(ask(ms).0, range, "{ms}");
        }
        let (later, later_until) = ask(11_000);
        assert_eq!(later, Some(dec!(0.99010)));
        assert_eq!(later_until, Some(at(12_000)));
        // 都滑出去了：只剩当前价，幅度 0，之后不会再自己变。
        assert_eq!(ask(12_000), (Some(Decimal::ZERO), None));
        assert_eq!(ask(60_000), (Some(Decimal::ZERO), None));
    }

    #[test]
    fn a_crossed_or_empty_book_leaves_the_record_alone() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        feed(&mut tape, t0, 0, dec!(100.0), dec!(100.2));
        // 交叉盘口：mid 为 None，不写也不删。
        feed(&mut tape, t0, 2_000, dec!(100.5), dec!(100.2));
        feed(&mut tape, t0, 6_000, dec!(100.0), dec!(100.2));
        assert_eq!(range(&tape, t0, 6_000), Some(Decimal::ZERO));
    }

    #[test]
    fn history_does_not_grow_without_bound() {
        let t0 = Instant::now();
        let mut tape = PriceTape::default();
        for i in 0..400u64 {
            let bid = dec!(100) + Decimal::from(i % 7) / dec!(100);
            feed(&mut tape, t0, i * 100, bid, bid + dec!(0.2));
        }
        let kept = tape.series[&Venue::LighterRh]["SOXL"].samples.len();
        assert!(kept <= 105, "只留 10 秒（每 100ms 一个）: {kept}");
    }
}
