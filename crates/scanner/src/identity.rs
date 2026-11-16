//! 合约身份判定：把「同一个 ticker」拆成「同一个资产」。
//!
//! `base`/`quote` 相等**不足以**判定两个读数可以互相配对。不同场所会复用同一个
//! ticker 指向完全不同的资产（改名币、分叉币、同名小币），把它们配成一对会得到
//! 一个看起来很大、实际无法对冲的「价差」。
//!
//! 判据是参考价：同一资产在不同场所的价格应当接近。指数价优先于标记价 ——
//! 指数价是资产本身的价格，标记价掺了本场所的资金费预期，用标记价判定会把
//! 「同一资产在不同场所的正常价差」误判成不同资产。

use std::collections::HashMap;

use arb_core::{Decimal, MarketSnapshot, Symbol, Venue, family_display_quote, settlement_family};

/// 参考价相差多少以内视为同一资产。
///
/// 5% 是个很松的门槛：真实的跨场所基差通常在 0.1% 以内，而「同名不同资产」的
/// 价格差异几乎总是数倍。松门槛的好处是不会把同一资产拆开（那会丢掉机会），
/// 代价是可能把两个真实价格相差 3% 的资产当成同一个 —— 这种情形下价差本身
/// 也不足以支撑套利，且入场基差会如实暴露出来。
pub const PRICE_TOLERANCE: Decimal = Decimal::from_parts(5, 0, 0, false, 2);

/// 一个身份已核实、可配对的合约簇。
#[derive(Debug, Clone)]
pub struct Cluster {
    pub symbol: Symbol,
    pub rates: Vec<MarketSnapshot>,
}

/// 身份无法核实、因此不参与配对的读数。
#[derive(Debug, Clone)]
pub struct Unverified {
    pub rate: MarketSnapshot,
    /// 为什么无法核实。要展示给用户看，不能只说「被排除了」。
    pub reason: &'static str,
}

/// 分簇结果。
#[derive(Debug, Clone, Default)]
pub struct Clustering {
    /// 可参与配对的簇。
    pub clusters: Vec<Cluster>,
    /// 身份无法核实的读数。
    ///
    /// 它们**不参与配对**，但必须被报出去 —— 静默消失会让用户以为「这家没这个合约」。
    pub unverified: Vec<Unverified>,
}

/// 缺参考价，无法判定它与其它场所是不是同一个资产。
pub const REASON_NO_REFERENCE_PRICE: &str = "缺少参考价，无法核实合约身份";

/// 标记价与同资产其它场所偏离过大。
///
/// 分簇用参考价（指数价优先），入场基差用标记价 —— 两者混用会出现：某家的指数价
/// 正常但标记价坏掉，它通过了身份判定，却带着一个**假的基差**参与配对。这一条
/// 让「通过身份判定」的配对，基差也是可信的。
pub const REASON_MARK_OUTLIER: &str = "标记价与同资产其它场所偏离过大，基差不可信";

/// 按 (base, 结算资产族) 分组，再按参考价分簇。
///
/// 分组用**结算资产族**而不是原始 quote：USDT 与 USDC 结算的合约可以互相配对
/// （见 [`arb_core::settlement_family`]），否则 USDC 场所会被静默排除。
/// 跨族的配对会在 [`crate::rank::Opportunity::quote_mismatch`] 上打标记。
///
/// 少于 `min_venues` 家场所的簇直接丢弃：双腿套利至少需要两家。
pub fn cluster(rates: Vec<MarketSnapshot>, min_venues: usize) -> Clustering {
    let mut by_pair: HashMap<(String, String), Vec<MarketSnapshot>> = HashMap::new();
    for rate in rates {
        let family = settlement_family(&rate.symbol.quote).to_string();
        by_pair
            .entry((rate.symbol.base.clone(), family))
            .or_default()
            .push(rate);
    }

    // HashMap 的迭代顺序在每次运行间都不同，而簇的顺序会影响下游展示。
    // 排序后处理，保证同一份输入产出同一份输出。
    let mut pairs: Vec<(String, String)> = by_pair.keys().cloned().collect();
    pairs.sort();

    let mut result = Clustering::default();
    for key in pairs {
        let group = by_pair.remove(&key).unwrap_or_default();
        let (base, family) = key;
        // 显示名用族的主流计价资产；每条读数的真实 `Symbol` 保持原样。
        let symbol = Symbol::perp(&base, family_display_quote(&family));

        let (priced, unpriced): (Vec<MarketSnapshot>, Vec<MarketSnapshot>) = group
            .into_iter()
            .partition(|rate| reference_price(rate).is_some());

        // 整组都没有参考价时不存在「同名不同资产」的歧义 —— 只有一种可能的身份。
        // 此时把它们全部视为已核实，否则这些场所会因为「拿不到价格」被静默排除。
        if priced.is_empty() {
            push_cluster(&mut result, symbol, unpriced, min_venues);
            continue;
        }

        let buckets = split_by_price(priced);
        let unambiguous = buckets.len() == 1;
        for bucket in buckets {
            push_cluster(&mut result, symbol.clone(), bucket, min_venues);
        }
        // 缺价格的读数：只有这一组**只有一个**簇时才敢归入其中。存在多个簇说明
        // 「同名不同资产」确实发生了，此时归入任何一簇都是猜测 —— 报出去，不配对。
        // 用下标而不是可变引用：`push_cluster` 可能因为场所数不够把那个簇丢掉，
        // 此时只能如实报成「无法核实」。
        let target = if unambiguous {
            result
                .clusters
                .iter()
                .rposition(|cluster| cluster.symbol == symbol)
        } else {
            None
        };
        match target {
            Some(index) => {
                let cluster = &mut result.clusters[index];
                cluster.rates.extend(unpriced);
                cluster.rates.sort_by_key(|rate| rate.venue);
            }
            None => result.unverified.extend(
                unpriced
                    .into_iter()
                    .map(|rate| Unverified {
                        rate,
                        reason: REASON_NO_REFERENCE_PRICE,
                    })
                    .collect::<Vec<_>>(),
            ),
        }
    }
    result
}

fn push_cluster(
    result: &mut Clustering,
    symbol: Symbol,
    rates: Vec<MarketSnapshot>,
    min_venues: usize,
) {
    let mut rates = rates;
    rates.sort_by_key(|rate| rate.venue);
    prune_mark_outliers(&mut rates, &mut result.unverified);
    if rates.len() >= min_venues {
        result.clusters.push(Cluster { symbol, rates });
    }
}

/// 剔除标记价与簇内其它场所严重不一致的成员。
///
/// 锚点用**中位数**而不是均值：坏掉的那一家不该把锚点一起拖走。
/// 只有双方都有标记价时才比较 —— 没有标记价说明本来就算不出基差，交给门槛去挡。
fn prune_mark_outliers(rates: &mut Vec<MarketSnapshot>, unverified: &mut Vec<Unverified>) {
    let mut marks: Vec<Decimal> = rates.iter().filter_map(mark_price).collect();
    if marks.len() < 2 {
        return;
    }
    marks.sort();
    let anchor = marks[marks.len() / 2];

    let mut kept = Vec::with_capacity(rates.len());
    for rate in rates.drain(..) {
        match mark_price(&rate) {
            Some(mark) if !within_tolerance(mark, anchor) => unverified.push(Unverified {
                rate,
                reason: REASON_MARK_OUTLIER,
            }),
            _ => kept.push(rate),
        }
    }
    *rates = kept;
}

/// 标记价：同样只接受正数。
fn mark_price(rate: &MarketSnapshot) -> Option<Decimal> {
    rate.mark_price.filter(|price| *price > Decimal::ZERO)
}

/// 已在台账里配成一对的两条腿（给这笔仓位补仓）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedPair {
    pub base: String,
    pub long: Venue,
    pub short: Venue,
}

/// 已在台账里配成一对的两腿之间，参考价、标记价彼此相差多少以内仍认作同一资产。
///
/// 比分簇的 [`PRICE_TOLERANCE`] 宽：这两腿开仓时已经过身份判定，「同名不同资产」的价格差
/// 几乎总是数倍；这里要挡的只是读数坏掉。2026-11-03 用户定为 10%（某合约 的 HL-io 溢价
/// 一小时内从 4.57% 拉到 5.25%，5% 把补仓挡掉了）。
pub const PINNED_TOLERANCE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);

/// [`pin_pair`] 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pinned {
    /// 两腿本来就在同一个簇里，不用补。
    AlreadyTogether,
    /// 另加了一个只含这两腿的簇。
    Added,
    /// 没加：原因（要报给用户，说清楚差了多少）。
    Refused(String),
}

/// 补仓时身份只在两腿之间核对：这两腿开仓时已经过身份判定、在台账里配成了一对。
///
/// 贪心分簇看的是簇均值，别家的报价会把它拉偏：某次实盘 十家 CEX/DEX 均值 1634.5，
/// HL-io 1734 偏 6.1% 被单独拆出、丢掉，而它和这笔仓位的另一腿 lighter（1668）只差 3.96%，
/// 补仓被拒成「两家没有落在同一个合约身份下」。
///
/// 两腿参考价与标记价彼此都在 [`PINNED_TOLERANCE`] 以内时，另加一个只含这两腿的簇
/// （只含这两腿，不让别家借它配对）；缺读数、结算资产族不同、超过容差时不加并给出原因。
pub fn pin_pair(clustering: &mut Clustering, raw: &[MarketSnapshot], pin: &PinnedPair) -> Pinned {
    let together = clustering.clusters.iter().any(|cluster| {
        cluster.symbol.base.eq_ignore_ascii_case(&pin.base)
            && [pin.long, pin.short]
                .iter()
                .all(|venue| cluster.rates.iter().any(|rate| rate.venue == *venue))
    });
    if together {
        return Pinned::AlreadyTogether;
    }
    if pin.long == pin.short {
        return Pinned::Refused("两腿是同一场所".into());
    }
    let leg = |venue: Venue| {
        raw.iter()
            .find(|rate| rate.venue == venue && rate.symbol.base.eq_ignore_ascii_case(&pin.base))
    };
    let (Some(long), Some(short)) = (leg(pin.long), leg(pin.short)) else {
        return Pinned::Refused("本轮扫描缺至少一条腿的读数".into());
    };
    let family = settlement_family(&long.symbol.quote);
    if family != settlement_family(&short.symbol.quote) {
        return Pinned::Refused(format!(
            "两腿结算资产不同（{} / {}）",
            long.symbol.quote, short.symbol.quote
        ));
    }
    let (Some(long_ref), Some(short_ref)) = (reference_price(long), reference_price(short)) else {
        return Pinned::Refused("缺参考价，无法核对两腿是不是同一资产".into());
    };
    let gap =
        |a: Decimal, b: Decimal| ((a - b).abs() / a.min(b) * Decimal::ONE_HUNDRED).round_dp(2);
    let limit = (PINNED_TOLERANCE * Decimal::ONE_HUNDRED).normalize();
    let ref_gap = gap(long_ref, short_ref);
    if ref_gap > limit {
        return Pinned::Refused(format!(
            "两腿参考价相差 {ref_gap}%（{} {long_ref} / {} {short_ref}），超过 {limit}%",
            pin.long, pin.short
        ));
    }
    if let (Some(long_mark), Some(short_mark)) = (mark_price(long), mark_price(short)) {
        let mark_gap = gap(long_mark, short_mark);
        if mark_gap > limit {
            return Pinned::Refused(format!(
                "两腿标记价相差 {mark_gap}%（{} {long_mark} / {} {short_mark}），超过 {limit}%",
                pin.long, pin.short
            ));
        }
    }
    let mut rates = vec![long.clone(), short.clone()];
    rates.sort_by_key(|rate| rate.venue);
    clustering.clusters.push(Cluster {
        symbol: Symbol::perp(&long.symbol.base, family_display_quote(family)),
        rates,
    });
    Pinned::Added
}

/// 按参考价贪心分簇：价格排序后，与当前簇的均值相差超过容差就另起一簇。
///
/// 用**均值**而不是首个成员作锚点：链式比较（a≈b、b≈c 但 a≉c）会让一个簇越滚越大，
/// 均值锚点会随成员增加而收敛。
fn split_by_price(mut rates: Vec<MarketSnapshot>) -> Vec<Vec<MarketSnapshot>> {
    rates.sort_by(|a, b| {
        reference_price(a)
            .cmp(&reference_price(b))
            .then_with(|| a.venue.cmp(&b.venue))
    });

    let mut buckets: Vec<Vec<MarketSnapshot>> = Vec::new();
    let mut sums: Vec<Decimal> = Vec::new();

    for rate in rates {
        let Some(price) = reference_price(&rate) else {
            continue;
        };
        match buckets.last().map(Vec::len) {
            Some(count) if count > 0 => {
                let idx = buckets.len() - 1;
                let mean = sums[idx] / Decimal::from(count as u32);
                if within_tolerance(price, mean) {
                    buckets[idx].push(rate);
                    sums[idx] += price;
                } else {
                    buckets.push(vec![rate]);
                    sums.push(price);
                }
            }
            _ => {
                buckets.push(vec![rate]);
                sums.push(price);
            }
        }
    }
    buckets
}

/// 参考价：只接受**正数**。0 或负数说明字段本身有问题，不能拿去判定身份。
fn reference_price(rate: &MarketSnapshot) -> Option<Decimal> {
    rate.reference_price()
        .filter(|price| *price > Decimal::ZERO)
}

fn within_tolerance(price: Decimal, anchor: Decimal) -> bool {
    if anchor <= Decimal::ZERO {
        return false;
    }
    ((price - anchor) / anchor).abs() <= PRICE_TOLERANCE
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Venue;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn rate(venue: Venue, base: &str, price: Option<Decimal>) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp(base, "USDT"),
            period_rate: dec!(0.0001),
            interval_h: 8,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: false,
            taker_fee: None,
            mark_price: price,
            index_price: price,
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

    #[test]
    fn same_ticker_with_far_apart_prices_becomes_two_clusters() {
        let rates = vec![
            rate(Venue::Binance, "AI", Some(dec!(2.0))),
            rate(Venue::Okx, "AI", Some(dec!(2.01))),
            rate(Venue::Gate, "AI", Some(dec!(0.05))),
            rate(Venue::Mexc, "AI", Some(dec!(0.051))),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 2, "同名不同资产必须拆开");
        assert!(out.unverified.is_empty());
        let sizes: Vec<usize> = out.clusters.iter().map(|c| c.rates.len()).collect();
        assert_eq!(sizes, vec![2, 2]);
    }

    #[test]
    fn small_normal_basis_stays_one_cluster() {
        let rates = vec![
            rate(Venue::Binance, "BTC", Some(dec!(100000))),
            rate(Venue::Okx, "BTC", Some(dec!(100120))),
            rate(Venue::Bybit, "BTC", Some(dec!(99950))),
        ];
        assert_eq!(cluster(rates, 2).clusters.len(), 1);
    }

    #[test]
    fn a_group_without_any_price_is_treated_as_one_identity() {
        let rates = vec![
            rate(Venue::Binance, "XYZ", None),
            rate(Venue::Okx, "XYZ", None),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 1);
        assert_eq!(out.clusters[0].rates.len(), 2);
        assert!(out.unverified.is_empty());
    }

    #[test]
    fn unpriced_venue_joins_the_only_cluster() {
        let rates = vec![
            rate(Venue::Binance, "BTC", Some(dec!(100000))),
            rate(Venue::Okx, "BTC", Some(dec!(100100))),
            rate(Venue::Lighter, "BTC", None),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 1);
        assert_eq!(out.clusters[0].rates.len(), 3, "唯一簇时可以直接归入");
        assert!(out.unverified.is_empty());
    }

    #[test]
    fn unpriced_venue_is_reported_not_guessed_when_ambiguous() {
        let rates = vec![
            rate(Venue::Binance, "AI", Some(dec!(2.0))),
            rate(Venue::Okx, "AI", Some(dec!(2.01))),
            rate(Venue::Gate, "AI", Some(dec!(0.05))),
            rate(Venue::Mexc, "AI", Some(dec!(0.051))),
            rate(Venue::Lighter, "AI", None),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 2);
        assert_eq!(out.unverified.len(), 1, "无法核实的读数必须报出去");
        assert_eq!(out.unverified[0].rate.venue, Venue::Lighter);
        assert_eq!(out.unverified[0].reason, REASON_NO_REFERENCE_PRICE);
        assert!(
            out.clusters
                .iter()
                .all(|c| c.rates.iter().all(|r| r.venue != Venue::Lighter)),
            "它绝不能参与配对"
        );
    }

    #[test]
    fn usdc_and_usdt_listings_of_the_same_asset_share_one_cluster() {
        // Variational 是 USDC 结算的。若按原始 quote 分组，它会永远配不上任何
        // USDT 场所 —— 不报错，只是一条机会都出不来。
        let mut usdc = rate(Venue::Variational, "BTC", Some(dec!(100010)));
        usdc.symbol = Symbol::perp("BTC", "USDC");
        let rates = vec![
            rate(Venue::Binance, "BTC", Some(dec!(100000))),
            rate(Venue::Okx, "BTC", Some(dec!(100100))),
            usdc,
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 1);
        assert_eq!(out.clusters[0].rates.len(), 3);
        // 显示名用族的主流计价资产
        assert_eq!(out.clusters[0].symbol.quote, "USDT");
        // 但每条读数的真实计价资产不能被改写
        assert!(
            out.clusters[0]
                .rates
                .iter()
                .any(|r| r.venue == Venue::Variational && r.symbol.quote == "USDC")
        );
    }

    #[test]
    fn unrelated_quotes_are_not_merged_into_one_family() {
        let mut other = rate(Venue::Binance, "BTC", Some(dec!(100000)));
        other.symbol = Symbol::perp("BTC", "BUSD");
        let rates = vec![
            other,
            rate(Venue::Okx, "BTC", Some(dec!(100000))),
            rate(Venue::Bybit, "BTC", Some(dec!(100000))),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 1);
        assert_eq!(out.clusters[0].rates.len(), 2, "BUSD 不属于 USDT/USDC 族");
    }

    #[test]
    fn a_broken_mark_price_is_excluded_even_when_the_index_looks_fine() {
        // 这一家的指数价正常（所以通过了身份判定），但标记价坏了 6%。
        // 分簇用参考价、基差用标记价 —— 不按标记价再筛一次，它就会带着一个
        // 假的基差参与配对。
        let mut broken = rate(Venue::Lighter, "BTC", Some(dec!(100000)));
        broken.mark_price = Some(dec!(94000));
        let rates = vec![
            rate(Venue::Binance, "BTC", Some(dec!(100000))),
            rate(Venue::Okx, "BTC", Some(dec!(100100))),
            broken,
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters.len(), 1);
        assert_eq!(out.clusters[0].rates.len(), 2, "坏掉的那一家不参与配对");
        assert_eq!(out.unverified.len(), 1);
        assert_eq!(out.unverified[0].rate.venue, Venue::Lighter);
        assert_eq!(out.unverified[0].reason, REASON_MARK_OUTLIER);
    }

    #[test]
    fn normal_mark_dispersion_is_not_pruned() {
        let rates = vec![
            rate(Venue::Binance, "BTC", Some(dec!(100000))),
            rate(Venue::Okx, "BTC", Some(dec!(100100))),
            rate(Venue::Bybit, "BTC", Some(dec!(99900))),
        ];
        let out = cluster(rates, 2);
        assert_eq!(out.clusters[0].rates.len(), 3);
        assert!(out.unverified.is_empty());
    }

    #[test]
    fn a_lone_mark_price_cannot_be_compared_so_it_is_kept() {
        // 只有一家有标记价时没有可比的锚点，不能凭它把别人排掉
        let mut only_mark = rate(Venue::Binance, "BTC", Some(dec!(100000)));
        only_mark.index_price = None;
        let mut other = rate(Venue::Okx, "BTC", Some(dec!(100000)));
        other.mark_price = None;
        other.index_price = Some(dec!(100000));
        let out = cluster(vec![only_mark, other], 2);
        assert_eq!(out.clusters[0].rates.len(), 2);
        assert!(out.unverified.is_empty());
    }

    #[test]
    fn clusters_below_min_venues_are_dropped() {
        let rates = vec![rate(Venue::Binance, "SOL", Some(dec!(150)))];
        assert!(cluster(rates, 2).clusters.is_empty());
    }

    #[test]
    fn output_order_is_deterministic() {
        let build = || {
            vec![
                rate(Venue::Okx, "ZZZ", Some(dec!(1))),
                rate(Venue::Binance, "AAA", Some(dec!(1))),
                rate(Venue::Bybit, "ZZZ", Some(dec!(1))),
                rate(Venue::Gate, "AAA", Some(dec!(1))),
            ]
        };
        let first: Vec<String> = cluster(build(), 2)
            .clusters
            .iter()
            .map(|c| c.symbol.to_string())
            .collect();
        for _ in 0..8 {
            let again: Vec<String> = cluster(build(), 2)
                .clusters
                .iter()
                .map(|c| c.symbol.to_string())
                .collect();
            assert_eq!(first, again);
        }
    }

    /// 某次实盘 的参考价：十家在 1612~1676，HL-io 1734。
    fn anthropic() -> Vec<MarketSnapshot> {
        [
            (Venue::Variational, dec!(1612.03)),
            (Venue::Aster, dec!(1613.86)),
            (Venue::Binance, dec!(1613.86)),
            (Venue::Mexc, dec!(1617.2)),
            (Venue::Ourbit, dec!(1626.7)),
            (Venue::Bitget, dec!(1627.42)),
            (Venue::Bybit, dec!(1639.94)),
            (Venue::Gate, dec!(1650.67)),
            (Venue::Lighter, dec!(1668.0)),
            (Venue::LighterRh, dec!(1675.6)),
            (Venue::HyperliquidIo, dec!(1734.0)),
        ]
        .into_iter()
        .map(|(venue, price)| rate(venue, "ANTHROPIC", Some(price)))
        .collect()
    }

    fn pin(long: Venue, short: Venue) -> PinnedPair {
        PinnedPair {
            base: "ANTHROPIC".into(),
            long,
            short,
        }
    }

    #[test]
    fn a_ledger_pair_split_by_other_venues_is_pinned_as_its_own_two_leg_cluster() {
        let raw = anthropic();
        let mut clustering = cluster(raw.clone(), 2);
        assert!(
            clustering
                .clusters
                .iter()
                .all(|c| c.rates.iter().all(|r| r.venue != Venue::HyperliquidIo)),
            "HL-io 被均值拉偏的簇拆出去、单家丢掉"
        );
        let before = clustering.clusters.len();
        assert_eq!(
            pin_pair(
                &mut clustering,
                &raw,
                &pin(Venue::Lighter, Venue::HyperliquidIo)
            ),
            Pinned::Added
        );
        assert_eq!(clustering.clusters.len(), before + 1);
        let pinned = clustering.clusters.last().unwrap();
        let venues: Vec<Venue> = pinned.rates.iter().map(|r| r.venue).collect();
        assert_eq!(venues, vec![Venue::HyperliquidIo, Venue::Lighter]);
        assert_eq!(pinned.symbol, Symbol::perp("ANTHROPIC", "USDT"));
    }

    #[test]
    fn a_pinned_pair_still_needs_its_two_legs_within_ten_percent_of_each_other() {
        let mut raw = anthropic();
        // 2026-11-03 04:40 的标记价：lighter 1660.3、HL-io 1747.4，相差 5.25%，10% 以内照样成对。
        for rate in &mut raw {
            match rate.venue {
                Venue::Lighter => rate.mark_price = Some(dec!(1660.3)),
                Venue::HyperliquidIo => rate.mark_price = Some(dec!(1747.4)),
                _ => {}
            }
        }
        let mut clustering = cluster(raw.clone(), 2);
        assert_eq!(
            pin_pair(
                &mut clustering,
                &raw,
                &pin(Venue::Lighter, Venue::HyperliquidIo)
            ),
            Pinned::Added
        );
        // HL-io 标记价拉到 1840：相差 10.82%，不成对，原因里写明差了多少。
        let mut far = raw.clone();
        for rate in &mut far {
            if rate.venue == Venue::HyperliquidIo {
                rate.mark_price = Some(dec!(1840));
            }
        }
        let mut clustering = cluster(far.clone(), 2);
        let before = clustering.clusters.len();
        match pin_pair(
            &mut clustering,
            &far,
            &pin(Venue::Lighter, Venue::HyperliquidIo),
        ) {
            Pinned::Refused(reason) => {
                assert!(reason.contains("标记价相差 10.82%"), "{reason}");
                assert!(reason.contains("超过 10%"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        // 本来就在同一个簇里：什么都不做。
        assert_eq!(
            pin_pair(&mut clustering, &far, &pin(Venue::Lighter, Venue::Binance)),
            Pinned::AlreadyTogether
        );
        // 缺一条腿的读数。
        assert!(matches!(
            pin_pair(
                &mut clustering,
                &far,
                &pin(Venue::Lighter, Venue::HyperliquidXyz)
            ),
            Pinned::Refused(_)
        ));
        assert_eq!(clustering.clusters.len(), before);
    }
}
