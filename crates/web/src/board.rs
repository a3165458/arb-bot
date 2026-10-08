//! 看板只下发**当前能看见的榜**，不下发整份扫描。
//!
//! 一次全量扫描大约是 1000 个合约、上万条配对，序列化后接近 20MB。页面每 30 秒
//! 拉一次，浏览器要下载、解析、再只画出前几十行。慢的是这份用不到的载荷，不是排名。
//!
//! 服务端继续留着全量快照（重排名、点开某一行的各场所读数都从这里来）。响应里只有：
//! 两条榜各自的前 N 行、这些行涉及到的合约读数、场所状态和少量提示。

use std::collections::{BTreeMap, BTreeSet};

use arb_core::{Decimal, MarketSnapshot, Symbol, Venue};
use arb_scanner::scan::RejectedPair;
use arb_scanner::{
    ExcludedReading, Opportunity, PairRisk, ScanReport, SymbolView, Totals, pair_risk,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// 页面「显示前 N 条」的上限，和看板下拉框一致。再大就又把整份扫描推回去了。
pub const MAX_TOP: usize = 300;
pub const DEFAULT_TOP: usize = 50;

#[derive(Debug, Serialize)]
pub struct BoardRow {
    pub symbol: Symbol,
    pub next_funding_at: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub opportunity: Opportunity,
    /// 按本次请求的杠杆算出的两腿风险。和排名无关，只在切出来的前 N 行上算。
    pub risk: Option<PairRisk>,
}

#[derive(Debug, Serialize)]
pub struct Board {
    pub fee_per_side: Decimal,
    pub amortize_days: Decimal,
    pub spread_hold_days: Decimal,
    pub max_entry_basis_pct: Option<Decimal>,
    /// 风险列用的杠杆。
    pub leverage: Decimal,
    pub min_venues: usize,
    pub venues: Vec<arb_scanner::VenueReport>,
    pub suspicious: Vec<ExcludedReading>,
    pub unverified: Vec<ExcludedReading>,
    pub gated: Vec<RejectedPair>,
    pub totals: Totals,
    pub funding: Vec<BoardRow>,
    pub spread: Vec<BoardRow>,
    /// 只含出现在上面两份榜里的合约。点开一行不用再为没上榜的几百个合约付钱。
    pub details: BTreeMap<String, Vec<MarketSnapshot>>,
    /// 本次请求做了半衰期拟合时，全榜（不是截断后的前 N 行）里有多少条配对用了实测值。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measured_pairs: Option<usize>,
}

pub fn parse_top(raw: Option<u32>) -> Result<usize, String> {
    match raw {
        None => Ok(DEFAULT_TOP),
        Some(value) if (1..=MAX_TOP as u32).contains(&value) => Ok(value as usize),
        Some(value) => Err(format!("top 必须在 1..={MAX_TOP}，收到 {value}")),
    }
}

/// 从全量报告切出看板。`bases` 为空表示不过滤。
pub fn project(
    report: &ScanReport,
    bases: &[String],
    top: usize,
    measured_pairs: Option<usize>,
    leverage: Decimal,
) -> Board {
    let top = top.min(MAX_TOP);
    let funding = top_rows(report, bases, top, true, leverage);
    let spread = top_rows(report, bases, top, false, leverage);

    let mut shown = BTreeSet::new();
    for row in funding.iter().chain(spread.iter()) {
        shown.insert(row.symbol.to_string());
    }
    let mut details = BTreeMap::new();
    for view in report
        .symbols
        .iter()
        .filter(|view| base_ok(&view.symbol, bases))
    {
        let key = view.symbol.to_string();
        if shown.contains(&key) {
            details.insert(key, view.rates.clone());
        }
    }

    let suspicious = filter_excluded(&report.suspicious, bases);
    let unverified = filter_excluded(&report.unverified, bases);
    let gated = report
        .gated
        .iter()
        .filter(|row| base_ok(&row.symbol, bases))
        .cloned()
        .collect::<Vec<_>>();

    Board {
        fee_per_side: report.fee_per_side,
        amortize_days: report.amortize_days,
        spread_hold_days: report.spread_hold_days,
        max_entry_basis_pct: report.max_entry_basis_pct,
        leverage,
        min_venues: report.min_venues,
        venues: report.venues.clone(),
        totals: totals_for(report, bases, &suspicious, &unverified, &gated),
        suspicious,
        unverified,
        gated,
        funding,
        spread,
        details,
        measured_pairs,
    }
}

fn top_rows(
    report: &ScanReport,
    bases: &[String],
    top: usize,
    funding: bool,
    leverage: Decimal,
) -> Vec<BoardRow> {
    let mut rows = Vec::new();
    for view in report
        .symbols
        .iter()
        .filter(|view| base_ok(&view.symbol, bases))
    {
        let list = if funding { &view.funding } else { &view.spread };
        for opportunity in list {
            rows.push(BoardRow {
                symbol: view.symbol.clone(),
                next_funding_at: next_funding(view, opportunity.short),
                opportunity: opportunity.clone(),
                risk: None,
            });
        }
    }
    rows.sort_by(|left, right| {
        let metric = if funding {
            right
                .opportunity
                .funding_apr
                .cmp(&left.opportunity.funding_apr)
        } else {
            right
                .opportunity
                .spread_net
                .cmp(&left.opportunity.spread_net)
        };
        metric
            .then_with(|| left.symbol.base.cmp(&right.symbol.base))
            .then_with(|| left.symbol.quote.cmp(&right.symbol.quote))
            .then_with(|| left.opportunity.long.cmp(&right.opportunity.long))
            .then_with(|| left.opportunity.short.cmp(&right.opportunity.short))
    });
    rows.truncate(top);
    for row in &mut rows {
        // 同名合约可能被身份判定拆成几个簇：找同时含这两条腿的那个。
        row.risk = report
            .symbols
            .iter()
            .filter(|view| view.symbol == row.symbol)
            .find_map(|view| {
                let long = leg(view, row.opportunity.long)?;
                let short = leg(view, row.opportunity.short)?;
                Some(pair_risk(&row.opportunity, long, short, leverage))
            });
    }
    rows
}

fn leg(view: &SymbolView, venue: Venue) -> Option<&MarketSnapshot> {
    view.rates.iter().find(|rate| rate.venue == venue)
}

fn next_funding(view: &SymbolView, venue: Venue) -> Option<DateTime<Utc>> {
    view.rates
        .iter()
        .find(|rate| rate.venue == venue)
        .map(|rate| rate.next_funding_at)
}

fn base_ok(symbol: &Symbol, bases: &[String]) -> bool {
    bases.is_empty()
        || bases
            .iter()
            .any(|base| symbol.base.eq_ignore_ascii_case(base))
}

fn filter_excluded(rows: &[ExcludedReading], bases: &[String]) -> Vec<ExcludedReading> {
    rows.iter()
        .filter(|row| base_ok(&row.symbol, bases))
        .cloned()
        .collect()
}

fn totals_for(
    report: &ScanReport,
    bases: &[String],
    suspicious: &[ExcludedReading],
    unverified: &[ExcludedReading],
    gated: &[RejectedPair],
) -> Totals {
    if bases.is_empty() {
        return report.totals.clone();
    }
    let symbols = report
        .symbols
        .iter()
        .filter(|view| base_ok(&view.symbol, bases));
    let mut profitable_pairs = 0;
    let mut spread_pairs = 0;
    let mut symbol_count = 0;
    for view in symbols {
        symbol_count += 1;
        profitable_pairs += view
            .funding
            .iter()
            .filter(|opportunity| opportunity.funding_profitable())
            .count();
        spread_pairs += view
            .spread
            .iter()
            .filter(|opportunity| opportunity.spread_profitable())
            .count();
    }
    Totals {
        venues_ok: report.totals.venues_ok,
        venues_failed: report.totals.venues_failed,
        rates: report.totals.rates,
        symbols: symbol_count,
        profitable_pairs,
        spread_pairs,
        excluded_rates: suspicious.len() + unverified.len(),
        gated_pairs: gated.len(),
    }
}

pub fn count_measured(report: &ScanReport) -> usize {
    report
        .symbols
        .iter()
        .flat_map(|view| view.spread.iter())
        .filter(|opportunity| opportunity.hold_measured)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Decimal;
    use arb_scanner::rank::{self, RankConfig};
    use chrono::Utc;
    use std::collections::HashMap;

    fn decimal(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    fn rate(venue: Venue, period: &str, mark: &str) -> MarketSnapshot {
        let price = decimal(mark);
        MarketSnapshot {
            venue,
            symbol: Symbol::perp("BTC", "USDT"),
            period_rate: decimal(period),
            interval_h: 8,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: false,
            taker_fee: Some(decimal("0.0005")),
            mark_price: Some(price),
            index_price: Some(price),
            best_bid: Some(price),
            best_ask: Some(price),
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: None,
            maintenance_margin: None,
            oi_capped: false,
        }
    }

    fn report_with(views: Vec<SymbolView>) -> ScanReport {
        ScanReport {
            generated_at: Utc::now(),
            fee_per_side: decimal("0.0005"),
            amortize_days: decimal("7"),
            spread_hold_days: decimal("3"),
            max_entry_basis_pct: Some(decimal("0.5")),
            min_venues: 2,
            venues: Vec::new(),
            symbols: views,
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            totals: Totals {
                venues_ok: 2,
                venues_failed: 0,
                rates: 4,
                symbols: 2,
                profitable_pairs: 2,
                spread_pairs: 2,
                excluded_rates: 0,
                gated_pairs: 0,
            },
        }
    }

    fn view(base: &str, high: Venue, low: Venue) -> SymbolView {
        let mut expensive = rate(high, "0.0004", "101");
        expensive.symbol = Symbol::perp(base, "USDT");
        let mut cheap = rate(low, "0.0001", "100");
        cheap.symbol = Symbol::perp(base, "USDT");
        let config = RankConfig {
            fee_per_side: decimal("0.0005"),
            amortize_days: decimal("7"),
            spread_hold_days: decimal("3"),
            measured_hold: HashMap::new(),
            max_entry_basis_pct: None,
        };
        let ranked = rank::rank(&[&expensive, &cheap], &config);
        SymbolView {
            symbol: expensive.symbol.clone(),
            rates: vec![expensive, cheap],
            funding: ranked.clone(),
            spread: ranked,
        }
    }

    #[test]
    fn the_board_keeps_only_the_requested_rows_and_their_books() {
        let report = report_with(vec![
            view("BBB", Venue::Binance, Venue::Okx),
            view("AAA", Venue::Bybit, Venue::Gate),
        ]);
        let board = project(&report, &[], 1, None, decimal("3"));
        assert_eq!(board.funding.len(), 1);
        assert_eq!(board.spread.len(), 1);
        assert_eq!(board.details.len(), 1, "没上榜的合约不该把盘口一起发出去");
        let shown = board.funding[0].symbol.to_string();
        assert!(board.details.contains_key(&shown));
        assert!(
            board.measured_pairs.is_none(),
            "普通刷新不带半衰期计数，避免前端把它当成实测结果"
        );
    }

    #[test]
    fn a_base_filter_drops_other_symbols_but_keeps_pair_totals() {
        let report = report_with(vec![
            view("BTC", Venue::Binance, Venue::Okx),
            view("ETH", Venue::Bybit, Venue::Gate),
        ]);
        let board = project(&report, &["btc".into()], 50, Some(0), decimal("3"));
        assert!(board.funding.iter().all(|row| row.symbol.base == "BTC"));
        assert!(board.spread.iter().all(|row| row.symbol.base == "BTC"));
        assert_eq!(board.totals.symbols, 1);
        assert_eq!(board.totals.venues_ok, 2, "场所计数仍是整轮扫描");
        assert_eq!(board.measured_pairs, Some(0));
    }

    #[test]
    fn every_shown_row_carries_its_risk_at_the_requested_leverage() {
        let report = report_with(vec![view("BTC", Venue::Binance, Venue::Okx)]);
        let board = project(&report, &[], 50, None, decimal("5"));
        assert_eq!(board.leverage, decimal("5"));
        let risk = board.funding[0]
            .risk
            .as_ref()
            .expect("两腿快照都在，应当有风险");
        assert_eq!(risk.leverage, decimal("5"));
        // 夹具没有维持保证金率：距离未知，而不是 0
        assert_eq!(risk.liq_distance_pct, None);
        assert!(risk.margin_apr > Decimal::ZERO);
    }

    #[test]
    fn top_query_rejects_the_full_universe() {
        assert_eq!(parse_top(None).unwrap(), 50);
        assert_eq!(parse_top(Some(300)).unwrap(), 300);
        assert!(parse_top(Some(301)).is_err());
        assert!(parse_top(Some(0)).is_err());
    }
}
