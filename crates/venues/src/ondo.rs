//! Ondo Perps（`https://api.ondoperps.xyz`）：Ondo 的美股代币化永续，USDC 保证金。
//!
//! `/v1/perps/contracts` 一次给全部永续：每期费率 `fundingRate`、下次结算 `nextFundingRateTimestamp`、
//! 公开吃单费率 `takerFee`（2026-10-20 实测 `"0.00025"`，活动价）、一档买卖价、`openInterestUsd`、
//! 24h 成交额 `usdVolume`、指数价。结算周期接口里没有：官方文档「Funding Rates」写明**每小时整点结算一次**。
//! 一档数量没有，报未知。标记价在另一个批量端点 `/v1/perps/mark_prices`（按市场名索引）。
//!
//! 名字是 `NVDA-USD.P`，`baseCurrency` = `NVDA`。计价写的是 `USD`，保证金实际是 USDC：按 USDC 记，
//! 才和其它 USDC / USDT 场所进同一个结算族（[`arb_core::settlement_family`]），身份分簇才对得上。
//! `disabled` 的跳过（下架、未开放）；`isClosed`（标的休市）照常列出，它仍可交易。

use std::collections::HashMap;

use arb_core::{ArbResult, Decimal, MarketSnapshot, Symbol, Venue, parse_decimal};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Ondo;
pub const CONTRACTS_URL: &str = "https://api.ondoperps.xyz/v1/perps/contracts";
const MARK_PRICES_URL: &str = "https://api.ondoperps.xyz/v1/perps/mark_prices";
/// 文档写明的结算周期（小时）。
const FUNDING_INTERVAL_H: u32 = 1;
/// 保证金资产（接口里的计价写 `USD`）。
const QUOTE: &str = "USDC";

pub struct OndoApi {
    client: Client,
}

impl OndoApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 标记价不是资金费读数的前提：端点失败时只把标记价报未知。
    async fn fetch_marks(&self) -> HashMap<String, String> {
        let body: Envelope<HashMap<String, MarkPrice>> =
            match get_json(self.client.get(MARK_PRICES_URL), VENUE).await {
                Ok(body) => body,
                Err(error) => {
                    warn!(venue = %VENUE, %error, "标记价端点失败，标记价按未知上报");
                    return HashMap::new();
                }
            };
        body.result
            .into_iter()
            .filter_map(|(market, mark)| Some((market, mark.mark_price?)))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    result: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkPrice {
    mark_price: Option<String>,
}

/// `/v1/perps/contracts` 的一行。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    pub market: String,
    pub base_currency: String,
    #[serde(default)]
    pub product_type: Option<String>,
    #[serde(default)]
    pub disabled: bool,
    pub bid: Option<String>,
    pub ask: Option<String>,
    pub usd_volume: Option<String>,
    pub open_interest_usd: Option<String>,
    pub index_price: Option<String>,
    pub funding_rate: Option<String>,
    pub next_funding_rate_timestamp: Option<String>,
    pub taker_fee: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl Contract {
    /// 在交易的永续。
    pub fn tradable(&self) -> bool {
        !self.disabled
            && self
                .product_type
                .as_deref()
                .is_none_or(|t| t == "perpetual")
            && !self.base_currency.is_empty()
    }
}

/// 拉全部合约（价差监控的市场列表也用它）。
pub async fn fetch_contracts(client: &Client) -> ArbResult<Vec<Contract>> {
    let body: Envelope<Vec<Contract>> = get_json(client.get(CONTRACTS_URL), VENUE).await?;
    Ok(body.result)
}

#[async_trait]
impl VenueApi for OndoApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let contracts = fetch_contracts(&self.client).await?;
        let marks = self.fetch_marks().await;
        let mut out = Vec::with_capacity(contracts.len());
        let mut unusable = 0usize;
        for item in contracts.iter().filter(|item| item.tradable()) {
            match parse_contract(item, marks.get(&item.market).map(String::as_str)) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }
}

fn parse_contract(item: &Contract, mark: Option<&str>) -> Option<MarketSnapshot> {
    let period_rate = item.funding_rate.as_deref().and_then(parse_decimal)?;
    let next_funding_at = item
        .next_funding_rate_timestamp
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())?
        .with_timezone(&Utc);
    let positive = |raw: Option<&str>| raw.and_then(parse_decimal).filter(|v| *v > Decimal::ZERO);
    let mut best_bid = positive(item.bid.as_deref());
    let mut best_ask = positive(item.ask.as_deref());
    // 交叉盘不能拿来算穿价。
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| ask < bid) {
        best_bid = None;
        best_ask = None;
    }
    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(&item.base_currency, QUOTE),
        period_rate,
        interval_h: FUNDING_INTERVAL_H,
        interval_assumed: false,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: item.taker_fee.as_deref().and_then(parse_decimal),
        mark_price: positive(mark),
        index_price: positive(item.index_price.as_deref()),
        best_bid,
        best_ask,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt: item.open_interest_usd.as_deref().and_then(parse_decimal),
        quote_volume_24h: item.usd_volume.as_deref().and_then(parse_decimal),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    /// 2026-10-20 实测 `/v1/perps/contracts` 的三行（节选字段）：股票、休市中的股票、未开放的 Pre-IPO。
    const CONTRACTS: &str = r#"{"success":true,"result":[
      {"market":"NVDA-USD.P","displayName":"NVDAUSD","productType":"perpetual","contractType":"linear","baseCurrency":"NVDA","quoteCurrency":"USD","disabled":false,"lastPrice":"217.02","usdVolume":"5980000.12","bid":"217.02","ask":"217.06","openInterestUsd":"1234567.8","indexPrice":"217.1","fundingRate":"0.0000125","nextFundingRate":"0.0000125","nextFundingRateTimestamp":"2026-10-20T05:00:00Z","makerFee":"0.0001","takerFee":"0.00025","isClosed":false,"tags":["Stock"]},
      {"market":"CXMT-USD.P","productType":"perpetual","baseCurrency":"CXMT","quoteCurrency":"USD","disabled":false,"usdVolume":"510000","bid":"6.1646","ask":"6.1818","indexPrice":"6.17","fundingRate":"-0.0001","nextFundingRateTimestamp":"2026-10-20T05:00:00Z","takerFee":"0.00025","isClosed":true,"tags":["Stock"]},
      {"market":"OPENAI-USD.P","productType":"perpetual","baseCurrency":"OPENAI","quoteCurrency":"USD","disabled":true,"bid":"1478.3","ask":"1499","fundingRate":"0","nextFundingRateTimestamp":"2026-10-20T05:00:00Z","takerFee":"0.00035","tags":["Pre-IPO"]}
    ]}"#;

    fn contracts() -> Vec<Contract> {
        serde_json::from_str::<Envelope<Vec<Contract>>>(CONTRACTS)
            .unwrap()
            .result
    }

    #[test]
    fn hourly_usdc_perps_with_public_taker_fee_and_disabled_markets_skipped() {
        let rows = contracts();
        let tradable: Vec<&Contract> = rows.iter().filter(|c| c.tradable()).collect();
        assert_eq!(tradable.len(), 2, "未开放的 OPENAI 不算");
        let nvda = parse_contract(tradable[0], Some("217.05")).unwrap();
        assert_eq!(nvda.symbol, Symbol::perp("NVDA", "USDC"));
        assert_eq!(
            (nvda.interval_h, nvda.interval_assumed),
            (1, false),
            "文档：每小时结算"
        );
        assert_eq!(nvda.period_rate, d("0.0000125"));
        assert_eq!(nvda.taker_fee, Some(d("0.00025")));
        assert_eq!(nvda.mark_price, Some(d("217.05")));
        assert_eq!(nvda.index_price, Some(d("217.1")));
        assert_eq!(
            (nvda.best_bid, nvda.best_ask),
            (Some(d("217.02")), Some(d("217.06")))
        );
        assert_eq!(nvda.bid_size_usdt, None, "接口不给一档数量，不编");
        assert_eq!(nvda.quote_volume_24h, Some(d("5980000.12")));
        assert_eq!(
            nvda.next_funding_at,
            "2026-10-20T05:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        let closed = parse_contract(tradable[1], None).unwrap();
        assert_eq!(closed.symbol.base, "CXMT", "休市中照常列出");
        assert_eq!(closed.mark_price, None, "标记价没取到就是未知");
    }

    #[test]
    fn crossed_tops_are_dropped_and_missing_funding_is_unusable() {
        let mut row = contracts().remove(0);
        row.bid = Some("217.10".into());
        row.ask = Some("217.00".into());
        let rate = parse_contract(&row, None).unwrap();
        assert_eq!((rate.best_bid, rate.best_ask), (None, None));
        row.funding_rate = None;
        assert!(parse_contract(&row, None).is_none());
    }
}
