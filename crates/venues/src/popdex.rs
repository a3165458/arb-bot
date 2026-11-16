//! PopDEX（`https://api.popdex.xyz`）：链上订单簿永续，USDT 保证金，加密币与美股 / 指数 / 商品（`assetClass` = `Rwa`）。
//!
//! 三个批量端点（都要带 `limit`，默认每页只给 20 条；`limit` 最大 100，超过的按 `cursor` 翻页）：
//! - `/api/v1/public/market/tickers`：标记价、指数价、一档买卖价与数量（**标的数量**）、`openInterest`（标的数量）、
//!   24h 成交额 `turnover24h`、当期费率 `fundingRate`；
//! - `/api/v1/market/funding-rate?category=Futures`：结算周期 `fundingRateInterval`（小时，2026-10-20 实测全部 `"1"`）
//!   与下次结算 `nextUpdate`（毫秒）；
//! - `/api/v1/public/fee-rate`：按 VIP 档的挂单 / 吃单费率，取 VIP 0（实测吃单 `"0.00032"`）。
//!
//! 合约名 `SNDKUSDT`、`kPEPEUSDT`（小写前缀要保留原样去订阅；标的名按大写记）。
//! 2026-10-20 从 `api.popdex.ai` 访问返回 403，`api.popdex.xyz` 正常。

use std::collections::HashMap;

use arb_core::{
    ArbResult, DEFAULT_FUNDING_INTERVAL_H, Decimal, MarketSnapshot, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::PopDex;
pub const API: &str = "https://api.popdex.xyz";
const TICKERS_PATH: &str = "/api/v1/public/market/tickers";
const FUNDING_PATH: &str = "/api/v1/market/funding-rate?category=Futures";
pub const FEE_PATH: &str = "/api/v1/public/fee-rate";
const QUOTE: &str = "USDT";
/// 每页最多几条（接口上限）。
const PAGE: usize = 100;
/// 最多翻几页（防止游标不前进时死循环）。
const MAX_PAGES: usize = 10;

pub struct PopDexApi {
    client: Client,
}

impl PopDexApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(default = "Vec::new")]
    data: Vec<T>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    total: Option<String>,
}

/// 翻页拉全一个列表端点。`path` 可以已经带了查询参数。
pub async fn fetch_pages<T: for<'de> Deserialize<'de>>(
    client: &Client,
    path: &str,
) -> ArbResult<Vec<T>> {
    let mut out: Vec<T> = Vec::new();
    let mut cursor: Option<String> = None;
    let joiner = if path.contains('?') { '&' } else { '?' };
    for _ in 0..MAX_PAGES {
        let mut url = format!("{API}{path}{joiner}limit={PAGE}");
        if let Some(cursor) = &cursor {
            url.push_str(&format!("&cursor={cursor}"));
        }
        let page: Page<T> = get_json(client.get(url), VENUE).await?;
        let got = page.data.len();
        out.extend(page.data);
        let total = page
            .total
            .as_deref()
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        if got == 0 || out.len() >= total || page.cursor.is_none() || page.cursor == cursor {
            break;
        }
        cursor = page.cursor;
    }
    Ok(out)
}

/// `/api/v1/public/market/tickers` 的一行。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ticker {
    pub symbol: String,
    #[serde(default)]
    pub status: Option<String>,
    pub bid1_price: Option<String>,
    pub ask1_price: Option<String>,
    pub bid1_size: Option<String>,
    pub ask1_size: Option<String>,
    pub turnover24h: Option<String>,
    pub index_price: Option<String>,
    pub mark_price: Option<String>,
    pub funding_rate: Option<String>,
    pub open_interest: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Funding {
    symbol: String,
    funding_rate_interval: Option<String>,
    next_update: Option<String>,
}

/// `/api/v1/public/fee-rate` 的一行。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeRow {
    pub symbol: String,
    #[serde(default)]
    pub fee_rates: Vec<FeeTier>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeTier {
    pub vip_level: String,
    pub taker_fee_rate: Option<String>,
}

impl FeeRow {
    /// VIP 0 的吃单费率。
    pub fn base_taker(&self) -> Option<Decimal> {
        self.fee_rates
            .iter()
            .find(|tier| tier.vip_level == "0")
            .and_then(|tier| tier.taker_fee_rate.as_deref())
            .and_then(parse_decimal)
    }
}

/// `SNDKUSDT` → `某合约`、`kPEPEUSDT` → `KPEPE`。不是 USDT 计价的不认。
pub fn base_of(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix(QUOTE)?;
    (!base.is_empty()).then(|| base.to_ascii_uppercase())
}

#[async_trait]
impl VenueApi for PopDexApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let tickers: Vec<Ticker> = fetch_pages(&self.client, TICKERS_PATH).await?;
        // 周期与吃单费率不是费率读数本身：取不到时周期按默认值并标为假定、费率报未知。
        let funding: HashMap<String, Funding> =
            match fetch_pages::<Funding>(&self.client, FUNDING_PATH).await {
                Ok(rows) => rows
                    .into_iter()
                    .map(|row| (row.symbol.clone(), row))
                    .collect(),
                Err(error) => {
                    warn!(venue = %VENUE, %error, "结算周期端点失败，周期按默认值上报");
                    HashMap::new()
                }
            };
        let fees: HashMap<String, Decimal> =
            match fetch_pages::<FeeRow>(&self.client, FEE_PATH).await {
                Ok(rows) => rows
                    .into_iter()
                    .filter_map(|row| Some((row.symbol.clone(), row.base_taker()?)))
                    .collect(),
                Err(error) => {
                    warn!(venue = %VENUE, %error, "费率端点失败，吃单费率按未知上报");
                    HashMap::new()
                }
            };
        let now = Utc::now();
        let mut out = Vec::with_capacity(tickers.len());
        let mut unusable = 0usize;
        for ticker in &tickers {
            if ticker.status.as_deref().is_some_and(|s| s != "Trading") {
                continue;
            }
            match parse_ticker(
                ticker,
                funding.get(&ticker.symbol),
                fees.get(&ticker.symbol).copied(),
                now,
            ) {
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

fn parse_ticker(
    ticker: &Ticker,
    funding: Option<&Funding>,
    taker_fee: Option<Decimal>,
    now: DateTime<Utc>,
) -> Option<MarketSnapshot> {
    let base = base_of(&ticker.symbol)?;
    let period_rate = ticker.funding_rate.as_deref().and_then(parse_decimal)?;
    let interval = funding
        .and_then(|f| f.funding_rate_interval.as_deref())
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|hours| *hours > 0);
    let (interval_h, interval_assumed) = match interval {
        Some(hours) => (hours, false),
        None => (DEFAULT_FUNDING_INTERVAL_H, true),
    };
    let next = funding
        .and_then(|f| f.next_update.as_deref())
        .and_then(|raw| raw.parse::<i64>().ok())
        .and_then(DateTime::from_timestamp_millis);
    let (next_funding_at, next_funding_estimated) = match next {
        Some(at) => (at, false),
        None => (next_boundary(now, interval_h), true),
    };
    let positive = |raw: Option<&str>| raw.and_then(parse_decimal).filter(|v| *v > Decimal::ZERO);
    let mark_price = positive(ticker.mark_price.as_deref());
    let mut best_bid = positive(ticker.bid1_price.as_deref());
    let mut best_ask = positive(ticker.ask1_price.as_deref());
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| ask < bid) {
        best_bid = None;
        best_ask = None;
    }
    let notional = |size: Option<&str>, price: Option<Decimal>| {
        positive(size)
            .zip(price)
            .and_then(|(s, p)| s.checked_mul(p))
    };
    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated,
        taker_fee,
        mark_price,
        index_price: positive(ticker.index_price.as_deref()),
        best_bid,
        best_ask,
        bid_size_usdt: notional(ticker.bid1_size.as_deref(), best_bid),
        ask_size_usdt: notional(ticker.ask1_size.as_deref(), best_ask),
        open_interest_usdt: notional(ticker.open_interest.as_deref(), mark_price),
        quote_volume_24h: ticker.turnover24h.as_deref().and_then(parse_decimal),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 下一个整 `hours` 小时（UTC）。只在接口没给下次结算时用，并标为推算。
fn next_boundary(now: DateTime<Utc>, hours: u32) -> DateTime<Utc> {
    let step = i64::from(hours.max(1)) * 3600;
    let next = (now.timestamp() / step + 1) * step;
    DateTime::from_timestamp(next, 0).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    /// 2026-10-20 实测 tickers 的两行。
    const TICKERS: &str = r#"{"code":"200","msg":"success","data":[
      {"category":"FUTURES","symbolId":"21102","symbol":"SNDKUSDT","lastPrice":"1357.5","ask1Price":"1358.4","bid1Price":"1357.9","bid1Size":"5.886","ask1Size":"8.396","turnover24h":"748904.8463","volume24h":"546.578","indexPrice":"1356.8","markPrice":"1357.8","fundingRate":"0.000019","openInterest":"312.257","updatedTime":"1792470146185","status":"Trading"},
      {"category":"FUTURES","symbolId":"20007","symbol":"kPEPEUSDT","lastPrice":"0.0042","ask1Price":"0.0042","bid1Price":"0.0042","bid1Size":"171800","ask1Size":"182100","turnover24h":"235222.2553","indexPrice":"0.0042","markPrice":"0.0042","fundingRate":"0.000012","openInterest":"27359333","status":"Trading"}
    ],"cursor":"66","preCursor":"","total":"66","limit":"100"}"#;

    const FUNDING: &str = r#"{"symbolId":"21102","symbol":"SNDKUSDT","fundingRate":"0.0000197","fundingRateInterval":"1","nextUpdate":"1792472400001","minFundingRate":"-0.0225","maxFundingRate":"0.0225"}"#;

    const FEES: &str = r#"{"symbolId":"21102","symbol":"SNDKUSDT","category":"Futures","feeRates":[{"vipLevel":"0","makerFeeRate":"0.0001","takerFeeRate":"0.00032"},{"vipLevel":"1","makerFeeRate":"0.00008","takerFeeRate":"0.0003"}]}"#;

    fn tickers() -> Vec<Ticker> {
        serde_json::from_str::<Page<Ticker>>(TICKERS).unwrap().data
    }

    #[test]
    fn hourly_usdt_perps_with_base_sizes_converted_to_notional() {
        let rows = tickers();
        let funding: Funding = serde_json::from_str(FUNDING).unwrap();
        let fee: FeeRow = serde_json::from_str(FEES).unwrap();
        assert_eq!(fee.base_taker(), Some(d("0.00032")), "取 VIP 0");
        let now = "2026-10-20T04:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let sndk = parse_ticker(&rows[0], Some(&funding), fee.base_taker(), now).unwrap();
        assert_eq!(sndk.symbol, Symbol::perp("SNDK", "USDT"));
        assert_eq!((sndk.interval_h, sndk.interval_assumed), (1, false));
        assert!(!sndk.next_funding_estimated);
        assert_eq!(sndk.next_funding_at.timestamp_millis(), 1792472400001);
        assert_eq!(sndk.period_rate, d("0.000019"));
        assert_eq!(sndk.taker_fee, Some(d("0.00032")));
        assert_eq!(sndk.mark_price, Some(d("1357.8")));
        assert_eq!(sndk.bid_size_usdt, Some(d("5.886") * d("1357.9")));
        assert_eq!(sndk.open_interest_usdt, Some(d("312.257") * d("1357.8")));
        assert_eq!(sndk.quote_volume_24h, Some(d("748904.8463")));
    }

    #[test]
    fn lowercase_prefixes_are_uppercased_and_missing_funding_meta_is_flagged() {
        let rows = tickers();
        let now = "2026-10-20T04:30:00Z".parse::<DateTime<Utc>>().unwrap();
        let pepe = parse_ticker(&rows[1], None, None, now).unwrap();
        assert_eq!(pepe.symbol.base, "KPEPE");
        assert_eq!(
            (pepe.interval_h, pepe.interval_assumed),
            (DEFAULT_FUNDING_INTERVAL_H, true),
            "周期没取到：默认值并标为假定"
        );
        assert!(pepe.next_funding_estimated);
        assert_eq!(pepe.taker_fee, None, "费率没取到就是未知，不编");
        assert_eq!(base_of("BTCUSDC"), None);
        assert_eq!(base_of("USDT"), None);
    }
}
