//! MEXC 永续。
//!
//! 三个批量端点配合，按 `symbol` 联接：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/contract/funding_rate` | 每期费率、**结算周期**、下次结算、标记价、指数价 | 吃单费率、面值 |
//! | `/contract/detail` | 吃单费率、面值、保证金币 | 费率、周期 |
//! | `/contract/ticker` | 持仓量、24h 成交额、**最优买卖价** | 费率、周期、**一档量** |
//!
//! **结算周期必须逐合约读。** 实测 1182 个合约的 `collectCycle` 是 1 / 4 / 8 / 24
//! 小时的混合（1h 有 8 个、4h 有 606 个、8h 有 567 个）。按固定 8h 折算，4h 合约的
//! 日化会低估一半、1h 合约低估 8 倍，而排名照常算得出来。
//!
//! 吃单费率也是逐合约的（实测 0 / 0.0001 / 0.0002 / 0.0004 / 0.001 五种），
//! 用一个全局默认值代替会让一部分合约的成本算错。
//!
//! 实测该场所会对同一合约在几秒内反复返回 0 与真值，所以**费率字段缺失一律丢行**，
//! 不做任何「回落成 0」的处理。
//!
//! **一档量保持未知。** 实测批量 `/contract/ticker` 的 `bid1`/`ask1` 是最优买卖价，
//! 没有 `bid1Vol`/`ask1Vol`；官方文档也未列出批量一档量端点。带量的
//! `/contract/depth/{symbol}` 要逐合约请求，不适合近千个合约的扫描，所以两侧量留
//! `None`，不能拿 24h 累计量或持仓量冒充。深度量的单位是张，若有可用批量来源，
//! USDT 名义应为 `张数 × contractSize × 该侧价格`，不能把张数当币数。
//!
//! **逐合约深度走 [`VenueApi::fetch_depth`]。** 它同样用 `/contract/depth/{symbol}`，
//! 但只对少数候选调用（深度体检），不进批量扫描。那条路上的量是**张**，换算成
//! USDT 名义必须乘每张面值 —— 见 [`build_book`]。
//!
//! **逐合约 K 线走 [`VenueApi::fetch_candles`]。** `/contract/kline/{symbol}` 的响应是
//! **列式**的（`{time:[…], close:[…], …}`，不是一行一根），所以必须按下标对齐：
//! 实测各列长度一致，长度不一致说明结构变了，这时宁可报错也不能截到较短的那一列 ——
//! 那会把两根不相干的 K 线拼在一起，而且看不出来。
//!
//! `interval` 是**枚举**，实测合法取值只有 `Min1` / `Min5` / `Min15` / `Min30` /
//! `Min60` / `Hour4` / `Hour8` / `Day1`（`Hour1`、`Min120`、`Min240`、`Min3` 都回
//! `success=false, code=600`）。请求到不支持的周期时**向上取整**到不小于它的合法值：
//! 返回比请求更细的序列会把实测出来的半衰期算成一个更短的时间，而更粗的只是分辨率
//! 变差。见 [`kline_interval`]。
//!
//! `time` 是**秒**（`Min60` 相邻两值差 3600），实测按升序返回；代码仍显式排序，
//! 不依赖这个没写进文档的顺序 —— 顺序反了不会报错，只会把半衰期算成正数。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, from_json_f64,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Mexc;
const FUNDING_URL: &str = "https://contract.mexc.com/api/v1/contract/funding_rate";
const DETAIL_URL: &str = "https://contract.mexc.com/api/v1/contract/detail";
const TICKER_URL: &str = "https://contract.mexc.com/api/v1/contract/ticker";
const DEPTH_URL: &str = "https://contract.mexc.com/api/v1/contract/depth";
const KLINE_URL: &str = "https://contract.mexc.com/api/v1/contract/kline";

/// `kline` 的 `interval` 合法取值（分钟 → 字符串），见
/// <https://www.mexc.com/api-docs/futures/market-endpoints/get-candlestick-data>：
/// Min1, Min5, Min15, Min30, Min60, Hour4, Hour8, Day1, Week1, Month1。
const KLINE_INTERVALS: [(u32, &str); 10] = [
    (1, "Min1"),
    (5, "Min5"),
    (15, "Min15"),
    (30, "Min30"),
    (60, "Min60"),
    (240, "Hour4"),
    (480, "Hour8"),
    (1440, "Day1"),
    (10080, "Week1"),
    (43200, "Month1"),
];

fn kline_interval(minutes: u32) -> ArbResult<&'static str> {
    KLINE_INTERVALS
        .iter()
        .find(|(value, _)| *value >= minutes)
        .map(|(_, name)| *name)
        .ok_or_else(|| {
            ArbError::config(format!(
                "K 线周期 {minutes} 分钟超过该端点支持的最大周期（{}）",
                KLINE_INTERVALS[KLINE_INTERVALS.len() - 1].0
            ))
        })
}

/// **列式** K 线：只要时间和收盘价两列。
#[derive(Debug, Deserialize)]
struct KlineData {
    #[serde(default)]
    time: Vec<i64>,
    #[serde(default)]
    close: Vec<f64>,
}

/// 按**下标**对齐两列。长度不一致说明数据坏了 —— 报错而不是截断。
fn parse_candles(data: &KlineData, limit: usize) -> ArbResult<Vec<Candle>> {
    if data.time.len() != data.close.len() {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!(
                "K 线列长度不一致：time={} close={}",
                data.time.len(),
                data.close.len()
            ),
        ));
    }
    let mut out: Vec<Candle> = data
        .time
        .iter()
        .zip(data.close.iter())
        .filter_map(|(seconds, close)| {
            let close = from_json_f64(*close)?;
            if close <= Decimal::ZERO {
                return None;
            }
            Some(Candle {
                // 单位是**秒**，不是毫秒 —— 用错会得到 1970 年。
                open_time: DateTime::from_timestamp(*seconds, 0)?,
                close,
            })
        })
        .collect();
    out.sort_by_key(|candle| candle.open_time);
    if out.len() > limit {
        out.drain(..out.len() - limit);
    }
    Ok(out)
}

/// 计价资产。只接 USDT 保证金；该场所还有 USDC / USD1 / 币本位合约，机制不同。
const QUOTE: &str = "USDT";

/// 结算周期的合理范围（小时）。超出这个范围说明字段语义变了，按未知处理。
const MAX_INTERVAL_H: u32 = 24;

pub struct MexcApi {
    client: Client,
}

impl MexcApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 面值与吃单费率，按 symbol 索引。失败只影响这两项，不该让整家场所消失。
    async fn fetch_details(&self) -> HashMap<String, Detail> {
        self.optional_rows::<Detail>(DETAIL_URL, "合约详情")
            .await
            .into_iter()
            .map(|row| (row.symbol.clone(), row))
            .collect()
    }

    /// 持仓量与 24h 成交额，按 symbol 索引。同样是「拿不到就报未知」，
    /// 不阻塞资金费读数。
    async fn fetch_tickers(&self) -> HashMap<String, Ticker> {
        self.optional_rows::<Ticker>(TICKER_URL, "行情")
            .await
            .into_iter()
            .map(|row| (row.symbol.clone(), row))
            .collect()
    }

    /// 取一批可选数据。端点失败或业务失败都只记 warn 并返回空表 ——
    /// 这两项不是资金费读数的前提，不该让整家场所消失。
    async fn optional_rows<T>(&self, url: &str, what: &'static str) -> Vec<T>
    where
        T: serde::de::DeserializeOwned,
    {
        match get_json::<Envelope<T>>(self.client.get(url), VENUE).await {
            Ok(rows) => rows.into_data(what).unwrap_or_else(|error| {
                warn!(venue = %VENUE, %error, "{}失败，相关字段按未知上报", what);
                Vec::new()
            }),
            Err(error) => {
                warn!(venue = %VENUE, %error, "{}失败，相关字段按未知上报", what);
                Vec::new()
            }
        }
    }
}

/// MEXC 的响应外壳。
///
/// 失败时 HTTP 仍是 **200**，只有 `success = false`。只看状态码会把「接口失败」
/// 当成「这家场所一个合约都没有」，场所就这样从排名里静默消失。
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct Envelope<T> {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    data: Option<Vec<T>>,
    #[serde(default)]
    code: Option<i64>,
}

impl<T> Envelope<T> {
    fn into_data(self, what: &str) -> ArbResult<Vec<T>> {
        if !self.success {
            return Err(ArbError::venue(
                VENUE.as_str(),
                format!(
                    "{what} success=false（code={}）",
                    self.code.unwrap_or_default()
                ),
            ));
        }
        Ok(self.data.unwrap_or_default())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRow {
    symbol: String,
    /// JSON number，不是字符串。
    funding_rate: Option<f64>,
    /// 结算周期（**小时**）。实测取值 1 / 4 / 8 / 24。
    collect_cycle: Option<i64>,
    /// 毫秒时间戳。
    next_settle_time: Option<i64>,
    fair_price: Option<f64>,
    idx_price: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Detail {
    symbol: String,
    settle_coin: Option<String>,
    /// 每张合约的标的数量（如 BTC_USDT 是 0.0001 BTC/张）。
    contract_size: Option<f64>,
    taker_fee_rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker {
    symbol: String,
    /// 持仓量（**张**，单边）。
    hold_vol: Option<f64>,
    /// 24h 成交额（计价币）。
    amount24: Option<f64>,
    bid1: Option<f64>,
    ask1: Option<f64>,
}

#[async_trait]
impl VenueApi for MexcApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        // 以资金费端点为基准：只有它同时给出费率与结算周期。
        let funding: Vec<FundingRow> =
            get_json::<Envelope<FundingRow>>(self.client.get(FUNDING_URL), VENUE)
                .await?
                .into_data("资金费")?;
        let details = self.fetch_details().await;
        let tickers = self.fetch_tickers().await;

        let mut out = Vec::with_capacity(funding.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for row in funding {
            if !is_usdt_perp(&row.symbol) {
                filtered += 1;
                continue;
            }
            let detail = details.get(&row.symbol);
            // 保证金币与符号不一致说明这条读数的口径和我们以为的不一样，丢行。
            if detail.is_some_and(|detail| {
                detail
                    .settle_coin
                    .as_deref()
                    .is_some_and(|coin| !coin.eq_ignore_ascii_case(QUOTE))
            }) {
                filtered += 1;
                continue;
            }
            match parse_row(&row, detail, tickers.get(&row.symbol)) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非 USDT 永续合约已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }
    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        if levels == 0 {
            return Err(ArbError::config("levels 必须大于 0"));
        }
        let native = format!("{}_{QUOTE}", symbol.base);

        let detail: DepthEnvelope<Detail> = get_json(
            self.client.get(format!("{DETAIL_URL}?symbol={native}")),
            VENUE,
        )
        .await?;
        let contract_size = detail
            .into_data("合约详情")?
            .contract_size
            .and_then(from_json_f64)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                ArbError::venue(
                    VENUE.as_str(),
                    format!("{native} 缺少合约面值，盘口无法换算成名义额"),
                )
            })?;

        let book: DepthEnvelope<DepthData> = get_json(
            self.client
                .get(format!("{DEPTH_URL}/{native}?limit={levels}")),
            VENUE,
        )
        .await?;
        build_book(symbol, &book.into_data("深度")?, contract_size, levels)
    }

    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `kline` 端点是**列式**响应（`{time: [...], close: [...]}`），不是行式：
    /// 两列必须按**下标对齐**，长度不一致时直接报错而不是截断 —— 截断会把
    /// 收盘价错配到别的时间点上，造出一段并不存在的基差序列。
    ///
    /// 它也没有 `limit` 参数，固定返回 2000 根；取回后只保留最后 `limit` 根。
    /// `interval` 是枚举字符串，请求的分钟数向上取到最近的合法值。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let native = format!("{}_{QUOTE}", symbol.base);
        let interval = kline_interval(interval_minutes)?;
        let url = format!("{KLINE_URL}/{native}?interval={interval}");
        let envelope: DepthEnvelope<KlineData> = get_json(self.client.get(&url), VENUE).await?;
        let data = envelope.into_data("K 线")?;
        parse_candles(&data, limit as usize)
    }
}

/// 把三个端点的一行合成一条读数。`None` = 这一行不可用。
///
/// 费率缺失或不可解析一律丢行：实测该场所会对同一合约在几秒内反复返回 0 与真值，
/// 把「没给」当成 0 会凭空造出巨大价差并排到榜首。
fn parse_row(
    row: &FundingRow,
    detail: Option<&Detail>,
    ticker: Option<&Ticker>,
) -> Option<MarketSnapshot> {
    let base = row.symbol.strip_suffix(&format!("_{QUOTE}"))?;
    let period_rate = row.funding_rate.and_then(from_json_f64)?;

    let next_settle = row.next_settle_time.filter(|ms| *ms > 0)?;
    let next_funding_at = DateTime::from_timestamp_millis(next_settle)?;

    // 只有「场所给了且落在合理范围内」才敢用；其余情况一律标记为回落值。
    // 不能写成 `interval_h == DEFAULT && cycle.is_none()`：那样 Some(48) 这种
    // 超出范围的取值也会被当成场所给的真值。
    let (interval_h, interval_assumed) = match row.collect_cycle {
        Some(hours) if (1..=MAX_INTERVAL_H as i64).contains(&hours) => (hours as u32, false),
        _ => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    let mark_price = row.fair_price.and_then(from_json_f64);
    let open_interest_usdt = ticker
        .and_then(|t| t.hold_vol)
        .and_then(from_json_f64)
        .filter(|vol| *vol >= Decimal::ZERO)
        .zip(detail.and_then(|d| d.contract_size).and_then(from_json_f64))
        .zip(mark_price)
        .and_then(|((contracts, size), price)| {
            // 张数 × 每张标的数量 × 标记价。面值缺失时不能默认一张等于一个币。
            if size <= Decimal::ZERO || price <= Decimal::ZERO {
                return None;
            }
            contracts.checked_mul(size)?.checked_mul(price)
        });

    let mut best_bid = ticker
        .and_then(|t| t.bid1)
        .and_then(from_json_f64)
        .filter(|price| *price > Decimal::ZERO);
    let mut best_ask = ticker
        .and_then(|t| t.ask1)
        .and_then(from_json_f64)
        .filter(|price| *price > Decimal::ZERO);
    // 零值不能当成免费成交；交叉盘无法判断哪侧可信，清掉盘口但保留独立的资金费读数。
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| ask < bid) {
        best_bid = None;
        best_ask = None;
    }

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: detail
            .and_then(|d| d.taker_fee_rate)
            .and_then(from_json_f64),
        mark_price,
        index_price: row.idx_price.and_then(from_json_f64),
        best_bid,
        best_ask,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt,
        quote_volume_24h: ticker.and_then(|t| t.amount24).and_then(from_json_f64),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 单个合约的盘口深度。
///
/// `/contract/depth/{symbol}` 的 `vol` 是**张数**，名义额是
/// `张数 × contractSize × price`（面值从 detail 端点取，用 `?symbol=` 只取一条）。
/// 面值缺失时整份盘口都换算不出来，直接报错。
/// **单对象**响应的外壳。
///
/// `depth/{symbol}` 与 `detail?symbol=` 的 `data` 都是**对象**（不是 `fetch_all` 用的
/// 那种数组）。套用 `Envelope<T>` 只会得到一句「期望数组却拿到对象」的反序列化错误，
/// 而真正的原因（`success=false` 的 message）被丢掉，排查方向会完全跑偏。
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct DepthEnvelope<T> {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    code: Option<i64>,
}

impl<T> DepthEnvelope<T> {
    fn into_data(self, what: &str) -> ArbResult<T> {
        if !self.success {
            return Err(ArbError::venue(
                VENUE.as_str(),
                format!(
                    "{what} success=false（code={}）",
                    self.code.unwrap_or_default()
                ),
            ));
        }
        self.data
            .ok_or_else(|| ArbError::venue(VENUE.as_str(), format!("{what} 的 data 为空")))
    }
}

/// MEXC 的档位是 `[价格, 张数, 挂单数]`，三个都是 JSON number。
#[derive(Debug, Deserialize)]
struct DepthData {
    #[serde(default)]
    bids: Vec<[f64; 3]>,
    #[serde(default)]
    asks: Vec<[f64; 3]>,
}

/// 把深度响应转成盘口。空盘或交叉盘一律报错。
fn build_book(
    symbol: &Symbol,
    depth: &DepthData,
    contract_size: Decimal,
    levels: u32,
) -> ArbResult<OrderBook> {
    let side = |rows: &[[f64; 3]]| -> Vec<Level> {
        let out: Vec<Level> = rows
            .iter()
            .filter_map(|[price, volume, _]| {
                let price = from_json_f64(*price)?;
                let contracts = from_json_f64(*volume)?;
                if price <= Decimal::ZERO || contracts <= Decimal::ZERO {
                    return None;
                }
                Some(Level {
                    price,
                    notional_usdt: contracts * contract_size * price,
                })
            })
            .collect();
        out
    };

    let mut bids = side(&depth.bids);
    let mut asks = side(&depth.asks);
    // 先排序再截断。先截断会在上游乱序时丢掉更优档，滑点按残盘来算。
    bids.sort_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_by_key(|a| a.price);
    let keep = levels as usize;
    bids.truncate(keep);
    asks.truncate(keep);

    if bids.is_empty() || asks.is_empty() {
        return Err(ArbError::venue(VENUE.as_str(), "盘口有一侧为空"));
    }
    if asks[0].price < bids[0].price {
        return Err(ArbError::venue(VENUE.as_str(), "盘口交叉（ask < bid）"));
    }
    Ok(OrderBook {
        venue: VENUE,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

/// MEXC 用 `_` 分隔 base 与计价资产（`BTC_USDT`），所以这里**不能**套用 Binance 那条
/// 「符号里不许有下划线」的规则 —— 那会把所有合约都排除掉。
///
/// 只接 USDT 保证金：`BTC_USDC`、`AVAX_USD`、`HYPE_USD1` 与币本位合约的资金费机制
/// 都不同。带数字前缀的（`1000000BABYDOGE_USDT`）是正常的倍数合约，不能当交割符号排掉。
fn is_usdt_perp(symbol: &str) -> bool {
    symbol
        .strip_suffix(&format!("_{QUOTE}"))
        .is_some_and(|base| !base.is_empty() && !base.contains('_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-09-19 打真实端点取到的片段，字段名与 JSON 类型原样保留。
    const FUNDING_FIXTURE: &str = r#"{"success":true,"code":0,"data":[
        {"symbol":"BTC_USDT","fundingRate":0.0001,"maxFundingRate":0.0018,"minFundingRate":-0.0018,
         "collectCycle":8,"nextSettleTime":1789833600000,"timestamp":1789827293197,
         "idxPrice":81382.7,"fairPrice":81351.6},
        {"symbol":"CVC_USDT","fundingRate":-0.000379,"collectCycle":1,
         "nextSettleTime":1789826400000,"idxPrice":0.028,"fairPrice":0.02788}
    ]}"#;

    const DETAIL_FIXTURE: &str = r#"{"success":true,"code":0,"data":[
        {"symbol":"BTC_USDT","settleCoin":"USDT","state":0,"contractSize":0.0001,"takerFeeRate":0.0004}
    ]}"#;

    const TICKER_FIXTURE: &str = r#"{"success":true,"code":0,"data":[
        {"symbol":"BTC_USDT","fairPrice":81449.3,"indexPrice":81483.2,"amount24":2969311688.44834,
         "holdVol":531307174,"volume24":366642357}
    ]}"#;

    // 2026-09-20 批量 ticker 的真实片段：没有一档量，不能用 volume24 或 holdVol 顶替。
    const BOOK_FIXTURE: &str = r#"{"symbol":"BTC_USDT","bid1":80347.3,"ask1":80347.4,
        "volume24":247517798,"amount24":2009840383.8286,"holdVol":554731422,
        "fairPrice":80347.6,"indexPrice":80373.3,"timestamp":1789879712857}"#;

    fn funding_rows() -> Vec<FundingRow> {
        serde_json::from_str::<Envelope<FundingRow>>(FUNDING_FIXTURE)
            .unwrap()
            .into_data("资金费")
            .unwrap()
    }

    fn detail() -> Detail {
        serde_json::from_str::<Envelope<Detail>>(DETAIL_FIXTURE)
            .unwrap()
            .into_data("详情")
            .unwrap()
            .remove(0)
    }

    fn ticker() -> Ticker {
        serde_json::from_str::<Envelope<Ticker>>(TICKER_FIXTURE)
            .unwrap()
            .into_data("行情")
            .unwrap()
            .remove(0)
    }

    #[test]
    fn batch_book_prices_do_not_fabricate_depth_from_contract_volumes() {
        let ticker: Ticker = serde_json::from_str(BOOK_FIXTURE).unwrap();
        let row = funding_rows().remove(0);
        for detail in [Some(detail()), None] {
            let rate = parse_row(&row, detail.as_ref(), Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, Some(Decimal::new(803473, 1)));
            assert_eq!(rate.best_ask, Some(Decimal::new(803474, 1)));
            assert_eq!((rate.bid_size_usdt, rate.ask_size_usdt), (None, None));
        }
    }

    #[test]
    fn missing_book_does_not_fall_back_to_mark_price() {
        let row = funding_rows().remove(0);
        let ticker = ticker();
        for ticker in [Some(&ticker), None] {
            let rate = parse_row(&row, Some(&detail()), ticker).unwrap();
            assert_eq!(
                (
                    rate.best_bid,
                    rate.best_ask,
                    rate.bid_size_usdt,
                    rate.ask_size_usdt
                ),
                (None, None, None, None)
            );
        }
    }

    #[test]
    fn invalid_book_sides_are_unknown_and_crossed_books_are_discarded() {
        let row = funding_rows().remove(0);
        let mut ticker: Ticker = serde_json::from_str(BOOK_FIXTURE).unwrap();
        for invalid in [
            None,
            Some(0.0),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ] {
            ticker.bid1 = invalid;
            ticker.ask1 = Some(80347.4);
            let rate = parse_row(&row, None, Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, None);
            assert_eq!(rate.best_ask, Some(Decimal::new(803474, 1)));

            ticker.bid1 = Some(80347.3);
            ticker.ask1 = invalid;
            let rate = parse_row(&row, None, Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, Some(Decimal::new(803473, 1)));
            assert_eq!(rate.best_ask, None);
        }

        ticker.bid1 = Some(80347.5);
        ticker.ask1 = Some(80347.4);
        let rate = parse_row(&row, None, Some(&ticker)).unwrap();
        assert_eq!((rate.best_bid, rate.best_ask), (None, None));
        assert_eq!(rate.period_rate, Decimal::new(1, 4));

        // 锁盘不是交叉盘：确实相等的买卖价必须保留，不能抹掉真实的零价差。
        ticker.bid1 = ticker.ask1;
        let rate = parse_row(&row, None, Some(&ticker)).unwrap();
        assert_eq!(rate.best_bid, Some(Decimal::new(803474, 1)));
        assert_eq!(rate.best_ask, rate.best_bid);
    }

    #[test]
    fn business_failure_in_a_200_response_is_an_error_not_an_empty_result() {
        let envelope: Envelope<FundingRow> =
            serde_json::from_str(r#"{"success":false,"code":1001,"message":"error"}"#).unwrap();
        assert!(envelope.into_data("资金费").is_err());

        let empty: Envelope<FundingRow> =
            serde_json::from_str(r#"{"success":true,"code":0,"data":null}"#).unwrap();
        assert!(empty.into_data("资金费").unwrap().is_empty());
    }

    #[test]
    fn the_settlement_cycle_is_read_per_contract() {
        let rows = funding_rows();
        let detail = detail();
        let btc = parse_row(&rows[0], Some(&detail), None).unwrap();
        assert_eq!((btc.interval_h, btc.interval_assumed), (8, false));
        // 同一家场所内 1h 合约与 8h 合约并存 —— 这正是不能用固定周期的原因
        let cvc = parse_row(&rows[1], None, None).unwrap();
        assert_eq!((cvc.interval_h, cvc.interval_assumed), (1, false));
    }

    #[test]
    fn a_missing_or_implausible_cycle_falls_back_and_is_flagged() {
        let mut row = funding_rows().remove(0);
        for cycle in [None, Some(0), Some(-1), Some(48), Some(i64::MAX)] {
            row.collect_cycle = cycle;
            let rate = parse_row(&row, None, None).unwrap();
            assert_eq!(
                (rate.interval_h, rate.interval_assumed),
                (DEFAULT_FUNDING_INTERVAL_H, true),
                "cycle={cycle:?}"
            );
        }
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        let mut row = funding_rows().remove(0);
        row.funding_rate = None;
        assert!(parse_row(&row, None, None).is_none());

        row.funding_rate = Some(f64::NAN);
        assert!(parse_row(&row, None, None).is_none());

        // 真值 0 必须保留 —— 它和「没给」是两回事
        row.funding_rate = Some(0.0);
        assert_eq!(
            parse_row(&row, None, None).unwrap().period_rate,
            Decimal::ZERO
        );
    }

    #[test]
    fn an_invalid_settlement_time_drops_the_row() {
        let mut row = funding_rows().remove(0);
        for time in [None, Some(0), Some(-1), Some(i64::MAX)] {
            row.next_settle_time = time;
            assert!(parse_row(&row, None, None).is_none(), "time={time:?}");
        }
    }

    #[test]
    fn the_per_contract_taker_fee_is_used_verbatim() {
        let rows = funding_rows();
        let mut detail = detail();
        assert_eq!(
            parse_row(&rows[0], Some(&detail), None).unwrap().taker_fee,
            Some(Decimal::new(4, 4))
        );
        // 实测存在 0 费率的合约：已知为 0 不能退化成「不知道」
        detail.taker_fee_rate = Some(0.0);
        assert_eq!(
            parse_row(&rows[0], Some(&detail), None).unwrap().taker_fee,
            Some(Decimal::ZERO)
        );
        // 详情缺失 → 未知，由排名回落到配置值
        assert_eq!(parse_row(&rows[0], None, None).unwrap().taker_fee, None);
    }

    #[test]
    fn open_interest_converts_contracts_to_usdt() {
        let rate = parse_row(&funding_rows().remove(0), Some(&detail()), Some(&ticker())).unwrap();
        // 531307174 张 × 0.0001 BTC/张 × 81351.6 ≈ 4.32e9 USDT
        let oi = rate.open_interest_usdt.unwrap();
        assert!(
            oi > Decimal::new(4_320_000_000, 0) && oi < Decimal::new(4_330_000_000, 0),
            "{oi}"
        );

        // 缺面值时不能默认一张等于一个币
        let mut without_size = detail();
        without_size.contract_size = None;
        assert!(
            parse_row(
                &funding_rows().remove(0),
                Some(&without_size),
                Some(&ticker())
            )
            .unwrap()
            .open_interest_usdt
            .is_none()
        );
    }

    #[test]
    fn only_usdt_perps_are_kept() {
        assert!(is_usdt_perp("BTC_USDT"));
        assert!(is_usdt_perp("1000000BABYDOGE_USDT"), "倍数合约是正常的");
        assert!(!is_usdt_perp("BTC_USDC"));
        assert!(!is_usdt_perp("AVAX_USD"));
        assert!(!is_usdt_perp("HYPE_USD1"));
        assert!(!is_usdt_perp("BTCUSD"));
    }

    #[test]
    fn the_underscore_separator_is_stripped_from_the_base() {
        let rows = funding_rows();
        assert_eq!(
            parse_row(&rows[0], None, None).unwrap().symbol.to_string(),
            "BTC/USDT"
        );
        // 带数字前缀的倍数合约也要还原成完整 base，不能截断
        let mut row = rows[0].clone();
        row.symbol = "1000000BABYDOGE_USDT".into();
        assert_eq!(
            parse_row(&row, None, None).unwrap().symbol.to_string(),
            "1000000BABYDOGE/USDT"
        );
    }

    #[test]
    fn a_quote_asset_mismatch_filters_the_row() {
        // 详情说保证金币不是 USDT，而符号看着像 USDT → 口径不一致，不采信
        let mut detail = detail();
        detail.settle_coin = Some("USDC".into());
        let row = funding_rows().remove(0);
        let mismatch = detail
            .settle_coin
            .as_deref()
            .is_some_and(|coin| !coin.eq_ignore_ascii_case(QUOTE));
        assert!(mismatch);
        // 解析本身不受影响，过滤发生在 fetch_all 里
        assert!(parse_row(&row, Some(&detail), None).is_some());
    }

    #[test]
    fn depth_sorts_before_truncating_so_worse_levels_cannot_crowd_out_the_touch() {
        let depth = DepthData {
            bids: vec![[100.0, 1.0, 1.0], [103.0, 2.0, 1.0], [102.0, 1.0, 1.0]],
            asks: vec![[110.0, 1.0, 1.0], [104.0, 2.0, 1.0], [105.0, 1.0, 1.0]],
        };
        let book = build_book(&Symbol::perp("BTC", QUOTE), &depth, Decimal::ONE, 1).unwrap();
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.best_bid(), Some(Decimal::from(103)));
        assert_eq!(book.best_ask(), Some(Decimal::from(104)));
        // 张数 2 × 面值 1 × 价格 103。
        assert_eq!(book.bids[0].notional_usdt, Decimal::from(206));
    }
}
