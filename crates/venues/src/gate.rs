//! Gate USDT 线性永续。
//!
//! `/futures/usdt/contracts` 提供每期费率、周期、下次结算、公开吃单费率及价格。
//! `funding_interval` 的单位是秒（官方 SDK 的 Contract 文档明确说明），必须除以
//! 3600；`taker_fee_rate` 实测为 `"0.00075"`，不能用常见的 0.0005 替代。
//!
//! 持仓量用 `position_size`（官方定义：总多头张数）乘面值与标记价。`/tickers`
//! 的 `total_size` 实测约为其两倍，不能把双边持仓当成单边未平仓量。
//! `/contracts` 没有 24h 成交额，因此另外批量取 `/tickers.volume_24h_quote`。
//!
//! 一档盘口同样取自批量 `/tickers`：`highest_bid`/`highest_size` 与
//! `lowest_ask`/`lowest_size`。下单 `size` 是张数、`quanto_multiplier` 是每张的
//! 标的币数量，所以一档量按 `张数 × quanto_multiplier × 价` 换算成 USDT 名义。
//! 乘数取同一 ticker 行，让价、量、乘数同源；缺失或无效时只将对应字段报未知。
//! 交叉盘两侧均报未知，绝不拿 `total_size`（双边持仓）或标记价顶替盘口。
//!
//! 多档深度取逐合约的 `/futures/usdt/order_book?contract=BTC_USDT&limit=N`。期货的
//! 这一端点和现货不同：档位是 `{"p": "80444.7", "s": 24312}` 对象而不是数组，价格是
//! 字符串、**量是 JSON 整数张数且两侧都是正数**（不靠符号区分买卖方向）。
//! `limit` 实测上限 300（301 返回 400 `TOO_BIG`），而 `limit=0` 会被端点当成「未指定」
//! 回落默认 10 档 —— 所以档数要收敛到 1..=300，不能原样透传。
//! 面值取自单合约端点 `/futures/usdt/contracts/BTC_USDT`（只回一行，比拉全量便宜）。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Gate;
const CONTRACTS_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/contracts";
const TICKERS_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/tickers";
const ORDER_BOOK_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/order_book";
const CANDLESTICKS_URL: &str = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks";

/// `/futures/usdt/candlesticks` 接受的周期（分钟 → 字符串）。
const CANDLE_INTERVALS: [(u32, &str); 8] = [
    (1, "1m"),
    (5, "5m"),
    (15, "15m"),
    (30, "30m"),
    (60, "1h"),
    (240, "4h"),
    (480, "8h"),
    (1440, "1d"),
];

fn candle_interval(minutes: u32) -> ArbResult<&'static str> {
    CANDLE_INTERVALS
        .iter()
        .find(|(value, _)| *value >= minutes)
        .map(|(_, name)| *name)
        .ok_or_else(|| {
            ArbError::config(format!(
                "K 线周期 {minutes} 分钟超过该端点支持的最大周期（{}）",
                CANDLE_INTERVALS[CANDLE_INTERVALS.len() - 1].0
            ))
        })
}

/// Gate 的 K 线是对象：`{"t": 秒, "c": "收盘价", …}`。
#[derive(Debug, Deserialize)]
struct GateCandle {
    t: i64,
    c: String,
}

/// 收盘价缺失/不可解析/非正的跳过 —— 填 0 会造出一个 −100% 的假跳变。
fn parse_candles(rows: &[GateCandle]) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .iter()
        .filter_map(|row| {
            let close = parse_decimal(&row.c)?;
            if close <= Decimal::ZERO {
                return None;
            }
            Some(Candle {
                // 时间戳单位是**秒**，不是毫秒 —— 用错会得到 1970 年。
                open_time: DateTime::from_timestamp(row.t, 0)?,
                close,
            })
        })
        .collect();
    out.sort_by_key(|candle| candle.open_time);
    out
}

/// 端点实测上限：`limit=301` 返回 400 `TOO_BIG`（`"limit 300"`）。
const MAX_DEPTH_LEVELS: u32 = 300;

pub struct GateApi {
    client: Client,
}

impl GateApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 成交额不是资金费读数的前提；端点失败时明确报未知，不伪造零成交额或零盘口。
    /// 成交额与一档盘口出自同一批响应，一次请求取齐，不额外逐合约拉盘口。
    async fn fetch_quotes(&self) -> HashMap<String, Ticker> {
        let tickers: Vec<Ticker> = match get_json(self.client.get(TICKERS_URL), VENUE).await {
            Ok(tickers) => tickers,
            Err(error) => {
                warn!(venue = %VENUE, %error, "行情端点失败，24h 成交额与一档盘口按未知上报");
                return HashMap::new();
            }
        };
        tickers
            .into_iter()
            .map(|mut ticker| (std::mem::take(&mut ticker.contract), ticker))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct Contract {
    name: String,
    #[serde(rename = "type")]
    contract_direction: String,
    status: Option<String>,
    in_delisting: Option<bool>,
    // 缺失或 null 必须落到行级处理，不能让一条缺费率的记录拖垮整个批次。
    funding_rate: Option<String>,
    funding_interval: Option<i64>,
    funding_next_apply: Option<i64>,
    taker_fee_rate: Option<String>,
    mark_price: Option<String>,
    index_price: Option<String>,
    position_size: Option<i64>,
    quanto_multiplier: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Ticker {
    contract: String,
    volume_24h_quote: Option<String>,
    highest_bid: Option<String>,
    lowest_ask: Option<String>,
    highest_size: Option<String>,
    lowest_size: Option<String>,
    quanto_multiplier: Option<String>,
}

#[async_trait]
impl VenueApi for GateApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let contracts: Vec<Contract> = get_json(self.client.get(CONTRACTS_URL), VENUE).await?;
        let quotes = self.fetch_quotes().await;
        let mut out = Vec::with_capacity(contracts.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for item in contracts {
            if !is_linear_perp(&item) {
                filtered += 1;
                continue;
            }
            match parse_contract(&item, quotes.get(&item.name)) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非活跃 USDT 线性永续或杠杆标的已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        // 只用 HashMap 查找，不靠其迭代顺序；输出另外按符号固定顺序。
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }
    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        if levels == 0 {
            return Err(ArbError::config("levels 必须大于 0"));
        }
        // 实测该端点超过 300 档会直接报 TOO_BIG，所以在这里就钳住 ——
        // 让上层拿到一个明确的「最多 300 档」，而不是一个 400。
        let levels = levels.min(MAX_DEPTH_LEVELS);
        let native = format!("{}_USDT", symbol.base);

        let contract: Contract =
            get_json(self.client.get(format!("{CONTRACTS_URL}/{native}")), VENUE).await?;
        let multiplier = contract
            .quanto_multiplier
            .as_deref()
            .and_then(parse_decimal)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or_else(|| {
                ArbError::venue(
                    VENUE.as_str(),
                    format!("{native} 缺少合约面值，盘口无法换算成名义额"),
                )
            })?;

        let book: OrderBookResponse = get_json(
            self.client.get(format!(
                "{ORDER_BOOK_URL}?contract={native}&limit={levels}&with_id=false"
            )),
            VENUE,
        )
        .await?;

        build_book(symbol, &book, multiplier, levels)
    }

    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `/futures/usdt/candlesticks` 的周期是字符串枚举，时间戳是**秒**（不是毫秒）。
    /// 请求的分钟数向上取到最近的合法值：更细的序列会把半衰期算短。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let contract = format!("{}_USDT", symbol.base);
        let interval = candle_interval(interval_minutes)?;
        let url = format!(
            "{CANDLESTICKS_URL}?contract={contract}&interval={interval}&limit={}",
            limit.min(2000)
        );
        let rows: Vec<GateCandle> = get_json(self.client.get(&url), VENUE).await?;
        Ok(parse_candles(&rows))
    }
}

/// 公共接口报零与没有费率不同：前者保留，后者丢行；结算时刻也绝不拿当前时间顶上。
fn parse_contract(item: &Contract, ticker: Option<&Ticker>) -> Option<MarketSnapshot> {
    if !is_linear_perp(item) {
        return None;
    }
    let base = item.name.strip_suffix("_USDT")?;
    let period_rate = item.funding_rate.as_deref().and_then(parse_decimal)?;
    let next_apply = item.funding_next_apply.filter(|seconds| *seconds > 0)?;
    let next_funding_at = DateTime::from_timestamp(next_apply, 0)?;
    let (interval_h, interval_assumed) = funding_interval_hours(item.funding_interval);
    let mark_price = item.mark_price.as_deref().and_then(parse_decimal);

    // 张数 × 每张标的数量 × USDT 标记价，保持同一份 contracts 快照。
    // 缺乘数时不能默认一张等于一个币，溢出也只能报未知而不是崩溃。
    let open_interest_usdt = item
        .position_size
        .filter(|size| *size >= 0)
        .map(Decimal::from)
        .zip(item.quanto_multiplier.as_deref().and_then(parse_decimal))
        .zip(mark_price)
        .and_then(|((size, multiplier), price)| {
            if multiplier <= Decimal::ZERO || price <= Decimal::ZERO {
                return None;
            }
            size.checked_mul(multiplier)?.checked_mul(price)
        });

    let positive = |raw: Option<&str>| {
        raw.and_then(parse_decimal)
            .filter(|value| *value > Decimal::ZERO)
    };
    let mut best_bid = positive(ticker.and_then(|row| row.highest_bid.as_deref()));
    let mut best_ask = positive(ticker.and_then(|row| row.lowest_ask.as_deref()));
    // 交叉盘不能用于穿价成本，否则坏数据会被误当成收益；资金费与成交额仍保留。
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| ask < bid) {
        best_bid = None;
        best_ask = None;
    }
    let multiplier = positive(ticker.and_then(|row| row.quanto_multiplier.as_deref()));
    let notional = |raw: Option<&str>, price: Option<Decimal>| {
        // 张数允许小数；缺面值不能默认一张等于一个币，空档与溢出也不能伪造名义量。
        positive(raw)?.checked_mul(multiplier?)?.checked_mul(price?)
    };
    let bid_size_usdt = notional(ticker.and_then(|row| row.highest_size.as_deref()), best_bid);
    let ask_size_usdt = notional(ticker.and_then(|row| row.lowest_size.as_deref()), best_ask);
    let quote_volume_24h = ticker
        .and_then(|row| row.volume_24h_quote.as_deref())
        .and_then(parse_decimal);

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, "USDT"),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: item.taker_fee_rate.as_deref().and_then(parse_decimal),
        mark_price,
        index_price: item.index_price.as_deref().and_then(parse_decimal),
        best_bid,
        best_ask,
        bid_size_usdt,
        ask_size_usdt,
        open_interest_usdt,
        quote_volume_24h,
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 领域类型只能表示整小时；缺失、非正数或非整小时周期不能截断后冒充真值。
fn funding_interval_hours(seconds: Option<i64>) -> (u32, bool) {
    match seconds {
        Some(seconds) if seconds > 0 && seconds % 3600 == 0 => {
            match u32::try_from(seconds / 3600) {
                Ok(hours) => (hours, false),
                Err(_) => (DEFAULT_FUNDING_INTERVAL_H, true),
            }
        }
        _ => (DEFAULT_FUNDING_INTERVAL_H, true),
    }
}

/// 单个合约的盘口深度。
///
/// `/futures/usdt/order_book` 的 `s` 是**张数**，名义额是
/// `张数 × quanto_multiplier × price`（面值从单合约的 contracts 端点取，一次额外请求）。
/// 面值缺失时**整个盘口都换算不出来**，直接报错 —— 拿张数当美元会让滑点差几个数量级。

#[derive(Debug, Deserialize)]
struct OrderBookResponse {
    #[serde(default)]
    bids: Vec<GateLevel>,
    #[serde(default)]
    asks: Vec<GateLevel>,
}

/// Gate 的档位是 `{"p": "80330.1", "s": 27659}`：价格是字符串、张数是数字。
#[derive(Debug, Deserialize)]
struct GateLevel {
    p: String,
    s: i64,
}

/// 把深度响应转成盘口。空盘或交叉盘一律报错。
fn build_book(
    symbol: &Symbol,
    book: &OrderBookResponse,
    multiplier: Decimal,
    levels: u32,
) -> ArbResult<OrderBook> {
    let side = |rows: &[GateLevel]| -> Vec<Level> {
        let out: Vec<Level> = rows
            .iter()
            .filter(|level| level.s > 0)
            .filter_map(|level| {
                let price = parse_decimal(&level.p)?;
                if price <= Decimal::ZERO {
                    return None;
                }
                Some(Level {
                    price,
                    notional_usdt: Decimal::from(level.s) * multiplier * price,
                })
            })
            .collect();
        out
    };

    let mut bids = side(&book.bids);
    let mut asks = side(&book.asks);
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

/// 端点限定 USDT 保证金，type 再限定线性；不要把 USDC_USDT 的标的币误当保证金币。
/// 下架/暂停与杠杆标的不参与配对；只排明确的倍数后缀，不误伤 JUP、SYRUP、UP。
fn is_linear_perp(item: &Contract) -> bool {
    item.contract_direction == "direct"
        && item.status.as_deref() == Some("trading")
        && item.in_delisting != Some(true)
        && item.name.strip_suffix("_USDT").is_some_and(|base| {
            !base.is_empty()
                && !base.contains('_')
                && !["2L", "2S", "3L", "3S", "5L", "5S"]
                    .iter()
                    .any(|suffix| base.ends_with(suffix))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    // 来自真实 contracts 响应，只删去无关字段，保留原始值与 JSON 类型。
    const CONTRACTS_FIXTURE: &str = r#"[
        {"name":"BTC_USDT","type":"direct","status":"trading","in_delisting":false,
         "funding_rate":"0.0001","funding_interval":28800,"funding_next_apply":1789833600,
         "mark_price":"81241","index_price":"81273.55","taker_fee_rate":"0.00075",
         "position_size":328743040,"quanto_multiplier":"0.0001"},
        {"name":"CVC_USDT","type":"direct","status":"trading","in_delisting":false,
         "funding_rate":"-0.000379","funding_interval":3600,"funding_next_apply":1789826400,
         "mark_price":"0.02788","index_price":"0.028","taker_fee_rate":"0.00075",
         "position_size":579142,"quanto_multiplier":"10"},
        {"name":"OPENAI_USDT","type":"direct","status":"trading","in_delisting":false,
         "funding_rate":"0","funding_interval":28800,"funding_next_apply":1789833600,
         "mark_price":"1503.54","index_price":"1503.54","taker_fee_rate":"0.00075",
         "position_size":1600204,"quanto_multiplier":"0.01"}
    ]"#;

    // 两个端点分别采样，不假装它们是原子快照。
    const TICKER_FIXTURE: &str = r#"{"contract":"BTC_USDT","volume_24h_quote":"4963023340"}"#;

    fn contract(name: &str) -> Contract {
        let rows: Vec<Contract> = serde_json::from_str(CONTRACTS_FIXTURE).unwrap();
        rows.into_iter().find(|row| row.name == name).unwrap()
    }

    #[test]
    fn seconds_are_converted_to_hours_and_unknown_intervals_are_flagged() {
        for (seconds, expected) in [(3600, 1), (14400, 4), (28800, 8)] {
            let mut item = contract("BTC_USDT");
            item.funding_interval = Some(seconds);
            let rate = parse_contract(&item, None).unwrap();
            assert_eq!((rate.interval_h, rate.interval_assumed), (expected, false));
        }
        for seconds in [None, Some(0), Some(-3600), Some(5400), Some(i64::MAX)] {
            let mut item = contract("BTC_USDT");
            item.funding_interval = seconds;
            let rate = parse_contract(&item, None).unwrap();
            assert_eq!(
                (rate.interval_h, rate.interval_assumed),
                (DEFAULT_FUNDING_INTERVAL_H, true)
            );
        }
    }

    #[test]
    fn missing_null_empty_and_invalid_rates_drop_only_the_bad_row() {
        for value in [
            None,
            Some(serde_json::Value::Null),
            Some(serde_json::json!("")),
            Some(serde_json::json!("n/a")),
        ] {
            let mut rows: serde_json::Value = serde_json::from_str(CONTRACTS_FIXTURE).unwrap();
            let first = rows[0].as_object_mut().unwrap();
            match value {
                Some(value) => {
                    first.insert("funding_rate".into(), value);
                }
                None => {
                    first.remove("funding_rate");
                }
            }
            let rows: Vec<Contract> = serde_json::from_value(rows).unwrap();
            assert!(parse_contract(&rows[0], None).is_none());
            assert_eq!(
                parse_contract(&rows[1], None).unwrap().period_rate,
                Decimal::new(-379, 6)
            );
        }
        let zero = parse_contract(&contract("OPENAI_USDT"), None).unwrap();
        assert_eq!(zero.period_rate, Decimal::ZERO);
    }

    #[test]
    fn settlement_is_unix_seconds_and_invalid_times_drop_the_row() {
        let mut item = contract("BTC_USDT");
        let rate = parse_contract(&item, None).unwrap();
        assert_eq!(rate.next_funding_at.timestamp(), 1_789_833_600);
        assert!(!rate.next_funding_estimated);
        for time in [None, Some(0), Some(-1), Some(i64::MAX)] {
            item.funding_next_apply = time;
            assert!(parse_contract(&item, None).is_none());
        }
    }

    #[test]
    fn reported_taker_fee_is_not_replaced_by_a_default() {
        let mut item = contract("BTC_USDT");
        assert_eq!(
            parse_contract(&item, None).unwrap().taker_fee,
            Some(Decimal::new(75, 5))
        );
        item.taker_fee_rate = Some("0".into());
        assert_eq!(
            parse_contract(&item, None).unwrap().taker_fee,
            Some(Decimal::ZERO)
        );
        for fee in [None, Some(String::new()), Some("invalid".into())] {
            item.taker_fee_rate = fee;
            assert_eq!(parse_contract(&item, None).unwrap().taker_fee, None);
        }
    }

    #[test]
    fn linear_usdt_active_contracts_are_kept_without_confusing_base_and_margin() {
        let mut item = contract("BTC_USDT");
        for name in [
            "BTC_USDT",
            "USDC_USDT",
            "JUP_USDT",
            "SYRUP_USDT",
            "币安人生_USDT",
        ] {
            item.name = name.into();
            assert!(parse_contract(&item, None).is_some(), "{name}");
        }
        for name in [
            "BTC_USDC",
            "BTC_USD",
            "BTC_USDT_20260926",
            "BTC_20260926_USDT",
            "_USDT",
            "BTCUSDT",
            "CSOPSAMSUNG2L_USDT",
            "BTC3S_USDT",
        ] {
            item.name = name.into();
            assert!(parse_contract(&item, None).is_none(), "{name}");
        }
        item = contract("BTC_USDT");
        item.contract_direction = "inverse".into();
        assert!(parse_contract(&item, None).is_none());
        item = contract("BTC_USDT");
        item.in_delisting = Some(true);
        assert!(parse_contract(&item, None).is_none());
        item = contract("BTC_USDT");
        item.status = Some("delisted".into());
        assert!(parse_contract(&item, None).is_none());
    }

    #[test]
    fn single_sided_open_interest_uses_contract_multiplier_and_mark_price() {
        let rate = parse_contract(&contract("BTC_USDT"), None).unwrap();
        // 328743040 张 × 0.0001 BTC/张 × 81241 USDT/BTC = 2_670_741_331.264
        assert_eq!(
            rate.open_interest_usdt,
            Some(Decimal::new(26_707_413_312_640, 4))
        );
        let cvc = parse_contract(&contract("CVC_USDT"), None).unwrap();
        assert_eq!(cvc.open_interest_usdt, Some(Decimal::new(1_614_647_896, 4)));
        let mut item = contract("BTC_USDT");
        item.quanto_multiplier = None;
        assert_eq!(
            parse_contract(&item, None).unwrap().open_interest_usdt,
            None
        );
        item = contract("BTC_USDT");
        item.position_size = Some(0);
        assert_eq!(
            parse_contract(&item, None).unwrap().open_interest_usdt,
            Some(Decimal::ZERO)
        );
        item.position_size = Some(i64::MAX);
        item.quanto_multiplier = Some(Decimal::MAX.to_string());
        assert_eq!(
            parse_contract(&item, None).unwrap().open_interest_usdt,
            None
        );
    }

    #[test]
    fn volume_and_prices_keep_their_units_and_missing_volume_stays_unknown() {
        let ticker: Ticker = serde_json::from_str(TICKER_FIXTURE).unwrap();
        let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
        assert_eq!(rate.quote_volume_24h, Some(Decimal::new(4_963_023_340, 0)));
        assert_eq!(rate.mark_price, Some(Decimal::new(81_241, 0)));
        assert_eq!(rate.index_price, Some(Decimal::new(8_127_355, 2)));
        assert_eq!(
            parse_contract(&contract("BTC_USDT"), None)
                .unwrap()
                .quote_volume_24h,
            None
        );
    }

    // 来自同一次批量 tickers 响应；只去掉无关字段，不把张数或字符串改成别的单位。
    const BOOK_FIXTURE: &str = r#"[
        {"contract":"BTC_USDT","volume_24h_quote":"2897444911","total_size":"642640562",
         "highest_bid":"80358","highest_size":"85193","lowest_ask":"80358.1",
         "lowest_size":"2","quanto_multiplier":"0.0001"},
        {"contract":"PEPE_USDT","volume_24h_quote":"37343788","total_size":"2723178",
         "highest_bid":"0.000003998","highest_size":"653.2","lowest_ask":"0.000003999",
         "lowest_size":"354.5","quanto_multiplier":"10000000"}
    ]"#;

    fn book_ticker() -> Ticker {
        serde_json::from_str::<Vec<Ticker>>(BOOK_FIXTURE)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn book_sizes_convert_contracts_to_quote_notional_including_fractional_contracts() {
        let tickers: Vec<Ticker> = serde_json::from_str(BOOK_FIXTURE).unwrap();
        let btc = parse_contract(&contract("BTC_USDT"), Some(&tickers[0])).unwrap();
        assert_eq!(btc.best_bid, Some(Decimal::new(80_358, 0)));
        assert_eq!(btc.best_ask, Some(Decimal::new(803_581, 1)));
        // 85193 张 × 0.0001 BTC/张 × 80358 USDT/BTC，不能漏掉面值或拿持仓量代替。
        assert_eq!(btc.bid_size_usdt, Some(Decimal::new(6_845_939_094, 4)));
        assert_eq!(btc.ask_size_usdt, Some(Decimal::new(1_607_162, 5)));
        assert_eq!(btc.quote_volume_24h, Some(Decimal::from(2_897_444_911u64)));

        let mut item = contract("BTC_USDT");
        item.name = "PEPE_USDT".into();
        item.quanto_multiplier = Some("10000000".into());
        let pepe = parse_contract(&item, Some(&tickers[1])).unwrap();
        assert_eq!(pepe.best_bid, Some(Decimal::new(3998, 9)));
        assert_eq!(pepe.best_ask, Some(Decimal::new(3999, 9)));
        assert_eq!(pepe.bid_size_usdt, Some(Decimal::new(26_114_936, 3)));
        assert_eq!(pepe.ask_size_usdt, Some(Decimal::new(14_176_455, 3)));
    }

    #[test]
    fn missing_book_fields_and_missing_ticker_never_use_mark_price() {
        let ticker: Ticker = serde_json::from_str(TICKER_FIXTURE).unwrap();
        for ticker in [None, Some(&ticker)] {
            let rate = parse_contract(&contract("BTC_USDT"), ticker).unwrap();
            assert_eq!(
                (
                    rate.best_bid,
                    rate.best_ask,
                    rate.bid_size_usdt,
                    rate.ask_size_usdt
                ),
                (None, None, None, None)
            );
            assert_eq!(rate.mark_price, Some(Decimal::from(81_241)));
        }
        let mut ticker = book_ticker();
        ticker.highest_bid = None;
        ticker.lowest_size = None;
        let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
        assert_eq!(rate.best_bid, None);
        assert_eq!(rate.bid_size_usdt, None);
        assert_eq!(rate.best_ask, Some(Decimal::new(803_581, 1)));
        assert_eq!(rate.ask_size_usdt, None);
    }

    #[test]
    fn crossed_book_is_unknown_without_discarding_funding_or_volume() {
        let mut ticker = book_ticker();
        ticker.lowest_ask = Some("80357.9".into());
        let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
        assert_eq!(
            (
                rate.best_bid,
                rate.best_ask,
                rate.bid_size_usdt,
                rate.ask_size_usdt
            ),
            (None, None, None, None)
        );
        assert_eq!(rate.period_rate, Decimal::new(1, 4));
        assert_eq!(rate.quote_volume_24h, Some(Decimal::from(2_897_444_911u64)));
        ticker.lowest_ask = ticker.highest_bid.clone();
        let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
        assert_eq!(rate.best_ask, rate.best_bid);
        assert_eq!(rate.ask_size_usdt, Some(Decimal::new(160_716, 4)));
    }

    #[test]
    fn unavailable_or_overflowing_multiplier_keeps_prices_but_not_sizes() {
        for multiplier in [None, Some(""), Some("invalid"), Some("0"), Some("-1")] {
            let mut ticker = book_ticker();
            ticker.quanto_multiplier = multiplier.map(str::to_owned);
            let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, Some(Decimal::from(80_358)));
            assert_eq!(rate.best_ask, Some(Decimal::new(803_581, 1)));
            assert_eq!((rate.bid_size_usdt, rate.ask_size_usdt), (None, None));
        }
        let mut ticker = book_ticker();
        ticker.quanto_multiplier = Some(Decimal::MAX.to_string());
        let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
        assert_eq!((rate.bid_size_usdt, rate.ask_size_usdt), (None, None));
    }

    #[test]
    fn invalid_side_fields_do_not_erase_the_other_side() {
        for raw in [None, Some(""), Some("bad"), Some("0"), Some("-1")] {
            let mut ticker = book_ticker();
            ticker.highest_bid = raw.map(str::to_owned);
            let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
            assert_eq!((rate.best_bid, rate.bid_size_usdt), (None, None));
            assert_eq!(rate.best_ask, Some(Decimal::new(803_581, 1)));
            assert_eq!(rate.ask_size_usdt, Some(Decimal::new(1_607_162, 5)));

            let mut ticker = book_ticker();
            ticker.highest_size = raw.map(str::to_owned);
            let rate = parse_contract(&contract("BTC_USDT"), Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, Some(Decimal::from(80_358)));
            assert_eq!(rate.bid_size_usdt, None);
            assert_eq!(rate.ask_size_usdt, Some(Decimal::new(1_607_162, 5)));
        }
    }

    #[test]
    fn depth_sorts_before_truncating_so_worse_levels_cannot_crowd_out_the_touch() {
        let level = |price: &str, size: i64| GateLevel {
            p: price.into(),
            s: size,
        };
        let book = OrderBookResponse {
            bids: vec![level("100", 1), level("103", 2), level("102", 1)],
            asks: vec![level("110", 1), level("104", 2), level("105", 1)],
        };
        let parsed = build_book(&Symbol::perp("BTC", "USDT"), &book, Decimal::ONE, 1).unwrap();
        assert_eq!(parsed.bids.len(), 1);
        assert_eq!(parsed.asks.len(), 1);
        assert_eq!(parsed.best_bid(), Some(Decimal::from(103)));
        assert_eq!(parsed.best_ask(), Some(Decimal::from(104)));
        // 张数 2 × 面值 1 × 价格 103。
        assert_eq!(parsed.bids[0].notional_usdt, Decimal::from(206));
    }
}
