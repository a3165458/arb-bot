//! Bybit USDT 线性永续。
//!
//! 批量 tickers 自带 `fundingIntervalHour`（小时），不必再请求 instruments-info。
//! 后者的 `fundingInterval` 是分钟：BTCUSDT 的 480 分钟对应 tickers 的 `"8"`；
//! 0GUSDT 的 240 分钟对应 `"4"`，并已用历史结算时间差复核。
//!
//! `category=linear` 也含 USDC 永续（`BTCPERP`）和交割（`BTCUSDT-02OCT26`），
//! 必须排除。tickers 没有吃单费率，普通合约的 `/v5/account/fee-rate` 需要 apiKey，
//! 因此本连接器的 `taker_fee` 为 `None`。预上市 instruments-info 另有阶段费率，
//! 但它不是普通合约的通用费率，不能外推，也不能把交割手续费当成吃单费率。
//!
//! 批量 tickers 自带 `bid1Price`/`ask1Price`/`bid1Size`/`ask1Size`，无需逐合约请求。
//! USDT 线性合约的数量是标的币口径，一档名义量必须乘同侧最优价，不能直接当成 USDT。
//! 依据 Bybit《P&L Calculations (USDT Perpetual and Expiry Contracts)》的
//! `Contract value in USDT = Quantity × Price`，并用真实 tickers 与 orderbook 一档量交叉核实。
//! 缺价、缺量各自保留未知；交叉盘只清空盘口，不丢有效资金费，也不拿标记价冒充可成交价。
//!
//! 历史 K 线走 `/v5/market/kline?category=linear`，`interval` 是**分钟数的字符串**
//! （`60` 是 1 小时、`240` 是 4 小时），但合法取值是**枚举**而不是任意分钟数：
//! 实测 `2/4/6/10/90/1440/Y` 都被接受成 `retCode=0` + **空 list**，
//! 与「这个合约没有历史」无法区分，所以映射表必须来自实测（见 [`interval_to_bybit`]）。
//! 请求不支持的周期时向上取合法值：粗一点只是少几个采样点，
//! 细一点会让基差半衰期被算成一个更短的时间尺度。
//! 响应 `list` 每根是 `[start(ms), open, high, low, close, volume, turnover]` 且**降序**，
//! 解析后显式按 `open_time` 升序排序，不依赖上游顺序。

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Bybit;
const TICKERS_URL: &str = "https://api.bybit.com/v5/market/tickers?category=linear";
const KLINE_URL: &str = "https://api.bybit.com/v5/market/kline";

/// `/v5/market/kline` 接受的 `interval`（分钟 → 字符串）。
const KLINE_INTERVALS: [(u32, &str); 11] = [
    (1, "1"),
    (3, "3"),
    (5, "5"),
    (15, "15"),
    (30, "30"),
    (60, "60"),
    (120, "120"),
    (240, "240"),
    (360, "360"),
    (720, "720"),
    (1440, "D"),
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KlineResponse {
    ret_code: i32,
    ret_msg: String,
    result: Option<KlineResult>,
}

#[derive(Debug, Deserialize)]
struct KlineResult {
    list: Option<Vec<Vec<String>>>,
}

/// 每行是 `[start(ms), open, high, low, close, volume, turnover]`，**降序**。
/// 收盘价缺失/不可解析/非正的跳过 —— 填 0 会造出一个 −100% 的假跳变。
fn parse_klines(rows: &[Vec<String>]) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .iter()
        .filter_map(|row| {
            let open_ms: i64 = row.first()?.parse().ok()?;
            let close = parse_decimal(row.get(4)?)?;
            if close <= Decimal::ZERO {
                return None;
            }
            Some(Candle {
                open_time: DateTime::from_timestamp_millis(open_ms)?,
                close,
            })
        })
        .collect();
    // 实测降序，但不依赖它 —— 顺序反了半衰期会算成正数（看起来正常）。
    out.sort_by_key(|candle| candle.open_time);
    out
}
const DEPTH_URL: &str = "https://api.bybit.com/v5/market/orderbook";

/// 计价与保证金资产。linear 分类里的 USDC 合约与交割合约要排除，见文件头。
const QUOTE: &str = "USDT";

pub struct BybitApi {
    client: Client,
}

impl BybitApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// v5 的响应外壳。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickersResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    // 业务失败会返回 result: {}；成功响应却缺 list 时必须报错，不能默认为空市场。
    result: Option<TickersResult>,
}

#[derive(Debug, Deserialize)]
struct TickersResult {
    list: Option<Vec<Ticker>>,
}

/// 数值字段使用字符串以保留精度；缺字段只影响对应行，而不是整批行情。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker {
    symbol: String,
    mark_price: Option<String>,
    index_price: Option<String>,
    open_interest_value: Option<String>,
    #[serde(rename = "turnover24h")]
    turnover_24h: Option<String>,
    funding_rate: Option<String>,
    next_funding_time: Option<String>,
    funding_interval_hour: Option<String>,
    #[serde(rename = "bid1Price")]
    bid1_price: Option<String>,
    #[serde(rename = "ask1Price")]
    ask1_price: Option<String>,
    #[serde(rename = "bid1Size")]
    bid1_size: Option<String>,
    #[serde(rename = "ask1Size")]
    ask1_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DepthResponse {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    result: Option<DepthResult>,
}

#[derive(Debug, Deserialize)]
struct DepthResult {
    s: Option<String>,
    b: Option<Vec<[String; 2]>>,
    a: Option<Vec<[String; 2]>>,
}

#[async_trait]
impl VenueApi for BybitApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let response: TickersResponse = get_json(self.client.get(TICKERS_URL), VENUE).await?;
        let parsed = parse_all(checked_list(response)?);

        // 两种「少了一条」分开报：符号过滤是预期内的（USDC 保证金、交割合约），
        // 字段不可用则说明数据源变了或合约处于结算空窗期，必须能一眼区分。
        if parsed.filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered = parsed.filtered, "非 USDT 永续合约已过滤");
        }
        if parsed.unusable > 0 {
            crate::http::note_unusable(VENUE, parsed.unusable);
        }
        Ok(parsed.rates)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        let ticker = format!("{}{}", symbol.base, symbol.quote);
        if symbol.quote != QUOTE || !is_linear_perp(&ticker) {
            return Err(ArbError::venue(VENUE.as_str(), "盘口仅支持 USDT 线性永续"));
        }
        // 实测 0 会回落到默认 25 档，超过 1000 会静默截断；显式限制避免无意多拉。
        let limit = levels.clamp(1, 1000);
        let response: DepthResponse = get_json(
            self.client.get(DEPTH_URL).query(&[
                ("category", "linear"),
                ("symbol", ticker.as_str()),
                ("limit", limit.to_string().as_str()),
            ]),
            VENUE,
        )
        .await?;
        parse_depth(response, symbol, limit)
    }
    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `/v5/market/kline` 的 `interval` 是**分钟数字符串**（`60` = 1 小时、`240` = 4 小时），
    /// 不是 `1h` 这种写法。请求的分钟数向上取到最近的合法值：更细的序列会把半衰期
    /// 算短，而那个错误不会报错。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let interval = kline_interval(interval_minutes)?;
        let ticker = format!("{}{}", symbol.base, symbol.quote);
        let url = format!(
            "{KLINE_URL}?category=linear&symbol={ticker}&interval={interval}&limit={}",
            limit.min(1000)
        );
        let response: KlineResponse = get_json(self.client.get(&url), VENUE).await?;
        if response.ret_code != 0 {
            return Err(ArbError::venue(
                VENUE.as_str(),
                format!("K 线 retCode={}：{}", response.ret_code, response.ret_msg),
            ));
        }
        let rows = response
            .result
            .and_then(|result| result.list)
            .unwrap_or_default();
        Ok(parse_klines(&rows))
    }
}

/// `retCode != 0` → 错误。
///
/// 必须先判 `retCode` 再看 `result`：Bybit 用 `HTTP 200 + retCode != 0` 表达失败，
/// 而失败时的 `result` 要么是 `{}`、要么是 `list: []`。把它当空结果返回，会让
/// 「限频 / 参数错 / 维护中」全部伪装成「这家没有这些合约」，配对凭空消失而排名照出。
fn checked_list(response: TickersResponse) -> ArbResult<Vec<Ticker>> {
    if response.ret_code != 0 {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("retCode {}：{}", response.ret_code, response.ret_msg),
        ));
    }
    response
        .result
        .and_then(|result| result.list)
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "成功响应缺少 result.list"))
}

fn parse_depth(response: DepthResponse, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
    // 错误响应实测 result={}，不能将参数错误或限频伪装成空盘口。
    if response.ret_code != 0 {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("retCode {}：{}", response.ret_code, response.ret_msg),
        ));
    }
    let result = response
        .result
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "成功响应缺少 result"))?;
    let ticker = format!("{}{}", symbol.base, symbol.quote);
    if result.s.as_deref() != Some(ticker.as_str()) {
        return Err(ArbError::venue(VENUE.as_str(), "盘口合约与请求不符"));
    }
    let mut bids = parse_depth_side(result.b.unwrap_or_default());
    let mut asks = parse_depth_side(result.a.unwrap_or_default());
    // 下游按顺序吃单；即使上游当前有序，也不能依赖未校验的响应顺序。
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|a| a.price);
    bids.truncate(levels as usize);
    asks.truncate(levels as usize);
    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
        return Err(ArbError::venue(VENUE.as_str(), "盘口为空或无有效档位"));
    };
    if ask.price < bid.price {
        return Err(ArbError::venue(VENUE.as_str(), "盘口交叉"));
    }
    Ok(OrderBook {
        venue: VENUE,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

fn parse_depth_side(rows: Vec<[String; 2]>) -> Vec<Level> {
    rows.into_iter()
        .filter_map(|[price, size]| {
            // 深度与 tickers 一档实测一致，复用币数量×价；零量不是可成交档位。
            let (price, notional) = parse_book_side(Some(&price), Some(&size));
            Some(Level {
                price: price?,
                notional_usdt: notional.filter(|value| *value > Decimal::ZERO)?,
            })
        })
        .collect()
}

/// 一次批量响应的解析结果。两种「少了一条」分开计数，方便分别记日志。
#[derive(Debug, Default)]
struct Parsed {
    rates: Vec<MarketSnapshot>,
    /// 符号过滤掉的行数（预期内）。
    filtered: usize,
    /// 字段不可用而丢弃的行数（数据源异常）。
    unusable: usize,
}

/// 排序 → 过滤 → 逐行转换。
///
/// 先按符号排序，是因为返回顺序会进到下游配对，而 v5 的 `list` 顺序没有任何文档保证；
/// 显式排一次比「碰巧是字典序」可靠。
fn parse_all(mut list: Vec<Ticker>) -> Parsed {
    list.sort_by(|a, b| a.symbol.cmp(&b.symbol));

    let mut parsed = Parsed {
        rates: Vec::with_capacity(list.len()),
        ..Parsed::default()
    };
    for item in &list {
        if !is_linear_perp(&item.symbol) {
            parsed.filtered += 1;
            continue;
        }
        match parse_ticker_row(item) {
            Some(rate) => parsed.rates.push(rate),
            None => parsed.unusable += 1,
        }
    }
    parsed
}

/// 把一行 ticker 转成领域类型。`None` = 这一行不可用，**整行丢弃**。
///
/// 抽成纯函数是为了让「费率缺失」「结算时刻缺失」「周期回落」这三条最容易出错的规则
/// 能被单测覆盖，不必真的打网络。
///
/// 三处都**不猜**：
/// - 费率缺失/不可解析 → 丢弃。**绝不回落成 0**：0 是合法读数（Bybit 的预上市合约
///   真的报 `"0"`），伪造出来的 0 会凭空造出巨大价差。
/// - `nextFundingTime` 缺失或为 `"0"` → 丢弃。交割合约实测就是 `"0"`，交给
///   `from_timestamp_millis` 会得到一个 1970 年的合法时间戳，一路进到面板。
/// - 周期拿不到 → 回落到默认值并置 `interval_assumed = true`，让下游自己决定要不要用。
fn parse_ticker_row(item: &Ticker) -> Option<MarketSnapshot> {
    let base = item
        .symbol
        .strip_suffix(QUOTE)
        .filter(|base| !base.is_empty())?;

    let period_rate = parse_decimal(item.funding_rate.as_deref()?)?;
    let next_funding_millis = item
        .next_funding_time
        .as_deref()?
        .trim()
        .parse::<i64>()
        .ok()?;
    let next_funding_at =
        DateTime::from_timestamp_millis(next_funding_millis).filter(|_| next_funding_millis > 0)?;

    let (interval_h, interval_assumed) = match item
        .funding_interval_hour
        .as_deref()
        .unwrap_or("")
        .trim()
        .parse::<u32>()
    {
        Ok(hours) if hours > 0 => (hours, false),
        // 空串与非数字都走这里，两者都是「场所没给」。
        _ => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    let (mut best_bid, mut bid_size_usdt) =
        parse_book_side(item.bid1_price.as_deref(), item.bid1_size.as_deref());
    let (mut best_ask, mut ask_size_usdt) =
        parse_book_side(item.ask1_price.as_deref(), item.ask1_size.as_deref());
    // 交叉盘会制造负穿价成本；只清空盘口，避免连有效资金费一起丢掉。
    if matches!((best_bid, best_ask), (Some(bid), Some(ask)) if ask < bid) {
        best_bid = None;
        best_ask = None;
        bid_size_usdt = None;
        ask_size_usdt = None;
    }

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        // Bybit 直接给下一次结算的毫秒时间戳，不需要按周期推算。
        next_funding_estimated: false,
        // 当前批量行情没有吃单费率，未知不等于零。
        taker_fee: None,
        mark_price: item.mark_price.as_deref().and_then(parse_decimal),
        index_price: item.index_price.as_deref().and_then(parse_decimal),
        best_bid,
        best_ask,
        bid_size_usdt,
        ask_size_usdt,
        // 已限定 USDT 合约，直接取场所给出的计价币持仓价值，避免二次相乘的舍入差异。
        open_interest_usdt: item.open_interest_value.as_deref().and_then(parse_decimal),
        // `turnover24h` 是计价币口径的成交额；`volume24h` 是币口径，不能混。
        quote_volume_24h: item.turnover_24h.as_deref().and_then(parse_decimal),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 币数量乘同侧最优价才是计价币名义量；缺价或乘法溢出时不能退回原始币数量。
fn parse_book_side(price: Option<&str>, size: Option<&str>) -> (Option<Decimal>, Option<Decimal>) {
    let price = price
        .and_then(parse_decimal)
        .filter(|price| *price > Decimal::ZERO);
    let notional = size
        .and_then(parse_decimal)
        .filter(|size| *size >= Decimal::ZERO)
        .zip(price)
        .and_then(|(size, price)| size.checked_mul(price));
    (price, notional)
}

/// 是不是 USDT 保证金的线性永续。
///
/// `category=linear` 里混着 USDC 永续（`BTCPERP`）与交割合约（`BTCUSDT-02OCT26`）：
/// 前者保证金资产不同，后者的资金费机制与永续不同，混进来都会被拿去配对。
fn is_linear_perp(symbol: &str) -> bool {
    symbol.ends_with(QUOTE)
        && !symbol.contains('-')
        && !symbol.contains('_')
        && symbol.len() > QUOTE.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, estimate_fill};

    // 真实 limit=3 响应保留价量原值；与紧随其后的 tickers 买一价量完全一致。
    const REAL_DEPTH: &str = r#"{
      "retCode":0,"retMsg":"OK","result":{
        "s":"BTCUSDT",
        "a":[["80427.90","1.404"],["80428.00","0.012"],["80428.10","0.002"]],
        "b":[["80427.80","2.992"],["80427.70","0.035"],["80427.50","0.006"]],
        "ts":1789898084003,"u":25547100,"seq":812890340709,"cts":1789898084001
      },"retExtInfo":{},"time":1789898084048
    }"#;

    fn depth(raw: &str, levels: u32) -> ArbResult<OrderBook> {
        parse_depth(
            serde_json::from_str(raw).unwrap(),
            &Symbol::perp("BTC", "USDT"),
            levels,
        )
    }

    #[test]
    fn depth_maps_base_quantity_to_quote_notional_and_estimates_fills() {
        let book = depth(REAL_DEPTH, 3).unwrap();
        assert_eq!(book.venue, Venue::Bybit);
        assert_eq!(book.symbol, Symbol::perp("BTC", "USDT"));
        assert_eq!(book.best_bid(), parse_decimal("80427.80"));
        assert_eq!(book.best_ask(), parse_decimal("80427.90"));
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("240639.97760").unwrap()
        );
        assert_eq!(
            book.asks[0].notional_usdt,
            parse_decimal("112920.77160").unwrap()
        );
        assert_eq!(
            book.bids[2].notional_usdt,
            parse_decimal("482.56500").unwrap()
        );
        assert_eq!(
            book.asks[2].notional_usdt,
            parse_decimal("160.85620").unwrap()
        );

        // 吃穿全部档位，具体成交额能同时防住币数量误当美元与漏档。
        for (side, expected) in [(Side::Buy, "114046.76380"), (Side::Sell, "243937.51210")] {
            let fill =
                estimate_fill(book.side(side), parse_decimal("1000000").unwrap(), side).unwrap();
            assert_eq!(fill.filled_usdt, parse_decimal(expected).unwrap());
            assert!(fill.exhausted);
            assert!(fill.slippage > Decimal::ZERO);
            match side {
                Side::Buy => assert!(fill.average_price > book.best_ask().unwrap()),
                Side::Sell => assert!(fill.average_price < book.best_bid().unwrap()),
            }
        }
    }

    #[test]
    fn depth_sorts_before_truncating_to_the_requested_count() {
        let mut raw: serde_json::Value = serde_json::from_str(REAL_DEPTH).unwrap();
        for side in ["b", "a"] {
            raw["result"][side].as_array_mut().unwrap().reverse();
        }
        let book = depth(&raw.to_string(), 2).unwrap();
        let bids: Vec<_> = book.bids.iter().map(|level| level.price).collect();
        let asks: Vec<_> = book.asks.iter().map(|level| level.price).collect();
        assert_eq!(
            bids,
            [
                parse_decimal("80427.80").unwrap(),
                parse_decimal("80427.70").unwrap()
            ]
        );
        assert_eq!(
            asks,
            [
                parse_decimal("80427.90").unwrap(),
                parse_decimal("80428.00").unwrap()
            ]
        );
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("240639.97760").unwrap()
        );
    }

    #[test]
    fn depth_rejects_empty_crossed_and_wrong_symbol_books() {
        for side in ["b", "a"] {
            let mut raw: serde_json::Value = serde_json::from_str(REAL_DEPTH).unwrap();
            raw["result"][side] = serde_json::json!([]);
            assert!(depth(&raw.to_string(), 3).is_err());
            raw["result"].as_object_mut().unwrap().remove(side);
            assert!(depth(&raw.to_string(), 3).is_err());
        }
        let mut raw: serde_json::Value = serde_json::from_str(REAL_DEPTH).unwrap();
        raw["result"]["a"][2][0] = serde_json::json!("80427.70");
        assert!(depth(&raw.to_string(), 3).is_err());
        // 相等仍是合法零价差，不能把边界也误判成交叉盘。
        raw["result"]["a"][2][0] = serde_json::json!("80427.80");
        assert!(depth(&raw.to_string(), 3).is_ok());
        raw["result"]["s"] = serde_json::json!("ETHUSDT");
        assert!(depth(&raw.to_string(), 3).is_err());
    }

    #[test]
    fn depth_preserves_business_errors_and_rejects_missing_payloads() {
        let error = depth(
            r#"{"retCode":10001,"retMsg":"params error: symbol invalid","result":{},"retExtInfo":{},"time":1789898100960}"#,
            3,
        )
        .unwrap_err();
        assert!(error.to_string().contains("10001"));
        for raw in [
            r#"{"retCode":0,"result":{}}"#,
            r#"{"retCode":0}"#,
            r#"{"retCode":10006,"retMsg":"Too many visits","result":null}"#,
        ] {
            assert!(depth(raw, 3).is_err());
        }
    }

    #[test]
    fn depth_never_substitutes_unconvertible_or_zero_quantities() {
        for invalid in ["", "n/a", "-1", "0", "79228162514264337593543950335"] {
            let mut raw: serde_json::Value = serde_json::from_str(REAL_DEPTH).unwrap();
            raw["result"]["b"][0][1] = serde_json::json!(invalid);
            let book = depth(&raw.to_string(), 3).unwrap();
            assert_eq!(book.best_bid(), parse_decimal("80427.70"));
            assert_eq!(book.bids.len(), 2);
            assert_eq!(
                book.bids[0].notional_usdt,
                parse_decimal("2814.96950").unwrap()
            );
        }
        let mut raw: serde_json::Value = serde_json::from_str(REAL_DEPTH).unwrap();
        raw["result"]["b"] = serde_json::json!([["0", "1"], ["invalid", "3"]]);
        assert!(depth(&raw.to_string(), 3).is_err());
    }

    /// 本次批量 tickers 的 BTCUSDT 原始片段；单独保留，避免改动既有资金费 fixture 的时点。
    const REAL_BOOK_TICKER: &str = r#"{
      "symbol":"BTCUSDT","indexPrice":"80363.66","markPrice":"80328.27",
      "openInterestValue":"4478004400.20","turnover24h":"2716974422.0735",
      "fundingRate":"0.0001","nextFundingTime":"1789891200000","fundingIntervalHour":"8",
      "bid1Price":"80327.50","ask1Price":"80327.60","bid1Size":"0.317","ask1Size":"5.774"
    }"#;

    fn book_ticker() -> Ticker {
        serde_json::from_str(REAL_BOOK_TICKER).unwrap()
    }

    #[test]
    fn real_book_prices_and_base_sizes_map_to_usdt_notional() {
        let rate = parse_ticker_row(&book_ticker()).unwrap();
        assert_eq!(rate.best_bid, parse_decimal("80327.50"));
        assert_eq!(rate.best_ask, parse_decimal("80327.60"));
        // 同侧价格乘币数量：0.317 BTC 与 5.774 BTC，不能误填成 USDT。
        assert_eq!(rate.bid_size_usdt, parse_decimal("25463.81750"));
        assert_eq!(rate.ask_size_usdt, parse_decimal("463811.56240"));
    }

    #[test]
    fn missing_book_fields_do_not_use_mark_price_or_zero() {
        for explicit_null in [false, true] {
            let mut raw: serde_json::Value = serde_json::from_str(REAL_BOOK_TICKER).unwrap();
            for key in ["bid1Price", "ask1Price", "bid1Size", "ask1Size"] {
                if explicit_null {
                    raw[key] = serde_json::Value::Null;
                } else {
                    raw.as_object_mut().unwrap().remove(key);
                }
            }
            let row: Ticker = serde_json::from_value(raw).unwrap();
            let rate = parse_ticker_row(&row).unwrap();
            assert_eq!(rate.best_bid, None);
            assert_eq!(rate.best_ask, None);
            assert_eq!(rate.bid_size_usdt, None);
            assert_eq!(rate.ask_size_usdt, None);
            assert_eq!(rate.mark_price, parse_decimal("80328.27"));
            assert_eq!(rate.period_rate, parse_decimal("0.0001").unwrap());
        }
    }

    #[test]
    fn unavailable_price_prevents_notional_but_missing_size_preserves_price() {
        let mut row = book_ticker();
        row.bid1_price = None;
        row.ask1_size = None;
        let rate = parse_ticker_row(&row).unwrap();
        assert_eq!(rate.best_bid, None);
        assert_eq!(rate.bid_size_usdt, None);
        assert_eq!(rate.best_ask, parse_decimal("80327.60"));
        assert_eq!(rate.ask_size_usdt, None);

        for invalid in ["", "invalid", "0", "-1"] {
            let mut row = book_ticker();
            row.bid1_price = Some(invalid.into());
            let rate = parse_ticker_row(&row).unwrap();
            assert_eq!(rate.best_bid, None);
            assert_eq!(rate.bid_size_usdt, None);
            assert_eq!(rate.ask_size_usdt, parse_decimal("463811.56240"));
        }
        for invalid in ["", "invalid", "-1"] {
            let mut row = book_ticker();
            row.ask1_size = Some(invalid.into());
            let rate = parse_ticker_row(&row).unwrap();
            assert_eq!(rate.best_ask, parse_decimal("80327.60"));
            assert_eq!(rate.ask_size_usdt, None);
        }
    }

    #[test]
    fn crossed_book_drops_only_book_and_equal_prices_remain_usable() {
        let mut row = book_ticker();
        row.ask1_price = Some("80327.40".into());
        let rate = parse_ticker_row(&row).unwrap();
        assert_eq!(rate.best_bid, None);
        assert_eq!(rate.best_ask, None);
        assert_eq!(rate.bid_size_usdt, None);
        assert_eq!(rate.ask_size_usdt, None);
        assert_eq!(rate.period_rate, parse_decimal("0.0001").unwrap());

        row.ask1_price = row.bid1_price.clone();
        let rate = parse_ticker_row(&row).unwrap();
        assert_eq!(rate.best_bid, parse_decimal("80327.50"));
        assert_eq!(rate.best_ask, parse_decimal("80327.50"));
        assert_eq!(rate.ask_size_usdt, parse_decimal("463810.98500"));
    }

    #[test]
    fn zero_size_is_known_but_overflow_is_unknown() {
        let mut row = book_ticker();
        row.bid1_size = Some("0".into());
        row.ask1_size = Some(Decimal::MAX.to_string());
        let rate = parse_ticker_row(&row).unwrap();
        assert_eq!(rate.bid_size_usdt, Some(Decimal::ZERO));
        assert_eq!(rate.ask_size_usdt, None);
        assert_eq!(rate.best_ask, parse_decimal("80327.60"));
    }

    /// 真实响应片段（2026-09-19 抓取，字段值原样保留，只删掉连接器用不到的键）。
    /// 五行覆盖：4h 永续、8h 永续、USDC 永续、交割合约、费率恰为 `"0"` 的预上市永续。
    const REAL_TICKERS: &str = r#"{
      "retCode": 0,
      "retMsg": "OK",
      "result": {
        "category": "linear",
        "list": [
          {"symbol":"0GUSDT","lastPrice":"0.2223","indexPrice":"0.2222","markPrice":"0.2223","openInterest":"10016488.2","openInterestValue":"2226665.33","turnover24h":"2100963.3153","volume24h":"9725606.0000","fundingRate":"0.00005","nextFundingTime":"1789833600000","deliveryTime":"0","fundingIntervalHour":"4"},
          {"symbol":"BTCUSDT","lastPrice":"81264.70","indexPrice":"81294.13","markPrice":"81264.70","openInterest":"57519.325","openInterestValue":"4674290690.33","turnover24h":"5819942674.0481","volume24h":"72158.3590","fundingRate":"0.0001","nextFundingTime":"1789833600000","deliveryTime":"0","fundingIntervalHour":"8"},
          {"symbol":"BTCPERP","lastPrice":"81251.70","indexPrice":"81276.45","markPrice":"81251.70","openInterest":"1363.268","openInterestValue":"110767842.56","turnover24h":"59348268.1638","volume24h":"735.5800","fundingRate":"0.0001","nextFundingTime":"1789833600000","deliveryTime":"0","fundingIntervalHour":"8"},
          {"symbol":"BTCUSDT-02OCT26","lastPrice":"81365.1","indexPrice":"81294.1","markPrice":"81525.3","openInterest":"27.204","openInterestValue":"2217814.26","turnover24h":"222781.9683","volume24h":"2.7390","fundingRate":"","nextFundingTime":"0","deliveryTime":"1790928000000","fundingIntervalHour":""},
          {"symbol":"OPENAIUSDT","lastPrice":"1551.72","indexPrice":"1546.21","markPrice":"1549.71","openInterest":"445.818","openInterestValue":"690888.61","turnover24h":"85042.3034","volume24h":"55.0930","fundingRate":"0","nextFundingTime":"1789833600000","deliveryTime":"0","fundingIntervalHour":"8"}
        ]
      }
    }"#;

    fn response(raw: &str) -> TickersResponse {
        serde_json::from_str(raw).expect("fixture 必须是真实响应的形状")
    }

    fn ticker(symbol: &str) -> Ticker {
        checked_list(response(REAL_TICKERS))
            .unwrap()
            .into_iter()
            .find(|item| item.symbol == symbol)
            .expect("fixture 里应当有这个合约")
    }

    #[test]
    fn only_usdt_margined_perps_survive_the_real_payload() {
        let parsed = parse_all(checked_list(response(REAL_TICKERS)).unwrap());

        let symbols: Vec<String> = parsed
            .rates
            .iter()
            .map(|rate| rate.symbol.to_string())
            .collect();
        assert_eq!(symbols, ["0G/USDT", "BTC/USDT", "OPENAI/USDT"]);
        assert_eq!(
            parsed.filtered, 2,
            "BTCPERP 是 USDC 保证金，BTCUSDT-02OCT26 是交割"
        );
        assert_eq!(parsed.unusable, 0, "保留下来的行都应当可用");
    }

    #[test]
    fn usdc_perps_and_delivery_futures_are_not_linear_usdt_perps() {
        assert!(is_linear_perp("BTCUSDT"));
        assert!(!is_linear_perp("BTCPERP"), "USDC 保证金永续");
        assert!(!is_linear_perp("BTCUSDT-02OCT26"), "交割合约");
        assert!(!is_linear_perp("USDT"), "没有 base");
    }

    #[test]
    fn a_real_row_maps_every_reported_field() {
        let rate = parse_ticker_row(&ticker("BTCUSDT")).unwrap();

        assert_eq!(rate.venue, Venue::Bybit);
        assert_eq!(rate.symbol, Symbol::perp("BTC", "USDT"));
        assert_eq!(rate.period_rate, parse_decimal("0.0001").unwrap());
        assert_eq!(rate.next_funding_at.timestamp_millis(), 1_789_833_600_000);
        assert!(!rate.next_funding_estimated, "时间戳是场所给的，不是推算的");
        assert_eq!(rate.mark_price, parse_decimal("81264.70"));
        assert_eq!(rate.index_price, parse_decimal("81294.13"));
        assert_eq!(rate.open_interest_usdt, parse_decimal("4674290690.33"));
        assert_eq!(rate.quote_volume_24h, parse_decimal("5819942674.0481"));
    }

    #[test]
    fn the_reported_interval_is_hours_and_absence_is_flagged_as_assumed() {
        // 4h 合约：`fundingIntervalHour` 直接就是小时数，不做任何换算。
        let four_hour = parse_ticker_row(&ticker("0GUSDT")).unwrap();
        assert_eq!(four_hour.interval_h, 4);
        assert!(!four_hour.interval_assumed, "场所给了周期就不能标记为假设");

        for broken in ["", "0", "abc", "4.5"] {
            let mut row = ticker("BTCUSDT");
            row.funding_interval_hour = Some(broken.into());
            let rate = parse_ticker_row(&row).unwrap();
            assert_eq!(
                rate.interval_h, DEFAULT_FUNDING_INTERVAL_H,
                "周期={broken:?}"
            );
            assert!(rate.interval_assumed, "回落值必须标记出来：{broken:?}");
        }
    }

    #[test]
    fn a_missing_or_unparseable_rate_drops_the_row_instead_of_faking_zero() {
        let mut empty = ticker("BTCUSDT");
        empty.funding_rate = None;
        assert!(parse_ticker_row(&empty).is_none(), "空费率必须整行丢弃");

        let mut junk = ticker("BTCUSDT");
        junk.funding_rate = Some("n/a".into());
        assert!(parse_ticker_row(&junk).is_none());

        // 反过来：场所真的报 0 是合法读数，必须留下（OPENAIUSDT 实测就是 "0"）。
        let zero = parse_ticker_row(&ticker("OPENAIUSDT")).unwrap();
        assert_eq!(zero.period_rate, parse_decimal("0").unwrap());
    }

    #[test]
    fn a_zero_or_unparseable_settlement_time_drops_the_row() {
        // 交割合约实测 nextFundingTime = "0"：直接交给 from_timestamp_millis 会得到
        // 一个 1970 年的合法时间戳，所以必须显式丢弃。
        assert_eq!(
            ticker("BTCUSDT-02OCT26").next_funding_time.as_deref(),
            Some("0")
        );

        for broken in [
            "0",
            "",
            "abc",
            "99999999999999999999",
            "9223372036854775807",
        ] {
            let mut row = ticker("BTCUSDT");
            row.next_funding_time = Some(broken.into());
            assert!(parse_ticker_row(&row).is_none(), "结算时刻={broken:?}");
        }
    }

    #[test]
    fn a_nonzero_ret_code_is_an_error_not_an_empty_result() {
        // 真实失败响应：HTTP 200，result 里连 list 键都没有。
        let illegal = response(
            r#"{"retCode":10001,"retMsg":"Illegal category","result":{},"retExtInfo":{},"time":1789824034328}"#,
        );
        let error = checked_list(illegal).expect_err("retCode != 0 必须是错误");
        let text = error.to_string();
        assert!(text.contains("10001"), "{text}");
        assert!(text.contains("Illegal category"), "{text}");

        // 另一种失败：result.list 存在但为空。同样不能当成「这家没合约」。
        let invalid = response(
            r#"{"retCode":10001,"retMsg":"params error: symbol invalid","result":{"category":"","list":[],"nextPageCursor":""},"retExtInfo":{},"time":1789824034952}"#,
        );
        assert!(checked_list(invalid).is_err());

        assert_eq!(checked_list(response(REAL_TICKERS)).unwrap().len(), 5);
    }

    #[test]
    fn taker_fee_stays_unknown_because_the_public_payload_has_no_fee_field() {
        let parsed = parse_all(checked_list(response(REAL_TICKERS)).unwrap());
        assert!(
            parsed.rates.iter().all(|rate| rate.taker_fee.is_none()),
            "拿不到就必须是 None，不能填猜测值"
        );
    }

    #[test]
    fn absent_market_fields_stay_none_instead_of_becoming_zero() {
        let mut row = ticker("BTCUSDT");
        row.mark_price = None;
        row.turnover_24h = None;
        let rate = parse_ticker_row(&row).unwrap();

        assert_eq!(rate.mark_price, None);
        assert_eq!(rate.quote_volume_24h, None);
        assert_eq!(
            rate.open_interest_usdt,
            parse_decimal("4674290690.33"),
            "缺字段不应影响同一行的其它字段"
        );
    }

    #[test]
    fn missing_json_fields_only_affect_the_corresponding_reading() {
        for key in ["fundingRate", "nextFundingTime"] {
            for explicit_null in [false, true] {
                let mut raw: serde_json::Value = serde_json::from_str(REAL_TICKERS).unwrap();
                let row = raw["result"]["list"][0].as_object_mut().unwrap();
                if explicit_null {
                    row.insert(key.into(), serde_json::Value::Null);
                } else {
                    row.remove(key);
                }
                let parsed = parse_all(checked_list(response(&raw.to_string())).unwrap());
                let symbols: Vec<_> = parsed
                    .rates
                    .iter()
                    .map(|rate| rate.symbol.base.as_str())
                    .collect();
                assert_eq!(symbols, ["BTC", "OPENAI"]);
                assert_eq!(parsed.unusable, 1);
            }
        }

        let mut raw: serde_json::Value = serde_json::from_str(REAL_TICKERS).unwrap();
        raw["result"]["list"][0]
            .as_object_mut()
            .unwrap()
            .remove("fundingIntervalHour");
        let parsed = parse_all(checked_list(response(&raw.to_string())).unwrap());
        assert_eq!(parsed.rates[0].interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(parsed.rates[0].interval_assumed);
    }

    #[test]
    fn a_missing_success_payload_is_not_an_empty_market() {
        for raw in [
            r#"{"retCode":0,"retMsg":"OK"}"#,
            r#"{"retCode":0,"retMsg":"OK","result":{}}"#,
        ] {
            assert!(checked_list(response(raw)).is_err());
        }
        assert!(
            checked_list(response(
                r#"{"retCode":0,"retMsg":"OK","result":{"list":[]}}"#
            ))
            .unwrap()
            .is_empty()
        );
    }
}
