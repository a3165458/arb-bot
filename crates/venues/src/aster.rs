//! Aster（DEX）USDT 永续。
//!
//! 六个端点配合：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/fapi/v1/premiumIndex` | 每期费率、标记价、指数价、下次结算时刻 | **结算周期**、合约是否还在交易 |
//! | `/fapi/v1/fundingInfo` | 每个合约的 `fundingIntervalHours` | 费率 |
//! | `/fapi/v1/exchangeInfo` | 合约类型、交易状态、保证金资产 | 费率 |
//! | `/fapi/v1/ticker/bookTicker` | 全合约的一档最优买卖价与一档量 | 24h 成交额、持仓量 |
//! | `/fapi/v1/depth` | **逐合约**的多档深度 | 批量版本；`limit` 只认 5/10/20/50/100/500/1000 |
//! | `/fapi/v1/klines` | **逐合约**的历史收盘价 | 批量版本；`interval` 只认 15 个离散值，`limit` 上限 1500 |
//!
//! `premiumIndex` 还会返回不在可交易合约列表里的读数：实测 APXUSDT 仍有正常的
//! 费率与结算时刻，但 K 线接口已报 `Invalid symbol`。因此必须用 `exchangeInfo`
//! 确认类型、交易状态和保证金资产，不能只凭 USDT 后缀接受报价。
//!
//! 一档盘口取自不带 `symbol` 的批量 `bookTicker`。`bidQty`/`askQty` 是标的币数量，
//! 必须乘本方价格换成 USDT 名义。盘口端点失败、字段缺失或交叉盘（`ask < bid`）时，
//! 四个盘口字段留空但保留资金费读数，绝不用标记价冒充最优价，否则会伪造零穿价成本。
//!
//! 深度端点的档位量与 `bookTicker` 是同一套单位（实测同一次抓取里逐位相同），因此同样
//! 是**标的币数量**，乘本方价格即 USDT 名义。`exchangeInfo` 里没有 `contractSize`
//! （实测 602 个合约零命中），数量不需要再乘面值。注意 `1000PEPEUSDT` 这类合约的
//! `baseAsset` 本身就是 `1000PEPE`：「标的币」由场所定义，不能拿符号名去猜倍数。
//!
//! 周期取自 `fundingInfo`，缺失时明确标记默认值；吃单费率接口 `commissionRate`
//! 实测要求 API key，文档费率还区分合约类别及 ASTER 抵扣，故 `taker_fee = None`。

use std::collections::{HashMap, HashSet};

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Aster;
const PREMIUM_INDEX_URL: &str = "https://fapi.asterdex.com/fapi/v1/premiumIndex";
const FUNDING_INFO_URL: &str = "https://fapi.asterdex.com/fapi/v1/fundingInfo";
const EXCHANGE_INFO_URL: &str = "https://fapi.asterdex.com/fapi/v1/exchangeInfo";
const KLINE_URL: &str = "https://fapi.asterdex.com/fapi/v1/klines";

/// `/fapi/v1/klines` 接受的周期（分钟 → 字符串）。
const KLINE_INTERVALS: [(u32, &str); 12] = [
    (1, "1m"),
    (3, "3m"),
    (5, "5m"),
    (15, "15m"),
    (30, "30m"),
    (60, "1h"),
    (120, "2h"),
    (240, "4h"),
    (360, "6h"),
    (480, "8h"),
    (720, "12h"),
    (1440, "1d"),
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

/// 每行是 `[openTime(ms), o, h, l, c, v, …]`。收盘价缺失/非正的跳过。
fn parse_klines(rows: &[Vec<serde_json::Value>]) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .iter()
        .filter_map(|row| {
            let open_ms = row.first()?.as_i64()?;
            let close = parse_decimal(row.get(4)?.as_str()?)?;
            if close <= Decimal::ZERO {
                return None;
            }
            Some(Candle {
                open_time: DateTime::from_timestamp_millis(open_ms)?,
                close,
            })
        })
        .collect();
    out.sort_by_key(|candle| candle.open_time);
    out
}
/// 不带 `symbol` 时返回全部合约的盘口数组，一次请求覆盖全场，不必逐合约拉。
const BOOK_TICKER_URL: &str = "https://fapi.asterdex.com/fapi/v1/ticker/bookTicker";
/// 逐合约的深度端点：**必须带 `symbol`**，没有批量版本（对近千个合约逐个拉会打爆限频）。
/// `limit` 只接受 5/10/20/50/100/500/1000，其他值返回 HTTP 400。
const DEPTH_URL: &str = "https://fapi.asterdex.com/fapi/v1/depth";

/// 计价与保证金资产。USDT 之外的（USD1、USDC）资金费机制与风险都不同，一律排除。
const QUOTE: &str = "USDT";
const CONTRACT_PERPETUAL: &str = "PERPETUAL";
const STATUS_TRADING: &str = "TRADING";

pub struct AsterApi {
    client: Client,
}

impl AsterApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 取「交易所自己声明在交易」的 USDT 永续符号集合。
    ///
    /// 仅凭资金费端点无法确认是否可交易；合约元数据不可用时不能放宽过滤。
    async fn fetch_live_perps(&self) -> ArbResult<HashSet<String>> {
        let info: ExchangeInfo = get_json(self.client.get(EXCHANGE_INFO_URL), VENUE).await?;
        Ok(info
            .symbols
            .into_iter()
            .filter(is_live_usdt_perp)
            .map(|contract| contract.symbol)
            .collect())
    }

    /// 取每个合约的结算周期。
    ///
    /// 这个端点失败时**不能**让整家场所失败，但也**不能**当无事发生：
    /// 全部按 8h 折算会让 4h 合约（本场所的多数）的日化低估一半。所以回落到空表 +
    /// warn，由 `interval_assumed` 把这个假设透传到面板上。
    async fn fetch_intervals(&self) -> HashMap<String, u32> {
        let info: Vec<FundingInfo> = match get_json(self.client.get(FUNDING_INFO_URL), VENUE).await
        {
            Ok(info) => info,
            Err(error) => {
                warn!(
                    venue = %VENUE,
                    %error,
                    "结算周期端点失败，全部按默认 {DEFAULT_FUNDING_INTERVAL_H}h 折算"
                );
                return HashMap::new();
            }
        };
        info.into_iter()
            .filter_map(|item| item.funding_interval_hours.map(|h| (item.symbol, h)))
            .collect()
    }

    /// 盘口未知不能使已有资金费读数失效；空表让四个字段显式保持未知。
    async fn fetch_books(&self) -> HashMap<String, BookTicker> {
        let books: Vec<BookTicker> = match get_json(self.client.get(BOOK_TICKER_URL), VENUE).await {
            Ok(books) => books,
            Err(error) => {
                warn!(venue = %VENUE, %error, "盘口端点失败，本轮最优买卖价与一档量留空");
                return HashMap::new();
            }
        };
        books
            .into_iter()
            .map(|mut book| (std::mem::take(&mut book.symbol), book))
            .collect()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PremiumIndex {
    symbol: String,
    // 缺字段或错误类型只影响本行，不让一条坏读数拖垮整批行情。
    #[serde(default)]
    mark_price: Value,
    #[serde(default)]
    index_price: Value,
    #[serde(default)]
    last_funding_rate: Value,
    #[serde(default)]
    next_funding_time: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingInfo {
    symbol: String,
    funding_interval_hours: Option<u32>,
}

/// `exchangeInfo` 只取判定「这个合约还在不在交易」需要的字段。
///
/// 响应本身是 `{timezone, futuresType, symbols: [...]}` 的包装体，不是裸数组。
#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<Contract>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Contract {
    symbol: String,
    contract_type: String,
    status: String,
    quote_asset: String,
    margin_asset: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookTicker {
    symbol: String,
    // 与资金费字段同样容忍缺失和错误类型，避免一条坏盘口拖垮整批。
    #[serde(default)]
    bid_price: Value,
    #[serde(default)]
    bid_qty: Value,
    #[serde(default)]
    ask_price: Value,
    #[serde(default)]
    ask_qty: Value,
}

#[derive(Debug, Deserialize)]
struct Depth {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

/// 场所只接受离散档数，向上取最近合法值避免少取；超过 1000 则受端点上限约束。
fn depth_limit(levels: u32) -> ArbResult<u32> {
    if levels == 0 {
        return Err(ArbError::config("Aster 深度档数必须大于 0"));
    }
    Ok([5, 10, 20, 50, 100, 500, 1000]
        .into_iter()
        .find(|limit| *limit >= levels)
        .unwrap_or(1000))
}

fn parse_depth(symbol: &Symbol, depth: Depth) -> ArbResult<OrderBook> {
    let parse_side = |rows: Vec<[String; 2]>| -> ArbResult<Vec<Level>> {
        rows.into_iter()
            .map(|[raw_price, raw_quantity]| {
                let invalid = || ArbError::venue(VENUE.as_str(), "深度档位价格、数量或名义额无效");
                let price = parse_decimal(&raw_price).ok_or_else(invalid)?;
                let quantity = parse_decimal(&raw_quantity).ok_or_else(invalid)?;
                if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
                    return Err(invalid());
                }
                // 与 bookTicker 同为标的币数量；不能因符号带 1000 再乘一次倍数。
                let notional_usdt = price.checked_mul(quantity).ok_or_else(invalid)?;
                if notional_usdt <= Decimal::ZERO {
                    return Err(invalid());
                }
                Ok(Level {
                    price,
                    notional_usdt,
                })
            })
            .collect()
    };
    // 不静默丢掉坏档，否则数据损坏会被误认为真实流动性不足。
    let mut bids = parse_side(depth.bids)?;
    let mut asks = parse_side(depth.asks)?;
    if bids.is_empty() || asks.is_empty() {
        return Err(ArbError::venue(VENUE.as_str(), "深度盘口缺少买盘或卖盘"));
    }
    // 不能依赖上游返回顺序：吃单估算必须从最优价开始。
    bids.sort_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_by_key(|a| a.price);
    if asks[0].price < bids[0].price {
        return Err(ArbError::venue(VENUE.as_str(), "深度盘口交叉"));
    }
    Ok(OrderBook {
        venue: VENUE,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

/// 坏盘口整体留空，不能把交叉盘算成负穿价成本；锁盘与零量则是合法读数。
fn parse_book_row(book: &BookTicker) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
    let bid = parse_decimal(book.bid_price.as_str()?)?;
    let ask = parse_decimal(book.ask_price.as_str()?)?;
    let bid_qty = parse_decimal(book.bid_qty.as_str()?)?;
    let ask_qty = parse_decimal(book.ask_qty.as_str()?)?;
    if bid <= Decimal::ZERO || ask < bid || bid_qty < Decimal::ZERO || ask_qty < Decimal::ZERO {
        return None;
    }
    // 标的币量乘各自价格才是 USDT 名义；异常大值不能让整轮扫描因溢出崩溃。
    Some((
        bid,
        ask,
        bid.checked_mul(bid_qty)?,
        ask.checked_mul(ask_qty)?,
    ))
}

#[async_trait]
impl VenueApi for AsterApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        // 无法确认合约资格时直接报错；周期缺失则有 interval_assumed 可以显式降级。
        let live = self.fetch_live_perps().await?;
        let intervals = self.fetch_intervals().await;
        let index: Vec<PremiumIndex> = get_json(self.client.get(PREMIUM_INDEX_URL), VENUE).await?;
        let books = self.fetch_books().await;

        let (out, filtered, unusable) = join_rows(index, &live, &intervals, &books);

        // 两种「少了一条」要分开报：合约过滤是预期内的（下架、交割、USD1 保证金），
        // 字段不可用则说明数据源变了或接口改了字段名，必须能一眼区分。
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非「在交易的 USDT 永续」合约已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        // 本连接器只输出 USDT 永续，不能把 USD1 的名义额误标成 USDT。
        if symbol.quote != QUOTE || symbol.base.is_empty() {
            return Err(ArbError::config(format!("Aster 不支持深度标的 {symbol}")));
        }
        let limit = depth_limit(levels)?;
        // 与 parse_index_row 去掉 USDT 后缀互逆，保留 1000PEPE 等原始合约单位。
        let exchange_symbol = format!("{}{}", symbol.base, symbol.quote);
        let depth = get_json(
            self.client
                .get(DEPTH_URL)
                .query(&[("symbol", exchange_symbol)])
                .query(&[("limit", limit)]),
            VENUE,
        )
        .await?;
        parse_depth(symbol, depth)
    }
    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `/fapi/v1/klines` 的周期是字符串枚举，请求的分钟数向上取到最近的合法值：
    /// 更细的序列会把半衰期算短。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let native = format!("{}{QUOTE}", symbol.base);
        let interval = kline_interval(interval_minutes)?;
        let url = format!(
            "{KLINE_URL}?symbol={native}&interval={interval}&limit={}",
            limit.min(1500)
        );
        let rows: Vec<Vec<serde_json::Value>> = get_json(self.client.get(&url), VENUE).await?;
        Ok(parse_klines(&rows))
    }
}

/// 把四个端点合成读数。返回 `(读数, 合约过滤掉的行数, 字段不可用的行数)`。
///
/// 抽成纯函数是为了让「下架合约」「周期回落」「费率缺失」这三条最容易出错的规则
/// 能被单测覆盖，不必真的打网络。
fn join_rows(
    index: Vec<PremiumIndex>,
    live: &HashSet<String>,
    intervals: &HashMap<String, u32>,
    books: &HashMap<String, BookTicker>,
) -> (Vec<MarketSnapshot>, usize, usize) {
    let mut out = Vec::with_capacity(index.len());
    let mut filtered = 0usize;
    let mut unusable = 0usize;

    for item in index {
        if !live.contains(&item.symbol) {
            filtered += 1;
            continue;
        }
        match parse_index_row(&item, intervals) {
            Some(mut rate) => {
                if let Some((bid, ask, bid_size, ask_size)) =
                    books.get(&item.symbol).and_then(parse_book_row)
                {
                    rate.best_bid = Some(bid);
                    rate.best_ask = Some(ask);
                    rate.bid_size_usdt = Some(bid_size);
                    rate.ask_size_usdt = Some(ask_size);
                }
                out.push(rate);
            }
            None => unusable += 1,
        }
    }

    // 输出顺序不跟 `premiumIndex` 的返回顺序走：接口没有承诺顺序，而下游要能对比
    // 两次扫描的差异。显式排序，代价只是几百个元素。
    out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
    (out, filtered, unusable)
}

/// 把 `premiumIndex` 的一行转成领域类型。`None` = 这一行不可用。
///
/// 三处都**不猜**：
/// - 费率字段缺失/不可解析 → 整行丢弃。**绝不回落成 0**：0 是一个合法的费率读数
///   （实测 KORUUSDT、TSLAUSDT 当期就是 0），伪造出来的 0 会凭空造出巨大价差。
/// - 结算时刻无法解析或为 0 → 丢弃，而不是拿当前时间顶上。
/// - 周期查不到 → 默认值 + `interval_assumed = true`，把这个假设透传到面板。
fn parse_index_row(
    item: &PremiumIndex,
    intervals: &HashMap<String, u32>,
) -> Option<MarketSnapshot> {
    let base = item
        .symbol
        .strip_suffix(QUOTE)
        .filter(|base| !base.is_empty())?;

    let period_rate = parse_decimal(item.last_funding_rate.as_str()?)?;
    // 0 毫秒也是合法时间戳，但不代表有效的下一次结算。
    let timestamp = item.next_funding_time.as_i64().filter(|time| *time > 0)?;
    let next_funding_at = DateTime::from_timestamp_millis(timestamp)?;

    let (interval_h, interval_assumed) = match intervals.get(item.symbol.as_str()) {
        Some(hours) if *hours > 0 => (*hours, false),
        _ => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: None,
        mark_price: item.mark_price.as_str().and_then(parse_decimal),
        index_price: item.index_price.as_str().and_then(parse_decimal),
        // 盘口在 join_rows 按交易所符号并入，缺失时不能用标记价顶替。
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        // 成交额另见 ticker/24hr；openInterest 必须逐合约请求，本轮不补。
        open_interest_usdt: None,
        quote_volume_24h: None,
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 是不是「交易所声明在交易的 USDT 保证金永续」。
///
/// 用元数据排除交割、非 USDT 保证金与暂停/未上市合约，而非猜测符号含义。
fn is_live_usdt_perp(contract: &Contract) -> bool {
    contract.contract_type == CONTRACT_PERPETUAL
        && contract.status == STATUS_TRADING
        && contract.quote_asset == QUOTE
        && contract.margin_asset == QUOTE
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Decimal;

    /// `GET /fapi/v1/premiumIndex` 的真实片段（2026-09-19 抓取，全量 752 行）。
    ///
    /// APXUSDT / MKRUSDT 不在 exchangeInfo 里，却仍返回正常的费率与结算时刻。
    const PREMIUM_INDEX_FIXTURE: &str = r#"[
        {"symbol":"GNSUSD","markPrice":"0.42702357","indexPrice":"0.42702357","estimatedSettlePrice":"0.42800499","lastFundingRate":"0","interestRate":"0","nextFundingTime":1789833600000,"time":1789823869000},
        {"symbol":"BTCUSDT","markPrice":"81267.96337318","indexPrice":"81294.30391304","estimatedSettlePrice":"81281.39149409","lastFundingRate":"0.00010000","interestRate":"0.00010000","nextFundingTime":1789833600000,"time":1789823869000},
        {"symbol":"TRUTHUSDT","markPrice":"0.01327107","indexPrice":"0.01325857","estimatedSettlePrice":"0.01326631","lastFundingRate":"0.00016927","interestRate":"0.00010000","nextFundingTime":1789833600000,"time":1789823869000},
        {"symbol":"KORUUSDT","markPrice":"19.70026009","indexPrice":"19.69853861","estimatedSettlePrice":"19.68599461","lastFundingRate":"0","interestRate":"0","nextFundingTime":1789833600000,"time":1789823869000},
        {"symbol":"APXUSDT","markPrice":"0.61244035","indexPrice":"0.61243000","estimatedSettlePrice":"0.61243000","lastFundingRate":"0.00010000","interestRate":"0.00010000","nextFundingTime":1789833600000,"time":1789823869000},
        {"symbol":"MKRUSDT","markPrice":"1301.90499849","indexPrice":"1301.90499849","estimatedSettlePrice":"0","lastFundingRate":"0.00010000","interestRate":"0.00010000","nextFundingTime":1789833600000,"time":1789823869000}
    ]"#;

    /// `GET /fapi/v1/fundingInfo` 的真实片段。`fundingIntervalHours` 是 JSON number，
    /// 不是字符串；实测全量 752 行，周期分布 1h/2h/4h/8h。
    const FUNDING_INFO_FIXTURE: &str = r#"[
        {"symbol":"ARGUSUSDT","interestRate":"0.00010000","time":1789823819000,"fundingIntervalHours":1,"fundingFeeCap":0.02,"fundingFeeFloor":-0.02},
        {"symbol":"TRUTHUSDT","interestRate":"0.00010000","time":1789823819000,"fundingIntervalHours":4,"fundingFeeCap":0.02,"fundingFeeFloor":-0.02},
        {"symbol":"BTCUSDT","interestRate":"0.00010000","time":1789823819000,"fundingIntervalHours":8,"fundingFeeCap":0.003,"fundingFeeFloor":-0.003},
        {"symbol":"MKRUSDT","interestRate":"0.00010000","time":1789823819000,"fundingIntervalHours":8,"fundingFeeCap":0.02,"fundingFeeFloor":-0.02}
    ]"#;

    /// `GET /fapi/v1/exchangeInfo` 的真实片段（2026-09-19 共 602 个合约）。
    /// 为了可读性只保留了本模块解析的字段，键名与取值都是原样。
    const EXCHANGE_INFO_FIXTURE: &str = r#"{"timezone":"UTC","futuresType":"U_MARGINED","symbols":[
        {"symbol":"TRUTHUSDT","pair":"TRUTHUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"TRUTH","quoteAsset":"USDT","marginAsset":"USDT"},
        {"symbol":"XAUUSD1","pair":"XAUUSD1","contractType":"PERPETUAL","status":"TRADING","baseAsset":"XAU","quoteAsset":"USD1","marginAsset":"USD1"},
        {"symbol":"TONUSDT","pair":"TONUSDT","contractType":"PERPETUAL","status":"SETTLING","baseAsset":"TON","quoteAsset":"USDT","marginAsset":"USDT"},
        {"symbol":"MBLUSDT","pair":"MBLUSDT","contractType":"","status":"PENDING_TRADING","baseAsset":"MBL","quoteAsset":"USDT","marginAsset":"USDT"}
    ]}"#;

    /// 批量 bookTicker 的真实片段（2026-09-20）；故意反排验证按符号而非数组位置关联。
    const BOOK_TICKER_FIXTURE: &str = r#"[
        {"symbol":"TRUTHUSDT","bidPrice":"0.0132650","bidQty":"8037","askPrice":"0.0132940","askQty":"150443","time":1789879750100,"lastUpdateId":553774082067},
        {"symbol":"BTCUSDT","bidPrice":"80332.6","bidQty":"0.727","askPrice":"80332.7","askQty":"0.023","time":1789879750150,"lastUpdateId":553774083799}
    ]"#;

    fn books() -> HashMap<String, BookTicker> {
        let books: Vec<BookTicker> = serde_json::from_str(BOOK_TICKER_FIXTURE).unwrap();
        books
            .into_iter()
            .map(|mut book| (std::mem::take(&mut book.symbol), book))
            .collect()
    }

    fn premium_index() -> Vec<PremiumIndex> {
        serde_json::from_str(PREMIUM_INDEX_FIXTURE).expect("真实 premiumIndex 片段应能解析")
    }

    /// 从真实 exchangeInfo 片段推出「在交易的 USDT 永续」集合，再补上 BTCUSDT / KORUUSDT
    /// （它们在真实响应里也是 TRADING 的 USDT 永续，只是没进这个片段）。
    fn live_set() -> HashSet<String> {
        let info: ExchangeInfo =
            serde_json::from_str(EXCHANGE_INFO_FIXTURE).expect("真实 exchangeInfo 片段应能解析");
        let mut live: HashSet<String> = info
            .symbols
            .into_iter()
            .filter(is_live_usdt_perp)
            .map(|contract| contract.symbol)
            .collect();
        live.insert("BTCUSDT".into());
        live.insert("KORUUSDT".into());
        live
    }

    fn intervals() -> HashMap<String, u32> {
        let info: Vec<FundingInfo> =
            serde_json::from_str(FUNDING_INFO_FIXTURE).expect("真实 fundingInfo 片段应能解析");
        info.into_iter()
            .filter_map(|item| item.funding_interval_hours.map(|h| (item.symbol, h)))
            .collect()
    }

    fn rows() -> Vec<MarketSnapshot> {
        join_rows(premium_index(), &live_set(), &intervals(), &books()).0
    }

    fn row(symbol: &str) -> MarketSnapshot {
        rows()
            .into_iter()
            .find(|rate| rate.symbol.base == symbol)
            .unwrap_or_else(|| panic!("{symbol} 应该在结果里"))
    }

    #[test]
    fn only_contracts_the_venue_declares_as_trading_usdt_perps_survive() {
        let info: ExchangeInfo =
            serde_json::from_str(EXCHANGE_INFO_FIXTURE).expect("真实 exchangeInfo 片段应能解析");
        let live: Vec<String> = info
            .symbols
            .into_iter()
            .filter(is_live_usdt_perp)
            .map(|contract| contract.symbol)
            .collect();

        // XAUUSD1（USD1 保证金）、TONUSDT（SETTLING）、MBLUSDT（PENDING_TRADING）
        // 都被排除，只剩 TRUTHUSDT。
        assert_eq!(live, vec!["TRUTHUSDT".to_string()]);
    }

    #[test]
    fn delivery_contracts_are_excluded_even_though_they_quote_in_usdt() {
        // Aster 目前一个交割合约都没有（602 条里 597 条 PERPETUAL，其余 5 条连
        // contractType 都是空的），所以这条用构造出来的季度合约当护栏：
        // 哪天上了交割合约，它不能靠「符号以 USDT 结尾」混进来。
        let quarterly: Contract = serde_json::from_str(
            r#"{"symbol":"BTCUSDT_250926","contractType":"CURRENT_QUARTER","status":"TRADING","quoteAsset":"USDT","marginAsset":"USDT"}"#,
        )
        .expect("构造的交割合约条目应能解析");
        assert!(!is_live_usdt_perp(&quarterly));

        let info: ExchangeInfo = serde_json::from_str(EXCHANGE_INFO_FIXTURE).unwrap();
        let mut inverse = info.symbols.into_iter().next().unwrap();
        inverse.margin_asset = "BTC".into();
        assert!(
            !is_live_usdt_perp(&inverse),
            "USDT 报价但币本位保证金也应排除"
        );
        inverse.margin_asset = "USDT".into();
        inverse.quote_asset = "USDC".into();
        assert!(
            !is_live_usdt_perp(&inverse),
            "保证金符合但报价不是 USDT 也应排除"
        );
    }

    #[test]
    fn delisted_contracts_are_dropped_although_they_report_a_normal_rate() {
        let (rates, filtered, unusable) =
            join_rows(premium_index(), &live_set(), &intervals(), &books());
        let bases: Vec<&str> = rates.iter().map(|r| r.symbol.base.as_str()).collect();

        // 按 base 排序，输出与 premiumIndex 的返回顺序无关。
        assert_eq!(bases, ["BTC", "KORU", "TRUTH"]);
        // GNSUSD（USD 计价）、APXUSDT 与 MKRUSDT（已下架）被合约过滤掉；
        // 没有任何一行的字段不可用。
        assert_eq!((filtered, unusable), (3, 0));
    }

    #[test]
    fn reported_interval_is_used_and_absence_is_flagged_as_assumed() {
        let truth = row("TRUTH");
        assert_eq!(truth.interval_h, 4, "fundingInfo 实测给的是 4h");
        assert!(!truth.interval_assumed);

        let btc = row("BTC");
        assert_eq!(btc.interval_h, 8);
        assert!(!btc.interval_assumed);

        // KORUUSDT 不在周期表里 → 回落默认值，且必须标记出来。
        let koru = row("KORU");
        assert_eq!(koru.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(koru.interval_assumed, "回落值必须标记出来");

        let mut intervals = intervals();
        intervals.insert("BTCUSDT".into(), 0);
        let btc = parse_index_row(&premium_index().remove(1), &intervals).unwrap();
        assert_eq!(btc.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(btc.interval_assumed, "零周期不可用于日化");

        let missing: FundingInfo = serde_json::from_str(r#"{"symbol":"BTCUSDT"}"#).unwrap();
        let absent: HashMap<String, u32> = missing
            .funding_interval_hours
            .map(|hours| (missing.symbol, hours))
            .into_iter()
            .collect();
        let btc = parse_index_row(&premium_index().remove(1), &absent).unwrap();
        assert_eq!(btc.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(btc.interval_assumed);
    }

    #[test]
    fn venue_given_settlement_time_is_used_verbatim() {
        let btc = row("BTC");
        assert_eq!(btc.next_funding_at.timestamp_millis(), 1_789_833_600_000);
        assert!(!btc.next_funding_estimated, "时刻是场所给的，不是推算的");
    }

    #[test]
    fn absent_or_bad_rates_drop_only_the_bad_row_and_count_it_separately() {
        for bad_rate in [
            None,
            Some(Value::Null),
            Some("".into()),
            Some("n/a".into()),
            Some(42.into()),
        ] {
            let mut json: Value = serde_json::from_str(PREMIUM_INDEX_FIXTURE).unwrap();
            let truth = json[2].as_object_mut().unwrap();
            match bad_rate {
                Some(value) => {
                    truth.insert("lastFundingRate".into(), value);
                }
                None => {
                    truth.remove("lastFundingRate");
                }
            }
            let index: Vec<PremiumIndex> = serde_json::from_value(json).unwrap();
            let (rates, filtered, unusable) = join_rows(index, &live_set(), &intervals(), &books());
            let bases: Vec<&str> = rates.iter().map(|rate| rate.symbol.base.as_str()).collect();
            assert_eq!(bases, ["BTC", "KORU"]);
            assert_eq!((filtered, unusable), (3, 1));
        }
    }

    #[test]
    fn a_reported_zero_rate_is_kept_because_zero_is_a_reading() {
        // 实测 KORUUSDT / TSLAUSDT 当期费率就是 "0"：这是合法读数，不是缺字段。
        assert_eq!(row("KORU").period_rate, Decimal::ZERO);
    }

    #[test]
    fn absent_zero_or_invalid_settlement_times_drop_the_row() {
        for bad_time in [
            None,
            Some(Value::Null),
            Some(0.into()),
            Some((-1).into()),
            Some(i64::MAX.into()),
            Some("bad".into()),
        ] {
            let mut json: Value = serde_json::from_str(PREMIUM_INDEX_FIXTURE).unwrap();
            let btc = json[1].as_object_mut().unwrap();
            match bad_time {
                Some(value) => {
                    btc.insert("nextFundingTime".into(), value);
                }
                None => {
                    btc.remove("nextFundingTime");
                }
            }
            let index: Vec<PremiumIndex> = serde_json::from_value(json).unwrap();
            let (rates, filtered, unusable) = join_rows(index, &live_set(), &intervals(), &books());
            let bases: Vec<&str> = rates.iter().map(|rate| rate.symbol.base.as_str()).collect();
            assert_eq!(bases, ["KORU", "TRUTH"]);
            assert_eq!((filtered, unusable), (3, 1));
        }
    }

    #[test]
    fn taker_fee_is_unknown_and_prices_are_parsed() {
        // 公共接口拿不到吃单费率（commissionRate 需要签名）→ None，不是 0，也不是猜的值。
        assert!(rows().iter().all(|rate| rate.taker_fee.is_none()));

        let btc = row("BTC");
        assert_eq!(btc.mark_price, parse_decimal("81267.96337318"));
        assert_eq!(btc.index_price, parse_decimal("81294.30391304"));
        assert_eq!(btc.period_rate, parse_decimal("0.00010000").unwrap());
        assert_eq!(btc.venue, Venue::Aster);
        assert_eq!(btc.symbol.to_string(), "BTC/USDT");
    }

    #[test]
    fn book_fixture_maps_sides_and_converts_base_quantity_to_quote_notional() {
        let btc = row("BTC");
        assert_eq!(btc.best_bid, parse_decimal("80332.6"));
        assert_eq!(btc.best_ask, parse_decimal("80332.7"));
        assert_eq!(btc.bid_size_usdt, parse_decimal("58401.8002"));
        assert_eq!(btc.ask_size_usdt, parse_decimal("1847.6521"));

        let truth = row("TRUTH");
        assert_eq!(truth.best_bid, parse_decimal("0.0132650"));
        assert_eq!(truth.best_ask, parse_decimal("0.0132940"));
        assert_eq!(truth.bid_size_usdt, parse_decimal("106.610805"));
        assert_eq!(truth.ask_size_usdt, parse_decimal("1999.989242"));
    }

    fn assert_unknown_book(rate: &MarketSnapshot) {
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

    #[test]
    fn absent_book_keeps_funding_without_substituting_mark_price() {
        let koru = row("KORU");
        assert_unknown_book(&koru);
        assert_eq!(koru.period_rate, Decimal::ZERO);
        assert_eq!(koru.mark_price, parse_decimal("19.70026009"));
        let (rates, filtered, unusable) =
            join_rows(premium_index(), &live_set(), &intervals(), &HashMap::new());
        assert_eq!((filtered, unusable), (3, 0));
        assert_eq!(rates.len(), 3);
        for rate in rates {
            assert_unknown_book(&rate);
        }
    }

    #[test]
    fn invalid_book_fields_only_blank_the_affected_book() {
        for (field, bad_value) in [
            ("bidPrice", None),
            ("askPrice", Some(Value::Null)),
            ("bidQty", None),
            ("askQty", Some("".into())),
            ("bidPrice", Some(42.into())),
            ("askQty", Some("bad".into())),
            ("bidPrice", Some("0".into())),
            ("askPrice", Some("80332.5".into())),
            ("bidQty", Some("-1".into())),
            ("askQty", Some("79228162514264337593543950335".into())),
        ] {
            let mut json: Value = serde_json::from_str(BOOK_TICKER_FIXTURE).unwrap();
            let btc = json[1].as_object_mut().unwrap();
            match bad_value {
                Some(value) => {
                    btc.insert(field.into(), value);
                }
                None => {
                    btc.remove(field);
                }
            }
            let bad_book: BookTicker = serde_json::from_value(json[1].clone()).unwrap();
            let mut books = books();
            books.insert("BTCUSDT".into(), bad_book);
            let (rates, filtered, unusable) =
                join_rows(premium_index(), &live_set(), &intervals(), &books);
            assert_eq!((filtered, unusable), (3, 0));
            let btc = rates.iter().find(|rate| rate.symbol.base == "BTC").unwrap();
            assert_unknown_book(btc);
            assert_eq!(btc.period_rate, parse_decimal("0.00010000").unwrap());
            let truth = rates
                .iter()
                .find(|rate| rate.symbol.base == "TRUTH")
                .unwrap();
            assert_eq!(truth.bid_size_usdt, parse_decimal("106.610805"));
        }
    }

    #[test]
    fn locked_book_and_reported_zero_quantity_remain_known() {
        let mut books = books();
        let btc = books.get_mut("BTCUSDT").unwrap();
        btc.ask_price = btc.bid_price.clone();
        btc.bid_qty = "0".into();
        let (rates, _, _) = join_rows(premium_index(), &live_set(), &intervals(), &books);
        let btc = rates.iter().find(|rate| rate.symbol.base == "BTC").unwrap();
        assert_eq!(btc.best_bid, parse_decimal("80332.6"));
        assert_eq!(btc.best_ask, parse_decimal("80332.6"));
        assert_eq!(btc.bid_size_usdt, Some(Decimal::ZERO));
        assert_eq!(btc.ask_size_usdt, parse_decimal("1847.6498"));
    }

    /// 真实 BTCUSDT 五档响应；后续反排只用于验证不能依赖上游顺序。
    const DEPTH_FIXTURE: &str = r#"{
        "lastUpdateId":554175445822,"E":1789898232271,"T":1789898232250,
        "bids":[["80459.4","0.070"],["80450.8","0.001"],["80448.8","0.001"],["80447.8","1.817"],["80447.7","5.093"]],
        "asks":[["80459.5","0.986"],["80459.8","0.008"],["80461.6","0.008"],["80462.0","1.301"],["80462.1","0.024"]]
    }"#;

    #[test]
    fn depth_fixture_maps_units_sorts_and_estimates_fill() {
        use arb_core::{Side, estimate_fill};

        let mut depth: Depth = serde_json::from_str(DEPTH_FIXTURE).unwrap();
        depth.bids.reverse();
        depth.asks.reverse();
        let symbol = Symbol::perp("BTC", QUOTE);
        let book = parse_depth(&symbol, depth).unwrap();
        assert_eq!(book.venue, VENUE);
        assert_eq!(book.symbol, symbol);
        assert_eq!(book.bids[0].price, parse_decimal("80459.4").unwrap());
        assert_eq!(book.asks[0].price, parse_decimal("80459.5").unwrap());
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("5632.1580").unwrap()
        );
        assert_eq!(
            book.asks[0].notional_usdt,
            parse_decimal("79333.0670").unwrap()
        );
        assert!(
            book.bids
                .windows(2)
                .all(|pair| pair[0].price >= pair[1].price)
        );
        assert!(
            book.asks
                .windows(2)
                .all(|pair| pair[0].price <= pair[1].price)
        );

        let target = Decimal::from(100_000);
        let buy = estimate_fill(&book.asks, target, Side::Buy).unwrap();
        assert_eq!(buy.filled_usdt, target);
        assert!(!buy.exhausted);
        assert_eq!(
            buy.average_price.round_dp(4),
            parse_decimal("80459.9999").unwrap()
        );
        assert!(buy.slippage > Decimal::ZERO);
        let sell = estimate_fill(&book.bids, target, Side::Sell).unwrap();
        assert_eq!(sell.filled_usdt, target);
        assert!(sell.slippage > Decimal::ZERO);
        let exhausted = estimate_fill(&book.asks, Decimal::from(500_000), Side::Buy).unwrap();
        assert_eq!(exhausted.filled_usdt, parse_decimal("187232.5906").unwrap());
        assert!(exhausted.exhausted);
    }

    #[test]
    fn thousand_base_depth_needs_no_contract_multiplier() {
        // 真实 1000PEPEUSDT 片段：价格已按 1000PEPE 报价，再乘 1000 会放大流动性。
        let depth: Depth = serde_json::from_str(
            r#"{"bids":[["0.0039675","348528"]],"asks":[["0.0039690","18000"]]}"#,
        )
        .unwrap();
        let book = parse_depth(&Symbol::perp("1000PEPE", QUOTE), depth).unwrap();
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("1382.7848400").unwrap()
        );
        assert_eq!(
            book.asks[0].notional_usdt,
            parse_decimal("71.4420000").unwrap()
        );
    }

    #[test]
    fn empty_crossed_and_unconvertible_depth_are_errors() {
        let symbol = Symbol::perp("BTC", QUOTE);
        for side in ["bids", "asks"] {
            let mut json: Value = serde_json::from_str(DEPTH_FIXTURE).unwrap();
            json[side] = serde_json::json!([]);
            assert!(parse_depth(&symbol, serde_json::from_value(json).unwrap()).is_err());
        }
        let mut depth: Depth = serde_json::from_str(DEPTH_FIXTURE).unwrap();
        depth.asks[4][0] = "80400".into();
        assert!(parse_depth(&symbol, depth).is_err());
        for (field, bad) in [
            (0, "0"),
            (1, "0"),
            (1, "-1"),
            (1, "bad"),
            (1, "79228162514264337593543950335"),
        ] {
            let mut depth: Depth = serde_json::from_str(DEPTH_FIXTURE).unwrap();
            depth.bids[0][field] = bad.into();
            assert!(parse_depth(&symbol, depth).is_err());
        }
        // 缺数量不能借默认值伪造可成交量；Aster 本身不需要额外面值元数据。
        assert!(
            serde_json::from_str::<Depth>(r#"{"bids":[["80459.4"]],"asks":[["80459.5","1"]]}"#)
                .is_err()
        );
        let mut depth: Depth = serde_json::from_str(DEPTH_FIXTURE).unwrap();
        depth.asks[0][0] = depth.bids[0][0].clone();
        assert!(parse_depth(&symbol, depth).is_ok());
    }

    #[test]
    fn depth_limits_follow_verified_endpoint_boundaries() {
        assert!(depth_limit(0).is_err());
        for (requested, expected) in [
            (1, 5),
            (5, 5),
            (7, 10),
            (20, 20),
            (21, 50),
            (100, 100),
            (101, 500),
            (500, 500),
            (501, 1000),
            (1000, 1000),
            (1001, 1000),
        ] {
            assert_eq!(depth_limit(requested).unwrap(), expected);
        }
    }
}
