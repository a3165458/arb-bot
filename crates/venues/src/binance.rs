//! Binance USDⓈ-M 永续。
//!
//! 三个端点配合：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/fapi/v1/premiumIndex` | 每期费率、标记价、指数价、下次结算时刻 | **结算周期**、盘口 |
//! | `/fapi/v1/fundingInfo` | 每个合约的 `fundingIntervalHours` | 费率 |
//! | `/fapi/v1/ticker/bookTicker` | 最优买卖价、一档量 | 费率、周期 |
//!
//! `premiumIndex` 没有周期字段，只看它会把所有合约当成 8h。`fundingInfo` 只列出**调整过**周期或费率
//! 上下限的合约（实测 804 个，其中 470 个是 4h），**没出现在里面的合约按默认 8h** —— 实测缺席的
//! 97 个里 94 个是已下架的，剩下 3 个（FRONT / EOS / MATIC）逐笔核对过 `fundingRate` 历史都是 8h。
//!
//! **`premiumIndex` 还会继续列出已下架的合约**（`exchangeInfo.status = SETTLING`，实测 130 个）：
//! 费率停在下架前的最后一个读数（常常是 0），`nextFundingTime` 却还是个将来的时刻。它们不能下单，
//! 但会作为「费率为 0 的一条腿」混进配对、排到榜上。所以按 `exchangeInfo` 只留 `status = TRADING`
//! （缓存 10 分钟；取不到时沿用上一份，一份都没有就不过滤并告警）。
//!
//! 盘口走不带 `symbol` 的批量 `bookTicker`，与 `premiumIndex` 按 `symbol` 左联接。
//! `bidQty` / `askQty` 是标的币数量，必须乘本侧价格才是 USDT 名义量。
//! 请求失败、符号缺失或字段不可用时保持 `None`，不拿标记价顶上，否则会低估穿价成本。
//!
//! 多档深度走**逐合约**的 `/fapi/v1/depth`：`limit` 只接受离散档数（见 [`DEPTH_LIMITS`]），
//! 返回的 `qty` 与 `bookTicker` 一样是**标的币数量**，名义额 = 数量 × 价。
//!
//! 历史收盘价走**逐合约**的 `/fapi/v1/klines`（见 [`BinanceApi::fetch_candles`]）。
//! 周期是**字符串枚举**而不是任意分钟数：实测 `7m`、`90m`、`1s` 都被上游以
//! `{"code":-1120,"msg":"Invalid interval."}` 拒绝，合法取值只有 [`KLINE_INTERVALS`]
//! 列出的 15 个。所以不支持的周期一律**向上**取到最近的合法值（7m → 15m）：
//! 返回比请求**更细**的序列会把半衰期算成更短的时间，而这个错误不会报错 ——
//! 算出来的持有期看着正常，方向却反了。宁可粗一点。
//!
//! 吃单费率需要签名接口（`/fapi/v1/commissionRate`），公共行情拿不到，
//! 因此 `taker_fee` 一律为 `None` —— 排名会回落到配置的单边费率。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Level, MarketSnapshot, OrderBook,
    Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Binance;
const PREMIUM_INDEX_URL: &str = "https://fapi.binance.com/fapi/v1/premiumIndex";
const FUNDING_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/fundingInfo";
const EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const KLINE_URL: &str = "https://fapi.binance.com/fapi/v1/klines";

/// `/fapi/v1/klines` 接受的周期（分钟 → 字符串），按分钟升序。
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

/// 把请求的分钟数映射到**不小于**它的合法周期。
///
/// 向上取整而不是向下：更细的序列会把基差的高频噪声当成信号，半衰期被算短，
/// 而持有期算短会让年化虚高 —— 一个不会报错、只会让人多下注的错误。
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

/// 把 K 线原始行转成收盘价序列。
///
/// 每行是 `[openTime(ms), open, high, low, close, volume, …]`。
/// 收盘价缺失/不可解析/非正的那根**跳过** —— 填 0 会造出一个 −100% 的假跳变。
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
    // 实测是升序，但不假设 —— 顺序反了半衰期会算成正数（看起来正常）。
    out.sort_by_key(|candle| candle.open_time);
    out
}
const BOOK_TICKER_URL: &str = "https://fapi.binance.com/fapi/v1/ticker/bookTicker";
const DEPTH_URL: &str = "https://fapi.binance.com/fapi/v1/depth";

/// `/fapi/v1/depth` 接受的档数**只有这几个值**。
///
/// 实测 `limit=1/3/7` 返回 `{"code":-4021,"msg":"… is not valid depth limit"}`，
/// `1500` 返回 `-1130`，省略 `limit` 时默认 500。所以调用方要的档数不能直接透传，
/// 必须向上取到合法值（见 [`depth_limit`]）。
const DEPTH_LIMITS: [u32; 7] = [5, 10, 20, 50, 100, 500, 1000];

/// 计价资产。USDⓈ-M 只有 USDT 保证金。
const QUOTE: &str = "USDT";

pub struct BinanceApi {
    client: Client,
}

impl BinanceApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 取每个合约的结算周期。
    ///
    /// 这个端点失败时**不能**让整家场所失败，但也**不能**当无事发生：
    /// 全部按 8h 折算会让 4h 合约的日化低估一半。所以回落到空表 + warn，
    /// 由 `interval_assumed` 把这个假设透传到面板上。
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

    /// 盘口是辅助数据，失败不能丢掉仍然有效的资金费；缺失必须显式透传。
    async fn fetch_books(&self) -> Vec<BookTicker> {
        match get_json(self.client.get(BOOK_TICKER_URL), VENUE).await {
            Ok(books) => books,
            Err(error) => {
                warn!(venue = %VENUE, %error, "盘口端点失败，最优价与一档量保持缺失");
                Vec::new()
            }
        }
    }

    /// 当前可交易（`status = TRADING`）的合约集合，缓存 [`TRADING_TTL`]。
    ///
    /// 取不到时沿用上一份（哪怕过期了：下架名单变化很慢）；一份都没有就返回 `None`，
    /// 由调用方不过滤并留下告警 —— 过滤失败不能让整家场所从榜上消失。
    async fn trading_symbols(&self) -> Option<Arc<HashSet<String>>> {
        if let Ok(cached) = TRADING.lock()
            && let Some((at, set)) = cached.as_ref()
            && at.elapsed() < TRADING_TTL
        {
            return Some(Arc::clone(set));
        }
        match get_json::<ExchangeInfo>(self.client.get(EXCHANGE_INFO_URL), VENUE).await {
            Ok(info) => {
                let set = Arc::new(trading_set(&info));
                if let Ok(mut cached) = TRADING.lock() {
                    *cached = Some((Instant::now(), Arc::clone(&set)));
                }
                Some(set)
            }
            Err(error) => {
                let stale = TRADING
                    .lock()
                    .ok()
                    .and_then(|cached| cached.as_ref().map(|(_, set)| Arc::clone(set)));
                warn!(
                    venue = %VENUE,
                    %error,
                    stale = stale.is_some(),
                    "合约状态端点失败：已下架的合约可能混在榜里"
                );
                stale
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PremiumIndex {
    symbol: String,
    mark_price: String,
    index_price: String,
    last_funding_rate: String,
    next_funding_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingInfo {
    symbol: String,
    funding_interval_hours: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

/// 缓存「可交易合约」多久。下架名单一天才变几次。
const TRADING_TTL: Duration = Duration::from_secs(600);

/// 可交易合约集合与取到的时刻。进程内一份：扫描与预检的 `BinanceApi` 实例共用。
static TRADING: LazyLock<Mutex<TradingCache>> = LazyLock::new(Default::default);

/// 缓存的内容：取到的时刻与可交易合约集合。
type TradingCache = Option<(Instant, Arc<HashSet<String>>)>;

/// 只留 `status = TRADING`。`SETTLING`（已下架、等待交割）、`PENDING_TRADING`（还没开）都不算。
fn trading_set(info: &ExchangeInfo) -> HashSet<String> {
    info.symbols
        .iter()
        .filter(|item| item.status == "TRADING")
        .map(|item| item.symbol.clone())
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookTicker {
    symbol: String,
    bid_price: Option<String>,
    bid_qty: Option<String>,
    ask_price: Option<String>,
    ask_qty: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Depth {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[async_trait]
impl VenueApi for BinanceApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let trading = self.trading_symbols().await;
        let index: Vec<PremiumIndex> = get_json(self.client.get(PREMIUM_INDEX_URL), VENUE).await?;
        let intervals = self.fetch_intervals().await;
        let books = self.fetch_books().await;
        let books: HashMap<_, _> = books
            .iter()
            .map(|book| (book.symbol.as_str(), book))
            .collect();

        let mut out = Vec::with_capacity(index.len());
        let mut filtered = 0usize;
        let mut delisted = 0usize;
        let mut unusable = 0usize;
        for item in index {
            if !is_linear_perp(&item.symbol) {
                filtered += 1;
                continue;
            }
            if trading
                .as_ref()
                .is_some_and(|trading| !trading.contains(&item.symbol))
            {
                delisted += 1;
                continue;
            }
            match parse_index_row(&item, &intervals, books.get(item.symbol.as_str()).copied()) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        // 两种「少了一条」要分开报：符号过滤是预期内的（交割合约、USDC 保证金），
        // 字段不可用则说明数据源变了或合约已下架，必须能一眼区分。
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非 USDT 永续合约已过滤");
        }
        if delisted > 0 {
            tracing::debug!(venue = %VENUE, delisted, "非 TRADING 状态的合约已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        let raw_symbol = format!("{}{}", symbol.base, symbol.quote);
        if symbol.quote != QUOTE || !is_linear_perp(&raw_symbol) {
            return Err(ArbError::config(format!(
                "Binance 深度只支持 USDT 线性永续：{symbol}"
            )));
        }
        let limit = depth_limit(levels)?;
        let depth: Depth = get_json(
            self.client
                .get(DEPTH_URL)
                .query(&[("symbol", raw_symbol), ("limit", limit.to_string())]),
            VENUE,
        )
        .await?;
        parse_depth(symbol, depth, levels)
    }
    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `/fapi/v1/klines` 的周期是**字符串枚举**，所以请求的分钟数要向上取到最近的
    /// 合法周期：**宁可粗一点，也不要更细** —— 更细的序列会把半衰期算短，
    /// 而那个错误看起来完全正常。
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

fn depth_limit(levels: u32) -> ArbResult<u32> {
    if levels > 0
        && let Some(limit) = DEPTH_LIMITS.into_iter().find(|limit| *limit >= levels)
    {
        return Ok(limit);
    }
    Err(ArbError::config("Binance 深度档数必须在 1..=1000 内"))
}

fn parse_depth(symbol: &Symbol, depth: Depth, levels: u32) -> ArbResult<OrderBook> {
    let parse_side = |rows: Vec<[String; 2]>| -> Vec<Level> {
        rows.into_iter()
            .filter_map(|[price, qty]| {
                let price = parse_decimal(&price).filter(|value| *value > Decimal::ZERO)?;
                let qty = parse_decimal(&qty).filter(|value| *value > Decimal::ZERO)?;
                // USDⓈ-M 数量已经是标的币单位；乘价格即可，不能再套币本位合约的面值。
                let notional_usdt = price.checked_mul(qty)?;
                (notional_usdt > Decimal::ZERO).then_some(Level {
                    price,
                    notional_usdt,
                })
            })
            .collect()
    };
    let mut bids = parse_side(depth.bids);
    let mut asks = parse_side(depth.asks);
    // 上游当前有序，但吃单顺序是领域契约，不能依赖上游永远保持这一实现细节。
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|left| left.price);
    // 离散 limit 向上取整后只保留请求的最佳档位，而不是任意截取原始响应。
    bids.truncate(levels as usize);
    asks.truncate(levels as usize);
    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("{symbol} 的深度没有有效买卖盘"),
        ));
    };
    if ask.price < bid.price {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("{symbol} 的深度交叉：bid {} > ask {}", bid.price, ask.price),
        ));
    }
    Ok(OrderBook {
        venue: VENUE,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

/// 把 `premiumIndex` 的一行转成领域类型。`None` = 这一行不可用。
///
/// 抽成纯函数是为了让「符号过滤」「周期回落」「费率缺失」这三条最容易出错的规则
/// 能被单测覆盖，不必真的打网络。
///
/// 三处都**不猜**：
/// - 交割合约（符号带 `_`）与非 USDT 保证金合约直接排除。
/// - 费率字段缺失/不可解析 → 整行丢弃。**绝不回落成 0**：0 是一个合法的费率读数，
///   伪造出来的 0 会凭空造出巨大价差（实测有场所对同一合约反复返回 0 与真值，
///   撞上「0 状态」时算出的净 APR 能排到榜首，方向完全反）。
/// - 结算时刻无法解析 → 丢弃，而不是拿当前时间顶上。
fn parse_index_row(
    item: &PremiumIndex,
    intervals: &HashMap<String, u32>,
    book: Option<&BookTicker>,
) -> Option<MarketSnapshot> {
    let base = item
        .symbol
        .strip_suffix(QUOTE)
        .filter(|base| !base.is_empty())?;

    let period_rate = parse_decimal(&item.last_funding_rate)?;
    // `nextFundingTime = 0` 表示这家场所**没有**下一次结算（合约已下架或暂停）。
    // 直接交给 `from_timestamp_millis` 会得到一个 1970 年的合法时间戳，
    // 于是一条「1970 年结算」的假数据会一路进到面板。
    let next_funding_at = DateTime::from_timestamp_millis(item.next_funding_time)
        .filter(|_| item.next_funding_time > 0)?;

    let (interval_h, interval_assumed) = match intervals.get(item.symbol.as_str()) {
        Some(hours) => (*hours, false),
        None => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    let positive = |raw: Option<&str>| {
        raw.and_then(parse_decimal)
            .filter(|value| *value > Decimal::ZERO)
    };
    let mut best_bid = book.and_then(|book| positive(book.bid_price.as_deref()));
    let mut best_ask = book.and_then(|book| positive(book.ask_price.as_deref()));
    // 交叉盘会制造负的穿价成本；只丢盘口，不丢独立有效的资金费。
    if matches!((best_bid, best_ask), (Some(bid), Some(ask)) if ask < bid) {
        best_bid = None;
        best_ask = None;
    }
    // 数量缺失不等于零深度；溢出也只能视为未知，不能让整次扫描崩溃。
    let bid_size_usdt =
        best_bid.and_then(|bid| bid.checked_mul(positive(book?.bid_qty.as_deref())?));
    let ask_size_usdt =
        best_ask.and_then(|ask| ask.checked_mul(positive(book?.ask_qty.as_deref())?));

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: None,
        mark_price: parse_decimal(&item.mark_price),
        index_price: parse_decimal(&item.index_price),
        best_bid,
        best_ask,
        bid_size_usdt,
        ask_size_usdt,
        open_interest_usdt: None,
        quote_volume_24h: None,
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 是不是 USDⓈ-M 的线性永续。
///
/// 交割合约的符号带 `_`（如 `BTCUSDT_250926`），USDC 保证金的是 `*USDC`。
/// 两者混进来都会参与配对，但资金费机制与永续不同。
fn is_linear_perp(symbol: &str) -> bool {
    symbol.ends_with(QUOTE) && !symbol.contains('_') && symbol.len() > QUOTE.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str) -> PremiumIndex {
        PremiumIndex {
            symbol: symbol.into(),
            mark_price: "100.5".into(),
            index_price: "100.4".into(),
            last_funding_rate: "0.00010000".into(),
            next_funding_time: 1_789_833_600_000,
        }
    }

    #[test]
    fn only_usdt_margined_perps_are_kept() {
        assert!(is_linear_perp("BTCUSDT"));
        assert!(!is_linear_perp("BTCUSDT_250926"), "交割合约");
        assert!(!is_linear_perp("BTCUSDC"), "USDC 保证金");
        assert!(!is_linear_perp("USDT"), "没有 base");
    }

    #[test]
    fn a_zero_next_funding_time_drops_the_row_instead_of_becoming_epoch_zero() {
        let intervals = HashMap::new();
        let mut settling = row("MDTUSDT");
        settling.next_funding_time = 0;
        assert!(parse_index_row(&settling, &intervals, None).is_none());
    }

    #[test]
    fn reported_interval_is_used_and_absence_is_flagged_as_assumed() {
        let mut intervals = HashMap::new();
        intervals.insert("LPTUSDT".to_string(), 4u32);

        let reported = parse_index_row(&row("LPTUSDT"), &intervals, None).unwrap();
        assert_eq!(reported.interval_h, 4);
        assert!(!reported.interval_assumed);

        let assumed = parse_index_row(&row("BTCUSDT"), &intervals, None).unwrap();
        assert_eq!(assumed.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(assumed.interval_assumed, "回落值必须标记出来");
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        let intervals = HashMap::new();
        let mut broken = row("BTCUSDT");
        broken.last_funding_rate = String::new();
        assert!(parse_index_row(&broken, &intervals, None).is_none());

        let mut nan = row("BTCUSDT");
        nan.last_funding_rate = "n/a".into();
        assert!(parse_index_row(&nan, &intervals, None).is_none());
    }

    #[test]
    fn an_unparseable_settlement_time_drops_the_row() {
        let intervals = HashMap::new();
        let mut broken = row("BTCUSDT");
        broken.next_funding_time = i64::MAX;
        assert!(parse_index_row(&broken, &intervals, None).is_none());
    }

    // 2026-09-20 批量端点的真实片段，保留字符串精度与交割合约。
    const BOOK_FIXTURE: &str = r#"[
        {"symbol":"BTCUSDT","bidPrice":"80333.00","bidQty":"3.183","askPrice":"80333.10","askQty":"4.925","time":1789879692796,"lastUpdateId":11604208191120},
        {"symbol":"BTCUSDT_260925","bidPrice":"80440.7","bidQty":"0.032","askPrice":"80447.5","askQty":"0.008","time":1789879692421,"lastUpdateId":11604208149944}
    ]"#;

    fn book() -> BookTicker {
        serde_json::from_str::<Vec<BookTicker>>(BOOK_FIXTURE)
            .unwrap()
            .remove(0)
    }

    fn assert_no_book(snapshot: &MarketSnapshot) {
        assert_eq!(snapshot.best_bid, None);
        assert_eq!(snapshot.best_ask, None);
        assert_eq!(snapshot.bid_size_usdt, None);
        assert_eq!(snapshot.ask_size_usdt, None);
    }

    #[test]
    fn real_book_maps_prices_and_converts_each_side_to_usdt() {
        let books: Vec<BookTicker> = serde_json::from_str(BOOK_FIXTURE).unwrap();
        let books: HashMap<_, _> = books
            .iter()
            .map(|book| (book.symbol.as_str(), book))
            .collect();
        let index: PremiumIndex = serde_json::from_str(
            r#"{"symbol":"BTCUSDT","markPrice":"80362.00363768","indexPrice":"80375.15717391","estimatedSettlePrice":"80418.62522754","lastFundingRate":"0.00010000","interestRate":"0.00010000","nextFundingTime":1789891200000,"time":1789879738002}"#,
        )
        .unwrap();
        let snapshot = parse_index_row(
            &index,
            &HashMap::new(),
            books.get(index.symbol.as_str()).copied(),
        )
        .unwrap();
        assert_eq!(snapshot.best_bid, Some(Decimal::new(8_033_300, 2)));
        assert_eq!(snapshot.best_ask, Some(Decimal::new(8_033_310, 2)));
        assert_eq!(snapshot.bid_size_usdt, Some(Decimal::new(255_699_939, 3)));
        assert_eq!(snapshot.ask_size_usdt, Some(Decimal::new(3_956_405_175, 4)));
        assert_eq!(snapshot.period_rate, Decimal::new(1, 4));
        assert!(!is_linear_perp("BTCUSDT_260925"));
        assert!(
            parse_index_row(
                &row("BTCUSDT_260925"),
                &HashMap::new(),
                books.get("BTCUSDT_260925").copied(),
            )
            .is_none()
        );
    }

    #[test]
    fn absent_book_keeps_funding_without_substituting_mark_price() {
        let snapshot = parse_index_row(&row("BTCUSDT"), &HashMap::new(), None).unwrap();
        assert_no_book(&snapshot);
        assert_eq!(snapshot.period_rate, Decimal::new(1, 4));
        assert_eq!(snapshot.mark_price, Some(Decimal::new(1005, 1)));
    }

    #[test]
    fn crossed_book_is_discarded_without_dropping_funding() {
        let mut book = book();
        book.ask_price = Some("80332.90".into());
        let snapshot = parse_index_row(&row("BTCUSDT"), &HashMap::new(), Some(&book)).unwrap();
        assert_no_book(&snapshot);
        assert_eq!(snapshot.period_rate, Decimal::new(1, 4));
    }

    #[test]
    fn missing_null_and_invalid_fields_remain_unknown() {
        for key in ["bidPrice", "askPrice", "bidQty", "askQty"] {
            for value in [
                None,
                Some(serde_json::Value::Null),
                Some(serde_json::json!("")),
                Some(serde_json::json!("n/a")),
                Some(serde_json::json!("0")),
                Some(serde_json::json!("-1")),
            ] {
                let mut fixture: serde_json::Value = serde_json::from_str(BOOK_FIXTURE).unwrap();
                let object = fixture[0].as_object_mut().unwrap();
                if let Some(value) = value {
                    object.insert(key.into(), value);
                } else {
                    object.remove(key);
                }
                let books: Vec<BookTicker> = serde_json::from_value(fixture).unwrap();
                let snapshot =
                    parse_index_row(&row("BTCUSDT"), &HashMap::new(), Some(&books[0])).unwrap();
                match key {
                    "bidPrice" => {
                        assert_eq!(snapshot.best_bid, None);
                        assert_eq!(snapshot.bid_size_usdt, None);
                        assert_eq!(snapshot.best_ask, Some(Decimal::new(8_033_310, 2)));
                    }
                    "askPrice" => {
                        assert_eq!(snapshot.best_ask, None);
                        assert_eq!(snapshot.ask_size_usdt, None);
                        assert_eq!(snapshot.best_bid, Some(Decimal::new(8_033_300, 2)));
                    }
                    "bidQty" => {
                        assert_eq!(snapshot.bid_size_usdt, None);
                        assert_eq!(snapshot.best_bid, Some(Decimal::new(8_033_300, 2)));
                    }
                    "askQty" => {
                        assert_eq!(snapshot.ask_size_usdt, None);
                        assert_eq!(snapshot.best_ask, Some(Decimal::new(8_033_310, 2)));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    #[test]
    fn overflowing_notional_is_unknown_and_locked_book_is_valid() {
        let mut book = book();
        book.ask_price = book.bid_price.clone();
        book.bid_qty = Some(Decimal::MAX.to_string());
        let snapshot = parse_index_row(&row("BTCUSDT"), &HashMap::new(), Some(&book)).unwrap();
        assert_eq!(snapshot.best_bid, Some(Decimal::new(8_033_300, 2)));
        assert_eq!(snapshot.best_ask, snapshot.best_bid);
        assert_eq!(snapshot.bid_size_usdt, None);
        assert_eq!(snapshot.ask_size_usdt, Some(Decimal::new(395_640_025, 3)));
    }

    /// 实测：`premiumIndex` 里 SETTLING（已下架）的合约费率停在 0、结算时刻还是将来 —— 只有
    /// `status = TRADING` 的能下单，其余必须在 `exchangeInfo` 这一步被挡掉。
    #[test]
    fn only_trading_contracts_survive_the_status_filter() {
        let info: ExchangeInfo = serde_json::from_str(
            r#"{"timezone":"UTC","symbols":[
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL"},
                {"symbol":"FUNUSDT","status":"SETTLING","contractType":"PERPETUAL"},
                {"symbol":"NEWUSDT","status":"PENDING_TRADING","contractType":"PERPETUAL"}
            ]}"#,
        )
        .unwrap();
        let trading = trading_set(&info);
        assert!(trading.contains("BTCUSDT"));
        assert!(!trading.contains("FUNUSDT"), "已下架的合约不能当成可交易");
        assert!(!trading.contains("NEWUSDT"), "还没开始交易的也不算");
        assert_eq!(trading.len(), 1);
    }
}
