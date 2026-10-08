//! Ourbit 永续。
//!
//! 四个批量端点配合：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/api/v1/contract/funding_rate` | 每期费率、结算周期、下次结算时刻 | 吃单费率、价格 |
//! | `/api/v1/contract/detail` | 吃单费率、合约面值、保证金币种 | 费率 |
//! | `/api/v1/contract/ticker` | 标记价（`fairPrice`）、指数价、24h 成交额、持仓量 | 周期、一档量 |
//! | `/api/v1/contract/book_ticker` | 最优买卖价与一档量 | 周期、费率 |
//!
//! `ticker` 有 `bid1`/`ask1`，但没有一档量；批量 `book_ticker` 同时给价格与量，
//! 因而四个盘口字段都取自后者，避免把不同时刻的数量乘上另一份快照的价格。
//! `bidQty`/`askQty` 与逐合约 depth 首档第二项一致，单位是张：
//! 张数 × `detail.contractSize`（基础币/张）× 同侧价才是 USDT 名义。
//! 实测 BTC 买档 `124445 × 0.0001 × 80356.6 = 999997.7087 USDT`；
//! 把张数当基础币会放大 10000 倍。盘口或面值缺失时不猜量，也不使用标记价代替。
//!
//! 合约 API 与 MEXC 同构（同样的 `/api/v1/contract/*` 路径与字段名），但域名**必须实测**：
//! `api.ourbit.com` 返回 404，行情只在 `futures.ourbit.com` 上。
//!
//! 结算周期在 `collectCycle`，单位是**小时**。实测同一时刻 734 个合约里 1h 有 2 个
//! （`LSK_USDT`、`T_USDT`）、4h 有 449 个、8h 有 282 个、24h 有 1 个（`US30_USDT`）。
//! 若统一按 8h 折算，4h 合约的日化低估一半、1h 低估 8 倍，而排名照常算得出来。
//!
//! 吃单费率在 `detail` 的 `takerFeeRate`，公开可拿：实测 734 个合约全是 0.0004。
//!
//! 费率只认资金费端点：`ticker` 里也有一个 `fundingRate`，但它**不带周期**，
//! 用它取费率就退化成「周期靠猜」。`ticker` 只用来取价格与成交额。
//!
//! 深度（`/api/v1/contract/depth/{symbol}`）是**逐合约**端点，形状与上面四个批量端点不同：
//! `data` 是对象而不是数组，档位形如 `[价格, 张数, 挂单数]`，第二项与 `book_ticker` 的
//! `bidQty` 同口径（张），要乘 `detail.contractSize` 才是 USDT 名义。实测 `limit` 是
//! **每侧**档数（`limit=1` 只回一档），而 `limit=0` 被当成「不限」并回全量（284 档）——
//! 那正是逐合约端点最该避免的大响应，所以 0 档直接报错，不透传。
//! 带 `symbol` 参数的 `detail` 同样回**对象**（不带参数才回数组）：全表实测 1 MB，
//! 逐合约查询因此只剩一行。
//!
//! K 线（`/api/v1/contract/kline/{symbol}`）也是逐合约端点，形状又是第三种：
//! `data` 是**列式**的 —— `time`/`close`/`vol`… 各是一个等长数组，默认 2000 根。
//! 实测 `time` 是**秒**时间戳（`1789905600` = 2026-09-20 12:00 UTC），返回顺序已经是
//! 升序，但本地仍排一遍：顺序反了半衰期会算成一个看起来正常的正数，实际却是负相关 ——
//! 这类错误不报错，只会给出错误的持有期。
//!
//! 周期参数是**区分大小写的白名单字符串**，实测只有
//! `Min1/Min5/Min15/Min30/Min60/Hour4/Hour8/Day1/Week1/Month1`；
//! `Min120`、`Min240`、`Hour1`、`Hour2`、`Hour12`、`Day2`、`Day7`、`Week2`、`Min45`、`Min3`
//! 以及 `min1`/`MIN1` 这类大小写变体一律回
//! `{"success":false,"code":600,"message":"Param error!"}`。所以 1h 只能落在 `Min60`、
//! 2h/4h 只能落在 `Hour4`：请求周期落在两个合法值之间时取**不小于**它的最小合法值 ——
//! 宁可粗一点，也不返回比请求更细的序列，那会把半衰期算短、持有期被低估。
//!
//! `limit` 实测生效并回**最近** N 根（`limit=0` 回空数组，`limit=-1` 报 Param error，
//! 超过 2000 被截到 2000）；本地仍按 `limit` 再截一次 —— 端点哪天改成忽略它
//! （`size` 就是这样被静默忽略的），整个 2000 根的窗口会灌进下游的拟合。
//! 最后一根是尚未走完的当期 K 线，`close` 还会变。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, from_json_f64,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Ourbit;
const FUNDING_RATE_URL: &str = "https://futures.ourbit.com/api/v1/contract/funding_rate";
const DETAIL_URL: &str = "https://futures.ourbit.com/api/v1/contract/detail";
const TICKER_URL: &str = "https://futures.ourbit.com/api/v1/contract/ticker";
const BOOK_TICKER_URL: &str = "https://futures.ourbit.com/api/v1/contract/book_ticker";
const DEPTH_URL: &str = "https://futures.ourbit.com/api/v1/contract/depth";
const KLINE_URL: &str = "https://futures.ourbit.com/api/v1/contract/kline";

/// `kline` 的 `interval` 合法取值（分钟 → 字符串）。
const KLINE_INTERVALS: [(u32, &str); 7] = [
    (1, "Min1"),
    (5, "Min5"),
    (15, "Min15"),
    (30, "Min30"),
    (60, "Min60"),
    (240, "Hour4"),
    (1440, "Day1"),
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

/// 单对象 `data` 的外壳（`detail?symbol=` 与 `kline/{symbol}` 都是对象）。
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct SingleEnvelope<T> {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    code: Option<i64>,
}

impl<T> SingleEnvelope<T> {
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

/// 计价与保证金资产。实测 734 个合约的 `quoteCoin` 与 `settleCoin` 全是 USDT。
const QUOTE: &str = "USDT";

pub struct OurbitApi {
    client: Client,
}

impl OurbitApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 拉一个**可选**端点：失败只记 warn 并返回空表。
    ///
    /// `detail` / `ticker` / `book_ticker` 的附加字段缺失只会让对应字段变成
    /// 「未知」，不该让整家场所消失。但也不能静默 —— 吃单费率缺失会让排名回落到
    /// 配置的单边费率，降级必须在日志里看得见。
    async fn optional_rows<T: DeserializeOwned>(&self, url: &'static str) -> Vec<T> {
        let envelope = match get_json::<Envelope<Vec<T>>>(self.client.get(url), VENUE).await {
            Ok(envelope) => envelope,
            Err(error) => {
                warn!(venue = %VENUE, %error, url = %url, "端点不可用，对应字段按未知处理");
                return Vec::new();
            }
        };
        match unwrap_envelope(envelope) {
            Ok(rows) => rows,
            Err(error) => {
                warn!(venue = %VENUE, %error, url = %url, "端点返回业务失败，对应字段按未知处理");
                Vec::new()
            }
        }
    }

    /// 取每个合约的吃单费率、面值与行情快照。
    ///
    /// 这三个端点都允许失败：失败时对应字段留 `None`，主端点（资金费）失败才让
    /// 整家场所失败 —— 没有费率与周期就一行有效读数都产不出来。
    async fn fetch_markets(&self) -> HashMap<String, Market> {
        let details: Vec<DetailRow> = self.optional_rows(DETAIL_URL).await;
        let tickers: Vec<TickerRow> = self.optional_rows(TICKER_URL).await;
        let books: Vec<BookRow> = self.optional_rows(BOOK_TICKER_URL).await;
        merge_markets(details, tickers, books)
    }
}

/// Ourbit 的响应统一包在 `{success, code, data, message}` 里。
/// `T` 表示完整 data：批量端点是数组，逐合约 detail 与 depth 是对象，业务错误规则相同。
///
/// `success` 必须显式检查：接口失败时 HTTP 仍是 **200**，实测
/// `{"success":false,"code":1001,"message":"Contract does not exist!"}` —— 连 `data`
/// 字段都不存在。只看 HTTP 状态码会把「接口失败」当成「这家场所一个合约都没有」，
/// 场所就这样从排名里静默消失。
#[derive(Debug, Deserialize)]
// 同 okx：`#[serde(default)]` 会让 derive 给泛型参数加 `T: Default`，
// 而这里只需要 `T: Deserialize`。
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct Envelope<T> {
    success: bool,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
}

/// 拆包并检查 `success`。
fn unwrap_envelope<T>(envelope: Envelope<T>) -> ArbResult<T> {
    if !envelope.success {
        let code = envelope.code.unwrap_or_default();
        let message = envelope.message.as_deref().unwrap_or("响应里没有 message");
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("success=false（code={code}）：{message}"),
        ));
    }
    envelope
        .data
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "success=true 但响应里没有 data"))
}

/// `/api/v1/contract/funding_rate` 的一行。
///
/// 字段全是 JSON **number**（实测），不是字符串 —— 所以走 `from_json_f64`
/// 而不是 `parse_decimal`。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRow {
    symbol: String,
    funding_rate: Option<f64>,
    /// 结算周期，单位**小时**。
    collect_cycle: Option<u32>,
    /// 下次结算时刻，毫秒时间戳。
    next_settle_time: Option<i64>,
}

/// `/api/v1/contract/detail` 的一行。响应里还有几十个字段，这里只声明用得到的。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DetailRow {
    symbol: String,
    /// 吃单费率，JSON number（实测 0.0004）。
    taker_fee_rate: Option<f64>,
    /// 一张合约的面值，单位是**基础币**（实测 `BTC_USDT` 为 0.0001 BTC）。
    contract_size: Option<f64>,
}

/// `/api/v1/contract/ticker` 的一行。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerRow {
    symbol: String,
    /// 标记价。Ourbit 叫 `fairPrice`，**不是** `markPrice`。
    fair_price: Option<f64>,
    index_price: Option<f64>,
    /// 24h 成交额，已经是 USDT 计价（实测 BTC 约 74.5 亿）。
    amount24: Option<f64>,
    /// 持仓量，单位是**张**（要乘面值才是币）。
    hold_vol: Option<f64>,
}

/// 批量盘口的数量是张，不能像现货数量一样直接乘价格。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookRow {
    symbol: String,
    bid_price: Option<f64>,
    ask_price: Option<f64>,
    bid_qty: Option<f64>,
    ask_qty: Option<f64>,
}

/// 一个合约的附加信息。字段全是 `Option`：可选端点失败、或某一行缺字段，
/// 都只让对应字段变成「未知」。
#[derive(Debug, Default, Clone, Copy)]
struct Market {
    taker_fee: Option<Decimal>,
    contract_size: Option<Decimal>,
    mark_price: Option<Decimal>,
    index_price: Option<Decimal>,
    quote_volume_24h: Option<Decimal>,
    open_interest_usdt: Option<Decimal>,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    bid_size_usdt: Option<Decimal>,
    ask_size_usdt: Option<Decimal>,
}

/// 把 `detail`、`ticker` 与 `book_ticker` 按符号并成一张查询表。
///
/// 这张表只用来**按符号查**，不参与产生输出顺序：返回顺序由资金费端点的行序决定，
/// 与哈希表的迭代顺序无关。
fn merge_markets(
    details: Vec<DetailRow>,
    tickers: Vec<TickerRow>,
    books: Vec<BookRow>,
) -> HashMap<String, Market> {
    let mut markets: HashMap<String, Market> = HashMap::with_capacity(details.len());

    for DetailRow {
        symbol,
        taker_fee_rate,
        contract_size,
    } in details
    {
        let entry = markets.entry(symbol).or_default();
        entry.taker_fee = taker_fee_rate.and_then(from_json_f64);
        entry.contract_size = contract_size.and_then(from_json_f64);
    }

    for TickerRow {
        symbol,
        fair_price,
        index_price,
        amount24,
        hold_vol,
    } in tickers
    {
        let entry = markets.entry(symbol).or_default();
        entry.mark_price = fair_price.and_then(from_json_f64);
        entry.index_price = index_price.and_then(from_json_f64);
        entry.quote_volume_24h = amount24.and_then(from_json_f64);
        let open_interest = open_interest_usdt(hold_vol, entry.contract_size, entry.mark_price);
        entry.open_interest_usdt = open_interest;
    }

    for book in books {
        let entry = markets.entry(book.symbol).or_default();
        let bid = book
            .bid_price
            .and_then(from_json_f64)
            .filter(|p| *p > Decimal::ZERO);
        let ask = book
            .ask_price
            .and_then(from_json_f64)
            .filter(|p| *p > Decimal::ZERO);
        // 交叉盘不代表负成本；只丢掉矛盾的盘口，不影响资金费与其它行情字段。
        let (bid, ask) = match (bid, ask) {
            (Some(bid), Some(ask)) if ask < bid => (None, None),
            pair => pair,
        };
        entry.best_bid = bid;
        entry.best_ask = ask;
        entry.bid_size_usdt = book_size_usdt(book.bid_qty, entry.contract_size, bid);
        entry.ask_size_usdt = book_size_usdt(book.ask_qty, entry.contract_size, ask);
    }

    markets
}

/// 持仓量换算成 USDT 名义。
///
/// `holdVol` 的单位是**张**，不是币：张数 × 合约面值（基础币数量）× 标记价才是 USDT。
/// 实测 `BTC_USDT` 的 `holdVol = 678017`、`contractSize = 0.0001`、`fairPrice = 81267.9`
/// → 约 551 万 USDT；把张数直接当成 USDT 会差好几个数量级。
/// 三个分量缺一个就算不出来 —— 宁可留 `None`，也不能拿张数冒充 USDT。
fn open_interest_usdt(
    hold_vol: Option<f64>,
    contract_size: Option<Decimal>,
    mark_price: Option<Decimal>,
) -> Option<Decimal> {
    Some(from_json_f64(hold_vol?)? * contract_size? * mark_price?)
}

/// 少了面值或同侧价格就无法换算；负量不是深度，零量则仍是已知的零。
fn book_size_usdt(
    quantity: Option<f64>,
    contract_size: Option<Decimal>,
    price: Option<Decimal>,
) -> Option<Decimal> {
    let quantity = quantity
        .and_then(from_json_f64)
        .filter(|qty| *qty >= Decimal::ZERO)?;
    let contract_size = contract_size.filter(|size| *size > Decimal::ZERO)?;
    quantity.checked_mul(contract_size)?.checked_mul(price?)
}

/// 第三项不参与名义额计算；不为未使用的统计字段额外分配内存。
#[derive(Debug, Deserialize)]
struct DepthRow(f64, f64, serde::de::IgnoredAny);

#[derive(Debug, Deserialize)]
struct DepthData {
    bids: Vec<DepthRow>,
    asks: Vec<DepthRow>,
}

/// 缺面值不能把张数冒充 USDT；逐合约 detail 还必须属于请求的合约。
fn depth_contract_size(detail: &DetailRow, native_symbol: &str) -> ArbResult<Decimal> {
    if detail.symbol != native_symbol {
        return Err(ArbError::venue(VENUE.as_str(), "detail 返回了其它合约"));
    }
    detail
        .contract_size
        .and_then(from_json_f64)
        .filter(|size| *size > Decimal::ZERO)
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "缺少有效合约面值，无法换算深度"))
}

fn parse_depth(
    data: DepthData,
    symbol: &Symbol,
    contract_size: Decimal,
    levels: u32,
) -> ArbResult<OrderBook> {
    let convert = |rows: Vec<DepthRow>| -> Vec<Level> {
        rows.into_iter()
            .filter_map(|DepthRow(price, quantity, _)| {
                let price = from_json_f64(price).filter(|price| *price > Decimal::ZERO)?;
                // 零量档无法成交，不让它充当最优价或占用请求的档数。
                let notional_usdt =
                    book_size_usdt(Some(quantity), Some(contract_size), Some(price))?;
                (notional_usdt > Decimal::ZERO).then_some(Level {
                    price,
                    notional_usdt,
                })
            })
            .collect()
    };
    let mut bids = convert(data.bids);
    let mut asks = convert(data.asks);
    // 即使实测响应已排序，也不能让上游顺序变化变成下游的负滑点。
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|a| a.price);
    bids.truncate(levels as usize);
    asks.truncate(levels as usize);
    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
        return Err(ArbError::venue(VENUE.as_str(), "深度缺少有效买盘或卖盘"));
    };
    if ask.price < bid.price {
        return Err(ArbError::venue(
            VENUE.as_str(),
            "深度交叉：最优卖价低于最优买价",
        ));
    }
    Ok(OrderBook {
        venue: VENUE,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

#[async_trait]
impl VenueApi for OurbitApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        // 主端点：费率、结算周期、下次结算时刻都只在这里。它失败就没有一行有效读数，
        // 整家场所跟着失败 —— 返回空表看起来像「这家场所没有合约」，那是静默少扫一家。
        let envelope: Envelope<Vec<FundingRow>> =
            get_json(self.client.get(FUNDING_RATE_URL), VENUE).await?;
        let rows = unwrap_envelope(envelope)?;
        let markets = self.fetch_markets().await;

        let mut out = Vec::with_capacity(rows.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for row in rows {
            if perp_base(&row.symbol).is_none() {
                filtered += 1;
                continue;
            }
            match parse_row(&row, &markets) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        // 两种「少了一条」要分开报：符号过滤是预期内的（交割合约、非 USDT 保证金），
        // 字段不可用则说明数据源变了或合约已下架，必须能一眼区分。
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非 USDT 本位线性永续已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        // limit=0 实测返回全量，不能把零档请求扩大成整个盘口。
        if levels == 0 {
            return Err(ArbError::config("Ourbit 深度档数必须大于零"));
        }
        let native_symbol = format!("{}_{}", symbol.base, symbol.quote);
        if symbol.quote != QUOTE || perp_base(&native_symbol) != Some(symbol.base.as_str()) {
            return Err(ArbError::config("Ourbit 深度仅支持 USDT 本位永续"));
        }
        // 带 symbol 的 detail 回单个对象，避免每次深度检查都下载约 1 MB 的合约全表。
        let detail: Envelope<DetailRow> = get_json(
            self.client
                .get(DETAIL_URL)
                .query(&[("symbol", native_symbol.as_str())]),
            VENUE,
        )
        .await?;
        let contract_size = depth_contract_size(&unwrap_envelope(detail)?, &native_symbol)?;
        let depth: Envelope<DepthData> = get_json(
            self.client
                .get(format!("{DEPTH_URL}/{native_symbol}"))
                .query(&[("limit", levels)]),
            VENUE,
        )
        .await?;
        parse_depth(unwrap_envelope(depth)?, symbol, contract_size, levels)
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
        let envelope: SingleEnvelope<KlineData> = get_json(self.client.get(&url), VENUE).await?;
        let data = envelope.into_data("K 线")?;
        parse_candles(&data, limit as usize)
    }
}

/// 把资金费端点的一行转成领域类型。`None` = 这一行不可用。
///
/// 抽成纯函数是为了让「周期回落」「费率缺失」「结算时刻缺失」这三条最容易出错的规则
/// 能被单测覆盖，不必真的打网络。
///
/// 三处都**不猜**：
/// - 费率字段缺失或不可解析 → 整行丢弃。**绝不回落成 0**：0 是合法读数（实测
///   `US30_USDT` 就是 0），伪造的 0 会凭空造出巨大价差。
/// - 结算时刻缺失或 ≤ 0 → 丢弃，而不是拿当前时间顶上。
/// - 周期缺失或为 0 → 回落默认值并置 `interval_assumed`，让面板看得见这个假设。
fn parse_row(row: &FundingRow, markets: &HashMap<String, Market>) -> Option<MarketSnapshot> {
    let base = perp_base(&row.symbol)?;

    let period_rate = from_json_f64(row.funding_rate?)?;
    // `nextSettleTime = 0` 表示这家场所没有下一次结算（合约已下架或暂停）。
    // 直接交给 `from_timestamp_millis` 会得到一个 1970 年的合法时间戳，
    // 于是一条「1970 年结算」的假数据会一路进到面板。
    let next_settle_time = row.next_settle_time.filter(|ms| *ms > 0)?;
    let next_funding_at = DateTime::from_timestamp_millis(next_settle_time)?;

    // 0 小时的周期会让日化除零，只能当成「场所没给」。
    let (interval_h, interval_assumed) = match row.collect_cycle.filter(|hours| *hours > 0) {
        Some(hours) => (hours, false),
        None => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    // 表里查不到这个符号（可选端点失败，或该合约没被它们收录）时只有附加字段是未知的：
    // 费率与周期来自主端点，这一行仍然有效。
    let market = markets.get(&row.symbol).copied().unwrap_or_default();

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: market.taker_fee,
        mark_price: market.mark_price,
        index_price: market.index_price,
        best_bid: market.best_bid,
        best_ask: market.best_ask,
        bid_size_usdt: market.bid_size_usdt,
        ask_size_usdt: market.ask_size_usdt,
        open_interest_usdt: market.open_interest_usdt,
        quote_volume_24h: market.quote_volume_24h,
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 是不是 USDT 本位线性永续；是则返回基础币。
///
/// Ourbit（同 MEXC）的永续符号是 `BASE_QUOTE`，如 `BTC_USDT`。交割合约会多一段日期
/// （`BTC_USDT_250926`），反向合约的计价不是 USDT（`BTC_USD`），USDC 保证金是
/// `BTC_USDC` —— 这些的资金费机制与永续不同，混进来会被拿去和真永续配对。
fn perp_base(symbol: &str) -> Option<&str> {
    let (base, quote) = symbol.split_once('_')?;
    (quote == QUOTE && !base.is_empty()).then_some(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, estimate_fill, parse_decimal};

    /// 真实响应片段：`curl -sS https://futures.ourbit.com/api/v1/contract/funding_rate`
    /// 里的三行 —— `BTC_USDT`（8h）、`LSK_USDT`（1h）、`US30_USDT`（24h，费率恰好为 0）。
    const FUNDING_ROWS: &str = r#"[
        {"symbol":"BTC_USDT","fundingRate":0.0001,"maxFundingRate":0.002,"minFundingRate":-0.002,"collectCycle":8,"nextSettleTime":1789833600000,"timestamp":1789823851605},
        {"symbol":"LSK_USDT","fundingRate":-0.001328,"maxFundingRate":0.02,"minFundingRate":-0.02,"collectCycle":1,"nextSettleTime":1789826400000,"timestamp":1789823851605},
        {"symbol":"US30_USDT","fundingRate":0,"maxFundingRate":0.02,"minFundingRate":-0.02,"collectCycle":24,"nextSettleTime":1789833600000,"timestamp":1789823851606}
    ]"#;

    /// 真实响应片段：`/api/v1/contract/detail` 的 `BTC_USDT` 行。线上这一行还有
    /// `riskTableItems`、`indexOrigin` 等几十个字段，serde 会忽略它们。
    const DETAIL_ROW: &str = r#"{"symbol":"BTC_USDT","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","futureType":1,"contractSize":0.0001,"takerFeeRate":0.0004,"makerFeeRate":0.0002,"state":0}"#;

    /// 真实响应片段：`/api/v1/contract/ticker` 的 `BTC_USDT` 行（原样保留嵌套的
    /// `riseFallRates` 等用不到的字段）。
    const TICKER_ROW: &str = r#"{"contractId":10,"symbol":"BTC_USDT","lastPrice":81268,"bid1":81267.9,"ask1":81268,"volume24":924381756,"amount24":7450984438.79259,"holdVol":678017,"lower24Price":77927,"high24Price":81720.7,"riseFallRate":0.0071,"riseFallValue":576.5,"indexPrice":81301.4,"fairPrice":81267.9,"fundingRate":0.0001,"maxBidPrice":89431.5,"minAskPrice":73171.2,"timestamp":1789823847475,"riseFallRates":{"zone":"UTC+8","r":0.0071,"v":576.5,"r7":0.0461,"r30":0.1858,"r90":0.2672,"r180":0.1809,"r365":-0.3085},"riseFallRatesOfTimezone":[0.0418,0.005,0.0071]}"#;

    /// 同一批 book_ticker 的实测两行：面值不同，能检出漏乘或写死面值的错误。
    const BOOK_ROWS: &str = r#"[
        {"symbol":"BTC_USDT","bidPrice":80356.6,"bidQty":124445,"askPrice":80356.7,"askQty":124416},
        {"symbol":"ETH_USDT","bidPrice":2577,"bidQty":23168,"askPrice":2577.01,"askQty":38594}
    ]"#;

    /// 实测 limit=5 的完整响应；用真实张数钉住换算，避免只验证看似合理的量级。
    const DEPTH_RESPONSE: &str = r#"{"success":true,"code":0,"data":{"asks":[[80450.1,67816,1],[80450.2,47517,2],[80450.3,70874,2],[80450.4,26855,2],[80450.5,33043,1]],"bids":[[80450,85067,1],[80449.9,54938,1],[80449.8,31559,1],[80449.7,67811,3],[80449.6,35086,2]],"version":12468467790,"timestamp":1789898051072}}"#;

    fn depth_fixture() -> DepthData {
        let envelope: Envelope<DepthData> = serde_json::from_str(DEPTH_RESPONSE).unwrap();
        unwrap_envelope(envelope).unwrap()
    }

    fn fixture_book(data: DepthData, levels: u32) -> ArbResult<OrderBook> {
        // symbol 参数使 detail.data 从数组变成对象；复用拆包逻辑而不是另写一份成功判断。
        let envelope: Envelope<DetailRow> = serde_json::from_str(&format!(
            r#"{{"success":true,"code":0,"data":{DETAIL_ROW}}}"#
        ))
        .unwrap();
        let size = depth_contract_size(&unwrap_envelope(envelope)?, "BTC_USDT")?;
        parse_depth(data, &Symbol::perp("BTC", "USDT"), size, levels)
    }

    #[test]
    fn depth_converts_contracts_sorts_and_limits_before_consumption() {
        let mut data = depth_fixture();
        data.bids.reverse();
        data.asks.reverse();
        let book = fixture_book(data, 2).unwrap();
        assert_eq!(book.venue, VENUE);
        assert_eq!(book.symbol, Symbol::perp("BTC", "USDT"));
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        assert_eq!(book.bids[0].price, parse_decimal("80450").unwrap());
        assert_eq!(book.bids[1].price, parse_decimal("80449.9").unwrap());
        assert_eq!(book.asks[0].price, parse_decimal("80450.1").unwrap());
        assert_eq!(book.asks[1].price, parse_decimal("80450.2").unwrap());
        // 85067 张是 8.5067 BTC，而不是 85067 BTC 或 85067 USDT。
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("684364.015").unwrap()
        );
        assert_eq!(
            book.bids[1].notional_usdt,
            parse_decimal("441975.66062").unwrap()
        );
        assert_eq!(
            book.asks[0].notional_usdt,
            parse_decimal("545580.39816").unwrap()
        );
        assert_eq!(
            book.asks[1].notional_usdt,
            parse_decimal("382275.21534").unwrap()
        );
    }

    #[test]
    fn depth_rejects_empty_crossed_and_unusable_sides() {
        let mut empty_bids = depth_fixture();
        empty_bids.bids.clear();
        assert!(fixture_book(empty_bids, 5).is_err());
        let mut empty_asks = depth_fixture();
        empty_asks.asks.clear();
        assert!(fixture_book(empty_asks, 5).is_err());
        let mut crossed = depth_fixture();
        crossed.asks[4].0 = 80449.0;
        assert!(fixture_book(crossed, 5).is_err());
        let mut unusable = depth_fixture();
        for row in &mut unusable.bids {
            row.1 = 0.0;
        }
        assert!(fixture_book(unusable, 5).is_err());
        assert!(fixture_book(depth_fixture(), 0).is_err());

        let mut locked = depth_fixture();
        locked.asks[0].0 = locked.bids[0].0;
        let book = fixture_book(locked, 5).unwrap();
        assert_eq!(book.best_bid(), book.best_ask());
    }

    #[test]
    fn depth_requires_the_requested_contracts_positive_face_value() {
        let mut detail: DetailRow = serde_json::from_str(DETAIL_ROW).unwrap();
        detail.contract_size = None;
        assert!(depth_contract_size(&detail, "BTC_USDT").is_err());
        detail.contract_size = Some(0.0);
        assert!(depth_contract_size(&detail, "BTC_USDT").is_err());
        detail.contract_size = Some(-0.0001);
        assert!(depth_contract_size(&detail, "BTC_USDT").is_err());
        detail.contract_size = Some(0.0001);
        assert!(depth_contract_size(&detail, "ETH_USDT").is_err());
        // ETH 的真实面值为 0.01，不能把 BTC 的 0.0001 写死到深度换算里。
        detail.symbol = "ETH_USDT".into();
        detail.contract_size = Some(0.01);
        let size = depth_contract_size(&detail, "ETH_USDT").unwrap();
        let data = serde_json::from_str(r#"{"bids":[[2577,23168,1]],"asks":[[2577.01,38594,1]]}"#)
            .unwrap();
        let book = parse_depth(data, &Symbol::perp("ETH", "USDT"), size, 1).unwrap();
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("597039.36").unwrap()
        );
        assert_eq!(
            book.asks[0].notional_usdt,
            parse_decimal("994571.2394").unwrap()
        );
    }

    #[test]
    fn depth_business_failure_and_missing_data_are_errors() {
        for raw in [
            r#"{"success":false,"code":1001,"message":"Contract does not exist!"}"#,
            r#"{"success":true,"code":0}"#,
        ] {
            let envelope: Envelope<DepthData> = serde_json::from_str(raw).unwrap();
            assert!(unwrap_envelope(envelope).is_err());
        }
    }

    #[test]
    fn depth_fixture_estimates_quote_fill_and_adverse_slippage() {
        let book = fixture_book(depth_fixture(), 5).unwrap();
        let requested = parse_decimal("900000").unwrap();
        let buy = estimate_fill(&book.asks, requested, Side::Buy).unwrap();
        assert_eq!(buy.filled_usdt, requested);
        assert!(!buy.exhausted);
        assert!(buy.average_price > book.asks[0].price);
        assert!(buy.average_price < book.asks[1].price);
        assert!(buy.slippage > Decimal::ZERO);
        let sell = estimate_fill(&book.bids, requested, Side::Sell).unwrap();
        assert_eq!(sell.filled_usdt, requested);
        assert!(sell.average_price < book.bids[0].price);
        assert!(sell.slippage > Decimal::ZERO);
        let large =
            estimate_fill(&book.asks, parse_decimal("5000000").unwrap(), Side::Buy).unwrap();
        assert_eq!(large.filled_usdt, parse_decimal("1979921.20607").unwrap());
        assert!(large.exhausted);
    }

    fn funding_rows() -> Vec<FundingRow> {
        serde_json::from_str(FUNDING_ROWS).unwrap()
    }

    fn markets() -> HashMap<String, Market> {
        merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            vec![serde_json::from_str(TICKER_ROW).unwrap()],
            serde_json::from_str(BOOK_ROWS).unwrap(),
        )
    }

    /// 一行字段齐全的读数，用来单独破坏某一个字段。
    fn row(symbol: &str) -> FundingRow {
        FundingRow {
            symbol: symbol.into(),
            funding_rate: Some(0.0001),
            collect_cycle: Some(8),
            next_settle_time: Some(1_789_833_600_000),
        }
    }

    #[test]
    fn only_usdt_margined_perps_are_kept() {
        assert_eq!(perp_base("BTC_USDT"), Some("BTC"));
        assert_eq!(
            perp_base("US30_USDT"),
            Some("US30"),
            "指数永续也是线性 USDT 永续"
        );
        assert!(perp_base("BTC_USDT_250926").is_none(), "交割合约");
        assert!(perp_base("BTC_USD").is_none(), "反向合约");
        assert!(perp_base("BTC_USDC").is_none(), "USDC 保证金");
        assert!(perp_base("USDT").is_none(), "没有 base");
        assert!(perp_base("_USDT").is_none(), "没有 base");
    }

    #[test]
    fn reported_interval_is_used_and_absence_is_flagged_as_assumed() {
        let markets = markets();
        let rows = funding_rows();

        let btc = parse_row(&rows[0], &markets).unwrap();
        assert_eq!(btc.interval_h, 8);
        assert!(!btc.interval_assumed);
        assert_eq!(btc.next_funding_at.timestamp_millis(), 1_789_833_600_000);
        assert!(!btc.next_funding_estimated, "结算时刻是场所给的");

        // 同一时刻的 1h 合约：把它当成 8h 会让日化低估 8 倍。
        let lsk = parse_row(&rows[1], &markets).unwrap();
        assert_eq!(lsk.interval_h, 1);
        assert!(!lsk.interval_assumed);

        let us30 = parse_row(&rows[2], &markets).unwrap();
        assert_eq!(us30.interval_h, 24);
        assert!(!us30.interval_assumed);

        // 场所没给周期（字段缺失或为 0）→ 回落默认值，但必须标记出来。
        let mut missing = row("BTC_USDT");
        missing.collect_cycle = None;
        let assumed = parse_row(&missing, &markets).unwrap();
        assert_eq!(assumed.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(assumed.interval_assumed, "回落值必须标记出来");

        let mut zero = row("BTC_USDT");
        zero.collect_cycle = Some(0);
        let assumed = parse_row(&zero, &markets).unwrap();
        assert_eq!(assumed.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(assumed.interval_assumed, "0 小时的周期会除零，只能当成没给");
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        let markets = markets();

        let mut missing = row("BTC_USDT");
        missing.funding_rate = None;
        assert!(parse_row(&missing, &markets).is_none());

        let mut not_a_number = row("BTC_USDT");
        not_a_number.funding_rate = Some(f64::NAN);
        assert!(parse_row(&not_a_number, &markets).is_none(), "NaN 不是费率");

        // 反过来：0 是合法读数（实测 `US30_USDT` 就是 0），不能被当成「缺失」丢掉。
        let us30 = parse_row(&funding_rows()[2], &markets).unwrap();
        assert_eq!(us30.period_rate, Decimal::ZERO);
    }

    #[test]
    fn a_missing_settlement_time_drops_the_row() {
        let markets = markets();

        let mut missing = row("BTC_USDT");
        missing.next_settle_time = None;
        assert!(parse_row(&missing, &markets).is_none());

        let mut zero = row("BTC_USDT");
        zero.next_settle_time = Some(0);
        assert!(
            parse_row(&zero, &markets).is_none(),
            "0 不能变成 1970 年结算"
        );
    }

    #[test]
    fn taker_fee_is_read_from_detail_and_zero_stays_known() {
        let markets = markets();
        let btc = parse_row(&funding_rows()[0], &markets).unwrap();
        assert_eq!(btc.taker_fee, parse_decimal("0.0004"));

        // 报 0 = 已知为 0；拿不到 = 未知。两者不能混。
        let zero_fee: DetailRow =
            serde_json::from_str(r#"{"symbol":"BTC_USDT","takerFeeRate":0}"#).unwrap();
        let markets = merge_markets(vec![zero_fee], Vec::new(), Vec::new());
        let known_zero = parse_row(&funding_rows()[0], &markets).unwrap();
        assert_eq!(known_zero.taker_fee, Some(Decimal::ZERO));

        let markets = merge_markets(Vec::new(), Vec::new(), Vec::new());
        let unknown = parse_row(&funding_rows()[0], &markets).unwrap();
        assert_eq!(unknown.taker_fee, None, "拿不到就必须是 None，不能猜");
    }

    /// 吃单费率是 JSON number，落到 `f64` 会带噪声。实测接口报的 `0.0004` 与
    /// `0.00020000000000000001` 这类字面量都长这样，去噪必须在连接器这一层做完，
    /// 否则噪声会一路进到排名比较里。
    #[test]
    fn a_noisy_taker_fee_is_rounded_to_the_declared_precision() {
        let noisy: DetailRow =
            serde_json::from_str(r#"{"symbol":"BTC_USDT","takerFeeRate":0.00020000000000000001}"#)
                .unwrap();
        let markets = merge_markets(vec![noisy], Vec::new(), Vec::new());
        assert_eq!(markets["BTC_USDT"].taker_fee, parse_decimal("0.0002"));
    }

    #[test]
    fn book_prices_and_contract_quantities_become_quote_notional() {
        let details = serde_json::from_str(
            r#"[{"symbol":"ETH_USDT","contractSize":0.01},
                {"symbol":"BTC_USDT","contractSize":0.0001}]"#,
        )
        .unwrap();
        let markets = merge_markets(
            details,
            Vec::new(),
            serde_json::from_str(BOOK_ROWS).unwrap(),
        );
        // detail 与盘口的顺序不同，必须按 symbol 取各自面值，而不是按下标配对。
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.best_bid, parse_decimal("80356.6"));
        assert_eq!(btc.best_ask, parse_decimal("80356.7"));
        assert_eq!(btc.bid_size_usdt, parse_decimal("999997.7087"));
        assert_eq!(btc.ask_size_usdt, parse_decimal("999765.91872"));
        let eth = parse_row(&row("ETH_USDT"), &markets).unwrap();
        assert_eq!(eth.best_bid, parse_decimal("2577"));
        assert_eq!(eth.best_ask, parse_decimal("2577.01"));
        assert_eq!(eth.bid_size_usdt, parse_decimal("597039.36"));
        assert_eq!(eth.ask_size_usdt, parse_decimal("994571.2394"));
    }

    #[test]
    fn missing_book_does_not_reuse_ticker_or_mark_prices() {
        let markets = merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            vec![serde_json::from_str(TICKER_ROW).unwrap()],
            Vec::new(),
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.mark_price, parse_decimal("81267.9"));
        assert_eq!((btc.best_bid, btc.best_ask), (None, None));
        assert_eq!((btc.bid_size_usdt, btc.ask_size_usdt), (None, None));
    }

    #[test]
    fn missing_contract_size_preserves_prices_without_guessing_notional() {
        let markets = merge_markets(
            Vec::new(),
            Vec::new(),
            serde_json::from_str(BOOK_ROWS).unwrap(),
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.best_bid, parse_decimal("80356.6"));
        assert_eq!(btc.best_ask, parse_decimal("80356.7"));
        assert_eq!((btc.bid_size_usdt, btc.ask_size_usdt), (None, None));
    }

    #[test]
    fn missing_fields_degrade_only_the_affected_side() {
        let book = serde_json::from_str(
            r#"{"symbol":"BTC_USDT","bidPrice":80356.6,"askPrice":null,"askQty":124416}"#,
        )
        .unwrap();
        let markets = merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            Vec::new(),
            vec![book],
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.best_bid, parse_decimal("80356.6"));
        assert_eq!(btc.best_ask, None);
        assert_eq!((btc.bid_size_usdt, btc.ask_size_usdt), (None, None));
    }

    #[test]
    fn crossed_book_is_discarded_without_losing_funding() {
        let mut books: Vec<BookRow> = serde_json::from_str(BOOK_ROWS).unwrap();
        books[0].ask_price = Some(80356.5);
        let markets = merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            Vec::new(),
            books,
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!((btc.best_bid, btc.best_ask), (None, None));
        assert_eq!((btc.bid_size_usdt, btc.ask_size_usdt), (None, None));
        assert_eq!(btc.period_rate, parse_decimal("0.0001").unwrap());
        assert_eq!(btc.interval_h, 8);
    }

    #[test]
    fn locked_prices_and_zero_quantity_remain_known() {
        let book = serde_json::from_str(
            r#"{"symbol":"BTC_USDT","bidPrice":100,"askPrice":100,"bidQty":0,"askQty":2}"#,
        )
        .unwrap();
        let markets = merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            Vec::new(),
            vec![book],
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.relative_spread(), Some(Decimal::ZERO));
        assert_eq!(btc.bid_size_usdt, Some(Decimal::ZERO));
        assert_eq!(btc.ask_size_usdt, parse_decimal("0.02"));
    }

    #[test]
    fn invalid_book_values_cannot_create_negative_notional() {
        let book = serde_json::from_str(
            r#"{"symbol":"BTC_USDT","bidPrice":0,"askPrice":100,"bidQty":2,"askQty":-1}"#,
        )
        .unwrap();
        let markets = merge_markets(
            vec![serde_json::from_str(DETAIL_ROW).unwrap()],
            Vec::new(),
            vec![book],
        );
        let btc = parse_row(&row("BTC_USDT"), &markets).unwrap();
        assert_eq!(btc.best_bid, None);
        assert_eq!(btc.best_ask, parse_decimal("100"));
        assert_eq!((btc.bid_size_usdt, btc.ask_size_usdt), (None, None));
    }

    #[test]
    fn ticker_fields_are_mapped_and_open_interest_becomes_usdt_notional() {
        let btc = parse_row(&funding_rows()[0], &markets()).unwrap();
        assert_eq!(btc.mark_price, parse_decimal("81267.9"));
        assert_eq!(btc.index_price, parse_decimal("81301.4"));

        // 678017 张 × 0.0001 BTC × 81267.9 = 5510101.77543 USDT。
        // 少了面值这一步会得到 5.5e10，量级完全不同。
        assert_eq!(btc.open_interest_usdt, parse_decimal("5510101.77543"));

        // `amount24` 是 USDT 计价（74.5 亿），`volume24` 是张数（9.2 亿）：
        // 拿错字段差一个数量级，所以只断言量级 —— f64 落地后小数第 7 位起就是噪声，
        // 10 位去噪救不回来，精确值断言会变成对浮点表示的断言。
        let volume = btc.quote_volume_24h.unwrap();
        assert!(
            volume > Decimal::from(7_400_000_000u64) && volume < Decimal::from(7_500_000_000u64),
            "{volume}"
        );
    }

    /// 失败响应是 HTTP 200 + `{"success":false,...}`，而且**没有** `data` 字段。
    /// 只看 HTTP 状态码会把它当成「这家场所没有合约」。
    #[test]
    fn a_failed_envelope_is_an_error_not_an_empty_venue() {
        let failed: Envelope<Vec<FundingRow>> = serde_json::from_str(
            r#"{"success":false,"code":1001,"message":"Contract does not exist!"}"#,
        )
        .unwrap();
        let error = unwrap_envelope(failed).unwrap_err();
        assert!(error.to_string().contains("1001"), "{error}");

        let ok: Envelope<Vec<FundingRow>> = serde_json::from_str(&format!(
            r#"{{"success":true,"code":0,"data":{FUNDING_ROWS}}}"#
        ))
        .unwrap();
        assert_eq!(unwrap_envelope(ok).unwrap().len(), 3);
    }
}
