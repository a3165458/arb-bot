//! Bitget USDT 永续（`productType=usdt-futures`）。
//!
//! 三个端点配合，各自补对方的空缺：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/api/v2/mix/market/tickers` | 一档买卖价与量、标记价、指数价、持仓量、24h 成交额 | **结算周期** |
//! | `/api/v2/mix/market/current-fund-rate` | 每期费率、`fundingRateInterval`、`nextUpdate` | 价格、持仓 |
//! | `/api/v2/mix/market/contracts` | `fundInterval`、`takerFeeRate` | 实时读数 |
//!
//! # 结算周期不是全场所统一值
//!
//! 实测（2026-09-19）809 个合约里 **1h / 4h / 8h 并存**：`ONEUSDT` / `LSKUSDT` /
//! `TUSDT` 是 1h，`BTCUSDT` 是 8h，另有 386 个 4h。把 Bitget 当成「一家 1h 场所」
//! 会让 8h 合约的日化虚高 8 倍，当成「一家 8h 场所」则让 1h 合约虚低 8 倍 ——
//! 两种都只会静默改变排名，所以周期必须**逐合约**读。
//!
//! `current-fund-rate` 同行给出费率、周期和下次结算时刻，避免混用不同端点的快照。
//! `tickers` 也有费率，但不用于本连接器的资金费读数。
//!
//! 吃单费率（`takerFeeRate`，字符串，实测全部为 `0.0006`）公开可读，不需要签名接口。
//!
//! `bidPr` / `askPr` 分别是我们能卖到 / 买到的价；`bidSz` / `askSz` 是基础币量，
//! 因此要乘各自那侧的价格才是 USDT 名义，不能再乘下单数量步长 `sizeMultiplier`。
//! 直接复用批量行情，避免逐合约拉盘口；缺失或坏字段保留未知，不拿标记价补齐。
//! 交叉盘（ask < bid）四个盘口字段一起清空，但保留独立的资金费读数。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Bitget;

/// 行情批量端点：一档买卖价与量、标记价、指数价、持仓量、24h 成交额。
const TICKERS_URL: &str =
    "https://api.bitget.com/api/v2/mix/market/tickers?productType=usdt-futures";
/// 资金费批量端点：每期费率 + 结算周期 + 下次结算时刻。
const CURRENT_FUND_RATE_URL: &str =
    "https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures";
/// 合约配置端点：`fundInterval` 与 `takerFeeRate`。
const CONTRACTS_URL: &str =
    "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures";

/// 计价资产。`usdt-futures` 只有 USDT 保证金。
const QUOTE: &str = "USDT";

/// Bitget 的成功码。它**不体现在 HTTP 状态上**：参数错误会返回 200 +
/// `code=400172` + `data=null`，只看 HTTP 状态会把「请求被拒」当成「这个场所没有合约」。
const SUCCESS_CODE: &str = "00000";

pub struct BitgetApi {
    client: Client,
}

impl BitgetApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 取一个列表端点。
    async fn fetch_list<T: DeserializeOwned>(&self, url: &str) -> ArbResult<Vec<T>> {
        let envelope: Envelope<T> = get_json(self.client.get(url), VENUE).await?;
        unwrap_envelope(envelope)
    }
}

/// Bitget 的统一包装：`{code, msg, requestTime, data}`。
#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: String,
    msg: String,
    data: Option<Vec<T>>,
}

/// 校验包装并取出 `data`。
///
/// 抽成纯函数是为了让「HTTP 200 但 code 是错的」这条被单测钉住：不显式检查的话，
/// `data: null` 只会得到一句「期望数组却拿到 null」的反序列化错误，真正的原因
/// （`msg`）被丢掉，排查方向会完全跑偏。
fn unwrap_envelope<T>(envelope: Envelope<T>) -> ArbResult<Vec<T>> {
    if envelope.code != SUCCESS_CODE {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("code={}：{}", envelope.code, envelope.msg),
        ));
    }
    envelope.data.ok_or_else(|| {
        ArbError::venue(
            VENUE.as_str(),
            format!("code={} 但 data 为空", envelope.code),
        )
    })
}

/// `tickers` 的一行。数值字段全是**字符串**。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker {
    symbol: String,
    mark_price: Option<String>,
    index_price: Option<String>,
    bid_pr: Option<String>,
    ask_pr: Option<String>,
    bid_sz: Option<String>,
    ask_sz: Option<String>,
    /// 持仓量，单位是**基础币**（实测与 `/market/open-interest` 的 `size` 一致）。
    holding_amount: Option<String>,
    /// 24h 成交额。USDT 永续下它与 `usdtVolume` 逐行相等（实测 797/797），
    /// 取这个与字段名（quote 口径）一致。
    quote_volume: Option<String>,
}

/// `current-fund-rate` 的一行。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentFundRate {
    symbol: String,
    funding_rate: Option<String>,
    /// 结算周期（小时）。
    funding_rate_interval: Option<String>,
    /// 下次结算时刻，毫秒时间戳字符串。
    next_update: Option<String>,
}

/// `contracts` 的一行。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Contract {
    symbol: String,
    /// 结算周期（小时）。与 `current-fund-rate` 的 `fundingRateInterval` 逐行一致
    /// （实测 797/797），作为兜底来源。
    fund_interval: Option<String>,
    taker_fee_rate: Option<String>,
    symbol_type: Option<String>,
    quote_coin: Option<String>,
    support_margin_coins: Option<Vec<String>>,
}

impl Contract {
    fn is_linear_perp(&self) -> bool {
        self.symbol_type.as_deref() == Some("perpetual")
            && self.quote_coin.as_deref() == Some(QUOTE)
            && self
                .support_margin_coins
                .as_ref()
                .is_some_and(|coins| coins.iter().any(|coin| coin == QUOTE))
    }
}

const MERGE_DEPTH_URL: &str =
    "https://api.bitget.com/api/v2/mix/market/merge-depth?productType=usdt-futures";
const CANDLES_URL: &str =
    "https://api.bitget.com/api/v2/mix/market/candles?productType=usdt-futures";

/// 一根 K 线里收盘价的下标。元素顺序是 `[开盘毫秒, 开, 高, 低, 收, 基础币量, 计价额]`，
/// 取错下标会拿到最高价或成交量 —— 量级看着也对，只有小数位不同，所以显式命名。
const CANDLE_CLOSE_INDEX: usize = 4;

/// `limit` 的合法上界。实测区间是 `(0, 1000]`：`limit=0` 与 `limit=1001` 都返回
/// `code=40053`，所以请求前要夹到这个区间里。
const MAX_CANDLES: u32 = 1000;

/// Bitget 的 `granularity` 合法档位与各自的分钟数。**大小写敏感**。
///
/// 只收录端点自报的合法集合（`code=400171` 的 `msg` 里逐字列出的那串）。
/// 实测 `2H` / `3D` 也能用，但它们不在自报集合里，随时可能被收紧；不用它们最多让
/// (1H,4H] 与 (1D,3D] 的请求粗一档 —— 那正是规则允许的方向。
const GRANULARITIES: [(&str, u32); 12] = [
    ("1m", 1),
    ("3m", 3),
    ("5m", 5),
    ("15m", 15),
    ("30m", 30),
    ("1H", 60),
    ("4H", 240),
    ("6H", 360),
    ("12H", 720),
    ("1D", 1_440),
    ("1W", 10_080),
    ("1M", 43_200),
];

/// 把请求的分钟数映射到**不小于**它的合法档位。
///
/// 向上取整而不是向下：更细的序列会把基差的高频噪声当信号、半衰期算短，
/// 而持有期算短会让年化虚高 —— 一个不会报错、只会让人多下注的错误。
fn candle_granularity(minutes: u32) -> ArbResult<&'static str> {
    GRANULARITIES
        .iter()
        .find(|(_, value)| *value >= minutes)
        .map(|(name, _)| *name)
        .ok_or_else(|| {
            ArbError::config(format!(
                "K 线周期 {minutes} 分钟超过该端点支持的最大周期（{}）",
                GRANULARITIES[GRANULARITIES.len() - 1].1
            ))
        })
}

/// 每行是 `[ts(ms), o, h, l, c, v, turnover]`，**降序**。
/// 收盘价缺失/不可解析/非正的跳过 —— 填 0 会造出一个 −100% 的假跳变。
fn parse_candles(rows: &[Vec<String>]) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .iter()
        .filter_map(|row| {
            let open_ms: i64 = row.first()?.parse().ok()?;
            let close = parse_decimal(row.get(CANDLE_CLOSE_INDEX)?)?;
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

/// `/mix/market/merge-depth` 的 `data` 是**对象**（不是数组），所以不能复用
/// `unwrap_envelope` 的 `Vec<T>` 语义 —— 硬套只会得到一句「期望数组却拿到对象」的
/// 反序列化错误，真正的原因（`msg`）被丢掉。
#[derive(Debug, Deserialize)]
struct DepthEnvelope {
    code: String,
    msg: String,
    data: Option<Depth>,
}

impl DepthEnvelope {
    fn into_depth(self) -> ArbResult<Depth> {
        if self.code != SUCCESS_CODE {
            return Err(ArbError::venue(
                VENUE.as_str(),
                format!("深度 code={}：{}", self.code, self.msg),
            ));
        }
        self.data.ok_or_else(|| {
            ArbError::venue(VENUE.as_str(), format!("code={} 但 data 为空", self.code))
        })
    }
}

#[derive(Debug, Deserialize)]
struct Depth {
    #[serde(default)]
    bids: Vec<[Decimal; 2]>,
    #[serde(default)]
    asks: Vec<[Decimal; 2]>,
}

/// 把深度响应转成盘口。空盘或交叉盘一律报错 —— 那是数据坏了，不是「没有流动性」。
fn build_book(symbol: &Symbol, depth: &Depth, levels: u32) -> ArbResult<OrderBook> {
    let side = |rows: &[[Decimal; 2]]| -> Vec<Level> {
        let out: Vec<Level> = rows
            .iter()
            .filter(|[price, size]| *price > Decimal::ZERO && *size > Decimal::ZERO)
            .map(|[price, size]| Level {
                price: *price,
                notional_usdt: size * price,
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

#[async_trait]
impl VenueApi for BitgetApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let tickers: Vec<Ticker> = self.fetch_list(TICKERS_URL).await?;
        let rate_rows: Vec<CurrentFundRate> = self.fetch_list(CURRENT_FUND_RATE_URL).await?;

        // 配置端点失败时仍有专属 USDT 产品端点与符号过滤；费率成本保留未知，
        // 不用猜测的统一费率冒充场所给出的值。
        let contracts: Vec<Contract> = match self.fetch_list(CONTRACTS_URL).await {
            Ok(contracts) => contracts,
            Err(error) => {
                warn!(venue = %VENUE, %error, "合约信息端点失败，吃单费率按未知处理");
                Vec::new()
            }
        };

        let rate_rows: HashMap<&str, &CurrentFundRate> = rate_rows
            .iter()
            .map(|row| (row.symbol.as_str(), row))
            .collect();
        let contracts: HashMap<&str, &Contract> = contracts
            .iter()
            .map(|row| (row.symbol.as_str(), row))
            .collect();

        let mut out = Vec::with_capacity(tickers.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for ticker in &tickers {
            let Some(base) = linear_perp_base(&ticker.symbol) else {
                filtered += 1;
                continue;
            };
            let row = rate_rows.get(ticker.symbol.as_str()).copied();
            let config = contracts.get(ticker.symbol.as_str()).copied();
            if config.is_some_and(|contract| !contract.is_linear_perp()) {
                filtered += 1;
                continue;
            }
            match parse_row(base, ticker, row, config) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        // 返回顺序必须确定：哈希表的迭代顺序不定，下游按稳定顺序比对与落盘。
        out.sort_by(|left, right| left.symbol.base.cmp(&right.symbol.base));

        // 两种「少了一条」要分开报：符号过滤是预期内的（交割合约、非 USDT 保证金），
        // 字段不可用则说明数据源变了或合约刚下架，必须能一眼区分。
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非 USDT 永续合约已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        Ok(out)
    }
    /// 单个合约的盘口深度。
    ///
    /// `/mix/market/merge-depth` 的 `size` 是**基础币数量**（与 `tickers.bidSz` 同口径，
    /// 已在 `fetch_all` 里核实过），所以名义额是 `size × price`。
    /// 实测 `limit=3` 仍会返回 20 档 —— 端点有最小档数，所以返回后按 `levels` 截断。
    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        if levels == 0 {
            return Err(ArbError::config("levels 必须大于 0"));
        }
        let native = format!("{}{QUOTE}", symbol.base);
        let url = format!("{MERGE_DEPTH_URL}&symbol={native}");
        let envelope: DepthEnvelope = get_json(self.client.get(&url), VENUE).await?;
        build_book(symbol, &envelope.into_depth()?, levels)
    }

    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `granularity` **区分大小写**（`1H` 不是 `1h`），响应**降序**。
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
        let granularity = candle_granularity(interval_minutes)?;
        let url = format!(
            "{CANDLES_URL}&symbol={native}&granularity={granularity}&limit={}",
            limit.min(MAX_CANDLES)
        );
        let envelope: Envelope<Vec<String>> = get_json(self.client.get(&url), VENUE).await?;
        Ok(parse_candles(&unwrap_envelope(envelope)?))
    }
}

/// 把「一行行情 + 一行资金费 + 一行合约配置」拼成领域读数。`None` = 这一行不可用。
///
/// 抽成纯函数是为了让「结算周期来源」「吃单费率取值」「费率缺失整行丢弃」这三条
/// 最容易出错的规则能被单测覆盖，不必真的打网络。
///
/// 三处都**不猜**：
/// - 费率缺失/不可解析 → 整行丢弃。**绝不回落成 0**：0 是一个合法的费率读数，
///   伪造出来的 0 会凭空造出巨大价差。
/// - 结算时刻用场所给的 `nextUpdate`，不按周期自己推算；缺失或为 0 → 丢弃，
///   而不是拿当前时间顶上。
/// - 持仓量换算需要标记价；拿不到标记价就留 `None`，不拿指数价顶替。
fn parse_row(
    base: &str,
    ticker: &Ticker,
    rate: Option<&CurrentFundRate>,
    contract: Option<&Contract>,
) -> Option<MarketSnapshot> {
    // 费率来自批量端点。行情端点也有一份，但两份快照实测并不一致，只用这一份，
    // 保证费率与周期、结算时刻同源。
    let rate = rate?;
    let period_rate = rate.funding_rate.as_deref().and_then(parse_decimal)?;
    let next_funding_at = rate.next_update.as_deref().and_then(parse_millis)?;

    let (interval_h, interval_assumed) = match interval_hours(rate, contract) {
        Some(hours) => (hours, false),
        None => (DEFAULT_FUNDING_INTERVAL_H, true),
    };

    let mark_price = ticker.mark_price.as_deref().and_then(parse_decimal);
    let index_price = ticker.index_price.as_deref().and_then(parse_decimal);

    let (mut best_bid, mut bid_size_usdt) =
        book_side(ticker.bid_pr.as_deref(), ticker.bid_sz.as_deref());
    let (mut best_ask, mut ask_size_usdt) =
        book_side(ticker.ask_pr.as_deref(), ticker.ask_sz.as_deref());
    // 交叉盘不能当成可成交报价；但盘口损坏不该抹掉独立的资金费数据。
    if best_bid.zip(best_ask).is_some_and(|(bid, ask)| ask < bid) {
        best_bid = None;
        best_ask = None;
        bid_size_usdt = None;
        ask_size_usdt = None;
    }

    // `holdingAmount` 是基础币口径，乘标记价才是 USDT 口径的持仓量。
    // 用 `checked_mul` 是因为一条离谱的持仓量在乘法里溢出会 panic，
    // 而 panic 会带走整轮扫描 —— 一个坏字段不该有这种代价。
    let open_interest_usdt = ticker
        .holding_amount
        .as_deref()
        .and_then(parse_decimal)
        .zip(mark_price)
        .and_then(|(size, price)| size.checked_mul(price));

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        // `nextUpdate` 是场所给的结算时刻，不是按周期推算的。
        next_funding_estimated: false,
        // 公开基础费率不等于账户 VIP 费率；缺失或解析失败时保留未知。
        taker_fee: contract
            .and_then(|config| config.taker_fee_rate.as_deref())
            .and_then(parse_decimal),
        mark_price,
        index_price,
        best_bid,
        best_ask,
        bid_size_usdt,
        ask_size_usdt,
        open_interest_usdt,
        quote_volume_24h: ticker.quote_volume.as_deref().and_then(parse_decimal),
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

/// 基础币量乘该侧价格才是 USDT 名义；没有有效价格就不能换算量。
/// 保留场所明确给出的零量，负量或溢出则是坏数据，不能让一条异常带走整轮扫描。
fn book_side(price: Option<&str>, size: Option<&str>) -> (Option<Decimal>, Option<Decimal>) {
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

/// 结算周期（小时）。两个来源都是场所给的，按可靠性排序：
/// `current-fund-rate` 的 `fundingRateInterval` 与费率、结算时刻同行返回，是同一个快照；
/// `contracts` 的 `fundInterval` 是合约配置，作兜底。实测两者在全部 797 个合约上一致。
///
/// `0` 视为无效：0 小时的结算周期没有意义，下游日化还会拿它做除数。
fn interval_hours(rate: &CurrentFundRate, contract: Option<&Contract>) -> Option<u32> {
    rate.funding_rate_interval
        .as_deref()
        .and_then(parse_interval)
        .or_else(|| {
            contract
                .and_then(|config| config.fund_interval.as_deref())
                .and_then(parse_interval)
        })
}

/// 毫秒时间戳字符串 → UTC 时刻。`0` 与不可解析都返回 `None`。
///
/// `0` 表示这家场所此刻**没有**下一次结算（合约已下架或暂停）。直接交给
/// `from_timestamp_millis` 会得到一个 1970 年的合法时间戳，于是一条「1970 年结算」
/// 的假数据会一路进到面板。
fn parse_millis(raw: &str) -> Option<DateTime<Utc>> {
    raw.trim()
        .parse::<i64>()
        .ok()
        .filter(|millis| *millis > 0)
        .and_then(DateTime::from_timestamp_millis)
}

/// 解析「小时」字段（`fundInterval` / `fundingRateInterval` 都是字符串）。
fn parse_interval(raw: &str) -> Option<u32> {
    raw.trim().parse::<u32>().ok().filter(|hours| *hours > 0)
}

/// 是不是 USDT 保证金的线性永续；是则返回基础币。
///
/// `usdt-futures` 实测只有永续（`symbolType` 全为 `perpetual`、`quoteCoin` 全为
/// `USDT`），但过滤必须留着：交割合约的符号带 `_`（如 `BTCUSDT_250926`），
/// 混进来会与永续配对，而两者的资金费机制不同。
fn linear_perp_base(symbol: &str) -> Option<&str> {
    let base = symbol.strip_suffix(QUOTE)?;
    (!base.is_empty() && !symbol.contains('_')).then_some(base)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 以下 fixture 都是 2026-09-19 打真实端点取到的响应片段
    // （`/api/v2/mix/market/tickers`、`/current-fund-rate`、`/contracts`，均带
    // `productType=usdt-futures`）。刻意保留原样的字符串类型，好让「字段是字符串」
    // 这件事被测试钉住。

    /// 1h 合约：行情。
    const TICKER_ONE: &str = r#"{
        "symbol": "ONEUSDT", "lastPr": "0.0024707", "high24h": "0.0028469",
        "low24h": "0.0014909", "ts": "1789823860561", "change24h": "0.39896",
        "baseVolume": "18679806823", "quoteVolume": "38627068.3223499",
        "usdtVolume": "38627068.3223499", "indexPrice": "0.00249485",
        "fundingRate": "-0.001295", "holdingAmount": "633958103",
        "deliveryStartTime": null, "deliveryTime": null, "deliveryStatus": "",
        "markPrice": "0.0024707"
    }"#;

    /// 8h 合约：行情。
    const TICKER_BTC: &str = r#"{
        "symbol": "BTCUSDT", "lastPr": "81270.9", "ts": "1789823860561",
        "baseVolume": "35697.6242", "quoteVolume": "2876530817.42665",
        "usdtVolume": "2876530817.42665", "indexPrice": "81299.6815",
        "fundingRate": "0.0001", "holdingAmount": "34070.435999999889",
        "deliveryStartTime": null, "deliveryTime": null, "deliveryStatus": "",
        "markPrice": "81270.9"
    }"#;

    /// 2026-09-20 批量行情真实片段；保留不同的买卖量以防方向或换算价格接反。
    const TICKER_BTC_BOOK: &str = r#"{
        "symbol": "BTCUSDT", "lastPr": "80344.5", "ts": "1789879674483",
        "bidPr": "80344.7", "bidSz": "4.364",
        "askPr": "80344.8", "askSz": "0.3837",
        "markPrice": "80346.8", "indexPrice": "80375.282"
    }"#;

    /// 1h 合约：资金费行。
    const RATE_ONE: &str = r#"{
        "symbol": "ONEUSDT", "fundingRate": "-0.001265", "fundingRateInterval": "1",
        "nextUpdate": "1789826400000", "minFundingRate": "-0.018", "maxFundingRate": "0.018"
    }"#;

    /// 8h 合约：资金费行。
    const RATE_BTC: &str = r#"{
        "symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateInterval": "8",
        "nextUpdate": "1789833600000", "minFundingRate": "-0.003", "maxFundingRate": "0.003"
    }"#;

    /// 4h 合约：资金费行。
    const RATE_XTZ: &str = r#"{
        "symbol": "XTZUSDT", "fundingRate": "-0.000609", "fundingRateInterval": "4",
        "nextUpdate": "1789833600000", "minFundingRate": "-0.018", "maxFundingRate": "0.018"
    }"#;

    /// 1h 合约：合约配置。
    const CONTRACT_ONE: &str = r#"{
        "symbol": "ONEUSDT", "baseCoin": "ONE", "quoteCoin": "USDT",
        "makerFeeRate": "0.0002", "takerFeeRate": "0.0006", "symbolType": "perpetual",
        "symbolStatus": "normal", "fundInterval": "1", "supportMarginCoins": ["USDT"],
        "deliveryTime": "", "minTradeUSDT": "5"
    }"#;

    /// 8h 合约：合约配置。
    const CONTRACT_BTC: &str = r#"{
        "symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT",
        "makerFeeRate": "0.0002", "takerFeeRate": "0.0006", "symbolType": "perpetual",
        "symbolStatus": "normal", "fundInterval": "8", "supportMarginCoins": ["USDT"],
        "deliveryTime": "", "minTradeUSDT": "5"
    }"#;

    /// 真实错误响应：HTTP 200，但 code 不是成功码、data 为 null。
    const ERROR_BODY: &str = r#"{
        "code": "400172", "msg": "Parameter verification failed",
        "requestTime": 1789823952362, "data": null
    }"#;

    fn ticker(raw: &str) -> Ticker {
        serde_json::from_str(raw).expect("行情 fixture 必须能反序列化")
    }

    fn rate(raw: &str) -> CurrentFundRate {
        serde_json::from_str(raw).expect("资金费 fixture 必须能反序列化")
    }

    fn contract(raw: &str) -> Contract {
        serde_json::from_str(raw).expect("合约 fixture 必须能反序列化")
    }

    #[test]
    fn only_usdt_margined_perps_are_kept() {
        assert_eq!(linear_perp_base("BTCUSDT"), Some("BTC"));
        assert_eq!(linear_perp_base("BTCUSDT_250926"), None, "交割合约");
        assert_eq!(linear_perp_base("BTCUSDC"), None, "USDC 保证金");
        assert_eq!(linear_perp_base("USDT"), None, "没有 base");
    }

    #[test]
    fn metadata_rejects_delivery_inverse_and_non_usdt_contracts() {
        let mut config = contract(CONTRACT_BTC);
        assert!(config.is_linear_perp());
        config.symbol_type = Some("delivery".into());
        assert!(!config.is_linear_perp());
        config = contract(CONTRACT_BTC);
        config.support_margin_coins = Some(vec!["BTC".into()]);
        assert!(!config.is_linear_perp(), "反向合约不能混入");
        config = contract(CONTRACT_BTC);
        config.quote_coin = Some("USDC".into());
        assert!(!config.is_linear_perp());
    }

    #[test]
    fn a_success_body_is_unwrapped_and_an_error_code_is_not_silently_empty() {
        let ok: Envelope<Ticker> = serde_json::from_str(&format!(
            r#"{{"code":"00000","msg":"success","data":[{TICKER_ONE}]}}"#
        ))
        .unwrap();
        assert_eq!(unwrap_envelope(ok).unwrap().len(), 1);

        // 这里如果只看 HTTP 状态就会把「请求被拒」当成「一家合约都没有」，
        // 于是整个场所静默消失。
        let failed: Envelope<Ticker> = serde_json::from_str(ERROR_BODY).unwrap();
        let error = unwrap_envelope(failed).unwrap_err();
        assert!(error.to_string().contains("400172"), "{error}");
    }

    #[test]
    fn bitget_is_not_a_single_interval_venue() {
        // 同一时刻、同一个端点里 1h / 4h / 8h 并存。任何「Bitget 是 N 小时场所」
        // 的常量都会让其中一部分合约的日化错 2~8 倍。
        let one = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate(RATE_ONE)),
            Some(&contract(CONTRACT_ONE)),
        )
        .unwrap();
        assert_eq!(interval_hours(&rate(RATE_XTZ), None), Some(4));
        let btc = parse_row(
            "BTC",
            &ticker(TICKER_BTC),
            Some(&rate(RATE_BTC)),
            Some(&contract(CONTRACT_BTC)),
        )
        .unwrap();

        assert_eq!((one.interval_h, btc.interval_h), (1, 8));
        for row in [&one, &btc] {
            assert!(!row.interval_assumed, "周期是场所给的");
            assert!(!row.next_funding_estimated, "结算时刻也是场所给的");
        }
    }

    #[test]
    fn the_contract_interval_is_the_fallback_when_the_rate_row_omits_it() {
        let mut rate_row = rate(RATE_ONE);
        rate_row.funding_rate_interval = None;

        let with_config = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate_row),
            Some(&contract(CONTRACT_ONE)),
        )
        .unwrap();
        assert_eq!(with_config.interval_h, 1);
        assert!(!with_config.interval_assumed);

        // 两个来源都没有才回落，并且必须标出来。
        let without_config = parse_row("ONE", &ticker(TICKER_ONE), Some(&rate_row), None).unwrap();
        assert_eq!(without_config.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(without_config.interval_assumed, "回落值必须标记出来");
    }

    #[test]
    fn a_zero_interval_is_treated_as_missing_not_as_zero_hours() {
        let mut rate_row = rate(RATE_ONE);
        rate_row.funding_rate_interval = Some("0".into());
        let row = parse_row("ONE", &ticker(TICKER_ONE), Some(&rate_row), None).unwrap();
        assert_eq!(row.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(row.interval_assumed);
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        for broken in [None, Some(String::new()), Some("n/a".into())] {
            let mut rate_row = rate(RATE_ONE);
            rate_row.funding_rate = broken;
            assert!(
                parse_row("ONE", &ticker(TICKER_ONE), Some(&rate_row), None).is_none(),
                "费率不可用必须整行丢弃"
            );
        }
        // 行情端点有费率、资金费端点没有这一行，也不能凑一条出来。
        assert!(parse_row("ONE", &ticker(TICKER_ONE), None, None).is_none());
    }

    #[test]
    fn a_missing_or_zero_settlement_time_drops_the_row() {
        for broken in [None, Some("0".into()), Some("soon".into())] {
            let mut rate_row = rate(RATE_ONE);
            rate_row.next_update = broken;
            assert!(
                parse_row("ONE", &ticker(TICKER_ONE), Some(&rate_row), None).is_none(),
                "结算时刻不可用必须整行丢弃，不能拿当前时间顶上"
            );
        }
    }

    #[test]
    fn a_reported_zero_taker_fee_is_kept_and_an_absent_contract_is_unknown() {
        // Bitget 实测全部是 0.0006，这里刻意构造 0：`Some(0)`（已知为 0）与
        // `None`（不知道）是完全相反的含义，混掉会把亏钱的机会排上来。
        let mut zero = contract(CONTRACT_ONE);
        zero.taker_fee_rate = Some("0".into());
        let known_zero = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate(RATE_ONE)),
            Some(&zero),
        )
        .unwrap();
        assert_eq!(known_zero.taker_fee, Some(arb_core::Decimal::ZERO));

        zero.taker_fee_rate = None;
        let missing_fee = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate(RATE_ONE)),
            Some(&zero),
        )
        .unwrap();
        assert_eq!(missing_fee.taker_fee, None);

        let reported = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate(RATE_ONE)),
            Some(&contract(CONTRACT_ONE)),
        )
        .unwrap();
        assert_eq!(
            reported.taker_fee,
            parse_decimal("0.0006"),
            "公开接口给的吃单费率要原样带出来"
        );

        // 合约配置整个拿不到 → 未知，绝不填一个猜测值。
        let unknown = parse_row("ONE", &ticker(TICKER_ONE), Some(&rate(RATE_ONE)), None).unwrap();
        assert_eq!(unknown.taker_fee, None);
    }

    #[test]
    fn prices_sizes_and_volume_come_from_the_ticker_row() {
        let row = parse_row(
            "ONE",
            &ticker(TICKER_ONE),
            Some(&rate(RATE_ONE)),
            Some(&contract(CONTRACT_ONE)),
        )
        .unwrap();

        assert_eq!(row.symbol.to_string(), "ONE/USDT");
        assert_eq!(row.period_rate, parse_decimal("-0.001265").unwrap());
        assert_eq!(row.mark_price, parse_decimal("0.0024707"));
        assert_eq!(row.index_price, parse_decimal("0.00249485"));
        assert_eq!(row.quote_volume_24h, parse_decimal("38627068.3223499"));
        // 持仓量是基础币口径，必须换算成 USDT 口径。
        assert_eq!(row.open_interest_usdt, parse_decimal("1566320.2850821"));

        // 结算时刻取场所给的毫秒时间戳。
        assert_eq!(
            row.next_funding_at.timestamp_millis(),
            1_789_826_400_000,
            "1h 合约的 nextUpdate 落在整点"
        );
    }

    #[test]
    fn an_unusable_price_leaves_the_optional_fields_empty() {
        let mut ticker_row = ticker(TICKER_ONE);
        ticker_row.mark_price = None;
        ticker_row.index_price = Some(String::new());

        let row = parse_row("ONE", &ticker_row, Some(&rate(RATE_ONE)), None).unwrap();
        assert_eq!(row.mark_price, None);
        assert_eq!(row.index_price, None);
        assert_eq!(row.open_interest_usdt, None, "没有标记价就不换算持仓量");
        assert_eq!(row.period_rate, parse_decimal("-0.001265").unwrap());
    }

    #[test]
    fn book_fixture_converts_base_sizes_at_their_own_side_prices() {
        let row = parse_row("BTC", &ticker(TICKER_BTC_BOOK), Some(&rate(RATE_BTC)), None).unwrap();
        assert_eq!(row.best_bid, parse_decimal("80344.7"));
        assert_eq!(row.best_ask, parse_decimal("80344.8"));
        assert_eq!(row.bid_size_usdt, parse_decimal("350624.2708"));
        assert_eq!(row.ask_size_usdt, parse_decimal("30828.29976"));
    }

    #[test]
    fn missing_book_fields_never_fall_back_to_mark_price() {
        let row = parse_row("BTC", &ticker(TICKER_BTC), Some(&rate(RATE_BTC)), None).unwrap();
        assert!(row.mark_price.is_some());
        assert_eq!(
            (
                row.best_bid,
                row.best_ask,
                row.bid_size_usdt,
                row.ask_size_usdt
            ),
            (None, None, None, None)
        );

        let mut book = ticker(TICKER_BTC_BOOK);
        book.bid_sz = None;
        book.ask_pr = None;
        let row = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
        assert_eq!(row.best_bid, parse_decimal("80344.7"));
        assert_eq!(row.bid_size_usdt, None);
        assert_eq!(row.best_ask, None);
        assert_eq!(row.ask_size_usdt, None, "没有价格不能将基础币量当成 USDT");
    }

    #[test]
    fn a_crossed_book_clears_all_book_fields_without_losing_funding() {
        let mut book = ticker(TICKER_BTC_BOOK);
        book.ask_pr = Some("80344.6".into());
        let row = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
        assert_eq!(
            (
                row.best_bid,
                row.best_ask,
                row.bid_size_usdt,
                row.ask_size_usdt
            ),
            (None, None, None, None)
        );
        assert_eq!(row.period_rate, parse_decimal("0.0001").unwrap());
        assert_eq!(row.interval_h, 8);

        // 锁盘是合法的零价差，不应被交叉盘校验误伤。
        book.ask_pr = book.bid_pr.clone();
        let locked = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
        assert_eq!(locked.best_bid, parse_decimal("80344.7"));
        assert_eq!(locked.best_ask, locked.best_bid);
        assert_eq!(locked.ask_size_usdt, parse_decimal("30828.26139"));
    }

    #[test]
    fn invalid_book_values_are_unknown_but_reported_zero_size_is_kept() {
        for bad_price in ["", "n/a", "0", "-1"] {
            let mut book = ticker(TICKER_BTC_BOOK);
            book.bid_pr = Some(bad_price.into());
            let row = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
            assert_eq!(row.best_bid, None);
            assert_eq!(row.bid_size_usdt, None);
            assert_eq!(row.best_ask, parse_decimal("80344.8"));
            assert_eq!(row.ask_size_usdt, parse_decimal("30828.29976"));
        }
        for bad_size in ["", "n/a", "-1", "79228162514264337593543950335"] {
            let mut book = ticker(TICKER_BTC_BOOK);
            book.ask_sz = Some(bad_size.into());
            let row = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
            assert_eq!(row.best_ask, parse_decimal("80344.8"));
            assert_eq!(row.ask_size_usdt, None);
            assert_eq!(row.bid_size_usdt, parse_decimal("350624.2708"));
        }
        let mut book = ticker(TICKER_BTC_BOOK);
        book.bid_sz = Some("0".into());
        let row = parse_row("BTC", &book, Some(&rate(RATE_BTC)), None).unwrap();
        assert_eq!(row.bid_size_usdt, Some(Decimal::ZERO));
    }

    #[test]
    fn depth_sorts_before_truncating_so_worse_levels_cannot_crowd_out_the_touch() {
        let px = |raw: &str| parse_decimal(raw).unwrap();
        let depth = Depth {
            // 更优买价放在原始数组末尾；先截断再排序会只留下 100。
            bids: vec![
                [px("100"), px("1")],
                [px("103"), px("2")],
                [px("102"), px("1")],
            ],
            asks: vec![
                [px("110"), px("1")],
                [px("104"), px("2")],
                [px("105"), px("1")],
            ],
        };
        let book = build_book(&Symbol::perp("BTC", QUOTE), &depth, 1).unwrap();
        assert_eq!(book.bids.len(), 1);
        assert_eq!(book.asks.len(), 1);
        assert_eq!(book.best_bid(), Some(px("103")));
        assert_eq!(book.best_ask(), Some(px("104")));
        assert_eq!(book.bids[0].notional_usdt, px("206"));
    }
}
