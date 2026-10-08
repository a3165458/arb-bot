//! OKX 永续（USDT 保证金）。
//!
//! 整轮只打 6 次请求，全部是文档化的公共批量端点：
//!
//! | 端点 | 提供 |
//! | --- | --- |
//! | `/api/v5/public/funding-rate?instId=ANY` | 每期费率、结算时刻（→ 结算周期） |
//! | `/api/v5/public/instruments?instType=SWAP` | 线性/USDT/在架过滤、指数名、合约面值 |
//! | `/api/v5/public/mark-price?instType=SWAP` | 标记价 |
//! | `/api/v5/market/index-tickers?quoteCcy=USDT` | 指数价 |
//! | `/api/v5/public/open-interest?instType=SWAP` | 持仓量（`oiUsd`） |
//! | `/api/v5/market/tickers?instType=SWAP` | 24h 成交额、一档买卖价与张数 |
//!
//! 上表是**整轮扫描**的 6 次请求。逐合约深度（`fetch_depth`）不在其中：它按合约
//! 调用，每次 2 个请求 —— `/api/v5/market/books` 拿盘口、`/api/v5/public/instruments`
//! 只查这一个 `instId` 拿面值（打全量 467 行只为一行是浪费），只在深度体检时对少数
//! 候选打。
//!
//! 逐合约 K 线（`fetch_candles`）同理：`/api/v5/market/candles` 一次只吃一个
//! `instId`，只在实测基差半衰期时对少数候选打。三处必须留意的场所特性：
//!
//! - **返回是降序**（最新一根在最前），所以取到的序列必须显式排成升序。顺序反了
//!   半衰期会算成一个正数（看起来很正常）而实际是负相关 —— 这种错不会报错。
//! - **`bar` 大小写敏感**，且只有一组枚举值。不支持的周期**一律向上取整**
//!   （请求 45 分钟拿 `1H`），绝不向下：更细的序列会把半衰期算成更短的时间。
//! - **`1D`/`1W`/`1M` 的边界是 UTC+8 的 00:00**（实测 `1D` 那根的 `ts` 是 16:00Z），
//!   所以「一根日线」不等于「UTC 的一天」。时间戳仍按场所给的原值解析成 UTC 时刻，
//!   不做任何对齐换算。
//!
//! **为什么必须是 `instId=ANY`**：`/api/v5/public/funding-rate` 要求 `instId`，
//! 逗号列表会被 `51000 Parameter instId error` 拒绝，文档里唯一的多合约用法就是
//! `instId=ANY`（"ANY to return the funding rate info of all perpetual and X-Perps
//! futures contracts"），实测一次返回 674 行（482 SWAP + 192 X-Perps FUTURES）。
//! 逐合约拉就是 467 次请求，而该端点限速 10 req/2s（rule: IP + Instrument ID），
//! 单轮要几十秒 —— 批量端点把这一家从「不可用」变成 1 次请求。
//!
//! **结算周期不是固定 8h**：实测 482 个 SWAP 里 273 个 8h、208 个 4h、1 个 1h。
//! `/api/v5/public/instruments` 里**没有** `fundingInterval` 字段（482 行全缺），
//! 周期只能从场所给的两个结算时刻推：`fundingTime - prevFundingTime`。
//!
//! **一档量不能直接当 USDT**：`bidPx`/`askPx` 是我们卖出/买入能成交的价，
//! `bidSz`/`askSz` 对 SWAP 是张数。官方 instruments 文档定义线性合约的
//! 名义为 `张数 × ctVal × 价格`；实测 BTC 的面值是 `0.01 BTC`，DOGE 是
//! `1000 DOGE`，漏乘面值会分别多算 100 倍、少算 1000 倍。
//! 只在 `ctValCcy` 与标的币一致时换算；面值未知只留空量，盘口缺失不拿
//! 标记价补齐，交叉盘四项都留空，避免把倒挂报价算成负穿价成本。
//!
//! 吃单费率公共行情拿不到（`/api/v5/account/trade-fee` 需要签名），`taker_fee`
//! 一律 `None` —— 排名会回落到配置的单边费率。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Candle, DEFAULT_FUNDING_INTERVAL_H, Decimal, Level, MarketSnapshot,
    OrderBook, Symbol, Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::DateTime;
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Okx;

/// `instId=ANY` 是 OKX 唯一的多合约资金费用法（见模块注释）。
const FUNDING_RATE_URL: &str = "https://www.okx.com/api/v5/public/funding-rate?instId=ANY";
const INSTRUMENTS_URL: &str = "https://www.okx.com/api/v5/public/instruments?instType=SWAP";
const MARK_PRICE_URL: &str = "https://www.okx.com/api/v5/public/mark-price?instType=SWAP";
const INDEX_TICKERS_URL: &str = "https://www.okx.com/api/v5/market/index-tickers?quoteCcy=USDT";
const OPEN_INTEREST_URL: &str = "https://www.okx.com/api/v5/public/open-interest?instType=SWAP";
const TICKERS_URL: &str = "https://www.okx.com/api/v5/market/tickers?instType=SWAP";
/// 逐合约盘口深度。**不在整轮扫描里**，只在深度体检时对少数候选调用。
const BOOKS_URL: &str = "https://www.okx.com/api/v5/market/books";

/// `sz` 的合法上界。实测 `sz=401` 与 `sz=0` 都是 `51000 Parameter sz error.`。
const MAX_DEPTH_LEVELS: u32 = 400;

/// 逐合约 K 线。**不在整轮扫描里**，只在实测基差半衰期时对少数候选调用。
const CANDLES_URL: &str = "https://www.okx.com/api/v5/market/candles";

/// `limit` 的合法上界。实测 `limit=301`/`400`/`1000` 都**静默截成 300 行**
/// （HTTP 200 + `code=0`），而 `limit=0` 是 `51000 Parameter limit error`。
/// 静默少拿比显式截断更坏：调用方会以为回归用的是 500 个点。
const MAX_CANDLE_LIMIT: u32 = 300;

/// 一根 K 线是 9 元素字符串数组：
/// `[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]`。
/// 收盘价下标是 4（实测 `data[0][4]=80320` 与同时刻 `markPx=80328.2` 同量级，
/// 确认取的是价格而不是成交量）。
const CANDLE_CLOSE_INDEX: usize = 4;
/// `confirm` 下标。`"1"` = 已收盘，`"0"` = 还在走的当前周期。
const CANDLE_CONFIRM_INDEX: usize = 8;

/// `bar` 的合法取值（**大小写敏感**：实测 `1h`/`4h`/`1d`/`1w` 全是
/// `51000 Parameter bar error`）与它覆盖的分钟数。
///
/// 端点还接受 `1s`，但调用方的单位是分钟，而 1 秒永远比任何分钟级请求更细，
/// 选了就是「返回比请求更细的序列」，所以不列进表里（请求 0 分钟取 `1m`）。
///
/// `1M`/`3M` 是**日历周期**，长度不固定（实测 `1M` 那根跨 31 天，2 月的 `1M`
/// 只有 28 天）。这里填**可能出现的最短长度**（`1M`=28 天、`3M`=89 天），
/// 这样「取不小于请求周期的值」对每个自然月都成立；填名义长度（30/90 天）会让
/// 2 月的请求悄悄拿到比请求更细的序列。
const BARS: [(u32, &str); 16] = [
    (1, "1m"),
    (3, "3m"),
    (5, "5m"),
    (15, "15m"),
    (30, "30m"),
    (60, "1H"),
    (120, "2H"),
    (240, "4H"),
    (360, "6H"),
    (720, "12H"),
    (1_440, "1D"),
    (2_880, "2D"),
    (4_320, "3D"),
    (10_080, "1W"),
    (40_320, "1M"),
    (128_160, "3M"),
];

/// 把请求的分钟数映射到**不小于**它的合法周期。
///
/// 向上取整而不是向下：更细的序列会把基差的高频噪声当信号、半衰期算短，
/// 而持有期算短会让年化虚高 —— 一个不会报错、只会让人多下注的错误。
fn bar_for(minutes: u32) -> ArbResult<&'static str> {
    BARS.iter()
        .find(|(value, _)| *value >= minutes)
        .map(|(_, name)| *name)
        .ok_or_else(|| {
            ArbError::config(format!(
                "K 线周期 {minutes} 分钟超过该端点支持的最大周期（{}）",
                BARS[BARS.len() - 1].0
            ))
        })
}

/// 每行是 `[ts(ms), o, h, l, c, …]` 的字符串数组，**降序**。
/// 收盘价缺失/不可解析/非正的跳过 —— 填 0 会造出一个 −100% 的假跳变。
fn parse_candles(rows: &[Vec<String>]) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .iter()
        .filter_map(|row| {
            if row.get(CANDLE_CONFIRM_INDEX).map(String::as_str) == Some("0") {
                return None;
            }
            let open_ms: i64 = row.first()?.parse().ok()?;
            let close = positive_decimal(row.get(CANDLE_CLOSE_INDEX)?)?;
            Some(Candle {
                open_time: DateTime::from_timestamp_millis(open_ms)?,
                close,
            })
        })
        .collect();
    out.sort_by_key(|candle| candle.open_time);
    out
}

/// 计价资产。OKX 的线性合约只有 USDT 保证金。
const QUOTE: &str = "USDT";

/// 线性 USDT 永续的合约名后缀，如 `BTC-USDT-SWAP`。
const SWAP_SUFFIX: &str = "-USDT-SWAP";

/// 一小时的毫秒数：结算周期由两个毫秒时间戳相减得到。
const MS_PER_HOUR: i64 = 3_600_000;

pub struct OkxApi {
    client: Client,
}

impl OkxApi {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// 打一个端点并剥掉外壳。`what` 只进错误信息，用来分辨是哪一步失败。
    async fn fetch<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        what: &str,
    ) -> ArbResult<Vec<T>> {
        let envelope: Envelope<T> = get_json(request, VENUE).await?;
        unwrap_data(envelope, what)
    }

    /// 可选端点：失败只降级成「这几个字段留空」，不能让整家场所消失。
    ///
    /// 缺标记价只是少一个参考价，缺资金费才是致命的 —— 所以资金费走
    /// [`Self::fetch`] 直接失败，这里只用于辅助读数。
    async fn fetch_optional<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        what: &str,
    ) -> Vec<T> {
        match self.fetch(request, what).await {
            Ok(rows) => rows,
            Err(error) => {
                warn!(venue = %VENUE, %error, what, "端点失败，相关字段留空");
                Vec::new()
            }
        }
    }
}

/// OKX 的响应外壳。**HTTP 200 + `code != "0"` 也是失败**。
///
/// 实测 `{"code":"50014","data":[],"msg":"Parameter instId can not be empty."}`
/// 与 `{"code":"51000",...}` 都是 200。只看状态码会把「参数写错、被限频」
/// 当成「这家场所今天没有合约」，静默少一家。
#[derive(Debug, Deserialize)]
// serde 的 derive 见到字段上的 `#[serde(default)]` 就会给泛型参数加上 `T: Default`
// 约束，而 `Vec<T>: Default` 并不需要 `T: Default`。显式写死反序列化约束，
// 否则这个外壳只能装 `Default` 类型。
#[serde(bound(deserialize = "T: serde::de::Deserialize<'de>"))]
struct Envelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    /// 出错时是 `[]` 或 `null`。`#[serde(default)]` 只覆盖「字段缺失」，
    /// **不覆盖显式 `null`** —— 用 `Option` 把两者都收下，成败仍由 `code` 判定。
    #[serde(default)]
    data: Option<Vec<T>>,
}

/// `code != "0"` → 错误，不是空结果。
fn unwrap_data<T>(envelope: Envelope<T>, what: &str) -> ArbResult<Vec<T>> {
    if envelope.code != "0" {
        return Err(ArbError::venue(
            VENUE.as_str(),
            format!("{what} code={} msg={}", envelope.code, envelope.msg),
        ));
    }
    Ok(envelope.data.unwrap_or_default())
}

/// `/api/v5/public/funding-rate?instId=ANY` 的一行。
///
/// OKX 把数字统一序列化成字符串，时间戳也是字符串，所以这里全按 `String` 收，
/// 解析失败就整行丢弃（见 [`parse_row`]）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateRow {
    inst_id: String,
    /// 同一条 feed 里混着 `SWAP` 与 X-Perps `FUTURES`，必须靠它分流。
    inst_type: String,
    /// 文档：预测的**下一期**费率（"Predicted funding rate for the upcoming
    /// settlement period"）。`settFundingRate` 是上一期已结算的值，已经过期。
    funding_rate: String,
    /// 本周期结算时刻（毫秒）。实测 482 行**全部在未来**，即下一次结算时刻。
    funding_time: String,
    /// 上一周期的结算时刻（毫秒）。`funding_time - prev_funding_time` 就是本周期长度。
    prev_funding_time: String,
}

/// `/api/v5/public/instruments?instType=SWAP` 的一行（只留用到的字段）。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Instrument {
    inst_id: String,
    ct_type: String,
    settle_ccy: String,
    state: String,
    /// 指数名（如 `BTC-USDT`），用来在 `index-tickers` 里找指数价 ——
    /// 比把 `-SWAP` 去掉猜更可靠。
    inst_family: String,
    /// 面值缺失不能让资金费整轮失败，但不能猜成「一张等于一个币」。
    #[serde(default)]
    ct_val: Option<String>,
    #[serde(default)]
    ct_val_ccy: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkPrice {
    inst_id: String,
    mark_px: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexTicker {
    inst_id: String,
    idx_px: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpenInterest {
    inst_id: String,
    /// 场所直接给的美元口径持仓量（`oi`/`oiCcy` 还得自己乘合约面值）。
    oi_usd: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker {
    inst_id: String,
    last: String,
    /// SWAP 的这个字段是**基础币**数量，不是计价币成交额：
    /// 实测 `vol24h × ctVal == volCcy24h` 逐位相等（BTC-USDT-SWAP：9617690.34 × 0.01）。
    vol_ccy_24h: String,
    /// 无盘口时只留空盘口，不能连累同一行的成交额。
    #[serde(default)]
    bid_px: Option<String>,
    #[serde(default)]
    ask_px: Option<String>,
    #[serde(default)]
    bid_sz: Option<String>,
    #[serde(default)]
    ask_sz: Option<String>,
}

/// 第二项是张数；后两项与名义额无关，但保留字符串数组以沿用场所的数字解析规则。
#[derive(Debug, Deserialize)]
struct DepthRow {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

/// 一行资金费之外的可选读数。全部来自批量端点，拿不到就是 `None`。
#[derive(Debug, Default)]
struct Extras<'a> {
    mark_px: Option<&'a str>,
    idx_px: Option<&'a str>,
    oi_usd: Option<&'a str>,
    last_px: Option<&'a str>,
    vol_ccy_24h: Option<&'a str>,
    ticker: Option<&'a Ticker>,
    instrument: Option<&'a Instrument>,
}

#[async_trait]
impl VenueApi for OkxApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        // 主端点：一次拿回全部永续 + X-Perps 的资金费。它失败就是这家场所失败。
        let rows = self
            .fetch::<FundingRateRow>(self.client.get(FUNDING_RATE_URL), "资金费")
            .await?;
        // 合约表是「只保留线性 USDT 永续」的唯一权威来源：资金费响应里只有
        // instId / instType，没有 ctType 与 settleCcy。它失败同样让这家场所失败 ——
        // 少了它就无法保证交割/反向/USDC 合约被挡住。
        let instruments = self
            .fetch::<Instrument>(self.client.get(INSTRUMENTS_URL), "合约列表")
            .await?;

        let eligible: HashMap<&str, &Instrument> = instruments
            .iter()
            .filter(|inst| is_linear_usdt_perp(inst))
            .map(|inst| (inst.inst_id.as_str(), inst))
            .collect();

        // 四个辅助批量端点：失败只让对应字段留空，费率本身照常返回。
        let mark_px: HashMap<String, String> = self
            .fetch_optional::<MarkPrice>(self.client.get(MARK_PRICE_URL), "标记价")
            .await
            .into_iter()
            .map(|row| (row.inst_id, row.mark_px))
            .collect();
        let idx_px: HashMap<String, String> = self
            .fetch_optional::<IndexTicker>(self.client.get(INDEX_TICKERS_URL), "指数价")
            .await
            .into_iter()
            .map(|row| (row.inst_id, row.idx_px))
            .collect();
        let oi_usd: HashMap<String, String> = self
            .fetch_optional::<OpenInterest>(self.client.get(OPEN_INTEREST_URL), "持仓量")
            .await
            .into_iter()
            .map(|row| (row.inst_id, row.oi_usd))
            .collect();
        let tickers: HashMap<String, Ticker> = self
            .fetch_optional::<Ticker>(self.client.get(TICKERS_URL), "24h 成交额与盘口")
            .await
            .into_iter()
            .map(|row| (row.inst_id.clone(), row))
            .collect();

        let mut out = Vec::with_capacity(rows.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for row in &rows {
            // 两层过滤都要：instType 挡掉同一条 feed 里的 X-Perps FUTURES，
            // 合约表挡掉反向合约、USDC 保证金与已下架合约。
            if row.inst_type != "SWAP" {
                filtered += 1;
                continue;
            }
            let Some(inst) = eligible.get(row.inst_id.as_str()) else {
                filtered += 1;
                continue;
            };

            let ticker = tickers.get(&row.inst_id);
            let extras = Extras {
                mark_px: mark_px.get(&row.inst_id).map(String::as_str),
                idx_px: idx_px.get(&inst.inst_family).map(String::as_str),
                oi_usd: oi_usd.get(&row.inst_id).map(String::as_str),
                last_px: ticker.map(|t| t.last.as_str()),
                vol_ccy_24h: ticker.map(|t| t.vol_ccy_24h.as_str()),
                ticker,
                instrument: Some(inst),
            };
            match parse_row(row, &extras) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        // 输出顺序不能依赖数据源的返回顺序：显式排序，保证同一轮扫描内稳定，
        // 便于 diff 与看板展示。
        out.sort_unstable_by(|a, b| a.symbol.base.cmp(&b.symbol.base));

        // 两种「少了一条」分开报：符号过滤是预期内的（X-Perps、反向合约），
        // 字段不可用则说明数据源变了，必须能一眼区分。
        if filtered > 0 {
            tracing::debug!(venue = %VENUE, filtered, "非 USDT 线性永续合约已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(VENUE, unusable);
        }
        if out.is_empty() {
            warn!(venue = %VENUE, "没有拿到任何线性 USDT 永续读数");
        }
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        // sz 是档数而非张数；拒绝越界而不是静默改变调用方请求的深度。
        if !(1..=MAX_DEPTH_LEVELS).contains(&levels) {
            return Err(ArbError::config("OKX 盘口档数必须在 1..=400"));
        }
        if symbol.quote != QUOTE || symbol.base.is_empty() {
            return Err(ArbError::config("OKX 深度仅支持 USDT 线性永续"));
        }
        let inst_id = format!("{}{SWAP_SUFFIX}", symbol.base);
        // 深度响应没有面值，必须另外取这一个合约的规格，不能猜一张等于一个币。
        let instruments = self
            .fetch::<Instrument>(
                self.client
                    .get(INSTRUMENTS_URL)
                    .query(&[("instId", inst_id.as_str())]),
                "深度合约规格",
            )
            .await?;
        let inst = instruments.iter().find(|inst| inst.inst_id == inst_id);
        let rows = self
            .fetch::<DepthRow>(
                self.client
                    .get(BOOKS_URL)
                    .query(&[("instId", inst_id.as_str()), ("sz", &levels.to_string())]),
                "盘口深度",
            )
            .await?;
        parse_depth(rows, inst, symbol)
    }

    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// 响应**降序**（最新在前），最后一根可能是**未收盘**的（`confirm = "0"`）——
    /// 未收盘的收盘价还在动，拿它拟合均值回归会把盘中噪声当信号、半衰期算短，
    /// 所以跳过。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let inst_id = format!("{}{SWAP_SUFFIX}", symbol.base);
        let bar = bar_for(interval_minutes)?;
        let url = format!(
            "{CANDLES_URL}?instId={inst_id}&bar={bar}&limit={}",
            limit.min(MAX_CANDLE_LIMIT)
        );
        let envelope: Envelope<Vec<String>> = get_json(self.client.get(&url), VENUE).await?;
        Ok(parse_candles(&unwrap_data(envelope, "K 线")?))
    }
}

/// 把一行资金费转成领域类型。`None` = 这一行不可用。
///
/// 抽成纯函数是为了让「周期推算」「费率缺失」「结算时刻缺失」这三条最容易出错的
/// 规则能被单测覆盖，不必真的打网络。三处都**不猜**：
///
/// - 费率字段缺失/不可解析 → 整行丢弃。**绝不回落成 0**：0 是合法的费率读数，
///   伪造出来的 0 会凭空造出巨大价差。
/// - 结算时刻缺失/为 0/超出可表示范围 → 丢弃，而不是拿当前时间顶上。
/// - 周期推不出来（缺 `prevFundingTime`、差值不是整数小时）→ 回落默认值并把
///   `interval_assumed` 置 `true`，而不是按 8h 硬算还标成场所给的。
fn parse_row(row: &FundingRateRow, extras: &Extras<'_>) -> Option<MarketSnapshot> {
    let base = row
        .inst_id
        .strip_suffix(SWAP_SUFFIX)
        .filter(|base| !base.is_empty())?;

    let period_rate = parse_decimal(&row.funding_rate)?;

    let funding_ms = parse_ms(&row.funding_time)?;
    let next_funding_at = DateTime::from_timestamp_millis(funding_ms)?;

    let (interval_h, interval_assumed) =
        match parse_ms(&row.prev_funding_time).and_then(|prev| interval_hours(funding_ms, prev)) {
            Some(hours) => (hours, false),
            None => (DEFAULT_FUNDING_INTERVAL_H, true),
        };

    // SWAP 没有直接给计价币成交额，只能用基础币数量乘最新价。
    // 这是估算（最新价 ≠ 24h 逐笔加权均价），但两个来源字段都是场所给的，不是编的；
    // 缺任何一个就留 `None`，不拿别的价格顶上。
    let quote_volume_24h = extras
        .vol_ccy_24h
        .and_then(parse_decimal)
        .zip(extras.last_px.and_then(parse_decimal))
        .map(|(base_volume, last_price)| base_volume * last_price);

    let (best_bid, best_ask) = match (
        extras
            .ticker
            .and_then(|t| t.bid_px.as_deref())
            .and_then(positive_decimal),
        extras
            .ticker
            .and_then(|t| t.ask_px.as_deref())
            .and_then(positive_decimal),
    ) {
        // 预开盘可能出现倒挂；它不是可成交的负成本，但资金费仍然有效。
        (Some(bid), Some(ask)) if ask < bid => (None, None),
        prices => prices,
    };
    let contract_value = extras.instrument.and_then(|inst| {
        // 只使用同一合约、标的币面值的线性 USDT 规格，未知单位宁可不报量。
        if inst.inst_id != row.inst_id
            || !is_linear_usdt_perp(inst)
            || inst.ct_val_ccy.as_deref() != Some(base)
        {
            return None;
        }
        inst.ct_val.as_deref().and_then(positive_decimal)
    });

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: Symbol::perp(base, QUOTE),
        period_rate,
        interval_h,
        interval_assumed,
        next_funding_at,
        next_funding_estimated: false,
        taker_fee: None,
        mark_price: extras.mark_px.and_then(parse_decimal),
        index_price: extras.idx_px.and_then(parse_decimal),
        best_bid,
        best_ask,
        bid_size_usdt: level_notional(
            extras.ticker.and_then(|t| t.bid_sz.as_deref()),
            contract_value,
            best_bid,
        ),
        ask_size_usdt: level_notional(
            extras.ticker.and_then(|t| t.ask_sz.as_deref()),
            contract_value,
            best_ask,
        ),
        open_interest_usdt: extras.oi_usd.and_then(parse_decimal),
        quote_volume_24h,
        max_leverage: None,
        maintenance_margin: None,
        oi_capped: false,
    })
}

fn positive_decimal(raw: &str) -> Option<Decimal> {
    parse_decimal(raw).filter(|value| *value > Decimal::ZERO)
}

/// 官方线性合约公式：张数 × 每张标的数量 × 本侧价格。
/// 任一环缺失或溢出都不能伪造深度；零量的一档也不能当作可成交流动性。
fn level_notional(
    contracts: Option<&str>,
    contract_value: Option<Decimal>,
    price: Option<Decimal>,
) -> Option<Decimal> {
    positive_decimal(contracts?)?
        .checked_mul(contract_value?)?
        .checked_mul(price?)
        .filter(|value| *value > Decimal::ZERO)
}

/// 与一档快照复用同一张数换算公式，避免逐合约深度另算一套单位。
fn parse_depth(
    rows: Vec<DepthRow>,
    inst: Option<&Instrument>,
    symbol: &Symbol,
) -> ArbResult<OrderBook> {
    let contract_value = inst
        .filter(|inst| {
            is_linear_usdt_perp(inst)
                && symbol.quote == QUOTE
                && inst.inst_id.strip_suffix(SWAP_SUFFIX) == Some(symbol.base.as_str())
                && inst.ct_val_ccy.as_deref() == Some(symbol.base.as_str())
        })
        .and_then(|inst| inst.ct_val.as_deref())
        .and_then(positive_decimal)
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "深度合约面值缺失或单位不匹配"))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| ArbError::venue(VENUE.as_str(), "深度响应没有数据行"))?;
    let parse_side = |raw: Vec<Vec<String>>| -> Vec<Level> {
        raw.into_iter()
            .filter_map(|entry| {
                let price = positive_decimal(entry.first()?)?;
                let notional_usdt = level_notional(
                    entry.get(1).map(String::as_str),
                    Some(contract_value),
                    Some(price),
                )?;
                Some(Level {
                    price,
                    notional_usdt,
                })
            })
            .collect()
    };
    let mut bids = parse_side(row.bids);
    let mut asks = parse_side(row.asks);
    // 实测上游已有序，但吃单估算依赖这个不变量，不能依赖上游永不改变排序。
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|a| a.price);
    match (bids.first(), asks.first()) {
        (Some(bid), Some(ask)) if ask.price >= bid.price => Ok(OrderBook {
            venue: VENUE,
            symbol: symbol.clone(),
            bids,
            asks,
        }),
        _ => Err(ArbError::venue(VENUE.as_str(), "深度盘口为空或交叉")),
    }
}

/// 毫秒时间戳字符串 → 毫秒数。`0` 与非法值都是 `None`：
/// `0` 交给 `from_timestamp_millis` 会变成 1970 年，一条「1970 年结算」的假数据
/// 会一路进到面板。
fn parse_ms(raw: &str) -> Option<i64> {
    raw.trim().parse::<i64>().ok().filter(|ms| *ms > 0)
}

/// 由本周期的两个结算时刻推出结算周期（小时）。
///
/// `fundingTime` 是本周期结算时刻、`prevFundingTime` 是上一周期的，两者都是场所给的
/// 毫秒时间戳，差值就是本周期长度。实测 482 个 SWAP 全部是整数小时
/// （273 个 8h、208 个 4h、1 个 1h）—— 直接按 8h 算会让 4h 合约的日化低估一半。
///
/// 差值不是正整数小时就不猜：返回 `None`，由调用方回落默认值并标记 `assumed`。
fn interval_hours(funding_time_ms: i64, prev_funding_time_ms: i64) -> Option<u32> {
    let delta = funding_time_ms.checked_sub(prev_funding_time_ms)?;
    if delta <= 0 || delta % MS_PER_HOUR != 0 {
        return None;
    }
    u32::try_from(delta / MS_PER_HOUR).ok().filter(|h| *h > 0)
}

/// 是不是 USDT 保证金的线性永续。
///
/// 判断用场所自己的字段（`ctType`/`settleCcy`/`state`），不用合约名猜。
/// OKX 的 SWAP 里混着 15 个反向合约（如 `BTC-USD-SWAP`，结算币是 BTC），
/// 资金费机制与线性合约不同，混进配对会算错。
fn is_linear_usdt_perp(inst: &Instrument) -> bool {
    inst.ct_type == "linear" && inst.settle_ccy == QUOTE && inst.state == "live"
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, estimate_fill};

    // 真实 sz=3 响应，保留张数与价格以防换算量级退化成「张数当 USDT」。
    const BTC_DEPTH: &str = r#"{"code":"0","msg":"","data":[{"asks":[["80430.3","41.24","0","47"],["80430.4","0.03","0","3"],["80430.5","0.02","0","1"]],"bids":[["80430.2","649.99","0","66"],["80430.1","0.02","0","2"],["80430","48.41","0","7"]],"ts":"1789898129604","seqId":340697334194}]}"#;

    fn depth_rows() -> Vec<DepthRow> {
        unwrap_data(serde_json::from_str(BTC_DEPTH).unwrap(), "盘口深度").unwrap()
    }

    #[test]
    fn depth_contract_sizes_map_to_quote_notionals_and_executable_order() {
        let inst = instrument(BTC_INSTRUMENT);
        let symbol = Symbol::perp("BTC", QUOTE);
        let mut rows = depth_rows();
        // 打乱真实响应顺序，防止实现把上游排序当作永久保证。
        rows[0].asks.reverse();
        rows[0].bids.rotate_left(1);
        let book = parse_depth(rows, Some(&inst), &symbol).unwrap();
        assert_eq!(book.venue, VENUE);
        assert_eq!(book.symbol, symbol);
        for (levels, expected) in [
            (
                &book.bids,
                [
                    ("80430.2", "522788.25698"),
                    ("80430.1", "16.08602"),
                    ("80430", "38936.163"),
                ],
            ),
            (
                &book.asks,
                [
                    ("80430.3", "33169.45572"),
                    ("80430.4", "24.12912"),
                    ("80430.5", "16.0861"),
                ],
            ),
        ] {
            assert_eq!(levels.len(), expected.len());
            for (level, (price, notional)) in levels.iter().zip(expected) {
                assert_eq!(level.price, parse_decimal(price).unwrap());
                assert_eq!(level.notional_usdt, parse_decimal(notional).unwrap());
            }
        }
        // 吃穿真实三档可成交名义；买卖两侧的不利滑点都应为正。
        for (side, expected) in [(Side::Buy, "33209.67094"), (Side::Sell, "561740.506")] {
            let fill =
                estimate_fill(book.side(side), parse_decimal("1000000").unwrap(), side).unwrap();
            assert_eq!(fill.filled_usdt, parse_decimal(expected).unwrap());
            assert!(fill.exhausted);
            assert!(fill.slippage > Decimal::ZERO);
            assert!(fill.slippage < parse_decimal("0.00001").unwrap());
        }
    }

    #[test]
    fn depth_rejects_empty_crossed_and_unconvertible_books() {
        let inst = instrument(BTC_INSTRUMENT);
        let symbol = Symbol::perp("BTC", QUOTE);
        assert!(parse_depth(Vec::new(), Some(&inst), &symbol).is_err());
        for side in [Side::Buy, Side::Sell] {
            let mut rows = depth_rows();
            match side {
                Side::Buy => rows[0].asks.clear(),
                Side::Sell => rows[0].bids.clear(),
            }
            assert!(parse_depth(rows, Some(&inst), &symbol).is_err());
        }
        let mut crossed = depth_rows();
        crossed[0].asks[0][0] = "80430.1".into();
        assert!(parse_depth(crossed, Some(&inst), &symbol).is_err());
        let mut locked = depth_rows();
        locked[0].asks[0][0] = "80430.2".into();
        let book = parse_depth(locked, Some(&inst), &symbol).unwrap();
        assert_eq!(book.best_bid(), book.best_ask());

        assert!(parse_depth(depth_rows(), None, &symbol).is_err());
        for value in [None, Some("0".into()), Some("bad".into())] {
            let mut missing = instrument(BTC_INSTRUMENT);
            missing.ct_val = value;
            assert!(parse_depth(depth_rows(), Some(&missing), &symbol).is_err());
        }
        let mut wrong_unit = instrument(BTC_INSTRUMENT);
        wrong_unit.ct_val_ccy = Some("USD".into());
        assert!(parse_depth(depth_rows(), Some(&wrong_unit), &symbol).is_err());
        let wrong_contract = instrument(DOGE_INSTRUMENT);
        assert!(parse_depth(depth_rows(), Some(&wrong_contract), &symbol).is_err());
    }

    #[test]
    fn depth_skips_invalid_levels_without_fabricating_liquidity() {
        let inst = instrument(BTC_INSTRUMENT);
        let symbol = Symbol::perp("BTC", QUOTE);
        let mut rows = depth_rows();
        rows[0].bids.extend([
            vec![],
            vec!["invalid".into(), "1".into()],
            vec!["80430".into(), "0".into()],
            vec!["80430".into(), Decimal::MAX.to_string()],
        ]);
        let book = parse_depth(rows, Some(&inst), &symbol).unwrap();
        assert_eq!(book.bids.len(), 3);
        assert_eq!(
            book.bids[0].notional_usdt,
            parse_decimal("522788.25698").unwrap()
        );

        let mut rows = depth_rows();
        for entry in &mut rows[0].asks {
            entry[1] = "0".into();
        }
        assert!(parse_depth(rows, Some(&inst), &symbol).is_err());
    }

    #[test]
    fn depth_business_errors_and_null_data_are_not_empty_books() {
        for body in [
            r#"{"code":"51001","msg":"Instrument ID does not exist."}"#,
            r#"{"code":"51000","msg":"Parameter sz error."}"#,
        ] {
            let envelope: Envelope<DepthRow> = serde_json::from_str(body).unwrap();
            assert!(unwrap_data(envelope, "盘口深度").is_err());
        }
        let envelope: Envelope<DepthRow> =
            serde_json::from_str(r#"{"code":"0","data":null,"msg":""}"#).unwrap();
        let inst = instrument(BTC_INSTRUMENT);
        assert!(
            parse_depth(
                unwrap_data(envelope, "盘口深度").unwrap(),
                Some(&inst),
                &Symbol::perp("BTC", QUOTE),
            )
            .is_err()
        );
    }

    /// 实测片段：2026-09-19T13:17Z 打 `?instId=ANY` 返回的 BTC-USDT-SWAP（8h 周期）。
    const BTC_ROW: &str = r#"{"formulaType":"withRate","fundingRate":"0.0001000000000000","fundingTime":"1789833600000","impactValue":"20000.0000000000000000","instId":"BTC-USDT-SWAP","instType":"SWAP","interestRate":"0.0001000000000000","maxFundingRate":"0.00375","method":"current_period","minFundingRate":"-0.00375","nextFundingRate":"","nextFundingTime":"1789862400000","premium":"-0.0001969158061855","prevFundingTime":"1789804800000","settFundingRate":"0.0001000000000000","settState":"settled","ts":"1789824243998"}"#;

    /// 同一次响应里的 OKB-USDT-SWAP：**4h** 周期（`prevFundingTime` 是 12:00）。
    const OKB_ROW: &str = r#"{"formulaType":"withRate","fundingRate":"0.0000500000000000","fundingTime":"1789833600000","impactValue":"4000.0000000000000000","instId":"OKB-USDT-SWAP","instType":"SWAP","interestRate":"0.0001000000000000","maxFundingRate":"0.01","method":"current_period","minFundingRate":"-0.01","nextFundingRate":"","nextFundingTime":"1789848000000","premium":"0.0001016353286903","prevFundingTime":"1789819200000","settFundingRate":"0.0000500000000000","settState":"settled","ts":"1789824245867"}"#;

    /// 同一次响应里的 ONE-USDT-SWAP：**1h** 周期。周期绝不能按 8h 假设。
    const ONE_ROW: &str = r#"{"formulaType":"withRate","fundingRate":"-0.0018087460514744","fundingTime":"1789826400000","impactValue":"2000.0000000000000000","instId":"ONE-USDT-SWAP","instType":"SWAP","interestRate":"0.0001000000000000","maxFundingRate":"0.01","method":"current_period","minFundingRate":"-0.01","nextFundingRate":"","nextFundingTime":"1789830000000","premium":"-0.0151184578313896","prevFundingTime":"1789822800000","settFundingRate":"-0.0020913845892044","settState":"settled","ts":"1789824245248"}"#;

    /// 同一条 feed 里的 X-Perps 交割合约：必须被挡掉。
    const XPERP_ROW: &str = r#"{"formulaType":"withRate","fundingRate":"0.0000000000000000","fundingTime":"1789833600000","impactValue":"10000.0000000000000000","instId":"XAU-USD_UM_XPERP-310502","instType":"FUTURES","interestRate":"0.0000000000000000","maxFundingRate":"0.0075","method":"current_period","minFundingRate":"-0.0075","nextFundingRate":"","nextFundingTime":"1789862400000","premium":"-0.0001392818081975","prevFundingTime":"1789804800000","settFundingRate":"0.0000000000000000","settState":"settled","ts":"1789824243487"}"#;

    /// 实测 `/api/v5/public/instruments?instType=SWAP` 的一行（只留本模块用到的字段）。
    const BTC_INSTRUMENT: &str = r#"{"instId":"BTC-USDT-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDT","state":"live","instFamily":"BTC-USDT","ctVal":"0.01","ctValCcy":"BTC"}"#;

    /// 实测的反向合约：结算币是 BTC，`ctType` 是 `inverse`。
    const BTC_INVERSE_INSTRUMENT: &str = r#"{"instId":"BTC-USD-SWAP","instType":"SWAP","ctType":"inverse","settleCcy":"BTC","state":"live","instFamily":"BTC-USD","ctVal":"100","ctValCcy":"USD"}"#;

    /// 字段形状取自真实响应，值改成 USDC 结算：当前 OKX 没有这类 SWAP，
    /// 但过滤规则必须挡住它（USDC 保证金的资金费与 USDT 不是一回事）。
    const BTC_USDC_INSTRUMENT: &str = r#"{"instId":"BTC-USDC-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDC","state":"live","instFamily":"BTC-USDC","ctVal":"0.01","ctValCcy":"BTC"}"#;

    /// 同上：字段形状取自真实响应，`state` 改成 `suspend`（下架/暂停的合约不能进配对）。
    const BTC_SUSPENDED_INSTRUMENT: &str = r#"{"instId":"BTC-USDT-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDT","state":"suspend","instFamily":"BTC-USDT","ctVal":"0.01","ctValCcy":"BTC"}"#;

    /// 批量 tickers 实测片段，保留盘口原值以防字段方向或面值换算出错。
    const BTC_TICKER: &str = r#"{"instId":"BTC-USDT-SWAP","last":"80352.6","volCcy24h":"46985.2381","bidPx":"80352.5","askPx":"80352.6","bidSz":"284.29","askSz":"700.61","ts":"1789879678366"}"#;
    const DOGE_TICKER: &str = r#"{"instId":"DOGE-USDT-SWAP","last":"0.0854","volCcy24h":"4500729030","bidPx":"0.08539","askPx":"0.0854","bidSz":"200.46","askSz":"209.87","ts":"1789879678463"}"#;
    const DOGE_INSTRUMENT: &str = r#"{"instId":"DOGE-USDT-SWAP","ctType":"linear","settleCcy":"USDT","state":"live","instFamily":"DOGE-USDT","ctVal":"1000","ctValCcy":"DOGE"}"#;

    fn snapshot_with_book(ticker: &Ticker, inst: Option<&Instrument>) -> MarketSnapshot {
        let mut funding = row(BTC_ROW);
        funding.inst_id.clone_from(&ticker.inst_id);
        parse_row(
            &funding,
            &Extras {
                ticker: Some(ticker),
                instrument: inst,
                // 盘口缺失时不能拿这些参考价兜底。
                mark_px: Some("90000"),
                last_px: Some(&ticker.last),
                ..Extras::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn batch_book_prices_and_contract_sizes_map_to_quote_notionals() {
        // BTC 的 0.01 与 DOGE 的 1000 面值分别防止多算 100 倍、少算 1000 倍。
        for (tick, inst, bid, ask, bid_notional, ask_notional) in [
            (
                BTC_TICKER,
                BTC_INSTRUMENT,
                "80352.5",
                "80352.6",
                "228434.12225",
                "562958.35086",
            ),
            (
                DOGE_TICKER,
                DOGE_INSTRUMENT,
                "0.08539",
                "0.0854",
                "17117.2794",
                "17922.898",
            ),
        ] {
            let ticker: Ticker = serde_json::from_str(tick).unwrap();
            let inst = instrument(inst);
            let snapshot = snapshot_with_book(&ticker, Some(&inst));
            assert_eq!(snapshot.best_bid, parse_decimal(bid));
            assert_eq!(snapshot.best_ask, parse_decimal(ask));
            assert_eq!(snapshot.bid_size_usdt, parse_decimal(bid_notional));
            assert_eq!(snapshot.ask_size_usdt, parse_decimal(ask_notional));
        }
    }

    #[test]
    fn missing_book_fields_never_fall_back_to_reference_prices() {
        let inst = instrument(BTC_INSTRUMENT);
        for json in [
            r#"{"instId":"BTC-USDT-SWAP","last":"80352.6","volCcy24h":"46985.2381"}"#,
            r#"{"instId":"BTC-USDT-SWAP","last":"80352.6","volCcy24h":"46985.2381","bidPx":"","askPx":null,"bidSz":"","askSz":null}"#,
        ] {
            let ticker: Ticker = serde_json::from_str(json).unwrap();
            let snapshot = snapshot_with_book(&ticker, Some(&inst));
            assert_eq!(snapshot.best_bid, None);
            assert_eq!(snapshot.best_ask, None);
            assert_eq!(snapshot.bid_size_usdt, None);
            assert_eq!(snapshot.ask_size_usdt, None);
            assert_eq!(snapshot.period_rate, parse_decimal("0.0001").unwrap());
        }
        let bare = parse_row(&row(BTC_ROW), &Extras::default()).unwrap();
        assert_eq!((bare.best_bid, bare.best_ask), (None, None));
        assert_eq!((bare.bid_size_usdt, bare.ask_size_usdt), (None, None));
    }

    #[test]
    fn crossed_book_is_unknown_but_locked_book_is_valid() {
        let inst = instrument(BTC_INSTRUMENT);
        let mut ticker: Ticker = serde_json::from_str(BTC_TICKER).unwrap();
        ticker.ask_px = Some("80352.4".into());
        let crossed = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!((crossed.best_bid, crossed.best_ask), (None, None));
        assert_eq!((crossed.bid_size_usdt, crossed.ask_size_usdt), (None, None));
        assert_eq!(crossed.period_rate, parse_decimal("0.0001").unwrap());
        ticker.ask_px.clone_from(&ticker.bid_px);
        let locked = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!(locked.best_bid, parse_decimal("80352.5"));
        assert_eq!(locked.best_ask, parse_decimal("80352.5"));
        assert_eq!(locked.ask_size_usdt, parse_decimal("562957.65025"));
    }

    #[test]
    fn missing_or_unknown_contract_value_keeps_prices_but_not_sizes() {
        let ticker: Ticker = serde_json::from_str(BTC_TICKER).unwrap();
        let missing = snapshot_with_book(&ticker, None);
        assert_eq!(missing.best_bid, parse_decimal("80352.5"));
        assert_eq!((missing.bid_size_usdt, missing.ask_size_usdt), (None, None));
        let mut inst = instrument(BTC_INSTRUMENT);
        inst.ct_val_ccy = Some("USD".into());
        let unknown_ccy = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!(unknown_ccy.best_ask, parse_decimal("80352.6"));
        assert_eq!(
            (unknown_ccy.bid_size_usdt, unknown_ccy.ask_size_usdt),
            (None, None)
        );
        inst.ct_val_ccy = Some("BTC".into());
        inst.ct_val = None;
        let missing_face_value = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!(missing_face_value.bid_size_usdt, None);
        assert_eq!(missing_face_value.ask_size_usdt, None);
    }

    #[test]
    fn unusable_side_and_overflow_do_not_fabricate_liquidity() {
        let mut inst = instrument(BTC_INSTRUMENT);
        let mut ticker: Ticker = serde_json::from_str(BTC_TICKER).unwrap();
        ticker.bid_px = Some("invalid".into());
        ticker.ask_sz = Some("0".into());
        let snapshot = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!((snapshot.best_bid, snapshot.bid_size_usdt), (None, None));
        assert_eq!(snapshot.best_ask, parse_decimal("80352.6"));
        assert_eq!(snapshot.ask_size_usdt, None);

        ticker = serde_json::from_str(BTC_TICKER).unwrap();
        inst.ct_val = Some(Decimal::MAX.to_string());
        let overflow = snapshot_with_book(&ticker, Some(&inst));
        assert_eq!(overflow.best_bid, parse_decimal("80352.5"));
        assert_eq!(
            (overflow.bid_size_usdt, overflow.ask_size_usdt),
            (None, None)
        );
    }

    fn row(json: &str) -> FundingRateRow {
        serde_json::from_str(json).expect("fixture 必须能反序列化")
    }

    fn instrument(json: &str) -> Instrument {
        serde_json::from_str(json).expect("fixture 必须能反序列化")
    }

    #[test]
    fn a_business_error_in_a_200_response_is_an_error_not_an_empty_result() {
        // 实测：这两种都是 HTTP 200 + code != "0"。当成空结果 = 静默少一家场所。
        let envelope: Envelope<FundingRateRow> = serde_json::from_str(
            r#"{"code":"50014","data":[],"msg":"Parameter instId can not be empty."}"#,
        )
        .unwrap();
        let error = unwrap_data(envelope, "资金费").unwrap_err();
        assert!(error.to_string().contains("50014"), "{error}");

        let envelope: Envelope<FundingRateRow> =
            serde_json::from_str(r#"{"code":"51000","data":[],"msg":"Parameter instId error"}"#)
                .unwrap();
        assert!(unwrap_data(envelope, "资金费").is_err());

        // `data` 为 null 时也不能 panic，只是空表（失败仍由 code 判定）。
        let envelope: Envelope<FundingRateRow> =
            serde_json::from_str(r#"{"code":"0","data":null,"msg":""}"#).unwrap();
        assert!(unwrap_data(envelope, "资金费").unwrap().is_empty());

        let body = format!(r#"{{"code":"0","data":[{BTC_ROW}],"msg":""}}"#);
        let envelope: Envelope<FundingRateRow> = serde_json::from_str(&body).unwrap();
        assert_eq!(unwrap_data(envelope, "资金费").unwrap().len(), 1);
    }

    #[test]
    fn the_settlement_interval_comes_from_the_venues_own_timestamps() {
        // 三个真实合约：8h / 4h / 1h。按固定 8h 算，4h 合约的日化会低估一半，
        // 1h 合约低估 8 倍。
        for (json, expected) in [(BTC_ROW, 8u32), (OKB_ROW, 4), (ONE_ROW, 1)] {
            let rate = parse_row(&row(json), &Extras::default()).unwrap();
            assert_eq!(rate.interval_h, expected, "{}", rate.symbol);
            assert!(!rate.interval_assumed, "周期是场所给的，不能标成假设值");
        }

        // 毫秒差值的单位换算本身。
        assert_eq!(
            interval_hours(1_789_833_600_000, 1_789_804_800_000),
            Some(8)
        );
        assert_eq!(
            interval_hours(1_789_833_600_000, 1_789_819_200_000),
            Some(4)
        );
    }

    #[test]
    fn an_unusable_previous_settlement_time_falls_back_to_the_default_and_is_flagged() {
        // `prevFundingTime` 缺失/为 0：推不出周期 → 默认值 + assumed。
        let mut missing = row(BTC_ROW);
        missing.prev_funding_time = "0".into();
        let rate = parse_row(&missing, &Extras::default()).unwrap();
        assert_eq!(rate.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(rate.interval_assumed, "回落值必须标记出来");

        // 差值不是整数小时（这里 30 分钟）→ 不四舍五入、也不当成 0h。
        let mut odd = row(BTC_ROW);
        odd.prev_funding_time = (1_789_833_600_000i64 - 1_800_000).to_string();
        let rate = parse_row(&odd, &Extras::default()).unwrap();
        assert_eq!(rate.interval_h, DEFAULT_FUNDING_INTERVAL_H);
        assert!(rate.interval_assumed);

        // 时间戳倒挂同样不猜。
        assert_eq!(interval_hours(1_000_000, 2_000_000), None);
        assert_eq!(interval_hours(1_000_000, 1_000_000), None);
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        let mut empty = row(BTC_ROW);
        empty.funding_rate = String::new();
        assert!(parse_row(&empty, &Extras::default()).is_none());

        let mut broken = row(BTC_ROW);
        broken.funding_rate = "n/a".into();
        assert!(parse_row(&broken, &Extras::default()).is_none());
    }

    #[test]
    fn a_zero_or_unparseable_settlement_time_drops_the_row() {
        let mut zero = row(BTC_ROW);
        zero.funding_time = "0".into();
        assert!(parse_row(&zero, &Extras::default()).is_none());

        let mut broken = row(BTC_ROW);
        broken.funding_time = "not-a-time".into();
        assert!(parse_row(&broken, &Extras::default()).is_none());

        let mut out_of_range = row(BTC_ROW);
        out_of_range.funding_time = i64::MAX.to_string();
        assert!(parse_row(&out_of_range, &Extras::default()).is_none());
    }

    #[test]
    fn next_funding_at_is_the_upcoming_settlement_time() {
        // 实测：响应里的 ts=1789824243998（13:24Z）早于 fundingTime=1789833600000
        // （2026-09-19T16:00:00Z），而 nextFundingTime 还要再晚一个周期。
        // 所以 `fundingTime` 才是「下一次结算」，不能用 nextFundingTime。
        let rate = parse_row(&row(BTC_ROW), &Extras::default()).unwrap();
        assert_eq!(rate.next_funding_at.timestamp_millis(), 1_789_833_600_000);
        assert!(!rate.next_funding_estimated, "结算时刻是场所给的");
        assert_eq!(rate.symbol, Symbol::perp("BTC", QUOTE));
    }

    #[test]
    fn taker_fee_is_unknown_not_zero() {
        // OKX 的吃单费率只在要签名的 `/api/v5/account/trade-fee` 里，公共行情拿不到。
        let rate = parse_row(&row(BTC_ROW), &Extras::default()).unwrap();
        assert_eq!(rate.taker_fee, None);
    }

    #[test]
    fn only_linear_usdt_live_swaps_are_eligible() {
        assert!(is_linear_usdt_perp(&instrument(BTC_INSTRUMENT)));
        assert!(
            !is_linear_usdt_perp(&instrument(BTC_INVERSE_INSTRUMENT)),
            "反向合约结算币是 BTC"
        );
        assert!(
            !is_linear_usdt_perp(&instrument(BTC_USDC_INSTRUMENT)),
            "USDC 保证金不是 USDT 永续"
        );
        assert!(
            !is_linear_usdt_perp(&instrument(BTC_SUSPENDED_INSTRUMENT)),
            "下架/暂停的合约不能进配对"
        );
    }

    #[test]
    fn x_perp_futures_rows_from_the_same_feed_are_not_perps() {
        // `instId=ANY` 会把 192 个 X-Perps FUTURES 一起返回（如 XAU-USD_UM_XPERP-310502）。
        let futures = row(XPERP_ROW);
        assert_eq!(futures.inst_type, "FUTURES");
        assert!(parse_row(&futures, &Extras::default()).is_none());
    }

    #[test]
    fn auxiliary_readings_are_mapped_and_quote_volume_is_derived_from_base_volume() {
        // 实测（同一时刻的批量端点）：OKB-USDT-SWAP 的
        // markPx / idxPx / oiUsd / last / volCcy24h。
        let extras = Extras {
            mark_px: Some("120.65"),
            idx_px: Some("120.54"),
            oi_usd: Some("30174313.0282"),
            last_px: Some("121.29"),
            vol_ccy_24h: Some("326142.74"),
            ..Extras::default()
        };
        let rate = parse_row(&row(OKB_ROW), &extras).unwrap();
        assert_eq!(rate.symbol, Symbol::perp("OKB", QUOTE));
        assert_eq!(rate.mark_price, parse_decimal("120.65"));
        assert_eq!(rate.index_price, parse_decimal("120.54"));
        assert_eq!(rate.open_interest_usdt, parse_decimal("30174313.0282"));
        // `volCcy24h` 是基础币数量（实测 vol24h × ctVal == volCcy24h），乘最新价才是 USDT。
        assert_eq!(rate.quote_volume_24h, parse_decimal("39557852.9346"));

        // 辅助端点整体失败时字段留空、费率照常给出 —— 不是 0，也不是别的价格顶上来。
        let bare = parse_row(&row(OKB_ROW), &Extras::default()).unwrap();
        assert_eq!(bare.mark_price, None);
        assert_eq!(bare.index_price, None);
        assert_eq!(bare.open_interest_usdt, None);
        assert_eq!(bare.quote_volume_24h, None);
        assert_eq!(
            bare.period_rate,
            parse_decimal("0.0000500000000000").unwrap()
        );
    }
}
