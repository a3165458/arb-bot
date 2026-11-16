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
//!
//! **股票等传统资产的合约名不是代码。** 实测（2026-10-21，1088 个 USDT 永续）`NVIDIA_USDT` 是 NVDA、
//! `AAPLSTOCK_USDT` 是 AAPL、`COINBASE_USDT` 是 某合约，标准代码只在 `displayNameEn`
//! （`NVDA_USDT PERPETUAL`）里。`tradfi` 板块（股票、ETF、指数、商品、外汇，405 个改名）的 base 取
//! 显示名里的代码，别家同名合约才对得上（仍要过扫描器的价格核实）；加密币照旧按合约名，不改动已有的
//! 资金费配对。深度、K 线要的原名按上一轮的对照反查，见 [`MexcApi::native`]。

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
pub const DETAIL_URL: &str = "https://contract.mexc.com/api/v1/contract/detail";
/// 传统资产（股票、ETF、指数、商品、外汇）所在的板块：只有它们的 base 取显示名里的代码。
const TRADFI_PLATE: &str = "mc-trade-zone-tradfi";

/// 我们的 API 账户在 MEXC 实际付的吃单费率。2026-10-26 按合约读 `tiered_fee_rate/v2`：NVIDIA、AAPL、TESLA、
/// 某合约、QQQ、某合约、XAU、BTC、某合约 全是 `realTakerFee` 0.0008（`level` 9999、`feeRateType` BASE、挂单 0.0006），
/// 某次实盘 实盘成交也是 0.080%。公开 `contract/detail` 的 `takerFeeRate`（0.0002，偶尔 0.0004）不适用这个账户，
/// 按它算会把成本低估四倍、开出扣费后不赚的单。
pub const API_TAKER_FEE: Decimal = Decimal::from_parts(8, 0, 0, false, 4);

/// 公开费率和 [`API_TAKER_FEE`] 取较大的：没给公开费率也按账户费率算（不退化成「不知道」）。
fn account_taker_fee(public: Option<Decimal>) -> Option<Decimal> {
    Some(public.map_or(API_TAKER_FEE, |fee| fee.max(API_TAKER_FEE)))
}
/// 商品类板块（价差监控的类别用）。
const COMMODITY_PLATES: [&str; 3] = [
    "mc-trade-zone-Commodities",
    "mc-trade-zone-metals",
    "mc-trade-zone-metalsfutures",
];
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
    /// 合约名 → base（[`canonical_bases`]），每轮扫描按合约详情刷新；详情这一轮没取到时沿用上一轮，
    /// 股票类的名字不会在 `NVDA` 和 `NVIDIA` 之间来回跳。
    bases: std::sync::Mutex<HashMap<String, String>>,
}

impl MexcApi {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            bases: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// 这一轮的合约名 → base。
    fn remember_bases(&self, details: &HashMap<String, Detail>) -> HashMap<String, String> {
        let mut cached = self.bases.lock().unwrap_or_else(|e| e.into_inner());
        if !details.is_empty() {
            *cached = canonical_bases(details.values().map(Detail::naming));
        }
        cached.clone()
    }

    /// base 对应的合约名（深度、K 线用）：先查上一轮扫描的对照，没有就按 `{base}_USDT`。
    fn native(&self, base: &str) -> String {
        let cached = self.bases.lock().unwrap_or_else(|e| e.into_inner());
        cached
            .iter()
            .find(|(_, known)| known.as_str() == base)
            .map(|(symbol, _)| symbol.clone())
            .unwrap_or_else(|| format!("{base}_{QUOTE}"))
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
    /// 第一档维持保证金率（我们的单笔远在第一档内：NVIDIA 第一档到 29000 张）。
    #[serde(default)]
    maintenance_margin_rate: Option<f64>,
    #[serde(default)]
    max_leverage: Option<f64>,
    #[serde(default)]
    display_name_en: Option<String>,
    #[serde(default)]
    concept_plate: Option<Vec<String>>,
    #[serde(default)]
    state: Option<i64>,
    /// 盘前代币（上线现货前的合约）。
    #[serde(default)]
    pre_market: Option<bool>,
}

impl Detail {
    fn in_plate(&self, plate: &str) -> bool {
        self.concept_plate
            .as_ref()
            .is_some_and(|plates| plates.iter().any(|p| p == plate))
    }

    /// 上市前（`mc-trade-zone-preipo`）或盘前（`mc-trade-zone-PreMarket` / `preMarket`）合约。
    fn pre_ipo(&self) -> bool {
        self.pre_market == Some(true)
            || self.concept_plate.as_ref().is_some_and(|plates| {
                plates.iter().any(|p| {
                    let p = p.to_ascii_lowercase();
                    p.ends_with("-preipo") || p.ends_with("-premarket")
                })
            })
    }

    fn naming(&self) -> (&str, Option<&str>, bool) {
        (
            &self.symbol,
            self.display_name_en.as_deref(),
            self.in_plate(TRADFI_PLATE),
        )
    }
}

/// 合约名里的 base（`BTC_USDT` → `BTC`）；不是 USDT 永续的名字为 `None`。
fn symbol_base(symbol: &str) -> Option<&str> {
    symbol
        .strip_suffix(&format!("_{QUOTE}"))
        .filter(|base| !base.is_empty() && !base.contains('_'))
}

/// 显示名里的代码（`NVDA_USDT PERPETUAL` → `NVDA`）。只认大写字母数字：`GOLD(XAU)`、中文名不用。
fn display_code(display: &str) -> Option<&str> {
    display
        .trim()
        .strip_suffix(" PERPETUAL")?
        .strip_suffix(&format!("_{QUOTE}"))
        .filter(|code| {
            !code.is_empty()
                && code
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        })
}

/// 合约名 → base。输入是（合约名, 显示名, 是否 `tradfi` 板块）。`tradfi` 板块用显示名里的代码，
/// 其余按合约名。改出来的名字和别的合约撞了（两份资产会被认成一个）就不改，保留合约名。
pub fn canonical_bases<'a>(
    rows: impl IntoIterator<Item = (&'a str, Option<&'a str>, bool)>,
) -> HashMap<String, String> {
    let mut plain: HashMap<String, String> = HashMap::new();
    let mut wanted: HashMap<String, String> = HashMap::new();
    for (symbol, display, tradfi) in rows {
        let Some(base) = symbol_base(symbol) else {
            continue;
        };
        plain.insert(symbol.to_string(), base.to_string());
        if let Some(code) = display.and_then(display_code).filter(|_| tradfi)
            && code != base
        {
            wanted.insert(symbol.to_string(), code.to_string());
        }
    }
    let mut uses: HashMap<String, usize> = HashMap::new();
    for (symbol, base) in &plain {
        *uses
            .entry(wanted.get(symbol).unwrap_or(base).clone())
            .or_default() += 1;
    }
    plain
        .into_iter()
        .map(|(symbol, base)| {
            let name = match wanted.get(&symbol) {
                Some(code) if uses.get(code) == Some(&1) => code.clone(),
                _ => base,
            };
            (symbol, name)
        })
        .collect()
}

/// 价差监控要的一个 USDT 永续（`/contract/detail` 全量一次）。
#[derive(Debug, Clone, PartialEq)]
pub struct Contract {
    /// 合约名（订阅、下单用），如 `NVIDIA_USDT`。
    pub symbol: String,
    /// 标准名，见 [`canonical_bases`]。
    pub base: String,
    /// 每张的标的数量：盘口量是张数，乘它才是标的数量。
    pub contract_size: Decimal,
    pub taker_fee: Option<Decimal>,
    /// `tradfi` 板块（股票、ETF、指数、商品、外汇）。
    pub tradfi: bool,
    pub commodity: bool,
    /// 上市前 / 盘前合约（见 `Detail::pre_ipo`）：各家上市换算规则不同，不交易。
    pub pre_ipo: bool,
}

fn detail_rows(details: &serde_json::Value) -> ArbResult<Vec<Detail>> {
    serde_json::from_value::<Envelope<Detail>>(details.clone())
        .map_err(|error| ArbError::venue(VENUE.as_str(), format!("合约详情格式不对：{error}")))?
        .into_data("合约详情")
}

/// 合约名 → 标准名，在详情的**全部**行上算（扫描、价差监控、下单三处同一份输入）：撞名保护看的是
/// 整张表，先按状态筛再算，某个合约一停牌，和它撞名的那个就会改名，持仓的标的跟着变。
pub fn bases_of(details: &serde_json::Value) -> ArbResult<HashMap<String, String>> {
    Ok(canonical_bases(
        detail_rows(details)?.iter().map(Detail::naming),
    ))
}

/// [`bases_of`] 的数据源（下单模块用）。
pub async fn fetch_bases(client: &Client) -> ArbResult<HashMap<String, String>> {
    let details: serde_json::Value = get_json(client.get(DETAIL_URL), VENUE).await?;
    bases_of(&details)
}

/// 在交易（`state == 0`）、USDT 结算、面值有效的永续。标准名按全部行算（见 [`bases_of`]）。
pub fn contracts(details: &serde_json::Value) -> ArbResult<Vec<Contract>> {
    let rows = detail_rows(details)?;
    let bases = canonical_bases(rows.iter().map(Detail::naming));
    Ok(rows
        .iter()
        .filter(|row| {
            row.state == Some(0)
                && row
                    .settle_coin
                    .as_deref()
                    .is_some_and(|coin| coin.eq_ignore_ascii_case(QUOTE))
        })
        .filter_map(|row| {
            let contract_size = row
                .contract_size
                .and_then(from_json_f64)
                .filter(|size| *size > Decimal::ZERO)?;
            Some(Contract {
                symbol: row.symbol.clone(),
                base: bases.get(&row.symbol)?.clone(),
                contract_size,
                taker_fee: account_taker_fee(row.taker_fee_rate.and_then(from_json_f64)),
                tradfi: row.in_plate(TRADFI_PLATE),
                commodity: COMMODITY_PLATES.iter().any(|plate| row.in_plate(plate)),
                pre_ipo: row.pre_ipo(),
            })
        })
        .collect())
}

/// [`contracts`] 的数据源（约 1.2 MB）。
pub async fn fetch_contracts(client: &Client) -> ArbResult<Vec<Contract>> {
    let details: serde_json::Value = get_json(client.get(DETAIL_URL), VENUE).await?;
    contracts(&details)
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
        let bases = self.remember_bases(&details);
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
            let base = bases.get(&row.symbol).map(String::as_str);
            match parse_row(&row, base, detail, tickers.get(&row.symbol)) {
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
        let native = self.native(&symbol.base);

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
        let native = self.native(&symbol.base);
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
/// `base` 是 [`canonical_bases`] 给的标准名；`None` 按合约名。
fn parse_row(
    row: &FundingRow,
    base: Option<&str>,
    detail: Option<&Detail>,
    ticker: Option<&Ticker>,
) -> Option<MarketSnapshot> {
    let base = match base {
        Some(base) => base,
        None => row.symbol.strip_suffix(&format!("_{QUOTE}"))?,
    };
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
        taker_fee: account_taker_fee(
            detail
                .and_then(|d| d.taker_fee_rate)
                .and_then(from_json_f64),
        ),
        mark_price,
        index_price: row.idx_price.and_then(from_json_f64),
        best_bid,
        best_ask,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt,
        quote_volume_24h: ticker.and_then(|t| t.amount24).and_then(from_json_f64),
        max_leverage: detail
            .and_then(|d| d.max_leverage)
            .and_then(from_json_f64)
            .filter(|max| *max >= Decimal::ONE),
        // 没有它时「自动加保证金」等规则算不出强平距离，MEXC 的每一笔开仓都会被拒。
        maintenance_margin: detail
            .and_then(|d| d.maintenance_margin_rate)
            .and_then(from_json_f64)
            .filter(|mmr| *mmr > Decimal::ZERO && *mmr < Decimal::ONE),
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
        {"symbol":"BTC_USDT","settleCoin":"USDT","state":0,"contractSize":0.0001,"takerFeeRate":0.0004,"maintenanceMarginRate":0.004,"maxLeverage":500}
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
            let rate = parse_row(&row, None, detail.as_ref(), Some(&ticker)).unwrap();
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
            let rate = parse_row(&row, None, Some(&detail()), ticker).unwrap();
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
            let rate = parse_row(&row, None, None, Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, None);
            assert_eq!(rate.best_ask, Some(Decimal::new(803474, 1)));

            ticker.bid1 = Some(80347.3);
            ticker.ask1 = invalid;
            let rate = parse_row(&row, None, None, Some(&ticker)).unwrap();
            assert_eq!(rate.best_bid, Some(Decimal::new(803473, 1)));
            assert_eq!(rate.best_ask, None);
        }

        ticker.bid1 = Some(80347.5);
        ticker.ask1 = Some(80347.4);
        let rate = parse_row(&row, None, None, Some(&ticker)).unwrap();
        assert_eq!((rate.best_bid, rate.best_ask), (None, None));
        assert_eq!(rate.period_rate, Decimal::new(1, 4));

        // 锁盘不是交叉盘：确实相等的买卖价必须保留，不能抹掉真实的零价差。
        ticker.bid1 = ticker.ask1;
        let rate = parse_row(&row, None, None, Some(&ticker)).unwrap();
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
        let btc = parse_row(&rows[0], None, Some(&detail), None).unwrap();
        assert_eq!((btc.interval_h, btc.interval_assumed), (8, false));
        // 同一家场所内 1h 合约与 8h 合约并存 —— 这正是不能用固定周期的原因
        let cvc = parse_row(&rows[1], None, None, None).unwrap();
        assert_eq!((cvc.interval_h, cvc.interval_assumed), (1, false));
    }

    #[test]
    fn a_missing_or_implausible_cycle_falls_back_and_is_flagged() {
        let mut row = funding_rows().remove(0);
        for cycle in [None, Some(0), Some(-1), Some(48), Some(i64::MAX)] {
            row.collect_cycle = cycle;
            let rate = parse_row(&row, None, None, None).unwrap();
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
        assert!(parse_row(&row, None, None, None).is_none());

        row.funding_rate = Some(f64::NAN);
        assert!(parse_row(&row, None, None, None).is_none());

        // 真值 0 必须保留 —— 它和「没给」是两回事
        row.funding_rate = Some(0.0);
        assert_eq!(
            parse_row(&row, None, None, None).unwrap().period_rate,
            Decimal::ZERO
        );
    }

    #[test]
    fn an_invalid_settlement_time_drops_the_row() {
        let mut row = funding_rows().remove(0);
        for time in [None, Some(0), Some(-1), Some(i64::MAX)] {
            row.next_settle_time = time;
            assert!(parse_row(&row, None, None, None).is_none(), "time={time:?}");
        }
    }

    /// 维持保证金率与最大杠杆来自合约详情：没有它们「自动加保证金」算不出强平距离，每一笔开仓都会被拒。
    #[test]
    fn maintenance_margin_and_max_leverage_come_from_the_detail() {
        let rows = funding_rows();
        let row = parse_row(&rows[0], None, Some(&detail()), None).unwrap();
        assert_eq!(row.maintenance_margin, Some(Decimal::new(4, 3)));
        assert_eq!(row.max_leverage, Some(Decimal::from(500)));
        let row = parse_row(&rows[0], None, None, None).unwrap();
        assert_eq!((row.maintenance_margin, row.max_leverage), (None, None));
    }

    /// 公开费率比 API 账户实际付的低（0.02% 对 0.08%）：按账户费率算；公开的更高才按公开的。
    #[test]
    fn the_taker_fee_is_what_the_api_account_actually_pays() {
        let rows = funding_rows();
        let mut detail = detail();
        let fee = |detail: Option<&_>| parse_row(&rows[0], None, detail, None).unwrap().taker_fee;
        assert_eq!(
            fee(Some(&detail)),
            Some(API_TAKER_FEE),
            "公开 0.0004 低于账户的 0.0008"
        );
        detail.taker_fee_rate = Some(0.0);
        assert_eq!(
            fee(Some(&detail)),
            Some(API_TAKER_FEE),
            "网页 0 费率不适用 API 账户"
        );
        detail.taker_fee_rate = Some(0.001);
        assert_eq!(
            fee(Some(&detail)),
            Some(Decimal::new(1, 3)),
            "公开的更高按公开的"
        );
        assert_eq!(fee(None), Some(API_TAKER_FEE), "没有详情也按账户费率");
    }

    #[test]
    fn open_interest_converts_contracts_to_usdt() {
        let rate = parse_row(
            &funding_rows().remove(0),
            None,
            Some(&detail()),
            Some(&ticker()),
        )
        .unwrap();
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
                None,
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

    // 2026-10-21 `/contract/detail` 的真实片段（字段截取）。
    const NAMING_FIXTURE: &str = r#"{"success":true,"code":0,"data":[
        {"symbol":"BTC_USDT","displayNameEn":"BTC_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.0001,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-mainly","mc-trade-zone-layer2","mc-trade-zone-pow"]},
        {"symbol":"XAU_USDT","displayNameEn":"GOLD(XAU)_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.001,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-metals","mc-trade-zone-tradfi","mc-trade-zone-metalsfutures","mc-trade-zone-Commodities"]},
        {"symbol":"SOXL_USDT","displayNameEn":"SOXL_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.01,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-Stock","mc-trade-zone-0fees","mc-trade-zone-tradfi","mc-trade-zone-ETF","mc-trade-zone-stockindex"]},
        {"symbol":"FILECOIN_USDT","displayNameEn":"FIL_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.01,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-layer2","mc-trade-zone-web3"]},
        {"symbol":"NVIDIA_USDT","displayNameEn":"NVDA_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.01,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-Stock","mc-trade-zone-0fees","mc-trade-zone-tradfi","mc-trade-zone-usstocks"]},
        {"symbol":"AAPLSTOCK_USDT","displayNameEn":"AAPL_USDT PERPETUAL","settleCoin":"USDT","state":0,"contractSize":0.01,"takerFeeRate":0.0004,"conceptPlate":["mc-trade-zone-Stock","mc-trade-zone-tradfi","mc-trade-zone-usstocks"]},
        {"symbol":"PAUSED_USDT","displayNameEn":"PAUSED_USDT PERPETUAL","settleCoin":"USDT","state":1,"contractSize":1,"takerFeeRate":0.0004},
        {"symbol":"BTC_USDC","displayNameEn":"BTC_USDC PERPETUAL","settleCoin":"USDC","state":0,"contractSize":0.0001,"takerFeeRate":0.0004}
    ]}"#;

    #[test]
    fn tradfi_contracts_take_the_ticker_from_the_display_name() {
        let all = contracts(&serde_json::from_str(NAMING_FIXTURE).unwrap()).unwrap();
        let by: HashMap<&str, &Contract> = all.iter().map(|c| (c.symbol.as_str(), c)).collect();
        assert_eq!(by["NVIDIA_USDT"].base, "NVDA");
        assert_eq!(by["AAPLSTOCK_USDT"].base, "AAPL");
        assert_eq!(by["SOXL_USDT"].base, "SOXL");
        // 显示名不是代码（GOLD(XAU)）：按合约名。
        assert_eq!(by["XAU_USDT"].base, "XAU");
        assert!(by["XAU_USDT"].tradfi && by["XAU_USDT"].commodity);
        // 加密币不改名（FILECOIN 显示 FIL 也不改：不动已有的资金费配对）。
        assert_eq!(by["FILECOIN_USDT"].base, "FILECOIN");
        assert!(!by["BTC_USDT"].tradfi);
        assert_eq!(by["NVIDIA_USDT"].contract_size, Decimal::new(1, 2));
        assert_eq!(
            by["NVIDIA_USDT"].taker_fee,
            Some(API_TAKER_FEE),
            "按账户实际费率"
        );
        // 不在交易、不是 USDT 结算：不列。
        assert!(!by.contains_key("PAUSED_USDT") && !by.contains_key("BTC_USDC"));
    }

    #[test]
    fn pre_ipo_and_pre_market_contracts_are_flagged() {
        // 2026-11-04 `/contract/detail` 的真实片段（字段截取）。
        let rows = serde_json::json!({"success": true, "code": 0, "data": [
            {"symbol": "OURASTOCK_USDT", "displayNameEn": "OURA_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.01, "takerFeeRate": 0.0004, "preMarket": false,
             "conceptPlate": ["mc-trade-zone-Stock", "mc-trade-zone-0fees", "mc-trade-zone-preipo", "mc-trade-zone-tradfi", "mc-trade-zone-usstocks"]},
            {"symbol": "XTEN_USDT", "displayNameEn": "XTEN_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 1, "takerFeeRate": 0.0004, "preMarket": true,
             "conceptPlate": ["mc-trade-zone-innovation", "mc-trade-zone-PreMarket"]},
            {"symbol": "NVIDIA_USDT", "displayNameEn": "NVDA_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.01, "takerFeeRate": 0.0004, "preMarket": false,
             "conceptPlate": ["mc-trade-zone-Stock", "mc-trade-zone-tradfi", "mc-trade-zone-usstocks"]}
        ]});
        let all = contracts(&rows).unwrap();
        let by: HashMap<&str, &Contract> = all.iter().map(|c| (c.base.as_str(), c)).collect();
        assert!(by["OURA"].pre_ipo, "上市前板块");
        assert!(by["XTEN"].pre_ipo, "盘前代币");
        assert!(!by["NVDA"].pre_ipo);
    }

    #[test]
    fn names_do_not_depend_on_which_contracts_are_trading() {
        // NVDA_USDT（加密币）和 NVIDIA_USDT（股票，显示 NVDA）撞名：NVIDIA 保留合约名。
        // 那个加密币停牌（state 1）也一样 —— 先筛再算的话，NVIDIA 会在停牌期间变成 NVDA。
        let rows = |state: i64| {
            serde_json::json!({"success": true, "code": 0, "data": [
                {"symbol": "NVDA_USDT", "displayNameEn": "NVDA_USDT PERPETUAL", "settleCoin": "USDT", "state": state, "contractSize": 1, "takerFeeRate": 0.0004},
                {"symbol": "NVIDIA_USDT", "displayNameEn": "NVDA_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.01, "takerFeeRate": 0.0004, "conceptPlate": ["mc-trade-zone-tradfi"]}
            ]})
        };
        for state in [0, 1] {
            let names = bases_of(&rows(state)).unwrap();
            assert_eq!(names["NVIDIA_USDT"], "NVIDIA", "state={state}");
            let listed = contracts(&rows(state)).unwrap();
            let nvidia = listed.iter().find(|c| c.symbol == "NVIDIA_USDT").unwrap();
            assert_eq!(nvidia.base, "NVIDIA", "state={state}");
        }
        assert_eq!(
            contracts(&rows(1)).unwrap().len(),
            1,
            "停牌的不列，但还参与命名"
        );
    }

    #[test]
    fn a_display_ticker_that_collides_keeps_the_contract_name() {
        // 有个加密币就叫 NVDA：NVIDIA 的股票合约不能也叫 NVDA。
        let bases = canonical_bases([
            ("NVDA_USDT", Some("NVDA_USDT PERPETUAL"), false),
            ("NVIDIA_USDT", Some("NVDA_USDT PERPETUAL"), true),
            ("AAPLSTOCK_USDT", Some("AAPL_USDT PERPETUAL"), true),
        ]);
        assert_eq!(bases["NVDA_USDT"], "NVDA");
        assert_eq!(bases["NVIDIA_USDT"], "NVIDIA");
        assert_eq!(bases["AAPLSTOCK_USDT"], "AAPL");
        // 两个股票合约显示同一个代码：都不改。
        let twins = canonical_bases([
            ("SKHYSTOCK_USDT", Some("SKHY_USDT PERPETUAL"), true),
            ("SKHY2STOCK_USDT", Some("SKHY_USDT PERPETUAL"), true),
        ]);
        assert_eq!(twins["SKHYSTOCK_USDT"], "SKHYSTOCK");
        assert_eq!(twins["SKHY2STOCK_USDT"], "SKHY2STOCK");
    }

    #[test]
    fn the_scan_reports_the_canonical_base_and_maps_it_back_to_the_contract() {
        let api = MexcApi::new(Client::new());
        let details: HashMap<String, Detail> =
            serde_json::from_str::<Envelope<Detail>>(NAMING_FIXTURE)
                .unwrap()
                .into_data("详情")
                .unwrap()
                .into_iter()
                .map(|row| (row.symbol.clone(), row))
                .collect();
        let bases = api.remember_bases(&details);
        let mut row = funding_rows().remove(0);
        row.symbol = "NVIDIA_USDT".into();
        let base = bases.get(&row.symbol).map(String::as_str);
        assert_eq!(
            parse_row(&row, base, None, None)
                .unwrap()
                .symbol
                .to_string(),
            "NVDA/USDT"
        );
        assert_eq!(api.native("NVDA"), "NVIDIA_USDT");
        assert_eq!(api.native("BTC"), "BTC_USDT");
        // 这一轮详情没取到：沿用上一轮，不退回 NVIDIA。
        assert_eq!(api.remember_bases(&HashMap::new())["NVIDIA_USDT"], "NVDA");
    }

    #[test]
    fn the_underscore_separator_is_stripped_from_the_base() {
        let rows = funding_rows();
        assert_eq!(
            parse_row(&rows[0], None, None, None)
                .unwrap()
                .symbol
                .to_string(),
            "BTC/USDT"
        );
        // 带数字前缀的倍数合约也要还原成完整 base，不能截断
        let mut row = rows[0].clone();
        row.symbol = "1000000BABYDOGE_USDT".into();
        assert_eq!(
            parse_row(&row, None, None, None)
                .unwrap()
                .symbol
                .to_string(),
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
        assert!(parse_row(&row, None, Some(&detail), None).is_some());
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
