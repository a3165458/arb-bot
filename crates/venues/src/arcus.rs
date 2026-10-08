//! Arcus（`arcus.xyz`）永续：Robinhood Chain 上的订单簿 DEX，USDG 保证金。
//!
//! 一个批量端点 `GET /v1/markets` 就给齐了排名要的全部字段（费率、结算时刻、标记价、
//! 预言机价、持仓量、保证金率），吃单费率另取 `GET /v1/feetiers`。
//!
//! # 必须踩准的口径（2026-09-29 实测主网）
//!
//! **1. 费率是每小时的，用 `nextFundingRate`。**
//!
//! 官方 Funding 文档：「Funding is charged once an hour」。实测 `/v1/fundingRates`
//! 相邻两条的 `time` 恰好差 3600 秒，BTC 基准费率 `0.0000125` = 0.01% / 8h ÷ 8。
//! `fundingRate` 是**上一次已结算**的费率，`nextFundingRate` 是下一次结算的预测值 ——
//! 与其它场所「当前这一期」的口径一致的是后者。实测两者在 HYPE、ZEC 上并不相等。
//!
//! **2. 标记价 `"0"` 表示「还没有」，不能回落到预言机价。**
//!
//! 官方字段说明原话：「"0" means no mark price has been received yet — callers must
//! not fall back to `oraclePrice`」。目前标记价等于预言机价，将来会换成 EWMA。
//! 预言机价是资产本身的价，填进 `index_price`，用于识别同名不同资产。
//!
//! **3. 股票、商品、指数永续有「盘后」制度。**
//!
//! `isOutsideRth == true` 时初始保证金率换成 `offHoursInitialMarginFraction`（实测 AMD
//! 从 0.1 升到 0.15），费率锁在 SOFR + 0.5%。最高杠杆取当前生效的那个；`isOutsideRth`
//! 为 `null`（服务端刚重启、还不知道日历）时按更严的盘后值算 —— 用宽松值会把强平距离
//! 算远。
//!
//! **4. 同名不同资产。** Arcus 的 `QNT`、`BE`、`BOT` 是**股票**，而别的场所同名的是
//! 加密币。这里照常按 `baseAsset` 出 Symbol，靠 `arb_scanner::identity` 的价格分簇拆开；
//! 不在连接器里改名。
//!
//! # 其它
//!
//! - 全部 67 个市场都以 USD 计价（`quoteAsset == "USD"`，USDG 保证金），与 Lighter RH
//!   同理计价资产写 USDT：两条腿之间没有换汇动作。
//! - `OFFLINE` 市场照样出现在列表里，官方说明「should not be quoted or traded」，跳过。
//! - 持仓量 `openInterest` 是**标的数量**，乘标记价才是美元名义；股票市场另有
//!   `openInterestCapNotional`（美元），名义触顶时开不了新仓，据此置 `oi_capped`。
//! - 盘口 `GET /v1/l2OrderBook/{显示名}`：路径参数只认 `BTC-USD` 这种显示名，用数字 id
//!   实测 404；每侧最多 100 档，档位是 `[价格, 标的数量]` 字符串。
//! - 限频按 IP 计权重（1,500/分钟）：`markets` 20、`feetiers` 2、盘口 2 ~ 7。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Decimal, FundingPoint, Level, MarketSnapshot, OrderBook, Symbol, Venue,
    parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const VENUE: Venue = Venue::Arcus;

/// 主网 REST 基址。实盘券商也用它。
pub const BASE_URL: &str = "https://api.arcus.xyz";

/// 计价资产。合约以美元计价（抵押是 USDG，两条腿之间没有换汇动作）。
const QUOTE: &str = "USDT";

/// 官方 Funding 文档：每小时结算一次。
const INTERVAL_H: u32 = 1;

/// 盘口端点每侧最多 100 档（官方：超过的请求被静默截到 100）。
const DEPTH_MAX_LEVELS: u32 = 100;

/// 费率档位表的单位：百万分之一。
const PPM: i64 = 1_000_000;

pub struct ArcusApi {
    client: Client,
    /// 盘口查询用的市场列表缓存（[`MARKETS_CACHE_TTL`]）。每拉一次盘口都重拉 `/v1/markets`
    /// 要花 20 的 IP 权重；后台预检与预览连着拉好几个盘口时，这是大头。
    markets_cache: std::sync::Mutex<Option<(std::time::Instant, Vec<ArcusMarket>)>>,
}

/// 盘口查询复用市场列表的时长。市场上下线、改 tick 不会快到这个量级。
const MARKETS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

impl ArcusApi {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            markets_cache: std::sync::Mutex::new(None),
        }
    }

    fn remember(&self, markets: &[ArcusMarket]) {
        if let Ok(mut cache) = self.markets_cache.lock() {
            *cache = Some((std::time::Instant::now(), markets.to_vec()));
        }
    }

    /// 盘口查询用的市场列表：缓存未过期就用缓存，否则重拉。
    async fn markets_for_depth(&self) -> ArbResult<Vec<ArcusMarket>> {
        if let Ok(cache) = self.markets_cache.lock()
            && let Some((at, markets)) = cache.as_ref()
            && at.elapsed() < MARKETS_CACHE_TTL
        {
            return Ok(markets.clone());
        }
        let (markets, _) = fetch_markets(&self.client).await?;
        self.remember(&markets);
        Ok(markets)
    }
}

#[derive(Debug, Deserialize)]
struct MarketsEnvelope {
    // 单行坏字段不能让其它市场一起消失；顶层结构错误仍由 get_json 报错。
    markets: Vec<Value>,
}

/// `GET /v1/markets` 里的一个市场。实盘券商也用它取步长、tick 与最高杠杆。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArcusMarket {
    pub market_id: u16,
    pub market_display_name: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    #[serde(rename = "type")]
    pub market_type: String,
    pub tick_size: String,
    pub step_size: String,
    #[serde(default)]
    pub tick_tiers: Vec<TickTier>,
    pub min_order_notional: String,
    pub min_order_size: String,
    pub max_order_size: String,
    pub oracle_price: String,
    pub mark_price: String,
    pub next_funding_rate: String,
    #[serde(default)]
    pub next_funding_at: Option<i64>,
    #[serde(default)]
    pub volume24h_notional: Option<String>,
    #[serde(default)]
    pub open_interest: Option<String>,
    #[serde(default)]
    pub open_interest_cap_notional: Option<String>,
    pub initial_margin_fraction: String,
    pub maintenance_margin_fraction: String,
    #[serde(default)]
    pub off_hours_initial_margin_fraction: Option<String>,
    #[serde(default)]
    pub is_outside_rth: Option<bool>,
}

/// 价格分段的 tick：`up_to_price` 以下（不含）用 `tick`，最后一段没有上限。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickTier {
    #[serde(default)]
    pub up_to_price: Option<String>,
    pub tick: String,
}

impl ArcusMarket {
    /// 能报价、能交易的永续：在线、美元计价、显示名就是 `{base}-USD`。
    ///
    /// 显示名的检查不是多余的：盘口端点按显示名寻址，实盘按 `marketId` 下单，
    /// 两者对不上时宁可丢掉这个市场，也不要把 A 的盘口配到 B 的订单上。
    pub fn is_tradable(&self) -> bool {
        self.status == "ONLINE"
            && self.market_type == "PERPETUAL"
            && self.quote_asset == "USD"
            && !self.base_asset.trim().is_empty()
            && self.market_display_name == format!("{}-USD", self.base_asset)
    }

    /// 本仓库的合约标识。扫描、下单、对账都走这一个函数 —— 对账按 `Symbol` 比较，
    /// 两处各拼一份，迟早一处多了个后缀，真实持仓就会被当成「台账之外的敞口」。
    pub fn symbol(&self) -> Symbol {
        arcus_symbol(&self.base_asset)
    }

    /// 当前生效的初始保证金率：盘后（或盘后状态未知）取更严的那个。
    pub fn effective_initial_margin(&self) -> Option<Decimal> {
        let regular = positive(&self.initial_margin_fraction)?;
        let off_hours = self
            .off_hours_initial_margin_fraction
            .as_deref()
            .and_then(positive);
        match (self.is_outside_rth, off_hours) {
            (Some(false), _) | (_, None) => Some(regular),
            (Some(true), Some(off)) => Some(off),
            (None, Some(off)) => Some(regular.max(off)),
        }
    }

    /// 最高杠杆 = 1 / 当前生效的初始保证金率。不取整：边界上取整会让被拒的杠杆看起来被允许。
    pub fn max_leverage(&self) -> Option<Decimal> {
        Decimal::ONE.checked_div(self.effective_initial_margin()?)
    }

    pub fn maintenance_margin(&self) -> Option<Decimal> {
        positive(&self.maintenance_margin_fraction).filter(|fraction| *fraction < Decimal::ONE)
    }

    /// 标记价。`"0"` = 还没有，**不**回落到预言机价（官方明确禁止）。
    pub fn mark(&self) -> Option<Decimal> {
        positive(&self.mark_price)
    }

    pub fn oracle(&self) -> Option<Decimal> {
        positive(&self.oracle_price)
    }
}

/// `baseAsset` → 本仓库的 [`Symbol`]。
pub fn arcus_symbol(base_asset: &str) -> Symbol {
    Symbol::perp(base_asset, QUOTE)
}

/// 拉全部市场并逐行解析。单行字段坏掉只丢那一行（返回的第二个值是丢掉的行数）。
pub async fn fetch_markets(client: &Client) -> ArbResult<(Vec<ArcusMarket>, usize)> {
    let envelope: MarketsEnvelope =
        get_json(client.get(format!("{BASE_URL}/v1/markets")), VENUE).await?;
    Ok(parse_markets(envelope.markets))
}

fn parse_markets(rows: Vec<Value>) -> (Vec<ArcusMarket>, usize) {
    let total = rows.len();
    let markets: Vec<ArcusMarket> = rows
        .into_iter()
        .filter_map(|row| serde_json::from_value(row).ok())
        .collect();
    let broken = total - markets.len();
    (markets, broken)
}

/// 基础档（新账户）的吃单费率。档位只会让费率更低，所以它是上限。
pub async fn fetch_base_taker_fee(client: &Client) -> ArbResult<Decimal> {
    let tiers: FeeTiers = get_json(client.get(format!("{BASE_URL}/v1/feetiers")), VENUE).await?;
    tiers.base_taker_fee()
}

#[derive(Debug, Deserialize)]
struct FeeTiers {
    tiers: Vec<FeeTier>,
}

#[derive(Debug, Deserialize)]
struct FeeTier {
    level: i64,
    taker_fee_ppm: i64,
}

impl FeeTiers {
    fn base_taker_fee(&self) -> ArbResult<Decimal> {
        let base = self
            .tiers
            .iter()
            .find(|tier| tier.level == 0)
            .ok_or_else(|| ArbError::venue(VENUE.as_str(), "费率档位表里没有基础档"))?;
        if !(0..PPM).contains(&base.taker_fee_ppm) {
            return Err(ArbError::venue(
                VENUE.as_str(),
                "基础档吃单费率超出合理范围",
            ));
        }
        Ok(Decimal::from(base.taker_fee_ppm) / Decimal::from(PPM))
    }
}

#[derive(Debug, Deserialize)]
struct DepthSnapshot {
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

#[async_trait]
impl VenueApi for ArcusApi {
    fn venue(&self) -> Venue {
        VENUE
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let (markets, broken) = fetch_markets(&self.client).await?;
        self.remember(&markets);
        // 拿不到费率表就如实报「不知道」，不拿一个猜测值冒充。
        let taker_fee = match fetch_base_taker_fee(&self.client).await {
            Ok(fee) => Some(fee),
            Err(error) => {
                warn!(venue = %VENUE, %error, "吃单费率取不到，按未知处理");
                None
            }
        };
        let now = Utc::now();
        let mut out = Vec::with_capacity(markets.len());
        let mut offline = 0usize;
        let mut unusable = broken;
        let mut seen: HashMap<Symbol, u16> = HashMap::new();
        for market in &markets {
            if !market.is_tradable() {
                offline += 1;
                continue;
            }
            // 同一个 Symbol 出现两次说明映射坏了，两行都不可信。
            if let Some(previous) = seen.insert(market.symbol(), market.market_id) {
                warn!(venue = %VENUE, symbol = %market.symbol(), previous, current = market.market_id, "同名市场重复，全部丢弃");
                out.retain(|snapshot: &MarketSnapshot| snapshot.symbol != market.symbol());
                unusable += 1;
                continue;
            }
            match parse_market(market, taker_fee, now) {
                Some(snapshot) => out.push(snapshot),
                None => unusable += 1,
            }
        }
        if offline > 0 {
            tracing::debug!(venue = %VENUE, offline, "未上线或非美元永续的市场已跳过");
        }
        if unusable > 0 {
            warn!(venue = %VENUE, unusable, "字段不可用的市场已跳过");
        }
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        // 不静默截断：上游超过 100 会悄悄截掉，调用方会以为自己拿到了足够的档数。
        if !(1..=DEPTH_MAX_LEVELS).contains(&levels) {
            return Err(ArbError::config("Arcus 盘口档数必须在 1..=100"));
        }
        let markets = self.markets_for_depth().await?;
        let market = markets
            .iter()
            .find(|market| market.is_tradable() && market.symbol() == *symbol)
            .ok_or_else(|| {
                ArbError::venue(VENUE.as_str(), format!("没有 {symbol} 对应的在线永续"))
            })?;
        let depth: DepthSnapshot = get_json(
            self.client
                .get(format!(
                    "{BASE_URL}/v1/l2OrderBook/{}",
                    market.market_display_name
                ))
                .query(&[("nLevels", levels)]),
            VENUE,
        )
        .await?;
        depth_book(depth, symbol)
    }

    fn supports_funding_history(&self) -> bool {
        true
    }

    /// `GET /v1/fundingRates`（公开）：最新在前，`from` 是微秒，费率是**小时**费率。
    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        let markets = self.markets_for_depth().await?;
        let market = markets
            .iter()
            .find(|market| market.is_tradable() && market.symbol() == *symbol)
            .ok_or_else(|| {
                ArbError::venue(VENUE.as_str(), format!("没有 {symbol} 对应的在线永续"))
            })?;
        let from = (Utc::now() - chrono::Duration::hours(i64::from(hours))).timestamp_micros();
        let page: FundingRatesPage = get_json(
            self.client
                .get(format!("{BASE_URL}/v1/fundingRates"))
                .query(&[
                    ("market", market.market_display_name.clone()),
                    ("from", from.to_string()),
                    ("limit", "1000".to_string()),
                ]),
            VENUE,
        )
        .await?;
        Ok(parse_funding_rates(page))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRatesPage {
    #[serde(default)]
    funding_rates: Vec<FundingRateRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateRow {
    funding_rate: String,
    /// 微秒。
    time: i64,
}

fn parse_funding_rates(page: FundingRatesPage) -> Vec<FundingPoint> {
    let mut points: Vec<FundingPoint> = page
        .funding_rates
        .into_iter()
        .filter_map(|row| {
            Some(FundingPoint {
                at: DateTime::from_timestamp_micros(row.time)?,
                rate: parse_decimal(&row.funding_rate)?,
            })
        })
        .collect();
    points.sort_by_key(|point| point.at);
    points
}

fn parse_market(
    market: &ArcusMarket,
    taker_fee: Option<Decimal>,
    now: DateTime<Utc>,
) -> Option<MarketSnapshot> {
    let period_rate = parse_decimal(&market.next_funding_rate)?;
    let (next_funding_at, next_funding_estimated) = match market
        .next_funding_at
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
    {
        Some(at) => (at, false),
        // 接口没给时刻才按「下一个 UTC 整点」推算，并如实标记为推算。
        None => (next_hour_boundary(now)?, true),
    };
    let mark_price = market.mark();

    // 持仓量是标的数量，没有标记价就报未知，不能把币数量当成美元。
    let open_interest_usdt = market
        .open_interest
        .as_deref()
        .and_then(parse_decimal)
        .filter(|coins| *coins >= Decimal::ZERO)
        .zip(mark_price)
        .and_then(|(coins, price)| coins.checked_mul(price));
    let oi_capped = match (
        open_interest_usdt,
        market
            .open_interest_cap_notional
            .as_deref()
            .and_then(positive),
    ) {
        (Some(notional), Some(cap)) => notional >= cap,
        _ => false,
    };

    Some(MarketSnapshot {
        venue: VENUE,
        symbol: market.symbol(),
        period_rate,
        interval_h: INTERVAL_H,
        // 1 小时周期来自官方文档（并用 `/v1/fundingRates` 的时间间隔核实过），
        // 不是这个响应里的字段 —— 与 Lighter 同样如实标记。
        interval_assumed: true,
        next_funding_at,
        next_funding_estimated,
        taker_fee,
        mark_price,
        index_price: market.oracle(),
        // 批量端点不给盘口；排名里的穿价留空，由深度体检按需补。
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt,
        quote_volume_24h: market.volume24h_notional.as_deref().and_then(parse_decimal),
        max_leverage: market.max_leverage(),
        maintenance_margin: market.maintenance_margin(),
        oi_capped,
    })
}

fn depth_book(depth: DepthSnapshot, symbol: &Symbol) -> ArbResult<OrderBook> {
    let bids = depth_levels(depth.bids, true)?;
    let asks = depth_levels(depth.asks, false)?;
    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
        return Err(ArbError::venue(VENUE.as_str(), "盘口一侧为空"));
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

/// `[价格, 标的数量]` → 计价币名义。上游已经按价位聚合，这里只排序、不合并。
fn depth_levels(rows: Vec<[String; 2]>, descending: bool) -> ArbResult<Vec<Level>> {
    let mut levels = Vec::with_capacity(rows.len());
    for [price, size] in rows {
        let (Some(price), Some(size)) = (parse_decimal(&price), parse_decimal(&size)) else {
            return Err(ArbError::venue(VENUE.as_str(), "盘口档位无法解析"));
        };
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            continue;
        }
        let notional_usdt = price
            .checked_mul(size)
            .ok_or_else(|| ArbError::venue(VENUE.as_str(), "盘口名义额溢出"))?;
        levels.push(Level {
            price,
            notional_usdt,
        });
    }
    levels.sort_unstable_by(|a, b| {
        if descending {
            b.price.cmp(&a.price)
        } else {
            a.price.cmp(&b.price)
        }
    });
    Ok(levels)
}

fn positive(raw: &str) -> Option<Decimal> {
    parse_decimal(raw).filter(|value| *value > Decimal::ZERO)
}

fn next_hour_boundary(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let next = (now.timestamp().div_euclid(3600) + 1) * 3600;
    DateTime::from_timestamp(next, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-09-29 GET https://api.arcus.xyz/v1/markets 的原样片段（只删了无关字段，
    // 非 BTC 市场的 tickTiers 只留第一段）：BTC 加密、HYPE 负费率、AMD 盘后股票
    // （带持仓上限）、F 下线。
    const MARKETS_FIXTURE: &str = r#"{"markets":[{"marketDisplayName":"BTC-USD","marketId":1,"status":"ONLINE","baseAsset":"BTC","quoteAsset":"USD","tickSize":"0.1","stepSize":"0.00000001","tickTiers":[{"upToPrice":"500000","tick":"0.1"},{"upToPrice":"1000000","tick":"0.2"},{"upToPrice":"2000000","tick":"0.5"},{"upToPrice":"5000000","tick":"1"},{"upToPrice":"10000000","tick":"2"},{"tick":"5"}],"minOrderNotional":"5","minOrderSize":"0.0001","maxOrderSize":"10000","oraclePrice":"83390.5","markPrice":"83401.9","fundingRate":"0.0000125","nextFundingRate":"0.0000125","nextFundingAt":1790661600,"volume24hNotional":"297244489.39","openInterest":"98.37559655","openInterestCapNotional":null,"initialMarginFraction":"0.025","maintenanceMarginFraction":"0.016667","offHoursInitialMarginFraction":"0.025","isOutsideRth":false,"type":"PERPETUAL","category":"CRYPTO"},{"marketDisplayName":"HYPE-USD","marketId":6,"status":"ONLINE","baseAsset":"HYPE","quoteAsset":"USD","tickSize":"0.001","stepSize":"0.000001","tickTiers":[{"upToPrice":"500","tick":"0.001"}],"minOrderNotional":"5","minOrderSize":"0.1","maxOrderSize":"1000000","oraclePrice":"87.539","markPrice":"87.49","fundingRate":"-0.000021155252169118","nextFundingRate":"-0.00001614859241078845","nextFundingAt":1790661600,"volume24hNotional":"5617369.94","openInterest":"9590.986481","openInterestCapNotional":null,"initialMarginFraction":"0.1","maintenanceMarginFraction":"0.066667","offHoursInitialMarginFraction":"0.1","isOutsideRth":false,"type":"PERPETUAL","category":"CRYPTO"},{"marketDisplayName":"AMD-USD","marketId":9,"status":"ONLINE","baseAsset":"AMD","quoteAsset":"USD","tickSize":"0.01","stepSize":"0.0000001","tickTiers":[{"upToPrice":"5000","tick":"0.01"}],"minOrderNotional":"5","minOrderSize":"0.01","maxOrderSize":"100000","oraclePrice":"604.55","markPrice":"604.93","fundingRate":"0.000005092592592592","nextFundingRate":"0.00000474537037037","nextFundingAt":1790661600,"volume24hNotional":"838473.31","openInterest":"259.403643","openInterestCapNotional":"250000","initialMarginFraction":"0.1","maintenanceMarginFraction":"0.066667","offHoursInitialMarginFraction":"0.15","isOutsideRth":true,"type":"PERPETUAL","category":"EQUITIES"},{"marketDisplayName":"F-USD","marketId":11,"status":"OFFLINE","baseAsset":"F","quoteAsset":"USD","tickSize":"0.01","stepSize":"0.0000001","tickTiers":[{"upToPrice":"5000","tick":"0.01"}],"minOrderNotional":"5","minOrderSize":"1","maxOrderSize":"10000000","oraclePrice":"0","markPrice":"0","fundingRate":"0","nextFundingRate":"0","nextFundingAt":1790661600,"volume24hNotional":"0","openInterest":"0","openInterestCapNotional":null,"initialMarginFraction":"0.2","maintenanceMarginFraction":"0.133334","offHoursInitialMarginFraction":"0.3","isOutsideRth":null,"type":"PERPETUAL","category":"EQUITIES"}]}"#;

    // 2026-09-29 GET /v1/l2OrderBook/BTC-USD?nLevels=3 的原样响应。
    const DEPTH_FIXTURE: &str = r#"{"bids":[["83377.5","1.22718576"],["83377.1","0.01293731"],["83377","0.0002"]],"asks":[["83377.6","0.29589854"],["83377.7","0.15976721"],["83377.8","0.00134933"]],"lastSequenceId":246647656,"globalSequenceId":2676675390,"timestamp":1790661570610619}"#;

    // 2026-09-29 GET /v1/feetiers 的前两档。
    const FEES_FIXTURE: &str = r#"{"tiers":[{"level":0,"name":"Base","volume_threshold":0,"maker_fee_ppm":0,"taker_fee_ppm":225},{"level":1,"name":"Bronze","volume_threshold":5000000000000000,"maker_fee_ppm":0,"taker_fee_ppm":190}]}"#;

    fn markets() -> Vec<ArcusMarket> {
        let envelope: MarketsEnvelope = serde_json::from_str(MARKETS_FIXTURE).unwrap();
        let (markets, broken) = parse_markets(envelope.markets);
        assert_eq!(broken, 0);
        markets
    }

    fn market(base: &str) -> ArcusMarket {
        markets()
            .into_iter()
            .find(|market| market.base_asset == base)
            .unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_790_661_000, 0).unwrap()
    }

    #[test]
    fn funding_rates_parse_microsecond_times_and_hourly_rates() {
        // 2026-09-30 实测 `GET /v1/fundingRates`：最新在前。
        let page: FundingRatesPage = serde_json::from_str(
            r#"{"fundingRates":[
                {"marketId":68,"marketDisplayName":"PONS-USD","fundingRate":"0.000122199487","time":1790769600000000},
                {"marketId":68,"marketDisplayName":"PONS-USD","fundingRate":"0.000159875159","time":1790766000000000}]}"#,
        )
        .unwrap();
        let points = parse_funding_rates(page);
        assert_eq!(points.len(), 2);
        assert!(points[0].at < points[1].at, "升序");
        assert_eq!(points[0].rate.to_string(), "0.000159875159");
    }

    #[test]
    fn hourly_rate_comes_from_the_next_funding_forecast() {
        let fee = Some(Decimal::new(225, 6));
        let btc = parse_market(&market("BTC"), fee, now()).unwrap();
        assert_eq!(btc.symbol, Symbol::perp("BTC", "USDT"));
        assert_eq!(btc.period_rate, Decimal::new(125, 7));
        assert_eq!(btc.interval_h, 1);
        // 0.0000125 × 24 × 365 = 10.95% APR，不能是 8 倍。
        assert_eq!(
            btc.period_rate * Decimal::from(24 * 365),
            Decimal::new(1095, 4)
        );
        assert_eq!(btc.next_funding_at.timestamp(), 1_790_661_600);
        assert!(!btc.next_funding_estimated);
        assert_eq!(btc.taker_fee, Some(Decimal::new(225, 6)));
        assert_eq!(btc.mark_price, Some(Decimal::new(834019, 1)));
        assert_eq!(btc.index_price, Some(Decimal::new(833905, 1)));
        assert_eq!(btc.max_leverage, Some(Decimal::from(40)));
        assert_eq!(btc.maintenance_margin, Some(Decimal::new(16667, 6)));
        assert!(!btc.oi_capped);

        // 已结算的是 -0.0000211…，下一期预测是 -0.0000161… —— 要后者。
        let hype = parse_market(&market("HYPE"), fee, now()).unwrap();
        assert_eq!(
            hype.period_rate,
            "-0.00001614859241078845".parse::<Decimal>().unwrap()
        );
    }

    #[test]
    fn off_hours_margin_is_used_while_the_underlying_is_closed() {
        let amd = market("AMD");
        assert_eq!(amd.effective_initial_margin(), Some(Decimal::new(15, 2)));
        let snapshot = parse_market(&amd, None, now()).unwrap();
        // 1 / 0.15 = 6.67 倍，而不是盘中的 10 倍。
        assert!(snapshot.max_leverage.unwrap() < Decimal::from(7));
        assert_eq!(snapshot.taker_fee, None);

        let mut unknown = amd.clone();
        unknown.is_outside_rth = None;
        assert_eq!(
            unknown.effective_initial_margin(),
            Some(Decimal::new(15, 2))
        );
        let mut open = amd;
        open.is_outside_rth = Some(false);
        assert_eq!(open.effective_initial_margin(), Some(Decimal::new(1, 1)));
    }

    #[test]
    fn open_interest_cap_is_compared_in_dollars() {
        let amd = market("AMD");
        let snapshot = parse_market(&amd, None, now()).unwrap();
        // 259.403643 × 604.93 ≈ 156,921 美元 < 250,000 上限。
        assert!(snapshot.open_interest_usdt.unwrap() > Decimal::from(156_000));
        assert!(!snapshot.oi_capped);
        let mut full = amd;
        full.open_interest = Some("500".into());
        assert!(parse_market(&full, None, now()).unwrap().oi_capped);
    }

    #[test]
    fn offline_or_mismatched_markets_are_not_tradable() {
        assert!(!market("F").is_tradable());
        assert!(market("BTC").is_tradable());
        let mut renamed = market("BTC");
        renamed.market_display_name = "XBT-USD".into();
        assert!(!renamed.is_tradable());
        let mut quoted = market("BTC");
        quoted.quote_asset = "USDC".into();
        assert!(!quoted.is_tradable());
    }

    #[test]
    fn a_zero_mark_price_is_unknown_not_the_oracle() {
        let mut btc = market("BTC");
        btc.mark_price = "0".into();
        let snapshot = parse_market(&btc, None, now()).unwrap();
        assert_eq!(snapshot.mark_price, None);
        assert_eq!(snapshot.open_interest_usdt, None);
        assert!(snapshot.index_price.is_some());
    }

    #[test]
    fn a_broken_row_only_drops_itself() {
        let mut rows: Vec<Value> = serde_json::from_str::<MarketsEnvelope>(MARKETS_FIXTURE)
            .unwrap()
            .markets;
        rows[1]["marketId"] = Value::String("six".into());
        let (markets, broken) = parse_markets(rows);
        assert_eq!(broken, 1);
        assert_eq!(markets.len(), 3);
    }

    #[test]
    fn base_tier_fee_is_parts_per_million() {
        let tiers: FeeTiers = serde_json::from_str(FEES_FIXTURE).unwrap();
        assert_eq!(tiers.base_taker_fee().unwrap(), Decimal::new(225, 6));
    }

    #[test]
    fn depth_levels_are_base_quantities_times_price() {
        let depth: DepthSnapshot = serde_json::from_str(DEPTH_FIXTURE).unwrap();
        let symbol = arcus_symbol("BTC");
        let book = depth_book(depth, &symbol).unwrap();
        assert_eq!(book.bids.len(), 3);
        assert_eq!(book.bids[0].price, Decimal::new(833775, 1));
        // 83377.5 × 1.22718576 = 102319.66…
        assert_eq!(
            book.bids[0].notional_usdt,
            Decimal::new(833775, 1) * Decimal::new(122_718_576, 8)
        );
        assert!(book.bids[0].price > book.bids[2].price);
        assert!(book.asks[0].price < book.asks[2].price);
        assert!(book.asks[0].price >= book.bids[0].price);
    }

    #[test]
    fn crossed_or_one_sided_books_are_errors() {
        let crossed: DepthSnapshot =
            serde_json::from_str(r#"{"bids":[["101","1"]],"asks":[["100","1"]]}"#).unwrap();
        assert!(depth_book(crossed, &arcus_symbol("BTC")).is_err());
        let empty: DepthSnapshot =
            serde_json::from_str(r#"{"bids":[],"asks":[["100","1"]]}"#).unwrap();
        assert!(depth_book(empty, &arcus_symbol("BTC")).is_err());
    }
}
