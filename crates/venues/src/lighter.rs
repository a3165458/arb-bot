//! Lighter（zk DEX）永续。
//!
//! 两个端点按 `market_id` 联接：
//!
//! | 端点 | 提供 | 不提供 |
//! | --- | --- | --- |
//! | `/api/v1/funding-rates` | 各场所的当前费率 | 吃单费率、价格 |
//! | `/api/v1/orderBookDetails` | 吃单费率、标记价、指数价、持仓量、成交额 | **费率** |
//!
//! # 两个必须踩准的口径
//!
//! **1. `/funding-rates` 是「8 小时基准」的横向对照表，不是本场所的每期费率。**
//!
//! 实测（2026-09-19，同一分钟）：Binance ETH 原始 8h 费率 `0.00005865`，表里
//! `binance/ETH` 也是 `0.00005865`；Bybit 原始 `0.0001`，表里也是 `0.0001`；
//! 而 Hyperliquid 的**每小时**原始费率是 `0.0000125`，表里 `hyperliquid/ETH` 却是
//! `0.0001` = 8 倍。也就是说这张表把各家的费率统一折算到 **8 小时**。
//!
//! Lighter 自己的资金费是**每小时**结算的（官方 Funding 文档："Funding payments
//! occur at each hour mark"），所以它的每期费率 = 表里数值 ÷ 8。直接把表里的数当
//! 每期费率会让 Lighter 的日化虚高 **8 倍**并霸榜 —— 而它看起来完全正常。
//!
//! **2. `exchange` 字段决定这一行是谁的费率。**
//!
//! 同一张表里混着 binance / bybit / hyperliquid / lighter 四家。不按
//! `exchange == "lighter"` 过滤，就会把别的场所的费率当成 Lighter 的。
//!
//! # 其它
//!
//! 吃单费率实测全部是字符串 `"0.0000"` —— 这是**真值 0**，不是缺省。
//! 抵押资产是 USDC，但合约以美元计价（指数价即 USD 现货价），所以计价资产写 USDT：
//! 两条腿之间没有换汇动作，写 USDC 只会让这个场所配不上任何 USDT 场所。
//!
//! # K 线（`/api/v1/candles`）：五个参数缺一不可，周期是枚举字符串
//!
//! 实测（2026-09-20）：`market_id`、`resolution`、`start_timestamp`、`end_timestamp`、
//! `count_back` **五个参数全都要给** —— 少给任何一个都返回
//! `{"code":20001,"message":"invalid param "}`（HTTP 400），多给一个（`limit`、
//! `interval`）也是同一个错。所以不能按「可选参数」去猜，也不存在 `limit` 这个参数名。
//! 两个时间戳是**秒**，且 `end_timestamp` 必须严格大于 `start_timestamp`
//! （相等返回 `{"code":22400,...}`）。
//!
//! `count_back` 是「至少往回数这么多根」，不是上限：实测窗口 100 小时 + `count_back=5`
//! 回 100 根，`count_back=500` 则越过 `start_timestamp` 往前补到 500 根。所以「取最近
//! N 根」要让窗口本身就正好覆盖 N 根，同时把 `count_back` 也设成 N。
//!
//! `resolution` 只认 [`CANDLE_RESOLUTIONS`] 里那八个字符串。请求不支持的周期时
//! **向上取整**到最近的合法值：返回比请求更细的序列会把基差半衰期算得更短，而结果
//! 看起来完全正常、不会报错。比 `1d` 还粗的请求没有合法值可退，直接报错 ——
//! 偷偷返回 1d 同样违反「不小于」。
//!
//! 每根 K 线的 `t` 是**毫秒**的**开盘**时刻（实测 1789905600000 = 2026-09-20T12:00:00Z，
//! 而请求时刻是 12:33，正在走的这根也在里面），必须真的解析成时刻，不能按数组下标
//! 乘周期推算 —— 缺 K 线时那个间隔正是我们要保留的信息。收盘价是每根里的 `c`，
//! 而外层数组键**也叫 `c`**：少解析一层就会把整个数组当成收盘价。
//!
//! 实测返回是升序，但「升序」是契约的硬要求（顺序反了半衰期会算成正相关），
//! 所以照样显式排序，不依赖上游方向。单次最多约 500 根（1h/5m 恰好 500，1m 只往回
//! 给约 8 小时），更大的 `limit` 截到 500：少几根只是历史更短，时间尺度不失真。
//!
//! # 两个部署
//!
//! 同一套 API 跑在两个独立实例上，账户与保证金互不相通，所以记为两家场所：
//!
//! | 场所 | 基址 | 抵押 | 市场 |
//! | --- | --- | --- | --- |
//! | `lighter` | `mainnet.zklighter.elliot.ai` | USDC | 加密为主，二百余个永续 |
//! | `lighter-rh` | `api.rh.lighter.xyz`（Robinhood Chain） | USDG | 美股、ETF、商品、Pre-IPO，五十余个永续 |
//!
//! 2026-09-23 实测 RH 的 `/funding-rates` 与主网同口径：也是 8 小时基准（同一分钟
//! Hyperliquid 每小时原始费率 `0.0000125`，表里 `hyperliquid/BTC` 是 `0.0001`），
//! 也混着 binance / bybit / hyperliquid 的对照行，`exchange == "lighter"` 的 57 行与
//! `orderBookDetails` 按 `market_id` 57/57 对齐。所以两个部署共用同一套解析，只换基址。
//! RH 的限频比主网紧：连续几次请求就会回 `{"code":23000,"message":"Too Many Requests!"}`，
//! 那是 `code != 200` 的错误，不能当成一张空表。
//!
//! # 保证金
//!
//! `orderBookDetails` 的保证金率是**万分之一**为单位的整数：`min_initial_margin_fraction`
//! 是最高杠杆对应的初始保证金率（主网 BTC `200` = 2% = 50 倍），
//! `maintenance_margin_fraction` 是维持保证金率（BTC `120` = 1.2%）。
//! `default_initial_margin_fraction` 是新账户的默认杠杆，不是上限，不能拿来算最大杠杆。

use std::collections::HashMap;

use arb_core::{
    ArbError, ArbResult, Decimal, FundingPoint, Level, MarketSnapshot, OrderBook, Symbol, Venue,
    from_json_f64, parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

use crate::connector::VenueApi;
use crate::http::get_json;

/// 一个 Lighter 部署：场所身份 + API 基址。
#[derive(Debug, Clone, Copy)]
struct Deployment {
    venue: Venue,
    base_url: &'static str,
}

const MAINNET: Deployment = Deployment {
    venue: Venue::Lighter,
    base_url: "https://mainnet.zklighter.elliot.ai",
};

const ROBINHOOD: Deployment = Deployment {
    venue: Venue::LighterRh,
    base_url: "https://api.rh.lighter.xyz",
};

impl Deployment {
    fn url(self, path: &str) -> String {
        format!("{}/api/v1/{path}", self.base_url)
    }
}

/// 保证金率字段的单位：万分之一。
const MARGIN_FRACTION_SCALE: i64 = 10_000;

/// 深度端点的 `limit` 上限：官方 OpenAPI 写的是 `minimum: 1, maximum: 250`，
/// 实测 250 通过、256 返回 `{"code":20001,"message":"invalid param "}`。
/// `limit` 是**必填**参数（不带就报同样的错）。
const DEPTH_MAX_ORDERS: u32 = 250;

/// 计价资产。合约以美元计价（抵押资产是 USDC，但两条腿之间没有换汇动作）。
const QUOTE: &str = "USDT";

/// 该场所真实的结算周期：官方 Funding 文档明确「每小时整点结算」。
const INTERVAL_H: u32 = 1;

/// `/funding-rates` 的基准周期。表里的值是 8 小时口径，本场所 1 小时结算，
/// 所以每期费率 = 表里数值 ÷ (8 / 1)。
const LIST_BASIS_HOURS: u32 = 8;

pub struct LighterApi {
    client: Client,
    deployment: Deployment,
    /// 盘口查询用的市场详情缓存。每拉一次盘口都重拉 `orderBookDetails` 等于请求数翻倍，
    /// 而 RH 部署按出口 IP 限频、额度很紧。
    details_cache: std::sync::Mutex<Option<(std::time::Instant, BookDetails)>>,
}

/// 盘口查询复用市场详情的时长。market_id 与上下线状态不会快到这个量级。
const DETAILS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

impl LighterApi {
    /// 主网部署（`lighter`）。
    pub fn mainnet(client: Client) -> Self {
        Self {
            client,
            deployment: MAINNET,
            details_cache: std::sync::Mutex::new(None),
        }
    }

    /// Robinhood Chain 部署（`lighter-rh`）。
    pub fn robinhood(client: Client) -> Self {
        Self {
            client,
            deployment: ROBINHOOD,
            details_cache: std::sync::Mutex::new(None),
        }
    }

    fn remember(&self, details: &BookDetails) {
        if let Ok(mut cache) = self.details_cache.lock() {
            *cache = Some((std::time::Instant::now(), details.clone()));
        }
    }

    /// 盘口查询用的市场详情：缓存未过期就用缓存，否则重拉。
    async fn details_for_depth(&self) -> ArbResult<BookDetails> {
        if let Ok(cache) = self.details_cache.lock()
            && let Some((at, details)) = cache.as_ref()
            && at.elapsed() < DETAILS_CACHE_TTL
        {
            return Ok(details.clone());
        }
        let details: BookDetails = get_json(
            self.client.get(self.deployment.url("orderBookDetails")),
            self.venue_id(),
        )
        .await?;
        self.remember(&details);
        Ok(details)
    }

    fn venue_id(&self) -> Venue {
        self.deployment.venue
    }
}

/// 资金费响应外壳：`{"code":200,"funding_rates":[...]}`。
///
/// `code` 是 **number**（不是字符串），字段名也不是通用的 `data` ——
/// 按 `data` 去解析会得到一张空表，场所就这样静默消失。
#[derive(Debug, Deserialize)]
struct FundingEnvelope {
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    funding_rates: Option<Vec<FundingRow>>,
}

impl FundingEnvelope {
    fn into_rows(self, venue: Venue) -> ArbResult<Vec<FundingRow>> {
        match self.code {
            Some(200) | None => Ok(self.funding_rates.unwrap_or_default()),
            Some(code) => Err(ArbError::venue(
                venue.as_str(),
                format!("资金费 code={code}"),
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
struct FundingRow {
    market_id: i64,
    exchange: String,
    symbol: String,
    /// JSON number（实测带 0.00009599999999999999 这类浮点噪声）。
    rate: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct BookDetails {
    #[serde(default)]
    order_book_details: Option<Vec<BookRow>>,
}

#[derive(Debug, Clone, Deserialize)]
struct BookRow {
    market_id: i64,
    symbol: String,
    market_type: Option<String>,
    status: Option<String>,
    /// 字符串 `"0.0000"`：真值 0。
    taker_fee: Option<String>,
    mark_price: Option<String>,
    index_price: Option<String>,
    /// **标的币数量**，不是 USDT 名义。
    open_interest: Option<f64>,
    daily_quote_token_volume: Option<f64>,
    /// 最高杠杆对应的初始保证金率，万分之一。
    #[serde(default)]
    min_initial_margin_fraction: Option<i64>,
    /// 维持保证金率，万分之一。
    #[serde(default)]
    maintenance_margin_fraction: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct DepthEnvelope {
    code: i64,
    #[serde(default)]
    bids: Vec<DepthOrder>,
    #[serde(default)]
    asks: Vec<DepthOrder>,
}

#[derive(Debug, Deserialize)]
struct DepthOrder {
    price: String,
    // 只能使用剩余量；initial_base_amount 会把已经成交的部分也算进可吃深度。
    remaining_base_amount: String,
}

impl DepthEnvelope {
    fn into_book(self, venue: Venue, symbol: &Symbol) -> ArbResult<OrderBook> {
        if self.code != 200 {
            return Err(ArbError::venue(
                venue.as_str(),
                format!("盘口深度 code={}", self.code),
            ));
        }
        let bids = depth_levels(venue, self.bids, true)?;
        let asks = depth_levels(venue, self.asks, false)?;
        let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
            return Err(ArbError::venue(venue.as_str(), "盘口一侧为空或无有效挂单"));
        };
        if ask.price < bid.price {
            return Err(ArbError::venue(venue.as_str(), "盘口交叉"));
        }
        Ok(OrderBook {
            venue,
            symbol: symbol.clone(),
            bids,
            asks,
        })
    }
}

fn depth_market_id(venue: Venue, details: &BookDetails, symbol: &Symbol) -> ArbResult<i64> {
    details
        .order_book_details
        .iter()
        .flatten()
        .find(|row| is_active_perp(row) && Symbol::perp(&row.symbol, QUOTE) == *symbol)
        .map(|row| row.market_id)
        .ok_or_else(|| {
            ArbError::venue(
                venue.as_str(),
                format!("没有 {}/{} 对应的在交易永续", symbol.base, symbol.quote),
            )
        })
}

fn depth_levels(venue: Venue, orders: Vec<DepthOrder>, descending: bool) -> ArbResult<Vec<Level>> {
    let mut levels: Vec<Level> = orders
        .into_iter()
        .filter_map(|order| {
            let price = parse_decimal(&order.price)?;
            let base_amount = parse_decimal(&order.remaining_base_amount)?;
            if price <= Decimal::ZERO || base_amount <= Decimal::ZERO {
                return None;
            }
            // REST 已返回标的币数量，不是整数编码或张数；例如 ETH 的 6.4693
            // 乘 2579.82 = 16689.629526 美元，不再乘 size_decimals 或合约面值。
            let notional_usdt = base_amount.checked_mul(price)?;
            (notional_usdt > Decimal::ZERO).then_some(Level {
                price,
                notional_usdt,
            })
        })
        .collect();
    levels.sort_unstable_by(|a, b| {
        if descending {
            b.price.cmp(&a.price)
        } else {
            a.price.cmp(&b.price)
        }
    });
    // 接口给的是逐笔挂单；原地合并同价，避免把一个价位的多笔订单当成多个档位。
    let mut len = 0;
    for index in 0..levels.len() {
        if len > 0 && levels[len - 1].price == levels[index].price {
            levels[len - 1].notional_usdt = levels[len - 1]
                .notional_usdt
                .checked_add(levels[index].notional_usdt)
                .ok_or_else(|| ArbError::venue(venue.as_str(), "同价挂单名义额溢出"))?;
        } else {
            levels.swap(len, index);
            len += 1;
        }
    }
    levels.truncate(len);
    Ok(levels)
}

#[async_trait]
impl VenueApi for LighterApi {
    fn venue(&self) -> Venue {
        self.venue_id()
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let venue = self.venue_id();
        let details: BookDetails = get_json(
            self.client.get(self.deployment.url("orderBookDetails")),
            venue,
        )
        .await?;
        self.remember(&details);
        let books: HashMap<i64, BookRow> = details
            .order_book_details
            .unwrap_or_default()
            .into_iter()
            .filter(is_active_perp)
            .map(|row| (row.market_id, row))
            .collect();

        let funding: Vec<FundingRow> = get_json::<FundingEnvelope>(
            self.client.get(self.deployment.url("funding-rates")),
            venue,
        )
        .await?
        .into_rows(venue)?;

        let mut out = Vec::with_capacity(funding.len());
        let mut filtered = 0usize;
        let mut unusable = 0usize;
        for row in funding {
            // 同一张表里混着四家场所，不过滤就会把别人的费率算成本场所的。
            if !row.exchange.eq_ignore_ascii_case("lighter") {
                filtered += 1;
                continue;
            }
            let Some(book) = books.get(&row.market_id) else {
                // 表里有、详情里没有（未上市或已下架）→ 没有价格与费率口径，丢行。
                unusable += 1;
                continue;
            };
            // 联接键是 market_id，但两边的 symbol 也必须一致：不一致说明联接断了，
            // 宁可丢这一行，也不要把 A 的价格配到 B 的费率上。
            if !row.symbol.eq_ignore_ascii_case(&book.symbol) {
                unusable += 1;
                continue;
            }
            match parse_row(venue, &row, book, Utc::now()) {
                Some(rate) => out.push(rate),
                None => unusable += 1,
            }
        }

        if filtered > 0 {
            tracing::debug!(%venue, filtered, "其它场所的对照费率已过滤");
        }
        if unusable > 0 {
            crate::http::note_unusable(venue, unusable);
        }
        out.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
        Ok(out)
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        // 不静默放大或截断请求；超出上游范围时明确报错，避免伪装成足够档数。
        if !(1..=DEPTH_MAX_ORDERS).contains(&levels) {
            return Err(ArbError::config("Lighter 盘口 limit 必须在 1..=250"));
        }
        let venue = self.venue_id();
        // 复用详情里的 market_id 与 fetch_all 的在交易永续过滤规则。每次多拉一次
        // details，避免依赖 fetch_all 先运行，也避免缓存市场映射的失效与并发问题。
        let details = self.details_for_depth().await?;
        let market_id = depth_market_id(venue, &details, symbol)?;
        // limit 实际限制每侧挂单笔数，不是不同价格数；原样传入，不为凑档数拉全量。
        // 同价合并后可能少于 levels 档，最后一个价格也可能只覆盖部分挂单。
        let depth: DepthEnvelope = get_json(
            self.client
                .get(self.deployment.url("orderBookOrders"))
                .query(&[("market_id", market_id), ("limit", i64::from(levels))]),
            venue,
        )
        .await?;
        depth.into_book(venue, symbol)
    }

    fn supports_funding_history(&self) -> bool {
        true
    }

    /// `GET /fundings`（公开，逐小时）：`rate` 是**百分数**，`direction` 为 `short` 时
    /// 表示费率为负（空头付给多头）。精度只有 0.0001%（1e-6/小时），够判断符号与量级。
    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        let venue = self.venue_id();
        let details = self.details_for_depth().await?;
        let market_id = depth_market_id(venue, &details, symbol)?;
        let now = Utc::now().timestamp();
        let page: FundingsPage = get_json(
            self.client.get(self.deployment.url("fundings")).query(&[
                ("market_id", market_id.to_string()),
                ("resolution", "1h".to_string()),
                (
                    "start_timestamp",
                    (now - i64::from(hours) * 3600).to_string(),
                ),
                ("end_timestamp", now.to_string()),
                ("count_back", hours.to_string()),
            ]),
            venue,
        )
        .await?;
        if page.code != 200 {
            return Err(ArbError::venue(
                venue.as_str(),
                format!("资金费历史 code={}", page.code),
            ));
        }
        Ok(parse_fundings(page))
    }
}

#[derive(Debug, Deserialize)]
struct FundingsPage {
    code: i64,
    #[serde(default)]
    fundings: Vec<FundingsRow>,
}

#[derive(Debug, Deserialize)]
struct FundingsRow {
    /// 秒。
    timestamp: i64,
    rate: String,
    direction: String,
}

fn parse_fundings(page: FundingsPage) -> Vec<FundingPoint> {
    let mut points: Vec<FundingPoint> = page
        .fundings
        .into_iter()
        .filter_map(|row| {
            let magnitude = parse_decimal(&row.rate)? / Decimal::ONE_HUNDRED;
            let rate = match row.direction.as_str() {
                "long" => magnitude,
                "short" => -magnitude,
                _ => return None,
            };
            Some(FundingPoint {
                at: DateTime::from_timestamp(row.timestamp, 0)?,
                rate,
            })
        })
        .collect();
    points.sort_by_key(|point| point.at);
    points
}

/// 把一张表里的费率与详情合成一条读数。`None` = 这一行不可用。
///
/// `now` 作为参数传入而不是在函数里取，是为了让「下次结算时刻」可被单测断言。
fn parse_row(
    venue: Venue,
    row: &FundingRow,
    book: &BookRow,
    now: DateTime<Utc>,
) -> Option<MarketSnapshot> {
    if book.symbol.is_empty() {
        return None;
    }
    let rate_8h = row.rate.and_then(from_json_f64)?;
    // 表里是 8 小时口径，本场所 1 小时结算 —— 直接当每期费率会虚高 8 倍。
    let period_rate = rate_8h / Decimal::from(LIST_BASIS_HOURS / INTERVAL_H);

    let next_funding_at = next_hour_boundary(now)?;
    let mark_price = book.mark_price.as_deref().and_then(parse_decimal);

    // 持仓量字段是**标的币数量**，必须乘标记价才是 USDT 名义。
    // 拿不到价格时宁可报未知，也不能把币数量当成美元填进去。
    let open_interest_usdt = book
        .open_interest
        .and_then(from_json_f64)
        .filter(|value| *value >= Decimal::ZERO)
        .zip(mark_price)
        .and_then(|(coins, price)| {
            if price <= Decimal::ZERO {
                return None;
            }
            coins.checked_mul(price)
        });

    let (max_leverage, maintenance_margin) = margin_of(book);

    Some(MarketSnapshot {
        venue,
        symbol: Symbol::perp(&book.symbol, QUOTE),
        period_rate,
        interval_h: INTERVAL_H,
        // 1 小时周期来自官方文档的协议约定，不是这个响应里的字段。
        interval_assumed: true,
        next_funding_at,
        // 接口没有结算时刻字段，按「下一个 UTC 整点」推算。
        next_funding_estimated: true,
        // `"0.0000"` 是真值 0，必须与「没给」区分开。
        taker_fee: book.taker_fee.as_deref().and_then(parse_decimal),
        mark_price,
        index_price: book.index_price.as_deref().and_then(parse_decimal),
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt,
        quote_volume_24h: book.daily_quote_token_volume.and_then(from_json_f64),
        max_leverage,
        maintenance_margin,
        // Lighter 公开接口没有持仓量上限信号。
        oi_capped: false,
    })
}

/// (最高杠杆, 维持保证金率)。任一字段缺失或非正就是 `None`，不回落成典型值。
///
/// 最高杠杆 = 1 / 最低初始保证金率；`3333` 这类值算出来是 3.0003 倍，照原样保留，
/// 取整会让边界上的杠杆看起来被允许而实际被拒。
fn margin_of(book: &BookRow) -> (Option<Decimal>, Option<Decimal>) {
    let scale = Decimal::from(MARGIN_FRACTION_SCALE);
    let max_leverage = book
        .min_initial_margin_fraction
        .filter(|fraction| *fraction > 0)
        .map(|fraction| scale / Decimal::from(fraction));
    let maintenance = book
        .maintenance_margin_fraction
        .filter(|fraction| (1..MARGIN_FRACTION_SCALE).contains(fraction))
        .map(|fraction| Decimal::from(fraction) / scale);
    (max_leverage, maintenance)
}

/// 只接在交易的永续。`order_book_details` 里还有非 active 的市场（实测 21 个），
/// 它们的价格与费率可能是停更的。
fn is_active_perp(row: &BookRow) -> bool {
    row.market_type.as_deref() == Some("perp") && row.status.as_deref() == Some("active")
}

/// 下一个 UTC 整点。资金费在整点结算，接口不给时刻，只能按周期推算 ——
/// 推算值必须标记为推算（`next_funding_estimated`），否则界面会把它当成场所给的真值。
fn next_hour_boundary(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let seconds = now.timestamp();
    let next = (seconds.div_euclid(3600) + 1) * 3600;
    DateTime::from_timestamp(next, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VENUE: Venue = Venue::Lighter;

    // 2026-09-19 打真实端点取到的片段，字段名与 JSON 类型原样保留。
    const FUNDING_FIXTURE: &str = r#"{"code":200,"funding_rates":[
        {"market_id":1,"exchange":"lighter","symbol":"BTC","rate":0.00009599999999999999},
        {"market_id":1,"exchange":"binance","symbol":"BTC","rate":0.0001},
        {"market_id":1,"exchange":"hyperliquid","symbol":"BTC","rate":0.0001}
    ]}"#;

    const BOOK_FIXTURE: &str = r#"{"code":200,"order_book_details":[
        {"market_id":1,"symbol":"BTC","market_type":"perp","status":"active","taker_fee":"0.0000",
         "mark_price":"81468.9","index_price":"81479.3","open_interest":2178.34573,
         "daily_quote_token_volume":548204182.531075,"default_initial_margin_fraction":500,
         "min_initial_margin_fraction":200,"maintenance_margin_fraction":120},
        {"market_id":9,"symbol":"DELISTED","market_type":"perp","status":"inactive",
         "taker_fee":"0.0000","mark_price":"1","index_price":"1","open_interest":1,
         "daily_quote_token_volume":1}
    ]}"#;

    // 2026-09-20 curl orderBookOrders?market_id=0&limit=20 的两侧前五笔，
    // 只省略无关字段。0.0976 初始量与 0.0776 剩余量来自同一笔真实部分成交单。
    const DEPTH_FIXTURE: &str = r#"{"code":200,"bids":[
        {"price":"2579.82","initial_base_amount":"6.4693","remaining_base_amount":"6.4693"},
        {"price":"2579.75","initial_base_amount":"0.5095","remaining_base_amount":"0.5095"},
        {"price":"2579.75","initial_base_amount":"0.0976","remaining_base_amount":"0.0776"},
        {"price":"2579.73","initial_base_amount":"13.5958","remaining_base_amount":"13.5958"},
        {"price":"2579.73","initial_base_amount":"0.5039","remaining_base_amount":"0.5039"}
    ],"asks":[
        {"price":"2579.97","remaining_base_amount":"2.3543"},
        {"price":"2580.00","remaining_base_amount":"1.0256"},
        {"price":"2580.01","remaining_base_amount":"68.1640"},
        {"price":"2580.03","remaining_base_amount":"0.0979"},
        {"price":"2580.06","remaining_base_amount":"0.5030"}
    ]}"#;

    fn depth_fixture() -> DepthEnvelope {
        serde_json::from_str(DEPTH_FIXTURE).unwrap()
    }

    #[test]
    fn fundings_are_percentages_and_short_direction_is_negative() {
        // 2026-09-30 实测 `GET /fundings`：rate 是百分数（0.0012 = 0.0012% = 1.2e-5 / 小时），
        // direction 为 short 表示费率为负。
        let page: FundingsPage = serde_json::from_str(
            r#"{"code":200,"resolution":"1h","fundings":[
                {"timestamp":1790665200,"value":"0.5","rate":"0.0012","direction":"long"},
                {"timestamp":1790668800,"value":"0.1","rate":"0.0006","direction":"short"},
                {"timestamp":1790672400,"value":"0.1","rate":"0.0006","direction":"???"}]}"#,
        )
        .unwrap();
        let points = parse_fundings(page);
        assert_eq!(points.len(), 2, "认不出方向的一行丢掉，不猜");
        assert_eq!(points[0].rate.to_string(), "0.000012");
        assert_eq!(points[1].rate.to_string(), "-0.000006");
    }

    #[test]
    fn depth_maps_only_the_matching_active_perpetual() {
        let details: BookDetails = serde_json::from_str(BOOK_FIXTURE).unwrap();
        assert_eq!(
            depth_market_id(VENUE, &details, &Symbol::perp("BTC", QUOTE)).unwrap(),
            1
        );
        for symbol in [
            Symbol::perp("BTC", "USDC"),
            Symbol::perp("DELISTED", QUOTE),
            Symbol::perp("UNKNOWN", QUOTE),
        ] {
            assert!(depth_market_id(VENUE, &details, &symbol).is_err());
        }
        let mut non_perp: BookDetails = serde_json::from_str(BOOK_FIXTURE).unwrap();
        non_perp.order_book_details.as_mut().unwrap()[0].market_type = Some("spot".into());
        assert!(depth_market_id(VENUE, &non_perp, &Symbol::perp("BTC", QUOTE)).is_err());
    }

    #[test]
    fn depth_sorts_and_aggregates_remaining_base_amount_as_quote_notional() {
        let mut depth = depth_fixture();
        // 扰乱真实响应，确保不是碰巧依赖上游排序。
        depth.bids.reverse();
        depth.asks.reverse();
        let symbol = Symbol::perp("ETH", QUOTE);
        let book = depth.into_book(VENUE, &symbol).unwrap();
        assert_eq!(book.venue, VENUE);
        assert_eq!(book.symbol, symbol);
        assert_eq!(
            book.bids
                .iter()
                .map(|level| level.price)
                .collect::<Vec<_>>(),
            vec![
                Decimal::new(257982, 2),
                Decimal::new(257975, 2),
                Decimal::new(257973, 2)
            ]
        );
        assert!(
            book.asks
                .windows(2)
                .all(|pair| pair[0].price < pair[1].price)
        );
        assert_eq!(book.asks[0].price, Decimal::new(257997, 2));
        assert_eq!(book.bids[0].notional_usdt, Decimal::new(16_689_629_526, 6));
        // 使用 initial_base_amount 会得到 1566.166225；正确剩余名义是 1514.571225。
        assert_eq!(book.bids[1].notional_usdt, Decimal::new(1_514_571_225, 6));
        assert_eq!(book.bids[2].notional_usdt, Decimal::new(36_373_419_081, 6));
        assert_eq!(book.asks[0].notional_usdt, Decimal::new(6_074_023_371, 6));
    }

    #[test]
    fn depth_rejects_empty_crossed_and_upstream_error_books() {
        let symbol = Symbol::perp("ETH", QUOTE);
        // 真实错误响应：未知 market_id 返回成功码和空两侧，limit=0 返回 20001。
        for raw in [
            r#"{"code":200,"total_asks":0,"asks":[],"total_bids":0,"bids":[]}"#,
            r#"{"code":20001,"message":"invalid param "}"#,
        ] {
            assert!(
                serde_json::from_str::<DepthEnvelope>(raw)
                    .unwrap()
                    .into_book(VENUE, &symbol)
                    .is_err()
            );
        }
        for clear_bids in [true, false] {
            let mut depth = depth_fixture();
            if clear_bids {
                depth.bids.clear();
            } else {
                depth.asks.clear();
            }
            assert!(depth.into_book(VENUE, &symbol).is_err());
        }
        let mut crossed = depth_fixture();
        crossed.asks[0].price = "2579.81".into();
        assert!(crossed.into_book(VENUE, &symbol).is_err());
        let mut locked = depth_fixture();
        locked.asks[0].price = "2579.82".into();
        assert_eq!(
            locked.into_book(VENUE, &symbol).unwrap().relative_spread(),
            Some(Decimal::ZERO)
        );
    }

    #[test]
    fn depth_drops_unconvertible_orders_and_rejects_missing_amounts() {
        let mut depth = depth_fixture();
        // 合成坏值，防止缺量或溢出被误填为美元；Lighter 的 base_amount 不需要面值。
        for amount in ["0", "-1", "invalid", "79228162514264337593543950335"] {
            depth.bids.push(DepthOrder {
                price: "2579.82".into(),
                remaining_base_amount: amount.into(),
            });
        }
        let book = depth.into_book(VENUE, &Symbol::perp("ETH", QUOTE)).unwrap();
        assert_eq!(book.bids[0].notional_usdt, Decimal::new(16_689_629_526, 6));
        let missing = r#"{"code":200,"bids":[{"price":"2579.82"}],"asks":[]}"#;
        assert!(serde_json::from_str::<DepthEnvelope>(missing).is_err());
        let mut all_invalid = depth_fixture();
        for order in &mut all_invalid.bids {
            order.remaining_base_amount = "0".into();
        }
        assert!(
            all_invalid
                .into_book(VENUE, &Symbol::perp("ETH", QUOTE))
                .is_err()
        );
    }

    #[test]
    fn depth_fixture_fills_notional_with_adverse_slippage_and_reports_exhaustion() {
        use arb_core::{Side, estimate_fill};

        let book = depth_fixture()
            .into_book(VENUE, &Symbol::perp("ETH", QUOTE))
            .unwrap();
        let buy = estimate_fill(&book.asks, Decimal::from(7000), Side::Buy).unwrap();
        assert_eq!(buy.filled_usdt, Decimal::from(7000));
        assert!(buy.average_price > Decimal::new(257997, 2));
        assert!(buy.average_price < Decimal::from(2580));
        assert!(buy.slippage > Decimal::ZERO);
        assert!(!buy.exhausted);
        let sell = estimate_fill(&book.bids, Decimal::from(60000), Side::Sell).unwrap();
        assert_eq!(sell.filled_usdt, Decimal::new(54_577_619_832, 6));
        assert!(sell.average_price < Decimal::new(257982, 2));
        assert!(sell.slippage > Decimal::ZERO);
        assert!(sell.exhausted);
    }

    fn funding_row(exchange: &str) -> FundingRow {
        let envelope: FundingEnvelope = serde_json::from_str(FUNDING_FIXTURE).unwrap();
        envelope
            .into_rows(VENUE)
            .unwrap()
            .into_iter()
            .find(|row| row.exchange == exchange)
            .unwrap()
    }

    fn book(symbol: &str) -> BookRow {
        let details: BookDetails = serde_json::from_str(BOOK_FIXTURE).unwrap();
        details
            .order_book_details
            .unwrap()
            .into_iter()
            .find(|row| row.symbol == symbol)
            .unwrap()
    }

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_789_827_293, 0).unwrap()
    }

    #[test]
    fn the_listed_rate_is_an_eight_hour_basis_and_is_converted_to_the_hourly_period() {
        let rate = parse_row(VENUE, &funding_row("lighter"), &book("BTC"), now()).unwrap();
        // 0.000096（8h 口径）÷ 8 = 0.000012（每小时）
        assert_eq!(rate.period_rate, Decimal::new(12, 6));
        assert_eq!(rate.interval_h, 1);

        // 日化：0.000012 × 24 = 0.000288，与 Binance 8h 的 0.0001 × 3 = 0.0003 同量级。
        // 不做 ÷8 换算就会得到 0.002304，虚高 8 倍。
        let daily = rate.period_rate * Decimal::from(24u32);
        assert_eq!(daily, Decimal::new(288, 6));
    }

    #[test]
    fn other_exchanges_in_the_same_table_are_not_mistaken_for_this_venue() {
        // 表里有 binance / hyperliquid 的同名行，它们的值与本场所不同
        assert_eq!(funding_row("binance").rate, Some(0.0001));
        assert_ne!(
            funding_row("binance").rate,
            funding_row("lighter").rate,
            "不过滤 exchange 就会把别家的费率算成本场所的"
        );
    }

    #[test]
    fn a_reported_zero_taker_fee_stays_zero_and_is_not_unknown() {
        let rate = parse_row(VENUE, &funding_row("lighter"), &book("BTC"), now()).unwrap();
        assert_eq!(rate.taker_fee, Some(Decimal::ZERO));
        assert!(rate.taker_fee.is_some(), "已知为 0 不能退化成「不知道」");

        let mut without_fee = book("BTC");
        without_fee.taker_fee = None;
        assert_eq!(
            parse_row(VENUE, &funding_row("lighter"), &without_fee, now())
                .unwrap()
                .taker_fee,
            None
        );
    }

    #[test]
    fn a_missing_rate_drops_the_row_instead_of_faking_zero() {
        let mut row = funding_row("lighter");
        row.rate = None;
        assert!(parse_row(VENUE, &row, &book("BTC"), now()).is_none());

        row.rate = Some(f64::NAN);
        assert!(parse_row(VENUE, &row, &book("BTC"), now()).is_none());

        row.rate = Some(0.0);
        assert_eq!(
            parse_row(VENUE, &row, &book("BTC"), now())
                .unwrap()
                .period_rate,
            Decimal::ZERO
        );
    }

    #[test]
    fn open_interest_is_converted_from_base_units_to_usdt() {
        let rate = parse_row(VENUE, &funding_row("lighter"), &book("BTC"), now()).unwrap();
        // 2178.34573 BTC × 81468.9 ≈ 1.774e8 USDT（把币数量当美元会得到 2178）
        let oi = rate.open_interest_usdt.unwrap();
        assert!(
            oi > Decimal::new(177_000_000, 0) && oi < Decimal::new(178_000_000, 0),
            "{oi}"
        );

        let mut without_price = book("BTC");
        without_price.mark_price = None;
        assert!(
            parse_row(VENUE, &funding_row("lighter"), &without_price, now())
                .unwrap()
                .open_interest_usdt
                .is_none()
        );
    }

    #[test]
    fn the_next_settlement_is_the_next_utc_hour_and_is_marked_as_estimated() {
        let rate = parse_row(VENUE, &funding_row("lighter"), &book("BTC"), now()).unwrap();
        assert!(
            rate.next_funding_estimated,
            "接口没给结算时刻，必须标记为推算值"
        );
        assert_eq!(rate.next_funding_at.timestamp() % 3600, 0);
        assert!(rate.next_funding_at > now());
        assert_eq!(rate.next_funding_at.timestamp(), 1_789_830_000);
    }

    #[test]
    fn inactive_markets_are_excluded() {
        let details: BookDetails = serde_json::from_str(BOOK_FIXTURE).unwrap();
        let kept: Vec<&BookRow> = details
            .order_book_details
            .as_ref()
            .unwrap()
            .iter()
            .filter(|row| is_active_perp(row))
            .collect();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].symbol, "BTC");
    }

    #[test]
    fn margin_fractions_are_in_ten_thousandths_and_default_is_not_the_cap() {
        let rate = parse_row(VENUE, &funding_row("lighter"), &book("BTC"), now()).unwrap();
        // min_initial_margin_fraction 200 = 2% → 50 倍；default 500（20 倍）只是新账户默认值
        assert_eq!(rate.max_leverage, Some(Decimal::from(50)));
        assert_eq!(rate.maintenance_margin, Some(Decimal::new(12, 3)));

        let mut missing = book("BTC");
        missing.min_initial_margin_fraction = None;
        missing.maintenance_margin_fraction = Some(0);
        let rate = parse_row(VENUE, &funding_row("lighter"), &missing, now()).unwrap();
        assert_eq!(rate.max_leverage, None, "缺字段不能回落成典型杠杆");
        assert_eq!(rate.maintenance_margin, None, "0 维持保证金率不可信");
    }

    #[test]
    fn the_robinhood_deployment_uses_the_same_basis_under_its_own_venue() {
        // 2026-09-23 api.rh.lighter.xyz 的真实片段：同一张表里 TSLA 有四家的对照行。
        let funding: FundingEnvelope = serde_json::from_str(
            r#"{"code":200,"funding_rates":[
                {"market_id":16,"exchange":"binance","symbol":"TSLA","rate":0.00010909},
                {"market_id":16,"exchange":"bybit","symbol":"TSLA","rate":0},
                {"market_id":16,"exchange":"hyperliquid","symbol":"TSLA","rate":5e-05},
                {"market_id":16,"exchange":"lighter","symbol":"TSLA","rate":3.2e-05}]}"#,
        )
        .unwrap();
        let details: BookDetails = serde_json::from_str(
            r#"{"code":200,"order_book_details":[{"market_id":16,"symbol":"TSLA","market_type":"perp",
                "status":"active","taker_fee":"0.0000","mark_price":"379.12","index_price":"378.81",
                "open_interest":3328.8061,"daily_quote_token_volume":1674127.315722,
                "default_initial_margin_fraction":5000,"min_initial_margin_fraction":500,
                "maintenance_margin_fraction":300}]}"#,
        )
        .unwrap();
        let row = funding
            .into_rows(Venue::LighterRh)
            .unwrap()
            .into_iter()
            .find(|row| row.exchange == "lighter")
            .unwrap();
        let book = details.order_book_details.unwrap().remove(0);
        let rate = parse_row(Venue::LighterRh, &row, &book, now()).unwrap();

        assert_eq!(rate.venue, Venue::LighterRh);
        assert_eq!(rate.symbol, Symbol::perp("TSLA", QUOTE));
        // 0.000032（8h 口径）÷ 8 = 0.000004（每小时）
        assert_eq!(rate.period_rate, Decimal::new(4, 6));
        assert_eq!(rate.max_leverage, Some(Decimal::from(20)));
        assert_eq!(rate.maintenance_margin, Some(Decimal::new(3, 2)));
        assert_eq!(
            ROBINHOOD.url("funding-rates"),
            "https://api.rh.lighter.xyz/api/v1/funding-rates"
        );
    }

    #[test]
    fn a_rate_limited_response_is_an_error_not_an_empty_table() {
        // RH 实测限频时的原样响应（HTTP 200）。
        let envelope: FundingEnvelope =
            serde_json::from_str(r#"{"code":23000,"message":"Too Many Requests!"}"#).unwrap();
        let error = envelope.into_rows(Venue::LighterRh).unwrap_err();
        assert!(error.to_string().contains("23000"), "{error}");
    }

    #[test]
    fn a_non_200_code_is_an_error_not_an_empty_result() {
        let envelope: FundingEnvelope =
            serde_json::from_str(r#"{"code":500,"funding_rates":[]}"#).unwrap();
        assert!(envelope.into_rows(VENUE).is_err());
    }

    #[test]
    fn the_funding_payload_key_is_not_the_generic_data_key() {
        // 按 `data` 解析会得到空表 —— 场所静默消失，而日志里什么都不像出错。
        let envelope: FundingEnvelope = serde_json::from_str(FUNDING_FIXTURE).unwrap();
        assert_eq!(envelope.into_rows(VENUE).unwrap().len(), 3);
    }
}
