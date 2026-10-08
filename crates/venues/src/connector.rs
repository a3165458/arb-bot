//! 连接器契约与注册表。
//!
//! 只定义**公共行情**接口：不持有密钥、不下单、不改任何状态。这是只读闭环的地基，
//! 也是「新增一家场所」唯一需要实现的东西。

use std::sync::Arc;

use crate::arcus::ArcusApi;
use crate::aster::AsterApi;
use crate::binance::BinanceApi;
use crate::bitget::BitgetApi;
use crate::bybit::BybitApi;
use crate::gate::GateApi;
use crate::hyperliquid::HyperliquidApi;
use crate::lighter::LighterApi;
use crate::mexc::MexcApi;
use crate::okx::OkxApi;
use crate::ourbit::OurbitApi;
use crate::variational::VariationalApi;
use arb_core::{
    ArbError, ArbResult, Candle, FundingPoint, MarketSnapshot, OrderBook, Settings, Symbol, Venue,
};
use async_trait::async_trait;
use reqwest::Client;

/// 一家场所的公共行情接口。
///
/// # 为什么只有批量端点
///
/// 逐合约的资金费端点普遍**不返回结算周期**，只能回落到默认 8h。实测 Binance 的
/// ACT / AERO / ONDO / WIF 实际是 4h，被当成 8h 后日化只乘 24/8 而非 24/4，
/// **日化与 APR 直接低估一半**（1h 合约低估 8 倍），而排名照常算出来。
///
/// 批量端点则都带周期字段。所以契约只给 `fetch_all`，从类型上堵掉「逐合约拉、
/// 周期靠猜」这条路。需要过滤特定币种时，在拿到全量之后过滤。
#[async_trait]
pub trait VenueApi: Send + Sync + 'static {
    fn venue(&self) -> Venue;

    /// 拉取该场所全部**线性永续**的资金费读数。
    ///
    /// 实现要求：
    /// - 只返回线性永续；交割合约、杠杆代币不要混进来。
    /// - `interval_h` 必须填真实周期；确实拿不到时才用
    ///   [`arb_core::DEFAULT_FUNDING_INTERVAL_H`] 并把 `interval_assumed` 置 `true`。
    /// - 拿不到吃单费率时 `taker_fee` 填 `None`，**不要填一个猜测值**。
    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>>;

    /// 单个合约的盘口深度。
    ///
    /// # 为什么是逐合约，而且默认不实现
    ///
    /// 所有场所的深度端点都是**逐合约**的，没有批量版本。整轮扫描覆盖近千个合约，
    /// 逐个拉就是上千次请求 —— 那会把上游限频直接打爆。所以深度只在需要时对**少数
    /// 候选**调用（`arb-scanner` 的深度体检），用来估算**仓位相关的**滑点；
    /// 整轮排名只用批量端点给的一档价。
    ///
    /// 默认实现返回错误：没实现深度的场所不该假装自己有一份空盘口 ——
    /// 空盘口会被下游当成「吃不到量」，而真相是「我们没取」。
    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        let _ = (symbol, levels);
        Err(ArbError::config(format!(
            "{} 的连接器尚未实现盘口深度",
            self.venue()
        )))
    }

    /// 这家场所有没有公开的逐合约 K 线。
    ///
    /// 默认 `false`。价差榜前排经常是没有这个端点的场所；若仍按榜单前 N 条去拉，
    /// 请求预算会花在必然失败的调用上。实现了 [`VenueApi::fetch_candles`] 的连接器
    /// 要把它改成 `true`。
    fn supports_candles(&self) -> bool {
        false
    }

    /// 单个合约的历史收盘价，按时间**升序**。
    ///
    /// # 为什么是逐合约，而且默认不实现
    ///
    /// 与 [`VenueApi::fetch_depth`] 同理：K 线端点都是逐合约的，整轮扫描覆盖近千个
    /// 合约就是上千次请求。它只在需要时对**少数候选**调用，用来实测基差的收敛速度
    /// （替掉「计划持有 3 天」这个拍脑袋的参数）。
    ///
    /// 默认实现返回错误，而不是一份空序列 —— 空序列会被下游当成「没有历史」，
    /// 而真相是「这家没实现」。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        let _ = (symbol, interval_minutes, limit);
        Err(ArbError::config(format!(
            "{} 的连接器尚未实现 K 线",
            self.venue()
        )))
    }

    /// 这家场所有没有公开的逐合约资金费历史。默认 `false`。
    fn supports_funding_history(&self) -> bool {
        false
    }

    /// 这次 [`VenueApi::fetch_funding_history`] 会不会直接命中缓存（不打上游）。默认 `false`。
    /// 限速的调用方据此决定要不要排队：命中缓存的调用不占上游配额。
    fn funding_history_cached(&self, symbol: &Symbol, hours: u32) -> bool {
        let _ = (symbol, hours);
        false
    }

    /// 单个合约最近 `hours` 小时的逐小时资金费率，按时间**升序**。
    ///
    /// 用来判断费差稳不稳（近几小时是不是一直为正），而不是排名 —— 排名只用批量端点给的
    /// 当前费率。逐合约、按需调用；默认返回错误而不是空序列：空序列会被当成「没有历史」，
    /// 而真相是「这家没实现」。
    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        let _ = (symbol, hours);
        Err(ArbError::config(format!(
            "{} 的连接器尚未实现资金费历史",
            self.venue()
        )))
    }
}

/// 构造单个场所的连接器。
///
/// 返回类型不是 `Result`：全部场所接入后，「构造失败」只剩「新增了 `Venue`
/// 变体却忘了在这里登记」这一种情况 —— 那应该**编译不过**，而不是运行时才报错。
/// 这个 `match` 是穷尽的，新增场所会让编译器在这里拦住你。
pub fn build(venue: Venue, client: Client) -> Arc<dyn VenueApi> {
    match venue {
        Venue::Arcus => Arc::new(ArcusApi::new(client)),
        Venue::Aster => Arc::new(AsterApi::new(client)),
        Venue::Binance => Arc::new(BinanceApi::new(client)),
        Venue::Bitget => Arc::new(BitgetApi::new(client)),
        Venue::Bybit => Arc::new(BybitApi::new(client)),
        Venue::Gate => Arc::new(GateApi::new(client)),
        Venue::Hyperliquid => Arc::new(HyperliquidApi::main(client)),
        Venue::HyperliquidIo => Arc::new(HyperliquidApi::io(client)),
        Venue::HyperliquidXyz => Arc::new(HyperliquidApi::xyz(client)),
        Venue::Lighter => Arc::new(LighterApi::mainnet(client)),
        Venue::LighterRh => Arc::new(LighterApi::robinhood(client)),
        Venue::Mexc => Arc::new(MexcApi::new(client)),
        Venue::Okx => Arc::new(OkxApi::new(client)),
        Venue::Ourbit => Arc::new(OurbitApi::new(client)),
        Venue::Variational => Arc::new(VariationalApi::new(client)),
    }
}

/// 按配置构造全部连接器。
///
/// `ARB_VENUES` 里的名字在解析阶段就已校验（拼错直接报错），这里不再有失败路径。
pub fn build_all(settings: &Settings, client: &Client) -> Vec<Arc<dyn VenueApi>> {
    settings
        .effective_venues()
        .into_iter()
        .map(|venue| {
            Arc::new(HistoryCached::new(build(venue, client.clone()))) as Arc<dyn VenueApi>
        })
        .collect()
}

/// 资金费历史缓存多久。历史按小时结算，十分钟内不会有新点；预览、预检、后台规则都要读它，
/// 不缓存会把 Lighter RH 这类限频紧的场所打满。
const HISTORY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(600);

type HistoryKey = (Symbol, u32);
type HistoryEntry = (std::time::Instant, Vec<FundingPoint>);

/// 只给 [`VenueApi::fetch_funding_history`] 加缓存的透明包装，其余调用原样转发。
/// 失败不缓存：下一次调用会重试。
pub struct HistoryCached {
    inner: Arc<dyn VenueApi>,
    history: std::sync::Mutex<std::collections::HashMap<HistoryKey, HistoryEntry>>,
}

impl HistoryCached {
    pub fn new(inner: Arc<dyn VenueApi>) -> Self {
        Self {
            inner,
            history: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl VenueApi for HistoryCached {
    fn venue(&self) -> Venue {
        self.inner.venue()
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        self.inner.fetch_all().await
    }

    async fn fetch_depth(&self, symbol: &Symbol, levels: u32) -> ArbResult<OrderBook> {
        self.inner.fetch_depth(symbol, levels).await
    }

    fn supports_candles(&self) -> bool {
        self.inner.supports_candles()
    }

    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        self.inner
            .fetch_candles(symbol, interval_minutes, limit)
            .await
    }

    fn supports_funding_history(&self) -> bool {
        self.inner.supports_funding_history()
    }

    fn funding_history_cached(&self, symbol: &Symbol, hours: u32) -> bool {
        self.history
            .lock()
            .ok()
            .and_then(|cache| {
                cache
                    .get(&(symbol.clone(), hours))
                    .map(|(at, _)| at.elapsed() < HISTORY_CACHE_TTL)
            })
            .unwrap_or(false)
    }

    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        let key = (symbol.clone(), hours);
        if let Ok(cache) = self.history.lock()
            && let Some((at, points)) = cache.get(&key)
            && at.elapsed() < HISTORY_CACHE_TTL
        {
            return Ok(points.clone());
        }
        let points = self.inner.fetch_funding_history(symbol, hours).await?;
        if let Ok(mut cache) = self.history.lock() {
            cache.insert(key, (std::time::Instant::now(), points.clone()));
        }
        Ok(points)
    }
}
