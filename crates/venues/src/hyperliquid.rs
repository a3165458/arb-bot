//! Hyperliquid 主 perp dex 与 HIP-3 子交易所的公共行情。
//!
//! `metaAndAssetCtxs` 的 universe 与 ctxs 必须按下标对齐，不能先移除 null 再配对。
//! `funding` 已是每小时费率，不再除以 8；官方规则明确周期与资产无关：
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
//! 实测 `predictedFundings` 的 HlPerp 全部为 1h，历史结算也每小时一次。
//!
//! 批量行情不含结算时间；实测 predictedFundings 的 nextFundingTime 落在过去，
//! 因此按下一 UTC 整点推算并标记 estimated，不冒充服务端时间。
//! 持仓量以合约基础单位返回，乘 markPx 得到报价资产名义量，不直接当成 USDT。
//! 吃单费率按**最低交易量档**（基础档）算：账户的交易量档位、质押与推荐优惠只会让它更低，
//! 所以这是任何账户都不会超过的上限（见 [`taker_fee_for`]）：
//! - 主 dex：基础档 0.045%；
//! - HIP-3：再乘 deployer 倍数（官方规则 `scale < 1 ? 1 + scale : 2 × scale`，xyz / io 实测 `1.0` → ×2）；
//!   开着 growth mode 的合约手续费（与返佣）打一折（×0.1）。逐合约读 `deployerFeeScale` / `growthMode`，
//!   缺倍数就不知道 —— 保留 None，不猜；
//! - `hyperliquid-io`（Entropy 部署）的自返佣：`ARB_ENTROPY_SELF_REBATE`（Tier 4 = `2`，即 200%）
//!   按 Entropy 那一半的份额返还，净费率 = 费率 × (1 − 返佣 / 2)，**最低 0，不算成负的**。
//!   只在倍数为 1（官方文档写明的五五分成）时适用。
//!
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
//! https://docs.entropy.io/equity-perp-mechanics/fees 、 https://docs.entropy.io/about-entropy/referrals
//!
//! 主 dex 合约通常 USDT 计价、USDC 保证金，属于无汇率换算的 quanto 合约；
//! 与 USDT 保证金合约仍有抵押品风险差异。HYPE/PURR 官方明确为 USDC 计价，排除。
//! 不移除 kPEPE 等名称的倍率前缀。
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications
//!
//! # HIP-3 子交易所
//!
//! 同一个 `/info` 端点，请求体多带 `"dex"` 就是子交易所的行情。每个 dex 有自己的
//! 清算所与保证金，所以各记为一家场所。只接两个：
//!
//! | 场所 | dex | 市场 |
//! | --- | --- | --- |
//! | `hyperliquid-xyz` | `xyz` | 美股、指数、商品、外汇，一百二十余个 |
//! | `hyperliquid-io` | `io` | Pre-IPO（OpenAI / Anthropic）与少量股票，全部逐仓 |
//!
//! `perpDexs` 里还有十来个 dex（flx / vntl / km …），多数与 `xyz` 重复上市同一批股票，
//! 接进来只会在同一合约里多出几条同名腿，而不是新的对冲来源。
//!
//! 子交易所的币名带前缀（`xyz:TSLA`），`l2Book` / `candleSnapshot` 都要原样带前缀
//! 查询（实测 `xyz:TSLA` 能拿到盘口）。对外的 `Symbol` 去掉前缀，才能与其它场所的
//! `TSLA` 配对。2026-09-23 实测 `xyz:TSLA` 每小时整点结算，与主 dex 同规则；
//! 两个 dex 的 `collateralToken` 都是 0（USDC），按主 dex 同样的理由记为 USDT。
//!
//! 少数合约在别家用的是另一个名字，按同一分钟的价格核实过才做映射：
//!
//! | 原名 | 映射为 | 核实（Hyperliquid 预言机价 ↔ Lighter RH 指数价） |
//! | --- | --- | --- |
//! | `xyz:GOLD` | `XAU` | 4336.3 ↔ 4335.67 |
//! | `xyz:SILVER` | `XAG` | 66.445 ↔ 66.4385 |
//! | `io:ANTH` | `ANTHROPIC` | 2181.9 ↔ 2189.9 |
//! | `io:OAI` | `OPENAI` | 1699.8 ↔ 1707.86 |
//!
//! 名字不同的**不能**凭印象映射：`xyz:CL`（原油期货）89.75 ↔ `USO`（原油 ETF）144.23、
//! `xyz:SP500` 7766 ↔ `SPY` 773.71 都是不同资产，映射过去就是拿两个资产算价差。
//!
//! # 杠杆与持仓量上限
//!
//! `meta.universe[].maxLeverage` 是最低档的最高杠杆。官方规则里维持保证金率是
//! 「最高杠杆下初始保证金率的一半」，即 `1 / (2 × maxLeverage)`：
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
//! 大仓位落到更高档时杠杆更低、维持保证金更高，这里只描述最低档。
//!
//! `perpsAtOpenInterestCap` 返回当前触及持仓量上限的币名列表（实测主 dex 有 9 个：
//! CANTO / FTM / HMSTR …），触顶时只能减仓。它是辅助信号：这个请求失败不能让整家
//! 场所的资金费读数跟着消失，只记警告，`oi_capped` 保持「未报告」。
//!
//! 批量 `metaAndAssetCtxs` 没有最优买卖价和一档量，四个盘口字段因此保持 None。
//! `impactPxs` 是买卖两侧成交固定冲击名义额的平均成交价，不是一档价格；
//! 资金费冲击名义额为 BTC/ETH 20000 USDC、其他资产 6000 USDC。即使一档足够厚、
//! 两者数值恰好相同，也不能把冲击价冒充最优价，更不能从它反推出一档量。
//! `midPx`/`markPx` 同样不能替代两侧可成交价，否则会伪造零穿价成本。
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
//! https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications
//!
//! 已核实的公共 REST 文档没有批量盘口接口；`allMids` 仅返回中间价，空盘还会回落成交价。
//! 真盘口 `l2Book` 必须指定一个 `coin`：扫描 N 个合约需额外 N 次请求，不能批量获取。
//! 2026-09-20 实测主 dex universe 有 234 条（含已下架）；逐条查询就是额外 234 次，
//! 即使先过滤已下架合约，仍需每个活跃合约一次，所以 `fetch_all` 只打批量端点，
//! 深度交给 [`HyperliquidApi::fetch_depth`] 按需对**少数候选**逐币拉：一次一个 `coin`。
//! https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
//!
//! `l2Book` 的 `sz` 是**标的币数量**：Hyperliquid 永续 1 张 = 1 标的单位，`meta` 的
//! `szDecimals` 就是标的的小数位（实测 kPEPE `szDecimals=0`，它的 `sz` 全是整数），
//! 所以计价币名义 = `sz × px`，不存在面值/张数换算。
//! `levels` 参数**映射不到该端点**：`l2Book` 只有 `nSigFigs`/`mantissa` 两个聚合参数，
//! 没有档数参数（实测传 `n` 或 `levels` 都被忽略），文档写明每侧最多 20 档，
//! 因此 `fetch_depth` 忽略 `levels`，只返回端点给的档位。
//! 已下架的 MATIC 实测返回 `levels: [[], []]` —— 对象在、两侧为空 —— 所以空盘口是
//! 真实会出现的响应，必须报错，而不是返回一份空盘口让下游以为「吃不到量」。
//! 币名**大小写敏感**：实测 `KPEPE` 返回 `null`、`kPEPE` 才给盘口；而 `Symbol::perp`
//! 会把 base 转成大写，所以直接按 `Symbol` 查不到的币名要回查一次 `meta` 还原拼写。
//!
//! 历史 K 线走 `candleSnapshot`。它**必须给 `startTime`/`endTime`**（实测只给
//! `endTime` 会 HTTP 422 报 `failed to parse: value expected`），所以 `limit` 只能先按
//! 周期反推成一个时间窗口，见 [`candle_window`]。窗口刻意比 `limit × 周期` 宽一倍：
//! 交易所会缺 K 线，窗口刚好等于需求量时缺口会让序列短于 `limit`；多要的部分只影响
//! 请求体大小（服务端本来就有上限），取最后 `limit` 根即可。
//!
//! `interval` 是**枚举**，不是任意分钟数：实测合法值只有
//! `1m/3m/5m/15m/30m/1h/2h/4h/8h/12h/1d/3d/1w/1M`，传 `2m`/`6h`/`45m`/`90m`/`10080`
//! 一律 HTTP 422（服务端 JSON 反序列化失败），因此必须自己映射，见 [`candle_interval`]。
//! 请求周期不在枚举里时**向上取粗**：取细了会让半衰期被算短、持有期估得激进，
//! 取粗只是保守；这是刻意选的偏差方向，不是误差。
//!
//! 响应是**裸数组**（不是 `{code,data}`）、**升序**（实测 541 根全部递增），每根是
//! `{"t","T","s","i","o","c","h","l","v","n"}`：只取 `t`（**开盘**时刻，**毫秒** epoch）
//! 和 `c`（收盘价，**字符串**）。`T` 是收盘时刻，`o/h/l` 是其它口径，`v`/`n` 是成交量
//! 与笔数 —— 都不能拿来当收盘价。已核对量级：BTC 1h 收盘 `80360.0` 与同刻 `markPx`
//! 一致，而 `v` 是 `1885.99984`（币数量），数量级完全不同。
//! 时刻一律用原始 `t` 解析，**不按「下标 × 周期」推算**：实测 Hyperliquid 会给无成交的
//! 分钟补一根（低量币 APEX 也返回满 4320 根），但那是它的实现细节；真出现缺口时，
//! 只有原始时间戳能保留缺口的距离信息，按下标推算会把缺口抹平。
//! 最后一根可能是**尚未走完**的当根 K 线（实测 `T` 落在请求的 `endTime` 之后），
//! 它的 `c` 就是当前价；这里照原样返回，不假装它是已收盘的观测。
//! 单次响应有约 700KB / 5000 根的上限：窗口超限时服务端返回**最近**的那一段
//! （实测 `1m` 要 10 万分钟只回 5047 根，且最后一根就是当前根），所以放宽窗口是安全的；
//! 但 `limit` 超过约 5000 时一次请求拿不全，只能返回能拿到的部分。
//! 未知币名实测返回 HTTP 200 + `null`（合法 JSON，不是空数组），已下架的 MATIC 则返回
//! `[]` —— 两者都要能和「有历史但没数据」区分开，见 [`parse_candles`] 与调用处。

use arb_core::{
    ArbError, ArbResult, Candle, Decimal, FundingPoint, Level, MarketSnapshot, OrderBook, Symbol,
    Venue, parse_decimal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tracing::warn;

use crate::connector::VenueApi;
use crate::http::get_json;

const INFO_URL: &str = "https://api.hyperliquid.xyz/info";

/// 永续吃单费率的基础档（最低交易量档）。交易量档位、质押与推荐优惠只会更低。
/// `userFees.feeSchedule.cross`（2026-10-09 实测 `"0.00045"`）。
pub const BASE_TAKER_FEE: Decimal = Decimal::from_parts(45, 0, 0, false, 5);

/// Entropy（`hyperliquid-io` 的部署方）自返佣比例的上限：Tier 4 = 200%。
pub const MAX_ENTROPY_SELF_REBATE: Decimal = Decimal::from_parts(2, 0, 0, false, 0);

/// 读 `ARB_ENTROPY_SELF_REBATE`（小数，`2` = 200%）。没设为 0；不合法返回错误。
pub fn entropy_self_rebate_from_env() -> ArbResult<Decimal> {
    let Ok(raw) = std::env::var("ARB_ENTROPY_SELF_REBATE") else {
        return Ok(Decimal::ZERO);
    };
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("off") {
        return Ok(Decimal::ZERO);
    }
    let value = parse_decimal(raw).ok_or_else(|| {
        ArbError::config(format!(
            "ARB_ENTROPY_SELF_REBATE 必须是小数（2 = 200%），收到 {raw:?}"
        ))
    })?;
    if !(Decimal::ZERO..=MAX_ENTROPY_SELF_REBATE).contains(&value) {
        return Err(ArbError::config(format!(
            "ARB_ENTROPY_SELF_REBATE 必须在 0 到 {MAX_ENTROPY_SELF_REBATE} 之间，收到 {value}"
        )));
    }
    Ok(value)
}

/// 一个合约的吃单费率（单边，小数）。`None` = HIP-3 合约缺 deployer 倍数，不知道。
///
/// `rebate` 只对 `hyperliquid-io` 生效，且只在倍数为 1 时（Entropy 拿一半）。结果不小于 0。
pub fn taker_fee_for(
    venue: Venue,
    deployer_fee_scale: Option<&str>,
    growth_mode: Option<&str>,
    rebate: Decimal,
) -> Option<Decimal> {
    if venue == Venue::Hyperliquid {
        return Some(BASE_TAKER_FEE);
    }
    let scale = parse_decimal(deployer_fee_scale?)?;
    if scale < Decimal::ZERO {
        return None;
    }
    let multiplier = if scale < Decimal::ONE {
        Decimal::ONE + scale
    } else {
        Decimal::TWO * scale
    };
    let mut fee = BASE_TAKER_FEE * multiplier;
    if growth_mode == Some("enabled") {
        fee *= Decimal::new(1, 1);
    }
    if venue == Venue::HyperliquidIo && scale == Decimal::ONE && rebate > Decimal::ZERO {
        let rebate = rebate.min(MAX_ENTROPY_SELF_REBATE);
        fee *= Decimal::ONE - rebate / Decimal::TWO;
    }
    Some(fee.max(Decimal::ZERO))
}

/// 原始币名（`xyz:GOLD`）→ 对外的 base（`XAU`），按该场所的前缀与已核实别名。不是本场所的币名为 `None`。
pub fn base_for(venue: Venue, raw: &str) -> Option<String> {
    let dex = match venue {
        Venue::Hyperliquid => MAIN_DEX,
        Venue::HyperliquidXyz => XYZ_DEX,
        Venue::HyperliquidIo => IO_DEX,
        _ => return None,
    };
    if dex.is_usdc_quoted(raw) {
        return None;
    }
    dex.base_of(raw)
}

/// 场所对应的 `/info` 请求里的 `dex`（主 dex 为 `None`）。
pub fn dex_name(venue: Venue) -> Option<&'static str> {
    match venue {
        Venue::HyperliquidXyz => XYZ_DEX.name,
        Venue::HyperliquidIo => IO_DEX.name,
        _ => None,
    }
}

/// 一个 perp dex：场所身份、请求里的 `dex` 参数、以及已核实的别名。
#[derive(Debug, Clone, Copy)]
struct Dex {
    venue: Venue,
    /// `None` = 主 dex（请求体不带 `dex`）。
    name: Option<&'static str>,
    /// (去掉前缀后的原名, 对外名)。
    aliases: &'static [(&'static str, &'static str)],
}

const MAIN_DEX: Dex = Dex {
    venue: Venue::Hyperliquid,
    name: None,
    aliases: &[],
};

const XYZ_DEX: Dex = Dex {
    venue: Venue::HyperliquidXyz,
    name: Some("xyz"),
    aliases: &[("GOLD", "XAU"), ("SILVER", "XAG")],
};

const IO_DEX: Dex = Dex {
    venue: Venue::HyperliquidIo,
    name: Some("io"),
    aliases: &[("ANTH", "ANTHROPIC"), ("OAI", "OPENAI")],
};

impl Dex {
    /// `/info` 请求体。子交易所多带一个 `dex`。
    fn body(self, kind: &str) -> serde_json::Value {
        match self.name {
            Some(dex) => json!({ "type": kind, "dex": dex }),
            None => json!({ "type": kind }),
        }
    }

    /// 原始币名 → 对外的 base。子交易所的币名必须带本 dex 的前缀，缺了说明响应不对，
    /// 返回 `None` 让这一行作废，而不是把别处的合约当成本 dex 的。
    fn base_of(self, raw: &str) -> Option<String> {
        let bare = match self.name {
            Some(dex) => raw.strip_prefix(dex)?.strip_prefix(':')?,
            None => raw,
        };
        if bare.is_empty() {
            return None;
        }
        let upper = bare.to_ascii_uppercase();
        Some(
            self.aliases
                .iter()
                .find(|(from, _)| *from == upper)
                .map_or(upper, |(_, to)| (*to).to_string()),
        )
    }

    /// 主 dex 的 HYPE/PURR 是 USDC 计价，不能标成 USDT。子交易所没有这条例外。
    fn is_usdc_quoted(self, raw: &str) -> bool {
        self.name.is_none() && matches!(raw, "HYPE" | "PURR")
    }
}

/// `candleSnapshot` 接受的周期（分钟 → 字符串）与它覆盖的分钟数。
const CANDLE_INTERVALS: [(u32, &str); 13] = [
    (1, "1m"),
    (3, "3m"),
    (5, "5m"),
    (15, "15m"),
    (30, "30m"),
    (60, "1h"),
    (120, "2h"),
    (240, "4h"),
    (480, "8h"),
    (720, "12h"),
    (1440, "1d"),
    (4320, "3d"),
    (10080, "1w"),
];

/// 返回 (实际周期分钟数, 周期字符串)。
///
/// 向上取整：更细的序列会把半衰期算短，而持有期算短会让年化虚高。
fn candle_interval(minutes: u32) -> ArbResult<(u32, &'static str)> {
    CANDLE_INTERVALS
        .iter()
        .find(|(value, _)| *value >= minutes)
        .copied()
        .ok_or_else(|| {
            ArbError::config(format!(
                "K 线周期 {minutes} 分钟超过该端点支持的最大周期（{}）",
                CANDLE_INTERVALS[CANDLE_INTERVALS.len() - 1].0
            ))
        })
}

#[derive(Debug, Deserialize)]
struct HlCandle {
    t: i64,
    c: String,
}

/// 收盘价缺失/不可解析/非正的跳过；只保留最后 `limit` 根。
fn parse_candles(rows: Vec<HlCandle>, limit: usize) -> Vec<Candle> {
    let mut out: Vec<Candle> = rows
        .into_iter()
        .filter_map(|row| {
            let close = parse_decimal(&row.c)?;
            if close <= Decimal::ZERO {
                return None;
            }
            Some(Candle {
                open_time: DateTime::from_timestamp_millis(row.t)?,
                close,
            })
        })
        .collect();
    out.sort_by_key(|candle| candle.open_time);
    if out.len() > limit {
        out.drain(..out.len() - limit);
    }
    out
}

const QUOTE: &str = "USDT";

/// 资金费结算周期（小时）。场所协议常量，实测每个币都是 1。
const FUNDING_INTERVAL_H: u32 = 1;

pub struct HyperliquidApi {
    client: Client,
    dex: Dex,
    /// Entropy 自返佣（只对 io 生效），见 [`taker_fee_for`]。
    rebate: Decimal,
}

impl HyperliquidApi {
    /// 主 perp dex（`hyperliquid`）。
    pub fn main(client: Client) -> Self {
        Self {
            client,
            dex: MAIN_DEX,
            rebate: Decimal::ZERO,
        }
    }

    /// HIP-3 `xyz` dex（`hyperliquid-xyz`）。
    pub fn xyz(client: Client) -> Self {
        Self {
            client,
            dex: XYZ_DEX,
            rebate: Decimal::ZERO,
        }
    }

    /// HIP-3 `io` dex（`hyperliquid-io`）。
    pub fn io(client: Client) -> Self {
        // 启动时 `arb-web` 已校验过这个变量；这里读不懂就按没有返佣（偏保守）。
        let rebate = entropy_self_rebate_from_env().unwrap_or_else(|error| {
            warn!(%error, "Entropy 自返佣读不懂，按 0 计");
            Decimal::ZERO
        });
        Self {
            client,
            dex: IO_DEX,
            rebate,
        }
    }

    async fn info<T: serde::de::DeserializeOwned>(&self, body: &serde_json::Value) -> ArbResult<T> {
        get_json(self.client.post(INFO_URL).json(body), self.dex.venue).await
    }

    async fn fetch_l2(&self, coin: &str) -> ArbResult<Option<L2Book>> {
        // 未知币名实测返回 HTTP 200 + null，不能把它当成一份空盘口。
        // l2Book 按带前缀的全名定位，不需要 dex 参数。
        self.info(&json!({ "type": "l2Book", "coin": coin })).await
    }

    /// 当前触及持仓量上限的原始币名。失败只记警告：它是辅助信号，不能拖垮资金费读数。
    async fn fetch_oi_capped(&self) -> Option<Vec<String>> {
        match self
            .info::<Vec<String>>(&self.dex.body("perpsAtOpenInterestCap"))
            .await
        {
            Ok(capped) => Some(capped),
            Err(error) => {
                warn!(venue = %self.dex.venue, %error, "持仓量上限列表取数失败，本轮不标记触顶");
                None
            }
        }
    }
}

/// 响应是 `[meta, ctxs]` 两元素数组，两个数组**按下标对齐**（不是按名字）。
#[derive(Debug, Deserialize)]
struct MetaAndAssetCtxs(Meta, Vec<Option<AssetCtx>>);

#[derive(Debug, Deserialize)]
struct Meta {
    universe: Vec<UniverseAsset>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UniverseAsset {
    name: String,
    /// 已下架合约仍会返回字面 0 的费率，不能因此当作活跃市场。
    #[serde(default)]
    is_delisted: bool,
    /// 最低档的最高杠杆（整数）。
    #[serde(default)]
    max_leverage: Option<u32>,
    /// HIP-3 deployer 手续费倍数（字符串，如 `"1.0"`）。主 dex 没有。
    #[serde(default)]
    deployer_fee_scale: Option<String>,
    /// `"enabled"` = growth mode：手续费与返佣打一折。
    #[serde(default)]
    growth_mode: Option<String>,
}

/// 缺少费率只丢该行；缺少辅助指标保留 None，不用零填充。
///
/// 不解析 `midPx` / `impactPxs`：中间价与冲击成交均价都不能代表最优买卖价。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetCtx {
    /// 每**小时**费率。
    funding: Option<String>,
    mark_px: Option<String>,
    /// 指数价（Hyperliquid 叫 oracle price）。
    oracle_px: Option<String>,
    /// **币**的数量，不是 USDT 名义。
    open_interest: Option<String>,
    /// 24h 名义成交额（USD）。
    day_ntl_vlm: Option<String>,
}

#[derive(Debug, Deserialize)]
struct L2Book {
    coin: String,
    // 固定两侧才能避免残缺响应被误当成正常的单边市场。
    levels: [Vec<L2Level>; 2],
}

#[derive(Debug, Deserialize)]
struct L2Level {
    px: String,
    sz: String,
}

#[async_trait]
impl VenueApi for HyperliquidApi {
    fn venue(&self) -> Venue {
        self.dex.venue
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        let venue = self.dex.venue;
        let body = self.dex.body("metaAndAssetCtxs");
        let (response, capped) =
            tokio::join!(self.info::<MetaAndAssetCtxs>(&body), self.fetch_oi_capped(),);
        let capped = capped.unwrap_or_default();
        let parsed = parse_response_with(self.dex, &response?, &capped, Utc::now(), self.rebate)?;

        // 两种「少了一条」要分开报：已下架是预期内的，字段不可用则说明数据源变了。
        if parsed.filtered > 0 {
            tracing::debug!(%venue, filtered = parsed.filtered, "已下架或非 USDT 计价合约已过滤");
        }
        if parsed.unusable > 0 {
            crate::http::note_unusable(venue, parsed.unusable);
        }
        Ok(parsed.rates)
    }

    async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
        let venue = self.dex.venue;
        // 与批量入口保持相同的计价资产和 dex 边界，不能把现货或 USDC 盘口标成 USDT。
        if symbol.quote != QUOTE
            || self.dex.is_usdc_quoted(&symbol.base)
            || symbol.base.contains([':', '@', '/'])
        {
            return Err(ArbError::venue(
                venue.as_str(),
                "仅支持本 dex 的 USDT 计价永续",
            ));
        }
        // l2Book 不提供档数参数；不拿聚合精度冒充档数，也不截断服务端给出的盘口。
        // 主 dex 先按大写拼写直接查；子交易所的币名带前缀、可能有别名，必须回查元数据。
        let direct = match self.dex.name {
            None => self.fetch_l2(&symbol.base).await?,
            Some(_) => None,
        };
        let response = match direct {
            Some(book) => book,
            None => {
                // Symbol 会大写化，但 API 只认 kPEPE 等原始拼写；仅在查不到时回查元数据。
                let meta: Meta = self.info(&self.dex.body("meta")).await?;
                let coin = depth_coin(self.dex, &meta, symbol)?;
                if self.dex.name.is_none() && coin == symbol.base {
                    return Err(ArbError::venue(
                        venue.as_str(),
                        format!("{symbol} 没有盘口"),
                    ));
                }
                self.fetch_l2(coin)
                    .await?
                    .ok_or_else(|| ArbError::venue(venue.as_str(), format!("{symbol} 没有盘口")))?
            }
        };
        parse_depth(self.dex, response, symbol)
    }
    fn supports_candles(&self) -> bool {
        true
    }

    /// 单个合约的历史收盘价。
    ///
    /// `candleSnapshot` 是 POST，而且**必须给时间窗口**（没有 limit 参数），
    /// 所以由 `limit × 周期` 反推起点，并留 50% 余量：缺 K 线是常态，
    /// 余量不够会让序列比请求的短。取回后只保留最后 `limit` 根。
    ///
    /// 币名**大小写敏感**（`kPEPE` 不是 `KPEPE`），复用 `depth_coin` 的 meta 回查。
    async fn fetch_candles(
        &self,
        symbol: &Symbol,
        interval_minutes: u32,
        limit: u32,
    ) -> ArbResult<Vec<Candle>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (effective_minutes, interval) = candle_interval(interval_minutes)?;
        let coin = self.candle_coin(symbol).await?;
        let now = Utc::now();
        let span_ms = i64::from(effective_minutes) * i64::from(limit) * 60_000 * 3 / 2;
        let body = serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": now.timestamp_millis() - span_ms,
                "endTime": now.timestamp_millis(),
            }
        });
        let rows: Vec<HlCandle> = self.info(&body).await?;
        Ok(parse_candles(rows, limit as usize))
    }

    fn supports_funding_history(&self) -> bool {
        true
    }

    /// `fundingHistory`：每小时一行，`fundingRate` 已经是小时费率（小数）。币名同样要按
    /// meta 回查原始拼写（子交易所带前缀）。
    async fn fetch_funding_history(
        &self,
        symbol: &Symbol,
        hours: u32,
    ) -> ArbResult<Vec<FundingPoint>> {
        let coin = self.candle_coin(symbol).await?;
        let start = Utc::now().timestamp_millis() - i64::from(hours) * 3_600_000;
        let rows: Vec<HlFundingRow> = self
            .info(&serde_json::json!({
                "type": "fundingHistory", "coin": coin, "startTime": start,
            }))
            .await?;
        Ok(parse_funding_rows(rows))
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HlFundingRow {
    funding_rate: String,
    /// 毫秒。
    time: i64,
}

fn parse_funding_rows(rows: Vec<HlFundingRow>) -> Vec<FundingPoint> {
    let mut points: Vec<FundingPoint> = rows
        .into_iter()
        .filter_map(|row| {
            Some(FundingPoint {
                at: chrono::DateTime::from_timestamp_millis(row.time)?,
                rate: parse_decimal(&row.funding_rate)?,
            })
        })
        .collect();
    points.sort_by_key(|point| point.at);
    points
}

impl HyperliquidApi {
    /// 解析该合约在 Hyperliquid 里的原始币名（含子交易所前缀）。
    ///
    /// `Symbol::perp` 会大写化并去掉前缀、套用别名，而 API 只认 `kPEPE`、`xyz:GOLD`
    /// 这类原始拼写，所以按本 dex 的 `meta` 反查。
    async fn candle_coin(&self, symbol: &Symbol) -> ArbResult<String> {
        let meta: Meta = self.info(&self.dex.body("meta")).await?;
        Ok(depth_coin(self.dex, &meta, symbol)?.to_string())
    }
}

fn depth_coin<'a>(dex: Dex, meta: &'a Meta, symbol: &Symbol) -> ArbResult<&'a str> {
    meta.universe
        .iter()
        .find(|asset| {
            is_tradable_perp(dex, asset)
                && dex
                    .base_of(&asset.name)
                    .is_some_and(|base| base == symbol.base)
        })
        .map(|asset| asset.name.as_str())
        .ok_or_else(|| {
            ArbError::venue(
                dex.venue.as_str(),
                format!("{} 没有可交易的 {symbol}", dex.venue),
            )
        })
}

fn parse_depth(dex: Dex, response: L2Book, symbol: &Symbol) -> ArbResult<OrderBook> {
    let venue = dex.venue;
    if dex.base_of(&response.coin).as_deref() != Some(symbol.base.as_str()) {
        return Err(ArbError::venue(
            venue.as_str(),
            format!("{symbol} 盘口币名不匹配"),
        ));
    }
    let parse_side = |raw: Vec<L2Level>| -> ArbResult<Vec<Level>> {
        raw.into_iter()
            .map(|level| {
                let parsed = parse_decimal(&level.px)
                    .zip(parse_decimal(&level.sz))
                    .filter(|(px, sz)| *px > Decimal::ZERO && *sz > Decimal::ZERO)
                    .and_then(|(price, size)| {
                        // 1 合约单位就是 1 标的单位，n 是聚合信息，不是换算乘数。
                        price.checked_mul(size).map(|notional_usdt| Level {
                            price,
                            notional_usdt,
                        })
                    })
                    .filter(|level| level.notional_usdt > Decimal::ZERO);
                // 坏价量或溢出必须显式失败，不能让缺损深度冒充完整的可成交盘口。
                parsed.ok_or_else(|| {
                    ArbError::venue(venue.as_str(), format!("{symbol} 盘口价量无效或名义额溢出"))
                })
            })
            .collect()
    };
    let [raw_bids, raw_asks] = response.levels;
    let mut bids = parse_side(raw_bids)?;
    let mut asks = parse_side(raw_asks)?;
    bids.sort_unstable_by_key(|level| std::cmp::Reverse(level.price));
    asks.sort_unstable_by_key(|a| a.price);
    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
        return Err(ArbError::venue(venue.as_str(), format!("{symbol} 空盘口")));
    };
    if ask.price < bid.price {
        return Err(ArbError::venue(
            venue.as_str(),
            format!("{symbol} 交叉盘口"),
        ));
    }
    Ok(OrderBook {
        venue,
        symbol: symbol.clone(),
        bids,
        asks,
    })
}

/// 一次解析的产出：可用读数 + 两种「少了一条」的计数。
#[derive(Debug, Default)]
struct Parsed {
    rates: Vec<MarketSnapshot>,
    /// 已下架或非 USDT 计价合约。
    filtered: usize,
    /// 字段不可用：ctx 为 null、费率缺失/不可解析。数据源异常。
    unusable: usize,
}

/// 保持场所顺序；拒绝长度不一致的数组，防止静默截断残缺的快照。
///
/// `capped` 是 `perpsAtOpenInterestCap` 返回的原始币名（含子交易所前缀）。
#[cfg(test)]
fn parse_response(
    dex: Dex,
    response: &MetaAndAssetCtxs,
    capped: &[String],
    now: DateTime<Utc>,
) -> ArbResult<Parsed> {
    parse_response_with(dex, response, capped, now, Decimal::ZERO)
}

/// `rebate`：Entropy 自返佣（只对 io 生效），见 [`taker_fee_for`]。
fn parse_response_with(
    dex: Dex,
    response: &MetaAndAssetCtxs,
    capped: &[String],
    now: DateTime<Utc>,
    rebate: Decimal,
) -> ArbResult<Parsed> {
    if response.0.universe.len() != response.1.len() {
        return Err(ArbError::venue(
            dex.venue.as_str(),
            format!(
                "meta.universe({}) 与 ctxs({}) 长度不一致",
                response.0.universe.len(),
                response.1.len()
            ),
        ));
    }
    let mut parsed = Parsed {
        rates: Vec::with_capacity(response.0.universe.len()),
        ..Parsed::default()
    };

    for (asset, ctx) in response.0.universe.iter().zip(response.1.iter()) {
        if !is_tradable_perp(dex, asset) {
            parsed.filtered += 1;
            continue;
        }
        let oi_capped = capped.contains(&asset.name);
        // null 不能提前移除，否则后面的币种会错配行情。
        match ctx
            .as_ref()
            .and_then(|ctx| parse_row(dex, asset, ctx, oi_capped, now, rebate))
        {
            Some(rate) => parsed.rates.push(rate),
            None => parsed.unusable += 1,
        }
    }
    Ok(parsed)
}

/// 官方规格明确主 dex 的 HYPE/PURR 为 USDC 计价，不能伪标成 USDT。
fn is_tradable_perp(dex: Dex, asset: &UniverseAsset) -> bool {
    !asset.is_delisted && !dex.is_usdc_quoted(&asset.name)
}

/// 把一个合约的行情转成领域类型。`None` = 这一行不可用。
///
/// 四处都**不猜**：
/// - 费率缺失/不可解析 → 整行丢弃，**绝不回落成 0**（伪造的 0 会凭空造出巨大价差）。
/// - 结算时刻推算不出来 → 丢弃，而不是拿当前时间顶上。
/// - 持仓量缺标记价 → `None`，而不是把币数量当成 USDT 名义填进去。
/// - 子交易所币名缺本 dex 前缀 → 丢弃，而不是把别处的合约记到本 dex 名下。
fn parse_row(
    dex: Dex,
    asset: &UniverseAsset,
    ctx: &AssetCtx,
    oi_capped: bool,
    now: DateTime<Utc>,
    rebate: Decimal,
) -> Option<MarketSnapshot> {
    let base = dex.base_of(&asset.name)?;
    let period_rate = parse_decimal(ctx.funding.as_deref()?)?;
    let next_funding_at = next_hour_boundary(now)?;
    let (max_leverage, maintenance_margin) = margin_of(asset);

    let mark_price = opt_decimal(&ctx.mark_px);
    Some(MarketSnapshot {
        venue: dex.venue,
        symbol: Symbol::perp(&base, QUOTE),
        period_rate,
        interval_h: FUNDING_INTERVAL_H,
        interval_assumed: false,
        next_funding_at,
        // 批量行情没有下一结算时刻，按已核实的小时规则推算。
        next_funding_estimated: true,
        // 基础档费率（上限）；HIP-3 缺倍数时不知道。
        taker_fee: taker_fee_for(
            dex.venue,
            asset.deployer_fee_scale.as_deref(),
            asset.growth_mode.as_deref(),
            rebate,
        ),
        mark_price,
        index_price: opt_decimal(&ctx.oracle_px),
        // 批量接口没有一档价量，不能用中间价、冲击价或标记价伪造可成交盘口。
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt: mark_price
            .zip(opt_decimal(&ctx.open_interest))
            .and_then(|(price, coins)| price.checked_mul(coins)),
        quote_volume_24h: opt_decimal(&ctx.day_ntl_vlm),
        max_leverage,
        maintenance_margin,
        oi_capped,
    })
}

/// (最高杠杆, 维持保证金率)，都是最低档。维持保证金率 = 1 / (2 × 最高杠杆)。
fn margin_of(asset: &UniverseAsset) -> (Option<Decimal>, Option<Decimal>) {
    match asset.max_leverage.filter(|leverage| *leverage > 0) {
        Some(leverage) => {
            let leverage = Decimal::from(leverage);
            (
                Some(leverage),
                Some(Decimal::ONE / (Decimal::TWO * leverage)),
            )
        }
        None => (None, None),
    }
}

/// 下一个整点（UTC）。
///
/// 历史结算记录位于 UTC 整点附近；这里只推算计划时刻，不保证实际到账毫秒数。
fn next_hour_boundary(now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    const HOUR_SECS: i64 = 3600;
    DateTime::from_timestamp((now.timestamp().div_euclid(HOUR_SECS) + 1) * HOUR_SECS, 0)
}

/// 字符串字段 → `Decimal`。缺字段、空串、非法值都是 `None`，不回落成 0。
fn opt_decimal(raw: &Option<String>) -> Option<arb_core::Decimal> {
    raw.as_deref().and_then(parse_decimal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Decimal;

    /// 真实响应片段：`POST https://api.hyperliquid.xyz/info` 的
    /// `{"type":"metaAndAssetCtxs"}`。每个对象取自本次 curl 抓取。
    ///
    /// 下面每个对象都是**逐字**复制，只是把 `universe` / `ctxs` 两个数组裁成四行
    /// （BTC / ETH / MATIC 已下架 / kPEPE），好让断言能一行一行看清楚。
    const BTC_ASSET: &str = r#"{"szDecimals":5,"name":"BTC","maxLeverage":40,"marginTableId":56}"#;
    const ETH_ASSET: &str = r#"{"szDecimals":4,"name":"ETH","maxLeverage":25,"marginTableId":55}"#;
    const MATIC_ASSET: &str =
        r#"{"szDecimals":1,"name":"MATIC","maxLeverage":20,"marginTableId":20,"isDelisted":true}"#;
    const KPEPE_ASSET: &str =
        r#"{"szDecimals":0,"name":"kPEPE","maxLeverage":10,"marginTableId":52}"#;

    const BTC_CTX: &str = r#"{"funding":"0.0000125","openInterest":"42602.10526","prevDayPx":"78015.0","dayNtlVlm":"3042689368.5903396606","premium":"0.0004305414","oraclePx":"81293.0","markPx":"81327.7","midPx":"81328.5","impactPxs":["81328.0","81329.0"],"dayBaseVlm":"37844.09033"}"#;
    const ETH_CTX: &str = r#"{"funding":"0.0000125","openInterest":"1116760.7799999991","prevDayPx":"2498.8","dayNtlVlm":"1607903255.0960111618","premium":"0.0003030533","oraclePx":"2639.8","markPx":"2640.7","midPx":"2640.65","impactPxs":["2640.6","2640.91"],"dayBaseVlm":"618422.5900999999"}"#;
    /// 已下架合约：费率与持仓量是字面 0，`premium`/`midPx`/`impactPxs` 是 `null`。
    const MATIC_CTX: &str = r#"{"funding":"0.0","openInterest":"0.0","prevDayPx":"0.37621","dayNtlVlm":"0.0","premium":null,"oraclePx":"0.3754","markPx":"0.37621","midPx":null,"impactPxs":null,"dayBaseVlm":"0.0"}"#;
    const KPEPE_CTX: &str = r#"{"funding":"0.0000784222","openInterest":"9503644164.0","prevDayPx":"0.003691","dayNtlVlm":"17227580.4344770126","premium":"0.001041938","oraclePx":"0.003839","markPx":"0.003844","midPx":"0.003843","impactPxs":["0.003843","0.003844"],"dayBaseVlm":"4528684053.0"}"#;

    /// 抓取时刻 2026-09-19T13:20:00Z（整点之间）。
    const MID_HOUR: i64 = 1_789_824_000;
    /// 上一个结算整点 13:00:00Z。
    const ON_HOUR: i64 = 1_789_822_800;
    const NEXT_HOUR: i64 = 1_789_826_400;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn dec(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    /// 用给定的 ctxs 拼一份真实形状的响应体。
    fn load(ctxs: &[&str]) -> MetaAndAssetCtxs {
        let body = format!(
            r#"[{{"universe":[{BTC_ASSET},{ETH_ASSET},{MATIC_ASSET},{KPEPE_ASSET}]}},[{}]]"#,
            ctxs.join(",")
        );
        serde_json::from_str(&body).expect("fixture 必须能反序列化")
    }

    fn parse(ctxs: &[&str]) -> Parsed {
        parse_response(MAIN_DEX, &load(ctxs), &[], at(MID_HOUR)).unwrap()
    }

    fn row<'a>(parsed: &'a Parsed, base: &str) -> &'a MarketSnapshot {
        parsed
            .rates
            .iter()
            .find(|rate| rate.symbol.base == base)
            .unwrap_or_else(|| panic!("{base} 应当有读数"))
    }

    #[test]
    fn funding_history_rows_parse_in_ascending_order() {
        // 2026-09-30 实测 `fundingHistory` 的形状：毫秒时间戳，小时费率。
        let rows: Vec<HlFundingRow> = serde_json::from_str(
            r#"[{"coin":"BTC","fundingRate":"0.0000125","premium":"-0.0003","time":1790690400028},
                {"coin":"BTC","fundingRate":"0.0000114704","premium":"-0.0004","time":1790686800058}]"#,
        )
        .unwrap();
        let points = parse_funding_rows(rows);
        assert_eq!(points.len(), 2);
        assert!(points[0].at < points[1].at);
        assert_eq!(points[1].rate.to_string(), "0.0000125");
    }

    #[test]
    fn real_response_maps_every_field() {
        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        let btc = row(&parsed, "BTC");

        assert_eq!(btc.venue, Venue::Hyperliquid);
        assert_eq!(btc.symbol.quote, "USDT");
        assert_eq!(btc.period_rate, dec("0.0000125"), "funding 是每期费率");
        assert_eq!(btc.mark_price, Some(dec("81327.7")));
        assert_eq!(btc.index_price, Some(dec("81293.0")), "oraclePx 是指数价");
        assert_eq!(btc.quote_volume_24h, Some(dec("3042689368.5903396606")));
        assert_eq!(btc.interval_h, 1, "Hyperliquid 每小时结算");
        assert!(
            !btc.interval_assumed,
            "周期来自场所明确的全资产小时结算规则"
        );
        assert_eq!(btc.next_funding_at, at(NEXT_HOUR));
        assert!(btc.next_funding_estimated, "下次结算时刻是推算的");
    }

    #[test]
    fn taker_fees_are_the_base_tier_upper_bound_with_hip3_scale_growth_and_capped_rebate() {
        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        assert_eq!(
            row(&parsed, "BTC").taker_fee,
            Some(dec("0.00045")),
            "主 dex 基础档"
        );
        // HIP-3：倍数 1.0 → ×2；growth mode → ×0.1。xyz:TSLA 开着 growth，xyz:GOLD 没开。
        let response = load_dex(&[XYZ_TSLA, XYZ_GOLD]);
        let parsed = parse_response(XYZ_DEX, &response, &[], at(MID_HOUR)).unwrap();
        assert_eq!(row(&parsed, "TSLA").taker_fee, Some(dec("0.00009")));
        assert_eq!(row(&parsed, "XAU").taker_fee, Some(dec("0.0009")));
        // 缺倍数：不知道，不猜。
        assert_eq!(
            taker_fee_for(Venue::HyperliquidXyz, None, None, Decimal::ZERO),
            None
        );
        // 倍数 < 1：1 + scale。
        assert_eq!(
            taker_fee_for(Venue::HyperliquidXyz, Some("0.5"), None, Decimal::ZERO),
            Some(dec("0.000675"))
        );
        // Entropy 返佣只对 io、只按一半份额：200% → 0，100% → 一半；不会变成负的；xyz 不适用。
        let response = load_dex(&[IO_ANTH]);
        let parsed = parse_response_with(IO_DEX, &response, &[], at(MID_HOUR), dec("2")).unwrap();
        assert_eq!(
            row(&parsed, "ANTHROPIC").taker_fee,
            Some(Decimal::ZERO),
            "Tier 4 返佣"
        );
        let io =
            |r: &str| taker_fee_for(Venue::HyperliquidIo, Some("1.0"), Some("enabled"), dec(r));
        assert_eq!(io("0"), Some(dec("0.00009")));
        assert_eq!(io("1"), Some(dec("0.000045")));
        assert_eq!(io("2"), Some(Decimal::ZERO));
        assert_eq!(io("5"), Some(Decimal::ZERO), "超过上限按上限，不出负数");
        assert_eq!(
            taker_fee_for(
                Venue::HyperliquidXyz,
                Some("1.0"),
                Some("enabled"),
                dec("2")
            ),
            Some(dec("0.00009")),
            "返佣只属于 Entropy 部署的 io"
        );
        assert_eq!(
            taker_fee_for(Venue::HyperliquidIo, Some("2.0"), None, dec("2")),
            Some(dec("0.0018")),
            "倍数不是 1 时分成比例未知，不套返佣"
        );
        assert_eq!(
            base_for(Venue::HyperliquidXyz, "xyz:GOLD").as_deref(),
            Some("XAU")
        );
        assert_eq!(
            base_for(Venue::HyperliquidIo, "io:ANTH").as_deref(),
            Some("ANTHROPIC")
        );
        assert_eq!(base_for(Venue::HyperliquidXyz, "io:ANTH"), None);
        assert_eq!(base_for(Venue::Hyperliquid, "HYPE"), None, "USDC 计价");
    }

    #[test]
    fn open_interest_is_converted_from_coins_to_usdt() {
        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        let btc = row(&parsed, "BTC");
        assert_eq!(btc.open_interest_usdt, Some(dec("3464731235.953702")));
        assert_ne!(
            btc.open_interest_usdt,
            Some(dec("42602.10526")),
            "币数量不能直接当成 USDT 名义"
        );

        // 缺标记价时宁可填 None，也不能把币数量当名义填进去。
        let no_mark = ETH_CTX.replace(r#""markPx":"2640.7""#, r#""markPx":null"#);
        let parsed = parse(&[BTC_CTX, &no_mark, MATIC_CTX, KPEPE_CTX]);
        assert_eq!(row(&parsed, "ETH").open_interest_usdt, None);

        // 上游坏数值也不能让整家场所因十进制乘法溢出而 panic。
        let mut response = load(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        response.1[0].as_mut().unwrap().open_interest = Some(Decimal::MAX.to_string());
        let parsed = parse_response(MAIN_DEX, &response, &[], at(MID_HOUR)).unwrap();
        assert_eq!(row(&parsed, "BTC").open_interest_usdt, None);
    }

    #[test]
    fn delisted_market_is_dropped_instead_of_entering_with_a_zero_rate() {
        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);

        assert!(parsed.rates.iter().all(|rate| rate.symbol.base != "MATIC"));
        assert_eq!(parsed.filtered, 1);
        assert_eq!(parsed.unusable, 0);
    }

    #[test]
    fn null_ctx_drops_only_that_row_and_later_rows_stay_aligned() {
        // 本次实测没有整行 null；从真实片段注入 null 专门验证下标不被压缩。
        let parsed = parse(&[BTC_CTX, "null", MATIC_CTX, KPEPE_CTX]);

        assert_eq!(parsed.unusable, 1, "ctx 为 null 的那一行");
        assert_eq!(parsed.filtered, 1, "已下架的 MATIC");
        assert_eq!(
            parsed
                .rates
                .iter()
                .map(|rate| rate.symbol.base.as_str())
                .collect::<Vec<_>>(),
            vec!["BTC", "KPEPE"],
        );
        // 下标对齐：kPEPE 必须拿到自己那一行的价格，而不是被顶到别人那行上。
        let kpepe = row(&parsed, "KPEPE");
        assert_eq!(kpepe.mark_price, Some(dec("0.003844")));
        assert_eq!(kpepe.period_rate, dec("0.0000784222"));
    }

    #[test]
    fn unparseable_rate_drops_the_row_instead_of_faking_zero() {
        for broken in [r#""funding":"n/a""#, r#""funding":"""#] {
            let ctx = ETH_CTX.replace(r#""funding":"0.0000125""#, broken);
            let parsed = parse(&[BTC_CTX, &ctx, MATIC_CTX, KPEPE_CTX]);

            assert_eq!(parsed.unusable, 1, "{broken} 应当让 ETH 整行丢弃");
            assert!(parsed.rates.iter().all(|rate| rate.symbol.base != "ETH"));
            assert_eq!(row(&parsed, "BTC").period_rate, dec("0.0000125"));
        }
    }

    #[test]
    fn mismatched_arrays_are_rejected_instead_of_truncated() {
        let response = load(&[BTC_CTX]);
        assert!(parse_response(MAIN_DEX, &response, &[], at(MID_HOUR)).is_err());
    }

    #[test]
    fn missing_funding_drops_the_row_but_a_reported_zero_is_preserved() {
        let mut ctx: serde_json::Value = serde_json::from_str(BTC_CTX).unwrap();
        ctx.as_object_mut().unwrap().remove("funding");
        let missing = ctx.to_string();
        let zero = ETH_CTX.replace(r#""funding":"0.0000125""#, r#""funding":"0""#);
        let parsed = parse(&[&missing, &zero, MATIC_CTX, KPEPE_CTX]);
        assert!(parsed.rates.iter().all(|rate| rate.symbol.base != "BTC"));
        assert_eq!(row(&parsed, "ETH").period_rate, Decimal::ZERO);
        assert_eq!(parsed.unusable, 1);
    }

    #[test]
    fn usdc_quoted_contracts_are_not_mislabeled_as_usdt() {
        let mut response = load(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        response.0.universe[0].name = "HYPE".into();
        response.0.universe[1].name = "PURR".into();
        let parsed = parse_response(MAIN_DEX, &response, &[], at(MID_HOUR)).unwrap();
        assert_eq!(
            parsed
                .rates
                .iter()
                .map(|rate| rate.symbol.base.as_str())
                .collect::<Vec<_>>(),
            vec!["KPEPE"]
        );
        assert_eq!(parsed.filtered, 3);
    }

    #[test]
    fn next_funding_is_the_next_utc_hour_and_marked_estimated() {
        assert_eq!(next_hour_boundary(at(ON_HOUR)), Some(at(NEXT_HOUR)));
        assert_eq!(next_hour_boundary(at(MID_HOUR)), Some(at(NEXT_HOUR)));
        assert_ne!(
            next_hour_boundary(at(ON_HOUR)),
            Some(at(ON_HOUR)),
            "整点上不能返回当前整点：那个时刻的结算已经发生"
        );

        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        assert!(parsed.rates.iter().all(|rate| rate.next_funding_estimated));
    }

    #[test]
    fn real_impact_and_mid_prices_do_not_become_a_book() {
        // 本次 curl 的 BTC ctx：冲击价和中间价都有值，仍不能推导一档价量。
        let ctx = r#"{"funding":"0.0000125","openInterest":"41207.85306","prevDayPx":"81015.0","dayNtlVlm":"1425675473.6728293896","premium":"0.0002251835","oraclePx":"80378.9","markPx":"80390.0","midPx":"80397.5","impactPxs":["80397.0","80399.5"],"dayBaseVlm":"17554.44756"}"#;
        let parsed = parse(&[ctx, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        let btc = row(&parsed, "BTC");
        assert_eq!(btc.mark_price, Some(dec("80390.0")));
        assert_unknown_book(btc);
    }

    #[test]
    fn missing_price_proxies_preserve_funding_without_fabricating_a_book() {
        let mut ctx: serde_json::Value = serde_json::from_str(BTC_CTX).unwrap();
        let fields = ctx.as_object_mut().unwrap();
        fields.remove("midPx");
        fields.remove("impactPxs");
        let ctx = ctx.to_string();
        let parsed = parse(&[&ctx, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        let btc = row(&parsed, "BTC");
        assert_eq!(btc.period_rate, dec("0.0000125"));
        assert_eq!(btc.mark_price, Some(dec("81327.7")));
        assert_unknown_book(btc);
    }

    #[test]
    fn crossed_impact_prices_do_not_produce_a_crossed_book() {
        // 注入交叉冲击价，确保坏数据也不会通过代理价格变成负价差。
        let mut ctx: serde_json::Value = serde_json::from_str(BTC_CTX).unwrap();
        ctx["impactPxs"] = json!(["81330.0", "81326.0"]);
        let ctx = ctx.to_string();
        let parsed = parse(&[&ctx, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        assert_unknown_book(row(&parsed, "BTC"));
    }

    fn assert_unknown_book(snapshot: &MarketSnapshot) {
        assert_eq!(snapshot.best_bid, None);
        assert_eq!(snapshot.best_ask, None);
        assert_eq!(snapshot.bid_size_usdt, None);
        assert_eq!(snapshot.ask_size_usdt, None);
        // 下游必须看到「穿价成本未知」，而不是错误的零成本。
        assert_eq!(snapshot.relative_spread(), None);
    }

    #[test]
    fn main_dex_leverage_and_maintenance_come_from_max_leverage() {
        let parsed = parse(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]);
        let btc = row(&parsed, "BTC");
        assert_eq!(btc.max_leverage, Some(Decimal::from(40)));
        // 1 / (2 × 40) = 1.25%
        assert_eq!(btc.maintenance_margin, Some(dec("0.0125")));
        assert!(!btc.oi_capped, "没给触顶列表时是「未报告」");

        let parsed = parse_response(
            MAIN_DEX,
            &load(&[BTC_CTX, ETH_CTX, MATIC_CTX, KPEPE_CTX]),
            &["kPEPE".to_string()],
            at(MID_HOUR),
        )
        .unwrap();
        assert!(row(&parsed, "KPEPE").oi_capped, "按原始拼写匹配触顶列表");
        assert!(!row(&parsed, "BTC").oi_capped);
    }

    /// 2026-09-23 `{"type":"metaAndAssetCtxs","dex":"xyz"}` / `"dex":"io"` 的逐字片段。
    const XYZ_TSLA: (&str, &str) = (
        r#"{"szDecimals":3,"name":"xyz:TSLA","maxLeverage":20,"marginTableId":20,"growthMode":"enabled","lastFeeScaleChangeTime":"2025-11-23T17:37:10.033211662","deployerFeeScale":"1.0"}"#,
        r#"{"funding":"0.00000625","openInterest":"120196.39","prevDayPx":"376.67","dayNtlVlm":"11751345.6089400016","premium":"0.0004659328","oraclePx":"378.81","markPx":"378.98","midPx":"378.995","impactPxs":["378.964","379.009"],"dayBaseVlm":"31104.515"}"#,
    );
    const XYZ_GOLD: (&str, &str) = (
        r#"{"szDecimals":4,"name":"xyz:GOLD","maxLeverage":25,"marginTableId":25,"lastFeeScaleChangeTime":"1970-01-01T00:00:00","deployerFeeScale":"1.0"}"#,
        r#"{"funding":"0.00000625","openInterest":"69467.4486","prevDayPx":"4346.0","dayNtlVlm":"47089231.0459600016","premium":"0.0001729585","oraclePx":"4336.3","markPx":"4337.0","midPx":"4337.05","impactPxs":["4337.0","4337.1"],"dayBaseVlm":"10852.8751"}"#,
    );
    const XYZ_CL: (&str, &str) = (
        r#"{"szDecimals":3,"name":"xyz:CL","maxLeverage":20,"marginTableId":20,"growthMode":"enabled","lastFeeScaleChangeTime":"2026-01-06T14:22:02.431616165","deployerFeeScale":"1.0"}"#,
        r#"{"funding":"-0.0000097638","openInterest":"1543429.7040000001","prevDayPx":"93.071","dayNtlVlm":"361325615.2505999207","premium":"-0.0004178273","oraclePx":"89.75","markPx":"89.715","midPx":"89.714","impactPxs":["89.708","89.717"],"dayBaseVlm":"3984702.6669999994"}"#,
    );
    const IO_ANTH: (&str, &str) = (
        r#"{"szDecimals":3,"name":"io:ANTH","maxLeverage":6,"marginTableId":6,"onlyIsolated":true,"marginMode":"strictIsolated","growthMode":"enabled","lastFeeScaleChangeTime":"2026-08-19T14:12:26.461176669","deployerFeeScale":"1.0"}"#,
        r#"{"funding":"0.0000215146","openInterest":"16535.858","prevDayPx":"2155.3","dayNtlVlm":"5808396.5986000001","premium":"0.0016391677","oraclePx":"2181.9","markPx":"2185.3","midPx":"2185.65","impactPxs":["2184.533","2186.42"],"dayBaseVlm":"2681.601"}"#,
    );

    fn load_dex(rows: &[(&str, &str)]) -> MetaAndAssetCtxs {
        let assets: Vec<&str> = rows.iter().map(|row| row.0).collect();
        let ctxs: Vec<&str> = rows.iter().map(|row| row.1).collect();
        let body = format!(
            r#"[{{"universe":[{}],"marginTables":[],"collateralToken":0}},[{}]]"#,
            assets.join(","),
            ctxs.join(",")
        );
        serde_json::from_str(&body).expect("fixture 必须能反序列化")
    }

    #[test]
    fn hip3_names_drop_the_dex_prefix_and_apply_only_verified_aliases() {
        let response = load_dex(&[XYZ_TSLA, XYZ_GOLD, XYZ_CL]);
        let parsed = parse_response(XYZ_DEX, &response, &[], at(MID_HOUR)).unwrap();
        let bases: Vec<&str> = parsed
            .rates
            .iter()
            .map(|rate| rate.symbol.base.as_str())
            .collect();
        assert_eq!(
            bases,
            vec!["TSLA", "XAU", "CL"],
            "CL 是原油期货，不能映射成 USO"
        );
        let tsla = row(&parsed, "TSLA");
        assert_eq!(tsla.venue, Venue::HyperliquidXyz);
        assert_eq!(tsla.symbol.quote, "USDT");
        assert_eq!(tsla.period_rate, dec("0.00000625"));
        assert_eq!(tsla.interval_h, 1, "实测 xyz:TSLA 每小时整点结算");
        assert_eq!(tsla.max_leverage, Some(Decimal::from(20)));
        assert_eq!(tsla.maintenance_margin, Some(dec("0.025")));
        assert_eq!(row(&parsed, "XAU").index_price, Some(dec("4336.3")));

        let response = load_dex(&[IO_ANTH]);
        let parsed =
            parse_response(IO_DEX, &response, &["io:ANTH".to_string()], at(MID_HOUR)).unwrap();
        let anth = row(&parsed, "ANTHROPIC");
        assert_eq!(anth.venue, Venue::HyperliquidIo);
        assert!(anth.oi_capped, "子交易所的触顶列表带前缀");
        assert_eq!(anth.max_leverage, Some(Decimal::from(6)));
    }

    #[test]
    fn hip3_rows_without_the_own_prefix_are_dropped_not_adopted() {
        // 主 dex 的币名混进子交易所响应 = 响应不对，不能记到 xyz 名下。
        let response = load_dex(&[(BTC_ASSET, BTC_CTX), XYZ_TSLA]);
        let parsed = parse_response(XYZ_DEX, &response, &[], at(MID_HOUR)).unwrap();
        assert_eq!(parsed.rates.len(), 1);
        assert_eq!(parsed.unusable, 1);
        assert_eq!(XYZ_DEX.base_of("io:ANTH"), None, "别的 dex 的前缀也不行");
        assert_eq!(XYZ_DEX.base_of("xyz:"), None);
    }

    #[test]
    fn hip3_depth_and_candles_resolve_back_to_the_prefixed_coin() {
        let response = load_dex(&[XYZ_TSLA, XYZ_GOLD]);
        let meta = response.0;
        assert_eq!(
            depth_coin(XYZ_DEX, &meta, &Symbol::perp("XAU", "USDT")).unwrap(),
            "xyz:GOLD"
        );
        assert!(depth_coin(XYZ_DEX, &meta, &Symbol::perp("GOLD", "USDT")).is_err());

        // 真实 l2Book 片段：coin 带前缀，sz 是标的数量。
        let book: L2Book = serde_json::from_str(
            r#"{"coin":"xyz:TSLA","time":1790132146231,"levels":[[{"px":"379.03","sz":"2.638","n":1}],[{"px":"379.05","sz":"1.0","n":1}]]}"#,
        )
        .unwrap();
        let book = parse_depth(XYZ_DEX, book, &Symbol::perp("TSLA", "USDT")).unwrap();
        assert_eq!(book.venue, Venue::HyperliquidXyz);
        assert_eq!(book.bids[0].notional_usdt, dec("999.88114"));
    }

    #[test]
    fn hip3_requests_carry_the_dex_and_main_does_not() {
        assert_eq!(
            XYZ_DEX.body("metaAndAssetCtxs"),
            json!({ "type": "metaAndAssetCtxs", "dex": "xyz" })
        );
        assert_eq!(
            MAIN_DEX.body("perpsAtOpenInterestCap"),
            json!({ "type": "perpsAtOpenInterestCap" })
        );
        assert!(!XYZ_DEX.is_usdc_quoted("HYPE"), "USDC 计价例外只属于主 dex");
    }
}
