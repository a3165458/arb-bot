//! 领域类型：场所、合约、资金费读数。
//!
//! 这里刻意不做归一化、不排名 —— 只描述「一家场所上某个合约的原始读数」。
//! 归一化与排名在 `arb-scanner` 里，因为那些规则会变，而类型不该跟着变。

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

/// 场所拿不到结算周期时回落的默认值（Binance 主流永续周期）。
///
/// 用它的同时必须把 [`MarketSnapshot::interval_assumed`] 置 `true`：4h 合约被当成 8h
/// 会让日化与 APR 低估一半（1h 低估 8 倍），而排名照常算出来，看不出来。
pub const DEFAULT_FUNDING_INTERVAL_H: u32 = 8;

/// 已接入的交易场所。
///
/// 新增场所时必须同时回答两个问题，否则排名会静默失真：
///
/// 1. **结算周期是几小时？** —— 逐合约端点普遍不返回这个字段，必须走批量端点。
/// 2. **公开接口给不给真实吃单费率？** —— 不给就只能回落到配置值，且必须如实标记
///    [`MarketSnapshot::taker_fee`] 为 `None`，不能拿一个猜测值冒充。
///
/// Hyperliquid 的 HIP-3 子交易所（`xyz`、`io`）与 Lighter 的 Robinhood Chain 部署
/// 各记为独立场所：它们各有独立的清算所与保证金，主 dex 的余额撑不住子交易所的仓位，
/// 一条腿在主 dex、另一条在 `xyz` 就是真实的跨场所双腿。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Venue {
    /// Arcus：Robinhood Chain 上的订单簿永续 DEX（USDG 保证金，加密与美股永续）。
    Arcus,
    Aster,
    Binance,
    Bitget,
    Bybit,
    Gate,
    Hyperliquid,
    /// HIP-3 `io` dex（EntropyIO）：Pre-IPO 与少量股票永续，全部逐仓。
    HyperliquidIo,
    /// HIP-3 `xyz` dex：美股、指数、商品、外汇永续。
    HyperliquidXyz,
    Lighter,
    /// Lighter 部署在 Robinhood Chain 上的实例（USDG 保证金，股票与商品永续为主）。
    LighterRh,
    Mexc,
    Okx,
    Ourbit,
    Variational,
}

impl Venue {
    /// 全部已接入场所。用于「默认扫描全部」与配置校验。
    pub const ALL: [Venue; 15] = [
        Venue::Arcus,
        Venue::Aster,
        Venue::Binance,
        Venue::Bitget,
        Venue::Bybit,
        Venue::Gate,
        Venue::Hyperliquid,
        Venue::HyperliquidIo,
        Venue::HyperliquidXyz,
        Venue::Lighter,
        Venue::LighterRh,
        Venue::Mexc,
        Venue::Okx,
        Venue::Ourbit,
        Venue::Variational,
    ];

    /// 稳定标识符。它会出现在配置、JSON 与看板里 —— 改名等于破坏下游。
    pub const fn as_str(self) -> &'static str {
        match self {
            Venue::Arcus => "arcus",
            Venue::Aster => "aster",
            Venue::Binance => "binance",
            Venue::Bitget => "bitget",
            Venue::Bybit => "bybit",
            Venue::Gate => "gate",
            Venue::Hyperliquid => "hyperliquid",
            Venue::HyperliquidIo => "hyperliquid-io",
            Venue::HyperliquidXyz => "hyperliquid-xyz",
            Venue::Lighter => "lighter",
            Venue::LighterRh => "lighter-rh",
            Venue::Mexc => "mexc",
            Venue::Okx => "okx",
            Venue::Ourbit => "ourbit",
            Venue::Variational => "variational",
        }
    }

    /// 去中心化场所。DEX 的成交成本里还有链上 gas，与 CEX 的纯手续费口径不同。
    pub const fn is_dex(self) -> bool {
        matches!(
            self,
            Venue::Arcus
                | Venue::Aster
                | Venue::Hyperliquid
                | Venue::HyperliquidIo
                | Venue::HyperliquidXyz
                | Venue::Lighter
                | Venue::LighterRh
                | Venue::Variational
        )
    }

    /// 解析配置里的场所名。大小写不敏感，但**不做模糊匹配**：
    /// 拼错的场所名应当报错，而不是悄悄少扫一家。
    pub fn parse(raw: &str) -> Option<Venue> {
        let needle = raw.trim().to_ascii_lowercase();
        Venue::ALL.into_iter().find(|v| v.as_str() == needle)
    }
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 序列化成稳定的短名（`"binance"`）。API 与看板都依赖它，不用枚举的 Debug 形式。
impl serde::Serialize for Venue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// 反序列化走 [`Venue::parse`]：**只认稳定短名**，不认 Debug 形式也不做模糊匹配。
/// 台账要靠它重放，认错场所等于把两家的仓位记到一起。
impl<'de> serde::Deserialize<'de> for Venue {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Venue::parse(&raw).ok_or_else(|| serde::de::Error::custom(format!("未知场所 {raw:?}")))
    }
}

/// 线性永续合约的标的，如 `BTC/USDT`。
///
/// 注意：`base`/`quote` 相等**不足以**判定两个读数属于同一份可对冲的合约 ——
/// 不同场所会复用同一个 ticker 指向不同资产。真正的身份判定要做价格分簇，
/// 见 `arb_scanner::identity`。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
}

impl Symbol {
    /// 构造一个永续合约标的。`base` 会被转成大写，`quote` 原样保留 ——
    /// 计价资产的大小写是有意义的（`USDT` 与 `USDC` 不是同一个合约）。
    pub fn perp(base: impl AsRef<str>, quote: impl AsRef<str>) -> Self {
        Self {
            base: base.as_ref().trim().to_ascii_uppercase(),
            quote: quote.as_ref().trim().to_ascii_uppercase(),
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

/// 序列化成 `"BTC/USDT"`：看板与下游脚本都按这个形状消费。
impl serde::Serialize for Symbol {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

/// 反序列化只接受 `BASE/QUOTE`。缺分隔符就报错 —— 猜一个 quote 出来会让
/// 台账里的合约身份悄悄变掉。
impl<'de> serde::Deserialize<'de> for Symbol {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        let (base, quote) = raw
            .split_once('/')
            .ok_or_else(|| serde::de::Error::custom(format!("合约缺少 `/` 分隔符：{raw:?}")))?;
        if base.is_empty() || quote.is_empty() {
            return Err(serde::de::Error::custom(format!("合约名不完整：{raw:?}")));
        }
        Ok(Symbol::perp(base, quote))
    }
}

/// 结算资产族：**同族内的计价资产可以互相配对**。
///
/// 目前只有 `{USDT, USDC}`。这是一个**显式**的兼容决定，不是假装它们相同：
/// - 不这么做的话，USDC 结算的场所（Variational）永远配不上任何 USDT 场所 ——
///   不报错，只是一条机会都出不来，而用户看不出原因。
/// - 跨族配对的机会会被打上 `quote_mismatch` 标记透到 API 与看板。
/// - **换汇成本没有计入手续费模型**（USDC/USDT 长期在 0.5% 以内），所以标记必须
///   展示出来，而不是默默配对。
pub fn settlement_family(quote: &str) -> &str {
    match quote {
        "USDT" | "USDC" => "USD_STABLE",
        other => other,
    }
}

/// 族的展示用计价资产。同一个族里的场所各有各的真实计价资产，
/// 簇的显示名取最主流的那个；每条读数的真实 `Symbol` 不变。
pub fn family_display_quote(family: &str) -> &str {
    match family {
        "USD_STABLE" => "USDT",
        other => other,
    }
}

/// 一家场所在某个合约上的**行情快照**：费率 + 价格 + 盘口 + 费用。
///
/// 它同时喂两条策略：资金费套利用 `period_rate`，跨所价差套利用 `best_bid/best_ask`。
/// 两者是同一笔双腿交易的不同期望来源（一个赚资金费、一个赚基差收敛），所以共用
/// 一份快照，而不是各拉一遍行情。
///
/// 字段分四类，混用会让下游算错：
///
/// - **费率**：`period_rate` 是**每结算周期**的值，既不是日化也不是年化。
/// - **周期**：`interval_h` + `interval_assumed`，归一化的前提。
/// - **价格**：`mark_price`（场内的公允价，用于基差）与 `index_price`（资产真价，用于身份判定）。
/// - **成本**：`taker_fee`（明码费用）与 `best_bid/best_ask`（穿价成本）。
///   两者都是 `None` = 不知道、`Some(0)` = 已知为 0，绝不可混。
#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketSnapshot {
    pub venue: Venue,
    pub symbol: Symbol,

    /// 每结算周期的费率。
    pub period_rate: Decimal,

    /// 结算周期（小时）。
    pub interval_h: u32,

    /// `interval_h` 是不是回落的默认值（场所没给）。面板必须能区分，
    /// 否则「按 8h 折算」这个假设会一直藏着。
    pub interval_assumed: bool,

    /// 下一次结算时刻。
    pub next_funding_at: DateTime<Utc>,

    /// 结算时刻是场所给的还是按周期推算的。
    pub next_funding_estimated: bool,

    /// 场所公开的真实吃单费率。`None` = 不知道，`Some(0)` = 已知为 0。
    pub taker_fee: Option<Decimal>,

    /// 标记价：场内公允价，用于算两腿之间的基差。
    pub mark_price: Option<Decimal>,

    /// 指数价：资产本身的价，用于识别「同名不同资产」的合约。
    pub index_price: Option<Decimal>,

    /// 最优买价 —— 我们**卖出**这一腿能成交的价。
    pub best_bid: Option<Decimal>,

    /// 最优卖价 —— 我们**买入**这一腿能成交的价。
    pub best_ask: Option<Decimal>,

    /// 一档买量（计价币名义）。
    ///
    /// 用来判断「这笔仓位还在一档之内吗」。**没有多档深度就不该估滑点** ——
    /// 编一个数字出来比留白更危险。
    pub bid_size_usdt: Option<Decimal>,

    /// 一档卖量（计价币名义）。
    pub ask_size_usdt: Option<Decimal>,

    pub open_interest_usdt: Option<Decimal>,
    pub quote_volume_24h: Option<Decimal>,

    /// 最低档（最小仓位）允许的最大杠杆。`None` = 批量接口不给。
    ///
    /// 大仓位会落到更高的风险档，允许的杠杆更低；这里只描述最低档，
    /// 所以按它算出的强平距离对大仓位是**乐观**的。
    pub max_leverage: Option<Decimal>,

    /// 最低档的维持保证金率（小数，0.01 = 1%）。`None` = 批量接口不给。
    ///
    /// 强平距离必须用它算：只用「1 / 杠杆」会把强平价算远，
    /// 高杠杆下差距能到几个百分点。拿不到就留空，不拿一个典型值顶上。
    pub maintenance_margin: Option<Decimal>,

    /// 场所报告该合约已触及持仓量上限：此时只能减仓，开不了新仓。
    ///
    /// Hyperliquid（含 HIP-3）直接公开这个信号；Arcus 公开美元上限，按「持仓名义 ≥ 上限」
    /// 推出来。其它场所恒为 `false`，意思是「没有报告触顶」，不是「确认没触顶」。
    pub oi_capped: bool,
}

impl MarketSnapshot {
    /// 参考价：优先指数价，其次标记价。
    ///
    /// 顺序不能反：指数价是资产本身的价格，标记价掺了本场所的资金费预期，
    /// 用标记价做身份判定会把「同一资产在不同场所的正常价差」误判成不同资产。
    pub fn reference_price(&self) -> Option<Decimal> {
        self.index_price.or(self.mark_price)
    }

    /// 相对买卖价差（小数）。两边都有且 `ask > bid` 时才有值。
    ///
    /// 这是**穿价成本**的单价：市价买要吃 `ask`、市价卖只能拿 `bid`。
    pub fn relative_spread(&self) -> Option<Decimal> {
        let (bid, ask) = (self.best_bid?, self.best_ask?);
        // 零价差是合法的（`Some(0)`）；只有交叉盘（ask < bid）说明数据有问题。
        if bid <= Decimal::ZERO || ask < bid {
            return None;
        }
        let mid = (bid + ask) / Decimal::TWO;
        Some((ask - bid) / mid)
    }
}
