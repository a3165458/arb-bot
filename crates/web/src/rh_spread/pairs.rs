//! 监控哪几组场所，以及每组里有哪些同名合约。
//!
//! 一组 = 两家场所 `a` / `b`。页面上的基差一律是 **(a − b) / 均值**，方向叫 `long_a` / `long_b`。
//! 最早那组 Arcus ↔ Lighter RH 的 `a` = Arcus、`b` = Lighter RH，与之前的 `long_arcus` /
//! `long_lighter`、历史文件里的基差符号完全一致，所以已经攒下的历史照常使用。
//!
//! 同名不等于同一资产（QNT 股票 vs QNT 币）：凡是有一边是 Hyperliquid 的组，两边都必须在扫描器的
//! **同一个身份簇**里才配对（价格分簇，见 `arb_scanner::identity`）。Arcus ↔ Lighter RH 都在
//! Robinhood Chain 上、按 `baseAsset` 对齐，沿用原来的发现逻辑。

use std::collections::{BTreeMap, HashMap, HashSet};

use arb_core::{Decimal, Venue};
use serde::Serialize;

/// 价差监控支持的场所（各有行情 WebSocket 接入）。
pub const SUPPORTED: [Venue; 13] = [
    Venue::Arcus,
    Venue::LighterRh,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
    Venue::Binance,
    Venue::Lighter,
    Venue::Gate,
    Venue::Bitget,
    Venue::Ondo,
    Venue::PopDex,
    Venue::Mexc,
    Venue::Aster,
];

/// 默认观察的场所（`ARB_RH_SPREAD_WATCH`）：还没有实盘账户、想先看看值不值得接的。它们只和
/// [`WATCH_PEERS`] 里正在监控的场所配对，照常算价差、攒历史（之后按历史统计信号次数与扣费后净额），
/// 但**不提醒、不交易**：这些行单独存放，自动交易、下单、提醒读的快照里根本没有它们（见 `Monitor::watch`）。
/// 已经出现在交易组里的场所（有实盘账户了）不再当观察场所。
pub const DEFAULT_WATCH: [Venue; 7] = [
    Venue::Lighter,
    Venue::Gate,
    Venue::Bitget,
    Venue::Ondo,
    Venue::PopDex,
    Venue::Mexc,
    Venue::Aster,
];

/// 观察场所和谁配对：链上这几家（自动交易主要做的）。观察场所彼此也不配（要两家都接了实盘才用得上）。
pub const WATCH_PEERS: [Venue; 4] = [
    Venue::Arcus,
    Venue::LighterRh,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
];

/// 只观察的场所：它所在的组照常算价差、攒历史，但**不提醒、不自动交易**。现在没有：Binance 原来在这里
/// （只做挂单测算，另有一套币安专用的自动交易开关），2026-11-01 起按用户要求和 Bitget / MEXC 一样当普通
/// 交易组（两腿吃单、通用规则，TradFi 吃单 0.04%）；挂单测算（[`super::maker`]）照常算，只是参考。
pub const OBSERVE_ONLY: [Venue; 0] = [];

/// 没有识别出实盘场所时的默认组：原来的一组 + Hyperliquid 股票子交易所与 RH 链上两家的组合。
pub const DEFAULT_PAIRS: &str = "arcus:lighter-rh,hyperliquid-xyz:lighter-rh,arcus:hyperliquid-xyz,hyperliquid-io:lighter-rh,arcus:hyperliquid-io";

/// 组里谁当 a（基差被减数）：排在前面的。定死这个顺序，已经攒下的历史（`arcus:lighter-rh`、
/// `hyperliquid-xyz:lighter-rh` …）方向不变；新场所只能往后加。
const ORDER: [Venue; 13] = [
    Venue::Arcus,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
    Venue::LighterRh,
    Venue::Binance,
    Venue::Lighter,
    Venue::Gate,
    Venue::Bitget,
    Venue::Ondo,
    Venue::PopDex,
    Venue::Mexc,
    Venue::Aster,
];

/// 按 [`ORDER`] 排好 a / b 的一组。
fn ordered(x: Venue, y: Venue) -> Pair {
    let rank = |v: Venue| ORDER.iter().position(|o| *o == v).unwrap_or(usize::MAX);
    if rank(x) <= rank(y) {
        Pair { a: x, b: y }
    } else {
        Pair { a: y, b: x }
    }
}

/// 观察组：`watch` 里每一家（已经在 `pairs` 里的除外）× `pairs` 里出现过的 [`WATCH_PEERS`]。
pub fn watch_pairs(pairs: &[Pair], watch: &[Venue]) -> Vec<Pair> {
    let present: Vec<Venue> = pairs.iter().flat_map(|p| [p.a, p.b]).collect();
    let mut out = Vec::new();
    for &venue in watch {
        if present.contains(&venue) {
            continue;
        }
        for peer in WATCH_PEERS {
            if present.contains(&peer) {
                let pair = ordered(peer, venue);
                if !out.contains(&pair) {
                    out.push(pair);
                }
            }
        }
    }
    out
}

/// 自动两两组合时不带的场所。Hyperliquid 主 dex 吃单费 0.045%/边：和 Arcus 一组往返就要 0.135%，
/// 比这些组常见的价差还大，做价差划不来（2026-10-09 起不做）。它照常用于资金费套利与实盘；
/// 真要监控它的价差，用 `ARB_RH_SPREAD_PAIRS` 显式写上。HIP-3 的 xyz / io 费率另算，照常组合。
const AUTO_EXCLUDED: [Venue; 1] = [Venue::Hyperliquid];

/// 观察场所里有实盘账户就自动配成交易组的：Lighter 主网和 Lighter RH 用同一套券商代码（只差部署），
/// 下单、对账路径已经跟着 RH 跑过；Bitget（经典账户）2026-10-21 补齐了缓存、资金费 / 成交流水、
/// 可用保证金、补保证金，股票类合约休市时不开不平（见 `arb_exec::bitget_broker`）；MEXC 2026-10-22 实盘往返
/// 通过（NVDA），10-26 补齐补保证金、数量步长，费率按 API 账户实际的 0.08%（见 `arb_venues::mexc::API_TAKER_FEE`），
/// 股票类合约周末实测照常交易。Gate 的下单还没和价差自动交易一起验证过（盘口是张数 × 乘数换算的），有凭据也只观察。
/// Aster 2026-11-15 起只观察：下单模块（`arb_exec::aster_broker`）还没和价差自动交易一起验证过，有凭据也只观察。
const WATCH_TRADABLE: [Venue; 3] = [Venue::Lighter, Venue::Bitget, Venue::Mexc];

/// 自动两两组合时不配在一起的两家。Bitget × MEXC：两家合约表重合七百多个，加上它 Bitget 一条行情连接要订阅
/// 八百多个盘口，2026-10-26 上线后 Bitget 服务端立刻反复重置连接（Bitget 所有组一起断）；两家吃单费加起来
/// 往返 0.28%～0.32%，这一组本来也很难有划算的价差。
const NOT_PAIRED: [(Venue, Venue); 1] = [(Venue::Bitget, Venue::Mexc)];

/// 这些场所里价差监控支持的、两两组合的全部组（顺序见 [`ORDER`]；跳过 [`AUTO_EXCLUDED`] 与 [`NOT_PAIRED`]）。不到两家为空。[`DEFAULT_WATCH`] 里的场所除了 [`WATCH_TRADABLE`]，即使有
/// 实盘凭据也不在这里自动配成交易组（只观察）：要交易得用 `ARB_RH_SPREAD_PAIRS` 显式写上。
pub fn all_pairs(venues: &[Venue]) -> Vec<Pair> {
    let watch_only = |v: &Venue| DEFAULT_WATCH.contains(v) && !WATCH_TRADABLE.contains(v);
    let present: Vec<Venue> = ORDER
        .into_iter()
        .filter(|v| venues.contains(v) && !AUTO_EXCLUDED.contains(v) && !watch_only(v))
        .collect();
    let mut out = Vec::new();
    for (i, &a) in present.iter().enumerate() {
        for &b in &present[i + 1..] {
            if NOT_PAIRED.contains(&(a, b)) || NOT_PAIRED.contains(&(b, a)) {
                continue;
            }
            out.push(Pair { a, b });
        }
    }
    out
}

/// 一组场所。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct Pair {
    pub a: Venue,
    pub b: Venue,
}

impl Pair {
    pub const RH: Pair = Pair {
        a: Venue::Arcus,
        b: Venue::LighterRh,
    };

    /// 稳定标识（页面、历史文件、自动交易设置都用它）：`arcus:lighter-rh`。
    pub fn id(self) -> String {
        format!("{}:{}", self.a, self.b)
    }

    /// 历史文件名前缀。最早那组沿用 `rh-spread`，其它组带上组名。
    pub fn file_prefix(self) -> String {
        if self == Pair::RH {
            "rh-spread".into()
        } else {
            format!("spread-{}-{}", self.a, self.b)
        }
    }

    pub fn parse(raw: &str) -> Result<Pair, String> {
        let (a, b) = raw
            .trim()
            .split_once(':')
            .ok_or_else(|| format!("组 {raw:?} 要写成 场所A:场所B"))?;
        let venue = |name: &str| {
            Venue::parse(name)
                .filter(|v| SUPPORTED.contains(v))
                .ok_or_else(|| {
                    format!(
                        "价差监控不支持场所 {name:?}（支持：{}）",
                        SUPPORTED.map(Venue::as_str).join("、")
                    )
                })
        };
        let pair = Pair {
            a: venue(a)?,
            b: venue(b)?,
        };
        if pair.a == pair.b {
            return Err(format!("组 {raw:?} 的两家是同一个场所"));
        }
        Ok(pair)
    }

    /// 解析逗号分隔的组列表；去重（同一组反过来写也算重复）。
    pub fn parse_list(raw: &str) -> Result<Vec<Pair>, String> {
        let mut out: Vec<Pair> = Vec::new();
        for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let pair = Pair::parse(part)?;
            if out
                .iter()
                .any(|p| (p.a, p.b) == (pair.a, pair.b) || (p.a, p.b) == (pair.b, pair.a))
            {
                return Err(format!("组 {part:?} 重复"));
            }
            out.push(pair);
        }
        if out.is_empty() {
            return Err("至少要监控一组".into());
        }
        Ok(out)
    }

    /// 有一边不在 Robinhood Chain 上（Hyperliquid、Binance、观察场所）：要按扫描器的身份簇核对同名合约
    /// （某合约 / 某合约 / QNT 这类名字在加密币与股票里都有）。只有 Arcus ↔ Lighter RH 按 `baseAsset` 对齐。
    pub fn needs_identity_check(self) -> bool {
        [self.a, self.b]
            .iter()
            .any(|v| !matches!(v, Venue::Arcus | Venue::LighterRh))
    }

    /// 这组只观察：不提醒、不自动交易（见 [`OBSERVE_ONLY`]）。
    pub fn observe_only(self) -> bool {
        OBSERVE_ONLY.contains(&self.a) || OBSERVE_ONLY.contains(&self.b)
    }

    /// 这组里挂单测算的那一家（Binance），没有为 `None`。
    pub fn maker_venue(self) -> Option<Venue> {
        [self.a, self.b]
            .into_iter()
            .find(|venue| *venue == Venue::Binance)
    }
}

/// 一个场所上的一个市场：它在行情订阅里的名字与交易时段信息。
#[derive(Debug, Clone, PartialEq)]
pub struct VenueMarket {
    /// 订阅与盘口索引用的名字：Lighter 的 market_id、Arcus 的 `SPY-USD`、Hyperliquid 的 `xyz:TSLA`、
    /// Gate 的 `NVDA_USDT`、Bitget / PopDEX / Aster 的 `NVDAUSDT`、Ondo 的 `NVDA-USD.P`。
    pub key: String,
    /// 单边吃单费率（小数）。`None` = 不知道（这一行不出净收益）。
    pub taker_fee: Option<Decimal>,
    /// 单边挂单费率（小数）。只有挂单测算的场所（Binance）有。
    pub maker_fee: Option<Decimal>,
    /// 类别：`EQUITIES` / `INDICES` / `COMMODITIES` / `CRYPTO`；不知道为 `None`。
    pub category: Option<String>,
    /// Arcus 的 `isOutsideRth`（节假日、提前收盘）。
    pub outside_rth: Option<bool>,
    /// Arcus 的成交价限制带：带外的挂单成交不了，算报价前剔掉。没有 / 不知道为 `None`。
    pub bounds: Option<arb_venues::arcus::TradingBounds>,
    /// 行情里的量要乘它才是标的数量（Gate 的盘口量是张数）。`None` = 本来就是标的数量。
    pub size_multiplier: Option<Decimal>,
}

/// 每家场所：base → 市场。
pub type Catalog = HashMap<Venue, BTreeMap<String, VenueMarket>>;

/// 一组里的一个同名合约。
#[derive(Debug, Clone, PartialEq)]
pub struct PairMarket {
    pub base: String,
    pub a: VenueMarket,
    pub b: VenueMarket,
    pub category: String,
    pub outside_rth: Option<bool>,
}

impl PairMarket {
    pub fn crypto(&self) -> bool {
        self.category == "CRYPTO"
    }

    /// 往返两腿手续费（%）：两家各开平一次。任一边不知道为 `None`。
    pub fn round_trip_pct(&self) -> Option<Decimal> {
        Some((self.a.taker_fee? + self.b.taker_fee?) * Decimal::TWO * Decimal::ONE_HUNDRED)
    }
}

/// 扫描器的身份簇：(场所, base) → 簇编号。同一个簇 = 价格核实过的同一资产。
pub type Identity = HashMap<(Venue, String), usize>;

/// 从最近一轮扫描里取身份簇。被可信度筛查排除的读数不算（排除的往往正是同名不同资产）。
pub fn identity_of(report: &arb_scanner::ScanReport) -> Identity {
    let excluded: HashSet<(Venue, String)> = report
        .suspicious
        .iter()
        .chain(report.unverified.iter())
        .map(|row| (row.venue, row.symbol.base.clone()))
        .collect();
    let mut out = Identity::new();
    for (index, view) in report.symbols.iter().enumerate() {
        for rate in &view.rates {
            let key = (rate.venue, rate.symbol.base.clone());
            if !excluded.contains(&key) {
                out.insert(key, index);
            }
        }
    }
    out
}

/// 一组的同名合约。`identity` 为 `None` 时（扫描还没出来）跳过需要核对身份的组。
///
/// 类别：Arcus 报了就用 Arcus 的；否则另一边报了用另一边；都没有时按 Hyperliquid 主 dex = 加密币、
/// 其它 = 股票类（股票类会按纽约时段分开统计正常基差，是更保守的一侧）。
pub fn pair_markets(
    pair: Pair,
    catalog: &Catalog,
    identity: Option<&Identity>,
    equities_only: bool,
) -> Vec<PairMarket> {
    let (Some(a), Some(b)) = (catalog.get(&pair.a), catalog.get(&pair.b)) else {
        return Vec::new();
    };
    if pair.needs_identity_check() && identity.is_none() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for (base, market_a) in a {
        let Some(market_b) = b.get(base) else {
            continue;
        };
        if pair.needs_identity_check() {
            let identity = identity.expect("上面检查过");
            match (
                identity.get(&(pair.a, base.clone())),
                identity.get(&(pair.b, base.clone())),
            ) {
                (Some(x), Some(y)) if x == y => {}
                _ => continue,
            }
        }
        let category = market_a
            .category
            .clone()
            .or_else(|| market_b.category.clone())
            .unwrap_or_else(|| {
                if pair.a == Venue::Hyperliquid || pair.b == Venue::Hyperliquid {
                    "CRYPTO".into()
                } else {
                    "EQUITIES".into()
                }
            });
        if equities_only && category == "CRYPTO" {
            continue;
        }
        // Binance 的组只看 TradFi（股票 / 商品 / 指数）：挂单 0 费率的是它们；加密币挂单 0.02%，
        // 而且盘口与成交推送最密，占掉监控主循环的大头。
        if pair.maker_venue().is_some() && category == "CRYPTO" {
            continue;
        }
        out.push(PairMarket {
            base: base.clone(),
            a: market_a.clone(),
            b: market_b.clone(),
            outside_rth: market_a.outside_rth.or(market_b.outside_rth),
            category,
        });
    }
    out
}
