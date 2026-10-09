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
pub const SUPPORTED: [Venue; 5] = [
    Venue::Arcus,
    Venue::LighterRh,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
];

/// 没有识别出实盘场所时的默认组：原来的一组 + Hyperliquid 股票子交易所与 RH 链上两家的组合。
pub const DEFAULT_PAIRS: &str = "arcus:lighter-rh,hyperliquid-xyz:lighter-rh,arcus:hyperliquid-xyz,hyperliquid-io:lighter-rh,arcus:hyperliquid-io";

/// 组里谁当 a（基差被减数）：排在前面的。定死这个顺序，已经攒下的历史（`arcus:lighter-rh`、
/// `hyperliquid-xyz:lighter-rh` …）方向不变。
const ORDER: [Venue; 5] = [
    Venue::Arcus,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
    Venue::LighterRh,
];

/// 这些场所里价差监控支持的、两两组合的全部组（顺序见 [`ORDER`]）。不到两家为空。
pub fn all_pairs(venues: &[Venue]) -> Vec<Pair> {
    let present: Vec<Venue> = ORDER.into_iter().filter(|v| venues.contains(v)).collect();
    let mut out = Vec::new();
    for (i, &a) in present.iter().enumerate() {
        for &b in &present[i + 1..] {
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

    /// 有一边是 Hyperliquid：要按扫描器的身份簇核对同名合约。
    pub fn needs_identity_check(self) -> bool {
        [self.a, self.b].iter().any(|v| {
            matches!(
                v,
                Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo
            )
        })
    }
}

/// 一个场所上的一个市场：它在行情订阅里的名字与交易时段信息。
#[derive(Debug, Clone, PartialEq)]
pub struct VenueMarket {
    /// 订阅与盘口索引用的名字：Lighter RH 的 market_id、Arcus 的 `SPY-USD`、Hyperliquid 的 `xyz:TSLA`。
    pub key: String,
    /// 单边吃单费率（小数）。`None` = 不知道（这一行不出净收益）。
    pub taker_fee: Option<Decimal>,
    /// 类别：`EQUITIES` / `INDICES` / `COMMODITIES` / `CRYPTO`；不知道为 `None`。
    pub category: Option<String>,
    /// Arcus 的 `isOutsideRth`（节假日、提前收盘）。
    pub outside_rth: Option<bool>,
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
