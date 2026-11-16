//! 各家场所的市场列表（REST，5 分钟刷新一次）：订阅名、吃单费率、类别、交易时段。纯解析函数单测覆盖。

use std::collections::BTreeMap;

use arb_core::{Decimal, Venue};
use serde_json::Value;

use super::pairs::VenueMarket;

/// Arcus `/v1/markets`：在线永续，`marketDisplayName == "{base}-USD"`。费率是基础档（上限）。
pub fn arcus(markets: &Value, taker: Decimal) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    anyhow::ensure!(
        (Decimal::ZERO..Decimal::new(1, 2)).contains(&taker),
        "Arcus 吃单费率 {taker} 超出合理范围"
    );
    let rows = markets
        .get("markets")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Arcus markets 格式不对"))?;
    let mut out = BTreeMap::new();
    for m in rows {
        if m.get("status").and_then(Value::as_str) != Some("ONLINE")
            || m.get("type").and_then(Value::as_str) != Some("PERPETUAL")
        {
            continue;
        }
        let (Some(base), Some(name)) = (
            m.get("baseAsset").and_then(Value::as_str),
            m.get("marketDisplayName").and_then(Value::as_str),
        ) else {
            continue;
        };
        if name != format!("{base}-USD") {
            continue;
        }
        out.insert(
            base.to_ascii_uppercase(),
            VenueMarket {
                key: name.to_string(),
                taker_fee: Some(taker),
                maker_fee: None,
                category: Some(
                    m.get("category")
                        .and_then(Value::as_str)
                        .unwrap_or("CRYPTO")
                        .to_string(),
                ),
                outside_rth: m.get("isOutsideRth").and_then(Value::as_bool),
                bounds: arb_venues::arcus::trading_bounds(
                    m.get("lowerTradingBound").and_then(Value::as_str),
                    m.get("upperTradingBound").and_then(Value::as_str),
                ),
                size_multiplier: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Arcus 没有在线永续");
    Ok(out)
}

/// Lighter（RH 部署或主网）`/api/v1/orderBookDetails`：活跃永续。`taker_fee` 与扫描器同口径直接当小数
/// （实测 `"0.0000"`）；没报费率的不当 0，记为不知道。
pub fn lighter(venue: Venue, details: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = details
        .get("order_book_details")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("{venue} orderBookDetails 格式不对"))?;
    let mut out = BTreeMap::new();
    for r in rows {
        if r.get("status").and_then(Value::as_str) != Some("active")
            || r.get("market_type")
                .and_then(Value::as_str)
                .is_some_and(|t| t != "perp")
        {
            continue;
        }
        let (Some(symbol), Some(id)) = (
            r.get("symbol").and_then(Value::as_str),
            r.get("market_id").and_then(Value::as_i64),
        ) else {
            continue;
        };
        out.insert(
            symbol.to_ascii_uppercase(),
            VenueMarket {
                key: id.to_string(),
                taker_fee: r
                    .get("taker_fee")
                    .and_then(Value::as_str)
                    .and_then(arb_core::parse_decimal),
                maker_fee: None,
                category: None,
                outside_rth: None,
                bounds: None,
                size_multiplier: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "{venue} 没有活跃永续");
    Ok(out)
}

/// Hyperliquid `meta`（主 dex 或 `dex` = xyz / io）：未下架的合约。费率按
/// [`arb_venues::hyperliquid::taker_fee_for`]（基础档上限、HIP-3 倍数、growth mode、io 的 Entropy 返佣）。
pub fn hyperliquid(
    venue: Venue,
    meta: &Value,
    rebate: Decimal,
) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = meta
        .get("universe")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("{venue} meta 格式不对"))?;
    let mut out = BTreeMap::new();
    for asset in rows {
        if asset
            .get("isDelisted")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        let Some(name) = asset.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(base) = arb_venues::hyperliquid::base_for(venue, name) else {
            continue;
        };
        let fee = arb_venues::hyperliquid::taker_fee_for(
            venue,
            asset.get("deployerFeeScale").and_then(Value::as_str),
            asset.get("growthMode").and_then(Value::as_str),
            rebate,
        );
        out.insert(
            base,
            VenueMarket {
                key: name.to_string(),
                taker_fee: fee,
                maker_fee: None,
                category: None,
                outside_rth: None,
                bounds: None,
                size_multiplier: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "{venue} 没有可交易的永续");
    Ok(out)
}

/// Binance 的费率（小数）：TradFi 永续与加密币永续不同档。按账户实测（2026-10-11，VIP0）：
/// TradFi 吃单 0.04%、挂单 0；加密币吃单 0.05%、挂单 0.02%。挂单 0 可能是活动价，以后要按
/// `commissionRate` 逐个合约读。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BinanceFees {
    pub tradfi_taker: Decimal,
    pub tradfi_maker: Decimal,
    pub crypto_taker: Decimal,
    pub crypto_maker: Decimal,
}

impl Default for BinanceFees {
    fn default() -> Self {
        Self {
            tradfi_taker: Decimal::new(4, 4),
            tradfi_maker: Decimal::ZERO,
            crypto_taker: Decimal::new(5, 4),
            crypto_maker: Decimal::new(2, 4),
        }
    }
}

/// Binance U 本位 `exchangeInfo`：在交易的 USDT 永续（`PERPETUAL` 加密币、`TRADIFI_PERPETUAL` 股票 / 商品 /
/// 指数 …）。订阅名是原生名（`NVDAUSDT`），类别按 `underlyingType`。
pub fn binance(info: &Value, fees: BinanceFees) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = info
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Binance exchangeInfo 格式不对"))?;
    let mut out = BTreeMap::new();
    for s in rows {
        let text = |key: &str| s.get(key).and_then(Value::as_str).unwrap_or("");
        let tradfi = match text("contractType") {
            "PERPETUAL" => false,
            "TRADIFI_PERPETUAL" => true,
            _ => continue,
        };
        if text("status") != "TRADING"
            || text("quoteAsset") != "USDT"
            || text("marginAsset") != "USDT"
        {
            continue;
        }
        let (symbol, base) = (text("symbol"), text("baseAsset"));
        if base.is_empty() || symbol != format!("{base}USDT") {
            continue;
        }
        let category = match text("underlyingType") {
            "COIN" => "CRYPTO",
            "COMMODITY" => "COMMODITIES",
            "INDEX" => "INDICES",
            // EQUITY / HK_EQUITY / KR_EQUITY / CN_EQUITY / PREMARKET / FX：按股票类分时段统计（更保守）。
            _ if tradfi => "EQUITIES",
            _ => "CRYPTO",
        };
        let (taker, maker) = if tradfi {
            (fees.tradfi_taker, fees.tradfi_maker)
        } else {
            (fees.crypto_taker, fees.crypto_maker)
        };
        out.insert(
            base.to_ascii_uppercase(),
            VenueMarket {
                key: symbol.to_string(),
                taker_fee: Some(taker),
                maker_fee: Some(maker),
                category: Some(category.to_string()),
                outside_rth: None,
                bounds: None,
                size_multiplier: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Binance 没有在交易的 USDT 永续");
    Ok(out)
}

/// Aster 股票 / ETF / 商品类（官方叫 RWA）吃单费率：1.25 bps，所有 VIP 档一样、挂单 0
/// （docs.asterdex.com「Fees」，2026-09-07 起；2026-11-15 核对）。`commissionRate` 要签名，接了账户后按它逐个读。
pub const ASTER_RWA_TAKER_FEE: Decimal = Decimal::from_parts(125, 0, 0, false, 6);

/// Aster 合约的类别（`underlyingSubType`；`underlyingType` 全是 `某合约`，不能用）。`symbolType == 1` 是
/// 股票 / ETF / 商品类（2026-11-15 实测 121 个 USDT 合约两种标记完全一致），子类缺了时靠它兜底。
fn aster_category(row: &Value) -> &'static str {
    let tagged = |name: &str| {
        row.get("underlyingSubType")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .any(|tag| tag.eq_ignore_ascii_case(name))
    };
    if tagged("Commodities") {
        "COMMODITIES"
    } else if tagged("STOCK")
        || tagged("ETF")
        || row.get("symbolType").and_then(Value::as_i64) == Some(1)
    {
        "EQUITIES"
    } else {
        "CRYPTO"
    }
}

/// Aster `/fapi/v1/exchangeInfo`（币安分叉，格式同币安）：在交易的 USDT 永续（过滤与命名同扫描，见
/// [`arb_venues::aster`]；`USD1` / `USDC` 计价的不列）。订阅名是原生名（`NVDAUSDT`），盘口量是标的数量。
///
/// 费率：股票 / ETF / 商品类统一 [`ASTER_RWA_TAKER_FEE`]。加密币分两档（普通 0.04%、Group B 0.1%，都随 VIP 变），
/// 接口里认不出哪个是 Group B，所以不给费率（这一行不出净收益），免得把 0.1% 的算成 0.04%。
pub fn aster(info: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = info
        .get("symbols")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Aster exchangeInfo 格式不对"))?;
    let mut out = BTreeMap::new();
    for s in rows {
        let text = |key: &str| s.get(key).and_then(Value::as_str).unwrap_or("");
        if text("contractType") != "PERPETUAL"
            || text("status") != "TRADING"
            || text("quoteAsset") != "USDT"
            || text("marginAsset") != "USDT"
        {
            continue;
        }
        let symbol = text("symbol");
        let Some(base) = symbol.strip_suffix("USDT").filter(|base| !base.is_empty()) else {
            continue;
        };
        let category = aster_category(s);
        let taker = (category != "CRYPTO").then_some(ASTER_RWA_TAKER_FEE);
        out.insert(
            arb_core::Symbol::perp(base, "USDT").base,
            watched(symbol, taker, Some(category)),
        );
    }
    anyhow::ensure!(!out.is_empty(), "Aster 没有在交易的 USDT 永续");
    Ok(out)
}

/// 观察场所的一个市场（费率、类别之外都是默认值）。
fn watched(key: &str, taker_fee: Option<Decimal>, category: Option<&str>) -> VenueMarket {
    VenueMarket {
        key: key.to_string(),
        taker_fee,
        maker_fee: None,
        category: category.map(str::to_string),
        outside_rth: None,
        bounds: None,
        size_multiplier: None,
    }
}

/// Gate `/futures/usdt/contracts`：在交易的 USDT 线性永续（过滤与命名同扫描，见
/// [`arb_venues::gate::linear_perps`]）。盘口量是张数，要乘面值；没有面值的换算不了，不列。
pub fn gate(contracts: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let mut out = BTreeMap::new();
    for perp in arb_venues::gate::linear_perps(contracts)? {
        let Some(multiplier) = perp.multiplier else {
            continue;
        };
        let mut market = watched(&perp.name, perp.taker_fee, None);
        market.size_multiplier = Some(multiplier);
        out.insert(perp.base, market);
    }
    anyhow::ensure!(!out.is_empty(), "Gate 没有在交易的 USDT 永续");
    Ok(out)
}

/// Bitget `/api/v2/mix/market/contracts?productType=usdt-futures`：`symbolStatus == normal` 的 USDT 永续
/// （`limit_open` 只能平不能开，不列）。命名同扫描（[`arb_venues::bitget::linear_perp_base`]）；
/// `isRwa == YES` 的是股票 / 商品类。盘口量是标的数量。
pub fn bitget(contracts: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = contracts
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Bitget contracts 格式不对"))?;
    let mut out = BTreeMap::new();
    for r in rows {
        let text = |key: &str| r.get(key).and_then(Value::as_str).unwrap_or("");
        if text("symbolType") != "perpetual"
            || text("quoteCoin") != "USDT"
            || text("symbolStatus") != "normal"
        {
            continue;
        }
        let symbol = text("symbol");
        let Some(base) = arb_venues::bitget::linear_perp_base(symbol) else {
            continue;
        };
        let category = if text("isRwa") == "YES" {
            "EQUITIES"
        } else {
            "CRYPTO"
        };
        out.insert(
            arb_core::Symbol::perp(base, "USDT").base,
            watched(
                symbol,
                arb_core::parse_decimal(text("takerFeeRate")),
                Some(category),
            ),
        );
    }
    anyhow::ensure!(!out.is_empty(), "Bitget 没有在交易的 USDT 永续");
    Ok(out)
}

/// MEXC `/contract/detail`：在交易的 USDT 永续。命名同扫描：股票等传统资产用显示名里的代码
/// （`NVIDIA_USDT` → `NVDA`，见 [`arb_venues::mexc::canonical_bases`]），订阅用合约名。盘口量是张数，
/// 乘面值（`contractSize`）才是标的数量。
pub fn mexc(
    contracts: &[arb_venues::mexc::Contract],
) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let mut out = BTreeMap::new();
    for contract in contracts {
        let category = if !contract.tradfi {
            "CRYPTO"
        } else if contract.commodity {
            "COMMODITIES"
        } else {
            "EQUITIES"
        };
        let mut market = watched(&contract.symbol, contract.taker_fee, Some(category));
        market.size_multiplier = Some(contract.contract_size);
        out.insert(arb_core::Symbol::perp(&contract.base, "USDT").base, market);
    }
    anyhow::ensure!(!out.is_empty(), "MEXC 没有在交易的 USDT 永续");
    Ok(out)
}

/// Ondo Perps `/v1/perps/contracts`：在交易的永续（命名同扫描，见 [`arb_venues::ondo`]）。类别按 `tags`。
pub fn ondo(
    contracts: &[arb_venues::ondo::Contract],
) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let mut out = BTreeMap::new();
    for c in contracts.iter().filter(|c| c.tradable()) {
        let category = if c.tags.iter().any(|t| t == "Crypto") {
            "CRYPTO"
        } else if c.tags.iter().any(|t| t == "Index") {
            "INDICES"
        } else if c.tags.iter().any(|t| t == "Commodity") {
            "COMMODITIES"
        } else {
            "EQUITIES"
        };
        out.insert(
            arb_core::Symbol::perp(&c.base_currency, "USDC").base,
            watched(
                &c.market,
                c.taker_fee.as_deref().and_then(arb_core::parse_decimal),
                Some(category),
            ),
        );
    }
    anyhow::ensure!(!out.is_empty(), "Ondo 没有在交易的永续");
    Ok(out)
}

/// PopDEX `/api/v1/config/symbols?category=Futures`：`status == Trading` 的永续；吃单费率取 `/fee-rate` 的 VIP 0。
/// 订阅名保留原样（`kPEPEUSDT`），标的名按扫描的规则（[`arb_venues::popdex::base_of`]）。
pub fn popdex(
    symbols: &Value,
    fees: &[arb_venues::popdex::FeeRow],
) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = symbols
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("PopDEX symbols 格式不对"))?;
    let fees: std::collections::HashMap<&str, Decimal> = fees
        .iter()
        .filter_map(|row| Some((row.symbol.as_str(), row.base_taker()?)))
        .collect();
    let mut out = BTreeMap::new();
    for r in rows {
        let text = |key: &str| r.get(key).and_then(Value::as_str).unwrap_or("");
        if !text("category").eq_ignore_ascii_case("futures") || text("status") != "Trading" {
            continue;
        }
        let symbol = text("symbol");
        let Some(base) = arb_venues::popdex::base_of(symbol) else {
            continue;
        };
        let category = if text("assetClass") == "Crypto" {
            "CRYPTO"
        } else {
            "EQUITIES"
        };
        out.insert(
            base,
            watched(symbol, fees.get(symbol).copied(), Some(category)),
        );
    }
    anyhow::ensure!(!out.is_empty(), "PopDEX 没有在交易的永续");
    Ok(out)
}
