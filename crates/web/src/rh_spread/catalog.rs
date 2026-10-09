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
                category: Some(
                    m.get("category")
                        .and_then(Value::as_str)
                        .unwrap_or("CRYPTO")
                        .to_string(),
                ),
                outside_rth: m.get("isOutsideRth").and_then(Value::as_bool),
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Arcus 没有在线永续");
    Ok(out)
}

/// Lighter RH `/api/v1/orderBookDetails`：活跃永续。`taker_fee` 与扫描器同口径直接当小数（实测 `"0.0000"`）；
/// 没报费率的不当 0，记为不知道。
pub fn lighter(details: &Value) -> anyhow::Result<BTreeMap<String, VenueMarket>> {
    let rows = details
        .get("order_book_details")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Lighter RH orderBookDetails 格式不对"))?;
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
                category: None,
                outside_rth: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "Lighter RH 没有活跃永续");
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
                category: None,
                outside_rth: None,
            },
        );
    }
    anyhow::ensure!(!out.is_empty(), "{venue} 没有可交易的永续");
    Ok(out)
}
