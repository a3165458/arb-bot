//! 上市前（Pre-IPO）合约的报价口径。
//!
//! 同名的上市前合约在各家**不是同一个东西**：报价单位一样（约等于「十亿美元估值」），上市时的换算却不同。
//! - Entropy（`hyperliquid-io`，`io:ANTH` / `io:OAI`）：1 美元 = 10 亿美元**完全稀释**估值；上市后
//!   「价格 = 股价 × 完全稀释股数 ÷ 10 亿」，股数含期权、RSU、可转债（docs.entropy.io，2026-11-03 定稿）。
//! - 币安 / Bybit / Lighter 等：按假定股本（10 亿股）报价，上市时按实际股数调整（不含期权、RSU）。
//!
//! 两种口径之间的价差不会收敛，上市换算时会被一次性拉到两种股数之比（某合约 估约 167 亿 / 127 亿 ≈ 1.31）。
//! 曾有一笔多 Lighter / 空 HL-io 的仓位：Entropy 改规则后溢价从约 2% 走到 6%，平仓净亏不少。
//! 所以这类组合**不开仓、不补仓**（自动、手动都一样）；已有的仓位照常监控、照常可以平。
//!
//! 2026-11-04 用户定：上市前（Pre-IPO）与盘前代币（Pre-Market）一律不交易，不管两腿口径是否一样 ——
//! 各家上市换算规则不同、有的没写清楚，换算那一刻价差会跳。识别来源（[`pre_ipo`]）：
//! - 币安 `exchangeInfo`：`underlyingSubType` 含 `Pre-IPO` / `Pre-Market`，或 `underlyingType == PREMARKET`；
//! - MEXC `/contract/detail`：`mc-trade-zone-preipo` / `mc-trade-zone-PreMarket` 板块，或 `preMarket`；
//! - Aster `exchangeInfo`：`underlyingSubType` 含 `pre-launch`；
//! - 内置名单 [`KNOWN`]：Lighter、Entropy（HL-io）、trade.xyz 的接口没有类型标记，靠它兜底；
//!   刚启动、两家的市场列表还没取到时也靠它。
//!
//! 识别结果随市场列表（5 分钟）刷新；某家取列表失败时保留它上一次的结果。

use std::collections::BTreeSet;
use std::sync::Mutex;

use arb_core::Venue;
use serde_json::Value;

/// 2026-11-04 核实过的上市前 / 盘前合约（对外 base）。交易所都没标的只能靠这里：
/// SPACEX、STABLECOINX 只在 Lighter；io:ANTH / io:OAI 在 Entropy。上市换算完成、各家价差收敛后再删。
const KNOWN: &[&str] = &[
    "ANTHROPIC",
    "OPENAI",
    "OURA",
    "MOONSHOT",
    "YMTC",
    "POLYMARKET",
    "SPACEX",
    "STABLECOINX",
    "XTEN",
    "PREDICT",
];

/// 各家标记出来的上市前 / 盘前合约，按场所分开存（每家刷新只换自己那份）。
static FLAGGED: Mutex<Vec<(Venue, BTreeSet<String>)>> = Mutex::new(Vec::new());

/// 记下某家这一轮标记的上市前 / 盘前合约；和上一轮不同时写日志。
pub fn record(venue: Venue, bases: BTreeSet<String>) {
    let mut flagged = FLAGGED.lock().unwrap_or_else(|e| e.into_inner());
    match flagged.iter_mut().find(|(v, _)| *v == venue) {
        Some((_, old)) if *old == bases => return,
        Some((_, old)) => *old = bases.clone(),
        None => flagged.push((venue, bases.clone())),
    }
    tracing::info!(
        "上市前 / 盘前合约识别：{venue} 标记 {}，不开仓、不补仓",
        bases.into_iter().collect::<Vec<_>>().join(",")
    );
}

/// 币安 `exchangeInfo` 里标为上市前 / 盘前的 USDT 永续（对外 base，同 `catalog::binance`）。
pub fn binance_flagged(info: &Value) -> BTreeSet<String> {
    info.get("symbols")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|s| {
            let text = |key: &str| s.get(key).and_then(Value::as_str).unwrap_or("");
            text("underlyingType").eq_ignore_ascii_case("PREMARKET")
                || s.get("underlyingSubType")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .any(|t| {
                        t.eq_ignore_ascii_case("Pre-IPO") || t.eq_ignore_ascii_case("Pre-Market")
                    })
        })
        .filter_map(|s| s.get("baseAsset").and_then(Value::as_str))
        .filter(|base| !base.is_empty())
        .map(str::to_ascii_uppercase)
        .collect()
}

/// Aster `exchangeInfo` 里标为上市前 / 盘前的 USDT 永续（对外 base，同 `catalog::aster`）：
/// `underlyingSubType` 含 `pre-launch`（2026-11-15 实测：某合约、某合约、PREDICT、XTEN 这类）。
pub fn aster_flagged(info: &Value) -> BTreeSet<String> {
    info.get("symbols")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|s| {
            s.get("underlyingSubType")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .any(|t| t.eq_ignore_ascii_case("pre-launch"))
        })
        .filter_map(|s| s.get("symbol").and_then(Value::as_str))
        .filter_map(|symbol| symbol.strip_suffix("USDT"))
        .filter(|base| !base.is_empty())
        .map(|base| arb_core::Symbol::perp(base, "USDT").base)
        .collect()
}

/// 这个合约是不是上市前 / 盘前合约；是的话给出不交易的原因。
pub fn pre_ipo(base: &str) -> Option<String> {
    let base = base.trim();
    let source = if KNOWN.iter().any(|k| k.eq_ignore_ascii_case(base)) {
        "内置名单".to_string()
    } else {
        let flagged = FLAGGED.lock().unwrap_or_else(|e| e.into_inner());
        let by: Vec<String> = flagged
            .iter()
            .filter(|(_, bases)| bases.iter().any(|b| b.eq_ignore_ascii_case(base)))
            .map(|(venue, _)| format!("{venue} 标记"))
            .collect();
        if by.is_empty() {
            return None;
        }
        by.join("、")
    };
    Some(format!(
        "{base} 是上市前 / 盘前合约（{source}）：各家上市换算规则不同，价差不一定收敛，不交易（不开仓、不补仓，\
         已有仓位照常监控、可以平）"
    ))
}

/// 开仓、补仓前的统一检查：两腿口径冲突（[`basis_conflict`]）或上市前 / 盘前合约（[`pre_ipo`]）。
pub fn open_block(base: &str, a: Venue, b: Venue) -> Option<String> {
    basis_conflict(base, a, b).or_else(|| pre_ipo(base))
}

/// 按完全稀释估值报价的上市前合约：(场所, 对外 base)。
const FULLY_DILUTED: &[(Venue, &str)] = &[
    (Venue::HyperliquidIo, "ANTHROPIC"),
    (Venue::HyperliquidIo, "OPENAI"),
];

fn fully_diluted(base: &str, venue: Venue) -> bool {
    FULLY_DILUTED
        .iter()
        .any(|(v, name)| *v == venue && name.eq_ignore_ascii_case(base))
}

/// 两条腿的上市换算口径不同时给出原因（不开仓、不补仓）；口径相同或不是这类合约时 `None`。
pub fn basis_conflict(base: &str, a: Venue, b: Venue) -> Option<String> {
    let (fa, fb) = (fully_diluted(base, a), fully_diluted(base, b));
    if fa == fb {
        return None;
    }
    let io = if fa { a } else { b };
    let other = if fa { b } else { a };
    Some(format!(
        "{base} 上市前合约两腿口径不同：{io} 按完全稀释估值报价、上市时按完全稀释股数换算，{other} 按假定股本报价、\
         上市时按实际股数调整，价差不会收敛（实盘遇到过，平仓亏损不小）：不开仓、不补仓"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_pre_ipo_against_any_other_venue_conflicts() {
        for base in ["ANTHROPIC", "anthropic", "OPENAI"] {
            for other in [
                Venue::Lighter,
                Venue::Bitget,
                Venue::Binance,
                Venue::LighterRh,
            ] {
                assert!(basis_conflict(base, Venue::HyperliquidIo, other).is_some());
                assert!(basis_conflict(base, other, Venue::HyperliquidIo).is_some());
            }
        }
    }

    #[test]
    fn same_basis_or_other_symbols_pass() {
        assert_eq!(
            basis_conflict("ANTHROPIC", Venue::Lighter, Venue::Binance),
            None
        );
        assert_eq!(
            basis_conflict("SNDK", Venue::HyperliquidIo, Venue::LighterRh),
            None
        );
        assert_eq!(
            basis_conflict("NVDA", Venue::HyperliquidIo, Venue::Bitget),
            None
        );
    }

    #[test]
    fn known_pre_ipo_symbols_are_blocked_on_any_pair() {
        for base in ["OURA", "oura", "MOONSHOT", "SPACEX", "STABLECOINX"] {
            let reason = open_block(base, Venue::Binance, Venue::Mexc).unwrap();
            assert!(reason.contains("上市前"), "{reason}");
        }
        assert!(open_block("NVDA", Venue::Binance, Venue::Mexc).is_none());
        assert!(
            open_block("SHEIN", Venue::Lighter, Venue::Mexc).is_none(),
            "已上市转股票"
        );
    }

    #[test]
    fn binance_flags_pre_ipo_and_pre_market() {
        // 2026-11-04 `exchangeInfo` 的真实片段（字段截取）。
        let info = serde_json::json!({"symbols": [
            {"symbol": "OURAUSDT", "baseAsset": "OURA", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "PREMARKET", "underlyingSubType": ["Pre-IPO", "TradFi"]},
            {"symbol": "PREDICTUSDT", "baseAsset": "PREDICT", "contractType": "PERPETUAL", "underlyingType": "COIN", "underlyingSubType": ["Pre-Market"]},
            {"symbol": "NVDAUSDT", "baseAsset": "NVDA", "contractType": "TRADIFI_PERPETUAL", "underlyingType": "EQUITY", "underlyingSubType": ["TradFi"]},
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "contractType": "PERPETUAL", "underlyingType": "COIN", "underlyingSubType": ["PoW"]}
        ]});
        let flagged = binance_flagged(&info);
        assert_eq!(
            flagged.into_iter().collect::<Vec<_>>(),
            vec!["OURA".to_string(), "PREDICT".to_string()]
        );
    }

    #[test]
    fn newly_flagged_symbols_are_blocked_until_the_venue_drops_the_flag() {
        // 用不会出现在别的测试里的名字：识别表是进程级的。
        let base = "ZZPREIPOTEST";
        assert!(pre_ipo(base).is_none());
        record(Venue::Binance, BTreeSet::from([base.to_string()]));
        record(Venue::Mexc, BTreeSet::from([base.to_string()]));
        let reason = open_block(base, Venue::Lighter, Venue::Bitget).unwrap();
        assert!(reason.contains("标记"), "{reason}");
        record(Venue::Binance, BTreeSet::new());
        assert!(pre_ipo(base).is_some(), "MEXC 还标着");
        record(Venue::Mexc, BTreeSet::new());
        assert!(pre_ipo(base).is_none(), "两家都不标了就恢复");
    }
}
