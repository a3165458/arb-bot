//! 保证金模式的共用校验与展示风险口径。全仓不能用逐仓公式承诺强平距离或减仓比例。
use crate::{MarginMode, TaskRules};
use arb_core::Venue;
use arb_scanner::PairRisk;

pub fn warning(mode: MarginMode) -> Option<&'static str> {
    mode.is_cross().then_some("全仓共用账户权益，名义÷杠杆仅为预计初始占用，不是独立保证金；开仓前强平价未知。爆仓保护仅在交易所返回有效强平价时整笔退出，缺数据不能执行，不保证恢复整个账户安全，不使用逐仓减仓公式。自动追加保证金仅限逐仓；纸面全仓不模拟账户级风险。")
}

pub fn leverage(
    venue: Venue,
    value: Option<arb_core::Decimal>,
) -> arb_core::ArbResult<arb_core::Decimal> {
    let value =
        value.ok_or_else(|| arb_core::ArbError::venue(venue.as_str(), "开仓必须指定杠杆"))?;
    if value < arb_core::Decimal::ONE || !value.fract().is_zero() {
        return Err(arb_core::ArbError::venue(
            venue.as_str(),
            "杠杆必须是正整数",
        ));
    }
    Ok(value)
}

pub fn reported_mode(value: &str) -> Option<MarginMode> {
    match value.to_ascii_lowercase().as_str() {
        "isolated" => Some(MarginMode::Isolated),
        "cross" | "crossed" => Some(MarginMode::Cross),
        _ => None,
    }
}

pub fn venue_state(
    mode: Option<MarginMode>,
    liquidation: Option<arb_core::Decimal>,
    margin: Option<arb_core::Decimal>,
) -> crate::VenueLegState {
    crate::VenueLegState {
        margin_mode: mode,
        liquidation_price: liquidation.filter(|value| *value > arb_core::Decimal::ZERO),
        margin_usdt: if mode == Some(MarginMode::Isolated) {
            margin.filter(|value| *value > arb_core::Decimal::ZERO)
        } else {
            None
        },
    }
}

pub fn validate_rules(mode: MarginMode, rules: &TaskRules) -> Result<(), String> {
    if mode.is_cross() && (rules.auto_margin_pct.is_some() || rules.auto_margin_max_usdt.is_some())
    {
        return Err("全仓共用账户保证金，不能启用自动追加逐仓保证金；请明确关闭此规则".into());
    }
    Ok(())
}

/// 全仓开仓前没有可靠的账户级强平价；不伪造距离。其余数值规则照常校验。
pub fn validate_open_rules(
    mode: MarginMode,
    rules: &TaskRules,
    distance: Option<arb_core::Decimal>,
) -> Result<(), String> {
    validate_rules(mode, rules)?;
    if !mode.is_cross() {
        return crate::monitor::validate_rules(rules, distance);
    }
    if let Some(threshold) = rules.liq_protection_pct
        && (threshold <= arb_core::Decimal::ZERO || threshold > arb_core::Decimal::ONE_HUNDRED)
    {
        return Err(
            "全仓爆仓保护门槛必须在 0 ~ 100%（不含 0）；仅在交易所返回有效强平价时执行".into(),
        );
    }
    let mut remaining = rules.clone();
    remaining.liq_protection_pct = None;
    crate::monitor::validate_rules(&remaining, None)
}

/// 场所级能力不是合约/账户级承诺；真实券商开仓前还要验证市场与账户限制。
pub fn supports_cross(venue: Venue) -> bool {
    !matches!(
        venue,
        Venue::HyperliquidIo | Venue::HyperliquidXyz | Venue::Ourbit | Venue::Variational
    )
}

pub fn validate_venues(mode: MarginMode, venues: &[Venue]) -> Result<(), String> {
    if mode.is_cross() {
        for &venue in venues {
            if !supports_cross(venue) {
                return Err(format!(
                    "{venue} 暂不支持通过本机器人全仓开仓；请选择逐仓或更换场所"
                ));
            }
        }
    }
    Ok(())
}

pub fn display_risk(mode: MarginMode, risk: &mut PairRisk) {
    if mode.is_cross() {
        risk.liq_distance_pct = None;
        risk.health = None;
        for leg in [&mut risk.long, &mut risk.short] {
            leg.liq_distance_pct = None;
            leg.health = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn cross_rejects_isolated_topups_and_unsupported_venues() {
        let rules = TaskRules {
            auto_margin_pct: Some(Decimal::TEN),
            ..TaskRules::default()
        };
        assert!(validate_rules(MarginMode::Cross, &rules).is_err());
        assert!(validate_rules(MarginMode::Isolated, &rules).is_ok());
        assert!(validate_venues(MarginMode::Cross, &[Venue::HyperliquidXyz]).is_err());
        assert!(validate_venues(MarginMode::Cross, &[Venue::Binance, Venue::LighterRh]).is_ok());
    }

    #[test]
    fn reported_states_do_not_invent_mode_margin_or_liquidation() {
        assert_eq!(reported_mode("ISOLATED"), Some(MarginMode::Isolated));
        assert_eq!(reported_mode("crossed"), Some(MarginMode::Cross));
        assert_eq!(reported_mode("portfolio"), None);
        let amount = Some(Decimal::TEN);
        for mode in [Some(MarginMode::Cross), None] {
            assert_eq!(venue_state(mode, amount, amount).margin_usdt, None);
        }
        assert_eq!(
            venue_state(Some(MarginMode::Isolated), amount, amount).margin_usdt,
            amount
        );
        assert_eq!(
            venue_state(Some(MarginMode::Cross), Some(Decimal::ZERO), amount).liquidation_price,
            None
        );
    }

    #[test]
    fn leverage_configuration_requires_an_explicit_positive_integer() {
        for value in [
            None,
            Some(Decimal::ZERO),
            Some(Decimal::NEGATIVE_ONE),
            Some(Decimal::new(15, 1)),
        ] {
            assert!(leverage(Venue::Binance, value).is_err());
        }
        assert_eq!(
            leverage(Venue::Binance, Some(Decimal::TEN)).unwrap(),
            Decimal::TEN
        );
    }

    #[test]
    fn modes_parse_strictly_and_old_orders_remain_isolated() {
        assert_eq!("cross".parse::<MarginMode>().unwrap(), MarginMode::Cross);
        assert!("portfolio".parse::<MarginMode>().is_err());
        assert_eq!(MarginMode::default(), MarginMode::Isolated);
    }
}
