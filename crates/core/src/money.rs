//! 十进制金额与费率工具。
//!
//! **金额与费率一律用 `Decimal`，不用 `f64`。** 费率要累加、要按天数摊销、要排序，
//! `0.1 + 0.2 != 0.3` 这类误差会直接变成错误的排名，而且看不出来。

use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

/// 解析交易所返回的十进制字符串。
///
/// 空串、`null`、非法值一律返回 `None`，**绝不返回 0**：
/// 「场所没给这个字段」和「场所给了 0」在费率上是完全相反的含义
/// （Lighter 的吃单费率真值就是 0）。
pub fn parse_decimal(raw: &str) -> Option<Decimal> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<Decimal>().ok()
}

/// 把 JSON number 转成 `Decimal`。
///
/// 显式命名，是为了让调用方意识到这一步经过了 `f64`：只应在来源本身就是 JSON
/// number 时使用（例如 MEXC 的 `takerFeeRate`）。能拿到字符串的地方用
/// [`parse_decimal`]，那条路没有精度损失。
pub fn from_json_f64(value: f64) -> Option<Decimal> {
    if !value.is_finite() {
        return None;
    }
    // JSON number 常带着 0.00020000000000000001 这类噪声，直接透传会一路进到
    // 展示与排名比较里。费率到小数点后 10 位已经远超任何交易所的精度。
    Some(Decimal::from_f64(value)?.round_dp(10).normalize())
}

/// 转百分比（×100）。展示与比较用，不参与计算。
pub fn to_pct(value: Decimal) -> Decimal {
    value * Decimal::from(100u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_invalid_are_none_not_zero() {
        assert_eq!(parse_decimal(""), None);
        assert_eq!(parse_decimal("   "), None);
        assert_eq!(parse_decimal("n/a"), None);
        assert_eq!(parse_decimal("0"), Some(Decimal::ZERO));
    }

    #[test]
    fn json_f64_noise_is_rounded_away() {
        // MEXC 实测会返回这种尾数
        assert_eq!(
            from_json_f64(0.000_200_000_000_01),
            Some(Decimal::new(2, 4))
        );
        assert_eq!(from_json_f64(f64::NAN), None);
        assert_eq!(from_json_f64(f64::INFINITY), None);
    }

    #[test]
    fn pct_scales_by_one_hundred() {
        assert_eq!(to_pct(Decimal::new(5, 4)), Decimal::new(5, 2));
    }
}
