//! 运行配置：全部来自环境变量，可选从 `.env` 加载。
//!
//! 所有解析都在这里一次做完并**在启动时校验**。让一个非法的费率活到扫描循环里，
//! 结果是整张排名表静默失真，而不是一条清晰的报错。

use rust_decimal::Decimal;

use crate::error::{ArbError, ArbResult};
use crate::money::parse_decimal;
use crate::types::Venue;

/// 单边手续费率的回落值（万 5）。
///
/// 只是**回落**：交易所公开接口报了真实费率时用真实的。这个值只影响那些
/// 不提供费率的场所（Binance / OKX / Bybit / Aster / Hyperliquid / Variational）。
pub const DEFAULT_FEE_PER_SIDE: &str = "0.0005";

/// 计划持有天数，一次往返的 4 笔手续费摊到这些天上。
///
/// 取 1 天等于假设只持有 1 天 —— 那是把一次性成本当成每日成本，会同时筛掉真实
/// 可盈利的机会、并鼓励高换手。默认 7 来自回测（120 天 / 39 币种 / 5 所）。
pub const DEFAULT_AMORTIZE_DAYS: &str = "7";

/// 费率上限。超过 10% 的单边费率不是费率，是配置写错了。
pub const MAX_FEE_PER_SIDE: Decimal = Decimal::from_parts(1, 0, 0, false, 1);

/// 摊销天数上限。再长就不是「持有期」而是「不打算平仓」了。
pub const MAX_AMORTIZE_DAYS: u32 = 365;

/// 不利入场基差的门槛默认值（%，正数）。
///
/// 实测（11 家场所 / 6464 条机会）入场基差的中位数是 +0.013%，p10 是 -0.224%。
/// 取 0.5% 是 p10 绝对值的 2.2 倍：只挡掉真正逆风的尾部（占全部机会的 3.2%），
/// 不动正常分布。取 0.1% 会砍掉净年化 Top100 的 54% —— 那是在砍噪声，不是砍风险。
pub const DEFAULT_MAX_ENTRY_BASIS_PCT: &str = "0.5";

/// 价差套利的计划持有天数默认值。
///
/// 价差套利赚的是基差收敛（一次性），所以持有期应当等于「预期多久收敛」，
/// 而不是资金费的结算节奏。3 天是个保守的默认值：它只影响年化折算，
/// 不影响基差本身是否值得做。
pub const DEFAULT_SPREAD_HOLD_DAYS: &str = "3";

/// 后台刷新间隔默认值（秒）。
///
/// 一次扫描要并发打十几家公共接口。间隔太短会把上游请求数乘上并发数，
/// 直接换来 429；而资金费是小时级的量，60 秒的新鲜度已经足够。
pub const DEFAULT_SCAN_INTERVAL_SEC: u64 = 60;

/// 双腿各自的默认杠杆。
///
/// 3 倍是按常见维持保证金率（1%）仍落在「健康」档（强平距离 ≥ 20%）的最高整数杠杆：
/// 3 倍约 32%，5 倍约 19%，已经掉进「注意」档。资金费套利的主要死法是价格单边
/// 走出一条腿的强平距离、另一条腿变成裸敞口，所以默认值按强平距离定，不按收益定。
pub const DEFAULT_LEVERAGE: &str = "3";

/// 杠杆上限。各家最高档都不超过 100 倍，再高就是配置写错了。
pub const MAX_LEVERAGE: u32 = 100;

#[derive(Debug, Clone)]
pub struct Settings {
    /// 单边吃单费率的回落值。
    pub fee_per_side: Decimal,
    /// 计划持有天数（摊费口径）。
    pub amortize_days: Decimal,
    /// 要扫描的场所。空 = 全部。
    pub venues: Vec<Venue>,
    /// 单个 HTTP 请求超时。
    pub http_timeout_sec: u64,
    /// 看板监听端口。
    pub http_port: u16,
    /// 后台快照刷新间隔（秒）。
    pub scan_interval_sec: u64,
    /// 一个币种至少要有几家场所才参与排名。
    pub min_venues: usize,
    /// 单笔最大名义额（计价币）。执行层用它做闸门。`None` = 不设上限（`ARB_MAX_POSITION_USDT=off`）：
    /// 这时单笔多大只由盘口深度（限价以内吃得满）、保证金与强平距离决定。
    pub max_position_usdt: Option<f64>,
    /// 单日最大亏损（计价币）。
    pub max_daily_loss_usdt: f64,
    /// 价差套利的计划持有天数（预期基差收敛要多久）。
    pub spread_hold_days: Decimal,
    /// 不利入场基差的上限（%，正数）。`None` = 不设门槛。
    ///
    /// 入场基差是**一次性**项：进场时空腿比多腿便宜多少，一开始就让掉多少。
    /// 只有基差收敛或反向才会变成盈亏，保持不变时净额为零 —— 所以它不该被直接扣进
    /// 日化，但必须在进场前挡住明显逆风的那一批。
    pub max_entry_basis_pct: Option<Decimal>,
    /// 双腿各自的计划杠杆。只影响强平距离与保证金收益率，不影响排名。
    pub leverage: Decimal,
    /// 日志过滤器（`tracing` 的 EnvFilter 语法）。
    pub log_filter: String,
}

impl Settings {
    /// 从环境变量加载。会先尝试读 `.env`（不存在就跳过）。
    pub fn from_env() -> ArbResult<Self> {
        let _ = dotenvy::dotenv();

        let settings = Self {
            fee_per_side: env_decimal("ARB_FEE_PER_SIDE", DEFAULT_FEE_PER_SIDE)?,
            amortize_days: env_decimal("ARB_AMORTIZE_DAYS", DEFAULT_AMORTIZE_DAYS)?,
            venues: parse_venue_list(&std::env::var("ARB_VENUES").unwrap_or_default())?,
            http_timeout_sec: env_u64("ARB_HTTP_TIMEOUT_SEC", 15)?,
            http_port: env_u16("ARB_HTTP_PORT", 8080)?,
            scan_interval_sec: env_u64("ARB_SCAN_INTERVAL_SEC", DEFAULT_SCAN_INTERVAL_SEC)?,
            min_venues: env_usize("ARB_MIN_VENUES", 2)?,
            max_position_usdt: env_optional_f64("ARB_MAX_POSITION_USDT", Some(1_000.0))?,
            max_daily_loss_usdt: env_f64("ARB_MAX_DAILY_LOSS_USDT", 100.0)?,
            spread_hold_days: env_decimal("ARB_SPREAD_HOLD_DAYS", DEFAULT_SPREAD_HOLD_DAYS)?,
            max_entry_basis_pct: env_optional_decimal(
                "ARB_MAX_ENTRY_BASIS_PCT",
                Some(DEFAULT_MAX_ENTRY_BASIS_PCT),
            )?,
            leverage: env_decimal("ARB_LEVERAGE", DEFAULT_LEVERAGE)?,
            log_filter: std::env::var("ARB_LOG").unwrap_or_else(|_| "info".into()),
        };
        settings.validate()?;
        Ok(settings)
    }

    /// 校验取值范围。
    ///
    /// 命令行覆盖之后**必须**再调一次：否则 `--fee 0.5` 会绕过环境变量那条路径的
    /// 检查，让一个荒谬的费率活到排名里。
    pub fn validate(&self) -> ArbResult<()> {
        if !(Decimal::ZERO..=MAX_FEE_PER_SIDE).contains(&self.fee_per_side) {
            return Err(ArbError::OutOfRange {
                what: "单边费率",
                value: self.fee_per_side,
                expected: "0 ~ 0.1",
            });
        }
        if self.amortize_days <= Decimal::ZERO
            || self.amortize_days > Decimal::from(MAX_AMORTIZE_DAYS)
        {
            return Err(ArbError::OutOfRange {
                what: "摊费天数",
                value: self.amortize_days,
                expected: "0 ~ 365（不含 0）",
            });
        }
        if self.min_venues < 2 {
            // 只有一家场所构不成双腿，排名必然是空的。这是配置错误，不是运行时情况。
            return Err(ArbError::config(
                "ARB_MIN_VENUES 至少要 2：套利需要两条腿分处两个场所",
            ));
        }
        if self.http_timeout_sec == 0 {
            return Err(ArbError::config("ARB_HTTP_TIMEOUT_SEC 必须大于 0"));
        }
        if self.scan_interval_sec == 0 {
            return Err(ArbError::config("ARB_SCAN_INTERVAL_SEC 必须大于 0"));
        }
        if self.spread_hold_days <= Decimal::ZERO
            || self.spread_hold_days > Decimal::from(MAX_AMORTIZE_DAYS)
        {
            return Err(ArbError::OutOfRange {
                what: "价差持有天数",
                value: self.spread_hold_days,
                expected: "0 ~ 365（不含 0）",
            });
        }
        if let Some(max) = self
            .max_entry_basis_pct
            .filter(|max| *max < Decimal::ZERO || *max > Decimal::from(100u32))
        {
            return Err(ArbError::OutOfRange {
                what: "入场基差门槛（%）",
                value: max,
                expected: "0 ~ 100",
            });
        }
        if self.leverage < Decimal::ONE || self.leverage > Decimal::from(MAX_LEVERAGE) {
            return Err(ArbError::OutOfRange {
                what: "杠杆",
                value: self.leverage,
                expected: "1 ~ 100",
            });
        }
        Ok(())
    }

    /// 用户是否显式指定了场所白名单。
    pub fn venues_explicit(&self) -> bool {
        !self.venues.is_empty()
    }

    /// 实际要扫描的场所列表。
    pub fn effective_venues(&self) -> Vec<Venue> {
        if self.venues.is_empty() {
            Venue::ALL.to_vec()
        } else {
            self.venues.clone()
        }
    }
}

/// 解析逗号分隔的场所白名单。
///
/// 拼错的场所名直接报错：静默忽略等于「我明明配了 gate，为什么榜单里没有它」。
pub fn parse_venue_list(raw: &str) -> ArbResult<Vec<Venue>> {
    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let venue = Venue::parse(part).ok_or_else(|| {
            ArbError::config(format!(
                "ARB_VENUES 里有无法识别的场所 {part:?}；可选：{}",
                Venue::ALL.map(Venue::as_str).join(", ")
            ))
        })?;
        if !out.contains(&venue) {
            out.push(venue);
        }
    }
    Ok(out)
}

fn env_decimal(name: &str, default: &str) -> ArbResult<Decimal> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => parse_decimal(default)
            .ok_or_else(|| ArbError::config(format!("内置默认值 {default:?} 不是合法十进制数"))),
        Err(error) => Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => parse_decimal(&raw)
            .ok_or_else(|| ArbError::config(format!("{name} 必须是十进制数，收到 {raw:?}"))),
    }
}

/// 解析可关闭的十进制配置。`off` / `none` / `-` / 空串表示关闭（`None`）。
/// 数字，或 `off` / `none` / `-`（空串）表示不设。没配用 `default`。
fn env_optional_f64(name: &str, default: Option<f64>) -> ArbResult<Option<f64>> {
    let raw = match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => raw,
    };
    parse_optional_f64(name, &raw)
}

fn parse_optional_f64(name: &str, raw: &str) -> ArbResult<Option<f64>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || matches!(trimmed.to_ascii_lowercase().as_str(), "off" | "none" | "-") {
        return Ok(None);
    }
    let value: f64 = trimmed
        .parse()
        .map_err(|_| ArbError::config(format!("{name} 必须是数字或 off，收到 {raw:?}")))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(ArbError::config(format!(
            "{name} 必须是正数或 off，收到 {raw:?}"
        )));
    }
    Ok(Some(value))
}

fn env_optional_decimal(name: &str, default: Option<&str>) -> ArbResult<Option<Decimal>> {
    let raw = match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => {
            return Ok(default.and_then(parse_decimal));
        }
        Err(error) => return Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => raw,
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || matches!(trimmed.to_ascii_lowercase().as_str(), "off" | "none" | "-") {
        return Ok(None);
    }
    parse_decimal(trimmed)
        .map(Some)
        .ok_or_else(|| ArbError::config(format!("{name} 必须是十进制数或 off，收到 {raw:?}")))
}

fn env_f64(name: &str, default: f64) -> ArbResult<f64> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => raw
            .trim()
            .parse::<f64>()
            .map_err(|_| ArbError::config(format!("{name} 必须是数字，收到 {raw:?}"))),
    }
}

fn env_u64(name: &str, default: u64) -> ArbResult<u64> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| ArbError::config(format!("{name} 必须是非负整数，收到 {raw:?}"))),
    }
}

fn env_usize(name: &str, default: usize) -> ArbResult<usize> {
    Ok(env_u64(name, default as u64)? as usize)
}

fn env_u16(name: &str, default: u16) -> ArbResult<u16> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(ArbError::config(format!("读取 {name} 失败：{error}"))),
        Ok(raw) => raw
            .trim()
            .parse::<u16>()
            .map_err(|_| ArbError::config(format!("{name} 必须是 0~65535，收到 {raw:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_position_cap_can_be_switched_off_but_never_set_to_nonsense() {
        assert_eq!(parse_optional_f64("X", "off").unwrap(), None);
        assert_eq!(parse_optional_f64("X", " OFF ").unwrap(), None);
        assert_eq!(parse_optional_f64("X", "").unwrap(), None);
        assert_eq!(parse_optional_f64("X", "2500").unwrap(), Some(2500.0));
        for bad in ["0", "-5", "abc", "inf", "NaN"] {
            assert!(parse_optional_f64("X", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn venue_list_is_parsed_case_insensitively_and_deduplicated() {
        assert_eq!(
            parse_venue_list(" Gate ,binance,gate").unwrap(),
            vec![Venue::Gate, Venue::Binance]
        );
        assert!(parse_venue_list("").unwrap().is_empty());
    }

    #[test]
    fn unknown_venue_is_an_error_not_silently_dropped() {
        let err = parse_venue_list("binance,gte").unwrap_err();
        assert!(err.to_string().contains("gte"), "{err}");
    }

    fn settings(fee: Decimal, days: Decimal, min_venues: usize) -> Settings {
        Settings {
            fee_per_side: fee,
            amortize_days: days,
            venues: Vec::new(),
            http_timeout_sec: 15,
            http_port: 8080,
            scan_interval_sec: 60,
            max_position_usdt: Some(1_000.0),
            max_daily_loss_usdt: 100.0,
            spread_hold_days: Decimal::from(3u32),
            min_venues,
            max_entry_basis_pct: Some(Decimal::new(5, 1)),
            leverage: Decimal::from(3u32),
            log_filter: "info".into(),
        }
    }

    #[test]
    fn leverage_is_range_checked() {
        let mut base = settings(Decimal::new(5, 4), Decimal::from(7u32), 2);
        base.leverage = Decimal::new(5, 1);
        assert!(base.validate().is_err(), "低于 1 倍不是杠杆");
        base.leverage = Decimal::from(101u32);
        assert!(base.validate().is_err());
        base.leverage = Decimal::ONE;
        assert!(base.validate().is_ok());
        assert_eq!(parse_decimal(DEFAULT_LEVERAGE), Some(Decimal::from(3u32)));
    }

    #[test]
    fn validate_rejects_values_a_cli_override_could_smuggle_in() {
        assert!(
            settings(Decimal::new(5, 4), Decimal::from(7u32), 2)
                .validate()
                .is_ok()
        );
        assert!(
            settings(Decimal::new(5, 1), Decimal::from(7u32), 2)
                .validate()
                .is_err()
        );
        assert!(
            settings(Decimal::new(5, 4), Decimal::ZERO, 2)
                .validate()
                .is_err()
        );
        assert!(
            settings(Decimal::new(5, 4), Decimal::from(400u32), 2)
                .validate()
                .is_err()
        );
        assert!(
            settings(Decimal::new(5, 4), Decimal::from(7u32), 1)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn the_entry_basis_gate_can_be_closed_and_is_range_checked() {
        let mut base = settings(Decimal::new(5, 4), Decimal::from(7u32), 2);

        base.max_entry_basis_pct = Some(Decimal::new(101, 0));
        assert!(base.validate().is_err());
        base.max_entry_basis_pct = Some(Decimal::new(-5, 1));
        assert!(base.validate().is_err());

        // 关闭门槛必须合法 —— 那是「我自己看，不要你替我挡」的显式选择
        base.max_entry_basis_pct = None;
        assert!(base.validate().is_ok());
    }

    #[test]
    fn defaults_match_the_documented_values() {
        assert_eq!(
            parse_decimal(DEFAULT_FEE_PER_SIDE),
            Some(Decimal::new(5, 4))
        );
        assert_eq!(
            parse_decimal(DEFAULT_AMORTIZE_DAYS),
            Some(Decimal::from(7u32))
        );
    }
}
