//! 实盘券商的连接：解析场所、校验价格保护、从环境变量读凭据逐家连接。
//!
//! `arb-live` 与看板（`arb-web`）共用这一份。两处各写一份，迟早一处多校验了什么、
//! 另一处漏了什么 —— 实盘的安全边界不能有两个版本。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arb_core::{Decimal, Venue};

use crate::arcus_broker::{ArcusBroker, ArcusCredentials};
use crate::aster_broker::{AsterBroker, AsterCredentials};
use crate::binance_broker::{BinanceBroker, BinanceCredentials};
use crate::bitget_broker::{BitgetBroker, BitgetCredentials};
use crate::broker::Broker;
use crate::bybit_broker::{BybitBroker, BybitCredentials};
use crate::cli::parse_venue;
use crate::gate_broker::{GateBroker, GateCredentials};
use crate::hyperliquid_broker::{HyperliquidBroker, HyperliquidDex, HyperliquidOptions};
use crate::lighter_broker::{LighterBroker, LighterConfig, LighterDeployment};
use crate::live_common::LiveOptions;
use crate::mexc_broker::{MexcBroker, MexcCredentials};
use crate::okx_broker::{OkxBroker, OkxCredentials};

/// 已有真实券商实现的场所。Ourbit、Variational 没有可用的官方交易接口（见 README）。
pub const SUPPORTED_VENUES: &[Venue] = &[
    Venue::Arcus,
    Venue::Aster,
    Venue::Binance,
    Venue::Bitget,
    Venue::Bybit,
    Venue::Gate,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
    Venue::Lighter,
    Venue::LighterRh,
    Venue::Mexc,
    Venue::Okx,
];

/// `ARB_LIVE_VENUES` / `--venues` 取这个值（或留空）时，按凭据自动识别要连接的场所。
pub const AUTO_LIVE_VENUES: &str = "auto";

/// 价格保护上限（小数）。超过它就不是「保护」了。
pub const MAX_MARKET_SLIPPAGE: Decimal = Decimal::from_parts(5, 0, 0, false, 2);

/// 连接选项。
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// 各场所订单意图日志所在目录：`<dir>/arb-live-<场所>-orders.jsonl`。
    pub journal_dir: PathBuf,
    /// 真实下单开关。关着时所有券商拒绝一切写操作。
    pub trading_enabled: bool,
    /// 无限价单（平仓 / 回滚 / 减仓）的价格保护，小数。
    pub market_slippage: Option<Decimal>,
}

impl ConnectOptions {
    /// 开启下单必须同时给出合法的价格保护：否则平仓与回滚没有价格上限。
    pub fn validate(&self) -> Result<()> {
        if !self.trading_enabled {
            return Ok(());
        }
        match self.market_slippage {
            None => {
                bail!("开启下单必须同时给出价格保护（market slippage）：否则平仓与回滚没有价格上限")
            }
            Some(s) if s <= Decimal::ZERO || s > MAX_MARKET_SLIPPAGE => {
                bail!("价格保护（market slippage）必须在 (0, {MAX_MARKET_SLIPPAGE}] 之间，收到 {s}")
            }
            Some(_) => Ok(()),
        }
    }

    fn live_options(&self) -> LiveOptions {
        LiveOptions {
            trading_enabled: self.trading_enabled,
            market_slippage: self.market_slippage,
        }
    }
}

// ───────────────────────────── 凭据识别 ─────────────────────────────

/// 每家券商连接时读的环境变量，按连接代码取用的顺序排列。连接（[`connect_venue`]）
/// 与识别（[`credential_status`]）都按这一张表读 —— 两处不会各认一套变量名。
pub fn credential_vars(venue: Venue) -> &'static [&'static str] {
    match venue {
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            &["ARB_HL_PRIVATE_KEY", "ARB_HL_ACCOUNT_ADDRESS"]
        }
        Venue::Lighter => &[
            "ARB_LIGHTER_ACCOUNT_INDEX",
            "ARB_LIGHTER_API_KEY_INDEX",
            "ARB_LIGHTER_API_PRIVATE_KEY",
            "ARB_LIGHTER_SIGNER_LIB",
        ],
        Venue::LighterRh => &[
            "ARB_LIGHTER_RH_ACCOUNT_INDEX",
            "ARB_LIGHTER_RH_API_KEY_INDEX",
            "ARB_LIGHTER_RH_API_PRIVATE_KEY",
            "ARB_LIGHTER_SIGNER_LIB",
        ],
        Venue::Arcus => &[
            "ARB_ARCUS_ADDRESS",
            "ARB_ARCUS_ACCOUNT_INDEX",
            "ARB_ARCUS_API_PRIVATE_KEY",
        ],
        Venue::Binance => &["ARB_BINANCE_API_KEY", "ARB_BINANCE_API_SECRET"],
        Venue::Bybit => &["ARB_BYBIT_API_KEY", "ARB_BYBIT_API_SECRET"],
        Venue::Gate => &["ARB_GATE_API_KEY", "ARB_GATE_API_SECRET"],
        Venue::Mexc => &["ARB_MEXC_API_KEY", "ARB_MEXC_API_SECRET"],
        Venue::Okx => &[
            "ARB_OKX_API_KEY",
            "ARB_OKX_API_SECRET",
            "ARB_OKX_PASSPHRASE",
        ],
        Venue::Bitget => &[
            "ARB_BITGET_API_KEY",
            "ARB_BITGET_API_SECRET",
            "ARB_BITGET_PASSPHRASE",
        ],
        Venue::Aster => &[
            "ARB_ASTER_USER",
            "ARB_ASTER_SIGNER",
            "ARB_ASTER_SIGNER_PRIVATE_KEY",
        ],
        _ => &[],
    }
}

/// 几家共用、不属于某一个账户的变量：只填了它们不算「填了一部分」。
const SHARED_VARS: &[&str] = &["ARB_LIGHTER_SIGNER_LIB"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialState {
    /// 变量齐全，离线格式检查也过了。能不能真连上要等连接时才知道。
    Ready,
    /// 变量齐全，但格式不对（连接时一定失败）。
    Invalid,
    /// 填了一部分。
    Partial,
    /// 一项都没填。
    Missing,
}

/// 一家场所的凭据识别结果。**只有变量名和问题描述，没有任何值、长度或前缀。**
#[derive(Debug, Clone, serde::Serialize)]
pub struct CredentialStatus {
    pub venue: Venue,
    pub state: CredentialState,
    pub vars: Vec<&'static str>,
    pub missing: Vec<&'static str>,
    pub problems: Vec<String>,
    /// `ARB_LIVE_VENUES=auto` 时会不会选入它。Hyperliquid 的 HIP-3 dex（xyz / io）与主站
    /// 共用同一把钥匙，钥匙齐全时三个一起选入：连接只读公开数据（dex 列表、部署者费率倍数、
    /// 合约元数据），不会因为「这个 dex 没用过」而失败。各 dex 的保证金互相独立。
    pub auto_selected: bool,
}

/// 按凭据识别每一家可实盘的场所。`lookup` 取环境变量（测试里换成假的）。
/// 只做离线检查：不联网、不碰钥匙本身之外的任何东西。
pub fn credential_status(lookup: &dyn Fn(&str) -> Option<String>) -> Vec<CredentialStatus> {
    let read = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
    SUPPORTED_VENUES
        .iter()
        .map(|&venue| {
            let vars = credential_vars(venue).to_vec();
            let missing: Vec<_> = vars.iter().copied().filter(|v| read(v).is_none()).collect();
            let problems = if missing.is_empty() {
                format_problems(venue, &|name| read(name).unwrap_or_default())
            } else {
                Vec::new()
            };
            // 共用的变量（Lighter 签名库）填了不代表这家「填了一部分」。
            let own = |list: &[&str]| list.iter().filter(|v| !SHARED_VARS.contains(v)).count();
            let state = if own(&missing) == own(&vars) {
                CredentialState::Missing
            } else if !missing.is_empty() {
                CredentialState::Partial
            } else if !problems.is_empty() {
                CredentialState::Invalid
            } else {
                CredentialState::Ready
            };
            let ready = state == CredentialState::Ready;
            CredentialStatus {
                venue,
                state,
                vars,
                missing,
                problems,
                auto_selected: ready,
            }
        })
        .collect()
}

/// 与连接代码同样的格式要求，提前说出来。问题描述里不带值。
fn format_problems(venue: Venue, value: &dyn Fn(&str) -> String) -> Vec<String> {
    let mut problems = Vec::new();
    match venue {
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            if !is_hex(&value("ARB_HL_PRIVATE_KEY"), 32) {
                problems.push("ARB_HL_PRIVATE_KEY 应是 64 位十六进制私钥（可带 0x）".into());
            }
            if !is_hex(&value("ARB_HL_ACCOUNT_ADDRESS"), 20) {
                problems.push("ARB_HL_ACCOUNT_ADDRESS 应是 0x 开头的 40 位十六进制地址".into());
            }
        }
        Venue::Lighter | Venue::LighterRh => {
            let prefix = if venue == Venue::Lighter {
                "ARB_LIGHTER"
            } else {
                "ARB_LIGHTER_RH"
            };
            if !value(&format!("{prefix}_ACCOUNT_INDEX"))
                .trim()
                .parse::<i64>()
                .is_ok_and(|index| index >= 1)
            {
                problems.push(format!("{prefix}_ACCOUNT_INDEX 应是正整数"));
            }
            if !value(&format!("{prefix}_API_KEY_INDEX"))
                .trim()
                .parse::<u8>()
                .is_ok_and(|index| index != 255)
            {
                problems.push(format!("{prefix}_API_KEY_INDEX 应是 0..=254 的整数"));
            }
            if !Path::new(value("ARB_LIGHTER_SIGNER_LIB").trim()).is_file() {
                problems.push("ARB_LIGHTER_SIGNER_LIB 指向的签名库文件不存在".into());
            }
        }
        Venue::Arcus => {
            if !is_hex(&value("ARB_ARCUS_ADDRESS"), 20) {
                problems.push("ARB_ARCUS_ADDRESS 应是 0x 开头的 40 位十六进制地址".into());
            }
            if !value("ARB_ARCUS_ACCOUNT_INDEX")
                .trim()
                .parse::<u8>()
                .is_ok_and(|index| index <= 9)
            {
                problems.push("ARB_ARCUS_ACCOUNT_INDEX 应是 0 ~ 9 的整数".into());
            }
            if !is_hex(&value("ARB_ARCUS_API_PRIVATE_KEY"), 32) {
                problems.push(
                    "ARB_ARCUS_API_PRIVATE_KEY 应是 64 位十六进制（网页上的 API Signing Key）"
                        .into(),
                );
            }
        }
        _ => {}
    }
    problems
}

fn is_hex(value: &str, bytes: usize) -> bool {
    let value = value.trim();
    let raw = value.strip_prefix("0x").unwrap_or(value);
    raw.len() == bytes * 2 && raw.bytes().all(|b| b.is_ascii_hexdigit())
}

fn env_lookup(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// 实盘要连接的场所，以及它们是怎么来的。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueSelection {
    pub venues: Vec<Venue>,
    /// 按凭据自动识别（而不是 `ARB_LIVE_VENUES` / `--venues` 显式列出）。
    pub auto: bool,
}

/// 解析要连接的场所。`flag` 为空时读 `ARB_LIVE_VENUES`。
///
/// - 显式列表：去重、只接受有真实券商实现的场所，缺凭据时连接那一步报错。
/// - 留空或 `auto`：选入凭据齐全、格式正确的每一家（含 Hyperliquid 的 HIP-3 dex）。
pub fn live_venues(flag: Option<&str>) -> Result<VenueSelection> {
    let raw = match flag {
        Some(raw) => raw.to_string(),
        None => std::env::var("ARB_LIVE_VENUES").unwrap_or_default(),
    };
    if is_auto(&raw) {
        return auto_venues(&credential_status(&env_lookup));
    }
    Ok(VenueSelection {
        venues: explicit_venues(&raw)?,
        auto: false,
    })
}

/// `ARB_LIVE_VENUES` / `--venues` 的这个取值是否表示按凭据自动识别。
pub fn is_auto(raw: &str) -> bool {
    raw.trim().is_empty() || raw.trim().eq_ignore_ascii_case(AUTO_LIVE_VENUES)
}

fn auto_venues(statuses: &[CredentialStatus]) -> Result<VenueSelection> {
    let venues: Vec<Venue> = statuses
        .iter()
        .filter(|status| status.auto_selected)
        .map(|status| status.venue)
        .collect();
    if venues.len() < 2 {
        let found = if venues.is_empty() {
            "按凭据自动识别没有找到凭据齐全的场所".to_string()
        } else {
            let names = venues
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("按凭据自动识别只找到 {names}")
        };
        let incomplete: Vec<String> = statuses
            .iter()
            .filter_map(|status| match status.state {
                CredentialState::Partial => {
                    Some(format!("{} 缺 {}", status.venue, status.missing.join("、")))
                }
                CredentialState::Invalid => {
                    Some(format!("{}：{}", status.venue, status.problems.join("；")))
                }
                _ => None,
            })
            .collect();
        bail!(
            "{found}，至少要两家凭据齐全的场所才能做双腿{}。\
             在 .env 里补全凭据，或用 ARB_LIVE_VENUES 显式指定",
            if incomplete.is_empty() {
                String::new()
            } else {
                format!("（未齐全：{}）", incomplete.join("；"))
            }
        );
    }
    Ok(VenueSelection { venues, auto: true })
}

fn explicit_venues(raw: &str) -> Result<Vec<Venue>> {
    let mut venues = Vec::new();
    for part in raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let venue = parse_venue(part).map_err(anyhow::Error::msg)?;
        if !SUPPORTED_VENUES.contains(&venue) {
            bail!(
                "{venue} 没有真实券商实现；可选：{}",
                SUPPORTED_VENUES
                    .iter()
                    .map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !venues.contains(&venue) {
            venues.push(venue);
        }
    }
    if venues.len() < 2 {
        bail!("至少要连接两家场所才能做双腿");
    }
    Ok(venues)
}

pub fn journal_path(dir: &Path, venue: Venue) -> PathBuf {
    dir.join(format!("arb-live-{}-orders.jsonl", venue.as_str()))
}

/// 从环境变量读凭据，逐家连接。凭据只进构造函数，不打印、不落盘。
///
/// 每家券商会对自己的订单意图日志加独占锁：同一个账户同时只能有一个进程连接。
/// 另一个进程（`arb-live` 或另一个看板）已经连着时，这里直接报错。
pub async fn connect(
    client: &reqwest::Client,
    venues: &[Venue],
    options: &ConnectOptions,
) -> Result<HashMap<Venue, Arc<dyn Broker>>> {
    options.validate()?;
    let mut brokers = HashMap::new();
    for &venue in venues {
        let broker = connect_venue(client, venue, options)
            .await
            .with_context(|| format!("连接 {venue} 账户失败"))?;
        brokers.insert(venue, broker);
    }
    Ok(brokers)
}

async fn connect_venue(
    client: &reqwest::Client,
    venue: Venue,
    options: &ConnectOptions,
) -> Result<Arc<dyn Broker>> {
    let journal = journal_path(&options.journal_dir, venue);
    let broker: Arc<dyn Broker> = match venue {
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            let dex = match venue {
                Venue::Hyperliquid => HyperliquidDex::Main,
                Venue::HyperliquidXyz => HyperliquidDex::Xyz,
                _ => HyperliquidDex::Io,
            };
            let [private_key, account] = secrets(venue)?;
            Arc::new(
                HyperliquidBroker::connect(
                    &private_key,
                    &account,
                    &journal,
                    HyperliquidOptions {
                        trading_enabled: options.trading_enabled,
                        market_slippage: options.market_slippage,
                    },
                    dex,
                )
                .await?,
            )
        }
        Venue::Lighter | Venue::LighterRh => {
            let deployment = match venue {
                Venue::Lighter => LighterDeployment::Mainnet,
                _ => LighterDeployment::Robinhood,
            };
            let [account, key_index, private_key, signer_lib] = secrets(venue)?;
            let names = credential_vars(venue);
            let mut config = LighterConfig::new(
                account
                    .parse()
                    .with_context(|| format!("{} 必须是整数", names[0]))?,
                key_index
                    .parse()
                    .with_context(|| format!("{} 必须是 0..=254 的整数", names[1]))?,
                private_key,
                PathBuf::from(signer_lib),
                journal,
            );
            config.deployment = deployment;
            config.trading_enabled = options.trading_enabled;
            config.market_slippage = options.market_slippage;
            Arc::new(LighterBroker::new(client.clone(), config).await?)
        }
        Venue::Arcus => {
            let [address, account_index, api_private_key] = secrets(venue)?;
            Arc::new(
                ArcusBroker::connect(
                    client.clone(),
                    ArcusCredentials {
                        address,
                        account_index,
                        api_private_key,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Binance => {
            let [api_key, api_secret] = secrets(venue)?;
            Arc::new(
                BinanceBroker::connect(
                    client.clone(),
                    BinanceCredentials {
                        api_key,
                        api_secret,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Bitget => {
            let [api_key, api_secret, passphrase] = secrets(venue)?;
            Arc::new(
                BitgetBroker::connect(
                    client.clone(),
                    BitgetCredentials {
                        api_key,
                        api_secret,
                        passphrase,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Bybit => {
            let [api_key, api_secret] = secrets(venue)?;
            Arc::new(
                BybitBroker::connect(
                    client.clone(),
                    BybitCredentials {
                        api_key,
                        api_secret,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Okx => {
            let [api_key, api_secret, passphrase] = secrets(venue)?;
            Arc::new(
                OkxBroker::connect(
                    client.clone(),
                    OkxCredentials {
                        api_key,
                        api_secret,
                        passphrase,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Gate => {
            let [api_key, api_secret] = secrets(venue)?;
            Arc::new(
                GateBroker::connect(
                    client.clone(),
                    GateCredentials {
                        api_key,
                        api_secret,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Mexc => {
            let [api_key, api_secret] = secrets(venue)?;
            Arc::new(
                MexcBroker::connect(
                    client.clone(),
                    MexcCredentials {
                        api_key,
                        api_secret,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        Venue::Aster => {
            let [user, signer, signer_private_key] = secrets(venue)?;
            Arc::new(
                AsterBroker::connect(
                    client.clone(),
                    AsterCredentials {
                        user,
                        signer,
                        signer_private_key,
                    },
                    &journal,
                    options.live_options(),
                )
                .await?,
            )
        }
        other => bail!("{other} 没有真实券商实现"),
    };
    Ok(broker)
}

/// 按 [`credential_vars`] 的顺序读出一家的全部凭据。凭据只进构造函数，不打印、不落盘。
fn secrets<const N: usize>(venue: Venue) -> Result<[String; N]> {
    secrets_from(venue, &env_lookup)
}

fn secrets_from<const N: usize>(
    venue: Venue,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<[String; N]> {
    let names = credential_vars(venue);
    if names.len() != N {
        bail!(
            "内部错误：{venue} 的凭据表有 {} 项，连接代码要 {N} 项",
            names.len()
        );
    }
    let mut values = Vec::with_capacity(N);
    for name in names {
        match lookup(name) {
            Some(value) if !value.trim().is_empty() => values.push(value.trim().to_string()),
            _ => bail!("缺少环境变量 {name}（见 .env.example 的实盘段落）"),
        }
    }
    values
        .try_into()
        .map_err(|_| anyhow::anyhow!("内部错误：{venue} 凭据数量不对"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn options(trading: bool, slippage: Option<Decimal>) -> ConnectOptions {
        ConnectOptions {
            journal_dir: PathBuf::from("."),
            trading_enabled: trading,
            market_slippage: slippage,
        }
    }

    #[test]
    fn trading_requires_a_bounded_slippage_but_read_only_does_not() {
        assert!(options(false, None).validate().is_ok());
        assert!(options(true, None).validate().is_err());
        assert!(options(true, Some(dec!(0))).validate().is_err());
        assert!(options(true, Some(dec!(0.06))).validate().is_err());
        assert!(options(true, Some(dec!(0.003))).validate().is_ok());
    }

    #[test]
    fn live_venues_need_two_supported_venues() {
        assert_eq!(
            live_venues(Some("hyperliquid, lighter,hyperliquid")).unwrap(),
            VenueSelection {
                venues: vec![Venue::Hyperliquid, Venue::Lighter],
                auto: false,
            }
        );
        assert!(live_venues(Some("hyperliquid")).is_err());
        assert!(live_venues(Some("hyperliquid,variational")).is_err());
    }

    const HL_KEY: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const HL_ADDRESS: &str = "0x2222222222222222222222222222222222222222";

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn status(statuses: &[CredentialStatus], venue: Venue) -> &CredentialStatus {
        statuses.iter().find(|s| s.venue == venue).unwrap()
    }

    #[test]
    fn every_live_venue_has_a_credential_table() {
        for &venue in SUPPORTED_VENUES {
            assert!(!credential_vars(venue).is_empty(), "{venue}");
        }
    }

    #[test]
    fn detection_reports_ready_partial_and_missing_without_values() {
        let signer = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
        let env = lookup(&[
            ("ARB_HL_PRIVATE_KEY", HL_KEY),
            ("ARB_HL_ACCOUNT_ADDRESS", HL_ADDRESS),
            ("ARB_LIGHTER_ACCOUNT_INDEX", "12345"),
            ("ARB_LIGHTER_API_KEY_INDEX", "3"),
            ("ARB_LIGHTER_API_PRIVATE_KEY", "secret-lighter-key"),
            ("ARB_LIGHTER_SIGNER_LIB", signer),
            ("ARB_BYBIT_API_KEY", "secret-bybit-key"),
            ("ARB_BYBIT_API_SECRET", "   "),
        ]);
        let statuses = credential_status(&env);
        assert_eq!(statuses.len(), SUPPORTED_VENUES.len());

        let hl = status(&statuses, Venue::Hyperliquid);
        assert_eq!(hl.state, CredentialState::Ready);
        assert!(hl.auto_selected);
        // HIP-3 dex 共用钥匙：钥匙齐全就一起选入。
        for dex in [Venue::HyperliquidXyz, Venue::HyperliquidIo] {
            let row = status(&statuses, dex);
            assert_eq!(row.state, CredentialState::Ready);
            assert!(row.auto_selected, "{dex}");
        }
        assert_eq!(
            status(&statuses, Venue::Lighter).state,
            CredentialState::Ready
        );
        // 只有空白也算没填。
        let bybit = status(&statuses, Venue::Bybit);
        assert_eq!(bybit.state, CredentialState::Partial);
        assert_eq!(bybit.missing, vec!["ARB_BYBIT_API_SECRET"]);
        assert_eq!(
            status(&statuses, Venue::Binance).state,
            CredentialState::Missing
        );
        // 只共用了签名库、自己的变量一项没填：算未配置，不算没填全。
        assert_eq!(
            status(&statuses, Venue::LighterRh).state,
            CredentialState::Missing
        );
        // 结果里绝不能出现任何值。
        let dump = format!("{statuses:?}");
        for value in [
            HL_KEY,
            HL_ADDRESS,
            "12345",
            "secret-lighter-key",
            "secret-bybit-key",
        ] {
            assert!(!dump.contains(value), "泄露了 {value}");
        }

        let selection = auto_venues(&statuses).unwrap();
        assert!(selection.auto);
        assert_eq!(
            selection.venues,
            vec![
                Venue::Hyperliquid,
                Venue::HyperliquidXyz,
                Venue::HyperliquidIo,
                Venue::Lighter
            ]
        );
    }

    #[test]
    fn malformed_credentials_are_flagged_and_not_auto_selected() {
        let env = lookup(&[
            ("ARB_HL_PRIVATE_KEY", "not-a-key"),
            ("ARB_HL_ACCOUNT_ADDRESS", HL_ADDRESS),
            ("ARB_LIGHTER_ACCOUNT_INDEX", "0"),
            ("ARB_LIGHTER_API_KEY_INDEX", "255"),
            ("ARB_LIGHTER_API_PRIVATE_KEY", "k"),
            ("ARB_LIGHTER_SIGNER_LIB", "/nonexistent/signer.so"),
        ]);
        let statuses = credential_status(&env);
        let hl = status(&statuses, Venue::Hyperliquid);
        assert_eq!(hl.state, CredentialState::Invalid);
        assert!(!hl.auto_selected);
        assert_eq!(hl.problems.len(), 1);
        let lighter = status(&statuses, Venue::Lighter);
        assert_eq!(lighter.state, CredentialState::Invalid);
        assert_eq!(lighter.problems.len(), 3);
        assert!(!format!("{statuses:?}").contains("not-a-key"));
    }

    #[test]
    fn auto_selection_needs_two_ready_venues_and_says_what_is_missing() {
        let env = lookup(&[
            ("ARB_BINANCE_API_KEY", "k"),
            ("ARB_BINANCE_API_SECRET", "s"),
            ("ARB_OKX_API_KEY", "k"),
            ("ARB_OKX_API_SECRET", "s"),
        ]);
        let error = auto_venues(&credential_status(&env))
            .unwrap_err()
            .to_string();
        assert!(error.contains("binance"), "{error}");
        assert!(error.contains("okx 缺 ARB_OKX_PASSPHRASE"), "{error}");
        assert!(!auto_venues(&credential_status(&lookup(&[]))).is_ok());
    }

    #[test]
    fn secrets_are_read_in_table_order_and_name_the_missing_variable() {
        let env = lookup(&[("ARB_OKX_API_KEY", "k"), ("ARB_OKX_API_SECRET", " s ")]);
        let error = secrets_from::<3>(Venue::Okx, &env).unwrap_err().to_string();
        assert!(error.contains("ARB_OKX_PASSPHRASE"));
        let env = lookup(&[
            ("ARB_OKX_API_KEY", "k"),
            ("ARB_OKX_API_SECRET", " s "),
            ("ARB_OKX_PASSPHRASE", "p"),
        ]);
        assert_eq!(
            secrets_from::<3>(Venue::Okx, &env).unwrap(),
            ["k".to_string(), "s".to_string(), "p".to_string()]
        );
        assert!(secrets_from::<2>(Venue::Okx, &env).is_err());
    }
}
