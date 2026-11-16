//! Production Hyperliquid **perpetual dex** broker for the first perp dex and the HIP-3
//! builder-deployed dexes `xyz` (`hyperliquid-xyz`) and `io` (`hyperliquid-io`); no spot
//! or vault routing. Each dex is a distinct venue with its own clearinghouse and margin
//! (in standard mode; unified/portfolio accounts keep one spot balance, see `free_collateral`),
//! so one instance trades exactly one dex.
//!
//! Construction is explicit:
//! `HyperliquidBroker::connect(key, account, journal, options, HyperliquidDex::Main)`.
//! `HyperliquidOptions::default()` forbids all exchange writes. Account reads use the
//! actual trading account, not the API-wallet address: Hyperliquid's `/info` is public,
//! address-scoped and does not accept authentication signatures. `/exchange` is signed.
//!
//! Every order is price-bounded IOC. Market-style orders require an explicitly supplied
//! slippage fraction. Quantities never round up; reduce-only quantities must be exact.
//! The separate, exclusively locked intent journal is synced BEFORE transmitting an
//! order. An ambiguous reserved ID is query-only forever, including after restart: never
//! delete/reuse this journal to retry a timeout. Use one API wallet per trading process
//! and one journal file per dex; the identity check rejects records from another dex.
//!
//! Fill amounts and fees come from `userFillsByTime`, not order acknowledgements or fee
//! estimates. Incomplete/expired fill history fails closed. USDC fees follow the existing
//! quanto accounting convention; HYPE/PURR retain their USDC symbol quote.
//!
//! # HIP-3 dexes
//!
//! HIP-3 markets share the same `/info` and `/exchange` endpoints and, for one account,
//! the same fills. The only differences threaded through this broker are the `dex`
//! request field, the `{dex}:{coin}` name prefix, the builder-deployed asset-ID scheme,
//! and the deployer fee scale:
//! - Asset IDs: `100000 + perp_dex_index * 10000 + index_in_meta`, where
//!   `perp_dex_index` is the position in the `perpDexs` response.
//! - Coins carry the dex prefix (`xyz:TSLA`); an instance accepts only its own prefix and
//!   rejects spot, main, and foreign-dex names. Verified aliases mirror
//!   `crates/venues/src/hyperliquid.rs` (xyz `GOLD→XAU`, `SILVER→XAG`; io `ANTH→ANTHROPIC`,
//!   `OAI→OPENAI`).
//! - `updateLeverage.isCross` follows the requested mode. HIP-3 deployments and
//!   mainnet isolated-only metadata reject cross before any signed write.
//! - `fee_per_side` applies the dex's `deployerFeeScale` to the account taker rate using
//!   the official rule (`scale < 1 ? 1 + scale : 2 * scale`). Growth-mode discounts are
//!   per-market and are deliberately NOT applied, so the estimate can only overstate the
//!   fee, never understate it.
//!
//! # Isolated-margin top-up (`Broker::add_margin`)
//!
//! One signed L1 action `{"type":"updateIsolatedMargin","asset":<id>,"isBuy":true,"ntli":<usdc*1e6>}`
//! (same signing/nonce/submit lock as orders, asset ID incl. the HIP-3 formula above).
//! `ntli` is a signed integer with 6 decimals; **positive adds** margin. The official docs only
//! say "amount to add or remove", but the official Python SDK example
//! `basic_leverage_adjustment.py` calls `update_isolated_margin(1, "ETH")` under the comment
//! "Add 1 dollar of extra margin", and the community SDK nktkas/hyperliquid tests `+2e6` as an
//! increase and `-1e6` as a decrease. Because the sign is not in the official reference, success is
//! only ever claimed from a readback, never from the `ok` acknowledgement.
//!
//! - Exactly one write per call, never retried (no idempotency key: a retry could add twice).
//! - The readback compares `position.leverage.rawUsd` (the isolated USD ledger) before and after,
//!   NOT `marginUsed`: for isolated positions `marginUsed = rawUsd + szi*entryPx + unrealizedPnl`
//!   (checked 2026-10-03 on 511 public isolated positions on the main dex and `xyz`), so it moves
//!   with the mark price and could fake or hide a top-up. Size and entry price must stay unchanged.
//! - Collateral is per dex in standard accounts: a top-up on `xyz`/`io` draws from that dex's own
//!   balance and is refused by the venue when it is empty (unified/portfolio-margin accounts share
//!   one USDC balance). This broker never moves collateral between dexes.
//!
//! Protocol sources (field order is part of the signature):
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/signing>
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size>
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint>
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint>
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids>
//! - <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees>
//! - <https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py>
//! - <https://github.com/hyperliquid-dex/hyperliquid-rust-sdk/tree/master/src/signature>
//! - <https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/examples/basic_leverage_adjustment.py>
//! - <https://github.com/nktkas/hyperliquid/blob/main/tests/api/exchange/updateIsolatedMargin.test.ts>

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Side, Symbol, Venue};
use async_trait::async_trait;
use fs2::FileExt;
use k256::ecdsa::SigningKey;
use reqwest::Client;
use rust_decimal::{Decimal, RoundingStrategy, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenueLegState, VenuePosition};
use crate::live_common::{Cached, Verified};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

/// 吃单限价读实时顶档（`arb_venues::live_book`）时，行情连接的心跳最多可以多旧。心跳每 2 秒一次，
/// 6 秒收不到任何消息监控就判死重连；超过这个值就当连接已经不可信，退回 REST。
const BBO_MAX_AGE: Duration = Duration::from_secs(6);
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
// Shared across broker instances in this process, never reused within a millisecond.
static NONCE: AtomicU64 = AtomicU64::new(0);
/// 补保证金后回读：最多读这么多次，两次之间间隔 [`MARGIN_READBACK_INTERVAL`]。
const MARGIN_READBACK_POLLS: usize = 6;
const MARGIN_READBACK_INTERVAL: Duration = Duration::from_millis(500);
/// 回读到的逐仓账本增量 ≥ 所补金额 × 这个比例才算生效（留 1% 给资金费结算之类的小额扰动）。
const MARGIN_CONFIRM_RATIO: Decimal = Decimal::from_parts(99, 0, 0, false, 2);
/// 下单被交易所受理后查订单状态：读节点会落后几秒，刚成交的 IOC 单这时 `orderStatus` 可能还回
/// `unknownOid`。最多查这么多次，两次之间间隔 [`ORDER_READBACK_INTERVAL`]（约 5 秒）。
const ORDER_READBACK_POLLS: usize = 10;
#[cfg(not(test))]
const ORDER_READBACK_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(test)]
const ORDER_READBACK_INTERVAL: Duration = Duration::from_millis(2);
/// 第一腿待核实状态的读回：逐笔成交还没追上订单状态时，最多读这么多轮（每轮之间隔一个读回间隔）。
const SETTLE_ATTEMPTS: usize = 4;
/// 市场列表（`meta`）在这段时间内复用：开仓前的预热拉一次，紧接着的准备、下单不再各拉一次。
/// 和 Arcus 的市场缓存同一个时长；下单被拒时清掉，下一次重新拉。
const META_TTL: Duration = Duration::from_secs(20);
/// 杠杆 / 保证金模式核对过（读回一致）后，这段时间内下单不再查 `activeAssetData`。
const LEVERAGE_VERIFIED_TTL: Duration = Duration::from_secs(20);
/// 账户的余额模式（`userAbstraction`：标准 / 统一 / 组合保证金）缓存多久。只是决定去哪里读可用保证金，
/// 改模式是人工的低频操作；改了之后最多这么久按旧模式读（读错只会让开仓前的保证金预检拦错或放过，
/// 真正下单仍由交易所按实际余额判定）。
const ACCOUNT_MODE_TTL: Duration = Duration::from_secs(300);
/// 下单（或核实）时已经从交易所完整读回、核对过的终态，这段时间内 `order_state` 直接给它：执行器下完单
/// 紧接着还要 `order_state` 一次，IOC 的终态不会再变，再读一遍订单状态和逐笔成交只是多两次往返（约 0.6 秒，
/// 第二腿和回滚都在等它）。只放内存（重启后照常去读）；待核实状态（[`Broker::place_for_hedge`]）从不放进来。
const TERMINAL_TTL: Duration = Duration::from_secs(600);
/// 只减仓单的本地持仓检查没过时，隔这么久再读（读节点滞后），一共再读这几次。
const REDUCE_REREADS: [Duration; 4] = [
    Duration::from_millis(300),
    Duration::from_millis(500),
    Duration::from_millis(800),
    Duration::from_millis(1200),
];

/// Which HyperCore perpetual dex a [`HyperliquidBroker`] instance trades.
///
/// All three share one `/info` and `/exchange` endpoint and one process-wide nonce, but
/// each is a separate venue because each has an independent clearinghouse and margin.
/// HIP-3 coin names carry the dex prefix (`xyz:TSLA`) and builder-deployed asset IDs
/// follow `100000 + perp_dex_index * 10000 + index_in_meta`:
/// <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HyperliquidDex {
    /// First perp dex (`hyperliquid`); request bodies omit `dex`.
    Main,
    /// HIP-3 `xyz` dex (`hyperliquid-xyz`): US equities, indices, commodities and FX.
    Xyz,
    /// HIP-3 `io` dex (`hyperliquid-io`): Pre-IPO and a few equities, isolated only.
    Io,
}

impl HyperliquidDex {
    pub const fn venue(self) -> Venue {
        match self {
            HyperliquidDex::Main => Venue::Hyperliquid,
            HyperliquidDex::Xyz => Venue::HyperliquidXyz,
            HyperliquidDex::Io => Venue::HyperliquidIo,
        }
    }

    /// `/info` `dex` value, or `None` for the first perp dex (the body omits the field).
    fn info_dex(self) -> Option<&'static str> {
        match self {
            HyperliquidDex::Main => None,
            HyperliquidDex::Xyz => Some("xyz"),
            HyperliquidDex::Io => Some("io"),
        }
    }

    /// Coin-name prefix including the separator; empty for the main dex.
    fn coin_prefix(self) -> &'static str {
        match self {
            HyperliquidDex::Main => "",
            HyperliquidDex::Xyz => "xyz:",
            HyperliquidDex::Io => "io:",
        }
    }

    /// Verified native-name → external-base aliases (bare name upper-cased → symbol base).
    fn aliases(self) -> &'static [(&'static str, &'static str)] {
        match self {
            HyperliquidDex::Main => &[],
            HyperliquidDex::Xyz => &[("GOLD", "XAU"), ("SILVER", "XAG")],
            HyperliquidDex::Io => &[("ANTH", "ANTHROPIC"), ("OAI", "OPENAI")],
        }
    }

    fn error(self, message: impl Into<String>) -> ArbError {
        ArbError::venue(self.venue().as_str(), message)
    }

    /// Official asset ID. Main perps use the meta index directly; builder-deployed perps
    /// use `100000 + perp_dex_index * 10000 + index_in_meta` with checked arithmetic.
    fn asset_id(self, perp_dex_index: u32, index_in_meta: u32) -> ArbResult<u32> {
        match self {
            HyperliquidDex::Main => Ok(index_in_meta),
            _ => perp_dex_index
                .checked_mul(10_000)
                .and_then(|v| v.checked_add(100_000))
                .and_then(|v| v.checked_add(index_in_meta))
                .ok_or_else(|| self.error("builder-deployed asset index overflow")),
        }
    }
}

/// [`HyperliquidBroker::submit_order`] 的结果：一次到位的确认，或 IOC 回包里的待核实状态。
enum Submitted {
    Ack(OrderAck),
    Provisional(Box<OrderState>),
}

#[derive(Debug, Clone, Default)]
pub struct HyperliquidOptions {
    pub trading_enabled: bool,
    /// Fraction, not percent. None rejects market-style orders without an explicit limit.
    pub market_slippage: Option<Decimal>,
}

pub struct HyperliquidBroker {
    client: Client,
    signer: SigningKey,
    account: String,
    signer_address: String,
    dex: HyperliquidDex,
    perp_dex_index: u32,
    options: HyperliquidOptions,
    taker_fee: Decimal,
    journal: SyncMutex<Journal>,
    submit: Mutex<()>,
    /// `/info`、`/exchange` 的地址（生产就是 [`INFO_URL`] / [`EXCHANGE_URL`]；测试指向本地假服务器）。
    info_url: String,
    exchange_url: String,
    /// 最近一次拉到的 `meta`，见 [`META_TTL`]。
    meta: Cached<Arc<Meta>>,
    /// 核对过的（币, 杠杆, 保证金模式），见 [`LEVERAGE_VERIFIED_TTL`]。
    verified_leverage: Verified<(String, u32, crate::MarginMode)>,
    /// `userAbstraction` 的应答，见 [`ACCOUNT_MODE_TTL`]。
    account_mode: Cached<Value>,
    /// 读回核对过的终态（cloid → 时刻, 状态），见 [`TERMINAL_TTL`]。
    terminal: SyncMutex<HashMap<String, (std::time::Instant, OrderState)>>,
    /// 下单回包给出的结局（cloid → 时刻, 状态），读节点还查不到这张单时兜底，见 [`Self::remember_reply`]。
    replied: SyncMutex<HashMap<String, (std::time::Instant, OrderState)>>,
    /// 每个合约最近一次读回的成交里实际收的费率（手续费 ÷ 成交额）。按回包估手续费时优先用它：HIP-3 各市场
    /// 费率不同（2026-10-31 核对：xyz 多数 0.0086%，STRC 0.0864%），配置的吃单费率 0.09% 会把多数单估高十倍。
    fee_rates: SyncMutex<HashMap<Symbol, Decimal>>,
}

// Deliberately no Debug/Serialize for the broker or signer-bearing construction inputs.
struct Journal {
    file: File,
    by_cloid: HashMap<String, Reservation>,
    poisoned: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Reservation {
    account: String,
    signer: String,
    cloid: String,
    order: NewOrder,
    effective_quantity: Decimal,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Asset {
    name: String,
    sz_decimals: u32,
    max_leverage: u32,
    #[serde(default)]
    is_delisted: bool,
    #[serde(default)]
    only_isolated: bool,
    #[serde(default)]
    margin_mode: Option<String>,
    /// HIP-3 部署者费率倍数。2026-09 起 Hyperliquid 把它从 `perpDexs`（整个 dex）挪到了
    /// 各 dex 的 `meta.universe[]`（逐合约），实测 xyz 109 个、io 8 个合约全是 `"1.0"`。
    #[serde(default)]
    deployer_fee_scale: Option<String>,
}

#[derive(Deserialize)]
struct Meta {
    universe: Vec<Asset>,
    /// 这个 dex 的保证金币（现货 token 编号，2026-10-11 三个 dex 都是整数 0）。统一账户按它在现货余额里
    /// 找可用额。按原始值存、用时再取整数：类型变了只影响这一处，不连累整份 `meta` 解析失败。
    #[serde(default, rename = "collateralToken")]
    collateral_token: Option<Value>,
}

/// 帐户余额模式（`userAbstraction`）。统一账户 / 组合保证金下，全部余额与占用都在现货状态里，
/// 各 perp dex 的 `clearinghouseState` 没有意义（官方文档 Account abstraction modes）。
fn balances_live_in_spot(mode: &Value) -> bool {
    matches!(mode.as_str(), Some("unifiedAccount" | "portfolioMargin"))
}

/// 统一账户里这个 dex 的保证金币还能用多少：`tokenToAvailableAfterMaintenance` 与 `total − hold`
/// 取小（前者只扣维持保证金，可能偏乐观）。两个都没有就是不知道，不当 0。
fn unified_free(spot: &Value, token: u64) -> Option<Decimal> {
    let parse = |v: &Value| v.as_str().and_then(|raw| Decimal::from_str(raw).ok());
    let available = spot
        .get("tokenToAvailableAfterMaintenance")
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter().find_map(|row| {
                let pair = row.as_array()?;
                (pair.first()?.as_u64()? == token).then(|| parse(pair.get(1)?))?
            })
        });
    let unlocked = spot
        .get("balances")
        .and_then(Value::as_array)
        .and_then(|rows| {
            rows.iter()
                .find(|row| row.get("token").and_then(Value::as_u64) == Some(token))
        })
        .and_then(|row| Some(parse(row.get("total")?)? - parse(row.get("hold")?)?));
    let free = match (available, unlocked) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    free.map(|value| value.max(Decimal::ZERO))
}

fn market_supports_mode(dex: HyperliquidDex, asset: &Asset, mode: crate::MarginMode) -> bool {
    !asset.is_delisted
        && (!mode.is_cross()
            || (dex == HyperliquidDex::Main
                && !asset.only_isolated
                && matches!(
                    asset.margin_mode.as_deref(),
                    None | Some("cross") | Some("default")
                )))
}

/// One `perpDexs` entry. The response array is positional; the main dex is `null`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpDex {
    name: String,
    /// 旧接口在 dex 一级给的部署者费率倍数；新接口已不给（改到逐合约，见 [`Asset`]）。
    #[serde(default)]
    deployer_fee_scale: Option<String>,
}

// Do not serialize signing actions via serde_json::Value: its maps may reorder keys.
// Named MessagePack + ordered structs match the official Python dict construction.
#[derive(Serialize)]
struct OrderAction<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    orders: [WireOrder<'a>; 1],
    grouping: &'static str,
}

#[derive(Serialize)]
struct WireOrder<'a> {
    a: u32,
    b: bool,
    p: &'a str,
    s: &'a str,
    r: bool,
    t: LimitType,
    c: &'a str,
}

#[derive(Serialize)]
struct LimitType {
    limit: TimeInForce,
}

#[derive(Serialize)]
struct TimeInForce {
    tif: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LeverageAction {
    #[serde(rename = "type")]
    kind: &'static str,
    asset: u32,
    is_cross: bool,
    leverage: u32,
}

/// `updateIsolatedMargin`。字段顺序就是 MessagePack 的签名顺序，和官方 Python SDK
/// `update_isolated_margin` 的 dict 构造一致：`type, asset, isBuy, ntli`。
/// `ntli` = USDC 金额 × 1e6 的整数，**正数 = 加保证金**（见模块文档）；`isBuy` 官方说
/// 「在对冲模式上线前没有作用」，两个官方 SDK 都固定发 `true`。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateIsolatedMarginAction {
    #[serde(rename = "type")]
    kind: &'static str,
    asset: u32,
    is_buy: bool,
    ntli: i64,
}

#[derive(Serialize)]
struct CancelAction {
    #[serde(rename = "type")]
    kind: &'static str,
    cancels: [WireCancel; 1],
}

#[derive(Serialize)]
struct WireCancel {
    a: u32,
    o: u64,
}

#[derive(Serialize)]
struct WireSignature {
    r: String,
    s: String,
    v: u8,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    orig_sz: String,
    oid: u64,
    timestamp: u64,
    reduce_only: bool,
    cloid: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteStatus {
    order: RemoteOrder,
    status: String,
    status_timestamp: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteFill {
    oid: u64,
    tid: u64,
    time: u64,
    coin: String,
    side: String,
    sz: String,
    px: String,
    fee: String,
    fee_token: String,
}

impl HyperliquidBroker {
    /// [`Broker::funding_since`]，「现在」由调用方给（测试用固定时刻，见 [`SharedFunding::usable_at`]）。
    async fn funding_since_at(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let since_ms = since.timestamp_millis();
        let key = (self.info_url.clone(), self.account.clone());
        let shared = shared_funding()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .and_then(|cached| {
                (cached.usable_at(now) && cached.start <= since_ms).then(|| {
                    cached
                        .rows
                        .iter()
                        .filter(|row| row.time >= since_ms)
                        .cloned()
                        .collect::<Vec<_>>()
                })
            });
        if let Some(raw) = shared {
            return Ok(Some(FundingTotal::from_rows(
                funding_rows(self.dex, symbol, raw)?,
                since,
            )));
        }
        let mut raw = Vec::new();
        let mut start = since_ms;
        for _ in 0..FUNDING_PAGES {
            let page: Vec<UserFunding> = post(
                &self.client,
                &self.info_url,
                &json!({"type":"userFunding", "user":self.account, "startTime":start}),
            )
            .await?;
            let full = page.len() >= FUNDING_PAGE;
            let last = page.iter().map(|row| row.time).max();
            raw.extend(page);
            match last {
                Some(last) if full => start = last + 1,
                _ => {
                    let rows = funding_rows(self.dex, symbol, raw.clone())?;
                    let mut shared = shared_funding()
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    // 已有的更新鲜、起点又更早的就留着它（它盖得住这次的）。
                    if shared
                        .get(&key)
                        .is_none_or(|cached| !cached.usable_at(now) || since_ms <= cached.start)
                    {
                        shared.insert(
                            key,
                            SharedFunding {
                                fetched: now,
                                start: since_ms,
                                rows: raw,
                            },
                        );
                    }
                    return Ok(Some(FundingTotal::from_rows(rows, since)));
                }
            }
        }
        Err(error(
            "funding history exceeds the page limit; total would be incomplete",
        ))
    }

    /// Reads metadata/actual fee schedule; never sends an exchange action at construction.
    /// The journal must be a durable path unique to this account, dex and broker process.
    /// Master-account or approved agent keys are supported; vault/subaccount routing is not.
    pub async fn connect(
        private_key: &str,
        account_address: &str,
        journal_path: impl AsRef<Path>,
        options: HyperliquidOptions,
        dex: HyperliquidDex,
    ) -> ArbResult<Self> {
        if let Some(slippage) = options.market_slippage
            && (slippage <= Decimal::ZERO || slippage >= Decimal::ONE)
        {
            return Err(error(
                "market_slippage must be strictly between zero and one",
            ));
        }
        let mut key = decode_hex::<32>(private_key).map_err(|_| error("invalid signing key"))?;
        let parsed_signer = SigningKey::from_slice(&key).map_err(|_| error("invalid signing key"));
        key.fill(0);
        let signer = parsed_signer?;
        let public = signer.verifying_key().to_encoded_point(false);
        let signer_address = hex(&keccak(&public.as_bytes()[1..])[12..]);
        let account = hex(&decode_hex::<20>(account_address)?);
        let journal = Journal::open(
            journal_path.as_ref(),
            &account,
            &signer_address,
            dex.venue(),
        )?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let fees: Value = post(
            &client,
            INFO_URL,
            &json!({"type":"userFees", "user":account}),
        )
        .await?;
        let base_fee = decimal_field(&fees, "userCrossRate")?;
        if base_fee < Decimal::ZERO || base_fee >= Decimal::ONE {
            return Err(error("invalid account taker fee"));
        }
        // Resolve the dex position for the builder-deployed asset-ID formula and, for
        // HIP-3, the deployer fee scale. Never guess either: a wrong scale understates fees.
        let (perp_dex_index, taker_fee) = match dex.info_dex() {
            None => (0u32, base_fee),
            Some(name) => {
                let dexs: Vec<Option<PerpDex>> =
                    post(&client, INFO_URL, &json!({"type":"perpDexs"})).await?;
                let mut found = None;
                for (index, entry) in dexs.into_iter().enumerate() {
                    if let Some(entry) = entry
                        && entry.name == name
                    {
                        if found.is_some() {
                            return Err(dex.error(
                                "duplicate perp dex name; refusing to guess the asset-ID index",
                            ));
                        }
                        found = Some((
                            u32::try_from(index)
                                .map_err(|_| dex.error("perp dex index overflow"))?,
                            entry.deployer_fee_scale,
                        ));
                    }
                }
                let (index, dex_scale) = found.ok_or_else(|| {
                    dex.error(format!(
                        "perp dex {name} is not deployed; cannot resolve builder asset IDs"
                    ))
                })?;
                // 费率倍数：dex 一级（旧接口）与逐合约（新接口）都纳入，取最大。
                // `fee_per_side` 是整个场所一个数，按最贵的合约算只会高估、不会低估；
                // growthMode 的折扣接口查不到，同样不扣。一处都没有才拒绝连接。
                let meta: Meta =
                    post(&client, INFO_URL, &json!({"type":"meta", "dex":name})).await?;
                let scale =
                    max_fee_scale(dex_scale.as_deref(), &meta.universe)?.ok_or_else(|| {
                        dex.error(
                        "HIP-3 deployer fee scale unavailable; refusing to understate taker fees",
                    )
                    })?;
                let fee = multiply(base_fee, hip3_fee_scale(scale)?)?;
                if fee >= Decimal::ONE {
                    return Err(dex.error("HIP-3 taker fee is not below one"));
                }
                (index, fee)
            }
        };
        let broker = Self {
            client,
            signer,
            account,
            signer_address,
            dex,
            perp_dex_index,
            options,
            taker_fee,
            journal: SyncMutex::new(journal),
            submit: Mutex::new(()),
            info_url: INFO_URL.to_string(),
            exchange_url: EXCHANGE_URL.to_string(),
            meta: Cached::new(),
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
            account_mode: Cached::new(),
            terminal: SyncMutex::new(HashMap::new()),
            replied: SyncMutex::new(HashMap::new()),
            fee_rates: SyncMutex::new(HashMap::new()),
        };
        broker.metadata().await?;
        // 下单用的常驻 WebSocket 连接（三个 dex 共用一条）；没连上时下单自动走 HTTP。
        crate::hl_ws::start_shared();
        Ok(broker)
    }

    fn authorize(&self) -> ArbResult<()> {
        if !self.options.trading_enabled {
            return Err(error(
                "production trading is disabled; explicit authorization is required",
            ));
        }
        Ok(())
    }

    async fn metadata(&self) -> ArbResult<Arc<Meta>> {
        self.metadata_at(&self.info_url).await
    }

    /// [`Broker::free_collateral`] 的实现（`info_url` 可指向测试用的假服务器）。只读。
    async fn free_collateral_at(&self, info_url: &str) -> ArbResult<Option<Decimal>> {
        let mode = match self.account_mode.get(ACCOUNT_MODE_TTL) {
            Some(mode) => mode,
            None => {
                let mode: Value = post(
                    &self.client,
                    info_url,
                    &json!({"type":"userAbstraction", "user":self.account}),
                )
                .await?;
                self.account_mode.put(mode.clone());
                mode
            }
        };
        if balances_live_in_spot(&mode) {
            // 保证金币（`meta`，有缓存）和现货余额并发读：开仓前的保证金预检和拉盘口同时做，
            // 它慢一个往返，报价就要多放一个往返才能下单。
            let spot_query = json!({"type":"spotClearinghouseState", "user":self.account});
            let (meta, spot) = tokio::join!(
                self.metadata_within_at(info_url),
                post::<Value>(&self.client, info_url, &spot_query),
            );
            // 没报保证金币就不知道该看哪个余额：给 None（只警告），不猜 USDC。
            let Some(token) = meta?.collateral_token.as_ref().and_then(Value::as_u64) else {
                return Ok(None);
            };
            return Ok(unified_free(&spot?, token));
        }
        let state: Value = post(
            &self.client,
            info_url,
            &json!({
                "type":"clearinghouseState", "user":self.account,
                "dex":self.dex.info_dex().unwrap_or(""),
            }),
        )
        .await?;
        let summary = state.get("marginSummary");
        let number = |key: &str| {
            summary
                .and_then(|summary| summary.get(key))
                .and_then(Value::as_str)
                .and_then(|raw| Decimal::from_str(raw).ok())
        };
        Ok(number("accountValue")
            .zip(number("totalMarginUsed"))
            .map(|(value, used)| (value - used).max(Decimal::ZERO)))
    }

    /// 拉一次最新的 `meta` 并放进缓存（[`META_TTL`]）。
    async fn metadata_at(&self, info_url: &str) -> ArbResult<Arc<Meta>> {
        let body = match self.dex.info_dex() {
            Some(dex) => json!({"type":"meta", "dex":dex}),
            None => json!({"type":"meta"}),
        };
        let meta: Meta = post(&self.client, info_url, &body).await?;
        if meta.universe.is_empty() {
            return Err(error("empty perpetual metadata"));
        }
        let meta = Arc::new(meta);
        self.meta.put(Arc::clone(&meta));
        Ok(meta)
    }

    /// 缓存里不超过 [`META_TTL`] 的 `meta`，没有就拉一次。
    async fn metadata_within_at(&self, info_url: &str) -> ArbResult<Arc<Meta>> {
        match self.meta.get(META_TTL) {
            Some(meta) => Ok(meta),
            None => self.metadata_at(info_url).await,
        }
    }

    async fn asset(&self, symbol: &Symbol) -> ArbResult<(u32, Asset)> {
        self.asset_at(&self.info_url, symbol).await
    }

    /// 缓存优先；缓存里找不到这个合约（可能刚上线）就再拉一次最新的 `meta`。
    async fn asset_at(&self, info_url: &str, symbol: &Symbol) -> ArbResult<(u32, Asset)> {
        if let Some(meta) = self.meta.get(META_TTL)
            && let Some(found) = self.find_asset(&meta, symbol)?
        {
            return Ok(found);
        }
        let meta = self.metadata_at(info_url).await?;
        self.find_asset(&meta, symbol)?
            .ok_or_else(|| error(format!("unknown perp market {symbol}")))
    }

    fn find_asset(&self, meta: &Meta, symbol: &Symbol) -> ArbResult<Option<(u32, Asset)>> {
        let mut found = None;
        for (index, asset) in meta.universe.iter().enumerate() {
            if symbol_of(self.dex, &asset.name)? == *symbol {
                if found.is_some() {
                    return Err(error("ambiguous perp market identity"));
                }
                let index = u32::try_from(index).map_err(|_| error("asset index overflow"))?;
                let id = self.dex.asset_id(self.perp_dex_index, index)?;
                found = Some((id, asset.clone()));
            }
        }
        Ok(found)
    }

    async fn configure_margin(
        &self,
        asset_id: u32,
        asset: &Asset,
        leverage: u32,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        if !market_supports_mode(self.dex, asset, mode) {
            return Err(error("此 Hyperliquid 合约不支持所选保证金模式"));
        }
        // 刚核对过（预热或上一次准备）：不再往返一次。
        let key = (asset.name.clone(), leverage, mode);
        if self.verified_leverage.is_fresh(&key) {
            return Ok(());
        }
        let query = json!({"type":"activeAssetData", "user":self.account, "coin":asset.name});
        let matches = |data: &Value| leverage_matches(data, &asset.name, leverage, mode);
        let current: Value = post(&self.client, &self.info_url, &query).await?;
        if matches(&current) {
            self.mark_verified(key);
            return Ok(());
        }
        // 要改杠杆了：这个币之前核对过的结果立刻作废（改到一半失败也不会留下过期的「核对过」）。
        self.verified_leverage
            .forget_where(|(coin, _, _)| coin == &asset.name);
        let reply = self
            .exchange(&LeverageAction {
                kind: "updateLeverage",
                asset: asset_id,
                is_cross: mode.is_cross(),
                leverage,
            })
            .await?;
        if reply.pointer("/response/type").and_then(Value::as_str) != Some("default") {
            return Err(error("leverage update not acknowledged"));
        }
        for _ in 0..5 {
            let readback: Value = post(&self.client, &self.info_url, &query).await?;
            if matches(&readback) {
                self.mark_verified(key);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(error("保证金模式/杠杆读回未确认，没有下单"))
    }

    /// 记下这个（币, 杠杆, 模式）读回核对过了：同一个币之前核对过的别的杠杆 / 模式一并作废（交易所里现在
    /// 是这一个，旧的不能再让下一次跳过核对）。
    fn mark_verified(&self, key: (String, u32, crate::MarginMode)) {
        self.verified_leverage
            .forget_where(|(coin, _, _)| coin == &key.0);
        self.verified_leverage.mark(key);
    }

    /// 记下一笔读回核对过的终态（活着的单不记），顺手清掉过期的。
    fn remember_terminal(&self, state: &OrderState) {
        if state.status.is_live() {
            return;
        }
        let cloid = client_cloid(&state.order.client_order_id);
        if let Ok(mut terminal) = self.terminal.lock() {
            terminal.retain(|_, (at, _)| at.elapsed() < TERMINAL_TTL);
            terminal.insert(cloid, (std::time::Instant::now(), state.clone()));
        }
    }

    fn known_terminal(&self, cloid: &str) -> Option<OrderState> {
        let terminal = self.terminal.lock().ok()?;
        let (at, state) = terminal.get(cloid)?;
        (at.elapsed() < TERMINAL_TTL).then(|| state.clone())
    }

    /// 记下下单回包里交易所给出的结局：IOC 的成交量与均价（[`Self::provisional`]），或明确拒绝（没进撮合、
    /// 一点没成交）。读节点会落后几秒到十几秒（10-17 xyz:MSFT 已成交 8 秒多还是 `unknownOid`），这时
    /// [`Broker::order_state`] 用它，不把一张结局已知的单当成「结果不明」。不进 [`Self::terminal`]：手续费是
    /// 估的、均价偏保守；读回一旦查到就以读回为准并清掉。只放内存：重启后读节点早已追上。
    fn remember_reply(&self, state: &OrderState) {
        let cloid = client_cloid(&state.order.client_order_id);
        if let Ok(mut replied) = self.replied.lock() {
            replied.retain(|_, (at, _)| at.elapsed() < TERMINAL_TTL);
            replied.insert(cloid, (std::time::Instant::now(), state.clone()));
        }
    }

    fn replied_state(&self, cloid: &str) -> Option<OrderState> {
        let replied = self.replied.lock().ok()?;
        let (at, state) = replied.get(cloid)?;
        (at.elapsed() < TERMINAL_TTL).then(|| state.clone())
    }

    fn forget_reply(&self, cloid: &str) {
        if let Ok(mut replied) = self.replied.lock() {
            replied.remove(cloid);
        }
    }

    fn reservation(&self, cloid: &str) -> ArbResult<Option<Reservation>> {
        let journal = self
            .journal
            .lock()
            .map_err(|_| error("intent journal lock poisoned"))?;
        if journal.poisoned {
            return Err(error(
                "intent journal persistence failed; stop and reconcile",
            ));
        }
        Ok(journal.by_cloid.get(cloid).cloned())
    }

    /// 授权 + 取进程内单调 nonce + 签名，返回可以直接 POST 到 `/exchange` 的请求体。
    /// 只做本地计算、不发请求；只读模式在这里就返回 `Err`，所以任何写操作都签不出东西。
    fn signed_request<T: Serialize>(&self, action: &T) -> ArbResult<Value> {
        self.authorize()?;
        let nonce = next_nonce()?;
        let signature = sign(&self.signer, action_hash(action, nonce)?)?;
        Ok(json!({"action":action, "nonce":nonce, "signature":signature, "vaultAddress":null}))
    }

    async fn exchange<T: Serialize + Sync>(&self, action: &T) -> ArbResult<Value> {
        let body = self.signed_request(action)?;
        let response: Value = post(&self.client, &self.exchange_url, &body).await?;
        if response.get("status").and_then(Value::as_str) != Some("ok") {
            // Never echo arbitrary server data that could contain a signed request.
            return Err(error(
                "exchange action rejected; query account/order state before retrying",
            ));
        }
        Ok(response)
    }

    /// 签名并**只发一次**下单，把应答分成「受理 / 明确拒绝 / 不知道」（见 [`classify_order_reply`]）。
    /// `Err` 只在没有发出任何请求时返回（只读模式、签名失败、请求构造失败）。
    async fn send_order_action<T: Serialize + Sync>(&self, action: &T) -> ArbResult<OrderSend> {
        let body = self.signed_request(action)?;
        // 优先走常驻 WebSocket 的 `post`（[`crate::hl_ws`]）：连接常驻、已握手。**只发一次**：
        // 没交给连接（没连上 / 判死 / 熔断 / 在途太多）才改走 HTTP，同一份没发出去过的请求体；
        // 一旦交给了连接，超时 / 断线都是「结果不明」，直接返回 `Unknown`，绝不再用 HTTP 补发。
        // 只在生产地址上用：指向测试假服务器时不碰真实连接。
        if self.exchange_url == EXCHANGE_URL
            && let Some(poster) = crate::hl_ws::shared()
        {
            match poster
                .post_action(body.clone(), crate::hl_ws::POST_TIMEOUT)
                .await
            {
                crate::hl_ws::PostOutcome::NotSent => {}
                crate::hl_ws::PostOutcome::Reply(Ok(payload)) => {
                    return Ok(classify_order_reply(200, Some(&payload.to_string())));
                }
                // 服务端的错误字符串镜像 HTTP 状态码与描述（如 `429 …`）：按同一口径分类，读不出状态码就当不知道。
                crate::hl_ws::PostOutcome::Reply(Err(text)) => {
                    return Ok(match text
                        .split_whitespace()
                        .next()
                        .and_then(|code| code.parse::<u16>().ok())
                    {
                        Some(status) => classify_order_reply(status, None),
                        None => OrderSend::Unknown(
                            "websocket error reply without a status code; the order may have been placed"
                                .to_string(),
                        ),
                    });
                }
                crate::hl_ws::PostOutcome::Unknown(reason) => {
                    return Ok(OrderSend::Unknown(format!(
                        "websocket order request outcome unknown ({reason}); the order may have been placed"
                    )));
                }
            }
        }
        let response = match self
            .client
            .post(&self.exchange_url)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) if e.is_builder() => {
                return Err(error("order request could not be built; nothing was sent"));
            }
            // 超时、连接中断……请求可能已经到了对面：只能当成不知道。
            Err(e) => return Ok(OrderSend::Unknown(transport_reason(&e))),
        };
        let status = response.status().as_u16();
        let text = response.text().await.ok();
        Ok(classify_order_reply(status, text.as_deref()))
    }

    /// `clearinghouseState` 里这个币的逐仓持仓；没有持仓、不是逐仓、读不到账本都是 `Err`。
    async fn isolated_leg_at(&self, info_url: &str, symbol: &Symbol) -> ArbResult<IsolatedLeg> {
        let state: Value = post(
            &self.client,
            info_url,
            &json!({
                "type":"clearinghouseState", "user":self.account,
                "dex":self.dex.info_dex().unwrap_or(""),
            }),
        )
        .await?;
        isolated_leg(&state, self.dex, symbol)
    }

    /// 签名并**只发一次** `updateIsolatedMargin`，把应答分成「已受理 / 明确拒绝 / 不知道」。
    /// `Err` 只在**没有发出任何请求**时返回（只读模式、签名失败、请求构造失败）。
    async fn send_margin_action(
        &self,
        exchange_url: &str,
        action: &UpdateIsolatedMarginAction,
    ) -> ArbResult<MarginSend> {
        let body = self.signed_request(action)?;
        let response = match self.client.post(exchange_url).json(&body).send().await {
            Ok(response) => response,
            Err(e) if e.is_builder() => {
                return Err(self
                    .dex
                    .error("margin request could not be built; nothing was sent"));
            }
            // 超时、连接中断……请求可能已经到了对面：绝不当成「没动钱」。
            Err(e) => return Ok(MarginSend::Unknown(transport_reason(&e))),
        };
        let status = response.status().as_u16();
        // 响应体只在 2xx 时有用；读不出来不会改变状态码给出的结论（2xx 读不出 = 不知道）。
        let text = response.text().await.ok();
        Ok(classify_margin_reply(status, text.as_deref()))
    }

    /// 回读逐仓账本：最多 [`MARGIN_READBACK_POLLS`] 次，`Ok(())` = 已确认增加 ≥ 99% 的金额；
    /// 其余一律 `Err(说明)`，调用方据此报 `Unknown`，**绝不**报 `Applied`。
    async fn confirm_margin(
        &self,
        info_url: &str,
        symbol: &Symbol,
        before: &IsolatedLeg,
        amount: Decimal,
        interval: Duration,
    ) -> Result<(), String> {
        let mut last = String::from("no readback completed");
        for attempt in 0..MARGIN_READBACK_POLLS {
            if attempt > 0 {
                tokio::time::sleep(interval).await;
            }
            last = match self.isolated_leg_at(info_url, symbol).await {
                Ok(after) => match judge_margin_change(before, &after, amount) {
                    MarginJudgement::Confirmed => return Ok(()),
                    MarginJudgement::PositionChanged => {
                        return Err(format!(
                            "position size changed from {} to {} or entry price changed from {} to {} during the readback; the margin change cannot be attributed",
                            before.size, after.size, before.entry_price, after.entry_price
                        ));
                    }
                    MarginJudgement::Decreased { delta } => format!(
                        "isolated margin DECREASED by {} USDC (expected +{amount}): the venue may have applied the amount with the opposite sign; stop automatic top-ups and check the account",
                        delta.abs()
                    ),
                    MarginJudgement::NotIncreased { delta } => {
                        format!("isolated margin changed by {delta} USDC (expected +{amount})")
                    }
                },
                Err(e) => format!("readback failed: {e}"),
            };
        }
        Err(format!("after {MARGIN_READBACK_POLLS} readbacks: {last}"))
    }

    /// [`Broker::add_margin`]，URL 与回读间隔作为参数只是为了让测试能指向本地假服务器。
    async fn add_margin_at(
        &self,
        info_url: &str,
        exchange_url: &str,
        poll_interval: Duration,
        symbol: &Symbol,
        amount_usdt: Decimal,
    ) -> ArbResult<MarginOutcome> {
        // 先授权再看金额：只读模式下什么都不碰（不查、不签、不发）。
        self.authorize()?;
        let ntli = margin_ntli(amount_usdt)?;
        // 和 place()/cancel() 同一把提交锁，补保证金不会和订单写在同一个密钥上赛跑；
        // 锁一直持有到回读结束，所以回读不会把本进程自己的订单当成补进去的钱。
        let _submission = self.submit.lock().await;
        let (asset_id, _) = self.asset_at(info_url, symbol).await?;
        let before = self.isolated_leg_at(info_url, symbol).await?;
        let action = UpdateIsolatedMarginAction {
            kind: "updateIsolatedMargin",
            asset: asset_id,
            is_buy: true,
            ntli,
        };
        let pending = match self.send_margin_action(exchange_url, &action).await? {
            MarginSend::Refused(reason) => return Ok(MarginOutcome::Refused(reason)),
            MarginSend::Accepted => "exchange acknowledged the action (status ok)".to_string(),
            MarginSend::Unknown(reason) => reason,
        };
        // 受理 ≠ 生效，超时/5xx 也可能其实生效了：只认回读。
        Ok(
            match self
                .confirm_margin(info_url, symbol, &before, amount_usdt, poll_interval)
                .await
            {
                Ok(()) => MarginOutcome::Applied,
                Err(detail) => MarginOutcome::Unknown(format!(
                    "{pending}, but the margin increase was not confirmed: {detail}"
                )),
            },
        )
    }

    async fn remote_status(&self, oid: Value) -> ArbResult<Option<RemoteStatus>> {
        self.remote_status_at(&self.info_url, oid).await
    }

    /// 交易所已经回了「受理」的订单：它一定存在，`unknownOid` 只是读节点还没追上，
    /// 所以按 [`ORDER_READBACK_POLLS`] 次轮询，查到就返回。轮询只读，不会重复提交任何东西。
    /// 轮询用完仍是 `unknownOid` 返回 `Ok(None)`；最后一次是请求错误则返回那个错误。
    async fn remote_status_after_ack(
        &self,
        info_url: &str,
        oid: Value,
        interval: Duration,
    ) -> ArbResult<Option<RemoteStatus>> {
        let mut last = Ok(None);
        for attempt in 0..ORDER_READBACK_POLLS {
            if attempt > 0 {
                tokio::time::sleep(interval).await;
            }
            last = self.remote_status_at(info_url, oid.clone()).await;
            if matches!(last, Ok(Some(_))) {
                break;
            }
        }
        last
    }

    async fn remote_status_at(
        &self,
        info_url: &str,
        oid: Value,
    ) -> ArbResult<Option<RemoteStatus>> {
        let response: Value = post(
            &self.client,
            info_url,
            &json!({"type":"orderStatus", "user":self.account, "oid":oid}),
        )
        .await?;
        match response.get("status").and_then(Value::as_str) {
            Some("unknownOid") => Ok(None),
            Some("order") => decode(
                response
                    .get("order")
                    .cloned()
                    .ok_or_else(|| error("missing order record"))?,
            )
            .map(Some),
            _ => Err(error("unknown order-status response")),
        }
    }

    async fn state_from_remote(
        &self,
        remote: RemoteStatus,
        requested_id: Option<&ClientOrderId>,
    ) -> ArbResult<OrderState> {
        let row = &remote.order;
        let saved = match row.cloid.as_deref() {
            Some(cloid) => self.reservation(cloid)?,
            None => None,
        };
        let symbol = symbol_of(self.dex, &row.coin)?;
        let side = parse_side(&row.side)?;
        let original = positive(&row.orig_sz)?;
        let remaining = decimal(&row.sz)?;
        if remaining < Decimal::ZERO || remaining > original {
            return Err(error("invalid remaining order quantity"));
        }
        let limit = positive(&row.limit_px)?;
        let id = requested_id
            .cloned()
            .or_else(|| saved.as_ref().map(|s| s.order.client_order_id.clone()))
            .unwrap_or_else(|| {
                ClientOrderId(format!("{}:oid:{}", self.dex.venue().as_str(), row.oid))
            });
        let order = if let Some(saved) = saved {
            if saved.order.client_order_id != id
                || saved.order.symbol != symbol
                || saved.order.side != side
                || saved.order.reduce_only != row.reduce_only
                || saved.effective_quantity != original
            {
                return Err(error("venue order does not match persisted intent"));
            }
            saved.order
        } else {
            NewOrder {
                post_only: false,
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: id,
                venue: self.dex.venue(),
                symbol,
                side,
                notional_usdt: multiply(original, limit)?,
                quantity: Some(original),
                limit_price: Some(limit),
                reduce_only: row.reduce_only,
                leverage: None,
            }
        };
        let status = map_status(&remote.status)?;
        // Terminal status has a bounded fill window; live status can race a new fill,
        // in which case the size consistency check deliberately fails closed.
        let end = if status.is_live() {
            now_ms()?
        } else {
            remote.status_timestamp
        };
        if end < row.timestamp {
            return Err(error("invalid order time interval"));
        }
        let fills = self.order_fills(row, end).await?;
        let (quantity, notional, fees) = aggregate_fills(&fills, row)?;
        let status = verified_status(status, original, remaining, quantity)?;
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(row.oid.to_string());
        state.status = status;
        state.filled_usdt = notional;
        state.average_price = if quantity > Decimal::ZERO {
            Some(divide(notional, quantity)?)
        } else {
            None
        };
        state.fee_usdt = fees;
        if notional > Decimal::ZERO
            && fees >= Decimal::ZERO
            && let Ok(mut rates) = self.fee_rates.lock()
        {
            rates.insert(state.order.symbol.clone(), fees / notional);
        }
        if status == OrderStatus::Rejected {
            state.reject_reason = Some(remote.status);
        }
        Ok(state)
    }

    async fn order_fills(&self, order: &RemoteOrder, end: u64) -> ArbResult<Vec<RemoteFill>> {
        let mut start = order.timestamp;
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        loop {
            let rows: Vec<RemoteFill> = post(
                &self.client,
                &self.info_url,
                &json!({
                    "type":"userFillsByTime", "user":self.account,
                    "startTime":start, "endTime":end, "aggregateByTime":false,
                }),
            )
            .await?;
            let count = rows.len();
            let mut last = start;
            let mut added = 0;
            for row in rows {
                if row.time < start || row.time > end {
                    return Err(error("fill response outside requested time range"));
                }
                last = last.max(row.time);
                if seen.insert((row.oid, row.tid)) {
                    added += 1;
                    if row.oid == order.oid {
                        result.push(row);
                    }
                }
            }
            // The API retains only 10,000 latest fills. Never claim complete capped history.
            if seen.len() >= 10_000 {
                return Err(error(
                    "fill history retention limit reached; archival reconciliation required",
                ));
            }
            if count < 2000 {
                return Ok(result);
            }
            if last <= start || added == 0 {
                return Err(error(
                    "fill pagination cannot advance without losing same-timestamp fills",
                ));
            }
            start = last; // inclusive overlap; dedupe by actual trade ID, not price/time.
        }
    }

    async fn price(&self, order: &NewOrder, asset: &Asset) -> ArbResult<Decimal> {
        let raw = if let Some(limit) = order.limit_price {
            limit
        } else {
            let slippage = self.options.market_slippage.ok_or_else(|| {
                error("market orders require explicit market_slippage or a limit price")
            })?;
            // 先读行情 WebSocket 已经放进本进程的顶档（`bbo`，见 `arb_venues::live_book`）：吃单限价只用对手方
            // 顶档，不必每单先 REST 拉一遍 `l2Book`（一个往返）。没有、连接心跳太旧、交叉、该侧为空
            // 都退回 REST，限价规则与滑点上限不变。
            if let Some(((bid, _), (ask, _))) =
                arb_venues::live_book::top(self.dex.venue(), &order.symbol.base, BBO_MAX_AGE)
            {
                let top = if order.side == Side::Buy { ask } else { bid };
                let factor = if order.side == Side::Buy {
                    Decimal::ONE + slippage
                } else {
                    Decimal::ONE - slippage
                };
                return tick_price(multiply(top, factor)?, asset.sz_decimals, order.side);
            }
            let book: Value = post(
                &self.client,
                &self.info_url,
                &match self.dex.info_dex() {
                    // `coin` already carries the dex prefix; `dex` is ignored by this
                    // endpoint but sent for parity with the other per-dex queries.
                    Some(dex) => json!({"type":"l2Book", "coin":asset.name, "dex":dex}),
                    None => json!({"type":"l2Book", "coin":asset.name}),
                },
            )
            .await?;
            let timestamp = book
                .get("time")
                .and_then(Value::as_u64)
                .ok_or_else(|| error("missing book timestamp"))?;
            let now = now_ms()?;
            if timestamp > now + 1000 || now.saturating_sub(timestamp) > 5000 {
                return Err(error("stale order book"));
            }
            let index = if order.side == Side::Buy { 1 } else { 0 };
            let level = book
                .get("levels")
                .and_then(|v| v.get(index))
                .and_then(|v| v.get(0))
                .ok_or_else(|| error("empty executable order-book side"))?;
            let top = decimal_field(level, "px")?;
            let factor = if order.side == Side::Buy {
                Decimal::ONE + slippage
            } else {
                Decimal::ONE - slippage
            };
            multiply(top, factor)?
        };
        tick_price(raw, asset.sz_decimals, order.side)
    }

    /// 下单（[`Broker::place`]、[`Broker::place_for_hedge`] 共用）。`quick`：IOC 回包里已经给出成交量和
    /// 均价时直接返回待核实状态（见 [`Self::provisional`]），不等订单状态与逐笔成交的读回。
    async fn submit_order(&self, order: &NewOrder, quick: bool) -> ArbResult<Submitted> {
        self.authorize()?;
        // 分段耗时（见下面「Hyperliquid 下单分段耗时」）：等下单锁、发单前的查重、其余准备、`/exchange` 往返。
        let entered = std::time::Instant::now();
        let _submission = self.submit.lock().await;
        let locked = std::time::Instant::now();
        if order.venue != self.dex.venue() || order.client_order_id.0.is_empty() {
            return Err(error("wrong venue or empty client order ID"));
        }
        let cloid = client_cloid(&order.client_order_id);
        let reservation = self.reservation(&cloid)?;
        if let Some(saved) = &reservation
            && !same_intent(&saved.order, order)
        {
            return Err(error("client order ID reused with a different intent"));
        }
        if let Some(remote) = self.remote_status(json!(cloid)).await? {
            let state = self
                .state_from_remote(remote, Some(&order.client_order_id))
                .await?;
            return ack(&state).map(Submitted::Ack);
        }
        let checked = std::time::Instant::now();
        if reservation.is_some() {
            return Err(error(
                "reserved order ID is unresolved/expired; do not resubmit, reconcile manually",
            ));
        }
        let (asset_id, asset) = self.asset(&order.symbol).await?;
        if asset.is_delisted
            || (!order.reduce_only && !market_supports_mode(self.dex, &asset, order.margin_mode))
        {
            return Err(error(
                "market is delisted or does not support the selected margin mode",
            ));
        }
        let price = self.price(order, &asset).await?;
        let quantity = order_quantity(order, price, asset.sz_decimals)?;
        // 10 美元下限：reduce-only 恰好平掉整笔持仓时 HL 不拦（下面读到持仓后再判断）。
        // 某次实盘：开仓第一腿只吃到 0.30 USDT，回滚和之后每次平仓都被这里本地拦下，
        // 零头裸在 HL 上平不掉。
        let below_minimum = multiply(quantity, price)? < Decimal::TEN;
        if below_minimum && !order.reduce_only {
            return Err(error("order is below Hyperliquid's $10 minimum notional"));
        }
        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| error("opening order requires explicit isolated leverage"))?;
            if !value.fract().is_zero()
                || value < Decimal::ONE
                || value > Decimal::from(asset.max_leverage)
            {
                return Err(error("leverage must be an integer within the market limit"));
            }
            Some(value.to_u32().ok_or_else(|| error("invalid leverage"))?)
        };
        // Enforce reduce-only on-chain as well as checking the current position locally.
        // 读节点会落后几秒（10-17 实测 8 秒以上）：刚成交就马上回滚 / 退零头时，持仓可能还没出现。
        // 本地检查没过就隔一会儿再读，最多 REDUCE_REREADS 次；订单本身带 `r:true`，交易所也会强制只减仓。
        if order.reduce_only {
            let mut rereads = REDUCE_REREADS.iter();
            loop {
                let position = self
                    .positions()
                    .await?
                    .into_iter()
                    .find(|p| p.symbol == order.symbol);
                let reduces = position.as_ref().is_some_and(|position| {
                    let closes = (position.net_quantity > Decimal::ZERO
                        && order.side == Side::Sell)
                        || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
                    closes && quantity <= position.net_quantity.abs()
                });
                if reduces {
                    let closes_all =
                        position.is_some_and(|position| quantity == position.net_quantity.abs());
                    if below_minimum && !closes_all {
                        return Err(error(
                            "order is below Hyperliquid's $10 minimum notional (only a reduce-only order closing the whole position may be smaller)",
                        ));
                    }
                    break;
                }
                match rereads.next() {
                    Some(delay) => tokio::time::sleep(*delay).await,
                    None => {
                        return Err(error(
                            "reduce-only quantity/direction exceeds the current position",
                        ));
                    }
                }
            }
        }
        // 杠杆先设好再预约订单号：设杠杆失败（读回不一致、限频）时这张单根本没发，不能留下一个永远
        // 「查不到、不能当没成交」的预约。
        if let Some(leverage) = leverage {
            self.configure_margin(asset_id, &asset, leverage, order.margin_mode)
                .await?;
        }
        // Reserve BEFORE any mutating call. A timeout or process death never permits replay.
        self.journal
            .lock()
            .map_err(|_| error("intent journal lock poisoned"))?
            .reserve(Reservation {
                account: self.account.clone(),
                signer: self.signer_address.clone(),
                cloid: cloid.clone(),
                order: order.clone(),
                effective_quantity: quantity,
            })?;
        let price_text = price.normalize().to_string();
        let quantity_text = quantity.normalize().to_string();
        let action = OrderAction {
            kind: "order",
            grouping: "na",
            orders: [WireOrder {
                a: asset_id,
                b: order.side == Side::Buy,
                p: &price_text,
                s: &quantity_text,
                r: order.reduce_only,
                t: LimitType {
                    limit: TimeInForce { tif: "Ioc" },
                },
                c: &cloid,
            }],
        };
        let sending = std::time::Instant::now();
        let sent = self.send_order_action(&action).await;
        // 时间花在哪：`exchange_ms` 是 `/exchange` 一个往返（交易所收单、进块、回包），本地改不动；
        // 其余几段是我们自己的。11-07～11-14 第一腿中位 765ms。
        tracing::info!(
            order = %order.client_order_id,
            venue = %self.dex.venue(),
            lock_ms = locked.duration_since(entered).as_millis() as u64,
            check_ms = checked.duration_since(locked).as_millis() as u64,
            prepare_ms = sending.duration_since(checked).as_millis() as u64,
            exchange_ms = sending.elapsed().as_millis() as u64,
            "Hyperliquid 下单分段耗时"
        );
        let reply = match sent? {
            OrderSend::Accepted(reply) => reply,
            // 请求在处理前就被拒（限频 429、其它 4xx、顶层 `status: err`）：这张单没进撮合。记下结局，紧接着的
            // `order_state` 读回用它 —— 以前这里直接 `?`，预约的订单号永远「查不到、不能当没成交」，
            // 做第二腿时第一腿就一直裸着。
            OrderSend::Refused(reason) => {
                self.meta.clear();
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(reason);
                self.remember_reply(&rejected);
                return Err(error(
                    "order refused before processing; reserved ID retained",
                ));
            }
            OrderSend::Unknown(reason) => {
                return Err(error(format!("{reason}; reconcile reserved ID")));
            }
        };
        let statuses = reply
            .pointer("/response/data/statuses")
            .and_then(Value::as_array)
            .ok_or_else(|| error("missing exchange order statuses; reconcile reserved ID"))?;
        if statuses.len() != 1
            || reply.pointer("/response/type").and_then(Value::as_str) != Some("order")
        {
            return Err(error(
                "unexpected exchange order response; reconcile reserved ID",
            ));
        }
        if let Some(reason) = statuses[0].get("error") {
            // 精度、上下架之类变了也会被拒：下一次重新拉 `meta`。
            self.meta.clear();
            // 回包明确拒绝 = 没进撮合，一点没成交（IOC 没有对手盘也是这样回）。读节点要过几秒才查得到这张
            // 被拒的单：记下回包的结局，紧接着的 `order_state` 查不到时用它（见 `remember_reply`）。
            let mut rejected = OrderState::new(order.clone());
            rejected.status = OrderStatus::Rejected;
            rejected.reject_reason = Some(venue_text(Some(reason)));
            self.remember_reply(&rejected);
            return Err(error("order rejected by venue; reserved ID retained"));
        }
        let replied = self.provisional(order, &statuses[0], quantity, price, asset.sz_decimals)?;
        if quick && let Some(state) = &replied {
            // 记下回包的结局：之后 `settle` 读不全（成交明细晚到）时，`order_state` 还能按它给出成交。
            self.remember_reply(state);
            return Ok(Submitted::Provisional(Box::new(state.clone())));
        }
        // Ack only proves acceptance; require actual status + complete fills including fees.
        // 回包里有交易所单号就按单号查（同 `settle_at`）。
        let lookup = replied
            .as_ref()
            .and_then(|state| state.venue_order_id.as_deref())
            .and_then(|raw| raw.parse::<u64>().ok())
            .map_or_else(|| json!(cloid), |oid| json!(oid));
        let polled = self
            .remote_status_after_ack(&self.info_url, lookup, ORDER_READBACK_INTERVAL)
            .await;
        let remote = match (polled, replied.clone()) {
            (Ok(Some(remote)), _) => remote,
            // 轮询完读节点还是查不到（或读回出错），但回包已经给出了成交量与均价：先按回包记，不把一张
            // 已经成交的单当成结果不明（那样执行器只能中断，台账少一条腿、对账一直不干净）。
            (_, Some(state)) => {
                tracing::warn!(
                    order = %order.client_order_id,
                    filled = %state.filled_usdt,
                    "下单回包已给出成交，读节点还查不到这张单：先按回包记（手续费是估的），之后读到以读回为准"
                );
                self.remember_reply(&state);
                return ack(&state).map(Submitted::Ack);
            }
            (polled, None) => polled?
                .ok_or_else(|| error("accepted order not yet queryable; reserved ID retained"))?,
        };
        let state = match self
            .state_from_remote(remote, Some(&order.client_order_id))
            .await
        {
            Ok(state) => state,
            // 订单状态先查到、逐笔成交还没追上（读节点滞后）：`verified_status` 核不齐会报错。回包已经给出
            // 成交量与均价时先按回包记，不把一张已经成交的单当成结果不明（执行器会把第二腿记成中断、仓位卡在
            // Opening）。之后读得齐了以读回为准。
            Err(err) => match replied {
                Some(state) => {
                    tracing::warn!(
                        order = %order.client_order_id,
                        filled = %state.filled_usdt,
                        error = %err,
                        "订单查到了但成交明细还没跟上：先按下单回包记（手续费是估的），之后读到以读回为准"
                    );
                    self.remember_reply(&state);
                    return ack(&state).map(Submitted::Ack);
                }
                None => return Err(err),
            },
        };
        self.remember_terminal(&state);
        ack(&state).map(Submitted::Ack)
    }

    /// 按回包估手续费用的费率：这个合约最近一次读回的实际费率，没有就用配置的吃单费率（偏保守）。
    fn estimated_fee_rate(&self, symbol: &Symbol) -> Decimal {
        self.fee_rates
            .lock()
            .ok()
            .and_then(|rates| rates.get(symbol).copied())
            .unwrap_or(self.taker_fee)
    }

    /// IOC 回包 `{"filled":{"totalSz","avgPx","oid"}}` 里的待核实状态（[`Broker::place_for_hedge`]）。
    /// 数量就是交易所报的成交量（准的）；均价按回包往不利方向放一个单位、且不越过订单限价 —— 回包的
    /// 均价可能是舍入过的，第二腿的保本价按它算只会略严、不会放宽。单位取「回包最后一位小数」和「这个
    /// 价位一个 tick」里更细的那个：价格字符串会去掉末尾的 0（5.3600 报成 `"5.36"`），只看字符串的话
    /// 低价合约一放就是 0.01（约 0.2%），比整笔的边际还大。手续费按吃单费率估。回包里没有成交、
    /// 字段缺失或对不上（超过下单量）返回 `None`，走完整的读回。
    fn provisional(
        &self,
        order: &NewOrder,
        status: &Value,
        quantity: Decimal,
        limit: Decimal,
        sz_decimals: u32,
    ) -> ArbResult<Option<OrderState>> {
        let Some(filled) = status.get("filled") else {
            return Ok(None);
        };
        let field = |key: &str| {
            filled
                .get(key)
                .and_then(Value::as_str)
                .and_then(|raw| Decimal::from_str(raw).ok())
        };
        let (Some(total), Some(reported), Some(oid)) = (
            field("totalSz"),
            field("avgPx"),
            filled.get("oid").and_then(Value::as_u64),
        ) else {
            return Ok(None);
        };
        if total <= Decimal::ZERO || total > quantity || reported <= Decimal::ZERO {
            return Ok(None);
        }
        let unit = Decimal::new(
            1,
            reported
                .scale()
                .max(tick_decimals(reported, sz_decimals))
                .min(28),
        );
        let price = match order.side {
            Side::Buy => (reported + unit).min(limit.max(reported)),
            Side::Sell => (reported - unit).max(limit.min(reported)),
        };
        let notional = multiply(total, price)?;
        let mut state = OrderState::new(order.clone());
        state.venue_order_id = Some(oid.to_string());
        // IOC：没成交完的部分已经撤了（和读回时 `verified_status` 的判定一致）。
        state.status = if total == quantity {
            OrderStatus::Filled
        } else {
            OrderStatus::Cancelled
        };
        state.filled_usdt = notional;
        state.average_price = Some(price);
        state.fee_usdt = multiply(notional, self.estimated_fee_rate(&order.symbol))?;
        state.fee_estimated = true;
        Ok(Some(state))
    }

    /// [`Broker::settle_hedge`]：按回包里的 oid 轮询订单状态（读节点会落后几秒），再按逐笔成交还原；
    /// 逐笔成交也可能还没追上，对不上就隔一会儿再读，最多 [`SETTLE_ATTEMPTS`] 次。
    async fn settle_at(
        &self,
        info_url: &str,
        provisional: &OrderState,
        interval: Duration,
    ) -> ArbResult<Option<OrderState>> {
        let id = &provisional.order.client_order_id;
        let oid = match provisional
            .venue_order_id
            .as_deref()
            .and_then(|raw| raw.parse::<u64>().ok())
        {
            Some(oid) => json!(oid),
            None => json!(client_cloid(id)),
        };
        let mut last = Err(error("order not settled"));
        for attempt in 0..SETTLE_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(interval).await;
            }
            let Some(remote) = self
                .remote_status_after_ack(info_url, oid.clone(), interval)
                .await?
            else {
                return Err(error(
                    "accepted order not yet queryable; reserved ID retained",
                ));
            };
            last = self.state_from_remote(remote, Some(id)).await;
            if let Ok(state) = &last {
                self.remember_terminal(state);
                break;
            }
        }
        last.map(Some)
    }
}

#[async_trait]
impl Broker for HyperliquidBroker {
    /// 只读预热：市场列表（顺手进缓存）与这个币当前的逐仓杠杆，一致就记下，之后的准备与下单
    /// 不再查。要改杠杆的留给 [`Broker::prepare_open_mode`]（下单已定之后）。
    /// 下单按 `szDecimals` 位小数向零取整（[`order_quantity`]）。
    async fn quantity_step(&self, symbol: &Symbol) -> ArbResult<Option<Decimal>> {
        let (_, asset) = self.asset(symbol).await?;
        lot_step(asset.sz_decimals).map(Some)
    }

    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let Some(leverage) = leverage else {
            return Ok(());
        };
        let (_, asset) = self.asset(symbol).await?;
        let leverage = crate::margin::leverage(self.dex.venue(), Some(leverage))?
            .to_u32()
            .ok_or_else(|| error("invalid leverage"))?;
        let mode = crate::MarginMode::Isolated;
        // 不合法的组合不在这里报：留给准备阶段按原来的路径拒绝。
        if leverage > asset.max_leverage || !market_supports_mode(self.dex, &asset, mode) {
            return Ok(());
        }
        let key = (asset.name.clone(), leverage, mode);
        if !self.verified_leverage.is_fresh(&key) {
            let current: Value = post(
                &self.client,
                &self.info_url,
                &json!({"type":"activeAssetData", "user":self.account, "coin":asset.name}),
            )
            .await?;
            if leverage_matches(&current, &asset.name, leverage, mode) {
                self.mark_verified(key);
            }
        }
        Ok(())
    }

    async fn prepare_open_mode(
        &self,
        symbol: &Symbol,
        _: Side,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.authorize()?;
        let _guard = self.submit.lock().await;
        let (asset_id, asset) = self.asset(symbol).await?;
        let leverage = crate::margin::leverage(self.dex.venue(), leverage)?
            .to_u32()
            .ok_or_else(|| error("invalid leverage"))?;
        if leverage > asset.max_leverage {
            return Err(error("leverage exceeds market limit"));
        }
        self.configure_margin(asset_id, &asset, leverage, mode)
            .await
    }

    fn venue(&self) -> Venue {
        self.dex.venue()
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        crate::broker::refuse_post_only(order)?;
        match self.submit_order(order, false).await? {
            Submitted::Ack(ack) => Ok(ack),
            Submitted::Provisional(state) => ack(&state),
        }
    }

    async fn place_for_hedge(&self, order: &NewOrder) -> ArbResult<Option<OrderState>> {
        match self.submit_order(order, true).await? {
            Submitted::Ack(_) => Ok(None),
            Submitted::Provisional(state) => Ok(Some(*state)),
        }
    }

    async fn settle_hedge(&self, provisional: &OrderState) -> ArbResult<Option<OrderState>> {
        self.settle_at(&self.info_url, provisional, ORDER_READBACK_INTERVAL)
            .await
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let external_prefix = format!("{}:oid:", self.dex.venue().as_str());
        let oid = match client_order_id.0.strip_prefix(&external_prefix) {
            Some(raw) => json!(
                raw.parse::<u64>()
                    .map_err(|_| error("invalid external order ID"))?
            ),
            None => {
                let cloid = client_cloid(client_order_id);
                if let Some(state) = self
                    .known_terminal(&cloid)
                    .filter(|state| state.order.client_order_id == *client_order_id)
                {
                    return Ok(Some(state));
                }
                json!(cloid)
            }
        };
        let cloid = client_cloid(client_order_id);
        let remote = match self.remote_status(oid).await {
            Ok(remote) => remote,
            // 读回出错：下单回包给过结局就先用它（见 `remember_reply`）。
            Err(err) => return self.replied_state(&cloid).map(Some).ok_or(err),
        };
        let Some(remote) = remote else {
            if let Some(state) = self.replied_state(&cloid) {
                return Ok(Some(state));
            }
            if self.reservation(&cloid)?.is_some() {
                return Err(error(
                    "reserved order no longer queryable; cannot assume it never filled",
                ));
            }
            return Ok(None);
        };
        let state = match self.state_from_remote(remote, Some(client_order_id)).await {
            Ok(state) => state,
            // 订单查到了、成交明细还没跟上：同读回出错，回包给过结局就先用它。
            Err(err) => return self.replied_state(&cloid).map(Some).ok_or(err),
        };
        self.remember_terminal(&state);
        self.forget_reply(&cloid);
        Ok(Some(state))
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        let oid = venue_order_id
            .parse::<u64>()
            .map_err(|_| error("invalid venue order ID"))?;
        let remote = self
            .remote_status(json!(oid))
            .await?
            .ok_or_else(|| error("cannot verify order to cancel"))?;
        if !map_status(&remote.status)?.is_live() {
            return Ok(());
        }
        let (asset, _) = self
            .asset(&symbol_of(self.dex, &remote.order.coin)?)
            .await?;
        let reply = self
            .exchange(&CancelAction {
                kind: "cancel",
                cancels: [WireCancel { a: asset, o: oid }],
            })
            .await?;
        let statuses = reply
            .pointer("/response/data/statuses")
            .and_then(Value::as_array)
            .ok_or_else(|| error("missing cancel statuses"))?;
        if statuses.len() != 1 || statuses[0].as_str() != Some("success") {
            return Err(error("cancel not confirmed"));
        }
        let after = self
            .remote_status(json!(oid))
            .await?
            .ok_or_else(|| error("cancelled order not queryable"))?;
        if map_status(&after.status)?.is_live() {
            return Err(error("order still live after cancellation"));
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let orders: Vec<RemoteOrder> = post(
            &self.client,
            &self.info_url,
            &json!({
                "type":"frontendOpenOrders", "user":self.account,
                "dex":self.dex.info_dex().unwrap_or(""),
            }),
        )
        .await?;
        let mut result = Vec::with_capacity(orders.len());
        for order in orders {
            // A per-dex endpoint can still return unexpected coins: never silently hide
            // spot, foreign-dex or main-dex orders from reconciliation.
            symbol_of(self.dex, &order.coin)?;
            let remote = self.remote_status(json!(order.oid)).await?.ok_or_else(|| {
                error("open order vanished during reconciliation; retry snapshot")
            })?;
            let state = self.state_from_remote(remote, None).await?;
            if state.status.is_live() {
                result.push(state);
            }
        }
        Ok(result)
    }

    /// `clearinghouseState` 里这个币的 `liquidationPx` 与 `marginUsed`。
    ///
    /// 逐仓持仓的 `marginUsed` 是 保证金 + 浮动盈亏（2026-10-03 实测 511 个逐仓持仓：
    /// `marginUsed = rawUsd + szi·entryPx + unrealizedPnl`），所以这里报告 `rawUsd + szi·entryPx`。
    /// 补保证金回读比较 `rawUsd`，同时核对数量与入场价未变（见 `isolated_leg`）。
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<VenueLegState>> {
        let state: Value = post(
            &self.client,
            &self.info_url,
            &json!({
                "type":"clearinghouseState", "user":self.account,
                "dex":self.dex.info_dex().unwrap_or(""),
            }),
        )
        .await?;
        let rows = state
            .get("assetPositions")
            .and_then(Value::as_array)
            .ok_or_else(|| error("missing clearinghouse positions"))?;
        for row in rows {
            let Some(position) = row.get("position") else {
                continue;
            };
            let coin = position.get("coin").and_then(Value::as_str).unwrap_or("");
            if symbol_of(self.dex, coin).ok().as_ref() != Some(symbol) {
                continue;
            }
            return Ok(Some(leg_state_from_position(position)));
        }
        Ok(None)
    }

    /// 能拿来开新仓的保证金。
    ///
    /// - 统一账户 / 组合保证金：余额在现货状态里，按本 dex 的保证金币（`meta.collateralToken`，
    ///   主 dex / xyz / io 都是 USDC = 0）取可用额。这时各 dex 的 `clearinghouseState` 恒为 0，
    ///   按它算会把有钱的账户当成 0（2026-10-09 io 开仓被误拒）。
    /// - 其它（标准模式）：本 dex 的账户价值减去已占用保证金；各 dex 的余额互相独立。
    ///   HL 的 `withdrawable` 是能提走的钱，和「能拿来开新仓」不是一回事，所以不用它。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        self.free_collateral_at(&self.info_url).await
    }

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// 往本 dex 的逐仓持仓补保证金：**一次调用最多一次写请求，绝不内部重试**。
    ///
    /// - `Err`（没发请求）：只读模式、金额 ≤ 0 或超过 6 位小数、币不存在、没有持仓、持仓不是
    ///   逐仓（`leverage.type != "isolated"`）、读不到逐仓账本。
    /// - `Refused`：交易所 `{"status":"err","response":"…"}`、HTTP 429、其余 4xx（请求在处理前
    ///   就被拒）。资金是**本 dex 自己的余额**（xyz/io 与主 dex 独立，统一账户除外），不够就是
    ///   这里的 `err`。
    /// - `Applied`：只在回读到 `rawUsd` 增加 ≥ 99% 金额（且持仓量、入场价没变）时。
    /// - `Unknown`：超时/传输错误/5xx/受理了但回读 6 次（间隔 500ms）都没看到增加；回读发现
    ///   保证金反而减少时会在原因里明说（符号约定只有第三方 SDK 佐证，见模块文档）。
    ///
    /// 官方接口：<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#update-isolated-margin>
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.add_margin_at(
            &self.info_url,
            &self.exchange_url,
            MARGIN_READBACK_INTERVAL,
            symbol,
            amount_usdt,
        )
        .await
    }

    /// `userFillsByTime`（公开）：所有 dex 的成交混在一起（HIP-3 的币名带前缀），按本券商这个
    /// dex 的合约过滤。一次最多 2000 条，满页就拒绝下结论（不完整的成交算不出盈亏）。
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<Vec<crate::settlement::VenueFill>>> {
        let page: Vec<UserFill> = post(
            &self.client,
            &self.info_url,
            &json!({
                "type":"userFillsByTime", "user":self.account,
                "startTime":since.timestamp_millis(), "endTime":until.timestamp_millis(),
            }),
        )
        .await?;
        if page.len() >= FILLS_PAGE {
            return Err(error(
                "fill history page is full; it may be truncated, so the pnl would be wrong",
            ));
        }
        let mut fills = Vec::new();
        for row in page {
            if symbol_of(self.dex, &row.coin).ok().as_ref() != Some(symbol) {
                continue;
            }
            fills.push(fill_from_user_fill(&row)?);
        }
        Ok(Some(fills))
    }

    /// `userFunding`（公开）：同一个地址所有 dex 的资金费都在里面（HIP-3 的币名带
    /// `dex:` 前缀），按本券商这个 dex 的合约过滤。`usdc` 正 = 收到；一次最多 500 条，
    /// 满页就从最后一条之后接着查。
    ///
    /// 拉到的原始流水按（接口, 地址）共享一会儿（见 [`SharedFunding`]）。
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        self.funding_since_at(symbol, since, chrono::Utc::now())
            .await
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let response: Value = post(
            &self.client,
            &self.info_url,
            &json!({
                "type":"clearinghouseState", "user":self.account,
                "dex":self.dex.info_dex().unwrap_or(""),
            }),
        )
        .await?;
        let rows = response
            .get("assetPositions")
            .and_then(Value::as_array)
            .ok_or_else(|| error("missing clearinghouse positions"))?;
        let mut positions = Vec::with_capacity(rows.len());
        for row in rows {
            // HIP-3 markets are isolated-only; a hedge (two-sided) position means the
            // account is in a mode this broker cannot size or close safely.
            if self.dex.info_dex().is_some()
                && row.get("type").and_then(Value::as_str) == Some("hedge")
            {
                return Err(error("hedge-mode position on an isolated-only HIP-3 dex"));
            }
            let position = row
                .get("position")
                .ok_or_else(|| error("missing position data"))?;
            let coin = position
                .get("coin")
                .and_then(Value::as_str)
                .ok_or_else(|| error("missing position coin"))?;
            let quantity = decimal_field(position, "szi")?;
            if quantity.is_zero() {
                continue;
            }
            let entry = decimal_field(position, "entryPx")?;
            let notional = decimal_field(position, "positionValue")?;
            if entry <= Decimal::ZERO || notional < Decimal::ZERO {
                return Err(error("invalid position valuation"));
            }
            positions.push(VenuePosition {
                venue: self.dex.venue(),
                symbol: symbol_of(self.dex, coin)?,
                net_quantity: quantity,
                average_price: Some(entry),
                notional_usdt: notional,
            });
        }
        Ok(positions)
    }
}

impl Journal {
    fn open(path: &Path, account: &str, signer: &str, venue: Venue) -> ArbResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()
            .map_err(|_| error("intent journal is already in use or cannot be locked"))?;
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        if !text.is_empty() && !text.ends_with('\n') {
            return Err(error(
                "torn intent journal record; manual reconciliation required",
            ));
        }
        let mut by_cloid = HashMap::new();
        for line in text.lines() {
            let item: Reservation =
                serde_json::from_str(line).map_err(|_| error("invalid intent journal record"))?;
            if item.account != account
                || item.signer != signer
                || item.order.venue != venue
                || item.cloid != client_cloid(&item.order.client_order_id)
                || item.effective_quantity <= Decimal::ZERO
            {
                return Err(error("intent journal identity/quantity mismatch"));
            }
            if by_cloid.insert(item.cloid.clone(), item).is_some() {
                return Err(error("duplicate reserved ID in intent journal"));
            }
        }
        // Persist a newly-created file and its directory before any future reservation.
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(Self {
            file,
            by_cloid,
            poisoned: false,
        })
    }

    fn reserve(&mut self, item: Reservation) -> ArbResult<()> {
        if self.poisoned || self.by_cloid.contains_key(&item.cloid) {
            return Err(error("cannot reserve order ID"));
        }
        let mut bytes =
            serde_json::to_vec(&item).map_err(|_| error("cannot serialize intent journal"))?;
        bytes.push(b'\n');
        // Poison first, including if a short write or sync failure leaves a partial record.
        self.poisoned = true;
        self.by_cloid.insert(item.cloid.clone(), item);
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        self.poisoned = false;
        Ok(())
    }
}

fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue("hyperliquid", message)
}

fn leg_state_from_position(position: &Value) -> VenueLegState {
    let decimal = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .and_then(|raw| Decimal::from_str(raw).ok())
    };
    let number = |key: &str| decimal(position.get(key)).filter(|value| *value > Decimal::ZERO);
    let isolated = position.pointer("/leverage/type").and_then(Value::as_str) == Some("isolated");
    // 逐仓：`marginUsed` 含浮动盈亏，不是强平公式里的保证金；存进去的那份是 `rawUsd + szi·entryPx`
    // （多头 rawUsd = 保证金 − 入场名义，空头 = 保证金 + 入场名义）。
    let deposited = (|| {
        let leverage = position.get("leverage")?;
        if !isolated {
            return None;
        }
        let raw_usd = decimal(leverage.get("rawUsd"))?;
        let signed_notional =
            decimal(position.get("szi"))?.checked_mul(decimal(position.get("entryPx"))?)?;
        raw_usd.checked_add(signed_notional)
    })()
    .filter(|margin| *margin > Decimal::ZERO);
    VenueLegState {
        margin_mode: position
            .pointer("/leverage/type")
            .and_then(Value::as_str)
            .and_then(crate::margin::reported_mode),
        // 逐仓账本缺失时不知道存入保证金，不能退回含浮盈亏的权益。
        margin_usdt: if isolated { deposited } else { None },
        liquidation_price: number("liquidationPx"),
    }
}

/// `amount` USDC → `ntli`（×1e6 的整数）。不四舍五入：超过 6 位小数就拒绝（官方 Python SDK 的
/// `float_to_usd_int` 同样在需要舍入时报错）。
fn margin_ntli(amount: Decimal) -> ArbResult<i64> {
    if amount <= Decimal::ZERO {
        return Err(error("margin amount must be positive"));
    }
    let scaled = multiply(amount, Decimal::from(1_000_000u32))?;
    if !scaled.fract().is_zero() {
        return Err(error(
            "margin amount has more than 6 decimals; refusing to round it",
        ));
    }
    scaled
        .to_i64()
        .ok_or_else(|| error("margin amount overflows ntli"))
}

/// 一条逐仓持仓的账本及入场规格；用于排除成交导致的账本变化。
#[derive(Debug, Clone, PartialEq, Eq)]
struct IsolatedLeg {
    /// 带符号持仓量 `szi`。回读时它变了说明持仓被动过，账本变化不能算到这次补仓头上。
    size: Decimal,
    entry_price: Decimal,
    /// `leverage.rawUsd`：这个逐仓持仓的 USD 账本（多头为负，= 保证金 − 入场名义）。
    /// 补保证金只加它，不随标记价格变；资金费结算、手续费会小幅改它。
    raw_usd: Decimal,
}

/// 从 `clearinghouseState` 取出 `symbol` 的逐仓持仓。没有持仓、同一币多行、不是逐仓、缺
/// `rawUsd` / 有效入场价一律 `Err`（调用方据此「没发请求」或「回读未确认」）。
fn isolated_leg(state: &Value, dex: HyperliquidDex, symbol: &Symbol) -> ArbResult<IsolatedLeg> {
    let rows = state
        .get("assetPositions")
        .and_then(Value::as_array)
        .ok_or_else(|| dex.error("missing clearinghouse positions"))?;
    let mut found = None;
    for row in rows {
        let Some(position) = row.get("position") else {
            continue;
        };
        let coin = position.get("coin").and_then(Value::as_str).unwrap_or("");
        if symbol_of(dex, coin).ok().as_ref() != Some(symbol) {
            continue;
        }
        if found.replace(position).is_some() {
            return Err(dex.error(format!(
                "ambiguous clearinghouse state: several positions for {symbol}"
            )));
        }
    }
    let no_position = || {
        dex.error(format!(
            "no open position on {symbol}; nothing to add margin to"
        ))
    };
    let position = found.ok_or_else(no_position)?;
    let size = decimal_field(position, "szi")?;
    if size.is_zero() {
        return Err(no_position());
    }
    let entry_price = decimal_field(position, "entryPx")?;
    if entry_price <= Decimal::ZERO {
        return Err(dex.error("isolated position has an invalid entry price"));
    }
    match position.pointer("/leverage/type").and_then(Value::as_str) {
        Some("isolated") => {}
        Some(other) => {
            return Err(dex.error(format!(
                "{symbol} position uses {other} margin, not isolated; refusing to add isolated margin"
            )));
        }
        None => {
            return Err(dex.error(format!(
                "{symbol} position margin mode is missing from the clearinghouse state"
            )));
        }
    }
    let raw_usd = position
        .pointer("/leverage/rawUsd")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            dex.error(format!(
                "{symbol} isolated margin ledger (rawUsd) is missing; cannot confirm a top-up"
            ))
        })
        .and_then(decimal)?;
    Ok(IsolatedLeg {
        size,
        entry_price,
        raw_usd,
    })
}

/// 回读结论（纯函数，见 [`judge_margin_change`]）。
#[derive(Debug, PartialEq, Eq)]
enum MarginJudgement {
    /// 持仓量与入场价没变，账本增加 ≥ 99% 的金额。
    Confirmed,
    /// 持仓量或入场价变了：账本变化可能是成交/强平造成的，不能算到补保证金头上。
    PositionChanged,
    /// 账本减少了至少一半的金额：符号约定可能反了，或者被别的动作扣走了。
    Decreased { delta: Decimal },
    /// 账本没有增加到要求。
    NotIncreased { delta: Decimal },
}

fn judge_margin_change(
    before: &IsolatedLeg,
    after: &IsolatedLeg,
    amount: Decimal,
) -> MarginJudgement {
    if before.size != after.size || before.entry_price != after.entry_price {
        return MarginJudgement::PositionChanged;
    }
    let Some(delta) = after.raw_usd.checked_sub(before.raw_usd) else {
        return MarginJudgement::NotIncreased {
            delta: Decimal::ZERO,
        };
    };
    if delta >= amount * MARGIN_CONFIRM_RATIO {
        MarginJudgement::Confirmed
    } else if delta <= -(amount / Decimal::TWO) {
        MarginJudgement::Decreased { delta }
    } else {
        MarginJudgement::NotIncreased { delta }
    }
}

/// `/exchange` 对下单的应答分类（纯函数，见 [`classify_order_reply`]）。
#[derive(Debug, PartialEq)]
enum OrderSend {
    /// `{"status":"ok"}`：受理了，每张单的结局在 `statuses` 里。
    Accepted(Value),
    /// 请求在处理前就被拒：这张单没进撮合。
    Refused(String),
    /// 可能已经被处理：结局要读回。
    Unknown(String),
}

/// 下单应答：2xx 看顶层 `status`（`ok` 受理、`err` 处理前被拒、其它不知道）；429 和其它 4xx（408 除外）在处理前
/// 就被拒；408、5xx 和其它状态码不知道。与补保证金的 [`classify_margin_reply`] 同一口径。
fn classify_order_reply(status: u16, body: Option<&str>) -> OrderSend {
    match status {
        200..=299 => {
            let Some(reply) = body.and_then(|text| serde_json::from_str::<Value>(text).ok()) else {
                return OrderSend::Unknown(format!(
                    "HTTP {status} with an unreadable body; the order may have been placed"
                ));
            };
            match reply.get("status").and_then(Value::as_str) {
                Some("ok") => OrderSend::Accepted(reply),
                Some("err") => OrderSend::Refused(format!(
                    "exchange rejected the order before processing: {}",
                    venue_text(reply.get("response"))
                )),
                _ => OrderSend::Unknown(format!(
                    "HTTP {status} with an unrecognised reply; the order may have been placed"
                )),
            }
        }
        429 => OrderSend::Refused(
            "HTTP 429: rate limited before processing; the order was not placed".to_string(),
        ),
        408 | 500..=599 => OrderSend::Unknown(format!(
            "HTTP {status}; the outcome is unknown and the order may have been placed"
        )),
        400..=499 => OrderSend::Refused(format!(
            "HTTP {status}: request rejected before processing; the order was not placed"
        )),
        _ => OrderSend::Unknown(format!(
            "HTTP {status} is unexpected; the order may have been placed"
        )),
    }
}

/// `/exchange` 对 `updateIsolatedMargin` 的应答分类（纯函数，见 [`classify_margin_reply`]）。
#[derive(Debug, PartialEq, Eq)]
enum MarginSend {
    /// `{"status":"ok"}`：受理了，但是否生效只认回读。
    Accepted,
    /// 交易所明确拒绝、请求在处理前就被拒：没动钱。
    Refused(String),
    /// 请求可能已经被处理：动没动钱不知道。
    Unknown(String),
}

/// HL 把业务失败放在 `200 + {"status":"err","response":"<string>"}`；`429` 是限频（处理前拒绝）；
/// 其余 4xx（如反序列化失败的 422）也是处理前拒绝；5xx、408、3xx 与任何读不懂的 2xx 都按
/// 「不知道」处理。错误文本只取场所返回的 `response` 字符串（去控制字符、截断），不回显请求体。
fn classify_margin_reply(status: u16, body: Option<&str>) -> MarginSend {
    match status {
        200..=299 => {
            let Some(reply) = body.and_then(|text| serde_json::from_str::<Value>(text).ok()) else {
                return MarginSend::Unknown(format!(
                    "HTTP {status} with an unreadable body; the action may have been applied"
                ));
            };
            match reply.get("status").and_then(Value::as_str) {
                Some("ok") => MarginSend::Accepted,
                Some("err") => MarginSend::Refused(format!(
                    "exchange rejected updateIsolatedMargin: {}",
                    venue_text(reply.get("response"))
                )),
                _ => MarginSend::Unknown(format!(
                    "HTTP {status} with an unrecognised reply; the action may have been applied"
                )),
            }
        }
        429 => MarginSend::Refused(
            "HTTP 429: rate limited before processing; no margin was added".to_string(),
        ),
        408 | 500..=599 => MarginSend::Unknown(format!(
            "HTTP {status}; the outcome is unknown and the action may have been applied"
        )),
        400..=499 => MarginSend::Refused(format!(
            "HTTP {status}: request rejected before processing (response body redacted)"
        )),
        _ => MarginSend::Unknown(format!(
            "HTTP {status} is unexpected; the action may have been applied"
        )),
    }
}

/// 场所返回的错误文本：去掉控制字符并截到 200 个字符；不是字符串就说「没有说明」。
fn venue_text(value: Option<&Value>) -> String {
    let text: String = value
        .and_then(Value::as_str)
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    if text.is_empty() {
        "no message".to_string()
    } else {
        text
    }
}

/// 传输层错误的说明：只给类别，不带 reqwest 的错误全文（里面可能有 URL 与内部细节）。
fn transport_reason(error: &reqwest::Error) -> String {
    let kind = if error.is_timeout() {
        "request timed out"
    } else if error.is_connect() {
        "connection failed"
    } else {
        "transport error"
    };
    format!("{kind}; the action may or may not have been applied")
}

/// `userFillsByTime` 一页的上限。
const FILLS_PAGE: usize = 2000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserFill {
    coin: String,
    px: String,
    sz: String,
    /// `B` 买 / `A` 卖。
    side: String,
    /// 毫秒。
    time: i64,
    fee: String,
    #[serde(default)]
    fee_token: Option<String>,
    /// `Open Long` / `Close Short` / `Buy` / `Sell` …
    #[serde(default)]
    dir: Option<String>,
}

fn fill_from_user_fill(row: &UserFill) -> ArbResult<crate::settlement::VenueFill> {
    use crate::settlement::{FillEffect, VenueFill};
    // 手续费不是 USDC 计价（比如用别的币抵扣）时折不成计价币，不猜。
    if row
        .fee_token
        .as_deref()
        .is_some_and(|token| token != "USDC")
    {
        return Err(error("fill fee is not in USDC; cannot convert it safely"));
    }
    let side = match row.side.as_str() {
        "B" => Side::Buy,
        "A" => Side::Sell,
        _ => return Err(error("fill has an unknown side")),
    };
    let effect = match row.dir.as_deref() {
        Some(dir) if dir.starts_with("Open") => FillEffect::Open,
        Some(dir) if dir.starts_with("Close") => FillEffect::Close,
        _ => FillEffect::Unknown,
    };
    let number =
        |raw: &str| Decimal::from_str(raw).map_err(|_| error("fill has an invalid number"));
    Ok(VenueFill {
        at: chrono::DateTime::from_timestamp_millis(row.time)
            .ok_or_else(|| error("fill has an invalid time"))?,
        side,
        quantity: number(&row.sz)?,
        price: number(&row.px)?,
        fee_usdt: number(&row.fee)?,
        effect,
    })
}

/// `userFunding` 一页的上限与最多翻几页。
const FUNDING_PAGE: usize = 500;
const FUNDING_PAGES: usize = 20;

/// 一页 `userFunding` 里属于这个 dex 这个合约的（时间, 金额）。别的 dex、现货或者
/// 认不出的币都不是这条腿的，跳过。
fn funding_rows(
    dex: HyperliquidDex,
    symbol: &Symbol,
    page: Vec<UserFunding>,
) -> ArbResult<Vec<(chrono::DateTime<chrono::Utc>, Decimal)>> {
    let mut rows = Vec::new();
    for row in page {
        if symbol_of(dex, &row.delta.coin).ok().as_ref() != Some(symbol) {
            continue;
        }
        let at = chrono::DateTime::from_timestamp_millis(row.time)
            .ok_or_else(|| error("funding row has an invalid time"))?;
        let usdc = Decimal::from_str(&row.delta.usdc)
            .map_err(|_| error("funding row has an invalid amount"))?;
        rows.push((at, usdc));
    }
    Ok(rows)
}

#[derive(Debug, Clone, Deserialize)]
struct UserFunding {
    /// 毫秒。
    time: i64,
    delta: FundingDelta,
}

/// 同一个地址的 `userFunding` 原始流水（全部 dex）：从 `start`（毫秒）起，`fetched` 时拉的。
///
/// `userFunding` 按地址返回全部 dex 的流水，而后台刷新资金费是每条腿各查一次、各自从开仓时刻起翻页：
/// 同一个地址上几条腿（主站、xyz、io 的不同合约）就把同一份数据拉几遍。按条目计权的接口这样很快把
/// 额度吃光（2026-10-24/25 东京每小时十几到四十几次 429，大半是它），连带拉盘口、读保证金一起被拒。
/// 所以拉过的原始流水按（接口, 地址）共享一会儿：起点不晚于这次要的，就直接按 dex 和合约过滤。
struct SharedFunding {
    fetched: chrono::DateTime<chrono::Utc>,
    start: i64,
    rows: Vec<UserFunding>,
}

/// [`SharedFunding`] 能用多久。后台刷新一轮按开仓时间把所有腿挨个查完只要几秒。
const SHARED_FUNDING_TTL: Duration = Duration::from_secs(10);
/// 整点结算的流水最晚多久能查到：拉取时刻早于「最近一个整点 + 这么久」的那份可能缺这一笔。
const FUNDING_SETTLE_GRACE: Duration = Duration::from_secs(60);

impl SharedFunding {
    /// `now` 还能不能用：没过 [`SHARED_FUNDING_TTL`]，并且拉的时候最近一次整点结算已经查得到了
    /// （止盈复核会强制重查资金费，不能拿结算之前那份顶替）。
    fn usable_at(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        let hour = now.timestamp().div_euclid(3600) * 3600;
        let settled = hour + FUNDING_SETTLE_GRACE.as_secs() as i64;
        now.signed_duration_since(self.fetched)
            .to_std()
            .is_ok_and(|age| age < SHARED_FUNDING_TTL)
            && self.fetched.timestamp() >= settled
    }
}

fn shared_funding() -> &'static SyncMutex<HashMap<(String, String), SharedFunding>> {
    static SHARED: std::sync::OnceLock<SyncMutex<HashMap<(String, String), SharedFunding>>> =
        std::sync::OnceLock::new();
    SHARED.get_or_init(|| SyncMutex::new(HashMap::new()))
}

#[derive(Debug, Clone, Deserialize)]
struct FundingDelta {
    coin: String,
    /// 正 = 收到。
    usdc: String,
}

async fn post<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    body: &Value,
) -> ArbResult<T> {
    let response = client.post(url).json(body).send().await?;
    if !response.status().is_success() {
        return Err(error(format!(
            "HTTP {}; response body redacted",
            response.status()
        )));
    }
    response.json().await.map_err(ArbError::from)
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> ArbResult<T> {
    serde_json::from_value(value).map_err(|_| error("malformed venue response"))
}

fn decimal(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw).map_err(|_| error("invalid venue decimal"))
}

fn positive(raw: &str) -> ArbResult<Decimal> {
    let value = decimal(raw)?;
    if value <= Decimal::ZERO {
        return Err(error("expected positive venue decimal"));
    }
    Ok(value)
}

fn decimal_field(value: &Value, key: &str) -> ArbResult<Decimal> {
    decimal(
        value
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| error(format!("missing decimal {key}")))?,
    )
}

fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b)
        .ok_or_else(|| error("decimal multiplication overflow"))
}

fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b)
        .ok_or_else(|| error("decimal division overflow or zero divisor"))
}

fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b)
        .ok_or_else(|| error("decimal addition overflow"))
}

fn parse_side(side: &str) -> ArbResult<Side> {
    match side {
        "B" => Ok(Side::Buy),
        "A" => Ok(Side::Sell),
        _ => Err(error("unknown order side")),
    }
}

/// Raw venue coin name → cross-venue [`Symbol`]. Fails closed for any name that does not
/// belong to `dex`: spot, outcome, the main dex, or another HIP-3 dex. For HIP-3 the name
/// must carry the instance's own `{dex}:` prefix and is mapped back through the same
/// verified aliases as `crates/venues/src/hyperliquid.rs`.
fn symbol_of(dex: HyperliquidDex, coin: &str) -> ArbResult<Symbol> {
    match dex {
        HyperliquidDex::Main => {
            if coin.is_empty() || coin.contains([':', '/', '@', '#']) {
                return Err(error(
                    "spot/HIP-3/outcome order encountered; main-perp reconciliation is incomplete",
                ));
            }
            Ok(Symbol::perp(
                coin,
                if matches!(coin, "HYPE" | "PURR") {
                    "USDC"
                } else {
                    "USDT"
                },
            ))
        }
        _ => {
            let prefix = dex.coin_prefix();
            let bare = coin.strip_prefix(prefix).ok_or_else(|| {
                dex.error(format!(
                    "coin does not carry the {} prefix; foreign-dex or spot market",
                    prefix.trim_end_matches(':')
                ))
            })?;
            if bare.is_empty() || bare.contains([':', '/', '@', '#']) {
                return Err(dex.error("invalid HIP-3 coin name"));
            }
            let upper = bare.to_ascii_uppercase();
            let base = dex
                .aliases()
                .iter()
                .find(|(from, _)| *from == upper)
                .map_or(upper, |(_, to)| (*to).to_string());
            Ok(Symbol::perp(base, "USDT"))
        }
    }
}

/// HIP-3 all-in taker multiplier from a dex's `deployerFeeScale`.
///
/// Official rule: `scale < 1` (builder share up to 100%) → the user pays `1 + scale`
/// units of the base rate; `scale >= 1` → the user pays `2 * scale` units. Growth-mode
/// markets pay a further 10%, which this deliberately does NOT apply per market (it can
/// only overstate the fee, never understate it).
/// <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees>
/// dex 一级与在交易合约的部署者费率倍数里取最大；都没有时是 `None`。
fn max_fee_scale(dex_scale: Option<&str>, universe: &[Asset]) -> ArbResult<Option<Decimal>> {
    let mut max: Option<Decimal> = None;
    let listed = universe
        .iter()
        .filter(|asset| !asset.is_delisted)
        .filter_map(|asset| asset.deployer_fee_scale.as_deref());
    for raw in dex_scale.into_iter().chain(listed) {
        let scale = decimal(raw)?;
        max = Some(max.map_or(scale, |current| current.max(scale)));
    }
    Ok(max)
}

fn hip3_fee_scale(deployer_fee_scale: Decimal) -> ArbResult<Decimal> {
    if deployer_fee_scale < Decimal::ZERO {
        return Err(error("negative HIP-3 deployer fee scale"));
    }
    if deployer_fee_scale < Decimal::ONE {
        add(deployer_fee_scale, Decimal::ONE)
    } else {
        multiply(deployer_fee_scale, Decimal::TWO)
    }
}

/// 下单数量步长（见 [`Broker::quantity_step`]）：[`order_quantity`] 按 `szDecimals` 位小数向零取整。
fn lot_step(sz_decimals: u32) -> ArbResult<Decimal> {
    if sz_decimals > 6 {
        return Err(error("invalid szDecimals"));
    }
    Ok(Decimal::new(1, sz_decimals))
}

fn order_quantity(order: &NewOrder, price: Decimal, decimals: u32) -> ArbResult<Decimal> {
    if decimals > 6 || price <= Decimal::ZERO || order.notional_usdt <= Decimal::ZERO {
        return Err(error("invalid precision, notional or price"));
    }
    if order.reduce_only && order.quantity.is_none() {
        return Err(error("reduce-only order requires exact base quantity"));
    }
    let quantity = match order.quantity {
        Some(q) => q,
        None => divide(order.notional_usdt, price)?,
    };
    let rounded = quantity.round_dp_with_strategy(decimals, RoundingStrategy::ToZero);
    if rounded <= Decimal::ZERO {
        return Err(error("quantity below minimum lot size"));
    }
    if order.reduce_only && rounded != quantity {
        return Err(error(
            "reduce-only quantity is not exactly representable at venue lot size",
        ));
    }
    Ok(rounded)
}

fn tick_price(price: Decimal, sz_decimals: u32, side: Side) -> ArbResult<Decimal> {
    if price <= Decimal::ZERO || sz_decimals > 6 {
        return Err(error("invalid price or szDecimals"));
    }
    if price.fract().is_zero() {
        return Ok(price);
    } // Integer prices have no significant-figure cap.
    let normalized = price.normalize();
    let mantissa_digits = normalized.mantissa().to_string().len() as u32;
    let remove = mantissa_digits.saturating_sub(5);
    let decimals = (6 - sz_decimals).min(normalized.scale().saturating_sub(remove));
    let strategy = if side == Side::Buy {
        RoundingStrategy::ToNegativeInfinity
    } else {
        RoundingStrategy::ToPositiveInfinity
    };
    let rounded = price.round_dp_with_strategy(decimals, strategy);
    if rounded <= Decimal::ZERO {
        return Err(error("price rounds to zero at venue tick"));
    }
    Ok(rounded.normalize())
}

/// 这个价位上永续一个 tick 有几位小数（和 [`tick_price`] 同一规则）：最多 5 位有效数字、最多
/// `6 − szDecimals` 位小数。
fn tick_decimals(price: Decimal, sz_decimals: u32) -> u32 {
    let most = 6u32.saturating_sub(sz_decimals);
    let price = price.abs();
    let significant = if price >= Decimal::ONE {
        let integer_digits = price.trunc().normalize().mantissa().to_string().len() as u32;
        5u32.saturating_sub(integer_digits)
    } else {
        // 0.0123 → 小数点后有 1 个前导 0：5 位有效数字要 6 位小数。
        let mut leading = 0u32;
        let mut scaled = price;
        while scaled > Decimal::ZERO && scaled < Decimal::new(1, 1) && leading < 20 {
            scaled *= Decimal::TEN;
            leading += 1;
        }
        5 + leading
    };
    most.min(significant)
}

fn same_intent(a: &NewOrder, b: &NewOrder) -> bool {
    if a.margin_mode != b.margin_mode {
        return false;
    }
    a.client_order_id == b.client_order_id
        && a.venue == b.venue
        && a.symbol == b.symbol
        && a.side == b.side
        && a.quantity == b.quantity
        && a.notional_usdt == b.notional_usdt
        && a.limit_price == b.limit_price
        && a.reduce_only == b.reduce_only
        && a.leverage == b.leverage
}

/// `activeAssetData` 的应答是不是这个币、这个保证金模式、这个杠杆。
fn leverage_matches(data: &Value, coin: &str, leverage: u32, mode: crate::MarginMode) -> bool {
    data.get("coin").and_then(Value::as_str) == Some(coin)
        && data.pointer("/leverage/type").and_then(Value::as_str) == Some(mode.as_str())
        && data.pointer("/leverage/value").and_then(Value::as_u64) == Some(u64::from(leverage))
}

fn map_status(status: &str) -> ArbResult<OrderStatus> {
    match status {
        "open" | "triggered" => Ok(OrderStatus::Open),
        "filled" => Ok(OrderStatus::Filled),
        "canceled"
        | "marginCanceled"
        | "vaultWithdrawalCanceled"
        | "openInterestCapCanceled"
        | "selfTradeCanceled"
        | "reduceOnlyCanceled"
        | "siblingFilledCanceled"
        | "delistedCanceled"
        | "liquidatedCanceled"
        | "scheduledCancel" => Ok(OrderStatus::Cancelled),
        "rejected"
        | "tickRejected"
        | "minTradeNtlRejected"
        | "perpMarginRejected"
        | "reduceOnlyRejected"
        | "badAloPxRejected"
        | "iocCancelRejected"
        | "badTriggerPxRejected"
        | "marketOrderNoLiquidityRejected"
        | "positionIncreaseAtOpenInterestCapRejected"
        | "positionFlipAtOpenInterestCapRejected"
        | "tooAggressiveAtOpenInterestCapRejected"
        | "openInterestIncreaseRejected"
        | "insufficientSpotBalanceRejected"
        | "oracleRejected"
        | "perpMaxPositionRejected" => Ok(OrderStatus::Rejected),
        _ => Err(error(
            "unknown venue order status; refusing to infer terminality",
        )),
    }
}

fn verified_status(
    status: OrderStatus,
    original: Decimal,
    remaining: Decimal,
    filled: Decimal,
) -> ArbResult<OrderStatus> {
    if remaining < Decimal::ZERO || remaining > original || filled != original - remaining {
        return Err(error(
            "fill history incomplete or order changed during query; cannot verify quantity/fees",
        ));
    }
    // A terminal IOC match is not necessarily a full fill of the effective request.
    if status == OrderStatus::Filled && filled < original {
        return Ok(OrderStatus::Cancelled);
    }
    if status == OrderStatus::Rejected && filled > Decimal::ZERO {
        return Ok(OrderStatus::Cancelled);
    }
    Ok(status)
}

fn aggregate_fills(
    fills: &[RemoteFill],
    order: &RemoteOrder,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let (mut quantity, mut notional, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    for fill in fills {
        if fill.oid != order.oid
            || fill.coin != order.coin
            || fill.side != order.side
            || fill.fee_token != "USDC"
        {
            return Err(error("fill identity or fee currency mismatch"));
        }
        let size = positive(&fill.sz)?;
        quantity = add(quantity, size)?;
        notional = add(notional, multiply(size, positive(&fill.px)?)?)?;
        fee = add(fee, decimal(&fill.fee)?)?; // Negative maker rebates are real, not clamped.
    }
    Ok((quantity, notional, fee))
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| error("missing venue order ID"))?,
        status: state.status,
    })
}

fn now_ms() -> ArbResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| error("system clock before epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| error("timestamp overflow"))
}

fn next_nonce() -> ArbResult<u64> {
    let now = now_ms()?;
    // compare_exchange 循环：fetch_update 在新版本里已弃用，try_update 又高于最低支持版本（1.88）。
    let mut previous = NONCE.load(Ordering::SeqCst);
    loop {
        let next = previous
            .checked_add(1)
            .map(|next| next.max(now))
            .ok_or_else(|| error("nonce overflow"))?;
        match NONCE.compare_exchange(previous, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return Ok(next),
            Err(actual) => previous = actual,
        }
    }
}

fn client_cloid(id: &ClientOrderId) -> String {
    hex(&keccak(id.0.as_bytes())[..16])
}
fn keccak(bytes: &[u8]) -> [u8; 32] {
    Keccak256::digest(bytes).into()
}

fn action_hash<T: Serialize>(action: &T, nonce: u64) -> ArbResult<[u8; 32]> {
    let mut bytes = rmp_serde::to_vec_named(action)
        .map_err(|_| error("action MessagePack serialization failed"))?;
    bytes.extend_from_slice(&nonce.to_be_bytes());
    bytes.push(0); // No vault address; expiresAfter omitted in both payload and hash.
    Ok(keccak(&bytes))
}

fn signing_digest(connection: [u8; 32]) -> [u8; 32] {
    let mut domain = [0u8; 160];
    domain[..32].copy_from_slice(&keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain[32..64].copy_from_slice(&keccak(b"Exchange"));
    domain[64..96].copy_from_slice(&keccak(b"1"));
    domain[120..128].copy_from_slice(&1337u64.to_be_bytes());
    let mut agent = [0u8; 96];
    agent[..32].copy_from_slice(&keccak(b"Agent(string source,bytes32 connectionId)"));
    agent[32..64].copy_from_slice(&keccak(b"a")); // Production mainnet only.
    agent[64..].copy_from_slice(&connection);
    let mut payload = [0u8; 66];
    payload[..2].copy_from_slice(&[0x19, 0x01]);
    payload[2..34].copy_from_slice(&keccak(&domain));
    payload[34..].copy_from_slice(&keccak(&agent));
    keccak(&payload)
}

fn sign(key: &SigningKey, connection: [u8; 32]) -> ArbResult<WireSignature> {
    let (signature, recovery) = key
        .sign_prehash_recoverable(&signing_digest(connection))
        .map_err(|_| error("secp256k1 signature failed"))?;
    if recovery.to_byte() > 1 {
        return Err(error("unsupported Ethereum recovery ID"));
    }
    let bytes = signature.to_bytes();
    Ok(WireSignature {
        r: hex(&bytes[..32]),
        s: hex(&bytes[32..]),
        v: recovery.to_byte() + 27,
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for &byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

fn decode_hex<const N: usize>(value: &str) -> ArbResult<[u8; N]> {
    let raw = value.strip_prefix("0x").unwrap_or(value).as_bytes();
    if raw.len() != N * 2 {
        return Err(error("invalid hexadecimal length"));
    }
    fn digit(byte: u8) -> ArbResult<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(error("invalid hexadecimal encoding")),
        }
    }
    let mut out = [0u8; N];
    for (index, pair) in raw.as_chunks::<2>().0.iter().enumerate() {
        out[index] = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order() -> NewOrder {
        NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("test-position-buy-0".into()),
            venue: Venue::Hyperliquid,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(123.45),
            quantity: None,
            limit_price: Some(dec!(100)),
            reduce_only: false,
            leverage: Some(dec!(3)),
        }
    }

    #[test]
    fn hyperliquid_positions_report_liquidation_without_guessing_unknown_margin() {
        let with = json!({"coin":"BTC","szi":"0.1","entryPx":"100000","marginUsed":"2500.5","liquidationPx":"91234.5"});
        let state = leg_state_from_position(&with);
        assert_eq!(state.margin_usdt, None);
        assert_eq!(state.margin_mode, None);
        assert_eq!(
            state.liquidation_price,
            Some(Decimal::from_str("91234.5").unwrap())
        );
        // liquidationPx 为 null（全仓且离强平很远）：没有，不是 0。
        let none = json!({"coin":"BTC","marginUsed":"10","liquidationPx":null});
        assert_eq!(leg_state_from_position(&none).liquidation_price, None);
    }

    /// 逐仓持仓的 `marginUsed` 含浮动盈亏（`= rawUsd + szi·entryPx + unrealizedPnl`，会随价格涨落），
    /// 不是强平公式里的「逐仓保证金」；公式要的是存进去的那份：`rawUsd + szi·entryPx`。
    /// 自动加保证金按它算要补多少，拿含盈亏的数会在亏损时低估保证金、补多。
    #[test]
    fn an_isolated_leg_reports_the_deposited_margin_not_equity() {
        // 多头：保证金 1000，入场名义 10000 → rawUsd = −9000；价格跌了，浮亏 −400 → marginUsed 600。
        let long = json!({"coin":"BTC","szi":"0.1","entryPx":"100000","marginUsed":"600",
            "unrealizedPnl":"-400","liquidationPx":"91000",
            "leverage":{"type":"isolated","value":10,"rawUsd":"-9000"}});
        assert_eq!(
            leg_state_from_position(&long).margin_usdt,
            Some(dec!(1000)),
            "多头：−9000 + 0.1 × 100000"
        );
        // 空头：保证金 1000，入场名义 10000 → rawUsd = +11000；浮亏 −400 → marginUsed 600。
        let short = json!({"coin":"BTC","szi":"-0.1","entryPx":"100000","marginUsed":"600",
            "unrealizedPnl":"-400","liquidationPx":"109000",
            "leverage":{"type":"isolated","value":10,"rawUsd":"11000"}});
        assert_eq!(
            leg_state_from_position(&short).margin_usdt,
            Some(dec!(1000))
        );
        // 全仓不能把占用权益当作独立逐仓保证金；逐仓缺账本也必须报未知。
        let cross = json!({"coin":"BTC","szi":"0.1","entryPx":"100000","marginUsed":"600",
            "leverage":{"type":"cross","value":10}});
        assert_eq!(leg_state_from_position(&cross).margin_usdt, None);
        assert_eq!(
            leg_state_from_position(&cross).margin_mode,
            Some(crate::MarginMode::Cross)
        );
        for field in ["rawUsd", "szi", "entryPx"] {
            let mut incomplete = long.clone();
            if field == "rawUsd" {
                incomplete["leverage"]
                    .as_object_mut()
                    .unwrap()
                    .remove(field);
            } else {
                incomplete.as_object_mut().unwrap().remove(field);
            }
            assert_eq!(
                leg_state_from_position(&incomplete).margin_usdt,
                None,
                "{field}"
            );
        }
    }

    #[test]
    fn market_mode_restrictions_are_enforced_without_a_write() {
        let mode = crate::MarginMode::Cross;
        let asset: Asset =
            serde_json::from_value(json!({"name":"ETH","szDecimals":4,"maxLeverage":50})).unwrap();
        assert!(market_supports_mode(HyperliquidDex::Main, &asset, mode));
        assert!(!market_supports_mode(HyperliquidDex::Io, &asset, mode));
        assert!(!market_supports_mode(HyperliquidDex::Xyz, &asset, mode));
        for value in [
            json!({"onlyIsolated":true}),
            json!({"marginMode":"noCross"}),
            json!({"marginMode":"strictIsolated"}),
            json!({"marginMode":"unknown"}),
        ] {
            let mut raw = json!({"name":"ETH","szDecimals":4,"maxLeverage":50});
            raw.as_object_mut()
                .unwrap()
                .extend(value.as_object().unwrap().clone());
            let asset: Asset = serde_json::from_value(raw).unwrap();
            assert!(!market_supports_mode(HyperliquidDex::Main, &asset, mode));
            assert!(market_supports_mode(
                HyperliquidDex::Main,
                &asset,
                crate::MarginMode::Isolated
            ));
        }
    }

    /// 报给执行器的步长和下单取整是同一份规格：步长整数倍的数量原样下出去（两腿同时下靠这个对齐数量）。
    #[test]
    fn a_quantity_on_the_reported_step_goes_out_unchanged() {
        let step = lot_step(2).unwrap();
        assert_eq!(step, dec!(0.01));
        let mut order = order();
        order.quantity = Some(step * Decimal::from(14880));
        order.notional_usdt = dec!(148.8) * dec!(100);
        assert_eq!(order_quantity(&order, dec!(100), 2).unwrap(), dec!(148.8));
        assert!(lot_step(7).is_err());
    }

    #[test]
    fn quantities_round_down_but_exits_must_be_exact() {
        let mut order = order();
        assert_eq!(order_quantity(&order, dec!(100), 3).unwrap(), dec!(1.234));
        order.quantity = Some(dec!(0.012349));
        assert_eq!(order_quantity(&order, dec!(100), 5).unwrap(), dec!(0.01234));
        order.reduce_only = true;
        assert!(order_quantity(&order, dec!(100), 5).is_err());
        order.quantity = Some(dec!(0.01234));
        assert_eq!(order_quantity(&order, dec!(100), 5).unwrap(), dec!(0.01234));
        order.quantity = Some(dec!(0.000001));
        assert!(order_quantity(&order, dec!(100), 5).is_err());
        order.quantity = None;
        assert!(order_quantity(&order, dec!(100), 5).is_err());
    }

    #[test]
    fn a_long_breakeven_limit_rounds_to_five_significant_figures_without_getting_worse() {
        // 第二腿的保本价（`Plan::hedged_after`）有十几位小数。
        assert_eq!(
            tick_price(dec!(5.364712345678), 1, Side::Sell).unwrap(),
            dec!(5.3648)
        );
        assert_eq!(
            tick_price(dec!(5.364712345678), 1, Side::Buy).unwrap(),
            dec!(5.3647)
        );
        assert_eq!(
            tick_price(dec!(1591.611111111111), 2, Side::Buy).unwrap(),
            dec!(1591.6)
        );
    }

    #[test]
    fn tick_rounding_never_worsens_limit_and_honors_size_decimals() {
        assert_eq!(
            tick_price(dec!(1234.56), 0, Side::Buy).unwrap(),
            dec!(1234.5)
        );
        assert_eq!(
            tick_price(dec!(1234.56), 0, Side::Sell).unwrap(),
            dec!(1234.6)
        );
        assert_eq!(
            tick_price(dec!(0.012345), 1, Side::Buy).unwrap(),
            dec!(0.01234)
        );
        assert_eq!(tick_price(dec!(1.23456), 5, Side::Sell).unwrap(), dec!(1.3));
        assert_eq!(
            tick_price(dec!(123456), 5, Side::Buy).unwrap(),
            dec!(123456)
        );
        assert!(tick_price(dec!(0.0000001), 0, Side::Buy).is_err());
    }

    #[test]
    fn unknown_status_does_not_become_success() {
        assert_eq!(map_status("filled").unwrap(), OrderStatus::Filled);
        assert_eq!(
            map_status("reduceOnlyCanceled").unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status("iocCancelRejected").unwrap(),
            OrderStatus::Rejected
        );
        assert!(map_status("futureStatus").is_err());
    }

    #[test]
    fn partial_ioc_and_missing_fills_never_report_full_execution() {
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(1), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(0), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert!(verified_status(OrderStatus::Filled, dec!(2), dec!(0), dec!(1)).is_err());
        assert!(verified_status(OrderStatus::Cancelled, dec!(2), dec!(1), dec!(0)).is_err());
    }

    #[test]
    fn fill_accounting_uses_actual_prices_fees_and_currency() {
        let order = RemoteOrder {
            coin: "BTC".into(),
            side: "B".into(),
            limit_px: "110".into(),
            sz: "0".into(),
            orig_sz: "3".into(),
            oid: 7,
            timestamp: 0,
            reduce_only: false,
            cloid: None,
        };
        let fill = |tid, size: &str, price: &str, fee: &str| RemoteFill {
            oid: 7,
            tid,
            time: 0,
            coin: "BTC".into(),
            side: "B".into(),
            sz: size.into(),
            px: price.into(),
            fee: fee.into(),
            fee_token: "USDC".into(),
        };
        let mut fills = vec![fill(1, "1", "100", "0.04"), fill(2, "2", "110", "-0.01")];
        let (quantity, notional, fees) = aggregate_fills(&fills, &order).unwrap();
        assert_eq!((quantity, notional, fees), (dec!(3), dec!(320), dec!(0.03)));
        assert_eq!(divide(notional, quantity).unwrap(), dec!(320) / dec!(3));
        fills[1].fee_token = "HYPE".into();
        assert!(aggregate_fills(&fills, &order).is_err());
    }

    #[test]
    fn signing_matches_official_rust_sdk_published_vector() {
        // Public upstream SDK fixture, never a credential used by this application.
        let key =
            decode_hex::<32>("e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e")
                .unwrap();
        let key = SigningKey::from_slice(&key).unwrap();
        let connection =
            decode_hex::<32>("de6c4037798a4434ca03cd05f00e3b803126221375cd1e7eaaaf041768be06eb")
                .unwrap();
        let signature = sign(&key, connection).unwrap();
        assert_eq!(
            signature.r,
            "0xfa8a41f6a3fa728206df80801a83bcbfbab08649cd34d9c0bfba7c7b2f99340f"
        );
        assert_eq!(
            signature.s,
            "0x53a00226604567b98a1492803190d65a201d6805e5831b7044f17fd530aec784"
        );
        assert_eq!(signature.v, 28);
    }

    #[test]
    fn reservation_survives_restart_and_prevents_ambiguous_resubmission() {
        let path = std::env::temp_dir().join(format!(
            "arb-hyperliquid-journal-{}-{}.jsonl",
            std::process::id(),
            next_nonce().unwrap()
        ));
        let order = order();
        let cloid = client_cloid(&order.client_order_id);
        let mut journal = Journal::open(&path, "account", "signer", Venue::Hyperliquid).unwrap();
        let reservation = Reservation {
            account: "account".into(),
            signer: "signer".into(),
            cloid: cloid.clone(),
            order: order.clone(),
            effective_quantity: dec!(1.234),
        };
        journal.reserve(reservation.clone()).unwrap();
        assert!(Journal::open(&path, "account", "signer", Venue::Hyperliquid).is_err());
        assert!(journal.reserve(reservation.clone()).is_err());
        drop(journal);
        let mut reopened = Journal::open(&path, "account", "signer", Venue::Hyperliquid).unwrap();
        let restored = reopened.by_cloid.get(&cloid).unwrap();
        assert!(same_intent(&restored.order, &order));
        assert_eq!(restored.effective_quantity, dec!(1.234));
        assert!(reopened.reserve(reservation).is_err());
        drop(reopened);
        assert!(Journal::open(&path, "other-account", "signer", Venue::Hyperliquid).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn action_hash_matches_official_python_algorithm_fixture() {
        let action = OrderAction {
            kind: "order",
            grouping: "na",
            orders: [WireOrder {
                a: 0,
                b: true,
                p: "1234.5",
                s: "0.001",
                r: false,
                t: LimitType {
                    limit: TimeInForce { tif: "Ioc" },
                },
                c: "0x000102030405060708090a0b0c0d0e0f",
            }],
        };
        assert_eq!(
            hex(&action_hash(&action, 1_750_000_000_000).unwrap()),
            "0x5c7089a522613674f7c2a73bab7db1e071ec64495fd976a39880bdd635049ab4"
        );
    }

    #[test]
    fn asset_ids_follow_the_official_formula() {
        assert_eq!(HyperliquidDex::Main.asset_id(1, 7).unwrap(), 7);
        assert_eq!(HyperliquidDex::Xyz.asset_id(1, 0).unwrap(), 110_000);
        assert_eq!(HyperliquidDex::Xyz.asset_id(1, 3).unwrap(), 110_003);
        assert_eq!(HyperliquidDex::Io.asset_id(2, 5).unwrap(), 120_005);
        // Overflow must fail closed rather than wrap into another dex's range.
        assert!(HyperliquidDex::Xyz.asset_id(u32::MAX, u32::MAX).is_err());
    }

    #[test]
    fn funding_rows_keep_only_this_dex_and_symbol() {
        // 2026-09-30 实测 `userFunding` 的形状：所有 dex 混在一起，HIP-3 的币名带前缀。
        let page: Vec<UserFunding> = serde_json::from_value(json!([
            {"time": 1790744400000_i64, "hash": "0x0", "delta": {"type": "funding", "coin": "io:ANTH", "usdc": "-0.1234", "szi": "-2.0", "fundingRate": "0.0000125", "nSamples": null}},
            {"time": 1790748000000_i64, "hash": "0x0", "delta": {"type": "funding", "coin": "io:ANTH", "usdc": "0.05", "szi": "-2.0", "fundingRate": "-0.00001", "nSamples": null}},
            {"time": 1790748000000_i64, "hash": "0x0", "delta": {"type": "funding", "coin": "BTC", "usdc": "9.99", "szi": "0.1", "fundingRate": "0.0001", "nSamples": null}}
        ]))
        .unwrap();
        let anth = symbol_of(HyperliquidDex::Io, "io:ANTH").unwrap();
        let rows = funding_rows(HyperliquidDex::Io, &anth, page).unwrap();
        assert_eq!(rows.len(), 2, "主 dex 的 BTC 不是这条腿的");
        assert_eq!(
            rows.iter().map(|(_, usdc)| *usdc).sum::<Decimal>(),
            Decimal::new(-734, 4)
        );
    }

    /// 同一个地址上的几条腿（这里是 xyz 的 TSLA 和 io 的 ANTH）共用一份 `userFunding`：只拉一次，各自按
    /// dex、合约、起点过滤；要的起点比手上那份更早才重拉。
    #[tokio::test]
    async fn legs_on_one_address_share_one_user_funding_fetch() {
        let mock =
            Mock::start(
                |path, body, _| match (path, body.get("type").and_then(Value::as_str)) {
                    ("/info", Some("userFunding")) => Reply::Json(
                        200,
                        json!([
                            {"time": 1_000_i64, "delta": {"coin": "xyz:TSLA", "usdc": "-1"}},
                            {"time": 2_000_i64, "delta": {"coin": "io:ANTH", "usdc": "0.5"}},
                            {"time": 3_000_i64, "delta": {"coin": "xyz:TSLA", "usdc": "-2"}},
                            {"time": 4_000_i64, "delta": {"coin": "io:ANTH", "usdc": "0.25"}},
                            {"time": 4_000_i64, "delta": {"coin": "BTC", "usdc": "9"}}
                        ]),
                    ),
                    _ => Reply::Json(404, json!({})),
                },
            )
            .await;
        let mut xyz = test_broker(HyperliquidDex::Xyz, 1, false, Duration::from_secs(1));
        xyz.info_url = mock.info();
        let mut io = test_broker(HyperliquidDex::Io, 2, false, Duration::from_secs(1));
        io.info_url = mock.info();
        let at = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        let anth_symbol = symbol_of(HyperliquidDex::Io, "io:ANTH").unwrap();
        // 整点后 5 分钟：最近一次结算早就查得到了。
        let now = chrono::DateTime::from_timestamp(1_790_000_000 / 3600 * 3600 + 300, 0).unwrap();

        let tsla = xyz
            .funding_since_at(&Symbol::perp("TSLA", "USDT"), at(1_000), now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((tsla.usdt, tsla.payments), (dec!(-3), 2));
        assert_eq!(mock.count("info:userFunding"), 1);

        // 另一个 dex、起点更晚：用手上那份，不再请求。
        let anth = io
            .funding_since_at(&anth_symbol, at(3_000), now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (anth.usdt, anth.payments),
            (dec!(0.25), 1),
            "起点之前的不算"
        );
        assert_eq!(mock.count("info:userFunding"), 1);

        // 起点更早：手上那份盖不住，重拉。
        let anth = io
            .funding_since_at(&anth_symbol, at(500), now)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((anth.usdt, anth.payments), (dec!(0.75), 2));
        assert_eq!(mock.count("info:userFunding"), 2);
    }

    /// 共享的那份流水：10 秒内、而且拉的时候最近一次整点结算已经查得到（整点后 60 秒）才能用。
    /// 止盈复核强制重查资金费，不能拿结算之前拉的那份顶替。
    #[test]
    fn shared_funding_is_not_used_across_an_hourly_settlement() {
        let hour = 1_790_000_000 / 3600 * 3600;
        let at = |secs: i64| chrono::DateTime::from_timestamp(secs, 0).unwrap();
        let fetched_at = |secs: i64| SharedFunding {
            fetched: at(secs),
            start: 0,
            rows: Vec::new(),
        };
        // 整点后 5 分钟拉的：5 秒后能用，10 秒后不能用。
        assert!(fetched_at(hour + 300).usable_at(at(hour + 305)));
        assert!(!fetched_at(hour + 300).usable_at(at(hour + 310)));
        // 整点前 3 秒拉的：过了整点就不能用（这一笔还没在里面）。
        assert!(fetched_at(hour - 3).usable_at(at(hour - 1)));
        assert!(!fetched_at(hour - 3).usable_at(at(hour + 2)));
        // 整点后 30 秒拉的（结算可能还没查得到）：不能用；整点后 60 秒起拉的才能用。
        assert!(!fetched_at(hour + 30).usable_at(at(hour + 35)));
        assert!(fetched_at(hour + 60).usable_at(at(hour + 65)));
    }

    #[test]
    fn hip3_symbols_round_trip_through_verified_aliases() {
        let xyz = HyperliquidDex::Xyz;
        assert_eq!(
            symbol_of(xyz, "xyz:TSLA").unwrap(),
            Symbol::perp("TSLA", "USDT")
        );
        assert_eq!(
            symbol_of(xyz, "xyz:GOLD").unwrap(),
            Symbol::perp("XAU", "USDT")
        );
        assert_eq!(
            symbol_of(xyz, "xyz:SILVER").unwrap(),
            Symbol::perp("XAG", "USDT")
        );
        let io = HyperliquidDex::Io;
        assert_eq!(
            symbol_of(io, "io:ANTH").unwrap(),
            Symbol::perp("ANTHROPIC", "USDT")
        );
        assert_eq!(
            symbol_of(io, "io:OAI").unwrap(),
            Symbol::perp("OPENAI", "USDT")
        );
        assert_eq!(
            symbol_of(io, "io:TSLA").unwrap(),
            Symbol::perp("TSLA", "USDT")
        );
        // Main keeps plain names and the HYPE/PURR USDC quote.
        assert_eq!(
            symbol_of(HyperliquidDex::Main, "HYPE").unwrap(),
            Symbol::perp("HYPE", "USDC")
        );
        assert_eq!(
            symbol_of(HyperliquidDex::Main, "BTC").unwrap(),
            Symbol::perp("BTC", "USDT")
        );
    }

    #[test]
    fn foreign_spot_and_unprefixed_coins_fail_closed() {
        // The main instance never adopts a HIP-3 or spot name.
        assert!(symbol_of(HyperliquidDex::Main, "xyz:TSLA").is_err());
        assert!(symbol_of(HyperliquidDex::Main, "@107").is_err());
        // Each HIP-3 instance accepts only its own prefix.
        assert!(symbol_of(HyperliquidDex::Xyz, "io:OAI").is_err());
        assert!(symbol_of(HyperliquidDex::Xyz, "TSLA").is_err());
        assert!(symbol_of(HyperliquidDex::Xyz, "xyz:").is_err());
        assert!(symbol_of(HyperliquidDex::Io, "xyz:TSLA").is_err());
        assert!(symbol_of(HyperliquidDex::Io, "@1").is_err());
    }

    #[test]
    fn hip3_fee_scale_now_comes_from_each_listed_asset() {
        // 2026-09-29 `meta(dex=io)` 的两个合约（原样截取）：倍数在逐合约字段上，dex 一级已不给。
        let meta: Meta = serde_json::from_str(
            r#"{"universe":[{"name":"io:OAI","szDecimals":3,"maxLeverage":3,"deployerFeeScale":"1.0","growthMode":"enabled","marginTableId":3,"lastFeeScaleChangeTime":null},{"name":"io:ANTH","szDecimals":2,"maxLeverage":3,"deployerFeeScale":"1.0","growthMode":"enabled"}]}"#,
        )
        .unwrap();
        assert_eq!(
            max_fee_scale(None, &meta.universe).unwrap(),
            Some(dec!(1.0))
        );
        // 旧接口在 dex 一级给了更大的值：取最大，只高估不低估。
        assert_eq!(
            max_fee_scale(Some("1.5"), &meta.universe).unwrap(),
            Some(dec!(1.5))
        );
        // 下架合约不算。
        let mut delisted = meta.universe.clone();
        delisted[0].deployer_fee_scale = Some("3".into());
        delisted[0].is_delisted = true;
        assert_eq!(max_fee_scale(None, &delisted).unwrap(), Some(dec!(1.0)));
        // 哪里都没有：拒绝下结论。
        let mut bare = meta.universe;
        for asset in &mut bare {
            asset.deployer_fee_scale = None;
        }
        assert_eq!(max_fee_scale(None, &bare).unwrap(), None);
    }

    #[test]
    fn hip3_fee_scale_follows_the_deployer_rule() {
        assert_eq!(hip3_fee_scale(dec!(0)).unwrap(), dec!(1));
        assert_eq!(hip3_fee_scale(dec!(0.5)).unwrap(), dec!(1.5));
        assert_eq!(hip3_fee_scale(dec!(1)).unwrap(), dec!(2));
        assert_eq!(hip3_fee_scale(dec!(3)).unwrap(), dec!(6));
        assert!(hip3_fee_scale(dec!(-0.1)).is_err());
    }

    #[test]
    fn journal_identity_pins_the_dex() {
        let path = std::env::temp_dir().join(format!(
            "arb-hyperliquid-dex-journal-{}-{}.jsonl",
            std::process::id(),
            next_nonce().unwrap()
        ));
        let mut order = order();
        order.venue = Venue::HyperliquidXyz;
        let mut journal = Journal::open(&path, "account", "signer", Venue::HyperliquidXyz).unwrap();
        journal
            .reserve(Reservation {
                account: "account".into(),
                signer: "signer".into(),
                cloid: client_cloid(&order.client_order_id),
                order,
                effective_quantity: dec!(1.234),
            })
            .unwrap();
        drop(journal);
        // The same file must be rejected by a differently-tagged instance.
        assert!(Journal::open(&path, "account", "signer", Venue::Hyperliquid).is_err());
        std::fs::remove_file(path).unwrap();
    }

    // ---- Broker::add_margin ----------------------------------------------------------

    /// 公开的上游 SDK 测试私钥（与 `signing_matches_official_rust_sdk_published_vector` 同一把），
    /// 不是本应用用过的凭据。
    const TEST_KEY: &str = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

    fn eth() -> Symbol {
        Symbol::perp("ETH", "USDT")
    }

    #[test]
    fn margin_ntli_is_exact_micro_usdc() {
        assert_eq!(margin_ntli(dec!(12.34)).unwrap(), 12_340_000);
        assert_eq!(margin_ntli(dec!(0.01)).unwrap(), 10_000);
        assert_eq!(margin_ntli(dec!(250)).unwrap(), 250_000_000);
        assert_eq!(margin_ntli(dec!(250.000000)).unwrap(), 250_000_000);
        assert_eq!(margin_ntli(dec!(0.000001)).unwrap(), 1);
        // 不凑整、不放大：零、负数、超过 6 位小数、溢出都拒绝。
        assert!(margin_ntli(dec!(0)).is_err());
        assert!(margin_ntli(dec!(-1)).is_err());
        assert!(margin_ntli(dec!(0.0000001)).is_err());
        assert!(margin_ntli(dec!(1.2345678)).is_err());
        assert!(margin_ntli(Decimal::MAX).is_err());
    }

    #[test]
    fn update_isolated_margin_matches_official_python_sdk_vectors() {
        // 向量由官方 Python SDK（hyperliquid/utils/signing.py 的 `action_hash` /
        // `sign_l1_action`，action = {"type":"updateIsolatedMargin","asset":..,"isBuy":True,
        // "ntli":float_to_usd_int(amount)}，mainnet，vault/expiresAfter 为空）在 2026-10-03 生成。
        // MessagePack 布局：0x84 = 4 项 map，键依次 type / asset / isBuy / ntli；
        // 正整数用无符号编码（ce = uint32，cd = uint16，00 = fixint 0）。
        let key = SigningKey::from_slice(&decode_hex::<32>(TEST_KEY).unwrap()).unwrap();
        let vectors = [
            (
                HyperliquidDex::Main.asset_id(0, 0).unwrap(),
                dec!(12.34),
                1_750_000_000_000u64,
                "0x84a474797065b475706461746549736f6c617465644d617267696ea5617373657400a56973427579c3a46e746c69ce00bc4b20",
                "0xeba3f56afa7f7000b6cd080ef3324853f4a5339ae591d4eb0450d3b058048c31",
                "0xc4a7bc5c20239aeaa21d395959d911eac2a758ef080c849199b329a86f6d446b",
                "0x0d530dfb2f78f37e0e69ae384e2110a7cb0004a6619d91ad444b61719d1172a3",
            ),
            (
                // xyz 是 perpDexs 里第 1 项，meta 里第 3 个币 → 110003。
                HyperliquidDex::Xyz.asset_id(1, 3).unwrap(),
                dec!(250),
                1_750_000_000_001,
                "0x84a474797065b475706461746549736f6c617465644d617267696ea56173736574ce0001adb3a56973427579c3a46e746c69ce0ee6b280",
                "0x99b264c75da221d2ab7fa56164c7129a8d97c19e41551d5f15babd9da50b6aa0",
                "0x7c1571c9bcae9c1cff512b72d84f82511e4c467beb229ad0093f827524c4ca5e",
                "0x696a9c4fc6c3448f55ceeb6c5662c24cb4246662e0c7c105267746889dc8df60",
            ),
            (
                // io 是 perpDexs 里第 10 项，meta 里第 5 个币 → 200005。
                HyperliquidDex::Io.asset_id(10, 5).unwrap(),
                dec!(0.01),
                1_750_000_000_002,
                "0x84a474797065b475706461746549736f6c617465644d617267696ea56173736574ce00030d45a56973427579c3a46e746c69cd2710",
                "0x0fdb85275c8508d170e9c90ae4942d08f3b3bbf4c4b0df438282b5547819b68d",
                "0x312f0bd60c94e57fd38e9dcc078c9dc0f4964ed8c948bdeeca9df6e9fdefdfea",
                "0x3cf26997f44de26628a8621d4b200cf0d8dab9ea7119a54c72f645119c6f6857",
            ),
        ];
        for (asset, amount, nonce, packed, hash, r, s) in vectors {
            let action = UpdateIsolatedMarginAction {
                kind: "updateIsolatedMargin",
                asset,
                is_buy: true,
                ntli: margin_ntli(amount).unwrap(),
            };
            assert_eq!(hex(&rmp_serde::to_vec_named(&action).unwrap()), packed);
            let digest = action_hash(&action, nonce).unwrap();
            assert_eq!(hex(&digest), hash);
            let signature = sign(&key, digest).unwrap();
            assert_eq!(signature.r, r);
            assert_eq!(signature.s, s);
            assert_eq!(signature.v, 27);
        }
    }

    /// 不联网的券商：只能指向本地假服务器，`/info`、`/exchange` 的 URL 由测试传入。
    fn test_broker(
        dex: HyperliquidDex,
        perp_dex_index: u32,
        trading_enabled: bool,
        timeout: Duration,
    ) -> HyperliquidBroker {
        let signer = SigningKey::from_slice(&decode_hex::<32>(TEST_KEY).unwrap()).unwrap();
        let public = signer.verifying_key().to_encoded_point(false);
        let signer_address = hex(&keccak(&public.as_bytes()[1..])[12..]);
        let account = hex(&[0x11; 20]);
        let path = std::env::temp_dir().join(format!(
            "arb-hyperliquid-margin-journal-{}-{}.jsonl",
            std::process::id(),
            next_nonce().unwrap()
        ));
        let journal = Journal::open(&path, &account, &signer_address, dex.venue()).unwrap();
        std::fs::remove_file(&path).unwrap();
        HyperliquidBroker {
            client: Client::builder()
                .timeout(timeout)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            signer,
            account,
            signer_address,
            dex,
            perp_dex_index,
            options: HyperliquidOptions {
                trading_enabled,
                market_slippage: None,
            },
            taker_fee: dec!(0.00045),
            journal: SyncMutex::new(journal),
            submit: Mutex::new(()),
            info_url: INFO_URL.to_string(),
            exchange_url: EXCHANGE_URL.to_string(),
            meta: Cached::new(),
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
            account_mode: Cached::new(),
            terminal: SyncMutex::new(HashMap::new()),
            replied: SyncMutex::new(HashMap::new()),
            fee_rates: SyncMutex::new(HashMap::new()),
        }
    }

    enum Reply {
        Json(u16, Value),
        /// 收到请求后不应答（让客户端超时）。
        Hang,
    }

    /// 最小的 HTTP/1.1 假服务器：记录每个请求（路径 + JSON 体），应答由脚本
    /// `(路径, 请求体, 同一路径同一 type 的第几次)` 决定。
    struct Mock {
        base: String,
        log: std::sync::Arc<SyncMutex<Vec<(String, Value)>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn request_kind(path: &str, body: &Value) -> String {
        match path {
            "/info" => format!("info:{}", body["type"].as_str().unwrap_or("?")),
            "/exchange" => "exchange".to_string(),
            other => format!("other:{other}"),
        }
    }

    /// 假服务器的日志锁：中毒也照常取（测试里只读日志），不对加锁结果做 `unwrap`。
    fn locked<T>(mutex: &SyncMutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<(String, Value)> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let path = head.lines().next()?.split_whitespace().nth(1)?.to_string();
        let length = head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            if key.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })?;
        while buf.len() < header_end + length {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = serde_json::from_slice(&buf[header_end..header_end + length]).ok()?;
        Some((path, body))
    }

    impl Mock {
        async fn start(
            script: impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static,
        ) -> Self {
            use tokio::io::AsyncWriteExt;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let log = std::sync::Arc::new(SyncMutex::new(Vec::<(String, Value)>::new()));
            let script = std::sync::Arc::new(script);
            let shared_log = log.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let script = script.clone();
                    let log = shared_log.clone();
                    tokio::spawn(async move {
                        let Some((path, body)) = read_request(&mut stream).await else {
                            return;
                        };
                        let kind = request_kind(&path, &body);
                        let nth = {
                            let mut log = locked(&log);
                            let nth = log
                                .iter()
                                .filter(|(p, b)| request_kind(p, b) == kind)
                                .count();
                            log.push((path.clone(), body.clone()));
                            nth
                        };
                        match script(&path, &body, nth) {
                            Reply::Json(status, value) => {
                                let text = value.to_string();
                                let reply = format!(
                                    "HTTP/1.1 {status} Reply\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
                                    text.len()
                                );
                                let _ = stream.write_all(reply.as_bytes()).await;
                                let _ = stream.shutdown().await;
                            }
                            Reply::Hang => tokio::time::sleep(Duration::from_secs(30)).await,
                        }
                    });
                }
            });
            Self { base, log, task }
        }

        fn info(&self) -> String {
            format!("{}/info", self.base)
        }

        fn exchange(&self) -> String {
            format!("{}/exchange", self.base)
        }

        fn total(&self) -> usize {
            locked(&self.log).len()
        }

        fn count(&self, kind: &str) -> usize {
            self.sequence().iter().filter(|k| *k == kind).count()
        }

        fn sequence(&self) -> Vec<String> {
            locked(&self.log)
                .iter()
                .map(|(path, body)| request_kind(path, body))
                .collect()
        }

        /// POST 到 `/exchange` 的全部请求体。
        fn writes(&self) -> Vec<Value> {
            locked(&self.log)
                .iter()
                .filter(|(path, _)| path == "/exchange")
                .map(|(_, body)| body.clone())
                .collect()
        }

        fn info_bodies(&self, kind: &str) -> Vec<Value> {
            locked(&self.log)
                .iter()
                .filter(|(path, body)| request_kind(path, body) == format!("info:{kind}"))
                .map(|(_, body)| body.clone())
                .collect()
        }
    }

    const RAW_BEFORE: &str = "11962.556183";
    const RAW_PLUS_25: &str = "11987.556183";

    fn meta_main() -> Value {
        json!({"universe":[
            {"name":"BTC","szDecimals":5,"maxLeverage":40},
            {"name":"ETH","szDecimals":4,"maxLeverage":25},
        ]})
    }

    /// 形状取自 2026-10-03 的公开 `clearinghouseState`（一个主 dex 的逐仓 ETH 空头）。
    fn position_state(
        coin: &str,
        leverage: Value,
        szi: &str,
        unrealized_pnl: &str,
        margin_used: &str,
    ) -> Value {
        json!({
            "marginSummary":{"accountValue":"4442.928428","totalNtlPos":"12830.069284","totalRawUsd":"17272.997712","totalMarginUsed":"4442.928428"},
            "crossMarginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},
            "assetPositions":[{"type":"oneWay","position":{
                "coin":coin,"szi":szi,"entryPx":"2665.29","positionValue":"9021.77011",
                "unrealizedPnl":unrealized_pnl,"marginUsed":margin_used,
                "liquidationPx":"3485.5993871201","maxLeverage":25,"leverage":leverage,
            }}]
        })
    }

    fn isolated_leverage(raw_usd: &str) -> Value {
        json!({"type":"isolated","value":3,"rawUsd":raw_usd})
    }

    fn eth_state(raw_usd: &str) -> Value {
        position_state(
            "ETH",
            isolated_leverage(raw_usd),
            "-3.3647",
            "-53.83763",
            "2940.786073",
        )
    }

    fn ok_reply() -> Reply {
        Reply::Json(200, json!({"status":"ok","response":{"type":"default"}}))
    }

    /// 主 dex 的 ETH：`reads(n)` 给第 n 次（从 0 起）`clearinghouseState` 的应答，
    /// `exchange()` 给 `/exchange` 的应答。
    fn eth_script(
        reads: impl Fn(usize) -> Value + Send + Sync + 'static,
        exchange: impl Fn() -> Reply + Send + Sync + 'static,
    ) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static {
        move |path, body, nth| match (path, body["type"].as_str()) {
            ("/info", Some("meta")) => Reply::Json(200, meta_main()),
            ("/info", Some("clearinghouseState")) => Reply::Json(200, reads(nth)),
            ("/exchange", _) => exchange(),
            _ => Reply::Json(404, json!({})),
        }
    }

    const FAST: Duration = Duration::from_millis(1);

    #[test]
    fn isolated_leg_requires_an_isolated_open_position() {
        let leg = isolated_leg(&eth_state(RAW_BEFORE), HyperliquidDex::Main, &eth()).unwrap();
        assert_eq!(
            leg,
            IsolatedLeg {
                size: dec!(-3.3647),
                entry_price: dec!(2665.29),
                raw_usd: dec!(11962.556183)
            }
        );
        // HIP-3：币名带 dex 前缀，且按本 dex 的符号映射匹配。
        let tsla = position_state(
            "xyz:TSLA",
            json!({"type":"isolated","value":10,"rawUsd":"-87.622794"}),
            "0.263",
            "-0.00263",
            "9.879196",
        );
        let xyz = isolated_leg(&tsla, HyperliquidDex::Xyz, &Symbol::perp("TSLA", "USDT")).unwrap();
        assert_eq!(xyz.size, dec!(0.263));
        assert_eq!(xyz.raw_usd, dec!(-87.622794));

        let message = |state: &Value, dex, symbol: &Symbol| {
            isolated_leg(state, dex, symbol).unwrap_err().to_string()
        };
        // 全仓：绝不当逐仓处理。
        let cross = position_state(
            "ETH",
            json!({"type":"cross","value":3}),
            "-3.3647",
            "0",
            "10",
        );
        assert!(message(&cross, HyperliquidDex::Main, &eth()).contains("not isolated"));
        // 缺模式 / 缺账本：读不准就不动。
        let no_mode = position_state("ETH", json!({"value":3}), "-3.3647", "0", "10");
        assert!(message(&no_mode, HyperliquidDex::Main, &eth()).contains("missing"));
        let no_raw = position_state(
            "ETH",
            json!({"type":"isolated","value":3}),
            "-3.3647",
            "0",
            "10",
        );
        assert!(message(&no_raw, HyperliquidDex::Main, &eth()).contains("rawUsd"));
        // 没有这个币的持仓、持仓量为 0、别的 dex 的币、没有持仓数组。
        let btc = Symbol::perp("BTC", "USDT");
        assert!(message(&eth_state(RAW_BEFORE), HyperliquidDex::Main, &btc).contains("no open"));
        let flat = position_state("ETH", isolated_leverage("0.0"), "0.0", "0", "0");
        assert!(message(&flat, HyperliquidDex::Main, &eth()).contains("no open"));
        assert!(message(&eth_state(RAW_BEFORE), HyperliquidDex::Xyz, &eth()).contains("no open"));
        assert!(
            message(&json!({"assetPositions":[]}), HyperliquidDex::Main, &eth())
                .contains("no open")
        );
        assert!(message(&json!({}), HyperliquidDex::Main, &eth()).contains("missing"));
        // 同一个币出现两行（对冲模式）：歧义，拒绝。
        let mut twice = eth_state(RAW_BEFORE);
        let row = twice["assetPositions"][0].clone();
        twice["assetPositions"].as_array_mut().unwrap().push(row);
        assert!(message(&twice, HyperliquidDex::Main, &eth()).contains("ambiguous"));
    }

    #[test]
    fn margin_readback_judges_only_the_isolated_ledger() {
        let before = IsolatedLeg {
            size: dec!(-3.3647),
            entry_price: dec!(2665.29),
            raw_usd: dec!(11962.556183),
        };
        let after = |size: Decimal, plus: Decimal| IsolatedLeg {
            size,
            entry_price: before.entry_price,
            raw_usd: before.raw_usd + plus,
        };
        let size = before.size;
        let amount = dec!(25);
        // 恰好 / 99% 边界 / 多一点（资金费入账）都算确认。
        for plus in [dec!(25), dec!(24.75), dec!(26.1)] {
            assert_eq!(
                judge_margin_change(&before, &after(size, plus), amount),
                MarginJudgement::Confirmed,
                "+{plus}"
            );
        }
        // 持仓量写法不同但数值相同（-3.3647 vs -3.36470）不算变。
        assert_eq!(
            judge_margin_change(&before, &after(dec!(-3.36470), dec!(25)), amount),
            MarginJudgement::Confirmed
        );
        // 没到 99%：没增加、增加不足、小额被扣（资金费）都是「没增加」。
        for plus in [dec!(24.74), dec!(1), dec!(0), dec!(-0.5), dec!(-12.49)] {
            assert_eq!(
                judge_margin_change(&before, &after(size, plus), amount),
                MarginJudgement::NotIncreased { delta: plus },
                "+{plus}"
            );
        }
        // 减少了至少一半金额：符号可能反了。
        for plus in [dec!(-12.5), dec!(-25)] {
            assert_eq!(
                judge_margin_change(&before, &after(size, plus), amount),
                MarginJudgement::Decreased { delta: plus },
                "{plus}"
            );
        }
        // 持仓量变了：就算账本涨了也不能算到补保证金头上。
        assert_eq!(
            judge_margin_change(&before, &after(dec!(-1.5), dec!(25)), amount),
            MarginJudgement::PositionChanged
        );
    }

    #[test]
    fn exchange_replies_classify_without_claiming_success() {
        let ok = r#"{"status":"ok","response":{"type":"default"}}"#;
        assert_eq!(classify_margin_reply(200, Some(ok)), MarginSend::Accepted);
        // 业务拒绝：200 + status err，文本取自场所。
        let refused = |status: u16, body: &str| match classify_margin_reply(status, Some(body)) {
            MarginSend::Refused(reason) => reason,
            other => panic!("expected Refused, got {other:?}"),
        };
        let unknown = |status: u16, body: Option<&str>| match classify_margin_reply(status, body) {
            MarginSend::Unknown(reason) => reason,
            other => panic!("expected Unknown, got {other:?}"),
        };
        assert!(
            refused(
                200,
                r#"{"status":"err","response":"Insufficient margin to add"}"#
            )
            .contains("Insufficient margin to add")
        );
        // err 但没有文本 / 文本不是字符串：仍然是拒绝。
        assert!(refused(200, r#"{"status":"err"}"#).contains("no message"));
        assert!(refused(200, r#"{"status":"err","response":{"a":1}}"#).contains("no message"));
        // 文本去控制字符并截到 200 个字符。
        let long = format!(
            "{{\"status\":\"err\",\"response\":\"a\\nb{}\"}}",
            "x".repeat(400)
        );
        let reason = refused(200, &long);
        assert!(!reason.contains('\n'));
        assert!(reason.contains("ab"));
        assert!(reason.len() < 300);
        // 限频与其余 4xx：处理前拒绝，没动钱。
        assert!(refused(429, "slow down").contains("429"));
        assert!(refused(422, "Failed to deserialize the JSON body").contains("422"));
        assert!(!refused(422, "SECRET-BODY").contains("SECRET-BODY"));
        assert!(refused(403, "").contains("403"));
        assert!(refused(400, "").contains("400"));
        // 5xx、408、3xx、读不懂的 2xx：不知道，可能已经生效。
        for status in [500, 502, 503, 504, 408, 302] {
            assert!(unknown(status, Some("")).contains(&status.to_string()));
        }
        assert!(unknown(200, Some("not json")).contains("unreadable"));
        assert!(unknown(200, None).contains("unreadable"));
        assert!(unknown(200, Some("{}")).contains("unrecognised"));
        assert!(unknown(200, Some(r#"{"status":"pending"}"#)).contains("unrecognised"));
        assert!(unknown(200, Some(r#"[]"#)).contains("unrecognised"));
    }

    #[test]
    fn signed_margin_request_has_the_exact_envelope() {
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let action = UpdateIsolatedMarginAction {
            kind: "updateIsolatedMargin",
            asset: 1,
            is_buy: true,
            ntli: 25_000_000,
        };
        let body = broker.signed_request(&action).unwrap();
        assert_eq!(
            body["action"],
            json!({"type":"updateIsolatedMargin","asset":1,"isBuy":true,"ntli":25_000_000})
        );
        assert!(body["vaultAddress"].is_null());
        assert!(body.get("expiresAfter").is_none());
        let nonce = body["nonce"].as_u64().unwrap();
        let expected = sign(&broker.signer, action_hash(&action, nonce).unwrap()).unwrap();
        assert_eq!(
            body["signature"],
            json!({"r":expected.r,"s":expected.s,"v":expected.v})
        );
        // 两次请求的 nonce 严格递增（与订单共用同一个进程内计数器）。
        let next = broker.signed_request(&action).unwrap();
        assert!(next["nonce"].as_u64().unwrap() > nonce);
        // 只读模式签不出任何写请求。
        let read_only = test_broker(HyperliquidDex::Main, 0, false, Duration::from_secs(1));
        assert!(read_only.signed_request(&action).is_err());
    }

    /// 2026-10-09 实盘 `orderStatus` 对已成交 IOC 单的应答（io:SNDK 卖出 0.4911）。
    fn filled_order_status() -> Value {
        json!({"status":"order","order":{"order":{
            "coin":"io:SNDK","side":"A","limitPx":"1623.8","sz":"0.0","oid":100000000001u64,
            "timestamp":1791552614871u64,"triggerCondition":"N/A","isTrigger":false,
            "triggerPx":"0.0","children":[],"isPositionTpsl":false,"reduceOnly":false,
            "orderType":"Limit","origSz":"0.4911","tif":"Ioc",
            "cloid":"0x0c5bb80cad7695768327520496232c7b"
        },"status":"filled","statusTimestamp":1791552614871u64}})
    }

    fn order_status_script(
        unknown_first: usize,
    ) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static {
        move |path, body, nth| match (path, body["type"].as_str()) {
            ("/info", Some("orderStatus")) if nth < unknown_first => {
                Reply::Json(200, json!({"status":"unknownOid"}))
            }
            ("/info", Some("orderStatus")) => Reply::Json(200, filled_order_status()),
            _ => Reply::Json(404, json!({})),
        }
    }

    #[tokio::test]
    async fn accepted_order_is_found_once_the_lagging_read_node_catches_up() {
        // 某次实盘：卖单已全部成交，但成交后头几次 orderStatus 还是 unknownOid。
        let mock = Mock::start(order_status_script(3)).await;
        let broker = test_broker(HyperliquidDex::Io, 2, true, Duration::from_secs(1));
        let remote = broker
            .remote_status_after_ack(&mock.info(), json!("0x0c5b"), FAST)
            .await
            .unwrap()
            .expect("the order exists once the node catches up");
        assert_eq!(remote.status, "filled");
        assert_eq!(
            mock.count("info:orderStatus"),
            4,
            "stops polling as soon as it is found"
        );
    }

    #[tokio::test]
    async fn accepted_order_still_unknown_after_all_polls_stays_unknown() {
        // 轮询有上限：一直 unknownOid 就如实返回 None，由调用方按「已受理但查不到」处理，不当成没发出去。
        let mock = Mock::start(order_status_script(usize::MAX)).await;
        let broker = test_broker(HyperliquidDex::Io, 2, true, Duration::from_secs(1));
        let remote = broker
            .remote_status_after_ack(&mock.info(), json!("0x0c5b"), FAST)
            .await
            .unwrap();
        assert!(remote.is_none());
        assert_eq!(mock.count("info:orderStatus"), ORDER_READBACK_POLLS);
    }

    #[tokio::test]
    async fn read_only_broker_refuses_before_sending_anything() {
        let mock = Mock::start(eth_script(|_| eth_state(RAW_BEFORE), ok_reply)).await;
        let broker = test_broker(HyperliquidDex::Main, 0, false, Duration::from_secs(1));
        assert!(broker.supports_add_margin());
        let err = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
        assert_eq!(mock.total(), 0, "read-only mode must not even read");
        // 公开入口走真实 URL，但在授权检查处就返回，不会有任何网络访问。
        let err = broker.add_margin(&eth(), dec!(25)).await.unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
    }

    #[tokio::test]
    async fn invalid_amounts_fail_before_any_request() {
        let mock = Mock::start(eth_script(|_| eth_state(RAW_BEFORE), ok_reply)).await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        for amount in [dec!(0), dec!(-5), dec!(0.0000001), dec!(1.2345678)] {
            let result = broker
                .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), amount)
                .await;
            assert!(result.is_err(), "{amount}");
        }
        assert_eq!(mock.total(), 0);
    }

    #[tokio::test]
    async fn add_margin_applies_only_after_the_readback_shows_the_increase() {
        // 第 0 次读 = 补之前；第 1 次读还是旧值（节点略落后）；第 2 次读到 +25。
        let mock = Mock::start(eth_script(
            |nth| eth_state(if nth < 2 { RAW_BEFORE } else { RAW_PLUS_25 }),
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        assert_eq!(outcome, MarginOutcome::Applied);
        // 先读币表和补之前的保证金，再写一次，最后只读到确认为止。
        assert_eq!(
            mock.sequence(),
            [
                "info:meta",
                "info:clearinghouseState",
                "exchange",
                "info:clearinghouseState",
                "info:clearinghouseState",
            ]
        );
        let writes = mock.writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0]["action"],
            json!({"type":"updateIsolatedMargin","asset":1,"isBuy":true,"ntli":25_000_000})
        );
        // 发出去的就是本地签好的那个信封。
        let action = UpdateIsolatedMarginAction {
            kind: "updateIsolatedMargin",
            asset: 1,
            is_buy: true,
            ntli: 25_000_000,
        };
        let nonce = writes[0]["nonce"].as_u64().unwrap();
        let expected = sign(&broker.signer, action_hash(&action, nonce).unwrap()).unwrap();
        assert_eq!(
            writes[0]["signature"],
            json!({"r":expected.r,"s":expected.s,"v":expected.v})
        );
        assert!(writes[0]["vaultAddress"].is_null());
        // 读的是交易账户（不是 API 钱包），主 dex 的 `dex` 是空串。
        assert_eq!(
            mock.info_bodies("clearinghouseState")[0],
            json!({"type":"clearinghouseState","user":hex(&[0x11; 20]),"dex":""})
        );
    }

    #[tokio::test]
    async fn acknowledged_but_unconfirmed_margin_is_unknown_even_when_margin_used_moves() {
        // 账本不动，但 marginUsed 因浮盈变化涨了 25：只看 marginUsed 会误报「已生效」。
        let mock = Mock::start(eth_script(
            |nth| {
                if nth == 0 {
                    eth_state(RAW_BEFORE)
                } else {
                    position_state(
                        "ETH",
                        isolated_leverage(RAW_BEFORE),
                        "-3.3647",
                        "-28.83763",
                        "2965.786073",
                    )
                }
            },
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("expected Unknown, got {outcome:?}");
        };
        assert!(reason.contains("acknowledged"), "{reason}");
        assert!(reason.contains("not confirmed"), "{reason}");
        assert_eq!(mock.writes().len(), 1, "never retried");
        // 补之前读 1 次 + 回读 6 次。
        assert_eq!(
            mock.count("info:clearinghouseState"),
            1 + MARGIN_READBACK_POLLS
        );
    }

    #[tokio::test]
    async fn a_decrease_after_an_ok_ack_is_unknown_with_a_loud_reason() {
        // 万一符号约定反了：ok 之后账本少了 25。绝不能报 Applied。
        let mock = Mock::start(eth_script(
            |nth| eth_state(if nth == 0 { RAW_BEFORE } else { "11937.556183" }),
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("expected Unknown, got {outcome:?}");
        };
        assert!(reason.contains("DECREASED by 25"), "{reason}");
        assert!(reason.contains("opposite sign"), "{reason}");
        assert_eq!(mock.writes().len(), 1);
    }

    #[tokio::test]
    async fn a_position_size_change_during_readback_is_unknown() {
        let mock = Mock::start(eth_script(
            |nth| {
                if nth == 0 {
                    eth_state(RAW_BEFORE)
                } else {
                    position_state(
                        "ETH",
                        isolated_leverage(RAW_PLUS_25),
                        "-1.5",
                        "-53.83763",
                        "2940.786073",
                    )
                }
            },
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("expected Unknown, got {outcome:?}");
        };
        assert!(reason.contains("position size changed"), "{reason}");
        assert_eq!(mock.writes().len(), 1);
        assert_eq!(
            mock.count("info:clearinghouseState"),
            2,
            "stops at the first attributable-less read"
        );
    }

    #[tokio::test]
    async fn a_reentered_position_cannot_confirm_a_margin_add_from_raw_usd() {
        // 同量重开：rawUsd 涨了 25，但空腿入场名义也涨了 25，实际存入保证金没有增加。
        let mock = Mock::start(eth_script(
            |nth| {
                let mut state = eth_state(if nth == 0 { "125" } else { "150" });
                state["assetPositions"][0]["position"]["szi"] = json!("-1");
                state["assetPositions"][0]["position"]["entryPx"] =
                    json!(if nth == 0 { "100" } else { "125" });
                state
            },
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("expected Unknown, got {outcome:?}");
        };
        assert!(reason.contains("entry price changed"), "{reason}");
        assert_eq!(mock.writes().len(), 1);
        assert_eq!(mock.count("info:clearinghouseState"), 2);
    }

    #[tokio::test]
    async fn a_venue_error_is_refused_with_its_message_and_not_retried() {
        let mock = Mock::start(eth_script(
            |_| eth_state(RAW_BEFORE),
            || {
                Reply::Json(
                    200,
                    json!({"status":"err","response":"Insufficient margin to add isolated margin"}),
                )
            },
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Refused(reason) = outcome else {
            panic!("expected Refused, got {outcome:?}");
        };
        assert!(reason.contains("Insufficient margin"), "{reason}");
        assert_eq!(mock.writes().len(), 1);
        assert_eq!(
            mock.count("info:clearinghouseState"),
            1,
            "no readback after a refusal"
        );
    }

    #[tokio::test]
    async fn http_status_decides_refused_or_unknown_with_a_single_write() {
        for (status, refused) in [
            (429u16, true),
            (422, true),
            (403, true),
            (500, false),
            (502, false),
            (503, false),
            (408, false),
        ] {
            let mock = Mock::start(eth_script(
                |_| eth_state(RAW_BEFORE),
                move || Reply::Json(status, json!("redacted by the test")),
            ))
            .await;
            let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
            let outcome = broker
                .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
                .await
                .unwrap();
            match (&outcome, refused) {
                (MarginOutcome::Refused(reason), true) => {
                    assert!(reason.contains(&status.to_string()), "{reason}")
                }
                (MarginOutcome::Unknown(reason), false) => {
                    assert!(reason.contains(&status.to_string()), "{reason}")
                }
                _ => panic!("HTTP {status}: unexpected {outcome:?}"),
            }
            assert_eq!(mock.writes().len(), 1, "HTTP {status}: exactly one write");
        }
    }

    #[tokio::test]
    async fn a_server_error_that_was_applied_anyway_is_confirmed_by_the_readback() {
        let mock = Mock::start(eth_script(
            |nth| eth_state(if nth == 0 { RAW_BEFORE } else { RAW_PLUS_25 }),
            || Reply::Json(502, json!("bad gateway")),
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        assert_eq!(outcome, MarginOutcome::Applied);
        assert_eq!(mock.writes().len(), 1);
    }

    #[tokio::test]
    async fn a_timeout_is_unknown_with_a_single_write() {
        let mock = Mock::start(eth_script(|_| eth_state(RAW_BEFORE), || Reply::Hang)).await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_millis(300));
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("expected Unknown, got {outcome:?}");
        };
        assert!(reason.contains("timed out"), "{reason}");
        assert!(reason.contains("may or may not"), "{reason}");
        assert_eq!(mock.writes().len(), 1, "a timeout must never be retried");
    }

    #[tokio::test]
    async fn missing_cross_or_unknown_positions_are_errors_without_a_write() {
        let cross = position_state(
            "ETH",
            json!({"type":"cross","value":3}),
            "-3.3647",
            "0",
            "10",
        );
        let empty = json!({"assetPositions":[]});
        for (name, state) in [("cross", cross), ("empty", empty)] {
            let mock = Mock::start(eth_script(move |_| state.clone(), ok_reply)).await;
            let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
            let result = broker
                .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
                .await;
            assert!(result.is_err(), "{name}");
            assert!(mock.writes().is_empty(), "{name}: nothing may be written");
        }
        // 币表里没有这个币。
        let mock = Mock::start(eth_script(|_| eth_state(RAW_BEFORE), ok_reply)).await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let err = broker
            .add_margin_at(
                &mock.info(),
                &mock.exchange(),
                FAST,
                &Symbol::perp("SOL", "USDT"),
                dec!(25),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown perp market"), "{err}");
        assert!(mock.writes().is_empty());
    }

    /// 形状取自 2026-10-09 的公开应答：统一账户，各 perp dex 的 clearinghouseState 全 0，
    /// USDC 在现货里。
    fn unified_script(
        mode: &'static str,
        spot: Value,
    ) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static {
        move |path, body, _| match (path, body["type"].as_str()) {
            ("/info", Some("userAbstraction")) => Reply::Json(200, json!(mode)),
            ("/info", Some("meta")) => Reply::Json(
                200,
                json!({"universe":[{"name":"io:ANTH","szDecimals":2,"maxLeverage":6}],"collateralToken":0}),
            ),
            ("/info", Some("spotClearinghouseState")) => Reply::Json(200, spot.clone()),
            ("/info", Some("clearinghouseState")) => Reply::Json(
                200,
                json!({"marginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"assetPositions":[]}),
            ),
            _ => Reply::Json(404, json!({})),
        }
    }

    #[tokio::test]
    async fn a_unified_account_reads_free_collateral_from_spot_not_the_empty_dex_state() {
        let spot = json!({
            "balances":[{"coin":"USDC","token":0,"total":"2299.995434","hold":"0.0","entryNtl":"0.0"}],
            "tokenToAvailableAfterMaintenance":[[0,"2299.995434"]],
        });
        let mock = Mock::start(unified_script("unifiedAccount", spot)).await;
        let broker = test_broker(HyperliquidDex::Io, 10, false, Duration::from_secs(1));
        let free = broker.free_collateral_at(&mock.info()).await.unwrap();
        assert_eq!(free, Some(dec!(2299.995434)), "有钱的统一账户不能读成 0");
        assert_eq!(
            mock.info_bodies("meta")[0],
            json!({"type":"meta","dex":"io"})
        );
        assert_eq!(mock.count("info:clearinghouseState"), 0);
        assert!(mock.writes().is_empty(), "只读");

        // 已有占用：按 total − hold 与维持后可用额里小的那个算。
        let held = json!({
            "balances":[{"coin":"USDC","token":0,"total":"1000","hold":"700","entryNtl":"0.0"}],
            "tokenToAvailableAfterMaintenance":[[0,"800"]],
        });
        let mock = Mock::start(unified_script("portfolioMargin", held)).await;
        let free = broker.free_collateral_at(&mock.info()).await.unwrap();
        assert_eq!(free, Some(dec!(300)));
    }

    #[tokio::test]
    async fn standard_mode_still_uses_the_dex_scoped_clearinghouse_state() {
        let mock = Mock::start(unified_script("default", json!({"balances":[]}))).await;
        let broker = test_broker(HyperliquidDex::Io, 10, false, Duration::from_secs(1));
        // 标准模式下各 dex 独立：这个 dex 确实是 0，就是 0（拦住开仓是对的）。
        assert_eq!(
            broker.free_collateral_at(&mock.info()).await.unwrap(),
            Some(Decimal::ZERO)
        );
        assert_eq!(
            mock.info_bodies("clearinghouseState")[0]["dex"],
            json!("io")
        );
        assert_eq!(mock.count("info:spotClearinghouseState"), 0);
    }

    /// 手动联网核对（只读 `/info`，不签名、不下单）：
    /// `ARB_PROBE_HL_ACCOUNT=0x… cargo test -p arb-exec --lib live_free_collateral_probe -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "联网，手动跑"]
    async fn live_free_collateral_probe() {
        let account = std::env::var("ARB_PROBE_HL_ACCOUNT").expect("ARB_PROBE_HL_ACCOUNT");
        for (dex, index) in [
            (HyperliquidDex::Main, 0),
            (HyperliquidDex::Xyz, 1),
            (HyperliquidDex::Io, 10),
        ] {
            let mut broker = test_broker(dex, index, false, Duration::from_secs(15));
            broker.account = hex(&decode_hex::<20>(&account).unwrap());
            let free = broker.free_collateral().await.unwrap();
            println!("{}: 可用保证金 {free:?}", dex.venue());
        }
    }

    #[test]
    fn unified_free_needs_the_collateral_token_and_never_goes_negative() {
        let spot = json!({
            "balances":[{"coin":"USDT0","token":268,"total":"50","hold":"0"}],
            "tokenToAvailableAfterMaintenance":[[268,"-5"]],
        });
        assert_eq!(unified_free(&spot, 0), None, "没有这个币是不知道，不是 0");
        assert_eq!(unified_free(&spot, 268), Some(Decimal::ZERO));
        assert!(balances_live_in_spot(&json!("unifiedAccount")));
        assert!(!balances_live_in_spot(&json!("dexAbstraction")));
        assert!(!balances_live_in_spot(&json!("default")));
    }

    #[tokio::test]
    async fn hip3_top_up_uses_the_builder_asset_id_and_dex_scoped_state() {
        // xyz 在 perpDexs 里是第 1 项；TSLA 在 xyz 的 meta 里是第 1 个（0 起）→ 110001。
        let mock = Mock::start(|path, body, nth| match (path, body["type"].as_str()) {
            ("/info", Some("meta")) => Reply::Json(
                200,
                json!({"universe":[
                    {"name":"xyz:XYZ100","szDecimals":3,"maxLeverage":20},
                    {"name":"xyz:TSLA","szDecimals":2,"maxLeverage":10},
                ]}),
            ),
            ("/info", Some("clearinghouseState")) => {
                let raw = if nth == 0 { "-87.622794" } else { "-62.622794" };
                Reply::Json(
                    200,
                    position_state(
                        "xyz:TSLA",
                        json!({"type":"isolated","value":10,"rawUsd":raw}),
                        "0.263",
                        "-0.00263",
                        "9.879196",
                    ),
                )
            }
            ("/exchange", _) => ok_reply(),
            _ => Reply::Json(404, json!({})),
        })
        .await;
        let broker = test_broker(HyperliquidDex::Xyz, 1, true, Duration::from_secs(1));
        let outcome = broker
            .add_margin_at(
                &mock.info(),
                &mock.exchange(),
                FAST,
                &Symbol::perp("TSLA", "USDT"),
                dec!(25),
            )
            .await
            .unwrap();
        assert_eq!(outcome, MarginOutcome::Applied);
        let writes = mock.writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0]["action"],
            json!({"type":"updateIsolatedMargin","asset":110_001,"isBuy":true,"ntli":25_000_000})
        );
        assert_eq!(
            mock.info_bodies("meta")[0],
            json!({"type":"meta","dex":"xyz"})
        );
        assert_eq!(
            mock.info_bodies("clearinghouseState")[0]["dex"],
            json!("xyz")
        );
    }

    #[tokio::test]
    async fn a_margin_write_waits_for_the_order_submit_lock() {
        let mock = Mock::start(eth_script(
            |nth| eth_state(if nth == 0 { RAW_BEFORE } else { RAW_PLUS_25 }),
            ok_reply,
        ))
        .await;
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        let order_in_flight = broker.submit.lock().await;
        let blocked = tokio::time::timeout(
            Duration::from_millis(150),
            broker.add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25)),
        )
        .await;
        assert!(
            blocked.is_err(),
            "must wait while an order write holds the lock"
        );
        assert_eq!(
            mock.total(),
            0,
            "nothing is read or sent before the lock is held"
        );
        drop(order_in_flight);
        let outcome = broker
            .add_margin_at(&mock.info(), &mock.exchange(), FAST, &eth(), dec!(25))
            .await
            .unwrap();
        assert_eq!(outcome, MarginOutcome::Applied);
    }

    /// io:SNDK 买单：800 USDT、限价 1623.8、3 倍逐仓。按 szDecimals 4 向下取整后下单量 0.4926。
    fn sndk_buy() -> NewOrder {
        NewOrder {
            post_only: false,
            client_order_id: ClientOrderId("live-1-buy-0".into()),
            venue: Venue::HyperliquidIo,
            symbol: Symbol::perp("SNDK", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(800),
            quantity: None,
            limit_price: Some(dec!(1623.8)),
            reduce_only: false,
            margin_mode: crate::MarginMode::Isolated,
            leverage: Some(dec!(3)),
        }
    }

    const SNDK_OID: u64 = 100000000001;
    const SNDK_AT: u64 = 1791552614871;

    /// 下单与读回的假交易所：第一次 `orderStatus`（下单前的幂等检查）没有这单；之后头 `lag` 次
    /// 读回仍是 `unknownOid`（读节点落后），再往后是全部成交。逐笔成交两笔，合计 0.4926。
    /// `exchange` 是下单回包（`None` = 回 `{"error":…}`）。
    fn sndk_script(
        lag: usize,
        exchange: Option<Value>,
    ) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static {
        let cloid = client_cloid(&sndk_buy().client_order_id);
        move |path, body, nth| match (path, body["type"].as_str()) {
            ("/info", Some("orderStatus")) if nth <= lag => {
                Reply::Json(200, json!({"status":"unknownOid"}))
            }
            ("/info", Some("orderStatus")) => Reply::Json(
                200,
                json!({"status":"order","order":{"order":{
                    "coin":"io:SNDK","side":"B","limitPx":"1623.8","sz":"0.0","oid":SNDK_OID,
                    "timestamp":SNDK_AT,"reduceOnly":false,"origSz":"0.4926","cloid":cloid
                },"status":"filled","statusTimestamp":SNDK_AT}}),
            ),
            ("/info", Some("meta")) => Reply::Json(
                200,
                json!({"universe":[{"name":"io:SNDK","szDecimals":4,"maxLeverage":10,"onlyIsolated":true}]}),
            ),
            ("/info", Some("activeAssetData")) => Reply::Json(
                200,
                json!({"coin":"io:SNDK","leverage":{"type":"isolated","value":3}}),
            ),
            ("/info", Some("userFillsByTime")) => Reply::Json(
                200,
                json!([
                    {"oid":SNDK_OID,"tid":1,"time":SNDK_AT,"coin":"io:SNDK","side":"B","sz":"0.3","px":"1623.7","fee":"0.2192","feeToken":"USDC"},
                    {"oid":SNDK_OID,"tid":2,"time":SNDK_AT,"coin":"io:SNDK","side":"B","sz":"0.1926","px":"1623.8","fee":"0.1407","feeToken":"USDC"}
                ]),
            ),
            ("/exchange", _) => Reply::Json(
                200,
                match &exchange {
                    Some(status) => {
                        json!({"status":"ok","response":{"type":"order","data":{"statuses":[status]}}})
                    }
                    None => json!({"status":"ok","response":{"type":"order","data":{"statuses":[
                        {"error":"Order could not immediately match against any resting orders."}
                    ]}}}),
                },
            ),
            _ => Reply::Json(404, json!({})),
        }
    }

    fn sndk_filled() -> Value {
        json!({"filled":{"totalSz":"0.4926","avgPx":"1623.75","oid":SNDK_OID}})
    }

    fn mocked_broker(mock: &Mock) -> HyperliquidBroker {
        let mut broker = test_broker(HyperliquidDex::Io, 2, true, Duration::from_secs(1));
        broker.info_url = mock.info();
        broker.exchange_url = mock.exchange();
        broker
    }

    /// 平掉 某合约 零头：卖 0.002（限价 1600，约 3.2 USDT，低于 10 美元下限），持仓 `szi` 由参数给。
    fn sndk_dust_close() -> NewOrder {
        NewOrder {
            client_order_id: ClientOrderId("live-1-sell-exit1".into()),
            side: Side::Sell,
            notional_usdt: dec!(3.2),
            quantity: Some(dec!(0.002)),
            limit_price: Some(dec!(1600)),
            reduce_only: true,
            leverage: None,
            ..sndk_buy()
        }
    }

    fn dust_script(
        szi: &'static str,
    ) -> impl Fn(&str, &Value, usize) -> Reply + Send + Sync + 'static {
        move |path, body, _| match (path, body["type"].as_str()) {
            ("/info", Some("orderStatus")) => Reply::Json(200, json!({"status":"unknownOid"})),
            ("/info", Some("meta")) => Reply::Json(
                200,
                json!({"universe":[{"name":"io:SNDK","szDecimals":4,"maxLeverage":10,"onlyIsolated":true}]}),
            ),
            ("/info", Some("clearinghouseState")) => Reply::Json(
                200,
                position_state("io:SNDK", isolated_leverage("10"), szi, "0", "1"),
            ),
            ("/exchange", _) => Reply::Json(
                200,
                json!({"status":"ok","response":{"type":"order","data":{"statuses":[
                    {"filled":{"totalSz":"0.002","avgPx":"1600","oid":SNDK_OID}}
                ]}}}),
            ),
            _ => Reply::Json(404, json!({})),
        }
    }

    #[tokio::test]
    async fn a_reduce_only_order_closing_the_whole_dust_position_is_sent_despite_the_ten_dollar_minimum()
     {
        // 某次实盘：第一腿只吃到 0.30 USDT，回滚与每次平仓都被本地 10 美元下限拦下，零头平不掉。
        let mock = Mock::start(dust_script("0.002")).await;
        let broker = mocked_broker(&mock);
        let _ = broker.place(&sndk_dust_close()).await;
        assert!(
            mock.sequence().iter().any(|step| step == "exchange"),
            "整笔平掉零头的 reduce-only 单要发出去：{:?}",
            mock.sequence()
        );
    }

    #[tokio::test]
    async fn a_reduce_only_order_below_ten_dollars_that_leaves_a_remainder_is_still_refused() {
        let mock = Mock::start(dust_script("0.005")).await;
        let broker = mocked_broker(&mock);
        let error = broker
            .place(&sndk_dust_close())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("$10 minimum"), "{error}");
        assert!(
            !mock.sequence().iter().any(|step| step == "exchange"),
            "只平一部分、又低于下限：不发"
        );
    }

    #[tokio::test]
    async fn a_hedge_first_leg_returns_the_ioc_fill_before_any_readback_and_settles_to_the_fills() {
        let mock = Mock::start(sndk_script(1, Some(sndk_filled()))).await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        let provisional = broker
            .place_for_hedge(&order)
            .await
            .unwrap()
            .expect("IOC 回包里有成交：直接给待核实状态");
        assert_eq!(
            mock.sequence(),
            [
                "info:orderStatus",
                "info:meta",
                "info:activeAssetData",
                "exchange"
            ],
            "不等订单状态、逐笔成交的读回"
        );
        assert_eq!(provisional.status, OrderStatus::Filled);
        assert_eq!(provisional.venue_order_id.as_deref(), Some("100000000001"));
        // 回包均价 1623.75 往不利方向（买：往上）放一位，不越过限价 1623.8。
        assert_eq!(provisional.average_price, Some(dec!(1623.76)));
        assert_eq!(provisional.filled_usdt, dec!(0.4926) * dec!(1623.76));
        assert_eq!(
            provisional.fee_usdt,
            provisional.filled_usdt * dec!(0.00045)
        );
        assert!(
            provisional.fee_estimated,
            "按回包记的手续费是估的，要标出来"
        );
        let cloid = client_cloid(&order.client_order_id);
        assert!(
            broker.known_terminal(&cloid).is_none(),
            "待核实状态不进终态缓存"
        );

        // 核实：读节点先落后一次，再按逐笔成交还原精确成交额与实收手续费。
        let settled = broker
            .settle_at(&mock.info(), &provisional, FAST)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.order, order);
        assert_eq!(settled.status, OrderStatus::Filled);
        assert_eq!(
            settled.filled_usdt,
            dec!(0.3) * dec!(1623.7) + dec!(0.1926) * dec!(1623.8)
        );
        assert_eq!(settled.fee_usdt, dec!(0.3599));
        assert!(!settled.fee_estimated);
        // 读回过一次实际费率：之后这个合约按回包估手续费就用它，不再用配置的吃单费率。
        assert_eq!(
            broker.estimated_fee_rate(&order.symbol),
            dec!(0.3599) / settled.filled_usdt
        );
        assert_eq!(
            broker.estimated_fee_rate(&Symbol::perp("OTHER", "USDT")),
            dec!(0.00045)
        );
        assert!(settled.average_price.unwrap() < provisional.average_price.unwrap());
        assert_eq!(mock.count("info:userFillsByTime"), 1);
        assert!(mock.count("info:orderStatus") >= 3);
        // 核实过的终态进缓存：之后的 order_state 不再往返。
        let before = mock.total();
        let again = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.filled_usdt, settled.filled_usdt);
        assert_eq!(mock.total(), before);
    }

    #[tokio::test]
    async fn a_plain_place_still_reads_back_the_order_and_its_fills() {
        let mock = Mock::start(sndk_script(0, Some(sndk_filled()))).await;
        let broker = mocked_broker(&mock);
        let acked = broker.place(&sndk_buy()).await.unwrap();
        assert_eq!(acked.venue_order_id, "100000000001");
        assert_eq!(acked.status, OrderStatus::Filled);
        // 执行器紧接着的 order_state：IOC 终态刚读回核对过，直接给，不再读两次。
        let state = broker
            .order_state(&sndk_buy().client_order_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.status, OrderStatus::Filled);
        assert_eq!(state.fee_usdt, dec!(0.3599));
        assert_eq!(
            mock.sequence(),
            [
                "info:orderStatus",
                "info:meta",
                "info:activeAssetData",
                "exchange",
                "info:orderStatus",
                "info:userFillsByTime"
            ]
        );
    }

    #[tokio::test]
    async fn warming_up_leaves_preparing_and_placing_without_meta_or_leverage_round_trips() {
        let mock = Mock::start(sndk_script(0, Some(sndk_filled()))).await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        broker
            .warm_reads(&order.symbol, Some(dec!(3)))
            .await
            .unwrap();
        assert_eq!(mock.sequence(), ["info:meta", "info:activeAssetData"]);
        broker
            .prepare_open_mode(&order.symbol, Side::Buy, Some(dec!(3)), order.margin_mode)
            .await
            .unwrap();
        assert_eq!(mock.total(), 2, "预热过：准备阶段一次往返都没有");
        broker.place_for_hedge(&order).await.unwrap().unwrap();
        assert_eq!(mock.count("info:meta"), 1);
        assert_eq!(mock.count("info:activeAssetData"), 1);

        // 杠杆不一致时预热不记、不改：留给准备阶段去改。
        let other = Mock::start(|path, body, _| match (path, body["type"].as_str()) {
            ("/info", Some("meta")) => Reply::Json(
                200,
                json!({"universe":[{"name":"io:SNDK","szDecimals":4,"maxLeverage":10}]}),
            ),
            ("/info", Some("activeAssetData")) => Reply::Json(
                200,
                json!({"coin":"io:SNDK","leverage":{"type":"isolated","value":5}}),
            ),
            _ => Reply::Json(404, json!({})),
        })
        .await;
        let broker = mocked_broker(&other);
        broker
            .warm_reads(&order.symbol, Some(dec!(3)))
            .await
            .unwrap();
        assert!(other.writes().is_empty(), "预热只读");
        assert!(!broker.verified_leverage.is_fresh(&(
            "io:SNDK".to_string(),
            3,
            crate::MarginMode::Isolated
        )));
    }

    #[tokio::test]
    async fn a_rejected_order_drops_the_cached_meta() {
        let mock = Mock::start(sndk_script(0, None)).await;
        let broker = mocked_broker(&mock);
        let err = broker.place_for_hedge(&sndk_buy()).await.unwrap_err();
        assert!(err.to_string().contains("rejected"), "{err}");
        assert!(broker.meta.get(META_TTL).is_none(), "下一次重新拉 meta");
        assert_eq!(mock.count("info:userFillsByTime"), 0);
    }

    /// 下单请求在处理前就被拒（限频 429、其它 4xx、顶层 `status: err`）：这张单没进撮合，读回给「被拒」，
    /// 不再是永远「查不到、不能当没成交」。5xx 仍然是结果不明。
    #[tokio::test]
    async fn an_order_refused_before_processing_reads_back_as_rejected() {
        for (status, body) in [
            (429, json!({})),
            (422, json!({})),
            (
                200,
                json!({"status":"err","response":"User or API Wallet does not exist."}),
            ),
        ] {
            let script = sndk_script(usize::MAX, None);
            let mock = Mock::start(move |path, body_in, nth| match path {
                "/exchange" => Reply::Json(status, body.clone()),
                _ => script(path, body_in, nth),
            })
            .await;
            let broker = mocked_broker(&mock);
            let order = sndk_buy();
            let err = broker.place(&order).await.unwrap_err();
            assert!(
                err.to_string().contains("refused before processing"),
                "{status}: {err}"
            );
            assert_eq!(mock.count("exchange"), 1, "只发一次");
            let state = broker
                .order_state(&order.client_order_id)
                .await
                .unwrap()
                .expect("被拒的单有结局");
            assert_eq!(state.status, OrderStatus::Rejected, "{status}");
        }
        let mock = Mock::start({
            let script = sndk_script(usize::MAX, None);
            move |path, body, nth| match path {
                "/exchange" => Reply::Json(502, json!({})),
                _ => script(path, body, nth),
            }
        })
        .await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        assert!(broker.place(&order).await.is_err());
        assert!(
            broker.order_state(&order.client_order_id).await.is_err(),
            "5xx 可能已经下了：仍然不能当成没成交"
        );
    }

    #[test]
    fn order_replies_are_classified_like_margin_replies() {
        let ok = r#"{"status":"ok","response":{"type":"order","data":{"statuses":[]}}}"#;
        assert!(matches!(
            classify_order_reply(200, Some(ok)),
            OrderSend::Accepted(_)
        ));
        for (status, body) in [
            (200, Some(r#"{"status":"err","response":"bad nonce"}"#)),
            (429, None),
            (400, None),
            (422, Some("x")),
        ] {
            assert!(
                matches!(classify_order_reply(status, body), OrderSend::Refused(_)),
                "{status}"
            );
        }
        for (status, body) in [
            (200, Some("not json")),
            (200, Some(r#"{"status":"?"}"#)),
            (408, None),
            (500, None),
            (502, None),
            (302, None),
        ] {
            assert!(
                matches!(classify_order_reply(status, body), OrderSend::Unknown(_)),
                "{status}"
            );
        }
    }

    /// 10-17 xyz:MSFT：IOC 回包已报全部成交，读节点 8 秒多还是 `unknownOid`。按回包记，不当成结果不明。
    #[tokio::test]
    async fn a_filled_reply_stands_in_when_the_read_node_never_catches_up() {
        let mock = Mock::start(sndk_script(usize::MAX, Some(sndk_filled()))).await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        let acked = broker.place(&order).await.unwrap();
        assert_eq!(acked.venue_order_id, SNDK_OID.to_string());
        assert_eq!(acked.status, OrderStatus::Filled);
        assert_eq!(
            mock.count("info:orderStatus"),
            1 + ORDER_READBACK_POLLS,
            "下单前查一次，下单后照常轮询完"
        );
        let state = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .expect("回包给过成交：不是结果不明");
        assert_eq!(state.order, order);
        assert_eq!(state.status, OrderStatus::Filled);
        assert_eq!(state.venue_order_id.as_deref(), Some("100000000001"));
        // 同第一腿的待核实状态：均价往不利方向放一位，手续费按吃单费率估。
        assert_eq!(state.average_price, Some(dec!(1623.76)));
        assert_eq!(state.filled_usdt, dec!(0.4926) * dec!(1623.76));
        assert_eq!(state.fee_usdt, state.filled_usdt * dec!(0.00045));
        assert_eq!(mock.count("info:userFillsByTime"), 0);
        let cloid = client_cloid(&order.client_order_id);
        assert!(
            broker.known_terminal(&cloid).is_none(),
            "估算的状态不进终态缓存"
        );
    }

    /// 订单状态先查到、逐笔成交还没追上：回包已报全部成交，就按回包记，不当成结果不明。
    #[tokio::test]
    async fn a_filled_reply_stands_in_when_fills_lag_behind_the_order_status() {
        let inner = sndk_script(0, Some(sndk_filled()));
        let mock = Mock::start(move |path: &str, body: &Value, nth: usize| {
            if path == "/info" && body["type"].as_str() == Some("userFillsByTime") {
                return Reply::Json(200, json!([]));
            }
            inner(path, body, nth)
        })
        .await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        let acked = broker.place(&order).await.unwrap();
        assert_eq!(acked.status, OrderStatus::Filled);
        assert!(
            mock.count("info:userFillsByTime") > 0,
            "查过成交明细、是空的"
        );
        let state = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .expect("回包给过成交：不是结果不明");
        assert_eq!(state.status, OrderStatus::Filled);
        // 回包的数（均价往不利方向放一位），不是逐笔成交算出来的。
        assert_eq!(state.filled_usdt, dec!(0.4926) * dec!(1623.76));
    }

    #[tokio::test]
    async fn once_the_read_node_catches_up_the_readback_replaces_the_reply() {
        // 下单前 1 次 + 轮询 ORDER_READBACK_POLLS 次都查不到，之后的 order_state 查到了。
        let mock = Mock::start(sndk_script(ORDER_READBACK_POLLS, Some(sndk_filled()))).await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        broker.place(&order).await.unwrap();
        let state = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.fee_usdt, dec!(0.3599), "实收手续费，不是估的");
        assert_eq!(
            state.filled_usdt,
            dec!(0.3) * dec!(1623.7) + dec!(0.1926) * dec!(1623.8)
        );
        let cloid = client_cloid(&order.client_order_id);
        assert!(
            broker.replied_state(&cloid).is_none(),
            "读到了就清掉回包的结局"
        );
        assert!(broker.known_terminal(&cloid).is_some());
    }

    /// 10-16 xyz:CRWV：第二腿 IOC 没有对手盘被拒，读节点 4 秒多才查得到，强制补腿因此晚了 4.5 秒。
    #[tokio::test]
    async fn an_error_reply_is_a_definite_rejection_before_the_read_node_knows_the_order() {
        let mock = Mock::start(sndk_script(usize::MAX, None)).await;
        let broker = mocked_broker(&mock);
        let order = sndk_buy();
        let err = broker.place(&order).await.unwrap_err();
        assert!(err.to_string().contains("rejected"), "{err}");
        let state = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .expect("回包明确拒绝：没成交，不是结果不明");
        assert_eq!(state.order, order);
        assert_eq!(state.status, OrderStatus::Rejected);
        assert_eq!(state.filled_usdt, Decimal::ZERO);
        assert!(state.venue_order_id.is_none());
        assert!(
            state
                .reject_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("could not immediately match")),
            "{:?}",
            state.reject_reason
        );
        // 被拒不轮询：下单前 1 次 + order_state 1 次。
        assert_eq!(mock.count("info:orderStatus"), 2);
        // 快速下单（第一腿）被拒同样记下。
        let mut first = sndk_buy();
        first.client_order_id = ClientOrderId("live-2-buy-0".into());
        broker.place_for_hedge(&first).await.unwrap_err();
        let state = broker
            .order_state(&first.client_order_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.status, OrderStatus::Rejected);
    }

    /// 安全底线：回包没给出能用的结局（字段缺失、读不懂、根本没收到回包）时，查不到仍然是结果不明，
    /// 绝不能当成没发出去或没成交。
    #[tokio::test]
    async fn without_a_usable_reply_an_unqueryable_order_stays_unknown() {
        for reply in [
            Some(json!({"filled":{"totalSz":"0.4926","oid":SNDK_OID}})),
            Some(json!({"filled":{"totalSz":"0","avgPx":"1623.75","oid":SNDK_OID}})),
            Some(json!({"resting":{"oid":SNDK_OID}})),
        ] {
            let mock = Mock::start(sndk_script(usize::MAX, reply.clone())).await;
            let broker = mocked_broker(&mock);
            let order = sndk_buy();
            let err = broker.place(&order).await.unwrap_err();
            assert!(
                err.to_string().contains("not yet queryable"),
                "{reply:?}: {err}"
            );
            let err = broker
                .order_state(&order.client_order_id)
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("no longer queryable"),
                "{reply:?}: {err}"
            );
        }
        // 下单请求本身没拿到回包（5xx）：同样是结果不明。
        let lost = Mock::start(|path, body, nth| match (path, body["type"].as_str()) {
            ("/exchange", _) => Reply::Json(502, json!({})),
            _ => sndk_script(usize::MAX, None)(path, body, nth),
        })
        .await;
        let broker = mocked_broker(&lost);
        let order = sndk_buy();
        broker.place(&order).await.unwrap_err();
        let err = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no longer queryable"), "{err}");
    }

    #[test]
    fn the_provisional_fill_is_conservative_and_only_taken_from_a_fill_reply() {
        let broker = test_broker(HyperliquidDex::Io, 2, true, Duration::from_secs(1));
        let buy = sndk_buy();
        let quantity = dec!(0.4926);
        let limit = dec!(1623.8);
        let state = |status: Value, order: &NewOrder, limit| {
            broker
                .provisional(order, &status, quantity, limit, 4)
                .unwrap()
        };
        // 部分成交：IOC 剩下的已撤。
        let partial = state(
            json!({"filled":{"totalSz":"0.2","avgPx":"1623.8","oid":1}}),
            &buy,
            limit,
        )
        .unwrap();
        assert_eq!(partial.status, OrderStatus::Cancelled);
        assert_eq!(
            partial.average_price,
            Some(dec!(1623.8)),
            "放一位也不越过限价"
        );
        // 卖单往下放，同样不越过限价。
        let mut sell = sndk_buy();
        sell.side = Side::Sell;
        let sold = state(
            json!({"filled":{"totalSz":"0.4926","avgPx":"1623.712","oid":1}}),
            &sell,
            dec!(1623.5),
        )
        .unwrap();
        assert_eq!(sold.average_price, Some(dec!(1623.711)));
        for status in [
            json!({"resting":{"oid":1}}),
            json!({"error":"no match"}),
            json!({"filled":{"totalSz":"0.5","avgPx":"1623.8","oid":1}}),
            json!({"filled":{"totalSz":"0.4926","avgPx":"0","oid":1}}),
            json!({"filled":{"totalSz":"0.4926","avgPx":"1623.8"}}),
        ] {
            assert!(state(status.clone(), &buy, limit).is_none(), "{status}");
        }

        // 低价合约：回包 "5.36" 是去掉了末尾 0 的 5.3600，放一个 tick（0.0001），不是 0.01。
        let mut near = sndk_buy();
        near.limit_price = Some(dec!(5.3707));
        let low = broker
            .provisional(
                &near,
                &json!({"filled":{"totalSz":"149","avgPx":"5.36","oid":1}}),
                dec!(149),
                dec!(5.3707),
                1,
            )
            .unwrap()
            .unwrap();
        assert_eq!(low.average_price, Some(dec!(5.3601)));
        assert_eq!(tick_decimals(dec!(5.36), 1), 4);
        assert_eq!(tick_decimals(dec!(1623.75), 4), 1);
        assert_eq!(tick_decimals(dec!(0.012345), 0), 6);
        assert_eq!(tick_decimals(dec!(0.012345), 2), 4);
        assert_eq!(tick_decimals(dec!(123456), 0), 0);
    }

    /// 没人监听的本地端口：连接立刻被拒绝，证明这条路径确实去联网了，又不碰任何真实服务。
    const DEAD_INFO: &str = "http://127.0.0.1:1/info";

    fn live_test_asset(name: &str) -> Asset {
        serde_json::from_value(json!({"name":name, "szDecimals":2, "maxLeverage":10})).unwrap()
    }

    #[tokio::test]
    // 测试之间靠这把锁串行（实时顶档缓存是进程内全局的）；每个测试各有线程和运行时，不会死锁。
    #[allow(clippy::await_holding_lock)]
    async fn market_style_orders_are_priced_from_the_live_top_without_a_book_request() {
        let _guard = arb_venues::live_book::test_lock();
        let venue = Venue::Hyperliquid;
        arb_venues::live_book::heartbeat(venue, std::time::Instant::now());
        arb_venues::live_book::set_top(
            venue,
            "TESTBBO",
            Some((dec!(99), dec!(5))),
            Some((dec!(101), dec!(5))),
            None,
        );
        let mut broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_secs(1));
        broker.info_url = DEAD_INFO.to_string();
        broker.options.market_slippage = Some(dec!(0.01));
        let asset = live_test_asset("TESTBBO");
        let mut buy = order();
        buy.symbol = Symbol::perp("TESTBBO", "USDT");
        buy.limit_price = None;
        buy.side = Side::Buy;
        // 买：对手方是卖一 101，上浮滑点上限 1% = 102.01（取整到合法价位）。
        let price = broker.price(&buy, &asset).await.unwrap();
        assert!(price >= dec!(101) && price <= dec!(102.01), "{price}");
        let mut sell = buy.clone();
        sell.side = Side::Sell;
        // 卖：对手方是买一 99，下浮 1% = 98.01。
        let price = broker.price(&sell, &asset).await.unwrap();
        assert!(price >= dec!(98.01) && price <= dec!(99), "{price}");
        // 显式限价永远优先于缓存。
        let mut limit = buy.clone();
        limit.limit_price = Some(dec!(100.5));
        assert_eq!(broker.price(&limit, &asset).await.unwrap(), dec!(100.5));
        arb_venues::live_book::remove_venue(venue);
    }

    #[tokio::test]
    // 测试之间靠这把锁串行（实时顶档缓存是进程内全局的）；每个测试各有线程和运行时，不会死锁。
    #[allow(clippy::await_holding_lock)]
    async fn a_dead_connection_or_one_sided_top_falls_back_to_the_rest_book() {
        let _guard = arb_venues::live_book::test_lock();
        let mut broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_millis(300));
        broker.info_url = DEAD_INFO.to_string();
        broker.options.market_slippage = Some(dec!(0.01));
        let venue = Venue::Hyperliquid;
        let mut buy = order();
        buy.limit_price = None;
        buy.side = Side::Buy;
        // 心跳太旧（连接静默死了）：顶档再「新」也不用。测试里 REST 连不上，走到 REST 就是 Err。
        buy.symbol = Symbol::perp("TESTSTALE", "USDT");
        arb_venues::live_book::set_top(
            venue,
            "TESTSTALE",
            Some((dec!(99), dec!(5))),
            Some((dec!(101), dec!(5))),
            None,
        );
        arb_venues::live_book::heartbeat(
            venue,
            std::time::Instant::now() - Duration::from_secs(60),
        );
        assert!(
            broker
                .price(&buy, &live_test_asset("TESTSTALE"))
                .await
                .is_err()
        );
        // 买单要卖一，卖一那侧没有挂单：不用缓存。
        arb_venues::live_book::heartbeat(venue, std::time::Instant::now());
        buy.symbol = Symbol::perp("TESTONESIDE", "USDT");
        arb_venues::live_book::set_top(venue, "TESTONESIDE", Some((dec!(99), dec!(5))), None, None);
        assert!(
            broker
                .price(&buy, &live_test_asset("TESTONESIDE"))
                .await
                .is_err()
        );
        arb_venues::live_book::remove_venue(venue);
    }

    #[test]
    fn a_new_verified_leverage_retires_the_older_one_of_the_same_coin_only() {
        let broker = test_broker(HyperliquidDex::Main, 0, true, Duration::from_millis(300));
        let key =
            |coin: &str, leverage: u32| (coin.to_string(), leverage, crate::MarginMode::Isolated);
        broker.mark_verified(key("BTC", 3));
        broker.mark_verified(key("ETH", 3));
        assert!(broker.verified_leverage.is_fresh(&key("BTC", 3)));
        // BTC 改成 5x 并读回确认：BTC 的 3x 不能再算「核对过」（20 秒内又要 3x 必须重新去查、去改），ETH 不受影响。
        broker.mark_verified(key("BTC", 5));
        assert!(broker.verified_leverage.is_fresh(&key("BTC", 5)));
        assert!(!broker.verified_leverage.is_fresh(&key("BTC", 3)));
        assert!(broker.verified_leverage.is_fresh(&key("ETH", 3)));
    }
}
