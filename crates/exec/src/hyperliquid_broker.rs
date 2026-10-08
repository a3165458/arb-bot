//! Production Hyperliquid **perpetual dex** broker for the first perp dex and the HIP-3
//! builder-deployed dexes `xyz` (`hyperliquid-xyz`) and `io` (`hyperliquid-io`); no spot
//! or vault routing. Each dex is a distinct venue with its own clearinghouse and margin,
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
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
// Shared across broker instances in this process, never reused within a millisecond.
static NONCE: AtomicU64 = AtomicU64::new(0);
/// 补保证金后回读：最多读这么多次，两次之间间隔 [`MARGIN_READBACK_INTERVAL`]。
const MARGIN_READBACK_POLLS: usize = 6;
const MARGIN_READBACK_INTERVAL: Duration = Duration::from_millis(500);
/// 回读到的逐仓账本增量 ≥ 所补金额 × 这个比例才算生效（留 1% 给资金费结算之类的小额扰动）。
const MARGIN_CONFIRM_RATIO: Decimal = Decimal::from_parts(99, 0, 0, false, 2);

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
        };
        broker.metadata().await?;
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

    async fn metadata(&self) -> ArbResult<Meta> {
        self.metadata_at(INFO_URL).await
    }

    async fn metadata_at(&self, info_url: &str) -> ArbResult<Meta> {
        let body = match self.dex.info_dex() {
            Some(dex) => json!({"type":"meta", "dex":dex}),
            None => json!({"type":"meta"}),
        };
        let meta: Meta = post(&self.client, info_url, &body).await?;
        if meta.universe.is_empty() {
            return Err(error("empty perpetual metadata"));
        }
        Ok(meta)
    }

    async fn asset(&self, symbol: &Symbol) -> ArbResult<(u32, Asset)> {
        self.asset_at(INFO_URL, symbol).await
    }

    async fn asset_at(&self, info_url: &str, symbol: &Symbol) -> ArbResult<(u32, Asset)> {
        let meta = self.metadata_at(info_url).await?;
        let mut found = None;
        for (index, asset) in meta.universe.into_iter().enumerate() {
            if symbol_of(self.dex, &asset.name)? == *symbol {
                if found.is_some() {
                    return Err(error("ambiguous perp market identity"));
                }
                let index = u32::try_from(index).map_err(|_| error("asset index overflow"))?;
                let id = self.dex.asset_id(self.perp_dex_index, index)?;
                found = Some((id, asset));
            }
        }
        found.ok_or_else(|| error(format!("unknown perp market {symbol}")))
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
        let query = json!({"type":"activeAssetData", "user":self.account, "coin":asset.name});
        let matches = |data: &Value| {
            data.get("coin").and_then(Value::as_str) == Some(asset.name.as_str())
                && data.pointer("/leverage/type").and_then(Value::as_str) == Some(mode.as_str())
                && data.pointer("/leverage/value").and_then(Value::as_u64)
                    == Some(u64::from(leverage))
        };
        let current: Value = post(&self.client, INFO_URL, &query).await?;
        if matches(&current) {
            return Ok(());
        }
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
            let readback: Value = post(&self.client, INFO_URL, &query).await?;
            if matches(&readback) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(error("保证金模式/杠杆读回未确认，没有下单"))
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
        let response: Value = post(&self.client, EXCHANGE_URL, &body).await?;
        if response.get("status").and_then(Value::as_str) != Some("ok") {
            // Never echo arbitrary server data that could contain a signed request.
            return Err(error(
                "exchange action rejected; query account/order state before retrying",
            ));
        }
        Ok(response)
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
        let response: Value = post(
            &self.client,
            INFO_URL,
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
                INFO_URL,
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
            let book: Value = post(
                &self.client,
                INFO_URL,
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
}

#[async_trait]
impl Broker for HyperliquidBroker {
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
        self.authorize()?;
        let _submission = self.submit.lock().await;
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
            return ack(&state);
        }
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
        if multiply(quantity, price)? < Decimal::TEN {
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
        if order.reduce_only {
            let position = self
                .positions()
                .await?
                .into_iter()
                .find(|p| p.symbol == order.symbol)
                .ok_or_else(|| error("reduce-only order has no matching position"))?;
            let closes = (position.net_quantity > Decimal::ZERO && order.side == Side::Sell)
                || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
            if !closes || quantity > position.net_quantity.abs() {
                return Err(error(
                    "reduce-only quantity/direction exceeds the current position",
                ));
            }
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
        if let Some(leverage) = leverage {
            self.configure_margin(asset_id, &asset, leverage, order.margin_mode)
                .await?;
        }
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
        let reply = self.exchange(&action).await?;
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
        if statuses[0].get("error").is_some() {
            return Err(error("order rejected by venue; reserved ID retained"));
        }
        // Ack only proves acceptance; require actual status + complete fills including fees.
        let remote = self
            .remote_status(json!(cloid))
            .await?
            .ok_or_else(|| error("accepted order not yet queryable; reserved ID retained"))?;
        let state = self
            .state_from_remote(remote, Some(&order.client_order_id))
            .await?;
        ack(&state)
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let external_prefix = format!("{}:oid:", self.dex.venue().as_str());
        let oid = match client_order_id.0.strip_prefix(&external_prefix) {
            Some(raw) => json!(
                raw.parse::<u64>()
                    .map_err(|_| error("invalid external order ID"))?
            ),
            None => json!(client_cloid(client_order_id)),
        };
        let Some(remote) = self.remote_status(oid).await? else {
            if self.reservation(&client_cloid(client_order_id))?.is_some() {
                return Err(error(
                    "reserved order no longer queryable; cannot assume it never filled",
                ));
            }
            return Ok(None);
        };
        self.state_from_remote(remote, Some(client_order_id))
            .await
            .map(Some)
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
            INFO_URL,
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
            INFO_URL,
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

    /// 本 dex 的账户价值减去已占用保证金（`clearinghouseState`）。HL 的 `withdrawable` 是能提走
    /// 的钱，和「能拿来开新仓」不是一回事，所以不用它。各 dex 的余额互相独立。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        let state: Value = post(
            &self.client,
            INFO_URL,
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
            INFO_URL,
            EXCHANGE_URL,
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
            INFO_URL,
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
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let mut rows = Vec::new();
        let mut start = since.timestamp_millis();
        for _ in 0..FUNDING_PAGES {
            let page: Vec<UserFunding> = post(
                &self.client,
                INFO_URL,
                &json!({"type":"userFunding", "user":self.account, "startTime":start}),
            )
            .await?;
            let full = page.len() >= FUNDING_PAGE;
            let last = page.iter().map(|row| row.time).max();
            rows.extend(funding_rows(self.dex, symbol, page)?);
            match last {
                Some(last) if full => start = last + 1,
                _ => return Ok(Some(FundingTotal::from_rows(rows, since))),
            }
        }
        Err(error(
            "funding history exceeds the page limit; total would be incomplete",
        ))
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let response: Value = post(
            &self.client,
            INFO_URL,
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

#[derive(Debug, Deserialize)]
struct UserFunding {
    /// 毫秒。
    time: i64,
    delta: FundingDelta,
}

#[derive(Debug, Deserialize)]
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
}
