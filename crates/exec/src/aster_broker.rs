//! Aster **Futures v3** production broker (USDT-margined linear perpetuals only).
//!
//! V1 API-key creation was disabled on 2026-03-25, so this broker implements the
//! V3 API-wallet (agent) authentication exclusively: every authenticated request
//! carries `user`, `signer`, `nonce` and a `signature` over the urlencoded parameter
//! string, signed with the operator's **signer** private key.
//!
//! Signature construction follows the official docs and demo exactly:
//! - parameter string: `urllib.parse.urlencode`-compatible (`quote_plus`) of
//!   `key=value&key=value…`, signed verbatim;
//! - EIP-712 typed data `Message(string msg)` in domain
//!   `{ name:"AsterSignTransaction", version:"1", chainId:1666, verifyingContract:0x0 }`;
//! - `keccak256("\x19\x01" ‖ domainSeparator ‖ hashStruct(Message))` then a
//!   recoverable secp256k1 ECDSA signature, hex-encoded as `r‖s‖v` (`v ∈ {27,28}`).
//!
//! The `chainId` is the fixed mainnet chain id **1666**. (The agent-registration
//! endpoint overrides the domain chain id with its `signatureChainId` request
//! parameter — that endpoint is not used here; trading endpoints use 1666.)
//!
//! Sources (primary):
//! - <https://github.com/asterdex/api-docs/blob/master/V3(Recommended)/EN/aster-finance-futures-api-v3.md>
//! - <https://github.com/asterdex/api-docs/blob/master/demo/aster-code.py>
//! - <https://github.com/asterdex/api-docs/blob/master/README.md>  (V3 required; deposit prerequisite)
//!
//! Every order is a **LIMIT IOC** order against a price we choose (a supplied
//! `limit_price`, or a fresh book bounded by [`LiveOptions::bound_price`]). Opening
//! orders require an explicit integer leverage and switch the symbol to **isolated**
//! margin; account-wide settings are never changed. Nonce is a strictly increasing
//! microsecond timestamp. The intent journal is fsynced before the order request;
//! a reserved client id is never resubmitted.
//!
//! # Operator configuration (required, never changed by this broker)
//! - The master account must be in **One-way** position mode (`dualSidePosition=false`).
//! - The master wallet must have completed at least one deposit; until then V3
//!   authenticated endpoints answer `{"code":-5050,...}`.
//! - The configured address must be an approved agent/API wallet for the account.
//!
//! `user`, `signer` and the signer private key are secrets: this type implements
//! neither `Debug` nor `Serialize`, and none of them are ever logged or placed in
//! an error string.

use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use k256::ecdsa::SigningKey;
use reqwest::{Client, Method};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sha3::Keccak256;
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, hex_lower, order_units, round_price, transport_error,
    venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE: &str = "https://fapi.asterdex.com";
const EXCHANGE_INFO: &str = "/fapi/v3/exchangeInfo";
const DEPTH: &str = "/fapi/v3/depth";
const COMMISSION_RATE: &str = "/fapi/v3/commissionRate";
const POSITION_MODE: &str = "/fapi/v3/positionSide/dual";
const POSITION_RISK: &str = "/fapi/v3/positionRisk";
const LEVERAGE_BRACKETS: &str = "/fapi/v3/leverageBrackets";
const LEVERAGE: &str = "/fapi/v3/leverage";
const MARGIN_TYPE: &str = "/fapi/v3/marginType";
const ORDER: &str = "/fapi/v3/order";
const OPEN_ORDERS: &str = "/fapi/v3/openOrders";
const USER_TRADES: &str = "/fapi/v3/userTrades";

/// Mainnet EIP-712 chain id used by the trading endpoints.
const CHAIN_ID: u64 = 1666;
const DOMAIN_NAME: &str = "AsterSignTransaction";
const DOMAIN_VERSION: &str = "1";
/// Aster's client order id charset/length limit `^[.A-Z:/a-z0-9_-]{1,36}$`.
const MAX_CLIENT_ID: usize = 36;
const CLIENT_ID_PREFIX: &str = "aster-";
/// Prefix for orders seen on the account that were not placed by this process.
const EXTERNAL_PREFIX: &str = "aster-external-";

/// Shared across broker instances in this process; strictly increasing microseconds.
static NONCE: AtomicU64 = AtomicU64::new(0);

/// Aster Futures v3 credentials. Secrets: no `Debug`, no `Serialize`.
pub struct AsterCredentials {
    /// Master account wallet address (`user`), sent verbatim.
    pub user: String,
    /// Approved agent/API wallet address (`signer`), sent verbatim.
    pub signer: String,
    /// Private key of `signer`, EIP-712 signer of every request.
    pub signer_private_key: String,
}

/// Aster Futures v3 broker.
pub struct AsterBroker {
    client: Client,
    signer_key: SigningKey,
    /// Verbatim signer address as supplied (case preserved for signing).
    signer: String,
    /// Verbatim master account address as supplied.
    user: String,
    options: LiveOptions,
    taker_fee: Decimal,
    clock_offset_us: i64,
    journal: Mutex<OrderJournal>,
    /// Serialises order placement and cancellation for this account.
    submit: Mutex<()>,
}

/// A parsed HTTP response: status plus already-decoded JSON body.
struct Reply {
    status: reqwest::StatusCode,
    body: Value,
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<Contract>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Contract {
    symbol: String,
    contract_type: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    margin_asset: String,
    #[serde(default)]
    filters: Vec<Filter>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Filter {
    filter_type: String,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    step_size: Option<String>,
    #[serde(default)]
    min_qty: Option<String>,
    #[serde(default)]
    notional: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Brackets {
    #[serde(default)]
    brackets: Vec<Bracket>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bracket {
    #[serde(default)]
    initial_leverage: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRow {
    client_order_id: String,
    order_id: i64,
    symbol: String,
    side: String,
    status: String,
    orig_qty: String,
    executed_qty: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    reduce_only: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: String,
    position_amt: String,
    #[serde(default)]
    entry_price: String,
    position_side: String,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    notional: Option<String>,
    #[serde(default)]
    margin_type: Option<String>,
    #[serde(default)]
    liquidation_price: Option<String>,
    #[serde(default)]
    isolated_wallet: Option<String>,
    #[serde(default)]
    leverage: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UserTrade {
    qty: String,
    price: String,
    #[serde(default)]
    quote_qty: Option<String>,
    commission: String,
    commission_asset: String,
}

/// Resolved exchange spec for one instrument (base-quantity venue: face value 1).
struct Instrument {
    native: String,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
}

impl AsterBroker {
    /// Opens the journal, verifies the credentials with a read-only authenticated
    /// request, confirms One-way position mode, and reads the account taker fee.
    /// Construction sends **no** mutating request.
    pub async fn connect(
        client: Client,
        credentials: AsterCredentials,
        journal_path: &std::path::Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(Venue::Aster)?;

        let mut key_bytes = decode_hex(&credentials.signer_private_key, 32)?;
        let parsed = SigningKey::from_slice(&key_bytes).map_err(|_| err("签名私钥无效"));
        key_bytes.fill(0);
        let signer_key = parsed?;
        decode_hex(&credentials.user, 20)?;
        let signer_bytes = decode_hex(&credentials.signer, 20)?;
        if hex_lower(&signer_bytes) != signer_address(&signer_key) {
            return Err(err("signer address does not match signing key"));
        }

        let identity = format!(
            "aster:{}",
            &hex_lower(&Sha256::digest(
                credentials.user.to_ascii_lowercase().as_bytes()
            ))[..16]
        );
        let journal = OrderJournal::open(journal_path, &identity)?;

        let mut broker = Self {
            client,
            signer_key,
            signer: credentials.signer,
            user: credentials.user,
            options,
            taker_fee: Decimal::ZERO,
            clock_offset_us: 0,
            journal: Mutex::new(journal),
            submit: Mutex::new(()),
        };
        let started = Instant::now();
        let before = chrono::Utc::now().timestamp_micros();
        let time = broker.public_get("/fapi/v3/time", &[]).await?;
        let server_ms = time
            .get("serverTime")
            .and_then(Value::as_i64)
            .ok_or_else(|| err("missing server time"))?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err("server clock synchronization took too long"));
        }
        let midpoint = before + (chrono::Utc::now().timestamp_micros() - before) / 2;
        broker.clock_offset_us = server_ms
            .checked_mul(1000)
            .and_then(|v| v.checked_sub(midpoint))
            .ok_or_else(|| err("invalid server time"))?;

        // Read-only authenticated probe: also yields the position mode.
        let mode = broker
            .authed_value(Method::GET, POSITION_MODE, Vec::new())
            .await?;
        if mode.get("dualSidePosition").and_then(Value::as_bool) != Some(false) {
            return Err(err(
                "账户不是单向持仓模式；请在交易所改为 One-way 后重试（本券商不会修改账户级设置）",
            ));
        }
        broker.check_account_mode().await?;

        let reference = broker.reference_symbol().await?;
        let body = broker
            .authed_value(
                Method::GET,
                COMMISSION_RATE,
                vec![param("symbol", reference)],
            )
            .await?;
        let taker = body
            .get("takerCommissionRate")
            .and_then(Value::as_str)
            .ok_or_else(|| err("交易所未返回吃单费率"))?;
        let taker = dec(taker)?;
        if taker < Decimal::ZERO || taker >= Decimal::ONE {
            return Err(err("账户吃单费率超出合理范围"));
        }
        broker.taker_fee = taker;
        Ok(broker)
    }

    async fn check_account_mode(&self) -> ArbResult<()> {
        let position = self
            .authed_value(Method::GET, POSITION_MODE, Vec::new())
            .await?;
        let margin = self
            .authed_value(Method::GET, "/fapi/v3/multiAssetsMargin", Vec::new())
            .await?;
        if position.get("dualSidePosition").and_then(Value::as_bool) != Some(false)
            || margin.get("multiAssetsMargin").and_then(Value::as_bool) != Some(false)
        {
            return Err(err(
                "configure One-way positions and Single-Asset margin in the Aster UI; account settings are never changed by this broker",
            ));
        }
        Ok(())
    }

    fn authorize(&self) -> ArbResult<()> {
        self.options.authorize(Venue::Aster)
    }

    /// Public GET (no authentication): exchange info, depth, etc.
    async fn public_get(&self, path: &str, params: &[(&str, &str)]) -> ArbResult<Value> {
        let mut request = self.client.get(format!("{BASE}{path}"));
        if !params.is_empty() {
            request = request.query(params);
        }
        let response = request
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|error| transport_error(Venue::Aster, path, error))?;
        read_reply(path, response).await
    }

    /// Authenticated request returning the raw reply (HTTP status + body). Transport
    /// failures and 429/5xx are hard errors; business codes are left to the caller.
    async fn authed_reply(
        &self,
        method: Method,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> ArbResult<Reply> {
        if method != Method::GET {
            self.authorize()?;
        }
        params.push(param(
            "nonce",
            next_nonce(self.clock_offset_us)?.to_string(),
        ));
        params.push(param("signer", self.signer.clone()));
        params.push(param("user", self.user.clone()));
        let param_string = sorted_param_string(&mut params);
        let signature = sign_message(&self.signer_key, &param_string)?;
        let signed = format!("{param_string}&signature={signature}");
        let request = if method == Method::GET {
            self.client
                .request(method, format!("{BASE}{path}?{signed}"))
        } else {
            self.client
                .request(method, format!("{BASE}{path}"))
                .body(signed)
        };
        let response = request
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|error| transport_error(Venue::Aster, path, error))?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(err(format!(
                "{path} 服务不可用（HTTP {}）",
                status.as_u16()
            )));
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| err(format!("{path} 响应无法解析")))?;
        if !status.is_success()
            && body
                .get("code")
                .and_then(Value::as_i64)
                .is_none_or(|c| c >= 0)
        {
            return Err(err(format!("{path} HTTP {}", status.as_u16())));
        }
        Ok(Reply { status, body })
    }

    /// Authenticated request that must succeed and carry no business error.
    async fn authed_value(
        &self,
        method: Method,
        path: &str,
        params: Vec<(String, String)>,
    ) -> ArbResult<Value> {
        let reply = self.authed_reply(method, path, params).await?;
        if let Some((code, message)) = business_failure(&reply) {
            return Err(business_error(path, code, &message));
        }
        Ok(reply.body)
    }

    /// A live USDT perpetual symbol to query account-wide data against.
    async fn reference_symbol(&self) -> ArbResult<String> {
        let info: ExchangeInfo = decode(self.public_get(EXCHANGE_INFO, &[]).await?)?;
        let mut candidates: Vec<String> = info
            .symbols
            .iter()
            .filter(|c| {
                c.contract_type == "PERPETUAL"
                    && c.status == "TRADING"
                    && c.margin_asset == "USDT"
                    && c.quote_asset == "USDT"
            })
            .map(|c| c.symbol.clone())
            .collect();
        if candidates.iter().any(|s| s == "BTCUSDT") {
            return Ok("BTCUSDT".to_string());
        }
        candidates.sort();
        candidates
            .into_iter()
            .next()
            .ok_or_else(|| err("交易所没有可交易的 USDT 永续合约"))
    }

    /// Fresh exchange spec for the requested internal symbol, matched against live
    /// metadata (never string guessing alone).
    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Instrument> {
        let info: ExchangeInfo = decode(self.public_get(EXCHANGE_INFO, &[]).await?)?;
        let base = symbol.base.to_uppercase();
        let quote = symbol.quote.to_uppercase();
        let native = format!("{base}{quote}");
        let contract = info
            .symbols
            .into_iter()
            .find(|c| {
                c.symbol == native
                    && c.contract_type == "PERPETUAL"
                    && c.status == "TRADING"
                    && c.margin_asset == "USDT"
                    && c.quote_asset == quote
                    && c.base_asset == base
            })
            .ok_or_else(|| err(format!("未找到在交易的 USDT 永续合约 {native}")))?;
        let tick = filter_decimal(&contract.filters, "PRICE_FILTER", "tickSize")?;
        let step = filter_decimal(&contract.filters, "LOT_SIZE", "stepSize")?;
        let min_qty = filter_decimal(&contract.filters, "LOT_SIZE", "minQty")?;
        let min_notional = filter_decimal(&contract.filters, "MIN_NOTIONAL", "notional")?;
        if tick <= Decimal::ZERO || step <= Decimal::ZERO || min_qty < Decimal::ZERO {
            return Err(err(format!("{native} 的过滤器非法")));
        }
        Ok(Instrument {
            native,
            tick,
            step,
            min_qty,
            min_notional,
        })
    }

    /// Price for the order: the supplied limit, or a bound from a fresh book.
    async fn order_price(&self, native: &str, order: &NewOrder) -> ArbResult<Decimal> {
        if let Some(limit) = order.limit_price {
            return Ok(limit);
        }
        let started = Instant::now();
        let body = self
            .public_get(DEPTH, &[("symbol", native), ("limit", "5")])
            .await?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err("stale order book: round trip exceeds 2 seconds"));
        }
        if let Some(timestamp) = body
            .get("T")
            .or_else(|| body.get("E"))
            .and_then(Value::as_i64)
        {
            let now = (chrono::Utc::now().timestamp_micros() + self.clock_offset_us) / 1000;
            if timestamp > now + 1000 || now - timestamp > 5000 {
                return Err(err("stale order book timestamp"));
            }
        }
        let bid = side_level(&body, "bids")?;
        let ask = side_level(&body, "asks")?;
        if bid.is_none() || ask.is_none() {
            return Err(err("order book must contain both sides"));
        }
        self.options.bound_price(Venue::Aster, order.side, bid, ask)
    }

    /// Maximum initial leverage declared for the symbol's tightest bracket.
    async fn max_leverage(&self, native: &str) -> ArbResult<u32> {
        let body = self
            .authed_value(
                Method::GET,
                LEVERAGE_BRACKETS,
                vec![param("symbol", native)],
            )
            .await?;
        let brackets: Brackets = decode(body)?;
        brackets
            .brackets
            .iter()
            .map(|b| b.initial_leverage)
            .max()
            .ok_or_else(|| err("交易所未返回杠杆档位"))
    }

    /// Switch the symbol to isolated margin and set the leverage, then read both
    /// back. Only per-symbol settings are touched.
    async fn configure_open(
        &self,
        native: &str,
        leverage: u32,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let reply = self
            .authed_reply(
                Method::POST,
                MARGIN_TYPE,
                vec![
                    param("symbol", native),
                    param(
                        "marginType",
                        if mode.is_cross() {
                            "CROSSED"
                        } else {
                            "ISOLATED"
                        },
                    ),
                ],
            )
            .await?;
        if let Some((code, message)) = business_failure(&reply) {
            let tolerated = code == -4046;
            if !tolerated {
                return Err(business_error(MARGIN_TYPE, code, &message));
            }
        }

        let reply = self
            .authed_reply(
                Method::POST,
                LEVERAGE,
                vec![
                    param("symbol", native),
                    param("leverage", leverage.to_string()),
                ],
            )
            .await?;
        if let Some((code, message)) = business_failure(&reply) {
            return Err(business_error(LEVERAGE, code, &message));
        }

        let rows: Vec<PositionRow> = decode(
            self.authed_value(Method::GET, POSITION_RISK, vec![param("symbol", native)])
                .await?,
        )?;
        let row = rows
            .into_iter()
            .find(|r| r.symbol == native)
            .ok_or_else(|| err("读取保证金设置失败"))?;
        let matches_mode = row
            .margin_type
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case(mode.as_str()));
        if !matches_mode {
            return Err(err("保证金模式与所选模式不符，拒绝下单"));
        }
        let current = row
            .leverage
            .as_deref()
            .map(dec)
            .transpose()?
            .ok_or_else(|| err("读取杠杆失败"))?;
        if current != Decimal::from(leverage) {
            return Err(err("杠杆设置未生效，拒绝下单"));
        }
        Ok(())
    }

    /// Query the venue for one order by its venue client id.
    async fn lookup_native(&self, native: &str, client_id: &str) -> ArbResult<Option<OrderState>> {
        let reply = self
            .authed_reply(
                Method::GET,
                ORDER,
                vec![
                    param("symbol", native),
                    param("origClientOrderId", client_id),
                ],
            )
            .await?;
        if let Some((code, message)) = business_failure(&reply) {
            if code == -2013 {
                return Ok(None);
            }
            return Err(business_error(ORDER, code, &message));
        }
        let row: OrderRow = decode(reply.body)?;
        self.state_from_row(&row, native).await.map(Some)
    }

    async fn query_order_by_id(&self, native: &str, order_id: i64) -> ArbResult<Option<OrderRow>> {
        let reply = self
            .authed_reply(
                Method::GET,
                ORDER,
                vec![
                    param("symbol", native),
                    param("orderId", order_id.to_string()),
                ],
            )
            .await?;
        if let Some((code, message)) = business_failure(&reply) {
            if code == -2013 {
                return Ok(None);
            }
            return Err(business_error(ORDER, code, &message));
        }
        decode(reply.body).map(Some)
    }

    async fn fetch_open(&self, symbol: Option<&str>) -> ArbResult<Vec<OrderRow>> {
        let params = symbol.map(|s| vec![param("symbol", s)]).unwrap_or_default();
        let body = self.authed_value(Method::GET, OPEN_ORDERS, params).await?;
        decode(body)
    }

    async fn user_trades(&self, native: &str, order_id: i64) -> ArbResult<Vec<UserTrade>> {
        let body = self
            .authed_value(
                Method::GET,
                USER_TRADES,
                vec![
                    param("symbol", native),
                    param("orderId", order_id.to_string()),
                    param("limit", "1000"),
                ],
            )
            .await?;
        let rows: Vec<UserTrade> = decode(body)?;
        if rows.len() == 1000 {
            return Err(err("成交记录可能被截断，手续费不可信"));
        }
        Ok(rows)
    }

    /// Build the full order state from a venue order row plus its actual trades.
    async fn state_from_row(&self, row: &OrderRow, native: &str) -> ArbResult<OrderState> {
        let symbol = symbol_of(native)?;
        let side = parse_side(&row.side)?;
        let orig_qty = dec(&row.orig_qty)?;
        let executed = dec(&row.executed_qty)?;
        let price = dec(&row.price)?;
        if row.symbol != native || row.order_id <= 0 || orig_qty < Decimal::ZERO {
            return Err(err("order identity or original quantity is invalid"));
        }

        let entry = {
            let journal = self.journal.lock().await;
            journal.by_venue_client_id(&row.client_order_id).cloned()
        };
        let client_order_id = match &entry {
            Some(entry) => entry.order.client_order_id.clone(),
            None => ClientOrderId(format!("{EXTERNAL_PREFIX}{}", row.order_id)),
        };
        let order = match entry {
            Some(entry) => {
                if entry.order.symbol != symbol
                    || entry.order.side != side
                    || entry.order.reduce_only != row.reduce_only
                    || entry.units != orig_qty
                {
                    return Err(err("交易所订单与本地意图不一致"));
                }
                entry.order
            }
            None => NewOrder {
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: client_order_id.clone(),
                venue: Venue::Aster,
                symbol,
                side,
                notional_usdt: if price > Decimal::ZERO {
                    multiply(orig_qty, price)?
                } else {
                    Decimal::ZERO
                },
                quantity: Some(orig_qty),
                limit_price: (price > Decimal::ZERO).then_some(price),
                reduce_only: row.reduce_only,
                leverage: None,
            },
        };

        let (quantity, notional, fee) = if executed > Decimal::ZERO {
            let trades = self.user_trades(native, row.order_id).await?;
            aggregate_fills(&trades)?
        } else {
            (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
        };
        if quantity != executed {
            return Err(err("成交记录与订单成交量不一致，手续费不可信"));
        }
        let status = verify_status(map_status(&row.status)?, orig_qty, executed)?;

        let mut state = OrderState::new(order);
        state.venue_order_id = Some(row.order_id.to_string());
        state.status = status;
        state.filled_usdt = notional;
        state.average_price = (quantity > Decimal::ZERO)
            .then(|| divide(notional, quantity))
            .transpose()?;
        state.fee_usdt = fee;
        if status == OrderStatus::Rejected {
            state.reject_reason = Some(row.status.clone());
        }
        Ok(state)
    }
}

#[async_trait]
impl Broker for AsterBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let native = self.instrument(symbol).await?.native;
        let rows: Vec<PositionRow> = decode(
            self.authed_value(Method::GET, POSITION_RISK, vec![param("symbol", &native)])
                .await?,
        )?;
        let Some(row) = rows.iter().find(|row| {
            row.symbol == native
                && row.position_side == "BOTH"
                && dec(&row.position_amt).is_ok_and(|q| q != Decimal::ZERO)
        }) else {
            return Ok(None);
        };
        let parse = |value: &Option<String>| value.as_deref().and_then(|raw| dec(raw).ok());
        Ok(Some(crate::margin::venue_state(
            row.margin_type
                .as_deref()
                .and_then(crate::margin::reported_mode),
            parse(&row.liquidation_price),
            parse(&row.isolated_wallet),
        )))
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
        self.check_account_mode().await?;
        let instrument = self.instrument(symbol).await?;
        let leverage = crate::margin::leverage(Venue::Aster, leverage)?
            .to_u32()
            .ok_or_else(|| err("invalid leverage"))?;
        if leverage > self.max_leverage(&instrument.native).await? {
            return Err(err("leverage exceeds market limit"));
        }
        self.configure_open(&instrument.native, leverage, mode)
            .await
    }

    fn venue(&self) -> Venue {
        Venue::Aster
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        if order.venue != Venue::Aster || order.client_order_id.0.is_empty() {
            return Err(err("场所不匹配或客户订单号为空"));
        }
        let client_id = venue_client_id(CLIENT_ID_PREFIX, &order.client_order_id, MAX_CLIENT_ID);

        // Idempotency: a reserved client id is never resubmitted.
        let prior = {
            let journal = self.journal.lock().await;
            if journal
                .get(&order.client_order_id)
                .is_some_and(|entry| entry.order != *order)
            {
                return Err(err("client id reused with different intent/margin mode"));
            }
            match journal.get(&order.client_order_id) {
                None => None,
                Some(entry) => match &entry.terminal {
                    Some(terminal) => Some(Prior::Terminal(Box::new(terminal.clone()))),
                    None => Some(Prior::Pending(
                        entry.instrument.clone(),
                        entry.venue_client_id.clone(),
                    )),
                },
            }
        };
        match prior {
            Some(Prior::Terminal(state)) => return ack(&state),
            Some(Prior::Pending(native, reserved_id)) => {
                return match self.lookup_native(&native, &reserved_id).await? {
                    Some(state) => {
                        if !state.status.is_live() {
                            self.journal.lock().await.record_terminal(&state)?;
                        }
                        ack(&state)
                    }
                    None => Err(err("已预约的订单号在交易所查不到；不得重发，请人工对账")),
                };
            }
            None => {}
        }

        self.check_account_mode().await?;
        let instrument = self.instrument(&order.symbol).await?;
        if !order.reduce_only {
            let fee = self
                .authed_value(
                    Method::GET,
                    COMMISSION_RATE,
                    vec![param("symbol", instrument.native.clone())],
                )
                .await?;
            let actual = fee
                .get("takerCommissionRate")
                .and_then(Value::as_str)
                .ok_or_else(|| err("missing instrument taker fee"))?;
            if dec(actual)? != self.taker_fee {
                return Err(err(
                    "instrument taker fee differs from the connected account rate; refusing to underestimate costs",
                ));
            }
        }
        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| err("开仓订单必须给出逐仓杠杆"))?;
            if !value.fract().is_zero() || value < Decimal::ONE {
                return Err(err("杠杆必须是不小于 1 的整数"));
            }
            let leverage = value.to_u32().ok_or_else(|| err("杠杆无效"))?;
            let max = self.max_leverage(&instrument.native).await?;
            if leverage > max {
                return Err(err(format!("杠杆 {leverage} 超过该合约上限 {max}")));
            }
            Some(leverage)
        };

        if let Some(leverage) = leverage {
            self.configure_open(&instrument.native, leverage, order.margin_mode)
                .await?;
        }
        let position = if order.reduce_only {
            Some(
                self.positions()
                    .await?
                    .into_iter()
                    .find(|p| p.symbol == order.symbol)
                    .ok_or_else(|| err("reduce-only order has no matching position"))?,
            )
        } else {
            None
        };
        let raw_price = self.order_price(&instrument.native, order).await?;
        let price = round_price(raw_price, instrument.tick, order.side)
            .ok_or_else(|| err("价格取整后非正"))?;
        let units = order_units(
            Venue::Aster,
            order,
            price,
            Decimal::ONE,
            instrument.step,
            instrument.min_qty,
        )?;
        let notional = multiply(units, price)?;
        if notional < instrument.min_notional {
            return Err(err(format!(
                "订单名义额低于交易所最小限额 {}",
                instrument.min_notional
            )));
        }

        if let Some(position) = position {
            let closes = (position.net_quantity > Decimal::ZERO && order.side == Side::Sell)
                || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
            if !closes || units > position.net_quantity.abs() {
                return Err(err(
                    "reduce-only direction or quantity exceeds the position",
                ));
            }
        }
        // Durable intent before the order write; no further network reads before signing.
        self.journal.lock().await.reserve(JournalEntry {
            order: order.clone(),
            venue_client_id: client_id.clone(),
            instrument: instrument.native.clone(),
            units,
            terminal: None,
        })?;

        let mut params = vec![
            param("symbol", instrument.native.clone()),
            param("side", side_str(order.side)),
            param("type", "LIMIT"),
            param("timeInForce", "IOC"),
            param("quantity", units.normalize().to_string()),
            param("price", price.normalize().to_string()),
            param("newClientOrderId", client_id.clone()),
            param("newOrderRespType", "RESULT"),
        ];
        if order.reduce_only {
            params.push(param("reduceOnly", "true"));
        }

        // Always query after a transport failure, HTTP error or rejection. An
        // absent reserved ID remains uncertain: never fabricate a rejection.
        let outcome = self.authed_reply(Method::POST, ORDER, params).await;

        match self.lookup_native(&instrument.native, &client_id).await? {
            Some(state) => {
                if !state.status.is_live() {
                    self.journal.lock().await.record_terminal(&state)?;
                }
                ack(&state)
            }
            None => match outcome {
                Err(error) => Err(error),
                Ok(reply) => match business_failure(&reply) {
                    // 4xx + business code: the venue refused the order and it is absent, so
                    // the refusal is final. Record it so `order_state` reports Rejected
                    // instead of an unresolved intent forever.
                    Some((code, message)) if definitive_rejection(reply.status, code) => {
                        let mut rejected = OrderState::new(order.clone());
                        rejected.status = OrderStatus::Rejected;
                        rejected.reject_reason = Some(format!("Aster code {code}"));
                        self.journal.lock().await.record_terminal(&rejected)?;
                        Err(business_error(ORDER, code, &message))
                    }
                    _ => Err(err("订单提交后查不到；已保留订单号，不得重发")),
                },
            },
        }
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let (native, reserved_id) = {
            let journal = self.journal.lock().await;
            match journal.get(client_order_id) {
                Some(entry) => match &entry.terminal {
                    Some(terminal) => return Ok(Some(terminal.clone())),
                    None => (entry.instrument.clone(), entry.venue_client_id.clone()),
                },
                None => {
                    if client_order_id.0.starts_with(EXTERNAL_PREFIX) {
                        return Err(err("外部订单号缺少合约，无法直接查询"));
                    }
                    return Ok(None);
                }
            }
        };
        match self.lookup_native(&native, &reserved_id).await? {
            Some(state) => {
                if !state.status.is_live() {
                    self.journal.lock().await.record_terminal(&state)?;
                }
                Ok(Some(state))
            }
            None => Err(err(
                "已预约的订单号在交易所查不到；不能当作从未提交，请人工对账",
            )),
        }
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        let order_id = venue_order_id
            .parse::<i64>()
            .map_err(|_| err("交易所订单号无效"))?;

        // Cancel needs the symbol; find the live order.
        let Some(row) = self
            .fetch_open(None)
            .await?
            .into_iter()
            .find(|r| r.order_id == order_id)
        else {
            return Ok(()); // Already gone/terminal: cancelling is idempotent.
        };
        if !map_status(&row.status)?.is_live() {
            return Ok(());
        }

        let reply = self
            .authed_reply(
                Method::DELETE,
                ORDER,
                vec![
                    param("symbol", row.symbol.clone()),
                    param("orderId", order_id.to_string()),
                ],
            )
            .await?;
        if let Some((code, message)) = business_failure(&reply)
            && code != -2011
        {
            return Err(business_error(ORDER, code, &message));
        }

        if let Some(after) = self.query_order_by_id(&row.symbol, order_id).await?
            && map_status(&after.status)?.is_live()
        {
            return Err(err("撤单后订单仍然活跃"));
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let rows = self.fetch_open(None).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let state = self.state_from_row(&row, &row.symbol).await?;
            if state.status.is_live() {
                result.push(state);
            }
        }
        Ok(result)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let rows: Vec<PositionRow> = decode(
            self.authed_value(Method::GET, POSITION_RISK, Vec::new())
                .await?,
        )?;
        let mut positions = Vec::new();
        for row in rows {
            if row.position_side != "BOTH" {
                return Err(err("账户存在双向持仓，本券商要求单向持仓"));
            }
            let amount = dec(&row.position_amt)?;
            if amount == Decimal::ZERO {
                continue;
            }
            let mark = row.mark_price.as_deref().map(dec).transpose()?;
            let notional = match row.notional.as_deref().map(dec).transpose()? {
                Some(value) => value.abs(),
                None => mark.map(|m| m * amount.abs()).unwrap_or(Decimal::ZERO),
            };
            let average_price = row
                .entry_price
                .parse::<Decimal>()
                .ok()
                .filter(|price| *price > Decimal::ZERO);
            positions.push(VenuePosition {
                venue: Venue::Aster,
                symbol: symbol_of(&row.symbol)?,
                net_quantity: amount,
                average_price,
                notional_usdt: notional,
            });
        }
        Ok(positions)
    }
}

enum Prior {
    Terminal(Box<OrderState>),
    Pending(String, String),
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| err("缺少交易所订单号"))?,
        status: state.status,
    })
}

fn err(message: impl Into<String>) -> ArbError {
    ArbError::venue(Venue::Aster.as_str(), message)
}

fn param(key: &str, value: impl Into<String>) -> (String, String) {
    (key.to_string(), value.into())
}

/// A 4xx reply carrying a business code is a final refusal, except the Binance-family codes
/// whose documented meaning is "execution status unknown" (-1001 disconnected, -1006
/// unexpected response, -1007 timeout): those may still have landed an order.
fn definitive_rejection(status: reqwest::StatusCode, code: i64) -> bool {
    status.is_client_error() && code < 0 && !matches!(code, -1001 | -1006 | -1007)
}

fn business_failure(reply: &Reply) -> Option<(i64, String)> {
    let code = reply.body.get("code").and_then(Value::as_i64)?;
    if code == 200 || code == 0 {
        return None;
    }
    let message = reply
        .body
        .get("msg")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some((code, message))
}

fn business_error(path: &str, code: i64, _message: &str) -> ArbError {
    let hint = match code {
        -5050 => "（主钱包未充值，V3 交易接口不可用）",
        -1022 => "（签名无效）",
        -2015 => "（API key / IP / 权限被拒）",
        _ => "",
    };
    err(format!("{path} business error code={code}{hint}"))
}

async fn read_reply(path: &str, response: reqwest::Response) -> ArbResult<Value> {
    let status = response.status();
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(err(format!(
            "{path} 服务不可用（HTTP {}）",
            status.as_u16()
        )));
    }
    let body: Value = response
        .json()
        .await
        .map_err(|_| err(format!("{path} 响应无法解析")))?;
    if let Some((code, message)) = business_failure(&Reply {
        status,
        body: body.clone(),
    }) {
        return Err(business_error(path, code, &message));
    }
    if !status.is_success() {
        return Err(err(format!("{path} HTTP {}", status.as_u16())));
    }
    Ok(body)
}

fn decode<T: DeserializeOwned>(value: Value) -> ArbResult<T> {
    serde_json::from_value(value).map_err(|_| err("交易所响应字段缺失或类型不符"))
}

fn dec(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw).map_err(|_| err("无法解析交易所数字"))
}

fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b).ok_or_else(|| err("十进制乘法溢出"))
}

fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b).ok_or_else(|| err("十进制除法溢出或除零"))
}

fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b).ok_or_else(|| err("十进制加法溢出"))
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn parse_side(raw: &str) -> ArbResult<Side> {
    match raw {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(err("未知的订单方向")),
    }
}

/// Native Aster symbol (`BTCUSDT`, `1000PEPEUSDT`) to internal [`Symbol`].
fn symbol_of(native: &str) -> ArbResult<Symbol> {
    let base = native
        .strip_suffix("USDT")
        .filter(|base| !base.is_empty())
        .ok_or_else(|| err("不是 USDT 永续合约"))?;
    Ok(Symbol::perp(base, "USDT"))
}

fn map_status(status: &str) -> ArbResult<OrderStatus> {
    match status {
        "NEW" | "PARTIALLY_FILLED" => Ok(OrderStatus::Open),
        "FILLED" => Ok(OrderStatus::Filled),
        "CANCELED" | "EXPIRED" => Ok(OrderStatus::Cancelled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        _ => Err(err("未知的交易所订单状态，拒绝推断终态")),
    }
}

/// A terminal IOC match need not be a full fill of the effective quantity.
fn verify_status(
    status: OrderStatus,
    original: Decimal,
    executed: Decimal,
) -> ArbResult<OrderStatus> {
    if executed < Decimal::ZERO || executed > original {
        return Err(err("订单成交量非法"));
    }
    match status {
        OrderStatus::Filled if executed < original => Ok(OrderStatus::Cancelled),
        OrderStatus::Rejected if executed > Decimal::ZERO => Ok(OrderStatus::Cancelled),
        _ => Ok(status),
    }
}

/// Sum fills: base quantity, quote notional, and signed fee (positive = cost).
fn aggregate_fills(rows: &[UserTrade]) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let mut quantity = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for row in rows {
        if row.commission_asset != "USDT" {
            return Err(err("手续费币种不是 USDT，无法折算"));
        }
        let qty = dec(&row.qty)?;
        let price = dec(&row.price)?;
        if qty <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(err("fill quantity and price must be positive"));
        }
        let quote = multiply(qty, price)?;
        if let Some(raw) = row.quote_qty.as_deref()
            && dec(raw)? != quote
        {
            return Err(err("fill quote amount disagrees with quantity times price"));
        }
        quantity = add(quantity, qty)?;
        notional = add(notional, quote)?;
        fee = add(fee, dec(&row.commission)?)?;
    }
    Ok((quantity, notional, fee))
}

fn filter_decimal(filters: &[Filter], filter_type: &str, field: &str) -> ArbResult<Decimal> {
    filter_decimal_opt(filters, filter_type, field)
        .ok_or_else(|| err(format!("缺少交易所过滤器 {filter_type}.{field}")))
}

fn filter_decimal_opt(filters: &[Filter], filter_type: &str, field: &str) -> Option<Decimal> {
    filters
        .iter()
        .find(|filter| filter.filter_type == filter_type)
        .and_then(|filter| match field {
            "tickSize" => filter.tick_size.as_deref(),
            "stepSize" => filter.step_size.as_deref(),
            "minQty" => filter.min_qty.as_deref(),
            "notional" => filter.notional.as_deref(),
            _ => None,
        })
        .and_then(|raw| Decimal::from_str(raw).ok())
}

/// Top-of-book price on one side, or `None` when the side is absent/empty.
fn side_level(body: &Value, side: &str) -> ArbResult<Option<Decimal>> {
    let raw = body
        .get(side)
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(|level| level.get(0))
        .and_then(Value::as_str);
    match raw {
        Some(raw) => dec(raw).map(Some),
        None => Ok(None),
    }
}

/// Strictly increasing microsecond nonce, process-wide.
fn next_nonce(offset_us: i64) -> ArbResult<u64> {
    let now: u64 = chrono::Utc::now()
        .timestamp_micros()
        .checked_add(offset_us)
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| err("invalid adjusted clock"))?;
    loop {
        let last = NONCE.load(Ordering::Relaxed);
        let next = now.max(last.checked_add(1).ok_or_else(|| err("nonce overflow"))?);
        if next.saturating_sub(now) > 50_000_000 {
            return Err(err(
                "nonce clock drift exceeds safe window; reconnect to synchronize",
            ));
        }
        if NONCE
            .compare_exchange_weak(last, next, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Ok(next);
        }
    }
}

/// URL-encode one component the way Python's `quote_plus` does.
fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(byte as char);
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(
                    char::from_digit(u32::from(byte >> 4), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit(u32::from(byte & 15), 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

/// Sort parameters by key (ASCII), then urlencode — deterministic and stable under
/// either a server that verifies the raw string or one that sorts first.
fn sorted_param_string(params: &mut [(String, String)]) -> String {
    params.sort_by(|a, b| a.0.cmp(&b.0));
    params
        .iter()
        .map(|(key, value)| format!("{}={}", encode_component(key), encode_component(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn keccak(bytes: &[u8]) -> [u8; 32] {
    Keccak256::digest(bytes).into()
}

/// EIP-712 digest for the V3 `Message(string msg)` payload.
fn eip712_digest(message: &str) -> [u8; 32] {
    let mut domain = [0u8; 160];
    domain[..32].copy_from_slice(&keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain[32..64].copy_from_slice(&keccak(DOMAIN_NAME.as_bytes()));
    domain[64..96].copy_from_slice(&keccak(DOMAIN_VERSION.as_bytes()));
    domain[120..128].copy_from_slice(&CHAIN_ID.to_be_bytes());
    let mut structure = [0u8; 64];
    structure[..32].copy_from_slice(&keccak(b"Message(string msg)"));
    structure[32..].copy_from_slice(&keccak(message.as_bytes()));
    let mut payload = [0u8; 66];
    payload[..2].copy_from_slice(&[0x19, 0x01]);
    payload[2..34].copy_from_slice(&keccak(&domain));
    payload[34..].copy_from_slice(&keccak(&structure));
    keccak(&payload)
}

/// 65-byte `r‖s‖v` hex signature (`v ∈ {27,28}`) as the Aster demo produces.
fn sign_message(key: &SigningKey, message: &str) -> ArbResult<String> {
    let digest = eip712_digest(message);
    let (signature, recovery) = key
        .sign_prehash_recoverable(&digest)
        .map_err(|_| err("secp256k1 签名失败"))?;
    if recovery.to_byte() > 1 {
        return Err(err("不支持的以太坊恢复号"));
    }
    let mut bytes = [0u8; 65];
    bytes[..64].copy_from_slice(&signature.to_bytes());
    bytes[64] = recovery.to_byte() + 27;
    Ok(hex_lower(&bytes))
}

/// Lowercase hex address derived from a signing key.
fn signer_address(key: &SigningKey) -> String {
    let public = key.verifying_key().to_encoded_point(false);
    hex_lower(&keccak(&public.as_bytes()[1..])[12..])
}

fn decode_hex(value: &str, expected_len: usize) -> ArbResult<Vec<u8>> {
    let raw = value.strip_prefix("0x").unwrap_or(value).as_bytes();
    if raw.len() != expected_len * 2 {
        return Err(err("十六进制长度不正确"));
    }
    fn digit(byte: u8) -> ArbResult<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err(err("十六进制字符无效")),
        }
    }
    let mut out = Vec::with_capacity(expected_len);
    for pair in raw.as_chunks::<2>().0 {
        out.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // Official Aster docs example key: signer address must equal the docs' value.
    const EXAMPLE_KEY: &str = "0x4fd0a42218f3eae43a6ce26d22544e986139a01e5b34a62db53757ffca81bae1";

    fn example_key() -> SigningKey {
        let bytes = decode_hex(EXAMPLE_KEY, 32).unwrap();
        SigningKey::from_slice(&bytes).unwrap()
    }

    /// Derived from the official demo (eth_account 0.13.7, EIP-712, chainId 1666)
    /// over the ASCII-sorted parameter string. Not invented: reproducible with the
    /// Python snippet in the module docs' demo file.
    #[test]
    fn signing_matches_pinned_eip712_vector() {
        let key = example_key();
        assert_eq!(
            signer_address(&key),
            "21cf8ae13bb72632562c6fff438652ba1a151bb0"
        );

        let mut params: Vec<(String, String)> = vec![
            ("symbol", "BTCUSDT"),
            ("type", "LIMIT"),
            ("side", "BUY"),
            ("timeInForce", "IOC"),
            ("quantity", "0.5"),
            ("price", "100"),
            ("user", "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"),
            ("signer", "0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0"),
            ("nonce", "1748310859508867"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let message = sorted_param_string(&mut params);
        assert_eq!(
            message,
            "nonce=1748310859508867&price=100&quantity=0.5&side=BUY&signer=0x21cF8Ae13Bb72632562c6Fff438652Ba1a151bb0&symbol=BTCUSDT&timeInForce=IOC&type=LIMIT&user=0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"
        );
        assert_eq!(
            hex_lower(&eip712_digest(&message)),
            "08cf67702ddffa1ada78528ab840afb6d8f6ef106e8c998dd2a4e75da751b5da"
        );
        assert_eq!(
            sign_message(&key, &message).unwrap(),
            "45f0190348edf340ffcf222927cc58dedd0fafa726db659a04afe8cf6a6bd75843ddc2fe58c8703ec3d3ee4864e3367e55c2c96e3a8934a390a5a1bdf71873991c"
        );
    }

    #[test]
    fn nonce_is_strictly_increasing_microseconds() {
        let a = next_nonce(0).unwrap();
        let b = next_nonce(0).unwrap();
        assert!(b > a);
        // Sanity: well past 2020-01-01 in microseconds.
        assert!(a > 1_577_836_800_000_000);
    }

    #[test]
    fn client_id_stays_within_the_venue_limit() {
        let id = ClientOrderId("position-1-buy-0".to_string());
        let venue_id = venue_client_id(CLIENT_ID_PREFIX, &id, MAX_CLIENT_ID);
        assert!(venue_id.starts_with("aster-"));
        assert_eq!(venue_id.len(), MAX_CLIENT_ID);
        assert!(
            venue_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
        );
    }

    #[test]
    fn status_mapping_refuses_unknown_states() {
        assert_eq!(map_status("NEW").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("PARTIALLY_FILLED").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("FILLED").unwrap(), OrderStatus::Filled);
        assert_eq!(map_status("CANCELED").unwrap(), OrderStatus::Cancelled);
        assert_eq!(map_status("EXPIRED").unwrap(), OrderStatus::Cancelled);
        assert_eq!(map_status("REJECTED").unwrap(), OrderStatus::Rejected);
        assert!(map_status("SOMETHING_NEW").is_err());
    }

    #[test]
    fn partial_ioc_reported_filled_becomes_cancelled() {
        assert_eq!(
            verify_status(OrderStatus::Filled, dec!(1), dec!(0.4)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            verify_status(OrderStatus::Filled, dec!(1), dec!(1)).unwrap(),
            OrderStatus::Filled
        );
        assert!(verify_status(OrderStatus::Open, dec!(1), dec!(2)).is_err());
    }

    #[test]
    fn fee_aggregation_requires_usdt_and_sums_signed() {
        let trades = vec![
            UserTrade {
                qty: "0.3".into(),
                price: "100".into(),
                quote_qty: Some("30".into()),
                commission: "0.015".into(),
                commission_asset: "USDT".into(),
            },
            UserTrade {
                qty: "0.2".into(),
                price: "100".into(),
                quote_qty: None,
                commission: "-0.005".into(),
                commission_asset: "USDT".into(),
            },
        ];
        let (qty, notional, fee) = aggregate_fills(&trades).unwrap();
        assert_eq!(qty, dec!(0.5));
        assert_eq!(notional, dec!(50));
        assert_eq!(fee, dec!(0.01));

        let bad = vec![UserTrade {
            qty: "0.1".into(),
            price: "100".into(),
            quote_qty: None,
            commission: "0.001".into(),
            commission_asset: "BNB".into(),
        }];
        assert!(aggregate_fills(&bad).is_err());
    }

    #[test]
    fn order_units_use_base_quantity_for_aster() {
        let order = NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("p1-buy-0".into()),
            venue: Venue::Aster,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(1000),
            quantity: None,
            limit_price: Some(dec!(100)),
            reduce_only: false,
            leverage: Some(dec!(5)),
        };
        // 1000 / 100 = 10, floored to the 0.001 step.
        assert_eq!(
            order_units(
                Venue::Aster,
                &order,
                dec!(100),
                Decimal::ONE,
                dec!(0.001),
                dec!(0.001)
            )
            .unwrap(),
            dec!(10)
        );
    }

    #[test]
    fn symbol_round_trips_through_native_names() {
        assert_eq!(symbol_of("BTCUSDT").unwrap(), Symbol::perp("BTC", "USDT"));
        assert_eq!(
            symbol_of("1000PEPEUSDT").unwrap(),
            Symbol::perp("1000PEPE", "USDT")
        );
        assert!(symbol_of("BTCUSD").is_err());
    }
}
