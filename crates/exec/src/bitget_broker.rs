//! Production Bitget **classic account** USDT-M futures broker (`productType=USDT-FUTURES`).
//!
//! Scope: USDT-margined linear perpetuals traded through the classic Bitget account
//! endpoints under `https://api.bitget.com/api/v2/mix/*`. Spot, coin-margined, USDC-margined
//! and the new unified account (UTA) are deliberately out of scope.
//!
//! Trading is disabled by default: [`LiveOptions`] must explicitly enable it and supply a
//! `market_slippage`, or every signed write is refused before a request is built. Every order
//! is LIMIT + IOC; orders without a limit price are bounded from a fresh merge-depth snapshot
//! fetched immediately before signing. Quantities never round up, and reduce-only quantities
//! must be exactly representable at the venue step. The exclusively locked intent journal is
//! fsynced BEFORE the order is transmitted; a reserved client id is NEVER resubmitted, even
//! after a timeout.
//!
//! Account-wide settings are verified, never changed: the account must be in one-way position
//! mode (`posMode=one_way_mode`) and single-asset mode (`assetMode=single`). Only per-symbol
//! margin mode (isolated) and leverage are set, and both are read back before the order.
//!
//! Sizes on USDT-FUTURES are in **base coin** (not contracts); the step is `sizeMultiplier`,
//! the minimum is `minTradeNum`, and `minTradeUSDT` is the minimum notional.
//!
//! Stock / index / commodity / FX contracts (`isRwa = YES`) trade 5×24: closed on weekends and
//! US market holidays, new orders refused while closed (cancels still allowed, mark price and
//! funding frozen). [`Broker::trading_halted`] reports a contract whose `symbolStatus` is not
//! `normal` (`limit_open` still lets a position close). Opening an RWA contract is refused from
//! New York Friday 16:00 to Monday 04:00 and around US holidays ([`rwa_session_closed`]);
//! closing / reducing is refused only around US holidays ([`rwa_holiday_closed`]) — on
//! 2026-10-31 (a Saturday) the RWA contracts were `normal` and trading, so a weekend close goes
//! by the venue's own status.
//!
//! Contract metadata is cached for [`CONTRACTS_TTL`] (status checks want it no older than
//! [`STATUS_TTL`]) and dropped whenever an order is refused; a symbol's margin mode + leverage
//! confirmed within [`SETTINGS_TTL`] is not re-read before the next order. Without these every
//! second leg paid six to eight extra round trips (the whole 846-row contract list twice, the
//! account row twice, a leverage write) while the first leg was already filled.
//!
//! Signature: `ACCESS-SIGN = base64(HMAC_SHA256(secret, timestamp + METHOD + requestPath
//! [+ "?" + queryString] + body))`, sent with `ACCESS-KEY`, `ACCESS-TIMESTAMP` (ms) and
//! `ACCESS-PASSPHRASE`.
//!
//! Protocol sources (researched from the primary documentation):
//! - REST / signature: <https://www.bitget.com/docs/classic/Introduction>
//! - Place order: <https://www.bitget.com/api-doc/contract/trade/Place-Order>
//! - Contract config: <https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts>
//! - Account / position mode: <https://www.bitget.com/api-doc/contract/account/Get-Account>
//! - Set leverage / margin mode: <https://www.bitget.com/api-doc/contract/account/Set-Leverage>
//! - Order detail / fills / pending / cancel: <https://www.bitget.com/api-doc/contract/trade/Get-Order-Details>
//! - Positions: <https://www.bitget.com/api-doc/contract/position/Get-All-Position>
//! - Trade rate / server time: <https://www.bitget.com/api-doc/common/public/Get-Trade-Rate>
//! - Account list / bills / fill history / position margin (classic docs, now under
//!   `/legacy-docs/classic/contract/…`): `Get-Account-List`, `Get-Account-Bill`,
//!   `Get-Fill-History`, `Change-Margin`

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenuePosition};
use crate::live_common::{
    Cached, JournalEntry, LiveOptions, OrderJournal, Verified, base64_standard, hex_lower,
    hmac_sha256, order_units, round_price, transport_error, venue_client_id,
};
use crate::settlement::{FillEffect, VenueFill};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

pub(crate) const BASE: &str = "https://api.bitget.com";
const PRODUCT: &str = "USDT-FUTURES";
pub(crate) const MARGIN_COIN: &str = "USDT";
/// Bitget success code; it is NOT the HTTP status (errors come back as HTTP 200 + a code).
pub(crate) const SUCCESS: &str = "00000";
/// "order does not exist"; the documented code returned by order-detail for an unknown id.
const CODE_NOT_FOUND: &str = "43001";
/// `clientOid` charset is alphanumeric; the strictest published length is 32.
pub(crate) const CLIENT_PREFIX: &str = "bg";
pub(crate) const CLIENT_MAX: usize = 32;
pub(crate) const PATH_TIME: &str = "/api/v2/public/time";
const PATH_CONTRACTS: &str = "/api/v2/mix/market/contracts";
const PATH_DEPTH: &str = "/api/v2/mix/market/merge-depth";
const PATH_ACCOUNT: &str = "/api/v2/mix/account/account";
const PATH_SET_MARGIN_MODE: &str = "/api/v2/mix/account/set-margin-mode";
const PATH_SET_LEVERAGE: &str = "/api/v2/mix/account/set-leverage";
const PATH_PLACE: &str = "/api/v2/mix/order/place-order";
const PATH_DETAIL: &str = "/api/v2/mix/order/detail";
const PATH_FILLS: &str = "/api/v2/mix/order/fills";
const PATH_PENDING: &str = "/api/v2/mix/order/orders-pending";
const PATH_CANCEL: &str = "/api/v2/mix/order/cancel-order";
const PATH_POSITIONS: &str = "/api/v2/mix/position/all-position";
const PATH_TRADE_RATE: &str = "/api/v2/common/all-trade-rate";
const PATH_ACCOUNTS: &str = "/api/v2/mix/account/accounts";
const PATH_BILL: &str = "/api/v2/mix/account/bill";
const PATH_FILL_HISTORY: &str = "/api/v2/mix/order/fill-history";
const PATH_SET_MARGIN: &str = "/api/v2/mix/account/set-margin";

/// The contract list rarely changes between orders; it is re-read this often, by
/// [`Broker::warm_reads`], and whenever an order is refused.
pub(crate) const CONTRACTS_TTL: Duration = Duration::from_secs(600);
/// [`Broker::trading_halted`] wants the status no older than this (an RWA market can close).
pub(crate) const STATUS_TTL: Duration = Duration::from_secs(60);
/// A symbol's margin mode + leverage confirmed this recently is not re-read before the next
/// order. It only skips a read; no check is relaxed.
pub(crate) const SETTINGS_TTL: Duration = Duration::from_secs(20);
/// Bills and fill history only reach back 90 days.
const HISTORY_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
/// `bill` refuses windows over 30 days, `fill-history` over a week; a second of margin each.
const BILL_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1_000 - 1_000;
const FILL_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000 - 1_000;
pub(crate) const HISTORY_PAGE: usize = 100;
/// A history query that needs more pages than this is refused rather than half-read.
pub(crate) const HISTORY_MAX_PAGES: usize = 200;
/// Right after an order ends, its fill rows can lag the order detail; re-read this many times.
pub(crate) const FILL_SYNC_ATTEMPTS: usize = 6;
pub(crate) const FILL_SYNC_GAP: Duration = Duration::from_millis(200);
const BILL_FUNDING: &str = "contract_settle_fee";
/// Business codes after which a write may still have been applied (see [`definitive_refusal`]).
const AMBIGUOUS_CODES: [&str; 6] = ["40010", "45001", "40725", "40015", "40200", "12004"];
/// The reason [`Broker::trading_halted`] gives for the conservative RWA weekend window (opening only).
pub const RWA_CLOSED_NOTE: &str =
    "Bitget 股票 / 商品类合约周末不开新仓（纽约时间周五 16:00 至周一 4:00、美股假日按休市处理）";
/// The reason for a close / reduce refused on a US market holiday.
pub const RWA_HOLIDAY_NOTE: &str =
    "Bitget 股票 / 商品类合约美股假日按休市处理（假日前一天 16:00 至假日后一天 4:00），平仓等开盘";

/// 开仓挡周末 + 假日（[`rwa_session_closed`]），平仓 / 减仓只挡假日（[`rwa_holiday_closed`]）。
pub fn rwa_closed_for(at: DateTime<Utc>, reduce_only: bool) -> bool {
    if reduce_only {
        rwa_holiday_closed(at)
    } else {
        rwa_session_closed(at)
    }
}

pub fn rwa_closed_note(reduce_only: bool) -> &'static str {
    if reduce_only {
        RWA_HOLIDAY_NOTE
    } else {
        RWA_CLOSED_NOTE
    }
}

/// Bitget API credentials. Secrets implement neither `Debug` nor `Serialize`.
pub struct BitgetCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

pub struct BitgetBroker {
    client: reqwest::Client,
    credentials: BitgetCredentials,
    options: LiveOptions,
    journal: SyncMutex<OrderJournal>,
    /// Serializes order submission so duplicate ids cannot race a single process.
    submit: Mutex<()>,
    fee_per_side: Decimal,
    /// `server_time - local_time`, in milliseconds; applied to every signed timestamp.
    time_offset_ms: i64,
    contracts: Cached<Arc<Vec<ContractRow>>>,
    /// (native symbol, leverage, margin mode) read back from the account recently.
    settings: Verified<(String, Decimal, crate::MarginMode)>,
}

impl BitgetBroker {
    /// Build a broker. Reads the server clock, verifies the account is in a compatible
    /// one-way/single-asset mode (never changing account-wide settings) and reads the
    /// account's actual taker fee rate. Fails closed if any of these is unavailable.
    pub async fn connect(
        client: reqwest::Client,
        credentials: BitgetCredentials,
        journal_path: &std::path::Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(Venue::Bitget)?;
        let hex = hex_lower(&Sha256::digest(credentials.api_key.as_bytes()));
        let identity = format!("bitget:{}", &hex[..16]);
        let journal = OrderJournal::open(journal_path, &identity)?;
        let mut broker = Self {
            client,
            credentials,
            options,
            journal: SyncMutex::new(journal),
            submit: Mutex::new(()),
            fee_per_side: Decimal::ZERO,
            time_offset_ms: 0,
            contracts: Cached::new(),
            settings: Verified::new(SETTINGS_TTL),
        };
        broker.time_offset_ms = broker.server_offset().await?;
        let contracts = broker.contract_rows(Duration::ZERO).await?;
        let reference = reference_symbol(&contracts)?;
        let account: AccountRow = broker
            .signed_data(
                Method::GET,
                PATH_ACCOUNT,
                &[
                    ("symbol", reference.clone()),
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        if account
            .margin_coin
            .as_deref()
            .map(str::to_ascii_uppercase)
            .as_deref()
            != Some(MARGIN_COIN)
        {
            return Err(error(
                "Bitget account row is not the USDT margin account; refusing to trade",
            ));
        }
        match account.pos_mode.as_deref() {
            Some("one_way_mode") => {}
            Some(other) => {
                return Err(error(format!(
                    "Bitget account is in '{other}' position mode; this broker requires one-way \
                     mode (change it in the Bitget UI before trading)"
                )));
            }
            None => {
                return Err(error(
                    "Bitget account position mode could not be read; refusing to trade",
                ));
            }
        }
        if matches!(account.asset_mode.as_deref(), Some("union")) {
            return Err(error(
                "Bitget account is in multi-assets (union) mode; this broker requires \
                 single-asset mode for isolated per-symbol accounting",
            ));
        }
        broker.fee_per_side = broker.account_taker_fee().await?;
        Ok(broker)
    }

    /// The account's actual taker fee, as reported by the signed trade-rate endpoint (one row per
    /// symbol). `fee_per_side` is only the fallback cost estimate, so a rate that differs across
    /// symbols (an RWA promotion, say) takes the highest one — conservative — and is logged,
    /// instead of refusing to connect: with an all-or-nothing live desk that refusal would stop
    /// every venue, not just this one.
    async fn account_taker_fee(&self) -> ArbResult<Decimal> {
        let rates: Vec<TradeRate> = self
            .signed_data(
                Method::GET,
                PATH_TRADE_RATE,
                &[("businessType", "mix".to_string())],
                None,
            )
            .await?;
        let (rate, uniform) = highest_taker_fee(&rates)?;
        if !uniform {
            tracing::warn!(
                fallback = %rate,
                "Bitget 账户各合约的吃单费率不一样：成本估算的回落值按最高的算"
            );
        }
        Ok(rate)
    }

    fn authorize(&self) -> ArbResult<()> {
        self.options.authorize(Venue::Bitget)
    }

    fn with_journal<T>(&self, f: impl FnOnce(&mut OrderJournal) -> ArbResult<T>) -> ArbResult<T> {
        let mut journal = self
            .journal
            .lock()
            .map_err(|_| error("intent journal lock poisoned"))?;
        f(&mut journal)
    }

    fn record_terminal(&self, state: &OrderState) -> ArbResult<()> {
        if state.status.is_live() {
            return Ok(());
        }
        self.with_journal(|journal| journal.record_terminal(state))
    }

    // ---- HTTP ---------------------------------------------------------------------------

    async fn request(
        &self,
        signed: bool,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        body: Option<Value>,
    ) -> ArbResult<Value> {
        let (status, envelope) = self.request_raw(signed, method, path, params, body).await?;
        if !status.is_success() {
            let code = envelope.get("code").and_then(Value::as_str).unwrap_or("-");
            return Err(error(format!("{path}: HTTP {status}, code {code}")));
        }
        Ok(envelope)
    }

    /// Any parsed response with its HTTP status. Bitget reports business errors (including
    /// "order not found") as HTTP 400 + code, so callers that must distinguish them read
    /// the code themselves. Transport failures stay errors.
    async fn request_raw(
        &self,
        signed: bool,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        body: Option<Value>,
    ) -> ArbResult<(reqwest::StatusCode, Value)> {
        let query = query_string(params);
        let body_bytes = match &body {
            Some(value) => {
                Some(serde_json::to_vec(value).map_err(|_| error("cannot encode request body"))?)
            }
            None => None,
        };
        let url = if query.is_empty() {
            format!("{BASE}{path}")
        } else {
            format!("{BASE}{path}?{query}")
        };
        let mut request = self.client.request(method.clone(), url);
        if signed {
            let timestamp = self.timestamp_ms().to_string();
            let body_str = body_bytes
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .unwrap_or("");
            let signature = self.signature(&timestamp, method.as_str(), path, &query, body_str);
            request = request
                .header("ACCESS-KEY", sensitive(&self.credentials.api_key)?)
                .header("ACCESS-SIGN", sensitive(&signature)?)
                .header("ACCESS-TIMESTAMP", sensitive(&timestamp)?)
                .header(
                    "ACCESS-PASSPHRASE",
                    sensitive(&self.credentials.passphrase)?,
                );
        }
        request = request
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("locale", "en-US");
        if let Some(bytes) = body_bytes {
            request = request.body(bytes);
        }
        let response = request
            .send()
            .await
            .map_err(|e| transport_error(Venue::Bitget, path, e))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| error(format!("{path} response body unavailable")))?;
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| {
            error(format!(
                "{path}: HTTP {status} with an unparseable response"
            ))
        })?;
        Ok((status, envelope))
    }

    fn signature(
        &self,
        timestamp: &str,
        method: &str,
        path: &str,
        query: &str,
        body: &str,
    ) -> String {
        let message = signing_payload(timestamp, method, path, query, body);
        base64_standard(&hmac_sha256(
            self.credentials.api_secret.as_bytes(),
            message.as_bytes(),
        ))
    }

    async fn public_data<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> ArbResult<T> {
        let envelope = self.request(false, Method::GET, path, params, None).await?;
        check_ok(&envelope)?;
        data_of(envelope)
    }

    async fn signed_data<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        body: Option<Value>,
    ) -> ArbResult<T> {
        let envelope = self.request(true, method, path, params, body).await?;
        check_ok(&envelope)?;
        data_of(envelope)
    }

    async fn server_offset(&self) -> ArbResult<i64> {
        let time: ServerTime = self.public_data(PATH_TIME, &[]).await?;
        let server = time
            .server_time
            .as_deref()
            .ok_or_else(|| error("server time is unavailable"))?;
        let server = i64::from_str(server)
            .map_err(|_| error("server time is not a millisecond timestamp"))?;
        Ok(server - now_ms())
    }

    fn timestamp_ms(&self) -> i64 {
        now_ms() + self.time_offset_ms
    }

    // ---- Instruments --------------------------------------------------------------------

    /// The contract list, no older than `max_age` (`Duration::ZERO` = always re-read).
    async fn contract_rows(&self, max_age: Duration) -> ArbResult<Arc<Vec<ContractRow>>> {
        if let Some(rows) = self.contracts.get(max_age) {
            return Ok(rows);
        }
        let rows: Vec<ContractRow> = self
            .public_data(PATH_CONTRACTS, &[("productType", PRODUCT.to_string())])
            .await?;
        let rows = Arc::new(rows);
        self.contracts.put(Arc::clone(&rows));
        Ok(rows)
    }

    /// The tradable contract for `symbol`; `closing` also accepts a `limit_open` contract (it
    /// still lets a position close). A cached list without it is re-read once before giving up.
    async fn instrument_for(&self, symbol: &Symbol, closing: bool) -> ArbResult<Instrument> {
        let rows = self.contract_rows(CONTRACTS_TTL).await?;
        match select_instrument(&rows, symbol, closing) {
            Ok(instrument) => Ok(instrument),
            Err(_) => {
                let fresh = self.contract_rows(Duration::ZERO).await?;
                select_instrument(&fresh, symbol, closing)
            }
        }
    }

    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Instrument> {
        self.instrument_for(symbol, false).await
    }

    // ---- Order helpers ------------------------------------------------------------------

    /// Bounded limit price for an order without an explicit limit, from a fresh book.
    async fn bound_price(
        &self,
        native: &str,
        side: Side,
        limit_ratio: Option<Decimal>,
    ) -> ArbResult<Decimal> {
        let depth: MergeDepth = self
            .public_data(
                PATH_DEPTH,
                &[
                    ("symbol", native.to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("limit", "5".to_string()),
                ],
            )
            .await?;
        let ts = depth
            .ts
            .as_deref()
            .and_then(|raw| i64::from_str(raw).ok())
            .ok_or_else(|| error("order book timestamp is missing"))?;
        if (self.timestamp_ms() - ts).abs() > 5_000 {
            return Err(error("order book is stale; refusing a price bound"));
        }
        let best = |levels: &[Vec<Value>], ask: bool| -> ArbResult<Option<Decimal>> {
            let mut best: Option<Decimal> = None;
            for level in levels {
                let price = level.first().map(value_dec).transpose()?;
                let size = level.get(1).map(value_dec).transpose()?;
                if let (Some(price), Some(size)) = (price, size)
                    && price > Decimal::ZERO
                    && size > Decimal::ZERO
                {
                    best = Some(match best {
                        None => price,
                        Some(current) if ask => current.min(price),
                        Some(current) => current.max(price),
                    });
                }
            }
            Ok(best)
        };
        let bid = best(&depth.bids, false)?;
        let ask = best(&depth.asks, true)?;
        banded(&self.options, limit_ratio).bound_price(Venue::Bitget, side, bid, ask)
    }

    async fn fetch_order(
        &self,
        instrument: &str,
        client_oid: &str,
    ) -> ArbResult<Option<RemoteOrder>> {
        let (status, envelope) = self
            .request_raw(
                true,
                Method::GET,
                PATH_DETAIL,
                &[
                    ("symbol", instrument.to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("clientOid", client_oid.to_string()),
                ],
                None,
            )
            .await?;
        if status.is_server_error() {
            return Err(error(format!("order detail: HTTP {status}")));
        }
        let code = envelope.get("code").and_then(Value::as_str).unwrap_or("");
        if code == SUCCESS {
            match envelope.get("data") {
                Some(data)
                    if !data.is_null() && data.as_object().is_some_and(|o| !o.is_empty()) =>
                {
                    serde_json::from_value(data.clone())
                        .map(Some)
                        .map_err(|_| error("malformed order detail response"))
                }
                _ => Ok(None),
            }
        } else if code == CODE_NOT_FOUND {
            Ok(None)
        } else {
            Err(api_error(
                code,
                envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
            ))
        }
    }

    /// Immediately after a successful place the order may take a moment to become queryable.
    async fn poll_order(&self, instrument: &str, client_oid: &str) -> ArbResult<RemoteOrder> {
        for _ in 0..12 {
            if let Some(order) = self.fetch_order(instrument, client_oid).await? {
                return Ok(order);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Err(error(
            "accepted order is not yet queryable; reserved id retained for reconciliation",
        ))
    }

    async fn order_fills(&self, order: &RemoteOrder) -> ArbResult<Vec<FillRow>> {
        let order_id = order
            .order_id
            .clone()
            .ok_or_else(|| error("order has no venue id"))?;
        let symbol = order.symbol.clone().unwrap_or_default();
        let mut seen = HashSet::new();
        let mut fills = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = vec![
                ("productType", PRODUCT.to_string()),
                ("orderId", order_id.clone()),
                ("limit", "100".to_string()),
            ];
            if !symbol.is_empty() {
                params.push(("symbol", symbol.clone()));
            }
            if let Some(id) = &cursor {
                params.push(("idLessThan", id.clone()));
            }
            let page: FillPage = self
                .signed_data(Method::GET, PATH_FILLS, &params, None)
                .await?;
            let count = page.fill_list.len();
            for row in page.fill_list {
                let key = row.trade_id.clone().unwrap_or_default();
                if seen.insert(key) {
                    fills.push(row);
                }
            }
            if fills.len() > 10_000 {
                return Err(error("fill history exceeds the reconciliation bound"));
            }
            match page.end_id {
                Some(id)
                    if count >= 100 && !id.is_empty() && cursor.as_deref() != Some(id.as_str()) =>
                {
                    cursor = Some(id);
                }
                _ => return Ok(fills),
            }
        }
    }

    async fn state_from_detail(
        &self,
        remote: &RemoteOrder,
        requested_id: Option<&ClientOrderId>,
    ) -> ArbResult<OrderState> {
        let symbol = symbol_from_native(remote.symbol.as_deref().unwrap_or(""))?;
        let side = parse_side(remote.side.as_deref().unwrap_or(""))?;
        let requested = nonneg(remote.size.as_deref())?;
        if requested <= Decimal::ZERO {
            return Err(error("order detail reports a non-positive size"));
        }
        let executed = nonneg(remote.base_volume.as_deref())?;
        // Fill rows can trail the order detail for a moment right after the order ends.
        let mut synced = None;
        for attempt in 0..FILL_SYNC_ATTEMPTS {
            let fills = self.order_fills(remote).await?;
            let totals = aggregate_fills(&fills, remote)?;
            if totals.0 == executed {
                synced = Some(totals);
                break;
            }
            if attempt + 1 < FILL_SYNC_ATTEMPTS {
                tokio::time::sleep(FILL_SYNC_GAP).await;
            }
        }
        let (filled_qty, filled_notional, fee) = synced.ok_or_else(|| {
            error("fills do not sum to the executed quantity; refusing to infer fees")
        })?;
        let status = verified_status(
            map_state(remote.state.as_deref().unwrap_or(""))?,
            requested,
            filled_qty,
        )?;
        let limit = opt_dec(remote.price.as_deref())?;
        let reduce_only = remote
            .reduce_only
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("YES"));
        let client_oid = remote.client_oid.clone().unwrap_or_default();
        let saved = if client_oid.is_empty() {
            None
        } else {
            self.with_journal(|journal| Ok(journal.by_venue_client_id(&client_oid).cloned()))?
        };
        let order = match &saved {
            Some(entry) => {
                if entry.order.symbol != symbol
                    || entry.order.side != side
                    || entry.order.reduce_only != reduce_only
                    || entry.units != requested
                {
                    return Err(error("venue order does not match the persisted intent"));
                }
                entry.order.clone()
            }
            None => {
                let id = requested_id
                    .cloned()
                    .unwrap_or_else(|| external_id(remote.order_id.as_deref().unwrap_or("")));
                let notional = if let Some(price) = limit {
                    multiply(requested, price)?
                } else if filled_qty > Decimal::ZERO {
                    filled_notional
                } else {
                    Decimal::ZERO
                };
                NewOrder {
                    post_only: false,
                    margin_mode: crate::MarginMode::Isolated,
                    client_order_id: id,
                    venue: Venue::Bitget,
                    symbol,
                    side,
                    notional_usdt: notional,
                    quantity: Some(requested),
                    limit_price: limit,
                    reduce_only,
                    leverage: None,
                }
            }
        };
        if let Some(requested) = requested_id
            && order.client_order_id != *requested
        {
            return Err(error("venue order does not match the requested client id"));
        }
        let mut state = OrderState::new(order);
        state.venue_order_id = remote.order_id.clone();
        state.status = status;
        state.filled_usdt = filled_notional;
        state.average_price = if filled_qty > Decimal::ZERO {
            Some(divide(filled_notional, filled_qty)?)
        } else {
            None
        };
        state.fee_usdt = fee;
        Ok(state)
    }

    /// Configure per-symbol isolated margin and leverage, then read both back. Account-wide
    /// settings are never touched.
    async fn configure(
        &self,
        instrument: &str,
        leverage: Decimal,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let wire_mode = if mode.is_cross() {
            "crossed"
        } else {
            "isolated"
        };
        self.authorize()?;
        let key = (instrument.to_string(), leverage, mode);
        if self.settings.is_fresh(&key) {
            return Ok(());
        }
        let account: AccountRow = self
            .signed_data(
                Method::GET,
                PATH_ACCOUNT,
                &[
                    ("symbol", instrument.to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        // Already in force (the usual case after the first order on a symbol): nothing to write.
        if mode_in_force(&account, leverage)? == Some(mode) {
            self.settings.mark(key);
            return Ok(());
        }
        if account.margin_mode.as_deref() != Some(wire_mode) {
            self.signed_data::<Value>(
                Method::POST,
                PATH_SET_MARGIN_MODE,
                &[],
                Some(json!({
                    "symbol": instrument,
                    "productType": PRODUCT,
                    "marginCoin": MARGIN_COIN,
                    "marginMode": wire_mode,
                })),
            )
            .await?;
        }
        self.signed_data::<Value>(
            Method::POST,
            PATH_SET_LEVERAGE,
            &[],
            Some(json!({
                "symbol": instrument,
                "productType": PRODUCT,
                "marginCoin": MARGIN_COIN,
                "leverage": leverage.normalize().to_string(),
            })),
        )
        .await?;
        let confirmed: AccountRow = self
            .signed_data(
                Method::GET,
                PATH_ACCOUNT,
                &[
                    ("symbol", instrument.to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        if confirmed.margin_mode.as_deref() != Some(wire_mode) {
            return Err(error(
                "isolated margin mode was not confirmed for the symbol",
            ));
        }
        let mut seen = Vec::new();
        let levels = if mode.is_cross() {
            vec![&confirmed.cross_margin_leverage]
        } else {
            vec![
                &confirmed.isolated_long_lever,
                &confirmed.isolated_short_lever,
            ]
        };
        for value in levels {
            if let Some(value) = value.as_ref().map(value_dec).transpose()?
                && value > Decimal::ZERO
            {
                seen.push(value);
            }
        }
        if seen.is_empty() || seen.iter().any(|value| *value != leverage) {
            return Err(error("isolated leverage was not confirmed for the symbol"));
        }
        self.settings.mark(key);
        Ok(())
    }

    /// Read-only: the margin mode already in force for `instrument` at exactly `leverage`, if any.
    async fn mode_in_force(
        &self,
        instrument: &str,
        leverage: Decimal,
    ) -> ArbResult<Option<crate::MarginMode>> {
        let account: AccountRow = self
            .signed_data(
                Method::GET,
                PATH_ACCOUNT,
                &[
                    ("symbol", instrument.to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        mode_in_force(&account, leverage)
    }

    /// One history window `[from, until]`, every page (`idLessThan` = the previous `endId` while
    /// pages come back full). A cursor that stops moving, or more pages than allowed, is an
    /// error rather than a short answer: an incomplete history computes wrong P&L.
    async fn history<P: DeserializeOwned, T>(
        &self,
        path: &str,
        extra: Vec<(&'static str, String)>,
        (from, until): (i64, i64),
        split: impl Fn(P) -> (Vec<T>, Option<String>),
    ) -> ArbResult<Vec<T>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..HISTORY_MAX_PAGES {
            let mut params = extra.clone();
            params.push(("productType", PRODUCT.to_string()));
            params.push(("startTime", from.to_string()));
            params.push(("endTime", until.to_string()));
            params.push(("limit", HISTORY_PAGE.to_string()));
            if let Some(id) = &cursor {
                params.push(("idLessThan", id.clone()));
            }
            let page: P = self.signed_data(Method::GET, path, &params, None).await?;
            let (rows, end_id) = split(page);
            let full = rows.len() >= HISTORY_PAGE;
            out.extend(rows);
            if !full {
                return Ok(out);
            }
            match end_id {
                Some(id) if !id.is_empty() && cursor.as_deref() != Some(id.as_str()) => {
                    cursor = Some(id);
                }
                _ => {
                    return Err(error(
                        "history pagination did not advance; refusing a partial history",
                    ));
                }
            }
        }
        Err(error(
            "history needs more pages than allowed; refusing a partial history",
        ))
    }

    /// Up-to-date enough contract rows for a status check: refreshed when older than
    /// [`STATUS_TTL`], or, if that read fails, any list still within [`CONTRACTS_TTL`].
    async fn status_rows(&self) -> ArbResult<Arc<Vec<ContractRow>>> {
        match self.contract_rows(STATUS_TTL).await {
            Ok(rows) => Ok(rows),
            Err(error) => self.contracts.get(CONTRACTS_TTL).ok_or(error),
        }
    }

    async fn account_net_quantity(&self, symbol: &Symbol) -> ArbResult<Decimal> {
        for position in self.positions().await? {
            if position.symbol == *symbol {
                return Ok(position.net_quantity);
            }
        }
        Ok(Decimal::ZERO)
    }
}

#[async_trait]
impl Broker for BitgetBroker {
    /// 下单按 [`order_units`] 把数量向下取整到市场的数量步长（只减仓单必须正好是它的整数倍）。
    async fn quantity_step(&self, symbol: &Symbol) -> ArbResult<Option<Decimal>> {
        Ok(Some(self.instrument_for(symbol, true).await?.step))
    }

    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let native = native_name(symbol);
        let rows: Vec<PositionRow> = self
            .signed_data(
                Method::GET,
                PATH_POSITIONS,
                &[
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        let Some(row) = rows.iter().find(|row| {
            row.symbol.as_deref() == Some(native.as_str())
                && row
                    .total
                    .as_deref()
                    .and_then(|raw| Decimal::from_str(raw).ok())
                    .is_some_and(|q| q > Decimal::ZERO)
        }) else {
            return Ok(None);
        };
        let parse =
            |value: &Option<String>| value.as_deref().and_then(|raw| Decimal::from_str(raw).ok());
        Ok(Some(crate::margin::venue_state(
            row.margin_mode
                .as_deref()
                .and_then(crate::margin::reported_mode),
            parse(&row.liquidation_price),
            parse(&row.margin_size),
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
        let instrument = self.instrument(symbol).await?;
        let leverage = crate::margin::leverage(Venue::Bitget, leverage)?;
        if leverage < instrument.min_lever || leverage > instrument.max_lever {
            return Err(error("leverage exceeds market limits"));
        }
        self.configure(&instrument.native, leverage, mode).await
    }

    /// Read-only: refreshes the contract list and records the symbol's margin mode when its
    /// leverage already is the requested one, so preparing and placing skip those round trips.
    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let instrument = self.instrument(symbol).await?;
        // Invalid requests are refused by the preparation step, not here.
        let Some(leverage) = leverage
            .and_then(|value| crate::margin::leverage(Venue::Bitget, Some(value)).ok())
            .filter(|value| *value >= instrument.min_lever && *value <= instrument.max_lever)
        else {
            return Ok(());
        };
        let fresh = [crate::MarginMode::Isolated, crate::MarginMode::Cross]
            .into_iter()
            .any(|mode| {
                self.settings
                    .is_fresh(&(instrument.native.clone(), leverage, mode))
            });
        if fresh {
            return Ok(());
        }
        if let Some(mode) = self.mode_in_force(&instrument.native, leverage).await? {
            self.settings.mark((instrument.native, leverage, mode));
        }
        Ok(())
    }

    async fn trading_halted(
        &self,
        symbol: &Symbol,
        reduce_only: bool,
    ) -> ArbResult<Option<String>> {
        let rows = self.status_rows().await?;
        let native = native_name(symbol);
        let Some(row) = rows
            .iter()
            .find(|row| row.symbol.eq_ignore_ascii_case(&native))
        else {
            return Ok(Some(format!("Bitget 合约列表里没有 {native}")));
        };
        if let Some(reason) = status_halt(row.symbol_status.as_deref(), reduce_only) {
            return Ok(Some(reason));
        }
        if row
            .is_rwa
            .as_deref()
            .is_some_and(|rwa| rwa.eq_ignore_ascii_case("yes"))
            && rwa_closed_for(Utc::now(), reduce_only)
        {
            return Ok(Some(rwa_closed_note(reduce_only).into()));
        }
        Ok(None)
    }

    /// The USDT row's maximum openable balance: the smaller of the isolated and cross figures
    /// (`available` when neither is reported); never negative.
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        let rows: Vec<AccountListRow> = self
            .signed_data(
                Method::GET,
                PATH_ACCOUNTS,
                &[("productType", PRODUCT.to_string())],
                None,
            )
            .await?;
        free_collateral_of(&rows).map(Some)
    }

    /// Funding actually settled on this symbol since `since`: `contract_settle_fee` bills. The
    /// bill list cannot be filtered by symbol, so every funding bill of the window is read (in
    /// 30-day windows) and the symbol's are kept.
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let native = native_name(symbol);
        let now = self.timestamp_ms();
        let start = since.timestamp_millis();
        within_retention(start, now)?;
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        for (from, until) in windows(start, now.max(start), BILL_WINDOW_MS) {
            let bills: Vec<BillRow> = self
                .history(
                    PATH_BILL,
                    vec![("businessType", BILL_FUNDING.to_string())],
                    (from, until),
                    |page: BillPage| (page.bills, page.end_id),
                )
                .await?;
            for bill in bills {
                if let Some((id, at, amount)) = funding_entry(&bill, &native, (from, until))?
                    && seen.insert(id)
                {
                    rows.push((at, amount));
                }
            }
        }
        Ok(Some(FundingTotal::from_rows(rows, since)))
    }

    /// Every trade of this account on this symbol in `[since, until]` (fill history, 7-day
    /// windows, every row checked to lie inside its window).
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> ArbResult<Option<Vec<VenueFill>>> {
        let (start, end) = (since.timestamp_millis(), until.timestamp_millis());
        if end < start {
            return Ok(Some(Vec::new()));
        }
        within_retention(start, self.timestamp_ms())?;
        let native = native_name(symbol);
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        for (from, to) in windows(start, end, FILL_WINDOW_MS) {
            let page_rows: Vec<FillRow> = self
                .history(
                    PATH_FILL_HISTORY,
                    vec![("symbol", native.clone())],
                    (from, to),
                    |page: FillPage| (page.fill_list, page.end_id),
                )
                .await?;
            for row in page_rows {
                let at = fill_time(&row)?;
                if at < from || at > to {
                    return Err(error("a fill lies outside the requested window"));
                }
                if !row
                    .symbol
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(&native))
                {
                    return Err(error("fill history returned another symbol"));
                }
                let id = row
                    .trade_id
                    .clone()
                    .ok_or_else(|| error("fill has no trade id"))?;
                if seen.insert(id) {
                    rows.push(row);
                }
            }
        }
        venue_fills(rows).map(Some)
    }

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// `POST /api/v2/mix/account/set-margin` into an existing isolated one-way position. At most
    /// one request, never retried: there is no idempotency key, a resend adds twice.
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.authorize()?;
        let amount = margin_amount_text(amount_usdt)?;
        let _submission = self.submit.lock().await;
        let native = native_name(symbol);
        let rows: Vec<PositionRow> = self
            .signed_data(
                Method::GET,
                PATH_POSITIONS,
                &[
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        let hold_side = isolated_hold_side(&rows, &native)?;
        let submitted = self
            .request_raw(
                true,
                Method::POST,
                PATH_SET_MARGIN,
                &[],
                Some(json!({
                    "symbol": native,
                    "productType": PRODUCT,
                    "marginCoin": MARGIN_COIN,
                    "holdSide": hold_side,
                    "amount": amount,
                })),
            )
            .await;
        // From here the request may have left this host: a failure is "unknown", never resent.
        Ok(match submitted {
            Ok((status, envelope)) => classify_margin_reply(status, &envelope),
            Err(reason) => MarginOutcome::Unknown(format!(
                "{reason}; the request may have arrived and the margin may have been added"
            )),
        })
    }

    fn venue(&self) -> Venue {
        Venue::Bitget
    }

    fn fee_per_side(&self) -> Decimal {
        self.fee_per_side
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        crate::broker::refuse_post_only(order)?;
        self.authorize()?;
        let _submission = self.submit.lock().await;
        if order.venue != Venue::Bitget || order.client_order_id.0.is_empty() {
            return Err(error("wrong venue or empty client order id"));
        }
        let client_oid = venue_client_id(CLIENT_PREFIX, &order.client_order_id, CLIENT_MAX);
        // Idempotency: a reserved id is never resubmitted.
        let reserved = self.with_journal(|journal| {
            Ok(journal.get(&order.client_order_id).map(|entry| {
                (
                    entry.order.clone(),
                    entry.venue_client_id.clone(),
                    entry.instrument.clone(),
                    entry.terminal.clone(),
                )
            }))
        })?;
        if let Some((saved_order, client_oid, instrument, terminal)) = reserved {
            if !same_intent(&saved_order, order) {
                return Err(error("client order id reused with a different intent"));
            }
            if let Some(state) = terminal {
                return ack(&state);
            }
            let remote = self
                .fetch_order(&instrument, &client_oid)
                .await?
                .ok_or_else(|| {
                    error("reserved client id is unresolved; do not resubmit, reconcile manually")
                })?;
            let state = self
                .state_from_detail(&remote, Some(&order.client_order_id))
                .await?;
            self.record_terminal(&state)?;
            return ack(&state);
        }

        let instrument = self
            .instrument_for(&order.symbol, order.reduce_only)
            .await?;
        // Last line of defence; the desk checks [`Broker::trading_halted`] before either leg.
        if instrument.rwa && rwa_closed_for(Utc::now(), order.reduce_only) {
            return Err(error(rwa_closed_note(order.reduce_only)));
        }
        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| error("opening order requires explicit isolated leverage"))?;
            if !value.fract().is_zero()
                || value < Decimal::ONE
                || value < instrument.min_lever
                || value > instrument.max_lever
            {
                return Err(error(
                    "leverage must be a whole number within the market's leverage limit",
                ));
            }
            Some(value)
        };
        // Per-symbol settings first (and read back): a failure leaves no reserved id, and the
        // price bound below is taken after these round trips, not before.
        if let Some(leverage) = leverage {
            self.configure(&instrument.native, leverage, order.margin_mode)
                .await?;
        }
        let raw_price = match order.limit_price {
            Some(price) => price,
            None => {
                self.bound_price(&instrument.native, order.side, instrument.limit_ratio)
                    .await?
            }
        };
        let price = round_price(raw_price, instrument.tick, order.side)
            .ok_or_else(|| error("price rounds to zero at the venue tick"))?;
        let units = order_units(
            Venue::Bitget,
            order,
            price,
            Decimal::ONE,
            instrument.step,
            instrument.min_qty,
        )?;
        let notional = multiply(units, price)?;
        if instrument.min_notional > Decimal::ZERO && notional < instrument.min_notional {
            return Err(error("order is below the venue minimum notional"));
        }
        if let Some(max) = instrument.max_qty
            && units > max
        {
            return Err(error("order exceeds the venue maximum order quantity"));
        }
        if order.reduce_only {
            let net = self.account_net_quantity(&order.symbol).await?;
            let closes = (net > Decimal::ZERO && order.side == Side::Sell)
                || (net < Decimal::ZERO && order.side == Side::Buy);
            if !closes || units > net.abs() {
                return Err(error(
                    "reduce-only quantity/direction does not match the current position",
                ));
            }
        }
        // Persist the intent BEFORE the order request.
        self.with_journal(|journal| {
            journal.reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: client_oid.clone(),
                instrument: instrument.native.clone(),
                units,
                terminal: None,
            })
        })?;
        let body = json!({
            "symbol": instrument.native,
            "productType": PRODUCT,
            "marginMode": if order.margin_mode.is_cross() { "crossed" } else { "isolated" },
            "marginCoin": MARGIN_COIN,
            "size": units.normalize().to_string(),
            "price": price.normalize().to_string(),
            "side": if order.side == Side::Buy { "buy" } else { "sell" },
            "orderType": "limit",
            "force": "ioc",
            "clientOid": client_oid,
            "reduceOnly": if order.reduce_only { "YES" } else { "NO" },
        });
        let submitted = self
            .request_raw(true, Method::POST, PATH_PLACE, &[], Some(body))
            .await;
        let accepted = matches!(&submitted, Ok((status, envelope))
            if status.is_success() && envelope_code(envelope) == SUCCESS);
        // The venue's own record decides, whatever the reply said (never the acknowledgement).
        let remote = if accepted {
            Some(self.poll_order(&instrument.native, &client_oid).await?)
        } else {
            self.fetch_order(&instrument.native, &client_oid).await?
        };
        if let Some(remote) = remote {
            let state = self
                .state_from_detail(&remote, Some(&order.client_order_id))
                .await?;
            self.record_terminal(&state)?;
            return ack(&state);
        }
        match submitted {
            // The order is absent and the venue answered with a business code: a final refusal,
            // unless the code means the outcome is unknown. Record it so `order_state` reports
            // Rejected instead of an intent that can never resolve.
            Ok((status, envelope))
                if !status.is_server_error() && definitive_refusal(envelope_code(&envelope)) =>
            {
                let code = envelope_code(&envelope).to_string();
                let reason = api_error(
                    &code,
                    envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
                );
                // Filters or the contract status may have changed: re-read them next time.
                self.contracts.clear();
                self.settings.clear();
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(format!("Bitget code {code}"));
                self.with_journal(|journal| journal.record_terminal(&rejected))?;
                Err(reason)
            }
            Ok((status, envelope)) => Err(error(format!(
                "order outcome unknown (HTTP {status}, code {}); reserved id retained, never resubmit",
                envelope_code(&envelope)
            ))),
            Err(error) => Err(error),
        }
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let entry = self.with_journal(|journal| {
            Ok(journal.get(client_order_id).map(|entry| {
                (
                    entry.venue_client_id.clone(),
                    entry.instrument.clone(),
                    entry.terminal.clone(),
                )
            }))
        })?;
        let Some((client_oid, instrument, terminal)) = entry else {
            // We always reserve an intent before transmitting; no intent means it was never
            // submitted, which is exactly the only case where `None` is permitted.
            return Ok(None);
        };
        if let Some(state) = terminal {
            return Ok(Some(state));
        }
        match self.fetch_order(&instrument, &client_oid).await? {
            Some(remote) => {
                let state = self
                    .state_from_detail(&remote, Some(client_order_id))
                    .await?;
                self.record_terminal(&state)?;
                Ok(Some(state))
            }
            None => Err(error(
                "reserved order is not found at the venue; it cannot be assumed unsubmitted",
            )),
        }
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        let pending = self.pending_order(venue_order_id).await?;
        let Some(row) = pending else {
            // No longer live (filled/cancelled/unknown): cancellation is idempotent.
            return Ok(());
        };
        let symbol = symbol_from_native(row.symbol.as_deref().unwrap_or(""))?;
        self.signed_data::<Value>(
            Method::POST,
            PATH_CANCEL,
            &[],
            Some(json!({
                "symbol": format!("{}{}", symbol.base, symbol.quote),
                "productType": PRODUCT,
                "marginCoin": MARGIN_COIN,
                "orderId": venue_order_id,
            })),
        )
        .await?;
        if self.pending_order(venue_order_id).await?.is_some() {
            return Err(error("order is still live after cancellation"));
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = vec![
                ("productType", PRODUCT.to_string()),
                ("limit", "100".to_string()),
            ];
            if let Some(id) = &cursor {
                params.push(("idLessThan", id.clone()));
            }
            let page: PendingPage = self
                .signed_data(Method::GET, PATH_PENDING, &params, None)
                .await?;
            let count = page.entrusted_list.len();
            for row in page.entrusted_list {
                let key = row.order_id.clone().unwrap_or_default();
                if seen.insert(key) {
                    rows.push(row);
                }
            }
            if rows.len() > 5_000 {
                return Err(error("pending order list exceeds the reconciliation bound"));
            }
            match page.end_id {
                Some(id)
                    if count >= 100 && !id.is_empty() && cursor.as_deref() != Some(id.as_str()) =>
                {
                    cursor = Some(id);
                }
                _ => break,
            }
        }
        let mut states = Vec::with_capacity(rows.len());
        for row in rows {
            let state = self.state_from_pending(&row)?;
            if state.status.is_live() {
                states.push(state);
            }
        }
        Ok(states)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let rows: Vec<PositionRow> = self
            .signed_data(
                Method::GET,
                PATH_POSITIONS,
                &[
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await?;
        let mut sides: HashMap<String, (bool, bool)> = HashMap::new();
        let mut positions = Vec::new();
        for row in rows {
            let total = nonneg(row.total.as_deref())?;
            if total.is_zero() {
                continue;
            }
            if row.pos_mode.as_deref() == Some("hedge_mode") {
                return Err(error(
                    "hedge-mode position found; this broker requires one-way positions",
                ));
            }
            let symbol = symbol_from_native(row.symbol.as_deref().unwrap_or(""))?;
            let sign = match row.hold_side.as_deref() {
                Some("long") => Decimal::ONE,
                Some("short") => -Decimal::ONE,
                _ => return Err(error("unknown position direction")),
            };
            let mark = dec(row.mark_price.as_deref().unwrap_or(""))?;
            let entry = sides.entry(symbol.base.clone()).or_insert((false, false));
            match row.hold_side.as_deref() {
                Some("long") => entry.0 = true,
                Some("short") => entry.1 = true,
                _ => {}
            }
            if entry.0 && entry.1 {
                return Err(error(
                    "two-sided position found for a symbol; refusing ambiguous net exposure",
                ));
            }
            positions.push(VenuePosition {
                venue: Venue::Bitget,
                symbol,
                net_quantity: total * sign,
                average_price: opt_dec(row.open_price_avg.as_deref())?,
                notional_usdt: multiply(total, mark)?,
            });
        }
        Ok(positions)
    }
}

impl BitgetBroker {
    async fn pending_order(&self, order_id: &str) -> ArbResult<Option<PendingRow>> {
        let page: PendingPage = self
            .signed_data(
                Method::GET,
                PATH_PENDING,
                &[
                    ("productType", PRODUCT.to_string()),
                    ("orderId", order_id.to_string()),
                ],
                None,
            )
            .await?;
        Ok(page
            .entrusted_list
            .into_iter()
            .find(|row| row.order_id.as_deref() == Some(order_id)))
    }

    fn state_from_pending(&self, row: &PendingRow) -> ArbResult<OrderState> {
        let symbol = symbol_from_native(row.symbol.as_deref().unwrap_or(""))?;
        let side = parse_side(row.side.as_deref().unwrap_or(""))?;
        let requested = nonneg(row.size.as_deref())?;
        let executed = nonneg(row.base_volume.as_deref())?;
        let status = verified_status(
            map_state(row.status.as_deref().unwrap_or(""))?,
            requested,
            executed,
        )?;
        let limit = opt_dec(row.price.as_deref())?;
        let reduce_only = row
            .reduce_only
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("YES"));
        let client_oid = row.client_oid.clone().unwrap_or_default();
        let saved = if client_oid.is_empty() {
            None
        } else {
            self.with_journal(|journal| Ok(journal.by_venue_client_id(&client_oid).cloned()))?
        };
        let order = match &saved {
            Some(entry) => {
                if entry.order.symbol != symbol || entry.order.side != side {
                    return Err(error("pending order does not match the persisted intent"));
                }
                entry.order.clone()
            }
            None => NewOrder {
                post_only: false,
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: external_id(row.order_id.as_deref().unwrap_or("")),
                venue: Venue::Bitget,
                symbol,
                side,
                notional_usdt: if let Some(price) = limit {
                    multiply(requested, price)?
                } else {
                    nonneg(row.quote_volume.as_deref())?
                },
                quantity: Some(requested),
                limit_price: limit,
                reduce_only,
                leverage: None,
            },
        };
        let filled_usdt = nonneg(row.quote_volume.as_deref())?;
        let mut state = OrderState::new(order);
        state.venue_order_id = row.order_id.clone();
        state.status = status;
        state.filled_usdt = filled_usdt;
        state.average_price = if executed > Decimal::ZERO && filled_usdt > Decimal::ZERO {
            Some(divide(filled_usdt, executed)?)
        } else {
            opt_dec(row.price_avg.as_deref())?
        };
        Ok(state)
    }
}

// ---- Structured response rows ------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ServerTime {
    pub(crate) server_time: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractRow {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    symbol_type: Option<String>,
    symbol_status: Option<String>,
    /// `YES` for stock / index / commodity / FX contracts (5×24 trading).
    #[serde(default)]
    is_rwa: Option<String>,
    #[serde(default)]
    support_margin_coins: Vec<String>,
    min_trade_num: Option<String>,
    price_end_step: Option<String>,
    volume_place: Option<String>,
    size_multiplier: Option<String>,
    min_trade_usdt: Option<String>,
    max_order_qty: Option<String>,
    min_lever: Option<String>,
    max_lever: Option<String>,
    #[serde(default)]
    buy_limit_price_ratio: Option<String>,
    #[serde(default)]
    sell_limit_price_ratio: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountRow {
    margin_coin: Option<String>,
    margin_mode: Option<String>,
    pos_mode: Option<String>,
    asset_mode: Option<String>,
    #[serde(default)]
    isolated_long_lever: Option<Value>,
    #[serde(default)]
    isolated_short_lever: Option<Value>,
    #[serde(default)]
    cross_margin_leverage: Option<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeDepth {
    #[serde(default)]
    asks: Vec<Vec<Value>>,
    #[serde(default)]
    bids: Vec<Vec<Value>>,
    ts: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeRate {
    pub(crate) taker_fee_rate: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    symbol: Option<String>,
    size: Option<String>,
    order_id: Option<String>,
    client_oid: Option<String>,
    base_volume: Option<String>,
    price: Option<String>,
    state: Option<String>,
    side: Option<String>,
    reduce_only: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillPage {
    #[serde(default)]
    fill_list: Vec<FillRow>,
    end_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountListRow {
    margin_coin: Option<String>,
    available: Option<String>,
    crossed_max_available: Option<String>,
    isolated_max_available: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BillPage {
    #[serde(default)]
    bills: Vec<BillRow>,
    end_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BillRow {
    bill_id: Option<String>,
    symbol: Option<String>,
    amount: Option<String>,
    business_type: Option<String>,
    coin: Option<String>,
    c_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillRow {
    trade_id: Option<String>,
    #[serde(default)]
    c_time: Option<String>,
    symbol: Option<String>,
    order_id: Option<String>,
    price: Option<String>,
    base_volume: Option<String>,
    side: Option<String>,
    #[serde(default)]
    fee_detail: Vec<FeeDetail>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeDetail {
    fee_coin: Option<String>,
    total_fee: Option<String>,
    #[allow(dead_code)]
    deduction: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingPage {
    #[serde(default)]
    entrusted_list: Vec<PendingRow>,
    end_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingRow {
    symbol: Option<String>,
    size: Option<String>,
    order_id: Option<String>,
    client_oid: Option<String>,
    base_volume: Option<String>,
    price_avg: Option<String>,
    price: Option<String>,
    status: Option<String>,
    side: Option<String>,
    quote_volume: Option<String>,
    reduce_only: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: Option<String>,
    total: Option<String>,
    hold_side: Option<String>,
    open_price_avg: Option<String>,
    mark_price: Option<String>,
    pos_mode: Option<String>,
    #[serde(default)]
    margin_mode: Option<String>,
    #[serde(default)]
    liquidation_price: Option<String>,
    #[serde(default)]
    margin_size: Option<String>,
}

// ---- Pure helpers ------------------------------------------------------------------------

pub(crate) struct Instrument {
    pub(crate) native: String,
    /// Stock / commodity / FX contract (5×24, see [`rwa_session_closed`]).
    pub(crate) rwa: bool,
    pub(crate) tick: Decimal,
    pub(crate) step: Decimal,
    pub(crate) min_qty: Decimal,
    pub(crate) min_notional: Decimal,
    pub(crate) max_qty: Option<Decimal>,
    pub(crate) min_lever: Decimal,
    pub(crate) max_lever: Decimal,
    /// How far a limit price may sit from the market (`buy/sellLimitPriceRatio`, the smaller):
    /// 0.02 on most stock contracts. Orders priced beyond it are refused.
    pub(crate) limit_ratio: Option<Decimal>,
}

/// The price protection for an order without a limit price: the configured slippage, but never
/// past nine tenths of the contract's price band (an order beyond the band is refused, and an
/// exit or rollback that cannot be sent is worse than one that fills a little less).
pub(crate) fn banded(options: &LiveOptions, limit_ratio: Option<Decimal>) -> LiveOptions {
    let mut options = options.clone();
    if let (Some(slippage), Some(ratio)) = (options.market_slippage, limit_ratio) {
        options.market_slippage = Some(slippage.min(ratio * Decimal::new(9, 1)));
    }
    options
}

/// The smaller of the buy and sell price-band ratios, when the venue reports a positive one.
pub(crate) fn limit_ratio(buy: Option<&str>, sell: Option<&str>) -> ArbResult<Option<Decimal>> {
    let ratios = [opt_dec(buy)?, opt_dec(sell)?];
    Ok(ratios
        .into_iter()
        .flatten()
        .filter(|ratio| *ratio > Decimal::ZERO)
        .min())
}

/// The signed message, in the exact documented order. The query string is appended without
/// its leading `?`; the `?` is inserted only when a query is present.
pub(crate) fn signing_payload(
    timestamp: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &str,
) -> String {
    let mut message = String::with_capacity(
        timestamp.len() + method.len() + path.len() + query.len() + body.len() + 2,
    );
    message.push_str(timestamp);
    message.push_str(method);
    message.push_str(path);
    if !query.is_empty() {
        message.push('?');
        message.push_str(query);
    }
    message.push_str(body);
    message
}

pub(crate) fn query_string(params: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (index, (key, value)) in params.iter().enumerate() {
        if index > 0 {
            out.push('&');
        }
        out.push_str(key);
        out.push('=');
        out.push_str(&percent_encode(value));
    }
    out
}

/// Unreserved-set percent encoding (RFC 3986). Values are symbols, decimals and hex ids.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(
                    char::from_digit((byte >> 4) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit((byte & 15) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

pub(crate) fn sensitive(value: &str) -> ArbResult<HeaderValue> {
    let mut header =
        HeaderValue::from_str(value).map_err(|_| error("invalid credential header value"))?;
    header.set_sensitive(true);
    Ok(header)
}

/// A business code that does not mean "outcome unknown". Success is not a refusal; the
/// timeout / unknown-error / upstream-error / "system abnormal, try again later" codes may still
/// have created the order (<https://www.bitget.com/docs/classic/error-code/restapi>:
/// 40010 Request timed out, 45001 Unknown error, 40725 service return an error,
/// 40015 System is abnormal, 40200 Server upgrade, 12004 Quote query failed).
fn definitive_refusal(code: &str) -> bool {
    !matches!(code, SUCCESS | "") && !AMBIGUOUS_CODES.contains(&code)
}

/// A margin top-up reply: confirmed, refused by a business code (nothing moved), or unknown
/// (server error, no code, or a code after which the write may still have happened).
fn classify_margin_reply(status: reqwest::StatusCode, envelope: &Value) -> MarginOutcome {
    let code = envelope_code(envelope);
    let message = || {
        api_error(
            code,
            envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
        )
        .to_string()
    };
    if status.is_success() && code == SUCCESS {
        MarginOutcome::Applied
    } else if status.is_server_error() || code.is_empty() || AMBIGUOUS_CODES.contains(&code) {
        MarginOutcome::Unknown(message())
    } else {
        MarginOutcome::Refused(message())
    }
}

/// A top-up amount must be positive and already rounded to the cent: the request then carries
/// exactly what the ledger records.
pub(crate) fn margin_amount_text(amount: Decimal) -> ArbResult<String> {
    if amount <= Decimal::ZERO {
        return Err(error("the margin amount must be positive"));
    }
    let amount = amount.normalize();
    if amount.scale() > 2 {
        return Err(error(
            "the margin amount must be rounded to the cent (at most two decimals)",
        ));
    }
    Ok(amount.to_string())
}

/// The side (`long` / `short`) of the open, isolated, one-way position on `native`.
fn isolated_hold_side(rows: &[PositionRow], native: &str) -> ArbResult<&'static str> {
    let row = rows
        .iter()
        .find(|row| {
            row.symbol
                .as_deref()
                .is_some_and(|symbol| symbol.eq_ignore_ascii_case(native))
                && nonneg(row.total.as_deref()).is_ok_and(|total| total > Decimal::ZERO)
        })
        .ok_or_else(|| error("no open position on this symbol to add margin to"))?;
    if row.pos_mode.as_deref() == Some("hedge_mode") {
        return Err(error("margin is only added to one-way positions"));
    }
    if row
        .margin_mode
        .as_deref()
        .and_then(crate::margin::reported_mode)
        != Some(crate::MarginMode::Isolated)
    {
        return Err(error("margin can only be added to an isolated position"));
    }
    match row.hold_side.as_deref() {
        Some("long") => Ok("long"),
        Some("short") => Ok("short"),
        _ => Err(error("unknown position direction")),
    }
}

/// The USDT row's maximum openable balance (see [`Broker::free_collateral`]).
fn free_collateral_of(rows: &[AccountListRow]) -> ArbResult<Decimal> {
    let Some(row) = rows
        .iter()
        .find(|row| normalized(row.margin_coin.as_deref()).as_deref() == Some(MARGIN_COIN))
    else {
        return Ok(Decimal::ZERO);
    };
    let limits = [
        opt_dec(row.isolated_max_available.as_deref())?,
        opt_dec(row.crossed_max_available.as_deref())?,
    ];
    let free = match limits.into_iter().flatten().min() {
        Some(free) => free,
        None => opt_dec(row.available.as_deref())?
            .ok_or_else(|| error("the USDT account reports no available balance"))?,
    };
    Ok(free.max(Decimal::ZERO))
}

pub(crate) fn within_retention(start_ms: i64, now_ms: i64) -> ArbResult<()> {
    if now_ms - start_ms > HISTORY_RETENTION_MS {
        return Err(error(
            "bills and fills older than 90 days are not available through the API",
        ));
    }
    Ok(())
}

/// `[start, end]` cut into consecutive, non-overlapping inclusive windows of at most `span` ms.
pub(crate) fn windows(start: i64, end: i64, span: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let mut from = start;
    while from <= end {
        let until = from.saturating_add(span).min(end);
        out.push((from, until));
        if until == end {
            break;
        }
        from = until + 1;
    }
    out
}

pub(crate) fn millis(raw: Option<&str>) -> ArbResult<i64> {
    raw.and_then(|value| i64::from_str(value.trim()).ok())
        .ok_or_else(|| error("missing or invalid millisecond timestamp"))
}

pub(crate) fn utc_millis(ms: i64) -> ArbResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms).ok_or_else(|| error("timestamp out of range"))
}

/// A funding bill of `native` inside `[from, until]` as (bill id, time, signed USDT amount;
/// positive = received). Other symbols are skipped (`None`); anything malformed is an error.
fn funding_entry(
    bill: &BillRow,
    native: &str,
    (from, until): (i64, i64),
) -> ArbResult<Option<(String, DateTime<Utc>, Decimal)>> {
    if bill.business_type.as_deref() != Some(BILL_FUNDING) {
        return Err(error(
            "the funding bill list returned another business type",
        ));
    }
    let at = millis(bill.c_time.as_deref())?;
    if at < from || at > until {
        return Err(error("a bill lies outside the requested window"));
    }
    if !bill
        .symbol
        .as_deref()
        .is_some_and(|symbol| symbol.eq_ignore_ascii_case(native))
    {
        return Ok(None);
    }
    if normalized(bill.coin.as_deref()).as_deref() != Some(MARGIN_COIN) {
        return Err(error("a funding bill is not denominated in USDT"));
    }
    let id = bill
        .bill_id
        .clone()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| error("a funding bill has no id"))?;
    Ok(Some((
        id,
        utc_millis(at)?,
        dec(bill.amount.as_deref().unwrap_or(""))?,
    )))
}

fn fill_time(row: &FillRow) -> ArbResult<i64> {
    millis(row.c_time.as_deref())
}

/// Fill-history rows as venue fills, oldest first. Fees are reported negative when charged
/// (`fee_usdt` is positive for a cost); a non-USDT fee is refused.
fn venue_fills(rows: Vec<FillRow>) -> ArbResult<Vec<VenueFill>> {
    let mut timed = Vec::with_capacity(rows.len());
    for row in rows {
        timed.push((fill_time(&row)?, row));
    }
    timed.sort_by(|a, b| (a.0, &a.1.trade_id).cmp(&(b.0, &b.1.trade_id)));
    let mut fills = Vec::with_capacity(timed.len());
    for (at, row) in timed {
        let quantity = dec(row.base_volume.as_deref().unwrap_or(""))?;
        let price = dec(row.price.as_deref().unwrap_or(""))?;
        if quantity <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(error("a fill has a non-positive quantity or price"));
        }
        let mut fee = Decimal::ZERO;
        for detail in &row.fee_detail {
            if normalized(detail.fee_coin.as_deref()).as_deref() != Some(MARGIN_COIN) {
                return Err(error(
                    "fill fee is not denominated in USDT; disable the BGB fee deduction",
                ));
            }
            fee = add(fee, dec(detail.total_fee.as_deref().unwrap_or(""))?)?;
        }
        fills.push(VenueFill {
            at: utc_millis(at)?,
            side: parse_side(&row.side.as_deref().unwrap_or("").to_ascii_lowercase())?,
            quantity,
            price,
            fee_usdt: -fee,
            effect: FillEffect::Unknown,
        });
    }
    Ok(fills)
}

pub(crate) fn envelope_code(envelope: &Value) -> &str {
    envelope.get("code").and_then(Value::as_str).unwrap_or("")
}

pub(crate) fn check_ok(envelope: &Value) -> ArbResult<()> {
    let code = envelope_code(envelope);
    if code == SUCCESS {
        Ok(())
    } else {
        Err(api_error(
            code,
            envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
        ))
    }
}

pub(crate) fn data_of<T: DeserializeOwned>(envelope: Value) -> ArbResult<T> {
    let data = envelope
        .get("data")
        .cloned()
        .ok_or_else(|| error("venue response is missing data"))?;
    if data.is_null() {
        return Err(error("venue response data is null"));
    }
    serde_json::from_value(data).map_err(|_| error("malformed venue response"))
}

pub(crate) fn api_error(code: &str, message: &str) -> ArbError {
    let message: String = message
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    error(format!("venue error code {code}: {message}"))
}

pub(crate) fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue("bitget", message)
}

pub(crate) fn parse_side(raw: &str) -> ArbResult<Side> {
    match raw {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(error("unknown order side")),
    }
}

fn map_state(state: &str) -> ArbResult<OrderStatus> {
    match state {
        "live" | "partially_filled" => Ok(OrderStatus::Open),
        "filled" => Ok(OrderStatus::Filled),
        "canceled" | "cancelled" => Ok(OrderStatus::Cancelled),
        _ => Err(error(
            "unknown venue order state; refusing to infer terminality",
        )),
    }
}

pub(crate) fn verified_status(
    status: OrderStatus,
    requested: Decimal,
    executed: Decimal,
) -> ArbResult<OrderStatus> {
    if executed < Decimal::ZERO || executed > requested {
        return Err(error(
            "executed quantity is inconsistent with the order size",
        ));
    }
    if status == OrderStatus::Rejected && executed > Decimal::ZERO {
        return Ok(OrderStatus::Cancelled);
    }
    if requested > Decimal::ZERO && executed >= requested {
        return Ok(OrderStatus::Filled);
    }
    if status == OrderStatus::Filled {
        // A terminal IOC match is not necessarily a full fill of the effective request.
        return Ok(OrderStatus::Cancelled);
    }
    Ok(status)
}

/// Fees are reported by Bitget as signed amounts where a charge is negative; `fee_usdt` uses
/// the opposite convention (positive = cost, rebates negative). Any non-USDT fee currency is
/// rejected rather than silently repriced.
fn aggregate_fills(
    fills: &[FillRow],
    order: &RemoteOrder,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let (mut quantity, mut notional, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut seen = std::collections::HashSet::new();
    for fill in fills {
        if fill.order_id.as_deref() != order.order_id.as_deref() {
            return Err(error("fill belongs to a different order"));
        }
        if normalized(fill.symbol.as_deref()) != normalized(order.symbol.as_deref()) {
            return Err(error("fill symbol does not match the order"));
        }
        if fill.side.as_deref() != order.side.as_deref() {
            return Err(error("fill side does not match the order"));
        }
        // Pagination overlap must not double-count a trade's size or fee.
        let trade_id = fill
            .trade_id
            .as_deref()
            .ok_or_else(|| error("fill has no trade id"))?;
        if !seen.insert(trade_id) {
            return Err(error("fill appears twice; fee history is not reliable"));
        }
        let base = dec(fill.base_volume.as_deref().unwrap_or(""))?;
        let price = dec(fill.price.as_deref().unwrap_or(""))?;
        quantity = add(quantity, base)?;
        notional = add(notional, multiply(base, price)?)?;
        for detail in &fill.fee_detail {
            let coin = normalized(detail.fee_coin.as_deref());
            if coin.as_deref() != Some("USDT") {
                return Err(error(
                    "fill fee is not denominated in USDT; cannot account for it as USDT",
                ));
            }
            fee = add(fee, dec(detail.total_fee.as_deref().unwrap_or(""))?)?;
        }
    }
    Ok((quantity, notional, -fee))
}

pub(crate) fn normalized(value: Option<&str>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
}

pub(crate) fn same_intent(a: &NewOrder, b: &NewOrder) -> bool {
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

pub(crate) fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| error("venue order id is missing; the order cannot be cancelled"))?,
        status: state.status,
    })
}

pub(crate) fn external_id(venue_order_id: &str) -> ClientOrderId {
    ClientOrderId(format!("bitget-external-{venue_order_id}"))
}

/// The native contract name of a scanner symbol (`NVDA/USDT` → `NVDAUSDT`).
pub(crate) fn native_name(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// Reconstruct the scanner's `Symbol` from a native symbol such as `BTCUSDT`. Delivery
/// contracts (`BTCUSDT_250926`) are rejected rather than silently hidden.
pub(crate) fn symbol_from_native(native: &str) -> ArbResult<Symbol> {
    let upper = native.trim().to_ascii_uppercase();
    if upper.contains('_') || !upper.ends_with("USDT") || upper.len() <= 4 {
        return Err(error("order/position is not a plain USDT perpetual"));
    }
    Ok(Symbol::perp(&upper[..upper.len() - 4], "USDT"))
}

/// Select the single tradable USDT-margined perp whose metadata maps to `symbol`. Matching is
/// by the live instrument metadata (base/quote/margin/symbolType/status), never by string
/// guessing alone; any ambiguity fails closed.
fn select_instrument(
    rows: &[ContractRow],
    symbol: &Symbol,
    closing: bool,
) -> ArbResult<Instrument> {
    let native = native_name(symbol);
    let mut found = None;
    for row in rows {
        let status = row.symbol_status.as_deref();
        if row.quote_coin.eq_ignore_ascii_case("USDT")
            && row.symbol.eq_ignore_ascii_case(&native)
            && row.base_coin.eq_ignore_ascii_case(&symbol.base)
            && row.symbol_type.as_deref() == Some("perpetual")
            && (status == Some("normal") || closing && status == Some("limit_open"))
            && row
                .support_margin_coins
                .iter()
                .any(|coin| coin.eq_ignore_ascii_case("USDT"))
        {
            if found.is_some() {
                return Err(error(format!(
                    "ambiguous USDT perpetual instrument {native}"
                )));
            }
            found = Some(parse_contract(row)?);
        }
    }
    found.ok_or_else(|| error(format!("unknown or untradable USDT perpetual {native}")))
}

fn parse_contract(row: &ContractRow) -> ArbResult<Instrument> {
    let tick = dec(row.price_end_step.as_deref().unwrap_or(""))?;
    let step = dec(row.size_multiplier.as_deref().unwrap_or(""))?;
    if tick <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(error("instrument reports a non-positive tick or step"));
    }
    let min_qty = dec(row.min_trade_num.as_deref().unwrap_or(""))?;
    let min_notional = dec(row.min_trade_usdt.as_deref().unwrap_or("0"))?;
    let max_qty = opt_dec(row.max_order_qty.as_deref())?.filter(|value| *value > Decimal::ZERO);
    let min_lever = opt_dec(row.min_lever.as_deref())?
        .filter(|value| *value > Decimal::ZERO)
        .unwrap_or(Decimal::ONE);
    let max_lever = dec(row.max_lever.as_deref().unwrap_or(""))?;
    // `volumePlace` is the number of size decimals; the step must not exceed it.
    if let Some(volume_place) = row
        .volume_place
        .as_deref()
        .and_then(|value| u32::from_str(value).ok())
        && step.scale() > volume_place
    {
        return Err(error(
            "instrument size step exceeds its documented precision",
        ));
    }
    Ok(Instrument {
        native: row.symbol.to_ascii_uppercase(),
        rwa: row
            .is_rwa
            .as_deref()
            .is_some_and(|rwa| rwa.eq_ignore_ascii_case("yes")),
        tick,
        step,
        min_qty,
        min_notional,
        max_qty,
        min_lever,
        max_lever,
        limit_ratio: limit_ratio(
            row.buy_limit_price_ratio.as_deref(),
            row.sell_limit_price_ratio.as_deref(),
        )?,
    })
}

/// The highest taker rate across the trade-rate rows, and whether they were all the same.
pub(crate) fn highest_taker_fee(rates: &[TradeRate]) -> ArbResult<(Decimal, bool)> {
    let mut parsed = Vec::with_capacity(rates.len());
    for row in rates {
        let rate = dec(&row.taker_fee_rate)?;
        if rate < Decimal::ZERO {
            return Err(error("account taker fee rate is negative"));
        }
        parsed.push(rate);
    }
    let highest = parsed
        .iter()
        .copied()
        .max()
        .ok_or_else(|| error("account taker fee rate is unavailable"))?;
    if highest <= Decimal::ZERO {
        return Err(error("account taker fee rate is not positive"));
    }
    Ok((highest, parsed.iter().all(|rate| *rate == highest)))
}

/// The margin mode in force when the account row already shows exactly `leverage` for it
/// (both sides when isolated). Anything unreadable is `None`: configure it.
fn mode_in_force(account: &AccountRow, leverage: Decimal) -> ArbResult<Option<crate::MarginMode>> {
    let Some(mode) = account
        .margin_mode
        .as_deref()
        .and_then(crate::margin::reported_mode)
    else {
        return Ok(None);
    };
    let levels = if mode.is_cross() {
        vec![&account.cross_margin_leverage]
    } else {
        vec![&account.isolated_long_lever, &account.isolated_short_lever]
    };
    let mut seen = Vec::new();
    for value in levels {
        if let Some(value) = value.as_ref().map(value_dec).transpose()?
            && value > Decimal::ZERO
        {
            seen.push(value);
        }
    }
    Ok((!seen.is_empty() && seen.iter().all(|value| *value == leverage)).then_some(mode))
}

/// Why this contract cannot take an order right now, from its `symbolStatus` (`None` = it can).
/// `closing`: only a reduce-only order is wanted (`limit_open` still allows it).
fn status_halt(status: Option<&str>, closing: bool) -> Option<String> {
    match status {
        Some("normal") => None,
        Some("limit_open") if closing => None,
        Some("limit_open") => Some("Bitget 这个合约现在只能平仓、不能开仓（limit_open）".into()),
        Some("maintain") => Some("Bitget 这个合约在维护 / 休市，开平仓都不行（maintain）".into()),
        Some("restrictedAPI") => Some("Bitget 这个合约现在不接受 API 下单（restrictedAPI）".into()),
        Some(other) => Some(format!("Bitget 这个合约现在不在交易（{other}）")),
        None => Some("Bitget 没报这个合约的交易状态".into()),
    }
}

/// 纽交所全天休市日（2026、2027 年；观察日已按周末顺延）。Bitget 的股票类合约美股假日也休市、拒新单
/// （见文件头）。**2028 年起要补上**，否则假日那天会照常当成开市。
const US_MARKET_HOLIDAYS: [(i32, u32, u32); 20] = [
    (2026, 1, 1),
    (2026, 1, 19),
    (2026, 2, 16),
    (2026, 4, 3),
    (2026, 5, 25),
    (2026, 6, 19),
    (2026, 7, 3),
    (2026, 9, 7),
    (2026, 11, 26),
    (2026, 12, 25),
    (2027, 1, 1),
    (2027, 1, 18),
    (2027, 2, 15),
    (2027, 3, 26),
    (2027, 5, 31),
    (2027, 6, 18),
    (2027, 7, 5),
    (2027, 9, 6),
    (2027, 11, 25),
    (2027, 12, 24),
];

/// 纽交所提前收盘（13:00）的日子：感恩节次日、平安夜（不是假日时）。
const US_MARKET_EARLY_CLOSES: [(i32, u32, u32); 3] =
    [(2026, 11, 27), (2026, 12, 24), (2027, 11, 26)];

fn listed(table: &[(i32, u32, u32)], date: chrono::NaiveDate) -> bool {
    table
        .iter()
        .any(|&(year, month, day)| (date.year(), date.month(), date.day()) == (year, month, day))
}

/// The conservative RWA closure window: New York Friday 16:00 to Monday 04:00. Bitget's stock
/// contracts trade 5×24 and refuse new orders while closed; the exact hours have not been
/// observed yet, so the window is wider than the US weekend on both ends.
///
/// 美股假日照周末的口径：前一天 16:00 起、假日全天、后一天 04:00 前都算休市；提前收盘的日子 13:00 起算休市。
/// 2026-10-31 审查发现以前只挡周末：11-26 感恩节 Bitget 做第二腿会被拒、第一腿要回滚，持仓的平仓单也发不出。
pub fn rwa_session_closed(at: DateTime<Utc>) -> bool {
    let local = arb_core::new_york::local(at);
    let weekend = match local.weekday() {
        Weekday::Fri => local.hour() >= 16,
        Weekday::Sat | Weekday::Sun => true,
        Weekday::Mon => local.hour() < 4,
        _ => false,
    };
    weekend || rwa_holiday_closed(at)
}

/// 平仓 / 减仓用的休市窗口：只挡美股假日（同 [`rwa_session_closed`] 的假日口径），**不挡周末**。
///
/// 2026-10-31（周六）实测：股票类合约 `symbolStatus` 全是 `normal`，盘口在动，公开成交一直到几分钟前
/// （某合约、某合约、某合约、某合约、某合约、NVDA 都是），周末照常能成交。开仓仍按整个周末窗口挡（周末的价格行为
/// 还没观察够）；平仓以交易所返回的合约状态为准。假日还没验证过，平仓照旧挡。
pub fn rwa_holiday_closed(at: DateTime<Utc>) -> bool {
    let local = arb_core::new_york::local(at);
    let date = local.date();
    let holiday = |date: Option<chrono::NaiveDate>| {
        date.is_some_and(|date| listed(&US_MARKET_HOLIDAYS, date))
    };
    holiday(Some(date))
        || (holiday(date.succ_opt()) && local.hour() >= 16)
        || (holiday(date.pred_opt()) && local.hour() < 4)
        || (listed(&US_MARKET_EARLY_CLOSES, date) && local.hour() >= 13)
}

/// A representative plain USDT perpetual, used for the account-level settings probe.
fn reference_symbol(rows: &[ContractRow]) -> ArbResult<String> {
    let valid = |row: &ContractRow| {
        row.quote_coin.eq_ignore_ascii_case("USDT")
            && row.symbol_type.as_deref() == Some("perpetual")
            && row.symbol_status.as_deref() == Some("normal")
            && !row.symbol.contains('_')
    };
    if let Some(row) = rows
        .iter()
        .find(|row| valid(row) && row.symbol.eq_ignore_ascii_case("BTCUSDT"))
    {
        return Ok(row.symbol.to_ascii_uppercase());
    }
    rows.iter()
        .find(|row| valid(row))
        .map(|row| row.symbol.to_ascii_uppercase())
        .ok_or_else(|| error("no tradable USDT perpetual contracts are available"))
}

pub(crate) fn dec(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| error("invalid decimal in venue data"))
}

pub(crate) fn opt_dec(raw: Option<&str>) -> ArbResult<Option<Decimal>> {
    match raw {
        Some(value) if !value.trim().is_empty() => dec(value).map(Some),
        _ => Ok(None),
    }
}

pub(crate) fn nonneg(raw: Option<&str>) -> ArbResult<Decimal> {
    let value = opt_dec(raw)?.unwrap_or(Decimal::ZERO);
    if value < Decimal::ZERO {
        return Err(error("venue reported a negative quantity"));
    }
    Ok(value)
}

pub(crate) fn value_dec(value: &Value) -> ArbResult<Decimal> {
    if let Some(text) = value.as_str() {
        dec(text)
    } else if value.is_number() {
        dec(&value.to_string())
    } else {
        Err(error("expected a decimal value in venue data"))
    }
}

pub(crate) fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b)
        .ok_or_else(|| error("decimal multiplication overflow"))
}

pub(crate) fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b)
        .ok_or_else(|| error("decimal addition overflow"))
}

pub(crate) fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b)
        .ok_or_else(|| error("decimal division overflow or zero divisor"))
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order() -> NewOrder {
        NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("position-buy-0".into()),
            venue: Venue::Bitget,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(250),
            quantity: None,
            limit_price: None,
            reduce_only: false,
            leverage: Some(dec!(5)),
        }
    }

    fn contract(symbol: &str, base: &str) -> ContractRow {
        ContractRow {
            symbol: symbol.into(),
            base_coin: base.into(),
            quote_coin: "USDT".into(),
            symbol_type: Some("perpetual".into()),
            symbol_status: Some("normal".into()),
            is_rwa: Some("NO".into()),
            support_margin_coins: vec!["USDT".into()],
            min_trade_num: Some("0.001".into()),
            price_end_step: Some("0.1".into()),
            volume_place: Some("3".into()),
            size_multiplier: Some("0.001".into()),
            min_trade_usdt: Some("5".into()),
            max_order_qty: Some("100".into()),
            min_lever: Some("1".into()),
            max_lever: Some("125".into()),
            buy_limit_price_ratio: Some("0.02".into()),
            sell_limit_price_ratio: Some("0.03".into()),
        }
    }

    #[test]
    fn signing_payload_follows_the_documented_concatenation() {
        assert_eq!(
            signing_payload(
                "1695806875837",
                "POST",
                "/api/v2/mix/order/place-order",
                "",
                "{\"a\":1}"
            ),
            "1695806875837POST/api/v2/mix/order/place-order{\"a\":1}"
        );
        assert_eq!(
            signing_payload(
                "1695806875837",
                "GET",
                "/api/v2/mix/order/detail",
                "symbol=BTCUSDT&productType=USDT-FUTURES",
                ""
            ),
            "1695806875837GET/api/v2/mix/order/detail?symbol=BTCUSDT&productType=USDT-FUTURES"
        );
    }

    #[test]
    fn hmac_and_base64_match_the_rfc4231_vector() {
        // RFC 4231 test case 2.
        assert_eq!(
            hex_lower(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            base64_standard(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "W9zBRr9gdU5qBCQmCJV1x1oAPwidJzmDnexYuWTsOEM="
        );
    }

    #[test]
    fn query_encoding_is_unreserved_percent_encoding() {
        assert_eq!(
            query_string(&[("symbol", "BTCUSDT".into()), ("value", "a b/c".into()),]),
            "symbol=BTCUSDT&value=a%20b%2Fc"
        );
    }

    #[test]
    fn client_ids_are_short_and_alphanumeric() {
        let id = ClientOrderId("very-long-internal-order-id-1234567890".into());
        let venue = venue_client_id(CLIENT_PREFIX, &id, CLIENT_MAX);
        assert!(venue.len() <= CLIENT_MAX && venue.len() > CLIENT_PREFIX.len());
        assert!(venue.starts_with(CLIENT_PREFIX));
        assert!(venue.chars().all(|c| c.is_ascii_alphanumeric()));
        // Deterministic across calls.
        assert_eq!(venue, venue_client_id(CLIENT_PREFIX, &id, CLIENT_MAX));
    }

    #[test]
    fn instrument_matching_uses_metadata_and_rejects_ambiguity() {
        let rows = vec![contract("BTCUSDT", "BTC"), contract("ETHUSDT", "ETH")];
        let instrument = select_instrument(&rows, &Symbol::perp("BTC", "USDT"), false).unwrap();
        assert_eq!(instrument.native, "BTCUSDT");
        assert!(!instrument.rwa);
        assert_eq!(instrument.tick, dec!(0.1));
        assert_eq!(instrument.step, dec!(0.001));
        assert_eq!(instrument.min_qty, dec!(0.001));
        assert_eq!(instrument.min_notional, dec!(5));
        assert_eq!(instrument.max_lever, dec!(125));
        assert_eq!(instrument.limit_ratio, Some(dec!(0.02)), "买卖两边取小");
        assert!(select_instrument(&rows, &Symbol::perp("SOL", "USDT"), false).is_err());
        // A non-perpetual or non-normal row must not match.
        let mut paused = contract("BTCUSDT", "BTC");
        paused.symbol_status = Some("off".into());
        assert!(select_instrument(&[paused], &Symbol::perp("BTC", "USDT"), true).is_err());
        // `limit_open` only takes closing orders.
        let mut closing_only = contract("BTCUSDT", "BTC");
        closing_only.symbol_status = Some("limit_open".into());
        let btc = Symbol::perp("BTC", "USDT");
        assert!(select_instrument(std::slice::from_ref(&closing_only), &btc, false).is_err());
        assert!(select_instrument(&[closing_only], &btc, true).is_ok());
    }

    #[test]
    fn quantities_follow_base_coin_units_and_exact_exits() {
        let mut order = order();
        // notional / price = 250/50000 = 0.005, floored to the 0.001 step.
        let units = order_units(
            Venue::Bitget,
            &order,
            dec!(50000),
            Decimal::ONE,
            dec!(0.001),
            dec!(0.001),
        )
        .unwrap();
        assert_eq!(units, dec!(0.005));
        // Reduce-only must be exactly on the step.
        order.reduce_only = true;
        order.quantity = Some(dec!(0.0055));
        assert!(
            order_units(
                Venue::Bitget,
                &order,
                dec!(50000),
                Decimal::ONE,
                dec!(0.001),
                dec!(0.001)
            )
            .is_err()
        );
        order.quantity = Some(dec!(0.005));
        assert_eq!(
            order_units(
                Venue::Bitget,
                &order,
                dec!(50000),
                Decimal::ONE,
                dec!(0.001),
                dec!(0.001)
            )
            .unwrap(),
            dec!(0.005)
        );
    }

    #[test]
    fn status_mapping_never_invents_terminality() {
        assert_eq!(map_state("live").unwrap(), OrderStatus::Open);
        assert_eq!(map_state("partially_filled").unwrap(), OrderStatus::Open);
        assert_eq!(map_state("filled").unwrap(), OrderStatus::Filled);
        assert_eq!(map_state("canceled").unwrap(), OrderStatus::Cancelled);
        assert!(map_state("whatever").is_err());
    }

    #[test]
    fn partial_ioc_is_not_reported_as_a_full_fill() {
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            verified_status(OrderStatus::Open, dec!(2), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert!(verified_status(OrderStatus::Open, dec!(2), dec!(3)).is_err());
    }

    #[test]
    fn fees_are_flipped_to_positive_cost_and_currency_checked() {
        let order = RemoteOrder {
            symbol: Some("BTCUSDT".into()),
            size: Some("1".into()),
            order_id: Some("7".into()),
            client_oid: Some("cid".into()),
            base_volume: Some("1".into()),
            price: Some("100".into()),
            state: Some("filled".into()),
            side: Some("buy".into()),
            reduce_only: Some("NO".into()),
        };
        let fill = |fee_coin: &str, total_fee: &str, base: &str, price: &str| FillRow {
            trade_id: Some(format!("t{fee_coin}{total_fee}")),
            c_time: Some("1792500000000".into()),
            symbol: Some("BTCUSDT".into()),
            order_id: Some("7".into()),
            price: Some(price.into()),
            base_volume: Some(base.into()),
            side: Some("buy".into()),
            fee_detail: vec![FeeDetail {
                fee_coin: Some(fee_coin.into()),
                total_fee: Some(total_fee.into()),
                deduction: Some("no".into()),
            }],
        };
        let fills = vec![
            fill("USDT", "-0.04", "0.4", "100"),
            fill("USDT", "-0.06", "0.6", "100"),
        ];
        let (qty, notional, fee) = aggregate_fills(&fills, &order).unwrap();
        assert_eq!(qty, dec!(1));
        assert_eq!(notional, dec!(100));
        assert_eq!(fee, dec!(0.1)); // 0.04 + 0.06 charges become positive cost
        // A rebate is preserved as a negative cost.
        let rebate = vec![fill("USDT", "0.02", "1", "100")];
        assert_eq!(aggregate_fills(&rebate, &order).unwrap().2, dec!(-0.02));
        // A non-USDT fee currency is refused rather than repriced.
        let foreign = vec![fill("BGB", "-0.04", "1", "100")];
        assert!(aggregate_fills(&foreign, &order).is_err());
    }

    #[test]
    fn native_symbols_map_back_to_the_scanner_symbol() {
        assert_eq!(
            symbol_from_native("btcusdt").unwrap(),
            Symbol::perp("BTC", "USDT")
        );
        assert_eq!(
            symbol_from_native("1000PEPEUSDT").unwrap(),
            Symbol::perp("1000PEPE", "USDT")
        );
        assert!(symbol_from_native("BTCUSDT_250926").is_err());
        assert!(symbol_from_native("BTCUSDC").is_err());
    }

    /// 「系统异常，请稍后再试」一类的码不代表订单没成：引擎可能已经受理，只是下单应答出了问题。
    /// 把它们记成 Rejected 会让执行器停止追踪一张可能已经成交的订单（唯一会藏起敞口的方向）。
    #[test]
    fn system_abnormal_codes_are_unknown_outcomes_not_refusals() {
        for unknown in [
            "40010", "45001", "40725", "40015", "40200", "12004", "", SUCCESS,
        ] {
            assert!(
                !definitive_refusal(unknown),
                "{unknown:?} 必须按结果未知处理"
            );
        }
        // 明确的业务拒绝（余额不足、参数错误、订单不存在）仍然是定论。
        for refusal in ["40762", "40808", "43001", "45110"] {
            assert!(definitive_refusal(refusal), "{refusal} 是明确拒绝");
        }
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    /// 美股假日照周末的口径：前一天 16:00 起、假日全天、后一天 4:00 前；提前收盘日 13:00 起。
    #[test]
    fn rwa_contracts_are_treated_as_closed_around_us_market_holidays() {
        // 2026-11-26 感恩节（周四，冬令时 UTC−5）。
        assert!(!rwa_session_closed(utc(2026, 11, 24, 17, 0)), "普通周二");
        assert!(
            !rwa_session_closed(utc(2026, 11, 25, 20, 59)),
            "前一天 15:59"
        );
        assert!(rwa_session_closed(utc(2026, 11, 25, 21, 0)), "前一天 16:00");
        assert!(rwa_session_closed(utc(2026, 11, 26, 15, 0)), "感恩节当天");
        assert!(rwa_session_closed(utc(2026, 11, 27, 8, 59)), "次日 3:59");
        assert!(!rwa_session_closed(utc(2026, 11, 27, 9, 0)), "次日 4:00 开");
        assert!(
            !rwa_session_closed(utc(2026, 11, 27, 17, 59)),
            "提前收盘日 12:59"
        );
        assert!(
            rwa_session_closed(utc(2026, 11, 27, 18, 0)),
            "提前收盘日 13:00"
        );
        // 2027-03-26 耶稣受难日（周五，夏令时 UTC−4）：周四 16:00 起一直到周一 4:00。
        assert!(!rwa_session_closed(utc(2027, 3, 25, 19, 59)));
        assert!(rwa_session_closed(utc(2027, 3, 25, 20, 0)));
        assert!(rwa_session_closed(utc(2027, 3, 26, 14, 0)));
        // 2027-01-18 马丁·路德·金日（周一）：周二 4:00 才开。
        assert!(rwa_session_closed(utc(2027, 1, 18, 15, 0)));
        assert!(rwa_session_closed(utc(2027, 1, 19, 8, 59)));
        assert!(!rwa_session_closed(utc(2027, 1, 19, 9, 0)));
        // 表里的日子都不是周末（观察日已经顺延过）。
        for &(year, month, day) in US_MARKET_HOLIDAYS.iter().chain(&US_MARKET_EARLY_CLOSES) {
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
            assert!(
                !matches!(date.weekday(), Weekday::Sat | Weekday::Sun),
                "{date}"
            );
        }
    }

    /// 周末只挡开仓、不挡平仓；美股假日两样都挡。
    #[test]
    fn a_weekend_blocks_only_opening_while_a_holiday_blocks_closing_too() {
        let saturday = utc(2026, 10, 31, 15, 0);
        assert!(rwa_closed_for(saturday, false), "周六不开新仓");
        assert!(!rwa_closed_for(saturday, true), "周六可以平仓");
        assert!(
            !rwa_closed_for(utc(2026, 10, 26, 7, 0), true),
            "周一凌晨可以平仓"
        );
        let thanksgiving = utc(2026, 11, 26, 15, 0);
        assert!(rwa_closed_for(thanksgiving, false) && rwa_closed_for(thanksgiving, true));
        assert!(
            rwa_closed_for(utc(2026, 11, 25, 21, 0), true),
            "感恩节前一天 16:00 起"
        );
        assert!(!rwa_closed_for(utc(2026, 11, 24, 17, 0), true), "普通周二");
        assert!(
            rwa_closed_note(true).contains("假日") && rwa_closed_note(false).contains("不开新仓")
        );
    }

    /// 股票类合约 5×24：纽约时间周五 16:00 到周一 4:00 一律按休市（夏令时、冬令时各验一遍边界）。
    #[test]
    fn rwa_contracts_are_treated_as_closed_from_friday_close_to_monday_morning() {
        // 2026-10-23 周五，夏令时（UTC−4）：15:59 还开着，16:00 起休市。
        assert!(!rwa_session_closed(utc(2026, 10, 23, 19, 59)));
        assert!(rwa_session_closed(utc(2026, 10, 23, 20, 0)));
        assert!(rwa_session_closed(utc(2026, 10, 24, 12, 0)), "周六");
        assert!(rwa_session_closed(utc(2026, 10, 25, 23, 0)), "周日晚上");
        // 周一 4:00 起才算开。
        assert!(rwa_session_closed(utc(2026, 10, 26, 7, 59)));
        assert!(!rwa_session_closed(utc(2026, 10, 26, 8, 0)));
        assert!(!rwa_session_closed(utc(2026, 10, 21, 12, 0)), "周三");
        // 冬令时（UTC−5）：周五 16:00 = UTC 21:00。
        assert!(!rwa_session_closed(utc(2026, 11, 6, 20, 59)));
        assert!(rwa_session_closed(utc(2026, 11, 6, 21, 0)));
    }

    /// 费率因合约而异（比如股票合约活动价）时不再拒绝连接 —— 那会拖垮整个实盘交易台 —— 而是按最高的算。
    #[test]
    fn a_non_uniform_account_fee_falls_back_to_the_highest_rate() {
        let rows = |rates: &[&str]| -> Vec<TradeRate> {
            rates
                .iter()
                .map(|rate| TradeRate {
                    taker_fee_rate: rate.to_string(),
                })
                .collect()
        };
        assert_eq!(
            highest_taker_fee(&rows(&["0.0006", "0.0006"])).unwrap(),
            (dec!(0.0006), true)
        );
        assert_eq!(
            highest_taker_fee(&rows(&["0.0006", "0", "0.0004"])).unwrap(),
            (dec!(0.0006), false)
        );
        assert!(highest_taker_fee(&rows(&[])).is_err());
        assert!(highest_taker_fee(&rows(&["0", "0"])).is_err());
        assert!(highest_taker_fee(&rows(&["-0.0001"])).is_err());
    }

    /// 股票合约限价只能偏离市价 2%：没带限价的平仓 / 回滚单，价格保护不超过这个带宽的九成。
    #[test]
    fn price_protection_stays_inside_the_contract_band() {
        let options = |slippage: &str| LiveOptions {
            trading_enabled: true,
            market_slippage: Some(slippage.parse().unwrap()),
        };
        assert_eq!(
            banded(&options("0.05"), Some(dec!(0.02))).market_slippage,
            Some(dec!(0.018))
        );
        assert_eq!(
            banded(&options("0.005"), Some(dec!(0.02))).market_slippage,
            Some(dec!(0.005)),
            "配置的更紧就用配置的"
        );
        assert_eq!(
            banded(&options("0.05"), None).market_slippage,
            Some(dec!(0.05))
        );
        let price = banded(&options("0.05"), Some(dec!(0.02)))
            .bound_price(Venue::Bitget, Side::Buy, Some(dec!(99)), Some(dec!(100)))
            .unwrap();
        assert_eq!(price, dec!(101.8));
        assert_eq!(
            limit_ratio(Some("0.02"), Some("0.15")).unwrap(),
            Some(dec!(0.02))
        );
        assert_eq!(limit_ratio(Some(""), Some("0")).unwrap(), None);
    }

    #[test]
    fn contract_status_decides_what_may_still_be_sent() {
        assert_eq!(status_halt(Some("normal"), false), None);
        assert_eq!(status_halt(Some("normal"), true), None);
        // 只能平不能开。
        assert!(status_halt(Some("limit_open"), false).is_some());
        assert_eq!(status_halt(Some("limit_open"), true), None);
        for closed in ["maintain", "restrictedAPI", "off", "listed"] {
            assert!(
                status_halt(Some(closed), true).is_some(),
                "{closed} 连平仓都不行"
            );
        }
        assert!(status_halt(None, true).is_some(), "没报状态不当成能下");
    }

    fn account_row(mode: &str, long: &str, short: &str, cross: &str) -> AccountRow {
        AccountRow {
            margin_coin: Some("USDT".into()),
            margin_mode: Some(mode.into()),
            pos_mode: Some("one_way_mode".into()),
            asset_mode: Some("single".into()),
            isolated_long_lever: Some(json!(long)),
            isolated_short_lever: Some(json!(short)),
            cross_margin_leverage: Some(json!(cross)),
        }
    }

    /// 已经是这个杠杆就不写：省下设杠杆和读回的两次往返（第二腿就在等它们）。
    #[test]
    fn leverage_already_in_force_is_recognised_per_margin_mode() {
        let isolated = account_row("isolated", "3", "3", "20");
        assert_eq!(
            mode_in_force(&isolated, dec!(3)).unwrap(),
            Some(crate::MarginMode::Isolated)
        );
        assert_eq!(mode_in_force(&isolated, dec!(5)).unwrap(), None);
        // 两边杠杆不一样：要设。
        let lopsided = account_row("isolated", "3", "5", "20");
        assert_eq!(mode_in_force(&lopsided, dec!(3)).unwrap(), None);
        let crossed = account_row("crossed", "3", "3", "5");
        assert_eq!(
            mode_in_force(&crossed, dec!(5)).unwrap(),
            Some(crate::MarginMode::Cross)
        );
        assert_eq!(mode_in_force(&crossed, dec!(3)).unwrap(), None);
        let unknown = account_row("", "3", "3", "3");
        assert_eq!(mode_in_force(&unknown, dec!(3)).unwrap(), None);
    }

    #[test]
    fn free_collateral_is_the_smaller_openable_balance_and_never_negative() {
        let row = |coin: &str, available: &str, crossed: Option<&str>, isolated: Option<&str>| {
            AccountListRow {
                margin_coin: Some(coin.into()),
                available: Some(available.into()),
                crossed_max_available: crossed.map(Into::into),
                isolated_max_available: isolated.map(Into::into),
            }
        };
        assert_eq!(
            free_collateral_of(&[row("USDT", "900", Some("820.5"), Some("800.25"))]).unwrap(),
            dec!(800.25)
        );
        assert_eq!(
            free_collateral_of(&[row("USDT", "900", None, None)]).unwrap(),
            dec!(900)
        );
        assert_eq!(
            free_collateral_of(&[row("USDT", "1", Some("-3"), Some("-2"))]).unwrap(),
            Decimal::ZERO
        );
        assert_eq!(
            free_collateral_of(&[row("BTC", "5", Some("5"), Some("5"))]).unwrap(),
            Decimal::ZERO,
            "没有 USDT 账户 = 没钱可用"
        );
    }

    #[test]
    fn history_windows_are_contiguous_inclusive_and_bounded() {
        assert_eq!(windows(0, 25, 10), vec![(0, 10), (11, 21), (22, 25)]);
        assert_eq!(windows(5, 5, 10), vec![(5, 5)]);
        assert!(windows(6, 5, 10).is_empty());
        let now = 100 * 24 * 60 * 60 * 1_000;
        assert!(within_retention(now - HISTORY_RETENTION_MS, now).is_ok());
        assert!(within_retention(now - HISTORY_RETENTION_MS - 1, now).is_err());
    }

    fn bill(id: &str, symbol: &str, amount: &str, at: &str) -> BillRow {
        BillRow {
            bill_id: Some(id.into()),
            symbol: Some(symbol.into()),
            amount: Some(amount.into()),
            business_type: Some(BILL_FUNDING.into()),
            coin: Some("USDT".into()),
            c_time: Some(at.into()),
        }
    }

    /// 流水接口不能按合约筛：别的合约的跳过，本合约的带符号记（正 = 收到），窗口外的、类型不对的报错。
    #[test]
    fn funding_bills_keep_the_symbol_and_the_sign() {
        let window = (1_000, 2_000);
        let (id, at, amount) = funding_entry(
            &bill("9", "NVDAUSDT", "-0.0123", "1500"),
            "NVDAUSDT",
            window,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            (id.as_str(), at.timestamp_millis(), amount),
            ("9", 1500, dec!(-0.0123))
        );
        assert!(
            funding_entry(&bill("8", "TSLAUSDT", "1", "1500"), "NVDAUSDT", window)
                .unwrap()
                .is_none()
        );
        assert!(funding_entry(&bill("7", "NVDAUSDT", "1", "2001"), "NVDAUSDT", window).is_err());
        let mut fee = bill("6", "NVDAUSDT", "1", "1500");
        fee.business_type = Some("open_long".into());
        assert!(funding_entry(&fee, "NVDAUSDT", window).is_err());
        let mut coin = bill("5", "NVDAUSDT", "1", "1500");
        coin.coin = Some("BGB".into());
        assert!(funding_entry(&coin, "NVDAUSDT", window).is_err());
        let total = FundingTotal::from_rows(
            [
                (utc_millis(1500).unwrap(), dec!(-0.0123)),
                (utc_millis(1800).unwrap(), dec!(0.02)),
            ],
            utc_millis(1000).unwrap(),
        );
        assert_eq!((total.usdt, total.payments), (dec!(0.0077), 2));
    }

    fn history_fill(id: &str, at: &str, side: &str, fee_coin: &str, total_fee: &str) -> FillRow {
        FillRow {
            trade_id: Some(id.into()),
            c_time: Some(at.into()),
            symbol: Some("NVDAUSDT".into()),
            order_id: Some("1".into()),
            price: Some("180.5".into()),
            base_volume: Some("2".into()),
            side: Some(side.into()),
            fee_detail: vec![FeeDetail {
                fee_coin: Some(fee_coin.into()),
                total_fee: Some(total_fee.into()),
                deduction: Some("no".into()),
            }],
        }
    }

    #[test]
    fn fill_history_becomes_venue_fills_oldest_first_with_positive_costs() {
        let fills = venue_fills(vec![
            history_fill("b", "2000", "Sell", "USDT", "-0.2166"),
            history_fill("a", "1000", "buy", "USDT", "-0.2166"),
        ])
        .unwrap();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].at.timestamp_millis(), 1000);
        assert_eq!(fills[0].side, Side::Buy);
        assert_eq!(fills[1].side, Side::Sell, "大小写都认");
        assert_eq!(fills[0].quantity, dec!(2));
        assert_eq!(fills[0].price, dec!(180.5));
        assert_eq!(fills[0].fee_usdt, dec!(0.2166), "扣费是负数，记成正的成本");
        assert!(venue_fills(vec![history_fill("c", "1", "buy", "BGB", "-0.1")]).is_err());
    }

    #[test]
    fn margin_replies_separate_applied_refused_and_unknown() {
        use reqwest::StatusCode;
        let reply = |code: &str| json!({"code": code, "msg": "m", "data": ""});
        assert_eq!(
            classify_margin_reply(StatusCode::OK, &reply(SUCCESS)),
            MarginOutcome::Applied
        );
        assert!(matches!(
            classify_margin_reply(StatusCode::BAD_REQUEST, &reply("40762")),
            MarginOutcome::Refused(_)
        ));
        assert!(matches!(
            classify_margin_reply(StatusCode::OK, &reply("40010")),
            MarginOutcome::Unknown(_)
        ));
        assert!(matches!(
            classify_margin_reply(StatusCode::BAD_GATEWAY, &reply("40762")),
            MarginOutcome::Unknown(_)
        ));
        assert!(matches!(
            classify_margin_reply(StatusCode::BAD_REQUEST, &json!({})),
            MarginOutcome::Unknown(_)
        ));
        assert_eq!(margin_amount_text(dec!(12.50)).unwrap(), "12.5");
        assert!(margin_amount_text(dec!(1.234)).is_err());
        assert!(margin_amount_text(Decimal::ZERO).is_err());
    }

    #[test]
    fn margin_is_added_only_to_an_open_isolated_one_way_position() {
        let row = |mode: &str, side: &str, total: &str| PositionRow {
            symbol: Some("NVDAUSDT".into()),
            total: Some(total.into()),
            hold_side: Some(side.into()),
            open_price_avg: Some("180".into()),
            mark_price: Some("181".into()),
            pos_mode: Some("one_way_mode".into()),
            margin_mode: Some(mode.into()),
            liquidation_price: None,
            margin_size: None,
        };
        assert_eq!(
            isolated_hold_side(&[row("isolated", "short", "2")], "NVDAUSDT").unwrap(),
            "short"
        );
        assert!(isolated_hold_side(&[row("crossed", "long", "2")], "NVDAUSDT").is_err());
        assert!(isolated_hold_side(&[row("isolated", "long", "0")], "NVDAUSDT").is_err());
        assert!(isolated_hold_side(&[row("isolated", "long", "2")], "TSLAUSDT").is_err());
    }

    /// Read-only probe of the real account in `ARB_BITGET_API_KEY` / `ARB_BITGET_API_SECRET` /
    /// `ARB_BITGET_PASSPHRASE`: no write request is possible (trading disabled). Runs the very
    /// `connect` the live desk runs (all-or-nothing: if it fails here, the whole live desk would
    /// fail to connect), then every read the bot relies on — even when `connect` failed — with
    /// cold and cached timings. Journal in `ARB_PROBE_DIR` (default: the system temp dir).
    /// Credentials come from the environment or, failing that, the repo `.env`. Run
    /// with `cargo test -p arb-exec --lib bitget_broker::tests::live_read_only_probe -- --ignored
    /// --nocapture`.
    #[tokio::test]
    #[ignore = "needs live Bitget credentials"]
    async fn live_read_only_probe() {
        use std::time::Instant;
        let _ = dotenvy::dotenv();
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let credentials = || BitgetCredentials {
            api_key: var("ARB_BITGET_API_KEY").expect("ARB_BITGET_API_KEY"),
            api_secret: var("ARB_BITGET_API_SECRET").expect("ARB_BITGET_API_SECRET"),
            passphrase: var("ARB_BITGET_PASSPHRASE").expect("ARB_BITGET_PASSPHRASE"),
        };
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let journal_path = dir.join("arb-live-bitget-probe-orders.jsonl");
        let options = LiveOptions {
            trading_enabled: false,
            market_slippage: None,
        };
        let client = reqwest::Client::new();
        let ms = |since: Instant| since.elapsed().as_millis();
        let started = Instant::now();
        let connected = BitgetBroker::connect(
            client.clone(),
            credentials(),
            &journal_path,
            options.clone(),
        )
        .await;
        println!(
            "connect (what the live desk runs): {} ({} ms)",
            match &connected {
                Ok(broker) => format!("OK, account taker fee {}", broker.fee_per_side),
                Err(error) => format!("FAILED — the live desk would not connect: {error}"),
            },
            ms(started)
        );
        let broker = match connected {
            Ok(broker) => broker,
            Err(_) => {
                // Keep reading so the report says what else is wrong.
                let identity = "bitget-probe-raw".to_string();
                let mut broker = BitgetBroker {
                    client,
                    credentials: credentials(),
                    options,
                    journal: SyncMutex::new(
                        OrderJournal::open(&dir.join("arb-live-bitget-probe-raw.jsonl"), &identity)
                            .unwrap(),
                    ),
                    submit: Mutex::new(()),
                    fee_per_side: Decimal::ZERO,
                    time_offset_ms: 0,
                    contracts: Cached::new(),
                    settings: Verified::new(SETTINGS_TTL),
                };
                broker.time_offset_ms = broker.server_offset().await.unwrap_or(0);
                broker
            }
        };
        println!("clock offset: {} ms", broker.time_offset_ms);
        let rates: ArbResult<Vec<TradeRate>> = broker
            .signed_data(
                Method::GET,
                PATH_TRADE_RATE,
                &[("businessType", "mix".to_string())],
                None,
            )
            .await;
        match rates {
            Ok(rows) => {
                let mut distinct: Vec<&str> =
                    rows.iter().map(|row| row.taker_fee_rate.as_str()).collect();
                distinct.sort_unstable();
                distinct.dedup();
                println!(
                    "trade-rate rows: {} (distinct taker rates: {distinct:?}; connect needs exactly one)",
                    rows.len()
                );
            }
            Err(error) => println!("trade-rate: {error}"),
        }
        let account: ArbResult<AccountRow> = broker
            .signed_data(
                Method::GET,
                PATH_ACCOUNT,
                &[
                    ("symbol", "BTCUSDT".to_string()),
                    ("productType", PRODUCT.to_string()),
                    ("marginCoin", MARGIN_COIN.to_string()),
                ],
                None,
            )
            .await;
        match account {
            Ok(row) => println!(
                "account: posMode {:?} (needs one_way_mode), assetMode {:?} (needs single), BTCUSDT marginMode {:?}",
                row.pos_mode, row.asset_mode, row.margin_mode
            ),
            Err(error) => println!("account: {error}"),
        }
        // BGB fee deduction must be off (a BGB-denominated fee leaves an order unaccountable).
        // A futures-only key may not be allowed to read this spot setting: then confirm by hand.
        let deduct: ArbResult<Value> = broker
            .signed_data(Method::GET, "/api/v2/spot/account/deduct-info", &[], None)
            .await;
        println!("BGB fee deduction (must be off): {deduct:?}");
        println!("free collateral: {:?}", broker.free_collateral().await);
        broker.contracts.clear();
        let nvda = Symbol::perp("NVDA", "USDT");
        for round in ["cold", "cached"] {
            let started = Instant::now();
            let instrument = broker.instrument(&nvda).await;
            println!(
                "NVDAUSDT instrument ({round}): {:?} ({} ms)",
                instrument.map(|i| (i.native, i.rwa, i.tick, i.step, i.max_lever)),
                ms(started)
            );
        }
        for round in ["cold", "cached"] {
            let started = Instant::now();
            let (positions, orders) = tokio::join!(broker.positions(), broker.open_orders());
            println!(
                "positions + open orders ({round}): {:?} / {:?} ({} ms)",
                positions.map(|rows| rows
                    .iter()
                    .map(|row| (row.symbol.base.clone(), row.net_quantity))
                    .collect::<Vec<_>>()),
                orders.map(|rows| rows.len()),
                ms(started)
            );
        }
        let btc = Symbol::perp("BTC", "USDT");
        for symbol in [&nvda, &btc] {
            println!(
                "{} halted now (open / close): {:?} / {:?}",
                symbol.base,
                broker.trading_halted(symbol, false).await,
                broker.trading_halted(symbol, true).await
            );
        }
        for round in ["first", "second"] {
            let started = Instant::now();
            let warmed = broker.warm_reads(&nvda, Some(dec!(3))).await;
            println!(
                "warm_reads(NVDA, 3x) {round}: {warmed:?} ({} ms)",
                ms(started)
            );
        }
        for mode in [crate::MarginMode::Isolated, crate::MarginMode::Cross] {
            println!(
                "NVDAUSDT 3x {} recorded as in force: {}",
                mode.as_str(),
                broker
                    .settings
                    .is_fresh(&("NVDAUSDT".to_string(), dec!(3), mode))
            );
        }
        println!("NVDA leg state: {:?}", broker.leg_state(&nvda).await);
        println!(
            "order lookup of an unknown id: {:?}",
            broker
                .fetch_order("NVDAUSDT", "bgprobeunknown")
                .await
                .map(|row| row.is_some())
        );
        let now = chrono::Utc::now();
        let started = Instant::now();
        println!(
            "NVDA funding since 30 days: {:?} ({} ms)",
            broker
                .funding_since(&nvda, now - chrono::Duration::days(30))
                .await,
            ms(started)
        );
        for days in [1, 20] {
            let started = Instant::now();
            let fills = broker
                .fills_between(&nvda, now - chrono::Duration::days(days), now)
                .await;
            println!(
                "NVDA fills over {days} day(s): {:?} ({} ms)",
                fills.map(|rows| rows.map(|rows| rows.len())),
                ms(started)
            );
        }
        println!(
            "funding since 100 days (beyond retention, must refuse): {:?}",
            broker
                .funding_since(&nvda, now - chrono::Duration::days(100))
                .await
                .map(|total| total.map(|total| total.payments))
        );
    }
}
