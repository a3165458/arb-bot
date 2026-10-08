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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Mutex as SyncMutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, base64_standard, hex_lower, hmac_sha256, order_units,
    round_price, transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE: &str = "https://api.bitget.com";
const PRODUCT: &str = "USDT-FUTURES";
const MARGIN_COIN: &str = "USDT";
/// Bitget success code; it is NOT the HTTP status (errors come back as HTTP 200 + a code).
const SUCCESS: &str = "00000";
/// "order does not exist"; the documented code returned by order-detail for an unknown id.
const CODE_NOT_FOUND: &str = "43001";
/// `clientOid` charset is alphanumeric; the strictest published length is 32.
const CLIENT_PREFIX: &str = "bg";
const CLIENT_MAX: usize = 32;
const PATH_TIME: &str = "/api/v2/public/time";
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
        };
        broker.time_offset_ms = broker.server_offset().await?;
        let contracts: Vec<ContractRow> = broker
            .public_data(PATH_CONTRACTS, &[("productType", PRODUCT.to_string())])
            .await?;
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

    /// The account's actual taker fee, as reported by the signed trade-rate endpoint. The
    /// endpoint is per symbol, so the account rate must be uniform across the product line;
    /// a non-uniform answer cannot be represented by a single `fee_per_side` and fails closed.
    async fn account_taker_fee(&self) -> ArbResult<Decimal> {
        let rates: Vec<TradeRate> = self
            .signed_data(
                Method::GET,
                PATH_TRADE_RATE,
                &[("businessType", "mix".to_string())],
                None,
            )
            .await?;
        let mut iter = rates.iter();
        let first = iter
            .next()
            .ok_or_else(|| error("account taker fee rate is unavailable"))?;
        let rate = dec(&first.taker_fee_rate)?;
        if rate <= Decimal::ZERO {
            return Err(error("account taker fee rate is not positive"));
        }
        for row in iter {
            if dec(&row.taker_fee_rate)? != rate {
                return Err(error(
                    "account taker fee rate differs across symbols; a single fee_per_side \
                     cannot represent it",
                ));
            }
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

    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Instrument> {
        let rows: Vec<ContractRow> = self
            .public_data(PATH_CONTRACTS, &[("productType", PRODUCT.to_string())])
            .await?;
        select_instrument(&rows, symbol)
    }

    // ---- Order helpers ------------------------------------------------------------------

    /// Bounded limit price for an order without an explicit limit, from a fresh book.
    async fn bound_price(&self, native: &str, side: Side) -> ArbResult<Decimal> {
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
        self.options.bound_price(Venue::Bitget, side, bid, ask)
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
        let fills = self.order_fills(remote).await?;
        let (filled_qty, filled_notional, fee) = aggregate_fills(&fills, remote)?;
        if filled_qty != executed {
            return Err(error(
                "fills do not sum to the executed quantity; refusing to infer fees",
            ));
        }
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
        Ok(())
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
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let native = self.instrument(symbol).await?.native;
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

    fn venue(&self) -> Venue {
        Venue::Bitget
    }

    fn fee_per_side(&self) -> Decimal {
        self.fee_per_side
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
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

        let instrument = self.instrument(&order.symbol).await?;
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
            None => self.bound_price(&instrument.native, order.side).await?,
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
struct ServerTime {
    server_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractRow {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    symbol_type: Option<String>,
    symbol_status: Option<String>,
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
struct TradeRate {
    taker_fee_rate: String,
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
struct FillRow {
    trade_id: Option<String>,
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

struct Instrument {
    native: String,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
    max_qty: Option<Decimal>,
    min_lever: Decimal,
    max_lever: Decimal,
}

/// The signed message, in the exact documented order. The query string is appended without
/// its leading `?`; the `?` is inserted only when a query is present.
fn signing_payload(timestamp: &str, method: &str, path: &str, query: &str, body: &str) -> String {
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

fn query_string(params: &[(&str, String)]) -> String {
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

fn sensitive(value: &str) -> ArbResult<HeaderValue> {
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
    !matches!(
        code,
        SUCCESS | "" | "40010" | "45001" | "40725" | "40015" | "40200" | "12004"
    )
}

fn envelope_code(envelope: &Value) -> &str {
    envelope.get("code").and_then(Value::as_str).unwrap_or("")
}

fn check_ok(envelope: &Value) -> ArbResult<()> {
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

fn data_of<T: DeserializeOwned>(envelope: Value) -> ArbResult<T> {
    let data = envelope
        .get("data")
        .cloned()
        .ok_or_else(|| error("venue response is missing data"))?;
    if data.is_null() {
        return Err(error("venue response data is null"));
    }
    serde_json::from_value(data).map_err(|_| error("malformed venue response"))
}

fn api_error(code: &str, message: &str) -> ArbError {
    let message: String = message
        .chars()
        .filter(|c| !c.is_control())
        .take(200)
        .collect();
    error(format!("venue error code {code}: {message}"))
}

fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue("bitget", message)
}

fn parse_side(raw: &str) -> ArbResult<Side> {
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

fn verified_status(
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

fn normalized(value: Option<&str>) -> Option<String> {
    value
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
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

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
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

fn external_id(venue_order_id: &str) -> ClientOrderId {
    ClientOrderId(format!("bitget-external-{venue_order_id}"))
}

/// Reconstruct the scanner's `Symbol` from a native symbol such as `BTCUSDT`. Delivery
/// contracts (`BTCUSDT_250926`) are rejected rather than silently hidden.
fn symbol_from_native(native: &str) -> ArbResult<Symbol> {
    let upper = native.trim().to_ascii_uppercase();
    if upper.contains('_') || !upper.ends_with("USDT") || upper.len() <= 4 {
        return Err(error("order/position is not a plain USDT perpetual"));
    }
    Ok(Symbol::perp(&upper[..upper.len() - 4], "USDT"))
}

/// Select the single tradable USDT-margined perp whose metadata maps to `symbol`. Matching is
/// by the live instrument metadata (base/quote/margin/symbolType/status), never by string
/// guessing alone; any ambiguity fails closed.
fn select_instrument(rows: &[ContractRow], symbol: &Symbol) -> ArbResult<Instrument> {
    let native = format!("{}{}", symbol.base, symbol.quote);
    let mut found = None;
    for row in rows {
        if row.quote_coin.eq_ignore_ascii_case("USDT")
            && row.symbol.eq_ignore_ascii_case(&native)
            && row.base_coin.eq_ignore_ascii_case(&symbol.base)
            && row.symbol_type.as_deref() == Some("perpetual")
            && row.symbol_status.as_deref() == Some("normal")
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
        tick,
        step,
        min_qty,
        min_notional,
        max_qty,
        min_lever,
        max_lever,
    })
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

fn dec(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| error("invalid decimal in venue data"))
}

fn opt_dec(raw: Option<&str>) -> ArbResult<Option<Decimal>> {
    match raw {
        Some(value) if !value.trim().is_empty() => dec(value).map(Some),
        _ => Ok(None),
    }
}

fn nonneg(raw: Option<&str>) -> ArbResult<Decimal> {
    let value = opt_dec(raw)?.unwrap_or(Decimal::ZERO);
    if value < Decimal::ZERO {
        return Err(error("venue reported a negative quantity"));
    }
    Ok(value)
}

fn value_dec(value: &Value) -> ArbResult<Decimal> {
    if let Some(text) = value.as_str() {
        dec(text)
    } else if value.is_number() {
        dec(&value.to_string())
    } else {
        Err(error("expected a decimal value in venue data"))
    }
}

fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b)
        .ok_or_else(|| error("decimal multiplication overflow"))
}

fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b)
        .ok_or_else(|| error("decimal addition overflow"))
}

fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b)
        .ok_or_else(|| error("decimal division overflow or zero divisor"))
}

fn now_ms() -> i64 {
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
            support_margin_coins: vec!["USDT".into()],
            min_trade_num: Some("0.001".into()),
            price_end_step: Some("0.1".into()),
            volume_place: Some("3".into()),
            size_multiplier: Some("0.001".into()),
            min_trade_usdt: Some("5".into()),
            max_order_qty: Some("100".into()),
            min_lever: Some("1".into()),
            max_lever: Some("125".into()),
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
        let instrument = select_instrument(&rows, &Symbol::perp("BTC", "USDT")).unwrap();
        assert_eq!(instrument.native, "BTCUSDT");
        assert_eq!(instrument.tick, dec!(0.1));
        assert_eq!(instrument.step, dec!(0.001));
        assert_eq!(instrument.min_qty, dec!(0.001));
        assert_eq!(instrument.min_notional, dec!(5));
        assert_eq!(instrument.max_lever, dec!(125));
        assert!(select_instrument(&rows, &Symbol::perp("SOL", "USDT")).is_err());
        // A non-perpetual or non-normal row must not match.
        let mut paused = contract("BTCUSDT", "BTC");
        paused.symbol_status = Some("off".into());
        assert!(select_instrument(&[paused], &Symbol::perp("BTC", "USDT")).is_err());
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
}
