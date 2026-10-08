//! Production Bybit **v5 linear USDT perpetual** broker (mainnet `https://api.bybit.com`).
//!
//! Scope: only USDT-margined linear perpetuals. `category=linear` also carries USDC
//! perpetuals and dated futures, so every instrument is matched against live
//! `instruments-info` metadata (`contractType=LinearPerpetual`, `quoteCoin=USDT`,
//! `settleCoin=USDT`, `status=Trading`, not pre-listing) rather than string guessing.
//!
//! Account-settings handling is read-only and fails closed: the broker NEVER changes
//! account-wide settings. A Unified Trading Account must already be in
//! `ISOLATED_MARGIN` (`/v5/account/info`); a Classic account is switched **per symbol**
//! at order time via `/v5/position/switch-isolated`. Position mode is a per-symbol/coin
//! setting on Bybit (system default one-way), so orders always carry `positionIdx=0`
//! and hedge-mode positions are rejected during reconciliation.
//!
//! Every order is LIMIT + `timeInForce=IOC` with a price bounded from a fresh book when
//! the intent has no limit price. Opening quantities round down; reduce-only quantities
//! must be exact and are checked against the live position. The exclusively locked intent
//! journal is fsynced BEFORE `/v5/order/create`; a recorded `orderLinkId` is never
//! resubmitted. Status and fees are read back from the venue (order record + execution
//! list), never inferred from the create acknowledgement.
//!
//! Primary sources (field names and signing string are load-bearing):
//! - Auth/headers: <https://bybit-exchange.github.io/docs/v5/guide>
//! - Place order: <https://bybit-exchange.github.io/docs/v5/order/create-order>
//! - Open & closed orders: <https://bybit-exchange.github.io/docs/v5/order/open-order>
//! - Order history: <https://bybit-exchange.github.io/docs/v5/order/order-list>
//! - Cancel order: <https://bybit-exchange.github.io/docs/v5/order/cancel-order>
//! - Trade history (fees): <https://bybit-exchange.github.io/docs/v5/order/execution>
//! - Instruments info: <https://bybit-exchange.github.io/docs/v5/market/instrument>
//! - Orderbook: <https://bybit-exchange.github.io/docs/v5/market/orderbook>
//! - Server time: <https://bybit-exchange.github.io/docs/v5/market/time>
//! - Fee rate: <https://bybit-exchange.github.io/docs/v5/account/fee-rate>
//! - Account info: <https://bybit-exchange.github.io/docs/v5/account/account-info>
//! - Set margin mode: <https://bybit-exchange.github.io/docs/v5/account/set-margin-mode>
//! - Set leverage: <https://bybit-exchange.github.io/docs/v5/position/leverage>
//! - Switch cross/isolated: <https://bybit-exchange.github.io/docs/v5/position/cross-isolate>
//! - Switch position mode: <https://bybit-exchange.github.io/docs/v5/position/position-mode>
//! - Position info: <https://bybit-exchange.github.io/docs/v5/position>
//! - Enums: <https://bybit-exchange.github.io/docs/v5/enum>

use std::path::Path;
use std::str::FromStr;

use arb_core::{ArbError, ArbResult, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, hex_lower, hmac_sha256, order_units, round_price,
    transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE_URL: &str = "https://api.bybit.com";
const CATEGORY: &str = "linear";
const QUOTE: &str = "USDT";

use std::time::{Duration, Instant};
/// Bybit's default validity window; the signed string must contain this exact value.
const RECV_WINDOW: &str = "5000";
/// Deterministic venue namespace; the journal identity provides account separation.
const LINK_PREFIX: &str = "bybit-";
/// Marks a client id synthesized for an order the process did not place.
const EXTERNAL_PREFIX: &str = "bybit-external-";
/// Guard against a paginated endpoint that keeps returning the same cursor.
const MAX_PAGES: usize = 100;

/// Secrets intentionally implement neither `Debug` nor `Serialize`.
pub struct BybitCredentials {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AccountType {
    /// Normal account: isolated/cross is a per-symbol setting.
    Classic,
    /// Unified Trading Account: margin mode is account-wide (read-only for us).
    Unified,
    UnifiedCross,
}

pub struct BybitBroker {
    client: Client,
    api_key: String,
    api_secret: String,
    journal: Mutex<OrderJournal>,
    /// Serializes submissions so two `place` calls cannot race the journal.
    submit: Mutex<()>,
    options: LiveOptions,
    account: AccountType,
    /// `server_time - local_time`; Bybit requires `timestamp` inside a tight window.
    time_offset_ms: i64,
    taker_fee: Decimal,
}

impl BybitBroker {
    /// Validates options, locks the intent journal, then performs read-only authenticated
    /// checks: server time skew, account/margin-mode compatibility, and the account's
    /// actual taker fee rate. No signed write is sent here (not even leverage).
    pub async fn connect(
        client: Client,
        credentials: BybitCredentials,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(Venue::Bybit)?;
        let identity = format!(
            "bybit:{}",
            &hex_lower(&Sha256::digest(credentials.api_key.as_bytes()))[..16]
        );
        let journal = OrderJournal::open(journal_path, &identity)?;
        let mut broker = Self {
            client,
            api_key: credentials.api_key,
            api_secret: credentials.api_secret,
            journal: Mutex::new(journal),
            submit: Mutex::new(()),
            options,
            // Placeholder; overwritten below before the broker is handed out.
            account: AccountType::Unified,
            time_offset_ms: 0,
            taker_fee: Decimal::ZERO,
        };

        let started = Utc::now().timestamp_millis();
        let server = broker.server_time_ms().await?;
        let ended = Utc::now().timestamp_millis();
        if ended - started > 2000 {
            return Err(error("server-time round trip exceeded two seconds"));
        }
        broker.time_offset_ms = server - (started + (ended - started) / 2);
        let info: AccountInfo = checked(broker.signed_get("/v5/account/info", &[]).await?)?;
        broker.account = account_type(&info)?;

        // Position mode is per symbol on Bybit and has no account-wide read: every open and
        // exit re-checks its own symbol (`position_for` rejects hedge mode), and
        // reconciliation rejects any hedge-mode position row.
        broker.positions().await?;

        let fees: FeeRateResult = checked(
            broker
                .signed_get(
                    "/v5/account/fee-rate",
                    &[("category", CATEGORY.to_string())],
                )
                .await?,
        )?;
        let mut taker: Option<Decimal> = None;
        for row in &fees.list {
            let rate = decimal(&row.taker_fee_rate)?;
            if let Some(previous) = taker {
                if previous != rate {
                    return Err(error(
                        "Bybit taker fee rate differs across contracts; refusing to guess",
                    ));
                }
            } else {
                taker = Some(rate);
            }
        }
        let taker = taker.ok_or_else(|| error("Bybit account taker fee rate is unavailable"))?;
        if taker < Decimal::ZERO {
            return Err(error("Bybit reported a negative taker fee rate"));
        }
        broker.taker_fee = taker;
        Ok(broker)
    }

    fn sign(&self, timestamp: u64, payload: &str) -> String {
        signature(&self.api_secret, timestamp, &self.api_key, payload)
    }

    fn timestamp(&self) -> ArbResult<u64> {
        let adjusted = Utc::now().timestamp_millis() + self.time_offset_ms;
        if adjusted < 0 {
            return Err(error("local clock is far ahead of the Bybit server"));
        }
        Ok(adjusted as u64)
    }

    async fn server_time_ms(&self) -> ArbResult<i64> {
        let result: TimeResult = checked(self.public_get("/v5/market/time", &[]).await?)?;
        let nanos =
            i64::from_str(result.time_nano.trim()).map_err(|_| error("invalid server time"))?;
        Ok(nanos / 1_000_000)
    }

    async fn send(&self, request: reqwest::RequestBuilder, path: &str) -> ArbResult<String> {
        let response = request
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| transport_error(Venue::Bybit, path, e))?;
        let status = response.status();
        if !status.is_success() {
            // Do not echo the body: failure envelopes can repeat request parameters.
            return Err(error(format!("HTTP {}", status.as_u16())));
        }
        response
            .text()
            .await
            .map_err(|e| transport_error(Venue::Bybit, path, e))
    }

    async fn public_get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> ArbResult<Envelope<T>> {
        let query = query_string(params);
        let url = if query.is_empty() {
            format!("{BASE_URL}{path}")
        } else {
            format!("{BASE_URL}{path}?{query}")
        };
        let text = self.send(self.client.get(url), path).await?;
        parse_envelope(&text)
    }

    /// Values are encoded once as query parameters; sign exactly those wire bytes.
    async fn signed_get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> ArbResult<Envelope<T>> {
        let query = query_string(params);
        let timestamp = self.timestamp()?;
        let signature = self.sign(timestamp, &query);
        let url = if query.is_empty() {
            format!("{BASE_URL}{path}")
        } else {
            format!("{BASE_URL}{path}?{query}")
        };
        let request = self
            .client
            .get(url)
            .header("X-BAPI-API-KEY", self.api_key.as_str())
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-SIGN-TYPE", "2");
        let text = self.send(request, path).await?;
        parse_envelope(&text)
    }

    /// Signed POST. The body string signed is exactly the body sent.
    async fn signed_post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &Value,
    ) -> ArbResult<Envelope<T>> {
        self.options.authorize(Venue::Bybit)?;
        let payload =
            serde_json::to_string(body).map_err(|_| error("request body could not be encoded"))?;
        let timestamp = self.timestamp()?;
        let signature = self.sign(timestamp, &payload);
        let request = self
            .client
            .post(format!("{BASE_URL}{path}"))
            .header("X-BAPI-API-KEY", self.api_key.as_str())
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", RECV_WINDOW)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-SIGN-TYPE", "2")
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload);
        let text = self.send(request, path).await?;
        parse_envelope(&text)
    }

    /// Resolve a domain symbol to a validated, freshly-fetched instrument spec.
    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Spec> {
        let native = native_symbol(symbol)?;
        let spec = self.instrument_by_native(&native).await?;
        if spec.base_coin != symbol.base {
            return Err(error(
                "instrument base coin does not match the requested symbol",
            ));
        }
        Ok(spec)
    }

    async fn instrument_by_native(&self, native: &str) -> ArbResult<Spec> {
        let params = vec![
            ("category", CATEGORY.to_string()),
            ("symbol", native.to_string()),
        ];
        let result: InstrumentsResult = checked(
            self.public_get("/v5/market/instruments-info", &params)
                .await?,
        )?;
        let instrument = result
            .list
            .into_iter()
            .find(|i| i.symbol == native)
            .ok_or_else(|| error("unknown Bybit instrument"))?;
        Spec::from(instrument)
    }

    async fn best_bid_ask(&self, native: &str) -> ArbResult<(Decimal, Decimal)> {
        let params = vec![
            ("category", CATEGORY.to_string()),
            ("symbol", native.to_string()),
            ("limit", "1".to_string()),
        ];
        let start = Instant::now();
        let result: BookResult = checked(self.public_get("/v5/market/orderbook", &params).await?)?;
        if start.elapsed() > Duration::from_secs(2) {
            return Err(error("order-book round trip exceeded two seconds"));
        }
        book_prices(&result, native, self.timestamp()? as i64)
    }

    async fn price(&self, order: &NewOrder, native: &str, spec: &Spec) -> ArbResult<Decimal> {
        let raw = match order.limit_price {
            Some(limit) => limit,
            None => {
                let (bid, ask) = self.best_bid_ask(native).await?;
                self.options
                    .bound_price(Venue::Bybit, order.side, Some(bid), Some(ask))?
            }
        };
        round_price(raw, spec.tick, order.side)
            .ok_or_else(|| error("price could not be rounded to the Bybit tick size"))
    }

    /// Per-symbol isolated margin + leverage. Account-wide margin mode is never touched:
    /// a UTA must already be isolated (verified at connect); a Classic account switches
    /// only this symbol. Reads the leverage back before returning.
    async fn set_isolated_leverage(
        &self,
        native: &str,
        leverage: Decimal,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let trade_mode = if mode.is_cross() { 0 } else { 1 };
        if self.account != AccountType::Classic
            && (mode.is_cross() != (self.account == AccountType::UnifiedCross))
        {
            return Err(error(
                "Bybit UTA 的保证金模式是账户级设置：请先在交易所设置所选模式，本机器人不会自动修改账户级模式",
            ));
        }
        let info: AccountInfo = checked(self.signed_get("/v5/account/info", &[]).await?)?;
        if account_type(&info)? != self.account {
            return Err(error("account type changed; reconnect before trading"));
        }
        let before = self
            .position_for(native)
            .await?
            .ok_or_else(|| error("missing symbol settings"))?;
        let level = leverage.normalize().to_string();
        if self.account == AccountType::Classic && before.trade_mode != Some(trade_mode) {
            let response: Envelope<Value> = self
                .signed_post(
                    "/v5/position/switch-isolated",
                    &json!({
                        "category": CATEGORY, "symbol": native, "tradeMode": trade_mode,
                        "buyLeverage": level, "sellLeverage": level,
                    }),
                )
                .await?;
            checked(response)?;
        }
        if decimal(&before.leverage)? != leverage {
            let response: Envelope<Value> = self
                .signed_post(
                    "/v5/position/set-leverage",
                    &json!({
                        "category": CATEGORY, "symbol": native,
                        "buyLeverage": level, "sellLeverage": level,
                    }),
                )
                .await?;
            if response.ret_code != 0 && response.ret_code != 110043 {
                return Err(error(format!("set-leverage retCode {}", response.ret_code)));
            }
        }
        let position = self
            .position_for(native)
            .await?
            .ok_or_else(|| error("leverage readback returned no position row"))?;
        if decimal(&position.leverage)? != leverage
            || (self.account == AccountType::Classic && position.trade_mode != Some(trade_mode))
        {
            return Err(error("isolated margin/leverage readback mismatch"));
        }
        let info: AccountInfo = checked(self.signed_get("/v5/account/info", &[]).await?)?;
        if account_type(&info)? != self.account {
            return Err(error("account type changed during leverage update"));
        }
        Ok(())
    }

    async fn position_for(&self, native: &str) -> ArbResult<Option<Position>> {
        let params = vec![
            ("category", CATEGORY.to_string()),
            ("symbol", native.to_string()),
        ];
        let result: PositionsResult =
            checked(self.signed_get("/v5/position/list", &params).await?)?;
        if result.list.len() != 1 || !result.next_page_cursor.is_empty() {
            return Err(error(
                "symbol settings missing or ambiguous; require one-way mode",
            ));
        }
        let position = result
            .list
            .into_iter()
            .next()
            .ok_or_else(|| error("missing position settings"))?;
        if position.symbol != native || position.position_idx != 0 {
            return Err(error(
                "symbol is in hedge mode; configure one-way mode in Bybit",
            ));
        }
        Ok(Some(position))
    }

    async fn check_reduce_only(
        &self,
        order: &NewOrder,
        native: &str,
        units: Decimal,
    ) -> ArbResult<()> {
        let position = self
            .position_for(native)
            .await?
            .ok_or_else(|| error("reduce-only order has no matching position"))?;
        let size = decimal(&position.size)?;
        if size <= Decimal::ZERO {
            return Err(error("reduce-only order has no position to reduce"));
        }
        let net = match parse_side(&position.side)? {
            Side::Buy => size,
            Side::Sell => -size,
        };
        let closes = (net > Decimal::ZERO && order.side == Side::Sell)
            || (net < Decimal::ZERO && order.side == Side::Buy);
        if !closes || units > net.abs() {
            return Err(error(
                "reduce-only quantity or direction exceeds the current position",
            ));
        }
        Ok(())
    }

    /// All executions for one order, paginated. Funding settlements are not order
    /// executions and are excluded so they cannot inflate the traded fee.
    async fn executions(&self, order_id: &str) -> ArbResult<Vec<Execution>> {
        let mut out = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("orderId", order_id.to_string()),
                ("limit", "100".to_string()),
            ];
            if !cursor.is_empty() {
                params.push(("cursor", cursor.clone()));
            }
            let result: ExecutionsResult =
                checked(self.signed_get("/v5/execution/list", &params).await?)?;
            out.extend(result.list);
            if result.next_page_cursor.is_empty() {
                return Ok(out);
            }
            cursor = result.next_page_cursor;
        }
        Err(error("Bybit execution pagination did not terminate"))
    }

    async fn find_order(&self, key: &OrderKey) -> ArbResult<Option<RemoteOrder>> {
        if let Some(order) = self.order_query("/v5/order/realtime", key).await? {
            return Ok(Some(order));
        }
        // After a release/restart, closed Unified-account orders only appear in history.
        self.order_query("/v5/order/history", key).await
    }

    async fn order_query(&self, path: &str, key: &OrderKey) -> ArbResult<Option<RemoteOrder>> {
        let mut params = vec![
            ("category", CATEGORY.to_string()),
            ("settleCoin", QUOTE.to_string()),
        ];
        match key {
            OrderKey::Link(link) => params.push(("orderLinkId", link.clone())),
            OrderKey::ByOrderId(id) => params.push(("orderId", id.clone())),
        }
        let result: OrdersResult = checked(self.signed_get(path, &params).await?)?;
        if !result.next_page_cursor.is_empty() || result.list.len() > 1 {
            return Err(error("Bybit order lookup returned ambiguous results"));
        }
        let remote = result.list.into_iter().next();
        if let Some(row) = &remote {
            let matches = match key {
                OrderKey::Link(link) => row.order_link_id == *link,
                OrderKey::ByOrderId(id) => row.order_id == *id,
            };
            if !matches {
                return Err(error("order lookup returned a different identifier"));
            }
        }
        Ok(remote)
    }

    /// Build an `OrderState` from the venue's order record and its actual executions.
    async fn observe(&self, order: NewOrder, remote: RemoteOrder) -> ArbResult<OrderState> {
        if remote.position_idx != 0 {
            return Err(error(
                "hedge-mode order cannot be represented as a net position",
            ));
        }
        if remote.order_id.is_empty()
            || parse_side(&remote.side)? != order.side
            || symbol_from_native(&remote.symbol)? != order.symbol
            || remote.reduce_only != order.reduce_only
        {
            return Err(error("venue order does not match the intended market/side"));
        }
        if let Some(saved) = self.journal.lock().await.get(&order.client_order_id)
            && (saved.instrument != remote.symbol
                || saved.venue_client_id != remote.order_link_id
                || saved.units != decimal(&remote.qty)?)
        {
            return Err(error(
                "venue order does not match the persisted quantity/identifier",
            ));
        }
        let qty = decimal(&remote.qty)?;
        let cum = decimal(&remote.cum_exec_qty)?;
        let status = verified_status(map_status(&remote.order_status)?, qty, cum)?;
        let executions = self.executions(&remote.order_id).await?;
        let (filled_base, filled_quote, fee) = aggregate_fills(
            &executions,
            &remote.order_id,
            &remote.symbol,
            &remote.side,
            cum,
        )?;
        let average_price = (filled_base > Decimal::ZERO).then(|| filled_quote / filled_base);
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(remote.order_id.clone());
        state.status = status;
        state.filled_usdt = filled_quote;
        state.average_price = average_price;
        state.fee_usdt = fee;
        if status == OrderStatus::Rejected {
            let reason = remote.reject_reason.trim();
            if !reason.is_empty() && reason != "EC_NoError" {
                state.reject_reason = Some(reason.to_string());
            }
        }
        Ok(state)
    }

    /// The intent behind a venue order: our journal entry if we placed it, otherwise a
    /// synthetic intent so reconciliation never hides a foreign order.
    async fn intent_order(&self, id: &ClientOrderId, remote: &RemoteOrder) -> ArbResult<NewOrder> {
        let saved = self
            .journal
            .lock()
            .await
            .get(id)
            .map(|entry| entry.order.clone());
        match saved {
            Some(order) => Ok(order),
            None => synthetic_order(remote),
        }
    }

    async fn record_terminal(&self, id: &ClientOrderId, state: &OrderState) -> ArbResult<()> {
        if state.status.is_live() {
            return Ok(());
        }
        let mut journal = self.journal.lock().await;
        if journal.get(id).is_some() {
            journal.record_terminal(state)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Broker for BybitBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let native = native_symbol(symbol)?;
        let Some(row) = self.position_for(&native).await? else {
            return Ok(None);
        };
        if decimal(&row.size)? == Decimal::ZERO {
            return Ok(None);
        }
        let mode = match self.account {
            AccountType::Classic => match row.trade_mode {
                Some(0) => Some(crate::MarginMode::Cross),
                Some(1) => Some(crate::MarginMode::Isolated),
                _ => None,
            },
            _ => {
                let info: AccountInfo = checked(self.signed_get("/v5/account/info", &[]).await?)?;
                match account_type(&info)? {
                    AccountType::Unified => Some(crate::MarginMode::Isolated),
                    AccountType::UnifiedCross => Some(crate::MarginMode::Cross),
                    _ => None,
                }
            }
        };
        let parse = |value: &Option<String>| value.as_deref().and_then(|raw| decimal(raw).ok());
        Ok(Some(crate::margin::venue_state(
            mode,
            parse(&row.liq_price),
            parse(&row.position_balance),
        )))
    }

    async fn prepare_open_mode(
        &self,
        symbol: &Symbol,
        _: Side,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.options.authorize(Venue::Bybit)?;
        let _guard = self.submit.lock().await;
        let native = native_symbol(symbol)?;
        let spec = self.instrument(symbol).await?;
        let leverage = crate::margin::leverage(Venue::Bybit, leverage)?;
        if leverage > spec.max_leverage {
            return Err(error("leverage exceeds market limit"));
        }
        self.set_isolated_leverage(&native, leverage, mode).await
    }

    fn venue(&self) -> Venue {
        Venue::Bybit
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.options.authorize(Venue::Bybit)?;
        let _submission = self.submit.lock().await;
        if order.venue != Venue::Bybit || order.client_order_id.0.is_empty() {
            return Err(error(
                "broker only accepts Bybit orders with a client order ID",
            ));
        }
        let link = venue_client_id(LINK_PREFIX, &order.client_order_id, 36);

        // Never resubmit a recorded intent.
        {
            let journal = self.journal.lock().await;
            if let Some(entry) = journal.get(&order.client_order_id) {
                if !same_intent(&entry.order, order) {
                    return Err(error("client order ID reused with a different intent"));
                }
                if let Some(terminal) = &entry.terminal {
                    return ack(terminal);
                }
            }
        }
        let has_intent = self
            .journal
            .lock()
            .await
            .get(&order.client_order_id)
            .is_some();
        if has_intent {
            return match self.find_order(&OrderKey::Link(link)).await? {
                Some(remote) => {
                    let intent = self.intent_order(&order.client_order_id, &remote).await?;
                    let state = self.observe(intent, remote).await?;
                    self.record_terminal(&order.client_order_id, &state).await?;
                    ack(&state)
                }
                None => Err(error(
                    "reserved order ID is unresolved; do not resubmit, reconcile manually",
                )),
            };
        }

        let native = native_symbol(&order.symbol)?;
        let spec = self.instrument(&order.symbol).await?;
        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| error("opening order requires explicit leverage"))?;
            if !value.fract().is_zero() || value < Decimal::ONE || value > spec.max_leverage {
                return Err(error(
                    "leverage must be an integer within the instrument limit",
                ));
            }
            Some(value)
        };
        if let Some(leverage) = leverage {
            self.set_isolated_leverage(&native, leverage, order.margin_mode)
                .await?;
        }

        let price = self.price(order, &native, &spec).await?;
        let units = order_units(
            Venue::Bybit,
            order,
            price,
            Decimal::ONE,
            spec.step,
            spec.min_qty,
        )?;
        if units > spec.max_qty {
            return Err(error("order quantity exceeds the instrument maximum"));
        }
        if spec.min_notional > Decimal::ZERO && units * price < spec.min_notional {
            return Err(error("order is below the Bybit minimum notional"));
        }
        if order.reduce_only {
            self.check_reduce_only(order, &native, units).await?;
        }

        // Reserve BEFORE the order write. A timeout never permits a replay.
        self.journal.lock().await.reserve(JournalEntry {
            order: order.clone(),
            venue_client_id: link.clone(),
            instrument: native.clone(),
            units,
            terminal: None,
        })?;

        let body = json!({
            "category": CATEGORY,
            "symbol": native,
            "side": side_str(order.side),
            "orderType": "Limit",
            "qty": units.normalize().to_string(),
            "price": price.normalize().to_string(),
            "timeInForce": "IOC",
            "positionIdx": 0,
            "reduceOnly": order.reduce_only,
            "orderLinkId": link,
        });
        let create = self.signed_post::<Value>("/v5/order/create", &body).await;
        // Always reconcile, including transport failures/timeouts. Never replay.
        let remote = match self.find_order(&OrderKey::Link(link)).await? {
            Some(remote) => remote,
            None => {
                return match create {
                    // HTTP 200 + retCode: Bybit processed and refused the order, and it is
                    // absent. Record the refusal so `order_state` reports Rejected instead of
                    // an intent that can never resolve. 10000/10016 (server timeout/error)
                    // leave the outcome unknown.
                    Ok(envelope)
                        if envelope.ret_code != 0
                            && !matches!(envelope.ret_code, 10000 | 10016) =>
                    {
                        let mut rejected = OrderState::new(order.clone());
                        rejected.status = OrderStatus::Rejected;
                        rejected.reject_reason =
                            Some(format!("Bybit retCode {}", envelope.ret_code));
                        self.journal.lock().await.record_terminal(&rejected)?;
                        Err(error(format!(
                            "order rejected: Bybit retCode {}",
                            envelope.ret_code
                        )))
                    }
                    Ok(_) => Err(error(
                        "submitted order not yet queryable; reserved ID retained",
                    )),
                    Err(error) => Err(error),
                };
            }
        };
        let intent = self.intent_order(&order.client_order_id, &remote).await?;
        let state = self.observe(intent, remote).await?;
        self.record_terminal(&order.client_order_id, &state).await?;
        ack(&state)
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let _submission = self.submit.lock().await;
        if let Some(terminal) = self
            .journal
            .lock()
            .await
            .get(client_order_id)
            .and_then(|entry| entry.terminal.clone())
        {
            return Ok(Some(terminal));
        }
        let key = match client_order_id.0.strip_prefix(EXTERNAL_PREFIX) {
            Some(order_id) => OrderKey::ByOrderId(order_id.to_string()),
            None => OrderKey::Link(venue_client_id(LINK_PREFIX, client_order_id, 36)),
        };
        if let Some(remote) = self.find_order(&key).await? {
            let intent = self.intent_order(client_order_id, &remote).await?;
            let state = self.observe(intent, remote).await?;
            self.record_terminal(client_order_id, &state).await?;
            return Ok(Some(state));
        }
        if self.journal.lock().await.get(client_order_id).is_some()
            || client_order_id.0.starts_with(EXTERNAL_PREFIX)
        {
            return Err(error(
                "previously observed order no longer queryable; cannot assume it never filled",
            ));
        }
        Ok(None)
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.options.authorize(Venue::Bybit)?;
        let _submission = self.submit.lock().await;
        let key = OrderKey::ByOrderId(venue_order_id.to_string());
        // No realtime record means the order is not on the book; treat as already gone.
        let remote = match self.find_order(&key).await? {
            Some(remote) => remote,
            None => return Ok(()),
        };
        if !map_status(&remote.order_status)?.is_live() {
            return Ok(());
        }
        let body = json!({
            "category": CATEGORY,
            "symbol": remote.symbol,
            "orderId": remote.order_id,
        });
        let cancel = self
            .signed_post::<Value>("/v5/order/cancel", &body)
            .await
            .and_then(checked);
        let after = self
            .find_order(&key)
            .await?
            .ok_or_else(|| error("cancelled order not queryable; cannot confirm terminal state"))?;
        if map_status(&after.order_status)?.is_live() {
            return Err(cancel
                .err()
                .unwrap_or_else(|| error("order is still live after cancellation")));
        }
        let id = self
            .journal
            .lock()
            .await
            .by_venue_client_id(&after.order_link_id)
            .map(|entry| entry.order.client_order_id.clone());
        if let Some(id) = id {
            let intent = self.intent_order(&id, &after).await?;
            let state = self.observe(intent, after).await?;
            self.record_terminal(&id, &state).await?;
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        // No symbol filter: reconciliation must see foreign/unknown orders too.
        let mut result = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("settleCoin", QUOTE.to_string()),
                ("openOnly", "0".to_string()),
                ("limit", "50".to_string()),
            ];
            if !cursor.is_empty() {
                params.push(("cursor", cursor.clone()));
            }
            let page: OrdersResult =
                checked(self.signed_get("/v5/order/realtime", &params).await?)?;
            for remote in page.list {
                if !map_status(&remote.order_status)?.is_live() {
                    continue;
                }
                let intent = {
                    let journal = self.journal.lock().await;
                    match journal.by_venue_client_id(&remote.order_link_id) {
                        Some(entry) => entry.order.clone(),
                        None => synthetic_order(&remote)?,
                    }
                };
                let state = self.observe(intent, remote).await?;
                if state.status.is_live() {
                    result.push(state);
                }
            }
            if page.next_page_cursor.is_empty() {
                return Ok(result);
            }
            cursor = page.next_page_cursor;
        }
        Err(error("Bybit open-order pagination did not terminate"))
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let mut result = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("settleCoin", QUOTE.to_string()),
                ("limit", "200".to_string()),
            ];
            if !cursor.is_empty() {
                params.push(("cursor", cursor.clone()));
            }
            let page: PositionsResult =
                checked(self.signed_get("/v5/position/list", &params).await?)?;
            for position in page.list {
                if position.position_idx != 0 {
                    return Err(error(
                        "hedge-mode position detected; switch the symbol to one-way mode",
                    ));
                }
                let size = decimal(&position.size)?;
                if size <= Decimal::ZERO {
                    continue;
                }
                let net_quantity = match parse_side(&position.side)? {
                    Side::Buy => size,
                    Side::Sell => -size,
                };
                let average_price = decimal_opt(&position.avg_price).filter(|p| *p > Decimal::ZERO);
                let notional_usdt = decimal_opt(&position.position_value)
                    .map(|value| value.abs())
                    .unwrap_or(Decimal::ZERO);
                result.push(VenuePosition {
                    venue: Venue::Bybit,
                    symbol: symbol_from_native(&position.symbol)?,
                    net_quantity,
                    average_price,
                    notional_usdt,
                });
            }
            if page.next_page_cursor.is_empty() {
                return Ok(result);
            }
            cursor = page.next_page_cursor;
        }
        Err(error("Bybit position pagination did not terminate"))
    }
}

enum OrderKey {
    Link(String),
    ByOrderId(String),
}

fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue(Venue::Bybit.as_str(), message)
}

/// v5 signature: `HMAC_SHA256(secret, timestamp + apiKey + recvWindow + payload)`, lowercase hex.
/// `payload` is the exact query string (GET) or the exact JSON body (POST).
fn signature(secret: &str, timestamp: u64, api_key: &str, payload: &str) -> String {
    let message = format!("{timestamp}{api_key}{RECV_WINDOW}{payload}");
    hex_lower(&hmac_sha256(secret.as_bytes(), message.as_bytes()))
}

/// `unifiedMarginStatus`: 1 = classic account (isolation is per symbol); 3/4 = UTA 1.0,
/// 5/6 = UTA 2.0 (margin mode is account-wide and must already be `ISOLATED_MARGIN`).
fn account_type(info: &AccountInfo) -> ArbResult<AccountType> {
    match info.unified_margin_status {
        1 => Ok(AccountType::Classic),
        3..=6 if info.margin_mode == "ISOLATED_MARGIN" => Ok(AccountType::Unified),
        3..=6 if info.margin_mode == "REGULAR_MARGIN" => Ok(AccountType::UnifiedCross),
        3..=6 => Err(error(
            "unified account must be in ISOLATED_MARGIN or REGULAR_MARGIN mode; switch it in Bybit first — \
             the broker never changes account-wide settings",
        )),
        _ => Err(error("unknown Bybit account type; refusing to trade")),
    }
}

/// Best bid/ask from a fresh one-level book. The response must be for this symbol, no
/// older than five seconds, non-empty on both sides and uncrossed.
fn book_prices(book: &BookResult, native: &str, now_ms: i64) -> ArbResult<(Decimal, Decimal)> {
    if book.s != native {
        return Err(error("order book is for a different symbol"));
    }
    if now_ms - book.ts > 5_000 || book.ts > now_ms + 1_000 {
        return Err(error("order book is stale"));
    }
    let best = |levels: &Option<Vec<Vec<String>>>| -> ArbResult<Decimal> {
        let level = levels
            .as_ref()
            .and_then(|levels| levels.first())
            .ok_or_else(|| error("order book side is empty"))?;
        let (price, size) = match level.as_slice() {
            [price, size, ..] => (decimal(price)?, decimal(size)?),
            _ => return Err(error("malformed order book level")),
        };
        if price <= Decimal::ZERO || size <= Decimal::ZERO {
            return Err(error("non-positive order book level"));
        }
        Ok(price)
    };
    let (bid, ask) = (best(&book.b)?, best(&book.a)?);
    if ask < bid {
        return Err(error("crossed order book"));
    }
    Ok((bid, ask))
}

/// A terminal IOC match need not be a full fill: `Filled` with less than the requested
/// quantity is `Cancelled`, and a "rejected" order that traded is `Cancelled` too.
fn verified_status(status: OrderStatus, qty: Decimal, cum: Decimal) -> ArbResult<OrderStatus> {
    if cum < Decimal::ZERO || cum > qty {
        return Err(error("executed quantity is outside the order quantity"));
    }
    Ok(match status {
        OrderStatus::Filled if cum < qty => OrderStatus::Cancelled,
        OrderStatus::Rejected if cum > Decimal::ZERO => OrderStatus::Cancelled,
        other => other,
    })
}

fn query_string(params: &[(&str, String)]) -> String {
    let mut url = reqwest::Url::parse(BASE_URL).expect("constant Bybit URL");
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in params {
            pairs.append_pair(key, value);
        }
    }
    url.query().unwrap_or("").to_string()
}

/// Read `retCode` before the typed `result`: a failure carries `result: {}`, which would
/// otherwise fail the typed parse and hide the real code. A failed envelope keeps no result.
fn parse_envelope<T: DeserializeOwned>(text: &str) -> ArbResult<Envelope<T>> {
    let raw: Envelope<Value> =
        serde_json::from_str(text).map_err(|_| error("malformed Bybit response"))?;
    if raw.ret_code != 0 {
        return Ok(Envelope {
            ret_code: raw.ret_code,
            result: None,
        });
    }
    let result = raw
        .result
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| error("Bybit result does not match the expected schema"))?;
    Ok(Envelope {
        ret_code: raw.ret_code,
        result,
    })
}

fn checked<T>(envelope: Envelope<T>) -> ArbResult<T> {
    if envelope.ret_code != 0 {
        return Err(error(format!("Bybit retCode {}", envelope.ret_code)));
    }
    envelope
        .result
        .ok_or_else(|| error("successful Bybit response is missing result"))
}

fn decimal(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| error("invalid Bybit decimal"))
}

fn decimal_opt(raw: &str) -> Option<Decimal> {
    Decimal::from_str(raw.trim()).ok()
}

fn parse_side(raw: &str) -> ArbResult<Side> {
    match raw {
        "Buy" => Ok(Side::Buy),
        "Sell" => Ok(Side::Sell),
        _ => Err(error("unknown Bybit order side")),
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "Buy",
        Side::Sell => "Sell",
    }
}

fn native_symbol(symbol: &Symbol) -> ArbResult<String> {
    if symbol.quote != QUOTE {
        return Err(error("only USDT-margined linear perpetuals are supported"));
    }
    Ok(format!("{}{}", symbol.base, symbol.quote))
}

fn symbol_from_native(native: &str) -> ArbResult<Symbol> {
    let base = native
        .strip_suffix(QUOTE)
        .filter(|base| !base.is_empty())
        .ok_or_else(|| error("symbol is not a USDT linear perpetual"))?;
    Ok(Symbol::perp(base, QUOTE))
}

fn same_intent(a: &NewOrder, b: &NewOrder) -> bool {
    if a.margin_mode != b.margin_mode {
        return false;
    }
    a.venue == b.venue
        && a.symbol == b.symbol
        && a.side == b.side
        && a.reduce_only == b.reduce_only
        && a.notional_usdt == b.notional_usdt
        && a.quantity == b.quantity
        && a.limit_price == b.limit_price
        && a.leverage == b.leverage
}

fn map_status(raw: &str) -> ArbResult<OrderStatus> {
    match raw {
        "New" | "PartiallyFilled" | "Untriggered" | "Triggered" => Ok(OrderStatus::Open),
        "Filled" => Ok(OrderStatus::Filled),
        "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" => Ok(OrderStatus::Cancelled),
        "Rejected" => Ok(OrderStatus::Rejected),
        _ => Err(error(format!("unknown Bybit order status {raw:?}"))),
    }
}

/// Rebuild an intent for an order this process did not place, so reconciliation and
/// `order_state` can still describe it.
fn synthetic_order(remote: &RemoteOrder) -> ArbResult<NewOrder> {
    let qty = decimal(&remote.qty)?;
    let price = decimal_opt(&remote.price).filter(|price| *price > Decimal::ZERO);
    Ok(NewOrder {
        margin_mode: crate::MarginMode::Isolated,
        client_order_id: ClientOrderId(format!("{EXTERNAL_PREFIX}{}", remote.order_id)),
        venue: Venue::Bybit,
        symbol: symbol_from_native(&remote.symbol)?,
        side: parse_side(&remote.side)?,
        notional_usdt: price.map(|price| price * qty).unwrap_or(Decimal::ZERO),
        quantity: Some(qty),
        limit_price: price,
        reduce_only: remote.reduce_only,
        leverage: None,
    })
}

/// Sum the traded quantity/value and fees from the execution list. Fees must be in
/// USDT (an empty `feeCurrency` means the contract's USDT settlement), and the fills
/// must add up to the order's executed quantity — otherwise the state is untrustworthy.
fn aggregate_fills(
    executions: &[Execution],
    order_id: &str,
    symbol: &str,
    side: &str,
    cum_exec_qty: Decimal,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let mut filled_base = Decimal::ZERO;
    let mut filled_quote = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for execution in executions {
        if execution.order_id != order_id
            || (!execution.symbol.is_empty() && execution.symbol != symbol)
            || (!execution.side.is_empty() && execution.side != side)
        {
            return Err(error("execution does not belong to the requested order"));
        }
        if execution.exec_type == "Funding" {
            // Funding settlement is not a trade for this order.
            continue;
        }
        let currency = execution.fee_currency.trim();
        if !currency.is_empty() && currency != QUOTE {
            return Err(error("execution fee is not charged in USDT"));
        }
        let quantity = decimal(&execution.exec_qty)?;
        let price = decimal(&execution.exec_price)?;
        filled_base += quantity;
        filled_quote += quantity * price;
        fee += decimal(&execution.exec_fee)?;
    }
    if filled_base != cum_exec_qty {
        return Err(error(
            "Bybit fills do not sum to the order's executed quantity",
        ));
    }
    Ok((filled_base, filled_quote, fee))
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| error("venue order ID is missing"))?,
        status: state.status,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<T> {
    ret_code: i64,
    result: Option<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimeResult {
    time_nano: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountInfo {
    unified_margin_status: i64,
    #[serde(default)]
    margin_mode: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeRateResult {
    #[serde(default)]
    list: Vec<FeeRate>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeRate {
    taker_fee_rate: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentsResult {
    #[serde(default)]
    list: Vec<Instrument>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Instrument {
    symbol: String,
    contract_type: String,
    status: String,
    base_coin: String,
    quote_coin: String,
    settle_coin: String,
    #[serde(default)]
    is_pre_listing: bool,
    price_filter: PriceFilter,
    lot_size_filter: LotSizeFilter,
    leverage_filter: LeverageFilter,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct PriceFilter {
    tick_size: String,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct LotSizeFilter {
    min_order_qty: String,
    max_order_qty: String,
    qty_step: String,
    #[serde(default)]
    min_notional_value: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct LeverageFilter {
    max_leverage: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookResult {
    #[serde(default)]
    s: String,
    #[serde(default)]
    ts: i64,
    #[serde(default)]
    b: Option<Vec<Vec<String>>>,
    #[serde(default)]
    a: Option<Vec<Vec<String>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrdersResult {
    #[serde(default)]
    list: Vec<RemoteOrder>,
    #[serde(default)]
    next_page_cursor: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    order_id: String,
    #[serde(default)]
    order_link_id: String,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    qty: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    position_idx: i64,
    order_status: String,
    #[serde(default)]
    reject_reason: String,
    #[serde(default)]
    cum_exec_qty: String,
    #[serde(default)]
    reduce_only: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionsResult {
    #[serde(default)]
    list: Vec<Execution>,
    #[serde(default)]
    next_page_cursor: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Execution {
    order_id: String,
    exec_qty: String,
    exec_price: String,
    exec_fee: String,
    #[serde(default)]
    fee_currency: String,
    #[serde(default)]
    exec_type: String,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    side: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionsResult {
    #[serde(default)]
    list: Vec<Position>,
    #[serde(default)]
    next_page_cursor: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Position {
    symbol: String,
    #[serde(default)]
    side: String,
    #[serde(default)]
    size: String,
    #[serde(default)]
    avg_price: String,
    #[serde(default)]
    position_value: String,
    #[serde(default)]
    leverage: String,
    #[serde(default)]
    position_idx: i64,
    /// Classic accounts: 0 = cross, 1 = isolated.
    #[serde(default)]
    trade_mode: Option<i64>,
    #[serde(default)]
    liq_price: Option<String>,
    #[serde(default)]
    position_balance: Option<String>,
}

/// Validated trading parameters for one USDT linear perpetual.
struct Spec {
    base_coin: String,
    tick: Decimal,
    step: Decimal,
    min_qty: Decimal,
    max_qty: Decimal,
    min_notional: Decimal,
    max_leverage: Decimal,
}

impl Spec {
    fn from(instrument: Instrument) -> ArbResult<Self> {
        if instrument.contract_type != "LinearPerpetual" {
            return Err(error("instrument is not a linear perpetual"));
        }
        if instrument.status != "Trading" {
            return Err(error("instrument is not currently trading"));
        }
        if instrument.quote_coin != QUOTE || instrument.settle_coin != QUOTE {
            return Err(error("instrument is not USDT-margined"));
        }
        if instrument.is_pre_listing {
            return Err(error("pre-market instruments are not supported"));
        }
        let tick = decimal(&instrument.price_filter.tick_size)?;
        let step = decimal(&instrument.lot_size_filter.qty_step)?;
        let min_qty = decimal(&instrument.lot_size_filter.min_order_qty)?;
        let max_qty = decimal(&instrument.lot_size_filter.max_order_qty)?;
        let min_notional = instrument
            .lot_size_filter
            .min_notional_value
            .as_deref()
            .map(decimal)
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        let max_leverage = decimal(&instrument.leverage_filter.max_leverage)?;
        if tick <= Decimal::ZERO
            || step <= Decimal::ZERO
            || min_qty <= Decimal::ZERO
            || max_qty <= Decimal::ZERO
            || max_leverage < Decimal::ONE
        {
            return Err(error("instrument has invalid trading parameters"));
        }
        Ok(Self {
            base_coin: instrument.base_coin,
            tick,
            step,
            min_qty,
            max_qty,
            min_notional,
            max_leverage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order(quantity: Option<Decimal>, reduce_only: bool) -> NewOrder {
        NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("live-1-buy-0".into()),
            venue: Venue::Bybit,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(1000),
            quantity,
            limit_price: Some(dec!(50000)),
            reduce_only,
            leverage: Some(dec!(5)),
        }
    }

    /// The secret for Bybit's published example is not public, so these expected digests
    /// were produced independently with Python's `hmac`/`hashlib` over the documented
    /// concatenation `timestamp + api_key + recv_window + payload`.
    #[test]
    fn hmac_signature_string_matches_documented_rule() {
        let post = format!(
            "{}{}{}{}",
            "1234567890123", "APIKEY", "5000", r#"{"category":"linear"}"#
        );
        assert_eq!(
            hex_lower(&hmac_sha256(b"0123456789abcdef", post.as_bytes())),
            "1d809fdd2ec708061d4ea491d2295f080ca36bff7e1e5b862db843b7e5685e79"
        );
        let get = format!(
            "{}{}{}{}",
            "1700000000000", "MYKEY", "5000", "category=linear&symbol=BTCUSDT"
        );
        assert_eq!(
            hex_lower(&hmac_sha256(b"0123456789abcdef", get.as_bytes())),
            "9c97af0eca23de85daa2a51c15a96af9acc3f89c5346dd2f76486b0d5c15978b"
        );
    }

    #[test]
    fn unified_margin_mode_is_an_account_prerequisite_not_implicitly_switched() {
        let info = |mode: &str| AccountInfo {
            unified_margin_status: 6,
            margin_mode: mode.into(),
        };
        assert!(matches!(
            account_type(&info("REGULAR_MARGIN")).unwrap(),
            AccountType::UnifiedCross
        ));
        assert!(matches!(
            account_type(&info("ISOLATED_MARGIN")).unwrap(),
            AccountType::Unified
        ));
        assert!(account_type(&info("PORTFOLIO_MARGIN")).is_err());
        assert!(account_type(&info("unknown")).is_err());
    }

    #[test]
    fn status_strings_map_without_guessing() {
        assert_eq!(map_status("New").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("PartiallyFilled").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("Filled").unwrap(), OrderStatus::Filled);
        assert_eq!(map_status("Cancelled").unwrap(), OrderStatus::Cancelled);
        assert_eq!(
            map_status("PartiallyFilledCanceled").unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(map_status("Deactivated").unwrap(), OrderStatus::Cancelled);
        assert_eq!(map_status("Rejected").unwrap(), OrderStatus::Rejected);
        assert!(map_status("SomethingNew").is_err());
    }

    #[test]
    fn linear_quantities_are_base_units_and_reduce_only_is_exact() {
        let units = |q, r| {
            order_units(
                Venue::Bybit,
                &order(q, r),
                dec!(50000),
                Decimal::ONE,
                dec!(0.001),
                dec!(0.001),
            )
        };
        // 1000 / 50000 = 0.02 → exact on the 0.001 step
        assert_eq!(units(None, false).unwrap(), dec!(0.02));
        assert_eq!(units(Some(dec!(0.0239)), false).unwrap(), dec!(0.023));
        assert_eq!(units(Some(dec!(0.023)), true).unwrap(), dec!(0.023));
        assert!(
            units(Some(dec!(0.0239)), true).is_err(),
            "reduce-only leftovers are not dropped"
        );
        assert!(units(None, true).is_err());
    }

    #[test]
    fn client_ids_fit_bybits_36_character_limit() {
        let id = ClientOrderId("live-1-buy-0".into());
        let link = venue_client_id(LINK_PREFIX, &id, 36);
        assert!(link.len() <= 36);
        assert!(link.starts_with(LINK_PREFIX));
        assert_eq!(link, venue_client_id(LINK_PREFIX, &id, 36));
    }

    #[test]
    fn native_symbol_round_trips_and_rejects_other_quotes() {
        assert_eq!(
            native_symbol(&Symbol::perp("BTC", "USDT")).unwrap(),
            "BTCUSDT"
        );
        assert_eq!(
            symbol_from_native("BTCUSDT").unwrap(),
            Symbol::perp("BTC", "USDT")
        );
        assert!(native_symbol(&Symbol::perp("BTC", "USDC")).is_err());
    }

    #[test]
    fn fills_aggregate_from_execution_list() {
        let executions: Vec<Execution> = serde_json::from_str(
            r#"[
              {"orderId":"o1","execQty":"0.01","execPrice":"50000","execFee":"0.25","feeCurrency":"","execType":"Trade"},
              {"orderId":"o1","execQty":"0.01","execPrice":"60000","execFee":"0.30","feeCurrency":"USDT","execType":"Trade"},
              {"orderId":"o1","execQty":"0","execPrice":"0","execFee":"9.99","feeCurrency":"USDT","execType":"Funding"}
            ]"#,
        )
        .unwrap();
        let (base, quote, fee) =
            aggregate_fills(&executions, "o1", "BTCUSDT", "Buy", dec!(0.02)).unwrap();
        assert_eq!(base, dec!(0.02));
        assert_eq!(quote, dec!(1100));
        // Funding rows are not order fees.
        assert_eq!(fee, dec!(0.55));
    }

    #[test]
    fn fills_must_sum_to_executed_quantity_and_be_usdt() {
        let mismatch: Vec<Execution> = serde_json::from_str(
            r#"[{"orderId":"o1","execQty":"0.01","execPrice":"50000","execFee":"0.25","execType":"Trade"}]"#,
        )
        .unwrap();
        assert!(aggregate_fills(&mismatch, "o1", "BTCUSDT", "Buy", dec!(0.02)).is_err());

        let wrong_currency: Vec<Execution> = serde_json::from_str(
            r#"[{"orderId":"o1","execQty":"0.01","execPrice":"50000","execFee":"0.25","feeCurrency":"USDC","execType":"Trade"}]"#,
        )
        .unwrap();
        assert!(aggregate_fills(&wrong_currency, "o1", "BTCUSDT", "Buy", dec!(0.01)).is_err());
        let other_side: Vec<Execution> = serde_json::from_str(
            r#"[{"orderId":"o1","execQty":"0.01","execPrice":"50000","execFee":"0.25","execType":"Trade","symbol":"BTCUSDT","side":"Sell"}]"#,
        )
        .unwrap();
        assert!(aggregate_fills(&other_side, "o1", "BTCUSDT", "Buy", dec!(0.01)).is_err());
    }

    #[test]
    fn instrument_matching_accepts_only_live_usdt_linear_perps() {
        let json = r#"{
          "symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading",
          "baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","isPreListing":false,
          "priceFilter":{"tickSize":"0.10"},
          "lotSizeFilter":{"minOrderQty":"0.001","maxOrderQty":"1500","qtyStep":"0.001","minNotionalValue":"5"},
          "leverageFilter":{"maxLeverage":"150"}
        }"#;
        let spec = Spec::from(serde_json::from_str(json).unwrap()).unwrap();
        assert_eq!(spec.base_coin, "BTC");
        assert_eq!(spec.tick, dec!(0.10));
        assert_eq!(spec.step, dec!(0.001));
        assert_eq!(spec.min_notional, dec!(5));
        assert_eq!(spec.max_leverage, dec!(150));

        let usdc = json.replace("USDT", "USDC");
        assert!(Spec::from(serde_json::from_str(&usdc).unwrap()).is_err());

        let inverse = json.replace("LinearPerpetual", "InversePerpetual");
        assert!(Spec::from(serde_json::from_str(&inverse).unwrap()).is_err());

        let pre_listing = json.replace("\"isPreListing\":false", "\"isPreListing\":true");
        assert!(Spec::from(serde_json::from_str(&pre_listing).unwrap()).is_err());
    }
}
