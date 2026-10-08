//! MEXC authenticated USDT perpetual broker; all orders are bounded LIMIT + IOC.
//!
//! Official sources (not the obsolete GitHub Pages maintenance notice):
//! - <https://www.mexc.com/api-docs/futures/update-log>: trading reopened 2026-03-31;
//!   the production base changed to `https://api.mexc.com` on 2026-01-19.
//! - <https://www.mexc.com/api-docs/futures/integration-guide>: KYC accounts can
//!   enable futures trading on ordinary API keys. Sign HMAC-SHA256 over
//!   `accessKey + millisecond timestamp + encoded query / exact POST JSON`.
//! - <https://www.mexc.com/api-docs/futures/market-endpoints/get-contract-info>:
//!   contractSize is base per contract; priceUnit/volUnit/minVol define rounding.
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order>:
//!   `/order/create`, type=3 IOC, openType=1 isolated / 2 cross, positionMode=2 one-way;
//!   sides 1=open long, 2=close short, 3=open short, 4=close long.
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-order-by-external-id>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-order-information-by-order-id>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-trade-records-by-order-id>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-current-orders>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/cancel-orders>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-open-positions>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-user-position-mode>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/modify-leverage>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-position-leverage-multipliers>
//! - <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-fee-details>
//!
//! Operator requirements: dedicated account/key, KYC, View Account/Order Details
//! and Order Placing permissions, one-way positions, and multi-asset mode OFF.
//! The official API documents changing multi-asset mode but no read of its current
//! value: <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/change-multi-asset-mode>.
//! This broker NEVER changes it. It verifies one-way mode on connect and before
//! placing; explicitly selects margin mode, confirms the per-symbol leverage
//! settings before submission, and rejects an owned order reporting a different mode or
//! mode after submission. Such an error leaves the durable intent unresolved for
//! reconciliation; it does not pretend the position vanished.
//!
//! A **definitive** business refusal (the documented pre-matching codes 2005/2011/
//! 2015/2028/2070, verified on <https://www.mexc.com/api-docs/futures/error-code>)
//! is recorded as a terminal `Rejected` so `order_state` stays truthful instead of
//! leaving a refused second leg dangling. Ambiguous codes (500/501/510/513/3016, all
//! documented "retry later"), HTTP 5xx/429, timeouts and unparseable bodies keep the
//! durable intent **unresolved** for the operator to reconcile — they are never
//! turned into a fake rejection.
//!
//! `fee_per_side` uses authenticated `realTakerFee`, never public web/app rates or
//! an original/pre-discount fallback. Actual order costs come only from deals.
//! MEXC publishes no fixed REST signature-result example in the cited guide;
//! tests pin independently computed synthetic GET and POST HMAC fixtures.
//! Requires serde_json's local `raw_value` feature to preserve numeric decimals.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, hex_lower, hmac_sha256, order_units, round_price,
    transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE: &str = "https://api.mexc.com";
const CONTRACTS: &str = "/api/v1/contract/detail/country";
const POSITION_MODE: &str = "/api/v1/private/position/position_mode";
const POSITIONS: &str = "/api/v1/private/position/open_positions";
const LEVERAGE: &str = "/api/v1/private/position/leverage";
const CHANGE_LEVERAGE: &str = "/api/v1/private/position/change_leverage";
const CREATE: &str = "/api/v1/private/order/create";
const CANCEL: &str = "/api/v1/private/order/cancel";
const OPEN_ORDERS: &str = "/api/v1/private/order/list/open_orders";
const FEES: &str = "/api/v1/private/account/tiered_fee_rate/v2";

/// 文档标注为「稍后重试 / 结果未知」的码（<https://www.mexc.com/api-docs/futures/error-code>）：
/// 500 内部错误、501 系统繁忙、510 请求过于频繁、513 无效请求、3016 下单超时。
/// 这些必须先去交易所查询，绝不能当成拒单。
const AMBIGUOUS_CODES: [i64; 5] = [500, 501, 510, 513, 3016];

/// Secrets intentionally implement neither Debug nor Serialize.
pub struct MexcCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub struct MexcBroker {
    client: Client,
    credentials: MexcCredentials,
    options: LiveOptions,
    journal: Mutex<OrderJournal>,
    clock_offset: i64,
    fee: Decimal,
}

impl MexcBroker {
    /// Construction is read-only, including when trading is enabled.
    pub async fn connect(
        client: Client,
        credentials: MexcCredentials,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(Venue::Mexc)?;
        if credentials.api_key.is_empty() || credentials.api_secret.is_empty() {
            return Err(err("API key and secret must be nonempty"));
        }
        let identity = format!(
            "mexc:{}",
            &hex_lower(&Sha256::digest(credentials.api_key.as_bytes()))[..16]
        );
        let journal = OrderJournal::open(journal_path, &identity)?;
        let mut broker = Self {
            client,
            credentials,
            options,
            journal: Mutex::new(journal),
            clock_offset: 0,
            fee: Decimal::ZERO,
        };
        let before = Utc::now().timestamp_millis();
        let started = Instant::now();
        let server: i64 = broker.get("/api/v1/contract/ping", &[]).await?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err(
                "server-time round trip exceeds two seconds; retry connection",
            ));
        }
        broker.clock_offset = server - (before + Utc::now().timestamp_millis()) / 2;
        broker.check_mode().await?;
        let fees: Fees = broker.get(FEES, &[]).await?;
        if fees.real_taker_fee.0 < Decimal::ZERO {
            return Err(err(
                "negative account taker fee is unsupported for cost estimates",
            ));
        }
        broker.fee = fees.real_taker_fee.0;
        Ok(broker)
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        body: Option<String>,
    ) -> ArbResult<T> {
        let private = path.starts_with("/api/v1/private/");
        if method != Method::GET {
            // Every signed write, including leverage and cancel, has its own gate.
            self.options.authorize(Venue::Mexc)?;
        }
        let query = query_string(params);
        let url = if query.is_empty() {
            format!("{BASE}{path}")
        } else {
            format!("{BASE}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method, url)
            .timeout(Duration::from_secs(10));
        if private {
            let timestamp = (Utc::now().timestamp_millis() + self.clock_offset).to_string();
            let payload = body.as_deref().unwrap_or(&query);
            let signature = sign(
                &self.credentials.api_secret,
                &self.credentials.api_key,
                &timestamp,
                payload,
            );
            request = request
                .header("ApiKey", sensitive(&self.credentials.api_key)?)
                .header("Request-Time", timestamp)
                .header("Signature", sensitive(&signature)?)
                .header("Recv-Window", "10");
        }
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| transport_error(Venue::Mexc, "MEXC request", e))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| transport_error(Venue::Mexc, "MEXC response", e))?;
        let envelope: Envelope = serde_json::from_slice(&bytes)
            .map_err(|_| err(format!("HTTP {status}; invalid response envelope")))?;
        if !status.is_success() || !envelope.success || envelope.code != 0 {
            // Codes only: even an error message could contain reflected credentials.
            return Err(err(format!("HTTP {status}; API code {}", envelope.code)));
        }
        let data = envelope.data.ok_or_else(|| err("missing response data"))?;
        decode(data.get())
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&str, String)],
    ) -> ArbResult<T> {
        self.request(Method::GET, path, params, None).await
    }

    async fn post(&self, path: &str, body: String) -> Result<Box<RawValue>, WriteError> {
        // Mutations such as change_leverage legitimately omit data on success.
        self.options
            .authorize(Venue::Mexc)
            .map_err(WriteError::Unknown)?;
        let timestamp = (Utc::now().timestamp_millis() + self.clock_offset).to_string();
        let signature = sign(
            &self.credentials.api_secret,
            &self.credentials.api_key,
            &timestamp,
            &body,
        );
        let response = self
            .client
            .post(format!("{BASE}{path}"))
            .header(
                "ApiKey",
                sensitive(&self.credentials.api_key).map_err(WriteError::Unknown)?,
            )
            .header("Request-Time", timestamp)
            .header(
                "Signature",
                sensitive(&signature).map_err(WriteError::Unknown)?,
            )
            .header("Recv-Window", "10")
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(10))
            .body(body)
            .send()
            .await
            .map_err(|e| WriteError::Unknown(transport_error(Venue::Mexc, "MEXC write", e)))?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(|e| {
            WriteError::Unknown(transport_error(Venue::Mexc, "MEXC write response", e))
        })?;
        classify_write(status, &bytes)
    }

    async fn check_mode(&self) -> ArbResult<()> {
        let mode: PositionMode = self.get(POSITION_MODE, &[]).await?;
        if mode.value() != 2 {
            return Err(err(
                "configure One-way position mode in MEXC UI first; broker never changes account-wide settings",
            ));
        }
        Ok(())
    }

    async fn contracts(&self) -> ArbResult<Vec<Contract>> {
        self.get(CONTRACTS, &[]).await
    }

    async fn contract(&self, native: &str) -> ArbResult<Contract> {
        let contract: Contract = self.get(CONTRACTS, &[("symbol", native.into())]).await?;
        if contract.symbol != native {
            return Err(err("contract metadata identity mismatch"));
        }
        contract.validate_units()?;
        Ok(contract)
    }

    async fn resolve(&self, symbol: &Symbol) -> ArbResult<Contract> {
        let mut matches = self
            .contracts()
            .await?
            .into_iter()
            .filter(|c| c.scanner_symbol().as_ref() == Some(symbol));
        let contract = matches
            .next()
            .ok_or_else(|| err("no matching USDT linear perpetual metadata"))?;
        if matches.next().is_some() {
            return Err(err("ambiguous contract mapping"));
        }
        contract.validate_units()?;
        if contract.state != 0
            || !contract.api_allowed
            || !matches!(contract.position_open_type, 1..=3)
        {
            return Err(err("contract not trading or API trading disabled"));
        }
        Ok(contract)
    }

    async fn bounded_price(&self, order: &NewOrder, contract: &Contract) -> ArbResult<Decimal> {
        let raw = if let Some(price) = order.limit_price {
            price
        } else {
            let started = Instant::now();
            let book: Book = self
                .get(
                    &format!("/api/v1/contract/depth/{}", contract.symbol),
                    &[("limit", "5".into())],
                )
                .await?;
            let now = Utc::now().timestamp_millis() + self.clock_offset;
            if started.elapsed() > Duration::from_secs(2)
                || now - book.timestamp > 5000
                || book.timestamp > now + 1000
            {
                return Err(err("stale order book"));
            }
            let best = |rows: &[Vec<Number>], buy: bool| -> ArbResult<Decimal> {
                if rows.is_empty() {
                    return Err(err("empty order-book side"));
                }
                let mut prices = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() < 2 || row[0].0 <= Decimal::ZERO || row[1].0 <= Decimal::ZERO {
                        return Err(err("invalid order-book level"));
                    }
                    prices.push(row[0].0);
                }
                if buy {
                    prices.into_iter().max()
                } else {
                    prices.into_iter().min()
                }
                .ok_or_else(|| err("empty book"))
            };
            let bid = best(&book.bids, true)?;
            let ask = best(&book.asks, false)?;
            let bound = self
                .options
                .bound_price(Venue::Mexc, order.side, Some(bid), Some(ask))?;
            clamp_touch_bound(order.side, bound, bid, ask)?
        };
        round_price(raw, contract.price_unit.0, order.side)
            .ok_or_else(|| err("invalid rounded price"))
    }

    async fn raw_positions(&self) -> ArbResult<Vec<Position>> {
        let rows: Vec<Position> = self.get(POSITIONS, &[]).await?;
        validate_positions(&rows)?;
        Ok(rows)
    }

    async fn configure(
        &self,
        contract: &Contract,
        side: Side,
        leverage: Decimal,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let open_type = if mode.is_cross() { 2 } else { 1 };
        if contract.position_open_type != 3 && contract.position_open_type != open_type {
            return Err(err("合约不支持所选保证金模式"));
        }
        let kind = if side == Side::Buy { 1 } else { 2 };
        let positions = self.raw_positions().await?;
        let position = positions
            .iter()
            .find(|p| p.symbol == contract.symbol && p.hold_vol.0 > Decimal::ZERO);
        let body = if let Some(p) = position {
            if p.position_type != kind || p.open_type != open_type || p.state != 1 {
                return Err(err(
                    "existing position is opposite, cross margin, or system-held; reconcile first",
                ));
            }
            format!(
                "{{\"positionId\":{},\"leverage\":{},\"marginSelected\":false,\"leverageSelected\":false}}",
                p.position_id, leverage
            )
        } else {
            format!(
                "{{\"symbol\":{},\"positionType\":{kind},\"openType\":{open_type},\"leverage\":{leverage},\"marginSelected\":false,\"leverageSelected\":false}}",
                quote(&contract.symbol)?
            )
        };
        self.post(CHANGE_LEVERAGE, body).await?;
        let rows: Vec<Leverage> = self
            .get(LEVERAGE, &[("symbol", contract.symbol.clone())])
            .await?;
        let mut matches = rows
            .iter()
            .filter(|row| row.position_type == kind && row.open_type == open_type);
        let row = matches
            .next()
            .ok_or_else(|| err("missing leverage readback"))?;
        if matches.next().is_some() || row.open_type != open_type || row.leverage.0 != leverage {
            return Err(err(
                "selected margin mode/leverage readback mismatch; no order submitted",
            ));
        }
        Ok(())
    }

    async fn remote(&self, entry: &JournalEntry) -> ArbResult<RemoteOrder> {
        self.get(
            &format!(
                "/api/v1/private/order/external/{}/{}",
                entry.instrument, entry.venue_client_id
            ),
            &[],
        )
        .await
    }

    async fn by_id(&self, id: &str) -> ArbResult<RemoteOrder> {
        validate_id(id)?;
        let row: RemoteOrder = self
            .get(&format!("/api/v1/private/order/get/{id}"), &[])
            .await?;
        if row.order_id != id {
            return Err(err("order ID mismatch"));
        }
        Ok(row)
    }

    async fn state(
        &self,
        row: RemoteOrder,
        entry: Option<&JournalEntry>,
        contract: &Contract,
    ) -> ArbResult<OrderState> {
        let order = verified_order(&row, entry, contract)?;
        let deals: Vec<Deal> = self
            .get(
                &format!("/api/v1/private/order/deal_details/{}", row.order_id),
                &[],
            )
            .await?;
        let (filled, notional, fees) = aggregate(&deals, &row, contract.contract_size.0)?;
        let status = map_status(row.state, row.vol.0, row.deal_vol.0)?;
        Ok(OrderState {
            order,
            venue_order_id: Some(row.order_id),
            status,
            filled_usdt: notional,
            average_price: if filled == Decimal::ZERO {
                None
            } else {
                Some(divide(notional, filled)?)
            },
            fee_usdt: fees,
            reject_reason: (status == OrderStatus::Rejected)
                .then(|| format!("MEXC errorCode={}", row.error_code)),
        })
    }

    async fn lookup(
        &self,
        journal: &mut OrderJournal,
        id: &ClientOrderId,
    ) -> ArbResult<Option<OrderState>> {
        let Some(entry) = journal.get(id).cloned() else {
            return Ok(None);
        };
        if let Some(state) = entry.terminal {
            return Ok(Some(state));
        }
        // Never map missing/expired remote history to None for a durable intent.
        let row = self.remote(&entry).await?;
        let contract = self.contract(&entry.instrument).await?;
        let state = self.state(row, Some(&entry), &contract).await?;
        if !state.status.is_live() {
            journal.record_terminal(&state)?;
        }
        Ok(Some(state))
    }
}

#[async_trait]
impl Broker for MexcBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let contract = self.resolve(symbol).await?;
        let rows = self.raw_positions().await?;
        let Some(row) = rows
            .iter()
            .find(|row| row.symbol == contract.symbol && row.hold_vol.0 > Decimal::ZERO)
        else {
            return Ok(None);
        };
        let mode = match row.open_type {
            1 => Some(crate::MarginMode::Isolated),
            2 => Some(crate::MarginMode::Cross),
            _ => None,
        };
        // 官方 liquidatePrice 只说明逐仓口径，不能当作全仓账户风险。
        let liquidation = if mode == Some(crate::MarginMode::Isolated) {
            row.liquidate_price.as_ref().map(|n| n.0)
        } else {
            None
        };
        Ok(Some(crate::margin::venue_state(
            mode,
            liquidation,
            row.im.as_ref().map(|n| n.0),
        )))
    }

    async fn prepare_open_mode(
        &self,
        symbol: &Symbol,
        side: Side,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.options.authorize(Venue::Mexc)?;
        let _guard = self.journal.lock().await;
        self.check_mode().await?;
        let contract = self.resolve(symbol).await?;
        let leverage = crate::margin::leverage(Venue::Mexc, leverage)?;
        if leverage < contract.min_leverage.0 || leverage > contract.max_leverage.0 {
            return Err(err("leverage exceeds contract limits"));
        }
        self.configure(&contract, side, leverage, mode).await
    }

    fn venue(&self) -> Venue {
        Venue::Mexc
    }
    fn fee_per_side(&self) -> Decimal {
        self.fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.options.authorize(Venue::Mexc)?;
        if order.venue != Venue::Mexc
            || order.client_order_id.0.is_empty()
            || order.notional_usdt <= Decimal::ZERO
        {
            return Err(err("wrong venue, empty client ID, or nonpositive notional"));
        }
        let mut journal = self.journal.lock().await;
        if let Some(entry) = journal.get(&order.client_order_id) {
            if !same_intent(&entry.order, order) {
                return Err(err("client ID reused with different intent"));
            }
            return ack(&self
                .lookup(&mut journal, &order.client_order_id)
                .await?
                .ok_or_else(|| err("missing recorded order"))?);
        }
        self.check_mode().await?;
        let contract = self.resolve(&order.symbol).await?;
        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| err("opens require explicit leverage"))?;
            if !value.fract().is_zero()
                || value < contract.min_leverage.0
                || value > contract.max_leverage.0
            {
                return Err(err("leverage must be integral and within contract limits"));
            }
            Some(value)
        };
        let mut price = self.bounded_price(order, &contract).await?;
        let units = order_units(
            Venue::Mexc,
            order,
            price,
            contract.contract_size.0,
            contract.vol_unit.0,
            contract.min_vol.0,
        )?;
        if units > contract.max_vol.0 {
            return Err(err("quantity exceeds contract maxVol"));
        }
        // The documented minimum is minVol contracts, not a separate quote minimum.
        let position = if order.reduce_only {
            let rows = self.raw_positions().await?;
            let p = rows
                .into_iter()
                .find(|p| p.symbol == contract.symbol && p.hold_vol.0 > Decimal::ZERO)
                .ok_or_else(|| err("no position to reduce"))?;
            let expected = if order.side == Side::Sell { 1 } else { 2 };
            if p.position_type != expected
                || p.state != 1
                || p.open_type != if order.margin_mode.is_cross() { 2 } else { 1 }
                || units > p.hold_vol.0 - p.frozen_vol.0
            {
                return Err(err(
                    "reduce-only direction, available size, or margin mode mismatch",
                ));
            }
            Some(p.position_id)
        } else {
            None
        };
        let entry = JournalEntry {
            order: order.clone(),
            venue_client_id: venue_client_id("mexc", &order.client_order_id, 32),
            instrument: contract.symbol.clone(),
            units,
            terminal: None,
        };
        // Leverage is a per-symbol setting, not an order: configure and read it back first,
        // so a failed readback leaves no reserved ID that can never resolve.
        if let Some(leverage) = leverage {
            self.configure(&contract, order.side, leverage, order.margin_mode)
                .await?;
        }
        if order.limit_price.is_none() {
            // Configuration round trips may age the first book. Refresh immediately
            // before signing, tightening (never widening) the original price bound.
            let fresh = self.bounded_price(order, &contract).await?;
            price = if order.side == Side::Buy {
                price.min(fresh)
            } else {
                price.max(fresh)
            };
        }
        // Reserve BEFORE the order request: a timeout or crash never permits a replay.
        journal.reserve(entry.clone())?;
        let open_type = if order.margin_mode.is_cross() { 2 } else { 1 };
        let body = format!(
            "{{\"symbol\":{},\"price\":{price},\"vol\":{units},\"side\":{},\"type\":3,\"openType\":{open_type},\"positionMode\":2,\"reduceOnly\":{},\"externalOid\":{}{}{}}}",
            quote(&contract.symbol)?,
            side_code(order.side, order.reduce_only),
            order.reduce_only,
            quote(&entry.venue_client_id)?,
            leverage
                .map(|n| format!(",\"leverage\":{n}"))
                .unwrap_or_default(),
            position
                .map(|id| format!(",\"positionId\":{id}"))
                .unwrap_or_default(),
        );
        // The acknowledgement, including errors/timeouts, is never a fill.
        let submission = self.post(CREATE, body).await;
        // 成功时 data 是 {orderId, ts}（place-order 文档），先按 orderId 直查一次；
        // 拿不到再按 externalOid 轮询。
        if let Ok(data) = &submission
            && let Ok(created) = decode::<Created>(data.get())
            && let Ok(row) = self.by_id(&created.order_id).await
        {
            let state = self.state(row, Some(&entry), &contract).await?;
            if !state.status.is_live() {
                journal.record_terminal(&state)?;
            }
            return ack(&state);
        }
        for attempt in 0..10 {
            if attempt != 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if let Ok(row) = self.remote(&entry).await {
                let state = self.state(row, Some(&entry), &contract).await?;
                if !state.status.is_live() {
                    journal.record_terminal(&state)?;
                }
                return ack(&state);
            }
        }
        match submission {
            Ok(_) => Err(err(
                "submitted order not queryable; durable intent retained, do not resubmit",
            )),
            Err(WriteError::Api { code, message }) => {
                Err(settle_failure(&mut journal, order, Some(code), message)?)
            }
            Err(WriteError::Unknown(error)) => Err(error),
        }
    }

    async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        self.lookup(&mut *self.journal.lock().await, id).await
    }

    async fn cancel(&self, id: &str) -> ArbResult<()> {
        self.options.authorize(Venue::Mexc)?;
        let mut journal = self.journal.lock().await;
        let before = self.by_id(id).await?;
        if !map_status(before.state, before.vol.0, before.deal_vol.0)?.is_live() {
            return Ok(());
        }
        let outcome = self.post(CANCEL, format!("{{\"orderIds\":[{id}]}}")).await;
        // Even cancellation timeouts must be followed by a read.
        let after = self.by_id(id).await?;
        if map_status(after.state, after.vol.0, after.deal_vol.0)?.is_live() {
            outcome?;
            return Err(err("cancel not confirmed; order remains live"));
        }
        if let Some(entry) = journal.by_venue_client_id(&after.external_oid).cloned() {
            let contract = self.contract(&after.symbol).await?;
            let state = self.state(after, Some(&entry), &contract).await?;
            journal.record_terminal(&state)?;
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let mut seen = HashSet::new();
        let mut states = Vec::new();
        let mut page = 1_u32;
        loop {
            let rows: Vec<RemoteOrder> = self
                .get(
                    OPEN_ORDERS,
                    &[("page_num", page.to_string()), ("page_size", "100".into())],
                )
                .await?;
            let count = rows.len();
            for row in rows {
                if !seen.insert(row.order_id.clone()) {
                    return Err(err(
                        "open-order pagination repeated an order; reconcile again",
                    ));
                }
                if !row.symbol.ends_with("_USDT") {
                    continue;
                }
                let contract = self.contract(&row.symbol).await?;
                if contract.scanner_symbol().is_none() {
                    continue;
                }
                let entry = self
                    .journal
                    .lock()
                    .await
                    .by_venue_client_id(&row.external_oid)
                    .cloned();
                let state = self.state(row, entry.as_ref(), &contract).await?;
                if state.status.is_live() {
                    states.push(state);
                }
            }
            if count < 100 {
                return Ok(states);
            }
            page = page
                .checked_add(1)
                .ok_or_else(|| err("pagination overflow"))?;
        }
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        self.check_mode().await?;
        let mut out = Vec::new();
        for row in self.raw_positions().await? {
            if row.hold_vol.0 == Decimal::ZERO {
                continue;
            }
            let contract = self.contract(&row.symbol).await?;
            let symbol = contract.scanner_symbol().ok_or_else(|| {
                err("nonzero unsupported position; reconcile outside this broker")
            })?;
            if row.hold_avg_price.0 <= Decimal::ZERO {
                return Err(err("invalid position average price"));
            }
            let quantity = multiply(row.hold_vol.0, contract.contract_size.0)?;
            out.push(VenuePosition {
                venue: Venue::Mexc,
                symbol,
                net_quantity: if row.position_type == 1 {
                    quantity
                } else {
                    -quantity
                },
                average_price: Some(row.hold_avg_price.0),
                // Endpoint has no marked notional; report entry-price notional.
                notional_usdt: multiply(quantity, row.hold_avg_price.0)?,
            });
        }
        Ok(out)
    }
}

fn err(message: impl Into<String>) -> ArbError {
    ArbError::venue("mexc", message)
}
fn decode<T: DeserializeOwned>(raw: &str) -> ArbResult<T> {
    serde_json::from_str(raw).map_err(|_| err("invalid MEXC response schema"))
}

/// 一次写请求的结果：把交易所**明确拒绝**与**结果未知**结构化地区分开。
///
/// `ArbError` 定义在 arb-core，不能携带 API `code`，所以这层私有错误只在本文件内使用；
/// `place` 靠 `Api { code }` 判断该不该把订单记成终态 `Rejected`。
enum WriteError {
    /// 响应可解析且 `success=false` / `code!=0`：`code` 是结构化的交易所业务码。
    Api { code: i64, message: String },
    /// 传输失败、超时、HTTP 5xx/429、无法解析的响应：结果未知，绝不能记成拒单。
    Unknown(ArbError),
}

impl From<WriteError> for ArbError {
    fn from(failure: WriteError) -> Self {
        match failure {
            WriteError::Api { message, .. } => err(message),
            WriteError::Unknown(error) => error,
        }
    }
}

/// 解析写请求的响应。文档 place-order：失败时 `success=false` 且 `data=null`
/// （<https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order>）。
/// 429/5xx 与 [`AMBIGUOUS_CODES`] 一律归为「结果未知」，交给操作者按日志对账。
fn classify_write(status: StatusCode, bytes: &[u8]) -> Result<Box<RawValue>, WriteError> {
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        // 请求可能已经落到交易所：不能当成拒单。
        return Err(WriteError::Unknown(err(format!(
            "HTTP {status}; outcome unknown; reconcile intent"
        ))));
    }
    let envelope: Envelope = match serde_json::from_slice(bytes) {
        Ok(envelope) => envelope,
        Err(_) => {
            return Err(WriteError::Unknown(err(format!(
                "HTTP {status}; unparseable response; reconcile intent"
            ))));
        }
    };
    if status.is_success() && envelope.success && envelope.code == 0 {
        // 成功的写操作（如 change_leverage）可以不带 data。
        return match envelope.data {
            Some(data) => Ok(data),
            None => RawValue::from_string("null".into())
                .map_err(|_| WriteError::Unknown(err("invalid null JSON"))),
        };
    }
    // Codes only: even an error message could contain reflected credentials.
    let message = format!("HTTP {status}; API code {}", envelope.code);
    if AMBIGUOUS_CODES.contains(&envelope.code) {
        return Err(WriteError::Unknown(err(message)));
    }
    Err(WriteError::Api {
        code: envelope.code,
        message,
    })
}

/// 文档明确列为「下单前拒绝」的业务码
/// （<https://www.mexc.com/api-docs/futures/error-code>）：2005 余额不足、
/// 2011 下单数量错误、2015 价格/数量精度错误、2028 超过单笔最大数量、
/// 2070 价格与 bid1/ask1 距离超过 5%。其余码结果未知，必须保留未决意图。
fn definitive_rejection(code: i64) -> bool {
    matches!(code, 2005 | 2011 | 2015 | 2028 | 2070)
}

/// 提交失败后的统一收尾（`place` 调用）：只有明确的业务拒单才记终态 `Rejected`，
/// 之后 `order_state` 如实返回；其余情况不写日志，意图保持未决等待对账。
fn settle_failure(
    journal: &mut OrderJournal,
    order: &NewOrder,
    code: Option<i64>,
    message: String,
) -> ArbResult<ArbError> {
    if let Some(code) = code
        && definitive_rejection(code)
    {
        let mut rejected = OrderState::new(order.clone());
        rejected.status = OrderStatus::Rejected;
        rejected.reject_reason = Some(message.clone());
        journal.record_terminal(&rejected)?;
    }
    Ok(err(message))
}

/// MEXC 硬拒与 bid1/ask1 距离超过 5% 的限价（错误码 2070）。`LiveOptions::bound_price` 按
/// 配置的 `market_slippage` 放大盘口价，0.05 时正好落在 5% 边界上；这里收紧到 4.9%
/// （留 0.1% 余量）。`round_price` 买单向下、卖单向上取整都只会让距离变小，因此最终价格
/// 一定 ≤4.9%，即使 slippage 配成 0.05 也不会触发 2070。
fn clamp_touch_bound(side: Side, bound: Decimal, bid: Decimal, ask: Decimal) -> ArbResult<Decimal> {
    let cap = Decimal::new(49, 3); // 0.049
    match side {
        Side::Buy => Ok(bound.min(multiply(ask, Decimal::ONE + cap)?)),
        Side::Sell => Ok(bound.max(multiply(bid, Decimal::ONE - cap)?)),
    }
}

fn quote(value: &str) -> ArbResult<String> {
    serde_json::to_string(value).map_err(|_| err("JSON encoding failed"))
}
fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b)
        .ok_or_else(|| err("decimal product overflow"))
}
fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b)
        .ok_or_else(|| err("decimal division overflow"))
}
fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b).ok_or_else(|| err("decimal sum overflow"))
}
fn sensitive(value: &str) -> ArbResult<reqwest::header::HeaderValue> {
    let mut header = reqwest::header::HeaderValue::from_str(value)
        .map_err(|_| err("invalid credential header"))?;
    header.set_sensitive(true);
    Ok(header)
}
fn sign(secret: &str, key: &str, timestamp: &str, payload: &str) -> String {
    hex_lower(&hmac_sha256(
        secret.as_bytes(),
        format!("{key}{timestamp}{payload}").as_bytes(),
    ))
}
fn query_string(params: &[(&str, String)]) -> String {
    let mut params: Vec<_> = params.iter().collect();
    params.sort_unstable_by_key(|(key, _)| *key);
    params
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}
fn encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::new();
    for b in value.bytes() {
        // Match the official Java URLEncoder + replace('+', "%20") example.
        if b.is_ascii_alphanumeric() || b"-._*".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[(b >> 4) as usize]));
            out.push(char::from(HEX[(b & 15) as usize]));
        }
    }
    out
}
fn validate_id(id: &str) -> ArbResult<()> {
    if id.is_empty()
        || !id.bytes().all(|b| b.is_ascii_digit())
        || id.parse::<u64>().ok().filter(|id| *id > 0).is_none()
    {
        return Err(err("invalid numeric venue ID"));
    }
    Ok(())
}
fn side_code(side: Side, reduce: bool) -> i64 {
    match (side, reduce) {
        (Side::Buy, false) => 1,
        (Side::Buy, true) => 2,
        (Side::Sell, false) => 3,
        (Side::Sell, true) => 4,
    }
}
fn map_status(code: i64, requested: Decimal, filled: Decimal) -> ArbResult<OrderStatus> {
    if requested <= Decimal::ZERO || filled < Decimal::ZERO || filled > requested {
        return Err(err("invalid executed quantity"));
    }
    match code {
        1 => Ok(OrderStatus::Pending),
        2 => Ok(OrderStatus::Open),
        3 if requested == filled => Ok(OrderStatus::Filled),
        3 | 4 => Ok(OrderStatus::Cancelled),
        5 if filled == Decimal::ZERO => Ok(OrderStatus::Rejected),
        _ => Err(err("unknown or inconsistent MEXC order status")),
    }
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
        status: state.status,
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| err("missing venue order ID"))?,
    })
}
fn verified_order(
    row: &RemoteOrder,
    entry: Option<&JournalEntry>,
    c: &Contract,
) -> ArbResult<NewOrder> {
    validate_id(&row.order_id)?;
    if row.symbol != c.symbol {
        return Err(err("order contract mismatch"));
    }
    let symbol = c
        .scanner_symbol()
        .ok_or_else(|| err("unsupported order contract"))?;
    let side = match row.side {
        1 | 2 => Side::Buy,
        3 | 4 => Side::Sell,
        _ => return Err(err("unknown order side")),
    };
    if let Some(e) = entry {
        if e.instrument != row.symbol
            || e.venue_client_id != row.external_oid
            || e.units != row.vol.0
            || e.order.symbol != symbol
            || side_code(e.order.side, e.order.reduce_only) != row.side
            || row.open_type != if e.order.margin_mode.is_cross() { 2 } else { 1 }
            || row.position_mode != 2
            || row.order_type != 3
            // 两个单笔查询端点（get-order-by-external-id、get-order-information-by-order-id，
            // https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/）的文档与
            // 示例都没有 reduceOnly 字段。因此缺失视为未知（接受），只有明确返回 false 才是冲突；
            // side 编码 1-4 + positionId + units + externalOid 已经把意图锁死。
            || (e.order.reduce_only && row.reduce_only == Some(false))
        {
            return Err(err(
                "remote order does not match intent/margin mode/one-way mode; reconcile exposure",
            ));
        }
        return Ok(e.order.clone());
    }
    if row.price.0 <= Decimal::ZERO || row.vol.0 <= Decimal::ZERO {
        return Err(err("invalid external order quantity/price"));
    }
    let quantity = multiply(row.vol.0, c.contract_size.0)?;
    Ok(NewOrder {
        margin_mode: if row.open_type == 2 {
            crate::MarginMode::Cross
        } else {
            crate::MarginMode::Isolated
        },
        client_order_id: ClientOrderId(format!("mexc-external-{}", row.order_id)),
        venue: Venue::Mexc,
        symbol,
        side,
        quantity: Some(quantity),
        notional_usdt: multiply(quantity, row.price.0)?,
        limit_price: Some(row.price.0),
        reduce_only: row.reduce_only.unwrap_or(matches!(row.side, 2 | 4)),
        leverage: Some(row.leverage.0),
    })
}
fn aggregate(
    deals: &[Deal],
    row: &RemoteOrder,
    unit: Decimal,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    if unit <= Decimal::ZERO {
        return Err(err("invalid contract size"));
    }
    let mut ids = HashSet::new();
    let (mut contracts, mut base, mut quote, mut fees) =
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    for fill in deals {
        if fill.order_id != row.order_id
            || fill.symbol != row.symbol
            || fill.side != row.side
            || !ids.insert(&fill.id)
            || fill.fee_currency != "USDT"
            || fill.vol.0 <= Decimal::ZERO
            || fill.price.0 <= Decimal::ZERO
        {
            return Err(err("invalid/duplicate/foreign-currency deal"));
        }
        let qty = multiply(fill.vol.0, unit)?;
        contracts = add(contracts, fill.vol.0)?;
        base = add(base, qty)?;
        quote = add(quote, multiply(qty, fill.price.0)?)?;
        fees = add(fees, fill.fee.0)?;
    }
    if contracts != row.deal_vol.0 {
        return Err(err("deals do not sum to executed order quantity"));
    }
    Ok((base, quote, fees))
}
fn validate_positions(rows: &[Position]) -> ArbResult<()> {
    let mut symbols = HashSet::new();
    for row in rows {
        if row.hold_vol.0 < Decimal::ZERO
            || row.frozen_vol.0 < Decimal::ZERO
            || row.frozen_vol.0 > row.hold_vol.0
        {
            return Err(err("invalid position size"));
        }
        if row.hold_vol.0 == Decimal::ZERO {
            continue;
        }
        if !matches!(row.position_type, 1 | 2)
            || !matches!(row.state, 1 | 2)
            || !symbols.insert(&row.symbol)
        {
            return Err(err(
                "hedged/duplicate or unknown nonzero position; reconcile account",
            ));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct Envelope {
    success: bool,
    code: i64,
    data: Option<Box<RawValue>>,
}

/// `positionMode` 的两种可能形状。文档参数表写的是 `positionMode` 字段，WS 推送也是
/// `{"positionMode":2}`（<https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-user-position-mode>），
/// 但该页的示例 JSON 是复制错误（贴成了风险限额数组，与 `positionMode` 毫无关系）。
/// 因此裸整数与对象两种形状都接受，避免真实返回是对象时整个券商不可用。
#[derive(Deserialize)]
#[serde(untagged)]
enum PositionMode {
    Bare(i64),
    Object {
        #[serde(rename = "positionMode")]
        mode: i64,
    },
}

impl PositionMode {
    fn value(&self) -> i64 {
        match self {
            Self::Bare(mode) | Self::Object { mode } => *mode,
        }
    }
}

/// Preserve every digit of numeric JSON: never pass financial fields through f64.
#[derive(Clone, Copy, Debug)]
struct Number(Decimal);
impl<'de> Deserialize<'de> for Number {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(de)?;
        let text = raw.get();
        let number = if text.starts_with('"') {
            serde_json::from_str::<String>(text).map_err(serde::de::Error::custom)?
        } else {
            text.to_string()
        };
        Decimal::from_str_exact(&number)
            .or_else(|_| Decimal::from_scientific(&number))
            .map(Self)
            .map_err(|_| serde::de::Error::custom("invalid exact decimal"))
    }
}
fn id<'de, D: Deserializer<'de>>(de: D) -> Result<String, D::Error> {
    let raw = Box::<RawValue>::deserialize(de)?;
    let text = raw.get();
    let result = if text.starts_with('"') {
        serde_json::from_str::<String>(text).map_err(serde::de::Error::custom)?
    } else {
        text.into()
    };
    validate_id(&result).map_err(|_| serde::de::Error::custom("invalid ID"))?;
    Ok(result)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Contract {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    settle_coin: String,
    future_type: i64,
    state: i64,
    api_allowed: bool,
    position_open_type: i64,
    contract_size: Number,
    price_unit: Number,
    vol_unit: Number,
    min_vol: Number,
    max_vol: Number,
    min_leverage: Number,
    max_leverage: Number,
}
impl Contract {
    fn scanner_symbol(&self) -> Option<Symbol> {
        // Scanner strips only the native quote suffix, never multiplier prefixes.
        let base = self.symbol.strip_suffix("_USDT")?;
        (self.quote_coin == "USDT"
            && self.settle_coin == "USDT"
            && self.future_type == 1
            && !base.is_empty()
            && base == self.base_coin)
            .then(|| Symbol::perp(base, "USDT"))
    }
    fn validate_units(&self) -> ArbResult<()> {
        if self.contract_size.0 <= Decimal::ZERO
            || self.price_unit.0 <= Decimal::ZERO
            || self.vol_unit.0 <= Decimal::ZERO
            || self.min_vol.0 <= Decimal::ZERO
            || self.max_vol.0 < self.min_vol.0
            || self.min_leverage.0 < Decimal::ONE
            || self.max_leverage.0 < self.min_leverage.0
        {
            return Err(err("invalid contract specifications"));
        }
        Ok(())
    }
}
#[derive(Deserialize)]
struct Book {
    bids: Vec<Vec<Number>>,
    asks: Vec<Vec<Number>>,
    timestamp: i64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fees {
    real_taker_fee: Number,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Leverage {
    position_type: i64,
    open_type: i64,
    leverage: Number,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Position {
    position_id: u64,
    symbol: String,
    position_type: i64,
    open_type: i64,
    state: i64,
    hold_vol: Number,
    frozen_vol: Number,
    hold_avg_price: Number,
    #[serde(default)]
    liquidate_price: Option<Number>,
    #[serde(default)]
    im: Option<Number>,
}
/// `/order/create` 成功时的 data：`orderId`（文档示例是字符串，数字也接受）与 `ts`。
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Created {
    #[serde(deserialize_with = "id")]
    order_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    #[serde(deserialize_with = "id")]
    order_id: String,
    symbol: String,
    external_oid: String,
    side: i64,
    state: i64,
    error_code: i64,
    vol: Number,
    deal_vol: Number,
    price: Number,
    leverage: Number,
    open_type: i64,
    position_mode: i64,
    order_type: i64,
    reduce_only: Option<bool>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Deal {
    #[serde(deserialize_with = "id")]
    id: String,
    #[serde(deserialize_with = "id")]
    order_id: String,
    symbol: String,
    side: i64,
    vol: Number,
    price: Number,
    fee: Number,
    fee_currency: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order() -> NewOrder {
        NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("pair-buy-0".into()),
            venue: Venue::Mexc,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(100),
            quantity: Some(dec!(0.001)),
            limit_price: Some(dec!(100000)),
            reduce_only: false,
            leverage: Some(dec!(5)),
        }
    }
    fn remote() -> RemoteOrder {
        decode(r#"{"orderId":"10","symbol":"BTC_USDT","externalOid":"abc","side":1,"state":3,"errorCode":0,"vol":2,"dealVol":2,"price":100,"leverage":5,"openType":1,"positionMode":2,"orderType":3,"reduceOnly":false}"#).unwrap()
    }
    fn contract() -> Contract {
        decode(r#"{"symbol":"BTC_USDT","baseCoin":"BTC","quoteCoin":"USDT","settleCoin":"USDT","futureType":1,"state":0,"apiAllowed":true,"positionOpenType":3,"contractSize":0.01,"priceUnit":0.1,"volUnit":1,"minVol":1,"maxVol":100000,"minLeverage":1,"maxLeverage":500}"#).unwrap()
    }
    fn deals() -> Vec<Deal> {
        decode(r#"[{"id":1,"orderId":10,"symbol":"BTC_USDT","side":1,"vol":1,"price":100,"fee":0.01,"feeCurrency":"USDT"},{"id":2,"orderId":10,"symbol":"BTC_USDT","side":1,"vol":1,"price":200,"fee":-0.002,"feeCurrency":"USDT"}]"#).unwrap()
    }
    #[test]
    fn signatures_pin_independent_get_and_post_vectors() {
        assert_eq!(
            sign(
                "FAKESECRET",
                "FAKEKEY",
                "1700000000000",
                "page_num=1&symbol=BTC_USDT"
            ),
            "362f2c4af59f80ce96e44da23ed055217ba8ffc9eb1a087d36912ed1269a53ee"
        );
        let body =
            r#"{"symbol":"BTC_USDT","price":100000.1,"vol":1,"side":1,"type":3,"openType":1}"#;
        assert_eq!(
            sign("FAKESECRET", "FAKEKEY", "1700000000000", body),
            "d343b33de5a0eb7328584bbea8e754963d0678748295ca83f9ff86c10cef0170"
        );
        assert_eq!(
            query_string(&[("symbol", "BTC_USDT".into()), ("ids", "1,2 ~*".into())]),
            "ids=1%2C2%20%7E*&symbol=BTC_USDT"
        );
    }
    #[test]
    fn exact_decimal_survives_raw_envelope() {
        let envelope: Envelope = serde_json::from_str(
            r#"{"success":true,"code":0,"data":{"realTakerFee":0.000000000000000123456789}}"#,
        )
        .unwrap();
        let fee: Fees = decode(envelope.data.unwrap().get()).unwrap();
        assert_eq!(fee.real_taker_fee.0, dec!(0.000000000000000123456789));
        assert_eq!(decode::<Number>("1e-12").unwrap().0, dec!(0.000000000001));
    }
    #[test]
    fn contract_conversion_and_exact_exits() {
        let mut order = order();
        assert_eq!(
            order_units(
                Venue::Mexc,
                &order,
                dec!(100000),
                dec!(0.0001),
                dec!(1),
                dec!(1)
            )
            .unwrap(),
            dec!(10)
        );
        order.quantity = Some(dec!(0.00105));
        assert_eq!(
            order_units(
                Venue::Mexc,
                &order,
                dec!(100000),
                dec!(0.0001),
                dec!(1),
                dec!(1)
            )
            .unwrap(),
            dec!(10)
        );
        order.reduce_only = true;
        assert!(
            order_units(
                Venue::Mexc,
                &order,
                dec!(100000),
                dec!(0.0001),
                dec!(1),
                dec!(1)
            )
            .is_err()
        );
        assert_eq!(
            (
                side_code(Side::Buy, false),
                side_code(Side::Buy, true),
                side_code(Side::Sell, false),
                side_code(Side::Sell, true)
            ),
            (1, 2, 3, 4)
        );
        let cid = venue_client_id("mexc", &order.client_order_id, 32);
        assert_eq!(cid.len(), 32);
        assert!(cid.bytes().all(|b| b.is_ascii_alphanumeric()));
    }
    #[test]
    fn terminal_partial_and_unknown_statuses() {
        assert_eq!(
            map_status(3, dec!(2), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            map_status(3, dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status(4, dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status(5, dec!(2), dec!(0)).unwrap(),
            OrderStatus::Rejected
        );
        assert_eq!(
            map_status(1, dec!(2), dec!(0)).unwrap(),
            OrderStatus::Pending
        );
        assert_eq!(map_status(2, dec!(2), dec!(1)).unwrap(), OrderStatus::Open);
        assert!(map_status(99, dec!(2), dec!(0)).is_err());
        assert!(map_status(3, dec!(2), dec!(3)).is_err());
        assert!(map_status(5, dec!(2), dec!(1)).is_err());
    }
    #[test]
    fn fills_convert_contracts_before_weighting_and_keep_rebates() {
        let (base, quote, fee) = aggregate(&deals(), &remote(), dec!(0.01)).unwrap();
        assert_eq!((base, quote, fee), (dec!(0.02), dec!(3), dec!(0.008)));
        assert_eq!(divide(quote, base).unwrap(), dec!(150));
        let mut foreign = deals();
        foreign[0].fee_currency = "MX".into();
        assert!(aggregate(&foreign, &remote(), dec!(0.01)).is_err());
        let mut duplicate = deals();
        duplicate[1].id = duplicate[0].id.clone();
        assert!(aggregate(&duplicate, &remote(), dec!(0.01)).is_err());
        assert!(aggregate(&deals()[..1], &remote(), dec!(0.01)).is_err());
    }
    #[test]
    fn returned_isolation_and_identity_are_verified() {
        let row = remote();
        let mut intent = order();
        intent.symbol = Symbol::perp("BTC", "USDT");
        let entry = JournalEntry {
            order: intent,
            venue_client_id: "abc".into(),
            instrument: "BTC_USDT".into(),
            units: dec!(2),
            terminal: None,
        };
        assert_eq!(
            verified_order(&row, Some(&entry), &contract())
                .unwrap()
                .client_order_id,
            entry.order.client_order_id
        );
        let mut cross = remote();
        cross.open_type = 2;
        assert!(verified_order(&cross, Some(&entry), &contract()).is_err());
        let mut mismatch = remote();
        mismatch.external_oid = "other".into();
        assert!(verified_order(&mismatch, Some(&entry), &contract()).is_err());
        let external = verified_order(&row, None, &contract()).unwrap();
        assert_eq!(external.client_order_id.0, "mexc-external-10");
        assert_eq!(external.quantity, Some(dec!(0.02)));
    }
    #[test]
    fn hedged_and_invalid_positions_fail_closed() {
        let rows: Vec<Position> = decode(r#"[{"positionId":1,"symbol":"BTC_USDT","positionType":1,"openType":1,"state":1,"holdVol":5,"frozenVol":0,"holdAvgPrice":100},{"positionId":2,"symbol":"BTC_USDT","positionType":2,"openType":1,"state":1,"holdVol":3,"frozenVol":0,"holdAvgPrice":100}]"#).unwrap();
        assert!(validate_positions(&rows).is_err());
        assert!(validate_positions(&rows[..1]).is_ok());
        assert_eq!(
            multiply(rows[0].hold_vol.0, dec!(0.0001)).unwrap(),
            dec!(0.0005)
        );
    }

    /// 独立临时订单日志：每个测试用不同文件名，避免并行运行时互抢 fs2 独占锁。
    fn test_journal(tag: &str) -> (OrderJournal, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("arb-mexc-test-{tag}-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        (OrderJournal::open(&path, "mexc:test").unwrap(), path)
    }

    fn test_entry(order: &NewOrder) -> JournalEntry {
        JournalEntry {
            order: order.clone(),
            venue_client_id: "abc".into(),
            instrument: "BTC_USDT".into(),
            units: dec!(2),
            terminal: None,
        }
    }

    /// 文档参数表写的是 `positionMode` 字段、WS 推送也是对象，但该页示例 JSON 是复制错误；
    /// 裸整数与对象两种形状都必须能解码，否则真实返回是对象时整个券商不可用。
    #[test]
    fn position_mode_decodes_bare_integer_and_object_shapes() {
        assert_eq!(decode::<PositionMode>("2").unwrap().value(), 2);
        assert_eq!(
            decode::<PositionMode>(r#"{"positionMode":2}"#)
                .unwrap()
                .value(),
            2
        );
        assert_eq!(
            decode::<PositionMode>(r#"{"positionMode":1,"extra":true}"#)
                .unwrap()
                .value(),
            1
        );
        assert!(decode::<PositionMode>("{}").is_err());
        assert!(decode::<PositionMode>(r#"{"mode":2}"#).is_err());
    }

    /// 2005（余额不足）是文档明确的下单前拒绝：必须记成终态，`order_state` 才能如实返回，
    /// 而不是让第二条腿永远停在未决状态。
    #[test]
    fn definitive_refusal_is_recorded_and_returned_as_rejected() {
        match classify_write(
            StatusCode::OK,
            br#"{"success":false,"code":2005,"data":null}"#,
        ) {
            Err(WriteError::Api { code, message }) => {
                assert_eq!(code, 2005);
                assert!(message.contains("2005") && definitive_rejection(code));
            }
            _ => panic!("2005 must be a structured business refusal"),
        }
        for code in [2011, 2015, 2028, 2070] {
            assert!(definitive_rejection(code));
        }
        assert!(!definitive_rejection(2001));
        // 成功响应里的 orderId 供 `place` 按 id 直查（文档示例是字符串，数字也接受）。
        assert_eq!(
            decode::<Created>(r#"{"orderId":"739113577038255616","ts":1761888808839}"#)
                .unwrap()
                .order_id,
            "739113577038255616"
        );
        assert_eq!(
            decode::<Created>(r#"{"orderId":123,"ts":1}"#)
                .unwrap()
                .order_id,
            "123"
        );
        let (mut journal, path) = test_journal("refused");
        let order = order();
        journal.reserve(test_entry(&order)).unwrap();
        let error = settle_failure(
            &mut journal,
            &order,
            Some(2005),
            "HTTP 200; API code 2005".into(),
        )
        .unwrap();
        assert!(error.to_string().contains("2005"));
        let terminal = journal
            .get(&order.client_order_id)
            .unwrap()
            .terminal
            .clone()
            .expect("a definitive refusal must be recorded as a terminal state");
        assert_eq!(terminal.status, OrderStatus::Rejected);
        assert_eq!(terminal.order.client_order_id, order.client_order_id);
        // `order_state`/`lookup` 读到的就是这条终态（有 terminal 就不再往返交易所）。
        assert!(terminal.reject_reason.unwrap().contains("2005"));
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    /// 510/429/5xx 与传输失败都归为「结果未知」：`place` 不写日志、不编造拒单。
    #[test]
    fn ambiguous_and_transport_failures_stay_unresolved() {
        let cases: [(StatusCode, &[u8]); 4] = [
            (
                StatusCode::OK,
                br#"{"success":false,"code":510,"data":null}"#,
            ),
            (
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"success":false,"code":510}"#,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"success":false,"code":500}"#,
            ),
            (StatusCode::OK, b"not json"),
        ];
        for (status, body) in cases {
            match classify_write(status, body) {
                Err(WriteError::Unknown(_)) => {}
                Err(WriteError::Api { code, .. }) => panic!("{code} must stay unknown"),
                Ok(_) => panic!("HTTP {status} error body must not be accepted"),
            }
        }
        assert!(AMBIGUOUS_CODES.contains(&3016));
        assert!(!definitive_rejection(510) && !definitive_rejection(500));
        let (mut journal, path) = test_journal("unknown");
        let order = order();
        journal.reserve(test_entry(&order)).unwrap();
        // 超时在上层是 Err（transport_error 文案「结果未知」），传给 settle_failure 时没有 code。
        settle_failure(
            &mut journal,
            &order,
            None,
            "MEXC write 超时（结果未知）".into(),
        )
        .unwrap();
        assert!(
            journal
                .get(&order.client_order_id)
                .unwrap()
                .terminal
                .is_none()
        );
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    /// 两个单笔查询端点都不返回 `reduceOnly`：缺失按未知接受，明确 false 才是冲突。
    #[test]
    fn verified_order_treats_missing_reduce_only_as_unknown() {
        let mut intent = order();
        intent.side = Side::Buy;
        intent.reduce_only = true;
        let entry = test_entry(&intent);
        let missing: RemoteOrder = decode(
            r#"{"orderId":"10","symbol":"BTC_USDT","externalOid":"abc","side":2,"state":3,"errorCode":0,"vol":2,"dealVol":2,"price":100,"leverage":5,"openType":1,"positionMode":2,"orderType":3}"#,
        )
        .unwrap();
        assert!(
            verified_order(&missing, Some(&entry), &contract())
                .unwrap()
                .reduce_only
        );
        let explicit_false: RemoteOrder = decode(
            r#"{"orderId":"10","symbol":"BTC_USDT","externalOid":"abc","side":2,"state":3,"errorCode":0,"vol":2,"dealVol":2,"price":100,"leverage":5,"openType":1,"positionMode":2,"orderType":3,"reduceOnly":false}"#,
        )
        .unwrap();
        assert!(verified_order(&explicit_false, Some(&entry), &contract()).is_err());
    }

    /// MEXC 拒收距 bid1/ask1 超过 5% 的限价（2070）：slippage 配到 0.05 时也要收紧到 4.9%。
    #[test]
    fn marketable_bound_never_exceeds_five_percent_from_the_touch() {
        let bid = dec!(100);
        let ask = dec!(100);
        let buy = clamp_touch_bound(Side::Buy, dec!(105), bid, ask).unwrap();
        let sell = clamp_touch_bound(Side::Sell, dec!(95), bid, ask).unwrap();
        assert_eq!((buy, sell), (dec!(104.9), dec!(95.1)));
        // tick 取整只会让距离更小（买向下、卖向上）。
        let buy_price = round_price(buy, dec!(0.1), Side::Buy).unwrap();
        let sell_price = round_price(sell, dec!(0.1), Side::Sell).unwrap();
        assert!((buy_price - ask) / ask <= dec!(0.049));
        assert!((bid - sell_price) / bid <= dec!(0.049));
        // 更小的 slippage 不会被放宽。
        assert_eq!(
            clamp_touch_bound(Side::Buy, dec!(100.2), bid, ask).unwrap(),
            dec!(100.2)
        );
        assert_eq!(
            clamp_touch_bound(Side::Sell, dec!(99.8), bid, ask).unwrap(),
            dec!(99.8)
        );
    }
}
