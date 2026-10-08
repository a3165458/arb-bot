//! Production Binance **USDⓈ-M futures** broker (`https://fapi.binance.com`).
//!
//! Scope: **USDT-margined linear perpetuals only**, one-way (net) position mode, single-asset
//! margin mode. Rows on the exchange are matched back to the scanner's
//! `Symbol::perp(base_without_quote_suffix, "USDT")` through live instrument metadata
//! (`baseAsset`/`quoteAsset`/`marginAsset`/`contractType`/`status`), never by string surgery
//! alone. Both `PERPETUAL` and `TRADIFI_PERPETUAL` rows are accepted because the scanner's
//! symbol space (any `premiumIndex` symbol ending in `USDT` without `_`) includes both; unknown
//! contract types and non-`TRADING` rows are refused rather than guessed.
//!
//! Construction is read-only: it syncs the server clock, verifies the credentials, requires
//! **one-way** position mode and **single-asset** margin mode (it fails with an actionable
//! message instead of changing account-wide settings), and reads the account's actual taker
//! commission rate. There is no `Debug`/`Serialize` on the credentials: the secret must never
//! reach a log line or an error string.
//!
//! Every order is `LIMIT` + `IOC`; there are no unbounded market orders. An order without a
//! limit price is priced from a freshly fetched order book inside the configured slippage
//! bound. Opens require explicit isolated leverage and set the per-symbol `marginType` and
//! `leverage`, which are read back before the order is signed. Quantities never round up,
//! reduce-only quantities must already sit on the venue step, and fees come from the actual
//! fills (`/fapi/v1/userTrades`) which must be charged in USDT.
//!
//! The exclusive intent journal is fsynced BEFORE the order request. A reserved ID is
//! query-only forever — never delete or reuse the journal to retry a timeout. A request the
//! venue definitively refused is recorded as `Rejected`; a request whose outcome is unknown
//! (transport failure, `-1000/-1001/-1006/-1007`, unparsable response) is looked up by
//! `origClientOrderId` and reported as an error when the venue has no record.
//!
//! Protocol sources (the HMAC vector is reproduced offline; live probes were GET-only):
//! - Signing / `X-MBX-APIKEY` / `recvWindow` / official HMAC vector:
//!   <https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info>
//! - Error codes: <https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code>
//! - New order: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order>
//! - Query order: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Query-Order>
//! - Cancel order: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Order>
//! - All open orders: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Current-All-Open-Orders>
//! - Account trade list: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Account-Trade-List>
//! - Change margin type: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Change-Margin-Type>
//! - Change initial leverage: <https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Change-Initial-Leverage>
//! - Position mode: <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Get-Current-Position-Mode>
//! - Multi-assets mode: <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Get-Current-Multi-Assets-Mode>
//! - User commission rate: <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/User-Commission-Rate>
//! - Leverage brackets: <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Notional-and-Leverage-Brackets>
//! - Position information (V2): <https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Position-Information-V2>
//! - Exchange information: <https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information>
//! - Conditional orders: <https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/Current-All-Algo-Open-Orders>
//! - Conditional lookup/cancel: <https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/Query-Algo-Order>
//!   <https://developers.binance.com/legacy-docs/derivatives/usds-margined-futures/trade/rest-api/Cancel-Algo-Order>
//!
//! `fee_per_side` is the authenticated BTCUSDT reference rate, not a claim that promotional
//! per-symbol rates are identical. Every execution uses actual USDT commissions. Disable BNB
//! fee deduction; an order charged in BNB fails reconciliation instead of guessing conversion.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Mutex as SyncMutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, hex_lower, hmac_sha256, order_units, round_price,
    transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE_URL: &str = "https://fapi.binance.com";
const VENUE: Venue = Venue::Binance;
const QUOTE: &str = "USDT";
/// Binance accepts at most 36 characters in `newClientOrderId`; the prefix is kept short so
/// the deterministic SHA-256 suffix still carries entropy.
const MAX_CLIENT_ID_LEN: usize = 36;
const ORDER_PREFIX: &str = "arb-";
const RECV_WINDOW_MS: u64 = 5_000;
const REQUEST_TIMEOUT_SECS: u64 = 15;
/// A book that is older than this is refused as stale when the venue stamps it.
const BOOK_MAX_AGE_MS: i64 = 10_000;
/// Header names are case-insensitive; lower case avoids any normalisation surprise.
const API_KEY_HEADER: &str = "x-mbx-apikey";

/// USDⓈ-M perpetual contract types this broker is willing to trade. Anything else (dated
/// futures, options, unknown future types) is refused instead of guessed.
const SUPPORTED_CONTRACT_TYPES: [&str; 2] = ["PERPETUAL", "TRADIFI_PERPETUAL"];

/// Binance business codes whose meaning is "the execution status is unknown", so the order
/// must be looked up by client id instead of being treated as refused.
const AMBIGUOUS_CODES: [i64; 4] = [-1000, -1001, -1006, -1007];
/// `-4046 NO_NEED_TO_CHANGE_MARGIN_TYPE`: the symbol is already isolated, which is the goal.
const CODE_NO_NEED_TO_CHANGE_MARGIN_TYPE: i64 = -4046;
/// `-2011 CANCEL_REJECTED` ("Unknown order sent") and `-2013 NO_SUCH_ORDER`: the order is
/// already gone, so cancellation is already effective.
const CODE_CANCEL_REJECTED: i64 = -2011;
const CODE_NO_SUCH_ORDER: i64 = -2013;
/// `-4028 INVALID_LEVERAGE`, which Binance also returns when the requested leverage is the
/// one already in force; the read-back below is what actually confirms the setting.
const CODE_INVALID_LEVERAGE: i64 = -4028;
/// `-1021 INVALID_TIMESTAMP`: the request was refused before it was queued, so nothing was
/// placed — but the clock offset must be refreshed for the next attempt to succeed.
const CODE_INVALID_TIMESTAMP: i64 = -1021;

/// Binance USDⓈ-M API credentials.
///
/// Deliberately no `Debug`/`Serialize`: the secret must never be formatted into a log line,
/// an error message or a journal record.
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub struct BinanceBroker {
    client: Client,
    credentials: BinanceCredentials,
    options: LiveOptions,
    /// Serialises order submission so two legs cannot race the per-symbol settings.
    submit: Mutex<()>,
    /// Millisecond offset applied to local time to match the venue clock.
    time_offset_ms: AtomicI64,
    /// Account taker commission rate, read at construction; used as `fee_per_side`.
    taker_fee: Decimal,
    /// Shared, exclusively locked intent journal (see [`crate::live_common::OrderJournal`]).
    journal: SyncMutex<OrderJournal>,
}

/// Resolved trading rules for one native instrument.
#[derive(Clone, Debug)]
struct Instrument {
    /// Native venue symbol, e.g. `BTCUSDT`.
    symbol: String,
    tick_size: Decimal,
    step_size: Decimal,
    min_qty: Decimal,
    min_notional: Decimal,
}

/// How a signed write request ended. Separating "the venue refused it" from "we do not know"
/// is what keeps `place` from inventing a terminal state.
enum SubmitOutcome {
    Accepted(Value),
    Refused { code: i64, message: String },
    Unknown(String),
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<RawSymbol>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSymbol {
    symbol: String,
    base_asset: String,
    quote_asset: String,
    margin_asset: String,
    contract_type: String,
    status: String,
    #[serde(default)]
    filters: Vec<RawFilter>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawFilter {
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
struct RawOrder {
    order_id: i64,
    symbol: String,
    client_order_id: String,
    status: String,
    side: String,
    position_side: String,
    #[serde(default)]
    reduce_only: bool,
    price: String,
    orig_qty: String,
    executed_qty: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTrade {
    id: i64,
    order_id: i64,
    symbol: String,
    price: String,
    qty: String,
    commission: String,
    commission_asset: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawAlgo {
    client_algo_id: String,
    symbol: String,
    side: String,
    position_side: String,
    quantity: String,
    price: String,
    reduce_only: bool,
    close_position: bool,
    algo_status: String,
    actual_order_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPosition {
    symbol: String,
    position_amt: String,
    entry_price: String,
    mark_price: String,
    margin_type: String,
    #[serde(default)]
    liquidation_price: Option<String>,
    #[serde(default)]
    isolated_wallet: Option<String>,
    position_side: String,
    #[serde(default)]
    leverage: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBracket {
    symbol: String,
    #[serde(default)]
    brackets: Vec<RawBracketLevel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBracketLevel {
    initial_leverage: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DualSidePosition {
    dual_side_position: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MultiAssetsMargin {
    multi_assets_margin: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommissionRate {
    taker_commission_rate: String,
}

#[derive(Deserialize)]
struct Depth {
    #[serde(default)]
    bids: Vec<Vec<String>>,
    #[serde(default)]
    asks: Vec<Vec<String>>,
    #[serde(rename = "E", default)]
    event_time: Option<i64>,
    #[serde(rename = "T", default)]
    transaction_time: Option<i64>,
}

#[derive(Deserialize)]
struct ServerTime {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

impl BinanceBroker {
    /// Validates the run options, locks the intent journal, syncs the venue clock and performs
    /// the read-only authenticated checks. No write request is sent, not even to the account's
    /// own settings.
    pub async fn connect(
        client: Client,
        credentials: BinanceCredentials,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(VENUE)?;
        if credentials.api_key.trim().is_empty() || credentials.api_secret.is_empty() {
            return Err(error("API key and secret must be non-empty"));
        }
        let identity = format!("binance:{}", short_hash(credentials.api_key.as_bytes()));
        let journal = OrderJournal::open(journal_path, &identity)?;
        let offset = server_time_offset_ms(&client, &credentials.api_key).await?;
        let broker = Self {
            client,
            credentials,
            options,
            submit: Mutex::new(()),
            time_offset_ms: AtomicI64::new(offset),
            taker_fee: Decimal::ZERO,
            journal: SyncMutex::new(journal),
        };
        broker.check_account_settings().await?;
        let taker_fee = broker.account_taker_fee().await?;
        Ok(Self {
            taker_fee,
            ..broker
        })
    }

    /// Fails, with an actionable message, when the account-wide settings are not the ones this
    /// broker can trade under. It never changes them: a live account's mode is the operator's.
    async fn check_account_settings(&self) -> ArbResult<()> {
        let dual: DualSidePosition = decode(
            self.signed_get("/fapi/v1/positionSide/dual", &[], &[])
                .await?,
        )?;
        if dual.dual_side_position {
            return Err(error(
                "account is in hedge (dual-side) position mode; switch to one-way (net) mode in \
                 the Binance UI/API before starting the bot — this broker never changes \
                 account-wide settings",
            ));
        }
        let assets: MultiAssetsMargin = decode(
            self.signed_get("/fapi/v1/multiAssetsMargin", &[], &[])
                .await?,
        )?;
        if assets.multi_assets_margin {
            return Err(error(
                "account is in multi-assets margin mode; switch to single-asset (USDT) mode \
                 before starting the bot — this broker never changes account-wide settings",
            ));
        }
        Ok(())
    }

    /// Reads the actual BTCUSDT taker rate as the account reference. Realized fees always
    /// come from fills; Binance exposes this endpoint per symbol, including promotions.
    async fn account_taker_fee(&self) -> ArbResult<Decimal> {
        let symbols = self.exchange_info().await?.symbols;
        let instrument = select_instrument(&symbols, &Symbol::perp("BTC", QUOTE))?;
        let rate: CommissionRate = decode(
            self.signed_get(
                "/fapi/v1/commissionRate",
                &[("symbol", instrument.symbol)],
                &[],
            )
            .await?,
        )?;
        let fee = decimal(&rate.taker_commission_rate)?;
        if fee < Decimal::ZERO || fee >= Decimal::ONE {
            return Err(error("account taker commission rate is out of range"));
        }
        Ok(fee)
    }

    fn timestamp_ms(&self) -> ArbResult<i64> {
        Ok(now_ms()?.saturating_add(self.time_offset_ms.load(Ordering::Relaxed)))
    }

    /// Re-reads the venue clock; called when a request is refused for a stale timestamp.
    async fn resync_clock(&self) -> ArbResult<()> {
        let offset = server_time_offset_ms(&self.client, &self.credentials.api_key).await?;
        self.time_offset_ms.store(offset, Ordering::Relaxed);
        Ok(())
    }

    fn signed_request(
        &self,
        method: Method,
        path: &str,
        params: &[(&str, String)],
    ) -> ArbResult<RequestBuilder> {
        if method != Method::GET {
            self.options.authorize(VENUE)?;
        }
        let query = signed_query(
            self.credentials.api_secret.as_bytes(),
            params,
            self.timestamp_ms()?,
            RECV_WINDOW_MS,
        )?;
        Ok(self
            .client
            .request(method, format!("{BASE_URL}{path}?{query}"))
            .header(API_KEY_HEADER, api_key_header(&self.credentials.api_key)?)
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
    }

    async fn signed_get(
        &self,
        path: &str,
        params: &[(&str, String)],
        tolerate: &[i64],
    ) -> ArbResult<Value> {
        let request = self.signed_request(Method::GET, path, params)?;
        request_json(request, path, tolerate).await
    }

    async fn signed_post(
        &self,
        path: &str,
        params: &[(&str, String)],
        tolerate: &[i64],
    ) -> ArbResult<Value> {
        let request = self.signed_request(Method::POST, path, params)?;
        request_json(request, path, tolerate).await
    }

    async fn signed_delete(
        &self,
        path: &str,
        params: &[(&str, String)],
        tolerate: &[i64],
    ) -> ArbResult<Value> {
        let request = self.signed_request(Method::DELETE, path, params)?;
        request_json(request, path, tolerate).await
    }

    /// Unsigned market data. The key header is sent anyway: the venue classifies market data as
    /// a key-bearing endpoint, and an extra read-only header changes nothing else.
    async fn public_get(&self, path: &str, params: &[(&str, String)]) -> ArbResult<Value> {
        let request = self
            .client
            .get(format!("{BASE_URL}{path}"))
            .query(params)
            .header(API_KEY_HEADER, api_key_header(&self.credentials.api_key)?)
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));
        request_json(request, path, &[]).await
    }

    async fn exchange_info(&self) -> ArbResult<ExchangeInfo> {
        let info: ExchangeInfo = decode(self.public_get("/fapi/v1/exchangeInfo", &[]).await?)?;
        if info.symbols.is_empty() {
            return Err(error("exchange reported no instruments"));
        }
        Ok(info)
    }

    /// Native symbol -> scanner symbol for every tradable USDT perpetual. Used by
    /// reconciliation so unknown venue orders are still attributed to a real market.
    async fn symbol_index(&self) -> ArbResult<HashMap<String, Symbol>> {
        reverse_index(&self.exchange_info().await?.symbols)
    }

    /// Maximum initial leverage allowed for this symbol's first notional bracket.
    async fn max_leverage(&self, instrument: &str) -> ArbResult<u32> {
        let response = self
            .signed_get(
                "/fapi/v1/leverageBracket",
                &[("symbol", instrument.to_string())],
                &[],
            )
            .await?;
        // The symbol-filtered endpoint returns one object, not the unfiltered array.
        let entry: RawBracket = decode(response)?;
        if entry.symbol != instrument {
            return Err(error("leverage bracket response is for another symbol"));
        }
        let level = entry
            .brackets
            .iter()
            .map(|level| level.initial_leverage)
            .max()
            .filter(|level| *level > 0)
            .ok_or_else(|| error("leverage bracket response has no usable bracket"))?;
        Ok(level)
    }

    /// Limit price for the order: the explicit limit, or a slippage-bounded price taken from a
    /// book fetched immediately before signing.
    async fn price(&self, order: &NewOrder, instrument: &Instrument) -> ArbResult<Decimal> {
        let raw = match order.limit_price {
            Some(limit) => limit,
            None => {
                let depth: Depth = decode(
                    self.public_get(
                        "/fapi/v1/depth",
                        &[
                            ("symbol", instrument.symbol.clone()),
                            ("limit", "5".to_string()),
                        ],
                    )
                    .await?,
                )?;
                let stamp = depth
                    .transaction_time
                    .or(depth.event_time)
                    .ok_or_else(|| error("order book has no timestamp"))?;
                let age = self
                    .timestamp_ms()?
                    .checked_sub(stamp)
                    .ok_or_else(|| error("invalid book timestamp"))?;
                if !(-1_000..=BOOK_MAX_AGE_MS).contains(&age) {
                    return Err(error("order book is stale; refusing to price the order"));
                }
                let best_bid = top_of_book(&depth.bids)?;
                let best_ask = top_of_book(&depth.asks)?;
                if best_bid.is_none() || best_ask.is_none() || best_bid >= best_ask {
                    return Err(error("empty, locked or crossed order book"));
                }
                self.options
                    .bound_price(VENUE, order.side, best_bid, best_ask)?
            }
        };
        round_price(raw, instrument.tick_size, order.side)
            .ok_or_else(|| error("price rounds to a non-positive tick"))
    }

    /// Sets isolated margin and the requested leverage for one symbol, then reads both back.
    /// Only per-symbol settings are touched; account-wide settings are never changed.
    async fn set_isolated(
        &self,
        instrument: &str,
        leverage: u32,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.signed_post(
            "/fapi/v1/marginType",
            &[
                ("symbol", instrument.to_string()),
                (
                    "marginType",
                    if mode.is_cross() {
                        "CROSSED"
                    } else {
                        "ISOLATED"
                    }
                    .to_string(),
                ),
            ],
            &[CODE_NO_NEED_TO_CHANGE_MARGIN_TYPE],
        )
        .await?;
        let applied: Value = self
            .signed_post(
                "/fapi/v1/leverage",
                &[
                    ("symbol", instrument.to_string()),
                    ("leverage", leverage.to_string()),
                ],
                &[CODE_INVALID_LEVERAGE],
            )
            .await?;
        if let Some(value) = applied.get("leverage").and_then(Value::as_u64)
            && value != u64::from(leverage)
        {
            return Err(error("venue applied a different leverage than requested"));
        }
        let rows: Vec<RawPosition> = decode(
            self.signed_get(
                "/fapi/v2/positionRisk",
                &[("symbol", instrument.to_string())],
                &[],
            )
            .await?,
        )?;
        if rows.len() != 1 {
            return Err(error("position risk read-back is missing or ambiguous"));
        }
        let row = &rows[0];
        if row.symbol != instrument || row.position_side != "BOTH" {
            return Err(error(
                "position risk read-back symbol or position mode mismatch",
            ));
        }
        if !row.margin_type.eq_ignore_ascii_case(mode.as_str()) {
            return Err(error(
                "requested margin mode was not confirmed by the venue; refusing to open",
            ));
        }
        if decimal(&row.leverage)? != Decimal::from(leverage) {
            return Err(error(
                "leverage read-back does not match the requested value; refusing to open",
            ));
        }
        Ok(())
    }

    async fn submit_order(
        &self,
        order: &NewOrder,
        instrument: &str,
        price: Decimal,
        units: Decimal,
        venue_id: &str,
    ) -> SubmitOutcome {
        let mut params: Vec<(&str, String)> = vec![
            ("symbol", instrument.to_string()),
            ("side", side_str(order.side).to_string()),
            ("type", "LIMIT".to_string()),
            ("timeInForce", "IOC".to_string()),
            ("quantity", units.normalize().to_string()),
            ("price", price.normalize().to_string()),
            ("newClientOrderId", venue_id.to_string()),
            ("newOrderRespType", "RESULT".to_string()),
        ];
        if order.reduce_only {
            params.push(("reduceOnly", "true".to_string()));
        }
        let request = match self.signed_request(Method::POST, "/fapi/v1/order", &params) {
            Ok(request) => request,
            Err(failure) => return SubmitOutcome::Unknown(failure.to_string()),
        };
        let response = match request.send().await {
            Ok(response) => response,
            Err(failure) => {
                return SubmitOutcome::Unknown(
                    transport_error(VENUE, "/fapi/v1/order", failure).to_string(),
                );
            }
        };
        let status = response.status();
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(failure) => {
                return SubmitOutcome::Unknown(
                    transport_error(VENUE, "/fapi/v1/order", failure).to_string(),
                );
            }
        };
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return SubmitOutcome::Unknown(format!(
                    "unparseable order response (HTTP {status})"
                ));
            }
        };
        match (status.is_success(), business_code(&value)) {
            (true, None) | (true, Some(200)) => SubmitOutcome::Accepted(value),
            (_, Some(code)) if AMBIGUOUS_CODES.contains(&code) => {
                SubmitOutcome::Unknown(vendor_message(&value, status))
            }
            (_, Some(_)) if status.is_server_error() => {
                SubmitOutcome::Unknown(vendor_message(&value, status))
            }
            (_, Some(code)) if definitive_rejection(code) => SubmitOutcome::Refused {
                code,
                message: vendor_message(&value, status),
            },
            _ => SubmitOutcome::Unknown(vendor_message(&value, status)),
        }
    }

    /// Looks an order up by the client id this broker generated. `Ok(None)` means the venue
    /// explicitly reported that it does not know the id.
    async fn lookup_order(&self, instrument: &str, venue_id: &str) -> ArbResult<Option<RawOrder>> {
        self.lookup_by(instrument, "origClientOrderId", venue_id)
            .await
    }

    async fn lookup_order_by_id(
        &self,
        instrument: &str,
        order_id: i64,
    ) -> ArbResult<Option<RawOrder>> {
        self.lookup_by(instrument, "orderId", &order_id.to_string())
            .await
    }

    async fn lookup_by(
        &self,
        instrument: &str,
        key: &str,
        value: &str,
    ) -> ArbResult<Option<RawOrder>> {
        let response = self
            .signed_get(
                "/fapi/v1/order",
                &[("symbol", instrument.to_string()), (key, value.to_string())],
                &[CODE_NO_SUCH_ORDER],
            )
            .await?;
        if business_code(&response) == Some(CODE_NO_SUCH_ORDER) {
            return Ok(None);
        }
        let raw: RawOrder = decode(response)?;
        if raw.symbol != instrument
            || (key == "origClientOrderId" && raw.client_order_id != value)
            || (key == "orderId" && raw.order_id.to_string() != value)
        {
            return Err(error("order lookup returned a different identity"));
        }
        Ok(Some(raw))
    }

    /// Fees and notional of one order, taken from the venue's own fill list. A fill list that
    /// does not match the order's executed quantity, or a fee charged in anything but USDT,
    /// is an error — never a silent approximation.
    async fn fills(
        &self,
        instrument: &str,
        order_id: i64,
        executed: Decimal,
    ) -> ArbResult<(Decimal, Option<Decimal>, Decimal)> {
        if executed < Decimal::ZERO {
            return Err(error("negative executed quantity"));
        }
        if executed.is_zero() {
            return Ok((Decimal::ZERO, None, Decimal::ZERO));
        }
        let mut trades = Vec::new();
        let mut from_id = 0_i64;
        loop {
            let page: Vec<RawTrade> = decode(
                self.signed_get(
                    "/fapi/v1/userTrades",
                    &[
                        ("symbol", instrument.to_string()),
                        ("orderId", order_id.to_string()),
                        ("fromId", from_id.to_string()),
                        ("limit", "1000".to_string()),
                    ],
                    &[],
                )
                .await?,
            )?;
            let count = page.len();
            for trade in page {
                if trade.id < from_id {
                    return Err(error("fill pagination repeated or reversed a trade ID"));
                }
                from_id = trade
                    .id
                    .checked_add(1)
                    .ok_or_else(|| error("fill ID overflow"))?;
                trades.push(trade);
            }
            if count < 1000 {
                break;
            }
        }
        aggregate_trades(instrument, order_id, executed, &trades)
    }

    async fn lookup_algo(&self, client_id: &str) -> ArbResult<RawAlgo> {
        let raw: RawAlgo = decode(
            self.signed_get(
                "/fapi/v1/algoOrder",
                &[("clientAlgoId", client_id.to_string())],
                &[],
            )
            .await?,
        )?;
        if raw.client_algo_id != client_id {
            return Err(error("conditional order identity mismatch"));
        }
        Ok(raw)
    }

    async fn algo_state(
        &self,
        raw: RawAlgo,
        index: &HashMap<String, Symbol>,
    ) -> ArbResult<OrderState> {
        if !matches!(
            raw.algo_status.as_str(),
            "NEW" | "TRIGGERING" | "TRIGGERED" | "FINISHED" | "CANCELED" | "EXPIRED" | "REJECTED"
        ) {
            return Err(error("unknown conditional order status"));
        }
        let symbol = index
            .get(&raw.symbol)
            .ok_or_else(|| error("conditional order has an unmapped instrument"))?;
        if raw.position_side != "BOTH" {
            return Err(error("hedge-mode conditional order cannot be reconciled"));
        }
        let id = ClientOrderId(format!("binance-external-algo:{}", raw.client_algo_id));
        if !raw.actual_order_id.is_empty() {
            let order_id = raw
                .actual_order_id
                .parse::<i64>()
                .map_err(|_| error("invalid conditional child order ID"))?;
            let child = self
                .lookup_order_by_id(&raw.symbol, order_id)
                .await?
                .ok_or_else(|| error("triggered conditional child order is not queryable"))?;
            let order = reconstructed_order(&id, symbol, &child)?;
            return self
                .state_from_order(&order, &child, decimal(&child.orig_qty)?)
                .await;
        }
        let status = match raw.algo_status.as_str() {
            "NEW" => OrderStatus::Open,
            "TRIGGERING" => OrderStatus::Pending,
            "CANCELED" | "EXPIRED" => OrderStatus::Cancelled,
            "REJECTED" => OrderStatus::Rejected,
            _ => {
                return Err(error(
                    "unknown or triggered conditional status without a child",
                ));
            }
        };
        let quantity = decimal(&raw.quantity)?;
        let price = decimal(&raw.price)?;
        let mut state = OrderState::new(NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: id,
            venue: VENUE,
            symbol: symbol.clone(),
            side: parse_side(&raw.side)?,
            notional_usdt: quantity
                .checked_mul(price)
                .ok_or_else(|| error("notional overflow"))?,
            quantity: (!raw.close_position).then_some(quantity),
            limit_price: (price > Decimal::ZERO).then_some(price),
            reduce_only: raw.reduce_only || raw.close_position,
            leverage: None,
        });
        state.venue_order_id = Some(format!("algo:{}", raw.client_algo_id));
        state.status = status;
        if status == OrderStatus::Rejected {
            state.reject_reason = Some("conditional order rejected by venue".to_string());
        }
        Ok(state)
    }

    async fn cancel_algo(&self, client_id: &str) -> ArbResult<()> {
        let index = self.symbol_index().await?;
        let before = self
            .algo_state(self.lookup_algo(client_id).await?, &index)
            .await?;
        if !before.status.is_live() {
            return Ok(());
        }
        if let Some(id) = before.venue_order_id.as_deref()
            && !id.starts_with("algo:")
        {
            return self.cancel_regular(id).await;
        }
        let result = self
            .signed_delete(
                "/fapi/v1/algoOrder",
                &[("clientAlgoId", client_id.to_string())],
                &[],
            )
            .await;
        let after = self
            .algo_state(self.lookup_algo(client_id).await?, &index)
            .await?;
        if after.status.is_live() {
            if let Some(id) = after.venue_order_id.as_deref()
                && !id.starts_with("algo:")
            {
                return self.cancel_regular(id).await;
            }
            result?;
            return Err(error(
                "conditional order or its child remains live after cancellation",
            ));
        }
        Ok(())
    }

    async fn cancel_regular(&self, venue_order_id: &str) -> ArbResult<()> {
        let (symbol, order_id) = parse_venue_order_id(venue_order_id)?;
        if let Some(raw) = self.lookup_order_by_id(&symbol, order_id).await?
            && !map_status(&raw.status)?.is_live()
        {
            return Ok(());
        }
        let result = self
            .signed_delete(
                "/fapi/v1/order",
                &[
                    ("symbol", symbol.clone()),
                    ("orderId", order_id.to_string()),
                ],
                &[CODE_CANCEL_REJECTED, CODE_NO_SUCH_ORDER],
            )
            .await;
        match self.lookup_order_by_id(&symbol, order_id).await? {
            Some(raw) if !map_status(&raw.status)?.is_live() => Ok(()),
            Some(_) => Err(error("order is still live after cancellation")),
            None => {
                result?;
                Err(error(
                    "cancelled order is not queryable; terminality cannot be confirmed",
                ))
            }
        }
    }

    async fn state_from_order(
        &self,
        order: &NewOrder,
        raw: &RawOrder,
        requested: Decimal,
    ) -> ArbResult<OrderState> {
        if raw.symbol != format!("{}{}", order.symbol.base, order.symbol.quote)
            || parse_side(&raw.side)? != order.side
            || raw.reduce_only != order.reduce_only
            || raw.position_side != "BOTH"
            || decimal(&raw.orig_qty)? != requested
        {
            return Err(error("venue order does not match the journaled intent"));
        }
        let executed = decimal(&raw.executed_qty)?;
        if requested <= Decimal::ZERO || executed < Decimal::ZERO || executed > requested {
            return Err(error("invalid venue order quantities"));
        }
        let status = verified_status(map_status(&raw.status)?, requested, executed);
        let (filled_usdt, average_price, fee_usdt) =
            self.fills(&raw.symbol, raw.order_id, executed).await?;
        let mut state = OrderState::new(order.clone());
        state.venue_order_id = Some(format_venue_order_id(&raw.symbol, raw.order_id));
        state.status = status;
        state.filled_usdt = filled_usdt;
        state.average_price = average_price;
        state.fee_usdt = fee_usdt;
        if status == OrderStatus::Rejected {
            state.reject_reason = Some(format!("venue reported {}", raw.status));
        }
        Ok(state)
    }

    /// Persists a terminal state so later lookups never re-ask the venue (its query window for
    /// old orders does expire).
    fn remember_terminal(&self, state: &OrderState) -> ArbResult<()> {
        if state.status.is_live() {
            return Ok(());
        }
        let mut journal = self
            .journal
            .lock()
            .map_err(|_| error("intent journal lock is poisoned"))?;
        journal.record_terminal(state)
    }

    fn journal_entry(&self, id: &ClientOrderId) -> ArbResult<Option<JournalEntry>> {
        let journal = self
            .journal
            .lock()
            .map_err(|_| error("intent journal lock is poisoned"))?;
        Ok(journal.get(id).cloned())
    }

    fn journal_entry_for_venue_id(&self, venue_id: &str) -> ArbResult<Option<JournalEntry>> {
        let journal = self
            .journal
            .lock()
            .map_err(|_| error("intent journal lock is poisoned"))?;
        Ok(journal.by_venue_client_id(venue_id).cloned())
    }

    async fn risk_positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let rows: Vec<RawPosition> =
            decode(self.signed_get("/fapi/v2/positionRisk", &[], &[]).await?)?;
        let index = self.symbol_index().await?;
        let mut positions = Vec::new();
        for row in rows {
            // One-way mode reports `BOTH`; anything else means hedge mode, which this broker
            // cannot reconcile into a single signed quantity.
            if row.position_side != "BOTH" {
                return Err(error(
                    "hedge-mode (two-sided) positions are not supported; switch the account to \
                     one-way position mode",
                ));
            }
            let amount = decimal(&row.position_amt)?;
            if amount.is_zero() {
                continue;
            }
            let symbol = index
                .get(&row.symbol.to_ascii_uppercase())
                .ok_or_else(|| {
                    error(
                        "position is held in a market this broker does not map; reconciliation \
                           is incomplete",
                    )
                })?
                .clone();
            let entry_price = decimal(&row.entry_price)?;
            let mark_price = decimal(&row.mark_price)?;
            if entry_price <= Decimal::ZERO || mark_price <= Decimal::ZERO {
                return Err(error("position valuation is missing or non-positive"));
            }
            let notional = amount
                .abs()
                .checked_mul(mark_price)
                .ok_or_else(|| error("position notional overflow"))?;
            positions.push(VenuePosition {
                venue: VENUE,
                symbol,
                net_quantity: amount,
                average_price: Some(entry_price),
                notional_usdt: notional,
            });
        }
        Ok(positions)
    }
}

#[async_trait]
impl Broker for BinanceBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let info = self.exchange_info().await?;
        let native = select_instrument(&info.symbols, symbol)?.symbol.clone();
        let rows: Vec<RawPosition> = decode(
            self.signed_get("/fapi/v2/positionRisk", &[("symbol", native.clone())], &[])
                .await?,
        )?;
        let Some(row) = rows.iter().find(|row| {
            row.symbol == native
                && row.position_side == "BOTH"
                && decimal(&row.position_amt).is_ok_and(|q| q != Decimal::ZERO)
        }) else {
            return Ok(None);
        };
        let parse = |value: &Option<String>| value.as_deref().and_then(|raw| decimal(raw).ok());
        Ok(Some(crate::margin::venue_state(
            crate::margin::reported_mode(&row.margin_type),
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
        self.options.authorize(Venue::Binance)?;
        let _guard = self.submit.lock().await;
        self.check_account_settings().await?;
        let info = self.exchange_info().await?;
        let instrument = select_instrument(&info.symbols, symbol)?;
        let leverage = crate::margin::leverage(Venue::Binance, leverage)?
            .to_u32()
            .ok_or_else(|| error("invalid leverage"))?;
        if leverage > self.max_leverage(&instrument.symbol).await? {
            return Err(error("leverage exceeds market limit"));
        }
        self.set_isolated(&instrument.symbol, leverage, mode).await
    }

    fn venue(&self) -> Venue {
        VENUE
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.options.authorize(VENUE)?;
        let _submission = self.submit.lock().await;
        if order.venue != VENUE || order.client_order_id.0.is_empty() {
            return Err(error("wrong venue or empty client order ID"));
        }
        let venue_id = venue_client_id(ORDER_PREFIX, &order.client_order_id, MAX_CLIENT_ID_LEN);
        if !valid_client_id(&venue_id) {
            return Err(error(
                "generated client order ID is not valid for this venue",
            ));
        }
        // Invariant 1: a journaled client id is never resubmitted.
        if let Some(entry) = self.journal_entry(&order.client_order_id)? {
            if !same_intent(&entry.order, order) {
                return Err(error(
                    "client order ID was already used with a different intent; refusing to \
                     resubmit",
                ));
            }
            if let Some(terminal) = entry.terminal {
                return ack(&terminal);
            }
            return match self.lookup_order(&entry.instrument, &venue_id).await? {
                Some(raw) => {
                    let state = self.state_from_order(order, &raw, entry.units).await?;
                    self.remember_terminal(&state)?;
                    ack(&state)
                }
                None => Err(error(
                    "a reserved order ID is unknown to the venue; do not resubmit, reconcile \
                     manually",
                )),
            };
        }
        // Fresh instrument metadata for every order: filters and status change without notice.
        let symbols = self.exchange_info().await?;
        let instrument = select_instrument(&symbols.symbols, &order.symbol)?;
        let max_leverage = self.max_leverage(&instrument.symbol).await?;
        self.check_account_settings().await?;
        let leverage = if order.reduce_only {
            None
        } else {
            let requested = order
                .leverage
                .ok_or_else(|| error("opening order requires explicit isolated leverage"))?;
            if !requested.fract().is_zero()
                || requested < Decimal::ONE
                || requested > Decimal::from(max_leverage)
            {
                return Err(error(format!(
                    "leverage must be an integer between 1 and {max_leverage}"
                )));
            }
            Some(
                requested
                    .to_u32()
                    .ok_or_else(|| error("leverage is not a valid integer"))?,
            )
        };
        let position = if order.reduce_only {
            Some(
                self.risk_positions()
                    .await?
                    .into_iter()
                    .find(|position| position.symbol == order.symbol)
                    .ok_or_else(|| error("reduce-only order has no matching position"))?,
            )
        } else {
            None
        };
        // Per-symbol margin mode and leverage land before the order; both are read back.
        if let Some(leverage) = leverage {
            self.set_isolated(&instrument.symbol, leverage, order.margin_mode)
                .await?;
        }
        // Fetch market-style pricing AFTER every potentially slow settings/read-back request.
        let price = self.price(order, &instrument).await?;
        let units = order_units(
            VENUE,
            order,
            price,
            Decimal::ONE,
            instrument.step_size,
            instrument.min_qty,
        )?;
        // Binance explicitly exempts reduce-only exits from MIN_NOTIONAL (-4164).
        if !order.reduce_only {
            meets_min_notional(units, price, instrument.min_notional)?;
        }
        if let Some(position) = position {
            let closes = (position.net_quantity > Decimal::ZERO && order.side == Side::Sell)
                || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
            if !closes || units > position.net_quantity.abs() {
                return Err(error(
                    "reduce-only quantity/direction exceeds the current position",
                ));
            }
        }
        // Reserve BEFORE the order request. A timeout or a crash never permits a replay.
        {
            let mut journal = self
                .journal
                .lock()
                .map_err(|_| error("intent journal lock is poisoned"))?;
            journal.reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: venue_id.clone(),
                instrument: instrument.symbol.clone(),
                units,
                terminal: None,
            })?;
        }
        match self
            .submit_order(order, &instrument.symbol, price, units, &venue_id)
            .await
        {
            SubmitOutcome::Accepted(response) => {
                // The venue's own status + fills decide, not the acknowledgement.
                match self.lookup_order(&instrument.symbol, &venue_id).await? {
                    Some(raw) => {
                        let state = self.state_from_order(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        ack(&state)
                    }
                    None => {
                        let raw: RawOrder = decode(response)?;
                        let state = self.state_from_order(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        ack(&state)
                    }
                }
            }
            SubmitOutcome::Refused { code, message } => {
                if code == CODE_INVALID_TIMESTAMP {
                    self.resync_clock().await?;
                }
                // Even a business error (including duplicate ID) must first be reconciled.
                if let Some(raw) = self.lookup_order(&instrument.symbol, &venue_id).await? {
                    let state = self.state_from_order(order, &raw, units).await?;
                    self.remember_terminal(&state)?;
                    return ack(&state);
                }
                // The venue explicitly refused: record it so `order_state` stays truthful.
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(message.clone());
                self.remember_terminal(&rejected)?;
                Err(error(message))
            }
            SubmitOutcome::Unknown(reason) => {
                match self.lookup_order(&instrument.symbol, &venue_id).await? {
                    Some(raw) => {
                        let state = self.state_from_order(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        ack(&state)
                    }
                    None => Err(error(format!(
                        "order outcome is unknown ({reason}) and the venue has no record of the \
                         reserved ID; reconcile manually, never resubmit"
                    ))),
                }
            }
        }
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        if let Some(raw_id) = client_order_id.0.strip_prefix("binance-external-") {
            let index = self.symbol_index().await?;
            if let Some(client_id) = raw_id.strip_prefix("algo:") {
                return self
                    .algo_state(self.lookup_algo(client_id).await?, &index)
                    .await
                    .map(Some);
            }
            let (instrument, order_id) = parse_venue_order_id(raw_id)?;
            let raw = self
                .lookup_order_by_id(&instrument, order_id)
                .await?
                .ok_or_else(|| error("external order is no longer queryable"))?;
            let symbol = index
                .get(&instrument)
                .ok_or_else(|| error("unmapped external order"))?;
            let order = reconstructed_order(client_order_id, symbol, &raw)?;
            return self
                .state_from_order(&order, &raw, decimal(&raw.orig_qty)?)
                .await
                .map(Some);
        }
        let Some(entry) = self.journal_entry(client_order_id)? else {
            // Never submitted through this journal: the only state in which `None` is truthful.
            return Ok(None);
        };
        if let Some(terminal) = entry.terminal {
            return Ok(Some(terminal));
        }
        match self
            .lookup_order(&entry.instrument, &entry.venue_client_id)
            .await?
        {
            Some(raw) => {
                let state = self
                    .state_from_order(&entry.order, &raw, entry.units)
                    .await?;
                self.remember_terminal(&state)?;
                Ok(Some(state))
            }
            None => Err(error(
                "a reserved order ID is unknown to the venue; cannot assume it never filled",
            )),
        }
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.options.authorize(VENUE)?;
        let _submission = self.submit.lock().await;
        if let Some(client_id) = venue_order_id.strip_prefix("algo:") {
            return self.cancel_algo(client_id).await;
        }
        self.cancel_regular(venue_order_id).await
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let rows: Vec<RawOrder> = decode(self.signed_get("/fapi/v1/openOrders", &[], &[]).await?)?;
        let algos: Vec<RawAlgo> =
            decode(self.signed_get("/fapi/v1/openAlgoOrders", &[], &[]).await?)?;
        let index = self.symbol_index().await?;
        let mut states = Vec::with_capacity(rows.len());
        for raw in rows {
            let symbol = index
                .get(&raw.symbol.to_ascii_uppercase())
                .ok_or_else(|| {
                    error(
                        "an open order is in a market this broker does not map; reconciliation \
                           is incomplete",
                    )
                })?
                .clone();
            let order = match self.journal_entry_for_venue_id(&raw.client_order_id)? {
                Some(entry) => {
                    if entry.instrument != raw.symbol {
                        return Err(error("journaled order maps to a different venue symbol"));
                    }
                    entry.order
                }
                None => reconstructed_order(
                    &ClientOrderId(format!("binance-external-{}:{}", raw.symbol, raw.order_id)),
                    &symbol,
                    &raw,
                )?,
            };
            let requested = decimal(&raw.orig_qty)?;
            states.push(self.state_from_order(&order, &raw, requested).await?);
        }
        for raw in algos {
            let state = self.algo_state(raw, &index).await?;
            if state.status.is_live() {
                states.push(state);
            }
        }
        Ok(states)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        self.risk_positions().await
    }
}

fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue("binance", message)
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> ArbResult<T> {
    serde_json::from_value(value).map_err(|_| error("invalid Binance response schema"))
}

fn decimal(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| error("invalid decimal in venue response"))
}

fn now_ms() -> ArbResult<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .map_err(|_| error("system clock is before the Unix epoch"))
}

/// First 16 hex characters of the SHA-256 of the API key: identifies the account in the journal
/// without ever writing the key itself to disk.
fn short_hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_lower(&digest[..8])
}

async fn server_time_offset_ms(client: &Client, api_key: &str) -> ArbResult<i64> {
    let request = client
        .get(format!("{BASE_URL}/fapi/v1/time"))
        .header(API_KEY_HEADER, api_key_header(api_key)?)
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS));
    let response: ServerTime = decode(request_json(request, "/fapi/v1/time", &[]).await?)?;
    Ok(response.server_time.saturating_sub(now_ms()?))
}

fn api_key_header(key: &str) -> ArbResult<reqwest::header::HeaderValue> {
    let mut value = reqwest::header::HeaderValue::from_str(key)
        .map_err(|_| error("API key is not a valid HTTP header value"))?;
    value.set_sensitive(true);
    Ok(value)
}

/// Percent-encodes one query parameter. Our values are all unreserved characters, but the
/// signature covers the exact transmitted string, so encoding must be explicit.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Builds `param=value&…&recvWindow=…&timestamp=…&signature=…` with the HMAC-SHA256 signature
/// taken over everything before `&signature=` (the order the venue documents).
fn signed_query(
    secret: &[u8],
    params: &[(&str, String)],
    timestamp_ms: i64,
    recv_window_ms: u64,
) -> ArbResult<String> {
    if secret.is_empty() {
        return Err(error("API secret must be non-empty"));
    }
    let mut query = String::with_capacity(256);
    for (key, value) in params {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(&percent_encode(key));
        query.push('=');
        query.push_str(&percent_encode(value));
    }
    if !query.is_empty() {
        query.push('&');
    }
    use std::fmt::Write;
    write!(
        &mut query,
        "recvWindow={recv_window_ms}&timestamp={timestamp_ms}"
    )
    .map_err(|_| error("query encoding failed"))?;
    let signature = hex_lower(&hmac_sha256(secret, query.as_bytes()));
    query.push_str("&signature=");
    query.push_str(&signature);
    Ok(query)
}

/// Sends a request and interprets the venue's dual status model (HTTP status *and* business
/// code). Codes in `tolerate` come back as `Ok` so the caller can branch on them; error
/// strings carry only the code and the venue's own message, never a body that could echo the
/// request.
async fn request_json(
    request: RequestBuilder,
    endpoint: &str,
    tolerate: &[i64],
) -> ArbResult<Value> {
    let response = request
        .send()
        .await
        .map_err(|failure| transport_error(VENUE, endpoint, failure))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|failure| transport_error(VENUE, endpoint, failure))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| error(format!("invalid JSON response (HTTP {status})")))?;
    let code = business_code(&value);
    if value.get("code").is_some() && code.is_none() {
        return Err(error("invalid venue business code"));
    }
    if !status.is_success() || code.is_some_and(|code| code != 0 && code != 200) {
        if let Some(code) = code
            && tolerate.contains(&code)
        {
            return Ok(value);
        }
        return Err(error(vendor_message(&value, status)));
    }
    Ok(value)
}

fn vendor_message(value: &Value, status: StatusCode) -> String {
    match business_code(value) {
        Some(code) => format!("venue code {code} (HTTP {status})"),
        None => format!("HTTP {status}"),
    }
}

fn business_code(value: &Value) -> Option<i64> {
    value.get("code").and_then(|code| {
        code.as_i64()
            .or_else(|| code.as_str().and_then(|raw| raw.parse().ok()))
    })
}

/// Only documented pre-matching rejections are definitive. Unknown codes, duplicate client
/// IDs and server errors retain an unresolved intent instead of inventing a rejection.
fn definitive_rejection(code: i64) -> bool {
    matches!(code, -1021 | -1022)
        || (-1136..=-1100).contains(&code)
        || matches!(
            code,
            -2014
                | -2015
                | -2018
                | -2019
                | -2022
                | -2024
                | -2027
                | -2028
                | -4001
                | -4002
                | -4003
                | -4004
                | -4005
                | -4013
                | -4014
                | -4015
                | -4016
                | -4023
                | -4024
                | -4061
                | -4062
                | -4164
        )
}

fn top_of_book(levels: &[Vec<String>]) -> ArbResult<Option<Decimal>> {
    let Some(level) = levels.first() else {
        return Ok(None);
    };
    if level.len() < 2 || decimal(&level[1])? <= Decimal::ZERO {
        return Err(error("invalid top-of-book quantity"));
    }
    let price = decimal(&level[0])?;
    if price <= Decimal::ZERO {
        return Err(error("invalid top-of-book price"));
    }
    Ok(Some(price))
}

/// `contractType` + margin/quote asset + status is the authoritative "is this a linear,
/// USDT-margined, currently trading perpetual" test. Dated futures are excluded by their
/// contract type and by their `symbol != baseAsset + quoteAsset` layout.
fn is_tradable_usdt_perp(raw: &RawSymbol) -> bool {
    SUPPORTED_CONTRACT_TYPES.contains(&raw.contract_type.as_str())
        && raw.status == "TRADING"
        && raw.quote_asset.eq_ignore_ascii_case(QUOTE)
        && raw.margin_asset.eq_ignore_ascii_case(QUOTE)
}

fn build_instrument(raw: &RawSymbol) -> ArbResult<Instrument> {
    let mut tick_size = None;
    let mut step_size = None;
    let mut min_qty = None;
    let mut min_notional = None;
    for filter in &raw.filters {
        match filter.filter_type.as_str() {
            "PRICE_FILTER" => tick_size = filter.tick_size.as_deref(),
            "LOT_SIZE" => {
                step_size = filter.step_size.as_deref();
                min_qty = filter.min_qty.as_deref();
            }
            "MIN_NOTIONAL" => min_notional = filter.notional.as_deref(),
            _ => {}
        }
    }
    let tick_size = decimal(tick_size.ok_or_else(|| error("instrument has no PRICE_FILTER"))?)?;
    let step_size = decimal(step_size.ok_or_else(|| error("instrument has no LOT_SIZE"))?)?;
    let min_qty = decimal(min_qty.ok_or_else(|| error("instrument has no LOT_SIZE minimum"))?)?;
    let min_notional =
        decimal(min_notional.ok_or_else(|| error("instrument has no MIN_NOTIONAL filter"))?)?;
    if tick_size <= Decimal::ZERO
        || step_size <= Decimal::ZERO
        || min_qty <= Decimal::ZERO
        || min_notional < Decimal::ZERO
    {
        return Err(error(
            "instrument reports a non-positive tick, step or minimum quantity",
        ));
    }
    Ok(Instrument {
        symbol: raw.symbol.clone(),
        tick_size,
        step_size,
        min_qty,
        min_notional,
    })
}

/// Maps a scanner [`Symbol`] onto exactly one live native instrument. The native name is
/// derived from the venue's own `baseAsset` rather than by trimming the scanner's string, so
/// contract multipliers (`1000SHIB`, `1000PEPE`) resolve correctly.
fn select_instrument(symbols: &[RawSymbol], want: &Symbol) -> ArbResult<Instrument> {
    if !want.quote.eq_ignore_ascii_case(QUOTE) {
        return Err(error("only USDT-margined perpetuals are supported"));
    }
    let native = format!(
        "{}{}",
        want.base.to_ascii_uppercase(),
        want.quote.to_ascii_uppercase()
    );
    let mut found: Option<&RawSymbol> = None;
    for raw in symbols {
        if !is_tradable_usdt_perp(raw) || !raw.base_asset.eq_ignore_ascii_case(&want.base) {
            continue;
        }
        if found.is_some() || !raw.symbol.eq_ignore_ascii_case(&native) {
            return Err(error(
                "the venue lists more than one tradable instrument for this base; refusing to \
                 guess which one was meant",
            ));
        }
        found = Some(raw);
    }
    build_instrument(
        found.ok_or_else(|| {
            error("no trading USDT-margined perpetual matches the requested symbol")
        })?,
    )
}

fn reverse_index(symbols: &[RawSymbol]) -> ArbResult<HashMap<String, Symbol>> {
    let mut index = HashMap::new();
    for raw in symbols {
        if !is_tradable_usdt_perp(raw) {
            continue;
        }
        let symbol = Symbol::perp(&raw.base_asset, QUOTE);
        if index
            .insert(raw.symbol.to_ascii_uppercase(), symbol)
            .is_some()
        {
            return Err(error("exchange metadata lists a duplicate symbol"));
        }
    }
    if index.is_empty() {
        return Err(error("exchange reports no tradable USDT perpetuals"));
    }
    Ok(index)
}

fn meets_min_notional(units: Decimal, price: Decimal, minimum: Decimal) -> ArbResult<Decimal> {
    let notional = units
        .checked_mul(price)
        .ok_or_else(|| error("order notional overflow"))?;
    if notional < minimum {
        return Err(error(format!(
            "order notional {notional} is below the venue minimum {minimum}"
        )));
    }
    Ok(notional)
}

fn map_status(status: &str) -> ArbResult<OrderStatus> {
    match status {
        "NEW" | "PARTIALLY_FILLED" => Ok(OrderStatus::Open),
        "FILLED" => Ok(OrderStatus::Filled),
        // `EXPIRED` is what an IOC's unfilled remainder becomes; `EXPIRED_IN_MATCH` is the STP
        // expiry. Both are terminal without being a venue rejection.
        "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => Ok(OrderStatus::Cancelled),
        "REJECTED" => Ok(OrderStatus::Rejected),
        _ => Err(error(
            "unknown venue order status; refusing to infer terminality",
        )),
    }
}

/// An IOC either fills completely or leaves a cancelled remainder: never report `Filled` for a
/// partial execution, and never report a rejection for an order that actually traded.
fn verified_status(status: OrderStatus, requested: Decimal, executed: Decimal) -> OrderStatus {
    match status {
        OrderStatus::Rejected if executed > Decimal::ZERO => OrderStatus::Cancelled,
        OrderStatus::Filled if executed < requested => OrderStatus::Cancelled,
        other => other,
    }
}

fn aggregate_trades(
    instrument: &str,
    order_id: i64,
    executed: Decimal,
    trades: &[RawTrade],
) -> ArbResult<(Decimal, Option<Decimal>, Decimal)> {
    let mut quantity = Decimal::ZERO;
    let mut quote = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for trade in trades {
        if trade.order_id != order_id {
            return Err(error("a fill does not belong to the queried order"));
        }
        if !trade.symbol.eq_ignore_ascii_case(instrument) {
            return Err(error("a fill is for a different symbol"));
        }
        if !trade.commission_asset.eq_ignore_ascii_case(QUOTE) {
            return Err(error(
                "fees are charged in a non-USDT asset; disable the BNB fee deduction",
            ));
        }
        let trade_qty = decimal(&trade.qty)?;
        let trade_price = decimal(&trade.price)?;
        if trade_qty <= Decimal::ZERO || trade_price <= Decimal::ZERO {
            return Err(error("a fill has a non-positive quantity or price"));
        }
        quantity = quantity
            .checked_add(trade_qty)
            .ok_or_else(|| error("fill quantity overflow"))?;
        quote = quote
            .checked_add(
                trade_qty
                    .checked_mul(trade_price)
                    .ok_or_else(|| error("fill notional overflow"))?,
            )
            .ok_or_else(|| error("fill notional overflow"))?;
        fee = fee
            .checked_add(decimal(&trade.commission)?)
            .ok_or_else(|| error("fill fee overflow"))?;
    }
    if quantity != executed {
        return Err(error(
            "fills do not sum to the order's executed quantity; refusing to report a partial cost",
        ));
    }
    if quantity <= Decimal::ZERO {
        return Err(error("the order reports executions but has no fills"));
    }
    Ok((quote, Some(quote / quantity), fee))
}

fn parse_side(side: &str) -> ArbResult<Side> {
    match side {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(error("unknown venue order side")),
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

/// Rebuilds the intent for an order the venue reports but the journal does not know (created
/// elsewhere). Reconciliation must see it, so it gets an explicit external client id.
fn reconstructed_order(
    client_order_id: &ClientOrderId,
    symbol: &Symbol,
    raw: &RawOrder,
) -> ArbResult<NewOrder> {
    let price = decimal(&raw.price)?;
    let orig_qty = decimal(&raw.orig_qty)?;
    let notional = price
        .checked_mul(orig_qty)
        .ok_or_else(|| error("order notional overflow"))?;
    Ok(NewOrder {
        margin_mode: crate::MarginMode::Isolated,
        client_order_id: client_order_id.clone(),
        venue: VENUE,
        symbol: symbol.clone(),
        side: parse_side(&raw.side)?,
        notional_usdt: notional,
        quantity: Some(orig_qty),
        limit_price: (price > Decimal::ZERO).then_some(price),
        reduce_only: raw.reduce_only,
        leverage: None,
    })
}

/// The venue order id carries the symbol, because `cancel` only receives the id string while
/// Binance requires `symbol` for every order operation.
fn format_venue_order_id(symbol: &str, order_id: i64) -> String {
    format!("{symbol}:{order_id}")
}

fn parse_venue_order_id(venue_order_id: &str) -> ArbResult<(String, i64)> {
    let (symbol, raw_id) = venue_order_id
        .rsplit_once(':')
        .ok_or_else(|| error("venue order ID is not in `<symbol>:<orderId>` form"))?;
    if symbol.is_empty() {
        return Err(error("venue order ID has an empty symbol"));
    }
    let order_id = raw_id
        .parse::<i64>()
        .map_err(|_| error("venue order ID has a non-numeric order id"))?;
    Ok((symbol.to_string(), order_id))
}

/// `newClientOrderId` rules: 1..=36 characters from `[A-Za-z0-9.:/_-]`.
fn valid_client_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CLIENT_ID_LEN
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | ':' | '/' | '_' | '-')
        })
}

fn same_intent(a: &NewOrder, b: &NewOrder) -> bool {
    if a.margin_mode != b.margin_mode {
        return false;
    }
    a.client_order_id == b.client_order_id
        && a.venue == b.venue
        && a.symbol == b.symbol
        && a.side == b.side
        && a.reduce_only == b.reduce_only
        && a.notional_usdt == b.notional_usdt
        && a.quantity == b.quantity
        && a.limit_price == b.limit_price
        && a.leverage == b.leverage
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| error("order has no venue order ID; it cannot be cancelled"))?,
        status: state.status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// The official USDⓈ-M futures signature example from the venue's general information
    /// page: same key, payload and expected digest.
    const DOC_SECRET: &[u8] = b"2b5eb11e18796d12d88f13dc27dbbd02c2cc51ff7059765ed9821957d82bb4d9";
    const DOC_PAYLOAD: &str = "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=9000&timeInForce=GTC&recvWindow=5000&timestamp=1591702613943";
    const DOC_SIGNATURE: &str = "3c661234138461fcc7a7d8746c6558c9842d4e10870d2ecbedf7777cad694af9";

    #[test]
    fn hmac_matches_the_official_usds_m_futures_vector() {
        assert_eq!(
            hex_lower(&hmac_sha256(DOC_SECRET, DOC_PAYLOAD.as_bytes())),
            DOC_SIGNATURE
        );
    }

    #[test]
    fn signed_query_reproduces_the_official_signed_payload() {
        let params: Vec<(&str, String)> = vec![
            ("symbol", "BTCUSDT".into()),
            ("side", "BUY".into()),
            ("type", "LIMIT".into()),
            ("quantity", "1".into()),
            ("price", "9000".into()),
            ("timeInForce", "GTC".into()),
        ];
        let query = signed_query(DOC_SECRET, &params, 1_591_702_613_943, 5_000).unwrap();
        assert_eq!(query, format!("{DOC_PAYLOAD}&signature={DOC_SIGNATURE}"));
        assert!(
            signed_query(b"", &params, 1, 5_000).is_err(),
            "empty secret"
        );
        assert_eq!(percent_encode("a b&c"), "a%20b%26c");
    }

    #[test]
    fn client_ids_fit_the_venue_charset_and_length_limit() {
        let id = venue_client_id(ORDER_PREFIX, &ClientOrderId("position-1-buy-0".into()), 36);
        assert!(id.len() <= 36, "{}", id.len());
        assert!(valid_client_id(&id));
        assert!(id.starts_with(ORDER_PREFIX));
        assert_eq!(
            id,
            venue_client_id(ORDER_PREFIX, &ClientOrderId("position-1-buy-0".into()), 36)
        );
        assert_ne!(
            id,
            venue_client_id(ORDER_PREFIX, &ClientOrderId("position-1-buy-1".into()), 36)
        );
        assert!(!valid_client_id(""));
        assert!(!valid_client_id("has space"));
        assert!(!valid_client_id(&"x".repeat(37)));
    }

    #[test]
    fn venue_order_ids_round_trip_through_cancel_parsing() {
        let id = format_venue_order_id("1000SHIBUSDT", 42);
        assert_eq!(id, "1000SHIBUSDT:42");
        assert_eq!(
            parse_venue_order_id(&id).unwrap(),
            ("1000SHIBUSDT".to_string(), 42)
        );
        assert!(parse_venue_order_id("42").is_err());
        assert!(parse_venue_order_id("BTCUSDT:abc").is_err());
    }

    #[test]
    fn status_mapping_is_exhaustive_and_never_infers_terminality() {
        assert_eq!(map_status("NEW").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("PARTIALLY_FILLED").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("FILLED").unwrap(), OrderStatus::Filled);
        assert_eq!(map_status("CANCELED").unwrap(), OrderStatus::Cancelled);
        assert_eq!(map_status("EXPIRED").unwrap(), OrderStatus::Cancelled);
        assert_eq!(
            map_status("EXPIRED_IN_MATCH").unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(map_status("REJECTED").unwrap(), OrderStatus::Rejected);
        assert!(map_status("NEW_INSURANCE").is_err());
        assert!(map_status("filled").is_err(), "status strings are exact");
    }

    #[test]
    fn partial_ioc_is_cancelled_not_filled() {
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(1)),
            OrderStatus::Cancelled
        );
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(2)),
            OrderStatus::Filled
        );
        assert_eq!(
            verified_status(OrderStatus::Open, dec!(2), dec!(1)),
            OrderStatus::Open
        );
        assert_eq!(
            verified_status(OrderStatus::Rejected, dec!(2), dec!(1)),
            OrderStatus::Cancelled
        );
    }

    #[test]
    fn fee_aggregation_requires_usdt_and_a_matching_quantity() {
        let trade =
            |order_id: i64, symbol: &str, qty: &str, price: &str, commission: &str, asset: &str| {
                RawTrade {
                    id: 1,
                    order_id,
                    symbol: symbol.into(),
                    price: price.into(),
                    qty: qty.into(),
                    commission: commission.into(),
                    commission_asset: asset.into(),
                }
            };
        let fills = aggregate_trades(
            "BTCUSDT",
            7,
            dec!(2),
            &[
                trade(7, "BTCUSDT", "1", "100", "0.05", "USDT"),
                trade(7, "BTCUSDT", "1", "102", "0.051", "USDT"),
            ],
        )
        .unwrap();
        assert_eq!(fills.0, dec!(202));
        assert_eq!(fills.1, Some(dec!(101)));
        assert_eq!(fills.2, dec!(0.101));

        // A rebate is a negative cost, not a missing fee.
        let rebate = aggregate_trades(
            "BTCUSDT",
            7,
            dec!(1),
            &[trade(7, "BTCUSDT", "1", "100", "-0.001", "USDT")],
        )
        .unwrap();
        assert_eq!(rebate.2, dec!(-0.001));

        // Never invent a fee when it is charged in another asset.
        assert!(
            aggregate_trades(
                "BTCUSDT",
                7,
                dec!(1),
                &[trade(7, "BTCUSDT", "1", "100", "0.05", "BNB")]
            )
            .is_err()
        );
        // Never accept a fill set that does not add up to the executed quantity.
        assert!(
            aggregate_trades(
                "BTCUSDT",
                7,
                dec!(3),
                &[trade(7, "BTCUSDT", "1", "100", "0.05", "USDT")]
            )
            .is_err()
        );
        // Never accept another order's or another symbol's fills.
        assert!(
            aggregate_trades(
                "BTCUSDT",
                7,
                dec!(1),
                &[trade(8, "BTCUSDT", "1", "100", "0.05", "USDT")]
            )
            .is_err()
        );
        assert!(
            aggregate_trades(
                "ETHUSDT",
                7,
                dec!(1),
                &[trade(7, "BTCUSDT", "1", "100", "0.05", "USDT")]
            )
            .is_err()
        );
    }

    #[test]
    fn minimum_notional_is_a_hard_boundary() {
        assert_eq!(
            meets_min_notional(dec!(0.001), dec!(50000), dec!(50)).unwrap(),
            dec!(50)
        );
        assert!(meets_min_notional(dec!(0.0005), dec!(50000), dec!(50)).is_err());
        assert!(meets_min_notional(dec!(1), dec!(1), dec!(0)).is_ok());
    }

    #[test]
    fn instrument_selection_uses_live_metadata_not_string_guessing() {
        let rows = vec![
            raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING"),
            raw_symbol("BTCUSDT_261225", "BTC", "CURRENT_QUARTER", "TRADING"),
            raw_symbol("ETHUSDT", "ETH", "PERPETUAL", "SETTLING"),
            raw_symbol("1000SHIBUSDT", "1000SHIB", "PERPETUAL", "TRADING"),
            raw_symbol("TSLAUSDT", "TSLA", "TRADIFI_PERPETUAL", "TRADING"),
        ];
        let btc = select_instrument(&rows, &Symbol::perp("btc", "usdt")).unwrap();
        assert_eq!(btc.symbol, "BTCUSDT");
        assert_eq!(btc.tick_size, dec!(0.10));
        assert_eq!(btc.step_size, dec!(0.001));
        assert_eq!(btc.min_qty, dec!(0.001));
        assert_eq!(btc.min_notional, dec!(5));
        // Contract multipliers come from the venue's `baseAsset`, never from trimming the name.
        assert_eq!(
            select_instrument(&rows, &Symbol::perp("1000SHIB", "USDT"))
                .unwrap()
                .symbol,
            "1000SHIBUSDT"
        );
        assert_eq!(
            reverse_index(&rows).unwrap()["TSLAUSDT"],
            Symbol::perp("TSLA", "USDT")
        );
        // A settling market, an unknown base and a non-USDT quote are all refused.
        assert!(select_instrument(&rows, &Symbol::perp("ETH", "USDT")).is_err());
        assert!(select_instrument(&rows, &Symbol::perp("DOGE", "USDT")).is_err());
        assert!(select_instrument(&rows, &Symbol::perp("BTC", "USDC")).is_err());

        // Ambiguity is refused rather than guessed.
        let ambiguous = vec![
            raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING"),
            raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING"),
        ];
        assert!(select_instrument(&ambiguous, &Symbol::perp("BTC", "USDT")).is_err());
        assert!(reverse_index(&ambiguous).is_err());
    }

    #[test]
    fn instruments_without_filters_are_refused() {
        let mut raw = raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING");
        raw.filters
            .retain(|filter| filter.filter_type != "MIN_NOTIONAL");
        assert!(select_instrument(&[raw], &Symbol::perp("BTC", "USDT")).is_err());
    }

    #[test]
    fn duplicate_and_unknown_errors_are_not_definitive_rejections() {
        assert!(!definitive_rejection(-4116));
        assert!(!definitive_rejection(-9999));
        for code in AMBIGUOUS_CODES {
            assert!(!definitive_rejection(code));
        }
        assert!(definitive_rejection(-2019));
        assert!(definitive_rejection(-4164));
    }

    #[test]
    fn base_quantities_round_down_but_reduce_only_must_be_exact() {
        let mut order = NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("quantity-test".into()),
            venue: VENUE,
            symbol: Symbol::perp("BTC", QUOTE),
            side: Side::Buy,
            notional_usdt: dec!(100),
            quantity: None,
            limit_price: Some(dec!(30)),
            reduce_only: false,
            leverage: Some(dec!(2)),
        };
        assert_eq!(
            order_units(
                VENUE,
                &order,
                dec!(30),
                Decimal::ONE,
                dec!(0.01),
                dec!(0.01)
            )
            .unwrap(),
            dec!(3.33)
        );
        order.quantity = Some(dec!(1.239));
        assert_eq!(
            order_units(
                VENUE,
                &order,
                dec!(30),
                Decimal::ONE,
                dec!(0.01),
                dec!(0.01)
            )
            .unwrap(),
            dec!(1.23)
        );
        order.reduce_only = true;
        assert!(
            order_units(
                VENUE,
                &order,
                dec!(30),
                Decimal::ONE,
                dec!(0.01),
                dec!(0.01)
            )
            .is_err()
        );
    }

    #[test]
    fn string_business_codes_are_checked_without_echoing_messages() {
        let response = serde_json::json!({"code":"-2015","msg":"secret-must-not-escape"});
        assert_eq!(business_code(&response), Some(-2015));
        assert!(!vendor_message(&response, StatusCode::UNAUTHORIZED).contains("secret"));
        assert_eq!(business_code(&serde_json::json!({"code":"200"})), Some(200));
    }

    fn raw_symbol(symbol: &str, base: &str, contract_type: &str, status: &str) -> RawSymbol {
        RawSymbol {
            symbol: symbol.into(),
            base_asset: base.into(),
            quote_asset: QUOTE.into(),
            margin_asset: QUOTE.into(),
            contract_type: contract_type.into(),
            status: status.into(),
            filters: vec![
                RawFilter {
                    filter_type: "PRICE_FILTER".into(),
                    tick_size: Some("0.10".into()),
                    step_size: None,
                    min_qty: None,
                    notional: None,
                },
                RawFilter {
                    filter_type: "LOT_SIZE".into(),
                    tick_size: None,
                    step_size: Some("0.001".into()),
                    min_qty: Some("0.001".into()),
                    notional: None,
                },
                RawFilter {
                    filter_type: "MIN_NOTIONAL".into(),
                    tick_size: None,
                    step_size: None,
                    min_qty: None,
                    notional: Some("5".into()),
                },
            ],
        }
    }
}
