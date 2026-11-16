//! Production Bitget **unified trading account** (UTA, `/api/v3`) USDT-M futures broker
//! (`category=USDT-FUTURES`). Bitget calls the classic account "maintenance mode" and an
//! upgraded account's key no longer reaches the classic `/api/v2/mix` endpoints, so an upgraded
//! account trades through this broker (`ARB_BITGET_ACCOUNT=uta`, the default); the classic one
//! ([`crate::bitget_broker`]) stays for accounts that were not upgraded.
//!
//! Same rules as the classic broker: trading is disabled unless [`LiveOptions`] enables it with
//! a `market_slippage`; every order is LIMIT + IOC at a price we choose; quantities never round
//! up; the exclusively locked intent journal is fsynced BEFORE the order is sent and a reserved
//! client id is never resubmitted. Signing is unchanged from v2 (Base64 HMAC-SHA256 over
//! `timestamp + METHOD + path[?query] + body`).
//!
//! # What differs from the classic account
//!
//! - **Margin mode is per order** (`marginMode`), and an order without it opens **cross**: every
//!   order, reduce-only exits included, carries the mode of its [`NewOrder`]. Leverage is set
//!   per symbol and margin mode (`set-leverage`) and read back from `settings.symbolConfigList`.
//! - The account must be unified (`accountMode=unified`, not mid-upgrade) and one-way
//!   (`holdMode=one_way_mode`); both are checked at connect, never changed.
//! - Contract status names: `online` trades; `limit_open` only closes; `limit_close` is treated
//!   as fully halted (a leg we could open but not close is worse than none); `offline`,
//!   `restrictedAPI`, `listed` are halted. RWA contracts get the same windows as the classic
//!   broker: opening refused over weekends and US holidays, closing only around holidays
//!   ([`rwa_closed_for`]).
//! - Error codes are new: `25204` = order does not exist; `25000` / `25001` / `25003` / `25004` /
//!   `25239` / `25653` / `40500` / `40725` / `40010` / `45001` and HTTP 429 leave the outcome
//!   **unknown**; `25212` (duplicate clientOid) means the id is already at the venue — look it
//!   up, never record a refusal.
//! - Fills and financial records cannot be filtered by symbol: they are read in 30-day windows
//!   and filtered here. Pagination is a `cursor` (the smallest id of the page).
//! - Fee amounts are taken by absolute value: every order is taker IOC (no post-only), so a fee
//!   is always a cost, whatever sign convention a field uses.
//! - **Cancelled API orders are kept for only two hours.** A zero-fill IOC ends `cancelled`, so
//!   its terminal state is journaled the moment `place` reads it; an intent left without a
//!   terminal state for longer than that can no longer be looked up and is reported (never
//!   assumed unsubmitted) for reconciliation.
//!
//! # Operator configuration (checked at connect, never changed)
//! - Account upgraded to UTA, basic or isolated-margin account level, one-way holding mode,
//!   BGB fee deduction off, USDT in the unified account (not the funding account).
//! - API key permissions: UTA trade (read & write) and UTA management (read & write — leverage
//!   and position margin are management writes); no withdrawal.
//!
//! Sources (official docs, `https://www.bitget.com/docs/catalog/…`, 2026-10-21):
//! `trading/order-management` (place / cancel / order-info / unfilled-orders / fills),
//! `trading/position-management` (current-position), `account/account-settings` (settings,
//! set-leverage, set-margin, deduct-info), `account/assets-balance` (assets, financial-records),
//! `account/risk-position` (all-fee-rate), `market/market-data` (instruments, orderbook),
//! `/docs/uta/enum` (financial record types), `/docs/uta/error-code/restapi`.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex as SyncMutex};
use std::time::Duration;

use arb_core::{ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::bitget_broker::{
    BASE, BitgetCredentials, CLIENT_MAX, CLIENT_PREFIX, CONTRACTS_TTL, FILL_SYNC_ATTEMPTS,
    FILL_SYNC_GAP, HISTORY_MAX_PAGES, HISTORY_PAGE, Instrument, MARGIN_COIN, PATH_TIME,
    SETTINGS_TTL, STATUS_TTL, SUCCESS, ServerTime, TradeRate, ack, add, api_error, banded,
    check_ok, data_of, dec, divide, envelope_code, error, external_id, highest_taker_fee,
    limit_ratio, margin_amount_text, millis, multiply, native_name, nonneg, normalized, now_ms,
    opt_dec, parse_side, query_string, rwa_closed_for, rwa_closed_note, same_intent, sensitive,
    signing_payload, symbol_from_native, utc_millis, value_dec, verified_status, windows,
    within_retention,
};
use crate::broker::{Broker, FundingTotal, MarginOutcome, VenuePosition};
use crate::live_common::{
    Cached, JournalEntry, LiveOptions, OrderJournal, Verified, base64_standard, hex_lower,
    hmac_sha256, order_units, round_price, transport_error, venue_client_id,
};
use crate::settlement::{FillEffect, VenueFill};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const CATEGORY: &str = "USDT-FUTURES";
/// `25204 Order does not exist`.
const CODE_NOT_FOUND: &str = "25204";
/// `25245 The account is not the unified account mode`.
const CODE_NOT_UNIFIED: &str = "25245";
/// Codes after which a write may still have been applied (system error, timeout, concurrent
/// operation, too frequent, "closing failed / order changed, try again", service error, and the
/// classic timeout / unknown codes the place-order page still lists), plus `25212` duplicate
/// clientOid: the id is already at the venue.
const AMBIGUOUS_CODES: [&str; 11] = [
    "25000", "25001", "25003", "25004", "25212", "25239", "25653", "40500", "40725", "40010",
    "45001",
];
const PATH_INSTRUMENTS: &str = "/api/v3/market/instruments";
const PATH_ORDERBOOK: &str = "/api/v3/market/orderbook";
const PATH_SETTINGS: &str = "/api/v3/account/settings";
const PATH_ALL_FEE_RATE: &str = "/api/v3/account/all-fee-rate";
const PATH_ASSETS: &str = "/api/v3/account/assets";
const PATH_FINANCIAL_RECORDS: &str = "/api/v3/account/financial-records";
const PATH_SET_LEVERAGE: &str = "/api/v3/account/set-leverage";
const PATH_SET_MARGIN: &str = "/api/v3/account/set-margin";
const PATH_PLACE: &str = "/api/v3/trade/place-order";
/// 按市价平掉某合约的整个持仓（没有数量参数），只用于平低于下限的零头，见 [`dust_rule`]。
const PATH_CLOSE_POSITIONS: &str = "/api/v3/trade/close-positions";
const PATH_CANCEL: &str = "/api/v3/trade/cancel-order";
const PATH_ORDER_INFO: &str = "/api/v3/trade/order-info";
const PATH_UNFILLED: &str = "/api/v3/trade/unfilled-orders";
const PATH_FILLS: &str = "/api/v3/trade/fills";
const PATH_POSITIONS: &str = "/api/v3/position/current-position";
/// `fills` and `financial-records` refuse windows over 30 days; a second of margin.
const HISTORY_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1_000 - 1_000;
/// Funding settlements in the financial records (isolated, cross, and the RWA cash-dividend
/// adjustments of both), user side only. The sign comes from the `_IN` / `_OUT` suffix.
const FUNDING_TYPES: [&str; 8] = [
    "MARGIN_SETTLE_FEE_USER_IN",
    "MARGIN_SETTLE_FEE_USER_OUT",
    "CONTRACT_MAIN_SETTLE_FEE_USER_IN",
    "CONTRACT_MAIN_SETTLE_FEE_USER_OUT",
    "RWA_FIXED_SETTLE_FEE_USER_IN",
    "RWA_FIXED_SETTLE_FEE_USER_OUT",
    "RWA_CONTRACT_MAIN_SETTLE_FEE_USER_IN",
    "RWA_CONTRACT_MAIN_SETTLE_FEE_USER_OUT",
];

/// Which Bitget account the operator uses (`ARB_BITGET_ACCOUNT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BitgetAccount {
    /// Unified trading account, `/api/v3`.
    #[default]
    Uta,
    /// Classic account, `/api/v2/mix`.
    Classic,
}

impl BitgetAccount {
    /// `uta` (or unset) / `classic`.
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("" | "uta" | "unified") => Ok(Self::Uta),
            Some("classic") => Ok(Self::Classic),
            Some(_) => {
                Err("ARB_BITGET_ACCOUNT 只能是 uta（统一账户，默认）或 classic（经典账户）".into())
            }
        }
    }
}

pub struct BitgetUtaBroker {
    client: reqwest::Client,
    credentials: BitgetCredentials,
    options: LiveOptions,
    journal: SyncMutex<OrderJournal>,
    /// Serializes order submission so duplicate ids cannot race a single process.
    submit: Mutex<()>,
    fee_per_side: Decimal,
    /// `server_time - local_time`, in milliseconds; applied to every signed timestamp.
    time_offset_ms: i64,
    instruments: Cached<Arc<Vec<InstrumentRow>>>,
    /// (native symbol, leverage, margin mode) read back from the account recently.
    settings: Verified<(String, Decimal, crate::MarginMode)>,
    /// 零头整笔平仓（close-positions）的交易所订单号：我们的订单号 → orderId（见 [`Self::close_whole_dust`]）。
    /// 那张市价单用的是交易所自己生成的 clientOid，按我们的订单号永远查不到；拿到 orderId 当场记进这里，
    /// 并追加写进订单日志旁的 `.dust-orders` 文件（重启后照样查得到）。不写进订单日志本身：旧版本不认新的记录类型，
    /// 回滚时会读不了整份日志。
    dust_orders: SyncMutex<HashMap<String, String>>,
    dust_path: Option<std::path::PathBuf>,
}

/// 零头平仓订单号旁路文件的一行。
#[derive(Serialize, Deserialize)]
struct DustOrder {
    client_order_id: String,
    order_id: String,
}

fn dust_path(journal_path: &std::path::Path) -> std::path::PathBuf {
    let mut name = journal_path.as_os_str().to_owned();
    name.push(".dust-orders");
    std::path::PathBuf::from(name)
}

fn load_dust_orders(path: &std::path::Path) -> HashMap<String, String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<DustOrder>(line).ok())
        .map(|row| (row.client_order_id, row.order_id))
        .collect()
}

impl BitgetUtaBroker {
    /// Build a broker. Reads the server clock and the instrument list, verifies the account is
    /// unified and one-way (never changing it) and reads the account's taker fee rates. Fails
    /// closed if any of these is unavailable.
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
        let mut broker = Self::assemble(client, credentials, options, journal);
        let dust = dust_path(journal_path);
        broker.dust_orders = SyncMutex::new(load_dust_orders(&dust));
        broker.dust_path = Some(dust);
        broker.time_offset_ms = broker.server_offset().await?;
        broker.instrument_rows(Duration::ZERO).await?;
        let (status, envelope) = broker
            .request_raw(true, Method::GET, PATH_SETTINGS, &[], None)
            .await?;
        if envelope_code(&envelope) == CODE_NOT_UNIFIED {
            return Err(error(
                "Bitget 账户还不是统一账户：升级后再连，或者设 ARB_BITGET_ACCOUNT=classic 用经典接口",
            ));
        }
        if !status.is_success() {
            return Err(error(format!(
                "{PATH_SETTINGS}: HTTP {status}, {}（Key 要有「统一账户管理」读权限）",
                api_error(
                    envelope_code(&envelope),
                    envelope.get("msg").and_then(Value::as_str).unwrap_or("")
                )
            )));
        }
        check_ok(&envelope)?;
        check_account(&data_of(envelope)?)?;
        let rates: Vec<TradeRate> = broker
            .signed_data(
                Method::GET,
                PATH_ALL_FEE_RATE,
                &[("category", CATEGORY.to_string())],
                None,
            )
            .await?;
        let (rate, uniform) = highest_taker_fee(&rates)?;
        if !uniform {
            tracing::warn!(
                fallback = %rate,
                "Bitget 统一账户各合约的吃单费率不一样：成本估算的回落值按最高的算"
            );
        }
        broker.fee_per_side = rate;
        Ok(broker)
    }

    fn assemble(
        client: reqwest::Client,
        credentials: BitgetCredentials,
        options: LiveOptions,
        journal: OrderJournal,
    ) -> Self {
        Self {
            client,
            credentials,
            options,
            journal: SyncMutex::new(journal),
            submit: Mutex::new(()),
            fee_per_side: Decimal::ZERO,
            time_offset_ms: 0,
            instruments: Cached::new(),
            settings: Verified::new(SETTINGS_TTL),
            dust_orders: SyncMutex::new(HashMap::new()),
            dust_path: None,
        }
    }

    /// 记下零头平仓的交易所订单号（内存 + 旁路文件，写文件失败只告警：内存里有，这次进程照样查得到）。
    fn remember_dust_order(&self, client_order_id: &ClientOrderId, order_id: &str) {
        if let Ok(mut orders) = self.dust_orders.lock() {
            orders.insert(client_order_id.0.clone(), order_id.to_string());
        }
        let Some(path) = &self.dust_path else {
            return;
        };
        let line = serde_json::to_string(&DustOrder {
            client_order_id: client_order_id.0.clone(),
            order_id: order_id.to_string(),
        })
        .unwrap_or_default();
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut file| {
                use std::io::Write;
                writeln!(file, "{line}")?;
                file.sync_data()
            });
        if let Err(failure) = written {
            tracing::warn!(path = %path.display(), %failure, "Bitget 零头平仓订单号没写进旁路文件（这次进程内仍查得到）");
        }
    }

    fn dust_order_id(&self, client_order_id: &ClientOrderId) -> Option<String> {
        self.dust_orders
            .lock()
            .ok()?
            .get(&client_order_id.0)
            .cloned()
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

    /// Any parsed response with its HTTP status (business errors come back as 4xx + a code, so
    /// callers that must tell them apart read the code themselves). Transport failures stay
    /// errors.
    async fn request_raw(
        &self,
        signed: bool,
        method: Method,
        path: &str,
        params: &[(&str, String)],
        body: Option<Value>,
    ) -> ArbResult<(StatusCode, Value)> {
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
            let timestamp = (now_ms() + self.time_offset_ms).to_string();
            let body_str = body_bytes
                .as_deref()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .unwrap_or("");
            let message = signing_payload(&timestamp, method.as_str(), path, &query, body_str);
            let signature = base64_standard(&hmac_sha256(
                self.credentials.api_secret.as_bytes(),
                message.as_bytes(),
            ));
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
            let code = envelope_code(&envelope);
            let message = envelope.get("msg").and_then(Value::as_str).unwrap_or("");
            return Err(error(format!(
                "{path}: HTTP {status}, {}",
                api_error(if code.is_empty() { "-" } else { code }, message)
            )));
        }
        Ok(envelope)
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
            .and_then(|raw| i64::from_str(raw).ok())
            .ok_or_else(|| error("server time is unavailable"))?;
        Ok(server - now_ms())
    }

    /// One history window `[from, until]` of a cursor-paged list, every page. A cursor that stops
    /// moving, or more pages than allowed, is an error rather than a short answer.
    async fn history<T: DeserializeOwned>(
        &self,
        path: &str,
        (from, until): (i64, i64),
    ) -> ArbResult<Vec<T>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..HISTORY_MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("startTime", from.to_string()),
                ("endTime", until.to_string()),
                ("limit", HISTORY_PAGE.to_string()),
            ];
            if let Some(cursor) = &cursor {
                params.push(("cursor", cursor.clone()));
            }
            let page: Page<T> = self.signed_data(Method::GET, path, &params, None).await?;
            let full = page.list.len() >= HISTORY_PAGE;
            out.extend(page.list);
            if !full {
                return Ok(out);
            }
            cursor = Some(next_cursor(page.cursor, cursor.as_deref())?);
        }
        Err(error(
            "history needs more pages than allowed; refusing a partial history",
        ))
    }

    // ---- Instruments and account settings -----------------------------------------------

    /// The instrument list, no older than `max_age` (`Duration::ZERO` = always re-read).
    async fn instrument_rows(&self, max_age: Duration) -> ArbResult<Arc<Vec<InstrumentRow>>> {
        if let Some(rows) = self.instruments.get(max_age) {
            return Ok(rows);
        }
        let rows: Vec<InstrumentRow> = self
            .public_data(PATH_INSTRUMENTS, &[("category", CATEGORY.to_string())])
            .await?;
        let rows = Arc::new(rows);
        self.instruments.put(Arc::clone(&rows));
        Ok(rows)
    }

    /// The tradable contract for `symbol`; `closing` also accepts a `limit_open` contract. A
    /// cached list without it is re-read once before giving up.
    async fn instrument_for(&self, symbol: &Symbol, closing: bool) -> ArbResult<Instrument> {
        let rows = self.instrument_rows(CONTRACTS_TTL).await?;
        match select_instrument(&rows, symbol, closing) {
            Ok(instrument) => Ok(instrument),
            Err(_) => {
                let fresh = self.instrument_rows(Duration::ZERO).await?;
                select_instrument(&fresh, symbol, closing)
            }
        }
    }

    async fn account_settings(&self) -> ArbResult<AccountSettings> {
        self.signed_data(Method::GET, PATH_SETTINGS, &[], None)
            .await
    }

    /// Set the symbol's leverage for `mode` and read it back. Nothing is written when the
    /// account already shows exactly that.
    async fn configure(
        &self,
        native: &str,
        leverage: Decimal,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.authorize()?;
        let key = (native.to_string(), leverage, mode);
        if self.settings.is_fresh(&key) {
            return Ok(());
        }
        if mode_in_force(&self.account_settings().await?, native, leverage)? == Some(mode) {
            self.settings.mark(key);
            return Ok(());
        }
        // One-way isolated and cross both take the single `leverage` field (no `posSide`).
        self.signed_data::<Value>(
            Method::POST,
            PATH_SET_LEVERAGE,
            &[],
            Some(json!({
                "category": CATEGORY,
                "symbol": native,
                "leverage": leverage.normalize().to_string(),
                "marginMode": wire_mode(mode),
            })),
        )
        .await?;
        if mode_in_force(&self.account_settings().await?, native, leverage)? != Some(mode) {
            return Err(error(format!(
                "{native} {} {}x was not confirmed by the account settings after set-leverage",
                wire_mode(mode),
                leverage.normalize()
            )));
        }
        self.settings.mark(key);
        Ok(())
    }

    /// Instrument rows fresh enough for a status check: refreshed when older than
    /// [`STATUS_TTL`], or, if that read fails, any list still within [`CONTRACTS_TTL`].
    async fn status_rows(&self) -> ArbResult<Arc<Vec<InstrumentRow>>> {
        match self.instrument_rows(STATUS_TTL).await {
            Ok(rows) => Ok(rows),
            Err(failure) => self.instruments.get(CONTRACTS_TTL).ok_or(failure),
        }
    }

    // ---- Orders ---------------------------------------------------------------------------

    /// Bounded limit price for an order without an explicit limit, from a fresh book.
    async fn bound_price(
        &self,
        native: &str,
        side: Side,
        limit_ratio: Option<Decimal>,
    ) -> ArbResult<Decimal> {
        let book: OrderBook = self
            .public_data(
                PATH_ORDERBOOK,
                &[
                    ("category", CATEGORY.to_string()),
                    ("symbol", native.to_string()),
                    ("limit", "5".to_string()),
                ],
            )
            .await?;
        let ts = millis(book.ts.as_deref())?;
        if (now_ms() + self.time_offset_ms - ts).abs() > 5_000 {
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
        let bid = best(&book.b, false)?;
        let ask = best(&book.a, true)?;
        banded(&self.options, limit_ratio).bound_price(Venue::Bitget, side, bid, ask)
    }

    /// The order by client id; `None` only on the documented "order does not exist".
    async fn fetch_order(&self, client_oid: &str) -> ArbResult<Option<RemoteOrder>> {
        self.order_info(("clientOid", client_oid)).await
    }

    async fn order_info(&self, (key, value): (&str, &str)) -> ArbResult<Option<RemoteOrder>> {
        let (status, envelope) = self
            .request_raw(
                true,
                Method::GET,
                PATH_ORDER_INFO,
                &[(key, value.to_string())],
                None,
            )
            .await?;
        if status.is_server_error() {
            return Err(error(format!("order info: HTTP {status}")));
        }
        match envelope_code(&envelope) {
            SUCCESS => match envelope.get("data") {
                Some(data) if data.as_object().is_some_and(|o| !o.is_empty()) => {
                    serde_json::from_value(data.clone())
                        .map(Some)
                        .map_err(|_| error("malformed order info response"))
                }
                _ => Ok(None),
            },
            CODE_NOT_FOUND => Ok(None),
            code => Err(api_error(
                code,
                envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
            )),
        }
    }

    /// Right after a successful place the order may take a moment to become queryable.
    async fn poll_order(&self, client_oid: &str) -> ArbResult<RemoteOrder> {
        for _ in 0..12 {
            if let Some(order) = self.fetch_order(client_oid).await? {
                return Ok(order);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        Err(error(
            "accepted order is not yet queryable; reserved id retained for reconciliation",
        ))
    }

    async fn order_fills(&self, order_id: &str) -> ArbResult<Vec<FillRow>> {
        let mut fills = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..HISTORY_MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("orderId", order_id.to_string()),
                ("limit", HISTORY_PAGE.to_string()),
            ];
            if let Some(cursor) = &cursor {
                params.push(("cursor", cursor.clone()));
            }
            let page: Page<FillRow> = self
                .signed_data(Method::GET, PATH_FILLS, &params, None)
                .await?;
            let full = page.list.len() >= HISTORY_PAGE;
            fills.extend(page.list);
            if !full {
                return Ok(fills);
            }
            cursor = Some(next_cursor(page.cursor, cursor.as_deref())?);
        }
        Err(error("fill list exceeds the reconciliation bound"))
    }

    async fn state_from_detail(
        &self,
        remote: &RemoteOrder,
        requested_id: Option<&ClientOrderId>,
    ) -> ArbResult<OrderState> {
        let symbol = symbol_from_native(remote.symbol.as_deref().unwrap_or(""))?;
        let side = parse_side(remote.side.as_deref().unwrap_or(""))?;
        let requested = nonneg(remote.qty.as_deref())?;
        if requested <= Decimal::ZERO {
            return Err(error("order info reports a non-positive quantity"));
        }
        let executed = nonneg(remote.cum_exec_qty.as_deref())?;
        let order_id = remote
            .order_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| error("order info has no order id"))?;
        // Fill rows can trail the order info for a moment right after the order ends.
        let mut synced = None;
        for attempt in 0..FILL_SYNC_ATTEMPTS {
            let fills = self.order_fills(&order_id).await?;
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
            map_status(remote.order_status.as_deref().unwrap_or(""))?,
            requested,
            filled_qty,
        )?;
        let limit = opt_dec(remote.price.as_deref())?;
        let reduce_only = remote
            .reduce_only
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("YES"));
        let order = self.intent_for(
            remote.client_oid.as_deref(),
            &order_id,
            requested_id,
            Intent {
                symbol,
                side,
                reduce_only,
                units: requested,
                limit,
                filled_notional,
                margin_mode: reported_mode(remote.margin_mode.as_deref()),
            },
        )?;
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(order_id);
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

    /// The order this venue row belongs to: our persisted intent (which must match), or an
    /// external order reconstructed from the row.
    fn intent_for(
        &self,
        client_oid: Option<&str>,
        order_id: &str,
        requested_id: Option<&ClientOrderId>,
        intent: Intent,
    ) -> ArbResult<NewOrder> {
        let saved = match client_oid.filter(|id| !id.is_empty()) {
            Some(id) => self.with_journal(|journal| Ok(journal.by_venue_client_id(id).cloned()))?,
            None => None,
        };
        let order = match saved {
            Some(entry) => {
                if entry.order.symbol != intent.symbol
                    || entry.order.side != intent.side
                    || entry.order.reduce_only != intent.reduce_only
                    || entry.units != intent.units
                {
                    return Err(error("venue order does not match the persisted intent"));
                }
                entry.order
            }
            None => NewOrder {
                post_only: false,
                margin_mode: intent.margin_mode,
                client_order_id: requested_id
                    .cloned()
                    .unwrap_or_else(|| external_id(order_id)),
                venue: Venue::Bitget,
                symbol: intent.symbol,
                side: intent.side,
                notional_usdt: match intent.limit {
                    Some(price) => multiply(intent.units, price)?,
                    None => intent.filled_notional,
                },
                quantity: Some(intent.units),
                limit_price: intent.limit,
                reduce_only: intent.reduce_only,
                leverage: None,
            },
        };
        if let Some(requested) = requested_id
            && order.client_order_id != *requested
        {
            return Err(error("venue order does not match the requested client id"));
        }
        Ok(order)
    }

    fn state_from_pending(&self, row: &RemoteOrder) -> ArbResult<OrderState> {
        let symbol = symbol_from_native(row.symbol.as_deref().unwrap_or(""))?;
        let side = parse_side(row.side.as_deref().unwrap_or(""))?;
        let requested = nonneg(row.qty.as_deref())?;
        let executed = nonneg(row.cum_exec_qty.as_deref())?;
        let status = verified_status(
            map_status(row.order_status.as_deref().unwrap_or(""))?,
            requested,
            executed,
        )?;
        let filled_usdt = nonneg(row.cum_exec_value.as_deref())?;
        let order_id = row.order_id.clone().unwrap_or_default();
        let order = self.intent_for(
            row.client_oid.as_deref(),
            &order_id,
            None,
            Intent {
                symbol,
                side,
                reduce_only: row
                    .reduce_only
                    .as_deref()
                    .is_some_and(|v| v.eq_ignore_ascii_case("YES")),
                units: requested,
                limit: opt_dec(row.price.as_deref())?,
                filled_notional: filled_usdt,
                margin_mode: reported_mode(row.margin_mode.as_deref()),
            },
        )?;
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(order_id);
        state.status = status;
        state.filled_usdt = filled_usdt;
        state.average_price = if executed > Decimal::ZERO && filled_usdt > Decimal::ZERO {
            Some(divide(filled_usdt, executed)?)
        } else {
            opt_dec(row.avg_price.as_deref())?
        };
        Ok(state)
    }

    async fn position_rows(&self) -> ArbResult<Vec<PositionRow>> {
        let page: Page<PositionRow> = self
            .signed_data(
                Method::GET,
                PATH_POSITIONS,
                &[("category", CATEGORY.to_string())],
                None,
            )
            .await?;
        Ok(page.list)
    }

    /// 整笔平掉低于下限（5 USDT）的零头。
    ///
    /// 某次实盘：平仓 IOC 卖 0.102 只成交 0.101，剩 0.001（约 0.82 USDT）低于下限，之后每次
    /// 退出重试都被本地下限拦下，这条腿裸了 35 分钟，最后手动在交易所平掉。place-order 要求不低于
    /// `minOrderAmount`，这里改用 close-positions：按市价平掉该合约的整个持仓。只在本单数量正好等于
    /// 整笔持仓时走这里（[`dust_rule`]），单向持仓下一个合约只有一笔仓位。
    ///
    /// 交易所生成自己的 clientOid：先照常预约意图（崩在中途也留痕），按回包的 orderId 查单，
    /// 终态记在我们的订单号下。
    async fn close_whole_dust(
        &self,
        order: &NewOrder,
        native: &str,
        client_oid: &str,
        units: Decimal,
    ) -> ArbResult<OrderAck> {
        self.with_journal(|journal| {
            journal.reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: client_oid.to_string(),
                instrument: native.to_string(),
                units,
                terminal: None,
            })
        })?;
        let submitted = self
            .request_raw(
                true,
                Method::POST,
                PATH_CLOSE_POSITIONS,
                &[],
                Some(json!({ "category": CATEGORY, "symbol": native })),
            )
            .await;
        let reply = match &submitted {
            Ok((status, envelope)) => dust_reply(*status, envelope),
            Err(_) => DustReply::Unknown,
        };
        let refuse = |code: String, message: String| -> ArbResult<OrderAck> {
            let mut rejected = OrderState::new(order.clone());
            rejected.status = OrderStatus::Rejected;
            rejected.reject_reason = Some(format!("Bitget code {code}"));
            self.with_journal(|journal| journal.record_terminal(&rejected))?;
            Err(api_error(&code, &message))
        };
        let order_id = match reply {
            DustReply::Placed(order_id) => {
                // 先记下交易所订单号再去查：之后查不到终态、查单出错、进程中途崩溃，都还能按它找到这张单。
                self.remember_dust_order(&order.client_order_id, &order_id);
                order_id
            }
            DustReply::Refused(code, message) => return refuse(code, message),
            DustReply::Unknown => {
                return match submitted {
                    Err(failure) => Err(failure),
                    Ok((status, envelope)) => Err(error(format!(
                        "close-positions outcome unknown (HTTP {status}, code {}); reserved id \
                         retained, reconcile by position",
                        envelope_code(&envelope)
                    ))),
                };
            }
        };
        // 市价单很快到终态；查得到且不再活跃才算数。
        let mut remote = None;
        for _ in 0..20 {
            if let Some(row) = self.order_info(("orderId", &order_id)).await?
                && map_status(row.order_status.as_deref().unwrap_or(""))
                    .is_ok_and(|status| !status.is_live())
            {
                remote = Some(row);
                break;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        let remote = remote.ok_or_else(|| {
            error(format!(
                "close-positions order {order_id} is not yet terminal; reserved id retained, \
                 reconcile by position"
            ))
        })?;
        if !same_symbol(remote.symbol.as_deref(), native)
            || parse_side(remote.side.as_deref().unwrap_or(""))? != order.side
        {
            return Err(error(
                "close-positions order does not match the requested side/symbol",
            ));
        }
        let mut state = self
            .state_from_detail(&remote, Some(&order.client_order_id))
            .await?;
        // 交易所那边是另一个 clientOid 的市价单：终态挂在本单的意图下。
        state.order = order.clone();
        self.record_terminal(&state)?;
        ack(&state)
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
impl Broker for BitgetUtaBroker {
    fn venue(&self) -> Venue {
        Venue::Bitget
    }

    /// 下单按 [`order_units`] 把数量向下取整到市场的数量步长（只减仓单必须正好是它的整数倍）。
    async fn quantity_step(&self, symbol: &Symbol) -> ArbResult<Option<Decimal>> {
        Ok(Some(self.instrument_for(symbol, true).await?.step))
    }

    fn fee_per_side(&self) -> Decimal {
        self.fee_per_side
    }

    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let native = native_name(symbol);
        let rows = self.position_rows().await?;
        let Some(row) = rows.iter().find(|row| {
            same_symbol(row.symbol.as_deref(), &native)
                && nonneg(row.total.as_deref()).is_ok_and(|total| total > Decimal::ZERO)
        }) else {
            return Ok(None);
        };
        Ok(Some(crate::margin::venue_state(
            row.margin_mode
                .as_deref()
                .and_then(crate::margin::reported_mode),
            opt_dec(row.liquidation_price.as_deref())?,
            opt_dec(row.position_balance.as_deref())?,
        )))
    }

    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let instrument = self.instrument_for(symbol, false).await?;
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
        if let Some(mode) = mode_in_force(
            &self.account_settings().await?,
            &instrument.native,
            leverage,
        )? {
            self.settings.mark((instrument.native, leverage, mode));
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
        let instrument = self.instrument_for(symbol, false).await?;
        let leverage = crate::margin::leverage(Venue::Bitget, leverage)?;
        if leverage < instrument.min_lever || leverage > instrument.max_lever {
            return Err(error("leverage exceeds market limits"));
        }
        self.configure(&instrument.native, leverage, mode).await
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
        if let Some(reason) = status_halt(row.status.as_deref(), reduce_only) {
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
                    entry.terminal.clone(),
                )
            }))
        })?;
        if let Some((saved_order, client_oid, terminal)) = reserved {
            if !same_intent(&saved_order, order) {
                return Err(error("client order id reused with a different intent"));
            }
            if let Some(state) = terminal {
                return ack(&state);
            }
            let remote = self.fetch_order(&client_oid).await?.ok_or_else(|| {
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
                .ok_or_else(|| error("opening order requires explicit leverage"))?;
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
        let below_minimum =
            instrument.min_notional > Decimal::ZERO && notional < instrument.min_notional;
        if below_minimum && !order.reduce_only {
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
            match dust_rule(below_minimum, units, net) {
                DustRule::Place => {}
                DustRule::Refuse => {
                    return Err(error(
                        "order is below the venue minimum notional (only an order closing the \
                         whole position may be smaller)",
                    ));
                }
                DustRule::CloseWhole => {
                    return self
                        .close_whole_dust(order, &instrument.native, &client_oid, units)
                        .await;
                }
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
        let body = order_body(&instrument.native, order, units, price, &client_oid);
        let submitted = self
            .request_raw(true, Method::POST, PATH_PLACE, &[], Some(body))
            .await;
        let accepted = matches!(&submitted, Ok((status, envelope))
            if status.is_success() && envelope_code(envelope) == SUCCESS);
        // The venue's own record decides, whatever the reply said (never the acknowledgement).
        let remote = if accepted {
            Some(self.poll_order(&client_oid).await?)
        } else {
            self.fetch_order(&client_oid).await?
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
            Ok((status, envelope)) if definitive_refusal(status, envelope_code(&envelope)) => {
                let code = envelope_code(&envelope).to_string();
                let reason = api_error(
                    &code,
                    envelope.get("msg").and_then(Value::as_str).unwrap_or(""),
                );
                // Filters or the contract status may have changed: re-read them next time.
                self.instruments.clear();
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
            Err(failure) => Err(failure),
        }
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let entry = self.with_journal(|journal| {
            Ok(journal
                .get(client_order_id)
                .map(|entry| (entry.venue_client_id.clone(), entry.terminal.clone())))
        })?;
        let Some((client_oid, terminal)) = entry else {
            // We always reserve an intent before transmitting; no intent means it was never
            // submitted, which is exactly the only case where `None` is permitted.
            return Ok(None);
        };
        if let Some(state) = terminal {
            return Ok(Some(state));
        }
        match self.fetch_order(&client_oid).await? {
            Some(remote) => {
                let state = self
                    .state_from_detail(&remote, Some(client_order_id))
                    .await?;
                self.record_terminal(&state)?;
                Ok(Some(state))
            }
            // 零头整笔平仓：交易所那张单是它自己的 clientOid，按记下的 orderId 查（见 `close_whole_dust`）。
            None if let Some(order_id) = self.dust_order_id(client_order_id) => {
                let Some(remote) = self.order_info(("orderId", &order_id)).await? else {
                    return Err(error(format!(
                        "close-positions order {order_id} is not found at the venue; reconcile by position"
                    )));
                };
                let order = self
                    .with_journal(|journal| {
                        Ok(journal.get(client_order_id).map(|e| e.order.clone()))
                    })?
                    .ok_or_else(|| error("dust close order has no journaled intent"))?;
                let mut state = self
                    .state_from_detail(&remote, Some(client_order_id))
                    .await?;
                state.order = order;
                self.record_terminal(&state)?;
                Ok(Some(state))
            }
            None => Err(error(
                "reserved order is not found at the venue (a cancelled API order is only kept \
                 for two hours); it cannot be assumed unsubmitted — reconcile by fills",
            )),
        }
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        let live = |row: &Option<RemoteOrder>| {
            row.as_ref().is_some_and(|row| {
                map_status(row.order_status.as_deref().unwrap_or("")).is_ok_and(|s| s.is_live())
            })
        };
        if !live(&self.order_info(("orderId", venue_order_id)).await?) {
            // No longer live (filled/cancelled/unknown): cancellation is idempotent.
            return Ok(());
        }
        self.signed_data::<Value>(
            Method::POST,
            PATH_CANCEL,
            &[],
            Some(json!({ "orderId": venue_order_id, "category": CATEGORY })),
        )
        .await?;
        if live(&self.order_info(("orderId", venue_order_id)).await?) {
            return Err(error("order is still live after cancellation"));
        }
        Ok(())
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..HISTORY_MAX_PAGES {
            let mut params = vec![
                ("category", CATEGORY.to_string()),
                ("limit", HISTORY_PAGE.to_string()),
            ];
            if let Some(cursor) = &cursor {
                params.push(("cursor", cursor.clone()));
            }
            let page: Page<RemoteOrder> = self
                .signed_data(Method::GET, PATH_UNFILLED, &params, None)
                .await?;
            let full = page.list.len() >= HISTORY_PAGE;
            for row in page.list {
                if seen.insert(row.order_id.clone().unwrap_or_default()) {
                    rows.push(row);
                }
            }
            if !full {
                let mut states = Vec::with_capacity(rows.len());
                for row in &rows {
                    let state = self.state_from_pending(row)?;
                    if state.status.is_live() {
                        states.push(state);
                    }
                }
                return Ok(states);
            }
            cursor = Some(next_cursor(page.cursor, cursor.as_deref())?);
        }
        Err(error("open order list exceeds the reconciliation bound"))
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        positions_of(&self.position_rows().await?)
    }

    /// The USDT row's available balance in the unified account (never negative).
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        let assets: Assets = self
            .signed_data(Method::GET, PATH_ASSETS, &[], None)
            .await?;
        free_collateral_of(&assets).map(Some)
    }

    /// Funding actually settled on this symbol since `since` (financial records, every funding
    /// type, 30-day windows read whole and filtered here).
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let native = native_name(symbol);
        let now = now_ms() + self.time_offset_ms;
        let start = since.timestamp_millis();
        within_retention(start, now)?;
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        for window in windows(start, now.max(start), HISTORY_WINDOW_MS) {
            let records: Vec<FinancialRecord> =
                self.history(PATH_FINANCIAL_RECORDS, window).await?;
            for record in records {
                if let Some((id, at, amount)) = funding_entry(&record, &native, window)?
                    && seen.insert(id)
                {
                    rows.push((at, amount));
                }
            }
        }
        Ok(Some(FundingTotal::from_rows(rows, since)))
    }

    /// Every trade of this account on this symbol in `[since, until]` (fill history in 30-day
    /// windows, filtered here; every row checked to lie inside its window).
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
        within_retention(start, now_ms() + self.time_offset_ms)?;
        let native = native_name(symbol);
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        for window in windows(start, end, HISTORY_WINDOW_MS) {
            let page_rows: Vec<FillRow> = self.history(PATH_FILLS, window).await?;
            for row in page_rows {
                let at = millis(row.created_time.as_deref())?;
                if at < window.0 || at > window.1 {
                    return Err(error("a fill lies outside the requested window"));
                }
                if !same_symbol(row.symbol.as_deref(), &native) {
                    continue;
                }
                let id = row
                    .exec_id
                    .clone()
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| error("fill has no exec id"))?;
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

    /// `POST /api/v3/account/set-margin` (`operation=add`) into an existing isolated one-way
    /// position. At most one request, never retried: a resend adds twice.
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.authorize()?;
        let amount = margin_amount_text(amount_usdt)?;
        let _submission = self.submit.lock().await;
        let native = native_name(symbol);
        let pos_side = isolated_pos_side(&self.position_rows().await?, &native)?;
        let submitted = self
            .request_raw(
                true,
                Method::POST,
                PATH_SET_MARGIN,
                &[],
                Some(json!({
                    "category": CATEGORY,
                    "symbol": native,
                    "posSide": pos_side,
                    "operation": "add",
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
}

// ---- Structured response rows ------------------------------------------------------------

/// Bitget writes an empty list as `null` (seen live: `"list": null` with no position or fill).
fn null_as_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", bound(deserialize = "T: Deserialize<'de>"))]
struct Page<T> {
    #[serde(default = "Vec::new", deserialize_with = "null_as_empty")]
    list: Vec<T>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentRow {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    #[serde(default)]
    is_rwa: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    status: Option<String>,
    min_order_qty: Option<String>,
    max_order_qty: Option<String>,
    price_multiplier: Option<String>,
    price_precision: Option<String>,
    quantity_multiplier: Option<String>,
    quantity_precision: Option<String>,
    min_order_amount: Option<String>,
    min_leverage: Option<String>,
    max_leverage: Option<String>,
    #[serde(default)]
    buy_limit_price_ratio: Option<String>,
    #[serde(default)]
    sell_limit_price_ratio: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountSettings {
    account_mode: Option<String>,
    account_level: Option<String>,
    hold_mode: Option<String>,
    #[serde(default)]
    delta_switch: Option<String>,
    #[serde(default, deserialize_with = "null_as_empty")]
    symbol_config_list: Vec<SymbolConfig>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SymbolConfig {
    category: Option<String>,
    symbol: Option<String>,
    margin_mode: Option<String>,
    leverage: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderBook {
    #[serde(default)]
    a: Vec<Vec<Value>>,
    #[serde(default)]
    b: Vec<Vec<Value>>,
    ts: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    order_id: Option<String>,
    client_oid: Option<String>,
    symbol: Option<String>,
    price: Option<String>,
    qty: Option<String>,
    cum_exec_qty: Option<String>,
    cum_exec_value: Option<String>,
    avg_price: Option<String>,
    order_status: Option<String>,
    side: Option<String>,
    reduce_only: Option<String>,
    margin_mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillRow {
    exec_id: Option<String>,
    order_id: Option<String>,
    symbol: Option<String>,
    exec_price: Option<String>,
    exec_qty: Option<String>,
    side: Option<String>,
    #[serde(default, deserialize_with = "null_as_empty")]
    fee_detail: Vec<FeeDetail>,
    created_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeDetail {
    fee_coin: Option<String>,
    fee: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    symbol: Option<String>,
    pos_side: Option<String>,
    total: Option<String>,
    avg_price: Option<String>,
    mark_price: Option<String>,
    margin_mode: Option<String>,
    hold_mode: Option<String>,
    #[serde(default)]
    liquidation_price: Option<String>,
    #[serde(default)]
    position_balance: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Assets {
    #[serde(default, deserialize_with = "null_as_empty")]
    assets: Vec<AssetRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetRow {
    coin: Option<String>,
    available: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FinancialRecord {
    id: Option<String>,
    symbol: Option<String>,
    coin: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    amount: Option<String>,
    ts: Option<String>,
}

/// What a venue order row says, for matching it to our intent (or reconstructing an external one).
struct Intent {
    symbol: Symbol,
    side: Side,
    reduce_only: bool,
    units: Decimal,
    limit: Option<Decimal>,
    filled_notional: Decimal,
    margin_mode: crate::MarginMode,
}

// ---- Pure helpers ------------------------------------------------------------------------

fn wire_mode(mode: crate::MarginMode) -> &'static str {
    if mode.is_cross() {
        "crossed"
    } else {
        "isolated"
    }
}

/// An external order's margin mode; unknown ones are taken as isolated (the bot's default).
fn reported_mode(raw: Option<&str>) -> crate::MarginMode {
    raw.and_then(crate::margin::reported_mode)
        .unwrap_or(crate::MarginMode::Isolated)
}

fn same_symbol(raw: Option<&str>, native: &str) -> bool {
    raw.is_some_and(|symbol| symbol.eq_ignore_ascii_case(native))
}

/// The v3 order body: always LIMIT + IOC, always the margin mode of the order (an order without
/// it would open a cross position), reduce-only as `yes` / `no`.
fn order_body(
    native: &str,
    order: &NewOrder,
    units: Decimal,
    price: Decimal,
    client_oid: &str,
) -> Value {
    json!({
        "category": CATEGORY,
        "symbol": native,
        "qty": units.normalize().to_string(),
        "price": price.normalize().to_string(),
        "side": if order.side == Side::Buy { "buy" } else { "sell" },
        "orderType": "limit",
        "timeInForce": "ioc",
        "clientOid": client_oid,
        "reduceOnly": if order.reduce_only { "yes" } else { "no" },
        "marginMode": wire_mode(order.margin_mode),
    })
}

/// The account must be unified (not mid-upgrade or switching back) and one-way.
fn check_account(settings: &AccountSettings) -> ArbResult<()> {
    match settings.account_mode.as_deref() {
        Some("unified") => {}
        Some(other) => {
            return Err(error(format!(
                "Bitget account mode is '{other}', not 'unified' (an upgrade or switch may still \
                 be in progress); refusing to trade"
            )));
        }
        None => return Err(error("Bitget account mode could not be read")),
    }
    match settings.hold_mode.as_deref() {
        Some("one_way_mode") => {}
        Some(other) => {
            return Err(error(format!(
                "Bitget account is in '{other}' holding mode; this broker requires one-way mode \
                 (change it in the Bitget UI before trading)"
            )));
        }
        None => return Err(error("Bitget holding mode could not be read")),
    }
    // Delta-neutral is now advanced mode with `deltaSwitch` on; `delta` is the older spelling.
    if settings.account_level.as_deref() == Some("delta")
        || settings.delta_switch.as_deref() == Some("yes")
    {
        return Err(error(
            "Bitget account is in delta-neutral mode; use basic or isolated margin mode",
        ));
    }
    Ok(())
}

/// The margin mode in force when the account's symbol configuration already shows exactly
/// `leverage` for `native` (`None`: configure it).
fn mode_in_force(
    settings: &AccountSettings,
    native: &str,
    leverage: Decimal,
) -> ArbResult<Option<crate::MarginMode>> {
    let Some(row) = settings.symbol_config_list.iter().find(|row| {
        row.category.as_deref().is_none_or(|c| c == CATEGORY)
            && same_symbol(row.symbol.as_deref(), native)
    }) else {
        return Ok(None);
    };
    let Some(mode) = row
        .margin_mode
        .as_deref()
        .and_then(crate::margin::reported_mode)
    else {
        return Ok(None);
    };
    Ok((opt_dec(row.leverage.as_deref())? == Some(leverage)).then_some(mode))
}

/// Why this contract cannot take an order right now (`None` = it can). `closing`: only a
/// reduce-only order is wanted. `limit_close` halts both ways: a leg that could be opened but
/// not closed is worse than no leg.
fn status_halt(status: Option<&str>, closing: bool) -> Option<String> {
    match status {
        Some("online") => None,
        Some("limit_open") if closing => None,
        Some("limit_open") => Some("Bitget 这个合约现在只能平仓、不能开仓（limit_open）".into()),
        Some("limit_close") => {
            Some("Bitget 这个合约现在不让平仓（limit_close），开平都不做".into())
        }
        Some("offline") => Some("Bitget 这个合约下线 / 维护中（offline）".into()),
        Some("restrictedAPI") => Some("Bitget 这个合约现在不接受 API 下单（restrictedAPI）".into()),
        Some(other) => Some(format!("Bitget 这个合约现在不在交易（{other}）")),
        None => Some("Bitget 没报这个合约的交易状态".into()),
    }
}

/// Select the single tradable USDT perpetual for `symbol` (`closing` also accepts `limit_open`).
fn select_instrument(
    rows: &[InstrumentRow],
    symbol: &Symbol,
    closing: bool,
) -> ArbResult<Instrument> {
    let native = native_name(symbol);
    let mut found = None;
    for row in rows {
        let status = row.status.as_deref();
        if row.quote_coin.eq_ignore_ascii_case(MARGIN_COIN)
            && row.symbol.eq_ignore_ascii_case(&native)
            && row.base_coin.eq_ignore_ascii_case(&symbol.base)
            && row.kind.as_deref() == Some("perpetual")
            && (status == Some("online") || closing && status == Some("limit_open"))
        {
            if found.is_some() {
                return Err(error(format!(
                    "ambiguous USDT perpetual instrument {native}"
                )));
            }
            found = Some(parse_instrument(row)?);
        }
    }
    found.ok_or_else(|| error(format!("unknown or untradable USDT perpetual {native}")))
}

fn parse_instrument(row: &InstrumentRow) -> ArbResult<Instrument> {
    let tick = dec(row.price_multiplier.as_deref().unwrap_or(""))?;
    let step = dec(row.quantity_multiplier.as_deref().unwrap_or(""))?;
    if tick <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(error("instrument reports a non-positive tick or step"));
    }
    // The multipliers must fit the documented decimal places.
    let fits = |value: Decimal, places: Option<&str>| {
        places
            .and_then(|raw| u32::from_str(raw.trim()).ok())
            .is_none_or(|places| value.normalize().scale() <= places)
    };
    if !fits(tick, row.price_precision.as_deref()) || !fits(step, row.quantity_precision.as_deref())
    {
        return Err(error(
            "instrument tick or step exceeds its documented precision",
        ));
    }
    Ok(Instrument {
        native: row.symbol.to_ascii_uppercase(),
        // v3 文档写的是 `YES`；大小写不敏感，免得写法一变周末 / 假日拦截就悄悄失效。
        rwa: row
            .is_rwa
            .as_deref()
            .is_some_and(|rwa| rwa.eq_ignore_ascii_case("yes")),
        tick,
        step,
        min_qty: opt_dec(row.min_order_qty.as_deref())?.unwrap_or(step),
        min_notional: opt_dec(row.min_order_amount.as_deref())?.unwrap_or(Decimal::ZERO),
        max_qty: opt_dec(row.max_order_qty.as_deref())?.filter(|value| *value > Decimal::ZERO),
        min_lever: opt_dec(row.min_leverage.as_deref())?
            .filter(|value| *value > Decimal::ZERO)
            .unwrap_or(Decimal::ONE),
        max_lever: dec(row.max_leverage.as_deref().unwrap_or(""))?,
        limit_ratio: limit_ratio(
            row.buy_limit_price_ratio.as_deref(),
            row.sell_limit_price_ratio.as_deref(),
        )?,
    })
}

/// 只减仓单低于最小下单额时怎么办。
#[derive(Debug, PartialEq, Eq)]
enum DustRule {
    /// 不低于下限：照常下单。
    Place,
    /// 正好平掉整笔持仓：改用 close-positions。
    CloseWhole,
    /// 只平一部分又低于下限：拒（平完会留下更小的零头）。
    Refuse,
}

fn dust_rule(below_minimum: bool, units: Decimal, net: Decimal) -> DustRule {
    if !below_minimum {
        DustRule::Place
    } else if units == net.abs() {
        DustRule::CloseWhole
    } else {
        DustRule::Refuse
    }
}

/// close-positions 的回包。外层 `00000` 不代表成功：每个持仓的结果在 `data.list` 里各带 code，
/// 文档示例就是外层成功、里面一条 `24056`。
#[derive(Debug, PartialEq, Eq)]
enum DustReply {
    Placed(String),
    Refused(String, String),
    Unknown,
}

fn dust_reply(status: StatusCode, envelope: &Value) -> DustReply {
    let code = envelope_code(envelope);
    let message = || {
        envelope
            .get("msg")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    if !(status.is_success() && code == SUCCESS) {
        return if definitive_refusal(status, code) {
            DustReply::Refused(code.to_string(), message())
        } else {
            DustReply::Unknown
        };
    }
    let Some(item) = envelope
        .pointer("/data/list")
        .and_then(Value::as_array)
        .and_then(|list| list.first())
    else {
        return DustReply::Unknown;
    };
    let text = |key: &str| {
        item.get(key)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let item_code = text("code");
    if !matches!(item_code.as_str(), "" | SUCCESS) {
        return if AMBIGUOUS_CODES.contains(&item_code.as_str()) {
            DustReply::Unknown
        } else {
            DustReply::Refused(item_code, text("msg"))
        };
    }
    match text("orderId") {
        id if id.is_empty() => DustReply::Unknown,
        id => DustReply::Placed(id),
    }
}

fn map_status(status: &str) -> ArbResult<OrderStatus> {
    match status {
        "live" | "new" | "partially_filled" => Ok(OrderStatus::Open),
        "filled" => Ok(OrderStatus::Filled),
        "cancelled" | "canceled" => Ok(OrderStatus::Cancelled),
        _ => Err(error(
            "unknown venue order status; refusing to infer terminality",
        )),
    }
}

/// A refusal the venue made before anything was placed: HTTP 429 (rate limited at the gateway),
/// or a business code that is neither success nor one of the "outcome unknown" codes and not a
/// server error. Only consulted after the venue's own record shows no such order.
///
/// 429 以前被排除在外：订单号从此永远「结果不明、不得重发」，做第二腿时第一腿就一直裸着。经典账户
/// 一直把 429 记成拒单，这里与它一致。
fn definitive_refusal(status: StatusCode, code: &str) -> bool {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return true;
    }
    !status.is_server_error() && !matches!(code, SUCCESS | "") && !AMBIGUOUS_CODES.contains(&code)
}

/// A margin top-up reply: confirmed, refused by a business code (nothing moved), or unknown.
fn classify_margin_reply(status: StatusCode, envelope: &Value) -> MarginOutcome {
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
    } else if definitive_refusal(status, code) {
        MarginOutcome::Refused(message())
    } else {
        MarginOutcome::Unknown(message())
    }
}

/// The next page's cursor: present and moved on, or the history cannot be read completely.
fn next_cursor(next: Option<String>, current: Option<&str>) -> ArbResult<String> {
    match next {
        Some(next) if !next.is_empty() && Some(next.as_str()) != current => Ok(next),
        _ => Err(error("pagination did not advance; refusing a partial list")),
    }
}

/// Fees as a positive cost (every order is taker IOC); any non-USDT fee is refused.
fn fee_cost(details: &[FeeDetail]) -> ArbResult<Decimal> {
    let mut fee = Decimal::ZERO;
    for detail in details {
        if normalized(detail.fee_coin.as_deref()).as_deref() != Some(MARGIN_COIN) {
            return Err(error(
                "fill fee is not denominated in USDT; disable the BGB fee deduction",
            ));
        }
        fee = add(fee, dec(detail.fee.as_deref().unwrap_or("0"))?.abs())?;
    }
    Ok(fee)
}

/// (quantity, notional, fee) of one order's fills, each checked to belong to it.
fn aggregate_fills(
    fills: &[FillRow],
    order: &RemoteOrder,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let (mut quantity, mut notional, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    let mut seen = HashSet::new();
    for fill in fills {
        if fill.order_id.as_deref() != order.order_id.as_deref() {
            return Err(error("fill belongs to a different order"));
        }
        if normalized(fill.symbol.as_deref()) != normalized(order.symbol.as_deref()) {
            return Err(error("fill symbol does not match the order"));
        }
        if normalized(fill.side.as_deref()) != normalized(order.side.as_deref()) {
            return Err(error("fill side does not match the order"));
        }
        let id = fill
            .exec_id
            .as_deref()
            .ok_or_else(|| error("fill has no exec id"))?;
        if !seen.insert(id) {
            return Err(error("fill appears twice; fee history is not reliable"));
        }
        let base = dec(fill.exec_qty.as_deref().unwrap_or(""))?;
        let price = dec(fill.exec_price.as_deref().unwrap_or(""))?;
        quantity = add(quantity, base)?;
        notional = add(notional, multiply(base, price)?)?;
        fee = add(fee, fee_cost(&fill.fee_detail)?)?;
    }
    Ok((quantity, notional, fee))
}

/// Fill-history rows as venue fills, oldest first.
fn venue_fills(rows: Vec<FillRow>) -> ArbResult<Vec<VenueFill>> {
    let mut timed = Vec::with_capacity(rows.len());
    for row in rows {
        timed.push((millis(row.created_time.as_deref())?, row));
    }
    timed.sort_by(|a, b| (a.0, &a.1.exec_id).cmp(&(b.0, &b.1.exec_id)));
    let mut fills = Vec::with_capacity(timed.len());
    for (at, row) in timed {
        let quantity = dec(row.exec_qty.as_deref().unwrap_or(""))?;
        let price = dec(row.exec_price.as_deref().unwrap_or(""))?;
        if quantity <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(error("a fill has a non-positive quantity or price"));
        }
        fills.push(VenueFill {
            at: utc_millis(at)?,
            side: parse_side(&row.side.as_deref().unwrap_or("").to_ascii_lowercase())?,
            quantity,
            price,
            fee_usdt: fee_cost(&row.fee_detail)?,
            effect: FillEffect::Unknown,
        });
    }
    Ok(fills)
}

/// A funding record of `native` inside `window` as (id, time, signed USDT; positive =
/// received). Records of other types or symbols are skipped; anything malformed is an error.
fn funding_entry(
    record: &FinancialRecord,
    native: &str,
    (from, until): (i64, i64),
) -> ArbResult<Option<(String, DateTime<Utc>, Decimal)>> {
    let Some(kind) = record.kind.as_deref().filter(|k| FUNDING_TYPES.contains(k)) else {
        return Ok(None);
    };
    if !same_symbol(record.symbol.as_deref(), native) {
        return Ok(None);
    }
    let at = millis(record.ts.as_deref())?;
    if at < from || at > until {
        return Err(error(
            "a financial record lies outside the requested window",
        ));
    }
    if normalized(record.coin.as_deref()).as_deref() != Some(MARGIN_COIN) {
        return Err(error("a funding record is not denominated in USDT"));
    }
    let id = record
        .id
        .clone()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| error("a funding record has no id"))?;
    let amount = dec(record.amount.as_deref().unwrap_or(""))?.abs();
    let signed = if kind.ends_with("_OUT") {
        -amount
    } else {
        amount
    };
    Ok(Some((id, utc_millis(at)?, signed)))
}

/// The USDT row's available balance (none or negative = nothing available).
fn free_collateral_of(assets: &Assets) -> ArbResult<Decimal> {
    match assets
        .assets
        .iter()
        .find(|row| normalized(row.coin.as_deref()).as_deref() == Some(MARGIN_COIN))
    {
        Some(row) => Ok(opt_dec(row.available.as_deref())?
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO)),
        None => Ok(Decimal::ZERO),
    }
}

/// Net positions, one per symbol; hedge-mode or two-sided rows are refused.
fn positions_of(rows: &[PositionRow]) -> ArbResult<Vec<VenuePosition>> {
    let mut sides: HashMap<String, (bool, bool)> = HashMap::new();
    let mut positions = Vec::new();
    for row in rows {
        let total = nonneg(row.total.as_deref())?;
        if total.is_zero() {
            continue;
        }
        if row.hold_mode.as_deref() == Some("hedge_mode") {
            return Err(error(
                "hedge-mode position found; this broker requires one-way positions",
            ));
        }
        let symbol = symbol_from_native(row.symbol.as_deref().unwrap_or(""))?;
        let sign = match row.pos_side.as_deref() {
            Some("long") => Decimal::ONE,
            Some("short") => -Decimal::ONE,
            _ => return Err(error("unknown position direction")),
        };
        let entry = sides.entry(symbol.base.clone()).or_insert((false, false));
        if sign > Decimal::ZERO {
            entry.0 = true;
        } else {
            entry.1 = true;
        }
        if entry.0 && entry.1 {
            return Err(error(
                "two-sided position found for a symbol; refusing ambiguous net exposure",
            ));
        }
        let mark = dec(row.mark_price.as_deref().unwrap_or(""))?;
        positions.push(VenuePosition {
            venue: Venue::Bitget,
            symbol,
            net_quantity: total * sign,
            average_price: opt_dec(row.avg_price.as_deref())?,
            notional_usdt: multiply(total, mark)?,
        });
    }
    Ok(positions)
}

/// The side (`long` / `short`) of the open, isolated, one-way position on `native`.
fn isolated_pos_side(rows: &[PositionRow], native: &str) -> ArbResult<&'static str> {
    let row = rows
        .iter()
        .find(|row| {
            same_symbol(row.symbol.as_deref(), native)
                && nonneg(row.total.as_deref()).is_ok_and(|total| total > Decimal::ZERO)
        })
        .ok_or_else(|| error("no open position on this symbol to add margin to"))?;
    if row.hold_mode.as_deref() == Some("hedge_mode") {
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
    match row.pos_side.as_deref() {
        Some("long") => Ok("long"),
        Some("short") => Ok("short"),
        _ => Err(error("unknown position direction")),
    }
}

#[cfg(test)]
mod tests;
