//! Gate.io v4 **USDT-margined linear perpetual** broker (`settle=usdt`).
//!
//! Construction is explicit: [`GateBroker::connect`] takes the API key/secret, the
//! exclusively locked intent journal and [`LiveOptions`]. With the default
//! [`LiveOptions`] every signed write (order, cancel, leverage) is refused before
//! anything is signed; only the read-only account probes run.
//!
//! # Account requirements (operator must configure these; we never change them)
//!
//! - **One-way position mode** (`position_mode == "single"`). In dual/hedge mode a
//!   contract can hold two opposing positions and "net quantity" is meaningless, so
//!   the constructor fails with an actionable message instead of guessing.
//! - **Not a portfolio-margin / unified account** (`enable_credit != true`). Those
//!   account-wide margin modes change what per-contract isolated leverage means.
//!
//! # Semantics we rely on
//!
//! - `size` is a **signed integer contract count** (positive buy, negative sell);
//!   one contract is `quanto_multiplier` units of the base asset, so that field is
//!   the `unit` passed to [`order_units`]. `order_size_min`/`order_size_max` bound
//!   the signed magnitude, `order_price_round` is the price tick.
//! - Every order is **LIMIT + `tif="ioc"`** (never an unbounded market order).
//!   `price="0"` combined with `tif=ioc` is Gate's market order; we always send a
//!   concrete bounded price.
//! - Per-contract isolated leverage is set with `POST
//!   /futures/{settle}/positions/{contract}/leverage?leverage=N` where a positive
//!   `leverage` means isolated and `0` means cross. We read the position back and
//!   confirm before sending the order; account-wide settings are never touched.
//! - Fees come from the fills endpoint `GET /futures/{settle}/my_trades` filtered
//!   by order id. The `fee` field is a *deduction* in the settlement currency
//!   (positive = cost, negative = maker rebate), so it is used as `fee_usdt`
//!   directly.
//!
//! # Protocol sources (signature and field semantics verified against these)
//!
//! - Signing: <https://www.gate.com/docs/developers/apiv4/en/#apiv4-signed-request-requirements>
//!   and <https://www.gate.com/docs/developers/apiv4/en/#api-signature-string-generation>
//!   (worked example reproduced in the unit tests).
//! - Futures order create: <https://github.com/gateio/gateapi-python/blob/master/docs/FuturesOrder.md>
//!   and <https://github.com/gateio/gateapi-python/blob/master/docs/FuturesApi.md>.
//! - Contract metadata: <https://github.com/gateio/gateapi-python/blob/master/docs/Contract.md>;
//!   position shape: <https://github.com/gateio/gateapi-python/blob/master/docs/Position.md>;
//!   fills: <https://github.com/gateio/gateapi-python/blob/master/docs/MyFuturesTrade.md>;
//!   account/fee: <https://github.com/gateio/gateapi-python/blob/master/docs/FuturesAccount.md>,
//!   <https://github.com/gateio/gateapi-python/blob/master/docs/TradeFee.md>.
//! - Fee sign ("Fee deducted"): <https://www.gate.com/docs/developers/futures/ws/en/#user-trades-notification>.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method, StatusCode};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, hex_lower, order_units, round_price, transport_error,
    venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const VENUE: Venue = Venue::Gate;
const HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";
/// Gate keeps an independent futures account per settlement currency; this broker is
/// USDT-only, so every path is pinned to this value.
const SETTLE: &str = "usdt";

/// Gate APIv4 key pair. No `Debug`/`Serialize`: secrets must never be formatted.
pub struct GateCredentials {
    pub api_key: String,
    pub api_secret: String,
}

/// Gate futures broker. The API secret lives only inside `credentials` and is used
/// solely by the signature routine.
pub struct GateBroker {
    client: Client,
    credentials: GateCredentials,
    options: LiveOptions,
    journal: Mutex<OrderJournal>,
    /// Account taker fee rate, read from `GET /wallet/fee` at connect.
    taker_fee: Decimal,
    /// `server_time - local_time` in ms, read from `GET /spot/time` at connect.
    /// The `Timestamp` header has a 60s window; a skewed clock must not break it.
    time_offset_ms: i64,
    /// Contract metadata cache keyed by native contract name (`BTC_USDT`).
    specs: Mutex<HashMap<String, ContractSpec>>,
}

#[derive(Debug, Clone)]
struct ContractSpec {
    name: String,
    kind: String,
    status: String,
    in_delisting: bool,
    /// Base-asset quantity per contract; the `unit` for [`order_units`].
    multiplier: Decimal,
    /// Minimum order price increment.
    price_tick: Decimal,
    /// Minimum order size, in contracts.
    size_min: Decimal,
    /// Maximum order size, in contracts.
    size_max: Decimal,
    leverage_min: Decimal,
    leverage_max: Decimal,
    mark_price: Decimal,
}

fn err(message: impl Into<String>) -> ArbError {
    ArbError::venue(VENUE.as_str(), message)
}

fn json_error() -> ArbError {
    err("响应无法解析")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `HexEncode(SHA512(bytes))`.
fn sha512_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha512::new();
    hasher.update(bytes);
    hex_lower(&hasher.finalize())
}

/// `HexEncode(HMAC_SHA512(secret, message))`.
fn hmac_sha512_hex(secret: &[u8], message: &[u8]) -> String {
    let mut mac = Hmac::<Sha512>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(message);
    hex_lower(&mac.finalize().into_bytes())
}

/// Gate APIv4 signature string:
///
/// ```text
/// METHOD \n path \n query \n HexEncode(SHA512(body)) \n timestamp
/// ```
///
/// `path` includes the `/api/v4` prefix (host and port are excluded). `query` is the
/// raw query string exactly as concatenated into the request URL, `""` when absent.
fn gate_signature(
    secret: &[u8],
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    timestamp: &str,
) -> String {
    let payload = format!(
        "{method}\n{path}\n{query}\n{}\n{timestamp}",
        sha512_hex(body)
    );
    hmac_sha512_hex(secret, payload.as_bytes())
}

fn decimal(value: &str) -> ArbResult<Decimal> {
    Decimal::from_str_exact(value).map_err(|_| err("Gate 返回了非法小数"))
}

fn positive(field: Option<String>, name: &str) -> ArbResult<Decimal> {
    let raw = field.ok_or_else(|| err(format!("合约缺少 {name}")))?;
    let value = decimal(&raw)?;
    if value <= Decimal::ZERO {
        return Err(err(format!("合约的 {name} 非正")));
    }
    Ok(value)
}

/// Signed integer contract count for a buy/sell of `units` contracts.
fn signed_contracts(units: Decimal, side: Side) -> ArbResult<i64> {
    if units <= Decimal::ZERO || units.fract() != Decimal::ZERO {
        return Err(err("下单张数必须是正整数"));
    }
    let magnitude = units.to_i64().ok_or_else(|| err("下单张数超出范围"))?;
    Ok(match side {
        Side::Buy => magnitude,
        Side::Sell => -magnitude,
    })
}

/// Map a Gate `status`/`finish_as` pair to our order status. Unknown strings are an
/// error: inferring a terminal state from an unrecognized one would silently hide a
/// live order or a partial fill.
fn map_status(
    status: &str,
    finish_as: Option<&str>,
    executed: i64,
    total: i64,
) -> ArbResult<OrderStatus> {
    match status {
        "open" => Ok(OrderStatus::Open),
        "finished" => {
            let reason = finish_as.ok_or_else(|| err("已结束订单缺少 finish_as"))?;
            match reason {
                "filled" => {
                    if executed != total {
                        Err(err("finish_as=filled 但成交量不完整"))
                    } else {
                        Ok(OrderStatus::Filled)
                    }
                }
                "cancelled" | "ioc" | "reduce_only" | "position_closed" | "reduce_out" | "stp"
                | "liquidated" | "auto_deleveraged" => Ok(OrderStatus::Cancelled),
                other => Err(err(format!("未知的 finish_as: {other}"))),
            }
        }
        other => Err(err(format!("未知的订单状态: {other}"))),
    }
}

/// A contract is usable for a scan symbol only when it is a currently trading,
/// non-delisting, direct (linear) USDT perpetual whose native name maps back to
/// exactly that symbol.
fn usable(spec: &ContractSpec, symbol: &Symbol) -> bool {
    spec.kind == "direct"
        && spec.status == "trading"
        && !spec.in_delisting
        && spec.multiplier > Decimal::ZERO
        && symbol_of(&spec.name).is_ok_and(|mapped| mapped == *symbol)
}

fn symbol_of(name: &str) -> ArbResult<Symbol> {
    name.strip_suffix("_USDT")
        .filter(|base| !base.is_empty())
        .map(|base| Symbol::perp(base, "USDT"))
        .ok_or_else(|| err("合约名不是 USDT 线性永续"))
}

/// One-way mode and a classic (non-portfolio) margin account are required.
fn ensure_account_mode(
    position_mode: Option<&str>,
    in_dual_mode: Option<bool>,
    enable_credit: Option<bool>,
) -> ArbResult<()> {
    if enable_credit == Some(true) {
        return Err(err(
            "该账户是组合保证金/统一账户（enable_credit=true），逐合约逐仓杠杆语义不同；请在网页端切换到经典保证金模式后再启动",
        ));
    }
    match position_mode {
        Some("single") => Ok(()),
        Some("dual") | Some("split") => Err(err(
            "该账户处于双向/对冲持仓模式，无法用净持仓对账；请在网页端切换为单向持仓模式（本程序不会替你修改账户设置）",
        )),
        Some(other) => Err(err(format!("未知的 position_mode: {other}"))),
        None => {
            if in_dual_mode == Some(false) {
                Ok(())
            } else {
                Err(err(
                    "无法确认账户是单向持仓模式（position_mode 与 in_dual_mode 都缺失或不一致）",
                ))
            }
        }
    }
}

/// Aggregate fills for one order: `(base_quantity, quote_value, fee)`.
///
/// The fee is a Gate deduction in the settlement currency, so positive means cost
/// and a negative maker rebate stays negative. A non-USDT fee currency, a fill that
/// quantity all fail closed. So does a trade id seen twice (pagination overlap) or a trade
/// from another contract: either would double-count or misprice the fee.
fn aggregate_fills(
    trades: &[WireTrade],
    order_id: &str,
    contract: &str,
    multiplier: Decimal,
    executed_contracts: i64,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let mut contracts = Decimal::ZERO;
    let mut base = Decimal::ZERO;
    let mut quote = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut seen = std::collections::HashSet::new();
    for trade in trades {
        if trade.order_id.as_deref() != Some(order_id) {
            return Err(err("成交记录不属于该订单"));
        }
        if trade
            .contract
            .as_deref()
            .is_some_and(|name| name != contract)
        {
            return Err(err("成交记录属于另一个合约"));
        }
        let id = trade
            .id
            .as_deref()
            .ok_or_else(|| err("成交记录缺少成交 id"))?;
        if !seen.insert(id) {
            return Err(err("成交记录重复出现，手续费无法可靠汇总"));
        }
        let size = trade.size.ok_or_else(|| err("成交记录缺少数量"))?;
        let price = trade
            .price
            .as_deref()
            .ok_or_else(|| err("成交记录缺少价格"))
            .and_then(decimal)?;
        if price <= Decimal::ZERO {
            return Err(err("成交价非正"));
        }
        if let Some(currency) = &trade.fee_currency
            && !currency.eq_ignore_ascii_case("USDT")
        {
            return Err(err("成交记录的手续费币种不是 USDT"));
        }
        let fee_amount = match &trade.fee {
            Some(value) => decimal(value)?,
            None => return Err(err("成交记录缺少手续费")),
        };
        let magnitude = Decimal::from(size.abs());
        contracts += magnitude;
        base += magnitude * multiplier;
        quote += magnitude * multiplier * price;
        fee += fee_amount;
    }
    if contracts != Decimal::from(executed_contracts) {
        return Err(err("成交记录数量与订单成交量不一致"));
    }
    Ok((base, quote, fee))
}

#[derive(Serialize)]
struct OrderRequest<'a> {
    contract: &'a str,
    size: i64,
    price: String,
    tif: &'static str,
    text: &'a str,
    reduce_only: bool,
    close: bool,
}

#[derive(Deserialize)]
struct WireError {
    #[serde(default)]
    label: String,
    #[serde(default)]
    message: String,
}

#[derive(Deserialize)]
struct WireTime {
    #[serde(default)]
    server_time: Option<i64>,
}

#[derive(Deserialize)]
struct WireContract {
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    in_delisting: Option<bool>,
    #[serde(default)]
    quanto_multiplier: Option<String>,
    #[serde(default)]
    order_price_round: Option<String>,
    #[serde(default)]
    order_size_min: Option<i64>,
    #[serde(default)]
    order_size_max: Option<i64>,
    #[serde(default)]
    leverage_min: Option<String>,
    #[serde(default)]
    leverage_max: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    last_price: Option<String>,
}

#[derive(Deserialize)]
struct WireLevel {
    #[serde(default)]
    p: Option<String>,
}

#[derive(Deserialize)]
struct WireBook {
    #[serde(default)]
    bids: Vec<WireLevel>,
    #[serde(default)]
    asks: Vec<WireLevel>,
}

#[derive(Deserialize)]
struct WireAccount {
    #[serde(default)]
    in_dual_mode: Option<bool>,
    #[serde(default)]
    position_mode: Option<String>,
    #[serde(default)]
    enable_credit: Option<bool>,
}

#[derive(Deserialize)]
struct WireFee {
    #[serde(default)]
    futures_taker_fee: Option<String>,
}

#[derive(Deserialize)]
struct WirePosition {
    #[serde(default)]
    contract: Option<String>,
    #[serde(default, deserialize_with = "flex_i64")]
    size: Option<i64>,
    #[serde(default)]
    leverage: Option<String>,
    #[serde(default)]
    cross_leverage_limit: Option<String>,
    #[serde(default)]
    liq_price: Option<String>,
    #[serde(default)]
    margin: Option<String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    entry_price: Option<String>,
    #[serde(default)]
    value: Option<String>,
}

#[derive(Deserialize)]
struct WireOrder {
    #[serde(default, deserialize_with = "flex_string")]
    id: Option<String>,
    #[serde(default)]
    contract: Option<String>,
    #[serde(default, deserialize_with = "flex_i64")]
    size: Option<i64>,
    #[serde(default, deserialize_with = "flex_i64")]
    left: Option<i64>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    fill_price: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    finish_as: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    reduce_only: Option<bool>,
    #[serde(default)]
    is_reduce_only: Option<bool>,
}

#[derive(Deserialize)]
struct WireTrade {
    #[serde(default, deserialize_with = "flex_string")]
    id: Option<String>,
    #[serde(default, deserialize_with = "flex_string")]
    order_id: Option<String>,
    #[serde(default)]
    contract: Option<String>,
    #[serde(default, deserialize_with = "flex_i64")]
    size: Option<i64>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    fee: Option<String>,
    #[serde(default)]
    fee_currency: Option<String>,
}

/// Accept a string or number for a scalar field and normalize it to `String`.
fn flex_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(Value::String(text)) => Some(text),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(flag)) => Some(flag.to_string()),
        _ => None,
    })
}

/// Accept a number or numeric string for a scalar field and normalize it to `i64`.
fn flex_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(Value::Number(number)) => number.as_i64(),
        Some(Value::String(text)) => text.parse::<i64>().ok(),
        _ => None,
    })
}

fn spec_from(wire: WireContract) -> ArbResult<ContractSpec> {
    let name = wire.name.ok_or_else(|| err("合约缺少名称"))?;
    let multiplier = positive(wire.quanto_multiplier, "quanto_multiplier")?;
    let price_tick = positive(wire.order_price_round, "order_price_round")?;
    let size_min = wire
        .order_size_min
        .filter(|value| *value > 0)
        .ok_or_else(|| err("合约缺少 order_size_min"))?;
    let size_max = wire
        .order_size_max
        .filter(|value| *value >= size_min)
        .ok_or_else(|| err("合约缺少合法的 order_size_max"))?;
    let leverage_max = positive(wire.leverage_max, "leverage_max")?;
    let leverage_min = wire
        .leverage_min
        .as_deref()
        .and_then(|raw| Decimal::from_str_exact(raw).ok())
        .filter(|value| *value > Decimal::ZERO)
        .unwrap_or(Decimal::ONE);
    let mark_price = wire
        .mark_price
        .as_deref()
        .and_then(|raw| Decimal::from_str_exact(raw).ok())
        .filter(|value| *value > Decimal::ZERO)
        .or_else(|| {
            wire.last_price
                .as_deref()
                .and_then(|raw| Decimal::from_str_exact(raw).ok())
                .filter(|value| *value > Decimal::ZERO)
        })
        .unwrap_or(Decimal::ZERO);
    Ok(ContractSpec {
        name,
        kind: wire.kind.unwrap_or_default(),
        status: wire.status.unwrap_or_default(),
        in_delisting: wire.in_delisting.unwrap_or(false),
        multiplier,
        price_tick,
        size_min: Decimal::from(size_min),
        size_max: Decimal::from(size_max),
        leverage_min,
        leverage_max,
        mark_price,
    })
}

impl GateBroker {
    /// Open the journal and verify the account read-only before the first order.
    pub async fn connect(
        client: Client,
        credentials: GateCredentials,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(VENUE)?;
        let identity = format!(
            "gate:{}",
            &hex_lower(&Sha256::digest(credentials.api_key.as_bytes()))[..16]
        );
        let journal = OrderJournal::open(journal_path, &identity)?;
        let mut broker = Self {
            client,
            credentials,
            options,
            journal: Mutex::new(journal),
            taker_fee: Decimal::ZERO,
            time_offset_ms: 0,
            specs: Mutex::new(HashMap::new()),
        };
        // The Timestamp header is only valid within 60s of server time.
        if let Some(offset) = broker.fetch_time_offset().await {
            broker.time_offset_ms = offset;
        }
        let accounts_path = format!("/futures/{SETTLE}/accounts");
        let account: WireAccount = broker.auth_get(&accounts_path, "").await?;
        ensure_account_mode(
            account.position_mode.as_deref(),
            account.in_dual_mode,
            account.enable_credit,
        )?;
        let fee_path = "/wallet/fee";
        let fee: WireFee = broker.auth_get(fee_path, "").await?;
        let taker = fee
            .futures_taker_fee
            .as_deref()
            .ok_or_else(|| err("wallet/fee 未返回 futures_taker_fee，无法确定吃单费率"))?;
        let taker = decimal(taker)?;
        if taker < Decimal::ZERO {
            return Err(err("账户吃单费率为负"));
        }
        broker.taker_fee = taker;
        Ok(broker)
    }

    fn timestamp(&self) -> String {
        (now_ms().saturating_add(self.time_offset_ms))
            .div_euclid(1000)
            .to_string()
    }

    async fn fetch_time_offset(&self) -> Option<i64> {
        let (status, bytes) = self
            .send(Method::GET, "/spot/time", "", None, false)
            .await
            .ok()?;
        if !status.is_success() {
            return None;
        }
        let wire: WireTime = serde_json::from_slice(&bytes).ok()?;
        Some(wire.server_time? - now_ms())
    }

    /// Perform one HTTP call. Only transport failures and unreadable bodies are
    /// `Err`; a non-2xx status is returned so callers can inspect the business code.
    /// `transport_error` strips the URL, which is never echoed here.
    async fn send(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: Option<Vec<u8>>,
        auth: bool,
    ) -> ArbResult<(StatusCode, Vec<u8>)> {
        let method_name = method.as_str().to_string();
        let url = if query.is_empty() {
            format!("{HOST}{API_PREFIX}{path}")
        } else {
            format!("{HOST}{API_PREFIX}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method, &url)
            .timeout(Duration::from_secs(15));
        if auth {
            let timestamp = self.timestamp();
            let sign = gate_signature(
                self.credentials.api_secret.as_bytes(),
                &method_name,
                &format!("{API_PREFIX}{path}"),
                query,
                body.as_deref().unwrap_or(&[]),
                &timestamp,
            );
            request = request
                .header("KEY", self.credentials.api_key.as_str())
                .header("Timestamp", timestamp)
                .header("SIGN", sign);
        }
        if let Some(bytes) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(bytes);
        }
        let response = request
            .send()
            .await
            .map_err(|error| transport_error(VENUE, path, error))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| err(format!("{path}: 响应体读取失败")))?
            .to_vec();
        Ok((status, bytes))
    }

    async fn auth_get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &str,
    ) -> ArbResult<T> {
        let (status, bytes) = self.send(Method::GET, path, query, None, true).await?;
        decode(path, status, &bytes)
    }

    /// Native contract metadata, cached. Not found is an error for this variant.
    async fn spec(&self, name: &str) -> ArbResult<ContractSpec> {
        self.try_spec(name)
            .await?
            .ok_or_else(|| err(format!("Gate 无此合约: {name}")))
    }

    /// Native contract metadata, `Ok(None)` when the venue does not know it.
    async fn try_spec(&self, name: &str) -> ArbResult<Option<ContractSpec>> {
        if let Some(spec) = self.specs.lock().await.get(name).cloned() {
            return Ok(Some(spec));
        }
        let path = format!("/futures/{SETTLE}/contracts/{name}");
        let (status, bytes) = self.send(Method::GET, &path, "", None, false).await?;
        if !status.is_success() {
            if is_not_found(status, &bytes, "CONTRACT_NOT_FOUND") {
                return Ok(None);
            }
            return Err(api_error(&path, status, &bytes));
        }
        let wire: WireContract = decode(&path, status, &bytes)?;
        let spec = spec_from(wire)?;
        self.specs
            .lock()
            .await
            .insert(name.to_string(), spec.clone());
        Ok(Some(spec))
    }

    async fn all_contracts(&self) -> ArbResult<Vec<ContractSpec>> {
        let mut specs = Vec::new();
        let mut offset = 0;
        for _ in 0..50 {
            let path = format!("/futures/{SETTLE}/contracts");
            let query = format!("limit=100&offset={offset}");
            let (status, bytes) = self.send(Method::GET, &path, &query, None, false).await?;
            let page: Vec<WireContract> = decode(&path, status, &bytes)?;
            let count = page.len();
            let mut cache = self.specs.lock().await;
            for wire in page {
                let spec = spec_from(wire)?;
                cache.insert(spec.name.clone(), spec.clone());
                specs.push(spec);
            }
            drop(cache);
            if count < 100 {
                break;
            }
            offset += 100;
        }
        Ok(specs)
    }

    /// Map a scan symbol to the live native contract and its metadata. The symbol's
    /// base is checked against live metadata, never trusted as a bare string.
    async fn resolve_contract(&self, symbol: &Symbol) -> ArbResult<(String, ContractSpec)> {
        if symbol.quote != "USDT" {
            return Err(err("Gate 券商只支持 USDT 计价合约"));
        }
        let candidate = format!("{}_USDT", symbol.base);
        if let Some(spec) = self.try_spec(&candidate).await?
            && usable(&spec, symbol)
        {
            return Ok((candidate, spec));
        }
        for spec in self.all_contracts().await? {
            if usable(&spec, symbol) {
                return Ok((spec.name.clone(), spec));
            }
        }
        Err(err(format!(
            "Gate 找不到与 {symbol} 匹配的 USDT 线性永续合约"
        )))
    }

    async fn best_bid_ask(&self, contract: &str) -> ArbResult<(Option<Decimal>, Option<Decimal>)> {
        let path = format!("/futures/{SETTLE}/order_book");
        let query = format!("contract={contract}&limit=1");
        let (status, bytes) = self.send(Method::GET, &path, &query, None, false).await?;
        let book: WireBook = decode(&path, status, &bytes)?;
        let best = |levels: &[WireLevel], pick_max: bool| -> Option<Decimal> {
            let mut chosen: Option<Decimal> = None;
            for level in levels {
                let Some(price) = level
                    .p
                    .as_deref()
                    .and_then(|raw| Decimal::from_str_exact(raw).ok())
                    .filter(|value| *value > Decimal::ZERO)
                else {
                    continue;
                };
                chosen = Some(match chosen {
                    Some(current) if pick_max => current.max(price),
                    Some(current) => current.min(price),
                    None => price,
                });
            }
            chosen
        };
        Ok((best(&book.bids, true), best(&book.asks, false)))
    }

    async fn lookup_order(&self, id_or_text: &str) -> ArbResult<Option<WireOrder>> {
        let path = format!("/futures/{SETTLE}/orders/{}", safe_segment(id_or_text)?);
        let (status, bytes) = self.send(Method::GET, &path, "", None, true).await?;
        if status.is_success() {
            return decode(&path, status, &bytes).map(Some);
        }
        if is_not_found(status, &bytes, "ORDER_NOT_FOUND") {
            return Ok(None);
        }
        Err(api_error(&path, status, &bytes))
    }

    async fn order_trades(&self, order_id: &str, contract: &str) -> ArbResult<Vec<WireTrade>> {
        let mut trades = Vec::new();
        let mut offset = 0;
        for _ in 0..50 {
            let path = format!("/futures/{SETTLE}/my_trades");
            let query = format!("contract={contract}&order={order_id}&limit=100&offset={offset}");
            let (status, bytes) = self.send(Method::GET, &path, &query, None, true).await?;
            let page: Vec<WireTrade> = decode(&path, status, &bytes)?;
            let count = page.len();
            trades.extend(page);
            if count < 100 {
                break;
            }
            offset += 100;
        }
        Ok(trades)
    }

    /// Build the authoritative state of an order from its venue record plus its
    /// fills. Fills are the only source of fees.
    async fn build_state(
        &self,
        wire: &WireOrder,
        order: &NewOrder,
        spec: &ContractSpec,
    ) -> ArbResult<OrderState> {
        let id = wire.id.clone().ok_or_else(|| err("订单缺少交易所订单号"))?;
        let contract = wire.contract.clone().unwrap_or_else(|| spec.name.clone());
        if contract != spec.name {
            return Err(err("订单合约与预期不符"));
        }
        let size = wire.size.ok_or_else(|| err("订单缺少数量"))?;
        let left = wire.left.unwrap_or(0);
        if left < 0 || left > size.abs() {
            return Err(err("订单剩余量不合法"));
        }
        let executed = size.abs() - left;
        let status_raw = wire.status.as_deref().ok_or_else(|| err("订单缺少状态"))?;
        let status = map_status(status_raw, wire.finish_as.as_deref(), executed, size.abs())?;
        let trades = self.order_trades(&id, &contract).await?;
        let (base, quote, fee) =
            aggregate_fills(&trades, &id, &contract, spec.multiplier, executed)?;
        Ok(OrderState {
            order: order.clone(),
            venue_order_id: Some(id),
            status,
            filled_usdt: quote,
            average_price: (base > Decimal::ZERO).then(|| quote / base),
            fee_usdt: fee,
            reject_reason: None,
        })
    }

    fn save_terminal(&self, journal: &mut OrderJournal, state: &OrderState) -> ArbResult<()> {
        if !state.status.is_live() {
            journal.record_terminal(state)?;
        }
        Ok(())
    }

    /// Resume an already-reserved order: never resubmit, and if the venue cannot
    /// find it, error out (the id was definitely submitted or is indeterminate).
    async fn resume(
        &self,
        journal: &mut OrderJournal,
        entry: &JournalEntry,
    ) -> ArbResult<OrderAck> {
        if let Some(terminal) = &entry.terminal {
            return ack(terminal);
        }
        let spec = self.spec(&entry.instrument).await?;
        let wire = self
            .lookup_order(&entry.venue_client_id)
            .await?
            .ok_or_else(|| err("订单意图已存在但交易所查不到，绝不允许重发，请人工对账"))?;
        let state = self.build_state(&wire, &entry.order, &spec).await?;
        self.save_terminal(journal, &state)?;
        ack(&state)
    }

    async fn set_isolated_leverage(
        &self,
        contract: &str,
        leverage: i64,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let path = format!("/futures/{SETTLE}/positions/{contract}/leverage");
        let query = if mode.is_cross() {
            format!("leverage=0&cross_leverage_limit={leverage}")
        } else {
            format!("leverage={leverage}")
        };
        let (status, bytes) = self.send(Method::POST, &path, &query, None, true).await?;
        let position: WirePosition = decode(&path, status, &bytes)?;
        if let Some(mode) = &position.mode
            && mode != "single"
        {
            return Err(err("持仓处于双向模式，无法设置逐仓杠杆"));
        }
        let applied = position
            .leverage
            .as_deref()
            .and_then(|raw| Decimal::from_str_exact(raw).ok())
            .ok_or_else(|| err("杠杆设置后无法读回"))?;
        let effective = if mode.is_cross() {
            if applied != Decimal::ZERO {
                return Err(err("全仓模式未生效"));
            }
            position
                .cross_leverage_limit
                .as_deref()
                .and_then(|raw| Decimal::from_str_exact(raw).ok())
                .ok_or_else(|| err("全仓杠杆读回缺失"))?
        } else {
            applied
        };
        if effective != Decimal::from(leverage) {
            return Err(err("所选保证金模式的杠杆未生效"));
        }
        Ok(())
    }

    async fn position_of(&self, contract: &str) -> ArbResult<WirePosition> {
        let path = format!("/futures/{SETTLE}/positions/{contract}");
        let (status, bytes) = self.send(Method::GET, &path, "", None, true).await?;
        decode(&path, status, &bytes)
    }
}

#[async_trait]
impl Broker for GateBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let (contract, _) = self.resolve_contract(symbol).await?;
        let row = self.position_of(&contract).await?;
        if row.size.unwrap_or(0) == 0 {
            return Ok(None);
        }
        let parse = |value: &Option<String>| {
            value
                .as_deref()
                .and_then(|raw| Decimal::from_str_exact(raw).ok())
        };
        let mode = parse(&row.leverage).map(|l| {
            if l == Decimal::ZERO {
                crate::MarginMode::Cross
            } else {
                crate::MarginMode::Isolated
            }
        });
        Ok(Some(crate::margin::venue_state(
            mode,
            parse(&row.liq_price),
            parse(&row.margin),
        )))
    }

    async fn prepare_open_mode(
        &self,
        symbol: &Symbol,
        _: Side,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        self.options.authorize(VENUE)?;
        let _guard = self.journal.lock().await;
        let (contract, spec) = self.resolve_contract(symbol).await?;
        let leverage = crate::margin::leverage(VENUE, leverage)?;
        if leverage < spec.leverage_min || leverage > spec.leverage_max {
            return Err(err("leverage exceeds market limits"));
        }
        self.set_isolated_leverage(
            &contract,
            leverage.to_i64().ok_or_else(|| err("invalid leverage"))?,
            mode,
        )
        .await
    }

    fn venue(&self) -> Venue {
        VENUE
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.options.authorize(VENUE)?;
        if order.venue != VENUE {
            return Err(err("本券商只接受 Gate 订单"));
        }
        // Hold the journal lock for the whole submission so two concurrent calls with
        // the same client id cannot both reach the exchange.
        let mut journal = self.journal.lock().await;
        if let Some(entry) = journal.get(&order.client_order_id).cloned() {
            if entry.order != *order {
                return Err(err("client id reused with different intent/margin mode"));
            }
            return self.resume(&mut journal, &entry).await;
        }

        let (contract, spec) = self.resolve_contract(&order.symbol).await?;

        // Resolve the limit price. A missing limit uses a FRESH book and the explicit
        // slippage bound; a stale/empty/crossed book is rejected inside `bound_price`.
        let price = match order.limit_price {
            Some(price) => price,
            None => {
                let (bid, ask) = self.best_bid_ask(&contract).await?;
                self.options.bound_price(VENUE, order.side, bid, ask)?
            }
        };
        let price = round_price(price, spec.price_tick, order.side)
            .ok_or_else(|| err("价格按 tick 取整后非正"))?;
        let units = order_units(
            VENUE,
            order,
            price,
            spec.multiplier,
            Decimal::ONE,
            spec.size_min,
        )?;
        if units > spec.size_max {
            return Err(err("下单张数超过合约最大下单量"));
        }

        if order.reduce_only {
            let position = self.position_of(&contract).await?;
            if let Some(mode) = &position.mode
                && mode != "single"
            {
                return Err(err("持仓处于双向模式，无法用净持仓校验 reduce-only"));
            }
            let current = Decimal::from(position.size.unwrap_or(0));
            let covers = match order.side {
                Side::Sell => current >= units,
                Side::Buy => current <= -units,
            };
            if !covers {
                return Err(err("reduce-only 的方向或数量超出当前持仓"));
            }
        } else {
            let leverage = order
                .leverage
                .ok_or_else(|| err("开仓必须给出显式的逐仓杠杆"))?;
            if leverage <= Decimal::ZERO || leverage.fract() != Decimal::ZERO {
                return Err(err("杠杆必须是正整数"));
            }
            if leverage < spec.leverage_min || leverage > spec.leverage_max {
                return Err(err("杠杆超出该合约允许的范围"));
            }
            let leverage = leverage.to_i64().ok_or_else(|| err("杠杆超出范围"))?;
            self.set_isolated_leverage(&contract, leverage, order.margin_mode)
                .await?;
        }

        let signed = signed_contracts(units, order.side)?;
        let venue_cid = venue_client_id("t-", &order.client_order_id, 28);
        let body = serde_json::to_vec(&OrderRequest {
            contract: &contract,
            size: signed,
            price: price.to_string(),
            tif: "ioc",
            text: &venue_cid,
            reduce_only: order.reduce_only,
            close: false,
        })
        .map_err(|_| err("订单体无法序列化"))?;

        // Intent is fsynced BEFORE the order request is transmitted.
        journal.reserve(JournalEntry {
            order: order.clone(),
            venue_client_id: venue_cid.clone(),
            instrument: contract.clone(),
            units,
            terminal: None,
        })?;

        let path = format!("/futures/{SETTLE}/orders");
        let submitted = self.send(Method::POST, &path, "", Some(body), true).await;
        match submitted {
            Ok((status, bytes)) if status.is_success() => {
                let wire: WireOrder = decode(&path, status, &bytes)?;
                let id = wire.id.clone().ok_or_else(|| err("下单响应缺少订单号"))?;
                let wire = self.lookup_order(&id).await?.unwrap_or(wire);
                let state = self.build_state(&wire, order, &spec).await?;
                self.save_terminal(&mut journal, &state)?;
                ack(&state)
            }
            Ok((status, bytes)) => {
                // A business error normally means the order was rejected, but verify
                // first: a 4xx/5xx could still have landed an order.
                if let Some(wire) = self.lookup_order(&venue_cid).await? {
                    let state = self.build_state(&wire, order, &spec).await?;
                    self.save_terminal(&mut journal, &state)?;
                    return ack(&state);
                }
                let reason = error_reason(&path, status, &bytes);
                // Only a 4xx is a definitive refusal. A 5xx may have landed an order that is
                // not visible yet: leave the intent unresolved so every later lookup retries.
                if !status.is_client_error() {
                    return Err(err(format!(
                        "Gate 下单结果未知（{reason}），不要重发，先对账"
                    )));
                }
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(reason);
                journal.record_terminal(&rejected)?;
                Err(err(format!(
                    "Gate 拒单: {}",
                    rejected.reject_reason.unwrap_or_default()
                )))
            }
            Err(error) => {
                // Transport failure: the order may or may not exist. Look it up; if it
                // is not visible yet, surface the original error (never resubmit).
                if let Some(wire) = self.lookup_order(&venue_cid).await? {
                    let state = self.build_state(&wire, order, &spec).await?;
                    self.save_terminal(&mut journal, &state)?;
                    return ack(&state);
                }
                Err(error)
            }
        }
    }

    async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let entry = {
            let journal = self.journal.lock().await;
            journal.get(id).cloned()
        };
        if let Some(entry) = &entry
            && let Some(terminal) = &entry.terminal
        {
            return Ok(Some(terminal.clone()));
        }
        let venue_cid = venue_client_id("t-", id, 28);
        let Some(wire) = self.lookup_order(&venue_cid).await? else {
            if entry.is_some() {
                return Err(err(
                    "订单意图已存在但交易所查不到，绝不允许重发，请人工对账",
                ));
            }
            return Ok(None);
        };
        let spec = self
            .spec(&wire.contract.clone().ok_or_else(|| err("订单缺少合约"))?)
            .await?;
        let order = match &entry {
            Some(entry) => {
                if entry.instrument != spec.name {
                    return Err(err("交易所订单与本地意图的合约不一致"));
                }
                entry.order.clone()
            }
            None => synthetic_order(&wire, &spec)?,
        };
        let state = self.build_state(&wire, &order, &spec).await?;
        if let Some(entry) = &entry
            && entry.order.client_order_id != state.order.client_order_id
        {
            return Err(err("交易所订单与本地意图的客户单号不一致"));
        }
        if !state.status.is_live() {
            let mut journal = self.journal.lock().await;
            if journal.get(id).is_some() {
                journal.record_terminal(&state)?;
            }
        }
        Ok(Some(state))
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.options.authorize(VENUE)?;
        let id = safe_segment(venue_order_id)?;
        let current = self
            .lookup_order(&id)
            .await?
            .ok_or_else(|| err("交易所不认识这个订单号，无法确认撤单"))?;
        if is_terminal(&current)? {
            return Ok(());
        }
        let path = format!("/futures/{SETTLE}/orders/{id}");
        let (status, bytes) = self.send(Method::DELETE, &path, "", None, true).await?;
        if !status.is_success() && !is_not_found(status, &bytes, "ORDER_NOT_FOUND") {
            return Err(api_error(&path, status, &bytes));
        }
        let after = self
            .lookup_order(&id)
            .await?
            .ok_or_else(|| err("撤单后订单无法核实"))?;
        if is_terminal(&after)? {
            Ok(())
        } else {
            Err(err("撤单未生效，订单仍是活跃状态"))
        }
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let mut wires = Vec::new();
        let mut offset = 0;
        let path = format!("/futures/{SETTLE}/orders");
        for _ in 0..50 {
            // No contract filter: reconciliation must see foreign orders too.
            let query = format!("status=open&limit=100&offset={offset}");
            let (status, bytes) = self.send(Method::GET, &path, &query, None, true).await?;
            let page: Vec<WireOrder> = decode(&path, status, &bytes)?;
            let count = page.len();
            wires.extend(page);
            if count < 100 {
                break;
            }
            offset += 100;
        }
        let mut states = Vec::with_capacity(wires.len());
        for wire in wires {
            let spec = self
                .spec(&wire.contract.clone().ok_or_else(|| err("订单缺少合约"))?)
                .await?;
            let known = {
                let journal = self.journal.lock().await;
                wire.text
                    .as_deref()
                    .and_then(|text| journal.by_venue_client_id(text))
                    .map(|entry| entry.order.clone())
            };
            let order = match known {
                Some(order) => order,
                None => synthetic_order(&wire, &spec)?,
            };
            states.push(self.build_state(&wire, &order, &spec).await?);
        }
        Ok(states)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let mut out = Vec::new();
        let mut offset = 0;
        let path = format!("/futures/{SETTLE}/positions");
        for _ in 0..50 {
            let query = format!("holding=true&limit=100&offset={offset}");
            let (status, bytes) = self.send(Method::GET, &path, &query, None, true).await?;
            let page: Vec<WirePosition> = decode(&path, status, &bytes)?;
            let count = page.len();
            for position in page {
                let size = position.size.unwrap_or(0);
                if size == 0 {
                    continue;
                }
                if let Some(mode) = &position.mode
                    && mode != "single"
                {
                    return Err(err("账户存在双向持仓，净持仓口径不成立，停止对账"));
                }
                let contract = position
                    .contract
                    .clone()
                    .ok_or_else(|| err("持仓缺少合约名"))?;
                let spec = self.spec(&contract).await?;
                let symbol = symbol_of(&contract)?;
                let net_quantity = Decimal::from(size) * spec.multiplier;
                let average_price = position
                    .entry_price
                    .as_deref()
                    .and_then(|raw| Decimal::from_str_exact(raw).ok())
                    .filter(|value| *value > Decimal::ZERO);
                let notional_usdt = position
                    .value
                    .as_deref()
                    .and_then(|raw| Decimal::from_str_exact(raw).ok())
                    .map(|value| value.abs())
                    .filter(|value| *value > Decimal::ZERO)
                    .unwrap_or_else(|| {
                        Decimal::from(size.abs()) * spec.multiplier * spec.mark_price
                    });
                out.push(VenuePosition {
                    venue: VENUE,
                    symbol,
                    net_quantity,
                    average_price,
                    notional_usdt,
                });
            }
            if count < 100 {
                break;
            }
            offset += 100;
        }
        Ok(out)
    }
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| err("订单缺少交易所单号"))?,
        status: state.status,
    })
}

fn is_terminal(wire: &WireOrder) -> ArbResult<bool> {
    let size = wire.size.ok_or_else(|| err("订单缺少数量"))?;
    let left = wire.left.unwrap_or(0);
    let status = wire.status.as_deref().ok_or_else(|| err("订单缺少状态"))?;
    let status = map_status(
        status,
        wire.finish_as.as_deref(),
        size.abs() - left,
        size.abs(),
    )?;
    Ok(!status.is_live())
}

/// Synthesize an intent for an exchange order we did not create so reconciliation
/// can see it. Never hidden, never silently dropped.
fn synthetic_order(wire: &WireOrder, spec: &ContractSpec) -> ArbResult<NewOrder> {
    let id = wire.id.clone().ok_or_else(|| err("订单缺少交易所订单号"))?;
    let size = wire.size.unwrap_or(0);
    let side = if size >= 0 { Side::Buy } else { Side::Sell };
    let magnitude = Decimal::from(size.abs());
    let price = wire
        .fill_price
        .as_deref()
        .and_then(|raw| Decimal::from_str_exact(raw).ok())
        .filter(|value| *value > Decimal::ZERO)
        .or_else(|| {
            wire.price
                .as_deref()
                .and_then(|raw| Decimal::from_str_exact(raw).ok())
                .filter(|value| *value > Decimal::ZERO)
        })
        .unwrap_or(spec.mark_price);
    Ok(NewOrder {
        margin_mode: crate::MarginMode::Isolated,
        client_order_id: ClientOrderId(format!("gate-external-{id}")),
        venue: VENUE,
        symbol: symbol_of(&spec.name)?,
        side,
        notional_usdt: magnitude * spec.multiplier * price,
        quantity: Some(magnitude * spec.multiplier),
        limit_price: Some(price),
        reduce_only: wire.reduce_only.or(wire.is_reduce_only).unwrap_or(false),
        leverage: None,
    })
}

/// Restrict an id to characters that are safe in a URL path segment.
fn safe_segment(value: &str) -> ArbResult<String> {
    if value.is_empty()
        || !value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(err("非法的订单号"));
    }
    Ok(value.to_string())
}

fn is_not_found(status: StatusCode, bytes: &[u8], label: &str) -> bool {
    if status == StatusCode::NOT_FOUND {
        return true;
    }
    serde_json::from_slice::<WireError>(bytes)
        .map(|error| error.label == label)
        .unwrap_or(false)
}

fn decode<T: serde::de::DeserializeOwned>(
    path: &str,
    status: StatusCode,
    bytes: &[u8],
) -> ArbResult<T> {
    if !status.is_success() {
        return Err(api_error(path, status, bytes));
    }
    serde_json::from_slice(bytes).map_err(|_| err(format!("{path}: {}", json_error())))
}

fn api_error(path: &str, status: StatusCode, bytes: &[u8]) -> ArbError {
    err(error_reason(path, status, bytes))
}

/// Business error text: label + message only, never the raw body (which could echo
/// signed request data).
fn error_reason(path: &str, status: StatusCode, bytes: &[u8]) -> String {
    match serde_json::from_slice::<WireError>(bytes) {
        Ok(error) => format!(
            "{path}: HTTP {status} {} {}",
            error.label,
            error.message.chars().take(200).collect::<String>()
        ),
        Err(_) => format!("{path}: HTTP {status}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn signature_matches_the_official_documented_example() {
        // Gate APIv4 docs, "API Signature string generation" example 1:
        // GET /api/v4/futures/orders with query, empty body, timestamp 1541993715,
        // key/secret both literal "key"/"secret".
        let sign = gate_signature(
            b"secret",
            "GET",
            "/api/v4/futures/orders",
            "contract=BTC_USD&status=finished&limit=50",
            b"",
            "1541993715",
        );
        assert_eq!(
            sign,
            "55f84ea195d6fe57ce62464daaa7c3c02fa9d1dde954e4c898289c9a2407a3d6fb3faf24deff16790d726b66ac9f74526668b13bd01029199cc4fcc522418b8a"
        );
    }

    #[test]
    fn sha512_of_empty_body_matches_the_documented_constant() {
        assert_eq!(
            sha512_hex(b""),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
    }

    #[test]
    fn size_is_a_signed_integer_contract_count() {
        assert_eq!(signed_contracts(dec!(5), Side::Buy).unwrap(), 5);
        assert_eq!(signed_contracts(dec!(5), Side::Sell).unwrap(), -5);
        assert!(signed_contracts(dec!(0.5), Side::Buy).is_err());
        assert!(signed_contracts(dec!(-1), Side::Buy).is_err());
    }

    #[test]
    fn status_mapping_rejects_unknown_strings_and_partial_fills() {
        assert_eq!(map_status("open", None, 0, 10).unwrap(), OrderStatus::Open);
        assert_eq!(
            map_status("finished", Some("filled"), 10, 10).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            map_status("finished", Some("ioc"), 4, 10).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status("finished", Some("reduce_only"), 0, 10).unwrap(),
            OrderStatus::Cancelled
        );
        assert!(map_status("finished", Some("filled"), 4, 10).is_err());
        assert!(map_status("finished", None, 0, 10).is_err());
        assert!(map_status("finished", Some("something_new"), 0, 10).is_err());
        assert!(map_status("weird", None, 0, 10).is_err());
    }

    fn trade(
        id: &str,
        order_id: &str,
        size: i64,
        price: &str,
        fee: &str,
        currency: Option<&str>,
    ) -> WireTrade {
        WireTrade {
            id: Some(id.to_string()),
            order_id: Some(order_id.to_string()),
            contract: Some("BTC_USDT".to_string()),
            size: Some(size),
            price: Some(price.to_string()),
            fee: Some(fee.to_string()),
            fee_currency: currency.map(str::to_string),
        }
    }

    #[test]
    fn fills_aggregate_into_base_quote_and_fee() {
        let trades = vec![
            trade("t1", "123", 5, "100", "0.5", Some("USDT")),
            trade("t2", "123", 2, "100", "0.2", None),
        ];
        let (base, quote, fee) =
            aggregate_fills(&trades, "123", "BTC_USDT", dec!(0.0001), 7).unwrap();
        assert_eq!(base, dec!(0.0007));
        assert_eq!(quote, dec!(0.07));
        assert_eq!(fee, dec!(0.7));
    }

    #[test]
    fn fills_fail_closed_on_currency_quantity_and_order_mismatch() {
        let agg = |trades: Vec<WireTrade>, executed| {
            aggregate_fills(&trades, "123", "BTC_USDT", dec!(0.0001), executed)
        };
        assert!(agg(vec![trade("t1", "123", 5, "100", "0.5", Some("GT"))], 5).is_err());
        assert!(agg(vec![trade("t1", "999", 5, "100", "0.5", Some("USDT"))], 5).is_err());
        assert!(agg(vec![trade("t1", "123", 5, "100", "0.5", Some("USDT"))], 6).is_err());
        // 分页重叠把同一笔成交返回两次：数量碰巧对上也不能双算手续费。
        let duplicated = vec![
            trade("t1", "123", 3, "100", "0.3", Some("USDT")),
            trade("t1", "123", 3, "100", "0.3", Some("USDT")),
        ];
        assert!(agg(duplicated, 6).is_err());
        let mut other_contract = trade("t1", "123", 5, "100", "0.5", Some("USDT"));
        other_contract.contract = Some("ETH_USDT".into());
        assert!(agg(vec![other_contract], 5).is_err());
    }

    #[test]
    fn account_mode_requires_one_way_and_classic_margin() {
        assert!(ensure_account_mode(Some("single"), None, None).is_ok());
        assert!(ensure_account_mode(None, Some(false), None).is_ok());
        assert!(ensure_account_mode(Some("dual"), Some(true), None).is_err());
        assert!(ensure_account_mode(Some("split"), None, None).is_err());
        assert!(ensure_account_mode(None, None, None).is_err());
        assert!(ensure_account_mode(Some("single"), None, Some(true)).is_err());
    }

    #[test]
    fn contract_metadata_maps_back_to_the_scan_symbol() {
        let wire: WireContract = serde_json::from_str(
            r#"{"name":"BTC_USDT","type":"direct","status":"trading","in_delisting":false,
                "quanto_multiplier":"0.0001","order_price_round":"0.1","order_size_min":1,
                "order_size_max":12000000,"leverage_min":"1","leverage_max":"100",
                "mark_price":"82947.27"}"#,
        )
        .unwrap();
        let spec = spec_from(wire).unwrap();
        assert_eq!(spec.multiplier, dec!(0.0001));
        assert_eq!(spec.price_tick, dec!(0.1));
        assert!(usable(&spec, &Symbol::perp("BTC", "USDT")));
        assert!(!usable(&spec, &Symbol::perp("ETH", "USDT")));
        // Inverse and delisted contracts are never used for a live order.
        assert!(!usable(
            &ContractSpec {
                kind: "inverse".into(),
                ..spec.clone()
            },
            &Symbol::perp("BTC", "USDT")
        ));
        assert!(!usable(
            &ContractSpec {
                in_delisting: true,
                ..spec.clone()
            },
            &Symbol::perp("BTC", "USDT")
        ));
    }

    #[test]
    fn synthetic_orders_are_visible_for_reconciliation() {
        let spec = ContractSpec {
            name: "BTC_USDT".into(),
            kind: "direct".into(),
            status: "trading".into(),
            in_delisting: false,
            multiplier: dec!(0.0001),
            price_tick: dec!(0.1),
            size_min: dec!(1),
            size_max: dec!(1000000),
            leverage_min: dec!(1),
            leverage_max: dec!(100),
            mark_price: dec!(80000),
        };
        let wire: WireOrder = serde_json::from_str(
            r#"{"id":42,"contract":"BTC_USDT","size":-3,"left":-1,"price":"80100",
                "fill_price":"80050","status":"open","reduce_only":true}"#,
        )
        .unwrap();
        let order = synthetic_order(&wire, &spec).unwrap();
        assert_eq!(order.client_order_id.0, "gate-external-42");
        assert_eq!(order.side, Side::Sell);
        assert!(order.reduce_only);
        assert_eq!(order.symbol, Symbol::perp("BTC", "USDT"));
    }

    #[test]
    fn client_text_id_is_within_the_documented_limit() {
        let id = venue_client_id("t-", &ClientOrderId("live-1-buy-0".into()), 28);
        assert_eq!(id.len(), 28);
        assert!(id.starts_with("t-"));
        assert!(safe_segment(&id).is_ok());
    }

    #[test]
    fn unsafe_order_segments_are_rejected() {
        assert!(safe_segment("123").is_ok());
        assert!(safe_segment("t-abcdef").is_ok());
        assert!(safe_segment("a/b").is_err());
        assert!(safe_segment("../x").is_err());
        assert!(safe_segment("").is_err());
    }
}
