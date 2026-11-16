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
//!   sides 1=open long, 2=close short, 3=open short, 4=close long. In one-way mode a close is
//!   side 1/3 with `reduceOnly:true`: side 2/4 together with `reduceOnly` is refused (2001,
//!   probed 10-22), and MEXC itself rewrites side 2/4 to 1/3 + `reduceOnly:true`.
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

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenuePosition};
use crate::live_common::{
    Cached, JournalEntry, LiveOptions, OrderJournal, Verified, hex_lower, hmac_sha256, order_units,
    round_price, transport_error, venue_client_id,
};
use crate::settlement::{FillEffect, VenueFill};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE: &str = "https://api.mexc.com";
const CONTRACTS: &str = "/api/v1/contract/detail/country";
const POSITION_MODE: &str = "/api/v1/private/position/position_mode";
const POSITIONS: &str = "/api/v1/private/position/open_positions";
const LEVERAGE: &str = "/api/v1/private/position/leverage";
const CHANGE_LEVERAGE: &str = "/api/v1/private/position/change_leverage";
/// <https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/modify-position-margin>：
/// `positionId`、`amount`、`type`（ADD / SUB），要「下单」权限，限频 20 次 / 2 秒，成功 `{"success":true,"code":0}`。
const CHANGE_MARGIN: &str = "/api/v1/private/position/change_margin";
const CREATE: &str = "/api/v1/private/order/create";
const CANCEL: &str = "/api/v1/private/order/cancel";
const OPEN_ORDERS: &str = "/api/v1/private/order/list/open_orders";
const FEES: &str = "/api/v1/private/account/tiered_fee_rate/v2";
const ASSET_USDT: &str = "/api/v1/private/account/asset/USDT";
const FUNDING_RECORDS: &str = "/api/v1/private/position/funding_records";
const ORDER_DEALS: &str = "/api/v1/private/order/list/order_deals";
/// 成交明细读不到 / 对不上时，每次重读前等多久（共约 2 秒）。
const DEAL_REREADS: [Duration; 3] = [
    Duration::from_millis(300),
    Duration::from_millis(600),
    Duration::from_millis(1200),
];

/// 合约规格与标准名 10 分钟内不重读（找不到的合约再读一次）。规格整表约 1.2 MB、0.7 秒，原来每笔单都读。
const CONTRACTS_TTL: Duration = Duration::from_secs(600);
/// 停牌 / 休市判断要 1 分钟内的合约状态。
const STATUS_TTL: Duration = Duration::from_secs(60);
/// 目录刷新失败（限频 510、超时）时，规格与名字最多退回这么老的一份；停牌判断不退。
const CATALOG_FALLBACK: Duration = Duration::from_secs(3600);
/// 持仓模式、杠杆核对过后这么久内不再读：只省读，下单体里照样写死单向、保证金模式和杠杆，
/// 回来的订单照样逐项核对。
const SETTINGS_TTL: Duration = Duration::from_secs(20);
/// 成交查询每段最长 30 天（2026-10-21 实测 89 天可以、100 天报 6003）。
const DEALS_WINDOW_MS: i64 = 30 * 86_400_000;
const HISTORY_PAGE: usize = 100;
const HISTORY_MAX_PAGES: usize = 100;

/// 文档标注为「稍后重试 / 结果未知」的码（<https://www.mexc.com/api-docs/futures/error-code>）：
/// 500 内部错误、501 系统繁忙、510 请求过于频繁、513 无效请求、3016 下单超时。
/// 这些必须先去交易所查询，绝不能当成拒单。
const AMBIGUOUS_CODES: [i64; 5] = [500, 501, 510, 513, 3016];
/// 510「请求过于频繁」：东京共用出口 IP 上实测出现过（见 `catalog`）。
const CODE_TOO_FREQUENT: i64 = 510;

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
    /// 合约规格与标准名（见 [`Self::catalog`]）。
    catalog: Cached<Arc<Catalog>>,
    /// 刚核对过是单向持仓。
    mode_ok: Verified<()>,
    /// 刚核对过的杠杆：（合约名, positionType, openType, 杠杆）。
    leverage_ok: Verified<(String, i64, i64, Decimal)>,
}

/// 合约规格（`/detail/country`）与标准名（[`arb_venues::mexc::fetch_bases`]：股票类按显示名里的代码，
/// `NVIDIA_USDT` → `NVDA`；和扫描、价差监控同一套，台账、对账里的标的才对得上）。
struct Catalog {
    contracts: HashMap<String, Contract>,
    names: HashMap<String, String>,
}

impl Catalog {
    /// 合约名对应的标准标的；不是 USDT 线性永续、或合约名和 `baseCoin` 对不上（倍数合约之类）为 `None`。
    fn symbol_of(&self, native: &str) -> Option<Symbol> {
        let contract = self.contracts.get(native)?;
        let base = native.strip_suffix("_USDT")?;
        (contract.quote_coin == "USDT"
            && contract.settle_coin == "USDT"
            && contract.future_type == 1
            && !base.is_empty()
            && base == contract.base_coin)
            .then_some(())?;
        Some(Symbol::perp(self.names.get(native)?, "USDT"))
    }

    /// 标准标的对应的合约（一个都没有为 `None`，不止一个是错）。
    fn find(&self, symbol: &Symbol) -> ArbResult<Option<Contract>> {
        let mut matches = self
            .contracts
            .keys()
            .filter(|native| self.symbol_of(native).as_ref() == Some(symbol));
        let Some(native) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() {
            return Err(err("ambiguous contract mapping"));
        }
        Ok(self.contracts.get(native).cloned())
    }
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
            catalog: Cached::new(),
            mode_ok: Verified::new(SETTINGS_TTL),
            leverage_ok: Verified::new(SETTINGS_TTL),
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
        check_deduction(&fees)?;
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

    /// A signed GET whose success may carry no `data`: MEXC's answer for an order it has no
    /// record of (`get-order-by-external-id`).
    async fn get_optional<T: DeserializeOwned>(&self, path: &str) -> ArbResult<Option<T>> {
        let timestamp = (Utc::now().timestamp_millis() + self.clock_offset).to_string();
        let signature = sign(
            &self.credentials.api_secret,
            &self.credentials.api_key,
            &timestamp,
            "",
        );
        let response = self
            .client
            .get(format!("{BASE}{path}"))
            .timeout(Duration::from_secs(10))
            .header("ApiKey", sensitive(&self.credentials.api_key)?)
            .header("Request-Time", timestamp)
            .header("Signature", sensitive(&signature)?)
            .header("Recv-Window", "10")
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
            return Err(err(format!("HTTP {status}; API code {}", envelope.code)));
        }
        envelope.data.map(|data| decode(data.get())).transpose()
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
        if self.mode_ok.is_fresh(&()) {
            return Ok(());
        }
        let mode: PositionMode = self.get(POSITION_MODE, &[]).await?;
        if mode.value() != 2 {
            return Err(err(
                "configure One-way position mode in MEXC UI first; broker never changes account-wide settings",
            ));
        }
        self.mode_ok.mark(());
        Ok(())
    }

    /// 合约规格与标准名，不比 `max_age` 老。
    async fn catalog(&self, max_age: Duration) -> ArbResult<Arc<Catalog>> {
        if let Some(catalog) = self.catalog.get(max_age) {
            return Ok(catalog);
        }
        let (rows, names) = tokio::join!(
            self.get::<Vec<Contract>>(CONTRACTS, &[]),
            arb_venues::mexc::fetch_bases(&self.client)
        );
        let catalog = Arc::new(Catalog {
            contracts: rows?
                .into_iter()
                .map(|contract| (contract.symbol.clone(), contract))
                .collect(),
            names: names?,
        });
        self.catalog.put(Arc::clone(&catalog));
        Ok(catalog)
    }

    /// 规格目录；刷新失败时退回 [`CATALOG_FALLBACK`] 内的上一份：规格和名字很少变，不能因为一次
    /// 限频让持仓、挂单、对账整家失败（和扫描器共用东京的 IP，510 实测出现过）。
    async fn catalog_lenient(&self, max_age: Duration) -> ArbResult<Arc<Catalog>> {
        match self.catalog(max_age).await {
            Ok(catalog) => Ok(catalog),
            Err(error) => match self.catalog.get(CATALOG_FALLBACK) {
                Some(catalog) => {
                    tracing::warn!(%error, "MEXC 合约目录刷新失败，沿用一小时内的上一份");
                    Ok(catalog)
                }
                None => Err(error),
            },
        }
    }

    /// 合约名对应的规格和标准标的（缓存里没有就重读一次）。
    async fn contract(&self, native: &str) -> ArbResult<(Contract, Option<Symbol>)> {
        for max_age in [CONTRACTS_TTL, Duration::ZERO] {
            let catalog = self.catalog_lenient(max_age).await?;
            if let Some(contract) = catalog.contracts.get(native) {
                contract.validate_units()?;
                return Ok((contract.clone(), catalog.symbol_of(native)));
            }
        }
        Err(err("contract metadata missing"))
    }

    /// 标准标的对应的合约，不看状态（查历史用；缓存里没有就重读一次）。
    async fn find(&self, symbol: &Symbol) -> ArbResult<Contract> {
        for max_age in [CONTRACTS_TTL, Duration::ZERO] {
            if let Some(contract) = self.catalog_lenient(max_age).await?.find(symbol)? {
                contract.validate_units()?;
                return Ok(contract);
            }
        }
        Err(err("no matching USDT linear perpetual metadata"))
    }

    /// 下单用的合约：要在交易、允许 API 下单。
    async fn resolve(&self, symbol: &Symbol) -> ArbResult<Contract> {
        let contract = self.find(symbol).await?;
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
            if !book_fresh(now, book.timestamp, started.elapsed()) {
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

    /// 这个合约这一边（`kind`）现在是不是已经是这个保证金模式和杠杆。
    async fn leverage_in_force(
        &self,
        native: &str,
        kind: i64,
        open_type: i64,
        leverage: Decimal,
    ) -> ArbResult<bool> {
        let rows: Vec<Leverage> = self.get(LEVERAGE, &[("symbol", native.into())]).await?;
        Ok(leverage_matches(&rows, kind, open_type, leverage))
    }

    /// 设好这一边的保证金模式和杠杆。先读：已经是了就不写（刚核对过的 20 秒内连读都省）；
    /// 写了就读回核对，对不上不下单。
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
        let key = (contract.symbol.clone(), kind, open_type, leverage);
        if self.leverage_ok.is_fresh(&key) {
            return Ok(());
        }
        if self
            .leverage_in_force(&contract.symbol, kind, open_type, leverage)
            .await?
        {
            self.leverage_ok.mark(key);
            return Ok(());
        }
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
        if !self
            .leverage_in_force(&contract.symbol, kind, open_type, leverage)
            .await?
        {
            return Err(err(
                "selected margin mode/leverage readback mismatch; no order submitted",
            ));
        }
        self.leverage_ok.mark(key);
        Ok(())
    }

    /// The order behind a reserved ID; `None` = MEXC has no record of it. Orders it did create
    /// stay queryable here, even an IOC cancelled with nothing filled (probed 10-22).
    async fn remote(&self, entry: &JournalEntry) -> ArbResult<Option<RemoteOrder>> {
        self.get_optional(&format!(
            "/api/v1/private/order/external/{}/{}",
            entry.instrument, entry.venue_client_id
        ))
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
        symbol: Option<&Symbol>,
    ) -> ArbResult<OrderState> {
        let order = verified_order(&row, entry, contract, symbol)?;
        // 订单已成交、成交明细还没出齐（数量对不上）或这一次没读到：隔一会儿再读，几次都不行才报「不明」。
        // 以前读一次失败就让整张单「结果不明」，开仓 / 平仓跟着中断。
        let mut attempt = 0;
        let (filled, notional, fees) = loop {
            let read = self
                .get::<Vec<Deal>>(
                    &format!("/api/v1/private/order/deal_details/{}", row.order_id),
                    &[],
                )
                .await
                .and_then(|deals| aggregate(&deals, &row, contract.contract_size.0));
            match read {
                Ok(totals) => break totals,
                Err(error) if attempt < DEAL_REREADS.len() => {
                    tracing::debug!(order = %row.order_id, %error, "MEXC 成交明细还没对上，稍后再读");
                    tokio::time::sleep(DEAL_REREADS[attempt]).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
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
            fee_estimated: false,
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
        let row = self.remote(&entry).await?.ok_or_else(|| {
            err("MEXC has no order for this reserved ID (refused, or not visible yet); reconcile")
        })?;
        let (contract, symbol) = self.contract(&entry.instrument).await?;
        let state = self
            .state(row, Some(&entry), &contract, symbol.as_ref())
            .await?;
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

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// 下单按 [`order_units`] 把标的数量换成张数、向下取整到 `volUnit` 张：标的数量的步长 = 每张面值 × `volUnit`。
    async fn quantity_step(&self, symbol: &Symbol) -> ArbResult<Option<Decimal>> {
        let contract = self.resolve(symbol).await?;
        quantity_step_of(&contract).map(Some)
    }

    /// `change_margin`（`type=ADD`）往这个合约已有的逐仓持仓补保证金。最多发一次请求、绝不重试：没有幂等键，
    /// 重发会补两次。请求发出之前的任何失败（只读模式、没有持仓、不是逐仓……）都是 `Err`，没动钱。
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.options.authorize(Venue::Mexc)?;
        let _guard = self.journal.lock().await;
        let contract = self.resolve(symbol).await?;
        let body = add_margin_body(&self.raw_positions().await?, &contract.symbol, amount_usdt)?;
        Ok(margin_outcome(self.post(CHANGE_MARGIN, body).await))
    }

    /// 开仓前预热：合约规格、持仓模式、杠杆读一遍（只读）；已经是目标杠杆的记下来，紧接着的下单不用再读。
    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let contract = self.resolve(symbol).await?;
        self.check_mode().await?;
        // 杠杆不合法由准备那一步拒，这里不管。
        let Some(leverage) = leverage
            .and_then(|value| crate::margin::leverage(Venue::Mexc, Some(value)).ok())
            .filter(|value| *value >= contract.min_leverage.0 && *value <= contract.max_leverage.0)
        else {
            return Ok(());
        };
        let rows: Vec<Leverage> = self
            .get(LEVERAGE, &[("symbol", contract.symbol.clone())])
            .await?;
        for row in &rows {
            if leverage_matches(&rows, row.position_type, row.open_type, leverage) {
                self.leverage_ok.mark((
                    contract.symbol.clone(),
                    row.position_type,
                    row.open_type,
                    leverage,
                ));
            }
        }
        Ok(())
    }

    /// 停牌（`state` 不是 0）或不许 API 下单。
    ///
    /// 股票 / 商品类合约**周末照常交易**，不按美股休市挡：2026-10-24 12:00 至 10-26 12:00 UTC 每 3 分钟记录
    /// NVIDIA、AAPL、TESLA、某合约、QQQ、某合约、XAU（95 上 `bgweekend/mexc-weekend.jsonl`），整个周末 `state` 0、
    /// `apiAllowed` true，盘口与指数价一直在动，买卖价差 0.1～7bp。和 Bitget（周末真停）不同。
    async fn trading_halted(
        &self,
        symbol: &Symbol,
        reduce_only: bool,
    ) -> ArbResult<Option<String>> {
        let _ = reduce_only;
        let Some(contract) = self.catalog(STATUS_TTL).await?.find(symbol)? else {
            return Ok(Some(format!("MEXC 合约列表里没有 {}", symbol.base)));
        };
        if contract.state != 0 || !contract.api_allowed {
            return Ok(Some(format!(
                "MEXC {} 现在不能交易（state {}，API 下单{}）",
                contract.symbol,
                contract.state,
                if contract.api_allowed {
                    "允许"
                } else {
                    "不允许"
                }
            )));
        }
        Ok(None)
    }

    /// USDT 可开仓余额（`availableOpen`）。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        let asset: Asset = self.get(ASSET_USDT, &[]).await?;
        if asset.currency != "USDT" {
            return Err(err("asset currency mismatch"));
        }
        Ok(Some(asset.available_open.0.max(Decimal::ZERO)))
    }

    /// 这个合约 `since` 起实际结算的资金费（按合约分页查，正 = 收到）。
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let native = self.find(symbol).await?.symbol;
        let start = since.timestamp_millis();
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        for page in 1..=HISTORY_MAX_PAGES {
            let reply: FundingPage = self
                .get(
                    FUNDING_RECORDS,
                    &[
                        ("symbol", native.clone()),
                        ("page_num", page.to_string()),
                        ("page_size", HISTORY_PAGE.to_string()),
                    ],
                )
                .await?;
            let (entries, done) = funding_page(&reply, &native, start, page)?;
            for (id, at, amount) in entries {
                if seen.insert(id) {
                    rows.push((at, amount));
                }
            }
            if done {
                return Ok(Some(FundingTotal::from_rows(rows, since)));
            }
        }
        Err(err(
            "funding history needs more pages than allowed; refusing a partial total",
        ))
    }

    /// 这个合约 `[since, until]` 的成交（30 天一段，每段翻到最后一页）。
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> ArbResult<Option<Vec<VenueFill>>> {
        let contract = self.find(symbol).await?;
        let (start, end) = (since.timestamp_millis(), until.timestamp_millis());
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut from = start;
        while from <= end {
            let to = from.saturating_add(DEALS_WINDOW_MS - 1).min(end);
            let mut page = 1;
            loop {
                let rows: Vec<Deal> = self
                    .get(
                        ORDER_DEALS,
                        &[
                            ("symbol", contract.symbol.clone()),
                            ("start_time", from.to_string()),
                            ("end_time", to.to_string()),
                            ("page_num", page.to_string()),
                            ("page_size", HISTORY_PAGE.to_string()),
                        ],
                    )
                    .await?;
                let full = rows.len() >= HISTORY_PAGE;
                for deal in &rows {
                    if seen.insert(deal.id.clone()) {
                        out.push(venue_fill(deal, &contract)?);
                    }
                }
                if !full {
                    break;
                }
                if page >= HISTORY_MAX_PAGES {
                    return Err(err(
                        "fill history needs more pages than allowed; refusing a partial history",
                    ));
                }
                page += 1;
            }
            from = to + 1;
        }
        out.sort_by_key(|fill| fill.at);
        Ok(Some(out))
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        crate::broker::refuse_post_only(order)?;
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
            side_code(order.side),
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
            let state = self
                .state(row, Some(&entry), &contract, Some(&order.symbol))
                .await?;
            if !state.status.is_live() {
                journal.record_terminal(&state)?;
            }
            return ack(&state);
        }
        if let Err(failure) = &submission {
            // The executor only reports the read-back after this, so the venue's own answer is
            // logged here (codes only, see `classify_write`).
            let failure = match failure {
                WriteError::Api { message, .. } => message.clone(),
                WriteError::Unknown(error) | WriteError::Throttled(error) => error.to_string(),
            };
            tracing::warn!(order = %order.client_order_id, %failure, "MEXC 下单请求没被接受，读回核实");
        }
        // Every poll answering "no such order" proves the venue never created it.
        let mut absent = true;
        for attempt in 0..10 {
            if attempt != 0 {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            match self.remote(&entry).await {
                Ok(Some(row)) => {
                    let state = self
                        .state(row, Some(&entry), &contract, Some(&order.symbol))
                        .await?;
                    if !state.status.is_live() {
                        journal.record_terminal(&state)?;
                    }
                    return ack(&state);
                }
                Ok(None) => {}
                Err(_) => absent = false,
            }
        }
        match submission {
            Ok(_) => Err(err(
                "submitted order not queryable; durable intent retained, do not resubmit",
            )),
            Err(WriteError::Api { code, message }) => Err(settle_failure(
                &mut journal,
                order,
                Some(code),
                absent,
                message,
            )?),
            Err(WriteError::Unknown(error)) => Err(error),
            // 被限频、随后 10 次读回每次都查无此单：交易所没建这张单，记成拒单。以前一律当结果不明，
            // 订单号从此永远查不到、不得重发，做第二腿时第一腿就一直裸着。读回出过错（`absent` 为假）不下结论。
            Err(WriteError::Throttled(error)) => {
                if absent {
                    let mut rejected = OrderState::new(order.clone());
                    rejected.status = OrderStatus::Rejected;
                    rejected.reject_reason = Some(format!("{error}; never created on the venue"));
                    journal.record_terminal(&rejected)?;
                }
                Err(error)
            }
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
            let (contract, symbol) = self.contract(&after.symbol).await?;
            let state = self
                .state(after, Some(&entry), &contract, symbol.as_ref())
                .await?;
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
                let (contract, symbol) = self.contract(&row.symbol).await?;
                let Some(symbol) = symbol else {
                    continue;
                };
                let entry = self
                    .journal
                    .lock()
                    .await
                    .by_venue_client_id(&row.external_oid)
                    .cloned();
                let state = self
                    .state(row, entry.as_ref(), &contract, Some(&symbol))
                    .await?;
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
            let (contract, symbol) = self.contract(&row.symbol).await?;
            let symbol = symbol.ok_or_else(|| {
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
    /// 传输失败、超时、HTTP 5xx、无法解析的响应：结果未知，绝不能记成拒单。
    Unknown(ArbError),
    /// 限频（HTTP 429、业务码 510「请求过于频繁」）：多半没被受理，但单凭回包不能下结论；
    /// 下单路径只在随后的读回**每次都查无此单**时才记成拒单（见 `place`）。其余调用方按 `Unknown` 对待。
    Throttled(ArbError),
}

impl From<WriteError> for ArbError {
    fn from(failure: WriteError) -> Self {
        match failure {
            WriteError::Api { message, .. } => err(message),
            WriteError::Unknown(error) | WriteError::Throttled(error) => error,
        }
    }
}

/// 解析写请求的响应。文档 place-order：失败时 `success=false` 且 `data=null`
/// （<https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order>）。
/// 429/5xx 与 [`AMBIGUOUS_CODES`] 一律归为「结果未知」，交给操作者按日志对账。
/// 公开盘口（`contract/depth`）的 `timestamp` 最多落后多久还算新鲜。它是服务端快照的生成时刻，不是我们收到的时刻：
/// 2026-10-26 实测快照每 5～6 秒才换一次，NVIDIA 这种活跃合约也会落后 5.4 秒。原来卡 5 秒，没带限价的单（平仓单）
/// 就会随机被自己拒掉（0G 往返的第一张平仓单「stale order book」，规则轮一分钟后重试才平掉，期间一条腿裸着）。
const DEPTH_MAX_AGE_MS: i64 = 15_000;

/// 拉来定保护价的盘口能不能用：请求本身不超过 2 秒，快照不比 [`DEPTH_MAX_AGE_MS`] 更旧，也不在未来。
fn book_fresh(now_ms: i64, book_ms: i64, elapsed: Duration) -> bool {
    elapsed <= Duration::from_secs(2)
        && now_ms - book_ms <= DEPTH_MAX_AGE_MS
        && book_ms <= now_ms + 1000
}

/// 标的数量的下单步长（见 [`Broker::quantity_step`]）。
fn quantity_step_of(contract: &Contract) -> ArbResult<Decimal> {
    contract.validate_units()?;
    multiply(contract.contract_size.0, contract.vol_unit.0)
}

/// 补保证金的请求体：这个合约上正在持有的那一笔逐仓仓位（单向持仓下只有一笔）。
fn add_margin_body(rows: &[Position], native: &str, amount: Decimal) -> ArbResult<String> {
    let amount = crate::bitget_broker::margin_amount_text(amount)?;
    let held: Vec<&Position> = rows
        .iter()
        .filter(|p| p.symbol == native && p.hold_vol.0 > Decimal::ZERO)
        .collect();
    let [position] = held.as_slice() else {
        return Err(err(if held.is_empty() {
            "no open position to add margin to"
        } else {
            "more than one position on this contract; reconcile first"
        }));
    };
    if position.open_type != 1 {
        return Err(err("margin can only be added to an isolated position"));
    }
    if position.state != 1 {
        return Err(err("position is not in normal holding state"));
    }
    Ok(format!(
        "{{\"positionId\":{},\"amount\":{amount},\"type\":\"ADD\"}}",
        position.position_id
    ))
}

/// 补保证金请求发出之后的结局：交易所明确拒绝（业务码）没动钱；传输失败、超时、5xx、「稍后重试」类的码
/// 都可能已经到账。
fn margin_outcome(result: Result<Box<RawValue>, WriteError>) -> MarginOutcome {
    match result {
        Ok(_) => MarginOutcome::Applied,
        Err(WriteError::Api { message, .. }) => MarginOutcome::Refused(message),
        Err(WriteError::Unknown(error) | WriteError::Throttled(error)) => MarginOutcome::Unknown(
            format!("{error}; the request may have arrived and the margin may have been added"),
        ),
    }
}

fn classify_write(status: StatusCode, bytes: &[u8]) -> Result<Box<RawValue>, WriteError> {
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(WriteError::Throttled(err(format!(
            "HTTP {status}; rate limited; reconcile intent"
        ))));
    }
    if status.is_server_error() {
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
    if envelope.code == CODE_TOO_FREQUENT {
        return Err(WriteError::Throttled(err(message)));
    }
    if AMBIGUOUS_CODES.contains(&envelope.code) {
        return Err(WriteError::Unknown(err(message)));
    }
    Err(WriteError::Api {
        code: envelope.code,
        message,
    })
}

/// 文档明确列为「下单前拒绝」的业务码
/// （<https://www.mexc.com/api-docs/futures/error-code>）：2001 订单方向错误、2005 余额不足、
/// 2011 下单数量错误、2015 价格/数量精度错误、2028 超过单笔最大数量、
/// 2070 价格与 bid1/ask1 距离超过 5%。其余码要靠读回确认没建单（见 [`settle_failure`]）。
fn definitive_rejection(code: i64) -> bool {
    matches!(code, 2001 | 2005 | 2011 | 2015 | 2028 | 2070)
}

/// 提交失败后的统一收尾（`place` 调用）：明确的业务拒单，或业务码报错且按外部单号
/// 每次都查无此单（`absent`，交易所建过的单哪怕 0 成交撤掉也查得到），记终态 `Rejected`，
/// 之后 `order_state` 如实返回、下一轮可以重发；其余情况意图保持未决等待对账。
/// 某次实盘：平仓单被 2001 拒、不在白名单里，意图未决，规则每轮「上一张退出单读不到、
/// 本轮不重发」卡住。
fn settle_failure(
    journal: &mut OrderJournal,
    order: &NewOrder,
    code: Option<i64>,
    absent: bool,
    message: String,
) -> ArbResult<ArbError> {
    if let Some(code) = code
        && (definitive_rejection(code) || absent)
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
/// One-way mode: buy 1, sell 3; reduce-only travels in `reduceOnly` (see the module docs).
fn side_code(side: Side) -> i64 {
    match side {
        Side::Buy => 1,
        Side::Sell => 3,
    }
}

/// Direction of an order/deal side code; 2/4 are the hedge-mode closes.
fn side_of(code: i64) -> ArbResult<Side> {
    match code {
        1 | 2 => Ok(Side::Buy),
        3 | 4 => Ok(Side::Sell),
        _ => Err(err("unknown order side")),
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
/// `symbol` 是合约的标准标的（[`Catalog::symbol_of`]）；`None` = 不支持的合约。
fn verified_order(
    row: &RemoteOrder,
    entry: Option<&JournalEntry>,
    c: &Contract,
    symbol: Option<&Symbol>,
) -> ArbResult<NewOrder> {
    validate_id(&row.order_id)?;
    if row.symbol != c.symbol {
        return Err(err("order contract mismatch"));
    }
    let symbol = symbol
        .cloned()
        .ok_or_else(|| err("unsupported order contract"))?;
    let side = side_of(row.side)?;
    if let Some(e) = entry {
        if e.instrument != row.symbol
            || e.venue_client_id != row.external_oid
            || e.units != row.vol.0
            || e.order.symbol != symbol
            || side != e.order.side
            || row.open_type != if e.order.margin_mode.is_cross() { 2 } else { 1 }
            || row.position_mode != 2
            || row.order_type != 3
            // 单向持仓下买卖都只回 1/3，只减仓看 reduceOnly：两个单笔查询端点文档示例没有
            // 这个字段，但 10-22 实测都会返回（开仓 false、平仓 true）。缺失按未知接受，
            // 明确返回且与意图不符才是冲突；2/4 只可能是平仓。
            || row.reduce_only.is_some_and(|reduce| reduce != e.order.reduce_only)
            || (matches!(row.side, 2 | 4) && !e.order.reduce_only)
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
        post_only: false,
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
            // 平仓单的成交可能记成 1/3 也可能记成 2/4：只核方向。
            || side_of(fill.side)? != side_of(row.side)?
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

/// 这一边（`kind`：1 多 / 2 空）恰好有一行、是这个保证金模式（`open_type`：1 逐仓 / 2 全仓）和杠杆。
fn leverage_matches(rows: &[Leverage], kind: i64, open_type: i64, leverage: Decimal) -> bool {
    let mut side = rows.iter().filter(|row| row.position_type == kind);
    match (side.next(), side.next()) {
        (Some(row), None) => row.open_type == open_type && row.leverage.0 == leverage,
        _ => false,
    }
}

fn millis(at: i64) -> ArbResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(at).ok_or_else(|| err("invalid timestamp"))
}

/// 一条资金费流水：（id, 结算时间, 金额）。
type FundingEntry = (String, DateTime<Utc>, Decimal);

/// 一页资金费流水与是否读完。按结算时间从新到旧排时，这一页最早的已早于 `start`
/// 就不必再往后翻；顺序对不上就一直翻到最后一页。
fn funding_page(
    reply: &FundingPage,
    native: &str,
    start: i64,
    page: usize,
) -> ArbResult<(Vec<FundingEntry>, bool)> {
    let mut rows = Vec::with_capacity(reply.result_list.len());
    for record in &reply.result_list {
        if record.symbol != native {
            return Err(err("funding record for another contract"));
        }
        rows.push((
            record.id.clone(),
            millis(record.settle_time)?,
            record.funding.0,
        ));
    }
    let times: Vec<i64> = reply.result_list.iter().map(|r| r.settle_time).collect();
    let newest_first = times.windows(2).all(|pair| pair[0] >= pair[1]);
    let done = reply.result_list.is_empty()
        || page as i64 >= reply.total_page
        || (newest_first && times.last().is_some_and(|oldest| *oldest < start));
    Ok((rows, done))
}

/// 成交列表的一行：数量是张数 × 面值，手续费正 = 付出（返佣为负），side 1 开多 / 2 平空 / 3 开空 / 4 平多。
fn venue_fill(deal: &Deal, contract: &Contract) -> ArbResult<VenueFill> {
    if deal.symbol != contract.symbol
        || deal.fee_currency != "USDT"
        || deal.vol.0 <= Decimal::ZERO
        || deal.price.0 <= Decimal::ZERO
    {
        return Err(err("invalid/foreign-currency deal"));
    }
    // 单向持仓下 1/3 既可能开仓也可能是只减仓的平仓（MEXC 把平仓单记成 1/3 +
    // reduceOnly），只有 2/4 确定是平仓。
    let (side, effect) = match deal.side {
        1 => (Side::Buy, FillEffect::Unknown),
        2 => (Side::Buy, FillEffect::Close),
        3 => (Side::Sell, FillEffect::Unknown),
        4 => (Side::Sell, FillEffect::Close),
        _ => return Err(err("unknown deal side")),
    };
    Ok(VenueFill {
        at: millis(
            deal.timestamp
                .ok_or_else(|| err("deal without timestamp"))?,
        )?,
        side,
        quantity: multiply(deal.vol.0, contract.contract_size.0)?,
        price: deal.price.0,
        fee_usdt: deal.fee.0,
        effect,
    })
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
#[derive(Clone, Copy, Debug, PartialEq)]
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

#[derive(Clone, Deserialize)]
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
    /// MX 抵扣手续费开着（2026-10-21 实测：网页上关掉后由 true 变 false；`joinDeduct` 是资格，不变）。
    #[serde(default)]
    enjoy_deduct: Option<bool>,
}

/// MX 抵扣开着就不连：手续费按 MX 扣时成交明细的 `feeCurrency` 不是 USDT，单子成交了却记不进账，
/// 这条腿悬着（`aggregate` 只认 USDT）。字段没给不拦（成交那一步仍会拒非 USDT 的手续费）。
fn check_deduction(fees: &Fees) -> ArbResult<()> {
    if fees.enjoy_deduct == Some(true) {
        return Err(err(
            "MX fee deduction is on: turn it off in the MEXC UI (fees paid in MX cannot be booked)",
        ));
    }
    Ok(())
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
    hold_avg_price: Derived,
    #[serde(default)]
    liquidate_price: Option<Derived>,
    #[serde(default)]
    im: Option<Derived>,
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
    /// 交易所算出来的手续费：派生数值，见 [`Derived`]。
    fee: Derived,
    fee_currency: String,
    /// 成交时刻（毫秒）。按订单查的明细不一定带，按合约查的成交列表带。
    #[serde(default)]
    timestamp: Option<i64>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Asset {
    currency: String,
    available_open: Derived,
}
/// 交易所算出来的派生数值（余额、持仓均价、保证金、强平价）：MEXC 会给出 40 位有效数字（实测
/// `availableOpen` 为 `412.3456789012345000000000000000029946341`），超出 `Decimal` 的 28 位，[`Number`]
/// 的精确解析会拒 —— 持仓读不出来，整家对账就失败。小数截到 18 位再解析（余额只会少算一点点）。
/// 下单数量、价格、成交这些原始数值仍用 [`Number`] 精确解析。
#[derive(Clone, Copy, Debug, PartialEq)]
struct Derived(Decimal);
impl<'de> Deserialize<'de> for Derived {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(de)?;
        let text = raw.get().trim_matches('"');
        let cut = match text.split_once('.') {
            Some((whole, fraction)) if fraction.len() > 18 => {
                format!("{whole}.{}", &fraction[..18])
            }
            _ => text.to_string(),
        };
        Decimal::from_str_exact(&cut)
            .map(Self)
            .map_err(|_| serde::de::Error::custom("invalid derived decimal"))
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingPage {
    #[serde(default)]
    result_list: Vec<FundingRecord>,
    total_page: i64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRecord {
    #[serde(deserialize_with = "id")]
    id: String,
    symbol: String,
    /// 正 = 收到，负 = 付出（实测 某次实盘 多仓、费率 +0.0093% 时为 −1.108）。派生数值，见 [`Derived`]。
    funding: Derived,
    settle_time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order() -> NewOrder {
        NewOrder {
            post_only: false,
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
    /// 报给执行器的步长（面值 × volUnit）和下单换算（[`order_units`]）是同一份规格：步长整数倍的数量原样下出去。
    #[test]
    fn a_quantity_on_the_reported_step_goes_out_unchanged() {
        let nvda = nvidia();
        let step = quantity_step_of(&nvda).unwrap();
        assert_eq!(step, dec!(0.01));
        let mut order = order();
        order.quantity = Some(step * Decimal::from(183));
        let units = order_units(
            Venue::Mexc,
            &order,
            dec!(218.95),
            nvda.contract_size.0,
            nvda.vol_unit.0,
            nvda.min_vol.0,
        )
        .unwrap();
        assert_eq!(units * nvda.contract_size.0, dec!(1.83));
        let mut lots = nvda;
        lots.vol_unit = Number(dec!(10));
        assert_eq!(quantity_step_of(&lots).unwrap(), dec!(0.1));
    }

    fn position(json: &str) -> Position {
        decode(json).unwrap()
    }

    /// 补保证金只对这个合约上那一笔正常持有的逐仓仓位发；没有、不是逐仓、不正常、金额不合法都不发（`Err`）。
    #[test]
    fn margin_goes_only_to_the_one_isolated_position_held() {
        let isolated = r#"{"positionId":4242,"symbol":"NVIDIA_USDT","positionType":2,"openType":1,"state":1,"holdVol":9,"frozenVol":0,"holdAvgPrice":218.95}"#;
        let other = r#"{"positionId":7,"symbol":"BTC_USDT","positionType":1,"openType":1,"state":1,"holdVol":3,"frozenVol":0,"holdAvgPrice":80000}"#;
        let rows = vec![position(other), position(isolated)];
        assert_eq!(
            add_margin_body(&rows, "NVIDIA_USDT", dec!(12.5)).unwrap(),
            r#"{"positionId":4242,"amount":12.5,"type":"ADD"}"#
        );
        assert!(
            add_margin_body(&rows, "TESLA_USDT", dec!(10)).is_err(),
            "没有持仓"
        );
        assert!(add_margin_body(&rows, "NVIDIA_USDT", dec!(0)).is_err());
        assert!(
            add_margin_body(&rows, "NVIDIA_USDT", dec!(1.234)).is_err(),
            "最多两位小数"
        );
        let cross = position(&isolated.replace(r#""openType":1"#, r#""openType":2"#));
        assert!(
            add_margin_body(&[cross], "NVIDIA_USDT", dec!(10)).is_err(),
            "全仓不补"
        );
        let closing = position(&isolated.replace(r#""state":1"#, r#""state":2"#));
        assert!(add_margin_body(&[closing], "NVIDIA_USDT", dec!(10)).is_err());
        let twice = vec![position(isolated), position(isolated)];
        assert!(add_margin_body(&twice, "NVIDIA_USDT", dec!(10)).is_err());
    }

    /// 补保证金发出之后：成功是 Applied；交易所明确的业务码是 Refused（没动钱）；5xx、限频、「稍后重试」类码、
    /// 解析不了的都是 Unknown（可能已经到账，不能当拒绝）。
    #[test]
    fn margin_replies_separate_refused_from_maybe_applied() {
        let outcome = |status: u16, body: &str| {
            margin_outcome(classify_write(
                StatusCode::from_u16(status).unwrap(),
                body.as_bytes(),
            ))
        };
        assert_eq!(
            outcome(200, r#"{"success":true,"code":0}"#),
            MarginOutcome::Applied
        );
        assert!(matches!(
            outcome(200, r#"{"success":false,"code":2005}"#),
            MarginOutcome::Refused(_)
        ));
        for (status, body) in [
            (200, r#"{"success":false,"code":510}"#),
            (500, r#"{"success":false,"code":0}"#),
            (429, ""),
            (200, "not json"),
        ] {
            assert!(
                matches!(outcome(status, body), MarginOutcome::Unknown(_)),
                "{status} {body}"
            );
        }
    }

    /// 公开盘口快照本来就落后几秒（实测到 5.4 秒）：15 秒内都能用来定保护价；更旧、在未来、请求太慢的不用。
    #[test]
    fn depth_snapshots_a_few_seconds_old_are_still_usable() {
        let now = 1_793_008_500_000;
        let fast = Duration::from_millis(30);
        assert!(book_fresh(now, now - 5_400, fast), "实测落后 5.4 秒的快照");
        assert!(book_fresh(now, now - 15_000, fast));
        assert!(!book_fresh(now, now - 15_001, fast));
        assert!(!book_fresh(now, now + 1_001, fast), "时间戳在未来");
        assert!(
            !book_fresh(now, now, Duration::from_millis(2_001)),
            "请求本身太慢"
        );
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
        // 10-22 实测：单向持仓下 side 2 + reduceOnly 被拒（2001），平仓 = 1/3 + reduceOnly。
        assert_eq!((side_code(Side::Buy), side_code(Side::Sell)), (1, 3));
        assert_eq!(
            [1, 2, 3, 4].map(|code| side_of(code).unwrap()),
            [Side::Buy, Side::Buy, Side::Sell, Side::Sell]
        );
        assert!(side_of(5).is_err());
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
            verified_order(
                &row,
                Some(&entry),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT"))
            )
            .unwrap()
            .client_order_id,
            entry.order.client_order_id
        );
        let mut cross = remote();
        cross.open_type = 2;
        assert!(
            verified_order(
                &cross,
                Some(&entry),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT"))
            )
            .is_err()
        );
        let mut mismatch = remote();
        mismatch.external_oid = "other".into();
        assert!(
            verified_order(
                &mismatch,
                Some(&entry),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT"))
            )
            .is_err()
        );
        let external =
            verified_order(&row, None, &contract(), Some(&Symbol::perp("BTC", "USDT"))).unwrap();
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
        // 2001：10-22 实测（单向持仓 side 2 + reduceOnly）回包后按外部单号查无此单。
        for code in [2001, 2011, 2015, 2028, 2070] {
            assert!(definitive_rejection(code));
        }
        assert!(!definitive_rejection(2009));
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
            false,
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
                Err(WriteError::Unknown(_) | WriteError::Throttled(_)) => {}
                Err(WriteError::Api { code, .. }) => panic!("{code} must stay unknown"),
                Ok(_) => panic!("HTTP {status} error body must not be accepted"),
            }
        }
        // 限频单独归一类：下单路径要靠读回「每次都查无此单」才敢记拒单，5xx 永远不记。
        for (status, body) in [
            (
                StatusCode::TOO_MANY_REQUESTS,
                &br#"{"success":false,"code":510}"#[..],
            ),
            (
                StatusCode::OK,
                &br#"{"success":false,"code":510,"data":null}"#[..],
            ),
        ] {
            assert!(matches!(
                classify_write(status, body),
                Err(WriteError::Throttled(_))
            ));
        }
        assert!(matches!(
            classify_write(
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"success":false,"code":500}"#
            ),
            Err(WriteError::Unknown(_))
        ));
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
            true,
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

    /// 某次实盘 平仓单：业务码不在白名单、但按外部单号每次都查无此单 = 交易所没建单，
    /// 记成拒单让下一轮重发；读回出过错（`absent` = false）就不能下结论。
    #[test]
    fn a_refused_order_the_venue_has_no_record_of_is_settled_as_rejected() {
        assert!(definitive_rejection(2001), "order direction error");
        for (absent, settled) in [(true, true), (false, false)] {
            let (mut journal, path) = test_journal(if absent { "absent" } else { "unread" });
            let order = order();
            journal.reserve(test_entry(&order)).unwrap();
            settle_failure(
                &mut journal,
                &order,
                Some(9999),
                absent,
                "HTTP 200 OK; API code 9999".into(),
            )
            .unwrap();
            let terminal = journal
                .get(&order.client_order_id)
                .unwrap()
                .terminal
                .clone();
            assert_eq!(
                terminal.map(|state| state.status),
                settled.then_some(OrderStatus::Rejected),
                "absent={absent}"
            );
            drop(journal);
            let _ = std::fs::remove_file(path);
        }
    }

    /// 单向持仓下平仓单回的是 1/3 + reduceOnly:true（10-22 实测 diagD 的原样字段）。
    #[test]
    fn a_one_way_close_reads_back_as_side_one_or_three_with_reduce_only() {
        let row = |side: i64, reduce: &str| -> RemoteOrder {
            decode(&format!(
                r#"{{"orderId":"100000000000000001","symbol":"BTC_USDT","externalOid":"abc","side":{side},"state":4,"errorCode":18,"vol":2,"dealVol":0,"price":100,"leverage":2,"openType":1,"positionMode":2,"orderType":3{reduce}}}"#
            ))
            .unwrap()
        };
        let check = |intent: &NewOrder, remote: &RemoteOrder| {
            verified_order(
                remote,
                Some(&test_entry(intent)),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT")),
            )
        };
        let mut close = order();
        close.side = Side::Buy;
        close.reduce_only = true;
        let mut open = close.clone();
        open.reduce_only = false;
        let reduce = r#","reduceOnly":true"#;
        assert!(check(&close, &row(1, reduce)).is_ok());
        assert!(check(&close, &row(2, "")).is_ok());
        assert!(check(&open, &row(1, r#","reduceOnly":false"#)).is_ok());
        assert!(
            check(&open, &row(1, reduce)).is_err(),
            "an open must not read back reduce-only"
        );
        assert!(
            check(&open, &row(2, "")).is_err(),
            "2/4 is only ever a close"
        );
        assert!(check(&close, &row(3, reduce)).is_err(), "wrong direction");
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
            verified_order(
                &missing,
                Some(&entry),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT"))
            )
            .unwrap()
            .reduce_only
        );
        let explicit_false: RemoteOrder = decode(
            r#"{"orderId":"10","symbol":"BTC_USDT","externalOid":"abc","side":2,"state":3,"errorCode":0,"vol":2,"dealVol":2,"price":100,"leverage":5,"openType":1,"positionMode":2,"orderType":3,"reduceOnly":false}"#,
        )
        .unwrap();
        assert!(
            verified_order(
                &explicit_false,
                Some(&entry),
                &contract(),
                Some(&Symbol::perp("BTC", "USDT"))
            )
            .is_err()
        );
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

    fn nvidia() -> Contract {
        decode(r#"{"symbol":"NVIDIA_USDT","baseCoin":"NVIDIA","quoteCoin":"USDT","settleCoin":"USDT","futureType":1,"state":0,"apiAllowed":true,"positionOpenType":3,"contractSize":0.01,"priceUnit":0.01,"volUnit":1,"minVol":1,"maxVol":3000000,"minLeverage":1,"maxLeverage":200,"conceptPlate":["mc-trade-zone-Stock","mc-trade-zone-tradfi","mc-trade-zone-usstocks"]}"#).unwrap()
    }

    fn catalog(rows: Vec<Contract>) -> Catalog {
        let names = arb_venues::mexc::canonical_bases(rows.iter().map(|c| {
            let display = if c.symbol == "NVIDIA_USDT" {
                "NVDA_USDT PERPETUAL"
            } else {
                "X"
            };
            (c.symbol.as_str(), Some(display), c.symbol == "NVIDIA_USDT")
        }));
        Catalog {
            contracts: rows.into_iter().map(|c| (c.symbol.clone(), c)).collect(),
            names,
        }
    }

    #[test]
    fn stock_contracts_trade_and_report_under_the_scanner_ticker() {
        let book = catalog(vec![nvidia(), contract()]);
        let nvda = Symbol::perp("NVDA", "USDT");
        assert_eq!(book.symbol_of("NVIDIA_USDT"), Some(nvda.clone()));
        assert_eq!(book.find(&nvda).unwrap().unwrap().symbol, "NVIDIA_USDT");
        assert!(
            book.find(&Symbol::perp("NVIDIA", "USDT"))
                .unwrap()
                .is_none(),
            "原名不再是标的"
        );
        assert_eq!(
            book.symbol_of("BTC_USDT"),
            Some(Symbol::perp("BTC", "USDT"))
        );
        // 合约名和 baseCoin 对不上（倍数合约之类）：不认。
        let mut odd = contract();
        odd.symbol = "1000BTC_USDT".into();
        let book = catalog(vec![odd]);
        assert_eq!(book.symbol_of("1000BTC_USDT"), None);
    }

    #[test]
    fn leverage_counts_only_when_this_side_has_exactly_that_mode_and_value() {
        // 2026-10-21 实测：每个合约多空各一行，openType 2 = 全仓。
        let rows: Vec<Leverage> = decode(r#"[{"currentMmr":0.004,"imr":0.1,"level":4,"leverage":10,"limitBySys":false,"maxLeverageView":200,"maxVol":3000000,"mmr":0.05,"openType":2,"positionType":1},{"currentMmr":0.004,"imr":0.1,"level":4,"leverage":3,"limitBySys":false,"maxLeverageView":200,"maxVol":3000000,"mmr":0.05,"openType":1,"positionType":2}]"#).unwrap();
        assert!(leverage_matches(&rows, 1, 2, dec!(10)));
        assert!(!leverage_matches(&rows, 1, 1, dec!(10)), "全仓不算逐仓");
        assert!(!leverage_matches(&rows, 1, 2, dec!(3)));
        assert!(leverage_matches(&rows, 2, 1, dec!(3)));
        let twice: Vec<Leverage> = decode(r#"[{"leverage":3,"openType":1,"positionType":2},{"leverage":3,"openType":1,"positionType":2}]"#).unwrap();
        assert!(!leverage_matches(&twice, 2, 1, dec!(3)), "同一边两行：不认");
        assert!(!leverage_matches(&[], 1, 1, dec!(3)));
    }

    #[test]
    fn funding_pages_keep_the_sign_and_stop_once_older_than_the_start() {
        // 2026-10-21 实测的一行（BTC 多仓付出资金费）。
        let page: FundingPage = decode(r#"{"currentPage":1,"pageSize":20,"resultList":[{"funding":-1.10000000002,"id":1951695009,"positionType":1,"positionValue":11917.65765,"rate":0.000093,"settleTime":1792454400000,"symbol":"BTC_USDT"}],"totalCount":1,"totalPage":1}"#).unwrap();
        let (rows, done) = funding_page(&page, "BTC_USDT", 0, 1).unwrap();
        assert_eq!(rows[0].2, dec!(-1.10000000002), "负 = 付出");
        assert_eq!(rows[0].1.timestamp_millis(), 1792454400000);
        assert!(done, "最后一页");
        assert!(
            funding_page(&page, "ETH_USDT", 0, 1).is_err(),
            "别的合约的流水"
        );
        // 还有下一页：从新到旧排、这一页最早的已早于起点才停。
        let two: FundingPage = decode(r#"{"resultList":[{"funding":1,"id":2,"settleTime":2000,"symbol":"BTC_USDT"},{"funding":1,"id":1,"settleTime":1000,"symbol":"BTC_USDT"}],"totalPage":5}"#).unwrap();
        assert!(!funding_page(&two, "BTC_USDT", 500, 1).unwrap().1);
        assert!(funding_page(&two, "BTC_USDT", 1500, 1).unwrap().1);
        let unordered: FundingPage = decode(r#"{"resultList":[{"funding":1,"id":1,"settleTime":1000,"symbol":"BTC_USDT"},{"funding":1,"id":2,"settleTime":2000,"symbol":"BTC_USDT"}],"totalPage":5}"#).unwrap();
        assert!(
            !funding_page(&unordered, "BTC_USDT", 1500, 1).unwrap().1,
            "顺序对不上就翻到最后一页"
        );
        let empty: FundingPage = decode(
            r#"{"currentPage":1,"pageSize":3,"resultList":[],"totalCount":0,"totalPage":0}"#,
        )
        .unwrap();
        assert!(funding_page(&empty, "BTC_USDT", 0, 1).unwrap().1);
    }

    #[test]
    fn the_usdt_asset_reads_the_amount_available_to_open() {
        // 2026-10-21 实测 `/account/asset/USDT`。
        let raw = r#"{"availableBalance":512.34567890123,"availableCash":512.3456789012345,"availableOpen":512.3456789012345,"bonus":0,"cashBalance":512.3456789,"contributeMarginAmount":0,"currency":"USDT","debtAmount":0,"equity":1000.0,"frozenBalance":0,"positionMargin":400.0,"unrealized":10.0,"vcoinId":"00000000000000000000000000000001"}"#;
        let asset: Asset = serde_json::from_str(raw).unwrap();
        assert_eq!(asset.currency, "USDT");
        assert_eq!(asset.available_open.0, dec!(512.3456789012345));
        // 同一个接口几分钟后的原文：40 位有效数字。截到 18 位小数，只会少算。
        let long =
            r#"{"currency":"USDT","availableOpen":412.3456789012345000000000000000029946341}"#;
        let asset: Asset = serde_json::from_str(long).unwrap();
        assert_eq!(asset.available_open.0, dec!(412.345678901234500000));
        assert!(
            serde_json::from_str::<Asset>(r#"{"currency":"USDT","availableOpen":"x"}"#).is_err()
        );
        // 持仓（2026-10-21 实测的一行，均价、保证金换成超长位数）：照样读得出来，不让整家对账失败。
        let position: Vec<Position> = decode(r#"[{"autoAddIm":false,"holdAvgPrice":5.3690000000000000000000000000000012345,"holdVol":1130,"frozenVol":0,"im":409.1237880000000000000000000000000001,"leverage":10,"liquidatePrice":4.308,"openType":2,"positionId":1000000001,"positionType":1,"state":1,"symbol":"NEAR_USDT"}]"#).unwrap();
        assert_eq!(position[0].hold_avg_price.0, dec!(5.369));
        assert_eq!(position[0].im.unwrap().0, dec!(409.123788));
        assert!(validate_positions(&position).is_ok());
    }

    /// 目录刷新失败（这里用连不上的代理模拟限频 / 超时）：规格与持仓映射退回一小时内的上一份，
    /// 停牌判断不退（要新鲜状态，读不到就报错）。
    #[tokio::test]
    async fn a_failed_catalog_refresh_falls_back_but_halts_do_not() {
        let (journal, path) = test_journal("catalog-fallback");
        let offline = Client::builder()
            .proxy(reqwest::Proxy::all("http://127.0.0.1:9").unwrap())
            .build()
            .unwrap();
        let broker = MexcBroker {
            client: offline,
            credentials: MexcCredentials {
                api_key: "k".into(),
                api_secret: "s".into(),
            },
            options: LiveOptions {
                trading_enabled: false,
                market_slippage: None,
            },
            journal: Mutex::new(journal),
            clock_offset: 0,
            fee: Decimal::ZERO,
            catalog: Cached::new(),
            mode_ok: Verified::new(SETTINGS_TTL),
            leverage_ok: Verified::new(SETTINGS_TTL),
        };
        let nvda = Symbol::perp("NVDA", "USDT");
        assert!(broker.find(&nvda).await.is_err(), "没有上一份：报错");
        broker
            .catalog
            .put(Arc::new(catalog(vec![nvidia(), contract()])));
        // 上一份已过 10 分钟的有效期也照样用（这里直接要求重读）。
        assert_eq!(
            broker
                .catalog_lenient(Duration::ZERO)
                .await
                .unwrap()
                .contracts
                .len(),
            2
        );
        assert_eq!(broker.find(&nvda).await.unwrap().symbol, "NVIDIA_USDT");
        let (_, symbol) = broker.contract("NVIDIA_USDT").await.unwrap();
        assert_eq!(symbol, Some(nvda.clone()));
        assert!(broker.contract("NOPE_USDT").await.is_err());
        assert!(
            broker.catalog(Duration::ZERO).await.is_err(),
            "严格读不退：停牌判断用它"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn mx_fee_deduction_must_be_off_to_connect() {
        // 2026-10-21 同一账户关抵扣前后的实测回包（节选）。
        let on: Fees = decode(r#"{"joinDeduct":true,"enjoyDeduct":true,"deductRate":0.8,"originalTakerFee":0.0008,"realTakerFee":0.00064}"#).unwrap();
        let off: Fees = decode(r#"{"joinDeduct":true,"enjoyDeduct":false,"deductRate":0.8,"originalTakerFee":0.0008,"realTakerFee":0.0008}"#).unwrap();
        assert!(check_deduction(&on).is_err());
        assert!(check_deduction(&off).is_ok());
        let unknown: Fees = decode(r#"{"realTakerFee":0.0008}"#).unwrap();
        assert!(check_deduction(&unknown).is_ok(), "没给不拦");
    }

    #[test]
    fn deal_list_rows_become_fills_in_base_units() {
        // 2026-10-21 实测（BTC 平多，挂单成交）。
        let deal: Deal = decode(r#"{"category":1,"fee":1.18654,"feeCurrency":"USDT","id":"10000000001","orderId":"100000000000000002","positionMode":1,"price":82000,"profit":26.66821,"side":4,"symbol":"BTC_USDT","taker":false,"timestamp":1792467757000,"vol":1447}"#).unwrap();
        let mut btc = contract();
        btc.contract_size = Number(dec!(0.0001));
        let fill = venue_fill(&deal, &btc).unwrap();
        assert_eq!(fill.side, Side::Sell);
        assert_eq!(fill.effect, FillEffect::Close);
        assert_eq!(fill.quantity, dec!(0.1447));
        assert_eq!(fill.price, dec!(82000));
        assert_eq!(fill.fee_usdt, dec!(1.18654), "正 = 付出");
        assert_eq!(fill.at.timestamp_millis(), 1792467757000);
        let mut foreign: Deal = decode(r#"{"fee":1,"feeCurrency":"MX","id":"1","orderId":"2","price":1,"side":1,"symbol":"BTC_USDT","timestamp":1,"vol":1}"#).unwrap();
        assert!(
            venue_fill(&foreign, &btc).is_err(),
            "MX 抵扣的手续费不能当 USDT"
        );
        foreign.fee_currency = "USDT".into();
        foreign.timestamp = None;
        assert!(venue_fill(&foreign, &btc).is_err(), "没有时间");
        foreign.timestamp = Some(1);
        let one_way = venue_fill(&foreign, &btc).unwrap();
        assert_eq!(
            (one_way.side, one_way.effect),
            (Side::Buy, FillEffect::Unknown),
            "单向持仓下 1/3 可能是只减仓的平仓"
        );
        foreign.symbol = "ETH_USDT".into();
        assert!(venue_fill(&foreign, &btc).is_err(), "别的合约");
    }

    /// Read-only probe of the real account in `ARB_MEXC_API_KEY` / `ARB_MEXC_API_SECRET` (from the
    /// environment or, failing that, the repo `.env`): trading disabled, so no write request is
    /// possible. Runs the very `connect` the live desk runs (all-or-nothing: if it fails here the
    /// whole live desk would fail to connect), then prints the raw shape of every read the bot
    /// will rely on (no response carries a secret). Journal in `ARB_PROBE_DIR` (default: the
    /// system temp dir). Run with `cargo test -p arb-exec --lib
    /// mexc_broker::tests::live_read_only_probe -- --ignored --nocapture`.
    /// **写操作，动真钱**：往一笔已有的逐仓持仓补保证金，前后读回 `leg_state`（保证金 im、强平价）。
    /// 只在显式给了 `ARB_PROBE_ADD_MARGIN=合约:金额`（如 `NVDA:1`）时才发请求；只给补保证金这一件事开写权限。
    /// `cargo test -p arb-exec --lib mexc_broker::tests::live_add_margin_probe -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "moves real money on MEXC"]
    async fn live_add_margin_probe() {
        let _ = dotenvy::dotenv();
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let Some(target) = var("ARB_PROBE_ADD_MARGIN") else {
            println!("ARB_PROBE_ADD_MARGIN 没给：什么都不做");
            return;
        };
        let (base, amount) = target.split_once(':').expect("格式 合约:金额");
        let amount: Decimal = amount.parse().expect("金额");
        let symbol = Symbol::perp(base, "USDT");
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let broker = MexcBroker::connect(
            Client::new(),
            MexcCredentials {
                api_key: var("ARB_MEXC_API_KEY").expect("ARB_MEXC_API_KEY"),
                api_secret: var("ARB_MEXC_API_SECRET").expect("ARB_MEXC_API_SECRET"),
            },
            &dir.join("arb-live-mexc-margin-probe-orders.jsonl"),
            LiveOptions {
                trading_enabled: true,
                market_slippage: Some(dec!(0.01)),
            },
        )
        .await
        .expect("connect");
        let before = broker.leg_state(&symbol).await.expect("leg_state before");
        println!("before: {before:?}");
        let outcome = broker.add_margin(&symbol, amount).await;
        println!("add_margin({amount}): {outcome:?}");
        for wait in [0u64, 1000, 3000] {
            tokio::time::sleep(Duration::from_millis(wait)).await;
            let after = broker.leg_state(&symbol).await.expect("leg_state after");
            println!("after +{wait}ms: {after:?}");
        }
    }

    #[tokio::test]
    #[ignore = "needs live MEXC credentials"]
    async fn live_read_only_probe() {
        let _ = dotenvy::dotenv();
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let credentials = || MexcCredentials {
            api_key: var("ARB_MEXC_API_KEY").expect("ARB_MEXC_API_KEY"),
            api_secret: var("ARB_MEXC_API_SECRET").expect("ARB_MEXC_API_SECRET"),
        };
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let options = || LiveOptions {
            trading_enabled: false,
            market_slippage: None,
        };
        let client = Client::new();
        let started = Instant::now();
        let connected = MexcBroker::connect(
            client.clone(),
            credentials(),
            &dir.join("arb-live-mexc-probe-orders.jsonl"),
            options(),
        )
        .await;
        // Printed reads continue after a failed connect (they help find why); the test itself
        // fails at the end, so `test result: ok` always means the live desk would connect.
        let connect_failure = connected.as_ref().err().map(ToString::to_string);
        match &connected {
            Ok(broker) => println!(
                "connect (what the live desk runs): OK, clock offset {} ms, account taker fee {} ({} ms)",
                broker.clock_offset,
                broker.fee,
                started.elapsed().as_millis()
            ),
            Err(failure) => println!(
                "connect (what the live desk runs): FAILED — the live desk would not connect: {failure} ({} ms)",
                started.elapsed().as_millis()
            ),
        }
        // ARB_PROBE_FEES=NVIDIA_USDT,BTC_USDT：按合约读账户实际费率（公开详情里的 takerFeeRate 不可信）。
        if let (Ok(broker), Some(list)) = (&connected, var("ARB_PROBE_FEES")) {
            for native in list.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                match broker
                    .get::<serde_json::Value>(FEES, &[("symbol", native.to_string())])
                    .await
                {
                    Ok(value) => println!("fees {native}: {value}"),
                    Err(error) => println!("fees {native}: FAILED {error}"),
                }
            }
        }
        let broker = match connected {
            Ok(broker) => broker,
            Err(_) => MexcBroker {
                client,
                credentials: credentials(),
                options: options(),
                journal: Mutex::new(
                    OrderJournal::open(
                        &dir.join("arb-live-mexc-probe-raw.jsonl"),
                        "mexc-probe-raw",
                    )
                    .unwrap(),
                ),
                clock_offset: 0,
                fee: Decimal::ZERO,
                catalog: Cached::new(),
                mode_ok: Verified::new(SETTINGS_TTL),
                leverage_ok: Verified::new(SETTINGS_TTL),
            },
        };
        let raw = |path: &'static str, params: Vec<(&'static str, String)>| {
            let broker = &broker;
            async move {
                let started = Instant::now();
                // 原文（不经 f64）：数字的位数原样可见。
                let reply: ArbResult<Box<RawValue>> = broker.get(path, &params).await;
                let text = match reply {
                    Ok(value) => value.get().to_string(),
                    Err(failure) => format!("ERROR {failure}"),
                };
                let cut: String = text.chars().take(900).collect();
                println!(
                    "{path} {params:?} ({} ms): {cut}",
                    started.elapsed().as_millis()
                );
            }
        };
        let now = Utc::now().timestamp_millis();
        let day = 86_400_000i64;
        raw(POSITION_MODE, vec![]).await;
        raw(FEES, vec![]).await;
        raw("/api/v1/private/account/assets", vec![]).await;
        raw("/api/v1/private/account/asset/USDT", vec![]).await;
        raw(POSITIONS, vec![]).await;
        raw(
            OPEN_ORDERS,
            vec![("page_num", "1".into()), ("page_size", "20".into())],
        )
        .await;
        raw(LEVERAGE, vec![("symbol", "NVIDIA_USDT".into())]).await;
        raw(
            "/api/v1/private/position/funding_records",
            vec![("page_num", "1".into()), ("page_size", "20".into())],
        )
        .await;
        raw(
            "/api/v1/private/order/list/order_deals",
            vec![
                ("symbol", "NVIDIA_USDT".into()),
                ("start_time", (now - 7 * day).to_string()),
                ("end_time", now.to_string()),
                ("page_num", "1".into()),
                ("page_size", "20".into()),
            ],
        )
        .await;
        raw(
            "/api/v1/private/order/list/history_orders",
            vec![("page_num", "1".into()), ("page_size", "5".into())],
        )
        .await;
        raw(CONTRACTS, vec![("symbol", "NVIDIA_USDT".into())]).await;
        // 账户费率按合约查（不带 symbol 回的是某个默认合约的）。
        for symbol in ["NVIDIA_USDT", "BTC_USDT", "NEAR_USDT", "AAPLSTOCK_USDT"] {
            raw(FEES, vec![("symbol", symbol.into())]).await;
        }
        // 成交查询的时间窗口上限：30 天、89 天、100 天各试一次（只看是否报错和条数）。
        for days in [30i64, 89, 100] {
            raw(
                "/api/v1/private/order/list/order_deals",
                vec![
                    ("symbol", "BTC_USDT".into()),
                    ("start_time", (now - days * day).to_string()),
                    ("end_time", now.to_string()),
                    ("page_num", "1".into()),
                    ("page_size", "100".into()),
                ],
            )
            .await;
        }
        // 有成交的合约：成交、资金费的行长什么样。
        for symbol in var("ARB_PROBE_DEALS").unwrap_or_default().split(',') {
            let symbol = symbol.trim();
            if symbol.is_empty() {
                continue;
            }
            raw(
                "/api/v1/private/order/list/order_deals",
                vec![
                    ("symbol", symbol.to_string()),
                    ("start_time", (now - 7 * day).to_string()),
                    ("end_time", now.to_string()),
                    ("page_num", "1".into()),
                    ("page_size", "3".into()),
                ],
            )
            .await;
            raw(
                "/api/v1/private/position/funding_records",
                vec![
                    ("symbol", symbol.to_string()),
                    ("page_num", "1".into()),
                    ("page_size", "3".into()),
                ],
            )
            .await;
        }
        for round in ["cold", "cached"] {
            let started = Instant::now();
            let catalog = broker.catalog(CONTRACTS_TTL).await;
            println!(
                "contract catalog ({round}): {:?} contracts, {:?} names ({} ms)",
                catalog.as_ref().map(|c| c.contracts.len()),
                catalog.as_ref().map(|c| c.names.len()),
                started.elapsed().as_millis()
            );
        }
        for base in ["BTC", "NVDA", "NVIDIA", "AAPL"] {
            let found = broker.resolve(&Symbol::perp(base, "USDT")).await;
            println!(
                "resolve {base}: {:?}",
                found.map(|c| (c.symbol, c.contract_size.0, c.state, c.api_allowed))
            );
        }
        println!(
            "positions: {:?}",
            broker.positions().await.map(|rows| rows.len())
        );
        println!(
            "open orders: {:?}",
            broker.open_orders().await.map(|rows| rows.len())
        );
        println!(
            "leg state NVDA: {:?}",
            broker
                .leg_state(&Symbol::perp("NVDA", "USDT"))
                .await
                .map(|state| state.is_some())
        );
        let nvda = Symbol::perp("NVDA", "USDT");
        for round in ["first", "second"] {
            let started = Instant::now();
            let warmed = broker.warm_reads(&nvda, Some(dec!(3))).await;
            println!(
                "warm_reads(NVDA, 3x) {round}: {warmed:?} ({} ms)",
                started.elapsed().as_millis()
            );
        }
        for base in ["NVDA", "XAU", "BTC", "NOPE"] {
            let symbol = Symbol::perp(base, "USDT");
            println!(
                "{base} halted now (open / close): {:?} / {:?}",
                broker.trading_halted(&symbol, false).await,
                broker.trading_halted(&symbol, true).await
            );
        }
        println!("free collateral: {:?}", broker.free_collateral().await);
        let since = Utc::now() - chrono::Duration::days(30);
        for base in var("ARB_PROBE_FILLS")
            .unwrap_or_else(|| "BTC".into())
            .split(',')
        {
            let symbol = Symbol::perp(base.trim(), "USDT");
            let started = Instant::now();
            println!(
                "{base} funding since 30 days: {:?} ({} ms)",
                broker.funding_since(&symbol, since).await,
                started.elapsed().as_millis()
            );
            let started = Instant::now();
            match broker.fills_between(&symbol, since, Utc::now()).await {
                Ok(Some(fills)) => {
                    for fill in fills.iter().rev().take(4) {
                        println!(
                            "  {base} fill {} {:?} {} @ {} fee {} {:?}",
                            fill.at,
                            fill.side,
                            fill.quantity,
                            fill.price,
                            fill.fee_usdt,
                            fill.effect
                        );
                    }
                    println!(
                        "{base}: {} fill(s) in 30 days ({} ms)",
                        fills.len(),
                        started.elapsed().as_millis()
                    );
                }
                other => println!("{base} fills: {other:?}"),
            }
        }
        if let Some(failure) = connect_failure {
            panic!("connect FAILED — the live desk would not connect: {failure}");
        }
    }
}
