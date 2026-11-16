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
//! Every order is `LIMIT` + `IOC` (`GTX` for a post-only open, which may rest on the book and is
//! polled / cancelled by the executor); there are no unbounded market orders. An order without a
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
//!
//! Speed (one round trip from this host is ~0.27 s; `exchangeInfo` is 1.15 MB and ~1.8 s):
//! trading rules are cached for 10 minutes (reconciliation's symbol index accepts 6 hours) and
//! dropped on a refusal or an unmapped market; leverage limits for 10 minutes; account-wide
//! settings and a symbol's margin mode + leverage for 20 seconds once confirmed (also by the
//! read-only [`Broker::warm_reads`]). Account positions come from `/fapi/v3/positionRisk`, which
//! lists only held symbols (v2 lists every market). fapi silently ignores unknown parameters
//! too, so every fresh order is read back as `LIMIT` + `IOC` (`GTX` when post-only) at the
//! submitted price.
//!
//! Also implemented: free collateral (`/fapi/v3/balance`), funding (`income`, `FUNDING_FEE`),
//! fills (`userTrades`, 7-day windows split on full pages, every row inside its window; both
//! histories only reach three months back, older windows are refused), and isolated margin
//! top-ups (`positionMargin`, one request, never retried).
//!
//! **Unified account** (`ARB_BINANCE_ACCOUNT=unified`, Portfolio Margin, 统一账户): the same
//! USDⓈ-M contracts traded through `https://papi.binance.com/papi/v1/um/*` (market data stays on
//! `fapi`). Differences this broker handles, all probed GET-only against a live unified account
//! or taken from the official connector's generated types:
//! - UM positions are always cross margin: isolated opens are refused before any request, only
//!   leverage is set, and it is read back from `/papi/v1/um/symbolConfig`.
//! - papi **silently ignores unknown parameters** (`userTrades` accepts `orderId` but does not
//!   filter by it). Fills are therefore listed for a few seconds around the order's own
//!   timestamps and attributed by `orderId` here; a full page is refused rather than guessed. A
//!   fresh order is read back and must be `LIMIT` + `IOC` at the submitted price.
//! - `newClientOrderId` is limited to 32 characters (36 on `fapi`).
//! - Account checks: one-way position mode always (also at connect), UM BNB fee deduction
//!   (`/papi/v1/um/feeBurn`) off before opening only; there is no single-asset mode to check.
//! - Conditional orders live under `/papi/v1/um/algo/*`; the open-order list omits
//!   `actualOrderId`, and positions omit `marginType` / `isolatedWallet`.
//! - Free collateral is the account's `totalAvailableBalance` (USD), zero unless `NORMAL`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use reqwest::{Client, Method, RequestBuilder, StatusCode};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenuePosition};
use crate::live_common::{
    Cached, JournalEntry, LiveOptions, OrderJournal, Verified, hex_lower, hmac_sha256, order_units,
    round_price, transport_error, venue_client_id,
};
use crate::settlement::{FillEffect, VenueFill};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

/// Classic futures REST, and the public market data for both account types.
const BASE_URL: &str = "https://fapi.binance.com";
const VENUE: Venue = Venue::Binance;
const QUOTE: &str = "USDT";
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
/// "Due to the order could not be executed as maker, the Post Only order will be rejected."
const CODE_POST_ONLY_WOULD_TRADE: i64 = -5022;

/// Exchange metadata (1.15 MB, ~1.8 s from this host) is served from the venue's own cache, so
/// re-reading it per order buys no freshness. Kept this long, refreshed by [`Broker::warm_reads`]
/// and dropped whenever an order is refused (filters may have changed).
const INSTRUMENTS_TTL: Duration = Duration::from_secs(600);
const LEVERAGE_LIMIT_TTL: Duration = Duration::from_secs(600);
/// Reconciliation only maps native symbols back to scanner symbols, so it accepts older
/// metadata; a symbol it cannot map drops the cache and the next round re-reads it.
const INDEX_TTL: Duration = Duration::from_secs(6 * 60 * 60);
/// Account-wide settings and per-symbol margin mode + leverage confirmed this recently are not
/// re-read before the next order. It only skips a read; no check is relaxed.
const SETTINGS_TTL: Duration = Duration::from_secs(20);

/// Income and trade history only reach back three months on both account types.
const HISTORY_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
/// `userTrades` refuses windows longer than 7 days (`-4165`); a second of margin.
const TRADE_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000 - 1_000;
/// Income has no documented window limit; one window covers the whole retention.
const INCOME_WINDOW_MS: i64 = HISTORY_RETENTION_MS;
const HISTORY_PAGE: usize = 1_000;
/// A history read needing more requests than this is refused rather than returned partial.
const HISTORY_MAX_REQUESTS: usize = 64;
const FUTURES_BALANCE: &str = "/fapi/v3/balance";
const FUTURES_POSITION_MARGIN: &str = "/fapi/v1/positionMargin";
/// Documented `positionMargin` refusals: not isolated (-4049), balance insufficient (-4050,
/// -4051), no position (-4054), non-positive amount (-4055). Nothing moved.
const MARGIN_REFUSALS: [i64; 5] = [-4049, -4050, -4051, -4054, -4055];

/// Unified account: fills are listed this far around the order's own timestamps.
const UNIFIED_FILL_MARGIN_MS: i64 = 5_000;
/// `/papi/v1/um/userTrades` refuses windows longer than 7 days (`-4165`).
const UNIFIED_MAX_FILL_WINDOW_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const UNIFIED_TRADE_PAGE: usize = 1_000;
/// An IOC's fills can trail its terminal status by a moment; a filled leg must not turn into an
/// "unknown" error mid-arbitrage because the trade list had not caught up yet. Both accounts:
/// the classic one was seen short on 10-16 (an 某合约 exit read right after it filled).
///
/// The waits start short and stretch: from Tokyo the list was always empty on the read right
/// after the order's reply and complete on the next one, so a flat 300 ms wait put 300 ms on
/// every filled order (11-07..11-14: 355 ms median per Binance leg, 348..369). Eight waits,
/// 1.5 s in all — the same budget the flat wait had.
const FILL_RETRY: [Duration; 8] = [
    Duration::from_millis(30),
    Duration::from_millis(50),
    Duration::from_millis(80),
    Duration::from_millis(120),
    Duration::from_millis(200),
    Duration::from_millis(300),
    Duration::from_millis(300),
    Duration::from_millis(420),
];
/// After a cancel the order query can still show the pre-cancel live state for ~100 ms (seen on
/// 10-16 from Tokyo, the cancel itself already effective); re-read briefly before reporting it live.
const CANCEL_LOOKUP_RETRY: [Duration; 2] = [Duration::from_millis(100), Duration::from_millis(200)];
/// 按下单回包记下终态（[`BinanceBroker::settled_by_reply`]）之后多久内，`order_state` 直接给回包那份、
/// 不去读回：逐笔成交常比撮合晚几十到几百毫秒，下单后紧跟的那一次查询读了也多半读不全，白等。
/// 过了这段时间再读（后台的 `correct_estimated_fees`、之后的任何查询），读全了就换成实际的。
const REPLY_VERIFY_AFTER: Duration = Duration::from_secs(3);
/// How long a terminal cancel reply outranks the order query (see `cancel_verdicts`). The lag
/// itself was over 7 s at the 00:00 UTC funding settlement on 10-22; this is a generous bound.
const CANCEL_VERDICT_TTL: Duration = Duration::from_secs(600);
/// Right after submission the order query can still answer "does not exist" (-2013) for an order
/// the venue already accepted (10-16 from Tokyo: an 某合约 post-only order, 150 ms after it
/// was placed). A reserved id the venue does not know is re-read this long before it counts.
const UNKNOWN_ORDER_RETRY: [Duration; 4] = [
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
];

/// Right after a symbol's margin type / leverage is changed, `/fapi/v2/positionRisk` most likely
/// still shows the old settings for a moment (11-01 from Tokyo: 15 TradFi symbols each refused
/// once the first time they were set to isolated 3x, neither call having reported an error).
/// Re-read this long before refusing; the refusal names what was read back.
const SETTINGS_READ_BACK_RETRY: [Duration; 4] = [
    Duration::from_millis(200),
    Duration::from_millis(500),
    Duration::from_millis(1_000),
    Duration::from_millis(2_000),
];

/// Which Binance account the API key trades (`ARB_BINANCE_ACCOUNT`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BinanceAccount {
    /// Classic USDⓈ-M futures account (`fapi`).
    #[default]
    Futures,
    /// Unified account (Portfolio Margin, 统一账户) trading UM futures through `papi`.
    Unified,
}

impl BinanceAccount {
    /// `futures` (or unset) / `unified`.
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            None | Some("" | "futures") => Ok(Self::Futures),
            Some("unified") => Ok(Self::Unified),
            Some(_) => Err(
                "ARB_BINANCE_ACCOUNT 只能是 futures（经典 U 本位合约账户）或 unified（统一账户）"
                    .into(),
            ),
        }
    }

    fn endpoints(self) -> &'static Endpoints {
        match self {
            Self::Futures => &FUTURES,
            Self::Unified => &UNIFIED,
        }
    }
}

/// Signed REST paths of one account type. Public market data always uses `fapi`.
struct Endpoints {
    base: &'static str,
    position_mode: &'static str,
    commission_rate: &'static str,
    leverage_bracket: &'static str,
    leverage: &'static str,
    /// One symbol's position row, with its margin type and leverage.
    position_risk: &'static str,
    /// The whole account's positions: only symbols that hold one (fapi v2 would list all ~900
    /// markets, 360 KB, twice as slow; v3 has no margin type / leverage, which is not needed).
    account_positions: &'static str,
    order: &'static str,
    open_orders: &'static str,
    user_trades: &'static str,
    algo_lookup: &'static str,
    algo_cancel: &'static str,
    open_algo_orders: &'static str,
    income: &'static str,
    /// `newClientOrderId` length limit; the prefix is kept short so the deterministic SHA-256
    /// suffix still carries entropy.
    max_client_id_len: usize,
}

const FUTURES: Endpoints = Endpoints {
    base: BASE_URL,
    position_mode: "/fapi/v1/positionSide/dual",
    commission_rate: "/fapi/v1/commissionRate",
    leverage_bracket: "/fapi/v1/leverageBracket",
    leverage: "/fapi/v1/leverage",
    position_risk: "/fapi/v2/positionRisk",
    account_positions: "/fapi/v3/positionRisk",
    order: "/fapi/v1/order",
    open_orders: "/fapi/v1/openOrders",
    user_trades: "/fapi/v1/userTrades",
    algo_lookup: "/fapi/v1/algoOrder",
    algo_cancel: "/fapi/v1/algoOrder",
    open_algo_orders: "/fapi/v1/openAlgoOrders",
    income: "/fapi/v1/income",
    max_client_id_len: 36,
};

const UNIFIED: Endpoints = Endpoints {
    base: "https://papi.binance.com",
    position_mode: "/papi/v1/um/positionSide/dual",
    commission_rate: "/papi/v1/um/commissionRate",
    leverage_bracket: "/papi/v1/um/leverageBracket",
    leverage: "/papi/v1/um/leverage",
    position_risk: "/papi/v1/um/positionRisk",
    account_positions: "/papi/v1/um/positionRisk",
    order: "/papi/v1/um/order",
    open_orders: "/papi/v1/um/openOrders",
    user_trades: "/papi/v1/um/userTrades",
    algo_lookup: "/papi/v1/um/algo/algoOrder",
    algo_cancel: "/papi/v1/um/algo/order",
    open_algo_orders: "/papi/v1/um/algo/openAlgoOrders",
    income: "/papi/v1/um/income",
    max_client_id_len: 32,
};

const UNIFIED_SYMBOL_CONFIG: &str = "/papi/v1/um/symbolConfig";
/// 经典 U 本位合约账户的「BNB 抵扣手续费」开关（`{"feeBurn": true|false}`）。
const CLASSIC_FEE_BURN: &str = "/fapi/v1/feeBurn";
const UNIFIED_FEE_BURN: &str = "/papi/v1/um/feeBurn";
const UNIFIED_ACCOUNT: &str = "/papi/v1/account";
const UNIFIED_CROSS_ONLY: &str = "the Binance unified account (统一账户) trades UM futures in cross \
     margin only; open this leg with cross margin (全仓)";

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
    account: BinanceAccount,
    options: LiveOptions,
    /// Serialises order submission so two legs cannot race the per-symbol settings.
    submit: Mutex<()>,
    /// Millisecond offset applied to local time to match the venue clock.
    time_offset_ms: AtomicI64,
    /// Account taker commission rate, read at construction; used as `fee_per_side`.
    taker_fee: Decimal,
    /// Shared, exclusively locked intent journal (see [`crate::live_common::OrderJournal`]).
    journal: SyncMutex<OrderJournal>,
    /// Parsed `exchangeInfo` ([`INSTRUMENTS_TTL`]).
    instruments: Cached<Arc<ExchangeInfo>>,
    /// Native symbol → maximum initial leverage ([`LEVERAGE_LIMIT_TTL`]).
    leverage_limits: SyncMutex<HashMap<String, (Instant, u32)>>,
    /// Account-wide settings confirmed recently, keyed by `opening`.
    settings_ok: Verified<bool>,
    /// (native symbol, leverage, margin mode) confirmed in force on the venue.
    symbol_settings: Verified<(String, u32, crate::MarginMode)>,
    /// Terminal cancel replies by (native symbol, order id): the matching engine's own verdict.
    /// The order query can trail it by seconds (某次实盘, 00:00 UTC funding settlement:
    /// the cancel returned at once on its reply, the query kept saying live for over 7 s), and a
    /// cancelled order never trades again, so a query that still says live is stale
    /// ([`CANCEL_VERDICT_TTL`]).
    cancel_verdicts: SyncMutex<HashMap<(String, i64), (Instant, RawOrder)>>,
    /// 刚按下单回包记过终态的客户订单号（见 [`REPLY_VERIFY_AFTER`]）。
    replied_recently: Verified<String>,
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

#[derive(Clone, Deserialize)]
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
    #[serde(default, rename = "type")]
    order_type: Option<String>,
    #[serde(default)]
    time_in_force: Option<String>,
    /// Creation / last update (ms). The unified account bounds fill lookups with them.
    #[serde(default)]
    time: Option<i64>,
    #[serde(default)]
    update_time: Option<i64>,
    /// 成交均价、成交额：下单的 RESULT 回包和订单查询都带（逐笔成交查不全时按它们先记，见
    /// [`BinanceBroker::state_or_estimate`]）。
    #[serde(default)]
    avg_price: Option<String>,
    #[serde(default)]
    cum_quote: Option<String>,
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
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    time: Option<i64>,
}

/// One row of `/fapi/v3/balance`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBalance {
    asset: String,
    available_balance: String,
}

/// One row of the income history.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawIncome {
    #[serde(default)]
    symbol: String,
    income_type: String,
    income: String,
    asset: String,
    time: i64,
    tran_id: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawAlgo {
    client_algo_id: String,
    symbol: String,
    side: String,
    position_side: String,
    #[serde(default = "zero_string")]
    quantity: String,
    #[serde(default = "zero_string")]
    price: String,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    close_position: bool,
    algo_status: String,
    /// Absent from the unified account's open-order list (only the single lookup carries it).
    #[serde(default)]
    actual_order_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPosition {
    symbol: String,
    position_amt: String,
    entry_price: String,
    mark_price: String,
    /// Absent on the unified account, whose UM positions are always cross.
    #[serde(default)]
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
#[serde(rename_all = "camelCase")]
struct FeeBurn {
    fee_burn: bool,
}

/// One row of `/papi/v1/um/symbolConfig`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSymbolConfig {
    symbol: String,
    margin_type: String,
    leverage: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UnifiedAccount {
    account_status: String,
    total_available_balance: String,
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
        account: BinanceAccount,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(VENUE)?;
        if credentials.api_key.trim().is_empty() || credentials.api_secret.is_empty() {
            return Err(error("API key and secret must be non-empty"));
        }
        // A journal written for one account type is never read as the other: the order ids
        // live in different order books (fapi vs papi).
        let kind = match account {
            BinanceAccount::Futures => "binance",
            BinanceAccount::Unified => "binance-unified",
        };
        let identity = format!("{kind}:{}", short_hash(credentials.api_key.as_bytes()));
        let journal = OrderJournal::open(journal_path, &identity)?;
        let offset = server_time_offset_ms(&client, &credentials.api_key).await?;
        let broker = Self::assemble(client, credentials, account, options, offset, journal);
        // Opening-only checks (BNB fee deduction) are left to the open path: the live desk
        // connects all venues or none, so a setting that only matters for new exposure must not
        // take every other venue down at the next restart.
        broker.check_account_settings(false).await?;
        let taker_fee = broker.account_taker_fee().await?;
        Ok(Self {
            taker_fee,
            ..broker
        })
    }

    /// The broker before any authenticated check (connect runs those next).
    fn assemble(
        client: Client,
        credentials: BinanceCredentials,
        account: BinanceAccount,
        options: LiveOptions,
        offset: i64,
        journal: OrderJournal,
    ) -> Self {
        Self {
            client,
            credentials,
            account,
            options,
            submit: Mutex::new(()),
            time_offset_ms: AtomicI64::new(offset),
            taker_fee: Decimal::ZERO,
            journal: SyncMutex::new(journal),
            instruments: Cached::new(),
            leverage_limits: SyncMutex::new(HashMap::new()),
            settings_ok: Verified::new(SETTINGS_TTL),
            symbol_settings: Verified::new(SETTINGS_TTL),
            cancel_verdicts: SyncMutex::new(HashMap::new()),
            replied_recently: Verified::new(REPLY_VERIFY_AFTER),
        }
    }

    fn api(&self) -> &'static Endpoints {
        self.account.endpoints()
    }

    /// Margin mode of every position on this account as far as the venue reports it: the
    /// unified account's UM positions are always cross.
    fn implied_margin_mode(&self) -> crate::MarginMode {
        match self.account {
            BinanceAccount::Futures => crate::MarginMode::Isolated,
            BinanceAccount::Unified => crate::MarginMode::Cross,
        }
    }

    /// Fails, with an actionable message, when the account-wide settings are not the ones this
    /// broker can trade under. It never changes them: a live account's mode is the operator's.
    /// `opening` adds the checks that only matter for new exposure (the unified account's BNB
    /// fee deduction), so they never block an exit.
    async fn check_account_settings(&self, opening: bool) -> ArbResult<()> {
        // A confirmed opening check covers the exit checks too.
        if self.settings_ok.is_fresh(&true) || self.settings_ok.is_fresh(&opening) {
            return Ok(());
        }
        self.read_account_settings(opening).await?;
        self.settings_ok.mark(opening);
        Ok(())
    }

    async fn read_account_settings(&self, opening: bool) -> ArbResult<()> {
        // The account-wide reads are independent: one round trip, not two.
        let mode = self.signed_get(self.api().position_mode, &[], &[]);
        // 经典账户开仓前同样要核「BNB 抵扣手续费」：开着又有 BNB 时手续费按 BNB 扣，`aggregate_trades` 认不了，
        // 已经成交的单读回报错，第一腿就裸着（11-07 审查）。只挡开仓，不挡平仓。
        let mut classic_burn = None;
        let (dual, other) = match self.account {
            BinanceAccount::Futures if opening => {
                let assets = self.signed_get("/fapi/v1/multiAssetsMargin", &[], &[]);
                let burn = self.signed_get(CLASSIC_FEE_BURN, &[], &[]);
                let (dual, assets, burn) = tokio::join!(mode, assets, burn);
                // 读不到这个开关不挡开仓（和以前一样）：只有明确读到「开着」才拒，免得一个接口出错就把币安的开仓全停了。
                match burn {
                    Ok(burn) => classic_burn = Some(burn),
                    Err(failure) => {
                        tracing::warn!(error = %failure, "币安经典账户「BNB 抵扣手续费」开关没读到：这次不核")
                    }
                }
                (dual?, Some(assets?))
            }
            BinanceAccount::Futures => {
                let assets = self.signed_get("/fapi/v1/multiAssetsMargin", &[], &[]);
                let (dual, assets) = tokio::join!(mode, assets);
                (dual?, Some(assets?))
            }
            BinanceAccount::Unified if opening => {
                let burn = self.signed_get(UNIFIED_FEE_BURN, &[], &[]);
                let (dual, burn) = tokio::join!(mode, burn);
                (dual?, Some(burn?))
            }
            BinanceAccount::Unified => (mode.await?, None),
        };
        let dual: DualSidePosition = decode(dual)?;
        if dual.dual_side_position {
            return Err(error(match self.account {
                BinanceAccount::Futures => {
                    "account is in hedge (dual-side) position mode; switch to one-way (net) mode \
                     in the Binance UI/API before starting the bot — this broker never changes \
                     account-wide settings"
                }
                BinanceAccount::Unified => {
                    "unified account UM futures are in hedge (dual-side) position mode; switch \
                     统一账户 U 本位合约 to one-way mode (持仓模式 → 单向持仓) in the Binance \
                     app — this broker never changes account-wide settings"
                }
            }));
        }
        match (self.account, other) {
            (BinanceAccount::Futures, Some(assets)) => {
                let assets: MultiAssetsMargin = decode(assets)?;
                if assets.multi_assets_margin {
                    return Err(error(
                        "account is in multi-assets margin mode; switch to single-asset (USDT) \
                         mode before starting the bot — this broker never changes account-wide \
                         settings",
                    ));
                }
                if let Some(burn) = classic_burn.and_then(|burn| decode::<FeeBurn>(burn).ok())
                    && burn.fee_burn
                {
                    return Err(error(
                        "futures account deducts fees in BNB; turn off 合约「BNB 抵扣手续费」 \
                         — fills charged in BNB cannot be reconciled",
                    ));
                }
            }
            (BinanceAccount::Unified, Some(burn)) => {
                let burn: FeeBurn = decode(burn)?;
                if burn.fee_burn {
                    return Err(error(
                        "unified account UM futures deduct fees in BNB; turn off 统一账户 U 本位 \
                         合约「BNB 抵扣手续费」 — fills charged in BNB cannot be reconciled",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Reads the actual BTCUSDT taker rate as the account reference. Realized fees always
    /// come from fills; Binance exposes this endpoint per symbol, including promotions.
    async fn account_taker_fee(&self) -> ArbResult<Decimal> {
        let info = self.exchange_info().await?;
        let instrument = select_instrument(&info.symbols, &Symbol::perp("BTC", QUOTE))?;
        let rate: CommissionRate = decode(
            self.signed_get(
                self.api().commission_rate,
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
            .request(method, format!("{}{path}?{query}", self.api().base))
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

    async fn exchange_info(&self) -> ArbResult<Arc<ExchangeInfo>> {
        if let Some(info) = self.instruments.get(INSTRUMENTS_TTL) {
            return Ok(info);
        }
        let info: ExchangeInfo = decode(self.public_get("/fapi/v1/exchangeInfo", &[]).await?)?;
        if info.symbols.is_empty() {
            return Err(error("exchange reported no instruments"));
        }
        let info = Arc::new(info);
        self.instruments.put(Arc::clone(&info));
        Ok(info)
    }

    /// Drops cached trading rules after a refusal: the next order re-reads them.
    fn forget_market_rules(&self) {
        self.instruments.clear();
        if let Ok(mut limits) = self.leverage_limits.lock() {
            limits.clear();
        }
        self.symbol_settings.clear();
    }

    /// Native symbol -> scanner symbol for every tradable USDT perpetual. Used by
    /// reconciliation so unknown venue orders are still attributed to a real market.
    async fn symbol_index(&self) -> ArbResult<HashMap<String, Symbol>> {
        let info = match self.instruments.get(INDEX_TTL) {
            Some(info) => info,
            None => self.exchange_info().await?,
        };
        reverse_index(&info.symbols)
    }

    /// A venue row in a market the cached index does not know (a new listing): the cache is
    /// dropped so the next read maps it, and this one fails rather than skipping the row.
    fn unmapped(&self, message: &str) -> ArbError {
        self.instruments.clear();
        error(message)
    }

    /// Maximum initial leverage allowed for this symbol's first notional bracket.
    async fn max_leverage(&self, instrument: &str) -> ArbResult<u32> {
        let cached = self.leverage_limits.lock().ok().and_then(|limits| {
            limits
                .get(instrument)
                .filter(|(at, _)| at.elapsed() < LEVERAGE_LIMIT_TTL)
                .map(|(_, limit)| *limit)
        });
        if let Some(limit) = cached {
            return Ok(limit);
        }
        let limit = self.read_max_leverage(instrument).await?;
        if let Ok(mut limits) = self.leverage_limits.lock() {
            limits.insert(instrument.to_string(), (Instant::now(), limit));
        }
        Ok(limit)
    }

    async fn read_max_leverage(&self, instrument: &str) -> ArbResult<u32> {
        let response = self
            .signed_get(
                self.api().leverage_bracket,
                &[("symbol", instrument.to_string())],
                &[],
            )
            .await?;
        bracket_max_leverage(response, instrument)
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
        let key = (instrument.to_string(), leverage, mode);
        if self.symbol_settings.is_fresh(&key) {
            return Ok(());
        }
        match self.account {
            BinanceAccount::Futures => self.set_futures_margin(instrument, leverage, mode).await?,
            BinanceAccount::Unified => {
                self.set_unified_leverage(instrument, leverage, mode)
                    .await?
            }
        }
        self.symbol_settings.mark(key);
        Ok(())
    }

    /// Classic account: margin type, then leverage, both read back from the position risk row.
    async fn set_futures_margin(
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
        let mut delays = SETTINGS_READ_BACK_RETRY.iter();
        loop {
            let rows: Vec<RawPosition> = decode(
                self.signed_get(
                    "/fapi/v2/positionRisk",
                    &[("symbol", instrument.to_string())],
                    &[],
                )
                .await?,
            )?;
            match confirm_futures_settings(&rows, instrument, leverage, mode)? {
                None => return Ok(()),
                Some(stale) => match delays.next() {
                    Some(delay) => tokio::time::sleep(*delay).await,
                    None => return Err(error(stale)),
                },
            }
        }
    }

    /// Unified account: cross is the only margin mode, so only the leverage is set; both are
    /// read back from the symbol configuration before the order is signed.
    async fn set_unified_leverage(
        &self,
        instrument: &str,
        leverage: u32,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        if !mode.is_cross() {
            return Err(error(UNIFIED_CROSS_ONLY));
        }
        let applied: Value = self
            .signed_post(
                self.api().leverage,
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
        let rows: Vec<RawSymbolConfig> = decode(
            self.signed_get(
                UNIFIED_SYMBOL_CONFIG,
                &[("symbol", instrument.to_string())],
                &[],
            )
            .await?,
        )?;
        confirm_unified_symbol_config(&rows, instrument, leverage)
    }

    /// The margin mode in force for this symbol when its leverage already is `leverage`
    /// (read-only; `None` when something would have to change).
    async fn symbol_mode_in_force(
        &self,
        instrument: &str,
        leverage: u32,
    ) -> ArbResult<Option<crate::MarginMode>> {
        let params = [("symbol", instrument.to_string())];
        Ok(match self.account {
            BinanceAccount::Futures => {
                let rows: Vec<RawPosition> = decode(
                    self.signed_get(self.api().position_risk, &params, &[])
                        .await?,
                )?;
                futures_mode_in_force(&rows, instrument, leverage)
            }
            BinanceAccount::Unified => {
                let rows: Vec<RawSymbolConfig> =
                    decode(self.signed_get(UNIFIED_SYMBOL_CONFIG, &params, &[]).await?)?;
                confirm_unified_symbol_config(&rows, instrument, leverage)
                    .is_ok()
                    .then_some(crate::MarginMode::Cross)
            }
        })
    }

    /// Rows of a time-windowed history endpoint (`startTime` / `endTime` / `limit`) over
    /// `[start, end]`: windows of at most `max_window` ms, a full page split in half until it is
    /// not, kept rows deduplicated by id. Every row must lie inside the window it was asked for:
    /// both APIs silently ignore parameters they do not know, so a dropped window would
    /// otherwise go unnoticed.
    async fn history<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(&'static str, String)],
        (start, end): (i64, i64),
        max_window: i64,
        time: impl Fn(&T) -> Option<i64>,
        keep: impl Fn(&T) -> Option<i64>,
    ) -> ArbResult<Vec<T>> {
        let mut pending = split_windows(start, end, max_window);
        pending.reverse();
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut requests = 0;
        while let Some((from, to)) = pending.pop() {
            requests += 1;
            if requests > HISTORY_MAX_REQUESTS {
                return Err(error(
                    "history needs too many requests; refusing to return a partial result",
                ));
            }
            let mut query = params.to_vec();
            query.extend([
                ("startTime", from.to_string()),
                ("endTime", to.to_string()),
                ("limit", HISTORY_PAGE.to_string()),
            ]);
            let page: Vec<T> = decode(self.signed_get(path, &query, &[]).await?)?;
            if page.len() >= HISTORY_PAGE {
                let [first, second] = halve(from, to).ok_or_else(|| {
                    error("a full page of history within one millisecond; refusing to guess")
                })?;
                pending.push(second);
                pending.push(first);
                continue;
            }
            collect_page(page, (from, to), &time, &keep, &mut seen, &mut rows)?;
        }
        Ok(rows)
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
            // A post-only order is `GTX`: rejected (-5022, no record left) if it would trade at once.
            (
                "timeInForce",
                if order.post_only { "GTX" } else { "IOC" }.to_string(),
            ),
            ("quantity", units.normalize().to_string()),
            ("price", price.normalize().to_string()),
            ("newClientOrderId", venue_id.to_string()),
            ("newOrderRespType", "RESULT".to_string()),
        ];
        if order.reduce_only {
            params.push(("reduceOnly", "true".to_string()));
        }
        let request = match self.signed_request(Method::POST, self.api().order, &params) {
            Ok(request) => request,
            Err(failure) => return SubmitOutcome::Unknown(failure.to_string()),
        };
        let (status, value) = match send_for_json(request, self.api().order).await {
            Ok(reply) => reply,
            Err(reason) => return SubmitOutcome::Unknown(reason),
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

    /// [`Self::lookup_order`] for an id this broker already submitted: "unknown" is re-read for
    /// a moment ([`UNKNOWN_ORDER_RETRY`]) before it is believed, since the query lags submission.
    async fn lookup_submitted_order(
        &self,
        instrument: &str,
        venue_id: &str,
    ) -> ArbResult<Option<RawOrder>> {
        let mut found = self.lookup_order(instrument, venue_id).await?;
        for delay in UNKNOWN_ORDER_RETRY {
            if found.is_some() {
                break;
            }
            tokio::time::sleep(delay).await;
            found = self.lookup_order(instrument, venue_id).await?;
        }
        Ok(found)
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
                self.api().order,
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
        raw: &RawOrder,
        executed: Decimal,
    ) -> ArbResult<(Decimal, Option<Decimal>, Decimal)> {
        if executed < Decimal::ZERO {
            return Err(error("negative executed quantity"));
        }
        if executed.is_zero() {
            return Ok((Decimal::ZERO, None, Decimal::ZERO));
        }
        let (instrument, order_id) = (raw.symbol.as_str(), raw.order_id);
        if self.account == BinanceAccount::Unified {
            return self.unified_fills(raw, executed).await;
        }
        let started = Instant::now();
        let mut waits = FILL_RETRY.iter();
        let mut reads = 0_usize;
        loop {
            let trades = self.order_trades_by_id(instrument, order_id).await?;
            reads += 1;
            let wait = waits.next();
            if traded_quantity(&trades)? >= executed || wait.is_none() {
                log_fill_reads(instrument, reads, started);
                return aggregate_trades(instrument, order_id, executed, &trades);
            }
            if let Some(wait) = wait {
                tokio::time::sleep(*wait).await;
            }
        }
    }

    /// Every fill of one order on the classic account, paged by trade id.
    async fn order_trades_by_id(
        &self,
        instrument: &str,
        order_id: i64,
    ) -> ArbResult<Vec<RawTrade>> {
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
        Ok(trades)
    }

    /// Unified account: `/papi/v1/um/userTrades` cannot filter by order (it ignores `orderId`),
    /// so the trades of a few seconds around the order's own timestamps are listed and
    /// attributed here. Retried briefly while they have not all appeared yet.
    async fn unified_fills(
        &self,
        raw: &RawOrder,
        executed: Decimal,
    ) -> ArbResult<(Decimal, Option<Decimal>, Decimal)> {
        let (start, end) = unified_fill_window(raw.time, raw.update_time)?;
        let started = Instant::now();
        let mut waits = FILL_RETRY.iter();
        let mut reads = 0_usize;
        loop {
            let page: Vec<RawTrade> = decode(
                self.signed_get(
                    self.api().user_trades,
                    &[
                        ("symbol", raw.symbol.clone()),
                        ("startTime", start.to_string()),
                        ("endTime", end.to_string()),
                        ("limit", UNIFIED_TRADE_PAGE.to_string()),
                    ],
                    &[],
                )
                .await?,
            )?;
            reads += 1;
            let mine = order_trades(page, raw.order_id)?;
            let wait = waits.next();
            if traded_quantity(&mine)? >= executed || wait.is_none() {
                log_fill_reads(&raw.symbol, reads, started);
                return aggregate_trades(&raw.symbol, raw.order_id, executed, &mine);
            }
            if let Some(wait) = wait {
                tokio::time::sleep(*wait).await;
            }
        }
    }

    /// A freshly submitted order must be exactly what was asked for. Both fapi and papi ignore
    /// parameters they do not know (probed live), so a request they misread could rest on the
    /// book as something else; such an order is cancelled at once.
    async fn confirm_submission(
        &self,
        raw: &RawOrder,
        order: &NewOrder,
        price: Decimal,
    ) -> ArbResult<()> {
        if submitted_order_matches(raw, price, order.post_only) {
            return Ok(());
        }
        let cancelled = self
            .cancel_regular(&format_venue_order_id(&raw.symbol, raw.order_id))
            .await;
        Err(error(format!(
            "the venue reports this order as {} {} at {} instead of LIMIT {} at the submitted \
             price; {}; reconcile manually",
            raw.order_type.as_deref().unwrap_or("?"),
            raw.time_in_force.as_deref().unwrap_or("?"),
            raw.price,
            expected_time_in_force(order.post_only),
            match cancelled {
                Ok(()) => "it was cancelled".to_string(),
                Err(failure) => format!("cancelling it failed: {failure}"),
            }
        )))
    }

    async fn lookup_algo(&self, client_id: &str) -> ArbResult<RawAlgo> {
        let raw: RawAlgo = decode(
            self.signed_get(
                self.api().algo_lookup,
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
            .ok_or_else(|| self.unmapped("conditional order has an unmapped instrument"))?;
        if raw.position_side != "BOTH" {
            return Err(error("hedge-mode conditional order cannot be reconciled"));
        }
        let id = ClientOrderId(format!("binance-external-algo:{}", raw.client_algo_id));
        if !raw.actual_order_id.is_empty() {
            let order_id = raw
                .actual_order_id
                .parse::<i64>()
                .map_err(|_| error("invalid conditional child order ID"))?;
            let child = self.settled(
                self.lookup_order_by_id(&raw.symbol, order_id)
                    .await?
                    .ok_or_else(|| error("triggered conditional child order is not queryable"))?,
            )?;
            let order = reconstructed_order(&id, symbol, &child, self.implied_margin_mode())?;
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
            post_only: false,
            margin_mode: self.implied_margin_mode(),
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
                self.api().algo_cancel,
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
        // Already cancelled by the matching engine's own word: a query still saying live is
        // stale, and a second DELETE only answers "unknown order".
        if self.cancel_verdict(&symbol, order_id)?.is_some() {
            return Ok(());
        }
        if let Some(raw) = self.lookup_order_by_id(&symbol, order_id).await?
            && !map_status(&raw.status)?.is_live()
        {
            return Ok(());
        }
        let result = self
            .signed_delete(
                self.api().order,
                &[
                    ("symbol", symbol.clone()),
                    ("orderId", order_id.to_string()),
                ],
                &[CODE_CANCEL_REJECTED, CODE_NO_SUCH_ORDER],
            )
            .await;
        // The cancel reply is the matching engine's own verdict at the moment of the cancel; the
        // order query lags it (see CANCEL_LOOKUP_RETRY). A terminal reply for this order settles
        // it, and later reads of the order go by it until the query catches up.
        if let Ok(reply) = &result
            && let Some(verdict) = terminal_cancel_reply(reply, &symbol, order_id)
        {
            self.record_cancel_verdict(verdict)?;
            return Ok(());
        }
        let mut found = self.lookup_order_by_id(&symbol, order_id).await?;
        for delay in CANCEL_LOOKUP_RETRY {
            match &found {
                Some(raw) if map_status(&raw.status)?.is_live() => {}
                _ => break,
            }
            tokio::time::sleep(delay).await;
            found = self.lookup_order_by_id(&symbol, order_id).await?;
        }
        match found {
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

    /// 下单回包已经是终态时，直接按它得出订单状态；不适用就 `None`，照旧去查。
    ///
    /// 为什么：吃单（IOC）用 `newOrderRespType=RESULT` 下，回包是撮合之后的最终结果（FILLED，或没吃完的
    /// EXPIRED），带已成交量、均价、成交额。以前回包之后还要按客户号查一次订单、再等逐笔成交出现
    /// （11-07～11-14 第二腿中位 355ms，发单本身只占一小段）。成交量、成交额回包里就是准的；只有手续费要逐笔
    /// 才有，先按账户的吃单费率估、打上 `fee_estimated`，过 [`REPLY_VERIFY_AFTER`] 之后的 `order_state`、后台的
    /// `correct_estimated_fees` 读回改成实际的 —— 和逐笔跟不上时的 [`Self::state_or_estimate`] 是同一套口径。
    ///
    /// 只认对得上的回包：客户号是这张单的、LIMIT + IOC + 我们报的价、状态已是终态、合约 / 方向 / 数量与意图
    /// 一致。挂单（GTX）、还活着的状态、任何一项对不上，都回到原来的「查订单 + 核对 + 查逐笔」。
    /// 一点没成交的终态没有逐笔可查，直接记（不是估的）。
    fn settled_by_reply(
        &self,
        order: &NewOrder,
        response: &Value,
        venue_id: &str,
        price: Decimal,
        units: Decimal,
    ) -> Option<OrderState> {
        if order.post_only {
            return None;
        }
        let raw: RawOrder = serde_json::from_value(response.clone()).ok()?;
        if raw.client_order_id != venue_id
            || !submitted_order_matches(&raw, price, false)
            || map_status(&raw.status).ok()?.is_live()
        {
            return None;
        }
        let executed = decimal(&raw.executed_qty).ok()?;
        if !executed.is_zero() {
            return estimated_state(order, &raw, units, self.taker_fee);
        }
        let matches = raw.symbol == format!("{}{}", order.symbol.base, order.symbol.quote)
            && parse_side(&raw.side).ok()? == order.side
            && raw.reduce_only == order.reduce_only
            && raw.position_side == "BOTH"
            && decimal(&raw.orig_qty).ok()? == units;
        if !matches {
            return None;
        }
        let mut state = OrderState::new(order.clone());
        state.venue_order_id = Some(format_venue_order_id(&raw.symbol, raw.order_id));
        state.status = verified_status(map_status(&raw.status).ok()?, units, executed);
        if state.status == OrderStatus::Rejected {
            state.reject_reason = Some(format!("venue reported {}", raw.status));
        }
        Some(state)
    }

    /// [`Self::state_from_order`]；逐笔成交还没跟上（查询落后于撮合，某次实盘：回包已报成交 2.51，按订单号
    /// 查不到、逐笔成交也对不上，下单报错、终态没记，之后一直「交易所不认识这个订单号」，执行判成结果不明、
    /// 自动交易被关）时，按订单自己报的成交量、均价先记：手续费按吃单费率估、打上 `fee_estimated`，之后
    /// `order_state` 重读、后台 `correct_estimated_fees` 按实际成交改正。订单本身对不上意图、没有成交量或
    /// 均价时照旧报错。
    async fn state_or_estimate(
        &self,
        order: &NewOrder,
        raw: &RawOrder,
        requested: Decimal,
    ) -> ArbResult<OrderState> {
        let failure = match self.state_from_order(order, raw, requested).await {
            Ok(state) => return Ok(state),
            Err(failure) => failure,
        };
        match estimated_state(order, raw, requested, self.taker_fee) {
            Some(state) => {
                tracing::warn!(
                    order = %order.client_order_id,
                    filled = %state.filled_usdt,
                    error = %failure,
                    "币安订单已报成交但逐笔成交还没跟上：先按订单报的成交量与均价记（手续费是估的），之后读到以实际为准"
                );
                Ok(state)
            }
            None => Err(failure),
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
        let (filled_usdt, average_price, fee_usdt) = self.fills(raw, executed).await?;
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

    fn record_cancel_verdict(&self, verdict: RawOrder) -> ArbResult<()> {
        tracing::info!(
            symbol = %verdict.symbol,
            order_id = verdict.order_id,
            status = %verdict.status,
            executed = %verdict.executed_qty,
            "币安撤单回包已是终态：之后查询若仍显示活跃，以回包为准"
        );
        let mut verdicts = self
            .cancel_verdicts
            .lock()
            .map_err(|_| error("cancel verdict lock is poisoned"))?;
        verdicts.retain(|_, (at, _)| at.elapsed() < CANCEL_VERDICT_TTL);
        verdicts.insert(
            (verdict.symbol.clone(), verdict.order_id),
            (Instant::now(), verdict),
        );
        Ok(())
    }

    /// The terminal cancel reply for this order, while it still outranks the query.
    fn cancel_verdict(&self, instrument: &str, order_id: i64) -> ArbResult<Option<RawOrder>> {
        let verdicts = self
            .cancel_verdicts
            .lock()
            .map_err(|_| error("cancel verdict lock is poisoned"))?;
        Ok(verdicts
            .get(&(instrument.to_string(), order_id))
            .filter(|(at, _)| at.elapsed() < CANCEL_VERDICT_TTL)
            .map(|(_, verdict)| verdict.clone()))
    }

    /// A queried order, unless it still reads live after the matching engine said it is gone.
    fn settled(&self, queried: RawOrder) -> ArbResult<RawOrder> {
        let verdict = self.cancel_verdict(&queried.symbol, queried.order_id)?;
        let queried_status = queried.status.clone();
        let settled = settled_order(queried, verdict);
        if settled.status != queried_status {
            tracing::info!(
                symbol = %settled.symbol,
                order_id = settled.order_id,
                queried = %queried_status,
                settled = %settled.status,
                executed = %settled.executed_qty,
                "币安订单查询落后于撤单回包，按回包记终态"
            );
        }
        Ok(settled)
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
        self.risk_positions_in(None).await
    }

    /// Positions of the whole account, or of one native symbol (a single small request on the
    /// reduce-only path instead of every market's row).
    async fn risk_positions_in(&self, instrument: Option<&str>) -> ArbResult<Vec<VenuePosition>> {
        let (path, params) = match instrument {
            Some(symbol) => (
                self.api().position_risk,
                vec![("symbol", symbol.to_string())],
            ),
            None => (self.api().account_positions, Vec::new()),
        };
        let rows: Vec<RawPosition> = decode(self.signed_get(path, &params, &[]).await?)?;
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
                    self.unmapped(
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
    /// 下单按 [`order_units`] 把数量向下取整到 `LOT_SIZE.stepSize`（只减仓单必须正好是它的整数倍）。
    async fn quantity_step(&self, symbol: &Symbol) -> ArbResult<Option<Decimal>> {
        let symbols = match self.instruments.get(INDEX_TTL) {
            Some(info) => info,
            None => self.exchange_info().await?,
        };
        Ok(Some(select_existing(&symbols.symbols, symbol)?.step_size))
    }

    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let info = self.exchange_info().await?;
        let native = select_existing(&info.symbols, symbol)?.symbol.clone();
        let rows: Vec<RawPosition> = decode(
            self.signed_get(self.api().position_risk, &[("symbol", native.clone())], &[])
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
        let mode = match self.account {
            BinanceAccount::Futures => crate::margin::reported_mode(&row.margin_type),
            BinanceAccount::Unified => Some(crate::MarginMode::Cross),
        };
        Ok(Some(crate::margin::venue_state(
            mode,
            parse(&row.liquidation_price),
            parse(&row.isolated_wallet),
        )))
    }

    /// Read-only: refreshes the trading rules and the account checks, and records the symbol's
    /// margin mode when its leverage already is the requested one, so preparing and placing
    /// skip those round trips. Nothing is changed.
    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let info = self.exchange_info().await?;
        let instrument = select_instrument(&info.symbols, symbol)?;
        self.check_account_settings(true).await?;
        // Invalid requests are refused by the preparation step, not here.
        let Some(leverage) = leverage
            .and_then(|value| crate::margin::leverage(VENUE, Some(value)).ok())
            .and_then(|value| value.to_u32())
        else {
            return Ok(());
        };
        if leverage > self.max_leverage(&instrument.symbol).await? {
            return Ok(());
        }
        let fresh = [crate::MarginMode::Isolated, crate::MarginMode::Cross]
            .into_iter()
            .any(|mode| {
                self.symbol_settings
                    .is_fresh(&(instrument.symbol.clone(), leverage, mode))
            });
        if fresh {
            return Ok(());
        }
        if let Some(mode) = self
            .symbol_mode_in_force(&instrument.symbol, leverage)
            .await?
        {
            self.symbol_settings
                .mark((instrument.symbol.clone(), leverage, mode));
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
        self.options.authorize(Venue::Binance)?;
        if self.account == BinanceAccount::Unified && !mode.is_cross() {
            return Err(error(UNIFIED_CROSS_ONLY));
        }
        let _guard = self.submit.lock().await;
        self.check_account_settings(true).await?;
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

    fn supports_post_only(&self) -> bool {
        true
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
        if order.post_only && (order.reduce_only || order.limit_price.is_none()) {
            // An exit must never wait on the book, and a post-only order needs its price.
            return Err(error(
                "a post-only order must be a limit order that opens a position",
            ));
        }
        let max_len = self.api().max_client_id_len;
        let venue_id = venue_client_id(ORDER_PREFIX, &order.client_order_id, max_len);
        if !valid_client_id(&venue_id, max_len) {
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
            return match self
                .lookup_submitted_order(&entry.instrument, &venue_id)
                .await?
            {
                Some(raw) => {
                    let raw = self.settled(raw)?;
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
        // Recent instrument metadata ([`INSTRUMENTS_TTL`]; a refusal drops it). An exit —
        // close, rollback, protection — must not wait 1.8 s for it: it is exempt from the
        // minimum notional, and a step or tick change is refused, which drops the cache too.
        let symbols = if order.reduce_only {
            match self.instruments.get(INDEX_TTL) {
                Some(info) => info,
                None => self.exchange_info().await?,
            }
        } else {
            self.exchange_info().await?
        };
        let instrument = if order.reduce_only {
            select_existing(&symbols.symbols, &order.symbol)?
        } else {
            select_instrument(&symbols.symbols, &order.symbol)?
        };
        self.check_account_settings(!order.reduce_only).await?;
        let leverage = if order.reduce_only {
            None
        } else {
            let max_leverage = self.max_leverage(&instrument.symbol).await?;
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
                self.risk_positions_in(Some(&instrument.symbol))
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
        let sent = Instant::now();
        match self
            .submit_order(order, &instrument.symbol, price, units, &venue_id)
            .await
        {
            SubmitOutcome::Accepted(response) => {
                // IOC 的 RESULT 回包就是撮合后的最终结果：对得上意图就直接按它记，不再查订单、不等逐笔。
                if let Some(state) =
                    self.settled_by_reply(order, &response, &venue_id, price, units)
                {
                    self.remember_terminal(&state)?;
                    if state.fee_estimated {
                        self.replied_recently.mark(order.client_order_id.0.clone());
                    }
                    tracing::info!(
                        order = %order.client_order_id,
                        status = ?state.status,
                        filled = %state.filled_usdt,
                        ms = sent.elapsed().as_millis() as u64,
                        "币安下单按回包记终态"
                    );
                    return ack(&state);
                }
                // The venue's own status + fills decide, not the acknowledgement.
                match self.lookup_order(&instrument.symbol, &venue_id).await? {
                    Some(raw) => {
                        self.confirm_submission(&raw, order, price).await?;
                        let state = self.state_or_estimate(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        ack(&state)
                    }
                    None => {
                        let raw: RawOrder = decode(response)?;
                        self.confirm_submission(&raw, order, price).await?;
                        let state = self.state_or_estimate(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        ack(&state)
                    }
                }
            }
            SubmitOutcome::Refused { code, message } => {
                // A post-only order that would have crossed is the normal outcome of a stale quote,
                // not a sign the cached market rules are wrong.
                if code != CODE_POST_ONLY_WOULD_TRADE {
                    self.forget_market_rules();
                }
                // 重同步时钟、确认历史这两步出错都不能挡住记拒单：拒单是交易所的业务码给的结论，
                // 不记终态的话订单号从此永远「查不到、不得重发」。
                if code == CODE_INVALID_TIMESTAMP
                    && let Err(resync) = self.resync_clock().await
                {
                    tracing::warn!(error = %resync, "币安时间戳被拒后重同步时钟失败，下一单再试");
                }
                // Even a business error (including duplicate ID) must first be reconciled.
                match self.lookup_order(&instrument.symbol, &venue_id).await {
                    Ok(Some(raw)) => {
                        self.confirm_submission(&raw, order, price).await?;
                        let state = self.state_or_estimate(order, &raw, units).await?;
                        self.remember_terminal(&state)?;
                        return ack(&state);
                    }
                    Ok(None) => {}
                    Err(check) => {
                        tracing::warn!(order = %order.client_order_id, code, error = %check,
                            "币安明确拒单后确认历史没查成：按拒单记终态");
                    }
                }
                // The venue explicitly refused: record it so `order_state` stays truthful.
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(message.clone());
                self.remember_terminal(&rejected)?;
                Err(error(message))
            }
            SubmitOutcome::Unknown(reason) => {
                match self
                    .lookup_submitted_order(&instrument.symbol, &venue_id)
                    .await?
                {
                    Some(raw) => {
                        self.confirm_submission(&raw, order, price).await?;
                        let state = self.state_or_estimate(order, &raw, units).await?;
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
            let raw = self.settled(
                self.lookup_order_by_id(&instrument, order_id)
                    .await?
                    .ok_or_else(|| error("external order is no longer queryable"))?,
            )?;
            let symbol = index
                .get(&instrument)
                .ok_or_else(|| self.unmapped("unmapped external order"))?;
            let order =
                reconstructed_order(client_order_id, symbol, &raw, self.implied_margin_mode())?;
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
            if !terminal.fee_estimated || self.replied_recently.is_fresh(&client_order_id.0) {
                return Ok(Some(terminal));
            }
            // 先按订单报的成交记过（手续费是估的）：再读一次，读得全就换成实际的，读不全照旧给估的。
            if let Ok(Some(raw)) = self
                .lookup_order(&entry.instrument, &entry.venue_client_id)
                .await
                && let Ok(raw) = self.settled(raw)
                && let Ok(state) = self.state_from_order(&entry.order, &raw, entry.units).await
            {
                self.remember_terminal(&state)?;
                return Ok(Some(state));
            }
            return Ok(Some(terminal));
        }
        match self
            .lookup_submitted_order(&entry.instrument, &entry.venue_client_id)
            .await?
        {
            Some(raw) => {
                let raw = self.settled(raw)?;
                let state = self
                    .state_or_estimate(&entry.order, &raw, entry.units)
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
        let (rows, algos) = tokio::join!(
            self.signed_get(self.api().open_orders, &[], &[]),
            self.signed_get(self.api().open_algo_orders, &[], &[])
        );
        let rows: Vec<RawOrder> = decode(rows?)?;
        let algos: Vec<RawAlgo> = decode(algos?)?;
        let index = self.symbol_index().await?;
        let mut states = Vec::with_capacity(rows.len());
        for raw in rows {
            // The open-order list can trail a cancel the same way the order query does.
            if self.cancel_verdict(&raw.symbol, raw.order_id)?.is_some() {
                continue;
            }
            let symbol = index
                .get(&raw.symbol.to_ascii_uppercase())
                .ok_or_else(|| {
                    self.unmapped(
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
                    self.implied_margin_mode(),
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

    /// Classic account: the USDT row's `availableBalance`. Unified account:
    /// `totalAvailableBalance` (USD), zero unless the account is `NORMAL` (margin call,
    /// reduce-only, liquidation… leave nothing for new exposure).
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        match self.account {
            BinanceAccount::Futures => {
                let rows: Vec<RawBalance> =
                    decode(self.signed_get(FUTURES_BALANCE, &[], &[]).await?)?;
                futures_free_collateral(&rows).map(Some)
            }
            BinanceAccount::Unified => {
                let account: UnifiedAccount =
                    decode(self.signed_get(UNIFIED_ACCOUNT, &[], &[]).await?)?;
                unified_free_collateral(&account).map(Some)
            }
        }
    }

    /// Funding fees actually settled on this symbol since `since` (income history, USDT).
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let info = self.exchange_info().await?;
        let native = select_existing(&info.symbols, symbol)?.symbol;
        let now = self.timestamp_ms()?;
        let start = since.timestamp_millis();
        within_retention(start, now)?;
        let rows: Vec<RawIncome> = self
            .history(
                self.api().income,
                &[
                    ("symbol", native.clone()),
                    ("incomeType", "FUNDING_FEE".to_string()),
                ],
                (start, now.max(start)),
                INCOME_WINDOW_MS,
                |row: &RawIncome| Some(row.time),
                |row: &RawIncome| {
                    (row.income_type == "FUNDING_FEE" && row.symbol == native)
                        .then_some(row.tran_id)
                },
            )
            .await?;
        funding_total(&rows, since).map(Some)
    }

    /// Every trade of this account on this symbol in `[since, until]`, from the venue's own
    /// trade list (7-day windows, split when a page fills up).
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<Vec<VenueFill>>> {
        let (start, end) = (since.timestamp_millis(), until.timestamp_millis());
        if end < start {
            return Ok(Some(Vec::new()));
        }
        within_retention(start, self.timestamp_ms()?)?;
        let info = self.exchange_info().await?;
        let native = select_existing(&info.symbols, symbol)?.symbol;
        let rows: Vec<RawTrade> = self
            .history(
                self.api().user_trades,
                &[("symbol", native.clone())],
                (start, end),
                TRADE_WINDOW_MS,
                |row: &RawTrade| row.time,
                |row: &RawTrade| (row.symbol == native).then_some(row.id),
            )
            .await?;
        venue_fills(rows).map(Some)
    }

    /// Isolated top-ups exist on the classic account only (unified UM positions are cross).
    fn supports_add_margin(&self) -> bool {
        self.account == BinanceAccount::Futures
    }

    /// `POST /fapi/v1/positionMargin` (type 1 = add) into an existing isolated one-way position.
    /// At most one request, never retried: there is no idempotency key, a resend adds twice.
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        // Read-only mode refuses before anything is read or signed.
        self.options.authorize(VENUE)?;
        if self.account != BinanceAccount::Futures {
            return Err(error(
                "the unified account has no isolated positions to add margin to",
            ));
        }
        let amount = margin_amount_text(amount_usdt)?;
        let _submission = self.submit.lock().await;
        let info = self.exchange_info().await?;
        let native = select_existing(&info.symbols, symbol)?.symbol;
        let rows: Vec<RawPosition> = decode(
            self.signed_get(self.api().position_risk, &[("symbol", native.clone())], &[])
                .await?,
        )?;
        isolated_position_for_margin(&rows, &native)?;
        let request = self.signed_request(
            Method::POST,
            FUTURES_POSITION_MARGIN,
            &[
                ("symbol", native),
                ("positionSide", "BOTH".to_string()),
                ("amount", amount),
                ("type", "1".to_string()),
            ],
        )?;
        // From here the request may have left this host: a failure is "unknown", never resent.
        let (status, value) = match send_for_json(request, FUTURES_POSITION_MARGIN).await {
            Ok(reply) => reply,
            Err(reason) => {
                return Ok(MarginOutcome::Unknown(format!(
                    "{reason}; the request may have arrived and the margin may have been added"
                )));
            }
        };
        if business_code(&value) == Some(CODE_INVALID_TIMESTAMP) {
            // Refused before queueing; refresh the clock for the next call (no resend here).
            let _ = self.resync_clock().await;
        }
        Ok(classify_margin_reply(status, &value))
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

fn zero_string() -> String {
    "0".to_string()
}

/// Highest initial leverage of the symbol's brackets. `fapi` answers a symbol-filtered request
/// with one object, `papi` with an array; the row is picked by symbol either way.
fn bracket_max_leverage(response: Value, instrument: &str) -> ArbResult<u32> {
    let entries: Vec<RawBracket> = match response {
        Value::Array(_) => decode(response)?,
        other => vec![decode(other)?],
    };
    let entry = entries
        .iter()
        .find(|entry| entry.symbol == instrument)
        .ok_or_else(|| error("leverage bracket response has no row for this symbol"))?;
    entry
        .brackets
        .iter()
        .map(|level| level.initial_leverage)
        .max()
        .filter(|level| *level > 0)
        .ok_or_else(|| error("leverage bracket response has no usable bracket"))
}

/// The classic account's position risk row must show the requested margin mode and leverage.
/// `Err` is a read-back that can never match (missing / ambiguous row, one-way mode broken);
/// `Ok(Some(why))` is a row still showing other settings, which right after a change is usually
/// the read lagging the write, so the caller re-reads before refusing with `why`.
fn confirm_futures_settings(
    rows: &[RawPosition],
    instrument: &str,
    leverage: u32,
    mode: crate::MarginMode,
) -> ArbResult<Option<String>> {
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
        return Ok(Some(format!(
            "requested margin mode was not confirmed by the venue (asked {}, read back {:?}); \
             refusing to open",
            mode.as_str(),
            row.margin_type
        )));
    }
    if decimal(&row.leverage)? != Decimal::from(leverage) {
        return Ok(Some(format!(
            "leverage read-back does not match the requested value (asked {leverage}x, read \
             back {}x); refusing to open",
            row.leverage
        )));
    }
    Ok(None)
}

/// The unified account's symbol configuration must show cross margin at the requested leverage.
fn confirm_unified_symbol_config(
    rows: &[RawSymbolConfig],
    instrument: &str,
    leverage: u32,
) -> ArbResult<()> {
    let row = rows
        .iter()
        .find(|row| row.symbol == instrument)
        .ok_or_else(|| error("symbol configuration read-back has no row for this symbol"))?;
    if !row.margin_type.eq_ignore_ascii_case("CROSSED") {
        return Err(error(
            "unified account symbol is not cross margined; refusing to open",
        ));
    }
    let applied = match &row.leverage {
        Value::Number(number) => decimal(&number.to_string())?,
        Value::String(raw) => decimal(raw)?,
        _ => return Err(error("symbol configuration has no leverage")),
    };
    if applied != Decimal::from(leverage) {
        return Err(error(
            "leverage read-back does not match the requested value; refusing to open",
        ));
    }
    Ok(())
}

/// Fill lookup window for a unified-account order: its own creation / last update, widened by
/// [`UNIFIED_FILL_MARGIN_MS`]. Without a timestamp the fills cannot be bounded at all.
fn unified_fill_window(created: Option<i64>, updated: Option<i64>) -> ArbResult<(i64, i64)> {
    let first = created
        .or(updated)
        .ok_or_else(|| error("order has no timestamp to bound its fills"))?;
    let last = updated.unwrap_or(first).max(first);
    let start = first.saturating_sub(UNIFIED_FILL_MARGIN_MS);
    let end = last.saturating_add(UNIFIED_FILL_MARGIN_MS);
    if end - start > UNIFIED_MAX_FILL_WINDOW_MS {
        return Err(error(
            "the order's fills span more than 7 days; the unified account cannot list them by \
             order — reconcile manually",
        ));
    }
    Ok((start, end))
}

/// The trades of one order out of a symbol's trade page. A full page may have cut some off:
/// refused instead of reporting a partial cost.
fn order_trades(page: Vec<RawTrade>, order_id: i64) -> ArbResult<Vec<RawTrade>> {
    if page.len() >= UNIFIED_TRADE_PAGE {
        return Err(error(
            "too many trades around this order to attribute its fills; reconcile manually",
        ));
    }
    Ok(page
        .into_iter()
        .filter(|trade| trade.order_id == order_id)
        .collect())
}

/// The time in force a fresh order must have on the venue: `IOC`, or `GTX` for a post-only one.
fn expected_time_in_force(post_only: bool) -> &'static str {
    if post_only { "GTX" } else { "IOC" }
}

/// What a fresh order must look like on the venue: a `LIMIT` `IOC` (`GTX` when post-only) at the
/// submitted price.
fn submitted_order_matches(raw: &RawOrder, price: Decimal, post_only: bool) -> bool {
    raw.order_type.as_deref() == Some("LIMIT")
        && raw.time_in_force.as_deref() == Some(expected_time_in_force(post_only))
        && decimal(&raw.price).is_ok_and(|reported| reported == price)
}

/// Sends a request and reads its JSON body. `Err` = the outcome is unknown (transport failure
/// or an unparsable body): the request may have been executed.
async fn send_for_json(
    request: RequestBuilder,
    endpoint: &str,
) -> Result<(StatusCode, Value), String> {
    let response = request
        .send()
        .await
        .map_err(|failure| transport_error(VENUE, endpoint, failure).to_string())?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|failure| transport_error(VENUE, endpoint, failure).to_string())?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|_| format!("unparseable response from {endpoint} (HTTP {status})"))?;
    Ok((status, value))
}

/// `positionMargin` reply → outcome. Only a success or a documented refusal is definite;
/// anything else may have moved the money.
fn classify_margin_reply(status: StatusCode, value: &Value) -> MarginOutcome {
    match (status.is_success(), business_code(value)) {
        (true, None) | (true, Some(200)) => MarginOutcome::Applied,
        (_, Some(code)) if AMBIGUOUS_CODES.contains(&code) => {
            MarginOutcome::Unknown(vendor_message(value, status))
        }
        _ if status.is_server_error() => MarginOutcome::Unknown(vendor_message(value, status)),
        (_, Some(code)) if definitive_rejection(code) || MARGIN_REFUSALS.contains(&code) => {
            MarginOutcome::Refused(vendor_message(value, status))
        }
        _ => MarginOutcome::Unknown(vendor_message(value, status)),
    }
}

/// A top-up amount must be positive and already rounded to the cent: the request then carries
/// exactly what the ledger records.
fn margin_amount_text(amount: Decimal) -> ArbResult<String> {
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

/// The symbol must hold a non-zero, isolated, one-way position before margin is added.
fn isolated_position_for_margin(rows: &[RawPosition], instrument: &str) -> ArbResult<()> {
    let row = rows
        .iter()
        .find(|row| {
            row.symbol == instrument
                && row.position_side == "BOTH"
                && decimal(&row.position_amt).is_ok_and(|amount| !amount.is_zero())
        })
        .ok_or_else(|| error("no open one-way position on this symbol to add margin to"))?;
    if crate::margin::reported_mode(&row.margin_type) != Some(crate::MarginMode::Isolated) {
        return Err(error("margin can only be added to an isolated position"));
    }
    Ok(())
}

/// Classic account: the margin mode in force when the symbol's leverage already matches.
fn futures_mode_in_force(
    rows: &[RawPosition],
    instrument: &str,
    leverage: u32,
) -> Option<crate::MarginMode> {
    let row = rows
        .iter()
        .find(|row| row.symbol == instrument && row.position_side == "BOTH")?;
    if decimal(&row.leverage).ok()? != Decimal::from(leverage) {
        return None;
    }
    crate::margin::reported_mode(&row.margin_type)
}

/// Adds one history page's kept rows (deduplicated by id) to `rows`. Every row — kept or
/// not — must lie inside the window that was asked for.
fn collect_page<T>(
    page: Vec<T>,
    (from, to): (i64, i64),
    time: &impl Fn(&T) -> Option<i64>,
    keep: &impl Fn(&T) -> Option<i64>,
    seen: &mut HashSet<i64>,
    rows: &mut Vec<T>,
) -> ArbResult<()> {
    for row in page {
        if !time(&row).is_some_and(|at| (from..=to).contains(&at)) {
            return Err(error(
                "a history row lies outside the requested window; refusing the result",
            ));
        }
        if let Some(id) = keep(&row)
            && seen.insert(id)
        {
            rows.push(row);
        }
    }
    Ok(())
}

/// `[start, end]` cut into consecutive windows no longer than `max_window` ms.
fn split_windows(start: i64, end: i64, max_window: i64) -> Vec<(i64, i64)> {
    let mut windows = Vec::new();
    let mut from = start;
    while from <= end {
        let to = from.saturating_add(max_window.max(0)).min(end);
        windows.push((from, to));
        if to == i64::MAX {
            break;
        }
        from = to + 1;
    }
    windows
}

/// The two halves of a window, or `None` when it cannot be split any further.
fn halve(from: i64, to: i64) -> Option<[(i64, i64); 2]> {
    if to <= from {
        return None;
    }
    let mid = from + (to - from) / 2;
    Some([(from, mid), (mid + 1, to)])
}

/// History older than the venue keeps would come back silently incomplete.
fn within_retention(start: i64, now: i64) -> ArbResult<()> {
    if start < now.saturating_sub(HISTORY_RETENTION_MS) {
        return Err(error(
            "Binance keeps income and trade history for three months only; a window starting \
             earlier would be incomplete",
        ));
    }
    Ok(())
}

fn utc_millis(ms: i64) -> ArbResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp_millis(ms).ok_or_else(|| error("invalid timestamp"))
}

/// Funding rows → total since `since`; every row must be settled in USDT.
fn funding_total(
    rows: &[RawIncome],
    since: chrono::DateTime<chrono::Utc>,
) -> ArbResult<FundingTotal> {
    let mut settled = Vec::with_capacity(rows.len());
    for row in rows {
        if !row.asset.eq_ignore_ascii_case(QUOTE) {
            return Err(error("a funding payment is settled in a non-USDT asset"));
        }
        settled.push((utc_millis(row.time)?, decimal(&row.income)?));
    }
    Ok(FundingTotal::from_rows(settled, since))
}

/// Trade rows → fills in time order. A row without a time or side, or charged in anything
/// but USDT, makes the whole list unusable rather than silently shorter.
fn venue_fills(mut rows: Vec<RawTrade>) -> ArbResult<Vec<VenueFill>> {
    rows.sort_by_key(|row| (row.time, row.id));
    let mut fills = Vec::with_capacity(rows.len());
    for row in rows {
        if !row.commission_asset.eq_ignore_ascii_case(QUOTE) {
            return Err(error(
                "fees are charged in a non-USDT asset; disable the BNB fee deduction",
            ));
        }
        let quantity = decimal(&row.qty)?;
        let price = decimal(&row.price)?;
        if quantity <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(error("a fill has a non-positive quantity or price"));
        }
        fills.push(VenueFill {
            at: utc_millis(row.time.ok_or_else(|| error("a fill has no time"))?)?,
            side: parse_side(
                row.side
                    .as_deref()
                    .ok_or_else(|| error("a fill has no side"))?,
            )?,
            quantity,
            price,
            fee_usdt: decimal(&row.commission)?,
            effect: FillEffect::Unknown,
        });
    }
    Ok(fills)
}

/// Classic account: the USDT row's available balance (none or negative = nothing available).
fn futures_free_collateral(rows: &[RawBalance]) -> ArbResult<Decimal> {
    match rows
        .iter()
        .find(|row| row.asset.eq_ignore_ascii_case(QUOTE))
    {
        Some(row) => Ok(decimal(&row.available_balance)?.max(Decimal::ZERO)),
        None => Ok(Decimal::ZERO),
    }
}

fn unified_free_collateral(account: &UnifiedAccount) -> ArbResult<Decimal> {
    if account.account_status != "NORMAL" {
        return Ok(Decimal::ZERO);
    }
    let available = decimal(&account.total_available_balance)?;
    Ok(available.max(Decimal::ZERO))
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
    // -1003 / -1015：限频（HTTP 429/418），请求在进撮合之前就被挡掉。
    matches!(code, -1003 | -1015 | -1021 | -1022)
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
                // 合约状态不许开仓（盘前币转换、结算）/ 合约已下架 / 账户只能下只减仓单 / 没有下单权限：
                // 交易所在受理前就拒了。以前这些落进「结果不明」，订单号从此永远查不到、不得重发。
                | -4087
                | -4088
                | -4140
                | -4141
                | -4400
                // Post-only order that would have traded at once: nothing rests, nothing is left.
                | -5022
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
    is_usdt_perp(raw) && raw.status == "TRADING"
}

/// 同 [`is_tradable_usdt_perp`]，但不看状态：合约停牌、结算、盘前转换（非 TRADING）时已有的持仓照样要认得出、
/// 平得掉、查得到资金费与成交。状态只用来拦开仓。
fn is_usdt_perp(raw: &RawSymbol) -> bool {
    SUPPORTED_CONTRACT_TYPES.contains(&raw.contract_type.as_str())
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

/// 交易所合约名 → 我们的合约（对账认持仓、挂单用）。收全部 USDT 永续、不看状态：只收 TRADING 的话，一个持仓
/// 合约进入非 TRADING（停牌、结算、盘前转换）就成了「认不出的持仓」，整个币安场所对账失败、所有币安腿冻结。
/// 已有持仓用的合约查找（只减仓单、持仓状态、资金费、成交、步长）：先按 [`select_instrument`] 找交易中的；
/// 找不到时接受唯一一个名字正好对上、但不在 TRADING 的 USDT 永续 —— 交易所拒不拒这张单由它自己说，
/// 不在本地先拦死（拦死了这笔仓位就永远平不掉、资金费也查不到）。开仓一律用 [`select_instrument`]。
fn select_existing(symbols: &[RawSymbol], want: &Symbol) -> ArbResult<Instrument> {
    match select_instrument(symbols, want) {
        Ok(instrument) => Ok(instrument),
        Err(trading) => {
            if !want.quote.eq_ignore_ascii_case(QUOTE) {
                return Err(trading);
            }
            let native = format!(
                "{}{}",
                want.base.to_ascii_uppercase(),
                want.quote.to_ascii_uppercase()
            );
            let mut matching = symbols.iter().filter(|raw| {
                is_usdt_perp(raw)
                    && raw.base_asset.eq_ignore_ascii_case(&want.base)
                    && raw.symbol.eq_ignore_ascii_case(&native)
            });
            match (matching.next(), matching.next()) {
                (Some(raw), None) => build_instrument(raw),
                _ => Err(trading),
            }
        }
    }
}

fn reverse_index(symbols: &[RawSymbol]) -> ArbResult<HashMap<String, Symbol>> {
    let mut index = HashMap::new();
    for raw in symbols {
        if !is_usdt_perp(raw) {
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

/// The cancel reply, if it reports this very order as no longer on the book. An error body, an
/// unparsable reply, another order or a live/unknown status is not a verdict: the query decides.
fn terminal_cancel_reply(reply: &Value, instrument: &str, order_id: i64) -> Option<RawOrder> {
    serde_json::from_value::<RawOrder>(reply.clone())
        .ok()
        .filter(|raw| {
            raw.symbol == instrument
                && raw.order_id == order_id
                && map_status(&raw.status).is_ok_and(|status| !status.is_live())
        })
}

/// A query that still reads live is stale once the matching engine answered the cancel with a
/// terminal state: the order cannot trade again, so its executed quantity is final. A terminal
/// query has caught up and is used as is. The reply carries no creation time, which the unified
/// account needs to find the fills; the query's is kept.
/// 只按订单自己报的成交量、均价（成交额）算出的状态（见 [`BinanceBroker::state_or_estimate`]）：订单要和意图
/// 对得上、有成交量、有正的均价，否则 `None`。成交额优先用 `cumQuote`，没有就用 数量 × 均价。
fn estimated_state(
    order: &NewOrder,
    raw: &RawOrder,
    requested: Decimal,
    taker_fee: Decimal,
) -> Option<OrderState> {
    let matches = raw.symbol == format!("{}{}", order.symbol.base, order.symbol.quote)
        && parse_side(&raw.side).ok()? == order.side
        && raw.reduce_only == order.reduce_only
        && raw.position_side == "BOTH"
        && decimal(&raw.orig_qty).ok()? == requested;
    let executed = decimal(&raw.executed_qty).ok()?;
    let average = raw
        .avg_price
        .as_deref()
        .and_then(|price| decimal(price).ok())
        .filter(|price| *price > Decimal::ZERO)?;
    if !matches || executed <= Decimal::ZERO || executed > requested {
        return None;
    }
    let filled = raw
        .cum_quote
        .as_deref()
        .and_then(|quote| decimal(quote).ok())
        .filter(|quote| *quote > Decimal::ZERO)
        .unwrap_or(executed * average);
    let status = verified_status(map_status(&raw.status).ok()?, requested, executed);
    let mut state = OrderState::new(order.clone());
    state.venue_order_id = Some(format_venue_order_id(&raw.symbol, raw.order_id));
    state.status = status;
    state.filled_usdt = filled;
    state.average_price = Some(average);
    state.fee_usdt = filled * taker_fee;
    state.fee_estimated = true;
    Some(state)
}

fn settled_order(queried: RawOrder, verdict: Option<RawOrder>) -> RawOrder {
    match verdict {
        Some(mut verdict) if map_status(&queried.status).is_ok_and(|status| status.is_live()) => {
            verdict.time = verdict.time.or(queried.time);
            verdict
        }
        _ => queried,
    }
}

/// Total quantity of a fill list, to tell a list that has not caught up yet from a complete one.
/// How long the fill list trailed the order (only when it did): the figure the retry waits in
/// [`FILL_RETRY`] are tuned from.
fn log_fill_reads(instrument: &str, reads: usize, started: Instant) {
    if reads > 1 {
        tracing::info!(
            instrument,
            reads,
            waited_ms = started.elapsed().as_millis() as u64,
            "币安成交明细回查：多读了几次才齐"
        );
    }
}

fn traded_quantity(trades: &[RawTrade]) -> ArbResult<Decimal> {
    trades.iter().try_fold(Decimal::ZERO, |sum, trade| {
        sum.checked_add(decimal(&trade.qty)?)
            .ok_or_else(|| error("fill quantity overflow"))
    })
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
    margin_mode: crate::MarginMode,
) -> ArbResult<NewOrder> {
    let price = decimal(&raw.price)?;
    let orig_qty = decimal(&raw.orig_qty)?;
    let notional = price
        .checked_mul(orig_qty)
        .ok_or_else(|| error("order notional overflow"))?;
    Ok(NewOrder {
        post_only: false,
        margin_mode,
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

/// `newClientOrderId` rules: 1..=36 (`fapi`) or 1..=32 (`papi`) characters from
/// `[A-Za-z0-9.:/_-]`.
fn valid_client_id(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
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
        && a.post_only == b.post_only
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
        assert!(valid_client_id(&id, FUTURES.max_client_id_len));
        assert!(id.starts_with(ORDER_PREFIX));
        assert_eq!(
            id,
            venue_client_id(ORDER_PREFIX, &ClientOrderId("position-1-buy-0".into()), 36)
        );
        assert_ne!(
            id,
            venue_client_id(ORDER_PREFIX, &ClientOrderId("position-1-buy-1".into()), 36)
        );
        assert!(!valid_client_id("", 36));
        assert!(!valid_client_id("has space", 36));
        assert!(!valid_client_id(&"x".repeat(37), 36));
        // The unified account's limit is 32: a fapi-length id would be refused there.
        let unified = UNIFIED.max_client_id_len;
        assert_eq!(unified, 32);
        let short = venue_client_id(
            ORDER_PREFIX,
            &ClientOrderId("live-1789000000000-hyperliquid-binance-sell-0".into()),
            unified,
        );
        assert!(
            short.len() <= 32 && valid_client_id(&short, unified),
            "{short}"
        );
        assert!(!valid_client_id(&"x".repeat(33), unified));
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
                    side: None,
                    time: None,
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
    fn a_held_contract_that_stops_trading_is_still_recognised_and_closable() {
        let rows = vec![
            raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING"),
            raw_symbol("ETHUSDT", "ETH", "PERPETUAL", "SETTLING"),
        ];
        // 对账：非 TRADING 的持仓照样认得出，不让整个币安场所对账失败。
        assert_eq!(
            reverse_index(&rows).unwrap()["ETHUSDT"],
            Symbol::perp("ETH", "USDT")
        );
        // 开仓照旧只认交易中的；已有持仓（平仓、资金费、成交）能找到它。
        assert!(select_instrument(&rows, &Symbol::perp("ETH", "USDT")).is_err());
        assert_eq!(
            select_existing(&rows, &Symbol::perp("ETH", "USDT"))
                .unwrap()
                .symbol,
            "ETHUSDT"
        );
        assert_eq!(
            select_existing(&rows, &Symbol::perp("BTC", "USDT"))
                .unwrap()
                .symbol,
            "BTCUSDT"
        );
        assert!(select_existing(&rows, &Symbol::perp("DOGE", "USDT")).is_err());
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
        // 只挂单会立刻成交而被拒：交易所不留记录，必须记成明确拒绝，而不是「结果不明」。
        assert!(definitive_rejection(CODE_POST_ONLY_WOULD_TRADE));
        // 限频、合约状态不许开仓、已下架、只能只减仓、没有下单权限：都在受理前就被拒。
        for code in [-1003, -1015, -4087, -4088, -4140, -4141, -4400] {
            assert!(definitive_rejection(code), "{code}");
        }
    }

    #[test]
    fn a_journaled_order_is_the_same_intent_only_with_the_same_time_in_force() {
        let ioc = NewOrder {
            post_only: false,
            client_order_id: ClientOrderId("p-buy-0".into()),
            venue: VENUE,
            symbol: Symbol::perp("DOGE", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(10),
            quantity: None,
            limit_price: Some(dec!(0.08)),
            reduce_only: false,
            margin_mode: crate::MarginMode::Isolated,
            leverage: Some(dec!(2)),
        };
        assert!(same_intent(&ioc, &ioc.clone()));
        // 同一个订单号被拿去发只挂单（或反过来）：不是同一个意图，拒绝重发。
        let resting = NewOrder {
            post_only: true,
            ..ioc.clone()
        };
        assert!(!same_intent(&ioc, &resting));
        assert!(!same_intent(&resting, &ioc));
    }

    #[test]
    fn base_quantities_round_down_but_reduce_only_must_be_exact() {
        let mut order = NewOrder {
            post_only: false,
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

    #[test]
    fn the_account_type_is_explicit_and_defaults_to_the_classic_futures_account() {
        for raw in [None, Some(""), Some(" futures "), Some("FUTURES")] {
            assert_eq!(
                BinanceAccount::parse(raw),
                Ok(BinanceAccount::Futures),
                "{raw:?}"
            );
        }
        for raw in ["unified", " Unified "] {
            assert_eq!(
                BinanceAccount::parse(Some(raw)),
                Ok(BinanceAccount::Unified)
            );
        }
        let error = BinanceAccount::parse(Some("portfolio")).unwrap_err();
        assert!(error.contains("ARB_BINANCE_ACCOUNT"), "{error}");
    }

    #[test]
    fn the_unified_account_signs_against_papi_and_the_classic_one_against_fapi() {
        let paths = |api: &Endpoints| {
            [
                api.position_mode,
                api.commission_rate,
                api.leverage_bracket,
                api.leverage,
                api.position_risk,
                api.account_positions,
                api.order,
                api.open_orders,
                api.user_trades,
                api.algo_lookup,
                api.algo_cancel,
                api.open_algo_orders,
                api.income,
            ]
        };
        assert_eq!(UNIFIED.base, "https://papi.binance.com");
        assert!(
            paths(&UNIFIED)
                .iter()
                .all(|path| path.starts_with("/papi/v1/um/"))
        );
        assert_eq!(FUTURES.base, BASE_URL);
        assert!(
            paths(&FUTURES)
                .iter()
                .all(|path| path.starts_with("/fapi/"))
        );
        assert_eq!(
            BinanceAccount::Unified.endpoints().order,
            "/papi/v1/um/order"
        );
        assert_eq!(BinanceAccount::Futures.endpoints().order, "/fapi/v1/order");
    }

    #[test]
    fn leverage_brackets_are_picked_by_symbol_from_either_response_shape() {
        let fapi = serde_json::json!({
            "symbol": "BTCUSDT",
            "brackets": [{"bracket": 1, "initialLeverage": 125}, {"bracket": 2, "initialLeverage": 50}]
        });
        assert_eq!(bracket_max_leverage(fapi.clone(), "BTCUSDT").unwrap(), 125);
        assert!(bracket_max_leverage(fapi, "ETHUSDT").is_err());
        // papi answers a symbol-filtered request with an array (probed live).
        let papi = serde_json::json!([
            {"symbol": "ETHUSDT", "brackets": [{"initialLeverage": 100}]},
            {"symbol": "BTCUSDT", "brackets": [{"initialLeverage": 150}, {"initialLeverage": 2}]}
        ]);
        assert_eq!(bracket_max_leverage(papi.clone(), "BTCUSDT").unwrap(), 150);
        assert!(bracket_max_leverage(papi, "SOLUSDT").is_err());
        let empty = serde_json::json!([{"symbol": "BTCUSDT", "brackets": []}]);
        assert!(bracket_max_leverage(empty, "BTCUSDT").is_err());
    }

    #[test]
    fn unified_responses_decode_without_the_fields_papi_leaves_out() {
        // `/papi/v1/um/positionRisk` has no marginType / isolatedWallet.
        let position: RawPosition = serde_json::from_value(serde_json::json!({
            "entryPrice": "6563.6", "leverage": "10", "markPrice": "6563.4",
            "maxNotionalValue": "100000000", "positionAmt": "0.002", "notional": "13.1",
            "symbol": "BTCUSDT", "unRealizedProfit": "-0.0004", "liquidationPrice": "5930.78",
            "positionSide": "BOTH", "updateTime": 1625474304765_i64
        }))
        .unwrap();
        assert_eq!(position.margin_type, "");
        assert_eq!(position.liquidation_price.as_deref(), Some("5930.78"));
        assert!(position.isolated_wallet.is_none());
        // The open algo-order list has no actualOrderId; a closePosition order no quantity.
        let algo: RawAlgo = serde_json::from_value(serde_json::json!({
            "algoId": 1, "clientAlgoId": "manual-1", "algoType": "CONDITIONAL",
            "orderType": "STOP_MARKET", "symbol": "BTCUSDT", "side": "SELL",
            "positionSide": "BOTH", "algoStatus": "NEW", "triggerPrice": "80000",
            "closePosition": true
        }))
        .unwrap();
        assert_eq!(algo.actual_order_id, "");
        assert_eq!((algo.quantity.as_str(), algo.price.as_str()), ("0", "0"));
        assert!(algo.close_position && !algo.reduce_only);
        // The new-order RESULT body carries updateTime but no creation time.
        let order: RawOrder = serde_json::from_value(serde_json::json!({
            "clientOrderId": "arb-x", "cumQty": "0", "executedQty": "0.002", "orderId": 22542179,
            "origQty": "0.002", "price": "82992.3", "reduceOnly": false, "side": "BUY",
            "positionSide": "BOTH", "status": "FILLED", "symbol": "BTCUSDT",
            "timeInForce": "IOC", "type": "LIMIT", "updateTime": 1566818724722_i64
        }))
        .unwrap();
        assert_eq!(order.time, None);
        assert_eq!(order.update_time, Some(1566818724722));
        assert_eq!(order.order_type.as_deref(), Some("LIMIT"));
    }

    #[test]
    fn a_terminal_cancel_reply_for_the_same_order_settles_the_cancel() {
        // 某次实盘: the cancel took effect at once, the order query still said
        // PARTIALLY_FILLED for ~100 ms. The DELETE reply (this shape) is the verdict.
        let reply = |order_id: i64, symbol: &str, status: &str| {
            serde_json::json!({
                "clientOrderId": "arb-00000000000000000000000000000002", "cumQty": "0.01",
                "cumQuote": "19.15370", "executedQty": "0.01", "orderId": order_id,
                "origQty": "0.41", "origType": "LIMIT", "price": "1915.37000",
                "reduceOnly": false, "side": "BUY", "positionSide": "BOTH", "status": status,
                "stopPrice": "0", "closePosition": false, "symbol": symbol,
                "timeInForce": "GTX", "type": "LIMIT", "updateTime": 1792141042542_i64,
                "workingType": "CONTRACT_PRICE", "priceProtect": false
            })
        };
        assert!(
            terminal_cancel_reply(
                &reply(100000001, "ANTHROPICUSDT", "CANCELED"),
                "ANTHROPICUSDT",
                100000001
            )
            .is_some()
        );
        assert!(
            terminal_cancel_reply(
                &reply(100000001, "ANTHROPICUSDT", "FILLED"),
                "ANTHROPICUSDT",
                100000001
            )
            .is_some()
        );
        for (order_id, symbol, status) in [
            (100000001, "ANTHROPICUSDT", "PARTIALLY_FILLED"),
            (100000001, "ANTHROPICUSDT", "NEW"),
            (100000001, "ANTHROPICUSDT", "SOMETHING_NEW"),
            (1, "ANTHROPICUSDT", "CANCELED"),
            (100000001, "OPENAIUSDT", "CANCELED"),
        ] {
            assert!(
                terminal_cancel_reply(&reply(order_id, symbol, status), "ANTHROPICUSDT", 100000001)
                    .is_none(),
                "{order_id} {symbol} {status}"
            );
        }
        let refused = serde_json::json!({"code": -2011, "msg": "Unknown order sent."});
        assert!(terminal_cancel_reply(&refused, "ANTHROPICUSDT", 100000001).is_none());
    }

    /// 某次实盘：下单 RESULT 回包已报全部成交，按订单号查不到、逐笔成交也对不上。按订单报的成交量与均价先记，
    /// 手续费按吃单费率估、打上估算标记；订单对不上意图、没成交、没均价都不估。
    #[test]
    fn a_filled_result_reply_is_recorded_with_an_estimated_fee_when_fills_lag() {
        let order = NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("live-1794174962518-sell-0".into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("PLTR", "USDT"),
            side: Side::Sell,
            notional_usdt: dec!(400),
            quantity: Some(dec!(2.51)),
            limit_price: Some(dec!(158.9)),
            reduce_only: false,
            leverage: Some(dec!(3)),
        };
        let reply = |executed: &str, avg: Option<&str>, symbol: &str| -> RawOrder {
            let mut value = serde_json::json!({
                "orderId": 9000123, "symbol": symbol, "clientOrderId": "arb-x", "status": "FILLED",
                "side": "SELL", "positionSide": "BOTH", "reduceOnly": false, "price": "158.90",
                "origQty": "2.51", "executedQty": executed, "cumQuote": "399.4165",
                "type": "LIMIT", "timeInForce": "IOC", "updateTime": 1794174963000_i64
            });
            if let Some(avg) = avg {
                value["avgPrice"] = serde_json::json!(avg);
            }
            serde_json::from_value(value).unwrap()
        };
        let state = estimated_state(
            &order,
            &reply("2.51", Some("159.13"), "PLTRUSDT"),
            dec!(2.51),
            dec!(0.0005),
        )
        .unwrap();
        assert_eq!(state.status, OrderStatus::Filled);
        assert_eq!(state.filled_usdt, dec!(399.4165), "成交额用 cumQuote");
        assert_eq!(state.average_price, Some(dec!(159.13)));
        assert_eq!(state.fee_usdt, dec!(399.4165) * dec!(0.0005));
        assert!(state.fee_estimated);
        assert_eq!(state.venue_order_id.as_deref(), Some("PLTRUSDT:9000123"));
        // 不估的：没有均价、没成交、合约对不上。
        assert!(
            estimated_state(
                &order,
                &reply("2.51", None, "PLTRUSDT"),
                dec!(2.51),
                dec!(0.0005)
            )
            .is_none()
        );
        assert!(
            estimated_state(
                &order,
                &reply("0", Some("159.13"), "PLTRUSDT"),
                dec!(2.51),
                dec!(0.0005)
            )
            .is_none()
        );
        assert!(
            estimated_state(
                &order,
                &reply("2.51", Some("159.13"), "NVDAUSDT"),
                dec!(2.51),
                dec!(0.0005)
            )
            .is_none()
        );
    }

    /// 吃单的 RESULT 回包已是终态、对得上意图：直接按回包记（不查订单、不等逐笔）。成交量与成交额是准的，
    /// 手续费先估；一点没成交的终态直接记、不算估的。挂单、还活着、客户号 / 价格 / 有效期 / 合约 / 数量
    /// 任何一项对不上，都不按回包记（回到查订单那条路）。
    #[tokio::test]
    async fn a_terminal_ioc_reply_settles_the_order_without_any_read_back() {
        let mut broker = offline_broker(BinanceAccount::Futures);
        broker.taker_fee = dec!(0.0005);
        let order = NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("live-1794174962518-sell-0".into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("PLTR", "USDT"),
            side: Side::Sell,
            notional_usdt: dec!(400),
            quantity: Some(dec!(2.51)),
            limit_price: Some(dec!(158.9)),
            reduce_only: false,
            leverage: Some(dec!(3)),
        };
        let reply = |status: &str, executed: &str, quote: &str| {
            serde_json::json!({
                "orderId": 9000123, "symbol": "PLTRUSDT", "clientOrderId": "arb-x", "status": status,
                "side": "SELL", "positionSide": "BOTH", "reduceOnly": false, "price": "158.90",
                "origQty": "2.51", "executedQty": executed, "cumQuote": quote, "avgPrice": "159.13",
                "type": "LIMIT", "timeInForce": "IOC", "updateTime": 1794174963000_i64
            })
        };
        let settle = |order: &NewOrder, reply: &Value| {
            broker.settled_by_reply(order, reply, "arb-x", dec!(158.9), dec!(2.51))
        };

        let filled = settle(&order, &reply("FILLED", "2.51", "399.4165")).unwrap();
        assert_eq!(filled.status, OrderStatus::Filled);
        assert_eq!(filled.filled_usdt, dec!(399.4165));
        assert_eq!(filled.average_price, Some(dec!(159.13)));
        assert_eq!(filled.fee_usdt, dec!(399.4165) * dec!(0.0005));
        assert!(filled.fee_estimated);

        // 没吃完：剩余部分过期，按已成交的记成 Cancelled。
        let partial = settle(&order, &reply("EXPIRED", "1.00", "159.13")).unwrap();
        assert_eq!(partial.status, OrderStatus::Cancelled);
        assert_eq!(partial.filled_usdt, dec!(159.13));
        assert!(partial.fee_estimated);

        // 一点没成交：没有逐笔可查，直接是终态，不算估的。
        let missed = settle(&order, &reply("EXPIRED", "0", "0")).unwrap();
        assert_eq!(missed.status, OrderStatus::Cancelled);
        assert!(missed.filled_usdt.is_zero());
        assert!(missed.average_price.is_none());
        assert!(!missed.fee_estimated);
        assert_eq!(missed.venue_order_id.as_deref(), Some("PLTRUSDT:9000123"));

        // 不按回包记的。
        let good = reply("FILLED", "2.51", "399.4165");
        let with = |key: &str, value: Value| {
            let mut changed = good.clone();
            changed[key] = value;
            changed
        };
        assert!(settle(&order, &reply("NEW", "0", "0")).is_none(), "还活着");
        assert!(settle(&order, &reply("PARTIALLY_FILLED", "1.00", "159.13")).is_none());
        assert!(settle(&order, &with("clientOrderId", "arb-y".into())).is_none());
        assert!(settle(&order, &with("price", "158.80".into())).is_none());
        assert!(settle(&order, &with("timeInForce", "GTC".into())).is_none());
        assert!(settle(&order, &with("symbol", "NVDAUSDT".into())).is_none());
        assert!(settle(&order, &with("origQty", "2.50".into())).is_none());
        assert!(settle(&order, &with("side", "BUY".into())).is_none());
        let zero = reply("EXPIRED", "0", "0");
        let mut other = zero.clone();
        other["origQty"] = "2.50".into();
        assert!(settle(&order, &other).is_none(), "没成交的也要对得上数量");
        let resting = NewOrder {
            post_only: true,
            ..order.clone()
        };
        assert!(settle(&resting, &good).is_none(), "挂单不按回包记");

        // 记下之后紧跟的查询直接给这一份：这个券商发不出任何请求，能答上来就说明没去读。
        broker
            .journal
            .lock()
            .unwrap()
            .reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: "arb-x".into(),
                instrument: "PLTRUSDT".into(),
                units: dec!(2.51),
                terminal: None,
            })
            .unwrap();
        broker.remember_terminal(&filled).unwrap();
        broker
            .replied_recently
            .mark(order.client_order_id.0.clone());
        let read = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .unwrap();
        assert!(read.fee_estimated);
        assert_eq!(read.filled_usdt, dec!(399.4165));
    }

    fn raw_order(order_id: i64, status: &str, executed: &str) -> RawOrder {
        serde_json::from_value(serde_json::json!({
            "orderId": order_id, "symbol": "ANTHROPICUSDT",
            "clientOrderId": "arb-00000000000000000000000000000003", "status": status,
            "side": "SELL", "positionSide": "BOTH", "reduceOnly": false, "price": "1936.24",
            "origQty": "0.2", "executedQty": executed, "type": "LIMIT", "timeInForce": "GTX"
        }))
        .unwrap()
    }

    #[test]
    fn a_terminal_cancel_reply_outranks_a_query_that_still_reads_live() {
        // 某次实盘 at the 00:00 UTC funding settlement: the DELETE reply said CANCELED,
        // the order query kept saying NEW for over 7 s and the open order was declared stuck.
        let verdict = raw_order(100000002, "CANCELED", "0.03");
        for live in ["NEW", "PARTIALLY_FILLED"] {
            let mut queried = raw_order(100000002, live, "0");
            queried.time = Some(1792627199400);
            let settled = settled_order(queried, Some(verdict.clone()));
            assert_eq!(
                settled.time,
                Some(1792627199400),
                "the unified account bounds fills by it"
            );
            assert_eq!(
                (settled.status.as_str(), settled.executed_qty.as_str()),
                ("CANCELED", "0.03"),
                "{live}"
            );
        }
        let caught_up = settled_order(raw_order(100000002, "CANCELED", "0.03"), Some(verdict));
        assert_eq!(caught_up.status, "CANCELED");
        let unknown = settled_order(
            raw_order(100000002, "SOMETHING_NEW", "0"),
            Some(raw_order(100000002, "CANCELED", "0")),
        );
        assert_eq!(
            unknown.status, "SOMETHING_NEW",
            "an unknown status still fails loudly"
        );
        let alone = settled_order(raw_order(100000002, "NEW", "0"), None);
        assert_eq!(alone.status, "NEW", "without a verdict the query stands");
    }

    #[tokio::test]
    async fn an_order_the_engine_cancelled_is_not_cancelled_again_until_the_verdict_expires() {
        // The key cannot even form a request, so `Ok` proves no request was needed.
        let broker = offline_broker(BinanceAccount::Futures);
        assert!(
            broker
                .cancel_regular("ANTHROPICUSDT:100000002")
                .await
                .is_err()
        );
        broker
            .record_cancel_verdict(raw_order(100000002, "CANCELED", "0"))
            .unwrap();
        assert!(
            broker
                .cancel_regular("ANTHROPICUSDT:100000002")
                .await
                .is_ok()
        );
        assert!(
            broker
                .cancel_regular("ANTHROPICUSDT:100000003")
                .await
                .is_err(),
            "another order still asks the venue"
        );
        assert_eq!(
            broker
                .settled(raw_order(100000002, "NEW", "0"))
                .unwrap()
                .status,
            "CANCELED"
        );
        for (at, _) in broker.cancel_verdicts.lock().unwrap().values_mut() {
            *at = at
                .checked_sub(CANCEL_VERDICT_TTL)
                .expect("uptime beyond the verdict TTL");
        }
        assert!(
            broker
                .cancel_verdict("ANTHROPICUSDT", 100000002)
                .unwrap()
                .is_none()
        );
        assert!(
            broker
                .cancel_regular("ANTHROPICUSDT:100000002")
                .await
                .is_err()
        );
        assert_eq!(
            broker
                .settled(raw_order(100000002, "NEW", "0"))
                .unwrap()
                .status,
            "NEW"
        );
    }

    #[test]
    fn a_fill_list_that_has_not_caught_up_sums_short_of_the_executed_quantity() {
        let trade = |qty: &str| RawTrade {
            id: 1,
            order_id: 7,
            symbol: "ANTHROPICUSDT".into(),
            price: "1908.63".into(),
            qty: qty.into(),
            commission: "0.01".into(),
            commission_asset: "USDT".into(),
            side: None,
            time: None,
        };
        assert_eq!(traded_quantity(&[]).unwrap(), Decimal::ZERO);
        let partial = [trade("0.04"), trade("0.02")];
        assert_eq!(traded_quantity(&partial).unwrap(), dec!(0.06));
        assert!(
            traded_quantity(&partial).unwrap() < dec!(0.42),
            "still catching up: re-read"
        );
        assert!(traded_quantity(&[trade("x")]).is_err());
    }

    #[test]
    fn the_unified_symbol_config_must_show_cross_at_the_requested_leverage() {
        let rows: Vec<RawSymbolConfig> = serde_json::from_value(serde_json::json!([
            {"symbol": "ETHUSDT", "marginType": "CROSSED", "leverage": 3},
            {"symbol": "BTCUSDT", "marginType": "CROSSED", "isAutoAddMargin": false,
             "leverage": 5, "maxNotionalValue": "100000000"}
        ]))
        .unwrap();
        assert!(confirm_unified_symbol_config(&rows, "BTCUSDT", 5).is_ok());
        assert!(confirm_unified_symbol_config(&rows, "BTCUSDT", 3).is_err());
        assert!(confirm_unified_symbol_config(&rows, "SOLUSDT", 5).is_err());
        let isolated: Vec<RawSymbolConfig> = serde_json::from_value(serde_json::json!([
            {"symbol": "BTCUSDT", "marginType": "ISOLATED", "leverage": "5"}
        ]))
        .unwrap();
        assert!(confirm_unified_symbol_config(&isolated, "BTCUSDT", 5).is_err());
        let as_string: Vec<RawSymbolConfig> = serde_json::from_value(serde_json::json!([
            {"symbol": "BTCUSDT", "marginType": "crossed", "leverage": "5"}
        ]))
        .unwrap();
        assert!(confirm_unified_symbol_config(&as_string, "BTCUSDT", 5).is_ok());
    }

    #[test]
    fn unified_fills_are_bounded_by_the_orders_own_timestamps() {
        assert_eq!(
            unified_fill_window(Some(1_000_000), Some(1_000_500)).unwrap(),
            (995_000, 1_005_500)
        );
        // RESULT bodies have only updateTime.
        assert_eq!(
            unified_fill_window(None, Some(2_000_000)).unwrap(),
            (1_995_000, 2_005_000)
        );
        assert_eq!(
            unified_fill_window(Some(3_000_000), None).unwrap(),
            (2_995_000, 3_005_000)
        );
        assert!(unified_fill_window(None, None).is_err());
        // A resting order that filled over more than the 7-day API window cannot be listed.
        let week = UNIFIED_MAX_FILL_WINDOW_MS;
        assert!(unified_fill_window(Some(0), Some(week)).is_err());
        assert!(unified_fill_window(Some(0), Some(week - 2 * UNIFIED_FILL_MARGIN_MS)).is_ok());
    }

    #[test]
    fn unified_fills_are_attributed_by_order_and_a_full_page_is_refused() {
        let trade = |id: i64, order_id: i64, qty: &str| RawTrade {
            id,
            order_id,
            symbol: "BTCUSDT".into(),
            price: "100".into(),
            qty: qty.into(),
            commission: "0.01".into(),
            commission_asset: QUOTE.into(),
            side: None,
            time: None,
        };
        // Other orders' trades in the same window are dropped instead of failing the order.
        let page = vec![trade(1, 7, "1"), trade(2, 8, "5"), trade(3, 7, "0.5")];
        let mine = order_trades(page, 7).unwrap();
        assert_eq!(mine.iter().map(|t| t.id).collect::<Vec<_>>(), [1, 3]);
        assert_eq!(
            aggregate_trades("BTCUSDT", 7, dec!(1.5), &mine).unwrap(),
            (dec!(150), Some(dec!(100)), dec!(0.02))
        );
        // Still short of the executed quantity: the sum check refuses a partial cost.
        assert!(aggregate_trades("BTCUSDT", 7, dec!(2), &mine).is_err());
        let full: Vec<RawTrade> = (0..UNIFIED_TRADE_PAGE as i64)
            .map(|id| trade(id, 9, "1"))
            .collect();
        assert!(order_trades(full, 7).is_err());
    }

    #[test]
    fn a_fresh_unified_order_must_be_the_limit_ioc_that_was_sent() {
        let order = |kind: Option<&str>, tif: Option<&str>, price: &str| {
            let mut raw = serde_json::json!({
                "orderId": 1, "symbol": "BTCUSDT", "clientOrderId": "arb-x", "status": "FILLED",
                "side": "BUY", "positionSide": "BOTH", "price": price, "origQty": "1",
                "executedQty": "1"
            });
            if let Some(kind) = kind {
                raw["type"] = kind.into();
            }
            if let Some(tif) = tif {
                raw["timeInForce"] = tif.into();
            }
            serde_json::from_value::<RawOrder>(raw).unwrap()
        };
        assert!(submitted_order_matches(
            &order(Some("LIMIT"), Some("IOC"), "82992.30"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), Some("GTC"), "82992.3"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(Some("MARKET"), Some("IOC"), "0"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), Some("IOC"), "82992.4"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(None, Some("IOC"), "82992.3"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), None, "82992.3"),
            dec!(82992.3),
            false
        ));
        // A post-only order must come back as GTX, an ordinary one must not.
        assert!(submitted_order_matches(
            &order(Some("LIMIT"), Some("GTX"), "82992.3"),
            dec!(82992.3),
            true
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), Some("IOC"), "82992.3"),
            dec!(82992.3),
            true
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), Some("GTX"), "82992.3"),
            dec!(82992.3),
            false
        ));
        assert!(!submitted_order_matches(
            &order(Some("LIMIT"), Some("GTC"), "82992.3"),
            dec!(82992.3),
            true
        ));
    }

    #[test]
    fn unified_free_collateral_is_zero_unless_the_account_is_normal() {
        let account = |status: &str, available: &str| UnifiedAccount {
            account_status: status.into(),
            total_available_balance: available.into(),
        };
        assert_eq!(
            unified_free_collateral(&account("NORMAL", "123.4")).unwrap(),
            dec!(123.4)
        );
        assert_eq!(
            unified_free_collateral(&account("NORMAL", "-5")).unwrap(),
            Decimal::ZERO
        );
        for status in [
            "MARGIN_CALL",
            "SUPPLY_MARGIN",
            "REDUCE_ONLY",
            "ACTIVE_LIQUIDATION",
        ] {
            assert_eq!(
                unified_free_collateral(&account(status, "999")).unwrap(),
                Decimal::ZERO
            );
        }
        assert!(unified_free_collateral(&account("NORMAL", "n/a")).is_err());
    }

    fn offline_broker(account: BinanceAccount) -> BinanceBroker {
        // Tests run in parallel: the counter keeps two brokers made in the same millisecond
        // from locking each other's journal.
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "arb-binance-offline-{}-{}-{}.jsonl",
            std::process::id(),
            now_ms().unwrap(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let journal = OrderJournal::open(&path, "binance:offline-test").unwrap();
        BinanceBroker::assemble(
            Client::new(),
            // Not a valid header value: any request fails before it is sent.
            BinanceCredentials {
                api_key: "offline\nkey".into(),
                api_secret: "not-a-real-secret".into(),
            },
            account,
            LiveOptions {
                trading_enabled: false,
                market_slippage: None,
            },
            0,
            journal,
        )
    }

    #[test]
    fn a_settings_read_back_still_showing_the_old_values_is_re_read_not_trusted() {
        let row = |margin: &str, leverage: &str| -> Vec<RawPosition> {
            serde_json::from_value(serde_json::json!([{
                "symbol": "MSFTUSDT", "positionAmt": "0", "entryPrice": "0",
                "markPrice": "500", "marginType": margin, "positionSide": "BOTH",
                "leverage": leverage
            }]))
            .unwrap()
        };
        let isolated = crate::MarginMode::Isolated;
        assert_eq!(
            confirm_futures_settings(&row("isolated", "3"), "MSFTUSDT", 3, isolated).unwrap(),
            None
        );
        let stale = confirm_futures_settings(&row("isolated", "20"), "MSFTUSDT", 3, isolated)
            .unwrap()
            .expect("an old leverage is re-read");
        assert!(stale.contains("asked 3x, read back 20x"), "{stale}");
        let stale = confirm_futures_settings(&row("cross", "3"), "MSFTUSDT", 3, isolated)
            .unwrap()
            .expect("an old margin mode is re-read");
        assert!(stale.contains("read back \"cross\""), "{stale}");
        // What a re-read cannot fix still fails at once.
        assert!(confirm_futures_settings(&[], "MSFTUSDT", 3, isolated).is_err());
        assert!(confirm_futures_settings(&row("isolated", "3"), "QQQUSDT", 3, isolated).is_err());
    }

    #[tokio::test]
    async fn recently_confirmed_settings_skip_their_round_trips_and_a_refusal_drops_the_rules() {
        // The key cannot even form a request, so `Ok` proves no request was needed.
        let broker = offline_broker(BinanceAccount::Futures);
        assert!(broker.check_account_settings(false).await.is_err());
        broker.settings_ok.mark(true);
        assert!(broker.check_account_settings(true).await.is_ok());
        assert!(
            broker.check_account_settings(false).await.is_ok(),
            "an opening check covers the exit checks"
        );
        let exits_only = offline_broker(BinanceAccount::Futures);
        exits_only.settings_ok.mark(false);
        assert!(exits_only.check_account_settings(false).await.is_ok());
        assert!(
            exits_only.check_account_settings(true).await.is_err(),
            "an exit check does not cover the opening checks"
        );

        let key = ("DOGEUSDT".to_string(), 2, crate::MarginMode::Isolated);
        broker.symbol_settings.mark(key);
        assert!(
            broker
                .set_isolated("DOGEUSDT", 2, crate::MarginMode::Isolated)
                .await
                .is_ok()
        );
        assert!(
            broker
                .set_isolated("DOGEUSDT", 2, crate::MarginMode::Cross)
                .await
                .is_err(),
            "another mode is not covered"
        );
        assert!(
            broker
                .set_isolated("DOGEUSDT", 3, crate::MarginMode::Isolated)
                .await
                .is_err(),
            "another leverage is not covered"
        );

        broker.instruments.put(Arc::new(ExchangeInfo {
            symbols: vec![raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING")],
        }));
        broker
            .leverage_limits
            .lock()
            .unwrap()
            .insert("BTCUSDT".into(), (Instant::now(), 125));
        assert_eq!(broker.exchange_info().await.unwrap().symbols.len(), 1);
        assert_eq!(broker.max_leverage("BTCUSDT").await.unwrap(), 125);
        broker.forget_market_rules();
        assert!(broker.instruments.get(INSTRUMENTS_TTL).is_none());
        assert!(broker.leverage_limits.lock().unwrap().is_empty());
        assert!(
            !broker.symbol_settings.is_fresh(&(
                "DOGEUSDT".to_string(),
                2,
                crate::MarginMode::Isolated
            )),
            "a refusal also re-confirms the symbol's margin mode and leverage"
        );
        // Reconciliation meeting a market the cached index does not know drops the cache too.
        broker.instruments.put(Arc::new(ExchangeInfo {
            symbols: vec![raw_symbol("BTCUSDT", "BTC", "PERPETUAL", "TRADING")],
        }));
        assert_eq!(broker.symbol_index().await.unwrap().len(), 1);
        let _ = broker.unmapped("a new listing");
        assert!(broker.instruments.get(INDEX_TTL).is_none());
    }

    #[test]
    fn history_windows_cover_the_range_without_gaps_and_split_in_half() {
        assert_eq!(split_windows(0, 10, 4), [(0, 4), (5, 9), (10, 10)]);
        assert_eq!(split_windows(7, 7, 4), [(7, 7)]);
        assert!(split_windows(5, 4, 4).is_empty());
        let day = 24 * 60 * 60 * 1_000;
        let windows = split_windows(0, 20 * day, TRADE_WINDOW_MS);
        assert_eq!(windows.len(), 3);
        assert_eq!(windows.first().unwrap().0, 0);
        assert_eq!(windows.last().unwrap().1, 20 * day);
        for pair in windows.windows(2) {
            assert_eq!(pair[0].1 + 1, pair[1].0);
        }
        assert!(
            windows
                .iter()
                .all(|(from, to)| to - from <= TRADE_WINDOW_MS)
        );
        assert!(
            TRADE_WINDOW_MS < 7 * day,
            "userTrades refuses 7 days or more"
        );
        assert_eq!(halve(0, 9), Some([(0, 4), (5, 9)]));
        assert_eq!(halve(3, 4), Some([(3, 3), (4, 4)]));
        assert_eq!(halve(3, 3), None);
    }

    #[test]
    fn history_rows_must_sit_in_their_window_and_are_kept_once() {
        let rows = |pairs: &[(i64, i64)]| pairs.to_vec();
        let time = |row: &(i64, i64)| Some(row.1);
        let keep = |row: &(i64, i64)| (row.0 % 2 == 0).then_some(row.0);
        let (mut seen, mut kept) = (HashSet::new(), Vec::new());
        collect_page(
            rows(&[(2, 10), (3, 11), (4, 12)]),
            (10, 20),
            &time,
            &keep,
            &mut seen,
            &mut kept,
        )
        .unwrap();
        // The same row again from the other half of a split window is not counted twice.
        collect_page(
            rows(&[(4, 12), (6, 20)]),
            (10, 20),
            &time,
            &keep,
            &mut seen,
            &mut kept,
        )
        .unwrap();
        assert_eq!(kept, [(2, 10), (4, 12), (6, 20)]);
        // A row outside the window — even one that would not be kept — refuses the page: the
        // venue ignored the window.
        let outside = collect_page(
            rows(&[(3, 21)]),
            (10, 20),
            &time,
            &keep,
            &mut seen,
            &mut kept,
        );
        assert!(outside.is_err());
        let untimed = collect_page(
            rows(&[(8, 15)]),
            (10, 20),
            &|_| None,
            &keep,
            &mut seen,
            &mut kept,
        );
        assert!(untimed.is_err());
    }

    #[test]
    fn history_older_than_three_months_is_refused_not_returned_short() {
        let day = 24 * 60 * 60 * 1_000;
        let now = 200 * day;
        assert!(within_retention(now - 89 * day, now).is_ok());
        assert!(within_retention(now, now).is_ok());
        assert!(within_retention(now - 91 * day, now).is_err());
    }

    #[test]
    fn funding_income_sums_usdt_payments_since_the_start() {
        let income = |amount: &str, asset: &str, at: i64, id: i64| RawIncome {
            symbol: "DOGEUSDT".into(),
            income_type: "FUNDING_FEE".into(),
            income: amount.into(),
            asset: asset.into(),
            time: at,
            tran_id: id,
        };
        let at = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        let rows = [
            income("-0.5", "USDT", 1_000, 1),
            income("0.2", "USDT", 2_000, 2),
        ];
        let total = funding_total(&rows, at(500)).unwrap();
        assert_eq!(
            (total.usdt, total.payments, total.last_at),
            (dec!(-0.3), 2, Some(at(2_000)))
        );
        assert_eq!(funding_total(&rows, at(1_500)).unwrap().payments, 1);
        let none = funding_total(&[], at(0)).unwrap();
        assert_eq!(
            (none.usdt, none.payments),
            (Decimal::ZERO, 0),
            "no rows is zero, not unknown"
        );
        assert!(funding_total(&[income("1", "BNB", 1_000, 3)], at(0)).is_err());
        let decoded: RawIncome = serde_json::from_value(serde_json::json!({
            "symbol": "DOGEUSDT", "incomeType": "FUNDING_FEE", "income": "-0.0012",
            "asset": "USDT", "info": "FUNDING_FEE", "time": 1570636800000_i64,
            "tranId": 9689322392_i64, "tradeId": ""
        }))
        .unwrap();
        assert_eq!((decoded.tran_id, decoded.time), (9689322392, 1570636800000));
    }

    #[test]
    fn trade_rows_become_time_ordered_fills_and_bad_rows_refuse_the_list() {
        let trade = |id: i64, side: Option<&str>, at: Option<i64>, asset: &str| RawTrade {
            id,
            order_id: 1,
            symbol: "DOGEUSDT".into(),
            price: "0.2".into(),
            qty: "50".into(),
            commission: "0.005".into(),
            commission_asset: asset.into(),
            side: side.map(str::to_string),
            time: at,
        };
        let fills = venue_fills(vec![
            trade(2, Some("SELL"), Some(2_000), "USDT"),
            trade(1, Some("BUY"), Some(1_000), "USDT"),
        ])
        .unwrap();
        assert_eq!(
            fills.iter().map(|fill| fill.side).collect::<Vec<_>>(),
            [Side::Buy, Side::Sell]
        );
        assert_eq!(
            (fills[0].quantity, fills[0].price, fills[0].fee_usdt),
            (dec!(50), dec!(0.2), dec!(0.005))
        );
        assert_eq!(fills[0].effect, FillEffect::Unknown);
        assert!(venue_fills(vec![trade(1, Some("BUY"), Some(1), "BNB")]).is_err());
        assert!(venue_fills(vec![trade(1, None, Some(1), "USDT")]).is_err());
        assert!(venue_fills(vec![trade(1, Some("BUY"), None, "USDT")]).is_err());
        let decoded: RawTrade = serde_json::from_value(serde_json::json!({
            "buyer": false, "commission": "-0.07819010", "commissionAsset": "USDT", "id": 698759,
            "maker": false, "orderId": 25851813, "price": "7819.01", "qty": "0.002",
            "quoteQty": "15.63802", "realizedPnl": "-0.91539999", "side": "SELL",
            "positionSide": "BOTH", "symbol": "BTCUSDT", "time": 1569514978020_i64
        }))
        .unwrap();
        assert_eq!(
            (decoded.side.as_deref(), decoded.time),
            (Some("SELL"), Some(1569514978020))
        );
    }

    #[test]
    fn classic_free_collateral_is_the_usdt_available_balance() {
        let balances =
            |rows: serde_json::Value| -> Vec<RawBalance> { serde_json::from_value(rows).unwrap() };
        let rows = balances(serde_json::json!([
            {"accountAlias": "x", "asset": "BNB", "balance": "1", "availableBalance": "1"},
            {"accountAlias": "x", "asset": "USDT", "balance": "5000.00000000",
             "crossWalletBalance": "5000", "availableBalance": "4800.5", "marginAvailable": true}
        ]));
        assert_eq!(futures_free_collateral(&rows).unwrap(), dec!(4800.5));
        let negative = balances(serde_json::json!([{"asset": "USDT", "availableBalance": "-3"}]));
        assert_eq!(futures_free_collateral(&negative).unwrap(), Decimal::ZERO);
        assert_eq!(futures_free_collateral(&[]).unwrap(), Decimal::ZERO);
        let garbage = balances(serde_json::json!([{"asset": "USDT", "availableBalance": "n/a"}]));
        assert!(futures_free_collateral(&garbage).is_err());
    }

    #[test]
    fn warming_records_only_the_margin_mode_actually_in_force() {
        let rows: Vec<RawPosition> = serde_json::from_value(serde_json::json!([
            {"symbol": "ETHUSDT", "positionAmt": "0", "entryPrice": "0", "markPrice": "1",
             "marginType": "isolated", "positionSide": "BOTH", "leverage": "5"},
            {"symbol": "BTCUSDT", "positionAmt": "0.000", "entryPrice": "0.0", "markPrice": "83000",
             "marginType": "cross", "positionSide": "BOTH", "leverage": "5"}
        ]))
        .unwrap();
        assert_eq!(
            futures_mode_in_force(&rows, "BTCUSDT", 5),
            Some(crate::MarginMode::Cross)
        );
        assert_eq!(
            futures_mode_in_force(&rows, "ETHUSDT", 5),
            Some(crate::MarginMode::Isolated)
        );
        assert_eq!(futures_mode_in_force(&rows, "BTCUSDT", 3), None);
        assert_eq!(futures_mode_in_force(&rows, "SOLUSDT", 5), None);
    }

    #[test]
    fn margin_is_only_added_to_an_isolated_position_in_whole_cents() {
        let rows = |margin: &str, amount: &str| -> Vec<RawPosition> {
            serde_json::from_value(serde_json::json!([
                {"symbol": "DOGEUSDT", "positionAmt": amount, "entryPrice": "0.2",
                 "markPrice": "0.2", "marginType": margin, "positionSide": "BOTH",
                 "isolatedWallet": "5", "leverage": "2"}
            ]))
            .unwrap()
        };
        assert!(isolated_position_for_margin(&rows("isolated", "50"), "DOGEUSDT").is_ok());
        assert!(isolated_position_for_margin(&rows("isolated", "-50"), "DOGEUSDT").is_ok());
        assert!(isolated_position_for_margin(&rows("cross", "50"), "DOGEUSDT").is_err());
        assert!(isolated_position_for_margin(&rows("isolated", "0"), "DOGEUSDT").is_err());
        assert!(isolated_position_for_margin(&rows("isolated", "50"), "BTCUSDT").is_err());
        assert_eq!(margin_amount_text(dec!(1)).unwrap(), "1");
        assert_eq!(margin_amount_text(dec!(12.50)).unwrap(), "12.5");
        assert!(margin_amount_text(Decimal::ZERO).is_err());
        assert!(margin_amount_text(dec!(-1)).is_err());
        assert!(margin_amount_text(dec!(1.234)).is_err());
    }

    #[test]
    fn a_margin_reply_is_applied_or_refused_only_when_the_venue_says_so() {
        let reply = |status: u16, body: serde_json::Value| {
            classify_margin_reply(StatusCode::from_u16(status).unwrap(), &body)
        };
        assert_eq!(
            reply(
                200,
                serde_json::json!({"amount": 1.0, "code": 200, "msg": "Successfully modify position margin.", "type": 1})
            ),
            MarginOutcome::Applied
        );
        assert_eq!(
            reply(200, serde_json::json!({"amount": 1.0, "type": 1})),
            MarginOutcome::Applied
        );
        for code in [-4049, -4050, -4051, -4054, -4055, -1102, -2015, -1021] {
            assert!(
                matches!(
                    reply(400, serde_json::json!({"code": code, "msg": "x"})),
                    MarginOutcome::Refused(_)
                ),
                "{code}"
            );
        }
        for (status, code) in [
            (400, -1007),
            (400, -1000),
            (400, -9999),
            (503, -1001),
            (502, -4051),
        ] {
            assert!(
                matches!(
                    reply(status, serde_json::json!({"code": code, "msg": "x"})),
                    MarginOutcome::Unknown(_)
                ),
                "{status} {code}"
            );
        }
    }

    /// Read-only probe of the real account in `ARB_BINANCE_API_KEY` / `ARB_BINANCE_API_SECRET`
    /// / `ARB_BINANCE_ACCOUNT`: no write request is possible (trading disabled). Reports the
    /// account checks, every read the bot relies on (even when a check fails) and how long the
    /// reconciliation reads and the warm-up take cold and cached. Journal in `ARB_PROBE_DIR`
    /// (default: the system temp dir). Run with
    /// `cargo test -p arb-exec --lib binance_broker::tests::live_read_only_probe -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "needs live Binance credentials"]
    async fn live_read_only_probe() {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        let credentials = BinanceCredentials {
            api_key: var("ARB_BINANCE_API_KEY").expect("ARB_BINANCE_API_KEY"),
            api_secret: var("ARB_BINANCE_API_SECRET").expect("ARB_BINANCE_API_SECRET"),
        };
        let account = BinanceAccount::parse(var("ARB_BINANCE_ACCOUNT").as_deref()).unwrap();
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let identity = format!(
            "binance-probe:{}",
            short_hash(credentials.api_key.as_bytes())
        );
        let journal =
            OrderJournal::open(&dir.join("arb-live-binance-probe-orders.jsonl"), &identity)
                .unwrap();
        let client = Client::new();
        let offset = server_time_offset_ms(&client, &credentials.api_key)
            .await
            .unwrap();
        let broker = BinanceBroker::assemble(
            client.clone(),
            credentials,
            account,
            LiveOptions {
                trading_enabled: false,
                market_slippage: None,
            },
            offset,
            journal,
        );
        let ms = |since: Instant| since.elapsed().as_millis();
        println!("account type: {account:?}; clock offset {offset} ms");
        for round in ["first", "cached"] {
            let started = Instant::now();
            let checks = broker.check_account_settings(true).await;
            println!("account checks ({round}): {checks:?} ({} ms)", ms(started));
        }
        println!("taker fee: {:?}", broker.account_taker_fee().await);
        println!("free collateral: {:?}", broker.free_collateral().await);
        // What reconciliation pays per round: cold (1.15 MB exchangeInfo), then cached.
        broker.instruments.clear();
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
        let btc = Symbol::perp("BTC", QUOTE);
        for round in ["first", "second"] {
            let started = Instant::now();
            let warmed = broker.warm_reads(&btc, Some(dec!(5))).await;
            println!(
                "warm_reads(BTC, 5x) {round}: {warmed:?} ({} ms)",
                ms(started)
            );
        }
        for mode in [crate::MarginMode::Isolated, crate::MarginMode::Cross] {
            println!(
                "BTCUSDT 5x {} recorded as in force: {}",
                mode.as_str(),
                broker
                    .symbol_settings
                    .is_fresh(&("BTCUSDT".to_string(), 5, mode))
            );
        }
        println!(
            "BTCUSDT max leverage: {:?}",
            broker.max_leverage("BTCUSDT").await
        );
        println!("BTC leg state: {:?}", broker.leg_state(&btc).await);
        println!(
            "order lookup of an unknown id: {:?}",
            broker
                .lookup_order("BTCUSDT", "arb-probe-unknown")
                .await
                .map(|raw| raw.is_some())
        );
        let now = chrono::Utc::now();
        let started = Instant::now();
        println!(
            "funding since 30 days: {:?} ({} ms)",
            broker
                .funding_since(&btc, now - chrono::Duration::days(30))
                .await,
            ms(started)
        );
        for days in [1, 20] {
            let started = Instant::now();
            let fills = broker
                .fills_between(&btc, now - chrono::Duration::days(days), now)
                .await;
            println!(
                "fills over {days} day(s): {:?} ({} ms)",
                fills.map(|rows| rows.map(|rows| rows.len())),
                ms(started)
            );
        }
        println!(
            "funding since 100 days (beyond retention, must refuse): {:?}",
            broker
                .funding_since(&btc, now - chrono::Duration::days(100))
                .await
                .map(|total| total.map(|total| total.payments))
        );
        // Request weight this IP has used this minute (shared with every process on the host).
        let weight = client
            .get(format!("{BASE_URL}/fapi/v1/ping"))
            .send()
            .await
            .ok()
            .and_then(|reply| {
                reply
                    .headers()
                    .get("x-mbx-used-weight-1m")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            });
        println!("used request weight (1m, this IP): {weight:?}");
    }

    /// **Real-money** round trip on the account in `ARB_BINANCE_*`: a tiny IOC open on
    /// `ARB_PROBE_SYMBOL` (default DOGE) at 2x — isolated, or cross with `ARB_PROBE_MARGIN=cross`
    /// (always cross on the unified account) — then its fills from the trade list, a 1 USDT
    /// isolated top-up (classic account, isolated only; it returns with the close), and the
    /// exact quantity closed reduce-only; the account must end flat with no open orders.
    /// Exercises warm-up, leverage + read-back, the submission check, fill attribution, the
    /// margin top-up and the reduce-only exit before Binance joins an arbitrage. Needs
    /// `--ignored` **and** `ARB_PROBE_TRADE=yes`; refuses to start unless the whole account is
    /// flat. Journal in `ARB_PROBE_DIR` (default: the system temp dir).
    #[tokio::test]
    #[ignore = "places real orders"]
    async fn live_round_trip_probe() {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        assert_eq!(
            var("ARB_PROBE_TRADE").as_deref(),
            Some("yes"),
            "this probe trades real money; set ARB_PROBE_TRADE=yes to run it"
        );
        let account = BinanceAccount::parse(var("ARB_BINANCE_ACCOUNT").as_deref()).unwrap();
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let broker = BinanceBroker::connect(
            Client::new(),
            BinanceCredentials {
                api_key: var("ARB_BINANCE_API_KEY").expect("ARB_BINANCE_API_KEY"),
                api_secret: var("ARB_BINANCE_API_SECRET").expect("ARB_BINANCE_API_SECRET"),
            },
            account,
            &dir.join("arb-live-binance-probe-trade-orders.jsonl"),
            LiveOptions {
                trading_enabled: true,
                market_slippage: Some(dec!(0.005)),
            },
        )
        .await
        .expect("connect");
        let ms = |since: Instant| since.elapsed().as_millis();
        let base = var("ARB_PROBE_SYMBOL").unwrap_or_else(|| "DOGE".into());
        let symbol = Symbol::perp(&base, QUOTE);
        assert!(
            broker.positions().await.unwrap().is_empty(),
            "account must start flat"
        );
        assert!(
            broker.open_orders().await.unwrap().is_empty(),
            "account must have no orders"
        );
        let instrument =
            select_instrument(&broker.exchange_info().await.unwrap().symbols, &symbol).unwrap();
        // Twice the venue minimum: rounding the quantity down must not drop below it.
        let notional = (instrument.min_notional * dec!(2)).max(dec!(10));
        let stamp = now_ms().unwrap();
        // The dashboard opens isolated by default; the unified account only has cross.
        let mode = if account == BinanceAccount::Unified
            || var("ARB_PROBE_MARGIN").as_deref() == Some("cross")
        {
            crate::MarginMode::Cross
        } else {
            crate::MarginMode::Isolated
        };
        println!(
            "account {account:?}; {} notional {notional} at 2x {}",
            instrument.symbol,
            mode.as_str()
        );
        let open = NewOrder {
            post_only: false,
            client_order_id: ClientOrderId(format!("probe-{stamp}-open")),
            venue: VENUE,
            symbol: symbol.clone(),
            side: Side::Buy,
            notional_usdt: notional,
            quantity: None,
            limit_price: None,
            reduce_only: false,
            margin_mode: mode,
            leverage: Some(dec!(2)),
        };
        let started = Instant::now();
        let warmed = broker.warm_reads(&symbol, Some(dec!(2))).await;
        println!("warm_reads: {warmed:?} ({} ms)", ms(started));
        let started = Instant::now();
        let prepared = broker
            .prepare_open_mode(&symbol, Side::Buy, Some(dec!(2)), mode)
            .await;
        println!("prepare: {prepared:?} ({} ms)", ms(started));
        prepared.unwrap();
        let window_start = chrono::Utc::now() - chrono::Duration::seconds(5);
        let started = Instant::now();
        let placed = broker.place(&open).await;
        println!("open: {placed:?} ({} ms)", ms(started));
        let opened = broker.order_state(&open.client_order_id).await;
        println!("open state: {opened:?}");
        // Whatever happened above, close what is actually held before asserting anything.
        let held = broker
            .positions()
            .await
            .unwrap()
            .into_iter()
            .find(|position| position.symbol == symbol);
        println!("held after open: {held:?}");
        let fills = broker
            .fills_between(
                &symbol,
                window_start,
                chrono::Utc::now() + chrono::Duration::seconds(5),
            )
            .await;
        println!("fills from the trade list: {fills:?}");
        let mut top_up = None;
        if held.is_some() && broker.supports_add_margin() && !mode.is_cross() {
            let before = broker.leg_state(&symbol).await;
            let outcome = broker.add_margin(&symbol, dec!(1)).await;
            let after = broker.leg_state(&symbol).await;
            println!("add 1 USDT margin: {outcome:?}; margin before {before:?}, after {after:?}");
            let margin = |state: &ArbResult<Option<crate::VenueLegState>>| {
                state
                    .as_ref()
                    .ok()
                    .and_then(|state| state.as_ref())
                    .and_then(|state| state.margin_usdt)
            };
            top_up = Some((outcome, margin(&before), margin(&after)));
        }
        println!(
            "funding since the open: {:?}",
            broker.funding_since(&symbol, window_start).await
        );
        if let Some(position) = &held {
            let quantity = position.net_quantity.abs();
            let close = NewOrder {
                post_only: false,
                client_order_id: ClientOrderId(format!("probe-{stamp}-close")),
                venue: VENUE,
                symbol: symbol.clone(),
                side: if position.net_quantity > Decimal::ZERO {
                    Side::Sell
                } else {
                    Side::Buy
                },
                notional_usdt: position.notional_usdt,
                quantity: Some(quantity),
                limit_price: None,
                reduce_only: true,
                margin_mode: mode,
                leverage: None,
            };
            let started = Instant::now();
            let closed = broker.place(&close).await;
            println!("close: {closed:?} ({} ms)", ms(started));
            println!(
                "close state: {:?}",
                broker.order_state(&close.client_order_id).await
            );
        }
        let after = broker.positions().await.unwrap();
        let orders = broker.open_orders().await.unwrap();
        println!("after: positions {after:?}; open orders {}", orders.len());
        assert!(
            after.is_empty(),
            "the probe left a position; close it by hand"
        );
        assert!(
            orders.is_empty(),
            "the probe left an open order; cancel it by hand"
        );
        let opened = opened.unwrap().expect("open order is journaled");
        assert!(
            placed.is_ok() && opened.filled_usdt > Decimal::ZERO,
            "the open did not fill"
        );
        let held = held.expect("the open left no position");
        let bought: Decimal = fills
            .unwrap()
            .unwrap_or_default()
            .iter()
            .filter(|fill| fill.side == Side::Buy)
            .map(|fill| fill.quantity)
            .sum();
        assert_eq!(
            bought, held.net_quantity,
            "the trade list must hold the open fill"
        );
        if let Some((outcome, before, after)) = top_up {
            assert_eq!(outcome.unwrap(), MarginOutcome::Applied);
            let (before, after) = (before.unwrap(), after.unwrap());
            assert!(after - before >= dec!(0.99), "margin {before} -> {after}");
        }
    }

    /// **Real-money, zero-cost** post-only (`GTX`) probe on the account in `ARB_BINANCE_*`, on
    /// `ARB_PROBE_SYMBOL` (default DOGE): (1) a post-only BUY far below the bid must rest — read
    /// back as an open `GTX` limit at the submitted price, listed in the open orders, still
    /// resting after more than 5 seconds, then cancelled with nothing filled; (2) a post-only BUY
    /// above the ask would trade at once and must be refused (-5022) with no order left behind.
    /// The account must end flat with no open orders. Needs `--ignored` **and**
    /// `ARB_PROBE_TRADE=yes`; refuses to start unless the whole account is flat.
    #[tokio::test]
    #[ignore = "places real orders"]
    async fn live_post_only_probe() {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        assert_eq!(
            var("ARB_PROBE_TRADE").as_deref(),
            Some("yes"),
            "this probe places real orders; set ARB_PROBE_TRADE=yes to run it"
        );
        let account = BinanceAccount::parse(var("ARB_BINANCE_ACCOUNT").as_deref()).unwrap();
        assert_eq!(
            account,
            BinanceAccount::Futures,
            "the post-only path is for the classic futures account"
        );
        let dir = var("ARB_PROBE_DIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let broker = BinanceBroker::connect(
            Client::new(),
            BinanceCredentials {
                api_key: var("ARB_BINANCE_API_KEY").expect("ARB_BINANCE_API_KEY"),
                api_secret: var("ARB_BINANCE_API_SECRET").expect("ARB_BINANCE_API_SECRET"),
            },
            account,
            &dir.join("arb-live-binance-probe-postonly-orders.jsonl"),
            LiveOptions {
                trading_enabled: true,
                market_slippage: Some(dec!(0.005)),
            },
        )
        .await
        .expect("connect");
        let base = var("ARB_PROBE_SYMBOL").unwrap_or_else(|| "DOGE".into());
        let symbol = Symbol::perp(&base, QUOTE);
        assert!(broker.positions().await.unwrap().is_empty(), "start flat");
        assert!(broker.open_orders().await.unwrap().is_empty(), "no orders");
        let instrument =
            select_instrument(&broker.exchange_info().await.unwrap().symbols, &symbol).unwrap();
        let depth: Depth = decode(
            broker
                .public_get(
                    "/fapi/v1/depth",
                    &[
                        ("symbol", instrument.symbol.clone()),
                        ("limit", "5".to_string()),
                    ],
                )
                .await
                .unwrap(),
        )
        .unwrap();
        let bid = top_of_book(&depth.bids).unwrap().expect("a bid");
        let ask = top_of_book(&depth.asks).unwrap().expect("an ask");
        println!("{} book: bid {bid} ask {ask}", instrument.symbol);
        let notional = (instrument.min_notional * dec!(2)).max(dec!(10));
        let stamp = now_ms().unwrap();
        let post_only = |tag: &str, price: Decimal| NewOrder {
            post_only: true,
            client_order_id: ClientOrderId(format!("probe-{stamp}-{tag}")),
            venue: VENUE,
            symbol: symbol.clone(),
            side: Side::Buy,
            notional_usdt: notional,
            quantity: None,
            limit_price: Some(price),
            reduce_only: false,
            margin_mode: crate::MarginMode::Isolated,
            leverage: Some(dec!(2)),
        };
        // 1.5% under the bid: far from the market and inside the venue's price band (TradFi
        // contracts accept a buy no lower than 97% of the mark price).
        let far_price = round_price(bid * dec!(0.985), instrument.tick_size, Side::Buy).unwrap();
        let resting = post_only("rest", far_price);
        let crossing = post_only(
            "cross",
            round_price(ask * dec!(1.01), instrument.tick_size, Side::Buy).unwrap(),
        );
        broker.warm_reads(&symbol, Some(dec!(2))).await.unwrap();
        broker
            .prepare_open_mode(
                &symbol,
                Side::Buy,
                Some(dec!(2)),
                crate::MarginMode::Isolated,
            )
            .await
            .unwrap();

        // 1. Rests.
        let started = Instant::now();
        let placed = broker.place(&resting).await;
        println!(
            "resting order: {placed:?} ({} ms)",
            started.elapsed().as_millis()
        );
        let first = broker.order_state(&resting.client_order_id).await;
        println!("state right after: {first:?}");
        let listed = broker.open_orders().await;
        println!("open orders: {:?}", listed.as_ref().map(Vec::len));
        tokio::time::sleep(std::time::Duration::from_millis(5_500)).await;
        let later = broker.order_state(&resting.client_order_id).await;
        println!("state after 5.5 s: {later:?}");
        let venue_id = placed.as_ref().ok().map(|ack| ack.venue_order_id.clone());
        let cancelled = match &venue_id {
            Some(id) => Some(broker.cancel(id).await),
            None => None,
        };
        println!("cancel: {cancelled:?}");
        let final_state = broker.order_state(&resting.client_order_id).await;
        println!("final state: {final_state:?}");

        // 2. Would trade at once: refused, nothing left behind. Only tried once step 1 PROVED the
        // venue honours GTX (the order rested for more than 5 s and read back as post-only) —
        // fapi silently ignores parameters it does not know, and without GTX this order would be
        // an ordinary marketable limit that really trades.
        let rested = placed.is_ok()
            && matches!(&first, Ok(Some(state)) if state.status == OrderStatus::Open)
            && matches!(&later, Ok(Some(state)) if state.status == OrderStatus::Open);
        let (refused, refused_state) = if rested {
            let refused = broker.place(&crossing).await;
            println!("crossing order: {refused:?}");
            let state = broker.order_state(&crossing.client_order_id).await;
            println!("crossing state: {state:?}");
            (Some(refused), Some(state))
        } else {
            println!("step 1 did not prove GTX; the crossing order is NOT sent");
            (None, None)
        };

        // Leave nothing behind before asserting: cancel what rests, close what is held.
        for order in broker.open_orders().await.unwrap() {
            if let Some(id) = &order.venue_order_id {
                let _ = broker.cancel(id).await;
            }
        }
        for position in broker.positions().await.unwrap() {
            let close = NewOrder {
                post_only: false,
                client_order_id: ClientOrderId(format!("probe-{stamp}-close")),
                venue: VENUE,
                symbol: position.symbol.clone(),
                side: if position.net_quantity > Decimal::ZERO {
                    Side::Sell
                } else {
                    Side::Buy
                },
                notional_usdt: position.notional_usdt,
                quantity: Some(position.net_quantity.abs()),
                limit_price: None,
                reduce_only: true,
                margin_mode: crate::MarginMode::Isolated,
                leverage: None,
            };
            println!("closing a held position: {:?}", broker.place(&close).await);
        }
        let positions = broker.positions().await.unwrap();
        let orders = broker.open_orders().await.unwrap();
        assert!(
            positions.is_empty(),
            "the probe left a position: {positions:?}"
        );
        assert!(orders.is_empty(), "the probe left an order: {orders:?}");

        let placed = placed.expect("the far post-only order was refused");
        assert_eq!(placed.status, OrderStatus::Open, "it must rest, not fill");
        let first = first.unwrap().expect("journaled");
        assert!(first.order.post_only);
        assert_eq!(first.status, OrderStatus::Open);
        assert_eq!(first.filled_usdt, Decimal::ZERO);
        assert!(
            listed
                .unwrap()
                .iter()
                .any(|order| order.venue_order_id == Some(placed.venue_order_id.clone())),
            "open_orders must list the resting order"
        );
        assert_eq!(
            later.unwrap().expect("journaled").status,
            OrderStatus::Open,
            "still resting after 5 s"
        );
        assert!(cancelled.unwrap().is_ok(), "the cancel failed");
        let final_state = final_state.unwrap().expect("journaled");
        assert_eq!(final_state.status, OrderStatus::Cancelled);
        assert_eq!(final_state.filled_usdt, Decimal::ZERO);
        let refused = refused.expect("step 1 proved GTX, so the crossing order was tried");
        assert!(
            refused.is_err(),
            "a crossing post-only order must be refused"
        );
        let refused_state = refused_state
            .unwrap()
            .unwrap()
            .expect("the refusal is journaled");
        assert_eq!(refused_state.status, OrderStatus::Rejected);
        assert_eq!(refused_state.filled_usdt, Decimal::ZERO);
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
