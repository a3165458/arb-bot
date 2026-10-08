//! Lighter private broker: Lighter mainnet (chain 304) and the Robinhood Chain deployment
//! (chain 466324) — both selected via [`LighterDeployment`].
//!
//! Signing is delegated to the official `elliottech/lighter-go` v1.0.10 C ABI,
//! never reimplemented. Linux x86_64 only: install the release asset at an absolute,
//! operator-controlled path. Its SHA-256 is pinned below and checked before dlopen.
//! Sources: <https://github.com/elliottech/lighter-go/releases/tag/v1.0.10>,
//! the accompanying `lighter-signer-linux-amd64.h`, and
//! <https://github.com/elliottech/lighter-python/blob/main/docs/OrderApi.md>.
//! Official chain ids: <https://apidocs.lighter.xyz/docs/get-started> and
//! <https://apidocs.rh.lighter.xyz/docs/get-started>. Robinhood documents its API
//! origin and USDG collateral at <https://docs.robinhood.com/chain/lighter-domains/>.
//! Symbols retain the scanner's `Symbol::perp(native_symbol, "USDT")` convention;
//! this does not convert USDG collateral into USDT.
//!
//! The pinned signer's registry is account -> API key index, NOT chain -> account:
//! <https://github.com/elliottech/lighter-go/blob/v1.0.10/client/client.go>.
//! Each client retains its signing chain in `client/tx_client.go`. Distinct account/key
//! pairs may coexist across deployments for the order/cancel/leverage/auth ABI used here,
//! but the same pair cannot: creating it again silently replaces the first client.
//! Registration rejects that collision before CreateClient. Creation is serialized
//! because <https://github.com/elliottech/lighter-go/blob/v1.0.10/sharedlib/main.go>
//! also writes a global chainId (used only for transfer/approve L1 signature bodies,
//! which this broker never invokes). Never extend this broker to those operations
//! without revisiting that global-state restriction.
//!
//! A dedicated, exclusively locked JSONL journal MUST survive restarts. It records
//! intent and nonce reservations before network writes, and terminal observations
//! afterwards. Never delete it to resolve an uncertain submission: reconcile first.
//! A recorded intent is NEVER resubmitted, even when the API's 24-hour/1K inactive
//! order lookup window has expired. Missing history then fails closed.
//! Use a dedicated API key and account; sharing a key with another process is unsafe.
//! `sendTx` is form encoded; its acknowledgement is NOT a fill. Orders are limit IOC
//! with an explicit price bound. Orders without `limit_price` (close/unwind/trim) are
//! bounded from a book fetched immediately before signing: buy ≤ best ask × (1 + s),
//! sell ≥ best bid × (1 − s), with operator-supplied `market_slippage` s. The book
//! endpoint carries no snapshot timestamp, so freshness is limited to a ≤2s round trip.
//! Exact reduce-only quantities are required; opening quantities round down only.
//!
//! `add_margin` tops up an existing ISOLATED position with tx type 29 (`L2UpdateMargin`,
//! signer `SignUpdateMargin`, direction 1 = AddToIsolatedMargin, amount in 1e-6 units of
//! collateral: USDC on mainnet, USDG on Robinhood Chain). The official RH contract at
//! <https://apidocs.lighter.xyz/docs/lighter-rh> specifies the same transaction format,
//! signer and API, with chain 466324 and USDG collateral. This is not deposit-only inference:
//! <https://github.com/elliottech/lighter-python/blob/ebc50660efc99f31e4055418e4514255456cb060/lighter/signer_client.py>
//! `update_margin` multiplies by `USDC_TICKER_SCALE = 1e6` before `sign_update_margin`,
//! including its RH deployment. We use exact Decimal conversion, rejecting rather than
//! truncating sub-unit amounts. These sources establish protocol units, not authenticated
//! production acceptance or observed USDG balance movement.
//!
//! It sends at most ONE `sendTx` per call and never retries (a re-send could add twice). `sendTx`
//! code 200 only means the API accepted the tx, so success is confirmed by reading the
//! position's `allocated_margin` back. An API-server rejection leaves the nonce unconsumed
//! (<https://apidocs.lighter.xyz/docs/core-concepts>, "When a rejected transaction consumes
//! its nonce"), so the journal records the reservation as released; any other outcome keeps
//! the reservation (fail closed), exactly as for orders.
//! Sources: <https://github.com/elliottech/lighter-go/blob/v1.0.10/types/txtypes/update_margin.go>,
//! <https://github.com/elliottech/lighter-go/blob/v1.0.10/sharedlib/main.go> (`SignUpdateMargin`).

use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString, c_char, c_void};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Side, Symbol, Venue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use libloading::Library;
use reqwest::{Client, RequestBuilder};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenueLegState, VenuePosition};
use crate::live_common::Verified;
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

/// Closed deployment selection prevents mismatched venue, API origin and signing chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LighterDeployment {
    #[default]
    Mainnet,
    Robinhood,
}

impl LighterDeployment {
    pub const fn venue(self) -> Venue {
        match self {
            Self::Mainnet => Venue::Lighter,
            Self::Robinhood => Venue::LighterRh,
        }
    }

    pub const fn base_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://mainnet.zklighter.elliot.ai",
            Self::Robinhood => "https://api.rh.lighter.xyz",
        }
    }

    pub const fn chain_id(self) -> u32 {
        match self {
            Self::Mainnet => 304,
            Self::Robinhood => 466_324,
        }
    }
}
const MAX_INDEX: i64 = (1_i64 << 48) - 1;
/// 只读查询被限频时的退避间隔。Lighter（尤其 RH 部署）按出口 IP 限频、额度很紧，同机其它
/// 程序也在分用；连接阶段一次 429 就会让整个看板启动失败。查询不改变任何状态，重试是安全的；
/// 写请求（`sendTx`）不走这条路，永不自动重试。
/// 杠杆刚核对过多久之内，`place` 不再重新核对。
const LEVERAGE_VERIFIED_TTL: Duration = Duration::from_secs(20);
/// 下单后等订单可查的轮询间隔：前 3 次快一点，之后放慢（读请求和扫描、预检共用同一个出口 IP 的限频）。
const ORDER_POLL_FAST: Duration = Duration::from_millis(100);
const ORDER_POLL_SLOW: Duration = Duration::from_millis(250);
const READ_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
];
/// `add_margin` 发出 `sendTx` 之后回读分配保证金的次数与间隔（共约 3 秒）。读请求和扫描共用同一个
/// 出口 IP 的限频，所以少读几次；窗口内没看到保证金增加就按「结果不明」处理。
const MARGIN_POLLS: usize = 6;
const MARGIN_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// 回读单次请求的超时：回读不重试，卡住的一次读不能把整个窗口拖长。
const MARGIN_READ_TIMEOUT: Duration = Duration::from_secs(5);
/// `TxTypeL2UpdateMargin`（lighter-go `types/txtypes/constants.go`）。
const TX_TYPE_UPDATE_MARGIN: u8 = 29;
/// `AddToIsolatedMargin`（同上）。`RemoveFromIsolatedMargin` = 0 永远不会从这里发出。
const ADD_TO_ISOLATED_MARGIN: i32 = 1;
/// `signer_client.py::update_margin` 的 `USDC_TICKER_SCALE`；RH 同格式，USDG 也按 1e6。
const MARGIN_SCALE: i64 = 1_000_000;
/// 官方 `MaxTransferAmount = MaxExchangeUSDC = 2^60 - 1`；我们另行严格拒绝非正数。
const MARGIN_MAX_WIRE: i64 = (1_i64 << 60) - 1;
/// 回读到的分配保证金至少涨到「补之前 + 这个比例 × 金额」才算生效（给资金费等小幅漂移留余地）。
const MARGIN_CONFIRM_RATIO: Decimal = Decimal::from_parts(99, 0, 0, false, 2);
/// SHA-256 independently matched to GitHub's release-asset `digest` field.
pub const SIGNER_SHA256: &str = "1022a277cbb8fb9647886a611bc02151ad28cdbc5a02f7ac6416faa2898be392";
pub const SIGNER_RELEASE_URL: &str = "https://github.com/elliottech/lighter-go/releases/download/v1.0.10/lighter-signer-linux-amd64.so";

/// Secrets intentionally implement neither Debug nor Serialize.
pub struct LighterConfig {
    /// Defaults to mainnet; choose Robinhood explicitly with its own journal path.
    pub deployment: LighterDeployment,
    pub account_index: i64,
    pub api_key_index: u8,
    api_private_key: String,
    pub signer_library: PathBuf,
    pub journal_path: PathBuf,
    pub trading_enabled: bool,
    /// Fraction (0.002 = 0.2%). Required when trading is enabled: without it the
    /// broker could open positions it cannot exit, since exits carry no limit price.
    pub market_slippage: Option<Decimal>,
}

impl LighterConfig {
    pub fn new(
        account_index: i64,
        api_key_index: u8,
        api_private_key: String,
        signer_library: PathBuf,
        journal_path: PathBuf,
    ) -> Self {
        Self {
            deployment: LighterDeployment::Mainnet,
            account_index,
            api_key_index,
            api_private_key,
            signer_library,
            journal_path,
            trading_enabled: false,
            market_slippage: None,
        }
    }
}

pub struct LighterBroker {
    client: Client,
    deployment: LighterDeployment,
    /// `{base_url}/api/v1/<endpoint>` 的前缀。生产里恒等于部署的官方地址（`deployment.base_url()`）；
    /// 单独成字段只是为了测试能把它指向本地假服务。
    base_url: String,
    signer: Arc<NativeSigner>,
    account: i64,
    key: u8,
    enabled: bool,
    slippage: Option<Decimal>,
    journal: Mutex<Journal>,
    fee: Decimal,
    /// 刚核对（或设置）并读回确认过的（市场, 保证金率）及时刻。开仓前的预热核对一次，
    /// `place` 在短时间内就不必再往返一次；过期后照旧重新核对。
    verified_leverage: Verified<(i32, i32, crate::MarginMode)>,
    /// 预热时按（合约, 杠杆）记下「已经是目标杠杆」：`prepare_open` 据此不必再读市场列表与账户。
    warmed: Verified<(Symbol, Decimal)>,
    _registration: Registration,
}

impl LighterBroker {
    /// Loads the pinned native signer, exclusively locks the journal, validates
    /// authentication via accountActiveOrders, and reads authenticated account fees.
    /// Construction never sends transactions, including leverage updates.
    pub async fn new(client: Client, mut config: LighterConfig) -> ArbResult<Self> {
        if !(1..MAX_INDEX).contains(&config.account_index) || config.api_key_index == 255 {
            return Err(err("invalid explicit account/API key index"));
        }
        if config
            .market_slippage
            .is_some_and(|s| s <= Decimal::ZERO || s >= Decimal::ONE)
        {
            return Err(err("market_slippage must be strictly between zero and one"));
        }
        if config.trading_enabled && config.market_slippage.is_none() {
            return Err(err(
                "enabled trading requires explicit market_slippage so positions stay exitable",
            ));
        }
        let registration = Registration::acquire(
            config.deployment.chain_id(),
            config.account_index,
            config.api_key_index,
        )?;
        let signer = NativeSigner::load(&config.signer_library)?;
        signer.create_client(
            &config.api_private_key,
            config.deployment.chain_id(),
            config.account_index,
            config.api_key_index,
        )?;
        // Do not retain another Rust-side copy after registering with the native signer.
        config.api_private_key.clear();
        let journal = Journal::open(
            &config.journal_path,
            config.deployment,
            config.account_index,
            config.api_key_index,
        )?;
        let mut broker = Self {
            client,
            deployment: config.deployment,
            base_url: config.deployment.base_url().to_string(),
            signer,
            account: config.account_index,
            key: config.api_key_index,
            enabled: config.trading_enabled,
            slippage: config.market_slippage,
            journal: Mutex::new(journal),
            fee: Decimal::ZERO,
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
            warmed: Verified::new(LEVERAGE_VERIFIED_TTL),
            _registration: registration,
        };
        let _: Orders = broker
            .get(
                "accountActiveOrders",
                &[("account_index", broker.account.to_string())],
                true,
            )
            .await?;
        let fees: AccountFees = broker
            .get(
                "accountLimits",
                &[("account_index", broker.account.to_string())],
                true,
            )
            .await?;
        if fees.current_taker_fee_tick < 0 {
            return Err(err("negative account taker fee"));
        }
        broker.fee = Decimal::from(fees.current_taker_fee_tick) / Decimal::from(1_000_000);
        Ok(broker)
    }

    fn authorize_write(&self) -> ArbResult<()> {
        if !self.enabled {
            return Err(err(
                "trading is disabled; no transaction was signed or submitted",
            ));
        }
        Ok(())
    }

    fn url(&self, endpoint: &str) -> String {
        format!("{}/api/v1/{endpoint}", self.base_url)
    }

    fn auth(&self) -> ArbResult<reqwest::header::HeaderValue> {
        let token = self
            .signer
            .auth(Utc::now().timestamp() + 600, self.key, self.account)?;
        let mut value = reqwest::header::HeaderValue::from_str(&token)
            .map_err(|_| err("native signer returned an invalid auth token"))?;
        value.set_sensitive(true);
        Ok(value)
    }

    async fn get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: &[(&str, String)],
        auth: bool,
    ) -> ArbResult<T> {
        for delay in READ_RETRY_DELAYS.iter().map(Some).chain([None]) {
            let mut request = self
                .client
                .get(self.url(endpoint))
                .query(query)
                .timeout(Duration::from_secs(15));
            if auth {
                request = request.header(reqwest::header::AUTHORIZATION, self.auth()?);
            }
            match fetch(request, endpoint).await {
                Ok(value) => return Ok(value),
                Err(Failure::Transient(error)) => match delay {
                    Some(delay) => tokio::time::sleep(*delay).await,
                    None => return Err(error),
                },
                Err(Failure::Other(error)) => return Err(error),
            }
        }
        Err(err(format!("{endpoint}: rate limited")))
    }

    async fn markets(&self) -> ArbResult<Vec<Market>> {
        let details: Markets = self.get("orderBookDetails", &[], false).await?;
        Ok(details.order_book_details)
    }

    fn account_query(&self) -> [(&'static str, String); 3] {
        [
            ("by", "index".into()),
            ("value", self.account.to_string()),
            ("active_only", "false".into()),
        ]
    }

    /// 校验 `account` 响应恰好是我们自己的那一个账户。
    fn checked_account(&self, data: Accounts) -> ArbResult<Account> {
        if data.accounts.len() != 1 || data.total != 1 {
            return Err(err("account response is missing or ambiguous"));
        }
        let account = data
            .accounts
            .into_iter()
            .next()
            .ok_or_else(|| err("missing account"))?;
        if account.index != self.account {
            return Err(err("account identity mismatch"));
        }
        Ok(account)
    }

    async fn account(&self) -> ArbResult<Account> {
        let data: Accounts = self.get("account", &self.account_query(), true).await?;
        self.checked_account(data)
    }

    /// 单次读取：不退避重试、短超时。补保证金发出写请求之后的回读用它 —— 回读窗口是有限的，
    /// 不能被 `get` 的退避重试（最长约 15 秒一次）拖长；读失败就当这一轮没读到。
    async fn account_once(&self) -> ArbResult<Account> {
        let request = self
            .client
            .get(self.url("account"))
            .query(&self.account_query())
            .timeout(MARGIN_READ_TIMEOUT)
            .header(reqwest::header::AUTHORIZATION, self.auth()?);
        let data: Accounts = response(request, "account").await?;
        self.checked_account(data)
    }

    /// Authenticated account position settings, including flat markets. The REST
    /// initial margin fraction is a percentage string: 5.00 means 5%, i.e. 20x.
    /// This is NOT the integer 1/10000-unit field used by SignUpdateLeverage.
    pub async fn margin_settings(&self) -> ArbResult<Vec<LighterMarginSetting>> {
        self.account()
            .await?
            .positions
            .into_iter()
            .map(|p| {
                let fraction = decimal(&p.initial_margin_fraction)?;
                if fraction <= Decimal::ZERO {
                    return Err(err("invalid account initial margin fraction"));
                }
                Ok(LighterMarginSetting {
                    market_id: p.market_id,
                    symbol: Symbol::perp(p.symbol, "USDT"),
                    isolated: p.margin_mode == 1,
                    leverage: Decimal::from(100) / fraction,
                })
            })
            .collect()
    }

    async fn remote_order(&self, index: i64) -> ArbResult<Option<RemoteOrder>> {
        let response: Orders = self
            .get(
                "accountOrders",
                &[
                    ("account_index", self.account.to_string()),
                    ("client_order_indexes", index.to_string()),
                ],
                true,
            )
            .await?;
        if response.orders.len() > 1 {
            return Err(err("ambiguous client order index"));
        }
        let row = response.orders.into_iter().next();
        if let Some(row) = &row
            && (row.client_order_index != index || row.owner_account_index != self.account)
        {
            return Err(err("order lookup returned another account or client index"));
        }
        Ok(row)
    }

    /// One API key's nonce is a server-managed sequence, NOT an arbitrary wall clock.
    /// Persist each reservation before sending. A timeout or unconsumed reservation
    /// blocks further writes instead of guessing whether reusing it is safe.
    async fn reserve_nonce(&self, journal: &mut Journal) -> ArbResult<i64> {
        let next: NextNonce = self
            .get(
                "nextNonce",
                &[
                    ("account_index", self.account.to_string()),
                    ("api_key_index", self.key.to_string()),
                ],
                false,
            )
            .await?;
        if next.nonce < 0 || journal.last_nonce.is_some_and(|last| next.nonce <= last) {
            return Err(err(
                "previous nonce is unresolved; reconcile before another transaction",
            ));
        }
        journal.append(&JournalRecord::Nonce { nonce: next.nonce })?;
        journal.last_nonce = Some(next.nonce);
        Ok(next.nonce)
    }

    /// Bounded IOC price for orders without an explicit limit. Never widens past `s`.
    async fn bound_price(&self, market: &Market, side: Side) -> ArbResult<Decimal> {
        let slippage = self
            .slippage
            .ok_or_else(|| err("orders without a limit price require explicit market_slippage"))?;
        let started = Instant::now();
        let book: Book = self
            .get(
                "orderBookOrders",
                &[
                    ("market_id", market.market_id.to_string()),
                    ("limit", "20".into()),
                ],
                false,
            )
            .await?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err(
                "order book round trip exceeded 2s; refusing a stale price bound",
            ));
        }
        let best = |rows: &[BookOrder], ask: bool| -> ArbResult<Option<Decimal>> {
            let mut best: Option<Decimal> = None;
            for row in rows {
                let (price, size) = (decimal(&row.price)?, decimal(&row.remaining_base_amount)?);
                if price <= Decimal::ZERO || size <= Decimal::ZERO {
                    continue;
                }
                best = Some(match best {
                    None => price,
                    Some(b) if ask => b.min(price),
                    Some(b) => b.max(price),
                });
            }
            Ok(best)
        };
        let (bid, ask) = (best(&book.bids, false)?, best(&book.asks, true)?);
        if let (Some(bid), Some(ask)) = (bid, ask)
            && ask < bid
        {
            return Err(err("crossed order book; refusing to price an exit"));
        }
        match side {
            Side::Buy => checked_mul(
                ask.ok_or_else(|| err("no asks to buy against"))?,
                Decimal::ONE + slippage,
            ),
            Side::Sell => checked_mul(
                bid.ok_or_else(|| err("no bids to sell against"))?,
                Decimal::ONE - slippage,
            ),
        }
    }

    async fn submit(&self, signed: SignedTx) -> ArbResult<()> {
        self.authorize_write()?;
        let ack: SendAck = response(
            self.client
                .post(self.url("sendTx"))
                .timeout(Duration::from_secs(15))
                .form(&[
                    ("tx_type", signed.kind.to_string()),
                    ("tx_info", signed.info),
                ]),
            "sendTx",
        )
        .await?;
        if ack.tx_hash != signed.hash {
            return Err(err("sendTx hash does not match signed transaction"));
        }
        Ok(())
    }

    /// 把这个市场设成所选保证金模式 + 指定杠杆，并读回确认。`known` 是刚读到的账户（预热时和市场列表并发读），
    /// 没有就现读一次。刚核对过的（短时间内）直接放行，不再往返。
    async fn configure_leverage(
        &self,
        journal: &mut Journal,
        market: &Market,
        leverage: Decimal,
        known: Option<&Account>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        // Ceiling ensures the actual leverage can never exceed the requested risk.
        let imf = margin_fraction(leverage, market.min_initial_margin_fraction)?;
        if self
            .verified_leverage
            .is_fresh(&(market.market_id, imf, mode))
        {
            return Ok(());
        }
        let matches = |a: &Account| leverage_matches(a, market.market_id, imf, mode);
        let fetched;
        let current = match known {
            Some(account) => account,
            None => {
                fetched = self.account().await?;
                &fetched
            }
        };
        if matches(current) {
            self.verified_leverage.mark((market.market_id, imf, mode));
            return Ok(());
        }
        let nonce = self.reserve_nonce(journal).await?;
        let signed =
            self.signer
                .leverage(market.market_id, imf, mode, nonce, self.key, self.account)?;
        self.submit(signed).await?;
        // Read back the selected margin mode and leverage, not just the tx ack.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if matches(&self.account().await?) {
                self.verified_leverage.mark((market.market_id, imf, mode));
                return Ok(());
            }
        }
        Err(err(
            "selected margin mode/leverage update not observed; order was not submitted",
        ))
    }

    async fn lookup(
        &self,
        journal: &mut Journal,
        id: &ClientOrderId,
    ) -> ArbResult<Option<OrderState>> {
        let index = client_index(self.deployment, self.account, id);
        let intent = journal.orders.get(&index).cloned();
        if let Some(intent) = &intent {
            if intent.order.client_order_id != *id {
                return Err(err("client index collision in journal"));
            }
            if let Some(state) = &intent.terminal {
                return Ok(Some(state.clone()));
            }
        }
        let Some(row) = self.remote_order(index).await? else {
            if intent.is_some() {
                return Err(err(
                    "recorded order is unresolved or outside API history; NEVER resubmit it",
                ));
            }
            return Ok(None);
        };
        let markets = self.markets().await?;
        let state = self.convert_order(&row, intent.as_ref(), &markets).await?;
        if state.order.client_order_id != *id {
            return Err(err(
                "remote order has no matching durable intent; restore the correct live journal",
            ));
        }
        self.save_terminal(journal, index, &state)?;
        Ok(Some(state))
    }

    fn save_terminal(
        &self,
        journal: &mut Journal,
        index: i64,
        state: &OrderState,
    ) -> ArbResult<()> {
        if !state.status.is_live() && journal.orders.contains_key(&index) {
            journal.append(&JournalRecord::Terminal {
                index,
                state: state.clone(),
            })?;
            if let Some(intent) = journal.orders.get_mut(&index) {
                intent.terminal = Some(state.clone());
            }
        }
        Ok(())
    }

    async fn convert_order(
        &self,
        row: &RemoteOrder,
        intent: Option<&Intent>,
        markets: &[Market],
    ) -> ArbResult<OrderState> {
        if row.owner_account_index != self.account {
            return Err(err("foreign account order"));
        }
        let market = markets
            .iter()
            .find(|m| m.market_id == row.market_index)
            .ok_or_else(|| err("unknown market in account order"))?;
        if market.market_type != "perp" {
            return Err(err(
                "non-perpetual order in live account; reconcile manually",
            ));
        }
        let initial = decimal(&row.initial_base_amount)?;
        let filled = decimal(&row.filled_base_amount)?;
        let quote = decimal(&row.filled_quote_amount)?;
        let price = decimal(&row.price)?;
        let side = if row.is_ask { Side::Sell } else { Side::Buy };
        let symbol = Symbol::perp(&market.symbol, "USDT");
        if initial <= Decimal::ZERO
            || filled < Decimal::ZERO
            || filled > initial
            || quote < Decimal::ZERO
        {
            return Err(err("invalid cumulative order quantities"));
        }
        let order = if let Some(intent) = intent {
            if intent.market != row.market_index
                || intent.quantity != initial
                || intent.order.side != side
                || intent.order.symbol != symbol
                || intent.order.reduce_only != row.reduce_only
            {
                return Err(err("remote order does not match journal intent"));
            }
            intent.order.clone()
        } else {
            // Expose unknown exchange orders for reconciliation, never hide them.
            NewOrder {
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId(format!(
                    "{}-external-{}",
                    self.deployment.venue(),
                    row.client_order_index
                )),
                venue: self.deployment.venue(),
                symbol,
                side,
                notional_usdt: checked_mul(initial, price)?,
                quantity: Some(initial),
                limit_price: Some(price),
                reduce_only: row.reduce_only,
                leverage: None,
            }
        };
        let status = order_status(&row.status, initial, filled)?;
        let fee = if filled > Decimal::ZERO {
            self.order_fees(row, filled, quote).await?
        } else {
            if !quote.is_zero() {
                return Err(err("quote fill without base fill"));
            }
            Decimal::ZERO
        };
        Ok(OrderState {
            order,
            venue_order_id: Some(format!("{}:{}", row.market_index, row.order_index)),
            status,
            filled_usdt: quote,
            average_price: if filled > Decimal::ZERO {
                Some(quote / filled)
            } else {
                None
            },
            fee_usdt: fee,
            reject_reason: row
                .status
                .starts_with("canceled-")
                .then(|| row.status.clone()),
        })
    }

    async fn order_fees(
        &self,
        order: &RemoteOrder,
        filled: Decimal,
        quote: Decimal,
    ) -> ArbResult<Decimal> {
        let mut cursor = String::new();
        let mut cursors = HashSet::new();
        let mut ids = HashSet::new();
        let mut size = Decimal::ZERO;
        let mut value = Decimal::ZERO;
        let mut fees = Decimal::ZERO;
        loop {
            let mut query = vec![
                ("account_index", self.account.to_string()),
                ("market_id", order.market_index.to_string()),
                ("order_index", order.order_index.to_string()),
                ("sort_by", "trade_id".into()),
                ("sort_dir", "desc".into()),
                ("limit", "100".into()),
                ("aggregate", "false".into()),
            ];
            if !cursor.is_empty() {
                query.push(("cursor", cursor.clone()));
            }
            let page: Trades = self.get("trades", &query, true).await?;
            for trade in page.trades {
                if !ids.insert(trade.trade_id) {
                    return Err(err(
                        "duplicate trade across pages; fee history is not reliable",
                    ));
                }
                let ours_ask =
                    trade.ask_account_id == self.account && trade.ask_id == order.order_index;
                let ours_bid =
                    trade.bid_account_id == self.account && trade.bid_id == order.order_index;
                if ours_ask == ours_bid || trade.market_id != order.market_index {
                    return Err(err("trade does not uniquely belong to this order"));
                }
                let maker = ours_ask == trade.is_maker_ask;
                let (tick, integrator) = trade_fee_ticks(&trade, maker, self.fee)?;
                let amount = decimal(&trade.usd_amount)?;
                let base = decimal(&trade.size)?;
                if amount <= Decimal::ZERO || base <= Decimal::ZERO {
                    return Err(err("invalid trade amounts"));
                }
                size = checked_add(size, base)?;
                value = checked_add(value, amount)?;
                // FeeTick=1e6 in official lighter-go constants; per-trade fee rates
                // include the account tier/discount. Never substitute the market's 0%.
                let rate = checked_add(Decimal::from(tick), Decimal::from(integrator))?
                    / Decimal::from(1_000_000);
                fees = checked_add(fees, checked_mul(amount, rate)?)?;
            }
            match page.next_cursor.filter(|c| !c.is_empty()) {
                Some(next) if cursors.insert(next.clone()) => cursor = next,
                Some(_) => return Err(err("repeated trades cursor")),
                None => break,
            }
        }
        if size != filled || value != quote {
            return Err(err(
                "fills/trades not yet consistent; refusing invented fee or fill",
            ));
        }
        Ok(fees)
    }

    /// `Broker::add_margin` 的实现。`polls` / `interval` 是 `sendTx` 之后回读分配保证金的次数与
    /// 间隔，生产恒为 [`MARGIN_POLLS`] / [`MARGIN_POLL_INTERVAL`]。
    ///
    /// 顺序（前三步任何一步出错都是 `Err`，此时**没有发出任何写请求**）：
    /// 1. 写授权（只读模式在这里返回，之后不会签名、不会建任何请求）与金额换算；
    /// 2. 拿 journal 锁 —— 与 place / cancel / 调杠杆是同一把锁，补保证金不会和下单同时用这把
    ///    key 的 nonce；
    /// 3. 读账户：持仓必须存在、必须是逐仓、必须报告分配保证金（回读要拿它做基准）；
    /// 4. 预留 nonce（落盘）→ 官方签名库签 `SignUpdateMargin` → **恰好发一次** `sendTx`，绝不重试；
    /// 5. API 服务器给出拒绝：官方文档说这种拒绝不消耗 nonce（"Rejected by the API server: fix
    ///    the issue and resend with the same nonce"，<https://apidocs.lighter.xyz/docs/core-concepts>），
    ///    于是把预留记为释放 —— 否则这把 key 之后的任何写（含平仓）都会被 `reserve_nonce` 的
    ///    「上一个 nonce 未决」拦住。其余结果（传输失败、5xx、已受理）保留预留，照旧 fail closed；
    /// 6. `code: 200` 只代表 API 收下了，序列器之后仍可能拒绝 → 回读 `allocated_margin`，涨到
    ///    补之前 + 0.99 × 金额且持仓方向、数量、入场价未变才算 `Applied`；否则就是 `Unknown`。
    ///
    /// 补进去的钱来自哪里，官方文档没写；[INFERENCE] 是全仓可用余额，不够时服务端拒绝（`Refused`）。
    ///
    /// journal 锁一直持有到回读结束（约 3 秒）；`order_state` 等也要这把锁，会在这段时间里排队。
    async fn add_margin_with(
        &self,
        symbol: &Symbol,
        amount_usdt: Decimal,
        polls: usize,
        interval: Duration,
    ) -> ArbResult<MarginOutcome> {
        self.authorize_write()?;
        let usdc = margin_wire_amount(amount_usdt)?;
        let mut journal = self.journal.lock().await;
        let account = self.account().await?;
        let target = margin_target(&account, symbol)?;
        let previous = journal.last_nonce;
        let nonce = self.reserve_nonce(&mut journal).await?;
        let signed = match self
            .signer
            .margin(target.market_id, usdc, nonce, self.key, self.account)
        {
            Ok(signed) => signed,
            Err(error) => {
                // Nothing was sent: the reservation is safe to hand back.
                return Err(match journal.release_nonce(nonce, previous) {
                    Ok(()) => error,
                    Err(release) => err(format!(
                        "{error}; nonce reservation also could not be released: {release}"
                    )),
                });
            }
        };
        match self.send_margin(&signed).await {
            MarginSend::Accepted => {}
            MarginSend::Refused(reason) => {
                return Ok(MarginOutcome::Refused(
                    match journal.release_nonce(nonce, previous) {
                        Ok(()) => reason,
                        Err(release) => format!(
                            "{reason} (nonce reservation not released, further writes stay blocked until reconciled: {release})"
                        ),
                    },
                ));
            }
            MarginSend::Unknown(reason) => {
                return Ok(MarginOutcome::Unknown(format!(
                    "{reason}; nonce {nonce} stays reserved until reconciled"
                )));
            }
        }
        Ok(self
            .confirm_margin(&target, amount_usdt, polls, interval)
            .await)
    }

    fn margin_request(&self, signed: &SignedTx) -> RequestBuilder {
        self.client
            .post(self.url("sendTx"))
            .timeout(Duration::from_secs(15))
            .form(&[
                ("tx_type", signed.kind.to_string()),
                ("tx_info", signed.info.clone()),
            ])
    }

    /// 发出这一次（也是唯一一次）补保证金的 `sendTx`，把响应分成三类。从不重试；错误文本里
    /// 只有 HTTP 状态与 API 码，不回显响应体（网关可能把鉴权头或签名内容原样带回）。
    async fn send_margin(&self, signed: &SignedTx) -> MarginSend {
        let response = match self.margin_request(signed).send().await {
            Ok(response) => response,
            Err(error) => {
                return MarginSend::Unknown(format!(
                    "sendTx transport failure ({}) after the request may have left; outcome unknown",
                    if error.is_timeout() {
                        "timeout"
                    } else {
                        "network"
                    }
                ));
            }
        };
        let status = response.status();
        match response.bytes().await {
            Ok(body) => classify_margin_send(status, &body, &signed.hash),
            Err(_) => MarginSend::Unknown(format!(
                "sendTx HTTP {status}: response body unavailable; outcome unknown"
            )),
        }
    }

    /// `sendTx` 被接受之后的回读：最多 `polls` 次、间隔 `interval`，每次只读一遍账户（不重试）。
    async fn confirm_margin(
        &self,
        target: &MarginTarget,
        amount: Decimal,
        polls: usize,
        interval: Duration,
    ) -> MarginOutcome {
        let mut last = None;
        for _ in 0..polls {
            tokio::time::sleep(interval).await;
            let Ok(account) = self.account_once().await else {
                continue;
            };
            let Some(now) = margin_of(&account, target) else {
                continue;
            };
            if margin_applied(target.before, now, amount) {
                return MarginOutcome::Applied;
            }
            last = Some(now);
        }
        MarginOutcome::Unknown(format!(
            "sendTx accepted (API code 200) but allocated margin did not rise by the requested amount within {polls} reads: before {}, last seen {}, requested +{amount}",
            target.before,
            last.map_or_else(|| "none".to_string(), |margin| margin.to_string()),
        ))
    }
}

#[async_trait]
impl Broker for LighterBroker {
    fn venue(&self) -> Venue {
        self.deployment.venue()
    }
    fn fee_per_side(&self) -> Decimal {
        self.fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.authorize_write()?;
        if order.venue != self.deployment.venue() {
            return Err(ArbError::venue(
                self.deployment.venue().as_str(),
                "order venue does not match this broker's deployment",
            ));
        }
        let mut journal = self.journal.lock().await;
        let index = client_index(self.deployment, self.account, &order.client_order_id);
        if let Some(prior) = journal.orders.get(&index) {
            if serde_json::to_value(&prior.order).map_err(json_error)?
                != serde_json::to_value(order).map_err(json_error)?
            {
                return Err(err(
                    "client id reused with different intent or hash collision",
                ));
            }
            return self
                .lookup(&mut journal, &order.client_order_id)
                .await?
                .as_ref()
                .map(ack)
                .transpose()?
                .ok_or_else(|| err("recorded order is not queryable"));
        }
        // Check the exchange too: another journal/process may have used the index.
        // The market list does not depend on that answer, so read both in one round trip;
        // the index check still decides first.
        let (existing, markets) = tokio::join!(self.remote_order(index), self.markets());
        if existing?.is_some() {
            return Err(err(
                "client index already exists without this journal's intent",
            ));
        }
        let markets = markets?;
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == order.symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market on this deployment"))?;
        let bound = match order.limit_price {
            Some(price) => price,
            None => self.bound_price(market, order.side).await?,
        };
        let prepared = prepare(order, market, bound)?;
        if !order.reduce_only {
            self.configure_leverage(
                &mut journal,
                market,
                order
                    .leverage
                    .ok_or_else(|| err("opens require explicit leverage"))?,
                None,
                order.margin_mode,
            )
            .await?;
        } else {
            let account = self.account().await?;
            let position = account
                .positions
                .iter()
                .find(|p| p.market_id == market.market_id)
                .ok_or_else(|| err("reduce-only order has no position"))?;
            let quantity = decimal(&position.position)?;
            if quantity < prepared.quantity
                || !matches!(
                    (position.sign, order.side),
                    (1, Side::Sell) | (-1, Side::Buy)
                )
            {
                return Err(err(
                    "reduce-only direction/quantity does not reduce the current position",
                ));
            }
        }
        let intent = Intent {
            order: order.clone(),
            market: market.market_id,
            quantity: prepared.quantity,
            terminal: None,
        };
        journal.append(&JournalRecord::Intent {
            index,
            intent: intent.clone(),
        })?;
        journal.orders.insert(index, intent);
        let nonce = self.reserve_nonce(&mut journal).await?;
        let signed = self.signer.order(
            market.market_id,
            index,
            prepared.base,
            prepared.price,
            order.side == Side::Sell,
            order.reduce_only,
            nonce,
            self.key,
            self.account,
        )?;
        self.submit(signed).await?;
        // IOC can still be pending after sendTx. Wait for an observable order;
        // a terminal fill is only reported from its cumulative executed quantities.
        for attempt in 0..40 {
            // A fresh IOC order is normally queryable within ~100ms; poll fast for the first few
            // tries only (every read costs rate-limit budget on a shared IP), then back off.
            tokio::time::sleep(if attempt < 3 {
                ORDER_POLL_FAST
            } else {
                ORDER_POLL_SLOW
            })
            .await;
            if let Some(row) = self.remote_order(index).await? {
                let state = self
                    .convert_order(&row, journal.orders.get(&index), &markets)
                    .await?;
                self.save_terminal(&mut journal, index, &state)?;
                return ack(&state);
            }
        }
        Err(err(
            "submission acknowledged but order not yet queryable; reconcile journal, do not resubmit",
        ))
    }

    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let Some(leverage) = leverage else {
            return Ok(());
        };
        // Read-only: the market list and the account in one round trip. Nothing is changed here;
        // a leverage that still has to be set is left to `prepare_open`.
        let (markets, account) = tokio::join!(self.markets(), self.account());
        let (markets, account) = (markets?, account?);
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == *symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market on this deployment"))?;
        let imf = margin_fraction(leverage, market.min_initial_margin_fraction)?;
        if leverage_matches(&account, market.market_id, imf, crate::MarginMode::Isolated) {
            self.verified_leverage
                .mark((market.market_id, imf, crate::MarginMode::Isolated));
            self.warmed.mark((symbol.clone(), leverage));
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
        self.authorize_write()?;
        let leverage = crate::margin::leverage(self.deployment.venue(), leverage)?;
        let (markets, account) = tokio::join!(self.markets(), self.account());
        let (markets, account) = (markets?, account?);
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == *symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market"))?;
        let mut journal = self.journal.lock().await;
        self.configure_leverage(&mut journal, market, leverage, Some(&account), mode)
            .await
    }

    async fn prepare_open(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        self.authorize_write()?;
        let leverage = leverage.ok_or_else(|| err("opens require explicit leverage"))?;
        // Just verified by `warm_reads`: nothing left to read.
        if self.warmed.is_fresh(&(symbol.clone(), leverage)) {
            return Ok(());
        }
        // The market list and the account are independent reads: one round trip for both.
        let (markets, account) = tokio::join!(self.markets(), self.account());
        let (markets, account) = (markets?, account?);
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == *symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market on this deployment"))?;
        let mut journal = self.journal.lock().await;
        self.configure_leverage(
            &mut journal,
            market,
            leverage,
            Some(&account),
            crate::MarginMode::Isolated,
        )
        .await?;
        self.warmed.mark((symbol.clone(), leverage));
        Ok(())
    }

    async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        self.lookup(&mut *self.journal.lock().await, id).await
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize_write()?;
        let (market, index) = venue_order_id
            .split_once(':')
            .ok_or_else(|| err("expected market:order venue id"))?;
        let market: i32 = market.parse().map_err(|_| err("invalid cancel market"))?;
        let index: i64 = index
            .parse()
            .map_err(|_| err("invalid cancel order index"))?;
        if !(0..=32767).contains(&market) || market == 255 || !(1..(1_i64 << 60)).contains(&index) {
            return Err(err("cancel identifiers out of range"));
        }
        let mut journal = self.journal.lock().await;
        let current: Orders = self
            .get(
                "accountActiveOrders",
                &[("account_index", self.account.to_string())],
                true,
            )
            .await?;
        if !current
            .orders
            .iter()
            .any(|o| o.market_index == market && o.order_index == index)
        {
            return Ok(());
        }
        let nonce = self.reserve_nonce(&mut journal).await?;
        self.submit(
            self.signer
                .cancel(market, index, nonce, self.key, self.account)?,
        )
        .await?;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let active: Orders = self
                .get(
                    "accountActiveOrders",
                    &[("account_index", self.account.to_string())],
                    true,
                )
                .await?;
            if !active
                .orders
                .iter()
                .any(|o| o.market_index == market && o.order_index == index)
            {
                return Ok(());
            }
        }
        Err(err("cancel not yet reflected by active orders"))
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        // No market filter: reconciliation must see foreign/unknown orders too.
        let orders: Orders = self
            .get(
                "accountActiveOrders",
                &[("account_index", self.account.to_string())],
                true,
            )
            .await?;
        let markets = self.markets().await?;
        let journal = self.journal.lock().await;
        let mut states = Vec::with_capacity(orders.orders.len());
        for row in orders.orders {
            states.push(
                self.convert_order(&row, journal.orders.get(&row.client_order_index), &markets)
                    .await?,
            );
        }
        Ok(states)
    }

    /// `account` 里这个市场持仓的 `liquidation_price` 与 `allocated_margin`。"0" 或缺省表示交易所
    /// 没给（全仓没有分配保证金），按没有处理，不当成 0。
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<VenueLegState>> {
        let account = self.account().await?;
        let Some(position) = account.positions.into_iter().find(|position| {
            Symbol::perp(&position.symbol, "USDT") == *symbol
                && decimal(&position.position).is_ok_and(|size| !size.is_zero())
        }) else {
            return Ok(None);
        };
        Ok(Some(leg_state_from(&position)))
    }

    /// `account` 里的 `available_balance`；响应里没有就返回 `None`（不知道，不当成 0）。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        self.account()
            .await?
            .available_balance
            .as_deref()
            .map(decimal)
            .transpose()
    }

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// 往已有的逐仓持仓补保证金，见 [`LighterBroker::add_margin_with`]。
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.add_margin_with(symbol, amount_usdt, MARGIN_POLLS, MARGIN_POLL_INTERVAL)
            .await
    }

    /// `trades`（要鉴权）：这个账户在这个合约上的全部成交，最新在前，按游标翻页，翻到 `since`
    /// 之前为止。我们这一侧由买卖双方的账户号判断；手续费用每笔成交自带的费率档（含账户
    /// 档位与折扣），缺省时只有账户吃单费率为 0 才按 0（见 `trade_fee_ticks`）。
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> ArbResult<Option<Vec<crate::settlement::VenueFill>>> {
        use crate::settlement::{FillEffect, VenueFill};
        let markets = self.markets().await?;
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == *symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market on this deployment"))?;
        let mut fills = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cursors = HashSet::new();
        let mut ids = HashSet::new();
        for _ in 0..TRADES_PAGES {
            let mut query = vec![
                ("account_index", self.account.to_string()),
                ("market_id", market.market_id.to_string()),
                ("sort_by", "trade_id".into()),
                ("sort_dir", "desc".into()),
                ("limit", "100".into()),
                ("aggregate", "false".into()),
            ];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            }
            let page: Trades = self.get("trades", &query, true).await?;
            let mut reached_start = page.trades.is_empty();
            for trade in page.trades {
                if !ids.insert(trade.trade_id) {
                    return Err(err(
                        "duplicate trade across pages; the fill history is not reliable",
                    ));
                }
                if trade.market_id != market.market_id {
                    continue;
                }
                let at = trade
                    .timestamp
                    .and_then(DateTime::from_timestamp_millis)
                    .ok_or_else(|| err("trade has no valid timestamp"))?;
                if at < since {
                    reached_start = true;
                    continue;
                }
                if at > until {
                    continue;
                }
                let ours_ask = trade.ask_account_id == self.account;
                let ours_bid = trade.bid_account_id == self.account;
                if ours_ask == ours_bid {
                    return Err(err(
                        "trade does not belong to exactly one side of this account",
                    ));
                }
                let maker = ours_ask == trade.is_maker_ask;
                let (tick, integrator) = trade_fee_ticks(&trade, maker, self.fee)?;
                let notional = decimal(&trade.usd_amount)?;
                let quantity = decimal(&trade.size)?;
                if notional <= Decimal::ZERO || quantity <= Decimal::ZERO {
                    return Err(err("invalid trade amounts"));
                }
                let rate =
                    (Decimal::from(tick) + Decimal::from(integrator)) / Decimal::from(1_000_000);
                fills.push(VenueFill {
                    at,
                    side: if ours_ask { Side::Sell } else { Side::Buy },
                    quantity,
                    price: notional / quantity,
                    fee_usdt: notional * rate,
                    // 这个接口不告诉我们是开仓还是平仓：只能靠方向与数量核对。
                    effect: FillEffect::Unknown,
                });
            }
            match page.next_cursor.filter(|c| !c.is_empty()) {
                Some(next) if !reached_start => {
                    if !cursors.insert(next.clone()) {
                        return Err(err("repeated trades cursor"));
                    }
                    cursor = Some(next);
                }
                _ => return Ok(Some(fills)),
            }
        }
        Err(err(
            "trade history exceeds the page limit; the pnl would be incomplete",
        ))
    }

    /// `positionFunding`（主账户要鉴权）：最新在前，按游标翻页，翻到开仓之前为止。
    /// `change` 正 = 收到。时间戳按量级判断秒 / 毫秒。
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: DateTime<Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let markets = self.markets().await?;
        let market = markets
            .iter()
            .find(|m| Symbol::perp(&m.symbol, "USDT") == *symbol && m.market_type == "perp")
            .ok_or_else(|| err("no matching perpetual market on this deployment"))?;
        let mut rows = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..FUNDING_PAGES {
            let mut query = vec![
                ("account_index", self.account.to_string()),
                ("market_id", market.market_id.to_string()),
                ("limit", FUNDING_PAGE.to_string()),
                ("side", "all".to_string()),
            ];
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.clone()));
            }
            let page: FundingPage = self.get("positionFunding", &query, true).await?;
            let mut reached_start = page.position_fundings.is_empty();
            for row in page.position_fundings {
                if row.market_id != market.market_id {
                    continue;
                }
                let at = funding_time(row.timestamp)?;
                reached_start |= at < since;
                rows.push((at, decimal(&row.change)?));
            }
            match page.next_cursor.filter(|next| !next.is_empty()) {
                Some(next) if !reached_start => cursor = Some(next),
                _ => return Ok(Some(FundingTotal::from_rows(rows, since))),
            }
        }
        Err(err(
            "positionFunding exceeds the page limit; total would be incomplete",
        ))
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        self.account()
            .await?
            .positions
            .into_iter()
            .filter_map(|p| {
                let quantity = match decimal(&p.position) {
                    Ok(q) => q,
                    Err(e) => return Some(Err(e)),
                };
                if quantity.is_zero() {
                    return None;
                }
                Some((|| {
                    if quantity < Decimal::ZERO || ![-1, 1].contains(&p.sign) {
                        return Err(err("invalid position sign/size"));
                    }
                    let average = decimal(&p.avg_entry_price)?;
                    if average <= Decimal::ZERO {
                        return Err(err("missing position entry price"));
                    }
                    Ok(VenuePosition {
                        venue: self.deployment.venue(),
                        symbol: Symbol::perp(p.symbol, "USDT"),
                        net_quantity: quantity * Decimal::from(p.sign),
                        average_price: Some(average),
                        notional_usdt: decimal(&p.position_value)?.abs(),
                    })
                })())
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LighterMarginSetting {
    pub market_id: i32,
    pub symbol: Symbol,
    pub isolated: bool,
    pub leverage: Decimal,
}

fn err(message: impl Into<String>) -> ArbError {
    ArbError::venue("lighter", message)
}
fn json_error(_: serde_json::Error) -> ArbError {
    err("invalid Lighter JSON/schema")
}
fn decimal(value: &str) -> ArbResult<Decimal> {
    Decimal::from_str_exact(value).map_err(|_| err("invalid decimal in Lighter response"))
}
fn checked_mul(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b)
        .ok_or_else(|| err("decimal product overflow"))
}
fn checked_add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b).ok_or_else(|| err("decimal sum overflow"))
}
fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| err("order has no venue id"))?,
        status: state.status,
    })
}

async fn response<T: DeserializeOwned>(request: RequestBuilder, endpoint: &str) -> ArbResult<T> {
    fetch(request, endpoint)
        .await
        .map_err(|failure| match failure {
            Failure::Transient(error) | Failure::Other(error) => error,
        })
}

enum Failure {
    /// 限频（HTTP 429 / API code 23000）或网关临时故障（5xx）：请求没有得到处理结果。
    /// 只读查询据此退避重试；写请求不重试。
    Transient(ArbError),
    Other(ArbError),
}

async fn fetch<T: DeserializeOwned>(request: RequestBuilder, endpoint: &str) -> Result<T, Failure> {
    let response = request.send().await.map_err(|e| {
        Failure::Other(err(format!(
            "{endpoint} transport failure: {}",
            if e.is_timeout() { "timeout" } else { "network" }
        )))
    })?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|_| Failure::Other(err(format!("{endpoint} response body unavailable"))))?;
    let classify = |error: ArbError, code: Option<i64>| {
        if is_transient(status, code) {
            Failure::Transient(error)
        } else {
            Failure::Other(error)
        }
    };
    // 不是 JSON：多半是网关错误页（502/503）或限频文本。如实报 HTTP 状态，
    // 不要说成「字段不符」—— 那会把人引向去查数据结构。
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return Err(classify(
            err(format!(
                "{endpoint}: HTTP {status}，响应不是 JSON（网关错误页或临时故障）"
            )),
            None,
        ));
    };
    let code = value.get("code").and_then(serde_json::Value::as_i64);
    if !status.is_success() || code != Some(200) {
        // Do not echo response bodies: gateways can reflect auth headers/signed payloads.
        let shown = code.map_or_else(|| "missing".to_string(), |code| code.to_string());
        return Err(classify(
            err(format!("{endpoint}: HTTP {status}, API code {shown}")),
            code,
        ));
    }
    serde_json::from_value(value).map_err(|e| Failure::Other(schema_error(endpoint, &e)))
}

/// 限频或网关临时故障。
/// 这笔成交里我们这一侧的费率（百万分之一）与集成商附加费。
///
/// 我们这一侧的费率字段缺省时，只有账户本身的吃单费率就是 0（`accountLimits` 读到的
/// `current_taker_fee_tick`）才按 0 记 —— 那正是 Lighter 省略 0 值的情形。付费账户缺了
/// 这个字段就拒绝下结论，绝不拿市场的 0% 顶上。集成商附加费缺省按 0：本程序不用集成商代码。
fn trade_fee_ticks(
    trade: &Trade,
    maker: bool,
    account_taker_fee: Decimal,
) -> ArbResult<(i64, i64)> {
    let tick = match (maker, trade.maker_fee, trade.taker_fee) {
        (true, Some(fee), _) | (false, _, Some(fee)) => fee,
        // 挂单（maker）成交同理：0 费率的账户，maker_fee 也是被省略的（2026-09-30 RH 上手动
        // 挂单平仓的成交就是这样）。账户吃单费率为 0 说明是不收费的标准账户；付费账户缺了
        // 字段照旧拒绝下结论。
        (true, None, _) | (false, _, None) if account_taker_fee.is_zero() => 0,
        _ => return Err(err("authenticated trade omitted the actual fee")),
    };
    let integrator = if maker {
        trade.integrator_maker_fee
    } else {
        trade.integrator_taker_fee
    }
    .unwrap_or(0);
    Ok((tick, integrator))
}

fn is_transient(status: reqwest::StatusCode, code: Option<i64>) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || code == Some(23000)
        || matches!(status.as_u16(), 500 | 502 | 503 | 504)
}

/// 字段与预期不符：说出是哪个字段、什么类型，但不回显任何值（双引号里的内容一律隐去）。
fn schema_error(endpoint: &str, error: &serde_json::Error) -> ArbError {
    let mut detail = String::new();
    let mut quoted = false;
    for ch in error.to_string().chars() {
        match ch {
            '"' if !quoted => {
                quoted = true;
                detail.push_str("\"…");
            }
            '"' => {
                quoted = false;
                detail.push('"');
            }
            _ if quoted => {}
            _ => detail.push(ch),
        }
    }
    err(format!("{endpoint}: 响应字段与预期不符（{detail}）"))
}

fn client_index(deployment: LighterDeployment, account: i64, id: &ClientOrderId) -> i64 {
    let mut hash = Sha256::new();
    // Preserve the original mainnet salt byte-for-byte, without allocating a String.
    let mut chain = deployment.chain_id();
    let mut digits = [0_u8; 10];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (chain % 10) as u8;
        chain /= 10;
        if chain == 0 {
            break;
        }
    }
    hash.update(b"arb-bot:lighter:");
    hash.update(&digits[start..]);
    hash.update(b":client-index:v1\0");
    hash.update(account.to_be_bytes());
    hash.update(id.0.as_bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 8];
    bytes[2..].copy_from_slice(&digest[..6]);
    i64::from_be_bytes(bytes).max(1)
}

struct Prepared {
    base: i64,
    price: u32,
    quantity: Decimal,
}
fn prepare(order: &NewOrder, market: &Market, price: Decimal) -> ArbResult<Prepared> {
    if market.status != "active"
        || market.market_type != "perp"
        || market.is_frozen == Some(true)
        || market.multiplier != "1.000000000000000000"
            && decimal(&market.multiplier)? != Decimal::ONE
        || market.quote_multiplier != 1
        || market.market_id == 255
        || !(0..=32767).contains(&market.market_id)
    {
        return Err(err(
            "market is inactive, frozen, or has unsupported contract units",
        ));
    }
    if price <= Decimal::ZERO {
        return Err(err("price bound must be positive"));
    }
    if order.notional_usdt <= Decimal::ZERO || order.client_order_id.0.is_empty() {
        return Err(err("non-positive notional or empty client order id"));
    }
    let requested = match order.quantity {
        Some(quantity) => quantity,
        None if !order.reduce_only => order
            .notional_usdt
            .checked_div(price)
            .ok_or_else(|| err("quantity conversion overflow"))?,
        None => return Err(err("reduce-only requires exact base quantity")),
    };
    let base = scaled_quantity(requested, market.supported_size_decimals, order.reduce_only)?;
    let quantity = Decimal::from(base) / scale(market.supported_size_decimals)?;
    let ticks = checked_mul(price, scale(market.supported_price_decimals)?)?;
    // Bound rounding preserves the limit: buy floor, sell ceil.
    let ticks = if order.side == Side::Buy {
        ticks.floor()
    } else {
        ticks.ceil()
    };
    let price = ticks
        .to_u32()
        .filter(|p| *p > 0)
        .ok_or_else(|| err("price outside uint32 range"))?;
    // Encoding decimals can differ from supported tick decimals. Use both explicitly.
    let wire_base = scaled_quantity(quantity, market.size_decimals, true)?;
    let bounded_price = Decimal::from(price) / scale(market.supported_price_decimals)?;
    let wire_price = checked_mul(bounded_price, scale(market.price_decimals)?)?;
    if !wire_price.fract().is_zero() {
        return Err(err("price cannot be encoded at wire precision"));
    }
    let wire_price = wire_price
        .to_u32()
        .filter(|p| *p > 0)
        .ok_or_else(|| err("wire price overflow"))?;
    if quantity < decimal(&market.min_base_amount)?
        || checked_mul(quantity, bounded_price)? < decimal(&market.min_quote_amount)?
    {
        return Err(err("order rounds to dust or is below venue minimum"));
    }
    Ok(Prepared {
        base: wire_base,
        price: wire_price,
        quantity,
    })
}
fn scale(decimals: u32) -> ArbResult<Decimal> {
    if decimals > 18 {
        return Err(err("unsupported market precision"));
    }
    Ok(Decimal::from(10_u64.pow(decimals)))
}
fn scaled_quantity(quantity: Decimal, decimals: u32, exact: bool) -> ArbResult<i64> {
    if quantity <= Decimal::ZERO {
        return Err(err("quantity must be positive"));
    }
    let value = checked_mul(quantity, scale(decimals)?)?;
    if exact && !value.fract().is_zero() {
        return Err(err(
            "reduce-only quantity is not exactly representable; residual exposure would remain",
        ));
    }
    value
        .floor()
        .to_i64()
        .filter(|v| (1..=MAX_INDEX).contains(v))
        .ok_or_else(|| err("quantity is dust or exceeds 48-bit limit"))
}
/// 账户里这个市场是否为所选模式、保证金率正好是 `imf`（万分之一为单位）。
fn leverage_matches(account: &Account, market_id: i32, imf: i32, mode: crate::MarginMode) -> bool {
    account.positions.iter().any(|p| {
        p.market_id == market_id
            && p.margin_mode == if mode.is_cross() { 0 } else { 1 }
            && decimal(&p.initial_margin_fraction).ok()
                == Some(Decimal::from(imf) / Decimal::from(100))
    })
}

fn margin_fraction(leverage: Decimal, minimum: i32) -> ArbResult<i32> {
    if leverage < Decimal::ONE || minimum <= 0 {
        return Err(err("invalid leverage or market margin minimum"));
    }
    let fraction = (Decimal::from(10_000) / leverage)
        .ceil()
        .to_i32()
        .ok_or_else(|| err("margin fraction overflow"))?;
    if fraction < minimum || fraction > 10_000 {
        return Err(err("leverage exceeds market limit"));
    }
    Ok(fraction)
}
fn order_status(status: &str, requested: Decimal, filled: Decimal) -> ArbResult<OrderStatus> {
    match status {
        "in-progress" | "pending" => Ok(OrderStatus::Pending),
        "open" => Ok(OrderStatus::Open),
        "filled" if filled == requested => Ok(OrderStatus::Filled),
        "filled" => Ok(OrderStatus::Cancelled),
        "canceled" => Ok(OrderStatus::Cancelled),
        s if s.starts_with("canceled-") => Ok(OrderStatus::Cancelled),
        _ => Err(err("unknown exchange order status")),
    }
}

#[derive(Deserialize)]
struct Markets {
    order_book_details: Vec<Market>,
}
#[derive(Deserialize)]
struct Market {
    market_id: i32,
    symbol: String,
    market_type: String,
    status: String,
    supported_size_decimals: u32,
    supported_price_decimals: u32,
    size_decimals: u32,
    price_decimals: u32,
    min_base_amount: String,
    min_quote_amount: String,
    multiplier: String,
    quote_multiplier: i64,
    /// 主网每个市场都给（实测 235/235，全为 false）；Robinhood Chain 部署**没有这个字段**
    /// （实测 57/57 缺失）。缺失时不当成冻结，交易资格仍由 `status == "active"` 把关。
    #[serde(default)]
    is_frozen: Option<bool>,
    min_initial_margin_fraction: i32,
}
#[derive(Deserialize)]
struct Orders {
    orders: Vec<RemoteOrder>,
}
#[derive(Deserialize)]
struct RemoteOrder {
    order_index: i64,
    client_order_index: i64,
    market_index: i32,
    owner_account_index: i64,
    initial_base_amount: String,
    filled_base_amount: String,
    filled_quote_amount: String,
    price: String,
    is_ask: bool,
    reduce_only: bool,
    status: String,
}
#[derive(Deserialize)]
struct Book {
    bids: Vec<BookOrder>,
    asks: Vec<BookOrder>,
}
#[derive(Deserialize)]
struct BookOrder {
    price: String,
    remaining_base_amount: String,
}
#[derive(Deserialize)]
struct Accounts {
    total: i64,
    accounts: Vec<Account>,
}
#[derive(Deserialize)]
struct Account {
    index: i64,
    positions: Vec<AccountPosition>,
    /// 可用于开新仓的余额。字段名以官方 `account` 响应为准；缺了就当不知道。
    #[serde(default)]
    available_balance: Option<String>,
}
#[derive(Deserialize)]
struct AccountPosition {
    market_id: i32,
    symbol: String,
    sign: i32,
    position: String,
    avg_entry_price: String,
    position_value: String,
    initial_margin_fraction: String,
    margin_mode: i32,
    /// 交易所报告的强平价（没有时缺省或 "0"）。
    #[serde(default)]
    liquidation_price: Option<String>,
    /// 这个仓位分配的保证金（逐仓才有；全仓是 "0"）。
    #[serde(default)]
    allocated_margin: Option<String>,
}
fn leg_state_from(position: &AccountPosition) -> VenueLegState {
    let positive = |raw: &Option<String>| {
        raw.as_deref()
            .and_then(|raw| decimal(raw).ok())
            .filter(|value| *value > Decimal::ZERO)
    };
    VenueLegState {
        margin_mode: match position.margin_mode {
            0 => Some(crate::MarginMode::Cross),
            1 => Some(crate::MarginMode::Isolated),
            _ => None,
        },
        margin_usdt: if position.margin_mode == 1 {
            positive(&position.allocated_margin)
        } else {
            None
        },
        liquidation_price: positive(&position.liquidation_price),
    }
}

/// `add_margin` 要补的持仓：市场、方向与入场规格，以及补之前的分配保证金。
#[derive(Debug, Clone, PartialEq, Eq)]
struct MarginTarget {
    market_id: i32,
    size: Decimal,
    sign: i32,
    entry_price: Decimal,
    before: Decimal,
}

/// 账户里这个符号的持仓必须恰好一个、非零、逐仓（`margin_mode == 1`）、报告了正的分配保证金；
/// 否则 `Err`（还没发出任何写请求）。全仓持仓没有「分配保证金」可补，也绝不替用户改保证金模式。
fn margin_target(account: &Account, symbol: &Symbol) -> ArbResult<MarginTarget> {
    let mut open = account.positions.iter().filter(|p| {
        Symbol::perp(&p.symbol, "USDT") == *symbol
            && decimal(&p.position).is_ok_and(|size| !size.is_zero())
    });
    let position = open
        .next()
        .ok_or_else(|| err("no open position on this market; nothing to add margin to"))?;
    if open.next().is_some() {
        return Err(err(
            "several open positions match this symbol; refusing to guess",
        ));
    }
    if position.margin_mode != 1 {
        return Err(err(
            "position is not isolated margin; add_margin never changes the margin mode",
        ));
    }
    // Same range the signer validates (and 255 is the nil market index).
    if !(0..=32767).contains(&position.market_id) || position.market_id == 255 {
        return Err(err("position market index is outside the signer's range"));
    }
    let size = decimal(&position.position)?;
    let entry_price = decimal(&position.avg_entry_price)?;
    if size <= Decimal::ZERO || !matches!(position.sign, -1 | 1) || entry_price <= Decimal::ZERO {
        return Err(err(
            "isolated position has invalid size, direction or entry price",
        ));
    }
    let before = leg_state_from(position).margin_usdt.ok_or_else(|| {
        err("isolated position reports no allocated margin; a top-up could not be verified")
    })?;
    Ok(MarginTarget {
        market_id: position.market_id,
        size,
        sign: position.sign,
        entry_price,
        before,
    })
}

/// 只比较同一条逐仓腿；成交/强平造成的数量、方向或入场价变化不能被当成补进去的保证金。
fn margin_of(account: &Account, target: &MarginTarget) -> Option<Decimal> {
    let mut rows = account
        .positions
        .iter()
        .filter(|p| p.market_id == target.market_id);
    let position = rows.next()?;
    if rows.next().is_some()
        || position.margin_mode != 1
        || position.sign != target.sign
        || decimal(&position.position).ok()? != target.size
        || decimal(&position.avg_entry_price).ok()? != target.entry_price
    {
        return None;
    }
    leg_state_from(position).margin_usdt
}

/// 回读到的保证金是否已涨到「补之前 + 0.99 × 金额」。减少或不变一律不算。
fn margin_applied(before: Decimal, now: Decimal, amount: Decimal) -> bool {
    amount
        .checked_mul(MARGIN_CONFIRM_RATIO)
        .and_then(|part| before.checked_add(part))
        .is_some_and(|threshold| now >= threshold)
}

/// 金额 → 线上整数（1e-6 个抵押币）。调用方已按分（2 位）取整；这里只接受能精确表示的值，
/// 绝不替调用方四舍五入（更不会放大）。
fn margin_wire_amount(amount: Decimal) -> ArbResult<i64> {
    if amount <= Decimal::ZERO {
        return Err(err("margin amount must be positive"));
    }
    let scaled = checked_mul(amount, Decimal::from(MARGIN_SCALE))?;
    if !scaled.fract().is_zero() {
        return Err(err(
            "margin amount has more than 6 decimals; refusing to round it",
        ));
    }
    scaled
        .to_i64()
        .filter(|wire| (1..=MARGIN_MAX_WIRE).contains(wire))
        .ok_or_else(|| err("margin amount exceeds the venue limit"))
}

/// 补保证金那一次 `sendTx` 的结果分类。
#[derive(Debug, PartialEq, Eq)]
enum MarginSend {
    /// API 收下了（`code: 200` 且 `tx_hash` 与签名的一致）；序列器之后仍可能拒绝。
    Accepted,
    /// API 服务器明确拒绝（限频 / 业务码），交易没有被排队，没动钱。
    Refused(String),
    /// 可能已经被处理：5xx、不是 JSON 的响应、`tx_hash` 对不上、未知形状。
    Unknown(String),
}

/// 分类 `sendTx` 的 HTTP 响应。规则（保守：拿不准一律 `Unknown`，错判 `Refused` 会让调用方重发）：
/// - 429 → `Refused`（限频发生在处理之前）；
/// - 5xx → `Unknown`；
/// - 不是 JSON → `Unknown`；
/// - 2xx 且 `code == 200` 且 `tx_hash` 与签名的一致 → `Accepted`，hash 缺失/不同 → `Unknown`；
/// - 2xx/4xx 且带非 200 的 API 码 → `Refused`；29500..=29999（内部错误 / 处理超时，
///   <https://apidocs.lighter.xyz/docs/data-structures-constants-and-errors>）除外 → `Unknown`；
/// - 其它（缺 `code`、3xx、4xx 却 `code == 200`）→ `Unknown`。
fn classify_margin_send(
    status: reqwest::StatusCode,
    body: &[u8],
    expected_hash: &str,
) -> MarginSend {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return MarginSend::Refused(
            "sendTx rate limited (HTTP 429) before processing; no margin moved".into(),
        );
    }
    if status.is_server_error() {
        return MarginSend::Unknown(format!(
            "sendTx HTTP {status}: server error after the request left; outcome unknown"
        ));
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return MarginSend::Unknown(format!(
            "sendTx HTTP {status}: response is not JSON; outcome unknown"
        ));
    };
    let code = value.get("code").and_then(serde_json::Value::as_i64);
    match code {
        Some(200) if status.is_success() => {
            match value.get("tx_hash").and_then(serde_json::Value::as_str) {
                Some(hash) if hash == expected_hash => MarginSend::Accepted,
                _ => MarginSend::Unknown(
                    "sendTx accepted but tx_hash is missing or differs from the signed hash".into(),
                ),
            }
        }
        Some(code)
            if code != 200
                && !(29_500..=29_999).contains(&code)
                && (status.is_success() || status.is_client_error()) =>
        {
            MarginSend::Refused(match margin_refusal_label(code) {
                Some(label) => format!(
                    "sendTx rejected by the API server: HTTP {status}, API code {code} ({label}); no margin moved"
                ),
                None => format!(
                    "sendTx rejected by the API server: HTTP {status}, API code {code}; no margin moved"
                ),
            })
        }
        _ => MarginSend::Unknown(format!(
            "sendTx HTTP {status}: unexpected response (API code {}); outcome unknown",
            code.map_or_else(|| "missing".to_string(), |code| code.to_string())
        )),
    }
}

/// 官方错误码表里与补保证金相关的几条（名称原样取自
/// <https://apidocs.lighter.xyz/docs/data-structures-constants-and-errors>）。表里没有的只报数字。
fn margin_refusal_label(code: i64) -> Option<&'static str> {
    Some(match code {
        21104 => "invalid nonce",
        21108 => "invalid PublicKey, please run changePubKey",
        21109 => "api key not found",
        21110 => "invalid api key index",
        21111 => {
            "account is in pre-liquidation and the transaction doesn't increase the account health"
        }
        21112 => "account is in liquidation",
        21118 => "transfer amount is too small",
        21119 => "transfer amount is too high",
        21120 => "invalid signature",
        21133 => "invalid risk change",
        21301 => "not enough collateral",
        21304 => "not enough asset balance",
        21501 => "invalid tx info",
        21506 => "too many pending txs",
        21507 => "account is below maintenance margin, can't execute transaction",
        21508 => "account is below initial margin, can't execute transaction",
        21602 => "invalid market index",
        21614 => "no position found",
        21615 => "invalid update margin direction",
        23000 => "Too Many Requests!",
        _ => return None,
    })
}

#[derive(Deserialize)]
struct AccountFees {
    current_taker_fee_tick: i64,
}
#[derive(Deserialize)]
struct NextNonce {
    nonce: i64,
}
#[derive(Deserialize)]
struct SendAck {
    tx_hash: String,
}
/// `trades` 最多翻几页（每页 100 条）。
const TRADES_PAGES: usize = 30;

/// `positionFunding` 一页多少条、最多翻几页。
const FUNDING_PAGE: usize = 100;
const FUNDING_PAGES: usize = 30;

#[derive(Deserialize)]
struct FundingPage {
    #[serde(default)]
    position_fundings: Vec<PositionFunding>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct PositionFunding {
    timestamp: i64,
    market_id: i32,
    /// 这次结算的资金费金额（正 = 收到）。
    change: String,
}

/// Lighter 的时间戳有的接口是秒、有的是毫秒：按量级判断。
fn funding_time(raw: i64) -> ArbResult<DateTime<Utc>> {
    let at = if raw > 100_000_000_000 {
        DateTime::from_timestamp_millis(raw)
    } else {
        DateTime::from_timestamp(raw, 0)
    };
    at.ok_or_else(|| err("positionFunding has an invalid timestamp"))
}

#[derive(Deserialize)]
struct Trades {
    trades: Vec<Trade>,
    next_cursor: Option<String>,
}
#[derive(Deserialize)]
struct Trade {
    trade_id: i64,
    market_id: i32,
    ask_id: i64,
    bid_id: i64,
    ask_account_id: i64,
    bid_account_id: i64,
    is_maker_ask: bool,
    /// 成交时刻（毫秒）。
    #[serde(default)]
    timestamp: Option<i64>,
    size: String,
    usd_amount: String,
    /// Lighter 对 0 值省略这两个字段：公开成交里标准账户（0 费率）的 `taker_fee` 几乎全部缺省，
    /// 只有付费账户才出现（实测 RH 94/100、主网 97/100 缺省）。
    maker_fee: Option<i64>,
    taker_fee: Option<i64>,
    /// 集成商附加费。本程序下单从不带集成商代码；公开成交在两个部署上都不给这两个字段。
    #[serde(default)]
    integrator_maker_fee: Option<i64>,
    #[serde(default)]
    integrator_taker_fee: Option<i64>,
}

// An append-only intent journal, distinct from the executor's pair ledger.
#[derive(Clone, Serialize, Deserialize)]
struct Intent {
    order: NewOrder,
    market: i32,
    quantity: Decimal,
    terminal: Option<OrderState>,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JournalRecord {
    Identity { chain: u32, account: i64, key: u8 },
    Intent { index: i64, intent: Intent },
    Nonce { nonce: i64 },
    Terminal { index: i64, state: OrderState },
    NonceReleased { nonce: i64, previous: Option<i64> },
}
struct Journal {
    file: File,
    orders: HashMap<i64, Intent>,
    last_nonce: Option<i64>,
}
impl Journal {
    fn open(path: &Path, deployment: LighterDeployment, account: i64, key: u8) -> ArbResult<Self> {
        let mut options = OpenOptions::new();
        options.read(true).append(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        fs2::FileExt::try_lock_exclusive(&file)
            .map_err(|_| err("Lighter journal is locked by another process"))?;
        let mut journal = Self {
            file,
            orders: HashMap::new(),
            last_nonce: None,
        };
        let mut identity = false;
        for line in BufReader::new(journal.file.try_clone()?).split(b'\n') {
            let line = line?;
            if line.is_empty() {
                return Err(err("empty/corrupt line in order journal"));
            }
            let record: JournalRecord = serde_json::from_slice(&line).map_err(json_error)?;
            match record {
                JournalRecord::Identity {
                    chain,
                    account: stored,
                    key: stored_key,
                } if !identity
                    && journal.orders.is_empty()
                    && chain == deployment.chain_id()
                    && stored == account
                    && stored_key == key =>
                {
                    identity = true
                }
                JournalRecord::Intent { index, intent } if identity => {
                    if client_index(deployment, account, &intent.order.client_order_id) != index
                        || intent.order.venue != deployment.venue()
                        || journal.orders.contains_key(&index)
                    {
                        return Err(err("duplicate or corrupt journal client index"));
                    }
                    journal.orders.insert(index, intent);
                }
                JournalRecord::Nonce { nonce } if identity => {
                    if journal.last_nonce.is_some_and(|prior| prior >= nonce) {
                        return Err(err("non-monotonic journal nonce"));
                    }
                    journal.last_nonce = Some(nonce);
                }
                JournalRecord::Terminal { index, state } if identity => {
                    let intent = journal
                        .orders
                        .get_mut(&index)
                        .ok_or_else(|| err("terminal journal state without intent"))?;
                    if state.status.is_live()
                        || state.order.client_order_id != intent.order.client_order_id
                    {
                        return Err(err("invalid terminal journal state"));
                    }
                    intent.terminal = Some(state);
                }
                // A margin reservation released after an API-server rejection (the nonce was
                // never consumed): restore the pre-reservation value so the next reservation of
                // the same nonce stays monotonic.
                JournalRecord::NonceReleased { nonce, previous } if identity => {
                    if journal.last_nonce != Some(nonce) || previous.is_some_and(|p| p >= nonce) {
                        return Err(err("invalid nonce release in order journal"));
                    }
                    journal.last_nonce = previous;
                }
                _ => {
                    return Err(err(
                        "journal identity/account mismatch or malformed record sequence",
                    ));
                }
            }
        }
        if !identity {
            journal.append(&JournalRecord::Identity {
                chain: deployment.chain_id(),
                account,
                key,
            })?;
        }
        Ok(journal)
    }
    fn append(&mut self, record: &JournalRecord) -> ArbResult<()> {
        let mut data = serde_json::to_vec(record).map_err(json_error)?;
        data.push(b'\n');
        self.file.write_all(&data)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// 撤销最近一次 `reserve_nonce`：仅在确定 nonce 没有被服务器消耗时使用（API 服务器拒绝、
    /// 或根本没发出过请求）。先落盘再改内存；对不上最近一次预留就拒绝。
    fn release_nonce(&mut self, nonce: i64, previous: Option<i64>) -> ArbResult<()> {
        if self.last_nonce != Some(nonce) || previous.is_some_and(|p| p >= nonce) {
            return Err(err("nonce release does not match the latest reservation"));
        }
        self.append(&JournalRecord::NonceReleased { nonce, previous })?;
        self.last_nonce = previous;
        Ok(())
    }
}

// Official C ABI from v1.0.10 header. repr(C) preserves pointer alignment.
#[repr(C)]
struct RawSigned {
    kind: u8,
    info: *mut c_char,
    hash: *mut c_char,
    message: *mut c_char,
    error: *mut c_char,
}
#[repr(C)]
struct RawAuth {
    token: *mut c_char,
    error: *mut c_char,
}
type Free = unsafe extern "C" fn(*mut c_void);
type Create = unsafe extern "C" fn(*mut c_char, *mut c_char, i32, i32, i64) -> *mut c_char;
type Auth = unsafe extern "C" fn(i64, i32, i64) -> RawAuth;
type SignOrder = unsafe extern "C" fn(
    i32,
    i64,
    i64,
    i32,
    i32,
    i32,
    i32,
    i32,
    i32,
    i64,
    i64,
    i32,
    i32,
    u8,
    u8,
    u8,
    i64,
    i32,
    i64,
) -> RawSigned;
type Cancel = unsafe extern "C" fn(i32, i64, u8, i64, i32, i64) -> RawSigned;
type Leverage = unsafe extern "C" fn(i32, i32, i32, u8, i64, i32, i64) -> RawSigned;
type Margin = unsafe extern "C" fn(i32, i64, i32, u8, i64, i32, i64) -> RawSigned;
struct SignedTx {
    kind: u8,
    info: String,
    hash: String,
}
struct NativeSigner {
    _library: Library,
    free: Free,
    create: Create,
    auth: Auth,
    order: SignOrder,
    cancel: Cancel,
    leverage: Leverage,
    margin: Margin,
}
static SIGNER: StdMutex<Option<Arc<NativeSigner>>> = StdMutex::new(None);
impl NativeSigner {
    fn load(path: &Path) -> ArbResult<Arc<Self>> {
        if !cfg!(all(target_os = "linux", target_arch = "x86_64")) || !path.is_absolute() {
            return Err(err(
                "pinned Lighter signer requires Linux x86_64 and an absolute library path",
            ));
        }
        let mut file = File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        if format!("{:x}", hasher.finalize()) != SIGNER_SHA256 {
            return Err(err(
                "signer SHA-256 differs from pinned official v1.0.10 release",
            ));
        }
        let mut global = SIGNER
            .lock()
            .map_err(|_| err("native signer lock poisoned"))?;
        if let Some(signer) = global.as_ref() {
            return Ok(Arc::clone(signer));
        }
        // Keep the opened inode through dlopen to avoid a path-replacement race.
        #[cfg(target_os = "linux")]
        let load_path = {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
        };
        #[cfg(not(target_os = "linux"))]
        let load_path = path.to_path_buf();
        // SAFETY: SHA-pinned official binary; functions below match its generated C header.
        let library = unsafe { Library::new(load_path) }
            .map_err(|_| err("cannot load pinned signer or its system dependencies"))?;
        unsafe {
            let signer = Arc::new(Self {
                free: *library
                    .get::<Free>(b"Free\0")
                    .map_err(|_| err("signer lacks Free"))?,
                create: *library
                    .get::<Create>(b"CreateClient\0")
                    .map_err(|_| err("signer lacks CreateClient"))?,
                auth: *library
                    .get::<Auth>(b"CreateAuthToken\0")
                    .map_err(|_| err("signer lacks CreateAuthToken"))?,
                order: *library
                    .get::<SignOrder>(b"SignCreateOrder\0")
                    .map_err(|_| err("signer lacks SignCreateOrder"))?,
                cancel: *library
                    .get::<Cancel>(b"SignCancelOrder\0")
                    .map_err(|_| err("signer lacks SignCancelOrder"))?,
                leverage: *library
                    .get::<Leverage>(b"SignUpdateLeverage\0")
                    .map_err(|_| err("signer lacks SignUpdateLeverage"))?,
                margin: *library
                    .get::<Margin>(b"SignUpdateMargin\0")
                    .map_err(|_| err("signer lacks SignUpdateMargin"))?,
                _library: library,
            });
            // The Go runtime remains loaded for process lifetime; never dlclose it.
            *global = Some(Arc::clone(&signer));
            Ok(signer)
        }
    }
    fn string(&self, ptr: *mut c_char) -> ArbResult<Option<String>> {
        if ptr.is_null() {
            return Ok(None);
        }
        // SAFETY: ABI returns a NUL-terminated C allocation, released with its own Free.
        let result = unsafe { CStr::from_ptr(ptr) }.to_str().map(str::to_owned);
        unsafe { (self.free)(ptr.cast()) };
        result
            .map(Some)
            .map_err(|_| err("native signer returned invalid UTF-8"))
    }
    fn signed(&self, raw: RawSigned, expected: u8) -> ArbResult<SignedTx> {
        // Free every allocation even if one field is malformed.
        let info = self.string(raw.info);
        let hash = self.string(raw.hash);
        let message = self.string(raw.message);
        let error = self.string(raw.error);
        if error?.is_some() || message?.is_some() || raw.kind != expected {
            return Err(err(
                "official signer rejected transaction; no unsigned fallback",
            ));
        }
        Ok(SignedTx {
            kind: raw.kind,
            info: info?.ok_or_else(|| err("signer omitted transaction"))?,
            hash: hash?.ok_or_else(|| err("signer omitted transaction hash"))?,
        })
    }
    fn create_client(&self, key: &str, chain: u32, account: i64, index: u8) -> ArbResult<()> {
        // CreateClient writes sharedlib's global chainId, even though our used signing
        // operations take their chain from the individual TxClient.
        let _creation = CLIENT_CREATION
            .lock()
            .map_err(|_| err("signer client creation lock poisoned"))?;
        let chain = i32::try_from(chain).map_err(|_| err("chain id exceeds signer ABI range"))?;
        let key = CString::new(key.strip_prefix("0x").unwrap_or(key))
            .map_err(|_| err("invalid private key encoding"))?;
        // Null URL disables the signer's own HTTP calls. Every nonce is explicit.
        let result = unsafe {
            (self.create)(
                std::ptr::null_mut(),
                key.as_ptr().cast_mut(),
                chain,
                i32::from(index),
                account,
            )
        };
        if self.string(result)?.is_some() {
            return Err(err("official signer could not initialize API key"));
        }
        Ok(())
    }
    fn auth(&self, deadline: i64, key: u8, account: i64) -> ArbResult<String> {
        let raw = unsafe { (self.auth)(deadline, i32::from(key), account) };
        let token = self.string(raw.token);
        let error = self.string(raw.error);
        if error?.is_some() {
            return Err(err("official signer could not generate auth token"));
        }
        token?.ok_or_else(|| err("official signer omitted auth token"))
    }
    #[allow(clippy::too_many_arguments)]
    fn order(
        &self,
        market: i32,
        index: i64,
        base: i64,
        price: u32,
        ask: bool,
        reduce: bool,
        nonce: i64,
        key: u8,
        account: i64,
    ) -> ArbResult<SignedTx> {
        // The C ABI takes int price but Go casts it to uint32; preserve all 32 bits.
        let raw = unsafe {
            (self.order)(
                market,
                index,
                base,
                price as i32,
                i32::from(ask),
                0,
                0,
                i32::from(reduce),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                nonce,
                i32::from(key),
                account,
            )
        };
        self.signed(raw, 14)
    }
    fn cancel(
        &self,
        market: i32,
        index: i64,
        nonce: i64,
        key: u8,
        account: i64,
    ) -> ArbResult<SignedTx> {
        self.signed(
            unsafe { (self.cancel)(market, index, 0, nonce, i32::from(key), account) },
            15,
        )
    }
    fn leverage(
        &self,
        market: i32,
        imf: i32,
        mode: crate::MarginMode,
        nonce: i64,
        key: u8,
        account: i64,
    ) -> ArbResult<SignedTx> {
        self.signed(
            unsafe {
                (self.leverage)(
                    market,
                    imf,
                    if mode.is_cross() { 0 } else { 1 },
                    0,
                    nonce,
                    i32::from(key),
                    account,
                )
            },
            20,
        )
    }

    /// `SignUpdateMargin(marketIndex, usdcAmount, direction, skipNonce, nonce, apiKeyIndex,
    /// accountIndex)`：tx type 29，`direction = 1` 加保证金，`skipNonce = 0`（严格 +1）。
    /// 签名链 id 来自 TxClient 自己的 chain（`GetUpdateMarginTransaction` 用 `c.chainId`），
    /// 与 `SignUpdateLeverage` 一样不碰 sharedlib 的全局 chainId。
    /// <https://github.com/elliottech/lighter-go/blob/v1.0.10/client/tx_get.go>
    /// → `types/txtypes/update_margin.go::Hash` 首个域元素就是该 chain id。
    fn margin(
        &self,
        market: i32,
        usdc: i64,
        nonce: i64,
        key: u8,
        account: i64,
    ) -> ArbResult<SignedTx> {
        self.signed(
            unsafe {
                (self.margin)(
                    market,
                    usdc,
                    ADD_TO_ISOLATED_MARGIN,
                    0,
                    nonce,
                    i32::from(key),
                    account,
                )
            },
            TX_TYPE_UPDATE_MARGIN,
        )
    }
}

static CLIENT_CREATION: StdMutex<()> = StdMutex::new(());
static REGISTRATIONS: LazyLock<StdMutex<HashSet<(u32, i64, u8)>>> =
    LazyLock::new(|| StdMutex::new(HashSet::new()));
struct Registration(u32, i64, u8);
impl Registration {
    fn acquire(chain: u32, account: i64, key: u8) -> ArbResult<Self> {
        let mut registrations = REGISTRATIONS
            .lock()
            .map_err(|_| err("signer registration lock poisoned"))?;
        if registrations
            .iter()
            .any(|&(other_chain, other_account, other_key)| {
                other_account == account && other_key == key && other_chain != chain
            })
        {
            return Err(err(
                "Lighter signer client collision across chains: account/API key index is already \
                 registered on another deployment; use a different API key index or separate processes",
            ));
        }
        if !registrations.insert((chain, account, key)) {
            return Err(err("account/API key already has a broker in this process"));
        }
        Ok(Self(chain, account, key))
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut registrations) = REGISTRATIONS.lock() {
            registrations.remove(&(self.0, self.1, self.2));
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn lighter_positions_report_allocated_margin_and_liquidation_price_but_zero_means_none() {
        let position = |extra: &str| -> AccountPosition {
            serde_json::from_str(&format!(
                r#"{{"market_id":38,"symbol":"LIT","sign":1,"position":"751.99","avg_entry_price":"3.987",
                    "position_value":"3052.3","initial_margin_fraction":"33.33","margin_mode":1{extra}}}"#
            ))
            .unwrap()
        };
        let isolated = leg_state_from(&position(
            r#","allocated_margin":"1784.5","liquidation_price":"2.61""#,
        ));
        assert_eq!(isolated.margin_usdt, Some(Decimal::new(17845, 1)));
        assert_eq!(isolated.liquidation_price, Some(Decimal::new(261, 2)));
        // 全仓：分配保证金与强平价是 "0"，按没有处理，不当成 0。
        let cross = leg_state_from(&position(
            r#","allocated_margin":"0","liquidation_price":"0""#,
        ));
        assert_eq!((cross.margin_usdt, cross.liquidation_price), (None, None));
        // 字段缺省（老响应）。
        let bare = leg_state_from(&position(""));
        assert_eq!((bare.margin_usdt, bare.liquidation_price), (None, None));
    }

    #[test]
    fn position_funding_rows_parse_with_second_or_millisecond_timestamps() {
        let page: FundingPage = serde_json::from_value(serde_json::json!({
            "code": 200,
            "position_fundings": [
                {"timestamp": 1790755200, "market_id": 7, "funding_id": 1, "change": "-0.012", "rate": "0.00001", "position_size": "1797.9", "position_side": "long"},
                {"timestamp": 1790751600000_i64, "market_id": 7, "funding_id": 2, "change": "0.004", "rate": "-0.000004", "position_size": "1797.9", "position_side": "long"}
            ],
            "next_cursor": ""
        }))
        .unwrap();
        assert_eq!(page.position_fundings.len(), 2);
        assert_eq!(
            funding_time(page.position_fundings[0].timestamp).unwrap(),
            funding_time(1_790_755_200_000).unwrap(),
            "秒与毫秒指向同一时刻"
        );
        let total: Decimal = page
            .position_fundings
            .iter()
            .map(|row| decimal(&row.change).unwrap())
            .sum();
        assert_eq!(total, Decimal::new(-8, 3));
    }

    #[test]
    fn robinhood_markets_without_is_frozen_still_parse() {
        // 2026-09-29 GET api.rh.lighter.xyz/api/v1/orderBookDetails 的一行（原样，删掉了无关字段）。
        let market: Market = serde_json::from_str(
            r#"{"market_id":38,"symbol":"ANTHROPIC","market_type":"perp","status":"active","supported_size_decimals":5,"supported_price_decimals":1,"size_decimals":5,"price_decimals":1,"min_base_amount":"0.00320","min_quote_amount":"10.000000","multiplier":"1.000000000000000000","quote_multiplier":1,"min_initial_margin_fraction":2000}"#,
        )
        .unwrap();
        assert_eq!(market.is_frozen, None);
        let frozen: Market = serde_json::from_str(
            r#"{"market_id":1,"symbol":"BTC","market_type":"perp","status":"active","supported_size_decimals":5,"supported_price_decimals":1,"size_decimals":5,"price_decimals":1,"min_base_amount":"0.0002","min_quote_amount":"10","multiplier":"1.000000000000000000","quote_multiplier":1,"is_frozen":true,"min_initial_margin_fraction":200}"#,
        )
        .unwrap();
        assert_eq!(frozen.is_frozen, Some(true));
    }

    #[test]
    fn omitted_fees_are_zero_only_for_fee_free_accounts() {
        // 2026-09-29 api.rh.lighter.xyz recentTrades 的一笔（原样）：taker_fee 与集成商费都缺省。
        let trade: Trade = serde_json::from_str(
            r#"{"trade_id":1141327917,"market_id":38,"ask_id":10977524113902927,"bid_id":11258999033826256,"ask_account_id":281474976710332,"bid_account_id":30946,"is_maker_ask":true,"size":"0.16041","usd_amount":"336.155196","maker_fee":102}"#,
        )
        .unwrap();
        // 我们是吃单（买方）：0 费率账户按 0 记，付费账户拒绝下结论。
        assert_eq!(
            trade_fee_ticks(&trade, false, Decimal::ZERO).unwrap(),
            (0, 0)
        );
        assert!(trade_fee_ticks(&trade, false, Decimal::new(35, 5)).is_err());
        // 我们是挂单方：字段在就用它。
        assert_eq!(
            trade_fee_ticks(&trade, true, Decimal::ZERO).unwrap(),
            (102, 0)
        );
        // 我们是挂单方、而 maker_fee 被省略（0 费率）：0 费率账户按 0 记，付费账户拒绝下结论。
        let free_maker: Trade = serde_json::from_str(
            r#"{"trade_id":1141327918,"market_id":38,"ask_id":1,"bid_id":2,"ask_account_id":30946,"bid_account_id":9,"is_maker_ask":true,"size":"1797.9","usd_amount":"1015.0"}"#,
        )
        .unwrap();
        assert_eq!(
            trade_fee_ticks(&free_maker, true, Decimal::ZERO).unwrap(),
            (0, 0)
        );
        assert!(trade_fee_ticks(&free_maker, true, Decimal::new(35, 5)).is_err());
        let mut paid = trade;
        paid.taker_fee = Some(350);
        paid.integrator_taker_fee = Some(20);
        assert_eq!(
            trade_fee_ticks(&paid, false, Decimal::ZERO).unwrap(),
            (350, 20)
        );
    }

    #[test]
    fn transient_failures_are_rate_limits_or_gateway_errors() {
        use reqwest::StatusCode;
        assert!(is_transient(StatusCode::TOO_MANY_REQUESTS, Some(23000)));
        assert!(is_transient(StatusCode::TOO_MANY_REQUESTS, None));
        // RH 有时用 200 以外的状态码配 23000，也算限频。
        assert!(is_transient(StatusCode::BAD_REQUEST, Some(23000)));
        for gateway in [500, 502, 503, 504] {
            assert!(is_transient(StatusCode::from_u16(gateway).unwrap(), None));
        }
        assert!(!is_transient(StatusCode::BAD_REQUEST, Some(20001)));
        assert!(!is_transient(StatusCode::UNAUTHORIZED, None));
        assert!(!is_transient(StatusCode::OK, Some(200)));
        // 全部间隔加起来不超过 15 秒：看板启动时最多多等这么久。
        let total: Duration = READ_RETRY_DELAYS.iter().sum();
        assert!(total <= Duration::from_secs(15));
    }

    #[test]
    fn schema_errors_name_the_field_but_never_echo_values() {
        let missing = serde_json::from_str::<Orders>(r#"{"code":200}"#)
            .err()
            .unwrap();
        let text = schema_error("accountActiveOrders", &missing).to_string();
        assert!(text.contains("orders"), "{text}");
        let leaked = serde_json::from_str::<Orders>(
            r#"{"orders":[{"order_index":"secret-token-abc","client_order_index":1}]}"#,
        )
        .err()
        .unwrap();
        let text = schema_error("accountActiveOrders", &leaked).to_string();
        assert!(!text.contains("secret-token-abc"), "{text}");
        assert!(text.contains("invalid type"), "{text}");
    }

    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn client_indices_preserve_mainnet_history_but_separate_robinhood() {
        let id = ClientOrderId("test-order-1".into());
        // Independent SHA-256 vector using the pre-deployment mainnet salt.
        assert_eq!(
            client_index(LighterDeployment::Mainnet, 123_456, &id),
            199_807_361_613_236
        );
        assert_eq!(
            client_index(LighterDeployment::Robinhood, 123_456, &id),
            170_803_919_974_821
        );
    }

    #[test]
    fn signer_registration_blocks_cross_chain_overwrite_and_releases_on_drop() {
        let mainnet = LighterDeployment::Mainnet.chain_id();
        let robinhood = LighterDeployment::Robinhood.chain_id();
        let first = Registration::acquire(mainnet, 912_345, 2).unwrap();
        assert!(Registration::acquire(mainnet, 912_345, 2).is_err());
        assert!(Registration::acquire(robinhood, 912_345, 2).is_err());
        // Different API key or account slots remain independent, even across chains.
        let second = Registration::acquire(robinhood, 912_345, 3).unwrap();
        let third = Registration::acquire(robinhood, 912_346, 2).unwrap();
        drop(first);
        let replacement = Registration::acquire(robinhood, 912_345, 2).unwrap();
        assert!(Registration::acquire(mainnet, 912_345, 2).is_err());
        drop((replacement, second, third));
    }

    #[test]
    fn journal_replays_each_deployment_and_refuses_the_other_chain() {
        let dir = std::env::temp_dir().join(format!(
            "arb-lighter-deployments-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        for deployment in [LighterDeployment::Mainnet, LighterDeployment::Robinhood] {
            let path = dir.join(format!("{}.jsonl", deployment.venue()));
            let order = NewOrder {
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId("test-order-1".into()),
                venue: deployment.venue(),
                symbol: Symbol::perp("TSLA", "USDT"),
                side: Side::Buy,
                notional_usdt: dec!(200),
                quantity: Some(dec!(1)),
                limit_price: Some(dec!(200)),
                reduce_only: false,
                leverage: Some(dec!(2)),
            };
            let index = client_index(deployment, 123_456, &order.client_order_id);
            {
                let mut journal = Journal::open(&path, deployment, 123_456, 2).unwrap();
                journal
                    .append(&JournalRecord::Intent {
                        index,
                        intent: Intent {
                            order: order.clone(),
                            market: 16,
                            quantity: dec!(1),
                            terminal: None,
                        },
                    })
                    .unwrap();
                journal.append(&JournalRecord::Nonce { nonce: 7 }).unwrap();
            }
            let journal = Journal::open(&path, deployment, 123_456, 2).unwrap();
            let restored = journal.orders.get(&index).unwrap();
            assert_eq!(restored.order.venue, deployment.venue());
            assert_eq!(restored.order.client_order_id, order.client_order_id);
            assert_eq!(restored.quantity, dec!(1));
            assert_eq!(journal.last_nonce, Some(7));
            drop(journal);
            let other = match deployment {
                LighterDeployment::Mainnet => LighterDeployment::Robinhood,
                LighterDeployment::Robinhood => LighterDeployment::Mainnet,
            };
            assert!(Journal::open(&path, other, 123_456, 2).is_err());
            assert!(Journal::open(&path, deployment, 123_457, 2).is_err());
            assert!(Journal::open(&path, deployment, 123_456, 3).is_err());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn opening_size_rounds_down_but_exit_must_be_exact() {
        assert_eq!(scaled_quantity(dec!(0.123456), 5, false).unwrap(), 12345);
        assert!(scaled_quantity(dec!(0.123456), 5, true).is_err());
        assert_eq!(scaled_quantity(dec!(0.12345), 5, true).unwrap(), 12345);
        assert!(scaled_quantity(dec!(0.000001), 5, false).is_err());
        assert!(scaled_quantity(Decimal::ZERO, 5, false).is_err());
        assert!(scaled_quantity(Decimal::from(MAX_INDEX) + Decimal::ONE, 0, true).is_err());
    }

    #[test]
    fn leverage_never_exceeds_requested_risk() {
        assert_eq!(margin_fraction(dec!(3), 200).unwrap(), 3334);
        assert_eq!(margin_fraction(dec!(50), 200).unwrap(), 200);
        assert!(margin_fraction(dec!(51), 200).is_err());
        assert!(margin_fraction(dec!(0), 200).is_err());
    }

    #[test]
    fn partial_ioc_is_not_a_complete_exit() {
        assert_eq!(
            order_status("filled", dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            order_status("filled", dec!(2), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            order_status("canceled-reduce-only", dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            order_status("in-progress", dec!(2), dec!(0)).unwrap(),
            OrderStatus::Pending
        );
        assert!(order_status("new-status", dec!(2), dec!(0)).is_err());
    }

    // ---------------------------------------------------------------------------------
    // add_margin (tx type 29 `L2UpdateMargin`)
    // ---------------------------------------------------------------------------------

    use std::collections::VecDeque;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const MARGIN_KEY: u8 = 2;
    const ACCOUNT_PATH: &str = "/api/v1/account";
    const NONCE_PATH: &str = "/api/v1/nextNonce";
    const SEND_PATH: &str = "/api/v1/sendTx";

    /// `SignUpdateMargin` 收到的 (market, usdc, direction, skipNonce, nonce, apiKey, account)。
    type SignCall = (i32, i64, i32, u8, i64, i32, i64);
    static MARGIN_SIGN_CALLS: StdMutex<Vec<SignCall>> = StdMutex::new(Vec::new());
    static LEVERAGE_SIGN_MODES: StdMutex<Vec<i32>> = StdMutex::new(Vec::new());

    /// 各测试用不同的账户号，所以按账户号过滤就能互不干扰地断言「签过几次、签了什么」。
    fn sign_calls_for(account: i64) -> Vec<SignCall> {
        MARGIN_SIGN_CALLS
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|call| call.6 == account)
            .collect()
    }

    fn fake_raw(kind: u8, info: String, hash: String) -> RawSigned {
        RawSigned {
            kind,
            info: CString::new(info).unwrap().into_raw(),
            hash: CString::new(hash).unwrap().into_raw(),
            message: std::ptr::null_mut(),
            error: std::ptr::null_mut(),
        }
    }
    fn fake_raw_error() -> RawSigned {
        RawSigned {
            kind: 0,
            info: std::ptr::null_mut(),
            hash: std::ptr::null_mut(),
            message: std::ptr::null_mut(),
            error: CString::new("fake signer failure").unwrap().into_raw(),
        }
    }
    unsafe extern "C" fn fake_free(ptr: *mut c_void) {
        drop(unsafe { CString::from_raw(ptr.cast()) });
    }
    unsafe extern "C" fn fake_create(
        _: *mut c_char,
        _: *mut c_char,
        _: i32,
        _: i32,
        _: i64,
    ) -> *mut c_char {
        std::ptr::null_mut()
    }
    unsafe extern "C" fn fake_auth(_: i64, _: i32, _: i64) -> RawAuth {
        RawAuth {
            token: CString::new("fake-token").unwrap().into_raw(),
            error: std::ptr::null_mut(),
        }
    }
    #[allow(clippy::too_many_arguments)]
    unsafe extern "C" fn fake_order(
        _: i32,
        _: i64,
        _: i64,
        _: i32,
        _: i32,
        _: i32,
        _: i32,
        _: i32,
        _: i32,
        _: i64,
        _: i64,
        _: i32,
        _: i32,
        _: u8,
        _: u8,
        _: u8,
        _: i64,
        _: i32,
        _: i64,
    ) -> RawSigned {
        fake_raw_error()
    }
    unsafe extern "C" fn fake_cancel(_: i32, _: i64, _: u8, _: i64, _: i32, _: i64) -> RawSigned {
        fake_raw_error()
    }
    unsafe extern "C" fn fake_leverage(
        _: i32,
        _: i32,
        mode: i32,
        _: u8,
        _: i64,
        _: i32,
        _: i64,
    ) -> RawSigned {
        LEVERAGE_SIGN_MODES.lock().unwrap().push(mode);
        fake_raw(20, "leverage-info".into(), "leverage-hash".into())
    }
    /// 记录入参；金额 666 USDC 是「签名库报错」的哨兵值。
    unsafe extern "C" fn fake_margin(
        market: i32,
        usdc: i64,
        direction: i32,
        skip: u8,
        nonce: i64,
        key: i32,
        account: i64,
    ) -> RawSigned {
        MARGIN_SIGN_CALLS
            .lock()
            .unwrap()
            .push((market, usdc, direction, skip, nonce, key, account));
        if usdc == 666_000_000 {
            return fake_raw_error();
        }
        fake_raw(
            29,
            format!("fake-tx-info-{account}-{nonce}"),
            format!("fakehash{nonce}"),
        )
    }
    fn fake_signer() -> Arc<NativeSigner> {
        Arc::new(NativeSigner {
            _library: Library::from(libloading::os::unix::Library::this()),
            free: fake_free,
            create: fake_create,
            auth: fake_auth,
            order: fake_order,
            cancel: fake_cancel,
            leverage: fake_leverage,
            margin: fake_margin,
        })
    }

    #[derive(Clone, Debug)]
    struct Seen {
        method: String,
        path: String,
        body: String,
    }
    struct FakeLighter {
        base: String,
        seen: Arc<StdMutex<Vec<Seen>>>,
    }
    impl FakeLighter {
        fn requests(&self, path: &str) -> Vec<Seen> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter(|seen| seen.path == path)
                .cloned()
                .collect()
        }
    }

    type Reply = (u16, String);

    /// 本地假 Lighter：每个路径依次回放脚本里的 (HTTP 状态, body)，队列只剩一个时一直重复它；
    /// 状态 0 = 收到请求后直接断开连接（模拟传输失败）；没有脚本的路径回 404。记录收到的每个请求。
    async fn fake_lighter(routes: Vec<(&'static str, Vec<Reply>)>) -> FakeLighter {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let routes: HashMap<String, VecDeque<Reply>> = routes
            .into_iter()
            .map(|(path, replies)| (path.to_string(), replies.into()))
            .collect();
        let routes = Arc::new(StdMutex::new(routes));
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (shared_routes, shared_seen) = (Arc::clone(&routes), Arc::clone(&seen));
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let (routes, seen) = (Arc::clone(&shared_routes), Arc::clone(&shared_seen));
                tokio::spawn(async move {
                    let mut data = Vec::new();
                    let mut chunk = [0_u8; 4096];
                    let header_end = loop {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        data.extend_from_slice(&chunk[..n]);
                        if let Some(at) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                            break at + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&data[..header_end]).to_string();
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    while data.len() < header_end + length {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&chunk[..n]);
                    }
                    let mut first = head.lines().next().unwrap_or("").split(' ');
                    let method = first.next().unwrap_or("").to_string();
                    let target = first.next().unwrap_or("");
                    let path = target.split('?').next().unwrap_or("").to_string();
                    seen.lock().unwrap().push(Seen {
                        method,
                        path: path.clone(),
                        body: String::from_utf8_lossy(&data[header_end..]).to_string(),
                    });
                    let (status, body) = {
                        let mut routes = routes.lock().unwrap();
                        match routes.get_mut(&path) {
                            Some(queue) if queue.len() > 1 => queue.pop_front().unwrap(),
                            Some(queue) => queue.front().cloned().unwrap_or((404, String::new())),
                            None => (404, r#"{"code":29404,"message":"not found"}"#.to_string()),
                        }
                    };
                    if status == 0 {
                        return;
                    }
                    let reply = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        FakeLighter { base, seen }
    }

    fn position_json(
        symbol: &str,
        market_id: i32,
        size: &str,
        mode: i32,
        allocated: &str,
    ) -> String {
        format!(
            r#"{{"market_id":{market_id},"symbol":"{symbol}","sign":1,"position":"{size}","avg_entry_price":"200","position_value":"500","initial_margin_fraction":"50.00","margin_mode":{mode},"allocated_margin":"{allocated}"}}"#
        )
    }
    fn account_with(positions: &[String]) -> Account {
        serde_json::from_str(&format!(
            r#"{{"index":1,"positions":[{}]}}"#,
            positions.join(",")
        ))
        .unwrap()
    }
    #[test]
    fn leverage_signing_and_readback_include_the_selected_mode() {
        let signer = fake_signer();
        for (mode, wire_mode) in [
            (crate::MarginMode::Cross, 0),
            (crate::MarginMode::Isolated, 1),
        ] {
            let tx = signer
                .leverage(7, 5000, mode, 0, MARGIN_KEY, 777777)
                .unwrap();
            assert_eq!(tx.kind, 20);
            assert_eq!(
                *LEVERAGE_SIGN_MODES.lock().unwrap().last().unwrap(),
                wire_mode
            );
            let account = account_with(&[position_json("TSLA", 7, "0", wire_mode, "0")]);
            assert!(leverage_matches(&account, 7, 5000, mode));
            let other = if mode.is_cross() {
                crate::MarginMode::Isolated
            } else {
                crate::MarginMode::Cross
            };
            assert!(!leverage_matches(&account, 7, 5000, other));
        }
    }

    /// `account` 的一个 200 响应：账户 `account` 在市场 7（TSLA）上有一个持仓。
    fn account_reply(account: i64, mode: i32, allocated: &str) -> Reply {
        (
            200,
            format!(
                r#"{{"code":200,"total":1,"accounts":[{{"index":{account},"positions":[{}]}}]}}"#,
                position_json("TSLA", 7, "2.5", mode, allocated)
            ),
        )
    }
    fn nonce_reply(nonce: i64) -> Reply {
        (200, format!(r#"{{"code":200,"nonce":{nonce}}}"#))
    }
    fn accepted_reply(hash: &str) -> Reply {
        (
            200,
            format!(r#"{{"code":200,"message":"ok","tx_hash":"{hash}"}}"#),
        )
    }
    fn tsla() -> Symbol {
        Symbol::perp("TSLA", "USDT")
    }

    struct Rig {
        broker: LighterBroker,
        server: FakeLighter,
        dir: PathBuf,
    }
    impl Rig {
        fn count(&self, path: &str) -> usize {
            self.server.requests(path).len()
        }
        fn total_requests(&self) -> usize {
            self.server.seen.lock().unwrap().len()
        }
        fn finish(self) {
            drop(self.broker);
            let _ = std::fs::remove_dir_all(self.dir);
        }
    }
    async fn rig(account: i64, enabled: bool, routes: Vec<(&'static str, Vec<Reply>)>) -> Rig {
        let server = fake_lighter(routes).await;
        let dir = std::env::temp_dir().join(format!(
            "arb-lighter-margin-{}-{account}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let deployment = LighterDeployment::Mainnet;
        let journal =
            Journal::open(&dir.join("journal.jsonl"), deployment, account, MARGIN_KEY).unwrap();
        let broker = LighterBroker {
            client: Client::builder().no_proxy().build().unwrap(),
            deployment,
            base_url: server.base.clone(),
            signer: fake_signer(),
            account,
            key: MARGIN_KEY,
            enabled,
            slippage: Some(dec!(0.01)),
            journal: Mutex::new(journal),
            fee: Decimal::ZERO,
            verified_leverage: Verified::new(Duration::from_secs(20)),
            warmed: Verified::new(Duration::from_secs(20)),
            _registration: Registration::acquire(deployment.chain_id(), account, MARGIN_KEY)
                .unwrap(),
        };
        Rig {
            broker,
            server,
            dir,
        }
    }
    const FAST: Duration = Duration::from_millis(5);

    #[tokio::test]
    async fn margin_is_applied_after_readback_and_exactly_one_tx_is_sent() {
        let account = 710_001;
        let rig = rig(
            account,
            true,
            vec![
                (
                    ACCOUNT_PATH,
                    // 预读、第一次回读（没变）、第二次回读（涨了 12.34）。
                    vec![
                        account_reply(account, 1, "100.00"),
                        account_reply(account, 1, "100.00"),
                        account_reply(account, 1, "112.34"),
                    ],
                ),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (SEND_PATH, vec![accepted_reply("fakehash41")]),
            ],
        )
        .await;
        assert!(rig.broker.supports_add_margin());
        let outcome = rig
            .broker
            .add_margin_with(&tsla(), dec!(12.34), 3, FAST)
            .await
            .unwrap();
        assert_eq!(outcome, MarginOutcome::Applied);
        assert_eq!(rig.count(SEND_PATH), 1, "one write, never a retry");
        assert_eq!(rig.count(ACCOUNT_PATH), 3);
        // 签名库收到的就是：市场 7、12.34 → 12_340_000、direction 1（加）、skipNonce 0、nonce 41。
        assert_eq!(
            sign_calls_for(account),
            vec![(7, 12_340_000, 1, 0, 41, i32::from(MARGIN_KEY), account)]
        );
        // 请求就是官方 sendTx 的表单：tx_type=29&tx_info=<签名库给的 txInfo>。
        let sent = rig.server.requests(SEND_PATH);
        assert_eq!(sent[0].method, "POST");
        assert_eq!(sent[0].body, "tx_type=29&tx_info=fake-tx-info-710001-41");
        // nonce 预留已落盘，且这次生效后保持占用。
        assert_eq!(rig.broker.journal.lock().await.last_nonce, Some(41));
        let text = std::fs::read_to_string(rig.dir.join("journal.jsonl")).unwrap();
        assert!(text.contains(r#""kind":"nonce","nonce":41"#), "{text}");
        rig.finish();
    }

    #[tokio::test]
    async fn api_rejection_is_refused_and_releases_the_unconsumed_nonce() {
        let account = 710_002;
        let rig = rig(
            account,
            true,
            vec![
                (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (
                    SEND_PATH,
                    vec![(
                        400,
                        r#"{"code":21301,"message":"not enough collateral"}"#.to_string(),
                    )],
                ),
            ],
        )
        .await;
        for call in 1..=2 {
            let outcome = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 3, FAST)
                .await
                .unwrap();
            let MarginOutcome::Refused(reason) = outcome else {
                panic!("expected Refused, got {outcome:?}");
            };
            assert!(reason.contains("21301"), "{reason}");
            assert!(reason.contains("not enough collateral"), "{reason}");
            assert_eq!(
                rig.broker.journal.lock().await.last_nonce,
                None,
                "an API rejection leaves the nonce unconsumed, so it must not stay reserved"
            );
            // 第二次没有被「上一个 nonce 未决」拦住，而且每次调用恰好一次 sendTx。
            assert_eq!(rig.count(SEND_PATH), call);
        }
        assert_eq!(sign_calls_for(account).len(), 2);
        assert_eq!(rig.count(ACCOUNT_PATH), 2, "no readback after a refusal");
        // 重放日志：预留与释放成对出现，重启后 nonce 状态是干净的（可以再预留 41）。
        let Rig { broker, dir, .. } = rig;
        drop(broker);
        let text = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
        assert_eq!(
            text.matches(r#""kind":"nonce_released""#).count(),
            2,
            "{text}"
        );
        let replayed = Journal::open(
            &dir.join("journal.jsonl"),
            LighterDeployment::Mainnet,
            account,
            MARGIN_KEY,
        )
        .unwrap();
        assert_eq!(replayed.last_nonce, None);
        drop(replayed);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn rate_limit_before_processing_is_refused_and_releases_the_nonce() {
        let account = 710_003;
        let rig = rig(
            account,
            true,
            vec![
                (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (SEND_PATH, vec![(429, "Too Many Requests".to_string())]),
            ],
        )
        .await;
        let outcome = rig
            .broker
            .add_margin_with(&tsla(), dec!(10), 3, FAST)
            .await
            .unwrap();
        let MarginOutcome::Refused(reason) = outcome else {
            panic!("expected Refused, got {outcome:?}");
        };
        assert!(reason.contains("429"), "{reason}");
        assert_eq!(rig.count(SEND_PATH), 1, "a 429 is not retried either");
        assert_eq!(rig.broker.journal.lock().await.last_nonce, None);
        rig.finish();
    }

    #[tokio::test]
    async fn server_errors_and_transport_failures_are_unknown_and_never_retried() {
        let cases: [(i64, u16, &str); 4] = [
            (710_010, 500, ""),
            (710_011, 502, "<html>bad gateway</html>"),
            (710_012, 0, ""),
            (710_013, 200, "not json"),
        ];
        for (account, status, body) in cases {
            let rig = rig(
                account,
                true,
                vec![
                    (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                    (NONCE_PATH, vec![nonce_reply(41)]),
                    (SEND_PATH, vec![(status, body.to_string())]),
                ],
            )
            .await;
            let outcome = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 3, FAST)
                .await
                .unwrap();
            let MarginOutcome::Unknown(reason) = outcome else {
                panic!("{status}: expected Unknown, got {outcome:?}");
            };
            assert!(reason.contains("outcome unknown"), "{reason}");
            assert_eq!(rig.count(SEND_PATH), 1, "{status}: exactly one write");
            assert_eq!(rig.count(ACCOUNT_PATH), 1, "{status}: no readback guess");
            // 结果不明：预留保持占用（fail closed），下一次写在发出任何请求之前就被拦下。
            assert_eq!(rig.broker.journal.lock().await.last_nonce, Some(41));
            let blocked = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 3, FAST)
                .await
                .unwrap_err();
            assert!(blocked.to_string().contains("unresolved"), "{blocked}");
            assert_eq!(rig.count(SEND_PATH), 1, "{status}: still one write");
            rig.finish();
        }
    }

    #[tokio::test]
    async fn accepted_tx_whose_margin_never_rises_is_unknown_after_a_bounded_readback() {
        for (account, later, last_seen) in
            [(710_020, "100.00", "100.00"), (710_021, "90.00", "90.00")]
        {
            let rig = rig(
                account,
                true,
                vec![
                    (
                        ACCOUNT_PATH,
                        vec![
                            account_reply(account, 1, "100.00"),
                            account_reply(account, 1, later),
                        ],
                    ),
                    (NONCE_PATH, vec![nonce_reply(41)]),
                    (SEND_PATH, vec![accepted_reply("fakehash41")]),
                ],
            )
            .await;
            let outcome = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 4, FAST)
                .await
                .unwrap();
            let MarginOutcome::Unknown(reason) = outcome else {
                panic!("expected Unknown, got {outcome:?}");
            };
            assert!(reason.contains("did not rise"), "{reason}");
            assert!(
                reason.contains(&format!("last seen {last_seen}")),
                "{reason}"
            );
            assert_eq!(rig.count(SEND_PATH), 1);
            assert_eq!(
                rig.count(ACCOUNT_PATH),
                1 + 4,
                "pre-read + exactly 4 readbacks"
            );
            rig.finish();
        }
    }

    #[tokio::test]
    async fn a_mismatching_tx_hash_is_unknown_without_guessing_from_a_readback() {
        let account = 710_022;
        let rig = rig(
            account,
            true,
            vec![
                (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (SEND_PATH, vec![accepted_reply("someotherhash")]),
            ],
        )
        .await;
        let outcome = rig
            .broker
            .add_margin_with(&tsla(), dec!(10), 3, FAST)
            .await
            .unwrap();
        assert!(matches!(outcome, MarginOutcome::Unknown(_)), "{outcome:?}");
        assert_eq!(rig.count(ACCOUNT_PATH), 1);
        assert_eq!(rig.count(SEND_PATH), 1);
        rig.finish();
    }

    #[tokio::test]
    async fn a_changed_position_cannot_confirm_an_accepted_margin_add() {
        for (account, field, value) in [
            (710_070, "position", serde_json::json!("3")),
            (710_071, "sign", serde_json::json!(-1)),
            (710_072, "avg_entry_price", serde_json::json!("210")),
        ] {
            let (status, body) = account_reply(account, 1, "120");
            let mut after: serde_json::Value = serde_json::from_str(&body).unwrap();
            after["accounts"][0]["positions"][0][field] = value;
            let rig = rig(
                account,
                true,
                vec![
                    (
                        ACCOUNT_PATH,
                        vec![
                            account_reply(account, 1, "100"),
                            (status, after.to_string()),
                        ],
                    ),
                    (NONCE_PATH, vec![nonce_reply(41)]),
                    (SEND_PATH, vec![accepted_reply("fakehash41")]),
                ],
            )
            .await;
            let outcome = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 2, FAST)
                .await
                .unwrap();
            assert!(
                matches!(outcome, MarginOutcome::Unknown(_)),
                "{field}: {outcome:?}"
            );
            assert_eq!(rig.count(SEND_PATH), 1);
            assert_eq!(rig.broker.journal.lock().await.last_nonce, Some(41));
            rig.finish();
        }
    }

    #[tokio::test]
    async fn read_only_broker_refuses_before_any_request_or_signature() {
        let account = 710_040;
        let rig = rig(
            account,
            false,
            vec![
                (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (SEND_PATH, vec![accepted_reply("fakehash41")]),
            ],
        )
        .await;
        let error = Broker::add_margin(&rig.broker, &tsla(), dec!(10))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("trading is disabled"), "{error}");
        assert_eq!(rig.total_requests(), 0, "not even a read was made");
        assert!(sign_calls_for(account).is_empty(), "nothing was signed");
        assert_eq!(rig.broker.journal.lock().await.last_nonce, None);
        rig.finish();
    }

    #[tokio::test]
    async fn unusable_positions_and_amounts_are_errors_before_any_write() {
        let empty = |account: i64| -> Reply {
            (
                200,
                format!(
                    r#"{{"code":200,"total":1,"accounts":[{{"index":{account},"positions":[]}}]}}"#
                ),
            )
        };
        let cases: [(i64, Reply, &str); 3] = [
            (710_030, account_reply(710_030, 0, "0"), "not isolated"),
            (710_031, empty(710_031), "no open position"),
            (
                710_032,
                account_reply(710_032, 1, "0"),
                "no allocated margin",
            ),
        ];
        for (account, reply, needle) in cases {
            let rig = rig(
                account,
                true,
                vec![
                    (ACCOUNT_PATH, vec![reply]),
                    (NONCE_PATH, vec![nonce_reply(41)]),
                    (SEND_PATH, vec![accepted_reply("fakehash41")]),
                ],
            )
            .await;
            let error = rig
                .broker
                .add_margin_with(&tsla(), dec!(10), 3, FAST)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(needle), "{error}");
            assert_eq!(rig.count(NONCE_PATH), 0, "{needle}: no nonce was reserved");
            assert_eq!(rig.count(SEND_PATH), 0, "{needle}: nothing was sent");
            assert!(
                sign_calls_for(account).is_empty(),
                "{needle}: nothing was signed"
            );
            assert_eq!(rig.broker.journal.lock().await.last_nonce, None);
            rig.finish();
        }
        // 金额不合法：连账户都不读。
        let account = 710_033;
        let rig = rig(
            account,
            true,
            vec![(ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")])],
        )
        .await;
        for amount in [dec!(0), dec!(-1), dec!(1.2345678)] {
            assert!(
                rig.broker
                    .add_margin_with(&tsla(), amount, 3, FAST)
                    .await
                    .is_err(),
                "{amount}"
            );
        }
        assert_eq!(rig.total_requests(), 0);
        rig.finish();
    }

    #[tokio::test]
    async fn a_signer_failure_releases_the_reservation_and_sends_nothing() {
        let account = 710_050;
        let rig = rig(
            account,
            true,
            vec![
                (ACCOUNT_PATH, vec![account_reply(account, 1, "100.00")]),
                (NONCE_PATH, vec![nonce_reply(41)]),
                (SEND_PATH, vec![accepted_reply("fakehash41")]),
            ],
        )
        .await;
        // 666 USDC 是假签名库的「报错」哨兵。
        let error = rig
            .broker
            .add_margin_with(&tsla(), dec!(666), 3, FAST)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("rejected transaction"),
            "{error}"
        );
        assert_eq!(sign_calls_for(account).len(), 1);
        assert_eq!(rig.count(SEND_PATH), 0);
        assert_eq!(rig.broker.journal.lock().await.last_nonce, None);
        rig.finish();
    }

    #[tokio::test]
    async fn the_request_is_a_form_post_to_the_deployments_own_send_tx_endpoint() {
        let account = 710_060;
        let mut rig = rig(account, true, vec![]).await;
        let signed = SignedTx {
            kind: 29,
            info: r#"{"AccountIndex":1}"#.into(),
            hash: "h".into(),
        };
        for (deployment, url) in [
            (
                LighterDeployment::Mainnet,
                "https://mainnet.zklighter.elliot.ai/api/v1/sendTx",
            ),
            (
                LighterDeployment::Robinhood,
                "https://api.rh.lighter.xyz/api/v1/sendTx",
            ),
        ] {
            rig.broker.deployment = deployment;
            rig.broker.base_url = deployment.base_url().to_string();
            let request = rig.broker.margin_request(&signed).build().unwrap();
            assert_eq!(request.method(), reqwest::Method::POST);
            assert_eq!(request.url().as_str(), url);
            assert_eq!(
                request.headers()[reqwest::header::CONTENT_TYPE],
                "application/x-www-form-urlencoded"
            );
            assert_eq!(
                request.body().and_then(|body| body.as_bytes()),
                Some(&b"tx_type=29&tx_info=%7B%22AccountIndex%22%3A1%7D"[..])
            );
        }
        assert_eq!(rig.total_requests(), 0);
        rig.finish();
    }

    #[test]
    fn margin_wire_amount_is_exact_in_millionths_and_never_rounds() {
        assert_eq!(margin_wire_amount(dec!(12.34)).unwrap(), 12_340_000);
        assert_eq!(margin_wire_amount(dec!(0.01)).unwrap(), 10_000);
        assert_eq!(margin_wire_amount(dec!(100)).unwrap(), 100_000_000);
        assert_eq!(margin_wire_amount(dec!(1.500000)).unwrap(), 1_500_000);
        // 6 位小数是线上精度的极限；再多一位就拒绝，不替调用方取整。
        assert_eq!(margin_wire_amount(dec!(1.234567)).unwrap(), 1_234_567);
        assert!(margin_wire_amount(dec!(1.2345678)).is_err());
        assert!(margin_wire_amount(dec!(0.0000001)).is_err());
        // 必须是正数，上限是官方的 2^60 - 1。
        assert!(margin_wire_amount(Decimal::ZERO).is_err());
        assert!(margin_wire_amount(dec!(-5)).is_err());
        assert_eq!(
            margin_wire_amount(dec!(1152921504606.846975)).unwrap(),
            MARGIN_MAX_WIRE
        );
        assert!(margin_wire_amount(dec!(1152921504606.846976)).is_err());
    }

    #[test]
    fn margin_target_needs_one_open_isolated_position_with_a_known_margin() {
        let tsla_isolated = position_json("TSLA", 7, "2.5", 1, "100.50");
        let target =
            margin_target(&account_with(std::slice::from_ref(&tsla_isolated)), &tsla()).unwrap();
        assert_eq!(
            target,
            MarginTarget {
                market_id: 7,
                size: dec!(2.5),
                sign: 1,
                entry_price: dec!(200),
                before: dec!(100.50)
            }
        );
        // 符号大小写不敏感，与别处的 Symbol::perp 一致；别的市场的持仓不干扰。
        let mixed = account_with(&[
            position_json("BTC", 1, "0.1", 1, "900"),
            position_json("tsla", 7, "2.5", 1, "100.50"),
        ]);
        assert_eq!(margin_target(&mixed, &tsla()).unwrap().market_id, 7);
        let refused = |positions: &[String]| {
            margin_target(&account_with(positions), &tsla())
                .unwrap_err()
                .to_string()
        };
        assert!(refused(&[]).contains("no open position"));
        assert!(refused(&[position_json("BTC", 1, "0.1", 1, "900")]).contains("no open position"));
        // 平掉的持仓（数量 0）不算有持仓。
        assert!(refused(&[position_json("TSLA", 7, "0", 1, "100")]).contains("no open position"));
        // 全仓：绝不替用户改保证金模式。
        assert!(refused(&[position_json("TSLA", 7, "2.5", 0, "0")]).contains("not isolated"));
        // 逐仓却没给分配保证金：回读没有基准，不冒险。
        assert!(
            refused(&[position_json("TSLA", 7, "2.5", 1, "0")]).contains("no allocated margin")
        );
        // 两个都匹配：不猜。
        assert!(
            refused(&[
                position_json("TSLA", 7, "2.5", 1, "100"),
                position_json("TSLA", 8, "1", 1, "50")
            ])
            .contains("several open positions")
        );
        // 市场号超出签名库接受的范围（255 是 nil 市场）。
        assert!(refused(&[position_json("TSLA", 255, "2.5", 1, "100")]).contains("outside"));
        assert!(refused(&[position_json("TSLA", 40_000, "2.5", 1, "100")]).contains("outside"));
    }

    #[test]
    fn margin_readback_requires_a_real_increase_of_the_requested_amount() {
        let applied =
            |before: Decimal, now: Decimal, amount: Decimal| margin_applied(before, now, amount);
        assert!(applied(dec!(100), dec!(110), dec!(10)));
        assert!(
            applied(dec!(100), dec!(109.90), dec!(10)),
            "0.99 × 金额是下限"
        );
        assert!(!applied(dec!(100), dec!(109.89), dec!(10)));
        assert!(!applied(dec!(100), dec!(100), dec!(10)));
        assert!(!applied(dec!(100), dec!(90), dec!(10)), "减少绝不算生效");
        // 读回只认逐仓且非零的持仓；全仓/平仓/没有都是 None。
        let account = account_with(&[position_json("TSLA", 7, "2.5", 1, "112.34")]);
        let target = margin_target(&account, &tsla()).unwrap();
        assert_eq!(margin_of(&account, &target), Some(dec!(112.34)));
        let missing = MarginTarget {
            market_id: 8,
            ..target.clone()
        };
        assert_eq!(margin_of(&account, &missing), None);
        for position in [
            position_json("TSLA", 7, "2.5", 0, "0"),
            position_json("TSLA", 7, "0", 1, "112.34"),
        ] {
            assert_eq!(margin_of(&account_with(&[position]), &target), None);
        }
    }

    #[test]
    fn send_margin_responses_are_classified_conservatively() {
        use reqwest::StatusCode;
        let classify = |status: u16, body: &str| {
            classify_margin_send(StatusCode::from_u16(status).unwrap(), body.as_bytes(), "h1")
        };
        assert_eq!(
            classify(
                200,
                r#"{"code":200,"message":"ok","tx_hash":"h1","predicted_execution_time_ms":1}"#
            ),
            MarginSend::Accepted
        );
        // 受理了但 hash 缺失/对不上：不能当成我们的那一笔，Unknown。
        for body in [r#"{"code":200,"tx_hash":"other"}"#, r#"{"code":200}"#] {
            assert!(
                matches!(classify(200, body), MarginSend::Unknown(_)),
                "{body}"
            );
        }
        // 明确拒绝：限频、官方业务码（2xx 或 4xx），没动钱。
        let refused: [(u16, &str, &str); 8] = [
            (
                400,
                r#"{"code":21301,"message":"x"}"#,
                "not enough collateral",
            ),
            (400, r#"{"code":21120,"message":"x"}"#, "invalid signature"),
            (400, r#"{"code":21615}"#, "invalid update margin direction"),
            (200, r#"{"code":21508,"message":"x"}"#, "initial margin"),
            (400, r#"{"code":20001,"message":"x"}"#, "API code 20001"),
            (429, "Too Many Requests", "HTTP 429"),
            (429, r#"{"code":23000,"message":"x"}"#, "HTTP 429"),
            (400, r#"{"code":23000,"message":"x"}"#, "Too Many Requests"),
        ];
        for (status, body, needle) in refused {
            match classify(status, body) {
                MarginSend::Refused(reason) => {
                    assert!(reason.contains(needle), "{status} {body}: {reason}");
                    assert!(reason.contains("no margin moved"), "{reason}");
                }
                other => panic!("{status} {body}: expected Refused, got {other:?}"),
            }
        }
        // 可能已处理：5xx（哪怕带业务码）、非 JSON、缺 code、服务端内部错误码、形状不对。
        let unknown: [(u16, &str); 12] = [
            (500, ""),
            (502, "<html>bad gateway</html>"),
            (503, r#"{"code":21301}"#),
            (504, ""),
            (200, "not json"),
            (400, "<html>forbidden</html>"),
            (200, "{}"),
            (200, r#"{"code":29500,"message":"internal server error"}"#),
            (400, r#"{"code":29501}"#),
            (400, r#"{"code":200}"#),
            (302, ""),
            (200, r#"{"code":"200"}"#),
        ];
        for (status, body) in unknown {
            assert!(
                matches!(classify(status, body), MarginSend::Unknown(_)),
                "{status} {body}: {:?}",
                classify(status, body)
            );
        }
        // 不回显响应体（网关可能把鉴权头原样带回）。
        match classify(400, r#"{"code":21120,"message":"Bearer abc.def.secret"}"#) {
            MarginSend::Refused(reason) => assert!(!reason.contains("secret"), "{reason}"),
            other => panic!("expected Refused, got {other:?}"),
        }
    }

    #[test]
    fn margin_constants_match_the_documented_wire_values() {
        // lighter-go v1.0.10 types/txtypes/constants.go: TxTypeL2UpdateMargin = 29,
        // AddToIsolatedMargin = 1, OneUSDC = 1_000_000, MaxExchangeUSDC = (1 << 60) - 1.
        assert_eq!((TX_TYPE_UPDATE_MARGIN, ADD_TO_ISOLATED_MARGIN), (29, 1));
        assert_eq!(MARGIN_SCALE, 1_000_000);
        assert_eq!(MARGIN_MAX_WIRE, 1_152_921_504_606_846_975);
        assert_eq!(MARGIN_CONFIRM_RATIO, dec!(0.99));
        // 回读窗口：6 次、间隔 500ms。
        assert_eq!(MARGIN_POLLS, 6);
        assert_eq!(MARGIN_POLL_INTERVAL, Duration::from_millis(500));
    }

    #[test]
    fn released_nonce_reservations_replay_and_mismatched_releases_are_rejected() {
        let dir = std::env::temp_dir().join(format!(
            "arb-lighter-release-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("journal.jsonl");
        let mainnet = LighterDeployment::Mainnet;
        {
            let mut journal = Journal::open(&path, mainnet, 123_456, 2).unwrap();
            for nonce in [5, 7] {
                journal.append(&JournalRecord::Nonce { nonce }).unwrap();
                journal.last_nonce = Some(nonce);
            }
            // 只能撤销最近一次预留，且 previous 必须比它小。
            assert!(journal.release_nonce(5, None).is_err());
            assert!(journal.release_nonce(7, Some(7)).is_err());
            assert!(journal.release_nonce(7, Some(9)).is_err());
            assert_eq!(
                journal.last_nonce,
                Some(7),
                "failed releases change nothing"
            );
            journal.release_nonce(7, Some(5)).unwrap();
            assert_eq!(journal.last_nonce, Some(5));
            // 释放之后同一个 nonce 可以再预留（单调性按还原后的值算），再释放一次。
            journal.append(&JournalRecord::Nonce { nonce: 7 }).unwrap();
            journal.last_nonce = Some(7);
            journal.release_nonce(7, Some(5)).unwrap();
            journal.append(&JournalRecord::Nonce { nonce: 6 }).unwrap();
            journal.last_nonce = Some(6);
        }
        let journal = Journal::open(&path, mainnet, 123_456, 2).unwrap();
        assert_eq!(journal.last_nonce, Some(6));
        drop(journal);
        // 手写一条对不上最近预留的释放记录：拒绝重放。
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(
            file,
            r#"{{"kind":"nonce_released","nonce":9,"previous":null}}"#
        )
        .unwrap();
        drop(file);
        assert!(Journal::open(&path, mainnet, 123_456, 2).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore = "requires pinned official native signer; offline only"]
    fn real_signer_preserves_exact_margin_amounts_on_mainnet_and_rh() {
        // Explicit opt-in; a missing library or SHA mismatch is a failure, never a pass.
        let path = std::env::var("ARB_LIGHTER_SIGNER_LIB")
            .unwrap_or_else(|_| "/opt/lighter/lighter-signer-linux-amd64.so".into());
        let signer = NativeSigner::load(Path::new(&path))
            .expect("install the pinned v1.0.10 signer and set ARB_LIGHTER_SIGNER_LIB");
        // Fake 40-byte key, in-memory clients only: create_client passes a null HTTP URL.
        // Separate account/key pairs avoid the official registry's cross-chain overwrite.
        let key = "0123456789abcdef".repeat(5);
        let deployments = [
            (LighterDeployment::Mainnet, 987_654),
            (LighterDeployment::Robinhood, 987_655),
        ];
        for (deployment, account) in deployments {
            signer
                .create_client(&key, deployment.chain_id(), account, 5)
                .unwrap();
        }
        for (deployment, account) in deployments {
            // Check the actual signed wire amount, not just our conversion or a fake ABI:
            // one collateral micro-unit, a six-decimal top-up, and the official maximum.
            for (amount, expected) in [
                (dec!(0.000001), 1_i64),
                (dec!(12.345678), 12_345_678),
                (dec!(1152921504606.846975), 1_152_921_504_606_846_975),
            ] {
                let signed = signer
                    .margin(7, margin_wire_amount(amount).unwrap(), 41, 5, account)
                    .unwrap();
                let info: serde_json::Value = serde_json::from_str(&signed.info).unwrap();
                assert_eq!(info["USDCAmount"], expected, "{deployment:?}: {amount}");
                assert_eq!(info["Direction"], 1, "must add, never remove collateral");
            }
            // Exercise official Validate, independently of our Decimal preflight.
            for (market, amount) in [
                (7, 0),
                (7, MARGIN_MAX_WIRE + 1),
                (255, 1_000_000),
                (32_768, 1_000_000),
            ] {
                assert!(
                    signer.margin(market, amount, 42, 5, account).is_err(),
                    "{deployment:?}: invalid market {market} or amount {amount}"
                );
            }
            // Missing nonce must fail locally: HTTP fallback remains disabled.
            assert!(signer.margin(7, 1_000_000, -1, 5, account).is_err());
        }
        // The C ABI generates ExpiredAt from the clock; accounts also differ across chains.
        // Comparing these transactions' hashes/signatures would NOT prove chain isolation.
        // For that proof, deserialize each native txInfo with pinned lighter-go v1.0.10,
        // recompute Hash(chain) and Hash(other_chain), and verify Sig against both hashes.
        // This also checks the domain without conflating expiry/account differences.
    }
}
