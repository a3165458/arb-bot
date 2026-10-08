//! Arcus（`arcus.xyz`）永续的真实券商。
//!
//! # 鉴权
//!
//! Arcus 是非托管的：账户是一个以太坊地址（+ 子账户序号 0 ~ 9），交易用一把 **Ed25519**
//! API key 签名。这把 key 在网页 API Keys 页生成（「API Signing Key」，32 字节十六进制，
//! 只显示一次），由钱包的 EIP-712 签名注册到地址名下；服务端只存公钥。本券商需要：
//! 主地址、子账户序号、API Signing Key（私钥种子）。钱包私钥**不需要**，也不该给。
//!
//! 签名有两种（官方 Authentication 文档）：
//!
//! - **下单 / 撤单**：签名消息就是一段按键名排序、无空白的紧凑 JSON，用引擎整数
//!   （价格 = tick 数、数量 = 步长数）：
//!   `{"ad":"0x…","ai":N,"c":"…","ct":N,"g":N,"m":N,"op":1,"p":N,"q":N,"r":0,"s":N,"t":N,"v":1}`。
//!   `ct` 是纳秒时间戳，必须等于 `X-Timestamp`；地址是**唯一**转小写的字段。
//! - **改杠杆 / 补保证金**：`纳秒时间戳 + 动作名 + 规范 JSON(body)` 直接拼接后签名。
//!
//! # 下单是异步的
//!
//! `placeOrder` 通常回 `202 ACK`：请求已转给撮合引擎，但结果还没有。所以下单后一定按
//! `orderId` 回查到终态（IOC 在毫秒级结束）；传输失败拿不到 `orderId` 时，按客户订单号
//! 在订单历史里找。**刚受理的订单查 `404` 是「还看不到」，绝不是「没提交」。**
//!
//! # 幂等
//!
//! Arcus 的 `clientId` 只在**活跃**订单之间唯一，订单结束后同一个号可以再用 ——
//! 交易所挡不住重发。所以幂等完全靠订单意图日志：先落盘再发单，落过盘的号永不重发，
//! 查不到时报错而不是当成没提交。
//!
//! # 状态映射（2026-09-29 实测主网公开订单历史）
//!
//! IOC 一笔都没吃到时，状态是 `REJECTED` + `IOC_CANCELED`，**不是** `CANCELED`；部分成交
//! 才是 `CANCELED`。前者按「撤单、零成交」处理，不能当成被拒 —— 否则执行层会把「没吃到量」
//! 当成场所拒单。
//!
//! # 其它约定
//!
//! - 一律 LIMIT IOC；开仓前把该市场设成**逐仓 + 整数杠杆**并读回核对。改杠杆接口权重
//!   125（每 IP 每分钟 1,500），所以先读、不一致才改。
//! - `goodTilTime` 对 IOC 也必填，且必须在一个月之后（官方：重放保护），取 40 天。
//! - 价格按所在价格段的 tick 取整（买向下、卖向上），签名里的整数一律按顶层 `tickSize` 换算。
//! - 永续目前是白名单内测：不在名单里的地址查账户就回 `403 address not on access whitelist`。
//! - 开启下单时，连接阶段会做一次**签名自检**：按一个从没用过的客户订单号撤单。撤单和下单
//!   一样是异步受理的，但**签名在网关当场校验**（2026-09-30 主网实测）：签名不对回
//!   `401 invalid order signature`；签名对了回 `202`、状态 `CANCEL_ACKNOWLEDGED`（撤的结果
//!   稍后走 `orders` 频道）；早先的版本同步回 `ORDER_NOT_FOUND`。所以 2xx 或「查无此单」
//!   都说明签名被接受。这是本券商唯一一个在构造阶段发出的写请求，
//!   它不改变任何状态（没有订单可撤），只消耗撤单额度里的 1 个令牌。不做它的话，第一笔
//!   实盘订单就是第一次签名测试 —— 如果 Arcus 是第二条腿，第一条腿已经成交了。
//!
//! # 补保证金（`adjustIsolatedMargin`）
//!
//! `POST /v1/adjustIsolatedMargin?address=0x…`，正数 = 把全仓桶里的可用保证金划进这条逐仓腿
//! （<https://docs.arcus.xyz/api-reference/exchange/add-or-remove-margin-on-an-isolated-mode-position.md>）。
//! 正文 `{"accountIndex":N,"address":"0x…","amount":"<美元十进制字符串>","marketId":N}`，`amount`
//! 是**美元**（`"100"` = 100 美元，不是报价量子；官方明确不要发量子）。
//!
//! - **签名**：Ed25519 + 旧式消息 `纳秒时间戳 + "adjustIsolatedMargin" + 规范 JSON(body)`。
//!   Authentication 的 Scheme 2（「everything else」）规定按键名排序、无空白、无分隔符
//!   （<https://docs.arcus.xyz/api-reference/authentication.md>）。WebSocket 规范明确列出
//!   `adjustIsolatedMargin` 使用旧式签名，并说明它对应 REST 的同名端点
//!   （<https://docs.arcus.xyz/api-reference/asyncapi.yaml>）。
//!   2026-10-03 读取的官方网页客户端也直接佐证 REST：`ExchangeModule.adjustIsolatedMargin`
//!   调用 `/v1/adjustIsolatedMargin`，`HttpClient.signAndSend` 将规范正文交给
//!   `Ed25519Signer.signHeaders`，签 `timestamp + actionForHttpPath(path) + body`
//!   （<https://app.arcus.xyz/assets/index-BcyibAMy.js>）。它把地址放在查询参数；本券商按 REST
//!   文档允许的方式同时放在查询与正文，子账户序号始终在签名正文中。
//!   这是**官方文档 + 客户端静态证据**，不是服务端鉴权实测；离线 Ed25519 金标也不能证明实盘接受。
//! - **回复**：`200 APPLIED`（引擎已确认）/ `202 ACK`（已转给引擎但没等到确认，引擎仍可能执行）/
//!   `422 REJECTED` + `rejectReason`（`UNKNOWN_MARKET`、`INVALID_AMOUNT`、`NOT_ISOLATED`、
//!   `NO_OPEN_POSITION`、`UNDERCOLLATERALIZED`、`MISSING_MARK_PRICE`）；400 / 401 / 403 是网关
//!   在转给引擎之前的拒绝；429 带 `Retry-After`。
//! - **没有幂等键**：`amount` 是增量、请求里没有任何客户号，重发会补两次。所以一次调用最多发
//!   **一次**写请求、绝不内部重试；只有 `202`（已受理未确认）才读回 `GET /v1/positions` 的
//!   `marginUsed` 核对（持仓数量与入场价必须未变），核对不上报「结果不明」；
//!   传输失败 / 超时 / 5xx 直接报「结果不明」，由调用方核对，绝不重发。
//! - **限频**：这个接口的权重官方没有写（rate-limits 页只列了 `setLeverage` / `withdraw` /
//!   `transfer` 为 125）；按重接口对待，每次调用只发一次写、读回至多几次（权重 2）。
//! - **全仓桶**：钱来自这个子账户的全仓可用保证金；不够就是 `UNDERCOLLATERALIZED`（拒绝、没动钱）。
//!
//! 主地址与 API Signing Key 是机密：本类型不实现 `Debug`，也不把它们写进任何错误文本。

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use arb_venues::arcus::{ArcusMarket, BASE_URL, fetch_base_taker_fee, fetch_markets};
use async_trait::async_trait;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::{Client, StatusCode};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, FundingTotal, MarginOutcome, VenueLegState, VenuePosition};
use crate::live_common::{
    Cached, JournalEntry, LiveOptions, OrderJournal, Verified, hex_lower, order_units, round_price,
    transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const VENUE: Venue = Venue::Arcus;
/// 测试网基址。只用于诊断「这把 key 是不是在测试网生成的」，从不在测试网交易。
const TESTNET_URL: &str = "https://api.testnet.arcus.xyz";
/// 客户订单号：只能是 `[A-Za-z0-9_-]`，1 ~ 36 个字符。
const CLIENT_ID_PREFIX: &str = "arb-";
const MAX_CLIENT_ID: usize = 36;
/// 账户上不是本进程下的订单。
const EXTERNAL_PREFIX: &str = "arcus-external-";
/// `goodTilTime` 至少一个月之后；留出余量取 40 天。
const GOOD_TIL_DAYS: i64 = 40;
const MICROS_PER_DAY: i64 = 86_400_000_000;
/// 列表接口单页上限。满页说明可能被截断，不能据此下「没有」的结论。
const PAGE_LIMIT: usize = 1000;
/// 按 `orderId` 回查：每 200ms 一次，最多 10 秒（每次权重 2）。
const ORDER_POLL: Duration = Duration::from_millis(200);
const ORDER_POLLS: usize = 50;
/// 按客户订单号翻历史：每次权重 20+，所以慢一点、少几次。
const HISTORY_POLL: Duration = Duration::from_millis(1000);
const HISTORY_POLLS: usize = 6;
/// 改杠杆后读回。
const LEVERAGE_POLL: Duration = Duration::from_millis(250);
const LEVERAGE_POLLS: usize = 20;
/// 补保证金被「已受理」（202）之后读回持仓保证金：每 500ms 一次、最多 6 次（每次
/// `GET /v1/positions` 权重 2）。整个读回还受 [`MARGIN_READBACK_BUDGET`] 约束 —— 它持着下单锁，
/// 不能被一个卡住的读请求拖得更久。
const MARGIN_POLL: Duration = Duration::from_millis(500);
const MARGIN_POLLS: usize = 6;
const MARGIN_READBACK_BUDGET: Duration = Duration::from_secs(10);
/// 读回的保证金至少涨到「补前 + 金额 × 0.99」才算到账（留 1% 给读回之间的资金费 / 取整）。
const MARGIN_CONFIRM_RATIO: Decimal = Decimal::from_parts(99, 0, 0, false, 2);
const MARGIN_PATH: &str = "/v1/adjustIsolatedMargin";
/// 旧式签名消息里的动作名：路径的最后一段。
const MARGIN_ACTION: &str = "adjustIsolatedMargin";
/// 成交记录可能比订单状态晚一点出现：400ms 一次，最多约 5 秒（原来 1 秒一次、5 次）。
/// 它在下单之后，不影响成交价，但它是 Arcus 当第一腿时两腿之间的一段、也决定下完单要等多久才看到结果。
/// `/v1/fills` 每次权重 20、出口 IP 的预算是每分钟 1500，所以不再更密。
const FILLS_POLL: Duration = Duration::from_millis(400);
const FILLS_POLLS: usize = 12;
/// 市场列表（`/v1/markets`，约 0.3 秒一次）在这个时间内复用：下单前后各要读一次，
/// 开仓前的预热会强制刷新一遍，所以这一笔用的元数据至多几秒钟。
const MARKETS_TTL: Duration = Duration::from_secs(20);
/// 杠杆刚核对过多久之内，`place` 不再重新核对。
const LEVERAGE_VERIFIED_TTL: Duration = Duration::from_secs(20);
/// 查成交时把起点往前放，订单的 `createdAt`（网关时刻）可能比成交时刻还晚几毫秒。
const FILLS_LOOKBACK_US: i64 = 60_000_000;
/// API key 剩余有效期低于这个就提醒。
const KEY_EXPIRY_WARN_MS: i64 = 7 * 86_400_000;

/// 进程内共享、严格递增的纳秒时间戳（同时是签名里的防重放 nonce）。
static LAST_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

/// Arcus 凭据。机密：不实现 `Debug`、`Serialize`。
pub struct ArcusCredentials {
    /// 主以太坊地址（钱包地址），`0x` + 40 位十六进制。
    pub address: String,
    /// 子账户序号 0 ~ 9（网页生成 key 时选的「Subaccount #」）。
    pub account_index: String,
    /// 网页 API Keys 页给出的 API Signing Key：Ed25519 私钥种子，64 位十六进制。
    pub api_private_key: String,
}

pub struct ArcusBroker {
    client: Client,
    /// REST 基址。生产恒为 [`BASE_URL`]；做成字段只是为了让「写 + 读回」的整条流程（补保证金）能对
    /// 本机的脚本化服务器测试，不依赖网络。
    base_url: String,
    key: SigningKey,
    /// 公钥十六进制，即 `X-API-Key`。
    api_key: String,
    /// 小写 `0x` 地址。
    address: String,
    account_index: u8,
    options: LiveOptions,
    taker_fee: Decimal,
    clock_offset_ns: i64,
    journal: Mutex<OrderJournal>,
    /// 本进程见过的「客户订单号 → 交易所订单号」。有它就按订单号直查（权重 2），
    /// 否则只能翻订单历史（权重 20+）。只是加速，不参与任何判断；重启后为空。
    order_ids: Mutex<HashMap<String, String>>,
    /// 同一账户的下单与撤单串行。
    submit: Mutex<()>,
    /// 最近一次拉到的市场列表及时刻，见 [`MARKETS_TTL`]。
    markets_cache: Cached<Vec<ArcusMarket>>,
    /// 刚核对（或设置）并读回确认过的（市场, 杠杆）及时刻。
    verified_leverage: Verified<(u16, u32, crate::MarginMode)>,
}

/// HTTP 状态 + 已解析的 JSON 体（解析失败时是 `Null`）。
struct Reply {
    status: StatusCode,
    body: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderRow {
    order_id: String,
    #[serde(default)]
    client_id: Option<String>,
    market_id: u16,
    side: String,
    status: String,
    price: String,
    original_size: String,
    #[serde(default)]
    filled_size: Option<String>,
    remaining_size: String,
    #[serde(default)]
    reduce_only: Option<bool>,
    #[serde(default)]
    rejection_reason: Option<String>,
    #[serde(default)]
    created_at: Option<i64>,
    #[serde(default)]
    tpsl_type: Option<String>,
}

#[derive(Deserialize)]
struct OrdersPage {
    orders: Vec<OrderRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FillRow {
    order_id: String,
    size: String,
    price: String,
    fee: String,
}

#[derive(Deserialize)]
struct FillsPage {
    fills: Vec<FillRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionRow {
    market_id: u16,
    size: String,
    average_entry_price: String,
    position_value_notional: String,
    /// 这个仓位占用的保证金（逐仓：开仓时的加上后来补的）。老响应可能没有。
    #[serde(default)]
    margin_used: Option<String>,
    /// `ISOLATED` | `CROSS`。老响应可能没有 —— 没有就无法确认是逐仓，不补保证金。
    #[serde(default)]
    margin_mode: Option<String>,
}

#[derive(Deserialize)]
struct PositionsPage {
    positions: HashMap<String, PositionRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageRow {
    market_id: u16,
    leverage: i64,
    isolated: bool,
}

#[derive(Deserialize)]
struct LeveragesPage {
    leverages: Vec<LeverageRow>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiKeyRow {
    api_key: String,
    #[serde(default)]
    all_subaccounts: Option<bool>,
    #[serde(default)]
    account_index: Option<u8>,
    status: String,
    valid_until: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiKeysPage {
    api_keys: Vec<ApiKeyRow>,
}

/// 一个市场的下单规格。
struct Instrument {
    market: ArcusMarket,
    tick: Decimal,
    step: Decimal,
    min_size: Decimal,
    max_size: Decimal,
    min_notional: Decimal,
    /// `(上限（不含）, tick)`，最后一段没有上限。
    tiers: Vec<(Option<Decimal>, Decimal)>,
}

impl ArcusBroker {
    /// 校验凭据格式、打开意图日志、对时，然后用只读请求核对：API key 已注册在这个
    /// 地址与子账户名下且未过期、账户存在（已入金、在白名单里）。开启下单时再做一次
    /// 签名自检（见模块文档）。
    pub async fn connect(
        client: Client,
        credentials: ArcusCredentials,
        journal_path: &std::path::Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(VENUE)?;
        let address = normalize_address(&credentials.address)?;
        let account_index = parse_account_index(&credentials.account_index)?;
        let mut seed = decode_hex::<32>(&credentials.api_private_key)
            .map_err(|_| err("API Signing Key 应是 64 位十六进制"))?;
        let key = SigningKey::from_bytes(&seed);
        // 只留私钥十六进制的摘要，用来诊断「私钥栏填成了公钥」；私钥本身不保留。
        let seed_digest: [u8; 32] = Sha256::digest(hex_lower(&seed).as_bytes()).into();
        seed.fill(0);
        let api_key = hex_lower(key.verifying_key().as_bytes());

        let identity = format!(
            "arcus:{}",
            &hex_lower(&Sha256::digest(format!("{address}:{account_index}")))[..16]
        );
        let journal = OrderJournal::open(journal_path, &identity)?;

        let mut broker = Self {
            client,
            base_url: BASE_URL.to_string(),
            key,
            api_key,
            address,
            account_index,
            options,
            taker_fee: Decimal::ZERO,
            clock_offset_ns: 0,
            journal: Mutex::new(journal),
            order_ids: Mutex::new(HashMap::new()),
            submit: Mutex::new(()),
            markets_cache: Cached::new(),
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
        };
        broker.sync_clock().await?;
        broker.check_api_key(&seed_digest).await?;
        broker.check_account().await?;
        broker.taker_fee = fetch_base_taker_fee(&broker.client).await?;
        if broker.options.trading_enabled {
            broker.signing_self_test().await?;
        }
        Ok(broker)
    }

    fn authorize(&self) -> ArbResult<()> {
        self.options.authorize(VENUE)
    }

    fn account_query(&self) -> Vec<(&'static str, String)> {
        vec![
            ("address", self.address.clone()),
            ("accountIndex", self.account_index.to_string()),
        ]
    }

    // ───────────────────────── 传输 ─────────────────────────

    async fn get(&self, path: &str, query: &[(&str, String)]) -> ArbResult<Reply> {
        let response = self
            .client
            .get(format!("{}{path}", self.base_url))
            .query(query)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|error| transport_error(VENUE, path, error))?;
        read_reply(response).await
    }

    /// 读接口：必须 200，否则报错（带交易所的错误文本，不带请求参数）。
    async fn get_ok<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> ArbResult<T> {
        let reply = self.get(path, query).await?;
        if reply.status != StatusCode::OK {
            return Err(http_error(path, &reply));
        }
        decode(reply.body)
    }

    /// 签名写请求。`body` 是要发送的原始 JSON 文本。
    async fn post_signed(
        &self,
        path: &str,
        body: String,
        timestamp: i64,
        signature: &str,
    ) -> ArbResult<Reply> {
        let response = self
            .client
            .post(format!("{}{path}", self.base_url))
            .query(&[("address", self.address.as_str())])
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("X-API-Key", &self.api_key)
            .header("X-Timestamp", timestamp.to_string())
            .header("X-Signature", signature)
            .body(body)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|error| transport_error(VENUE, path, error))?;
        read_reply(response).await
    }

    fn sign(&self, message: &str) -> String {
        hex_lower(&self.key.sign(message.as_bytes()).to_bytes())
    }

    fn timestamp(&self) -> ArbResult<i64> {
        next_timestamp(self.clock_offset_ns)
    }

    // ───────────────────────── 连接检查 ─────────────────────────

    async fn sync_clock(&mut self) -> ArbResult<()> {
        let started = Instant::now();
        let before = now_ns()?;
        let time: Value = self.get_ok("/v1/time", &[]).await?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err("对时往返超过 2 秒，时钟偏差不可信"));
        }
        let server = time
            .get("timeNs")
            .and_then(Value::as_i64)
            .ok_or_else(|| err("服务器时间缺失"))?;
        let midpoint = before + (now_ns()? - before) / 2;
        self.clock_offset_ns = server
            .checked_sub(midpoint)
            .ok_or_else(|| err("服务器时间无效"))?;
        Ok(())
    }

    async fn check_api_key(&self, seed_digest: &[u8; 32]) -> ArbResult<()> {
        let page: ApiKeysPage = self
            .get_ok("/v1/apiKeys", &[("address", self.address.clone())])
            .await?;
        let Some(entry) = page
            .api_keys
            .iter()
            .find(|row| row.api_key.eq_ignore_ascii_case(&self.api_key))
        else {
            return Err(err(self.missing_key_reason(&page, seed_digest).await));
        };
        if entry.status != "ACTIVE" {
            return Err(err("API key 不是 ACTIVE 状态"));
        }
        let scoped =
            entry.all_subaccounts == Some(true) || entry.account_index == Some(self.account_index);
        if !scoped {
            return Err(err(format!(
                "API key 授权的子账户不是 {}（ARB_ARCUS_ACCOUNT_INDEX 填错了）",
                self.account_index
            )));
        }
        if entry.valid_until != 0 {
            let now_ms = (now_ns()? + self.clock_offset_ns) / 1_000_000;
            if entry.valid_until <= now_ms {
                return Err(err("API key 已过期，请在 Arcus 网页重新生成"));
            }
            if entry.valid_until - now_ms < KEY_EXPIRY_WARN_MS {
                tracing::warn!(venue = %VENUE, "API key 7 天内过期，请尽快在 Arcus 网页续期");
            }
        }
        Ok(())
    }

    /// key 不在该地址名下时，说清楚最可能是哪一种填错。只比较摘要与公开数据，不输出任何 key。
    async fn missing_key_reason(&self, page: &ApiKeysPage, seed_digest: &[u8; 32]) -> String {
        if pasted_public_key(page, seed_digest) {
            return "ARB_ARCUS_API_PRIVATE_KEY 填成了公钥（网页上的「API Key」）；应填生成 key 时只显示一次的「API Signing Key」".into();
        }
        if self.registered_on_testnet().await {
            return "这把 key 是在测试网（testnet.arcus.xyz）生成的；本程序连的是主网，请到 app.arcus.xyz/api-keys 重新生成".into();
        }
        if page.api_keys.is_empty() {
            return "该地址在 Arcus 主网上没有任何 API key：ARB_ARCUS_ADDRESS 应是生成 key 时连接的那个钱包地址".into();
        }
        format!(
            "该地址名下有 {} 把 API key，但没有这一把：确认 ARB_ARCUS_API_PRIVATE_KEY 是这个地址的「API Signing Key」，且 key 没被撤销",
            page.api_keys.len()
        )
    }

    /// 尽力而为：测试网查不到或出错都当成「不是」。
    async fn registered_on_testnet(&self) -> bool {
        let Ok(response) = self
            .client
            .get(format!("{TESTNET_URL}/v1/apiKeys"))
            .query(&[("address", self.address.as_str())])
            .timeout(Duration::from_secs(10))
            .send()
            .await
        else {
            return false;
        };
        match response.json::<ApiKeysPage>().await {
            Ok(page) => page
                .api_keys
                .iter()
                .any(|row| row.api_key.eq_ignore_ascii_case(&self.api_key)),
            Err(_) => false,
        }
    }

    async fn check_account(&self) -> ArbResult<()> {
        let reply = self.get("/v1/account", &self.account_query()).await?;
        match reply.status {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(err("账户还没有任何入金（Arcus 返回 404）")),
            StatusCode::FORBIDDEN => Err(err(format!(
                "地址被拒绝：{}（永续目前是白名单内测）",
                error_text(&reply.body)
            ))),
            _ => Err(http_error("/v1/account", &reply)),
        }
    }

    /// 按一个从没用过的客户订单号撤单：401 = 签名不对；`ORDER_NOT_FOUND` = 签名被接受。
    async fn signing_self_test(&self) -> ArbResult<()> {
        let (markets, _) = fetch_markets(&self.client).await?;
        let market = markets
            .iter()
            .find(|market| market.is_tradable())
            .ok_or_else(|| err("没有在线市场，无法做签名自检"))?;
        let timestamp = self.timestamp()?;
        let client_id = venue_client_id(
            "arb-selftest-",
            &ClientOrderId(format!("selftest-{timestamp}")),
            MAX_CLIENT_ID,
        );
        let message = cancel_message(&CancelPayload {
            address: &self.address,
            account_index: self.account_index,
            client_id: Some(&client_id),
            timestamp_ns: timestamp,
            order_id: None,
            market_id: market.market_id,
        });
        let body = json!({
            "accountIndex": self.account_index,
            "address": self.address,
            "clientId": client_id,
            "kind": "clientId",
            "marketId": market.market_id,
            "timestamp": timestamp,
        });
        let reply = self
            .post_signed(
                "/v1/cancelOrder",
                body.to_string(),
                timestamp,
                &self.sign(&message),
            )
            .await?;
        self_test_verdict(&reply)
    }

    // ───────────────────────── 市场与规格 ─────────────────────────

    /// 市场列表。`max_age` 之内拉过的直接复用，否则现拉并记下。
    async fn markets_within(&self, max_age: Duration) -> ArbResult<Vec<ArcusMarket>> {
        if let Some(markets) = self.markets_cache.get(max_age) {
            return Ok(markets);
        }
        let (markets, _) = fetch_markets(&self.client).await?;
        self.markets_cache.put(markets.clone());
        Ok(markets)
    }

    async fn market_map(&self) -> ArbResult<HashMap<u16, ArcusMarket>> {
        Ok(self
            .markets_within(MARKETS_TTL)
            .await?
            .into_iter()
            .map(|market| (market.market_id, market))
            .collect())
    }

    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Instrument> {
        let markets = self.markets_within(MARKETS_TTL).await?;
        let market = markets
            .into_iter()
            .find(|market| market.is_tradable() && market.symbol() == *symbol)
            .ok_or_else(|| err(format!("没有 {symbol} 对应的在线永续")))?;
        instrument_of(market)
    }

    /// 给定限价，或者刚拉的盘口 ± 价格保护。
    async fn order_price(&self, instrument: &Instrument, order: &NewOrder) -> ArbResult<Decimal> {
        if let Some(limit) = order.limit_price {
            return Ok(limit);
        }
        let started = Instant::now();
        let reply = self
            .get(
                &format!("/v1/l2OrderBook/{}", instrument.market.market_display_name),
                &[("nLevels", "5".to_string())],
            )
            .await?;
        if started.elapsed() > Duration::from_secs(2) {
            return Err(err("盘口往返超过 2 秒，价格已过时"));
        }
        if reply.status != StatusCode::OK {
            return Err(http_error("/v1/l2OrderBook", &reply));
        }
        if let Some(stamp_us) = reply.body.get("timestamp").and_then(Value::as_i64) {
            let now_us = (now_ns()? + self.clock_offset_ns) / 1000;
            if stamp_us > now_us + 1_000_000 || now_us - stamp_us > 5_000_000 {
                return Err(err("盘口时间戳过旧"));
            }
        }
        let top = |side: &str| -> ArbResult<Option<Decimal>> {
            match reply
                .body
                .get(side)
                .and_then(Value::as_array)
                .and_then(|levels| levels.first())
                .and_then(|level| level.get(0))
                .and_then(Value::as_str)
            {
                Some(raw) => dec(raw).map(Some),
                None => Ok(None),
            }
        };
        let (bid, ask) = (top("bids")?, top("asks")?);
        if bid.is_none() || ask.is_none() {
            return Err(err("盘口两侧必须都有挂单"));
        }
        self.options.bound_price(VENUE, order.side, bid, ask)
    }

    /// 读回这个市场的杠杆与保证金模式。
    async fn leverage_of(&self, market_id: u16) -> ArbResult<Option<LeverageRow>> {
        let mut query = self.account_query();
        query.push(("market", market_id.to_string()));
        let page: LeveragesPage = self.get_ok("/v1/leverages", &query).await?;
        Ok(page
            .leverages
            .into_iter()
            .find(|row| row.market_id == market_id))
    }

    /// 把市场设成逐仓 + 指定整数杠杆，并读回核对。只改这一个市场。
    async fn configure_open(
        &self,
        market_id: u16,
        leverage: u32,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        if self
            .verified_leverage
            .is_fresh(&(market_id, leverage, mode))
        {
            return Ok(());
        }
        let applied = |row: &Option<LeverageRow>| {
            row.as_ref().is_some_and(|row| {
                row.isolated == !mode.is_cross() && row.leverage == i64::from(leverage)
            })
        };
        if applied(&self.leverage_of(market_id).await?) {
            self.verified_leverage.mark((market_id, leverage, mode));
            return Ok(());
        }
        let body = json!({
            "accountIndex": self.account_index,
            "address": self.address,
            "isolated": !mode.is_cross(),
            "leverage": leverage,
            "marketId": market_id,
        });
        let canonical = canonical_json(&body);
        let timestamp = self.timestamp()?;
        let signature = self.sign(&legacy_message(timestamp, "setLeverage", &body));
        let reply = self
            .post_signed("/v1/setLeverage", canonical, timestamp, &signature)
            .await?;
        match reply.status {
            StatusCode::OK | StatusCode::ACCEPTED => {}
            StatusCode::UNPROCESSABLE_ENTITY => {
                let reason = reply
                    .body
                    .get("rejectReason")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let hint = match reason {
                    "HAS_OPEN_POSITION" => {
                        "（这个市场已有全仓仓位，切不了逐仓；先在网页平掉或改成逐仓）"
                    }
                    "UNDERCOLLATERALIZED" => "（保证金不足）",
                    "INVALID_LEVERAGE" => "（杠杆超出该市场范围）",
                    _ => "",
                };
                return Err(err(format!("交易所拒绝设置杠杆 {reason}{hint}；没有下单")));
            }
            _ => return Err(http_error("/v1/setLeverage", &reply)),
        }
        for _ in 0..LEVERAGE_POLLS {
            if applied(&self.leverage_of(market_id).await?) {
                self.verified_leverage.mark((market_id, leverage, mode));
                return Ok(());
            }
            tokio::time::sleep(LEVERAGE_POLL).await;
        }
        Err(err("逐仓杠杆设置未读回生效；没有下单"))
    }

    // ───────────────────────── 补保证金 ─────────────────────────

    /// 这个市场现在的逐仓保证金（`GET /v1/positions` 的 `marginUsed`，权重 2）。
    async fn isolated_margin(&self, market_id: u16) -> ArbResult<IsolatedMargin> {
        let page: PositionsPage = self.get_ok("/v1/positions", &self.account_query()).await?;
        isolated_margin_in(&page, market_id)
    }

    /// [`Broker::add_margin`] 的实现；`poll` 是 202 之后读回的间隔（生产用 [`MARGIN_POLL`]）。
    ///
    /// 顺序：只读模式 / 金额校验 → 取下单锁 → 读持仓（必须存在且是逐仓）→ 签名 → **一次** POST →
    /// 按回复分类（202 才读回）。POST 之前的任何失败都是 `Err`（没发请求）；POST 一旦发出，
    /// 就只会返回 `Ok(Applied | Refused | Unknown)`。
    async fn add_margin_polled(
        &self,
        symbol: &Symbol,
        amount: Decimal,
        poll: Duration,
    ) -> ArbResult<MarginOutcome> {
        // 只读模式在读任何东西、构造任何请求之前就拒绝：签名与发送都不可能发生。
        self.authorize()?;
        let amount_text = margin_amount_text(amount)?;
        // 与下单、撤单串行，并一直持到读回结束；回读仍核对入场规格，避免外部成交/强平冒充补仓。
        let _submission = self.submit.lock().await;

        let markets = self.market_map().await?;
        let page: PositionsPage = self.get_ok("/v1/positions", &self.account_query()).await?;
        let market_id = position_market(&page, &markets, symbol)?;
        let before = isolated_margin_in(&page, market_id)?;

        let timestamp = self.timestamp()?;
        let request = margin_request(
            &self.address,
            self.account_index,
            market_id,
            &amount_text,
            timestamp,
        );
        let signature = self.sign(&request.message);
        // 从这里起请求可能已经离开本机：传输失败 / 超时都只能是「结果不明」（可能已到账），
        // 绝不重发 —— 没有幂等键，重发会补两次。
        let reply = match self
            .post_signed(MARGIN_PATH, request.body, timestamp, &signature)
            .await
        {
            Ok(reply) => reply,
            Err(error) => {
                return Ok(MarginOutcome::Unknown(format!(
                    "{error}；请求可能已送达，钱可能已到账"
                )));
            }
        };
        Ok(match classify_margin_reply(&reply) {
            MarginReply::Applied => MarginOutcome::Applied,
            MarginReply::Refused(reason) => MarginOutcome::Refused(reason),
            MarginReply::Unknown(reason) => MarginOutcome::Unknown(reason),
            MarginReply::Acknowledged(note) => {
                self.confirm_margin(market_id, &before, amount, poll, &note)
                    .await
            }
        })
    }

    /// 回复只是「已受理」：读回保证金，涨够了才算到账，否则「结果不明」。读回本身失败也只是
    /// 继续读 / 最后报不明，不会改成别的结论。
    async fn confirm_margin(
        &self,
        market_id: u16,
        before: &IsolatedMargin,
        amount: Decimal,
        poll: Duration,
        note: &str,
    ) -> MarginOutcome {
        let deadline = tokio::time::Instant::now() + MARGIN_READBACK_BUDGET;
        let mut last = String::from("一次都没读到");
        for _ in 0..MARGIN_POLLS {
            tokio::time::sleep(poll).await;
            match tokio::time::timeout_at(deadline, self.isolated_margin(market_id)).await {
                Ok(Ok(after))
                    if after.size != before.size || after.entry_price != before.entry_price =>
                {
                    return MarginOutcome::Unknown(format!(
                        "{note}；读回期间持仓数量或入场价变化，保证金增加无法归因于这次补仓；不要重发"
                    ));
                }
                Ok(Ok(after)) if margin_confirmed(before.margin, amount, after.margin) => {
                    return MarginOutcome::Applied;
                }
                Ok(Ok(after)) => last = format!("最近一次读到 {}", after.margin),
                Ok(Err(error)) => last = format!("最近一次读回失败：{error}"),
                Err(_) => {
                    last = "读回超时".to_string();
                    break;
                }
            }
        }
        MarginOutcome::Unknown(format!(
            "{note}；读回期间保证金没有涨到 {} + {amount}×{MARGIN_CONFIRM_RATIO}（{last}）。\
             钱可能已到账、也可能稍后才生效，由调用方读 leg_state 核对，不要重发",
            before.margin
        ))
    }

    // ───────────────────────── 订单查询 ─────────────────────────

    /// 按交易所订单号查。`404` → `None`（刚受理时可能还看不到）。
    async fn order_by_id(&self, order_id: &str) -> ArbResult<Option<OrderRow>> {
        if !is_order_id(order_id) {
            return Err(err("交易所订单号格式不对"));
        }
        let reply = self
            .get(&format!("/v1/order/{order_id}"), &self.account_query())
            .await?;
        match reply.status {
            StatusCode::OK => decode(reply.body).map(Some),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(http_error("/v1/order", &reply)),
        }
    }

    /// 按客户订单号在这个市场的订单历史里找。满页还没找到 → 报错（可能被截断）。
    async fn order_by_client_id(
        &self,
        market_id: u16,
        client_id: &str,
    ) -> ArbResult<Option<OrderRow>> {
        let mut query = self.account_query();
        query.push(("market", market_id.to_string()));
        query.push(("limit", PAGE_LIMIT.to_string()));
        let page: OrdersPage = self.get_ok("/v1/orders", &query).await?;
        let found = page
            .orders
            .iter()
            .find(|row| row.client_id.as_deref() == Some(client_id))
            .cloned();
        if found.is_none() && page.orders.len() >= PAGE_LIMIT {
            return Err(err("订单历史满页仍没找到这个订单号，结果不可信"));
        }
        Ok(found)
    }

    /// 下单后等到终态：有 `orderId` 就按它查，否则翻历史。返回最后一次看到的行。
    async fn resolve(
        &self,
        market_id: u16,
        order_id: Option<&str>,
        client_id: &str,
    ) -> ArbResult<Option<OrderRow>> {
        let (polls, interval) = match order_id {
            Some(_) => (ORDER_POLLS, ORDER_POLL),
            None => (HISTORY_POLLS, HISTORY_POLL),
        };
        let mut last = None;
        for attempt in 0..polls {
            if attempt > 0 {
                tokio::time::sleep(interval).await;
            }
            let row = match order_id {
                Some(id) => self.order_by_id(id).await?,
                None => self.order_by_client_id(market_id, client_id).await?,
            };
            if let Some(row) = row {
                if row.client_id.as_deref() != Some(client_id) {
                    return Err(err("回查到的订单客户号与本地不符"));
                }
                let terminal = !map_status(&row)?.is_live();
                last = Some(row);
                if terminal {
                    break;
                }
            }
        }
        Ok(last)
    }

    /// 一笔订单的全部成交：数量、名义、手续费（正 = 成本）。与订单的已成交量不符时
    /// 等一会再查（成交记录可能晚到），最终不符就报错 —— 手续费不可信就不能记账。
    async fn fills_of(
        &self,
        row: &OrderRow,
        filled: Decimal,
    ) -> ArbResult<(Decimal, Decimal, Decimal)> {
        let from = row
            .created_at
            .ok_or_else(|| err("订单缺创建时刻，无法定位成交"))?
            .saturating_sub(FILLS_LOOKBACK_US);
        let mut query = self.account_query();
        query.push(("market", row.market_id.to_string()));
        query.push(("from", from.to_string()));
        query.push(("limit", PAGE_LIMIT.to_string()));
        for attempt in 0..FILLS_POLLS {
            if attempt > 0 {
                tokio::time::sleep(FILLS_POLL).await;
            }
            let page: FillsPage = self.get_ok("/v1/fills", &query).await?;
            if page.fills.len() >= PAGE_LIMIT {
                return Err(err("成交记录满页，可能被截断，手续费不可信"));
            }
            let totals = aggregate_fills(page.fills.iter().filter(|f| f.order_id == row.order_id))?;
            if totals.0 == filled {
                return Ok(totals);
            }
        }
        Err(err("成交记录与订单成交量不一致，手续费不可信"))
    }

    /// 交易所订单行 → 完整订单状态（含成交与手续费）。
    async fn state_from_row(
        &self,
        row: &OrderRow,
        markets: &HashMap<u16, ArcusMarket>,
    ) -> ArbResult<OrderState> {
        let market = markets
            .get(&row.market_id)
            .ok_or_else(|| err("订单所在市场不在市场列表里"))?;
        let symbol = market.symbol();
        let side = parse_side(&row.side)?;
        let original = dec(&row.original_size)?;
        let filled = filled_size(row)?;
        let price = dec(&row.price)?;
        let reduce_only = row.reduce_only.unwrap_or(false);
        let status = map_status(row)?;
        if original <= Decimal::ZERO || filled > original {
            return Err(err("订单数量非法"));
        }

        let entry = match row.client_id.as_deref() {
            Some(client_id) => self
                .journal
                .lock()
                .await
                .by_venue_client_id(client_id)
                .cloned(),
            None => None,
        };
        let order = match entry {
            Some(entry) => {
                if entry.instrument != row.market_id.to_string()
                    || entry.order.symbol != symbol
                    || entry.order.side != side
                    || entry.order.reduce_only != reduce_only
                    || entry.units != original
                {
                    return Err(err("交易所订单与本地意图不一致"));
                }
                entry.order
            }
            None => NewOrder {
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId(format!("{EXTERNAL_PREFIX}{}", row.order_id)),
                venue: VENUE,
                symbol,
                side,
                notional_usdt: multiply(original, price)?,
                quantity: Some(original),
                limit_price: Some(price),
                reduce_only,
                leverage: None,
            },
        };

        let (quantity, notional, fee) = if filled > Decimal::ZERO {
            self.fills_of(row, filled).await?
        } else {
            (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
        };
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(format!("{}:{}", row.market_id, row.order_id));
        state.status = status;
        state.filled_usdt = notional;
        state.average_price = (quantity > Decimal::ZERO)
            .then(|| divide(notional, quantity))
            .transpose()?;
        state.fee_usdt = fee;
        if status == OrderStatus::Rejected {
            state.reject_reason = Some(
                row.rejection_reason
                    .clone()
                    .unwrap_or_else(|| row.status.clone()),
            );
        }
        Ok(state)
    }

    /// 查一笔已落盘意图的当前状态；终态写回日志。查不到就报错 —— 发出去过的订单号
    /// 不能当成「从未提交」。
    async fn lookup_reserved(&self, entry: &JournalEntry) -> ArbResult<OrderState> {
        let market_id: u16 = entry
            .instrument
            .parse()
            .map_err(|_| err("日志里的市场号无效"))?;
        let known = self
            .order_ids
            .lock()
            .await
            .get(&entry.venue_client_id)
            .cloned();
        let row = self
            .resolve(market_id, known.as_deref(), &entry.venue_client_id)
            .await?
            .ok_or_else(|| err("已预约的订单号在交易所查不到；不能当作从未提交，请人工对账"))?;
        let state = self.state_from_row(&row, &self.market_map().await?).await?;
        if !state.status.is_live() {
            self.journal.lock().await.record_terminal(&state)?;
        }
        Ok(state)
    }
}

#[async_trait]
impl Broker for ArcusBroker {
    fn venue(&self) -> Venue {
        VENUE
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        if order.venue != VENUE || order.client_order_id.0.is_empty() {
            return Err(err("场所不匹配或客户订单号为空"));
        }

        // 幂等：落过盘的号永不重发，只查。
        let prior = self
            .journal
            .lock()
            .await
            .get(&order.client_order_id)
            .cloned();
        if let Some(entry) = prior {
            if entry.order != *order {
                return Err(err("client id reused with different intent/margin mode"));
            }
            if let Some(terminal) = &entry.terminal {
                return ack(terminal);
            }
            return ack(&self.lookup_reserved(&entry).await?);
        }

        let instrument = self.instrument(&order.symbol).await?;
        let market_id = instrument.market.market_id;
        if !order.reduce_only {
            let leverage = open_leverage(&instrument, order.leverage)?;
            self.configure_open(market_id, leverage, order.margin_mode)
                .await?;
        }
        let position = if order.reduce_only {
            Some(
                self.positions()
                    .await?
                    .into_iter()
                    .find(|p| p.symbol == order.symbol)
                    .ok_or_else(|| err("reduce-only 订单没有对应持仓"))?,
            )
        } else {
            None
        };

        let raw_price = self.order_price(&instrument, order).await?;
        let price = tiered_price(&instrument, raw_price, order.side)?;
        let units = order_units(
            VENUE,
            order,
            price,
            Decimal::ONE,
            instrument.step,
            instrument.min_size,
        )?;
        if units > instrument.max_size {
            return Err(err(format!(
                "数量超过该市场单笔上限 {}",
                instrument.max_size
            )));
        }
        if !order.reduce_only && multiply(units, price)? < instrument.min_notional {
            return Err(err(format!(
                "订单名义额低于交易所最小限额 {}",
                instrument.min_notional
            )));
        }
        if let Some(position) = position {
            let closes = (position.net_quantity > Decimal::ZERO && order.side == Side::Sell)
                || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
            if !closes || units > position.net_quantity.abs() {
                return Err(err("reduce-only 方向或数量超出持仓"));
            }
        }
        let price_ticks = exact_units(price, instrument.tick)?;
        let quantity_steps = exact_units(units, instrument.step)?;

        let client_id = venue_client_id(CLIENT_ID_PREFIX, &order.client_order_id, MAX_CLIENT_ID);
        // 先落盘，之后不再做任何网络读，直接签名发送。
        self.journal.lock().await.reserve(JournalEntry {
            order: order.clone(),
            venue_client_id: client_id.clone(),
            instrument: market_id.to_string(),
            units,
            terminal: None,
        })?;

        let timestamp = self.timestamp()?;
        let good_til_us = timestamp / 1000 + GOOD_TIL_DAYS * MICROS_PER_DAY;
        let message = place_message(&PlacePayload {
            address: &self.address,
            account_index: self.account_index,
            client_id: &client_id,
            timestamp_ns: timestamp,
            good_til_ns: good_til_us * 1000,
            market_id,
            price_ticks,
            quantity_steps,
            reduce_only: order.reduce_only,
            side: order.side,
        });
        let body = json!({
            "accountIndex": self.account_index,
            "address": self.address,
            "clientId": client_id,
            "goodTilTime": good_til_us.to_string(),
            "marketId": market_id,
            "orderSide": side_str(order.side),
            "orderType": "LIMIT",
            "price": price.normalize().to_string(),
            "quantity": units.normalize().to_string(),
            "reduceOnly": order.reduce_only,
            "timeInForce": "IOC",
            "timestamp": timestamp,
        });
        let outcome = self
            .post_signed(
                "/v1/placeOrder",
                body.to_string(),
                timestamp,
                &self.sign(&message),
            )
            .await;

        let definitive = match &outcome {
            Ok(reply) => definitive_refusal(reply.status),
            Err(_) => false,
        };
        let order_id = outcome
            .as_ref()
            .ok()
            .filter(|reply| reply.status.is_success())
            .and_then(|reply| reply.body.get("orderId").and_then(Value::as_str))
            .filter(|id| is_order_id(id))
            .map(str::to_string);
        if let Some(id) = &order_id {
            self.order_ids
                .lock()
                .await
                .insert(client_id.clone(), id.clone());
        }

        // 明确拒收（没进撮合）：再确认一次历史里没有，才记成拒单。
        if definitive {
            if let Some(row) = self.order_by_client_id(market_id, &client_id).await? {
                let state = self.state_from_row(&row, &self.market_map().await?).await?;
                if !state.status.is_live() {
                    self.journal.lock().await.record_terminal(&state)?;
                }
                return ack(&state);
            }
            let reply = outcome.as_ref().map_err(|_| err("内部错误"))?;
            let mut rejected = OrderState::new(order.clone());
            rejected.status = OrderStatus::Rejected;
            rejected.reject_reason = Some(format!(
                "HTTP {} {}",
                reply.status.as_u16(),
                error_text(&reply.body)
            ));
            self.journal.lock().await.record_terminal(&rejected)?;
            return Err(http_error("/v1/placeOrder", reply));
        }

        match self
            .resolve(market_id, order_id.as_deref(), &client_id)
            .await?
        {
            Some(row) => {
                let state = self.state_from_row(&row, &self.market_map().await?).await?;
                if !state.status.is_live() {
                    self.journal.lock().await.record_terminal(&state)?;
                }
                ack(&state)
            }
            None => match outcome {
                Err(error) => Err(error),
                Ok(_) => Err(err("订单提交后查不到；已保留订单号，不得重发，请人工对账")),
            },
        }
    }

    async fn warm_reads(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        let Some(leverage) = leverage else {
            return Ok(());
        };
        // 只读：市场列表（顺手进缓存）与这个市场当前的杠杆。要改的留给 `prepare_open`。
        let markets = self.markets_within(MARKETS_TTL).await?;
        let market = markets
            .into_iter()
            .find(|market| market.is_tradable() && market.symbol() == *symbol)
            .ok_or_else(|| err(format!("没有 {symbol} 对应的在线永续")))?;
        let instrument = instrument_of(market)?;
        let leverage = open_leverage(&instrument, Some(leverage))?;
        let market_id = instrument.market.market_id;
        if !self
            .verified_leverage
            .is_fresh(&(market_id, leverage, crate::MarginMode::Isolated))
        {
            let row = self.leverage_of(market_id).await?;
            if row
                .as_ref()
                .is_some_and(|row| row.isolated && row.leverage == i64::from(leverage))
            {
                self.verified_leverage
                    .mark((market_id, leverage, crate::MarginMode::Isolated));
            }
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
        let instrument = self.instrument(symbol).await?;
        let leverage = open_leverage(&instrument, leverage)?;
        self.configure_open(instrument.market.market_id, leverage, mode)
            .await
    }

    async fn prepare_open(&self, symbol: &Symbol, leverage: Option<Decimal>) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        // 缓存优先：预热过的话这里一次往返都没有。
        let instrument = self.instrument(symbol).await?;
        let leverage = open_leverage(&instrument, leverage)?;
        self.configure_open(
            instrument.market.market_id,
            leverage,
            crate::MarginMode::Isolated,
        )
        .await
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let entry = self.journal.lock().await.get(client_order_id).cloned();
        match entry {
            Some(entry) => match &entry.terminal {
                Some(terminal) => Ok(Some(terminal.clone())),
                None => self.lookup_reserved(&entry).await.map(Some),
            },
            None if client_order_id.0.starts_with(EXTERNAL_PREFIX) => {
                Err(err("外部订单只能按交易所订单号查询"))
            }
            None => Ok(None),
        }
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.authorize()?;
        let _submission = self.submit.lock().await;
        let (market_id, order_id) = venue_order_id
            .split_once(':')
            .ok_or_else(|| err("交易所订单号应是「市场:订单」"))?;
        let market_id: u16 = market_id.parse().map_err(|_| err("市场号无效"))?;
        let Some(row) = self.order_by_id(order_id).await? else {
            return Err(err("要撤的订单查不到，无法确认状态"));
        };
        if !map_status(&row)?.is_live() {
            return Ok(());
        }
        let timestamp = self.timestamp()?;
        let message = cancel_message(&CancelPayload {
            address: &self.address,
            account_index: self.account_index,
            client_id: None,
            timestamp_ns: timestamp,
            order_id: Some(order_id),
            market_id,
        });
        let body = json!({
            "accountIndex": self.account_index,
            "address": self.address,
            "kind": "orderId",
            "marketId": market_id,
            "orderId": order_id,
            "timestamp": timestamp,
        });
        let reply = self
            .post_signed(
                "/v1/cancelOrder",
                body.to_string(),
                timestamp,
                &self.sign(&message),
            )
            .await?;
        if matches!(
            reply.status,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
        ) || reply.status.is_server_error()
        {
            return Err(http_error("/v1/cancelOrder", &reply));
        }
        // 撤单同样是异步的；以订单的实际状态为准。
        for _ in 0..ORDER_POLLS {
            if let Some(row) = self.order_by_id(order_id).await?
                && !map_status(&row)?.is_live()
            {
                return Ok(());
            }
            tokio::time::sleep(ORDER_POLL).await;
        }
        Err(err("撤单后订单仍然活跃"))
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let mut query = self.account_query();
        query.push(("limit", PAGE_LIMIT.to_string()));
        let page: OrdersPage = self.get_ok("/v1/openOrders", &query).await?;
        if page.orders.len() >= PAGE_LIMIT {
            return Err(err("挂单满页，可能被截断"));
        }
        let markets = self.market_map().await?;
        let mut states = Vec::with_capacity(page.orders.len());
        for row in &page.orders {
            let state = self.state_from_row(row, &markets).await?;
            if state.status.is_live() {
                states.push(state);
            }
        }
        Ok(states)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let page: PositionsPage = self.get_ok("/v1/positions", &self.account_query()).await?;
        let markets = self.market_map().await?;
        positions_from(page, &markets)
    }

    /// `GET /v1/positions` 里这个市场的 `marginUsed`（逐仓保证金，含补进去的）。Arcus 不给强平价，
    /// 强平价由调用方按这个保证金和维持保证金率算。
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<VenueLegState>> {
        let page: PositionsPage = self.get_ok("/v1/positions", &self.account_query()).await?;
        let markets = self.market_map().await?;
        let Some(market_id) = markets
            .values()
            .find(|market| market.symbol() == *symbol)
            .map(|market| market.market_id)
        else {
            return Ok(None);
        };
        let Some(row) = page
            .positions
            .values()
            .find(|row| row.market_id == market_id)
        else {
            return Ok(None);
        };
        leg_state_from_row(row).map(Some)
    }

    /// `GET /v1/account` 的 `freeCollateral`：能拿来开新仓的保证金。
    async fn free_collateral(&self) -> ArbResult<Option<Decimal>> {
        let account: AccountSummary = self.get_ok("/v1/account", &self.account_query()).await?;
        account.free_collateral.as_deref().map(dec).transpose()
    }

    fn supports_add_margin(&self) -> bool {
        true
    }

    /// `POST /v1/adjustIsolatedMargin`（见模块文档「补保证金」）：把 `amount_usdt` 美元从这个子账户的
    /// 全仓桶划进这个合约已有的**逐仓**持仓。
    ///
    /// - `Err`（没发请求）：只读模式、金额非正或不是整分、这个合约没有持仓、不是逐仓、读不到当前保证金。
    /// - `Applied`：`200 APPLIED`（引擎确认），或 `202 ACK` 之后读回的保证金涨够了
    ///   （≥ 补前 + 金额 × 0.99，且数量、入场价未变）。
    /// - `Refused`：`422 REJECTED` + `rejectReason`（全仓可用保证金不够、没有逐仓持仓……）、
    ///   网关的 400 / 401 / 403、429 —— 都在引擎处理之前或被引擎明确拒绝，没动钱。
    ///   401 可能是 API key、时间戳或签名问题，不代表旧式签名方案本身有误。
    /// - `Unknown`：传输失败 / 超时 / 5xx / 其它状态码、422 生命周期冲突或缺失，或读回不到足额保证金。
    ///
    /// 一次调用至多一个 POST、不重试；钱来自全仓桶，所以全仓可用保证金不够时是 `Refused`。
    async fn add_margin(&self, symbol: &Symbol, amount_usdt: Decimal) -> ArbResult<MarginOutcome> {
        self.add_margin_polled(symbol, amount_usdt, MARGIN_POLL)
            .await
    }

    /// `GET /v1/fills`（公开）：最新在前，`from` / `to` 是微秒；满页就用最旧那条的时间往前翻。
    async fn fills_between(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
        until: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<Vec<crate::settlement::VenueFill>>> {
        let markets = self.market_map().await?;
        let market_id = markets
            .values()
            .find(|market| market.symbol() == *symbol)
            .map(|market| market.market_id)
            .ok_or_else(|| err(format!("没有 {symbol} 对应的市场")))?;
        let mut fills = Vec::new();
        let mut to = until.timestamp_micros();
        for _ in 0..FILLS_PAGES {
            let mut query = self.account_query();
            query.push(("market", market_id.to_string()));
            query.push(("from", since.timestamp_micros().to_string()));
            query.push(("to", to.to_string()));
            query.push(("limit", FILLS_PAGE.to_string()));
            let page: AccountFillsPage = self.get_ok("/v1/fills", &query).await?;
            let full = page.fills.len() >= FILLS_PAGE;
            let oldest = page.fills.iter().map(|row| row.created_at).min();
            for row in &page.fills {
                fills.push(fill_from_row(row)?);
            }
            match oldest {
                Some(oldest) if full => to = oldest - 1,
                _ => return Ok(Some(fills)),
            }
        }
        Err(err("成交记录超过翻页上限，不完整的成交算不出盈亏"))
    }

    /// `GET /v1/funding`（公开、按地址与子账户）：最新在前，`from` 是微秒；一页最多
    /// 1000 条，满页就用最旧那条的时间往前翻。`payment` 正 = 收到。
    async fn funding_since(
        &self,
        symbol: &Symbol,
        since: chrono::DateTime<chrono::Utc>,
    ) -> ArbResult<Option<FundingTotal>> {
        let markets = self.market_map().await?;
        let market_id = markets
            .values()
            .find(|market| market.symbol() == *symbol)
            .map(|market| market.market_id)
            .ok_or_else(|| err(format!("没有 {symbol} 对应的市场")))?;
        let mut rows = Vec::new();
        let mut to: Option<i64> = None;
        for _ in 0..FUNDING_PAGES {
            let mut query = self.account_query();
            query.push(("market", market_id.to_string()));
            query.push(("from", since.timestamp_micros().to_string()));
            query.push(("limit", FUNDING_PAGE.to_string()));
            if let Some(to) = to {
                query.push(("to", to.to_string()));
            }
            let page: FundingPage = self.get_ok("/v1/funding", &query).await?;
            let full = page.funding_payments.len() >= FUNDING_PAGE;
            let oldest = page.funding_payments.iter().map(|row| row.time).min();
            for row in page.funding_payments {
                let at = chrono::DateTime::from_timestamp_micros(row.time)
                    .ok_or_else(|| err("资金费流水的时间无效"))?;
                rows.push((at, dec(&row.payment)?));
            }
            match oldest {
                Some(oldest) if full => to = Some(oldest - 1),
                _ => return Ok(Some(FundingTotal::from_rows(rows, since))),
            }
        }
        Err(err("资金费流水超过翻页上限，合计不完整"))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountSummary {
    #[serde(default)]
    free_collateral: Option<String>,
}

fn leg_state_from_row(row: &PositionRow) -> ArbResult<VenueLegState> {
    let mode = row
        .margin_mode
        .as_deref()
        .and_then(crate::margin::reported_mode);
    Ok(VenueLegState {
        margin_mode: mode,
        // 0 表示没有分配保证金，不是「保证金为 0」：按没有处理。
        margin_usdt: if mode == Some(crate::MarginMode::Isolated) {
            row.margin_used
                .as_deref()
                .map(dec)
                .transpose()?
                .filter(|margin| *margin > Decimal::ZERO)
        } else {
            None
        },
        liquidation_price: None,
    })
}

/// 补保证金金额的请求文本：正数、已取整到分（至多两位小数；调用方负责取整，这里不改它），
/// 十进制字符串。官方：`amount` 是美元（`"100"` = 100 美元），不是报价量子；零回 400。
fn margin_amount_text(amount: Decimal) -> ArbResult<String> {
    if amount <= Decimal::ZERO {
        return Err(err("补保证金的金额必须为正"));
    }
    let amount = amount.normalize();
    if amount.scale() > 2 {
        return Err(err("补保证金的金额必须已取整到分（至多两位小数）"));
    }
    Ok(amount.to_string())
}

/// 补保证金的请求：要发送的正文（规范 JSON）与待签的消息。
struct MarginRequest {
    body: String,
    message: String,
}

/// 正文 `{accountIndex, address, amount, marketId}`；签名消息 = `纳秒时间戳 + 动作名 + 规范 JSON(body)`，
/// 发送的正文就是签过的那份规范 JSON（逐字节一致）。
fn margin_request(
    address: &str,
    account_index: u8,
    market_id: u16,
    amount_text: &str,
    timestamp_ns: i64,
) -> MarginRequest {
    let body = json!({
        "accountIndex": account_index,
        "address": address,
        "amount": amount_text,
        "marketId": market_id,
    });
    MarginRequest {
        message: legacy_message(timestamp_ns, MARGIN_ACTION, &body),
        body: canonical_json(&body),
    }
}

/// 一次 `adjustIsolatedMargin` 回复的含义。
#[derive(Debug, PartialEq, Eq)]
enum MarginReply {
    /// `200 APPLIED`：引擎已确认。
    Applied,
    /// 只是受理（`202 ACK`；或 200 但没有 `status: APPLIED`）：引擎可能稍后才执行，要读回核对。
    Acknowledged(String),
    /// 明确拒绝，没动钱。
    Refused(String),
    /// 结果不明：钱可能已到账。
    Unknown(String),
}

/// 回复怎么分类（官方 OpenAPI `adjustIsolatedMargin`）：
/// 200 = 引擎确认；202 = 转给引擎但没等到确认；422 必须同时有 `REJECTED` 与 `rejectReason`
/// 才是明确拒绝，冲突 / 缺失的生命周期不能证明没动钱。400 / 401 / 403 = 网关校验失败；
/// 429 = 限频（网关在处理之前拒绝）。5xx 与其它状态码一律「不明」—— 即使是 `Transmission`。
fn classify_margin_reply(reply: &Reply) -> MarginReply {
    let detail = error_text(&reply.body);
    match reply.status {
        StatusCode::OK if reply.body.get("status").and_then(Value::as_str) == Some("APPLIED") => {
            MarginReply::Applied
        }
        StatusCode::OK => MarginReply::Acknowledged("HTTP 200 但响应里没有 status=APPLIED".into()),
        StatusCode::ACCEPTED => {
            MarginReply::Acknowledged("HTTP 202 ACK：交易所已受理，引擎尚未确认".into())
        }
        StatusCode::UNPROCESSABLE_ENTITY
            if reply.body.get("status").and_then(Value::as_str) == Some("REJECTED") =>
        {
            match reply
                .body
                .get("rejectReason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.is_empty())
            {
                Some(reason) => {
                    let reason: String = reason.chars().take(64).collect();
                    MarginReply::Refused(format!(
                        "交易所拒绝补保证金：{reason}{}；没有动钱",
                        margin_reject_hint(&reason)
                    ))
                }
                None => MarginReply::Unknown(format!(
                    "HTTP 422 但没有 rejectReason（{detail}）；无法确认是否已处理"
                )),
            }
        }
        StatusCode::TOO_MANY_REQUESTS => {
            let wait = reply
                .body
                .get("retryAfterMs")
                .and_then(Value::as_u64)
                .map(|ms| format!("，{ms}ms 后可再试"))
                .unwrap_or_default();
            MarginReply::Refused(format!(
                "限频（HTTP 429{wait}）：网关在处理之前拒绝了请求，没有动钱、没有重发"
            ))
        }
        StatusCode::BAD_REQUEST => {
            MarginReply::Refused(format!("网关拒绝了请求（HTTP 400：{detail}）；没有动钱"))
        }
        StatusCode::UNAUTHORIZED => MarginReply::Refused(format!(
            "交易所鉴权失败（HTTP 401：{detail}）。请核对 API key 授权、纳秒时间戳与时钟偏差、\
             以及旧式消息签名；没有动钱、没有重发"
        )),
        StatusCode::FORBIDDEN => MarginReply::Refused(format!(
            "交易所拒绝了请求（HTTP 403：{detail}）：地址或子账户与 API key 不符；没有动钱"
        )),
        other => MarginReply::Unknown(format!(
            "HTTP {}：{detail}；请求可能已被处理，钱可能已到账",
            other.as_u16()
        )),
    }
}

/// 官方 `rejectReason` 的说明（未知的原因不加说明）。
fn margin_reject_hint(reason: &str) -> &'static str {
    match reason {
        "UNDERCOLLATERALIZED" => "（全仓可用保证金不够，或这笔会让账户更糟）",
        "NOT_ISOLATED" => "（这个合约不是逐仓）",
        "NO_OPEN_POSITION" => "（这个合约没有持仓）",
        "INVALID_AMOUNT" => "（金额无效）",
        "UNKNOWN_MARKET" => "（市场不存在）",
        "MISSING_MARK_PRICE" => "（市场暂时没有标记价，稍后可再试）",
        _ => "",
    }
}

/// 持仓页里这个标的**有仓位**的市场号。恰好一个才行：没有 / 多个都不能补。
/// 按持仓行反查市场（而不是按标的找第一个市场）—— 离线的旧市场可能与在线的同名。
fn position_market(
    page: &PositionsPage,
    markets: &HashMap<u16, ArcusMarket>,
    symbol: &Symbol,
) -> ArbResult<u16> {
    let mut found = Vec::new();
    for row in page.positions.values() {
        if markets
            .get(&row.market_id)
            .is_some_and(|market| market.symbol() == *symbol)
            && !dec(&row.size)?.is_zero()
        {
            found.push(row.market_id);
        }
    }
    match found.as_slice() {
        [market_id] => Ok(*market_id),
        [] => Err(err(format!("{symbol} 在 Arcus 上没有持仓，补不了保证金"))),
        _ => Err(err(format!(
            "{symbol} 对应多个有持仓的市场，无法确定补哪一个"
        ))),
    }
}

/// 回读基准：保证金必须来自同一条腿，不能把加仓带来的保证金误当成补仓已生效。
#[derive(Debug)]
struct IsolatedMargin {
    margin: Decimal,
    size: Decimal,
    entry_price: Decimal,
}

/// 这个市场的逐仓保证金。必须有仓位、`marginMode` 明确是 `ISOLATED`、`marginUsed` 读得到
/// （没有「补前」的数就无法核对补进去的钱）。
fn isolated_margin_in(page: &PositionsPage, market_id: u16) -> ArbResult<IsolatedMargin> {
    let mut rows = page
        .positions
        .values()
        .filter(|row| row.market_id == market_id);
    let row = rows.next().ok_or_else(|| err("这个合约没有持仓"))?;
    if rows.next().is_some() {
        return Err(err("这个合约有多条持仓，无法确定逐仓保证金"));
    }
    let size = dec(&row.size)?;
    if size.is_zero() {
        return Err(err("这个合约没有持仓"));
    }
    match row.margin_mode.as_deref() {
        Some(mode) if mode.eq_ignore_ascii_case("ISOLATED") => {}
        Some(mode) => {
            return Err(err(format!(
                "这个合约的保证金模式是 {}，不是 ISOLATED：全仓持仓没有可补的逐仓保证金",
                mode.chars().take(16).collect::<String>()
            )));
        }
        None => return Err(err("持仓没有给出保证金模式，无法确认是逐仓")),
    }
    let margin = leg_state_from_row(row)?.margin_usdt.ok_or_else(|| {
        err("读不到这条腿的逐仓保证金（marginUsed 缺失或为 0），无法核对补进去的钱")
    })?;
    let entry_price = dec(&row.average_entry_price)?;
    if entry_price <= Decimal::ZERO {
        return Err(err("逐仓持仓的入场价无效，无法核对补进去的钱"));
    }
    Ok(IsolatedMargin {
        margin,
        size,
        entry_price,
    })
}

/// 读回的保证金是否涨够：`after ≥ before + amount × 0.99`。算术溢出按「没涨够」。
fn margin_confirmed(before: Decimal, amount: Decimal, after: Decimal) -> bool {
    amount
        .checked_mul(MARGIN_CONFIRM_RATIO)
        .and_then(|part| before.checked_add(part))
        .is_some_and(|floor| after >= floor)
}

/// 成交记录一页多少条（接口上限）与最多翻几页。
const FILLS_PAGE: usize = 1000;
const FILLS_PAGES: usize = 10;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountFillsPage {
    #[serde(default)]
    fills: Vec<AccountFillRow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountFillRow {
    /// `BUY` / `SELL`。
    side: String,
    size: String,
    price: String,
    fee: String,
    /// `OPEN_LONG` / `CLOSE_SHORT` …（旧记录可能没有）。
    #[serde(default)]
    position_effect: Option<String>,
    /// 微秒。
    created_at: i64,
}

fn fill_from_row(row: &AccountFillRow) -> ArbResult<crate::settlement::VenueFill> {
    use crate::settlement::{FillEffect, VenueFill};
    let side = match row.side.as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => return Err(err(format!("成交记录里有认不出的方向 {other}"))),
    };
    let effect = match row.position_effect.as_deref() {
        Some(effect) if effect.starts_with("OPEN_") => FillEffect::Open,
        Some(effect) if effect.starts_with("CLOSE_") => FillEffect::Close,
        _ => FillEffect::Unknown,
    };
    Ok(VenueFill {
        at: chrono::DateTime::from_timestamp_micros(row.created_at)
            .ok_or_else(|| err("成交记录的时间无效"))?,
        side,
        quantity: dec(&row.size)?,
        price: dec(&row.price)?,
        fee_usdt: dec(&row.fee)?,
        effect,
    })
}

/// 资金费流水一页多少条（接口上限）与最多翻几页（按小时结算，约 1000 × 10 小时）。
const FUNDING_PAGE: usize = 1000;
const FUNDING_PAGES: usize = 10;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingPage {
    #[serde(default)]
    funding_payments: Vec<FundingRow>,
}

#[derive(Debug, Deserialize)]
struct FundingRow {
    /// 正 = 收到，负 = 付出。
    payment: String,
    /// 微秒。
    time: i64,
}

// ───────────────────────── 纯函数（可单测） ─────────────────────────

struct PlacePayload<'a> {
    address: &'a str,
    account_index: u8,
    client_id: &'a str,
    timestamp_ns: i64,
    good_til_ns: i64,
    market_id: u16,
    price_ticks: u64,
    quantity_steps: u64,
    reduce_only: bool,
    side: Side,
}

/// 下单签名消息（op 1，IOC）。键名固定按字母序、无空白；地址小写。
fn place_message(p: &PlacePayload<'_>) -> String {
    format!(
        r#"{{"ad":"{}","ai":{},"c":"{}","ct":{},"g":{},"m":{},"op":1,"p":{},"q":{},"r":{},"s":{},"t":2,"v":1}}"#,
        p.address.to_ascii_lowercase(),
        p.account_index,
        p.client_id,
        p.timestamp_ns,
        p.good_til_ns,
        p.market_id,
        p.price_ticks,
        p.quantity_steps,
        u8::from(p.reduce_only),
        match p.side {
            Side::Buy => 0,
            Side::Sell => 1,
        },
    )
}

struct CancelPayload<'a> {
    address: &'a str,
    account_index: u8,
    client_id: Option<&'a str>,
    timestamp_ns: i64,
    order_id: Option<&'a str>,
    market_id: u16,
}

/// 撤单签名消息（op 2）。按订单号撤时没有 `c`，按客户号撤时没有 `id`。
fn cancel_message(p: &CancelPayload<'_>) -> String {
    let mut out = format!(
        r#"{{"ad":"{}","ai":{}"#,
        p.address.to_ascii_lowercase(),
        p.account_index
    );
    if let Some(client_id) = p.client_id {
        out.push_str(&format!(r#","c":"{client_id}""#));
    }
    out.push_str(&format!(r#","ct":{}"#, p.timestamp_ns));
    if let Some(order_id) = p.order_id {
        out.push_str(&format!(r#","id":"{order_id}""#));
    }
    out.push_str(&format!(r#","m":{},"op":2,"v":1}}"#, p.market_id));
    out
}

/// 签名自检（撤一个不存在的订单）的回复怎么判。见模块文档「其它约定」。
fn self_test_verdict(reply: &Reply) -> ArbResult<()> {
    match reply.status {
        StatusCode::UNAUTHORIZED => Err(err(format!(
            "签名自检失败：交易所拒绝了签名（401 {}）。检查 API Signing Key 是否与地址匹配",
            error_text(&reply.body)
        ))),
        StatusCode::FORBIDDEN => Err(err(format!(
            "签名自检失败：{}（地址或子账户与 API key 不符）",
            error_text(&reply.body)
        ))),
        // 网关当场验签，验过了才受理（202）或直接给出结论（200）。
        StatusCode::OK | StatusCode::ACCEPTED => Ok(()),
        _ if mentions_not_found(&reply.body) => Ok(()),
        _ => Err(err(format!(
            "签名自检结果无法判断（HTTP {}：{}）；为安全起见不开启下单",
            reply.status.as_u16(),
            error_text(&reply.body)
        ))),
    }
}

/// 旧式签名消息：`时间戳 + 动作 + 规范 JSON(body)`，无分隔符。
fn legacy_message(timestamp_ns: i64, action: &str, body: &Value) -> String {
    format!("{timestamp_ns}{action}{}", canonical_json(body))
}

/// 规范 JSON：递归按键名排序、无空白。不依赖 serde_json 的 map 实现（开了
/// `preserve_order` 特性时它会保留插入顺序，签名就对不上了）。
fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<&String, &Value> = map.iter().collect();
            let fields: Vec<String> = sorted
                .into_iter()
                .map(|(key, value)| {
                    format!("{}:{}", Value::String(key.clone()), canonical_json(value))
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}

/// 开仓订单的杠杆：必须给出、是不小于 1 的整数、不超过该市场当前上限。
fn open_leverage(instrument: &Instrument, leverage: Option<Decimal>) -> ArbResult<u32> {
    let leverage = leverage.ok_or_else(|| err("开仓订单必须给出逐仓杠杆"))?;
    if !leverage.fract().is_zero() || leverage < Decimal::ONE {
        return Err(err("杠杆必须是不小于 1 的整数"));
    }
    let max = instrument
        .market
        .max_leverage()
        .ok_or_else(|| err("市场没有给出保证金率"))?
        .floor();
    if leverage > max {
        return Err(err(format!("杠杆 {leverage} 超过该市场当前上限 {max}")));
    }
    leverage.to_u32().ok_or_else(|| err("杠杆无效"))
}

fn instrument_of(market: ArcusMarket) -> ArbResult<Instrument> {
    let tick = dec(&market.tick_size)?;
    let step = dec(&market.step_size)?;
    let min_size = dec(&market.min_order_size)?;
    let max_size = dec(&market.max_order_size)?;
    let min_notional = dec(&market.min_order_notional)?;
    if tick <= Decimal::ZERO
        || step <= Decimal::ZERO
        || min_size < Decimal::ZERO
        || max_size <= Decimal::ZERO
    {
        return Err(err(format!(
            "{} 的下单规格非法",
            market.market_display_name
        )));
    }
    let mut tiers = Vec::with_capacity(market.tick_tiers.len());
    for tier in &market.tick_tiers {
        let tier_tick = dec(&tier.tick)?;
        // 每一段的 tick 都必须是顶层 tick 的整数倍，否则签名用的整数换算不成立。
        if tier_tick <= Decimal::ZERO || !(tier_tick % tick).is_zero() {
            return Err(err("价格分段的 tick 不是顶层 tick 的整数倍"));
        }
        tiers.push((tier.up_to_price.as_deref().map(dec).transpose()?, tier_tick));
    }
    Ok(Instrument {
        market,
        tick,
        step,
        min_size,
        max_size,
        min_notional,
        tiers,
    })
}

/// 这个价格所在价格段的 tick；没有分段时用顶层 tick。
fn tier_tick(instrument: &Instrument, price: Decimal) -> Decimal {
    for (limit, tick) in &instrument.tiers {
        match limit {
            Some(limit) if price >= *limit => continue,
            _ => return *tick,
        }
    }
    instrument.tick
}

/// 按所在价格段的 tick 取整，不让价格变差；取整后若跨进更粗的段，再按新段取一次。
fn tiered_price(instrument: &Instrument, raw: Decimal, side: Side) -> ArbResult<Decimal> {
    let mut price = raw;
    for _ in 0..3 {
        let tick = tier_tick(instrument, price);
        let rounded = round_price(price, tick, side).ok_or_else(|| err("价格取整后非正"))?;
        if (rounded % tier_tick(instrument, rounded)).is_zero() {
            return Ok(rounded);
        }
        price = rounded;
    }
    Err(err("价格无法落在任何价格段的 tick 上"))
}

/// 十进制 → 引擎整数（tick 数 / 步长数）。必须整除，有余数就是换算错了，拒绝签名。
fn exact_units(value: Decimal, unit: Decimal) -> ArbResult<u64> {
    if unit <= Decimal::ZERO || value <= Decimal::ZERO {
        return Err(err("价格或数量非正"));
    }
    let ratio = divide(value, unit)?;
    if !ratio.fract().is_zero() {
        return Err(err("价格或数量不是 tick / 步长的整数倍"));
    }
    ratio.to_u64().ok_or_else(|| err("价格或数量超出范围"))
}

/// 交易所订单状态 → 本仓库状态。未知状态、强平 / ADL / 止盈止损单一律报错，不猜终态。
fn map_status(row: &OrderRow) -> ArbResult<OrderStatus> {
    if row.tpsl_type.is_some() {
        return Err(err("账户上有止盈止损单，本券商不处理，请人工核对"));
    }
    let filled = filled_size(row)?;
    let original = dec(&row.original_size)?;
    let status = match row.status.as_str() {
        "ACK" | "PENDING" => OrderStatus::Pending,
        "OPEN" | "PARTIALLY_FILLED" | "CANCEL_ACKNOWLEDGED" | "CANCEL_PENDING" => OrderStatus::Open,
        "FILLED" if filled == original => OrderStatus::Filled,
        // 名为全部成交、实际没满：按部分成交后结束处理。
        "FILLED" => OrderStatus::Cancelled,
        "CANCELED" | "MARGIN_CANCELED" => OrderStatus::Cancelled,
        // IOC 一笔没吃到：交易所记成 REJECTED/IOC_CANCELED，实质是撤单、零成交。
        "REJECTED"
            if row.rejection_reason.as_deref() == Some("IOC_CANCELED") && filled.is_zero() =>
        {
            OrderStatus::Cancelled
        }
        "REJECTED" | "ERROR" if filled.is_zero() => OrderStatus::Rejected,
        "REJECTED" | "ERROR" => OrderStatus::Cancelled,
        "LIQUIDATED" | "ADL" => return Err(err("订单是强平或自动减仓产生的，请人工核对")),
        _ => return Err(err(format!("未知的订单状态 {}，拒绝推断终态", row.status))),
    };
    Ok(status)
}

fn filled_size(row: &OrderRow) -> ArbResult<Decimal> {
    let filled = match row.filled_size.as_deref() {
        Some(raw) => dec(raw)?,
        None => dec(&row.original_size)? - dec(&row.remaining_size)?,
    };
    if filled < Decimal::ZERO {
        return Err(err("成交量为负"));
    }
    Ok(filled)
}

/// 汇总成交：数量、名义、手续费（正 = 成本；实测吃单 fee 为正）。
fn aggregate_fills<'a>(
    rows: impl Iterator<Item = &'a FillRow>,
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let (mut quantity, mut notional, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    for row in rows {
        let size = dec(&row.size)?;
        let price = dec(&row.price)?;
        if size <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(err("成交数量与价格必须为正"));
        }
        quantity = add(quantity, size)?;
        notional = add(notional, multiply(size, price)?)?;
        fee = add(fee, dec(&row.fee)?)?;
    }
    Ok((quantity, notional, fee))
}

fn positions_from(
    page: PositionsPage,
    markets: &HashMap<u16, ArcusMarket>,
) -> ArbResult<Vec<VenuePosition>> {
    let mut positions = Vec::new();
    for (key, row) in page.positions {
        if key != row.market_id.to_string() {
            return Err(err("持仓的键与市场号不一致"));
        }
        let size = dec(&row.size)?;
        if size.is_zero() {
            continue;
        }
        let market = markets
            .get(&row.market_id)
            .ok_or_else(|| err("持仓所在市场不在市场列表里"))?;
        positions.push(VenuePosition {
            venue: VENUE,
            symbol: market.symbol(),
            net_quantity: size,
            average_price: dec(&row.average_entry_price)
                .ok()
                .filter(|price| *price > Decimal::ZERO),
            notional_usdt: dec(&row.position_value_notional)?.abs(),
        });
    }
    positions.sort_by(|a, b| a.symbol.base.cmp(&b.symbol.base));
    Ok(positions)
}

/// 下单明确没进撮合的回复：参数 / 规格被拒（400）、鉴权失败、限频。5xx 与超时不算 ——
/// 那种情况下订单可能已经进了撮合。
fn definitive_refusal(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::TOO_MANY_REQUESTS
    )
}

async fn read_reply(response: reqwest::Response) -> ArbResult<Reply> {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    Ok(Reply {
        status,
        body: serde_json::from_str(&text).unwrap_or(Value::Null),
    })
}

/// 交易所的错误文本（`error` / `errorType` / `rejectionReason`），截断到 200 字符。
fn error_text(body: &Value) -> String {
    let mut parts = Vec::new();
    for key in [
        "errorType",
        "error",
        "rejectionReason",
        "rejectReason",
        "reason",
    ] {
        if let Some(text) = body.get(key).and_then(Value::as_str) {
            parts.push(text.to_string());
        }
    }
    let joined = parts.join(" / ");
    joined.chars().take(200).collect()
}

fn mentions_not_found(body: &Value) -> bool {
    let text = body.to_string().to_ascii_lowercase();
    text.contains("order_not_found") || text.contains("not found") || text.contains("not_found")
}

/// 私钥栏里填的其实是某把已注册 key 的公钥（网页上两者挨着显示，最容易复制错）。
fn pasted_public_key(page: &ApiKeysPage, seed_digest: &[u8; 32]) -> bool {
    page.api_keys.iter().any(|row| {
        let digest: [u8; 32] = Sha256::digest(row.api_key.to_ascii_lowercase().as_bytes()).into();
        &digest == seed_digest
    })
}

fn http_error(path: &str, reply: &Reply) -> ArbError {
    err(format!(
        "{path} HTTP {}：{}",
        reply.status.as_u16(),
        error_text(&reply.body)
    ))
}

fn ack(state: &OrderState) -> ArbResult<OrderAck> {
    Ok(OrderAck {
        client_order_id: state.order.client_order_id.clone(),
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| err("缺少交易所订单号"))?,
        status: state.status,
    })
}

fn err(message: impl Into<String>) -> ArbError {
    ArbError::venue(VENUE.as_str(), message)
}

fn decode<T: DeserializeOwned>(value: Value) -> ArbResult<T> {
    serde_json::from_value(value).map_err(|_| err("交易所响应字段缺失或类型不符"))
}

fn dec(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw.trim()).map_err(|_| err("无法解析交易所数字"))
}

fn multiply(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_mul(b).ok_or_else(|| err("十进制乘法溢出"))
}

fn divide(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_div(b).ok_or_else(|| err("十进制除法溢出或除零"))
}

fn add(a: Decimal, b: Decimal) -> ArbResult<Decimal> {
    a.checked_add(b).ok_or_else(|| err("十进制加法溢出"))
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

fn parse_side(raw: &str) -> ArbResult<Side> {
    match raw {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(err("未知的订单方向")),
    }
}

fn is_order_id(raw: &str) -> bool {
    !raw.is_empty() && raw.len() <= 64 && raw.bytes().all(|b| b.is_ascii_hexdigit())
}

fn normalize_address(raw: &str) -> ArbResult<String> {
    let bytes = decode_hex::<20>(raw).map_err(|_| err("地址应是 0x 开头的 40 位十六进制"))?;
    Ok(format!("0x{}", hex_lower(&bytes)))
}

fn parse_account_index(raw: &str) -> ArbResult<u8> {
    raw.trim()
        .parse::<u8>()
        .ok()
        .filter(|index| *index <= 9)
        .ok_or_else(|| err("子账户序号应是 0 ~ 9"))
}

fn decode_hex<const N: usize>(raw: &str) -> ArbResult<[u8; N]> {
    let trimmed = raw.trim();
    let hex = trimmed.strip_prefix("0x").unwrap_or(trimmed).as_bytes();
    if hex.len() != N * 2 {
        return Err(err("十六进制长度不对"));
    }
    let digit = |byte: u8| -> ArbResult<u8> {
        char::from(byte)
            .to_digit(16)
            .map(|d| d as u8)
            .ok_or_else(|| err("不是十六进制"))
    };
    let mut out = [0u8; N];
    for (index, pair) in hex.chunks_exact(2).enumerate() {
        out[index] = digit(pair[0])? << 4 | digit(pair[1])?;
    }
    Ok(out)
}

fn now_ns() -> ArbResult<i64> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| err("本机时间超出范围"))
}

/// 严格递增的纳秒时间戳（按服务器时钟校正）。
fn next_timestamp(offset_ns: i64) -> ArbResult<i64> {
    let candidate = now_ns()?
        .checked_add(offset_ns)
        .ok_or_else(|| err("时间戳溢出"))?;
    let mut previous = LAST_TIMESTAMP.load(Ordering::SeqCst);
    loop {
        let next = candidate.max(previous + 1);
        match LAST_TIMESTAMP.compare_exchange(previous, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return Ok(next),
            Err(current) => previous = current,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier};
    use rust_decimal_macros::dec;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    // RFC 8032 §7.1 TEST 1：种子 → 公钥、空消息的签名。证明 API Signing Key 按 32 字节
    // 种子解释（Python `Ed25519PrivateKey.from_private_bytes` 同一口径）。
    const RFC_SEED: &str = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const RFC_PUBLIC: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
    const RFC_SIGNATURE: &str = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b";

    fn rfc_key() -> SigningKey {
        SigningKey::from_bytes(&decode_hex::<32>(RFC_SEED).unwrap())
    }

    // 2026-09-29 主网公开订单历史里的一行（IOC 零成交）。
    const IOC_MISS: &str = r#"{"orderId":"31e67de1662c4950","clientId":"1790661690814229079","address":"0x00000000000000000000000000000000000000aa","marketId":8,"marketDisplayName":"ZEC-USD","side":"BUY","type":"LIMIT","status":"REJECTED","price":"1388.365","originalSize":"1.439962","filledSize":"0","remainingSize":"1.439962","timeInForce":"IOC","goodTilTime":"1794117690814229","rejectionReason":"IOC_CANCELED","createdAt":1790661690878597,"updatedAt":1790661690874697,"sequenceNumber":1586026}"#;

    // 同一时刻 `/v1/positions` 的两条（删掉了与本券商无关的字段）。
    const POSITIONS: &str = r#"{"positions":{"28":{"address":"0x00000000000000000000000000000000000000aa","accountIndex":0,"marketId":28,"marketDisplayName":"NVDA-USD","side":"LONG","size":"0.3797444","averageEntryPrice":"229.06","leverage":"2","marginMode":"CROSS","positionValueNotional":"87.026024148","markPx":"229.17"},"1":{"address":"0x00000000000000000000000000000000000000aa","accountIndex":0,"marketId":1,"marketDisplayName":"BTC-USD","side":"SHORT","size":"-0.22684768","averageEntryPrice":"83797","leverage":"2","marginMode":"CROSS","positionValueNotional":"-19001.033894016","markPx":"83761.2"}}}"#;

    // 同一时刻 `/v1/fills` 的两条（同一订单拆成两笔的形状是构造的，数值取自真实行）。
    const FILLS: &str = r#"{"fills":[{"tradeId":"t1","orderId":"9767133dca7d4da2","marketId":2,"marketDisplayName":"ETH-USD","side":"BUY","originalSize":"1.7","size":"1.6378074","price":"2675.56","fee":"0.503936032","role":"TAKER","createdAt":1790661661434343},{"tradeId":"t2","orderId":"f021f73c8c0b471c","marketId":2,"marketDisplayName":"ETH-USD","side":"BUY","originalSize":"0.06","size":"0.0560385","price":"2675.89","fee":"0.017244579","role":"TAKER","createdAt":1790661632650845}]}"#;

    fn row(status: &str, reason: Option<&str>, filled: &str) -> OrderRow {
        let mut row: OrderRow = serde_json::from_str(IOC_MISS).unwrap();
        row.status = status.into();
        row.rejection_reason = reason.map(str::to_string);
        row.filled_size = Some(filled.into());
        row
    }

    fn btc_instrument() -> Instrument {
        let market: ArcusMarket = serde_json::from_str(r#"{"marketDisplayName":"BTC-USD","marketId":1,"status":"ONLINE","baseAsset":"BTC","quoteAsset":"USD","type":"PERPETUAL","tickSize":"0.1","stepSize":"0.00000001","tickTiers":[{"upToPrice":"500000","tick":"0.1"},{"upToPrice":"1000000","tick":"0.2"},{"upToPrice":"2000000","tick":"0.5"},{"tick":"5"}],"minOrderNotional":"5","minOrderSize":"0.0001","maxOrderSize":"10000","oraclePrice":"83390.5","markPrice":"83401.9","nextFundingRate":"0.0000125","initialMarginFraction":"0.025","maintenanceMarginFraction":"0.016667"}"#).unwrap();
        instrument_of(market).unwrap()
    }

    fn dec_str(raw: &str) -> Decimal {
        raw.parse().unwrap()
    }

    #[test]
    fn a_topped_up_isolated_position_reports_its_real_margin() {
        // 2026-09-30 实测 `GET /v1/positions`（LIT 空腿，补过约 785 USDT 保证金后；开仓时是 999.14）。
        let row: PositionRow = serde_json::from_str(
            r#"{"accountIndex":0,"marketId":41,"marketDisplayName":"LIT-USD","side":"SHORT","size":"-751.99",
                "averageEntryPrice":"3.986","leverage":"3","marginMode":"ISOLATED","borrowedCapital":"2997.490277603",
                "marginUsed":"1784.94782831","positionValueNotional":"-3052.32741","unrealizedPnl":"-54.837132397","markPx":"4.059"}"#,
        )
        .unwrap();
        let state = leg_state_from_row(&row).unwrap();
        assert_eq!(state.margin_usdt, Some(dec_str("1784.94782831")));
        assert_eq!(
            state.liquidation_price, None,
            "Arcus 不给强平价，由调用方按保证金算"
        );
        // 老响应没有这个字段：当作没有，不是 0。
        let old: PositionRow = serde_json::from_str(
            r#"{"marketId":41,"size":"-1","averageEntryPrice":"1","positionValueNotional":"-1"}"#,
        )
        .unwrap();
        assert_eq!(leg_state_from_row(&old).unwrap().margin_usdt, None);
    }

    #[test]
    fn funding_payments_parse_and_sum_from_the_start_of_the_position() {
        // `GET /v1/funding` 的形状（官方文档）：最新在前，时间是微秒，payment 正 = 收到。
        let page: FundingPage = serde_json::from_value(json!({
            "fundingPayments": [
                {"marketId": 56, "marketDisplayName": "PONS-USD", "fundingRate": "0.00002", "size": "-1797.9", "payment": "0.0198", "time": 1790755200000000_i64},
                {"marketId": 56, "marketDisplayName": "PONS-USD", "fundingRate": "0.00001", "size": "-1797.9", "payment": "0.0099", "time": 1790751600000000_i64},
                {"marketId": 56, "marketDisplayName": "PONS-USD", "fundingRate": "-0.00001", "size": "-1797.9", "payment": "-0.0099", "time": 1790740000000000_i64}
            ],
            "total": 3
        }))
        .unwrap();
        let since = chrono::DateTime::from_timestamp_micros(1790749336878000).unwrap();
        let rows = page
            .funding_payments
            .iter()
            .map(|row| {
                (
                    chrono::DateTime::from_timestamp_micros(row.time).unwrap(),
                    dec(&row.payment).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let total = FundingTotal::from_rows(rows, since);
        // 开仓之前那一笔不算。
        assert_eq!(total.payments, 2);
        assert_eq!(total.usdt, Decimal::new(297, 4));
        assert_eq!(
            total.last_at,
            chrono::DateTime::from_timestamp_micros(1790755200000000)
        );
    }

    #[test]
    fn the_signing_self_test_accepts_an_acknowledged_async_cancel() {
        let reply = |status: u16, body: Value| Reply {
            status: StatusCode::from_u16(status).unwrap(),
            body,
        };
        // 2026-09-30 主网实测：签名对了，撤一个不存在的客户订单号回 202 + CANCEL_ACKNOWLEDGED。
        let acknowledged = reply(
            202,
            json!({
                "accountIndex": 0,
                "address": "0x0000000000000000000000000000000000000001",
                "clientId": "arb-probe-0123456789abcdef",
                "marketDisplayName": "BTC-USD",
                "marketId": 1,
                "rateLimit": {},
                "status": "CANCEL_ACKNOWLEDGED",
                "updateTime": 0
            }),
        );
        assert!(self_test_verdict(&acknowledged).is_ok());
        // 早先的同步回复：查无此单，同样说明签名被接受。
        assert!(self_test_verdict(&reply(404, json!({"error": "ORDER_NOT_FOUND"}))).is_ok());
        // 同一时刻用随机密钥签的：网关当场拒绝。
        let rejected = reply(
            401,
            json!({"error": "invalid order signature", "errorType": "Unauthorized", "errorSource": "Cancel"}),
        );
        let error = self_test_verdict(&rejected).unwrap_err().to_string();
        assert!(error.contains("invalid order signature"), "{error}");
        // 网关自己出错：判断不了，不开下单。
        let error = self_test_verdict(&reply(500, json!({"error": "internal"})))
            .unwrap_err()
            .to_string();
        assert!(error.contains("无法判断"), "{error}");
    }

    #[test]
    fn signing_key_is_the_rfc8032_seed() {
        let key = rfc_key();
        assert_eq!(hex_lower(key.verifying_key().as_bytes()), RFC_PUBLIC);
        assert_eq!(hex_lower(&key.sign(b"").to_bytes()), RFC_SIGNATURE);
    }

    #[test]
    fn place_payload_is_byte_exact_and_verifies() {
        let message = place_message(&PlacePayload {
            address: "0xAbCd000000000000000000000000000000000001",
            account_index: 3,
            client_id: "arb-0123abcd",
            timestamp_ns: 1_790_661_690_814_229_079,
            good_til_ns: 1_794_117_690_814_229_000,
            market_id: 1,
            price_ticks: 833_775,
            quantity_steps: 120_000,
            reduce_only: false,
            side: Side::Sell,
        });
        assert_eq!(
            message,
            r#"{"ad":"0xabcd000000000000000000000000000000000001","ai":3,"c":"arb-0123abcd","ct":1790661690814229079,"g":1794117690814229000,"m":1,"op":1,"p":833775,"q":120000,"r":0,"s":1,"t":2,"v":1}"#
        );
        let key = rfc_key();
        let signature = Signature::from_bytes(&key.sign(message.as_bytes()).to_bytes());
        assert!(
            key.verifying_key()
                .verify(message.as_bytes(), &signature)
                .is_ok()
        );
        assert_eq!(hex_lower(&signature.to_bytes()).len(), 128);
    }

    #[test]
    fn cancel_payload_omits_the_unused_identifier() {
        let by_id = cancel_message(&CancelPayload {
            address: "0xABCD000000000000000000000000000000000001",
            account_index: 0,
            client_id: None,
            timestamp_ns: 5,
            order_id: Some("9767133dca7d4da2"),
            market_id: 2,
        });
        assert_eq!(
            by_id,
            r#"{"ad":"0xabcd000000000000000000000000000000000001","ai":0,"ct":5,"id":"9767133dca7d4da2","m":2,"op":2,"v":1}"#
        );
        let by_client = cancel_message(&CancelPayload {
            address: "0xabcd000000000000000000000000000000000001",
            account_index: 0,
            client_id: Some("arb-x"),
            timestamp_ns: 5,
            order_id: None,
            market_id: 2,
        });
        assert_eq!(
            by_client,
            r#"{"ad":"0xabcd000000000000000000000000000000000001","ai":0,"c":"arb-x","ct":5,"m":2,"op":2,"v":1}"#
        );
    }

    #[test]
    fn legacy_message_is_timestamp_action_then_sorted_body() {
        let body = json!({"marketId": 1, "leverage": 3, "isolated": true, "address": "0xab", "accountIndex": 0, "nested": {"b": [1, {"d": 1, "c": 2}], "a": "x"}});
        assert_eq!(
            legacy_message(7, "setLeverage", &body),
            r#"7setLeverage{"accountIndex":0,"address":"0xab","isolated":true,"leverage":3,"marketId":1,"nested":{"a":"x","b":[1,{"c":2,"d":1}]}}"#
        );
    }

    #[test]
    fn ioc_without_fills_is_a_cancel_not_a_rejection() {
        let miss: OrderRow = serde_json::from_str(IOC_MISS).unwrap();
        assert_eq!(map_status(&miss).unwrap(), OrderStatus::Cancelled);
        assert_eq!(
            map_status(&row("REJECTED", Some("UNDERCOLLATERALIZED"), "0")).unwrap(),
            OrderStatus::Rejected
        );
        assert_eq!(
            map_status(&row("ERROR", None, "0")).unwrap(),
            OrderStatus::Rejected
        );
        assert_eq!(
            map_status(&row("CANCELED", None, "0.5")).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status(&row("MARGIN_CANCELED", None, "0")).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            map_status(&row("FILLED", None, "1.439962")).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            map_status(&row("FILLED", None, "1")).unwrap(),
            OrderStatus::Cancelled
        );
        for live in [
            "ACK",
            "PENDING",
            "OPEN",
            "PARTIALLY_FILLED",
            "CANCEL_PENDING",
        ] {
            assert!(
                map_status(&row(live, None, "0")).unwrap().is_live(),
                "{live}"
            );
        }
        for foreign in [
            "LIQUIDATED",
            "ADL",
            "UNTRIGGERED",
            "TPSL_PLACED",
            "SOMETHING_NEW",
        ] {
            assert!(map_status(&row(foreign, None, "0")).is_err(), "{foreign}");
        }
        let mut tpsl = row("OPEN", None, "0");
        tpsl.tpsl_type = Some("STOP_LOSS".into());
        assert!(map_status(&tpsl).is_err());
    }

    #[test]
    fn prices_round_within_their_tier_without_getting_worse() {
        let btc = btc_instrument();
        assert_eq!(
            tiered_price(&btc, dec!(83377.56), Side::Buy).unwrap(),
            dec!(83377.5)
        );
        assert_eq!(
            tiered_price(&btc, dec!(83377.51), Side::Sell).unwrap(),
            dec!(83377.6)
        );
        // 50 万到 100 万之间 tick 是 0.2。
        assert_eq!(
            tiered_price(&btc, dec!(600000.3), Side::Buy).unwrap(),
            dec!(600000.2)
        );
        assert_eq!(
            tiered_price(&btc, dec!(600000.3), Side::Sell).unwrap(),
            dec!(600000.4)
        );
        // 卖单向上取整跨进更粗的段后，按新段再取一次。
        assert_eq!(
            tiered_price(&btc, dec!(1999999.9), Side::Sell).unwrap(),
            dec!(2000000)
        );
        assert_eq!(
            tiered_price(&btc, dec!(2000001), Side::Buy).unwrap(),
            dec!(2000000)
        );
        // 签名整数一律按顶层 tick。
        assert_eq!(exact_units(dec!(600000.2), btc.tick).unwrap(), 6_000_002);
    }

    #[test]
    fn engine_integers_must_divide_exactly() {
        assert_eq!(
            exact_units(dec!(0.0012), dec!(0.00000001)).unwrap(),
            120_000
        );
        assert!(exact_units(dec!(0.123456789), dec!(0.00000001)).is_err());
        assert!(exact_units(dec!(0), dec!(0.1)).is_err());
    }

    #[test]
    fn fills_sum_quantity_notional_and_positive_fees() {
        let page: FillsPage = serde_json::from_str(FILLS).unwrap();
        let (quantity, notional, fee) = aggregate_fills(
            page.fills
                .iter()
                .filter(|f| f.order_id == "9767133dca7d4da2"),
        )
        .unwrap();
        assert_eq!(quantity, dec!(1.6378074));
        assert_eq!(notional, dec!(1.6378074) * dec!(2675.56));
        assert_eq!(fee, dec!(0.503936032));
    }

    #[test]
    fn positions_use_signed_sizes_and_the_shared_symbol() {
        let page: PositionsPage = serde_json::from_str(POSITIONS).unwrap();
        let markets: HashMap<u16, ArcusMarket> = [(1, "BTC"), (28, "NVDA")]
            .into_iter()
            .map(|(id, base)| {
                let mut market = btc_instrument().market;
                market.market_id = id;
                market.base_asset = base.into();
                (id, market)
            })
            .collect();
        let positions = positions_from(page, &markets).unwrap();
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].symbol, arb_venues::arcus::arcus_symbol("BTC"));
        assert_eq!(positions[0].net_quantity, dec!(-0.22684768));
        assert_eq!(positions[0].notional_usdt, dec!(19001.033894016));
        assert_eq!(positions[1].symbol, Symbol::perp("NVDA", "USDT"));
        assert_eq!(positions[1].average_price, Some(dec!(229.06)));
    }

    #[test]
    fn client_ids_fit_the_venue_charset() {
        let id = venue_client_id(
            CLIENT_ID_PREFIX,
            &ClientOrderId("live-1790000000000-buy-exit12".into()),
            MAX_CLIENT_ID,
        );
        assert!(id.len() <= MAX_CLIENT_ID && id.starts_with(CLIENT_ID_PREFIX));
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        );
    }

    #[test]
    fn timestamps_strictly_increase() {
        let a = next_timestamp(0).unwrap();
        let b = next_timestamp(-10_000_000_000).unwrap();
        assert!(b > a);
    }

    #[test]
    fn credentials_are_validated_without_echoing_them() {
        assert_eq!(
            normalize_address("0xABCDEF0000000000000000000000000000000001").unwrap(),
            "0xabcdef0000000000000000000000000000000001"
        );
        let error = normalize_address("0xnothex").unwrap_err().to_string();
        assert!(!error.contains("nothex"));
        assert_eq!(parse_account_index(" 9 ").unwrap(), 9);
        assert!(parse_account_index("10").is_err());
        assert!(decode_hex::<32>(RFC_SEED).is_ok());
        assert!(decode_hex::<32>(&RFC_SEED[2..]).is_err());
    }

    /// 联网只读冒烟测试（默认不跑）：`cargo test -p arb-exec arcus -- --ignored`。
    ///
    /// 账户读接口不需要鉴权，所以拿排行榜上一个活跃地址，跳过 `connect` 的 key 校验直接
    /// 构造券商，把持仓、挂单、订单 → 成交 → 手续费这条换算链对真实主网跑一遍。
    /// 不签名、不发任何写请求（`trading_enabled` 为假）。
    #[tokio::test]
    #[ignore = "联网：读 Arcus 主网公开账户数据"]
    async fn live_read_paths_parse_real_accounts() {
        let client = Client::new();
        let board: Value = client
            .get(format!("{BASE_URL}/v1/leaderboard?limit=5"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let address = board["entries"][2]["address"].as_str().unwrap().to_string();
        let journal_path =
            std::env::temp_dir().join(format!("arcus-smoke-{}.jsonl", now_ns().unwrap()));
        let mut broker = ArcusBroker {
            client: client.clone(),
            base_url: BASE_URL.to_string(),
            key: SigningKey::from_bytes(&[7; 32]),
            api_key: String::new(),
            address: normalize_address(&address).unwrap(),
            account_index: 0,
            options: LiveOptions::default(),
            taker_fee: fetch_base_taker_fee(&client).await.unwrap(),
            clock_offset_ns: 0,
            journal: Mutex::new(OrderJournal::open(&journal_path, "arcus:smoke").unwrap()),
            order_ids: Mutex::new(HashMap::new()),
            submit: Mutex::new(()),
            markets_cache: Cached::new(),
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
        };
        broker.sync_clock().await.unwrap();
        assert!(broker.clock_offset_ns.abs() < 5_000_000_000);

        let positions = broker.positions().await.unwrap();
        for position in &positions {
            assert!(!position.symbol.base.is_empty() && position.symbol.quote == "USDT");
            assert!(!position.net_quantity.is_zero() && position.notional_usdt > Decimal::ZERO);
        }
        let open = broker.open_orders().await.unwrap();
        assert!(open.iter().all(|state| state.status.is_live()));

        let mut query = broker.account_query();
        query.push(("limit", "300".to_string()));
        let page: OrdersPage = broker.get_ok("/v1/orders", &query).await.unwrap();
        let markets = broker.market_map().await.unwrap();
        let filled = page
            .orders
            .iter()
            .find(|row| {
                filled_size(row).unwrap() > Decimal::ZERO && !map_status(row).unwrap().is_live()
            })
            .expect("地址最近应有成交");
        let state = broker.state_from_row(filled, &markets).await.unwrap();
        assert!(state.filled_usdt > Decimal::ZERO && state.fee_usdt >= Decimal::ZERO);
        assert!(state.order.client_order_id.0.starts_with(EXTERNAL_PREFIX));
        // 限价单的成交均价不会比限价差：买 ≤ 限价，卖 ≥ 限价。
        let average = state.average_price.unwrap();
        let limit = dec(&filled.price).unwrap();
        match state.order.side {
            Side::Buy => assert!(average <= limit, "{average} > {limit}"),
            Side::Sell => assert!(average >= limit, "{average} < {limit}"),
        }
        let by_id = broker.order_by_id(&filled.order_id).await.unwrap().unwrap();
        assert_eq!(by_id.order_id, filled.order_id);
        if let Some(client_id) = filled.client_id.as_deref() {
            let by_client = broker
                .order_by_client_id(filled.market_id, client_id)
                .await
                .unwrap();
            assert_eq!(
                by_client.map(|row| row.order_id),
                Some(filled.order_id.clone())
            );
        }
        if let Some(miss) = page
            .orders
            .iter()
            .find(|row| row.rejection_reason.as_deref() == Some("IOC_CANCELED"))
        {
            let state = broker.state_from_row(miss, &markets).await.unwrap();
            assert_eq!(state.status, OrderStatus::Cancelled);
            assert!(state.filled_usdt.is_zero());
        }
        println!(
            "持仓 {} 笔，挂单 {} 笔；样例成交 {} {} 名义 {} 手续费 {} 均价 {:?}",
            positions.len(),
            open.len(),
            state.order.symbol,
            state.status as u8,
            state.filled_usdt,
            state.fee_usdt,
            state.average_price
        );
        drop(broker);
        let _ = std::fs::remove_file(journal_path);
    }

    #[test]
    fn a_public_key_in_the_private_key_field_is_recognised() {
        let page: ApiKeysPage = serde_json::from_str(&format!(
            r#"{{"apiKeys":[{{"apiKey":"{RFC_PUBLIC}","address":"0xab","accountIndex":0,"allSubaccounts":false,"status":"ACTIVE","validUntil":0,"createdAt":1}}]}}"#
        ))
        .unwrap();
        // 私钥栏里填的正是已注册的公钥。
        let pasted: [u8; 32] = Sha256::digest(RFC_PUBLIC.as_bytes()).into();
        assert!(pasted_public_key(&page, &pasted));
        // 填的是真正的私钥种子：不是这种错误。
        let seed: [u8; 32] = Sha256::digest(RFC_SEED.as_bytes()).into();
        assert!(!pasted_public_key(&page, &seed));
    }

    #[test]
    fn only_pre_matching_refusals_are_definitive() {
        for status in [400, 401, 403, 429] {
            assert!(
                definitive_refusal(StatusCode::from_u16(status).unwrap()),
                "{status}"
            );
        }
        for status in [200, 202, 500, 502, 503, 504] {
            assert!(
                !definitive_refusal(StatusCode::from_u16(status).unwrap()),
                "{status}"
            );
        }
    }

    // ───────────────────────── 补保证金 ─────────────────────────

    const MARGIN_ADDRESS: &str = "0xabcd000000000000000000000000000000000001";

    fn market(market_id: u16, base: &str) -> ArcusMarket {
        serde_json::from_value(json!({
            "marketDisplayName": format!("{base}-USD"), "marketId": market_id, "status": "ONLINE",
            "baseAsset": base, "quoteAsset": "USD", "type": "PERPETUAL", "tickSize": "0.001",
            "stepSize": "0.01", "minOrderNotional": "5", "minOrderSize": "0.01",
            "maxOrderSize": "100000", "oraclePrice": "4", "markPrice": "4",
            "nextFundingRate": "0.00001", "initialMarginFraction": "0.2",
            "maintenanceMarginFraction": "0.133334"
        }))
        .unwrap()
    }

    /// 2026-09-30 实测 `GET /v1/positions` 的 LIT 空腿（字段取自真实响应）。
    fn lit_row(market_id: u16, mode: Option<&str>, margin: Option<&str>, size: &str) -> Value {
        let mut row = json!({
            "accountIndex": 3, "marketId": market_id, "marketDisplayName": "LIT-USD",
            "side": "SHORT", "size": size, "averageEntryPrice": "3.986", "leverage": "3",
            "borrowedCapital": "2997.490277603", "positionValueNotional": "-3052.32741",
            "unrealizedPnl": "-54.837132397", "markPx": "4.059"
        });
        if let Some(mode) = mode {
            row["marginMode"] = json!(mode);
        }
        if let Some(margin) = margin {
            row["marginUsed"] = json!(margin);
        }
        row
    }

    fn lit_positions(mode: Option<&str>, margin: Option<&str>, size: &str) -> Value {
        json!({"positions": {"41": lit_row(41, mode, margin, size)}})
    }

    fn lit_page(mode: Option<&str>, margin: Option<&str>, size: &str) -> PositionsPage {
        serde_json::from_value(lit_positions(mode, margin, size)).unwrap()
    }

    fn margin_reply(status: u16, body: Value) -> Reply {
        Reply {
            status: StatusCode::from_u16(status).unwrap(),
            body,
        }
    }

    #[test]
    fn margin_request_is_byte_exact_and_matches_an_independent_signature() {
        let request = margin_request(MARGIN_ADDRESS, 3, 41, "12.5", 1_790_661_690_814_229_079);
        // 正文：键名排序、无空白；`amount` 是美元字符串，`marketId` / `accountIndex` 是数字。
        assert_eq!(
            request.body,
            r#"{"accountIndex":3,"address":"0xabcd000000000000000000000000000000000001","amount":"12.5","marketId":41}"#
        );
        // 待签消息 = 时间戳 + 动作名 + 同一份规范 JSON（官方 asyncapi：legacy scheme）。
        assert_eq!(
            request.message,
            format!("1790661690814229079adjustIsolatedMargin{}", request.body)
        );
        // 金标：同一条消息用独立实现（Python `cryptography` / OpenSSL 的 Ed25519，RFC 8032 测试
        // 种子）签出；Ed25519 是确定性的。这不是 Arcus 服务端提供的验签金标。
        assert_eq!(
            hex_lower(&rfc_key().sign(request.message.as_bytes()).to_bytes()),
            "3c39638992ffa923ee1f35d9af0b6d661ae7465bb2a2b46c2c4bf54798a2ac44417f8b080e304f474b3e080ec7a829c7b0f023389f9de04cba477f3417015304"
        );
    }

    #[test]
    fn margin_amounts_are_positive_dollar_strings_in_whole_cents() {
        for (amount, text) in [
            (dec!(100), "100"),
            (dec!(100.00), "100"),
            (dec!(12.50), "12.5"),
            (dec!(0.01), "0.01"),
            (dec!(1234.56), "1234.56"),
        ] {
            assert_eq!(margin_amount_text(amount).unwrap(), text, "{amount}");
        }
        // 零（官方：400）、负数（那是「取回」）、不是整分的金额都不发。
        for amount in [
            Decimal::ZERO,
            dec!(-5),
            dec!(-0.01),
            dec!(12.345),
            dec!(0.001),
        ] {
            assert!(margin_amount_text(amount).is_err(), "{amount}");
        }
    }

    #[test]
    fn margin_replies_map_to_applied_acknowledged_refused_or_unknown() {
        let kind =
            |status: u16, body: Value| match classify_margin_reply(&margin_reply(status, body)) {
                MarginReply::Applied => "applied",
                MarginReply::Acknowledged(_) => "ack",
                MarginReply::Refused(_) => "refused",
                MarginReply::Unknown(_) => "unknown",
            };
        let engine = |status: &str| {
            json!({"requestId": "r", "address": MARGIN_ADDRESS, "accountIndex": 3, "marketId": 41,
                   "amount": "25", "newIsolatedMarginQuoteBalance": "0", "status": status})
        };
        assert_eq!(kind(200, engine("APPLIED")), "applied");
        assert_eq!(kind(202, engine("ACK")), "ack");
        // 200 但没有 APPLIED / 没有正文：不能当成确认，交给读回。
        assert_eq!(kind(200, engine("ACK")), "ack");
        assert_eq!(kind(200, Value::Null), "ack");
        // 422 REJECTED：官方列出的每一种 rejectReason，以及将来新增的，都是明确拒绝。
        for reason in [
            "UNKNOWN_MARKET",
            "INVALID_AMOUNT",
            "NOT_ISOLATED",
            "NO_OPEN_POSITION",
            "UNDERCOLLATERALIZED",
            "MISSING_MARK_PRICE",
            "SOMETHING_NEW",
        ] {
            let mut body = engine("REJECTED");
            body["rejectReason"] = json!(reason);
            assert_eq!(kind(422, body), "refused", "{reason}");
        }
        // 422 但没有原因：说不清是谁拒的，不当成明确拒绝。
        assert_eq!(kind(422, engine("REJECTED")), "unknown");
        assert_eq!(kind(422, Value::Null), "unknown");
        // 网关在转给引擎之前的拒绝与限频。
        assert_eq!(kind(400, json!({"error": "invalid amount"})), "refused");
        assert_eq!(kind(401, json!({"error": "invalid signature"})), "refused");
        assert_eq!(kind(403, json!({"error": "address mismatch"})), "refused");
        assert_eq!(kind(429, json!({"error": "rate limited"})), "refused");
        assert_eq!(kind(429, Value::Null), "refused");
        // 5xx 与其它状态码：请求可能已被处理。
        for status in [404, 408, 409, 500, 502, 503, 504] {
            assert_eq!(kind(status, json!({"error": "x"})), "unknown", "{status}");
            assert_eq!(kind(status, Value::Null), "unknown", "{status}");
        }
    }

    #[test]
    fn margin_reply_texts_say_what_happened_and_what_to_do() {
        let text =
            |status: u16, body: Value| match classify_margin_reply(&margin_reply(status, body)) {
                MarginReply::Refused(text) | MarginReply::Unknown(text) => text,
                other => panic!("{other:?}"),
            };
        let rejected = text(
            422,
            json!({"status": "REJECTED", "rejectReason": "UNDERCOLLATERALIZED"}),
        );
        assert!(rejected.contains("UNDERCOLLATERALIZED") && rejected.contains("没有动钱"));
        let unauthorized = text(401, json!({"error": "invalid order signature"}));
        assert!(
            unauthorized.contains("invalid order signature"),
            "{unauthorized}"
        );
        assert!(unauthorized.contains("没有重发"));
        let limited = text(429, json!({"error": "rate limited", "retryAfterMs": 1250}));
        assert!(
            limited.contains("1250ms") && limited.contains("没有动钱"),
            "{limited}"
        );
        let server = text(
            503,
            json!({"error": "unavailable", "errorType": "Transmission"}),
        );
        assert!(
            server.contains("Transmission") && server.contains("可能已到账"),
            "{server}"
        );
    }

    #[test]
    fn margin_counts_as_applied_only_once_nearly_all_of_it_shows_up() {
        let (before, amount) = (dec!(1000), dec!(50));
        for (after, confirmed) in [
            (dec!(1050), true),
            (dec!(1049.5), true),
            (dec!(1060), true),
            (dec!(1049.49), false),
            (dec!(1000), false),
            // 保证金反而少了（比如符号搞反成了「取回」）：绝不算到账。
            (dec!(950), false),
            (dec!(0), false),
        ] {
            assert_eq!(
                margin_confirmed(before, amount, after),
                confirmed,
                "{after}"
            );
        }
    }

    #[test]
    fn only_an_open_isolated_leg_with_a_known_margin_can_be_topped_up() {
        assert_eq!(
            isolated_margin_in(
                &lit_page(Some("ISOLATED"), Some("1784.94782831"), "-751.99"),
                41
            )
            .unwrap()
            .margin,
            dec_str("1784.94782831")
        );
        assert!(
            isolated_margin_in(&lit_page(Some("isolated"), Some("10"), "-1"), 41).is_ok(),
            "大小写不敏感"
        );
        let refusal = |page: PositionsPage, market_id: u16| {
            isolated_margin_in(&page, market_id)
                .unwrap_err()
                .to_string()
        };
        assert!(refusal(lit_page(Some("CROSS"), Some("100"), "-1"), 41).contains("CROSS"));
        assert!(refusal(lit_page(None, Some("100"), "-1"), 41).contains("保证金模式"));
        assert!(refusal(lit_page(Some("ISOLATED"), None, "-1"), 41).contains("marginUsed"));
        assert!(refusal(lit_page(Some("ISOLATED"), Some("0"), "-1"), 41).contains("marginUsed"));
        assert!(refusal(lit_page(Some("ISOLATED"), Some("100"), "0"), 41).contains("没有持仓"));
        assert!(refusal(lit_page(Some("ISOLATED"), Some("100"), "-1"), 1).contains("没有持仓"));
    }

    #[test]
    fn the_leg_is_found_by_symbol_among_the_position_rows() {
        let markets = |extra: &[(u16, &str)]| -> HashMap<u16, ArcusMarket> {
            [(41, "LIT"), (1, "BTC")]
                .iter()
                .chain(extra)
                .map(|(id, base)| (*id, market(*id, base)))
                .collect()
        };
        let lit = market(41, "LIT").symbol();
        let page = lit_page(Some("ISOLATED"), Some("100"), "-1");
        assert_eq!(position_market(&page, &markets(&[]), &lit).unwrap(), 41);
        // 标的没有持仓 / 持仓是 0 / 市场列表里没有这个市场。
        let btc = market(1, "BTC").symbol();
        assert!(position_market(&page, &markets(&[]), &btc).is_err());
        let flat = lit_page(Some("ISOLATED"), Some("100"), "0");
        assert!(position_market(&flat, &markets(&[]), &lit).is_err());
        let unlisted: HashMap<u16, ArcusMarket> = HashMap::from([(1, market(1, "BTC"))]);
        assert!(position_market(&page, &unlisted, &lit).is_err());
        // 两个同名市场都有仓位：不猜。
        let both: PositionsPage = serde_json::from_value(json!({"positions": {
            "41": lit_row(41, Some("ISOLATED"), Some("100"), "-1"),
            "77": lit_row(77, Some("ISOLATED"), Some("100"), "-1"),
        }}))
        .unwrap();
        let error = position_market(&both, &markets(&[(77, "LIT")]), &lit)
            .unwrap_err()
            .to_string();
        assert!(error.contains("多个"), "{error}");
        // 同名的旧市场没有仓位：只认有仓位的那个。
        let old: PositionsPage = serde_json::from_value(json!({"positions": {
            "41": lit_row(41, Some("ISOLATED"), Some("100"), "-1"),
            "77": lit_row(77, Some("ISOLATED"), Some("100"), "0"),
        }}))
        .unwrap();
        assert_eq!(
            position_market(&old, &markets(&[(77, "LIT")]), &lit).unwrap(),
            41
        );
    }

    // ── 整条流程：本机脚本化服务器（没有任何网络），记录每个请求 ──

    #[derive(Clone)]
    struct Seen {
        method: String,
        target: String,
        headers: HashMap<String, String>,
        body: String,
    }

    /// (方法, 路径) → 按顺序给出的回复；最后一个无限重复。状态码 0 = 收完请求后直接断开连接。
    type Script = HashMap<(String, String), VecDeque<(u16, String)>>;

    /// (方法, 路径, 依次给出的 (状态码, 正文))。
    type Route<'a> = (&'a str, &'a str, Vec<(u16, Value)>);

    struct FakeArcus {
        base_url: String,
        seen: Arc<std::sync::Mutex<Vec<Seen>>>,
    }

    /// 测试服务器任务里 panic 不该把别的断言一起毒掉：忽略 poison。
    fn locked<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    impl FakeArcus {
        async fn start(routes: Vec<Route<'_>>) -> Self {
            let script: Script = routes
                .into_iter()
                .map(|(method, path, replies)| {
                    (
                        (method.to_string(), path.to_string()),
                        replies
                            .into_iter()
                            .map(|(status, body)| (status, body.to_string()))
                            .collect(),
                    )
                })
                .collect();
            let script = Arc::new(std::sync::Mutex::new(script));
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let (task_script, task_seen) = (script.clone(), seen.clone());
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(serve(stream, task_script.clone(), task_seen.clone()));
                }
            });
            Self { base_url, seen }
        }

        fn seen(&self, method: &str) -> Vec<Seen> {
            locked(&self.seen)
                .iter()
                .filter(|request| request.method == method)
                .cloned()
                .collect()
        }

        fn requests(&self) -> usize {
            locked(&self.seen).len()
        }
    }

    async fn serve(
        mut stream: TcpStream,
        script: Arc<std::sync::Mutex<Script>>,
        seen: Arc<std::sync::Mutex<Vec<Seen>>>,
    ) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(at) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
                break at + 4;
            }
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let mut request_line = lines.next().unwrap_or_default().split(' ');
        let method = request_line.next().unwrap_or_default().to_string();
        let target = request_line.next().unwrap_or_default().to_string();
        let headers: HashMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        let length: usize = headers
            .get("content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        while buf.len() < head_end + length {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let body = String::from_utf8_lossy(&buf[head_end..head_end + length]).into_owned();
        let path = target.split('?').next().unwrap_or_default().to_string();
        let reply = {
            let mut script = locked(&script);
            match script.get_mut(&(method.clone(), path)) {
                Some(queue) if queue.len() > 1 => queue.pop_front(),
                Some(queue) => queue.front().cloned(),
                None => Some((404, r#"{"error":"not found"}"#.to_string())),
            }
        };
        locked(&seen).push(Seen {
            method,
            target,
            headers,
            body,
        });
        if let Some((status, text)) = reply
            && status != 0
        {
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                text.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        }
        let _ = stream.shutdown().await;
    }

    struct TempJournal(std::path::PathBuf);

    impl Drop for TempJournal {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// 子账户 3、RFC 8032 测试私钥、市场列表里有 LIT(41) 与 BTC(1)；不联网。
    fn margin_broker(base_url: &str, trading_enabled: bool) -> (ArcusBroker, TempJournal) {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "arcus-margin-{}-{}.jsonl",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let broker = ArcusBroker {
            client: Client::builder().no_proxy().build().unwrap(),
            base_url: base_url.to_string(),
            key: rfc_key(),
            api_key: RFC_PUBLIC.to_string(),
            address: MARGIN_ADDRESS.to_string(),
            account_index: 3,
            options: LiveOptions {
                trading_enabled,
                market_slippage: trading_enabled.then(|| dec!(0.01)),
            },
            taker_fee: Decimal::ZERO,
            clock_offset_ns: 0,
            journal: Mutex::new(OrderJournal::open(&path, "arcus:margin-test").unwrap()),
            order_ids: Mutex::new(HashMap::new()),
            submit: Mutex::new(()),
            markets_cache: Cached::new(),
            verified_leverage: Verified::new(LEVERAGE_VERIFIED_TTL),
        };
        broker
            .markets_cache
            .put(vec![market(41, "LIT"), market(1, "BTC")]);
        (broker, TempJournal(path))
    }

    const FAST_POLL: Duration = Duration::from_millis(1);

    /// 补前 `margin` 的 LIT 逐仓空腿；`reads` 是第 2 次起读回时依次看到的保证金（最后一个重复）。
    async fn lit_leg(
        margin: &str,
        reads: &[&str],
        post: Vec<(u16, Value)>,
    ) -> (FakeArcus, ArcusBroker, TempJournal) {
        let mut positions = vec![(
            200,
            lit_positions(Some("ISOLATED"), Some(margin), "-751.99"),
        )];
        positions.extend(
            reads
                .iter()
                .map(|read| (200, lit_positions(Some("ISOLATED"), Some(read), "-751.99"))),
        );
        let fake = FakeArcus::start(vec![
            ("GET", "/v1/positions", positions),
            ("POST", MARGIN_PATH, post),
        ])
        .await;
        let (broker, journal) = margin_broker(&fake.base_url, true);
        (fake, broker, journal)
    }

    fn lit() -> Symbol {
        market(41, "LIT").symbol()
    }

    fn engine_reply(status: &str, amount: &str) -> Value {
        json!({"requestId": "6f1d", "address": MARGIN_ADDRESS, "accountIndex": 3, "marketId": 41,
               "amount": amount, "newIsolatedMarginQuoteBalance": "0", "status": status})
    }

    #[tokio::test]
    async fn a_read_only_broker_never_reads_or_signs_a_margin_request() {
        let fake = FakeArcus::start(vec![]).await;
        let (broker, _journal) = margin_broker(&fake.base_url, false);
        assert!(broker.supports_add_margin());
        let error = broker
            .add_margin(&lit(), dec!(25))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("下单未开启"), "{error}");
        assert_eq!(fake.requests(), 0, "只读模式不能发出任何请求");
    }

    #[tokio::test]
    async fn an_unusable_amount_or_leg_is_an_error_before_anything_is_posted() {
        let (fake, broker, _journal) =
            lit_leg("1000", &[], vec![(200, engine_reply("APPLIED", "25"))]).await;
        for amount in [Decimal::ZERO, dec!(-5), dec!(12.345)] {
            assert!(broker.add_margin(&lit(), amount).await.is_err(), "{amount}");
        }
        assert_eq!(fake.requests(), 0, "金额不合法时连读都不读");
        // 没有持仓的标的（BTC）。
        let error = broker
            .add_margin(&market(1, "BTC").symbol(), dec!(25))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("没有持仓"), "{error}");
        assert!(fake.seen("POST").is_empty());

        // 全仓腿、没给保证金模式的腿、没有保证金读数的腿：都不发。
        for (mode, margin, expected) in [
            (Some("CROSS"), Some("1000"), "CROSS"),
            (None, Some("1000"), "保证金模式"),
            (Some("ISOLATED"), None, "marginUsed"),
        ] {
            let fake = FakeArcus::start(vec![
                (
                    "GET",
                    "/v1/positions",
                    vec![(200, lit_positions(mode, margin, "-751.99"))],
                ),
                (
                    "POST",
                    MARGIN_PATH,
                    vec![(200, engine_reply("APPLIED", "25"))],
                ),
            ])
            .await;
            let (broker, _journal) = margin_broker(&fake.base_url, true);
            let error = broker
                .add_margin(&lit(), dec!(25))
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
            assert!(fake.seen("POST").is_empty(), "{mode:?}");
        }
    }

    #[tokio::test]
    async fn a_synchronously_confirmed_add_is_one_documented_signed_post() {
        let (fake, broker, _journal) =
            lit_leg("1000", &[], vec![(200, engine_reply("APPLIED", "25"))]).await;
        let outcome = broker
            .add_margin_polled(&lit(), dec!(25.00), FAST_POLL)
            .await;
        assert_eq!(outcome.unwrap(), MarginOutcome::Applied);

        // 200 APPLIED 是引擎的确认：一次读（补前）、一次写，没有读回。
        let reads = fake.seen("GET");
        assert_eq!(reads.len(), 1);
        assert!(
            reads[0].target.contains("accountIndex=3"),
            "{}",
            reads[0].target
        );
        let posts = fake.seen("POST");
        assert_eq!(posts.len(), 1);
        let post = &posts[0];
        assert_eq!(
            post.target,
            format!("{MARGIN_PATH}?address={MARGIN_ADDRESS}")
        );
        assert_eq!(post.headers["content-type"], "application/json");
        assert_eq!(post.headers["x-api-key"], RFC_PUBLIC);
        assert_eq!(
            post.body,
            r#"{"accountIndex":3,"address":"0xabcd000000000000000000000000000000000001","amount":"25","marketId":41}"#
        );
        // 头里的时间戳就是签名消息里的时间戳；签名能被这把 key 的公钥验过。
        let timestamp: i64 = post.headers["x-timestamp"].parse().unwrap();
        let message = format!("{timestamp}adjustIsolatedMargin{}", post.body);
        let signature =
            Signature::from_bytes(&decode_hex::<64>(&post.headers["x-signature"]).unwrap());
        assert!(
            rfc_key()
                .verifying_key()
                .verify(message.as_bytes(), &signature)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn an_acknowledged_add_is_applied_only_once_the_margin_reads_back_up() {
        // 补前 1000；读回依次看到 1000、1000、1024.8（≥ 1000 + 25×0.99）。
        let (fake, broker, _journal) = lit_leg(
            "1000",
            &["1000", "1000", "1024.8"],
            vec![(202, engine_reply("ACK", "25"))],
        )
        .await;
        let outcome = broker.add_margin_polled(&lit(), dec!(25), FAST_POLL).await;
        assert_eq!(outcome.unwrap(), MarginOutcome::Applied);
        assert_eq!(fake.seen("GET").len(), 4, "补前 1 次 + 读回 3 次");
        assert_eq!(fake.seen("POST").len(), 1, "绝不重发");

        // 200 但没有 status=APPLIED，同样走读回。
        let (fake, broker, _journal) =
            lit_leg("1000", &["1030"], vec![(200, engine_reply("ACK", "25"))]).await;
        let outcome = broker.add_margin_polled(&lit(), dec!(25), FAST_POLL).await;
        assert_eq!(outcome.unwrap(), MarginOutcome::Applied);
        assert_eq!(fake.seen("POST").len(), 1);
    }

    #[tokio::test]
    async fn an_acknowledged_add_that_never_shows_up_is_unknown_and_never_resent() {
        for reads in [&["1000"][..], &["1000", "900"][..]] {
            let (fake, broker, _journal) =
                lit_leg("1000", reads, vec![(202, engine_reply("ACK", "25"))]).await;
            let outcome = broker
                .add_margin_polled(&lit(), dec!(25), FAST_POLL)
                .await
                .unwrap();
            let MarginOutcome::Unknown(reason) = outcome else {
                panic!("{outcome:?}")
            };
            assert!(
                reason.contains("202") && reason.contains("不要重发"),
                "{reason}"
            );
            assert_eq!(fake.seen("GET").len(), 1 + MARGIN_POLLS);
            assert_eq!(fake.seen("POST").len(), 1);
        }
    }

    #[tokio::test]
    async fn an_ack_with_a_changed_position_cannot_confirm_a_margin_add() {
        for (field, value) in [("size", "-800"), ("averageEntryPrice", "4.5")] {
            let mut after = lit_positions(Some("ISOLATED"), Some("1100"), "-751.99");
            after["positions"]["41"][field] = json!(value);
            let fake = FakeArcus::start(vec![
                (
                    "GET",
                    "/v1/positions",
                    vec![
                        (
                            200,
                            lit_positions(Some("ISOLATED"), Some("1000"), "-751.99"),
                        ),
                        (200, after),
                    ],
                ),
                ("POST", MARGIN_PATH, vec![(202, engine_reply("ACK", "25"))]),
            ])
            .await;
            let (broker, _journal) = margin_broker(&fake.base_url, true);
            let outcome = broker
                .add_margin_polled(&lit(), dec!(25), FAST_POLL)
                .await
                .unwrap();
            let MarginOutcome::Unknown(reason) = outcome else {
                panic!("{field}: expected Unknown, got {outcome:?}");
            };
            assert!(reason.contains("无法归因"), "{reason}");
            assert_eq!(fake.seen("POST").len(), 1);
            assert_eq!(fake.seen("GET").len(), 2);
        }
    }

    #[tokio::test]
    async fn a_failing_readback_after_an_ack_is_unknown_not_an_error() {
        let fake = FakeArcus::start(vec![
            (
                "GET",
                "/v1/positions",
                vec![
                    (
                        200,
                        lit_positions(Some("ISOLATED"), Some("1000"), "-751.99"),
                    ),
                    (503, json!({"error": "unavailable"})),
                ],
            ),
            ("POST", MARGIN_PATH, vec![(202, engine_reply("ACK", "25"))]),
        ])
        .await;
        let (broker, _journal) = margin_broker(&fake.base_url, true);
        let outcome = broker
            .add_margin_polled(&lit(), dec!(25), FAST_POLL)
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("{outcome:?}")
        };
        assert!(reason.contains("读回失败"), "{reason}");
        assert_eq!(fake.seen("POST").len(), 1);
    }

    #[tokio::test]
    async fn refusals_and_unknown_replies_are_returned_without_a_second_post() {
        let rejected = |status: &str, reason: &str| {
            let mut body = engine_reply(status, "25");
            body["rejectReason"] = json!(reason);
            body
        };
        let cases: Vec<(u16, Value, bool)> = vec![
            (422, rejected("REJECTED", "UNDERCOLLATERALIZED"), true),
            (422, rejected("REJECTED", "NO_OPEN_POSITION"), true),
            (422, rejected("REJECTED", "MISSING_MARK_PRICE"), true),
            // HTTP 状态与生命周期冲突 / 缺失，不能声称「没动钱」让调用方再次补仓。
            (422, rejected("APPLIED", "UNDERCOLLATERALIZED"), false),
            (422, rejected("ACK", "UNDERCOLLATERALIZED"), false),
            (422, json!({"rejectReason": "UNDERCOLLATERALIZED"}), false),
            (
                429,
                json!({"error": "rate limited", "retryAfterMs": 900}),
                true,
            ),
            (401, json!({"error": "invalid signature"}), true),
            (403, json!({"error": "address mismatch"}), true),
            (400, json!({"error": "invalid amount"}), true),
            (
                500,
                json!({"error": "internal", "errorType": "Internal"}),
                false,
            ),
            (502, Value::Null, false),
            (503, json!({"errorType": "Transmission"}), false),
            (504, Value::Null, false),
        ];
        for (status, body, refused) in cases {
            let (fake, broker, _journal) = lit_leg("1000", &[], vec![(status, body)]).await;
            let outcome = broker
                .add_margin_polled(&lit(), dec!(25), FAST_POLL)
                .await
                .unwrap();
            match (&outcome, refused) {
                (MarginOutcome::Refused(_), true) | (MarginOutcome::Unknown(_), false) => {}
                _ => panic!("HTTP {status}: {outcome:?}"),
            }
            assert_eq!(fake.seen("POST").len(), 1, "HTTP {status}：不重发");
            assert_eq!(fake.seen("GET").len(), 1, "HTTP {status}：不读回");
        }
    }

    #[tokio::test]
    async fn a_connection_dropped_after_the_request_is_unknown_not_an_error() {
        // 服务器收完请求就断开：写请求可能已经被处理，只能报「结果不明」，不能重发。
        let (fake, broker, _journal) = lit_leg("1000", &[], vec![(0, Value::Null)]).await;
        let outcome = broker
            .add_margin_polled(&lit(), dec!(25), FAST_POLL)
            .await
            .unwrap();
        let MarginOutcome::Unknown(reason) = outcome else {
            panic!("{outcome:?}")
        };
        assert!(
            reason.contains(MARGIN_PATH) && reason.contains("可能已到账"),
            "{reason}"
        );
        assert!(!reason.contains(MARGIN_ADDRESS), "错误文本不带地址");
        assert_eq!(fake.seen("POST").len(), 1);
    }
}
