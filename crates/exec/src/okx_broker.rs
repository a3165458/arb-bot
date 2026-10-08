//! 生产用 OKX v5 **USDT 保证金线性永续**券商（`https://www.okx.com`）。
//!
//! 只用官方 v5 REST 私有接口，范围限定为 `instType=SWAP` 的 USDT 线性永续：
//! `ctType=linear`、`settleCcy=USDT`、`ctValCcy` 等于标的币、`state=live`。现货、
//! 交割、期权、反向合约、USDC 保证金、事件合约一律不在范围内，遇到时跳过而不是猜测。
//!
//! ## 硬性规则
//!
//! - **每笔订单都是限价 IOC**（`ordType=ioc` + `px`）：不存在的“无界市价单”不允许下；
//!   无限价时用下单前**刚拉取**的盘口按 `market_slippage` 定价，盘口过旧/为空/交叉即拒绝。
//! - **所选保证金模式 + 显式杠杆**：开仓前用 `mgnMode=isolated|cross` 设置该合约杠杆并**读回确认**，
//!   只改这一个合约的设置，绝不触碰账户级设置（持仓模式、账户模式）。
//! - **数量以张计**：`order_units(unit=ctVal, step=lotSz, min=minSz)`，开仓向下取整、
//!   reduce-only 必须是步长整数倍。
//! - **先落盘再发单**：`OrderJournal` 在任何写请求之前 fsync；登记过的 `clOrdId` 永不重发。
//! - **手续费取实际成交**：`filled_usdt`/`average_price`/`fee_usdt` 全部来自
//!   `fills`/`fills-history` 的逐笔成交，OKX 的 `fee` 负数表示费用，这里统一成
//!   正数表示成本；`feeCcy` 不是 USDT 或明细数量对不上订单累计成交量即报错。
//! - 传输层错误一律经 `transport_error` 去掉 URL；业务错误只带 `code`/`msg`。
//!
//! ## 官方文档来源
//!
//! - 签名与请求头：<https://www.okx.com/docs-v5/en/#overview-rest-authentication>
//! - 账户配置（`posMode`/`acctLv`）：<https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-account-configuration>
//! - 设置杠杆：<https://www.okx.com/docs-v5/en/#trading-account-rest-api-set-leverage>
//! - 费率：<https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-fee-rates>
//! - 合约规格：<https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments>
//! - 下单：<https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order>
//! - 订单详情 / 未结订单：<https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details>
//! - 成交明细：<https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-transaction-details>
//! - 持仓：<https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-positions>
//! - 服务器时间：<https://www.okx.com/docs-v5/en/#public-data-rest-api-get-system-time>
//! - 盘口：<https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-order-book>
//!
//! ## 运营方必须先在网页端配好的账户设置
//!
//! 本程序**不会**代改账户级设置。`connect` 会校验并要求：
//!
//! 1. 持仓模式为**单向持仓** `posMode=net_mode`（双向 long/short 模式无法表达本系统的净头寸）。
//! 2. 账户模式为**合约模式** `acctLv=2` 或**跨币种保证金模式** `acctLv=3`（组合保证金/现货模式
//!    不支持逐仓 SWAP）。
//! 3. API key 具备 **`trade`** 权限（否则下单会被拒）。

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use arb_core::{ArbError, ArbResult, Decimal, Side, Symbol, Venue};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::broker::{Broker, VenuePosition};
use crate::live_common::{
    JournalEntry, LiveOptions, OrderJournal, base64_standard, hmac_sha256, order_units,
    round_price, transport_error, venue_client_id,
};
use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState, OrderStatus};

const BASE_URL: &str = "https://www.okx.com";
const VENUE: &str = "okx";
/// 客户订单号前缀；`clOrdId` 只允许字母数字，长度至多 32。
const CLIENT_ID_PREFIX: &str = "okx";
const CLIENT_ID_MAX: usize = 32;

/// OKX v5 凭据。字段是密钥：**不要**给它加 `Debug`/`Serialize`，也不要把值放进错误串。
pub struct OkxCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

/// OKX v5 USDT 线性永续券商。默认禁止一切写操作，必须显式开启。
pub struct OkxBroker {
    client: Client,
    credentials: OkxCredentials,
    options: LiveOptions,
    /// 订单意图日志：发单前落盘、单账户独占锁。
    journal: Mutex<OrderJournal>,
    /// 账户实际吃单费率（正数 = 成本，已把 OKX 的负数费用口径转换过来）。
    taker_fee: Decimal,
    /// 本地时钟相对交易所服务器的毫秒偏移（服务器 − 本地）。签名时间戳带偏移。
    time_offset_ms: i64,
}

impl OkxBroker {
    /// 只读构造：校验凭据、账户模式与持仓模式、抓取真实吃单费率。构造期间**不发任何写请求**。
    ///
    /// 日志路径必须是该账户独占的持久化路径。持仓模式或账户模式不兼容时直接失败并给出
    /// 可执行的说明 —— 账户级设置只能由运营方在网页端改。
    pub async fn connect(
        client: Client,
        credentials: OkxCredentials,
        journal_path: &Path,
        options: LiveOptions,
    ) -> ArbResult<Self> {
        options.validate(Venue::Okx)?;
        if credentials.api_key.is_empty()
            || credentials.api_secret.is_empty()
            || credentials.passphrase.is_empty()
        {
            return Err(error("API key、secret 与 passphrase 都必须提供"));
        }
        let identity = format!(
            "okx:{}",
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
        };
        broker.time_offset_ms = broker.sync_time().await?;

        let configs: Vec<AccountConfig> = broker
            .get_signed("/api/v5/account/config", "account/config")
            .await?;
        let account = configs
            .into_iter()
            .next()
            .ok_or_else(|| error("账户配置响应为空"))?;
        if account.pos_mode != "net_mode" {
            return Err(error(
                "账户持仓模式必须是单向持仓 net_mode；请在网页端切换到单向持仓后重启，本程序不会代改账户级设置",
            ));
        }
        if !matches!(account.acct_lv.as_str(), "2" | "3") {
            return Err(error(
                "账户模式不支持逐仓 SWAP：需要「合约模式」(acctLv=2) 或「跨币种保证金模式」(acctLv=3)；现货模式与组合保证金模式不可用",
            ));
        }
        if !account.perms().any(|perm| perm == "trade") {
            return Err(error("API key 缺少 trade 权限，无法下单"));
        }
        broker.taker_fee = broker.taker_rate().await?;
        Ok(broker)
    }

    /// 读服务器时间，算出本地时钟偏移。OKX 要求签名时间戳与服务器相差不超过 30s。
    async fn sync_time(&self) -> ArbResult<i64> {
        let rows: Vec<ServerTime> = self
            .get_public("/api/v5/public/time", "public/time")
            .await?;
        let raw = rows
            .into_iter()
            .next()
            .ok_or_else(|| error("服务器时间响应为空"))?
            .ts;
        let server = raw
            .parse::<i64>()
            .map_err(|_| error("服务器时间无法解析"))?;
        Ok(server - now_ms()?)
    }

    /// 账户实际吃单费率。SWAP 走 `feeGroup[].taker`；没有分组时退回顶层 `taker`。
    ///
    /// 多个分组时取最大者，宁可高估成本也不低估（同一账户各标准永续分组费率通常一致）。
    async fn taker_rate(&self) -> ArbResult<Decimal> {
        let rows: Vec<TradeFee> = self
            .get_signed(
                "/api/v5/account/trade-fee?instType=SWAP",
                "account/trade-fee",
            )
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| error("费率响应为空"))?;
        let mut best: Option<Decimal> = None;
        for group in &row.fee_group {
            if group.taker.is_empty() {
                continue;
            }
            let rate = parse_decimal(&group.taker)?;
            best = Some(best.map_or(rate, |current| current.max(rate)));
        }
        let raw = match best {
            Some(rate) => rate,
            None if row.taker.is_empty() => {
                return Err(error("拿不到 USDT 永续的 taker 费率，拒绝猜测"));
            }
            None => parse_decimal(&row.taker)?,
        };
        if raw.abs() >= Decimal::ONE {
            return Err(error("taker 费率超出合理范围"));
        }
        // OKX 的口径：负数=手续费支出。这里统一成正数=成本。
        Ok(-raw)
    }

    /// 签名：`Base64(HMAC-SHA256(secret, timestamp + method + requestPath + body))`。
    fn sign(&self, timestamp: &str, method: &str, path: &str, body: &str) -> String {
        signature(&self.credentials.api_secret, timestamp, method, path, body)
    }

    /// 发一个请求并解出完整信封（HTTP 状态非 2xx 与传输层错误在这里已经上抛）。
    ///
    /// 业务 `code` **不在这里**判定：单笔下单体 HTTP 200 也可能返回
    /// `{"code":"1","data":[{"sCode":"51008",...}],"msg":"All operations failed"}`，
    /// 真正的订单级原因在 `data[0].sCode`，必须由调用方按错误码表区分明确拒绝与结果未知。
    async fn send_envelope<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        endpoint: &str,
        signed: bool,
    ) -> ArbResult<Envelope<T>> {
        let url = format!("{BASE_URL}{path}");
        let mut request = match method {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url),
            _ => return Err(error("不支持的 HTTP 方法")),
        };
        let body_text = body.unwrap_or("");
        if signed {
            let timestamp = iso_ms(now_ms()? + self.time_offset_ms)?;
            let signed_value = self.sign(&timestamp, method, path, body_text);
            request = request
                .header("OK-ACCESS-KEY", self.credentials.api_key.as_str())
                .header("OK-ACCESS-SIGN", signed_value)
                .header("OK-ACCESS-TIMESTAMP", timestamp)
                .header("OK-ACCESS-PASSPHRASE", self.credentials.passphrase.as_str());
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }
        let response = request
            .send()
            .await
            .map_err(|e| transport_error(Venue::Okx, endpoint, e))?;
        if !response.status().is_success() {
            // 不回显响应体：它可能包含被签名的请求数据。
            return Err(error(format!(
                "{endpoint} HTTP {}",
                response.status().as_u16()
            )));
        }
        let text = response
            .text()
            .await
            .map_err(|e| transport_error(Venue::Okx, endpoint, e))?;
        serde_json::from_str(&text).map_err(|_| error("响应不是合法的 OKX JSON"))
    }

    /// 发一个请求并解出 `data` 数组。请求级 `code != "0"` 视为业务错误。
    ///
    /// 需要看 `data[0].sCode` 才能判定的单笔下单 / 撤单端点请直接用 [`Self::send_envelope`]。
    async fn send<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        endpoint: &str,
        signed: bool,
    ) -> ArbResult<Vec<T>> {
        let envelope = self
            .send_envelope::<T>(method, path, body, endpoint, signed)
            .await?;
        if envelope.code != "0" {
            return Err(envelope_error(endpoint, &envelope));
        }
        Ok(envelope.data.unwrap_or_default())
    }

    async fn get_signed<T: DeserializeOwned>(
        &self,
        path: &str,
        endpoint: &str,
    ) -> ArbResult<Vec<T>> {
        self.send("GET", path, None, endpoint, true).await
    }

    async fn post_signed<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &str,
        endpoint: &str,
    ) -> ArbResult<Vec<T>> {
        self.send("POST", path, Some(body), endpoint, true).await
    }

    async fn get_public<T: DeserializeOwned>(
        &self,
        path: &str,
        endpoint: &str,
    ) -> ArbResult<Vec<T>> {
        self.send("GET", path, None, endpoint, false).await
    }

    /// 把所有在架 SWAP 元数据拉一份，筛掉非 USDT 线性永续。
    async fn swap_instruments(&self) -> ArbResult<Vec<Instrument>> {
        let rows: Vec<Instrument> = self
            .get_public(
                "/api/v5/public/instruments?instType=SWAP",
                "public/instruments",
            )
            .await?;
        Ok(rows.into_iter().filter(is_usdt_linear).collect())
    }

    /// 按 `Symbol` 定位唯一在架合约：靠场所字段（`ctType`/`settleCcy`/`ctValCcy`/`state`）
    /// 与合约名结构共同匹配，不靠字符串猜测。
    async fn instrument(&self, symbol: &Symbol) -> ArbResult<Instrument> {
        let mut found: Option<Instrument> = None;
        for row in self.swap_instruments().await? {
            let Some(base) = usdt_swap_base(&row.inst_id) else {
                continue;
            };
            if row.state != "live" || row.ct_val_ccy != base {
                continue;
            }
            if Symbol::perp(base, "USDT") != *symbol {
                continue;
            }
            if found.is_some() {
                return Err(error("同一个标的匹配到多个在架 USDT 线性永续合约"));
            }
            found = Some(row);
        }
        found.ok_or_else(|| error("找不到该标的对应的在架 USDT 线性永续合约"))
    }

    /// `instId -> 合约面值`。只含 USDT 线性永续。
    async fn instruments_map(&self) -> ArbResult<HashMap<String, Decimal>> {
        let mut map = HashMap::new();
        for row in self.swap_instruments().await? {
            let Some(base) = usdt_swap_base(&row.inst_id) else {
                continue;
            };
            if row.ct_val_ccy != base {
                continue;
            }
            if let Ok(value) = parse_decimal(&row.ct_val)
                && value > Decimal::ZERO
            {
                map.insert(row.inst_id.clone(), value);
            }
        }
        Ok(map)
    }

    async fn ct_val_for(&self, inst_id: &str) -> ArbResult<Decimal> {
        self.instruments_map()
            .await?
            .get(inst_id)
            .copied()
            .ok_or_else(|| error("找不到该合约的 USDT 线性永续元数据"))
    }

    /// 下单前用**刚拉取**的盘口按 slippage 定价。
    async fn bound_price(&self, instrument: &Instrument, side: Side) -> ArbResult<Decimal> {
        let path = format!("/api/v5/market/books?instId={}&sz=1", instrument.inst_id);
        let rows: Vec<Book> = self.get_public(&path, "market/books").await?;
        let book = rows.into_iter().next().ok_or_else(|| error("盘口为空"))?;
        let ts = book
            .ts
            .parse::<i64>()
            .map_err(|_| error("盘口时间无法解析"))?;
        let now = now_ms()?;
        if ts > now + 1000 || now.saturating_sub(ts) > 5000 {
            return Err(error("盘口过旧，拒绝定价"));
        }
        let best_bid = first_price(&book.bids)?;
        let best_ask = first_price(&book.asks)?;
        self.options
            .bound_price(Venue::Okx, side, best_bid, best_ask)
    }

    /// 设置该合约所选模式的杠杆并读回确认。只改这一个合约。
    async fn set_leverage(
        &self,
        inst_id: &str,
        leverage: Decimal,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let body = serde_json::json!({
            "instId": inst_id,
            "lever": leverage.normalize().to_string(),
            "mgnMode": mode.as_str(),
        })
        .to_string();
        let rows: Vec<LeverageAck> = self
            .post_signed(
                "/api/v5/account/set-leverage",
                &body,
                "account/set-leverage",
            )
            .await?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| error("设置杠杆响应为空"))?;
        let applied = parse_decimal(&row.lever)?;
        if row.mgn_mode != mode.as_str() || applied != leverage {
            return Err(error("所选保证金模式/杠杆未被交易所确认"));
        }
        Ok(())
    }

    /// 按客户订单号查订单；查不到返回 `None`。查到终态则写入日志。
    async fn query_state(
        &self,
        requested: Option<&ClientOrderId>,
        cl_ord_id: &str,
        inst_id: &str,
        ct_val: Decimal,
    ) -> ArbResult<Option<OrderState>> {
        let has_entry = {
            let journal = self.journal.lock().await;
            journal.by_venue_client_id(cl_ord_id).is_some()
        };
        let path = format!("/api/v5/trade/order?instId={inst_id}&clOrdId={cl_ord_id}");
        let rows: Vec<RemoteOrder> = self.get_signed(&path, "trade/order").await?;
        let Some(remote) = rows.into_iter().next() else {
            return Ok(None);
        };
        let state = self.state_from_remote(&remote, requested, ct_val).await?;
        if has_entry && !state.status.is_live() {
            self.journal.lock().await.record_terminal(&state)?;
        }
        Ok(Some(state))
    }

    /// 把交易所订单还原成 [`OrderState`]。成交量/名义额/手续费全部取自逐笔成交。
    async fn state_from_remote(
        &self,
        remote: &RemoteOrder,
        requested: Option<&ClientOrderId>,
        ct_val: Decimal,
    ) -> ArbResult<OrderState> {
        if remote.ord_id.is_empty() {
            return Err(error("交易所订单缺少订单号"));
        }
        let symbol = symbol_of_inst(&remote.inst_id)?;
        let side = parse_side(&remote.side)?;
        let requested_sz = parse_positive(&remote.sz)?;
        let filled_sz = parse_decimal(&remote.acc_fill_sz)?;
        if filled_sz < Decimal::ZERO || filled_sz > requested_sz {
            return Err(error("订单累计成交量非法"));
        }
        let reduce_only = remote.reduce_only == "true";
        let entry = if remote.cl_ord_id.is_empty() {
            None
        } else {
            let journal = self.journal.lock().await;
            journal.by_venue_client_id(&remote.cl_ord_id).cloned()
        };
        let order = match (&entry, requested) {
            (Some(entry), Some(id)) => {
                if entry.order.client_order_id != *id {
                    return Err(error("交易所订单与本地意图不一致"));
                }
                check_identity(&entry.order, symbol.clone(), side, reduce_only)?;
                entry.order.clone()
            }
            (Some(entry), None) => {
                check_identity(&entry.order, symbol.clone(), side, reduce_only)?;
                entry.order.clone()
            }
            (None, _) => {
                // 外部订单：只能按交易所字段重建意图，用合成订单号。
                let price = if !remote.px.is_empty() {
                    parse_positive(&remote.px)?
                } else if !remote.avg_px.is_empty() {
                    parse_positive(&remote.avg_px)?
                } else {
                    Decimal::ZERO
                };
                let base = requested_sz
                    .checked_mul(ct_val)
                    .ok_or_else(|| error("数量换算溢出"))?;
                let limit_price = (!remote.px.is_empty())
                    .then(|| parse_decimal(&remote.px))
                    .transpose()?;
                let leverage = if remote.lever.is_empty() {
                    None
                } else {
                    Some(parse_decimal(&remote.lever)?)
                };
                NewOrder {
                    margin_mode: crate::MarginMode::Isolated,
                    client_order_id: ClientOrderId(format!("okx-external-{}", remote.ord_id)),
                    venue: Venue::Okx,
                    symbol,
                    side,
                    notional_usdt: base
                        .checked_mul(price)
                        .ok_or_else(|| error("名义额换算溢出"))?,
                    quantity: Some(base),
                    limit_price,
                    reduce_only,
                    leverage,
                }
            }
        };
        let status = verified_status(map_status(&remote.state)?, requested_sz, filled_sz)?;
        let fills = self.fetch_fills(&remote.inst_id, &remote.ord_id).await?;
        let (base_qty, notional, fee) =
            aggregate_fills(&remote.inst_id, &remote.ord_id, ct_val, filled_sz, &fills)?;
        let mut state = OrderState::new(order);
        state.venue_order_id = Some(remote.ord_id.clone());
        state.status = status;
        state.filled_usdt = notional;
        state.average_price = if base_qty > Decimal::ZERO {
            Some(
                notional
                    .checked_div(base_qty)
                    .ok_or_else(|| error("均价计算溢出"))?,
            )
        } else {
            None
        };
        state.fee_usdt = fee;
        Ok(state)
    }

    /// 拉某笔订单的逐笔成交。先查 3 个月窗口，为空且有成交时再查 3 天窗口。
    async fn fetch_fills(&self, inst_id: &str, ord_id: &str) -> ArbResult<Vec<RemoteFill>> {
        let mut fills = self.paginate_fills("fills-history", ord_id).await?;
        if fills.is_empty() {
            fills = self.paginate_fills("fills", ord_id).await?;
        }
        for fill in &fills {
            if fill.inst_id != inst_id {
                return Err(error("成交明细的合约与订单不一致"));
            }
        }
        Ok(fills)
    }

    async fn paginate_fills(&self, endpoint: &str, ord_id: &str) -> ArbResult<Vec<RemoteFill>> {
        let mut result = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..64 {
            let mut path =
                format!("/api/v5/trade/{endpoint}?instType=SWAP&ordId={ord_id}&limit=100");
            if let Some(after) = &after {
                path.push_str(&format!("&after={after}"));
            }
            let rows: Vec<RemoteFill> = self.get_signed(&path, endpoint).await?;
            let count = rows.len();
            if count == 0 {
                break;
            }
            let last = rows.iter().rev().find(|row| !row.bill_id.is_empty());
            let last_id = last.map(|row| row.bill_id.clone());
            result.extend(rows);
            match last_id {
                Some(id) if count == 100 => after = Some(id),
                _ => break,
            }
        }
        Ok(result)
    }

    async fn order_state_impl(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        let cl_ord_id = venue_client_id(CLIENT_ID_PREFIX, id, CLIENT_ID_MAX);
        let entry = {
            let journal = self.journal.lock().await;
            journal.by_venue_client_id(&cl_ord_id).cloned()
        };
        let Some(entry) = entry else {
            // 从未登记过 —— 确定从未提交。
            return Ok(None);
        };
        if let Some(terminal) = entry.terminal {
            return Ok(Some(terminal));
        }
        let ct_val = self.ct_val_for(&entry.instrument).await?;
        match self
            .query_state(Some(id), &cl_ord_id, &entry.instrument, ct_val)
            .await?
        {
            Some(state) => Ok(Some(state)),
            None => Err(error(
                "订单登记过但在交易所查不到，不能当成从未提交，请人工对账",
            )),
        }
    }
}

#[async_trait]
impl Broker for OkxBroker {
    async fn leg_state(&self, symbol: &Symbol) -> ArbResult<Option<crate::VenueLegState>> {
        let instrument = self.instrument(symbol).await?;
        let rows: Vec<RemotePosition> = self
            .get_signed(
                &format!("/api/v5/account/positions?instId={}", instrument.inst_id),
                "account/positions",
            )
            .await?;
        let Some(row) = rows.iter().find(|row| {
            row.inst_id == instrument.inst_id
                && parse_decimal(&row.pos).is_ok_and(|q| q != Decimal::ZERO)
        }) else {
            return Ok(None);
        };
        let parse =
            |value: &Option<String>| value.as_deref().and_then(|raw| parse_decimal(raw).ok());
        Ok(Some(crate::margin::venue_state(
            crate::margin::reported_mode(&row.mgn_mode),
            parse(&row.liq_px),
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
        self.options.authorize(Venue::Okx)?;
        let instrument = self.instrument(symbol).await?;
        let leverage = crate::margin::leverage(Venue::Okx, leverage)?;
        if instrument.lever.is_empty() || leverage > parse_positive(&instrument.lever)? {
            return Err(error("leverage exceeds market limit"));
        }
        self.set_leverage(&instrument.inst_id, leverage, mode).await
    }

    fn venue(&self) -> Venue {
        Venue::Okx
    }

    fn fee_per_side(&self) -> Decimal {
        self.taker_fee
    }

    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        self.options.authorize(Venue::Okx)?;
        if order.venue != Venue::Okx || order.client_order_id.0.is_empty() {
            return Err(error("下单请求的场所不对或客户订单号为空"));
        }
        let cl_ord_id = venue_client_id(CLIENT_ID_PREFIX, &order.client_order_id, CLIENT_ID_MAX);

        // 幂等：同一个客户订单号重复提交绝不重发。
        let existing = {
            let journal = self.journal.lock().await;
            journal.by_venue_client_id(&cl_ord_id).cloned()
        };
        if let Some(entry) = existing {
            if !same_intent(&entry.order, order) {
                return Err(error("客户订单号被复用且意图不同"));
            }
            if let Some(terminal) = entry.terminal {
                return ack(&terminal);
            }
            let ct_val = self.ct_val_for(&entry.instrument).await?;
            return match self
                .query_state(
                    Some(&order.client_order_id),
                    &cl_ord_id,
                    &entry.instrument,
                    ct_val,
                )
                .await?
            {
                Some(state) => ack(&state),
                None => Err(error("已登记的订单查不到，停止并人工对账")),
            };
        }

        let instrument = self.instrument(&order.symbol).await?;
        let ct_val = parse_positive(&instrument.ct_val)?;
        let lot = parse_positive(&instrument.lot_sz)?;
        let min = parse_positive(&instrument.min_sz)?;
        let tick = parse_positive(&instrument.tick_sz)?;

        let leverage = if order.reduce_only {
            None
        } else {
            let value = order
                .leverage
                .ok_or_else(|| error("开仓必须给出显式杠杆"))?;
            if !value.fract().is_zero() || value < Decimal::ONE {
                return Err(error("杠杆必须是 ≥1 的整数"));
            }
            if instrument.lever.is_empty() {
                return Err(error("合约元数据缺少最大杠杆，拒绝开仓"));
            }
            if value > parse_positive(&instrument.lever)? {
                return Err(error("杠杆超过该合约上限"));
            }
            Some(value)
        };
        // 杠杆是这个合约的设置、不是订单：先设置并读回。失败时不留下永远查不到的意图，
        // 而且之后拉的盘口不会被设置请求的往返拖旧。
        if let Some(leverage) = leverage {
            self.set_leverage(&instrument.inst_id, leverage, order.margin_mode)
                .await?;
        }

        let raw_price = match order.limit_price {
            Some(price) => price,
            None => self.bound_price(&instrument, order.side).await?,
        };
        let price = round_price(raw_price, tick, order.side)
            .ok_or_else(|| error("价格按 tick 取整后非法"))?;
        let units = order_units(Venue::Okx, order, price, ct_val, lot, min)?;

        if order.reduce_only {
            let position = self
                .positions()
                .await?
                .into_iter()
                .find(|p| p.symbol == order.symbol)
                .ok_or_else(|| error("reduce-only 订单没有对应持仓"))?;
            let base = units
                .checked_mul(ct_val)
                .ok_or_else(|| error("数量换算溢出"))?;
            let closes = (position.net_quantity > Decimal::ZERO && order.side == Side::Sell)
                || (position.net_quantity < Decimal::ZERO && order.side == Side::Buy);
            if !closes || base > position.net_quantity.abs() {
                return Err(error("reduce-only 方向或数量超过当前持仓"));
            }
        }

        // 下单请求之前先落盘意图：超时或崩溃之后绝不重发。
        {
            let mut journal = self.journal.lock().await;
            journal.reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: cl_ord_id.clone(),
                instrument: instrument.inst_id.clone(),
                units,
                terminal: None,
            })?;
        }

        let body = serde_json::json!({
            "instId": instrument.inst_id,
            "tdMode": order.margin_mode.as_str(),
            "clOrdId": cl_ord_id,
            "side": if order.side == Side::Buy { "buy" } else { "sell" },
            "ordType": "ioc",
            "sz": units.normalize().to_string(),
            "px": price.normalize().to_string(),
            "reduceOnly": order.reduce_only,
        })
        .to_string();

        let envelope = match self
            .send_envelope::<PlacedOrder>(
                "POST",
                "/api/v5/trade/order",
                Some(&body),
                "trade/order",
                true,
            )
            .await
        {
            Ok(envelope) => envelope,
            Err(send_error) => {
                // 传输层 / HTTP 错误：结果未知，查一次，查到就照实返回，
                // 查不到则上抛原始错误交由执行层对账。
                return match self
                    .query_state(
                        Some(&order.client_order_id),
                        &cl_ord_id,
                        &instrument.inst_id,
                        ct_val,
                    )
                    .await?
                {
                    Some(state) => ack(&state),
                    None => Err(send_error),
                };
            }
        };

        match self
            .apply_place_response(order, &cl_ord_id, &envelope)
            .await?
        {
            OrderOutcome::Accepted => match self
                .query_state(
                    Some(&order.client_order_id),
                    &cl_ord_id,
                    &instrument.inst_id,
                    ct_val,
                )
                .await?
            {
                Some(state) => ack(&state),
                None => Err(error("订单已受理但查不到，保留意图等待对账")),
            },
            OrderOutcome::Refused { message, .. } => Err(error(message)),
            OrderOutcome::Unknown { reason } => {
                // 结果未知：查一次，查到就照实返回，查不到则上抛，由执行层对账。
                match self
                    .query_state(
                        Some(&order.client_order_id),
                        &cl_ord_id,
                        &instrument.inst_id,
                        ct_val,
                    )
                    .await?
                {
                    Some(state) => ack(&state),
                    None => Err(error(format!(
                        "下单结果未知（{reason}），保留意图等待对账，绝不重发"
                    ))),
                }
            }
        }
    }

    async fn order_state(&self, client_order_id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        self.order_state_impl(client_order_id).await
    }

    async fn cancel(&self, venue_order_id: &str) -> ArbResult<()> {
        self.options.authorize(Venue::Okx)?;
        if venue_order_id.is_empty() {
            return Err(error("撤单需要交易所订单号"));
        }
        // 必须拉全未结订单：只读默认首页（100 条）会把后面的订单当成不存在。
        let pending = self.pending_orders().await?;
        let Some(target) = pending.iter().find(|row| row.ord_id == venue_order_id) else {
            // 不在完整未结订单列表里，还要向交易所确认这笔订单确实不活跃，
            // 绝不因为列表里没有就当成已经撤掉。
            return self.confirm_inactive(None, venue_order_id).await;
        };
        let body = serde_json::json!({
            "instId": target.inst_id,
            "ordId": venue_order_id,
        })
        .to_string();
        let envelope = self
            .send_envelope::<PlacedOrder>(
                "POST",
                "/api/v5/trade/cancel-order",
                Some(&body),
                "trade/cancel-order",
                true,
            )
            .await?;
        let rows = envelope.data.as_deref().unwrap_or_default();
        match classify_cancel_response(&envelope.code, &envelope.msg, rows) {
            CancelOutcome::AlreadyInactive => Ok(()),
            // 受理、明确失败、结果未知都回到交易所查这一笔订单：仍活跃就报错，终态才算成功。
            CancelOutcome::Accepted
            | CancelOutcome::Refused { .. }
            | CancelOutcome::Unknown { .. } => {
                self.confirm_inactive(Some(&target.inst_id), venue_order_id)
                    .await
            }
        }
    }

    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        let instruments = self.instruments_map().await?;
        let mut result = Vec::new();
        for remote in self.pending_orders().await? {
            let Some(ct_val) = instruments.get(&remote.inst_id).copied() else {
                // 非 USDT 线性永续（反向/USDC/交割）不在本券商范围内。
                continue;
            };
            let state = self.state_from_remote(&remote, None, ct_val).await?;
            if state.status.is_live() {
                result.push(state);
            }
        }
        Ok(result)
    }

    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        let instruments = self.instruments_map().await?;
        let rows: Vec<RemotePosition> = self
            .get_signed(
                "/api/v5/account/positions?instType=SWAP",
                "account/positions",
            )
            .await?;
        let mut result = Vec::new();
        for row in rows {
            let Some(ct_val) = instruments.get(&row.inst_id).copied() else {
                continue;
            };
            if let Some(position) = venue_position(ct_val, &row)? {
                result.push(position);
            }
        }
        Ok(result)
    }
}

impl OkxBroker {
    /// 依据单笔下单响应决定结果；交易所明确拒绝时把终态 `Rejected` 写进日志，
    /// 这样 `order_state` 能把它返回给执行层（执行层据此记为拒单而不是未知）。
    async fn apply_place_response(
        &self,
        order: &NewOrder,
        cl_ord_id: &str,
        envelope: &Envelope<PlacedOrder>,
    ) -> ArbResult<OrderOutcome> {
        let rows = envelope.data.as_deref().unwrap_or_default();
        match classify_order_response(&envelope.code, &envelope.msg, rows) {
            OrderOutcome::Accepted => {
                let row = rows.first().ok_or_else(|| error("下单响应为空"))?;
                if !row.cl_ord_id.is_empty() && row.cl_ord_id != cl_ord_id {
                    return Err(error("下单回执的客户订单号与请求不符，停止并人工对账"));
                }
                Ok(OrderOutcome::Accepted)
            }
            OrderOutcome::Refused { code, message } => {
                let reason = if message.is_empty() {
                    format!("下单被拒绝 sCode={code}")
                } else {
                    format!("下单被拒绝 sCode={code} {message}")
                };
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.venue_order_id = rows
                    .first()
                    .map(|row| row.ord_id.clone())
                    .filter(|id| !id.is_empty());
                rejected.reject_reason = Some(reason.clone());
                self.journal.lock().await.record_terminal(&rejected)?;
                Ok(OrderOutcome::Refused {
                    code,
                    message: reason,
                })
            }
            OrderOutcome::Unknown { reason } => Ok(OrderOutcome::Unknown { reason }),
        }
    }

    /// 全部未结订单。默认首页只有 100 条，必须用 `after` 翻页直到出现不满页的一页，
    /// 否则第二页之后的订单会被误判成「不存在」。
    async fn pending_orders(&self) -> ArbResult<Vec<RemoteOrder>> {
        let mut result = Vec::new();
        let mut after: Option<String> = None;
        for _ in 0..64 {
            let mut path = String::from("/api/v5/trade/orders-pending?instType=SWAP&limit=100");
            if let Some(after) = &after {
                path.push_str(&format!("&after={after}"));
            }
            let rows: Vec<RemoteOrder> = self.get_signed(&path, "trade/orders-pending").await?;
            let next = next_after(&rows, 100);
            result.extend(rows);
            match next {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(result)
    }

    /// 按交易所订单号查单笔订单；交易所明确表示没有这笔订单时返回 `None`。
    ///
    /// 文档把 `instId` 列为必填，但这里已知就带上、未知只传 `ordId`（兼容两种文档口径）。
    /// 见 <https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details>。
    async fn order_by_ord_id(
        &self,
        inst_id: Option<&str>,
        ord_id: &str,
    ) -> ArbResult<Option<RemoteOrder>> {
        let mut path = format!("/api/v5/trade/order?ordId={ord_id}");
        if let Some(inst_id) = inst_id.filter(|id| !id.is_empty()) {
            path.push_str(&format!("&instId={inst_id}"));
        }
        let envelope = self
            .send_envelope::<RemoteOrder>("GET", &path, None, "trade/order", true)
            .await?;
        if envelope.code != "0" {
            if order_not_found(&envelope.code) {
                return Ok(None);
            }
            return Err(envelope_error("trade/order", &envelope));
        }
        Ok(envelope.data.unwrap_or_default().into_iter().next())
    }

    /// 确认某笔订单已经不活跃：查单笔订单，终态或「不存在」都算不活跃；
    /// 仍然活跃就报错（撤单没生效），查询本身失败也报错（结果未知，交给运营对账）。
    async fn confirm_inactive(&self, inst_id: Option<&str>, ord_id: &str) -> ArbResult<()> {
        let remote = self.order_by_ord_id(inst_id, ord_id).await?;
        match cancel_confirmation(remote.as_ref())? {
            CancelConfirmation::Inactive => Ok(()),
            CancelConfirmation::StillLive => Err(error("撤单后订单仍显示活跃，停止并人工对账")),
        }
    }
}

/// `Base64(HMAC-SHA256(secret, timestamp + method + path + body))`。
fn signature(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let mut message =
        String::with_capacity(timestamp.len() + method.len() + path.len() + body.len());
    message.push_str(timestamp);
    message.push_str(method);
    message.push_str(path);
    message.push_str(body);
    base64_standard(&hmac_sha256(secret.as_bytes(), message.as_bytes()))
}

/// 订单在 O(1) 内重建时校验：标的、方向、reduce-only 必须与本地意图一致。
fn check_identity(
    order: &NewOrder,
    symbol: Symbol,
    side: Side,
    reduce_only: bool,
) -> ArbResult<()> {
    if order.symbol != symbol || order.side != side || order.reduce_only != reduce_only {
        return Err(error("交易所订单与本地登记的意图不一致"));
    }
    Ok(())
}

/// 逐笔成交聚合：返回 (标的成交量, 计价名义额, 手续费成本)。
///
/// OKX 的 `fee` 为负表示费用，这里转成正数=成本；`feeCcy` 必须是 USDT，且标的成交量
/// 必须与订单累计成交量（张数 × 面值）逐位相等，否则拒绝（成交明细不完整）。
fn aggregate_fills(
    inst_id: &str,
    ord_id: &str,
    ct_val: Decimal,
    acc_fill_sz: Decimal,
    fills: &[RemoteFill],
) -> ArbResult<(Decimal, Decimal, Decimal)> {
    let (mut base, mut notional, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    for fill in fills {
        if fill.ord_id != ord_id || fill.inst_id != inst_id {
            return Err(error("成交明细与订单不一致"));
        }
        if fill.fee_ccy != "USDT" {
            return Err(error("成交手续费不是 USDT 计价，无法计入成本"));
        }
        let size = parse_positive(&fill.fill_sz)?;
        let price = parse_positive(&fill.fill_px)?;
        let units = size
            .checked_mul(ct_val)
            .ok_or_else(|| error("数量换算溢出"))?;
        let quote = units
            .checked_mul(price)
            .ok_or_else(|| error("名义额换算溢出"))?;
        base = base
            .checked_add(units)
            .ok_or_else(|| error("成交量累加溢出"))?;
        notional = notional
            .checked_add(quote)
            .ok_or_else(|| error("名义额累加溢出"))?;
        let charged = parse_decimal(&fill.fee)?;
        fee = fee
            .checked_sub(charged)
            .ok_or_else(|| error("手续费累加溢出"))?;
    }
    let expected = acc_fill_sz
        .checked_mul(ct_val)
        .ok_or_else(|| error("成交量换算溢出"))?;
    if base != expected {
        return Err(error("成交明细数量与订单累计成交量不符"));
    }
    Ok((base, notional, fee))
}

/// 交易所状态 → 系统状态。未知状态一律报错，绝不推断终态。
fn map_status(state: &str) -> ArbResult<OrderStatus> {
    match state {
        "live" | "partially_filled" => Ok(OrderStatus::Open),
        "filled" => Ok(OrderStatus::Filled),
        "canceled" | "mmp_canceled" => Ok(OrderStatus::Cancelled),
        _ => Err(error(format!("未知的订单状态 {state}，拒绝推断"))),
    }
}

/// IOC 的终态成交可能是部分成交：只有整单数量成交完才算 `Filled`。
fn verified_status(
    status: OrderStatus,
    requested: Decimal,
    filled: Decimal,
) -> ArbResult<OrderStatus> {
    if filled < Decimal::ZERO || filled > requested {
        return Err(error("成交量非法"));
    }
    if status == OrderStatus::Filled && filled < requested {
        return Ok(OrderStatus::Cancelled);
    }
    Ok(status)
}

/// `BTC-USDT-SWAP` → `Symbol::perp("BTC", "USDT")`；其他后缀直接拒绝。
fn symbol_of_inst(inst_id: &str) -> ArbResult<Symbol> {
    let base = usdt_swap_base(inst_id).ok_or_else(|| error("订单不是 USDT 线性永续合约"))?;
    Ok(Symbol::perp(base, "USDT"))
}

/// 从合约名里取出标的币（要求以 `-USDT-SWAP` 结尾）。
fn usdt_swap_base(inst_id: &str) -> Option<&str> {
    inst_id
        .strip_suffix("-USDT-SWAP")
        .filter(|base| !base.is_empty())
}

/// 只认 USDT 保证金的线性永续（靠场所字段，不靠合约名猜）。
fn is_usdt_linear(row: &Instrument) -> bool {
    row.ct_type == "linear" && row.settle_ccy == "USDT"
}

fn first_price(levels: &[Vec<String>]) -> ArbResult<Option<Decimal>> {
    match levels.first() {
        None => Ok(None),
        Some(level) => {
            let raw = level.first().ok_or_else(|| error("盘口档位缺少价格"))?;
            Ok(Some(parse_decimal(raw)?))
        }
    }
}

fn parse_decimal(raw: &str) -> ArbResult<Decimal> {
    Decimal::from_str(raw).map_err(|_| error("无法解析交易所返回的数值"))
}

fn parse_positive(raw: &str) -> ArbResult<Decimal> {
    let value = parse_decimal(raw)?;
    if value <= Decimal::ZERO {
        return Err(error("交易所返回的数值应为正"));
    }
    Ok(value)
}

fn parse_side(raw: &str) -> ArbResult<Side> {
    match raw {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(error("未知的订单方向")),
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
        venue_order_id: state
            .venue_order_id
            .clone()
            .ok_or_else(|| error("订单缺少交易所订单号，无法撤单"))?,
        status: state.status,
    })
}

fn now_ms() -> ArbResult<i64> {
    Ok(chrono::Utc::now().timestamp_millis())
}

/// 毫秒时间戳 → ISO 8601 UTC（毫秒精度），例如 `2020-12-08T09:08:57.715Z`。
fn iso_ms(ms: i64) -> ArbResult<String> {
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| error("时间戳超出可表示范围"))?;
    Ok(datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 15), 16).unwrap_or('0'));
    }
    out
}

fn error(message: impl Into<String>) -> ArbError {
    ArbError::venue(VENUE, message)
}

/// 请求级业务错误文案：带上信封里真实的 `code`/`msg`（`data` 可能为 `null`）。
fn envelope_error<T>(endpoint: &str, envelope: &Envelope<T>) -> ArbError {
    error(format!(
        "{endpoint} 业务错误 code={} msg={}",
        envelope.code, envelope.msg
    ))
}

/// 单笔下单体 / 撤单体的判定结果。
#[derive(Debug, PartialEq, Eq)]
enum OrderOutcome {
    /// 交易所受理（订单级 `sCode == "0"`）。
    Accepted,
    /// 交易所明确拒绝，且不会产生活动订单。
    Refused { code: String, message: String },
    /// 结果未知（请求级错误或无法归类的 `sCode`）：绝不能记为拒单。
    Unknown { reason: String },
}

/// 撤单响应判定，比 [`OrderOutcome`] 多一档「本来就不活跃」。
#[derive(Debug, PartialEq, Eq)]
enum CancelOutcome {
    /// 撤单请求被受理；最终状态仍需向交易所确认。
    Accepted,
    /// 订单已成 / 已撤 / 不存在：幂等算成功。
    AlreadyInactive,
    /// 明确失败（如参数错误、订单类型不可撤）。
    Refused { code: String, message: String },
    /// 结果未知：上抛，交给运营对账。
    Unknown { reason: String },
}

/// 单笔下单响应判定：**订单级结果在 `data[0].sCode`，不是信封的 `code`**。
/// 下单被拒时官方返回 `{"code":"1","data":[{"sCode":"51008",...}],"msg":"All operations failed"}`；
/// 见 <https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order>。
fn classify_order_response(code: &str, msg: &str, rows: &[PlacedOrder]) -> OrderOutcome {
    if let Some(row) = rows.first()
        && !row.s_code.is_empty()
        && row.s_code != "0"
    {
        return if definitive_order_rejection(&row.s_code) {
            OrderOutcome::Refused {
                code: row.s_code.clone(),
                message: row.s_msg.clone(),
            }
        } else {
            OrderOutcome::Unknown {
                reason: format!("sCode={} {}", row.s_code, row.s_msg),
            }
        };
    }
    if code != "0" {
        return OrderOutcome::Unknown {
            reason: format!("code={code} msg={msg}"),
        };
    }
    OrderOutcome::Accepted
}

/// 撤单响应判定。撤单被拒时 `514xx` / `51603` 表示订单本来就不活跃，可幂等返回。
fn classify_cancel_response(code: &str, msg: &str, rows: &[PlacedOrder]) -> CancelOutcome {
    if let Some(row) = rows.first()
        && !row.s_code.is_empty()
        && row.s_code != "0"
    {
        return if cancel_already_inactive(&row.s_code) {
            CancelOutcome::AlreadyInactive
        } else if definitive_order_rejection(&row.s_code) {
            CancelOutcome::Refused {
                code: row.s_code.clone(),
                message: row.s_msg.clone(),
            }
        } else {
            CancelOutcome::Unknown {
                reason: format!("sCode={} {}", row.s_code, row.s_msg),
            }
        };
    }
    if code != "0" {
        return CancelOutcome::Unknown {
            reason: format!("code={code} msg={msg}"),
        };
    }
    CancelOutcome::Accepted
}

/// 官方错误码表里「请求已被明确拒绝、不会产生活动订单」的订单级 `sCode`。
///
/// 只收录含义明确的参数 / 金额 / 价格 / 保证金 / 张数类拒绝。系统级错误
/// （50011 限频、50013 系统繁忙、50026 系统错误、50004 网关超时）**不在此列**：
/// 它们的执行结果未知，必须查询对账，绝不能记成拒单。含义对照
/// <https://www.okx.com/docs-v5/en/#error-code> 与 ccxt 的 okx 错误映射。
fn definitive_order_rejection(code: &str) -> bool {
    matches!(
        code,
        "51000" // Parameter error
            | "51003" // Either clOrdId or ordId is required
            | "51004" // Order amount exceeds current tier limit
            | "51005" // Order amount exceeds the limit
            | "51006" // Order price is out of the limit
            | "51007" // Order amount should be at least 1 contract
            | "51008" // Order failed. Insufficient balance / margin
            | "51020" // Order amount should be greater than the min available amount
            | "51023" // Position does not exist
            | "51118" // Total amount should exceed the min amount per order
            | "51119" // Order failed. Insufficient balance
            | "51120" // Order quantity is less than the minimum
            | "51121" // Order quantity must be a multiple of the lot size
            | "51122" // Order price should be higher than the min price
            | "51131" // Insufficient balance
            | "51132" // Position amount is negative and below the minimum
            | "51133" // Reduce-only unavailable for this account
            | "51134" // Closing failed: check holdings and pending orders
            | "51185" // The maximum value allowed per order is exceeded
            | "51201" // Per market order value exceeds the limit
            | "51202" // Market order amount exceeds the max amount
            | "51203" // Order amount exceeds the limit
            | "51204" // Price for a limit order cannot be empty
            | "51205" // Reduce-only is not available
    )
}

/// 撤单响应里表示订单已不活跃（已成 / 已撤 / 不存在）的 `sCode`。
fn cancel_already_inactive(code: &str) -> bool {
    matches!(
        code,
        "51400" // Cancellation failed as the order does not exist
            | "51401" // Cancellation failed as the order is already canceled
            | "51402" // Cancellation failed as the order is already completed
            | "51405" // Cancellation failed as you have no pending orders
            | "51603" // Order does not exist
    )
}

/// 查询单笔订单时表示「没有这笔订单」的请求级 `code`。
fn order_not_found(code: &str) -> bool {
    code == "51603"
}

/// 撤单确认：以单笔订单查询结果判断订单是否已不活跃。
#[derive(Debug, PartialEq, Eq)]
enum CancelConfirmation {
    Inactive,
    StillLive,
}

/// 依据单笔订单查询结果判定撤单是否已经生效。`None` = 交易所明确说没有这笔订单。
fn cancel_confirmation(remote: Option<&RemoteOrder>) -> ArbResult<CancelConfirmation> {
    match remote {
        None => Ok(CancelConfirmation::Inactive),
        Some(remote) => {
            if map_status(&remote.state)?.is_live() {
                Ok(CancelConfirmation::StillLive)
            } else {
                Ok(CancelConfirmation::Inactive)
            }
        }
    }
}

/// 分页游标：只有满页且能取到最后一个 `ordId` 时才继续翻页，否则停止。
fn next_after(rows: &[RemoteOrder], limit: usize) -> Option<String> {
    if rows.len() < limit {
        return None;
    }
    rows.iter()
        .rev()
        .find(|row| !row.ord_id.is_empty())
        .map(|row| row.ord_id.clone())
}

/// 把逐仓 / 全仓持仓还原成本系统的净持仓参与对账，未知模式拒绝。
/// 数量为零的持仓返回 `None`。
fn venue_position(ct_val: Decimal, row: &RemotePosition) -> ArbResult<Option<VenuePosition>> {
    if !row.pos_side.is_empty() && row.pos_side != "net" {
        return Err(error(
            "账户存在双向（long/short）持仓；本券商只支持单向持仓 net_mode",
        ));
    }
    let contracts = parse_decimal(&row.pos)?;
    if contracts.is_zero() {
        return Ok(None);
    }
    if !matches!(row.mgn_mode.as_str(), "isolated" | "cross") {
        return Err(error(format!(
            "持仓 {} 的保证金模式是「{}」，本券商只接受 isolated / cross；请先核对账户配置",
            row.inst_id,
            if row.mgn_mode.is_empty() {
                "未知"
            } else {
                row.mgn_mode.as_str()
            }
        )));
    }
    let base = contracts
        .checked_mul(ct_val)
        .ok_or_else(|| error("持仓数量换算溢出"))?;
    let average = if row.avg_px.is_empty() {
        None
    } else {
        Some(parse_positive(&row.avg_px)?)
    };
    // `notionalUsd` 为空时**不能**把标的数量当成美元：有均价就用 标的数量 × 均价，
    // 否则留 0，宁可为空也不用错的数量级。
    let notional = if !row.notional_usd.is_empty() {
        parse_decimal(&row.notional_usd)?.abs()
    } else if let Some(price) = average {
        base.abs()
            .checked_mul(price)
            .ok_or_else(|| error("名义额换算溢出"))?
    } else {
        Decimal::ZERO
    };
    Ok(Some(VenuePosition {
        venue: Venue::Okx,
        symbol: symbol_of_inst(&row.inst_id)?,
        net_quantity: base,
        average_price: average,
        notional_usdt: notional,
    }))
}

/// OKX v5 响应信封。
///
/// `data` 用 `Option<Vec<T>>`：官方文档的响应体既可能是 `"data":[]`，也可能是
/// `"data":null`（例如系统级错误），`Vec<T>` + `serde(default)` 不接受显式 `null`，
/// 会让真实的 `code`/`msg` 因为反序列化失败而丢失。见
/// <https://www.okx.com/docs-v5/en/#overview-response-format>。
#[derive(Deserialize)]
struct Envelope<T> {
    code: String,
    #[serde(default)]
    msg: String,
    #[serde(default = "no_data")]
    data: Option<Vec<T>>,
}

/// 缺省 `data` 字段 → `None`。用路径形式的默认值，避免裸 `#[serde(default)]`
/// 给泛型 `T` 加上 `T: Default` 约束。
fn no_data<T>() -> Option<Vec<T>> {
    None
}

#[derive(Deserialize)]
struct ServerTime {
    ts: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountConfig {
    acct_lv: String,
    pos_mode: String,
    #[serde(default)]
    perm: String,
}

impl AccountConfig {
    fn perms(&self) -> impl Iterator<Item = &str> {
        self.perm
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeFee {
    #[serde(default)]
    fee_group: Vec<FeeGroup>,
    #[serde(default)]
    taker: String,
}

#[derive(Deserialize)]
struct FeeGroup {
    #[serde(default)]
    taker: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Instrument {
    inst_id: String,
    ct_type: String,
    settle_ccy: String,
    ct_val: String,
    ct_val_ccy: String,
    lot_sz: String,
    min_sz: String,
    tick_sz: String,
    #[serde(default)]
    lever: String,
    #[serde(default)]
    state: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageAck {
    lever: String,
    mgn_mode: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlacedOrder {
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    cl_ord_id: String,
    #[serde(default)]
    s_code: String,
    #[serde(default)]
    s_msg: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteOrder {
    inst_id: String,
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    cl_ord_id: String,
    side: String,
    #[serde(default)]
    sz: String,
    #[serde(default)]
    px: String,
    #[serde(default)]
    avg_px: String,
    #[serde(default)]
    acc_fill_sz: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    lever: String,
    #[serde(default)]
    reduce_only: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemoteFill {
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    fill_sz: String,
    #[serde(default)]
    fill_px: String,
    #[serde(default)]
    fee: String,
    #[serde(default)]
    fee_ccy: String,
    #[serde(default)]
    bill_id: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RemotePosition {
    inst_id: String,
    pos: String,
    #[serde(default)]
    pos_side: String,
    #[serde(default)]
    avg_px: String,
    #[serde(default)]
    notional_usd: String,
    /// `mgnMode`：isolated / cross。
    #[serde(default)]
    mgn_mode: String,
    #[serde(default)]
    liq_px: Option<String>,
    #[serde(default)]
    margin: Option<String>,
}

#[derive(Deserialize)]
struct Book {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    ts: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order(quantity: Option<Decimal>, reduce_only: bool) -> NewOrder {
        NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("live-1-buy-0".into()),
            venue: Venue::Okx,
            symbol: Symbol::perp("DOGE", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(1000),
            quantity,
            limit_price: Some(dec!(0.1)),
            reduce_only,
            leverage: Some(dec!(5)),
        }
    }

    fn fill(ord_id: &str, size: &str, px: &str, fee: &str, fee_ccy: &str) -> RemoteFill {
        RemoteFill {
            inst_id: "BTC-USDT-SWAP".into(),
            ord_id: ord_id.into(),
            fill_sz: size.into(),
            fill_px: px.into(),
            fee: fee.into(),
            fee_ccy: fee_ccy.into(),
            bill_id: "1".into(),
        }
    }

    #[test]
    fn signature_matches_the_documented_okx_algorithm() {
        // 独立算出：Base64(HMAC-SHA256(secret, ts + 'GET' + path + body))。
        let secret = "22582BD0CFF14C41EDBF1AB98506286D";
        let ts = "2020-12-08T09:08:57.715Z";
        assert_eq!(
            signature(secret, ts, "GET", "/api/v5/account/balance?ccy=BTC", ""),
            "HiZhvSfMtWJA3uUIVXV3a/bSXNPCWvYFXoGCVS8V4zY="
        );
        assert_eq!(
            signature(
                secret,
                ts,
                "POST",
                "/api/v5/account/set-leverage",
                r#"{"instId":"BTC-USDT","lever":"5","mgnMode":"isolated"}"#,
            ),
            "eCnnCgWLjlQ9XnpUkrcny3qNq3WW/81KNrDr/XR6Xv8="
        );
    }

    #[test]
    fn client_ids_are_alphanumeric_and_bounded() {
        let id = venue_client_id(
            CLIENT_ID_PREFIX,
            &ClientOrderId("live-1-buy-0".into()),
            CLIENT_ID_MAX,
        );
        assert_eq!(id.len(), CLIENT_ID_MAX);
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(id.starts_with(CLIENT_ID_PREFIX));
        assert_eq!(
            id,
            venue_client_id(
                CLIENT_ID_PREFIX,
                &ClientOrderId("live-1-buy-0".into()),
                CLIENT_ID_MAX
            )
        );
        assert_ne!(
            id,
            venue_client_id(
                CLIENT_ID_PREFIX,
                &ClientOrderId("live-1-buy-1".into()),
                CLIENT_ID_MAX
            )
        );
    }

    #[test]
    fn quantities_are_in_contracts_and_exits_must_be_exact() {
        // DOGE-USDT-SWAP 一张 = 1000 DOGE。
        let exact = order(Some(dec!(10000)), false);
        assert_eq!(
            order_units(Venue::Okx, &exact, dec!(0.1), dec!(1000), dec!(1), dec!(1)).unwrap(),
            dec!(10)
        );
        let exact_exit = order(Some(dec!(10000)), true);
        assert_eq!(
            order_units(
                Venue::Okx,
                &exact_exit,
                dec!(0.1),
                dec!(1000),
                dec!(1),
                dec!(1)
            )
            .unwrap(),
            dec!(10)
        );
        let ragged_exit = order(Some(dec!(10500)), true);
        assert!(
            order_units(
                Venue::Okx,
                &ragged_exit,
                dec!(0.1),
                dec!(1000),
                dec!(1),
                dec!(1)
            )
            .is_err(),
            "半张平不掉，必须拒绝"
        );
        let below_min = order(Some(dec!(500)), false);
        assert!(
            order_units(
                Venue::Okx,
                &below_min,
                dec!(0.1),
                dec!(1000),
                dec!(1),
                dec!(1)
            )
            .is_err()
        );
    }

    #[test]
    fn status_mapping_refuses_unknown_states() {
        assert_eq!(map_status("live").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("partially_filled").unwrap(), OrderStatus::Open);
        assert_eq!(map_status("filled").unwrap(), OrderStatus::Filled);
        assert_eq!(map_status("canceled").unwrap(), OrderStatus::Cancelled);
        assert_eq!(map_status("mmp_canceled").unwrap(), OrderStatus::Cancelled);
        assert!(map_status("weird").is_err());
    }

    #[test]
    fn partial_ioc_is_not_a_full_fill() {
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(2)).unwrap(),
            OrderStatus::Filled
        );
        assert_eq!(
            verified_status(OrderStatus::Filled, dec!(2), dec!(1)).unwrap(),
            OrderStatus::Cancelled
        );
        assert_eq!(
            verified_status(OrderStatus::Open, dec!(2), dec!(1)).unwrap(),
            OrderStatus::Open
        );
        assert!(verified_status(OrderStatus::Open, dec!(2), dec!(3)).is_err());
    }

    #[test]
    fn fill_aggregation_converts_signs_and_rejects_mismatch() {
        // 2 张 × 0.01 BTC × 60000 = 1200 USDT；fee -0.6（支出）→ 成本 0.6。
        let fills = vec![fill("o1", "2", "60000", "-0.6", "USDT")];
        let (base, notional, fee) =
            aggregate_fills("BTC-USDT-SWAP", "o1", dec!(0.01), dec!(2), &fills).unwrap();
        assert_eq!(base, dec!(0.02));
        assert_eq!(notional, dec!(1200));
        assert_eq!(fee, dec!(0.6));

        // 明细数量对不上订单累计成交量 → 拒绝。
        assert!(aggregate_fills("BTC-USDT-SWAP", "o1", dec!(0.01), dec!(3), &fills).is_err());
        // 手续费不是 USDT 计价 → 拒绝。
        let wrong_ccy = vec![fill("o1", "2", "60000", "-0.6", "BTC")];
        assert!(aggregate_fills("BTC-USDT-SWAP", "o1", dec!(0.01), dec!(2), &wrong_ccy).is_err());
    }

    #[test]
    fn rebate_is_a_negative_cost() {
        let fills = vec![fill("o1", "1", "60000", "0.01", "USDT")];
        let (_, _, fee) =
            aggregate_fills("BTC-USDT-SWAP", "o1", dec!(0.01), dec!(1), &fills).unwrap();
        assert_eq!(fee, dec!(-0.01));
    }

    #[test]
    fn instrument_symbols_require_the_usdt_linear_swap_suffix() {
        assert_eq!(
            symbol_of_inst("BTC-USDT-SWAP").unwrap(),
            Symbol::perp("BTC", "USDT")
        );
        assert!(symbol_of_inst("BTC-USD-SWAP").is_err());
        assert!(symbol_of_inst("BTC-USDC-SWAP").is_err());
        assert!(symbol_of_inst("BTC-USDT").is_err());
    }

    #[test]
    fn iso_timestamp_matches_the_documented_format() {
        assert_eq!(iso_ms(1607418537715).unwrap(), "2020-12-08T09:08:57.715Z");
    }

    // ---- 单笔下单 / 撤单响应判定与日志终态 ----

    fn test_broker(tag: &str) -> OkxBroker {
        let dir = std::env::temp_dir().join(format!("arb-okx-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("orders.jsonl");
        let _ = std::fs::remove_file(&path);
        OkxBroker {
            client: Client::new(),
            credentials: OkxCredentials {
                api_key: "key".into(),
                api_secret: "secret".into(),
                passphrase: "pass".into(),
            },
            options: LiveOptions::default(),
            journal: Mutex::new(OrderJournal::open(&path, "okx:test").unwrap()),
            taker_fee: Decimal::ZERO,
            time_offset_ms: 0,
        }
    }

    async fn reserve_intent(broker: &OkxBroker, order: &NewOrder) -> String {
        let cl_ord = venue_client_id(CLIENT_ID_PREFIX, &order.client_order_id, CLIENT_ID_MAX);
        broker
            .journal
            .lock()
            .await
            .reserve(JournalEntry {
                order: order.clone(),
                venue_client_id: cl_ord.clone(),
                instrument: "DOGE-USDT-SWAP".into(),
                units: dec!(10),
                terminal: None,
            })
            .unwrap();
        cl_ord
    }

    fn placed(ord_id: &str, cl_ord_id: &str, s_code: &str, s_msg: &str) -> PlacedOrder {
        PlacedOrder {
            ord_id: ord_id.into(),
            cl_ord_id: cl_ord_id.into(),
            s_code: s_code.into(),
            s_msg: s_msg.into(),
        }
    }

    fn remote_order(ord_id: &str) -> RemoteOrder {
        RemoteOrder {
            inst_id: "DOGE-USDT-SWAP".into(),
            ord_id: ord_id.into(),
            cl_ord_id: String::new(),
            side: "buy".into(),
            sz: String::new(),
            px: String::new(),
            avg_px: String::new(),
            acc_fill_sz: String::new(),
            state: String::new(),
            lever: String::new(),
            reduce_only: String::new(),
        }
    }

    fn envelope(body: &str) -> Envelope<PlacedOrder> {
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn the_rejection_table_splits_definitive_refusals_from_unknown_outcomes() {
        // 明确拒绝：参数 / 金额 / 价格 / 保证金 / 张数类。
        for code in [
            "51000", "51004", "51005", "51006", "51007", "51008", "51020", "51023", "51119",
            "51120", "51121", "51131", "51201", "51202", "51203",
        ] {
            assert!(definitive_order_rejection(code), "{code} 应是明确拒绝");
        }
        // 系统级错误：结果未知，绝不能记成拒单。
        for code in [
            "50011", // 限频
            "50013", // 系统繁忙
            "50026", // 系统错误
            "50004", // 网关超时（官方 FAQ：不代表请求结果）
            "50040", "99999",
        ] {
            assert!(
                !definitive_order_rejection(code),
                "{code} 结果未知，不能记成拒单"
            );
        }

        let refused = placed("", "c", "51008", "Order failed. Insufficient USDT margin");
        assert!(matches!(
            classify_order_response("1", "All operations failed", std::slice::from_ref(&refused)),
            OrderOutcome::Refused { .. }
        ));
        let busy = placed("", "c", "50013", "System busy, please try again later");
        assert!(matches!(
            classify_order_response("1", "All operations failed", std::slice::from_ref(&busy)),
            OrderOutcome::Unknown { .. }
        ));
        // 请求级错误 + data 为空：仍是未知。
        assert!(matches!(
            classify_order_response("50011", "Rate limit reached", &[]),
            OrderOutcome::Unknown { .. }
        ));
        let accepted = placed("312269865356374016", "c", "0", "");
        assert_eq!(
            classify_order_response("0", "", std::slice::from_ref(&accepted)),
            OrderOutcome::Accepted
        );
    }

    #[tokio::test]
    async fn a_definitive_place_rejection_becomes_a_terminal_rejected_state() {
        let broker = test_broker("place-rejected");
        let order = order(Some(dec!(10000)), false);
        let cl_ord = reserve_intent(&broker, &order).await;
        let response = envelope(&format!(
            r#"{{"code":"1","msg":"All operations failed","data":[{{"clOrdId":"{cl_ord}","ordId":"312269865356374016","sCode":"51008","sMsg":"Order failed. Insufficient USDT margin in account"}}]}}"#
        ));
        let outcome = broker
            .apply_place_response(&order, &cl_ord, &response)
            .await
            .unwrap();
        assert!(matches!(outcome, OrderOutcome::Refused { .. }));

        // 执行层可见的行为：order_state 返回终态 Rejected，而不是 Err/None。
        let state = broker
            .order_state(&order.client_order_id)
            .await
            .unwrap()
            .expect("已登记订单必须有状态");
        assert_eq!(state.status, OrderStatus::Rejected);
        assert_eq!(state.venue_order_id.as_deref(), Some("312269865356374016"));
        assert!(
            state
                .reject_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("51008")),
            "拒绝原因要带 sCode"
        );
    }

    #[tokio::test]
    async fn an_unknown_place_outcome_is_never_recorded_as_rejected() {
        let broker = test_broker("place-unknown");
        let order = order(Some(dec!(10000)), false);
        let cl_ord = reserve_intent(&broker, &order).await;
        let response = envelope(&format!(
            r#"{{"code":"1","msg":"All operations failed","data":[{{"clOrdId":"{cl_ord}","ordId":"","sCode":"50013","sMsg":"System busy, please try again later"}}]}}"#
        ));
        let outcome = broker
            .apply_place_response(&order, &cl_ord, &response)
            .await
            .unwrap();
        assert!(matches!(outcome, OrderOutcome::Unknown { .. }));
        assert!(
            broker
                .journal
                .lock()
                .await
                .by_venue_client_id(&cl_ord)
                .unwrap()
                .terminal
                .is_none(),
            "未知结果绝不能落成终态"
        );
    }

    #[test]
    fn a_null_data_field_keeps_the_real_code_and_message() {
        let response: Envelope<PlacedOrder> = serde_json::from_str(
            r#"{"code":"50013","msg":"System busy, please try again later","data":null}"#,
        )
        .unwrap();
        assert!(response.data.is_none(), "显式 null 必须能解析");
        let text = envelope_error("trade/order", &response).to_string();
        assert!(text.contains("50013"), "错误文案要带真实 code：{text}");
        assert!(text.contains("System busy"), "错误文案要带真实 msg：{text}");

        // 缺省 data 字段同样可以解析。
        let missing: Envelope<PlacedOrder> =
            serde_json::from_str(r#"{"code":"0","msg":""}"#).unwrap();
        assert!(missing.data.is_none());
    }

    // ---- 撤单：分页与「已不活跃」确认 ----

    #[test]
    fn pagination_stops_only_on_a_short_page_or_a_missing_id() {
        let rows = |count: usize, last_id: &str| -> Vec<RemoteOrder> {
            (0..count)
                .map(|i| remote_order(if i + 1 == count { last_id } else { "id" }))
                .collect()
        };
        assert_eq!(
            next_after(&rows(100, "cursor-100"), 100).as_deref(),
            Some("cursor-100")
        );
        assert_eq!(
            next_after(&rows(99, "cursor-99"), 100),
            None,
            "不满页就停止"
        );
        assert_eq!(
            next_after(&rows(100, ""), 100).as_deref(),
            Some("id"),
            "末行缺 ordId 时退到最后一个非空 ordId，仍能继续翻页"
        );
        let all_empty: Vec<RemoteOrder> = (0..100).map(|_| remote_order("")).collect();
        assert_eq!(
            next_after(&all_empty, 100),
            None,
            "整页都没有 ordId 必须停止，避免死循环"
        );
    }

    #[test]
    fn cancel_confirmation_requires_a_query_that_says_inactive() {
        assert_eq!(
            cancel_confirmation(None).unwrap(),
            CancelConfirmation::Inactive,
            "交易所说没有这笔订单 = 已不活跃"
        );
        for state in ["canceled", "filled"] {
            let mut remote = remote_order("1");
            remote.state = state.into();
            assert_eq!(
                cancel_confirmation(Some(&remote)).unwrap(),
                CancelConfirmation::Inactive
            );
        }
        let mut live = remote_order("1");
        live.state = "live".into();
        assert_eq!(
            cancel_confirmation(Some(&live)).unwrap(),
            CancelConfirmation::StillLive
        );
        let mut weird = remote_order("1");
        weird.state = "weird".into();
        assert!(
            cancel_confirmation(Some(&weird)).is_err(),
            "未知状态不能推断已不活跃"
        );
    }

    #[test]
    fn cancel_response_separates_already_inactive_from_unknown() {
        assert_eq!(
            classify_cancel_response("0", "", &[placed("1", "c", "0", "")]),
            CancelOutcome::Accepted
        );
        for code in ["51400", "51401", "51402", "51603"] {
            assert_eq!(
                classify_cancel_response("1", "All operations failed", &[placed("", "", code, "")]),
                CancelOutcome::AlreadyInactive,
                "{code} 应视为已不活跃"
            );
        }
        assert!(matches!(
            classify_cancel_response(
                "1",
                "All operations failed",
                &[placed("", "", "50013", "System busy")]
            ),
            CancelOutcome::Unknown { .. }
        ));
    }

    // ---- 持仓换算 ----

    #[test]
    fn positions_preserve_contract_units_for_both_margin_modes() {
        // notionalUsd 为空、有均价：用 张数×面值×均价，而不是把标的数量当美元。
        let row = RemotePosition {
            liq_px: None,
            margin: None,
            inst_id: "BTC-USDT-SWAP".into(),
            pos: "-2".into(),
            pos_side: "net".into(),
            avg_px: "60000".into(),
            notional_usd: String::new(),
            mgn_mode: "isolated".into(),
        };
        let position = venue_position(dec!(0.01), &row).unwrap().unwrap();
        assert_eq!(position.net_quantity, dec!(-0.02));
        assert_eq!(position.notional_usdt, dec!(1200));
        assert_ne!(position.notional_usdt, dec!(0.02), "绝不能把标的数量当美元");

        // 均价也没有：名义额只能是 0。
        let no_price = RemotePosition {
            avg_px: String::new(),
            ..row.clone()
        };
        let position = venue_position(dec!(0.01), &no_price).unwrap().unwrap();
        assert_eq!(position.notional_usdt, Decimal::ZERO);

        // 全仓持仓也应参与对账，数量口径不变。
        let cross = RemotePosition {
            mgn_mode: "cross".into(),
            ..row.clone()
        };
        let position = venue_position(dec!(0.01), &cross).unwrap().unwrap();
        assert_eq!(position.net_quantity, dec!(-0.02));
        let unknown = RemotePosition {
            mgn_mode: "portfolio".into(),
            ..row.clone()
        };
        assert!(venue_position(dec!(0.01), &unknown).is_err());

        // 零持仓不产生记录。
        let flat = RemotePosition {
            pos: "0".into(),
            ..row.clone()
        };
        assert!(venue_position(dec!(0.01), &flat).unwrap().is_none());
    }
}
