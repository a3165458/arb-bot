//! Lighter 账户订单状态推送（`account_all_orders/{账户}`，带鉴权）—— **影子阶段：只记录、不参与下单判断**。
//!
//! 为什么要它：成交推送（[`super::fill_feed`]）只在**全部成交**时有用，可第二腿全成交时对冲早已完成，早知道
//! 不降低风险；真正要快的是「没成交 / 只成交一部分」—— 早知道就能早重试、早强制补腿、早回滚。这些结局没有
//! 成交推送，现在只能等 REST `accountOrders` 查得到（撮合后约 1.1 秒）。订单频道会推订单的状态变化（含撤单），
//! 按道理撮合后几十毫秒就到。
//!
//! 这个频道的消息格式还没在实盘核实过，所以先只做影子：连上、订阅、按客户订单号记下每张单最早一次推到终态
//! （`filled` / `canceled*`）的时刻和数量；REST 读到这张单的结局时，在日志里对比推送早了多少、状态和成交量
//! 对不对得上（[`OrderFeed::compare`]）。核实无误后再让下单路径用它。连不上、格式不对，一律不影响交易。
//!
//! 鉴权 token 由官方签名库生成（与 REST 读单用的同一个），有效期 [`TOKEN_LIFE`] 多一点：每过 [`TOKEN_LIFE`] 换新
//! token 重连一次。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, Weak};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::types::OrderState;

/// 推送来的订单留多久：够 REST 读回来对比就行。
const KEEP: Duration = Duration::from_secs(600);
const SILENCE: Duration = Duration::from_secs(90);
const PING_EVERY: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RETRY_MAX: Duration = Duration::from_secs(60);
/// 一条连接最多用多久就换新 token 重连（token 的有效期比它多一分钟）。
pub(super) const TOKEN_LIFE: Duration = Duration::from_secs(540);
/// 开头几条原始推送照抄进日志（截断），用来核对消息格式。
const SAMPLE_LOGS: u32 = 3;

/// 生成鉴权 token：参数是 token 的过期时刻（Unix 秒）。
pub(super) type TokenFn = Arc<dyn Fn(i64) -> Result<String, String> + Send + Sync>;

/// 一张单在推送里最早一次到终态的样子。
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Seen {
    pub at: Instant,
    pub status: String,
    pub filled_base: Decimal,
    pub filled_quote: Decimal,
    pub market: i64,
}

pub(super) struct OrderFeed {
    account: i64,
    terminal: SyncMutex<HashMap<i64, Seen>>,
    connected: AtomicBool,
    samples: AtomicU32,
}

impl OrderFeed {
    pub(super) fn new(account: i64) -> Arc<Self> {
        Arc::new(Self {
            account,
            terminal: SyncMutex::new(HashMap::new()),
            connected: AtomicBool::new(false),
            samples: AtomicU32::new(0),
        })
    }

    pub(super) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(super) fn spawn(self: &Arc<Self>, url: String, venue: Venue, token: TokenFn) {
        let feed = Arc::downgrade(self);
        let account = self.account;
        tokio::spawn(async move { run(feed, url, account, venue, token).await });
    }

    /// 收下一条推送里的订单：只记属于这个账户、到了终态的，同一张单只留最早那一次。
    pub(super) fn record(&self, orders: &[Value]) {
        let now = Instant::now();
        let mut terminal = self.terminal.lock().unwrap_or_else(|p| p.into_inner());
        terminal.retain(|_, seen| now.saturating_duration_since(seen.at) < KEEP);
        for order in orders {
            let Some(row) = parse_order(order) else {
                continue;
            };
            if row.owner.is_some_and(|owner| owner != self.account) || !is_terminal(&row.status) {
                continue;
            }
            terminal.entry(row.client).or_insert(Seen {
                at: now,
                status: row.status,
                filled_base: row.filled_base,
                filled_quote: row.filled_quote,
                market: row.market,
            });
        }
    }

    pub(super) fn seen(&self, client: i64) -> Option<Seen> {
        let terminal = self.terminal.lock().unwrap_or_else(|p| p.into_inner());
        terminal.get(&client).cloned()
    }

    /// REST 读到这张单的结局（`rest`，`submitted` 是发单时刻）后，和推送比：推送早到多少、状态和成交额对不对得上。
    /// 返回一句给日志的话；推送没连上时返回 `None`（不比）。
    pub(super) fn compare(
        &self,
        client: i64,
        rest: &OrderState,
        submitted: Instant,
        now: Instant,
    ) -> Option<String> {
        if !self.is_connected() {
            return None;
        }
        let rest_ms = now.saturating_duration_since(submitted).as_millis();
        let Some(seen) = self.seen(client) else {
            return Some(format!(
                "没收到这张单的终态推送（REST {rest_ms}ms 读到 {:?}，成交额 {}）",
                rest.status, rest.filled_usdt
            ));
        };
        let push_ms = seen.at.saturating_duration_since(submitted).as_millis();
        let push_filled = seen.filled_base > Decimal::ZERO;
        let rest_filled = rest.filled_usdt > Decimal::ZERO;
        let same_fill = (seen.filled_quote - rest.filled_usdt).abs() <= Decimal::new(1, 6);
        let verdict = if push_filled == rest_filled && same_fill {
            "一致"
        } else {
            "不一致"
        };
        Some(format!(
            "推送 {push_ms}ms 到终态 {}（成交额 {}），REST {rest_ms}ms 读到 {:?}（成交额 {}）：早 {}ms，{verdict}",
            seen.status,
            seen.filled_quote,
            rest.status,
            rest.filled_usdt,
            rest_ms.saturating_sub(push_ms)
        ))
    }
}

/// 推送里一张单要用的几项（数字可能是字符串也可能是数，都认）。
#[derive(Debug, PartialEq)]
struct Row {
    client: i64,
    market: i64,
    owner: Option<i64>,
    status: String,
    filled_base: Decimal,
    filled_quote: Decimal,
}

fn int(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn dec(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

fn parse_order(value: &Value) -> Option<Row> {
    Some(Row {
        client: int(value.get("client_order_index")?)?,
        market: value.get("market_index").and_then(int).unwrap_or(-1),
        owner: value.get("owner_account_index").and_then(int),
        status: value.get("status")?.as_str()?.to_string(),
        filled_base: value
            .get("filled_base_amount")
            .and_then(dec)
            .unwrap_or_default(),
        filled_quote: value
            .get("filled_quote_amount")
            .and_then(dec)
            .unwrap_or_default(),
    })
}

fn is_terminal(status: &str) -> bool {
    status == "filled" || status == "canceled" || status.starts_with("canceled-")
}

/// 推送里的订单列表：`orders` 可能是「市场 → 订单数组」，也可能直接是数组。
fn orders_of(envelope: &Value) -> Vec<Value> {
    match envelope.get("orders") {
        Some(Value::Object(by_market)) => by_market
            .values()
            .flat_map(|orders| orders.as_array().cloned().unwrap_or_default())
            .collect(),
        Some(Value::Array(orders)) => orders.clone(),
        _ => Vec::new(),
    }
}

async fn run(feed: Weak<OrderFeed>, url: String, account: i64, venue: Venue, token: TokenFn) {
    let mut failures = 0u32;
    while feed.strong_count() > 0 {
        let ended = session(&feed, &url, account, venue, &token).await;
        let was_connected = feed
            .upgrade()
            .is_some_and(|feed| feed.connected.swap(false, Ordering::Relaxed));
        let error = match ended {
            Ok(Session::Gone) => return,
            // token 到期换新的：马上重连，不算失败。
            Ok(Session::Refresh) => {
                failures = 0;
                continue;
            }
            Err(error) => error,
        };
        if was_connected {
            failures = 0;
        }
        let wait = Duration::from_secs(1u64 << failures.min(6)).min(RETRY_MAX);
        if failures == 0 || wait == RETRY_MAX {
            warn!(%venue, "Lighter 订单推送（影子）断了，{} 秒后重连（不影响交易）：{error}", wait.as_secs());
        }
        failures += 1;
        tokio::time::sleep(wait).await;
    }
}

enum Session {
    /// 券商没了：停。
    Gone,
    /// 用满 [`TOKEN_LIFE`]：换新 token 重连。
    Refresh,
}

async fn session(
    feed: &Weak<OrderFeed>,
    url: &str,
    account: i64,
    venue: Venue,
    token: &TokenFn,
) -> Result<Session, String> {
    let expires = chrono::Utc::now().timestamp() + TOKEN_LIFE.as_secs() as i64 + 60;
    let auth = token(expires)?;
    let (mut ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| "连接超时".to_string())?
        .map_err(|error| error.to_string())?;
    let subscribe = serde_json::json!({
        "type": "subscribe",
        "channel": format!("account_all_orders/{account}"),
        "auth": auth,
    })
    .to_string();
    ws.send(Message::text(subscribe))
        .await
        .map_err(|error| error.to_string())?;
    let refresh_at = tokio::time::Instant::now() + TOKEN_LIFE;
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            message = tokio::time::timeout(SILENCE, ws.next()) => {
                let message = message.map_err(|_| format!("{} 秒没收到任何消息", SILENCE.as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let Some(feed) = feed.upgrade() else { return Ok(Session::Gone) };
                        let Ok(envelope) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        let kind = envelope.get("type").and_then(Value::as_str).unwrap_or_default();
                        // 只有订单更新占样本名额：每 9 分钟换 token 重连一次，订阅回执（`orders` 是空的）会把名额用光。
                        if kind == "update/account_all_orders"
                            && feed.samples.fetch_add(1, Ordering::Relaxed) < SAMPLE_LOGS
                        {
                            let sample: String = text.chars().take(800).collect();
                            info!(%venue, kind, "Lighter 订单推送（影子）原始消息样本：{sample}");
                        }
                        match kind {
                            // 订阅时的快照里是现有的订单：也记（终态的一般不在里面），之后的更新是重点。
                            "subscribed/account_all_orders" => {
                                feed.connected.store(true, Ordering::Relaxed);
                                feed.record(&orders_of(&envelope));
                                info!(%venue, "Lighter 订单推送（影子）已连上：只记录，不参与下单判断");
                            }
                            "update/account_all_orders" => feed.record(&orders_of(&envelope)),
                            "error" => {
                                let text: String = text.chars().take(300).collect();
                                return Err(format!("服务端报错：{text}"));
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?;
                    }
                    Some(Ok(Message::Close(frame))) => return Err(format!("服务端关闭连接：{frame:?}")),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.to_string()),
                    None => return Err("连接结束".into()),
                }
            }
            _ = tokio::time::sleep_until(refresh_at) => {
                let _ = ws.close(None).await;
                return Ok(Session::Refresh);
            }
            _ = ping.tick() => {
                if feed.strong_count() == 0 {
                    let _ = ws.close(None).await;
                    return Ok(Session::Gone);
                }
                ws.send(Message::Ping(Vec::new().into())).await.map_err(|error| error.to_string())?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ClientOrderId, NewOrder, OrderStatus};
    use arb_core::{Side, Symbol};
    use rust_decimal_macros::dec;

    const US: i64 = 710_001;

    fn order(client: i64, status: &str, filled: &str, quote: &str, owner: i64) -> Value {
        serde_json::json!({
            "order_index": 900, "client_order_index": client, "market_index": 24,
            "owner_account_index": owner, "initial_base_amount": "2.51",
            "filled_base_amount": filled, "filled_quote_amount": quote, "status": status,
        })
    }

    fn rest(status: OrderStatus, filled: Decimal) -> OrderState {
        let mut state = OrderState::new(NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("p-sell-0".into()),
            venue: Venue::Lighter,
            symbol: Symbol::perp("PLTR", "USDT"),
            side: Side::Sell,
            notional_usdt: dec!(400),
            quantity: Some(dec!(2.51)),
            limit_price: None,
            reduce_only: false,
            leverage: None,
        });
        state.status = status;
        state.filled_usdt = filled;
        state
    }

    #[test]
    fn only_terminal_orders_of_this_account_are_kept_and_the_first_one_wins() {
        let feed = OrderFeed::new(US);
        feed.record(&[
            order(41, "open", "0", "0", US),
            order(42, "canceled-not-enough-liquidity", "0", "0", US),
            order(43, "filled", "2.51", "399.4", 77),
        ]);
        assert!(feed.seen(41).is_none(), "还活着的不记");
        assert!(feed.seen(43).is_none(), "别的账户的不记");
        let first = feed.seen(42).unwrap();
        assert_eq!(first.status, "canceled-not-enough-liquidity");
        feed.record(&[order(42, "canceled", "0", "0", US)]);
        assert_eq!(
            feed.seen(42).unwrap().status,
            first.status,
            "同一张单只留最早那次"
        );
        // 数字是数而不是字符串也认；按市场分组的 `orders` 也认。
        let envelope = serde_json::json!({"type": "update/account_all_orders", "orders": {"24": [
            {"client_order_index": "44", "market_index": 24, "status": "filled",
             "filled_base_amount": 2.51, "filled_quote_amount": "399.4"}
        ]}});
        feed.record(&orders_of(&envelope));
        assert_eq!(feed.seen(44).unwrap().filled_quote, dec!(399.4));
    }

    #[test]
    fn the_shadow_compares_the_push_with_the_rest_outcome() {
        let feed = OrderFeed::new(US);
        let submitted = Instant::now();
        assert!(
            feed.compare(
                42,
                &rest(OrderStatus::Cancelled, Decimal::ZERO),
                submitted,
                submitted
            )
            .is_none(),
            "没连上不比"
        );
        feed.connected.store(true, Ordering::Relaxed);
        feed.record(&[order(42, "canceled", "0", "0", US)]);
        let later = submitted + Duration::from_millis(1400);
        let line = feed
            .compare(
                42,
                &rest(OrderStatus::Cancelled, Decimal::ZERO),
                submitted,
                later,
            )
            .unwrap();
        assert!(line.contains("一致") && !line.contains("不一致"), "{line}");
        let line = feed
            .compare(
                42,
                &rest(OrderStatus::Filled, dec!(399.4)),
                submitted,
                later,
            )
            .unwrap();
        assert!(line.contains("不一致"), "{line}");
        let line = feed
            .compare(
                43,
                &rest(OrderStatus::Filled, dec!(399.4)),
                submitted,
                later,
            )
            .unwrap();
        assert!(line.contains("没收到"), "{line}");
    }
}
