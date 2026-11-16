//! Arcus 账户订单推送（`orders` 频道，按地址订阅、不用鉴权）：下单后早一点知道「已经全部成交」。
//!
//! 为什么：`placeOrder` 是异步受理的，结果要回查。2026-10-22 东京实测，发单→撮合中位 64ms（68 笔，
//! 发单时刻取订单 `goodTilTime` 里的签名时间戳），撮合后这个频道约 15ms 推到（某合约 平仓实测）；
//! 而按 `GET /v1/order` 每 200ms 轮询到终态，第一腿中位 673ms、第二腿 904ms（第二腿还含查成交记录）。
//!
//! 推送来的订单行和 `GET /v1/order` 是同一个形状，只替代「轮询等终态」这一段
//! （[`super::ArcusBroker::resolve`]）：第一腿据它给待核实状态、马上发第二腿；成交额、手续费照旧按
//! `/v1/fills` 与公开逐笔核算，记账口径不变。只信「FILLED 且已成交 = 原始数量」的行：部分成交（交易所
//! 给的终态是 CANCELED）、没成交（REJECTED）、还活着、市场对不上，一律等 REST，和没有推送时完全一样。
//! 推送断了、没收到也一样。连接在后台自己重连，连不上不影响交易台连接。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, Weak};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::{OrderRow, map_status};
use crate::types::OrderStatus;

pub(super) const STREAM_URL: &str = "wss://api.arcus.xyz/v1/ws";
/// 推送来的订单行留多久：够下单那一刻等完就行。
const KEEP: Duration = Duration::from_secs(120);
/// 多久一帧都没有就当连接已死、重连。服务端约每 30 秒发一次 ping，我们也每 [`PING_EVERY`] 发一次。
const SILENCE: Duration = Duration::from_secs(90);
const PING_EVERY: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// 重连等待：1、2、4 … 秒，封顶 60 秒；订阅成功过就从头算。
const RETRY_MAX: Duration = Duration::from_secs(60);

pub(super) struct OrderFeed {
    /// 小写 `0x` 地址。
    address: String,
    account_index: u8,
    /// 交易所客户订单号（`arb-…`）→ 推送来的订单行（到达时刻）。
    rows: SyncMutex<HashMap<String, Vec<(Instant, OrderRow)>>>,
    arrived: Notify,
    /// 订阅成功、连接还在。断了照样能下单，只是走 REST 轮询。
    connected: AtomicBool,
}

#[derive(Deserialize)]
struct Frame {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    channel: Option<String>,
    #[serde(default, rename = "accountIndex")]
    account_index: Option<u8>,
    #[serde(default)]
    contents: Option<Value>,
    #[serde(default)]
    message: Option<String>,
}

/// 一帧对我们意味着什么。
#[derive(Debug)]
enum Event {
    Subscribed,
    Row(Box<OrderRow>),
    Error(String),
    Other,
}

impl OrderFeed {
    pub(super) fn new(address: String, account_index: u8) -> Arc<Self> {
        Arc::new(Self {
            address,
            account_index,
            rows: SyncMutex::new(HashMap::new()),
            arrived: Notify::new(),
            connected: AtomicBool::new(false),
        })
    }

    /// 推送现在是不是连着（订阅成功之后、断开之前）。
    pub(super) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn set_connected(&self, on: bool) {
        self.connected.store(on, Ordering::Relaxed);
    }

    /// 在后台常连 `url` 并订阅这个子账户的订单；券商没了（`Arc` 都丢了）就停。
    pub(super) fn spawn(self: &Arc<Self>, url: String) {
        let feed = Arc::downgrade(self);
        tokio::spawn(async move { run(feed, url).await });
    }

    fn subscribe_message(&self) -> String {
        json!({
            "type": "subscribe",
            "channel": "orders",
            "id": self.address,
            "accountIndex": self.account_index,
            // 订阅时的快照是以前的订单，等的是刚发出的单：只要之后的更新。
            "snapshot": false,
        })
        .to_string()
    }

    /// 解析一帧。别的子账户、别的频道的帧一律不算。
    fn event(&self, text: &str) -> Event {
        let Ok(frame) = serde_json::from_str::<Frame>(text) else {
            return Event::Other;
        };
        if frame.kind == "error" {
            return Event::Error(frame.message.unwrap_or_else(|| text.to_string()));
        }
        if frame.channel.as_deref() != Some("orders")
            || frame.account_index != Some(self.account_index)
        {
            return Event::Other;
        }
        match frame.kind.as_str() {
            "subscribed" => Event::Subscribed,
            "channel_data" => frame
                .contents
                .and_then(|contents| serde_json::from_value::<OrderRow>(contents).ok())
                .map_or(Event::Other, |row| Event::Row(Box::new(row))),
            _ => Event::Other,
        }
    }

    /// 收下推送来的订单行，顺手丢掉超过 [`KEEP`] 的。
    pub(super) fn record(&self, rows: impl IntoIterator<Item = OrderRow>) {
        let now = Instant::now();
        {
            let mut by_client = self.rows.lock().unwrap_or_else(|p| p.into_inner());
            by_client.retain(|_, kept| {
                kept.retain(|(at, _)| now.saturating_duration_since(*at) < KEEP);
                !kept.is_empty()
            });
            for row in rows {
                if let Some(client) = row.client_id.clone() {
                    by_client.entry(client).or_default().push((now, row));
                }
            }
        }
        self.arrived.notify_waiters();
    }

    /// 客户订单号为 `client` 的单在推送里**已经全部成交**（FILLED、已成交 = 原始数量、市场对得上）时，
    /// 返回那一行。其余（还活着、部分成交后撤销、没成交、止盈止损单）都返回 `None` —— 交给 REST。
    pub(super) fn filled(&self, client: &str, market: u16) -> Option<OrderRow> {
        let by_client = self.rows.lock().unwrap_or_else(|p| p.into_inner());
        by_client
            .get(client)?
            .iter()
            .map(|(_, row)| row)
            .find(|row| {
                row.market_id == market
                    && row.client_id.as_deref() == Some(client)
                    && matches!(map_status(row), Ok(OrderStatus::Filled))
            })
            .cloned()
    }

    /// 等到 [`Self::filled`] 有结果，最多等 `within`。
    pub(super) async fn wait_filled(
        &self,
        client: &str,
        market: u16,
        within: Duration,
    ) -> Option<OrderRow> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            // 先登记要等的通知再查：查完、开始等之间到的推送不会漏掉。
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            if let Some(row) = self.filled(client, market) {
                return Some(row);
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return None;
            }
        }
    }
}

async fn run(feed: Weak<OrderFeed>, url: String) {
    let mut failures = 0u32;
    while feed.strong_count() > 0 {
        let ended = session(&feed, &url).await;
        let was_connected = feed
            .upgrade()
            .is_some_and(|feed| feed.connected.swap(false, Ordering::Relaxed));
        let error = match ended {
            Ok(()) => return,
            Err(error) => error,
        };
        if was_connected {
            failures = 0;
        }
        let wait = Duration::from_secs(1u64 << failures.min(6)).min(RETRY_MAX);
        // 刚断的那一次记 warn，之后连续失败只在等待封顶时再记，免得刷屏。
        if failures == 0 || wait == RETRY_MAX {
            warn!(
                "Arcus 订单推送断了，{} 秒后重连（下单后照常用 REST 轮询确认）：{error}",
                wait.as_secs()
            );
        }
        failures += 1;
        tokio::time::sleep(wait).await;
    }
}

/// 一次连接：连上、订阅、收推送，直到出错（`Err`）或券商没了（`Ok`）。
async fn session(feed: &Weak<OrderFeed>, url: &str) -> Result<(), String> {
    let subscribe = match feed.upgrade() {
        Some(feed) => feed.subscribe_message(),
        None => return Ok(()),
    };
    let (mut ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| "连接超时".to_string())?
        .map_err(|error| error.to_string())?;
    ws.send(Message::text(subscribe))
        .await
        .map_err(|error| error.to_string())?;
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            message = tokio::time::timeout(SILENCE, ws.next()) => {
                let message = message.map_err(|_| format!("{} 秒没收到任何消息", SILENCE.as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let Some(feed) = feed.upgrade() else { return Ok(()) };
                        match feed.event(&text) {
                            Event::Subscribed => {
                                feed.connected.store(true, Ordering::Relaxed);
                                info!("Arcus 订单推送已连上：下单后约 80 毫秒就能知道是否全部成交");
                            }
                            Event::Row(row) => feed.record([*row]),
                            Event::Error(message) => return Err(format!("服务端报错：{message}")),
                            Event::Other => {}
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
            _ = ping.tick() => {
                if feed.strong_count() == 0 {
                    let _ = ws.close(None).await;
                    return Ok(());
                }
                ws.send(Message::Ping(Vec::new().into())).await.map_err(|error| error.to_string())?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";

    /// 2026-10-22 08:27 实测的 某合约 平仓推送（地址换成测试地址）。
    fn mstr_frame(account_index: u8, status: &str, filled: &str, remaining: &str) -> String {
        json!({"type": "channel_data", "channel": "orders", "id": ADDRESS, "accountIndex": account_index,
               "contents": {"orderId": "00000000000000b2", "clientId": "arb-00000000000000000000000000000001",
                            "marketId": 47, "marketDisplayName": "MSTR-USD", "side": "SELL", "type": "LIMIT",
                            "status": status, "price": "144.01", "originalSize": "2.7019",
                            "filledSize": filled, "remainingSize": remaining, "avgFillPrice": "144.44",
                            "timeInForce": "IOC", "goodTilTime": "1796113676915089", "state": status,
                            "positionEffect": "CLOSE_LONG", "reduceOnly": true,
                            "createdAt": 1792657676978472_i64, "updatedAt": 1792657676978472_i64,
                            "sequenceNumber": 44517}})
        .to_string()
    }

    const MSTR_CLIENT: &str = "arb-00000000000000000000000000000001";

    fn row_of(feed: &OrderFeed, text: &str) -> OrderRow {
        match feed.event(text) {
            Event::Row(row) => *row,
            other => panic!("应当是订单行：{other:?}"),
        }
    }

    #[test]
    fn frames_are_read_only_for_this_subaccounts_orders() {
        let feed = OrderFeed::new(ADDRESS.into(), 0);
        let subscribe: Value = serde_json::from_str(&feed.subscribe_message()).unwrap();
        assert_eq!(
            subscribe,
            json!({"type": "subscribe", "channel": "orders", "id": ADDRESS, "accountIndex": 0,
                   "snapshot": false})
        );
        let row = row_of(&feed, &mstr_frame(0, "FILLED", "2.7019", "0"));
        assert_eq!(row.client_id.as_deref(), Some(MSTR_CLIENT));
        assert_eq!(row.created_at, Some(1792657676978472));
        assert!(matches!(
            feed.event(r#"{"type":"subscribed","channel":"orders","id":"0x","accountIndex":0,"contents":{}}"#),
            Event::Subscribed
        ));
        // 别的子账户、别的频道、连接问候：都不算。
        assert!(matches!(
            feed.event(&mstr_frame(3, "FILLED", "2.7019", "0")),
            Event::Other
        ));
        assert!(matches!(
            feed.event(r#"{"type":"subscribed","channel":"orders","id":"0x","accountIndex":3}"#),
            Event::Other
        ));
        assert!(matches!(
            feed.event(r#"{"type":"channel_data","channel":"userFills","accountIndex":0,"contents":{"fills":[]}}"#),
            Event::Other
        ));
        assert!(matches!(
            feed.event(r#"{"type":"connected","connection_id":"1"}"#),
            Event::Other
        ));
        // 实测的报错帧形状。
        assert!(matches!(
            feed.event(r#"{"type":"error","message":"account index must be between 0 and 9"}"#),
            Event::Error(message) if message.contains("between 0 and 9")
        ));
    }

    #[test]
    fn only_a_fully_filled_order_is_trusted() {
        let feed = OrderFeed::new(ADDRESS.into(), 0);
        // 还活着、部分成交后撤销（IOC 没吃满，交易所记 CANCELED）、名为 FILLED 实际没满、没成交：都交给 REST。
        for (status, filled, remaining) in [
            ("OPEN", "0", "2.7019"),
            ("CANCELED", "1.2", "1.5019"),
            ("FILLED", "1.2", "1.5019"),
            ("REJECTED", "0", "2.7019"),
        ] {
            feed.record([row_of(&feed, &mstr_frame(0, status, filled, remaining))]);
            assert!(feed.filled(MSTR_CLIENT, 47).is_none(), "{status} {filled}");
        }
        feed.record([row_of(&feed, &mstr_frame(0, "FILLED", "2.7019", "0"))]);
        let row = feed.filled(MSTR_CLIENT, 47).expect("全部成交");
        assert_eq!(row.order_id, "00000000000000b2");
        // 市场、客户订单号对不上：不算。
        assert!(feed.filled(MSTR_CLIENT, 48).is_none());
        assert!(feed.filled("arb-other", 47).is_none());
    }

    #[tokio::test]
    async fn waiting_returns_as_soon_as_the_fill_arrives_and_gives_up_after_the_window() {
        let feed = OrderFeed::new(ADDRESS.into(), 0);
        let row = row_of(&feed, &mstr_frame(0, "FILLED", "2.7019", "0"));
        let pusher = Arc::clone(&feed);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            pusher.record([row]);
        });
        let started = Instant::now();
        let row = feed
            .wait_filled(MSTR_CLIENT, 47, Duration::from_secs(5))
            .await;
        assert_eq!(row.map(|row| row.order_id), Some("00000000000000b2".into()));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(
            feed.wait_filled("arb-other", 47, Duration::from_millis(50))
                .await
                .is_none()
        );
    }

    /// 实网只读：连 Arcus 推送、订阅这个子账户的订单（不用鉴权，只给地址），几秒内应当订阅成功。
    /// `cargo test -p arb-exec --lib order_feed::tests::live_feed_subscribes -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "连实网"]
    async fn live_feed_subscribes() {
        let _ = dotenvy::dotenv();
        let address = super::super::normalize_address(
            &std::env::var("ARB_ARCUS_ADDRESS").expect("ARB_ARCUS_ADDRESS"),
        )
        .unwrap();
        let index = super::super::parse_account_index(
            &std::env::var("ARB_ARCUS_ACCOUNT_INDEX").expect("ARB_ARCUS_ACCOUNT_INDEX"),
        )
        .unwrap();
        let feed = OrderFeed::new(address, index);
        feed.spawn(STREAM_URL.to_string());
        for _ in 0..100 {
            if feed.is_connected() {
                println!("已订阅账户订单推送");
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("10 秒内没订阅上");
    }
}
