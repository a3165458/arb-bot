//! Hyperliquid 常驻 WebSocket 下单通道（官方 `post` 请求）。
//!
//! 官方文档：<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests>。
//! 签名后的 `/exchange` 请求体（`action` / `nonce` / `signature` / `vaultAddress`）原样放进
//! `{"method":"post","id":N,"request":{"type":"action","payload":<请求体>}}`，应答是
//! `{"channel":"post","data":{"id":N,"response":{"type":"action"|"error","payload":…}}}`，
//! `action` 的 `payload` 就是 HTTP 应答体。连接常驻、已握手：下单不再每次走一遍 HTTP 请求的建立与解析。
//!
//! # 安全：只发一次，发出去就不重发
//!
//! - 连接没连上、请求没交给连接任务：[`PostOutcome::NotSent`]，调用方可以放心退回 HTTP。
//! - 请求已交给连接任务之后，不管发生什么（超时、断线、关闭）都是 [`PostOutcome::Unknown`]：
//!   请求可能已经到了交易所，调用方**不能**再用 HTTP 重发，按「结果不明、保留预留、对账」处理，
//!   与 HTTP 超时完全同一个语义。
//! - 官方限额：每 IP 同时在途的 post 最多 100 条、每分钟最多 2000 条消息（所有 WebSocket 连接合计）。
//!   这里按 [`MAX_INFLIGHT`] 自限，超了当作没发（退回 HTTP）。
//!
//! `ARB_HL_WS_ORDERS=off` 关闭（全部走 HTTP）。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

pub const HYPERLIQUID_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// 自己限制的在途 post 数（官方上限 100，留给同 IP 的其它程序）。
const MAX_INFLIGHT: usize = 20;
/// 应用层心跳间隔（官方：`{"method":"ping"}` 服务端回 `{"channel":"pong"}`；60 秒没有消息服务端会断开）。
/// 取得短，是为了让「TCP 静默断开」几秒内就被发现，而不是让下单一直等到超时。
#[cfg(not(test))]
const PING_EVERY: Duration = Duration::from_secs(5);
#[cfg(test)]
const PING_EVERY: Duration = Duration::from_millis(50);
/// 超过这么久没收到服务端任何消息（含 pong）就判定连接已死，重连；这期间下单退回 HTTP。
#[cfg(not(test))]
const DEAD_AFTER: Duration = Duration::from_secs(15);
#[cfg(test)]
const DEAD_AFTER: Duration = Duration::from_millis(400);
/// 连续这么多次拿不到成功应答（超时 / 服务端错误）就暂时停用 WebSocket 下单，改走 HTTP。
const BREAKER_FAILURES: u32 = 3;
/// 熔断后多久再试 WebSocket。
const BREAKER_COOLDOWN: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 一次 post 等应答最久多久（与 HTTP 客户端的 15 秒超时同量级，略短）。
pub const POST_TIMEOUT: Duration = Duration::from_secs(10);

/// 一次 post 的结局。
#[derive(Debug, Clone, PartialEq)]
pub enum PostOutcome {
    /// 请求没有发出去（没连上、队列满、在途太多）。可以安全地改用 HTTP。
    NotSent,
    /// 服务端应答。`Ok` 是 `action` 的 `payload`（与 HTTP 应答体相同）；`Err` 是服务端的错误字符串
    /// （镜像 HTTP 状态码与描述，说明请求在被处理前就被拒了）。
    Reply(Result<Value, String>),
    /// 请求已交给连接，但没拿到应答（超时、断线）。**可能已经成交**：不能重发。
    Unknown(String),
}

struct Shared {
    connected: AtomicBool,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    /// 连续没拿到成功应答的次数（拿到一次成功应答就清零）。
    failures: AtomicU64,
    /// 熔断到什么时候（之前一律当作没发出去、走 HTTP）。
    disabled_until: Mutex<Option<std::time::Instant>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            connected: AtomicBool::new(false),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            failures: AtomicU64::new(0),
            disabled_until: Mutex::new(None),
        }
    }

    fn disabled(&self) -> bool {
        let mut until = self
            .disabled_until
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *until {
            Some(at) if std::time::Instant::now() < at => true,
            Some(_) => {
                *until = None;
                false
            }
            None => false,
        }
    }

    /// 记一次结局：成功清零；失败累计，到阈值就熔断。
    fn record(&self, ok: bool, breaker: (u32, Duration)) {
        if ok {
            self.failures.store(0, Ordering::SeqCst);
            return;
        }
        let failures = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= u64::from(breaker.0) {
            self.failures.store(0, Ordering::SeqCst);
            *self
                .disabled_until
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(std::time::Instant::now() + breaker.1);
            warn!(
                "Hyperliquid WebSocket 下单连续 {failures} 次没有成功应答，{} 秒内改走 HTTP",
                breaker.1.as_secs()
            );
        }
    }
}

pub struct WsPoster {
    shared: Arc<Shared>,
    tx: mpsc::Sender<(u64, String)>,
    breaker: (u32, Duration),
}

impl WsPoster {
    /// 起连接任务（断线指数退避重连）。必须在 tokio 运行时里调用。
    pub fn spawn(url: &str) -> Arc<Self> {
        Self::spawn_with(url, (BREAKER_FAILURES, BREAKER_COOLDOWN))
    }

    fn spawn_with(url: &str, breaker: (u32, Duration)) -> Arc<Self> {
        let shared = Arc::new(Shared::new());
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(connection_loop(url.to_string(), Arc::clone(&shared), rx));
        Arc::new(Self {
            shared,
            tx,
            breaker,
        })
    }

    pub fn is_connected(&self) -> bool {
        self.shared.connected.load(Ordering::SeqCst)
    }

    /// 发一个签名后的 action 请求体（与 HTTP `/exchange` 的 JSON 相同）并等应答。
    pub async fn post_action(&self, payload: Value, timeout: Duration) -> PostOutcome {
        if !self.is_connected() || self.shared.disabled() {
            return PostOutcome::NotSent;
        }
        let id = self.shared.next_id.fetch_add(1, Ordering::SeqCst);
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut pending = self
                .shared
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if pending.len() >= MAX_INFLIGHT {
                return PostOutcome::NotSent;
            }
            pending.insert(id, reply_tx);
        }
        let text = json!({
            "method": "post",
            "id": id,
            "request": {"type": "action", "payload": payload},
        })
        .to_string();
        // 交给连接任务：成功之后就当它已经发出去了。
        if self.tx.try_send((id, text)).is_err() {
            self.forget(id);
            return PostOutcome::NotSent;
        }
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(result)) => {
                self.shared.record(result.is_ok(), self.breaker);
                PostOutcome::Reply(result)
            }
            Ok(Err(_)) => {
                self.shared.record(false, self.breaker);
                PostOutcome::Unknown("下单连接中断，没有收到应答".into())
            }
            Err(_) => {
                self.forget(id);
                self.shared.record(false, self.breaker);
                PostOutcome::Unknown(format!("{} 秒没有收到应答", timeout.as_secs_f32()))
            }
        }
    }

    fn forget(&self, id: u64) {
        self.shared
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&id);
    }
}

/// 连接任务的主循环。
async fn connection_loop(url: String, shared: Arc<Shared>, mut rx: mpsc::Receiver<(u64, String)>) {
    let mut failures = 0u32;
    loop {
        let started = std::time::Instant::now();
        let result = session(&url, &shared, &mut rx).await;
        shared.connected.store(false, Ordering::SeqCst);
        // 这条连接上还没拿到应答的请求：结果不明。丢掉发送端，等待方会得到 `Unknown`。
        shared
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        // 还排在队里、没写出去的也一并作废（等待方同样得到 `Unknown`，宁可当成不明）。
        while rx.try_recv().is_ok() {}
        if started.elapsed() > Duration::from_secs(60) {
            failures = 0;
        }
        let wait = Duration::from_secs(1u64 << failures.min(5)).min(Duration::from_secs(30));
        failures += 1;
        match result {
            Ok(()) => debug!("Hyperliquid 下单连接结束，{} 秒后重连", wait.as_secs()),
            Err(error) => warn!(
                "Hyperliquid 下单连接断开，{} 秒后重连（期间下单走 HTTP）：{error}",
                wait.as_secs()
            ),
        }
        tokio::time::sleep(wait).await;
    }
}

async fn session(
    url: &str,
    shared: &Shared,
    rx: &mut mpsc::Receiver<(u64, String)>,
) -> Result<(), String> {
    let (mut ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| "连接超时".to_string())?
        .map_err(|error| error.to_string())?;
    // 断线期间排进队列的请求：它们的调用方早已按「结果不明」放弃（或在连接断开时被清掉），
    // 绝不能在新连接上被发出去 —— 那会把一张已被放弃的旧价订单真的送到交易所。
    while rx.try_recv().is_ok() {}
    shared.connected.store(true, Ordering::SeqCst);
    info!("Hyperliquid 下单连接已建立（下单走 WebSocket post，断线自动退回 HTTP）");
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    let mut last_heard = std::time::Instant::now();
    loop {
        tokio::select! {
            outgoing = rx.recv() => {
                let Some((id, text)) = outgoing else { return Ok(()) };
                // 写出去之前再确认调用方还在等：已经超时放弃的（它的在途记录被清掉了）一律不发。
                let waiting = shared
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains_key(&id);
                if waiting {
                    ws.send(Message::text(text)).await.map_err(|error| error.to_string())?;
                }
            }
            incoming = ws.next() => {
                last_heard = std::time::Instant::now();
                match incoming {
                    Some(Ok(Message::Text(text))) => deliver(shared, &text),
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
                // 静默断开（TCP 没有报错）时，心跳的应答会一直不来：超过 DEAD_AFTER 就判死、重连，
                // 这期间 `connected` 为假，下单走 HTTP，不会每一单都白等到超时。
                if last_heard.elapsed() > DEAD_AFTER {
                    return Err(format!("{} 秒没有收到服务端任何消息", DEAD_AFTER.as_secs()));
                }
                ws.send(Message::text(r#"{"method":"ping"}"#)).await.map_err(|error| error.to_string())?;
            }
        }
    }
}

/// 把一条服务端消息交给等它的请求。
fn deliver(shared: &Shared, text: &str) {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return;
    };
    if value.get("channel").and_then(Value::as_str) != Some("post") {
        return;
    }
    let data = &value["data"];
    let Some(id) = data.get("id").and_then(Value::as_u64) else {
        return;
    };
    let response = &data["response"];
    let result = match response.get("type").and_then(Value::as_str) {
        Some("action") => Ok(response["payload"].clone()),
        Some("error") => Err(response["payload"]
            .as_str()
            .map_or_else(|| response["payload"].to_string(), str::to_string)),
        _ => Err("无法识别的 post 应答".to_string()),
    };
    let sender = shared
        .pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(result);
    }
}

static SHARED: OnceLock<Option<Arc<WsPoster>>> = OnceLock::new();

/// 进程内共用的下单连接（三个 dex 的券商共用一条，限额按 IP 合计）。第一次调用时在当前 tokio 运行时里
/// 建立；`ARB_HL_WS_ORDERS=off` 时返回 `None`。
pub fn start_shared() -> Option<Arc<WsPoster>> {
    SHARED
        .get_or_init(|| {
            let off = std::env::var("ARB_HL_WS_ORDERS").is_ok_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "off" | "0" | "false"
                )
            });
            (!off).then(|| WsPoster::spawn(HYPERLIQUID_WS))
        })
        .clone()
}

/// 已经建立的共用下单连接（没调用过 [`start_shared`] 或被关闭时为 `None`）。
pub fn shared() -> Option<Arc<WsPoster>> {
    SHARED.get().and_then(Clone::clone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// 本地假服务器：每收到一条 post 就按 `script` 回一条（`None` = 不回，让客户端超时；`Some(None)` 之外的
    /// 关闭连接用 `drop_after` 表示）。
    async fn server(
        script: impl Fn(u64, &Value) -> Option<Value> + Send + Sync + 'static,
        close_after_first: bool,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        serve(listener, script, close_after_first, true);
        url
    }

    /// 在已绑定的端口上跑假服务器。`answer_ping = false` 模拟 TCP 静默断开（连接还在，但再没有任何消息）。
    fn serve(
        listener: TcpListener,
        script: impl Fn(u64, &Value) -> Option<Value> + Send + Sync + 'static,
        close_after_first: bool,
        answer_ping: bool,
    ) {
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    continue;
                };
                while let Some(Ok(message)) = ws.next().await {
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let value: Value = serde_json::from_str(&text).unwrap();
                    if value["method"] == "ping" {
                        if answer_ping {
                            ws.send(Message::text(r#"{"channel":"pong"}"#))
                                .await
                                .unwrap();
                        }
                        continue;
                    }
                    if value["method"] != "post" {
                        continue;
                    }
                    let id = value["id"].as_u64().unwrap();
                    if close_after_first {
                        return;
                    }
                    if let Some(response) = script(id, &value["request"]["payload"]) {
                        let reply = json!({"channel":"post","data":{"id":id,"response":response}});
                        ws.send(Message::text(reply.to_string())).await.unwrap();
                    }
                }
            }
        });
    }

    async fn connected(url: &str) -> Arc<WsPoster> {
        let poster = WsPoster::spawn(url);
        for _ in 0..200 {
            if poster.is_connected() {
                return poster;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("没连上本地假服务器");
    }

    #[tokio::test]
    async fn a_signed_action_round_trips_and_the_payload_is_forwarded_verbatim() {
        let url = server(
            |_, payload| {
                Some(json!({"type":"action","payload":{
                    "status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"oid":7}}]}},
                    "echo": payload,
                }}))
            },
            false,
        )
        .await;
        let poster = connected(&url).await;
        let body = json!({"action":{"type":"order"},"nonce":1,"signature":{"r":"0x1","s":"0x2","v":27},"vaultAddress":null});
        let PostOutcome::Reply(Ok(reply)) = poster
            .post_action(body.clone(), Duration::from_secs(2))
            .await
        else {
            panic!("应当拿到应答");
        };
        assert_eq!(reply["status"], "ok");
        assert_eq!(reply["echo"], body, "请求体原样送达");
        // 并发的两条各自拿到自己的应答。
        let (a, b) = tokio::join!(
            poster.post_action(json!({"n":1}), Duration::from_secs(2)),
            poster.post_action(json!({"n":2}), Duration::from_secs(2)),
        );
        for (outcome, n) in [(a, 1), (b, 2)] {
            let PostOutcome::Reply(Ok(reply)) = outcome else {
                panic!("{n}");
            };
            assert_eq!(reply["echo"]["n"], n);
        }
    }

    #[tokio::test]
    async fn a_server_error_string_is_a_reply_not_an_unknown() {
        let url = server(
            |_, _| Some(json!({"type":"error","payload":"429 Too Many Requests"})),
            false,
        )
        .await;
        let poster = connected(&url).await;
        assert_eq!(
            poster.post_action(json!({}), Duration::from_secs(2)).await,
            PostOutcome::Reply(Err("429 Too Many Requests".into()))
        );
    }

    #[tokio::test]
    async fn without_a_connection_nothing_is_sent_so_http_is_safe() {
        // 没人监听的端口：永远连不上。
        let poster = WsPoster::spawn("ws://127.0.0.1:1");
        assert!(!poster.is_connected());
        assert_eq!(
            poster
                .post_action(json!({}), Duration::from_millis(50))
                .await,
            PostOutcome::NotSent
        );
    }

    #[tokio::test]
    async fn after_the_request_is_handed_over_silence_or_a_drop_is_unknown_never_notsent() {
        // 服务器收到不回：超时 = 结果不明（可能已成交），不能当成没发出去。
        let url = server(|_, _| None, false).await;
        let poster = connected(&url).await;
        let outcome = poster
            .post_action(json!({}), Duration::from_millis(150))
            .await;
        assert!(matches!(outcome, PostOutcome::Unknown(_)), "{outcome:?}");
        // 超时之后在途表里不留残余。
        assert!(poster.shared.pending.lock().unwrap().is_empty());

        // 服务器收到就断线：同样是结果不明。
        let url = server(|_, _| None, true).await;
        let poster = connected(&url).await;
        let outcome = poster.post_action(json!({}), Duration::from_secs(2)).await;
        assert!(matches!(outcome, PostOutcome::Unknown(_)), "{outcome:?}");
        // 断线之后到重连之前：没连上，没发出去。
        for _ in 0..100 {
            if !poster.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            poster
                .post_action(json!({}), Duration::from_millis(50))
                .await,
            PostOutcome::NotSent
        );
    }

    #[test]
    fn replies_without_a_waiting_request_or_on_other_channels_are_ignored() {
        let shared = Shared::new();
        deliver(&shared, "not json");
        deliver(&shared, r#"{"channel":"pong"}"#);
        deliver(
            &shared,
            r#"{"channel":"post","data":{"id":99,"response":{"type":"action","payload":{}}}}"#,
        );
        assert!(shared.pending.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_request_queued_while_disconnected_is_never_sent_after_the_reconnect() {
        // 先占一个端口再放掉：连接任务连不上，进入退避。
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let poster = WsPoster::spawn(&format!("ws://{addr}"));
        // 模拟竞态：调用方在「还连着」的瞬间通过了检查，请求进了队列，随后连接断了，调用方按「结果不明」放弃。
        // 它的在途记录已被清掉（id 不在 pending 里），消息却还留在队列里。
        poster
            .tx
            .try_send((
                424242,
                json!({"method":"post","id":424242,"stale":true}).to_string(),
            ))
            .unwrap();
        // 服务器这时才出现，记下它收到的每一条 post。
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let log = Arc::clone(&seen);
        serve(
            TcpListener::bind(addr).await.unwrap(),
            move |id, payload| {
                log.lock().unwrap().push(payload.clone());
                Some(json!({"type":"action","payload":{"id":id}}))
            },
            false,
            true,
        );
        for _ in 0..400 {
            if poster.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(poster.is_connected(), "应当重连成功");
        // 之后的正常请求照常通过，而且是服务器收到的**唯一**一条。
        let outcome = poster
            .post_action(json!({"fresh":true}), Duration::from_secs(2))
            .await;
        assert!(matches!(outcome, PostOutcome::Reply(Ok(_))), "{outcome:?}");
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "陈旧的排队请求不能在新连接上发出：{seen:?}");
        assert_eq!(seen[0]["fresh"], true);
    }

    #[tokio::test]
    async fn a_request_whose_caller_already_gave_up_is_not_written() {
        let seen = Arc::new(Mutex::new(0usize));
        let count = Arc::clone(&seen);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        serve(
            listener,
            move |id, _| {
                *count.lock().unwrap() += 1;
                Some(json!({"type":"action","payload":{"id":id}}))
            },
            false,
            true,
        );
        let poster = connected(&url).await;
        // 已连接但调用方没登记（等价于已放弃）：不写。
        poster
            .tx
            .try_send((777, json!({"method":"post","id":777}).to_string()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(*seen.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_silently_dead_connection_is_detected_and_orders_go_to_http_meanwhile() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        // 连得上、握手成功，但再没有任何消息（不回 pong）：TCP 静默断开。
        serve(listener, |_, _| None, false, false);
        let poster = connected(&url).await;
        for _ in 0..200 {
            if !poster.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!poster.is_connected(), "心跳应答一直不来，要判定连接已死");
        assert_eq!(
            poster
                .post_action(json!({}), Duration::from_millis(50))
                .await,
            PostOutcome::NotSent,
            "判死之后下单走 HTTP，而不是每一单白等到超时"
        );
    }

    #[tokio::test]
    async fn repeated_failures_trip_the_breaker_and_a_success_resets_the_count() {
        // 服务器对 {"slow":true} 不回，其余正常回。
        let url = server(
            |id, payload| {
                (payload["slow"] != true).then(|| json!({"type":"action","payload":{"id":id}}))
            },
            false,
        )
        .await;
        let poster = WsPoster::spawn_with(&url, (3, Duration::from_secs(60)));
        for _ in 0..200 {
            if poster.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let slow = || poster.post_action(json!({"slow":true}), Duration::from_millis(60));
        // 两次失败 + 一次成功：计数清零，不熔断。
        assert!(matches!(slow().await, PostOutcome::Unknown(_)));
        assert!(matches!(slow().await, PostOutcome::Unknown(_)));
        assert!(matches!(
            poster.post_action(json!({}), Duration::from_secs(1)).await,
            PostOutcome::Reply(Ok(_))
        ));
        assert!(matches!(slow().await, PostOutcome::Unknown(_)));
        assert!(matches!(slow().await, PostOutcome::Unknown(_)));
        // 再连续失败到第 3 次：熔断。之后一律当作没发出去（走 HTTP），哪怕连接是好的。
        assert!(matches!(slow().await, PostOutcome::Unknown(_)));
        assert!(poster.is_connected());
        assert_eq!(
            poster.post_action(json!({}), Duration::from_secs(1)).await,
            PostOutcome::NotSent
        );
    }
}
