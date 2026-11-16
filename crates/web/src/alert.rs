//! 告警通知（Telegram）。
//!
//! 默认只播报订单相关事件（开仓、补仓、平仓及其失败结果）。机会、状态、对账与服务通知只记日志；
//! `ARB_ALERT_MODE=all` 可恢复全部通知。主动查询命令走独立回复通道，不受播报范围影响。
//!
//! - **可选**：没配 `ARB_ALERT_TELEGRAM_TOKEN` / `ARB_ALERT_TELEGRAM_CHAT` 就什么都不发。
//!   密钥由用户自己写进 `.env`，不经过任何对话。
//! - **去重限频**：同一个 key 在冷却时间内只发一次（对账一直不干净不会每分钟响一次），
//!   全局每分钟最多几条（出问题时不会被自己的告警淹没，也不会把 Telegram 打到限频）。
//! - **不阻塞**：发送在后台任务里，带超时；失败只记日志，不影响交易主流程。
//! - **不泄密**：消息里不放地址、账户号、密钥；发送前再过一遍 [`redact`]，把长十六进制串
//!   （地址、密钥、哈希）抹掉。日志里也不打印 reqwest 的错误文本（它会带上含 token 的 URL）。
//! - **按钮**：只有命令机器人在跑、且告警发到的就是管理员私聊时才挂（[`Alerter::enable_actions`]），
//!   否则点了也会被机器人的授权静默忽略。按钮背后的动作由 [`crate::telegram`] 处理。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::{info, warn};

/// 同一个 key 多久内只发一次。
const COOLDOWN: Duration = Duration::from_secs(30 * 60);
/// 全局限频：一个窗口内最多几条。
const RATE_WINDOW: Duration = Duration::from_secs(60);
const RATE_MAX: usize = 6;
/// 静音时长上限：静音会挡掉包括「对账不一致」在内的所有告警，不能一直静下去。
const MAX_MUTE: Duration = Duration::from_secs(24 * 3600);
/// 单条发送的超时。
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// Telegram 单条消息上限 4096 字符：留余量。
const MAX_CHARS: usize = 3500;

/// 消息下面的内联按钮：每行若干个（显示文字，回调数据）。
pub type Keyboard = Vec<Vec<(String, String)>>;

/// 消息送到哪里。抽出来是为了不联网也能测去重与限频。
pub trait Sink: Send + Sync + 'static {
    fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

    /// 带按钮发。不支持按钮的接收端只发文字。
    fn send_with_buttons(
        &self,
        text: String,
        keyboard: Keyboard,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let _ = keyboard;
        self.send(text)
    }
}

/// Telegram Bot API。
pub struct Telegram {
    client: reqwest::Client,
    token: String,
    chat: String,
}

impl Sink for Telegram {
    fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        self.send_with_buttons(text, Vec::new())
    }

    fn send_with_buttons(
        &self,
        text: String,
        keyboard: Keyboard,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let body = message_body(&self.chat, &text, &keyboard);
        let request = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.token
            ))
            .json(&body)
            .timeout(SEND_TIMEOUT);
        Box::pin(async move {
            // reqwest 的错误文本会带上请求 URL（含 token）：只报类别，不报原文。
            match request.send().await {
                Ok(response) if response.status().is_success() => Ok(()),
                Ok(response) => Err(format!("Telegram 返回 HTTP {}", response.status())),
                Err(error) if error.is_timeout() => Err("Telegram 请求超时".into()),
                Err(error) if error.is_connect() => Err("连不上 Telegram".into()),
                Err(_) => Err("Telegram 请求失败".into()),
            }
        })
    }
}

/// `sendMessage` 的请求体；有按钮时带 `reply_markup.inline_keyboard`。
fn message_body(chat: &str, text: &str, keyboard: &Keyboard) -> serde_json::Value {
    let mut body = serde_json::json!({
        "chat_id": chat,
        "text": text,
        "disable_web_page_preview": true,
    });
    if !keyboard.is_empty() {
        let rows: Vec<Vec<serde_json::Value>> = keyboard
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(label, data)| serde_json::json!({"text": label, "callback_data": data}))
                    .collect()
            })
            .collect();
        body["reply_markup"] = serde_json::json!({"inline_keyboard": rows});
    }
    body
}

#[derive(Default)]
struct State {
    /// 静音到什么时候（Telegram 里 /mute）。静音期间什么都不发，也计入被挡掉的条数。
    muted_until: Option<Instant>,
    /// key → 上次发送时刻。
    last: HashMap<String, Instant>,
    /// 窗口内已发送的时刻。
    recent: Vec<Instant>,
    /// 被去重 / 限频挡掉的条数（启动以来）。
    suppressed: u64,
}

/// 主动播报范围。用显式事件类型筛选，不从消息文本猜测「是否交易」。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationMode {
    Trades,
    All,
}

impl NotificationMode {
    fn parse(raw: Option<&str>) -> Option<Self> {
        match raw.unwrap_or("trades").trim().to_ascii_lowercase().as_str() {
            "" | "trades" => Some(Self::Trades),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    General,
    Order,
}

/// 告警发送器。没配置时是空操作。
pub struct Alerter {
    sink: Option<Arc<dyn Sink>>,
    mode: NotificationMode,
    state: Mutex<State>,
    /// 消息下面挂不挂动作按钮（见 [`Alerter::enable_actions`]）。
    actions: AtomicBool,
}

impl Alerter {
    /// 从环境变量组装。两个变量都要有；缺一个就当没配置（并提示一次）。
    pub fn from_env(client: &reqwest::Client) -> Arc<Self> {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let (token, chat) = (
            var("ARB_ALERT_TELEGRAM_TOKEN"),
            var("ARB_ALERT_TELEGRAM_CHAT"),
        );
        let sink: Option<Arc<dyn Sink>> = match (token, chat) {
            (Some(token), Some(chat)) => {
                info!("告警通知已启用（Telegram）");
                Some(Arc::new(Telegram {
                    client: client.clone(),
                    token,
                    chat,
                }))
            }
            (None, None) => None,
            _ => {
                warn!(
                    "告警通知没启用：ARB_ALERT_TELEGRAM_TOKEN 与 ARB_ALERT_TELEGRAM_CHAT 要同时配置"
                );
                None
            }
        };
        let mode = NotificationMode::parse(var("ARB_ALERT_MODE").as_deref()).unwrap_or_else(|| {
            warn!("ARB_ALERT_MODE 只能是 trades / all，按 trades（仅订单）处理");
            NotificationMode::Trades
        });
        info!(mode = mode.as_str(), "Telegram 主动播报范围");
        Self::with_mode(sink, mode)
    }

    #[cfg(test)]
    pub fn with_sink(sink: Option<Arc<dyn Sink>>) -> Arc<Self> {
        Self::with_mode(sink, NotificationMode::All)
    }

    pub fn with_mode(sink: Option<Arc<dyn Sink>>, mode: NotificationMode) -> Arc<Self> {
        Arc::new(Self {
            sink,
            mode,
            state: Mutex::new(State::default()),
            actions: AtomicBool::new(false),
        })
    }

    pub fn enabled(&self) -> bool {
        self.sink.is_some()
    }

    pub fn mode(&self) -> NotificationMode {
        self.mode
    }

    /// 开仓、补仓、平仓的结果；不受播报范围过滤，仍尊重静音、脱敏与全局限频。
    pub fn notify_order(&self, text: impl Into<String>) -> bool {
        self.notify_order_keyed("", text)
    }

    /// 同一笔订单的重复失败按 key 去重。
    pub fn notify_order_keyed(&self, key: &str, text: impl Into<String>) -> bool {
        self.enqueue(Instant::now(), key, text.into(), Vec::new(), Kind::Order)
    }

    /// 命令机器人启动、且告警发到的就是管理员私聊时调用：之后 [`Alerter::notify_with_buttons`] 才真挂按钮。
    pub fn enable_actions(&self) {
        self.actions.store(true, Ordering::SeqCst);
    }

    /// 发出去的消息能不能带动作按钮。
    pub fn actions_enabled(&self) -> bool {
        self.sink.is_some() && self.actions.load(Ordering::SeqCst)
    }

    /// 发一条告警。`key` 相同的在冷却时间内只发一次；返回是否真的发出去了（没配置、
    /// 被去重、被限频都返回 `false`）。发送本身在后台任务里，不等它。
    pub fn notify(&self, key: &str, text: impl Into<String>) -> bool {
        self.notify_at(Instant::now(), key, text.into())
    }

    /// 不受冷却限制的一次性通知（启动、恢复这类每次都该发的），仍受全局限频。
    pub fn notify_always(&self, text: impl Into<String>) -> bool {
        self.notify_at(Instant::now(), "", text.into())
    }

    /// 同 [`Alerter::notify`]，消息下面再挂按钮（没 [`Alerter::enable_actions`] 时只发文字）。
    pub fn notify_with_buttons(
        &self,
        key: &str,
        text: impl Into<String>,
        keyboard: Keyboard,
    ) -> bool {
        let keyboard = if self.actions.load(Ordering::SeqCst) {
            keyboard
        } else {
            Vec::new()
        };
        self.notify_keyboard(Instant::now(), key, text.into(), keyboard)
    }

    fn notify_at(&self, now: Instant, key: &str, text: String) -> bool {
        self.notify_keyboard(now, key, text, Vec::new())
    }

    fn notify_keyboard(&self, now: Instant, key: &str, text: String, keyboard: Keyboard) -> bool {
        self.enqueue(now, key, text, keyboard, Kind::General)
    }

    fn enqueue(
        &self,
        now: Instant,
        key: &str,
        text: String,
        keyboard: Keyboard,
        kind: Kind,
    ) -> bool {
        let Some((sink, text)) = self.admit(now, key, text, false, kind) else {
            return false;
        };
        // 在后台发：交易主流程不等它，失败只记日志。
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(deliver(sink, text, keyboard));
        }
        true
    }

    /// 停机用：**等发送完成**再返回（最多 `SEND_TIMEOUT` + 2 秒），因为进程马上要退出、后台任务
    /// 来不及发。不受全局限频约束（停机前的忙碌时段额度常常已经用完，而这是最后一条消息），
    /// 但仍尊重操作者主动设置的静音。
    pub async fn notify_final(&self, text: impl Into<String>) -> bool {
        let Some((sink, text)) = self.admit(Instant::now(), "", text.into(), true, Kind::General)
        else {
            return false;
        };
        deliver(sink, text, Vec::new()).await;
        true
    }

    /// 静音、冷却、全局限频三道关。放行时返回要发的接收端和已脱敏、截断的文本。
    /// `last_word` 跳过全局限频（见 [`Alerter::notify_final`]）。
    fn admit(
        &self,
        now: Instant,
        key: &str,
        text: String,
        last_word: bool,
        kind: Kind,
    ) -> Option<(Arc<dyn Sink>, String)> {
        let sink = self.sink.as_ref()?;
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // 范围过滤必须在冷却与限频之前：大量机会提醒不能挤掉真正的订单结果。
            if self.mode == NotificationMode::Trades && kind != Kind::Order {
                state.suppressed += 1;
                return None;
            }
            if state.muted_until.is_some_and(|until| now < until) {
                state.suppressed += 1;
                return None;
            }
            if !key.is_empty()
                && state
                    .last
                    .get(key)
                    .is_some_and(|at| now.duration_since(*at) < COOLDOWN)
            {
                state.suppressed += 1;
                return None;
            }
            state
                .recent
                .retain(|at| now.duration_since(*at) < RATE_WINDOW);
            if !last_word && state.recent.len() >= RATE_MAX {
                state.suppressed += 1;
                return None;
            }
            state.recent.push(now);
            if !key.is_empty() {
                state.last.insert(key.to_string(), now);
            }
            // 别让 key 表无限长大。
            state
                .last
                .retain(|_, at| now.duration_since(*at) < COOLDOWN * 4);
        }
        Some((Arc::clone(sink), truncate(redact(&text))))
    }

    /// 静音一段时间（最长 24 小时）。返回实际静音的时长。
    pub fn mute(&self, duration: Duration) -> Duration {
        let duration = duration.min(MAX_MUTE);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.muted_until = Some(Instant::now() + duration);
        duration
    }

    pub fn unmute(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.muted_until = None;
    }

    /// 还要静音多久。没静音返回 `None`。
    pub fn muted_for(&self) -> Option<Duration> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .muted_until
            .and_then(|until| until.checked_duration_since(Instant::now()))
            .filter(|left| !left.is_zero())
    }

    /// 启动以来被去重 / 限频挡掉的条数（给健康检查看）。
    pub fn suppressed(&self) -> u64 {
        self.state
            .lock()
            .map(|state| state.suppressed)
            .unwrap_or_default()
    }
}

/// 发送并把失败记成日志；整体有时限，卡住的接收端拖不住调用方。
async fn deliver(sink: Arc<dyn Sink>, text: String, keyboard: Keyboard) {
    let send = if keyboard.is_empty() {
        sink.send(text)
    } else {
        sink.send_with_buttons(text, keyboard)
    };
    match tokio::time::timeout(SEND_TIMEOUT + Duration::from_secs(2), send).await {
        Ok(Ok(())) => {}
        Ok(Err(reason)) => warn!("告警没发出去：{reason}"),
        Err(_) => warn!("告警没发出去：超时"),
    }
}

/// 抹掉消息里的长十六进制串（地址、密钥、哈希）：告警走第三方，宁可多抹。
/// 32 位以上的连续十六进制（可带 `0x`）一律换成 `…`。
pub fn redact(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let start = i;
        if chars[i] == '0' && chars.get(i + 1) == Some(&'x') {
            i += 2;
        }
        let digits_from = i;
        while i < chars.len() && chars[i].is_ascii_hexdigit() {
            i += 1;
        }
        if i - digits_from >= 32 {
            out.push('…');
        } else if i > start {
            out.extend(&chars[start..i]);
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

fn truncate(text: String) -> String {
    if text.chars().count() <= MAX_CHARS {
        return text;
    }
    let mut cut: String = text.chars().take(MAX_CHARS).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting(Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>);

    impl Sink for Counting {
        fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            self.1.lock().unwrap().push(text);
            Box::pin(async { Ok(()) })
        }
    }

    fn alerter() -> (Arc<Alerter>, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
        let sent = Arc::new(AtomicUsize::new(0));
        let texts = Arc::new(Mutex::new(Vec::new()));
        let alerter = Alerter::with_sink(Some(Arc::new(Counting(
            Arc::clone(&sent),
            Arc::clone(&texts),
        ))));
        (alerter, sent, texts)
    }

    #[test]
    fn notification_mode_defaults_to_orders_only() {
        assert_eq!(
            NotificationMode::parse(None),
            Some(NotificationMode::Trades)
        );
        assert_eq!(
            NotificationMode::parse(Some(" TRADES ")),
            Some(NotificationMode::Trades)
        );
        assert_eq!(
            NotificationMode::parse(Some("all")),
            Some(NotificationMode::All)
        );
        assert_eq!(NotificationMode::parse(Some("typo")), None);
    }

    #[tokio::test]
    async fn trades_only_filters_all_generic_paths_before_rate_limiting() {
        let sent = Arc::new(AtomicUsize::new(0));
        let texts = Arc::new(Mutex::new(Vec::new()));
        let alerts = Alerter::with_mode(
            Some(Arc::new(Counting(Arc::clone(&sent), Arc::clone(&texts)))),
            NotificationMode::Trades,
        );
        alerts.enable_actions();
        // 标题含「下单」也不能冒充交易事件；机会按钮、无冷却通知、停机通知都必须过滤。
        for i in 0..100 {
            assert!(!alerts.notify(&format!("signal:{i}"), "下单机会提醒"));
        }
        assert!(!alerts.notify_always("服务启动 / 设置已更新 / 对账恢复"));
        assert!(!alerts.notify_with_buttons(
            "signal",
            "机会",
            vec![vec![("下单".into(), "o:123".into())]]
        ));
        assert!(!alerts.notify_final("服务停止").await);
        assert!(alerts.notify_order("开仓成交"));
        assert!(alerts.notify_order("补仓成交"));
        assert!(alerts.notify_order("平仓成交"));
        assert!(alerts.notify_order_keyed("close-failure:1", "平仓失败"));
        assert!(!alerts.notify_order_keyed("close-failure:1", "重复失败"));
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::SeqCst), 4);
        assert_eq!(
            *texts.lock().unwrap(),
            vec!["开仓成交", "补仓成交", "平仓成交", "平仓失败"]
        );
        assert_eq!(
            alerts.state.lock().unwrap().recent.len(),
            4,
            "被过滤的提醒不能抢交易通知的限频额度"
        );
    }

    #[tokio::test]
    async fn order_notifications_still_obey_mute_and_redaction() {
        let sent = Arc::new(AtomicUsize::new(0));
        let texts = Arc::new(Mutex::new(Vec::new()));
        let alerts = Alerter::with_mode(
            Some(Arc::new(Counting(Arc::clone(&sent), Arc::clone(&texts)))),
            NotificationMode::Trades,
        );
        alerts.mute(Duration::from_secs(60));
        assert!(!alerts.notify_order("开仓"));
        alerts.unmute();
        assert!(alerts.notify_order("平仓 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"));
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::SeqCst), 1);
        assert_eq!(texts.lock().unwrap()[0], "平仓 …");
    }

    #[tokio::test]
    async fn the_same_key_is_sent_once_per_cooldown() {
        let (alerter, sent, _) = alerter();
        let start = Instant::now();
        assert!(alerter.notify_at(start, "recon", "对账不干净".into()));
        assert!(!alerter.notify_at(
            start + Duration::from_secs(60),
            "recon",
            "对账不干净".into()
        ));
        // 别的 key 不受影响。
        assert!(alerter.notify_at(start + Duration::from_secs(61), "naked", "裸敞口".into()));
        // 冷却过了可以再发。
        assert!(alerter.notify_at(
            start + COOLDOWN + Duration::from_secs(1),
            "recon",
            "又不干净了".into()
        ));
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::SeqCst), 3);
        assert_eq!(alerter.suppressed(), 1);
    }

    #[tokio::test]
    async fn a_burst_is_rate_limited_globally() {
        let (alerter, sent, _) = alerter();
        let start = Instant::now();
        let delivered = (0..20)
            .filter(|i| alerter.notify_at(start, &format!("k{i}"), format!("消息 {i}")))
            .count();
        assert_eq!(delivered, RATE_MAX);
        // 窗口过去后恢复。
        assert!(alerter.notify_at(
            start + RATE_WINDOW + Duration::from_secs(1),
            "later",
            "恢复".into()
        ));
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::SeqCst), RATE_MAX + 1);
    }

    #[test]
    fn without_credentials_nothing_is_sent() {
        let alerter = Alerter::with_sink(None);
        assert!(!alerter.enabled());
        assert!(!alerter.notify("k", "x"));
        assert!(!alerter.notify_always("x"));
    }

    #[tokio::test]
    async fn addresses_keys_and_hashes_never_leave_the_process() {
        let (alerter, _, texts) = alerter();
        alerter.notify_always(
            "仓位 live-1 已平：地址 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef，\
             签名密钥 abababababababababababababababababababababababababababababababab，盈亏 +3.41",
        );
        tokio::task::yield_now().await;
        let text = texts.lock().unwrap()[0].clone();
        assert!(
            !text.contains("deadbeefdead") && !text.contains("abababababab"),
            "{text}"
        );
        assert!(text.contains("live-1") && text.contains("+3.41"), "{text}");
    }

    #[test]
    fn redaction_keeps_short_hex_words_and_ids() {
        assert_eq!(
            redact("live-1700000000000 cafe deadbeef"),
            "live-1700000000000 cafe deadbeef"
        );
        assert_eq!(redact("x 0x1234 y"), "x 0x1234 y");
        assert_eq!(
            redact("k=0123456789abcdef0123456789abcdef!"),
            "k=…!",
            "32 位以上的十六进制抹掉"
        );
    }

    #[test]
    fn long_messages_are_truncated() {
        let long = "字".repeat(MAX_CHARS + 50);
        let cut = truncate(long);
        assert_eq!(cut.chars().count(), MAX_CHARS + 1);
    }

    #[tokio::test]
    async fn muting_silences_everything_until_it_expires_and_is_capped() {
        let (alerter, sent, _) = alerter();
        let start = Instant::now();
        let muted = alerter.mute(Duration::from_secs(48 * 3600));
        assert_eq!(muted, MAX_MUTE, "最长 24 小时");
        assert!(alerter.muted_for().is_some());
        assert!(!alerter.notify_at(start, "k", "静音中".into()));
        assert!(!alerter.notify_at(start, "", "启动这类也不发".into()));
        alerter.unmute();
        assert!(alerter.muted_for().is_none());
        assert!(alerter.notify_at(start, "k", "取消静音后正常".into()));
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::SeqCst), 1);
        assert_eq!(alerter.suppressed(), 2);
    }

    /// 记下每条消息带的按钮。
    struct Keyed(Arc<Mutex<Vec<(String, Keyboard)>>>);

    impl Sink for Keyed {
        fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
            self.send_with_buttons(text, Vec::new())
        }
        fn send_with_buttons(
            &self,
            text: String,
            keyboard: Keyboard,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
            self.0.lock().unwrap().push((text, keyboard));
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn buttons_are_attached_only_once_actions_are_enabled_and_still_deduplicate() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let alerter = Alerter::with_sink(Some(Arc::new(Keyed(Arc::clone(&sent)))));
        let keyboard = || vec![vec![("🛒 下单…".to_string(), "o:abc".to_string())]];
        assert!(!alerter.actions_enabled());
        assert!(alerter.notify_with_buttons("a", "first", keyboard()));
        alerter.enable_actions();
        assert!(alerter.actions_enabled());
        assert!(alerter.notify_with_buttons("b", "second", keyboard()));
        assert!(
            !alerter.notify_with_buttons("b", "again", keyboard()),
            "同一个 key 照样去重"
        );
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(
            sent[0],
            ("first".to_string(), Vec::new()),
            "没开启时只发文字"
        );
        assert_eq!(sent[1], ("second".to_string(), keyboard()));
        // 真正发给 Telegram 的请求体：按钮在 reply_markup.inline_keyboard 里，没有按钮就不带。
        assert_eq!(
            message_body("42", "hi", &keyboard()),
            serde_json::json!({
                "chat_id": "42",
                "text": "hi",
                "disable_web_page_preview": true,
                "reply_markup": {"inline_keyboard": [[{"text": "🛒 下单…", "callback_data": "o:abc"}]]},
            })
        );
        assert!(
            message_body("42", "hi", &Vec::new())
                .get("reply_markup")
                .is_none()
        );
        // 没配接收端时永远不算开启。
        let silent = Alerter::with_sink(None);
        silent.enable_actions();
        assert!(!silent.actions_enabled());
    }
}
