//! 告警通知（Telegram）。
//!
//! 实盘账户里是真钱，没人 24 小时盯着面板：规则平了仓、外部平仓被识别、出现裸敞口、
//! 对账持续不干净、服务重启……都该主动推到手机上。
//!
//! - **可选**：没配 `ARB_ALERT_TELEGRAM_TOKEN` / `ARB_ALERT_TELEGRAM_CHAT` 就什么都不发。
//!   密钥由用户自己写进 `.env`，不经过任何对话。
//! - **去重限频**：同一个 key 在冷却时间内只发一次（对账一直不干净不会每分钟响一次），
//!   全局每分钟最多几条（出问题时不会被自己的告警淹没，也不会把 Telegram 打到限频）。
//! - **不阻塞**：发送在后台任务里，带超时；失败只记日志，不影响交易主流程。
//! - **不泄密**：消息里不放地址、账户号、密钥；发送前再过一遍 [`redact`]，把长十六进制串
//!   （地址、密钥、哈希）抹掉。日志里也不打印 reqwest 的错误文本（它会带上含 token 的 URL）。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
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

/// 消息送到哪里。抽出来是为了不联网也能测去重与限频。
pub trait Sink: Send + Sync + 'static {
    fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
}

/// Telegram Bot API。
pub struct Telegram {
    client: reqwest::Client,
    token: String,
    chat: String,
}

impl Sink for Telegram {
    fn send(&self, text: String) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
        let request = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/sendMessage",
                self.token
            ))
            .json(&serde_json::json!({
                "chat_id": self.chat,
                "text": text,
                "disable_web_page_preview": true,
            }))
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

/// 告警发送器。没配置时是空操作。
pub struct Alerter {
    sink: Option<Arc<dyn Sink>>,
    state: Mutex<State>,
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
        Self::with_sink(sink)
    }

    pub fn with_sink(sink: Option<Arc<dyn Sink>>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            state: Mutex::new(State::default()),
        })
    }

    pub fn enabled(&self) -> bool {
        self.sink.is_some()
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

    fn notify_at(&self, now: Instant, key: &str, text: String) -> bool {
        let Some((sink, text)) = self.admit(now, key, text, false) else {
            return false;
        };
        // 在后台发：交易主流程不等它，失败只记日志。
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(deliver(sink, text));
        }
        true
    }

    /// 停机用：**等发送完成**再返回（最多 `SEND_TIMEOUT` + 2 秒），因为进程马上要退出、后台任务
    /// 来不及发。不受全局限频约束（停机前的忙碌时段额度常常已经用完，而这是最后一条消息），
    /// 但仍尊重操作者主动设置的静音。
    pub async fn notify_final(&self, text: impl Into<String>) -> bool {
        let Some((sink, text)) = self.admit(Instant::now(), "", text.into(), true) else {
            return false;
        };
        deliver(sink, text).await;
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
    ) -> Option<(Arc<dyn Sink>, String)> {
        let sink = self.sink.as_ref()?;
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
async fn deliver(sink: Arc<dyn Sink>, text: String) {
    match tokio::time::timeout(SEND_TIMEOUT + Duration::from_secs(2), sink.send(text)).await {
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
}
