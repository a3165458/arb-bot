//! Telegram 机器人：手机上的菜单与只读查询，**只有你自己能用**。
//!
//! # 授权
//!
//! 只认「私聊，且发消息的用户 ID 等于配置的管理员 ID」。管理员 ID 默认取
//! `ARB_ALERT_TELEGRAM_CHAT`（私聊的 chat id 就是用户 ID）；那个值是群 / 频道（负数）时必须
//! 另配 `ARB_TELEGRAM_ADMIN_ID`，否则命令功能不启用。别人（包括在群里）发来的任何消息和按钮
//! 一律**静默忽略** —— 不回复、不报错，也就不暴露这个机器人是谁的、能做什么。
//!
//! # 能做什么
//!
//! 只有只读查询（状态、持仓与盈亏、今日盈亏、账户可用保证金、当前最优机会）、告警静音，
//! 以及「暂停 / 恢复开新仓」。**没有下单、也没有平仓**：手机上一个手滑的按钮不该动真钱，
//! 平仓请到面板里输仓位号确认。暂停的只是「加风险」，平仓、规则、对账照常。
//!
//! # 通道
//!
//! 长轮询 `getUpdates`（同一个 token 同一时间只能有一个进程在轮询，另一个会收到 409）。
//! 启动时丢掉积压的旧消息，免得停机期间点的按钮在重启后才执行。
//! 错误文本一律不带 URL（里面有 token）。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::AppState;

type Fut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

// ───────────────────────────── 协议类型 ─────────────────────────────

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    pub message: Option<Message>,
    pub callback_query: Option<Callback>,
}

#[derive(Debug, Deserialize)]
pub struct Message {
    pub chat: Chat,
    pub from: Option<User>,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct User {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct Callback {
    pub id: String,
    pub from: User,
    pub message: Option<Message>,
    pub data: Option<String>,
}

/// 内联按钮：显示文字与回调数据（就是命令名）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Button {
    pub label: String,
    pub data: String,
}

fn button(label: &str, data: &str) -> Button {
    Button {
        label: label.into(),
        data: data.into(),
    }
}

#[derive(Debug)]
pub enum PollError {
    /// 另一个进程在用同一个 token 轮询（409）。
    Conflict,
    Other(String),
}

/// Telegram Bot API 的最小子集。抽出来是为了不联网也能测授权与命令。
pub trait Api: Send + Sync + 'static {
    fn get_updates(&self, offset: i64, timeout_s: u64) -> Fut<Result<Vec<Update>, PollError>>;
    fn send(
        &self,
        chat_id: i64,
        text: String,
        keyboard: Vec<Vec<Button>>,
    ) -> Fut<Result<(), String>>;
    fn answer_callback(&self, id: String) -> Fut<Result<(), String>>;
    fn set_commands(&self, commands: Vec<(String, String)>) -> Fut<Result<(), String>>;
}

/// 命令背后的数据与控制。真实实现读看板状态；测试里换成桩。
pub trait Backend: Send + Sync + 'static {
    fn status(&self) -> Fut<String>;
    fn positions(&self) -> Fut<String>;
    fn daily(&self) -> Fut<String>;
    fn balance(&self) -> Fut<String>;
    fn top(&self) -> Fut<String>;
    /// 暂停 / 恢复开新仓。实盘没开返回错误。
    fn set_opens_paused(&self, paused: bool) -> Result<(), String>;
    fn opens_paused(&self) -> bool;
    /// 静音（分钟），返回实际时长；取消静音。
    fn mute(&self, minutes: u64) -> Duration;
    fn unmute(&self);
}

// ───────────────────────────── 菜单与路由 ─────────────────────────────

/// 注册到 Telegram 「/」菜单里的命令。
pub const COMMANDS: &[(&str, &str)] = &[
    ("menu", "主菜单"),
    ("status", "系统状态"),
    ("positions", "当前持仓与盈亏"),
    ("daily", "今日已实现盈亏"),
    ("balance", "各账户可用保证金"),
    ("top", "当前最优资金费机会"),
    ("mute", "静音告警（/mute 60 = 60 分钟）"),
    ("unmute", "取消静音"),
    ("pause", "暂停开新仓"),
    ("resume", "恢复开新仓"),
];

/// 静音的默认与上限（分钟）。
const MUTE_DEFAULT_MIN: u64 = 60;
const MUTE_MAX_MIN: u64 = 24 * 60;

/// 一条回复。
pub struct Reply {
    pub text: String,
    pub keyboard: Vec<Vec<Button>>,
}

pub struct Bot {
    api: Arc<dyn Api>,
    backend: Arc<dyn Backend>,
    admin: i64,
    /// 被忽略的未授权消息条数（只用来限频地留一条日志，不含内容）。
    ignored: AtomicU64,
    /// 只读查询有总时限，交易所慢请求不能永久占住查询槽位。
    query_timeout: Duration,
}

impl Bot {
    pub fn new(api: Arc<dyn Api>, backend: Arc<dyn Backend>, admin: i64) -> Self {
        Self {
            api,
            backend,
            admin,
            ignored: AtomicU64::new(0),
            query_timeout: Duration::from_secs(20),
        }
    }

    /// 是不是管理员本人的私聊。
    fn authorized(&self, chat: Option<&Chat>, from: Option<&User>) -> bool {
        matches!((chat, from), (Some(chat), Some(user))
            if chat.kind == "private" && chat.id == self.admin && user.id == self.admin)
    }

    fn ignore(&self) {
        let n = self.ignored.fetch_add(1, Ordering::Relaxed) + 1;
        // 只留一条不含内容的日志，且限频：被人刷消息时不把日志灌满。
        if n == 1 || n.is_multiple_of(50) {
            warn!(count = n, "收到非管理员的 Telegram 消息，已忽略");
        }
    }

    /// 只有已授权的只读查询能进入后台槽位；控制命令仍按收到的顺序执行。
    fn authorized_query(&self, update: &Update) -> bool {
        let command = if let Some(callback) = &update.callback_query {
            if !self.authorized(
                callback.message.as_ref().map(|m| &m.chat),
                Some(&callback.from),
            ) {
                return false;
            }
            callback.data.clone().unwrap_or_default()
        } else if let Some(message) = &update.message {
            if !self.authorized(Some(&message.chat), message.from.as_ref()) {
                return false;
            }
            parse_command(message.text.as_deref().unwrap_or_default())
                .map_or(String::new(), |(command, _)| command)
        } else {
            return false;
        };
        matches!(
            command.to_ascii_lowercase().as_str(),
            "status" | "positions" | "daily" | "balance" | "top"
        )
    }

    async fn dispatch(self: &Arc<Self>, update: Update, queries: &mut tokio::task::JoinSet<()>) {
        while let Some(result) = queries.try_join_next() {
            if result.is_err() {
                warn!("Telegram 只读查询任务异常结束");
            }
        }
        if !self.authorized_query(&update) {
            self.handle(update).await;
        } else if queries.len() < MAX_QUERY_TASKS {
            let bot = Arc::clone(self);
            queries.spawn(async move {
                bot.handle(update).await;
            });
        } else {
            if let Some(callback) = update.callback_query {
                self.acknowledge(callback.id).await;
            }
            self.deliver(
                self.admin,
                self.plain("已有查询正在进行，请稍后重试；暂停 / 恢复开仓仍可使用。".into()),
            )
            .await;
        }
    }

    async fn acknowledge(&self, id: String) {
        let _ = tokio::time::timeout(Duration::from_secs(2), self.api.answer_callback(id)).await;
    }

    async fn query(&self, command: &str, future: Fut<String>) -> String {
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(self.query_timeout, future).await;
        info!(
            command,
            elapsed_ms = started.elapsed().as_millis() as u64,
            timed_out = result.is_err(),
            "Telegram 只读查询完成"
        );
        result.unwrap_or_else(|_| format!("/{command} 查询超过 {} 秒，未得到完整结果；没有把未知数据按 0 计算。请稍后重试，暂停 / 恢复开仓不受该查询阻塞。", self.query_timeout.as_secs()))
    }

    /// 处理一条更新。
    pub async fn handle(&self, update: Update) {
        if let Some(callback) = update.callback_query {
            let allowed = self.authorized(
                callback.message.as_ref().map(|message| &message.chat),
                Some(&callback.from),
            );
            if !allowed {
                self.ignore();
                return;
            }
            // 先应答，按钮上的转圈才会停。
            self.acknowledge(callback.id.clone()).await;
            let data = callback.data.unwrap_or_default();
            let reply = self.reply_to(&data, "").await;
            self.deliver(self.admin, reply).await;
            return;
        }
        let Some(message) = update.message else {
            return;
        };
        if !self.authorized(Some(&message.chat), message.from.as_ref()) {
            self.ignore();
            return;
        }
        let text = message.text.unwrap_or_default();
        let reply = match parse_command(&text) {
            Some((command, args)) => self.reply_to(&command, &args).await,
            None => Reply {
                text: "发 /menu 打开菜单。".into(),
                keyboard: self.menu(),
            },
        };
        self.deliver(self.admin, reply).await;
    }

    async fn deliver(&self, chat_id: i64, reply: Reply) {
        if let Err(reason) = self.api.send(chat_id, reply.text, reply.keyboard).await {
            warn!("Telegram 回复没发出去：{reason}");
        }
    }

    /// 主菜单按钮。暂停 / 恢复按钮按当前状态换。
    fn menu(&self) -> Vec<Vec<Button>> {
        let paused = self.backend.opens_paused();
        vec![
            vec![button("📊 状态", "status"), button("📌 持仓", "positions")],
            vec![
                button("💰 今日盈亏", "daily"),
                button("🏦 保证金", "balance"),
            ],
            vec![button("🔝 机会", "top")],
            vec![
                button("🔕 静音 1 小时", "mute"),
                button("🔔 取消静音", "unmute"),
            ],
            vec![if paused {
                button("▶️ 恢复开仓", "resume")
            } else {
                button("⏸ 暂停开仓", "pause")
            }],
        ]
    }

    /// 路由一条命令。命令名不区分大小写；未知命令给出提示而不是沉默。
    pub async fn reply_to(&self, command: &str, args: &str) -> Reply {
        let text = match command.to_ascii_lowercase().as_str() {
            "start" | "menu" | "help" => {
                let paused = if self.backend.opens_paused() {
                    "\n⏸ 开新仓当前已暂停。"
                } else {
                    ""
                };
                return Reply {
                    text: format!(
                        "arb-bot 菜单：点下面的按钮，或用 / 菜单里的命令。\n只有查询和静音、暂停开仓，这里不能下单也不能平仓。{paused}"
                    ),
                    keyboard: self.menu(),
                };
            }
            "status" => self.query("status", self.backend.status()).await,
            "positions" => self.query("positions", self.backend.positions()).await,
            "daily" => self.query("daily", self.backend.daily()).await,
            "balance" => self.query("balance", self.backend.balance()).await,
            "top" => self.query("top", self.backend.top()).await,
            "mute" => {
                let minutes = match args.trim() {
                    "" => MUTE_DEFAULT_MIN,
                    raw => match raw.parse::<u64>() {
                        Ok(minutes) if minutes > 0 => minutes.min(MUTE_MAX_MIN),
                        _ => {
                            return self.plain(format!(
                                "静音分钟数要是正整数，例如 /mute 60（最长 {MUTE_MAX_MIN}）。"
                            ));
                        }
                    },
                };
                let muted = self.backend.mute(minutes);
                format!(
                    "🔕 已静音 {} 分钟：期间所有告警都不发（包括对账不一致）。/unmute 取消。",
                    muted.as_secs() / 60
                )
            }
            "unmute" => {
                self.backend.unmute();
                "🔔 已取消静音。".to_string()
            }
            "pause" => match self.backend.set_opens_paused(true) {
                Ok(()) => {
                    "⏸ 已暂停开新仓：网页里的实盘下单会被拒绝。平仓、规则、对账照常。/resume 恢复。\
                    暂停状态已保存，重启后仍保持暂停。"
                        .to_string()
                }
                Err(reason) => format!("暂停失败：{reason}"),
            },
            "resume" => match self.backend.set_opens_paused(false) {
                Ok(()) => "▶️ 已恢复开新仓。".to_string(),
                Err(reason) => format!("恢复失败：{reason}"),
            },
            other => format!("不认识命令 /{other}。发 /menu 看有哪些。"),
        };
        Reply {
            text,
            keyboard: self.menu(),
        }
    }

    fn plain(&self, text: String) -> Reply {
        Reply {
            text,
            keyboard: self.menu(),
        }
    }
}

/// `/status`、`/status@my_bot`、`/mute 30` → （命令，参数）。不是命令返回 `None`。
pub fn parse_command(text: &str) -> Option<(String, String)> {
    let text = text.trim();
    let rest = text.strip_prefix('/')?;
    let (head, args) = match rest.split_once(char::is_whitespace) {
        Some((head, args)) => (head, args.trim()),
        None => (rest, ""),
    };
    let command = head.split('@').next().unwrap_or(head);
    (!command.is_empty()).then(|| (command.to_string(), args.to_string()))
}

// ───────────────────────────── 长轮询主循环 ─────────────────────────────

/// 长轮询等待多久（秒）。HTTP 超时要比它长。
const POLL_SECONDS: u64 = 25;
/// 有界只读并发，避免反复点击放大交易所账户接口请求。
const MAX_QUERY_TASKS: usize = 2;

/// 启动时丢掉积压的旧更新，返回下一个 offset。停机期间点的按钮不该在重启后才执行。
pub async fn skip_backlog(api: &dyn Api) -> i64 {
    match api.get_updates(-1, 0).await {
        Ok(updates) => updates
            .iter()
            .map(|update| update.update_id + 1)
            .max()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

pub async fn run(bot: Arc<Bot>) {
    let commands = COMMANDS
        .iter()
        .map(|(name, description)| (name.to_string(), description.to_string()))
        .collect();
    if let Err(reason) = bot.api.set_commands(commands).await {
        warn!("注册 Telegram 菜单命令失败：{reason}");
    }
    let mut offset = skip_backlog(bot.api.as_ref()).await;
    let mut backoff = 1u64;
    let mut last_error = String::new();
    let mut queries = tokio::task::JoinSet::new();
    loop {
        match bot.api.get_updates(offset, POLL_SECONDS).await {
            Ok(updates) => {
                backoff = 1;
                last_error.clear();
                for update in updates {
                    offset = offset.max(update.update_id + 1);
                    bot.dispatch(update, &mut queries).await;
                }
            }
            Err(PollError::Conflict) => {
                if last_error != "conflict" {
                    warn!(
                        "Telegram 轮询冲突：另一个进程在用同一个机器人 token 轮询（409），30 秒后重试"
                    );
                    last_error = "conflict".into();
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Err(PollError::Other(reason)) => {
                // 只在原因变化时 WARN：断网时每次重试都报一遍没意义。
                if last_error != reason {
                    warn!("Telegram 轮询失败：{reason}");
                    last_error = reason;
                } else {
                    debug!("Telegram 轮询仍然失败");
                }
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(60);
            }
        }
    }
}

// ───────────────────────────── 真实的 Telegram API ─────────────────────────────

pub struct HttpApi {
    client: reqwest::Client,
    base: String,
}

impl HttpApi {
    fn new(client: reqwest::Client, token: &str) -> Self {
        let root = std::env::var("ARB_TELEGRAM_API_BASE")
            .ok()
            .filter(|base| !base.trim().is_empty())
            .unwrap_or_else(|| "https://api.telegram.org".into());
        Self {
            client,
            base: format!("{}/bot{token}", root.trim_end_matches('/')),
        }
    }

    /// 调一个方法。错误只报类别 / 状态码，不带 URL（里面有 token）。
    async fn call(
        client: reqwest::Client,
        url: String,
        body: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, PollError> {
        let response = client
            .post(url)
            .json(&body)
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| {
                PollError::Other(if error.is_timeout() {
                    "请求超时".into()
                } else if error.is_connect() {
                    "连不上 Telegram".into()
                } else {
                    "请求失败".into()
                })
            })?;
        let status = response.status();
        if status.as_u16() == 409 {
            return Err(PollError::Conflict);
        }
        let value: serde_json::Value = response
            .json()
            .await
            .map_err(|_| PollError::Other(format!("HTTP {status}，响应不是 JSON")))?;
        if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            let description = value
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("未知错误");
            return Err(PollError::Other(format!("HTTP {status}：{description}")));
        }
        Ok(value)
    }
}

impl Api for HttpApi {
    fn get_updates(&self, offset: i64, timeout_s: u64) -> Fut<Result<Vec<Update>, PollError>> {
        let (client, url) = (self.client.clone(), format!("{}/getUpdates", self.base));
        Box::pin(async move {
            let value = Self::call(
                client,
                url,
                serde_json::json!({
                    "offset": offset,
                    "timeout": timeout_s,
                    "allowed_updates": ["message", "callback_query"],
                }),
                Duration::from_secs(timeout_s + 10),
            )
            .await?;
            serde_json::from_value(value.get("result").cloned().unwrap_or_default())
                .map_err(|_| PollError::Other("更新格式认不出".into()))
        })
    }

    fn send(
        &self,
        chat_id: i64,
        text: String,
        keyboard: Vec<Vec<Button>>,
    ) -> Fut<Result<(), String>> {
        let (client, url) = (self.client.clone(), format!("{}/sendMessage", self.base));
        Box::pin(async move {
            let mut body = serde_json::json!({
                "chat_id": chat_id,
                "text": truncate(&text),
                "disable_web_page_preview": true,
            });
            if !keyboard.is_empty() {
                let rows: Vec<Vec<serde_json::Value>> = keyboard
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|b| serde_json::json!({"text": b.label, "callback_data": b.data}))
                            .collect()
                    })
                    .collect();
                body["reply_markup"] = serde_json::json!({"inline_keyboard": rows});
            }
            Self::call(client, url, body, Duration::from_secs(15))
                .await
                .map(|_| ())
                .map_err(|error| match error {
                    PollError::Conflict => "冲突".into(),
                    PollError::Other(reason) => reason,
                })
        })
    }

    fn answer_callback(&self, id: String) -> Fut<Result<(), String>> {
        let (client, url) = (
            self.client.clone(),
            format!("{}/answerCallbackQuery", self.base),
        );
        Box::pin(async move {
            Self::call(
                client,
                url,
                serde_json::json!({"callback_query_id": id}),
                Duration::from_secs(10),
            )
            .await
            .map(|_| ())
            .map_err(|error| match error {
                PollError::Conflict => "冲突".into(),
                PollError::Other(reason) => reason,
            })
        })
    }

    fn set_commands(&self, commands: Vec<(String, String)>) -> Fut<Result<(), String>> {
        let client = self.client.clone();
        let base = self.base.clone();
        Box::pin(async move {
            let list: Vec<serde_json::Value> = commands
                .iter()
                .map(|(command, description)| {
                    serde_json::json!({"command": command, "description": description})
                })
                .collect();
            let map = |error: PollError| match error {
                PollError::Conflict => "冲突".to_string(),
                PollError::Other(reason) => reason,
            };
            Self::call(
                client.clone(),
                format!("{base}/setMyCommands"),
                serde_json::json!({"commands": list}),
                Duration::from_secs(10),
            )
            .await
            .map_err(map)?;
            // 让聊天输入框旁边的菜单按钮显示命令列表。
            Self::call(
                client,
                format!("{base}/setChatMenuButton"),
                serde_json::json!({"menu_button": {"type": "commands"}}),
                Duration::from_secs(10),
            )
            .await
            .map(|_| ())
            .map_err(map)
        })
    }
}

fn truncate(text: &str) -> String {
    const MAX: usize = 3800;
    if text.chars().count() <= MAX {
        return text.to_string();
    }
    let mut cut: String = text.chars().take(MAX).collect();
    cut.push('…');
    cut
}

// ───────────────────────────── 真实的后端：读看板状态 ─────────────────────────────

pub struct AppBackend {
    pub state: Arc<AppState>,
}

fn money(value: Decimal) -> String {
    let v = value.round_dp(2);
    if v.is_sign_negative() && !v.is_zero() {
        format!("-${}", v.abs())
    } else {
        format!("+${v}")
    }
}

fn pct(value: Decimal) -> String {
    format!("{}%", (value * Decimal::ONE_HUNDRED).round_dp(1))
}

fn human_duration(seconds: i64) -> String {
    let seconds = seconds.max(0);
    match seconds {
        s if s < 90 => format!("{s} 秒"),
        s if s < 5400 => format!("{} 分钟", s / 60),
        s if s < 172_800 => format!("{} 小时 {} 分", s / 3600, (s % 3600) / 60),
        s => format!("{} 天 {} 小时", s / 86_400, (s % 86_400) / 3600),
    }
}

fn snapshot_note(
    age: Duration,
    generated_at: chrono::DateTime<chrono::Utc>,
    interval_sec: u64,
) -> String {
    format!(
        "行情快照：{} UTC（{}前）{}",
        generated_at.format("%Y-%m-%d %H:%M:%S"),
        human_duration(age.as_secs() as i64),
        if age.as_secs() > interval_sec.saturating_mul(3) {
            " ⚠️ 已过期，不代表当前行情"
        } else {
            ""
        }
    )
}

impl Backend for AppBackend {
    fn status(&self) -> Fut<String> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut lines = vec!["📊 系统状态".to_string()];
            match state.cache.get().await {
                Some(snapshot) => lines.push(format!(
                    "扫描：{}/{} 家场所正常 · 快照 {} 前{}",
                    snapshot.report.totals.venues_ok,
                    snapshot.report.totals.venues_ok + snapshot.report.totals.venues_failed,
                    human_duration(snapshot.age().as_secs() as i64),
                    if snapshot.age().as_secs() > state.settings.scan_interval_sec * 3 {
                        " ⚠️ 已过期"
                    } else {
                        ""
                    }
                )),
                None => lines.push("扫描：首轮还没完成".into()),
            }
            match state.trade.live_health().await {
                None => lines.push("实盘：未开启（纸面）".into()),
                Some(live) if live.disconnected => {
                    let detail = state.trade.live_pending().map_or(String::new(), |pending| {
                        format!(
                            "（{} 起，已重试 {} 次：{}）",
                            pending.since.format("%m-%d %H:%M UTC"),
                            pending.attempts,
                            crate::alert::redact(&pending.error)
                                .chars()
                                .take(160)
                                .collect::<String>()
                        )
                    });
                    lines.push(format!(
                        "🚨 实盘账户暂时连不上{detail}：下单、规则与对账暂停，后台每分钟自动重连"
                    ));
                }
                Some(live) => {
                    lines.push(format!(
                        "实盘：{} · 规则轮 {} · 对账 {}",
                        if live.mode == crate::trade::LiveMode::Trade {
                            "可下单"
                        } else {
                            "只读"
                        },
                        live.last_round_age_s
                            .map_or("还没跑过".into(), |age| format!(
                                "{} 前",
                                human_duration(age)
                            )),
                        match live.reconciliation_clean {
                            Some(true) => "✅ 干净".to_string(),
                            Some(false) => format!("⚠️ 不一致（连续 {} 轮）", live.dirty_rounds),
                            None => "未知".to_string(),
                        }
                    ));
                    if live.stalled {
                        lines.push("🚨 规则轮已停：超过三个周期没跑".into());
                    }
                    lines.push(if live.opens_paused {
                        "开新仓：⏸ 已暂停".into()
                    } else {
                        "开新仓：允许".into()
                    });
                }
            }
            lines.push(format!(
                "告警：{} · 已挡掉 {} 条",
                match state.alerts.muted_for() {
                    Some(left) => format!(
                        "🔕 静音中（还剩 {}）",
                        human_duration(left.as_secs() as i64)
                    ),
                    None => "🔔 开启".to_string(),
                },
                state.alerts.suppressed()
            ));
            lines.join("\n")
        })
    }

    fn positions(&self) -> Fut<String> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let Some(snapshot) = state.cache.get().await else {
                return "首轮扫描还没完成，稍后再试。".into();
            };
            match state.trade.live_positions(&snapshot.report).await {
                Ok(view) => format!(
                    "{}\n资金费流水缓存最长 5 分钟；交易所结算入账也可能延迟，非逐秒账户盈亏。\n\n{}",
                    snapshot_note(
                        snapshot.age(),
                        snapshot.report.generated_at,
                        state.settings.scan_interval_sec
                    ),
                    render_positions(&view, chrono::Utc::now())
                ),
                Err(reason) => format!("读不出持仓：{reason}"),
            }
        })
    }

    fn daily(&self) -> Fut<String> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            match state.trade.daily().await {
                Ok(daily) => {
                    let mut text = format!(
                        "💰 今日（UTC {}）已实现盈亏\n今天结束 {} 笔：",
                        daily.date, daily.closed
                    );
                    match daily.net_usdt {
                        Some(net) => text.push_str(&format!(
                            "合计 {} USDT（价格 − 手续费 + 资金费）",
                            money(net)
                        )),
                        None => text.push_str(&format!(
                            "有 {} 笔没有盈亏记录，合计算不出来（不按 0 算）",
                            daily.unknown.len()
                        )),
                    }
                    text.push_str(&format!(
                        "\n每日亏损上限 {} USDT。只算看板台账里的平仓。",
                        state.settings.max_daily_loss_usdt
                    ));
                    text
                }
                Err(reason) => format!("读不出今日盈亏：{reason}"),
            }
        })
    }

    fn balance(&self) -> Fut<String> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let rows = state.trade.free_collaterals().await;
            if rows.is_empty() {
                return "实盘没开启，没有账户可查。".into();
            }
            let mut lines = vec!["🏦 各账户可用保证金（能拿来开新仓的部分）".to_string()];
            for (venue, result) in rows {
                lines.push(match result {
                    Ok(Some(free)) => format!("{venue}：{} USDT", free.round_dp(2)),
                    Ok(None) => format!("{venue}：交易所没给这个数"),
                    Err(reason) => format!("{venue}：没查成（{reason}）"),
                });
            }
            lines.join("\n")
        })
    }

    fn top(&self) -> Fut<String> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let Some(snapshot) = state.cache.get().await else {
                return "首轮扫描还没完成，稍后再试。".into();
            };
            let connected: Option<Vec<arb_core::Venue>> =
                state.trade.live_venues().map(<[arb_core::Venue]>::to_vec);
            let rows: Vec<_> =
                arb_exec::cli::pick(&snapshot.report, "funding", usize::MAX, None, None)
                    .into_iter()
                    .filter(|op| {
                        connected.as_ref().is_none_or(|venues| {
                            venues.contains(&op.long) && venues.contains(&op.short)
                        })
                    })
                    .take(5)
                    .collect();
            let note = snapshot_note(
                snapshot.age(),
                snapshot.report.generated_at,
                state.settings.scan_interval_sec,
            );
            if rows.is_empty() {
                return format!("现在没有可做的资金费机会（实盘只看已连接的场所）。\n{note}");
            }
            let mut lines = vec!["🔝 当前最优资金费机会（摊费后净年化）".to_string()];
            for (index, op) in rows.iter().enumerate() {
                lines.push(format!(
                    "{}. {} 多 {} / 空 {}：{}",
                    index + 1,
                    op.symbol,
                    op.long,
                    op.short,
                    pct(op.funding_apr)
                ));
            }
            lines.push(note);
            lines.push("按扫描快照排的，没做稳定性与盘口预检；下单以网页策略页为准。".into());
            lines.join("\n")
        })
    }

    fn set_opens_paused(&self, paused: bool) -> Result<(), String> {
        if self.state.trade.set_opens_paused(paused) {
            Ok(())
        } else {
            Err("看板没有开启实盘".into())
        }
    }

    fn opens_paused(&self) -> bool {
        self.state.trade.opens_paused()
    }

    fn mute(&self, minutes: u64) -> Duration {
        self.state.alerts.mute(Duration::from_secs(minutes * 60))
    }

    fn unmute(&self) {
        self.state.alerts.unmute();
    }
}

/// 持仓列表的文字版：每笔一段，价格盈亏、手续费、资金费与合计分开写。
pub fn render_positions(
    view: &crate::strategy::Positions,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    if view.open.is_empty() {
        return "📌 当前没有未平仓位。".into();
    }
    let mut blocks = vec![format!("📌 当前 {} 笔持仓", view.open.len())];
    for item in &view.open {
        let position = &item.position;
        let legs = |leg: &Option<arb_exec::LegFill>| {
            leg.as_ref()
                .map_or("—".to_string(), |leg| leg.venue.to_string())
        };
        let size = position
            .long
            .as_ref()
            .map_or(Decimal::ZERO, |leg| leg.notional_usdt);
        let mut lines = vec![
            format!(
                "{} {} · 多 {} / 空 {}",
                position.symbol,
                match position.strategy {
                    arb_exec::Strategy::Funding => "资金费",
                    arb_exec::Strategy::Spread => "价差",
                },
                legs(&position.long),
                legs(&position.short)
            ),
            format!(
                "单腿名义 {} · {} · 杠杆 {} · 已持有 {}",
                format!("${}", size.round_dp(0)),
                if position.margin_mode.is_cross() {
                    "全仓"
                } else {
                    "逐仓"
                },
                position
                    .leverage
                    .map_or("?".into(), |leverage| format!("{}x", leverage.normalize())),
                human_duration((now - position.opened_at).num_seconds())
            ),
        ];
        let pnl = item
            .evaluation
            .as_ref()
            .map(|evaluation| &evaluation.observation.pnl);
        let funding = item.funding.as_ref().and_then(|funding| funding.total_usdt);
        match (pnl.and_then(|pnl| pnl.price_pnl_usdt), pnl) {
            (Some(price), Some(pnl)) => {
                let total = item
                    .evaluation
                    .as_ref()
                    .and_then(|evaluation| evaluation.observation.net_with_funding_usdt);
                lines.push(format!(
                    "价格 {} · 手续费 -${} · 资金费 {} → 合计 {}",
                    money(price),
                    pnl.fees_usdt.round_dp(2),
                    funding.map_or("未知".to_string(), money),
                    total.map_or("未知".to_string(), money)
                ));
            }
            _ => lines.push("盈亏：这一轮缺行情，算不出".into()),
        }
        if let Some(observation) = item.evaluation.as_ref().map(|e| &e.observation) {
            lines.push(format!(
                "费差年化 {}{}",
                pct(observation.funding_apr),
                observation
                    .funding_avg_apr
                    .map_or(String::new(), |avg| format!(
                        "（近 6 小时均值 {}）",
                        pct(avg)
                    ))
            ));
        }
        lines.push(format!(
            "规则：{}",
            arb_exec::cli::describe_rules(&position.rules)
        ));
        if let Some(target) = position.rules.take_profit_usdt {
            let net = item
                .evaluation
                .as_ref()
                .and_then(|evaluation| evaluation.observation.net_with_funding_usdt);
            lines.push(format!(
                "止盈进度：{} / {} USDT（标记价，平仓前核对盘口）",
                net.map_or("未知".to_string(), money),
                target.normalize()
            ));
        }
        if let Some((_, cap)) = position.rules.auto_margin() {
            lines.push(format!(
                "累计补保证金：{} / {} USDT（结果不明的也占上限）",
                position.margin_added_usdt.normalize(),
                cap.normalize()
            ));
        }
        if let Some(evaluation) = &item.evaluation {
            for skipped in &evaluation.skipped {
                lines.push(format!("⚠️ {skipped}"));
            }
        }
        if let Some(note) = &position.note {
            lines.push(format!("备注：{note}"));
        }
        blocks.push(lines.join("\n"));
    }
    blocks.join("\n\n")
}

// ───────────────────────────── 启动 ─────────────────────────────

/// 配了 Telegram 密钥就启动命令机器人。不配、或 `ARB_TELEGRAM_COMMANDS=off` 就不启动。
pub fn spawn(state: &Arc<AppState>, client: &reqwest::Client) {
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    if var("ARB_TELEGRAM_COMMANDS").is_some_and(|value| value.eq_ignore_ascii_case("off")) {
        return;
    }
    let (Some(token), Some(chat)) = (
        var("ARB_ALERT_TELEGRAM_TOKEN"),
        var("ARB_ALERT_TELEGRAM_CHAT"),
    ) else {
        return;
    };
    // 管理员 ID：显式配的优先；否则取 chat id（私聊的 chat id 就是用户 ID，必须是正数）。
    let admin = var("ARB_TELEGRAM_ADMIN_ID")
        .and_then(|raw| raw.parse::<i64>().ok())
        .or_else(|| chat.parse::<i64>().ok().filter(|id| *id > 0));
    let Some(admin) = admin else {
        warn!(
            "Telegram 命令功能没启用：ARB_ALERT_TELEGRAM_CHAT 是群 / 频道（负数），\
             请另配 ARB_TELEGRAM_ADMIN_ID（你自己的用户 ID）"
        );
        return;
    };
    let bot = Arc::new(Bot::new(
        Arc::new(HttpApi::new(client.clone(), &token)),
        Arc::new(AppBackend {
            state: Arc::clone(state),
        }),
        admin,
    ));
    info!("Telegram 命令机器人已启动（只有管理员的私聊能用）");
    tokio::spawn(run(bot));
}

#[cfg(test)]
mod tests;
