use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use super::*;

const ADMIN: i64 = 4242;

type Sent = (i64, String, Vec<Vec<Button>>);

/// 记下发出去的一切；不联网。
#[derive(Default)]
struct MockApi {
    sent: Mutex<Vec<Sent>>,
    answered: Mutex<Vec<String>>,
    commands: Mutex<Vec<(String, String)>>,
}

impl Api for MockApi {
    fn get_updates(&self, _: i64, _: u64) -> Fut<Result<Vec<Update>, PollError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn send(
        &self,
        chat_id: i64,
        text: String,
        keyboard: Vec<Vec<Button>>,
    ) -> Fut<Result<(), String>> {
        self.sent.lock().unwrap().push((chat_id, text, keyboard));
        Box::pin(async { Ok(()) })
    }
    fn answer_callback(&self, id: String) -> Fut<Result<(), String>> {
        self.answered.lock().unwrap().push(id);
        Box::pin(async { Ok(()) })
    }
    fn set_commands(&self, commands: Vec<(String, String)>) -> Fut<Result<(), String>> {
        *self.commands.lock().unwrap() = commands;
        Box::pin(async { Ok(()) })
    }
}

/// 桩后端：记下被调用的次数与控制状态。
#[derive(Default)]
struct StubBackend {
    calls: AtomicUsize,
    paused: AtomicBool,
    muted_minutes: Mutex<Option<u64>>,
    live: bool,
    query_gate: Option<Arc<tokio::sync::Notify>>,
    previews: AtomicUsize,
    /// 真正下单的次数（`order_execute`）。
    executions: AtomicUsize,
    /// 设了就让下单卡在这里，直到被放行。
    order_gate: Option<Arc<tokio::sync::Notify>>,
    /// 设了就让下一单在执行中途 panic 一次。
    panic_once: AtomicBool,
}

/// 桩计划：只有 `prompt` / `button` / `summary` 对机器人有意义。
fn stub_plan(ticket: &str) -> order::Plan {
    order::Plan {
        intent: order::Intent {
            pair: "arcus:lighter-rh".into(),
            base: "TSLA".into(),
            direction: "long_a",
        },
        long: arb_core::Venue::Arcus,
        short: arb_core::Venue::LighterRh,
        action: order::Action::Open {
            rules: arb_exec::TaskRules::default(),
        },
        mode: crate::trade::Mode::Live,
        size: Decimal::from(800),
        leverage: Decimal::from(3),
        relative_to_normal: false,
        max_total: Decimal::from(2400),
        entry_pct: Decimal::new(4, 1),
        prompt: format!("[confirm {ticket}]"),
        button: "✅ 确认实盘开仓 TSLA 800U".into(),
        summary: format!("[summary {ticket}]"),
    }
}

impl StubBackend {
    fn live() -> Self {
        Self {
            live: true,
            ..Self::default()
        }
    }
    fn text(&self, name: &str) -> Fut<String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = format!("[{name}]");
        let gate = matches!(name, "positions" | "balance")
            .then(|| self.query_gate.clone())
            .flatten();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            text
        })
    }
}

impl Backend for StubBackend {
    fn status(&self) -> Fut<String> {
        self.text("status")
    }
    fn positions(&self) -> Fut<String> {
        self.text("positions")
    }
    fn daily(&self) -> Fut<String> {
        self.text("daily")
    }
    fn balance(&self) -> Fut<String> {
        self.text("balance")
    }
    fn top(&self) -> Fut<String> {
        self.text("top")
    }
    fn set_opens_paused(&self, paused: bool) -> Result<(), String> {
        if !self.live {
            return Err("看板没有开启实盘".into());
        }
        self.paused.store(paused, Ordering::SeqCst);
        Ok(())
    }
    fn opens_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }
    fn mute(&self, minutes: u64) -> Duration {
        *self.muted_minutes.lock().unwrap() = Some(minutes);
        Duration::from_secs(minutes * 60)
    }
    fn unmute(&self) {
        *self.muted_minutes.lock().unwrap() = None;
    }
    fn order_preview(&self, ticket: String) -> Fut<Result<order::Plan, String>> {
        self.previews.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if ticket.starts_with("good") {
                Ok(stub_plan(&ticket))
            } else {
                Err("这一刻不满足开仓硬条件".into())
            }
        })
    }
    fn order_execute(&self, plan: order::Plan) -> Fut<String> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let gate = self.order_gate.clone();
        let panic = self.panic_once.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            assert!(!panic, "测试：执行中途 panic");
            if let Some(gate) = gate {
                gate.notified().await;
            }
            format!("[executed {}]", plan.summary)
        })
    }
}

fn setup(backend: StubBackend) -> (Bot, Arc<MockApi>, Arc<StubBackend>) {
    let api = Arc::new(MockApi::default());
    let backend = Arc::new(backend);
    let bot = Bot::new(
        Arc::clone(&api) as Arc<dyn Api>,
        Arc::clone(&backend) as Arc<dyn Backend>,
        ADMIN,
    );
    (bot, api, backend)
}

fn message(chat_id: i64, kind: &str, from: Option<i64>, text: &str) -> Update {
    Update {
        update_id: 1,
        message: Some(Message {
            chat: Chat {
                id: chat_id,
                kind: kind.into(),
            },
            from: from.map(|id| User { id }),
            text: Some(text.into()),
        }),
        callback_query: None,
    }
}

fn callback(chat_id: i64, from: i64, data: &str) -> Update {
    Update {
        update_id: 2,
        message: None,
        callback_query: Some(Callback {
            id: "cb-1".into(),
            from: User { id: from },
            message: Some(Message {
                chat: Chat {
                    id: chat_id,
                    kind: "private".into(),
                },
                from: None,
                text: None,
            }),
            data: Some(data.into()),
        }),
    }
}

async fn wait_for_queries(backend: &StubBackend, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.calls.load(Ordering::SeqCst) < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queries must have started");
}

#[tokio::test]
async fn slow_queries_do_not_block_pause_and_resume_and_controls_stay_ordered() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let (bot, api, backend) = setup(StubBackend {
        query_gate: Some(gate.clone()),
        ..StubBackend::live()
    });
    let bot = Arc::new(bot);
    let mut queries = tokio::task::JoinSet::new();
    bot.dispatch(
        message(ADMIN, "private", Some(ADMIN), "/positions"),
        &mut queries,
    )
    .await;
    wait_for_queries(&backend, 1).await;
    assert_eq!(queries.len(), 1);
    assert!(api.sent.lock().unwrap().is_empty());
    tokio::time::timeout(
        Duration::from_secs(2),
        bot.dispatch(callback(ADMIN, ADMIN, "pause"), &mut queries),
    )
    .await
    .unwrap();
    assert!(backend.opens_paused());
    bot.dispatch(
        message(ADMIN, "private", Some(ADMIN), "/resume"),
        &mut queries,
    )
    .await;
    assert!(!backend.opens_paused());
    assert_eq!(queries.len(), 1);
    gate.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), queries.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let sent = api.sent.lock().unwrap();
    assert!(sent[0].1.contains("已暂停"));
    assert!(sent[1].1.contains("已恢复"));
    assert!(sent[2].1.contains("[positions]"));
}

#[tokio::test]
async fn query_slots_are_bounded_and_unauthorized_messages_never_use_them() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let (bot, api, backend) = setup(StubBackend {
        query_gate: Some(gate.clone()),
        ..StubBackend::live()
    });
    let bot = Arc::new(bot);
    let mut queries = tokio::task::JoinSet::new();
    bot.dispatch(
        message(999, "private", Some(999), "/positions"),
        &mut queries,
    )
    .await;
    assert!(queries.is_empty());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert!(api.sent.lock().unwrap().is_empty());
    for command in ["/positions", "/balance"] {
        bot.dispatch(
            message(ADMIN, "private", Some(ADMIN), command),
            &mut queries,
        )
        .await;
    }
    wait_for_queries(&backend, MAX_QUERY_TASKS).await;
    bot.dispatch(callback(ADMIN, ADMIN, "positions"), &mut queries)
        .await;
    assert_eq!(queries.len(), MAX_QUERY_TASKS);
    assert_eq!(backend.calls.load(Ordering::SeqCst), MAX_QUERY_TASKS);
    assert!(api.sent.lock().unwrap()[0].1.contains("已有查询"));
    assert_eq!(api.answered.lock().unwrap().len(), 1);
    bot.dispatch(
        message(ADMIN, "private", Some(ADMIN), "/pause"),
        &mut queries,
    )
    .await;
    assert!(backend.opens_paused());
    gate.notify_waiters();
    while let Some(result) = queries.join_next().await {
        result.unwrap();
    }
    bot.dispatch(
        message(ADMIN, "private", Some(ADMIN), "/status"),
        &mut queries,
    )
    .await;
    queries.join_next().await.unwrap().unwrap();
    assert!(
        api.sent
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .1
            .contains("[status]")
    );
}

#[tokio::test]
async fn timed_out_queries_return_an_explicit_unknown_result_and_release_the_slot() {
    let (mut bot, api, _) = setup(StubBackend {
        query_gate: Some(Arc::new(tokio::sync::Notify::new())),
        ..StubBackend::live()
    });
    bot.query_timeout = Duration::from_millis(10);
    let bot = Arc::new(bot);
    let mut queries = tokio::task::JoinSet::new();
    bot.dispatch(
        message(ADMIN, "private", Some(ADMIN), "/positions"),
        &mut queries,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), queries.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(queries.is_empty());
    let text = api.sent.lock().unwrap()[0].1.clone();
    assert!(text.contains("未得到完整结果") && text.contains("未知数据"));
    assert!(!text.contains("[positions]"));
}

#[test]
fn snapshot_times_mark_stale_values_instead_of_presenting_them_as_current() {
    let now = chrono::Utc::now();
    let fresh = snapshot_note(Duration::from_secs(12), now, 60);
    assert!(fresh.contains("UTC") && fresh.contains("12 秒前"));
    assert!(!fresh.contains("已过期"));
    assert!(snapshot_note(Duration::from_secs(181), now, 60).contains("已过期"));
    assert!(snapshot_note(Duration::from_secs(180), now, 60).contains("3 分钟前"));
}

#[test]
fn position_replies_show_the_mode_and_do_not_fabricate_missing_pnl() {
    let now = chrono::Utc::now();
    for (mode, label) in [("isolated", "逐仓"), ("cross", "全仓")] {
        let position = serde_json::from_value(serde_json::json!({
            "id":"mode-view", "symbol":arb_core::Symbol::perp("BTC", "USDT"),
            "strategy":"funding", "status":"opening", "opened_at":now,
            "entry_basis_pct":"0", "expected_round_trip_cost":"0", "margin_mode":mode
        }))
        .unwrap();
        let view = crate::strategy::Positions {
            ledger: "test".into(),
            broken_lines: 0,
            closed: vec![],
            closed_total: 0,
            dismissed_count: 0,
            failed_count: 0,
            closed_page: 1,
            closed_per_page: crate::strategy::MAX_CLOSED_POSITIONS,
            stats: crate::stats::trade_stats(
                &arb_exec::Replayed {
                    orders: Default::default(),
                    positions: Default::default(),
                },
                now,
            ),
            open: vec![crate::strategy::PositionView {
                position,
                evaluation: None,
                quotes_missing: true,
                funding: None,
                breakdown: None,
                traded: None,
                dismissed: false,
            }],
        };
        let text = render_positions(&view, now);
        assert!(text.contains(label));
        assert!(text.contains("缺行情，算不出"));
        assert!(!text.contains("合计 +$0"));
    }
}

#[tokio::test]
async fn only_the_admins_private_chat_is_answered() {
    let (bot, api, backend) = setup(StubBackend::live());
    // 别的用户私聊、管理员在群里说话、别人在群里、没有发送者、伪造的按钮：一律不回。
    for update in [
        message(999, "private", Some(999), "/status"),
        message(-1001, "supergroup", Some(ADMIN), "/status"),
        message(-1001, "group", Some(999), "/pause"),
        message(ADMIN, "private", None, "/status"),
        message(ADMIN, "private", Some(999), "/status"),
        callback(ADMIN, 999, "pause"),
        callback(-1001, ADMIN, "pause"),
    ] {
        bot.handle(update).await;
    }
    assert!(api.sent.lock().unwrap().is_empty(), "不该有任何回复");
    assert!(api.answered.lock().unwrap().is_empty());
    assert_eq!(
        backend.calls.load(Ordering::SeqCst),
        0,
        "后端一次都不该被碰"
    );
    assert!(!backend.opens_paused(), "别人不能暂停开仓");
    // 管理员本人的私聊正常。
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/status"))
        .await;
    let sent = api.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, ADMIN);
    assert!(sent[0].1.contains("[status]"));
}

#[tokio::test]
async fn the_menu_lists_every_query_and_flips_the_pause_button() {
    let (bot, api, backend) = setup(StubBackend::live());
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/menu"))
        .await;
    let labels = |keyboard: &Vec<Vec<Button>>| {
        keyboard
            .iter()
            .flatten()
            .map(|b| b.data.clone())
            .collect::<Vec<_>>()
    };
    let first = api.sent.lock().unwrap()[0].2.clone();
    let data = labels(&first);
    for expected in [
        "status",
        "positions",
        "daily",
        "balance",
        "top",
        "mute",
        "unmute",
        "pause",
    ] {
        assert!(
            data.contains(&expected.to_string()),
            "{expected} 不在菜单里：{data:?}"
        );
    }
    assert!(!data.contains(&"resume".to_string()));
    // 暂停之后按钮变成「恢复」。
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/pause"))
        .await;
    assert!(backend.opens_paused());
    let after = api.sent.lock().unwrap().last().unwrap().2.clone();
    assert!(labels(&after).contains(&"resume".to_string()));
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/resume"))
        .await;
    assert!(!backend.opens_paused());
}

#[tokio::test]
async fn buttons_run_the_same_commands_and_stop_the_spinner() {
    let (bot, api, _) = setup(StubBackend::live());
    bot.handle(callback(ADMIN, ADMIN, "positions")).await;
    assert_eq!(
        api.answered.lock().unwrap().as_slice(),
        ["cb-1".to_string()]
    );
    assert!(api.sent.lock().unwrap()[0].1.contains("[positions]"));
}

#[tokio::test]
async fn mute_takes_minutes_caps_them_and_rejects_nonsense() {
    let (bot, api, backend) = setup(StubBackend::live());
    let ask = |text: &'static str| message(ADMIN, "private", Some(ADMIN), text);
    bot.handle(ask("/mute 30")).await;
    assert_eq!(*backend.muted_minutes.lock().unwrap(), Some(30));
    bot.handle(ask("/mute")).await;
    assert_eq!(
        *backend.muted_minutes.lock().unwrap(),
        Some(MUTE_DEFAULT_MIN)
    );
    bot.handle(ask("/mute 999999")).await;
    assert_eq!(
        *backend.muted_minutes.lock().unwrap(),
        Some(MUTE_MAX_MIN),
        "最长 24 小时"
    );
    bot.handle(ask("/unmute")).await;
    assert_eq!(*backend.muted_minutes.lock().unwrap(), None);
    // 乱写的不静音，并且说明怎么写。
    bot.handle(ask("/mute abc")).await;
    bot.handle(ask("/mute 0")).await;
    assert_eq!(*backend.muted_minutes.lock().unwrap(), None);
    let sent = api.sent.lock().unwrap();
    assert!(
        sent[sent.len() - 1].1.contains("正整数"),
        "{}",
        sent[sent.len() - 1].1
    );
}

#[tokio::test]
async fn pausing_without_live_trading_says_so() {
    let (bot, api, backend) = setup(StubBackend::default());
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/pause"))
        .await;
    assert!(!backend.opens_paused());
    assert!(api.sent.lock().unwrap()[0].1.contains("没有开启实盘"));
}

#[tokio::test]
async fn unknown_commands_and_plain_text_get_a_hint_not_silence() {
    let (bot, api, _) = setup(StubBackend::live());
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/close_all"))
        .await;
    bot.handle(message(ADMIN, "private", Some(ADMIN), "你好"))
        .await;
    let sent = api.sent.lock().unwrap();
    assert!(sent[0].1.contains("不认识命令 /close_all"), "{}", sent[0].1);
    assert!(sent[1].1.contains("/menu"), "{}", sent[1].1);
}

#[test]
fn commands_parse_with_bot_suffix_case_and_arguments() {
    assert_eq!(parse_command("/status"), Some(("status".into(), "".into())));
    assert_eq!(
        parse_command("/status@arb_du_bot"),
        Some(("status".into(), "".into()))
    );
    assert_eq!(
        parse_command("  /mute   45 "),
        Some(("mute".into(), "45".into()))
    );
    assert_eq!(
        parse_command("/MUTE@x 5"),
        Some(("MUTE".into(), "5".into()))
    );
    assert_eq!(parse_command("hello"), None);
    assert_eq!(parse_command("/"), None);
}

#[test]
fn the_menu_never_offers_trading_actions() {
    // 手机上不放下单 / 平仓：命令表里只能有查询、静音、暂停开仓。
    let names: Vec<&str> = COMMANDS.iter().map(|(name, _)| *name).collect();
    for forbidden in ["open", "close", "buy", "sell", "trade", "withdraw"] {
        assert!(
            !names.iter().any(|name| name.contains(forbidden)),
            "{forbidden}"
        );
    }
    assert!(names.contains(&"pause") && names.contains(&"resume"));
    // 主菜单按钮也没有下单入口：下单只挂在价差提醒下面。
    for paused in [false, true] {
        assert!(
            menu_keyboard(paused)
                .iter()
                .flatten()
                .all(|button| !button.data.contains(':'))
        );
    }
}

#[tokio::test]
async fn the_backlog_from_while_the_service_was_down_is_skipped() {
    struct Backlog;
    impl Api for Backlog {
        fn get_updates(&self, offset: i64, _: u64) -> Fut<Result<Vec<Update>, PollError>> {
            assert_eq!(offset, -1, "只取最新一条来确定 offset");
            Box::pin(async {
                Ok(vec![Update {
                    update_id: 77,
                    message: None,
                    callback_query: None,
                }])
            })
        }
        fn send(&self, _: i64, _: String, _: Vec<Vec<Button>>) -> Fut<Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn answer_callback(&self, _: String) -> Fut<Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn set_commands(&self, _: Vec<(String, String)>) -> Fut<Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
    }
    assert_eq!(skip_backlog(&Backlog).await, 78);
}

#[test]
fn updates_parse_from_the_real_telegram_shape() {
    let raw = r#"[{"update_id":10,"message":{"message_id":1,"from":{"id":4242,"is_bot":false,"first_name":"x"},"chat":{"id":4242,"first_name":"x","type":"private"},"date":1,"text":"/status"}},
                  {"update_id":11,"callback_query":{"id":"abc","from":{"id":4242,"is_bot":false,"first_name":"x"},"message":{"message_id":2,"chat":{"id":4242,"type":"private"},"date":1},"data":"top"}}]"#;
    let updates: Vec<Update> = serde_json::from_str(raw).unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].message.as_ref().unwrap().chat.kind, "private");
    assert_eq!(
        updates[1].callback_query.as_ref().unwrap().data.as_deref(),
        Some("top")
    );
}

/// 假的 Telegram 服务：记下收到的方法与内容，按方法回固定的 JSON。
async fn fake_telegram() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Json, Router};
    type Seen = Arc<Mutex<Vec<(String, serde_json::Value)>>>;
    async fn handle(
        State(seen): State<Seen>,
        Path((token, method)): Path<(String, String)>,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        seen.lock().unwrap().push((method.clone(), body.clone()));
        if token == "bad" {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"ok": false, "description": "Unauthorized"})),
            );
        }
        match method.as_str() {
            "getUpdates" if body["offset"] == 409 => (
                StatusCode::CONFLICT,
                Json(
                    serde_json::json!({"ok": false, "description": "Conflict: terminated by other getUpdates request"}),
                ),
            ),
            "getUpdates" => (
                StatusCode::OK,
                Json(serde_json::json!({"ok": true, "result": [
                    {"update_id": 5, "message": {"chat": {"id": 4242, "type": "private"}, "from": {"id": 4242}, "text": "/status"}}
                ]})),
            ),
            _ => (
                StatusCode::OK,
                Json(serde_json::json!({"ok": true, "result": true})),
            ),
        }
    }
    let seen: Seen = Arc::default();
    let app = Router::new()
        .route("/bot{token}/{method}", post(handle))
        .with_state(Arc::clone(&seen));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base, seen)
}

#[tokio::test]
async fn the_http_client_speaks_telegrams_shapes_handles_conflicts_and_never_leaks_the_token() {
    let (base, seen) = fake_telegram().await;
    let api = HttpApi {
        client: reqwest::Client::new(),
        base: format!("{base}/botSECRETTOKEN123"),
    };
    // 长轮询：只订阅消息与按钮，解析出更新。
    let updates = api.get_updates(10, 0).await.unwrap();
    assert_eq!(updates[0].update_id, 5);
    assert_eq!(
        updates[0].message.as_ref().unwrap().text.as_deref(),
        Some("/status")
    );
    // 另一个进程也在轮询：409 要认成冲突，而不是普通失败。
    assert!(matches!(
        api.get_updates(409, 0).await,
        Err(PollError::Conflict)
    ));
    // 发消息带内联键盘。
    api.send(4242, "你好".into(), vec![vec![button("📊 状态", "status")]])
        .await
        .unwrap();
    // 注册菜单：命令列表 + 让输入框旁的菜单按钮显示命令。
    api.set_commands(vec![("status".into(), "系统状态".into())])
        .await
        .unwrap();
    api.answer_callback("cb-1".into()).await.unwrap();
    {
        let seen = seen.lock().unwrap();
        let body_of = |method: &str| {
            seen.iter()
                .find(|(m, _)| m == method)
                .map(|(_, body)| body.clone())
                .unwrap_or_else(|| panic!("没有调用 {method}"))
        };
        assert_eq!(
            body_of("getUpdates")["allowed_updates"],
            serde_json::json!(["message", "callback_query"])
        );
        let sent = body_of("sendMessage");
        assert_eq!(sent["chat_id"], 4242);
        assert_eq!(
            sent["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            "status"
        );
        assert_eq!(body_of("setMyCommands")["commands"][0]["command"], "status");
        assert_eq!(
            body_of("setChatMenuButton")["menu_button"]["type"],
            "commands"
        );
        assert_eq!(body_of("answerCallbackQuery")["callback_query_id"], "cb-1");
    }
    // 错误文本里不能有 token：服务端拒绝、连不上，两种都查。
    let bad = HttpApi {
        client: reqwest::Client::new(),
        base: format!("{base}/botbad"),
    };
    let rejected = bad.send(1, "x".into(), Vec::new()).await.unwrap_err();
    assert!(
        rejected.contains("Unauthorized") && !rejected.contains("bad/"),
        "{rejected}"
    );
    let unreachable = HttpApi {
        client: reqwest::Client::new(),
        base: "http://127.0.0.1:1/botSECRETTOKEN123".into(),
    };
    let error = format!("{:?}", unreachable.get_updates(0, 0).await.unwrap_err());
    let error2 = unreachable
        .send(1, "x".into(), Vec::new())
        .await
        .unwrap_err();
    for text in [error, error2] {
        assert!(
            !text.contains("SECRETTOKEN123") && !text.contains("127.0.0.1"),
            "{text}"
        );
    }
}

/// 最后一条回复里「确认」按钮的票据。
fn confirm_ticket(api: &MockApi) -> String {
    let sent = api.sent.lock().unwrap();
    let (_, _, keyboard) = sent.last().expect("应该回了确认消息");
    keyboard
        .iter()
        .flatten()
        .find_map(|button| button.data.strip_prefix("ok:"))
        .expect("确认消息上要有「确认」按钮")
        .to_string()
}

fn last_text(api: &MockApi) -> String {
    api.sent.lock().unwrap().last().unwrap().1.clone()
}

async fn wait_for_text(api: &MockApi, needle: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !api
            .sent
            .lock()
            .unwrap()
            .iter()
            .any(|(_, text, _)| text.contains(needle))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("等不到「{needle}」"));
}

#[tokio::test]
async fn an_alert_button_takes_two_taps_and_each_confirmation_executes_once() {
    let (bot, api, backend) = setup(StubBackend::live());
    bot.handle(callback(ADMIN, ADMIN, "o:good-1")).await;
    assert_eq!(last_text(&api), "[confirm good-1]");
    assert_eq!(
        backend.executions.load(Ordering::SeqCst),
        0,
        "第一下绝不下单"
    );
    let ticket = confirm_ticket(&api);
    {
        let sent = api.sent.lock().unwrap();
        let rows = &sent.last().unwrap().2;
        assert_eq!(rows[0][0].label, "✅ 确认实盘开仓 TSLA 800U");
        assert_eq!(rows[1][0].data, format!("no:{ticket}"));
    }
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    assert!(last_text(&api).contains("正在下单：[summary good-1]"));
    wait_for_text(&api, "[executed [summary good-1]]").await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);
    // 同一个确认再点（双击、重发）：作废了，不再下。
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    assert!(last_text(&api).contains("过期或用过了"));
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);
    // 每次点都是新的票据，按钮上的转圈都停了。
    assert_eq!(api.answered.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn cancelled_unknown_and_refused_orders_never_execute() {
    let (bot, api, backend) = setup(StubBackend::live());
    bot.handle(callback(ADMIN, ADMIN, "o:good-2")).await;
    let ticket = confirm_ticket(&api);
    bot.handle(callback(ADMIN, ADMIN, &format!("no:{ticket}")))
        .await;
    assert_eq!(last_text(&api), "已取消，没下单。");
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    assert!(last_text(&api).contains("过期或用过了"));
    bot.handle(callback(ADMIN, ADMIN, "ok:000000000000")).await;
    assert!(last_text(&api).contains("过期或用过了"));
    // 预览不过：说原因，只给菜单，不给「确认」。
    bot.handle(callback(ADMIN, ADMIN, "o:bad")).await;
    assert_eq!(last_text(&api), "没法下单：这一刻不满足开仓硬条件");
    let sent = api.sent.lock().unwrap();
    assert!(
        sent.last()
            .unwrap()
            .2
            .iter()
            .flatten()
            .all(|button| !button.data.starts_with("ok:"))
    );
    assert_eq!(backend.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn order_buttons_from_anyone_but_the_admins_private_chat_are_ignored() {
    let (bot, api, backend) = setup(StubBackend::live());
    bot.handle(callback(ADMIN, 999, "o:good-3")).await;
    bot.handle(callback(-100_123, ADMIN, "o:good-3")).await;
    assert!(api.sent.lock().unwrap().is_empty());
    assert_eq!(backend.previews.load(Ordering::SeqCst), 0);
    // 别人拿到了确认票据也没用。
    bot.handle(callback(ADMIN, ADMIN, "o:good-3")).await;
    let ticket = confirm_ticket(&api);
    bot.handle(callback(ADMIN, 999, &format!("ok:{ticket}")))
        .await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 0);
    // 票据还在，管理员自己点照常有效。
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    wait_for_text(&api, "[executed").await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_executing_order_blocks_a_second_one_but_not_pause() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let (bot, api, backend) = setup(StubBackend {
        order_gate: Some(Arc::clone(&gate)),
        ..StubBackend::live()
    });
    bot.handle(callback(ADMIN, ADMIN, "o:good-a")).await;
    let first = confirm_ticket(&api);
    bot.handle(callback(ADMIN, ADMIN, "o:good-b")).await;
    let second = confirm_ticket(&api);
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{first}")))
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.executions.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // 下单卡在交易所那边时：第二单不下，/pause 照样立刻生效。
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{second}")))
        .await;
    assert!(last_text(&api).contains("已有一单正在执行"));
    bot.handle(message(ADMIN, "private", Some(ADMIN), "/pause"))
        .await;
    assert!(backend.paused.load(Ordering::SeqCst));
    assert!(last_text(&api).contains("已暂停开新仓"));
    gate.notify_one();
    wait_for_text(&api, "[executed [summary good-a]]").await;
    assert_eq!(backend.executions.load(Ordering::SeqCst), 1);
    // 结果消息带的菜单反映暂停后的状态。
    let sent = api.sent.lock().unwrap();
    let (_, _, keyboard) = sent.last().unwrap();
    assert!(
        keyboard
            .iter()
            .flatten()
            .any(|button| button.data == "resume")
    );
}

#[tokio::test]
async fn a_panicking_order_does_not_leave_the_next_one_blocked() {
    let (bot, api, backend) = setup(StubBackend {
        panic_once: AtomicBool::new(true),
        ..StubBackend::live()
    });
    bot.handle(callback(ADMIN, ADMIN, "o:good-p")).await;
    let ticket = confirm_ticket(&api);
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.executions.load(Ordering::SeqCst) < 1 || bot.ordering.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("panic 之后标志要复位");
    bot.handle(callback(ADMIN, ADMIN, "o:good-q")).await;
    let ticket = confirm_ticket(&api);
    bot.handle(callback(ADMIN, ADMIN, &format!("ok:{ticket}")))
        .await;
    assert!(last_text(&api).contains("正在下单：[summary good-q]"));
    wait_for_text(&api, "[executed [summary good-q]]").await;
}

#[test]
fn net_profit_line_is_the_order_book_figure_and_never_falls_back_to_mark_price() {
    let shown = book_net_line(Some(rust_decimal::Decimal::new(-48, 2)));
    assert!(shown.contains("净收益（按盘口现在平仓"), "{shown}");
    assert!(shown.contains("0.48"), "{shown}");
    let missing = book_net_line(None);
    assert!(missing.contains("未核对"), "{missing}");
}
