//! 离线端到端回归：不加载 .env，live=null；盘口为本地桩，只有 PaperBroker。
//! 测试文件全部在仓库 target/ 内，不能访问生产台账/自动交易设置。

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use arb_core::{ArbResult, Level, MarketSnapshot, OrderBook, Symbol};
use arb_venues::VenueApi;
use rust_decimal_macros::dec;

use super::*;
use crate::rh_spread::{Config, LocalBook, Normal, Session, evaluate, pairs};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "anthropic-paper-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Book {
    venue: Venue,
    bid: Decimal,
    ask: Decimal,
    depth_calls: AtomicUsize,
}

#[async_trait::async_trait]
impl VenueApi for Book {
    fn venue(&self) -> Venue {
        self.venue
    }

    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        Ok(vec![MarketSnapshot {
            venue: self.venue,
            symbol: Symbol::perp("SNDK", "USDT"),
            period_rate: Decimal::ZERO,
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: true,
            taker_fee: Some(Decimal::ZERO),
            mark_price: Some((self.bid + self.ask) / Decimal::TWO),
            index_price: Some(dec!(2000)),
            best_bid: Some(self.bid),
            best_ask: Some(self.ask),
            bid_size_usdt: Some(dec!(100000)),
            ask_size_usdt: Some(dec!(100000)),
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: Some(dec!(6)),
            maintenance_margin: Some(dec!(0.01)),
            oi_capped: false,
        }])
    }

    async fn fetch_depth(&self, symbol: &Symbol, _: u32) -> ArbResult<OrderBook> {
        assert_eq!(symbol, &Symbol::perp("SNDK", "USDT"));
        self.depth_calls.fetch_add(1, Ordering::SeqCst);
        let level = |price| Level {
            price,
            notional_usdt: dec!(100000),
        };
        Ok(OrderBook {
            venue: self.venue,
            symbol: symbol.clone(),
            bids: vec![level(self.bid)],
            asks: vec![level(self.ask)],
        })
    }
}

struct SilentSink;

impl crate::alert::Sink for SilentSink {
    fn send(
        &self,
        _: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        Box::pin(async { Ok(()) })
    }
}

async fn sandbox(negative_entry: bool, enabled: bool) -> (TestDir, Arc<AppState>, Vec<Arc<Book>>) {
    let dir = TestDir::new();
    let books: Vec<Arc<Book>> = [
        (
            Venue::HyperliquidIo,
            if negative_entry {
                dec!(1957)
            } else {
                dec!(2002)
            },
        ),
        (Venue::LighterRh, dec!(2000)),
    ]
    .into_iter()
    .map(|(venue, bid)| {
        Arc::new(Book {
            venue,
            bid,
            ask: bid + dec!(0.2),
            depth_calls: AtomicUsize::new(0),
        })
    })
    .collect();
    let apis: Arc<Vec<Arc<dyn VenueApi>>> = Arc::new(
        books
            .iter()
            .map(|b| b.clone() as Arc<dyn VenueApi>)
            .collect(),
    );
    let settings = arb_core::Settings {
        fee_per_side: Decimal::ZERO,
        amortize_days: dec!(7),
        venues: vec![Venue::HyperliquidIo, Venue::LighterRh],
        http_timeout_sec: 1,
        http_port: 0,
        scan_interval_sec: 60,
        min_venues: 2,
        max_position_usdt: Some(1000.0),
        max_daily_loss_usdt: 100.0,
        max_open_positions: Some(5),
        spread_hold_days: dec!(3),
        max_entry_basis_pct: None,
        leverage: dec!(3),
        log_filter: "off".into(),
    };
    let cache = Arc::new(crate::cache::ScanCache::default());
    cache.put(arb_scanner::scan(&apis, &settings).await).await;
    let alerts = crate::alert::Alerter::with_sink(Some(Arc::new(SilentSink)));
    let config = Config {
        enabled: true,
        size_usdt: dec!(2000),
        alert_net_pct: dec!(0.05),
        equities_only: false,
        dir: dir.0.join("spread"),
        pairs: vec![pairs::Pair {
            a: Venue::HyperliquidIo,
            b: Venue::LighterRh,
        }],
        watch_pairs: Vec::new(),
        entropy_rebate: dec!(2),
        hl_shadow: crate::rh_spread::hl_shadow::Config::default(),
        maker_shadow: true,
    };
    let monitor = crate::rh_spread::Monitor::new(config.clone(), alerts.clone());
    let mut line = if negative_entry {
        // 用户原提醒：入场 -2.138%，正常 -2.232%，回到正常 +0.064%。
        let mut line = tests::line(
            "SNDK",
            "long_b",
            tests::leg("-2.138", "-2.168", "0.064"),
            -2.232,
        );
        line.a = Venue::HyperliquidIo;
        line.basis_pct = Some(dec!(-2.113));
        line.fee_round_trip_pct = Some(Decimal::ZERO);
        line.pair = config.pairs[0].id();
        line.long_b.as_mut().unwrap().quote.exit_cross_pct = dec!(0.030);
        line
    } else {
        // 对照场景不是篡改原信号：价格倒转、正入场价差，按真实评估函数算出 +0.064%。
        let now = Instant::now();
        let local = |b: &Book| {
            let mut out = LocalBook::new(now);
            out.bids.insert(b.bid, dec!(100));
            out.asks.insert(b.ask, dec!(100));
            out
        };
        evaluate(
            config.pairs[0],
            &pairs::PairMarket {
                base: "SNDK".into(),
                category: "EQUITIES".into(),
                outside_rth: Some(true),
                a: pairs::VenueMarket {
                    key: "io:ANTH".into(),
                    taker_fee: Some(Decimal::ZERO),
                    maker_fee: None,
                    category: None,
                    outside_rth: None,
                    bounds: None,
                    size_multiplier: None,
                },
                b: pairs::VenueMarket {
                    key: "38".into(),
                    taker_fee: Some(Decimal::ZERO),
                    maker_fee: None,
                    category: None,
                    outside_rth: None,
                    bounds: None,
                    size_multiplier: None,
                },
            },
            Some(&local(&books[0])),
            Some(&local(&books[1])),
            Some(Normal {
                session: Session::Off,
                median: 0.01596,
                p10: 0.0,
                p90: 0.03,
                mad: 0.01,
                minutes: 200,
            }),
            200,
            Session::Off,
            &config,
            now,
        )
    };
    line.session = Session::Off;
    line.normal.as_mut().unwrap().session = Session::Off;
    let best = line.best.as_mut().unwrap();
    if negative_entry {
        best.signal = true;
        best.net_usdt = Some(dec!(1.29)); // 原通知的金额（百分比已四舍五入）。
    } else {
        assert!(best.signal, "正入场对照使用 evaluate 的真实结果");
    }
    best.signal_sec = 10;
    let text =
        crate::rh_spread::alert_text(&line, line.best.as_ref().unwrap(), &config, None).unwrap();
    if negative_entry {
        assert!(text.contains("仅偏离提醒，不满足现有自动开仓规则"));
        assert!(text.contains("-2.138% ≤ 0"));
        assert!(text.contains("多 Lighter RH / 空 HL-io"));
    } else {
        assert!(text.contains("是否自动下单见面板"));
        assert!(!text.contains("不满足现有自动开仓规则"));
    }
    let mut view = tests::view(vec![line]);
    view.connected.venues = [(Venue::HyperliquidIo, true), (Venue::LighterRh, true)].into();
    monitor.set_test_view(view).await;
    let auto = AutoTrader::in_memory(Settings {
        enabled,
        mode: Mode::Paper,
        size_usdt: dec!(800),
        hold_sec: 5,
        hold_ms: None,
        open_gap_sec: 60,
        take_profit_usdt: Some(dec!(0.5)),
        max_positions: 5,
        daily_max_opens: 20,
        ..tests::settings()
    });
    let trade = crate::trade::Trade::paper_only_for_test(
        dir.0.join("paper.jsonl").to_str().unwrap().to_string(),
        &apis,
    );
    assert!(trade.live_opt().is_none() && trade.live_venues().is_none());
    let precheck = crate::precheck::Prechecker::new(&apis, dir.0.join("precheck.json"), vec![]);
    let state = Arc::new(AppState {
        settings,
        cache,
        apis,
        trade,
        precheck,
        alerts,
        rh_spread: monitor,
        rh_auto: auto,
    });
    (dir, state, books)
}

#[tokio::test]
async fn reported_anthropic_alert_is_blocked_before_ordering_and_preview_also_rejects_it() {
    let (dir, state, books) = sandbox(true, true).await;
    for _ in 0..3 {
        state.rh_auto.step(&state).await;
    }
    let status = state.rh_auto.snapshot(Some(&[])).await;
    let why = status["status"].as_str().unwrap();
    assert!(why.contains("SNDK") && why.contains("-2.138% ≤ 0"), "{why}");
    assert_eq!(status["opened_today"], 0);
    assert!(
        !dir.0.join("paper.jsonl").exists(),
        "筛选时已拦，不必读取/创建台账"
    );
    assert!(
        books
            .iter()
            .all(|b| b.depth_calls.load(Ordering::SeqCst) == 0)
    );
    let rules = arb_exec::TaskRules {
        basis_exit_pct: Some(dec!(-2.232)),
        ..Default::default()
    };
    let body = crate::trade::auto_open_body(
        Mode::Paper,
        "SNDK/USDT",
        Venue::LighterRh,
        Venue::HyperliquidIo,
        dec!(800),
        dec!(3),
        arb_exec::MarginMode::Isolated,
        &rules,
    );
    // 即使绕过候选筛选，正式预览路径仍会拒绝；只预览纸面，不执行。
    let (code, preview) = crate::trade::open_for_auto(state, body, false).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{preview}");
    assert!(
        preview["error"]
            .as_str()
            .unwrap()
            .contains("可成交价差不为正"),
        "{preview}"
    );
    println!(
        "原提醒 dry-run：{why}；纸面预览 HTTP {}，{}",
        code.as_u16(),
        preview["error"]
    );
}

#[tokio::test]
async fn positive_after_hours_anthropic_signal_opens_on_paper_despite_alert_deduplication() {
    let (dir, state, _) = sandbox(false, true).await;
    let view = state.rh_spread.view().await;
    let line = &view.lines[0];
    assert_eq!(
        line.long_b.as_ref().unwrap().net_to_normal_pct,
        Some(dec!(0.064))
    );
    let key = "spread:hyperliquid-io:lighter-rh:SNDK:long_b";
    assert!(state.alerts.notify(key, "仅在本地内存 sink 测试"));
    assert!(!state.alerts.notify(key, "30 分钟提醒去重"));
    state.rh_auto.step(&state).await;
    assert_eq!(
        state.rh_auto.snapshot(Some(&[])).await["opened_today"],
        0,
        "仍要等连续保持"
    );
    let candidate = candidate_of(line, &state.rh_auto.settings().await).unwrap();
    // 使用注入的时间而非睡眠：连续成立超过 hold_sec，仍经过真正的 step/attempt/open_for_auto。
    state.rh_auto.inner.lock().await.since.insert(
        candidate.hold_key(),
        Instant::now() - Duration::from_secs(6),
    );
    state.rh_auto.step(&state).await;
    assert!(state.trade.live_opt().is_none(), "live 始终为 null");
    let (replayed, _) = arb_exec::Ledger::open(dir.0.join("paper.jsonl"))
        .await
        .unwrap()
        .replay()
        .await
        .unwrap();
    let exposed = replayed.exposed();
    assert_eq!(exposed.len(), 1, "必须真的得到纸面仓位，不只是组装请求");
    let position = exposed[0];
    assert!(position.id.starts_with("paper-"));
    assert_eq!(position.symbol, Symbol::perp("SNDK", "USDT"));
    assert_eq!(
        (
            position.long.as_ref().unwrap().venue,
            position.short.as_ref().unwrap().venue
        ),
        (Venue::LighterRh, Venue::HyperliquidIo)
    );
    assert_eq!(position.rules.basis_exit_pct, Some(dec!(0.016)));
    let status = state.rh_auto.snapshot(Some(&[])).await;
    assert_eq!(status["opened_today"], 1);
    // 越过开仓间隔、清掉测试里的尝试冷却，仍应被真实纸面台账的同合约持仓挡下。
    {
        let mut inner = state.rh_auto.inner.lock().await;
        inner.last_open = Some(Instant::now() - MIN_OPEN_GAP - Duration::from_secs(1));
        inner.cooldown.clear();
    }
    state.rh_auto.step(&state).await;
    let again = state.rh_auto.snapshot(Some(&[])).await;
    assert_eq!(again["opened_today"], 1);
    assert!(
        again["status"]
            .as_str()
            .unwrap()
            .contains("已有这个合约的仓位"),
        "{again}"
    );
    println!(
        "对照 dry-run：live=null，盘后 +0.064% 正入场信号 → {}；提醒去重不拦纸面开仓",
        position.id
    );
}

#[tokio::test]
async fn telegram_order_button_opens_then_adds_on_paper_through_the_order_path() {
    use crate::telegram::Backend;
    use crate::telegram::order::{Action, Intent};
    // 自动交易关着也能手动下；单腿 400，补一次后 800 不超过单笔上限 1000。
    let (dir, state, books) = sandbox(false, false).await;
    state
        .rh_auto
        .inner
        .lock()
        .await
        .persisted
        .settings
        .size_usdt = dec!(400);
    let backend = crate::telegram::AppBackend {
        state: Arc::clone(&state),
    };
    let ticket = state.rh_spread.issue_order_ticket(Intent {
        pair: "hyperliquid-io:lighter-rh".into(),
        base: "SNDK".into(),
        direction: "long_b",
    });
    let expired = backend
        .order_preview("000000000000".into())
        .await
        .unwrap_err();
    assert!(expired.contains("已经过期"), "{expired}");
    let plan = backend.order_preview(ticket.clone()).await.unwrap();
    assert!(matches!(plan.action, Action::Open { .. }));
    assert!(plan.prompt.contains("确认纸面开仓"), "{}", plan.prompt);
    assert!(
        !dir.0.join("paper.jsonl").exists()
            && books
                .iter()
                .all(|b| b.depth_calls.load(Ordering::SeqCst) == 0),
        "第一下只预览：不碰台账、不拉盘口"
    );
    let result = backend.order_execute(plan).await;
    assert!(result.contains("纸面已开仓 paper-"), "{result}");
    let replay = || async {
        arb_exec::Ledger::open(dir.0.join("paper.jsonl"))
            .await
            .unwrap()
            .replay()
            .await
            .unwrap()
            .0
    };
    let replayed = replay().await;
    let exposed = replayed.exposed();
    assert_eq!(exposed.len(), 1, "真的开出了纸面仓位");
    let position = exposed[0].clone();
    assert_eq!(
        (
            position.long.as_ref().unwrap().venue,
            position.short.as_ref().unwrap().venue
        ),
        (Venue::LighterRh, Venue::HyperliquidIo)
    );
    // 和自动交易同一套规则：回到正常基差的目标、止盈。
    assert_eq!(position.rules.basis_exit_pct, Some(dec!(0.016)));
    assert_eq!(position.rules.take_profit_usdt, Some(dec!(0.5)));
    assert_eq!(
        state.rh_auto.snapshot(Some(&[])).await["opened_today"],
        0,
        "手动单不算进自动交易的笔数"
    );
    // 同一条提醒再点：已有同方向仓位，变成补仓。
    let plan = backend.order_preview(ticket).await.unwrap();
    assert!(
        matches!(&plan.action, Action::Add(target) if target.id == position.id),
        "{:?}",
        plan.action
    );
    assert!(plan.prompt.contains("确认纸面补仓"), "{}", plan.prompt);
    let result = backend.order_execute(plan.clone()).await;
    assert!(
        result.contains(&format!("已补仓到 {}", position.id)),
        "{result}"
    );
    let replayed = replay().await;
    let added = replayed.exposed()[0].clone();
    assert_eq!(added.id, position.id);
    assert_eq!(added.long.as_ref().unwrap().notional_usdt, dec!(800));
    // 拿旧的补仓确认再执行一次：复核时补完会超过单笔上限，不下。
    let refused = backend.order_execute(plan).await;
    assert!(
        refused.contains("没下单") && refused.contains("超过上限 1000 USDT"),
        "{refused}"
    );
}

#[tokio::test]
async fn disabled_auto_never_opens_even_a_positive_paper_signal() {
    let (dir, state, books) = sandbox(false, false).await;
    state.rh_auto.step(&state).await;
    assert_eq!(state.rh_auto.snapshot(Some(&[])).await["status"], "未开启");
    assert!(!dir.0.join("paper.jsonl").exists());
    assert!(
        books
            .iter()
            .all(|b| b.depth_calls.load(Ordering::SeqCst) == 0)
    );
}

#[test]
fn hard_entry_boundaries_and_diagnostics_do_not_confuse_alert_profit_with_order_eligibility() {
    let settings = tests::settings();
    for (entry, zero, expected) in [
        ("-2.138", "-2.168", Some("开仓可成交价差 -2.138% ≤ 0")),
        ("0", "0.064", Some("开仓可成交价差 0% ≤ 0")),
        ("0.09", "0", Some("收敛到 0 净收益 0% ≤ 0")),
        ("0.09", "-0.001", Some("收敛到 0 净收益 -0.001% ≤ 0")),
        ("0.09", "0.064", None),
    ] {
        let line = tests::line("SNDK", "long_b", tests::leg(entry, zero, "0.064"), -2.232);
        let view = tests::view(vec![line.clone()]);
        assert_eq!(candidate_of(&line, &settings).is_none(), expected.is_some());
        match expected {
            Some(reason) => {
                let why =
                    pick(&view, &settings, Utc::now(), &|_| true, &|_| 10, &|_| None).unwrap_err();
                assert!(why.contains(reason), "{why}");
            }
            None => assert!(blocked_signal_reason(&view, &settings).is_none()),
        }
    }
    let blocked = tests::view(vec![tests::line(
        "SNDK",
        "long_b",
        tests::leg("-2.138", "-2.168", "0.064"),
        -2.232,
    )]);
    let higher_threshold = Settings {
        min_net_pct: dec!(0.06401),
        ..settings.clone()
    };
    assert!(blocked_signal_reason(&blocked, &higher_threshold).is_none());
    let positive = tests::line("SNDK", "long_b", tests::leg("0.09", "0.08", "0.064"), 0.016);
    let equal_threshold = Settings {
        min_net_pct: dec!(0.064),
        ..settings.clone()
    };
    assert!(
        candidate_of(&positive, &equal_threshold).is_some(),
        "百分比等于门槛也合格"
    );
    assert!(candidate_of(&positive, &higher_threshold).is_none());
    let mixed = tests::view(vec![Line::clone(&blocked.lines[0]), positive]);
    assert!(
        pick(&mixed, &settings, Utc::now(), &|_| true, &|_| 10, &|_| None).is_ok(),
        "偏离提醒的诊断不能挡住其它合格信号"
    );
    let whitelist = Settings {
        symbols: vec!["OPENAI".into()],
        ..settings
    };
    assert!(
        blocked_signal_reason(&blocked, &whitelist).is_none(),
        "不显示不在名单内的合约"
    );
}

/// 端到端：同方向的新信号比已有纸面仓位的实际价差好出门槛 → 自动往那笔上补仓（真的走
/// step → attempt_add → add_for_auto → 执行器），而不是被「已有这个合约的仓位」挡掉。
#[tokio::test]
async fn a_better_signal_on_an_existing_paper_position_is_added_to_it() {
    let (dir, state, _) = sandbox(false, true).await;
    let view = state.rh_spread.view().await;
    let candidate = candidate_of(&view.lines[0], &state.rh_auto.settings().await).unwrap();
    state.rh_auto.inner.lock().await.since.insert(
        candidate.hold_key(),
        Instant::now() - Duration::from_secs(6),
    );
    state.rh_auto.step(&state).await;
    let ledger = dir.0.join("paper.jsonl");
    let (replayed, _) = arb_exec::replay_file(&ledger).await.unwrap();
    let opened = replayed.exposed()[0].clone();
    let before = opened.long.as_ref().unwrap().notional_usdt;

    // 新信号的可成交价差比开仓时好 0.2%；开补仓（单笔上限 1000：补 100，总名义 900 以内）。
    let mut better = view.clone();
    Arc::make_mut(&mut better.lines[0])
        .long_b
        .as_mut()
        .unwrap()
        .quote
        .entry_pct += dec!(0.2);
    better.updated_at = Some(Utc::now());
    state.rh_spread.set_test_view(better).await;
    {
        let mut inner = state.rh_auto.inner.lock().await;
        let settings = &mut inner.persisted.settings;
        settings.size_usdt = dec!(100);
        settings.add_min_improve_pct = Some(dec!(0.05));
        settings.add_max_multiple = dec!(10);
        settings.history_gate_pct = Some(dec!(20));
        // 别的组合通过，不能给这个补仓放行。
        inner.history.insert(
            candidate.hold_key(),
            (Instant::now(), Some("本组合历史分位不合格".into())),
        );
        inner.history.insert(
            (candidate.base.clone(), candidate.long, Venue::Bitget),
            (Instant::now(), None),
        );
        inner.last_open = Some(Instant::now() - MIN_OPEN_GAP - Duration::from_secs(1));
        inner.cooldown.clear();
        inner.since.insert(
            candidate.hold_key(),
            Instant::now() - Duration::from_secs(6),
        );
    }
    state.rh_auto.step(&state).await;
    let (blocked, _) = arb_exec::replay_file(&ledger).await.unwrap();
    assert_eq!(
        blocked.exposed()[0].long.as_ref().unwrap().notional_usdt,
        before,
        "补仓也不能绕过本组合历史门槛"
    );
    {
        let mut inner = state.rh_auto.inner.lock().await;
        inner
            .history
            .insert(candidate.hold_key(), (Instant::now(), None));
        inner.history.insert(
            (candidate.base.clone(), candidate.long, Venue::Bitget),
            (Instant::now(), Some("不支持历史价差图".into())),
        );
    }
    state.rh_auto.step(&state).await;

    let (replayed, _) = arb_exec::replay_file(&ledger).await.unwrap();
    let exposed = replayed.exposed();
    assert_eq!(exposed.len(), 1, "补在原来那笔上，不新开");
    let added = exposed[0];
    assert_eq!(added.id, opened.id);
    let after = added.long.as_ref().unwrap().notional_usdt;
    assert!(after > before + dec!(99), "{before} → {after}");
    assert!(
        replayed
            .orders
            .keys()
            .any(|id| id.starts_with(&opened.id) && id.ends_with("-add1"))
    );
    let status = state.rh_auto.snapshot(Some(&[])).await;
    assert_eq!(status["opened_today"], 2, "补仓算进每日笔数");
    assert!(
        status["status"].as_str().unwrap().contains("已补仓"),
        "{status}"
    );
    // 补仓不占同时持仓名额。
    let inner = state.rh_auto.inner.lock().await;
    assert_eq!(inner.persisted.opened.iter().filter(|o| o.add).count(), 1);
}

/// 「同时最多 / 每日最多」填 0 = 不限：今天已自动开过 30 笔，仍会照常开（纸面，live=null）；
/// 填了数时照旧拦。同一合约已有仓位、开仓间隔、下单路径的闸门不受影响。
#[tokio::test]
async fn zero_daily_cap_means_unlimited_and_a_set_cap_still_blocks() {
    for (daily, expect_open) in [(0, true), (20, false)] {
        let (dir, state, _) = sandbox(false, true).await;
        let settings = Settings {
            max_positions: 0,
            daily_max_opens: daily,
            ..state.rh_auto.settings().await
        };
        state.rh_auto.update(settings, None, false).await.unwrap();
        let view = state.rh_spread.view().await;
        let candidate = candidate_of(&view.lines[0], &state.rh_auto.settings().await).unwrap();
        {
            let mut inner = state.rh_auto.inner.lock().await;
            for i in 0..30 {
                inner.persisted.opened.push(Opened {
                    id: format!("paper-old-{i}"),
                    symbol: "OLD/USDT".into(),
                    mode: Mode::Paper,
                    at: Utc::now(),
                    add: false,
                    binance: false,
                    size_usdt: Decimal::ZERO,
                });
            }
            inner.since.insert(
                candidate.hold_key(),
                Instant::now() - Duration::from_secs(6),
            );
        }
        state.rh_auto.step(&state).await;
        let status = state.rh_auto.snapshot(Some(&[])).await;
        assert_eq!(
            dir.0.join("paper.jsonl").exists(),
            expect_open,
            "daily {daily}: {status}"
        );
        if !expect_open {
            assert!(
                status["status"].as_str().unwrap().contains("到上限"),
                "{status}"
            );
        }
    }
}

/// 「同时最多」0 = 不限，填 1 时第二笔被拦（第一笔是真实的纸面持仓，不是伪造的记录）。
#[tokio::test]
async fn zero_position_cap_means_unlimited_and_one_blocks_a_second_open() {
    for (cap, expect_second) in [(0, true), (1, false)] {
        let (_dir, state, _) = sandbox(false, true).await;
        let settings = Settings {
            max_positions: cap,
            daily_max_opens: 0,
            ..state.rh_auto.settings().await
        };
        state.rh_auto.update(settings, None, false).await.unwrap();
        let view = state.rh_spread.view().await;
        let candidate = candidate_of(&view.lines[0], &state.rh_auto.settings().await).unwrap();
        state.rh_auto.inner.lock().await.since.insert(
            candidate.hold_key(),
            Instant::now() - Duration::from_secs(6),
        );
        state.rh_auto.step(&state).await;
        let exposed = state.trade.exposed_positions(false).await.unwrap();
        assert_eq!(exposed.len(), 1, "第一笔照常开");
        // 第一笔记成别的合约，好让同一个 某合约 信号不被「已有这个合约的仓位」挡住，只剩名额这一道。
        let first = exposed[0].0.clone();
        {
            let mut inner = state.rh_auto.inner.lock().await;
            assert!(inner.persisted.opened.iter().any(|o| o.id == first));
            inner.last_open = Some(Instant::now() - MIN_OPEN_GAP - Duration::from_secs(1));
            inner.cooldown.clear();
        }
        let open_auto = state
            .rh_auto
            .snapshot(Some(
                &exposed
                    .iter()
                    .map(|(id, s)| (id.clone(), s.to_string()))
                    .collect::<Vec<_>>(),
            ))
            .await;
        assert_eq!(open_auto["open_positions"].as_array().unwrap().len(), 1);
        state.rh_auto.step(&state).await;
        let why = state.rh_auto.snapshot(Some(&[])).await["status"]
            .as_str()
            .unwrap()
            .to_string();
        if expect_second {
            // 不限：不会因为名额停下；这里被同合约已有仓位挡住，说明已经越过名额检查。
            assert!(why.contains("已有这个合约的仓位"), "{why}");
        } else {
            assert!(why.contains("自动仓位已到上限（1/1）"), "{why}");
        }
    }
}

/// 「相对正常基差」开着：同一条 −2.138% 的 某合约 信号（原口径一定被拒）走完整的
/// step → attempt → open_for_auto → run_open，真的开出纸面仓位，并带上回到正常的目标和止损。
/// 关着时照旧不下单。live=null，盘口是本地桩。
#[tokio::test]
async fn relative_to_normal_opens_the_negative_entry_signal_on_paper_with_target_and_stop() {
    // 先确认：设置不全时根本存不进去。
    let base = Settings {
        enabled: true,
        mode: Mode::Paper,
        size_usdt: dec!(800),
        hold_sec: 5,
        hold_ms: None,
        open_gap_sec: 60,
        max_positions: 0,
        daily_max_opens: 0,
        ..tests::settings()
    };
    let missing_stop = Settings {
        relative_to_normal: true,
        ..base.clone()
    };
    assert!(missing_stop.validate(None).unwrap_err().contains("止损"));
    let missing_back = Settings {
        relative_to_normal: true,
        back_to_normal: false,
        take_profit_pct: Some(dec!(0.05)),
        stop_loss_usdt: Some(dec!(4)),
        ..base.clone()
    };
    assert!(
        missing_back
            .validate(None)
            .unwrap_err()
            .contains("回到同时段正常基差")
    );

    for relative in [false, true] {
        let (dir, state, _) = sandbox(true, true).await;
        let settings = Settings {
            relative_to_normal: relative,
            stop_loss_usdt: Some(dec!(4)),
            ..base.clone()
        };
        state
            .rh_auto
            .update(settings.clone(), None, false)
            .await
            .unwrap();
        let view = state.rh_spread.view().await;
        let line = &view.lines[0];
        match candidate_of(line, &settings) {
            None => assert!(!relative, "开了相对模式就应当成为候选"),
            Some(candidate) => {
                assert!(relative, "关着时 −2.138% 不能成为候选");
                assert!(candidate.entry_pct < Decimal::ZERO);
                state.rh_auto.inner.lock().await.since.insert(
                    candidate.hold_key(),
                    Instant::now() - Duration::from_secs(6),
                );
            }
        }
        state.rh_auto.step(&state).await;
        assert!(state.trade.live_opt().is_none(), "live 始终为 null");
        let exposed = if dir.0.join("paper.jsonl").exists() {
            let (replayed, _) = arb_exec::Ledger::open(dir.0.join("paper.jsonl"))
                .await
                .unwrap()
                .replay()
                .await
                .unwrap();
            replayed.exposed().into_iter().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let status = state.rh_auto.snapshot(Some(&[])).await;
        if !relative {
            assert!(exposed.is_empty(), "{status}");
            continue;
        }
        assert_eq!(exposed.len(), 1, "{status}");
        let p = &exposed[0];
        assert_eq!(p.symbol, Symbol::perp("SNDK", "USDT"));
        assert_eq!(
            (
                p.long.as_ref().unwrap().venue,
                p.short.as_ref().unwrap().venue
            ),
            (Venue::LighterRh, Venue::HyperliquidIo)
        );
        assert_eq!(p.rules.basis_exit_pct, Some(dec!(-2.232)));
        assert_eq!(p.rules.stop_loss_usdt, Some(dec!(4)));
        assert!(p.entry_basis_pct < Decimal::ZERO, "{}", p.entry_basis_pct);
        println!(
            "相对正常基差 dry-run：{} 入场基差 {}%，目标 {:?}%，止损 {:?} USDT",
            p.id,
            p.entry_basis_pct.round_dp(3),
            p.rules.basis_exit_pct,
            p.rules.stop_loss_usdt
        );
    }
}

/// 滑点缓冲、手续费占比、按比例止盈：纯函数口径。
#[test]
fn slippage_buffer_fee_share_and_percentage_take_profit() {
    let s = tests::settings();
    // 0.064% 回到正常：门槛 0.05 够；扣 0.02 缓冲后 0.044 不够。
    let line = tests::line("X", "long_a", tests::leg("0.3", "0.2", "0.064"), -0.1);
    assert!(candidate_of(&line, &s).is_some());
    let buffered = Settings {
        slippage_buffer_pct: dec!(0.02),
        ..s.clone()
    };
    assert!(candidate_of(&line, &buffered).is_none());
    let view = tests::view(vec![line.clone()]);
    assert!(
        blocked_signal_reason(&view, &buffered)
            .unwrap()
            .contains("扣滑点缓冲")
    );
    // 往返费 0.045%，毛利 0.064 + 0.045 = 0.109%：费占 41%。上限 50 放行、30 拦下。
    let fee_ok = Settings {
        max_fee_share_pct: Some(dec!(50)),
        ..s.clone()
    };
    assert!(candidate_of(&line, &fee_ok).is_some());
    let fee_tight = Settings {
        max_fee_share_pct: Some(dec!(30)),
        ..s.clone()
    };
    assert!(candidate_of(&line, &fee_tight).is_none());
    assert!(
        blocked_signal_reason(&view, &fee_tight)
            .unwrap()
            .contains("往返手续费")
    );
    // 止盈：0.05% × 800 = 0.4；与固定 0.05 都设取大的。
    let tp = Settings {
        size_usdt: dec!(800),
        take_profit_usdt: Some(dec!(0.05)),
        take_profit_pct: Some(dec!(0.05)),
        ..s.clone()
    };
    assert_eq!(tp.take_profit_amount(), Some(dec!(0.4)));
    assert_eq!(tp.rules(None).take_profit_usdt, Some(dec!(0.4)));
    let only_fixed = Settings {
        take_profit_usdt: Some(dec!(0.05)),
        ..s.clone()
    };
    assert_eq!(only_fixed.take_profit_amount(), Some(dec!(0.05)));
    assert!(
        Settings {
            back_to_normal: false,
            take_profit_pct: Some(dec!(0.05)),
            ..s.clone()
        }
        .validate(None)
        .is_ok(),
        "只有按比例止盈也算一条退出规则"
    );
    for bad in [
        Settings {
            slippage_buffer_pct: dec!(2),
            ..s.clone()
        },
        Settings {
            max_fee_share_pct: Some(dec!(0)),
            ..s.clone()
        },
        Settings {
            take_profit_pct: Some(dec!(20)),
            ..s.clone()
        },
        Settings {
            stop_loss_usdt: Some(dec!(0)),
            ..s.clone()
        },
    ] {
        assert!(bad.validate(None).is_err());
    }
}

/// 历史分位门槛（纸面，live=null，K 线是注入的假历史，不联网）：
/// 同一个 +0.064% 的正入场信号，历史上这个价差处在低位时不开；处在历史最高附近时开。
#[tokio::test]
async fn history_percentile_gate_blocks_mid_range_entries_and_allows_tops() {
    use crate::spread_chart::{Bar, chart_from, seed_cache};
    assert!(history_gate_passes(85.0, 20.0));
    assert!(!history_gate_passes(79.0, 20.0));
    for bad in [dec!(0), dec!(60)] {
        let s = Settings {
            history_gate_pct: Some(bad),
            ..tests::settings()
        };
        assert!(s.validate(None).unwrap_err().contains("历史分位"));
    }
    let s = Settings {
        history_gate_pct: Some(dec!(20)),
        history_interval: "2h".into(),
        ..tests::settings()
    };
    assert!(s.validate(None).unwrap_err().contains("周期"));
    let s = Settings {
        history_gate_pct: Some(dec!(20)),
        history_days: dec!(100),
        ..tests::settings()
    };
    assert!(s.validate(None).unwrap_err().contains("天数"));

    // 每小时一根，48 根，最后一根是「现在」。多 RH（a），空 HL-io（b）：价差 = io − rh。
    let now_h = chrono::Utc::now().timestamp_millis() / 3_600_000 * 3_600_000;
    let series = |last_spread: f64| -> (Vec<Bar>, Vec<Bar>) {
        let mut rh = Vec::new();
        let mut io = Vec::new();
        for i in 0..48 {
            let t = now_h - (47 - i) * 3_600_000;
            // 历史价差在 0.0% ~ 2.0% 之间来回；最后一根用给定值。
            let spread = if i == 47 {
                last_spread
            } else {
                (i % 10) as f64 * 0.2
            };
            let base = 2000.0;
            rh.push(Bar {
                t,
                o: base,
                h: base,
                l: base,
                c: base,
            });
            let io_px = base * (1.0 + spread / 100.0);
            io.push(Bar {
                t,
                o: io_px,
                h: io_px,
                l: io_px,
                c: io_px,
            });
        }
        (rh, io)
    };
    for (last, expect_open) in [(0.4, false), (2.2, true)] {
        let (dir, state, _) = sandbox(false, true).await;
        let (rh, io) = series(last);
        seed_cache(chart_from(
            "SNDK",
            Venue::LighterRh,
            Venue::HyperliquidIo,
            "1h",
            2.0,
            &rh,
            &io,
        ))
        .await;
        let settings = Settings {
            max_positions: 0,
            daily_max_opens: 0,
            history_gate_pct: Some(dec!(20)),
            history_interval: "1h".into(),
            history_days: dec!(2),
            ..state.rh_auto.settings().await
        };
        state
            .rh_auto
            .update(settings.clone(), None, false)
            .await
            .unwrap();
        let view = state.rh_spread.view().await;
        let candidate = candidate_of(&view.lines[0], &settings).unwrap();
        assert_eq!(
            (candidate.long, candidate.short),
            (Venue::LighterRh, Venue::HyperliquidIo)
        );
        state.rh_auto.inner.lock().await.since.insert(
            candidate.hold_key(),
            Instant::now() - Duration::from_secs(6),
        );
        state.rh_auto.step(&state).await;
        let status = state.rh_auto.snapshot(Some(&[])).await;
        let opened = dir.0.join("paper.jsonl").exists();
        assert_eq!(opened, expect_open, "last {last}: {status}");
        if !expect_open {
            assert!(
                status["status"].as_str().unwrap().contains("历史价差分位"),
                "{status}"
            );
        }
        assert!(state.trade.live_opt().is_none());
    }
}

/// 同币种的历史结论不能串到其它交易所组合或反向交易；未知、在途与过期都不能借用别组的通过。
#[tokio::test]
async fn history_gate_is_scoped_to_the_exact_pair_and_direction() {
    for (case, own, other, pending, expect_open) in [
        (
            "other pair rejected",
            Some((false, None)),
            Some("不支持历史价差图"),
            false,
            true,
        ),
        (
            "own pair rejected",
            Some((false, Some("本组合历史分位不合格"))),
            None,
            false,
            false,
        ),
        ("own pair missing", None, None, true, false),
        ("own pair expired", Some((true, None)), None, true, false),
    ] {
        let (dir, state, _) = sandbox(false, true).await;
        let settings = Settings {
            history_gate_pct: Some(dec!(20)),
            ..state.rh_auto.settings().await
        };
        state
            .rh_auto
            .update(settings.clone(), None, false)
            .await
            .unwrap();
        let view = state.rh_spread.view().await;
        let c = candidate_of(&view.lines[0], &settings).unwrap();
        let now = Instant::now();
        {
            let mut inner = state.rh_auto.inner.lock().await;
            inner
                .since
                .insert(c.hold_key(), now - Duration::from_secs(6));
            if let Some((expired, verdict)) = own {
                inner.history.insert(
                    c.hold_key(),
                    (
                        if expired {
                            now - HISTORY_RECHECK - Duration::from_secs(1)
                        } else {
                            now
                        },
                        verdict.map(str::to_string),
                    ),
                );
            }
            // 相同 base，不同空腿：失败不能误拦，成功也不能给当前候选放行。
            inner.history.insert(
                (c.base.clone(), c.long, Venue::Bitget),
                (now, other.map(str::to_string)),
            );
            // 反向交易同样必须独立检查。
            inner.history.insert(
                (c.base.clone(), c.short, c.long),
                (now, other.map(str::to_string)),
            );
            if pending {
                // 模拟后台查询在途，保持完全离线，不能因为别组通过就提前下单。
                inner.history_pending.insert(c.hold_key());
            }
        }
        state.rh_auto.step(&state).await;
        let status = state.rh_auto.snapshot(Some(&[])).await;
        assert_eq!(
            dir.0.join("paper.jsonl").exists(),
            expect_open,
            "{case}: {status}"
        );
        if !expect_open {
            assert!(
                status["status"].as_str().unwrap().contains("历史"),
                "{case}: {status}"
            );
        }
        assert!(state.trade.live_opt().is_none());
    }
}

/// 提前准备的时机：信号保持到离 `hold_sec` 还差 4 秒（至少 1 秒）时做一次；同一组合 10 秒内不重复；
/// 保持够了的那一刻不再「提前」。这里没有实盘，准备本身会直接失败（下单时照常现做）。
#[tokio::test]
async fn prearming_starts_once_inside_the_window_before_the_hold_completes() {
    let (_dir, state, _) = sandbox(false, true).await;
    let settings = Settings {
        hold_sec: 5,
        hold_ms: None,
        open_gap_sec: 60,
        ..state.rh_auto.settings().await
    };
    let view = state.rh_spread.view().await;
    let candidate = candidate_of(&view.lines[0], &settings).unwrap();
    let mut inner = state.rh_auto.inner.lock().await;
    for held in [0, 999, 5_000, 6_000] {
        AutoTrader::maybe_arm(&mut inner, &state, &settings, &candidate, held);
        assert!(inner.arming.is_none(), "held {held}：不在窗口内");
    }
    AutoTrader::maybe_arm(&mut inner, &state, &settings, &candidate, 1_000);
    let arming = inner.arming.take().expect("保持 1 秒：开始提前准备");
    assert!(inner.armed_at.contains_key(&candidate.hold_key()));
    arming.await.unwrap();
    AutoTrader::maybe_arm(&mut inner, &state, &settings, &candidate, 2_000);
    assert!(inner.arming.is_none(), "同一组合 10 秒内只做一次");

    // 保持门槛 20 秒：离门槛 4 秒（第 16 秒）才开始，免得做完之后放过期了。
    let long_hold = Settings {
        hold_sec: 20,
        hold_ms: None,
        open_gap_sec: 60,
        ..settings.clone()
    };
    inner.armed_at.clear();
    AutoTrader::maybe_arm(&mut inner, &state, &long_hold, &candidate, 15_999);
    assert!(inner.arming.is_none());
    AutoTrader::maybe_arm(&mut inner, &state, &long_hold, &candidate, 16_000);
    assert!(inner.arming.take().is_some());

    // 保持 0.5 秒：短于提前准备的最短保持（1 秒），不提前准备，下单时自己做。
    let short_hold = Settings {
        hold_ms: Some(500),
        ..settings
    };
    inner.armed_at.clear();
    for held in [0, 400, 500, 1_000] {
        AutoTrader::maybe_arm(&mut inner, &state, &short_hold, &candidate, held);
        assert!(
            inner.arming.is_none(),
            "held {held} ms：0.5 秒保持不提前准备"
        );
    }
}

/// 历史分位在信号刚保持满 1 秒时就在后台算：保持够了的那一刻不必再等 K 线。
#[tokio::test]
async fn the_history_verdict_is_computed_in_the_background_while_the_signal_holds() {
    use crate::spread_chart::{Bar, chart_from, seed_cache};
    let now_h = chrono::Utc::now().timestamp_millis() / 3_600_000 * 3_600_000;
    let bars = |spread: f64| -> Vec<Bar> {
        (0..48)
            .map(|i| {
                let c = 2000.0 * (1.0 + if i == 47 { spread } else { 0.0 } / 100.0);
                Bar {
                    t: now_h - (47 - i) * 3_600_000,
                    o: c,
                    h: c,
                    l: c,
                    c,
                }
            })
            .collect()
    };
    let (_dir, state, _) = sandbox(false, true).await;
    seed_cache(chart_from(
        "SNDK",
        Venue::LighterRh,
        Venue::HyperliquidIo,
        "1h",
        2.0,
        &bars(0.0),
        &bars(2.0),
    ))
    .await;
    let settings = Settings {
        max_positions: 0,
        daily_max_opens: 0,
        history_gate_pct: Some(dec!(20)),
        history_interval: "1h".into(),
        history_days: dec!(2),
        hold_sec: 5,
        hold_ms: None,
        open_gap_sec: 60,
        ..state.rh_auto.settings().await
    };
    state
        .rh_auto
        .update(settings.clone(), None, false)
        .await
        .unwrap();
    let view = state.rh_spread.view().await;
    let key = candidate_of(&view.lines[0], &settings).unwrap().hold_key();
    {
        let mut inner = state.rh_auto.inner.lock().await;
        inner
            .since
            .insert(key.clone(), Instant::now() - Duration::from_secs(2));
        inner.seen.insert(key.clone(), Instant::now());
    }
    state.rh_auto.step(&state).await;
    assert!(
        !std::path::Path::new(&_dir.0.join("paper.jsonl")).exists(),
        "还没保持够：不下单"
    );
    for _ in 0..200 {
        if state.rh_auto.inner.lock().await.history.contains_key(&key) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let inner = state.rh_auto.inner.lock().await;
    assert!(inner.history.contains_key(&key), "后台已经算出结论");
    assert!(!inner.history_pending.contains(&key));
}

/// 看板手动补仓：页面只给仓位 id 和名义，合约、方向、杠杆取台账里这笔仓位的；补在原来那笔上。
/// 名义不对、仓位不存在、实盘没带确认的都直接拒绝，台账不动。
#[tokio::test]
async fn a_manual_add_from_the_panel_goes_into_the_existing_position() {
    use crate::trade::{AddBody, run_manual_add};
    let (dir, state, _) = sandbox(false, true).await;
    let view = state.rh_spread.view().await;
    let candidate = candidate_of(&view.lines[0], &state.rh_auto.settings().await).unwrap();
    state.rh_auto.inner.lock().await.since.insert(
        candidate.hold_key(),
        Instant::now() - Duration::from_secs(6),
    );
    state.rh_auto.step(&state).await;
    let ledger = dir.0.join("paper.jsonl");
    let (replayed, _) = arb_exec::replay_file(&ledger).await.unwrap();
    let opened = replayed.exposed()[0].clone();
    let before = opened.long.as_ref().unwrap().notional_usdt;

    let status_of = |response: Response| response.status();
    for (body, expect) in [
        (
            AddBody::new(Mode::Paper, &opened.id, "0", None),
            StatusCode::BAD_REQUEST,
        ),
        (
            AddBody::new(Mode::Paper, &opened.id, "abc", None),
            StatusCode::BAD_REQUEST,
        ),
        (
            AddBody::new(Mode::Paper, "paper-nope", "100", None),
            StatusCode::NOT_FOUND,
        ),
        (
            AddBody::new(Mode::Live, &opened.id, "100", None),
            StatusCode::BAD_REQUEST,
        ),
        (
            AddBody::new(Mode::Live, &opened.id, "100", Some("SNDK")),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        assert_eq!(
            status_of(run_manual_add(&state, &body).await),
            expect,
            "{body:?}"
        );
    }
    let (untouched, _) = arb_exec::replay_file(&ledger).await.unwrap();
    assert_eq!(
        untouched.exposed()[0].long.as_ref().unwrap().notional_usdt,
        before
    );

    let response =
        run_manual_add(&state, &AddBody::new(Mode::Paper, &opened.id, "100", None)).await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["added"], true, "{body}");

    let (replayed, _) = arb_exec::replay_file(&ledger).await.unwrap();
    let exposed = replayed.exposed();
    assert_eq!(exposed.len(), 1, "补在原来那笔上，不新开");
    assert_eq!(exposed[0].id, opened.id);
    let after = exposed[0].long.as_ref().unwrap().notional_usdt;
    assert!(after > before + dec!(99), "{before} → {after}");
    assert_eq!(exposed[0].leverage, opened.leverage, "杠杆沿用原仓位");
    assert!(
        replayed
            .orders
            .keys()
            .any(|id| id.starts_with(&opened.id) && id.ends_with("-add1"))
    );
}

/// 上市前合约两腿口径不同（HL-io 完全稀释估值 ↔ 别家假定股本）：预览就拒，台账不动（实盘遇到过，平仓亏损不小）。
/// 上面的夹具原来就是 某合约（hyperliquid-io:lighter-rh），2026-11-04 起这一对不再允许下单，夹具改成了 某合约。
#[tokio::test]
async fn pre_ipo_pairs_with_different_ipo_conversion_are_refused_on_open() {
    let (dir, state, books) = sandbox(false, false).await;
    let body = crate::trade::auto_open_body(
        Mode::Paper,
        "ANTHROPIC/USDT",
        Venue::LighterRh,
        Venue::HyperliquidIo,
        dec!(200),
        dec!(3),
        arb_exec::MarginMode::Isolated,
        &arb_exec::TaskRules::default(),
    );
    let (code, preview) = crate::trade::open_for_auto(Arc::clone(&state), body, true).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{preview}");
    assert!(
        preview["error"].as_str().unwrap().contains("口径不同"),
        "{preview}"
    );
    assert!(!dir.0.join("paper.jsonl").exists(), "没碰台账");
    assert!(
        books
            .iter()
            .all(|b| b.depth_calls.load(Ordering::SeqCst) == 0),
        "没拉盘口"
    );
}

/// 上市前 / 盘前合约就算两腿口径一样也不做（2026-11-04 用户定：不交易 pre-IPO 等资产）。
#[tokio::test]
async fn pre_ipo_symbols_are_refused_on_open_even_with_matching_bases() {
    let (dir, state, books) = sandbox(false, false).await;
    for base in ["OURA", "MOONSHOT"] {
        let body = crate::trade::auto_open_body(
            Mode::Paper,
            &format!("{base}/USDT"),
            Venue::LighterRh,
            Venue::Lighter,
            dec!(200),
            dec!(3),
            arb_exec::MarginMode::Isolated,
            &arb_exec::TaskRules::default(),
        );
        let (code, preview) = crate::trade::open_for_auto(Arc::clone(&state), body, true).await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "{preview}");
        assert!(
            preview["error"].as_str().unwrap().contains("上市前"),
            "{preview}"
        );
    }
    assert!(!dir.0.join("paper.jsonl").exists(), "没碰台账");
    assert!(
        books
            .iter()
            .all(|b| b.depth_calls.load(Ordering::SeqCst) == 0),
        "没拉盘口"
    );
}
