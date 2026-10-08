use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use arb_core::Level;
use arb_scanner::{ExcludedReading, SymbolView, Totals};
use rust_decimal_macros::dec;

/// 桩场所：一档买一卖一，每档名义额可调；`fail` 时拉盘口直接报错。
struct Book {
    venue: Venue,
    bid: Decimal,
    ask: Decimal,
    depth_usdt: Decimal,
    fail: Option<&'static str>,
    calls: AtomicUsize,
}

impl Book {
    fn new(venue: Venue, bid: Decimal, ask: Decimal) -> Self {
        Self {
            venue,
            bid,
            ask,
            depth_usdt: dec!(100000),
            fail: None,
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl VenueApi for Book {
    fn venue(&self) -> Venue {
        self.venue
    }
    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        Ok(Vec::new())
    }
    async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(message) = self.fail {
            return Err(ArbError::venue("stub", message));
        }
        let level = |price| Level {
            price,
            notional_usdt: self.depth_usdt,
        };
        Ok(OrderBook {
            venue: self.venue,
            symbol: symbol.clone(),
            bids: vec![level(self.bid)],
            asks: vec![level(self.ask)],
        })
    }
}

fn symbol(base: &str) -> Symbol {
    Symbol::perp(base, "USDT")
}

/// DEX 快照：有标记价、没有买一卖一。
fn snap(venue: Venue, base: &str, mark: Decimal) -> MarketSnapshot {
    MarketSnapshot {
        venue,
        symbol: symbol(base),
        period_rate: dec!(0.00001),
        interval_h: 1,
        interval_assumed: true,
        next_funding_at: chrono::Utc::now(),
        next_funding_estimated: true,
        taker_fee: Some(dec!(0.0002)),
        mark_price: Some(mark),
        index_price: Some(mark),
        best_bid: None,
        best_ask: None,
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt: None,
        quote_volume_24h: None,
        max_leverage: Some(dec!(10)),
        maintenance_margin: Some(dec!(0.01)),
        oi_capped: false,
    }
}

fn report(views: Vec<Vec<MarketSnapshot>>) -> ScanReport {
    ScanReport {
        generated_at: chrono::Utc::now(),
        fee_per_side: dec!(0.0005),
        amortize_days: dec!(7),
        spread_hold_days: dec!(3),
        max_entry_basis_pct: Some(dec!(0.5)),
        min_venues: 2,
        venues: Vec::new(),
        symbols: views
            .into_iter()
            .map(|rates| SymbolView {
                symbol: rates[0].symbol.clone(),
                rates,
                funding: Vec::new(),
                spread: Vec::new(),
            })
            .collect(),
        unverified: Vec::new(),
        suspicious: Vec::new(),
        gated: Vec::new(),
        totals: Totals::default(),
    }
}

/// ANTHROPIC：Lighter 100、Hyperliquid 102，价差 ≈ 2%，多 Lighter / 空 Hyperliquid。
fn spread_report() -> ScanReport {
    report(vec![vec![
        snap(Venue::Hyperliquid, "ANTHROPIC", dec!(102)),
        snap(Venue::Lighter, "ANTHROPIC", dec!(100)),
    ]])
}

fn limits() -> Limits {
    Limits {
        max_position_usdt: dec!(5000),
        max_open_positions: 5,
        max_daily_loss_usdt: dec!(100),
        max_slippage: dec!(0.002),
        max_entry_basis_pct: dec!(5),
        min_liq_distance_pct: dec!(8),
        require_stable_funding: false,
    }
}

fn checker(long: Book, short: Book) -> Arc<Prechecker> {
    Prechecker::with_apis(HashMap::from([
        (long.venue, Arc::new(long) as Arc<dyn VenueApi>),
        (short.venue, Arc::new(short) as Arc<dyn VenueApi>),
    ]))
}

#[test]
fn margin_modes_do_not_share_watch_or_verdict_cache_keys() {
    let isolated = watch_key(dec!(1000));
    let mut cross = isolated.clone();
    cross.margin_mode = arb_exec::MarginMode::Cross;
    assert_ne!(isolated, cross);
    let report = report(vec![vec![
        snap(Venue::Hyperliquid, "BTC", dec!(102)),
        snap(Venue::Lighter, "BTC", dec!(100)),
    ]]);
    let board = pair_board(&report, isolated.a, isolated.b, "spread");
    assert_ne!(
        VerdictKey::of(&isolated, &board.rows[0]).unwrap(),
        VerdictKey::of(&cross, &board.rows[0]).unwrap()
    );
}

fn watch_key(size: Decimal) -> WatchKey {
    WatchKey {
        margin_mode: arb_exec::MarginMode::Isolated,
        view: "spread".into(),
        a: Venue::Hyperliquid,
        b: Venue::Lighter,
        size,
        leverage: dec!(2),
        live: false,
    }
}

/// 登记一组关注，跑一步，返回 ANTHROPIC 那一行的结论。
async fn check_once(checker: &Prechecker, report: &ScanReport, key: &WatchKey) -> RowCheck {
    checker.watch(key.clone(), None);
    assert!(checker.step(report, &limits()).await, "应当有一行要查");
    let board = pair_board(report, key.a, key.b, &key.view);
    checker
        .annotate(&board, key, None, &TaskRules::default())
        .rows
        .remove("ANTHROPIC/USDT")
        .expect("ANTHROPIC 应当有结论")
}

#[tokio::test]
async fn a_pair_with_deep_books_passes() {
    let checker = checker(
        Book::new(Venue::Lighter, dec!(99.9), dec!(100)),
        Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)),
    );
    let report = spread_report();
    let key = watch_key(dec!(1000));
    let row = check_once(&checker, &report, &key).await;
    assert_eq!(row.status, Status::Pass, "{}", row.reason);
    assert!(row.reason.contains("净"), "{}", row.reason);
    // 查过了、还没到期：不再重复查。
    assert!(!checker.step(&report, &limits()).await);
}

#[tokio::test]
async fn a_thin_book_is_blocked_with_the_available_size() {
    let mut thin = Book::new(Venue::Lighter, dec!(99.9), dec!(100));
    thin.depth_usdt = dec!(300);
    let checker = checker(thin, Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)));
    let row = check_once(&checker, &spread_report(), &watch_key(dec!(1000))).await;
    assert_eq!(row.status, Status::Blocked);
    assert!(row.reason.contains("可吃到"), "{}", row.reason);
}

#[tokio::test]
async fn a_book_that_cannot_be_fetched_is_unknown_not_blocked() {
    let mut broken = Book::new(Venue::Lighter, dec!(99.9), dec!(100));
    broken.fail = Some("HTTP 502");
    let checker = checker(
        broken,
        Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)),
    );
    let row = check_once(&checker, &spread_report(), &watch_key(dec!(1000))).await;
    assert_eq!(row.status, Status::Unknown);
    assert!(row.reason.contains("盘口没拉到"), "{}", row.reason);
}

#[tokio::test]
async fn a_different_size_is_a_different_verdict() {
    let checker = checker(
        Book::new(Venue::Lighter, dec!(99.9), dec!(100)),
        Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)),
    );
    let report = spread_report();
    check_once(&checker, &report, &watch_key(dec!(1000))).await;
    let other = watch_key(dec!(500));
    checker.watch(other.clone(), None);
    let board = pair_board(&report, other.a, other.b, &other.view);
    let annotated = checker.annotate(&board, &other, None, &TaskRules::default());
    assert_eq!(annotated.pending, 1);
    assert_eq!(annotated.rows["ANTHROPIC/USDT"].status, Status::Pending);
    // 新金额的那一组排在最前：下一步就查它。
    assert!(checker.step(&report, &limits()).await);
    let annotated = checker.annotate(&board, &other, None, &TaskRules::default());
    assert_eq!(annotated.rows["ANTHROPIC/USDT"].status, Status::Pass);
}

/// C0 … C{n-1}：Hyperliquid 贵、Lighter 便宜，价差从大到小（C0 最大）。
fn ranked_report(count: usize) -> ScanReport {
    let views = (0..count)
        .map(|index| {
            let base = format!("C{index}");
            let rich = dec!(120) - Decimal::from(index);
            vec![
                snap(Venue::Hyperliquid, &base, rich),
                snap(Venue::Lighter, &base, dec!(100)),
            ]
        })
        .collect();
    report(views)
}

#[test]
fn every_tradable_row_is_a_candidate_with_the_focus_first() {
    let count = PRIORITY_ROWS + 3;
    let mut report = ranked_report(count);
    report.suspicious.push(ExcludedReading {
        venue: Venue::Lighter,
        symbol: symbol("C1"),
        reason: "差异过大".into(),
    });
    let board = pair_board(&report, Venue::Hyperliquid, Venue::Lighter, "spread");
    let names = |rows: Vec<&PairRow>| {
        rows.iter()
            .map(|row| row.symbol.base.clone())
            .collect::<Vec<_>>()
    };

    let picked = names(candidates(&board, None));
    assert_eq!(picked.len(), count - 1, "全部候选都查，只少被排除的那一行");
    assert_eq!(picked[0], "C0");
    assert!(!picked.contains(&"C1".to_string()), "被排除的行不查");

    // 选中的行排在最前，其余照排名，不重复。
    let last = format!("C{}", count - 1);
    let picked = names(candidates(&board, Some(&format!("{last}/USDT"))));
    assert_eq!(picked[0], last);
    assert_eq!(picked.len(), count - 1);
    assert_eq!(picked[1], "C0");
}

#[test]
fn rows_beyond_the_priority_tier_are_rechecked_less_often() {
    let now = Instant::now();
    let checked = now - Duration::from_secs(150);
    let verdict = Verdict {
        status: Status::Pass,
        reason: String::new(),
        checked_at: checked,
        attempted_at: checked,
        liq_distance_pct: None,
        spread_depth_net: None,
        retry_error: None,
    };
    // 两分半前查过、通过：通过的 90 秒就重查，列出来的要新鲜。
    assert!(verdict.due(now, Tier::Background, false));
    // 不过的：前几名到期，其余还没到。
    let blocked = Verdict {
        status: Status::Blocked,
        ..verdict.clone()
    };
    assert!(blocked.due(now, Tier::Priority, false));
    assert!(!blocked.due(now, Tier::Background, false));
    // 选中的那一行 30 秒就重查。
    let recent = Verdict {
        attempted_at: now - Duration::from_secs(40),
        ..blocked.clone()
    };
    assert!(recent.due(now, Tier::Focus, false));
    assert!(!recent.due(now, Tier::Priority, false));
    assert_eq!(Tier::of(0, true), Tier::Focus);
    assert_eq!(Tier::of(PRIORITY_ROWS - 1, false), Tier::Priority);
    assert_eq!(Tier::of(PRIORITY_ROWS, false), Tier::Background);
    assert_eq!(
        Tier::of(PRIORITY_ROWS, true),
        Tier::Priority,
        "选中的那一行占了一个位置"
    );
}

#[test]
fn a_pass_that_has_not_been_rechecked_for_long_is_not_shown_as_orderable() {
    let report = spread_report();
    let key = watch_key(dec!(1000));
    let board = pair_board(&report, key.a, key.b, &key.view);
    let checker = Prechecker::with_apis(HashMap::new());
    let row = &board.rows[0];
    let old = Instant::now() - PASS_STALE - Duration::from_secs(5);
    checker.lock().verdicts.insert(
        VerdictKey::of(&key, row).unwrap(),
        Verdict {
            status: Status::Pass,
            reason: String::new(),
            checked_at: old,
            attempted_at: old,
            liq_distance_pct: Some(dec!(30)),
            spread_depth_net: None,
            retry_error: None,
        },
    );
    let annotated = checker.annotate(&board, &key, Some("ANTHROPIC/USDT"), &TaskRules::default());
    let shown = &annotated.rows["ANTHROPIC/USDT"];
    assert_eq!(shown.status, Status::Pending);
    assert!(shown.reason.contains("正在重查"), "{}", shown.reason);
    assert!(
        annotated.focus_refreshing,
        "选中的那一行结论太旧：页面要很快再取"
    );
}

#[tokio::test]
async fn a_failed_recheck_keeps_the_previous_verdict() {
    let checker = checker(
        Book::new(Venue::Lighter, dec!(99.9), dec!(100)),
        Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)),
    );
    let report = spread_report();
    let key = watch_key(dec!(1000));
    assert_eq!(
        check_once(&checker, &report, &key).await.status,
        Status::Pass
    );
    // 重查碰到限频冷却：结论仍是上一次的通过，只记下这次没做成。
    let verdict_key = checker.lock().verdicts.keys().next().cloned().unwrap();
    let now = Instant::now();
    checker.lock().record(
        verdict_key.clone(),
        Verdict {
            status: Status::Unknown,
            reason: "lighter 刚碰到限频，预检暂停 90 秒".into(),
            checked_at: now,
            attempted_at: now,
            liq_distance_pct: None,
            spread_depth_net: None,
            retry_error: None,
        },
    );
    let board = pair_board(&report, key.a, key.b, &key.view);
    let row = checker
        .annotate(&board, &key, None, &TaskRules::default())
        .rows
        .remove("ANTHROPIC/USDT")
        .unwrap();
    assert_eq!(row.status, Status::Pass);
    assert!(row.retry_error.unwrap().contains("限频"));
    // 没做成的要尽快重试。
    let inner = checker.lock();
    let kept = &inner.verdicts[&verdict_key];
    assert!(kept.due(now + UNKNOWN_RETRY, Tier::Background, false));
}

#[tokio::test]
async fn pairs_the_snapshot_already_rules_out_are_blocked_without_fetching_books() {
    // 两腿都有买一卖一，可成交价差为正但扣掉成本不划算。
    let mut long = snap(Venue::Lighter, "ANTHROPIC", dec!(100));
    long.best_bid = Some(dec!(101.9));
    long.best_ask = Some(dec!(102));
    long.bid_size_usdt = Some(dec!(100000));
    long.ask_size_usdt = Some(dec!(100000));
    // 空腿买一只比多腿卖一高 0.01%，不够往返手续费。
    let mut short = snap(Venue::Hyperliquid, "ANTHROPIC", dec!(102));
    short.best_bid = Some(dec!(102.01));
    short.best_ask = Some(dec!(102.02));
    short.bid_size_usdt = Some(dec!(100000));
    short.ask_size_usdt = Some(dec!(100000));
    // 另一个合约至少一条腿触及持仓量上限。
    let mut capped = snap(Venue::Lighter, "CAPPED", dec!(100));
    capped.oi_capped = true;
    // 标记价上 Hyperliquid 贵，但盘口上空腿买一低于多腿卖一：可成交价差为负。
    let mut inverted_long = snap(Venue::Lighter, "INVERTED", dec!(100));
    inverted_long.best_ask = Some(dec!(101));
    let mut inverted_short = snap(Venue::Hyperliquid, "INVERTED", dec!(102));
    inverted_short.best_bid = Some(dec!(100.5));
    let report = report(vec![
        vec![short, long],
        vec![snap(Venue::Hyperliquid, "CAPPED", dec!(102)), capped],
        vec![inverted_short, inverted_long],
    ]);
    let lighter = Arc::new(Book::new(Venue::Lighter, dec!(99.9), dec!(100)));
    let checker = Prechecker::with_apis(HashMap::from([
        (Venue::Lighter, Arc::clone(&lighter) as Arc<dyn VenueApi>),
        (
            Venue::Hyperliquid,
            Arc::new(Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1))) as Arc<dyn VenueApi>,
        ),
    ]));
    let key = watch_key(dec!(1000));
    checker.watch(key.clone(), None);
    assert!(!checker.step(&report, &limits()).await, "没有要拉盘口的行");
    assert_eq!(lighter.calls.load(Ordering::SeqCst), 0);

    let board = pair_board(&report, key.a, key.b, &key.view);
    let annotated = checker.annotate(&board, &key, None, &TaskRules::default());
    assert_eq!(annotated.covered, 0);
    let spread = &annotated.rows["ANTHROPIC/USDT"];
    assert_eq!(spread.status, Status::Blocked);
    assert!(spread.reason.contains("不划算"), "{}", spread.reason);
    let inverted = &annotated.rows["INVERTED/USDT"];
    assert_eq!(inverted.status, Status::Blocked);
    assert!(inverted.reason.contains("不为正"), "{}", inverted.reason);
    let capped = &annotated.rows["CAPPED/USDT"];
    assert_eq!(capped.status, Status::Blocked);
    assert!(capped.reason.contains("OI 上限"), "{}", capped.reason);
}

#[tokio::test]
async fn closing_rules_are_applied_when_the_board_is_shown() {
    let checker = checker(
        Book::new(Venue::Lighter, dec!(99.9), dec!(100)),
        Book::new(Venue::Hyperliquid, dec!(102), dec!(102.1)),
    );
    let report = spread_report();
    let key = watch_key(dec!(1000));
    assert_eq!(
        check_once(&checker, &report, &key).await.status,
        Status::Pass
    );
    let board = pair_board(&report, key.a, key.b, &key.view);
    let show = |rules: TaskRules| {
        checker
            .annotate(&board, &key, None, &rules)
            .rows
            .remove("ANTHROPIC/USDT")
            .unwrap()
    };
    // 入场基差约 1.98%：收敛目标 0.1% 成立，3% 不成立（开仓就会触发平仓）。
    let fine = show(TaskRules {
        basis_exit_pct: Some(dec!(0.1)),
        ..TaskRules::default()
    });
    assert_eq!(fine.status, Status::Pass);
    let too_high = show(TaskRules {
        basis_exit_pct: Some(dec!(3)),
        ..TaskRules::default()
    });
    assert_eq!(too_high.status, Status::Blocked);
    assert!(
        too_high.reason.contains("基差收敛目标"),
        "{}",
        too_high.reason
    );
    // 目标低于入场基差（约 1.98%）、但高于吃完深度扣完成本的保本线：收敛到目标就平，
    // 扣完成本不赚钱，与下单闸门同一个结论。
    let no_room = show(TaskRules {
        basis_exit_pct: Some(dec!(1.9)),
        ..TaskRules::default()
    });
    assert_eq!(no_room.status, Status::Blocked);
    assert!(no_room.reason.contains("保本线"), "{}", no_room.reason);
    // 爆仓保护门槛不低于开仓强平距离：不成立。换规则不重新拉盘口。
    let protect = show(TaskRules {
        liq_protection_pct: Some(dec!(99)),
        ..TaskRules::default()
    });
    assert_eq!(protect.status, Status::Blocked);
    assert!(protect.reason.contains("爆仓保护"), "{}", protect.reason);
    assert!(!checker.step(&report, &limits()).await);
}

#[test]
fn watches_expire_and_are_capped() {
    let checker = Prechecker::with_apis(HashMap::new());
    for size in 1..=(MAX_WATCHES as u32 + 2) {
        checker.watch(watch_key(Decimal::from(size)), None);
    }
    let inner = checker.lock();
    assert_eq!(inner.watches.len(), MAX_WATCHES);
    // 留下的是最近登记的几组。
    assert!(!inner.watches.contains_key(&watch_key(Decimal::ONE)));
    drop(inner);

    let mut inner = checker.lock();
    let later = Instant::now() + WATCH_TTL + Duration::from_secs(1);
    inner.expire(later);
    assert!(inner.watches.is_empty());
}

#[tokio::test]
async fn throttled_requests_are_spaced_out() {
    let book = Arc::new(Book::new(Venue::Lighter, dec!(99), dec!(100)));
    let throttled = Throttled::with_spacing(book, Duration::from_millis(60));
    let started = Instant::now();
    for _ in 0..3 {
        throttled.fetch_depth(&symbol("ETH"), 5).await.unwrap();
    }
    // 第一次立即发，后两次各等一个间隔。
    assert!(
        started.elapsed() >= Duration::from_millis(120),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_rate_limited_venue_cools_down_without_calling_upstream() {
    let mut limited = Book::new(Venue::LighterRh, dec!(99), dec!(100));
    limited.fail = Some("HTTP 429 Too Many Requests");
    let limited = Arc::new(limited);
    let throttled =
        Throttled::with_spacing(Arc::clone(&limited) as Arc<dyn VenueApi>, Duration::ZERO);
    assert!(throttled.fetch_depth(&symbol("ETH"), 5).await.is_err());
    let error = throttled.fetch_depth(&symbol("ETH"), 5).await.unwrap_err();
    assert!(error.to_string().contains("限频"), "{error}");
    assert_eq!(
        limited.calls.load(Ordering::SeqCst),
        1,
        "冷却期间不再打上游"
    );
}

#[tokio::test]
async fn unchecked_rows_come_before_rechecks() {
    let report = ranked_report(4);
    let key = watch_key(dec!(100));
    let board = pair_board(&report, key.a, key.b, &key.view);
    let checker = Prechecker::with_apis(HashMap::new());
    checker.watch(key.clone(), Some("C3/USDT".into()));
    // C0 通过、早该重查；C1、C2 还没查过；C3 是选中的那一行，刚查过。
    let now = Instant::now();
    let verdict = |age: u64| Verdict {
        status: Status::Pass,
        reason: String::new(),
        checked_at: now - Duration::from_secs(age),
        attempted_at: now - Duration::from_secs(age),
        liq_distance_pct: Some(dec!(30)),
        spread_depth_net: None,
        retry_error: None,
    };
    let row = |base: &str| {
        board
            .rows
            .iter()
            .find(|row| row.symbol.base == base)
            .unwrap()
    };
    {
        let mut inner = checker.lock();
        inner
            .verdicts
            .insert(VerdictKey::of(&key, row("C0")).unwrap(), verdict(200));
        inner
            .verdicts
            .insert(VerdictKey::of(&key, row("C3")).unwrap(), verdict(5));
    }
    assert_eq!(
        checker.next_job(&report).unwrap().symbol.base,
        "C1",
        "先查没查过的"
    );
    // 选中的那一行结论过期了：它最先。
    checker
        .lock()
        .verdicts
        .insert(VerdictKey::of(&key, row("C3")).unwrap(), verdict(40));
    assert_eq!(checker.next_job(&report).unwrap().symbol.base, "C3");
    // 都查过了：按排名重查到期的。
    {
        let mut inner = checker.lock();
        for base in ["C1", "C2", "C3"] {
            inner
                .verdicts
                .insert(VerdictKey::of(&key, row(base)).unwrap(), verdict(5));
        }
    }
    assert_eq!(checker.next_job(&report).unwrap().symbol.base, "C0");
}

#[test]
fn recently_used_parameters_are_remembered_across_restarts() {
    let dir = std::env::temp_dir().join(format!("arb-precheck-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("watches.json");
    let _ = std::fs::remove_file(&path);
    let seed = watch_key(dec!(1000));
    // 文件还没有：从页面默认参数开始。
    let checker = Prechecker::new(&[], path.clone(), vec![seed.clone()]);
    assert_eq!(checker.lock().remembered, vec![seed.clone()]);
    for size in 1..=(REMEMBERED as u32 + 1) {
        checker.watch(watch_key(Decimal::from(size)), None);
    }
    let expected: Vec<WatchKey> = (2..=(REMEMBERED as u32 + 1))
        .rev()
        .map(|size| watch_key(Decimal::from(size)))
        .collect();
    assert_eq!(checker.lock().remembered, expected, "最近的在前，只留几组");
    // 「重启」：从文件读回同一份名单。
    let restarted = Prechecker::new(&[], path.clone(), vec![seed]);
    assert_eq!(restarted.lock().remembered, expected);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn remembered_parameters_are_checked_while_nobody_is_looking() {
    let report = ranked_report(2);
    let key = watch_key(dec!(100));
    let checker = Prechecker::with_apis(HashMap::new());
    checker.watch(key.clone(), None);
    // 页面关了：关注过期，但参数还记着，后台照样有活干。
    checker
        .lock()
        .expire(Instant::now() + WATCH_TTL + Duration::from_secs(1));
    assert!(checker.lock().watches.is_empty());
    assert_eq!(checker.next_job(&report).unwrap().symbol.base, "C0");
    // 没人看时放慢：通过的 4 分钟、不过的 10 分钟才重查。
    let now = Instant::now();
    let verdict = Verdict {
        status: Status::Pass,
        reason: String::new(),
        checked_at: now - Duration::from_secs(150),
        attempted_at: now - Duration::from_secs(150),
        liq_distance_pct: None,
        spread_depth_net: None,
        retry_error: None,
    };
    assert!(verdict.due(now, Tier::Priority, false));
    assert!(!verdict.due(now, Tier::Priority, true));
    assert!(verdict.due(now + IDLE_PASS_REFRESH, Tier::Priority, true));
    assert!(IDLE_PASS_REFRESH < PASS_STALE && IDLE_REFRESH < VERDICT_KEEP);
}

/// 带缓存命中标志的桩：`cached` 为真时表示这次不会打上游。
struct CachedHistory {
    cached: bool,
    calls: AtomicUsize,
}

#[async_trait]
impl VenueApi for CachedHistory {
    fn venue(&self) -> Venue {
        Venue::Lighter
    }
    async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
        Ok(Vec::new())
    }
    fn supports_funding_history(&self) -> bool {
        true
    }
    fn funding_history_cached(&self, _: &Symbol, _: u32) -> bool {
        self.cached
    }
    async fn fetch_funding_history(&self, _: &Symbol, _: u32) -> ArbResult<Vec<FundingPoint>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn a_cached_history_read_is_not_paced_but_an_uncached_one_is() {
    let cached = Arc::new(CachedHistory {
        cached: true,
        calls: AtomicUsize::new(0),
    });
    let throttled = Throttled::with_spacing(
        Arc::clone(&cached) as Arc<dyn VenueApi>,
        Duration::from_millis(200),
    );
    let started = Instant::now();
    for _ in 0..4 {
        throttled
            .fetch_funding_history(&symbol("ETH"), 30)
            .await
            .unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(cached.calls.load(Ordering::SeqCst), 4);

    let cold = Arc::new(CachedHistory {
        cached: false,
        calls: AtomicUsize::new(0),
    });
    let throttled = Throttled::with_spacing(cold as Arc<dyn VenueApi>, Duration::from_millis(80));
    let started = Instant::now();
    for _ in 0..3 {
        throttled
            .fetch_funding_history(&symbol("ETH"), 30)
            .await
            .unwrap();
    }
    assert!(
        started.elapsed() >= Duration::from_millis(160),
        "{:?}",
        started.elapsed()
    );
}
