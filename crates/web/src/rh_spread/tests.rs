use super::history::History;
use super::*;

/// 手动联网核对（默认忽略）：`cargo test -p arb-web live_probe -- --ignored --nocapture`。
#[tokio::test]
#[ignore]
async fn live_probe() {
    let dir = std::env::temp_dir().join(format!("rh-spread-probe-{}", std::process::id()));
    let pairs = Pair::parse_list(pairs::DEFAULT_PAIRS).unwrap();
    let config = Config {
        enabled: true,
        size_usdt: Decimal::from(2000),
        alert_net_pct: Decimal::new(5, 2),
        equities_only: false,
        dir: dir.clone(),
        // `PROBE_WATCH=off`：不带观察组（对比资源占用用）。
        watch_pairs: if std::env::var("PROBE_WATCH").as_deref() == Ok("off") {
            Vec::new()
        } else {
            pairs::watch_pairs(&pairs, &pairs::DEFAULT_WATCH)
        },
        pairs,
        entropy_rebate: Decimal::TWO,
        hl_shadow: hl_shadow::Config::default(),
        maker_shadow: true,
    };
    // 观察组的历史在后台读：先给第一个观察组放 150 分钟的历史，跑完看它读回来了没有。
    let seeded = config.watch_pairs.first().copied();
    if let Some(pair) = seeded {
        let now = Utc::now();
        let rows: Vec<history::Row> = (1..=150)
            .map(|back| history::Row {
                t: (now.timestamp() / 60 - 200 + back) * 60,
                s: "NVDA".into(),
                k: Session::Rth,
                b: 0.1,
                n: 10,
                ea: None,
                el: None,
            })
            .collect();
        history::append(&dir, &pair.file_prefix(), &rows, now)
            .await
            .unwrap();
    }
    let monitor = Monitor::new(config, crate::alert::Alerter::with_sink(None));
    let client = arb_venues::build_client(20).unwrap();
    // 先跑一轮扫描给 Hyperliquid 的组核对身份。
    let settings = arb_core::Settings::from_env().unwrap();
    let apis = arb_venues::build_all(&settings, &client);
    let cache = Arc::new(crate::cache::ScanCache::default());
    cache.put(arb_scanner::scan(&apis, &settings).await).await;
    monitor.spawn(client, cache);
    tokio::time::sleep(Duration::from_secs(90)).await;
    let panel = monitor.panel_view().await;
    if let Some(pair) = seeded {
        let seen = panel
            .watch
            .pairs
            .iter()
            .find(|p| p.id == pair.id())
            .unwrap();
        assert!(
            seen.history_minutes >= 150,
            "观察组的历史没读回来：{}",
            seen.history_minutes
        );
    }
    let mut view = panel.view;
    view.pairs.extend(panel.watch.pairs);
    view.lines.extend(panel.watch.lines);
    println!(
        "connected={:?} error={:?} lines={}",
        view.connected,
        view.error,
        view.lines.len()
    );
    for pair in &view.pairs {
        println!(
            "pair {} markets {} fee {:?}~{:?} history {} note {:?}",
            pair.id,
            pair.markets,
            pair.fee_min_pct,
            pair.fee_max_pct,
            pair.history_minutes,
            pair.note
        );
    }
    for line in view.lines.iter().take(400) {
        let leg = |l: &Option<Leg>| {
            l.as_ref()
                .map(|l| {
                    format!(
                        "entry {:+.3} exit {:.3} net0 {:+.3}",
                        l.quote.entry_pct, l.quote.exit_cross_pct, l.net_to_zero_pct
                    )
                })
                .unwrap_or("-".into())
        };
        println!(
            "{:28} {:10} {:6} basis {:>9} age {:?} | long_a {} | long_b {} | note {:?}",
            line.pair,
            line.base,
            line.session.label(),
            line.basis_pct.map(|b| b.to_string()).unwrap_or("-".into()),
            line.age_sec,
            leg(&line.long_a),
            leg(&line.long_b),
            line.note
        );
    }
    let files: Vec<_> = std::fs::read_dir(&dir)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    for f in &files {
        println!(
            "file {} lines {}",
            f.display(),
            std::fs::read_to_string(f).unwrap().lines().count()
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

use chrono::TimeZone;
use rust_decimal_macros::dec;

fn book(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)], at: Instant) -> LocalBook {
    let mut b = LocalBook::new(at);
    bids.iter()
        .for_each(|(p, q)| LocalBook::apply(&mut b.bids, *p, *q));
    asks.iter()
        .for_each(|(p, q)| LocalBook::apply(&mut b.asks, *p, *q));
    b
}

fn config() -> Config {
    Config {
        enabled: true,
        size_usdt: dec!(1000),
        alert_net_pct: dec!(0.05),
        equities_only: false,
        dir: std::env::temp_dir(),
        pairs: vec![Pair::RH],
        watch_pairs: Vec::new(),
        entropy_rebate: Decimal::ZERO,
        hl_shadow: hl_shadow::Config::default(),
        maker_shadow: true,
    }
}

#[test]
fn observer_markets_do_not_pollute_trade_latency_or_wake_the_fast_path() {
    let mut traded = market("equity");
    traded.a.key = "traded-a".into();
    traded.b.key = "traded-b".into();
    let mut watched = market("equity");
    watched.a.key = "watch-a".into();
    watched.b.key = "watch-b".into();
    let state = |watch, market| PairState {
        pair: Pair::RH,
        markets: vec![market],
        history: History::default(),
        minute: Minute::default(),
        normals: HashMap::new(),
        recents: HashMap::new(),
        note: None,
        watch,
    };
    let active = active_books(&[state(false, traded), state(true, watched)]);
    assert!(active[&Venue::Arcus].contains("traded-a"));
    assert!(active[&Venue::LighterRh].contains("traded-b"));
    assert!(!active[&Venue::Arcus].contains("watch-a"));
    assert!(!active[&Venue::LighterRh].contains("watch-b"));
    assert_eq!(active.values().map(|keys| keys.len()).sum::<usize>(), 2);
}

/// Arcus（a，吃单 0.0225%）↔ Lighter RH（b，0）的 SPY。
fn market(category: &str) -> PairMarket {
    market_with(category, Some(dec!(0.000225)))
}

fn market_with(category: &str, arcus_fee: Option<Decimal>) -> PairMarket {
    let venue_market = |key: &str, fee| pairs::VenueMarket {
        key: key.into(),
        taker_fee: fee,
        maker_fee: None,
        category: None,
        outside_rth: None,
        bounds: None,
        size_multiplier: None,
    };
    PairMarket {
        base: "SPY".into(),
        a: venue_market("SPY-USD", arcus_fee),
        b: venue_market("26", Some(Decimal::ZERO)),
        category: category.into(),
        outside_rth: Some(true),
    }
}

fn normal(median: f64) -> Normal {
    Normal {
        session: Session::Off,
        median,
        p10: median - 0.02,
        p90: median + 0.02,
        mad: 0.01,
        minutes: 500,
    }
}

#[test]
fn book_walks_depth_and_rejects_crossed_or_thin_books() {
    let now = Instant::now();
    let b = book(
        &[(dec!(99), dec!(5)), (dec!(98), dec!(5))],
        &[(dec!(101), dec!(5)), (dec!(102), dec!(5))],
        now,
    );
    assert_eq!(b.top(), Some((dec!(99), dec!(101))));
    assert_eq!(b.buy_avg(dec!(10)), Some(dec!(101.5)));
    assert_eq!(b.sell_avg(dec!(10)), Some(dec!(98.5)));
    assert_eq!(b.buy_avg(dec!(11)), None, "深度不够不能当吃满");
    let mut deleted = b.clone();
    LocalBook::apply(&mut deleted.asks, dec!(101), Decimal::ZERO);
    assert_eq!(deleted.best_ask(), Some(dec!(102)), "数量 0 = 删档");
    let crossed = book(&[(dec!(101), dec!(1))], &[(dec!(100), dec!(1))], now);
    assert_eq!(crossed.mid(), None, "交叉盘口不能用");
}

#[test]
fn direction_quote_is_executable_and_includes_exit_crossing() {
    let now = Instant::now();
    // Arcus 便宜：买 Arcus 100.0、卖 Lighter 100.2。
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let q = quote(&arcus, &lighter, dec!(1000)).unwrap();
    // 参考价 = (99.99 + 100.21)/2 = 100.1；价差 0.2 → 0.1998%。
    assert_eq!(q.entry_pct, dec!(0.1998));
    // 平仓穿价：两边各半个价差 0.01 + 0.01 = 0.02 → 0.01998%。
    assert_eq!(q.exit_cross_pct, dec!(0.01998));
    assert!(quote(&lighter, &arcus, dec!(1000)).unwrap().entry_pct < Decimal::ZERO);
    assert_eq!(mid_basis_pct(&arcus, &lighter), Some(dec!(-0.21978)));
}

#[test]
fn arcus_bids_outside_its_trading_band_are_not_a_spread() {
    // 2026-10-17 21:31 UTC MU：Lighter 跌到 819，Arcus 被钉在盘后价格带下限 831.38，买单挂在 829～831。
    // 看起来空 Arcus、多 Lighter 有 1% 以上的价差，实际卖单整张被拒 FILL_WILL_EXCEED_TRADING_BOUND。
    let now = Instant::now();
    let arcus = book(
        &[(dec!(831.10), dec!(5)), (dec!(829.60), dec!(5))],
        &[(dec!(831.50), dec!(5)), (dec!(832.00), dec!(5))],
        now,
    );
    let lighter = book(&[(dec!(818.90), dec!(5))], &[(dec!(819.10), dec!(5))], now);
    let cfg = config();
    let run = |bounds: Option<(Decimal, Decimal)>| {
        let mut m = market("EQUITIES");
        m.a.bounds = bounds.map(|(lower, upper)| arb_venues::arcus::TradingBounds { lower, upper });
        evaluate(
            Pair::RH,
            &m,
            Some(&arcus),
            Some(&lighter),
            Some(normal(0.0)),
            500,
            Session::Off,
            &cfg,
            now,
        )
    };
    let unknown = run(None);
    assert!(unknown.long_b.as_ref().unwrap().quote.entry_pct > dec!(1));
    assert!(
        unknown.best.as_ref().unwrap().signal,
        "不知道价格带时是个大信号"
    );

    let pinned = run(Some((dec!(831.38), dec!(918.88))));
    assert!(pinned.long_a.is_none() && pinned.long_b.is_none());
    assert!(pinned.best.is_none(), "没有信号就不提醒、不自动开仓");
    let note = pinned.note.unwrap();
    assert!(note.contains("arcus") && note.contains("831.38"), "{note}");
    assert_eq!(
        pinned.basis_pct, unknown.basis_pct,
        "基差照原盘口记，历史不受影响"
    );

    // 带把整本盘口都包住：和不知道带一样。
    let wide = run(Some((dec!(800), dec!(900))));
    assert_eq!(
        wide.long_b.unwrap().quote,
        unknown.long_b.as_ref().unwrap().quote
    );
    assert_eq!(wide.note, None);

    // 只剔掉一部分（下限 830：829.60 那档没了，831.10 那档还在）：按剩下的算，深度够就照常。
    let partial = run(Some((dec!(830), dec!(900))));
    assert!(partial.long_b.is_some());
    assert_eq!(partial.note, None);
}

#[test]
fn net_to_normal_subtracts_the_part_of_the_basis_that_never_comes_back() {
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let m = market("INDICES");
    let cfg = config();
    // 没有正常水平：只给「收敛到 0」，不发信号。
    let line = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        None,
        30,
        Session::Off,
        &cfg,
        now,
    );
    let leg = line.long_a.clone().unwrap();
    assert_eq!(
        leg.net_to_zero_pct,
        dec!(0.1998) - dec!(0.01998) - dec!(0.045)
    );
    assert_eq!(leg.net_to_normal_pct, None);
    assert_eq!(line.normal_missing_minutes, history::MIN_MINUTES - 30);
    let best = line.best.unwrap();
    assert_eq!(best.direction, "long_a");
    assert!(!best.signal, "没有正常水平不提醒");

    // 正常基差就是 −0.2%（休市时 Arcus 一直便宜）：回到正常水平几乎什么都赚不到。
    let line = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.2)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let leg = line.long_a.clone().unwrap();
    assert_eq!(
        leg.net_to_normal_pct,
        Some((leg.net_to_zero_pct - dec!(0.2)).round_dp(5))
    );
    assert!(!line.best.unwrap().signal, "系统性偏差不是机会");

    // 正常基差 −0.05%：现在偏到 −0.22%，回到 −0.05% 净赚 ≈ 0.085%，超过门槛 0.05%。
    let line = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let best = line.best.clone().unwrap();
    assert!(best.signal);
    assert_eq!(best.net_usdt, Some(dec!(0.85)));
    assert!(line.z.unwrap() < -10.0);
    let text = alert_text(&line, &best, &cfg, None).unwrap();
    // 自动交易给了结论：单独一行写它，替换笼统的「仅偏离提醒」。
    let with_auto =
        alert_text(&line, &best, &cfg, Some("自动交易：未开启，不会自动下单。")).unwrap();
    assert!(
        with_auto.contains("\n🤖 自动交易：未开启，不会自动下单。\n同一合约")
            && !with_auto.contains("仅偏离提醒"),
        "{with_auto}"
    );
    for part in [
        "SPY",
        "多 Arcus / 空 Lighter RH",
        "盘后",
        "-0.050%",
        "0.85 USDT",
        "Arcus − Lighter RH",
    ] {
        assert!(text.contains(part), "{part} 不在：{text}");
    }

    // 反方向：Lighter 便宜时选多 Lighter；正常基差的符号反过来用。
    let line = evaluate(
        Pair::RH,
        &m,
        Some(&lighter),
        Some(&arcus),
        Some(normal(0.05)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let best = line.best.unwrap();
    assert_eq!(best.direction, "long_b");
    assert!(best.signal);
    assert_eq!(best.net_usdt, Some(dec!(0.85)));
}

#[test]
fn stale_thin_or_missing_books_never_signal() {
    let now = Instant::now();
    let arcus = book(&[(dec!(99.98), dec!(1))], &[(dec!(100.00), dec!(1))], now);
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let m = market("INDICES");
    let cfg = config();
    let thin = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(normal(0.0)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    assert!(thin.best.is_none() && thin.note.unwrap().contains("深度不够"));
    let old = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let later = now + Duration::from_secs(16);
    let stale = evaluate(
        Pair::RH,
        &m,
        Some(&old),
        Some(&lighter),
        Some(normal(0.0)),
        500,
        Session::Off,
        &cfg,
        later,
    );
    assert!(stale.best.is_none() && stale.note.unwrap().contains("没更新"));
    let missing = evaluate(
        Pair::RH,
        &m,
        None,
        Some(&lighter),
        None,
        0,
        Session::Off,
        &cfg,
        now,
    );
    assert!(missing.best.is_none() && missing.basis_pct.is_none());
    let no_fee = evaluate(
        Pair::RH,
        &market_with("INDICES", None),
        Some(&old),
        Some(&lighter),
        Some(normal(0.0)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    assert!(no_fee.best.is_none(), "不知道手续费就不估净收益");
}

#[test]
fn sessions_follow_new_york_time_and_dst() {
    let at = |y, mo, d, h, mi| Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap();
    // 2026-10-07 周三 14:00 UTC = 纽约 10:00（夏令时）。
    assert_eq!(classify(at(2026, 10, 7, 14, 0), false, None), Session::Rth);
    assert_eq!(classify(at(2026, 10, 7, 13, 29), false, None), Session::Off);
    assert_eq!(classify(at(2026, 10, 7, 20, 0), false, None), Session::Off);
    // 12 月是标准时间：14:30 UTC = 9:30。
    assert_eq!(classify(at(2026, 12, 2, 14, 30), false, None), Session::Rth);
    assert_eq!(classify(at(2026, 12, 2, 14, 29), false, None), Session::Off);
    // 周六纽约时间；周五晚 UTC 已是周六但纽约仍是周五。
    assert_eq!(
        classify(at(2026, 10, 10, 15, 0), false, None),
        Session::Weekend
    );
    assert_eq!(classify(at(2026, 10, 10, 2, 0), false, None), Session::Off);
    // Arcus 说了算（节假日）；加密币不分时段。
    assert_eq!(
        classify(at(2026, 10, 7, 14, 0), false, Some(true)),
        Session::Off
    );
    assert_eq!(
        classify(at(2026, 10, 10, 15, 0), true, Some(true)),
        Session::All
    );
    // Arcus 的标志 5 分钟才刷新：16:00 以后还报「盘中」不信（时钟说盘外就是盘外）；
    // 9:30 后头几分钟还报「盘外」也不信（可能是开盘前的旧值），过了这几分钟才当节假日 / 提前收盘。
    // 2026-10-07 20:00 UTC = 纽约 16:00；13:30 UTC = 9:30。
    assert_eq!(
        classify(at(2026, 10, 7, 20, 2), false, Some(false)),
        Session::Off
    );
    assert_eq!(
        classify(at(2026, 10, 7, 13, 32), false, Some(true)),
        Session::Rth
    );
    assert_eq!(
        classify(at(2026, 10, 7, 13, 36), false, Some(true)),
        Session::Off
    );
    assert_eq!(arb_core::new_york::offset_hours(at(2026, 3, 8, 6, 59)), -5);
    assert_eq!(arb_core::new_york::offset_hours(at(2026, 3, 8, 7, 0)), -4);
    assert_eq!(arb_core::new_york::offset_hours(at(2026, 11, 1, 5, 59)), -4);
    assert_eq!(arb_core::new_york::offset_hours(at(2026, 11, 1, 6, 0)), -5);
}

/// 近期可成交价差：只看最近几小时、每个方向各自够 60 分钟样本才给中位数；超过 24 小时的从内存里裁掉。
#[test]
fn the_recent_entry_level_follows_a_level_shift_within_hours() {
    let now = Utc.with_ymd_and_hms(2026, 10, 16, 3, 43, 0).unwrap();
    let mut h = History::default();
    let row = |minutes_ago: i64, ea: Option<f64>, el: Option<f64>| Row {
        t: now.timestamp() - 60 * minutes_ago,
        s: "OPENAI".into(),
        k: Session::All,
        b: -2.0,
        n: 60,
        ea,
        el,
    };
    // 20 小时前到 8 小时前在旧水平 1.7%，最近 8 小时在新水平 2.65%（10-15 那次跳变）。
    for minutes_ago in (1..=20 * 60).rev() {
        let ea = if minutes_ago > 8 * 60 { 1.70 } else { 2.65 };
        // 反方向只有最近 30 分钟有盘口。
        let el = (minutes_ago <= 30).then_some(-2.70);
        h.push(&row(minutes_ago, Some(ea), el));
    }
    let recent = h.recent_entry("OPENAI", 12, now).unwrap();
    assert_eq!(recent.hours, 12);
    assert_eq!(recent.long_a_pct, Some(2.65), "近 12 小时已经是新水平");
    assert_eq!(recent.long_b_pct, None, "不够 60 分钟样本的方向不给");
    // 窗口越长越被旧水平拖住（20 小时里旧水平占多数）；超过保留时长的按保留时长算。
    assert_eq!(
        h.recent_entry("OPENAI", 20, now).unwrap().long_a_pct,
        Some(1.70)
    );
    assert_eq!(
        h.recent_entry("OPENAI", 48, now).unwrap().hours,
        history::RECENT_KEEP_HOURS
    );
    assert!(h.recent_entry("SPY", 12, now).is_none());
    // 没有可成交价差（缺盘口）的行不进近期序列。
    let mut empty = History::default();
    for minutes_ago in 1..=120 {
        empty.push(&row(minutes_ago, None, None));
    }
    assert!(empty.recent_entry("OPENAI", 12, now).is_none());
    // 裁剪：一天之后这些都出了窗口。
    h.prune(now + chrono::Duration::hours(25));
    assert!(
        h.recent_entry("OPENAI", 12, now + chrono::Duration::hours(25))
            .is_none()
    );
}

#[test]
fn normal_needs_enough_minutes_in_the_same_session_and_window() {
    let now = Utc.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap();
    let mut h = History::default();
    for i in 0..(history::MIN_MINUTES as i64 - 1) {
        h.push(&Row {
            t: now.timestamp() - 60 * (500 - i),
            s: "SPY".into(),
            k: Session::Off,
            b: -0.1,
            n: 30,
            ea: None,
            el: None,
        });
    }
    assert!(
        h.normal("SPY", Session::Off, now).is_none(),
        "差一分钟也不给"
    );
    h.push(&Row {
        t: now.timestamp() - 60,
        s: "SPY".into(),
        k: Session::Off,
        b: 0.3,
        n: 30,
        ea: None,
        el: None,
    });
    let n = h.normal("SPY", Session::Off, now).unwrap();
    assert_eq!((n.median, n.minutes), (-0.1, history::MIN_MINUTES));
    assert!(h.normal("SPY", Session::Rth, now).is_none(), "时段分开");
    // 乱序行丢弃；窗口外的裁掉。
    h.push(&Row {
        t: 0,
        s: "SPY".into(),
        k: Session::Off,
        b: 9.0,
        n: 1,
        ea: None,
        el: None,
    });
    assert_eq!(h.minutes("SPY", Session::Off, now), history::MIN_MINUTES);
    h.prune(now + chrono::Duration::days(history::WINDOW_DAYS + 1));
    assert_eq!(h.coverage_minutes(), 0);
}

#[tokio::test]
async fn history_round_trips_through_daily_files_and_skips_broken_lines() {
    let dir =
        std::env::temp_dir().join(format!("rh-spread-test-{}-{}", std::process::id(), line!()));
    let now = Utc.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap();
    let rows: Vec<Row> = (0..3)
        .map(|i| Row {
            t: now.timestamp() - 180 + 60 * i,
            s: "QQQ".into(),
            k: Session::Off,
            b: -0.05,
            n: 20,
            ea: Some(0.01),
            el: None,
        })
        .collect();
    history::append(&dir, "rh-spread", &rows, now)
        .await
        .unwrap();
    // 另一组的文件：前缀不同，互不影响、不被误删。
    let other = Pair::parse("hyperliquid-xyz:lighter-rh")
        .unwrap()
        .file_prefix();
    assert_eq!(other, "spread-hyperliquid-xyz-lighter-rh");
    assert_eq!(
        Pair::RH.file_prefix(),
        "rh-spread",
        "最早那组沿用原来的文件，历史照常读"
    );
    let other_old = history::file_for(
        &dir,
        &other,
        (now - chrono::Duration::days(history::KEEP_DAYS + 3)).date_naive(),
    );
    std::fs::write(&other_old, "").unwrap();
    let path = history::file_for(&dir, "rh-spread", now.date_naive());
    let mut text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("\"el\""), "缺的字段不写");
    text.push_str("{broken\n");
    std::fs::write(&path, text).unwrap();
    // 超过保留期的旧文件在下次写入时删掉。
    let ancient = history::file_for(
        &dir,
        "rh-spread",
        (now - chrono::Duration::days(history::KEEP_DAYS + 3)).date_naive(),
    );
    std::fs::write(&ancient, "").unwrap();
    history::append(&dir, "rh-spread", &rows[..1], now)
        .await
        .unwrap();
    assert!(!ancient.exists());
    assert!(other_old.exists(), "只清理本组的旧文件");
    let (loaded, broken) = history::load(&dir, "rh-spread", now).await;
    assert_eq!((loaded.minutes("QQQ", Session::Off, now), broken), (3, 1));
    let (empty, _) = history::load(&dir, &other, now).await;
    assert_eq!(empty.coverage_minutes(), 0, "各组历史分开");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn the_history_writer_appends_in_order_off_the_loop_and_cleans_old_files_once_a_day() {
    let dir =
        std::env::temp_dir().join(format!("rh-spread-test-{}-{}", std::process::id(), line!()));
    std::fs::create_dir_all(&dir).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap();
    let row = |minute: i64| Row {
        t: now.timestamp() + 60 * minute,
        s: "QQQ".into(),
        k: Session::Off,
        b: -0.05,
        n: 20,
        ea: None,
        el: None,
    };
    let old = |prefix: &str| {
        history::file_for(
            &dir,
            prefix,
            (now - chrono::Duration::days(history::KEEP_DAYS + 3)).date_naive(),
        )
    };
    std::fs::write(old("rh-spread"), "").unwrap();
    let writer = history::Writer::spawn(dir.clone());
    writer.send(
        vec![
            ("rh-spread".into(), vec![row(0)]),
            ("spread-x".into(), Vec::new()),
        ],
        now,
    );
    writer.send(vec![("rh-spread".into(), vec![row(1)])], now);
    // 同一天第二次不再扫目录：这时放进去的旧文件要等到第二天才清。
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!old("rh-spread").exists(), "当天第一次写时清掉旧文件");
    std::fs::write(old("rh-spread"), "").unwrap();
    writer.send(vec![("rh-spread".into(), vec![row(2)])], now);
    writer.finish(Duration::from_secs(5)).await;
    assert!(old("rh-spread").exists(), "同一天只清一次");
    assert!(
        !history::file_for(&dir, "spread-x", now.date_naive()).exists(),
        "空的组不写文件"
    );
    let (loaded, broken) =
        history::load(&dir, "rh-spread", now + chrono::Duration::minutes(3)).await;
    assert_eq!(
        (
            loaded.minutes("QQQ", Session::Off, now + chrono::Duration::minutes(3)),
            broken
        ),
        (3, 0)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn feeds_parse_snapshots_deltas_and_errors() {
    let snap = r#"{"channel":"order_book:26","order_book":{"code":0,"asks":[{"price":"780.58","size":"0.1252"}],"bids":[{"price":"780.50","size":"2"}],"offset":1,"nonce":10,"begin_nonce":9},"type":"subscribed/order_book"}"#;
    assert_eq!(
        feed::parse_lighter(snap).unwrap(),
        feed::Event::Snapshot {
            market: "26".into(),
            bids: vec![(dec!(780.50), dec!(2))],
            asks: vec![(dec!(780.58), dec!(0.1252))],
            nonce: Some(10)
        }
    );
    let delta = r#"{"channel":"order_book:26","order_book":{"code":0,"asks":[{"price":"780.58","size":"0.0000"}],"bids":[],"nonce":12,"begin_nonce":10},"type":"update/order_book"}"#;
    let feed::Event::Delta {
        begin_nonce,
        nonce,
        asks,
        ..
    } = feed::parse_lighter(delta).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        (begin_nonce, nonce, asks[0].1),
        (Some(10), Some(12), Decimal::ZERO)
    );
    assert!(matches!(
        feed::parse_lighter(r#"{"error":{"code":30005,"message":"Invalid Channel"}}"#).unwrap(),
        feed::Event::Error(_)
    ));
    assert_eq!(
        feed::parse_lighter(r#"{"type":"pong"}"#).unwrap(),
        feed::Event::Other
    );
    assert!(feed::parse_lighter(r#"{"channel":"order_book:26","order_book":{"asks":[{"price":"x","size":"1"}],"bids":[]},"type":"update/order_book"}"#).is_err());

    let arcus = r#"{"type":"channel_data","channel":"l2Orderbook","id":"SPY-USD","contents":{"bids":[["779.35","33.1"]],"asks":[["779.36","83.1"]],"lastSequenceId":1}}"#;
    assert_eq!(
        feed::parse_arcus(arcus).unwrap(),
        feed::Event::Snapshot {
            market: "SPY-USD".into(),
            bids: vec![(dec!(779.35), dec!(33.1))],
            asks: vec![(dec!(779.36), dec!(83.1))],
            nonce: None
        }
    );
    assert_eq!(
        feed::parse_arcus(
            r#"{"type":"channel_data","channel":"bbo","id":"SPY-USD","contents":{}}"#
        )
        .unwrap(),
        feed::Event::Other
    );
    let feed::Event::Error(message) = feed::parse_arcus(&format!(
        r#"{{"type":"error","message":"Invalid market {}"}}"#,
        "X".repeat(500)
    ))
    .unwrap() else {
        panic!()
    };
    assert!(message.chars().count() <= 160);
}

#[test]
fn lighter_nonce_gap_drops_the_book_and_asks_for_a_resubscribe() {
    let mut books = HashMap::new();
    let (tx, mut rx) = mpsc::channel(4);
    let lighter = Venue::LighterRh;
    let key = (lighter, "26".to_string());
    apply(
        &mut books,
        lighter,
        feed::Event::Snapshot {
            market: "26".into(),
            bids: vec![(dec!(1), dec!(1))],
            asks: vec![(dec!(2), dec!(1))],
            nonce: Some(5),
        },
        Some(&tx),
    );
    apply(
        &mut books,
        lighter,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![(dec!(1.5), dec!(1))],
            asks: vec![],
            begin_nonce: Some(5),
            nonce: Some(7),
        },
        Some(&tx),
    );
    assert_eq!(books[&key].best_bid(), Some(dec!(1.5)));
    // 别家同名的市场是另一本盘口。
    apply(
        &mut books,
        Venue::Arcus,
        feed::Event::Snapshot {
            market: "26".into(),
            bids: vec![(dec!(9), dec!(1))],
            asks: vec![(dec!(10), dec!(1))],
            nonce: None,
        },
        None,
    );
    assert_eq!(books[&key].best_bid(), Some(dec!(1.5)));
    apply(
        &mut books,
        lighter,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![],
            asks: vec![],
            begin_nonce: Some(9),
            nonce: Some(10),
        },
        Some(&tx),
    );
    assert!(!books.contains_key(&key), "丢了增量的盘口不能再用");
    assert_eq!(rx.try_recv().unwrap(), "26");
    // 没有快照之前的增量忽略。
    apply(
        &mut books,
        lighter,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![(dec!(1), dec!(1))],
            asks: vec![],
            begin_nonce: Some(10),
            nonce: Some(11),
        },
        Some(&tx),
    );
    assert!(!books.contains_key(&key));
}

#[test]
fn catalogs_keep_fees_honest_and_pairs_need_a_verified_identity() {
    let arcus = serde_json::json!({"markets": [
        {"marketDisplayName": "SPY-USD", "baseAsset": "SPY", "status": "ONLINE", "type": "PERPETUAL", "category": "INDICES", "isOutsideRth": true, "lowerTradingBound": "631.5", "upperTradingBound": "698.5"},
        {"marketDisplayName": "BTC-USD", "baseAsset": "BTC", "status": "ONLINE", "type": "PERPETUAL", "category": "CRYPTO", "isOutsideRth": null},
        {"marketDisplayName": "F-USD", "baseAsset": "F", "status": "OFFLINE", "type": "PERPETUAL", "category": "EQUITIES"},
        {"marketDisplayName": "XBT-USD", "baseAsset": "QQQ", "status": "ONLINE", "type": "PERPETUAL", "category": "INDICES"},
        {"marketDisplayName": "QNT-USD", "baseAsset": "QNT", "status": "ONLINE", "type": "PERPETUAL", "category": "EQUITIES"}
    ]});
    let lighter = serde_json::json!({"order_book_details": [
        {"symbol": "SPY", "market_id": 26, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "BTC", "market_id": 1, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "F", "market_id": 3, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "QQQ", "market_id": 4, "status": "active", "market_type": "perp"}
    ]});
    let xyz = serde_json::json!({"universe": [
        {"name": "xyz:SPY", "deployerFeeScale": "1.0", "growthMode": "enabled"},
        {"name": "xyz:QNT", "deployerFeeScale": "1.0", "growthMode": "enabled"},
        {"name": "xyz:GOLD", "deployerFeeScale": "1.0"},
        {"name": "xyz:OLD", "deployerFeeScale": "1.0", "isDelisted": true},
        {"name": "io:ANTH", "deployerFeeScale": "1.0"}
    ]});
    let io = serde_json::json!({"universe": [{"name": "io:ANTH", "deployerFeeScale": "1.0", "growthMode": "enabled"}]});
    let mut catalog = Catalog::new();
    catalog.insert(
        Venue::Arcus,
        catalog::arcus(&arcus, dec!(0.000225)).unwrap(),
    );
    catalog.insert(
        Venue::LighterRh,
        catalog::lighter(Venue::LighterRh, &lighter).unwrap(),
    );
    catalog.insert(
        Venue::HyperliquidXyz,
        catalog::hyperliquid(Venue::HyperliquidXyz, &xyz, Decimal::TWO).unwrap(),
    );
    catalog.insert(
        Venue::HyperliquidIo,
        catalog::hyperliquid(Venue::HyperliquidIo, &io, Decimal::TWO).unwrap(),
    );
    assert!(catalog::arcus(&arcus, dec!(0.5)).is_err());

    // 最早那组：按 base 对齐，不需要扫描。Lighter 没报费率的 QQQ 不知道往返费，不当 0。
    let rh = pairs::pair_markets(Pair::RH, &catalog, None, false);
    assert_eq!(
        rh.iter().map(|m| m.base.as_str()).collect::<Vec<_>>(),
        ["BTC", "QQQ", "SPY"]
            .iter()
            .filter(|b| **b != "QQQ")
            .copied()
            .collect::<Vec<_>>(),
        "XBT-USD 名不符的 QQQ 不配"
    );
    let spy = rh.iter().find(|m| m.base == "SPY").unwrap();
    assert_eq!(spy.round_trip_pct(), Some(dec!(0.045)));
    assert_eq!((spy.a.key.as_str(), spy.b.key.as_str()), ("SPY-USD", "26"));
    assert_eq!(spy.outside_rth, Some(true));
    assert_eq!(
        spy.a.bounds,
        Some(arb_venues::arcus::TradingBounds {
            lower: dec!(631.5),
            upper: dec!(698.5)
        }),
        "Arcus 的成交价限制带"
    );
    let btc = rh.iter().find(|m| m.base == "BTC").unwrap();
    assert_eq!(btc.a.bounds, None, "没报带就不知道，不据此挡");
    assert_eq!(btc.b.bounds, None);
    assert_eq!(
        pairs::pair_markets(Pair::RH, &catalog, None, true).len(),
        1,
        "只看股票类"
    );

    // HL-xyz 的组：没有扫描就不配；身份簇不同（QNT 股票 vs 币）不配；同簇才配。
    let xyz_rh = Pair::parse("hyperliquid-xyz:lighter-rh").unwrap();
    assert!(pairs::pair_markets(xyz_rh, &catalog, None, false).is_empty());
    let arcus_xyz = Pair::parse("arcus:hyperliquid-xyz").unwrap();
    let mut identity = pairs::Identity::new();
    identity.insert((Venue::Arcus, "SPY".into()), 1);
    identity.insert((Venue::HyperliquidXyz, "SPY".into()), 1);
    identity.insert((Venue::Arcus, "QNT".into()), 2);
    identity.insert((Venue::HyperliquidXyz, "QNT".into()), 3);
    let found = pairs::pair_markets(arcus_xyz, &catalog, Some(&identity), false);
    assert_eq!(
        found.iter().map(|m| m.base.as_str()).collect::<Vec<_>>(),
        ["SPY"]
    );
    // growth mode：xyz 单边 0.045% × 2 × 0.1 = 0.009%，Arcus 0.0225% → 往返 (0.009 + 0.0225) × 2 = 0.063%；类别取 Arcus 的。
    assert_eq!(found[0].round_trip_pct(), Some(dec!(0.063)));
    assert_eq!(found[0].category, "INDICES");
    assert_eq!(found[0].b.key, "xyz:SPY");
    // xyz 的 catalog：别名 GOLD→XAU、下架的跳过、别的 dex 的币名不收。
    let x = &catalog[&Venue::HyperliquidXyz];
    assert!(x.contains_key("XAU") && !x.contains_key("OLD") && !x.contains_key("ANTHROPIC"));
    assert_eq!(
        x["XAU"].taker_fee,
        Some(dec!(0.0009)),
        "没开 growth：×2 不打折"
    );
    // io：Tier 4 返佣 → 0（不是负数）。
    assert_eq!(
        catalog[&Venue::HyperliquidIo]["ANTHROPIC"].taker_fee,
        Some(Decimal::ZERO)
    );

    // 组的解析：不支持的场所、同一场所、重复（反过来写也算）都报错。
    assert!(Pair::parse("okx:arcus").is_err());
    assert!(Pair::parse("arcus:arcus").is_err());
    assert!(Pair::parse_list("arcus:lighter-rh,lighter-rh:arcus").is_err());
    assert_eq!(Pair::parse_list(pairs::DEFAULT_PAIRS).unwrap()[0], Pair::RH);
    // 配了 API 的场所两两组合；a / b 顺序固定，已有历史的组方向不变；不支持的场所（okx）忽略；
    // Hyperliquid 主 dex 手续费太高，不自动组合（xyz / io 照常）；Binance 和 Bitget / MEXC 一样与所有家配对、
    // 是普通交易组（不再只观察），挂单测算照常算。
    let all = pairs::all_pairs(&[
        Venue::LighterRh,
        Venue::Binance,
        Venue::HyperliquidIo,
        Venue::Arcus,
        Venue::Hyperliquid,
        Venue::HyperliquidXyz,
        Venue::Okx,
    ]);
    assert_eq!(all.len(), 10);
    for (a, b) in [
        (Venue::Arcus, Venue::LighterRh),
        (Venue::HyperliquidXyz, Venue::LighterRh),
        (Venue::Arcus, Venue::HyperliquidXyz),
        (Venue::HyperliquidIo, Venue::LighterRh),
        (Venue::Arcus, Venue::HyperliquidIo),
        (Venue::HyperliquidXyz, Venue::HyperliquidIo),
        (Venue::Arcus, Venue::Binance),
        (Venue::LighterRh, Venue::Binance),
        (Venue::HyperliquidXyz, Venue::Binance),
        (Venue::HyperliquidIo, Venue::Binance),
    ] {
        assert!(all.contains(&Pair { a, b }), "{a}:{b}");
    }
    for pair in &all {
        let binance = pair.a == Venue::Binance || pair.b == Venue::Binance;
        assert!(!pair.observe_only(), "{}：币安组也是普通交易组", pair.id());
        assert_eq!(pair.maker_venue(), binance.then_some(Venue::Binance));
        if binance {
            assert!(pair.needs_identity_check(), "Binance 的同名合约要核对身份");
        }
    }
    assert!(
        pairs::all_pairs(&[Venue::Binance, Venue::Lighter, Venue::Bitget, Venue::Mexc])
            .iter()
            .filter(|pair| pair.maker_venue().is_some())
            .count()
            == 3,
        "币安和 Lighter / Bitget / MEXC 也配（Bitget × MEXC 照旧不配）"
    );
    assert!(
        all.iter()
            .all(|pair| pair.a != Venue::Hyperliquid && pair.b != Venue::Hyperliquid)
    );
    assert_eq!(
        pairs::all_pairs(&[Venue::Arcus, Venue::Binance]),
        [Pair {
            a: Venue::Arcus,
            b: Venue::Binance
        }]
    );
    assert!(pairs::all_pairs(&[Venue::Arcus, Venue::Hyperliquid]).is_empty());
    // 显式指定时仍可监控主 dex（用户自己的选择）。
    assert_eq!(
        Pair::parse_list("arcus:hyperliquid").unwrap(),
        [Pair {
            a: Venue::Arcus,
            b: Venue::Hyperliquid
        }]
    );
}

#[test]
fn hyperliquid_feed_parses_full_snapshots() {
    let text = r#"{"channel":"l2Book","data":{"coin":"xyz:NVDA","time":1791514562612,"levels":[[{"px":"232.16","sz":"14.29","n":2}],[{"px":"232.17","sz":"65.506","n":3}]]}}"#;
    assert_eq!(
        feed::parse_hyperliquid(text).unwrap(),
        feed::Event::Snapshot {
            market: "xyz:NVDA".into(),
            bids: vec![(dec!(232.16), dec!(14.29))],
            asks: vec![(dec!(232.17), dec!(65.506))],
            nonce: None
        }
    );
    assert_eq!(
        feed::parse_hyperliquid(r#"{"channel":"subscriptionResponse","data":{}}"#).unwrap(),
        feed::Event::Other
    );
    assert!(matches!(
        feed::parse_hyperliquid(r#"{"channel":"error","data":"Invalid subscription"}"#).unwrap(),
        feed::Event::Error(_)
    ));
    assert!(
        feed::parse_hyperliquid(r#"{"channel":"l2Book","data":{"coin":"x","levels":[[]]}}"#)
            .is_err()
    );
    // 默认只订 bbo（不用 l2Book：东京实测约 12 秒才推一份，太慢）；`ARB_HL_BOOK=l2` 才订 l2Book。
    assert_eq!(
        feed::hyperliquid_subscribe("io:ANTH"),
        r#"{"method":"subscribe","subscription":{"coin":"io:ANTH","type":"bbo"}}"#
    );
    assert_eq!(
        feed::hyperliquid_l2_subscribe("io:ANTH"),
        r#"{"method":"subscribe","subscription":{"coin":"io:ANTH","type":"l2Book"}}"#
    );
    // bbo 订阅不会立刻推当前值：订阅之后用一次 WebSocket post 取初值。
    let seed: serde_json::Value =
        serde_json::from_str(&feed::hyperliquid_seed_request("io:ANTH", 3)).unwrap();
    assert_eq!(seed["method"], "post");
    assert_eq!(seed["id"], 1_000_003);
    assert_eq!(seed["request"]["type"], "info");
    assert_eq!(seed["request"]["payload"]["type"], "l2Book");
    assert_eq!(seed["request"]["payload"]["coin"], "io:ANTH");
    // 原文里认出市场名（不解析整条 JSON）。
    assert_eq!(
        feed::hyperliquid_market_hint(r#"{"channel":"bbo","data":{"coin":"xyz:TSLA","time":1}}"#),
        Some("xyz:TSLA")
    );
    assert_eq!(feed::hyperliquid_market_hint(r#"{"channel":"pong"}"#), None);
}

#[test]
fn snapshot_books_go_stale_per_venue_not_after_a_flat_fifteen_seconds() {
    let now = Instant::now();
    let pair = Pair::parse("arcus:hyperliquid-xyz").unwrap();
    let mut m = market("INDICES");
    m.b.taker_fee = Some(dec!(0.00009));
    let fresh = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let at = |age: u64| {
        let mut old = book(
            &[(dec!(100.20), dec!(100))],
            &[(dec!(100.22), dec!(100))],
            now,
        );
        old.updated = now - Duration::from_secs(age);
        old
    };
    let eval = |b: &LocalBook| {
        evaluate(
            pair,
            &m,
            Some(&fresh),
            Some(b),
            Some(normal(-0.05)),
            500,
            Session::Off,
            &config(),
            now,
        )
    };
    // Hyperliquid 默认只订 bbo、只推变化：安静的市场几十秒没有推送是常态，不按年龄判旧
    // （新鲜度靠连接心跳，断线即清空盘口）。
    for age in [4, 6, 60] {
        let line = eval(&at(age));
        assert!(line.note.is_none(), "{age} 秒：{:?}", line.note);
        assert!(line.best.is_some(), "{age} 秒");
    }
    // Arcus 没变也推整本快照：6 秒没更新就不算，说清楚是哪家；4 秒还算。
    let eval_arcus_old = |b: &LocalBook| {
        evaluate(
            pair,
            &m,
            Some(b),
            Some(&fresh),
            Some(normal(-0.05)),
            500,
            Session::Off,
            &config(),
            now,
        )
    };
    let line = eval_arcus_old(&at(7));
    assert!(line.best.is_none());
    assert!(line.note.unwrap().contains("arcus"));
    let line = eval_arcus_old(&at(4));
    assert!(line.note.is_none(), "{:?}", line.note);
    assert_eq!(line.age_sec, Some(4));
    assert_eq!(book_stale_after(Venue::Arcus), Duration::from_secs(6));
    assert_eq!(
        book_stale_after(Venue::HyperliquidIo),
        Duration::from_secs(5)
    );
}

#[test]
fn only_snapshot_feeds_go_stale_on_their_age_and_the_note_names_the_venue() {
    let now = Instant::now();
    let pair = Pair::parse("arcus:hyperliquid-xyz").unwrap();
    let mut m = market("INDICES");
    m.b.taker_fee = Some(dec!(0.00009));
    let fresh = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let mut old = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    old.updated = now - Duration::from_secs(20);
    // Hyperliquid 默认只订 bbo（不用 l2Book）：和 Lighter 一样只推变化，安静的市场盘口旧不算过期。
    let line = evaluate(
        pair,
        &m,
        Some(&fresh),
        Some(&old),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &config(),
        now,
    );
    assert!(line.best.is_some(), "bbo 只推变化：HL 盘口旧不算过期");
    // Arcus 没变也推整本快照：它的盘口旧才是过期，而且说清楚是哪家。
    let line = evaluate(
        pair,
        &m,
        Some(&old),
        Some(&fresh),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &config(),
        now,
    );
    assert!(line.best.is_none());
    assert!(line.note.unwrap().contains("arcus"), "说清楚是哪家过期");
    // Lighter RH 只推变化：它的盘口旧不算过期。
    let line = evaluate(
        Pair::RH,
        &market("INDICES"),
        Some(&fresh),
        Some(&old),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &config(),
        now,
    );
    assert!(line.best.is_some());
    assert_eq!(
        (line.a, line.b, line.pair.as_str()),
        (Venue::Arcus, Venue::LighterRh, "arcus:lighter-rh")
    );
    let (long, short, _) = line.legs(line.best.as_ref().unwrap().direction).unwrap();
    assert_eq!((long, short), (Venue::Arcus, Venue::LighterRh));
}

struct Recorder(std::sync::Mutex<Vec<String>>);

impl crate::alert::Sink for Recorder {
    fn send(
        &self,
        text: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        self.0.lock().unwrap().push(text);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn alerts_need_a_held_signal_and_never_exceed_their_own_rate_limit() {
    let sink = Arc::new(Recorder(std::sync::Mutex::new(Vec::new())));
    let alerter =
        crate::alert::Alerter::with_sink(Some(sink.clone() as Arc<dyn crate::alert::Sink>));
    let monitor = Monitor::new(config(), alerter);
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let mut line = evaluate(
        Pair::RH,
        &market("INDICES"),
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &config(),
        now,
    );
    assert!(line.best.as_ref().unwrap().signal);
    monitor.maybe_alert(&line, &config());
    tokio::task::yield_now().await;
    assert!(sink.0.lock().unwrap().is_empty(), "刚亮的信号不推");
    line.best.as_mut().unwrap().signal_sec = SIGNAL_HOLD.as_secs();
    // 只观察的组（Binance）保持多久都不推。
    let observed = Line {
        observe_only: true,
        ..line.clone()
    };
    monitor.maybe_alert(&observed, &config());
    tokio::task::yield_now().await;
    assert!(sink.0.lock().unwrap().is_empty(), "只观察的组不推");
    // 观察组（没有实盘账户的场所）同样不推。
    let watched = Line {
        watch_only: true,
        ..line.clone()
    };
    monitor.maybe_alert(&watched, &config());
    tokio::task::yield_now().await;
    assert!(sink.0.lock().unwrap().is_empty(), "观察组不推");
    for base in ["SPY", "QQQ", "NVDA", "AMZN"] {
        line.base = base.into();
        monitor.maybe_alert(&line, &config());
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        sink.0.lock().unwrap().len(),
        ALERTS_PER_MINUTE,
        "价差提醒每分钟不超过自己的上限"
    );
}

type Keyed = std::sync::Mutex<Vec<(String, crate::alert::Keyboard)>>;

struct KeyRecorder(Arc<Keyed>);

impl crate::alert::Sink for KeyRecorder {
    fn send(
        &self,
        text: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        self.send_with_buttons(text, Vec::new())
    }
    fn send_with_buttons(
        &self,
        text: String,
        keyboard: crate::alert::Keyboard,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        self.0.lock().unwrap().push((text, keyboard));
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn alerts_to_the_admin_carry_an_order_button_whose_ticket_names_the_line() {
    let sent = Arc::new(Keyed::default());
    let alerter = crate::alert::Alerter::with_sink(Some(
        Arc::new(KeyRecorder(Arc::clone(&sent))) as Arc<dyn crate::alert::Sink>
    ));
    let monitor = Monitor::new(config(), Arc::clone(&alerter));
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let mut line = evaluate(
        Pair::RH,
        &market("INDICES"),
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &config(),
        now,
    );
    line.best.as_mut().unwrap().signal_sec = SIGNAL_HOLD.as_secs();
    // 没开启（告警不是发到管理员私聊）：只有文字。
    line.base = "SPY".into();
    monitor.maybe_alert(&line, &config());
    alerter.enable_actions();
    line.base = "QQQ".into();
    monitor.maybe_alert(&line, &config());
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].1.is_empty());
    let (label, data) = &sent[1].1[0][0];
    assert_eq!(label, "🛒 下单…");
    assert!(data.len() <= 64, "callback_data 最多 64 字节");
    let ticket = data.strip_prefix("o:").expect("下单按钮的回调以 o: 开头");
    assert_eq!(
        monitor.order_ticket(ticket),
        Some(crate::telegram::order::Intent {
            pair: Pair::RH.id(),
            base: "QQQ".into(),
            direction: line.best.as_ref().unwrap().direction,
        })
    );
    assert_eq!(monitor.order_ticket("000000000000"), None);
}

// ───────────────────────────── Binance：只观察与挂单测算 ─────────────────────────────

#[test]
fn binance_catalog_takes_trading_usdt_perpetuals_with_their_fee_tier() {
    let row = |symbol: &str,
               base: &str,
               quote: &str,
               kind: &str,
               status: &str,
               underlying: &str| {
        serde_json::json!({"symbol": symbol, "baseAsset": base, "quoteAsset": quote, "marginAsset": quote,
            "contractType": kind, "status": status, "underlyingType": underlying})
    };
    let info = serde_json::json!({"symbols": [
        row("NVDAUSDT", "NVDA", "USDT", "TRADIFI_PERPETUAL", "TRADING", "EQUITY"),
        row("XAUUSDT", "XAU", "USDT", "TRADIFI_PERPETUAL", "TRADING", "COMMODITY"),
        row("BTCUSDT", "BTC", "USDT", "PERPETUAL", "TRADING", "COIN"),
        row("BTCUSDT_261225", "BTC", "USDT", "CURRENT_QUARTER", "TRADING", "COIN"),
        row("OLDUSDT", "OLD", "USDT", "PERPETUAL", "SETTLING", "COIN"),
        row("ETHUSDC", "ETH", "USDC", "PERPETUAL", "TRADING", "COIN"),
    ]});
    let markets = catalog::binance(&info, catalog::BinanceFees::default()).unwrap();
    assert_eq!(markets.keys().collect::<Vec<_>>(), ["BTC", "NVDA", "XAU"]);
    let nvda = &markets["NVDA"];
    assert_eq!(nvda.key, "NVDAUSDT");
    assert_eq!(
        (nvda.taker_fee, nvda.maker_fee),
        (Some(dec!(0.0004)), Some(Decimal::ZERO))
    );
    assert_eq!(nvda.category.as_deref(), Some("EQUITIES"));
    assert_eq!(markets["XAU"].category.as_deref(), Some("COMMODITIES"));
    let btc = &markets["BTC"];
    assert_eq!(
        (btc.taker_fee, btc.maker_fee),
        (Some(dec!(0.0005)), Some(dec!(0.0002)))
    );
    assert_eq!(btc.category.as_deref(), Some("CRYPTO"));
    assert!(catalog::binance(&serde_json::json!({"symbols": []}), Default::default()).is_err());
}

#[test]
fn binance_feed_parses_partial_books_and_trades_and_batches_subscriptions() {
    let depth = r#"{"e":"depthUpdate","E":1,"T":1,"s":"NVDAUSDT","ps":"NVDAUSDT","U":1,"u":42,"pu":0,"b":[["231.10000","3.74"],["231.09000","21.87"]],"a":[["231.11000","1.5"]]}"#;
    assert_eq!(
        feed::parse_binance(depth).unwrap(),
        feed::Event::Snapshot {
            market: "NVDAUSDT".into(),
            bids: vec![
                (dec!(231.10000), dec!(3.74)),
                (dec!(231.09000), dec!(21.87))
            ],
            asks: vec![(dec!(231.11000), dec!(1.5))],
            nonce: Some(42),
        }
    );
    let trade = r#"{"e":"aggTrade","E":1791730459461,"a":11839323,"s":"NVDAUSDT","p":"231.10000","q":"0.07","nq":"0.07","f":1,"l":1,"T":1791730459310,"m":true,"st":1}"#;
    assert_eq!(
        feed::parse_binance(trade).unwrap(),
        feed::Event::Trade {
            market: "NVDAUSDT".into(),
            price: dec!(231.10000),
            quantity: dec!(0.07),
            seller_initiated: true,
            time_ms: 1791730459310,
        }
    );
    assert_eq!(
        feed::parse_binance(r#"{"result":null,"id":1}"#).unwrap(),
        feed::Event::Other
    );
    assert!(matches!(
        feed::parse_binance(r#"{"error":{"code":2,"msg":"Invalid request"},"id":1}"#).unwrap(),
        feed::Event::Error(_)
    ));
    // 每秒最多 10 条客户端消息：几十个流合成一条，每条最多 50 个。
    let keys: Vec<String> = (0..120).map(|i| format!("SYM{i}USDT")).collect();
    let messages = feed::binance_depth_subscribe(&keys);
    assert_eq!(messages.len(), 3);
    let first: Value = serde_json::from_str(&messages[0]).unwrap();
    assert_eq!(first["method"], "SUBSCRIBE");
    assert_eq!(first["params"].as_array().unwrap().len(), 50);
    assert_eq!(first["params"][0], "sym0usdt@depth20@100ms");
    let trades: Value =
        serde_json::from_str(&feed::binance_trade_subscribe(&keys[..2])[0]).unwrap();
    assert_eq!(trades["params"][1], "sym1usdt@aggTrade");
    // 盘口与成交走不同的路径：连错路径不报错、只是收不到。
    assert!(feed::BINANCE_BOOK_WS.ends_with("/public/ws"));
    assert!(feed::BINANCE_TRADE_WS.ends_with("/market/ws"));
    assert!(!snapshot_feed(Venue::Binance), "只推变化：新鲜度靠连接");
}

// ───────────────────────────── Aster：只观察 ─────────────────────────────

#[test]
fn aster_catalog_takes_trading_usdt_perpetuals_and_classifies_by_sub_type() {
    // 2026-11-15 `exchangeInfo` 的真实片段（字段截取）：`underlyingType` 全是 某合约，类别在 `underlyingSubType`。
    let row = |symbol: &str,
               quote: &str,
               kind: &str,
               status: &str,
               sub: &[&str],
               symbol_type: i64| {
        serde_json::json!({"symbol": symbol, "pair": symbol, "contractType": kind, "status": status,
            "quoteAsset": quote, "marginAsset": quote, "underlyingType": "COIN",
            "underlyingSubType": sub, "symbolType": symbol_type})
    };
    let info = serde_json::json!({"symbols": [
        row("NVDAUSDT", "USDT", "PERPETUAL", "TRADING", &["STOCK", "Semiconductor"], 1),
        row("QQQUSDT", "USDT", "PERPETUAL", "TRADING", &["ETF"], 1),
        row("XAUUSDT", "USDT", "PERPETUAL", "TRADING", &["Commodities"], 1),
        row("ANTHROPICUSDT", "USDT", "PERPETUAL", "TRADING", &["pre-launch", "STOCK"], 1),
        row("NEWUSDT", "USDT", "PERPETUAL", "TRADING", &[], 1),
        row("BTCUSDT", "USDT", "PERPETUAL", "TRADING", &["Top"], 0),
        row("XAUUSD1", "USD1", "PERPETUAL", "TRADING", &["USD1-RWA"], 1),
        row("TONUSDT", "USDT", "PERPETUAL", "SETTLING", &[], 0),
        row("MBLUSDT", "USDT", "", "PENDING_TRADING", &[], 0),
        row("BTCUSDT_261225", "USDT", "CURRENT_QUARTER", "TRADING", &[], 0),
    ]});
    let markets = catalog::aster(&info).unwrap();
    assert_eq!(
        markets.keys().collect::<Vec<_>>(),
        ["ANTHROPIC", "BTC", "NEW", "NVDA", "QQQ", "XAU"]
    );
    let nvda = &markets["NVDA"];
    assert_eq!(nvda.key, "NVDAUSDT");
    assert_eq!(nvda.category.as_deref(), Some("EQUITIES"));
    // 股票 / ETF / 商品类吃单 1.25 bps；盘口量本来就是标的数量。
    assert_eq!(catalog::ASTER_RWA_TAKER_FEE, dec!(0.000125));
    assert_eq!(nvda.taker_fee, Some(dec!(0.000125)));
    assert_eq!((nvda.maker_fee, nvda.size_multiplier), (None, None));
    assert_eq!(markets["QQQ"].category.as_deref(), Some("EQUITIES"));
    assert_eq!(markets["XAU"].category.as_deref(), Some("COMMODITIES"));
    assert_eq!(markets["XAU"].taker_fee, Some(dec!(0.000125)));
    assert_eq!(
        markets["NEW"].category.as_deref(),
        Some("EQUITIES"),
        "没有子类时靠 symbolType == 1 兜底"
    );
    // 加密币分两档费率、接口里认不出来：不给费率，这一行不出净收益。
    let btc = &markets["BTC"];
    assert_eq!(btc.category.as_deref(), Some("CRYPTO"));
    assert_eq!(btc.taker_fee, None);
    assert!(catalog::aster(&serde_json::json!({"symbols": []})).is_err());
    assert_eq!(
        crate::preipo::aster_flagged(&info)
            .into_iter()
            .collect::<Vec<_>>(),
        ["ANTHROPIC"]
    );
}

#[test]
fn aster_feed_reuses_the_binance_format_and_caps_streams_per_connection() {
    // 2026-11-15 实测的一条（档位截短）。
    let depth = r#"{"e":"depthUpdate","E":1794674596382,"T":1794674596350,"s":"NVDAUSDT","U":654032659570,"u":654032660974,"pu":654032657350,"b":[["159.910000","3.22"],["159.900000","5.63"]],"a":[["159.940000","5.6"]]}"#;
    assert_eq!(
        feed::parse_binance(depth).unwrap(),
        feed::Event::Snapshot {
            market: "NVDAUSDT".into(),
            bids: vec![(dec!(159.91), dec!(3.22)), (dec!(159.90), dec!(5.63))],
            asks: vec![(dec!(159.94), dec!(5.6))],
            nonce: Some(654032660974),
        }
    );
    assert_eq!(feed::aster_market(depth), Some("NVDAUSDT"));
    assert_eq!(feed::aster_market(r#"{"id":1,"result":null}"#), None);
    assert_eq!(
        feed::parse_binance(r#"{"id":1,"result":null}"#).unwrap(),
        feed::Event::Other
    );
    let keys: Vec<String> = (0..120).map(|i| format!("SYM{i}USDT")).collect();
    let messages = feed::aster_depth_subscribe(&keys);
    assert_eq!(messages.len(), 3);
    let first: Value = serde_json::from_str(&messages[0]).unwrap();
    assert_eq!(first["method"], "SUBSCRIBE");
    assert_eq!(first["params"][0], "sym0usdt@depth20@100ms");
    // 一条连接最多 200 个流，超了整条连接被关：多出来的不订。
    let keys: Vec<String> = (0..260).map(|i| format!("SYM{i}USDT")).collect();
    let streams: usize = feed::aster_depth_subscribe(&keys)
        .iter()
        .map(|text| {
            serde_json::from_str::<Value>(text).unwrap()["params"]
                .as_array()
                .unwrap()
                .len()
        })
        .sum();
    assert_eq!(streams, feed::ASTER_STREAMS_PER_CONNECTION);
    let aster = protocol(Venue::Aster, Stream::Books).unwrap();
    assert_eq!(aster.url, feed::ASTER_WS);
    assert!(
        aster.coalesce.is_some(),
        "观察场所：同一市场 500ms 内只解析最后一条"
    );
    assert!(protocol(Venue::Aster, Stream::Trades).is_none());
    assert!(!snapshot_feed(Venue::Aster), "只推变化：新鲜度靠连接");
    assert_eq!(venue_label(Venue::Aster), "Aster");
}

#[test]
fn aster_is_watched_by_default_and_never_auto_paired_for_trading() {
    assert!(pairs::SUPPORTED.contains(&Venue::Aster));
    assert!(pairs::DEFAULT_WATCH.contains(&Venue::Aster));
    let trading = pairs::all_pairs(&[Venue::Arcus, Venue::LighterRh, Venue::HyperliquidXyz]);
    let watch = pairs::watch_pairs(&trading, &[Venue::Aster]);
    // 老场所当 a（基差方向不变），Aster 排在最后当 b。
    assert_eq!(
        watch.iter().map(|p| p.id()).collect::<Vec<_>>(),
        ["arcus:aster", "lighter-rh:aster", "hyperliquid-xyz:aster"]
    );
    // 有实盘凭据也不自动配成交易组（下单模块还没和价差自动交易一起验证过）。
    let with_account = pairs::all_pairs(&[Venue::Arcus, Venue::LighterRh, Venue::Aster]);
    assert!(
        with_account
            .iter()
            .all(|p| p.a != Venue::Aster && p.b != Venue::Aster)
    );
    assert_eq!(
        Pair::parse("hyperliquid-xyz:aster").unwrap().id(),
        "hyperliquid-xyz:aster"
    );
}

/// Lighter RH（a，吃单 0）↔ Binance（b，TradFi 吃单 0.04%、挂单 0）的 NVDA。
fn binance_market() -> PairMarket {
    let venue_market = |key: &str, taker, maker| pairs::VenueMarket {
        key: key.into(),
        taker_fee: Some(taker),
        maker_fee: maker,
        category: None,
        outside_rth: None,
        bounds: None,
        size_multiplier: None,
    };
    PairMarket {
        base: "NVDA".into(),
        a: venue_market("26", Decimal::ZERO, None),
        b: venue_market("NVDAUSDT", dec!(0.0004), Some(Decimal::ZERO)),
        category: "EQUITIES".into(),
        outside_rth: None,
    }
}

const BINANCE_PAIR: Pair = Pair {
    a: Venue::LighterRh,
    b: Venue::Binance,
};

/// Lighter 100.00 / 100.02（中间价 100.01）、Binance 99.98 / 99.99（99.985）：均值 99.9975。
fn binance_books(at: Instant) -> (LocalBook, LocalBook) {
    (
        book(
            &[(dec!(100.00), dec!(100))],
            &[(dec!(100.02), dec!(100))],
            at,
        ),
        book(&[(dec!(99.98), dec!(100))], &[(dec!(99.99), dec!(100))], at),
    )
}

#[test]
fn the_maker_estimate_quotes_binance_at_its_own_best_price_and_hedges_by_depth() {
    let (lighter, binance) = binance_books(Instant::now());
    let view = maker::evaluate(
        BINANCE_PAIR,
        &binance_market(),
        &lighter,
        &binance,
        Some(dec!(-0.06)),
        dec!(1000),
    )
    .unwrap();
    assert_eq!(view.venue, Venue::Binance);
    // 挂单开 0 + 吃单平 0.04% + Lighter 吃单两次 0；挂单退出则全程 0。
    assert_eq!(
        (view.fee_taker_exit_pct, view.fee_maker_exit_pct),
        (dec!(0.04), Decimal::ZERO)
    );
    // long_b = 多 Binance：挂买一 99.98，卖在 Lighter 买盘 100.00。
    let long_b = view.long_b.unwrap();
    assert_eq!(long_b.price, dec!(99.98));
    assert_eq!(long_b.entry_pct, dec!(0.02000));
    assert_eq!(long_b.exit_cross_taker_pct, dec!(0.01500));
    assert_eq!(long_b.exit_cross_maker_pct, dec!(0.00500));
    // 正常基差 a − b = −0.06%：多 b 回到正常时多收回 0.06。
    assert_eq!(long_b.net_to_normal_taker_exit_pct, Some(dec!(0.02500)));
    assert_eq!(long_b.net_to_normal_maker_exit_pct, Some(dec!(0.07500)));
    // long_a = 空 Binance：挂卖一 99.99，买在 Lighter 卖盘 100.02。
    let long_a = view.long_a.unwrap();
    assert_eq!(long_a.price, dec!(99.99));
    assert_eq!(long_a.entry_pct, dec!(-0.03000));
    assert_eq!(long_a.net_to_normal_taker_exit_pct, Some(dec!(-0.14500)));
    // 没有挂单费率（不是挂单场所）就不出测算。
    assert!(
        maker::evaluate(
            Pair::RH,
            &market("EQUITIES"),
            &lighter,
            &binance,
            None,
            dec!(1000)
        )
        .is_none()
    );
}

/// 2026-11-01 起币安组和 Bitget / MEXC 一样是普通交易组：不再只观察（照常提醒、按通用规则自动交易），
/// 挂单测算照常算（只是参考）。
#[test]
fn binance_lines_are_ordinary_trading_lines_that_still_carry_the_maker_estimate() {
    let now = Instant::now();
    let (lighter, binance) = binance_books(now);
    let mut config = config();
    config.pairs = vec![BINANCE_PAIR];
    let line = evaluate(
        BINANCE_PAIR,
        &binance_market(),
        Some(&lighter),
        Some(&binance),
        Some(normal(-0.06)),
        900,
        Session::Rth,
        &config,
        now,
    );
    assert!(!line.observe_only, "币安组不再只观察");
    let maker = line.maker.as_ref().unwrap();
    assert_eq!(
        maker.long_b.as_ref().unwrap().net_to_normal_taker_exit_pct,
        Some(dec!(0.02500))
    );
}

#[test]
fn a_virtual_maker_fill_needs_a_trade_strictly_through_the_quote() {
    // 买单：卖方主动、成交价低于挂单价才算。
    assert!(maker::crosses(true, dec!(99.98), dec!(99.97), true));
    assert!(
        !maker::crosses(true, dec!(99.98), dec!(99.98), true),
        "同价排队不知道"
    );
    assert!(
        !maker::crosses(true, dec!(99.98), dec!(99.97), false),
        "买方主动不会吃到买单"
    );
    // 卖单：买方主动、成交价高于挂单价。
    assert!(maker::crosses(false, dec!(99.99), dec!(100.00), false));
    assert!(!maker::crosses(false, dec!(99.99), dec!(99.99), false));
    assert!(!maker::crosses(false, dec!(99.99), dec!(100.00), true));
}

#[tokio::test]
async fn shadow_fills_record_the_hedge_after_the_fill_persist_and_cool_down() {
    let dir = std::env::temp_dir().join(format!(
        "arb-maker-shadow-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let (mut shadow, broken) = maker::Shadow::load(&dir, Utc::now()).await;
    assert_eq!((broken, shadow.summary(Utc::now()).fills), (0, 0));
    let t0 = Instant::now();
    let market = binance_market();
    let (lighter, binance) = binance_books(t0);
    let mut books: HashMap<(Venue, String), LocalBook> = HashMap::from([
        ((Venue::LighterRh, "26".to_string()), lighter.clone()),
        ((Venue::Binance, "NVDAUSDT".to_string()), binance.clone()),
    ]);
    // 正常基差 −0.1%：多 Binance（挂买一）回到正常净赚 0.065% ≥ 门槛 0.05%，挂着；反方向不挂。
    let median = Some(dec!(-0.1));
    let view = maker::evaluate(
        BINANCE_PAIR,
        &market,
        &lighter,
        &binance,
        median,
        dec!(1000),
    );
    shadow.observe(
        BINANCE_PAIR,
        &market,
        view.as_ref(),
        Some(&lighter),
        Some(&binance),
        median,
        Session::Rth,
        dec!(1000),
        t0,
    );
    assert_eq!(shadow.summary(Utc::now()).quoting, 1);
    let trade = |shadow: &mut maker::Shadow, books: &HashMap<_, _>, price, seller, at| {
        shadow.on_trade(
            "NVDAUSDT",
            price,
            dec!(2),
            seller,
            books,
            Venue::Binance,
            at,
            Utc::now(),
        )
    };
    // 同价、买方主动、别的合约：都不算成交。
    trade(&mut shadow, &books, dec!(99.98), true, t0);
    trade(&mut shadow, &books, dec!(99.97), false, t0);
    shadow.on_trade(
        "PLTRUSDT",
        dec!(1),
        dec!(1),
        true,
        &books,
        Venue::Binance,
        t0,
        Utc::now(),
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_secs(6)),
        0
    );
    // 卖方主动、严格穿过买一：成交，立刻采第一个样（对冲价 100.00 → 净 0.065%）。
    trade(&mut shadow, &books, dec!(99.97), true, t0);
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_millis(100)),
        0
    );
    // 之后 Lighter 买一掉到 99.99：0.3 秒的样就少赚 0.01%（逆向选择）。
    books.insert(
        (Venue::LighterRh, "26".to_string()),
        book(
            &[(dec!(99.99), dec!(100))],
            &[(dec!(100.01), dec!(100))],
            t0,
        ),
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_millis(400)),
        0
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_millis(5100)),
        1
    );
    let summary = shadow.summary(Utc::now());
    assert_eq!(summary.fills, 1);
    let stat = &summary.stats[0];
    assert_eq!(
        (
            stat.pair.as_str(),
            stat.base.as_str(),
            stat.direction.as_str()
        ),
        ("lighter-rh:binance", "NVDA", "long_b")
    );
    assert_eq!(stat.median_quoted_net_pct, Some(dec!(0.06500)));
    assert_eq!(
        stat.median_net_pct,
        vec![
            (0, Some(dec!(0.06500))),
            (300, Some(dec!(0.05500))),
            (1_000, Some(dec!(0.05500))),
            (5_000, Some(dec!(0.05500))),
        ]
    );
    assert_eq!(stat.positive_share_1s, Some(dec!(100.0)));
    // 冷却期内再穿一次：不再记（真成交之后会持仓）。
    trade(
        &mut shadow,
        &books,
        dec!(99.96),
        true,
        t0 + Duration::from_secs(10),
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_secs(20)),
        0
    );
    // 不再监控这个合约：撤掉虚拟单。
    shadow.retain_markets(&|_, _| false);
    assert_eq!(shadow.summary(Utc::now()).quoting, 0);
    // 落盘的记录读得回来（写盘在后台，先等它做完）。
    shadow.settle().await;
    let (loaded, broken) = maker::Shadow::load(&dir, Utc::now()).await;
    assert_eq!((broken, loaded.summary(Utc::now()).fills), (0, 1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_virtual_quote_keeps_its_price_until_it_has_rested_and_is_pulled_when_it_no_longer_pays()
{
    let dir = std::env::temp_dir().join(format!(
        "arb-maker-sticky-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let (mut shadow, _) = maker::Shadow::load(&dir, Utc::now()).await;
    let t0 = Instant::now();
    let market = binance_market();
    let median = Some(dec!(-0.1));
    let observe = |shadow: &mut maker::Shadow, lighter: &LocalBook, binance: &LocalBook, at| {
        let view = maker::evaluate(BINANCE_PAIR, &market, lighter, binance, median, dec!(1000));
        shadow.observe(
            BINANCE_PAIR,
            &market,
            view.as_ref(),
            Some(lighter),
            Some(binance),
            median,
            Session::Rth,
            dec!(1000),
            at,
        );
    };
    let (lighter, binance) = binance_books(t0);
    observe(&mut shadow, &lighter, &binance, t0);
    assert_eq!(shadow.summary(Utc::now()).quoting, 1, "挂在买一 99.98");
    // 1 秒后 Binance 买一扫到 99.97：虚拟单还挂在 99.98（不到 5 秒不改价）。
    let swept = book(&[(dec!(99.97), dec!(100))], &[(dec!(99.98), dec!(100))], t0);
    let at = t0 + Duration::from_secs(1);
    observe(&mut shadow, &lighter, &swept, at);
    let books: HashMap<(Venue, String), LocalBook> = HashMap::from([
        ((Venue::LighterRh, "26".to_string()), lighter.clone()),
        ((Venue::Binance, "NVDAUSDT".to_string()), swept.clone()),
    ]);
    // 盘口先到、成交后到：99.975 的卖方成交仍然穿过还挂在 99.98 的单 —— 这正是要量的那种成交。
    shadow.on_trade(
        "NVDAUSDT",
        dec!(99.975),
        dec!(1),
        true,
        &books,
        Venue::Binance,
        at,
        Utc::now(),
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, at + Duration::from_secs(6)),
        1
    );
    let summary = shadow.summary(Utc::now());
    assert_eq!(summary.stats[0].fills, 1);
    shadow.settle().await;
    let text = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|entry| std::fs::read_to_string(entry.path()).unwrap())
        .collect::<String>();
    let fill: maker::Fill = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(fill.maker_price, dec!(99.98));
    assert!(
        (1_000..1_100).contains(&fill.quote_age_ms),
        "{}",
        fill.quote_age_ms
    );

    // 另一张：挂满 5 秒、最优价变了才按新价重挂。
    let (mut shadow, _) =
        maker::Shadow::load(&std::env::temp_dir().join("arb-maker-unused"), Utc::now()).await;
    observe(&mut shadow, &lighter, &binance, t0);
    let lower = book(&[(dec!(99.96), dec!(100))], &[(dec!(99.97), dec!(100))], t0);
    observe(&mut shadow, &lighter, &lower, t0 + Duration::from_secs(6));
    let books: HashMap<(Venue, String), LocalBook> = HashMap::from([
        ((Venue::LighterRh, "26".to_string()), lighter.clone()),
        ((Venue::Binance, "NVDAUSDT".to_string()), lower.clone()),
    ]);
    // 新价 99.96：99.97 的成交不再穿过。
    shadow.on_trade(
        "NVDAUSDT",
        dec!(99.97),
        dec!(1),
        true,
        &books,
        Venue::Binance,
        t0 + Duration::from_secs(6),
        Utc::now(),
    );
    assert_eq!(
        shadow.tick(&books, Venue::Binance, t0 + Duration::from_secs(12)),
        0
    );
    assert_eq!(shadow.summary(Utc::now()).quoting, 1);
    // 对面买盘掉到 99.90：按挂单价算不够门槛了 —— 撤（不等 5 秒）。
    let thin = book(&[(dec!(99.90), dec!(100))], &[(dec!(99.92), dec!(100))], t0);
    observe(&mut shadow, &thin, &lower, t0 + Duration::from_secs(7));
    assert_eq!(shadow.summary(Utc::now()).quoting, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn binance_pairs_only_take_tradfi_markets() {
    let venue_market = |key: &str, category: Option<&str>| pairs::VenueMarket {
        key: key.into(),
        taker_fee: Some(Decimal::ZERO),
        maker_fee: Some(Decimal::ZERO),
        category: category.map(str::to_string),
        outside_rth: None,
        bounds: None,
        size_multiplier: None,
    };
    let catalog: pairs::Catalog = HashMap::from([
        (
            Venue::Arcus,
            std::collections::BTreeMap::from([
                ("BTC".to_string(), venue_market("BTC-USD", Some("CRYPTO"))),
                (
                    "NVDA".to_string(),
                    venue_market("NVDA-USD", Some("EQUITIES")),
                ),
            ]),
        ),
        (
            Venue::Binance,
            std::collections::BTreeMap::from([
                ("BTC".to_string(), venue_market("BTCUSDT", Some("CRYPTO"))),
                (
                    "NVDA".to_string(),
                    venue_market("NVDAUSDT", Some("EQUITIES")),
                ),
            ]),
        ),
    ]);
    let identity: pairs::Identity = HashMap::from([
        ((Venue::Arcus, "BTC".to_string()), 1),
        ((Venue::Binance, "BTC".to_string()), 1),
        ((Venue::Arcus, "NVDA".to_string()), 2),
        ((Venue::Binance, "NVDA".to_string()), 2),
    ]);
    let pair = Pair {
        a: Venue::Arcus,
        b: Venue::Binance,
    };
    let markets = pairs::pair_markets(pair, &catalog, Some(&identity), false);
    assert_eq!(
        markets.iter().map(|m| m.base.as_str()).collect::<Vec<_>>(),
        ["NVDA"]
    );
    // 没有身份簇（首轮扫描前）：一个都不配。
    assert!(pairs::pair_markets(pair, &catalog, None, false).is_empty());
}

/// 主循环里快（每 100ms）、慢（整秒）两条路径都在 `evaluate` 之后调这一个函数填 `recent_move`；
/// 两家都记满窗口才有值，缺一家就是 `None`（自动交易对 `None` 放行）。
#[test]
fn recent_move_needs_both_venues_to_have_a_full_window_and_keeps_a_and_b_apart() {
    let t0 = Instant::now();
    let at = |ms: u64| t0 + Duration::from_millis(ms);
    let book = |bid: Decimal, ask: Decimal, now: Instant| {
        let mut book = LocalBook::new(now);
        LocalBook::apply(&mut book.bids, bid, Decimal::ONE);
        LocalBook::apply(&mut book.asks, ask, Decimal::ONE);
        book
    };
    let market = market("EQUITIES");
    let mut tape = tape::PriceTape::default();
    // Lighter RH（b）一直在记；Arcus（a）晚 3 秒才有盘口。
    for (ms, lighter) in [
        (0, (dec!(100.0), dec!(100.2))),
        (2_000, (dec!(99.6), dec!(99.8))),
        (3_000, (dec!(99.6), dec!(99.8))),
        (6_000, (dec!(99.6), dec!(99.8))),
    ] {
        let mut books = HashMap::new();
        books.insert(
            (Venue::LighterRh, "26".to_string()),
            book(lighter.0, lighter.1, at(ms)),
        );
        if ms >= 3_000 {
            books.insert(
                (Venue::Arcus, "SPY-USD".to_string()),
                book(dec!(100.0), dec!(100.2), at(ms)),
            );
        }
        tape.sync(&books, at(ms));
    }
    assert_eq!(
        recent_move(&tape, Pair::RH, &market, at(6_000)).0,
        None,
        "Arcus 只记了 3 秒"
    );
    let mut books = HashMap::new();
    books.insert(
        (Venue::LighterRh, "26".to_string()),
        book(dec!(99.6), dec!(99.8), at(8_100)),
    );
    books.insert(
        (Venue::Arcus, "SPY-USD".to_string()),
        book(dec!(100.0), dec!(100.2), at(8_100)),
    );
    tape.sync(&books, at(8_100));
    let moved = recent_move(&tape, Pair::RH, &market, at(8_100))
        .0
        .expect("两家都满窗口了");
    // 窗口 [3.1s, 8.1s]：Arcus 没动；Lighter 在 2 秒时的下跌已出窗口，也没动。
    assert_eq!((moved.a_pct, moved.b_pct), (Decimal::ZERO, Decimal::ZERO));
    // 再来一次 Lighter 的急跌：只有 b 动，a 不动；`of` 按场所取，`larger` 取大的那家。
    let mut books = HashMap::new();
    books.insert(
        (Venue::LighterRh, "26".to_string()),
        book(dec!(98.6), dec!(98.8), at(9_000)),
    );
    books.insert(
        (Venue::Arcus, "SPY-USD".to_string()),
        book(dec!(100.0), dec!(100.2), at(9_000)),
    );
    tape.sync(&books, at(9_000));
    let moved = recent_move(&tape, Pair::RH, &market, at(9_000)).0.unwrap();
    assert_eq!(moved.a_pct, Decimal::ZERO);
    assert!(moved.b_pct > dec!(1.0), "{}", moved.b_pct);
    let line = evaluate(
        Pair::RH,
        &market,
        None,
        None,
        None,
        0,
        Session::Off,
        &config(),
        at(9_000),
    );
    assert_eq!(moved.of(&line, Venue::LighterRh), moved.b_pct);
    assert_eq!(moved.of(&line, Venue::Arcus), moved.a_pct);
    assert_eq!(moved.larger(&line), (Venue::LighterRh, moved.b_pct));
    assert!(
        line.recent_move.is_none(),
        "evaluate 本身不填，由主循环在它之后填"
    );
}

// ---- 观察场所：Lighter 主网、Gate、Bitget、Ondo、PopDEX ----

fn pair(a: Venue, b: Venue) -> Pair {
    Pair { a, b }
}

/// 观察场所只和交易组里出现过的链上场所配对；已经在交易组里的场所不当观察场所；a / b 按固定顺序
/// （老场所在前，已有历史的方向不变）；有实盘凭据的观察场所只有 Lighter 主网会被自动配成交易组。
#[test]
fn watch_venues_pair_with_the_monitored_onchain_peers_only() {
    let trading = vec![
        Pair::RH,
        pair(Venue::HyperliquidXyz, Venue::LighterRh),
        pair(Venue::Arcus, Venue::Binance),
    ];
    let watch = pairs::watch_pairs(&trading, &pairs::DEFAULT_WATCH);
    assert!(watch.contains(&pair(Venue::Arcus, Venue::Gate)));
    assert!(watch.contains(&pair(Venue::LighterRh, Venue::Ondo)));
    assert!(watch.contains(&pair(Venue::HyperliquidXyz, Venue::PopDex)));
    assert!(watch.contains(&pair(Venue::HyperliquidXyz, Venue::Mexc)));
    assert!(
        !watch
            .iter()
            .any(|p| p.a == Venue::HyperliquidIo || p.b == Venue::HyperliquidIo),
        "交易组里没有 io，不配"
    );
    assert!(
        !watch
            .iter()
            .any(|p| p.a == Venue::Binance || p.b == Venue::Binance),
        "不和 Binance 配"
    );
    assert_eq!(watch.len(), 7 * 3, "7 家观察场所 × 3 家链上场所");
    assert!(
        watch.iter().all(|p| pairs::DEFAULT_WATCH.contains(&p.b)),
        "观察场所排在后面当 b"
    );
    // 交易组里已经有 Lighter 主网（显式配了）：它不再当观察场所。
    let with_lighter = vec![Pair::RH, pair(Venue::Arcus, Venue::Lighter)];
    let watch = pairs::watch_pairs(&with_lighter, &pairs::DEFAULT_WATCH);
    assert!(
        !watch
            .iter()
            .any(|p| p.a == Venue::Lighter || p.b == Venue::Lighter)
    );
    // 自动配交易组时观察场所不进来，即使有凭据 —— Lighter 主网、Bitget 除外。
    let auto = pairs::all_pairs(&[Venue::Arcus, Venue::LighterRh, Venue::Gate, Venue::Ondo]);
    assert_eq!(auto, vec![Pair::RH]);
    // 东京现在的实盘场所 + Lighter 主网：多出和它配的 5 组，组名（历史文件）与观察时一样；
    // Binance 和 Bitget / MEXC 一样也和主网配（2026-11-01 起）。
    let live = [
        Venue::Arcus,
        Venue::Binance,
        Venue::Hyperliquid,
        Venue::HyperliquidXyz,
        Venue::HyperliquidIo,
        Venue::LighterRh,
        Venue::Lighter,
        Venue::Gate,
    ];
    let auto = pairs::all_pairs(&live);
    let before = pairs::all_pairs(&live[..6]);
    let added: Vec<Pair> = auto
        .iter()
        .filter(|p| !before.contains(p))
        .copied()
        .collect();
    assert_eq!(
        added.iter().map(|p| p.id()).collect::<Vec<_>>(),
        [
            "arcus:lighter",
            "hyperliquid-xyz:lighter",
            "hyperliquid-io:lighter",
            "lighter-rh:lighter",
            "binance:lighter"
        ]
    );
    assert_eq!(auto.len(), before.len() + 5, "原来的组一个不少");
    assert!(
        !auto
            .iter()
            .any(|p| p.a == Venue::Gate || p.b == Venue::Gate),
        "Gate 有凭据也只观察"
    );
    let watch = pairs::watch_pairs(&auto, &pairs::DEFAULT_WATCH);
    assert!(
        !watch
            .iter()
            .any(|p| p.a == Venue::Lighter || p.b == Venue::Lighter),
        "进了交易组就不再是观察组"
    );
    assert!(watch.contains(&pair(Venue::Arcus, Venue::Gate)));
    // 再接上 Bitget：和每家链上场所（含 Lighter 主网）各一组，组名与观察时一样（历史接着用）；
    // 也和 Binance 配（2026-11-01 起币安是普通交易组），不和 Hyperliquid 主站配。
    let with_bitget: Vec<Venue> = live.iter().copied().chain([Venue::Bitget]).collect();
    let bitget_groups: Vec<String> = pairs::all_pairs(&with_bitget)
        .into_iter()
        .filter(|p| !auto.contains(p))
        .map(|p| p.id())
        .collect();
    assert_eq!(
        bitget_groups,
        [
            "arcus:bitget",
            "hyperliquid-xyz:bitget",
            "hyperliquid-io:bitget",
            "lighter-rh:bitget",
            "binance:bitget",
            "lighter:bitget"
        ]
    );
    for id in &bitget_groups[..4] {
        let watched = pairs::watch_pairs(&auto, &pairs::DEFAULT_WATCH);
        assert!(watched.iter().any(|p| p.id() == *id), "{id} 原来就是观察组");
    }
    // 再接上 MEXC：同样和每家链上场所各一组（前四组原来是观察组），但**不和 Bitget 配**（两家重合七百多个
    // 合约，Bitget 一条连接订阅不下，2026-10-26 上线后整条 Bitget 行情被反复重置）。
    let with_mexc: Vec<Venue> = with_bitget.iter().copied().chain([Venue::Mexc]).collect();
    let mexc_groups: Vec<String> = pairs::all_pairs(&with_mexc)
        .into_iter()
        .filter(|p| p.a == Venue::Mexc || p.b == Venue::Mexc)
        .map(|p| p.id())
        .collect();
    assert_eq!(
        mexc_groups,
        [
            "arcus:mexc",
            "hyperliquid-xyz:mexc",
            "hyperliquid-io:mexc",
            "lighter-rh:mexc",
            "binance:mexc",
            "lighter:mexc"
        ]
    );
    // 观察组都要核对同名合约是不是同一资产；只有 Arcus ↔ Lighter RH 不用。
    assert!(pair(Venue::Arcus, Venue::Lighter).needs_identity_check());
    assert!(pair(Venue::LighterRh, Venue::PopDex).needs_identity_check());
    assert!(!Pair::RH.needs_identity_check());
}

#[test]
fn the_watch_list_comes_from_the_environment_and_can_be_turned_off() {
    assert_eq!(parse_watch(None).unwrap(), pairs::DEFAULT_WATCH.to_vec());
    assert!(parse_watch(Some("off")).unwrap().is_empty());
    assert!(parse_watch(Some(" None ")).unwrap().is_empty());
    assert_eq!(
        parse_watch(Some("gate, ondo,gate")).unwrap(),
        vec![Venue::Gate, Venue::Ondo]
    );
    assert!(parse_watch(Some("gate,nope")).unwrap_err().contains("nope"));
    assert!(
        parse_watch(Some("okx")).is_err(),
        "价差监控没有 OKX 的行情接入"
    );
}

/// 2026-10-20 实测消息（节选档位）。
#[test]
fn watch_feeds_parse_their_full_books_and_ignore_heartbeats() {
    // Gate：量是 JSON 整数张数；只认 `all`。
    let gate = r#"{"time":1792470044,"time_ms":1792470044762,"channel":"futures.order_book","event":"all","result":{"t":1792470044663,"id":1108166219,"contract":"OPENAI_USDT","asks":[{"p":"1486.81","s":133},{"p":"1486.91","s":37}],"bids":[{"p":"1486.78","s":33}]}}"#;
    assert_eq!(
        feed::parse_gate(gate).unwrap(),
        feed::Event::Snapshot {
            market: "OPENAI_USDT".into(),
            bids: vec![(dec!(1486.78), dec!(33))],
            asks: vec![(dec!(1486.81), dec!(133)), (dec!(1486.91), dec!(37))],
            nonce: None,
        }
    );
    let gate_update = r#"{"channel":"futures.order_book","event":"update","result":[{"p":"1486.81","s":0,"c":"OPENAI_USDT"}]}"#;
    assert_eq!(
        feed::parse_gate(gate_update).unwrap(),
        feed::Event::Other,
        "增量不认，等下一份整本"
    );
    let gate_sub = r#"{"time":1792470044,"channel":"futures.order_book","event":"subscribe","payload":["NVDA_USDT","20","0"],"result":{"status":"success"}}"#;
    assert_eq!(feed::parse_gate(gate_sub).unwrap(), feed::Event::Other);
    let gate_bad = r#"{"time":1792470871,"channel":"futures.order_book","event":"subscribe","payload":["NOPE_USDT","20","0"],"error":{"code":2,"message":"unknown currency pair NOPE_USDT"},"result":{"status":"failed"}}"#;
    assert!(matches!(
        feed::parse_gate(gate_bad).unwrap(),
        feed::Event::Error(_)
    ));
    let gate_pong = r#"{"time":1792470864,"channel":"futures.pong","event":"","result":null}"#;
    assert_eq!(feed::parse_gate(gate_pong).unwrap(), feed::Event::Other);
    assert_eq!(
        feed::gate_subscribe("NVDA_USDT"),
        r#"{"channel":"futures.order_book","event":"subscribe","payload":["NVDA_USDT","10","0"]}"#
    );

    // Bitget：books15 每条整本；心跳回执是文本 pong。
    let bitget = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books15","instId":"NVDAUSDT"},"data":[{"asks":[["217.58","8.6"],["217.59","5.38"]],"bids":[["217.57","0.69"]],"ts":"1792470045613","seq":1}],"ts":1792470045614}"#;
    assert_eq!(
        feed::parse_bitget(bitget).unwrap(),
        feed::Event::Snapshot {
            market: "NVDAUSDT".into(),
            bids: vec![(dec!(217.57), dec!(0.69))],
            asks: vec![(dec!(217.58), dec!(8.6)), (dec!(217.59), dec!(5.38))],
            nonce: None,
        }
    );
    assert_eq!(feed::parse_bitget("pong").unwrap(), feed::Event::Other);
    assert_eq!(
        feed::parse_bitget(r#"{"event":"subscribe","arg":{"instType":"USDT-FUTURES","channel":"books15","instId":"NVDAUSDT"}}"#).unwrap(),
        feed::Event::Other
    );
    assert!(matches!(
        feed::parse_bitget(r#"{"event":"error","code":30001,"msg":"instType:USDT-FUTURES,channel:books15,instId:NOPEUSDT doesn't exist"}"#).unwrap(),
        feed::Event::Error(_)
    ));
    let subs = feed::bitget_subscribe(&(0..45).map(|i| format!("X{i}USDT")).collect::<Vec<_>>());
    assert_eq!(subs.len(), 3, "每条最多 20 个");

    // Ondo：data 是数组，一条可能有几个市场。
    let ondo = r#"{"type":"update","channel":"depthBooksPerps","timestamp":"2026-10-20T04:20:43.962185494Z","data":[{"market":"NVDA-USD.P","time":"2026-10-20T04:20:43.914185058Z","asks":[["217.24","0.46"]],"bids":[["217.2","4.6"]]},{"market":"SNDK-USD.P","asks":[["1356.9","6.877"]],"bids":[["1356.3","0.147"]]}]}"#;
    let feed::Event::Batch(events) = feed::parse_ondo(ondo).unwrap() else {
        panic!("Ondo 一条消息是一批")
    };
    assert_eq!(events.len(), 2, "两个市场都要");
    assert_eq!(
        events[1],
        feed::Event::Snapshot {
            market: "SNDK-USD.P".into(),
            bids: vec![(dec!(1356.3), dec!(0.147))],
            asks: vec![(dec!(1356.9), dec!(6.877))],
            nonce: None,
        }
    );
    assert_eq!(
        feed::parse_ondo(
            r#"{"type":"subscribed","channel":"depthBooksPerps","data":{"op":"subscribe"}}"#
        )
        .unwrap(),
        feed::Event::Other
    );
    assert_eq!(
        feed::parse_ondo(r#"{"type":"pong"}"#).unwrap(),
        feed::Event::Other
    );
    assert!(matches!(
        feed::parse_ondo(r#"{"type":"error","data":"unknown market"}"#).unwrap(),
        feed::Event::Error(_)
    ));

    // PopDEX：books5 每条整本；市场名保留原样（小写前缀）。
    let popdex = r#"{"action":"snapshot","arg":{"category":"futures","topic":"books5","symbolId":20007,"symbol":"kPEPEUSDT"},"data":{"a":[["0.0042","182100"]],"b":[["0.0041","171800"]],"ts":"1"}}"#;
    assert_eq!(
        feed::parse_popdex(popdex).unwrap(),
        feed::Event::Snapshot {
            market: "kPEPEUSDT".into(),
            bids: vec![(dec!(0.0041), dec!(171800))],
            asks: vec![(dec!(0.0042), dec!(182100))],
            nonce: None,
        }
    );
    assert_eq!(feed::parse_popdex("pong").unwrap(), feed::Event::Other);
    assert!(matches!(
        feed::parse_popdex(r#"{"event":"error","code":30002,"msg":"Unrecognized request"}"#)
            .unwrap(),
        feed::Event::Error(_)
    ));
    // MEXC：push.depth.full 整本，[价格, 张数, 笔数] 都是数字；回执、心跳忽略，订错合约报错。
    let mexc = r#"{"symbol":"NVIDIA_USDT","data":{"cts":1792594379679,"asks":[[184.55,1210,2],[184.56,33316,1]],"bids":[[184.53,18391,1]],"version":42828680545},"channel":"push.depth.full","ts":1792594379687}"#;
    assert_eq!(
        feed::parse_mexc(mexc).unwrap(),
        feed::Event::Snapshot {
            market: "NVIDIA_USDT".into(),
            bids: vec![(dec!(184.53), dec!(18391))],
            asks: vec![(dec!(184.55), dec!(1210)), (dec!(184.56), dec!(33316))],
            nonce: None,
        }
    );
    let tiny = r#"{"symbol":"X_USDT","data":{"asks":[[1e-5,10,1]],"bids":[]},"channel":"push.depth.full"}"#;
    let feed::Event::Snapshot { asks, .. } = feed::parse_mexc(tiny).unwrap() else {
        panic!("科学计数法的价格也要认")
    };
    assert_eq!(asks, vec![(dec!(0.00001), dec!(10))]);
    let mexc_ack = r#"{"channel":"rs.sub.depth.full","data":"success","ts":1792594379460}"#;
    assert_eq!(feed::parse_mexc(mexc_ack).unwrap(), feed::Event::Other);
    assert_eq!(
        feed::parse_mexc(r#"{"channel":"pong","data":1792594379463,"ts":1792594379463}"#).unwrap(),
        feed::Event::Other
    );
    let mexc_bad =
        r#"{"channel":"rs.error","data":"Contract [NOPE_USDT] not exists","ts":1792594379463}"#;
    assert!(matches!(
        feed::parse_mexc(mexc_bad).unwrap(),
        feed::Event::Error(_)
    ));
    assert_eq!(
        feed::mexc_subscribe("NVIDIA_USDT"),
        r#"{"method":"sub.depth.full","param":{"limit":10,"symbol":"NVIDIA_USDT"}}"#
    );
    assert_eq!(feed::mexc_market(mexc), Some("NVIDIA_USDT"));
    assert_eq!(feed::mexc_market(mexc_ack), None);
    assert_eq!(feed::mexc_market(mexc_bad), None);
    // 合并用的市场名：只认整本，别的消息（回执、报错、心跳）照常解析。
    assert_eq!(feed::gate_market(gate), Some("OPENAI_USDT"));
    assert_eq!(feed::gate_market(gate_sub), None);
    assert_eq!(feed::gate_market(gate_bad), None);
    assert_eq!(feed::bitget_market(bitget), Some("NVDAUSDT"));
    assert_eq!(feed::bitget_market("pong"), None);
    assert_eq!(feed::popdex_market(popdex), Some("kPEPEUSDT"));
    assert_eq!(
        feed::popdex_market(r#"{"event":"subscribe","arg":{"symbol":"SNDKUSDT"}}"#),
        None
    );
    let popdex_sub = &feed::popdex_subscribe(&["kPEPEUSDT".to_string()])[0];
    assert!(
        popdex_sub.contains(r#""symbol":"kPEPEUSDT""#)
            && popdex_sub.contains(r#""topic":"books5""#)
    );
    assert!(feed::ondo_subscribe(&["NVDA-USD.P".to_string()])[0].contains(r#""limit":10"#));
}

/// Gate 的盘口量是张数：乘面值换成标的数量；不知道面值的市场整条丢掉；别家原样。Ondo 的一批要逐个落到盘口。
#[test]
fn gate_contract_sizes_become_base_quantities_and_batches_land_in_every_book() {
    let mut multipliers = HashMap::new();
    multipliers.insert((Venue::Gate, "OPENAI_USDT".to_string()), dec!(0.01));
    let snapshot = |market: &str| feed::Event::Snapshot {
        market: market.into(),
        bids: vec![(dec!(1486.78), dec!(33))],
        asks: vec![(dec!(1486.81), dec!(133))],
        nonce: None,
    };
    let feed::Event::Snapshot { bids, asks, .. } =
        scaled(snapshot("OPENAI_USDT"), Venue::Gate, &multipliers).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        (bids[0].1, asks[0].1),
        (dec!(0.33), dec!(1.33)),
        "33 张 × 0.01"
    );
    assert!(
        scaled(snapshot("NVDA_USDT"), Venue::Gate, &multipliers).is_none(),
        "不知道面值：不拿张数当数量"
    );
    assert_eq!(
        scaled(snapshot("NVDAUSDT"), Venue::Bitget, &multipliers),
        Some(snapshot("NVDAUSDT"))
    );

    let mut books = HashMap::new();
    apply(
        &mut books,
        Venue::Ondo,
        feed::Event::Batch(vec![snapshot("NVDA-USD.P"), snapshot("SNDK-USD.P")]),
        None,
    );
    assert!(books.contains_key(&(Venue::Ondo, "NVDA-USD.P".to_string())));
    assert!(books.contains_key(&(Venue::Ondo, "SNDK-USD.P".to_string())));
}

/// Lighter 主网的订阅排队发：任意 1 秒不超过 20 条、60 秒不超过 180 条（主网限额：在途 50 条、
/// 每分钟 200 条；10-20 一口气发近百条被拒 9 条，那 9 个市场一直没有盘口）。同一条不重复排；
/// 只有主网排队，RH 照旧一次发完。
#[test]
fn lighter_mainnet_subscriptions_are_paced_under_its_limits() {
    let mut pacer = Pacer::default();
    (0..120).for_each(|id| pacer.push(feed::lighter_subscribe(id)));
    pacer.push(feed::lighter_subscribe(0));
    let start = Instant::now();
    assert_eq!(pacer.take(start).len(), 20);
    assert!(
        pacer.take(start + Duration::from_millis(500)).is_empty(),
        "同一秒内不再发"
    );
    let mut sent = 20;
    let mut at = start;
    for _ in 0..5 {
        at += Duration::from_secs(1);
        sent += pacer.take(at).len();
    }
    assert_eq!(sent, 120, "重复的那条没有再排；120 条 6 秒发完");
    assert!(pacer.is_empty());

    // 成批重订阅：60 秒内累计到 180 条就停，等最早那批满 60 秒。
    (0..100).for_each(|id| pacer.push(feed::lighter_unsubscribe(id)));
    let mut burst = 0;
    for _ in 0..3 {
        at += Duration::from_secs(1);
        burst += pacer.take(at).len();
    }
    assert_eq!(burst, 60);
    assert!(
        pacer.take(at + Duration::from_secs(1)).is_empty(),
        "60 秒内已经 180 条"
    );
    assert_eq!(pacer.take(start + Duration::from_secs(60)).len(), 20);

    assert!(protocol(Venue::Lighter, Stream::Books).unwrap().paced);
    assert!(!protocol(Venue::LighterRh, Stream::Books).unwrap().paced);
}

/// Lighter 主网和 RH 一样按 nonce 查连续性、断了要重订阅；别家不用。
#[test]
fn both_lighter_deployments_resubscribe_on_a_nonce_gap() {
    assert!(resubscribes(Venue::LighterRh));
    assert!(resubscribes(Venue::Lighter));
    for venue in [
        Venue::Arcus,
        Venue::Gate,
        Venue::Bitget,
        Venue::Ondo,
        Venue::PopDex,
        Venue::Binance,
    ] {
        assert!(!resubscribes(venue), "{venue}");
    }
    let mut books = HashMap::new();
    let (tx, mut rx) = mpsc::channel(4);
    let key = (Venue::Lighter, "1".to_string());
    apply(
        &mut books,
        Venue::Lighter,
        feed::Event::Snapshot {
            market: "1".into(),
            bids: vec![(dec!(1), dec!(1))],
            asks: vec![(dec!(2), dec!(1))],
            nonce: Some(5),
        },
        Some(&tx),
    );
    apply(
        &mut books,
        Venue::Lighter,
        feed::Event::Delta {
            market: "1".into(),
            bids: vec![],
            asks: vec![],
            begin_nonce: Some(9),
            nonce: Some(10),
        },
        Some(&tx),
    );
    assert!(!books.contains_key(&key));
    assert_eq!(rx.try_recv().unwrap(), "1");
    // 观察场所推的是整本：只按「没变也推」判断新鲜度的是 Arcus 与 Hyperliquid。
    for venue in [
        Venue::Lighter,
        Venue::Gate,
        Venue::Bitget,
        Venue::Ondo,
        Venue::PopDex,
    ] {
        assert!(!snapshot_feed(venue), "{venue}");
    }
    assert!(snapshot_feed(Venue::Arcus));
    // Hyperliquid：默认只订 bbo（只推变化，新鲜度靠连接心跳）；l2 模式才是整本快照。
    for venue in [
        Venue::Hyperliquid,
        Venue::HyperliquidXyz,
        Venue::HyperliquidIo,
    ] {
        assert!(!snapshot_feed_in(venue, feed::HlBook::Bbo), "{venue}");
        assert!(snapshot_feed_in(venue, feed::HlBook::L2), "{venue}");
    }
    assert_eq!(
        feed::hyperliquid_book_mode(),
        feed::HlBook::Bbo,
        "默认不用 l2Book"
    );
}

/// 观察场所的市场列表：过滤、命名（与扫描同一套，身份分簇才对得上）、费率、类别、Gate 的面值。
#[test]
fn watch_catalogs_name_markets_like_the_scanner_and_keep_fees_honest() {
    let gate = serde_json::json!([
        {"name": "OPENAI_USDT", "type": "direct", "status": "trading", "in_delisting": false, "quanto_multiplier": "0.01", "taker_fee_rate": "0.00075"},
        {"name": "NVDA_USDT", "type": "direct", "status": "trading", "quanto_multiplier": "0", "taker_fee_rate": "0.00075"},
        {"name": "BTC3L_USDT", "type": "direct", "status": "trading", "quanto_multiplier": "1", "taker_fee_rate": "0.00075"},
        {"name": "OLD_USDT", "type": "direct", "status": "delisting", "quanto_multiplier": "1", "taker_fee_rate": "0.00075"}
    ]);
    let gate = catalog::gate(&gate).unwrap();
    assert_eq!(
        gate.keys().collect::<Vec<_>>(),
        vec!["OPENAI"],
        "面值为 0 的换算不了，不列"
    );
    assert_eq!(gate["OPENAI"].key, "OPENAI_USDT");
    assert_eq!(gate["OPENAI"].size_multiplier, Some(dec!(0.01)));
    assert_eq!(gate["OPENAI"].taker_fee, Some(dec!(0.00075)));

    let bitget = serde_json::json!({"code": "00000", "data": [
        {"symbol": "NVDAUSDT", "baseCoin": "NVDA", "quoteCoin": "USDT", "symbolType": "perpetual", "symbolStatus": "normal", "takerFeeRate": "0.0006", "isRwa": "YES"},
        {"symbol": "QNTUSDT", "baseCoin": "QNT", "quoteCoin": "USDT", "symbolType": "perpetual", "symbolStatus": "normal", "takerFeeRate": "0.0006", "isRwa": "NO"},
        {"symbol": "XUSDT", "baseCoin": "X", "quoteCoin": "USDT", "symbolType": "perpetual", "symbolStatus": "limit_open", "takerFeeRate": "0.0006", "isRwa": "NO"},
        {"symbol": "BTCUSDT_261226", "baseCoin": "BTC", "quoteCoin": "USDT", "symbolType": "delivery", "symbolStatus": "normal", "takerFeeRate": "0.0006"}
    ]});
    let bitget = catalog::bitget(&bitget).unwrap();
    assert_eq!(bitget.keys().collect::<Vec<_>>(), vec!["NVDA", "QNT"]);
    assert_eq!(bitget["NVDA"].category.as_deref(), Some("EQUITIES"));
    assert_eq!(bitget["QNT"].category.as_deref(), Some("CRYPTO"));
    assert_eq!(bitget["NVDA"].taker_fee, Some(dec!(0.0006)));

    let ondo: Vec<arb_venues::ondo::Contract> = serde_json::from_value(serde_json::json!([
        {"market": "NVDA-USD.P", "productType": "perpetual", "baseCurrency": "NVDA", "disabled": false, "takerFee": "0.00025", "tags": ["Stock"]},
        {"market": "US500-USD.P", "productType": "perpetual", "baseCurrency": "US500", "disabled": false, "takerFee": "0.00025", "tags": ["Index"]},
        {"market": "BTC-USD.P", "productType": "perpetual", "baseCurrency": "BTC", "disabled": false, "takerFee": "0.00025", "tags": ["Crypto"]},
        {"market": "OPENAI-USD.P", "productType": "perpetual", "baseCurrency": "OPENAI", "disabled": true, "takerFee": "0.00035", "tags": ["Pre-IPO"]}
    ]))
    .unwrap();
    let ondo = catalog::ondo(&ondo).unwrap();
    assert_eq!(
        ondo.keys().collect::<Vec<_>>(),
        vec!["BTC", "NVDA", "US500"],
        "未开放的不列"
    );
    assert_eq!(ondo["NVDA"].key, "NVDA-USD.P");
    assert_eq!(
        (
            ondo["NVDA"].category.as_deref(),
            ondo["US500"].category.as_deref(),
            ondo["BTC"].category.as_deref()
        ),
        (Some("EQUITIES"), Some("INDICES"), Some("CRYPTO"))
    );

    let symbols = serde_json::json!({"code": "200", "data": [
        {"symbol": "SNDKUSDT", "baseToken": "SNDK", "category": "Futures", "status": "Trading", "assetClass": "Rwa"},
        {"symbol": "kPEPEUSDT", "baseToken": "kPEPE", "category": "Futures", "status": "Trading", "assetClass": "Crypto"},
        {"symbol": "SNDKUSDC", "baseToken": "SNDK", "category": "Spot", "status": "Trading", "assetClass": "Rwa"},
        {"symbol": "OLDUSDT", "baseToken": "OLD", "category": "Futures", "status": "Delisted", "assetClass": "Crypto"}
    ]});
    let fees: Vec<arb_venues::popdex::FeeRow> = serde_json::from_value(serde_json::json!([
        {"symbol": "SNDKUSDT", "feeRates": [{"vipLevel": "1", "takerFeeRate": "0.0003"}, {"vipLevel": "0", "takerFeeRate": "0.00032"}]}
    ]))
    .unwrap();
    let popdex = catalog::popdex(&symbols, &fees).unwrap();
    assert_eq!(popdex.keys().collect::<Vec<_>>(), vec!["KPEPE", "SNDK"]);
    assert_eq!(popdex["KPEPE"].key, "kPEPEUSDT", "订阅名保留原样");
    assert_eq!(popdex["SNDK"].taker_fee, Some(dec!(0.00032)), "VIP 0");
    assert_eq!(popdex["KPEPE"].taker_fee, None, "没报费率的不当 0");
    assert_eq!(popdex["SNDK"].category.as_deref(), Some("EQUITIES"));

    // MEXC：股票按显示名里的代码（订阅仍用合约名），量是张数要乘面值，加密币不改名。
    let mexc = serde_json::json!({"success": true, "code": 0, "data": [
        {"symbol": "NVIDIA_USDT", "displayNameEn": "NVDA_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.01, "takerFeeRate": 0.0004, "conceptPlate": ["mc-trade-zone-Stock", "mc-trade-zone-tradfi", "mc-trade-zone-usstocks"]},
        {"symbol": "XAU_USDT", "displayNameEn": "GOLD(XAU)_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.001, "takerFeeRate": 0.0004, "conceptPlate": ["mc-trade-zone-metals", "mc-trade-zone-tradfi", "mc-trade-zone-Commodities"]},
        {"symbol": "FILECOIN_USDT", "displayNameEn": "FIL_USDT PERPETUAL", "settleCoin": "USDT", "state": 0, "contractSize": 0.01, "takerFeeRate": 0.0004, "conceptPlate": ["mc-trade-zone-web3"]}
    ]});
    let mexc = catalog::mexc(&arb_venues::mexc::contracts(&mexc).unwrap()).unwrap();
    assert_eq!(
        mexc.keys().collect::<Vec<_>>(),
        vec!["FILECOIN", "NVDA", "XAU"]
    );
    assert_eq!(mexc["NVDA"].key, "NVIDIA_USDT");
    assert_eq!(mexc["NVDA"].size_multiplier, Some(dec!(0.01)));
    // 公开 takerFeeRate 0.0004 不适用 API 账户：按账户实际付的 0.08% 算。
    assert_eq!(
        mexc["NVDA"].taker_fee,
        Some(arb_venues::mexc::API_TAKER_FEE)
    );
    assert_eq!(mexc["NVDA"].category.as_deref(), Some("EQUITIES"));
    assert_eq!(mexc["XAU"].category.as_deref(), Some("COMMODITIES"));
    assert_eq!(mexc["FILECOIN"].category.as_deref(), Some("CRYPTO"));

    let lighter = serde_json::json!({"code": 200, "order_book_details": [
        {"symbol": "QNT", "market_id": 7, "status": "active", "market_type": "perp", "taker_fee": "0.0000"}
    ]});
    let lighter = catalog::lighter(Venue::Lighter, &lighter).unwrap();
    assert_eq!(lighter["QNT"].key, "7");
    assert!(
        catalog::lighter(Venue::Lighter, &serde_json::json!({}))
            .unwrap_err()
            .to_string()
            .starts_with("lighter ")
    );
}

/// 观察组的行和交易快照分开放：自动交易、下单读的 `view()` 里没有，面板的 `panel_view()` 里有。
#[tokio::test]
async fn watch_pairs_live_outside_the_trading_snapshot() {
    let mut config = config();
    config.watch_pairs = pairs::watch_pairs(&config.pairs, &pairs::DEFAULT_WATCH);
    assert_eq!(config.watch_pairs.len(), 14, "7 家 × Arcus、Lighter RH");
    let monitor = Monitor::new(config.clone(), crate::alert::Alerter::with_sink(None));
    let view = monitor.view().await;
    assert_eq!(view.pairs.len(), 1);
    assert!(view.pairs.iter().all(|p| !p.watch_only));
    let panel = monitor.panel_view().await;
    assert_eq!(panel.watch.pairs.len(), 14);
    assert!(panel.watch.pairs.iter().all(|p| p.watch_only));
    assert_eq!(panel.view.pairs.len(), 1);
    // evaluate 给观察组的行打上标记。
    let now = Instant::now();
    let line = evaluate(
        pair(Venue::Arcus, Venue::Gate),
        &market("EQUITIES"),
        Some(&book(
            &[(dec!(99), dec!(100))],
            &[(dec!(100), dec!(100))],
            now,
        )),
        Some(&book(
            &[(dec!(99), dec!(100))],
            &[(dec!(100), dec!(100))],
            now,
        )),
        None,
        0,
        Session::Rth,
        &config,
        now,
    );
    assert!(line.watch_only);
    let json = serde_json::to_value(&panel).unwrap();
    assert!(json.get("lines").is_some() && json.get("watch").is_some());
}

/// 单笔阶梯：只在有信号的方向上，从监控名义起每 100 USDT 一档，吃深度越多价差越差，深度不够的档就停；
/// 第一档与监控本身那一档的数字完全一样。
#[test]
fn signal_lines_carry_a_size_ladder_that_stops_where_depth_runs_out() {
    let now = Instant::now();
    // Arcus 卖盘约 600 USDT，越往上越贵；Lighter RH 买盘很厚。
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[
            (dec!(100.00), dec!(2)),
            (dec!(100.05), dec!(2)),
            (dec!(100.10), dec!(2)),
        ],
        now,
    );
    let lighter = book(
        &[(dec!(100.30), dec!(100))],
        &[(dec!(100.32), dec!(100))],
        now,
    );
    let cfg = Config {
        size_usdt: dec!(200),
        ..config()
    };
    let m = market("INDICES");
    let line = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    assert!(line.best.as_ref().unwrap().signal);
    assert_eq!(line.best.as_ref().unwrap().direction, "long_a");
    let leg = line.long_a.as_ref().unwrap();
    let sizes: Vec<Decimal> = leg.ladder.iter().map(|r| r.size_usdt).collect();
    assert_eq!(
        sizes,
        [dec!(200), dec!(300), dec!(400), dec!(500), dec!(600)],
        "700 USDT 吃穿 Arcus 卖盘，停在 600"
    );
    let first = &leg.ladder[0];
    assert_eq!(first.entry_pct, leg.quote.entry_pct);
    assert_eq!(first.net_to_zero_pct, leg.net_to_zero_pct);
    assert_eq!(first.net_to_normal_pct, leg.net_to_normal_pct);
    assert!(
        leg.ladder
            .windows(2)
            .all(|w| w[1].net_to_normal_pct < w[0].net_to_normal_pct),
        "吃得越深越不赚"
    );
    // 没信号的方向不算阶梯，接口里也不出现这个字段。
    let other = line.long_b.as_ref().unwrap();
    assert!(other.ladder.is_empty());
    assert!(serde_json::to_value(other).unwrap().get("ladder").is_none());

    // 没信号（正常基差 −0.3%，就是现在的水平，系统性偏差）：两个方向都没有阶梯。
    let quiet = evaluate(
        Pair::RH,
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.3)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    assert!(!quiet.best.as_ref().unwrap().signal);
    assert!(quiet.long_a.as_ref().unwrap().ladder.is_empty());
    assert!(quiet.long_b.as_ref().unwrap().ladder.is_empty());
}

#[test]
fn the_estimate_size_starts_from_the_env_and_follows_the_auto_trading_size() {
    let monitor = Monitor::new(config(), crate::alert::Alerter::with_sink(None));
    assert_eq!(monitor.size_usdt(), dec!(1000));
    monitor.set_size_usdt(dec!(400));
    assert_eq!(monitor.size_usdt(), dec!(400));
    // 提醒里「可成交价差 …（N USDT）」用每轮的估算名义。
    let cfg = Config {
        size_usdt: monitor.size_usdt(),
        ..config()
    };
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.20), dec!(100))],
        &[(dec!(100.22), dec!(100))],
        now,
    );
    let line = evaluate(
        Pair::RH,
        &market("INDICES"),
        Some(&arcus),
        Some(&lighter),
        Some(normal(-0.05)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let best = line.best.clone().unwrap();
    let text = alert_text(&line, &best, &cfg, None).unwrap();
    assert!(text.contains("（400 USDT）"), "{text}");
}

#[test]
fn a_subscription_change_is_described_by_what_was_added_and_dropped() {
    let list = |keys: &[&str]| keys.iter().map(|key| key.to_string()).collect::<Vec<_>>();
    assert_eq!(
        describe_change(&list(&["1", "2", "3"]), &list(&["2", "3", "4"])),
        "+[4] -[1]"
    );
    assert_eq!(describe_change(&list(&["1"]), &list(&["1"])), "+[] -[]");
    let many: Vec<String> = (0..12).map(|n| n.to_string()).collect();
    assert_eq!(describe_change(&[], &many), "+[0,1,2,3,4,5,6,7…等12个] -[]");
}

/// 订阅列表退订缓冲：进进出出的市场一直订着，不反复重连；真没了的 15 分钟后退订；新增的马上订。
#[test]
fn markets_that_drop_out_of_the_subscription_list_linger_before_unsubscribing() {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    let t0 = Instant::now();
    let min = |m: u64| t0 + Duration::from_secs(m * 60);
    let wanted = |keys: &[&str]| -> HashMap<Venue, Vec<String>> {
        [(Venue::Lighter, keys.iter().map(|k| k.to_string()).collect())].into()
    };
    let mut seen = HashMap::new();
    let list = |out: HashMap<Venue, Vec<String>>| out[&Venue::Lighter].clone();
    assert_eq!(
        list(super::lingering(&wanted(&["7", "192"]), &mut seen, min(0))),
        ["7", "192"]
    );
    // 5 分钟后 192 不在列表里了：还订着（列表不变，不重连）。
    assert_eq!(
        list(super::lingering(&wanted(&["7"]), &mut seen, min(5))),
        ["7", "192"]
    );
    // 又回来：照旧，计时刷新到 10 分钟。
    assert_eq!(
        list(super::lingering(&wanted(&["7", "192"]), &mut seen, min(10))),
        ["7", "192"]
    );
    // 新增的马上进列表（排在要订的里，按 wanted 的顺序）。
    assert_eq!(
        list(super::lingering(
            &wanted(&["7", "4097"]),
            &mut seen,
            min(20)
        )),
        ["7", "4097", "192"]
    );
    // 192 自 10 分钟后一直没再要：满 15 分钟（25 分钟时）退订。
    assert_eq!(
        list(super::lingering(
            &wanted(&["7", "4097"]),
            &mut seen,
            min(25)
        )),
        ["7", "4097"]
    );
    assert!(!seen.contains_key(&(Venue::Lighter, "192".to_string())));
    // 整家都不要了：缓冲期里还留着这家的列表。
    let out = super::lingering(&HashMap::new(), &mut seen, min(30));
    assert_eq!(out[&Venue::Lighter], ["4097", "7"]);
    assert!(super::lingering(&HashMap::new(), &mut seen, min(60)).is_empty());
}

/// 换分钟后正常水平不再一次全算：每轮最多刷新 budget 个，其余先用上一分钟的；从没算过的这一轮先给 None。
#[test]
fn history_stats_refresh_a_few_per_pass_instead_of_all_at_the_minute_boundary() {
    use std::collections::HashMap;
    let mut cache: HashMap<&str, (i64, u32)> = HashMap::new();
    let mut calls = 0;
    let pass =
        |cache: &mut HashMap<&str, (i64, u32)>, minute: i64, budget: usize, calls: &mut u32| {
            let mut left = super::RefreshBudget::new(
                budget,
                std::time::Duration::from_secs(60),
                std::time::Instant::now(),
            );
            ["A", "B", "C"]
                .iter()
                .map(|key| {
                    super::cached_history(cache, *key, minute, &mut left, || {
                        *calls += 1;
                        minute as u32
                    })
                })
                .collect::<Vec<_>>()
        };
    // 第一轮只够算两个：C 先没有。
    assert_eq!(
        pass(&mut cache, 60, 2, &mut calls),
        [Some(60), Some(60), None]
    );
    // 同一分钟：算过的不再算，只补 C。
    assert_eq!(
        pass(&mut cache, 60, 2, &mut calls),
        [Some(60), Some(60), Some(60)]
    );
    assert_eq!(calls, 3);
    // 换分钟：这一轮只够刷新一个，其余先用旧值（不是 None，也不全算）。
    assert_eq!(
        pass(&mut cache, 120, 1, &mut calls),
        [Some(120), Some(60), Some(60)]
    );
    assert_eq!(
        pass(&mut cache, 120, 1, &mut calls),
        [Some(120), Some(120), Some(60)]
    );
    assert_eq!(
        pass(&mut cache, 120, 1, &mut calls),
        [Some(120), Some(120), Some(120)]
    );
    assert_eq!(calls, 6);
    assert_eq!(
        pass(&mut cache, 120, 1, &mut calls),
        [Some(120), Some(120), Some(120)]
    );
    assert_eq!(calls, 6, "都刷新过就不再算");
}

/// 刷新额度也按时间算：超过时间上限就停（每轮至少刷新一个，保证总能刷完）。
#[test]
fn the_history_refresh_budget_stops_on_time_but_always_allows_one() {
    use std::time::{Duration, Instant};
    let mut spent = super::RefreshBudget::new(80, Duration::ZERO, Instant::now());
    let mut cache: std::collections::HashMap<&str, (i64, u32)> = Default::default();
    let got: Vec<_> = ["A", "B", "C"]
        .iter()
        .map(|key| super::cached_history(&mut cache, *key, 60, &mut spent, || 1))
        .collect();
    assert_eq!(got, [Some(1), None, None], "时间到了也先刷新一个");
    assert_eq!(spent.used(), 1);
    let mut count = super::RefreshBudget::new(2, Duration::from_secs(60), Instant::now());
    let got: Vec<_> = ["A", "B", "C"]
        .iter()
        .map(|key| super::cached_history(&mut cache, *key, 60, &mut count, || 2))
        .collect();
    assert_eq!(got, [Some(1), Some(2), Some(2)], "A 这一分钟算过，不占额度");
    assert_eq!(count.used(), 2);
}

#[test]
fn a_lost_lighter_book_is_resubscribed_after_a_minute_without_churning() {
    let start = Instant::now();
    let market = "SOXL".to_string();
    let id = (Venue::Lighter, market.clone());
    let active: ActiveBooks = [(Venue::Lighter, [market.clone()].into_iter().collect())].into();
    let mut had = std::collections::HashSet::new();
    let mut missing = HashMap::new();
    let check = |at: Instant, present: bool, had: &mut _, missing: &mut _| {
        books_to_resubscribe(
            at,
            [Venue::Lighter].into_iter(),
            &active,
            &|_| present,
            had,
            missing,
        )
    };
    // 还没收到过盘口（刚订阅）：不管。
    assert!(check(start, false, &mut had, &mut missing).is_empty());
    // 有过盘口，然后没了：不到一分钟不催，满一分钟申请一次，之后再隔一分钟才再申请。
    assert!(check(start, true, &mut had, &mut missing).is_empty());
    assert!(check(start, false, &mut had, &mut missing).is_empty());
    assert!(
        check(
            start + Duration::from_secs(59),
            false,
            &mut had,
            &mut missing
        )
        .is_empty()
    );
    assert_eq!(
        check(
            start + Duration::from_secs(60),
            false,
            &mut had,
            &mut missing
        ),
        vec![id.clone()]
    );
    assert!(
        check(
            start + Duration::from_secs(61),
            false,
            &mut had,
            &mut missing
        )
        .is_empty()
    );
    assert_eq!(
        check(
            start + Duration::from_secs(120),
            false,
            &mut had,
            &mut missing
        ),
        vec![id]
    );
    // 盘口回来了：计时清掉。
    assert!(
        check(
            start + Duration::from_secs(121),
            true,
            &mut had,
            &mut missing
        )
        .is_empty()
    );
    assert!(missing.is_empty());
}

/// 面板只取用得上的那部分：按组 / 合约 / 只看信号筛、最多 N 行；统计数的是筛出来的全部行，
/// 不带参数（`PanelFilter::full()`）还是整份。
#[tokio::test]
async fn panel_filters_and_caps_rows_on_the_server() {
    let other = pair(Venue::Arcus, Venue::HyperliquidXyz);
    let watched = pair(Venue::Arcus, Venue::Gate);
    let config = Config {
        watch_pairs: vec![watched],
        ..config()
    };
    let monitor = Monitor::new(config.clone(), crate::alert::Alerter::with_sink(None));
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.30), dec!(100))],
        &[(dec!(100.32), dec!(100))],
        now,
    );
    let row = |pair: Pair, base: &str, normal_pct: f64| {
        let mut m = market("INDICES");
        m.base = base.into();
        evaluate(
            pair,
            &m,
            Some(&arcus),
            Some(&lighter),
            Some(normal(normal_pct)),
            500,
            Session::Off,
            &config,
            now,
        )
    };
    {
        let mut view = monitor.view.write().await;
        view.lines = [
            row(Pair::RH, "SPY", -0.05),
            row(Pair::RH, "QQQ", -0.3),
            row(other, "SPY", -0.3),
            row(other, "NVDA", -0.05),
        ]
        .map(Arc::new)
        .to_vec();
        let mut watch = monitor.watch.write().await;
        watch.lines = vec![Arc::new(row(watched, "SPY", -0.05))];
    }
    let signals = |panel: &PanelView| {
        panel
            .view
            .lines
            .iter()
            .filter(|line| panel_signal(line))
            .count()
    };

    // 不带参数：整份（脚本、排查用），和原来的 panel_view 一样。
    let full = monitor.panel(&PanelFilter::full()).await;
    assert_eq!(full.view.lines.len(), 4);
    assert_eq!(full.watch.lines.len(), 1);
    assert!(full.latency.is_some());
    assert_eq!(
        full.counts,
        PanelCounts {
            lines: 5,
            signals: 2,
            shown: 5
        },
        "观察组的行不算信号"
    );
    assert_eq!(signals(&full), 2);

    // 页面默认：不带观察组的行、最多 N 行；统计仍是全部。
    let capped = monitor
        .panel(&PanelFilter {
            limit: Some(3),
            ..PanelFilter::default()
        })
        .await;
    assert_eq!(capped.view.lines.len(), 3);
    assert!(capped.watch.lines.is_empty());
    assert!(capped.latency.is_none());
    assert_eq!(
        capped.counts,
        PanelCounts {
            lines: 4,
            signals: 2,
            shown: 3
        }
    );
    let bases: Vec<&str> = capped.view.lines.iter().map(|l| l.base.as_str()).collect();
    assert_eq!(bases, ["SPY", "QQQ", "SPY"], "保持快照里的顺序，取前 N 行");

    // 只看信号：行数预算只花在信号行上。
    let signal_only = monitor
        .panel(&PanelFilter {
            signal_only: true,
            limit: Some(1),
            ..PanelFilter::default()
        })
        .await;
    assert_eq!(signal_only.view.lines.len(), 1);
    assert_eq!(signal_only.view.lines[0].base, "SPY");
    assert_eq!(signal_only.counts.signals, 2);

    // 按组、按合约（不分大小写）。
    let by_pair = monitor
        .panel(&PanelFilter {
            pair: Some(other.id()),
            ..PanelFilter::default()
        })
        .await;
    assert_eq!(by_pair.view.lines.len(), 2);
    assert!(by_pair.view.lines.iter().all(|l| l.pair == other.id()));
    assert_eq!(by_pair.counts.lines, 2);
    let by_base = monitor
        .panel(&PanelFilter {
            base: Some("spy".into()),
            watch: true,
            ..PanelFilter::default()
        })
        .await;
    assert_eq!(by_base.view.lines.len(), 2);
    assert_eq!(by_base.watch.lines.len(), 1);
    assert_eq!(by_base.counts.shown, 3);
    assert!(by_base.view.maker.is_none());

    // 行数预算在交易组和观察组之间共用，交易组在前。
    let shared = monitor
        .panel(&PanelFilter {
            watch: true,
            limit: Some(4),
            ..PanelFilter::default()
        })
        .await;
    assert_eq!(shared.view.lines.len(), 4);
    assert!(shared.watch.lines.is_empty());
    assert_eq!(shared.counts.lines, 5);
}

/// 两个方向一起估，和分别估一模一样（含深度不够、盘口交叉、多档成交）。
#[test]
fn quoting_both_directions_at_once_matches_quoting_each() {
    let now = Instant::now();
    let deep_a = book(
        &[
            (dec!(99.90), dec!(1)),
            (dec!(99.95), dec!(2)),
            (dec!(99.98), dec!(3)),
        ],
        &[
            (dec!(100.00), dec!(1)),
            (dec!(100.07), dec!(2)),
            (dec!(100.11), dec!(50)),
        ],
        now,
    );
    let deep_b = book(
        &[(dec!(100.21), dec!(4)), (dec!(100.30), dec!(1.5))],
        &[(dec!(100.32), dec!(0.7)), (dec!(100.41), dec!(9))],
        now,
    );
    let thin = book(
        &[(dec!(100.1), dec!(0.5))],
        &[(dec!(100.2), dec!(0.5))],
        now,
    );
    let one_sided = book(&[(dec!(100.1), dec!(50))], &[], now);
    for (a, b) in [
        (&deep_a, &deep_b),
        (&deep_b, &deep_a),
        (&deep_a, &thin),
        (&thin, &deep_b),
        (&deep_a, &one_sided),
    ] {
        for size in [dec!(0), dec!(100), dec!(400), dec!(1000), dec!(100000)] {
            let both = book::quote_both(a, b, size);
            assert_eq!(
                both.as_ref().map(|q| q.0.clone()),
                quote(a, b, size),
                "{size}"
            );
            assert_eq!(both.map(|q| q.1), quote(b, a, size), "{size}");
        }
    }
}

/// 增量排序（只挪换过的行）和整个重排的结果一样：多轮、随机换掉一部分行、行数变化都对得上。
#[test]
fn sorting_only_the_replaced_rows_matches_a_plain_sort() {
    let now = Instant::now();
    let arcus = book(
        &[(dec!(99.98), dec!(100))],
        &[(dec!(100.00), dec!(100))],
        now,
    );
    let lighter = book(
        &[(dec!(100.30), dec!(100))],
        &[(dec!(100.32), dec!(100))],
        now,
    );
    let config = config();
    let line = |pair: &str, base: &str, net: Option<Decimal>| {
        let mut m = market("INDICES");
        m.base = base.into();
        let mut line = evaluate(
            Pair::RH,
            &m,
            Some(&arcus),
            Some(&lighter),
            Some(normal(-0.3)),
            500,
            Session::Off,
            &config,
            now,
        );
        line.pair = pair.into();
        line.best = Some(Best {
            direction: "long_a",
            signal: false,
            signal_sec: 0,
            net_usdt: net,
        });
        Arc::new(line)
    };
    let names = |lines: &[Arc<Line>]| -> Vec<String> {
        lines
            .iter()
            .map(|l| format!("{}/{}", l.pair, l.base))
            .collect()
    };
    // 简单的线性同余随机数：测试要可重复。
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut random = |below: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % below
    };
    let net = |random: &mut dyn FnMut(u64) -> u64| match random(5) {
        // 一部分行没有净收益、一部分并列：要靠组、合约兜底。
        0 => None,
        1 => Some(dec!(0.5)),
        _ => Some(Decimal::new(random(2000) as i64 - 1000, 2)),
    };
    let mut rank = Vec::new();
    for count in [0usize, 1, 2, 60, 60, 61, 300] {
        let name = |i: usize| (format!("p{}", i % 7), format!("B{i:03}"));
        let mut built: Vec<Arc<Line>> = (0..count)
            .map(|i| line(&name(i).0, &name(i).1, net(&mut random)))
            .collect();
        for round in 0..30 {
            // 第一轮全是新的；之后每轮随机换掉一部分（偶尔一行不换、偶尔全换）。
            let share = [0, 3, 3, 10, 50, 100][random(6) as usize];
            let fresh: Vec<bool> = (0..count)
                .map(|i| {
                    let replace = round == 0 || random(100) < share;
                    if replace && round > 0 {
                        built[i] = line(&name(i).0, &name(i).1, net(&mut random));
                    }
                    replace
                })
                .collect();
            let mut plain = built.clone();
            plain.sort_by(|a, b| line_order(a, b));
            let mut ranked = built.clone();
            sort_lines(&mut ranked, &fresh, &mut rank);
            assert_eq!(names(&ranked), names(&plain), "{count} 行，第 {round} 轮");
            assert!(ranked.iter().zip(&plain).all(|(a, b)| Arc::ptr_eq(a, b)));
            assert_eq!(rank.len(), count);
        }
    }
}

// ───────────────────────── Hyperliquid 只订 bbo（不用 l2Book） ─────────────────────────

fn bbo_event(
    market: &str,
    bid: (Decimal, Decimal),
    ask: (Decimal, Decimal),
    seed: bool,
) -> feed::Event {
    feed::Event::Bbo {
        market: market.into(),
        bid: Some(bid),
        ask: Some(ask),
        seed,
    }
}

fn stamped(source_ms: i64) -> latency::Stamp {
    let mut stamp = latency::Stamp::now();
    stamp.source_ms = Some(source_ms);
    stamp
}

#[test]
fn a_bbo_push_and_a_seed_reply_parse_into_a_one_level_event_with_the_exchange_time() {
    let push = r#"{"channel":"bbo","data":{"coin":"xyz:TSLA","time":1760000000123,"bbo":[{"px":"411.2","sz":"3.5","n":2},{"px":"411.4","sz":"1.0","n":1}]}}"#;
    assert_eq!(
        feed::parse_hyperliquid_timed(push).unwrap(),
        (
            bbo_event(
                "xyz:TSLA",
                (dec!(411.2), dec!(3.5)),
                (dec!(411.4), dec!(1.0)),
                false
            ),
            Some(1_760_000_000_123)
        )
    );
    // 某一侧没有挂单是 null，不是错误；没有时间戳就没法和已有的排先后：拒收。
    let one_sided =
        r#"{"channel":"bbo","data":{"coin":"x","time":5,"bbo":[{"px":"1","sz":"2","n":1},null]}}"#;
    let (event, source) = feed::parse_hyperliquid_timed(one_sided).unwrap();
    assert_eq!(source, Some(5));
    assert!(matches!(event, feed::Event::Bbo { ask: None, .. }));
    assert!(
        feed::parse_hyperliquid_timed(r#"{"channel":"bbo","data":{"coin":"x","bbo":[null,null]}}"#)
            .is_err()
    );

    // 订阅后取初值：只取最优一档，不拿它的深度。
    let seed = r#"{"channel":"post","data":{"id":1000001,"response":{"type":"info","payload":{"type":"l2Book","data":{"coin":"xyz:TSLA","time":1760000000100,"levels":[[{"px":"411.2","sz":"3.5","n":1},{"px":"411.1","sz":"9","n":1}],[{"px":"411.4","sz":"1.0","n":1},{"px":"411.5","sz":"9","n":1}]]}}}}}"#;
    assert_eq!(
        feed::parse_hyperliquid_timed(seed).unwrap(),
        (
            bbo_event(
                "xyz:TSLA",
                (dec!(411.2), dec!(3.5)),
                (dec!(411.4), dec!(1.0)),
                true
            ),
            Some(1_760_000_000_100)
        )
    );
    // 下单类 / 错误的 post 应答不是初值。
    for other in [
        r#"{"channel":"post","data":{"id":1,"response":{"type":"error","payload":"429"}}}"#,
        r#"{"channel":"post","data":{"id":1,"response":{"type":"action","payload":{"status":"ok"}}}}"#,
    ] {
        assert_eq!(
            feed::parse_hyperliquid_timed(other).unwrap().0,
            feed::Event::Other
        );
    }
}

#[test]
fn bbo_mode_keeps_one_level_replaces_it_whole_and_feeds_the_live_book() {
    let _guard = arb_venues::live_book::test_lock();
    let venue = Venue::HyperliquidXyz;
    let key = (venue, "xyz:BBOMODE".to_string());
    let mut books = HashMap::new();
    // 不需要先有整本快照：第一条（初值或推送）就建出这本盘口。
    assert!(apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:BBOMODE",
            (dec!(10), dec!(5)),
            (dec!(10.2), dec!(4)),
            true
        ),
        None,
        stamped(1000),
    ));
    assert_eq!(books[&key].best_bid(), Some(dec!(10)));
    // 买一 10 被吃光，新买一 9.9：旧的 10 不能留成第二档（整体替换，没有幻影深度）。
    assert!(apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:BBOMODE",
            (dec!(9.9), dec!(2)),
            (dec!(10.2), dec!(4)),
            false
        ),
        None,
        stamped(2000),
    ));
    let book = &books[&key];
    assert_eq!((book.bids.len(), book.asks.len()), (1, 1));
    assert_eq!(book.best_bid(), Some(dec!(9.9)));
    // 实时顶档同步给了下单报价与开仓核算（有连接心跳才可信）。
    arb_venues::live_book::heartbeat(venue, Instant::now());
    let (bid, ask) = arb_venues::live_book::top(venue, "BBOMODE", Duration::from_secs(6))
        .expect("顶档推送之后可信");
    assert_eq!((bid, ask), ((dec!(9.9), dec!(2)), (dec!(10.2), dec!(4))));
    arb_venues::live_book::remove_venue(venue);
}

#[test]
fn a_late_seed_reply_never_overwrites_a_newer_push() {
    let _guard = arb_venues::live_book::test_lock();
    let venue = Venue::HyperliquidXyz;
    let key = (venue, "xyz:SEEDORDER".to_string());
    let mut books = HashMap::new();
    arb_venues::live_book::heartbeat(venue, Instant::now());
    // bbo 推送（时间 2000）先到；info 查询的初值（时间 1900，读节点落后）后到：不能盖掉。
    assert!(apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(10), dec!(1)),
            (dec!(10.2), dec!(1)),
            false
        ),
        None,
        stamped(2000),
    ));
    assert!(!apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(9), dec!(1)),
            (dec!(9.2), dec!(1)),
            true
        ),
        None,
        stamped(1900),
    ));
    assert_eq!(books[&key].best_bid(), Some(dec!(10)));
    let (bid, _) = arb_venues::live_book::top(venue, "SEEDORDER", Duration::from_secs(6)).unwrap();
    assert_eq!(bid.0, dec!(10), "下单报价与开仓核算读到的也是新的");
    // 同一时间戳、内容相同：重复，不刷新；内容不同：保留原本。
    assert!(!apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(10), dec!(1)),
            (dec!(10.2), dec!(1)),
            false
        ),
        None,
        stamped(2000),
    ));
    assert!(!apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(10.1), dec!(1)),
            (dec!(10.3), dec!(1)),
            false
        ),
        None,
        stamped(2000),
    ));
    assert_eq!(books[&key].best_bid(), Some(dec!(10)));
    // 没有交易所时间戳：没法排先后，不收。
    assert!(!apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(11), dec!(1)),
            (dec!(11.2), dec!(1)),
            false
        ),
        None,
        latency::Stamp::now(),
    ));
    // 之后一条更新的重取、顶档变了（中间漏了推送）：按重取更新。
    assert!(apply_at(
        &mut books,
        venue,
        bbo_event(
            "xyz:SEEDORDER",
            (dec!(10.4), dec!(1)),
            (dec!(10.6), dec!(1)),
            true
        ),
        None,
        stamped(3000),
    ));
    assert_eq!(books[&key].best_bid(), Some(dec!(10.4)));
    arb_venues::live_book::remove_venue(venue);
}

#[test]
fn only_arcus_is_judged_by_snapshot_age_when_hyperliquid_runs_on_bbo() {
    // 默认（bbo）：安静的市场几十秒没有推送是常态，Hyperliquid 和 Lighter 一样不按年龄判过期。
    assert_eq!(
        feed::hyperliquid_book_mode(),
        feed::HlBook::Bbo,
        "默认不用 l2Book"
    );
    assert!(snapshot_feed(Venue::Arcus));
    for venue in [
        Venue::Hyperliquid,
        Venue::HyperliquidXyz,
        Venue::HyperliquidIo,
    ] {
        assert!(!snapshot_feed_in(venue, feed::HlBook::Bbo), "{venue}");
        // `ARB_HL_BOOK=l2` 回到参考树原行为：整本快照，按年龄判。
        assert!(snapshot_feed_in(venue, feed::HlBook::L2), "{venue}");
    }
    assert!(!snapshot_feed(Venue::LighterRh));
}
