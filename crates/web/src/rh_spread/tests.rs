use super::history::History;
use super::*;

/// 手动联网核对（默认忽略）：`cargo test -p arb-web live_probe -- --ignored --nocapture`。
#[tokio::test]
#[ignore]
async fn live_probe() {
    let dir = std::env::temp_dir().join(format!("rh-spread-probe-{}", std::process::id()));
    let config = Config {
        enabled: true,
        size_usdt: Decimal::from(2000),
        alert_net_pct: Decimal::new(5, 2),
        equities_only: false,
        dir: dir.clone(),
        pairs: Pair::parse_list(pairs::DEFAULT_PAIRS).unwrap(),
        entropy_rebate: Decimal::TWO,
    };
    let monitor = Monitor::new(config, crate::alert::Alerter::with_sink(None));
    let client = arb_venues::build_client(20).unwrap();
    // 先跑一轮扫描给 Hyperliquid 的组核对身份。
    let settings = arb_core::Settings::from_env().unwrap();
    let apis = arb_venues::build_all(&settings, &client);
    let cache = Arc::new(crate::cache::ScanCache::default());
    cache.put(arb_scanner::scan(&apis, &settings).await).await;
    monitor.spawn(client, cache);
    tokio::time::sleep(Duration::from_secs(90)).await;
    let view = monitor.view().await;
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
    for line in view.lines.iter().take(80) {
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
        entropy_rebate: Decimal::ZERO,
    }
}

/// Arcus（a，吃单 0.0225%）↔ Lighter RH（b，0）的 SPY。
fn market(category: &str) -> PairMarket {
    market_with(category, Some(dec!(0.000225)))
}

fn market_with(category: &str, arcus_fee: Option<Decimal>) -> PairMarket {
    let venue_market = |key: &str, fee| pairs::VenueMarket {
        key: key.into(),
        taker_fee: fee,
        category: None,
        outside_rth: None,
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
    let text = alert_text(&line, &best, &cfg).unwrap();
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
    assert_eq!(session::new_york_offset_hours(at(2026, 3, 8, 6, 59)), -5);
    assert_eq!(session::new_york_offset_hours(at(2026, 3, 8, 7, 0)), -4);
    assert_eq!(session::new_york_offset_hours(at(2026, 11, 1, 5, 59)), -4);
    assert_eq!(session::new_york_offset_hours(at(2026, 11, 1, 6, 0)), -5);
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
        {"marketDisplayName": "SPY-USD", "baseAsset": "SPY", "status": "ONLINE", "type": "PERPETUAL", "category": "INDICES", "isOutsideRth": true},
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
    catalog.insert(Venue::LighterRh, catalog::lighter(&lighter).unwrap());
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
    assert!(Pair::parse("binance:arcus").is_err());
    assert!(Pair::parse("arcus:arcus").is_err());
    assert!(Pair::parse_list("arcus:lighter-rh,lighter-rh:arcus").is_err());
    assert_eq!(Pair::parse_list(pairs::DEFAULT_PAIRS).unwrap()[0], Pair::RH);
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
    assert_eq!(
        feed::hyperliquid_subscribe("io:ANTH"),
        r#"{"method":"subscribe","subscription":{"coin":"io:ANTH","type":"l2Book"}}"#
    );
}

#[test]
fn a_pair_with_two_snapshot_feeds_goes_stale_on_the_older_book() {
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
    assert!(line.best.is_none());
    assert!(
        line.note.unwrap().contains("hyperliquid-xyz"),
        "说清楚是哪家过期"
    );
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
    monitor.maybe_alert(&line);
    tokio::task::yield_now().await;
    assert!(sink.0.lock().unwrap().is_empty(), "刚亮的信号不推");
    line.best.as_mut().unwrap().signal_sec = SIGNAL_HOLD.as_secs();
    for base in ["SPY", "QQQ", "NVDA", "AMZN"] {
        line.base = base.into();
        monitor.maybe_alert(&line);
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
