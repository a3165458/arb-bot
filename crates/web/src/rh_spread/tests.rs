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
    };
    let monitor = Monitor::new(config, crate::alert::Alerter::with_sink(None));
    let client = arb_venues::build_client(20).unwrap();
    monitor.spawn(client);
    tokio::time::sleep(Duration::from_secs(90)).await;
    let view = monitor.view().await;
    println!(
        "connected={:?} fee={:?} error={:?} lines={}",
        view.connected,
        view.fee_round_trip_pct,
        view.error,
        view.lines.len()
    );
    for line in view.lines.iter().take(40) {
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
            "{:8} {:6} basis {:>9} age {:?} | A {} | L {} | note {:?}",
            line.base,
            line.session.label(),
            line.basis_pct.map(|b| b.to_string()).unwrap_or("-".into()),
            line.age_sec,
            leg(&line.long_arcus),
            leg(&line.long_lighter),
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
    }
}

fn market(category: &str) -> Market {
    Market {
        base: "SPY".into(),
        lighter_id: 26,
        arcus_name: "SPY-USD".into(),
        category: category.into(),
        outside_rth: Some(true),
    }
}

const FEES: Fees = Fees {
    arcus_taker: Decimal::from_parts(225, 0, 0, false, 6),
    lighter_taker: Decimal::ZERO,
};

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
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(FEES),
        None,
        30,
        Session::Off,
        &cfg,
        now,
    );
    let leg = line.long_arcus.clone().unwrap();
    assert_eq!(
        leg.net_to_zero_pct,
        dec!(0.1998) - dec!(0.01998) - dec!(0.045)
    );
    assert_eq!(leg.net_to_normal_pct, None);
    assert_eq!(line.normal_missing_minutes, history::MIN_MINUTES - 30);
    let best = line.best.unwrap();
    assert_eq!(best.direction, "long_arcus");
    assert!(!best.signal, "没有正常水平不提醒");

    // 正常基差就是 −0.2%（休市时 Arcus 一直便宜）：回到正常水平几乎什么都赚不到。
    let line = evaluate(
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(FEES),
        Some(normal(-0.2)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let leg = line.long_arcus.clone().unwrap();
    assert_eq!(
        leg.net_to_normal_pct,
        Some((leg.net_to_zero_pct - dec!(0.2)).round_dp(5))
    );
    assert!(!line.best.unwrap().signal, "系统性偏差不是机会");

    // 正常基差 −0.05%：现在偏到 −0.22%，回到 −0.05% 净赚 ≈ 0.085%，超过门槛 0.05%。
    let line = evaluate(
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(FEES),
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
        "不会自动下单",
    ] {
        assert!(text.contains(part), "{part} 不在：{text}");
    }

    // 反方向：Lighter 便宜时选多 Lighter；正常基差的符号反过来用。
    let line = evaluate(
        &m,
        Some(&lighter),
        Some(&arcus),
        Some(FEES),
        Some(normal(0.05)),
        500,
        Session::Off,
        &cfg,
        now,
    );
    let best = line.best.unwrap();
    assert_eq!(best.direction, "long_lighter");
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
        &m,
        Some(&arcus),
        Some(&lighter),
        Some(FEES),
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
        &m,
        Some(&old),
        Some(&lighter),
        Some(FEES),
        Some(normal(0.0)),
        500,
        Session::Off,
        &cfg,
        later,
    );
    assert!(stale.best.is_none() && stale.note.unwrap().contains("没更新"));
    let missing = evaluate(
        &m,
        None,
        Some(&lighter),
        Some(FEES),
        None,
        0,
        Session::Off,
        &cfg,
        now,
    );
    assert!(missing.best.is_none() && missing.basis_pct.is_none());
    let no_fee = evaluate(
        &m,
        Some(&old),
        Some(&lighter),
        None,
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
    history::append(&dir, &rows, now).await.unwrap();
    let path = history::file_for(&dir, now.date_naive());
    let mut text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("\"el\""), "缺的字段不写");
    text.push_str("{broken\n");
    std::fs::write(&path, text).unwrap();
    // 超过保留期的旧文件在下次写入时删掉。
    let ancient = history::file_for(
        &dir,
        (now - chrono::Duration::days(history::KEEP_DAYS + 3)).date_naive(),
    );
    std::fs::write(&ancient, "").unwrap();
    history::append(&dir, &rows[..1], now).await.unwrap();
    assert!(!ancient.exists());
    let (loaded, broken) = history::load(&dir, now).await;
    assert_eq!((loaded.minutes("QQQ", Session::Off, now), broken), (3, 1));
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
    let index: HashMap<String, usize> = [("26".to_string(), 0)].into();
    let mut books = vec![None];
    let (tx, mut rx) = mpsc::channel(4);
    apply(
        &mut books,
        &index,
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
        &index,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![(dec!(1.5), dec!(1))],
            asks: vec![],
            begin_nonce: Some(5),
            nonce: Some(7),
        },
        Some(&tx),
    );
    assert_eq!(books[0].as_ref().unwrap().best_bid(), Some(dec!(1.5)));
    apply(
        &mut books,
        &index,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![],
            asks: vec![],
            begin_nonce: Some(9),
            nonce: Some(10),
        },
        Some(&tx),
    );
    assert!(books[0].is_none(), "丢了增量的盘口不能再用");
    assert_eq!(rx.try_recv().unwrap(), 26);
    // 没有快照之前的增量忽略。
    apply(
        &mut books,
        &index,
        feed::Event::Delta {
            market: "26".into(),
            bids: vec![(dec!(1), dec!(1))],
            asks: vec![],
            begin_nonce: Some(10),
            nonce: Some(11),
        },
        Some(&tx),
    );
    assert!(books[0].is_none());
}

#[test]
fn discovery_matches_online_perps_by_base_and_keeps_fees_honest() {
    let arcus = serde_json::json!({"markets": [
        {"marketDisplayName": "SPY-USD", "baseAsset": "SPY", "status": "ONLINE", "type": "PERPETUAL", "category": "INDICES", "isOutsideRth": true},
        {"marketDisplayName": "BTC-USD", "baseAsset": "BTC", "status": "ONLINE", "type": "PERPETUAL", "category": "CRYPTO", "isOutsideRth": null},
        {"marketDisplayName": "F-USD", "baseAsset": "F", "status": "OFFLINE", "type": "PERPETUAL", "category": "EQUITIES"},
        {"marketDisplayName": "XBT-USD", "baseAsset": "QQQ", "status": "ONLINE", "type": "PERPETUAL", "category": "INDICES"}
    ]});
    let lighter = serde_json::json!({"order_book_details": [
        {"symbol": "SPY", "market_id": 26, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "BTC", "market_id": 1, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "F", "market_id": 3, "status": "active", "market_type": "perp", "taker_fee": "0.0000"},
        {"symbol": "QQQ", "market_id": 4, "status": "active", "market_type": "perp", "taker_fee": "0.0000"}
    ]});
    let (markets, fees) = discover_from(&arcus, dec!(0.000225), &lighter, false).unwrap();
    assert_eq!(
        markets.iter().map(|m| m.base.as_str()).collect::<Vec<_>>(),
        ["BTC", "SPY"]
    );
    assert_eq!(markets[1].outside_rth, Some(true));
    assert_eq!(fees.round_trip_pct(), dec!(0.045));
    let (equities, _) = discover_from(&arcus, dec!(0.000225), &lighter, true).unwrap();
    assert_eq!(equities.len(), 1);
    // Lighter 没报费率：不当 0，按 Arcus 费率保守估。
    let unknown = serde_json::json!({"order_book_details": [{"symbol": "SPY", "market_id": 26, "status": "active"}]});
    let (_, fees) = discover_from(&arcus, dec!(0.000225), &unknown, false).unwrap();
    assert_eq!(fees.round_trip_pct(), dec!(0.090));
    assert!(discover_from(&arcus, dec!(0.5), &lighter, false).is_err());
    assert!(
        discover_from(
            &serde_json::json!({"markets": []}),
            dec!(0.000225),
            &lighter,
            false
        )
        .is_err()
    );
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
        &market("INDICES"),
        Some(&arcus),
        Some(&lighter),
        Some(FEES),
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
