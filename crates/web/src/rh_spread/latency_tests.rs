use super::*;

fn snapshot(price: i64) -> feed::Event {
    feed::Event::Snapshot {
        market: "xyz:TSLA".into(),
        bids: vec![(Decimal::from(price), Decimal::ONE)],
        asks: vec![(Decimal::from(price + 1), Decimal::ONE)],
        nonce: None,
    }
}
fn stamp(at: Instant, source: i64) -> latency::Stamp {
    latency::Stamp {
        received: at,
        enqueued: at,
        utc_ms: source + 1,
        source_ms: Some(source),
    }
}

#[test]
fn old_duplicate_and_conflicting_snapshots_cannot_roll_back_or_refresh_a_book() {
    let mut books = HashMap::new();
    let at = Instant::now() - Duration::from_secs(20);
    let venue = Venue::HyperliquidXyz;
    assert!(apply_at(
        &mut books,
        venue,
        snapshot(100),
        None,
        stamp(at, 2000)
    ));
    assert!(!apply_at(
        &mut books,
        venue,
        snapshot(99),
        None,
        stamp(Instant::now(), 1999)
    ));
    assert!(!apply_at(
        &mut books,
        venue,
        snapshot(100),
        None,
        stamp(Instant::now(), 2000)
    ));
    assert!(!apply_at(
        &mut books,
        venue,
        snapshot(101),
        None,
        stamp(Instant::now(), 2000)
    ));
    let mut missing_time = stamp(Instant::now(), 2001);
    missing_time.source_ms = None;
    assert!(!apply_at(
        &mut books,
        venue,
        snapshot(50),
        None,
        missing_time
    ));
    let b = &books[&(venue, "xyz:TSLA".into())];
    assert_eq!(b.best_bid(), Some(Decimal::from(100)));
    assert_eq!(
        b.updated, at,
        "neither queue delay nor duplicates may make old data look fresh"
    );
    assert!(b.updated.elapsed() > STALE);
    assert!(apply_at(
        &mut books,
        venue,
        snapshot(102),
        None,
        stamp(Instant::now(), 2001)
    ));
    assert_eq!(
        books[&(venue, "xyz:TSLA".into())].source_time_ms,
        Some(2001)
    );
    assert!(
        apply_at(
            &mut books,
            Venue::HyperliquidIo,
            snapshot(50),
            None,
            stamp(at, 1)
        ),
        "logical dexes cannot share version fences"
    );
}

#[test]
fn active_snapshot_feeds_get_a_short_coalescing_window_but_observers_stay_at_500ms() {
    for venue in [Venue::Bitget, Venue::Mexc, Venue::Gate, Venue::PopDex] {
        assert_eq!(
            protocol_for_role(venue, Stream::Books, true)
                .unwrap()
                .coalesce
                .unwrap()
                .1,
            TRADE_COALESCE
        );
        assert_eq!(
            protocol_for_role(venue, Stream::Books, false)
                .unwrap()
                .coalesce
                .unwrap()
                .1,
            Duration::from_millis(500)
        );
    }
    assert!(
        protocol_for_role(Venue::Lighter, Stream::Books, true)
            .unwrap()
            .coalesce
            .is_none(),
        "never coalesce incremental frames"
    );
    assert!(
        protocol_for_role(Venue::HyperliquidXyz, Stream::Books, true)
            .unwrap()
            .coalesce
            .is_none()
    );
}

#[tokio::test]
async fn forward_retains_native_source_time_and_original_receive_time_through_a_queue() {
    let (tx, mut rx) = mpsc::channel(4);
    let protocol = protocol(Venue::HyperliquidXyz, Stream::Books).unwrap();
    let received = Instant::now() - Duration::from_millis(500);
    let original = latency::Stamp {
        received,
        enqueued: received,
        utc_ms: 2001,
        source_ms: None,
    };
    let text = r#"{"channel":"l2Book","data":{"coin":"xyz:TSLA","time":2000,"levels":[[{"px":"100","sz":"2"}],[{"px":"101","sz":"2"}]]}}"#;
    assert_eq!(
        forward(Venue::HyperliquidXyz, &protocol, text, original, &tx).await,
        Forwarded::Data
    );
    let Feed::Book(venue, event, saved) = rx.recv().await.unwrap() else {
        panic!("not a book");
    };
    assert_eq!(saved.received, received);
    assert_eq!(saved.source_ms, Some(2000));
    let mut books = HashMap::new();
    assert!(apply_at(&mut books, venue, event, None, saved));
    assert_eq!(books[&(venue, "xyz:TSLA".into())].updated, received);
    assert_eq!(
        forward(venue, &protocol, r#"{"channel":"pong"}"#, original, &tx).await,
        Forwarded::Other,
        "心跳回执不算行情数据"
    );
    assert!(
        rx.try_recv().is_err(),
        "heartbeats cannot trigger recalculation or refresh data"
    );
}

#[test]
fn producer_time_is_validated_without_repurposing_the_incremental_nonce() {
    let text = r#"{"channel":"l2Book","data":{"coin":"BTC","time":12345,"levels":[[],[]]}}"#;
    let (event, time) = feed::parse_hyperliquid_timed(text).unwrap();
    assert_eq!(time, Some(12345));
    assert!(matches!(event, feed::Event::Snapshot { nonce: None, .. }));
    for invalid in ["0", "-1", "1.5", "null", "\"12345\""] {
        assert!(feed::parse_hyperliquid_timed(&text.replace("12345", invalid)).is_err());
    }
    assert_eq!(
        feed::parse_hyperliquid_timed(&text.replace("\"time\":12345,", ""))
            .unwrap()
            .1,
        None
    );
    let fast: Value = serde_json::from_str(&feed::hyperliquid_fast_subscribe("xyz:TSLA")).unwrap();
    assert_eq!(fast["subscription"]["fast"], true);
    let slow: Value = serde_json::from_str(&feed::hyperliquid_subscribe("xyz:TSLA")).unwrap();
    assert!(
        slow["subscription"].get("fast").is_none(),
        "primary subscriptions must remain unchanged during shadow evaluation"
    );
}

fn levels(prices: &[i64]) -> Vec<(Decimal, Decimal)> {
    prices
        .iter()
        .map(|p| (Decimal::from(*p), Decimal::ONE))
        .collect()
}
fn full(bids: &[i64], asks: &[i64]) -> feed::Event {
    feed::Event::Snapshot {
        market: "xyz:TSLA".into(),
        bids: levels(bids),
        asks: levels(asks),
        nonce: None,
    }
}
fn top(bids: &[i64], asks: &[i64]) -> feed::Event {
    feed::Event::Top {
        market: "xyz:TSLA".into(),
        bids: levels(bids),
        asks: levels(asks),
    }
}
fn prices(side: &std::collections::BTreeMap<Decimal, Decimal>) -> Vec<i64> {
    side.keys()
        .map(|p| p.to_string().parse().unwrap())
        .collect()
}

#[test]
fn fast_frames_are_parsed_as_top_of_book_and_need_a_time() {
    let text = r#"{"channel":"l2Book","data":{"coin":"xyz:TSLA","time":2000,"levels":[[{"px":"100","sz":"2"}],[{"px":"101","sz":"2"}]],"fast":true}}"#;
    let (event, time) = feed::parse_hyperliquid_timed(text).unwrap();
    assert_eq!(time, Some(2000));
    assert!(matches!(event, feed::Event::Top { .. }), "{event:?}");
    assert!(feed::parse_hyperliquid_timed(&text.replace("\"time\":2000,", "")).is_err());
    let slow = text.replace(",\"fast\":true", "");
    assert!(matches!(
        feed::parse_hyperliquid_timed(&slow).unwrap().0,
        feed::Event::Snapshot { .. }
    ));
}

#[test]
fn a_fast_top_refreshes_the_front_of_the_book_and_keeps_the_depth() {
    let venue = Venue::HyperliquidXyz;
    let key = (venue, "xyz:TSLA".to_string());
    let at = Instant::now();
    let mut books = HashMap::new();
    // 还没有整本：5 档不能当整本。
    assert!(!apply_at(
        &mut books,
        venue,
        top(&[99, 98, 97, 96, 95], &[101, 102, 103, 104, 105]),
        None,
        stamp(at, 900)
    ));
    assert!(books.is_empty());

    let bids: Vec<i64> = (81..=100).rev().collect();
    let asks: Vec<i64> = (101..=120).collect();
    assert!(apply_at(
        &mut books,
        venue,
        full(&bids, &asks),
        None,
        stamp(at, 1000)
    ));
    // 同一时刻的 fast（和整本一致）：不算更新。
    assert!(!apply_at(
        &mut books,
        venue,
        top(&[100, 99, 98, 97, 96], &[101, 102, 103, 104, 105]),
        None,
        stamp(at, 1000)
    ));

    // 买盘顶上 100、98 没了，多了 99.5 的价位不存在（整数价）——新顶部 99,97,96,95,94；卖盘上移到 102 起。
    let later = at + Duration::from_millis(500);
    assert!(apply_at(
        &mut books,
        venue,
        top(&[99, 97, 96, 95, 94], &[102, 103, 104, 105, 107]),
        None,
        stamp(later, 1500)
    ));
    let book = &books[&key];
    assert_eq!(book.best_bid(), Some(Decimal::from(99)));
    assert_eq!(book.best_ask(), Some(Decimal::from(102)));
    let b = prices(&book.bids);
    assert!(
        !b.contains(&100) && !b.contains(&98),
        "顶部区间里 fast 没有的价位要删：{b:?}"
    );
    assert_eq!(b.first(), Some(&81), "深处沿用整本");
    assert_eq!(b.len(), 5 + 13);
    let a = prices(&book.asks);
    assert!(!a.contains(&101) && !a.contains(&106), "{a:?}");
    assert_eq!(a.last(), Some(&120));
    assert_eq!(book.source_time_ms, Some(1500));
    assert_eq!(book.depth_time_ms, Some(1000));
    assert_eq!(book.updated, later);

    // 更旧的 fast 不能回滚。
    assert!(!apply_at(
        &mut books,
        venue,
        top(&[100], &[101]),
        None,
        stamp(later, 1400)
    ));
    // 同一时刻随后到的整本要收（把深处补到同一时刻）。
    let bids2: Vec<i64> = (70..=99).rev().collect();
    assert!(apply_at(
        &mut books,
        venue,
        full(&bids2, &asks[1..]),
        None,
        stamp(later, 1500)
    ));
    assert_eq!(books[&key].depth_time_ms, Some(1500));
    assert_eq!(prices(&books[&key].bids).first(), Some(&70));
}

#[test]
fn a_short_side_or_a_stale_depth_leaves_only_the_fast_levels() {
    let venue = Venue::HyperliquidXyz;
    let key = (venue, "xyz:TSLA".to_string());
    let at = Instant::now();
    let mut books = HashMap::new();
    assert!(apply_at(
        &mut books,
        venue,
        full(
            &[100, 99, 98, 97, 96, 95, 94],
            &[101, 102, 103, 104, 105, 106, 107]
        ),
        None,
        stamp(at, 1000)
    ));
    // 买盘只推了 2 档 = 买盘整个就这 2 档；卖盘满 5 档，深处保留。
    assert!(apply_at(
        &mut books,
        venue,
        top(&[100, 99], &[101, 102, 103, 104, 105]),
        None,
        stamp(at, 1500)
    ));
    assert_eq!(prices(&books[&key].bids), vec![99, 100]);
    assert_eq!(
        prices(&books[&key].asks),
        vec![101, 102, 103, 104, 105, 106, 107]
    );
    // 整本 30 秒以上没来：深处不再拼旧的。
    assert!(apply_at(
        &mut books,
        venue,
        top(&[100, 99, 98, 97, 96], &[101, 102, 103, 104, 105]),
        None,
        stamp(at, 1000 + 30_001)
    ));
    assert_eq!(prices(&books[&key].asks), vec![101, 102, 103, 104, 105]);
    assert_eq!(prices(&books[&key].bids), vec![96, 97, 98, 99, 100]);
}

#[tokio::test]
async fn a_frame_stamped_far_in_the_future_is_dropped_instead_of_freezing_the_book() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let protocol = protocol(Venue::HyperliquidXyz, Stream::Books).unwrap();
    let now = Instant::now();
    let stamp = latency::Stamp {
        received: now,
        enqueued: now,
        utc_ms: 2_000,
        source_ms: None,
    };
    let future = r#"{"channel":"l2Book","data":{"coin":"xyz:TSLA","time":60000,"levels":[[{"px":"100","sz":"2"}],[{"px":"101","sz":"2"}]]}}"#;
    assert_eq!(
        forward(Venue::HyperliquidXyz, &protocol, future, stamp, &tx).await,
        Forwarded::Other,
        "比本机快一分钟：丢掉"
    );
    assert!(rx.try_recv().is_err());
    let near = r#"{"channel":"l2Book","data":{"coin":"xyz:TSLA","time":3000,"levels":[[{"px":"100","sz":"2"}],[{"px":"101","sz":"2"}]]}}"#;
    assert_eq!(
        forward(Venue::HyperliquidXyz, &protocol, near, stamp, &tx).await,
        Forwarded::Data,
        "快一秒（时钟误差以内）照收"
    );
}
