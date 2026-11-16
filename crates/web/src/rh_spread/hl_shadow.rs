//! Public, read-only HL fast/BBO A/B feeds. Deliberately has NO trading-feed sender,
//! LocalBook or order interface: a shadow frame cannot drive a live decision.
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use super::{
    Subscriptions, feed,
    latency::{Counter, METRICS, Scope, Stage},
};
use arb_core::Venue;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

const RESERVE_CONNECTIONS: usize = 2;
const SUBSCRIPTION_BUDGET: usize = 900; // Leave 100 for other consumers.
const MAX_MARKETS: usize = 16;
const MAX_RACES: usize = 2048;

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub connections: u8,
}
impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let raw = std::env::var("ARB_HL_SHADOW_CONNECTIONS").unwrap_or_else(|_| "0".into());
        let connections = raw
            .trim()
            .parse::<u8>()
            .map_err(|_| anyhow::anyhow!("ARB_HL_SHADOW_CONNECTIONS 必须是 0 到 4"))?;
        anyhow::ensure!(connections <= 4, "ARB_HL_SHADOW_CONNECTIONS 必须是 0 到 4");
        Ok(Self { connections })
    }
}
pub(super) fn is_hl(v: Venue) -> bool {
    matches!(
        v,
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo
    )
}
/// One global pool for all three logical dexes; deterministic sampling, bounded subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Plan {
    connections: u8,
    coins: Vec<String>,
}
pub(super) fn plan(config: &Config, subs: &Subscriptions) -> Plan {
    let live_connections = subs
        .iter()
        .filter(|(v, keys)| is_hl(**v) && !keys.is_empty())
        .count();
    let live_subs: usize = subs
        .iter()
        .filter(|(v, _)| is_hl(**v))
        .map(|(_, keys)| keys.len())
        .sum::<usize>()
        // 业务连接每个币还订了 fast 顶部（feed::Event::Top）。
        * if feed::hyperliquid_fast_top_enabled() { 2 } else { 1 };
    let connections = usize::from(config.connections.min(4))
        .min(10usize.saturating_sub(live_connections + RESERVE_CONNECTIONS));
    // Round-robin dexes: alphabetical truncation could otherwise sample only io.
    let groups: Vec<Vec<String>> = [
        Venue::Hyperliquid,
        Venue::HyperliquidXyz,
        Venue::HyperliquidIo,
    ]
    .iter()
    .map(|venue| {
        let mut keys = subs.get(venue).cloned().unwrap_or_default();
        keys.sort();
        keys.dedup();
        keys
    })
    .collect();
    let mut coins = Vec::new();
    for index in 0..groups.iter().map(Vec::len).max().unwrap_or(0) {
        for group in &groups {
            if let Some(coin) = group.get(index)
                && !coins.contains(coin)
            {
                coins.push(coin.clone());
            }
        }
        if coins.len() >= MAX_MARKETS {
            break;
        }
    }
    // Each shadow connection subscribes to fast book + BBO. Business slow book is the control.
    let max_markets = if connections == 0 {
        0
    } else {
        SUBSCRIPTION_BUDGET.saturating_sub(live_subs) / (2 * connections)
    };
    coins.truncate(MAX_MARKETS.min(max_markets));
    Plan {
        connections: if coins.is_empty() {
            0
        } else {
            connections as u8
        },
        coins,
    }
}

/// Shared across business and shadow HL connections. Rolling windows, no lock held across await.
struct Window {
    limit: usize,
    sent: VecDeque<Instant>,
}
impl Window {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            sent: VecDeque::new(),
        }
    }
    fn reserve(&mut self, now: Instant) -> Duration {
        while self
            .sent
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= Duration::from_secs(60))
        {
            self.sent.pop_front();
        }
        if self.sent.len() < self.limit {
            self.sent.push_back(now);
            Duration::ZERO
        } else {
            Duration::from_secs(60)
                .saturating_sub(now.saturating_duration_since(*self.sent.front().unwrap()))
        }
    }
}
static CONNECTIONS: LazyLock<Mutex<Window>> = LazyLock::new(|| Mutex::new(Window::new(24)));
static MESSAGES: LazyLock<Mutex<Window>> = LazyLock::new(|| Mutex::new(Window::new(1800)));
static SHADOW_CONNECTIONS: LazyLock<Mutex<Window>> = LazyLock::new(|| Mutex::new(Window::new(12)));
static SHADOW_MESSAGES: LazyLock<Mutex<Window>> = LazyLock::new(|| Mutex::new(Window::new(900)));
async fn reserve(window: &Mutex<Window>) {
    loop {
        let wait = window
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve(Instant::now());
        if wait.is_zero() {
            return;
        }
        tokio::time::sleep(wait).await;
    }
}
pub(super) async fn connection_budget() {
    reserve(&CONNECTIONS).await;
}
async fn send_shadow(ws: &mut super::Ws, message: Message) -> anyhow::Result<()> {
    reserve(&SHADOW_MESSAGES).await;
    send(ws, message, true).await
}
pub(super) async fn send(ws: &mut super::Ws, message: Message, hl: bool) -> anyhow::Result<()> {
    if hl {
        reserve(&MESSAGES).await;
    }
    ws.send(message).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Version {
    coin: String,
    channel: String,
    source_ms: i64,
    fingerprint: u64,
}
struct Race {
    first: Instant,
    baseline: Option<Instant>,
    seen: u8,
    recorded: bool,
}
#[derive(Default)]
struct Tracker {
    races: HashMap<Version, Race>,
    order: VecDeque<(Instant, Version)>,
    last: HashMap<(u8, String, String), (i64, u64, Instant)>,
}
impl Tracker {
    fn record(
        &mut self,
        connection: u8,
        value: &Value,
        received: Instant,
        utc_ms: i64,
        metrics: &super::latency::Metrics,
    ) {
        let Some(channel @ ("l2Book" | "bbo")) = value.get("channel").and_then(Value::as_str)
        else {
            return;
        };
        let data = &value["data"];
        let (Some(coin), Some(source_ms)) = (
            data["coin"].as_str(),
            data["time"].as_i64().filter(|&t| t > 0),
        ) else {
            return;
        };
        let body = if channel == "l2Book" {
            &data["levels"]
        } else {
            &data["bbo"]
        };
        if !body.is_array() {
            return;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        body.to_string().hash(&mut hasher);
        let fingerprint = hasher.finish();
        let scope = Scope::Shadow(connection);
        let key = (connection, coin.to_owned(), channel.to_owned());
        if let Some(&(last_source, last_hash, last_at)) = self.last.get(&key) {
            if source_ms < last_source || (source_ms == last_source && fingerprint == last_hash) {
                return;
            }
            metrics.observe(
                scope,
                if channel == "l2Book" {
                    Stage::ShadowBookInterval
                } else {
                    Stage::ShadowBboInterval
                },
                received.saturating_duration_since(last_at),
            );
        }
        if self.last.len() >= 4 * MAX_MARKETS * 2
            && !self.last.contains_key(&key)
            && let Some(old) = self.last.keys().next().cloned()
        {
            self.last.remove(&old);
        }
        self.last.insert(key, (source_ms, fingerprint, received));
        metrics.count(scope, Counter::ShadowMessage);
        if let Some(age) = utc_ms.checked_sub(source_ms).filter(|&a| a >= 0) {
            metrics.observe(
                scope,
                Stage::ShadowSourceAge,
                Duration::from_millis(age as u64),
            );
        }
        while self.order.front().is_some_and(|(at, _)| {
            self.races.len() >= MAX_RACES
                || received.saturating_duration_since(*at) >= Duration::from_secs(60)
        }) {
            let (_, key) = self.order.pop_front().unwrap();
            self.races.remove(&key);
        }
        let version = Version {
            coin: coin.into(),
            channel: channel.into(),
            source_ms,
            fingerprint,
        };
        let race = self.races.entry(version.clone()).or_insert_with(|| {
            self.order.push_back((received, version));
            metrics.count(scope, Counter::ShadowFirst);
            Race {
                first: received,
                baseline: None,
                seen: 0,
                recorded: false,
            }
        });
        race.first = race.first.min(received);
        race.seen |= 1u8 << connection;
        if connection == 0 {
            race.baseline = Some(received);
        }
        if !race.recorded
            && race.seen.count_ones() >= 2
            && let Some(baseline) = race.baseline
        {
            metrics.observe(
                Scope::Pipeline,
                Stage::ShadowBaselineSaved,
                baseline.saturating_duration_since(race.first),
            );
            metrics.count(Scope::Pipeline, Counter::ShadowMatched);
            race.recorded = true;
        }
    }
}

pub(super) fn spawn(config: &Config, initial: &Subscriptions) -> watch::Sender<Plan> {
    let initial = plan(config, initial);
    if initial.connections < config.connections {
        METRICS.count(Scope::Pipeline, Counter::ShadowBudgetClipped);
    }
    let (tx, rx) = watch::channel(initial);
    let tracker = std::sync::Arc::new(Mutex::new(Tracker::default()));
    // Spawn configured workers once; a clipped/empty plan parks unused workers.
    for connection in 0..config.connections.min(4) {
        tokio::spawn(worker(connection, rx.clone(), tracker.clone()));
    }
    tx
}
async fn worker(
    connection: u8,
    mut plans: watch::Receiver<Plan>,
    tracker: std::sync::Arc<Mutex<Tracker>>,
) {
    let mut failures = 0;
    loop {
        if crate::shutdown::is_draining() {
            return;
        }
        let plan = plans.borrow_and_update().clone();
        if connection >= plan.connections || plan.coins.is_empty() {
            if plans.changed().await.is_err() {
                return;
            }
            continue;
        }
        tokio::time::sleep(Duration::from_millis(u64::from(connection) * 137)).await;
        let started = Instant::now();
        match session(connection, &plan, &mut plans, &tracker).await {
            Ok(false) => return,
            Ok(true) => failures = 0,
            Err(error) => {
                METRICS.count(Scope::Shadow(connection), Counter::ShadowError);
                METRICS.count(Scope::Shadow(connection), Counter::ShadowReconnect);
                tracing::warn!(connection, "HL 影子行情断开（不影响交易盘口）：{error:#}");
                if started.elapsed() > Duration::from_secs(60) {
                    failures = 0;
                }
                let wait = super::backoff(failures);
                failures += 1;
                tokio::time::sleep(wait).await;
            }
        }
    }
}
async fn session(
    connection: u8,
    plan: &Plan,
    plans: &mut watch::Receiver<Plan>,
    tracker: &Mutex<Tracker>,
) -> anyhow::Result<bool> {
    reserve(&SHADOW_CONNECTIONS).await;
    let mut ws = super::connect(feed::HYPERLIQUID_WS).await?;
    tracker
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .last
        .retain(|(id, _, _), _| *id != connection);
    tracing::info!(
        connection,
        markets = plan.coins.len(),
        "HL fast/BBO 影子采样启动（不进入交易盘口）"
    );
    let warmup = Instant::now() + Duration::from_secs(5);
    for coin in &plan.coins {
        send_shadow(
            &mut ws,
            Message::text(feed::hyperliquid_fast_subscribe(coin)),
        )
        .await?;
        send_shadow(
            &mut ws,
            Message::text(feed::hyperliquid_bbo_subscribe(coin)),
        )
        .await?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    ping.tick().await;
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            message = ws.next() => {
                last_frame = Instant::now();
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let utc = chrono::Utc::now().timestamp_millis();
                        let value: Value = serde_json::from_str(&text)?;
                        if value["channel"].as_str() == Some("error") { anyhow::bail!("public subscription rejected"); }
                        if let Some(coin) = value["data"]["coin"].as_str() && plan.coins.iter().any(|c| c == coin) && last_frame >= warmup {
                            tracker.lock().unwrap_or_else(|e| e.into_inner()).record(connection, &value, last_frame, utc, &METRICS);
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => send_shadow(&mut ws, Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(_))) | None => anyhow::bail!("public WS closed"),
                    Some(Err(error)) => return Err(error.into()),
                    _ => {}
                }
            }
            changed = plans.changed() => { let _ = ws.close(None).await; return Ok(changed.is_ok()); }
            _ = ping.tick() => {
                if crate::shutdown::is_draining() { let _ = ws.close(None).await; return Ok(false); }
                anyhow::ensure!(last_frame.elapsed() < Duration::from_secs(60), "shadow feed idle");
                send_shadow(&mut ws, Message::text(r#"{"method":"ping"}"#)).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budgets_are_global_across_main_xyz_io_and_leave_headroom() {
        let mut subs = Subscriptions::new();
        for venue in [
            Venue::Hyperliquid,
            Venue::HyperliquidXyz,
            Venue::HyperliquidIo,
        ] {
            subs.insert(
                venue,
                // 业务连接每个币订普通 + fast 两个：145 × 3 × 2 = 870。
                (0..145)
                    .map(|i| format!("{}:{i}", venue.as_str()))
                    .collect(),
            );
        }
        let p = plan(&Config { connections: 4 }, &subs);
        assert_eq!(p.connections, 4);
        assert_eq!(p.coins.len(), 3);
        assert!(870 + p.coins.len() * 2 * p.connections as usize <= SUBSCRIPTION_BUDGET);
        subs.get_mut(&Venue::Hyperliquid)
            .unwrap()
            .extend((145..175).map(|i| i.to_string()));
        assert_eq!(plan(&Config { connections: 4 }, &subs).connections, 0);
    }
    #[test]
    fn sampling_is_balanced_and_requested_connections_are_always_capped() {
        let subs = Subscriptions::from([
            (
                Venue::HyperliquidXyz,
                (0..30).map(|i| format!("xyz:{i}")).collect(),
            ),
            (
                Venue::HyperliquidIo,
                (0..30).map(|i| format!("io:{i}")).collect(),
            ),
        ]);
        let p = plan(&Config { connections: 255 }, &subs);
        assert_eq!(p.connections, 4);
        assert_eq!(p.coins.len(), MAX_MARKETS);
        assert_eq!(p.coins.iter().filter(|c| c.starts_with("xyz:")).count(), 8);
        assert_eq!(p.coins.iter().filter(|c| c.starts_with("io:")).count(), 8);
        assert_eq!(plan(&Config::default(), &subs).connections, 0);
    }

    #[test]
    fn connection_window_is_rolling_and_never_sleeps_while_locked() {
        let now = Instant::now();
        let mut w = Window::new(2);
        assert!(w.reserve(now).is_zero());
        assert!(w.reserve(now).is_zero());
        assert_eq!(
            w.reserve(now + Duration::from_secs(1)),
            Duration::from_secs(59)
        );
        assert!(w.reserve(now + Duration::from_secs(60)).is_zero());
    }
    #[test]
    fn matched_identical_versions_measure_race_without_mixing_coins_or_channels() {
        let mut tracker = Tracker::default();
        let m = super::super::latency::Metrics::default();
        let t = Instant::now();
        let v = serde_json::json!({"channel":"bbo","data":{"coin":"xyz:TSLA","time":1000,"bbo":[{"px":"10","sz":"1"},{"px":"11","sz":"1"}]}});
        tracker.record(1, &v, t, 1001, &m);
        tracker.record(0, &v, t + Duration::from_millis(30), 1031, &m);
        tracker.record(0, &v, t + Duration::from_millis(40), 1041, &m);
        assert_eq!(tracker.races.len(), 1);
        let r = m.report();
        let saved = r
            .histograms
            .iter()
            .find(|h| h.stage == "shadow_saved_vs_connection_0")
            .unwrap();
        assert_eq!(saved.count, 1);
        assert_eq!(saved.max_us, 30_000);
        assert!(
            r.histograms
                .iter()
                .all(|h| h.stage != "shadow_bbo_interval")
        );
        for source in 1001..(1001 + MAX_RACES as i64 + 10) {
            let mut newer = v.clone();
            newer["data"]["time"] = source.into();
            tracker.record(1, &newer, t, source, &m);
        }
        assert!(tracker.races.len() <= MAX_RACES);
        for i in 0..1000 {
            let mut changing_catalog = v.clone();
            changing_catalog["data"]["coin"] = format!("xyz:{i}").into();
            tracker.record(1, &changing_catalog, t, 1001, &m);
        }
        assert!(
            tracker.last.len() <= 4 * MAX_MARKETS * 2,
            "catalog rotation must not grow memory without bound"
        );
        assert!(tracker.races.len() <= MAX_RACES);
    }
}
