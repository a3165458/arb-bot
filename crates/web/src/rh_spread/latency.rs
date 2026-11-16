//! Bounded, account-free market-data telemetry. Monotonic clocks measure local work;
//! source age includes producer clock skew and is NOT network latency.
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use arb_core::Venue;
use serde::Serialize;

pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);
const BUCKETS_US: [u64; 16] = [
    100,
    500,
    1_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    15_000_000,
    u64::MAX,
];

#[derive(Debug, Clone, Copy)]
pub(super) struct Stamp {
    pub received: Instant,
    pub enqueued: Instant,
    pub utc_ms: i64,
    pub source_ms: Option<i64>,
}
impl Stamp {
    pub fn now() -> Self {
        Self {
            received: Instant::now(),
            enqueued: Instant::now(),
            utc_ms: chrono::Utc::now().timestamp_millis(),
            source_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage {
    ReceiveToApply,
    BookUpdateInterval,
    ReceiveToEnqueue,
    EnqueueToApply,
    ReceiveToPublish,
    ReceiveToWatchPublish,
    Recompute,
    PublishToAutoCheck,
    SourceAge,
    Prepare,
    QuoteToFill,
    Unhedged,
    OpenTotal,
    ShadowBookInterval,
    ShadowBboInterval,
    ShadowSourceAge,
    ShadowBaselineSaved,
}
impl Stage {
    fn label(self) -> &'static str {
        match self {
            Self::ReceiveToApply => "receive_to_apply",
            Self::BookUpdateInterval => "accepted_book_interval",
            Self::ReceiveToEnqueue => "receive_to_enqueue",
            Self::EnqueueToApply => "enqueue_to_apply",
            Self::ReceiveToPublish => "receive_to_publish",
            Self::ReceiveToWatchPublish => "receive_to_watch_publish",
            Self::Recompute => "recompute",
            Self::PublishToAutoCheck => "publish_to_auto_check",
            Self::SourceAge => "source_age_including_clock_skew",
            Self::Prepare => "prepare",
            Self::QuoteToFill => "quote_to_fill",
            Self::Unhedged => "unhedged",
            Self::OpenTotal => "open_total",
            Self::ShadowBookInterval => "shadow_fast_book_interval",
            Self::ShadowBboInterval => "shadow_bbo_interval",
            Self::ShadowSourceAge => "shadow_source_age_including_clock_skew",
            Self::ShadowBaselineSaved => "shadow_saved_vs_connection_0",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Counter {
    Received,
    Applied,
    Duplicate,
    Older,
    TimestampConflict,
    FutureSource,
    ShadowMessage,
    ShadowError,
    ShadowReconnect,
    ShadowFirst,
    ShadowMatched,
    ShadowBudgetClipped,
}
impl Counter {
    fn label(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Duplicate => "duplicate",
            Self::Older => "older",
            Self::TimestampConflict => "timestamp_conflict",
            Self::FutureSource => "future_source",
            Self::ShadowMessage => "shadow_message",
            Self::ShadowError => "shadow_error",
            Self::ShadowReconnect => "shadow_reconnect",
            Self::ShadowFirst => "shadow_first",
            Self::ShadowMatched => "shadow_matched",
            Self::ShadowBudgetClipped => "shadow_budget_clipped",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    Pipeline,
    Venue(Venue),
    Shadow(u8),
}
impl Scope {
    fn label(self) -> String {
        match self {
            Self::Pipeline => "pipeline".into(),
            Self::Venue(v) => v.as_str().into(),
            Self::Shadow(n) => format!("shadow-{}", n.min(3)),
        }
    }
    fn bounded(self) -> Self {
        match self {
            Self::Shadow(n) => Self::Shadow(n.min(3)),
            other => other,
        }
    }
}
#[derive(Default)]
struct Histogram {
    counts: [u64; 16],
    count: u64,
    sum_us: u128,
    max_us: u64,
}
impl Histogram {
    fn observe(&mut self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = BUCKETS_US.iter().position(|&upper| us <= upper).unwrap();
        self.counts[bucket] += 1;
        self.count += 1;
        self.sum_us += us as u128;
        self.max_us = self.max_us.max(us);
    }
    fn quantile(&self, percentile: u64) -> u64 {
        let rank = self.count.saturating_mul(percentile).div_ceil(100).max(1);
        let mut seen = 0;
        for (&n, &upper) in self.counts.iter().zip(&BUCKETS_US) {
            seen += n;
            if seen >= rank {
                return upper.min(self.max_us);
            }
        }
        self.max_us
    }
}
#[derive(Default)]
struct Inner {
    histograms: BTreeMap<(Scope, Stage), Histogram>,
    counters: BTreeMap<(Scope, Counter), u64>,
    published: Option<Instant>,
}
#[derive(Default)]
pub struct Metrics {
    inner: Mutex<Inner>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub histograms: Vec<Sample>,
    pub counters: Vec<Count>,
}
#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    pub scope: String,
    pub stage: &'static str,
    pub count: u64,
    pub p50_upper_us: u64,
    pub p95_upper_us: u64,
    pub p99_upper_us: u64,
    pub max_us: u64,
}
#[derive(Debug, Clone, Serialize)]
pub struct Count {
    pub scope: String,
    pub event: &'static str,
    pub count: u64,
}
impl Metrics {
    pub fn observe(&self, scope: Scope, stage: Stage, elapsed: Duration) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .histograms
            .entry((scope.bounded(), stage))
            .or_default()
            .observe(elapsed);
    }
    pub fn count(&self, scope: Scope, event: Counter) {
        *self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .counters
            .entry((scope.bounded(), event))
            .or_default() += 1;
    }
    pub(super) fn received(&self, venue: Venue, stamp: Stamp) {
        self.count(Scope::Venue(venue), Counter::Received);
        if let Some(source) = stamp.source_ms {
            match stamp.utc_ms.checked_sub(source) {
                Some(age) if age >= 0 => self.observe(
                    Scope::Venue(venue),
                    Stage::SourceAge,
                    Duration::from_millis(age as u64),
                ),
                _ => self.count(Scope::Venue(venue), Counter::FutureSource),
            }
        }
    }
    pub fn publish(&self, started: Instant, oldest: Option<Instant>) {
        let now = Instant::now();
        self.observe(
            Scope::Pipeline,
            Stage::Recompute,
            now.saturating_duration_since(started),
        );
        if let Some(oldest) = oldest {
            self.observe(
                Scope::Pipeline,
                Stage::ReceiveToPublish,
                now.saturating_duration_since(oldest),
            );
        }
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .published = Some(now);
    }
    pub fn auto_check(&self) {
        let published = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .published;
        if let Some(at) = published {
            self.observe(Scope::Pipeline, Stage::PublishToAutoCheck, at.elapsed());
        }
    }
    pub fn execution(&self, report: &arb_exec::OpenReport) {
        for (stage, ms) in [
            (Stage::Prepare, report.prepare_ms),
            (Stage::Unhedged, report.unhedged_ms),
            (Stage::OpenTotal, report.total_ms),
        ] {
            if ms >= 0 {
                self.observe(Scope::Pipeline, stage, Duration::from_millis(ms as u64));
            }
        }
        for leg in [&report.first, &report.second] {
            if let Some(ms) = leg.quote_to_fill_ms.filter(|&ms| ms >= 0) {
                self.observe(
                    Scope::Venue(leg.venue),
                    Stage::QuoteToFill,
                    Duration::from_millis(ms as u64),
                );
            }
        }
    }
    pub fn report(&self) -> Report {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Report {
            histograms: inner
                .histograms
                .iter()
                .map(|(&(scope, stage), h)| Sample {
                    scope: scope.label(),
                    stage: stage.label(),
                    count: h.count,
                    p50_upper_us: h.quantile(50),
                    p95_upper_us: h.quantile(95),
                    p99_upper_us: h.quantile(99),
                    max_us: h.max_us,
                })
                .collect(),
            counters: inner
                .counters
                .iter()
                .map(|(&(scope, event), &count)| Count {
                    scope: scope.label(),
                    event: event.label(),
                    count,
                })
                .collect(),
        }
    }
    pub fn prometheus(&self) -> String {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = String::from(
            "# HELP arb_md_seconds Monotonic local timings; source age includes clock skew.\n# TYPE arb_md_seconds histogram\n",
        );
        for (&(scope, stage), h) in &inner.histograms {
            let labels = format!("scope=\"{}\",stage=\"{}\"", scope.label(), stage.label());
            let mut cumulative = 0;
            for (&count, &upper) in h.counts.iter().zip(&BUCKETS_US) {
                cumulative += count;
                let le = if upper == u64::MAX {
                    "+Inf".into()
                } else {
                    format!("{}", upper as f64 / 1_000_000.0)
                };
                let _ = writeln!(
                    out,
                    "arb_md_seconds_bucket{{{labels},le=\"{le}\"}} {cumulative}"
                );
            }
            let _ = writeln!(
                out,
                "arb_md_seconds_count{{{labels}}} {}\narb_md_seconds_sum{{{labels}}} {}",
                h.count,
                h.sum_us as f64 / 1_000_000.0
            );
        }
        out.push_str("# HELP arb_md_events_total Bounded market-data counters.\n# TYPE arb_md_events_total counter\n");
        for (&(scope, event), count) in &inner.counters {
            let _ = writeln!(
                out,
                "arb_md_events_total{{scope=\"{}\",event=\"{}\"}} {count}",
                scope.label(),
                event.label()
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn histograms_are_cumulative_bounded_and_do_not_include_symbols() {
        let m = Metrics::default();
        for us in [100, 101, 500, 501, 50_000] {
            m.observe(
                Scope::Venue(Venue::HyperliquidXyz),
                Stage::ReceiveToApply,
                Duration::from_micros(us),
            );
        }
        let r = m.report();
        let h = &r.histograms[0];
        assert_eq!(h.count, 5);
        assert_eq!(h.p50_upper_us, 500);
        assert_eq!(h.p95_upper_us, 50_000);
        let text = m.prometheus();
        assert!(text.contains("le=\"0.0001\"} 1"));
        assert!(text.contains("le=\"0.0005\"} 3"));
        assert!(text.contains("le=\"+Inf\"} 5"));
        assert!(!text.contains("symbol="));
    }
    #[test]
    fn producer_clock_skew_is_not_reported_as_negative_network_latency() {
        let m = Metrics::default();
        m.received(
            Venue::Hyperliquid,
            Stamp {
                received: Instant::now(),
                enqueued: Instant::now(),
                utc_ms: 1000,
                source_ms: Some(1001),
            },
        );
        assert!(m.report().histograms.is_empty());
        assert_eq!(m.report().counters[1].event, "future_source");
    }
    #[test]
    fn shadow_labels_cannot_grow_with_connection_ids() {
        let m = Metrics::default();
        for n in 0..255 {
            m.count(Scope::Shadow(n), Counter::ShadowMessage);
        }
        assert_eq!(m.report().counters.len(), 4);
    }
}
