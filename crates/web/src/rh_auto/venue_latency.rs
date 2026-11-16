//! 交易所下单变慢时暂停用它开新仓（第六批，11-09）。
//!
//! 每笔自动开仓的报告里有两腿各自的下单往返（发单到拿到结果，见 `OpenReport::first_order_ms`）。按交易所记最近
//! [`WINDOW`] 个样本；攒够 [`MIN_BASELINE`] 个以后，最近**连续 2 次**都明显慢于之前的中位数（超过中位数的
//! [`SLOW_RATIO`] 倍、且多出 [`SLOW_EXTRA_MS`] 以上），就 [`PAUSE`] 内不用这家开新仓。慢的样本照样进窗口：一直慢下去，
//! 基线会慢慢跟上，暂停到期后不会没完没了地再停。
//!
//! 为什么：交易所延迟突然变大（比如 Lighter 从 0.3 秒变成 1 秒以上）时，第二腿的裸露窗口跟着变长，保本价附近的
//! 单子更容易吃不到、回滚或强制补腿。第二腿重试过的那次不算（重试本身就拉长了耗时）。

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use arb_core::Venue;

/// 每家留最近多少个样本。
const WINDOW: usize = 20;
/// 至少多少个样本才有基线。
const MIN_BASELINE: usize = 8;
/// 比基线中位数慢多少倍、多多少毫秒才算慢（两个都要满足）。
const SLOW_RATIO: f64 = 1.5;
const SLOW_EXTRA_MS: u64 = 600;
/// 判定变慢后多久不用这家开新仓。
pub const PAUSE: Duration = Duration::from_secs(600);

#[derive(Default)]
pub struct VenueLatency {
    samples: HashMap<Venue, VecDeque<u64>>,
    /// 最近一个样本算不算慢（连续 2 次才停）。
    last_slow: HashMap<Venue, bool>,
    paused_until: HashMap<Venue, Instant>,
}

impl VenueLatency {
    /// 记一个样本；这一下刚好触发暂停时返回一句说明。
    pub fn record(&mut self, venue: Venue, ms: u64, now: Instant) -> Option<String> {
        let window = self.samples.entry(venue).or_default();
        let baseline = (window.len() >= MIN_BASELINE).then(|| median(window));
        window.push_back(ms);
        while window.len() > WINDOW {
            window.pop_front();
        }
        let Some(baseline) = baseline else {
            self.last_slow.insert(venue, false);
            return None;
        };
        let slow = ms as f64 > baseline as f64 * SLOW_RATIO && ms > baseline + SLOW_EXTRA_MS;
        let before = self.last_slow.insert(venue, slow).unwrap_or(false);
        if !(slow && before) || self.paused(venue, now).is_some() {
            return None;
        }
        self.paused_until.insert(venue, now + PAUSE);
        // 停了就重新数：到期后要再连续 2 次慢才再停。
        self.last_slow.insert(venue, false);
        Some(format!(
            "{venue} 下单连续两次明显变慢（这次 {ms}ms，近期中位 {baseline}ms）：{} 分钟内不用它开新仓",
            PAUSE.as_secs() / 60
        ))
    }

    /// 这家还在暂停中：还剩多少秒。
    pub fn paused(&self, venue: Venue, now: Instant) -> Option<u64> {
        self.paused_until
            .get(&venue)
            .filter(|until| **until > now)
            .map(|until| until.duration_since(now).as_secs().max(1))
    }
}

fn median(window: &VecDeque<u64>) -> u64 {
    let mut sorted: Vec<u64> = window.iter().copied().collect();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

/// 从开仓响应里取出（场所, 下单往返毫秒）样本：第一腿总取；第二腿只在一次就成交时取。
pub fn samples_of(report: &serde_json::Value) -> Vec<(Venue, u64)> {
    let venue = |leg: &str| serde_json::from_value::<Venue>(report[leg]["venue"].clone()).ok();
    let mut out = Vec::new();
    if let (Some(venue), Some(ms)) = (venue("first"), report["first_order_ms"].as_u64()) {
        out.push((venue, ms));
    }
    if let (Some(venue), Some(ms), Some(1)) = (
        venue("second"),
        report["second_order_ms"].as_u64(),
        report["second_attempts"].as_u64(),
    ) {
        out.push((venue, ms));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_clearly_slow_orders_in_a_row_pause_the_venue() {
        let mut latency = VenueLatency::default();
        let now = Instant::now();
        for _ in 0..MIN_BASELINE {
            assert!(latency.record(Venue::Lighter, 350, now).is_none());
        }
        // 一次慢不停；慢得不够多（1.5 倍但不到 +600ms）不算。
        assert!(latency.record(Venue::Lighter, 1200, now).is_none());
        assert!(latency.record(Venue::Lighter, 400, now).is_none());
        assert!(latency.record(Venue::Lighter, 700, now).is_none());
        assert!(latency.record(Venue::Lighter, 800, now).is_none());
        // 连续两次明显慢：停。
        assert!(latency.record(Venue::Lighter, 1300, now).is_none());
        let why = latency.record(Venue::Lighter, 1400, now).unwrap();
        assert!(why.contains("lighter") && why.contains("1400"), "{why}");
        assert!(latency.paused(Venue::Lighter, now).is_some());
        assert!(
            latency.paused(Venue::Arcus, now).is_none(),
            "别的交易所不受影响"
        );
        assert!(
            latency.paused(Venue::Lighter, now + PAUSE).is_none(),
            "到期恢复"
        );
    }

    #[test]
    fn samples_come_from_the_report_and_skip_a_retried_second_leg() {
        let report = serde_json::json!({
            "first": {"venue": "hyperliquid-xyz"}, "second": {"venue": "lighter-rh"},
            "first_order_ms": 720, "second_order_ms": 1420, "second_attempts": 1
        });
        assert_eq!(
            samples_of(&report),
            vec![(Venue::HyperliquidXyz, 720), (Venue::LighterRh, 1420)]
        );
        let retried = serde_json::json!({
            "first": {"venue": "lighter"}, "second": {"venue": "lighter-rh"},
            "first_order_ms": 1380, "second_order_ms": 3100, "second_attempts": 2
        });
        assert_eq!(samples_of(&retried), vec![(Venue::Lighter, 1380)]);
        assert!(
            samples_of(&serde_json::json!({})).is_empty(),
            "旧报告没有这几项"
        );
    }
}
