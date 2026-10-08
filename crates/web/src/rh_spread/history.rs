//! 价差历史：每个合约每分钟一行（中间价基差的分钟中位数），落盘成按 UTC 日期分的 JSONL，
//! 重启后读回最近几天。两家都没有可用的历史价差接口，「正常水平」只能自己攒。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::session::Session;

/// 「正常水平」回看多少天。
pub const WINDOW_DAYS: i64 = 7;
/// 同一时段至少攒够多少分钟才给「正常水平」：太少的样本只会把一时的偏离当成常态。
pub const MIN_MINUTES: usize = 120;
/// 文件保留多少天。
pub const KEEP_DAYS: i64 = 14;

/// 落盘的一行。字段名短：一天约 5 万行。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Row {
    /// 这一分钟的起点（Unix 秒）。
    pub t: i64,
    /// 合约 base（如 `SPY`）。
    pub s: String,
    pub k: Session,
    /// 中间价基差（%）：(Arcus − Lighter RH) / 均值，这一分钟的中位数。
    pub b: f64,
    /// 这一分钟的采样数。
    pub n: u32,
    /// 一档可成交价差（%）的分钟最大值：多 Arcus 空 RH / 多 RH 空 Arcus。缺盘口时为 `None`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ea: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub el: Option<f64>,
}

/// 一个合约在一个时段里的「正常水平」。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Normal {
    pub session: Session,
    pub median: f64,
    pub p10: f64,
    pub p90: f64,
    /// 中位数绝对偏差（%）。
    pub mad: f64,
    /// 用了多少分钟的样本。
    pub minutes: usize,
}

#[derive(Debug, Default)]
pub struct History {
    rows: HashMap<String, VecDeque<(i64, Session, f64)>>,
}

impl History {
    pub fn push(&mut self, row: &Row) {
        let series = self.rows.entry(row.s.clone()).or_default();
        // 文件按时间写，读回也按时间；乱序的（时钟回拨）丢掉，不打乱窗口裁剪。
        if series.back().is_some_and(|(t, ..)| *t >= row.t) {
            return;
        }
        series.push_back((row.t, row.k, row.b));
    }

    /// 丢掉窗口之外的旧行。
    pub fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = (now - Duration::days(WINDOW_DAYS)).timestamp();
        for series in self.rows.values_mut() {
            while series.front().is_some_and(|(t, ..)| *t < cutoff) {
                series.pop_front();
            }
        }
        self.rows.retain(|_, series| !series.is_empty());
    }

    /// 这个合约在这个时段的正常水平；样本不够 [`MIN_MINUTES`] 为 `None`。
    pub fn normal(&self, symbol: &str, session: Session, now: DateTime<Utc>) -> Option<Normal> {
        let cutoff = (now - Duration::days(WINDOW_DAYS)).timestamp();
        let mut values: Vec<f64> = self
            .rows
            .get(symbol)?
            .iter()
            .filter(|(t, k, b)| *t >= cutoff && *k == session && b.is_finite())
            .map(|(.., b)| *b)
            .collect();
        if values.len() < MIN_MINUTES {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let median = percentile(&values, 0.5);
        let mut deviations: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
        deviations.sort_by(f64::total_cmp);
        Some(Normal {
            session,
            median,
            p10: percentile(&values, 0.1),
            p90: percentile(&values, 0.9),
            mad: percentile(&deviations, 0.5),
            minutes: values.len(),
        })
    }

    /// 攒了多少分钟（全部合约、全部时段里最多的那个合约）。给页面说明历史够不够用。
    pub fn coverage_minutes(&self) -> usize {
        self.rows.values().map(VecDeque::len).max().unwrap_or(0)
    }

    /// 某合约在某时段已有多少分钟（不足 [`MIN_MINUTES`] 时页面显示进度）。
    pub fn minutes(&self, symbol: &str, session: Session, now: DateTime<Utc>) -> usize {
        let cutoff = (now - Duration::days(WINDOW_DAYS)).timestamp();
        self.rows.get(symbol).map_or(0, |series| {
            series
                .iter()
                .filter(|(t, k, _)| *t >= cutoff && *k == session)
                .count()
        })
    }
}

/// 已排序切片的线性插值分位数。
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let position = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let (low, high) = (position.floor() as usize, position.ceil() as usize);
    let weight = position - low as f64;
    sorted[low] * (1.0 - weight) + sorted[high] * weight
}

pub fn file_for(dir: &Path, day: NaiveDate) -> PathBuf {
    dir.join(format!("rh-spread-{}.jsonl", day.format("%Y%m%d")))
}

/// 读回最近 [`WINDOW_DAYS`] 天。坏行跳过并计数（不让一行坏数据拦住启动）。
pub async fn load(dir: &Path, now: DateTime<Utc>) -> (History, usize) {
    let mut history = History::default();
    let mut broken = 0;
    for back in (0..=WINDOW_DAYS).rev() {
        let day = (now - Duration::days(back)).date_naive();
        let path = file_for(dir, day);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                warn!(path = %path.display(), "读不了价差历史：{error}");
                continue;
            }
        };
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            match serde_json::from_str::<Row>(line) {
                Ok(row) => history.push(&row),
                Err(_) => broken += 1,
            }
        }
    }
    history.prune(now);
    (history, broken)
}

/// 追加一批行到当天文件，并删掉超过 [`KEEP_DAYS`] 的旧文件。
pub async fn append(dir: &Path, rows: &[Row], now: DateTime<Utc>) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    if rows.is_empty() {
        return Ok(());
    }
    tokio::fs::create_dir_all(dir).await?;
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_for(dir, now.date_naive()))
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    let oldest = file_for(dir, (now - Duration::days(KEEP_DAYS)).date_naive());
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("rh-spread-") && name.ends_with(".jsonl") && path < oldest {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
    Ok(())
}
