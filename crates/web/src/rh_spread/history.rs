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
/// 近期可成交价差在内存里留多久（[`History::recent_entry`] 的窗口不超过它）。
pub const RECENT_KEEP_HOURS: i64 = 24;
/// 近期可成交价差的中位数至少要这么多分钟样本，不够就当不知道。
pub const RECENT_MIN_MINUTES: usize = 60;
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
    /// 中间价基差（%）：(a − b) / 均值，这一分钟的中位数（最早那组 a = Arcus、b = Lighter RH）。
    pub b: f64,
    /// 这一分钟的采样数。
    pub n: u32,
    /// 可成交价差（%）的分钟最大值：多 a 空 b / 多 b 空 a。缺盘口时为 `None`。
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
    /// 最近 [`RECENT_KEEP_HOURS`] 每分钟的可成交价差（多 a 空 b、多 b 空 a 的分钟最大值，缺盘口为 NaN）。
    recent: HashMap<String, VecDeque<(i64, f32, f32)>>,
}

/// 一个合约近期（最近几小时）两个方向「可成交价差」的分钟最大值的中位数（%）。样本不够的方向为 `None`。
///
/// 和 7 天的「正常水平」不同：价差整体换了水平（某次实盘 从 1.7% 跳到 2.7%）之后，7 天中位要等
/// 新水平攒够一半才跟上，这段时间里每个读数都显得「远高于正常」。近期中位几个小时就跟上了。
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct RecentEntry {
    /// 回看了几小时。
    pub hours: i64,
    pub long_a_pct: Option<f64>,
    pub long_b_pct: Option<f64>,
}

impl History {
    pub fn push(&mut self, row: &Row) {
        let series = self.rows.entry(row.s.clone()).or_default();
        // 文件按时间写，读回也按时间；乱序的（时钟回拨）丢掉，不打乱窗口裁剪。
        if series.back().is_some_and(|(t, ..)| *t >= row.t) {
            return;
        }
        series.push_back((row.t, row.k, row.b));
        if row.ea.is_some() || row.el.is_some() {
            let as_f32 = |value: Option<f64>| value.map_or(f32::NAN, |v| v as f32);
            self.recent.entry(row.s.clone()).or_default().push_back((
                row.t,
                as_f32(row.ea),
                as_f32(row.el),
            ));
        }
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
        let recent_cutoff = (now - Duration::hours(RECENT_KEEP_HOURS)).timestamp();
        for series in self.recent.values_mut() {
            while series.front().is_some_and(|(t, ..)| *t < recent_cutoff) {
                series.pop_front();
            }
        }
        self.recent.retain(|_, series| !series.is_empty());
    }

    /// 这个合约最近 `hours` 小时（不超过 [`RECENT_KEEP_HOURS`]）两个方向可成交价差的中位数，见 [`RecentEntry`]。
    /// 两个方向都不够 [`RECENT_MIN_MINUTES`] 分钟样本时为 `None`。
    pub fn recent_entry(
        &self,
        symbol: &str,
        hours: i64,
        now: DateTime<Utc>,
    ) -> Option<RecentEntry> {
        let hours = hours.clamp(1, RECENT_KEEP_HOURS);
        let cutoff = (now - Duration::hours(hours)).timestamp();
        let series = self.recent.get(symbol)?;
        let median = |pick: fn(&(i64, f32, f32)) -> f32| {
            let mut values: Vec<f64> = series
                .iter()
                .filter(|row| row.0 >= cutoff)
                .map(pick)
                .filter(|value| value.is_finite())
                .map(f64::from)
                .collect();
            if values.len() < RECENT_MIN_MINUTES {
                return None;
            }
            values.sort_by(f64::total_cmp);
            Some((percentile(&values, 0.5) * 1e5).round() / 1e5)
        };
        let entry = RecentEntry {
            hours,
            long_a_pct: median(|row| row.1),
            long_b_pct: median(|row| row.2),
        };
        (entry.long_a_pct.is_some() || entry.long_b_pct.is_some()).then_some(entry)
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

/// `prefix` 见 [`super::pairs::Pair::file_prefix`]（最早那组是 `rh-spread`）。
pub fn file_for(dir: &Path, prefix: &str, day: NaiveDate) -> PathBuf {
    dir.join(format!("{prefix}-{}.jsonl", day.format("%Y%m%d")))
}

/// 读回最近 [`WINDOW_DAYS`] 天。坏行跳过并计数（不让一行坏数据拦住启动）。
pub async fn load(dir: &Path, prefix: &str, now: DateTime<Utc>) -> (History, usize) {
    let mut history = History::default();
    let mut broken = 0;
    for back in (0..=WINDOW_DAYS).rev() {
        let day = (now - Duration::days(back)).date_naive();
        let path = file_for(dir, prefix, day);
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
pub async fn append(
    dir: &Path,
    prefix: &str,
    rows: &[Row],
    now: DateTime<Utc>,
) -> std::io::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    write_rows(dir, prefix, rows, now).await?;
    remove_expired(dir, prefix, now).await;
    Ok(())
}

/// 只追加到当天文件，不清理旧文件（清理要扫整个目录，见 [`Writer`]）。
pub async fn write_rows(
    dir: &Path,
    prefix: &str,
    rows: &[Row],
    now: DateTime<Utc>,
) -> std::io::Result<()> {
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
        .open(file_for(dir, prefix, now.date_naive()))
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

/// 删掉这一组超过 [`KEEP_DAYS`] 的旧文件。
pub async fn remove_expired(dir: &Path, prefix: &str, now: DateTime<Utc>) {
    let oldest = file_for(dir, prefix, (now - Duration::days(KEEP_DAYS)).date_naive());
    let head = format!("{prefix}-");
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // 只删本组的文件：别的组前缀不同；`{prefix}-` 后面紧跟 8 位日期，不会误删更长前缀的组。
            let dated = name
                .strip_prefix(&head)
                .and_then(|rest| rest.strip_suffix(".jsonl"))
                .is_some_and(|day| day.len() == 8 && day.bytes().all(|c| c.is_ascii_digit()));
            if dated && path < oldest {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
}

/// 后台写分钟历史：价差监控主循环换分钟时把各组的行丢进来就走，不在主循环里等文件读写。
///
/// 以前主循环逐组 `append`（约 48 组，每组还要扫一遍几百个文件的目录删旧文件），
/// 线上换分钟那一轮要 0.3～0.8 秒，期间各家盘口更新都在排队。
/// 这里按收到的顺序写；旧文件每组每天只清一次。
/// 一次换分钟要写的：各组（文件前缀, 行），以及时刻。
type WriteBatch = (Vec<(String, Vec<Row>)>, DateTime<Utc>);

pub struct Writer {
    tx: tokio::sync::mpsc::UnboundedSender<WriteBatch>,
    task: tokio::task::JoinHandle<()>,
}

impl Writer {
    pub fn spawn(dir: std::path::PathBuf) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WriteBatch>();
        let task = tokio::spawn(async move {
            let mut cleaned: HashMap<String, NaiveDate> = HashMap::new();
            while let Some((batch, now)) = rx.recv().await {
                for (prefix, rows) in batch {
                    if let Err(error) = write_rows(&dir, &prefix, &rows, now).await {
                        warn!(prefix = %prefix, "价差历史写不进去：{error}");
                    }
                    let today = now.date_naive();
                    if cleaned.get(&prefix) != Some(&today) {
                        remove_expired(&dir, &prefix, now).await;
                        cleaned.insert(prefix, today);
                    }
                }
            }
        });
        Self { tx, task }
    }

    /// 交给后台写；空的组不发。
    pub fn send(&self, batch: Vec<(String, Vec<Row>)>, now: DateTime<Utc>) {
        let batch: Vec<_> = batch
            .into_iter()
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        if !batch.is_empty() {
            let _ = self.tx.send((batch, now));
        }
    }

    /// 停机：等已经交出去的写完（最多 `wait`）。
    pub async fn finish(self, wait: std::time::Duration) {
        drop(self.tx);
        let _ = tokio::time::timeout(wait, self.task).await;
    }
}
