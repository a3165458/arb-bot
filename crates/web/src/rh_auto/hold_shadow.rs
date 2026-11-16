//! 信号保持影子：只记录，不下单。
//!
//! 2026-11-04 用户问「信号保持从 5 秒减到 0.5 秒，会赚还是亏」。这个问题事后回溯不了：
//! - 价差历史只按分钟存最大值；
//! - 日志只记真正去开仓的信号；
//! - 「出现了、没撑到 5 秒就没了」的弱信号一条都没留下。
//!
//! 所以从现在起逐个记下来。
//!
//! **范围**：只看「要等保持」的弱信号。强信号（[`Settings::instant_multiple`]）本来就 0 秒下，规则改不改都一样。
//! 判定达标与自动交易同一套 [`candidate_of`]，已含门槛、手续费占比、急行情、近期偏离等。中断判定也一样：
//! 连续 [`HOLD_GRACE`] 没出现才算断。
//!
//! **每个信号两笔虚拟开仓**，同一个信号上按两种规则比：
//! - **快**：撑到 [`QUICK_HOLD`]（0.5 秒）那一刻触发；
//! - **慢**：按现行规则，撑满 `hold_sec`，或中途变成强信号的那一刻触发。
//!
//! **虚拟成交与平仓**：
//! - 触发后 [`FILL_DELAY`] 取那一刻的可成交价差当成交价。实盘报价到第一腿成交的中位数约 0.82 秒。
//! - 之后每次检查都按反方向的可成交价差估「现在整笔平掉净多少」：开仓价差 + 平仓价差 − 往返手续费。
//! - 到止盈（按现行设置换算成 %）就记平仓；[`HORIZON`] 内没到，按最后一次的值记。
//! - 资金费、回滚不在这里算。行里留着触发时与成交时的价差，事后按「成交时价差还够不够」估回滚。
//!
//! 整个信号的两笔都有结果后，写一行到 `rh-spread/hold-shadow-YYYYMMDD.jsonl`，保留 [`KEEP_DAYS`] 天。
//! 只在自动交易实际在看的时候认信号（关着、暂停、开完一笔后的间隔、退避时不认 —— 那时两种规则都不会开）；
//! 已经触发的虚拟仓位在停看时照常按行情跟到平仓。重启丢掉还没写的。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use tracing::{info, warn};

use super::{HOLD_GRACE, HOLD_UNOBSERVED, HoldKey, Settings, candidate_of};
use crate::rh_spread::{Leg, Line, View};

pub const FILE_PREFIX: &str = "hold-shadow";
pub const KEEP_DAYS: i64 = 14;
/// 要评估的保持时间。
pub const QUICK_HOLD: Duration = Duration::from_millis(500);
/// 触发到成交：实盘自动开仓「报价 → 第一腿成交」中位 819 毫秒（10-15～11-04）。
pub const FILL_DELAY: Duration = Duration::from_millis(900);
/// 虚拟仓位最多跟多久；到时没止盈就按那时的值记。
pub const HORIZON: Duration = Duration::from_secs(2 * 3600);
/// 已成交的虚拟仓位多久估一次平仓（2026-11-05 线上采样：每 100 毫秒全估一遍占了约 2.7% 的 CPU）。
const EXIT_EVERY: Duration = Duration::from_secs(1);
/// 多久在日志里汇总一次。
const SUMMARY_EVERY: Duration = Duration::from_secs(3600);

/// 某一刻的价差（%）。
#[derive(Debug, Clone, Copy)]
struct Snap {
    entry: Decimal,
    net: Option<Decimal>,
    net_zero: Decimal,
}

/// 一种规则下的虚拟开仓。
#[derive(Debug, Clone, Default)]
struct Arm {
    /// 触发时刻（距信号出现，毫秒）与那一刻的价差。
    trigger: Option<(u64, Snap)>,
    due: Option<Instant>,
    /// 成交时刻（距信号出现，毫秒）与成交价差、往返手续费。
    fill: Option<(u64, Snap, Decimal)>,
    entered: Option<Instant>,
    /// 止盈线（%）。
    tp: Decimal,
    best: Option<Decimal>,
    worst: Option<Decimal>,
    last: Option<Decimal>,
    /// 结果：`tp` / `horizon` / `no_quote`（成交那一刻没报价）。
    done: Option<&'static str>,
    exit_ms: Option<u64>,
    /// 这个组合的历史分位门槛结论（自动交易缓存里有的话）：`true` = 放行。触发时没有、信号还在时陆续补上。
    hist: Option<bool>,
}

impl Arm {
    fn settled(&self) -> bool {
        self.trigger.is_none() || self.done.is_some()
    }

    fn fire(&mut self, at_ms: u64, snap: Snap, now: Instant, tp: Decimal) {
        self.trigger = Some((at_ms, snap));
        self.due = Some(now + FILL_DELAY);
        self.tp = tp;
    }

    /// 到点成交、之后估平仓。`legs`：(开仓方向, 反方向, 往返手续费)。`exits` = 这一轮估平仓
    /// （每 [`EXIT_EVERY`] 一次；成交照样每轮看，好按时取成交价）。
    fn follow(
        &mut self,
        started: Instant,
        now: Instant,
        legs: Option<(&Leg, Option<&Leg>, Decimal)>,
        exits: bool,
    ) {
        if self.done.is_some() {
            return;
        }
        if self.fill.is_none() {
            if self.due.is_none_or(|due| due > now) {
                return;
            }
            match legs {
                Some((leg, _, fee)) => {
                    self.fill = Some((ms(now.saturating_duration_since(started)), snap(leg), fee));
                    self.entered = Some(now);
                }
                None => self.done = Some("no_quote"),
            }
            return;
        }
        let (Some((_, entry, fee)), Some(entered)) = (self.fill, self.entered) else {
            return;
        };
        if !exits {
            return;
        }
        if let Some((_, Some(back), _)) = legs {
            let value = entry.entry + back.quote.entry_pct - fee;
            self.last = Some(value);
            self.best = Some(self.best.map_or(value, |best| best.max(value)));
            self.worst = Some(self.worst.map_or(value, |worst| worst.min(value)));
            if value >= self.tp {
                self.done = Some("tp");
                self.exit_ms = Some(ms(now.saturating_duration_since(entered)));
                return;
            }
        }
        if now.saturating_duration_since(entered) >= HORIZON {
            self.done = Some("horizon");
            self.exit_ms = Some(ms(now.saturating_duration_since(entered)));
        }
    }

    fn row(&self) -> Option<ArmRow> {
        let (at_ms, trigger) = self.trigger?;
        let fill = self.fill;
        Some(ArmRow {
            at_ms,
            trig_entry: f(trigger.entry),
            trig_net: trigger.net.map(f),
            fill_ms: fill.map(|(at, _, _)| at),
            entry: fill.map(|(_, s, _)| f(s.entry)),
            net: fill.and_then(|(_, s, _)| s.net.map(f)),
            net_zero: fill.map(|(_, s, _)| f(s.net_zero)),
            fee: fill.map(|(_, _, fee)| f(fee)),
            tp: f(self.tp),
            exit: self.done,
            exit_ms: self.exit_ms,
            exit_net: match self.done {
                Some("tp") | Some("horizon") => self.last.map(f),
                _ => None,
            },
            best: self.best.map(f),
            worst: self.worst.map(f),
            hist: self.hist,
        })
    }
}

/// 一个弱信号（从出现到中断）。
#[derive(Debug, Clone)]
struct Episode {
    key: HoldKey,
    started: Instant,
    started_at: DateTime<Utc>,
    last_seen: Instant,
    hold_sec: u64,
    /// 变成强信号的时刻（距出现，毫秒）。
    strong_ms: Option<u64>,
    /// 一出现就是强信号（或 0.5 秒内变强）：两种规则一样，不记。
    same: bool,
    lasted_ms: Option<u64>,
    /// 自动交易停看超过 [`HOLD_UNOBSERVED`]（开完一笔后隔 60 秒、拒单后退避 15 秒、暂停……）时还在的信号：
    /// 按断了处理（停看期间两种规则都不会开），`lasted_ms` 只是下限。虚拟仓位照常跟到平仓。
    cut: bool,
    quick: Arm,
    slow: Arm,
    written: bool,
}

/// 写到文件里的一行。
#[derive(Debug, Clone, Serialize)]
pub struct Row {
    /// 信号出现时刻（UTC）。
    pub t: DateTime<Utc>,
    pub s: String,
    pub long: Venue,
    pub short: Venue,
    pub hold_sec: u64,
    /// 信号持续了多久（毫秒）；`None` = 写的时候还在（超过 [`HORIZON`] 的长信号）。
    pub lasted_ms: Option<u64>,
    /// 信号是被自动交易停看截断的（`lasted_ms` 是下限）。
    pub cut: bool,
    pub strong_ms: Option<u64>,
    /// 0.5 秒规则那笔。
    pub quick: Option<ArmRow>,
    /// 现行规则那笔；信号没撑满保持时间（也没变强）时为 `None` —— 现行规则不会开。
    pub slow: Option<ArmRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArmRow {
    pub at_ms: u64,
    pub trig_entry: f64,
    pub trig_net: Option<f64>,
    pub fill_ms: Option<u64>,
    pub entry: Option<f64>,
    pub net: Option<f64>,
    pub net_zero: Option<f64>,
    pub fee: Option<f64>,
    pub tp: f64,
    pub exit: Option<&'static str>,
    pub exit_ms: Option<u64>,
    pub exit_net: Option<f64>,
    pub best: Option<f64>,
    pub worst: Option<f64>,
    pub hist: Option<bool>,
}

/// 小时汇总。
#[derive(Debug, Default, Clone)]
struct Tally {
    weak: u64,
    quick: u64,
    slow: u64,
    quick_only: u64,
    quick_net: f64,
    quick_done: u64,
    slow_net: f64,
    slow_done: u64,
}

pub struct Shadow {
    dir: Option<PathBuf>,
    live: HashMap<HoldKey, Episode>,
    ended: Vec<Episode>,
    last_observe: Option<Instant>,
    /// 上一次估平仓的时刻（[`EXIT_EVERY`]）。
    last_exits: Option<Instant>,
    /// 最近一次看到的单腿名义（汇总换算 USDT 用）。
    size: Decimal,
    tally: Tally,
    tally_since: Option<Instant>,
}

impl Shadow {
    pub fn new(dir: Option<PathBuf>) -> Self {
        Self {
            dir,
            live: HashMap::new(),
            ended: Vec::new(),
            last_observe: None,
            last_exits: None,
            size: Decimal::ZERO,
            tally: Tally::default(),
            tally_since: None,
        }
    }

    /// 这一行是不是在跟的组合（信号中或虚拟仓位，两个方向都算）。自动交易据此只把这几行交给
    /// [`Self::observe`]，不必每次把整份快照扣一遍资金费、建一遍索引。
    pub fn tracks(&self, line: &Line) -> bool {
        self.live.values().chain(self.ended.iter()).any(|episode| {
            let (base, long, short) = &episode.key;
            base == &line.base
                && ((*long == line.a && *short == line.b) || (*long == line.b && *short == line.a))
        })
    }

    /// 还有虚拟仓位在跟（自动交易停看时也要按行情跟下去）。
    pub fn has_open(&self) -> bool {
        !self.ended.is_empty() || !self.live.is_empty()
    }

    /// 自动交易每次检查调一次（与保持计时同一个快照）：认信号、触发、跟虚拟仓位。返回写完的行。
    /// `hist`：这个组合的历史分位门槛结论（缓存里没有为 `None`）。
    pub fn observe(
        &mut self,
        view: &View,
        settings: &Settings,
        hist: &dyn Fn(&HoldKey) -> Option<bool>,
        now: Instant,
        now_utc: DateTime<Utc>,
    ) -> Vec<Row> {
        // 有一阵子没在看：那段时间信号是否连续不知道（两种规则也都不会开），还在的按断了处理，
        // 已触发的虚拟仓位照常跟下去。
        if self
            .last_observe
            .is_some_and(|at| now.saturating_duration_since(at) >= HOLD_UNOBSERVED)
        {
            let keys: Vec<HoldKey> = self.live.keys().cloned().collect();
            for key in keys {
                self.end(&key, true);
            }
        }
        self.last_observe = Some(now);
        self.size = settings.size_usdt;

        let size = settings.size_usdt;
        let tp = match settings.take_profit_amount_for(size) {
            Some(amount) if size > Decimal::ZERO => amount / size * Decimal::ONE_HUNDRED,
            _ => Decimal::ZERO,
        };
        let mut current: HashMap<HoldKey, bool> = HashMap::new();
        for line in &view.lines {
            if let Some(candidate) = candidate_of(line, settings) {
                // 挂单候选不在这次评估的范围里（币安挂单早就关了）。
                if candidate.maker.is_some() {
                    continue;
                }
                let strong = settings.is_strong(&candidate);
                let entry = current.entry(candidate.hold_key()).or_insert(false);
                *entry |= strong;
            }
        }
        // 没有信号、也没有在跟的虚拟仓位（绝大多数时候）：不必给 1800 行建索引。
        let index = if current.is_empty() && !self.has_open() {
            Index::new()
        } else {
            index_of(view)
        };

        for (key, strong) in &current {
            let episode = self.live.entry(key.clone()).or_insert_with(|| {
                self.tally.weak += u64::from(!*strong);
                Episode {
                    key: key.clone(),
                    started: now,
                    started_at: now_utc,
                    last_seen: now,
                    hold_sec: settings.hold_sec,
                    strong_ms: None,
                    same: false,
                    lasted_ms: None,
                    cut: false,
                    quick: Arm::default(),
                    slow: Arm::default(),
                    written: false,
                }
            });
            episode.last_seen = now;
            let age = now.saturating_duration_since(episode.started);
            if *strong && episode.strong_ms.is_none() {
                episode.strong_ms = Some(ms(age));
            }
            if episode.same || episode.written {
                continue;
            }
            for arm in [&mut episode.quick, &mut episode.slow] {
                if arm.trigger.is_some() && arm.hist.is_none() {
                    arm.hist = hist(key);
                }
            }
            let Some(snap_now) = legs_of(&index, key).map(|(leg, _, _)| snap(leg)) else {
                continue;
            };
            if episode.quick.trigger.is_none() {
                if episode.strong_ms.is_some() {
                    // 0.5 秒之前就变强了：现行规则那一刻也会下，两种规则一样。
                    episode.same = true;
                    continue;
                }
                if age >= QUICK_HOLD {
                    episode.quick.fire(ms(age), snap_now, now, tp);
                    episode.quick.hist = hist(key);
                    self.tally.quick += 1;
                }
            }
            if episode.quick.trigger.is_some()
                && episode.slow.trigger.is_none()
                && (episode.strong_ms.is_some() || age >= Duration::from_secs(episode.hold_sec))
            {
                episode.slow.fire(ms(age), snap_now, now, tp);
                episode.slow.hist = hist(key);
                self.tally.slow += 1;
            }
        }

        // 连续 HOLD_GRACE 没出现：信号断了，转去只跟虚拟仓位。
        let gone: Vec<HoldKey> = self
            .live
            .iter()
            .filter(|(key, e)| {
                !current.contains_key(*key)
                    && now.saturating_duration_since(e.last_seen) >= HOLD_GRACE
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in gone {
            self.end(&key, false);
        }
        self.settle(&index, now)
    }

    /// 自动交易停看时（开完一笔后隔 60 秒、退避、暂停……）只跟虚拟仓位，不认新信号。
    pub fn follow_only(&mut self, view: &View, now: Instant) -> Vec<Row> {
        let index = index_of(view);
        self.settle(&index, now)
    }

    /// 信号结束：没触发的丢掉，触发了的转去只跟虚拟仓位。
    fn end(&mut self, key: &HoldKey, cut: bool) {
        let Some(mut episode) = self.live.remove(key) else {
            return;
        };
        episode.lasted_ms = Some(ms(episode
            .last_seen
            .saturating_duration_since(episode.started)));
        episode.cut = cut;
        if episode.quick.trigger.is_some() && episode.slow.trigger.is_none() {
            self.tally.quick_only += 1;
        }
        if !episode.written && episode.quick.trigger.is_some() {
            self.ended.push(episode);
        }
    }

    /// 跟虚拟仓位、写有结果的、到点打汇总。
    fn settle(&mut self, index: &Index<'_>, now: Instant) -> Vec<Row> {
        let mut out = Vec::new();
        let exits = self
            .last_exits
            .is_none_or(|at| now.saturating_duration_since(at) >= EXIT_EVERY);
        if exits {
            self.last_exits = Some(now);
        }
        for episode in self.live.values_mut().chain(self.ended.iter_mut()) {
            let legs = legs_of(index, &episode.key);
            let started = episode.started;
            episode.quick.follow(started, now, legs, exits);
            episode.slow.follow(started, now, legs, exits);
        }
        // 写：断了的且两笔都有结果；还在的超过 HORIZON 且两笔都有结果的也先写（之后不再记这个信号）。
        let mut keep = Vec::new();
        for mut episode in std::mem::take(&mut self.ended) {
            if episode.quick.settled() && episode.slow.settled() {
                out.extend(self.finish(&mut episode));
            } else {
                keep.push(episode);
            }
        }
        self.ended = keep;
        let mut long_rows = Vec::new();
        for episode in self.live.values_mut() {
            if !episode.written
                && episode.quick.trigger.is_some()
                && episode.quick.settled()
                && episode.slow.settled()
                && now.saturating_duration_since(episode.started) >= HORIZON
            {
                long_rows.push(episode.clone());
                episode.written = true;
            }
        }
        for mut episode in long_rows {
            out.extend(self.finish(&mut episode));
        }

        let size = self.size;
        let since = *self.tally_since.get_or_insert(now);
        if now.saturating_duration_since(since) >= SUMMARY_EVERY {
            let t = std::mem::take(&mut self.tally);
            let usdt = |pct: f64| pct * size.to_f64().unwrap_or(0.0) / 100.0;
            info!(
                "信号保持影子（近 1 小时，只记录不下单）：弱信号 {} 个，撑过 0.5 秒 {} 个，按现行规则会开 {} 个，只有 0.5 秒规则会开 {} 个；\
                 虚拟平仓 0.5 秒规则 {} 笔合计 {:+.2} USDT，现行规则 {} 笔合计 {:+.2} USDT（未含资金费与回滚）",
                t.weak,
                t.quick,
                t.slow,
                t.quick_only,
                t.quick_done,
                usdt(t.quick_net),
                t.slow_done,
                usdt(t.slow_net)
            );
            self.tally_since = Some(now);
        }
        out
    }

    fn finish(&mut self, episode: &mut Episode) -> Option<Row> {
        episode.written = true;
        let quick = episode.quick.row()?;
        let slow = episode.slow.row();
        for (arm, net, done) in [
            (
                Some(&quick),
                &mut self.tally.quick_net,
                &mut self.tally.quick_done,
            ),
            (
                slow.as_ref(),
                &mut self.tally.slow_net,
                &mut self.tally.slow_done,
            ),
        ] {
            if let Some(value) = arm.and_then(|a| a.exit_net) {
                *net += value;
                *done += 1;
            }
        }
        Some(Row {
            t: episode.started_at,
            s: episode.key.0.clone(),
            long: episode.key.1,
            short: episode.key.2,
            hold_sec: episode.hold_sec,
            lasted_ms: episode.lasted_ms,
            cut: episode.cut,
            strong_ms: episode.strong_ms,
            quick: Some(quick),
            slow,
        })
    }

    /// 后台追加到当天的文件（测试里没有目录就不写）。
    pub fn write(&self, rows: Vec<Row>, now: DateTime<Utc>) {
        let Some(dir) = self.dir.clone() else {
            return;
        };
        if rows.is_empty() {
            return;
        }
        tokio::spawn(async move {
            if let Err(error) = append(&dir, &rows, now).await {
                warn!("信号保持影子写不了文件：{error}");
            }
        });
    }
}

type Index<'a> = HashMap<(&'a str, Venue, Venue), &'a Line>;

fn index_of(view: &View) -> Index<'_> {
    view.lines
        .iter()
        .map(|line| ((line.base.as_str(), line.a, line.b), &**line))
        .collect()
}

/// 这个组合的（开仓方向, 反方向, 往返手续费）。
fn legs_of<'a>(index: &Index<'a>, key: &HoldKey) -> Option<(&'a Leg, Option<&'a Leg>, Decimal)> {
    let (base, long, short) = key;
    if let Some(line) = index.get(&(base.as_str(), *long, *short)) {
        return Some((
            line.long_a.as_ref()?,
            line.long_b.as_ref(),
            line.fee_round_trip_pct?,
        ));
    }
    let line = index.get(&(base.as_str(), *short, *long))?;
    Some((
        line.long_b.as_ref()?,
        line.long_a.as_ref(),
        line.fee_round_trip_pct?,
    ))
}

fn snap(leg: &Leg) -> Snap {
    Snap {
        entry: leg.quote.entry_pct,
        net: leg.net_to_normal_pct,
        net_zero: leg.net_to_zero_pct,
    }
}

fn ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn f(value: Decimal) -> f64 {
    value.round_dp(5).to_f64().unwrap_or(0.0)
}

fn file_for(dir: &Path, day: chrono::NaiveDate) -> PathBuf {
    dir.join(format!("{FILE_PREFIX}-{}.jsonl", day.format("%Y%m%d")))
}

async fn append(dir: &Path, rows: &[Row], now: DateTime<Utc>) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    tokio::fs::create_dir_all(dir).await?;
    let mut text = String::new();
    for row in rows {
        text.push_str(&serde_json::to_string(row).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    let path = file_for(dir, now.date_naive());
    let fresh = !tokio::fs::try_exists(&path).await.unwrap_or(true);
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    if fresh {
        let oldest = file_for(dir, (now - chrono::Duration::days(KEEP_DAYS)).date_naive());
        let head = format!("{FILE_PREFIX}-");
        if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
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
    Ok(())
}
