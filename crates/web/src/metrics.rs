//! `/metrics`：Prometheus 文本格式，给 24 小时运行的看板做外部监控与告警。
//!
//! 和 `/healthz` 同一个原则：**不需要令牌，所以只放计数、时间和布尔值**，没有地址、账户号、
//! 仓位 id 与金额。标签的取值范围都是有限的（场所名、仓位状态、操作类型），不会随运行时间膨胀。
//!
//! 两类指标：
//! - **读出来的**（gauge）：每次抓取时现算 —— 快照年龄、各场所上一轮扫描的成败与耗时、限频冷却、
//!   实盘规则轮与对账状态、台账里各状态的仓位数、进程内存。没有哪个后台任务专门为它们写数。
//! - **数出来的**（counter）：只能在事件发生时累加 —— 下单 / 平仓 / 规则操作的成败、令牌校验失败、
//!   后台任务 panic。放在本模块的静态原子量里，进程重启归零（Prometheus 的 `rate()` 按计数器重置处理）。

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// 交易台上的写操作类型（计数用）。
#[derive(Debug, Clone, Copy)]
pub enum Op {
    Open,
    Close,
    Monitor,
    Rules,
}

impl Op {
    const ALL: [Op; 4] = [Op::Open, Op::Close, Op::Monitor, Op::Rules];

    fn label(self) -> &'static str {
        match self {
            Op::Open => "open",
            Op::Close => "close",
            Op::Monitor => "monitor",
            Op::Rules => "rules",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// `[操作][0 = 成功, 1 = 失败]`。
static OPS: [[AtomicU64; 2]; 4] = [
    [AtomicU64::new(0), AtomicU64::new(0)],
    [AtomicU64::new(0), AtomicU64::new(0)],
    [AtomicU64::new(0), AtomicU64::new(0)],
    [AtomicU64::new(0), AtomicU64::new(0)],
];
static AUTH_FAILURES: AtomicU64 = AtomicU64::new(0);
static TASK_PANICS: AtomicU64 = AtomicU64::new(0);

/// 记一次写操作的结果。`ok` 看的是 HTTP 状态码：2xx 成功，其余（含 409 忙、503 停机）都算失败。
pub fn count_op(op: Op, ok: bool) {
    OPS[op.index()][usize::from(!ok)].fetch_add(1, Ordering::Relaxed);
}

/// 令牌校验失败一次，返回累计次数（调用方据此决定要不要写日志）。
pub fn count_auth_failure() -> u64 {
    AUTH_FAILURES.fetch_add(1, Ordering::Relaxed) + 1
}

/// 后台任务（扫描轮、规则轮）panic 了一次。
pub fn count_task_panic() {
    TASK_PANICS.fetch_add(1, Ordering::Relaxed);
}

/// 一次抓取要写出的内容。字段都是调用方现算好的，本模块只管排版，不碰状态。
pub struct Scrape {
    /// 当前快照多少秒了；首轮扫描还没完成为 `None`。
    pub snapshot_age_s: Option<f64>,
    pub venues: Vec<VenueSample>,
    pub live: Option<LiveSample>,
    /// 台账里各状态的仓位数（实盘）。
    pub positions: Vec<(String, usize)>,
    pub alerts_suppressed: u64,
    pub draining: bool,
    pub rss_bytes: Option<u64>,
}

pub struct VenueSample {
    pub venue: &'static str,
    pub ok: bool,
    pub rates: usize,
    pub elapsed_ms: u64,
    pub cooldown_s: f64,
}

pub struct LiveSample {
    pub trade_mode: bool,
    pub last_round_age_s: Option<i64>,
    pub reconciliation_clean: Option<bool>,
    pub dirty_rounds: u32,
    pub stalled: bool,
    pub opens_paused: bool,
    /// 实盘开着但账户暂时连不上。
    pub disconnected: bool,
}

/// 写成 Prometheus 文本格式。
pub fn render(scrape: &Scrape) -> String {
    let mut out = String::with_capacity(4096);
    header(
        &mut out,
        "arb_build_info",
        "gauge",
        "构建信息（值恒为 1）。",
    );
    let _ = writeln!(
        out,
        "arb_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );

    header(
        &mut out,
        "arb_draining",
        "gauge",
        "1 = 进程正在停机排空，不再受理新的下单 / 平仓 / 规则轮。",
    );
    let _ = writeln!(out, "arb_draining {}", u8::from(scrape.draining));

    if let Some(rss) = scrape.rss_bytes {
        header(
            &mut out,
            "process_resident_memory_bytes",
            "gauge",
            "进程常驻内存。",
        );
        let _ = writeln!(out, "process_resident_memory_bytes {rss}");
    }

    header(
        &mut out,
        "arb_snapshot_age_seconds",
        "gauge",
        "行情快照的年龄；超过扫描间隔的三倍就是扫描卡住了。首轮扫描完成前没有这条。",
    );
    if let Some(age) = scrape.snapshot_age_s {
        let _ = writeln!(out, "arb_snapshot_age_seconds {age:.3}");
    }

    header(
        &mut out,
        "arb_venue_up",
        "gauge",
        "场所上一轮扫描是否取数成功（1 / 0）。",
    );
    for sample in &scrape.venues {
        let _ = writeln!(
            out,
            "arb_venue_up{{venue=\"{}\"}} {}",
            sample.venue,
            u8::from(sample.ok)
        );
    }
    header(
        &mut out,
        "arb_venue_scan_milliseconds",
        "gauge",
        "场所上一轮取数耗时（毫秒，含失败的）。",
    );
    for sample in &scrape.venues {
        let _ = writeln!(
            out,
            "arb_venue_scan_milliseconds{{venue=\"{}\"}} {}",
            sample.venue, sample.elapsed_ms
        );
    }
    header(
        &mut out,
        "arb_venue_rates",
        "gauge",
        "场所上一轮取到的读数条数。",
    );
    for sample in &scrape.venues {
        let _ = writeln!(
            out,
            "arb_venue_rates{{venue=\"{}\"}} {}",
            sample.venue, sample.rates
        );
    }
    header(
        &mut out,
        "arb_venue_cooldown_seconds",
        "gauge",
        "场所因限频（429 / 418 / 业务限频码）还要冷却多久；0 = 没在冷却。",
    );
    for sample in &scrape.venues {
        let _ = writeln!(
            out,
            "arb_venue_cooldown_seconds{{venue=\"{}\"}} {:.0}",
            sample.venue, sample.cooldown_s
        );
    }

    if let Some(live) = &scrape.live {
        header(
            &mut out,
            "arb_live_trade_enabled",
            "gauge",
            "1 = 实盘可下单（trade），0 = 实盘只读（readonly）。没开实盘时没有 arb_live_* 指标。",
        );
        let _ = writeln!(out, "arb_live_trade_enabled {}", u8::from(live.trade_mode));
        if let Some(age) = live.last_round_age_s {
            header(
                &mut out,
                "arb_live_last_round_age_seconds",
                "gauge",
                "实盘规则轮上一次跑完多少秒了。",
            );
            let _ = writeln!(out, "arb_live_last_round_age_seconds {age}");
        }
        header(
            &mut out,
            "arb_live_rules_stalled",
            "gauge",
            "1 = 规则轮停了（超过三个周期没跑）：持仓没人自动平仓 / 减仓。",
        );
        let _ = writeln!(out, "arb_live_rules_stalled {}", u8::from(live.stalled));
        if let Some(clean) = live.reconciliation_clean {
            header(
                &mut out,
                "arb_live_reconciliation_clean",
                "gauge",
                "最近一次实盘对账是否干净（1 / 0）。不干净时规则轮不会自动操作。",
            );
            let _ = writeln!(out, "arb_live_reconciliation_clean {}", u8::from(clean));
        }
        header(
            &mut out,
            "arb_live_dirty_rounds",
            "gauge",
            "连续几轮对账不干净。",
        );
        let _ = writeln!(out, "arb_live_dirty_rounds {}", live.dirty_rounds);
        header(
            &mut out,
            "arb_live_opens_paused",
            "gauge",
            "1 = 开新仓被暂停（手动 /pause 或连续失败熔断）；平仓、规则、对账不受影响。",
        );
        let _ = writeln!(out, "arb_live_opens_paused {}", u8::from(live.opens_paused));
        header(
            &mut out,
            "arb_live_connected",
            "gauge",
            "1 = 实盘账户已连上；0 = 开着实盘但暂时连不上（下单、规则、对账暂停，后台每分钟重连）。",
        );
        let _ = writeln!(out, "arb_live_connected {}", u8::from(!live.disconnected));
    }

    if !scrape.positions.is_empty() {
        header(
            &mut out,
            "arb_ledger_positions",
            "gauge",
            "实盘台账里各状态的仓位数（unwound = 开仓没做完被回滚的）。",
        );
        for (status, count) in &scrape.positions {
            let _ = writeln!(out, "arb_ledger_positions{{status=\"{status}\"}} {count}");
        }
    }

    header(
        &mut out,
        "arb_trade_ops_total",
        "counter",
        "交易台写操作的次数（按 HTTP 状态码分成功 / 失败；后台规则轮不在内）。",
    );
    for op in Op::ALL {
        for (slot, result) in [(0, "ok"), (1, "error")] {
            let _ = writeln!(
                out,
                "arb_trade_ops_total{{op=\"{}\",result=\"{result}\"}} {}",
                op.label(),
                OPS[op.index()][slot].load(Ordering::Relaxed)
            );
        }
    }
    header(
        &mut out,
        "arb_auth_failures_total",
        "counter",
        "令牌校验失败的次数。",
    );
    let _ = writeln!(
        out,
        "arb_auth_failures_total {}",
        AUTH_FAILURES.load(Ordering::Relaxed)
    );
    header(
        &mut out,
        "arb_task_panics_total",
        "counter",
        "后台扫描 / 规则轮 panic 的次数（每次只作废那一轮，循环照常继续）。",
    );
    let _ = writeln!(
        out,
        "arb_task_panics_total {}",
        TASK_PANICS.load(Ordering::Relaxed)
    );
    header(
        &mut out,
        "arb_alerts_suppressed_total",
        "counter",
        "被静音 / 去重 / 限频挡掉的告警条数。",
    );
    let _ = writeln!(
        out,
        "arb_alerts_suppressed_total {}",
        scrape.alerts_suppressed
    );
    out
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// 进程常驻内存（字节）：读 `/proc/self/status` 的 `VmRSS`。不是 Linux 时为 `None`。
pub fn resident_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Scrape {
        Scrape {
            snapshot_age_s: Some(1.5),
            venues: vec![
                VenueSample {
                    venue: "binance",
                    ok: true,
                    rates: 740,
                    elapsed_ms: 812,
                    cooldown_s: 0.0,
                },
                VenueSample {
                    venue: "lighter-rh",
                    ok: false,
                    rates: 0,
                    elapsed_ms: 30,
                    cooldown_s: 118.0,
                },
            ],
            live: Some(LiveSample {
                trade_mode: true,
                last_round_age_s: Some(42),
                reconciliation_clean: Some(false),
                dirty_rounds: 2,
                stalled: false,
                opens_paused: true,
                disconnected: false,
            }),
            positions: vec![("open".into(), 2), ("unwound".into(), 1)],
            alerts_suppressed: 7,
            draining: false,
            rss_bytes: Some(123),
        }
    }

    /// Prometheus 文本格式的硬规则：每个指标族先有 `# TYPE`，样本行是 `名{标签} 值`，
    /// 不能有空值、NaN；否则抓取端整份丢弃。
    #[test]
    fn the_exposition_is_well_formed() {
        let text = render(&sample());
        let mut typed = std::collections::HashSet::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                typed.insert(rest.split_whitespace().next().unwrap().to_string());
            } else if !line.starts_with('#') {
                let (name, value) = line.rsplit_once(' ').expect(line);
                let family = name.split('{').next().unwrap();
                assert!(typed.contains(family), "{family} 的样本出现在 # TYPE 之前");
                assert!(value.parse::<f64>().is_ok_and(f64::is_finite), "{line}");
            }
        }
        assert!(text.contains("arb_venue_up{venue=\"lighter-rh\"} 0"));
        assert!(text.contains("arb_venue_cooldown_seconds{venue=\"lighter-rh\"} 118"));
        assert!(text.contains("arb_live_reconciliation_clean 0"));
        assert!(text.contains("arb_live_opens_paused 1"));
        assert!(text.contains("arb_live_connected 1"));
        assert!(text.contains("arb_ledger_positions{status=\"unwound\"} 1"));
    }

    #[test]
    fn counters_accumulate_per_operation_and_result() {
        // 全局计数器：只断言增量，别的测试并发累加也不影响。
        let before = |op: Op, slot: usize| OPS[op.index()][slot].load(Ordering::Relaxed);
        let (ok0, err0) = (before(Op::Close, 0), before(Op::Close, 1));
        count_op(Op::Close, true);
        count_op(Op::Close, false);
        count_op(Op::Close, false);
        assert!(before(Op::Close, 0) > ok0);
        assert!(before(Op::Close, 1) >= err0 + 2);
        let first = count_auth_failure();
        assert!(count_auth_failure() > first);
    }

    /// 没开实盘、首轮扫描没完成时没有对应指标，而不是写出假的 0。
    #[test]
    fn absent_state_is_omitted_not_zeroed() {
        let text = render(&Scrape {
            snapshot_age_s: None,
            venues: Vec::new(),
            live: None,
            positions: Vec::new(),
            alerts_suppressed: 0,
            draining: true,
            rss_bytes: None,
        });
        // 只看样本行：HELP / TYPE 头里也有指标名。
        let samples: Vec<&str> = text.lines().filter(|line| !line.starts_with('#')).collect();
        let has = |prefix: &str| samples.iter().any(|line| line.starts_with(prefix));
        assert!(!has("arb_snapshot_age_seconds"));
        assert!(!has("arb_live_"));
        assert!(!has("process_resident_memory_bytes"));
        assert!(text.contains("arb_draining 1"));
    }
}
