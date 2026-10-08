//! 优雅停机：收到 SIGINT / SIGTERM 后等进行中的下单做完再退出。
//!
//! pm2 / systemd 停进程时先发 SIGINT 或 SIGTERM。没人处理的话进程当场退出：恰好在下单
//! （两条腿之间）就留下一条裸腿，只能等重启后的对账事后发现。这里的做法：
//!
//! 1. 停止接受新连接，进行中的 HTTP 请求跑完；
//! 2. 等纸面与实盘交易台上的操作（下单、平仓、一轮规则）结束，并**一直持有**两把锁 ——
//!    此后后台规则轮和任何新请求都拿不到锁，不会在退出前又开一笔；
//! 3. 整个排空有时限（`ARB_SHUTDOWN_GRACE_SEC`，默认 120 秒），超时照样退出并告警；
//! 4. 第二个信号立即退出，给操作者一条出路。
//!
//! pm2 的 `kill_timeout` 必须大于这个时限，否则 pm2 会在排空中途 SIGKILL（见
//! `ecosystem.config.js`）。

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::{Instant, timeout_at};

/// 进程正在停机：不再开始新的下单 / 平仓 / 规则轮。整个进程一份，所以放在这里而不是穿过
/// 每一层传参；只有 [`begin_drain`] 会置位，且不会再复位（停机不可撤销）。
static DRAINING: AtomicBool = AtomicBool::new(false);

/// 开始排空：此后 [`is_draining`] 恒为真。
pub fn begin_drain() {
    DRAINING.store(true, Ordering::SeqCst);
}

/// 是否已经开始停机。
pub fn is_draining() -> bool {
    DRAINING.load(Ordering::SeqCst)
}

/// 排空时限的默认值：一次实盘开仓（对账、现扫、两条腿加回查）实测几秒到二十几秒，
/// 加上一次回滚重试，两分钟足够。
const DEFAULT_GRACE_SEC: u64 = 120;
/// 上限：必须小于 pm2 的 `kill_timeout`（`ecosystem.config.js` 按「时限 + 30 秒」推出它）。
/// 排空时限比 `kill_timeout` 长，pm2 会在排空中途 SIGKILL。再长也没有意义：那意味着有一笔操作已经卡死。
const MAX_GRACE_SEC: u64 = 300;

/// 从 `ARB_SHUTDOWN_GRACE_SEC` 读排空时限。填错直接报错：停机策略静默回落到默认值，
/// 会让 pm2 的 `kill_timeout` 与它对不上。
pub fn grace_from_env() -> Result<Duration> {
    grace_from(std::env::var("ARB_SHUTDOWN_GRACE_SEC").ok().as_deref())
}

fn grace_from(raw: Option<&str>) -> Result<Duration> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(Duration::from_secs(DEFAULT_GRACE_SEC));
    };
    match raw.parse::<u64>() {
        Ok(sec) if (1..=MAX_GRACE_SEC).contains(&sec) => Ok(Duration::from_secs(sec)),
        _ => bail!("ARB_SHUTDOWN_GRACE_SEC={raw} 不合法：要 1 ~ {MAX_GRACE_SEC} 秒的整数"),
    }
}

/// SIGINT 与 SIGTERM。
pub struct Signals {
    interrupt: Signal,
    terminate: Signal,
}

impl Signals {
    /// 注册处理器。从这一刻起信号不再直接杀进程，而是排队等 [`Signals::recv`]。
    pub fn install() -> io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// 等下一个信号，返回它的名字。
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }
}

/// 排空结果：持有各交易台的锁直到被丢弃，`busy` 是时限内没等到的交易台。
pub struct Quiesced<'a> {
    _guards: Vec<MutexGuard<'a, ()>>,
    pub busy: Vec<&'static str>,
}

/// 依次等各交易台的操作锁，到 `deadline` 为止；拿到的锁一直持有。
///
/// 时限过了之后，空闲的锁仍然拿得到（`timeout_at` 先轮询内部 future 再看时钟），
/// 所以前面一家等了很久不会让后面空闲的一家被误报成忙。
pub async fn quiesce<'a>(
    desks: &[(&'static str, &'a Mutex<()>)],
    deadline: Instant,
) -> Quiesced<'a> {
    let mut guards = Vec::with_capacity(desks.len());
    let mut busy = Vec::new();
    for (name, lock) in desks {
        match timeout_at(deadline, lock.lock()).await {
            Ok(guard) => guards.push(guard),
            Err(_) => busy.push(*name),
        }
    }
    Quiesced {
        _guards: guards,
        busy,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn grace_defaults_and_rejects_nonsense() {
        assert_eq!(grace_from(None).unwrap(), Duration::from_secs(120));
        assert_eq!(grace_from(Some("  ")).unwrap(), Duration::from_secs(120));
        assert_eq!(grace_from(Some("45")).unwrap(), Duration::from_secs(45));
        assert_eq!(grace_from(Some("300")).unwrap(), Duration::from_secs(300));
        for bad in ["0", "301", "-5", "1.5", "两分钟"] {
            assert!(grace_from(Some(bad)).is_err(), "{bad} 应该被拒绝");
        }
    }

    #[tokio::test]
    async fn quiesce_waits_for_a_running_operation_then_holds_the_lock() {
        let desk = Arc::new(Mutex::new(()));
        let running = Arc::clone(&desk);
        let operation = tokio::spawn(async move {
            let _guard = running.lock().await;
            tokio::time::sleep(Duration::from_millis(80)).await;
        });
        tokio::task::yield_now().await;

        let deadline = Instant::now() + Duration::from_secs(5);
        let quiesced = quiesce(&[("实盘", &*desk)], deadline).await;
        assert!(quiesced.busy.is_empty());
        assert!(operation.is_finished(), "必须等到操作结束才算排空");
        // 排空之后别人（后台规则轮、新请求）拿不到锁。
        assert!(desk.try_lock().is_err());
        drop(quiesced);
        assert!(desk.try_lock().is_ok());
    }

    #[tokio::test]
    async fn quiesce_reports_the_desk_that_outlives_the_deadline() {
        let stuck = Arc::new(Mutex::new(()));
        let idle = Mutex::new(());
        let _held = stuck.lock().await;

        let deadline = Instant::now() + Duration::from_millis(50);
        let quiesced = quiesce(&[("实盘", &*stuck), ("纸面", &idle)], deadline).await;
        // 前面一家等到时限，后面空闲的一家照样拿到，不被误报。
        assert_eq!(quiesced.busy, vec!["实盘"]);
        assert!(idle.try_lock().is_err());
    }
}
