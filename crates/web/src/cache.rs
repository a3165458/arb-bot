//! 扫描快照缓存与后台刷新。
//!
//! 一次扫描要并发打 11 家交易所的公共接口（实测约 3 秒、7000+ 条读数）。让每个
//! HTTP 请求各自触发一次扫描，等于把上游请求数乘上并发用户数 —— Aster 这类按 IP
//! 计权的接口会从 429 直接升级成封禁。
//!
//! 所以：后台按固定间隔刷新一份**全量**快照，请求只读快照并在其上重排名
//! （重排名是纯计算，微秒级，还让「持有天数」这种参数可以即时生效）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::Settings;
use arb_scanner::{ScanReport, scan};
use arb_venues::VenueApi;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

/// 一份扫描快照。
pub struct Snapshot {
    pub report: ScanReport,
    pub taken_at: Instant,
}

impl Snapshot {
    pub fn age(&self) -> Duration {
        self.taken_at.elapsed()
    }
}

#[derive(Default)]
pub struct ScanCache {
    inner: RwLock<Option<Arc<Snapshot>>>,
}

impl ScanCache {
    pub async fn get(&self) -> Option<Arc<Snapshot>> {
        self.inner.read().await.clone()
    }

    pub async fn put(&self, report: ScanReport) {
        let mut guard = self.inner.write().await;
        *guard = Some(Arc::new(Snapshot {
            report,
            taken_at: Instant::now(),
        }));
    }
}

/// 启动后台刷新循环。
///
/// 循环体不因单次失败退出：一次全场所失败（网络抖动、被限频）不该让面板永久停在
/// 旧数据上，下一轮会自己恢复。
pub fn spawn_refresher(
    apis: Arc<Vec<Arc<dyn VenueApi>>>,
    settings: Settings,
    cache: Arc<ScanCache>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(settings.scan_interval_sec.max(5));
        loop {
            let started = Instant::now();
            // 排名里有未检查的 Decimal 运算（溢出会 panic）：panic 发生在这个循环里就会永久终止
            // 它，快照从此冻结而进程照常在线。放进独立任务，panic 只作废这一轮。
            let round = {
                let apis = Arc::clone(&apis);
                let settings = settings.clone();
                tokio::spawn(async move { scan(&apis, &settings).await })
            };
            let report = match round.await {
                Ok(report) => report,
                Err(join_error) => {
                    crate::metrics::count_task_panic();
                    error!(%join_error, "扫描任务异常退出（panic）：保留上一份快照，下一轮重试");
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };
            let elapsed = started.elapsed();
            info!(
                elapsed_ms = elapsed.as_millis() as u64,
                venues_failed = report.totals.venues_failed,
                "快照已刷新"
            );
            if report.totals.venues_ok == 0 {
                warn!("本轮没有任何场所应答，保留上一份快照");
            } else {
                cache.put(report).await;
            }
            // 扫描本身耗时要从间隔里扣掉，否则实际周期会漂成「间隔 + 扫描耗时」。
            tokio::time::sleep(interval.saturating_sub(elapsed).max(Duration::from_secs(1))).await;
        }
    })
}
