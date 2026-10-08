//! 暂停开新仓：持久化的开关，加上「连续开仓失败」熔断。
//!
//! **为什么要落盘。** 暂停原来只放内存：看板崩溃、被 OOM 杀掉、pm2 重启之后，开仓自动恢复 ——
//! 恰恰是「出了问题、操作者按了暂停、然后进程又重启了」这种最需要它记住的时刻。开关写在台账
//! 旁边的 `<台账>.pause` 文件里（原子替换），启动时读回来。
//!
//! **熔断。** 连续 [`BREAKER_FAILURES`] 笔开仓都以回滚收场（`Unwound`：没开成、或开到一半被拆掉），
//! 说明有什么东西坏了（保证金、限频、接口、价格保护参数），继续开只会一次次付回滚的手续费与
//! 穿价。熔断只拦**新开仓**，不碰平仓、规则、对账 —— 暂停的是「加风险」，不能连「减风险」一起停。
//! 熔断后要操作者看过原因、手动恢复；恢复时记下时刻，之后只数这个时刻之后的新失败，旧的失败
//! 不会让恢复后的第一次失败立刻再次熔断。

use std::path::{Path, PathBuf};

use arb_exec::{PairPosition, PositionStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 连续几笔开仓以回滚收场就熔断。
pub const BREAKER_FAILURES: usize = 3;

/// 落盘的状态。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PauseState {
    pub paused: bool,
    /// 为什么暂停（手动 / 熔断）。
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub since: Option<DateTime<Utc>>,
    /// 熔断只数这个时刻之后开的仓位：上一次恢复的时刻。
    #[serde(default)]
    pub failures_after: Option<DateTime<Utc>>,
}

/// `<台账>.pause`。
pub fn path_for(ledger: &Path) -> PathBuf {
    let mut name = ledger.as_os_str().to_owned();
    name.push(".pause");
    PathBuf::from(name)
}

/// 读开关。文件不存在 = 没暂停过；**文件存在但读不懂 = 当作暂停**：状态不明时拦住新仓
/// 比放开安全，由操作者看过再 `/resume`（那会重写这个文件）。
pub fn load(path: &Path) -> PauseState {
    match std::fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|error| PauseState {
            paused: true,
            reason: Some(format!(
                "暂停状态文件读不懂（{error}）：按已暂停处理，核对后 /resume"
            )),
            since: Some(Utc::now()),
            failures_after: None,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => PauseState::default(),
        Err(error) => PauseState {
            paused: true,
            reason: Some(format!("暂停状态文件读不了（{error}）：按已暂停处理")),
            since: Some(Utc::now()),
            failures_after: None,
        },
    }
}

/// 原子地写开关：先写临时文件、fsync、再 rename。
pub fn save(path: &Path, state: &PauseState) -> std::io::Result<()> {
    use std::io::Write as _;
    let tmp = {
        let mut name = path.as_os_str().to_owned();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(&serde_json::to_vec_pretty(state).map_err(std::io::Error::other)?)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// 最近连续几笔开仓是以回滚收场的（只数 `after` 之后开的）。
///
/// 从最新的一笔往回数：`Unwound` 算一次失败，遇到任何别的结局（成功开成的 `Open` / `Closed` /
/// `Closing` / ……，或还在进行的 `Opening` / `Unwinding`）就停 —— 成功开过一笔，说明问题已经过去。
pub fn consecutive_failed_opens<'a>(
    positions: impl IntoIterator<Item = &'a PairPosition>,
    after: Option<DateTime<Utc>>,
) -> usize {
    let mut recent: Vec<&PairPosition> = positions
        .into_iter()
        .filter(|position| after.is_none_or(|floor| position.opened_at > floor))
        .collect();
    recent.sort_by_key(|position| std::cmp::Reverse(position.opened_at));
    recent
        .iter()
        .take_while(|position| position.status == PositionStatus::Unwound)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Symbol;
    use arb_exec::{Strategy, TaskRules};
    use rust_decimal::Decimal;

    fn position(id: &str, status: PositionStatus, minutes_ago: i64) -> PairPosition {
        PairPosition {
            margin_mode: arb_exec::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: Utc::now() - chrono::Duration::minutes(minutes_ago),
            closed_at: None,
            note: None,
            leverage: None,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        }
    }

    #[test]
    fn only_the_newest_unbroken_run_of_rollbacks_counts() {
        use PositionStatus::*;
        let history = [
            position("a", Unwound, 50),
            position("b", Closed, 40),
            position("c", Unwound, 30),
            position("d", Unwound, 20),
            position("e", Unwound, 10),
        ];
        assert_eq!(consecutive_failed_opens(&history, None), 3);
        // 一笔成功开成的在中间，就把计数截断。
        let healed = [
            position("c", Unwound, 30),
            position("d", Open, 20),
            position("e", Unwound, 10),
        ];
        assert_eq!(consecutive_failed_opens(&healed, None), 1);
        // 还在进行的不算失败也不算成功：从它就停。
        let running = [position("x", Unwound, 20), position("y", Opening, 5)];
        assert_eq!(consecutive_failed_opens(&running, None), 0);
    }

    /// 恢复之后只数新失败：旧的三次失败不能让恢复后的第一次失败立刻再次熔断。
    #[test]
    fn failures_before_the_last_resume_are_forgotten() {
        use PositionStatus::*;
        let history = [
            position("a", Unwound, 30),
            position("b", Unwound, 20),
            position("c", Unwound, 10),
        ];
        let resumed = Some(Utc::now() - chrono::Duration::minutes(5));
        assert_eq!(consecutive_failed_opens(&history, resumed), 0);
        let again = [history.to_vec(), vec![position("d", Unwound, 1)]].concat();
        assert_eq!(consecutive_failed_opens(&again, resumed), 1);
    }

    #[test]
    fn the_switch_survives_a_restart_and_an_unreadable_file_means_paused() {
        let dir = std::env::temp_dir().join(format!("arb-pause-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = path_for(&dir.join("ledger.jsonl"));
        let _ = std::fs::remove_file(&path);

        assert_eq!(load(&path), PauseState::default(), "没有文件 = 没暂停");
        let state = PauseState {
            paused: true,
            reason: Some("连续 3 笔开仓回滚".into()),
            since: Some(Utc::now()),
            failures_after: None,
        };
        save(&path, &state).unwrap();
        assert_eq!(load(&path), state, "重启后读回来的必须是同一个状态");

        std::fs::write(&path, "{ 写到一半").unwrap();
        let unreadable = load(&path);
        assert!(unreadable.paused, "读不懂时宁可拦住新仓");
        assert!(unreadable.reason.unwrap().contains("读不懂"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
