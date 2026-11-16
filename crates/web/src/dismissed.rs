//! 「最近结束」里被用户移除的仓位。
//!
//! **为什么不是删台账。** 台账是只追加的审计记录：对账、当日亏损闸门、熔断、盈亏核算都读它；往里写不认识的行，
//! 会被当成「台账有行无法解析」而停止交易。所以「移除」只是把仓位 id 记在台账旁边的 `<台账>.dismissed.json`
//! 里（原子替换、0600），列表默认不再显示它们。台账原样保留，**所有统计（当日盈亏、熔断、对账）照旧把它们算进去**；
//! 「显示已隐藏」随时能看到、也能恢复。
//!
//! **哪些能移除。** 只有已经结束（已平仓 / 已回滚）的仓位；还有敞口的永远不行。
//! 区分两类：
//! - **没成交过**（它的订单一笔都没成交：被交易所拒了、第一腿没吃到、准备阶段就没过）：什么都没发生，随便移除，
//!   页面还给「一键清理」；
//! - **有成交**（真的开过、回滚过或平过）：这是一笔真实交易的记录，只有请求里明确 `allow_traded` 才移除，
//!   页面会先让用户确认。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arb_exec::{OrderState, PairPosition, PositionStatus, Replayed};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{info, warn};

use crate::AppState;

/// 一次请求最多处理几个 id（防手误；「一键清理」也受它限制）。
pub const MAX_IDS: usize = 500;

/// 同一时刻只有一个请求在读改写清单文件。
static LOCK: Mutex<()> = Mutex::new(());

/// 清单里的一条：什么时候移除的、当时的合约与状态、有没有成交过（页面「显示已隐藏」时用来说明）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub at: DateTime<Utc>,
    pub symbol: String,
    pub status: String,
    pub traded: bool,
}

/// 仓位 id → 条目。
pub type Dismissed = BTreeMap<String, Entry>;

/// 清单文件：台账旁边的 `<台账>.dismissed.json`。
pub fn path_for(ledger_path: &str) -> PathBuf {
    let mut name = Path::new(ledger_path).as_os_str().to_owned();
    name.push(".dismissed.json");
    PathBuf::from(name)
}

/// 读清单。文件不存在是空；**读不懂也当空**（宁可多显示，不能因为一个坏文件把仓位藏起来），但会警告。
pub fn load(ledger_path: &str) -> Dismissed {
    let path = path_for(ledger_path);
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_else(|error| {
            warn!(path = %path.display(), "已移除清单读不懂，按没有隐藏任何仓位处理：{error}");
            Dismissed::new()
        }),
        Err(_) => Dismissed::new(),
    }
}

fn save(ledger_path: &str, list: &Dismissed) -> std::io::Result<()> {
    let path = path_for(ledger_path);
    let body = serde_json::to_string_pretty(list).map_err(std::io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    std::io::Write::write_all(&mut file, body.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// 这笔仓位有没有任何一张订单成交过（含回滚的平仓单）。订单号都以 `{仓位 id}-` 开头。
pub fn traded(position: &PairPosition, orders: &HashMap<String, OrderState>) -> bool {
    let prefix = format!("{}-", position.id);
    orders
        .iter()
        .any(|(id, order)| id.starts_with(&prefix) && order.filled_usdt > Decimal::ZERO)
}

/// 结束了的（已平仓 / 已回滚）仓位才能移除。
fn ended(position: &PairPosition) -> bool {
    matches!(
        position.status,
        PositionStatus::Closed | PositionStatus::Unwound
    )
}

/// 没成交过、还没被移除的已结束仓位（「一键清理」的对象），最近结束的在前。
pub fn failed_ids(replayed: &Replayed, dismissed: &Dismissed) -> Vec<String> {
    let mut found: Vec<&PairPosition> = replayed
        .positions
        .values()
        .filter(|position| {
            ended(position)
                && !dismissed.contains_key(&position.id)
                && !traded(position, &replayed.orders)
        })
        .collect();
    found.sort_by_key(|position| {
        std::cmp::Reverse(position.closed_at.unwrap_or(position.opened_at))
    });
    found
        .into_iter()
        .map(|position| position.id.clone())
        .collect()
}

/// 一次移除 / 恢复的结果。
#[derive(Debug, Default, Serialize, PartialEq, Eq)]
pub struct Outcome {
    /// 这次真正改动了的 id。
    pub changed: Vec<String>,
    /// 没改的 id 与原因（不存在、还有敞口、有成交但没有 `allow_traded`、已经移除过……）。
    pub skipped: Vec<(String, String)>,
}

/// 移除。逐个检查，不合格的跳过并说明原因，合格的一次写入。纯函数 + 一次写文件。
pub fn dismiss(
    ledger_path: &str,
    replayed: &Replayed,
    ids: &[String],
    allow_traded: bool,
) -> Result<Outcome, String> {
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut list = load(ledger_path);
    let mut outcome = Outcome::default();
    for id in ids {
        let Some(position) = replayed.positions.get(id) else {
            outcome
                .skipped
                .push((id.clone(), "台账里没有这个仓位".into()));
            continue;
        };
        if !ended(position) {
            outcome.skipped.push((
                id.clone(),
                "还没有结束（有敞口或在进行中），不能移除".into(),
            ));
            continue;
        }
        if list.contains_key(id) {
            outcome.skipped.push((id.clone(), "已经移除过".into()));
            continue;
        }
        let had_trades = traded(position, &replayed.orders);
        if had_trades && !allow_traded {
            outcome.skipped.push((
                id.clone(),
                "这笔有成交（真实交易记录），要确认后才能移除".into(),
            ));
            continue;
        }
        list.insert(
            id.clone(),
            Entry {
                at: Utc::now(),
                symbol: position.symbol.to_string(),
                status: format!("{:?}", position.status).to_ascii_lowercase(),
                traded: had_trades,
            },
        );
        outcome.changed.push(id.clone());
    }
    if !outcome.changed.is_empty() {
        save(ledger_path, &list).map_err(|error| format!("写不了已移除清单：{error}"))?;
    }
    Ok(outcome)
}

/// 恢复（取消移除）。
pub fn restore(ledger_path: &str, ids: &[String]) -> Result<Outcome, String> {
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut list = load(ledger_path);
    let mut outcome = Outcome::default();
    for id in ids {
        if list.remove(id).is_some() {
            outcome.changed.push(id.clone());
        } else {
            outcome
                .skipped
                .push((id.clone(), "不在已移除清单里".into()));
        }
    }
    if !outcome.changed.is_empty() {
        save(ledger_path, &list).map_err(|error| format!("写不了已移除清单：{error}"))?;
    }
    Ok(outcome)
}

#[derive(Debug, Deserialize)]
pub struct DismissBody {
    /// `paper` 或 `live`。
    mode: String,
    /// 要移除（或恢复）的仓位 id。
    #[serde(default)]
    ids: Vec<String>,
    /// 一键清理：移除所有「没成交过」的已结束仓位（`ids` 被忽略）。
    #[serde(default)]
    all_failed: bool,
    /// 允许移除有成交的仓位（页面确认之后才会带）。
    #[serde(default)]
    allow_traded: bool,
    /// 恢复而不是移除。
    #[serde(default)]
    restore: bool,
}

/// `POST /api/trade/dismiss`：移除 / 恢复「最近结束」里的仓位（只是隐藏，台账原样保留）。要令牌。
pub async fn api_dismiss(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<DismissBody>,
) -> Response {
    if let Err(denied) = state.trade.authorize(&headers) {
        return denied.into_response();
    }
    let bad = |status: StatusCode, message: &str| {
        (status, Json(json!({ "error": message }))).into_response()
    };
    let ledger = match body.mode.as_str() {
        "paper" => crate::ledger_path(),
        "live" => match crate::trade::live_ledger_path(&state) {
            Some(path) => path,
            None => {
                return bad(
                    StatusCode::BAD_REQUEST,
                    "看板没有开启实盘（ARB_WEB_LIVE=off）",
                );
            }
        },
        other => {
            return bad(
                StatusCode::BAD_REQUEST,
                &format!("mode 只能是 paper 或 live，收到 {other:?}"),
            );
        }
    };
    if !body.all_failed && body.ids.is_empty() {
        return bad(StatusCode::BAD_REQUEST, "没有给要处理的仓位 id");
    }
    if body.ids.len() > MAX_IDS {
        return bad(
            StatusCode::BAD_REQUEST,
            &format!("一次最多处理 {MAX_IDS} 笔"),
        );
    }
    let (replayed, _) = match arb_exec::replay_file(&ledger).await {
        Ok(read) => read,
        Err(error) => {
            return bad(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("读不了台账 {ledger}：{error}"),
            );
        }
    };
    let result = if body.restore {
        restore(&ledger, &body.ids)
    } else {
        let ids = if body.all_failed {
            let mut ids = failed_ids(&replayed, &load(&ledger));
            ids.truncate(MAX_IDS);
            ids
        } else {
            body.ids.clone()
        };
        // 一键清理只会碰「没成交过」的，不需要也不接受 allow_traded。
        let allow_traded = body.allow_traded && !body.all_failed;
        dismiss(&ledger, &replayed, &ids, allow_traded)
    };
    match result {
        Ok(outcome) => {
            info!(
                mode = %body.mode,
                restore = body.restore,
                changed = outcome.changed.len(),
                skipped = outcome.skipped.len(),
                "「最近结束」移除 / 恢复"
            );
            Json(json!({ "changed": outcome.changed, "skipped": outcome.skipped })).into_response()
        }
        Err(message) => bad(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::{Side, Symbol, Venue};
    use arb_exec::{ClientOrderId, MarginMode, NewOrder, Strategy, TaskRules};

    fn position(id: &str, status: PositionStatus) -> PairPosition {
        PairPosition {
            margin_mode: MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("ZEC", "USDT"),
            strategy: Strategy::Spread,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: Utc::now(),
            closed_at: Some(Utc::now()),
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
            planned: None,
        }
    }

    fn order(id: &str, filled: i64) -> (String, OrderState) {
        let mut state = OrderState::new(NewOrder {
            post_only: false,
            client_order_id: ClientOrderId(id.into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("ZEC", "USDT"),
            side: Side::Buy,
            notional_usdt: Decimal::from(50),
            quantity: None,
            limit_price: None,
            reduce_only: false,
            margin_mode: MarginMode::Isolated,
            leverage: None,
        });
        state.filled_usdt = Decimal::from(filled);
        (id.to_string(), state)
    }

    /// failed：第一腿没吃到（订单没成交）；real：开仓成交后平仓；open：还在持仓；bare：连订单都没有（准备阶段就停了）。
    fn ledger_with_cases() -> Replayed {
        let mut replayed = Replayed::default();
        for (id, status) in [
            ("failed", PositionStatus::Unwound),
            ("real", PositionStatus::Closed),
            ("open", PositionStatus::Open),
            ("bare", PositionStatus::Unwound),
        ] {
            replayed.positions.insert(id.into(), position(id, status));
        }
        for (id, filled) in [
            ("failed-buy-0", 0),
            ("real-buy-0", 50),
            ("real-sell-exit1", 50),
            ("open-buy-0", 50),
        ] {
            let (key, state) = order(id, filled);
            replayed.orders.insert(key, state);
        }
        replayed
    }

    fn temp_ledger(name: &str) -> String {
        let path =
            std::env::temp_dir().join(format!("arb-dismiss-{name}-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(path_for(path.to_str().unwrap()));
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn a_position_that_never_traded_is_told_apart_from_a_real_one() {
        let replayed = ledger_with_cases();
        let is = |id: &str| traded(&replayed.positions[id], &replayed.orders);
        assert!(!is("failed"), "订单一笔都没成交");
        assert!(!is("bare"), "连订单都没有");
        assert!(is("real"));
        assert!(is("open"));
        // 前缀要带分隔符：id `real` 不能把 `real2-…` 的订单算成自己的。
        let mut only: HashMap<String, OrderState> = HashMap::new();
        let (key, state) = order("real2-buy-0", 10);
        only.insert(key, state);
        assert!(!traded(&position("real", PositionStatus::Closed), &only));
    }

    #[test]
    fn only_ended_untraded_positions_are_offered_for_the_one_click_clean_up() {
        let replayed = ledger_with_cases();
        let mut ids = failed_ids(&replayed, &Dismissed::new());
        ids.sort();
        assert_eq!(ids, vec!["bare", "failed"]);
        let mut hidden = Dismissed::new();
        hidden.insert(
            "bare".into(),
            Entry {
                at: Utc::now(),
                symbol: "ZEC/USDT".into(),
                status: "unwound".into(),
                traded: false,
            },
        );
        assert_eq!(
            failed_ids(&replayed, &hidden),
            vec!["failed"],
            "已移除的不再提供"
        );
    }

    #[test]
    fn dismissing_checks_every_id_and_only_a_confirmed_request_may_hide_a_real_trade() {
        let replayed = ledger_with_cases();
        let ledger = temp_ledger("rules");
        let ids: Vec<String> = ["failed", "real", "open", "ghost", "failed"]
            .map(String::from)
            .to_vec();
        let outcome = dismiss(&ledger, &replayed, &ids, false).unwrap();
        // failed 移除；同一个 id 第二次是「已经移除过」；有成交的没确认不动；有敞口的和不存在的不动。
        assert_eq!(outcome.changed, vec!["failed"]);
        let reasons: HashMap<&str, &str> = outcome
            .skipped
            .iter()
            .map(|(id, why)| (id.as_str(), why.as_str()))
            .collect();
        assert!(reasons["real"].contains("有成交"));
        assert!(reasons["open"].contains("没有结束"));
        assert!(reasons["ghost"].contains("没有这个仓位"));
        assert_eq!(
            outcome
                .skipped
                .iter()
                .filter(|(id, _)| id == "failed")
                .count(),
            1
        );
        // 确认之后可以移除有成交的；清单落盘，重新读回来一样。
        let confirmed = dismiss(&ledger, &replayed, &["real".to_string()], true).unwrap();
        assert_eq!(confirmed.changed, vec!["real"]);
        let list = load(&ledger);
        assert_eq!(
            list.keys().cloned().collect::<Vec<_>>(),
            vec!["failed", "real"]
        );
        assert!(!list["failed"].traded && list["real"].traded);
        // 恢复。
        let back = restore(&ledger, &["real".to_string(), "nope".to_string()]).unwrap();
        assert_eq!(back.changed, vec!["real"]);
        assert_eq!(back.skipped.len(), 1);
        assert_eq!(
            load(&ledger).keys().cloned().collect::<Vec<_>>(),
            vec!["failed"]
        );
        let _ = std::fs::remove_file(path_for(&ledger));
    }

    #[test]
    fn an_unreadable_list_hides_nothing() {
        let ledger = temp_ledger("corrupt");
        std::fs::write(path_for(&ledger), "{ not json").unwrap();
        assert!(load(&ledger).is_empty());
        let _ = std::fs::remove_file(path_for(&ledger));
        assert!(load(&ledger).is_empty(), "没有文件也是空");
    }
}
