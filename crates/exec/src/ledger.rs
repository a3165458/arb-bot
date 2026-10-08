//! 追加式台账。
//!
//! 为什么是**追加式**而不是「最新状态覆盖」：执行层最需要回答的问题是「当时到底
//! 发生了什么」。覆盖式存储只能告诉你当前状态，而事故总是发生在状态转移的缝里
//! —— 「第一腿成交了、第二腿超时」这种缝，覆盖式一写就没了。
//!
//! 格式是 JSONL：一行一个记录，可以直接 `tail`、`grep`、灌进任何工具，不需要
//! 数据库驱动。行数不多（每笔交易几条），换来的可审计性远比省下的空间值钱。

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::types::{OrderState, PairPosition};

/// 台账里的一行。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Order(Box<OrderState>),
    Position(Box<PairPosition>),
}

impl Record {
    /// 这一行的主键。重放时按它取最新值。
    pub fn key(&self) -> &str {
        match self {
            Record::Order(order) => &order.order.client_order_id.0,
            Record::Position(position) => &position.id,
        }
    }
}

/// 重放结果：每个主键的最新状态。
#[derive(Debug, Default)]
pub struct Replayed {
    pub orders: HashMap<String, OrderState>,
    pub positions: HashMap<String, PairPosition>,
}

impl Replayed {
    /// 还有敞口的仓位。对账与风控只看这些。
    pub fn exposed(&self) -> Vec<&PairPosition> {
        self.positions
            .values()
            .filter(|position| position.status.has_exposure())
            .collect()
    }
}

pub struct Ledger {
    path: PathBuf,
    file: Mutex<tokio::fs::File>,
}

impl Ledger {
    /// 打开（不存在则创建）台账文件。目录会被自动创建。
    pub async fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        // 崩溃可能留下**半行**（写到一半断电）。`append` 在 `sync_data` 之后才算成功，所以半行
        // 从来不是一条已确认的记录：调用方没有拿到 `Ok`，订单意图没发出去，仓位记录也还停在
        // 上一条。把它挪进 `.torn` 旁路文件（留证）并从台账里截掉 —— 不能只补个换行封口：
        // 封口后它就成了一条永久解析不了的坏行，对账看见坏行会拒绝一切开仓与规则。
        // 例外：只缺换行的完整记录（崩溃正好断在换行之前）照常保留，补上换行。
        if let Ok(content) = tokio::fs::read(&path).await
            && !content.is_empty()
            && !content.ends_with(b"\n")
        {
            let start = content
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |at| at + 1);
            let tail = &content[start..];
            if serde_json::from_slice::<Record>(tail).is_ok() {
                let mut repair = tokio::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .await?;
                repair.write_all(b"\n").await?;
                repair.sync_data().await?;
            } else {
                let mut sidecar = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(torn_path(&path))
                    .await?;
                sidecar.write_all(tail).await?;
                sidecar.write_all(b"\n").await?;
                sidecar.sync_data().await?;
                let truncate = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .await?;
                truncate.set_len(start as u64).await?;
                truncate.sync_data().await?;
                tracing::warn!(
                    path = %path.display(),
                    bytes = tail.len(),
                    "台账末尾有写到一半的记录（上次崩溃的现场）：已移到 .torn 旁路文件并从台账截掉"
                );
            }
        }

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 追加一行并调用 `sync_data` 持久化；订单意图必须在网络提交前落盘。
    pub async fn append(&self, record: &Record) -> std::io::Result<()> {
        let mut line = serde_json::to_string(record)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        line.push('\n');
        let mut file = self.file.lock().await;
        file.write_all(line.as_bytes()).await?;
        file.sync_data().await
    }

    /// 这个仓位 id 的全部历史记录（最旧在前）。结束后的仓位已经清掉了两条腿，事后要拿
    /// 开仓时的腿（价格、数量）就得回头翻它。
    pub async fn position_history(&self, id: &str) -> std::io::Result<Vec<PairPosition>> {
        let content = read_complete_lines(&self.path).await?;
        Ok(content
            .lines()
            .filter_map(|line| serde_json::from_str::<Record>(line).ok())
            .filter_map(|record| match record {
                Record::Position(position) if position.id == id => Some(*position),
                _ => None,
            })
            .collect())
    }

    /// 重放整个台账。坏行只跳过并计数，不让一行脏数据毁掉整份历史。
    pub async fn replay(&self) -> std::io::Result<(Replayed, usize)> {
        replay_file(&self.path).await
    }
}

/// 只读地重放一份台账文件：不创建、不修补、不追加。
///
/// 看板用它展示持仓。[`Ledger::open`] 会创建文件并给半行补换行，那是写操作 ——
/// 只读的一方不该改动执行方正在追加的文件。文件不存在时返回空结果。
pub async fn replay_file(path: impl AsRef<Path>) -> std::io::Result<(Replayed, usize)> {
    let content = read_complete_lines(path.as_ref()).await?;

    let mut replayed = Replayed::default();
    let mut broken = 0usize;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Record>(line) {
            Ok(Record::Order(order)) => {
                replayed
                    .orders
                    .insert(order.order.client_order_id.0.clone(), *order);
            }
            Ok(Record::Position(position)) => {
                replayed.positions.insert(position.id.clone(), *position);
            }
            Err(_) => broken += 1,
        }
    }
    Ok((replayed, broken))
}

/// 台账旁路文件：崩溃留下的半行被挪到这里。
fn torn_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".torn");
    PathBuf::from(name)
}

/// 读出台账里**写完整了**的部分（到最后一个换行为止）。文件不存在返回空串。
///
/// 末尾没有换行的那一段要么是另一个任务正在追加的一行（读到一半），要么是崩溃留下的半行
/// （下次 [`Ledger::open`] 会处理）：两种都不是已确认的记录，所以不解析、也不算坏行。
/// 按字节切分后再解码：半行可能正好断在一个多字节字符中间，整体 `read_to_string` 会因此
/// 报错，让同一时刻的另一个读者（看板刷新）连带读不了整份台账。
async fn read_complete_lines(path: &Path) -> std::io::Result<String> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error),
    };
    let end = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1);
    Ok(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        ClientOrderId, NewOrder, OrderState, PairPosition, PositionStatus, Strategy, TaskRules,
    };
    use arb_core::{Side, Symbol, Venue};
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn order(id: &str, notional: Decimal) -> OrderState {
        OrderState::new(NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId(id.into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: notional,
            limit_price: None,
            quantity: None,
            reduce_only: false,
            leverage: None,
        })
    }

    fn position(id: &str, status: PositionStatus) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: dec!(0.1),
            expected_round_trip_cost: dec!(0.002),
            status,
            opened_at: Utc::now(),
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        }
    }

    #[test]
    fn a_position_line_written_before_leverage_and_rules_still_replays() {
        // 加入杠杆 / 规则 / 保证金字段之前的台账行形状。
        let line = r#"{"kind":"position","id":"paper-1","symbol":"BTC/USDT","strategy":"funding","long":{"venue":"binance","side":"buy","notional_usdt":"1000","average_price":"100","fee_usdt":"0.5","client_order_id":"paper-1-buy-0"},"short":null,"entry_basis_pct":"0.1","expected_round_trip_cost":"0.002","status":"opening","opened_at":"2026-09-22T10:00:00Z","closed_at":null,"note":null}"#;
        let Record::Position(position) = serde_json::from_str::<Record>(line).unwrap() else {
            panic!("应当解析成仓位");
        };
        assert_eq!(position.leverage, None);
        assert!(position.rules.is_empty(), "旧仓位没有任何规则");
        assert_eq!(position.trims, 0);
        assert_eq!(position.long.unwrap().margin_usdt, None);
    }

    #[tokio::test]
    async fn replay_keeps_the_latest_state_per_key() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();

        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        let mut progressed = order("o1", dec!(100));
        progressed.status = crate::types::OrderStatus::Filled;
        progressed.filled_usdt = dec!(100);
        ledger
            .append(&Record::Order(Box::new(progressed)))
            .await
            .unwrap();

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        assert_eq!(replayed.orders.len(), 1, "同一个 key 只保留最新一条");
        assert_eq!(
            replayed.orders["o1"].status,
            crate::types::OrderStatus::Filled
        );

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 崩溃留下的半行：重启时挪到旁路文件、从台账截掉，而不是封口成永久的坏行
    /// （坏行会让对账拒绝一切开仓与规则）。追加的后续记录不受牵连。
    #[tokio::test]
    async fn a_torn_tail_is_quarantined_not_sealed_into_a_permanent_bad_line() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-torn-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let _ = tokio::fs::remove_file(torn_path(&path)).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        drop(ledger);

        // 半行写入（崩溃现场），故意断在多字节字符中间：整体 `read_to_string` 会因此报错。
        let torn = "{\"kind\":\"order\",\"note\":\"第一腿".as_bytes();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(&torn[..torn.len() - 1]).await.unwrap();
        drop(file);

        // 重启前别的读者（看板刷新）读到它：不报错、不当坏行。
        let (seen, broken) = replay_file(&path).await.unwrap();
        assert_eq!((seen.orders.len(), broken), (1, 0));

        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0, "半行不能留下坏行：对账见到坏行会拒绝一切操作");
        assert_eq!(replayed.orders.len(), 2);
        assert!(replayed.orders.contains_key("o2"));
        let kept = tokio::fs::read(torn_path(&path)).await.unwrap();
        assert!(kept.starts_with(&torn[..torn.len() - 1]), "半行要留证");

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_file(torn_path(&path)).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 只缺换行的完整记录是一条真记录：保留，补上换行。
    #[tokio::test]
    async fn a_complete_record_missing_only_its_newline_is_kept() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-nl-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let line = serde_json::to_string(&Record::Order(Box::new(order("o1", dec!(100))))).unwrap();
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(&path, line).await.unwrap();

        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!((replayed.orders.len(), broken), (2, 0));

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 文件中间的坏行（不是崩溃的尾巴）仍然只被计数：对账据此拒绝操作，由人核对。
    #[tokio::test]
    async fn a_corrupt_line_in_the_middle_is_counted_and_does_not_destroy_the_history() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-bad-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"not json at all\n").await.unwrap();
        drop(file);
        ledger
            .append(&Record::Order(Box::new(order("o2", dec!(200)))))
            .await
            .unwrap();

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 1, "坏行被计数");
        assert_eq!(replayed.orders.len(), 2, "其余记录照常重放");

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn only_positions_with_exposure_are_reported() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-exp-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "open",
                PositionStatus::Open,
            ))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "closed",
                PositionStatus::Closed,
            ))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "naked",
                PositionStatus::Unwinding,
            ))))
            .await
            .unwrap();

        let (replayed, _) = ledger.replay().await.unwrap();
        let exposed: Vec<&str> = replayed.exposed().iter().map(|p| p.id.as_str()).collect();
        assert!(exposed.contains(&"open"));
        assert!(exposed.contains(&"naked"), "回滚中的仓位仍然有敞口");
        assert!(!exposed.contains(&"closed"));

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
