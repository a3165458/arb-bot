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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
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
#[derive(Debug, Default, Clone)]
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
    /// 这个进程里追加过几次（成功与否都算）：缓存「上次读台账时的结论」的调用方拿它判断台账变没变。
    generation: AtomicU64,
    /// [`Ledger::replay`] 已经解析过的前缀。
    replayed: Mutex<Option<ReplayCache>>,
}

/// 台账已解析的前缀：到第几个字节（总在一个换行之后）、那时的重放结果。
///
/// 整本重放是纯 CPU 解析：实盘台账几 MB、几千行时，一次几十毫秒，下单前要做三次，
/// 而且台账只会越来越长。追加式台账只在末尾长，所以只解析新追加的行。
struct ReplayCache {
    offset: u64,
    /// 前缀最后这几个字节：下次接着读之前先核对，对不上（文件被别的工具改过）就整本重来。
    tail: Vec<u8>,
    replayed: Replayed,
    broken: usize,
    full_at: Instant,
}

/// 接着读之前核对的前缀末尾字节数。
const REPLAY_TAIL: u64 = 256;
/// 至少隔多久整本重放一次：万一有改动没被末尾核对发现，最多错这么久。
const FULL_REPLAY_EVERY: Duration = Duration::from_secs(300);

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
            generation: AtomicU64::new(0),
            replayed: Mutex::new(None),
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
        // 先记一次再写：写到一半失败的也算「变过」，依赖旧结论的调用方宁可重读。
        self.generation.fetch_add(1, Ordering::SeqCst);
        // 写之前的长度：写失败时截回这里。不截的话，文件末尾留着半行（磁盘满时常见），下一次写成功的
        // 那条记录会接在半行后面、合成一行解析不了的坏行 —— 那条记录在重放里消失，对账看见坏行还会
        // 拒绝一切开仓与规则，直到人工修台账。
        let before = file.metadata().await?.len();
        let written = match file.write_all(line.as_bytes()).await {
            // tokio 的 `write_all` 只是把数据交给后台线程，写失败（磁盘满、IO 错误）要到 `flush` 才报出来；
            // 直接 `sync_data` 会把这个错误吞掉、返回成功，留到下一次写才报，台账从此多一条半行。
            Ok(()) => match file.flush().await {
                Ok(()) => file.sync_data().await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        if let Err(error) = written {
            match file.set_len(before).await {
                Ok(()) => tracing::warn!(
                    path = %self.path.display(), %error,
                    "台账写入失败：已截回写之前的长度，没有留下半行"
                ),
                Err(cut) => tracing::error!(
                    path = %self.path.display(), %error, %cut,
                    "台账写入失败，截回也失败：末尾可能有半行，重启时会移到 .torn 旁路文件"
                ),
            }
            return Err(error);
        }
        Ok(())
    }

    /// 追加过几次（见字段说明）。两次读到的值相同 = 这期间本进程没写过台账。
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// 这个仓位 id 的全部历史记录（最旧在前）。结束后的仓位已经清掉了两条腿，事后要拿
    /// 开仓时的腿（价格、数量）就得回头翻它。
    ///
    /// 平仓前就要用（[`crate::executor`] 的 `before_exit`）：只解析含这个 id 的行 —— JSON 只转义引号、
    /// 反斜杠和控制字符，id 里没有这些，它的记录里就必然原样有这个 id；其余几千行不用解析（整本解析
    /// 东京要约 50ms）。id 里有要转义的字符就照旧逐行解析。
    pub async fn position_history(&self, id: &str) -> std::io::Result<Vec<PairPosition>> {
        let content = read_complete_lines(&self.path).await?;
        let literal = !id.chars().any(|c| c == '"' || c == '\\' || c.is_control());
        Ok(content
            .lines()
            .filter(|line| !literal || line.contains(id))
            .filter_map(|line| serde_json::from_str::<Record>(line).ok())
            .filter_map(|record| match record {
                Record::Position(position) if position.id == id => Some(*position),
                _ => None,
            })
            .collect())
    }

    /// 重放整个台账。坏行只跳过并计数，不让一行脏数据毁掉整份历史。
    ///
    /// 结果与 [`replay_file`] 整本重放完全相同，只是把解析过的前缀缓存下来，之后只解析新追加的行
    /// （见 [`ReplayCache`]）。文件变短、前缀末尾对不上、或离上次整本重放超过 [`FULL_REPLAY_EVERY`]
    /// 时整本重来（放到阻塞线程池里做，不占异步工作线程）。
    pub async fn replay(&self) -> std::io::Result<(Replayed, usize)> {
        let mut cache = self.replayed.lock().await;
        if let Some(cached) = cache.as_mut()
            && cached.full_at.elapsed() < FULL_REPLAY_EVERY
            && extend(&self.path, cached).await?
        {
            return Ok((cached.replayed.clone(), cached.broken));
        }
        let path = self.path.clone();
        let full = tokio::task::spawn_blocking(move || replay_prefix(&path))
            .await
            .map_err(std::io::Error::other)??;
        let result = (full.replayed.clone(), full.broken);
        *cache = Some(full);
        Ok(result)
    }
}

/// 把缓存接着读到文件当前末尾。返回 `false` = 接不上（文件变短、前缀末尾变了），要整本重放。
async fn extend(path: &Path, cached: &mut ReplayCache) -> std::io::Result<bool> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let len = file.metadata().await?.len();
    if len < cached.offset {
        return Ok(false);
    }
    let from = cached.offset - cached.tail.len() as u64;
    file.seek(std::io::SeekFrom::Start(from)).await?;
    let mut bytes = Vec::with_capacity((len - from) as usize);
    file.read_to_end(&mut bytes).await?;
    if !bytes.starts_with(&cached.tail) {
        return Ok(false);
    }
    let fresh = &bytes[cached.tail.len()..];
    let end = complete_end(fresh);
    if end == 0 {
        return Ok(true);
    }
    cached.broken += apply_lines(
        &mut cached.replayed,
        &String::from_utf8_lossy(&fresh[..end]),
    );
    cached.offset += end as u64;
    let consumed = &bytes[..cached.tail.len() + end];
    cached.tail = consumed[consumed.len().saturating_sub(REPLAY_TAIL as usize)..].to_vec();
    Ok(true)
}

/// 整本重放并记下解析到哪里（同步版，在阻塞线程池里跑）。
fn replay_prefix(path: &Path) -> std::io::Result<ReplayCache> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error),
    };
    let end = complete_end(&bytes);
    let mut replayed = Replayed::default();
    let broken = apply_lines(&mut replayed, &String::from_utf8_lossy(&bytes[..end]));
    Ok(ReplayCache {
        offset: end as u64,
        tail: bytes[end.saturating_sub(REPLAY_TAIL as usize)..end].to_vec(),
        replayed,
        broken,
        full_at: Instant::now(),
    })
}

/// 到最后一个换行为止（见 [`read_complete_lines`]）。
fn complete_end(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |at| at + 1)
}

/// 按顺序把每一行记进重放结果（后来的覆盖先前的），返回解析不了的行数。
fn apply_lines(replayed: &mut Replayed, content: &str) -> usize {
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
    broken
}

/// 只读地重放一份台账文件：不创建、不修补、不追加。
///
/// 看板用它展示持仓。[`Ledger::open`] 会创建文件并给半行补换行，那是写操作 ——
/// 只读的一方不该改动执行方正在追加的文件。文件不存在时返回空结果。
pub async fn replay_file(path: impl AsRef<Path>) -> std::io::Result<(Replayed, usize)> {
    let content = read_complete_lines(path.as_ref()).await?;
    let mut replayed = Replayed::default();
    let broken = apply_lines(&mut replayed, &content);
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
    let end = complete_end(&bytes);
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
            post_only: false,
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

    /// 写盘失败（这里用 /dev/full 模拟磁盘满）必须在**这一次** `append` 报出来：订单意图「发单前必须落盘」，
    /// 返回成功却没写进去，执行器就会照常发单。以前错误被 `sync_data` 吞掉、留到下一次写才报。
    #[tokio::test]
    async fn a_failed_write_is_reported_by_the_append_that_made_it() {
        let full = std::path::Path::new("/dev/full");
        if !full.exists() {
            return;
        }
        // 不走 `Ledger::open`：它会先把整个文件读一遍，而 /dev/full 读出来是无穷的零。
        let file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(full)
            .await
            .unwrap();
        let ledger = Ledger {
            path: full.to_path_buf(),
            file: Mutex::new(file),
            generation: AtomicU64::new(0),
            replayed: Mutex::new(None),
        };
        let error = ledger
            .append(&Record::Order(Box::new(order("full-1", dec!(10)))))
            .await
            .expect_err("磁盘满时这一次写就要报错");
        assert_eq!(error.raw_os_error(), Some(28), "ENOSPC: {error}");
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
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
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

    /// 增量重放的结果必须与整本重放一模一样：同一个 key 后来的覆盖先前的、坏行照样计数、
    /// 还没写完的半行不算（等它写完下次再读到）。
    #[tokio::test]
    async fn incremental_replay_matches_a_full_replay() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-inc-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        let summary = |replayed: &Replayed, broken: usize| {
            let mut orders: Vec<_> = replayed
                .orders
                .iter()
                .map(|(id, state)| (id.clone(), state.status, state.filled_usdt))
                .collect();
            orders.sort_by(|a, b| a.0.cmp(&b.0));
            let mut positions: Vec<_> = replayed
                .positions
                .iter()
                .map(|(id, position)| (id.clone(), position.status))
                .collect();
            positions.sort_by(|a, b| a.0.cmp(&b.0));
            format!("{orders:?} {positions:?} {broken}")
        };
        let same_as_full = |label: &'static str| {
            let ledger = &ledger;
            let path = path.clone();
            async move {
                let (cached, cached_broken) = ledger.replay().await.unwrap();
                let (full, full_broken) = replay_file(&path).await.unwrap();
                assert_eq!(
                    summary(&cached, cached_broken),
                    summary(&full, full_broken),
                    "{label}"
                );
                (cached, cached_broken)
            }
        };

        let (empty, _) = same_as_full("空台账").await;
        assert!(empty.orders.is_empty());
        ledger
            .append(&Record::Order(Box::new(order("o1", dec!(100)))))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "p1",
                PositionStatus::Opening,
            ))))
            .await
            .unwrap();
        same_as_full("头两行").await;

        let mut filled = order("o1", dec!(100));
        filled.status = crate::types::OrderStatus::Filled;
        filled.filled_usdt = dec!(100);
        ledger
            .append(&Record::Order(Box::new(filled)))
            .await
            .unwrap();
        ledger
            .append(&Record::Position(Box::new(position(
                "p1",
                PositionStatus::Open,
            ))))
            .await
            .unwrap();
        let (replayed, _) = same_as_full("同一个 key 的新状态").await;
        assert_eq!(
            replayed.orders["o1"].status,
            crate::types::OrderStatus::Filled
        );
        assert_eq!(replayed.positions["p1"].status, PositionStatus::Open);

        // 坏行与写到一半的行（另一个任务正在追加）。
        let mut raw = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        raw.write_all(b"not json\n").await.unwrap();
        let next = serde_json::to_string(&Record::Order(Box::new(order("o2", dec!(5))))).unwrap();
        let (head, rest) = next.split_at(next.len() / 2);
        raw.write_all(head.as_bytes()).await.unwrap();
        raw.sync_data().await.unwrap();
        let (replayed, broken) = same_as_full("坏行计数、半行不算").await;
        assert_eq!(broken, 1);
        assert!(!replayed.orders.contains_key("o2"));
        raw.write_all(rest.as_bytes()).await.unwrap();
        raw.write_all(b"\n").await.unwrap();
        raw.sync_data().await.unwrap();
        let (replayed, _) = same_as_full("半行写完之后").await;
        assert_eq!(replayed.orders["o2"].order.notional_usdt, dec!(5));

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 台账被别的工具改过（变短、或者已解析过的部分被改写）：缓存接不上，要整本重放，
    /// 不能拿旧结论加上新尾巴。
    #[tokio::test]
    async fn incremental_replay_notices_a_rewritten_ledger() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-rewrite-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        for id in ["o1", "o2", "o3"] {
            ledger
                .append(&Record::Order(Box::new(order(id, dec!(100)))))
                .await
                .unwrap();
        }
        assert_eq!(ledger.replay().await.unwrap().0.orders.len(), 3);

        // 变短：只留第一行。
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let first = content.lines().next().unwrap();
        tokio::fs::write(&path, format!("{first}\n")).await.unwrap();
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.orders.len(), 1, "台账变短了，要按新内容重放");

        // 同样长、但已解析过的部分被改写（金额 100 → 900），后面又追加了一行。
        let changed = first.replace("\"100\"", "\"900\"");
        assert_ne!(changed, first, "测试数据里要有能改的金额");
        let extra = serde_json::to_string(&Record::Order(Box::new(order("o4", dec!(1))))).unwrap();
        tokio::fs::write(&path, format!("{changed}\n{extra}\n"))
            .await
            .unwrap();
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.orders.len(), 2);
        assert_eq!(
            replayed.orders["o1"].order.notional_usdt,
            dec!(900),
            "前缀被改写过：不能沿用旧结论"
        );

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 仓位历史先按 id 子串过滤再解析：结果与逐行解析完全相同（前缀相同的别的仓位、订单行都不混进来）。
    #[tokio::test]
    async fn position_history_keeps_every_record_of_exactly_that_position() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-history-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Ledger::open(&path).await.unwrap();
        for (id, status) in [
            ("p1", PositionStatus::Opening),
            ("p10", PositionStatus::Opening),
            ("p1", PositionStatus::Open),
            ("p10", PositionStatus::Open),
            ("p1", PositionStatus::Closed),
        ] {
            ledger
                .append(&Record::Position(Box::new(position(id, status))))
                .await
                .unwrap();
            ledger
                .append(&Record::Order(Box::new(order(
                    &format!("{id}-buy-0"),
                    dec!(1),
                ))))
                .await
                .unwrap();
        }
        let statuses = |history: Vec<PairPosition>| {
            history
                .into_iter()
                .map(|position| (position.id, position.status))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            statuses(ledger.position_history("p1").await.unwrap()),
            [
                ("p1".to_string(), PositionStatus::Opening),
                ("p1".to_string(), PositionStatus::Open),
                ("p1".to_string(), PositionStatus::Closed),
            ]
        );
        assert_eq!(ledger.position_history("p10").await.unwrap().len(), 2);
        assert!(ledger.position_history("p2").await.unwrap().is_empty());

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
