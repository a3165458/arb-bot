//! 真实券商共用的部件：订单意图日志、交易所订单号映射、数量与价格取整、签名原语。
//!
//! 这些规则是**全部**真实券商的共同不变量，放在一处是为了不让七家各写一份、再各错一点：
//!
//! - 开仓数量只向下取整；reduce-only 的数量必须恰好落在交易所步长上，否则拒单 ——
//!   平不干净的残量就是没人管的敞口。
//! - 限价按 tick 取整时**不让价格变差**：买向下、卖向上。
//! - 订单意图先落盘（fsync）再发单；记录过的订单号永不重发。
//! - 传输层错误去掉 URL 再上抛：签名、时间戳可能在查询串里。

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Decimal, Side, Venue};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::types::{ClientOrderId, NewOrder, OrderState};

/// 「刚核对过」的记号：某个键在 `ttl` 内被标记过就算新鲜，过期自动失效。
///
/// 开仓前的预热核对一次杠杆，紧接着的 `place` 在这个时间内就不必再往返一次；超时没用上，
/// 下一次照旧重新核对。只是省一次读，不放宽任何检查。
pub struct Verified<K> {
    ttl: Duration,
    seen: StdMutex<HashMap<K, Instant>>,
}

impl<K: Hash + Eq> Verified<K> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            seen: StdMutex::new(HashMap::new()),
        }
    }

    pub fn is_fresh(&self, key: &K) -> bool {
        self.seen
            .lock()
            .ok()
            .and_then(|seen| seen.get(key).copied())
            .is_some_and(|at| at.elapsed() < self.ttl)
    }

    pub fn mark(&self, key: K) {
        if let Ok(mut seen) = self.seen.lock() {
            seen.insert(key, Instant::now());
        }
    }
}

/// 带时刻的单值缓存：`get` 只返回不比 `max_age` 更老的值。
pub struct Cached<T> {
    slot: StdMutex<Option<(Instant, T)>>,
}

impl<T: Clone> Cached<T> {
    pub fn new() -> Self {
        Self {
            slot: StdMutex::new(None),
        }
    }

    pub fn get(&self, max_age: Duration) -> Option<T> {
        let slot = self.slot.lock().ok()?;
        let (at, value) = slot.as_ref()?;
        (at.elapsed() < max_age).then(|| value.clone())
    }

    pub fn put(&self, value: T) {
        if let Ok(mut slot) = self.slot.lock() {
            *slot = Some((Instant::now(), value));
        }
    }
}

impl<T: Clone> Default for Cached<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// 真实券商的运行开关。默认：禁止一切写操作、没有价格保护。
#[derive(Debug, Clone, Default)]
pub struct LiveOptions {
    /// 为假时任何签名写操作（下单、撤单、改杠杆 / 保证金模式）都必须在签名前拒绝。
    pub trading_enabled: bool,
    /// 无限价单（平仓 / 回滚 / 减仓）的价格保护，小数（0.003 = 0.3%）。
    pub market_slippage: Option<Decimal>,
}

impl LiveOptions {
    /// 构造券商时调用：开启下单必须同时给出价格保护，否则会开出平不掉的仓位。
    pub fn validate(&self, venue: Venue) -> ArbResult<()> {
        if let Some(slippage) = self.market_slippage
            && (slippage <= Decimal::ZERO || slippage >= Decimal::ONE)
        {
            return Err(ArbError::venue(
                venue.as_str(),
                "market_slippage 必须在 (0, 1) 之间",
            ));
        }
        if self.trading_enabled && self.market_slippage.is_none() {
            return Err(ArbError::venue(
                venue.as_str(),
                "开启下单必须给出 market_slippage，否则平仓与回滚没有价格上限",
            ));
        }
        Ok(())
    }

    /// 每个签名写操作之前调用。
    pub fn authorize(&self, venue: Venue) -> ArbResult<()> {
        if self.trading_enabled {
            Ok(())
        } else {
            Err(ArbError::venue(
                venue.as_str(),
                "下单未开启：没有签名、没有发送任何写请求",
            ))
        }
    }

    /// 无限价单的价格上限：买 ≤ 卖一 × (1 + s)，卖 ≥ 买一 × (1 − s)。
    /// 盘口必须刚拉取（调用方负责），一侧为空或交叉时拒绝。
    pub fn bound_price(
        &self,
        venue: Venue,
        side: Side,
        best_bid: Option<Decimal>,
        best_ask: Option<Decimal>,
    ) -> ArbResult<Decimal> {
        let slippage = self
            .market_slippage
            .ok_or_else(|| ArbError::venue(venue.as_str(), "无限价单需要显式的 market_slippage"))?;
        if let (Some(bid), Some(ask)) = (best_bid, best_ask)
            && (bid <= Decimal::ZERO || ask < bid)
        {
            return Err(ArbError::venue(
                venue.as_str(),
                "盘口交叉或价格非正，拒绝定价",
            ));
        }
        match side {
            Side::Buy => best_ask
                .filter(|ask| *ask > Decimal::ZERO)
                .map(|ask| ask * (Decimal::ONE + slippage)),
            Side::Sell => best_bid
                .filter(|bid| *bid > Decimal::ZERO)
                .map(|bid| bid * (Decimal::ONE - slippage)),
        }
        .ok_or_else(|| ArbError::venue(venue.as_str(), "盘口该侧为空，拒绝定价"))
    }
}

/// 向下取到 `step` 的整数倍。`step` 必须为正。
pub fn floor_to_step(value: Decimal, step: Decimal) -> Option<Decimal> {
    (step > Decimal::ZERO).then(|| (value / step).floor() * step)
}

/// 限价按 tick 取整，**不让价格变差**：买单向下（少付）、卖单向上（多收）。
pub fn round_price(price: Decimal, tick: Decimal, side: Side) -> Option<Decimal> {
    if tick <= Decimal::ZERO || price <= Decimal::ZERO {
        return None;
    }
    let ticks = price / tick;
    let rounded = match side {
        Side::Buy => ticks.floor(),
        Side::Sell => ticks.ceil(),
    } * tick;
    (rounded > Decimal::ZERO).then(|| rounded.normalize())
}

/// 这笔订单要下多少**交易所数量单位**（张数或标的数量，由 `unit` 决定）。
///
/// - `unit`：一个交易所数量单位等于多少标的（线性 USDT 合约用基础币数量时为 1，
///   OKX / Gate 这类按张计的为合约面值）。
/// - `step` / `min`：交易所数量单位下的步长与最小数量。
///
/// 规则：`quantity` 为 `None` 的开仓按 `notional / price` 换算后向下取整；给了 `quantity`
/// 的开仓（第二腿按第一腿成交量）也只向下取整；reduce-only 必须给精确数量且恰好是步长的
/// 整数倍，否则拒绝 —— 平不干净的残量就是没人管的敞口。
pub fn order_units(
    venue: Venue,
    order: &NewOrder,
    price: Decimal,
    unit: Decimal,
    step: Decimal,
    min: Decimal,
) -> ArbResult<Decimal> {
    let fail = |message: &str| ArbError::venue(venue.as_str(), message.to_string());
    if price <= Decimal::ZERO || unit <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(fail("价格、合约面值或数量步长非正"));
    }
    let base = match (order.quantity, order.reduce_only) {
        (Some(quantity), _) => quantity,
        (None, false) => order.notional_usdt / price,
        (None, true) => return Err(fail("reduce-only 订单必须给出精确的标的数量")),
    };
    if base <= Decimal::ZERO {
        return Err(fail("数量必须为正"));
    }
    let raw = base / unit;
    let units = floor_to_step(raw, step).ok_or_else(|| fail("数量步长非正"))?;
    if order.reduce_only && units != raw {
        return Err(fail(
            "reduce-only 数量不是交易所步长的整数倍，平仓会留下残量",
        ));
    }
    if units <= Decimal::ZERO || units < min {
        return Err(fail("数量取整后低于交易所最小下单量"));
    }
    Ok(units.normalize())
}

/// 把内部订单号映射成交易所可接受的客户订单号：`prefix` + SHA-256 十六进制前缀，
/// 总长不超过 `max_len`。确定性：同一个内部 id 永远得到同一个交易所 id，重启后可查。
pub fn venue_client_id(prefix: &str, id: &ClientOrderId, max_len: usize) -> String {
    let digest = Sha256::digest(id.0.as_bytes());
    let mut out = String::with_capacity(max_len);
    out.push_str(prefix);
    for byte in digest {
        for nibble in [byte >> 4, byte & 15] {
            if out.len() >= max_len {
                return out;
            }
            out.push(char::from_digit(u32::from(nibble), 16).unwrap_or('0'));
        }
    }
    out
}

pub fn hmac_sha256(secret: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC 接受任意长度密钥");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

pub fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 15), 16).unwrap_or('0'));
    }
    out
}

pub fn base64_standard(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// 传输层错误：去掉 URL（查询串里可能有签名与时间戳），只保留场所与类别。
pub fn transport_error(venue: Venue, endpoint: &str, error: reqwest::Error) -> ArbError {
    let kind = if error.is_timeout() {
        "超时（结果未知）"
    } else if error.is_connect() {
        "连接失败"
    } else {
        "传输失败"
    };
    ArbError::venue(venue.as_str(), format!("{endpoint} {kind}"))
}

/// 一条订单意图。先于任何写请求落盘。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub order: NewOrder,
    /// 交易所侧的客户订单号（[`venue_client_id`] 的结果）。
    pub venue_client_id: String,
    /// 交易所原生合约 id（如 `BTCUSDT`、`BTC-USDT-SWAP`）。
    pub instrument: String,
    /// 实际下单数量（交易所数量单位）。
    pub units: Decimal,
    /// 已确认的终态。有它就不再查询交易所（查询窗口可能已过期）。
    #[serde(default)]
    pub terminal: Option<OrderState>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum JournalRecord {
    Identity {
        identity: String,
    },
    Intent {
        entry: JournalEntry,
    },
    Terminal {
        client_order_id: String,
        state: OrderState,
    },
}

/// 单账户的订单意图日志：追加式 JSONL、独占锁、每行 fsync。
///
/// 超时、崩溃之后它是「这个订单号发出去过」的唯一证据：券商据此拒绝重发，
/// 并在交易所查不到时报错而不是当成从未提交。不要删它来「解决」卡住的订单。
pub struct OrderJournal {
    file: File,
    entries: HashMap<String, JournalEntry>,
    by_venue_id: HashMap<String, String>,
}

impl OrderJournal {
    /// `identity` 标识账户（如 `binance:<API key 的 SHA-256 前缀>`），与日志首行不符时拒绝打开 ——
    /// 拿 A 账户的日志去核对 B 账户的订单，会把真实成交当成「从未提交」。
    pub fn open(path: &Path, identity: &str) -> ArbResult<Self> {
        let mut options = OpenOptions::new();
        options.read(true).append(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        fs2::FileExt::try_lock_exclusive(&file).map_err(|_| {
            ArbError::config(format!(
                "订单日志 {} 已被另一个进程占用（arb-live 或另一个看板正连着同一个账户）",
                path.display()
            ))
        })?;

        let mut journal = Self {
            file,
            entries: HashMap::new(),
            by_venue_id: HashMap::new(),
        };
        let mut seen_identity = false;
        for line in BufReader::new(journal.file.try_clone()?).split(b'\n') {
            let line = line?;
            let record: JournalRecord = serde_json::from_slice(&line).map_err(|_| {
                ArbError::config(format!(
                    "订单日志 {} 有损坏的行，需人工核对",
                    path.display()
                ))
            })?;
            match record {
                JournalRecord::Identity { identity: stored } if !seen_identity => {
                    if stored != identity {
                        return Err(ArbError::config(format!(
                            "订单日志 {} 属于另一个账户，拒绝使用",
                            path.display()
                        )));
                    }
                    seen_identity = true;
                }
                JournalRecord::Intent { entry } if seen_identity => journal.insert(entry)?,
                JournalRecord::Terminal {
                    client_order_id,
                    state,
                } if seen_identity => {
                    let entry = journal
                        .entries
                        .get_mut(&client_order_id)
                        .ok_or_else(|| ArbError::config("订单日志里有终态但没有对应的意图"))?;
                    if state.status.is_live()
                        || state.order.client_order_id != entry.order.client_order_id
                    {
                        return Err(ArbError::config("订单日志里的终态记录不合法"));
                    }
                    entry.terminal = Some(state);
                }
                _ => return Err(ArbError::config("订单日志记录顺序不合法")),
            }
        }
        if !seen_identity {
            journal.append(&JournalRecord::Identity {
                identity: identity.to_string(),
            })?;
            // 新文件：连同目录项一起落盘，之后的意图才有意义。
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            File::open(parent)?.sync_all()?;
        }
        Ok(journal)
    }

    fn insert(&mut self, entry: JournalEntry) -> ArbResult<()> {
        let id = entry.order.client_order_id.0.clone();
        if self.entries.contains_key(&id) || self.by_venue_id.contains_key(&entry.venue_client_id) {
            return Err(ArbError::config("订单日志里有重复的订单号"));
        }
        self.by_venue_id
            .insert(entry.venue_client_id.clone(), id.clone());
        self.entries.insert(id, entry);
        Ok(())
    }

    pub fn get(&self, id: &ClientOrderId) -> Option<&JournalEntry> {
        self.entries.get(&id.0)
    }

    pub fn by_venue_client_id(&self, venue_client_id: &str) -> Option<&JournalEntry> {
        self.by_venue_id
            .get(venue_client_id)
            .and_then(|id| self.entries.get(id))
    }

    /// 记录一条意图并 fsync。之后才允许发出任何与它相关的写请求。
    pub fn reserve(&mut self, entry: JournalEntry) -> ArbResult<()> {
        if entry.terminal.is_some() {
            return Err(ArbError::config("新意图不能带终态"));
        }
        let record = JournalRecord::Intent {
            entry: entry.clone(),
        };
        self.insert(entry)?;
        self.append(&record)
    }

    /// 记录已确认的终态（非活跃状态），重复记录同一终态是无害的。
    pub fn record_terminal(&mut self, state: &OrderState) -> ArbResult<()> {
        if state.status.is_live() {
            return Ok(());
        }
        let id = state.order.client_order_id.0.clone();
        let entry = self
            .entries
            .get_mut(&id)
            .ok_or_else(|| ArbError::config("终态没有对应的意图"))?;
        entry.terminal = Some(state.clone());
        self.append(&JournalRecord::Terminal {
            client_order_id: id,
            state: state.clone(),
        })
    }

    fn append(&mut self, record: &JournalRecord) -> ArbResult<()> {
        let mut line =
            serde_json::to_vec(record).map_err(|_| ArbError::config("订单日志记录无法序列化"))?;
        line.push(b'\n');
        self.file.write_all(&line)?;
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_verified_mark_is_fresh_only_for_its_key_and_only_until_it_expires() {
        let verified = Verified::new(Duration::from_millis(60));
        assert!(!verified.is_fresh(&(5, 3)));
        verified.mark((5, 3));
        assert!(verified.is_fresh(&(5, 3)));
        assert!(!verified.is_fresh(&(5, 4)), "别的杠杆不能沾光");
        assert!(!verified.is_fresh(&(6, 3)), "别的市场不能沾光");
        std::thread::sleep(Duration::from_millis(90));
        assert!(!verified.is_fresh(&(5, 3)), "过期后要重新核对");
    }

    #[test]
    fn a_cached_value_is_returned_only_while_young_enough() {
        let cache: Cached<Vec<u8>> = Cached::new();
        assert_eq!(cache.get(Duration::from_secs(10)), None);
        cache.put(vec![1, 2, 3]);
        assert_eq!(cache.get(Duration::from_secs(10)), Some(vec![1, 2, 3]));
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            cache.get(Duration::from_millis(10)),
            None,
            "比要求的更老就不能用"
        );
        assert_eq!(cache.get(Duration::from_secs(10)), Some(vec![1, 2, 3]));
    }

    use super::*;
    use crate::types::OrderStatus;
    use arb_core::Symbol;
    use rust_decimal_macros::dec;

    fn order(quantity: Option<Decimal>, reduce_only: bool) -> NewOrder {
        NewOrder {
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("live-1-buy-0".into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(1000),
            quantity,
            limit_price: Some(dec!(100)),
            reduce_only,
            leverage: Some(dec!(3)),
        }
    }

    #[test]
    fn opens_round_down_and_exits_must_be_exact() {
        let units = |q, r| {
            order_units(
                Venue::Binance,
                &order(q, r),
                dec!(300),
                Decimal::ONE,
                dec!(0.01),
                dec!(0.01),
            )
        };
        // 1000 / 300 = 3.333… → 3.33
        assert_eq!(units(None, false).unwrap(), dec!(3.33));
        assert_eq!(units(Some(dec!(1.239)), false).unwrap(), dec!(1.23));
        assert_eq!(units(Some(dec!(1.23)), true).unwrap(), dec!(1.23));
        assert!(
            units(Some(dec!(1.239)), true).is_err(),
            "平仓残量不能被悄悄丢掉"
        );
        assert!(units(None, true).is_err());
        assert!(units(Some(dec!(0.009)), false).is_err());
    }

    #[test]
    fn contract_units_divide_by_face_value() {
        // OKX 一张 0.01 BTC：0.05 BTC = 5 张
        let got = order_units(
            Venue::Okx,
            &order(Some(dec!(0.05)), true),
            dec!(60000),
            dec!(0.01),
            Decimal::ONE,
            Decimal::ONE,
        );
        assert_eq!(got.unwrap(), dec!(5));
        let partial = order_units(
            Venue::Okx,
            &order(Some(dec!(0.055)), true),
            dec!(60000),
            dec!(0.01),
            Decimal::ONE,
            Decimal::ONE,
        );
        assert!(partial.is_err(), "半张平不掉");
    }

    #[test]
    fn price_rounding_never_worsens_the_limit() {
        assert_eq!(
            round_price(dec!(100.37), dec!(0.1), Side::Buy),
            Some(dec!(100.3))
        );
        assert_eq!(
            round_price(dec!(100.31), dec!(0.1), Side::Sell),
            Some(dec!(100.4))
        );
        assert_eq!(round_price(dec!(0.04), dec!(0.1), Side::Buy), None);
    }

    #[test]
    fn market_bound_uses_the_executable_side() {
        let options = LiveOptions {
            trading_enabled: true,
            market_slippage: Some(dec!(0.01)),
        };
        let bound = |side, bid, ask| options.bound_price(Venue::Binance, side, bid, ask);
        assert_eq!(
            bound(Side::Buy, Some(dec!(99)), Some(dec!(100))).unwrap(),
            dec!(101)
        );
        assert_eq!(
            bound(Side::Sell, Some(dec!(99)), Some(dec!(100))).unwrap(),
            dec!(98.01)
        );
        assert!(bound(Side::Sell, None, Some(dec!(100))).is_err());
        assert!(
            bound(Side::Buy, Some(dec!(101)), Some(dec!(100))).is_err(),
            "交叉盘口"
        );
        assert!(
            LiveOptions {
                trading_enabled: true,
                market_slippage: None
            }
            .validate(Venue::Binance)
            .is_err()
        );
    }

    #[test]
    fn venue_client_ids_are_deterministic_and_bounded() {
        let a = venue_client_id("t-", &ClientOrderId("live-1-buy-0".into()), 28);
        assert_eq!(a.len(), 28);
        assert!(a.starts_with("t-"));
        assert_eq!(
            a,
            venue_client_id("t-", &ClientOrderId("live-1-buy-0".into()), 28)
        );
        assert_ne!(
            a,
            venue_client_id("t-", &ClientOrderId("live-1-buy-1".into()), 28)
        );
    }

    #[test]
    fn hmac_matches_the_rfc_4231_vector() {
        // RFC 4231 test case 2
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex_lower(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn journal_survives_restart_and_refuses_other_accounts() {
        let dir = std::env::temp_dir().join(format!("arb-journal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("orders.jsonl");
        let entry = JournalEntry {
            order: order(Some(dec!(1)), false),
            venue_client_id: "abc".into(),
            instrument: "BTCUSDT".into(),
            units: dec!(1),
            terminal: None,
        };
        {
            let mut journal = OrderJournal::open(&path, "binance:k1").unwrap();
            journal.reserve(entry.clone()).unwrap();
            assert!(
                journal.reserve(entry.clone()).is_err(),
                "同一订单号不能记两次"
            );
            assert!(OrderJournal::open(&path, "binance:k1").is_err(), "独占锁");
            let mut state = OrderState::new(entry.order.clone());
            state.status = OrderStatus::Filled;
            journal.record_terminal(&state).unwrap();
        }
        let journal = OrderJournal::open(&path, "binance:k1").unwrap();
        let restored = journal.by_venue_client_id("abc").unwrap();
        assert_eq!(
            restored.terminal.as_ref().unwrap().status,
            OrderStatus::Filled
        );
        drop(journal);
        assert!(
            OrderJournal::open(&path, "binance:k2").is_err(),
            "另一个账户"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
