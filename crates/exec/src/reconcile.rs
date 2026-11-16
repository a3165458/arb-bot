//! 对账：把本地台账与交易所的真实状态对齐。
//!
//! 断线、进程重启、或一次没有响应的请求，都会让本地状态与交易所状态分叉。
//! 对账**不能假设本地是对的**：它只回答一个问题 —— 「两边哪里不一样」，
//! 然后让调用方去决定怎么处理。
//!
//! 最危险的两种分叉：
//!
//! - **本地有仓、交易所没有**：说明平仓其实成功了但我们不知道，或者仓位记录是脏的。
//! - **交易所有挂单、本地没有**：说明有一笔订单发出去了、响应丢了 —— 它随时可能成交，
//!   变成一条我们没预期的腿。这是必须人工介入的状态。

use std::collections::HashMap;
use std::sync::Arc;

use arb_core::{ArbResult, Symbol, Venue};
use rust_decimal::Decimal;

use crate::broker::Broker;
use crate::types::{PairPosition, PositionStatus};

/// 一处不一致。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Divergence {
    pub kind: DivergenceKind,
    pub venue: Option<Venue>,
    /// 这处不一致属于哪个币。`None`：整个场所没核对（查询失败），或归不到哪个币（见 [`Self::blocks`]）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Symbol>,
    /// 仓位 id 或客户订单号。
    pub reference: String,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceKind {
    /// 本地记着有敞口，交易所那边看不到对应的持仓。
    PositionMissingOnVenue,
    /// 本地以为两腿都在，交易所那边只有一条腿。
    NakedLeg,
    /// 交易所有挂单，本地台账里没有。
    UnknownOpenOrder,
    /// 场所持仓方向或标的数量与台账不符（含台账之外的持仓）。
    PositionMismatch,
    /// 本地记着挂单，交易所那边没有。
    OrderMissingOnVenue,
    /// 券商查询失败 —— 这一项**没核对**，不等于一致。
    Unverified,
}

impl Divergence {
    /// 是否需要人工介入。`Unverified` 也算：没核对过就不能当成没问题。
    pub fn needs_attention(&self) -> bool {
        true
    }

    /// 这处不一致是否波及 `symbol` 在 `venues` 上的仓位（要冻结它：不评估规则、不开新仓、不为续上建仓发单）。
    ///
    /// 只冻结出问题的币，其余照常：知道属于哪个币的只管那个币；整个场所没核对（持仓或挂单查询失败）的，
    /// 冻结在这个场所上有腿的；归不到哪个币也归不到哪个场所的，全部冻结。
    pub fn blocks(&self, symbol: &Symbol, venues: &[Venue]) -> bool {
        self.blocks_where(&|own| own == symbol, venues)
    }

    /// 同 [`Self::blocks`]，但是不是同一个币由 `same` 判断（开仓请求只有 base，quote 可能没给）。
    pub fn blocks_where(&self, same: &dyn Fn(&Symbol) -> bool, venues: &[Venue]) -> bool {
        match (&self.symbol, self.kind, self.venue) {
            (Some(own), _, _) => same(own),
            (None, DivergenceKind::Unverified, Some(venue)) => venues.contains(&venue),
            _ => true,
        }
    }
}

/// 对账结果。
#[derive(Debug, Default, serde::Serialize)]
pub struct Reconciliation {
    pub divergences: Vec<Divergence>,
    /// 核对过的仓位数。
    pub checked_positions: usize,
    /// 核对过的场所数。
    pub checked_venues: usize,
    /// 各场所这次核对（持仓、挂单两次查询并发）用了多少毫秒。只给日志看哪家拖慢了对账。
    #[serde(skip)]
    pub elapsed_ms: Vec<(Venue, u64)>,
}

impl Reconciliation {
    /// 最慢的那家场所和它用的毫秒数（对账要等它）。
    pub fn slowest(&self) -> Option<(Venue, u64)> {
        self.elapsed_ms.iter().copied().max_by_key(|(_, ms)| *ms)
    }

    /// 是否完全一致（且全部核对过）。
    pub fn is_clean(&self) -> bool {
        self.divergences.is_empty()
    }

    /// 有没有哪处不一致波及 `symbol` 在 `venues` 上的仓位（见 [`Divergence::blocks`]）。
    pub fn blocks(&self, symbol: &Symbol, venues: &[Venue]) -> bool {
        self.divergences
            .iter()
            .any(|divergence| divergence.blocks(symbol, venues))
    }

    /// 开新仓 / 补仓用：`base`（及给了的 `quote`）在两腿场所 `venues` 上是否被冻结。
    pub fn blocks_open(&self, base: &str, quote: Option<&str>, venues: &[Venue]) -> bool {
        let (base, quote) = (base.trim(), quote.map(str::trim));
        let same = |own: &Symbol| {
            own.base.eq_ignore_ascii_case(base)
                && quote.is_none_or(|quote| own.quote.eq_ignore_ascii_case(quote))
        };
        self.divergences
            .iter()
            .any(|divergence| divergence.blocks_where(&same, venues))
    }

    /// 冻结了什么，给人看：出问题的币、整个没核对的场所；有归不到哪里的就是「全部」。
    pub fn frozen_scope(&self) -> Vec<String> {
        let mut scope: Vec<String> = Vec::new();
        for divergence in &self.divergences {
            let item = match (&divergence.symbol, divergence.kind, divergence.venue) {
                (Some(symbol), _, _) => symbol.to_string(),
                (None, DivergenceKind::Unverified, Some(venue)) => format!("{venue} 上的全部仓位"),
                _ => return vec!["全部".into()],
            };
            if !scope.contains(&item) {
                scope.push(item);
            }
        }
        scope
    }
}

/// 台账数量与场所净数量的相对容差：百万分之一。
const QUANTITY_TOLERANCE: Decimal = Decimal::from_parts(1, 0, 0, false, 6);

/// 两个数量对不对得上。台账里的数量是「名义 ÷ 均价」反推的，十进制除不尽时会留一条尾巴
/// （2026-09-30 LIT：`751.99000000000000000000000001` 对场所的 `751.99`），严格相等会把
/// 一笔完全正常的仓位判成不一致，进而让所有自动规则暂停。百万分之一的容差远小于任何真实的
/// 数量差（最小下单步长都比它大几个数量级）。
pub(crate) fn quantities_match(ledger: Decimal, venue: Decimal) -> bool {
    (ledger - venue).abs() <= ledger.abs().max(venue.abs()) * QUANTITY_TOLERANCE
}

/// 用交易所的真实状态核对本地仓位与挂单。
///
/// `positions` 应当来自台账里「还有敞口」的那些；`local_orders` 是本地记着还活着的订单号。
///
/// 各场所**并发**核对（每个场所内部仍是先持仓后挂单）：以前是一个接一个，5 个场所、每个两次请求，
/// 开仓前后各来一遍，光对账就占了点击到出结果里好几秒。结果按场所名排序，输出稳定。
pub async fn reconcile(
    positions: &[&PairPosition],
    local_orders: &HashMap<Venue, Vec<String>>,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
) -> ArbResult<Reconciliation> {
    let mut venues: Vec<(&Venue, &Arc<dyn Broker>)> = brokers.iter().collect();
    venues.sort_by_key(|(venue, _)| venue.as_str());
    let parts =
        futures_util::future::join_all(venues.into_iter().map(|(venue, broker)| {
            reconcile_venue(*venue, broker.as_ref(), positions, local_orders)
        }))
        .await;

    let mut result = Reconciliation::default();
    for part in parts {
        result.checked_venues += part.checked_venues;
        result.checked_positions += part.checked_positions;
        result.divergences.extend(part.divergences);
        result.elapsed_ms.extend(part.elapsed_ms);
    }
    Ok(result)
}

/// 核对一个场所：交易所的持仓对台账的敞口，交易所的挂单对本地记着的挂单。
async fn reconcile_venue(
    venue: Venue,
    broker: &dyn Broker,
    positions: &[&PairPosition],
    local_orders: &HashMap<Venue, Vec<String>>,
) -> Reconciliation {
    let started = std::time::Instant::now();
    let (positions_read, open_read) =
        futures_util::future::join(broker.positions(), broker.open_orders()).await;
    let mut result = reconcile_reads(&venue, positions_read, open_read, positions, local_orders);
    result.elapsed_ms = vec![(venue, started.elapsed().as_millis() as u64)];
    result
}

/// [`reconcile_venue`] 的核对部分：持仓与挂单两次查询的结果已经拿到（两次查询互不依赖，并发发出）。
/// 持仓查询失败就只报「未核对」，不看挂单。
fn reconcile_reads(
    venue: &Venue,
    positions_read: ArbResult<Vec<crate::broker::VenuePosition>>,
    open_read: ArbResult<Vec<crate::types::OrderState>>,
    positions: &[&PairPosition],
    local_orders: &HashMap<Venue, Vec<String>>,
) -> Reconciliation {
    let mut result = Reconciliation {
        checked_venues: 1,
        ..Reconciliation::default()
    };

    // 1) 交易所的持仓 vs 本地的敞口。
    let venue_positions = match positions_read {
        Ok(positions) => positions,
        Err(error) => {
            result.divergences.push(Divergence {
                kind: DivergenceKind::Unverified,
                venue: Some(*venue),
                symbol: None,
                reference: venue.to_string(),
                detail: format!("持仓查询失败，本场所**未核对**：{error}"),
            });
            return result;
        }
    };

    let mut expected: HashMap<_, Decimal> = HashMap::new();
    for position in positions.iter().filter(|p| {
        p.long.as_ref().map(|l| l.venue) == Some(*venue)
            || p.short.as_ref().map(|l| l.venue) == Some(*venue)
    }) {
        result.checked_positions += 1;
        if position.status == PositionStatus::Open && !position.is_hedged() {
            result.divergences.push(Divergence {
                kind: DivergenceKind::NakedLeg,
                venue: Some(*venue),
                symbol: Some(position.symbol.clone()),
                reference: position.id.clone(),
                detail: "仓位状态是 Open 但只有一条腿的成交记录".into(),
            });
        }
        for (leg, direction) in [
            (position.long.as_ref(), Decimal::ONE),
            (position.short.as_ref(), -Decimal::ONE),
        ] {
            if let Some(leg) = leg.filter(|leg| leg.venue == *venue) {
                match leg.quantity() {
                    Some(quantity) if quantity > Decimal::ZERO => {
                        *expected.entry(position.symbol.clone()).or_default() +=
                            direction * quantity;
                    }
                    _ => result.divergences.push(Divergence {
                        kind: DivergenceKind::Unverified,
                        venue: Some(*venue),
                        symbol: Some(position.symbol.clone()),
                        reference: position.id.clone(),
                        detail: "台账缺成交数量，无法核对真实持仓".into(),
                    }),
                }
            }
        }
    }
    let mut actual: HashMap<_, Decimal> = HashMap::new();
    for position in venue_positions {
        *actual.entry(position.symbol).or_default() += position.net_quantity;
    }
    for (symbol, quantity) in &expected {
        let on_venue = actual.remove(symbol).unwrap_or_default();
        if !quantities_match(*quantity, on_venue) {
            result.divergences.push(Divergence {
                kind: if on_venue.is_zero() {
                    DivergenceKind::PositionMissingOnVenue
                } else {
                    DivergenceKind::PositionMismatch
                },
                venue: Some(*venue),
                symbol: Some(symbol.clone()),
                reference: symbol.to_string(),
                detail: format!("{symbol} 台账数量 {quantity}，场所净数量 {on_venue}"),
            });
        }
    }
    for (symbol, quantity) in actual {
        if !quantity.is_zero() {
            result.divergences.push(Divergence {
                kind: DivergenceKind::PositionMismatch,
                venue: Some(*venue),
                symbol: Some(symbol.clone()),
                reference: symbol.to_string(),
                detail: format!("{symbol} 台账没有持仓，场所净数量 {quantity}"),
            });
        }
    }

    // 2) 交易所的挂单 vs 本地台账。交易所多出来的挂单最危险。
    let venue_open = match open_read {
        Ok(orders) => orders,
        Err(error) => {
            result.divergences.push(Divergence {
                kind: DivergenceKind::Unverified,
                venue: Some(*venue),
                symbol: None,
                reference: venue.to_string(),
                detail: format!("挂单查询失败，本场所**未核对**：{error}"),
            });
            return result;
        }
    };
    let known: Vec<&String> = local_orders
        .get(venue)
        .map(|ids| ids.iter().collect())
        .unwrap_or_default();

    for order in &venue_open {
        let id = &order.order.client_order_id.0;
        if !known.contains(&id) {
            result.divergences.push(Divergence {
                kind: DivergenceKind::UnknownOpenOrder,
                venue: Some(*venue),
                symbol: Some(order.order.symbol.clone()),
                reference: id.clone(),
                detail: format!(
                    "交易所有一笔本地台账不知道的挂单：{} {} {}，它随时可能成交",
                    order.order.symbol, order.order.side as u8, order.order.notional_usdt
                ),
            });
        }
    }

    for id in known {
        if !venue_open
            .iter()
            .any(|order| &order.order.client_order_id.0 == id)
        {
            result.divergences.push(Divergence {
                kind: DivergenceKind::OrderMissingOnVenue,
                venue: Some(*venue),
                // 本地挂单清单只有订单号：调用方按台账补上是哪个币（见 `desk::reconcile_ledger`）。
                symbol: None,
                reference: id.clone(),
                detail: "本地记着这笔挂单，交易所那边已经不在场上了（成交或已撤）".into(),
            });
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::VenuePosition;
    use crate::types::{ClientOrderId, NewOrder, OrderAck, OrderState};
    use arb_core::{ArbError, ArbResult, Symbol};
    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    struct AccountWithUntrackedExposure;

    #[async_trait]
    impl Broker for AccountWithUntrackedExposure {
        fn venue(&self) -> Venue {
            Venue::Lighter
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, _: &NewOrder) -> ArbResult<OrderAck> {
            Err(ArbError::config("测试不提交订单"))
        }
        async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(None)
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Err(ArbError::config("测试不撤单"))
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(Vec::new())
        }
        async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
            Ok(vec![VenuePosition {
                venue: Venue::Lighter,
                symbol: Symbol::perp("BTC", "USDT"),
                net_quantity: dec!(2),
                average_price: Some(dec!(100)),
                notional_usdt: dec!(200),
            }])
        }
    }

    #[tokio::test]
    async fn exposure_outside_the_ledger_blocks_a_clean_reconciliation() {
        let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([(
            Venue::Lighter,
            Arc::new(AccountWithUntrackedExposure) as Arc<dyn Broker>,
        )]);
        let result = reconcile(&[], &HashMap::new(), &brokers).await.unwrap();
        assert!(!result.is_clean());
        assert_eq!(result.divergences.len(), 1);
        assert_eq!(result.divergences[0].kind, DivergenceKind::PositionMismatch);
        assert!(result.divergences[0].detail.contains("净数量 2"));
    }

    /// 每次查询都要 150ms、什么都没有的桩场所。
    struct SlowFlat(Venue);

    #[async_trait]
    impl Broker for SlowFlat {
        fn venue(&self) -> Venue {
            self.0
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, _: &NewOrder) -> ArbResult<OrderAck> {
            Err(ArbError::config("测试不提交订单"))
        }
        async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(None)
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Err(ArbError::config("测试不撤单"))
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            Ok(Vec::new())
        }
        async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn the_venues_are_reconciled_at_the_same_time_and_counted_once_each() {
        let venues = [
            Venue::Lighter,
            Venue::LighterRh,
            Venue::Arcus,
            Venue::Hyperliquid,
            Venue::HyperliquidXyz,
        ];
        let brokers: HashMap<Venue, Arc<dyn Broker>> = venues
            .iter()
            .map(|venue| (*venue, Arc::new(SlowFlat(*venue)) as Arc<dyn Broker>))
            .collect();
        let started = std::time::Instant::now();
        let result = reconcile(&[], &HashMap::new(), &brokers).await.unwrap();
        // 一个场所内部先持仓后挂单是 300ms，五个串行要 1.5 秒；并发约 300ms。
        assert!(
            started.elapsed() < std::time::Duration::from_millis(1000),
            "各场所应并发对账，用了 {:?}",
            started.elapsed()
        );
        assert!(result.is_clean());
        assert_eq!(result.checked_venues, 5);
    }

    #[tokio::test]
    async fn one_venue_failing_is_reported_without_hiding_the_others() {
        struct Down;
        #[async_trait]
        impl Broker for Down {
            fn venue(&self) -> Venue {
                Venue::Arcus
            }
            fn fee_per_side(&self) -> Decimal {
                Decimal::ZERO
            }
            async fn place(&self, _: &NewOrder) -> ArbResult<OrderAck> {
                Err(ArbError::config("测试不提交订单"))
            }
            async fn order_state(&self, _: &ClientOrderId) -> ArbResult<Option<OrderState>> {
                Ok(None)
            }
            async fn cancel(&self, _: &str) -> ArbResult<()> {
                Err(ArbError::config("测试不撤单"))
            }
            async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
                Ok(Vec::new())
            }
            async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
                Err(ArbError::config("限频"))
            }
        }
        let brokers: HashMap<Venue, Arc<dyn Broker>> = HashMap::from([
            (Venue::Arcus, Arc::new(Down) as Arc<dyn Broker>),
            (
                Venue::Lighter,
                Arc::new(AccountWithUntrackedExposure) as Arc<dyn Broker>,
            ),
        ]);
        let result = reconcile(&[], &HashMap::new(), &brokers).await.unwrap();
        let kinds: Vec<_> = result.divergences.iter().map(|d| d.kind).collect();
        assert!(kinds.contains(&DivergenceKind::Unverified), "{kinds:?}");
        assert!(
            kinds.contains(&DivergenceKind::PositionMismatch),
            "{kinds:?}"
        );
        assert_eq!(result.checked_venues, 2);
    }

    #[test]
    fn a_decimal_division_tail_is_not_a_mismatch_but_a_real_difference_is() {
        // 2026-09-30 LIT 实盘：台账反推的数量带一条十进制尾巴。
        let ledger: Decimal = "751.99000000000000000000000001".parse().unwrap();
        assert!(quantities_match(ledger, dec!(751.99)));
        assert!(quantities_match(dec!(-1797.9), dec!(-1797.9)));
        assert!(quantities_match(Decimal::ZERO, Decimal::ZERO));
        // 少了最小步长级别的一点、或者一整份，都要报出来。
        assert!(!quantities_match(dec!(751.99), dec!(751.98)));
        assert!(!quantities_match(dec!(751.99), dec!(700)));
        assert!(!quantities_match(dec!(751.99), Decimal::ZERO));
        assert!(
            !quantities_match(dec!(751.99), dec!(-751.99)),
            "方向反了不是同一个数量"
        );
    }
}
