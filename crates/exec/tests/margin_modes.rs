//! Offline margin-mode regression tests. No credentials, network or signed writes.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arb_core::{ArbError, ArbResult, MarketSnapshot, Side, Symbol, Venue};
use arb_exec::monitor::{Action, Inputs, evaluate_full};
use arb_exec::{
    Broker, ClientOrderId, Executor, Ledger, LegPlan, MarginMode, NewOrder, OrderAck, OrderState,
    OrderStatus, PairPosition, Plan, PositionSetup, PositionStatus, Strategy, TaskRules,
    VenueLegState, VenuePosition,
};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::json;

fn legacy_position() -> PairPosition {
    let leg = |venue: &str, side: &str| {
        json!({
            "venue":venue, "side":side, "notional_usdt":"1000", "average_price":"100",
            "fee_usdt":"0", "client_order_id":format!("old-{side}"), "margin_usdt":"200"
        })
    };
    serde_json::from_value(json!({
        "id":"old", "symbol":Symbol::perp("BTC", "USDT"), "strategy":"funding",
        "long":leg("binance", "buy"), "short":leg("okx", "sell"),
        "entry_basis_pct":"0", "expected_round_trip_cost":"0", "status":"open",
        "opened_at":Utc::now(), "leverage":"5"
    }))
    .unwrap()
}

fn snapshot(venue: Venue) -> MarketSnapshot {
    MarketSnapshot {
        venue,
        symbol: Symbol::perp("BTC", "USDT"),
        period_rate: dec!(0.0001),
        interval_h: 1,
        interval_assumed: false,
        next_funding_at: Utc::now(),
        next_funding_estimated: false,
        taker_fee: None,
        mark_price: Some(dec!(100)),
        index_price: None,
        best_bid: Some(dec!(100)),
        best_ask: Some(dec!(100)),
        bid_size_usdt: None,
        ask_size_usdt: None,
        open_interest_usdt: None,
        quote_volume_24h: None,
        max_leverage: Some(dec!(50)),
        maintenance_margin: None,
        oi_capped: false,
    }
}

#[test]
fn old_orders_and_positions_default_to_isolated_and_roundtrip_cross_exactly() {
    let mut position = legacy_position();
    assert_eq!(position.margin_mode, MarginMode::Isolated);
    position.margin_mode = MarginMode::Cross;
    let reloaded: PairPosition =
        serde_json::from_value(serde_json::to_value(&position).unwrap()).unwrap();
    assert_eq!(reloaded.margin_mode, MarginMode::Cross);
    let old = json!({
        "client_order_id":"old-order", "venue":"binance", "symbol":position.symbol,
        "side":"buy", "notional_usdt":"1000", "limit_price":"100"
    });
    let order: NewOrder = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(order.margin_mode, MarginMode::Isolated);
    let mut invalid = old;
    invalid["margin_mode"] = json!("portfolio");
    assert!(serde_json::from_value::<NewOrder>(invalid).is_err());
}

#[test]
fn cross_never_invents_liquidation_from_a_legacy_isolated_margin() {
    let mut position = legacy_position();
    position.margin_mode = MarginMode::Cross;
    position.rules.liq_protection_pct = Some(dec!(10));
    let mut long = snapshot(Venue::Binance);
    let mut short = snapshot(Venue::Okx);
    long.maintenance_margin = Some(dec!(0.01));
    short.maintenance_margin = Some(dec!(0.01));
    let result = evaluate_full(&position, &long, &short, Inputs::default()).unwrap();
    assert_eq!(result.action, Action::Hold);
    assert_eq!(result.observation.long.margin_usdt, None);
    assert_eq!(result.observation.long.liquidation_price, None);
    assert_eq!(result.observation.short.distance_pct, None);
    assert_eq!(result.skipped.len(), 2);
}

#[test]
fn cross_protection_closes_the_pair_using_only_venue_liquidation_not_a_trim_formula() {
    let mut position = legacy_position();
    position.margin_mode = MarginMode::Cross;
    position.rules.liq_protection_pct = Some(dec!(10));
    let state = VenueLegState {
        margin_mode: Some(MarginMode::Cross),
        liquidation_price: Some(dec!(95)),
        margin_usdt: Some(dec!(99999)), // Must not be used as independent collateral.
    };
    let result = evaluate_full(
        &position,
        &snapshot(Venue::Binance),
        &snapshot(Venue::Okx),
        Inputs {
            long_state: Some(&state),
            ..Inputs::default()
        },
    )
    .unwrap();
    assert_eq!(result.observation.long.distance_pct, Some(dec!(5)));
    assert_eq!(result.observation.long.margin_usdt, None);
    assert!(matches!(result.action, Action::Close { .. }));
    assert!(result.fallback.is_none());
}

#[test]
fn external_margin_mode_changes_pause_automatic_rules_instead_of_switching_or_trading() {
    let mut position = legacy_position();
    position.rules.size_mismatch_pct = Some(dec!(1));
    position.short.as_mut().unwrap().notional_usdt = dec!(900);
    let cross = VenueLegState {
        margin_mode: Some(MarginMode::Cross),
        liquidation_price: Some(dec!(99)),
        margin_usdt: None,
    };
    let result = evaluate_full(
        &position,
        &snapshot(Venue::Binance),
        &snapshot(Venue::Okx),
        Inputs {
            long_state: Some(&cross),
            ..Inputs::default()
        },
    )
    .unwrap();
    assert_eq!(result.action, Action::Hold);
    assert!(result.skipped[0].contains("模式与台账不符"));
}

#[derive(Default)]
struct Events {
    report_wrong_mode: bool,
    prepared: Vec<(Venue, MarginMode)>,
    orders: Vec<NewOrder>,
}
struct TestBroker {
    venue: Venue,
    events: Arc<Mutex<Events>>,
    states: Mutex<HashMap<String, OrderState>>,
    fail_prepare: bool,
    occupied: bool,
}
#[async_trait]
impl Broker for TestBroker {
    fn venue(&self) -> Venue {
        self.venue
    }
    fn fee_per_side(&self) -> Decimal {
        Decimal::ZERO
    }
    async fn prepare_open_mode(
        &self,
        _: &Symbol,
        _: Side,
        _: Option<Decimal>,
        mode: MarginMode,
    ) -> ArbResult<()> {
        self.events
            .lock()
            .unwrap()
            .prepared
            .push((self.venue, mode));
        if self.fail_prepare {
            Err(ArbError::config("mode readback failed"))
        } else {
            Ok(())
        }
    }
    async fn place(&self, order: &NewOrder) -> ArbResult<OrderAck> {
        let mut events = self.events.lock().unwrap();
        assert_eq!(
            events.prepared.len(),
            2,
            "Both settings must be verified before either order"
        );
        events.orders.push(order.clone());
        let mut state = OrderState::new(order.clone());
        if events.report_wrong_mode {
            state.order.margin_mode = if order.margin_mode.is_cross() {
                MarginMode::Isolated
            } else {
                MarginMode::Cross
            };
        }
        state.status = OrderStatus::Filled;
        state.venue_order_id = Some(order.client_order_id.0.clone());
        state.average_price = Some(dec!(100));
        state.filled_usdt = order
            .quantity
            .map_or(order.notional_usdt, |q| q * dec!(100));
        self.states
            .lock()
            .unwrap()
            .insert(order.client_order_id.0.clone(), state);
        Ok(OrderAck {
            client_order_id: order.client_order_id.clone(),
            status: OrderStatus::Filled,
            venue_order_id: order.client_order_id.0.clone(),
        })
    }
    async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
        Ok(self.states.lock().unwrap().get(&id.0).cloned())
    }
    async fn cancel(&self, _: &str) -> ArbResult<()> {
        Ok(())
    }
    async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
        Ok(vec![])
    }
    async fn positions(&self) -> ArbResult<Vec<VenuePosition>> {
        Ok(if self.occupied {
            vec![VenuePosition {
                venue: self.venue,
                symbol: Symbol::perp("BTC", "USDT"),
                net_quantity: dec!(1),
                average_price: Some(dec!(100)),
                notional_usdt: dec!(100),
            }]
        } else {
            vec![]
        })
    }
}
fn execution_plan() -> Plan {
    let leg = |venue, side| LegPlan {
        symbol: Symbol::perp("BTC", "USDT"),
        venue,
        side,
        notional_usdt: dec!(1000),
        limit_price: dec!(100),
        best_price: dec!(100),
        expected_price: dec!(100),
        slippage: Decimal::ZERO,
        book_notional: dec!(10000),
    };
    Plan {
        symbol: Symbol::perp("BTC", "USDT"),
        long: leg(Venue::Binance, Side::Buy),
        short: leg(Venue::Okx, Side::Sell),
        expected_cost: Decimal::ZERO,
    }
}
async fn fixture(
    fail_prepare: bool,
    occupied: bool,
) -> (
    Executor,
    Arc<Ledger>,
    Arc<Mutex<Events>>,
    std::path::PathBuf,
) {
    let path = std::env::temp_dir().join(format!(
        "arb-mode-test-{}-{}.jsonl",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let ledger = Arc::new(Ledger::open(&path).await.unwrap());
    let events = Arc::new(Mutex::new(Events::default()));
    let brokers = [Venue::Binance, Venue::Okx]
        .into_iter()
        .map(|venue| {
            Arc::new(TestBroker {
                venue,
                events: events.clone(),
                states: Mutex::new(HashMap::new()),
                fail_prepare: fail_prepare && venue == Venue::Okx,
                occupied: occupied && venue == Venue::Okx,
            }) as Arc<dyn Broker>
        })
        .collect();
    (Executor::new(ledger.clone(), brokers), ledger, events, path)
}

#[tokio::test]
async fn both_modes_survive_open_close_order_journaling_and_ledger_replay() {
    for mode in [MarginMode::Isolated, MarginMode::Cross] {
        let (executor, ledger, events, path) = fixture(false, false).await;
        let setup = PositionSetup {
            margin_mode: mode,
            leverage: Some(dec!(5)),
            ..PositionSetup::default()
        };
        let outcome = executor
            .open(
                &execution_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "mode-pair",
                &setup,
            )
            .await
            .unwrap();
        assert!(outcome.is_open());
        let mut position = outcome.position;
        assert_eq!(position.margin_mode, mode);
        assert_eq!(
            position.long.as_ref().unwrap().margin_usdt,
            if mode.is_cross() {
                None
            } else {
                Some(dec!(200))
            }
        );
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        assert_eq!(replayed.positions["mode-pair"].margin_mode, mode);
        {
            let events = events.lock().unwrap();
            assert_eq!(events.orders.len(), 4);
            assert!(events.orders.iter().all(|order| order.margin_mode == mode));
            assert!(events.orders[2..].iter().all(|order| order.reduce_only));
        }
        drop(executor);
        drop(ledger);
        tokio::fs::remove_file(path).await.unwrap();
    }
}

#[tokio::test]
async fn failed_mode_readback_or_existing_exposure_prevents_both_orders() {
    for (fail, occupied) in [(true, false), (false, true)] {
        let (executor, ledger, events, path) = fixture(fail, occupied).await;
        let setup = PositionSetup {
            margin_mode: MarginMode::Cross,
            leverage: Some(dec!(5)),
            ..PositionSetup::default()
        };
        let outcome = executor
            .open(
                &execution_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "blocked",
                &setup,
            )
            .await
            .unwrap();
        assert_eq!(outcome.position.status, PositionStatus::Unwound);
        assert!(events.lock().unwrap().orders.is_empty());
        if occupied {
            assert!(
                !events
                    .lock()
                    .unwrap()
                    .prepared
                    .iter()
                    .any(|(v, _)| *v == Venue::Okx)
            );
        }
        drop(executor);
        drop(ledger);
        tokio::fs::remove_file(path).await.unwrap();
    }
}

#[tokio::test]
async fn a_wrong_mode_in_order_readback_stops_execution_before_the_second_leg() {
    let (executor, ledger, events, path) = fixture(false, false).await;
    events.lock().unwrap().report_wrong_mode = true;
    let setup = PositionSetup {
        margin_mode: MarginMode::Cross,
        leverage: Some(dec!(5)),
        ..PositionSetup::default()
    };
    let failure = executor
        .open(
            &execution_plan(),
            Strategy::Funding,
            Decimal::ZERO,
            "wrong-mode",
            &setup,
        )
        .await
        .err()
        .expect("a mismatched mode must not be accepted");
    assert!(failure.to_string().contains("意图不一致"));
    assert_eq!(events.lock().unwrap().orders.len(), 1);
    assert_eq!(
        ledger.replay().await.unwrap().0.positions["wrong-mode"].status,
        PositionStatus::Opening
    );
    drop(executor);
    drop(ledger);
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn cross_isolated_topups_are_rejected_before_any_ledger_or_broker_operation() {
    let (executor, ledger, events, path) = fixture(false, false).await;
    let setup = PositionSetup {
        margin_mode: MarginMode::Cross,
        leverage: Some(dec!(5)),
        rules: TaskRules {
            auto_margin_pct: Some(dec!(15)),
            auto_margin_max_usdt: Some(dec!(100)),
            ..TaskRules::default()
        },
        ..PositionSetup::default()
    };
    assert!(
        executor
            .open(
                &execution_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "blocked",
                &setup
            )
            .await
            .is_err()
    );
    assert!(events.lock().unwrap().prepared.is_empty());
    assert!(ledger.replay().await.unwrap().0.positions.is_empty());
    let mut position = legacy_position();
    position.margin_mode = MarginMode::Cross;
    assert!(
        executor
            .add_margin(&mut position, Venue::Binance, dec!(10), "test")
            .await
            .is_err()
    );
    drop(executor);
    drop(ledger);
    tokio::fs::remove_file(path).await.unwrap();
}
