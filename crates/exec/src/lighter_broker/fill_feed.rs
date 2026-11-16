//! Lighter 账户成交推送（`account_all_trades/{账户}`，公开频道、不用鉴权）：第一腿早一点拿到成交。
//!
//! 为什么：标准账户（0 费率）的吃单在 `sendTx` 之后约 0.3 秒撮合（交易所给 0 费率账户的固定延迟），
//! 但 `accountOrders` 要再过约 1.1 秒才查得到这张单。2026-10-21/22 东京 18 笔对照：发单→撮合中位
//! 299ms，撮合→程序确认中位 1089ms；同一时期这个频道在撮合后几十毫秒就推到（某合约 实测 27ms），
//! 推送里带着我们的客户订单号（`bid_client_id` / `ask_client_id`）。
//!
//! 只用来让 `place_for_hedge` 早返回一份**待核实**的成交（数量、成交额、手续费都按推送里的逐笔算），
//! 第二腿据此马上发出；记账仍以 REST 读回为准（`settle_hedge`）。推送断了、没收到、对不上（只成交了
//! 一部分、数量超了、方向或市场不对），一律等 REST 轮询的结果，和没有推送时完全一样。连接在后台自己
//! 重连，连不上也不影响交易台连接。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, Weak};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Side, Venue};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use super::{Trade, checked_add, checked_mul, decimal, trade_fee_ticks};

/// 推送来的成交留多久：够第一腿等完就行。
const KEEP: Duration = Duration::from_secs(120);
/// 多久一条消息都没有就当连接已死、重连。服务端要求 2 分钟内至少收到一帧，我们每 [`PING_EVERY`] 发一次。
const SILENCE: Duration = Duration::from_secs(90);
const PING_EVERY: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// 重连等待：1、2、4 … 秒，封顶 60 秒；连上过就从头算。
const RETRY_MAX: Duration = Duration::from_secs(60);

/// 一张单在推送里合起来的成交。
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Pushed {
    /// 交易所给的订单号（`venue_order_id` 里的那个）。
    pub order_index: i64,
    pub size: Decimal,
    pub value: Decimal,
    pub fee: Decimal,
}

pub(super) struct FillFeed {
    account: i64,
    /// 账户吃单费率（`accountLimits`），逐笔省略费率字段时用它判断能不能按 0 算（同 REST 读回）。
    taker_fee: Decimal,
    /// 客户订单号 → 推送来的逐笔（到达时刻）。
    trades: SyncMutex<HashMap<i64, Vec<(Instant, Trade)>>>,
    arrived: Notify,
    /// 订阅成功、连接还在。只用于观察（推送断了照样能下单，只是第一腿走 REST）。
    connected: AtomicBool,
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    trades: HashMap<String, Vec<Trade>>,
}

impl FillFeed {
    pub(super) fn new(account: i64, taker_fee: Decimal) -> Arc<Self> {
        Arc::new(Self {
            account,
            taker_fee,
            trades: SyncMutex::new(HashMap::new()),
            arrived: Notify::new(),
            connected: AtomicBool::new(false),
        })
    }

    /// 推送现在是不是连着（订阅成功之后、断开之前）。
    pub(super) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn set_connected(&self, on: bool) {
        self.connected.store(on, Ordering::Relaxed);
    }

    /// 在后台常连 `url`（`wss://…/stream`）并订阅这个账户的成交；券商没了（`Arc` 都丢了）就停。
    pub(super) fn spawn(self: &Arc<Self>, url: String, venue: Venue) {
        let feed = Arc::downgrade(self);
        let account = self.account;
        tokio::spawn(async move { run(feed, url, account, venue).await });
    }

    /// 收下一批推送来的成交（已经收过的同一笔不重复记），顺手丢掉超过 [`KEEP`] 的。
    pub(super) fn record(&self, trades: impl IntoIterator<Item = Trade>) {
        let now = Instant::now();
        {
            let mut by_client = self.trades.lock().unwrap_or_else(|p| p.into_inner());
            by_client.retain(|_, rows| {
                rows.retain(|(at, _)| now.saturating_duration_since(*at) < KEEP);
                !rows.is_empty()
            });
            for trade in trades {
                for client in self.clients_of(&trade) {
                    let rows = by_client.entry(client).or_default();
                    if !rows.iter().any(|(_, row)| row.trade_id == trade.trade_id) {
                        rows.push((now, trade.clone()));
                    }
                }
            }
        }
        self.arrived.notify_waiters();
    }

    /// 这笔成交属于我们哪张单（客户订单号）。
    fn clients_of(&self, trade: &Trade) -> Vec<i64> {
        let mut clients = Vec::new();
        if trade.ask_account_id == self.account
            && let Some(client) = trade.ask_client_id
        {
            clients.push(client);
        }
        if trade.bid_account_id == self.account
            && let Some(client) = trade.bid_client_id
        {
            clients.push(client);
        }
        clients
    }

    /// 客户订单号为 `client` 的单在推送里**已经全部成交**（逐笔合计正好是 `quantity`）时，返回合起来的
    /// 成交。还没到齐、只成交一部分、对不上（市场、方向、交易所订单号不一致、数量超了）都返回 `None`
    /// —— 交给 REST 读回去定。
    pub(super) fn filled(
        &self,
        client: i64,
        market: i32,
        side: Side,
        quantity: Decimal,
    ) -> Option<Pushed> {
        let by_client = self.trades.lock().unwrap_or_else(|p| p.into_inner());
        let rows = by_client.get(&client)?;
        let mut order_index = None;
        let (mut size, mut value, mut fee) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
        for (_, trade) in rows {
            let ours_ask =
                trade.ask_account_id == self.account && trade.ask_client_id == Some(client);
            let ours_bid =
                trade.bid_account_id == self.account && trade.bid_client_id == Some(client);
            let (ours, index) = match side {
                Side::Sell => (ours_ask && !ours_bid, trade.ask_id),
                Side::Buy => (ours_bid && !ours_ask, trade.bid_id),
            };
            if !ours || trade.market_id != market || *order_index.get_or_insert(index) != index {
                return None;
            }
            let maker = ours_ask == trade.is_maker_ask;
            let (tick, integrator) = trade_fee_ticks(trade, maker, self.taker_fee).ok()?;
            let amount = decimal(&trade.usd_amount).ok()?;
            let base = decimal(&trade.size).ok()?;
            if amount <= Decimal::ZERO || base <= Decimal::ZERO {
                return None;
            }
            let rate = Decimal::from(tick.checked_add(integrator)?) / Decimal::from(1_000_000);
            size = checked_add(size, base).ok()?;
            value = checked_add(value, amount).ok()?;
            fee = checked_add(fee, checked_mul(amount, rate).ok()?).ok()?;
        }
        (size == quantity).then_some(Pushed {
            order_index: order_index?,
            size,
            value,
            fee,
        })
    }

    /// 等到 [`Self::filled`] 有结果，最多等 `within`。
    pub(super) async fn wait_filled(
        &self,
        client: i64,
        market: i32,
        side: Side,
        quantity: Decimal,
        within: Duration,
    ) -> Option<Pushed> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            // 先登记要等的通知再查：查完、开始等之间到的推送不会漏掉。
            let arrived = self.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            if let Some(pushed) = self.filled(client, market, side, quantity) {
                return Some(pushed);
            }
            if tokio::time::timeout_at(deadline, arrived).await.is_err() {
                return None;
            }
        }
    }
}

async fn run(feed: Weak<FillFeed>, url: String, account: i64, venue: Venue) {
    let mut failures = 0u32;
    while feed.strong_count() > 0 {
        let ended = session(&feed, &url, account, venue).await;
        let was_connected = feed
            .upgrade()
            .is_some_and(|feed| feed.connected.swap(false, Ordering::Relaxed));
        let error = match ended {
            Ok(()) => return,
            Err(error) => error,
        };
        if was_connected {
            failures = 0;
        }
        let wait = Duration::from_secs(1u64 << failures.min(6)).min(RETRY_MAX);
        // 刚断的那一次记 warn，之后连续失败只在等待封顶时再记，免得刷屏。
        if failures == 0 || wait == RETRY_MAX {
            warn!(%venue, "Lighter 成交推送断了，{} 秒后重连（第一腿照常用 REST 确认）：{error}", wait.as_secs());
        }
        failures += 1;
        tokio::time::sleep(wait).await;
    }
}

/// 一次连接：连上、订阅、收推送，直到出错（`Err`）或券商没了（`Ok`）。
async fn session(
    feed: &Weak<FillFeed>,
    url: &str,
    account: i64,
    venue: Venue,
) -> Result<(), String> {
    let (mut ws, _) = tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| "连接超时".to_string())?
        .map_err(|error| error.to_string())?;
    let subscribe = format!(r#"{{"type":"subscribe","channel":"account_all_trades/{account}"}}"#);
    ws.send(Message::text(subscribe))
        .await
        .map_err(|error| error.to_string())?;
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            message = tokio::time::timeout(SILENCE, ws.next()) => {
                let message = message.map_err(|_| format!("{} 秒没收到任何消息", SILENCE.as_secs()))?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let Some(feed) = feed.upgrade() else { return Ok(()) };
                        let Ok(envelope) = serde_json::from_str::<Envelope>(&text) else {
                            continue;
                        };
                        match envelope.kind.as_str() {
                            // 订阅时的快照是以前的成交，第一腿等的是刚发出的单：只认之后的更新。
                            "subscribed/account_all_trades" => {
                                feed.connected.store(true, Ordering::Relaxed);
                                info!(%venue, "Lighter 成交推送已连上：第一腿撮合后几十毫秒就能拿到成交");
                            }
                            "update/account_all_trades" => {
                                feed.record(envelope.trades.into_values().flatten());
                            }
                            "error" => return Err(format!("服务端报错：{text}")),
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        ws.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?;
                    }
                    Some(Ok(Message::Close(frame))) => return Err(format!("服务端关闭连接：{frame:?}")),
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.to_string()),
                    None => return Err("连接结束".into()),
                }
            }
            _ = ping.tick() => {
                if feed.strong_count() == 0 {
                    let _ = ws.close(None).await;
                    return Ok(());
                }
                ws.send(Message::Ping(Vec::new().into())).await.map_err(|error| error.to_string())?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const US: i64 = 710_001;

    /// 一笔成交：我们（`US`）买、客户订单号 `client`、交易所订单号 900，对手挂单卖。
    fn bought(trade_id: i64, client: i64, size: &str, usd: &str, extra: &str) -> Trade {
        serde_json::from_str(&format!(
            r#"{{"trade_id":{trade_id},"market_id":24,"ask_id":5,"bid_id":900,"ask_client_id":77,"bid_client_id":{client},"ask_account_id":9,"bid_account_id":{US},"is_maker_ask":true,"size":"{size}","usd_amount":"{usd}"{extra}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn a_fully_filled_order_is_summed_from_its_pushed_trades() {
        let feed = FillFeed::new(US, Decimal::ZERO);
        // 同一笔推两次（重连后的重复）只算一次。
        feed.record([
            bought(1, 42, "1.0", "76.514", ""),
            bought(1, 42, "1.0", "76.514", ""),
        ]);
        assert_eq!(
            feed.filled(42, 24, Side::Buy, dec!(2.573)),
            None,
            "还没到齐"
        );
        feed.record([bought(2, 42, "1.573", "120.356522", "")]);
        assert_eq!(
            feed.filled(42, 24, Side::Buy, dec!(2.573)),
            Some(Pushed {
                order_index: 900,
                size: dec!(2.573),
                value: dec!(196.870522),
                fee: Decimal::ZERO,
            })
        );
        // 方向、市场、数量对不上：都不算，交给 REST。
        assert_eq!(feed.filled(42, 24, Side::Sell, dec!(2.573)), None);
        assert_eq!(feed.filled(42, 25, Side::Buy, dec!(2.573)), None);
        assert_eq!(feed.filled(42, 24, Side::Buy, dec!(2.0)), None);
        assert_eq!(feed.filled(43, 24, Side::Buy, dec!(2.573)), None);
    }

    #[test]
    fn trades_from_two_exchange_orders_or_a_missing_paid_fee_are_not_trusted() {
        let feed = FillFeed::new(US, Decimal::ZERO);
        let mut other = bought(2, 42, "1.573", "120.356522", "");
        other.bid_id = 901;
        feed.record([bought(1, 42, "1.0", "76.514", ""), other]);
        assert_eq!(feed.filled(42, 24, Side::Buy, dec!(2.573)), None);
        // 付费账户：逐笔带着费率就按它算；缺了就不下结论（同 REST 读回）。
        let paid = FillFeed::new(US, dec!(0.00028));
        paid.record([bought(3, 50, "1.0", "100", r#","taker_fee":280"#)]);
        assert_eq!(
            paid.filled(50, 24, Side::Buy, dec!(1.0)).map(|p| p.fee),
            Some(dec!(0.028))
        );
        paid.record([bought(4, 51, "1.0", "100", "")]);
        assert_eq!(paid.filled(51, 24, Side::Buy, dec!(1.0)), None);
    }

    /// 实网只读：连 Lighter RH 推送、订阅这个账户的成交（公开频道，不用凭据），几秒内应当订阅成功。
    /// `cargo test -p arb-exec --lib fill_feed::tests::live_feed_subscribes -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "连实网"]
    async fn live_feed_subscribes() {
        let _ = dotenvy::dotenv();
        let account: i64 = std::env::var("ARB_LIGHTER_RH_ACCOUNT_INDEX")
            .expect("ARB_LIGHTER_RH_ACCOUNT_INDEX")
            .parse()
            .unwrap();
        let feed = FillFeed::new(account, Decimal::ZERO);
        feed.spawn(
            super::super::LighterDeployment::Robinhood
                .stream_url()
                .to_string(),
            Venue::LighterRh,
        );
        for _ in 0..100 {
            if feed.is_connected() {
                println!("已订阅账户成交推送");
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("10 秒内没订阅上");
    }

    /// 实网只读：拿一张**已成交**的真实订单，用生产的匹配代码（[`FillFeed::record`] / [`FillFeed::filled`]）
    /// 对 Lighter 主网 `account_all_trades` 订阅快照，确认客户订单号、数量都对得上。订单由环境变量给：
    /// `ARB_PROBE_LIGHTER_ORDER`（客户订单号，如 `live-…-sell-0`）、`ARB_PROBE_LIGHTER_MARKET`、
    /// `ARB_PROBE_LIGHTER_SIDE`（buy / sell）、`ARB_PROBE_LIGHTER_QTY`。
    #[tokio::test]
    #[ignore = "连实网"]
    async fn live_snapshot_matches_a_real_order() {
        let _ = dotenvy::dotenv();
        let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("缺 {name}"));
        let account: i64 = var("ARB_LIGHTER_ACCOUNT_INDEX").parse().unwrap();
        let id = crate::types::ClientOrderId(var("ARB_PROBE_LIGHTER_ORDER"));
        let market: i32 = var("ARB_PROBE_LIGHTER_MARKET").parse().unwrap();
        let side = match var("ARB_PROBE_LIGHTER_SIDE").as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => panic!("方向只能是 buy / sell：{other}"),
        };
        let quantity: Decimal = var("ARB_PROBE_LIGHTER_QTY").parse().unwrap();
        let deployment = super::super::LighterDeployment::Mainnet;
        let (mut ws, _) = tokio_tungstenite::connect_async(deployment.stream_url())
            .await
            .unwrap();
        ws.send(Message::text(format!(
            r#"{{"type":"subscribe","channel":"account_all_trades/{account}"}}"#
        )))
        .await
        .unwrap();
        let feed = FillFeed::new(account, Decimal::ZERO);
        loop {
            let Some(Ok(Message::Text(text))) = ws.next().await else {
                continue;
            };
            let envelope: Envelope = serde_json::from_str(&text).unwrap();
            if envelope.kind == "subscribed/account_all_trades" {
                feed.record(envelope.trades.into_values().flatten());
                break;
            }
        }
        let client = super::super::client_index(deployment, account, &id);
        let pushed = feed.filled(client, market, side, quantity);
        println!("{id} → 客户订单号 {client}：{pushed:?}");
        assert!(pushed.is_some(), "快照里对不上这张单");
    }

    #[tokio::test]
    async fn waiting_returns_as_soon_as_the_last_trade_arrives_and_gives_up_after_the_window() {
        let feed = FillFeed::new(US, Decimal::ZERO);
        let pusher = Arc::clone(&feed);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            pusher.record([bought(1, 42, "1.0", "76.514", "")]);
        });
        let started = Instant::now();
        let pushed = feed
            .wait_filled(42, 24, Side::Buy, dec!(1.0), Duration::from_secs(5))
            .await;
        assert_eq!(pushed.map(|p| p.value), Some(dec!(76.514)));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            feed.wait_filled(43, 24, Side::Buy, dec!(1.0), Duration::from_millis(50))
                .await,
            None
        );
    }
}
