//! 两家的行情 WebSocket：把消息解析成「对某个市场盘口的改动」，不持有状态。
//!
//! - Lighter RH `wss://api.rh.lighter.xyz/stream`，频道 `order_book/{market_id}`：订阅时给全量快照
//!   （`subscribed/order_book`），之后每 50ms 一批增量（`update/order_book`，`size` 为 0 = 删档）。
//!   连续性：本条 `begin_nonce` 必须等于上一条 `nonce`，否则丢了增量，要重订阅。
//!   2 分钟内必须发一帧，发 `{"type":"ping"}`。
//! - Arcus `wss://api.arcus.xyz/v1/ws`，频道 `l2Orderbook`（`id` = `SPY-USD`、`nLevels`）：
//!   每 ~200ms 推一份**完整快照**，直接替换，不需要连续性检查。
//!
//! 实测（2026-10-07）37 个合约两家合计约 240 KB/s。

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

pub const LIGHTER_WS: &str = "wss://api.rh.lighter.xyz/stream";
pub const ARCUS_WS: &str = "wss://api.arcus.xyz/v1/ws";
/// Arcus 每侧订阅多少档。2000 USDT 级别的名义，20 档在 37 个合约上都吃得满（最薄的 AMD 10 档约 2300 USDT）。
pub const ARCUS_LEVELS: u32 = 25;

/// 一条消息对某个市场盘口的改动。
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// 整本替换。
    Snapshot {
        market: String,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
        nonce: Option<i64>,
    },
    /// 增量（只有 Lighter）。`begin_nonce` 用来查连续性。
    Delta {
        market: String,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
        begin_nonce: Option<i64>,
        nonce: Option<i64>,
    },
    /// 服务端报错（如订阅了不存在的市场）。
    Error(String),
    /// 心跳、确认、取消订阅回执等，忽略。
    Other,
}

#[derive(Deserialize)]
struct LighterLevel {
    price: String,
    size: String,
}

fn level(price: &str, size: &str) -> Option<(Decimal, Decimal)> {
    Some((
        arb_core::parse_decimal(price)?,
        arb_core::parse_decimal(size)?,
    ))
}

fn lighter_side(value: &Value) -> Result<Vec<(Decimal, Decimal)>, String> {
    let levels: Vec<LighterLevel> =
        serde_json::from_value(value.clone()).map_err(|error| format!("档位格式不对：{error}"))?;
    levels
        .iter()
        .map(|l| {
            level(&l.price, &l.size)
                .ok_or_else(|| format!("档位不是数字：{} / {}", l.price, l.size))
        })
        .collect()
}

/// 解析 Lighter 的一条消息。`market` 是 `order_book:26` 里的 `26`。
pub fn parse_lighter(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    if let Some(error) = value.get("error") {
        return Ok(Event::Error(error.to_string()));
    }
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    let snapshot = match kind {
        "subscribed/order_book" => true,
        "update/order_book" => false,
        _ => return Ok(Event::Other),
    };
    let market = value
        .get("channel")
        .and_then(Value::as_str)
        .and_then(|channel| channel.strip_prefix("order_book:"))
        .ok_or("缺 channel")?
        .to_string();
    let book = value.get("order_book").ok_or("缺 order_book")?;
    let bids = lighter_side(book.get("bids").unwrap_or(&Value::Null))?;
    let asks = lighter_side(book.get("asks").unwrap_or(&Value::Null))?;
    let nonce = book.get("nonce").and_then(Value::as_i64);
    Ok(if snapshot {
        Event::Snapshot {
            market,
            bids,
            asks,
            nonce,
        }
    } else {
        Event::Delta {
            market,
            bids,
            asks,
            begin_nonce: book.get("begin_nonce").and_then(Value::as_i64),
            nonce,
        }
    })
}

fn arcus_side(value: Option<&Value>) -> Result<Vec<(Decimal, Decimal)>, String> {
    let Some(Value::Array(rows)) = value else {
        return Err("缺档位".into());
    };
    rows.iter()
        .map(|row| {
            let pair = row
                .as_array()
                .filter(|pair| pair.len() >= 2)
                .ok_or("档位不是 [价格, 数量]")?;
            match (pair[0].as_str(), pair[1].as_str()) {
                (Some(price), Some(size)) => {
                    level(price, size).ok_or_else(|| format!("档位不是数字：{price} / {size}"))
                }
                _ => Err("档位不是字符串".to_string()),
            }
        })
        .collect()
}

/// 解析 Arcus 的一条消息。`market` 是 `SPY-USD`。`l2Orderbook` 每条都是完整快照。
pub fn parse_arcus(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    if kind == "error" {
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("未知错误");
        // 无效市场时服务端会把全部合法市场列出来，截短。
        return Ok(Event::Error(message.chars().take(160).collect()));
    }
    if !matches!(kind, "subscribed" | "channel_data")
        || value.get("channel").and_then(Value::as_str) != Some("l2Orderbook")
    {
        return Ok(Event::Other);
    }
    let market = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or("缺 id")?
        .to_string();
    let contents = value.get("contents").ok_or("缺 contents")?;
    Ok(Event::Snapshot {
        market,
        bids: arcus_side(contents.get("bids"))?,
        asks: arcus_side(contents.get("asks"))?,
        nonce: None,
    })
}

pub fn lighter_subscribe(market_id: i64) -> String {
    serde_json::json!({"type": "subscribe", "channel": format!("order_book/{market_id}")})
        .to_string()
}

pub fn lighter_unsubscribe(market_id: i64) -> String {
    serde_json::json!({"type": "unsubscribe", "channel": format!("order_book/{market_id}")})
        .to_string()
}

pub fn arcus_subscribe(market: &str) -> String {
    serde_json::json!({"type": "subscribe", "channel": "l2Orderbook", "id": market, "nLevels": ARCUS_LEVELS})
        .to_string()
}
