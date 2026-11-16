//! 两家的行情 WebSocket：把消息解析成「对某个市场盘口的改动」，不持有状态。
//!
//! - Lighter RH `wss://api.rh.lighter.xyz/stream`，频道 `order_book/{market_id}`：订阅时给全量快照
//!   （`subscribed/order_book`），之后每 50ms 一批增量（`update/order_book`，`size` 为 0 = 删档）。
//!   连续性：本条 `begin_nonce` 必须等于上一条 `nonce`，否则丢了增量，要重订阅。
//!   2 分钟内必须发一帧，发 `{"type":"ping"}`。
//! - Arcus `wss://api.arcus.xyz/v1/ws`，频道 `l2Orderbook`（`id` = `SPY-USD`、`nLevels`）：
//!   每 ~200ms 推一份**完整快照**，直接替换，不需要连续性检查。
//!
//! - Hyperliquid `wss://api.hyperliquid.xyz/ws`，`{"method":"subscribe","subscription":{"type":"l2Book","coin":"xyz:TSLA"}}`：
//!   每侧 20 档**完整快照**，2026-10-09 实测每 ~5 秒推一次（盘口没变也推）。一条连接最多 1000 个订阅。
//!
//! 实测（2026-10-07）37 个合约两家合计约 240 KB/s。
//!
//! 观察场所（2026-10-20 实测，每条都是**整本**，直接替换）。只订前几档：估算名义 2000 USDT 实测几档就吃得满
//! （Gate 10 档、Ondo 10 档、PopDEX 5 档都在几千到十几万 USDT），档数少消息就小，观察组不多占 CPU：
//! - Lighter 主网 `wss://mainnet.zklighter.elliot.ai/stream`：协议同 Lighter RH。
//! - Gate `wss://fx-ws.gateio.ws/v4/ws/usdt`，`futures.order_book`（`[合约, "10", "0"]`）：`event = all` 是整本，
//!   量是**张数**（要乘面值）；单个合约每秒约 4 条。心跳 `{"channel":"futures.ping"}`（不带 `time` 也认）。
//! - Bitget `wss://ws.bitget.com/v2/ws/public`，`books15`：`action = snapshot` 整本 15 档；心跳发文本 `ping`。
//! - Ondo `wss://api.ondoperps.xyz/ws`，`depthBooksPerps`：`type = update`，`data` 是数组（一条可能有几个市场），
//!   每个都是整本；心跳 `{"op":"ping"}`，180 秒不说话会被断开。
//! - PopDEX `wss://ws.popdex.xyz/v1/ws/public`，`books5`：`action = snapshot` 整本；心跳发文本 `ping`。

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

pub const LIGHTER_WS: &str = "wss://api.rh.lighter.xyz/stream";
pub const ARCUS_WS: &str = "wss://api.arcus.xyz/v1/ws";
pub const HYPERLIQUID_WS: &str = "wss://api.hyperliquid.xyz/ws";
/// Arcus 每侧订阅多少档。2000 USDT 级别的名义，20 档在 37 个合约上都吃得满（最薄的 AMD 10 档约 2300 USDT）。
pub const ARCUS_LEVELS: u32 = 25;
/// Hyperliquid `fast` 盘口每侧推几档（10-31 东京实测：普通 20 档，fast 只有 5 档）。
/// 某侧不满这么多档 = 那一侧整个就这几档。
pub const HL_TOP_LEVELS: usize = 5;

/// 一条消息对某个市场盘口的改动（或一笔成交）。
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// 整本替换。
    Snapshot {
        market: String,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
        nonce: Option<i64>,
    },
    /// 盘口顶部几档（Hyperliquid `fast` 盘口，买卖各最多 [`HL_TOP_LEVELS`] 档、约 0.5 秒一推）：
    /// 只替换它覆盖到的价格区间，更深的档位沿用最近一次整本（普通 20 档、5~10 秒一推）。
    Top {
        market: String,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    },
    /// Hyperliquid `bbo`（或订阅后取一次的当前值）：**只有买一 / 卖一**，没有更深的档位。`None` = 该侧没有挂单。
    /// 只在 [`HlBook::Bbo`] 模式产生；整本盘口就只有这一档，每次整体替换（见 `rh_spread::apply_at`）。
    Bbo {
        market: String,
        bid: Option<(Decimal, Decimal)>,
        ask: Option<(Decimal, Decimal)>,
        /// 订阅后取一次当前值 / 静默市场对账重取的应答（不是 `bbo` 推送）。
        seed: bool,
    },
    /// 增量（只有 Lighter）。`begin_nonce` 用来查连续性。
    Delta {
        market: String,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
        begin_nonce: Option<i64>,
        nonce: Option<i64>,
    },
    /// 一笔（聚合）成交，不改盘口。只有 Binance 的 `aggTrade`（挂单测算用）。
    Trade {
        market: String,
        price: Decimal,
        quantity: Decimal,
        /// 主动方是卖方（买方挂单被吃）：`m == true`。
        seller_initiated: bool,
        /// 成交时刻（毫秒）。
        time_ms: i64,
    },
    /// 一条消息里有几个市场（Ondo）。
    Batch(Vec<Event>),
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

/// 解析 Hyperliquid 的一条消息。`market` 是原始币名（`xyz:TSLA`）。`l2Book` 每条都是完整快照。
pub fn parse_hyperliquid(text: &str) -> Result<Event, String> {
    parse_hyperliquid_timed(text).map(|(event, _)| event)
}

/// Same JSON parse, retaining the producer timestamp separately from Lighter's nonce.
pub fn parse_hyperliquid_timed(text: &str) -> Result<(Event, Option<i64>), String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    match value.get("channel").and_then(Value::as_str) {
        Some("error") => {
            let message = value.get("data").map(Value::to_string).unwrap_or_default();
            return Ok((Event::Error(message.chars().take(160).collect()), None));
        }
        Some("l2Book") => {}
        Some("bbo") => return parse_hyperliquid_bbo(&value),
        Some("post") => return parse_hyperliquid_seed(&value),
        _ => return Ok((Event::Other, None)),
    }
    let data = value.get("data").ok_or("缺 data")?;
    let market = data
        .get("coin")
        .and_then(Value::as_str)
        .ok_or("缺 coin")?
        .to_string();
    let levels = data
        .get("levels")
        .and_then(Value::as_array)
        .filter(|sides| sides.len() == 2)
        .ok_or("levels 必须是两侧")?;
    let side = |v: &Value| -> Result<Vec<(Decimal, Decimal)>, String> {
        v.as_array()
            .ok_or("档位不是数组")?
            .iter()
            .map(|l| {
                let (Some(px), Some(sz)) = (
                    l.get("px").and_then(Value::as_str),
                    l.get("sz").and_then(Value::as_str),
                ) else {
                    return Err("档位缺 px / sz".to_string());
                };
                level(px, sz).ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
            })
            .collect()
    };
    let source_ms = match data.get("time") {
        None => None,
        Some(raw) => Some(
            raw.as_i64()
                .filter(|&ms| ms > 0)
                .ok_or("time 必须是正整数毫秒")?,
        ),
    };
    let (bids, asks) = (side(&levels[0])?, side(&levels[1])?);
    // `fast` 订阅的推送带 `"fast": true`（同一条连接上两种订阅都走 l2Book 频道，只能靠它分）。
    if data.get("fast").and_then(Value::as_bool) == Some(true) {
        // 没有时间就没法和整本排先后：丢掉，不拿它刷新盘口。
        if source_ms.is_none() {
            return Err("fast 盘口缺 time".into());
        }
        return Ok((Event::Top { market, bids, asks }, source_ms));
    }
    Ok((
        Event::Snapshot {
            market,
            bids,
            asks,
            nonce: None,
        },
        source_ms,
    ))
}

/// Hyperliquid 的盘口来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlBook {
    /// 只订 `bbo`（默认，**不用 `l2Book`**）：买一 / 卖一变化就推（BTC 约 115ms 一条），只有顶档。
    Bbo,
    /// 参考树原行为：`l2Book` 整本（约 12 秒一份）+ `fast` 五档顶部（约 0.5 秒一推）。
    L2,
}

/// `ARB_HL_BOOK=l2` 回到 [`HlBook::L2`]；其余（含不设）= [`HlBook::Bbo`]。
pub fn hyperliquid_book_mode() -> HlBook {
    static MODE: std::sync::OnceLock<HlBook> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| {
        match std::env::var("ARB_HL_BOOK")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Ok("l2") => HlBook::L2,
            _ => HlBook::Bbo,
        }
    })
}

/// 业务盘口订阅一个市场要发的那条：默认 `bbo`，`l2` 模式是 `l2Book`。
pub fn hyperliquid_subscribe(coin: &str) -> String {
    match hyperliquid_book_mode() {
        HlBook::Bbo => hyperliquid_bbo_subscribe(coin),
        HlBook::L2 => hyperliquid_l2_subscribe(coin),
    }
}

pub fn hyperliquid_l2_subscribe(coin: &str) -> String {
    serde_json::json!({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}})
        .to_string()
}

/// 初值请求的 `id` 起点：监控不靠 `id` 对应应答（应答自带币名），只要与别的请求区分开。
const SEED_ID_BASE: u64 = 1_000_000;

/// `bbo` 订阅之后不会立刻推当前值（东京实测首条 p90 约 8 秒、最长近 20 秒）：订阅之后用一次 WebSocket
/// `post`（info `l2Book`，约 4ms）取每个市场当前的最优一档做初值，只取顶档，不拿它的深度。
/// `index` 用来生成互不相同的请求 `id`。
pub fn hyperliquid_seed_request(coin: &str, index: usize) -> String {
    serde_json::json!({
        "method": "post",
        "id": SEED_ID_BASE + index as u64,
        "request": {"type": "info", "payload": {"type": "l2Book", "coin": coin}},
    })
    .to_string()
}

/// 一条 Hyperliquid 原文里的市场名（`bbo` 推送与初值应答都有 `"coin":"…"`）：不解析整条 JSON，
/// 给会话判断「这个市场多久没动静」用。
pub fn hyperliquid_market_hint(text: &str) -> Option<&str> {
    let rest = &text[text.find("\"coin\":\"")? + 8..];
    Some(&rest[..rest.find('"')?])
}

fn parse_hyperliquid_bbo(value: &Value) -> Result<(Event, Option<i64>), String> {
    let data = value.get("data").ok_or("缺 data")?;
    let market = data
        .get("coin")
        .and_then(Value::as_str)
        .ok_or("缺 coin")?
        .to_string();
    let sides = data
        .get("bbo")
        .and_then(Value::as_array)
        .filter(|sides| sides.len() == 2)
        .ok_or("bbo 必须是两侧")?;
    let side = |v: &Value| -> Result<Option<(Decimal, Decimal)>, String> {
        if v.is_null() {
            return Ok(None);
        }
        let (Some(px), Some(sz)) = (
            v.get("px").and_then(Value::as_str),
            v.get("sz").and_then(Value::as_str),
        ) else {
            return Err("bbo 档位缺 px / sz".to_string());
        };
        level(px, sz)
            .map(Some)
            .ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
    };
    let source_ms = data
        .get("time")
        .and_then(Value::as_i64)
        .filter(|&ms| ms > 0)
        .ok_or("bbo 缺 time")?;
    Ok((
        Event::Bbo {
            market,
            bid: side(&sides[0])?,
            ask: side(&sides[1])?,
            seed: false,
        },
        Some(source_ms),
    ))
}

/// 初值的应答：`{"channel":"post","data":{"id":…,"response":{"type":"info","payload":{"type":"l2Book","data":{…}}}}}`。
/// 取最优一档变成 [`Event::Bbo`]；别的 `post` 应答（下单类、错误）忽略。
fn parse_hyperliquid_seed(value: &Value) -> Result<(Event, Option<i64>), String> {
    let response = &value["data"]["response"];
    if response.get("type").and_then(Value::as_str) != Some("info") {
        return Ok((Event::Other, None));
    }
    let book = &response["payload"]["data"];
    let Some(market) = book.get("coin").and_then(Value::as_str) else {
        return Ok((Event::Other, None));
    };
    let sides = book
        .get("levels")
        .and_then(Value::as_array)
        .filter(|sides| sides.len() == 2)
        .ok_or("levels 必须是两侧")?;
    let best = |side: &Value| -> Result<Option<(Decimal, Decimal)>, String> {
        let Some(top) = side.as_array().and_then(|levels| levels.first()) else {
            return Ok(None);
        };
        let (Some(px), Some(sz)) = (
            top.get("px").and_then(Value::as_str),
            top.get("sz").and_then(Value::as_str),
        ) else {
            return Err("档位缺 px / sz".to_string());
        };
        level(px, sz)
            .map(Some)
            .ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
    };
    let source_ms = book
        .get("time")
        .and_then(Value::as_i64)
        .filter(|&ms| ms > 0)
        .ok_or("初值缺 time")?;
    Ok((
        Event::Bbo {
            market: market.to_string(),
            bid: best(&sides[0])?,
            ask: best(&sides[1])?,
            seed: true,
        },
        Some(source_ms),
    ))
}

/// 业务盘口同时订 fast（见 [`Event::Top`]）。`ARB_HL_FAST_TOP=0` 关掉，只用普通 20 档。
pub fn hyperliquid_fast_top_enabled() -> bool {
    static ON: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        std::env::var("ARB_HL_FAST_TOP").map_or(true, |v| v.trim() != "0")
    });
    *ON
}

pub fn hyperliquid_fast_subscribe(coin: &str) -> String {
    serde_json::json!({"method":"subscribe","subscription":{"type":"l2Book","coin":coin,"fast":true}}).to_string()
}

pub fn hyperliquid_bbo_subscribe(coin: &str) -> String {
    serde_json::json!({"method":"subscribe","subscription":{"type":"bbo","coin":coin}}).to_string()
}

/// Binance U 本位行情按路径分流：盘口在 `/public`，成交在 `/market`。**连错路径不报错，只是收不到**
/// （实测旧的 `wss://fstream.binance.com/ws` 订阅 `aggTrade` 一条都没有）。
pub const BINANCE_BOOK_WS: &str = "wss://fstream.binance.com/public/ws";
pub const BINANCE_TRADE_WS: &str = "wss://fstream.binance.com/market/ws";
/// 一条 `SUBSCRIBE` 最多带这么多个流：连接每秒最多收 10 条客户端消息，按市场一条条发会被断开。
pub const BINANCE_STREAMS_PER_MESSAGE: usize = 50;

fn binance_streams(method: &str, keys: &[String], suffix: &str) -> Vec<String> {
    keys.chunks(BINANCE_STREAMS_PER_MESSAGE)
        .enumerate()
        .map(|(index, chunk)| {
            let params: Vec<String> = chunk
                .iter()
                .map(|symbol| format!("{}@{suffix}", symbol.to_ascii_lowercase()))
                .collect();
            serde_json::json!({"method": method, "params": params, "id": index + 1}).to_string()
        })
        .collect()
}

/// 前 20 档部分盘口，100ms 一次，**只在有变化时推**（周末冷门合约 12 秒 2 条），每条都是完整的前 20 档。
pub fn binance_depth_subscribe(keys: &[String]) -> Vec<String> {
    binance_streams("SUBSCRIBE", keys, "depth20@100ms")
}

pub fn binance_trade_subscribe(keys: &[String]) -> Vec<String> {
    binance_streams("SUBSCRIBE", keys, "aggTrade")
}

/// 解析 Binance 的一条消息（`ws` 模式：不带 `stream` 外包装）。`market` 是原生名（`NVDAUSDT`）。
pub fn parse_binance(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    if let Some(error) = value.get("error") {
        return Ok(Event::Error(error.to_string()));
    }
    let field = |key: &str| value.get(key).and_then(Value::as_str);
    match field("e") {
        Some("depthUpdate") => {
            let market = field("s").ok_or("depthUpdate 缺 s")?.to_string();
            let side = |key: &str| -> Result<Vec<(Decimal, Decimal)>, String> {
                value
                    .get(key)
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("depthUpdate 缺 {key}"))?
                    .iter()
                    .map(|l| {
                        let (Some(px), Some(sz)) = (
                            l.get(0).and_then(Value::as_str),
                            l.get(1).and_then(Value::as_str),
                        ) else {
                            return Err("档位不是 [价格, 数量]".to_string());
                        };
                        level(px, sz).ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
                    })
                    .collect()
            };
            Ok(Event::Snapshot {
                market,
                bids: side("b")?,
                asks: side("a")?,
                nonce: value.get("u").and_then(Value::as_i64),
            })
        }
        Some("aggTrade") => {
            let (Some(market), Some(price), Some(quantity), Some(maker), Some(time_ms)) = (
                field("s"),
                field("p").and_then(arb_core::parse_decimal),
                field("q").and_then(arb_core::parse_decimal),
                value.get("m").and_then(Value::as_bool),
                value.get("T").and_then(Value::as_i64),
            ) else {
                return Err("aggTrade 缺字段".into());
            };
            Ok(Event::Trade {
                market: market.to_string(),
                price,
                quantity,
                seller_initiated: maker,
                time_ms,
            })
        }
        _ => Ok(Event::Other),
    }
}

/// Aster（币安分叉）的行情：消息格式与币安 `ws` 模式一致（`depthUpdate`，每条是完整的前 20 档，量是标的数量），
/// 解析直接用 [`parse_binance`]。2026-11-15 实测：周日股票类照常推。
pub const ASTER_WS: &str = "wss://fstream.asterdex.com/ws";
/// Aster 一条连接最多 200 个流。超了不是拒掉多出来的，而是整条连接被服务端关掉
/// （2026-11-15 实测订 300 个：`3003 subscribed channels exceeds limit`），所以多出来的不订。
pub const ASTER_STREAMS_PER_CONNECTION: usize = 200;

/// Aster 前 20 档部分盘口，100ms 一次，只在有变化时推。一条消息 50 个流（连接每秒最多收 10 条客户端消息）；
/// 超过 [`ASTER_STREAMS_PER_CONNECTION`] 的市场不订（它们没有盘口，页面上显示没报价）。
pub fn aster_depth_subscribe(keys: &[String]) -> Vec<String> {
    if keys.len() > ASTER_STREAMS_PER_CONNECTION {
        tracing::warn!(
            "价差监控：Aster 要订 {} 个盘口，一条连接最多 {ASTER_STREAMS_PER_CONNECTION} 个，后面的 {} 个不订",
            keys.len(),
            keys.len() - ASTER_STREAMS_PER_CONNECTION
        );
    }
    binance_streams(
        "SUBSCRIBE",
        &keys[..keys.len().min(ASTER_STREAMS_PER_CONNECTION)],
        "depth20@100ms",
    )
}

/// Aster 盘口（`depthUpdate`）的合约名（`s` 排在档位前面）。
pub fn aster_market(text: &str) -> Option<&str> {
    text.contains(r#""e":"depthUpdate""#)
        .then(|| field_value(text, r#""s":""#))
        .flatten()
}

pub const LIGHTER_MAINNET_WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const GATE_WS: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
pub const BITGET_WS: &str = "wss://ws.bitget.com/v2/ws/public";
pub const ONDO_WS: &str = "wss://api.ondoperps.xyz/ws";
pub const POPDEX_WS: &str = "wss://ws.popdex.xyz/v1/ws/public";
pub const MEXC_WS: &str = "wss://contract.mexc.com/edge";
/// 观察场所每条订阅消息最多带几个市场。
const WATCH_ARGS_PER_MESSAGE: usize = 20;

/// `[["价格", "数量"], …]`。
fn pairs_side(value: Option<&Value>) -> Result<Vec<(Decimal, Decimal)>, String> {
    let Some(Value::Array(rows)) = value else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let (Some(px), Some(sz)) = (
                row.get(0).and_then(Value::as_str),
                row.get(1).and_then(Value::as_str),
            ) else {
                return Err("档位不是 [价格, 数量]".to_string());
            };
            level(px, sz).ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
        })
        .collect()
}

/// 心跳回执（文本 `pong`）不是 JSON。
fn pong(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("pong")
}

pub fn gate_subscribe(contract: &str) -> String {
    serde_json::json!({"channel": "futures.order_book", "event": "subscribe", "payload": [contract, "10", "0"]})
        .to_string()
}

/// 解析 Gate 的一条消息。`market` 是合约名（`NVDA_USDT`），量是**张数**。只认 `all`（整本）；
/// 实测只推 `all`，万一推增量（`update`）就忽略，等下一份整本（每秒好几份）。
pub fn parse_gate(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
        return Ok(Event::Error(error.to_string().chars().take(160).collect()));
    }
    if value.get("channel").and_then(Value::as_str) != Some("futures.order_book")
        || value.get("event").and_then(Value::as_str) != Some("all")
    {
        return Ok(Event::Other);
    }
    let result = value.get("result").ok_or("缺 result")?;
    let market = result
        .get("contract")
        .and_then(Value::as_str)
        .ok_or("缺 contract")?
        .to_string();
    let side = |key: &str| -> Result<Vec<(Decimal, Decimal)>, String> {
        let Some(Value::Array(rows)) = result.get(key) else {
            return Ok(Vec::new());
        };
        rows.iter()
            .map(|row| {
                let price = row.get("p").and_then(Value::as_str).ok_or("档位缺 p")?;
                // 量是 JSON 整数张数（也兼容字符串）。
                let size = match row.get("s") {
                    Some(Value::Number(n)) => n.to_string(),
                    Some(Value::String(raw)) => raw.clone(),
                    _ => return Err("档位缺 s".to_string()),
                };
                level(price, &size).ok_or_else(|| format!("档位不是数字：{price} / {size}"))
            })
            .collect()
    };
    Ok(Event::Snapshot {
        market,
        bids: side("bids")?,
        asks: side("asks")?,
        nonce: None,
    })
}

/// Bitget：一条订阅消息带多个市场。
pub fn bitget_subscribe(keys: &[String]) -> Vec<String> {
    keys.chunks(WATCH_ARGS_PER_MESSAGE)
        .map(|chunk| {
            let args: Vec<Value> = chunk
                .iter()
                .map(|symbol| serde_json::json!({"instType": "USDT-FUTURES", "channel": "books15", "instId": symbol}))
                .collect();
            serde_json::json!({"op": "subscribe", "args": args}).to_string()
        })
        .collect()
}

/// 解析 Bitget 的一条消息。`market` 是 `instId`（`NVDAUSDT`）；`books15` 每条都是整本。
pub fn parse_bitget(text: &str) -> Result<Event, String> {
    if pong(text) {
        return Ok(Event::Other);
    }
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    if value.get("event").and_then(Value::as_str) == Some("error") {
        let message = value.get("msg").map(Value::to_string).unwrap_or_default();
        return Ok(Event::Error(message.chars().take(160).collect()));
    }
    if value.get("action").and_then(Value::as_str) != Some("snapshot") {
        return Ok(Event::Other);
    }
    let market = value
        .get("arg")
        .and_then(|arg| arg.get("instId"))
        .and_then(Value::as_str)
        .ok_or("缺 instId")?
        .to_string();
    let book = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or("缺 data")?;
    Ok(Event::Snapshot {
        market,
        bids: pairs_side(book.get("bids"))?,
        asks: pairs_side(book.get("asks"))?,
        nonce: None,
    })
}

/// Ondo：一条订阅消息带全部市场（服务端单条上限 32 KB，几十个市场名远不到）。
pub fn ondo_subscribe(keys: &[String]) -> Vec<String> {
    keys.chunks(WATCH_ARGS_PER_MESSAGE * 5)
        .map(|chunk| {
            serde_json::json!({"op": "subscribe", "channel": "depthBooksPerps", "markets": chunk, "limit": 10})
                .to_string()
        })
        .collect()
}

/// 解析 Ondo 的一条消息。`market` 是 `NVDA-USD.P`；`data` 里每个市场都是整本。
pub fn parse_ondo(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    match value.get("type").and_then(Value::as_str) {
        Some("error") => {
            let message = value
                .get("data")
                .or(value.get("message"))
                .map(Value::to_string);
            return Ok(Event::Error(
                message.unwrap_or_default().chars().take(160).collect(),
            ));
        }
        Some("update")
            if value.get("channel").and_then(Value::as_str) == Some("depthBooksPerps") => {}
        _ => return Ok(Event::Other),
    }
    let rows = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or("缺 data")?;
    let events = rows
        .iter()
        .map(|row| {
            Ok(Event::Snapshot {
                market: row
                    .get("market")
                    .and_then(Value::as_str)
                    .ok_or("缺 market")?
                    .to_string(),
                bids: pairs_side(row.get("bids"))?,
                asks: pairs_side(row.get("asks"))?,
                nonce: None,
            })
        })
        .collect::<Result<Vec<Event>, String>>()?;
    Ok(Event::Batch(events))
}

/// PopDEX：一条订阅消息带多个市场。
pub fn popdex_subscribe(keys: &[String]) -> Vec<String> {
    keys.chunks(WATCH_ARGS_PER_MESSAGE)
        .map(|chunk| {
            let args: Vec<Value> = chunk
                .iter()
                .map(|symbol| serde_json::json!({"topic": "books5", "category": "Futures", "symbol": symbol}))
                .collect();
            serde_json::json!({"op": "subscribe", "args": args}).to_string()
        })
        .collect()
}

/// 解析 PopDEX 的一条消息。`market` 是合约名原样（`SNDKUSDT`、`kPEPEUSDT`）；`books5` 每条都是整本。
pub fn parse_popdex(text: &str) -> Result<Event, String> {
    if pong(text) {
        return Ok(Event::Other);
    }
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    if value.get("event").and_then(Value::as_str) == Some("error") {
        let message = value.get("msg").map(Value::to_string).unwrap_or_default();
        return Ok(Event::Error(message.chars().take(160).collect()));
    }
    if value.get("action").and_then(Value::as_str) != Some("snapshot") {
        return Ok(Event::Other);
    }
    let market = value
        .get("arg")
        .and_then(|arg| arg.get("symbol"))
        .and_then(Value::as_str)
        .ok_or("缺 symbol")?
        .to_string();
    let data = value.get("data").ok_or("缺 data")?;
    Ok(Event::Snapshot {
        market,
        bids: pairs_side(data.get("b"))?,
        asks: pairs_side(data.get("a"))?,
        nonce: None,
    })
}

/// 不解析整条 JSON，从原文里取 `"字段":"值"` 的值（第一处）。
fn field_value<'a>(text: &'a str, field: &str) -> Option<&'a str> {
    let start = text.find(field)? + field.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    (end > 0).then(|| &rest[..end])
}

/// MEXC：每个合约一条订阅，`sub.depth.full` 每次推前 10 档整本（实测 2026-10-21：一条连接订 200 个
/// 股票合约全部成功，约 120 条/秒）。
pub fn mexc_subscribe(symbol: &str) -> String {
    serde_json::json!({"method": "sub.depth.full", "param": {"symbol": symbol, "limit": 10}})
        .to_string()
}

/// JSON 数字（价格、张数）。很小的价格可能写成科学计数法（`1e-5`）。
fn number_text(value: &serde_json::Number) -> Option<Decimal> {
    let text = value.to_string();
    arb_core::parse_decimal(&text).or_else(|| Decimal::from_scientific(&text).ok())
}

/// `[价格, 张数, 笔数]`，都是 JSON 数字。
fn number_side(value: Option<&Value>) -> Result<Vec<(Decimal, Decimal)>, String> {
    let Some(Value::Array(rows)) = value else {
        return Ok(Vec::new());
    };
    rows.iter()
        .map(|row| {
            let (Some(Value::Number(px)), Some(Value::Number(sz))) = (row.get(0), row.get(1))
            else {
                return Err("档位不是 [价格, 张数, 笔数]".to_string());
            };
            number_text(px)
                .zip(number_text(sz))
                .ok_or_else(|| format!("档位不是数字：{px} / {sz}"))
        })
        .collect()
}

/// 解析 MEXC 的一条消息。`market` 是合约名（`NVIDIA_USDT`，不是代码），量是**张数**。
/// 订阅回执 `rs.sub.depth.full`、心跳回执 `pong` 忽略；订了不存在的合约回 `rs.error`。
pub fn parse_mexc(text: &str) -> Result<Event, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| format!("不是 JSON：{error}"))?;
    match value.get("channel").and_then(Value::as_str) {
        Some("push.depth.full") => {}
        Some("rs.error") => {
            let message = value.get("data").map(Value::to_string).unwrap_or_default();
            return Ok(Event::Error(message.chars().take(160).collect()));
        }
        _ => return Ok(Event::Other),
    }
    let market = value
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or("缺 symbol")?
        .to_string();
    let book = value.get("data").ok_or("缺 data")?;
    Ok(Event::Snapshot {
        market,
        bids: number_side(book.get("bids"))?,
        asks: number_side(book.get("asks"))?,
        nonce: None,
    })
}

/// Gate 整本（`event = all`）的合约名，用来合并（其它消息不合并，照常解析）。
pub fn gate_market(text: &str) -> Option<&str> {
    text.contains(r#""event":"all""#)
        .then(|| field_value(text, r#""contract":""#))
        .flatten()
}

/// Bitget 整本（`action = snapshot`）的 `instId`。
pub fn bitget_market(text: &str) -> Option<&str> {
    text.contains(r#""action":"snapshot""#)
        .then(|| field_value(text, r#""instId":""#))
        .flatten()
}

/// MEXC 整本（`push.depth.full`）的合约名（顶层 `symbol` 排在 `data` 前面）。
pub fn mexc_market(text: &str) -> Option<&str> {
    text.contains(r#""channel":"push.depth.full""#)
        .then(|| field_value(text, r#""symbol":""#))
        .flatten()
}

/// PopDEX 整本（`action = snapshot`）的合约名。
pub fn popdex_market(text: &str) -> Option<&str> {
    text.contains(r#""action":"snapshot""#)
        .then(|| field_value(text, r#""symbol":""#))
        .flatten()
}
