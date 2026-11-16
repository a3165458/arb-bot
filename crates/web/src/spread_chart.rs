//! 历史价差 K 线（参照 zuudot 的 trading 页）：拉两家同一合约的历史 K 线，按时间对齐，算出
//! 「空腿 − 多腿」的价差序列与统计（最高、最低、中位数、当前处在历史的哪个分位）。
//!
//! - **只读**：只打各家公开的 K 线接口，不碰账户、不下单。
//! - **口径**：价差 = (B 收盘 − A 收盘) / 两者均值 × 100%（同时给出价格差）。A = 多腿（exchange1），
//!   B = 空腿（exchange2），与 zuudot 的 `long1_short2` 一致：价差越大越适合开仓，收敛就是赚。
//! - **不是可成交价差**：K 线是成交价（Arcus 是预言机价），不含盘口买卖价差、深度与手续费。图只回答
//!   「现在的价差在历史上处在什么位置」；真正下单仍按现拉盘口核算。
//! - **对齐**：两边按 K 线开盘时刻取交集，缺 K 线（停盘、上新）的时刻不硬凑。
//! - **限频**：每家每次最多 [`MAX_BARS`] 根；Lighter RH 的额度和实盘共用，分页之间留间隔。全局同时
//!   只拉一张图，结果缓存 [`CACHE_TTL`]。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use arb_core::Venue;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::AppState;

mod contract_history;

/// 每家每次最多拉这么多根。
pub const MAX_BARS: usize = 3000;
/// 同一张图（合约、两家、周期、天数）的结果缓存多久。
const CACHE_TTL: Duration = Duration::from_secs(60);
/// Lighter RH 分页之间的间隔：它的公开额度很紧，而且和实盘交易共用同一个 IP。
pub(crate) const LIGHTER_PAGE_GAP: Duration = Duration::from_millis(600);

/// 支持的周期（与 zuudot 一致）：分钟数、默认天数、最多天数。
pub const INTERVALS: [(&str, u32, f64, f64); 6] = [
    ("1m", 1, 16.0 / 24.0, 2.0),
    ("5m", 5, 3.0, 8.0),
    ("15m", 15, 6.0, 12.0),
    ("30m", 30, 12.0, 24.0),
    ("1h", 60, 24.0, 48.0),
    ("4h", 240, 48.0, 96.0),
];

pub fn interval_minutes(name: &str) -> Option<u32> {
    INTERVALS.iter().find(|i| i.0 == name).map(|i| i.1)
}

/// 支持画图的场所。
pub const VENUES: [Venue; 12] = [
    Venue::Okx,
    Venue::Binance,
    Venue::Bybit,
    Venue::Hyperliquid,
    Venue::HyperliquidXyz,
    Venue::HyperliquidIo,
    Venue::Lighter,
    Venue::LighterRh,
    Venue::Arcus,
    Venue::Mexc,
    Venue::Bitget,
    Venue::Aster,
];

/// 币安格式合约接口（`/fapi/v1/...`）的域名：币安自己，或它的分叉 Aster。
pub fn fapi_host(venue: Venue) -> &'static str {
    if venue == Venue::Aster {
        "https://fapi.asterdex.com"
    } else {
        "https://fapi.binance.com"
    }
}

/// 一根 K 线（毫秒开盘时刻）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Bar {
    pub t: i64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
}

/// 对齐后的一个点。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Point {
    /// 秒（lightweight-charts 用秒）。
    pub time: i64,
    pub a: f64,
    pub b: f64,
    /// B − A（价格）。
    pub diff: f64,
    /// (B − A) / 均值 × 100。
    pub pct: f64,
}

/// 价差统计（按百分比口径）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Stats {
    pub points: usize,
    pub max_pct: f64,
    pub max_at: i64,
    pub min_pct: f64,
    pub min_at: i64,
    pub median_pct: f64,
    pub p10_pct: f64,
    pub p90_pct: f64,
    pub last_pct: f64,
    pub last_at: i64,
    /// 最新一点在窗口里的分位（0 = 历史最低，100 = 历史最高）。
    pub last_percentile: f64,
}

/// 两边按开盘时刻取交集，算出价差。价格非正的点丢弃。
pub fn align(a: &[Bar], b: &[Bar]) -> Vec<Point> {
    let b_by_t: HashMap<i64, &Bar> = b.iter().map(|bar| (bar.t, bar)).collect();
    let mut out: Vec<Point> = a
        .iter()
        .filter_map(|x| {
            let y = b_by_t.get(&x.t)?;
            if x.c <= 0.0 || y.c <= 0.0 || !x.c.is_finite() || !y.c.is_finite() {
                return None;
            }
            let diff = y.c - x.c;
            Some(Point {
                time: x.t / 1000,
                a: x.c,
                b: y.c,
                diff,
                pct: diff / ((x.c + y.c) / 2.0) * 100.0,
            })
        })
        .collect();
    out.sort_by_key(|p| p.time);
    out.dedup_by_key(|p| p.time);
    out
}

/// `value` 在 `sorted` 里的分位（0～100）：严格小于它的占比 + 相等的一半。
pub fn percentile_of(sorted: &[f64], value: f64) -> f64 {
    if sorted.is_empty() {
        return 50.0;
    }
    let below = sorted.partition_point(|x| *x < value);
    let upto = sorted.partition_point(|x| *x <= value);
    ((below as f64 + (upto - below) as f64 / 2.0) / sorted.len() as f64 * 100.0).clamp(0.0, 100.0)
}

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let pos = q * (sorted.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo as f64)
}

pub fn stats(points: &[Point]) -> Option<Stats> {
    let last = points.last()?;
    let mut sorted: Vec<f64> = points.iter().map(|p| p.pct).collect();
    sorted.sort_by(f64::total_cmp);
    let max = points.iter().max_by(|x, y| x.pct.total_cmp(&y.pct))?;
    let min = points.iter().min_by(|x, y| x.pct.total_cmp(&y.pct))?;
    Some(Stats {
        points: points.len(),
        max_pct: max.pct,
        max_at: max.time,
        min_pct: min.pct,
        min_at: min.time,
        median_pct: quantile(&sorted, 0.5),
        p10_pct: quantile(&sorted, 0.1),
        p90_pct: quantile(&sorted, 0.9),
        last_pct: last.pct,
        last_at: last.time,
        last_percentile: percentile_of(&sorted, last.pct),
    })
}

/// 把细周期的价差点聚合成「价差 K 线」：每 `bucket_sec` 一根，开 / 高 / 低 / 收取这段时间里的价差。
/// `bucket_sec` 不大于点间距时，每点自成一根（开 = 上一点的收）。
pub fn spread_candles(points: &[Point], bucket_sec: i64, pct: bool) -> Vec<Value> {
    let pick = |p: &Point| if pct { p.pct } else { p.diff };
    let mut buckets: BTreeMap<i64, (f64, f64, f64, f64)> = BTreeMap::new();
    let mut prev: Option<f64> = None;
    for p in points {
        let v = pick(p);
        let key = p.time - p.time.rem_euclid(bucket_sec.max(1));
        buckets
            .entry(key)
            .and_modify(|(_, h, l, c)| {
                *h = h.max(v);
                *l = l.min(v);
                *c = v;
            })
            .or_insert_with(|| {
                let open = prev.unwrap_or(v);
                (open, v.max(open), v.min(open), v)
            });
        prev = Some(v);
    }
    buckets
        .into_iter()
        .map(|(time, (o, h, l, c))| json!({ "time": time, "open": o, "high": h, "low": l, "close": c }))
        .collect()
}

// ───────────────────────────── 各家 K 线 ─────────────────────────────

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| arb_venues::build_client(15).expect("HTTP 客户端"))
}

pub(crate) async fn get(url: String, venue: Venue) -> Result<Value> {
    arb_venues::get_json::<Value>(client().get(url), venue)
        .await
        .map_err(|e| anyhow!("{e}"))
}

pub(crate) async fn post(url: &str, body: Value, venue: Venue) -> Result<Value> {
    arb_venues::get_json::<Value>(client().post(url).json(&body), venue)
        .await
        .map_err(|e| anyhow!("{e}"))
}

pub(crate) fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
    .filter(|x| x.is_finite())
}

/// 一行 `[t, o, h, l, c, …]`（Binance / OKX / Bybit）。
fn array_bar(row: &Value) -> Option<Bar> {
    let row = row.as_array()?;
    Some(Bar {
        t: num(row.first()?)? as i64,
        o: num(row.get(1)?)?,
        h: num(row.get(2)?)?,
        l: num(row.get(3)?)?,
        c: num(row.get(4)?)?,
    })
}

fn finish(mut bars: Vec<Bar>, start_ms: i64, end_ms: i64) -> Vec<Bar> {
    bars.retain(|b| b.t >= start_ms && b.t <= end_ms && b.c > 0.0);
    bars.sort_by_key(|b| b.t);
    bars.dedup_by_key(|b| b.t);
    if bars.len() > MAX_BARS {
        bars.drain(..bars.len() - MAX_BARS);
    }
    bars
}

/// Hyperliquid（含 HIP-3 子交易所）上 `base` 的原始币名（如 `xyz:GIGADEV`、`kPEPE`）：按 meta 反查。
pub(crate) async fn hl_coin(venue: Venue, base: &str) -> Result<String> {
    let dex = arb_venues::hyperliquid::dex_name(venue);
    let meta_body = match dex {
        Some(dex) => json!({"type": "meta", "dex": dex}),
        None => json!({"type": "meta"}),
    };
    let meta = post("https://api.hyperliquid.xyz/info", meta_body, venue).await?;
    meta["universe"]
        .as_array()
        .and_then(|u| {
            u.iter().find_map(|asset| {
                let name = asset["name"].as_str()?;
                (arb_venues::hyperliquid::base_for(venue, name).as_deref() == Some(base)
                    && !asset["isDelisted"].as_bool().unwrap_or(false))
                .then(|| name.to_string())
            })
        })
        .ok_or_else(|| anyhow!("{venue} 没有 {base} 永续"))
}

/// Lighter（主网或 RH）的接口地址与 `base` 的 market_id。
pub(crate) async fn lighter_market(venue: Venue, base: &str) -> Result<(&'static str, i64)> {
    let host = if venue == Venue::LighterRh {
        "https://api.rh.lighter.xyz"
    } else {
        "https://mainnet.zklighter.elliot.ai"
    };
    let details = get(format!("{host}/api/v1/orderBookDetails"), venue).await?;
    let market = details["order_book_details"]
        .as_array()
        .and_then(|rows| {
            rows.iter().find(|r| {
                r["symbol"]
                    .as_str()
                    .is_some_and(|s| s.eq_ignore_ascii_case(base))
                    && r["status"].as_str() == Some("active")
            })
        })
        .and_then(|r| r["market_id"].as_i64())
        .ok_or_else(|| anyhow!("{venue} 没有 {base} 永续"))?;
    Ok((host, market))
}

/// 拉 `venue` 上 `base` 的永续 K 线，`[start_ms, end_ms]`，周期 `interval`（[`INTERVALS`] 里的名字）。
pub async fn fetch_bars(
    venue: Venue,
    base: &str,
    interval: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<Bar>> {
    let minutes = interval_minutes(interval).ok_or_else(|| anyhow!("不支持的周期 {interval}"))?;
    let step = i64::from(minutes) * 60_000;
    let base = base.trim().to_ascii_uppercase();
    anyhow::ensure!(
        !base.is_empty() && base.chars().all(|c| c.is_ascii_alphanumeric()),
        "合约名只能是字母数字"
    );
    let bars = match venue {
        // Aster 是币安分叉：K 线接口与格式一样，只是域名不同。
        Venue::Binance | Venue::Aster => {
            let host = fapi_host(venue);
            let mut out = Vec::new();
            let mut from = start_ms;
            while from <= end_ms && out.len() < MAX_BARS {
                let rows = get(
                    format!(
                        "{host}/fapi/v1/klines?symbol={base}USDT&interval={interval}&startTime={from}&endTime={end_ms}&limit=1500"
                    ),
                    venue,
                )
                .await?;
                let page: Vec<Bar> = rows
                    .as_array()
                    .map(|r| r.iter().filter_map(array_bar).collect())
                    .unwrap_or_default();
                let Some(last) = page.last().map(|b| b.t) else {
                    break;
                };
                out.extend(page);
                from = last + step;
            }
            out
        }
        Venue::Bybit => {
            let bybit = match minutes {
                240 => "240".to_string(),
                m => m.to_string(),
            };
            let mut out = Vec::new();
            let mut to = end_ms;
            while to >= start_ms && out.len() < MAX_BARS {
                let v = get(
                    format!(
                        "https://api.bybit.com/v5/market/kline?category=linear&symbol={base}USDT&interval={bybit}&start={start_ms}&end={to}&limit=1000"
                    ),
                    venue,
                )
                .await?;
                if v["retCode"].as_i64() != Some(0) {
                    bail!("Bybit K 线：{}", v["retMsg"].as_str().unwrap_or("未知错误"));
                }
                let page: Vec<Bar> = v["result"]["list"]
                    .as_array()
                    .map(|r| r.iter().filter_map(array_bar).collect())
                    .unwrap_or_default();
                let Some(oldest) = page.iter().map(|b| b.t).min() else {
                    break;
                };
                out.extend(page);
                to = oldest - 1;
            }
            out
        }
        Venue::Okx => {
            let bar = match minutes {
                60 => "1H",
                240 => "4H",
                _ => interval,
            };
            // history-candles 覆盖更久的历史，每页 100 根，最新在前；`after` = 早于这个时刻。
            let mut out = Vec::new();
            let mut after = end_ms + step;
            while after > start_ms && out.len() < MAX_BARS {
                let v = get(
                    format!(
                        "https://www.okx.com/api/v5/market/history-candles?instId={base}-USDT-SWAP&bar={bar}&after={after}&limit=100"
                    ),
                    venue,
                )
                .await?;
                if v["code"].as_str() != Some("0") {
                    bail!("OKX K 线：{}", v["msg"].as_str().unwrap_or("未知错误"));
                }
                let page: Vec<Bar> = v["data"]
                    .as_array()
                    .map(|r| r.iter().filter_map(array_bar).collect())
                    .unwrap_or_default();
                let Some(oldest) = page.iter().map(|b| b.t).min() else {
                    break;
                };
                out.extend(page);
                after = oldest;
            }
            out
        }
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            let coin = hl_coin(venue, &base).await?;
            let mut out = Vec::new();
            let mut from = start_ms;
            while from <= end_ms && out.len() < MAX_BARS {
                let rows = post(
                    "https://api.hyperliquid.xyz/info",
                    json!({"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": from, "endTime": end_ms}}),
                    venue,
                )
                .await?;
                let page: Vec<Bar> = rows
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some(Bar {
                                    t: x["t"].as_i64()?,
                                    o: num(&x["o"])?,
                                    h: num(&x["h"])?,
                                    l: num(&x["l"])?,
                                    c: num(&x["c"])?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let Some(last) = page.last().map(|b| b.t) else {
                    break;
                };
                let more = page.len() >= 5000;
                out.extend(page);
                if !more {
                    break;
                }
                from = last + step;
            }
            out
        }
        Venue::Lighter | Venue::LighterRh => {
            let (host, market) = lighter_market(venue, &base).await?;
            let mut out = Vec::new();
            let mut to = end_ms / 1000;
            let start = start_ms / 1000;
            let page_span = 480 * i64::from(minutes) * 60;
            let mut first = true;
            while to > start && out.len() < MAX_BARS {
                if !first {
                    tokio::time::sleep(LIGHTER_PAGE_GAP).await;
                }
                first = false;
                let from = (to - page_span).max(start);
                let v = get(
                    format!(
                        "{host}/api/v1/candles?market_id={market}&resolution={interval}&start_timestamp={from}&end_timestamp={to}&count_back=480"
                    ),
                    venue,
                )
                .await?;
                if v["code"].as_i64() != Some(200) {
                    bail!("{venue} K 线 code={}", v["code"]);
                }
                let page: Vec<Bar> = v["c"]
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some(Bar {
                                    t: x["t"].as_i64()?,
                                    o: num(&x["o"])?,
                                    h: num(&x["h"])?,
                                    l: num(&x["l"])?,
                                    c: num(&x["c"])?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                if page.is_empty() {
                    break;
                }
                out.extend(page);
                to = from - 1;
            }
            out
        }
        Venue::Mexc | Venue::Bitget => {
            contract_history::fetch(venue, &base, interval, start_ms, end_ms).await?
        }
        Venue::Arcus => {
            let mut out = Vec::new();
            let mut to = (end_ms + step) * 1000;
            while to / 1000 > start_ms && out.len() < MAX_BARS {
                let v = get(
                    format!(
                        "https://api.arcus.xyz/v1/candles?market={base}-USD&timeframe={interval}&to={to}&countback=1500"
                    ),
                    venue,
                )
                .await?;
                if let Some(error) = v["error"].as_str() {
                    bail!("Arcus K 线：{error}");
                }
                let page: Vec<Bar> = v["candles"]
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some(Bar {
                                    t: x["openTime"].as_i64()? / 1000,
                                    o: num(&x["open"])?,
                                    h: num(&x["high"])?,
                                    l: num(&x["low"])?,
                                    c: num(&x["close"])?,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let Some(oldest) = page.iter().map(|b| b.t).min() else {
                    break;
                };
                let full = page.len() >= 1500;
                out.extend(page);
                if !full {
                    break;
                }
                to = oldest * 1000;
            }
            out
        }
        other => bail!("{other} 暂不支持历史价差图"),
    };
    Ok(finish(bars, start_ms, end_ms))
}

// ───────────────────────────── 拼图 + 缓存 ─────────────────────────────

/// 一张图的完整结果。
#[derive(Debug, Clone, Serialize)]
pub struct Chart {
    pub base: String,
    pub a: Venue,
    pub b: Venue,
    /// 两边价格各乘的系数（zuudot 的 ratio1 / ratio2）：合约单位不同时用。
    pub ratio_a: f64,
    pub ratio_b: f64,
    pub interval: String,
    pub days: f64,
    pub points: Vec<Point>,
    pub stats: Option<Stats>,
    pub a_bars: usize,
    pub b_bars: usize,
    pub notes: Vec<String>,
    /// 两边各自的 K 线（已乘比例），给主图画两家的蜡烛。不进 JSON 的 `chart`，由接口另行给出。
    #[serde(skip)]
    pub bars_a: Vec<Bar>,
    #[serde(skip)]
    pub bars_b: Vec<Bar>,
}

type Key = (String, Venue, Venue, String, u64, u64, u64);
type Cache = Mutex<HashMap<Key, (Instant, Arc<Chart>)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 测试用：把一张图放进缓存（不联网），之后同参数的 [`chart`] 直接命中。
#[cfg(test)]
pub(crate) async fn seed_cache(chart: Chart) {
    let key: Key = (
        chart.base.clone(),
        chart.a,
        chart.b,
        chart.interval.clone(),
        (chart.days * 1000.0) as u64,
        chart.ratio_a.to_bits(),
        chart.ratio_b.to_bits(),
    );
    cache()
        .lock()
        .await
        .insert(key, (Instant::now(), Arc::new(chart)));
}

/// 测试用：从价格序列造一张图（与 [`chart`] 同一套对齐与统计）。
#[cfg(test)]
pub(crate) fn chart_from(
    base: &str,
    a: Venue,
    b: Venue,
    interval: &str,
    days: f64,
    bars_a: &[Bar],
    bars_b: &[Bar],
) -> Chart {
    let points = align(bars_a, bars_b);
    Chart {
        base: base.to_string(),
        a,
        b,
        ratio_a: 1.0,
        ratio_b: 1.0,
        interval: interval.to_string(),
        days,
        stats: stats(&points),
        points,
        a_bars: bars_a.len(),
        b_bars: bars_b.len(),
        notes: Vec::new(),
        bars_a: bars_a.to_vec(),
        bars_b: bars_b.to_vec(),
    }
}

/// 全局只同时拉一张图（同一张图的并发请求等前一个结果、走缓存）。
fn fetch_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// 拉一张图（带缓存）。`days` 会夹在这个周期允许的范围内。
pub async fn chart(
    base: &str,
    a: Venue,
    b: Venue,
    interval: &str,
    days: f64,
) -> Result<Arc<Chart>> {
    chart_with(base, a, b, interval, days, 1.0, 1.0).await
}

/// [`chart`]，两边价格各乘 `ratio_a` / `ratio_b`（0.000001 ～ 1000000）。
pub async fn chart_with(
    base: &str,
    a: Venue,
    b: Venue,
    interval: &str,
    days: f64,
    ratio_a: f64,
    ratio_b: f64,
) -> Result<Arc<Chart>> {
    for r in [ratio_a, ratio_b] {
        anyhow::ensure!(
            r.is_finite() && (1e-6..=1e6).contains(&r),
            "比例必须在 0.000001 到 1000000 之间"
        );
    }
    let (_, minutes, _, max_days) = *INTERVALS
        .iter()
        .find(|i| i.0 == interval)
        .ok_or_else(|| anyhow!("周期只能是 1m / 5m / 15m / 30m / 1h / 4h"))?;
    anyhow::ensure!(a != b, "两边不能是同一家");
    for venue in [a, b] {
        anyhow::ensure!(VENUES.contains(&venue), "{venue} 暂不支持历史价差图");
    }
    let days = days.clamp(1.0 / 24.0, max_days);
    let base = base.trim().to_ascii_uppercase();
    let key: Key = (
        base.clone(),
        a,
        b,
        interval.to_string(),
        (days * 1000.0) as u64,
        ratio_a.to_bits(),
        ratio_b.to_bits(),
    );
    if let Some((at, hit)) = cache().lock().await.get(&key)
        && at.elapsed() < CACHE_TTL
    {
        return Ok(Arc::clone(hit));
    }
    let _guard = fetch_lock().lock().await;
    if let Some((at, hit)) = cache().lock().await.get(&key)
        && at.elapsed() < CACHE_TTL
    {
        return Ok(Arc::clone(hit));
    }
    let end_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = end_ms - (days * 86_400_000.0) as i64;
    let (bars_a, bars_b) = tokio::join!(
        fetch_bars(a, &base, interval, start_ms, end_ms),
        fetch_bars(b, &base, interval, start_ms, end_ms)
    );
    let bars_a = bars_a.with_context(|| format!("{a} 的 {base} K 线"))?;
    let bars_b = bars_b.with_context(|| format!("{b} 的 {base} K 线"))?;
    let scale = |bars: &[Bar], r: f64| -> Vec<Bar> {
        bars.iter()
            .map(|x| Bar {
                t: x.t,
                o: x.o * r,
                h: x.h * r,
                l: x.l * r,
                c: x.c * r,
            })
            .collect()
    };
    let (bars_a, bars_b) = (scale(&bars_a, ratio_a), scale(&bars_b, ratio_b));
    let points = align(&bars_a, &bars_b);
    let mut notes = Vec::new();
    if [a, b].contains(&Venue::Arcus) {
        notes.push("Arcus 的 K 线是预言机价，不是成交价".into());
    }
    let expected = (days * 1440.0 / f64::from(minutes)) as usize;
    if points.len() * 10 < expected * 7 && !points.is_empty() {
        notes.push(format!(
            "两家能对上的 K 线只有 {} 根（这段时间约 {expected} 根）：有一家停盘、上新不久或缺数据",
            points.len()
        ));
    }
    if let Some(s) = stats(&points)
        && (s.max_pct.abs() > 50.0 || s.min_pct.abs() > 50.0)
    {
        notes.push("价差超过 50%：两家的合约单位不同（用「比例」换算）或根本不是同一个标的，换算对之前别用来交易".into());
    }
    let chart = Arc::new(Chart {
        base,
        a,
        b,
        interval: interval.to_string(),
        days,
        ratio_a,
        ratio_b,
        stats: stats(&points),
        points,
        a_bars: bars_a.len(),
        b_bars: bars_b.len(),
        notes,
        bars_a,
        bars_b,
    });
    let mut cache = cache().lock().await;
    cache.retain(|_, (at, _)| at.elapsed() < CACHE_TTL);
    cache.insert(key, (Instant::now(), Arc::clone(&chart)));
    Ok(chart)
}

/// K 线转成 lightweight-charts 的蜡烛（秒）。
fn candles(bars: &[Bar]) -> Vec<Value> {
    bars.iter()
        .map(|b| json!({ "time": b.t / 1000, "open": b.o, "high": b.h, "low": b.l, "close": b.c }))
        .collect()
}

#[derive(Debug, Deserialize)]
pub struct ChartQuery {
    symbol: String,
    exchange1: String,
    exchange2: String,
    #[serde(default)]
    interval: Option<String>,
    #[serde(default)]
    days: Option<f64>,
    /// `long1_short2`（默认，A 多 B 空）或 `short1_long2`（反过来）。
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    ratio1: Option<f64>,
    #[serde(default)]
    ratio2: Option<f64>,
}

/// `GET /api/spread-chart?symbol=某合约&exchange1=okx&exchange2=binance&interval=5m&days=3`：只读。
pub async fn api_chart(
    State(_state): State<Arc<AppState>>,
    Query(q): Query<ChartQuery>,
) -> Response {
    let fail = |status: StatusCode, message: String| {
        (status, Json(json!({ "error": message }))).into_response()
    };
    let parse = |raw: &str| Venue::parse(raw).ok_or_else(|| format!("未知场所 {raw:?}"));
    let (mut a, mut b) = match (parse(&q.exchange1), parse(&q.exchange2)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return fail(StatusCode::BAD_REQUEST, e),
    };
    let (mut ra, mut rb) = (q.ratio1.unwrap_or(1.0), q.ratio2.unwrap_or(1.0));
    match q.direction.as_deref().unwrap_or("long1_short2") {
        "long1_short2" => {}
        "short1_long2" => {
            std::mem::swap(&mut a, &mut b);
            std::mem::swap(&mut ra, &mut rb);
        }
        other => {
            return fail(
                StatusCode::BAD_REQUEST,
                format!("direction 只能是 long1_short2 / short1_long2，收到 {other:?}"),
            );
        }
    }
    let interval = q.interval.unwrap_or_else(|| "5m".into());
    let default_days = INTERVALS
        .iter()
        .find(|i| i.0 == interval)
        .map_or(3.0, |i| i.2);
    match chart_with(&q.symbol, a, b, &interval, q.days.unwrap_or(default_days), ra, rb).await {
        Ok(chart) => Json(json!({
            "chart": &*chart,
            "long": chart.a,
            "short": chart.b,
            "candles_a": candles(&chart.bars_a),
            "candles_b": candles(&chart.bars_b),
            "spread_candles": spread_candles(&chart.points, i64::from(interval_minutes(&interval).unwrap_or(5)) * 60, true),
            "intervals": INTERVALS.iter().map(|i| json!({"name": i.0, "default_days": i.2, "max_days": i.3})).collect::<Vec<_>>(),
            "venues": VENUES,
        }))
        .into_response(),
        Err(error) => fail(StatusCode::BAD_GATEWAY, format!("{error:#}")),
    }
}

// ───────────────────────────── 给自动交易的历史分位门槛 ─────────────────────────────

/// 一个候选（多 `long`、空 `short`）现在的价差在历史上处在第几分位（0～100，越高越适合开）。
/// 用 1 小时线、`days` 天；结果缓存在 [`chart`] 里。
pub async fn entry_percentile(
    base: &str,
    long: Venue,
    short: Venue,
    interval: &str,
    days: f64,
) -> Result<(f64, Stats)> {
    let chart = chart(base, long, short, interval, days).await?;
    let stats = chart
        .stats
        .ok_or_else(|| anyhow!("两家没有能对上的历史 K 线"))?;
    let minutes = interval_minutes(interval).unwrap_or(60);
    let age = chrono::Utc::now().timestamp() - stats.last_at;
    anyhow::ensure!(
        age <= i64::from(minutes) * 60 * 3,
        "最新一根能对上的 K 线是 {} 分钟前的，太旧",
        age / 60
    );
    anyhow::ensure!(stats.points >= 24, "历史只有 {} 根，太少", stats.points);
    Ok((stats.last_percentile, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(t: i64, c: f64) -> Bar {
        Bar {
            t,
            o: c,
            h: c,
            l: c,
            c,
        }
    }

    #[test]
    fn the_user_example_okx_vs_binance_openai() {
        // 9-25：币安 1597.3、OK 1612.5 → 差 15.2；9-30：币安 1604.1、OK 1653.8 → 差 49.7。
        let binance = [bar(1_000, 1597.3), bar(2_000, 1604.1), bar(3_000, 1600.0)];
        let okx = [bar(1_000, 1612.5), bar(2_000, 1653.8), bar(4_000, 1700.0)];
        let p = align(&binance, &okx);
        assert_eq!(p.len(), 2, "只对齐两边都有的时刻");
        assert!((p[0].diff - 15.2).abs() < 1e-9);
        assert!((p[1].diff - 49.7).abs() < 1e-9);
        assert!((p[1].diff - p[0].diff - 34.5).abs() < 1e-9);
        assert!((p[0].pct - 15.2 / 1604.9 * 100.0).abs() < 1e-9);
        let s = stats(&p).unwrap();
        assert_eq!((s.max_at, s.min_at), (2, 1));
        assert_eq!(
            s.last_percentile, 75.0,
            "两点里最高的那个：一个比它低 + 自己的一半"
        );
    }

    #[test]
    fn percentiles_quantiles_and_bad_prices() {
        let sorted: Vec<f64> = (0..=100).map(f64::from).collect();
        assert!((percentile_of(&sorted, 90.0) - 89.6).abs() < 0.5);
        assert_eq!(percentile_of(&sorted, -1.0), 0.0);
        assert_eq!(percentile_of(&sorted, 1000.0), 100.0);
        assert_eq!(quantile(&sorted, 0.5), 50.0);
        assert!(
            align(&[bar(1, 0.0)], &[bar(1, 10.0)]).is_empty(),
            "价格非正的点丢掉"
        );
        assert!(stats(&[]).is_none());
    }

    #[test]
    fn spread_candles_bucket_and_carry_the_previous_close() {
        let p: Vec<Point> = [(0, 1.0), (60, 3.0), (120, 2.0), (300, 5.0)]
            .iter()
            .map(|&(t, pct)| Point {
                time: t,
                a: 1.0,
                b: 1.0,
                diff: pct * 10.0,
                pct,
            })
            .collect();
        let c = spread_candles(&p, 300, true);
        assert_eq!(c.len(), 2);
        assert_eq!(c[0]["open"], 1.0);
        assert_eq!(c[0]["high"], 3.0);
        assert_eq!(c[0]["close"], 2.0);
        assert_eq!(c[1]["open"], 2.0, "下一根从上一根的收开始");
        assert_eq!(c[1]["high"], 5.0);
        let abs = spread_candles(&p, 300, false);
        assert_eq!(abs[1]["close"], 50.0);
    }

    #[test]
    fn interval_table_matches_the_reference_site() {
        assert_eq!(interval_minutes("5m"), Some(5));
        assert_eq!(interval_minutes("4h"), Some(240));
        assert_eq!(interval_minutes("2h"), None);
        let five = INTERVALS.iter().find(|i| i.0 == "5m").unwrap();
        assert_eq!((five.2, five.3), (3.0, 8.0));
        // 最长的周期乘上最多天数，不超过每家的上限。
        for (_, minutes, _, max_days) in INTERVALS {
            assert!(max_days * 1440.0 / f64::from(minutes) <= MAX_BARS as f64 + 1.0);
        }
    }

    /// 联网核对：`cargo test -p arb-web --bin arb-web spread_chart::tests::live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "联网"]
    async fn live() {
        for (base, a, b) in [
            ("OPENAI", Venue::Okx, Venue::Binance),
            ("OPENAI", Venue::HyperliquidIo, Venue::LighterRh),
            ("USO", Venue::Arcus, Venue::LighterRh),
            ("TSLA", Venue::HyperliquidXyz, Venue::Bybit),
        ] {
            match chart(base, a, b, "1h", 10.0).await {
                Ok(c) => println!(
                    "{base} {a}/{b}: {} / {} 根，对上 {}；{:?}；{:?}",
                    c.a_bars,
                    c.b_bars,
                    c.points.len(),
                    c.stats,
                    c.notes
                ),
                Err(e) => println!("{base} {a}/{b}: 失败 {e:#}"),
            }
        }
    }
}
