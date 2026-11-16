//! 历史资金费（参照 zuudot trading 页的 Funding 副图）：两家同一合约的资金费率历史、结算点，以及
//! 按方向算出的「净资金费」结算柱与合计。
//!
//! - **只读**：只打各家公开的资金费历史接口，不碰账户。
//! - **历史点是已结算的费率**：各家公开接口只有结算值，没有盘中预测值的历史（zuudot 的曲线是它自己每分钟
//!   采的预测值，回填不了）。曲线的**最后一点**是扫描快照里的当前费率（下一次结算适用的预测值）。
//! - **单位**：每个结算周期的原始费率（小数），不折成每小时、不年化。Binance 4h 和 HL 1h 各按各的周期。
//! - **对齐**：结算时刻取整到分钟（Binance 带 +1ms、HL 带几十毫秒的偏移），两家同一分钟结算算「同时」。
//! - **净资金费**：多 `long`、空 `short`：净 = 空腿费率 − 多腿费率（正 = 收钱）。只有一家结算的时刻只算那一家。
//! - **限频**：结果缓存 [`CACHE_TTL`]（已结算的历史只在结算时变化）；全局同时只拉一组。

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use arb_core::Venue;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::AppState;
use crate::spread_chart::{LIGHTER_PAGE_GAP, VENUES, get, hl_coin, lighter_market, num, post};

/// 同一家同一合约的结算历史缓存多久。
const CACHE_TTL: Duration = Duration::from_secs(5 * 60);
/// 每家最多翻多少页（防止接口行为变了以后死循环）。
const MAX_PAGES: usize = 40;
/// OKX 公共接口 10 次 / 2 秒：翻页之间留间隔（Lighter 的额度和实盘共用，用价差图同一个更长的间隔）。
const PAGE_GAP: Duration = Duration::from_millis(250);
/// 最多回看多少天（与价差图 4 小时线的上限一致）。
pub const MAX_DAYS: f64 = 96.0;

/// 一次结算（毫秒，已取整到分钟；`rate` 是这个周期的费率小数，正 = 多头付给空头）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Settlement {
    pub t: i64,
    /// 这一刻结算的合计费率（含 `special`）。
    pub rate: f64,
    /// 其中币安的分红资金费（`rateType: "Special"`，除息日在常规结算后 1 秒多再结一笔，空头付给多头）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub special: Option<f64>,
    /// 这一刻只有分红那一笔、没有常规结算：推结算周期时不算它。
    #[serde(skip)]
    pub special_only: bool,
}

/// 毫秒取整到最近的整分钟。
pub fn round_minute(ms: i64) -> i64 {
    (ms + 30_000).div_euclid(60_000) * 60_000
}

/// 取整、裁剪到 `[start_ms, end_ms]`（前后各放宽一分钟）、排序、同一分钟留最后一条。
pub fn normalize(raw: Vec<(i64, f64)>, start_ms: i64, end_ms: i64) -> Vec<Settlement> {
    let mut by_t: BTreeMap<i64, f64> = BTreeMap::new();
    for (ms, rate) in raw {
        let t = round_minute(ms);
        if rate.is_finite() && t >= start_ms - 60_000 && t <= end_ms + 60_000 {
            by_t.insert(t, rate);
        }
    }
    by_t.into_iter()
        .map(|(t, rate)| Settlement {
            t,
            rate,
            special: None,
            special_only: false,
        })
        .collect()
}

/// 把币安的分红资金费（按 `fundingTime` 原值去重过）并进结算序列：和常规结算落在同一分钟就加进那一次
/// （常规那条不能被它盖掉 —— 按分钟去重会只留一条），没有常规结算就单独成一点。窗口裁剪同 [`normalize`]。
pub fn merge_special(
    list: &mut Vec<Settlement>,
    special: &BTreeMap<i64, f64>,
    start_ms: i64,
    end_ms: i64,
) {
    for (&ms, &rate) in special {
        let t = round_minute(ms);
        if !rate.is_finite() || t < start_ms - 60_000 || t > end_ms + 60_000 {
            continue;
        }
        match list.binary_search_by_key(&t, |s| s.t) {
            Ok(i) => {
                let s = &mut list[i];
                s.rate += rate;
                s.special = Some(s.special.unwrap_or(0.0) + rate);
            }
            Err(i) => list.insert(
                i,
                Settlement {
                    t,
                    rate,
                    special: Some(rate),
                    special_only: true,
                },
            ),
        }
    }
}

/// 按结算间隔推周期（小时）：相邻结算间隔的最小值。少于两条推不出来。
pub fn infer_interval_h(settlements: &[Settlement]) -> Option<u32> {
    let regular: Vec<i64> = settlements
        .iter()
        .filter(|s| !s.special_only)
        .map(|s| s.t)
        .collect();
    let gap = regular
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|gap| *gap > 0)
        .min()?;
    let hours = (gap as f64 / 3_600_000.0).round();
    (hours >= 1.0).then_some(hours as u32)
}

/// 一个结算时刻的净资金费（多 `long`、空 `short`）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct NetPoint {
    pub t: i64,
    /// `both` = 两家同一分钟结算；`long` / `short` = 只有多腿 / 空腿结算。
    pub kind: &'static str,
    pub long_rate: Option<f64>,
    pub short_rate: Option<f64>,
    /// 空腿费率 − 多腿费率（正 = 这次结算收钱）。
    pub net: f64,
}

fn net_point(t: i64, long_rate: Option<f64>, short_rate: Option<f64>) -> NetPoint {
    let kind = match (long_rate, short_rate) {
        (Some(_), Some(_)) => "both",
        (Some(_), None) => "long",
        _ => "short",
    };
    NetPoint {
        t,
        kind,
        long_rate,
        short_rate,
        net: short_rate.unwrap_or(0.0) - long_rate.unwrap_or(0.0),
    }
}

/// 把两家的结算按时刻合并成净资金费序列（两边都已取整到分钟）。
pub fn net_settlements(long: &[Settlement], short: &[Settlement]) -> Vec<NetPoint> {
    let mut by_t: BTreeMap<i64, (Option<f64>, Option<f64>)> = BTreeMap::new();
    for s in long {
        by_t.entry(s.t).or_default().0 = Some(s.rate);
    }
    for s in short {
        by_t.entry(s.t).or_default().1 = Some(s.rate);
    }
    by_t.into_iter()
        .map(|(t, (l, s))| net_point(t, l, s))
        .collect()
}

/// 一家现在的费率（来自扫描快照）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Current {
    /// 下一次结算适用的费率（每周期小数）。
    pub rate: f64,
    /// 下一次结算时刻（毫秒）。
    pub next_ms: i64,
    /// 结算时刻是按周期推算的（场所没给）。
    pub next_estimated: bool,
    pub interval_h: u32,
    /// 周期是回落的默认值（场所没给）。
    pub interval_assumed: bool,
    /// 快照多少秒前拍的。
    pub age_sec: u64,
    /// 场所公开的吃单费率（小数）。
    pub taker_fee: Option<f64>,
    /// 24 小时成交额、持仓量（计价币名义）。批量接口不给就是 `None`。
    pub volume_24h: Option<f64>,
    pub open_interest: Option<f64>,
}

/// 下一次结算的净资金费：两家里先到的那个结算时刻；同一分钟就都算上。缺一家给不出。
pub fn next_net(long: Option<&Current>, short: Option<&Current>) -> Option<NetPoint> {
    let (long, short) = (long?, short?);
    let (tl, ts) = (round_minute(long.next_ms), round_minute(short.next_ms));
    let t = tl.min(ts);
    Some(net_point(
        t,
        (tl == t).then_some(long.rate),
        (ts == t).then_some(short.rate),
    ))
}

/// 所选范围的合计。
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Summary {
    /// 从哪一刻起算（两家都有数据的起点，见 [`overlap_start`]）。
    pub from_ms: i64,
    pub count: usize,
    pub both: usize,
    pub long_only: usize,
    pub short_only: usize,
    /// 净资金费合计（小数，相对单腿名义）。
    pub total: f64,
    pub days: f64,
    /// 按单利折算的年化（小数）。
    pub apr: f64,
}

pub fn summarize(net: &[NetPoint], from_ms: i64, end_ms: i64) -> Summary {
    let count_kind = |kind: &str| net.iter().filter(|p| p.kind == kind).count();
    let total: f64 = net.iter().map(|p| p.net).sum();
    let days = (end_ms - from_ms).max(0) as f64 / 86_400_000.0;
    Summary {
        from_ms,
        count: net.len(),
        both: count_kind("both"),
        long_only: count_kind("long"),
        short_only: count_kind("short"),
        total,
        days,
        apr: if days > 0.0 {
            total / days * 365.0
        } else {
            0.0
        },
    }
}

/// 净资金费从哪一刻起算：两家都有数据的那段。一家的数据从「第一次结算往前一个周期」开始算
/// （那次结算覆盖的就是这段）；在这之前另一家的结算不算，否则新上市的币、或者历史只给三个月的
/// OKX，会把另一家的结算全算成「只有一家结算」，合计和年化都偏。任一家一条结算都没有给 `None`。
pub fn overlap_start(
    window_start_ms: i64,
    long: &[Settlement],
    long_interval_h: Option<u32>,
    short: &[Settlement],
    short_interval_h: Option<u32>,
) -> Option<i64> {
    let covers_from = |list: &[Settlement], interval_h: Option<u32>| -> Option<i64> {
        let first = list.first()?.t;
        Some(first - i64::from(interval_h.unwrap_or(0)) * 3_600_000 + 60_000)
    };
    Some(
        window_start_ms
            .max(covers_from(long, long_interval_h)?)
            .max(covers_from(short, short_interval_h)?),
    )
}

/// 这段时间里有分红资金费就说一句（时刻、费率），没有给 `None`。
fn special_note(venue: Venue, list: &[Settlement]) -> Option<String> {
    let items: Vec<String> = list
        .iter()
        .filter_map(|s| {
            let rate = s.special?;
            let at = chrono::DateTime::from_timestamp_millis(s.t)?;
            Some(format!(
                "{} {:+.4}%",
                at.format("%m-%d %H:%M"),
                rate * 100.0
            ))
        })
        .collect();
    (!items.is_empty()).then(|| {
        format!(
            "{venue} 有 {} 次分红资金费（除息日在常规结算后 1 秒多再结一笔，负 = 空头付给多头，已并进当次结算和合计）：{}",
            items.len(),
            items.join("、")
        )
    })
}

// ───────────────────────────── 各家结算历史 ─────────────────────────────

/// 这一家这个合约在接口里的名字（展示用）。Hyperliquid 要按 meta 回查原始币名。
pub async fn inst_id(venue: Venue, base: &str) -> Result<String> {
    Ok(match venue {
        Venue::Binance | Venue::Bybit | Venue::Aster => format!("{base}USDT"),
        Venue::Okx => format!("{base}-USDT-SWAP"),
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            hl_coin(venue, base).await?
        }
        Venue::Arcus => format!("{base}-USD"),
        _ => base.to_string(),
    })
}

/// 拉 `venue` 上 `base` 在 `[start_ms, end_ms]` 内的已结算资金费，返回（接口里的合约名，结算序列）。
pub async fn fetch_settlements(
    venue: Venue,
    base: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<(String, Vec<Settlement>)> {
    let base = base.trim().to_ascii_uppercase();
    anyhow::ensure!(
        !base.is_empty() && base.chars().all(|c| c.is_ascii_alphanumeric()),
        "合约名只能是字母数字"
    );
    let inst = inst_id(venue, &base).await?;
    let mut raw: Vec<(i64, f64)> = Vec::new();
    // 币安分红资金费：按 fundingTime 原值去重（翻页重叠时同一条会来两次）。
    let mut special: BTreeMap<i64, f64> = BTreeMap::new();
    match venue {
        // Aster 是币安分叉：接口与格式一样（没有 `rateType`，都当普通结算）。
        Venue::Binance | Venue::Aster => {
            let host = crate::spread_chart::fapi_host(venue);
            // 升序，每页最多 1000 条。
            let mut from = start_ms;
            for _ in 0..MAX_PAGES {
                let rows = get(
                    format!(
                        "{host}/fapi/v1/fundingRate?symbol={inst}&startTime={from}&endTime={end_ms}&limit=1000"
                    ),
                    venue,
                )
                .await?;
                let page: Vec<(i64, f64, bool)> = rows
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some((
                                    x["fundingTime"].as_i64()?,
                                    num(&x["fundingRate"])?,
                                    x["rateType"].as_str() == Some("Special"),
                                ))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let full = page.len() >= 1000;
                let Some(last) = page.iter().map(|p| p.0).max() else {
                    break;
                };
                for (ms, rate, is_special) in page {
                    if is_special {
                        special.insert(ms, rate);
                    } else {
                        raw.push((ms, rate));
                    }
                }
                if !full {
                    break;
                }
                from = last + 1;
            }
        }
        Venue::Okx => {
            // 最新在前，每页 100 条；`after` = 早于这个时刻。用实际结算的 realizedRate。
            let mut after: Option<i64> = None;
            for page_no in 0..MAX_PAGES {
                if page_no > 0 {
                    tokio::time::sleep(PAGE_GAP).await;
                }
                let cursor = after.map(|a| format!("&after={a}")).unwrap_or_default();
                let v = get(
                    format!(
                        "https://www.okx.com/api/v5/public/funding-rate-history?instId={inst}&limit=100{cursor}"
                    ),
                    venue,
                )
                .await?;
                if v["code"].as_str() != Some("0") {
                    bail!(
                        "OKX 资金费历史：{}",
                        v["msg"].as_str().unwrap_or("未知错误")
                    );
                }
                let page: Vec<(i64, f64)> = v["data"]
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                let t = num(&x["fundingTime"])? as i64;
                                let rate =
                                    num(&x["realizedRate"]).or_else(|| num(&x["fundingRate"]))?;
                                Some((t, rate))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let full = page.len() >= 100;
                let Some(oldest) = page.iter().map(|p| p.0).min() else {
                    break;
                };
                raw.extend(page);
                if !full || oldest <= start_ms {
                    break;
                }
                after = Some(oldest);
            }
        }
        Venue::Bybit => {
            // 最新在前，每页最多 200 条。
            let mut to = end_ms;
            for _ in 0..MAX_PAGES {
                let v = get(
                    format!(
                        "https://api.bybit.com/v5/market/funding/history?category=linear&symbol={inst}&startTime={start_ms}&endTime={to}&limit=200"
                    ),
                    venue,
                )
                .await?;
                if v["retCode"].as_i64() != Some(0) {
                    bail!(
                        "Bybit 资金费历史：{}",
                        v["retMsg"].as_str().unwrap_or("未知错误")
                    );
                }
                let page: Vec<(i64, f64)> = v["result"]["list"]
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some((
                                    num(&x["fundingRateTimestamp"])? as i64,
                                    num(&x["fundingRate"])?,
                                ))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let full = page.len() >= 200;
                let Some(oldest) = page.iter().map(|p| p.0).min() else {
                    break;
                };
                raw.extend(page);
                if !full {
                    break;
                }
                to = oldest - 1;
            }
        }
        Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo => {
            // 升序，每次最多 500 条；费率已是小时费率（HL 每小时结算）。
            let mut from = start_ms;
            for _ in 0..MAX_PAGES {
                let rows = post(
                    "https://api.hyperliquid.xyz/info",
                    json!({"type": "fundingHistory", "coin": inst, "startTime": from, "endTime": end_ms}),
                    venue,
                )
                .await?;
                let page: Vec<(i64, f64)> = rows
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| Some((x["time"].as_i64()?, num(&x["fundingRate"])?)))
                            .collect()
                    })
                    .unwrap_or_default();
                let full = page.len() >= 500;
                let Some(last) = page.iter().map(|p| p.0).max() else {
                    break;
                };
                raw.extend(page);
                if !full {
                    break;
                }
                from = last + 1;
            }
        }
        Venue::Lighter | Venue::LighterRh => {
            // 每小时一条，每次最多给区间里最新的 750 条（升序）；`rate` 是百分数，`direction` = short 表示负。
            let (host, market) = lighter_market(venue, &base).await?;
            let start = start_ms / 1000;
            let mut to = end_ms / 1000;
            for page_no in 0..MAX_PAGES {
                if page_no > 0 {
                    tokio::time::sleep(LIGHTER_PAGE_GAP).await;
                }
                let v = get(
                    format!(
                        "{host}/api/v1/fundings?market_id={market}&resolution=1h&start_timestamp={start}&end_timestamp={to}&count_back=750"
                    ),
                    venue,
                )
                .await?;
                if v["code"].as_i64() != Some(200) {
                    bail!("{venue} 资金费历史 code={}", v["code"]);
                }
                let page: Vec<(i64, f64)> = v["fundings"]
                    .as_array()
                    .map(|r| r.iter().filter_map(lighter_row).collect())
                    .unwrap_or_default();
                let full = page.len() >= 750;
                let Some(oldest) = page.iter().map(|p| p.0).min() else {
                    break;
                };
                raw.extend(page);
                if !full || oldest / 1000 <= start {
                    break;
                }
                to = oldest / 1000 - 1;
            }
        }
        Venue::Arcus => {
            // 最新在前，每页最多 1000 条；`from` / `to` / `time` 都是微秒。费率是小时费率。
            let from = start_ms * 1000;
            let mut to = end_ms * 1000;
            for _ in 0..MAX_PAGES {
                let v = get(
                    format!(
                        "https://api.arcus.xyz/v1/fundingRates?market={inst}&from={from}&to={to}&limit=1000"
                    ),
                    venue,
                )
                .await?;
                if let Some(error) = v["error"].as_str() {
                    bail!("Arcus 资金费历史：{error}");
                }
                let page: Vec<(i64, f64)> = v["fundingRates"]
                    .as_array()
                    .map(|r| {
                        r.iter()
                            .filter_map(|x| {
                                Some((x["time"].as_i64()? / 1000, num(&x["fundingRate"])?))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let full = page.len() >= 1000;
                let Some(oldest) = page.iter().map(|p| p.0).min() else {
                    break;
                };
                raw.extend(page);
                if !full {
                    break;
                }
                to = oldest * 1000 - 1;
            }
        }
        other => bail!("{other} 暂不支持历史资金费"),
    }
    let mut list = normalize(raw, start_ms, end_ms);
    merge_special(&mut list, &special, start_ms, end_ms);
    Ok((inst, list))
}

/// Lighter `fundings` 的一行：`(毫秒, 费率小数)`。
fn lighter_row(x: &Value) -> Option<(i64, f64)> {
    let magnitude = num(&x["rate"])? / 100.0;
    let rate = match x["direction"].as_str()? {
        "long" => magnitude,
        "short" => -magnitude,
        _ => return None,
    };
    Some((x["timestamp"].as_i64()? * 1000, rate))
}

// ───────────────────────────── 缓存 ─────────────────────────────

/// （拉取时刻，覆盖的起点，合约名，结算序列）。
type Entry = (Instant, i64, String, Arc<Vec<Settlement>>);
type Cache = Mutex<HashMap<(Venue, String), Entry>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn fetch_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn cached(venue: Venue, base: &str, start_ms: i64) -> Option<(String, Arc<Vec<Settlement>>)> {
    let cache = cache().lock().await;
    let (at, covered, inst, list) = cache.get(&(venue, base.to_string()))?;
    (at.elapsed() < CACHE_TTL && *covered <= start_ms + 60_000)
        .then(|| (inst.clone(), Arc::clone(list)))
}

/// [`fetch_settlements`]，带缓存：缓存覆盖了更早的起点就直接裁出来。
pub async fn settlements(
    venue: Venue,
    base: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<(String, Vec<Settlement>)> {
    let clip = |list: &[Settlement]| -> Vec<Settlement> {
        list.iter()
            .copied()
            .filter(|s| s.t >= start_ms - 60_000)
            .collect()
    };
    if let Some((inst, list)) = cached(venue, base, start_ms).await {
        return Ok((inst, clip(&list)));
    }
    let _guard = fetch_lock().lock().await;
    if let Some((inst, list)) = cached(venue, base, start_ms).await {
        return Ok((inst, clip(&list)));
    }
    let (inst, list) = fetch_settlements(venue, base, start_ms, end_ms).await?;
    let list = Arc::new(list);
    let mut cache = cache().lock().await;
    cache.retain(|_, (at, ..)| at.elapsed() < CACHE_TTL);
    cache.insert(
        (venue, base.to_string()),
        (Instant::now(), start_ms, inst.clone(), Arc::clone(&list)),
    );
    Ok((inst, clip(&list)))
}

// ───────────────────────────── 接口 ─────────────────────────────

/// 扫描快照里这一家这个合约现在的费率。
async fn current(state: &AppState, venue: Venue, base: &str) -> Option<Current> {
    let snapshot = state.cache.get().await?;
    // 同一个 base 可能分在几组里（计价资产不同）：在所有同名组里找这一家。
    let r = snapshot
        .report
        .symbols
        .iter()
        .filter(|s| s.symbol.base.eq_ignore_ascii_case(base))
        .flat_map(|s| s.rates.iter())
        .find(|r| r.venue == venue)?;
    Some(Current {
        rate: r.period_rate.to_f64()?,
        next_ms: r.next_funding_at.timestamp_millis(),
        next_estimated: r.next_funding_estimated,
        interval_h: r.interval_h,
        interval_assumed: r.interval_assumed,
        age_sec: snapshot.age().as_secs(),
        taker_fee: r.taker_fee.and_then(|f| f.to_f64()),
        volume_24h: r.quote_volume_24h.and_then(|v| v.to_f64()),
        open_interest: r.open_interest_usdt.and_then(|v| v.to_f64()),
    })
}

#[derive(Debug, Deserialize)]
pub struct FundingQuery {
    symbol: String,
    exchange1: String,
    exchange2: String,
    #[serde(default)]
    days: Option<f64>,
    /// `long1_short2`（默认，1 多 2 空）或 `short1_long2`。
    #[serde(default)]
    direction: Option<String>,
}

/// `GET /api/spread-chart/funding?symbol=GIGADEV&exchange1=binance&exchange2=hyperliquid-xyz&days=3`：只读。
pub async fn api_funding(
    State(state): State<Arc<AppState>>,
    Query(q): Query<FundingQuery>,
) -> Response {
    let fail = |status: StatusCode, message: String| {
        (status, Json(json!({ "error": message }))).into_response()
    };
    let parse = |raw: &str| Venue::parse(raw).ok_or_else(|| format!("未知场所 {raw:?}"));
    let (mut long, mut short) = match (parse(&q.exchange1), parse(&q.exchange2)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return fail(StatusCode::BAD_REQUEST, e),
    };
    match q.direction.as_deref().unwrap_or("long1_short2") {
        "long1_short2" => {}
        "short1_long2" => std::mem::swap(&mut long, &mut short),
        other => {
            return fail(
                StatusCode::BAD_REQUEST,
                format!("direction 只能是 long1_short2 / short1_long2，收到 {other:?}"),
            );
        }
    }
    if long == short {
        return fail(StatusCode::BAD_REQUEST, "两边不能是同一家".into());
    }
    for venue in [long, short] {
        if !VENUES.contains(&venue) {
            return fail(
                StatusCode::BAD_REQUEST,
                format!("{venue} 暂不支持历史资金费"),
            );
        }
    }
    let base = q.symbol.trim().to_ascii_uppercase();
    if base.is_empty() || !base.chars().all(|c| c.is_ascii_alphanumeric()) {
        return fail(StatusCode::BAD_REQUEST, "合约名只能是字母数字".into());
    }
    let days = q.days.unwrap_or(3.0).clamp(1.0 / 24.0, MAX_DAYS);
    let end_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = end_ms - (days * 86_400_000.0) as i64;
    let (hist_long, hist_short) = tokio::join!(
        settlements(long, &base, start_ms, end_ms),
        settlements(short, &base, start_ms, end_ms)
    );
    let (cur_long, cur_short) = (
        current(&state, long, &base).await,
        current(&state, short, &base).await,
    );
    let mut notes: Vec<String> = Vec::new();
    if [long, short]
        .iter()
        .any(|v| matches!(v, Venue::Lighter | Venue::LighterRh))
    {
        notes.push("Lighter 的历史费率只精确到 0.0001%（每小时）".into());
    }
    for (venue, hist) in [(long, &hist_long), (short, &hist_short)] {
        if let Some(note) = hist.as_ref().ok().and_then(|(_, l)| special_note(venue, l)) {
            notes.push(note);
        }
    }
    let list = |hist: &Result<(String, Vec<Settlement>)>| -> Vec<Settlement> {
        hist.as_ref().map(|(_, l)| l.clone()).unwrap_or_default()
    };
    // 周期：快照给的（不是回落默认值）优先，其次按结算间隔推，最后才用回落值。
    let interval = |list: &[Settlement], cur: Option<Current>| -> Option<u32> {
        cur.filter(|c| !c.interval_assumed)
            .map(|c| c.interval_h)
            .or_else(|| infer_interval_h(list))
            .or(cur.map(|c| c.interval_h))
    };
    let (list_long, list_short) = (list(&hist_long), list(&hist_short));
    let (h_long, h_short) = (
        interval(&list_long, cur_long),
        interval(&list_short, cur_short),
    );
    let leg = |venue: Venue,
               hist: &Result<(String, Vec<Settlement>)>,
               cur: Option<Current>,
               interval_h: Option<u32>| {
        let (inst, list, error) = match hist {
            Ok((inst, list)) => (Some(inst.clone()), list.as_slice(), None),
            Err(e) => (None, &[][..], Some(format!("{e:#}"))),
        };
        json!({
            "venue": venue,
            "inst_id": inst,
            "settlements": list,
            "current": cur,
            "interval_h": interval_h,
            "error": error,
        })
    };
    let from = overlap_start(start_ms, &list_long, h_long, &list_short, h_short);
    let (net, summary) = match (&hist_long, &hist_short, from) {
        (Ok(_), Ok(_), Some(from)) => {
            let net: Vec<NetPoint> = net_settlements(&list_long, &list_short)
                .into_iter()
                .filter(|p| p.t >= from && p.t <= end_ms + 60_000)
                .collect();
            let summary = summarize(&net, from, end_ms);
            (net, Some(summary))
        }
        (Ok(_), Ok(_), None) => {
            notes.push("有一家在所选范围内没有结算记录（上新不久？），净资金费不算".into());
            (Vec::new(), None)
        }
        _ => {
            notes.push("有一家的资金费历史没拉到，净资金费不算（只算一家会误导）".into());
            (Vec::new(), None)
        }
    };
    Json(json!({
        "base": base,
        "long": long,
        "short": short,
        "days": days,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "now_ms": end_ms,
        "legs": [
            leg(long, &hist_long, cur_long, h_long),
            leg(short, &hist_short, cur_short, h_short),
        ],
        "net": net,
        "next": next_net(cur_long.as_ref(), cur_short.as_ref()),
        "summary": summary,
        "notes": notes,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t: i64, rate: f64) -> Settlement {
        Settlement {
            t,
            rate,
            special: None,
            special_only: false,
        }
    }

    #[test]
    fn binance_dividend_funding_adds_to_the_regular_settlement_instead_of_replacing_it() {
        let h = 3_600_000;
        // 某次实盘：00:00:00.001 常规 0.0001，00:00:01.002 分红 −0.0143；04:00 只有常规；
        // 另有一笔分红落在没有常规结算的 01:00:01。
        let mut list = normalize(
            vec![(1, 0.0001), (4 * h + 1, 0.0002), (8 * h + 1, 0.0)],
            0,
            10 * h,
        );
        let special: BTreeMap<i64, f64> = [(1_002, -0.0143), (h + 1_002, -0.003), (99 * h, -1.0)]
            .into_iter()
            .collect();
        merge_special(&mut list, &special, 0, 10 * h);
        assert_eq!(list.len(), 4, "窗口外那笔不算");
        assert!((list[0].rate - (0.0001 - 0.0143)).abs() < 1e-12);
        assert_eq!(
            (list[0].t, list[0].special, list[0].special_only),
            (0, Some(-0.0143), false)
        );
        assert_eq!(
            (list[1].t, list[1].special, list[1].special_only),
            (h, Some(-0.003), true)
        );
        assert_eq!((list[2].rate, list[2].special), (0.0002, None));
        assert_eq!(
            infer_interval_h(&list),
            Some(4),
            "只有分红的那一点不能把周期推成 1 小时"
        );
        let net = net_settlements(&[], &list);
        let total: f64 = net.iter().map(|p| p.net).sum();
        assert!(
            (total - (0.0001 - 0.0143 - 0.003 + 0.0002)).abs() < 1e-12,
            "空腿合计含分红"
        );
        let note = special_note(Venue::Binance, &list).unwrap();
        assert!(note.contains("2 次") && note.contains("-1.4300%"), "{note}");
        assert!(special_note(Venue::Binance, &list[2..]).is_none());
        let json = serde_json::to_value(list[2]).unwrap();
        assert!(json.get("special").is_none() && json.get("special_only").is_none());
    }

    #[test]
    fn settlement_times_round_to_the_minute() {
        // Binance 带 +1ms，HL 带 +63ms：都落到整点。
        assert_eq!(round_minute(1_791_374_400_001), 1_791_374_400_000);
        assert_eq!(round_minute(1_791_363_600_063), 1_791_363_600_000);
        assert_eq!(round_minute(1_791_363_599_990), 1_791_363_600_000);
        let n = normalize(
            vec![
                (3_600_063, 0.2),
                (1, 0.1),
                (3_600_001, 0.3),
                (99_999_999, 9.0),
            ],
            0,
            10_000_000,
        );
        assert_eq!(
            n,
            vec![s(0, 0.1), s(3_600_000, 0.3)],
            "同一分钟留后到的，超出范围的丢掉"
        );
    }

    #[test]
    fn net_matches_the_reference_site_sign_and_kinds() {
        // zuudot 的 GIGADEV 例子：Binance 4h（多）、HL 1h（空）。leg2_only、HL −0.000632 → 净 −0.000632。
        let h = 3_600_000;
        let binance = [s(0, 0.0003), s(4 * h, 0.0)];
        let hl: Vec<Settlement> = (0..=4)
            .map(|i| s(i * h, if i == 1 { -0.000632 } else { 0.0001 }))
            .collect();
        let net = net_settlements(&binance, &hl);
        assert_eq!(net.len(), 5);
        assert_eq!(net[0].kind, "both");
        assert!((net[0].net - (0.0001 - 0.0003)).abs() < 1e-12);
        assert_eq!(net[1].kind, "short");
        assert!((net[1].net + 0.000632).abs() < 1e-12);
        assert_eq!(
            (net[1].long_rate, net[1].short_rate),
            (None, Some(-0.000632))
        );
        let only_long = net_settlements(&[s(0, 0.001)], &[]);
        assert_eq!(only_long[0].kind, "long");
        assert!(
            (only_long[0].net + 0.001).abs() < 1e-12,
            "多腿结算正费率 = 付钱"
        );
        let sum = summarize(&net, 0, 86_400_000);
        assert!((sum.days - 1.0).abs() < 1e-12);
        assert_eq!(
            (sum.count, sum.both, sum.long_only, sum.short_only),
            (5, 2, 0, 3)
        );
        let expected: f64 = net.iter().map(|p| p.net).sum();
        assert!((sum.total - expected).abs() < 1e-12);
        assert!((sum.apr - expected * 365.0).abs() < 1e-9);
    }

    #[test]
    fn next_settlement_takes_the_earlier_leg_or_both() {
        let cur = |rate: f64, next_ms: i64| Current {
            rate,
            next_ms,
            next_estimated: false,
            interval_h: 1,
            interval_assumed: false,
            age_sec: 3,
            taker_fee: None,
            volume_24h: None,
            open_interest: None,
        };
        let (a, b) = (cur(0.0, 14_400_001), cur(-0.000053, 3_600_050));
        let n = next_net(Some(&a), Some(&b)).unwrap();
        assert_eq!((n.t, n.kind), (3_600_000, "short"));
        assert!((n.net + 0.000053).abs() < 1e-12);
        let both = next_net(Some(&cur(0.0001, 3_600_000)), Some(&b)).unwrap();
        assert_eq!(both.kind, "both");
        assert!((both.net - (-0.000053 - 0.0001)).abs() < 1e-12);
        assert!(next_net(None, Some(&b)).is_none());
    }

    #[test]
    fn net_only_counts_the_span_both_legs_have_data_for() {
        let h = 3_600_000;
        // 窗口从 0 起；HL 每小时都有，Binance（4h）第 10 天才上市。
        let hl: Vec<Settlement> = (1..=24 * 12).map(|i| s(i * h, -0.0001)).collect();
        let late: Vec<Settlement> = (60..=72).map(|i| s(i * 4 * h, 0.0002)).collect();
        let from = overlap_start(0, &late, Some(4), &hl, Some(1)).unwrap();
        assert_eq!(
            from,
            236 * h + 60_000,
            "Binance 第一次结算（240h）往前一个周期"
        );
        let net: Vec<NetPoint> = net_settlements(&late, &hl)
            .into_iter()
            .filter(|p| p.t >= from)
            .collect();
        assert_eq!(
            net.first().unwrap().t,
            237 * h,
            "上市前那段 HL 的结算不算成「只有一家」"
        );
        let sum = summarize(&net, from, 288 * h);
        assert_eq!((sum.both, sum.long_only), (13, 0));
        assert!((sum.days - (288.0 - 236.0 - 1.0 / 60.0) / 24.0).abs() < 1e-9);
        // 两家都从窗口开始就有：起点就是窗口开头（第一次结算往前一个周期，再加一分钟）。
        let early: Vec<Settlement> = (1..=72).map(|i| s(i * 4 * h, 0.0)).collect();
        assert_eq!(
            overlap_start(0, &early, Some(4), &hl, Some(1)),
            Some(60_000)
        );
        assert_eq!(
            overlap_start(-h, &early, Some(4), &hl, Some(1)),
            Some(60_000),
            "窗口更早也不早于数据"
        );
        let from_start: Vec<Settlement> = (0..=72).map(|i| s(i * 4 * h, 0.0)).collect();
        let hl0: Vec<Settlement> = (0..=24).map(|i| s(i * h, 0.0)).collect();
        assert_eq!(
            overlap_start(0, &from_start, Some(4), &hl0, Some(1)),
            Some(0)
        );
        assert_eq!(overlap_start(0, &[], Some(4), &hl, Some(1)), None);
    }

    #[test]
    fn interval_is_inferred_from_settlement_spacing() {
        let h = 3_600_000;
        assert_eq!(
            infer_interval_h(&[s(0, 0.0), s(4 * h, 0.0), s(8 * h, 0.0)]),
            Some(4)
        );
        assert_eq!(
            infer_interval_h(&[s(0, 0.0), s(h, 0.0), s(3 * h, 0.0)]),
            Some(1)
        );
        assert_eq!(infer_interval_h(&[s(0, 0.0)]), None);
    }

    #[test]
    fn lighter_rows_are_percent_with_direction() {
        let row = json!({"timestamp": 1_791_619_200, "rate": "0.0012", "direction": "short"});
        let (t, rate) = lighter_row(&row).unwrap();
        assert_eq!(t, 1_791_619_200_000);
        assert!((rate + 0.000012).abs() < 1e-15);
        assert!(lighter_row(&json!({"timestamp": 1, "rate": "1", "direction": "?"})).is_none());
    }

    /// 联网核对：`cargo test -p arb-web --bin arb-web funding_chart::tests::live -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "联网"]
    async fn live() {
        let end = chrono::Utc::now().timestamp_millis();
        let start = end - 3 * 86_400_000;
        for (venue, base) in [
            (Venue::Binance, "GIGADEV"),
            (Venue::HyperliquidXyz, "GIGADEV"),
            (Venue::Okx, "BTC"),
            (Venue::Bybit, "BTC"),
            (Venue::Hyperliquid, "BTC"),
            (Venue::LighterRh, "AAPL"),
            (Venue::Lighter, "BTC"),
            (Venue::Arcus, "SPY"),
        ] {
            match fetch_settlements(venue, base, start, end).await {
                Ok((inst, list)) => println!(
                    "{venue} {inst}: {} 条，周期 {:?}h，最新 {:?}",
                    list.len(),
                    infer_interval_h(&list),
                    list.last()
                ),
                Err(e) => println!("{venue} {base}: 失败 {e:#}"),
            }
        }
    }
}
