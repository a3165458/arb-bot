//! 看板上的交易：纸面与实盘两张交易台。
//!
//! 流程与命令行是**同一份代码**（[`arb_exec::desk`]）：扫描、规则与强平校验、深度体检、
//! 闸门、双腿执行与回滚、对账。这里只负责三件事：鉴权、把 HTTP 参数翻译成请求、
//! 以及保证同一张交易台同一时刻只跑一笔操作。
//!
//! # 安全边界
//!
//! - **所有写操作都要令牌**（`ARB_WEB_TOKEN`，`Authorization: Bearer …`）。没配令牌时写接口
//!   一律 403；实盘账户的状态（真实持仓）同样要令牌。请求体只收 JSON：浏览器表单发不出
//!   `application/json`，也带不上 `Authorization` 头，跨站请求伪造无从下手。
//! - **实盘默认关闭**（`ARB_WEB_LIVE=off`）。`readonly` 只连接、对账、出计划；`trade` 才会
//!   签名发单，并且必须给 `ARB_WEB_MARKET_SLIPPAGE`。连接失败（包括另一个进程已经持有
//!   订单意图日志的锁）时看板直接启动失败，不悄悄退回只读。
//! - **实盘开仓不用缓存快照**：先对账（必须干净），再对实盘场所现扫一轮。风险、持仓量上限、
//!   最大杠杆与入场基差都来自扫描，不来自盘口，陈旧 60 秒就可能把杠杆上限算错。
//! - **实盘下单要二次确认**：开仓的 `confirm` 必须等于合约 base，平仓的 `confirm` 必须等于
//!   仓位 id。服务端校验，不只靠页面。
//! - **不排队、不重试**：同一张交易台上已有操作在跑时直接返回 409，避免双击开出两笔；
//!   执行出错时返回错误与台账状态，由人看对账结果决定，页面不会自动重发。
//! - 实盘的持仓规则由看板进程执行（后台一轮一轮地跑，`ARB_WEB_LIVE_WATCH_SEC`）：券商持有
//!   订单意图日志的独占锁，看板连着实盘时 `arb-live watch` 连不上，规则只能在这里跑。

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::pause::{self, PauseState};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::alert::Alerter;
use anyhow::{Context, Result, bail};
use arb_core::{Decimal, MarketSnapshot, Settings, Symbol, Venue, money::parse_decimal};
use arb_exec::broker::Broker;
use arb_exec::cli::limits_from;
use arb_exec::desk::{self, ExternalNote, LeveragePolicy, MonitorReport, OpenRequest};
use arb_exec::live_connect::{self, ConnectOptions};
use arb_exec::{
    Divergence, Executor, FundingTotal, Ledger, PairPosition, PositionStatus, Reconciliation,
    TaskRules,
};
use arb_scanner::ScanReport;
use arb_venues::{VenueApi, build_all};
use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::{error, info, warn};

use crate::AppState;

/// 盘口请求多少档。与命令行默认值一致。
const DEPTH_LEVELS: u32 = 20;
/// 令牌最短长度。短令牌在本机上也能被暴力试出来。
const MIN_TOKEN_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LiveMode {
    Off,
    /// 连接、对账、出计划；不签名、不发单。
    Readonly,
    /// 允许真实下单。
    Trade,
}

impl LiveMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Readonly => "readonly",
            Self::Trade => "trade",
        }
    }

    fn from_env() -> Result<Self> {
        let raw = std::env::var("ARB_WEB_LIVE").unwrap_or_default();
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "0" | "false" => Ok(Self::Off),
            "readonly" | "read-only" | "ro" => Ok(Self::Readonly),
            "trade" | "on" | "1" | "true" => Ok(Self::Trade),
            other => bail!("ARB_WEB_LIVE 只能是 off / readonly / trade，收到 {other:?}"),
        }
    }
}

/// 一轮监控的摘要：页面展示「规则上一次什么时候跑的、做了什么」。
#[derive(Debug, Clone, Serialize)]
pub struct RoundSummary {
    pub at: DateTime<Utc>,
    /// `auto`（后台）或 `manual`（页面按钮）。
    pub trigger: &'static str,
    /// 实盘：本轮对账是否干净。不干净时只冻结出问题的币（见 `frozen`），其余照常。
    pub reconciliation_clean: Option<bool>,
    pub divergences: Vec<Divergence>,
    /// 对账不一致时冻结了什么：出问题的币、没核对的场所，或「全部」（归不到哪个币时）。
    pub frozen: Vec<String>,
    /// 本轮对账发现的、不是看板平掉的仓位：等第二次确认，或只对上一部分（裸敞口）。
    pub external: Vec<ExternalNote>,
    /// 本轮在台账里结束的、已在交易所外部平掉的仓位 id。
    pub adopted: Vec<String>,
    pub reports: Vec<MonitorReport>,
    pub error: Option<String>,
}

impl RoundSummary {
    fn new(trigger: &'static str) -> Self {
        Self {
            at: Utc::now(),
            trigger,
            reconciliation_clean: None,
            divergences: Vec::new(),
            frozen: Vec::new(),
            external: Vec::new(),
            adopted: Vec::new(),
            reports: Vec::new(),
            error: None,
        }
    }
}

/// 纸面交易台。券商是 `PaperBroker`：真实盘口、纸面成交，不碰资金。
pub struct PaperDesk {
    ledger_path: String,
    /// 第一次交易时才打开台账：只看不做的看板不该创建或修补台账文件。
    ledger: OnceCell<Arc<Ledger>>,
    by_venue: HashMap<Venue, Arc<dyn VenueApi>>,
    fee_per_side: Decimal,
    lock: Mutex<()>,
    last_round: RwLock<Option<RoundSummary>>,
    watch_sec: u64,
    /// 补保证金的冷却与退避（见 [`desk::ExitRetries`]）。
    retries: desk::ExitRetries,
}

impl PaperDesk {
    async fn ledger(&self) -> Result<Arc<Ledger>> {
        let ledger = self
            .ledger
            .get_or_try_init(|| async {
                Ledger::open(&self.ledger_path)
                    .await
                    .map(Arc::new)
                    .with_context(|| format!("打开纸面台账 {} 失败", self.ledger_path))
            })
            .await?;
        Ok(Arc::clone(ledger))
    }

    /// 用台账重建的纸面账户 + 快照里的真实费率，组装执行器。
    async fn executor(
        &self,
        report: &ScanReport,
    ) -> Result<(Arc<Ledger>, Vec<PairPosition>, Executor)> {
        let ledger = self.ledger().await?;
        let (replayed, _) = ledger.replay().await?;
        let existing: Vec<PairPosition> = replayed.exposed().into_iter().cloned().collect();
        let snapshots: Vec<MarketSnapshot> = report
            .symbols
            .iter()
            .flat_map(|view| view.rates.iter().cloned())
            .collect();
        let (executor, _) = desk::paper_executor(
            &self.by_venue,
            self.fee_per_side,
            DEPTH_LEVELS,
            &ledger,
            &snapshots,
            &existing,
        );
        Ok((ledger, existing, executor))
    }

    async fn round(&self, trigger: &'static str) -> RoundSummary {
        let mut summary = RoundSummary::new(trigger);
        match self.ledger().await {
            Ok(ledger) => {
                let ctx = desk::RoundCtx {
                    funding: &desk::PaperFunding,
                    retries: &self.retries,
                    exit_confirm_gap: desk::EXIT_CONFIRM_GAP,
                };
                match desk::paper_round(
                    &self.by_venue,
                    self.fee_per_side,
                    DEPTH_LEVELS,
                    &ledger,
                    &ctx,
                )
                .await
                {
                    Ok(reports) => summary.reports = reports,
                    Err(error) => summary.error = Some(format!("{error:#}")),
                }
            }
            Err(error) => summary.error = Some(format!("{error:#}")),
        }
        *self.last_round.write().await = Some(summary.clone());
        summary
    }
}

/// 实盘交易台。
pub struct LiveDesk {
    /// 自动交易提前做好的下单前准备（见 [`prearm_for_auto`]）：下单时取走一次。
    armed: std::sync::Mutex<Option<Armed>>,
    mode: LiveMode,
    venues: Vec<Venue>,
    /// 场所是按凭据自动识别的（`ARB_LIVE_VENUES` 留空或 `auto`）。
    auto_venues: bool,
    /// 场所白名单换成实盘场所的配置：开仓前的现扫只打这几家。
    settings: Settings,
    apis: Vec<Arc<dyn VenueApi>>,
    by_venue: HashMap<Venue, Arc<dyn VenueApi>>,
    brokers: HashMap<Venue, Arc<dyn Broker>>,
    ledger: Arc<Ledger>,
    market_slippage: Option<Decimal>,
    lock: Mutex<()>,
    last_round: RwLock<Option<RoundSummary>>,
    /// 最近一次对账（预览、下单、平仓、刷新账户、规则轮都会对账）。只读模式下后台不跑
    /// 规则轮，`last_round` 一直是空的，所以单独记一份给后台预检看。
    last_reconcile: RwLock<Option<ReconcileMark>>,
    /// 上一轮对账不一致的「指纹」：同样的不一致每一轮都 WARN 会把日志灌满，变了才出声。
    divergence_sig: std::sync::Mutex<String>,
    /// 没走完的退出的退避状态（见 [`desk::ExitRetries`]）。
    exit_retries: desk::ExitRetries,
    /// 只剩一条腿的仓位从什么时候起被看到（见 [`naked_alerts`]）。只放内存，重启后重新计时。
    naked_since: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    /// 外部平仓的实际盈亏还没核出来的仓位：已经试过几次。连试 [`PNL_ATTEMPTS`] 次都不行就
    /// 把原因记进台账放弃。只放内存，重启后重新计数。
    pnl_attempts: std::sync::Mutex<HashMap<String, u32>>,
    /// 估算手续费已经核过、不用再核的订单（见 [`desk::correct_estimated_fees`]）。只放内存。
    fee_checked: std::sync::Mutex<std::collections::HashSet<String>>,
    /// 告警通知（没配置时是空操作）。
    alerts: Arc<Alerter>,
    /// 暂停开新仓（Telegram 里 /pause）。只拦网页的实盘下单；平仓、规则轮、对账不受影响 ——
    /// 暂停的是「加风险」，不能连「减风险」一起停。持久化在 `pause_file`：重启后仍然暂停。
    opens_paused: std::sync::atomic::AtomicBool,
    /// 暂停状态文件（`<台账>.pause`）与它的内存副本。
    pause_file: PathBuf,
    pause_state: std::sync::Mutex<PauseState>,
    /// 连续几轮对账不干净了；够多才告警（偶尔一轮抽风不值得响铃）。
    dirty_rounds: std::sync::atomic::AtomicU32,
    /// 对账不干净的告警已经发过（恢复时要发「已恢复」）。
    dirty_alerted: std::sync::atomic::AtomicBool,
    /// 各条腿在交易所的实际保证金状态缓存：持仓页 30 秒刷一次，Lighter RH 这类限频紧的场所经不起
    /// 每次都查。只缓存 [`LEG_STATE_TTL`]。
    leg_state_cache: Mutex<HashMap<(Venue, String), LegStateEntry>>,
    /// 最近一轮规则轮之后还开着的双腿持仓，交给分批平仓测算（[`crate::rh_spread::exit_shadow`]）。
    exit_watched: std::sync::RwLock<Arc<Vec<crate::rh_spread::exit_shadow::Watched>>>,
    /// 每笔「交易所里已经没有仓位」的候选第一次被发现的时刻：连续两次对账（间隔 ≥ 60 秒）
    /// 都如此才在台账里结束它。只放内存，重启后重新计时。
    external_seen: std::sync::Mutex<HashMap<String, Instant>>,
    /// 台账里没终态的补仓单第一次被看到的时刻：满 [`desk::ADD_SETTLE_AFTER`] 才去核实（见
    /// [`desk::settle_stale_adds`]）。只放内存，重启后重新计时（重启时的启动核实本来就会先读一遍）。
    add_seen: std::sync::Mutex<HashMap<String, Instant>>,
    /// 各条腿的资金费流水合计，按（场所, 合约, 起点）缓存一会儿：持仓页 30 秒刷新一次，
    /// Lighter RH 这类限频紧的场所经不起每次都查。
    funding_cache: Mutex<HashMap<FundingKey, (Instant, FundingResult)>>,
    watch_sec: u64,
    /// 看板后台扫描记下的各场所原始读数（见 [`LiveDesk::scan_for`]）。
    rates: Arc<arb_scanner::RatesCache>,
}

/// 一次对账的结论：什么时候、几处不一致（或为什么没做成）。
#[derive(Debug, Clone)]
struct ReconcileMark {
    at: DateTime<Utc>,
    outcome: Result<usize, String>,
    /// 不一致时冻结了什么（见 [`Reconciliation::frozen_scope`]）。
    frozen: Vec<String>,
}

/// 实盘规则轮的健康状态。
#[derive(Debug, Clone, Serialize)]
pub struct LiveHealth {
    pub mode: LiveMode,
    pub watch_sec: u64,
    /// 上一轮规则跑完多少秒了。还没跑过是 `None`。
    pub last_round_age_s: Option<i64>,
    pub reconciliation_clean: Option<bool>,
    /// 连续几轮对账不一致。
    pub dirty_rounds: u32,
    /// 上一轮对账不一致时冻结了什么（只冻结出问题的币，其余照常）。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub frozen: Vec<String>,
    /// 规则轮停了（过了三个周期还没跑）。
    pub stalled: bool,
    /// 开新仓被暂停了。
    pub opens_paused: bool,
    /// 实盘开着但账户暂时没连上（交易所维护、限频）：下单、规则、对账都暂停。
    pub disconnected: bool,
}

/// 只剩一条腿超过这么久才告警：开仓补第二腿、平仓重试一两轮就好的不打扰。
const NAKED_ALERT_AFTER: std::time::Duration = std::time::Duration::from_secs(10 * 60);
/// 之后每隔这么久再推一次（带上已经裸了多久），直到这条腿平掉或补上。
const NAKED_ALERT_EVERY: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// 有敞口、却只剩一条腿的仓位（平仓一条腿平不掉、开仓第二腿没补上）：裸了多久、要不要推送。
///
/// 某次实盘：Bitget 腿的平仓单被拒 15 次、退避到 30 分钟一次，Lighter 腿早就平了，Bitget 多腿裸了五个多小时；
/// 某次实盘 的 Lighter 空腿裸了近两小时。每次重试失败都有一条「规则执行失败」，但没有一条说「已经裸了多久、
/// 多大」。这里按仓位记下第一次看到它只剩一条腿的时刻，满 [`NAKED_ALERT_AFTER`] 推一次，之后每
/// [`NAKED_ALERT_EVERY`] 再推一次（键里带上第几个周期，不受同键冷却影响）。返回 (告警键, 文本)；`since` 里不再
/// 裸着的仓位被清掉。
fn naked_alerts(
    exposed: &[&arb_exec::PairPosition],
    since: &mut HashMap<String, std::time::Instant>,
    now: std::time::Instant,
) -> Vec<(String, String)> {
    let mut alerts = Vec::new();
    let mut naked = Vec::new();
    for position in exposed {
        let leg = match (&position.long, &position.short) {
            (Some(leg), None) | (None, Some(leg)) => leg,
            _ => continue,
        };
        naked.push(position.id.clone());
        let started = *since.entry(position.id.clone()).or_insert(now);
        let elapsed = now.saturating_duration_since(started);
        if elapsed < NAKED_ALERT_AFTER {
            continue;
        }
        let round = (elapsed - NAKED_ALERT_AFTER).as_secs() / NAKED_ALERT_EVERY.as_secs();
        alerts.push((
            format!("naked:{}:{round}", position.id),
            format!(
                "🚨 仓位 {} {} 只剩 {} {} 一条腿，已经裸了 {} 分钟（约 {} USDT，{}）：{}。请检查，必要时手动平掉这条腿",
                position.id,
                position.symbol,
                leg.venue,
                match leg.side {
                    arb_core::Side::Buy => "多",
                    arb_core::Side::Sell => "空",
                },
                elapsed.as_secs() / 60,
                leg.notional_usdt.round_dp(2).normalize(),
                match position.status {
                    arb_exec::PositionStatus::Opening => "建仓中，第二腿还没补上",
                    arb_exec::PositionStatus::Closing => "平仓中，另一条腿已平",
                    arb_exec::PositionStatus::Unwinding => "回滚中",
                    _ => "持仓中",
                },
                position.note.as_deref().unwrap_or("没有备注")
            ),
        ));
    }
    since.retain(|id, _| naked.contains(id));
    alerts
}

/// 每个刷新周期最多核几张估算手续费的订单：每张要读一次订单状态和成交，别和规则轮抢 Hyperliquid 的限频。
const FEE_CHECKS_PER_ROUND: usize = 6;

/// 外部平仓的实际盈亏最多试几次（每次隔一个刷新周期，约 5 分钟）：成交记录可能要过几分钟
/// 才出现，也可能永远对不上，总不能一直试下去。
const PNL_ATTEMPTS: u32 = 6;

/// 资金费流水缓存的键：（场所, 合约, 完整起点）；同一秒内重开也不能串流水。
type FundingKey = (Venue, String, DateTime<Utc>);
/// 查到的合计（场所没接入为 `None`），或者没查成的原因。
type FundingResult = Result<Option<FundingTotal>, String>;

/// 资金费流水缓存多久。结算是按小时的；后台每隔这么久为实盘持仓刷新一次，持仓页
/// 打开时读缓存，不必等。Lighter 查一次要先拉市场表，RH 部署限频紧，别查得太勤。
const FUNDING_CACHE_TTL: Duration = Duration::from_secs(300);
/// 每轮最多核对几笔已平仓仓位的资金费：每笔每条腿要查两次流水，Lighter RH 按出口 IP 限频极紧。
const FUNDING_RECHECKS_PER_ROUND: usize = 2;

/// 一条腿开仓以来实际收付的资金费。
#[derive(Debug, Clone, Serialize)]
pub struct LegFunding {
    pub venue: Venue,
    /// 合计（正 = 收到）。场所没接入流水、或者没查成时为 `None`。
    pub usdt: Option<Decimal>,
    pub payments: usize,
    pub last_at: Option<DateTime<Utc>>,
    /// 没查成的原因，或者「这家没接入资金费流水」。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// 一笔仓位开仓以来两条腿实际收付的资金费（交易所结算流水）。
#[derive(Debug, Clone, Serialize)]
pub struct PositionFunding {
    pub long: Option<LegFunding>,
    pub short: Option<LegFunding>,
    /// 两条腿都查到时的合计。
    pub total_usdt: Option<Decimal>,
    pub since: DateTime<Utc>,
}

/// 缓存里的一条：查询时刻与结果（查不到是 `None`）。
type LegStateEntry = (Instant, Option<arb_exec::VenueLegState>);

/// 交易所保证金状态缓存多久。
const LEG_STATE_TTL: Duration = Duration::from_secs(20);

impl LiveDesk {
    /// 一条腿在交易所实际的保证金状态（带缓存）。查不到就是 `None`，评估回落到台账里的数。
    async fn leg_state_cached(
        &self,
        venue: Venue,
        symbol: &Symbol,
    ) -> Option<arb_exec::VenueLegState> {
        let key = (venue, symbol.to_string());
        // 读数到缓存写入共用锁；否则规则轮清缓存后，先前在途的旧读数还能写回来。
        let mut cache = self.leg_state_cache.lock().await;
        if let Some((at, state)) = cache.get(&key)
            && at.elapsed() < LEG_STATE_TTL
        {
            return state.clone();
        }
        let state = match self.brokers.get(&venue) {
            Some(broker) => match broker.leg_state(symbol).await {
                Ok(state) => state,
                Err(error) => {
                    warn!(%venue, %symbol, "读交易所保证金没成功，强平价按台账里的保证金算：{error}");
                    None
                }
            },
            None => None,
        };
        cache.insert(key, (Instant::now(), state.clone()));
        state
    }

    /// 这些仓位两条腿在交易所的实际保证金状态。
    async fn leg_states(&self, positions: &[&PairPosition]) -> crate::strategy::LegStates {
        let mut out = crate::strategy::LegStates::new();
        for position in positions {
            let mut states = [None, None];
            for (slot, leg) in states
                .iter_mut()
                .zip([position.long.as_ref(), position.short.as_ref()])
            {
                if let Some(leg) = leg {
                    *slot = self.leg_state_cached(leg.venue, &position.symbol).await;
                }
            }
            let [long, short] = states;
            out.insert(position.id.clone(), (long, short));
        }
        out
    }

    /// 一条腿自 `since` 起的资金费流水合计（带缓存；`refresh` 时不看缓存、直接重查）。
    async fn leg_funding(
        &self,
        venue: Venue,
        symbol: &Symbol,
        since: DateTime<Utc>,
        refresh: bool,
    ) -> LegFunding {
        let key = (venue, symbol.to_string(), since);
        let cached = if refresh {
            None
        } else {
            let cache = self.funding_cache.lock().await;
            cache
                .get(&key)
                .filter(|(at, _)| at.elapsed() < FUNDING_CACHE_TTL)
                .map(|(_, result)| result.clone())
        };
        let result = match cached {
            Some(result) => result,
            None => {
                let result = match self.brokers.get(&venue) {
                    Some(broker) => broker
                        .funding_since(symbol, since)
                        .await
                        .map_err(|error| error.to_string()),
                    None => Err(format!("{venue} 没有连接实盘")),
                };
                match &result {
                    Ok(Some(total)) => info!(
                        %venue,
                        %symbol,
                        payments = total.payments,
                        usdt = %total.usdt,
                        "资金费流水已更新"
                    ),
                    Ok(None) => {}
                    Err(error) => warn!(%venue, %symbol, "资金费流水没查成：{error}"),
                }
                self.funding_cache
                    .lock()
                    .await
                    .insert(key, (Instant::now(), result.clone()));
                result
            }
        };
        match result {
            Ok(Some(total)) => LegFunding {
                venue,
                usdt: Some(total.usdt.round_dp(8)),
                payments: total.payments,
                last_at: total.last_at,
                note: None,
            },
            Ok(None) => LegFunding {
                venue,
                usdt: None,
                payments: 0,
                last_at: None,
                note: Some(format!("{venue} 还没接入资金费流水")),
            },
            Err(error) => LegFunding {
                venue,
                usdt: None,
                payments: 0,
                last_at: None,
                note: Some(format!("没查成：{error}")),
            },
        }
    }

    async fn position_funding(&self, position: &PairPosition, refresh: bool) -> PositionFunding {
        let since = position.opened_at;
        let mut legs = [None, None];
        for (slot, leg) in legs
            .iter_mut()
            .zip([position.long.as_ref(), position.short.as_ref()])
        {
            if let Some(leg) = leg {
                *slot = Some(
                    self.leg_funding(leg.venue, &position.symbol, since, refresh)
                        .await,
                );
            }
        }
        let [long, short] = legs;
        let total_usdt = match (&long, &short) {
            (Some(long), Some(short)) => long.usdt.zip(short.usdt).map(|(a, b)| a + b),
            (Some(only), None) | (None, Some(only)) => only.usdt,
            (None, None) => None,
        };
        PositionFunding {
            long,
            short,
            total_usdt,
            since,
        }
    }

    /// 对账；发现有仓位已经在交易所被外部（手动）平掉，就在台账里结束它，再对一次账。
    ///
    /// **调用方必须已持有 `self.lock`**：结束仓位要写台账，不能和下单、平仓、规则轮同时进行。
    /// 只识别、不下单。只剩一条腿、且连续两次确认的，记成 `Closing` 交给退出重试平掉剩下那条
    /// （[`desk::adopt_lone_legs`]，这里推告警）；其余只在返回的说明里提示（见
    /// [`desk::check_external_closes`]）。返回（对账结果，说明，本次结束的仓位 id）。
    async fn reconcile_adopting(
        &self,
    ) -> anyhow::Result<(Reconciliation, Vec<ExternalNote>, Vec<String>)> {
        let reconciliation = self.reconcile().await?;
        if reconciliation.divergences.is_empty() {
            if let Ok(mut seen) = self.external_seen.lock() {
                seen.clear();
            }
            return Ok((reconciliation, Vec::new(), Vec::new()));
        }
        let (replayed, _) = self.ledger.replay().await?;
        let exposed = replayed.exposed();
        let check = {
            let mut seen = self
                .external_seen
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            desk::check_external_closes(&reconciliation, &exposed, &mut seen, Instant::now())
        };
        if check.adopt.is_empty() && check.lone.is_empty() {
            return Ok((reconciliation, check.notes, Vec::new()));
        }
        // 一条腿在交易所没了（多半是被强平）：记下，下一次退出重试用只减仓单平掉剩下那条。
        // 告警在这里发：开仓前的对账也会走到这里，不只是规则轮。
        let lone = desk::adopt_lone_legs(&self.ledger, &exposed, &check.lone).await?;
        for position in &lone {
            let (gone, rest) = check
                .lone
                .iter()
                .find(|(id, _)| id == &position.id)
                .map(|(_, gone)| {
                    let rest = position
                        .long
                        .as_ref()
                        .or(position.short.as_ref())
                        .map_or_else(|| "另一".to_string(), |leg| leg.venue.to_string());
                    (gone.to_string(), rest)
                })
                .unwrap_or_default();
            self.alerts.notify_order_keyed(
                &format!("lone:{}", position.id),
                format!(
                    "🚨 {} {} 的 {gone} 腿在交易所已没有仓位（可能被强平），对账连续两次确认；正在用只减仓单自动平掉剩下的 {rest} 腿。实际盈亏平完后按交易所成交记录核算。",
                    position.id, position.symbol
                ),
            );
        }
        let adopted =
            desk::adopt_external_closes(&self.ledger, &self.brokers, &exposed, &check.adopt)
                .await?
                .into_iter()
                .map(|position| position.id)
                .collect::<Vec<_>>();
        let lone_ids: Vec<String> = lone.into_iter().map(|position| position.id).collect();
        // 结束之后再对一次账：本轮是否干净要按结束后的台账算，否则这笔自己的不一致
        // 还会让其它规则再被跳过一轮。
        let after = self.reconcile().await?;
        let notes = check
            .notes
            .into_iter()
            .filter(|note| {
                !adopted.contains(&note.position_id) && !lone_ids.contains(&note.position_id)
            })
            .collect();
        Ok((after, notes, adopted))
    }

    /// 补算已平仓仓位的实际盈亏（外部平仓的，和早于逐笔记账上线、当时没记下的）：按交易所的
    /// 成交记录核，数量逐腿对上才记。**只读交易所、不下单。**
    /// 每次调用对每笔待核算的仓位试一次；连续 [`PNL_ATTEMPTS`] 次都不行就把原因记进台账。
    /// 平仓 [`desk::FUNDING_RECHECK_AFTER`] 之后，按两条腿、[开仓, 平仓] 窗口重新核对已平仓仓位的
    /// 资金费；不一致就追加更正记录并推送。**只读交易所、不下单。** 调用方必须已持有 `self.lock`。
    async fn recheck_closed_funding(&self) {
        let replayed = match self.ledger.replay().await {
            Ok((replayed, _)) => replayed,
            Err(error) => {
                warn!("核对资金费时读不了实盘台账：{error}");
                return;
            }
        };
        let now = chrono::Utc::now();
        let mut due: Vec<&PairPosition> = replayed
            .positions
            .values()
            .filter(|position| {
                position.status == arb_exec::PositionStatus::Closed
                    && position.realized_source.is_some()
                    && position.funding_checked_at.is_none()
            })
            .collect();
        due.sort_by_key(|position| position.closed_at);
        // 每轮最多核几笔：每笔每条腿要查两次流水，Lighter RH 按出口 IP 限频极紧，一次打太多会让
        // 场所进入冷却、连带实盘的对账与查询一起失败。其余的下一轮（约 5 分钟后）再核。
        for position in due.into_iter().take(FUNDING_RECHECKS_PER_ROUND) {
            match desk::recheck_funding(&self.ledger, &self.brokers, position, now, false).await {
                Ok(desk::FundingCheck::Corrected(old, new)) => {
                    warn!(position = %position.id, ?old, %new, "已平仓仓位的资金费与交易所流水不一致，已更正");
                    self.alerts.notify_always(format!(
                        "🧾 {} {} 的资金费已按交易所流水更正：{} → {} USDT（两条腿、开仓至平仓）",
                        position.id,
                        position.symbol,
                        old.map_or("未知".to_string(), |old| old
                            .round_dp(4)
                            .normalize()
                            .to_string()),
                        new.round_dp(4).normalize()
                    ));
                }
                Ok(desk::FundingCheck::Confirmed) => {
                    info!(position = %position.id, "已平仓仓位的资金费已与交易所流水核对一致");
                }
                Ok(desk::FundingCheck::Unavailable | desk::FundingCheck::NotDue) => {}
                Err(error) => warn!(position = %position.id, "核对资金费失败：{error:#}"),
            }
        }
    }

    /// 台账里估算的手续费按交易所成交记录改正（见 [`desk::correct_estimated_fees`]），有改正就推一条汇总。
    /// **只读交易所、不下单。** 调用方必须已持有 `self.lock`。
    async fn correct_estimated_fees(&self) {
        let mut checked = std::mem::take(
            &mut *self
                .fee_checked
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let result = desk::correct_estimated_fees(
            &self.ledger,
            &self.brokers,
            &mut checked,
            FEE_CHECKS_PER_ROUND,
        )
        .await;
        if let Ok(mut kept) = self.fee_checked.lock() {
            kept.extend(checked);
        }
        let corrections = match result {
            Ok(corrections) => corrections,
            Err(error) => {
                warn!("改正估算的手续费失败：{error:#}");
                return;
            }
        };
        if corrections.is_empty() {
            return;
        }
        let total: Decimal = corrections
            .iter()
            .map(|fix| fix.actual - fix.recorded)
            .sum();
        let lines = corrections
            .iter()
            .map(|fix| {
                format!(
                    "· {} {}（{}）{} → {} USDT",
                    fix.symbol.base,
                    fix.order_id,
                    fix.venue,
                    fix.recorded.round_dp(4).normalize(),
                    fix.actual.round_dp(4).normalize()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        self.alerts.notify_always(format!(
            "🧾 {} 张订单的手续费原来是估的，已按交易所成交记录更正（合计 {} USDT）：\n{lines}",
            corrections.len(),
            total.round_dp(4).normalize()
        ));
    }

    async fn backfill_external_pnl(&self) {
        let replayed = match self.ledger.replay().await {
            Ok((replayed, _)) => replayed,
            Err(error) => {
                warn!("补算外部平仓盈亏时读不了实盘台账：{error}");
                return;
            }
        };
        let pending: Vec<PairPosition> = replayed
            .positions
            .values()
            .filter(|position| {
                position.status == arb_exec::PositionStatus::Closed
                    && position.realized_source.is_none()
                    && position.pnl_unattributed.is_none()
            })
            .cloned()
            .collect();
        for position in pending {
            match desk::settle_closed_from_fills(&self.ledger, &self.brokers, &position).await {
                Ok(desk::SettleOutcome::Settled(settled)) => {
                    if let Ok(mut attempts) = self.pnl_attempts.lock() {
                        attempts.remove(&position.id);
                    }
                    self.alerts.notify_always(format!(
                        "💰 {} {} 的实际盈亏已核算：{}",
                        settled.id,
                        settled.symbol,
                        settled.note.as_deref().unwrap_or_default()
                    ));
                }
                Ok(desk::SettleOutcome::Impossible(reason)) => {
                    warn!(position = %position.id, "外部平仓的实际盈亏核不出来：{reason}");
                    self.alerts.notify(
                        &format!("pnl-final:{}", position.id),
                        format!(
                            "⚠️ {} {} 的实际盈亏核不出来：{reason}",
                            position.id, position.symbol
                        ),
                    );
                    let _ = desk::mark_pnl_unattributed(&self.ledger, &position, &reason).await;
                }
                Ok(desk::SettleOutcome::Retry(reason)) => {
                    let tries = {
                        let mut attempts = self
                            .pnl_attempts
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let tries = attempts.entry(position.id.clone()).or_insert(0);
                        *tries += 1;
                        *tries
                    };
                    warn!(position = %position.id, tries, "外部平仓的实际盈亏这次没核出来：{reason}");
                    if tries >= PNL_ATTEMPTS {
                        let _ = desk::mark_pnl_unattributed(&self.ledger, &position, &reason).await;
                    }
                }
                Err(error) => warn!(position = %position.id, "补算外部平仓盈亏失败：{error:#}"),
            }
        }
    }

    /// 对账并记下结论。
    async fn reconcile(&self) -> anyhow::Result<Reconciliation> {
        let result = desk::reconcile_ledger(&self.ledger, &self.brokers).await;
        let (outcome, frozen) = match &result {
            Ok(reconciliation) => (
                Ok(reconciliation.divergences.len()),
                reconciliation.frozen_scope(),
            ),
            Err(error) => (Err(format!("{error:#}")), Vec::new()),
        };
        *self.last_reconcile.write().await = Some(ReconcileMark {
            at: Utc::now(),
            outcome,
            frozen,
        });
        result
    }

    /// 只对账这几家（开仓前用：这笔只用到两条腿的场所）。完整对账要等最慢的那家（Bitget / Arcus / MEXC 常要
    /// 100～170ms），而开仓只看这个币在这两条腿上能不能开（`blocks_open`）。不写 `last_reconcile`（看板和 healthz 的
    /// 「对账干净」要的是全部场所），也不做外部平仓认领（确认计时跟着完整对账走，局部的会把别家的计时清掉）——
    /// 那两件事照旧由规则轮的完整对账做。
    async fn reconcile_legs(&self, venues: &[Venue]) -> anyhow::Result<Reconciliation> {
        desk::reconcile_ledger_on(&self.ledger, &self.brokers, Some(venues)).await
    }

    fn executor(&self) -> Executor {
        Executor::new(
            Arc::clone(&self.ledger),
            self.brokers.values().cloned().collect(),
        )
    }

    fn ledger_path(&self) -> String {
        self.ledger.path().display().to_string()
    }

    /// 暂停 / 恢复开新仓，并落盘（重启后仍然生效）。恢复时记下时刻：熔断之后只数这个时刻
    /// 之后的新失败，旧的失败不会让恢复后的第一次失败立刻再次熔断。
    fn set_paused(&self, paused: bool, reason: &str) {
        let mut state = self
            .pause_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if paused {
            state.paused = true;
            state.reason = Some(reason.to_string());
            state.since = Some(Utc::now());
        } else {
            state.paused = false;
            state.reason = None;
            state.since = None;
            state.failures_after = Some(Utc::now());
        }
        if let Err(error) = pause::save(&self.pause_file, &state) {
            // 内存里照样生效，但要大声说：重启后会忘掉。
            error!(%error, path = %self.pause_file.display(), "暂停状态没写进文件：重启后会恢复开仓");
            self.alerts.notify(
                "pause-save-failed",
                "⚠️ 暂停状态写不进文件：重启后会自动恢复开仓",
            );
        }
        self.opens_paused
            .store(paused, std::sync::atomic::Ordering::SeqCst);
    }

    /// 一笔开仓以回滚收场（或中断）之后检查熔断：连续 [`pause::BREAKER_FAILURES`] 笔都回滚
    /// 就暂停开新仓。只拦新开仓，不碰平仓、规则、对账。
    async fn check_breaker(&self) {
        let Ok((replayed, _)) = self.ledger.replay().await else {
            return;
        };
        let floor = self
            .pause_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .failures_after;
        // 一张单都没成交、或挂单成交太碎回滚了一小块：不算失败，也不打断计数（见 `pause::is_benign_attempt`）。
        let failed = pause::consecutive_failed_opens(
            replayed
                .positions
                .values()
                .filter(|position| !pause::is_benign_attempt(position, &replayed.orders)),
            floor,
        );
        if failed >= pause::BREAKER_FAILURES
            && !self.opens_paused.load(std::sync::atomic::Ordering::SeqCst)
        {
            let reason = format!("连续 {failed} 笔开仓以回滚收场（熔断）");
            warn!(failed, "开仓熔断：暂停开新仓");
            self.set_paused(true, &reason);
            self.alerts.notify_always(format!(
                "⛔ 已自动暂停开新仓：{reason}。平仓、规则、对账照常。查明原因后在 Telegram 发 /resume 恢复。"
            ));
        }
    }

    /// 补仓中断留下的没终态的单：对账之前先核实、了结（见 [`desk::settle_stale_adds`]），了结之后同一轮的对账就是干净的。
    /// 没成交的只留日志；读回已有成交的会让持仓对不上，告警交人工。
    async fn settle_stale_adds(&self) {
        let mut seen = std::mem::take(
            &mut *self
                .add_seen
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let result = desk::settle_stale_adds(
            &self.executor(),
            &self.ledger,
            &mut seen,
            Instant::now(),
            desk::ADD_SETTLE_AFTER,
        )
        .await;
        *self
            .add_seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = seen;
        let settled = match result {
            Ok(settled) => settled,
            Err(error) => {
                warn!("核实补仓单时读不了台账，下一轮再试：{error:#}");
                return;
            }
        };
        for item in settled {
            match item.outcome {
                arb_exec::AddSettle::NoFill(why) => {
                    info!(position = %item.position_id, order = %item.order_id, %why,
                        "补仓单没有成交，已在台账里了结");
                }
                arb_exec::AddSettle::Filled(usdt) => {
                    warn!(position = %item.position_id, order = %item.order_id, %usdt,
                        "补仓单读回时已有成交但没并进腿：台账与交易所持仓对不上");
                    self.alerts.notify_always(format!(
                        "⚠️ {} 的补仓单 {} 读回时已有成交 {} USDT，但补仓中断、没有并进腿：台账与交易所持仓对不上，这个币已被对账冻结，请对照账户人工处理。",
                        item.symbol,
                        item.order_id,
                        usdt.round_dp(2)
                    ));
                }
                arb_exec::AddSettle::Waiting(why) => {
                    warn!(position = %item.position_id, order = %item.order_id, %why,
                        "补仓单还核实不了，下一轮再试");
                }
                arb_exec::AddSettle::Unbalanced(why) => {
                    warn!(position = %item.position_id, order = %item.order_id, %why,
                        "补仓单了结了，但两腿没拉齐");
                    self.alerts.notify_always(format!(
                        "⚠️ {} 的订单 {} 已核实，但两腿没拉齐：{why}。请对照账户人工处理。",
                        item.symbol, item.order_id
                    ));
                }
                arb_exec::AddSettle::Applied(why) => {
                    warn!(position = %item.position_id, order = %item.order_id, %why,
                        "读回了没终态订单的结局，已入账");
                }
                arb_exec::AddSettle::Hedged(why) => {
                    warn!(position = %item.position_id, order = %item.order_id, %why,
                        "补仓第二腿读回已成交，已并进腿");
                }
            }
        }
    }

    /// 对账干净才执行规则：台账与账户对不上时，自动平仓 / 减仓可能作用在错误的数量上。
    async fn round(&self, trigger: &'static str) -> RoundSummary {
        let mut summary = RoundSummary::new(trigger);
        // 先撤掉遗留的只挂单（挂单期间出错、开仓提前返回时可能留下）：交易台的锁在我们手里，
        // 不会碰上正在挂的单子。撤前成交的部分没有仓位记录，对账会如实报出，这里再告警一次。
        match desk::cancel_resting_post_only(&self.ledger, &self.brokers).await {
            Ok(cancelled) if cancelled.resolved.is_empty() && cancelled.still_open.is_empty() => {}
            Ok(cancelled) => {
                warn!(
                    resolved = ?cancelled.resolved,
                    still_open = ?cancelled.still_open,
                    filled_unrecorded = ?cancelled.filled_unrecorded,
                    "撤了遗留的只挂单"
                );
                self.alerts.notify_order(format!(
                    "⚠️ 发现并处理了遗留的只挂单：已撤 {} 张{}{}。",
                    cancelled.resolved.len(),
                    if cancelled.filled_unrecorded.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "，其中 {} 张撤前已有成交（{}），台账里没有对应的仓位，对账会报持仓不符，请对照账户人工处理",
                            cancelled.filled_unrecorded.len(),
                            cancelled.filled_unrecorded.join("、")
                        )
                    },
                    if cancelled.still_open.is_empty() {
                        String::new()
                    } else {
                        format!("；{} 张没撤掉（{}），请到币安手动撤", cancelled.still_open.len(), cancelled.still_open.join("、"))
                    }
                ));
            }
            Err(error) => warn!("撤遗留的只挂单时出错（对账会如实报出）：{error:#}"),
        }
        self.settle_stale_adds().await;
        match self.reconcile_adopting().await {
            Ok((reconciliation, notes, adopted)) => {
                summary.external = notes;
                summary.adopted = adopted;
                if reconciliation.is_clean() {
                    if let Ok(mut last) = self.divergence_sig.lock()
                        && !last.is_empty()
                    {
                        info!("实盘对账已恢复一致");
                        last.clear();
                    }
                } else {
                    // 不干净就要留下线索（否则只能靠猜为什么规则一直没执行），但同样的不一致
                    // 不必每一轮重复：指纹变了才 WARN，没变只记 DEBUG。
                    let sig = reconciliation
                        .divergences
                        .iter()
                        .map(|d| format!("{:?}|{:?}|{}", d.kind, d.venue, d.reference))
                        .collect::<Vec<_>>()
                        .join(";");
                    let changed = {
                        let mut last = self
                            .divergence_sig
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        let changed = *last != sig;
                        *last = sig;
                        changed
                    };
                    for divergence in &reconciliation.divergences {
                        if changed {
                            warn!(
                                kind = ?divergence.kind,
                                venue = ?divergence.venue,
                                reference = %divergence.reference,
                                "实盘对账不一致：{}",
                                divergence.detail
                            );
                        } else {
                            tracing::debug!(
                                kind = ?divergence.kind,
                                reference = %divergence.reference,
                                "实盘对账仍不一致"
                            );
                        }
                    }
                }
                Self::note_reconciliation(&mut summary, &reconciliation);
                // 只冻结出问题的币：它不评估规则（台账与账户对不上，自动平仓 / 减仓可能作用在错误的数量上），
                // 其余照常。没走完的退出（Closing / Unwinding）照常重试：那是在补裸腿，订单是 reduce-only。
                let ctx = desk::RoundCtx {
                    funding: self,
                    retries: &self.exit_retries,
                    exit_confirm_gap: desk::EXIT_CONFIRM_GAP,
                };
                let frozen = if reconciliation.is_clean() {
                    desk::Frozen::Nothing
                } else {
                    desk::Frozen::By(&reconciliation)
                };
                match desk::live_round(&self.executor(), &self.ledger, &self.by_venue, &ctx, frozen)
                    .await
                {
                    Ok(reports) => summary.reports = reports,
                    Err(error) => {
                        summary.error = Some(format!("规则轮出错：{error:#}"));
                    }
                }
                if summary.reports.iter().any(|report| report.reverify) {
                    self.resume_after_reverify(&mut summary).await;
                }
            }
            Err(error) => summary.error = Some(format!("对账失败，本轮跳过自动动作：{error:#}")),
        }
        for report in &summary.reports {
            if let Some(hold) = report
                .evaluation
                .as_ref()
                .and_then(|evaluation| evaluation.take_profit_hold.as_ref())
            {
                warn!(
                    position = %report.position_id,
                    symbol = %report.symbol,
                    target_usdt = %hold.target_usdt,
                    book_min_usdt = %hold.book_min_usdt,
                    mark_net_usdt = %hold.mark_net_usdt.round_dp(4),
                    book_net_usdt = ?hold.book_net_usdt.map(|net| net.round_dp(4)),
                    funding_usdt = %hold.funding_usdt.round_dp(4),
                    gap_mark_vs_mid_usdt = ?hold.gap.map(|gap| gap.mark_vs_mid_usdt.round_dp(4)),
                    gap_spread_usdt = ?hold.gap.map(|gap| gap.spread_usdt.round_dp(4)),
                    gap_depth_usdt = ?hold.gap.map(|gap| gap.depth_usdt.round_dp(4)),
                    gap_fee_usdt = ?hold.gap.map(|gap| gap.fee_usdt.round_dp(4)),
                    "止盈按标记价达标，但盘口核对未通过，继续持有：{}",
                    hold.reason
                );
            }
            if report.executed {
                info!(
                    position = %report.position_id,
                    status = ?report.status,
                    error = ?report.error,
                    "实盘规则已执行"
                );
                // Applied、Unknown 和失败后部分成交都可能改变真实保证金；不能等页面缓存过期。
                let symbol = report.symbol.to_string();
                self.leg_state_cache
                    .lock()
                    .await
                    .retain(|(_, cached_symbol), _| cached_symbol != &symbol);
            }
        }
        self.alert_round(&summary);
        self.alert_naked().await;
        self.publish_exit_watch(&summary).await;
        *self.last_round.write().await = Some(summary.clone());
        summary
    }

    /// 盘口触发止盈（价差监控逐秒看盘口，整笔平掉已到止盈时交过来，见 [`desk::book_trigger_exit`]）：不等下一个
    /// 规则轮，现拉盘口核对两次、仍到止盈就平。
    ///
    /// 只在规则轮本来就会动作的时候做：实盘下单模式、后台规则轮开着；交易台的锁拿不到（正在下单、规则轮在跑）
    /// 就不等 —— 窗口只有几秒，等到了也过去了，规则轮自己会看。上一轮对账不干净、或太久没跑（超过 3 个周期）
    /// 也不做：这一路不对账，靠的是上一轮的结论。
    pub(crate) async fn book_trigger(&self, position_id: String) {
        if self.mode != LiveMode::Trade || self.watch_sec == 0 || crate::shutdown::is_draining() {
            return;
        }
        let Ok(_guard) = self.lock.try_lock() else {
            tracing::debug!(position = %position_id, "盘口触发止盈：交易台正忙，交给规则轮");
            return;
        };
        let stale_after = chrono::Duration::seconds(
            i64::try_from(self.watch_sec.saturating_mul(3)).unwrap_or(i64::MAX),
        );
        let recent_clean = self.last_round.read().await.as_ref().is_some_and(|round| {
            round.reconciliation_clean == Some(true) && Utc::now() - round.at <= stale_after
        });
        if !recent_clean {
            info!(position = %position_id, "盘口触发止盈：上一轮对账不干净或太久没跑，不插手，交给规则轮");
            return;
        }
        let ctx = desk::RoundCtx {
            funding: self,
            retries: &self.exit_retries,
            exit_confirm_gap: desk::EXIT_CONFIRM_GAP,
        };
        let report = match desk::book_trigger_exit(
            &self.executor(),
            &self.ledger,
            &self.by_venue,
            &ctx,
            &position_id,
        )
        .await
        {
            Ok(Some(report)) => report,
            Ok(None) => return,
            Err(error) => {
                warn!(position = %position_id, "盘口触发止盈没做成（台账读不了）：{error:#}");
                return;
            }
        };
        if !report.executed {
            return;
        }
        info!(
            position = %report.position_id,
            status = ?report.status,
            error = ?report.error,
            "实盘规则已执行（盘口触发止盈）"
        );
        let symbol = report.symbol.to_string();
        self.leg_state_cache
            .lock()
            .await
            .retain(|(_, cached_symbol), _| cached_symbol != &symbol);
        if report.status != PositionStatus::Open {
            // 平掉了（或进了没走完的退出）：马上从盯盘名单里拿掉，别在下一轮之前又交过来一次。
            let mut watched = self
                .exit_watched
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let kept: Vec<_> = watched
                .iter()
                .filter(|item| item.position.id != report.position_id)
                .cloned()
                .collect();
            *watched = Arc::new(kept);
        }
        // 平成了单独说一句是「盘口触发」，和规则轮的止盈分得开；出错、限价保护没成交照规则轮的说法推。
        if report.error.is_none() && report.exit_held.is_none() {
            self.alerts.notify_order(format!(
                "✅ 盘口触发止盈：{} {} → {:?}。{}",
                report.position_id,
                report.symbol,
                report.status,
                report.note.as_deref().unwrap_or_default()
            ));
            return;
        }
        let mut summary = RoundSummary::new("book");
        summary.reports = vec![report];
        self.alert_round(&summary);
    }

    /// 把这一轮之后还开着的双腿持仓（连同平仓吃单费率、这一轮查到的资金费）交给分批平仓测算。
    /// 台账读不了就保留上一轮的：测算只看不下单，旧一轮的持仓顶多多算一笔刚平掉的。
    async fn publish_exit_watch(&self, summary: &RoundSummary) {
        let replayed = match self.ledger.replay().await {
            Ok((replayed, _)) => replayed,
            Err(error) => {
                warn!("分批平仓测算没拿到持仓（台账读不了）：{error:#}");
                return;
            }
        };
        let funding: HashMap<&str, Option<Decimal>> = summary
            .reports
            .iter()
            .map(|report| {
                (
                    report.position_id.as_str(),
                    report
                        .evaluation
                        .as_ref()
                        .and_then(|evaluation| evaluation.observation.funding_usdt),
                )
            })
            .collect();
        let executor = self.executor();
        let watched: Vec<_> = replayed
            .positions
            .into_values()
            .filter(|position| position.status == PositionStatus::Open && position.is_hedged())
            .map(|position| crate::rh_spread::exit_shadow::Watched {
                fees: executor.exit_fees(&position),
                funding_usdt: funding.get(position.id.as_str()).copied().flatten(),
                position,
            })
            .collect();
        *self
            .exit_watched
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(watched);
    }

    /// 把对账结论写进本轮摘要：干净与否、不一致明细、冻结了什么。
    fn note_reconciliation(summary: &mut RoundSummary, reconciliation: &Reconciliation) {
        let clean = reconciliation.is_clean();
        summary.reconciliation_clean = Some(clean);
        summary.divergences = reconciliation.divergences.clone();
        summary.frozen = reconciliation.frozen_scope();
        summary.error = (!clean).then(|| {
            format!(
                "对账不一致：冻结 {}（不评估规则、不开新仓，只重试没走完的退出）；其余照常",
                summary.frozen.join("、")
            )
        });
    }

    /// 续上建仓刚把开仓单的结局、腿补进台账（还差第二腿）：同一把锁里马上再对一次账，按新的结论再续一遍，
    /// 补第二腿不必等下一轮。只再来一遍；还要再核实的留给下一轮。
    async fn resume_after_reverify(&self, summary: &mut RoundSummary) {
        // 只为补腿前那道关取一份新结论：不认领外部平仓（那有自己按时间的二次确认，同一轮里连对两次账
        // 不该被当成两次确认）。
        let reconciliation = match self.reconcile().await {
            Ok(reconciliation) => reconciliation,
            Err(error) => {
                warn!("续上建仓之后的再次对账没做成，下一轮再补：{error:#}");
                return;
            }
        };
        Self::note_reconciliation(summary, &reconciliation);
        let ctx = desk::RoundCtx {
            funding: self,
            retries: &self.exit_retries,
            exit_confirm_gap: desk::EXIT_CONFIRM_GAP,
        };
        let frozen = if reconciliation.is_clean() {
            desk::Frozen::Nothing
        } else {
            desk::Frozen::By(&reconciliation)
        };
        match desk::resume_openings(&self.executor(), &self.ledger, &ctx, frozen).await {
            Ok(reports) => {
                for report in reports {
                    match summary
                        .reports
                        .iter_mut()
                        .find(|existing| existing.position_id == report.position_id)
                    {
                        Some(existing) => *existing = report,
                        None => summary.reports.push(report),
                    }
                }
            }
            Err(error) => warn!("续上建仓（再次对账后）出错，下一轮再试：{error:#}"),
        }
    }

    /// 一条腿裸着太久就升级告警（见 [`naked_alerts`]）。读台账失败只记日志：告警不能挡住规则轮。
    async fn alert_naked(&self) {
        let replayed = match self.ledger.replay().await {
            Ok((replayed, _)) => replayed,
            Err(error) => {
                warn!("裸腿告警读不了实盘台账：{error}");
                return;
            }
        };
        let exposed = replayed.exposed();
        let alerts = {
            let mut since = self
                .naked_since
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            naked_alerts(&exposed, &mut since, std::time::Instant::now())
        };
        for (key, text) in alerts {
            warn!("{text}");
            self.alerts.notify(&key, text);
        }
    }

    /// 这一轮里值得推到手机上的事：规则执行 / 失败、外部平仓、裸敞口、对账持续不干净。
    /// 消息只含仓位 id、合约、场所名和金额，不含地址与账户号（见 [`crate::alert`]）。
    fn alert_round(&self, summary: &RoundSummary) {
        use std::sync::atomic::Ordering;
        let alerts = &self.alerts;
        for id in &summary.adopted {
            alerts.notify_order_keyed(
                &format!("adopted:{id}"),
                format!("ℹ️ 仓位 {id} 已在交易所被外部平掉，台账已结束这笔；实际盈亏稍后按成交记录核算。"),
            );
        }
        for note in &summary.external {
            // 等第二次确认的说明不必推送；裸敞口、数量对不上要人看。
            if note.message.contains("裸敞口") || note.message.contains("数量与台账不符")
            {
                let mut hash = 0u32;
                for byte in note.message.bytes() {
                    hash = hash.wrapping_mul(31).wrapping_add(u32::from(byte));
                }
                alerts.notify(
                    &format!("ext:{}:{hash:08x}", note.position_id),
                    format!("⚠️ 仓位 {}：{}", note.position_id, note.message),
                );
            }
        }
        for report in &summary.reports {
            if let Some(hold) = report
                .evaluation
                .as_ref()
                .and_then(|evaluation| evaluation.take_profit_hold.as_ref())
            {
                alerts.notify(
                    &format!("take-profit-hold:{}", report.position_id),
                    take_profit_hold_alert(&report.position_id, &report.symbol, hold),
                );
            }
            // 自动加保证金没补成 / 结果不明 / 上限用完：要人看一眼（同一笔仓位同类消息 30 分钟一条）。
            if let Some(text) = &report.attention {
                alerts.notify(
                    &format!("margin:{}", report.position_id),
                    format!(
                        "⚠️ 自动加保证金 {} {}：{text}",
                        report.position_id, report.symbol
                    ),
                );
            }
            if let Some(error) = &report.error {
                let key = format!("err:{}", report.position_id);
                let text = format!(
                    "❌ 规则执行失败：{} {}：{error}",
                    report.position_id, report.symbol
                );
                if is_order_report(report) {
                    alerts.notify_order_keyed(&key, text);
                } else {
                    alerts.notify(&key, text);
                }
            } else if report.resumed {
                alerts.notify_order(format!(
                    "🔧 建仓中断已自动续上：{} {} → {:?}。{}",
                    report.position_id,
                    report.symbol,
                    report.status,
                    report.note.as_deref().unwrap_or_default()
                ));
            } else if let Some(text) = executed_alert(report) {
                if is_order_report(report) {
                    alerts.notify_order(text);
                } else {
                    alerts.notify_always(text);
                }
            }
        }
        match summary.reconciliation_clean {
            Some(false) => {
                let rounds = self.dirty_rounds.fetch_add(1, Ordering::SeqCst) + 1;
                if rounds >= DIRTY_ROUNDS_TO_ALERT {
                    let sig = summary
                        .divergences
                        .iter()
                        .map(|d| format!("{:?}:{}", d.kind, d.reference))
                        .collect::<Vec<_>>()
                        .join(",");
                    let detail = summary
                        .divergences
                        .iter()
                        .take(5)
                        .map(|d| format!("· {}", d.detail))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if alerts.notify(
                        &format!("recon:{sig}"),
                        format!(
                            "⚠️ 实盘对账已连续 {rounds} 轮不一致，冻结 {}（这些不执行自动规则、不开新仓），其余照常：\n{detail}",
                            summary.frozen.join("、")
                        ),
                    ) {
                        self.dirty_alerted.store(true, Ordering::SeqCst);
                    }
                }
            }
            Some(true) => {
                self.dirty_rounds.store(0, Ordering::SeqCst);
                if self.dirty_alerted.swap(false, Ordering::SeqCst) {
                    alerts.notify_always("✅ 实盘对账已恢复一致，冻结解除。");
                }
            }
            None => {
                if let Some(error) = &summary.error {
                    alerts.notify("round-failed", format!("⚠️ 实盘规则轮没跑成：{error}"));
                }
            }
        }
    }
}

/// 止盈决策现查结算流水；页面的 5 分钟缓存不能授权平仓。
#[async_trait::async_trait]
impl desk::FundingSource for LiveDesk {
    async fn funding_usdt(&self, position: &PairPosition) -> Option<Decimal> {
        self.position_funding(position, true).await.total_usdt
    }
}

/// 连续几轮对账不一致才告警：偶尔一轮抽风（接口超时）不值得响铃。
const DIRTY_ROUNDS_TO_ALERT: u32 = 3;

/// 两张交易台 + 令牌。
pub struct Trade {
    token: Option<String>,
    pub paper: Arc<PaperDesk>,
    /// 实盘交易台。开着实盘但启动时连不上（某家交易所维护、限频）时先空着，后台重连成功后填上：
    /// 行情、价差监控、Telegram、纸面不跟着停摆。**连接是全有或全无的** —— 不带着缺一家的
    /// 半个实盘运行：缺一家时对账会把那家的腿当成不存在，规则也会作用在错误的数量上。
    live_slot: Arc<std::sync::OnceLock<Arc<LiveDesk>>>,
    /// 实盘开着（`ARB_WEB_LIVE` 不是 `off`）。
    live_wanted: Option<LiveMode>,
    /// 实盘还没连上时最近一次失败的原因（给页面、/healthz、Telegram）。
    live_pending: Arc<std::sync::Mutex<Option<LivePending>>>,
    /// 自动交易每次检查都要读台账（哪些合约已有仓位、哪些仓位能补）；盘口一变就检查，最多每秒十次。
    /// 台账文件没变（长度、修改时间相同）就用上一次重放的结果。
    replays: std::sync::Mutex<HashMap<PathBuf, ReplayEntry>>,
}

/// [`Trade::replayed`] 的缓存项：文件长度、修改时间与重放结果。
struct ReplayEntry {
    len: u64,
    modified: std::time::SystemTime,
    replayed: Arc<arb_exec::Replayed>,
}

/// 实盘还没连上：从什么时候开始、试了几次、最近一次为什么失败。
#[derive(Debug, Clone, serde::Serialize)]
pub struct LivePending {
    pub since: chrono::DateTime<Utc>,
    pub attempts: u32,
    pub error: String,
}

impl Trade {
    /// 仅测试：明确无实盘、不读取 .env、不构造券商或启动后台重连。
    #[cfg(test)]
    pub(crate) fn paper_only_for_test(ledger_path: String, apis: &[Arc<dyn VenueApi>]) -> Self {
        Self {
            token: None,
            paper: Arc::new(PaperDesk {
                ledger_path,
                ledger: OnceCell::new(),
                by_venue: crate::api_map(apis),
                fee_per_side: Decimal::ZERO,
                lock: Mutex::new(()),
                last_round: RwLock::new(None),
                watch_sec: 0,
                retries: desk::ExitRetries::default(),
            }),
            live_slot: Arc::new(std::sync::OnceLock::new()),
            live_wanted: None,
            live_pending: Arc::new(std::sync::Mutex::new(None)),
            replays: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// 从环境变量组装。实盘开着但连接失败时返回错误 —— 看板不带着一个半连接的实盘启动。
    pub async fn from_env(
        settings: &Settings,
        client: &reqwest::Client,
        apis: &[Arc<dyn VenueApi>],
        loopback: bool,
        alerts: &Arc<Alerter>,
        rates: Arc<arb_scanner::RatesCache>,
    ) -> Result<Self> {
        let token = std::env::var("ARB_WEB_TOKEN")
            .ok()
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty());
        if let Some(token) = &token
            && token.chars().count() < MIN_TOKEN_LEN
        {
            bail!("ARB_WEB_TOKEN 至少 {MIN_TOKEN_LEN} 个字符");
        }
        match &token {
            None => warn!("没有配置 ARB_WEB_TOKEN：看板的交易接口全部关闭，只能看"),
            Some(_) if !loopback => {
                warn!("看板监听在非回环地址上：令牌走明文 HTTP，请在前面加 TLS 反向代理")
            }
            Some(_) => {}
        }

        let paper = Arc::new(PaperDesk {
            ledger_path: crate::ledger_path(),
            ledger: OnceCell::new(),
            by_venue: crate::api_map(apis),
            fee_per_side: settings.fee_per_side,
            lock: Mutex::new(()),
            last_round: RwLock::new(None),
            watch_sec: env_u64("ARB_WEB_PAPER_WATCH_SEC", 0)?,
            retries: desk::ExitRetries::default(),
        });

        let mode = LiveMode::from_env()?;
        let trade = Self {
            token,
            paper,
            live_slot: Arc::new(std::sync::OnceLock::new()),
            live_wanted: (mode != LiveMode::Off).then_some(mode),
            live_pending: Arc::new(std::sync::Mutex::new(None)),
            replays: std::sync::Mutex::new(HashMap::new()),
        };
        if mode == LiveMode::Off {
            return Ok(trade);
        }
        if trade.token.is_none() {
            bail!(
                "ARB_WEB_LIVE={} 需要先配置 ARB_WEB_TOKEN：实盘账户不能在没有鉴权的页面上暴露",
                mode.as_str()
            );
        }
        // 配置错误（滑点、场所名、凭据格式、台账打不开）仍然立刻报错退出：那不会自己好。
        // 只有交易所连不上 / 限频 / 维护这类**暂时的**失败，才让看板先起来、后台重连。
        let plan = LivePlan::from_env(settings, mode, rates)?;
        match connect_live(&plan, client, Arc::clone(alerts)).await {
            Ok(desk) => {
                let _ = trade.live_slot.set(Arc::new(desk));
            }
            Err(error) => {
                let message = format!("{error:#}");
                error!(
                    "实盘账户暂时连不上，先启动看板（行情、价差监控、Telegram、纸面照常），后台每 {} 秒重连：{message}",
                    LIVE_RETRY.as_secs()
                );
                alerts.notify_always(format!(
                    "⚠️ 实盘账户暂时连不上：{}。看板已先启动（行情、价差监控、Telegram 照常），实盘下单、规则与对账暂停；后台会自动重连，连上后再通知。",
                    crate::alert::redact(&message)
                ));
                *trade.live_pending.lock().unwrap_or_else(|p| p.into_inner()) = Some(LivePending {
                    since: Utc::now(),
                    attempts: 1,
                    error: message,
                });
                trade.spawn_live_reconnect(plan, client.clone(), Arc::clone(alerts));
            }
        }
        Ok(trade)
    }

    /// 配置的实盘模式（不管连没连上）。
    pub(crate) fn live_mode(&self) -> LiveMode {
        self.live_wanted.unwrap_or(LiveMode::Off)
    }

    /// 实盘交易台（已连上时）。
    pub(crate) fn live_opt(&self) -> Option<&Arc<LiveDesk>> {
        self.live_slot.get()
    }

    /// 给分批平仓测算（[`crate::rh_spread::exit_shadow`]）的持仓来源：实盘最近一轮规则轮之后还开着的双腿持仓；
    /// 实盘没连上时是空的。
    pub(crate) fn exit_source(&self) -> crate::rh_spread::exit_shadow::Source {
        let slot = Arc::clone(&self.live_slot);
        Box::new(move || {
            slot.get().map_or_else(Arc::default, |live| {
                Arc::clone(
                    &live
                        .exit_watched
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                )
            })
        })
    }

    /// 盘口触发止盈交给谁（[`crate::rh_spread::exit_shadow::Shadow::take_fired`]）：实盘交易台（已连上时），
    /// 在独立任务里做（[`LiveDesk::book_trigger`]），不卡价差监控的循环。
    pub(crate) fn exit_trigger(&self) -> crate::rh_spread::exit_shadow::Trigger {
        let slot = Arc::clone(&self.live_slot);
        Box::new(move |position_id| {
            if let Some(live) = slot.get() {
                let live = Arc::clone(live);
                tokio::spawn(async move { live.book_trigger(position_id).await });
            }
        })
    }

    /// 实盘开着但还没连上：原因。
    pub(crate) fn live_pending(&self) -> Option<LivePending> {
        if self.live_slot.get().is_some() {
            return None;
        }
        self.live_pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// 后台重连实盘：直到连上或开始停机。连上后填入交易台并启动它的后台任务。
    fn spawn_live_reconnect(&self, plan: LivePlan, client: reqwest::Client, alerts: Arc<Alerter>) {
        let slot = Arc::clone(&self.live_slot);
        let pending = Arc::clone(&self.live_pending);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(LIVE_RETRY).await;
                if crate::shutdown::is_draining() {
                    return;
                }
                match connect_live(&plan, &client, Arc::clone(&alerts)).await {
                    Ok(desk) => {
                        let desk = Arc::new(desk);
                        if slot.set(Arc::clone(&desk)).is_err() {
                            return;
                        }
                        let down = pending
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .take();
                        let waited = down
                            .as_ref()
                            .map_or(0, |d| (Utc::now() - d.since).num_minutes());
                        info!(
                            waited_min = waited,
                            "实盘账户已连上：实盘下单、规则与对账恢复"
                        );
                        alerts.notify_always(format!(
                            "✅ 实盘账户已连上（中断约 {waited} 分钟）：实盘下单、持仓规则与对账已恢复。"
                        ));
                        spawn_live_tasks(&desk);
                        return;
                    }
                    Err(error) => {
                        let message = format!("{error:#}");
                        let attempts = {
                            let mut guard = pending.lock().unwrap_or_else(|p| p.into_inner());
                            let entry = guard.get_or_insert_with(|| LivePending {
                                since: Utc::now(),
                                attempts: 0,
                                error: String::new(),
                            });
                            entry.attempts += 1;
                            entry.error = message.clone();
                            entry.attempts
                        };
                        warn!(attempts, "实盘账户仍然连不上：{message}");
                        // 每 30 分钟提醒一次还没恢复（同一 key 的告警自带 30 分钟冷却）。
                        alerts.notify(
                            "live-reconnect",
                            format!(
                                "⚠️ 实盘账户仍然连不上（已试 {attempts} 次）：{}。实盘下单、规则与对账仍暂停。",
                                crate::alert::redact(&message)
                            ),
                        );
                    }
                }
            }
        });
    }

    /// 后台为实盘持仓刷新资金费流水（见 [`spawn_live_tasks`]）。实盘还没连上时由重连任务在连上后启动。
    pub fn spawn_funding_refresher(&self) {
        // 实盘的后台任务统一在 [`spawn_live_tasks`] 里启动。
    }

    pub fn spawn_watchers(&self) {
        if self.paper.watch_sec > 0 {
            let paper = Arc::clone(&self.paper);
            let interval = Duration::from_secs(paper.watch_sec);
            info!(interval_sec = paper.watch_sec, "纸面持仓规则由看板后台执行");
            tokio::spawn(rounds_forever(interval, "纸面", None, move || {
                let paper = Arc::clone(&paper);
                async move {
                    let _guard = paper.lock.lock().await;
                    if !crate::shutdown::is_draining() {
                        paper.round("auto").await;
                    }
                }
            }));
        }
        if let Some(live) = self.live_opt() {
            spawn_live_tasks(live);
        }
    }

    /// 停机排空：等两张交易台上进行中的操作（下单、平仓、一轮规则）做完，并一直持有锁，
    /// 直到返回值被丢弃。实盘放在前面：它是真实资金。
    pub async fn quiesce(&self, deadline: tokio::time::Instant) -> crate::shutdown::Quiesced<'_> {
        let mut desks = Vec::with_capacity(2);
        if let Some(live) = self.live_opt() {
            desks.push(("实盘", &live.lock));
        }
        desks.push(("纸面", &self.paper.lock));
        crate::shutdown::quiesce(&desks, deadline).await
    }

    pub(crate) fn authorize(&self, headers: &HeaderMap) -> Result<(), Denied> {
        let Some(expected) = &self.token else {
            return Err(Denied(
                StatusCode::FORBIDDEN,
                "看板没有配置 ARB_WEB_TOKEN，交易接口已关闭",
            ));
        };
        let provided = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        if constant_time_eq(provided.trim().as_bytes(), expected.as_bytes()) {
            Ok(())
        } else {
            // 令牌失败要留痕、要能被数出来：监听在非回环地址时这是唯一能看到有人在猜令牌的地方。
            // 日志只记次数，不记来源提供的任何内容（那可能就是令牌本身）。
            let failures = crate::metrics::count_auth_failure();
            if failures.is_power_of_two() {
                warn!(
                    failures,
                    "令牌校验失败（累计）：收到了错误或缺失的 Authorization"
                );
            }
            Err(Denied(StatusCode::UNAUTHORIZED, "令牌不对或没有提供"))
        }
    }

    /// 执行中断（结果不明）之后自愈了没有：`since` 之后做过一次完整对账、结果干净，台账里也没有结局没定的单
    /// （续上建仓、了结补仓单都做完了）。自动交易据此决定能不能恢复开仓（见 `rh_auto` 的中断暂停）。
    /// `Err` 带上还在等什么。
    pub(crate) async fn settled_since(&self, since: DateTime<Utc>) -> Result<(), String> {
        let live = self.live_opt().ok_or("实盘没连上")?;
        let mark = live
            .last_reconcile
            .read()
            .await
            .clone()
            .ok_or("还没做过对账")?;
        if mark.at <= since {
            return Err("中断之后还没做过完整对账".into());
        }
        match &mark.outcome {
            Ok(0) => {}
            Ok(count) => return Err(format!("最近一次对账还有 {count} 处不一致")),
            Err(error) => return Err(format!("最近一次对账没做成：{error}")),
        }
        let (replayed, _) = live
            .ledger
            .replay()
            .await
            .map_err(|error| format!("读不了台账：{error}"))?;
        let pending: Vec<&str> = replayed
            .orders
            .values()
            .filter(|state| state.status.is_live())
            .map(|state| state.order.client_order_id.0.as_str())
            .collect();
        if !pending.is_empty() {
            return Err(format!(
                "台账里还有 {} 张单结局没定（{}）",
                pending.len(),
                pending.join("、")
            ));
        }
        Ok(())
    }

    /// 最近一次实盘对账只冻结了部分币时的说明（挡不住整张表，只提示哪些币开不了）。
    pub(crate) async fn live_reconcile_note(&self) -> Option<String> {
        let live = self.live_opt()?;
        let mark = live.last_reconcile.read().await.clone()?;
        match mark.outcome {
            Ok(count) if count > 0 && !mark.frozen.iter().any(|item| item == "全部") => {
                Some(format!(
                    "最近一次实盘对账有 {count} 处不一致，冻结 {}：这些开不了新仓，其余照常",
                    mark.frozen.join("、")
                ))
            }
            _ => None,
        }
    }

    /// 最近一次实盘对账不干净（或没做成）时的说明：实盘预览开仓前要先对账，
    /// 不干净就一律拒绝，这一关后台预检看不到，要单独告诉操作者。
    pub(crate) async fn live_reconcile_warning(&self) -> Option<String> {
        let live = self.live_opt()?;
        let mark = live.last_reconcile.read().await.clone()?;
        let ago = (Utc::now() - mark.at).num_seconds().max(0);
        let ago = if ago < 60 {
            format!("{ago} 秒前")
        } else {
            format!("{} 分钟前", ago / 60)
        };
        match mark.outcome {
            Ok(0) => None,
            // 只冻结出问题的币：挡不住整张表，交给 `live_reconcile_note` 说明。
            Ok(_) if !mark.frozen.iter().any(|item| item == "全部") => None,
            Ok(count) => Some(format!(
                "最近一次实盘对账（{ago}）有 {count} 处不一致、归不到哪个币：实盘预览会拒绝开任何新仓，✓ 只说明盘口与闸门这一关能过。先到「持仓」页处理台账之外的持仓或挂单"
            )),
            Err(error) => Some(format!(
                "最近一次实盘对账没做成（{ago}）：{error}。实盘预览同样要先对账，可能会被拒绝"
            )),
        }
    }

    /// 台账里还有敞口的仓位：(id, 合约)。纸面台账还没建时为空。
    pub(crate) async fn exposed_positions(
        &self,
        live: bool,
    ) -> Result<Vec<(String, Symbol)>, String> {
        let Some(replayed) = self.replayed(live).await? else {
            return Ok(Vec::new());
        };
        Ok(replayed
            .exposed()
            .into_iter()
            .map(|position| (position.id.clone(), position.symbol.clone()))
            .collect())
    }

    /// 重放实盘 / 纸面台账（只读）。纸面台账文件还不存在时是 `None`。台账文件的长度与修改时间都没变时
    /// 直接用上一次的结果（追加一行长度一定变）。
    async fn replayed(&self, live: bool) -> Result<Option<Arc<arb_exec::Replayed>>, String> {
        let path = if live {
            let Some(live) = self.live_opt() else {
                return Err("实盘账户没连上".into());
            };
            PathBuf::from(live.ledger_path())
        } else {
            PathBuf::from(&self.paper.ledger_path)
        };
        let meta = match tokio::fs::metadata(&path).await {
            Ok(meta) => Some(meta),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !live => return Ok(None),
            Err(_) => None,
        };
        let stamp = meta
            .as_ref()
            .and_then(|meta| Some((meta.len(), meta.modified().ok()?)));
        if let Some((len, modified)) = stamp
            && let Some(hit) = self
                .replays
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&path)
                .filter(|entry| entry.len == len && entry.modified == modified)
        {
            return Ok(Some(Arc::clone(&hit.replayed)));
        }
        let replayed = Arc::new(
            arb_exec::replay_file(&path)
                .await
                .map_err(|error| error.to_string())?
                .0,
        );
        if let Some((len, modified)) = stamp {
            self.replays
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(
                    path,
                    ReplayEntry {
                        len,
                        modified,
                        replayed: Arc::clone(&replayed),
                    },
                );
        }
        Ok(Some(replayed))
    }

    /// 可以往上补仓的仓位：持仓中、两腿齐全的价差套利仓位，带上实际开仓价差与补过几次。
    pub(crate) async fn add_targets(
        &self,
        live: bool,
    ) -> Result<Vec<crate::rh_auto::AddTarget>, String> {
        let Some(replayed) = self.replayed(live).await? else {
            return Ok(Vec::new());
        };
        Ok(replayed
            .exposed()
            .into_iter()
            .filter(|position| {
                position.status == PositionStatus::Open
                    && position.strategy == arb_exec::Strategy::Spread
            })
            .filter_map(|position| {
                let (long, short) = (position.long.as_ref()?, position.short.as_ref()?);
                let mid = (long.average_price + short.average_price) / Decimal::TWO;
                (mid > Decimal::ZERO).then(|| crate::rh_auto::AddTarget {
                    id: position.id.clone(),
                    base: position.symbol.base.clone(),
                    long: long.venue,
                    short: short.venue,
                    entry_spread_pct: (short.average_price - long.average_price) / mid
                        * Decimal::ONE_HUNDRED,
                    notional_usdt: long.notional_usdt.max(short.notional_usdt),
                    adds: arb_exec::executor::add_rounds(&position.id, &replayed.orders),
                })
            })
            .collect())
    }

    /// 实盘是否可下单（连上且 `trade` 模式）。
    pub(crate) fn live_can_trade(&self) -> bool {
        self.live_opt()
            .is_some_and(|live| live.mode == LiveMode::Trade)
    }

    /// 纸面规则轮间隔（0 = 看板不跑纸面规则）。
    pub(crate) fn paper_watch_sec(&self) -> u64 {
        self.paper.watch_sec
    }

    /// 台账里还有敞口的仓位数（与预览数的同一个口径）。纸面台账还没建时是 0 ——
    /// 只看不做的看板不该为了数仓位去创建台账文件。
    pub(crate) async fn open_positions(&self, live: bool) -> Result<usize, String> {
        if live {
            let Some(live) = self.live_opt() else {
                return Ok(0);
            };
            let (replayed, _) = live
                .ledger
                .replay()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(replayed.exposed().len());
        }
        let (replayed, _) = arb_exec::replay_file(&self.paper.ledger_path)
            .await
            .map_err(|error| error.to_string())?;
        Ok(replayed.exposed().len())
    }

    /// 上一轮规则轮按盘口估的「现在整笔平掉」含资金费净额，按仓位 id。盘口没拉到 / 资金费未知的仓位不在里面。
    pub(crate) async fn live_book_nets(&self) -> HashMap<String, Decimal> {
        let Some(live) = self.live_opt() else {
            return HashMap::new();
        };
        let last = live.last_round.read().await;
        last.iter()
            .flat_map(|round| round.reports.iter())
            .filter_map(|report| {
                let net = report
                    .evaluation
                    .as_ref()?
                    .observation
                    .book_net_with_funding_usdt()?;
                Some((report.position_id.clone(), net))
            })
            .collect()
    }

    /// 实盘规则轮的健康状态（给 `/healthz`）：只有布尔值和时间，不含账户信息。实盘没开为 `None`。
    pub(crate) async fn live_health(&self) -> Option<LiveHealth> {
        let mode = self.live_wanted?;
        let Some(live) = self.live_opt() else {
            // 开着实盘却连不上：规则轮根本没在跑，对监控来说就是停了（/healthz 返回 503）。
            return Some(LiveHealth {
                mode,
                watch_sec: 0,
                last_round_age_s: None,
                reconciliation_clean: None,
                dirty_rounds: 0,
                frozen: Vec::new(),
                stalled: true,
                opens_paused: false,
                disconnected: true,
            });
        };
        let last = live.last_round.read().await;
        let age_s = last
            .as_ref()
            .map(|round| (Utc::now() - round.at).num_seconds().max(0));
        let clean = last.as_ref().and_then(|round| round.reconciliation_clean);
        let frozen = last
            .as_ref()
            .map(|round| round.frozen.clone())
            .unwrap_or_default();
        // 规则轮该每 watch_sec 秒跑一次：超过三个周期（且至少 3 分钟）没跑就是停了。只有 trade
        // 模式才有后台规则轮，只读模式不算。
        let stalled = live.mode == LiveMode::Trade
            && live.watch_sec > 0
            && age_s.is_some_and(|age| age > (live.watch_sec as i64 * 3).max(180));
        Some(LiveHealth {
            mode: live.mode,
            watch_sec: live.watch_sec,
            last_round_age_s: age_s,
            reconciliation_clean: clean,
            dirty_rounds: live.dirty_rounds.load(std::sync::atomic::Ordering::Relaxed),
            frozen,
            stalled,
            opens_paused: live.opens_paused.load(std::sync::atomic::Ordering::Relaxed),
            disconnected: false,
        })
    }

    /// 暂停 / 恢复开新仓。实盘没开时返回 `false`。
    pub(crate) fn set_opens_paused(&self, paused: bool) -> bool {
        match self.live_opt() {
            Some(live) => {
                live.set_paused(paused, "手动暂停（Telegram /pause）");
                true
            }
            None => false,
        }
    }

    pub(crate) fn opens_paused(&self) -> bool {
        self.live_opt()
            .as_ref()
            .is_some_and(|live| live.opens_paused.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// 实盘台账里各状态的仓位数（给 `/metrics`）：包含已结束的，所以 `unwound`（开仓没做完、
    /// 被回滚）的累计数也在里面。实盘没开或台账读不了时为空。
    pub(crate) async fn ledger_status_counts(&self) -> Vec<(String, usize)> {
        let Some(live) = self.live_opt() else {
            return Vec::new();
        };
        let Ok((replayed, _)) = live.ledger.replay().await else {
            return Vec::new();
        };
        let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
        for position in replayed.positions.values() {
            *counts
                .entry(format!("{:?}", position.status).to_lowercase())
                .or_default() += 1;
        }
        counts.into_iter().collect()
    }

    /// 各实盘场所现在的可用保证金（只读）。查不到的带原因。
    pub(crate) async fn free_collaterals(&self) -> Vec<(Venue, Result<Option<Decimal>, String>)> {
        let Some(live) = self.live_opt() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for venue in &live.venues {
            let Some(broker) = live.brokers.get(venue) else {
                continue;
            };
            out.push((
                *venue,
                broker
                    .free_collateral()
                    .await
                    .map_err(|error| error.to_string()),
            ));
        }
        out
    }

    /// 当日（UTC）已实现盈亏（来自台账，不联网）。
    pub(crate) async fn daily(&self) -> Result<desk::DailyPnl, String> {
        match self.live_opt() {
            Some(live) => live_daily(live).await,
            None => Err("看板没有开启实盘".into()),
        }
    }

    /// 实盘台账路径下的持仓视图（含开仓以来的资金费）。
    pub(crate) async fn live_positions(
        &self,
        report: &ScanReport,
    ) -> Result<crate::strategy::Positions, String> {
        self.live_positions_page(report, crate::strategy::ClosedPage::default())
            .await
    }

    /// [`Self::live_positions`]，「最近结束」取第 `page` 页。
    pub(crate) async fn live_positions_page(
        &self,
        report: &ScanReport,
        page: crate::strategy::ClosedPage,
    ) -> Result<crate::strategy::Positions, String> {
        let live = self.live_opt().ok_or("看板没有开启实盘")?;
        let (replayed, broken) = live
            .ledger
            .replay()
            .await
            .map_err(|error| format!("读不了实盘台账：{error}"))?;
        let exposed: Vec<&PairPosition> = replayed.exposed();
        let states = live.leg_states(&exposed).await;
        let mut funding = HashMap::with_capacity(exposed.len());
        for position in &exposed {
            funding.insert(
                position.id.clone(),
                live.position_funding(position, false).await,
            );
        }
        let ledger_path = live.ledger_path();
        let dismissed = crate::dismissed::load(&ledger_path);
        // 扫描快照里找不到的腿（价格偏离其它场所超过 5%，被扫描当成了别的资产）：只对这几个场所实时拉一次行情顶上，
        // 持仓页才有标记价 / 浮动盈亏 / 强平距离 / 每日资金费。拉不到就空着，和以前一样。
        let missing = crate::strategy::venues_missing_from_scan(&exposed, report);
        let fallback = if missing.is_empty() {
            HashMap::new()
        } else {
            arb_exec::cli::fetch_snapshots(&live.by_venue, &missing).await
        };
        let mut view = crate::strategy::positions_page_full(
            &replayed,
            broken,
            &ledger_path,
            report,
            &fallback,
            &states,
            Some(funding),
            page,
            &dismissed,
        );
        // 按交易所的成交额：实盘连着的每一家都列出来，没成交的记 0。
        view.stats.include_venues(&live.venues);
        Ok(view)
    }

    /// 实盘开着但只读：能预览、不能下单。
    pub(crate) fn live_readonly(&self) -> bool {
        self.live_opt()
            .as_ref()
            .is_some_and(|live| live.mode != LiveMode::Trade)
    }

    /// 实盘连着的场所。实盘没开时为 `None`。
    pub(crate) fn live_venues(&self) -> Option<&[Venue]> {
        self.live_opt().map(|live| live.venues.as_slice())
    }

    pub(crate) fn supports_add_margin(&self, venue: Venue) -> bool {
        self.live_opt()
            .as_ref()
            .and_then(|live| live.brokers.get(&venue))
            .is_some_and(|broker| broker.supports_add_margin())
    }

    fn live(&self) -> Result<&Arc<LiveDesk>, Denied> {
        match (self.live_opt(), self.live_wanted) {
            (Some(live), _) => Ok(live),
            (None, None) => Err(Denied(
                StatusCode::BAD_REQUEST,
                "看板没有开启实盘（ARB_WEB_LIVE=off）",
            )),
            // 开着实盘但交易所暂时连不上：503，页面与脚本知道是暂时的。
            (None, Some(_)) => Err(Denied(
                StatusCode::SERVICE_UNAVAILABLE,
                "实盘账户暂时连不上（交易所维护或限频），后台每分钟自动重连；在此之前实盘下单、规则与对账暂停",
            )),
        }
    }
}

/// 鉴权或能力检查没过。用小结构体而不是整个 `Response` 放在 `Err` 里：后者太大。
pub(crate) struct Denied(StatusCode, &'static str);

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        fail(self.0, self.1)
    }
}

/// 实盘交易台的后台任务：持仓规则轮（`trade` 模式）与资金费流水刷新 / 外部平仓核算 / 资金费核对。
/// 启动时连上就在启动时调用；启动时没连上，由重连任务连上后调用。每个交易台只调用一次。
fn spawn_live_tasks(live: &Arc<LiveDesk>) {
    {
        let live = Arc::clone(live);
        tokio::spawn(async move {
            loop {
                match live.ledger.replay().await {
                    Ok((replayed, _)) => {
                        // 从开得最早的查起：Hyperliquid 同一个地址的流水拉一次就共享（起点不晚于要的就直接用），
                        // 先拉最早的那份，后面几条腿都用得上。
                        let mut exposed = replayed.exposed();
                        exposed.sort_by_key(|position| position.opened_at);
                        for position in exposed {
                            live.position_funding(position, true).await;
                        }
                    }
                    Err(error) => warn!("刷新资金费流水时读不了实盘台账：{error}"),
                }
                // 顺带补算外部平仓的实际盈亏（没有待核算的仓位时什么都不做）。它会往台账里写，
                // 所以要占交易台的锁：不和下单、规则轮同时追加，停机排空也会等它写完。忙就跳过，下个周期再来。
                if let Ok(_guard) = live.lock.try_lock() {
                    live.backfill_external_pnl().await;
                    live.recheck_closed_funding().await;
                    live.correct_estimated_fees().await;
                }
                // 比缓存有效期略短：页面读到的永远是后台刚刷新过的。
                tokio::time::sleep(FUNDING_CACHE_TTL - Duration::from_secs(10)).await;
            }
        });
    }
    if live.mode == LiveMode::Trade && live.watch_sec > 0 {
        let live = Arc::clone(live);
        let interval = Duration::from_secs(live.watch_sec);
        info!(interval_sec = live.watch_sec, "实盘持仓规则由看板后台执行");
        let alerts = Arc::clone(&live.alerts);
        tokio::spawn(rounds_forever(
            interval,
            "实盘",
            Some(alerts),
            move || {
                let live = Arc::clone(&live);
                async move {
                    let _guard = live.lock.lock().await;
                    // 等锁的时候收到了停机信号：这一轮不开始（已经在跑的那轮不受影响）。
                    if !crate::shutdown::is_draining() {
                        live.round("auto").await;
                    }
                }
            },
        ));
    }
}

/// 实盘配置（从环境变量解析一次）。解析失败是配置错误，启动即报；连接失败才重试。
#[derive(Clone)]
struct LivePlan {
    mode: LiveMode,
    venues: Vec<Venue>,
    auto: bool,
    market_slippage: Option<Decimal>,
    options: ConnectOptions,
    settings: Settings,
    ledger_path: String,
    watch_sec: u64,
    /// 看板后台扫描记下的各场所读数（见 [`LiveDesk::scan_for`]）。
    rates: Arc<arb_scanner::RatesCache>,
}

impl LivePlan {
    fn from_env(
        settings: &Settings,
        mode: LiveMode,
        rates: Arc<arb_scanner::RatesCache>,
    ) -> Result<Self> {
        let selection = live_connect::live_venues(None)?;
        let venues = selection.venues;
        let market_slippage = match std::env::var("ARB_WEB_MARKET_SLIPPAGE") {
            Ok(raw) if !raw.trim().is_empty() => Some(
                parse_decimal(raw.trim())
                    .with_context(|| format!("ARB_WEB_MARKET_SLIPPAGE 必须是小数，收到 {raw:?}"))?,
            ),
            _ => None,
        };
        let options = ConnectOptions {
            journal_dir: PathBuf::from(
                std::env::var("ARB_LIVE_JOURNAL_DIR").unwrap_or_else(|_| ".".into()),
            ),
            trading_enabled: mode == LiveMode::Trade,
            market_slippage,
        };
        options
            .validate()
            .context("ARB_WEB_LIVE=trade 必须同时给 ARB_WEB_MARKET_SLIPPAGE")?;
        let mut live_settings = settings.clone();
        live_settings.venues = venues.clone();
        live_settings.validate()?;
        let watch_sec = env_u64("ARB_WEB_LIVE_WATCH_SEC", settings.scan_interval_sec)?;
        Ok(Self {
            mode,
            venues,
            auto: selection.auto,
            market_slippage,
            options,
            settings: live_settings,
            ledger_path: std::env::var("ARB_LIVE_LEDGER")
                .unwrap_or_else(|_| "arb-live-ledger.jsonl".into()),
            watch_sec,
            rates,
        })
    }
}

/// 实盘启动没连上时多久重试一次。不用退避到很长：交易所维护结束后应尽快恢复规则轮；
/// 每次重试只是每家一两次只读请求。
const LIVE_RETRY: Duration = Duration::from_secs(60);

/// 连接全部实盘场所并组装交易台。**全有或全无**：任何一家失败就整体失败（已连上的券商随之丢弃、
/// 释放意图日志锁），由调用方决定是退出还是稍后重试。
async fn connect_live(
    plan: &LivePlan,
    client: &reqwest::Client,
    alerts: Arc<Alerter>,
) -> Result<LiveDesk> {
    let LivePlan {
        mode,
        venues,
        auto,
        market_slippage,
        options,
        settings: live_settings,
        ledger_path,
        watch_sec,
        rates,
    } = plan.clone();
    let apis = build_all(&live_settings, client);
    let brokers = live_connect::connect(client, &venues, &options)
        .await
        .context("连接实盘账户失败")?;
    let ledger = Arc::new(Ledger::open(&ledger_path).await?);
    // 启动时先把台账里没有终态的订单对到券商的记录上：上次进程若死在「意图已落盘、终态未落盘」
    // 之间，那张单永远停在 Pending，对账会把它报成永远消不掉的不一致，所有开仓与规则被拒。
    match desk::resolve_pending_orders(&ledger, &brokers).await {
        Ok(resolution) if resolution.is_empty() => {}
        Ok(resolution) => {
            warn!(
                resolved = resolution.resolved.len(),
                still_open = resolution.still_open.len(),
                unknown = resolution.unknown.len(),
                filled_unrecorded = resolution.filled_unrecorded.len(),
                "启动时核实了台账里没有终态的订单"
            );
            if !resolution.filled_unrecorded.is_empty() {
                alerts.notify_always(format!(
                    "⚠️ 启动核实：{} 张订单在上次中断时已经成交、但台账没记（{}）。\
                     仓位记录里没有这些成交，对账会报持仓不符：请对照各场所账户人工处理。",
                    resolution.filled_unrecorded.len(),
                    resolution.filled_unrecorded.join("、")
                ));
            }
        }
        Err(error) => warn!("启动时核实台账里的订单失败（对账会如实报出）：{error:#}"),
    }
    let pause_file = pause::path_for(Path::new(&ledger_path));
    let initial_pause = pause::load(&pause_file);
    if initial_pause.paused {
        let reason = initial_pause.reason.clone().unwrap_or_default();
        warn!(%reason, "开新仓处于暂停状态（来自上次运行）：发 /resume 恢复");
        alerts.notify_always(format!(
            "⏸ 启动时开新仓仍处于暂停状态：{reason}。发 /resume 恢复。"
        ));
    }
    info!(
        mode = ?mode,
        venues = %venues.iter().map(|v| v.as_str()).collect::<Vec<_>>().join(","),
        auto = auto,
        ledger = %ledger_path,
        "实盘交易台已连接"
    );
    if mode == LiveMode::Trade && watch_sec == 0 {
        warn!("ARB_WEB_LIVE_WATCH_SEC=0：实盘持仓规则不会自动执行，只能在页面上手动跑");
    }
    Ok(LiveDesk {
        armed: std::sync::Mutex::new(None),
        mode,
        by_venue: crate::api_map(&apis),
        apis,
        venues,
        auto_venues: auto,
        settings: live_settings,
        brokers,
        ledger,
        market_slippage,
        lock: Mutex::new(()),
        last_round: RwLock::new(None),
        last_reconcile: RwLock::new(None),
        funding_cache: Mutex::new(HashMap::new()),
        external_seen: std::sync::Mutex::new(HashMap::new()),
        add_seen: std::sync::Mutex::new(HashMap::new()),
        pnl_attempts: std::sync::Mutex::new(HashMap::new()),
        fee_checked: std::sync::Mutex::new(std::collections::HashSet::new()),
        divergence_sig: std::sync::Mutex::new(String::new()),
        leg_state_cache: Mutex::new(HashMap::new()),
        exit_watched: std::sync::RwLock::new(Arc::default()),
        alerts,
        opens_paused: std::sync::atomic::AtomicBool::new(initial_pause.paused),
        pause_file,
        pause_state: std::sync::Mutex::new(initial_pause),
        exit_retries: desk::ExitRetries::default(),
        naked_since: std::sync::Mutex::new(HashMap::new()),
        dirty_rounds: std::sync::atomic::AtomicU32::new(0),
        dirty_alerted: std::sync::atomic::AtomicBool::new(false),
        watch_sec,
        rates,
    })
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .with_context(|| format!("{name} 必须是非负整数，收到 {raw:?}")),
        _ => Ok(default),
    }
}

/// 订单相关规则：平仓 / 减仓 / 续上中断建仓。补保证金和「继续持有」不是订单播报。
fn is_order_report(report: &MonitorReport) -> bool {
    report.resumed
        || matches!(
            report.status,
            PositionStatus::Opening | PositionStatus::Closing | PositionStatus::Unwinding
        )
        || (report.executed
            && matches!(
                report.status,
                PositionStatus::Closed | PositionStatus::Unwound
            ))
        || matches!(
            report.evaluation.as_ref().map(|e| &e.action),
            Some(arb_exec::Action::Close { .. } | arb_exec::Action::Trim { .. })
        )
}

/// 一笔仓位这一轮执行了规则（没出错、不是续上建仓）时推送的「✅ 规则已执行」。平仓限价保护先发的那条腿没成交、
/// 两条腿都没动（`exit_held`）不推：什么都没变，而且这种情况可能每轮都有。
fn executed_alert(report: &MonitorReport) -> Option<String> {
    (report.executed && report.exit_held.is_none()).then(|| {
        format!(
            "✅ 规则已执行：{} {} → {:?}。{}",
            report.position_id,
            report.symbol,
            report.status,
            report.note.as_deref().unwrap_or_default()
        )
    })
}

/// 逐字节比较、不提前返回：别让响应时间泄露令牌前缀对了几位。
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// 后台规则轮：每个周期在**独立任务**里跑一轮 —— 一轮里的 panic 只作废这一轮、不会带走整个循环
/// （任务一死，持仓就再也没人自动平仓 / 减仓，而进程照常在线）。停机中不再开始新的一轮。
async fn rounds_forever<F, Fut>(
    interval: Duration,
    label: &'static str,
    alerts: Option<Arc<Alerter>>,
    round: F,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        tokio::time::sleep(interval).await;
        if crate::shutdown::is_draining() {
            continue;
        }
        if let Err(error) = tokio::spawn(round()).await {
            crate::metrics::count_task_panic();
            error!(%error, label, "规则轮异常退出（panic）：这一轮作废，下一轮照常");
            if let Some(alerts) = &alerts {
                alerts.notify(
                    &format!("round-panic:{label}"),
                    format!("❌ {label}规则轮异常退出（panic），已自动继续下一轮；请查日志。"),
                );
            }
        }
    }
}

fn fail(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// 看板手动操作（补仓、平仓）被拒时把原因写进日志：拒绝只回给页面的话，页面没看到提示
/// （某次实盘 补仓：提示在持仓区顶部、按钮在下面）就无从查起。
async fn log_rejection(action: &str, position_id: &str, response: Response) -> Response {
    if response.status().is_success() {
        return response;
    }
    let (parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 256 * 1024)
        .await
        .unwrap_or_default();
    let reason = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| value.get("error")?.as_str().map(str::to_string))
        .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
    warn!(
        position = %position_id,
        status = %parts.status,
        %reason,
        "看板手动{action}被拒"
    );
    Response::from_parts(parts, axum::body::Body::from(bytes))
}

fn fail_with(status: StatusCode, message: &str, extra: serde_json::Value) -> Response {
    let mut body = json!({ "error": message });
    if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            body.insert(key.clone(), value.clone());
        }
    }
    (status, Json(body)).into_response()
}

fn busy() -> Response {
    fail(
        StatusCode::CONFLICT,
        "这张交易台上有一笔操作（或一轮规则监控）正在进行，等它结束再试",
    )
}

/// 把下单 / 平仓 / 一轮规则放进独立任务里跑，handler 只等它的结果。
///
/// 请求处理 future 会在客户端断开（关掉页面、反向代理超时、`curl` 被 Ctrl-C）时被**直接
/// 丢弃**，丢弃发生在任意一个 `.await` 上：下单序列正好停在两条腿之间，就是一条没人管的
/// 裸腿。独立任务不随请求取消；它持有的交易台锁也要等它真正结束才释放，优雅停机据此排空。
async fn detached<F>(op: crate::metrics::Op, work: F) -> Response
where
    F: Future<Output = Response> + Send + 'static,
{
    if crate::shutdown::is_draining() {
        crate::metrics::count_op(op, false);
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "服务正在停机：不再受理新的下单、平仓和规则轮，重启完成后再试",
        );
    }
    let response = match tokio::spawn(work).await {
        Ok(response) => response,
        Err(error) => {
            error!(%error, "交易操作的任务异常退出");
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "操作中途异常退出（panic）：以台账与对账结果为准，不要直接重试",
            )
        }
    };
    crate::metrics::count_op(op, response.status().is_success());
    response
}

// ───────────────────────────── 请求 ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Paper,
    Live,
}

/// 自动交易补仓：往已有的价差仓位 `position_id` 上再加一笔。请求结构与开仓相同（场所、名义、
/// 杠杆、确认），闸门也相同（对账干净、现拉盘口重算深度与净收益、单笔上限、当日亏损、暂停开仓、
/// 保证金够不够），只是不新开仓位，而是把成交并进原来那笔（见 [`arb_exec::Executor::increase`]）。
/// 补完后这笔的单腿名义受 `max_total` 与单笔上限约束；补仓不改这笔仓位的规则。
pub(crate) async fn add_for_auto(
    state: Arc<AppState>,
    body: OpenBody,
    position_id: String,
    max_total: Decimal,
) -> (StatusCode, serde_json::Value) {
    let response = detached(crate::metrics::Op::Open, async move {
        run_add(&state, &body, &position_id, max_total).await
    })
    .await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap_or_default();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

async fn run_add(
    state: &AppState,
    body: &OpenBody,
    position_id: &str,
    max_total: Decimal,
) -> Response {
    let daily_default = if body.mode == Mode::Live {
        live_daily_default(state).await
    } else {
        Ok(Decimal::ZERO)
    };
    let mut request = match read_open(state, body, daily_default) {
        Ok(request) => request,
        Err(message) => return fail(StatusCode::BAD_REQUEST, &message),
    };
    // 补仓不改这笔仓位的规则；规则闸门（止盈……）是开仓时的事。收敛目标例外：拿到仓位以后按它自己的
    // `basis_exit_pct` 填回去（见下面的纸面 / 实盘两条路），保本线才按这笔仓位真正的平仓点算。
    request.rules = TaskRules::default();
    let max_total = limits_from(&state.settings)
        .max_position_usdt
        .min(max_total);
    match body.mode {
        Mode::Paper => {
            let Some(snapshot) = state.cache.get().await else {
                return fail(StatusCode::SERVICE_UNAVAILABLE, "首轮扫描尚未完成");
            };
            if snapshot.age().as_secs() > state.settings.scan_interval_sec * 3 {
                return fail(
                    StatusCode::CONFLICT,
                    "快照已过期（后台扫描可能卡住了）：不按陈旧行情补仓",
                );
            }
            let paper = &state.trade.paper;
            let Ok(_guard) = paper.lock.try_lock() else {
                return busy();
            };
            let (_, existing, executor) = match paper.executor(&snapshot.report).await {
                Ok(parts) => parts,
                Err(error) => {
                    return fail(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}"));
                }
            };
            let mut position = match desk::add_target(&existing, position_id, &request, max_total) {
                Ok(position) => position,
                Err(message) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &message),
            };
            if let Some(leverage) = position.leverage {
                request.leverage = leverage;
            }
            request.margin_mode = position.margin_mode;
            request.rules.basis_exit_pct = position.rules.basis_exit_pct;
            let prepared = match desk::prepare(
                &snapshot.report,
                &paper.by_venue,
                &request,
                existing.len().saturating_sub(1),
                &limits_from(&state.settings),
                LeveragePolicy::CapToPair,
            )
            .await
            {
                Ok(prepared) => prepared,
                Err(error) => {
                    return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}"));
                }
            };
            info!(position = %position_id, symbol = %position.symbol, "看板纸面补仓");
            match desk::execute_add(&executor, &prepared, &mut position).await {
                Ok(added) => Json(json!({
                    "mode": "paper",
                    "added": added,
                    "prepared": prepared,
                    "position": position,
                }))
                .into_response(),
                Err(error) => fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("补仓中断：{error:#}；以台账为准"),
                ),
            }
        }
        Mode::Live => {
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            if live.opens_paused.load(std::sync::atomic::Ordering::SeqCst) {
                return fail(
                    StatusCode::LOCKED,
                    "开新仓已被暂停（Telegram /pause）：补仓同样暂停。在 Telegram 里发 /resume 恢复",
                );
            }
            if live.mode != LiveMode::Trade {
                return fail(
                    StatusCode::FORBIDDEN,
                    "实盘是只读模式（ARB_WEB_LIVE=readonly）：不能补仓",
                );
            }
            let confirmed = body
                .confirm
                .as_deref()
                .is_some_and(|confirm| confirm.trim().eq_ignore_ascii_case(&request.base));
            if !confirmed {
                return fail(
                    StatusCode::BAD_REQUEST,
                    &format!(
                        "实盘补仓需要二次确认：confirm 必须填合约名 {}",
                        request.base
                    ),
                );
            }
            // 价差监控发起的（自动交易）：第一腿下单前按实时盘口再核一次，同开仓。
            let monitor =
                (body.from_monitor && body.maker.is_none()).then_some(state.rh_spread.as_ref());
            add_live(
                live,
                request,
                position_id,
                max_total,
                monitor,
                body.entry_gate,
            )
            .await
        }
    }
}

/// 看板手动补仓：页面只给仓位 id 和这次每条腿补多少名义。合约、方向、杠杆、保证金模式一律取台账里
/// 这笔仓位记下的，页面不用（也不能）另填；其余闸门与自动补仓同一条路（[`run_add`]）。
#[derive(Debug, Deserialize)]
pub struct AddBody {
    mode: Mode,
    position_id: String,
    /// 这次每条腿补的名义（USDT）。
    size: String,
    /// 实盘补仓的二次确认：必须等于仓位 id（页面点「确认补仓」时自动带上）。
    confirm: Option<String>,
}

pub async fn api_trade_add(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<AddBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    let position_id = body.position_id.trim().to_string();
    let response = detached(crate::metrics::Op::Open, async move {
        run_manual_add(&state, &body).await
    })
    .await;
    log_rejection("补仓", &position_id, response).await
}

#[cfg(test)]
impl AddBody {
    pub(crate) fn new(mode: Mode, position_id: &str, size: &str, confirm: Option<&str>) -> Self {
        Self {
            mode,
            position_id: position_id.into(),
            size: size.into(),
            confirm: confirm.map(str::to_string),
        }
    }
}

pub(crate) async fn run_manual_add(state: &AppState, body: &AddBody) -> Response {
    let position_id = body.position_id.trim();
    if position_id.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "缺少 position_id");
    }
    if body.mode == Mode::Live && body.confirm.as_deref().map(str::trim) != Some(position_id) {
        return fail(
            StatusCode::BAD_REQUEST,
            "实盘补仓需要二次确认：confirm 必须填仓位 id",
        );
    }
    let size = match parse_decimal(body.size.trim()) {
        Some(size) if size > Decimal::ZERO => size,
        _ => {
            return fail(
                StatusCode::BAD_REQUEST,
                &format!("补仓名义必须是正数（USDT），收到 {:?}", body.size),
            );
        }
    };
    let ledger = match body.mode {
        Mode::Paper => match state.trade.paper.ledger().await {
            Ok(ledger) => ledger,
            Err(error) => return fail(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}")),
        },
        Mode::Live => match state.trade.live() {
            Ok(live) => Arc::clone(&live.ledger),
            Err(response) => return response.into_response(),
        },
    };
    let position = match ledger.replay().await {
        Ok((replayed, _)) => replayed
            .exposed()
            .into_iter()
            .find(|position| position.id == position_id)
            .cloned(),
        Err(error) => return fail(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let Some(position) = position else {
        return fail(
            StatusCode::NOT_FOUND,
            &format!("台账里没有持仓中的 {position_id}"),
        );
    };
    let (Some(long), Some(short)) = (position.long.as_ref(), position.short.as_ref()) else {
        return fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!("{position_id} 不是两腿齐全的仓位，不能补仓"),
        );
    };
    let symbol = format!("{}/{}", position.symbol.base, position.symbol.quote);
    let mut open = auto_open_body(
        body.mode,
        &symbol,
        long.venue,
        short.venue,
        size,
        // 实盘一律用这笔仓位记下的杠杆（没记就拒绝，见 add_live）；这里只是填个合法值。
        position.leverage.unwrap_or(Decimal::ONE),
        position.margin_mode,
        &TaskRules::default(),
    );
    // 页面发起的：同页面开仓，不要求价差监控里有这一行（REST 现拉盘口照样重算）。
    open.from_monitor = false;
    info!(
        position = %position_id,
        symbol = %symbol,
        size = %size,
        mode = ?body.mode,
        "看板手动补仓请求"
    );
    // 补完的单腿名义只受单笔上限（ARB_MAX_POSITION_USDT）约束，run_add 里取两者较小。
    run_add(state, &open, position_id, Decimal::MAX).await
}

async fn add_live(
    live: &LiveDesk,
    mut request: OpenRequest,
    position_id: &str,
    max_total: Decimal,
    monitor: Option<&crate::rh_spread::Monitor>,
    gate: Option<EntryGate>,
) -> Response {
    if !live.brokers.contains_key(&request.long) || !live.brokers.contains_key(&request.short) {
        return fail(StatusCode::BAD_REQUEST, "两腿必须是实盘已连接的场所");
    }
    let Ok(_guard) = live.lock.try_lock() else {
        return busy();
    };
    // 对账与现扫并发（都只读）；对账不干净时现扫结果丢掉，照样拒单。
    let (reconciled, report) =
        tokio::join!(live.reconcile_adopting(), live.scan_for_pair(&request));
    let reconciliation = match reconciled {
        Ok((reconciliation, _, _)) => reconciliation,
        Err(error) => return fail(StatusCode::CONFLICT, &format!("{error:#}")),
    };
    if reconciliation.blocks_open(
        &request.base,
        request.quote.as_deref(),
        &[request.long, request.short],
    ) {
        return fail_with(
            StatusCode::CONFLICT,
            &format!(
                "对账不一致（冻结 {}），拒绝补这个币：台账之外的持仓或挂单要先人工处理",
                reconciliation.frozen_scope().join("、")
            ),
            json!({ "reconciliation": reconciliation }),
        );
    }
    let exposed: Vec<PairPosition> = match live.ledger.replay().await {
        Ok((replayed, _)) => replayed.exposed().into_iter().cloned().collect(),
        Err(error) => return fail(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let mut position = match desk::add_target(&exposed, position_id, &request, max_total) {
        Ok(position) => position,
        Err(message) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &message),
    };
    // 杠杆与保证金模式一律用这笔仓位记下的：券商下单前核对，一致时不改，原来的仓位不受影响。
    let Some(leverage) = position.leverage else {
        return fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "这笔仓位没记杠杆，补仓无法保证不改动原仓位的杠杆，不补",
        );
    };
    request.leverage = leverage;
    request.margin_mode = position.margin_mode;
    // 保本线、盈利闸门、实时复核都按这笔仓位自己的收敛目标算（`run_add` 把规则清成了默认值，目标就成了 0：
    // 目标为正的仓位补进去的那块，收敛到目标时一定亏；目标为负的则永远补不进）。其余规则照旧不带：补仓不改仓位的规则。
    request.rules.basis_exit_pct = position.rules.basis_exit_pct;
    let limits = limits_from(&live.settings);
    let legs = [(request.long, request.size), (request.short, request.size)];
    let (prepared, collateral, tradable, ()) = tokio::join!(
        desk::prepare(
            &report,
            &live.by_venue,
            &request,
            exposed.len().saturating_sub(1),
            &limits,
            LeveragePolicy::Strict,
        ),
        desk::check_legs_collateral(&live.brokers, &legs, request.leverage),
        // 休市、维护的场所不补：两单只有一单下得去就是裸腿。
        desk::check_legs_tradable(
            &live.brokers,
            &position.symbol,
            [request.long, request.short]
        ),
        // 只读预热：补仓的两单下单时就不必再各核对一次杠杆（核对的是这笔仓位记下的杠杆）。
        desk::warm_legs(&live.brokers, &report, &request),
    );
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}")),
    };
    if let Err(error) = collateral.and(tradable) {
        return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}"));
    }
    // 现拉的盘口到这里已经过了几百毫秒（对账、现扫、保证金核对）：按价差监控的实时盘口再核一次，
    // 两腿现在可成交价差不够第二腿的保本线就不补（422：自动交易记「补仓前检查没通过」、冷却，不关自动交易）。
    if let (Some(monitor), Some(floor)) = (monitor, prepared.plan.hedge_floor()) {
        let view = monitor.view().await;
        match live_entry_check_with(
            &view,
            &request.base,
            request.long,
            request.short,
            floor,
            prepared.size_usdt,
            Utc::now(),
            gate,
            prepared.plan.expected_entry_pct(),
        ) {
            Ok(Some(entry)) => info!(
                position = %position_id,
                entry_pct = %entry,
                floor_pct = %(floor * Decimal::ONE_HUNDRED).round_dp(5),
                "补仓第一腿下单前实时盘口复核通过"
            ),
            Ok(None) => warn!(
                position = %position_id,
                monitor_size = %view.size_usdt,
                size = %prepared.size_usdt,
                "价差监控估算名义比这笔小、逐档估算也没有这么大的档：补仓前按最大的一档核过了（偏乐观），没核全"
            ),
            Err(reason) => {
                warn!(position = %position_id, %reason, "补仓第一腿下单前实时盘口复核没过，不补");
                return fail(StatusCode::UNPROCESSABLE_ENTITY, &reason);
            }
        }
    }
    warn!(
        position = %position_id,
        symbol = %position.symbol,
        size = %prepared.size_usdt,
        "看板实盘补仓"
    );
    let outcome = desk::execute_add(&live.executor(), &prepared, &mut position).await;
    let after = live.reconcile().await.map_err(|error| format!("{error:#}"));
    match &outcome {
        Ok(true) => live.alerts.notify_order(format!(
            "➕ 实盘补仓：{} {} 多 {} / 空 {}，补单腿 {} USDT。{}",
            position_id,
            position.symbol,
            request.long,
            request.short,
            prepared.size_usdt.round_dp(2),
            position.note.as_deref().unwrap_or_default()
        )),
        Ok(false) => live.alerts.notify_order(format!(
            "ℹ️ 实盘补仓没补成：{} {}。{}",
            position_id,
            position.symbol,
            position.note.as_deref().unwrap_or_default()
        )),
        Err(error) => live.alerts.notify_order(format!(
            "❌ 实盘补仓中断：{} {}：{error}。以台账与对账为准，不要重复提交。",
            position_id, position.symbol
        )),
    };
    match outcome {
        Ok(added) => Json(json!({
            "mode": "live",
            "added": added,
            "prepared": prepared,
            "position": position,
            "reconciliation": after.as_ref().ok(),
            "reconciliation_error": after.as_ref().err(),
        }))
        .into_response(),
        Err(error) => fail_with(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("补仓中断：{error:#}。以台账与对账结果为准，不要重复提交"),
            json!({
                "position_id": position_id,
                "reconciliation": after.as_ref().ok(),
                "reconciliation_error": after.as_ref().err(),
            }),
        ),
    }
}

#[derive(Debug, Deserialize)]
pub struct OpenBody {
    #[serde(default)]
    margin_mode: arb_exec::MarginMode,
    mode: Mode,
    /// `BASE/QUOTE`。
    symbol: String,
    long: String,
    short: String,
    size: String,
    leverage: String,
    /// 当日已实现盈亏（USDT，亏损为负）。实盘必填。
    daily_pnl: Option<String>,
    /// `funding`（默认）或 `spread`。
    view: Option<String>,
    #[serde(flatten)]
    rules: RuleFields,
    /// 实盘开仓的二次确认：必须等于合约 base。
    confirm: Option<String>,
    /// 「相对正常基差」的价差单（见 [`arb_exec::desk::OpenRequest::relative_to_normal`]）。
    /// **只有自动交易能设**：`skip` 让 HTTP 请求体里的同名字段被忽略，页面与外部调用永远是 `false`。
    #[serde(skip)]
    relative_to_normal: bool,
    /// 价差单先下哪条腿（只有自动交易能设；没设 = 先下费率明显低的那条，见
    /// [`arb_exec::preflight::LegOrder`]）。
    #[serde(skip)]
    leg_order: Option<arb_exec::preflight::LegOrder>,
    /// 价差监控里的一行发起的（自动交易、Telegram 提醒下的「下单…」）：第一腿下单前按价差监控的实时
    /// 盘口再核一次（[`live_entry_check`]）。页面与外部调用永远是 `false`。
    #[serde(skip)]
    from_monitor: bool,
    /// 其中一条腿用只挂单开仓（只有自动交易能设，页面与外部调用永远是 `None`）：见
    /// [`arb_exec::desk::MakerLeg`]。
    #[serde(skip)]
    maker: Option<arb_exec::desk::MakerLeg>,
    /// 按盘口深度定单笔（只有自动交易能设，页面与外部调用永远是 `None`）：`size` 当作上限，见
    /// [`arb_exec::sizing`]。
    #[serde(skip)]
    depth_fit: Option<arb_exec::sizing::DepthFit>,
    /// 强制补腿的最差锁定价差（%，只有自动交易能设，页面与外部调用永远是 `None`）：见
    /// [`arb_exec::desk::OpenRequest::force_hedge_pct`]。
    #[serde(skip)]
    force_hedge_pct: Option<Decimal>,
    /// 自动交易的入场门槛（只有自动交易能设，页面与外部调用永远是 `None`）：第一腿下单前的实时复核除了保本线，
    /// 也按它再核一次（[`live_entry_check_with`]）。
    #[serde(skip)]
    entry_gate: Option<EntryGate>,
}

/// 自动交易挑信号时用的入场门槛，第一腿下单前按实时盘口再核一次：信号保持、准备的几秒里价差可能缩到只够保本线、
/// 不够这两道门槛（10-31 审查：实时复核以前只核保本线）。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct EntryGate {
    /// 可成交价差扣往返手续费后至少剩这么多（%）。
    pub min_edge_pct: Option<Decimal>,
    /// 两腿里有薄盘口交易所时，可成交价差至少这么多（%）。
    pub thin_min_entry_pct: Option<Decimal>,
}

/// [`auto_open_body`]，再带上自动交易的入场门槛（见 [`EntryGate`]）。
pub(crate) fn with_entry_gate(mut body: OpenBody, gate: Option<EntryGate>) -> OpenBody {
    body.entry_gate = gate;
    body
}

/// 开仓用的当日已实现盈亏：手填优先，但实盘手填的只能比台账更差、不能更好 —— 页面早上预填的 0、或外部调用
/// 随手填的数，不能把台账里已经算得出来的当日亏损盖掉、绕过当日亏损闸门。台账算不出来时（有仓位盈亏未知）
/// 才完全按手填的；实盘没填就用台账的，台账算不出来就报错（不知道的不按 0 算）。纸面不看台账。
fn effective_daily(
    typed: Option<&str>,
    live: bool,
    ledger: Result<Decimal, String>,
) -> Result<Decimal, String> {
    match typed.map(str::trim) {
        Some(raw) if !raw.is_empty() => {
            let typed = parse_decimal(raw)
                .ok_or_else(|| format!("daily_pnl 必须是十进制数，收到 {raw:?}"))?;
            Ok(match (live, ledger) {
                (true, Ok(ledger)) => typed.min(ledger),
                _ => typed,
            })
        }
        _ if live => ledger,
        _ => Ok(Decimal::ZERO),
    }
}

fn read_open(
    state: &AppState,
    body: &OpenBody,
    daily_default: Result<Decimal, String>,
) -> Result<OpenRequest, String> {
    let Some((base, quote)) = body.symbol.split_once('/') else {
        return Err("symbol 必须是 BASE/QUOTE".into());
    };
    if base.trim().is_empty() || quote.trim().is_empty() {
        return Err("symbol 必须是 BASE/QUOTE".into());
    }
    let long = Venue::parse(&body.long).ok_or("未知的做多场所")?;
    let short = Venue::parse(&body.short).ok_or("未知的做空场所")?;
    // 开仓、补仓（自动与手动）都经过这里：上市换算口径不同的两腿、上市前 / 盘前合约一律不做。
    if let Some(reason) = crate::preipo::open_block(base.trim(), long, short) {
        return Err(reason);
    }
    let size = crate::parse_bounded(
        Some(body.size.as_str()),
        Decimal::ZERO,
        Decimal::from(crate::strategy::MAX_PLAN_SIZE_USDT),
        "size",
    )?;
    if size <= Decimal::ZERO {
        return Err("单腿名义必须大于 0".into());
    }
    let leverage = crate::parse_leverage(Some(body.leverage.as_str()), state.settings.leverage)?;
    let daily_pnl = effective_daily(
        body.daily_pnl.as_deref(),
        body.mode == Mode::Live,
        daily_default,
    )?;
    let view = match body.view.as_deref().unwrap_or("funding") {
        view @ ("funding" | "spread") => view.to_string(),
        other => return Err(format!("view 只能是 funding 或 spread，收到 {other:?}")),
    };
    Ok(OpenRequest {
        margin_mode: body.margin_mode,
        base: base.trim().to_ascii_uppercase(),
        quote: Some(quote.trim().to_ascii_uppercase()),
        long,
        short,
        size,
        leverage,
        daily_pnl,
        view,
        depth_levels: DEPTH_LEVELS,
        rules: body.rules.parse()?,
        relative_to_normal: body.relative_to_normal,
        maker: body.maker,
        // 价差单默认先下费率低的那条腿；自动交易可在面板上关掉（见 `rh_auto::Settings::cheap_leg_first`）。
        leg_order: body
            .leg_order
            .unwrap_or(arb_exec::preflight::LegOrder::CheapFirst),
        depth_fit: body.depth_fit,
        force_hedge_pct: body.force_hedge_pct,
    })
}

/// 一笔仓位的全部规则（开仓时与开仓后改规则共用同一份解析）。字段都是字符串：空 / `off` = 关闭。
#[derive(Debug, Default, Deserialize)]
pub struct RuleFields {
    /// 费差自动平仓门槛（年化 %）。
    min_funding_apr: Option<String>,
    /// 爆仓保护：强平距离（%）低于它两腿等比例减仓。
    liq_protection: Option<String>,
    size_mismatch: Option<String>,
    /// 基差收敛平仓目标（%）。价差套利用。
    basis_exit: Option<String>,
    /// 止盈：含资金费的净盈利（USDT）达到它就平仓。
    take_profit: Option<String>,
    /// 止盈的盘口核对门槛（USDT）：标记价达到止盈后，按盘口平掉为正且 ≥ 它就平；0 = 为正即平，空 = 同止盈金额。
    #[serde(default)]
    take_profit_book_min: Option<String>,
    /// 自动加保证金：触发的强平距离（%）与累计上限（USDT），成对。
    auto_margin: Option<String>,
    auto_margin_max: Option<String>,
    /// 超时退出：持仓小时数与按盘口可接受的最大亏损（USDT），成对。
    #[serde(default)]
    timeout_hours: Option<String>,
    #[serde(default)]
    timeout_max_loss: Option<String>,
    /// 止损：按盘口平掉亏到 −它（USDT）就整笔平仓。
    #[serde(default)]
    stop_loss: Option<String>,
}

impl RuleFields {
    fn parse(&self) -> Result<TaskRules, String> {
        let min_funding_apr = crate::parse_optional_bounded(
            self.min_funding_apr.as_deref(),
            None,
            Decimal::from(1000u32),
            "min_funding_apr",
        )?
        .map(|pct| pct / Decimal::ONE_HUNDRED);
        let timeout_hours = crate::parse_optional_bounded(
            self.timeout_hours.as_deref(),
            None,
            arb_exec::monitor::MAX_TIMEOUT_HOURS,
            "timeout_hours",
        )?;
        let timeout_max_loss_usdt = crate::parse_optional_bounded(
            self.timeout_max_loss.as_deref(),
            None,
            Decimal::from(crate::strategy::MAX_PLAN_SIZE_USDT),
            "timeout_max_loss",
        )?;
        if timeout_hours.is_some() != timeout_max_loss_usdt.is_some() {
            return Err("timeout_hours 与 timeout_max_loss 必须同时设置或同时关闭".into());
        }
        let stop_loss_usdt = crate::parse_optional_bounded(
            self.stop_loss.as_deref(),
            None,
            Decimal::from(crate::strategy::MAX_PLAN_SIZE_USDT),
            "stop_loss",
        )?;
        let take_profit_book_min_usdt = crate::parse_optional_bounded(
            self.take_profit_book_min.as_deref(),
            None,
            Decimal::from(1_000_000u32),
            "take_profit_book_min",
        )?;
        Ok(TaskRules {
            min_funding_apr,
            timeout_hours,
            timeout_max_loss_usdt,
            stop_loss_usdt,
            take_profit_book_min_usdt,
            liq_protection_pct: crate::parse_optional_bounded(
                self.liq_protection.as_deref(),
                None,
                Decimal::from(100u32),
                "liq_protection",
            )?,
            size_mismatch_pct: crate::parse_optional_bounded(
                self.size_mismatch.as_deref(),
                None,
                Decimal::from(100u32),
                "size_mismatch",
            )?,
            basis_exit_pct: crate::parse_basis_exit(self.basis_exit.as_deref())?,
            ..crate::parse_extra_rules(
                self.take_profit.as_deref(),
                self.auto_margin.as_deref(),
                self.auto_margin_max.as_deref(),
            )?
        })
    }
}

// ───────────────────────────── 开仓后改规则 ─────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RulesBody {
    mode: Mode,
    position_id: String,
    /// **整套替换**：没给 / 空 / `off` 的规则就是关闭。页面总是把当前全部规则一起发过来。
    #[serde(flatten)]
    rules: RuleFields,
    /// 新规则一生效就会触发（平仓 / 减仓 / 补保证金 / 止盈已达标）时，要显式确认才写入。
    #[serde(default)]
    force: bool,
}

/// 新规则套在这笔仓位上、按当前行情评估的结果。
struct RulesCheck {
    /// 两腿里更近的当前强平距离（%）。任一腿算不出为 `None`。
    distance_pct: Option<Decimal>,
    /// 新规则一生效马上就会做的事（给确认用）。
    immediate: Option<String>,
}

/// 把候选规则套在仓位上评估一次：算当前强平距离（校验门槛用），并看有没有马上就会触发的。
/// 评估不带盘口核对（`NotFetched`）：平仓类规则能否真的平，要等后台那一轮现拉盘口再定，
/// 这里只回答「按当前标记价，它已经满足触发条件了吗」。
fn check_new_rules(
    report: &ScanReport,
    position: &PairPosition,
    rules: &TaskRules,
    states: &(
        Option<arb_exec::VenueLegState>,
        Option<arb_exec::VenueLegState>,
    ),
    funding: Option<Decimal>,
) -> RulesCheck {
    use arb_exec::monitor::{
        Action, Inputs, basis_exit_triggered, evaluate_full, take_profit_triggered,
    };
    let (Some(long_leg), Some(short_leg)) = (position.long.as_ref(), position.short.as_ref())
    else {
        return RulesCheck {
            distance_pct: None,
            immediate: None,
        };
    };
    let quote = |venue| crate::strategy::leg_snapshot(report, venue, &position.symbol);
    let (Some(long), Some(short)) = (quote(long_leg.venue), quote(short_leg.venue)) else {
        return RulesCheck {
            distance_pct: None,
            immediate: None,
        };
    };
    let mut candidate = position.clone();
    candidate.rules = rules.clone();
    let Some(evaluation) = evaluate_full(
        &candidate,
        long,
        short,
        Inputs {
            funding_usdt: funding,
            long_state: states.0.as_ref(),
            short_state: states.1.as_ref(),
            ..Inputs::default()
        },
    ) else {
        return RulesCheck {
            distance_pct: None,
            immediate: None,
        };
    };
    let observation = &evaluation.observation;
    let distance_pct = observation
        .long
        .distance_pct
        .zip(observation.short.distance_pct)
        .map(|(a, b)| a.min(b));
    let mut immediate = match &evaluation.action {
        Action::Hold => None,
        Action::Close { reason }
        | Action::Trim { reason, .. }
        | Action::AddMargin { reason, .. } => Some(reason.clone()),
    };
    // 没有退出盘口时，evaluate_full 可能选中补款或减仓；确认不能因此漏掉更高优先级的平仓候选。
    if let (Some(net), Some(target)) = (observation.net_with_funding_usdt, rules.take_profit_usdt)
        && take_profit_triggered(&candidate, long, short, funding)
    {
        use std::fmt::Write as _;
        let reason = immediate.get_or_insert_with(String::new);
        if !reason.is_empty() {
            reason.push('；');
        }
        write!(
            reason,
            "含资金费的净盈利（按标记价）{} USDT 已达止盈 {} USDT，下一轮核对盘口、扣除平仓费用后仍达标才平仓",
            net.round_dp(2),
            target.normalize(),
        )
        .expect("写入 String 不会失败");
    }
    if basis_exit_triggered(&candidate, long, short) {
        let reason = immediate.get_or_insert_with(String::new);
        if !reason.is_empty() {
            reason.push('；');
        }
        reason.push_str("标记价基差已经收敛到目标以内，下一轮核对盘口、扣除平仓费用后仍为正才平仓");
    }
    RulesCheck {
        distance_pct,
        immediate,
    }
}

/// 止盈按标记价达标但盘口核对没通过时的告警文本（同一笔 30 分钟一条，由告警冷却保证）。
fn take_profit_hold_alert(
    position_id: &str,
    symbol: &arb_core::Symbol,
    hold: &arb_exec::monitor::TakeProfitHold,
) -> String {
    let threshold = if hold.book_min_usdt == hold.target_usdt {
        String::new()
    } else if hold.book_min_usdt.is_zero() {
        "，盘口门槛为正即平".to_string()
    } else {
        format!("，盘口门槛 {} USDT", hold.book_min_usdt.normalize())
    };
    let book = match hold.book_net_usdt {
        Some(net) => format!("按盘口现在平仓预估 {} USDT{threshold}", net.round_dp(2)),
        None => "盘口核对不了".to_string(),
    };
    // 标记价与盘口之间差的钱花在哪：买卖价差、手续费几乎和平仓量成正比，只有「吃深度」那一项分批才可能省。
    let gap = hold
        .gap
        .map(|gap| format!("{}。", gap.describe()))
        .unwrap_or_default();
    format!(
        "ℹ️ 止盈未执行：{position_id} {symbol} 按标记价含资金费净额 {} USDT 已达目标 {} USDT，但{book}（含资金费 {} USDT、平仓手续费与穿价），继续持有。{gap}同一笔 30 分钟内不重复提醒。",
        hold.mark_net_usdt.round_dp(2),
        hold.target_usdt.normalize(),
        hold.funding_usdt.round_dp(2),
    )
}

/// 校验提示里的「开仓」说法换成改规则时的口径（同一份 `validate_rules`，开仓时说开仓、这里说当前）。
fn rules_wording(message: String) -> String {
    message
        .replace("开仓强平距离", "当前强平距离")
        .replace("开仓就会触发", "马上就会触发")
}

/// 改一笔已开仓位的规则。
pub async fn api_trade_rules(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RulesBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    detached(crate::metrics::Op::Rules, async move {
        run_rules(&state, &body).await
    })
    .await
}

async fn run_rules(state: &AppState, body: &RulesBody) -> Response {
    let position_id = body.position_id.trim();
    if position_id.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "缺少 position_id");
    }
    let rules = match body.rules.parse() {
        Ok(rules) => rules,
        Err(message) => return fail(StatusCode::BAD_REQUEST, &message),
    };
    let Some(snapshot) = state.cache.get().await else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, "首轮扫描尚未完成");
    };
    if snapshot.age().as_secs() > state.settings.scan_interval_sec * 3 {
        return fail(
            StatusCode::CONFLICT,
            "快照已过期：无法确认新规则会不会立即触发",
        );
    }
    match body.mode {
        Mode::Paper => {
            let paper = &state.trade.paper;
            let Ok(_guard) = paper.lock.try_lock() else {
                return busy();
            };
            let ledger = match paper.ledger().await {
                Ok(ledger) => ledger,
                Err(error) => {
                    return fail(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}"));
                }
            };
            let (_, brokers) = desk::paper_executor(
                &paper.by_venue,
                paper.fee_per_side,
                DEPTH_LEVELS,
                &ledger,
                &[],
                &[],
            );
            // 纸面不结算资金费，止盈按 0 算（与台账里的盈亏一致）。
            apply_rules(
                &ledger,
                &brokers,
                &snapshot.report,
                position_id,
                rules,
                body.force,
                (None, None),
                Some(Decimal::ZERO),
                "paper",
            )
            .await
        }
        Mode::Live => {
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            if live.mode != LiveMode::Trade {
                return fail(
                    StatusCode::FORBIDDEN,
                    "实盘是只读模式（ARB_WEB_LIVE=readonly）：规则会驱动真实的平仓、减仓和补保证金，不能改",
                );
            }
            let Ok(_guard) = live.lock.try_lock() else {
                return busy();
            };
            let (replayed, _) = match live.ledger.replay().await {
                Ok(replayed) => replayed,
                Err(error) => {
                    return fail(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("读不了实盘台账：{error}"),
                    );
                }
            };
            let Some(position) = replayed.positions.get(position_id).cloned() else {
                return fail(
                    StatusCode::NOT_FOUND,
                    &format!("台账里没有仓位 {position_id}"),
                );
            };
            // 风险确认现读保证金；启用止盈时也现查结算流水，不用页面缓存。
            let states = live.executor().leg_states(&position).await;
            let funding = if rules.take_profit_usdt.is_some() {
                live.position_funding(&position, true).await.total_usdt
            } else {
                None
            };
            apply_rules(
                &live.ledger,
                &live.brokers,
                &snapshot.report,
                position_id,
                rules,
                body.force,
                states,
                funding,
                "live",
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_rules(
    ledger: &Ledger,
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    report: &ScanReport,
    position_id: &str,
    rules: TaskRules,
    force: bool,
    states: (
        Option<arb_exec::VenueLegState>,
        Option<arb_exec::VenueLegState>,
    ),
    funding: Option<Decimal>,
    mode: &'static str,
) -> Response {
    let (replayed, _) = match ledger.replay().await {
        Ok(replayed) => replayed,
        Err(error) => {
            return fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("读不了台账：{error}"),
            );
        }
    };
    let Some(position) = replayed.positions.get(position_id) else {
        return fail(
            StatusCode::NOT_FOUND,
            &format!("台账里没有仓位 {position_id}"),
        );
    };
    if position.status != PositionStatus::Open || !position.is_hedged() {
        return fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "只有两腿都在的 Open 仓位能改规则",
        );
    }
    if rules == position.rules {
        return Json(json!({
            "mode": mode, "position": position, "changed": false, "forced": false,
        }))
        .into_response();
    }
    if let Err(reason) = arb_exec::margin::validate_rules(position.margin_mode, &rules) {
        return fail(StatusCode::UNPROCESSABLE_ENTITY, &reason);
    }
    let check = check_new_rules(report, position, &rules, &states, funding);
    if let Err(reason) = arb_exec::monitor::validate_rules_update(&rules, check.distance_pct) {
        return fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            &rules_wording(format!("规则不成立：{reason}")),
        );
    }
    if let (Some(what), false) = (&check.immediate, force) {
        return fail_with(
            StatusCode::CONFLICT,
            &format!("这组规则一生效就会触发：{what}。确认要这样，勾上「仍然保存」再提交"),
            json!({ "would_trigger": what }),
        );
    }
    match desk::update_rules(ledger, brokers, position_id, rules, check.distance_pct).await {
        Ok((position, changed)) => {
            if changed {
                warn!(position = %position_id, mode, rules = ?position.rules, "改了持仓规则");
            }
            Json(json!({
                "mode": mode,
                "position": position,
                "changed": changed,
                "forced": check.immediate.is_some(),
            }))
            .into_response()
        }
        Err(error) => fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            &rules_wording(format!("{error:#}")),
        ),
    }
}

// ───────────────────────────── 接口 ─────────────────────────────

/// 交易能力与模式。不需要令牌：不含任何账户信息。
pub async fn api_trade_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let trade = &state.trade;
    Json(json!({
        "auth_configured": trade.token.is_some(),
        "margin_modes": arb_exec::live_connect::SUPPORTED_VENUES.iter().map(|venue| {
            // 统一账户的币安 U 本位合约只有全仓。
            let modes = if arb_exec::live_connect::cross_only(*venue) {
                vec!["cross"]
            } else if arb_exec::margin::supports_cross(*venue) {
                vec!["isolated", "cross"]
            } else {
                vec!["isolated"]
            };
            (venue.as_str().to_string(), modes)
        }).collect::<std::collections::BTreeMap<_, _>>(),
        "paper": {
            "ledger": trade.paper.ledger_path,
            "watch_sec": trade.paper.watch_sec,
        },
        "live": trade.live_opt().map(|live| json!({
            "mode": live.mode,
            "venues": live.venues,
            "market_slippage": live.market_slippage.map(|value| value.to_string()),
            "ledger": live.ledger_path(),
            "watch_sec": live.watch_sec,
            // 哪些实盘场所接入了补保证金：自动加保证金两条腿都得在里面。
            "auto_margin_venues": live
                .venues
                .iter()
                .filter(|venue| live.brokers.get(venue).is_some_and(|broker| broker.supports_add_margin()))
                .collect::<Vec<_>>(),
        })),
        // 实盘开着但暂时没连上：给页面显示横幅。错误文本抹掉长十六进制串（地址、密钥）。
        "live_pending": trade.live_pending().map(|pending| json!({
            "mode": trade.live_mode(),
            "since": pending.since,
            "attempts": pending.attempts,
            "error": crate::alert::redact(&pending.error),
        })),
    }))
}

/// 出计划：走完全部校验与闸门，但不下单。
pub async fn api_trade_preview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<OpenBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    run_open(&state, &body, false).await
}

/// 开仓。
pub async fn api_trade_open(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<OpenBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    detached(crate::metrics::Op::Open, async move {
        run_open(&state, &body, true).await
    })
    .await
}

/// 台账算出来的当日（UTC）已实现盈亏；算不出来时给出原因。
async fn live_daily_default(state: &AppState) -> Result<Decimal, String> {
    let Some(live) = state.trade.live_opt() else {
        return Err("看板没有开启实盘".into());
    };
    let daily = live_daily(live).await?;
    daily.net_usdt.ok_or_else(|| {
        format!(
            "实盘必须填写当日已实现盈亏（亏损为负）：台账里今天结束的 {} 笔仓位里有 {} 笔没有盈亏记录（{}），算不出合计，不按 0 算",
            daily.closed,
            daily.unknown.len(),
            daily.unknown.join("、")
        )
    })
}

async fn live_daily(live: &LiveDesk) -> Result<desk::DailyPnl, String> {
    let (replayed, _) = live
        .ledger
        .replay()
        .await
        .map_err(|error| format!("读不了实盘台账：{error}"))?;
    Ok(desk::daily_realized(&replayed, Utc::now().date_naive()))
}

/// 当日（UTC）已实现盈亏：来自实盘台账，不联网。开仓表单据此提示，留空时服务端也用它。
pub async fn api_trade_daily(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    let live = match state.trade.live() {
        Ok(live) => live,
        Err(response) => return response.into_response(),
    };
    match live_daily(live).await {
        Ok(daily) => Json(json!({
            "daily": daily,
            "max_daily_loss_usdt": state.settings.max_daily_loss_usdt,
        }))
        .into_response(),
        Err(message) => fail(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

/// 自动交易（RH 价差）用的开仓请求：与页面下单同一个结构、同一条路径（[`run_open`]），
/// 所以对账、闸门、当日亏损、熔断、暂停开仓、二次确认、Telegram 通知一个不少。
#[allow(clippy::too_many_arguments)]
pub(crate) fn auto_open_body(
    mode: Mode,
    symbol: &str,
    long: Venue,
    short: Venue,
    size: Decimal,
    leverage: Decimal,
    margin_mode: arb_exec::MarginMode,
    rules: &TaskRules,
) -> OpenBody {
    let opt = |value: Option<Decimal>| value.map(|v| v.normalize().to_string());
    OpenBody {
        margin_mode,
        mode,
        symbol: symbol.to_string(),
        long: long.as_str().to_string(),
        short: short.as_str().to_string(),
        size: size.normalize().to_string(),
        leverage: leverage.normalize().to_string(),
        // 留空：实盘用台账算出来的当日已实现盈亏（算不出就拒绝，不按 0 算）。
        daily_pnl: None,
        view: Some("spread".into()),
        rules: RuleFields {
            min_funding_apr: None,
            liq_protection: opt(rules.liq_protection_pct),
            size_mismatch: opt(rules.size_mismatch_pct),
            basis_exit: opt(rules.basis_exit_pct),
            take_profit: opt(rules.take_profit_usdt),
            take_profit_book_min: opt(rules.take_profit_book_min_usdt),
            auto_margin: opt(rules.auto_margin_pct),
            auto_margin_max: opt(rules.auto_margin_max_usdt),
            timeout_hours: opt(rules.timeout_hours),
            timeout_max_loss: opt(rules.timeout_max_loss_usdt),
            stop_loss: opt(rules.stop_loss_usdt),
        },
        // 自动交易是用户在面板上显式开启的；这里照样走二次确认那道检查（填合约名）。
        confirm: symbol.split('/').next().map(str::to_string),
        relative_to_normal: false,
        leg_order: None,
        from_monitor: true,
        maker: None,
        depth_fit: None,
        force_hedge_pct: None,
        entry_gate: None,
    }
}

/// [`auto_open_body`]，再让 `maker` 指定的那条腿用只挂单开仓（只有自动交易会这么做）。挂单的单子一定是
/// 「相对正常基差」的价差单，所以顺手打开它。
pub(crate) fn with_maker(mut body: OpenBody, maker: arb_exec::desk::MakerLeg) -> OpenBody {
    body.maker = Some(maker);
    body.relative_to_normal = true;
    body
}

#[cfg(test)]
impl OpenBody {
    /// 按盘口深度定单笔的参数（自动交易专用）；没开就是 `None`。
    pub(crate) fn depth_fit(&self) -> Option<arb_exec::sizing::DepthFit> {
        self.depth_fit
    }

    /// 强制补腿的最差锁定价差（%）；没开就是 `None`。
    pub(crate) fn force_hedge_pct(&self) -> Option<Decimal> {
        self.force_hedge_pct
    }

    /// 自动交易带上的入场门槛（见 [`EntryGate`]）。
    pub(crate) fn entry_gate(&self) -> Option<EntryGate> {
        self.entry_gate
    }
}

/// [`auto_open_body`]，再开强制补腿（第二腿保本价内没吃满时按最差锁定价差 `-pct`% 追上，不回滚）。
pub(crate) fn with_force_hedge(mut body: OpenBody, pct: Option<Decimal>) -> OpenBody {
    body.force_hedge_pct = pct;
    body
}

/// [`auto_open_body`]，再按盘口深度定单笔（`size` 当作上限，面板上的「按盘口深度定单笔」）。
pub(crate) fn with_depth_fit(
    mut body: OpenBody,
    fit: Option<arb_exec::sizing::DepthFit>,
) -> OpenBody {
    body.depth_fit = fit;
    body
}

/// [`auto_open_body`]，再指定价差单先下哪条腿（面板上的「先下费率低的腿」）。
pub(crate) fn with_leg_order(mut body: OpenBody, order: arb_exec::preflight::LegOrder) -> OpenBody {
    body.leg_order = Some(order);
    body
}

/// [`live_entry_check`] 要求价差监控的快照不比这更旧（秒）。
const LIVE_CHECK_STALE_SEC: i64 = 2;

/// 价差监控发起的价差单，第一腿下单前的最后一道：在价差监控的实时盘口上（Lighter RH 每 50ms、
/// Arcus 每 200ms 推一次，比下单路径现拉的 REST 盘口新），这个方向两腿**现在**吃单的可成交价差，
/// 够不够第二腿实际用的保本线（[`arb_exec::Plan::hedge_floor`]）。不够就一张单都不下，返回原因。
///
/// - `floor` 是**小数**（0.0024 = 0.24%），监控的 `entry_pct` 是**百分数**（0.24）。
/// - 监控按 `view.size_usdt`（跟着自动交易的单腿名义）吃深度，比这笔大时估得更保守；比这笔小时
///   （按盘口放大的单、币安单笔不同）改用信号行逐档估算里够这么大的最小一档；没有这一档才不判断，
///   返回 `Ok(None)`（调用方记一条日志）。
/// - 拦不住第一腿成交之后那几秒盘口被吃空（某次实盘）：那由先下哪条腿与第二腿重试处理。
#[cfg(test)]
pub(crate) fn live_entry_check(
    view: &crate::rh_spread::View,
    base: &str,
    long: Venue,
    short: Venue,
    floor: Decimal,
    size_usdt: Decimal,
    now: chrono::DateTime<Utc>,
) -> Result<Option<Decimal>, String> {
    live_entry_check_with(view, base, long, short, floor, size_usdt, now, None, None)
}

/// 同 [`live_entry_check`]，再按自动交易的入场门槛（[`EntryGate`]）核一次。
#[allow(clippy::too_many_arguments)]
pub(crate) fn live_entry_check_with(
    view: &crate::rh_spread::View,
    base: &str,
    long: Venue,
    short: Venue,
    floor: Decimal,
    size_usdt: Decimal,
    now: chrono::DateTime<Utc>,
    gate: Option<EntryGate>,
    planned_entry_pct: Option<Decimal>,
) -> Result<Option<Decimal>, String> {
    // 按毫秒比（整秒截断时 2.999 秒都算 2 秒）；时钟回拨算出负的也不放行（年龄说不清的快照不能当新鲜的用）。
    let age_ms = view.updated_at.map(|at| (now - at).num_milliseconds());
    if age_ms.is_none_or(|age| !(0..=LIVE_CHECK_STALE_SEC * 1000).contains(&age)) {
        return Err(format!(
            "第一腿下单前的实时盘口复核没做成：价差监控的快照{}，不下单",
            age_ms.map_or_else(
                || "还没有".to_string(),
                |age| format!(" {:.1} 秒没更新", age as f64 / 1000.0)
            )
        ));
    }
    let line = view
        .lines
        .iter()
        .find(|line| {
            line.base.eq_ignore_ascii_case(base)
                && ((line.a == long && line.b == short) || (line.a == short && line.b == long))
        })
        .ok_or_else(|| format!("第一腿下单前的实时盘口复核：价差监控里没有 {base}（{long} / {short}）这一行，不下单"))?;
    if let Some(note) = &line.note {
        return Err(format!("第一腿下单前的实时盘口复核：{base} {note}，不下单"));
    }
    if !view.connected.up(long) || !view.connected.up(short) {
        return Err("第一腿下单前的实时盘口复核：有一家的行情连接断了，不下单".into());
    }
    let leg = if line.a == long {
        &line.long_a
    } else {
        &line.long_b
    };
    let leg = leg.as_ref().ok_or_else(|| {
        format!("第一腿下单前的实时盘口复核：{base} 这个方向现在没有可成交报价，不下单")
    })?;
    // `covered`：估算深度够不够这一笔。不够时（按盘口放大的单子）退而用最大的一档：比这笔小、价差偏乐观，
    // 连它都不过就肯定不过；过了也只算「没核全」（返回 `None`），以前是整个跳过（10-31 审查）。
    let (entry, depth, covered) = if view.size_usdt >= size_usdt {
        (leg.quote.entry_pct, view.size_usdt, true)
    } else {
        // 阶梯从小到大：取够这笔的最小一档，没有就取最大的一档。
        match leg.ladder.iter().find(|rung| rung.size_usdt >= size_usdt) {
            Some(rung) => (rung.entry_pct, rung.size_usdt, true),
            None => match leg
                .ladder
                .last()
                .filter(|rung| rung.size_usdt > view.size_usdt)
            {
                Some(rung) => (rung.entry_pct, rung.size_usdt, false),
                None => (leg.quote.entry_pct, view.size_usdt, false),
            },
        }
    };
    let floor_pct = (floor * Decimal::ONE_HUNDRED).round_dp(5);
    if entry < floor_pct {
        return Err(format!(
            "第一腿下单前按实时盘口复核：{base} 多 {long} / 空 {short} 现在两腿可成交价差 {}%，低于第二腿的保本线 {}%（按 {} USDT 深度），不下单",
            entry.normalize(),
            floor_pct.normalize(),
            depth.normalize()
        ));
    }
    if let Some(gate) = gate {
        // 价差监控的盘口是推送来的：只推变化的交易所（币安、Bitget、MEXC）推送断了、连接还在时它会旧着。
        // 下单计划是刚用 REST 现拉的盘口算的 —— 两边取更差的那个核门槛（手续费仍用价差监控按各市场实际费率
        // 算的往返费率：计划里的费率是配置的吃单费率，Hyperliquid 的会高估十倍）。
        let (entry, source) = match planned_entry_pct {
            Some(planned) if planned < entry => (planned, "按刚现拉的盘口算"),
            _ => (entry, "现在"),
        };
        if let (Some(min_edge), Some(fee)) = (gate.min_edge_pct, line.fee_round_trip_pct) {
            let edge = entry - fee;
            if edge < min_edge {
                return Err(format!(
                    "第一腿下单前按实时盘口复核：{base} 多 {long} / 空 {short} {source}可成交价差 {}% 扣往返手续费 {}% 后只剩 {}%，不到往返净边际门槛 {}%（按 {} USDT 深度），不下单",
                    entry.round_dp(3).normalize(),
                    fee.round_dp(3).normalize(),
                    edge.round_dp(3).normalize(),
                    min_edge.normalize(),
                    depth.normalize()
                ));
            }
        }
        if let Some(min_entry) = gate.thin_min_entry_pct
            && entry < min_entry
        {
            return Err(format!(
                "第一腿下单前按实时盘口复核：{base} 多 {long} / 空 {short} {source}可成交价差 {}%，不到薄盘口门槛 {}%（按 {} USDT 深度），不下单",
                entry.round_dp(3).normalize(),
                min_entry.normalize(),
                depth.normalize()
            ));
        }
    }
    Ok(covered.then_some(entry))
}

/// [`auto_open_body`]，再打开「相对正常基差」（只有自动交易会这么做）。
pub(crate) fn with_relative_to_normal(mut body: OpenBody, on: bool) -> OpenBody {
    body.relative_to_normal = on;
    body
}

/// 开仓（或预览）并返回状态码与 JSON。供自动交易调用：与页面下单完全同一条路径，
/// 在独立任务里跑（不随调用方取消），停机排空时拒绝。
pub(crate) async fn open_for_auto(
    state: Arc<AppState>,
    body: OpenBody,
    execute: bool,
) -> (StatusCode, serde_json::Value) {
    let response = if execute {
        detached(crate::metrics::Op::Open, async move {
            run_open(&state, &body, true).await
        })
        .await
    } else {
        run_open(&state, &body, false).await
    };
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap_or_default();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

async fn run_open(state: &AppState, body: &OpenBody, execute: bool) -> Response {
    let daily_default = if body.mode == Mode::Live {
        live_daily_default(state).await
    } else {
        Ok(Decimal::ZERO)
    };
    let request = match read_open(state, body, daily_default) {
        Ok(request) => request,
        Err(message) => return fail(StatusCode::BAD_REQUEST, &message),
    };
    match body.mode {
        Mode::Paper => open_paper(state, &request, execute).await,
        Mode::Live => {
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            if execute && live.opens_paused.load(std::sync::atomic::Ordering::SeqCst) {
                return fail(
                    StatusCode::LOCKED,
                    "开新仓已被暂停（Telegram /pause）。在 Telegram 里发 /resume 恢复；平仓与规则不受影响",
                );
            }
            if execute {
                if live.mode != LiveMode::Trade {
                    return fail(
                        StatusCode::FORBIDDEN,
                        "实盘是只读模式（ARB_WEB_LIVE=readonly）：只能出计划，不能下单",
                    );
                }
                let confirmed = body
                    .confirm
                    .as_deref()
                    .is_some_and(|confirm| confirm.trim().eq_ignore_ascii_case(&request.base));
                if !confirmed {
                    return fail(
                        StatusCode::BAD_REQUEST,
                        &format!(
                            "实盘开仓需要二次确认：confirm 必须填合约名 {}",
                            request.base
                        ),
                    );
                }
            }
            // 挂单的单子不做这一道：它核的是「两腿现在吃单」的价差，挂单的第一腿不吃单。挂单价来自刚拉的盘口，
            // 价差账由 `desk::prepare` 按挂单价重算；挂不住（会立刻成交）的单子交易所直接拒绝，不留记录。
            let monitor =
                (body.from_monitor && body.maker.is_none()).then_some(state.rh_spread.as_ref());
            open_live(live, &request, execute, monitor, body.entry_gate).await
        }
    }
}

async fn open_paper(state: &AppState, request: &OpenRequest, execute: bool) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, "首轮扫描尚未完成");
    };
    if snapshot.age().as_secs() > state.settings.scan_interval_sec * 3 {
        return fail(
            StatusCode::CONFLICT,
            "快照已过期（后台扫描可能卡住了）：不按陈旧行情开仓",
        );
    }
    let paper = &state.trade.paper;
    let Ok(_guard) = paper.lock.try_lock() else {
        return busy();
    };
    let (ledger, existing, executor) = match paper.executor(&snapshot.report).await {
        Ok(parts) => parts,
        Err(error) => return fail(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}")),
    };
    let prepared = match desk::prepare(
        &snapshot.report,
        &paper.by_venue,
        request,
        existing.len(),
        &limits_from(&state.settings),
        LeveragePolicy::CapToPair,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}")),
    };
    if !execute {
        return Json(json!({ "mode": "paper", "prepared": prepared })).into_response();
    }
    let position_id = desk::new_position_id("paper");
    info!(position = %position_id, symbol = %prepared.opportunity.symbol, "看板纸面开仓");
    match desk::execute(&executor, &prepared, &position_id).await {
        Ok(position) => Json(json!({
            "mode": "paper",
            "prepared": prepared,
            "position": position,
            "ledger": ledger.path().display().to_string(),
        }))
        .into_response(),
        Err(error) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("执行中断：{error:#}；以台账为准"),
        ),
    }
}

/// 提前准备的结果多久内可以拿来下单（从开始对账算起）。自动交易在信号保持到离门槛还差约 4 秒时开始做，
/// 保持够了马上下单，正常 5 秒以内就用上。对账查到的是这一刻之前的账户；之后本进程写过台账就作废。
const ARM_TTL: Duration = Duration::from_secs(10);

/// 下单前的只读准备里，和「这一笔」相关的那几个参数：合约、两腿、名义、杠杆、保证金模式。
#[derive(Debug, Clone, PartialEq)]
struct ArmKey {
    base: String,
    quote: Option<String>,
    long: Venue,
    short: Venue,
    size: Decimal,
    leverage: Decimal,
    margin_mode: arb_exec::MarginMode,
}

impl ArmKey {
    fn of(request: &OpenRequest) -> Self {
        Self {
            base: request.base.trim().to_ascii_uppercase(),
            quote: request
                .quote
                .as_deref()
                .map(|quote| quote.trim().to_ascii_uppercase()),
            long: request.long,
            short: request.short,
            size: request.size,
            leverage: request.leverage,
            margin_mode: request.margin_mode,
        }
    }
}

/// 提前做好的下单前准备：对账干净、现扫一轮、两个账户的保证金够、两腿预热过、两腿场所都没有这个合约。
struct Armed {
    key: ArmKey,
    /// 开始对账的时刻。
    at: Instant,
    /// 做完时台账的写入计数（[`arb_exec::Ledger::generation`]）。
    generation: u64,
    report: ScanReport,
    open_positions: usize,
}

/// 下单前对账和拉盘口算计划并发（见 [`open_live`]）：对账比计划晚完成超过这么多毫秒，就按现拉的盘口
/// 重算计划，不拿放旧了的盘口下单。
const STALE_BOOK_AFTER_MS: u128 = 100;

/// 下单前现扫里，两腿以外的场所沿用看板后台扫描的读数最多多旧（几个扫描周期）。看板一个周期扫一轮，
/// 留一个周期的余量：某一轮那家没取到，下一轮之前还用上一轮的；再旧就照样现拉。
const SCAN_REUSE_ROUNDS: u64 = 2;

impl LiveDesk {
    /// 下单前的现扫：只现拉两条腿的场所，其余实盘场所沿用看板后台扫描的读数（只拿来分簇与可信度筛查，
    /// 见 [`arb_scanner::RatesCache`]）。两腿的标记价、资金费、杠杆上限都是刚拉的。
    async fn scan_for(&self, legs: [Venue; 2]) -> ScanReport {
        let max_age =
            Duration::from_secs(self.settings.scan_interval_sec.max(5) * SCAN_REUSE_ROUNDS);
        arb_scanner::scan_reusing(&self.apis, &self.settings, &self.rates, &legs, max_age).await
    }

    /// 同 [`Self::scan_for`]，给台账里已配成一对的两腿补仓用：两腿被别家的报价拆进不同的簇、
    /// 彼此参考价仍在容差以内时，按这一对单独成簇（见 [`arb_scanner::identity::pin_pair`]）。
    async fn scan_for_pair(&self, request: &OpenRequest) -> ScanReport {
        let max_age =
            Duration::from_secs(self.settings.scan_interval_sec.max(5) * SCAN_REUSE_ROUNDS);
        let pin = arb_scanner::PinnedPair {
            base: request.base.trim().to_ascii_uppercase(),
            long: request.long,
            short: request.short,
        };
        arb_scanner::scan_reusing_pinned(
            &self.apis,
            &self.settings,
            &self.rates,
            &[request.long, request.short],
            max_age,
            Some(&pin),
        )
        .await
    }

    /// 取走提前做好的准备：同一笔、没过期、台账之后没写过才给；不管给不给都清掉（只用一次）。
    fn take_armed(&self, request: &OpenRequest) -> Option<Armed> {
        let armed = self
            .armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()?;
        (armed.key == ArmKey::of(request)
            && armed.at.elapsed() <= ARM_TTL
            && armed.generation == self.ledger.generation())
        .then_some(armed)
    }
}

/// 自动交易：信号还在保持中时，先把这一笔下单前的只读准备做掉（对账、现扫、保证金、预热、两腿无持仓）。
/// 保持够了真的下单时，[`open_live`] 直接用它，下单那一刻只剩拉盘口、算计划、发单。
///
/// 和下单时现做的完全一样，只是早做几秒：同样占交易锁（对账可能要把交易所里已平的仓位写进台账）；
/// 不干净、保证金不够、已有持仓都不记（下单时现做一遍，照常报原因）。返回耗时（毫秒）。
pub(crate) async fn prearm_for_auto(state: Arc<AppState>, body: OpenBody) -> Result<u128, String> {
    // 准备占着交易锁（规则轮、平仓都要这把锁），不能因为某家接口慢就长时间占着：超时就放弃、放锁，
    // 什么都不留（下单时现做）。信号保持是 0 时它是预判式、会比较密地被调用（见 `rh_auto::maybe_arm_near`），
    // 上限更要紧。
    match tokio::time::timeout(ARM_BUDGET, prearm_inner(state, body)).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "超过 {} 秒，放弃这次提前准备",
            ARM_BUDGET.as_secs_f32()
        )),
    }
}

/// 一次提前准备最多花多久：它占着交易锁，超了就放弃（见 [`prearm_for_auto`]）。
const ARM_BUDGET: Duration = Duration::from_secs(2);

async fn prearm_inner(state: Arc<AppState>, body: OpenBody) -> Result<u128, String> {
    let daily_default = live_daily_default(&state).await;
    let request = read_open(&state, &body, daily_default)?;
    let live = state
        .trade
        .live_opt()
        .ok_or_else(|| "实盘没连上".to_string())?;
    if live.mode != LiveMode::Trade || live.opens_paused.load(std::sync::atomic::Ordering::SeqCst) {
        return Err("实盘不能下单（只读或暂停）".into());
    }
    if !live.brokers.contains_key(&request.long) || !live.brokers.contains_key(&request.short) {
        return Err("两腿不全是实盘已连接的场所".into());
    }
    let Ok(_guard) = live.lock.try_lock() else {
        return Err("交易台忙".into());
    };
    let started = Instant::now();
    // 只对账这两条腿的场所（和下单现做的路径一致）：完整对账要等最慢的那家、还可能写台账，预判式准备
    // 会比较密地被调用，不能每次都全场所对账（私有接口请求量按出口 IP 限频）。认领被手动平掉的仓位
    // 留给规则轮的完整对账；两条腿上有不一致就不准备，下单时现做路径照常判断。
    let venues = [request.long, request.short];
    let (reconciled, report) = tokio::join!(live.reconcile_legs(&venues), live.scan_for(venues));
    let reconciliation = reconciled.map_err(|error| format!("{error:#}"))?;
    if reconciliation.blocks_open(
        &request.base,
        request.quote.as_deref(),
        &[request.long, request.short],
    ) {
        return Err(format!(
            "对账不一致（冻结 {}）",
            reconciliation.frozen_scope().join("、")
        ));
    }
    let open_positions = live
        .ledger
        .replay()
        .await
        .map_err(|error| error.to_string())?
        .0
        .exposed()
        .len();
    let legs = [(request.long, request.size), (request.short, request.size)];
    let (collateral, (), flat) = tokio::join!(
        desk::check_legs_collateral(&live.brokers, &legs, request.leverage),
        desk::warm_legs(&live.brokers, &report, &request),
        desk::check_legs_flat(&live.brokers, &report, &request)
    );
    collateral.map_err(|error| format!("{error:#}"))?;
    // 准备时发现已有持仓就不准备（下单时现做一遍，照常报原因）；开仓那一刻还会并发再问一遍（见 `open_live`）。
    flat.map_err(|error| format!("{error:#}"))?;
    *live
        .armed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Armed {
        key: ArmKey::of(&request),
        at: started,
        generation: live.ledger.generation(),
        report,
        open_positions,
    });
    Ok(started.elapsed().as_millis())
}

/// 自动交易空闲时：提前把 `base`/USDT 在 `venue` 上的逐仓杠杆设好（[`desk::preset_leverage`]）。
///
/// 占交易锁（忙就不做，下次再来）：不和下单、规则轮同时动同一个账户。
pub(crate) async fn preset_leverage_for_auto(
    state: Arc<AppState>,
    venue: Venue,
    base: String,
    leverage: Decimal,
) -> Result<Preset, String> {
    let live = state
        .trade
        .live_opt()
        .ok_or_else(|| "实盘没连上".to_string())?;
    if live.mode != LiveMode::Trade
        || live.opens_paused.load(std::sync::atomic::Ordering::SeqCst)
        || crate::shutdown::is_draining()
    {
        return Err("实盘不能下单（只读、暂停或正在停机）".into());
    }
    let broker = live
        .brokers
        .get(&venue)
        .ok_or_else(|| format!("{venue} 不是实盘已连接的场所"))?;
    let Ok(_guard) = live.lock.try_lock() else {
        return Ok(Preset::Busy);
    };
    let started = Instant::now();
    let symbol = Symbol::perp(&base, "USDT");
    match desk::preset_leverage(broker.as_ref(), &symbol, leverage).await {
        Ok(true) => Ok(Preset::Checked(started.elapsed().as_millis())),
        Ok(false) => Ok(Preset::Skipped),
        Err(error) => Err(error.to_string()),
    }
}

/// [`preset_leverage_for_auto`] 的结果。
pub(crate) enum Preset {
    /// 核对过（必要时设置过）杠杆，用了多少毫秒。
    Checked(u128),
    /// 有持仓、或现在开不了：没动。
    Skipped,
    /// 交易台正忙（下单、规则轮）：没动，下次再来。
    Busy,
}

async fn open_live(
    live: &LiveDesk,
    request: &OpenRequest,
    execute: bool,
    monitor: Option<&crate::rh_spread::Monitor>,
    gate: Option<EntryGate>,
) -> Response {
    if !live.brokers.contains_key(&request.long) || !live.brokers.contains_key(&request.short) {
        return fail(
            StatusCode::BAD_REQUEST,
            &format!(
                "两腿必须是实盘已连接的场所（当前连接：{}）",
                live.venues
                    .iter()
                    .map(|venue| venue.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }
    if request.rules.auto_margin().is_some() {
        for venue in [request.long, request.short] {
            if !live.brokers[&venue].supports_add_margin() {
                return fail(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    &format!("{venue} 没有接入补保证金，不能开启自动加保证金"),
                );
            }
        }
    }
    // 预览也占锁：它要对账、现扫一轮，和下单、监控挤在一起只会互相拖慢、多打上游。
    let Ok(_guard) = live.lock.try_lock() else {
        return busy();
    };
    let started = Instant::now();
    // 自动交易在信号保持期间已经把下单前的只读准备做掉了（见 [`prearm_for_auto`]）：同一笔、没过期、
    // 台账之后没写过就直接用，这一刻只剩拉盘口、算计划。预览从不用它。
    let armed = if execute {
        live.take_armed(request)
    } else {
        None
    };
    let armed_age_ms = armed.as_ref().map(|armed| armed.at.elapsed().as_millis());
    let limits = limits_from(&live.settings);
    let legs = [(request.long, request.size), (request.short, request.size)];
    // 用提前做好的准备：对账、现扫、保证金、预热都已做完，这一刻只剩按现拉的盘口算计划。两处兜底：
    // - 对账与「两腿无持仓」是几秒前做的，这几秒里有人手动开了同一个合约，台账不知道：开仓这一刻并发再问
    //   一遍两家的持仓（一个往返，藏在拉盘口后面）。
    // - 准备里的现扫比信号旧（入场基差、杠杆上限、强平风险都取自它）：用它算计划没通过不等于这一笔不能做，
    //   退回现做的路径重来一次，不拿旧读数直接拒掉一个本来能成的信号。
    let armed_outcome = match armed {
        Some(armed) => {
            let prepare_started = Instant::now();
            let (prepared, flat) = tokio::join!(
                desk::prepare(
                    &armed.report,
                    &live.by_venue,
                    request,
                    armed.open_positions,
                    &limits,
                    LeveragePolicy::Strict,
                ),
                desk::check_legs_flat(&live.brokers, &armed.report, request)
            );
            match (prepared, flat) {
                (_, Err(error)) => {
                    return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}"));
                }
                (Ok(prepared), Ok(flat)) => {
                    let phases = (0, 0, prepare_started.elapsed().as_millis());
                    Some((None, Ok(prepared), Ok(Vec::new()), Ok(flat), phases))
                }
                (Err(error), Ok(_)) => {
                    info!("提前准备的读数没能通过计划，改用现做的路径重算：{error:#}");
                    None
                }
            }
        }
        None => None,
    };
    let (reconciliation, prepared, collateral, flat, phases) = match armed_outcome {
        Some(done) => done,
        None => {
            // 对账、现扫一轮（只现拉两腿场所，见 [`LiveDesk::scan_for`]）、拉盘口算计划、两个账户撑不撑得住
            // 这笔的保证金（两腿的场所、名义、杠杆请求里就有，不必等计划）、预热与两腿无持仓**全部并发**：
            // 都只读，互不依赖。对账不干净时其余结果直接丢掉，照样拒单。串着做的话，第一单要多等一整轮对账
            // （各家持仓与挂单）。
            //
            // 计划先按对账之前的敞口仓位数算：对账只会把交易所里已经平掉的仓位在台账里结束（敞口只少不多），
            // 所以按它算是偏严的；对账真结束了仓位（台账写过）就按结束后的数重算一遍计划。
            let generation = live.ledger.generation();
            let open_before = match live.ledger.replay().await {
                Ok((replayed, _)) => replayed.exposed().len(),
                Err(error) => return fail(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
            };
            let (reconciled, (report, scanned_ms, prepared, flat, prepared_ms), collateral) = tokio::join!(
                async {
                    // 只核这笔的两条腿（完整对账、外部平仓认领由规则轮做）：省掉等最慢那家（常是 100ms 以上）。
                    let reconciled = live
                        .reconcile_legs(&[request.long, request.short])
                        .await
                        .map(|reconciliation| {
                            (
                                reconciliation,
                                Vec::<desk::ExternalNote>::new(),
                                Vec::<String>::new(),
                            )
                        });
                    (reconciled, started.elapsed().as_millis())
                },
                async {
                    let report = live.scan_for([request.long, request.short]).await;
                    let scanned_ms = started.elapsed().as_millis();
                    // 只读预热（杠杆核对、市场列表）与「两腿场所都还没有这个合约的持仓」和拉盘口同时做，
                    // 执行器就不必在报价之后再查。预览不预热、不查（它不下单）。
                    let (prepared, flat) = tokio::join!(
                        desk::prepare(
                            &report,
                            &live.by_venue,
                            request,
                            open_before,
                            &limits,
                            LeveragePolicy::Strict,
                        ),
                        async {
                            if !execute {
                                return Ok(None);
                            }
                            let (_, flat) = tokio::join!(
                                desk::warm_legs(&live.brokers, &report, request),
                                desk::check_legs_flat(&live.brokers, &report, request)
                            );
                            flat
                        }
                    );
                    let prepared_ms = started.elapsed().as_millis() - scanned_ms;
                    (report, scanned_ms, prepared, flat, prepared_ms)
                },
                desk::check_legs_collateral(&live.brokers, &legs, request.leverage)
            );
            let (reconciliation, reconciled_ms) = match reconciled {
                (Ok((reconciliation, _, _)), ms) => (reconciliation, ms),
                (Err(error), _) => {
                    return fail(StatusCode::CONFLICT, &format!("{error:#}"));
                }
            };
            if reconciliation.blocks_open(
                &request.base,
                request.quote.as_deref(),
                &[request.long, request.short],
            ) {
                return fail_with(
                    StatusCode::CONFLICT,
                    &format!(
                        "对账不一致（冻结 {}），拒绝开这个币的新仓：台账之外的持仓或挂单要先人工处理（请用专用账户）",
                        reconciliation.frozen_scope().join("、")
                    ),
                    json!({ "reconciliation": reconciliation }),
                );
            }
            // 对账比计划晚完成太多（拖尾时几百毫秒）：计划里的盘口已经放旧了，按现拉的盘口重算一遍 ——
            // 串着做时盘口总是在对账之后才拉。
            let books_stale = reconciled_ms > scanned_ms + prepared_ms + STALE_BOOK_AFTER_MS;
            let prepared = if live.ledger.generation() == generation && !books_stale {
                prepared
            } else {
                let open_positions = match live.ledger.replay().await {
                    Ok((replayed, _)) => replayed.exposed().len(),
                    Err(error) => {
                        return fail(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
                    }
                };
                desk::prepare(
                    &report,
                    &live.by_venue,
                    request,
                    open_positions,
                    &limits,
                    LeveragePolicy::Strict,
                )
                .await
            };
            (
                Some(reconciliation),
                prepared,
                collateral,
                flat,
                (reconciled_ms, scanned_ms, prepared_ms),
            )
        }
    };
    let (reconciled_ms, scanned_ms, prepared_ms) = phases;
    // 对账要等最慢的那家：记下是谁，好知道下一步该提哪家。
    let reconcile_slowest = reconciliation
        .as_ref()
        .and_then(|reconciliation| reconciliation.slowest())
        .map(|(venue, ms)| format!("{venue}:{ms}"));
    // 计划被拒的话照旧先报计划的原因，再报保证金、两腿无持仓。
    let mut prepared = match prepared {
        Ok(prepared) => prepared,
        Err(error) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}")),
    };
    let warnings = match collateral {
        Ok(warnings) => warnings,
        Err(error) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}")),
    };
    match flat {
        Ok(checked) => prepared.flat_checked = checked,
        Err(error) => return fail(StatusCode::UNPROCESSABLE_ENTITY, &format!("{error:#}")),
    }
    if !execute {
        return Json(json!({
            "mode": "live",
            "trading_enabled": live.mode == LiveMode::Trade,
            "prepared": prepared,
            "reconciliation": reconciliation,
            "warnings": warnings,
            // 点预览时顺便给出「下单前这几步各花多久」，嫌慢时看这里。
            "phases_ms": {
                "reconcile": reconciled_ms,
                "scan": scanned_ms,
                "prepare_and_collateral": prepared_ms,
                "total": started.elapsed().as_millis(),
            },
        }))
        .into_response();
    }

    // 价差监控发起的价差单：第一腿下单前按实时盘口再核一次。没过就一张单都不下、不建仓位（422：
    // 自动交易记「下单前检查没通过」，不算回滚、不进熔断）。
    if let (Some(monitor), Some(floor)) = (monitor, prepared.plan.hedge_floor()) {
        let view = monitor.view().await;
        match live_entry_check_with(
            &view,
            &request.base,
            request.long,
            request.short,
            floor,
            prepared.size_usdt,
            Utc::now(),
            gate,
            prepared.plan.expected_entry_pct(),
        ) {
            Ok(Some(entry)) => info!(
                symbol = %prepared.opportunity.symbol,
                entry_pct = %entry,
                floor_pct = %(floor * Decimal::ONE_HUNDRED).round_dp(5),
                "第一腿下单前实时盘口复核通过"
            ),
            Ok(None) => warn!(
                symbol = %prepared.opportunity.symbol,
                monitor_size = %view.size_usdt,
                size = %prepared.size_usdt,
                "价差监控估算名义比这笔小、逐档估算也没有这么大的档：按最大的一档核过了（偏乐观），没核全"
            ),
            Err(reason) => {
                warn!(symbol = %prepared.opportunity.symbol, %reason, "第一腿下单前实时盘口复核没过，不下单");
                return fail(StatusCode::UNPROCESSABLE_ENTITY, &reason);
            }
        }
    }

    let position_id = desk::new_position_id("live");
    warn!(
        position = %position_id,
        symbol = %prepared.opportunity.symbol,
        long = %prepared.opportunity.long,
        short = %prepared.opportunity.short,
        size = %prepared.size_usdt,
        leverage = %prepared.leverage,
        first = %prepared.plan.first_leg().venue,
        depth_fit = %prepared.fit.as_ref().map_or_else(|| "未开".to_string(), arb_exec::sizing::SizeFit::describe),
        "看板实盘开仓"
    );
    let execute_started = Instant::now();
    // 从这一刻起到交给执行器：第一单发出之前一共等了多久（`total_ms` 还含开仓之后那次对账）。
    let pre_ms = execute_started.duration_since(started).as_millis();
    let outcome = desk::execute(&live.executor(), &prepared, &position_id).await;
    let executed_ms = execute_started.elapsed().as_millis();
    let after = live.reconcile().await.map_err(|error| format!("{error:#}"));
    info!(
        position = %position_id,
        reconcile_ms = reconciled_ms,
        scan_ms = scanned_ms,
        prepare_ms = prepared_ms,
        pre_ms,
        reconcile_slowest = ?reconcile_slowest,
        execute_ms = executed_ms,
        total_ms = started.elapsed().as_millis(),
        armed_age_ms = ?armed_age_ms,
        "开仓各阶段耗时"
    );
    // 以回滚收场里「不是失败」的两种（见 `pause::is_benign_attempt`）。「一点都没成交」要看订单的成交
    // 记录判断：回滚会把仓位的两条腿清空，腿为空不等于没成交过。
    let maker_first = prepared.plan.first_leg().maker_rest_ms.is_some();
    let (nothing_traded, benign) = match &outcome {
        Ok(position) if position.status == PositionStatus::Unwound => {
            match live.ledger.replay().await {
                Ok((replayed, _)) => {
                    let prefix = format!("{}-", position.id);
                    let traded = replayed.orders.iter().any(|(id, order)| {
                        id.starts_with(&prefix) && order.filled_usdt > Decimal::ZERO
                    });
                    (
                        Some(!traded),
                        pause::is_benign_attempt(position, &replayed.orders),
                    )
                }
                Err(_) => (None, false),
            }
        }
        _ => (None, false),
    };
    // 挂单一点都没成交：只记日志，不发 Telegram。成交太碎回滚的那种照常通知（真的成交过、平过）；
    // 吃单第一腿被拒也照常通知（要让人看到交易所为什么拒）。
    if let Ok(position) = &outcome
        && let Some(report) = position.open_report.as_ref()
    {
        crate::rh_spread::latency::METRICS.execution(report);
    }
    let unfilled_maker = maker_first && nothing_traded == Some(true);
    if unfilled_maker {
        info!(
            position = %position_id,
            symbol = %prepared.opportunity.symbol,
            note = ?outcome.as_ref().ok().and_then(|position| position.note.as_deref()),
            "挂单没成交，已撤，什么都没发生"
        );
    }
    match &outcome {
        _ if unfilled_maker => false,
        // 吃单第一腿被拒 / 没吃到（HL 的 iocCancelRejected 最常见：HL 吃单要 0.5～0.7 秒，盘口顶上那一档常被
        // 别人先吃掉，10-20 起约 5%）：两条腿都没成交、没动用资金。照常通知（要看得到交易所为什么拒），但不写成
        // 「→ Unwound」加整套规则 —— 那样读起来像出了事故。
        Ok(position)
            if nothing_traded == Some(true) && position.status == PositionStatus::Unwound =>
        {
            live.alerts.notify_order(format!(
                "⚪ 实盘开仓没做成：{} 多 {} / 空 {}，单腿名义 {} USDT。{}。两条腿都没有成交，没有动用资金；这个合约歇一会儿再看。",
                prepared.opportunity.symbol,
                prepared.opportunity.long,
                prepared.opportunity.short,
                prepared.size_usdt.round_dp(2),
                position.note.as_deref().unwrap_or("第一腿没成交"),
            ))
        }
        Ok(position) => live.alerts.notify_order(format!(
            "📈 实盘开仓：{} 多 {} / 空 {}，单腿名义 {} USDT，杠杆 {}x → {:?}{}\n规则：{}{}",
            prepared.opportunity.symbol,
            prepared.opportunity.long,
            prepared.opportunity.short,
            prepared.size_usdt.round_dp(2),
            prepared.leverage.normalize(),
            position.status,
            position
                .note
                .as_deref()
                .map(|note| format!("。{note}"))
                .unwrap_or_default(),
            arb_exec::cli::describe_rules(&position.rules),
            position
                .open_report
                .as_ref()
                .map(|report| format!("\n{}", report.summary()))
                .unwrap_or_default()
        )),
        Err(error) => live.alerts.notify_order(format!(
            "❌ 实盘开仓中断：{} 多 {} / 空 {}：{error}。以台账与对账为准，不要重复提交。",
            prepared.opportunity.symbol, prepared.opportunity.long, prepared.opportunity.short
        )),
    };
    // 开仓以回滚收场、或执行中断：看看是不是已经连着失败了好几笔（熔断）。
    let failed_open = match &outcome {
        Ok(position) => position.status == PositionStatus::Unwound && !benign,
        Err(_) => true,
    };
    if failed_open {
        live.check_breaker().await;
    }
    match outcome {
        Ok(position) => Json(json!({
            "mode": "live",
            "prepared": prepared,
            "position": position,
            // 以回滚收场时：订单里是不是一点都没成交过（自动交易据此不计失败）。
            "nothing_traded": nothing_traded,
            "reconciliation": after.as_ref().ok(),
            "reconciliation_error": after.as_ref().err(),
            "phases_ms": {
                "reconcile": reconciled_ms,
                "scan": scanned_ms,
                "prepare_and_collateral": prepared_ms,
                "execute": executed_ms,
                "total": started.elapsed().as_millis(),
            },
        }))
        .into_response(),
        Err(error) => fail_with(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("执行中断：{error:#}。以台账与对账结果为准，不要重复提交"),
            json!({
                "position_id": position_id,
                "reconciliation": after.as_ref().ok(),
                "reconciliation_error": after.as_ref().err(),
            }),
        ),
    }
}

#[derive(Debug, Deserialize)]
pub struct CloseBody {
    mode: Mode,
    position_id: String,
    /// 实盘平仓的二次确认：必须等于仓位 id。
    confirm: Option<String>,
}

/// 平仓；也用于重试停在 `Closing` / `Unwinding` 的仓位。
pub async fn api_trade_close(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CloseBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    let position_id = body.position_id.trim().to_string();
    let response = detached(crate::metrics::Op::Close, async move {
        run_close(&state, &body).await
    })
    .await;
    log_rejection("平仓", &position_id, response).await
}

async fn run_close(state: &AppState, body: &CloseBody) -> Response {
    let position_id = body.position_id.trim();
    if position_id.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "缺少 position_id");
    }
    match body.mode {
        Mode::Paper => {
            let Some(snapshot) = state.cache.get().await else {
                return fail(StatusCode::SERVICE_UNAVAILABLE, "首轮扫描尚未完成");
            };
            let paper = &state.trade.paper;
            let Ok(_guard) = paper.lock.try_lock() else {
                return busy();
            };
            let (ledger, _, executor) = match paper.executor(&snapshot.report).await {
                Ok(parts) => parts,
                Err(error) => {
                    return fail(StatusCode::INTERNAL_SERVER_ERROR, &format!("{error:#}"));
                }
            };
            info!(position = %position_id, "看板纸面平仓");
            match desk::close(&executor, &ledger, position_id).await {
                Ok((position, error)) => {
                    Json(json!({ "mode": "paper", "position": position, "error": error }))
                        .into_response()
                }
                Err(error) => fail(StatusCode::BAD_REQUEST, &format!("{error:#}")),
            }
        }
        Mode::Live => {
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            if live.mode != LiveMode::Trade {
                return fail(
                    StatusCode::FORBIDDEN,
                    "实盘是只读模式（ARB_WEB_LIVE=readonly）：不能平仓",
                );
            }
            if body.confirm.as_deref().map(str::trim) != Some(position_id) {
                return fail(
                    StatusCode::BAD_REQUEST,
                    "实盘平仓需要二次确认：confirm 必须填仓位 id",
                );
            }
            let Ok(_guard) = live.lock.try_lock() else {
                return busy();
            };
            warn!(position = %position_id, "看板实盘平仓");
            let result = desk::close(&live.executor(), &live.ledger, position_id).await;
            let after = live.reconcile().await.map_err(|error| format!("{error:#}"));
            match &result {
                Ok((position, None)) => live.alerts.notify_order(format!(
                    "📉 实盘平仓：{} {} → {:?}。{}",
                    position.id,
                    position.symbol,
                    position.status,
                    position.note.as_deref().unwrap_or_default()
                )),
                Ok((position, Some(error))) => live.alerts.notify_order(format!(
                    "❌ 实盘平仓没走完：{} {}：{error}",
                    position.id, position.symbol
                )),
                Err(error) => live
                    .alerts
                    .notify_order(format!("❌ 实盘平仓失败：{position_id}：{error:#}")),
            };
            match result {
                Ok((position, error)) => Json(json!({
                    "mode": "live",
                    "position": position,
                    "error": error,
                    "reconciliation": after.as_ref().ok(),
                    "reconciliation_error": after.as_ref().err(),
                }))
                .into_response(),
                Err(error) => fail(StatusCode::BAD_REQUEST, &format!("{error:#}")),
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MonitorBody {
    mode: Mode,
}

/// 立刻跑一轮规则监控（与后台那一轮是同一份流程）。
pub async fn api_trade_monitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<MonitorBody>,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    detached(crate::metrics::Op::Monitor, async move {
        run_monitor(&state, &body).await
    })
    .await
}

async fn run_monitor(state: &AppState, body: &MonitorBody) -> Response {
    match body.mode {
        Mode::Paper => {
            let paper = &state.trade.paper;
            let Ok(_guard) = paper.lock.try_lock() else {
                return busy();
            };
            Json(paper.round("manual").await).into_response()
        }
        Mode::Live => {
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            if live.mode != LiveMode::Trade {
                return fail(
                    StatusCode::FORBIDDEN,
                    "实盘是只读模式（ARB_WEB_LIVE=readonly）：规则不能执行",
                );
            }
            let Ok(_guard) = live.lock.try_lock() else {
                return busy();
            };
            Json(live.round("manual").await).into_response()
        }
    }
}

#[derive(Debug, Serialize)]
struct VenueAccount {
    venue: Venue,
    fee_per_side: Decimal,
    positions: Option<Vec<arb_exec::VenuePosition>>,
    error: Option<String>,
    /// 可用保证金（能拿来开新仓的部分）。交易所没给这个数时为 `None`，不当成 0。
    free_collateral: Option<Decimal>,
    /// 可用保证金没查成的原因。与持仓分开报：一边失败不连累另一边。
    balance_error: Option<String>,
}

/// 实盘账户：对账结果、各家真实持仓与可用保证金。要令牌 —— 这是真实账户数据。
pub async fn api_live_status(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    let live = match state.trade.live() {
        Ok(live) => live,
        Err(response) => return response.into_response(),
    };
    let reconciliation: Result<Reconciliation, String> =
        live.reconcile().await.map_err(|error| format!("{error:#}"));
    let mut accounts = Vec::new();
    for &venue in &live.venues {
        let Some(broker) = live.brokers.get(&venue) else {
            continue;
        };
        // 持仓与余额同一家并发查：都是只读，各自失败各自报。
        let (positions, free) = tokio::join!(broker.positions(), broker.free_collateral());
        let (positions, error) = match positions {
            Ok(positions) => (Some(positions), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let (free_collateral, balance_error) = match free {
            Ok(free) => (free, None),
            Err(error) => (None, Some(error.to_string())),
        };
        accounts.push(VenueAccount {
            venue,
            fee_per_side: broker.fee_per_side(),
            positions,
            error,
            free_collateral,
            balance_error,
        });
    }
    Json(json!({
        "mode": live.mode,
        "venues": live.venues,
        "auto_venues": live.auto_venues,
        "reconciliation": reconciliation.as_ref().ok(),
        "reconciliation_error": reconciliation.as_ref().err(),
        "accounts": accounts,
        "last_round": *live.last_round.read().await,
        "watch_sec": live.watch_sec,
    }))
    .into_response()
}

/// 交易所账户识别：按进程环境里的凭据判断每家能不能实盘。要令牌 —— 它说出了你在
/// 哪几家有账户。只有变量名与问题描述，**不含任何值**；也不联网（真连要等开启实盘）。
///
/// 读的是 arb-web 启动时的环境：改了 .env 要重启看板才会重新识别。
pub async fn api_trade_accounts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = state.trade.authorize(&headers) {
        return response.into_response();
    }
    let raw = std::env::var("ARB_LIVE_VENUES").unwrap_or_default();
    let auto = live_connect::is_auto(&raw);
    let setting = if auto {
        live_connect::AUTO_LIVE_VENUES.to_string()
    } else {
        raw.trim().to_string()
    };
    let selection = match live_connect::live_venues(None) {
        Ok(selection) => json!({ "venues": selection.venues, "auto": selection.auto }),
        Err(error) => json!({ "error": format!("{error:#}") }),
    };
    let live_mode = state.trade.live_mode();
    Json(json!({
        "live_mode": live_mode.as_str(),
        "live": state.trade.live_opt().map(|live| json!({
            "venues": live.venues,
            "auto": live.auto_venues,
        })),
        "venues_setting": setting,
        "auto": auto,
        "selection": selection,
        "accounts": live_connect::credential_status(&|name| std::env::var(name).ok()),
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct RoundQuery {
    mode: Option<String>,
}

/// 上一轮规则监控的摘要。纸面不要令牌（纸面台账本来就能在持仓页看）；实盘要。
pub async fn api_trade_round(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<RoundQuery>,
) -> Response {
    match query.mode.as_deref().unwrap_or("paper") {
        "live" => {
            if let Err(response) = state.trade.authorize(&headers) {
                return response.into_response();
            }
            let live = match state.trade.live() {
                Ok(live) => live,
                Err(response) => return response.into_response(),
            };
            Json(json!({
                "watch_sec": if live.mode == LiveMode::Trade { live.watch_sec } else { 0 },
                "last_round": *live.last_round.read().await,
            }))
            .into_response()
        }
        _ => {
            let paper = &state.trade.paper;
            Json(json!({
                "watch_sec": paper.watch_sec,
                "last_round": *paper.last_round.read().await,
            }))
            .into_response()
        }
    }
}

/// 实盘台账路径（持仓页只读展示用）。
pub fn live_ledger_path(state: &AppState) -> Option<String> {
    state.trade.live_opt().map(|live| live.ledger_path())
}

#[cfg(test)]
mod tests {

    #[test]
    fn a_typed_daily_pnl_can_only_make_the_ledger_figure_worse() {
        let ledger = |v: i64| Ok::<Decimal, String>(Decimal::from(v));
        // 台账今天亏 120：页面预填的 0 盖不掉。
        assert_eq!(
            effective_daily(Some("0"), true, ledger(-120)),
            Ok(Decimal::from(-120))
        );
        // 手填的更差照用。
        assert_eq!(
            effective_daily(Some("-150"), true, ledger(-120)),
            Ok(Decimal::from(-150))
        );
        // 台账算不出来：只能按手填的。
        assert_eq!(
            effective_daily(Some("-3"), true, Err("未知".into())),
            Ok(Decimal::from(-3))
        );
        // 实盘没填：台账的；台账算不出来就报错。
        assert_eq!(
            effective_daily(None, true, ledger(-5)),
            Ok(Decimal::from(-5))
        );
        assert!(effective_daily(Some("  "), true, Err("未知".into())).is_err());
        // 纸面：手填的照用，没填按 0。
        assert_eq!(
            effective_daily(Some("7"), false, ledger(-120)),
            Ok(Decimal::from(7))
        );
        assert_eq!(
            effective_daily(None, false, ledger(-120)),
            Ok(Decimal::ZERO)
        );
        assert!(effective_daily(Some("abc"), true, ledger(0)).is_err());
    }

    use super::*;

    /// 某次实盘 那样只剩一条腿的仓位：满 10 分钟推第一次，之后每 15 分钟换一个键再推；两腿都在的不推；
    /// 平掉之后计时清零。
    #[test]
    fn a_leg_left_alone_is_escalated_after_ten_minutes_and_every_fifteen_after() {
        let leg = serde_json::json!({"venue":"bitget","side":"buy","notional_usdt":"199.683",
            "average_price":"2.980343283582","fee_usdt":"0.1198098",
            "client_order_id":"live-1-buy-0","margin_usdt":"66.561"});
        let position = |id: &str, short: bool| -> PairPosition {
            serde_json::from_value(serde_json::json!({
                "id": id, "symbol": "LIT/USDT", "strategy": "spread",
                "long": leg,
                "short": if short { serde_json::json!({"venue":"lighter","side":"sell",
                    "notional_usdt":"200.3606","average_price":"2.9860","fee_usdt":"0",
                    "client_order_id":"live-1-sell-0"}) } else { serde_json::Value::Null },
                "entry_basis_pct": "0.45", "expected_round_trip_cost": "0.0008", "status": "closing",
                "opened_at": "2026-10-29T12:19:41Z", "closed_at": null,
                "note": "bitget 退出未足额成交", "margin_mode": "isolated", "leverage": "3",
                "rules": {}, "trims": 0, "exits": 1, "margin_added_usdt": "0",
                "realized_pnl_usdt": "0", "realized_fee_usdt": "0"
            }))
            .unwrap()
        };
        let naked = position("live-1", false);
        let hedged = position("live-2", true);
        let exposed = [&naked, &hedged];
        let start = std::time::Instant::now();
        let at = |minutes: u64| start + std::time::Duration::from_secs(minutes * 60);
        let mut since = HashMap::new();
        assert!(naked_alerts(&exposed, &mut since, start).is_empty());
        assert!(naked_alerts(&exposed, &mut since, at(9)).is_empty());
        let first = naked_alerts(&exposed, &mut since, at(10));
        assert_eq!(first.len(), 1, "两腿都在的不推");
        let (key, text) = &first[0];
        for part in [
            "live-1",
            "LIT/USDT",
            "bitget 多",
            "10 分钟",
            "199.68",
            "平仓中",
            "bitget 退出未足额成交",
        ] {
            assert!(text.contains(part), "{part} 不在：{text}");
        }
        assert_eq!(
            naked_alerts(&exposed, &mut since, at(20))[0].0,
            *key,
            "同一个周期同一个键（冷却挡住）"
        );
        let later = naked_alerts(&exposed, &mut since, at(25));
        assert_ne!(later[0].0, *key, "下一个周期换键，照样推");
        assert!(later[0].1.contains("25 分钟"));
        // 平掉了：计时清掉。
        assert!(naked_alerts(&[&hedged], &mut since, at(30)).is_empty());
        assert!(since.is_empty());
    }

    #[test]
    fn take_profit_hold_alert_names_both_figures_and_the_cooldown() {
        let symbol = arb_core::Symbol::perp("BE", "USDT");
        let mut hold = arb_exec::monitor::TakeProfitHold {
            target_usdt: Decimal::from(2),
            book_min_usdt: Decimal::from(2),
            mark_net_usdt: Decimal::new(2_3456, 4),
            book_net_usdt: Some(Decimal::new(-4_801, 3)),
            funding_usdt: Decimal::new(1_105, 3),
            gap: None,
            reason: String::new(),
        };
        let text = take_profit_hold_alert("live-1", &symbol, &hold);
        for part in [
            "live-1",
            "BE/USDT",
            "2.35",
            "目标 2 USDT",
            "-4.80",
            "1.10",
            "30 分钟",
        ] {
            assert!(text.contains(part), "{part} 不在：{text}");
        }
        assert!(!text.contains("盘口门槛"), "没降低门槛就不提：{text}");
        hold.book_min_usdt = Decimal::ZERO;
        assert!(take_profit_hold_alert("live-1", &symbol, &hold).contains("盘口门槛为正即平"));
        hold.book_min_usdt = Decimal::new(5, 1);
        assert!(take_profit_hold_alert("live-1", &symbol, &hold).contains("盘口门槛 0.5 USDT"));
        hold.book_min_usdt = hold.target_usdt;
        hold.book_net_usdt = None;
        assert!(take_profit_hold_alert("live-1", &symbol, &hold).contains("盘口核对不了"));
        // 带了差额拆分就写进告警：钱花在哪一项上。
        hold.gap = Some(arb_exec::monitor::ExitGap {
            mark_vs_mid_usdt: Decimal::new(310, 3),
            spread_usdt: Decimal::new(520, 3),
            depth_usdt: Decimal::new(80, 3),
            fee_usdt: Decimal::new(150, 3),
        });
        let text = take_profit_hold_alert("live-1", &symbol, &hold);
        for part in [
            "标记价与盘口的差额 1.06",
            "买卖价差 0.52",
            "吃深度 0.08",
            "平仓手续费 0.15",
        ] {
            assert!(text.contains(part), "{part} 不在：{text}");
        }
    }

    #[tokio::test]
    async fn a_live_desk_that_could_not_connect_yet_reports_why_instead_of_blocking_startup() {
        let trade = Trade {
            token: Some("t".repeat(MIN_TOKEN_LEN)),
            paper: Arc::new(PaperDesk {
                ledger_path: "unused.jsonl".into(),
                ledger: OnceCell::new(),
                by_venue: HashMap::new(),
                fee_per_side: Decimal::ZERO,
                lock: Mutex::new(()),
                last_round: RwLock::new(None),
                watch_sec: 0,
                retries: desk::ExitRetries::default(),
            }),
            live_slot: Arc::new(std::sync::OnceLock::new()),
            live_wanted: Some(LiveMode::Trade),
            replays: std::sync::Mutex::new(HashMap::new()),
            live_pending: Arc::new(std::sync::Mutex::new(Some(LivePending {
                since: Utc::now(),
                attempts: 3,
                error: "连接 lighter-rh 账户失败：HTTP 404；地址 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
            }))),
        };
        // 实盘请求：503（暂时的），不是 400「没开实盘」。
        let Err(denied) = trade.live() else {
            panic!("没连上不该给出交易台")
        };
        assert_eq!(denied.0, StatusCode::SERVICE_UNAVAILABLE);
        assert!(denied.1.contains("暂时连不上"), "{}", denied.1);
        assert_eq!(trade.live_mode(), LiveMode::Trade);
        assert!(trade.live_venues().is_none() && !trade.opens_paused());
        // 健康检查：规则轮没在跑 = 停了（/healthz 503），并标明是连不上。
        let health = trade.live_health().await.expect("实盘开着就要有健康状态");
        assert!(health.disconnected && health.stalled);
        let pending = trade.live_pending().expect("要说明为什么");
        assert_eq!(pending.attempts, 3);
        // 连上之后：不再 pending，live() 给出交易台；只能填一次。
        let (desk, ..) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        let desk = Arc::new(desk);
        assert!(trade.live_slot.set(Arc::clone(&desk)).is_ok());
        assert!(
            trade.live_slot.set(desk).is_err(),
            "交易台只能填一次，不会被第二次重连覆盖"
        );
        assert!(trade.live_pending().is_none());
        assert!(trade.live().is_ok());
        assert!(!trade.live_health().await.unwrap().disconnected);
    }

    #[test]
    fn token_comparison_needs_an_exact_match() {
        assert!(constant_time_eq(b"abcdef0123456789", b"abcdef0123456789"));
        assert!(!constant_time_eq(b"abcdef0123456789", b"abcdef012345678"));
        assert!(!constant_time_eq(b"abcdef0123456789", b"abcdef0123456780"));
        assert!(!constant_time_eq(b"", b"abcdef0123456789"));
    }

    #[tokio::test]
    async fn a_logged_rejection_reaches_the_page_unchanged() {
        let response = log_rejection(
            "补仓",
            "live-1",
            fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                "两家没有落在同一个合约身份下",
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "两家没有落在同一个合约身份下");
    }

    #[tokio::test]
    async fn detached_work_survives_the_caller_being_dropped() {
        let (done, finished) = tokio::sync::oneshot::channel();
        let handler = detached(crate::metrics::Op::Close, async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = done.send(());
            fail(StatusCode::OK, "done")
        });
        // 客户端在操作进行到一半时断开：handler future 被丢弃。
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handler)
                .await
                .is_err()
        );
        // 操作必须照样做完（没有独立任务的话，`done` 随 future 一起被丢弃，这里会是 Err）。
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .expect("操作没在时限内做完")
            .expect("操作被取消了");
    }

    #[tokio::test]
    async fn a_panicking_operation_becomes_a_500_not_a_dead_handler() {
        let response = detached(crate::metrics::Op::Close, async { panic!("boom") }).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn live_mode_is_off_unless_spelled_out() {
        // SAFETY: 测试进程里只有这个测试读写这个变量。
        unsafe { std::env::remove_var("ARB_WEB_LIVE") };
        assert_eq!(LiveMode::from_env().unwrap(), LiveMode::Off);
        unsafe { std::env::set_var("ARB_WEB_LIVE", "readonly") };
        assert_eq!(LiveMode::from_env().unwrap(), LiveMode::Readonly);
        unsafe { std::env::set_var("ARB_WEB_LIVE", "Trade") };
        assert_eq!(LiveMode::from_env().unwrap(), LiveMode::Trade);
        unsafe { std::env::set_var("ARB_WEB_LIVE", "yes please") };
        assert!(LiveMode::from_env().is_err());
        unsafe { std::env::remove_var("ARB_WEB_LIVE") };
    }

    fn rules_fixture() -> (ScanReport, PairPosition) {
        use arb_core::Side;
        let symbol = Symbol::perp("BTC", "USDT");
        let leg = |venue, side| {
            json!({
                "venue": venue, "side": side, "notional_usdt": "1000",
                "average_price": "100", "fee_usdt": "0",
                "client_order_id": "rule-fixture", "margin_usdt": "200",
            })
        };
        let position = serde_json::from_value(json!({
            "id": "rule-fixture", "symbol": symbol, "strategy": "funding",
            "long": leg(Venue::Lighter, Side::Buy),
            "short": leg(Venue::Hyperliquid, Side::Sell),
            "entry_basis_pct": "0", "expected_round_trip_cost": "0",
            "status": "open", "opened_at": Utc::now(), "margin_added_usdt": "45",
        }))
        .unwrap();
        let snapshot = |venue| MarketSnapshot {
            venue,
            symbol: symbol.clone(),
            period_rate: Decimal::ZERO,
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: true,
            taker_fee: None,
            mark_price: Some(Decimal::from(100)),
            index_price: None,
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: None,
            maintenance_margin: Some(Decimal::new(1, 2)),
            oi_capped: false,
        };
        let report = ScanReport {
            generated_at: Utc::now(),
            fee_per_side: Decimal::ZERO,
            amortize_days: Decimal::from(7),
            spread_hold_days: Decimal::from(3),
            max_entry_basis_pct: None,
            min_venues: 2,
            venues: Vec::new(),
            symbols: vec![arb_scanner::SymbolView {
                symbol: symbol.clone(),
                rates: vec![snapshot(Venue::Lighter), snapshot(Venue::Hyperliquid)],
                funding: Vec::new(),
                spread: Vec::new(),
            }],
            unverified: Vec::new(),
            suspicious: Vec::new(),
            gated: Vec::new(),
            totals: arb_scanner::Totals::default(),
        };
        (report, position)
    }

    type FundingRows = Vec<(DateTime<Utc>, Decimal)>;

    /// 本地账户与盘口；不读取环境变量、不接交易所、不发送订单。
    struct RuleAccount {
        leg: arb_exec::LegFill,
        snapshot: MarketSnapshot,
        funding: std::sync::Mutex<Option<FundingRows>>,
        state: std::sync::Mutex<arb_exec::VenueLegState>,
        margin_outcome: arb_exec::broker::MarginOutcome,
        margin_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Broker for RuleAccount {
        fn venue(&self) -> Venue {
            self.leg.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, _: &arb_exec::NewOrder) -> arb_core::ArbResult<arb_exec::OrderAck> {
            Err(arb_core::ArbError::venue(
                self.leg.venue.as_str(),
                "本地账户不发送订单",
            ))
        }
        async fn order_state(
            &self,
            _: &arb_exec::ClientOrderId,
        ) -> arb_core::ArbResult<Option<arb_exec::OrderState>> {
            Ok(None)
        }
        async fn cancel(&self, _: &str) -> arb_core::ArbResult<()> {
            Err(arb_core::ArbError::venue(
                self.leg.venue.as_str(),
                "本地账户没有订单",
            ))
        }
        async fn open_orders(&self) -> arb_core::ArbResult<Vec<arb_exec::OrderState>> {
            Ok(Vec::new())
        }
        async fn positions(&self) -> arb_core::ArbResult<Vec<arb_exec::broker::VenuePosition>> {
            let quantity = self.leg.notional_usdt / self.leg.average_price;
            Ok(vec![arb_exec::broker::VenuePosition {
                venue: self.leg.venue,
                symbol: self.snapshot.symbol.clone(),
                net_quantity: match self.leg.side {
                    arb_core::Side::Buy => quantity,
                    arb_core::Side::Sell => -quantity,
                },
                average_price: Some(self.leg.average_price),
                notional_usdt: self.leg.notional_usdt,
            }])
        }
        async fn funding_since(
            &self,
            _: &Symbol,
            since: DateTime<Utc>,
        ) -> arb_core::ArbResult<Option<FundingTotal>> {
            Ok(self
                .funding
                .lock()
                .unwrap()
                .as_ref()
                .map(|rows| FundingTotal::from_rows(rows.iter().copied(), since)))
        }
        async fn leg_state(
            &self,
            _: &Symbol,
        ) -> arb_core::ArbResult<Option<arb_exec::VenueLegState>> {
            Ok(Some(self.state.lock().unwrap().clone()))
        }
        fn supports_add_margin(&self) -> bool {
            true
        }
        async fn add_margin(
            &self,
            _: &Symbol,
            amount: Decimal,
        ) -> arb_core::ArbResult<arb_exec::broker::MarginOutcome> {
            self.margin_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Unknown 也模拟已经到账、只是回执丢失；不能退款或重发。
            if matches!(
                self.margin_outcome,
                arb_exec::broker::MarginOutcome::Applied
                    | arb_exec::broker::MarginOutcome::Unknown(_)
            ) {
                let mut state = self.state.lock().unwrap();
                state.margin_usdt = state.margin_usdt.map(|margin| margin + amount);
            }
            Ok(self.margin_outcome.clone())
        }
    }

    #[async_trait::async_trait]
    impl VenueApi for RuleAccount {
        fn venue(&self) -> Venue {
            self.leg.venue
        }
        async fn fetch_all(&self) -> arb_core::ArbResult<Vec<MarketSnapshot>> {
            Ok(vec![self.snapshot.clone()])
        }
        async fn fetch_depth(
            &self,
            symbol: &Symbol,
            _: u32,
        ) -> arb_core::ArbResult<arb_core::OrderBook> {
            let level = arb_core::Level {
                price: self.snapshot.mark_price.unwrap(),
                notional_usdt: Decimal::from(1_000_000),
            };
            Ok(arb_core::OrderBook {
                venue: self.leg.venue,
                symbol: symbol.clone(),
                bids: vec![level.clone()],
                asks: vec![level],
            })
        }
    }

    async fn live_rules_fixture(
        mark: Decimal,
        outcome: arb_exec::broker::MarginOutcome,
    ) -> (LiveDesk, Arc<RuleAccount>, Arc<RuleAccount>, PairPosition) {
        let (mut report, position) = rules_fixture();
        for snapshot in &mut report.symbols[0].rates {
            snapshot.mark_price = Some(mark);
        }
        let account = |leg: &arb_exec::LegFill, snapshot: &MarketSnapshot| {
            Arc::new(RuleAccount {
                leg: leg.clone(),
                snapshot: snapshot.clone(),
                funding: std::sync::Mutex::new(Some(Vec::new())),
                state: std::sync::Mutex::new(arb_exec::VenueLegState {
                    margin_mode: None,
                    margin_usdt: leg.margin_usdt,
                    liquidation_price: None,
                }),
                margin_outcome: outcome.clone(),
                margin_calls: std::sync::atomic::AtomicUsize::new(0),
            })
        };
        let long = account(position.long.as_ref().unwrap(), &report.symbols[0].rates[0]);
        let short = account(
            position.short.as_ref().unwrap(),
            &report.symbols[0].rates[1],
        );
        let path = std::env::temp_dir().join(format!(
            "arb-web-rules-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        ledger
            .append(&arb_exec::Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        let live = LiveDesk {
            armed: std::sync::Mutex::new(None),
            mode: LiveMode::Readonly,
            venues: vec![long.leg.venue, short.leg.venue],
            auto_venues: false,
            settings: Settings {
                fee_per_side: Decimal::ZERO,
                amortize_days: Decimal::from(7),
                venues: vec![long.leg.venue, short.leg.venue],
                http_timeout_sec: 15,
                http_port: 0,
                scan_interval_sec: 60,
                max_position_usdt: Some(1000.0),
                max_daily_loss_usdt: 100.0,
                max_open_positions: Some(5),
                spread_hold_days: Decimal::from(3),
                min_venues: 2,
                max_entry_basis_pct: None,
                leverage: Decimal::from(5),
                log_filter: "info".into(),
            },
            apis: vec![long.clone(), short.clone()],
            by_venue: HashMap::from([
                (long.leg.venue, long.clone() as Arc<dyn VenueApi>),
                (short.leg.venue, short.clone() as Arc<dyn VenueApi>),
            ]),
            brokers: HashMap::from([
                (long.leg.venue, long.clone() as Arc<dyn Broker>),
                (short.leg.venue, short.clone() as Arc<dyn Broker>),
            ]),
            pause_file: pause::path_for(&path),
            ledger,
            market_slippage: Some(Decimal::new(3, 3)),
            lock: Mutex::new(()),
            last_round: RwLock::new(None),
            last_reconcile: RwLock::new(None),
            divergence_sig: std::sync::Mutex::new(String::new()),
            exit_retries: desk::ExitRetries::default(),
            naked_since: std::sync::Mutex::new(HashMap::new()),
            pnl_attempts: std::sync::Mutex::new(HashMap::new()),
            fee_checked: std::sync::Mutex::new(std::collections::HashSet::new()),
            alerts: Alerter::with_sink(None),
            opens_paused: std::sync::atomic::AtomicBool::new(false),
            pause_state: std::sync::Mutex::new(PauseState::default()),
            dirty_rounds: std::sync::atomic::AtomicU32::new(0),
            dirty_alerted: std::sync::atomic::AtomicBool::new(false),
            leg_state_cache: Mutex::new(HashMap::new()),
            exit_watched: std::sync::RwLock::new(Arc::default()),
            external_seen: std::sync::Mutex::new(HashMap::new()),
            add_seen: std::sync::Mutex::new(HashMap::new()),
            funding_cache: Mutex::new(HashMap::new()),
            watch_sec: 0,
            rates: Arc::default(),
        };
        (live, long, short, position)
    }

    async fn remove_live_fixture(live: LiveDesk) {
        let path = live.ledger.path().to_path_buf();
        drop(live);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn a_prearmed_open_is_used_once_and_only_while_fresh_unchanged_and_for_the_same_order() {
        let (live, _, _, _) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        let request = OpenRequest {
            maker: None,
            margin_mode: arb_exec::MarginMode::Isolated,
            base: "btc".into(),
            quote: Some("usdt".into()),
            long: Venue::Lighter,
            short: Venue::Hyperliquid,
            size: Decimal::from(800),
            leverage: Decimal::from(3),
            daily_pnl: Decimal::ZERO,
            view: "spread".into(),
            depth_levels: 20,
            rules: arb_exec::TaskRules::default(),
            relative_to_normal: true,
            leg_order: arb_exec::preflight::LegOrder::default(),
            depth_fit: None,
            force_hedge_pct: None,
        };
        let arm = |live: &LiveDesk, at: Instant| {
            *live.armed.lock().unwrap() = Some(Armed {
                key: ArmKey::of(&request),
                at,
                generation: live.ledger.generation(),
                report: rules_fixture().0,
                open_positions: 1,
            });
        };

        // 同一笔、刚做完：给，且只给一次。规则、日内盈亏之类不影响只读准备，不算「不同」。
        arm(&live, Instant::now());
        let same = OpenRequest {
            base: "BTC".into(),
            daily_pnl: Decimal::from(-5),
            ..request.clone()
        };
        let taken = live.take_armed(&same).expect("同一笔");
        assert_eq!(taken.open_positions, 1);
        assert!(live.take_armed(&request).is_none(), "只用一次");

        // 名义、腿、杠杆不同：不给（并作废）。
        for other in [
            OpenRequest {
                size: Decimal::from(1600),
                ..request.clone()
            },
            OpenRequest {
                long: Venue::Hyperliquid,
                short: Venue::Lighter,
                ..request.clone()
            },
            OpenRequest {
                leverage: Decimal::from(5),
                ..request.clone()
            },
        ] {
            arm(&live, Instant::now());
            assert!(live.take_armed(&other).is_none());
            assert!(live.armed.lock().unwrap().is_none(), "不匹配也清掉");
        }

        // 过期：不给。
        arm(&live, Instant::now() - ARM_TTL - Duration::from_secs(1));
        assert!(live.take_armed(&request).is_none());

        // 做完之后台账写过一行（规则轮、别的下单）：不给，下单时现做。
        arm(&live, Instant::now());
        live.ledger
            .append(&arb_exec::Record::Position(Box::new(
                live.ledger.replay().await.unwrap().0.positions["rule-fixture"].clone(),
            )))
            .await
            .unwrap();
        assert!(live.take_armed(&request).is_none());
        remove_live_fixture(live).await;
    }

    /// 下单前的对账和现扫、算计划、保证金、两腿无持仓并发跑：对账不干净时照样先报对账（409），一张单都不下；
    /// 对账干净才轮到其余检查的结论。
    #[tokio::test]
    async fn a_frozen_symbol_is_refused_first_although_the_other_checks_ran_alongside() {
        let (live, _, _, position) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        let request = OpenRequest {
            maker: None,
            margin_mode: arb_exec::MarginMode::Isolated,
            base: "BTC".into(),
            quote: Some("USDT".into()),
            long: Venue::Lighter,
            short: Venue::Hyperliquid,
            size: Decimal::from(100),
            leverage: Decimal::from(3),
            daily_pnl: Decimal::ZERO,
            view: "funding".into(),
            depth_levels: 20,
            rules: arb_exec::TaskRules::default(),
            relative_to_normal: false,
            leg_order: arb_exec::preflight::LegOrder::default(),
            depth_fit: None,
            force_hedge_pct: None,
        };

        // 对账干净（台账的 BTC 仓位两家都在）：轮到其余检查（两家已有 BTC 持仓、没有资金费机会），422。
        let response = open_live(&live, &request, true, None, None).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        // 台账里这笔已经结束、交易所还拿着：BTC 冻结，先报对账。
        let mut closed = position.clone();
        closed.status = PositionStatus::Closed;
        live.ledger
            .append(&arb_exec::Record::Position(Box::new(closed)))
            .await
            .unwrap();
        let response = open_live(&live, &request, true, None, None).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let (replayed, _) = live.ledger.replay().await.unwrap();
        assert!(replayed.orders.is_empty(), "一张单都没下");
        remove_live_fixture(live).await;
    }

    #[tokio::test]
    async fn take_profit_rechecks_settled_funding_instead_of_the_display_cache() {
        let (live, long, _, mut position) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        position.rules.take_profit_usdt = Some(Decimal::ONE);
        *long.funding.lock().unwrap() = Some(vec![(position.opened_at, Decimal::from(2))]);
        assert_eq!(
            live.position_funding(&position, false).await.total_usdt,
            Some(Decimal::from(2))
        );
        // 新结算付出 3，实际合计 -1；旧页面缓存仍显示 +2，不能据此止盈。
        long.funding
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .push((position.opened_at, Decimal::from(-3)));
        let funding = desk::FundingSource::funding_usdt(&live, &position).await;
        assert_eq!(funding, Some(Decimal::from(-1)));
        assert!(!arb_exec::monitor::take_profit_triggered(
            &position,
            &long.snapshot,
            &long.snapshot,
            funding
        ));
        // 新查询变成未知，旧的成功值也不能继续驱动平仓。
        *long.funding.lock().unwrap() = None;
        assert_eq!(
            desk::FundingSource::funding_usdt(&live, &position).await,
            None
        );
        remove_live_fixture(live).await;
    }

    #[tokio::test]
    async fn reopening_within_one_second_does_not_inherit_the_previous_funding() {
        let (live, long, _, mut position) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        position.rules.take_profit_usdt = Some(Decimal::ONE);
        position.opened_at = DateTime::from_timestamp(1_700_000_000, 100_000_000).unwrap();
        *long.funding.lock().unwrap() = Some(vec![(
            position.opened_at + chrono::Duration::milliseconds(100),
            Decimal::from(2),
        )]);
        assert_eq!(
            live.position_funding(&position, false).await.total_usdt,
            Some(Decimal::from(2))
        );
        position.opened_at += chrono::Duration::milliseconds(800);
        let funding = live.position_funding(&position, false).await.total_usdt;
        assert_eq!(funding, Some(Decimal::ZERO));
        assert!(!arb_exec::monitor::take_profit_triggered(
            &position,
            &long.snapshot,
            &long.snapshot,
            funding
        ));
        remove_live_fixture(live).await;
    }

    #[tokio::test]
    async fn margin_round_refreshes_display_state_and_keeps_unknown_budget_reserved() {
        for outcome in [
            arb_exec::broker::MarginOutcome::Applied,
            arb_exec::broker::MarginOutcome::Unknown("回执丢失".into()),
        ] {
            let unknown = matches!(outcome, arb_exec::broker::MarginOutcome::Unknown(_));
            let (live, _, short, mut position) =
                live_rules_fixture(Decimal::from(115), outcome).await;
            position.rules.auto_margin_pct = Some(Decimal::from(12));
            position.rules.auto_margin_max_usdt = Some(Decimal::from(200));
            live.ledger
                .append(&arb_exec::Record::Position(Box::new(position.clone())))
                .await
                .unwrap();
            assert_eq!(
                live.leg_state_cached(short.leg.venue, &position.symbol)
                    .await
                    .unwrap()
                    .margin_usdt,
                Some(Decimal::from(200))
            );
            let summary = live.round("test").await;
            assert!(summary.error.is_none(), "{:?}", summary.error);
            assert_eq!(summary.reports.len(), 1);
            assert!(summary.reports[0].executed);
            assert!(
                !is_order_report(&summary.reports[0]),
                "补保证金执行结果不是开仓/补仓/平仓播报"
            );
            assert_eq!(summary.reports[0].attention.is_some(), unknown);
            let (replayed, _) = live.ledger.replay().await.unwrap();
            assert_eq!(
                replayed.positions[&position.id].margin_added_usdt,
                Decimal::from(200)
            );
            assert_eq!(
                live.leg_state_cached(short.leg.venue, &position.symbol)
                    .await
                    .unwrap()
                    .margin_usdt,
                Some(Decimal::from(355)),
                "补款后的持仓页不能继续显示缓存的 200"
            );
            let second = live.round("test").await;
            assert!(!second.reports[0].executed);
            assert_eq!(
                short.margin_calls.load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            remove_live_fixture(live).await;
        }
    }

    /// 平仓限价保护没成交（两条腿都没动）不推「规则已执行」；真平了照推。
    #[test]
    fn a_held_guarded_exit_is_not_announced_as_executed() {
        let report = MonitorReport {
            position_id: "p".into(),
            symbol: Symbol::perp("NBIS", "USDT"),
            evaluation: None,
            executed: true,
            retried_exit: false,
            skipped: None,
            error: None,
            attention: None,
            exit_held: None,
            status: PositionStatus::Closed,
            note: Some("已实现盈亏 0.3 USDT".into()),
            resumed: false,
            reverify: false,
        };
        assert!(executed_alert(&report).is_some());
        assert!(
            is_order_report(&report),
            "没有 evaluation 的重试平仓结果也必须播报"
        );
        let mut not_order = report.clone();
        not_order.status = PositionStatus::Open;
        assert!(!is_order_report(&not_order), "补保证金不是仓位订单");
        not_order.resumed = true;
        assert!(is_order_report(&not_order), "续上建仓属于下单结果");
        let held = MonitorReport {
            exit_held: Some("没成交".into()),
            ..report.clone()
        };
        assert!(executed_alert(&held).is_none());
        let idle = MonitorReport {
            executed: false,
            ..report
        };
        assert!(executed_alert(&idle).is_none());
        assert!(!is_order_report(&idle));
    }

    /// 每轮规则轮之后，还开着的双腿持仓连同平仓吃单费率、这一轮查到的资金费交给分批平仓测算。
    #[tokio::test]
    async fn each_round_hands_the_open_positions_to_the_exit_shadow() {
        let (live, _, _, mut position) =
            live_rules_fixture(Decimal::from(100), arb_exec::broker::MarginOutcome::Applied).await;
        assert!(live.exit_watched.read().unwrap().is_empty());
        position.rules.take_profit_usdt = Some(Decimal::from(1000));
        live.ledger
            .append(&arb_exec::Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        let summary = live.round("test").await;
        assert!(summary.error.is_none(), "{:?}", summary.error);
        let watched = Arc::clone(&live.exit_watched.read().unwrap());
        assert_eq!(watched.len(), 1);
        assert_eq!(watched[0].position.id, position.id);
        assert_eq!(
            watched[0].position.rules.take_profit_usdt,
            Some(Decimal::from(1000))
        );
        assert_eq!(
            watched[0].fees,
            arb_exec::monitor::ExitFees {
                long: Some(Decimal::ZERO),
                short: Some(Decimal::ZERO),
            }
        );
        assert_eq!(watched[0].funding_usdt, Some(Decimal::ZERO));
        // 平掉之后下一轮就不再交过去。
        let mut closed = position.clone();
        closed.status = PositionStatus::Closed;
        live.ledger
            .append(&arb_exec::Record::Position(Box::new(closed)))
            .await
            .unwrap();
        live.round("test").await;
        assert!(live.exit_watched.read().unwrap().is_empty());
        remove_live_fixture(live).await;
    }

    #[tokio::test]
    async fn immediately_firing_rule_edits_require_confirmation_without_touching_the_ledger() {
        let (report, position) = rules_fixture();
        let path = std::env::temp_dir().join(format!(
            "arb-rule-confirm-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap(),
        ));
        let ledger = Ledger::open(&path).await.unwrap();
        ledger
            .append(&arb_exec::Record::Position(Box::new(position)))
            .await
            .unwrap();
        let brokers = HashMap::new();
        // 强平距离约 19%，新减仓线 25% 会立即触发；入口必须阻止未经确认的修改。
        let rules = TaskRules {
            liq_protection_pct: Some(Decimal::from(25)),
            ..TaskRules::default()
        };
        let response = apply_rules(
            &ledger,
            &brokers,
            &report,
            "rule-fixture",
            rules.clone(),
            false,
            (None, None),
            Some(Decimal::ZERO),
            "paper",
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let (unchanged, _) = ledger.replay().await.unwrap();
        assert!(unchanged.positions["rule-fixture"].rules.is_empty());
        // force 只能越过立即触发确认，不能越过参数与状态校验。
        let invalid = TaskRules {
            take_profit_usdt: Some(Decimal::ZERO),
            ..rules.clone()
        };
        let response = apply_rules(
            &ledger,
            &brokers,
            &report,
            "rule-fixture",
            invalid,
            true,
            (None, None),
            Some(Decimal::ZERO),
            "paper",
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let response = apply_rules(
            &ledger,
            &brokers,
            &report,
            "rule-fixture",
            rules.clone(),
            true,
            (None, None),
            Some(Decimal::ZERO),
            "paper",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.positions["rule-fixture"].rules, rules);
        assert_eq!(
            replayed.positions["rule-fixture"].margin_added_usdt,
            Decimal::from(45)
        );
        drop(ledger);
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[test]
    fn settled_funding_drives_profit_checks_and_missing_funding_remains_unknown() {
        let (report, mut position) = rules_fixture();
        position.realized_pnl_usdt = Decimal::from(4);
        position.realized_fee_usdt = Decimal::from(2);
        let rules = TaskRules {
            take_profit_usdt: Some(Decimal::from(10)),
            ..TaskRules::default()
        };
        assert!(
            check_new_rules(
                &report,
                &position,
                &rules,
                &(None, None),
                Some(Decimal::from(9))
            )
            .immediate
            .is_some()
        );
        assert!(
            check_new_rules(&report, &position, &rules, &(None, None), None)
                .immediate
                .is_none()
        );
        position.rules = rules;
        let mut replayed = arb_exec::Replayed::default();
        replayed
            .positions
            .insert(position.id.clone(), position.clone());
        let funding = PositionFunding {
            long: None,
            short: None,
            total_usdt: Some(Decimal::from(9)),
            since: position.opened_at,
        };
        let live = crate::strategy::positions(
            &replayed,
            0,
            "fixture",
            &report,
            &HashMap::new(),
            Some(HashMap::from([(position.id.clone(), funding)])),
        );
        let observation = &live.open[0].evaluation.as_ref().unwrap().observation;
        assert_eq!(observation.net_with_funding_usdt, Some(Decimal::from(11)));
        let missing = crate::strategy::positions(
            &replayed,
            0,
            "fixture",
            &report,
            &HashMap::new(),
            Some(HashMap::new()),
        );
        assert_eq!(
            missing.open[0]
                .evaluation
                .as_ref()
                .unwrap()
                .observation
                .net_with_funding_usdt,
            None
        );
        let paper =
            crate::strategy::positions(&replayed, 0, "fixture", &report, &HashMap::new(), None);
        assert_eq!(
            paper.open[0]
                .evaluation
                .as_ref()
                .unwrap()
                .observation
                .net_with_funding_usdt,
            Some(Decimal::from(2))
        );
    }
}
