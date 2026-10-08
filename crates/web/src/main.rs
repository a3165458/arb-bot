//! `arb-web` —— 资金费套利看板，附纸面与实盘交易台。
//!
//! 与 CLI 共用同一套 `arb-scanner` 计算，所以「命令行看到的」和「面板看到的」
//! 必然是同一份排名。面板不自己算净年化：它只把参数传给服务端重排名。
//!
//! 行情与排名接口是只读的。交易接口（`/api/trade/*`）要令牌，流程与 `arb-paper` /
//! `arb-live` 是同一份代码，安全边界见 [`trade`]。

use std::collections::HashMap;
use std::sync::Arc;

use arb_core::{
    Decimal, MAX_AMORTIZE_DAYS, MAX_FEE_PER_SIDE, MAX_LEVERAGE, OrderBook, Settings, Side, Symbol,
    Venue, estimate_fill, logging, money::parse_decimal,
};
use arb_scanner::rank::RankConfig;
use arb_scanner::{ScanReport, filter_by_base, rerank};
use arb_venues::{VenueApi, build_all, build_client};
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info, warn};

mod alert;
mod board;
mod cache;
mod metrics;
mod pause;
mod precheck;
mod rh_spread;
mod shutdown;
mod strategy;
mod telegram;
mod trade;

use cache::{ScanCache, spawn_refresher};

const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_JS: &str = include_str!("../web/app.js");
const STYLE_CSS: &str = include_str!("../web/style.css");

/// 前端资源随二进制一起编译进去。
///
/// 不读磁盘：部署时只需要一个可执行文件，也不存在「服务起来了但静态目录路径不对」
/// 这种只在生产环境出现的失败。
const NO_STORE: (header::HeaderName, &str) = (header::CACHE_CONTROL, "no-store");

struct AppState {
    settings: Settings,
    cache: Arc<ScanCache>,
    apis: Arc<Vec<Arc<dyn VenueApi>>>,
    trade: trade::Trade,
    precheck: Arc<precheck::Prechecker>,
    alerts: Arc<alert::Alerter>,
    rh_spread: Arc<rh_spread::Monitor>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::from_env()?;
    logging::init(&settings.log_filter);
    // 停机排空时限：配错在启动时就报，不留到停机那一刻才发现。
    let grace = shutdown::grace_from_env()?;
    // 价差监控的配置也在启动时校验：配错立刻报，不留到后台任务里静默失败。
    let rh_spread_config = rh_spread::Config::from_env()?;

    let client = build_client(settings.http_timeout_sec)?;
    let apis = Arc::new(build_all(&settings, &client));
    let cache = Arc::new(ScanCache::default());

    // 先连实盘再扫描：实盘开着但连不上时应当立刻失败，而不是等首轮扫描跑完。
    let host = bind_host();
    let loopback = host
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(host == "localhost");
    let alerts = alert::Alerter::from_env(&client);
    let trade = trade::Trade::from_env(&settings, &client, &apis, loopback, &alerts).await?;

    // 先跑一轮再开始服务：否则启动瞬间的请求只能拿到 503。
    let first = arb_scanner::scan(&apis, &settings).await;
    info!(
        venues_ok = first.totals.venues_ok,
        symbols = first.totals.symbols,
        "首轮扫描完成"
    );
    cache.put(first).await;

    spawn_refresher(Arc::clone(&apis), settings.clone(), Arc::clone(&cache));

    // 从这里起后台规则轮才可能下单：信号改为排队等排空，而不是直接杀进程。
    // （此前没有任何在途订单，收到信号立即退出就是对的。）
    let mut signals = shutdown::Signals::install()?;
    trade.spawn_watchers();
    trade.spawn_funding_refresher();

    // 页面的默认参数（1000 USDT、3 倍）：参数文件还没有时从它们开始预检，第一次打开页面
    // 也不用等。实盘模式下页面默认选实盘连着的前两家，两个策略都备好；纸面是 Hyperliquid
    // 对 Lighter 的资金费。
    let seed = |view: &str, a: Venue, b: Venue, live: bool| precheck::WatchKey {
        view: view.into(),
        a,
        b,
        size: Decimal::ONE_THOUSAND,
        leverage: Decimal::from(3u32),
        margin_mode: arb_exec::MarginMode::Isolated,
        live,
    };
    let mut seeds = Vec::new();
    if let Some(&[first, second, ..]) = trade.live_venues() {
        seeds.push(seed("funding", first, second, true));
        seeds.push(seed("spread", first, second, true));
    }
    seeds.push(seed("funding", Venue::Hyperliquid, Venue::Lighter, false));
    let precheck = precheck::Prechecker::new(&apis, precheck_state_path(), seeds);
    precheck.spawn(Arc::clone(&cache), settings.clone());

    // Lighter RH ↔ Arcus 价差监控（只读）：行情走 WebSocket，不占 REST 限频额度。
    let rh_spread = rh_spread::Monitor::new(rh_spread_config, Arc::clone(&alerts));
    rh_spread.spawn(client.clone());

    let bind = format!("{host}:{}", settings.http_port);
    let state = Arc::new(AppState {
        rh_spread,
        settings,
        cache,
        apis,
        trade,
        precheck,
        alerts: Arc::clone(&alerts),
    });
    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_page))
        .route("/api/config", get(api_config))
        .route("/api/scan", get(api_scan))
        .route("/api/board", get(api_board))
        .route("/api/depth", get(api_depth))
        .route("/api/strategy", get(api_strategy))
        .route("/api/plan", get(api_plan))
        .route("/api/positions", get(api_positions))
        .route("/api/rh-spread", get(api_rh_spread))
        .route("/api/trade/config", get(trade::api_trade_config))
        .route("/api/trade/daily", get(trade::api_trade_daily))
        .route("/api/trade/preview", post(trade::api_trade_preview))
        .route("/api/trade/open", post(trade::api_trade_open))
        .route("/api/trade/close", post(trade::api_trade_close))
        .route("/api/trade/monitor", post(trade::api_trade_monitor))
        .route("/api/trade/rules", post(trade::api_trade_rules))
        .route("/api/trade/round", get(trade::api_trade_round))
        .route("/api/trade/live/status", get(trade::api_live_status))
        .route("/api/trade/accounts", get(trade::api_trade_accounts))
        .with_state(Arc::clone(&state))
        // 只压缩文本（JSON / JS / CSS / HTML）：客户端带 Accept-Encoding: gzip 才压。
        .layer(tower_http::compression::CompressionLayer::new())
        // 安全响应头：不许被嵌进别人的页面（点击劫持）、不许猜内容类型、不带来源页、任何响应都不缓存
        // （行情与账户状态过时就是错的，中间代理缓存一份旧的比没有更糟）。
        .layer(security_header(header::X_FRAME_OPTIONS, "DENY"))
        .layer(security_header(header::X_CONTENT_TYPE_OPTIONS, "nosniff"))
        .layer(security_header(header::REFERRER_POLICY, "no-referrer"))
        .layer(security_header(
            header::CONTENT_SECURITY_POLICY,
            "frame-ancestors 'none'",
        ))
        .layer(security_header(header::CACHE_CONTROL, "no-store"));

    // 命令机器人（菜单与只读查询）：配了 Telegram 密钥才启动，只有管理员的私聊能用。
    telegram::spawn(&state, &client);
    // 启动通知：重启是很多问题的起点（崩溃循环、误重启），要能在手机上看到。
    alerts.notify_always(
        format!(
            "🟢 arb-web 已启动：{}",
            match state.trade.live_venues() {
                Some(venues) => format!(
                    "实盘{}，连接 {}",
                    if state.trade.live_readonly() {
                        "只读"
                    } else {
                        "可下单"
                    },
                    venues
                        .iter()
                        .map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join("、")
                ),
                None if state.trade.live_mode() != trade::LiveMode::Off => {
                    "实盘账户暂时连不上，后台每分钟重连（行情、价差监控照常）".to_string()
                }
                None => "实盘未开启（纸面）".to_string(),
            }
        ) + "。发 /menu 打开菜单（状态、持仓、盈亏、保证金、机会）。",
    );
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    info!(%bind, "看板已启动");
    println!("看板已启动：http://{bind}/");

    let (stop_server, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let mut server_done = false;
    let cause = tokio::select! {
        signal = signals.recv() => format!("收到 {signal}"),
        ended = &mut server => {
            server_done = true;
            format!("HTTP 服务自己退出了（{ended:?}）")
        }
    };
    // 先置位再做别的：之后后台规则轮和新的下单 / 平仓请求都不再开始。
    shutdown::begin_drain();
    warn!(%cause, grace_sec = grace.as_secs(), "停机：不再接受新请求，等进行中的操作结束");
    alerts.notify_always(format!(
        "🟠 arb-web {cause}，正在排空进行中的操作（最长 {} 秒）",
        grace.as_secs()
    ));
    // 排空期间再来一个信号：操作者不想等了，立即退出。
    tokio::spawn(async move {
        let signal = signals.recv().await;
        error!(signal, "排空期间又收到信号：立即退出");
        std::process::exit(130);
    });

    let deadline = tokio::time::Instant::now() + grace;
    let _ = stop_server.send(());
    if !server_done
        && tokio::time::timeout_at(deadline, &mut server)
            .await
            .is_err()
    {
        warn!("时限内仍有未结束的 HTTP 请求，不再等它们");
    }
    // 下单 / 平仓 / 规则轮都握着交易台的锁：拿到锁就是它们都结束了，并且之后谁也开不了新的。
    // 锁一直握到进程结束：所以这里直接 `exit`，不 `return`（返回会先放锁，运行时被丢弃之前
    // 后台规则轮有机会抢到它再开始一轮）。
    let quiesced = state.trade.quiesce(deadline).await;
    if quiesced.busy.is_empty() {
        info!("交易台已空闲，退出");
        alerts
            .notify_final("🔴 arb-web 已停止（停机时没有进行中的操作）")
            .await;
        std::process::exit(0);
    }
    let busy = quiesced.busy.join("、");
    error!(%busy, "排空超时，强制退出");
    alerts
        .notify_final(format!(
            "🔴 arb-web 停机时 {busy} 上还有没做完的操作，已强制退出：重启后先看持仓页的对账结果"
        ))
        .await;
    std::process::exit(1);
}

/// 默认只监听回环地址。
///
/// 面板会展示交易机会与费率结构，默认不该对整个网络开放；要对外提供就显式设
/// `ARB_BIND_HOST=0.0.0.0`（并自己在前面加鉴权）。
fn bind_host() -> String {
    std::env::var("ARB_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".into())
}

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8"), NO_STORE],
        INDEX_HTML,
    )
}

async fn app_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            NO_STORE,
        ],
        APP_JS,
    )
}

async fn style_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8"), NO_STORE],
        STYLE_CSS,
    )
}

/// 健康检查：给 pm2 / 外部监控用。不需要令牌，所以只放布尔值、时间和计数，不放账户信息。
///
/// 快照过期（后台扫描卡住）时返回 503：面板还活着但数据已经是旧的，那对监控来说就是不健康。
/// 实盘开着时附带规则轮的状态：上一轮多久前、对账是否干净 —— 「进程在但规则轮停了」这种
/// 最危险的故障，只看进程存活是发现不了的。
async fn healthz(State(state): State<Arc<AppState>>) -> Response {
    match state.cache.get().await {
        Some(snapshot) => {
            let age = snapshot.age();
            let stale = age.as_secs() > state.settings.scan_interval_sec * 3;
            let live = state.trade.live_health().await;
            let round_stalled = live.as_ref().is_some_and(|live| live.stalled);
            let ok = !stale && !round_stalled;
            let body = json!({
                "ok": ok,
                "age_ms": age.as_millis() as u64,
                "stale": stale,
                "venues_ok": snapshot.report.totals.venues_ok,
                "venues_failed": snapshot.report.totals.venues_failed,
                "live": live,
                "alerts": {
                    "enabled": state.alerts.enabled(),
                    "suppressed": state.alerts.suppressed(),
                },
            });
            let status = if ok {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, Json(body)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"ok": false, "error": "首轮扫描尚未完成"})),
        )
            .into_response(),
    }
}

/// 给每个响应补一个固定的安全响应头（处理函数自己设了的不覆盖）。
fn security_header(
    name: header::HeaderName,
    value: &'static str,
) -> tower_http::set_header::SetResponseHeaderLayer<header::HeaderValue> {
    tower_http::set_header::SetResponseHeaderLayer::if_not_present(
        name,
        header::HeaderValue::from_static(value),
    )
}

/// Prometheus 抓取端点：计数、时间、布尔值，不含账户信息，所以和 `/healthz` 一样不要令牌
/// （见 [`metrics`]）。
async fn metrics_page(State(state): State<Arc<AppState>>) -> Response {
    let snapshot = state.cache.get().await;
    let venues = snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .report
                .venues
                .iter()
                .map(|report| metrics::VenueSample {
                    venue: report.venue.as_str(),
                    ok: report.ok,
                    rates: report.rates,
                    elapsed_ms: report.elapsed_ms,
                    cooldown_s: arb_venues::http::cooling(report.venue)
                        .map_or(0.0, |left| left.as_secs_f64()),
                })
                .collect()
        })
        .unwrap_or_default();
    let live = state
        .trade
        .live_health()
        .await
        .map(|health| metrics::LiveSample {
            trade_mode: health.mode == trade::LiveMode::Trade,
            last_round_age_s: health.last_round_age_s,
            reconciliation_clean: health.reconciliation_clean,
            dirty_rounds: health.dirty_rounds,
            stalled: health.stalled,
            opens_paused: health.opens_paused,
            disconnected: health.disconnected,
        });
    let scrape = metrics::Scrape {
        snapshot_age_s: snapshot
            .as_ref()
            .map(|snapshot| snapshot.age().as_secs_f64()),
        venues,
        live,
        positions: state.trade.ledger_status_counts().await,
        alerts_suppressed: state.alerts.suppressed(),
        draining: shutdown::is_draining(),
        rss_bytes: metrics::resident_memory_bytes(),
    };
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render(&scrape),
    )
        .into_response()
}

async fn api_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "fee_per_side": state.settings.fee_per_side.to_string(),
        "amortize_days": state.settings.amortize_days.to_string(),
        "max_entry_basis_pct": state
            .settings
            .max_entry_basis_pct
            .map(|value| value.to_string()),
        "min_venues": state.settings.min_venues,
        "leverage": state.settings.leverage.to_string(),
        "scan_interval_sec": state.settings.scan_interval_sec,
        "venues": state
            .settings
            .effective_venues()
            .iter()
            .map(|venue| venue.as_str())
            .collect::<Vec<_>>(),
    }))
}

#[derive(Debug, Deserialize)]
struct ScanQuery {
    symbols: Option<String>,
    fee: Option<String>,
    amortize_days: Option<String>,
    /// 价差套利的计划持有天数。
    spread_hold_days: Option<String>,
    /// 入场基差门槛（%）。`off` 关闭。
    max_entry_basis_pct: Option<String>,
    /// 对价差榜前 N 条实测基差半衰期。不传则不打 K 线。
    measure_convergence: Option<String>,
    /// 每条榜最多返回多少行。只对 `/api/board` 生效。
    top: Option<u32>,
    /// 风险列用的两腿杠杆。只对 `/api/board` 生效，不影响排名。
    leverage: Option<String>,
}

/// 扫描结果 + 快照新鲜度。
///
/// `age_ms` 必须透出去：面板要能说清「这是几秒前的数据」。把陈旧快照渲染成实时
/// 数据是这类工具最容易犯的错。
#[derive(Debug, Serialize)]
struct ScanResponse {
    #[serde(flatten)]
    report: ScanReport,
    age_ms: u64,
    stale: bool,
}

struct RankInputs {
    fee: Decimal,
    amortize_days: Decimal,
    spread_hold_days: Decimal,
    max_entry_basis_pct: Option<Decimal>,
    bases: Vec<String>,
    measure: usize,
}

fn read_rank(state: &AppState, query: &ScanQuery) -> Result<RankInputs, String> {
    let fee = parse_bounded(
        query.fee.as_deref(),
        state.settings.fee_per_side,
        MAX_FEE_PER_SIDE,
        "fee",
    )?;
    let amortize_days = match parse_bounded(
        query.amortize_days.as_deref(),
        state.settings.amortize_days,
        Decimal::from(MAX_AMORTIZE_DAYS),
        "amortize_days",
    ) {
        Ok(value) if value > Decimal::ZERO => value,
        Ok(_) => return Err("amortize_days 必须大于 0".into()),
        Err(message) => return Err(message),
    };
    let spread_hold_days = match parse_bounded(
        query.spread_hold_days.as_deref(),
        state.settings.spread_hold_days,
        Decimal::from(MAX_AMORTIZE_DAYS),
        "spread_hold_days",
    ) {
        Ok(value) if value > Decimal::ZERO => value,
        Ok(_) => return Err("spread_hold_days 必须大于 0".into()),
        Err(message) => return Err(message),
    };
    let max_entry_basis_pct = parse_optional_bounded(
        query.max_entry_basis_pct.as_deref(),
        state.settings.max_entry_basis_pct,
        Decimal::from(100u32),
        "max_entry_basis_pct",
    )?;
    let measure = parse_measure(query.measure_convergence.as_deref())?;
    let bases = query
        .symbols
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(|base| base.trim().to_ascii_uppercase())
        .filter(|base| !base.is_empty())
        .collect();
    Ok(RankInputs {
        fee,
        amortize_days,
        spread_hold_days,
        max_entry_basis_pct,
        bases,
        measure,
    })
}

fn matches_snapshot(inputs: &RankInputs, report: &ScanReport) -> bool {
    inputs.measure == 0
        && inputs.fee == report.fee_per_side
        && inputs.amortize_days == report.amortize_days
        && inputs.spread_hold_days == report.spread_hold_days
        && inputs.max_entry_basis_pct == report.max_entry_basis_pct
}

async fn rerank_report(
    snapshot: &ScanReport,
    inputs: &RankInputs,
    apis: &[Arc<dyn VenueApi>],
) -> ScanReport {
    let mut report = snapshot.clone();
    filter_by_base(&mut report, &inputs.bases);
    let measured_hold = if inputs.measure == 0 {
        HashMap::new()
    } else {
        // 先按配置持有期排出价差候选，再只对前 N 条拉 K 线。
        rerank(
            &mut report,
            &rank_config(
                inputs.fee,
                inputs.amortize_days,
                inputs.spread_hold_days,
                HashMap::new(),
                inputs.max_entry_basis_pct,
            ),
        );
        let api_map = api_map(apis);
        arb_scanner::convergence::measure_holds(&api_map, &report, inputs.measure, 60, 200)
            .await
            .into_iter()
            .map(|((symbol, long, short), half_life)| ((symbol, long, short), half_life.days))
            .collect()
    };
    rerank(
        &mut report,
        &rank_config(
            inputs.fee,
            inputs.amortize_days,
            inputs.spread_hold_days,
            measured_hold,
            inputs.max_entry_basis_pct,
        ),
    );
    report
}

async fn api_scan(State(state): State<Arc<AppState>>, Query(query): Query<ScanQuery>) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "首轮扫描尚未完成"})),
        )
            .into_response();
    };
    let inputs = match read_rank(&state, &query) {
        Ok(inputs) => inputs,
        Err(message) => return bad_request(&message),
    };
    let report = rerank_report(&snapshot.report, &inputs, &state.apis).await;
    let age = snapshot.age();
    Json(ScanResponse {
        report,
        age_ms: age.as_millis() as u64,
        stale: age.as_secs() > state.settings.scan_interval_sec * 3,
    })
    .into_response()
}

#[derive(Serialize)]
struct BoardHttp {
    #[serde(flatten)]
    board: board::Board,
    age_ms: u64,
    stale: bool,
}

/// 看板用的瘦响应：两条榜各前 N 行，外加这些行的合约读数。
///
/// 参数和快照一致、又没要求拟合半衰期时，直接切快照，不克隆整份报告。
async fn api_board(State(state): State<Arc<AppState>>, Query(query): Query<ScanQuery>) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "首轮扫描尚未完成"})),
        )
            .into_response();
    };
    let inputs = match read_rank(&state, &query) {
        Ok(inputs) => inputs,
        Err(message) => return bad_request(&message),
    };
    let top = match board::parse_top(query.top) {
        Ok(top) => top,
        Err(message) => return bad_request(&message),
    };
    let leverage = match parse_leverage(query.leverage.as_deref(), state.settings.leverage) {
        Ok(leverage) => leverage,
        Err(message) => return bad_request(&message),
    };
    let age = snapshot.age();
    let stale = age.as_secs() > state.settings.scan_interval_sec * 3;
    // 杠杆只影响风险列，不影响排名，所以不参与「和快照一致」的判断。
    let board_body = if matches_snapshot(&inputs, &snapshot.report) {
        board::project(&snapshot.report, &inputs.bases, top, None, leverage)
    } else {
        let report = rerank_report(&snapshot.report, &inputs, &state.apis).await;
        let measured_pairs = (inputs.measure > 0).then(|| board::count_measured(&report));
        // 币种过滤已经做在 rerank 之前。再传 bases 会把空名单当成「全部」，这里传空。
        board::project(&report, &[], top, measured_pairs, leverage)
    };
    Json(BoardHttp {
        board: board_body,
        age_ms: age.as_millis() as u64,
        stale,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
struct StrategyQuery {
    a: Option<String>,
    b: Option<String>,
    /// `funding`（默认）或 `spread`。
    view: Option<String>,
    /// 后台预检按多大单腿名义（USDT）查。缺省 1000（不超过单笔上限）。
    size: Option<String>,
    /// 后台预检按多少倍杠杆查。缺省按配置。
    leverage: Option<String>,
    #[serde(default)]
    margin_mode: arb_exec::MarginMode,
    /// `paper`（默认）或 `live`：实盘按严格杠杆口径查，且只查实盘连着的场所。
    mode: Option<String>,
    /// 当前选中的合约（`BASE/QUOTE`）：最先查。
    focus: Option<String>,
    /// 平仓规则（与开仓计划同名同义）：规则在某一行上不成立时，这一行下不了单。
    liq_protection: Option<String>,
    size_mismatch: Option<String>,
    basis_exit: Option<String>,
    /// 费差自动平仓门槛（年化 %）。空 = 关闭。
    min_funding_apr: Option<String>,
    /// 止盈（USDT，含资金费的净盈利）。空 = 关闭。
    take_profit: Option<String>,
    /// 自动加保证金的触发强平距离（%）与累计上限（USDT）。必须同时设置或同时关闭。
    auto_margin: Option<String>,
    auto_margin_max: Option<String>,
}

/// 两家场所同时上市的合约与两边费率。不传场所时默认 Hyperliquid 对 Lighter。
async fn api_strategy(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<StrategyQuery>,
) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "首轮扫描尚未完成"})),
        )
            .into_response();
    };
    let live = strategy::live_venues(&snapshot.report);
    let fallback = |preferred: Venue, other: Option<Venue>| {
        if live.contains(&preferred) && Some(preferred) != other {
            Some(preferred)
        } else {
            live.iter().copied().find(|venue| Some(*venue) != other)
        }
    };
    let a = match query.a.as_deref().filter(|raw| !raw.trim().is_empty()) {
        Some(raw) => match Venue::parse(raw) {
            Some(venue) => Some(venue),
            None => return bad_request(&format!("未知场所 {raw:?}")),
        },
        None => fallback(Venue::Hyperliquid, None),
    };
    let b = match query.b.as_deref().filter(|raw| !raw.trim().is_empty()) {
        Some(raw) => match Venue::parse(raw) {
            Some(venue) => Some(venue),
            None => return bad_request(&format!("未知场所 {raw:?}")),
        },
        None => fallback(Venue::Lighter, a),
    };
    let (Some(a), Some(b)) = (a, b) else {
        return bad_request("本轮取数成功的场所不足两家");
    };
    if a == b {
        return bad_request("两家场所不能相同");
    }
    let board = strategy::pair_board(
        &snapshot.report,
        a,
        b,
        query.view.as_deref().unwrap_or("funding"),
    );
    let key = match precheck_key(&state, &query, &board) {
        Ok(key) => key,
        Err(message) => return bad_request(&message),
    };
    let rules = match precheck_rules(&query) {
        Ok(rules) => rules,
        Err(message) => return bad_request(&message),
    };
    let stale = snapshot.age().as_secs() > state.settings.scan_interval_sec * 3;
    let unavailable = live_gap(&state, &key).or_else(|| {
        if key.live && rules.auto_margin().is_some() {
            [key.a, key.b]
                .into_iter()
                .find(|venue| !state.trade.supports_add_margin(*venue))
                .map(|venue| format!("{venue} 没有接入补保证金，不能开启自动加保证金"))
        } else {
            None
        }
    });
    let precheck = match unavailable {
        Some(reason) => precheck::BoardCheck::paused(&key, reason),
        None if stale => {
            precheck::BoardCheck::paused(&key, "快照已过期（后台扫描可能卡住了），预检暂停")
        }
        None => {
            let focus = query
                .focus
                .as_deref()
                .map(str::trim)
                .filter(|focus| !focus.is_empty());
            state.precheck.watch(key.clone(), focus.map(str::to_string));
            let mut check = state.precheck.annotate(&board, &key, focus, &rules);
            // 预检结论只用公开行情；实盘的持仓数与对账结果是账户状态，和 /api/positions、
            // 实盘账户接口一样要令牌才给。
            let authorized = state.trade.authorize(&headers).is_ok();
            let mut notes = Vec::new();
            if !key.live || authorized {
                check.account_block = account_block(&state, key.live).await;
            } else {
                notes.push("实盘的持仓数与对账要令牌才能核对：在右上角「令牌」里填好后，挡住整张表的原因会一并显示");
            }
            if key.live && state.trade.live_readonly() {
                notes.push("实盘当前是只读（ARB_WEB_LIVE=readonly）：列出的是开启下单后能下的，现在只能预览、不能下单");
            }
            check.note = (!notes.is_empty()).then(|| notes.join("；"));
            check
        }
    };
    Json(json!({
        "board": board,
        "precheck": precheck,
        "age_ms": snapshot.age().as_millis() as u64,
    }))
    .into_response()
}

/// 这张表按什么参数预检。
fn precheck_key(
    state: &AppState,
    query: &StrategyQuery,
    board: &strategy::PairBoard,
) -> Result<precheck::WatchKey, String> {
    let cap = state
        .settings
        .max_position_usdt
        .and_then(Decimal::from_f64_retain)
        .filter(|cap| *cap > Decimal::ZERO)
        .unwrap_or(Decimal::ONE_THOUSAND);
    let size = parse_bounded(
        query.size.as_deref(),
        Decimal::ONE_THOUSAND.min(cap),
        Decimal::from(strategy::MAX_PLAN_SIZE_USDT),
        "size",
    )?;
    if size <= Decimal::ZERO {
        return Err("单腿名义必须大于 0".into());
    }
    let leverage = parse_leverage(query.leverage.as_deref(), state.settings.leverage)?;
    let live = match query.mode.as_deref().map(str::trim).unwrap_or("paper") {
        "" | "paper" => false,
        "live" => true,
        other => return Err(format!("mode 只能是 paper 或 live，收到 {other:?}")),
    };
    Ok(precheck::WatchKey {
        view: board.view.clone(),
        a: board.a,
        b: board.b,
        size: size.normalize(),
        leverage: leverage.normalize(),
        margin_mode: query.margin_mode,
        live,
    })
}

/// 预检展示时核对的平仓规则。与开仓计划同一套解析与边界。
fn precheck_rules(query: &StrategyQuery) -> Result<arb_exec::TaskRules, String> {
    Ok(arb_exec::TaskRules {
        min_funding_apr: parse_optional_bounded(
            query.min_funding_apr.as_deref(),
            None,
            Decimal::from(1000u32),
            "min_funding_apr",
        )?
        .map(|pct| pct / Decimal::ONE_HUNDRED),
        liq_protection_pct: parse_optional_bounded(
            query.liq_protection.as_deref(),
            None,
            Decimal::from(100u32),
            "liq_protection",
        )?,
        size_mismatch_pct: parse_optional_bounded(
            query.size_mismatch.as_deref(),
            None,
            Decimal::from(100u32),
            "size_mismatch",
        )?,
        basis_exit_pct: parse_basis_exit(query.basis_exit.as_deref())?,
        ..parse_extra_rules(
            query.take_profit.as_deref(),
            query.auto_margin.as_deref(),
            query.auto_margin_max.as_deref(),
        )?
    })
}

/// 账户级的关：挡住的是这张表上的每一行，与合约无关。
///
/// - 持仓数已到上限（与预览同一个 `max_open_positions`）；
/// - 实盘：最近一次对账不干净或没做成（实盘预览开仓前必须对账干净）。
async fn account_block(state: &AppState, live: bool) -> Option<String> {
    let limit = arb_exec::cli::limits_from(&state.settings).max_open_positions;
    match state.trade.open_positions(live).await {
        Ok(open) if open >= limit => {
            return Some(format!(
                "{}已有 {open} 笔敞口仓位，达到上限 {limit} 笔：先在「持仓」页平掉一些才能开新仓",
                if live { "实盘" } else { "纸面" }
            ));
        }
        Ok(_) => {}
        Err(error) => return Some(format!("读不了台账，数不清已有几笔仓位：{error}")),
    }
    if live {
        return state.trade.live_reconcile_warning().await;
    }
    None
}

/// 实盘预检查不了的原因：实盘没开，或这对场所有一边没连实盘。
fn live_gap(state: &AppState, key: &precheck::WatchKey) -> Option<String> {
    if !key.live {
        return None;
    }
    let Some(connected) = state.trade.live_venues() else {
        if state.trade.live_mode() != trade::LiveMode::Off {
            return Some(
                "实盘账户暂时连不上（交易所维护或限频），后台重连成功前不做实盘预检".into(),
            );
        }
        return Some("看板没有开启实盘（ARB_WEB_LIVE=off），实盘预检不可用".into());
    };
    let missing: Vec<&str> = [key.a, key.b]
        .into_iter()
        .filter(|venue| !connected.contains(venue))
        .map(|venue| venue.as_str())
        .collect();
    (!missing.is_empty()).then(|| {
        format!(
            "实盘没有连接 {}（当前连接：{}），这对场所下不了实盘，不做预检",
            missing.join("、"),
            connected
                .iter()
                .map(|venue| venue.as_str())
                .collect::<Vec<_>>()
                .join("、")
        )
    })
}

#[derive(Debug, Deserialize)]
struct PlanQuery {
    #[serde(default)]
    margin_mode: arb_exec::MarginMode,
    symbol: String,
    long: String,
    short: String,
    size: Option<String>,
    leverage: Option<String>,
    /// 费差自动平仓门槛（年化 %）。空 = 关闭。
    min_funding_apr: Option<String>,
    /// 止盈（USDT，含资金费的净盈利）。空 = 关闭。
    take_profit: Option<String>,
    /// 自动加保证金的触发强平距离（%）与累计上限（USDT）。
    auto_margin: Option<String>,
    auto_margin_max: Option<String>,
    /// 爆仓保护门槛（强平距离 %）。空 = 关闭。
    liq_protection: Option<String>,
    /// 数量失衡门槛（%）。空 = 关闭。
    size_mismatch: Option<String>,
    /// 基差收敛平仓目标（%）。空 = 关闭。
    basis_exit: Option<String>,
    /// `funding`（默认）或 `spread`。
    view: Option<String>,
}

/// 对一对腿给出纸面开仓计划。只算不下单：开仓走 `arb-paper`，响应里给出对应命令。
async fn api_plan(State(state): State<Arc<AppState>>, Query(query): Query<PlanQuery>) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "首轮扫描尚未完成"})),
        )
            .into_response();
    };
    let input = match read_plan(&state, &query) {
        Ok(input) => input,
        Err(message) => return bad_request(&message),
    };
    match strategy::plan(&snapshot.report, &input) {
        Ok(view) => Json(view).into_response(),
        Err(message) => bad_request(&message),
    }
}

fn read_plan(state: &AppState, query: &PlanQuery) -> Result<strategy::PlanInput, String> {
    let Some((base, quote)) = query.symbol.split_once('/') else {
        return Err("symbol 必须是 BASE/QUOTE".into());
    };
    let long = Venue::parse(&query.long).ok_or("未知的做多场所")?;
    let short = Venue::parse(&query.short).ok_or("未知的做空场所")?;
    let size_usdt = parse_bounded(
        query.size.as_deref(),
        Decimal::from(1000u32),
        Decimal::from(strategy::MAX_PLAN_SIZE_USDT),
        "size",
    )?;
    let leverage = parse_leverage(query.leverage.as_deref(), state.settings.leverage)?;
    let min_funding_apr = parse_optional_bounded(
        query.min_funding_apr.as_deref(),
        None,
        Decimal::from(1000u32),
        "min_funding_apr",
    )?
    .map(|pct| pct / Decimal::ONE_HUNDRED);
    let liq_protection_pct = parse_optional_bounded(
        query.liq_protection.as_deref(),
        None,
        Decimal::from(100u32),
        "liq_protection",
    )?;
    let size_mismatch_pct = parse_optional_bounded(
        query.size_mismatch.as_deref(),
        None,
        Decimal::from(100u32),
        "size_mismatch",
    )?;
    let basis_exit_pct = parse_basis_exit(query.basis_exit.as_deref())?;
    let view = match query.view.as_deref().unwrap_or("funding") {
        view @ ("funding" | "spread") => view.to_string(),
        other => return Err(format!("view 只能是 funding 或 spread，收到 {other:?}")),
    };
    Ok(strategy::PlanInput {
        symbol: Symbol::perp(base, quote),
        long,
        short,
        size_usdt,
        leverage,
        margin_mode: query.margin_mode,
        view,
        rules: arb_exec::TaskRules {
            min_funding_apr,
            liq_protection_pct,
            size_mismatch_pct,
            basis_exit_pct,
            ..parse_extra_rules(
                query.take_profit.as_deref(),
                query.auto_margin.as_deref(),
                query.auto_margin_max.as_deref(),
            )?
        },
    })
}

/// 「最近用过的预检参数」存在哪里：看板重启后照着它继续在后台预检。
fn precheck_state_path() -> std::path::PathBuf {
    std::env::var("ARB_PRECHECK_STATE")
        .unwrap_or_else(|_| "arb-precheck-watches.json".into())
        .into()
}

/// 纸面台账路径，与 `arb-paper --ledger` 的默认值一致。
fn ledger_path() -> String {
    std::env::var("ARB_LEDGER").unwrap_or_else(|_| "arb-ledger.jsonl".into())
}

#[derive(Debug, Deserialize)]
struct PositionsQuery {
    /// `paper`（默认）或 `live`。
    mode: Option<String>,
}

/// 台账里的持仓 + 按当前快照的监控评估。只读：不创建、不修补台账文件。
///
/// 实盘台账要令牌：仓位就是真实账户的持仓。
async fn api_positions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<PositionsQuery>,
) -> Response {
    let Some(snapshot) = state.cache.get().await else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "首轮扫描尚未完成"})),
        )
            .into_response();
    };
    let path = match query.mode.as_deref().unwrap_or("paper") {
        "paper" => ledger_path(),
        "live" => {
            if let Err(response) = state.trade.authorize(&headers) {
                return response.into_response();
            }
            match trade::live_ledger_path(&state) {
                Some(path) => path,
                None => return bad_request("看板没有开启实盘（ARB_WEB_LIVE=off）"),
            }
        }
        other => return bad_request(&format!("mode 只能是 paper 或 live，收到 {other:?}")),
    };
    // 实盘：带上交易所实际的保证金状态与开仓以来的资金费（读交易所，有缓存）。
    if query.mode.as_deref() == Some("live") {
        return match state.trade.live_positions(&snapshot.report).await {
            Ok(view) => Json(view).into_response(),
            Err(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": message})),
            )
                .into_response(),
        };
    }
    match arb_exec::replay_file(&path).await {
        Ok((replayed, broken)) => Json(strategy::positions(
            &replayed,
            broken,
            &path,
            &snapshot.report,
            &strategy::LegStates::new(),
            None,
        ))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("读取台账 {path} 失败：{error}")})),
        )
            .into_response(),
    }
}

/// 止盈与自动加保证金这三个规则的解析。四个入口（开仓预览 / 下单、开仓计划、预检、改规则）共用，
/// 边界只此一份。只填这三个字段，调用方用 `..` 补上其余的。
///
/// 自动加保证金的触发线与上限必须成对：只给一个按解析错误处理（`validate_rules` 也会再拦一次）。
pub(crate) fn parse_extra_rules(
    take_profit: Option<&str>,
    auto_margin: Option<&str>,
    auto_margin_max: Option<&str>,
) -> Result<arb_exec::TaskRules, String> {
    let rules = arb_exec::TaskRules {
        take_profit_usdt: parse_optional_bounded(
            take_profit,
            None,
            Decimal::from(1_000_000u32),
            "take_profit",
        )?,
        auto_margin_pct: parse_optional_bounded(
            auto_margin,
            None,
            Decimal::from(100u32),
            "auto_margin",
        )?,
        auto_margin_max_usdt: parse_optional_bounded(
            auto_margin_max,
            None,
            Decimal::from(strategy::MAX_PLAN_SIZE_USDT),
            "auto_margin_max",
        )?,
        ..arb_exec::TaskRules::default()
    };
    if rules.auto_margin_pct.is_some() != rules.auto_margin_max_usdt.is_some() {
        return Err("auto_margin 与 auto_margin_max 必须同时设置或同时关闭".into());
    }
    Ok(rules)
}
/// 解析可关闭的十进制查询参数。`off` / `none` / `-` / 空串 = 关闭（`None`）。
fn parse_optional_bounded(
    raw: Option<&str>,
    default: Option<Decimal>,
    max: Decimal,
    what: &str,
) -> Result<Option<Decimal>, String> {
    let Some(raw) = raw.map(str::trim) else {
        return Ok(default);
    };
    if raw.is_empty() || matches!(raw.to_ascii_lowercase().as_str(), "off" | "none" | "-") {
        return Ok(None);
    }
    let value =
        parse_decimal(raw).ok_or_else(|| format!("{what} 必须是十进制数或 off，收到 {raw:?}"))?;
    if value < Decimal::ZERO || value > max {
        return Err(format!("{what} 必须在 0 到 {max} 之间，收到 {value}"));
    }
    Ok(Some(value))
}

/// 基差目标允许负数（两腿穿过 0 后再平），边界与执行层一致。
pub(crate) fn parse_basis_exit(raw: Option<&str>) -> Result<Option<Decimal>, String> {
    let Some(raw) = raw.map(str::trim) else {
        return Ok(None);
    };
    if raw.is_empty() || matches!(raw.to_ascii_lowercase().as_str(), "off" | "none" | "-") {
        return Ok(None);
    }
    let value = parse_decimal(raw).ok_or_else(|| "basis_exit 必须是十进制数或 off".to_string())?;
    if value.abs() > arb_exec::monitor::MAX_BASIS_EXIT_PCT {
        return Err("basis_exit 必须在 -5 到 5 之间".into());
    }
    Ok(Some(value))
}

/// 解析查询参数里的十进制数并做范围校验。
///
/// 这里**必须**复用与 `Settings::validate` 相同的边界：面板传进来的值绕过了环境
/// 变量那条校验路径，不挡住就能让 `?fee=5` 一路算进排名。
fn parse_bounded(
    raw: Option<&str>,
    default: Decimal,
    max: Decimal,
    what: &str,
) -> Result<Decimal, String> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(default);
    };
    let value = parse_decimal(raw).ok_or_else(|| format!("{what} 必须是十进制数，收到 {raw:?}"))?;
    if value < Decimal::ZERO || value > max {
        return Err(format!("{what} 必须在 0 到 {max} 之间，收到 {value}"));
    }
    Ok(value)
}

fn rank_config(
    fee_per_side: Decimal,
    amortize_days: Decimal,
    spread_hold_days: Decimal,
    measured_hold: HashMap<(Symbol, Venue, Venue), Decimal>,
    max_entry_basis_pct: Option<Decimal>,
) -> RankConfig {
    RankConfig {
        fee_per_side,
        amortize_days,
        spread_hold_days,
        measured_hold,
        max_entry_basis_pct,
    }
}

/// 杠杆必须落在 `1..=100`，与 `Settings::validate` 同一个边界。
fn parse_leverage(raw: Option<&str>, default: Decimal) -> Result<Decimal, String> {
    let value = parse_bounded(raw, default, Decimal::from(MAX_LEVERAGE), "leverage")?;
    if value < Decimal::ONE {
        return Err(format!("leverage 至少为 1，收到 {value}"));
    }
    Ok(value)
}

/// 空串表示不实测。上限 30：每条候选要拉两腿 K 线，再大就会把上游打满。
fn parse_measure(raw: Option<&str>) -> Result<usize, String> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(0);
    };
    let value: usize = raw
        .parse()
        .map_err(|_| format!("measure_convergence 必须是整数，收到 {raw:?}"))?;
    if value > 30 {
        return Err("measure_convergence 最大是 30".into());
    }
    Ok(value)
}

fn api_map(apis: &[Arc<dyn VenueApi>]) -> HashMap<Venue, Arc<dyn VenueApi>> {
    apis.iter()
        .map(|api| (api.venue(), Arc::clone(api)))
        .collect()
}

#[derive(Debug, Deserialize)]
struct DepthQuery {
    symbol: String,
    long: String,
    short: String,
    size: Option<String>,
    levels: Option<u32>,
}

/// Lighter RH ↔ Arcus 价差监控的当前状态（只读，不含账户信息）。
async fn api_rh_spread(State(state): State<Arc<AppState>>) -> Response {
    Json(state.rh_spread.view().await).into_response()
}

fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
}

/// 对一条已选中的配对拉多档深度，估算给定名义额的滑点。
///
/// 整轮排名只用批量一档价。这里只在用户点开某一行时打两次逐合约请求。
async fn api_depth(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DepthQuery>,
) -> Response {
    let Some((base, quote)) = query.symbol.split_once('/') else {
        return bad_request("symbol 必须是 BASE/QUOTE");
    };
    if base.is_empty() || quote.is_empty() {
        return bad_request("symbol 必须是 BASE/QUOTE");
    }
    let symbol = Symbol::perp(base, quote);
    let Some(long) = Venue::parse(&query.long) else {
        return bad_request("未知的做多场所");
    };
    let Some(short) = Venue::parse(&query.short) else {
        return bad_request("未知的做空场所");
    };
    if long == short {
        return bad_request("两腿不能是同一场所");
    }
    let size = match parse_bounded(
        query.size.as_deref(),
        Decimal::from(1000u32),
        Decimal::from(1_000_000u32),
        "size",
    ) {
        Ok(value) if value > Decimal::ZERO => value,
        Ok(_) => return bad_request("size 必须大于 0"),
        Err(message) => return bad_request(&message),
    };
    let levels = match query.levels {
        None => 20,
        Some(levels) if (1..=50).contains(&levels) => levels,
        Some(levels) => {
            let message = format!("levels 必须在 1..=50，收到 {levels}");
            return bad_request(&message);
        }
    };

    let apis = api_map(&state.apis);
    let (long_book, short_book) = tokio::join!(
        fetch_depth(&apis, long, &symbol, levels),
        fetch_depth(&apis, short, &symbol, levels),
    );
    Json(json!({
        "symbol": symbol.to_string(),
        "size": size.to_string(),
        "long": leg_payload(long, Side::Buy, long_book, size),
        "short": leg_payload(short, Side::Sell, short_book, size),
    }))
    .into_response()
}

async fn fetch_depth(
    apis: &HashMap<Venue, Arc<dyn VenueApi>>,
    venue: Venue,
    symbol: &Symbol,
    levels: u32,
) -> Result<OrderBook, String> {
    let Some(api) = apis.get(&venue) else {
        return Err(format!("{venue} 没有连接器"));
    };
    api.fetch_depth(symbol, levels)
        .await
        .map_err(|error| error.to_string())
}

/// 做多腿吃卖盘，做空腿吃买盘。缺盘口时 `ok=false`，不能把滑点渲染成 0。
fn leg_payload(
    venue: Venue,
    side: Side,
    fetched: Result<OrderBook, String>,
    size: Decimal,
) -> serde_json::Value {
    let book = match fetched {
        Ok(book) => book,
        Err(error) => {
            return json!({
                "venue": venue.as_str(),
                "side": side_name(side),
                "ok": false,
                "error": error,
            });
        }
    };
    let depth = book.side(side);
    let Some(fill) = estimate_fill(depth, size, side) else {
        return json!({
            "venue": venue.as_str(),
            "side": side_name(side),
            "ok": false,
            "error": "盘口为空或名义额无效，无法估算滑点",
        });
    };
    json!({
        "venue": venue.as_str(),
        "side": side_name(side),
        "ok": true,
        "best_bid": book.best_bid().map(|price| price.to_string()),
        "best_ask": book.best_ask().map(|price| price.to_string()),
        "relative_spread": book.relative_spread().map(|spread| spread.to_string()),
        "levels": depth.len(),
        "slippage": fill.slippage.to_string(),
        "average_price": fill.average_price.to_string(),
        "filled_usdt": fill.filled_usdt.to_string(),
        "exhausted": fill.exhausted,
    })
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::Level;

    fn level(price: i64, notional: i64) -> Level {
        Level {
            price: Decimal::from(price),
            notional_usdt: Decimal::from(notional),
        }
    }

    fn book() -> OrderBook {
        OrderBook {
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            bids: vec![level(99, 500), level(98, 800)],
            asks: vec![level(100, 400), level(110, 2_000)],
        }
    }

    #[test]
    fn depth_payload_buys_asks_and_sells_bids() {
        let size = Decimal::from(1000u32);
        let long = leg_payload(Venue::Binance, Side::Buy, Ok(book()), size);
        assert_eq!(long["ok"], true);
        assert_eq!(long["exhausted"], false);
        assert_eq!(long["filled_usdt"], "1000");
        assert!(long["slippage"].as_str().unwrap().parse::<f64>().unwrap() > 0.0);

        let short = leg_payload(Venue::Okx, Side::Sell, Ok(book()), size);
        assert_eq!(short["ok"], true);
        assert_eq!(short["exhausted"], false);
        assert!(short["slippage"].as_str().unwrap().parse::<f64>().unwrap() > 0.0);

        let thin = leg_payload(Venue::Okx, Side::Sell, Ok(book()), Decimal::from(5_000));
        assert_eq!(thin["exhausted"], true);
        assert_eq!(thin["filled_usdt"], "1300");
    }

    #[test]
    fn depth_payload_keeps_fetch_errors_distinct_from_zero_slippage() {
        let payload = leg_payload(
            Venue::Variational,
            Side::Buy,
            Err("尚未实现盘口深度".into()),
            Decimal::from(1000u32),
        );
        assert_eq!(payload["ok"], false);
        assert!(payload.get("slippage").is_none());
    }

    #[test]
    fn leverage_query_defaults_and_is_bounded() {
        let default = Decimal::from(3u32);
        assert_eq!(parse_leverage(None, default).unwrap(), default);
        assert_eq!(
            parse_leverage(Some("5"), default).unwrap(),
            Decimal::from(5u32)
        );
        assert!(parse_leverage(Some("0.5"), default).is_err());
        assert!(parse_leverage(Some("101"), default).is_err());
        assert!(parse_leverage(Some("x"), default).is_err());
    }

    #[test]
    fn measure_query_is_off_by_default_and_capped() {
        assert_eq!(parse_measure(None).unwrap(), 0);
        assert_eq!(parse_measure(Some("  ")).unwrap(), 0);
        assert_eq!(parse_measure(Some("20")).unwrap(), 20);
        assert!(parse_measure(Some("31")).is_err());
        assert!(parse_measure(Some("nope")).is_err());
    }
}
