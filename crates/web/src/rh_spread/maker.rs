//! 挂单测算（第一阶段：只观察、不下单）。
//!
//! Binance 的 TradFi 永续挂单 0 费率、吃单 0.04%。在 Binance **挂单**、另一家（Lighter RH / Arcus）**吃单**
//! 对冲，往返手续费能低到 0（挂单退出）或 0.04%（吃单退出）。但挂单什么时候成交不由我们定：最常见的成交
//! 恰恰是价格正在穿过我们的挂单价的时候（逆向选择）。这里量化的就是这个：
//!
//! - **虚拟挂单**：挂在 Binance 那一侧的最优价（做多挂买一、做空挂卖一），按这个价、对面按深度吃单，算「回到正常」
//!   的净收益，达到 [`QUOTE_MIN_NET_PCT`] 才挂。挂上之后**价格不动**：真挂单挂出不到 5 秒就撤算无效撤单（量化规则），
//!   所以至少挂满 [`MIN_REST`] 才按新的最优价重挂；期间按挂单价对着对面现在的盘口重算，不够门槛就撤。
//!   价格跟着最优价每 100ms 走的虚拟单永远不会「过期被吃」，测出来的逆向选择会偏轻；而且盘口和成交走两条连接，
//!   盘口先到时挂单价已经下移、那笔穿价成交就漏掉了 —— 漏掉的恰恰是最伤的那种。
//! - **虚拟成交**：Binance 的逐笔成交**严格穿过**挂单价（买单：成交价 < 挂单价；卖单：> 挂单价）才算成交。
//!   排队位置不知道，同价成交不算 —— 宁可少算。
//! - **成交后收益**：成交那一刻、以及 [`HORIZONS_MS`] 之后，对面按深度吃单对冲能拿到的价格，换算成入场价差与
//!   「回到正常」净收益。成交时报的净收益与之后实际能拿到的差，就是逆向选择的代价。
//! - 同一个（组, 合约, 方向）[`FILL_COOLDOWN`] 内只记一笔：真成交之后会持仓，不会马上再挂。
//!
//! 记录逐笔写进 `rh-spread/maker-shadow-YYYYMMDD.jsonl`（保留 [`KEEP_DAYS`] 天），启动时读回来做汇总。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arb_core::{Decimal, Venue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::book::LocalBook;
use super::pairs::{Pair, PairMarket};
use super::session::Session;

/// 虚拟挂单的门槛：挂单价下「回到正常」净收益（吃单退出）至少这么多（%）才算挂着。
pub const QUOTE_MIN_NET_PCT: Decimal = Decimal::from_parts(5, 0, 0, false, 2);
/// 虚拟挂单至少挂这么久才重新定价（Binance 量化规则：挂出不到 5 秒撤单算无效撤单）。
pub const MIN_REST: Duration = Duration::from_secs(5);
/// 成交后对冲价的采样时刻（毫秒）。Lighter RH 一笔单从发出到成交约 0.3 秒。
pub const HORIZONS_MS: [u64; 4] = [0, 300, 1_000, 5_000];
/// 同一个（组, 合约, 方向）两笔虚拟成交之间至少隔这么久。
pub const FILL_COOLDOWN: Duration = Duration::from_secs(60);
/// 记录保留天数。
pub const KEEP_DAYS: i64 = 14;
/// 内存里留多少笔做汇总。
const RECENT_MAX: usize = 50_000;
const FILE_PREFIX: &str = "maker-shadow";

/// 一个方向上 Binance 挂单、对面吃单的报价。百分比相对两家中间价均值。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MakerLeg {
    /// Binance 的挂单价（做多挂买一、做空挂卖一）。
    pub price: Decimal,
    /// 挂单价成交、对面按深度吃单的入场价差（%）。
    pub entry_pct: Decimal,
    /// 平仓：两腿都吃单的穿价（%）。
    pub exit_cross_taker_pct: Decimal,
    /// 平仓：Binance 挂单（挂在对侧最优价）、对面吃单的穿价（%）。可以为负（挂单赚半个价差）。
    pub exit_cross_maker_pct: Decimal,
    /// 回到正常水平的净收益（%）：吃单退出 / 挂单退出。没有正常水平为 `None`。
    pub net_to_normal_taker_exit_pct: Option<Decimal>,
    pub net_to_normal_maker_exit_pct: Option<Decimal>,
}

/// 一行的挂单测算。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MakerView {
    /// 挂单的那一家（Binance）。
    pub venue: Venue,
    /// 挂单场所的单边挂单 / 吃单费率（小数，币安 TradFi：0 / 0.0004）。自动交易挂单开仓时要用：
    /// 开仓付挂单费率，之后吃单平仓（以及回滚）付吃单费率。
    pub maker_fee: Decimal,
    pub taker_fee: Decimal,
    /// 往返手续费（%）：Binance 挂单开 + 吃单平 + 对面吃单两次 / Binance 挂单开平 + 对面吃单两次。
    pub fee_taker_exit_pct: Decimal,
    pub fee_maker_exit_pct: Decimal,
    pub long_a: Option<MakerLeg>,
    pub long_b: Option<MakerLeg>,
}

/// 按名义 `size_usdt` 估算：`maker`（Binance）挂单做多（`maker_long`）或做空，`other` 吃单反向。
/// 任一边盘口不完整、深度不够为 `None`。
pub fn maker_quote(
    maker: &LocalBook,
    maker_long: bool,
    other: &LocalBook,
    size_usdt: Decimal,
) -> Option<(Decimal, Decimal, Decimal, Decimal)> {
    let (maker_bid, maker_ask) = maker.top()?;
    let price = if maker_long { maker_bid } else { maker_ask };
    let (entry, taker, maker_exit) = maker_leg_at(maker, maker_long, other, size_usdt, price)?;
    Some((price, entry, taker, maker_exit))
}

/// 挂单价固定为 `price`（已经挂着的虚拟单）时的入场价差与两种平仓穿价（%）。
pub fn maker_leg_at(
    maker: &LocalBook,
    maker_long: bool,
    other: &LocalBook,
    size_usdt: Decimal,
    price: Decimal,
) -> Option<(Decimal, Decimal, Decimal)> {
    let (maker_bid, maker_ask) = maker.top()?;
    let (maker_mid, other_mid) = (maker.mid()?, other.mid()?);
    let reference = (maker_mid + other_mid) / Decimal::TWO;
    if reference <= Decimal::ZERO || size_usdt <= Decimal::ZERO {
        return None;
    }
    let quantity = size_usdt / reference;
    let pct = |value: Decimal| (value / reference * Decimal::ONE_HUNDRED).round_dp(5);
    if maker_long {
        // 买在 Binance 买一（挂单），卖在对面买盘（吃单）；平仓卖 Binance、买回对面。
        let hedge = other.sell_avg(quantity)?;
        let other_exit = other.buy_avg(quantity)?;
        let maker_exit = maker.sell_avg(quantity)?;
        Some((
            pct(hedge - price),
            pct((maker_mid - maker_exit) + (other_exit - other_mid)),
            pct((maker_mid - maker_ask) + (other_exit - other_mid)),
        ))
    } else {
        let hedge = other.buy_avg(quantity)?;
        let other_exit = other.sell_avg(quantity)?;
        let maker_exit = maker.buy_avg(quantity)?;
        Some((
            pct(price - hedge),
            pct((maker_exit - maker_mid) + (other_mid - other_exit)),
            pct((maker_bid - maker_mid) + (other_mid - other_exit)),
        ))
    }
}

/// 一行的挂单测算：`pair` 里有一家是挂单场所才有。`normal_median` 是「正常」基差（a − b，%）。
pub fn evaluate(
    pair: Pair,
    market: &PairMarket,
    book_a: &LocalBook,
    book_b: &LocalBook,
    normal_median: Option<Decimal>,
    size_usdt: Decimal,
) -> Option<MakerView> {
    let venue = pair.maker_venue()?;
    let maker_is_a = pair.a == venue;
    let (maker_market, other_market) = if maker_is_a {
        (&market.a, &market.b)
    } else {
        (&market.b, &market.a)
    };
    let (maker_fee, maker_taker, other_taker) = (
        maker_market.maker_fee?,
        maker_market.taker_fee?,
        other_market.taker_fee?,
    );
    let hundred = Decimal::ONE_HUNDRED;
    let fee_taker_exit_pct =
        ((maker_fee + maker_taker + other_taker * Decimal::TWO) * hundred).round_dp(5);
    let fee_maker_exit_pct = ((maker_fee + other_taker) * Decimal::TWO * hundred).round_dp(5);
    let (maker_book, other_book) = if maker_is_a {
        (book_a, book_b)
    } else {
        (book_b, book_a)
    };
    // long_a：多 a 空 b，回到正常基差 m（a − b）时还剩 −m 收不回 → +m；long_b 反之。
    let leg = |long_a: bool| {
        let maker_long = long_a == maker_is_a;
        let (price, entry_pct, exit_cross_taker_pct, exit_cross_maker_pct) =
            maker_quote(maker_book, maker_long, other_book, size_usdt)?;
        let sign = if long_a { Decimal::ONE } else { -Decimal::ONE };
        let net = |cross: Decimal, fee: Decimal| {
            normal_median.map(|median| (entry_pct - cross - fee + sign * median).round_dp(5))
        };
        Some(MakerLeg {
            price,
            entry_pct,
            exit_cross_taker_pct,
            exit_cross_maker_pct,
            net_to_normal_taker_exit_pct: net(exit_cross_taker_pct, fee_taker_exit_pct),
            net_to_normal_maker_exit_pct: net(exit_cross_maker_pct, fee_maker_exit_pct),
        })
    };
    Some(MakerView {
        venue,
        maker_fee,
        taker_fee: maker_taker,
        fee_taker_exit_pct,
        fee_maker_exit_pct,
        long_a: leg(true),
        long_b: leg(false),
    })
}

// ───────────────────────────── 虚拟挂单与成交 ─────────────────────────────

/// （组, 合约, 方向）。
type Key = (String, String, &'static str);

/// 正在挂着的虚拟单。
#[derive(Debug, Clone)]
struct Quote {
    /// Binance 原生名（成交流按它对上）。
    market: String,
    maker_long: bool,
    price: Decimal,
    /// 对面盘口（场所, 订阅名）：成交后按它采样对冲价。
    other: (Venue, String),
    quantity: Decimal,
    reference: Decimal,
    sign: Decimal,
    normal_median: Decimal,
    entry_pct: Decimal,
    exit_cross_taker_pct: Decimal,
    exit_cross_maker_pct: Decimal,
    fee_taker_exit_pct: Decimal,
    fee_maker_exit_pct: Decimal,
    net_taker_exit_pct: Decimal,
    net_maker_exit_pct: Option<Decimal>,
    session: Session,
    /// 挂上的时刻（价格从这时起不动，至少 [`MIN_REST`]）。
    placed_at: Instant,
}

/// 一次采样：成交后 `after_ms` 时对面的对冲价与换算出的收益。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sample {
    /// 计划的采样时刻（毫秒）与实际距成交多久。
    pub horizon_ms: u64,
    pub after_ms: u64,
    /// 对面按深度吃单对冲的均价；盘口不可用 / 深度不够为 `None`。
    pub hedge_price: Option<Decimal>,
    pub entry_pct: Option<Decimal>,
    pub net_taker_exit_pct: Option<Decimal>,
    pub net_maker_exit_pct: Option<Decimal>,
    /// 那一刻 Binance 的中间价（看成交之后价格往哪边走）。
    pub maker_mid: Option<Decimal>,
}

/// 一笔虚拟成交的完整记录（落盘的一行）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Fill {
    pub at: DateTime<Utc>,
    pub pair: String,
    pub base: String,
    pub direction: String,
    /// Binance 这条腿是多还是空。
    pub maker_long: bool,
    pub session: Session,
    pub maker_price: Decimal,
    /// 穿过挂单价的那笔成交。
    pub trade_price: Decimal,
    pub trade_quantity: Decimal,
    pub reference: Decimal,
    pub normal_median: Decimal,
    /// 挂着时（成交前最后一次重算）报的入场价差与净收益。
    pub quoted_entry_pct: Decimal,
    pub quoted_net_taker_exit_pct: Decimal,
    pub quoted_net_maker_exit_pct: Option<Decimal>,
    pub fee_taker_exit_pct: Decimal,
    pub fee_maker_exit_pct: Decimal,
    /// 成交时这张虚拟单已经挂了多久（毫秒）。
    #[serde(default)]
    pub quote_age_ms: u64,
    pub samples: Vec<Sample>,
}

impl Fill {
    /// 某个采样时刻的「回到正常」净收益（吃单退出）。
    pub fn net_at(&self, horizon_ms: u64) -> Option<Decimal> {
        self.samples
            .iter()
            .find(|sample| sample.horizon_ms == horizon_ms)
            .and_then(|sample| sample.net_taker_exit_pct)
    }
}

/// 成交后还在采样的记录。
struct Pending {
    fill: Fill,
    quote: Quote,
    at: Instant,
}

/// 一个（组, 合约, 方向）的汇总。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Stat {
    pub pair: String,
    pub base: String,
    pub direction: String,
    pub fills: usize,
    /// 最近一笔。
    pub last_at: DateTime<Utc>,
    /// 中位数（%）：挂着时报的净收益，与成交后各采样时刻实际能拿到的净收益（吃单退出）。
    pub median_quoted_net_pct: Option<Decimal>,
    pub median_net_pct: Vec<(u64, Option<Decimal>)>,
    /// 成交后 1 秒净收益仍为正的比例。
    pub positive_share_1s: Option<Decimal>,
}

/// 给接口的汇总。
#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Summary {
    /// 正在挂着的虚拟单数。
    pub quoting: usize,
    /// 记录里的虚拟成交（最近 [`KEEP_DAYS`] 天）。
    pub fills: usize,
    pub since: Option<DateTime<Utc>>,
    /// 按虚拟成交笔数从多到少。
    pub stats: Vec<Stat>,
}

/// 虚拟挂单、成交与采样的状态。主循环里单线程使用。
pub struct Shadow {
    dir: PathBuf,
    quotes: HashMap<Key, Quote>,
    last_fill: HashMap<Key, Instant>,
    pending: Vec<Pending>,
    recent: VecDeque<Fill>,
    /// 后台写盘任务（不在监控主循环里等文件 IO）。
    writes: Vec<tokio::task::JoinHandle<()>>,
    /// 上次清理旧文件是哪天（一天最多清一次）。
    pruned_on: Option<chrono::NaiveDate>,
}

impl Shadow {
    /// 读回最近的记录（坏行跳过并计数）。
    pub async fn load(dir: &Path, now: DateTime<Utc>) -> (Self, usize) {
        let mut recent = VecDeque::new();
        let mut broken = 0;
        for back in (0..=KEEP_DAYS).rev() {
            let day = (now - chrono::Duration::days(back)).date_naive();
            let path = super::history::file_for(dir, FILE_PREFIX, day);
            let Ok(text) = tokio::fs::read_to_string(&path).await else {
                continue;
            };
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                match serde_json::from_str::<Fill>(line) {
                    Ok(fill) => {
                        recent.push_back(fill);
                        if recent.len() > RECENT_MAX {
                            recent.pop_front();
                        }
                    }
                    Err(_) => broken += 1,
                }
            }
        }
        (
            Self {
                dir: dir.to_path_buf(),
                quotes: HashMap::new(),
                last_fill: HashMap::new(),
                pending: Vec::new(),
                recent,
                writes: Vec::new(),
                pruned_on: None,
            },
            broken,
        )
    }

    /// 重算之后更新一行的虚拟挂单：没挂着、或挂满 [`MIN_REST`] 且最优价变了，就按最优价（够门槛时）挂；
    /// 否则价格不动，按挂单价对着现在的盘口重算，不够门槛就撤。
    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        pair: Pair,
        market: &PairMarket,
        view: Option<&MakerView>,
        book_a: Option<&LocalBook>,
        book_b: Option<&LocalBook>,
        normal_median: Option<Decimal>,
        session: Session,
        size_usdt: Decimal,
        now: Instant,
    ) {
        for (direction, long_a) in [("long_a", true), ("long_b", false)] {
            let key: Key = (pair.id(), market.base.clone(), direction);
            let existing = self.quotes.get(&key);
            let next = (|| {
                let view = view?;
                let median = normal_median?;
                let maker_is_a = pair.a == view.venue;
                let (maker_market, other_venue, other_market) = if maker_is_a {
                    (&market.a, pair.b, &market.b)
                } else {
                    (&market.b, pair.a, &market.a)
                };
                let (book_a, book_b) = (book_a?, book_b?);
                let (maker_book, other_book) = if maker_is_a {
                    (book_a, book_b)
                } else {
                    (book_b, book_a)
                };
                let reference = (book_a.mid()? + book_b.mid()?) / Decimal::TWO;
                (reference > Decimal::ZERO).then_some(())?;
                let maker_long = long_a == maker_is_a;
                let sign = if long_a { Decimal::ONE } else { -Decimal::ONE };
                // 按某个挂单价对着现在的盘口算一张虚拟单（不够门槛为 None）。
                let quote_at = |price: Decimal, placed_at: Instant| {
                    let (entry_pct, exit_cross_taker_pct, exit_cross_maker_pct) =
                        maker_leg_at(maker_book, maker_long, other_book, size_usdt, price)?;
                    let net = |cross: Decimal, fee: Decimal| {
                        (entry_pct - cross - fee + sign * median).round_dp(5)
                    };
                    let net_taker_exit_pct = net(exit_cross_taker_pct, view.fee_taker_exit_pct);
                    (net_taker_exit_pct >= QUOTE_MIN_NET_PCT).then(|| Quote {
                        market: maker_market.key.clone(),
                        maker_long,
                        price,
                        other: (other_venue, other_market.key.clone()),
                        quantity: size_usdt / reference,
                        reference,
                        sign,
                        normal_median: median,
                        entry_pct,
                        exit_cross_taker_pct,
                        exit_cross_maker_pct,
                        fee_taker_exit_pct: view.fee_taker_exit_pct,
                        fee_maker_exit_pct: view.fee_maker_exit_pct,
                        net_taker_exit_pct,
                        net_maker_exit_pct: Some(net(
                            exit_cross_maker_pct,
                            view.fee_maker_exit_pct,
                        )),
                        session,
                        placed_at,
                    })
                };
                let best = if long_a {
                    view.long_a.as_ref()
                } else {
                    view.long_b.as_ref()
                }
                .map(|leg| leg.price);
                let may_reprice = existing
                    .is_none_or(|quote| now.saturating_duration_since(quote.placed_at) >= MIN_REST);
                if may_reprice
                    && let Some(best) = best
                    && existing.is_none_or(|quote| quote.price != best)
                    && let Some(fresh) = quote_at(best, now)
                {
                    return Some(fresh);
                }
                let quote = existing?;
                quote_at(quote.price, quote.placed_at)
            })();
            match next {
                Some(quote) => {
                    self.quotes.insert(key, quote);
                }
                None => {
                    self.quotes.remove(&key);
                }
            }
        }
    }

    /// 某组的合约不再监控（重建组合后）：撤掉它们的虚拟单。
    pub fn retain_markets(&mut self, keep: &dyn Fn(&str, &str) -> bool) {
        self.quotes.retain(|(pair, base, _), _| keep(pair, base));
    }

    /// Binance 的一笔成交：穿过哪张虚拟单就记一笔虚拟成交，并立刻采第一个样。
    #[allow(clippy::too_many_arguments)]
    pub fn on_trade(
        &mut self,
        market: &str,
        price: Decimal,
        quantity: Decimal,
        seller_initiated: bool,
        books: &HashMap<(Venue, String), LocalBook>,
        maker_venue: Venue,
        now: Instant,
        now_utc: DateTime<Utc>,
    ) {
        let crossed: Vec<Key> = self
            .quotes
            .iter()
            .filter(|(_, quote)| {
                quote.market == market
                    && crosses(quote.maker_long, quote.price, price, seller_initiated)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in crossed {
            if self
                .last_fill
                .get(&key)
                .is_some_and(|at| now.saturating_duration_since(*at) < FILL_COOLDOWN)
            {
                continue;
            }
            let Some(quote) = self.quotes.get(&key).cloned() else {
                continue;
            };
            self.last_fill.insert(key.clone(), now);
            let fill = Fill {
                at: now_utc,
                pair: key.0.clone(),
                base: key.1.clone(),
                direction: key.2.to_string(),
                maker_long: quote.maker_long,
                session: quote.session,
                maker_price: quote.price,
                trade_price: price,
                trade_quantity: quantity,
                reference: quote.reference,
                normal_median: quote.normal_median,
                quoted_entry_pct: quote.entry_pct,
                quoted_net_taker_exit_pct: quote.net_taker_exit_pct,
                quoted_net_maker_exit_pct: quote.net_maker_exit_pct,
                fee_taker_exit_pct: quote.fee_taker_exit_pct,
                fee_maker_exit_pct: quote.fee_maker_exit_pct,
                quote_age_ms: u64::try_from(
                    now.saturating_duration_since(quote.placed_at).as_millis(),
                )
                .unwrap_or(u64::MAX),
                samples: Vec::new(),
            };
            let mut pending = Pending {
                fill,
                quote,
                at: now,
            };
            sample_due(&mut pending, books, maker_venue, now);
            self.pending.push(pending);
        }
    }

    /// 采到期的样；全部采完的交给后台写盘。返回采完的笔数。
    pub fn tick(
        &mut self,
        books: &HashMap<(Venue, String), LocalBook>,
        maker_venue: Venue,
        now: Instant,
    ) -> usize {
        for pending in &mut self.pending {
            sample_due(pending, books, maker_venue, now);
        }
        let (done, waiting): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|pending| pending.fill.samples.len() == HORIZONS_MS.len());
        self.pending = waiting;
        if done.is_empty() {
            return 0;
        }
        let fills: Vec<Fill> = done.into_iter().map(|pending| pending.fill).collect();
        let now_utc = Utc::now();
        let prune = self.pruned_on != Some(now_utc.date_naive());
        if prune {
            self.pruned_on = Some(now_utc.date_naive());
        }
        self.writes.retain(|write| !write.is_finished());
        let (dir, rows) = (self.dir.clone(), fills.clone());
        self.writes.push(tokio::spawn(async move {
            if let Err(error) = append(&dir, &rows, now_utc, prune).await {
                warn!("挂单测算记录写不进去：{error}");
            }
        }));
        let count = fills.len();
        for fill in fills {
            self.recent.push_back(fill);
            if self.recent.len() > RECENT_MAX {
                self.recent.pop_front();
            }
        }
        count
    }

    /// 等后台写盘都做完（停机、测试）。
    pub async fn settle(&mut self) {
        for write in std::mem::take(&mut self.writes) {
            let _ = write.await;
        }
    }

    /// 给接口的汇总（最近 [`KEEP_DAYS`] 天）。
    pub fn summary(&self, now: DateTime<Utc>) -> Summary {
        let cutoff = now - chrono::Duration::days(KEEP_DAYS);
        let mut groups: HashMap<(String, String, String), Vec<&Fill>> = HashMap::new();
        for fill in self.recent.iter().filter(|fill| fill.at >= cutoff) {
            groups
                .entry((fill.pair.clone(), fill.base.clone(), fill.direction.clone()))
                .or_default()
                .push(fill);
        }
        let mut stats: Vec<Stat> = groups
            .into_iter()
            .map(|((pair, base, direction), fills)| stat(pair, base, direction, &fills))
            .collect();
        stats.sort_by(|a, b| {
            b.fills
                .cmp(&a.fills)
                .then_with(|| a.pair.cmp(&b.pair))
                .then_with(|| a.base.cmp(&b.base))
        });
        Summary {
            quoting: self.quotes.len(),
            fills: stats.iter().map(|stat| stat.fills).sum(),
            since: self.recent.iter().map(|fill| fill.at).min(),
            stats,
        }
    }
}

/// 一笔成交是否**严格穿过**挂单价：买单要有卖方主动、成交价低于挂单价；卖单反之。同价不算（排队位置不知道）。
pub fn crosses(maker_long: bool, quote: Decimal, trade: Decimal, seller_initiated: bool) -> bool {
    if maker_long {
        seller_initiated && trade < quote
    } else {
        !seller_initiated && trade > quote
    }
}

/// 采所有到期的样（同一时刻可能到期好几个：主循环最慢每秒转一次）。
fn sample_due(
    pending: &mut Pending,
    books: &HashMap<(Venue, String), LocalBook>,
    maker_venue: Venue,
    now: Instant,
) {
    let elapsed = now.saturating_duration_since(pending.at);
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    while let Some(&horizon_ms) = HORIZONS_MS.get(pending.fill.samples.len()) {
        if elapsed_ms < horizon_ms {
            break;
        }
        let quote = &pending.quote;
        let other = books.get(&quote.other);
        let hedge_price = other.and_then(|book| {
            if quote.maker_long {
                book.sell_avg(quote.quantity)
            } else {
                book.buy_avg(quote.quantity)
            }
        });
        let maker_mid = books
            .get(&(maker_venue, quote.market.clone()))
            .and_then(LocalBook::mid);
        pending.fill.samples.push(sample(
            quote,
            horizon_ms,
            elapsed_ms,
            hedge_price,
            maker_mid,
        ));
    }
}

/// 一次采样换算：对冲价 → 入场价差 → 「回到正常」净收益（平仓穿价与手续费按挂着时的估）。
fn sample(
    quote: &Quote,
    horizon_ms: u64,
    after_ms: u64,
    hedge_price: Option<Decimal>,
    maker_mid: Option<Decimal>,
) -> Sample {
    let entry_pct = hedge_price.map(|hedge| {
        let spread = if quote.maker_long {
            hedge - quote.price
        } else {
            quote.price - hedge
        };
        (spread / quote.reference * Decimal::ONE_HUNDRED).round_dp(5)
    });
    let net = |cross: Decimal, fee: Decimal| {
        entry_pct.map(|entry| (entry - cross - fee + quote.sign * quote.normal_median).round_dp(5))
    };
    Sample {
        horizon_ms,
        after_ms,
        hedge_price,
        entry_pct,
        net_taker_exit_pct: net(quote.exit_cross_taker_pct, quote.fee_taker_exit_pct),
        net_maker_exit_pct: net(quote.exit_cross_maker_pct, quote.fee_maker_exit_pct),
        maker_mid,
    }
}

fn median(mut values: Vec<Decimal>) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    values.sort();
    let mid = values.len() / 2;
    Some(if values.len() % 2 == 1 {
        values[mid]
    } else {
        ((values[mid - 1] + values[mid]) / Decimal::TWO).round_dp(5)
    })
}

fn stat(pair: String, base: String, direction: String, fills: &[&Fill]) -> Stat {
    let at_1s: Vec<Decimal> = fills.iter().filter_map(|fill| fill.net_at(1_000)).collect();
    let positive = at_1s.iter().filter(|net| **net > Decimal::ZERO).count();
    Stat {
        pair,
        base,
        direction,
        fills: fills.len(),
        last_at: fills.iter().map(|fill| fill.at).max().unwrap_or_default(),
        median_quoted_net_pct: median(
            fills
                .iter()
                .map(|fill| fill.quoted_net_taker_exit_pct)
                .collect(),
        ),
        median_net_pct: HORIZONS_MS
            .iter()
            .map(|&horizon| {
                (
                    horizon,
                    median(
                        fills
                            .iter()
                            .filter_map(|fill| fill.net_at(horizon))
                            .collect(),
                    ),
                )
            })
            .collect(),
        positive_share_1s: (!at_1s.is_empty()).then(|| {
            (Decimal::from(positive) / Decimal::from(at_1s.len()) * Decimal::ONE_HUNDRED)
                .round_dp(1)
        }),
    }
}

/// 逐笔追加到当天的文件；`prune` 时顺便删掉 [`KEEP_DAYS`] 天前的。
async fn append(
    dir: &Path,
    fills: &[Fill],
    now: DateTime<Utc>,
    prune: bool,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    tokio::fs::create_dir_all(dir).await?;
    let mut text = String::new();
    for fill in fills {
        text.push_str(&serde_json::to_string(fill).map_err(std::io::Error::other)?);
        text.push('\n');
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(super::history::file_for(dir, FILE_PREFIX, now.date_naive()))
        .await?;
    file.write_all(text.as_bytes()).await?;
    file.flush().await?;
    if !prune {
        return Ok(());
    }
    let oldest = super::history::file_for(
        dir,
        FILE_PREFIX,
        (now - chrono::Duration::days(KEEP_DAYS)).date_naive(),
    );
    let head = format!("{FILE_PREFIX}-");
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let dated = name
                .strip_prefix(&head)
                .and_then(|rest| rest.strip_suffix(".jsonl"))
                .is_some_and(|day| day.len() == 8 && day.bytes().all(|c| c.is_ascii_digit()));
            if dated && path < oldest {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
    Ok(())
}
