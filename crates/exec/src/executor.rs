//! 双腿执行。
//!
//! # 为什么这件事比「下两单」复杂
//!
//! 交易所没有「原子地下两单」这个操作。所以双腿执行永远存在一个窗口：第一腿成了、
//! 第二腿还没成。这个窗口里的仓位是**单腿裸奔**的 —— 方向性敞口，与套利无关。
//!
//! 处理它只有三条路，这里三条都做：
//!
//! 1. **把难成交的腿排在前面**（[`Plan::first_leg`]）。它失败时你手上什么都没有。
//! 2. **失败就回滚**（[`Executor::unwind`]）。已成交的部分按反向单平掉，
//!    并明确记成 `Unwound` 而不是「开仓失败」—— 后者会让人以为什么都没发生。
//! 3. **超时/断线后靠对账**（[`crate::reconcile`]）发现本地不知道的成交。
//!
//! 全程用**幂等**的 `client_order_id`：超时重试复用同一个 id，由交易所侧去重。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arb_core::{ArbError, ArbResult, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::broker::{Broker, leg_fill};
use crate::ledger::{Ledger, Record};
use crate::preflight::{LegPlan, Plan};
use crate::report::{OpenTiming, open_report};
use crate::types::{
    ClientOrderId, LegFill, NewOrder, OrderRole, OrderState, OrderStatus, PairPosition,
    PositionStatus, RealizedSource, Strategy, TaskRules,
};

/// 建仓时就定下来、之后由监控使用的设置。
#[derive(Debug, Clone, Default)]
pub struct PositionSetup {
    pub margin_mode: crate::MarginMode,
    /// 两腿的开仓杠杆。`None` = 不记录（强平距离就无从算起）。
    pub leverage: Option<Decimal>,
    pub rules: TaskRules,
    /// 取盘口（报价）的时刻。执行报告据此算「报价到成交」用了多久；`None` 就不算。
    pub quoted_at: Option<DateTime<Utc>>,
    /// 调用方已经在下单前（同一把交易锁内、拉盘口的同时）用 [`ensure_flat`] 核对过两腿场所都没有这个
    /// 合约的持仓：执行器不必在报价之后再各查一次。`false` 就照常在下单前查。
    pub positions_checked: bool,
    /// 强制补腿（价差单、两腿都吃单才用；默认关）：第一腿成交后，第二腿在保本价内没吃满（一点没吃到、或只吃到一部分）时，
    /// 不回滚，而是把**剩下的数量**按「两腿锁定价差不低于 `−这个数`」的最差限价追上（小数，0.003 = −0.3%，
    /// 见 [`crate::preflight::Plan::forced_after`]）。追上就是一笔锁定了负价差的对冲仓位，追不上才回滚。
    /// 它不消除亏损，只是把回滚兑现的亏损换成锁定的负价差。
    pub force_hedge: Option<Decimal>,
}

/// 执行结果。
pub struct ExecutionOutcome {
    pub position: PairPosition,
}

impl ExecutionOutcome {
    /// 仓位是否真的建立起来了（两腿都在）。
    pub fn is_open(&self) -> bool {
        self.position.status == PositionStatus::Open && self.position.is_hedged()
    }
}

pub struct Executor {
    brokers: HashMap<Venue, Arc<dyn Broker>>,
    ledger: Arc<Ledger>,
}

impl Executor {
    pub fn new(ledger: Arc<Ledger>, brokers: Vec<Arc<dyn Broker>>) -> Self {
        Self {
            brokers: brokers
                .into_iter()
                .map(|broker| (broker.venue(), broker))
                .collect(),
            ledger,
        }
    }

    fn broker(&self, venue: Venue) -> ArbResult<&Arc<dyn Broker>> {
        self.brokers
            .get(&venue)
            .ok_or_else(|| ArbError::config(format!("{venue} 没有配置券商，无法执行")))
    }

    /// 这笔仓位现在平不平得了：还在的腿里有一家这个合约下不了平仓单（休市、维护，见
    /// [`Broker::trading_halted`]）就返回原因 —— 这时两条腿都不要动，否则另一家平掉了、这一家平不掉，
    /// 剩一条裸腿。查不到状态的照常平（不知道不等于下不了；交易所自己会拒）。
    pub async fn exit_halted(&self, position: &PairPosition) -> Option<String> {
        let venues = [position.long.as_ref(), position.short.as_ref()]
            .into_iter()
            .flatten()
            .map(|leg| leg.venue);
        for venue in venues {
            let Some(broker) = self.brokers.get(&venue) else {
                continue;
            };
            match broker.trading_halted(&position.symbol, true).await {
                Ok(Some(reason)) => return Some(format!("{venue}：{reason}")),
                Ok(None) => {}
                Err(error) => {
                    warn!(position = %position.id, %venue, "查不到合约能不能平仓，照常平：{error}");
                }
            }
        }
        None
    }

    /// 两条腿在交易所实际的保证金状态。查不到的（没接入、出错）是 `None`，评估会回落到台账里
    /// 开仓时的保证金 —— 出错时不能拿空值顶替，更不能中断规则轮。
    pub async fn leg_states(
        &self,
        position: &PairPosition,
    ) -> (
        Option<crate::broker::VenueLegState>,
        Option<crate::broker::VenueLegState>,
    ) {
        let fetch = |leg: Option<&LegFill>| {
            let leg = leg.cloned();
            async move {
                let leg = leg?;
                let broker = self.brokers.get(&leg.venue)?;
                match broker.leg_state(&position.symbol).await {
                    Ok(state) => state,
                    Err(error) => {
                        warn!(venue = %leg.venue, symbol = %position.symbol, "读交易所保证金没成功，强平价按台账里的保证金算：{error}");
                        None
                    }
                }
            }
        };
        (
            fetch(position.long.as_ref()).await,
            fetch(position.short.as_ref()).await,
        )
    }

    /// 持仓期间这些场所实际结算的资金费合计（交易所流水）。任何一家没接入或没查到就返回
    /// `None`：不能拿只查到的一部分冒充合计。
    pub(crate) async fn funding_total(
        &self,
        venues: &[Venue],
        symbol: &arb_core::Symbol,
        since: chrono::DateTime<Utc>,
    ) -> Option<Decimal> {
        let mut total = Decimal::ZERO;
        for venue in venues {
            match self.brokers.get(venue)?.funding_since(symbol, since).await {
                Ok(Some(funding)) => total += funding.usdt,
                _ => return None,
            }
        }
        Some(total)
    }

    /// 已结束仓位 `[from, until]` 内两条腿实际结算的资金费合计。任一家没接入或没查到就是 `None`。
    pub(crate) async fn funding_window(
        &self,
        venues: &[Venue],
        symbol: &arb_core::Symbol,
        from: chrono::DateTime<Utc>,
        until: chrono::DateTime<Utc>,
    ) -> Option<Decimal> {
        funding_window(&self.brokers, venues, symbol, from, until).await
    }

    /// 平这笔仓位时两条腿各按什么费率估手续费：所在场所账户的吃单费率；没配券商的腿为 `None`
    /// （见 [`crate::monitor::ExitFees`]）。
    pub fn exit_fees(&self, position: &PairPosition) -> crate::monitor::ExitFees {
        let rate = |leg: Option<&LegFill>| {
            leg.and_then(|leg| self.brokers.get(&leg.venue))
                .map(|broker| broker.fee_per_side())
        };
        crate::monitor::ExitFees {
            long: rate(position.long.as_ref()),
            short: rate(position.short.as_ref()),
        }
    }

    /// 这个场所账户现在的可用保证金（计价币）。没接入或查不到是 `None`：不知道不等于不够。
    pub async fn free_collateral(&self, venue: Venue) -> Option<Decimal> {
        match self.brokers.get(&venue)?.free_collateral().await {
            Ok(free) => free,
            Err(error) => {
                warn!(%venue, "读账户可用保证金没成功：{error}");
                None
            }
        }
    }

    /// 这个场所有没有接入补保证金。
    pub fn supports_add_margin(&self, venue: Venue) -> bool {
        self.brokers
            .get(&venue)
            .is_some_and(|broker| broker.supports_add_margin())
    }

    /// 往一条腿的逐仓保证金里补 `amount_usdt`（自动加保证金规则执行用）。**动真钱。**
    ///
    /// 记账顺序是整件事的安全所在：
    /// 1. 先把**累计补了多少**（`margin_added_usdt`）连同这次的金额落盘，再发请求 —— 进程死在
    ///    中间，上限也已经按这次算过了，偏保守；
    /// 2. 交易所明确拒绝 / 根本没发出请求：把这次的金额退回去再落盘（没动钱，不占上限）；
    /// 3. 结果不明：**不退**（可能已经到账），留在累计里。下一轮按交易所读回的真实保证金重新
    ///    评估，不会因为超时重试把上限冲破。
    ///
    /// 上限在这里再校验一遍（规则评估已经夹过，这是第二道）。
    pub async fn add_margin(
        &self,
        position: &mut PairPosition,
        venue: Venue,
        amount_usdt: Decimal,
        reason: &str,
    ) -> ArbResult<crate::broker::MarginOutcome> {
        use crate::broker::MarginOutcome;
        if position.status != PositionStatus::Open {
            return Err(ArbError::config(format!(
                "仓位 {} 不是 Open，不补保证金",
                position.id
            )));
        }
        if position.margin_mode.is_cross() {
            return Err(ArbError::config("全仓不能追加逐仓保证金"));
        }
        if amount_usdt <= Decimal::ZERO {
            return Err(ArbError::config("补保证金的金额必须为正"));
        }
        let Some((_, cap)) = position.rules.auto_margin() else {
            return Err(ArbError::config("这笔仓位没有开启自动加保证金"));
        };
        if position.margin_added_usdt + amount_usdt > cap {
            return Err(ArbError::config(format!(
                "再补 {amount_usdt} USDT 会超过累计上限 {cap} USDT（已补 {}）",
                position.margin_added_usdt
            )));
        }
        let is_long = match (position.long.as_ref(), position.short.as_ref()) {
            (Some(leg), _) if leg.venue == venue => true,
            (_, Some(leg)) if leg.venue == venue => false,
            _ => {
                return Err(ArbError::config(format!(
                    "仓位 {} 在 {venue} 上没有腿",
                    position.id
                )));
            }
        };
        let broker = self.broker(venue)?;
        if !broker.supports_add_margin() {
            return Err(ArbError::venue(venue.as_str(), "这个场所没有接入补保证金"));
        }

        position.margin_added_usdt += amount_usdt;
        position.note = Some(format!(
            "正在往 {venue} 补保证金 {amount_usdt} USDT：{reason}"
        ));
        self.persist(position).await?;

        let outcome = broker.add_margin(&position.symbol, amount_usdt).await;
        let leg = if is_long {
            position.long.as_mut()
        } else {
            position.short.as_mut()
        };
        match &outcome {
            Ok(MarginOutcome::Applied) => {
                if let Some(leg) = leg {
                    leg.margin_usdt = Some(leg.margin_usdt.unwrap_or_default() + amount_usdt);
                }
                position.note = Some(format!(
                    "自动加保证金：往 {venue} 补了 {amount_usdt} USDT（{reason}）"
                ));
                info!(position = %position.id, %venue, %amount_usdt, "自动加保证金已生效");
            }
            Ok(MarginOutcome::Unknown(why)) => {
                position.note = Some(format!(
                    "自动加保证金：往 {venue} 补 {amount_usdt} USDT 的结果不明（{why}），按已补计入上限；下一轮按交易所的真实保证金核对"
                ));
                warn!(position = %position.id, %venue, %amount_usdt, %why, "自动加保证金结果不明");
            }
            Ok(MarginOutcome::Refused(why)) => {
                position.margin_added_usdt -= amount_usdt;
                position.note = Some(format!("自动加保证金被 {venue} 拒绝：{why}"));
                warn!(position = %position.id, %venue, %amount_usdt, %why, "自动加保证金被拒绝");
            }
            Err(error) => {
                position.margin_added_usdt -= amount_usdt;
                position.note = Some(format!("自动加保证金没有发出：{error}"));
                warn!(position = %position.id, %venue, %amount_usdt, %error, "自动加保证金没有发出");
            }
        }
        self.persist(position).await?;
        outcome
    }

    /// 建一笔双腿仓位。
    pub async fn open(
        &self,
        plan: &Plan,
        strategy: Strategy,
        entry_basis_pct: Decimal,
        position_id: &str,
        setup: &PositionSetup,
    ) -> ArbResult<ExecutionOutcome> {
        crate::margin::validate_rules(setup.margin_mode, &setup.rules).map_err(ArbError::config)?;
        // 开了自动加保证金，两条腿所在场所都得接入补保证金：否则规则只会在出事时才发现补不了。
        if setup.rules.auto_margin().is_some() {
            for leg in [&plan.long, &plan.short] {
                if !self.supports_add_margin(leg.venue) {
                    return Err(ArbError::venue(
                        leg.venue.as_str(),
                        "这个场所没有接入补保证金，不能开启自动加保证金",
                    ));
                }
            }
        }
        // 只挂单只能是第一腿，而且那家券商得真的支持：一张单都没发、仓位也没落盘之前就拒绝。
        if plan.second_leg().maker_rest_ms.is_some() {
            return Err(ArbError::config("只挂单只能是双腿开仓的第一腿"));
        }
        let first_plan = plan.first_leg();
        if let Some(rest_ms) = first_plan.maker_rest_ms {
            let probe = leg_order(
                ClientOrderId::for_leg(position_id, first_plan.side, 0),
                first_plan,
                setup.leverage,
                setup.margin_mode,
                None,
            );
            self.check_post_only(&probe, Some(Duration::from_millis(rest_ms)))?;
        }
        let mut position = PairPosition {
            id: position_id.to_string(),
            symbol: plan.symbol.clone(),
            strategy,
            long: None,
            short: None,
            entry_basis_pct,
            expected_round_trip_cost: plan.expected_cost,
            status: PositionStatus::Opening,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: setup.leverage,
            margin_mode: setup.margin_mode,
            rules: setup.rules.clone(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: Some(crate::types::PlannedLegs {
                long: plan.long.venue,
                short: plan.short.venue,
                first: plan.first_leg().side,
                force_hedge: setup.force_hedge,
            }),
        };
        self.persist(&position).await?;
        let started_at = Utc::now();

        // 第一腿：难成交的那条。它失败时手上什么都没有。限价按保本线封顶（见 [`Plan::first_leg_capped`]）。
        let first = plan.first_leg_capped();
        let second = plan.second_leg().clone();

        // 下单前的准备（杠杆核对、市场元数据）两条腿并发做完：第二腿下单时不用再单独往返一次，
        // 第二腿的杠杆设不上也在第一腿成交之前就发现，不必先成交一条再回滚。两腿能不能同时下（要读两家的
        // 数量步长，元数据都已缓存）也一起看。
        let (first_ready, second_ready, together) = tokio::join!(
            self.prepare_leg(
                &first,
                setup.leverage,
                setup.margin_mode,
                setup.positions_checked
            ),
            self.prepare_leg(
                &second,
                setup.leverage,
                setup.margin_mode,
                setup.positions_checked
            ),
            self.concurrent_quantity(plan, &first, &second)
        );
        if let Err(error) = first_ready.and(second_ready) {
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some(format!("开仓前的准备没通过，没有下任何单：{error}"));
            self.persist(&position).await?;
            warn!(position = %position.id, note = ?position.note, "建仓中止：开仓前准备失败");
            return Ok(ExecutionOutcome { position });
        }
        let ready_at = Utc::now();
        // 各腿各自的往返耗时（下单到拿到结果），记进「双腿建仓完成」：慢在哪家、哪一步，看这里。
        let first_started = Instant::now();
        let first_id = ClientOrderId::for_leg(position_id, first.side, 0);

        // 两腿同时下（见 [`Self::concurrent_quantity`]）：第二腿的第 0 次和第一腿一起发出，数量相同；限价按第一腿的
        // **预估**均价收紧到保本线（顺序下时按第一腿的实际均价，所以第一腿成交得比预估差时，锁定价差可能比保本线低
        // 那么一点）。它的结果当作第二腿的第 0 次，交给下面的重试、强制补腿、配平与回滚。
        // 同时下时第一腿自己什么时候返回的（耗时、时刻）：两条腿是一起等的，等完再取就成了两腿里慢的那个
        // （11-07～11-14 两条腿都在 Lighter 上的 first_ms 记成 1357～1433ms，其实第一腿约 350ms 就确认了）。
        let (first_result, first_done, early) = match together {
            Some(quantity) => {
                let sized = |leg: &LegPlan| LegPlan {
                    notional_usdt: quantity * leg.limit_price,
                    ..leg.clone()
                };
                let first_leg = sized(&first);
                let early_leg = sized(plan.hedged_after(first.expected_price).second_leg());
                let ((first_result, first_done), early) = tokio::join!(
                    async {
                        let result = self
                            .submit_first(
                                first_id,
                                &first_leg,
                                setup.leverage,
                                setup.margin_mode,
                                Some(quantity),
                            )
                            .await;
                        (result, (first_started.elapsed().as_millis(), Utc::now()))
                    },
                    async {
                        let result = self
                            .submit(
                                position_id,
                                &early_leg,
                                0,
                                setup.leverage,
                                setup.margin_mode,
                                Some(quantity),
                            )
                            .await;
                        (result, first_started.elapsed().as_millis(), Utc::now())
                    }
                );
                (first_result, Some(first_done), Some(early))
            }
            None => (
                self.submit_first(first_id, &first, setup.leverage, setup.margin_mode, None)
                    .await,
                None,
                None,
            ),
        };
        let concurrent = early.is_some();
        // 同时发出的第二腿成交了的话先记进仓位（回滚、续上建仓都按仓位上的腿与开仓单来）。
        if let Some((Ok((Some(fill), _)), _, _)) = &early {
            set_leg(&mut position, fill);
        }
        let early_note = early.as_ref().map(|(result, _, _)| {
            let leg = format!(
                "同时发出的第二腿（{} {}）",
                second.venue,
                side_label(second.side)
            );
            match result {
                Ok((Some(fill), _)) => format!(
                    "{leg}成交了 {} USDT",
                    fill.notional_usdt.round_dp(2).normalize()
                ),
                Ok((None, reason)) => {
                    format!("{leg}没成交：{}", reason.as_deref().unwrap_or("未成交"))
                }
                Err(error) => format!("{leg}结果不明：{error}"),
            }
        });

        let (first_fill, first_reason, pending) = match first_result {
            Ok(result) => result,
            Err(error) => {
                // 第一腿结果不明：订单可能已经成交。记下原因再上抛，持仓页与台账里才看得到为什么停在这里。
                position.note = Some(format!(
                    "第一腿（{} {}）结果不明：{error}。{}先对账、不要重复开仓",
                    first.venue,
                    side_label(first.side),
                    early_note
                        .as_deref()
                        .map(|note| format!("{note}。"))
                        .unwrap_or_default()
                ));
                let _ = self.persist(&position).await;
                return Err(error);
            }
        };
        let (first_ms, first_filled_at) =
            first_done.unwrap_or_else(|| (first_started.elapsed().as_millis(), Utc::now()));
        let Some(mut first_fill) = first_fill else {
            let first_note = format!(
                "第一腿（{} {}）未成交：{}",
                first.venue,
                side_label(first.side),
                first_reason.unwrap_or_else(|| "未成交".into())
            );
            match &early {
                // 同时发出的第二腿成交了：一条裸腿，回滚它。
                Some((Ok((Some(_), _)), _, _)) => {
                    self.persist(&position).await?;
                    warn!(position = %position.id, first = %first.venue, second = %second.venue,
                        "第一腿未成交、同时发出的第二腿成交了，回滚第二腿");
                    self.unwind(&mut position).await?;
                    let why = format!("{first_note}；{}，已回滚", early_note.unwrap_or_default());
                    self.explain_unwind(&mut position, Some(&why)).await?;
                    return Ok(ExecutionOutcome { position });
                }
                // 同时发出的第二腿结果不明：可能已经成交，不能当什么都没发生。
                Some((Err(_), _, _)) => {
                    position.note = Some(format!(
                        "{first_note}；{}。先对账、不要重复开仓",
                        early_note.unwrap_or_default()
                    ));
                    let _ = self.persist(&position).await;
                    let Some((Err(error), _, _)) = early else {
                        unreachable!("上面刚匹配过")
                    };
                    return Err(error);
                }
                _ => {}
            }
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some(first_note);
            self.persist(&position).await?;
            warn!(position = %position.id, note = ?position.note, "建仓中止：第一腿未成交");
            return Ok(ExecutionOutcome { position });
        };
        set_leg(&mut position, &first_fill);
        self.persist(&position).await?;
        // 价差单的第一腿部分成交（薄盘口上先下的那条最常见）：成交的部分够下单就按它对冲第二腿，
        // 不回滚 —— 第二腿本来就按第一腿成交的数量下。太少（凑不够一张单）或资金费单才回滚。
        let hedge_min = if first.maker_rest_ms.is_some() {
            MAKER_PARTIAL_HEDGE_MIN_USDT
        } else {
            PARTIAL_HEDGE_MIN_USDT
        };
        let partial_hedge = first_reason.is_some()
            && plan.min_locked_basis.is_some()
            && first_fill.notional_usdt >= hedge_min;
        if let Some(reason) = first_reason.as_deref().filter(|_| partial_hedge) {
            info!(
                position = %position.id,
                %reason,
                filled = %first_fill.notional_usdt,
                planned = %first.notional_usdt,
                "第一腿部分成交，按成交的数量对冲第二腿"
            );
        }
        if first_reason.is_some() && !partial_hedge {
            // 同时发出的第二腿结果不明：不能回滚（它可能成交了，也可能没有），交给对账与续上建仓。
            if let Some((Err(_), _, _)) = &early {
                position.note = Some(format!(
                    "第一腿（{} {}）成交太少、要回滚，但{}。先对账、不要重复开仓",
                    first.venue,
                    side_label(first.side),
                    early_note.unwrap_or_default()
                ));
                let _ = self.persist(&position).await;
                let Some((Err(error), _, _)) = early else {
                    unreachable!("上面刚匹配过")
                };
                return Err(error);
            }
            // 回滚要按最终成交记账：先把待核实的第一腿核实掉（这时不赶时间）。
            if let Some(pending) = &pending
                && let Some(fill) = self.settle(pending).await?
            {
                first_fill = fill;
                set_leg(&mut position, &first_fill);
            }
            self.unwind(&mut position).await?;
            if first.maker_rest_ms.is_some() && plan.min_locked_basis.is_some() {
                self.explain_unwind(&mut position, Some(MAKER_DUST_NOTE))
                    .await?;
            }
            return Ok(ExecutionOutcome { position });
        }

        // 第二腿：数量按第一腿成交的数量；价差单的限价再按第一腿的实际均价收紧到保本线以内。
        let first_quantity = first_fill
            .quantity()
            .ok_or_else(|| ArbError::config("第一腿成交价不可用，不能计算对冲数量"))?;
        let hedged = plan.hedged_after(first_fill.average_price);
        let second = hedged.second_leg().clone();
        let breakeven = breakeven_note(plan, &hedged, &first_fill);
        let second_started = Instant::now();
        // 强制补腿只用在价差单、两腿都吃单的开仓上。开着时保本价只试一次（它每多试一次就多裸露两秒，
        // 12:37 某合约 在保本价上试两次就耗了 3.2 秒），不行马上追。
        let force = setup
            .force_hedge
            .filter(|_| plan.min_locked_basis.is_some() && first.maker_rest_ms.is_none());
        let same_limit_attempts = if force.is_some() {
            1
        } else {
            SECOND_LEG_ATTEMPTS
        };
        // 两腿同时下时，和第一腿一起发出的那一次就是第二腿的第 0 次（订单号也是 0）。
        let mut early = early;
        let submitted = async {
            // 第二腿一个都没吃到（交易所明确说了：IOC 没成交、被撤）时，盘口常常几百毫秒就补上了：
            // 在 [`SECOND_LEG_RETRY_WINDOW`] 内按同一个限价再试几次，都不行才回滚。结果不明（`Err`）
            // 与部分成交都不重试 —— 前者可能其实成交了，后者交给原来的「不足额就回滚」。
            let mut attempt = 0;
            // 最后一次结果来自同时发出的那一次：（从发出到返回的毫秒数, 返回时刻）。
            let mut from_early;
            let mut submitted = loop {
                let submitted = match early.take() {
                    Some((result, ms, at)) => {
                        from_early = Some((ms, at));
                        result
                    }
                    None => {
                        from_early = None;
                        self.submit(
                            position_id,
                            &second,
                            attempt,
                            setup.leverage,
                            setup.margin_mode,
                            Some(first_quantity),
                        )
                        .await
                    }
                };
                let retry = matches!(&submitted, Ok((None, Some(reason))) if !pointless_retry(reason))
                    && attempt + 1 < same_limit_attempts
                    && second_started.elapsed() + SECOND_LEG_RETRY_GAP < SECOND_LEG_RETRY_WINDOW;
                if !retry {
                    break submitted;
                }
                if let Ok((_, Some(reason))) = &submitted {
                    info!(position = %position_id, attempt, %reason, limit = %second.limit_price, "第二腿没吃到，按同一限价重试");
                }
                attempt += 1;
                tokio::time::sleep(SECOND_LEG_RETRY_GAP).await;
            };
            // 强制补腿：保本价内一点没吃到、或只吃到一部分（交易所明确说了「没成交」或「部分成交」）才追。
            // 结果不明（`Err`）绝不追 —— 那一单可能其实成交了，再追一单就多对冲一份，变成反方向的裸敞口。
            let mut forced = false;
            if let Some(tolerance) = force
                && matches!(&submitted, Ok((None, Some(_))) | Ok((Some(_), Some(_))))
            {
                let before = attempt + 1;
                let chased = self
                    .chase_second_leg(
                        position_id,
                        plan,
                        &first_fill,
                        &second,
                        tolerance,
                        before,
                        (setup.leverage, setup.margin_mode),
                        first_quantity,
                        submitted,
                    )
                    .await;
                attempt = before + FORCE_HEDGE_ATTEMPTS;
                // 只有真的发过追单才算「被追出来的」：保本价那一档已经几乎吃满（偏差 ≤ 0.5%）时一单都不发。
                (submitted, forced) = match chased {
                    Ok((result, sent)) => (Ok(result), sent),
                    Err(error) => (Err(error), false),
                };
            }
            // 第二腿的成交时刻在它自己返回时记：不等第一腿的核实（核实可能更慢，报告里的
            // 「两腿间隔」要的是只有一条腿的窗口）。同时下的，耗时从和第一腿一起发出时算起。
            let (second_ms, second_filled_at) = match from_early {
                Some((ms, at)) if !forced => (ms, at),
                _ if concurrent => (first_started.elapsed().as_millis(), Utc::now()),
                _ => (second_started.elapsed().as_millis(), Utc::now()),
            };
            (submitted, second_ms, second_filled_at, attempt + 1, forced)
        };
        // 第一腿的手续费与精确成交额和第二腿同时核实，不挡在第二腿前面。第二腿明确没成交时不等核实
        // （`deferred_settle`）：先回滚，回滚完再核实 —— 待核实的状态不落盘，不补这一步台账里那张单永远是 Pending。
        let mut deferred_settle = false;
        let ((submitted, second_ms, second_filled_at, second_attempts, forced), settled, settle_ms) =
            match &pending {
                Some(pending) => {
                    let settling = async {
                        let settled = self.settle(pending).await;
                        (settled, second_started.elapsed().as_millis())
                    };
                    tokio::pin!(submitted);
                    tokio::pin!(settling);
                    tokio::select! {
                        biased;
                        (settled, settle_ms) = &mut settling => {
                            (submitted.await, Some(settled), Some(settle_ms))
                        }
                        submitted = &mut submitted => {
                            // 第二腿明确一点没成交（交易所说了没成交，不是结果不明）：马上回滚第一腿，不再等它的核实。
                            // 读节点滞后时核实要十几秒（Lighter 的核实还占着下单要用的 journal 锁，回滚单发不出去），
                            // 第一腿就多裸这么久。回滚按待核实的成交记（数量是交易所报的准数，手续费可能是估的）。
                            if matches!(&submitted.0, Ok((None, Some(_)))) {
                                warn!(position = %position_id, first = %first.venue, "第二腿没成交：不等第一腿核实，马上回滚");
                                deferred_settle = true;
                                (submitted, None, None)
                            } else {
                                let (settled, settle_ms) = settling.await;
                                (submitted, Some(settled), Some(settle_ms))
                            }
                        }
                    }
                }
                None => (submitted.await, None, None),
            };
        if let Some(settled) = settled
            && let Some(fill) = settled?
        {
            first_fill = fill;
            set_leg(&mut position, &first_fill);
        }
        let first_quantity = first_fill.quantity().unwrap_or(first_quantity);
        let (second_fill, second_reason) = match submitted {
            Ok(result) => result,
            Err(error) => {
                // 第二腿结果不明（超时、限频、5xx）：第一腿已成交。**不自动回滚** —— 第二腿可能其实
                // 成交了，这时回滚第一腿会把对冲拆掉。仓位保持 Opening，写明原因；下一轮对账
                // 会如实报出两边的真实持仓，没成交的话由规则轮按「没走完的开仓」平掉第一腿。
                position.note = Some(format!(
                    "第二腿（{} {}）结果不明：{error}。第一腿已成交，先对账再决定，不要重复开仓",
                    second.venue,
                    side_label(second.side)
                ));
                let _ = self.persist(&position).await;
                return Err(error);
            }
        };

        match second_fill {
            Some(fill) => {
                let second_quantity = fill
                    .quantity()
                    .ok_or_else(|| ArbError::config("第二腿成交价不可用，无法核实对冲数量"))?;
                set_leg(&mut position, &fill);
                self.persist(&position).await?;
                let mismatch = (first_quantity - second_quantity).abs() / first_quantity;
                // 第二腿有成交但和第一腿数量对不上：两家下单数量步长不同（某次实盘：lighter-rh 卖
                // 0.09855、hyperliquid-io 只能下到 0.098）、或只吃到一部分。只要第二腿成交够一半，就不再两腿全回滚，
                // 把多出的那条腿的零头 reduce-only 退掉，按对上的数量持有（见 `balance_legs`）。
                let balanced = if second_reason.is_some() || mismatch > SECOND_LEG_QTY_TOLERANCE {
                    self.balance_legs(
                        &mut position,
                        (&first, first_quantity),
                        (&second, second_quantity),
                        second_reason.as_deref(),
                    )
                    .await?
                } else {
                    Ok(None)
                };
                if let Err(why) = balanced {
                    warn!(position = %position.id, %mismatch, limit = %second.limit_price, first = %first.venue, first_ms, second = %second.venue, second_ms, second_attempts, concurrent, %why, "第二腿未足额成交，回滚两腿");
                    self.unwind(&mut position).await?;
                    self.explain_unwind(&mut position, breakeven.as_deref())
                        .await?;
                } else {
                    position.status = PositionStatus::Open;
                    if let Ok(Some(note)) = &balanced {
                        position.note = Some(note.clone());
                    }
                    if partial_hedge {
                        let note = format!(
                            "第一腿（{} {}）只成交了 {} / {} USDT，按成交的数量对冲",
                            first.venue,
                            side_label(first.side),
                            first_fill.notional_usdt.round_dp(2).normalize(),
                            first.notional_usdt.round_dp(2).normalize()
                        );
                        position.note = Some(match position.note.take() {
                            Some(existing) if !existing.is_empty() => format!("{note}。{existing}"),
                            _ => note,
                        });
                    }
                    if forced {
                        let pct = |value: Decimal| value.round_dp(4).normalize().to_string();
                        let floor = hedged
                            .hedge_floor()
                            .map(|floor| floor * Decimal::ONE_HUNDRED);
                        let locked = locked_basis_pct(&position);
                        let note = format!(
                            "{FORCED_HEDGE_NOTE}：第二腿（{} {}）在保本价 {} 内没吃满，按最差限价（锁定价差不低于 −{}%）\
                            追到均价 {}；两腿锁定价差 {}%，{}。收敛到目标不一定赚钱，注意盯着",
                            second.venue,
                            side_label(second.side),
                            second.limit_price.round_dp(8).normalize(),
                            pct(setup.force_hedge.unwrap_or_default() * Decimal::ONE_HUNDRED),
                            fill.average_price.round_dp(8).normalize(),
                            locked.map_or("未知".to_string(), pct),
                            floor.map_or("保本线未知".to_string(), |floor| format!(
                                "低于保本线 {}%",
                                pct(floor)
                            ))
                        );
                        warn!(position = %position.id, symbol = %position.symbol, "{note}");
                        position.note = Some(match position.note.take() {
                            Some(existing) if !existing.is_empty() => format!("{existing}。{note}"),
                            _ => note,
                        });
                    }
                    position.open_report = open_report(
                        &hedged,
                        &first_fill,
                        &fill,
                        &OpenTiming {
                            quoted_at: setup.quoted_at,
                            started_at,
                            ready_at,
                            first_filled_at,
                            second_filled_at,
                        },
                    );
                    if let Some(report) = position.open_report.as_mut() {
                        report.first_order_ms = u64::try_from(first_ms).ok();
                        report.second_order_ms = u64::try_from(second_ms).ok();
                        report.second_attempts = Some(second_attempts);
                    }
                    self.persist(&position).await?;
                    info!(
                        position = %position.id,
                        symbol = %position.symbol,
                        notional = %position.total_notional(),
                        first = %first.venue,
                        first_ms,
                        second = %second.venue,
                        second_ms,
                        second_attempts,
                        settle_ms = ?settle_ms,
                        concurrent,
                        "双腿建仓完成"
                    );
                    if let Some(report) = &position.open_report {
                        info!(position = %position.id, "{}", report.summary());
                    }
                }
            }
            None => {
                warn!(position = %position.id, reason = ?second_reason, limit = %second.limit_price, first = %first.venue, first_ms, second = %second.venue, second_ms, second_attempts, concurrent, "第二腿未成交，回滚第一腿");
                let unwound = self.unwind(&mut position).await;
                if deferred_settle && let Some(pending) = &pending {
                    // 回滚已经发完，这时不赶时间：把第一腿的最终状态落进台账，再把仓位上按待核实成交记的
                    // 入场价与手续费换成最终的（HL 估的手续费按基础吃单费率，比实收高得多）。
                    match self.settle(pending).await {
                        Ok(Some(fill)) => {
                            self.correct_unwound_entry(&mut position, &first_fill, &fill)
                                .await
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(position = %position.id, %error, "回滚后核实第一腿没落成台账");
                        }
                    }
                }
                unwound?;
                self.explain_unwind(&mut position, breakeven.as_deref())
                    .await?;
            }
        }

        Ok(ExecutionOutcome { position })
    }

    /// 往一笔已有的双腿仓位上**补仓**：价差套利出现了比这笔仓位更好的价差时加码。两条腿按计划
    /// 再各成交一笔，成交后并进原来的腿（开仓均价按数量加权；名义、手续费、逐仓保证金累加）。
    ///
    /// 和 [`Self::open`] 的区别都在「已经有仓位」上：
    /// - 不跑开仓前那道「该合约已有持仓就拒绝」：补仓本来就是在已有持仓上加。杠杆与保证金模式一律
    ///   用这笔仓位记下的（券商下单前会读交易所的当前设置，一致时不改）；交易所报的保证金模式与台账
    ///   不符时不补 —— 那时下单会去改模式，动到原来那部分仓位。
    /// - 失败时只处理补的那一部分，原来的仓位一点不动：
    ///   - 第一腿没成交：什么都没变，返回 `Ok(false)`；
    ///   - 第一腿成交、第二腿没成交或明显不足：成交了的都并进腿，再把第一腿多补的数量
    ///     reduce-only 退掉（[`ClientOrderId::for_undo`]），两腿回到对等；
    ///   - 任一腿结果不明，或退不掉：写明原因、返回错误。仓位保持 `Open`，两腿数量可能已不对等 ——
    ///     对账会发现与交易所不一致并暂停规则，等人核对。
    ///
    /// 补仓序号从台账里这笔仓位已有的补仓单推出来；发单前订单意图已落盘，所以重试必换新号。
    /// 返回是否真的补上了（两腿都有成交）。
    pub async fn increase(
        &self,
        position: &mut PairPosition,
        plan: &Plan,
        entry_basis_pct: Decimal,
    ) -> ArbResult<bool> {
        if position.status != PositionStatus::Open || !position.is_hedged() {
            return Err(ArbError::config(format!(
                "仓位 {} 不是两腿齐全的持仓中仓位，不能补仓",
                position.id
            )));
        }
        let (Some(long), Some(short)) = (position.long.clone(), position.short.clone()) else {
            return Err(ArbError::config("双腿持仓缺腿"));
        };
        if plan.symbol != position.symbol
            || plan.long.venue != long.venue
            || plan.short.venue != short.venue
        {
            return Err(ArbError::config("补仓计划的合约或两腿场所与原仓位不一致"));
        }
        let (long_state, short_state) = self.leg_states(position).await;
        for (venue, state) in [(long.venue, long_state), (short.venue, short_state)] {
            if let Some(mode) = state.and_then(|state| state.margin_mode)
                && mode != position.margin_mode
            {
                return Err(ArbError::venue(
                    venue.as_str(),
                    "交易所的保证金模式与台账不符，不补仓：下单会去改模式，动到原来的仓位",
                ));
            }
        }
        let n = self.next_add(&position.id).await?;
        let first = plan.first_leg_capped();
        let (leverage, mode) = (position.leverage, position.margin_mode);
        let label = |leg: &LegPlan| format!("{} {}", leg.venue, side_label(leg.side));
        let old_notional = long.notional_usdt;

        let (first_fill, first_reason, pending) = match self
            .submit_first(
                ClientOrderId::for_add(&position.id, first.side, n),
                &first,
                leverage,
                mode,
                None,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                position.note = Some(format!(
                    "第 {n} 次补仓的第一腿（{}）结果不明：{error}。原来的仓位没动；先对账，不要重复补仓",
                    label(&first)
                ));
                let _ = self.persist(position).await;
                return Err(error);
            }
        };
        let Some(mut first_fill) = first_fill else {
            position.note = Some(format!(
                "第 {n} 次补仓没补成：第一腿（{}）未成交（{}），原来的仓位没动",
                label(&first),
                first_reason.unwrap_or_else(|| "未成交".into())
            ));
            self.persist(position).await?;
            return Ok(false);
        };
        let first_quantity = first_fill
            .quantity()
            .filter(|quantity| *quantity > Decimal::ZERO)
            .ok_or_else(|| ArbError::config("补仓第一腿成交价不可用，不能计算对冲数量"))?;
        let before_add = position.clone();
        merge_leg(position, &first_fill);
        self.persist(position).await?;

        // 第二腿的限价按第一腿的实际均价收紧到保本线以内（同开仓）；第一腿待核实的话同时核实。
        let hedged = plan.hedged_after(first_fill.average_price);
        let second = hedged.second_leg().clone();
        let submitted = self.submit_with_id(
            ClientOrderId::for_add(&position.id, second.side, n),
            &second,
            leverage,
            mode,
            Some(first_quantity),
        );
        let (submitted, settled) = match &pending {
            Some(pending) => {
                let (submitted, settled) = tokio::join!(submitted, self.settle(pending));
                (submitted, Some(settled))
            }
            None => (submitted.await, None),
        };
        if let Some(settled) = settled
            && let Some(fill) = settled?
        {
            // 待核实的那份已经并进腿里了：换成最终成交重新并一次。
            first_fill = fill;
            *position = before_add;
            merge_leg(position, &first_fill);
        }
        let first_quantity = first_fill
            .quantity()
            .filter(|quantity| *quantity > Decimal::ZERO)
            .unwrap_or(first_quantity);
        let (second_fill, second_reason) = match submitted {
            Ok(result) => result,
            Err(error) => {
                position.note = Some(format!(
                    "第 {n} 次补仓的第二腿（{}）结果不明：{error}。第一腿已补 {} 个并记入，两腿数量可能不对等；对账会暂停规则，先核对，不要重复补仓",
                    label(&second),
                    first_quantity.round_dp(8).normalize()
                ));
                let _ = self.persist(position).await;
                return Err(error);
            }
        };
        let second_quantity = second_fill
            .as_ref()
            .and_then(LegFill::quantity)
            .unwrap_or(Decimal::ZERO);
        if let Some(fill) = &second_fill {
            merge_leg(position, fill);
            // 入场基差按名义加权：原来那部分与这次补的各占一份。
            let added = position
                .long
                .as_ref()
                .map_or(Decimal::ZERO, |leg| leg.notional_usdt)
                - old_notional;
            let total = old_notional + added;
            if total > Decimal::ZERO && added > Decimal::ZERO {
                position.entry_basis_pct =
                    (position.entry_basis_pct * old_notional + entry_basis_pct * added) / total;
            }
        }
        let excess = first_quantity - second_quantity;
        if excess / first_quantity <= HEDGE_TOLERANCE {
            position.note = Some(format!(
                "第 {n} 次补仓：两腿各加约 {}（{} / {}）",
                second_quantity.round_dp(8).normalize(),
                label(&first),
                label(&second)
            ));
            self.persist(position).await?;
            info!(position = %position.id, n, quantity = %second_quantity, "补仓完成");
            return Ok(true);
        }

        // 第二腿没成交或明显不足：把第一腿多补的那部分退掉，回到对等。
        let why = second_reason.unwrap_or_else(|| "成交不足".into());
        self.persist(position).await?;
        // 零头不到各家最小下单额（约 10 USDT）：退单发了也是被拒（某次实盘 多补 0.04 个，约 3.9 USDT，
        // 退掉 0 个，把整笔补仓判成中断、自动交易跟着被关）。和开仓一样，不发退单，带着这点差额持有。
        let excess_usdt = excess * first_fill.average_price;
        if excess_usdt < UNDO_MIN_USDT {
            warn!(position = %position.id, n, %excess, excess_usdt = %excess_usdt.round_dp(2), reason = %why,
                "补仓两腿数量差低于最小下单额：不发退单，带着差额持有");
            return self
                .keep_add_residual(
                    position,
                    (&first, &second),
                    n,
                    (second_quantity, excess, Decimal::ZERO),
                    &why,
                    first_fill.average_price,
                )
                .await;
        }
        warn!(position = %position.id, n, %excess, reason = %why, "补仓第二腿不足，退掉第一腿多补的部分");
        match self.undo_excess(position, first.side, excess, n).await {
            Ok(undone)
                if (excess - undone) / first_quantity > HEDGE_TOLERANCE
                    && accept_residual(
                        (excess - undone).max(Decimal::ZERO),
                        first_quantity,
                        first_fill.average_price,
                    ) =>
            {
                self.keep_add_residual(
                    position,
                    (&first, &second),
                    n,
                    (second_quantity, excess, undone),
                    &why,
                    first_fill.average_price,
                )
                .await
            }
            Ok(undone) if (excess - undone) / first_quantity <= HEDGE_TOLERANCE => {
                position.note = Some(format!(
                    "第 {n} 次补仓第二腿（{}）只成交 {} 个（{why}）：第一腿多补的 {} 个已退掉，两腿回到对等",
                    label(&second),
                    second_quantity.round_dp(8).normalize(),
                    undone.round_dp(8).normalize()
                ));
                self.persist(position).await?;
                Ok(second_quantity > Decimal::ZERO)
            }
            Ok(undone) => {
                let error = ArbError::venue(
                    first.venue.as_str(),
                    format!(
                        "第 {n} 次补仓第二腿不足（{why}），第一腿多补 {} 个只退掉 {} 个，两腿数量不对等，请人工核对",
                        excess.round_dp(8).normalize(),
                        undone.round_dp(8).normalize()
                    ),
                );
                position.note = Some(error.to_string());
                self.persist(position).await?;
                Err(error)
            }
            Err(error) => {
                position.note = Some(format!(
                    "第 {n} 次补仓第二腿不足（{why}），退掉第一腿多补的 {} 个时出错：{error}。两腿数量可能不对等，请人工核对",
                    excess.round_dp(8).normalize()
                ));
                let _ = self.persist(position).await;
                Err(error)
            }
        }
    }

    /// 补仓第二腿不足、多补的零头退不掉（低于交易所最小下单额，或退了一部分剩下的在可持有范围内）：
    /// 按这点差额持有，备注写明，补仓算成功（只要第二腿有成交）。平仓时两腿各按自己的数量平。
    async fn keep_add_residual(
        &self,
        position: &mut PairPosition,
        (first, second): (&LegPlan, &LegPlan),
        n: u32,
        (second_quantity, excess, undone): (Decimal, Decimal, Decimal),
        why: &str,
        price: Decimal,
    ) -> ArbResult<bool> {
        let residual = (excess - undone).max(Decimal::ZERO);
        position.note = Some(format!(
            "第 {n} 次补仓第二腿（{} {}）只成交 {} 个（{why}）：第一腿（{} {}）多补的 {} 个{}，\
            按两腿数量差 {}（约 {} USDT）持有；平仓时两腿各按自己的数量平",
            second.venue,
            side_label(second.side),
            second_quantity.round_dp(8).normalize(),
            first.venue,
            side_label(first.side),
            excess.round_dp(8).normalize(),
            if undone > Decimal::ZERO {
                format!(
                    "退掉了 {} 个，剩下的退不掉（低于交易所最小下单额）",
                    undone.round_dp(8).normalize()
                )
            } else {
                "低于交易所最小下单额，没有退单".to_string()
            },
            residual.round_dp(8).normalize(),
            (residual * price).round_dp(2).normalize()
        ));
        self.persist(position).await?;
        Ok(second_quantity > Decimal::ZERO)
    }

    /// 下一次补仓的序号：台账里这笔仓位已有的补仓（含补仓回退）单的最大序号 + 1。
    async fn next_add(&self, position_id: &str) -> ArbResult<u32> {
        let (replayed, _) = self.ledger.replay().await?;
        Ok(add_rounds(position_id, &replayed.orders) + 1)
    }

    /// 开仓时第二腿有成交、但两腿数量对不上（偏差超过 [`SECOND_LEG_QTY_TOLERANCE`]）：**不两腿全回滚**，
    /// 把多出来的那条腿的零头 reduce-only 退掉（订单号 `undo0`，记账与补仓回退相同），按对上的数量持有。
    ///
    /// - 第二腿成交不到第一腿的 [`KEEP_HEDGED_MIN`]（一半）：对冲太小，返回 `Ok(Err(原因))`，调用方照旧回滚；
    /// - 退掉了（剩下的偏差在容差内）：返回 `Ok(Ok(Some(备注)))`；
    /// - 零头名义低于 [`UNDO_MIN_USDT`]（各家最小下单额）：不发退单，直接带着这点差额持有；
    /// - 退不掉或只退了一部分，剩下的偏差不超过 [`RESIDUAL_ACCEPT_PCT`] 且名义不超过 [`RESIDUAL_ACCEPT_USDT`]：
    ///   按这点小偏差持有，备注写明；
    ///   超过就返回 `Ok(Err(原因))` 回滚（一大块多出来的数量退不掉，说明场所出了别的问题）；
    /// - 退单本身结果不明（`Err`）：仓位按两腿已建记为持仓中、备注写明，错误往上抛 —— 对账会对出真实数量。
    async fn balance_legs(
        &self,
        position: &mut PairPosition,
        (first, first_quantity): (&LegPlan, Decimal),
        (second, second_quantity): (&LegPlan, Decimal),
        reason: Option<&str>,
    ) -> ArbResult<Result<Option<String>, String>> {
        let ratio = second_quantity / first_quantity;
        let why = reason.map_or_else(
            || "成交数量与第一腿对不上（两家下单数量步长不同）".to_string(),
            str::to_string,
        );
        if ratio < KEEP_HEDGED_MIN {
            return Ok(Err(format!(
                "第二腿只成交了第一腿数量的 {}%（{why}），对冲太小",
                (ratio * Decimal::ONE_HUNDRED).round_dp(1).normalize()
            )));
        }
        // 多出来的是哪条腿：正常是第一腿（第二腿的数量以第一腿为上限）。
        let (larger, excess) = if first_quantity >= second_quantity {
            (first, first_quantity - second_quantity)
        } else {
            (second, second_quantity - first_quantity)
        };
        let qty = |value: Decimal| value.round_dp(8).normalize().to_string();
        let label = format!("{} {}", larger.venue, side_label(larger.side));
        // 零头不到各家最小下单额（约 10 USDT）就不发退单：发了也是被拒，而拒单的回读一旦不正常（HL 的
        // 「reserved order no longer queryable」）就成了结果不明，反倒把开仓搞成中断。直接带着这点差额持有。
        let excess_usdt = excess * larger.limit_price;
        let skip_undo = excess_usdt < UNDO_MIN_USDT;
        if skip_undo {
            warn!(position = %position.id, venue = %larger.venue, excess = %excess,
                excess_usdt = %excess_usdt.round_dp(2), reason = %why,
                "两腿数量对不上，零头低于最小下单额：不发退单、不回滚，带着差额持有");
        } else {
            warn!(position = %position.id, venue = %larger.venue, excess = %excess, reason = %why,
                "两腿数量对不上，不回滚：退掉多出来的零头");
        }
        let undone = if skip_undo {
            Decimal::ZERO
        } else {
            match self.undo_excess(position, larger.side, excess, 0).await {
                Ok(undone) => undone,
                Err(error) => {
                    position.status = PositionStatus::Open;
                    position.note = Some(format!(
                        "两腿数量差 {}（{why}），退掉 {label} 多出来的零头时结果不明：{error}。两腿都已建仓，数量可能不对等，先对账",
                        qty(excess)
                    ));
                    let _ = self.persist(position).await;
                    return Err(error);
                }
            }
        };
        let residual = (excess - undone).max(Decimal::ZERO);
        let hedged = first_quantity.min(second_quantity);
        if residual <= first_quantity * SECOND_LEG_QTY_TOLERANCE {
            return Ok(Ok(Some(format!(
                "{BALANCED_HEDGE_NOTE}：第二腿（{} {}）成交 {}，第一腿 {}（{why}）；{label} 多出来的 {} 已 reduce-only 退掉，按 {} 对冲持有",
                second.venue,
                side_label(second.side),
                qty(second_quantity),
                qty(first_quantity),
                qty(undone),
                qty(hedged)
            ))));
        }
        let residual_pct = residual / first_quantity;
        let residual_usdt = residual * larger.limit_price;
        // 低于最小下单额的零头本来就退不掉，按金额看（几 USDT 的裸敞口），不再卡比例。
        if residual_usdt <= RESIDUAL_ACCEPT_USDT
            && (residual_pct <= RESIDUAL_ACCEPT_PCT || residual_usdt < UNDO_MIN_USDT)
        {
            return Ok(Ok(Some(format!(
                "{BALANCED_HEDGE_NOTE}：第二腿（{} {}）成交 {}，第一腿 {}（{why}）；{label} 多出来的 {} 退不掉（低于交易所最小下单额），\
                按两腿数量差 {}%（约 {} USDT）持有；平仓时两腿各按自己的数量平",
                second.venue,
                side_label(second.side),
                qty(second_quantity),
                qty(first_quantity),
                qty(residual),
                (residual_pct * Decimal::ONE_HUNDRED)
                    .round_dp(2)
                    .normalize(),
                residual_usdt.round_dp(2).normalize()
            ))));
        }
        Ok(Err(format!(
            "两腿数量差 {}（{why}），{label} 多出来的只退掉了 {}，剩下的 {}（约 {} USDT）超出可以带着持有的范围",
            qty(excess),
            qty(undone),
            qty(residual),
            residual_usdt.round_dp(2).normalize()
        )))
    }

    /// 补仓第二腿不足时，把 `leg_plan` 那条腿多补的 `quantity` reduce-only 退掉。记账与减仓相同：
    /// 这部分的价格盈亏按腿的（已并入补仓的）开仓均价当场入账，手续费留在腿上，平仓时一并结算。
    /// 返回实际退掉的数量。
    async fn undo_excess(
        &self,
        position: &mut PairPosition,
        leg_side: Side,
        quantity: Decimal,
        n: u32,
    ) -> ArbResult<Decimal> {
        let is_long = leg_side == Side::Buy;
        let reduce_side = match leg_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let leg = if is_long {
            position.long.clone()
        } else {
            position.short.clone()
        }
        .ok_or_else(|| ArbError::config("双腿持仓缺腿"))?;
        let leg_quantity = leg
            .quantity()
            .filter(|quantity| *quantity > Decimal::ZERO)
            .ok_or_else(|| ArbError::config(format!("{} 腿入场价非正", leg.venue)))?;
        // 零头 = 两腿数量相减，被减的那条腿按另一家的步长成交，差值不一定是这一家步长的整数倍；只减仓单
        // 不对齐步长，实盘券商本地就拒（`live_common::order_units`），退掉 0 —— 本该留下的部分对冲变成两腿
        // 全回滚，补仓时则留下裸着的多补部分。先向下取整到这一家的步长；取整后不到一步就不发单，剩下的尾数
        // 交给调用方按「零头残量」判断能不能带着持有。
        let quantity = match self
            .broker(leg.venue)?
            .quantity_step(&position.symbol)
            .await
        {
            Ok(Some(step)) if step > Decimal::ZERO => {
                crate::live_common::floor_to_step(quantity, step).unwrap_or(Decimal::ZERO)
            }
            Ok(_) => quantity,
            Err(error) => {
                warn!(position = %position.id, venue = %leg.venue, %error,
                    "读不到数量步长，退零头按原数量发");
                quantity
            }
        };
        if quantity <= Decimal::ZERO {
            warn!(position = %position.id, venue = %leg.venue,
                "零头不到这一家一个数量步长：不发退单，留给残量判断");
            return Ok(Decimal::ZERO);
        }
        let order = NewOrder {
            post_only: false,
            client_order_id: ClientOrderId::for_undo(&position.id, reduce_side, n),
            venue: leg.venue,
            symbol: position.symbol.clone(),
            side: reduce_side,
            notional_usdt: quantity * leg.average_price,
            quantity: Some(quantity),
            limit_price: None,
            reduce_only: true,
            leverage: None,
            margin_mode: position.margin_mode,
        };
        let state = self.execute_order(&order).await?;
        let Some(price) = state.average_price.filter(|price| *price > Decimal::ZERO) else {
            return Ok(Decimal::ZERO);
        };
        let filled = state.filled_usdt / price;
        if filled <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        let realized = if is_long {
            filled * (price - leg.average_price)
        } else {
            filled * (leg.average_price - price)
        };
        position.realized_pnl_usdt += realized;
        let kept = (Decimal::ONE - filled / leg_quantity).max(Decimal::ZERO);
        let updated = LegFill {
            notional_usdt: leg.notional_usdt * kept,
            fee_usdt: leg.fee_usdt + state.fee_usdt,
            margin_usdt: leg
                .margin_usdt
                .map(|margin| margin + realized - state.fee_usdt),
            ..leg
        };
        if is_long {
            position.long = Some(updated);
        } else {
            position.short = Some(updated);
        }
        self.persist(position).await?;
        Ok(filled)
    }

    /// 平掉一笔仓位。也用于重试停在 `Closing` / `Unwinding` 的仓位：已经退出的腿
    /// 不在仓位上了，只对剩下的敞口发单。
    /// 刚平掉的这笔仓位，最近一张平仓单在两条腿上的实际成交（从台账订单还原）。读台账失败 / 没有成交记录为 `None`。
    pub async fn last_exit_fills(
        &self,
        position: &PairPosition,
    ) -> Option<crate::breakdown::ExitFills> {
        let (replayed, _) = self.ledger.replay().await.ok()?;
        crate::breakdown::last_exit_fills(position, &replayed.orders)
    }

    pub async fn close(&self, position: &mut PairPosition) -> ArbResult<()> {
        // 一条腿都没记下的 `Opening`：开仓在第一腿成交前就中断了（进程在落盘意图之后、成交之前死掉）。
        // 它不是「平掉了一笔仓位」—— 按已平仓记会在当日盈亏里多出一笔盈亏为 0 的假交易。
        // 但订单有成交而仓位没记腿（死在「成交」与「写入仓位」之间）是台账之外的敞口，
        // 不能宣布没事，也不能自动平：留着并说明，由对账和人工处理。
        if position.status == PositionStatus::Opening
            && position.long.is_none()
            && position.short.is_none()
        {
            if self.has_fills(&position.id).await? {
                let error = ArbError::venue(
                    "executor",
                    "仓位没有记录任何一条腿，但它的订单有成交：请对照各场所账户人工核对、处理",
                );
                position.note = Some(error.to_string());
                self.persist(position).await?;
                return Err(error);
            }
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some("开仓在第一腿成交之前就中断了：没有任何成交，没有留下敞口".into());
            self.persist(position).await?;
            warn!(position = %position.id, "没有成交过的开仓：记为 Unwound");
            return Ok(());
        }
        // 开仓还没走完（`Opening`）、台账里还有结局没定的开仓单：现在平只会平掉台账里记着的那条腿，那张单要是其实
        // 成交了，另一家就留下一条台账外的裸腿，而且没人再去核实它（`resume_open` 只管 Opening 的仓位）。先等规则轮的
        // 续上建仓把它读回来（每轮自动做），之后再平。
        if position.status == PositionStatus::Opening
            && let Some(pending) = self.pending_entry(&position.id).await?
        {
            return Err(ArbError::venue(
                "executor",
                format!(
                    "开仓单 {pending} 的结局还没核实（可能其实成交了），现在平会留下台账外的腿：等续上建仓核实完（规则轮每轮自动做）再平"
                ),
            ));
        }
        // 有一家现在平不了（休市）：一单都不发，仓位原样留着，等能下单了再平。
        if let Some(reason) = self.exit_halted(position).await {
            return Err(ArbError::venue(
                "executor",
                format!("{reason}，现在不平（两条腿都没动），能下单后再平"),
            ));
        }
        // 先记下进来时的状态（`before_exit` 要看）：回滚没做完、这一轮接着平的仓位，收尾仍记 `Unwound`。
        let before = self.before_exit(position).await?;
        position.status = PositionStatus::Closing;
        // 两条腿都还在（对冲中）就同时平，缩短只剩一条腿的窗口；重试时只剩一条腿，同时与否一样。
        let (failures, exits) = self.exit_legs(position, true, true).await?;
        self.finish_close(position, &before, failures, &exits).await
    }

    /// 这笔仓位在台账里还没有结局的开仓单（第一张的订单号）；没有就是 `None`。
    async fn pending_entry(&self, position_id: &str) -> ArbResult<Option<ClientOrderId>> {
        let (replayed, _) = self.ledger.replay().await?;
        let mut pending: Vec<&ClientOrderId> = replayed
            .orders
            .values()
            .filter(|state| {
                state.status.is_live()
                    && matches!(
                        state.order.client_order_id.parse_for(position_id),
                        Some((_, OrderRole::Entry { .. }))
                    )
            })
            .map(|state| &state.order.client_order_id)
            .collect();
        pending.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pending.first().map(|id| (*id).clone()))
    }

    /// 平仓前要先记下的：入场两腿价格与名义（退出后就从仓位上消失了，备注里对比开平仓的成交价差），
    /// 以及资金费要查哪几家（按开仓时的两条腿，从台账历史找：重试时只剩一条腿，只查那条会漏掉另一条的）。
    async fn before_exit(&self, position: &PairPosition) -> ArbResult<BeforeExit> {
        Ok(BeforeExit {
            entry_prices: position
                .long
                .as_ref()
                .zip(position.short.as_ref())
                .map(|(long, short)| (long.average_price, short.average_price)),
            entry_notional: position.long.as_ref().map(|leg| leg.notional_usdt),
            funding_venues: position_venues(&self.ledger, position)
                .await
                .map_err(ArbError::from)?,
            unwinding: position.status == PositionStatus::Unwinding,
        })
    }

    /// 获利平仓的价格保护：先单独发 `guard` 那条腿的限价 IOC（reduce-only、整条腿的数量），看它的结局再动另一条。
    ///
    /// - 一点没成交（交易所撤了 / 拒了，没有对手盘）：两条腿都没动，仓位回到 `Open`，返回 [`GuardedExit::Held`]。
    ///   不是失败：获利平仓不急这一轮，下一轮再看。
    /// - 成交了（全部或一部分）：剩下的 —— 另一条腿、这条腿没成交的部分 —— 马上按市价平完，同 [`Self::close`]。
    /// - 结果不明（`Err`）：留在 `Closing`，原样返回错误；重试路径先读回这张单的结局再平（绝不回到 `Open`）。
    ///
    /// 为什么只保护一条腿：两腿各自设限价时，价格朝一个方向动，往往一条腿越过限价没成交、另一条照常成交，
    /// 最后还是按市价追没成交的那条（2026-10-15～24 变差超过 0.05% 的 5 条 HL-xyz 腿里，3 条另一腿同时变好了）。
    /// 先发容易滑的那条，它不成交就什么都没发生。代价：两条腿不再同时发，只剩一条腿的窗口多出第一条腿的往返。
    ///
    /// 仓位不是 `Open` 的完整双腿（重试中的 `Closing` 等）时，退回 [`Self::close`]。
    pub async fn close_guarded(
        &self,
        position: &mut PairPosition,
        guard: ExitGuard,
    ) -> ArbResult<GuardedExit> {
        if position.status != PositionStatus::Open || !position.is_hedged() {
            self.close(position).await?;
            return Ok(GuardedExit::Closed);
        }
        if let Some(reason) = self.exit_halted(position).await {
            return Err(ArbError::venue(
                "executor",
                format!("{reason}，现在不平（两条腿都没动），能下单后再平"),
            ));
        }
        // 之前的退出单都有终态才走这条路：有结果不明的旧单，交给 `close`（它先读回旧单再平）。
        if position.exits > 0 {
            let (replayed, _) = self.ledger.replay().await?;
            let prefix = format!("{}-", position.id);
            let pending = replayed.orders.values().any(|state| {
                state.status.is_live()
                    && state
                        .order
                        .client_order_id
                        .0
                        .strip_prefix(&prefix)
                        .is_some_and(|rest| {
                            rest.starts_with("buy-exit") || rest.starts_with("sell-exit")
                        })
            });
            if pending {
                self.close(position).await?;
                return Ok(GuardedExit::Closed);
            }
        }
        let Some((venue, side, notional, quantity)) = legs_of(position)?
            .into_iter()
            .find(|(_, side, _, _)| *side == guard.side)
        else {
            return Err(ArbError::config("要保护的那条腿不在仓位上"));
        };
        let before = self.before_exit(position).await?;
        position.status = PositionStatus::Closing;
        position.exits += 1;
        let n = position.exits;
        self.persist(position).await?;
        let order = NewOrder {
            post_only: false,
            client_order_id: ClientOrderId::for_exit(&position.id, side, n),
            venue,
            symbol: position.symbol.clone(),
            side,
            notional_usdt: notional,
            quantity: Some(quantity),
            limit_price: Some(guard.limit_price),
            reduce_only: true,
            leverage: None,
            margin_mode: position.margin_mode,
        };
        let started = Instant::now();
        let result = self.execute_order(&order).await;
        let first_ms = started.elapsed().as_millis() as u64;
        let state = match result {
            Ok(state) => state,
            Err(error) => {
                let error = ArbError::venue(
                    "executor",
                    format!(
                        "平仓未完成，保留 Closing 等待重试或对账：{venue} 限价平仓单结果不明（{error}），另一条腿还没动"
                    ),
                );
                position.note = Some(error.to_string());
                self.persist(position).await?;
                return Err(error);
            }
        };
        if state.status.is_live() {
            // IOC 不该还在场上（`execute_order` 会撤掉再读）；真碰上了不能再发一张：交给重试路径先读回它。
            let error = ArbError::venue(
                "executor",
                format!(
                    "平仓未完成，保留 Closing 等待重试或对账：{venue} 限价平仓单 {} 仍在场上，另一条腿还没动",
                    order.client_order_id
                ),
            );
            position.note = Some(error.to_string());
            self.persist(position).await?;
            return Err(error);
        }
        let mut exits = Vec::new();
        apply_exit_state(position, side, quantity, &state, &mut exits);
        if state.filled_usdt <= Decimal::ZERO {
            position.status = PositionStatus::Open;
            let why = format!(
                "{venue} 限价 {} 内没成交（{:?}{}），两条腿都没动，继续持有，下一轮再看",
                guard.limit_price.normalize(),
                state.status,
                state
                    .reject_reason
                    .as_deref()
                    .map(|why| format!("：{why}"))
                    .unwrap_or_default()
            );
            position.note = Some(why.clone());
            self.persist(position).await?;
            info!(position = %position.id, %venue, first_ms, "平仓限价保护：先发的那条腿没成交");
            return Ok(GuardedExit::Held(why));
        }
        self.persist(position).await?;
        // 成交了：剩下的马上按市价平完。这张单的终态刚读到，不用再回放台账读旧单。
        let (failures, more) = self.exit_legs_with(position, true, true, false).await?;
        // 只剩一条腿的窗口 ≈ 先发那条腿的确认（first_ms）+ 剩下的单（total_ms − first_ms）。
        info!(
            position = %position.id,
            %venue,
            status = ?state.status,
            filled_usdt = %state.filled_usdt.round_dp(4),
            first_ms,
            total_ms = started.elapsed().as_millis() as u64,
            "平仓限价保护：先发的那条腿成交了，剩下的已按市价发出"
        );
        exits.extend(more);
        self.finish_close(position, &before, failures, &exits)
            .await?;
        Ok(GuardedExit::Closed)
    }

    /// 退出单都发完之后：没平干净就留在 `Closing` 等重试或对账；平干净了记 `Closed`、资金费与备注。
    async fn finish_close(
        &self,
        position: &mut PairPosition,
        before: &BeforeExit,
        failures: Vec<String>,
        exits: &[(Side, Decimal)],
    ) -> ArbResult<()> {
        let BeforeExit {
            entry_prices,
            entry_notional,
            funding_venues,
            unwinding,
        } = before;
        if !failures.is_empty() {
            let error = ArbError::venue(
                "executor",
                format!(
                    "平仓未完成，保留 Closing 等待重试或对账：{}",
                    failures.join("；")
                ),
            );
            position.note = Some(error.to_string());
            self.persist(position).await?;
            return Err(error);
        }

        position.status = if *unwinding {
            PositionStatus::Unwound
        } else {
            PositionStatus::Closed
        };
        position.closed_at = Some(Utc::now());
        // 一条腿在交易所被强平 / 外部平掉、看板只平了剩下那条（[`crate::desk::adopt_lone_legs`]）：这里只记得到
        // 剩下那条的成交，整笔的实际盈亏留给事后按交易所成交记录核算（[`crate::desk::settle_closed_from_fills`]）。
        position.realized_source =
            (!position.closed_externally).then_some(RealizedSource::Executor);
        // 只算 [开仓, 平仓] 窗口。刚结算完的那一笔两家的流水可能一前一后出现，这里的数是初值；
        // 平仓后稍等一会儿由后台按同一窗口重新核对（[`crate::desk::recheck_funding`]）。
        position.realized_funding_usdt = self
            .funding_window(
                funding_venues,
                &position.symbol,
                position.opened_at,
                position.closed_at.unwrap_or_else(Utc::now),
            )
            .await;
        position.note = Some(if *unwinding {
            "建仓未完成，已成交腿回滚（上一轮回滚没做完，这一轮补平）".to_string()
        } else if position.closed_externally {
            // 只有剩下那条腿的成交：这里的数不含被强平那条腿的亏损，不能当整笔盈亏推出去。
            "剩下的腿已用只减仓单平掉；另一条腿此前在交易所已没有仓位（强平、ADL 或手动），整笔实际盈亏稍后按交易所成交记录核算"
                .to_string()
        } else {
            realized_note(position, *entry_prices, *entry_notional, exits)
        });
        self.persist(position).await?;
        Ok(())
    }

    /// 两腿按同一比例减仓，保持对冲。爆仓保护走这条路。
    ///
    /// 减仓单的名义按**当前**标记价折算（`数量 × 比例 × 标记价`）：拿入场名义去减，
    /// 价格涨了会少减、跌了会多减，两腿就不再等量。回写仓位时按**实际成交数量**缩小
    /// 入场名义；减掉部分的已实现盈亏计入这条腿的保证金、手续费从中扣除 ——
    /// 与 [`arb_scanner::leverage::trim_fraction`] 的假设一致，否则算出来的减仓比例
    /// 拉不回目标距离。
    ///
    /// 一条腿减成、另一条没减成时两腿数量就失衡了：写明原因并返回错误，
    /// 失衡由下一轮监控的数量规则或人工处理，不在这里静默重试。
    pub async fn trim(
        &self,
        position: &mut PairPosition,
        fraction: Decimal,
        long_mark: Decimal,
        short_mark: Decimal,
        reason: &str,
    ) -> ArbResult<()> {
        if fraction <= Decimal::ZERO || fraction >= Decimal::ONE {
            return Err(ArbError::config(format!(
                "减仓比例 {fraction} 必须在 (0, 1) 之间"
            )));
        }
        if let Some(reason) = self.exit_halted(position).await {
            return Err(ArbError::venue(
                "executor",
                format!("{reason}，现在不减仓（两条腿都没动）"),
            ));
        }
        if position.status != PositionStatus::Open || !position.is_hedged() {
            return Err(ArbError::config(format!(
                "仓位 {} 不是完整的双腿持仓，不能减仓",
                position.id
            )));
        }
        // 上一次减仓做到一半（某条腿的单结果不明）：先弄清那张单的结局并入账，弄不清就这一轮不减 ——
        // 换新号再发会把已经减过的那条腿再减一次。
        let blocked = self.settle_prior_trims(position).await?;
        if !blocked.is_empty() {
            return Err(ArbError::venue(
                "executor",
                format!("{}，这一轮不减仓", blocked.join("；")),
            ));
        }
        let (long_qty, short_qty) = match (position.long.as_ref(), position.short.as_ref()) {
            (Some(long), Some(short)) => (
                long.quantity()
                    .ok_or_else(|| ArbError::config(format!("{} 腿入场价非正", long.venue)))?,
                short
                    .quantity()
                    .ok_or_else(|| ArbError::config(format!("{} 腿入场价非正", short.venue)))?,
            ),
            _ => return Err(ArbError::config("双腿持仓缺腿")),
        };
        // 两腿数量差超过容差（上一次减仓只减了一条腿、或部分成交）：两腿都减到「较少那条 ×（1 − 比例）」，
        // 这一次就把两腿拉回一致，不在差额上再按比例各减一次。
        let smaller = long_qty.min(short_qty);
        let balanced =
            (long_qty - short_qty).abs() <= long_qty.max(short_qty) * SECOND_LEG_QTY_TOLERANCE;
        let plan: [(bool, Decimal, Decimal); 2] = if balanced {
            [
                (true, long_qty * fraction, long_mark),
                (false, short_qty * fraction, short_mark),
            ]
        } else {
            let target = smaller * (Decimal::ONE - fraction);
            [
                (true, long_qty - target, long_mark),
                (false, short_qty - target, short_mark),
            ]
        };
        if !balanced {
            warn!(position = %position.id, %long_qty, %short_qty,
                "两腿数量对不上（上一次减仓没做完）：这次两腿都减到同一数量");
        }
        // 只减仓单的数量必须正好是交易所步长的整数倍（实盘券商本地就拒，见 `live_common::order_units`），
        // 比例算出来的数有 28 位小数：先向下取整。两腿等比例减时尽量减同一个数（粗步长是细步长的整数倍时
        // 按粗的取整），两腿才保持对冲。
        let plan = self.trim_to_steps(position, plan, balanced).await?;
        // 先记下这次尝试再发单（与 `exit_legs` 同理）：减仓做到一半失败时，下一轮重试必须换新的
        // 订单号。不换号的话，券商要么拒绝重发（爆仓保护从此再也发不出单），要么查询返回上一次
        // 的旧成交，执行器会把它当成这一次的结果再记一遍账。
        position.trims += 1;
        let n = position.trims;
        self.persist(position).await?;
        let mut done: Vec<String> = Vec::new();
        for (is_long, reduce_quantity, mark) in plan {
            if reduce_quantity <= Decimal::ZERO {
                continue;
            }
            let leg = if is_long {
                position.long.as_ref()
            } else {
                position.short.as_ref()
            }
            .cloned()
            .ok_or_else(|| ArbError::config("双腿持仓缺腿"))?;
            let reduce_side = match leg.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let order = NewOrder {
                post_only: false,
                client_order_id: ClientOrderId::for_trim(&position.id, reduce_side, n),
                venue: leg.venue,
                symbol: position.symbol.clone(),
                side: reduce_side,
                notional_usdt: reduce_quantity * mark,
                quantity: Some(reduce_quantity),
                limit_price: None,
                reduce_only: true,
                leverage: None,
                margin_mode: position.margin_mode,
            };
            let done_text = if done.is_empty() {
                "两腿未变".to_string()
            } else {
                format!("{}已减、这条腿未定，两腿数量可能失衡", done.join("、"))
            };
            let state = match self.execute_order(&order).await {
                Ok(state) => state,
                Err(error) => {
                    // 结果不明：已经减了的那条腿已落盘；这张单的结局下一次减仓前先核实（见 settle_prior_trims）。
                    position.note = Some(format!(
                        "第 {n} 次减仓在 {} 结果不明（{error}），{done_text}；下次减仓前先核实这张单",
                        leg.venue
                    ));
                    self.persist(position).await?;
                    return Err(error);
                }
            };
            let Some(_) = apply_trim_fill(position, is_long, &state)? else {
                let why = state
                    .reject_reason
                    .clone()
                    .unwrap_or_else(|| format!("订单状态 {:?}", state.status));
                position.note = Some(format!(
                    "第 {n} 次减仓在 {} 失败（{why}），{done_text}",
                    leg.venue
                ));
                self.persist(position).await?;
                return Err(ArbError::venue(
                    leg.venue.as_str(),
                    format!("减仓失败：{why}"),
                ));
            };
            // 每条腿成交就落盘：另一条腿出错时，这条腿已经减掉的部分不会只留在内存里。
            done.push(format!("{} 腿", leg.venue));
            self.persist(position).await?;
            if state.status != OrderStatus::Filled {
                position.note = Some(format!(
                    "第 {n} 次减仓在 {} 仅部分成交，停止另一条腿；下次减仓会先把两腿拉回一致",
                    leg.venue
                ));
                self.persist(position).await?;
                return Err(ArbError::venue(
                    leg.venue.as_str(),
                    "减仓部分成交，两腿数量暂时不一致",
                ));
            }
        }
        // `trims` 已在发单前推进，这里不再改。
        position.note = Some(format!(
            "第 {n} 次减仓 {}%：{reason}",
            (fraction * Decimal::ONE_HUNDRED).round_dp(2)
        ));
        self.persist(position).await?;
        info!(position = %position.id, %fraction, reason, "两腿等比例减仓完成");
        Ok(())
    }

    /// 回滚之后在备注里补上第二腿的保本限价（[`breakeven_note`]）：没吃到它才回滚的，要看得出来。
    async fn explain_unwind(
        &self,
        position: &mut PairPosition,
        breakeven: Option<&str>,
    ) -> ArbResult<()> {
        let Some(breakeven) = breakeven else {
            return Ok(());
        };
        position.note = Some(match position.note.take() {
            Some(note) if !note.is_empty() => format!("{note}。{breakeven}"),
            _ => breakeven.to_string(),
        });
        self.persist(position).await
    }

    /// 第二腿没成交、没等第一腿核实就回滚了（见 `deferred_settle`）：核实回来以后把仓位上按待核实成交记的
    /// 入场价与手续费换成最终的。只在数量对得上时改（对不上说明成交本身变了，交给对账）。回滚已经做完
    /// （`Unwound`）就把差额补进已实现盈亏与手续费；回滚还没做完（`Unwinding`）就换掉腿，之后平仓按最终的算。
    async fn correct_unwound_entry(
        &self,
        position: &mut PairPosition,
        provisional: &LegFill,
        settled: &LegFill,
    ) {
        if settled.quantity() != provisional.quantity() {
            warn!(position = %position.id, provisional = ?provisional.quantity(), settled = ?settled.quantity(),
                "第一腿核实后的成交数量和待核实的不一样：不改仓位，交给对账");
            return;
        }
        let Some(quantity) = settled.quantity() else {
            return;
        };
        match position.status {
            PositionStatus::Unwound => {
                let price_diff = settled.average_price - provisional.average_price;
                position.realized_pnl_usdt += match settled.side {
                    // 多腿入场价低了（核实的比待核实的好），平仓的价格盈亏就多这么多。
                    Side::Buy => -price_diff * quantity,
                    Side::Sell => price_diff * quantity,
                };
                position.realized_fee_usdt += settled.fee_usdt - provisional.fee_usdt;
            }
            PositionStatus::Unwinding => set_leg(position, settled),
            _ => return,
        }
        if let Err(error) = self.persist(position).await {
            warn!(position = %position.id, %error, "第一腿核实后的修正没落成台账");
        }
    }

    /// 回滚已经成交的腿。    /// 回滚已经成交的腿。
    ///
    /// 回滚失败意味着仓位仍然裸着 —— 那必须留在 `Unwinding` 并写明原因，
    /// 不能只记一条日志就当处理完了：对账会拿这个状态去交易所核对。
    async fn unwind(&self, position: &mut PairPosition) -> ArbResult<()> {
        position.status = PositionStatus::Unwinding;
        // 已经是裸敞口：每条腿都要尽量平掉，一条失败不妨碍另一条。
        let (failures, _) = self.exit_legs(position, false, false).await?;
        if failures.is_empty() {
            // 回滚的逐笔成交（含手续费）已经当场入账：这笔的亏损是确定的。
            position.realized_source = Some(RealizedSource::Executor);
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some("建仓未完成，已成交腿已全部回滚".into());
            warn!(position = %position.id, "建仓失败并已回滚，无残留敞口");
        } else {
            position.note = Some(format!("回滚未完全成功，仍有敞口：{}", failures.join("；")));
            warn!(position = %position.id, note = ?position.note, "回滚失败，仓位仍裸");
        }
        self.persist(position).await?;
        Ok(())
    }

    /// 续上一笔中断的建仓：开仓执行到一半报了「结果不明」（第二腿、或第一腿的订单读不到结局），仓位停在
    /// `Opening`，第一腿可能已经裸着。规则轮每轮调一次（交易台的锁内，不会碰上正在执行的开仓）：
    ///
    /// 1. 把这笔仓位名下还没终态的**开仓单**读回结局、补进台账（同 [`Self::settle_prior_exits`]：读到终态照记、
    ///    券商明确没收到记 `Rejected`、仍读不到就这一轮什么都不做）。只读、不发单，所以对账不干净时也做 ——
    ///    对账不干净多半就是这些单造成的。
    /// 2. 按开仓单的实际成交重建两条腿。两腿数量对得上（偏差 ≤ [`SECOND_LEG_QTY_TOLERANCE`]）就转为持仓，
    ///    两腿都没成交记为 `Unwound`：这两种都不发单。
    /// 3. 还差第二腿（一点没成交、或只成交一部分）就**补第二腿**：剩下的数量按强制补腿的最差限价（锁定价差不低于
    ///    −开仓时的容差，没记的按 [`RESUME_HEDGE_TOLERANCE`]）追。**不回滚**（用户 10-17 定的）：没补齐就返回
    ///    [`Resume::Retry`]、第一腿留着，下一轮（约 1 分钟后）按同样的上限再补；还差的不到最小下单额就带着差额
    ///    持有（第二腿一点没有时补不了，交给人）。
    ///
    /// 发单前有两道关：这一次调用刚把腿补进台账（台账变了）就先返回 [`Resume::Verified`]，等补完腿之后的那次对账；
    /// `can_trade`（这个币、两条腿所在场所的对账都干净）为假返回 [`Resume::Frozen`]。人工已经平掉第一腿、读回出错
    /// 导致重复对冲这类情况，都会在那次对账里现形，不会补出一条反向的裸腿。
    pub async fn resume_open(
        &self,
        position: &mut PairPosition,
        can_trade: bool,
    ) -> ArbResult<Resume> {
        let (replayed, _) = self.ledger.replay().await?;
        let mut entries: Vec<(Side, u32, OrderState)> = replayed
            .orders
            .values()
            .filter_map(
                |state| match state.order.client_order_id.parse_for(&position.id) {
                    Some((side, OrderRole::Entry { attempt })) => {
                        Some((side, attempt, state.clone()))
                    }
                    _ => None,
                },
            )
            .collect();
        entries.sort_by(|a, b| {
            a.2.order
                .client_order_id
                .0
                .cmp(&b.2.order.client_order_id.0)
        });
        let mut changed = false;
        for (_, _, state) in entries
            .iter_mut()
            .filter(|(_, _, state)| state.status.is_live())
        {
            let (id, venue) = (state.order.client_order_id.clone(), state.order.venue);
            match self.broker(venue)?.order_state(&id).await {
                Ok(Some(read)) if read.order == state.order && !read.status.is_live() => {
                    self.persist_order(&read).await?;
                    info!(position = %position.id, order = %id, status = ?read.status,
                        filled = %read.filled_usdt, "建仓中断：读回了开仓单的结局，已补进台账");
                    *state = read;
                    changed = true;
                }
                Ok(Some(read)) if read.order == state.order => {
                    return Ok(Resume::Waiting(format!("{venue} 的开仓单 {id} 还在场上")));
                }
                Ok(Some(_)) => {
                    return Ok(Resume::Stuck(format!(
                        "{venue} 的开仓单 {id} 与台账里的意图对不上，请人工核对"
                    )));
                }
                Ok(None) => {
                    let mut rejected = state.clone();
                    rejected.status = OrderStatus::Rejected;
                    rejected.reject_reason = Some("券商没有收到这个订单号：按未提交处理".into());
                    self.persist_order(&rejected).await?;
                    *state = rejected;
                    changed = true;
                }
                // 读不到（交易所明确拒收、却被券商当成结果不明；或真的查不到）：看场所上这个币的净持仓，
                // 同退出单（`settle_prior_exits`）。和台账一致 = 这张开仓单没有动过持仓，按未成交了结，往下照常
                // 补腿 / 收尾。以前这里只会每轮等一次、不告警，另一条已成交的腿就一直裸着。
                // 台账里这一侧的腿正是这张单记下的（第一腿的待核实成交）时不能这么判：那份成交已经算在台账里了。
                Err(error) => {
                    let recorded = match state.order.side {
                        Side::Buy => position.long.as_ref(),
                        Side::Sell => position.short.as_ref(),
                    };
                    let counted =
                        recorded.is_some_and(|leg| leg.venue == venue && leg.client_order_id == id);
                    let unchanged = !counted
                        && matches!(
                            self.venue_net_matches_ledger(venue, &position.symbol).await,
                            Ok(true)
                        );
                    if !unchanged {
                        return Ok(Resume::Waiting(format!(
                            "{venue} 的开仓单 {id} 的结果还读不到，{} 的净持仓也不能证明它没成交：{error}",
                            position.symbol
                        )));
                    }
                    let mut rejected = state.clone();
                    rejected.status = OrderStatus::Rejected;
                    rejected.reject_reason = Some(format!(
                        "订单读不到（{error}），但 {venue} 上 {} 的净持仓与台账一致：这张开仓单没有动过持仓，按未成交处理",
                        position.symbol
                    ));
                    self.persist_order(&rejected).await?;
                    warn!(position = %position.id, order = %id, %venue, %error,
                        "建仓中断：开仓单读不到，但场所净持仓没变，按未成交了结");
                    *state = rejected;
                    changed = true;
                }
            }
        }
        // 按开仓单的实际成交重建两条腿（同一侧的几次提交合成一条）。
        let (mut buys, mut sells): (Vec<LegFill>, Vec<LegFill>) = (Vec::new(), Vec::new());
        let mut by_attempt: Vec<&(Side, u32, OrderState)> = entries.iter().collect();
        by_attempt.sort_by_key(|(_, attempt, _)| *attempt);
        for (side, _, state) in by_attempt {
            if state.filled_usdt <= Decimal::ZERO {
                continue;
            }
            match leg_fill(&state.order, state).filter(|fill| fill.quantity().is_some()) {
                Some(fill) => match side {
                    Side::Buy => buys.push(fill),
                    Side::Sell => sells.push(fill),
                },
                None => {
                    return Ok(Resume::Stuck(format!(
                        "开仓单 {} 有成交但缺合法均价，请人工核对",
                        state.order.client_order_id
                    )));
                }
            }
        }
        let quantity_of =
            |fills: &[LegFill]| -> Decimal { fills.iter().filter_map(LegFill::quantity).sum() };
        for side in [Side::Buy, Side::Sell] {
            let rebuilt = merge_fills(match side {
                Side::Buy => &buys,
                Side::Sell => &sells,
            });
            let recorded = match side {
                Side::Buy => position.long.as_ref(),
                Side::Sell => position.short.as_ref(),
            };
            match (recorded, rebuilt) {
                (None, None) => {}
                // 已经记下的腿和开仓单对得上就原样留着（手续费、核实过的均价都在上面）。
                (Some(leg), Some(fill))
                    if leg.venue == fill.venue
                        && leg
                            .quantity()
                            .zip(fill.quantity())
                            .is_some_and(|(a, b)| crate::reconcile::quantities_match(a, b)) => {}
                (Some(leg), Some(fill)) if leg.venue == fill.venue => {
                    set_leg(position, &fill);
                    changed = true;
                }
                (None, Some(fill)) => {
                    set_leg(position, &fill);
                    changed = true;
                }
                (Some(leg), _) => {
                    return Ok(Resume::Stuck(format!(
                        "台账记着 {} {} 腿，但这一侧的开仓单对不上它，请人工核对",
                        leg.venue,
                        side_label(side)
                    )));
                }
            }
        }
        let (long_q, short_q) = (quantity_of(&buys), quantity_of(&sells));
        if long_q.is_zero() && short_q.is_zero() {
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some(format!(
                "{RESUMED_NOTE}：核实开仓单都没有成交，没有留下敞口"
            ));
            self.persist(position).await?;
            warn!(position = %position.id, "建仓中断后核实：两腿都没有成交，记为 Unwound");
            return Ok(Resume::Done);
        }
        let filled_side = match (position.long.is_some(), position.short.is_some()) {
            (true, false) => Some(Side::Buy),
            (false, true) => Some(Side::Sell),
            _ => None,
        };
        // 计划里的第一腿一点没成交、只有另一边成交了（两腿同时下，第一腿核实没成交）：把成交的那边当第一腿，
        // 去原来第一腿的场所补。同样是「补腿不回滚」，以前这里直接交人工，成交的那条腿一直裸着。
        let first = match (position.planned.map(|planned| planned.first), filled_side) {
            (Some(planned), Some(filled)) if planned != filled => {
                warn!(position = %position.id, planned = side_label(planned),
                    filled = side_label(filled),
                    "建仓中断后核实：计划的第一腿没成交、另一边成交了，改为补另一边");
                Some(filled)
            }
            (Some(planned), _) => Some(planned),
            (None, filled) => filled,
        };
        let base = first.map_or(long_q.max(short_q), |side| match side {
            Side::Buy => long_q,
            Side::Sell => short_q,
        });
        if long_q > Decimal::ZERO
            && short_q > Decimal::ZERO
            && (long_q - short_q).abs() <= base * SECOND_LEG_QTY_TOLERANCE
        {
            position.status = PositionStatus::Open;
            position.note = Some(format!(
                "{RESUMED_NOTE}：核实两腿都已成交（多 {} / 空 {}），两腿对冲，转为持仓",
                long_q.round_dp(8).normalize(),
                short_q.round_dp(8).normalize()
            ));
            self.persist(position).await?;
            info!(position = %position.id, symbol = %position.symbol, %long_q, %short_q,
                "建仓中断后核实两腿都已成交，转为持仓");
            return Ok(Resume::Done);
        }
        // 往下要发单：补第二腿、退零头或回滚。
        let Some(first) = first else {
            return Ok(Resume::Stuck(
                "认不出哪条是第一腿（台账里没有开仓计划），请人工核对".into(),
            ));
        };
        let second = opposite(first);
        let first_leg = match first {
            Side::Buy => position.long.clone(),
            Side::Sell => position.short.clone(),
        };
        let Some(first_leg) = first_leg else {
            return Ok(Resume::Stuck(
                "只有第二腿有成交、第一腿没有，请人工核对".into(),
            ));
        };
        if changed {
            self.persist(position).await?;
            return Ok(Resume::Verified);
        }
        if !can_trade {
            return Ok(Resume::Frozen);
        }
        let second_venue = entries
            .iter()
            .find(|(side, _, _)| *side == second)
            .map(|(_, _, state)| state.order.venue)
            .or(position.planned.map(|planned| planned.venue(second)));
        let Some(second_venue) = second_venue else {
            return Ok(Resume::Stuck(
                "不知道第二腿该在哪家补（台账里没有开仓计划，也没有第二腿的订单），请人工处理"
                    .into(),
            ));
        };
        let tolerance = position
            .planned
            .and_then(|planned| planned.force_hedge)
            .unwrap_or(RESUME_HEDGE_TOLERANCE);
        let Some(limit) = crate::preflight::forced_limit(first, first_leg.average_price, tolerance)
        else {
            return Ok(Resume::Stuck(format!(
                "补第二腿的最差限价算不出来（第一腿均价 {}、容差 {tolerance}），请人工处理",
                first_leg.average_price
            )));
        };
        let (first_q, second_q) = match first {
            Side::Buy => (long_q, short_q),
            Side::Sell => (short_q, long_q),
        };
        let first_plan = LegPlan {
            symbol: position.symbol.clone(),
            venue: first_leg.venue,
            side: first,
            notional_usdt: first_leg.notional_usdt,
            limit_price: first_leg.average_price,
            best_price: first_leg.average_price,
            expected_price: first_leg.average_price,
            slippage: Decimal::ZERO,
            book_notional: Decimal::ZERO,
            fee: Decimal::ZERO,
            maker_rest_ms: None,
        };
        let second_plan = LegPlan {
            venue: second_venue,
            side: second,
            notional_usdt: (first_q - second_q).max(Decimal::ZERO) * limit,
            limit_price: limit,
            best_price: limit,
            expected_price: limit,
            ..first_plan.clone()
        };
        let label = format!("{second_venue} {}", side_label(second));
        let qty = |value: Decimal| value.round_dp(8).normalize().to_string();
        let mut second_fills = match second {
            Side::Buy => buys,
            Side::Sell => sells,
        };
        let shortfall = first_q - second_q;
        // 还差的不到各家最小下单额（约 10 USDT）就补不了：第二腿一点没有时只能交给人（不回滚），
        // 有了的话带着这点差额持有（下面）。
        let below_minimum = shortfall * limit < UNDO_MIN_USDT;
        if shortfall > first_q * SECOND_LEG_QTY_TOLERANCE && below_minimum && second_q.is_zero() {
            return Ok(Resume::Stuck(format!(
                "第一腿只成交了 {}（约 {} USDT），低于最小下单额，补不了第二腿；不回滚，请人工处理",
                qty(first_q),
                (first_q * limit).round_dp(2).normalize()
            )));
        }
        let mut chased = false;
        if shortfall > first_q * SECOND_LEG_QTY_TOLERANCE && !below_minimum {
            self.prepare_leg(&second_plan, position.leverage, position.margin_mode, true)
                .await?;
            let next = entries
                .iter()
                .filter(|(side, _, _)| *side == second)
                .map(|(_, attempt, _)| attempt + 1)
                .max()
                .unwrap_or(0);
            warn!(position = %position.id, symbol = %position.symbol, venue = %second_venue,
                remaining = %shortfall.round_dp(8), forced_limit = %limit, %tolerance,
                "建仓中断后补第二腿：按强制补腿的最差限价追剩下的数量");
            let earlier = std::mem::take(&mut second_fills);
            let ((merged, _), _) = match self
                .chase_remaining(
                    &position.id,
                    &second_plan,
                    limit,
                    next,
                    (position.leverage, position.margin_mode),
                    first_q,
                    earlier,
                    Some("建仓中断后补第二腿".into()),
                )
                .await
            {
                Ok(result) => result,
                Err(error) => {
                    position.note = Some(format!(
                        "建仓中断后补第二腿（{label}）结果不明：{error}。下一轮先读回它的结局，不重复补"
                    ));
                    let _ = self.persist(position).await;
                    return Err(error);
                }
            };
            if let Some(fill) = &merged {
                set_leg(position, fill);
                self.persist(position).await?;
            }
            second_fills = merged.into_iter().collect();
            chased = true;
        }
        let second_q = quantity_of(&second_fills);
        let shortfall = first_q - second_q;
        let what = if chased {
            format!(
                "第二腿（{label}）按最差限价 {}（锁定价差不低于 −{}%）补到 {}，第一腿 {}",
                limit.round_dp(8).normalize(),
                (tolerance * Decimal::ONE_HUNDRED).round_dp(4).normalize(),
                qty(second_q),
                qty(first_q)
            )
        } else {
            format!(
                "第二腿（{label}）已成交 {}，第一腿 {}",
                qty(second_q),
                qty(first_q)
            )
        };
        let note = if shortfall.abs() <= first_q * SECOND_LEG_QTY_TOLERANCE {
            format!("{RESUMED_NOTE}：{what}，两腿对冲，转为持仓")
        } else if shortfall > Decimal::ZERO && second_q > Decimal::ZERO && below_minimum {
            format!(
                "{RESUMED_NOTE}：{what}；还差的 {}（约 {} USDT）低于最小下单额补不了，带着差额持有；平仓时两腿各按自己的数量平",
                qty(shortfall),
                (shortfall * limit).round_dp(2).normalize()
            )
        } else if shortfall > Decimal::ZERO {
            // 最差限价内没补齐：不回滚（用户 10-17 定的），第一腿留着，下一轮（约 1 分钟后）按同样的上限再补。
            let why = format!(
                "{what}，还差 {}：最差限价 {} 以内没有足够的对手盘。不回滚，下一轮再补",
                qty(shortfall),
                limit.round_dp(8).normalize()
            );
            position.note = Some(format!("{RESUMED_NOTE}（未完）：{why}"));
            self.persist(position).await?;
            warn!(position = %position.id, symbol = %position.symbol, %why, "建仓中断后补第二腿还没补齐，下一轮再补");
            return Ok(Resume::Retry(why));
        } else {
            // 第二腿比第一腿多（不该出现）：退掉第二腿多出来的零头；退不掉交给人，不回滚。
            match self
                .balance_legs(
                    position,
                    (&first_plan, first_q),
                    (&second_plan, second_q),
                    None,
                )
                .await?
            {
                Ok(extra) => format!("{RESUMED_NOTE}：{what}。{}", extra.unwrap_or_default()),
                Err(why) => {
                    return Ok(Resume::Stuck(format!("{what}：{why}；不回滚，请人工处理")));
                }
            }
        };
        position.status = PositionStatus::Open;
        position.note = Some(note);
        self.persist(position).await?;
        warn!(position = %position.id, symbol = %position.symbol, note = ?position.note,
            "建仓中断后已补上第二腿，转为持仓");
        Ok(Resume::Done)
    }

    /// 新一次减仓之前，弄清这个仓位名下「台账里还没有终态」的减仓单的结局并入账（同 [`Self::settle_prior_exits`]）：
    /// - 读到终态 → 补进台账，按成交减掉那条腿；
    /// - 券商明确没收到 → 记 `Rejected`；
    /// - 读不到 → 看场所上这个币的净持仓：与台账一致 = 没动过持仓，记 `Rejected`；
    /// - 仍在场上 / 对不上 / 净持仓不一致 → 返回原因，这一轮不减仓（换号再发可能把同一条腿减两次）。
    ///
    /// 减仓在交易台的锁里串行，这些单不是「正在发」的。
    async fn settle_prior_trims(&self, position: &mut PairPosition) -> ArbResult<Vec<String>> {
        let (replayed, _) = self.ledger.replay().await?;
        let mut pending: Vec<(u32, OrderState)> = replayed
            .orders
            .values()
            .filter(|state| state.status.is_live())
            .filter_map(
                |state| match state.order.client_order_id.parse_for(&position.id) {
                    Some((_, OrderRole::Trim { n })) => Some((n, state.clone())),
                    _ => None,
                },
            )
            .collect();
        pending.sort_by_key(|(n, _)| *n);
        let mut blocked = Vec::new();
        for (_, local) in pending {
            if let AddSettle::Waiting(why) = self.settle_reduce_order(position, &local).await? {
                blocked.push(why);
            }
        }
        Ok(blocked)
    }

    /// 减仓单（`-trim{n}`）、补仓退单（`-undo{n}`）这类 reduce-only 单在台账里一直没终态时，弄清结局并入账。
    /// 规则轮的后台核实（`desk::settle_stale_adds`）和下一次减仓之前（[`Self::settle_prior_trims`]）都走这里。
    ///
    /// 以前只有重启时的启动核实管它们：中途读不到的单停在 `Pending`，对账报「本地记着挂单、交易所没有」并冻结这个币，
    /// 规则跟着停 —— 下一次减仓也就永远不会来。
    /// - 读到终态 → 补进台账，按成交减掉那条腿（同 [`apply_trim_fill`]）；
    /// - 券商明确没收到 → 记 `Rejected`；
    /// - 读不到 → 看场所上这个币的净持仓：与台账一致 = 没动过持仓，记 `Rejected`；
    /// - 仍在场上 / 对不上 / 净持仓不一致 → [`AddSettle::Waiting`]，什么都不改。
    ///
    /// 了结之后两腿数量差超过容差、又不在可持有的零头范围内（例如减仓只减了一条腿、退单没退成），返回
    /// [`AddSettle::Unbalanced`]（推告警）；不发任何单。调用方必须持有 `live.lock`。
    pub async fn settle_reduce_order(
        &self,
        position: &mut PairPosition,
        local: &OrderState,
    ) -> ArbResult<AddSettle> {
        let (id, venue) = (local.order.client_order_id.clone(), local.order.venue);
        let (is_long, what) = match id.parse_for(&position.id) {
            Some((side, role @ OrderRole::Trim { .. })) => (role.is_long_leg(side), "减仓单"),
            Some((side, role @ OrderRole::Undo { .. })) => (role.is_long_leg(side), "补仓退单"),
            _ => {
                return Err(ArbError::config(format!(
                    "{id} 不是仓位 {} 的减仓单或补仓退单",
                    position.id
                )));
            }
        };
        let reason = match self.broker(venue)?.order_state(&id).await {
            Ok(Some(state)) if state.order == local.order && !state.status.is_live() => {
                self.persist_order(&state).await?;
                let filled = apply_trim_fill(position, is_long, &state)?;
                self.persist(position).await?;
                info!(position = %position.id, order = %id, status = ?state.status,
                    filled = %state.filled_usdt, "读回了{what}的结局，已入账");
                let text = format!(
                    "{what} {id} 读回是 {:?}{}",
                    state.status,
                    filled.map_or("、没有成交".to_string(), |q| format!(
                        "，成交 {} 个已从腿上减掉",
                        q.round_dp(8).normalize()
                    ))
                );
                return self.after_reduce(position, text).await;
            }
            Ok(Some(state)) if state.order == local.order => {
                return Ok(AddSettle::Waiting(format!(
                    "{venue} 上的{what} {id} 仍在场上"
                )));
            }
            Ok(Some(_)) => {
                return Ok(AddSettle::Waiting(format!(
                    "{venue} 上的{what} {id} 与台账里的意图对不上"
                )));
            }
            Ok(None) => "券商没有收到这个订单号：按未提交处理".to_string(),
            Err(error) => match self.venue_net_matches_ledger(venue, &position.symbol).await {
                Ok(true) => format!(
                    "订单读不到（{error}），但 {venue} 上 {} 的净持仓与台账一致：这张单没有动过持仓，按未成交处理",
                    position.symbol
                ),
                Ok(false) => {
                    return Ok(AddSettle::Waiting(format!(
                        "{venue} 上的{what} {id} 的结果读不到，且 {} 的净持仓与台账不一致（可能已成交）：{error}",
                        position.symbol
                    )));
                }
                Err(check) => {
                    return Ok(AddSettle::Waiting(format!(
                        "{venue} 上的{what} {id} 的结果读不到，核对净持仓也失败（{check}）：{error}"
                    )));
                }
            },
        };
        let mut rejected = local.clone();
        rejected.status = OrderStatus::Rejected;
        rejected.reject_reason = Some(reason.clone());
        self.persist_order(&rejected).await?;
        warn!(position = %position.id, order = %id, %venue, %reason, "{what}按未成交了结");
        self.after_reduce(position, format!("{what} {id} 按未成交了结：{reason}"))
            .await
    }

    /// reduce-only 单了结之后：两腿数量差超过容差、又不是可以带着持有的零头时，说明白并要求人工核对。
    async fn after_reduce(
        &self,
        position: &mut PairPosition,
        text: String,
    ) -> ArbResult<AddSettle> {
        let unbalanced = match (Self::leg_gap(position), position.long.as_ref()) {
            (Some((gap, larger)), Some(long)) => {
                gap.abs() > larger * HEDGE_TOLERANCE
                    && !accept_residual(gap.abs(), larger, long.average_price)
            }
            _ => false,
        };
        let text = match Self::leg_gap(position) {
            Some((gap, _)) if unbalanced => format!(
                "{text}；两腿数量相差 {} 个，不对等，请人工核对（下一次减仓会先把两腿拉齐）",
                gap.abs().round_dp(8).normalize()
            ),
            _ => text,
        };
        position.note = Some(text.clone());
        self.persist(position).await?;
        Ok(if unbalanced {
            AddSettle::Unbalanced(text)
        } else {
            AddSettle::Applied(text)
        })
    }

    /// 重试退出之前，先弄清这个仓位名下「台账里还没有终态」的退出单的结局。
    ///
    /// 2026-10-15 东京：lighter-rh 买回单已成交，立刻回读时成交明细还没出齐，`execute_order` 报「状态不明」；
    /// 订单在台账里停在 `Pending`、仓位那条腿没摘 —— 之后每次重试换新号再发，都被 reduce-only 拒绝（仓位早就是 0），
    /// 对账连续不干净、所有规则暂停，直到重启时的启动核实补上终态（某合约、某合约 各一次）。这里把同样的核实放进
    /// 每次重试的入口：
    /// - 读到终态 → 补进台账，并按成交记进仓位（腿平完了就摘掉），**不再重发**；
    /// - 券商明确没收到这个订单号 → 记 `Rejected`，照常重发；
    /// - 还读不到 / 还在场上 / 与台账里的意图对不上 → 这条腿本轮**不重发**（重发可能重复平仓），原因返回给调用方。
    ///
    /// 只碰这个仓位自己的退出单。重试时上一次 `exit_legs` 早已返回（同一仓位的退出是串行的），
    /// 这些订单不是「正在发」的，不会和在途下单抢状态 —— 这是它和「全局定时核实」的区别。
    async fn settle_prior_exits(
        &self,
        position: &mut PairPosition,
        exits: &mut Vec<(Side, Decimal)>,
    ) -> ArbResult<Vec<(Side, String)>> {
        let (replayed, _) = self.ledger.replay().await?;
        let prefix = format!("{}-", position.id);
        let mut pending: Vec<OrderState> = replayed
            .orders
            .values()
            .filter(|state| {
                state.status.is_live()
                    && state
                        .order
                        .client_order_id
                        .0
                        .strip_prefix(&prefix)
                        .is_some_and(|rest| {
                            rest.starts_with("buy-exit") || rest.starts_with("sell-exit")
                        })
            })
            .cloned()
            .collect();
        // 按退出编号从小到大：先发的先记账。
        pending.sort_by_key(|state| exit_number(&state.order.client_order_id.0));
        let mut blocked = Vec::new();
        for local in pending {
            let (id, side, venue) = (
                local.order.client_order_id.clone(),
                local.order.side,
                local.order.venue,
            );
            let broker = self.broker(venue)?;
            match broker.order_state(&id).await {
                Ok(Some(state)) if state.order == local.order => {
                    if state.status.is_live() {
                        blocked.push((
                            side,
                            format!("{venue} 上一张退出单 {id} 仍在场上，本轮不重发"),
                        ));
                        continue;
                    }
                    let Some(quantity) = state.order.quantity.filter(|q| *q > Decimal::ZERO) else {
                        blocked.push((
                            side,
                            format!("{venue} 退出单 {id} 没有数量，无法按成交记账，本轮不重发"),
                        ));
                        continue;
                    };
                    self.persist_order(&state).await?;
                    apply_exit_state(position, side, quantity, &state, exits);
                    self.persist(position).await?;
                    info!(position = %position.id, order = %id, status = ?state.status,
                        "重试前读回了上一张退出单的结局，已入账，不重发");
                }
                Ok(Some(_)) => blocked.push((
                    side,
                    format!("{venue} 上一张退出单 {id} 与台账里的意图对不上，本轮不重发"),
                )),
                Ok(None) => {
                    let mut rejected = local.clone();
                    rejected.status = OrderStatus::Rejected;
                    rejected.reject_reason = Some("券商没有收到这个订单号：按未提交处理".into());
                    self.persist_order(&rejected).await?;
                }
                // 读不到（Lighter：订单历史里没有、台账里有意图，读回永远 fail closed）：看场所上这个币的净持仓。
                // 和台账（这条腿还没摘）一致 = 这张退出单没有动过持仓，当成没成交、记 `Rejected`，照常重发 ——
                // reduce-only，重发不会反向开仓。对不上（可能已成交 / 部分成交）就仍然不重发，交对账与人工。
                // 某次实盘：Lighter 多腿的 `sell-exit1` 读不到，bitget 空腿已平，Lighter 多腿裸着，
                // 重试每分钟都被「本轮不重发」挡住。
                Err(error) => match self.venue_net_matches_ledger(venue, &position.symbol).await {
                    Ok(true) => {
                        let mut rejected = local.clone();
                        rejected.status = OrderStatus::Rejected;
                        rejected.reject_reason = Some(format!(
                            "订单读不到（{error}），但 {venue} 上 {} 的净持仓与台账一致：这张退出单没有动过持仓，按未成交处理，重发",
                            position.symbol
                        ));
                        self.persist_order(&rejected).await?;
                        warn!(position = %position.id, order = %id, %venue, %error,
                            "退出单读不到，但场所净持仓没变：按未成交了结，重发退出");
                    }
                    Ok(false) => blocked.push((
                        side,
                        format!(
                            "{venue} 上一张退出单 {id} 的结果仍读不到，且 {} 的净持仓与台账不一致（可能已成交），本轮不重发：{error}",
                            position.symbol
                        ),
                    )),
                    Err(check) => blocked.push((
                        side,
                        format!(
                            "{venue} 上一张退出单 {id} 的结果仍读不到，核对净持仓也失败（{check}），本轮不重发：{error}"
                        ),
                    )),
                },
            }
        }
        Ok(blocked)
    }

    /// 补仓的某一张单在台账里一直没有终态（`Pending` / `Open`）时，弄清它的结局并落账。
    ///
    /// 某次实盘：Lighter 补仓第一腿 `sell-add1` 下单后读回连续报「查不到、不许重发」（约 3 秒），
    /// 补仓被判「结果未知」中断，订单在台账里停在 `Pending`；它其实没成交（两家持仓都与台账一致），但没有任何
    /// 运行时机制去核实补仓单（自愈只管退出单，启动核实只在重启时跑），对账一直报 `OrderMissingOnVenue`，
    /// 这个币的规则、开仓一直被冻结，直到重启。这里补上：
    /// - 读到终态 → 补进台账。**没成交**的就此了结；**有成交**的只记订单，不合并进腿（合并要连带第二腿一起算）：
    ///   台账与交易所的持仓从此对不上，对账会如实冻结这个币、告警，交人工；
    /// - 券商明确没收到这个订单号 → 记 `Rejected`；
    /// - 仍读不到（Lighter：交易所的订单历史里没有，台账里有意图，读回永远 fail closed）→ 看**场所上这个币的净持仓**：
    ///   和台账（不含这张单）一致，说明这张单没有改变过持仓，按未成交记 `Rejected`；对不上就什么都不动，
    ///   持仓不符本身就是对账要报的事；
    /// - 仍在场上、与意图对不上 → 不动。
    ///
    /// 调用方必须持有 `live.lock`：补仓整个过程（两条腿 + 读回）都在这把锁里，所以持锁时台账里还没终态的补仓单
    /// 一定是上一次调用遗留的，不会和在途下单抢状态；并且要先让它在台账里「放过一阵」再来（见 `desk::settle_stale_adds`）。
    pub async fn settle_add_order(
        &self,
        position: &mut PairPosition,
        local: &OrderState,
    ) -> ArbResult<AddSettle> {
        let (id, venue) = (&local.order.client_order_id, local.order.venue);
        // 了结后可能要发退单（退第一腿多补的部分，见 [`Self::rebalance_add`]、[`Self::undo_lone_first_leg`]）：
        // 场所休市时退单下不去，而这张单一旦落了终态就不会再来核实 —— 先不了结，等开市。
        if let Some(reason) = self.exit_halted(position).await {
            return Ok(AddSettle::Waiting(format!(
                "{reason}：补仓单 {id} 了结后可能要发退单，等能下单了再核实"
            )));
        }
        let broker = self.broker(venue)?;
        let reason = match broker.order_state(id).await {
            Ok(Some(state)) if state.order == local.order => {
                if state.status.is_live() {
                    return Ok(AddSettle::Waiting(format!(
                        "{venue} 上的补仓单 {id} 仍在场上"
                    )));
                }
                self.persist_order(&state).await?;
                if state.filled_usdt > Decimal::ZERO {
                    // 同一次补仓的第一腿已并进腿、这张（第二腿）的成交正好是两腿的差额：它就是没并进去的第二腿，
                    // 并进去两腿就对上了（不足的部分再退第一腿）。对不上的（例如第一腿自己待核实）照旧交人工。
                    if let Some(fill) = self.unmerged_second_leg(position, &state).await? {
                        merge_leg(position, &fill);
                        self.persist(position).await?;
                        let balance = self.rebalance_add(position, &state).await;
                        let text = format!(
                            "补仓单 {id} 读回已成交 {} USDT，是没并进腿的第二腿：已并进腿{}",
                            state.filled_usdt.round_dp(4).normalize(),
                            balance
                                .as_ref()
                                .map(|(b, _)| format!("；{b}"))
                                .unwrap_or_default()
                        );
                        position.note = Some(text.clone());
                        self.persist(position).await?;
                        if balance.is_some_and(|(_, attention)| attention) {
                            return Ok(AddSettle::Unbalanced(text));
                        }
                        return Ok(AddSettle::Hedged(text));
                    }
                    // 第一腿结果不明、第二腿从没发出（台账里没有同一次补仓的另一张单）：这笔成交是多出来的敞口，
                    // 记进腿再 reduce-only 退掉，回到补仓前（同 increase 里第二腿没成交时的处理）。
                    if let Some(outcome) = self.undo_lone_first_leg(position, &state).await? {
                        return Ok(outcome);
                    }
                    position.note = Some(format!(
                        "补仓单 {id} 读回时已有成交 {} USDT，但补仓中断、没有并进腿：台账与交易所持仓对不上，请人工核对",
                        state.filled_usdt.round_dp(4).normalize()
                    ));
                    self.persist(position).await?;
                    return Ok(AddSettle::Filled(state.filled_usdt));
                }
                let balance = self.rebalance_add(position, &state).await;
                let text = format!(
                    "补仓单 {id} 读回是 {:?}、没有成交{}",
                    state.status,
                    balance
                        .as_ref()
                        .map_or("：原来的仓位没动".to_string(), |(b, _)| format!(
                            "；{b}"
                        ))
                );
                position.note = Some(text.clone());
                self.persist(position).await?;
                if balance.is_some_and(|(_, attention)| attention) {
                    return Ok(AddSettle::Unbalanced(text));
                }
                return Ok(AddSettle::NoFill(format!("{:?}", state.status)));
            }
            Ok(Some(_)) => {
                return Ok(AddSettle::Waiting(format!(
                    "{venue} 上的补仓单 {id} 与台账里的意图对不上"
                )));
            }
            Ok(None) => "券商没有收到这个订单号：按未提交处理".to_string(),
            Err(error) => match self.venue_net_matches_ledger(venue, &position.symbol).await {
                Ok(true) => format!(
                    "订单读不到（{error}），但 {venue} 上 {} 的净持仓与台账一致：这张单没有改变过持仓，按未成交处理",
                    position.symbol
                ),
                Ok(false) => {
                    return Ok(AddSettle::Waiting(format!(
                        "{venue} 上的补仓单 {id} 读不到（{error}），且 {} 的净持仓与台账不一致：不能当成没成交",
                        position.symbol
                    )));
                }
                Err(check) => {
                    return Ok(AddSettle::Waiting(format!(
                        "{venue} 上的补仓单 {id} 读不到（{error}），核对持仓也失败（{check}）"
                    )));
                }
            },
        };
        let mut rejected = local.clone();
        rejected.status = OrderStatus::Rejected;
        rejected.reject_reason = Some(reason.clone());
        self.persist_order(&rejected).await?;
        let balance = self.rebalance_add(position, &rejected).await;
        let text = format!(
            "补仓单 {id} 按未成交了结：{reason}；{}",
            balance
                .as_ref()
                .map_or_else(|| "原来的仓位没动".to_string(), |(b, _)| b.clone())
        );
        position.note = Some(text.clone());
        self.persist(position).await?;
        if balance.is_some_and(|(_, attention)| attention) {
            return Ok(AddSettle::Unbalanced(text));
        }
        Ok(AddSettle::NoFill(reason))
    }

    /// 同一次补仓的另一张单（`{仓位}-{另一边}-add{n}`）：台账里已是终态、有成交时返回它的成交数量。
    async fn add_sibling_quantity(
        &self,
        position: &PairPosition,
        settled: &OrderState,
    ) -> ArbResult<Option<(Side, Decimal, Decimal)>> {
        let Some((side, OrderRole::Add { n })) =
            settled.order.client_order_id.parse_for(&position.id)
        else {
            return Ok(None);
        };
        let other = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let (replayed, _) = self.ledger.replay().await?;
        let Some(sibling) = replayed
            .orders
            .get(&ClientOrderId::for_add(&position.id, other, n).0)
            .filter(|state| !state.status.is_live())
        else {
            return Ok(None);
        };
        let Some(price) = sibling.average_price.filter(|p| *p > Decimal::ZERO) else {
            return Ok(None);
        };
        let quantity = sibling.filled_usdt / price;
        Ok((quantity > Decimal::ZERO).then_some((other, quantity, price)))
    }

    /// 两腿数量差（多 − 空）。
    fn leg_gap(position: &PairPosition) -> Option<(Decimal, Decimal)> {
        let long = position.long.as_ref()?.quantity()?;
        let short = position.short.as_ref()?.quantity()?;
        Some((long - short, long.max(short)))
    }

    /// 这张读回已成交的补仓单是不是「第一腿已并进、它自己没并进」的第二腿：同一次补仓的另一张已成交，
    /// 多出来的正是另一张那条腿，差额和这张的成交数量对得上。是就返回要并进的成交。
    async fn unmerged_second_leg(
        &self,
        position: &PairPosition,
        state: &OrderState,
    ) -> ArbResult<Option<LegFill>> {
        let Some((sibling_side, sibling_quantity, _)) =
            self.add_sibling_quantity(position, state).await?
        else {
            return Ok(None);
        };
        let Some(fill) = crate::broker::leg_fill(&state.order, state) else {
            return Ok(None);
        };
        let (Some(quantity), Some((gap, larger))) = (fill.quantity(), Self::leg_gap(position))
        else {
            return Ok(None);
        };
        // 另一张是买 = 多腿多出来（gap > 0）。差额正好是另一张整笔的成交 = 这张（第二腿）一点都没并进去；
        // 这张的成交不超过差额（部分成交时并进去后剩下的差额再退第一腿）。
        let tolerance = larger * HEDGE_TOLERANCE;
        let sibling_larger = (sibling_side == Side::Buy) == (gap > Decimal::ZERO);
        let unmerged = (gap.abs() - sibling_quantity).abs() <= tolerance;
        let fits = quantity <= gap.abs() + tolerance;
        Ok((sibling_larger && unmerged && fits).then_some(fill))
    }

    /// 补仓单了结后：同一次补仓的另一条腿（第一腿）已成交并进了腿，而这张（第二腿）没成交或成交不足，
    /// 两腿差额超过容差时，把第一腿多补的部分 reduce-only 退掉（同 [`Self::increase`] 里第二腿不足时的处理）。
    /// 退的数量不超过第一腿这次补的数量，也不超过两腿实际差额；不到最小下单额就带着差额持有。
    /// 返回给备注的说明；没有要做的返回 `None`。退单出错写进说明，交对账与人工。
    async fn rebalance_add(
        &self,
        position: &mut PairPosition,
        settled: &OrderState,
    ) -> Option<(String, bool)> {
        let (sibling_side, sibling_quantity, price) =
            match self.add_sibling_quantity(position, settled).await {
                Ok(Some(found)) => found,
                Ok(None) => return None,
                Err(error) => {
                    return Some((
                        format!("查同一次补仓的另一腿失败（{error}），两腿可能不对等"),
                        true,
                    ));
                }
            };
        let n = match settled.order.client_order_id.parse_for(&position.id) {
            Some((_, OrderRole::Add { n })) => n,
            _ => return None,
        };
        self.undo_add_extra(position, sibling_side, sibling_quantity, n, price)
            .await
    }

    /// 把 `leg_side` 那条腿多出来的部分（不超过 `cap`，也不超过两腿实际差额）reduce-only 退掉，订单号
    /// `for_undo(n)`。两腿差额在容差内、或多出来的不是这条腿时什么都不做（返回 `None`）；不到最小下单额就
    /// 带着差额持有。返回给备注的说明和「要不要人工看」。
    async fn undo_add_extra(
        &self,
        position: &mut PairPosition,
        leg_side: Side,
        cap: Decimal,
        n: u32,
        price: Decimal,
    ) -> Option<(String, bool)> {
        let (gap, larger) = Self::leg_gap(position)?;
        if gap.abs() <= larger * HEDGE_TOLERANCE {
            return None;
        }
        // 多出来的必须是这条腿；不然不是这次补仓留下的差额，不动。
        if (leg_side == Side::Buy) != (gap > Decimal::ZERO) {
            return None;
        }
        let excess = gap.abs().min(cap);
        let qty = |q: Decimal| q.round_dp(8).normalize();
        if excess * price < UNDO_MIN_USDT {
            return Some((
                format!(
                    "第一腿多补的 {} 个不到最小下单额，带着差额持有",
                    qty(excess)
                ),
                false,
            ));
        }
        warn!(position = %position.id, n, %excess, "补仓另一腿没补上：退掉第一腿多补的部分");
        match self.undo_excess(position, leg_side, excess, n).await {
            Ok(undone) if (excess - undone) <= larger * HEDGE_TOLERANCE => Some((
                format!("第一腿多补的 {} 个已退掉，两腿回到对等", qty(undone)),
                false,
            )),
            Ok(undone) => Some((
                format!(
                    "第一腿多补 {} 个只退掉 {} 个，两腿数量不对等，请人工核对",
                    qty(excess),
                    qty(undone)
                ),
                true,
            )),
            Err(error) => Some((
                format!(
                    "退掉第一腿多补的 {} 个时出错（{error}），两腿数量可能不对等，请人工核对",
                    qty(excess)
                ),
                true,
            )),
        }
    }

    /// 补仓第一腿结果不明中断（第二腿从没发出，台账里没有同一次补仓的另一张单），事后读回**有成交**：
    /// - 两腿现在对等 = 这笔成交还没记进腿：记进去；
    /// - 这条腿正好多出这笔成交的数量 = 已经记过了（例如待核实的估算先并了进去）：不再记；
    /// - 都不是：不是这种情况，返回 `None`，交人工（[`AddSettle::Filled`]）。
    ///
    /// 然后把这笔 reduce-only 退掉，回到补仓前。返回了结结果。
    async fn undo_lone_first_leg(
        &self,
        position: &mut PairPosition,
        state: &OrderState,
    ) -> ArbResult<Option<AddSettle>> {
        let Some((side, OrderRole::Add { n })) =
            state.order.client_order_id.parse_for(&position.id)
        else {
            return Ok(None);
        };
        let other = match side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let (replayed, _) = self.ledger.replay().await?;
        if replayed
            .orders
            .contains_key(&ClientOrderId::for_add(&position.id, other, n).0)
        {
            return Ok(None);
        }
        let Some(fill) = crate::broker::leg_fill(&state.order, state) else {
            return Ok(None);
        };
        let (Some(quantity), Some((gap, larger))) = (fill.quantity(), Self::leg_gap(position))
        else {
            return Ok(None);
        };
        let tolerance = larger * HEDGE_TOLERANCE;
        let merged_already = (side == Side::Buy) == (gap > Decimal::ZERO)
            && (gap.abs() - quantity).abs() <= tolerance;
        if gap.abs() <= tolerance {
            merge_leg(position, &fill);
            self.persist(position).await?;
        } else if !merged_already {
            return Ok(None);
        }
        let balance = self
            .undo_add_extra(position, side, quantity, n, fill.average_price)
            .await;
        let id = &state.order.client_order_id;
        let text = format!(
            "补仓单 {id} 读回已成交 {} USDT，但同一次补仓的另一腿从没发出：已记进腿{}",
            state.filled_usdt.round_dp(4).normalize(),
            balance
                .as_ref()
                .map(|(b, _)| format!("；{b}"))
                .unwrap_or_default()
        );
        position.note = Some(text.clone());
        self.persist(position).await?;
        Ok(Some(if balance.is_some_and(|(_, attention)| attention) {
            AddSettle::Unbalanced(text)
        } else {
            AddSettle::Applied(text)
        }))
    }

    /// 场所上这个币的净持仓是否与台账一致（台账按所有还有敞口的仓位在这个场所上的腿合计，同对账口径）。
    async fn venue_net_matches_ledger(
        &self,
        venue: Venue,
        symbol: &arb_core::Symbol,
    ) -> ArbResult<bool> {
        let (replayed, _) = self.ledger.replay().await?;
        let mut expected = Decimal::ZERO;
        for position in replayed.exposed() {
            if position.symbol != *symbol {
                continue;
            }
            for (leg, direction) in [
                (position.long.as_ref(), Decimal::ONE),
                (position.short.as_ref(), -Decimal::ONE),
            ] {
                if let Some(leg) = leg.filter(|leg| leg.venue == venue) {
                    let quantity = leg
                        .quantity()
                        .filter(|quantity| *quantity > Decimal::ZERO)
                        .ok_or_else(|| ArbError::config("台账缺成交数量，无法核对真实持仓"))?;
                    expected += direction * quantity;
                }
            }
        }
        let on_venue: Decimal = self
            .broker(venue)?
            .positions()
            .await?
            .into_iter()
            .filter(|position| position.symbol == *symbol)
            .map(|position| position.net_quantity)
            .sum();
        Ok(crate::reconcile::quantities_match(expected, on_venue))
    }

    /// 对仍在仓位上的腿逐条发 reduce-only 退出单，返回没能完全退出的原因。
    ///
    /// 每次调用用新的退出编号（`exits`），先落盘再发单。完全成交的腿从仓位上移除；
    /// 部分成交按实际成交数量缩小，下一次重试只平剩下的数量 —— 否则 reduce-only
    /// 数量超过真实持仓，交易所会拒单，敞口就永远平不掉。
    ///
    /// 同时把每笔成交的价格盈亏记进 `realized_pnl_usdt`、完全平掉的腿的手续费记进
    /// `realized_fee_usdt`（腿上累计的开仓、减仓、部分退出手续费加上这一笔的，只计一次）。
    /// 返回没能完全退出的原因，以及这一次各条腿的退出成交均价（按平仓方向：卖 = 多腿）。
    async fn exit_legs(
        &self,
        position: &mut PairPosition,
        stop_on_failure: bool,
        concurrent: bool,
    ) -> ArbResult<(Vec<String>, Vec<(Side, Decimal)>)> {
        let settle_prior = position.exits > 0;
        self.exit_legs_with(position, stop_on_failure, concurrent, settle_prior)
            .await
    }

    /// 同 [`Self::exit_legs`]；`settle_prior` = 发单前先读回上一次的退出单（见 `settle_prior_exits`）。
    /// 只有调用方刚刚自己读到了上一张退出单的终态时才能给 `false`（[`Self::close_guarded`] 的第二步：
    /// 那时只剩一条腿裸着，省掉一次台账回放）。
    async fn exit_legs_with(
        &self,
        position: &mut PairPosition,
        stop_on_failure: bool,
        concurrent: bool,
        settle_prior: bool,
    ) -> ArbResult<(Vec<String>, Vec<(Side, Decimal)>)> {
        let mut failures = Vec::new();
        let mut exits = Vec::new();
        // 重试：上一次发出去、结果没读到的退出单，先弄清结局再决定要不要重发（见 `settle_prior_exits`）。
        // 第一次退出（`exits == 0`）没有旧单，不付这一次台账回放的代价。
        let mut blocked: Vec<Side> = Vec::new();
        if settle_prior {
            for (side, why) in self.settle_prior_exits(position, &mut exits).await? {
                blocked.push(side);
                failures.push(why);
            }
        }
        position.exits += 1;
        let n = position.exits;
        self.persist(position).await?;

        let orders: Vec<(NewOrder, Decimal)> = legs_of(position)?
            .into_iter()
            .filter(|(_, side, _, _)| !blocked.contains(side))
            .map(|(venue, side, notional, quantity)| {
                let order = NewOrder {
                    post_only: false,
                    client_order_id: ClientOrderId::for_exit(&position.id, side, n),
                    venue,
                    symbol: position.symbol.clone(),
                    side,
                    notional_usdt: notional,
                    quantity: Some(quantity),
                    limit_price: None,
                    reduce_only: true,
                    leverage: None,
                    margin_mode: position.margin_mode,
                };
                (order, quantity)
            })
            .collect();
        // 平对冲仓位：两条腿**同时**发。一条一条发时第二条腿要等第一条回读完（Lighter / Arcus 3–8 秒），
        // 这段时间只剩一条腿、价格一动就是纯亏 —— 实盘规则平仓里，后平那条腿相对决定时
        // 盘口的滑点明显比先平那条大，有一笔单腿几秒内就亏掉了不少（xyz 那几秒跌了 0.25%）。
        // 代价：一条腿平不掉时另一条已经平了，敞口留给 Closing 的重试去平（退出单失败率约 1/75）。
        let results: Vec<ArbResult<OrderState>> = if concurrent {
            futures_util::future::join_all(
                orders.iter().map(|(order, _)| self.execute_order(order)),
            )
            .await
        } else {
            let mut results = Vec::with_capacity(orders.len());
            for (order, _) in &orders {
                let result = self.execute_order(order).await;
                let failed = !matches!(&result, Ok(state) if state.status == OrderStatus::Filled);
                results.push(result);
                if failed && stop_on_failure {
                    break;
                }
            }
            results
        };
        for ((order, quantity), result) in orders.iter().zip(results) {
            let (venue, side, quantity) = (order.venue, order.side, *quantity);
            let state = match result {
                Ok(state) => state,
                Err(error) => {
                    failures.push(format!("{venue} 退出状态不明：{error}"));
                    continue;
                }
            };
            let done = apply_exit_state(position, side, quantity, &state, &mut exits);
            self.persist(position).await?;
            if !done {
                failures.push(format!(
                    "{venue} 退出未足额成交（{:?}{}）",
                    state.status,
                    state
                        .reject_reason
                        .map(|why| format!("：{why}"))
                        .unwrap_or_default()
                ));
            }
        }
        Ok((failures, exits))
    }

    /// 一条腿下单前的准备（见 [`Broker::prepare_open`]）。`positions_checked` 见
    /// [`PositionSetup::positions_checked`]。
    async fn prepare_leg(
        &self,
        leg: &LegPlan,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
        positions_checked: bool,
    ) -> ArbResult<()> {
        let broker = self.broker(leg.venue)?;
        if !positions_checked {
            ensure_flat(broker.as_ref(), &leg.symbol).await?;
        }
        broker
            .prepare_open_mode(&leg.symbol, leg.side, leverage, mode)
            .await
    }

    /// 强制补腿：保本价内第二腿没吃满，把**还没对冲的剩余数量**按最差限价（锁定价差不低于 `−tolerance`）追上。
    ///
    /// `earlier` 是保本价那一档的结果（一点没吃到，或只吃到一部分）。返回合并后的第二腿结果，形状同
    /// [`Self::submit`]：几笔成交合成一条腿（均价按数量加权、名义与手续费相加）；追齐了（数量偏差不超过
    /// [`SECOND_LEG_QTY_TOLERANCE`]）原因为 `None`，追不齐原因里说明，调用方照旧回滚。
    ///
    /// 追单只在交易所明确说了「没成交」或「部分成交」的前提下才会发（调用方保证）；追单自己的结果不明
    /// （`Err`）立刻停下、原样往上抛 —— 前面已经成交的部分记在台账里，由对账处理，不在这里回滚也不再追。
    /// 追单有自己的预算（[`FORCE_HEDGE_ATTEMPTS`] 次、[`FORCE_HEDGE_WINDOW`]），订单号接着保本价那一档的序号编。
    #[allow(clippy::too_many_arguments)]
    async fn chase_second_leg(
        &self,
        position_id: &str,
        plan: &Plan,
        first_fill: &LegFill,
        second: &LegPlan,
        tolerance: Decimal,
        first_attempt: u32,
        (leverage, margin_mode): (Option<Decimal>, crate::MarginMode),
        first_quantity: Decimal,
        earlier: ArbResult<(Option<LegFill>, Option<String>)>,
    ) -> ArbResult<((Option<LegFill>, Option<String>), bool)> {
        let (earlier_fill, earlier_reason) = earlier?;
        // 最差限价：不比保本价那一档更紧（追的就是更差的价），其余由 `tolerance` 一个数定。
        let mut chase = plan
            .forced_after(first_fill.average_price, tolerance)
            .second_leg()
            .clone();
        chase.limit_price = match chase.side {
            Side::Buy => chase.limit_price.max(second.limit_price),
            Side::Sell => chase.limit_price.min(second.limit_price),
        };
        self.chase_remaining(
            position_id,
            &chase,
            second.limit_price,
            first_attempt,
            (leverage, margin_mode),
            first_quantity,
            earlier_fill.into_iter().collect(),
            earlier_reason,
        )
        .await
    }

    /// [`Self::chase_second_leg`] 的追单循环：按 `chase` 的限价把第二腿追到 `first_quantity`（`fills` 是这条腿
    /// 已经成交的部分，`reason` 是它没吃满的原因），预算 [`FORCE_HEDGE_ATTEMPTS`] 次、[`FORCE_HEDGE_WINDOW`]，
    /// 订单号从 `first_attempt` 接着编。`breakeven_limit` 只用于日志。续上中断的建仓
    /// （[`Self::resume_open`]）也用它：那时没有原来的计划，追单的限价由调用方按第一腿均价算好。
    #[allow(clippy::too_many_arguments)]
    async fn chase_remaining(
        &self,
        position_id: &str,
        chase: &LegPlan,
        breakeven_limit: Decimal,
        first_attempt: u32,
        (leverage, margin_mode): (Option<Decimal>, crate::MarginMode),
        first_quantity: Decimal,
        mut fills: Vec<LegFill>,
        mut reason: Option<String>,
    ) -> ArbResult<((Option<LegFill>, Option<String>), bool)> {
        let mut sent = false;
        let started = Instant::now();
        let quantity_of =
            |fills: &[LegFill]| -> Decimal { fills.iter().filter_map(LegFill::quantity).sum() };
        for round in 0..FORCE_HEDGE_ATTEMPTS {
            let remaining = first_quantity - quantity_of(&fills);
            if remaining <= first_quantity * SECOND_LEG_QTY_TOLERANCE {
                reason = None;
                break;
            }
            if let Some(why) = reason.as_deref().filter(|why| pointless_retry(why)) {
                warn!(position = %position_id, venue = %chase.venue, reason = why, "第二腿是被确定性原因拒的（价格带、保证金、仓位不允许、离标记价太远……）：追价也一样被拒，不追，直接回滚");
                break;
            }
            if round > 0 {
                if started.elapsed() + FORCE_HEDGE_GAP >= FORCE_HEDGE_WINDOW {
                    break;
                }
                tokio::time::sleep(FORCE_HEDGE_GAP).await;
            }
            warn!(
                position = %position_id,
                venue = %chase.venue,
                remaining = %remaining.round_dp(8),
                breakeven_limit = %breakeven_limit,
                forced_limit = %chase.limit_price,
                round,
                "第二腿在保本价内没吃满，强制补腿：按最差限价追剩余数量"
            );
            // 追单的名义额按**剩余数量**重算：不能还是整笔的名义，万一哪家券商在数量和名义额同时给出时
            // 以名义额为准，部分成交后的追单就会把整笔再买一次，多出一份对冲。
            let leg = LegPlan {
                notional_usdt: remaining * chase.limit_price,
                ..chase.clone()
            };
            sent = true;
            let (fill, why) = self
                .submit(
                    position_id,
                    &leg,
                    first_attempt + round,
                    leverage,
                    margin_mode,
                    Some(remaining),
                )
                .await?;
            fills.extend(fill);
            reason = why;
            if reason.is_none() {
                break;
            }
        }
        let remaining = first_quantity - quantity_of(&fills);
        if remaining <= first_quantity * SECOND_LEG_QTY_TOLERANCE {
            reason = None;
        } else if reason.is_none() {
            reason = Some("强制补腿后仍不足额".to_string());
        }
        Ok(((merge_fills(&fills), reason), sent))
    }

    /// 提交一条腿并把订单状态写进台账。
    ///
    /// 重试时 `attempt` 递增，但**同一个 attempt 复用同一个 `client_order_id`** ——
    /// 幂等键必须可复现，否则交易所侧无法去重。
    async fn submit(
        &self,
        position_id: &str,
        leg: &LegPlan,
        attempt: u32,
        leverage: Option<Decimal>,
        margin_mode: crate::MarginMode,
        quantity: Option<Decimal>,
    ) -> ArbResult<(Option<LegFill>, Option<String>)> {
        self.submit_with_id(
            ClientOrderId::for_leg(position_id, leg.side, attempt),
            leg,
            leverage,
            margin_mode,
            quantity,
        )
        .await
    }

    /// 同 [`Self::submit`]，订单号由调用方给（补仓用 [`ClientOrderId::for_add`]）。
    async fn submit_with_id(
        &self,
        client_order_id: ClientOrderId,
        leg: &LegPlan,
        leverage: Option<Decimal>,
        margin_mode: crate::MarginMode,
        quantity: Option<Decimal>,
    ) -> ArbResult<(Option<LegFill>, Option<String>)> {
        let order = leg_order(client_order_id, leg, leverage, margin_mode, quantity);
        let state = self.execute_order(&order).await?;
        leg_outcome(&order, state)
    }

    /// 两腿同时下（第二腿不等第一腿的结果）时两腿共同的数量；`None` = 照旧顺序下。
    ///
    /// 顺序下时第二腿要等第一腿确认成交才发：Lighter 第一腿约 0.35 秒、Arcus 约 0.1 秒，这段时间里第二腿的盘口
    /// 可能被别人吃掉。同时下省掉这段，代价是第一腿没成交（或成交太少）时第二腿已经成交、要回滚（用户 10-26 定：
    /// 只在第一腿不是 Hyperliquid 时这样做 —— 实盘里吃单第一腿没成交的几次都是它，它走链也最慢）。
    ///
    /// 只用在两腿都吃单的价差单上。两腿必须下**同一个数量**（顺序下时第二腿直接用第一腿的成交量）：两家都报得出
    /// 数量步长（[`Broker::quantity_step`]）、粗的步长是细的整数倍时，按第一腿名义 ÷ 限价向下取整到粗的步长；
    /// 报不出、凑不齐就顺序下。
    /// 爆仓保护减仓的两腿数量（见 [`Self::trim`]）向下取整到各家的数量步长。等比例减（`balanced`）且两家步长
    /// 一粗一细、粗的是细的整数倍时，两腿都按粗的取整到同一个数（取两腿原数量较小的那个）。券商报不出步长的
    /// （纸面）原样不动。任何一条腿取整后为 0 就报错、一单不发：只减一条腿会把对冲拆开。
    async fn trim_to_steps(
        &self,
        position: &PairPosition,
        plan: [(bool, Decimal, Decimal); 2],
        balanced: bool,
    ) -> ArbResult<[(bool, Decimal, Decimal); 2]> {
        let venue_of = |is_long: bool| {
            if is_long {
                position.long.as_ref().map(|leg| leg.venue)
            } else {
                position.short.as_ref().map(|leg| leg.venue)
            }
            .ok_or_else(|| ArbError::config("双腿持仓缺腿"))
        };
        let mut steps = [None, None];
        for (slot, (is_long, _, _)) in plan.iter().enumerate() {
            let venue = venue_of(*is_long)?;
            steps[slot] = self
                .broker(venue)?
                .quantity_step(&position.symbol)
                .await?
                .filter(|step| *step > Decimal::ZERO);
        }
        let common = match (balanced, steps[0], steps[1]) {
            (true, Some(a), Some(b)) => {
                let (coarse, fine) = if a >= b { (a, b) } else { (b, a) };
                (coarse % fine).is_zero().then_some(coarse)
            }
            _ => None,
        };
        let mut out = plan;
        let same = plan[0].1.min(plan[1].1);
        for (slot, item) in out.iter_mut().enumerate() {
            let (raw, step) = match common {
                Some(coarse) => (same, Some(coarse)),
                None => (item.1, steps[slot]),
            };
            let Some(step) = step else {
                item.1 = raw;
                continue;
            };
            let rounded = crate::live_common::floor_to_step(raw, step)
                .ok_or_else(|| ArbError::config("数量步长非正"))?;
            if rounded <= Decimal::ZERO && item.1 > Decimal::ZERO {
                return Err(ArbError::venue(
                    "executor",
                    format!(
                        "{} 这一腿要减的 {} 不到交易所一个数量步长 {}，两腿都不减（只减一条会拆开对冲）",
                        venue_of(item.0)?,
                        item.1.round_dp(8).normalize(),
                        step.normalize()
                    ),
                ));
            }
            item.1 = rounded;
        }
        Ok(out)
    }

    async fn concurrent_quantity(
        &self,
        plan: &Plan,
        first: &LegPlan,
        second: &LegPlan,
    ) -> Option<Decimal> {
        let hyperliquid = matches!(
            first.venue,
            Venue::Hyperliquid | Venue::HyperliquidXyz | Venue::HyperliquidIo
        );
        if plan.min_locked_basis.is_none()
            || first.maker_rest_ms.is_some()
            || second.maker_rest_ms.is_some()
            || hyperliquid
            || first.limit_price <= Decimal::ZERO
        {
            return None;
        }
        let step = |leg: &LegPlan| {
            let broker = self.broker(leg.venue).ok();
            let (venue, symbol) = (leg.venue, leg.symbol.clone());
            async move {
                match broker?.quantity_step(&symbol).await {
                    Ok(step) => step.filter(|step| *step > Decimal::ZERO),
                    Err(error) => {
                        warn!(%venue, %symbol, %error, "读不到数量步长，两腿照旧顺序下");
                        None
                    }
                }
            }
        };
        let (a, b) = tokio::join!(step(first), step(second));
        let (a, b) = (a?, b?);
        let (coarse, fine) = if a >= b { (a, b) } else { (b, a) };
        if !(coarse % fine).is_zero() {
            return None;
        }
        crate::live_common::floor_to_step(first.notional_usdt / first.limit_price, coarse)
            .filter(|quantity| *quantity > Decimal::ZERO)
    }

    /// 双腿的**第一腿**：同 [`Self::submit_with_id`]，但券商能先报出成交数量时
    /// （[`Broker::place_for_hedge`]）立刻返回，好让第二腿马上发出。第三项是待核实的订单状态：
    /// 成交额、均价是偏保守的估计，手续费是估的 —— 第二腿发出之后用 [`Self::settle`] 换成最终状态。
    /// 券商一次到位时第三项为 `None`，与 `submit_with_id` 完全相同。`quantity` 只在两腿同时下时给
    /// （两腿同一个数量，见 [`Self::concurrent_quantity`]），平常按名义额下。
    async fn submit_first(
        &self,
        client_order_id: ClientOrderId,
        leg: &LegPlan,
        leverage: Option<Decimal>,
        margin_mode: crate::MarginMode,
        quantity: Option<Decimal>,
    ) -> ArbResult<(Option<LegFill>, Option<String>, Option<OrderState>)> {
        let order = leg_order(client_order_id, leg, leverage, margin_mode, quantity);
        let rest = leg.maker_rest_ms.map(Duration::from_millis);
        let (state, provisional) = self.execute_first(&order, rest).await?;
        let pending = provisional.then(|| state.clone());
        let (fill, reason) = leg_outcome(&order, state)?;
        Ok((fill, reason, pending))
    }

    /// 把第一腿的待核实状态换成交易所的最终状态（手续费、精确成交额）并落盘，返回按最终状态算的腿。
    ///
    /// 查不成（网络错误、查到的对不上）就按待核实的数落盘并警告：台账里不能留一笔还在 `Pending` 的订单
    /// —— 对账会把它当成「交易所上没有的挂单」，拦住之后的开仓。
    async fn settle(&self, provisional: &OrderState) -> ArbResult<Option<LegFill>> {
        let order = &provisional.order;
        let looked_up = match self.broker(order.venue) {
            Ok(broker) => broker.settle_hedge(provisional).await,
            Err(error) => Err(error),
        };
        let state = match looked_up {
            Ok(Some(state))
                if state.order == *order
                    && !state.status.is_live()
                    && state.filled_usdt > Decimal::ZERO
                    && state
                        .average_price
                        .is_some_and(|price| price > Decimal::ZERO) =>
            {
                state
            }
            other => {
                let why = match other {
                    Ok(Some(_)) => "交易所返回的状态与待核实的对不上".to_string(),
                    Ok(None) => "交易所查不到这笔订单".to_string(),
                    Err(error) => error.to_string(),
                };
                warn!(
                    order = %order.client_order_id,
                    venue = %order.venue,
                    notional = %provisional.filled_usdt,
                    fee = %provisional.fee_usdt,
                    "第一腿的手续费与精确成交额没核实成（{why}），台账先按估算记"
                );
                provisional.clone()
            }
        };
        self.persist_order(&state).await?;
        Ok(leg_fill(order, &state))
    }

    /// 同 [`Self::execute_order`]，但用 [`Broker::place_for_hedge`] 下单：返回 `(状态, 是否待核实)`。
    /// 待核实的状态**不落盘**（台账里这笔订单暂时还是 `Pending`），由 [`Self::settle`] 落最终状态。
    async fn execute_first(
        &self,
        order: &NewOrder,
        rest: Option<Duration>,
    ) -> ArbResult<(OrderState, bool)> {
        // 只挂单的单子：券商不支持、或没给挂多久，一个请求都不发（也不往台账写意图）。
        self.check_post_only(order, rest)?;
        let broker = self.broker(order.venue)?;
        self.persist_order(&OrderState::new(order.clone())).await?;
        let placed = match broker.place_for_hedge(order).await {
            Ok(Some(state))
                if state.order == *order
                    && !state.status.is_live()
                    && state.filled_usdt > Decimal::ZERO
                    && state
                        .average_price
                        .is_some_and(|price| price > Decimal::ZERO) =>
            {
                return Ok((state, true));
            }
            // 没给出能用的待核实状态（没成交、券商一次到位）：照常按 `order_state` 读完整结果。
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        };
        self.resolve_placed(order, placed, rest)
            .await
            .map(|state| (state, false))
    }

    /// 只挂单的单子发出前的检查：券商必须支持，且必须给了挂多久。
    fn check_post_only(&self, order: &NewOrder, rest: Option<Duration>) -> ArbResult<()> {
        if !order.post_only {
            return Ok(());
        }
        if !self.broker(order.venue)?.supports_post_only() {
            return Err(ArbError::venue(
                order.venue.as_str(),
                "这个场所没有接入只挂单（post-only），不能当 IOC 悄悄发出去",
            ));
        }
        if rest.is_none() || order.reduce_only {
            return Err(ArbError::config(
                "只挂单必须是开仓单，并且要给出最长挂单时间",
            ));
        }
        Ok(())
    }

    /// 提交意图先落盘。未知结果不能记为拒单，也不能开始下一条腿。
    ///
    /// `place` 报错且场所不认识这个 id（[`Broker::order_state`] 返回 `None`）才算明确
    /// 没发出去，记为拒单；券商在无法确定时必须返回错误而不是 `None`。
    async fn execute_order(&self, order: &NewOrder) -> ArbResult<OrderState> {
        // 挂在簿上等的只有第一腿（`execute_first`）；其余的单（对冲腿、平仓、回滚）一律是 IOC。
        self.check_post_only(order, None)
            .map_err(|_| ArbError::config("只挂单只能作为双腿开仓的第一腿"))?;
        // 先确认有这家的券商再落意图：没配 / 没连上的场所，落了意图就是一张永远 Pending 的单，之后每次重试退出都卡在它上面。
        let broker = self.broker(order.venue)?;
        self.persist_order(&OrderState::new(order.clone())).await?;
        let placed = broker.place(order).await.map(|_| ());
        self.resolve_placed(order, placed, None).await
    }

    /// `place` 之后：以 [`Broker::order_state`] 为准定下这笔订单的状态并落盘；还活着就撤掉再读。
    ///
    /// 只挂单的单子（`rest` 给了最长挂单时间）先挂在簿上等：见 [`Self::rest_on_book`]。等完还活着的
    /// 同样撤掉再读，撤的时候它恰好成交了（撤单报「订单不存在」）也没关系 —— 以撤后读到的状态为准。
    async fn resolve_placed(
        &self,
        order: &NewOrder,
        placed: ArbResult<()>,
        rest: Option<Duration>,
    ) -> ArbResult<OrderState> {
        let broker = self.broker(order.venue)?;
        // 下单请求自己报的错先记下来：之后以读回为准，读回也说不清时，这一行是唯一能看出交易所当时说了什么的地方
        // （某次实盘 补仓：币安下单 3.5 秒后报错，读回一直「不认识这个订单号」，原错误没留下）。
        if let Err(error) = &placed {
            warn!(order = %order.client_order_id, venue = %order.venue, %error, "下单请求返回出错：以读回的订单状态为准");
        }
        // 刚下的单同样可能还查不到（见 READ_SETTLE_BACKOFF）：读出错、或下单没报错却查不到，都重读；
        // 券商明确不认识这个 id 且下单报了错 = 没发出去，不用等。
        let mut read = broker.order_state(&order.client_order_id).await;
        let mut rereads = 0;
        for delay in READ_SETTLE_BACKOFF {
            match &read {
                Ok(Some(_)) => break,
                Ok(None) if placed.is_err() => break,
                Ok(None) => {
                    warn!(order = %order.client_order_id, "刚下的单已受理但还查不到，稍后重读");
                }
                Err(error) => {
                    warn!(order = %order.client_order_id, %error, "刚下的单读回出错，稍后重读");
                }
            }
            tokio::time::sleep(delay).await;
            read = broker.order_state(&order.client_order_id).await;
            rereads += 1;
        }
        if rereads > 0 && matches!(read, Ok(Some(_))) {
            info!(order = %order.client_order_id, rereads, "刚下的单头几次读回落后于交易所，重读后已读到");
        }
        let mut state = match (read?, placed) {
            (Some(state), _) => state,
            (None, Err(error)) => {
                let mut rejected = OrderState::new(order.clone());
                rejected.status = OrderStatus::Rejected;
                rejected.reject_reason = Some(error.to_string());
                self.persist_order(&rejected).await?;
                return Ok(rejected);
            }
            (None, Ok(_)) => {
                return Err(ArbError::venue(
                    order.venue.as_str(),
                    format!(
                        "订单 {} 已受理但查不到，停止执行并人工对账",
                        order.client_order_id
                    ),
                ));
            }
        };
        if state.order != *order {
            return Err(ArbError::venue(
                order.venue.as_str(),
                "查询到的订单与已落盘意图不一致",
            ));
        }
        self.persist_order(&state).await?;
        if state.status.is_live()
            && order.post_only
            && let Some(rest) = rest
        {
            state = self.rest_on_book(order, state, rest).await?;
        }
        if state.status.is_live() {
            let venue_id = state.venue_order_id.clone().ok_or_else(|| {
                ArbError::venue(order.venue.as_str(), "活跃订单缺交易所单号，无法撤单")
            })?;
            let cancelled = broker.cancel(&venue_id).await;
            if let Err(error) = cancelled {
                // IOC 单撤不掉就是出事了。挂着的单可能恰好在撤单前成交：错误不当结论，读回状态再定。
                if !order.post_only {
                    return Err(error);
                }
                warn!(order = %order.client_order_id, %error, "挂单撤单报错，读回订单状态再判断");
            }
            // 读到活跃、读回出错都先不下结论：查询可能还没跟上撮合（见 READ_SETTLE_BACKOFF）。
            let mut read = self.read_back_after_cancel(order).await;
            let mut rereads = 0;
            for (attempt, delay) in READ_SETTLE_BACKOFF.into_iter().enumerate() {
                match &read {
                    Ok(state) if !state.status.is_live() => break,
                    Ok(_) => {}
                    Err(error) => {
                        warn!(order = %order.client_order_id, %error, "撤单后读回出错，稍后重读");
                    }
                }
                tokio::time::sleep(delay).await;
                if attempt == CANCEL_RESEND_AT
                    && read.as_ref().is_ok_and(|state| state.status.is_live())
                    && let Err(error) = broker.cancel(&venue_id).await
                {
                    warn!(order = %order.client_order_id, %error, "撤单后仍读到活跃，再撤一次报错，以读回为准");
                }
                read = self.read_back_after_cancel(order).await;
                rereads += 1;
            }
            state = read?;
            if state.status.is_live() {
                return Err(ArbError::venue(
                    order.venue.as_str(),
                    "撤单后仍有活跃订单，停止双腿执行",
                ));
            }
            if rereads > 0 {
                info!(
                    order = %order.client_order_id,
                    rereads,
                    status = ?state.status,
                    filled = %state.filled_usdt,
                    "撤单后头几次读回落后于交易所，重读后已落终态"
                );
            }
        }
        Ok(state)
    }

    /// 撤单后读回一次订单：核对是不是同一笔意图，读到的状态落盘。
    async fn read_back_after_cancel(&self, order: &NewOrder) -> ArbResult<OrderState> {
        let state = self
            .broker(order.venue)?
            .order_state(&order.client_order_id)
            .await?
            .ok_or_else(|| ArbError::venue(order.venue.as_str(), "撤单后订单状态无法核实"))?;
        if state.order != *order {
            return Err(ArbError::venue(
                order.venue.as_str(),
                "撤单后查询返回了不同订单意图/保证金模式",
            ));
        }
        self.persist_order(&state).await?;
        Ok(state)
    }

    /// 只挂单的第一腿挂出之后等成交：每 [`MAKER_POLL`] 读一次订单状态，直到
    /// - 订单不再活着（全部成交、被撤、被拒），或
    /// - 到了最长挂单时间（[`MAKER_MAX_REST`] 封顶），或
    /// - **成交够对冲了**（≥ [`MAKER_PARTIAL_HEDGE_MIN_USDT`]）：此后最多再等 [`MAKER_FILL_GRACE`]，让它多成交一点，
    ///   然后不再等。已成交的部分没有对冲，不能为了等剩下的而拉长；多笔增量对冲更不做 —— 撤掉剩余之后读一次
    ///   最终成交量，对冲只发一次。成交还不够对冲时不开始宽限，继续挂到截止。
    ///
    /// 读不到状态（网络错误）不算结论：不再等，交给调用方撤单；撤单之后读回的状态才是结论。
    /// 状态有变化（成交量、状态）才落盘，不是每次轮询都写台账。
    async fn rest_on_book(
        &self,
        order: &NewOrder,
        mut state: OrderState,
        rest: Duration,
    ) -> ArbResult<OrderState> {
        let broker = self.broker(order.venue)?;
        let started = Instant::now();
        let mut deadline = started + rest.min(MAKER_MAX_REST);
        // 宽限只在成交够对冲了才开始；成交还不够对冲腿的最小额时继续挂到截止，指望再成交一手。
        let hedgeable = |state: &OrderState| state.filled_usdt >= MAKER_PARTIAL_HEDGE_MIN_USDT;
        let mut filled_seen = hedgeable(&state);
        if filled_seen {
            deadline = deadline.min(Instant::now() + MAKER_FILL_GRACE);
        }
        while state.status.is_live() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep(MAKER_POLL.min(deadline - now)).await;
            let polled = match broker.order_state(&order.client_order_id).await {
                Ok(Some(polled)) => polled,
                Ok(None) => {
                    warn!(order = %order.client_order_id, "挂单读不到订单状态，不再等，撤单后读回");
                    break;
                }
                Err(error) => {
                    warn!(order = %order.client_order_id, %error, "挂单轮询出错，不再等，撤单后读回");
                    break;
                }
            };
            if polled.order != *order {
                return Err(ArbError::venue(
                    order.venue.as_str(),
                    "挂单轮询时查询到的订单与已落盘意图不一致",
                ));
            }
            let changed = polled.status != state.status || polled.filled_usdt != state.filled_usdt;
            state = polled;
            if changed {
                self.persist_order(&state).await?;
            }
            if !filled_seen && hedgeable(&state) {
                filled_seen = true;
                deadline = deadline.min(Instant::now() + MAKER_FILL_GRACE);
                info!(
                    order = %order.client_order_id,
                    filled = %state.filled_usdt,
                    waited_ms = started.elapsed().as_millis() as u64,
                    "挂单的成交够对冲了，再多等一小会儿就撤掉剩余、按成交量对冲"
                );
            }
        }
        Ok(state)
    }

    async fn persist(&self, position: &PairPosition) -> ArbResult<()> {
        self.ledger
            .append(&Record::Position(Box::new(position.clone())))
            .await
            .map_err(ArbError::from)
    }

    async fn persist_order(&self, state: &OrderState) -> ArbResult<()> {
        self.ledger
            .append(&Record::Order(Box::new(state.clone())))
            .await
            .map_err(ArbError::from)
    }

    /// 这笔仓位的订单里有没有任何成交（按订单号前缀 `{仓位}-` 在台账里找）。
    async fn has_fills(&self, position_id: &str) -> ArbResult<bool> {
        let (replayed, _) = self.ledger.replay().await?;
        let prefix = format!("{position_id}-");
        Ok(replayed
            .orders
            .iter()
            .any(|(id, order)| id.starts_with(&prefix) && order.filled_usdt > Decimal::ZERO))
    }
}

/// 一条腿的开仓单（IOC 限价，限价来自计划）。
/// 开仓前：这家场所还没有这个合约的持仓。有就拒绝 —— 改保证金模式 / 杠杆或叠加开仓都会动到原仓位。
pub async fn ensure_flat(broker: &dyn Broker, symbol: &arb_core::Symbol) -> ArbResult<()> {
    if broker
        .positions()
        .await?
        .iter()
        .any(|position| position.symbol == *symbol && position.net_quantity != Decimal::ZERO)
    {
        return Err(ArbError::venue(
            broker.venue().as_str(),
            "该合约已有持仓，拒绝修改保证金模式/杠杆或叠加开仓；请先核对并退出原仓位",
        ));
    }
    Ok(())
}

fn leg_order(
    client_order_id: ClientOrderId,
    leg: &LegPlan,
    leverage: Option<Decimal>,
    margin_mode: crate::MarginMode,
    quantity: Option<Decimal>,
) -> NewOrder {
    NewOrder {
        post_only: leg.maker_rest_ms.is_some(),
        client_order_id,
        venue: leg.venue,
        symbol: leg.symbol.clone(),
        side: leg.side,
        notional_usdt: leg.notional_usdt,
        quantity,
        limit_price: Some(leg.limit_price),
        reduce_only: false,
        leverage,
        margin_mode,
    }
}

/// 订单状态 → 这条腿的成交（没成交为 `None`）与没完全成交的原因。
fn leg_outcome(
    order: &NewOrder,
    state: OrderState,
) -> ArbResult<(Option<LegFill>, Option<String>)> {
    if state.filled_usdt > Decimal::ZERO
        && state
            .average_price
            .is_none_or(|price| price <= Decimal::ZERO)
    {
        return Err(ArbError::venue(
            order.venue.as_str(),
            "订单已有成交但缺合法均价，停止对冲并对账",
        ));
    }
    let fill = leg_fill(order, &state).filter(|fill| fill.notional_usdt > Decimal::ZERO);
    let post_only = order.post_only;
    let reason = (state.status != OrderStatus::Filled).then(|| {
        state.reject_reason.unwrap_or_else(|| {
            if post_only && fill.is_none() {
                "只挂单没有成交，到点已撤".to_string()
            } else if post_only {
                format!("只挂单只成交了一部分（订单状态 {:?}）", state.status)
            } else {
                format!("订单状态 {:?}", state.status)
            }
        })
    });
    Ok((fill, reason))
}

/// 第二腿的限价被保本线收紧了的话（[`Plan::hedged_after`]），说清楚收紧到哪、为什么：没成交回滚时写进备注。
fn breakeven_note(plan: &Plan, hedged: &Plan, first: &LegFill) -> Option<String> {
    let (before, after) = (plan.second_leg(), hedged.second_leg());
    let strict = hedged.min_locked_basis?;
    let floor = hedged.hedge_floor()?;
    let pct = |value: Decimal| (value * Decimal::ONE_HUNDRED).round_dp(4).normalize();
    (after.limit_price != before.limit_price).then(|| {
        format!(
            "第二腿（{} {}）限价 {}：按第一腿成交均价 {} 反推的保本价（两腿锁定价差至少 {}% = 保本线 {}% − 回滚第一腿的成本 {}%：\
             再低就不如回滚；原限价 {}）；没吃到时同一限价最多试 {} 次",
            after.venue,
            side_label(after.side),
            after.limit_price.round_dp(8).normalize(),
            first.average_price.round_dp(8).normalize(),
            pct(floor),
            pct(strict),
            pct(hedged.rollback_allowance()),
            before.limit_price.round_dp(8).normalize(),
            SECOND_LEG_ATTEMPTS
        )
    })
}

/// [`Executor::before_exit`]：平仓前记下、平完写备注和查资金费要用的。
struct BeforeExit {
    entry_prices: Option<(Decimal, Decimal)>,
    entry_notional: Option<Decimal>,
    funding_venues: Vec<Venue>,
    /// 进来时是 `Unwinding`（开仓失败、回滚上一轮没做完）：平完记 `Unwound`，不能算成一笔正常平仓 ——
    /// 否则连续回滚的熔断、开仓成功率和「规则已执行」推送都会把它当成功。
    unwinding: bool,
}

/// 获利平仓的价格保护（[`Executor::close_guarded`]）：先单独发一条腿的限价 IOC，成交了才平另一条。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExitGuard {
    /// 先发的那条腿的平仓方向：`Sell` = 多腿、`Buy` = 空腿。
    pub side: Side,
    /// 它的限价。
    pub limit_price: Decimal,
}

/// [`Executor::close_guarded`] 的结局。
#[derive(Debug, Clone, PartialEq)]
pub enum GuardedExit {
    /// 两条腿都平掉了（或者没平干净、留在 `Closing` 由重试接着平：那时返回的是错误）。
    Closed,
    /// 先发的那条腿在限价内一点没成交：两条腿都没动，仓位回到 `Open`。附原因。
    Held(String),
}

/// 两腿反向操作：(场所, 方向, 入场名义额, 标的数量)；名义大的先平。
/// 一张退出单的结果记进仓位：成交部分的价格盈亏当场入账，完全成交的腿摘掉并结算手续费，
/// 部分成交按成交数量缩小那条腿。返回这张单是否把这条腿平完了。
///
/// `exit_legs`（刚发完的单）和 `settle_prior_exits`（重试前读回的旧单）共用这一份记账。
fn apply_exit_state(
    position: &mut PairPosition,
    side: Side,
    quantity: Decimal,
    state: &OrderState,
    exits: &mut Vec<(Side, Decimal)>,
) -> bool {
    let filled = state
        .average_price
        .filter(|price| *price > Decimal::ZERO)
        .map_or(Decimal::ZERO, |price| state.filled_usdt / price);
    // 平多腿是卖、平空腿是买。
    let slot = match side {
        Side::Sell => &mut position.long,
        Side::Buy => &mut position.short,
    };
    let done = state.status == OrderStatus::Filled && filled > Decimal::ZERO;
    // 入场价在腿被移除之前取：这一笔成交的价格盈亏当场入账。
    if let (Some(leg), Some(price)) = (slot.as_ref(), state.average_price)
        && filled > Decimal::ZERO
    {
        position.realized_pnl_usdt += match side {
            Side::Sell => filled * (price - leg.average_price),
            Side::Buy => filled * (leg.average_price - price),
        };
        exits.push((side, price));
    }
    if done {
        if let Some(leg) = slot.take() {
            position.realized_fee_usdt += leg.fee_usdt + state.fee_usdt;
        }
    } else if let Some(leg) = slot.as_mut().filter(|_| filled > Decimal::ZERO) {
        leg.notional_usdt *= (Decimal::ONE - filled / quantity).max(Decimal::ZERO);
        leg.fee_usdt += state.fee_usdt;
    }
    done
}

/// 退出单号 `{仓位}-{buy|sell}-exit{n}` 里的 n；认不出来排最后。
fn exit_number(client_order_id: &str) -> u32 {
    client_order_id
        .rsplit_once("-exit")
        .and_then(|(_, n)| n.parse().ok())
        .unwrap_or(u32::MAX)
}

fn legs_of(position: &PairPosition) -> ArbResult<Vec<(Venue, Side, Decimal, Decimal)>> {
    let mut legs = Vec::with_capacity(2);
    for (leg, side) in [
        (position.long.as_ref(), Side::Sell),
        (position.short.as_ref(), Side::Buy),
    ] {
        if let Some(leg) = leg {
            let quantity = leg
                .quantity()
                .filter(|quantity| *quantity > Decimal::ZERO)
                .ok_or_else(|| {
                    ArbError::config(format!("{} 腿标的数量不可用，不能平仓", leg.venue))
                })?;
            legs.push((leg.venue, side, leg.notional_usdt, quantity));
        }
    }
    legs.sort_by_key(|leg| std::cmp::Reverse(leg.2));
    Ok(legs)
}

/// 两腿数量偏差在这个比例以内算对等（步长取整造成的零头），与建仓时的判断相同。
const HEDGE_TOLERANCE: Decimal = Decimal::from_parts(5, 0, 0, false, 3);

/// 只挂单的第一腿部分成交时，成交名义至少这么多（USDT）就按它对冲：只要够对冲场所（Arcus、Lighter RH 各
/// 约 10 USDT）的最小下单额。币安 TradFi 合约一手 0.01 张，某合约 约 17、某合约 约 20 USDT，成交是一手一手来的，
/// 沿用吃单的 20 USDT 会让一手成交的单子（某合约）几乎都被回滚。
pub const MAKER_PARTIAL_HEDGE_MIN_USDT: Decimal = Decimal::from_parts(12, 0, 0, false, 0);
/// 只挂单第一腿成交太小、凑不够对冲腿最小额而回滚时，写进仓位备注的标记：这种回滚只付一笔平仓的吃单费
/// 与穿价，不说明有什么东西坏了，调用方（自动交易、熔断）据此不把它算作失败。
pub const MAKER_DUST_NOTE: &str = "挂单成交的量太小，凑不够对冲腿的最小下单额，已回滚";

/// 价差单第一腿部分成交时，成交名义至少这么多（USDT）才按它对冲第二腿：各家最小下单额（Arcus、Lighter
/// 约 10 USDT）以上，第二腿不会因为太小被拒。
pub const PARTIAL_HEDGE_MIN_USDT: Decimal = Decimal::from_parts(20, 0, 0, false, 0);

/// 只挂单的第一腿挂在簿上时，多久读一次订单状态。币安读一次订单权重 1，挂 10 秒约 40 次。
#[cfg(not(test))]
pub const MAKER_POLL: Duration = Duration::from_millis(250);
#[cfg(test)]
pub const MAKER_POLL: Duration = Duration::from_millis(5);
/// 只挂单的第一腿**一旦有成交**，最多再等这么久就撤掉剩下的、按已成交的量对冲：成交了的部分是
/// 没对冲的敞口，不能为了等剩下的而拉长。
#[cfg(not(test))]
pub const MAKER_FILL_GRACE: Duration = Duration::from_secs(2);
#[cfg(test)]
pub const MAKER_FILL_GRACE: Duration = Duration::from_millis(40);
/// 只挂单的第一腿最长挂多久（不论计划里给多少）：挂着的时候交易台的锁被占着，持仓规则轮要等。
pub const MAKER_MAX_REST: Duration = Duration::from_secs(20);

/// 读回订单还没跟上交易所时的重读间隔。有的交易所查询落后于撮合（币安，东京）：
/// - 刚下的单：受理 0.15 秒后按订单号查，回「不存在」（10-16 一次，执行中断、挂单无人看管 23 秒）；
/// - 撤单后：撤单当场生效，之后 7~100ms 内查到的仍是撤单前的「部分成交、活跃」（10-16 三次挂单撤单全中，
///   一次留下了没对冲的成交）；成交明细也会晚到。
///
/// 读出错、查不到、撤后仍活跃都先不下结论，按这个间隔重读，读完仍是那样才算数。
#[cfg(not(test))]
const READ_SETTLE_BACKOFF: [Duration; 5] = [
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
    Duration::from_millis(1_600),
];
#[cfg(test)]
const READ_SETTLE_BACKOFF: [Duration; 5] = [Duration::from_millis(2); 5];
/// 重读到第几次（从 0 数）之前再撤一次：万一第一次撤单真没送到。对已撤的单重复撤单无害。
const CANCEL_RESEND_AT: usize = 2;

/// 第二腿一个都没吃到时最多提交几次（含第一次）。
pub const SECOND_LEG_ATTEMPTS: u32 = 3;

/// 交易所说这一单会成交在价格带外（Arcus 盘后被钉在带边）：同价重试、强制补腿追更差的价，限价都会被
/// 收回同一个带边、再被拒，只是白白多裸露两三秒（某次实盘 等四笔各试了 3 次）。直接回滚。
fn band_rejected(reason: &str) -> bool {
    reason.contains(arb_venues::arcus::BOUND_REJECT)
}

/// 第二腿被拒的原因里，同价重试、强制追价都不会改变结局的（确定性拒单）：直接回滚，不再多裸两三秒。
/// 排除清单而不是允许清单：认不出的原因（各家「没对手盘」的说法不一样）照旧重试。台账里实际见过的：
/// Arcus `FILL_WILL_EXCEED_TRADING_BOUND`（价格带）、Lighter `canceled-margin-not-allowed` /
/// `canceled-position-not-allowed`、HL `Price too far from oracle`；另外各家保证金、余额不足、低于最小下单额。
/// 「没对手盘」的那几种（HL `iocCancelRejected` / `could not immediately match`、Lighter `not-enough-liquidity`）
/// 不在里面 —— 薄盘口几百毫秒就补上，重试值得。
fn pointless_retry(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    band_rejected(reason)
        || [
            "margin",
            "position-not-allowed",
            "too far from oracle",
            "insufficient",
            "balance",
            "min notional",
            "min_notional",
            "minimum",
            "reduce-only",
            "reduce only",
        ]
        .iter()
        .any(|pattern| lower.contains(pattern))
        || ["保证金", "余额", "最小下单"]
            .iter()
            .any(|pattern| reason.contains(pattern))
}
/// 两次提交之间等多久：薄盘口被吃空后通常几百毫秒就有人补单。
pub const SECOND_LEG_RETRY_GAP: std::time::Duration = std::time::Duration::from_millis(400);
/// 从第二腿第一次提交起，最晚这么久还没吃到就不再试、回滚第一腿（只有一条腿的窗口不能无限拉长）。
pub const SECOND_LEG_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);

/// 强制补腿最多追几次（每次只追还没对冲的剩余数量）。
pub const FORCE_HEDGE_ATTEMPTS: u32 = 2;
/// 两次追单之间等多久。
pub const FORCE_HEDGE_GAP: std::time::Duration = std::time::Duration::from_millis(300);
/// 从第一次追单起，最晚这么久还没追齐就不再追、回滚（有自己的预算：保本价那一档的尝试多半已经把
/// [`SECOND_LEG_RETRY_WINDOW`] 用完了）。
pub const FORCE_HEDGE_WINDOW: std::time::Duration = std::time::Duration::from_secs(4);
/// 开仓第二腿有成交、但数量对不上时，第二腿至少成交了第一腿数量的这个比例（一半）才保留仓位、退掉第一腿的零头；
/// 更少就照旧回滚（对冲太小，持有的意义不大，平仓时还可能低于交易所最小下单额）。
pub const KEEP_HEDGED_MIN: Decimal = Decimal::from_parts(5, 0, 0, false, 1);
/// 零头退不掉（低于交易所最小下单额）时，两腿数量差不超过这个比例（5%）……
pub const RESIDUAL_ACCEPT_PCT: Decimal = Decimal::from_parts(5, 0, 0, false, 2);
/// ……且不超过这么多 USDT，就带着这点差额持有，不回滚。
pub const RESIDUAL_ACCEPT_USDT: Decimal = Decimal::from_parts(25, 0, 0, false, 0);
/// 零头的名义低于它就不发退单（各家最小下单额约 10 USDT，同 [`MAKER_PARTIAL_HEDGE_MIN_USDT`]）。
pub const UNDO_MIN_USDT: Decimal = MAKER_PARTIAL_HEDGE_MIN_USDT;
/// 一张减仓单的成交记进那条腿：减掉的部分按成交价记已实现价格盈亏；手续费留在腿上，腿完全平掉时一并结算。
/// 没有成交返回 `None`（腿不变）。
fn apply_trim_fill(
    position: &mut PairPosition,
    is_long: bool,
    state: &OrderState,
) -> ArbResult<Option<Decimal>> {
    let Some((filled_quantity, fill_price, fee)) = state
        .average_price
        .filter(|price| *price > Decimal::ZERO)
        .and_then(|price| {
            (state.filled_usdt > Decimal::ZERO).then_some((
                state.filled_usdt / price,
                price,
                state.fee_usdt,
            ))
        })
    else {
        return Ok(None);
    };
    let leg = if is_long {
        position.long.as_ref()
    } else {
        position.short.as_ref()
    }
    .cloned()
    .ok_or_else(|| ArbError::config("减仓单对应的腿不在仓位上"))?;
    let quantity = leg
        .quantity()
        .ok_or_else(|| ArbError::config(format!("{} 腿入场价非正", leg.venue)))?;
    let kept = (Decimal::ONE - filled_quantity / quantity).max(Decimal::ZERO);
    let realized = match leg.side {
        Side::Buy => filled_quantity * (fill_price - leg.average_price),
        Side::Sell => filled_quantity * (leg.average_price - fill_price),
    };
    position.realized_pnl_usdt += realized;
    let updated = LegFill {
        notional_usdt: leg.notional_usdt * kept,
        fee_usdt: leg.fee_usdt + fee,
        margin_usdt: leg.margin_usdt.map(|margin| margin + realized - fee),
        ..leg
    };
    if is_long {
        position.long = Some(updated);
    } else {
        position.short = Some(updated);
    }
    Ok(Some(filled_quantity))
}

/// 第二腿与第一腿的数量偏差不超过它（0.5%）就算对冲到位。
pub const SECOND_LEG_QTY_TOLERANCE: Decimal = Decimal::from_parts(5, 0, 0, false, 3);
/// 开仓时两腿数量对不上、没有回滚而是退掉零头（或带着小差额）持有的仓位，备注以它开头。
/// 补仓回退后剩下的差额能不能带着持有：金额不超过 [`RESIDUAL_ACCEPT_USDT`]，且比例不超过 [`RESIDUAL_ACCEPT_PCT`]
/// 或低于最小下单额（本来就退不掉）。与开仓 `balance_legs` 同一口径。
fn accept_residual(residual: Decimal, quantity: Decimal, price: Decimal) -> bool {
    let usdt = residual * price;
    usdt <= RESIDUAL_ACCEPT_USDT
        && (residual / quantity <= RESIDUAL_ACCEPT_PCT || usdt < UNDO_MIN_USDT)
}

pub const BALANCED_HEDGE_NOTE: &str = "两腿数量对齐，没有回滚";
/// 用了强制补腿的仓位，备注以它开头。自动交易的事件、通知据此提示「这笔的价差是被追出来的」。
pub const FORCED_HEDGE_NOTE: &str = "第二腿按强制补腿价成交";
/// 续上中断的建仓（[`Executor::resume_open`]）写进备注的开头。
pub const RESUMED_NOTE: &str = "建仓中断后已自动续上";
/// 续上中断的建仓补第二腿时，仓位没记开仓时的强制补腿容差（旧仓位、开仓时没开）就按这个：锁定价差不低于 −0.3%，
/// 与自动交易现行的强制补腿设置一致。
pub const RESUME_HEDGE_TOLERANCE: Decimal = Decimal::from_parts(3, 0, 0, false, 3);

/// [`Executor::resume_open`] 这一次走到了哪一步。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resume {
    /// 还有开仓单读不到结局、或还在场上：这一轮什么单都没发，下一轮再查。
    Waiting(String),
    /// 自己判断不了、要人来处理（订单与意图对不上、认不出第一腿、不知道第二腿该在哪家补……）：什么单都没发。
    Stuck(String),
    /// 补了第二腿，最差限价内没补齐：不回滚，第一腿留着，下一轮再补（原因在里面）。
    Retry(String),
    /// 刚读回了开仓单的结局、把腿补进了台账，还差的部分要发单：先等补完腿之后的那次对账（调用方马上再对一次账）。
    Verified,
    /// 还差的部分要发单，但这个币（或两腿所在场所）的对账还不干净：不发单，等对账干净。
    Frozen,
    /// 收尾了：转为持仓，或两腿都没成交记为 `Unwound`（看仓位的状态与备注）。
    Done,
}

/// [`Executor::settle_add_order`] 对一张补仓单的处理结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddSettle {
    /// 没成交（读回是未成交的终态、券商没收到，或读不到但场所持仓与台账一致），已在台账里了结；原来的仓位没动。
    NoFill(String),
    /// 读回时已有成交（USDT）：订单记进了台账，但没并进腿，台账与交易所的持仓对不上 —— 要人工核对。
    Filled(Decimal),
    /// 读回已成交，是没并进腿的第二腿（第一腿已并进、差额对得上）：已并进腿，两腿对上（不足的已退第一腿）。
    Hedged(String),
    /// 了结了，但两腿没拉齐（退单没做成、减仓只减了一条腿……）：两腿数量可能不对等，要人工核对（推告警）。
    Unbalanced(String),
    /// 读回了结局并已记进腿（减仓单、退单的成交；补仓第一腿的孤单成交已退掉），两腿对等。
    Applied(String),
    /// 这一轮判断不了（还在场上、与意图对不上、读不到且持仓对不上、核对没做成）：什么都没改，下一轮再看。
    Waiting(String),
}

/// 这笔仓位已经尝试过几次补仓（台账里补仓、补仓回退单的最大序号；发过单就算，不论成败）。
/// 补仓次数上限按它数：失败的尝试也占次数，偏保守。
pub fn add_rounds(position_id: &str, orders: &HashMap<String, OrderState>) -> u32 {
    orders
        .values()
        .filter_map(|state| state.order.client_order_id.parse_for(position_id))
        .filter_map(|(_, role)| match role {
            crate::types::OrderRole::Add { n } | crate::types::OrderRole::Undo { n } => Some(n),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// 把补仓的成交并进腿：开仓均价按数量加权，名义、手续费累加；有杠杆（逐仓）时保证金加上
/// 这笔名义 ÷ 杠杆。
fn merge_leg(position: &mut PairPosition, fill: &LegFill) {
    let leverage = position
        .leverage
        .filter(|leverage| !position.margin_mode.is_cross() && *leverage > Decimal::ZERO);
    let slot = match fill.side {
        Side::Buy => &mut position.long,
        Side::Sell => &mut position.short,
    };
    let Some(leg) = slot.as_mut() else {
        return;
    };
    let quantity =
        leg.quantity().unwrap_or(Decimal::ZERO) + fill.quantity().unwrap_or(Decimal::ZERO);
    let notional = leg.notional_usdt + fill.notional_usdt;
    if quantity > Decimal::ZERO {
        leg.average_price = notional / quantity;
    }
    leg.notional_usdt = notional;
    leg.fee_usdt += fill.fee_usdt;
    if let (Some(margin), Some(leverage)) = (leg.margin_usdt, leverage) {
        leg.margin_usdt = Some(margin + fill.notional_usdt / leverage);
    }
}

/// 记下一条腿。有杠杆时同时记下它的逐仓保证金 = 名义 ÷ 杠杆。
/// 同一条腿的几笔成交合成一条：名义、手续费相加，均价按数量加权。一笔时原样返回，没有成交返回 `None`。
fn merge_fills(fills: &[LegFill]) -> Option<LegFill> {
    let first = fills.first()?;
    if fills.len() == 1 {
        return Some(first.clone());
    }
    let notional: Decimal = fills.iter().map(|fill| fill.notional_usdt).sum();
    let quantity: Decimal = fills.iter().filter_map(LegFill::quantity).sum();
    if quantity <= Decimal::ZERO {
        return Some(first.clone());
    }
    Some(LegFill {
        notional_usdt: notional,
        // 不舍入：舍到 12 位的均价反推数量会带尾巴（某次实盘 67.000000000002 被 Bitget 拒平仓）。
        average_price: notional / quantity,
        fee_usdt: fills.iter().map(|fill| fill.fee_usdt).sum(),
        margin_usdt: None,
        ..first.clone()
    })
}

/// 两条腿实际锁定的价差（%）：(空腿均价 − 多腿均价) / 中间价 × 100。
fn locked_basis_pct(position: &PairPosition) -> Option<Decimal> {
    let (long, short) = (position.long.as_ref()?, position.short.as_ref()?);
    let mid = (long.average_price + short.average_price) / Decimal::TWO;
    (mid > Decimal::ZERO).then(|| {
        ((short.average_price - long.average_price) / mid * Decimal::ONE_HUNDRED).round_dp(4)
    })
}

fn set_leg(position: &mut PairPosition, fill: &LegFill) {
    let mut fill = fill.clone();
    fill.margin_usdt = position
        .leverage
        .filter(|leverage| !position.margin_mode.is_cross() && *leverage > Decimal::ZERO)
        .map(|leverage| fill.notional_usdt / leverage);
    match fill.side {
        Side::Buy => position.long = Some(fill),
        Side::Sell => position.short = Some(fill),
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn side_label(side: Side) -> &'static str {
    match side {
        Side::Buy => "买入",
        Side::Sell => "卖出",
    }
}

/// 平仓后的备注：按实际成交算的已实现盈亏（含全部手续费，不含资金费），以及开、平仓
/// 两次的成交价差 —— 不再用标记价的纸面数。
/// 一笔仓位开过腿的全部场所（按台账历史找）。平仓重试、外部平仓时当前记录里可能只剩一条腿、
/// 甚至一条都不剩；资金费必须按开仓时的两条腿算，否则会漏掉已经平掉那条腿的收付。
pub async fn position_venues(
    ledger: &Ledger,
    position: &PairPosition,
) -> std::io::Result<Vec<Venue>> {
    let mut venues = Vec::new();
    let mut add = |venue: Venue| {
        if !venues.contains(&venue) {
            venues.push(venue);
        }
    };
    for record in ledger.position_history(&position.id).await? {
        for leg in [record.long, record.short].into_iter().flatten() {
            add(leg.venue);
        }
        if let Some(entry) = record.entry_legs {
            add(entry.long.venue);
            add(entry.short.venue);
        }
    }
    for leg in [position.long.as_ref(), position.short.as_ref()]
        .into_iter()
        .flatten()
    {
        add(leg.venue);
    }
    if let Some(entry) = &position.entry_legs {
        add(entry.long.venue);
        add(entry.short.venue);
    }
    Ok(venues)
}

/// `venues` 在 `[from, until]` 内结算的资金费合计。任何一家没连上、没接入或没查到
/// 就返回 `None`：不能拿只查到的一部分冒充合计。少于两家也是 `None`（对冲仓位一定有两条腿）。
pub async fn funding_window(
    brokers: &HashMap<Venue, Arc<dyn Broker>>,
    venues: &[Venue],
    symbol: &arb_core::Symbol,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
) -> Option<Decimal> {
    if venues.len() < 2 {
        return None;
    }
    let mut total = Decimal::ZERO;
    for venue in venues {
        match brokers
            .get(venue)?
            .funding_between(symbol, from, until)
            .await
        {
            Ok(Some(funding)) => total += funding.usdt,
            Ok(None) => return None,
            Err(error) => {
                warn!(%venue, %symbol, "资金费流水没查成：{error}");
                return None;
            }
        }
    }
    Some(total)
}

fn realized_note(
    position: &PairPosition,
    entry_prices: Option<(Decimal, Decimal)>,
    entry_notional: Option<Decimal>,
    exits: &[(Side, Decimal)],
) -> String {
    let funding = position.realized_funding_usdt;
    let net = position.realized_pnl_usdt - position.realized_fee_usdt + funding.unwrap_or_default();
    let mut note = format!("已实现盈亏 {} USDT", net.round_dp(4).normalize());
    if let Some(notional) = entry_notional.filter(|notional| *notional > Decimal::ZERO) {
        note.push_str(&format!(
            "（{}%）",
            (net / notional * Decimal::ONE_HUNDRED)
                .round_dp(4)
                .normalize()
        ));
    }
    note.push_str(&format!(
        "，已扣手续费 {} USDT，{}",
        position.realized_fee_usdt.round_dp(4).normalize(),
        match funding {
            Some(funding) => format!("含资金费 {} USDT", funding.round_dp(4).normalize()),
            None => "不含资金费".to_string(),
        }
    ));
    let exit_price = |side: Side| {
        exits
            .iter()
            .find(|(exit_side, _)| *exit_side == side)
            .map(|(_, price)| *price)
    };
    let basis = |long: Decimal, short: Decimal| {
        let mid = (long + short) / Decimal::TWO;
        (mid > Decimal::ZERO).then(|| ((short - long) / mid * Decimal::ONE_HUNDRED).round_dp(4))
    };
    // 平多腿是卖、平空腿是买。
    if let (Some((entry_long, entry_short)), Some(exit_long), Some(exit_short)) =
        (entry_prices, exit_price(Side::Sell), exit_price(Side::Buy))
        && let (Some(entry), Some(exit)) =
            (basis(entry_long, entry_short), basis(exit_long, exit_short))
    {
        note.push_str(&format!(
            "；成交价差 开仓 {}% → 平仓 {}%",
            entry.normalize(),
            exit.normalize()
        ));
    }
    note
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::PaperBroker;
    use crate::monitor::{Action, evaluate};
    use arb_core::{Level, MarketSnapshot, OrderBook, Symbol};
    use arb_scanner::leverage::{distance_to_liquidation_pct, liquidation_price};
    use arb_venues::VenueApi;
    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    /// 固定价格、深度足够的桩场所：只给盘口，不给批量行情。
    struct FixedBook {
        venue: Venue,
        price: Arc<tokio::sync::Mutex<Decimal>>,
    }

    #[async_trait]
    impl VenueApi for FixedBook {
        fn venue(&self) -> Venue {
            self.venue
        }

        async fn fetch_all(&self) -> ArbResult<Vec<MarketSnapshot>> {
            Ok(Vec::new())
        }

        async fn fetch_depth(&self, symbol: &Symbol, _levels: u32) -> ArbResult<OrderBook> {
            let price = *self.price.lock().await;
            let level = Level {
                price,
                notional_usdt: dec!(1_000_000),
            };
            Ok(OrderBook {
                venue: self.venue,
                symbol: symbol.clone(),
                bids: vec![level.clone()],
                asks: vec![level],
            })
        }
    }

    fn snapshot(venue: Venue, mark: Decimal) -> MarketSnapshot {
        MarketSnapshot {
            venue,
            symbol: Symbol::perp("BTC", "USDT"),
            period_rate: dec!(0.00002),
            interval_h: 1,
            interval_assumed: false,
            next_funding_at: Utc::now(),
            next_funding_estimated: true,
            taker_fee: None,
            mark_price: Some(mark),
            index_price: Some(mark),
            best_bid: None,
            best_ask: None,
            bid_size_usdt: None,
            ask_size_usdt: None,
            open_interest_usdt: None,
            quote_volume_24h: None,
            max_leverage: Some(dec!(50)),
            maintenance_margin: Some(dec!(0.01)),
            oi_capped: false,
        }
    }

    fn leg(venue: Venue, side: Side) -> LegFill {
        LegFill {
            venue,
            side,
            notional_usdt: dec!(1000),
            average_price: dec!(100),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("t1", side, 0),
            margin_usdt: Some(dec!(200)),
        }
    }
    /// 第一次退出：交易所接单、只成交 4 个就剩在簿上，撤单后是部分成交；
    /// 之后的退出按请求数量全部成交。
    struct PartialThenFullBroker {
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    #[async_trait]
    impl Broker for PartialThenFullBroker {
        fn venue(&self) -> Venue {
            Venue::Lighter
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let mut orders = self.orders.lock().await;
            let first = orders.is_empty();
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            state.average_price = Some(dec!(100));
            if first {
                state.status = OrderStatus::Open;
                state.filled_usdt = dec!(400);
            } else {
                state.status = OrderStatus::Filled;
                state.filled_usdt = order.quantity.unwrap() * dec!(100);
            }
            let status = state.status;
            orders.insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, venue_id: &str) -> ArbResult<()> {
            self.orders.lock().await.get_mut(venue_id).unwrap().status = OrderStatus::Cancelled;
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 订单立刻全部成交，但前 `unreadable` 次读订单状态报错 —— 2026-10-15 东京 lighter-rh：订单已成交，
    /// 立刻回读时成交明细没出齐，报「状态不明」。`forget_first_order` 模拟券商从没收到第一张单。
    struct LaggingRead {
        venue: Venue,
        price: Decimal,
        unreadable: std::sync::atomic::AtomicUsize,
        forget_first_order: bool,
        placed: std::sync::atomic::AtomicUsize,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
        /// 场所上 BTC 的净持仓；`None` = 场所上没有持仓。
        net: std::sync::Mutex<Option<Decimal>>,
    }

    impl LaggingRead {
        fn new(
            venue: Venue,
            price: Decimal,
            unreadable: usize,
            forget_first_order: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                venue,
                price,
                unreadable: std::sync::atomic::AtomicUsize::new(unreadable),
                forget_first_order,
                placed: std::sync::atomic::AtomicUsize::new(0),
                orders: Default::default(),
                net: std::sync::Mutex::new(None),
            })
        }
        fn holding(self: Arc<Self>, net: Decimal) -> Arc<Self> {
            *self.net.lock().unwrap() = Some(net);
            self
        }
        async fn order_ids(&self) -> Vec<String> {
            let mut ids: Vec<String> = self.orders.lock().await.keys().cloned().collect();
            ids.sort();
            ids
        }
    }

    #[async_trait]
    impl Broker for LaggingRead {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let nth = self
                .placed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !(self.forget_first_order && nth == 0) {
                let mut state = OrderState::new(order.clone());
                state.venue_order_id = Some(order.client_order_id.0.clone());
                state.status = OrderStatus::Filled;
                state.average_price = Some(self.price);
                state.filled_usdt = order.quantity.unwrap() * self.price;
                self.orders
                    .lock()
                    .await
                    .insert(order.client_order_id.0.clone(), state);
            }
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            let left = self.unreadable.load(std::sync::atomic::Ordering::SeqCst);
            if left > 0 {
                self.unreadable
                    .store(left - 1, std::sync::atomic::Ordering::SeqCst);
                return Err(ArbError::venue(
                    self.venue.as_str(),
                    "fills/trades not yet consistent; refusing invented fee or fill",
                ));
            }
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(self
                .net
                .lock()
                .unwrap()
                .map(|net| crate::broker::VenuePosition {
                    venue: self.venue,
                    symbol: Symbol::perp("BTC", "USDT"),
                    net_quantity: net,
                    average_price: None,
                    notional_usdt: Decimal::ZERO,
                })
                .into_iter()
                .collect())
        }
    }

    fn hedged_position(id: &str) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Hyperliquid, Side::Buy)),
            short: Some(leg(Venue::LighterRh, Side::Sell)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        }
    }

    /// 平仓限价保护的桩券商：带限价的单按 `limit_fill` 的比例成交（0 = 没有对手盘、被拒，同 Hyperliquid 的
    /// 「could not immediately match」；不到 1 = IOC 剩下的撤了），不带限价的（市价）全部成交，都按 `price`、
    /// 每张收 `fee`。`unreadable` 是接下来几次读订单状态报错（下单本身也报错）：模拟结果不明。记下每张单。
    struct GuardBroker {
        venue: Venue,
        price: Decimal,
        fee: Decimal,
        limit_fill: std::sync::Mutex<Decimal>,
        /// 有值时带限价的单按这个数量成交（真实成交都是步长的整数倍），不按 `limit_fill` 的比例。
        limit_quantity: std::sync::Mutex<Option<Decimal>>,
        unreadable: std::sync::atomic::AtomicUsize,
        placed: std::sync::Mutex<Vec<NewOrder>>,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    impl GuardBroker {
        fn new(venue: Venue, price: Decimal, limit_fill: Decimal) -> Arc<Self> {
            Arc::new(Self {
                venue,
                price,
                fee: dec!(0.1),
                limit_fill: std::sync::Mutex::new(limit_fill),
                limit_quantity: std::sync::Mutex::new(None),
                unreadable: std::sync::atomic::AtomicUsize::new(0),
                placed: Default::default(),
                orders: Default::default(),
            })
        }

        fn placed(&self) -> Vec<NewOrder> {
            self.placed.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Broker for GuardBroker {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            self.placed.lock().unwrap().push(order.clone());
            let quantity = order.quantity.unwrap();
            let fill = match order.limit_price {
                Some(_) => *self.limit_fill.lock().unwrap(),
                None => Decimal::ONE,
            };
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            if fill.is_zero() {
                state.status = OrderStatus::Rejected;
                state.reject_reason =
                    Some("Order could not immediately match against any resting orders.".into());
            } else {
                state.status = if fill == Decimal::ONE {
                    OrderStatus::Filled
                } else {
                    OrderStatus::Cancelled
                };
                let filled = match (order.limit_price, *self.limit_quantity.lock().unwrap()) {
                    (Some(_), Some(exact)) => exact,
                    _ => quantity * fill,
                };
                state.average_price = Some(self.price);
                state.filled_usdt = filled * self.price;
                state.fee_usdt = self.fee;
            }
            let status = state.status;
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            if self.unreadable.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                return Err(ArbError::venue(self.venue.as_str(), "下单回包超时"));
            }
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            let left = self.unreadable.load(std::sync::atomic::Ordering::SeqCst);
            if left > 0 {
                self.unreadable
                    .store(left - 1, std::sync::atomic::Ordering::SeqCst);
                return Err(ArbError::venue(self.venue.as_str(), "查询超时"));
            }
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 多腿在 Hyperliquid（入场 100、10 个），先发它的限价卖单（限价 101.5）。
    const GUARD: ExitGuard = ExitGuard {
        side: Side::Sell,
        limit_price: dec!(101.5),
    };

    /// 先发的那条腿一点没成交：另一条腿一张单都不发，两条腿原样、仓位回到 Open，不算失败；
    /// 下一轮再平时换新的订单号，成交了就按市价平另一条腿。
    #[tokio::test]
    async fn a_guarded_exit_that_finds_no_match_leaves_both_legs_and_retries_next_round() {
        let (ledger, path) = ledger_for("guard-held").await;
        let hl = GuardBroker::new(Venue::Hyperliquid, dec!(102), Decimal::ZERO);
        let lighter = GuardBroker::new(Venue::LighterRh, dec!(99), Decimal::ONE);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&hl) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let mut position = hedged_position("guard-0");
        ledger
            .append(&Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        let held = executor.close_guarded(&mut position, GUARD).await.unwrap();
        let GuardedExit::Held(why) = held else {
            panic!("应当继续持有：{held:?}");
        };
        assert!(
            why.contains("没成交") && why.contains("could not immediately match"),
            "{why}"
        );
        assert_eq!(position.status, PositionStatus::Open);
        let untouched = hedged_position("guard-0");
        for (now, before) in [
            (position.long.as_ref(), untouched.long.as_ref()),
            (position.short.as_ref(), untouched.short.as_ref()),
        ] {
            let (now, before) = (now.unwrap(), before.unwrap());
            assert_eq!(
                (now.notional_usdt, now.fee_usdt, now.average_price),
                (before.notional_usdt, before.fee_usdt, before.average_price)
            );
        }
        assert_eq!(position.realized_pnl_usdt, Decimal::ZERO);
        assert!(lighter.placed().is_empty(), "另一条腿一张单都不能发");
        let sent = hl.placed();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].client_order_id.0, "guard-0-sell-exit1");
        assert_eq!(sent[0].limit_price, Some(dec!(101.5)));
        assert_eq!(sent[0].quantity, Some(dec!(10)));
        assert!(sent[0].reduce_only && sent[0].side == Side::Sell);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.positions["guard-0"].status, PositionStatus::Open);
        assert_eq!(
            replayed.orders["guard-0-sell-exit1"].status,
            OrderStatus::Rejected
        );

        // 下一轮：限价内成交了 → 另一条腿按市价平，Closed。价格盈亏只记一次：(102 − 100) × 10 + (100 − 99) × 10。
        *hl.limit_fill.lock().unwrap() = Decimal::ONE;
        let closed = executor.close_guarded(&mut position, GUARD).await.unwrap();
        assert_eq!(closed, GuardedExit::Closed);
        assert_eq!(position.status, PositionStatus::Closed);
        assert_eq!(position.realized_pnl_usdt, dec!(30));
        assert_eq!(position.realized_fee_usdt, dec!(0.2));
        let ids = |broker: &GuardBroker| -> Vec<String> {
            broker
                .placed()
                .into_iter()
                .map(|order| order.client_order_id.0)
                .collect()
        };
        assert_eq!(ids(&hl), ["guard-0-sell-exit1", "guard-0-sell-exit2"]);
        assert_eq!(ids(&lighter), ["guard-0-buy-exit3"]);
        assert_eq!(lighter.placed()[0].limit_price, None, "另一条腿按市价");
        assert!(position.note.as_deref().unwrap().contains("已实现盈亏"));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 先发的那条腿只成交了一部分：它剩下的和另一条腿马上按市价平完；成交的盈亏、手续费都只记一次。
    #[tokio::test]
    async fn a_partly_filled_guarded_leg_is_finished_at_market_together_with_the_other_leg() {
        let (ledger, path) = ledger_for("guard-partial").await;
        let hl = GuardBroker::new(Venue::Hyperliquid, dec!(102), dec!(0.4));
        let lighter = GuardBroker::new(Venue::LighterRh, dec!(99), Decimal::ONE);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&hl) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let mut position = hedged_position("guard-1");
        let closed = executor.close_guarded(&mut position, GUARD).await.unwrap();
        assert_eq!(closed, GuardedExit::Closed);
        assert_eq!(position.status, PositionStatus::Closed);
        assert!(position.long.is_none() && position.short.is_none());
        // 4 个限价 + 6 个市价都在 102：(102 − 100) × 10；空腿 (100 − 99) × 10。三张单各 0.1 手续费。
        assert_eq!(position.realized_pnl_usdt, dec!(30));
        assert_eq!(position.realized_fee_usdt, dec!(0.3));
        let sent = hl.placed();
        assert_eq!(sent.len(), 2);
        assert_eq!(
            (sent[0].limit_price, sent[0].quantity),
            (Some(dec!(101.5)), Some(dec!(10)))
        );
        assert_eq!(
            (sent[1].limit_price, sent[1].quantity),
            (None, Some(dec!(6)))
        );
        assert_eq!(lighter.placed().len(), 1);
        assert_eq!(lighter.placed()[0].quantity, Some(dec!(10)));
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.positions["guard-1"].status, PositionStatus::Closed);
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 部分成交后剩下的数量必须**正好**是步长的整数倍：Hyperliquid 的 reduce-only 数量不精确就拒单
    /// （`order_quantity`：不能按步长表示），那时另一条腿已经按市价平了，HL 剩一截裸腿。腿上记的是名义和均价，
    /// 剩下的数量是 名义 × (1 − 成交 ÷ 原数量) ÷ 均价 算回来的：用真实量级的数（均价除不尽）核对它没有尾巴。
    #[tokio::test]
    async fn the_remainder_after_a_partial_guarded_fill_is_an_exact_lot_multiple() {
        let (ledger, path) = ledger_for("guard-lot").await;
        let hl = GuardBroker::new(Venue::Hyperliquid, dec!(23.61), dec!(0.5));
        *hl.limit_quantity.lock().unwrap() = Some(dec!(6.8));
        let lighter = GuardBroker::new(Venue::LighterRh, dec!(23.5), Decimal::ONE);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&hl) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let mut position = hedged_position("guard-lot");
        // 17.02 个：多腿均价 23.4567（名义 399.233034），空腿 23.5（名义 399.97）。
        position.long.as_mut().unwrap().notional_usdt = dec!(399.233034);
        position.long.as_mut().unwrap().average_price = dec!(23.4567);
        position.short.as_mut().unwrap().notional_usdt = dec!(399.97);
        position.short.as_mut().unwrap().average_price = dec!(23.5);
        let guard = ExitGuard {
            side: Side::Sell,
            limit_price: dec!(23.6),
        };
        let closed = executor.close_guarded(&mut position, guard).await.unwrap();
        assert_eq!(closed, GuardedExit::Closed);
        let sent = hl.placed();
        assert_eq!(sent[0].quantity, Some(dec!(17.02)));
        assert_eq!(
            sent[1].quantity,
            Some(dec!(10.22)),
            "剩下的数量不能带除法尾巴"
        );
        assert!(sent[1].quantity.unwrap().scale() <= 2);
        assert_eq!(lighter.placed()[0].quantity, Some(dec!(17.02)));
        // (23.61 − 23.4567) × 17.02 + (23.5 − 23.5) × 17.02
        assert_eq!(position.realized_pnl_usdt.round_dp(10), dec!(2.609166));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 同上，扫一遍各种量级（均价除不尽、成交价也除不尽）：部分成交入账后，`legs_of` 给出的剩余数量都正好是
    /// 原数量减成交数量。
    #[test]
    fn partial_exit_bookkeeping_leaves_an_exact_remaining_quantity() {
        let cases = [
            (dec!(17.02), dec!(23.4567), dec!(6.8), dec!(23.61)),
            (dec!(0.4911), dec!(399.8981), dec!(0.2), dec!(401.37)),
            (dec!(3), dec!(1234.5678), dec!(1), dec!(1240.1234)),
            (dec!(123.4), dec!(7.77777), dec!(45.6), dec!(7.81239)),
            (dec!(0.00123), dec!(97123.45), dec!(0.0005), dec!(97500.5)),
            (dec!(2537), dec!(0.0157933), dec!(1012), dec!(0.0161117)),
        ];
        for (quantity, entry, filled, exit) in cases {
            let mut position = hedged_position("lot");
            let long = position.long.as_mut().unwrap();
            long.average_price = entry;
            long.notional_usdt = quantity * entry;
            let mut state = OrderState::new(NewOrder {
                post_only: false,
                client_order_id: ClientOrderId::for_exit("lot", Side::Sell, 1),
                venue: Venue::Hyperliquid,
                symbol: position.symbol.clone(),
                side: Side::Sell,
                notional_usdt: quantity * entry,
                quantity: Some(quantity),
                limit_price: Some(exit),
                reduce_only: true,
                leverage: None,
                margin_mode: position.margin_mode,
            });
            state.status = OrderStatus::Cancelled;
            state.average_price = Some(exit);
            state.filled_usdt = filled * exit;
            let mut exits = Vec::new();
            assert!(!apply_exit_state(
                &mut position,
                Side::Sell,
                quantity,
                &state,
                &mut exits
            ));
            let remaining = legs_of(&position)
                .unwrap()
                .into_iter()
                .find(|leg| leg.1 == Side::Sell)
                .unwrap()
                .3;
            assert_eq!(
                remaining,
                quantity - filled,
                "{quantity} @ {entry}，成交 {filled} @ {exit}"
            );
        }
    }

    /// 先发的那条腿结果不明：留在 Closing（绝不回到 Open），另一条腿不动；重试先读回它的结局再平另一条，不重发。
    #[tokio::test]
    async fn an_unknown_guarded_leg_stays_closing_and_the_retry_reads_it_back() {
        let (ledger, path) = ledger_for("guard-unknown").await;
        let hl = GuardBroker::new(Venue::Hyperliquid, dec!(102), Decimal::ONE);
        hl.unreadable
            .store(READS_PER_ATTEMPT, std::sync::atomic::Ordering::SeqCst);
        let lighter = GuardBroker::new(Venue::LighterRh, dec!(99), Decimal::ONE);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&hl) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let mut position = hedged_position("guard-2");
        let error = executor
            .close_guarded(&mut position, GUARD)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("结果不明") && error.contains("Closing"),
            "{error}"
        );
        assert_eq!(position.status, PositionStatus::Closing);
        assert!(position.long.is_some() && position.short.is_some());
        assert!(lighter.placed().is_empty());
        // 重试走普通平仓：读回 exit1（已成交）入账，不重发；再平另一条腿。
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert_eq!(hl.placed().len(), 1, "不能换号重发");
        assert_eq!(lighter.placed().len(), 1);
        assert_eq!(position.realized_pnl_usdt, dec!(30));
        // 重试中的仓位再走保护路径：退回普通平仓（这里已经 Closed，不会再发单）。
        tokio::fs::remove_file(&path).await.unwrap();
    }

    async fn ledger_for(name: &str) -> (Arc<Ledger>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("arb-{name}-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        (Arc::new(Ledger::open(&path).await.unwrap()), path)
    }

    /// 一次平仓里，一张单最多读几次（下单后读一次 + 重读）。
    const READS_PER_ATTEMPT: usize = 1 + READ_SETTLE_BACKOFF.len();

    /// 只是短暂读不到（查询还没跟上）：同一次平仓里重读到就入账，不留在 Closing。
    #[tokio::test]
    async fn an_exit_that_is_briefly_unreadable_is_settled_within_the_same_close() {
        let (ledger, path) = ledger_for("heal-brief").await;
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), 2, false);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![lighter.clone() as Arc<dyn Broker>],
        );
        let mut position = hedged_position("heal-0");
        position.long = None;
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert_eq!(lighter.order_ids().await, ["heal-0-buy-exit1"]);
        assert_eq!(position.realized_pnl_usdt, dec!(10));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 某合约 / 某合约：一条腿正常平掉，另一条腿的买回单已成交、但这一次平仓里怎么读都读不到。
    /// 重试必须先读回那张单的结局并入账，**不能**换新号再发（reduce-only 会被拒，仓位永远卡在 Closing）。
    #[tokio::test]
    async fn a_retry_reads_back_an_unreadable_exit_instead_of_resending_it() {
        let (ledger, path) = ledger_for("heal-readback").await;
        let hl = LaggingRead::new(Venue::Hyperliquid, dec!(101), 0, false);
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), READS_PER_ATTEMPT, false);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                hl.clone() as Arc<dyn Broker>,
                lighter.clone() as Arc<dyn Broker>,
            ],
        );
        let mut position = hedged_position("heal-1");

        // 第一次：hyperliquid 腿平掉了，lighter-rh 腿状态不明 → 保留 Closing。
        let error = executor.close(&mut position).await.unwrap_err().to_string();
        assert!(error.contains("退出状态不明"), "{error}");
        assert_eq!(position.status, PositionStatus::Closing);
        assert!(position.long.is_none(), "hyperliquid 那条腿已经平掉");
        assert!(
            position.short.is_some(),
            "lighter-rh 那条腿的结果还没读回来"
        );
        // 台账里那张单还停在 Pending —— 这正是对账里 OrderMissingOnVenue 的来源。
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders["heal-1-buy-exit1"].status.is_live());

        // 重试：读回 exit1（已成交），入账，不发新单；两条腿都平了 → Closed。
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert!(position.long.is_none() && position.short.is_none());
        assert_eq!(
            lighter.order_ids().await,
            ["heal-1-buy-exit1"],
            "不能换号重发"
        );
        assert_eq!(hl.order_ids().await, ["heal-1-sell-exit1"]);
        // 价格盈亏：多腿 100→101 卖出 10 个 = +10；空腿 100→99 买回 10 个 = +10。
        assert_eq!(position.realized_pnl_usdt, dec!(20));
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(
            replayed
                .orders
                .values()
                .all(|state| !state.status.is_live()),
            "没有遗留未落终态的订单"
        );
        assert_eq!(replayed.positions["heal-1"].status, PositionStatus::Closed);
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 读回仍然失败：这条腿本轮不重发（重发可能重复平仓），下一轮读回了再入账。
    #[tokio::test]
    async fn an_exit_that_is_still_unreadable_is_not_resent_until_it_can_be_read() {
        let (ledger, path) = ledger_for("heal-blocked").await;
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), READS_PER_ATTEMPT + 1, false);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![lighter.clone() as Arc<dyn Broker>],
        );
        let mut position = hedged_position("heal-2");
        position.long = None;

        assert!(executor.close(&mut position).await.is_err());
        // 第一次读回也失败：不发新单。
        let error = executor.close(&mut position).await.unwrap_err().to_string();
        assert!(error.contains("本轮不重发"), "{error}");
        assert_eq!(position.status, PositionStatus::Closing);
        assert_eq!(lighter.order_ids().await, ["heal-2-buy-exit1"]);
        // 第二次读回成功：入账、Closed，仍然没有新单。
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert_eq!(lighter.order_ids().await, ["heal-2-buy-exit1"]);
        assert_eq!(position.realized_pnl_usdt, dec!(10));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 券商明确没收到旧单：记 Rejected，照常用新号重发，把这条腿平掉。
    #[tokio::test]
    async fn an_exit_the_venue_never_received_is_rejected_and_sent_again() {
        let (ledger, path) = ledger_for("heal-never-received").await;
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), 1, true);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![lighter.clone() as Arc<dyn Broker>],
        );
        let mut position = hedged_position("heal-3");
        position.long = None;

        assert!(executor.close(&mut position).await.is_err());
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        // 只有第二张单在券商那边；第一张在台账里被记为 Rejected 而不是一直停在 Pending。
        assert_eq!(lighter.order_ids().await, ["heal-3-buy-exit2"]);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["heal-3-buy-exit1"].status,
            OrderStatus::Rejected
        );
        assert_eq!(
            replayed.orders["heal-3-buy-exit2"].status,
            OrderStatus::Filled
        );
        assert_eq!(position.realized_pnl_usdt, dec!(10));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 某次实盘：退出单读不到（场所订单历史里没有），但场所净持仓和台账一致 = 没动过持仓：
    /// 记 Rejected，用新号重发，把这条腿平掉；不再一直卡在「本轮不重发」。
    #[tokio::test]
    async fn an_unreadable_exit_is_resent_when_the_venue_position_never_moved() {
        let (ledger, path) = ledger_for("heal-flat-evidence").await;
        // 第一张退出单从没到过场所（读回报错），场所上空腿 −10 个原封不动。
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), READS_PER_ATTEMPT + 1, true)
            .holding(dec!(-10));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![lighter.clone() as Arc<dyn Broker>],
        );
        let mut position = hedged_position("heal-4");
        position.long = None;

        assert!(executor.close(&mut position).await.is_err());
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert_eq!(lighter.order_ids().await, ["heal-4-buy-exit2"]);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["heal-4-buy-exit1"];
        assert_eq!(first.status, OrderStatus::Rejected);
        assert!(
            first
                .reject_reason
                .as_deref()
                .unwrap()
                .contains("净持仓与台账一致")
        );
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 读不到、而且场所净持仓变了（可能已成交 / 部分成交）：不重发，交对账与人工。
    #[tokio::test]
    async fn an_unreadable_exit_is_not_resent_when_the_venue_position_changed() {
        let (ledger, path) = ledger_for("heal-moved-evidence").await;
        // 场所上空腿已经是 0（退出单可能已经成交）。
        let lighter = LaggingRead::new(Venue::LighterRh, dec!(99), READS_PER_ATTEMPT + 1, true)
            .holding(dec!(0));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![lighter.clone() as Arc<dyn Broker>],
        );
        let mut position = hedged_position("heal-5");
        position.long = None;

        assert!(executor.close(&mut position).await.is_err());
        let error = executor.close(&mut position).await.unwrap_err().to_string();
        assert!(
            error.contains("不一致") && error.contains("本轮不重发"),
            "{error}"
        );
        assert_eq!(position.status, PositionStatus::Closing);
        assert!(lighter.order_ids().await.is_empty());
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["heal-5-buy-exit1"].status,
            OrderStatus::Pending
        );
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 一条腿在交易所被强平、看板只平剩下那条（[`crate::desk::adopt_lone_legs`]）：平完记 `Closed`，但实际盈亏
    /// 不当场定（只有剩下那条的成交），留给事后按交易所成交记录核算整笔。
    #[tokio::test]
    async fn closing_the_leg_left_after_a_liquidation_leaves_the_pnl_to_the_venue_fills() {
        let (ledger, path) = ledger_for("lone-close").await;
        let lighter = Arc::new(FillAt {
            venue: Venue::LighterRh,
            price: dec!(99),
            fee: dec!(0.01),
            orders: Default::default(),
        });
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![Arc::clone(&lighter) as Arc<dyn Broker>],
        );
        let mut position = hedged_position("lone-1");
        position.entry_legs = position
            .long
            .clone()
            .zip(position.short.clone())
            .map(|(long, short)| crate::EntryLegs { long, short });
        position.closed_externally = true;
        position.long = None;
        position.status = PositionStatus::Closing;
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert!(position.short.is_none());
        assert_eq!(
            position.realized_source, None,
            "只平了一条腿：整笔盈亏要按交易所成交记录核"
        );
        let note = position.note.as_deref().unwrap();
        assert!(
            !note.contains("已实现盈亏") && note.contains("稍后按交易所成交记录核算"),
            "推送里不能出现只算一条腿的盈亏：{note}"
        );
        let placed = lighter.orders.lock().await;
        assert_eq!(placed.len(), 1);
        let order = placed.values().next().unwrap();
        assert!(order.order.reduce_only && order.order.side == Side::Buy);
        drop(placed);
        // 看板自己平的照旧当场定。
        let mut normal = hedged_position("lone-2");
        normal.long = None;
        normal.status = PositionStatus::Closing;
        executor.close(&mut normal).await.unwrap();
        assert_eq!(normal.realized_source, Some(RealizedSource::Executor));
        tokio::fs::remove_file(&path).await.unwrap();
    }

    /// 每笔单都按固定价全部成交、收固定手续费的桩券商。
    struct FillAt {
        venue: Venue,
        price: Decimal,
        fee: Decimal,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    #[async_trait]
    impl Broker for FillAt {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            state.status = OrderStatus::Filled;
            state.average_price = Some(self.price);
            state.filled_usdt = order.quantity.unwrap() * self.price;
            state.fee_usdt = self.fee;
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 一家休市（[`Broker::trading_halted`]）的场所：其余照 [`FillAt`]。
    struct Halted(FillAt);

    #[async_trait]
    impl Broker for Halted {
        fn venue(&self) -> Venue {
            self.0.venue()
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn trading_halted(&self, _: &Symbol, _: bool) -> ArbResult<Option<String>> {
            Ok(Some("休市".into()))
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            self.0.place(order).await
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            self.0.order_state(id).await
        }
        async fn cancel(&self, venue_id: &str) -> ArbResult<()> {
            self.0.cancel(venue_id).await
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 有一家休市：平仓、减仓一单都不发（另一家照常能下也不动），仓位原样留着，不进 `Closing`。
    #[tokio::test]
    async fn a_position_with_a_halted_leg_is_neither_closed_nor_trimmed() {
        let path = std::env::temp_dir().join(format!("arb-halted-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let open = Arc::new(FillAt {
            venue: Venue::HyperliquidXyz,
            price: dec!(100),
            fee: Decimal::ZERO,
            orders: Default::default(),
        });
        let halted = Arc::new(Halted(FillAt {
            venue: Venue::Bitget,
            price: dec!(100),
            fee: Decimal::ZERO,
            orders: Default::default(),
        }));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&open) as Arc<dyn Broker>,
                Arc::clone(&halted) as Arc<dyn Broker>,
            ],
        );
        let mut position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "soxl".into(),
            symbol: Symbol::perp("SOXL", "USDT"),
            strategy: Strategy::Spread,
            long: Some(LegFill {
                venue: Venue::HyperliquidXyz,
                ..leg(Venue::HyperliquidXyz, Side::Buy)
            }),
            short: Some(LegFill {
                venue: Venue::Bitget,
                ..leg(Venue::Bitget, Side::Sell)
            }),
            entry_basis_pct: dec!(0.3),
            expected_round_trip_cost: dec!(0.002),
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        let reason = executor.exit_halted(&position).await.unwrap();
        assert!(
            reason.contains("bitget") && reason.contains("休市"),
            "{reason}"
        );
        let error = executor.close(&mut position).await.unwrap_err().to_string();
        assert!(error.contains("休市"), "{error}");
        assert_eq!(position.status, PositionStatus::Open);
        assert!(
            executor
                .trim(&mut position, dec!(0.5), dec!(100), dec!(100), "保护")
                .await
                .is_err()
        );
        assert!(open.orders.lock().await.is_empty(), "能下单的那家也没动");
        assert!(halted.0.orders.lock().await.is_empty());
        // 只剩能下单的那条腿（Bitget 那条已经平掉）：照常平。
        position.short = None;
        assert!(executor.exit_halted(&position).await.is_none());
        let _ = tokio::fs::remove_file(&path).await;
    }

    /// 一组实盘的四笔成交（取整后用作夹具）：按实际成交记账应是 −1.2949 USDT，
    /// 备注里写开、平仓两次的成交价差（0.1137% → 0.1961%）。
    #[tokio::test]
    async fn closing_records_realized_pnl_from_the_actual_fills() {
        let path = std::env::temp_dir().join(format!("arb-realized-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let fill = |venue, price, fee| {
            Arc::new(FillAt {
                venue,
                price,
                fee,
                orders: Default::default(),
            }) as Arc<dyn Broker>
        };
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                fill(
                    Venue::LighterRh,
                    dec!(4.9612594058918998672109378842),
                    Decimal::ZERO,
                ),
                fill(Venue::Arcus, dec!(4.971), dec!(0.227424765)),
            ],
        );
        let mut position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "near".into(),
            symbol: Symbol::perp("NEAR", "USDT"),
            strategy: Strategy::Spread,
            long: Some(LegFill {
                venue: Venue::LighterRh,
                notional_usdt: dec!(1000.464932),
                average_price: dec!(4.9204),
                fee_usdt: Decimal::ZERO,
                ..leg(Venue::LighterRh, Side::Buy)
            }),
            short: Some(LegFill {
                venue: Venue::Arcus,
                notional_usdt: dec!(1001.626673088),
                average_price: dec!(4.926),
                fee_usdt: dec!(0.225375889),
                ..leg(Venue::Arcus, Side::Sell)
            }),
            entry_basis_pct: dec!(0.1442),
            expected_round_trip_cost: dec!(0.000535),
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        let net = position.realized_pnl_usdt - position.realized_fee_usdt;
        assert!((net - dec!(-1.2949)).abs() < dec!(0.0001), "{net}");
        assert_eq!(position.realized_fee_usdt, dec!(0.452800654));
        let note = position.note.clone().unwrap();
        assert!(note.contains("已实现盈亏 -1.29"), "{note}");
        assert_eq!(position.realized_source, Some(RealizedSource::Executor));
        assert!(
            note.contains("0.1137%") && note.contains("0.1961%"),
            "{note}"
        );
        // 台账回放得到同一份记账。
        let (replayed, _) = ledger.replay().await.unwrap();
        let stored = &replayed.positions["near"];
        assert_eq!(stored.realized_fee_usdt, position.realized_fee_usdt);
        let _ = tokio::fs::remove_file(&path).await;
    }

    /// 「最近结束」的双边明细从台账订单还原（[`crate::breakdown`]）：开仓、减仓、平仓各走一遍，
    /// 两腿之和必须等于执行器自己记的已实现价格盈亏与手续费。
    #[tokio::test]
    async fn the_closed_breakdown_adds_up_to_what_the_executor_booked() {
        let path = std::env::temp_dir().join(format!("arb-breakdown-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let fill = |venue, price, fee| {
            Arc::new(FillAt {
                venue,
                price,
                fee,
                orders: Default::default(),
            }) as Arc<dyn Broker>
        };
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                fill(
                    Venue::LighterRh,
                    dec!(4.9612594058918998672109378842),
                    Decimal::ZERO,
                ),
                fill(Venue::Arcus, dec!(4.971), dec!(0.227424765)),
            ],
        );
        let symbol = Symbol::perp("NEAR", "USDT");
        let long = LegFill {
            notional_usdt: dec!(1000.464932),
            average_price: dec!(4.9204),
            client_order_id: ClientOrderId::for_leg("bd", Side::Buy, 0),
            ..leg(Venue::LighterRh, Side::Buy)
        };
        let short = LegFill {
            notional_usdt: dec!(1001.626673088),
            average_price: dec!(4.926),
            fee_usdt: dec!(0.225375889),
            client_order_id: ClientOrderId::for_leg("bd", Side::Sell, 0),
            ..leg(Venue::Arcus, Side::Sell)
        };
        // 开仓单照执行器的样子落盘：腿上的数就来自这两张单的成交。
        for leg in [&long, &short] {
            let mut state = OrderState::new(NewOrder {
                post_only: false,
                client_order_id: leg.client_order_id.clone(),
                venue: leg.venue,
                symbol: symbol.clone(),
                side: leg.side,
                notional_usdt: leg.notional_usdt,
                quantity: None,
                limit_price: None,
                reduce_only: false,
                leverage: Some(dec!(3)),
                margin_mode: crate::MarginMode::Isolated,
            });
            state.status = OrderStatus::Filled;
            state.filled_usdt = leg.notional_usdt;
            state.average_price = Some(leg.average_price);
            state.fee_usdt = leg.fee_usdt;
            ledger
                .append(&crate::Record::Order(Box::new(state)))
                .await
                .unwrap();
        }
        let mut position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "bd".into(),
            symbol,
            strategy: Strategy::Spread,
            long: Some(long),
            short: Some(short),
            entry_basis_pct: dec!(0.1442),
            expected_round_trip_cost: dec!(0.000535),
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        executor
            .trim(
                &mut position,
                dec!(0.25),
                dec!(4.95),
                dec!(4.95),
                "测试减仓",
            )
            .await
            .unwrap();
        executor.close(&mut position).await.unwrap();

        let (replayed, _) = ledger.replay().await.unwrap();
        let stored = &replayed.positions["bd"];
        assert_eq!(stored.status, PositionStatus::Closed);
        let breakdown = crate::breakdown::position_breakdown(stored, &replayed.orders).unwrap();
        assert!((breakdown.price_pnl_usdt - stored.realized_pnl_usdt).abs() < dec!(1e-12));
        assert!((breakdown.fee_usdt - stored.realized_fee_usdt).abs() < dec!(1e-12));
        assert_eq!(breakdown.pnl_gap_usdt, None);
        assert_eq!(breakdown.fee_gap_usdt, None);
        assert!(!breakdown.external);

        let long = breakdown.long.unwrap();
        assert_eq!((long.venue, long.side), (Venue::LighterRh, Side::Buy));
        assert_eq!(long.entry_price, Some(dec!(4.9204)));
        let roles: Vec<_> = long.exits.iter().map(|exit| exit.role).collect();
        assert_eq!(
            roles,
            [
                crate::types::OrderRole::Trim { n: 1 },
                crate::types::OrderRole::Exit { n: 1 }
            ]
        );
        assert!((long.exited_quantity - long.entry_quantity).abs() < dec!(1e-9));
        // 「平仓对账」要的是最后那张平仓单的实际成交，不是减仓那张：数量是减仓后剩下的，不是全部。
        let fills = executor
            .last_exit_fills(&position)
            .await
            .expect("平仓单有成交");
        let exit = long
            .exits
            .iter()
            .find(|exit| exit.role == crate::types::OrderRole::Exit { n: 1 })
            .unwrap();
        let trim = long
            .exits
            .iter()
            .find(|exit| exit.role == crate::types::OrderRole::Trim { n: 1 })
            .unwrap();
        let long_fill = fills.long.expect("多腿平仓成交");
        assert_eq!(
            (long_fill.price, long_fill.quantity, long_fill.fee_usdt),
            (exit.average_price, exit.quantity, exit.fee_usdt)
        );
        assert!(long_fill.quantity > trim.quantity, "不是减仓那一张");
        assert!(fills.short.is_some());
        let short = breakdown.short.unwrap();
        assert_eq!((short.venue, short.side), (Venue::Arcus, Side::Sell));
        // 开仓 + 减仓 + 平仓三笔手续费。
        assert_eq!(
            short.fee_usdt,
            dec!(0.225375889) + dec!(0.227424765) + dec!(0.227424765)
        );
        // 空腿 4.926 开、4.971 平：亏。
        assert!(short.price_pnl_usdt < Decimal::ZERO);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn a_partial_exit_stays_closing_and_the_retry_closes_only_the_remainder() {
        let path = std::env::temp_dir().join(format!("arb-exit-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let broker = Arc::new(PartialThenFullBroker {
            orders: Default::default(),
        });
        let executor = Executor::new(Arc::clone(&ledger), vec![broker.clone() as Arc<dyn Broker>]);
        let mut position = PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: "exit-1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };

        // 10 个只卖出 4 个：不能标 Closed，剩下 6 个留在仓位上。
        assert!(executor.close(&mut position).await.is_err());
        assert_eq!(position.status, PositionStatus::Closing);
        assert!(position.closed_at.is_none());
        assert_eq!(position.long.as_ref().unwrap().quantity(), Some(dec!(6)));

        // 重试换新号、只卖剩下的 6 个。
        executor.close(&mut position).await.unwrap();
        assert_eq!(position.status, PositionStatus::Closed);
        assert!(position.long.is_none());

        let orders = broker.orders.lock().await;
        assert_eq!(orders["exit-1-sell-exit1"].status, OrderStatus::Cancelled);
        assert_eq!(orders["exit-1-sell-exit2"].order.quantity, Some(dec!(6)));
        assert!(orders.values().all(|state| state.order.reduce_only));
        drop(orders);
        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        assert_eq!(replayed.positions["exit-1"].status, PositionStatus::Closed);
        // 明细：部分成交后撤掉的那张也算，两张平仓单合起来正好是整条腿。台账里没有开仓单
        // （这笔是直接构造的），开仓均价就是不知道，不拿 0 去算盈亏。
        let breakdown =
            crate::breakdown::position_breakdown(&replayed.positions["exit-1"], &replayed.orders)
                .unwrap();
        let long = breakdown.long.unwrap();
        assert_eq!(long.exits.len(), 2);
        assert_eq!(long.exited_quantity, dec!(10));
        assert_eq!(long.entry_price, None);
        assert!(long.exits.iter().all(|exit| exit.price_pnl_usdt.is_none()));
        assert!(breakdown.short.is_none());
        tokio::fs::remove_file(&path).await.unwrap();
    }

    #[tokio::test]
    async fn an_undo_is_floored_to_the_venue_step_or_not_sent() {
        let dir = std::env::temp_dir().join(format!("arb-undo-step-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let price = Arc::new(tokio::sync::Mutex::new(dec!(100)));
        let api: Arc<dyn VenueApi> = Arc::new(FixedBook {
            venue: Venue::Lighter,
            price: Arc::clone(&price),
        });
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let lighter = Stepped::wrap(
            Arc::new(PaperBroker::new(api, Decimal::ZERO, 5)),
            Some(dec!(0.1)),
            0,
            &log,
        );
        lighter
            .place(&NewOrder {
                post_only: false,
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId("seed-lighter".into()),
                venue: Venue::Lighter,
                symbol: Symbol::perp("BTC", "USDT"),
                side: Side::Buy,
                notional_usdt: dec!(1000),
                quantity: Some(dec!(10)),
                limit_price: None,
                reduce_only: false,
                leverage: Some(dec!(5)),
            })
            .await
            .unwrap();
        let executor = Executor::new(Arc::clone(&ledger), vec![lighter]);
        let mut position = PairPosition {
            id: "t-undo".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Spread,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Opening,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            margin_mode: crate::MarginMode::Isolated,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        // 多出 0.25 个、这一家步长 0.1：退 0.2，尾数 0.05 留给残量判断。
        let undone = executor
            .undo_excess(&mut position, Side::Buy, dec!(0.25), 0)
            .await
            .unwrap();
        assert_eq!(undone, dec!(0.2));
        // 不到一步：不发单。
        let undone = executor
            .undo_excess(&mut position, Side::Buy, dec!(0.05), 1)
            .await
            .unwrap();
        assert_eq!(undone, Decimal::ZERO);
    }

    #[tokio::test]
    async fn an_opening_position_with_an_unsettled_entry_order_is_not_closed_by_hand() {
        let ledger = temp_ledger("opening-close").await;
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let lighter = Hedging::new(Venue::LighterRh, dec!(5.36), Decimal::ZERO, &log);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![Arc::new(lighter) as Arc<dyn Broker>],
        );
        // 第一腿已成交记在仓位上；第二腿的开仓单还是 Pending（结果不明）。
        let mut position = PairPosition {
            id: "t-opening".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Spread,
            long: Some(leg(Venue::LighterRh, Side::Buy)),
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Opening,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            margin_mode: crate::MarginMode::Isolated,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        let pending = OrderState::new(NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId::for_leg("t-opening", Side::Sell, 0),
            venue: Venue::Hyperliquid,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Sell,
            notional_usdt: dec!(1000),
            quantity: Some(dec!(10)),
            limit_price: None,
            reduce_only: false,
            leverage: Some(dec!(3)),
        });
        executor.persist_order(&pending).await.unwrap();
        let error = executor.close(&mut position).await.unwrap_err().to_string();
        assert!(
            error.contains("开仓单") && error.contains("核实"),
            "{error}"
        );
        assert_eq!(position.status, PositionStatus::Opening);
        assert!(position.long.is_some(), "台账里的腿没动");
        assert!(
            log.lock().unwrap().is_empty(),
            "一单都没发：{:?}",
            log.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn a_rollback_finished_on_retry_stays_unwound_not_closed() {
        let dir = std::env::temp_dir().join(format!("arb-unwind-retry-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let price = Arc::new(tokio::sync::Mutex::new(dec!(100)));
        let api: Arc<dyn VenueApi> = Arc::new(FixedBook {
            venue: Venue::Lighter,
            price: Arc::clone(&price),
        });
        let lighter: Arc<dyn Broker> = Arc::new(PaperBroker::new(api, Decimal::ZERO, 5));
        lighter
            .place(&NewOrder {
                post_only: false,
                margin_mode: crate::MarginMode::Isolated,
                client_order_id: ClientOrderId("seed-lighter".into()),
                venue: Venue::Lighter,
                symbol: Symbol::perp("BTC", "USDT"),
                side: Side::Buy,
                notional_usdt: dec!(1000),
                quantity: Some(dec!(10)),
                limit_price: None,
                reduce_only: false,
                leverage: Some(dec!(5)),
            })
            .await
            .unwrap();
        let executor = Executor::new(Arc::clone(&ledger), vec![lighter]);
        // 第二腿没成交、第一腿回滚上一轮没平成：仓位停在 Unwinding，只剩第一腿。
        let mut position = PairPosition {
            id: "t-unwind".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Spread,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Unwinding,
            opened_at: Utc::now(),
            closed_at: None,
            note: Some("回滚未完全成功，仍有敞口".into()),
            leverage: Some(dec!(5)),
            margin_mode: crate::MarginMode::Isolated,
            rules: TaskRules::default(),
            trims: 0,
            exits: 1,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };
        executor.close(&mut position).await.unwrap();
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(position.long.is_none() && position.short.is_none());
        assert!(position.closed_at.is_some());
        assert!(
            position.note.as_deref().unwrap().contains("回滚"),
            "{:?}",
            position.note
        );
    }

    #[tokio::test]
    async fn a_protection_trim_rounds_both_legs_to_the_venue_step_or_sends_nothing() {
        for (steps, expect) in [
            // 粗步长 0.1 是细步长 0.01 的整数倍：两腿按 0.1 减同一个数。
            ([Some(dec!(0.01)), Some(dec!(0.1))], Some(dec!(0.1))),
            // 步长比要减的量还大：一单不发、报错，减仓次数不加。
            ([Some(dec!(100)), Some(dec!(0.01))], None),
        ] {
            let dir = std::env::temp_dir().join(format!(
                "arb-trim-step-{}-{}",
                std::process::id(),
                steps[0].unwrap()
            ));
            let path = dir.join("ledger.jsonl");
            let _ = tokio::fs::remove_file(&path).await;
            let ledger = Arc::new(Ledger::open(&path).await.unwrap());
            let price = Arc::new(tokio::sync::Mutex::new(dec!(100)));
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let brokers: Vec<Arc<dyn Broker>> = [Venue::Lighter, Venue::Hyperliquid]
                .into_iter()
                .zip(steps)
                .map(|(venue, step)| {
                    let api: Arc<dyn VenueApi> = Arc::new(FixedBook {
                        venue,
                        price: Arc::clone(&price),
                    });
                    Stepped::wrap(
                        Arc::new(PaperBroker::new(api, Decimal::ZERO, 5)),
                        step,
                        0,
                        &log,
                    )
                })
                .collect();
            for broker in &brokers {
                let side = if broker.venue() == Venue::Lighter {
                    Side::Buy
                } else {
                    Side::Sell
                };
                broker
                    .place(&NewOrder {
                        post_only: false,
                        margin_mode: crate::MarginMode::Isolated,
                        client_order_id: ClientOrderId(format!("seed-{}", broker.venue())),
                        venue: broker.venue(),
                        symbol: Symbol::perp("BTC", "USDT"),
                        side,
                        notional_usdt: dec!(1000),
                        quantity: Some(dec!(10)),
                        limit_price: None,
                        reduce_only: false,
                        leverage: Some(dec!(5)),
                    })
                    .await
                    .unwrap();
            }
            let executor = Executor::new(Arc::clone(&ledger), brokers);
            let mut position = PairPosition {
                id: "t-step".into(),
                symbol: Symbol::perp("BTC", "USDT"),
                strategy: Strategy::Funding,
                long: Some(leg(Venue::Lighter, Side::Buy)),
                short: Some(leg(Venue::Hyperliquid, Side::Sell)),
                entry_basis_pct: Decimal::ZERO,
                expected_round_trip_cost: Decimal::ZERO,
                status: PositionStatus::Open,
                opened_at: Utc::now(),
                closed_at: None,
                note: None,
                leverage: Some(dec!(5)),
                margin_mode: crate::MarginMode::Isolated,
                rules: TaskRules::default(),
                trims: 0,
                exits: 0,
                margin_added_usdt: Decimal::ZERO,
                realized_pnl_usdt: Decimal::ZERO,
                realized_fee_usdt: Decimal::ZERO,
                realized_source: None,
                realized_funding_usdt: None,
                funding_checked_at: None,
                closed_externally: false,
                entry_legs: None,
                pnl_unattributed: None,
                open_report: None,
                planned: None,
            };
            // 监控算出来的比例带 28 位小数：0.2345… × 10 = 2.345… 个。
            let fraction = dec!(0.2345678901234567890123456789);
            let before = position.long.as_ref().unwrap().quantity().unwrap();
            let result = executor
                .trim(&mut position, fraction, dec!(100), dec!(100), "测试")
                .await;
            match expect {
                Some(step) => {
                    result.unwrap();
                    assert_eq!(position.trims, 1);
                    let long = position.long.as_ref().unwrap().quantity().unwrap();
                    let short = position.short.as_ref().unwrap().quantity().unwrap();
                    let cut = before - long;
                    assert!(cut > Decimal::ZERO && (cut % step).is_zero(), "减了 {cut}");
                    assert_eq!(long.round_dp(10), short.round_dp(10), "两腿减同一个数");
                }
                None => {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("数量步长"), "{error}");
                    assert_eq!(position.trims, 0);
                    assert_eq!(position.long.as_ref().unwrap().quantity().unwrap(), before);
                }
            }
        }
    }

    #[tokio::test]
    async fn a_protection_trim_lands_exactly_on_the_target_distance() {
        let dir = std::env::temp_dir().join(format!("arb-trim-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let price = Arc::new(tokio::sync::Mutex::new(dec!(100)));
        let brokers: Vec<Arc<dyn Broker>> = [Venue::Lighter, Venue::Hyperliquid]
            .into_iter()
            .map(|venue| {
                let api: Arc<dyn VenueApi> = Arc::new(FixedBook {
                    venue,
                    price: Arc::clone(&price),
                });
                Arc::new(PaperBroker::new(api, Decimal::ZERO, 5)) as Arc<dyn Broker>
            })
            .collect();
        for broker in &brokers {
            let side = if broker.venue() == Venue::Lighter {
                Side::Buy
            } else {
                Side::Sell
            };
            broker
                .place(&NewOrder {
                    post_only: false,
                    margin_mode: crate::MarginMode::Isolated,
                    client_order_id: ClientOrderId(format!("seed-{}", broker.venue())),
                    venue: broker.venue(),
                    symbol: Symbol::perp("BTC", "USDT"),
                    side,
                    notional_usdt: dec!(1000),
                    quantity: Some(dec!(10)),
                    limit_price: None,
                    reduce_only: false,
                    leverage: Some(dec!(5)),
                })
                .await
                .unwrap();
        }
        *price.lock().await = dec!(110);
        let executor = Executor::new(Arc::clone(&ledger), brokers);

        let mut position = PairPosition {
            id: "t1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: Some(leg(Venue::Hyperliquid, Side::Sell)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            margin_mode: crate::MarginMode::Isolated,
            rules: TaskRules {
                liq_protection_pct: Some(dec!(10)),
                ..TaskRules::default()
            },
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };

        let long = snapshot(Venue::Lighter, dec!(110));
        let short = snapshot(Venue::Hyperliquid, dec!(110));
        let evaluation = evaluate(&position, &long, &short).unwrap();
        let Action::Trim { fraction, reason } = evaluation.action else {
            panic!("空腿距离约 8%，应当减仓：{:?}", evaluation.action);
        };
        executor
            .trim(&mut position, fraction, dec!(110), dec!(110), &reason)
            .await
            .unwrap();

        assert_eq!(position.trims, 1);
        let short_leg = position.short.as_ref().unwrap();
        let long_leg = position.long.as_ref().unwrap();
        let kept = Decimal::ONE - fraction;
        assert_eq!(
            short_leg.notional_usdt.round_dp(8),
            (dec!(1000) * kept).round_dp(8)
        );
        assert_eq!(
            long_leg.notional_usdt.round_dp(8),
            (dec!(1000) * kept).round_dp(8)
        );
        // 空腿的已实现亏损留在保证金里，多腿的已实现盈利也是
        let realized = dec!(10) * fraction * dec!(10);
        assert_eq!(
            short_leg.margin_usdt.unwrap().round_dp(8),
            (dec!(200) - realized).round_dp(8)
        );
        assert_eq!(
            long_leg.margin_usdt.unwrap().round_dp(8),
            (dec!(200) + realized).round_dp(8)
        );

        // 减完之后空腿的强平距离正好是门槛的 1.5 倍
        let liq = liquidation_price(
            short_leg.average_price,
            short_leg.notional_usdt,
            short_leg.margin_usdt.unwrap(),
            dec!(0.01),
            Side::Sell,
        )
        .unwrap();
        let after = distance_to_liquidation_pct(dec!(110), liq, Side::Sell).unwrap();
        assert_eq!(after.round_dp(6), dec!(15));
        assert_eq!(
            evaluate(&position, &long, &short).unwrap().action,
            Action::Hold
        );

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        assert!(
            replayed.orders.contains_key("t1-buy-trim1"),
            "空腿减仓是买单"
        );
        assert!(
            replayed.orders.contains_key("t1-sell-trim1"),
            "多腿减仓是卖单"
        );
        assert_eq!(replayed.positions["t1"].trims, 1);

        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 像真实券商那样：订单号一旦用过就不能再用（再次下单报错，查询仍返回旧订单）；
    /// `reject_once` 里的订单号第一次下单会被明确拒绝（不成交，查询不到）。
    struct IdStrict {
        venue: Venue,
        price: Decimal,
        state: tokio::sync::Mutex<IdStrictState>,
    }

    #[derive(Default)]
    struct IdStrictState {
        reject_once: std::collections::HashSet<String>,
        used: std::collections::HashSet<String>,
        filled: HashMap<String, OrderState>,
    }

    impl IdStrict {
        fn new(venue: Venue, price: Decimal, reject_once: &[&str]) -> Self {
            Self {
                venue,
                price,
                state: tokio::sync::Mutex::new(IdStrictState {
                    reject_once: reject_once.iter().map(|id| id.to_string()).collect(),
                    ..IdStrictState::default()
                }),
            }
        }

        async fn accepted(&self) -> usize {
            self.state.lock().await.filled.len()
        }
    }

    #[async_trait]
    impl Broker for IdStrict {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let id = order.client_order_id.0.clone();
            let mut book = self.state.lock().await;
            if !book.used.insert(id.clone()) {
                return Err(ArbError::venue(
                    self.venue.as_str(),
                    "订单号已经用过，拒绝重发",
                ));
            }
            if book.reject_once.remove(&id) {
                return Err(ArbError::venue(self.venue.as_str(), "业务拒单"));
            }
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(id.clone());
            state.status = OrderStatus::Filled;
            state.average_price = Some(self.price);
            state.filled_usdt = order.quantity.unwrap() * self.price;
            book.filled.insert(id.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: id,
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(self.state.lock().await.filled.get(&id.0).cloned())
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 减仓做到一半失败（多腿减了、空腿被拒）后，下一次重试必须用新的订单号：
    /// 复用旧号时券商要么拒绝，要么（查询返回旧成交）让执行器把旧成交再记一遍账。
    #[tokio::test]
    async fn a_retry_after_an_incomplete_trim_uses_fresh_order_ids() {
        let dir = std::env::temp_dir().join(format!("arb-trim-retry-{}", std::process::id()));
        let path = dir.join("ledger.jsonl");
        let _ = tokio::fs::remove_file(&path).await;
        let ledger = Arc::new(Ledger::open(&path).await.unwrap());
        let long_broker = Arc::new(IdStrict::new(Venue::Lighter, dec!(100), &[]));
        // 空腿减仓是买单：第一次编号为 trim1 的那笔被拒。
        let short_broker = Arc::new(IdStrict::new(
            Venue::Hyperliquid,
            dec!(100),
            &["t1-buy-trim1"],
        ));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&long_broker) as Arc<dyn Broker>,
                Arc::clone(&short_broker) as Arc<dyn Broker>,
            ],
        );
        let mut position = PairPosition {
            id: "t1".into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: Some(leg(Venue::Hyperliquid, Side::Sell)),
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(5)),
            margin_mode: crate::MarginMode::Isolated,
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        };

        let first = executor
            .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
            .await;
        assert!(first.is_err(), "空腿被拒，第一次减仓必须报错");
        assert_eq!(position.trims, 1, "失败的那次也要占掉订单号");

        executor
            .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
            .await
            .expect("重试要换新订单号，不能被订单号去重挡住");
        assert_eq!(position.trims, 2);
        // 多腿两次减仓都是真的发出去并成交的；复用旧号的话这里只有 1 笔，
        // 第二次的“成交”其实是旧订单。
        assert_eq!(long_broker.accepted().await, 2);
        assert_eq!(short_broker.accepted().await, 1);

        let (replayed, broken) = ledger.replay().await.unwrap();
        assert_eq!(broken, 0);
        for id in ["t1-sell-trim1", "t1-sell-trim2", "t1-buy-trim2"] {
            assert!(replayed.orders.contains_key(id), "{id} 应当在台账里");
        }
        tokio::fs::remove_file(&path).await.ok();
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// 记录调用顺序的桩券商：开仓单按固定价全部成交（第二腿带数量就按数量）。
    struct Logged {
        venue: Venue,
        price: Decimal,
        fee: Decimal,
        prepare_error: Option<&'static str>,
        log: Arc<std::sync::Mutex<Vec<String>>>,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    impl Logged {
        fn new(
            venue: Venue,
            price: Decimal,
            fee: Decimal,
            log: &Arc<std::sync::Mutex<Vec<String>>>,
        ) -> Self {
            Self {
                venue,
                price,
                fee,
                prepare_error: None,
                log: Arc::clone(log),
                orders: Default::default(),
            }
        }
    }

    #[async_trait]
    impl Broker for Logged {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn prepare_open(&self, _: &Symbol, _: Option<Decimal>) -> ArbResult<()> {
            self.log
                .lock()
                .unwrap()
                .push(format!("prepare:{}", self.venue));
            match self.prepare_error {
                Some(reason) => Err(ArbError::venue(self.venue.as_str(), reason)),
                None => Ok(()),
            }
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            self.log
                .lock()
                .unwrap()
                .push(format!("place:{}", self.venue));
            let quantity = order
                .quantity
                .unwrap_or_else(|| (order.notional_usdt / self.price).round_dp(8));
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            state.status = OrderStatus::Filled;
            state.average_price = Some(self.price);
            state.filled_usdt = quantity * self.price;
            state.fee_usdt = self.fee;
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 按限价成交的 IOC：买单的限价不低于 `price`、卖单的限价不高于它才按 `price` 全部成交，否则一点不成交
    /// （reduce-only 回滚单照常按 `price` 成交）。`provisional` 有值时 [`Broker::place_for_hedge`] 先报
    /// 待核实的（均价, 手续费），之后 `order_state` 等 `settle_delay` 才给最终状态（或 `settle_error`）。
    struct Hedging {
        venue: Venue,
        price: Decimal,
        fee: Decimal,
        provisional: Option<(Decimal, Decimal)>,
        settle_delay: std::time::Duration,
        settle_error: bool,
        /// reduce-only 单数量低于它就拒（模拟交易所最小下单额：零头退不掉）。默认 0 = 不拒。
        min_reduce: Decimal,
        log: Arc<std::sync::Mutex<Vec<String>>>,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
        unsettled: tokio::sync::Mutex<std::collections::HashSet<String>>,
    }

    impl Hedging {
        fn new(
            venue: Venue,
            price: Decimal,
            fee: Decimal,
            log: &Arc<std::sync::Mutex<Vec<String>>>,
        ) -> Self {
            Self {
                venue,
                price,
                fee,
                provisional: None,
                settle_delay: std::time::Duration::ZERO,
                settle_error: false,
                min_reduce: Decimal::ZERO,
                log: Arc::clone(log),
                orders: Default::default(),
                unsettled: Default::default(),
            }
        }

        fn note(&self, what: &str) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{what}:{}", self.venue));
        }

        async fn fill(&self, order: &NewOrder) -> OrderState {
            self.note("place");
            let reachable = order.reduce_only
                || match (order.side, order.limit_price) {
                    (_, None) => true,
                    (Side::Buy, Some(limit)) => limit >= self.price,
                    (Side::Sell, Some(limit)) => limit <= self.price,
                };
            let quantity = order
                .quantity
                .unwrap_or_else(|| (order.notional_usdt / self.price).round_dp(6));
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            if order.reduce_only && quantity < self.min_reduce {
                state.status = OrderStatus::Rejected;
                state.reject_reason = Some("order rounds to dust or is below venue minimum".into());
            } else if reachable {
                state.status = OrderStatus::Filled;
                state.average_price = Some(self.price);
                state.filled_usdt = quantity * self.price;
                state.fee_usdt = self.fee;
            } else {
                state.status = OrderStatus::Cancelled;
                state.reject_reason = Some("IOC 在限价以内没有对手盘".into());
            }
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state.clone());
            state
        }
    }

    #[async_trait]
    impl Broker for Hedging {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let state = self.fill(order).await;
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: state.status,
            })
        }
        async fn place_for_hedge(&self, order: &NewOrder) -> ArbResult<Option<OrderState>> {
            let state = self.fill(order).await;
            let Some((price, fee)) = self
                .provisional
                .filter(|_| state.filled_usdt > Decimal::ZERO)
            else {
                return Ok(None);
            };
            self.unsettled
                .lock()
                .await
                .insert(order.client_order_id.0.clone());
            let mut provisional = state.clone();
            provisional.filled_usdt = state.filled_usdt / self.price * price;
            provisional.average_price = Some(price);
            provisional.fee_usdt = fee;
            Ok(Some(provisional))
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            if self.unsettled.lock().await.remove(&id.0) {
                tokio::time::sleep(self.settle_delay).await;
                self.note("settled");
                if self.settle_error {
                    return Err(ArbError::venue(self.venue.as_str(), "成交记录查询超时"));
                }
            }
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 某次实盘：先在 Arcus 买（预估滑点大一点，先做），后在 Lighter RH 卖；Lighter 计划时买一 5.3685，
    /// 限价 5.3685 × (1 − 0.2%)。保本线按收敛目标 −0.042% 加平仓穿价与往返手续费，约 +0.0156%。
    fn near_plan(floor: Option<Decimal>) -> Plan {
        let leg = |venue, side, best: Decimal, expected: Decimal, limit| LegPlan {
            symbol: Symbol::perp("NEAR", "USDT"),
            venue,
            side,
            notional_usdt: dec!(800),
            limit_price: limit,
            best_price: best,
            expected_price: expected,
            slippage: (expected - best).abs() / best,
            book_notional: dec!(10000),
            fee: Decimal::ZERO,
            maker_rest_ms: None,
        };
        Plan {
            symbol: Symbol::perp("NEAR", "USDT"),
            long: leg(
                Venue::Arcus,
                Side::Buy,
                dec!(5.363),
                dec!(5.3633751),
                dec!(5.373726),
            ),
            short: leg(
                Venue::LighterRh,
                Side::Sell,
                dec!(5.3685),
                dec!(5.3685),
                dec!(5.357763),
            ),
            expected_cost: dec!(0.0002),
            min_locked_basis: floor,
            leg_order: crate::preflight::LegOrder::Slippage,
        }
    }

    fn near_setup() -> PositionSetup {
        PositionSetup {
            margin_mode: crate::MarginMode::Isolated,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            quoted_at: Some(Utc::now()),
            positions_checked: false,
            force_hedge: None,
        }
    }

    /// 开 某合约：Arcus 成交在 `arcus` 的价、Lighter 的买一已经掉到 `lighter`。
    async fn open_near(
        name: &str,
        floor: Option<Decimal>,
        arcus: Hedging,
        lighter: Decimal,
        log: &Arc<std::sync::Mutex<Vec<String>>>,
    ) -> (PairPosition, Arc<Ledger>) {
        let ledger = temp_ledger(name).await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(arcus) as Arc<dyn Broker>,
                Arc::new(Hedging::new(Venue::LighterRh, lighter, Decimal::ZERO, log)),
            ],
        );
        let outcome = executor
            .open(
                &near_plan(floor),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &near_setup(),
            )
            .await
            .unwrap();
        (outcome.position, ledger)
    }

    /// 第二腿用的桩券商：前 `misses` 张开仓单在限价内一个都吃不到（IOC 被撤），之后按 `price` 全部成交；
    /// `partial` 时第一张只成交一半；`unknown` 时第一张结果不明（下单与查单都报错：可能其实成交了）。
    /// 平仓单（reduce-only）总是成交。
    struct Refills {
        venue: Venue,
        price: Decimal,
        misses: usize,
        partial: bool,
        /// `partial` 时第一张成交的比例（默认一半）。
        partial_ratio: Decimal,
        unknown: bool,
        /// 没吃到时报的原因（默认流动性不足）。
        miss_reason: &'static str,
        /// 持仓接口报的 某合约 净数量（默认 0 = 没有持仓）。
        net: std::sync::Mutex<Decimal>,
        entries: std::sync::atomic::AtomicUsize,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    impl Refills {
        fn new(venue: Venue, price: Decimal, misses: usize) -> Self {
            Self {
                venue,
                price,
                misses,
                partial: false,
                partial_ratio: Decimal::new(5, 1),
                unknown: false,
                miss_reason: "canceled-not-enough-liquidity",
                net: std::sync::Mutex::new(Decimal::ZERO),
                entries: Default::default(),
                orders: Default::default(),
            }
        }
    }

    #[async_trait]
    impl Broker for Refills {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let n = if order.reduce_only {
                usize::MAX
            } else {
                self.entries
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            };
            if self.unknown && n == 0 {
                self.orders.lock().await.insert(
                    order.client_order_id.0.clone(),
                    OrderState::new(order.clone()),
                );
                return Err(ArbError::venue(self.venue.as_str(), "下单超时（结果未知）"));
            }
            let quantity = order
                .quantity
                .unwrap_or_else(|| (order.notional_usdt / self.price).round_dp(6));
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            if !order.reduce_only && n < self.misses {
                state.status = OrderStatus::Cancelled;
                state.reject_reason = Some(self.miss_reason.into());
            } else {
                let filled = if self.partial && n == 0 {
                    quantity * self.partial_ratio
                } else {
                    quantity
                };
                state.status = if filled < quantity {
                    OrderStatus::Cancelled
                } else {
                    OrderStatus::Filled
                };
                state.average_price = Some(self.price);
                state.filled_usdt = filled * self.price;
            }
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state.clone());
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: state.status,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            let state = self.orders.lock().await.get(&id.0).cloned();
            if state
                .as_ref()
                .is_some_and(|state| state.status == OrderStatus::Pending)
            {
                return Err(ArbError::venue(self.venue.as_str(), "查不到结论"));
            }
            Ok(state)
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            let net = *self.net.lock().unwrap();
            Ok((!net.is_zero())
                .then(|| crate::broker::VenuePosition {
                    venue: self.venue,
                    symbol: Symbol::perp("NEAR", "USDT"),
                    net_quantity: net,
                    average_price: Some(self.price),
                    notional_usdt: net.abs() * self.price,
                })
                .into_iter()
                .collect())
        }
    }

    /// 开 某合约：Arcus（第一腿）成交在 5.36387，Lighter（第二腿）按 `lighter` 的脚本走。
    async fn open_with_second(
        name: &str,
        lighter: Refills,
    ) -> (ArbResult<ExecutionOutcome>, Arc<Ledger>, Arc<Refills>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let lighter = Arc::new(lighter);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::Arcus,
                    dec!(5.36387),
                    dec!(0.1701),
                    &log,
                )) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let outcome = executor
            .open(
                &near_plan(Some(dec!(0.000156))),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &near_setup(),
            )
            .await;
        (outcome, ledger, lighter)
    }

    fn entries(lighter: &Refills) -> usize {
        lighter.entries.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 报得出数量步长的券商（其余照 `inner`）：两腿都是它才会同时下。`log` 里按时间记「开始下单」「下单返回」，
    /// 用来判断两腿是不是同时发出的；`delay` 是每张开仓单在交易所那边要多久。
    struct Stepped {
        inner: Arc<dyn Broker>,
        step: Option<Decimal>,
        delay: std::time::Duration,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Stepped {
        fn wrap(
            inner: Arc<dyn Broker>,
            step: Option<Decimal>,
            delay_ms: u64,
            log: &Arc<std::sync::Mutex<Vec<String>>>,
        ) -> Arc<dyn Broker> {
            Arc::new(Self {
                inner,
                step,
                delay: std::time::Duration::from_millis(delay_ms),
                log: Arc::clone(log),
            })
        }

        async fn around<T>(
            &self,
            order: &NewOrder,
            work: impl std::future::Future<Output = T>,
        ) -> T {
            let venue = self.inner.venue();
            let entry = !order.reduce_only;
            if entry {
                self.log.lock().unwrap().push(format!("start:{venue}"));
                tokio::time::sleep(self.delay).await;
            }
            let result = work.await;
            if entry {
                self.log.lock().unwrap().push(format!("done:{venue}"));
            }
            result
        }
    }

    #[async_trait]
    impl Broker for Stepped {
        fn venue(&self) -> Venue {
            self.inner.venue()
        }
        fn fee_per_side(&self) -> Decimal {
            self.inner.fee_per_side()
        }
        async fn quantity_step(&self, _: &Symbol) -> ArbResult<Option<Decimal>> {
            Ok(self.step)
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            self.around(order, self.inner.place(order)).await
        }
        async fn place_for_hedge(&self, order: &NewOrder) -> ArbResult<Option<OrderState>> {
            self.around(order, self.inner.place_for_hedge(order)).await
        }
        async fn settle_hedge(&self, provisional: &OrderState) -> ArbResult<Option<OrderState>> {
            self.inner.settle_hedge(provisional).await
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            self.inner.order_state(id).await
        }
        async fn cancel(&self, venue_id: &str) -> ArbResult<()> {
            self.inner.cancel(venue_id).await
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            self.inner.open_orders().await
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            self.inner.positions().await
        }
    }

    /// 开 某合约（第一腿 `first`、第二腿 Lighter RH 的 `second`），两家都报步长时同时下。返回日志。
    async fn open_concurrent(
        name: &str,
        plan: Plan,
        first: Arc<dyn Broker>,
        second: Arc<dyn Broker>,
        setup: PositionSetup,
    ) -> (ArbResult<ExecutionOutcome>, Arc<Ledger>) {
        let ledger = temp_ledger(name).await;
        let executor = Executor::new(Arc::clone(&ledger), vec![first, second]);
        let outcome = executor
            .open(&plan, Strategy::Spread, dec!(0.1268), name, &setup)
            .await;
        (outcome, ledger)
    }

    fn arcus_fills(log: &Arc<std::sync::Mutex<Vec<String>>>) -> Arc<dyn Broker> {
        Arc::new(Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), log))
    }

    fn logged(log: &Arc<std::sync::Mutex<Vec<String>>>) -> Vec<String> {
        log.lock()
            .unwrap()
            .iter()
            .filter(|line| line.starts_with("start:") || line.starts_with("done:"))
            .cloned()
            .collect()
    }

    /// 第一腿不是 Hyperliquid、两家都报得出步长：第二腿不等第一腿，两腿同时发出、同一个数量（粗的步长取整）；
    /// 第二腿第 0 次的限价按第一腿**预估**均价收紧到保本线。
    #[tokio::test]
    async fn legs_go_out_together_with_one_quantity_when_the_first_is_not_hyperliquid() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let plan = near_plan(Some(dec!(0.000156)));
        let (outcome, ledger) = open_concurrent(
            "together-ok",
            plan.clone(),
            Stepped::wrap(arcus_fills(&log), Some(dec!(0.1)), 50, &log),
            Stepped::wrap(
                Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0)),
                Some(dec!(0.01)),
                50,
                &log,
            ),
            near_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(
            &logged(&log)[..2],
            ["start:arcus", "start:lighter-rh"],
            "第二腿在第一腿返回之前就发出了：{:?}",
            logged(&log)
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["together-ok-buy-0"];
        let second = &replayed.orders["together-ok-sell-0"];
        // 第一腿限价按保本线封顶到 5.368038（Lighter 预估 5.3685 × (2 − 0.0086%) / (2 + 0.0086%)，见
        // `Plan::first_leg_capped`）：800 / 5.368038 = 149.03…，按两家里粗的步长 0.1 向下取整。
        assert_eq!(first.order.quantity, Some(dec!(149.0)));
        assert_eq!(second.order.quantity, Some(dec!(149.0)));
        assert_eq!(
            second.order.limit_price,
            Some(
                plan.hedged_after(plan.long.expected_price)
                    .short
                    .limit_price
            )
        );
        assert_eq!(
            position.long.unwrap().quantity(),
            position.short.unwrap().quantity()
        );
    }

    /// 第一腿没成交、同时发出的第二腿成交了：回滚第二腿，备注说明是怎么回事；这是真回滚（有成交），不算「什么都没发生」。
    #[tokio::test]
    async fn a_second_leg_filled_alongside_an_unfilled_first_is_unwound() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let second = Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0));
        let (outcome, ledger) = open_concurrent(
            "together-first-miss",
            near_plan(Some(dec!(0.000156))),
            // 卖一涨到了限价之外：第一腿一点没吃到。
            Stepped::wrap(
                Arc::new(Hedging::new(Venue::Arcus, dec!(5.5), Decimal::ZERO, &log)),
                Some(dec!(0.1)),
                0,
                &log,
            ),
            Stepped::wrap(second.clone(), Some(dec!(0.01)), 0, &log),
            near_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Unwound);
        let note = position.note.unwrap();
        assert!(
            note.contains("同时发出的第二腿") && note.contains("已回滚"),
            "{note}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders["together-first-miss-sell-0"].filled_usdt > Decimal::ZERO);
        let undone: Vec<_> = replayed
            .orders
            .values()
            .filter(|order| order.order.venue == Venue::LighterRh && order.order.reduce_only)
            .collect();
        assert_eq!(undone.len(), 1, "第二腿 reduce-only 平掉");
        assert_eq!(undone[0].order.quantity, Some(dec!(149.0)));
    }

    /// 第一腿只成交了一点（不够对冲）、第二腿全成：两条都回滚。
    #[tokio::test]
    async fn a_tiny_first_fill_unwinds_both_legs_sent_together() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut first = Refills::new(Venue::Arcus, dec!(5.36387), 0);
        first.partial = true;
        first.partial_ratio = dec!(0.01);
        let (outcome, ledger) = open_concurrent(
            "together-tiny",
            near_plan(Some(dec!(0.000156))),
            Stepped::wrap(Arc::new(first), Some(dec!(0.1)), 0, &log),
            Stepped::wrap(
                Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0)),
                Some(dec!(0.01)),
                0,
                &log,
            ),
            near_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        for venue in [Venue::Arcus, Venue::LighterRh] {
            assert!(
                replayed
                    .orders
                    .values()
                    .any(|order| order.order.venue == venue && order.order.reduce_only),
                "{venue} 要回滚"
            );
        }
    }

    /// 第一腿部分成交但够对冲、第二腿全成：不回滚，把第二腿多出来的部分 reduce-only 退掉，按第一腿的数量持有。
    #[tokio::test]
    async fn a_partial_first_fill_trims_the_second_leg_sent_together() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut first = Refills::new(Venue::Arcus, dec!(5.36387), 0);
        first.partial = true;
        first.partial_ratio = dec!(0.6);
        let (outcome, _) = open_concurrent(
            "together-trim",
            near_plan(Some(dec!(0.000156))),
            Stepped::wrap(Arc::new(first), Some(dec!(0.1)), 0, &log),
            Stepped::wrap(
                Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0)),
                Some(dec!(0.01)),
                0,
                &log,
            ),
            near_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(
            position
                .note
                .as_deref()
                .unwrap()
                .contains(BALANCED_HEDGE_NOTE)
        );
        assert_eq!(position.long.unwrap().quantity(), Some(dec!(89.4)));
    }

    /// 同时发出的第二腿一点没吃到：之后照旧按第一腿的**实际**均价收紧的保本价重试（订单号接着 1）。
    #[tokio::test]
    async fn a_missed_second_leg_sent_together_is_retried_at_the_actual_breakeven() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let plan = near_plan(Some(dec!(0.000156)));
        let (outcome, ledger) = open_concurrent(
            "together-retry",
            plan.clone(),
            Stepped::wrap(arcus_fills(&log), Some(dec!(0.1)), 0, &log),
            Stepped::wrap(
                Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 1)),
                Some(dec!(0.01)),
                0,
                &log,
            ),
            near_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["together-retry-sell-0"].filled_usdt,
            Decimal::ZERO
        );
        let retry = &replayed.orders["together-retry-sell-1"];
        assert!(retry.filled_usdt > Decimal::ZERO);
        assert_eq!(
            retry.order.limit_price,
            Some(plan.hedged_after(dec!(5.36387)).short.limit_price)
        );
    }

    /// 同时发出的第二腿只吃到一半、开着强制补腿：照旧按最差限价追剩下的。
    #[tokio::test]
    async fn a_partial_second_leg_sent_together_is_chased_with_force_hedge() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut second = Refills::new(Venue::LighterRh, dec!(5.3685), 0);
        second.partial = true;
        let (outcome, ledger) = open_concurrent(
            "together-chase",
            near_plan(Some(dec!(0.000156))),
            Stepped::wrap(arcus_fills(&log), Some(dec!(0.1)), 0, &log),
            Stepped::wrap(Arc::new(second), Some(dec!(0.01)), 0, &log),
            PositionSetup {
                force_hedge: Some(dec!(0.003)),
                ..near_setup()
            },
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["together-chase-sell-1"].order.quantity,
            Some(dec!(74.5)),
            "追的是剩下的一半"
        );
        assert_eq!(position.short.unwrap().quantity(), Some(dec!(149.0)));
    }

    /// 同时下时任何一条腿结果不明：不回滚、不追，仓位留在 Opening 交给对账；续上建仓在那张单读出结局之前什么都不发。
    #[tokio::test]
    async fn an_unknown_leg_sent_together_is_left_for_reconciliation_without_chasing() {
        for (name, first_unknown, second_unknown) in [
            ("together-unknown-first", true, false),
            ("together-unknown-second", false, true),
            ("together-unknown-both", true, true),
        ] {
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut first = Refills::new(Venue::Arcus, dec!(5.36387), 0);
            first.unknown = first_unknown;
            let mut second = Refills::new(Venue::LighterRh, dec!(5.3685), 0);
            second.unknown = second_unknown;
            let second = Arc::new(second);
            let ledger = temp_ledger(name).await;
            let executor = Executor::new(
                Arc::clone(&ledger),
                vec![
                    Stepped::wrap(Arc::new(first), Some(dec!(0.1)), 0, &log),
                    Stepped::wrap(second.clone(), Some(dec!(0.01)), 0, &log),
                ],
            );
            let outcome = executor
                .open(
                    &near_plan(Some(dec!(0.000156))),
                    Strategy::Spread,
                    dec!(0.1268),
                    name,
                    &near_setup(),
                )
                .await;
            assert!(outcome.is_err(), "{name}");
            let (replayed, _) = ledger.replay().await.unwrap();
            let mut position = replayed.positions[name].clone();
            assert_eq!(position.status, PositionStatus::Opening, "{name}");
            assert!(
                replayed
                    .orders
                    .values()
                    .all(|order| !order.order.reduce_only),
                "{name}：不回滚"
            );
            let sent = entries(&second);
            // 场所上有台账不知道的持仓（读不到的单可能成交了）：结局读出来之前什么都不发。
            *second.net.lock().unwrap() = dec!(-1);
            let step = executor.resume_open(&mut position, true).await.unwrap();
            if second_unknown {
                assert!(matches!(step, Resume::Waiting(_)), "{name}: {step:?}");
                assert_eq!(entries(&second), sent, "{name}：结局读出来之前不补第二腿");
            }
            // 场所持仓与台账一致：读不到的单按没成交了结（2026-10-31 审查：以前这里每轮只会等，
            // 成交的那条腿一直裸着）。第一腿没成交、第二腿成交了 → 改补第一腿那家；第二腿没成交 → 补第二腿；
            // 两张都没成交 → 记为 Unwound。
            *second.net.lock().unwrap() = Decimal::ZERO;
            let mut step = executor.resume_open(&mut position, true).await.unwrap();
            if step == Resume::Verified {
                step = executor.resume_open(&mut position, true).await.unwrap();
            }
            assert_eq!(step, Resume::Done, "{name}: {:?}", position.note);
            let expected = if first_unknown && second_unknown {
                PositionStatus::Unwound
            } else {
                PositionStatus::Open
            };
            assert_eq!(position.status, expected, "{name}: {:?}", position.note);
        }
    }

    /// 第一腿是 Hyperliquid、或任一家报不出步长：照旧顺序下 —— 第一腿返回之前第二腿一张单都不发。
    #[tokio::test]
    async fn legs_stay_sequential_for_a_hyperliquid_first_leg_or_an_unknown_step() {
        for (name, first_venue, first_step) in [
            ("seq-hl-first", Venue::HyperliquidXyz, Some(dec!(0.1))),
            ("seq-no-step", Venue::Arcus, None),
        ] {
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut plan = near_plan(Some(dec!(0.000156)));
            plan.long.venue = first_venue;
            let (outcome, _) = open_concurrent(
                name,
                plan,
                Stepped::wrap(
                    Arc::new(Hedging::new(first_venue, dec!(5.36387), dec!(0.1701), &log)),
                    first_step,
                    20,
                    &log,
                ),
                Stepped::wrap(
                    Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0)),
                    Some(dec!(0.01)),
                    0,
                    &log,
                ),
                near_setup(),
            )
            .await;
            let position = outcome.unwrap().position;
            assert_eq!(
                position.status,
                PositionStatus::Open,
                "{name}: {:?}",
                position.note
            );
            let order = logged(&log);
            assert_eq!(
                order[..2],
                [
                    format!("start:{first_venue}"),
                    format!("done:{first_venue}")
                ],
                "{name}: {order:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_second_leg_that_found_no_liquidity_is_retried_and_opens_when_the_book_refills() {
        let (outcome, ledger, lighter) = open_with_second(
            "retry-fills",
            Refills::new(Venue::LighterRh, dec!(5.3685), 1),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&lighter), 2);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["retry-fills-sell-0"].filled_usdt,
            Decimal::ZERO
        );
        assert!(replayed.orders["retry-fills-sell-1"].filled_usdt > Decimal::ZERO);
        // 两次都是同一个（保本）限价：重试不放松价格。
        assert_eq!(
            replayed.orders["retry-fills-sell-0"].order.limit_price,
            replayed.orders["retry-fills-sell-1"].order.limit_price
        );
        let short = position.short.unwrap();
        assert_eq!(short.client_order_id.0, "retry-fills-sell-1");
    }

    #[tokio::test]
    async fn a_second_leg_that_never_finds_liquidity_is_tried_three_times_within_the_window_then_unwound()
     {
        let started = std::time::Instant::now();
        let (outcome, ledger, lighter) = open_with_second(
            "retry-gives-up",
            Refills::new(Venue::LighterRh, dec!(5.3685), 99),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert_eq!(entries(&lighter), SECOND_LEG_ATTEMPTS as usize);
        assert!(started.elapsed() < SECOND_LEG_RETRY_WINDOW + std::time::Duration::from_secs(1));
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders.contains_key("retry-gives-up-sell-2"));
        assert!(!replayed.orders.contains_key("retry-gives-up-sell-3"));
        let note = position.note.unwrap();
        assert!(note.contains("最多试 3 次"), "{note}");
    }

    #[tokio::test]
    async fn a_partial_second_leg_is_not_retried_and_an_unknown_one_stays_opening() {
        let (outcome, ledger, lighter) = open_with_second(
            "retry-partial",
            Refills {
                partial: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
        )
        .await;
        let position = outcome.unwrap().position;
        // 第二腿成交了一半：不重试、也不回滚，退掉第一腿多出来的一半，按一半对冲持有。
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(
            position
                .note
                .as_deref()
                .unwrap()
                .contains(BALANCED_HEDGE_NOTE)
        );
        assert_eq!(entries(&lighter), 1, "部分成交不按同一限价重试");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(!replayed.orders.contains_key("retry-partial-sell-1"));
        assert!(
            replayed.orders.contains_key("retry-partial-sell-undo0"),
            "第一腿多出来的退掉"
        );

        // 结果不明：可能其实成交了 —— 不重试、不回滚，仓位停在 Opening 等对账。
        let (outcome, ledger, lighter) = open_with_second(
            "retry-unknown",
            Refills {
                unknown: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
        )
        .await;
        assert!(outcome.is_err());
        assert_eq!(entries(&lighter), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.positions["retry-unknown"].status,
            PositionStatus::Opening
        );
        assert!(!replayed.orders.contains_key("retry-unknown-sell-1"));
    }

    /// 强制补腿打开：最差锁定价差 −0.3%。
    fn forced_setup() -> PositionSetup {
        PositionSetup {
            force_hedge: Some(dec!(0.003)),
            ..near_setup()
        }
    }

    /// 同 [`open_with_second`]，但带上 `setup`（强制补腿开关）。
    async fn open_with_second_using(
        name: &str,
        lighter: Refills,
        setup: PositionSetup,
    ) -> (ArbResult<ExecutionOutcome>, Arc<Ledger>, Arc<Refills>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let lighter = Arc::new(lighter);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::Arcus,
                    dec!(5.36387),
                    dec!(0.1701),
                    &log,
                )) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let outcome = executor
            .open(
                &near_plan(Some(dec!(0.000156))),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &setup,
            )
            .await;
        (outcome, ledger, lighter)
    }

    /// 保本价内一点没吃到：开着强制补腿时保本价只试一次，然后马上按最差限价追，追上就是一笔对冲仓位。
    #[tokio::test]
    async fn a_forced_hedge_chases_a_second_leg_that_found_nothing_instead_of_unwinding() {
        let (outcome, ledger, lighter) = open_with_second_using(
            "force-none",
            Refills::new(Venue::LighterRh, dec!(5.3613), 1),
            forced_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(position.is_hedged());
        // 保本价只试一次（不是 3 次），第二次就是追单。
        assert_eq!(entries(&lighter), 2);
        let (replayed, _) = ledger.replay().await.unwrap();
        let breakeven = replayed.orders["force-none-sell-0"]
            .order
            .limit_price
            .unwrap();
        let forced = replayed.orders["force-none-sell-1"]
            .order
            .limit_price
            .unwrap();
        assert_eq!(
            replayed.orders["force-none-sell-0"].filled_usdt,
            Decimal::ZERO
        );
        assert!(
            forced < breakeven,
            "追单的限价比保本价宽：{forced} < {breakeven}"
        );
        // 第一腿 Arcus 买在 5.36387：锁定价差恰好不低于 −0.3%。
        let locked = (forced - dec!(5.36387)) / ((forced + dec!(5.36387)) / Decimal::TWO);
        assert!(
            locked >= dec!(-0.003) && locked < dec!(-0.003) + dec!(0.000000001),
            "{locked}"
        );
        // 备注写明是追出来的，以及锁定的价差。
        let note = position.note.unwrap();
        assert!(note.starts_with(FORCED_HEDGE_NOTE), "{note}");
        assert!(
            note.contains("低于保本线") && note.contains("−0.3%"),
            "{note}"
        );
        // 成交价 5.3613：锁定价差为负（−0.05% 左右）。
        let short = position.short.unwrap();
        assert_eq!(short.average_price, dec!(5.3613));
    }

    /// 保本价内只吃到一半：追**剩下的一半**，合并成一条腿；不再因为「部分成交」把两腿都回滚。
    #[tokio::test]
    async fn a_forced_hedge_completes_a_partly_filled_second_leg_with_just_the_remainder() {
        let (outcome, ledger, lighter) = open_with_second_using(
            "force-partial",
            Refills {
                partial: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            forced_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&lighter), 2);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["force-partial-sell-0"];
        let second = &replayed.orders["force-partial-sell-1"];
        let first_quantity = first.order.quantity.unwrap();
        // 追单的数量 = 还没对冲的那一半，不是整笔。
        assert_eq!(
            second.order.quantity.unwrap(),
            first_quantity / Decimal::TWO
        );
        // 名义额也按剩余数量重算（不是整笔的 200）：万一哪家券商以名义额为准，也不会把整笔再对冲一次。
        let limit = second.order.limit_price.unwrap();
        assert_eq!(
            second.order.notional_usdt,
            first_quantity / Decimal::TWO * limit
        );
        assert!(second.order.notional_usdt < first.order.notional_usdt);
        // 合并后的腿数量 = 两笔之和，名义相加，均价按数量加权。
        let short = position.short.unwrap();
        let hedged_quantity = short.quantity().unwrap();
        let arcus_quantity = position.long.unwrap().quantity().unwrap();
        assert!(
            (hedged_quantity - arcus_quantity).abs() / arcus_quantity <= SECOND_LEG_QTY_TOLERANCE
        );
        assert_eq!(short.notional_usdt, first.filled_usdt + second.filled_usdt);
        assert!(position.note.unwrap().starts_with(FORCED_HEDGE_NOTE));
        // 台账重放出的逐腿明细也认这两张开仓单（均价加权，没有差额）。
        let (replayed, _) = ledger.replay().await.unwrap();
        let breakdown = crate::breakdown::position_breakdown(
            &replayed.positions["force-partial"],
            &replayed.orders,
        )
        .unwrap();
        let sold = breakdown.short.unwrap();
        assert_eq!(
            sold.entry_notional_usdt,
            first.filled_usdt + second.filled_usdt
        );
    }

    /// 追单也追不到：回滚，和没开时一样（最后一道兜底）；追单有自己的次数预算。
    #[tokio::test]
    async fn a_forced_hedge_that_gets_nothing_either_still_unwinds_within_its_own_budget() {
        let started = std::time::Instant::now();
        let (outcome, ledger, lighter) = open_with_second_using(
            "force-giveup",
            Refills::new(Venue::LighterRh, dec!(5.3613), 99),
            forced_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert_eq!(entries(&lighter), 1 + FORCE_HEDGE_ATTEMPTS as usize);
        assert!(started.elapsed() < FORCE_HEDGE_WINDOW + std::time::Duration::from_secs(1));
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders.contains_key("force-giveup-sell-2"));
        assert!(!replayed.orders.contains_key("force-giveup-sell-3"));
        assert!(
            !position
                .note
                .unwrap_or_default()
                .starts_with(FORCED_HEDGE_NOTE)
        );
    }

    /// 第二腿被价格带拒绝（Arcus 被钉在带边）：同价不重试、强制补腿也不追，直接回滚
    /// （某次实盘 各试了 3 次，限价都被收回同一个带边、再被拒，白裸露两三秒）。
    #[tokio::test]
    async fn a_second_leg_refused_by_the_price_band_is_neither_retried_nor_chased() {
        for (name, setup) in [
            ("band-forced", forced_setup()),
            ("band-plain", near_setup()),
        ] {
            let (outcome, ledger, lighter) = open_with_second_using(
                name,
                Refills {
                    miss_reason: arb_venues::arcus::BOUND_REJECT,
                    ..Refills::new(Venue::LighterRh, dec!(5.3613), 99)
                },
                setup,
            )
            .await;
            let position = outcome.unwrap().position;
            assert_eq!(position.status, PositionStatus::Unwound, "{name}");
            assert_eq!(entries(&lighter), 1, "{name}：只下了一次");
            let (replayed, _) = ledger.replay().await.unwrap();
            assert!(
                !replayed.orders.contains_key(&format!("{name}-sell-1")),
                "{name}"
            );
        }
    }

    /// 结果不明（下单超时）绝不追：那一单可能其实成交了，再追一单就多对冲一份。
    #[tokio::test]
    async fn a_forced_hedge_never_chases_after_an_unknown_result() {
        let (outcome, ledger, lighter) = open_with_second_using(
            "force-unknown",
            Refills {
                unknown: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            forced_setup(),
        )
        .await;
        assert!(outcome.is_err());
        assert_eq!(entries(&lighter), 1, "不追");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.positions["force-unknown"].status,
            PositionStatus::Opening
        );
        assert!(!replayed.orders.contains_key("force-unknown-sell-1"));
    }

    /// 保本价那一档只差 ≤0.5% 就吃满了：一张追单都不发，也不能给仓位贴「强制补腿」的备注。
    #[tokio::test]
    async fn an_almost_complete_second_leg_sends_no_chase_and_gets_no_forced_note() {
        let (outcome, ledger, lighter) = open_with_second_using(
            "force-almost",
            Refills {
                partial: true,
                // 第一张成交 99.8%（只差 0.2%）。
                partial_ratio: dec!(0.998),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            forced_setup(),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&lighter), 1, "不追");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(!replayed.orders.contains_key("force-almost-sell-1"));
        assert!(
            !position
                .note
                .unwrap_or_default()
                .contains(FORCED_HEDGE_NOTE)
        );
    }

    /// 没开强制补腿（默认）：不追单；第二腿一点没吃到、或只吃到不到一半，照旧回滚。
    #[tokio::test]
    async fn without_the_switch_nothing_changes() {
        let (outcome, _, lighter) = open_with_second_using(
            "force-off",
            Refills {
                partial: true,
                partial_ratio: dec!(0.3),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            near_setup(),
        )
        .await;
        assert_eq!(outcome.unwrap().position.status, PositionStatus::Unwound);
        assert_eq!(entries(&lighter), 1);
        let (outcome, _, lighter) = open_with_second_using(
            "force-off-none",
            Refills::new(Venue::LighterRh, dec!(5.3613), 99),
            near_setup(),
        )
        .await;
        assert_eq!(outcome.unwrap().position.status, PositionStatus::Unwound);
        assert_eq!(entries(&lighter), SECOND_LEG_ATTEMPTS as usize);
    }

    /// 第一腿 Arcus（可设 reduce-only 最小数量）、第二腿 `lighter` 的开仓。
    /// 开 某合约 到一半中断：Arcus（第一腿）买入成交在 5.36387；Lighter（第二腿）第一张单结果不明（下单、查单都报错）。
    async fn interrupted(
        name: &str,
        lighter: Refills,
        setup: PositionSetup,
    ) -> (Executor, Arc<Ledger>, Arc<Refills>, PairPosition) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let lighter = Arc::new(Refills {
            unknown: true,
            ..lighter
        });
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::Arcus,
                    dec!(5.36387),
                    dec!(0.1701),
                    &log,
                )) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let outcome = executor
            .open(
                &near_plan(Some(dec!(0.000156))),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &setup,
            )
            .await;
        assert!(outcome.is_err(), "第二腿结果不明，开仓中断");
        let (replayed, _) = ledger.replay().await.unwrap();
        let position = replayed.positions[name].clone();
        assert_eq!(position.status, PositionStatus::Opening);
        assert!(position.long.is_some() && position.short.is_none());
        (executor, ledger, lighter, position)
    }

    /// 那张结果不明的单，交易所后来查得到了：成交 `filled` 个（0 = IOC 一点没吃到、已撤）。
    async fn venue_settles(lighter: &Refills, id: &str, filled: Decimal) {
        let mut orders = lighter.orders.lock().await;
        let state = orders.get_mut(id).unwrap();
        state.venue_order_id = Some(id.into());
        if filled > Decimal::ZERO {
            state.status = OrderStatus::Filled;
            state.average_price = Some(lighter.price);
            state.filled_usdt = filled * lighter.price;
        } else {
            state.status = OrderStatus::Cancelled;
            state.reject_reason = Some("IOC 在限价以内没有对手盘".into());
        }
    }

    fn first_quantity(position: &PairPosition) -> Decimal {
        position.long.as_ref().unwrap().quantity().unwrap()
    }

    /// 某次实盘：第二腿其实已经成交，只是当时读不到。续上时核实到两腿都在，直接转为持仓，一张单都不补 ——
    /// 这一步不发单，所以这个币还冻结着也照做（冻结多半就是这笔造成的）。
    #[tokio::test]
    async fn an_interrupted_open_whose_second_leg_did_fill_becomes_open_without_any_order() {
        let (executor, ledger, lighter, mut position) = interrupted(
            "resume-filled",
            Refills::new(Venue::LighterRh, dec!(5.3685), 0),
            forced_setup(),
        )
        .await;
        venue_settles(&lighter, "resume-filled-sell-0", first_quantity(&position)).await;
        let step = executor.resume_open(&mut position, false).await.unwrap();
        assert_eq!(step, Resume::Done);
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(position.is_hedged());
        assert_eq!(
            position.short.as_ref().unwrap().client_order_id.0,
            "resume-filled-sell-0"
        );
        assert!(position.note.as_deref().unwrap().starts_with(RESUMED_NOTE));
        assert_eq!(entries(&lighter), 1, "一张单都没补");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["resume-filled-sell-0"].status,
            OrderStatus::Filled
        );
        assert_eq!(
            replayed.positions["resume-filled"].status,
            PositionStatus::Open
        );
    }

    /// 某次实盘：第二腿当时被拒（没成交），只是读不到。续上时先只核实、补进台账（不发单）；这个币的对账还不干净
    /// 就不补；干净了才按强制补腿的最差限价把整笔数量补上，订单号接着编。
    #[tokio::test]
    async fn an_interrupted_open_whose_second_leg_was_rejected_is_hedged_only_after_a_fresh_reconcile()
     {
        let (executor, ledger, lighter, mut position) = interrupted(
            "resume-rejected",
            Refills::new(Venue::LighterRh, dec!(5.3685), 0),
            near_setup(),
        )
        .await;
        venue_settles(&lighter, "resume-rejected-sell-0", Decimal::ZERO).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified,
            "刚核实完：等补完腿之后的那次对账"
        );
        assert_eq!(entries(&lighter), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["resume-rejected-sell-0"].status,
            OrderStatus::Cancelled
        );
        assert_eq!(position.status, PositionStatus::Opening);

        assert_eq!(
            executor.resume_open(&mut position, false).await.unwrap(),
            Resume::Frozen
        );
        assert_eq!(entries(&lighter), 1, "对账不干净不补");

        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(entries(&lighter), 2);
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(position.is_hedged());
        let (replayed, _) = ledger.replay().await.unwrap();
        let chase = &replayed.orders["resume-rejected-sell-1"].order;
        assert_eq!(chase.quantity, Some(first_quantity(&position)));
        // 开仓时没开强制补腿：按默认容差 0.3% 算最差限价。
        assert_eq!(
            Some(chase.limit_price.unwrap()),
            crate::preflight::forced_limit(Side::Buy, dec!(5.36387), RESUME_HEDGE_TOLERANCE)
        );
        assert!(!chase.reduce_only);
        let note = position.note.unwrap();
        assert!(
            note.starts_with(RESUMED_NOTE) && note.contains("补到"),
            "{note}"
        );
    }

    /// 第二腿的开仓单读不到、场所上又有台账不知道的持仓（这张单可能成交了）：不下结论，一张单都不发。
    /// 场所持仓没动的情形见 `an_unreadable_open_order_on_an_unchanged_venue_is_closed_as_unfilled`。
    #[tokio::test]
    async fn an_interrupted_open_whose_order_is_still_unreadable_waits_without_sending() {
        let (executor, _ledger, lighter, mut position) = interrupted(
            "resume-unknown",
            Refills::new(Venue::LighterRh, dec!(5.3685), 0),
            forced_setup(),
        )
        .await;
        *lighter.net.lock().unwrap() = dec!(-149);
        for can_trade in [true, false] {
            let step = executor
                .resume_open(&mut position, can_trade)
                .await
                .unwrap();
            assert!(
                matches!(&step, Resume::Waiting(why) if why.contains("读不到")),
                "{step:?}"
            );
        }
        assert_eq!(entries(&lighter), 1);
        assert_eq!(position.status, PositionStatus::Opening);
    }

    /// 补第二腿在最差限价内一点没吃到：不回滚（用户 10-17 定的），第一腿留着、仓位停在建仓中，下一轮再补；
    /// 盘口补上来了就补齐、转为持仓。
    #[tokio::test]
    async fn a_resumed_hedge_that_gets_nothing_keeps_the_first_leg_and_retries_next_round() {
        // 第 0 张（结果不明的那张）之后，再有 2 张吃不到，第 4 张起成交。
        let (executor, ledger, lighter, mut position) = interrupted(
            "resume-nothing",
            Refills::new(Venue::LighterRh, dec!(5.3685), 3),
            forced_setup(),
        )
        .await;
        venue_settles(&lighter, "resume-nothing-sell-0", Decimal::ZERO).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified
        );
        let step = executor.resume_open(&mut position, true).await.unwrap();
        assert!(
            matches!(&step, Resume::Retry(why) if why.contains("不回滚")),
            "{step:?}"
        );
        assert_eq!(entries(&lighter), 1 + FORCE_HEDGE_ATTEMPTS as usize);
        assert_eq!(position.status, PositionStatus::Opening);
        assert!(position.long.is_some(), "第一腿留着，没有回滚");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.positions["resume-nothing"].status,
            PositionStatus::Opening
        );
        assert!(
            !replayed.orders.keys().any(|id| id.contains("-exit")),
            "没发任何平仓 / 回滚单"
        );

        // 下一轮：盘口补上来了。
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(position.is_hedged());
        assert_eq!(entries(&lighter), 2 + FORCE_HEDGE_ATTEMPTS as usize);
    }

    /// 规则轮里补不齐：每一轮都再补（不退避），报告带着「还没补齐」去告警，第一腿一直留着。
    #[tokio::test]
    async fn every_round_tries_the_missing_second_leg_again_without_backing_off() {
        let (executor, ledger, lighter, _) = interrupted(
            "resume-every-round",
            Refills::new(Venue::LighterRh, dec!(5.3685), 99),
            forced_setup(),
        )
        .await;
        venue_settles(&lighter, "resume-every-round-sell-0", Decimal::ZERO).await;
        let retries = crate::desk::ExitRetries::default();
        let ctx = crate::desk::RoundCtx {
            funding: &crate::desk::PaperFunding,
            retries: &retries,
            exit_confirm_gap: Duration::ZERO,
        };
        let venues = HashMap::new();
        let round = || {
            crate::desk::live_round(
                &executor,
                &ledger,
                &venues,
                &ctx,
                crate::desk::Frozen::Nothing,
            )
        };
        assert!(round().await.unwrap()[0].reverify);
        for n in 1..=2 {
            let report = &round().await.unwrap()[0];
            assert!(report.executed);
            assert!(
                report
                    .error
                    .as_deref()
                    .is_some_and(|why| why.contains("还没补齐")),
                "{report:?}"
            );
            assert_eq!(report.status, PositionStatus::Opening);
            assert_eq!(entries(&lighter), 1 + n * FORCE_HEDGE_ATTEMPTS as usize);
        }
    }

    /// 第二腿已经成交了大部分，还差的不到最小下单额：补不了，也不回滚，带着这点差额转为持仓。
    #[tokio::test]
    async fn a_shortfall_below_the_venue_minimum_is_kept_without_an_order() {
        let (executor, _ledger, lighter, mut position) = interrupted(
            "resume-dust",
            Refills::new(Venue::LighterRh, dec!(5.3685), 0),
            forced_setup(),
        )
        .await;
        let short = first_quantity(&position) - dec!(2);
        venue_settles(&lighter, "resume-dust-sell-0", short).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified
        );
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert!(position.note.as_deref().unwrap().contains("带着差额持有"));
        assert_eq!(entries(&lighter), 1, "没补单");
    }

    /// 第一腿结果不明（第二腿还没发）、仓位一条腿都没记：靠开仓计划知道第二腿在哪家、容差多少。
    /// `first_filled`：第一腿交易所后来查得到的成交数量（0 = 没成交）；`complete` = 全部成交，否则 IOC 剩下的已撤。
    async fn interrupted_first(
        name: &str,
        first_filled: Decimal,
        complete: bool,
    ) -> (Executor, Arc<Ledger>, Arc<Refills>, PairPosition) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let mut position = bare_position(name, PositionStatus::Opening);
        position.symbol = Symbol::perp("NEAR", "USDT");
        position.strategy = Strategy::Spread;
        position.planned = Some(crate::types::PlannedLegs {
            long: Venue::Arcus,
            short: Venue::LighterRh,
            first: Side::Buy,
            force_hedge: Some(dec!(0.005)),
        });
        let order = leg_order(
            ClientOrderId::for_leg(name, Side::Buy, 0),
            near_plan(None).first_leg(),
            Some(dec!(3)),
            crate::MarginMode::Isolated,
            None,
        );
        ledger
            .append(&Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        ledger
            .append(&Record::Order(Box::new(OrderState::new(order.clone()))))
            .await
            .unwrap();
        let arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        let mut venue = OrderState::new(order.clone());
        venue.venue_order_id = Some("a1".into());
        if first_filled > Decimal::ZERO {
            venue.status = if complete {
                OrderStatus::Filled
            } else {
                OrderStatus::Cancelled
            };
            venue.average_price = Some(dec!(5.36387));
            venue.filled_usdt = first_filled * dec!(5.36387);
            venue.fee_usdt = dec!(0.1701);
        } else {
            venue.status = OrderStatus::Cancelled;
        }
        arcus
            .orders
            .lock()
            .await
            .insert(order.client_order_id.0.clone(), venue);
        let lighter = Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(arcus) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        (executor, ledger, lighter, position)
    }

    /// 订单永远读不到（交易所明确拒收、券商却当成结果不明）的场所：`unreadable` 里的订单号读回一律报错；
    /// 新下的单按 `price` 全部成交；持仓接口报 `net`（某合约）。
    struct Unreadable {
        venue: Venue,
        price: Decimal,
        net: Decimal,
        unreadable: Vec<String>,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    #[async_trait]
    impl Broker for Unreadable {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let quantity = order
                .quantity
                .unwrap_or_else(|| (order.notional_usdt / self.price).round_dp(6));
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            state.status = OrderStatus::Filled;
            state.average_price = Some(self.price);
            state.filled_usdt = quantity * self.price;
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            if self.unreadable.contains(&id.0) {
                return Err(ArbError::venue(
                    self.venue.as_str(),
                    "recorded order is unresolved or outside API history; NEVER resubmit it",
                ));
            }
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok((!self.net.is_zero())
                .then(|| crate::broker::VenuePosition {
                    venue: self.venue,
                    symbol: Symbol::perp("NEAR", "USDT"),
                    net_quantity: self.net,
                    average_price: Some(self.price),
                    notional_usdt: self.net.abs() * self.price,
                })
                .into_iter()
                .collect())
        }
    }

    /// 开 某合约：计划多 Arcus（第一腿）/ 空 Lighter RH。Arcus 的开仓单读不到；`second_filled` 时 Lighter 的
    /// 开仓单（两腿同时下）已成交 149 个并记进了仓位。
    async fn unreadable_first(
        name: &str,
        arcus_net: Decimal,
        second_filled: bool,
    ) -> (Executor, Arc<Ledger>, Arc<Unreadable>, PairPosition) {
        let ledger = temp_ledger(name).await;
        let mut position = bare_position(name, PositionStatus::Opening);
        position.symbol = Symbol::perp("NEAR", "USDT");
        position.strategy = Strategy::Spread;
        position.planned = Some(crate::types::PlannedLegs {
            long: Venue::Arcus,
            short: Venue::LighterRh,
            first: Side::Buy,
            force_hedge: Some(dec!(0.005)),
        });
        let first = leg_order(
            ClientOrderId::for_leg(name, Side::Buy, 0),
            near_plan(None).first_leg(),
            Some(dec!(3)),
            crate::MarginMode::Isolated,
            None,
        );
        let mut orders = vec![OrderState::new(first.clone())];
        if second_filled {
            let second = leg_order(
                ClientOrderId::for_leg(name, Side::Sell, 0),
                near_plan(None).second_leg(),
                Some(dec!(3)),
                crate::MarginMode::Isolated,
                Some(dec!(149)),
            );
            let mut filled = OrderState::new(second);
            filled.venue_order_id = Some("l1".into());
            filled.status = OrderStatus::Filled;
            filled.average_price = Some(dec!(5.3685));
            filled.filled_usdt = dec!(149) * dec!(5.3685);
            set_leg(&mut position, &leg_fill(&filled.order, &filled).unwrap());
            orders.push(filled);
        }
        ledger
            .append(&Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        for order in orders {
            ledger
                .append(&Record::Order(Box::new(order)))
                .await
                .unwrap();
        }
        let arcus = Arc::new(Unreadable {
            venue: Venue::Arcus,
            price: dec!(5.36387),
            net: arcus_net,
            unreadable: vec![first.client_order_id.0.clone()],
            orders: Default::default(),
        });
        let lighter = Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&arcus) as Arc<dyn Broker>,
                lighter as Arc<dyn Broker>,
            ],
        );
        (executor, ledger, arcus, position)
    }

    /// 2026-10-31 审查：交易所明确拒收、券商却当成「结果不明」的开仓单，以前每轮只会等、不告警。
    /// 读不到但场所净持仓与台账一致 → 按未成交了结；持仓对不上 → 仍然等，不下结论。
    #[tokio::test]
    async fn an_unreadable_open_order_on_an_unchanged_venue_is_closed_as_unfilled() {
        let (executor, ledger, _arcus, mut position) =
            unreadable_first("unread-flat", Decimal::ZERO, false).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Unwound);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["unread-flat-buy-0"];
        assert_eq!(first.status, OrderStatus::Rejected);
        assert!(
            first
                .reject_reason
                .as_deref()
                .unwrap()
                .contains("净持仓与台账一致")
        );

        // 场所上有台账不知道的持仓（这张单可能成交了）：不下结论，等。
        let (executor, ledger, _arcus, mut position) =
            unreadable_first("unread-moved", dec!(149), false).await;
        let step = executor.resume_open(&mut position, true).await.unwrap();
        assert!(
            matches!(&step, Resume::Waiting(why) if why.contains("不能证明")),
            "{step:?}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders["unread-moved-buy-0"].status.is_live());
    }

    /// 两腿同时下：第二腿（Lighter 空）成交了，第一腿（Arcus 多）核实没成交。把成交的空腿当第一腿，
    /// 去 Arcus 补多腿（补腿不回滚）；以前这里直接交人工，空腿一直裸着。
    #[tokio::test]
    async fn when_only_the_second_leg_filled_the_first_venue_is_chased_instead() {
        let (executor, ledger, arcus, mut position) =
            unreadable_first("unread-swap", Decimal::ZERO, true).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified
        );
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let long = position.long.as_ref().unwrap();
        assert_eq!(long.venue, Venue::Arcus);
        assert_eq!(long.quantity(), Some(dec!(149)));
        let (replayed, _) = ledger.replay().await.unwrap();
        let chase = &replayed.orders["unread-swap-buy-1"].order;
        assert_eq!(chase.venue, Venue::Arcus);
        assert_eq!(chase.quantity, Some(dec!(149)));
        // 最差限价按已成交的空腿均价、开仓时记下的 0.5% 容差算。
        assert_eq!(
            chase.limit_price,
            crate::preflight::forced_limit(Side::Sell, dec!(5.3685), dec!(0.005))
        );
        assert_eq!(arcus.orders.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn an_open_interrupted_at_the_first_leg_gets_its_second_leg_from_the_plan() {
        let (executor, ledger, lighter, mut position) =
            interrupted_first("resume-first", dec!(149), true).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified
        );
        assert_eq!(first_quantity(&position), dec!(149));
        assert_eq!(entries(&lighter), 0);
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (replayed, _) = ledger.replay().await.unwrap();
        let hedge = &replayed.orders["resume-first-sell-0"].order;
        assert_eq!(hedge.venue, Venue::LighterRh);
        assert_eq!(hedge.quantity, Some(dec!(149)));
        // 开仓时记下的容差 0.5%，不是默认的 0.3%。
        assert_eq!(
            Some(hedge.limit_price.unwrap()),
            crate::preflight::forced_limit(Side::Buy, dec!(5.36387), dec!(0.005))
        );
    }

    #[tokio::test]
    async fn an_interrupted_open_that_never_filled_ends_as_unwound_without_orders() {
        let (executor, _ledger, lighter, mut position) =
            interrupted_first("resume-none", Decimal::ZERO, false).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Unwound);
        assert!(position.note.as_deref().unwrap().starts_with(RESUMED_NOTE));
        assert_eq!(entries(&lighter), 0);
    }

    /// 第一腿只成交了一点（不到最小下单额）、第二腿还没发：补不了第二腿，也不回滚，交给人。
    /// 只成交一部分但够下单的，照常按成交的数量补第二腿。
    #[tokio::test]
    async fn a_first_leg_too_small_to_hedge_is_left_for_a_human_not_unwound() {
        let (executor, _ledger, lighter, mut position) =
            interrupted_first("resume-tiny", dec!(2), false).await;
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Verified
        );
        let step = executor.resume_open(&mut position, true).await.unwrap();
        assert!(
            matches!(&step, Resume::Stuck(why) if why.contains("不回滚")),
            "{step:?}"
        );
        assert_eq!(position.status, PositionStatus::Opening);
        assert!(position.long.is_some());
        assert_eq!(entries(&lighter), 0, "没补第二腿");

        let (executor, ledger, lighter, mut position) =
            interrupted_first("resume-partial", dec!(100), false).await;
        executor.resume_open(&mut position, true).await.unwrap();
        assert_eq!(
            executor.resume_open(&mut position, true).await.unwrap(),
            Resume::Done
        );
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&lighter), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["resume-partial-sell-0"].order.quantity,
            Some(dec!(100))
        );
    }

    /// 自己判断不了的（这里：台账记着一条腿，但这一侧没有开仓单）不发单，交给人。
    #[tokio::test]
    async fn an_opening_the_executor_cannot_explain_is_left_for_a_human() {
        let (executor, ledger, lighter, mut position) =
            interrupted_first("resume-stuck", Decimal::ZERO, false).await;
        position.short = Some(LegFill {
            venue: Venue::LighterRh,
            side: Side::Sell,
            notional_usdt: dec!(100),
            average_price: dec!(5.3685),
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId("elsewhere".into()),
            margin_usdt: None,
        });
        ledger
            .append(&Record::Position(Box::new(position.clone())))
            .await
            .unwrap();
        let step = executor.resume_open(&mut position, true).await.unwrap();
        assert!(
            matches!(&step, Resume::Stuck(why) if why.contains("人工")),
            "{step:?}"
        );
        assert_eq!(entries(&lighter), 0);
        assert_eq!(position.status, PositionStatus::Opening);
    }

    /// 规则轮把「这个币有没有被对账冻结」交给续上建仓：同一个币有不一致就不补，别的币有不一致不影响它。
    #[tokio::test]
    async fn the_round_resumes_an_interrupted_open_unless_its_own_symbol_is_frozen() {
        use crate::reconcile::{Divergence, DivergenceKind, Reconciliation};
        let (executor, ledger, lighter, _) = interrupted(
            "resume-round",
            Refills::new(Venue::LighterRh, dec!(5.3685), 0),
            forced_setup(),
        )
        .await;
        venue_settles(&lighter, "resume-round-sell-0", Decimal::ZERO).await;
        let frozen_on = |symbol: &str| Reconciliation {
            divergences: vec![Divergence {
                kind: DivergenceKind::UnknownOpenOrder,
                venue: Some(Venue::LighterRh),
                symbol: Some(Symbol::perp(symbol, "USDT")),
                reference: "x".into(),
                detail: "测试".into(),
            }],
            checked_positions: 0,
            checked_venues: 1,
            elapsed_ms: Vec::new(),
        };
        let retries = crate::desk::ExitRetries::default();
        let ctx = crate::desk::RoundCtx {
            funding: &crate::desk::PaperFunding,
            retries: &retries,
            exit_confirm_gap: Duration::ZERO,
        };
        let near = frozen_on("NEAR");
        let venues = HashMap::new();
        let round = |frozen| crate::desk::live_round(&executor, &ledger, &venues, &ctx, frozen);
        let first = round(crate::desk::Frozen::By(&near)).await.unwrap();
        assert!(first[0].reverify, "核实与补进台账不受冻结影响");
        let second = round(crate::desk::Frozen::By(&near)).await.unwrap();
        assert!(!second[0].executed);
        assert!(
            second[0]
                .skipped
                .as_deref()
                .unwrap()
                .contains("对账还不干净")
        );
        assert_eq!(entries(&lighter), 1);
        // 看板在同一把锁里再对一次账之后只续建仓中断的这几笔（`resume_openings`）。
        let eth = frozen_on("ETH");
        let third =
            crate::desk::resume_openings(&executor, &ledger, &ctx, crate::desk::Frozen::By(&eth))
                .await
                .unwrap();
        assert_eq!(third.len(), 1);
        assert!(third[0].resumed && third[0].executed, "{:?}", third[0]);
        assert_eq!(third[0].status, PositionStatus::Open);
        assert_eq!(entries(&lighter), 2);
    }

    async fn open_with_min_reduce(
        name: &str,
        lighter: Refills,
        min_reduce: Decimal,
    ) -> (ArbResult<ExecutionOutcome>, Arc<Ledger>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let first = Hedging {
            min_reduce,
            ..Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log)
        };
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(first) as Arc<dyn Broker>,
                Arc::new(lighter) as Arc<dyn Broker>,
            ],
        );
        let outcome = executor
            .open(
                &near_plan(Some(dec!(0.000156))),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &near_setup(),
            )
            .await;
        (outcome, ledger)
    }

    fn quantity(leg: Option<&LegFill>) -> Decimal {
        leg.and_then(LegFill::quantity).unwrap()
    }

    /// 第二腿只成交了 97%（差 3%、约 24 USDT，够最小下单额）：以前两腿全回滚；现在退掉第一腿多出来的零头，两腿对等持有。
    #[tokio::test]
    async fn a_lot_size_shortfall_trims_the_first_leg_instead_of_unwinding() {
        let (outcome, ledger) = open_with_min_reduce(
            "lot-trim",
            Refills {
                partial: true,
                partial_ratio: dec!(0.97),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            Decimal::ZERO,
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (long, short) = (
            quantity(position.long.as_ref()),
            quantity(position.short.as_ref()),
        );
        assert!(
            (long - short).abs() / long <= SECOND_LEG_QTY_TOLERANCE,
            "退完两腿对等：{long} vs {short}"
        );
        let note = position.note.unwrap();
        assert!(
            note.starts_with(BALANCED_HEDGE_NOTE) && note.contains("reduce-only 退掉"),
            "{note}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        let undo = &replayed.orders["lot-trim-sell-undo0"];
        assert!(undo.order.reduce_only && undo.status == OrderStatus::Filled);
        assert!(
            !replayed.orders.contains_key("lot-trim-sell-exit1"),
            "没有回滚单"
        );
    }

    /// 某次实盘：第二腿按自己的数量步长只成交了 99.44%（差 0.56%，超过 0.5% 容差）。零头约 4.5 USDT，
    /// 低于各家最小下单额：不发退单（发了也被拒、回读还可能出岔子），带着这点差额持有，不回滚。
    #[tokio::test]
    async fn a_residual_below_the_venue_minimum_is_kept_without_sending_an_order() {
        let (outcome, ledger) = open_with_min_reduce(
            "lot-dust",
            Refills {
                partial: true,
                partial_ratio: dec!(0.9944),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            Decimal::ZERO,
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let note = position.note.unwrap();
        assert!(
            note.starts_with(BALANCED_HEDGE_NOTE) && note.contains("退不掉"),
            "{note}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(
            !replayed.orders.contains_key("lot-dust-sell-undo0"),
            "零头太小，不发退单"
        );
        assert!(
            !replayed.orders.contains_key("lot-dust-sell-exit1"),
            "没有回滚单"
        );
    }

    /// 零头够最小下单额、但交易所还是拒了（差 3%、约 24 USDT）：在可以带着的范围内，持有不回滚。
    #[tokio::test]
    async fn a_rejected_trim_with_a_small_residual_is_kept_not_unwound() {
        let (outcome, ledger) = open_with_min_reduce(
            "lot-keep",
            Refills {
                partial: true,
                partial_ratio: dec!(0.97),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            dec!(1000),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let note = position.note.unwrap();
        assert!(
            note.starts_with(BALANCED_HEDGE_NOTE) && note.contains("退不掉"),
            "{note}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["lot-keep-sell-undo0"].status,
            OrderStatus::Rejected
        );
        assert!(!replayed.orders.contains_key("lot-keep-sell-exit1"));
    }

    /// 第二腿只成交了 80%、多出来的 20% 又退不掉：这么大的裸敞口不能带着，照旧回滚。
    #[tokio::test]
    async fn a_large_residual_that_cannot_be_trimmed_still_unwinds() {
        let (outcome, _) = open_with_min_reduce(
            "lot-big",
            Refills {
                partial: true,
                partial_ratio: dec!(0.8),
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            // 第一腿约 149 个：多出来的 30 个退不掉（< 60），整条腿回滚退得掉。
            dec!(60),
        )
        .await;
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
    }

    /// 第一腿下单用的是按保本线封顶的限价（`Plan::first_leg_capped`）：Arcus 的卖盘在下单前跳到 5.3700，
    /// 原限价（5.373726）照样买得到，但 Lighter 按预估 5.3685 卖出去就锁不住保本线 —— 第一腿不成交，什么都不花，
    /// 第二腿根本不发。
    #[tokio::test]
    async fn a_first_leg_whose_price_ran_past_the_breakeven_cap_is_not_filled_and_nothing_else_is_sent()
     {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("first-capped").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(Venue::Arcus, dec!(5.3700), dec!(0.1701), &log))
                    as Arc<dyn Broker>,
                Arc::new(Hedging::new(
                    Venue::LighterRh,
                    dec!(5.3685),
                    Decimal::ZERO,
                    &log,
                )),
            ],
        );
        let plan = near_plan(Some(dec!(0.000156)));
        assert_eq!(plan.first_leg().venue, Venue::Arcus);
        assert!(dec!(5.3700) < plan.long.limit_price, "原限价买得到");
        let position = executor
            .open(
                &plan,
                Strategy::Spread,
                dec!(0.1268),
                "first-capped",
                &near_setup(),
            )
            .await
            .unwrap()
            .position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(position.long.is_none() && position.short.is_none());
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["first-capped-buy-0"];
        assert_eq!(
            first.order.limit_price,
            Some(plan.first_leg_capped().limit_price)
        );
        assert!(first.order.limit_price.unwrap() < dec!(5.3700));
        assert!(
            !replayed.orders.keys().any(|id| id.contains("-sell-")),
            "第二腿不该发：{:?}",
            replayed.orders.keys().collect::<Vec<_>>()
        );
    }

    /// 第二腿是**买**（先卖 Lighter、后买 Arcus）：追单的限价是「最多买到多高」，锁定价差同样不低于 −X%。
    #[tokio::test]
    async fn a_forced_hedge_caps_the_price_a_second_buy_may_pay() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("force-buy").await;
        // 先卖 Lighter（费率 0）@5.3685（预估价，过得了第一腿的保本封顶）；Arcus 的卖盘已经涨到 5.3800：
        // 保本价（约 5.3677）买不到，−0.3% 以内（约 5.3846）买得到。
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::LighterRh,
                    dec!(5.3685),
                    Decimal::ZERO,
                    &log,
                )) as Arc<dyn Broker>,
                Arc::new(Hedging::new(Venue::Arcus, dec!(5.3800), dec!(0.1701), &log)),
            ],
        );
        let mut plan = near_plan(Some(dec!(0.000156)));
        plan.long.fee = dec!(0.000225);
        plan.leg_order = crate::preflight::LegOrder::CheapFirst;
        assert_eq!(plan.first_leg().venue, Venue::LighterRh);
        let outcome = executor
            .open(
                &plan,
                Strategy::Spread,
                dec!(0.1268),
                "force-buy",
                &forced_setup(),
            )
            .await
            .unwrap();
        let position = outcome.position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (replayed, _) = ledger.replay().await.unwrap();
        let breakeven = replayed.orders["force-buy-buy-0"]
            .order
            .limit_price
            .unwrap();
        let forced = replayed.orders["force-buy-buy-1"]
            .order
            .limit_price
            .unwrap();
        assert!(
            forced > breakeven,
            "最多买到的价比保本价高：{forced} > {breakeven}"
        );
        let locked = (dec!(5.3685) - forced) / ((forced + dec!(5.3685)) / Decimal::TWO);
        assert!(
            locked >= dec!(-0.003) && locked < dec!(-0.003) + dec!(0.000000001),
            "{locked}"
        );
        assert!(position.note.unwrap().starts_with(FORCED_HEDGE_NOTE));
        // 市场再涨过 −0.3%（5.40）：追单也买不到，回滚，不无限追。
        let ledger = temp_ledger("force-buy-gap").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::LighterRh,
                    dec!(5.36387),
                    Decimal::ZERO,
                    &log,
                )) as Arc<dyn Broker>,
                Arc::new(Hedging::new(Venue::Arcus, dec!(5.40), dec!(0.1701), &log)),
            ],
        );
        let outcome = executor
            .open(
                &plan,
                Strategy::Spread,
                dec!(0.1268),
                "force-buy-gap",
                &forced_setup(),
            )
            .await
            .unwrap();
        assert_eq!(outcome.position.status, PositionStatus::Unwound);
    }

    /// Arcus（费率 0.0225%）↔ Lighter（0）：`leg_order` 下开 某合约；Lighter 按 `lighter` 的脚本走，Arcus 在 5.36387 成交。
    async fn open_with_lighter(
        name: &str,
        lighter: Refills,
        leg_order: crate::preflight::LegOrder,
        floor: Option<Decimal>,
        notional: Decimal,
    ) -> (PairPosition, Arc<Ledger>, Arc<Refills>) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger(name).await;
        let lighter = Arc::new(lighter);
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Hedging::new(
                    Venue::Arcus,
                    dec!(5.36387),
                    dec!(0.1701),
                    &log,
                )) as Arc<dyn Broker>,
                Arc::clone(&lighter) as Arc<dyn Broker>,
            ],
        );
        let mut plan = near_plan(floor);
        plan.long.fee = dec!(0.000225);
        plan.leg_order = leg_order;
        plan.long.notional_usdt = notional;
        plan.short.notional_usdt = notional;
        let outcome = executor
            .open(&plan, Strategy::Spread, dec!(0.1268), name, &near_setup())
            .await
            .unwrap();
        (outcome.position, ledger, lighter)
    }

    #[tokio::test]
    async fn the_cheap_leg_goes_first_and_a_partial_fill_is_hedged_instead_of_unwound() {
        let (position, ledger, lighter) = open_with_lighter(
            "cheap-partial",
            Refills {
                partial: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            crate::preflight::LegOrder::CheapFirst,
            Some(dec!(0.000156)),
            dec!(800),
        )
        .await;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&lighter), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        // Lighter 先下（卖 0 号单），只成交了一半；Arcus 按这一半的数量买。
        let sell = &replayed.orders["cheap-partial-sell-0"];
        let buy = &replayed.orders["cheap-partial-buy-0"];
        assert!(sell.filled_usdt > Decimal::ZERO && sell.filled_usdt < dec!(800));
        let sold = sell.filled_usdt / dec!(5.3685);
        assert_eq!(
            buy.order.quantity.map(|q| q.round_dp(4)),
            Some(sold.round_dp(4))
        );
        let note = position.note.unwrap();
        assert!(
            note.contains("只成交了") && note.contains("lighter-rh"),
            "{note}"
        );
    }

    #[tokio::test]
    async fn a_partial_first_leg_too_small_to_hedge_or_on_a_funding_plan_is_still_unwound() {
        // 30 USDT 的一半 = 15 USDT，凑不够一张对冲单：回滚。
        let (position, _, _) = open_with_lighter(
            "cheap-partial-small",
            Refills {
                partial: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            crate::preflight::LegOrder::CheapFirst,
            Some(dec!(0.000156)),
            dec!(30),
        )
        .await;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        // 资金费单（没有保本线）：部分成交照旧回滚。
        let (position, _, _) = open_with_lighter(
            "cheap-partial-funding",
            Refills {
                partial: true,
                ..Refills::new(Venue::LighterRh, dec!(5.3685), 0)
            },
            crate::preflight::LegOrder::CheapFirst,
            None,
            dec!(800),
        )
        .await;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
    }

    #[tokio::test]
    async fn a_second_leg_that_cannot_reach_the_breakeven_price_is_not_filled_and_the_first_leg_is_unwound()
     {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        // 当时的样子：Arcus 买在 5.36387，Lighter 买一已经掉到 5.3613。
        let arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        let (position, ledger) = open_near(
            "near-breakeven",
            Some(dec!(0.000156)),
            arcus,
            dec!(5.3613),
            &log,
        )
        .await;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(position.long.is_none() && position.short.is_none());
        let note = position.note.as_deref().unwrap();
        assert!(note.contains("已成交腿已全部回滚"), "{note}");
        assert!(
            note.contains("保本价") && note.contains("5.36387"),
            "{note}"
        );
        // Lighter 那张单的限价是按 Arcus 成交价反推的保本价，不再是 5.357763。
        let (replayed, _) = ledger.replay().await.unwrap();
        let sell = &replayed.orders["near-breakeven-sell-0"];
        let limit = sell.order.limit_price.unwrap();
        assert!(limit > dec!(5.3613) && limit < dec!(5.3685), "{limit}");
        assert_eq!(sell.filled_usdt, Decimal::ZERO);
        // Arcus 那条腿按反向单平掉了。
        assert!(
            replayed
                .orders
                .values()
                .any(|order| order.order.venue == Venue::Arcus
                    && order.order.reduce_only
                    && order.filled_usdt > Decimal::ZERO)
        );

        // 不设保本线（资金费单的老样子）：同样的盘口照旧成交，锁定价差为负。
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        let (position, _) = open_near("near-no-floor", None, arcus, dec!(5.3613), &log).await;
        assert_eq!(position.status, PositionStatus::Open);
        assert_eq!(position.short.as_ref().unwrap().average_price, dec!(5.3613));

        // 盘口还在保本价以上：照常成交。
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        let (position, _) =
            open_near("near-ok", Some(dec!(0.000156)), arcus, dec!(5.3670), &log).await;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
    }

    #[tokio::test]
    async fn a_hedge_first_leg_sends_the_second_leg_before_its_fees_are_settled() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        // 待核实：均价往不利方向放一位，手续费按费率估。
        arcus.provisional = Some((dec!(5.364), dec!(0.17)));
        arcus.settle_delay = std::time::Duration::from_millis(300);
        let (position, ledger) = open_near(
            "near-quick",
            Some(dec!(0.000156)),
            arcus,
            dec!(5.3670),
            &log,
        )
        .await;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);

        // 第二腿在第一腿核实完之前就发出去了。
        let log = log.lock().unwrap().clone();
        assert_eq!(
            log,
            ["place:arcus", "place:lighter-rh", "settled:arcus"],
            "{log:?}"
        );

        // 仓位、台账、执行报告都按核实后的最终成交。
        let long = position.long.as_ref().unwrap();
        assert_eq!(long.average_price, dec!(5.36387));
        assert_eq!(long.fee_usdt, dec!(0.1701));
        let short = position.short.as_ref().unwrap();
        assert_eq!(short.quantity(), long.quantity());
        let report = position.open_report.as_ref().unwrap();
        assert_eq!(report.first.actual_price, dec!(5.36387));
        assert_eq!(report.fees_usdt, dec!(0.1701));
        // 「两腿间隔」到第二腿成交为止，不含第一腿核实的 300 毫秒。
        assert!(report.unhedged_ms < 250, "{}", report.unhedged_ms);
        let (replayed, _) = ledger.replay().await.unwrap();
        let buy = &replayed.orders["near-quick-buy-0"];
        assert_eq!(buy.status, OrderStatus::Filled);
        assert_eq!(buy.average_price, Some(dec!(5.36387)));
        assert_eq!(buy.fee_usdt, dec!(0.1701));
        assert_eq!(
            replayed.positions["near-quick"]
                .long
                .as_ref()
                .unwrap()
                .average_price,
            dec!(5.36387)
        );
    }

    /// 第二腿明确没成交时不等第一腿核实（读节点滞后时要十几秒）：马上回滚，回滚完再把第一腿的最终状态落进台账。
    #[test]
    fn deterministic_rejections_are_not_retried_but_liquidity_misses_are() {
        for pointless in [
            "FILL_WILL_EXCEED_TRADING_BOUND",
            "canceled-margin-not-allowed",
            "canceled-position-not-allowed",
            "Price too far from oracle asset=110016",
            "Insufficient margin to place order",
            "可用保证金不够",
            "数量取整后低于交易所最小下单量",
        ] {
            assert!(pointless_retry(pointless), "{pointless}");
        }
        for worth in [
            "iocCancelRejected",
            "Order could not immediately match against any resting orders. asset=110016",
            "canceled-not-enough-liquidity",
            "IOC 在限价以内没有对手盘",
            "venue reported EXPIRED",
        ] {
            assert!(!pointless_retry(worth), "{worth}");
        }
    }

    #[tokio::test]
    async fn a_missed_second_leg_unwinds_before_the_first_leg_is_settled() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        arcus.provisional = Some((dec!(5.364), dec!(0.17)));
        // 读节点滞后：核实要 3 秒，比第二腿的同价重试窗口还长。
        arcus.settle_delay = std::time::Duration::from_secs(3);
        // lighter-rh 盘口 5.30：卖单在保本价内吃不到。
        let started = std::time::Instant::now();
        let (position, ledger) = open_near(
            "near-miss-quick",
            Some(dec!(0.000156)),
            arcus,
            dec!(5.30),
            &log,
        )
        .await;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "没等第一腿 3 秒的核实：{:?}",
            started.elapsed()
        );
        let log = log.lock().unwrap().clone();
        assert_eq!(
            log.last().map(String::as_str),
            Some("place:arcus"),
            "回滚了第一腿：{log:?}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        let buy = &replayed.orders["near-miss-quick-buy-0"];
        assert_eq!(buy.status, OrderStatus::Filled, "第一腿不能停在 Pending");
        assert_eq!(
            buy.average_price,
            Some(dec!(5.36387)),
            "落的是核实后的最终状态"
        );
        // 仓位上的入场价与手续费也换成核实后的：回滚按 5.36387 平回去，价格盈亏 0；手续费 = 入场实收 + 回滚。
        let position = &replayed.positions["near-miss-quick"];
        assert_eq!(position.realized_pnl_usdt.round_dp(10), Decimal::ZERO);
        assert_eq!(position.realized_fee_usdt, dec!(0.1701) + dec!(0.1701));
    }

    #[tokio::test]
    async fn a_first_leg_that_cannot_be_settled_is_recorded_with_its_estimate() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        arcus.provisional = Some((dec!(5.364), dec!(0.17)));
        arcus.settle_error = true;
        let (position, ledger) = open_near(
            "near-unsettled",
            Some(dec!(0.000156)),
            arcus,
            dec!(5.3670),
            &log,
        )
        .await;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let long = position.long.as_ref().unwrap();
        assert_eq!(long.average_price, dec!(5.364));
        assert_eq!(long.fee_usdt, dec!(0.17));
        // 台账里不留 Pending：对账会把它当成交易所上没有的挂单。
        let (replayed, _) = ledger.replay().await.unwrap();
        let buy = &replayed.orders["near-unsettled-buy-0"];
        assert_eq!(buy.status, OrderStatus::Filled);
        assert_eq!(buy.average_price, Some(dec!(5.364)));
    }

    #[tokio::test]
    async fn an_add_merges_the_settled_first_leg_once() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("near-add-quick").await;
        let mut arcus = Hedging::new(Venue::Arcus, dec!(5.36387), dec!(0.1701), &log);
        arcus.provisional = Some((dec!(5.364), dec!(0.17)));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(arcus) as Arc<dyn Broker>,
                Arc::new(Hedging::new(
                    Venue::LighterRh,
                    dec!(5.3670),
                    Decimal::ZERO,
                    &log,
                )),
            ],
        );
        let plan = near_plan(Some(dec!(0.000156)));
        let mut position = executor
            .open(
                &plan,
                Strategy::Spread,
                dec!(0.1268),
                "near-add-quick",
                &near_setup(),
            )
            .await
            .unwrap()
            .position;
        let opened = position.long.clone().unwrap();
        assert!(
            executor
                .increase(&mut position, &plan, dec!(0.1268))
                .await
                .unwrap()
        );
        let long = position.long.as_ref().unwrap();
        // 待核实的那份不留在腿里：名义、手续费各是两笔最终成交之和，均价不变。
        assert_eq!(long.notional_usdt, opened.notional_usdt * Decimal::TWO);
        assert_eq!(long.fee_usdt, dec!(0.3402));
        assert_eq!(long.average_price.round_dp(10), dec!(5.36387));
        assert_eq!(
            position
                .short
                .as_ref()
                .unwrap()
                .quantity()
                .unwrap()
                .round_dp(8),
            long.quantity().unwrap().round_dp(8)
        );
    }

    /// Lighter 买腿：最优价 100，吃深度后预估 100.2（滑点最大，先执行）；Arcus 卖腿：最优价 100.1。
    fn open_plan() -> Plan {
        let leg = |venue, side, best: Decimal, expected: Decimal, limit| LegPlan {
            symbol: Symbol::perp("BTC", "USDT"),
            venue,
            side,
            notional_usdt: dec!(1000),
            limit_price: limit,
            best_price: best,
            expected_price: expected,
            slippage: (expected - best).abs() / best,
            book_notional: dec!(100000),
            fee: Decimal::ZERO,
            maker_rest_ms: None,
        };
        Plan {
            symbol: Symbol::perp("BTC", "USDT"),
            long: leg(
                Venue::LighterRh,
                Side::Buy,
                dec!(100),
                dec!(100.2),
                dec!(100.4),
            ),
            short: leg(
                Venue::Arcus,
                Side::Sell,
                dec!(100.1),
                dec!(100.1),
                dec!(99.9),
            ),
            expected_cost: dec!(0.002),
            min_locked_basis: None,
            leg_order: crate::preflight::LegOrder::Slippage,
        }
    }

    async fn temp_ledger(name: &str) -> Arc<Ledger> {
        let path = std::env::temp_dir().join(format!("arb-{name}-{}.jsonl", std::process::id()));
        let _ = tokio::fs::remove_file(&path).await;
        Arc::new(Ledger::open(&path).await.unwrap())
    }

    #[tokio::test]
    async fn opening_prepares_both_legs_first_and_records_the_plan_against_the_fills() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("open-report").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Logged::new(
                    Venue::LighterRh,
                    dec!(100.3),
                    Decimal::ZERO,
                    &log,
                )) as Arc<dyn Broker>,
                Arc::new(Logged::new(Venue::Arcus, dec!(100.05), dec!(0.225), &log)),
            ],
        );
        let setup = PositionSetup {
            margin_mode: crate::MarginMode::Isolated,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            quoted_at: Some(Utc::now() - chrono::Duration::seconds(2)),
            positions_checked: false,
            force_hedge: None,
        };
        let outcome = executor
            .open(
                &open_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "t-open",
                &setup,
            )
            .await
            .unwrap();
        assert!(outcome.is_open(), "{:?}", outcome.position.note);

        // 两条腿的准备都在第一笔订单之前，然后先 Lighter（滑点大的先做）、后 Arcus。
        let log = log.lock().unwrap().clone();
        assert_eq!(log.len(), 4, "{log:?}");
        assert!(
            log[..2].iter().all(|entry| entry.starts_with("prepare:")),
            "{log:?}"
        );
        assert_eq!(log[2..], ["place:lighter-rh", "place:arcus"]);

        let report = outcome
            .position
            .open_report
            .as_ref()
            .expect("成功开仓要附执行报告");
        assert_eq!(report.first.venue, Venue::LighterRh);
        // 实际 100.3 对预估 100.2：多付约 0.1%；对最优价 100：多付 0.3%。
        assert_eq!(report.first.vs_expected.round_dp(4), dec!(0.001));
        assert_eq!(report.first.vs_best.round_dp(4), dec!(0.003));
        // Arcus 卖在 100.05，比预估 100.1 低 0.05%。
        assert_eq!(report.second.vs_expected.round_dp(4), dec!(0.0005));
        assert!(report.first.quote_to_fill_ms.unwrap() >= 2000);
        assert!(report.total_ms >= report.unhedged_ms);
        assert_eq!(report.fees_usdt, dec!(0.225));

        // 报告随仓位一起落了盘：重放台账还在。
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.positions["t-open"].open_report.is_some());
    }

    #[tokio::test]
    async fn a_leg_that_cannot_be_prepared_stops_the_open_before_any_order() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("open-prepare-fails").await;
        let mut arcus = Logged::new(Venue::Arcus, dec!(100.1), Decimal::ZERO, &log);
        arcus.prepare_error = Some("交易所拒绝设置杠杆");
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Logged::new(
                    Venue::LighterRh,
                    dec!(100.2),
                    Decimal::ZERO,
                    &log,
                )) as Arc<dyn Broker>,
                Arc::new(arcus),
            ],
        );
        let setup = PositionSetup {
            leverage: Some(dec!(3)),
            ..PositionSetup::default()
        };
        let outcome = executor
            .open(
                &open_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "t-stop",
                &setup,
            )
            .await
            .unwrap();
        assert_eq!(outcome.position.status, PositionStatus::Unwound);
        assert!(outcome.position.long.is_none() && outcome.position.short.is_none());
        assert!(outcome.position.open_report.is_none());
        let note = outcome.position.note.as_deref().unwrap();
        assert!(
            note.contains("没有下任何单") && note.contains("交易所拒绝设置杠杆"),
            "{note}"
        );
        // 没有任何一笔订单发出去：这正是预热放到第一腿之前的意义。
        assert!(
            log.lock()
                .unwrap()
                .iter()
                .all(|entry| !entry.starts_with("place:")),
            "{:?}",
            log.lock().unwrap()
        );
    }

    fn bare_position(id: &str, status: PositionStatus) -> PairPosition {
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol: Symbol::perp("BTC", "USDT"),
            strategy: Strategy::Funding,
            long: None,
            short: None,
            entry_basis_pct: Decimal::ZERO,
            expected_round_trip_cost: Decimal::ZERO,
            status,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        }
    }

    /// 开仓在第一腿成交前就中断（进程死在意图落盘之后）：这不是「平掉了一笔仓位」。
    /// 按已平仓记会在当日盈亏里多出一笔盈亏为 0 的假交易。
    #[tokio::test]
    async fn an_opening_that_never_traded_ends_as_unwound_not_as_a_zero_pnl_close() {
        let ledger = temp_ledger("never-traded").await;
        let executor = Executor::new(Arc::clone(&ledger), vec![]);
        let mut position = bare_position("nt-1", PositionStatus::Opening);

        executor.close(&mut position).await.unwrap();

        assert_eq!(position.status, PositionStatus::Unwound);
        assert!(position.closed_at.is_some());
        assert!(
            position.realized_source.is_none(),
            "没有成交，没有已实现盈亏的出处"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(replayed.positions["nt-1"].status, PositionStatus::Unwound);
    }

    /// 订单有成交、仓位却没记腿（死在「成交」与「写入仓位」之间）：台账之外有敞口，
    /// 不能宣布没事（Unwound），也不能按台账自动平 —— 留着，报错，由人对账。
    #[tokio::test]
    async fn an_opening_whose_order_filled_but_has_no_legs_is_left_for_a_human() {
        let ledger = temp_ledger("filled-no-legs").await;
        let mut filled = OrderState::new(NewOrder {
            post_only: false,
            margin_mode: crate::MarginMode::Isolated,
            client_order_id: ClientOrderId("fl-1-buy-0".into()),
            venue: Venue::Binance,
            symbol: Symbol::perp("BTC", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(1000),
            quantity: None,
            limit_price: None,
            reduce_only: false,
            leverage: None,
        });
        filled.status = OrderStatus::Filled;
        filled.filled_usdt = dec!(1000);
        filled.average_price = Some(dec!(100));
        ledger
            .append(&Record::Order(Box::new(filled)))
            .await
            .unwrap();
        let executor = Executor::new(Arc::clone(&ledger), vec![]);
        let mut position = bare_position("fl-1", PositionStatus::Opening);

        let result = executor.close(&mut position).await;

        assert!(result.is_err(), "有成交而没有腿：必须报错，不能当成没事");
        assert_eq!(position.status, PositionStatus::Opening);
        assert!(position.note.as_deref().unwrap().contains("人工核对"));
        assert!(position.closed_at.is_none());
    }

    /// 只会补保证金的桩券商：结果由测试给定，并数发出了几次请求。
    struct MarginStub {
        venue: Venue,
        outcome: std::sync::Mutex<Option<ArbResult<crate::broker::MarginOutcome>>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl MarginStub {
        fn new(venue: Venue, outcome: ArbResult<crate::broker::MarginOutcome>) -> Arc<Self> {
            Arc::new(Self {
                venue,
                outcome: std::sync::Mutex::new(Some(outcome)),
                calls: Default::default(),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Broker for MarginStub {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        fn supports_add_margin(&self) -> bool {
            true
        }
        async fn add_margin(
            &self,
            _symbol: &Symbol,
            _amount: Decimal,
        ) -> ArbResult<crate::broker::MarginOutcome> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.outcome
                .lock()
                .unwrap()
                .take()
                .expect("桩只应答一次：一次调用只许发一次写请求")
        }
        async fn place(&self, _order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            Err(ArbError::config("不下单"))
        }
        async fn order_state(&self, _id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(None)
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    fn margin_position() -> PairPosition {
        PairPosition {
            long: Some(leg(Venue::Lighter, Side::Buy)),
            short: Some(leg(Venue::Hyperliquid, Side::Sell)),
            rules: TaskRules {
                auto_margin_pct: Some(dec!(12)),
                auto_margin_max_usdt: Some(dec!(100)),
                ..TaskRules::default()
            },
            ..bare_position("m1", PositionStatus::Open)
        }
    }

    /// 自动加保证金的记账：上限先落盘再发请求；明确没动钱的退回，结果不明的不退。
    #[tokio::test]
    async fn add_margin_books_the_cap_before_sending_and_only_refunds_what_never_moved() {
        use crate::broker::MarginOutcome::{Applied, Refused, Unknown};
        let cases: [(&str, ArbResult<crate::broker::MarginOutcome>, Decimal, bool); 4] = [
            ("到账", Ok(Applied), dec!(30), true),
            (
                "明确拒绝",
                Ok(Refused("可用资金不足".into())),
                dec!(0),
                false,
            ),
            ("结果不明", Ok(Unknown("超时".into())), dec!(30), false),
            ("没发出", Err(ArbError::config("只读模式")), dec!(0), false),
        ];
        for (name, outcome, counted, leg_grows) in cases {
            let ledger = temp_ledger(&format!("add-margin-{name}")).await;
            let stub = MarginStub::new(Venue::Hyperliquid, outcome);
            let executor = Executor::new(
                Arc::clone(&ledger),
                vec![Arc::clone(&stub) as Arc<dyn Broker>],
            );
            let mut position = margin_position();
            let before = position.short.as_ref().unwrap().margin_usdt.unwrap();

            let result = executor
                .add_margin(&mut position, Venue::Hyperliquid, dec!(30), "测试")
                .await;

            assert_eq!(stub.calls(), 1, "{name}：一次调用只发一次写请求");
            assert_eq!(result.is_err(), name == "没发出", "{name}");
            assert_eq!(position.margin_added_usdt, counted, "{name}：累计补了多少");
            let after = position.short.as_ref().unwrap().margin_usdt.unwrap();
            assert_eq!(
                after,
                if leg_grows { before + dec!(30) } else { before },
                "{name}：腿上的保证金"
            );
            // 落盘的与内存里一致：重启后上限不会被重置。
            let (replayed, broken) = ledger.replay().await.unwrap();
            assert_eq!(broken, 0);
            assert_eq!(
                replayed.positions["m1"].margin_added_usdt, counted,
                "{name}：台账"
            );
            assert!(position.note.is_some(), "{name}：要留下说明");
        }
    }

    /// 上限是第二道保险：规则评估夹过之后，执行器还会再校验一遍；超了就连请求都不发。
    #[tokio::test]
    async fn add_margin_refuses_to_exceed_the_cap_or_to_run_without_the_rule() {
        let ledger = temp_ledger("add-margin-cap").await;
        let stub = MarginStub::new(
            Venue::Hyperliquid,
            Ok(crate::broker::MarginOutcome::Applied),
        );
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![Arc::clone(&stub) as Arc<dyn Broker>],
        );
        let mut position = margin_position();
        position.margin_added_usdt = dec!(80);
        assert!(
            executor
                .add_margin(&mut position, Venue::Hyperliquid, dec!(30), "超上限")
                .await
                .is_err()
        );
        assert_eq!(position.margin_added_usdt, dec!(80));

        let mut without = margin_position();
        without.rules = TaskRules::default();
        assert!(
            executor
                .add_margin(&mut without, Venue::Hyperliquid, dec!(1), "没开规则")
                .await
                .is_err()
        );
        // 不是这笔仓位的场所 / 不是 Open：同样不发请求。
        assert!(
            executor
                .add_margin(&mut position, Venue::Binance, dec!(1), "没有这条腿")
                .await
                .is_err()
        );
        let mut closing = margin_position();
        closing.status = PositionStatus::Closing;
        assert!(
            executor
                .add_margin(&mut closing, Venue::Hyperliquid, dec!(1), "不是 Open")
                .await
                .is_err()
        );
        assert_eq!(stub.calls(), 0, "上面每一种都不该打到交易所");
    }

    /// 开仓时开了自动加保证金，两条腿的场所都得接入补保证金：不接入就在下第一笔单之前拒绝。
    #[tokio::test]
    async fn opening_with_auto_margin_needs_both_venues_to_support_it() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ledger = temp_ledger("open-auto-margin").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::new(Logged::new(
                    Venue::LighterRh,
                    dec!(100.2),
                    Decimal::ZERO,
                    &log,
                )) as Arc<dyn Broker>,
                Arc::new(Logged::new(Venue::Arcus, dec!(100.1), Decimal::ZERO, &log)),
            ],
        );
        let setup = PositionSetup {
            leverage: Some(dec!(3)),
            rules: TaskRules {
                auto_margin_pct: Some(dec!(12)),
                auto_margin_max_usdt: Some(dec!(100)),
                ..TaskRules::default()
            },
            ..PositionSetup::default()
        };
        let error = executor
            .open(
                &open_plan(),
                Strategy::Funding,
                Decimal::ZERO,
                "t-am",
                &setup,
            )
            .await
            .err()
            .expect("没接入补保证金，必须拒绝");
        assert!(error.to_string().contains("没有接入补保证金"), "{error}");
        assert!(
            log.lock().unwrap().is_empty(),
            "一笔单都不能发：{:?}",
            log.lock().unwrap()
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.positions.is_empty(), "也不留一条 Opening 仓位");
    }

    /// 补仓测试用的券商：按固定价成交（按名义下的单数量 = 名义 ÷ 价）；订单号含 `reject` 的一律不成交。
    struct AddBroker {
        venue: Venue,
        price: Decimal,
        fee: Decimal,
        reject: Option<&'static str>,
        /// 补仓单（订单号含 `-add`）少成交这么多个：模拟「第二腿只差一点零头」。
        add_shortfall: Decimal,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    impl AddBroker {
        fn short_on_adds(venue: Venue, price: Decimal, shortfall: Decimal) -> Arc<Self> {
            Arc::new(Self {
                venue,
                price,
                fee: Decimal::ZERO,
                reject: None,
                add_shortfall: shortfall,
                orders: Default::default(),
            })
        }

        fn new(
            venue: Venue,
            price: Decimal,
            fee: Decimal,
            reject: Option<&'static str>,
        ) -> Arc<Self> {
            Arc::new(Self {
                venue,
                price,
                fee,
                reject,
                add_shortfall: Decimal::ZERO,
                orders: Default::default(),
            })
        }
    }

    #[async_trait]
    impl Broker for AddBroker {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            if self
                .reject
                .is_some_and(|tag| order.client_order_id.0.contains(tag))
            {
                state.status = OrderStatus::Rejected;
                state.reject_reason = Some("测试：不成交".into());
            } else {
                let mut quantity = order.quantity.unwrap_or(order.notional_usdt / self.price);
                if order.client_order_id.0.contains("-add") {
                    quantity -= self.add_shortfall;
                }
                state.status = OrderStatus::Filled;
                state.average_price = Some(self.price);
                state.filled_usdt = quantity * self.price;
                state.fee_usdt = self.fee;
            }
            let status = state.status;
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            Ok(self.orders.lock().await.get(&id.0).cloned())
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 一笔 10 个的价差仓位：多 LighterRh @100、空 Arcus @101，3 倍逐仓；开仓单照执行器的样子落盘。
    async fn hedged_for_add(ledger: &Ledger, id: &str) -> PairPosition {
        let symbol = Symbol::perp("NEAR", "USDT");
        let long = LegFill {
            notional_usdt: dec!(1000),
            average_price: dec!(100),
            client_order_id: ClientOrderId::for_leg(id, Side::Buy, 0),
            ..leg(Venue::LighterRh, Side::Buy)
        };
        let short = LegFill {
            notional_usdt: dec!(1010),
            average_price: dec!(101),
            fee_usdt: dec!(0.5),
            client_order_id: ClientOrderId::for_leg(id, Side::Sell, 0),
            ..leg(Venue::Arcus, Side::Sell)
        };
        for leg in [&long, &short] {
            let mut state = OrderState::new(NewOrder {
                post_only: false,
                client_order_id: leg.client_order_id.clone(),
                venue: leg.venue,
                symbol: symbol.clone(),
                side: leg.side,
                notional_usdt: leg.notional_usdt,
                quantity: None,
                limit_price: None,
                reduce_only: false,
                leverage: Some(dec!(3)),
                margin_mode: crate::MarginMode::Isolated,
            });
            state.status = OrderStatus::Filled;
            state.filled_usdt = leg.notional_usdt;
            state.average_price = Some(leg.average_price);
            state.fee_usdt = leg.fee_usdt;
            ledger
                .append(&crate::Record::Order(Box::new(state)))
                .await
                .unwrap();
        }
        PairPosition {
            margin_mode: crate::MarginMode::Isolated,
            id: id.into(),
            symbol,
            strategy: Strategy::Spread,
            long: Some(long),
            short: Some(short),
            entry_basis_pct: dec!(1),
            expected_round_trip_cost: dec!(0.001),
            status: PositionStatus::Open,
            opened_at: Utc::now(),
            closed_at: None,
            note: None,
            leverage: Some(dec!(3)),
            rules: TaskRules::default(),
            trims: 0,
            exits: 0,
            margin_added_usdt: Decimal::ZERO,
            realized_pnl_usdt: Decimal::ZERO,
            realized_fee_usdt: Decimal::ZERO,
            realized_source: None,
            realized_funding_usdt: None,
            funding_checked_at: None,
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
            planned: None,
        }
    }

    /// 补 495 USDT：LighterRh 买腿滑点大，先做；Arcus 卖腿按第一腿成交数量对冲。
    fn add_plan() -> Plan {
        let leg = |venue, side, slippage| LegPlan {
            symbol: Symbol::perp("NEAR", "USDT"),
            venue,
            side,
            notional_usdt: dec!(495),
            limit_price: dec!(100),
            best_price: dec!(100),
            expected_price: dec!(100),
            slippage,
            book_notional: dec!(100000),
            fee: Decimal::ZERO,
            maker_rest_ms: None,
        };
        Plan {
            symbol: Symbol::perp("NEAR", "USDT"),
            long: leg(Venue::LighterRh, Side::Buy, dec!(0.002)),
            short: leg(Venue::Arcus, Side::Sell, dec!(0.001)),
            expected_cost: dec!(0.001),
            min_locked_basis: None,
            leg_order: crate::preflight::LegOrder::Slippage,
        }
    }

    #[tokio::test]
    async fn adding_to_a_position_merges_both_legs_and_the_close_still_adds_up() {
        let ledger = temp_ledger("add-merge").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                AddBroker::new(Venue::LighterRh, dec!(99), Decimal::ZERO, None) as Arc<dyn Broker>,
                AddBroker::new(Venue::Arcus, dec!(101.5), dec!(0.2), None),
            ],
        );
        let mut position = hedged_for_add(&ledger, "inc").await;
        assert!(
            executor
                .increase(&mut position, &add_plan(), dec!(2))
                .await
                .unwrap()
        );
        let long = position.long.clone().unwrap();
        let short = position.short.clone().unwrap();
        // 多腿 10 @100 + 5 @99 = 15 @99.666…；空腿 10 @101 + 5 @101.5 = 15 @101.1666…
        assert_eq!(long.quantity(), Some(dec!(15)));
        assert_eq!(short.quantity(), Some(dec!(15)));
        assert_eq!(long.notional_usdt, dec!(1495));
        assert_eq!(short.notional_usdt, dec!(1517.5));
        assert_eq!(short.fee_usdt, dec!(0.7));
        assert_eq!(long.margin_usdt, Some(dec!(200) + dec!(495) / dec!(3)));
        // 入场基差按名义加权：1% × 1000 + 2% × 495。
        assert_eq!(
            position.entry_basis_pct,
            (dec!(1) * dec!(1000) + dec!(2) * dec!(495)) / dec!(1495)
        );
        assert_eq!(position.status, PositionStatus::Open);
        assert!(position.note.as_deref().unwrap().contains("第 1 次补仓"));
        // 下一次补仓换新号。
        assert_eq!(executor.next_add("inc").await.unwrap(), 2);

        // 平掉之后，从台账订单还原的双边明细仍与执行器入账一致。
        executor.close(&mut position).await.unwrap();
        let (replayed, _) = ledger.replay().await.unwrap();
        let stored = &replayed.positions["inc"];
        let breakdown = crate::breakdown::position_breakdown(stored, &replayed.orders).unwrap();
        assert_eq!(breakdown.pnl_gap_usdt, None, "{breakdown:?}");
        assert_eq!(breakdown.fee_gap_usdt, None, "{breakdown:?}");
        let long = breakdown.long.unwrap();
        assert_eq!(long.entry_quantity, dec!(15));
        assert_eq!(long.adds, 1);
    }

    #[tokio::test]
    async fn a_failed_second_add_leg_only_backs_out_what_was_just_added() {
        let ledger = temp_ledger("add-undo").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                AddBroker::new(Venue::LighterRh, dec!(99), Decimal::ZERO, None) as Arc<dyn Broker>,
                AddBroker::new(Venue::Arcus, dec!(101.5), dec!(0.2), Some("-add")),
            ],
        );
        let mut position = hedged_for_add(&ledger, "undo").await;
        assert!(
            !executor
                .increase(&mut position, &add_plan(), dec!(2))
                .await
                .unwrap()
        );
        // 原来的 10 个一点没动：多腿补进的 5 个已退掉，两腿回到对等，空腿什么都没变。
        let long = position.long.clone().unwrap();
        assert!((long.quantity().unwrap() - dec!(10)).abs() < dec!(1e-9));
        assert_eq!(position.short.as_ref().unwrap().notional_usdt, dec!(1010));
        assert_eq!(position.status, PositionStatus::Open);
        let note = position.note.clone().unwrap();
        assert!(
            note.contains("已退掉") && note.contains("两腿回到对等"),
            "{note}"
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        let undo = &replayed.orders["undo-sell-undo1"];
        assert!(undo.order.reduce_only);
        assert_eq!(undo.order.quantity, Some(dec!(5)));
        // 一买一卖都按 99 成交：这一来一回没有价格盈亏（按并入后的均价记账，总数不变）。
        let close = executor.close(&mut position).await;
        assert!(close.is_ok());
        let (replayed, _) = ledger.replay().await.unwrap();
        let stored = &replayed.positions["undo"];
        let breakdown = crate::breakdown::position_breakdown(stored, &replayed.orders).unwrap();
        assert_eq!(breakdown.pnl_gap_usdt, None, "{breakdown:?}");
        assert_eq!(breakdown.fee_gap_usdt, None, "{breakdown:?}");
    }

    /// 某次实盘：bitget 两笔成交 44 @ 2.98 + 23 @ 2.981 合并后，反推的数量必须正好是 67：
    /// 均价不能被舍到 12 位小数，否则 reduce-only 平仓数量带尾巴（67.000000000002），被 Bitget 一直拒。
    #[test]
    fn merged_partial_fills_keep_an_exact_quantity() {
        let fill = |quantity: Decimal, price: Decimal| LegFill {
            venue: Venue::Bitget,
            side: Side::Buy,
            notional_usdt: quantity * price,
            average_price: price,
            fee_usdt: Decimal::ZERO,
            client_order_id: ClientOrderId::for_leg("lit", Side::Buy, 0),
            margin_usdt: None,
        };
        let merged =
            merge_fills(&[fill(dec!(44), dec!(2.98)), fill(dec!(23), dec!(2.981))]).unwrap();
        assert_eq!(merged.quantity(), Some(dec!(67)));
        assert_eq!(merged.notional_usdt, dec!(199.683));
    }

    /// 某次实盘：补仓第二腿只差 0.04 个（约 3.9 USDT，低于最小下单额），退单发了也是被拒。
    /// 不发退单、不报中断，带着这点差额持有，补仓算成功。
    #[tokio::test]
    async fn an_add_short_by_less_than_the_minimum_order_is_kept_not_aborted() {
        let ledger = temp_ledger("add-dust").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                AddBroker::new(Venue::LighterRh, dec!(99), Decimal::ZERO, None) as Arc<dyn Broker>,
                AddBroker::short_on_adds(Venue::Arcus, dec!(101.5), dec!(0.04)),
            ],
        );
        let mut position = hedged_for_add(&ledger, "dust").await;
        assert!(
            executor
                .increase(&mut position, &add_plan(), dec!(2))
                .await
                .unwrap()
        );
        let note = position.note.clone().unwrap();
        assert!(note.contains("没有退单") && note.contains("持有"), "{note}");
        // 多腿 10 + 5 = 15，空腿 10 + 4.96：差额留着，没有发 undo 单。
        assert_eq!(position.long.as_ref().unwrap().quantity(), Some(dec!(15)));
        assert!(
            (position.short.as_ref().unwrap().quantity().unwrap() - dec!(14.96)).abs() < dec!(1e-9)
        );
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(
            replayed.orders.keys().all(|id| !id.contains("undo")),
            "不该发退单"
        );
        assert_eq!(position.status, PositionStatus::Open);
    }

    #[tokio::test]
    async fn an_unfilled_first_add_leg_changes_nothing() {
        let ledger = temp_ledger("add-none").await;
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                AddBroker::new(Venue::LighterRh, dec!(99), Decimal::ZERO, Some("-add"))
                    as Arc<dyn Broker>,
                AddBroker::new(Venue::Arcus, dec!(101.5), dec!(0.2), None),
            ],
        );
        let mut position = hedged_for_add(&ledger, "none").await;
        let before = (
            position.long.clone().unwrap(),
            position.short.clone().unwrap(),
        );
        assert!(
            !executor
                .increase(&mut position, &add_plan(), dec!(2))
                .await
                .unwrap()
        );
        assert_eq!(
            position.long.as_ref().unwrap().notional_usdt,
            before.0.notional_usdt
        );
        assert_eq!(
            position.short.as_ref().unwrap().notional_usdt,
            before.1.notional_usdt
        );
        assert!(position.note.as_deref().unwrap().contains("原来的仓位没动"));
        // 失败的尝试也占一次补仓次数。
        assert_eq!(executor.next_add("none").await.unwrap(), 2);
        // 不是两腿齐全的持仓不能补。
        let mut closing = position.clone();
        closing.status = PositionStatus::Closing;
        assert!(
            executor
                .increase(&mut closing, &add_plan(), dec!(2))
                .await
                .is_err()
        );
    }

    // ---- 补仓单自愈（某次实盘：补仓第一腿读回不到，订单停在 Pending，对账冻结这个币）----

    /// 读回行为可调的券商：`read` 是 `order_state` 的答案，`net` 是场所上这个币的净持仓。
    struct StaleBroker {
        venue: Venue,
        read: tokio::sync::Mutex<Result<Option<OrderState>, &'static str>>,
        net: Decimal,
        /// 发出去的单（只有退单）：按限价 / 名义全部成交，`order_state` 读到的是它。
        placed: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    #[async_trait]
    impl Broker for StaleBroker {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            Decimal::ZERO
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            assert!(order.reduce_only, "核实补仓单只会发 reduce-only 退单");
            let mut state = OrderState::new(order.clone());
            state.status = OrderStatus::Filled;
            state.average_price = Some(dec!(99));
            state.filled_usdt = order.quantity.unwrap() * dec!(99);
            state.venue_order_id = Some(order.client_order_id.0.clone());
            self.placed
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state);
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: OrderStatus::Filled,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            if let Some(state) = self.placed.lock().await.get(&id.0) {
                return Ok(Some(state.clone()));
            }
            self.read
                .lock()
                .await
                .clone()
                .map_err(|why| ArbError::venue(self.venue.as_str(), why))
        }
        async fn cancel(&self, _venue_id: &str) -> ArbResult<()> {
            unreachable!("核实补仓单不撤单")
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![crate::broker::VenuePosition {
                venue: self.venue,
                symbol: Symbol::perp("NEAR", "USDT"),
                net_quantity: self.net,
                average_price: None,
                notional_usdt: Decimal::ZERO,
            }])
        }
    }

    const UNRESOLVED: &str =
        "recorded order is unresolved or outside API history; NEVER resubmit it";

    /// 台账里 `pos` 的第 1 次补仓第一腿（LighterRh 买，名义 495）停在 `Pending`；返回执行器、订单。
    async fn stale_add(
        name: &str,
        read: impl FnOnce(&OrderState) -> Result<Option<OrderState>, &'static str>,
        net: Decimal,
    ) -> (Executor, Arc<Ledger>, OrderState) {
        let ledger = temp_ledger(name).await;
        let position = hedged_for_add(&ledger, "pos").await;
        ledger
            .append(&crate::Record::Position(Box::new(position)))
            .await
            .unwrap();
        let order = NewOrder {
            post_only: false,
            client_order_id: ClientOrderId::for_add("pos", Side::Buy, 1),
            venue: Venue::LighterRh,
            symbol: Symbol::perp("NEAR", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(495),
            quantity: None,
            limit_price: Some(dec!(100)),
            reduce_only: false,
            leverage: Some(dec!(3)),
            margin_mode: crate::MarginMode::Isolated,
        };
        let local = OrderState::new(order);
        ledger
            .append(&crate::Record::Order(Box::new(local.clone())))
            .await
            .unwrap();
        let broker = Arc::new(StaleBroker {
            venue: Venue::LighterRh,
            read: tokio::sync::Mutex::new(read(&local)),
            net,
            placed: Default::default(),
        });
        let executor = Executor::new(Arc::clone(&ledger), vec![broker as Arc<dyn Broker>]);
        (executor, ledger, local)
    }

    async fn stored_order(ledger: &Ledger, id: &str) -> OrderState {
        ledger.replay().await.unwrap().0.orders[id].clone()
    }

    #[tokio::test]
    async fn an_unreadable_add_order_is_closed_as_unfilled_when_the_venue_position_is_unchanged() {
        // 台账多腿 10 个；场所净持仓也是 10 个 = 这张补仓单没动过持仓。
        let (executor, ledger, local) =
            stale_add("stale-add-flat", |_| Err(UNRESOLVED), dec!(10)).await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::NoFill(_)), "{outcome:?}");
        let stored = stored_order(&ledger, &local.order.client_order_id.0).await;
        assert_eq!(stored.status, OrderStatus::Rejected);
        assert!(stored.reject_reason.unwrap().contains("净持仓与台账一致"));
        assert!(position.note.as_deref().unwrap().contains("原来的仓位没动"));
        // 台账里不再有没终态的单，对账不会再报 OrderMissingOnVenue。
        assert!(
            ledger
                .replay()
                .await
                .unwrap()
                .0
                .orders
                .values()
                .all(|state| !state.status.is_live())
        );
    }

    #[tokio::test]
    async fn an_unreadable_add_order_is_left_alone_when_the_venue_position_moved() {
        // 场所净持仓 14.95（比台账多了一整张补仓单）：不能当成没成交。
        let (executor, ledger, local) =
            stale_add("stale-add-moved", |_| Err(UNRESOLVED), dec!(14.95)).await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::Waiting(_)), "{outcome:?}");
        let stored = stored_order(&ledger, &local.order.client_order_id.0).await;
        assert_eq!(stored.status, OrderStatus::Pending);
        assert!(position.note.is_none());
    }

    #[tokio::test]
    async fn an_add_order_the_venue_never_received_is_closed_as_rejected() {
        let (executor, ledger, local) =
            stale_add("stale-add-unknown", |_| Ok(None), dec!(10)).await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::NoFill(_)), "{outcome:?}");
        assert_eq!(
            stored_order(&ledger, &local.order.client_order_id.0)
                .await
                .status,
            OrderStatus::Rejected
        );
    }

    #[tokio::test]
    async fn an_add_order_read_back_cancelled_is_recorded_as_read() {
        let (executor, ledger, local) = stale_add(
            "stale-add-cancelled",
            |local| {
                let mut cancelled = local.clone();
                cancelled.status = OrderStatus::Cancelled;
                Ok(Some(cancelled))
            },
            dec!(10),
        )
        .await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::NoFill(_)), "{outcome:?}");
        assert_eq!(
            stored_order(&ledger, &local.order.client_order_id.0)
                .await
                .status,
            OrderStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn a_lone_filled_first_add_leg_is_booked_and_undone() {
        // 第一腿 `buy-add1` 结果不明中断（第二腿从没发出），事后读回成交 5 个 @99：记进多腿（10 → 15），
        // 再 reduce-only 退掉 5 个，回到补仓前的 10 个。
        let (executor, ledger, local) = stale_add(
            "stale-add-filled",
            |local| {
                let mut filled = local.clone();
                filled.status = OrderStatus::Filled;
                filled.filled_usdt = dec!(495);
                filled.average_price = Some(dec!(99));
                Ok(Some(filled))
            },
            dec!(15),
        )
        .await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::Applied(_)), "{outcome:?}");
        let stored = stored_order(&ledger, &local.order.client_order_id.0).await;
        assert_eq!(stored.status, OrderStatus::Filled);
        let undo = stored_order(&ledger, "pos-sell-undo1").await;
        assert_eq!(undo.order.quantity, Some(dec!(5)));
        assert!(undo.order.reduce_only);
        assert_eq!(position.long.as_ref().unwrap().quantity(), Some(dec!(10)));
        assert!(
            position.note.as_deref().unwrap().contains("两腿回到对等"),
            "{:?}",
            position.note
        );
        let saved = ledger.replay().await.unwrap().0.positions["pos"].clone();
        assert_eq!(saved.long.unwrap().quantity(), Some(dec!(10)));
    }

    /// 一张减仓单（多腿 `sell-trim1`，2 个）停在台账里：后台核实读回已成交，从多腿上减掉；只减了一条腿，
    /// 两腿差 2 个（约 200 USDT），推告警。另一张读不到、净持仓没变的按未成交了结。
    #[tokio::test]
    async fn stale_trim_orders_are_settled_in_the_background() {
        for (name, read, net, long_after, unbalanced) in [
            ("stale-trim-filled", true, dec!(8), dec!(8), true),
            ("stale-trim-none", false, dec!(10), dec!(10), false),
        ] {
            let ledger = temp_ledger(name).await;
            let position = hedged_for_add(&ledger, "pos").await;
            ledger
                .append(&crate::Record::Position(Box::new(position)))
                .await
                .unwrap();
            let local = OrderState::new(NewOrder {
                post_only: false,
                client_order_id: ClientOrderId::for_trim("pos", Side::Sell, 1),
                venue: Venue::LighterRh,
                symbol: Symbol::perp("NEAR", "USDT"),
                side: Side::Sell,
                notional_usdt: dec!(200),
                quantity: Some(dec!(2)),
                limit_price: None,
                reduce_only: true,
                leverage: None,
                margin_mode: crate::MarginMode::Isolated,
            });
            ledger
                .append(&crate::Record::Order(Box::new(local.clone())))
                .await
                .unwrap();
            let answer = if read {
                let mut filled = local.clone();
                filled.status = OrderStatus::Filled;
                filled.filled_usdt = dec!(200);
                filled.average_price = Some(dec!(100));
                Ok(Some(filled))
            } else {
                Err(UNRESOLVED)
            };
            let broker = Arc::new(StaleBroker {
                venue: Venue::LighterRh,
                read: tokio::sync::Mutex::new(answer),
                net,
                placed: Default::default(),
            });
            let executor = Executor::new(Arc::clone(&ledger), vec![broker as Arc<dyn Broker>]);
            let mut seen = HashMap::new();
            let settled = crate::desk::settle_stale_adds(
                &executor,
                &ledger,
                &mut seen,
                std::time::Instant::now(),
                Duration::ZERO,
            )
            .await
            .unwrap();
            assert_eq!(settled.len(), 1, "{name}");
            assert_eq!(
                matches!(settled[0].outcome, AddSettle::Unbalanced(_)),
                unbalanced,
                "{name}: {:?}",
                settled[0].outcome
            );
            let (replayed, _) = ledger.replay().await.unwrap();
            assert!(
                !replayed.orders["pos-sell-trim1"].status.is_live(),
                "{name}"
            );
            assert_eq!(
                replayed.positions["pos"].long.as_ref().unwrap().quantity(),
                Some(long_after),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn stale_adds_are_settled_only_after_the_grace_and_only_add_orders() {
        let (executor, ledger, local) =
            stale_add("stale-add-grace", |_| Err(UNRESOLVED), dec!(10)).await;
        // 一张同仓位的退出单也没终态：不归这里管。
        let mut exit = local.clone();
        exit.order.client_order_id = ClientOrderId::for_exit("pos", Side::Sell, 1);
        ledger
            .append(&crate::Record::Order(Box::new(exit.clone())))
            .await
            .unwrap();
        let grace = Duration::from_secs(60);
        let t0 = std::time::Instant::now();
        let mut seen = HashMap::new();
        // 第一次看到：只记时间。
        let first = crate::desk::settle_stale_adds(&executor, &ledger, &mut seen, t0, grace)
            .await
            .unwrap();
        assert!(first.is_empty());
        let early = crate::desk::settle_stale_adds(
            &executor,
            &ledger,
            &mut seen,
            t0 + Duration::from_secs(30),
            grace,
        )
        .await
        .unwrap();
        assert!(early.is_empty());
        assert_eq!(
            stored_order(&ledger, &local.order.client_order_id.0)
                .await
                .status,
            OrderStatus::Pending
        );
        // 满了宽限：了结补仓单，退出单不碰。
        let late = crate::desk::settle_stale_adds(&executor, &ledger, &mut seen, t0 + grace, grace)
            .await
            .unwrap();
        assert_eq!(late.len(), 1);
        assert_eq!(late[0].order_id, local.order.client_order_id.0);
        assert!(matches!(late[0].outcome, AddSettle::NoFill(_)));
        assert_eq!(
            stored_order(&ledger, &local.order.client_order_id.0)
                .await
                .status,
            OrderStatus::Rejected
        );
        assert_eq!(
            stored_order(&ledger, &exit.order.client_order_id.0)
                .await
                .status,
            OrderStatus::Pending
        );
        // 了结之后记时被清掉，不会重复处理。
        let again =
            crate::desk::settle_stale_adds(&executor, &ledger, &mut seen, t0 + grace * 2, grace)
                .await
                .unwrap();
        assert!(again.is_empty());
        assert!(!seen.contains_key(&local.order.client_order_id.0));
    }

    // ---- 只挂单的第一腿（币安 GTX）----

    /// 能「挂着」的桩券商：只挂单的开仓单先是 `Open`，被读到第 N 次时按脚本成交一部分或全部；
    /// 其余的单（对冲腿、平仓）立刻成交。
    struct Resting {
        venue: Venue,
        price: Decimal,
        /// （第几次读到它时，累计成交占订单数量的比例）。
        script: Vec<(usize, Decimal)>,
        /// 撤单时订单其实刚好全部成交了：撤单报错，读回是 `Filled`。
        cancel_loses_race: bool,
        /// 查询落后于撮合（币安东京实测）：撤单当场生效，但撤单报「撤单后仍活跃」，之后头这么多次
        /// 读回仍是撤单前的活跃状态。
        stale_reads_after_cancel: usize,
        /// 撤单后头这么多次读回直接报错（成交明细还没跟上）。
        failing_reads_after_cancel: usize,
        stale_left: std::sync::atomic::AtomicUsize,
        failing_left: std::sync::atomic::AtomicUsize,
        /// 查询落后于下单（币安东京实测）：只挂单受理后，头这么多次读回报错（「保留的订单号交易所不认识」）。
        erroring_reads_after_place: usize,
        /// 同上，但读回是「查无此单」（`None`）。
        missing_reads_after_place: usize,
        after_place_left: std::sync::atomic::AtomicUsize,
        /// 下单就被交易所拒绝（返回错误，查单是 `Rejected`）。
        reject: Option<String>,
        supports: bool,
        polls: std::sync::atomic::AtomicUsize,
        cancels: std::sync::atomic::AtomicUsize,
        placed: std::sync::atomic::AtomicUsize,
        orders: tokio::sync::Mutex<HashMap<String, OrderState>>,
    }

    impl Resting {
        fn new(script: Vec<(usize, Decimal)>) -> Self {
            Self {
                venue: Venue::Binance,
                price: dec!(5.36),
                script,
                cancel_loses_race: false,
                stale_reads_after_cancel: 0,
                failing_reads_after_cancel: 0,
                stale_left: Default::default(),
                failing_left: Default::default(),
                erroring_reads_after_place: 0,
                missing_reads_after_place: 0,
                after_place_left: Default::default(),
                reject: None,
                supports: true,
                polls: Default::default(),
                cancels: Default::default(),
                placed: Default::default(),
                orders: Default::default(),
            }
        }
    }

    #[async_trait]
    impl Broker for Resting {
        fn venue(&self) -> Venue {
            self.venue
        }
        fn fee_per_side(&self) -> Decimal {
            dec!(0.0004)
        }
        fn supports_post_only(&self) -> bool {
            self.supports
        }
        async fn place(&self, order: &NewOrder) -> ArbResult<crate::types::OrderAck> {
            self.placed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut state = OrderState::new(order.clone());
            state.venue_order_id = Some(order.client_order_id.0.clone());
            if let Some(reason) = &self.reject
                && order.post_only
            {
                state.status = OrderStatus::Rejected;
                state.reject_reason = Some(reason.clone());
                self.orders
                    .lock()
                    .await
                    .insert(order.client_order_id.0.clone(), state);
                return Err(ArbError::venue(self.venue.as_str(), reason.clone()));
            }
            if order.post_only {
                state.status = OrderStatus::Open;
                self.after_place_left.store(
                    self.erroring_reads_after_place + self.missing_reads_after_place,
                    std::sync::atomic::Ordering::SeqCst,
                );
            } else {
                let quantity = order
                    .quantity
                    .unwrap_or_else(|| (order.notional_usdt / self.price).round_dp(6));
                state.status = OrderStatus::Filled;
                state.average_price = Some(self.price);
                state.filled_usdt = quantity * self.price;
            }
            self.orders
                .lock()
                .await
                .insert(order.client_order_id.0.clone(), state.clone());
            Ok(crate::types::OrderAck {
                client_order_id: order.client_order_id.clone(),
                venue_order_id: order.client_order_id.0.clone(),
                status: state.status,
            })
        }
        async fn order_state(&self, id: &ClientOrderId) -> ArbResult<Option<OrderState>> {
            use std::sync::atomic::Ordering::SeqCst;
            let mut orders = self.orders.lock().await;
            let Some(state) = orders.get_mut(&id.0) else {
                return Ok(None);
            };
            if state.order.post_only
                && self
                    .after_place_left
                    .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                return if self.erroring_reads_after_place > 0 {
                    Err(ArbError::venue(
                        self.venue.as_str(),
                        "a reserved order ID is unknown to the venue; cannot assume it never filled",
                    ))
                } else {
                    Ok(None)
                };
            }
            if !state.status.is_live() && state.order.post_only {
                if self
                    .failing_left
                    .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    return Err(ArbError::venue(
                        self.venue.as_str(),
                        "fills do not sum to the order's executed quantity",
                    ));
                }
                if self
                    .stale_left
                    .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    let mut stale = state.clone();
                    stale.status = OrderStatus::Open;
                    return Ok(Some(stale));
                }
            }
            if state.status == OrderStatus::Open && state.order.post_only {
                let n = self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let fraction = self
                    .script
                    .iter()
                    .filter(|(at, _)| *at <= n)
                    .map(|(_, fraction)| *fraction)
                    .max()
                    .unwrap_or(Decimal::ZERO);
                let full = (state.order.notional_usdt / self.price).round_dp(6);
                if fraction > Decimal::ZERO {
                    state.filled_usdt = full * fraction * self.price;
                    state.average_price = Some(self.price);
                    if fraction >= Decimal::ONE {
                        state.status = OrderStatus::Filled;
                    }
                }
            }
            Ok(Some(state.clone()))
        }
        async fn cancel(&self, id: &str) -> ArbResult<()> {
            use std::sync::atomic::Ordering::SeqCst;
            let first = self.cancels.fetch_add(1, SeqCst) == 0;
            let mut orders = self.orders.lock().await;
            let state = orders.get_mut(id).expect("cancel of an unknown order");
            if self.stale_reads_after_cancel > 0 || self.failing_reads_after_cancel > 0 {
                state.status = OrderStatus::Cancelled;
                if first {
                    self.stale_left.store(self.stale_reads_after_cancel, SeqCst);
                    self.failing_left
                        .store(self.failing_reads_after_cancel, SeqCst);
                }
                if self.stale_left.load(SeqCst) > 0 {
                    return Err(ArbError::venue(
                        self.venue.as_str(),
                        "order is still live after cancellation",
                    ));
                }
                return Ok(());
            }
            if self.cancel_loses_race {
                let full = (state.order.notional_usdt / self.price).round_dp(6);
                state.status = OrderStatus::Filled;
                state.filled_usdt = full * self.price;
                state.average_price = Some(self.price);
                return Err(ArbError::venue(self.venue.as_str(), "Unknown order sent."));
            }
            state.status = OrderStatus::Cancelled;
            Ok(())
        }
        async fn open_orders(&self) -> ArbResult<Vec<OrderState>> {
            Ok(vec![])
        }
        async fn positions(&self) -> ArbResult<Vec<crate::broker::VenuePosition>> {
            Ok(vec![])
        }
    }

    /// 币安挂单买（第一腿）+ Lighter RH 吃单卖（对冲腿）的 某合约 计划，挂 `rest_ms` 毫秒。
    fn maker_plan(rest_ms: u64) -> Plan {
        let mut plan = near_plan(Some(dec!(0.000156)));
        plan.long.venue = Venue::Binance;
        plan.long.limit_price = dec!(5.36);
        plan.long.best_price = dec!(5.36);
        plan.long.expected_price = dec!(5.36);
        plan.long.slippage = Decimal::ZERO;
        plan.long.fee = dec!(0.0004);
        plan.long.maker_rest_ms = Some(rest_ms);
        plan
    }

    async fn open_maker(
        name: &str,
        maker: Resting,
        rest_ms: u64,
    ) -> (
        ArbResult<ExecutionOutcome>,
        Arc<Ledger>,
        Arc<Resting>,
        Arc<Refills>,
    ) {
        let ledger = temp_ledger(name).await;
        let maker = Arc::new(maker);
        let hedge = Arc::new(Refills::new(Venue::LighterRh, dec!(5.3685), 0));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&maker) as Arc<dyn Broker>,
                Arc::clone(&hedge) as Arc<dyn Broker>,
            ],
        );
        let outcome = executor
            .open(
                &maker_plan(rest_ms),
                Strategy::Spread,
                dec!(0.1268),
                name,
                &near_setup(),
            )
            .await;
        (outcome, ledger, maker, hedge)
    }

    #[tokio::test]
    async fn a_resting_first_leg_that_fills_is_hedged_with_the_same_quantity() {
        let (outcome, ledger, maker, hedge) =
            open_maker("maker-fills", Resting::new(vec![(3, Decimal::ONE)]), 2_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["maker-fills-buy-0"];
        assert!(first.order.post_only, "第一腿必须是只挂单");
        assert_eq!(first.status, OrderStatus::Filled);
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 0);
        // 对冲腿不是只挂单，数量等于第一腿成交的数量。
        let second = &replayed.orders["maker-fills-sell-0"];
        assert!(!second.order.post_only);
        let long = position.long.unwrap();
        let short = position.short.unwrap();
        assert_eq!(second.order.quantity, long.quantity());
        assert_eq!(short.venue, Venue::LighterRh);
        assert_eq!(entries(&hedge), 1);
    }

    #[tokio::test]
    async fn a_resting_first_leg_that_never_fills_is_cancelled_and_the_hedge_is_never_sent() {
        let (outcome, ledger, maker, hedge) =
            open_maker("maker-idle", Resting::new(vec![]), 60).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Unwound);
        assert!(position.long.is_none() && position.short.is_none());
        assert!(
            position.note.as_deref().unwrap().contains("只挂单没有成交"),
            "{:?}",
            position.note
        );
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(entries(&hedge), 0, "第一腿没成交，对面一张单都不能发");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["maker-idle-buy-0"].status,
            OrderStatus::Cancelled
        );
        assert!(
            replayed
                .orders
                .values()
                .all(|order| !order.status.is_live())
        );
    }

    #[tokio::test]
    async fn a_resting_first_leg_with_a_fill_stops_waiting_soon_and_hedges_what_filled() {
        // 第 2 次读到时成交了 60%；订单挂 5 秒，但有成交后只再多等 MAKER_FILL_GRACE。
        let started = std::time::Instant::now();
        let (outcome, ledger, maker, hedge) =
            open_maker("maker-partial", Resting::new(vec![(2, dec!(0.6))]), 5_000).await;
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "有成交之后不能一直等到挂单到期：{:?}",
            started.elapsed()
        );
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["maker-partial-buy-0"];
        assert_eq!(first.status, OrderStatus::Cancelled, "剩下的那部分要撤掉");
        let long = position.long.unwrap();
        // 对冲只发一次，数量就是第一腿最终成交的数量（不是订单原来的数量）。
        assert_eq!(entries(&hedge), 1);
        let second = &replayed.orders["maker-partial-sell-0"];
        assert_eq!(second.order.quantity, long.quantity());
        assert!(long.notional_usdt < dec!(800) && long.notional_usdt >= PARTIAL_HEDGE_MIN_USDT);
    }

    #[tokio::test]
    async fn a_fill_too_small_to_hedge_keeps_resting_to_the_deadline_then_is_rolled_back_with_the_dust_mark()
     {
        // 800 USDT 的单子成交了 1%（8 USDT，不到对冲腿的最小额）：不开宽限，一直挂到截止；
        // 到点撤掉、回滚、对面一张单都不发，备注带上「凑不够对冲」的标记。
        let started = std::time::Instant::now();
        let (outcome, ledger, maker, hedge) =
            open_maker("maker-dust", Resting::new(vec![(2, dec!(0.01))]), 300).await;
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(280),
            "还没够对冲就不能提前撤：{:?}",
            started.elapsed()
        );
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(
            position.note.as_deref().unwrap().contains(MAKER_DUST_NOTE),
            "{:?}",
            position.note
        );
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(entries(&hedge), 0, "凑不够对冲就不对冲");
        let (replayed, _) = ledger.replay().await.unwrap();
        // 回滚的是已成交的那一小块，用的是只减仓的吃单。
        let exit = replayed
            .orders
            .values()
            .find(|order| order.order.reduce_only)
            .expect("回滚单");
        assert!(!exit.order.post_only && exit.filled_usdt > Decimal::ZERO);
    }

    #[tokio::test]
    async fn a_resting_fill_of_one_lot_is_hedged_when_it_covers_the_hedge_venues_minimum() {
        // 1.75% × 800 = 14 USDT：不到吃单的 20，但够对冲腿的最小额（12），要对冲而不是回滚。
        let (outcome, ledger, _maker, hedge) =
            open_maker("maker-lot", Resting::new(vec![(2, dec!(0.0175))]), 2_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&hedge), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["maker-lot-buy-0"];
        assert!(
            first.filled_usdt >= MAKER_PARTIAL_HEDGE_MIN_USDT
                && first.filled_usdt < PARTIAL_HEDGE_MIN_USDT
        );
    }

    #[tokio::test]
    async fn a_cancel_that_loses_the_race_with_a_fill_is_settled_from_the_state_read_back() {
        let mut maker = Resting::new(vec![]);
        maker.cancel_loses_race = true;
        let (outcome, ledger, _maker, hedge) = open_maker("maker-race", maker, 40).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&hedge), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["maker-race-buy-0"].status,
            OrderStatus::Filled
        );
    }

    #[tokio::test]
    async fn a_cancel_read_back_that_lags_the_venue_is_reread_and_the_partial_fill_is_hedged() {
        // 某次实盘：挂单成交了一部分，撤剩余时撤单当场生效，但撤单报「仍活跃」，紧接着两次读回
        // 也是撤单前的「部分成交、活跃」。不能当结论中断（那样留下没对冲的成交），要重读到终态再对冲。
        let mut maker = Resting::new(vec![(2, dec!(0.6))]);
        maker.stale_reads_after_cancel = 3;
        let (outcome, ledger, maker, hedge) = open_maker("maker-stale", maker, 5_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&hedge), 1, "按成交量对冲一次");
        // 读回一直是活跃时中途补撤过一次。
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 2);
        let (replayed, _) = ledger.replay().await.unwrap();
        let first = &replayed.orders["maker-stale-buy-0"];
        assert_eq!(first.status, OrderStatus::Cancelled);
        let long = position.long.unwrap();
        assert!(long.notional_usdt < dec!(800) && long.notional_usdt > Decimal::ZERO);
        assert_eq!(
            replayed.orders["maker-stale-sell-0"].order.quantity,
            long.quantity()
        );
    }

    #[tokio::test]
    async fn a_cancel_read_back_that_errors_while_fills_catch_up_is_reread() {
        let mut maker = Resting::new(vec![(2, dec!(0.6))]);
        maker.failing_reads_after_cancel = 2;
        let (outcome, _ledger, _maker, hedge) = open_maker("maker-lagfills", maker, 5_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&hedge), 1);
    }

    #[tokio::test]
    async fn a_fresh_order_the_venue_query_does_not_know_yet_is_reread_instead_of_aborting() {
        // 某次实盘：币安已受理只挂单，0.15 秒后按订单号查却报「不认识」。不能当「结果不明」
        // 中断（挂单会无人看管地挂着、自动交易被关），要重读到它，照常挂着等成交。
        let mut maker = Resting::new(vec![(3, Decimal::ONE)]);
        maker.erroring_reads_after_place = 3;
        let (outcome, ledger, _maker, hedge) = open_maker("maker-unknown", maker, 2_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Open, "{:?}", position.note);
        assert_eq!(entries(&hedge), 1);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["maker-unknown-buy-0"].status,
            OrderStatus::Filled
        );
    }

    #[tokio::test]
    async fn a_fresh_accepted_order_that_reads_as_missing_is_reread_instead_of_aborting() {
        let mut maker = Resting::new(vec![]);
        maker.missing_reads_after_place = 2;
        let (outcome, _ledger, maker, hedge) = open_maker("maker-missing", maker, 60).await;
        let position = outcome.unwrap().position;
        assert_eq!(
            position.status,
            PositionStatus::Unwound,
            "{:?}",
            position.note
        );
        assert!(
            position.note.as_deref().unwrap().contains("只挂单没有成交"),
            "{:?}",
            position.note
        );
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(entries(&hedge), 0);
    }

    #[tokio::test]
    async fn a_fresh_order_that_never_becomes_readable_still_stops_the_two_legs() {
        let mut maker = Resting::new(vec![]);
        maker.erroring_reads_after_place = 1_000;
        let (outcome, _ledger, _maker, hedge) = open_maker("maker-neverseen", maker, 60).await;
        let Err(error) = outcome else {
            panic!("一直读不到仍要停下");
        };
        assert!(
            error.to_string().contains("unknown to the venue"),
            "{error}"
        );
        assert_eq!(entries(&hedge), 0);
    }

    #[tokio::test]
    async fn an_order_that_still_reads_live_after_every_reread_stops_the_two_legs() {
        let mut maker = Resting::new(vec![(2, dec!(0.6))]);
        maker.stale_reads_after_cancel = 1_000;
        let (outcome, _ledger, _maker, hedge) = open_maker("maker-stuck", maker, 5_000).await;
        let Err(error) = outcome else {
            panic!("一直读到活跃仍要停下");
        };
        assert!(error.to_string().contains("撤单后仍有活跃订单"), "{error}");
        assert_eq!(entries(&hedge), 0, "状态不明不能发对冲腿");
    }

    #[tokio::test]
    async fn a_post_only_order_the_venue_refuses_leaves_nothing_resting_and_sends_no_hedge() {
        let mut maker = Resting::new(vec![]);
        maker.reject = Some("Due to the order could not be executed as maker (-5022)".into());
        let (outcome, ledger, maker, hedge) = open_maker("maker-refused", maker, 2_000).await;
        let position = outcome.unwrap().position;
        assert_eq!(position.status, PositionStatus::Unwound);
        assert!(
            position.note.as_deref().unwrap().contains("-5022"),
            "{:?}",
            position.note
        );
        assert_eq!(maker.cancels.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(entries(&hedge), 0);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["maker-refused-buy-0"].status,
            OrderStatus::Rejected
        );
    }

    #[tokio::test]
    async fn a_venue_without_post_only_support_is_refused_before_the_ledger_or_the_venue_is_touched()
     {
        let mut maker = Resting::new(vec![]);
        maker.supports = false;
        let (outcome, ledger, maker, hedge) = open_maker("maker-unsupported", maker, 2_000).await;
        assert!(outcome.is_err());
        assert_eq!(maker.placed.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(entries(&hedge), 0);
        let (replayed, _) = ledger.replay().await.unwrap();
        assert!(replayed.orders.is_empty() && replayed.positions.is_empty());
    }

    /// 减仓：多腿 Lighter、空腿 Hyperliquid，各 10 个 某合约 @100。空腿第一次减仓单（`buy-trim1`）读不到 = 结果不明。
    /// `short_net` 是 Hyperliquid 上的真实净持仓。
    async fn trim_setup(
        name: &str,
        short_net: Decimal,
    ) -> (
        Executor,
        Arc<Ledger>,
        Arc<Unreadable>,
        Arc<Unreadable>,
        PairPosition,
    ) {
        let ledger = temp_ledger(name).await;
        let long = Arc::new(Unreadable {
            venue: Venue::Lighter,
            price: dec!(100),
            net: dec!(9),
            unreadable: vec![],
            orders: Default::default(),
        });
        let short = Arc::new(Unreadable {
            venue: Venue::Hyperliquid,
            price: dec!(100),
            net: short_net,
            unreadable: vec![format!("{name}-buy-trim1")],
            orders: Default::default(),
        });
        let mut position = bare_position(name, PositionStatus::Open);
        position.symbol = Symbol::perp("NEAR", "USDT");
        position.long = Some(leg(Venue::Lighter, Side::Buy));
        position.short = Some(leg(Venue::Hyperliquid, Side::Sell));
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                Arc::clone(&long) as Arc<dyn Broker>,
                Arc::clone(&short) as Arc<dyn Broker>,
            ],
        );
        (executor, ledger, long, short, position)
    }

    fn qty(leg: &Option<LegFill>) -> Decimal {
        leg.as_ref().unwrap().quantity().unwrap()
    }

    /// 空腿减仓结果不明：多腿已减的部分必须已经落盘；下一次减仓先核实那张单（读不到、但净持仓没变 = 没成交），
    /// 再把两腿减到同一数量，而不是在差额上两腿各按比例再减一次（多腿会被减两次）。
    #[tokio::test]
    async fn an_unknown_second_trim_leg_is_persisted_settled_and_rebalanced_next_time() {
        let (executor, ledger, long, short, mut position) =
            trim_setup("trim-unknown", dec!(-10)).await;
        let first = executor
            .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
            .await;
        assert!(first.is_err(), "空腿结果不明，这次减仓要报错");
        let (replayed, _) = ledger.replay().await.unwrap();
        let saved = &replayed.positions["trim-unknown"];
        assert_eq!(qty(&saved.long), dec!(9), "多腿已减的 1 个必须已落盘");
        assert_eq!(qty(&saved.short), dec!(10));
        assert!(
            saved.note.as_deref().unwrap().contains("结果不明"),
            "{:?}",
            saved.note
        );

        executor
            .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
            .await
            .expect("上一张读不到但净持仓没变：按没成交了结，照常减仓");
        assert_eq!(qty(&position.long), dec!(8.1));
        assert_eq!(qty(&position.short), dec!(8.1), "两腿拉回同一数量");
        let (replayed, _) = ledger.replay().await.unwrap();
        assert_eq!(
            replayed.orders["trim-unknown-buy-trim1"].status,
            OrderStatus::Rejected
        );
        assert_eq!(qty(&replayed.positions["trim-unknown"].long), dec!(8.1));
        let sent = |orders: &HashMap<String, OrderState>| {
            let mut q: Vec<(String, Decimal)> = orders
                .values()
                .map(|s| (s.order.client_order_id.0.clone(), s.order.quantity.unwrap()))
                .collect();
            q.sort();
            q
        };
        assert_eq!(
            sent(&*long.orders.lock().await),
            vec![
                ("trim-unknown-sell-trim1".to_string(), dec!(1)),
                ("trim-unknown-sell-trim2".to_string(), dec!(0.9)),
            ]
        );
        assert_eq!(
            sent(&*short.orders.lock().await)[1],
            ("trim-unknown-buy-trim2".to_string(), dec!(1.9))
        );
    }

    /// 上一张减仓单读不到、而且场所净持仓和台账对不上（可能成交了）：这一轮不减，不发任何单。
    #[tokio::test]
    async fn an_unresolved_trim_with_a_changed_venue_position_blocks_the_next_trim() {
        let (executor, _ledger, long, short, mut position) =
            trim_setup("trim-blocked", dec!(-9)).await;
        assert!(
            executor
                .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
                .await
                .is_err()
        );
        let error = executor
            .trim(&mut position, dec!(0.1), dec!(100), dec!(100), "测试")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("这一轮不减仓"), "{error}");
        assert_eq!(position.trims, 1, "被挡住的这次不占订单号、不发单");
        assert_eq!(long.orders.lock().await.len(), 1);
        assert_eq!(short.orders.lock().await.len(), 1);
    }

    /// 补仓第一腿（Arcus 空 5 个 @101）已成交并进了空腿（10 → 15），第二腿（Lighter RH 多 `buy-add1`）结果不明、
    /// 停在台账里。`read` 是第二腿读回的结果，`net` 是 Lighter RH 上的真实净持仓。
    async fn stale_second_add(
        name: &str,
        read: impl FnOnce(&OrderState) -> Result<Option<OrderState>, &'static str>,
        net: Decimal,
    ) -> (Executor, Arc<Ledger>, OrderState, Arc<Unreadable>) {
        let ledger = temp_ledger(name).await;
        let mut position = hedged_for_add(&ledger, "pos").await;
        let mut first = OrderState::new(NewOrder {
            post_only: false,
            client_order_id: ClientOrderId::for_add("pos", Side::Sell, 1),
            venue: Venue::Arcus,
            symbol: Symbol::perp("NEAR", "USDT"),
            side: Side::Sell,
            notional_usdt: dec!(505),
            quantity: None,
            limit_price: Some(dec!(101)),
            reduce_only: false,
            leverage: Some(dec!(3)),
            margin_mode: crate::MarginMode::Isolated,
        });
        first.status = OrderStatus::Filled;
        first.filled_usdt = dec!(505);
        first.average_price = Some(dec!(101));
        ledger
            .append(&crate::Record::Order(Box::new(first.clone())))
            .await
            .unwrap();
        merge_leg(
            &mut position,
            &crate::broker::leg_fill(&first.order, &first).unwrap(),
        );
        ledger
            .append(&crate::Record::Position(Box::new(position)))
            .await
            .unwrap();
        let local = OrderState::new(NewOrder {
            post_only: false,
            client_order_id: ClientOrderId::for_add("pos", Side::Buy, 1),
            venue: Venue::LighterRh,
            symbol: Symbol::perp("NEAR", "USDT"),
            side: Side::Buy,
            notional_usdt: dec!(500),
            quantity: Some(dec!(5)),
            limit_price: Some(dec!(100)),
            reduce_only: false,
            leverage: Some(dec!(3)),
            margin_mode: crate::MarginMode::Isolated,
        });
        ledger
            .append(&crate::Record::Order(Box::new(local.clone())))
            .await
            .unwrap();
        let lighter = Arc::new(StaleBroker {
            venue: Venue::LighterRh,
            read: tokio::sync::Mutex::new(read(&local)),
            net,
            placed: Default::default(),
        });
        let arcus = Arc::new(Unreadable {
            venue: Venue::Arcus,
            price: dec!(101),
            net: dec!(-15),
            unreadable: vec![],
            orders: Default::default(),
        });
        let executor = Executor::new(
            Arc::clone(&ledger),
            vec![
                lighter as Arc<dyn Broker>,
                Arc::clone(&arcus) as Arc<dyn Broker>,
            ],
        );
        (executor, ledger, local, arcus)
    }

    /// 第二腿读不到、Lighter RH 净持仓没变（= 没成交）：了结成未成交之后，把第一腿多补的 5 个退掉，两腿回到 10 / 10。
    #[tokio::test]
    async fn an_unfilled_second_add_leg_undoes_the_first_legs_extra() {
        let (executor, ledger, local, arcus) =
            stale_second_add("add2-nofill", |_| Err(UNRESOLVED), dec!(10)).await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        assert_eq!(qty(&position.short), dec!(15));
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::NoFill(_)), "{outcome:?}");
        assert_eq!(qty(&position.long), dec!(10));
        assert_eq!(qty(&position.short), dec!(10), "第一腿多补的部分要退掉");
        let undo = arcus.orders.lock().await;
        let undo = undo.get("pos-buy-undo1").expect("退第一腿多补部分的单");
        assert_eq!(undo.order.quantity, Some(dec!(5)));
        assert!(undo.order.reduce_only);
        assert!(
            position.note.as_deref().unwrap().contains("两腿回到对等"),
            "{:?}",
            position.note
        );
        let stored = ledger.replay().await.unwrap().0.positions["pos"].clone();
        assert_eq!(qty(&stored.short), dec!(10));
    }

    /// 第二腿读回已成交 5 个、两腿差额正好是第一腿这次补的 5 个：它就是没并进腿的第二腿，并进去，不退第一腿。
    #[tokio::test]
    async fn a_filled_second_add_leg_that_was_never_merged_is_merged() {
        let (executor, ledger, local, arcus) = stale_second_add(
            "add2-filled",
            |local| {
                let mut filled = local.clone();
                filled.status = OrderStatus::Filled;
                filled.filled_usdt = dec!(500);
                filled.average_price = Some(dec!(100));
                Ok(Some(filled))
            },
            dec!(15),
        )
        .await;
        let mut position = ledger.replay().await.unwrap().0.positions["pos"].clone();
        let outcome = executor
            .settle_add_order(&mut position, &local)
            .await
            .unwrap();
        assert!(matches!(outcome, AddSettle::Hedged(_)), "{outcome:?}");
        assert_eq!(qty(&position.long), dec!(15));
        assert_eq!(qty(&position.short), dec!(15));
        assert!(arcus.orders.lock().await.is_empty(), "两腿已对上，不发退单");
        let stored = ledger.replay().await.unwrap().0.positions["pos"].clone();
        assert_eq!(qty(&stored.long), dec!(15));
    }
}
