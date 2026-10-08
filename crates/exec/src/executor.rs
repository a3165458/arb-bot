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

use arb_core::{ArbError, ArbResult, Side, Venue};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::broker::{Broker, leg_fill};
use crate::ledger::{Ledger, Record};
use crate::preflight::{LegPlan, Plan};
use crate::report::{OpenTiming, open_report};
use crate::types::{
    ClientOrderId, LegFill, NewOrder, OrderState, OrderStatus, PairPosition, PositionStatus,
    RealizedSource, Strategy, TaskRules,
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
        };
        self.persist(&position).await?;
        let started_at = Utc::now();

        // 第一腿：难成交的那条。它失败时手上什么都没有。
        let first = plan.first_leg().clone();
        let second = plan.second_leg().clone();

        // 下单前的准备（杠杆核对、市场元数据）两条腿并发做完：第二腿下单时不用再单独往返一次，
        // 第二腿的杠杆设不上也在第一腿成交之前就发现，不必先成交一条再回滚。
        let (first_ready, second_ready) = tokio::join!(
            self.prepare_leg(&first, setup.leverage, setup.margin_mode),
            self.prepare_leg(&second, setup.leverage, setup.margin_mode)
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

        let (first_fill, first_reason) = match self
            .submit(
                position_id,
                &first,
                0,
                setup.leverage,
                setup.margin_mode,
                None,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                // 第一腿结果不明：订单可能已经成交。记下原因再上抛，持仓页与台账里才看得到为什么停在这里。
                position.note = Some(format!(
                    "第一腿（{} {}）结果不明：{error}。先对账、不要重复开仓",
                    first.venue,
                    side_label(first.side)
                ));
                let _ = self.persist(&position).await;
                return Err(error);
            }
        };
        let first_filled_at = Utc::now();
        let Some(first_fill) = first_fill else {
            position.status = PositionStatus::Unwound;
            position.closed_at = Some(Utc::now());
            position.note = Some(format!(
                "第一腿（{} {}）未成交：{}",
                first.venue,
                side_label(first.side),
                first_reason.unwrap_or_else(|| "未成交".into())
            ));
            self.persist(&position).await?;
            warn!(position = %position.id, note = ?position.note, "建仓中止：第一腿未成交");
            return Ok(ExecutionOutcome { position });
        };
        set_leg(&mut position, &first_fill);
        self.persist(&position).await?;
        if first_reason.is_some() {
            self.unwind(&mut position).await?;
            return Ok(ExecutionOutcome { position });
        }

        // 第二腿。
        let first_quantity = first_fill
            .quantity()
            .ok_or_else(|| ArbError::config("第一腿成交价不可用，不能计算对冲数量"))?;
        let (second_fill, second_reason) = match self
            .submit(
                position_id,
                &second,
                0,
                setup.leverage,
                setup.margin_mode,
                Some(first_quantity),
            )
            .await
        {
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
        let second_filled_at = Utc::now();

        match second_fill {
            Some(fill) => {
                let second_quantity = fill
                    .quantity()
                    .ok_or_else(|| ArbError::config("第二腿成交价不可用，无法核实对冲数量"))?;
                set_leg(&mut position, &fill);
                self.persist(&position).await?;
                let mismatch = (first_quantity - second_quantity).abs() / first_quantity;
                if second_reason.is_some() || mismatch > Decimal::new(5, 3) {
                    warn!(position = %position.id, %mismatch, "第二腿未足额成交，回滚两腿");
                    self.unwind(&mut position).await?;
                } else {
                    position.status = PositionStatus::Open;
                    position.open_report = open_report(
                        plan,
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
                    self.persist(&position).await?;
                    info!(
                        position = %position.id,
                        symbol = %position.symbol,
                        notional = %position.total_notional(),
                        "双腿建仓完成"
                    );
                    if let Some(report) = &position.open_report {
                        info!(position = %position.id, "{}", report.summary());
                    }
                }
            }
            None => {
                warn!(position = %position.id, reason = ?second_reason, "第二腿未成交，回滚第一腿");
                self.unwind(&mut position).await?;
            }
        }

        Ok(ExecutionOutcome { position })
    }

    /// 平掉一笔仓位。也用于重试停在 `Closing` / `Unwinding` 的仓位：已经退出的腿
    /// 不在仓位上了，只对剩下的敞口发单。
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
        position.status = PositionStatus::Closing;
        // 入场的两腿价格在退出后就从仓位上消失了：先记下来，备注里对比开平仓的成交价差。
        let entry_prices = position
            .long
            .as_ref()
            .zip(position.short.as_ref())
            .map(|(long, short)| (long.average_price, short.average_price));
        let entry_notional = position.long.as_ref().map(|leg| leg.notional_usdt);
        // 两腿都在时才查资金费：重试时只剩一条腿，只查那条会漏掉另一条的。
        let funding_venues: Option<Vec<Venue>> = position
            .long
            .as_ref()
            .zip(position.short.as_ref())
            .map(|(long, short)| vec![long.venue, short.venue]);
        // 对冲中的一腿平不掉时停手：继续平另一腿会把对冲变成裸敞口。
        let (failures, exits) = self.exit_legs(position, true).await?;
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

        position.status = PositionStatus::Closed;
        position.closed_at = Some(Utc::now());
        position.realized_source = Some(RealizedSource::Executor);
        if let Some(venues) = funding_venues {
            position.realized_funding_usdt = self
                .funding_total(&venues, &position.symbol, position.opened_at)
                .await;
        }
        position.note = Some(realized_note(
            position,
            entry_prices,
            entry_notional,
            &exits,
        ));
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
        if position.status != PositionStatus::Open || !position.is_hedged() {
            return Err(ArbError::config(format!(
                "仓位 {} 不是完整的双腿持仓，不能减仓",
                position.id
            )));
        }
        // 先记下这次尝试再发单（与 `exit_legs` 同理）：减仓做到一半失败时，下一轮重试必须换新的
        // 订单号。不换号的话，券商要么拒绝重发（爆仓保护从此再也发不出单），要么查询返回上一次
        // 的旧成交，执行器会把它当成这一次的结果再记一遍账。
        position.trims += 1;
        let n = position.trims;
        self.persist(position).await?;
        for (is_long, mark) in [(true, long_mark), (false, short_mark)] {
            let leg = if is_long {
                position.long.as_ref()
            } else {
                position.short.as_ref()
            }
            .cloned()
            .ok_or_else(|| ArbError::config("双腿持仓缺腿"))?;
            let quantity = leg
                .quantity()
                .ok_or_else(|| ArbError::config(format!("{} 腿入场价非正", leg.venue)))?;
            let reduce_side = match leg.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let order = NewOrder {
                client_order_id: ClientOrderId::for_trim(&position.id, reduce_side, n),
                venue: leg.venue,
                symbol: position.symbol.clone(),
                side: reduce_side,
                notional_usdt: quantity * fraction * mark,
                quantity: Some(quantity * fraction),
                limit_price: None,
                reduce_only: true,
                leverage: None,
                margin_mode: position.margin_mode,
            };
            let state = self.execute_order(&order).await?;
            let filled = state
                .average_price
                .filter(|price| *price > Decimal::ZERO)
                .and_then(|price| {
                    (state.filled_usdt > Decimal::ZERO).then_some((
                        state.filled_usdt / price,
                        price,
                        state.fee_usdt,
                    ))
                });
            let Some((filled_quantity, fill_price, fee)) = filled else {
                let why = state
                    .reject_reason
                    .clone()
                    .unwrap_or_else(|| format!("订单状态 {:?}", state.status));
                position.note = Some(format!(
                    "第 {n} 次减仓在 {} 失败（{why}），{}",
                    leg.venue,
                    if is_long {
                        "两腿未变"
                    } else {
                        "多腿已减、空腿未减，两腿数量失衡"
                    }
                ));
                self.persist(position).await?;
                return Err(ArbError::venue(
                    leg.venue.as_str(),
                    format!("减仓失败：{why}"),
                ));
            };
            let kept = (Decimal::ONE - filled_quantity / quantity).max(Decimal::ZERO);
            let realized = match leg.side {
                Side::Buy => filled_quantity * (fill_price - leg.average_price),
                Side::Sell => filled_quantity * (leg.average_price - fill_price),
            };
            // 减掉部分的价格盈亏计入已实现；手续费留在腿上，腿完全平掉时一并结算。
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
            if state.status != OrderStatus::Filled {
                position.note = Some(format!(
                    "第 {n} 次减仓在 {} 仅部分成交，停止另一条腿并人工对账",
                    leg.venue
                ));
                self.persist(position).await?;
                return Err(ArbError::venue(
                    leg.venue.as_str(),
                    "减仓部分成交，需人工对账",
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

    /// 回滚已经成交的腿。
    ///
    /// 回滚失败意味着仓位仍然裸着 —— 那必须留在 `Unwinding` 并写明原因，
    /// 不能只记一条日志就当处理完了：对账会拿这个状态去交易所核对。
    async fn unwind(&self, position: &mut PairPosition) -> ArbResult<()> {
        position.status = PositionStatus::Unwinding;
        // 已经是裸敞口：每条腿都要尽量平掉，一条失败不妨碍另一条。
        let (failures, _) = self.exit_legs(position, false).await?;
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
    ) -> ArbResult<(Vec<String>, Vec<(Side, Decimal)>)> {
        position.exits += 1;
        let n = position.exits;
        self.persist(position).await?;

        let mut failures = Vec::new();
        let mut exits = Vec::new();
        for (venue, side, notional, quantity) in legs_of(position)? {
            let order = NewOrder {
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
            let state = match self.execute_order(&order).await {
                Ok(state) => state,
                Err(error) => {
                    failures.push(format!("{venue} 退出状态不明：{error}"));
                    if stop_on_failure {
                        break;
                    }
                    continue;
                }
            };
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
                if stop_on_failure {
                    break;
                }
            }
        }
        Ok((failures, exits))
    }

    /// 一条腿下单前的准备（见 [`Broker::prepare_open`]）。
    async fn prepare_leg(
        &self,
        leg: &LegPlan,
        leverage: Option<Decimal>,
        mode: crate::MarginMode,
    ) -> ArbResult<()> {
        let broker = self.broker(leg.venue)?;
        if broker
            .positions()
            .await?
            .iter()
            .any(|position| position.symbol == leg.symbol && position.net_quantity != Decimal::ZERO)
        {
            return Err(ArbError::venue(
                leg.venue.as_str(),
                "该合约已有持仓，拒绝修改保证金模式/杠杆或叠加开仓；请先核对并退出原仓位",
            ));
        }
        broker
            .prepare_open_mode(&leg.symbol, leg.side, leverage, mode)
            .await
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
        let order = NewOrder {
            client_order_id: ClientOrderId::for_leg(position_id, leg.side, attempt),
            venue: leg.venue,
            symbol: leg.symbol.clone(),
            side: leg.side,
            notional_usdt: leg.notional_usdt,
            quantity,
            limit_price: Some(leg.limit_price),
            reduce_only: false,
            leverage,
            margin_mode,
        };
        let state = self.execute_order(&order).await?;
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
        let fill = leg_fill(&order, &state).filter(|fill| fill.notional_usdt > Decimal::ZERO);
        let reason = (state.status != OrderStatus::Filled).then(|| {
            state
                .reject_reason
                .unwrap_or_else(|| format!("订单状态 {:?}", state.status))
        });
        Ok((fill, reason))
    }

    /// 提交意图先落盘。未知结果不能记为拒单，也不能开始下一条腿。
    ///
    /// `place` 报错且场所不认识这个 id（[`Broker::order_state`] 返回 `None`）才算明确
    /// 没发出去，记为拒单；券商在无法确定时必须返回错误而不是 `None`。
    async fn execute_order(&self, order: &NewOrder) -> ArbResult<OrderState> {
        self.persist_order(&OrderState::new(order.clone())).await?;
        let broker = self.broker(order.venue)?;
        let placed = broker.place(order).await;
        let mut state = match (broker.order_state(&order.client_order_id).await?, placed) {
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
        if state.status.is_live() {
            let venue_id = state.venue_order_id.as_deref().ok_or_else(|| {
                ArbError::venue(order.venue.as_str(), "活跃订单缺交易所单号，无法撤单")
            })?;
            broker.cancel(venue_id).await?;
            state = broker
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
            if state.status.is_live() {
                return Err(ArbError::venue(
                    order.venue.as_str(),
                    "撤单后仍有活跃订单，停止双腿执行",
                ));
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

/// 两腿反向操作：(场所, 方向, 入场名义额, 标的数量)；名义大的先平。
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

/// 记下一条腿。有杠杆时同时记下它的逐仓保证金 = 名义 ÷ 杠杆。
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

fn side_label(side: Side) -> &'static str {
    match side {
        Side::Buy => "买入",
        Side::Sell => "卖出",
    }
}

/// 平仓后的备注：按实际成交算的已实现盈亏（含全部手续费，不含资金费），以及开、平仓
/// 两次的成交价差 —— 不再用标记价的纸面数。
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

    /// 2026-09-30 实盘 NEAR 的四笔成交：按实际成交记账应是 −1.2949 USDT，
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
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
        tokio::fs::remove_file(&path).await.unwrap();
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
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
            closed_externally: false,
            entry_legs: None,
            pnl_unattributed: None,
            open_report: None,
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
}
