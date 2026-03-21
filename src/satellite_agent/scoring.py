from __future__ import annotations

import hashlib
from datetime import timedelta

from .config import Settings
from .indicators import clamp
from .models import EventInsight, IndicatorSnapshot, OpportunityCard, PriceRange, utcnow


def _trend_state_cn(value: str) -> str:
    return {
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "震荡",
        "uptrend": "多头",
        "downtrend": "空头",
    }.get(value, value)


class SignalScorer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def score(self, insight: EventInsight, snapshot: IndicatorSnapshot) -> OpportunityCard:
        horizon_settings = self.settings.horizons[snapshot.horizon]
        bias = "long" if insight.sentiment >= 0 else "short"
        event_score = self._event_score(insight)
        market_score = self._market_score(snapshot, bias, horizon_settings.rsi_floor, horizon_settings.rsi_ceiling, horizon_settings.atr_percent_ceiling)
        final_score = round((0.6 * event_score) + (0.4 * market_score), 2)
        priority = "suppressed"
        if event_score >= self.settings.event_score_threshold and market_score >= horizon_settings.market_score_threshold:
            priority = "high" if final_score >= horizon_settings.priority_threshold else "normal" if final_score >= 60 else "suppressed"
        action_label, confidence_label, confidence_score = self._action_and_confidence(
            priority=priority,
            final_score=final_score,
            market_data_complete=True,
            bias=bias,
        )
        structure_note = self._structure_note(snapshot, bias)
        created_at = utcnow()
        dedup_key = f"{insight.symbol}:{insight.event_id}:{insight.event_type}:{snapshot.horizon}"
        reason_to_watch = (
            f"{self._event_type_cn(insight.event_type)}强度 {event_score:.1f} 分，"
            f"重要性 {insight.importance:.0f}、来源可信度 {insight.source_credibility:.0f}；"
            f"市场确认 {market_score:.1f} 分，当前为{_trend_state_cn(snapshot.trend_state)}结构，"
            f"RSI {snapshot.rsi_14:.1f}，相对量能 {snapshot.relative_volume:.2f} 倍。"
            f" {structure_note}"
        )
        positioning_hint = self._positioning_hint(action_label, bias=bias)
        card_id = hashlib.sha1(f"{dedup_key}:{created_at.isoformat()}".encode("utf-8")).hexdigest()
        return OpportunityCard(
            card_id=card_id,
            event_id=insight.event_id,
            symbol=insight.symbol,
            horizon=snapshot.horizon,
            event_type=insight.event_type,
            headline_summary=insight.headline_summary,
            bull_case=insight.bull_case,
            bear_case=insight.bear_case,
            event_score=round(event_score, 2),
            market_score=round(market_score, 2),
            final_score=final_score,
            entry_range=PriceRange(snapshot.last_price, snapshot.last_price),
            take_profit_range=PriceRange(snapshot.last_price, snapshot.last_price),
            invalidation_level=round(snapshot.support_20 or snapshot.last_price, 2),
            invalidation_reason="等待入场与失效价计算完成。",
            risk_notes=insight.risk_notes,
            source_refs=insight.source_refs,
            created_at=created_at,
            ttl=created_at + timedelta(days=horizon_settings.ttl_days),
            priority=priority,
            dedup_key=dedup_key,
            bias=bias,
            display_name=insight.symbol,
            action_label=action_label,
            confidence_label=confidence_label,
            confidence_score=confidence_score,
            reason_to_watch=reason_to_watch,
            trend_state=snapshot.trend_state,
            rsi_14=round(snapshot.rsi_14, 1),
            relative_volume=round(snapshot.relative_volume, 2),
            positioning_hint=positioning_hint,
        )

    def score_event_only(
        self,
        insight: EventInsight,
        horizon: str,
        *,
        failure_reason: str = "",
    ) -> OpportunityCard:
        event_score = round(self._event_score(insight), 2)
        priority = "high" if event_score >= self.settings.event_only_alert_min_event_score else "suppressed"
        created_at = utcnow()
        dedup_key = f"{insight.symbol}:{insight.event_id}:{insight.event_type}:{horizon}:event_only"
        bias = "long" if insight.sentiment >= 0 else "short"
        action_label, confidence_label, confidence_score = self._action_and_confidence(
            priority=priority,
            final_score=event_score,
            market_data_complete=False,
            bias=bias,
        )
        market_note = "行情快照暂不可用，本提醒仅基于事件强度生成，请人工确认价格位置后再行动。"
        if failure_reason:
            market_note = f"{market_note}（失败原因：{failure_reason}）"
        card_id = hashlib.sha1(f"{dedup_key}:{created_at.isoformat()}".encode("utf-8")).hexdigest()
        return OpportunityCard(
            card_id=card_id,
            event_id=insight.event_id,
            symbol=insight.symbol,
            horizon=horizon,
            event_type=insight.event_type,
            headline_summary=insight.headline_summary,
            bull_case=insight.bull_case,
            bear_case=insight.bear_case,
            event_score=event_score,
            market_score=0.0,
            final_score=event_score,
            entry_range=PriceRange(0.0, 0.0),
            take_profit_range=PriceRange(0.0, 0.0),
            invalidation_level=0.0,
            invalidation_reason="行情快照缺失，暂不提供自动计算的失效价。",
            risk_notes=insight.risk_notes,
            source_refs=insight.source_refs,
            created_at=created_at,
            ttl=created_at + timedelta(days=self.settings.horizons[horizon].ttl_days),
            priority=priority,
            dedup_key=dedup_key,
            bias=bias,
            display_name=insight.symbol,
            action_label=action_label,
            confidence_label=confidence_label,
            confidence_score=confidence_score,
            reason_to_watch=(
                f"{self._event_type_cn(insight.event_type)}强度 {event_score:.1f} 分；"
                "当前行情确认缺失，请先加入观察名单，待价格位置与量价结构明确后再决定是否参与。"
            ),
            market_data_complete=False,
            market_data_note=market_note,
            positioning_hint=self._positioning_hint(action_label, bias=bias),
        )

    def _action_and_confidence(
        self,
        *,
        priority: str,
        final_score: float,
        market_data_complete: bool,
        bias: str,
    ) -> tuple[str, str, float]:
        if bias != "long":
            if not market_data_complete:
                return ("加入观察", "低", round(min(final_score, 58.0), 2))
            return ("加入观察", "中", round(clamp(final_score, 45.0, 72.0), 2))
        if not market_data_complete:
            return ("加入观察", "低", round(min(final_score, 58.0), 2))
        if priority == "high":
            return ("确认做多", "高", round(clamp(max(final_score, 75.0), 0.0, 100.0), 2))
        if priority == "normal":
            return ("试探建仓", "中", round(clamp(max(final_score, 60.0), 0.0, 100.0), 2))
        return ("加入观察", "低", round(clamp(final_score, 0.0, 59.0), 2))

    def _positioning_hint(self, action_label: str, *, bias: str) -> str:
        if bias != "long":
            return "当前仍以风险观察为主，暂不将其视为可直接执行的反向交易信号。"
        return {
            "加入观察": "先纳入观察名单，优先等待价格结构或事件催化进一步明朗。",
            "试探建仓": "可先用观察仓或轻仓试错，优先等待回踩承接或放量确认。",
            "确认做多": "事件与市场确认已经成形，可按正式做多机会管理计划。",
        }.get(action_label, "")

    def _structure_note(self, snapshot: IndicatorSnapshot, bias: str) -> str:
        if bias != "long":
            return "当前先作为风险观察，不建议直接按反向交易处理。"
        if snapshot.is_pullback:
            return "价格处于回踩承接区，更适合等待企稳后再放大仓位。"
        if snapshot.intraday_breakout:
            return "价格正处于突破确认区，适合结合量能判断是否跟随。"
        return "价格站在关键均线附近，可继续观察是否形成更明确的进攻结构。"

    def _event_score(self, insight: EventInsight) -> float:
        sentiment_strength = abs(insight.sentiment) * 100.0
        return clamp(
            (0.30 * insight.importance)
            + (0.25 * insight.source_credibility)
            + (0.20 * insight.novelty)
            + (0.15 * insight.theme_relevance)
            + (0.10 * sentiment_strength),
            0.0,
            100.0,
        )

    def _market_score(
        self,
        snapshot: IndicatorSnapshot,
        bias: str,
        rsi_floor: float,
        rsi_ceiling: float,
        atr_percent_ceiling: float,
    ) -> float:
        trend_score = self._trend_score(snapshot, bias)
        volume_score = clamp(snapshot.relative_volume * 55.0, 20.0, 100.0)
        volatility_score = self._volatility_score(snapshot.atr_percent, atr_percent_ceiling)
        proximity_score = self._proximity_score(snapshot, bias)
        rsi_score = self._rsi_score(snapshot.rsi_14, bias, rsi_floor, rsi_ceiling)
        return clamp(
            (0.25 * trend_score)
            + (0.15 * volume_score)
            + (0.20 * volatility_score)
            + (0.20 * proximity_score)
            + (0.20 * rsi_score),
            0.0,
            100.0,
        )

    def _trend_score(self, snapshot: IndicatorSnapshot, bias: str) -> float:
        if bias == "long":
            return {"bullish": 88.0, "neutral": 58.0, "bearish": 30.0}[snapshot.trend_state]
        return {"bullish": 30.0, "neutral": 58.0, "bearish": 88.0}[snapshot.trend_state]

    def _volatility_score(self, atr_percent: float, ceiling: float) -> float:
        if atr_percent <= ceiling:
            return 85.0
        overflow = min((atr_percent - ceiling) * 8.0, 55.0)
        return clamp(85.0 - overflow, 20.0, 85.0)

    def _proximity_score(self, snapshot: IndicatorSnapshot, bias: str) -> float:
        if bias == "long":
            if snapshot.is_pullback:
                return 85.0
            if snapshot.intraday_breakout:
                return 78.0
            distance = abs(snapshot.last_price - snapshot.support_20)
            atr = max(snapshot.atr_14, 0.01)
            return clamp(85.0 - ((distance / atr) * 10.0), 25.0, 85.0)
        distance = abs(snapshot.resistance_20 - snapshot.last_price)
        atr = max(snapshot.atr_14, 0.01)
        return clamp(85.0 - ((distance / atr) * 10.0), 25.0, 85.0)

    def _rsi_score(self, rsi: float, bias: str, floor: float, ceiling: float) -> float:
        if bias == "long":
            if floor <= rsi <= ceiling:
                return 90.0
            if 35.0 <= rsi < floor:
                return 72.0
            return 38.0
        if 100.0 - ceiling <= rsi <= 100.0 - floor:
            return 90.0
        if 100.0 - floor < rsi <= 65.0:
            return 72.0
        return 38.0

    def _event_type_cn(self, event_type: str) -> str:
        return {
            "earnings": "财报事件",
            "guidance": "指引事件",
            "sec": "公告事件",
            "research": "研报事件",
            "m&a": "并购事件",
            "strategic": "战略事件",
            "product": "产品事件",
            "news": "新闻事件",
        }.get(event_type, "事件")
