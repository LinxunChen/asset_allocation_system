from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..entry_exit import EntryExitEngine
from ..prewatch import build_candidate_pool_candidate
from ..scoring import SignalScorer
from ..config import HorizonSettings
from ..models import EventInsight, IndicatorSnapshot, OpportunityCard, PriceRange
from .types import MarketAssessment, PricePlan


class MarketUnderstandingEngine(Protocol):
    def score_confirmation(self, insight: EventInsight, snapshot: IndicatorSnapshot) -> tuple[OpportunityCard, MarketAssessment]:
        ...

    def score_event_only(
        self,
        insight: EventInsight,
        horizon: str,
        *,
        failure_reason: str = "",
    ) -> tuple[OpportunityCard, MarketAssessment]:
        ...

    def build_candidate_pool(self, snapshot: IndicatorSnapshot, horizon_settings: HorizonSettings, *, min_score: float):
        ...

    def build_prewatch(self, snapshot: IndicatorSnapshot, horizon_settings: HorizonSettings, *, min_score: float):
        ...


def _momentum_state(snapshot: IndicatorSnapshot) -> str:
    if 48.0 <= snapshot.rsi_14 <= 65.0:
        return "healthy"
    if snapshot.rsi_14 < 48.0:
        return "recovering"
    return "overheated"


def _volume_state(snapshot: IndicatorSnapshot) -> str:
    if snapshot.relative_volume >= 1.8:
        return "surging"
    if snapshot.relative_volume >= 1.2:
        return "elevated"
    return "normal"


def _position_state(snapshot: IndicatorSnapshot) -> str:
    if snapshot.intraday_breakout:
        return "breakout"
    if snapshot.is_pullback:
        return "pullback"
    return "range"


def _volatility_state(snapshot: IndicatorSnapshot) -> str:
    if snapshot.atr_percent <= 4.0:
        return "contained"
    if snapshot.atr_percent <= 8.0:
        return "normal"
    return "expanded"


def _exit_signal_state(card: OpportunityCard, snapshot: IndicatorSnapshot) -> str:
    if card.bias != "long":
        return "observe"
    if snapshot.trend_state == "bearish":
        return "risk_exit"
    if snapshot.rsi_14 >= 72.0:
        return "take_profit_watch"
    return "hold"


def build_market_assessment(
    card: OpportunityCard,
    snapshot: IndicatorSnapshot | None,
    *,
    market_data_complete: bool,
    market_data_note: str = "",
) -> MarketAssessment:
    if snapshot is None:
        return MarketAssessment(
            trend_state="unknown",
            momentum_state="unknown",
            volume_state="unknown",
            position_state="unknown",
            volatility_state="unknown",
            market_confirmation_score=card.market_score,
            entry_plan=PricePlan(
                entry_range=card.entry_range,
                take_profit_range=card.take_profit_range,
                invalidation_level=card.invalidation_level,
                invalidation_reason=card.invalidation_reason,
                ttl_iso=card.ttl.isoformat(),
            ),
            risk_flags=list(card.risk_notes),
            candidate_pool_structure_eligible=False,
            exit_signal_state="observe",
            market_data_complete=market_data_complete,
            market_data_note=market_data_note,
        )
    return MarketAssessment(
        trend_state=snapshot.trend_state,
        momentum_state=_momentum_state(snapshot),
        volume_state=_volume_state(snapshot),
        position_state=_position_state(snapshot),
        volatility_state=_volatility_state(snapshot),
        market_confirmation_score=round(card.market_score, 2),
        entry_plan=PricePlan(
            entry_range=card.entry_range,
            take_profit_range=card.take_profit_range,
            invalidation_level=card.invalidation_level,
            invalidation_reason=card.invalidation_reason,
            ttl_iso=card.ttl.isoformat(),
        ),
        risk_flags=list(card.risk_notes),
        candidate_pool_structure_eligible=snapshot.trend_state != "bearish",
        exit_signal_state=_exit_signal_state(card, snapshot),
        market_data_complete=market_data_complete,
        market_data_note=market_data_note,
        rsi_14=round(snapshot.rsi_14, 1),
        relative_volume=round(snapshot.relative_volume, 2),
    )


@dataclass
class RuleMarketUnderstandingEngine:
    scorer: SignalScorer
    entry_exit: EntryExitEngine

    def score_confirmation(self, insight: EventInsight, snapshot: IndicatorSnapshot) -> tuple[OpportunityCard, MarketAssessment]:
        card = self.scorer.score(insight, snapshot)
        card = self.entry_exit.enrich(card, snapshot)
        assessment = build_market_assessment(card, snapshot, market_data_complete=True)
        return card, assessment

    def score_event_only(
        self,
        insight: EventInsight,
        horizon: str,
        *,
        failure_reason: str = "",
    ) -> tuple[OpportunityCard, MarketAssessment]:
        card = self.scorer.score_event_only(insight, horizon, failure_reason=failure_reason)
        assessment = build_market_assessment(
            card,
            None,
            market_data_complete=False,
            market_data_note=card.market_data_note,
        )
        return card, assessment

    def build_candidate_pool(self, snapshot: IndicatorSnapshot, horizon_settings: HorizonSettings, *, min_score: float):
        candidate = build_candidate_pool_candidate(snapshot, horizon_settings, min_score=min_score)
        base_card = OpportunityCard(
            card_id="",
            event_id="",
            symbol=snapshot.symbol,
            horizon=snapshot.horizon,
            event_type="candidate_pool",
            headline_summary="",
            bull_case="",
            bear_case="",
            event_score=0.0,
            market_score=0.0,
            final_score=0.0,
            entry_range=PriceRange(snapshot.last_price, snapshot.last_price),
            take_profit_range=PriceRange(snapshot.last_price, snapshot.last_price),
            invalidation_level=0.0,
            invalidation_reason="",
            risk_notes=[],
            source_refs=[],
            created_at=snapshot.as_of,
            ttl=snapshot.as_of,
            priority="normal",
            dedup_key="",
        )
        # Reuse the assessment builder shape even for candidate-pool flows.
        assessment = build_market_assessment(base_card, snapshot, market_data_complete=True)
        return candidate, assessment

    def build_prewatch(self, snapshot: IndicatorSnapshot, horizon_settings: HorizonSettings, *, min_score: float):
        return self.build_candidate_pool(snapshot, horizon_settings, min_score=min_score)
