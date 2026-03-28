from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


UTC = timezone.utc


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass
class SourceEvent:
    event_id: str
    source: str
    source_type: str
    symbol: str
    headline: str
    summary: str
    published_at: datetime
    url: str = ""
    sentiment_hint: Optional[float] = None
    event_fingerprint: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["published_at"] = ensure_utc(self.published_at).isoformat()
        return payload


@dataclass
class EventInsight:
    event_id: str
    symbol: str
    event_type: str
    headline_summary: str
    bull_case: str
    bear_case: str
    importance: float
    source_credibility: float
    novelty: float
    sentiment: float
    theme_relevance: float
    llm_confidence: float
    risk_notes: List[str]
    source_refs: List[str]
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utcnow)

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = ensure_utc(self.created_at).isoformat()
        return payload


@dataclass
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    adjusted: bool = False


@dataclass
class IndicatorSnapshot:
    symbol: str
    horizon: str
    as_of: datetime
    last_price: float
    rsi_14: float
    atr_14: float
    sma_20: float
    sma_60: float
    relative_volume: float
    support_20: float
    resistance_20: float
    support_60: float
    resistance_60: float
    gap_percent: float
    intraday_breakout: bool
    is_pullback: bool
    trend_state: str
    atr_percent: float

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = ensure_utc(self.as_of).isoformat()
        return payload


@dataclass
class PriceRange:
    low: float
    high: float

    def normalized(self) -> "PriceRange":
        low, high = sorted((round(self.low, 2), round(self.high, 2)))
        return PriceRange(low=low, high=high)


@dataclass
class OpportunityCard:
    card_id: str
    event_id: str
    symbol: str
    horizon: str
    event_type: str
    headline_summary: str
    bull_case: str
    bear_case: str
    event_score: float
    market_score: float
    final_score: float
    entry_range: PriceRange
    take_profit_range: PriceRange
    invalidation_level: float
    invalidation_reason: str
    risk_notes: List[str]
    source_refs: List[str]
    created_at: datetime
    ttl: datetime
    priority: str
    dedup_key: str
    bias: str = "long"
    display_name: str = ""
    action_label: str = ""
    confidence_label: str = ""
    confidence_score: float = 0.0
    reason_to_watch: str = ""
    trend_state: str = ""
    rsi_14: Optional[float] = None
    relative_volume: Optional[float] = None
    theme_tags: List[str] = field(default_factory=list)
    confirmed_peer_symbols: List[str] = field(default_factory=list)
    chain_summary: str = ""
    llm_summary: str = ""
    llm_impact_inference: str = ""
    llm_reasoning: str = ""
    llm_uncertainty: str = ""
    market_regime: str = ""
    rate_risk: str = ""
    geopolitical_risk: str = ""
    macro_risk_score: float = 0.0
    macro_penalty_applied: float = 0.0
    macro_action_before_overlay: str = ""
    macro_overlay_note: str = ""
    narrative_priority_adjustment: float = 0.0
    market_data_complete: bool = True
    market_data_note: str = ""
    promoted_from_prewatch: bool = False
    candidate_score: float = 0.0
    candidate_setup_type: str = ""
    positioning_hint: str = ""
    execution_eligible: bool = True
    execution_note: str = ""
    lifecycle_pool: str = ""
    delivery_category: str = ""
    normalized_close_reason: str = ""
    holding_management_reason: str = ""
    holding_management_subreason: str = ""
    holding_management_source_decision_id: str = ""
    candidate_observation_count: int = 0
    candidate_alert_sent_count: int = 0
    candidate_first_seen_at: str = ""
    candidate_last_seen_at: str = ""
    candidate_last_alert_sent_at: str = ""
    candidate_source_decision_id: str = ""
    candidate_promotion_reason: str = ""
    active_cycle_status: str = ""
    previous_formal_action: str = ""
    previous_formal_sent: bool = False
    downgraded_from_formal: bool = False
    downgrade_explainer: str = ""
    recent_candidate_observation_count_72h: int = 0
    is_breakthrough_event: bool = False

    def ttl_delta(self) -> timedelta:
        return self.ttl - self.created_at

    @property
    def prewatch_score(self) -> float:
        return self.candidate_score

    @prewatch_score.setter
    def prewatch_score(self, value: float) -> None:
        self.candidate_score = float(value or 0.0)

    @property
    def prewatch_setup_type(self) -> str:
        return self.candidate_setup_type

    @prewatch_setup_type.setter
    def prewatch_setup_type(self, value: str) -> None:
        self.candidate_setup_type = str(value or "")

    @property
    def prewatch_observation_count(self) -> int:
        return self.candidate_observation_count

    @prewatch_observation_count.setter
    def prewatch_observation_count(self, value: int) -> None:
        self.candidate_observation_count = int(value or 0)

    @property
    def prewatch_alert_sent_count(self) -> int:
        return self.candidate_alert_sent_count

    @prewatch_alert_sent_count.setter
    def prewatch_alert_sent_count(self, value: int) -> None:
        self.candidate_alert_sent_count = int(value or 0)

    @property
    def prewatch_first_seen_at(self) -> str:
        return self.candidate_first_seen_at

    @prewatch_first_seen_at.setter
    def prewatch_first_seen_at(self, value: str) -> None:
        self.candidate_first_seen_at = str(value or "")

    @property
    def prewatch_last_seen_at(self) -> str:
        return self.candidate_last_seen_at

    @prewatch_last_seen_at.setter
    def prewatch_last_seen_at(self, value: str) -> None:
        self.candidate_last_seen_at = str(value or "")

    @property
    def prewatch_last_alert_sent_at(self) -> str:
        return self.candidate_last_alert_sent_at

    @prewatch_last_alert_sent_at.setter
    def prewatch_last_alert_sent_at(self, value: str) -> None:
        self.candidate_last_alert_sent_at = str(value or "")

    @property
    def prewatch_source_decision_id(self) -> str:
        return self.candidate_source_decision_id

    @prewatch_source_decision_id.setter
    def prewatch_source_decision_id(self, value: str) -> None:
        self.candidate_source_decision_id = str(value or "")

    @property
    def prewatch_promotion_reason(self) -> str:
        return self.candidate_promotion_reason

    @prewatch_promotion_reason.setter
    def prewatch_promotion_reason(self, value: str) -> None:
        self.candidate_promotion_reason = str(value or "")

    @property
    def exit_pool_subreason(self) -> str:
        return self.holding_management_subreason

    @exit_pool_subreason.setter
    def exit_pool_subreason(self, value: str) -> None:
        self.holding_management_subreason = str(value or "")

    @property
    def exit_pool_source_decision_id(self) -> str:
        return self.holding_management_source_decision_id

    @exit_pool_source_decision_id.setter
    def exit_pool_source_decision_id(self, value: str) -> None:
        self.holding_management_source_decision_id = str(value or "")

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["entry_range"] = asdict(self.entry_range)
        payload["take_profit_range"] = asdict(self.take_profit_range)
        payload["created_at"] = ensure_utc(self.created_at).isoformat()
        payload["ttl"] = ensure_utc(self.ttl).isoformat()
        payload["candidate_score"] = self.candidate_score
        payload["candidate_setup_type"] = self.candidate_setup_type
        payload["candidate_observation_count"] = self.candidate_observation_count
        payload["candidate_alert_sent_count"] = self.candidate_alert_sent_count
        payload["candidate_first_seen_at"] = self.candidate_first_seen_at
        payload["candidate_last_seen_at"] = self.candidate_last_seen_at
        payload["candidate_last_alert_sent_at"] = self.candidate_last_alert_sent_at
        payload["candidate_source_decision_id"] = self.candidate_source_decision_id
        payload["candidate_promotion_reason"] = self.candidate_promotion_reason
        payload["active_cycle_status"] = self.active_cycle_status
        payload["previous_formal_action"] = self.previous_formal_action
        payload["previous_formal_sent"] = self.previous_formal_sent
        payload["downgraded_from_formal"] = self.downgraded_from_formal
        payload["downgrade_explainer"] = self.downgrade_explainer
        payload["recent_candidate_observation_count_72h"] = self.recent_candidate_observation_count_72h
        payload["is_breakthrough_event"] = self.is_breakthrough_event
        payload["holding_management_subreason"] = self.holding_management_subreason
        payload["holding_management_source_decision_id"] = self.holding_management_source_decision_id
        payload["prewatch_score"] = self.prewatch_score
        payload["prewatch_setup_type"] = self.prewatch_setup_type
        payload["prewatch_observation_count"] = self.prewatch_observation_count
        payload["prewatch_alert_sent_count"] = self.prewatch_alert_sent_count
        payload["prewatch_first_seen_at"] = self.prewatch_first_seen_at
        payload["prewatch_last_seen_at"] = self.prewatch_last_seen_at
        payload["prewatch_last_alert_sent_at"] = self.prewatch_last_alert_sent_at
        payload["prewatch_source_decision_id"] = self.prewatch_source_decision_id
        payload["prewatch_promotion_reason"] = self.prewatch_promotion_reason
        payload["exit_pool_subreason"] = self.exit_pool_subreason
        payload["exit_pool_source_decision_id"] = self.exit_pool_source_decision_id
        return payload


@dataclass
class AlertDecision:
    sent: bool
    priority: str
    reason: str
    dedup_key: str
    notified_at: datetime = field(default_factory=utcnow)

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["notified_at"] = ensure_utc(self.notified_at).isoformat()
        return payload


@dataclass
class SourceHealthCheck:
    source_name: str
    status: str
    detail: str
    checked_at: datetime = field(default_factory=utcnow)
    latency_ms: Optional[int] = None

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["checked_at"] = ensure_utc(self.checked_at).isoformat()
        return payload


@dataclass
class CandidatePoolCandidate:
    symbol: str
    horizon: str
    setup_type: str
    score: float
    headline_summary: str
    action_hint: str
    reason_to_watch: str
    last_price: float
    rsi_14: float
    relative_volume: float
    trend_state: str
    support_20: float
    resistance_20: float
    trigger_mode: str = "structure"
    trigger_event_type: str = ""
    trigger_theme: str = ""
    trigger_symbols: List[str] = field(default_factory=list)
    as_of: datetime = field(default_factory=utcnow)

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = ensure_utc(self.as_of).isoformat()
        return payload

# Backward-compatible alias for older call sites and serialized references.
PrewatchCandidate = CandidatePoolCandidate
