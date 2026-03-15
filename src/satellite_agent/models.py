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
    reason_to_watch: str = ""

    def ttl_delta(self) -> timedelta:
        return self.ttl - self.created_at

    def to_record(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["entry_range"] = asdict(self.entry_range)
        payload["take_profit_range"] = asdict(self.take_profit_range)
        payload["created_at"] = ensure_utc(self.created_at).isoformat()
        payload["ttl"] = ensure_utc(self.ttl).isoformat()
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
