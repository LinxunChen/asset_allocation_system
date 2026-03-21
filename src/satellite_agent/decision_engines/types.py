from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..models import PriceRange


@dataclass(frozen=True)
class EventAssessment:
    symbol: str
    event_type: str
    event_strength: float
    direction: str
    source_quality_tier: str
    credibility: float
    novelty: float
    theme_candidates: list[str] = field(default_factory=list)
    reason_fragments: list[str] = field(default_factory=list)
    llm_used: bool = False
    llm_confidence: float = 0.0
    event_prewatch_eligible: bool = False
    headline_summary: str = ""
    source_refs: list[str] = field(default_factory=list)
    risk_notes: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PricePlan:
    entry_range: PriceRange
    take_profit_range: PriceRange
    invalidation_level: float
    invalidation_reason: str
    ttl_iso: str = ""

    def to_record(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["entry_range"] = asdict(self.entry_range)
        payload["take_profit_range"] = asdict(self.take_profit_range)
        return payload


@dataclass(frozen=True)
class MarketAssessment:
    trend_state: str
    momentum_state: str
    volume_state: str
    position_state: str
    volatility_state: str
    market_confirmation_score: float
    entry_plan: PricePlan | None
    risk_flags: list[str] = field(default_factory=list)
    prewatch_structure_eligible: bool = False
    exit_signal_state: str = "hold"
    market_data_complete: bool = True
    market_data_note: str = ""
    rsi_14: float | None = None
    relative_volume: float | None = None

    def to_record(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.entry_plan is not None:
            payload["entry_plan"] = self.entry_plan.to_record()
        return payload


@dataclass(frozen=True)
class ThemeAssessment:
    theme_ids: list[str] = field(default_factory=list)
    theme_heat: float = 0.0
    theme_role: str = "standalone"
    confirmed_peers: list[str] = field(default_factory=list)
    prewatch_peers: list[str] = field(default_factory=list)
    dynamic_theme_detected: bool = False
    theme_chain_note: str = ""
    theme_boosts: dict[str, float] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReasonSections:
    event: str = ""
    market: str = ""
    theme: str = ""
    positioning: str = ""

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceBundle:
    refs: list[str] = field(default_factory=list)
    quality_tier: str = "secondary"

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DecisionPacket:
    pool: str
    action: str
    priority: str
    confidence: str
    symbol: str
    horizon: str
    event_id: str
    event_assessment: EventAssessment
    market_assessment: MarketAssessment
    theme_assessment: ThemeAssessment
    price_plan: PricePlan | None
    reason_sections: ReasonSections
    source_bundle: SourceBundle
    final_score: float
    event_score: float
    market_score: float
    theme_score: float
    trigger_mode: str = ""
    llm_used: bool = False

    def to_record(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_assessment"] = self.event_assessment.to_record()
        payload["market_assessment"] = self.market_assessment.to_record()
        payload["theme_assessment"] = self.theme_assessment.to_record()
        payload["reason_sections"] = self.reason_sections.to_record()
        payload["source_bundle"] = self.source_bundle.to_record()
        if self.price_plan is not None:
            payload["price_plan"] = self.price_plan.to_record()
        return payload
