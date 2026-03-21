from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..llm import OpenAIExtractor, RuleBasedExtractor
from ..models import EventInsight, SourceEvent
from .types import EventAssessment

LOW_SIGNAL_LLM_TERMS: tuple[str, ...] = (
    "stock price today",
    "price prediction",
    "top 10",
    "best stocks",
    "should you buy",
    "history",
    "battle royale",
)

EVENT_QUALITY_TIERS: dict[str, str] = {
    "filing": "official",
    "earnings": "official",
    "press_release": "official",
    "research": "secondary",
    "news": "secondary",
}

EVENT_PREWATCH_TYPES = {"earnings", "guidance", "m&a", "strategic", "product"}


class EventUnderstandingEngine(Protocol):
    def understand(self, event: SourceEvent, *, allow_llm: bool = True) -> tuple[EventInsight, EventAssessment, dict]:
        ...


def _source_quality_tier(event: SourceEvent, insight: EventInsight) -> str:
    if insight.source_credibility >= 88:
        return "official"
    if insight.source_credibility >= 78:
        return "primary_media"
    return EVENT_QUALITY_TIERS.get(event.source_type, "secondary")


def _direction(insight: EventInsight) -> str:
    if insight.sentiment > 0.15:
        return "bullish"
    if insight.sentiment < -0.15:
        return "bearish"
    return "neutral"


def _theme_candidates(event: SourceEvent, insight: EventInsight) -> list[str]:
    themes: list[str] = []
    text = f"{event.headline} {event.summary}".lower()
    keywords = (
        ("半导体与AI", ("ai", "gpu", "chip", "semiconductor", "data center")),
        ("软件与云", ("cloud", "software", "saas", "platform")),
        ("金融与金融科技", ("bank", "payment", "fintech", "credit")),
        ("医疗健康", ("drug", "health", "biotech", "medical")),
    )
    for label, terms in keywords:
        if any(term in text for term in terms) and label not in themes:
            themes.append(label)
    return themes


def _reason_fragments(insight: EventInsight) -> list[str]:
    fragments = [insight.headline_summary.strip()]
    if insight.bull_case:
        fragments.append(insight.bull_case.strip())
    if insight.bear_case:
        fragments.append(insight.bear_case.strip())
    return [item for item in fragments if item]


def build_event_assessment(
    event: SourceEvent,
    insight: EventInsight,
    *,
    llm_used: bool,
) -> EventAssessment:
    strength = round(
        (0.45 * insight.importance)
        + (0.25 * insight.novelty)
        + (0.20 * insight.source_credibility)
        + (0.10 * abs(insight.sentiment) * 100.0),
        2,
    )
    return EventAssessment(
        symbol=insight.symbol,
        event_type=insight.event_type,
        event_strength=strength,
        direction=_direction(insight),
        source_quality_tier=_source_quality_tier(event, insight),
        credibility=round(insight.source_credibility, 2),
        novelty=round(insight.novelty, 2),
        theme_candidates=_theme_candidates(event, insight),
        reason_fragments=_reason_fragments(insight),
        llm_used=llm_used,
        llm_confidence=round(insight.llm_confidence, 2),
        event_prewatch_eligible=insight.event_type in EVENT_PREWATCH_TYPES and strength >= 74.0,
        headline_summary=insight.headline_summary,
        source_refs=list(insight.source_refs),
        risk_notes=list(insight.risk_notes),
    )


@dataclass
class RuleEventUnderstandingEngine:
    extractor: RuleBasedExtractor

    def understand(self, event: SourceEvent, *, allow_llm: bool = True) -> tuple[EventInsight, EventAssessment, dict]:
        insight = self.extractor.extract(event)
        assessment = build_event_assessment(event, insight, llm_used=False)
        return insight, assessment, {
            "used_llm": False,
            "success": True,
            "reason": "rule_only",
            "prompt_tokens_estimate": 0,
            "completion_tokens_estimate": 0,
        }


@dataclass
class LLMEventUnderstandingEngine:
    extractor: OpenAIExtractor

    def understand(self, event: SourceEvent, *, allow_llm: bool = True) -> tuple[EventInsight, EventAssessment, dict]:
        insight, metadata = self.extractor.extract_with_metadata(event)
        assessment = build_event_assessment(event, insight, llm_used=bool(metadata.get("used_llm")))
        return insight, assessment, metadata


@dataclass
class HybridEventUnderstandingEngine:
    rule_extractor: RuleBasedExtractor
    llm_extractor: OpenAIExtractor | None = None

    def understand(self, event: SourceEvent, *, allow_llm: bool = True) -> tuple[EventInsight, EventAssessment, dict]:
        if not allow_llm or self.llm_extractor is None or not self.llm_extractor.api_key:
            return RuleEventUnderstandingEngine(self.rule_extractor).understand(event, allow_llm=False)
        text = f"{event.headline} {event.summary}".lower()
        if any(term in text for term in LOW_SIGNAL_LLM_TERMS):
            return RuleEventUnderstandingEngine(self.rule_extractor).understand(event, allow_llm=False)
        return LLMEventUnderstandingEngine(self.llm_extractor).understand(event, allow_llm=True)
