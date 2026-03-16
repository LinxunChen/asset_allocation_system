from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict
from urllib import request

from .models import EventInsight, SourceEvent, utcnow


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def _keyword_score(text: str, positive: tuple[str, ...], negative: tuple[str, ...]) -> float:
    text = text.lower()
    positive_hits = sum(1 for term in positive if term in text)
    negative_hits = sum(1 for term in negative if term in text)
    if positive_hits == negative_hits == 0:
        return 0.0
    total = positive_hits + negative_hits
    return (positive_hits - negative_hits) / total


@dataclass
class RuleBasedExtractor:
    def extract(self, event: SourceEvent) -> EventInsight:
        text = f"{event.headline}. {event.summary}".lower()
        event_type = self._event_type(text)
        sentiment = _keyword_score(
            text,
            positive=("beat", "raise", "upgrade", "surge", "launch", "approval", "partnership"),
            negative=("miss", "cut", "downgrade", "lawsuit", "probe", "delay", "recall"),
        )
        importance = 85.0 if event_type in {"earnings", "guidance", "sec", "m&a", "strategic"} else 68.0
        credibility = 90.0 if event.source_type in {"filing", "earnings", "press_release"} else 75.0
        novelty = 80.0 if any(
            word in text
            for word in (
                "new",
                "first",
                "launch",
                "initiate",
                "investment",
                "invests",
                "invested",
                "strategic",
                "partnership",
                "collaboration",
                "stake",
                "funding",
                "backed",
            )
        ) else 62.0
        theme_relevance = 78.0 if any(word in text for word in ("ai", "cloud", "chip", "data center")) else 60.0
        bull_case = self._bull_case(event_type, sentiment)
        bear_case = self._bear_case(event_type, sentiment)
        risk_notes = self._risk_notes(event_type, sentiment)
        return EventInsight(
            event_id=event.event_id,
            symbol=event.symbol,
            event_type=event_type,
            headline_summary=self._headline_summary(event, event_type),
            bull_case=bull_case,
            bear_case=bear_case,
            importance=importance,
            source_credibility=credibility,
            novelty=novelty,
            sentiment=sentiment,
            theme_relevance=theme_relevance,
            llm_confidence=72.0,
            risk_notes=risk_notes,
            source_refs=[value for value in [event.url, event.source] if value],
            raw_payload={"mode": "rule_based"},
            created_at=utcnow(),
        )

    def _event_type(self, text: str) -> str:
        if any(word in text for word in ("earnings", "eps", "revenue", "quarter")):
            return "earnings"
        if any(word in text for word in ("guidance", "outlook", "forecast")):
            return "guidance"
        if any(word in text for word in ("8-k", "10-q", "10-k", "sec", "filing")):
            return "sec"
        if any(word in text for word in ("upgrade", "downgrade", "price target", "analyst")):
            return "research"
        if any(word in text for word in ("merger", "acquisition", "buyout")):
            return "m&a"
        if any(
            word in text
            for word in (
                "strategic partnership",
                "partnership",
                "collaboration",
                "investment",
                "invests",
                "invested",
                "stake",
                "backed",
                "funding",
            )
        ):
            return "strategic"
        if any(word in text for word in ("launch", "release", "announce", "product")):
            return "product"
        return "news"

    def _bull_case(self, event_type: str, sentiment: float) -> str:
        if sentiment >= 0:
            return f"{self._event_type_cn(event_type)}若被市场继续验证，股价可能延续当前方向。"
        return f"{self._event_type_cn(event_type)}虽偏负面，但若市场反应过度，仍可能出现情绪修复。"

    def _bear_case(self, event_type: str, sentiment: float) -> str:
        if sentiment >= 0:
            return f"{self._event_type_cn(event_type)}可能已被部分定价，追涨后存在回落风险。"
        return f"{self._event_type_cn(event_type)}若对应基本面走弱，首轮波动后仍可能继续下探。"

    def _risk_notes(self, event_type: str, sentiment: float) -> list[str]:
        base = ["需结合成交量与事件后的波动扩张情况确认信号有效性。"]
        if sentiment < 0:
            base.append("负面催化可能带来跳空与反弹失败风险。")
        if event_type == "earnings":
            base.append("财报后波动往往放大，过紧的止损容易被噪音触发。")
        return base

    def _headline_summary(self, event: SourceEvent, event_type: str) -> str:
        templates = {
            "earnings": f"检测到与 {event.symbol} 相关的财报资讯，建议重点核对营收、利润和管理层表述。",
            "guidance": f"检测到与 {event.symbol} 相关的业绩指引资讯，建议确认后续展望是否上修或下修。",
            "sec": f"检测到与 {event.symbol} 相关的公告或监管披露，建议结合原文确认关键变动。",
            "research": f"检测到与 {event.symbol} 相关的研究评级资讯，建议核对评级或目标价是否调整。",
            "m&a": f"检测到与 {event.symbol} 相关的并购资讯，建议确认交易条款与潜在影响。",
            "strategic": f"检测到与 {event.symbol} 相关的战略合作或资本加持资讯，建议重点核对合作规模、投资方与后续兑现路径。",
            "product": f"检测到与 {event.symbol} 相关的产品或发布动态，需确认是否能转化为经营催化。",
            "news": f"检测到与 {event.symbol} 相关的新闻事件，建议结合原文判断是否构成有效交易催化。",
        }
        return templates.get(event_type, templates["news"])[:280]

    def _event_type_cn(self, event_type: str) -> str:
        return {
            "earnings": "财报催化",
            "guidance": "指引催化",
            "sec": "公告催化",
            "research": "研报催化",
            "m&a": "并购催化",
            "strategic": "战略催化",
            "product": "产品催化",
            "news": "新闻催化",
        }.get(event_type, "事件催化")


class OpenAIExtractor(RuleBasedExtractor):
    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    def extract(self, event: SourceEvent) -> EventInsight:
        if not self.api_key:
            return super().extract(event)
        prompt = self._build_prompt(event)
        try:
            payload = self._call_api(prompt)
            return self._payload_to_insight(event, payload)
        except Exception:
            return super().extract(event)

    def _build_prompt(self, event: SourceEvent) -> str:
        return (
            "You are extracting a structured investment event card. "
            "Respond with JSON only using keys: event_type, headline_summary, bull_case, "
            "bear_case, importance, source_credibility, novelty, sentiment, theme_relevance, "
            "llm_confidence, risk_notes.\n"
            f"Symbol: {event.symbol}\n"
            f"Source: {event.source}\n"
            f"Headline: {event.headline}\n"
            f"Summary: {event.summary}\n"
        )

    def _call_api(self, prompt: str) -> Dict[str, Any]:
        body = json.dumps(
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "Return strict JSON. No markdown."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
            }
        ).encode("utf-8")
        req = request.Request(
            self.base_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        with request.urlopen(req, timeout=20) as response:
            raw = json.loads(response.read().decode("utf-8"))
        content = raw["choices"][0]["message"]["content"]
        match = JSON_BLOCK_RE.search(content)
        if not match:
            raise ValueError("LLM response did not contain JSON")
        return json.loads(match.group(0))

    def _payload_to_insight(self, event: SourceEvent, payload: Dict[str, Any]) -> EventInsight:
        fallback = super().extract(event)
        def bounded(name: str, default: float) -> float:
            value = payload.get(name, default)
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = default
            if name == "sentiment":
                return max(-1.0, min(1.0, value))
            return max(0.0, min(100.0, value))

        risk_notes = payload.get("risk_notes", fallback.risk_notes)
        if not isinstance(risk_notes, list):
            risk_notes = fallback.risk_notes
        return EventInsight(
            event_id=event.event_id,
            symbol=event.symbol,
            event_type=str(payload.get("event_type", fallback.event_type)),
            headline_summary=str(payload.get("headline_summary", fallback.headline_summary))[:280],
            bull_case=str(payload.get("bull_case", fallback.bull_case)),
            bear_case=str(payload.get("bear_case", fallback.bear_case)),
            importance=bounded("importance", fallback.importance),
            source_credibility=bounded("source_credibility", fallback.source_credibility),
            novelty=bounded("novelty", fallback.novelty),
            sentiment=bounded("sentiment", fallback.sentiment),
            theme_relevance=bounded("theme_relevance", fallback.theme_relevance),
            llm_confidence=bounded("llm_confidence", fallback.llm_confidence),
            risk_notes=[str(note) for note in risk_notes],
            source_refs=[value for value in [event.url, event.source] if value],
            raw_payload=payload,
            created_at=utcnow(),
        )
