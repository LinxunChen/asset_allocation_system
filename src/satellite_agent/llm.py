from __future__ import annotations

import json
import re
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Dict
from urllib import request

from .models import EventInsight, OpportunityCard, SourceEvent, utcnow


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
OPENAI_API_TIMEOUT_SECONDS = 90
SEC_FILING_PATTERNS = (
    re.compile(r"\b8-k\b"),
    re.compile(r"\b10-q\b"),
    re.compile(r"\b10-k\b"),
    re.compile(r"\bsec filing(s)?\b"),
    re.compile(r"\bsecurities and exchange commission\b"),
    re.compile(r"\bfiled with the sec\b"),
    re.compile(r"\bfiled an? (8-k|10-q|10-k)\b"),
    re.compile(r"\bfiling(s)?\b"),
)


def _normalize_narrative_text(text: str, *, max_chars: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    cleaned = cleaned.replace("：", "：").replace("，", "，")
    if len(cleaned) <= max_chars:
        return cleaned
    truncated = cleaned[: max_chars - 1].rstrip("，。；：、,. ")
    return f"{truncated}…"


def _soften_trader_jargon(text: str) -> str:
    softened = str(text or "")
    replacements = (
        ("风险偏好环境提供安全边际", "当前市场环境不拖后腿"),
        ("风险偏好提供安全边际", "当前市场环境不拖后腿"),
        ("提供安全边际", "不至于太吃亏"),
        ("放量确认多头动能", "成交量放大，说明有资金在跟"),
        ("量价配合共振", "价格走强、量能也有跟上"),
        ("量价确认", "价格和成交量都在配合"),
        ("顺趋势的二次确认", "顺着当前强势方向跟随"),
        ("动能确认", "资金还在跟"),
        ("多头动能", "做多资金在跟"),
        ("结构向上", "价格结构偏强"),
        ("结构走弱", "价格结构偏弱"),
    )
    for source, target in replacements:
        softened = softened.replace(source, target)
    softened = re.sub(r"安全边际", "缓冲", softened)
    return softened


def _keyword_score(text: str, positive: tuple[str, ...], negative: tuple[str, ...]) -> float:
    text = text.lower()
    positive_hits = sum(1 for term in positive if term in text)
    negative_hits = sum(1 for term in negative if term in text)
    if positive_hits == negative_hits == 0:
        return 0.0
    total = positive_hits + negative_hits
    return (positive_hits - negative_hits) / total


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def _build_openai_chat_body(
    *,
    model: str,
    prompt: str,
    temperature: float,
    max_tokens: int,
) -> bytes:
    return json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": "Return strict JSON. No markdown."},
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
            "enable_thinking": False,
        }
    ).encode("utf-8")


def _extract_message_content(raw: Dict[str, Any]) -> str:
    message = raw["choices"][0]["message"]
    content = message.get("content")
    if isinstance(content, list):
        text_parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                text_parts.append(str(part.get("text", "")))
        content = "".join(text_parts)
    if not content:
        content = message.get("reasoning_content", "")
    return str(content or "")


def _extract_usage_metadata(raw: Dict[str, Any]) -> dict[str, int]:
    usage = raw.get("usage") or {}
    try:
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
    except (TypeError, ValueError):
        prompt_tokens = 0
    try:
        completion_tokens = int(usage.get("completion_tokens") or 0)
    except (TypeError, ValueError):
        completion_tokens = 0
    return {
        "prompt_tokens": max(0, prompt_tokens),
        "completion_tokens": max(0, completion_tokens),
    }


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
        if any(pattern.search(text) for pattern in SEC_FILING_PATTERNS):
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
        insight, _ = self.extract_with_metadata(event)
        return insight

    def extract_with_metadata(self, event: SourceEvent) -> tuple[EventInsight, Dict[str, Any]]:
        if not self.api_key:
            return super().extract(event), {
                "used_llm": False,
                "success": False,
                "reason": "missing_api_key",
                "model": self.model,
                "prompt_tokens_estimate": 0,
                "completion_tokens_estimate": 0,
            }
        prompt = self._build_prompt(event)
        prompt_tokens = _estimate_tokens(prompt)
        try:
            payload, call_metadata = self._call_api_with_metadata(prompt)
            insight = self._payload_to_insight(event, payload)
            completion_tokens = int(call_metadata.get("completion_tokens") or 0)
            if completion_tokens <= 0:
                completion_tokens = _estimate_tokens(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return insight, {
                "used_llm": True,
                "success": True,
                "reason": "ok",
                "model": self.model,
                "prompt_tokens_estimate": int(call_metadata.get("prompt_tokens") or prompt_tokens),
                "completion_tokens_estimate": completion_tokens,
                "latency_ms": int(call_metadata.get("latency_ms") or 0),
            }
        except Exception as exc:
            return super().extract(event), {
                "used_llm": True,
                "success": False,
                "reason": f"api_error:{exc.__class__.__name__}",
                "model": self.model,
                "prompt_tokens_estimate": prompt_tokens,
                "completion_tokens_estimate": 0,
                "latency_ms": 0,
            }

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
        payload, _ = self._call_api_with_metadata(prompt)
        return payload

    def _call_api_with_metadata(self, prompt: str) -> tuple[Dict[str, Any], Dict[str, int]]:
        body = _build_openai_chat_body(
            model=self.model,
            prompt=prompt,
            temperature=0.1,
            max_tokens=400,
        )
        req = request.Request(
            self.base_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        started = perf_counter()
        with request.urlopen(req, timeout=OPENAI_API_TIMEOUT_SECONDS) as response:
            raw = json.loads(response.read().decode("utf-8"))
        latency_ms = int((perf_counter() - started) * 1000)
        content = _extract_message_content(raw)
        match = JSON_BLOCK_RE.search(content)
        if not match:
            raise ValueError("LLM response did not contain JSON")
        return json.loads(match.group(0)), {
            **_extract_usage_metadata(raw),
            "latency_ms": latency_ms,
        }

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


@dataclass
class NarrativeOutput:
    summary: str = ""
    impact_inference: str = ""
    reasoning: str = ""
    uncertainty: str = ""
    priority_adjustment: float = 0.0


class OpenAINarrator:
    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        self.api_key = api_key
        self.model = model
        self.base_url = base_url

    def narrate(
        self,
        *,
        insight: EventInsight,
        card: OpportunityCard,
        market_regime: str,
        rate_risk: str,
        geopolitical_risk: str,
        theme_text: str,
        chain_summary: str,
    ) -> NarrativeOutput:
        output, _ = self.narrate_with_metadata(
            insight=insight,
            card=card,
            market_regime=market_regime,
            rate_risk=rate_risk,
            geopolitical_risk=geopolitical_risk,
            theme_text=theme_text,
            chain_summary=chain_summary,
        )
        return output

    def narrate_with_metadata(
        self,
        *,
        insight: EventInsight,
        card: OpportunityCard,
        market_regime: str,
        rate_risk: str,
        geopolitical_risk: str,
        theme_text: str,
        chain_summary: str,
        lite: bool = False,
    ) -> tuple[NarrativeOutput, Dict[str, Any]]:
        if not self.api_key:
            return NarrativeOutput(), {
                "used_llm": False,
                "success": False,
                "reason": "missing_api_key",
                "model": self.model,
                "prompt_tokens_estimate": 0,
                "completion_tokens_estimate": 0,
                "latency_ms": 0,
            }
        if lite:
            prompt = (
                "Rewrite this prewatch stock alert for a Chinese-speaking trader. "
                "Return JSON only with keys: summary, impact_inference, reasoning, uncertainty, priority_adjustment. "
                "Use concise Simplified Chinese. No hype, no markdown, no extra keys. "
                "summary: facts only, 1 short sentence. "
                "impact_inference: 1 short sentence explaining what still needs confirmation before upgrading. "
                "reasoning: 1 short sentence explaining why it stays on watch instead of direct execution. "
                "uncertainty: 1 short sentence naming the main failure mode. "
                "priority_adjustment: always return 0.\n"
                f"Symbol: {card.symbol}\n"
                f"Event type: {insight.event_type}\n"
                f"Headline: {insight.headline_summary}\n"
                f"Action: {card.action_label}\n"
                f"Trend state: {card.trend_state}\n"
                f"Theme: {theme_text}\n"
                f"Chain summary: {chain_summary}\n"
                f"RSI: {card.rsi_14}\n"
                f"Relative volume: {card.relative_volume}\n"
            )
        else:
            prompt = (
                "Rewrite this stock alert card for a Chinese-speaking trader. "
                "Return JSON only with keys: summary, impact_inference, reasoning, uncertainty, priority_adjustment. "
                "Use concise Simplified Chinese. No hype, no markdown, no extra keys. "
                "summary: facts only, at most 2 short sentences, no market judgment words. "
                "impact_inference: what the market may trade over the next 1-7 trading days and what confirmation is still missing. "
                "reasoning: explain in plain trader language using three concrete layers when relevant: message, price structure, and funds/volume. "
                "Avoid abstract terms like 风险偏好, 安全边际, 动能确认, 共振 unless you immediately explain them in plain words. "
                "uncertainty: one short sentence naming the main failure mode. "
                "priority_adjustment: number between -8 and 8.\n"
                f"Symbol: {card.symbol}\n"
                f"Event type: {insight.event_type}\n"
                f"Headline: {insight.headline_summary}\n"
                f"Bull case: {insight.bull_case[:160]}\n"
                f"Bear case: {insight.bear_case[:160]}\n"
                f"Action: {card.action_label}\n"
                f"Trend state: {card.trend_state}\n"
                f"Theme: {theme_text}\n"
                f"Chain summary: {chain_summary}\n"
                f"Market regime: {market_regime}\n"
                f"Rate risk: {rate_risk}\n"
                f"Geopolitical risk: {geopolitical_risk}\n"
                f"Event score: {card.event_score:.2f}\n"
                f"Market score: {card.market_score:.2f}\n"
                f"Final score: {card.final_score:.2f}\n"
                f"RSI: {card.rsi_14}\n"
                f"Relative volume: {card.relative_volume}\n"
            )
        prompt_tokens = _estimate_tokens(prompt)
        try:
            payload, call_metadata = self._call_api_with_metadata(
                prompt,
                max_tokens=180 if lite else 300,
            )
        except Exception as exc:
            return NarrativeOutput(), {
                "used_llm": True,
                "success": False,
                "reason": f"api_error:{exc.__class__.__name__}",
                "model": self.model,
                "prompt_tokens_estimate": prompt_tokens,
                "completion_tokens_estimate": 0,
                "latency_ms": 0,
            }
        output = NarrativeOutput(
            summary=_normalize_narrative_text(str(payload.get("summary", "")), max_chars=80),
            impact_inference=_normalize_narrative_text(
                _soften_trader_jargon(str(payload.get("impact_inference", ""))),
                max_chars=80,
            ),
            reasoning=_normalize_narrative_text(
                _soften_trader_jargon(str(payload.get("reasoning", ""))),
                max_chars=96,
            ),
            uncertainty=_normalize_narrative_text(
                _soften_trader_jargon(str(payload.get("uncertainty", ""))),
                max_chars=48,
            ),
            priority_adjustment=_bounded_float(payload.get("priority_adjustment"), -8.0, 8.0),
        )
        completion_tokens = int(call_metadata.get("completion_tokens") or 0)
        if completion_tokens <= 0:
            completion_tokens = _estimate_tokens(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return output, {
            "used_llm": True,
            "success": any(
                [
                    output.summary,
                    output.impact_inference,
                    output.reasoning,
                    output.uncertainty,
                ]
            ),
            "reason": "ok",
            "model": self.model,
            "prompt_tokens_estimate": int(call_metadata.get("prompt_tokens") or prompt_tokens),
            "completion_tokens_estimate": completion_tokens,
            "latency_ms": int(call_metadata.get("latency_ms") or 0),
        }

    def _call_api(self, prompt: str) -> Dict[str, Any]:
        payload, _ = self._call_api_with_metadata(prompt)
        return payload

    def _call_api_with_metadata(self, prompt: str, *, max_tokens: int = 300) -> tuple[Dict[str, Any], Dict[str, int]]:
        body = _build_openai_chat_body(
            model=self.model,
            prompt=prompt,
            temperature=0.2,
            max_tokens=max_tokens,
        )
        req = request.Request(
            self.base_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        started = perf_counter()
        with request.urlopen(req, timeout=OPENAI_API_TIMEOUT_SECONDS) as response:
            raw = json.loads(response.read().decode("utf-8"))
        latency_ms = int((perf_counter() - started) * 1000)
        content = _extract_message_content(raw)
        match = JSON_BLOCK_RE.search(content)
        if not match:
            raise ValueError("LLM response did not contain JSON")
        return json.loads(match.group(0)), {
            **_extract_usage_metadata(raw),
            "latency_ms": latency_ms,
        }


def _bounded_float(value: Any, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(minimum, min(maximum, parsed))
