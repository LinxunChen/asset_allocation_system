from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.llm import (
    OpenAINarrator,
    _build_openai_chat_body,
    _extract_message_content,
    _normalize_narrative_text,
    _soften_trader_jargon,
)
from satellite_agent.models import EventInsight, OpportunityCard, PriceRange, utcnow


class _FakeNarrator(OpenAINarrator):
    def __init__(self) -> None:
        super().__init__(api_key="test-key", model="gpt-test", base_url="https://example.com")

    def _call_api_with_metadata(self, prompt: str, *, max_tokens: int = 300):  # type: ignore[override]
        return (
            {
                "summary": "  这是一个很长很长的摘要，里面故意放很多空格。   "
                "它应该在本地被压缩成更短、更紧凑的卡片文案，而且不要无限变长。  ",
                "impact_inference": "  市场接下来几天更可能交易资本开支继续抬升这条线索，"
                "如果量能继续配合，强势方向仍可能延续。  ",
                "reasoning": "  事件催化和量价确认目前是同向的，所以这张卡更像顺着主线做跟随，"
                "而不是去赌一次纯情绪反抽；如果后续量能掉下来，就不要硬追。  ",
                "uncertainty": "  最大风险是开盘后量能衰减，强势叙事无法继续扩散。  ",
                "priority_adjustment": 2.5,
            },
            {
                "prompt_tokens": 123,
                "completion_tokens": 45,
                "latency_ms": 678,
            },
        )


def _make_card() -> OpportunityCard:
    now = utcnow()
    return OpportunityCard(
        card_id="card-1",
        event_id="evt-1",
        symbol="NVDA",
        horizon="swing",
        event_type="earnings",
        headline_summary="NVIDIA demand commentary improves.",
        bull_case="Demand remains strong.",
        bear_case="Move may be priced in.",
        event_score=80.0,
        market_score=78.0,
        final_score=84.0,
        entry_range=PriceRange(100.0, 101.0),
        take_profit_range=PriceRange(104.0, 108.0),
        invalidation_level=98.0,
        invalidation_reason="Breakdown through support.",
        risk_notes=["Volatility can expand quickly."],
        source_refs=["https://example.com"],
        created_at=now,
        ttl=now,
        priority="high",
        dedup_key="NVDA:evt-1:earnings:swing",
        bias="long",
        display_name="NVIDIA",
        action_label="确认做多",
        confidence_label="高",
        trend_state="bullish",
        rsi_14=63.2,
        relative_volume=1.85,
    )


def _make_insight() -> EventInsight:
    return EventInsight(
        event_id="evt-1",
        symbol="NVDA",
        event_type="earnings",
        headline_summary="财报和需求信号继续偏强。",
        bull_case="需求与指引继续强化。",
        bear_case="估值和预期已经不低。",
        importance=82.0,
        source_credibility=88.0,
        novelty=70.0,
        sentiment=0.7,
        theme_relevance=85.0,
        llm_confidence=78.0,
        risk_notes=["量能衰减会影响延续性。"],
        source_refs=["https://example.com"],
        raw_payload={"mode": "test"},
        created_at=utcnow(),
    )


class LLMTests(unittest.TestCase):
    def test_normalize_narrative_text_compacts_and_truncates(self) -> None:
        text = "  这是   一个   很长的   文本  " * 20
        normalized = _normalize_narrative_text(text, max_chars=24)
        self.assertLessEqual(len(normalized), 24)
        self.assertNotIn("  ", normalized)
        self.assertTrue(normalized.endswith("…"))

    def test_soften_trader_jargon_rewrites_abstract_phrases(self) -> None:
        text = "事件驱动叠加趋势向上，风险偏好环境提供安全边际，放量确认多头动能。"
        softened = _soften_trader_jargon(text)
        self.assertIn("当前市场环境不拖后腿", softened)
        self.assertIn("成交量放大，说明有资金在跟", softened)
        self.assertNotIn("安全边际", softened)

    def test_openai_narrator_trims_outputs_for_card_use(self) -> None:
        narrator = _FakeNarrator()
        output = narrator.narrate(
            insight=_make_insight(),
            card=_make_card(),
            market_regime="risk_off",
            rate_risk="high",
            geopolitical_risk="medium",
            theme_text="AI芯片与半导体设备",
            chain_summary="首次出现",
        )
        self.assertLessEqual(len(output.summary), 80)
        self.assertLessEqual(len(output.impact_inference), 80)
        self.assertLessEqual(len(output.reasoning), 96)
        self.assertLessEqual(len(output.uncertainty), 48)
        self.assertNotIn("  ", output.summary)
        self.assertEqual(output.priority_adjustment, 2.5)

    def test_openai_narrator_returns_metadata_for_usage_reporting(self) -> None:
        narrator = _FakeNarrator()
        output, metadata = narrator.narrate_with_metadata(
            insight=_make_insight(),
            card=_make_card(),
            market_regime="risk_off",
            rate_risk="high",
            geopolitical_risk="medium",
            theme_text="AI芯片与半导体设备",
            chain_summary="首次出现",
        )
        self.assertTrue(output.summary)
        self.assertTrue(metadata["used_llm"])
        self.assertTrue(metadata["success"])
        self.assertEqual(metadata["model"], "gpt-test")
        self.assertEqual(metadata["prompt_tokens_estimate"], 123)
        self.assertEqual(metadata["completion_tokens_estimate"], 45)
        self.assertEqual(metadata["latency_ms"], 678)

    def test_openai_body_disables_thinking_and_requests_json(self) -> None:
        body = _build_openai_chat_body(
            model="Qwen/Qwen3.5-35B-A3B",
            prompt="test",
            temperature=0.2,
            max_tokens=123,
        )
        payload = __import__("json").loads(body.decode("utf-8"))
        self.assertEqual(payload["model"], "Qwen/Qwen3.5-35B-A3B")
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertFalse(payload["enable_thinking"])
        self.assertEqual(payload["max_tokens"], 123)

    def test_extract_message_content_falls_back_to_reasoning_content(self) -> None:
        raw = {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "reasoning_content": "{\"summary\":\"一句话\"}",
                    }
                }
            ]
        }
        self.assertEqual(_extract_message_content(raw), "{\"summary\":\"一句话\"}")


if __name__ == "__main__":
    unittest.main()
