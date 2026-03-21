from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.models import OpportunityCard, PriceRange, utcnow
from satellite_agent.notifier import FeishuTransport, Notifier
from satellite_agent.store import Store


class DummyTransport:
    def __init__(self) -> None:
        self.calls = []

    def send(self, title: str, body: str) -> None:
        self.calls.append((title, body))


def make_card(final_score: float) -> OpportunityCard:
    now = utcnow()
    return OpportunityCard(
        card_id=f"card-{final_score}",
        event_id="evt-1",
        symbol="NVDA",
        horizon="swing",
        event_type="earnings",
        headline_summary="Nvidia demand commentary improves.",
        bull_case="Demand remains strong.",
        bear_case="Move may be priced in.",
        event_score=80.0,
        market_score=78.0,
        final_score=final_score,
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
    )


class NotifierTests(unittest.TestCase):
    def test_deduplicates_within_four_hours_unless_score_improves(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            transport = DummyTransport()
            notifier = Notifier(store=store, transport=transport, dry_run=False)
            first = notifier.send(make_card(76.0))
            second = notifier.send(make_card(80.0))
            third = notifier.send(make_card(91.5))
            self.assertTrue(first.sent)
            self.assertFalse(second.sent)
            self.assertTrue(third.sent)
            self.assertEqual(len(transport.calls), 2)

    def test_feishu_transport_builds_interactive_card_payload(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        payload = transport._build_interactive_payload(make_card(91.5))
        self.assertEqual(payload["msg_type"], "interactive")
        self.assertEqual(payload["card"]["header"]["template"], "red")
        self.assertIn("NVDA", payload["card"]["header"]["title"]["content"])
        self.assertIn("确认做多", payload["card"]["header"]["title"]["content"])
        ratio_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "预期盈亏比" in field["text"]["content"]
        )
        self.assertIn("偏弱", ratio_field)
        self.assertIn("1.00", ratio_field)
        self.assertEqual(payload["card"]["elements"][-1]["tag"], "action")
        self.assertEqual(payload["card"]["elements"][-1]["actions"][0]["url"], "https://example.com")

    def test_feishu_transport_builds_event_only_payload_without_price_plan(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        card = make_card(84.0)
        card.market_data_complete = False
        card.market_data_note = "行情快照暂不可用，仅基于事件强度提醒。"
        payload = transport._build_interactive_payload(card)
        self.assertIn("事件强提醒", payload["card"]["header"]["title"]["content"])
        content_text = payload["card"]["elements"][0]["text"]["content"]
        self.assertIn("行情状态", content_text)
        price_section = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "价格计划" in field["text"]["content"]
        )
        self.assertIn("未自动生成入场/止盈/失效价", price_section)

    def test_no_transport_uses_explicit_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=False)
            decision = notifier.send(make_card(76.0))
            self.assertTrue(decision.sent)
            self.assertEqual(decision.reason, "no_transport_configured")

    def test_plain_text_body_includes_risk_reward_ratio(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            body = notifier._body(make_card(76.0))
            self.assertIn("预期盈亏比：偏弱（1.00）", body)

    def test_skip_records_explicit_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=False)
            decision = notifier.skip(make_card(76.0), reason="quality_cutoff")
            self.assertFalse(decision.sent)
            self.assertEqual(decision.reason, "quality_cutoff")


if __name__ == "__main__":
    unittest.main()
