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
        display_name="NVIDIA",
        action_label="确认做多",
        confidence_label="高",
        chain_summary="2天前加入观察 -> 今日确认做多",
        llm_summary="NVIDIA 相关财报与需求信息继续强化，短线情绪仍偏正面。",
        llm_impact_inference="若量能继续配合，市场可能继续交易 AI 服务器需求强化。",
        llm_reasoning="事件催化和量价配合共振，当前更像是顺趋势的二次确认。",
        llm_uncertainty="如果开盘后量能衰减，冲高回落风险会明显抬升。",
        market_regime="risk_off",
        rate_risk="high",
        macro_penalty_applied=12.0,
        macro_action_before_overlay="确认做多",
        macro_overlay_note="宏观风险覆盖已生效：综合分下调 12.0 分，动作由「确认做多」降为「试探建仓」",
        relative_volume=1.85,
        trend_state="bullish",
        rsi_14=63.2,
    )


def make_formal_card(final_score: float = 91.5) -> OpportunityCard:
    card = make_card(final_score)
    card.take_profit_range = PriceRange(106.0, 110.0)
    card.priority = "high"
    card.execution_eligible = True
    return card


def make_exit_card() -> OpportunityCard:
    card = make_formal_card(82.0)
    card.action_label = "进入兑现池"
    card.exit_pool_subreason = "macro_protection"
    card.chain_summary = "3天前确认做多 -> 今日进入兑现池"
    card.exit_pool_source_decision_id = "decision-confirm-1"
    card.reason_to_watch = "宏观环境转差且已有浮盈，当前更适合先做利润保护。"
    card.positioning_hint = "外部风险抬升时，先把已有利润锁住，比继续硬扛更重要。"
    card.llm_reasoning = "已有浮盈后遇到宏观风险抬升，当前更适合转入兑现管理。"
    card.llm_uncertainty = "如果宏观压力很快消退，过早兑现可能少赚后续波段。"
    return card


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
        payload = transport._build_interactive_payload(make_formal_card())
        self.assertEqual(payload["msg_type"], "interactive")
        self.assertEqual(payload["card"]["header"]["template"], "red")
        self.assertIn("NVIDIA", payload["card"]["header"]["title"]["content"])
        self.assertIn("确认做多", payload["card"]["header"]["title"]["content"])
        self.assertNotIn("高优先级", payload["card"]["header"]["title"]["content"])
        first_block = payload["card"]["elements"][0]["text"]["content"]
        self.assertIn("链路", first_block)
        self.assertIn("正式操作卡", first_block)
        event_block = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and "事件类型" in element.get("text", {}).get("content", "")
        )
        self.assertIn("事件倾向", event_block)
        self.assertIn("事实摘要", event_block)
        market_block = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and "当前环境" in element.get("text", {}).get("content", "")
        )
        self.assertIn("宏观覆盖", market_block)
        note_text = payload["card"]["elements"][-2]["elements"][0]["content"]
        self.assertIn("来源：example.com", note_text)
        signal_heading = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and element.get("text", {}).get("content", "") == "**信号评分**"
        )
        self.assertEqual(signal_heading, "**信号评分**")
        ratio_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "预期盈亏比" in field["text"]["content"]
        )
        self.assertIn("可接受", ratio_field)
        self.assertIn("1.67", ratio_field)
        score_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "事件分" in field["text"]["content"]
        )
        self.assertIn("强催化", score_field)
        self.assertIn("消息本身强度高", score_field)
        trend_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "结构状态" in field["text"]["content"]
        )
        self.assertIn("价格结构仍偏强", trend_field)
        bias_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "事件倾向" in field["text"]["content"]
        )
        self.assertIn("偏利多", bias_field)
        self.assertIn("偏正面", bias_field)
        macro_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "宏观覆盖" in field["text"]["content"]
        )
        self.assertIn("综合分下调 12.0 分", macro_field)
        self.assertIn("环境分压制：-12.0", macro_field)
        self.assertEqual(payload["card"]["elements"][-1]["tag"], "action")
        self.assertEqual(payload["card"]["elements"][-1]["actions"][0]["url"], "https://example.com")

    def test_feishu_transport_builds_event_only_payload_without_price_plan(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        card = make_card(84.0)
        card.market_data_complete = False
        card.market_data_note = "行情快照暂不可用，仅基于事件强度提醒。"
        card.action_label = "加入观察"
        payload = transport._build_interactive_payload(card)
        self.assertIn("加入观察", payload["card"]["header"]["title"]["content"])
        overview_text = payload["card"]["elements"][0]["text"]["content"]
        self.assertIn("观察卡", overview_text)
        market_text = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and "当前环境" in element.get("text", {}).get("content", "")
        )
        self.assertIn("行情状态", market_text)
        plan_heading = payload["card"]["elements"][2]["text"]["content"]
        self.assertIn("观察计划", plan_heading)
        plan_text = payload["card"]["elements"][3]["text"]["content"]
        self.assertIn("关注重点", plan_text)
        self.assertIn("升级触发", plan_text)
        self.assertIn("量能继续配合", plan_text)

    def test_feishu_transport_builds_exit_pool_payload_with_guardrail(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        payload = transport._build_interactive_payload(make_exit_card())
        overview_text = payload["card"]["elements"][0]["text"]["content"]
        self.assertIn("兑现管理卡", overview_text)
        self.assertIn("优先保护利润", overview_text)
        decision_text = payload["card"]["elements"][1]["text"]["content"]
        self.assertIn("为什么进入兑现池", decision_text)
        self.assertIn("宏观保护", decision_text)
        self.assertIn("来源链路", decision_text)
        self.assertIn("3天前确认做多 -> 今日进入兑现池", decision_text)
        self.assertIn("只面向已有浮盈仓位", decision_text)
        plan_text = payload["card"]["elements"][3]["text"]["content"]
        self.assertIn("原目标区", plan_text)
        self.assertIn("来源链路", plan_text)
        self.assertIn("106.00 - 110.00", plan_text)

    def test_watch_card_title_does_not_repeat_observation_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_card(84.0)
            card.action_label = "加入观察"
            title = notifier._title(card)
            self.assertEqual(title, "NVIDIA（NVDA） | 加入观察 | 财报事件")

    def test_formal_card_title_does_not_append_priority_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            title = notifier._title(card)
            self.assertEqual(title, "NVIDIA（NVDA） | 确认做多 | 财报事件")

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
            self.assertIn("NVIDIA（NVDA）", body)
            self.assertIn("链路：2天前加入观察 -> 今日加入观察", body)
            self.assertIn("降级原因：降级观察：盈亏比不足", body)
            self.assertIn("为什么现在先观察", body)
            self.assertIn("结构状态：多头（结构向上）（代表价格结构仍偏强", body)
            self.assertIn("事件倾向：偏利多（代表事件内容整体偏正面", body)
            self.assertIn("宏观覆盖：宏观风险覆盖已生效：综合分下调 12.0 分", body)
            self.assertIn("事件分：80.00（强催化，代表消息本身强度高", body)

    def test_formal_card_body_prioritizes_execution_plan_and_core_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            body = notifier._body(make_formal_card())
            self.assertIn("执行计划：", body)
            self.assertIn("入场区间：100.00-101.00", body)
            self.assertIn("失效价：98.00", body)
            self.assertIn("预期盈亏比：可接受（1.67）", body)
            self.assertIn("一句话核心：", body)
            self.assertIn("最大风险：", body)
            self.assertNotIn("动作由「确认做多」降为「试探建仓」", body)

    def test_formal_card_deduplicates_and_prioritizes_sources(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        card = make_formal_card()
        card.source_refs = [
            "https://news.google.com/articles/1",
            "https://www.sec.gov/ixviewer/doc",
            "https://www.reuters.com/world/us/example",
            "https://news.google.com/articles/2",
        ]
        payload = transport._build_interactive_payload(card)
        buttons = payload["card"]["elements"][-1]["actions"]
        labels = [button["text"]["content"] for button in buttons]
        self.assertEqual(labels, ["SEC Edgar", "Reuters", "Google News"])

    def test_plain_text_watch_card_uses_observation_specific_labels(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_card(76.0)
            card.market_data_complete = False
            card.action_label = "加入观察"
            card.reason_to_watch = "先盯合作细节和订单金额是否继续落地。"
            card.positioning_hint = "当前先放入观察名单，不追价。"
            body = notifier._body(card)
            self.assertIn("为什么现在先观察", body)
            self.assertIn("当前处理：当前先放入观察名单，不追价。", body)
            self.assertIn("关注重点：先盯合作细节和订单金额是否继续落地。", body)
            self.assertIn("升级触发：若量能继续配合", body)

    def test_plain_text_exit_card_uses_exit_specific_labels_and_guardrail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            body = notifier._body(make_exit_card())
            self.assertIn("卡片定位：兑现管理卡", body)
            self.assertIn("为什么进入兑现池", body)
            self.assertIn("兑现原因：宏观保护", body)
            self.assertIn("来源链路：3天前确认做多 -> 今日进入兑现池", body)
            self.assertIn("使用边界：只面向已有浮盈仓位，不代表新的开仓信号。", body)
            self.assertIn("兑现计划：", body)
            self.assertIn("原目标区：106.00-110.00", body)

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
