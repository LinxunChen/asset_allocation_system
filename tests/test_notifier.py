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
from satellite_agent.notifier import FeishuTransport, Notifier, build_render_view
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
    card.holding_management_subreason = "macro_protection"
    card.chain_summary = "3天前确认做多 -> 今日进入兑现池"
    card.holding_management_source_decision_id = "decision-confirm-1"
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
        self.assertNotIn("链路", first_block)
        self.assertIn("正式卡", first_block)
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
            if element.get("tag") == "div" and "结构状态" in element.get("text", {}).get("content", "")
        )
        self.assertIn("相对量能", market_block)
        self.assertIn("执行判断", market_block)
        theme_heading = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and "**题材联动**" in element.get("text", {}).get("content", "")
        )
        self.assertIn("**题材联动**", theme_heading)
        macro_block = next(
            element["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div" and "市场环境" in element.get("text", {}).get("content", "")
        )
        self.assertIn("宏观覆盖", macro_block)
        ratio_field = next(
            field["text"]["content"]
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
            for field in element.get("fields", [])
            if "预期盈亏比" in field["text"]["content"]
        )
        self.assertIn("可接受", ratio_field)
        self.assertIn("1.67", ratio_field)
        self.assertNotIn("信号评分", "\n".join(
            element.get("text", {}).get("content", "")
            for element in payload["card"]["elements"]
            if element.get("tag") == "div"
        ))
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
            if element.get("tag") == "div" and "结构状态" in element.get("text", {}).get("content", "")
        )
        self.assertIn("行情状态", market_text)
        plan_heading = payload["card"]["elements"][2]["text"]["content"]
        self.assertIn("观察计划", plan_heading)
        plan_text = payload["card"]["elements"][3]["text"]["content"]
        self.assertIn("关注重点", plan_text)
        self.assertIn("升级触发", plan_text)
        self.assertIn("量能继续配合", plan_text)

    def test_formal_card_action_detail_uses_execution_copy_instead_of_watch_label(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        card = make_formal_card(63.4)
        card.action_label = "试探建仓"
        payload = transport._build_interactive_payload(card)
        overview_text = payload["card"]["elements"][0]["text"]["content"]

        self.assertIn("试探建仓（先小仓验证）", overview_text)
        self.assertNotIn("试探建仓（观察优先）", overview_text)

    def test_feishu_transport_builds_exit_pool_payload_with_guardrail(self) -> None:
        transport = FeishuTransport("https://example.com/webhook")
        payload = transport._build_interactive_payload(make_exit_card())
        overview_text = payload["card"]["elements"][0]["text"]["content"]
        self.assertIn("退出卡", overview_text)
        self.assertIn("优先保护利润", overview_text)
        decision_text = payload["card"]["elements"][1]["text"]["content"]
        self.assertIn("为什么现在退出", decision_text)
        self.assertIn("宏观保护", decision_text)
        self.assertNotIn("来源链路", decision_text)
        self.assertIn("只面向已有仓位的持仓管理", decision_text)
        plan_text = payload["card"]["elements"][3]["text"]["content"]
        self.assertIn("原目标区", plan_text)
        self.assertNotIn("来源链路", plan_text)
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
            self.assertNotIn("链路：", body)
            self.assertIn("执行判断：事件催化和量价配合共振，当前更像是顺趋势的二次确认。", body)
            self.assertIn("结构状态：多头（结构向上）（代表价格结构仍偏强", body)
            self.assertIn("宏观覆盖：宏观风险覆盖已生效：综合分下调 12.0 分", body)
            self.assertIn("题材联动：", body)

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
            self.assertIn("事件摘要：", body)
            self.assertIn("宏观环境：", body)
            self.assertNotIn("动作由「确认做多」降为「试探建仓」", body)

    def test_formal_card_core_summary_rewrites_observation_like_reasoning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            card.llm_reasoning = "消息面虽有催化但市场反应平淡，未形成明确突破，仍需等待确认。"
            card.relative_volume = 0.29
            card.trend_state = "neutral"
            body = notifier._body(card)
            self.assertIn("卡片定位：正式卡", body)
            self.assertIn("执行判断：事件有催化，但当前更适合按计划小步执行，不宜追高。", body)
            self.assertIn("操作建议：确认做多（按计划执行）", body)

    def test_formal_card_event_and_market_sections_do_not_repeat_observation_language(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            card.symbol = "TSM"
            card.display_name = "Taiwan Semiconductor"
            card.llm_impact_inference = "未来 1-7 个交易日可能维持震荡，仍需等待确认方向。"
            card.llm_summary = "TSM 触发新闻事件"
            card.headline_summary = "TSM 触发新闻事件"
            body = notifier._body(card)
            self.assertIn("事实摘要：TSM 当前更适合把新闻当作背景催化，执行上仍以结构和量价确认为主。", body)
            self.assertIn("影响推理：当前更适合围绕既定入场区和失效价执行，不宜脱离计划追价。", body)
            self.assertIn("结构状态：多头（结构向上）", body)
            self.assertIn("相对量能：1.85 倍", body)
            self.assertIn("执行判断：事件催化和量价配合共振，当前更像是顺趋势的二次确认。", body)
            self.assertNotIn("仍需等待确认", body)

    def test_core_summary_is_not_hard_truncated_to_ellipsis(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            card.llm_reasoning = "合作规模与兑现路径已经基本过线，当前更像可按计划执行的机会，而不是继续停留在观察层。"
            body = notifier._body(card)
            self.assertIn("执行判断：合作规模与兑现路径已经基本过线，当前更像可按计划执行的机会，而不是继续停留在观察层。", body)
            self.assertNotIn("执行判断：合作规模与兑现路径已经基本过线，当前更像可按计划...", body)

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
            self.assertIn("观察计划：", body)
            self.assertIn("当前处理：当前先放入观察名单，不追价。", body)
            self.assertIn("关注重点：先盯合作细节和订单金额是否继续落地。", body)
            self.assertIn("升级触发：若量能继续配合", body)
            self.assertIn("题材联动：", body)
            self.assertIn("宏观环境：", body)

    def test_watch_card_polishes_observation_copy_and_hides_zero_atr_wording(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_card(76.0)
            card.market_data_complete = False
            card.action_label = "加入观察"
            card.reason_to_watch = "量能略有抬升至 1.00 倍；波动率可控，ATR 占比 0.0%。"
            card.llm_impact_inference = ""
            body = notifier._body(card)
            self.assertIn("量能回到常态附近（1.00 倍），但尚未形成放量确认", body)
            self.assertIn("波动率极低", body)
            self.assertNotIn("ATR 占比 0.0%", body)

    def test_repeated_watch_chain_is_compressed_to_single_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_card(84.0)
            card.market_data_complete = False
            card.action_label = "加入观察"
            card.chain_summary = "今日加入观察 -> 今日加入观察"
            body = notifier._body(card)
            self.assertNotIn("链路：", body)
            self.assertNotIn("今日加入观察 -> 今日加入观察", body)

    def test_downgraded_watch_card_does_not_reuse_formal_execution_hint(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            card.priority = "suppressed"
            card.execution_eligible = False
            card.action_label = "确认做多"
            card.positioning_hint = "当前优先按价格计划执行。"
            card.reason_to_watch = "如果当前文案读起来足够清楚，说明链路已经适合上线。"
            card.llm_impact_inference = ""
            card.relative_volume = 0.29
            card.trend_state = "neutral"
            body = notifier._body(card)
            self.assertIn("卡片定位：正式卡", body)
            self.assertIn("操作建议：确认做多（按计划执行）", body)
            self.assertIn("执行计划：", body)

    def test_formal_card_with_neutral_structure_and_weak_volume_auto_downgrades(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_formal_card()
            card.trend_state = "neutral"
            card.relative_volume = 0.29
            body = notifier._body(card)
            self.assertIn("卡片定位：正式卡", body)
            self.assertIn("操作建议：确认做多（按计划执行）", body)
            self.assertNotIn("降级原因：", body)

    def test_render_view_keeps_formal_card_formal_even_when_structure_is_weak(self) -> None:
        card = make_formal_card()
        card.trend_state = "neutral"
        card.relative_volume = 0.29

        render = build_render_view(card)

        self.assertTrue(render["is_formal_card"])
        self.assertFalse(render["is_watch_card"])
        self.assertFalse(render["downgraded_to_watch"])
        self.assertEqual(render["action_label"], "确认做多")
        self.assertEqual(render["render_warning"], "")

    def test_generic_preview_style_event_summary_falls_back_to_contextual_copy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            card = make_card(76.0)
            card.action_label = "加入观察"
            card.market_data_complete = False
            card.event_type = "news"
            card.llm_summary = "模拟候选池卡片，用于预览当前文案与展示层效果。"
            card.headline_summary = "模拟候选池卡片，用于预览当前文案与展示层效果。"
            body = notifier._body(card)
            self.assertIn("事实摘要：NVDA 当前以结构观察为主，新闻仅作背景参考。", body)
            self.assertNotIn("用于预览", body)

    def test_plain_text_exit_card_uses_exit_specific_labels_and_guardrail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "test.db")
            store.initialize()
            notifier = Notifier(store=store, transport=None, dry_run=True)
            body = notifier._body(make_exit_card())
            self.assertIn("卡片定位：退出卡", body)
            self.assertIn("执行判断：已有浮盈后遇到宏观风险抬升，当前更适合转入兑现管理。", body)
            self.assertIn("当前状态：宏观保护", body)
            self.assertNotIn("来源链路：", body)
            self.assertIn("使用边界：只面向已有仓位的持仓管理，不代表新的开仓信号。", body)
            self.assertIn("持仓管理计划：", body)
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
