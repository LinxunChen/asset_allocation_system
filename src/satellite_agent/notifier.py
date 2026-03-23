from __future__ import annotations

import json
from datetime import timedelta, timezone
from typing import Optional, Protocol
from urllib import request
from urllib.parse import urlparse

from .decision_engines.mappers import build_delivery_view_from_card
from .models import AlertDecision, OpportunityCard, utcnow
from .store import Store

CN_TZ = timezone(timedelta(hours=8))


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线（1-7个交易日）", "position": "波段（2-6周）"}.get(horizon, horizon)


def _display_trade_cycle(horizon: str) -> str:
    return f"交易周期：{_display_horizon(horizon)}"


def _display_priority(priority: str) -> str:
    return {"high": "高优先级", "normal": "普通", "suppressed": "压制"}.get(priority, priority)


def _display_event_type(event_type: str) -> str:
    return {
        "earnings": "财报事件",
        "guidance": "指引事件",
        "sec": "公告事件",
        "research": "研报事件",
        "m&a": "并购事件",
        "strategic": "战略合作",
        "product": "产品催化",
        "news": "新闻事件",
    }.get(event_type, event_type)


def _display_trend_state(value: str) -> str:
    return {
        "bullish": "多头（结构向上）",
        "bearish": "空头（结构走弱）",
        "neutral": "震荡（等待方向）",
        "uptrend": "多头（结构向上）",
        "downtrend": "空头（结构走弱）",
    }.get(value, value or "未识别")


def _risk_level_display(value: str | None) -> str:
    return {"low": "低", "medium": "中", "high": "高"}.get(value or "", value or "未识别")


def _display_valid_until(card: OpportunityCard) -> str:
    valid_until = card.ttl.astimezone(CN_TZ)
    return valid_until.strftime("%m-%d %H:%M")


def _risk_reward_ratio(card: OpportunityCard) -> float | None:
    if not card.market_data_complete:
        return None
    if card.bias == "short":
        expected_reward = float(card.entry_range.low) - float(card.take_profit_range.high)
        expected_risk = float(card.invalidation_level) - float(card.entry_range.low)
    else:
        expected_reward = float(card.take_profit_range.low) - float(card.entry_range.high)
        expected_risk = float(card.entry_range.high) - float(card.invalidation_level)
    if expected_reward <= 0 or expected_risk <= 0:
        return None
    return round(expected_reward / expected_risk, 2)


def _display_risk_reward(card: OpportunityCard) -> str:
    ratio = _risk_reward_ratio(card)
    if ratio is None:
        return "待行情确认后再计算"
    if ratio >= 2.0:
        label = "较优"
    elif ratio >= 1.5:
        label = "可接受"
    else:
        label = "偏弱"
    return f"{label}（{ratio:.2f}）"


def _is_watch_card(card: OpportunityCard, delivery: dict | None = None) -> bool:
    action = (delivery or {}).get("action_label_effective") or card.action_label
    return action == "加入观察"


def _linked_sources(card: OpportunityCard) -> list[str]:
    links: list[str] = []
    for source in card.source_refs[:3]:
        if source.startswith(("http://", "https://")):
            links.append(f"[{_source_label(source)}]({source})")
    return links


def _source_label(source_ref: str) -> str:
    if not source_ref:
        return "未知来源"
    if not source_ref.startswith(("http://", "https://")):
        return source_ref
    host = urlparse(source_ref).netloc.lower()
    if not host:
        return source_ref
    mapping = {
        "www.sec.gov": "SEC Edgar",
        "sec.gov": "SEC Edgar",
        "news.google.com": "Google News",
        "www.gurufocus.com": "GuruFocus",
        "gurufocus.com": "GuruFocus",
        "www.reuters.com": "Reuters",
        "reuters.com": "Reuters",
        "www.benzinga.com": "Benzinga",
        "benzinga.com": "Benzinga",
        "www.fool.com": "Motley Fool",
        "www.zacks.com": "Zacks",
        "finance.yahoo.com": "Yahoo Finance",
    }
    return mapping.get(host, host.replace("www.", ""))


class NotificationTransport(Protocol):
    def send(self, title: str, body: str) -> None:
        ...

    def send_card(self, card: OpportunityCard) -> None:
        ...


class FeishuTransport:
    def __init__(self, webhook: str) -> None:
        self.webhook = webhook

    def send(self, title: str, body: str) -> None:
        payload = json.dumps(
            {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": title,
                            "content": [[{"tag": "text", "text": body}]],
                        }
                    }
                },
            }
        ).encode("utf-8")
        self._post(payload)

    def send_card(self, card: OpportunityCard) -> None:
        payload = json.dumps(self._build_interactive_payload(card)).encode("utf-8")
        self._post(payload)

    def _post(self, payload: bytes) -> None:
        req = request.Request(
            self.webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=10) as _:
            return

    def _build_interactive_payload(self, card: OpportunityCard) -> dict:
        delivery = build_delivery_view_from_card(card)
        is_watch_card = _is_watch_card(card, delivery)
        header_template = (
            "blue"
            if is_watch_card
            else {
                "high": "red",
                "normal": "orange",
                "suppressed": "grey",
            }.get(card.priority, "blue")
        )
        risk_text = "；".join(card.risk_notes[:3]) if card.risk_notes else "无"
        identity = delivery["identity"]
        action_label = delivery["action_label_effective"]
        confidence_text = delivery["confidence_label_effective"]
        priority_text = _display_priority(card.priority)
        theme_text = delivery["theme_text"]
        peer_text = "、".join(delivery["confirmed_peers"][:3]) if delivery["confirmed_peers"] else "暂无"
        event_line = delivery["event_reason_line"]
        market_line = delivery["market_reason_line"]
        theme_line = delivery["theme_reason_line"]
        valid_until_text = delivery["valid_until_text"] or _display_valid_until(card)
        risk_reward_text = _display_risk_reward(card)
        source_text = delivery["source_summary"]
        source_links = _linked_sources(card)
        source_line = " / ".join(source_links) if source_links else source_text
        market_env_line = (
            f"{delivery['market_regime_display']} | 利率压力 {_risk_level_display(card.rate_risk)}"
            + (
                f" | 地缘风险 {_risk_level_display(card.geopolitical_risk)}"
                if card.geopolitical_risk
                else ""
            )
        )
        overview_lines = [
            f"**标的**：{identity}",
            f"**卡片定位**：{'预备池观察（先看事件发酵与结构确认）' if is_watch_card else '正式操作卡（可结合价格计划执行）'}",
            f"**操作建议**：{action_label}（{priority_text} / 置信度 {confidence_text}）",
            f"**交易周期**：{delivery['horizon_display']}",
            f"**链路**：{delivery['chain_summary']}",
        ]
        event_lines = [
            f"**事件类型**：{delivery['event_type_display']}",
            f"**事件倾向**：{delivery['event_bias_display']} | {delivery['event_bias_explainer']}",
            f"**事实摘要**：{delivery['llm_summary']}",
        ]
        if delivery["llm_impact_inference"]:
            event_lines.append(f"**影响推理**：{delivery['llm_impact_inference']}")
        market_lines = [
            f"**当前环境**：{market_env_line}",
            f"**结构状态**：{delivery['trend_state_display']} | {delivery['trend_state_explainer']}",
            f"**量价状态**：{market_line}",
        ]
        if delivery["macro_overlay_note"]:
            market_lines.append(f"**宏观覆盖**：{delivery['macro_overlay_note']}")
        if not card.market_data_complete:
            market_lines.append(f"**行情状态**：{card.market_data_note or '行情确认暂缺'}")
        execution_metric_lines = [
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**交易周期**\n{delivery['horizon_display']}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**操作建议**\n{action_label}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**强度 / 置信度**\n{priority_text} / {confidence_text}",
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**题材**\n{theme_text}",
                },
            },
        ]
        signal_metric_lines = [
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**综合分**\n{card.final_score:.2f}（{delivery['final_score_label']}）\n"
                        f"{delivery['final_score_explainer']}"
                    ),
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**市场分**\n{card.market_score:.2f}（{delivery['market_score_label']}）\n"
                        f"{delivery['market_score_explainer']}"
                    ),
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**事件分**\n{card.event_score:.2f}（{delivery['event_score_label']}）\n"
                        f"{delivery['event_score_explainer']}"
                    ),
                },
            },
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**题材联动**\n{theme_line}",
                },
            },
        ]
        if card.rsi_14 is not None:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**RSI**\n{card.rsi_14:.1f}（{delivery['rsi_label']}）",
                    },
                }
            )
        if card.relative_volume is not None:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**相对量能**\n{card.relative_volume:.2f} 倍（{delivery['relative_volume_label']}）\n"
                            f"{delivery['relative_volume_explainer']}"
                        ),
                    },
                }
            )
        if card.trend_state:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**结构状态**\n{delivery['trend_state_display']}\n{delivery['trend_state_explainer']}",
                    },
                }
            )
        if card.bias:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**事件倾向**\n{delivery['event_bias_display']}\n{delivery['event_bias_explainer']}",
                    },
                }
            )
        if delivery["macro_overlay_note"]:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**宏观覆盖**\n{delivery['macro_overlay_note']}\n"
                            f"环境分压制：-{delivery['macro_penalty_applied']:.1f}"
                        ),
                    },
                }
            )
        actions = []
        for source in card.source_refs[:3]:
            actions.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": _source_label(source)},
                    "type": "default",
                    "url": source,
                }
            )
        if not actions:
            actions.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "无外部链接"},
                    "type": "default",
                    "value": {"action": "noop"},
                }
            )
        if is_watch_card:
            decision_lines = [
                f"**为什么现在先观察**：{delivery['llm_reasoning'] or event_line}",
                f"**当前处理**：{card.positioning_hint or '先放入观察名单，等待更明确的催化与结构确认。'}",
                f"**最大风险**：{delivery['llm_uncertainty'] or risk_text}",
            ]
            observation_lines = [
                f"**关注重点**：{card.reason_to_watch or delivery['llm_reasoning'] or '先观察事件发酵与结构确认。'}",
                f"**升级触发**：{delivery['llm_impact_inference'] or delivery['trend_state_explainer'] or '等待结构转强与量能进一步确认。'}",
                f"**观察周期**：{delivery['horizon_display']}",
            ]
        else:
            decision_lines = [
                f"**为什么可以动**：{delivery['llm_reasoning'] or event_line}",
                f"**当前处理**：{card.positioning_hint or card.reason_to_watch or '按价格计划跟随，不追高扩仓。'}",
                f"**最大风险**：{delivery['llm_uncertainty'] or risk_text}",
            ]
            observation_lines = [
                f"**关注重点**：{card.positioning_hint or card.reason_to_watch or delivery['llm_reasoning'] or '先观察事件发酵与结构确认。'}",
                f"**升级关注**：{delivery['llm_impact_inference'] or delivery['trend_state_explainer'] or '等待结构转强与量能进一步确认。'}",
                f"**关注周期**：{delivery['horizon_display']}",
            ]
        return {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True, "enable_forward": True},
                "header": {
                    "template": header_template,
                    "title": {
                        "tag": "plain_text",
                        "content": self._title(card),
                    },
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(overview_lines),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**事件理解**\n先看这条资讯说了什么、事件倾向是什么，再判断市场会围绕哪条线索交易。",
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(event_lines),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**市场与结构**\n再看当前环境、结构状态和量价确认，判断这条事件现在是否值得跟进。",
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(market_lines),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**信号评分**\n最后用事件分、市场分和综合分，快速判断这张卡是观察优先还是可以执行。",
                        },
                    },
                    {
                        "tag": "div",
                        "fields": signal_metric_lines,
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**决策结论**\n把事件、结构、题材和风险收束成一句可执行的人话结论。",
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "\n".join(decision_lines),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**{'观察计划' if is_watch_card else '执行计划'}**\n{'当前更适合先观察后续确认。' if is_watch_card else '当前更适合按价格计划执行。'}",
                        },
                    },
                    {"tag": "hr"},
                    *(
                        [
                            {
                                "tag": "div",
                                "fields": [
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": (
                                                f"**入场区间**\n{card.entry_range.low:.2f} - {card.entry_range.high:.2f}"
                                            ),
                                        },
                                    },
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": (
                                                f"**止盈区间**\n{card.take_profit_range.low:.2f} - {card.take_profit_range.high:.2f}"
                                            ),
                                        },
                                    },
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": (
                                                f"**失效价**\n{card.invalidation_level:.2f}"
                                            ),
                                        },
                                    },
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": (
                                                f"**预期盈亏比**\n{risk_reward_text}"
                                            ),
                                        },
                                    },
                                ],
                            }
                        ]
                        if card.market_data_complete and not is_watch_card
                        else [
                            {
                                "tag": "div",
                                "text": {
                                    "tag": "lark_md",
                                    "content": "\n".join(observation_lines),
                                },
                            }
                        ]
                    ),
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": (
                                    f"来源：{source_text} | {_display_trade_cycle(card.horizon)} | 失效条件：{card.invalidation_reason} | 风险提示：{risk_text}"
                                    + (
                                        f" | 行情说明：{card.market_data_note}"
                                        if card.market_data_note
                                        else ""
                                    )
                                ),
                            }
                        ],
                    },
                    {"tag": "action", "actions": actions},
                ],
            },
        }

    def _title(self, card: OpportunityCard) -> str:
        delivery = build_delivery_view_from_card(card)
        return (
            f"{delivery['identity']} | {delivery['action_label_effective']} | "
            f"{delivery['event_type_display']}"
        )


class Notifier:
    def __init__(self, store: Store, transport: Optional[NotificationTransport] = None, dry_run: bool = False) -> None:
        self.store = store
        self.transport = transport
        self.dry_run = dry_run

    def send(self, card: OpportunityCard, run_id: str = "") -> AlertDecision:
        if card.priority == "suppressed":
            return self.skip(card, reason="threshold_not_met", run_id=run_id)
        previous = self.store.get_last_alert(card.dedup_key)
        if previous is not None and self._is_duplicate(card, previous):
            return self.skip(card, reason="deduplicated", run_id=run_id)
        if self.transport and not self.dry_run:
            try:
                if hasattr(self.transport, "send_card"):
                    self.transport.send_card(card)
                else:
                    self.transport.send(self._title(card), self._body(card))
            except Exception as exc:
                return self.skip(
                    card,
                    reason=f"transport_error:{exc.__class__.__name__}",
                    run_id=run_id,
                )
        if self.transport and not self.dry_run:
            success_reason = "sent"
        elif self.dry_run:
            success_reason = "dry_run"
        else:
            success_reason = "no_transport_configured"
        decision = AlertDecision(
            sent=True,
            priority=card.priority,
            reason=success_reason,
            dedup_key=card.dedup_key,
        )
        self.store.record_alert(card, decision, run_id=run_id)
        return decision

    def skip(self, card: OpportunityCard, reason: str, run_id: str = "") -> AlertDecision:
        decision = AlertDecision(
            sent=False,
            priority=card.priority,
            reason=reason,
            dedup_key=card.dedup_key,
        )
        self.store.record_alert(card, decision, run_id=run_id)
        return decision

    def _is_duplicate(self, card: OpportunityCard, previous: object) -> bool:
        last_time = utcnow()
        if hasattr(previous, "__getitem__"):
            last_time = previous["notified_at"]
            if isinstance(last_time, str):
                from datetime import datetime

                last_time = datetime.fromisoformat(last_time)
            score_delta = card.final_score - float(previous["final_score"])
            same_invalidation = abs(card.invalidation_level - float(previous["invalidation_level"])) < 0.01
            within_window = utcnow() - last_time <= timedelta(hours=4)
            return within_window and score_delta <= 10.0 and same_invalidation
        return False

    def _title(self, card: OpportunityCard) -> str:
        delivery = build_delivery_view_from_card(card)
        return (
            f"{delivery['identity']} | {delivery['action_label_effective']} | "
            f"{delivery['event_type_display']}"
        )

    def _body(self, card: OpportunityCard) -> str:
        delivery = build_delivery_view_from_card(card)
        event_line = delivery["event_reason_line"]
        market_line = delivery["market_reason_line"]
        theme_line = delivery["theme_reason_line"]
        confidence_text = delivery["confidence_label_effective"]
        risk_reward_text = _display_risk_reward(card)
        source_line = " / ".join(_linked_sources(card)) or delivery["source_summary"]
        market_env_line = (
            f"{delivery['market_regime_display']} | 利率压力 {_risk_level_display(card.rate_risk)}"
            + (
                f" | 地缘风险 {_risk_level_display(card.geopolitical_risk)}"
                if card.geopolitical_risk
                else ""
            )
        )
        price_plan_block = ""
        if card.market_data_complete and not _is_watch_card(card, delivery):
            price_plan_block = (
                f"入场区间：{card.entry_range.low:.2f}-{card.entry_range.high:.2f}\n"
                f"止盈区间：{card.take_profit_range.low:.2f}-{card.take_profit_range.high:.2f}\n"
                f"失效价：{card.invalidation_level:.2f}（{card.invalidation_reason}）\n"
                f"预期盈亏比：{risk_reward_text}\n"
            )
        overview_lines = [
            f"标的：{delivery['identity']}",
            f"卡片定位：{'预备池观察（先看事件发酵与结构确认）' if _is_watch_card(card, delivery) else '正式操作卡（可结合价格计划执行）'}",
            _display_trade_cycle(card.horizon),
            f"操作建议：{delivery['action_label_effective']}（{_display_priority(card.priority)} / 置信度 {confidence_text}）",
            f"链路：{delivery['chain_summary']}",
        ]
        event_section = [
            "事件理解：",
            f"- 事件类型：{delivery['event_type_display']}",
            f"- 事件倾向：{delivery['event_bias_display']}（{delivery['event_bias_explainer']}）",
            f"- 事实摘要：{delivery['llm_summary']}",
        ]
        if delivery["llm_impact_inference"]:
            event_section.append(f"- 影响推理：{delivery['llm_impact_inference']}")
        market_section = [
            "市场与结构：",
            f"- 当前环境：{market_env_line}",
            f"- 结构状态：{delivery['trend_state_display']}（{delivery['trend_state_explainer']}）",
            f"- 量价状态：{market_line}",
        ]
        if delivery["macro_overlay_note"]:
            market_section.append(f"- 宏观覆盖：{delivery['macro_overlay_note']}")
        if card.rsi_14 is not None:
            market_section.append(f"- RSI：{card.rsi_14:.1f}（{delivery['rsi_label']}）")
        if card.relative_volume is not None:
            market_section.append(
                f"- 相对量能：{card.relative_volume:.2f} 倍（{delivery['relative_volume_label']}，{delivery['relative_volume_explainer']}）"
            )
        score_section = [
            "信号评分：",
            f"- 事件分：{card.event_score:.2f}（{delivery['event_score_label']}，{delivery['event_score_explainer']}）",
            f"- 市场分：{card.market_score:.2f}（{delivery['market_score_label']}，{delivery['market_score_explainer']}）",
            f"- 综合分：{card.final_score:.2f}（{delivery['final_score_label']}，{delivery['final_score_explainer']}）",
            f"- 题材联动：{theme_line}",
        ]
        if _is_watch_card(card, delivery):
            decision_section = [
                "决策结论：",
                f"- 为什么现在先观察：{delivery['llm_reasoning'] or event_line}",
                f"- 当前处理：{card.positioning_hint or '先放入观察名单，等待更明确的催化与结构确认。'}",
                f"- 最大风险：{delivery['llm_uncertainty'] or '需继续核对原文与价格结构'}",
            ]
        else:
            decision_section = [
                "决策结论：",
                f"- 为什么可以动：{delivery['llm_reasoning'] or event_line}",
                f"- 当前处理：{card.positioning_hint or card.reason_to_watch}",
                f"- 最大风险：{delivery['llm_uncertainty'] or '需继续核对原文与价格结构'}",
            ]
        body_lines = [
            *overview_lines,
            "",
            *event_section,
        ]
        body_lines.extend(["", *market_section, "", *score_section, "", *decision_section])
        if price_plan_block:
            body_lines.extend(["", "执行计划：", price_plan_block.rstrip()])
        elif _is_watch_card(card, delivery):
            body_lines.extend(
                [
                    "",
                    "观察计划：",
                    f"- 关注重点：{card.reason_to_watch or delivery['llm_reasoning']}",
                    f"- 升级触发：{delivery['llm_impact_inference'] or delivery['trend_state_explainer']}",
                ]
            )
        body_lines.extend(
            [
                "",
                f"原文与来源：{source_line}",
                f"原文链接：{' | '.join(card.source_refs)}",
                "",
                f"风险提示：{' | '.join(card.risk_notes)}",
            ]
        )
        return "\n".join(body_lines)
