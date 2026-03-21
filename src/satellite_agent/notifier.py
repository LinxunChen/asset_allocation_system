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
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


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
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "震荡",
        "uptrend": "多头",
        "downtrend": "空头",
    }.get(value, value or "未识别")


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
        header_template = {
            "high": "red",
            "normal": "orange",
            "suppressed": "grey",
        }.get(card.priority, "blue")
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
        signal_metric_lines = [
            {
                "is_short": True,
                "text": {
                    "tag": "lark_md",
                    "content": f"**交易周期**\n{_display_horizon(card.horizon)}",
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
        if card.rsi_14 is not None:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**RSI**\n{card.rsi_14:.1f}",
                    },
                }
            )
        if card.relative_volume is not None:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**相对量能**\n{card.relative_volume:.2f} 倍",
                    },
                }
            )
        if card.trend_state:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**趋势状态**\n{delivery['trend_state_display']}",
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
                            "content": (
                                f"**标的**：{identity}\n"
                                f"**事件类型**：{delivery['event_type_display']}\n"
                                f"**摘要**：{card.headline_summary}\n"
                                f"**操作建议**：{action_label}（{priority_text} / 置信度 {confidence_text}）\n"
                                f"**交易周期**：{delivery['horizon_display']}\n"
                                f"**信息来源**：{source_text}"
                                + (
                                    f"\n**行情状态**：{card.market_data_note or '行情确认暂缺'}"
                                    if not card.market_data_complete
                                    else ""
                                )
                            ),
                        },
                    },
                    {
                        "tag": "div",
                        "fields": signal_metric_lines,
                    },
                    {
                        "tag": "div",
                        "fields": [
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**事件分**\n{card.event_score:.2f}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**市场分**\n{card.market_score:.2f}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**综合分**\n{card.final_score:.2f}",
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": f"**同题材确认**\n{peer_text}",
                                },
                            },
                        ],
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
                        if card.market_data_complete
                        else [
                            {
                                "tag": "div",
                                "fields": [
                                    {
                                        "is_short": False,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": "**价格计划**\n行情快照暂不可用，未自动生成入场/止盈/失效价。",
                                        },
                                    },
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": f"**有效至**\n{valid_until_text}",
                                        },
                                    },
                                ],
                            }
                        ]
                    ),
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": (
                                "**决策理由**\n"
                                f"- 事件判断：{event_line}\n"
                                f"- 市场状态：{market_line}\n"
                                f"- 题材联动：{theme_line}\n"
                                f"- 仓位提示：{card.positioning_hint or card.reason_to_watch or card.headline_summary}"
                            ),
                        },
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": (
                                    f"{_display_trade_cycle(card.horizon)} | 失效条件：{card.invalidation_reason} | 风险提示：{risk_text}"
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
        if not card.market_data_complete:
            return (
                f"{delivery['identity']} | {delivery['action_label_effective']} | "
                f"{delivery['event_type_display']} | 事件强提醒"
            )
        return (
            f"{delivery['identity']} | {delivery['action_label_effective']} | "
            f"{delivery['event_type_display']} | {_display_priority(card.priority)}"
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
            f"{delivery['event_type_display']} | {_display_priority(card.priority)}"
        )

    def _body(self, card: OpportunityCard) -> str:
        delivery = build_delivery_view_from_card(card)
        event_line = delivery["event_reason_line"]
        market_line = delivery["market_reason_line"]
        theme_line = delivery["theme_reason_line"]
        confidence_text = delivery["confidence_label_effective"]
        risk_reward_text = _display_risk_reward(card)
        return (
            f"标的：{delivery['identity']}\n"
            f"{_display_trade_cycle(card.horizon)}\n"
            f"操作建议：{delivery['action_label_effective']}（{_display_priority(card.priority)} / 置信度 {confidence_text}）\n"
            f"事件类型：{delivery['event_type_display']}\n"
            f"摘要：{card.headline_summary}\n"
            f"决策理由：\n"
            f"- 事件判断：{event_line}\n"
            f"- 市场状态：{market_line}\n"
            f"- 题材联动：{theme_line}\n"
            f"- 仓位提示：{card.positioning_hint or card.reason_to_watch}\n"
            f"入场区间：{card.entry_range.low:.2f}-{card.entry_range.high:.2f}\n"
            f"止盈区间：{card.take_profit_range.low:.2f}-{card.take_profit_range.high:.2f}\n"
            f"失效价：{card.invalidation_level:.2f}（{card.invalidation_reason}）\n"
            f"预期盈亏比：{risk_reward_text}\n"
            f"风险提示：{' | '.join(card.risk_notes)}\n"
            f"信息来源：{delivery['source_summary']}\n"
            f"原文链接：{' | '.join(card.source_refs)}"
        )
