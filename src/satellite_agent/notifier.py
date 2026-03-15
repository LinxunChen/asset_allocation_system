from __future__ import annotations

import json
from datetime import timedelta
from typing import Optional, Protocol
from urllib import request

from .models import AlertDecision, OpportunityCard, utcnow
from .store import Store


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


def _display_priority(priority: str) -> str:
    return {"high": "高优先级", "normal": "普通", "suppressed": "压制"}.get(priority, priority)


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
        header_template = {
            "high": "red",
            "normal": "orange",
            "suppressed": "grey",
        }.get(card.priority, "blue")
        ttl_days = max(card.ttl_delta().days, 0)
        risk_text = "；".join(card.risk_notes[:3]) if card.risk_notes else "无"
        reason_text = card.reason_to_watch or card.headline_summary
        actions = []
        for index, source in enumerate(card.source_refs[:3], start=1):
            actions.append(
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": f"查看来源{index}"},
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
                                f"**摘要**：{card.headline_summary}\n"
                                f"**关注理由**：{reason_text}\n"
                                f"**方向**：{'做多' if card.bias == 'long' else '做空'}"
                            ),
                        },
                    },
                    {
                        "tag": "div",
                        "fields": [
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": (
                                        f"**事件分**\n{card.event_score:.2f}"
                                    ),
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": (
                                        f"**市场分**\n{card.market_score:.2f}"
                                    ),
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": (
                                        f"**综合分**\n{card.final_score:.2f}"
                                    ),
                                },
                            },
                            {
                                "is_short": True,
                                "text": {
                                    "tag": "lark_md",
                                    "content": (
                                        f"**周期 / 优先级**\n{_display_horizon(card.horizon)} / {_display_priority(card.priority)}"
                                    ),
                                },
                            },
                        ],
                    },
                    {"tag": "hr"},
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
                                        f"**有效期**\n{ttl_days} 天"
                                    ),
                                },
                            },
                        ],
                    },
                    {
                        "tag": "note",
                        "elements": [
                            {
                                "tag": "plain_text",
                                "content": f"失效条件：{card.invalidation_reason} | 风险提示：{risk_text}",
                            }
                        ],
                    },
                    {"tag": "action", "actions": actions},
                ],
            },
        }

    def _title(self, card: OpportunityCard) -> str:
        return f"[{_display_priority(card.priority)}] {card.symbol} {_display_horizon(card.horizon)}机会"


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
        return f"[{_display_priority(card.priority)}] {card.symbol} {_display_horizon(card.horizon)}机会"

    def _body(self, card: OpportunityCard) -> str:
        return (
            f"{card.headline_summary}\n"
            f"方向：{'做多' if card.bias == 'long' else '做空'}\n"
            f"关注理由：{card.reason_to_watch}\n"
            f"入场区间：{card.entry_range.low:.2f}-{card.entry_range.high:.2f}\n"
            f"止盈区间：{card.take_profit_range.low:.2f}-{card.take_profit_range.high:.2f}\n"
            f"失效价：{card.invalidation_level:.2f}（{card.invalidation_reason}）\n"
            f"风险提示：{' | '.join(card.risk_notes)}\n"
            f"原文链接：{' | '.join(card.source_refs)}"
        )
