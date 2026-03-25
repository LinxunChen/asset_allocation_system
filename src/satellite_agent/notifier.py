from __future__ import annotations

import json
import re
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


def _truncate_text(value: str, limit: int) -> str:
    text = " ".join((value or "").split()).strip()
    if len(text) <= limit:
        return text
    if limit <= 1:
        return text[:limit]
    return f"{text[: limit - 1].rstrip()}..."


def _normalize_user_facing_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    text = text.replace(
        "当前止盈空间折算后不足 1.5R，赔率不合格，先降级为观察。",
        "按当前入场区、止盈区和失效价估算，预期盈亏比不足，先降级为观察。",
    )
    text = text.replace("赔率不合格", "预期盈亏比不足")
    text = text.replace("低赔率", "预期盈亏比不足")
    text = text.replace("赔率还不够舒服", "预期盈亏比还不够理想")
    text = re.sub(r"不足\s*1\.5R", "不足最低预期盈亏比要求", text)
    return text


def _simplify_chain_summary(chain_summary: str) -> str:
    steps = [step.strip() for step in (chain_summary or "").split("->") if step.strip()]
    if not steps:
        return "首次出现"
    compressed: list[str] = []
    for step in steps:
        if not compressed or compressed[-1] != step:
            compressed.append(step)
    if len(compressed) <= 2:
        return " -> ".join(compressed)
    return f"{compressed[0]} -> {compressed[-1]}"


def _rewrite_terminal_chain_step(step: str, final_action: str) -> str:
    clean_step = (step or "").strip()
    if not clean_step:
        return final_action
    for action in ("进入兑现池", "确认做多", "试探建仓", "加入观察"):
        index = clean_step.rfind(action)
        if index >= 0:
            prefix = clean_step[:index].rstrip(" ：:-")
            prefix = prefix.replace("升级", "").strip()
            return f"{prefix}{final_action}".strip() if prefix else final_action
    return final_action


def _final_chain_summary(chain_summary: str, *, final_action: str, downgraded_to_watch: bool) -> str:
    simplified = _simplify_chain_summary(chain_summary)
    if not downgraded_to_watch:
        return simplified
    steps = [step.strip() for step in simplified.split("->") if step.strip()]
    if not steps:
        return final_action
    if len(steps) == 1:
        return _rewrite_terminal_chain_step(steps[0], final_action)
    steps[-1] = _rewrite_terminal_chain_step(steps[-1], final_action)
    return " -> ".join(steps)


def _source_kind(source_ref: str) -> str:
    if not source_ref.startswith(("http://", "https://")):
        return "other"
    host = urlparse(source_ref).netloc.lower()
    if host in {"www.sec.gov", "sec.gov"}:
        return "official"
    if host in {"www.reuters.com", "reuters.com", "finance.yahoo.com", "www.benzinga.com", "benzinga.com"}:
        return "media"
    if host in {"news.google.com"}:
        return "aggregator"
    return "other"


def _select_source_refs(source_refs: list[str], *, limit: int = 3) -> list[str]:
    ranked = sorted(
        enumerate(source_refs),
        key=lambda item: (
            {"official": 0, "media": 1, "aggregator": 2, "other": 3}.get(_source_kind(item[1]), 4),
            item[0],
        ),
    )
    selected: list[str] = []
    seen_labels: set[str] = set()
    for _, source in ranked:
        label = _source_label(source)
        dedupe_key = f"{_source_kind(source)}::{label}"
        if dedupe_key in seen_labels:
            continue
        seen_labels.add(dedupe_key)
        selected.append(source)
        if len(selected) >= limit:
            break
    return selected


def _visible_risk_notes(card: OpportunityCard, *, downgraded_to_watch: bool) -> list[str]:
    notes = list(card.risk_notes or [])
    if downgraded_to_watch:
        return notes
    filtered: list[str] = []
    for note in notes:
        normalized = (note or "").strip()
        if normalized.startswith("当前止盈空间折算后不足"):
            continue
        filtered.append(_normalize_user_facing_text(normalized))
    return filtered


def _visible_macro_overlay_note(card: OpportunityCard, *, final_action: str) -> str:
    note = (card.macro_overlay_note or "").strip()
    if not note:
        return ""
    if "动作由" in note and final_action not in {"加入观察", "试探建仓"}:
        primary = note.split("，动作由", 1)[0].strip()
        return _normalize_user_facing_text(f"{primary}。".replace("：", "：", 1) if primary else "")
    return _normalize_user_facing_text(note)


def _downgrade_reason(card: OpportunityCard) -> str:
    ratio = _risk_reward_ratio(card)
    if not card.market_data_complete:
        return "降级观察：执行条件不满足"
    if ratio is not None and ratio < 1.5:
        return "降级观察：盈亏比不足"
    if card.relative_volume is not None and card.relative_volume < 1.0:
        return "降级观察：量能不足"
    if card.trend_state in {"neutral", "bearish", "downtrend"}:
        return "降级观察：结构未确认"
    if card.macro_penalty_applied > 0 and card.macro_risk_score >= 70.0:
        return "降级观察：宏观风险压制"
    return "降级观察：执行条件不满足"


def _build_render_view(card: OpportunityCard) -> dict:
    delivery = build_delivery_view_from_card(card)
    current_action = str(delivery.get("action_label_effective") or card.action_label or "").strip()
    is_exit_card = current_action == "进入兑现池"
    requested_formal = current_action in {"确认做多", "试探建仓"}
    ratio = _risk_reward_ratio(card)
    formal_incompatible = requested_formal and (
        not card.market_data_complete
        or not getattr(card, "execution_eligible", True)
        or card.priority == "suppressed"
        or (ratio is not None and ratio < 1.5)
    )
    downgraded_to_watch = not is_exit_card and formal_incompatible
    is_watch_card = downgraded_to_watch or current_action == "加入观察"
    final_action = "加入观察" if is_watch_card else current_action
    final_type = "exit" if is_exit_card else "watch" if is_watch_card else "formal"
    source_refs = _select_source_refs(list(card.source_refs or []))
    source_labels = [_source_label(source) for source in source_refs]
    risk_notes = _visible_risk_notes(card, downgraded_to_watch=downgraded_to_watch)
    risk_text = "；".join(risk_notes[:3]) if risk_notes else "无"
    downgrade_reason = _downgrade_reason(card) if downgraded_to_watch else ""
    if final_type == "watch":
        card_positioning = (
            "观察卡"
            if not downgrade_reason
            else f"自动降级观察卡 | {downgrade_reason}"
        )
    elif final_type == "exit":
        card_positioning = "兑现管理卡"
    else:
        card_positioning = "正式操作卡"
    priority_text = "观察" if downgraded_to_watch else _display_priority(card.priority)
    confidence_text = "观察优先" if downgraded_to_watch else delivery["confidence_label_effective"]
    chain_summary = _final_chain_summary(
        str(delivery.get("chain_summary") or card.chain_summary or "首次出现"),
        final_action=final_action,
        downgraded_to_watch=downgraded_to_watch,
    )
    source_line = " / ".join(
        f"[{_source_label(source)}]({source})" for source in source_refs if source.startswith(("http://", "https://"))
    ) or "、".join(source_labels[:4]) or "暂无"
    core_reason = (
        _normalize_user_facing_text(card.positioning_hint)
        if downgraded_to_watch and card.positioning_hint
        else _normalize_user_facing_text(
            delivery["llm_reasoning"] or card.positioning_hint or card.reason_to_watch or delivery["event_reason_line"]
        )
    )
    risk_reason = _normalize_user_facing_text(delivery["llm_uncertainty"] or (risk_notes[0] if risk_notes else ""))
    execution_plan_lines: list[str] = []
    if final_type == "formal" and card.market_data_complete:
        execution_plan_lines = [
            f"入场区间：{card.entry_range.low:.2f}-{card.entry_range.high:.2f}",
            f"失效价：{card.invalidation_level:.2f}",
            f"预期盈亏比：{_display_risk_reward(card)}",
        ]
    return {
        "delivery": delivery,
        "card_type": final_type,
        "is_watch_card": final_type == "watch",
        "is_exit_card": final_type == "exit",
        "is_formal_card": final_type == "formal",
        "downgraded_to_watch": downgraded_to_watch,
        "downgrade_reason": downgrade_reason,
        "identity": delivery["identity"],
        "action_label": final_action,
        "confidence_text": confidence_text,
        "priority_text": priority_text,
        "card_positioning": card_positioning,
        "chain_summary": chain_summary,
        "source_refs": source_refs,
        "source_labels": source_labels,
        "source_summary": "、".join(source_labels[:4]) if source_labels else "暂无",
        "source_line": source_line,
        "risk_text": risk_text,
        "risk_notes": risk_notes,
        "risk_reward_text": _display_risk_reward(card),
        "market_env_line": (
            f"{delivery['market_regime_display']} | 利率压力 {_risk_level_display(card.rate_risk)}"
            + (
                f" | 地缘风险 {_risk_level_display(card.geopolitical_risk)}"
                if card.geopolitical_risk
                else ""
            )
        ),
        "core_summary": _truncate_text(core_reason or "等待更明确的催化与结构确认。", 38),
        "max_risk": _truncate_text(risk_reason or "需继续核对原文与价格结构。", 22),
        "execution_plan_lines": execution_plan_lines,
        "render_warning": "formal_render_conflict_auto_downgraded" if downgraded_to_watch else "",
        "macro_overlay_note": _visible_macro_overlay_note(card, final_action=final_action),
        "action_detail_text": (
            "已自动降级"
            if downgraded_to_watch
            else "当前按观察处理"
            if final_type == "watch"
            else "优先保护利润"
            if final_type == "exit"
            else delivery["final_score_label"]
        ),
    }


def build_render_view(card: OpportunityCard) -> dict:
    return _build_render_view(card)


def _is_watch_card(card: OpportunityCard, delivery: dict | None = None) -> bool:
    action = (delivery or {}).get("action_label_effective") or card.action_label
    return action == "加入观察"


def _is_exit_card(card: OpportunityCard, delivery: dict | None = None) -> bool:
    action = (delivery or {}).get("action_label_effective") or card.action_label
    return action == "进入兑现池"


def _exit_pool_subreason_display(value: str) -> str:
    return {
        "target_hit": "达标止盈",
        "weakening_after_tp_zone": "提前锁盈",
        "macro_protection": "宏观保护",
    }.get((value or "").strip(), "兑现管理")


def _exit_pool_subreason_explainer(value: str) -> str:
    normalized = (value or "").strip()
    if normalized == "target_hit":
        return "价格已进入计划止盈区更深位置，当前更适合按计划兑现利润。"
    if normalized == "weakening_after_tp_zone":
        return "进入止盈区后连续走弱，继续持有更容易把浮盈回吐回去。"
    if normalized == "macro_protection":
        return "宏观环境转差且已有浮盈，当前更适合先做利润保护。"
    return "当前这笔交易已从进攻逻辑切换到兑现管理。"


def _exit_pool_guardrail_text() -> str:
    return "只面向已有浮盈仓位，不代表新的开仓信号。"


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
        render = _build_render_view(card)
        delivery = render["delivery"]
        is_watch_card = render["is_watch_card"]
        is_exit_card = render["is_exit_card"]
        header_template = (
            "blue"
            if is_watch_card
            else {
                "high": "red",
                "normal": "orange",
                "suppressed": "grey",
            }.get(card.priority, "blue")
        )
        risk_text = render["risk_text"]
        identity = render["identity"]
        action_label = render["action_label"]
        confidence_text = render["confidence_text"]
        priority_text = render["priority_text"]
        theme_text = delivery["theme_text"]
        peer_text = "、".join(delivery["confirmed_peers"][:3]) if delivery["confirmed_peers"] else "暂无"
        event_line = delivery["event_reason_line"]
        market_line = delivery["market_reason_line"]
        theme_line = delivery["theme_reason_line"]
        valid_until_text = delivery["valid_until_text"] or _display_valid_until(card)
        source_text = render["source_summary"]
        source_line = render["source_line"]
        market_env_line = render["market_env_line"]
        card_positioning = render["card_positioning"]
        overview_lines = [
            f"**标的**：{identity}",
            f"**卡片定位**：{card_positioning}",
            f"**操作建议**：{action_label}（{render['action_detail_text']}）",
            f"**链路**：{render['chain_summary']}",
        ]
        if render["downgrade_reason"]:
            overview_lines.append(f"**降级原因**：{render['downgrade_reason']}")
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
        if render["macro_overlay_note"]:
            market_lines.append(f"**宏观覆盖**：{render['macro_overlay_note']}")
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
        if render["macro_overlay_note"]:
            signal_metric_lines.append(
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"**宏观覆盖**\n{render['macro_overlay_note']}\n"
                            f"环境分压制：-{delivery['macro_penalty_applied']:.1f}"
                        ),
                    },
                }
            )
        actions = []
        for source in render["source_refs"]:
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
        elif is_exit_card:
            exit_reason = _exit_pool_subreason_display(card.exit_pool_subreason)
            exit_explainer = _exit_pool_subreason_explainer(card.exit_pool_subreason)
            decision_lines = [
                f"**为什么进入兑现池**：{delivery['llm_reasoning'] or event_line}",
                f"**兑现原因**：{exit_reason} | {exit_explainer}",
                f"**来源链路**：{delivery['chain_summary']}",
                f"**使用边界**：{_exit_pool_guardrail_text()}",
                f"**最大风险**：{delivery['llm_uncertainty'] or risk_text}",
            ]
            observation_lines = [
                f"**当前状态**：{exit_reason}，这笔交易已从进攻切换到兑现管理。",
                f"**来源链路**：{delivery['chain_summary']}",
                f"**原目标区**：{card.take_profit_range.low:.2f} - {card.take_profit_range.high:.2f}",
                f"**处理建议**：{card.positioning_hint or '优先兑现利润，不再继续追新仓。'}",
                f"**使用边界**：{_exit_pool_guardrail_text()}",
            ]
        else:
            decision_lines = [
                f"**一句话核心**：{render['core_summary']}",
                f"**最大风险**：{render['max_risk']}",
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
                            "content": "\n".join(decision_lines),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": (
                                f"**{'观察计划' if is_watch_card else '兑现计划' if is_exit_card else '执行计划'}**\n"
                                f"{'当前更适合先观察后续确认。' if is_watch_card else '当前更适合优先兑现或保护利润。' if is_exit_card else '当前优先按价格计划执行。'}"
                            ),
                        },
                    },
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
                                                f"**失效价**\n{card.invalidation_level:.2f}"
                                            ),
                                        },
                                    },
                                    {
                                        "is_short": True,
                                        "text": {
                                            "tag": "lark_md",
                                            "content": (
                                                f"**预期盈亏比**\n{render['risk_reward_text']}"
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
                                ],
                            }
                        ]
                        if render["is_formal_card"] and card.market_data_complete
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
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**事件摘要**",
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
                            "content": "**市场与结构**",
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
                            "content": (
                                f"**题材联动**\n题材：{theme_text}"
                                + (f"；同题材确认：{peer_text}" if peer_text != "暂无" else "")
                            ),
                        },
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": "**信号评分**",
                        },
                    },
                    {
                        "tag": "div",
                        "fields": signal_metric_lines,
                    },
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
        render = _build_render_view(card)
        delivery = render["delivery"]
        return f"{render['identity']} | {render['action_label']} | {delivery['event_type_display']}"


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
        render = _build_render_view(card)
        delivery = render["delivery"]
        return f"{render['identity']} | {render['action_label']} | {delivery['event_type_display']}"

    def _body(self, card: OpportunityCard) -> str:
        render = _build_render_view(card)
        delivery = render["delivery"]
        is_watch_card = render["is_watch_card"]
        is_exit_card = render["is_exit_card"]
        event_line = delivery["event_reason_line"]
        market_line = delivery["market_reason_line"]
        theme_line = delivery["theme_reason_line"]
        confidence_text = render["confidence_text"]
        risk_reward_text = render["risk_reward_text"]
        source_line = render["source_line"]
        market_env_line = render["market_env_line"]
        price_plan_block = ""
        if render["is_formal_card"] and card.market_data_complete:
            price_plan_block = (
                f"入场区间：{card.entry_range.low:.2f}-{card.entry_range.high:.2f}\n"
                f"失效价：{card.invalidation_level:.2f}（{card.invalidation_reason}）\n"
                f"预期盈亏比：{risk_reward_text}\n"
                f"止盈区间：{card.take_profit_range.low:.2f}-{card.take_profit_range.high:.2f}\n"
            )
        overview_lines = [
            f"标的：{render['identity']}",
            f"卡片定位：{render['card_positioning']}",
            f"操作建议：{render['action_label']}（{render['action_detail_text']}）",
            f"链路：{render['chain_summary']}",
        ]
        if render["downgrade_reason"]:
            overview_lines.append(f"降级原因：{render['downgrade_reason']}")
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
        if render["macro_overlay_note"]:
            market_section.append(f"- 宏观覆盖：{render['macro_overlay_note']}")
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
        if is_watch_card:
            decision_section = [
                "决策结论：",
                f"- 为什么现在先观察：{delivery['llm_reasoning'] or event_line}",
                f"- 当前处理：{card.positioning_hint or '先放入观察名单，等待更明确的催化与结构确认。'}",
                f"- 最大风险：{delivery['llm_uncertainty'] or '需继续核对原文与价格结构'}",
            ]
        elif is_exit_card:
            exit_reason = _exit_pool_subreason_display(card.exit_pool_subreason)
            exit_explainer = _exit_pool_subreason_explainer(card.exit_pool_subreason)
            decision_section = [
                "兑现结论：",
                f"- 为什么进入兑现池：{delivery['llm_reasoning'] or event_line}",
                f"- 兑现原因：{exit_reason}（{exit_explainer}）",
                f"- 来源链路：{render['chain_summary']}",
                f"- 使用边界：{_exit_pool_guardrail_text()}",
                f"- 最大风险：{delivery['llm_uncertainty'] or '继续持有可能回吐利润，过早兑现也可能少赚。'}",
            ]
        else:
            decision_section = [
                "决策结论：",
                f"- 一句话核心：{render['core_summary']}",
                f"- 最大风险：{render['max_risk']}",
            ]
        body_lines = [
            *overview_lines,
            "",
            "执行计划：" if render["is_formal_card"] else "观察计划：" if is_watch_card else "兑现计划：",
        ]
        if price_plan_block:
            body_lines.append(price_plan_block.rstrip())
        elif is_watch_card:
            body_lines.extend(
                [
                    f"- 关注重点：{card.reason_to_watch or delivery['llm_reasoning']}",
                    f"- 升级触发：{delivery['llm_impact_inference'] or delivery['trend_state_explainer']}",
                ]
            )
        else:
            exit_reason = _exit_pool_subreason_display(card.exit_pool_subreason)
            body_lines.extend(
                [
                    f"- 当前状态：{exit_reason}，这笔交易已从进攻切换到兑现管理。",
                    f"- 来源链路：{render['chain_summary']}",
                    f"- 原目标区：{card.take_profit_range.low:.2f}-{card.take_profit_range.high:.2f}",
                    f"- 处理建议：{card.positioning_hint or '优先兑现利润，不再继续追新仓。'}",
                ]
            )
        body_lines.extend(
            [
                "",
                *decision_section,
                "",
                *event_section,
            ]
        )
        body_lines.extend(["", *market_section, "", *score_section])
        if is_exit_card and card.positioning_hint:
            body_lines.extend(["", f"当前处理：{card.positioning_hint}"])
        if is_exit_card:
            body_lines.extend(
                [
                    "",
                    "兑现补充：",
                    f"- 使用边界：{_exit_pool_guardrail_text()}",
                ]
            )
        body_lines.extend(
            [
                "",
                f"原文与来源：{source_line}",
                f"原文链接：{' | '.join(render['source_refs'] or card.source_refs)}",
                "",
                f"风险提示：{' | '.join(render['risk_notes'] or ['无'])}",
            ]
        )
        return "\n".join(body_lines)
