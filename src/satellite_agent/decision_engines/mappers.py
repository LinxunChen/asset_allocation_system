from __future__ import annotations

from dataclasses import replace
from datetime import timedelta, timezone
from urllib.parse import urlparse

from ..models import OpportunityCard
from .types import DecisionPacket

CN_TZ = timezone(timedelta(hours=8))


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


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
        "test_notification": "测试提醒",
    }.get(event_type, event_type)


def _display_trend_state(value: str) -> str:
    return {
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "震荡",
        "uptrend": "多头",
        "downtrend": "空头",
    }.get(value, value or "未识别")


def _source_label(source_ref: str) -> str:
    if not source_ref:
        return "未知来源"
    if not source_ref.startswith(("http://", "https://")):
        return source_ref
    host = urlparse(source_ref).netloc.lower()
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
    return mapping.get(host, host.replace("www.", "") if host else source_ref)


def _identity(display_name: str, symbol: str) -> str:
    clean_name = (display_name or "").strip()
    clean_symbol = (symbol or "").strip().upper()
    if not clean_name or clean_name.upper() == clean_symbol:
        return clean_symbol
    return f"{clean_name}（{clean_symbol}）"


def _action_label(action_label: str, *, market_data_complete: bool, priority: str) -> str:
    if action_label:
        return action_label
    if not market_data_complete:
        return "加入观察"
    return {"high": "确认做多", "normal": "试探建仓", "suppressed": "加入观察"}.get(priority, "加入观察")


def build_delivery_view_from_card(card: OpportunityCard) -> dict:
    return build_delivery_view_from_record(card.to_record())


def build_delivery_view_from_record(card: dict) -> dict:
    source_refs = list(card.get("source_refs") or [])
    source_labels = []
    for source in source_refs:
        label = _source_label(source)
        if label not in source_labels:
            source_labels.append(label)
    market_data_complete = bool(card.get("market_data_complete", True))
    priority = str(card.get("priority") or "")
    confidence_label = str(card.get("confidence_label") or ("低" if not market_data_complete else "中"))
    trend_state = str(card.get("trend_state") or "")
    action_label = _action_label(
        str(card.get("action_label") or ""),
        market_data_complete=market_data_complete,
        priority=priority,
    )
    event_line = f"{_display_event_type(str(card.get('event_type') or ''))}：{card.get('headline_summary') or ''}".strip("：")
    if market_data_complete:
        market_bits = []
        if trend_state:
            market_bits.append(f"当前为{_display_trend_state(trend_state)}结构")
        if card.get("rsi_14") is not None:
            market_bits.append(f"RSI {float(card['rsi_14']):.1f}")
        if card.get("relative_volume") is not None:
            market_bits.append(f"相对量能 {float(card['relative_volume']):.2f} 倍")
        market_line = "；".join(market_bits) if market_bits else "行情结构正常。"
    else:
        market_line = str(card.get("market_data_note") or "行情快照暂不可用，需人工确认价格结构。")
    theme_tags = list(card.get("theme_tags") or [])
    confirmed_peers = list(card.get("confirmed_peer_symbols") or [])
    theme_text = " / ".join(theme_tags) if theme_tags else "未标注"
    if confirmed_peers:
        theme_line = f"题材：{theme_text}；同题材已有确认标的：{'、'.join(confirmed_peers[:3])}"
    else:
        theme_line = f"题材：{theme_text}"
    ttl_raw = card.get("ttl")
    valid_until_text = ""
    if ttl_raw:
        try:
            valid_until_text = card["ttl"]
            from datetime import datetime

            valid_until_text = (
                datetime.fromisoformat(str(ttl_raw))
                .astimezone(CN_TZ)
                .strftime("%m-%d %H:%M")
            )
        except ValueError:
            valid_until_text = str(ttl_raw)
    return {
        "identity": _identity(str(card.get("display_name") or ""), str(card.get("symbol") or "")),
        "symbol": str(card.get("symbol") or "").upper(),
        "display_name": str(card.get("display_name") or ""),
        "event_type_display": _display_event_type(str(card.get("event_type") or "")),
        "priority_display": _display_priority(priority),
        "horizon_display": _display_horizon(str(card.get("horizon") or "")),
        "action_label_effective": action_label,
        "confidence_label_effective": confidence_label,
        "theme_text": theme_text,
        "theme_tags": theme_tags,
        "confirmed_peers": confirmed_peers,
        "source_labels": source_labels,
        "source_summary": "、".join(source_labels[:4]) if source_labels else "暂无",
        "event_reason_line": event_line,
        "market_reason_line": market_line,
        "theme_reason_line": theme_line,
        "trend_state_display": _display_trend_state(trend_state),
        "trend_state_raw": trend_state,
        "valid_until_text": valid_until_text,
        "market_data_complete": market_data_complete,
    }


def decision_packet_to_card(packet: DecisionPacket, base_card: OpportunityCard) -> OpportunityCard:
    reason_to_watch = " ".join(
        part.strip()
        for part in (
            packet.reason_sections.event,
            packet.reason_sections.market,
            packet.reason_sections.theme,
            packet.reason_sections.positioning,
        )
        if part and part.strip()
    ).strip()
    positioning_hint = packet.reason_sections.positioning or base_card.positioning_hint
    price_plan = packet.price_plan
    entry_range = price_plan.entry_range if price_plan is not None else base_card.entry_range
    take_profit_range = price_plan.take_profit_range if price_plan is not None else base_card.take_profit_range
    invalidation_level = price_plan.invalidation_level if price_plan is not None else base_card.invalidation_level
    invalidation_reason = price_plan.invalidation_reason if price_plan is not None else base_card.invalidation_reason
    return replace(
        base_card,
        action_label=packet.action,
        confidence_label=packet.confidence,
        confidence_score=base_card.confidence_score,
        reason_to_watch=reason_to_watch or base_card.reason_to_watch,
        positioning_hint=positioning_hint,
        theme_tags=list(packet.theme_assessment.theme_ids),
        confirmed_peer_symbols=list(packet.theme_assessment.confirmed_peers),
        trend_state=packet.market_assessment.trend_state or base_card.trend_state,
        rsi_14=packet.market_assessment.rsi_14,
        relative_volume=packet.market_assessment.relative_volume,
        market_data_complete=packet.market_assessment.market_data_complete,
        market_data_note=packet.market_assessment.market_data_note,
        risk_notes=list(dict.fromkeys(base_card.risk_notes + packet.market_assessment.risk_flags + packet.event_assessment.risk_notes)),
        source_refs=list(packet.source_bundle.refs) or base_card.source_refs,
        entry_range=entry_range,
        take_profit_range=take_profit_range,
        invalidation_level=invalidation_level,
        invalidation_reason=invalidation_reason,
    )
