from __future__ import annotations

from datetime import datetime
import json
import re
from typing import Any, Iterable

from .decision_engines.mappers import build_delivery_view_from_record
from .outcomes import normalize_close_reason
from .theme_linkage import (
    build_candidate_pool_peer_map,
    build_theme_display_name_map_from_watchlist_payload,
    build_symbol_theme_map_from_watchlist_payload,
    build_theme_snapshot_rows,
    theme_tags_for_symbol,
)
from .timefmt import format_beijing_minute, normalize_timestamp_fields, parse_datetime


def _display_run_status(status: str) -> str:
    return {"success": "成功", "failed": "失败"}.get(status, status)


def _display_event_type(event_type: str) -> str:
    return {
        "earnings": "财报",
        "guidance": "指引",
        "sec": "公告",
        "research": "研报",
        "m&a": "并购",
        "strategic": "战略合作",
        "product": "产品",
        "news": "新闻",
    }.get(event_type, event_type)


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


def _display_identity(row: dict[str, Any]) -> str:
    display_name = str(row.get("display_name") or "").strip()
    symbol = str(row.get("symbol") or "").strip().upper()
    if not display_name or display_name.upper() == symbol:
        return symbol
    return f"{display_name}（{symbol}）"


def _display_priority(priority: str) -> str:
    return {"high": "高优先级", "normal": "普通", "suppressed": "压制"}.get(priority, priority)


def _display_alert_skip_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    mapping = {
        "threshold_not_met": "停留在观察级别",
        "quality_cutoff": "低于普通提醒门槛",
        "run_alert_budget_exhausted": "超出本轮提醒上限",
        "symbol_alert_budget_exhausted": "超出单标的提醒上限",
        "deduplicated": "命中去重",
        "sent": "已发送",
        "dry_run": "演练发送",
        "no_transport_configured": "未配置发送通道",
    }
    if normalized in mapping:
        return mapping[normalized]
    if normalized.startswith("transport_error:"):
        return f"发送失败（{normalized.split(':', 1)[1] or 'Unknown'}）"
    return normalized or "未说明"


def _display_parameter_label(parameter_key: str) -> str:
    return {
        "event_score_threshold": "确认阈值",
        "confirmation_bonus": "确认加分项",
        "execution_entry_buffer_pct": "入场缓冲",
        "execution_invalidation_buffer_pct": "失效缓冲",
        "normal_alert_min_final_score": "普通提醒门槛",
        "max_alerts_per_run": "单轮提醒上限",
        "max_alerts_per_symbol_per_run": "单标的提醒上限",
        "cross_source_dedup_hours": "跨源去重窗口",
        "swing.priority_threshold": "短线优先级阈值",
        "position.priority_threshold": "波段优先级阈值",
    }.get(parameter_key, parameter_key)


def _display_parameter_direction(direction: str) -> str:
    return {
        "high": "更可能偏高",
        "low": "更可能偏低",
        "inspect": "先排查",
    }.get(direction, direction or "先排查")


def _display_candidate_setup(setup_type: str) -> str:
    return {
        "breakout_watch": "突破预热",
        "pullback_watch": "回踩蓄势",
        "relative_strength_watch": "相对强势",
    }.get(setup_type, setup_type)


def _display_candidate_trigger_mode(trigger_mode: str) -> str:
    return {
        "event": "事件预热",
        "structure": "结构预热",
    }.get(trigger_mode, trigger_mode or "结构预热")


def _normalize_candidate_stage(stage: str) -> str:
    normalized = str(stage or "").strip()
    if normalized == "prewatch":
        return "candidate_pool"
    return normalized


def _normalize_decision_pool(pool: str) -> str:
    normalized = str(pool or "").strip()
    if normalized == "prewatch":
        return "candidate_pool"
    return normalized


def _candidate_stage_summary(summary: dict[str, Any], stage: str) -> dict[str, Any]:
    normalized = _normalize_candidate_stage(stage)
    if normalized == "candidate_pool":
        return dict(summary.get("candidate_pool") or summary.get("prewatch") or {})
    return dict(summary.get(normalized) or {})


def _display_prewatch_setup(setup_type: str) -> str:
    return _display_candidate_setup(setup_type)


def _display_prewatch_trigger_mode(trigger_mode: str) -> str:
    return _display_candidate_trigger_mode(trigger_mode)


def _display_pool(pool: str) -> str:
    normalized = _normalize_decision_pool(pool)
    return {
        "candidate_pool": "第一池：候选池",
        "prewatch": "第一池：候选池",
        "confirmation": "第二池：确认池",
        "exit": "第三池：持仓管理",
        "holding_management": "第三池：持仓管理",
    }.get(normalized, normalized)


def _format_alert_suppression_lines(
    card_diagnostics: list[dict[str, Any]],
    candidate_evaluation_summary: dict[str, Any] | None,
) -> list[str]:
    summary = _build_alert_suppression_summary(card_diagnostics, candidate_evaluation_summary)
    if int(summary.get("total_cards", 0) or 0) <= 0:
        return ["  本轮没有可诊断的提醒压制数据。"]

    lines = [
        "  "
        f"共 {summary.get('total_cards', 0)} 张卡片，其中 {summary.get('observation_level_count', 0)} 张停留在观察级别，"
        f"{summary.get('delivery_candidate_count', 0)} 张进入正式提醒候选，实际发送 {summary.get('sent_count', 0)} 张。"
    ]
    confirmation_blocked = summary.get("confirmation_blocked_reasons") or []
    if confirmation_blocked:
        parts = [
            f"{item.get('label', item.get('reason', '-'))} {int(item.get('count', 0) or 0)} 条"
            for item in confirmation_blocked
        ]
        lines.append(f"  确认机会候选被挡主因：{' / '.join(parts)}")
    delivery_blocked = summary.get("delivery_blocked_reasons") or []
    if delivery_blocked:
        parts = [
            f"{item.get('label', item.get('reason', '-'))} {int(item.get('count', 0) or 0)} 张"
            for item in delivery_blocked[:3]
        ]
        lines.append(f"  提醒阶段被拦主因：{' / '.join(parts)}")
    elif int(summary.get("delivery_candidate_count", 0) or 0) > 0:
        lines.append("  进入正式提醒候选的卡片本轮都已经发出。")
    else:
        lines.append("  本轮没有卡片进入正式提醒候选，优先检查确认机会门槛和优先级门槛。")

    return lines


def _build_alert_suppression_summary(
    card_diagnostics: list[dict[str, Any]] | None,
    candidate_evaluation_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    diagnostics = card_diagnostics or []
    observation_level_cards = [row for row in diagnostics if str(row.get("priority") or "") == "suppressed"]
    delivery_candidates = [row for row in diagnostics if str(row.get("priority") or "") != "suppressed"]
    sent_candidates = [row for row in delivery_candidates if row.get("sent")]
    blocked_candidates = [row for row in delivery_candidates if not row.get("sent")]

    confirmation_summary = (candidate_evaluation_summary or {}).get("confirmation") or {}
    confirmation_blocked = [
        {
            "reason": str(item.get("reason") or ""),
            "label": str(item.get("label") or item.get("reason") or ""),
            "count": int(item.get("count", 0) or 0),
        }
        for item in (confirmation_summary.get("top_blocked_reasons") or [])
    ]

    delivery_reason_counts: dict[str, int] = {}
    for row in blocked_candidates:
        reason = str(row.get("alert_reason") or "").strip()
        if not reason:
            continue
        delivery_reason_counts[reason] = delivery_reason_counts.get(reason, 0) + 1
    delivery_blocked = [
        {
            "reason": reason,
            "label": _display_alert_skip_reason(reason),
            "count": count,
        }
        for reason, count in sorted(delivery_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    ]

    return {
        "total_cards": len(diagnostics),
        "observation_level_count": len(observation_level_cards),
        "delivery_candidate_count": len(delivery_candidates),
        "sent_count": len(sent_candidates),
        "blocked_delivery_count": len(blocked_candidates),
        "confirmation_blocked_reasons": confirmation_blocked,
        "delivery_blocked_reasons": delivery_blocked,
        "parameter_checklist": _build_alert_suppression_parameter_checklist(diagnostics, candidate_evaluation_summary),
    }


def _build_alert_suppression_parameter_checklist(
    card_diagnostics: list[dict[str, Any]],
    candidate_evaluation_summary: dict[str, Any] | None,
    *,
    limit: int = 5,
) -> list[str]:
    hints: list[tuple[str, str]] = []
    confirmation_summary = (candidate_evaluation_summary or {}).get("confirmation") or {}
    blocked_reasons = [
        str(item.get("reason") or "").strip()
        for item in (confirmation_summary.get("top_blocked_reasons") or [])
        if str(item.get("reason") or "").strip()
    ]
    if "threshold_not_met" in blocked_reasons:
        hints.extend(
            [
                ("event_score_threshold", "high"),
                ("confirmation_bonus", "low"),
            ]
        )
    if "execution_ineligible" in blocked_reasons:
        hints.extend(
            [
                ("execution_entry_buffer_pct", "inspect"),
                ("execution_invalidation_buffer_pct", "inspect"),
            ]
        )

    observation_level_cards = [row for row in card_diagnostics if str(row.get("priority") or "") == "suppressed"]
    if observation_level_cards:
        hints.extend(
            [
                ("swing.priority_threshold", "high"),
                ("position.priority_threshold", "high"),
            ]
        )

    alert_reasons = {
        str(row.get("alert_reason") or "").strip()
        for row in card_diagnostics
        if str(row.get("priority") or "") != "suppressed" and not row.get("sent")
    }
    if "quality_cutoff" in alert_reasons:
        hints.append(("normal_alert_min_final_score", "high"))
    if "run_alert_budget_exhausted" in alert_reasons:
        hints.append(("max_alerts_per_run", "low"))
    if "symbol_alert_budget_exhausted" in alert_reasons:
        hints.append(("max_alerts_per_symbol_per_run", "low"))
    if "deduplicated" in alert_reasons:
        hints.append(("cross_source_dedup_hours", "inspect"))

    seen: set[str] = set()
    items: list[str] = []
    for key, direction in hints:
        if not key or key in seen:
            continue
        seen.add(key)
        items.append(f"{_display_parameter_label(key)}（{key}，{_display_parameter_direction(direction)}）")
        if len(items) >= limit:
            break
    return items


def _display_outcome_support_title(title: str) -> str:
    return {
        "Thin Evidence": "支撑偏薄",
        "Building Evidence": "支撑积累中",
        "Strong Evidence": "支撑较强",
        "Thin": "偏薄",
        "Building": "积累中",
        "Strong": "较强",
    }.get(title, title)


def _display_outcome_recommendation_title(title: str) -> str:
    return {
        "Expand Outcome Coverage": "补足后验覆盖",
        "Provisional Bias": "观察优先",
        "Measured Bias": "渐进确认",
        "Promote Mature Bias": "成熟偏好优先",
    }.get(title, title)


def _display_strategy_tilt_title(title: str) -> str:
    return {
        "Stay Balanced": "均衡推进",
        "Lean Offensive": "偏进攻",
        "Prefer Selectivity": "偏精选",
        "Stay Defensive": "偏防守",
        "Wait For Maturity": "等待成熟",
    }.get(title, title)


def _display_llm_component(component: str) -> str:
    return {
        "event_extraction": "事件抽取",
        "narration": "卡片叙事",
        "ranking_assist": "排序辅助",
    }.get(component, component or "未标注")


def _config_summary(config_snapshot: dict[str, Any]) -> str:
    settings = config_snapshot.get("settings", {})
    horizons = settings.get("horizons", {})
    swing = horizons.get("swing", {})
    position = horizons.get("position", {})
    event_floor = settings.get("event_score_threshold")
    llm_event_extraction = settings.get("use_llm_event_extraction")
    dedup_hours = settings.get("cross_source_dedup_hours")
    llm_narration = settings.get("use_llm_narration")
    llm_ranking = settings.get("use_llm_ranking_assist")
    macro_overlay = settings.get("use_macro_risk_overlay")
    event_score_weights = settings.get("event_score_weights") or {}
    if (
        event_floor is None
        and llm_event_extraction is None
        and dedup_hours is None
        and llm_narration is None
        and llm_ranking is None
        and macro_overlay is None
        and not event_score_weights
        and not horizons
    ):
        return "-"
    toggle_parts = []
    if llm_event_extraction is not None:
        toggle_parts.append(f"X{1 if llm_event_extraction else 0}")
    if llm_narration is not None:
        toggle_parts.append(f"N{1 if llm_narration else 0}")
    if llm_ranking is not None:
        toggle_parts.append(f"R{1 if llm_ranking else 0}")
    if macro_overlay is not None:
        toggle_parts.append(f"M{1 if macro_overlay else 0}")
    parts = [
        f"E{event_floor}",
        f"S{swing.get('market_score_threshold', '-')}-{swing.get('priority_threshold', '-')}",
        f"P{position.get('market_score_threshold', '-')}-{position.get('priority_threshold', '-')}",
        f"D{dedup_hours}",
    ]
    if toggle_parts:
        parts.append("/".join(toggle_parts))
    if event_score_weights:
        parts.append(
            ",".join(
                [
                    f"I{event_score_weights.get('importance', '-')}",
                    f"C{event_score_weights.get('source_credibility', '-')}",
                    f"Nv{event_score_weights.get('novelty', '-')}",
                    f"T{event_score_weights.get('theme_relevance', '-')}",
                    f"S{event_score_weights.get('sentiment', '-')}",
                ]
            )
        )
    return "/".join(parts)


def _format_summary_value(key: str, value: Any) -> Any:
    if key.endswith("_at") or key in {"since", "timestamp"}:
        parsed = parse_datetime(value)
        if parsed is not None:
            return format_beijing_minute(parsed)
    return value


def serialize_runs(rows: Iterable[object]) -> list[dict[str, Any]]:
    items = []
    for row in rows:
        summary = json.loads(row["summary_json"])
        failures = (
            summary.get("extraction_failures", 0)
            + summary.get("market_data_failures", 0)
            + summary.get("scoring_failures", 0)
            + summary.get("notification_failures", 0)
        )
        items.append(
            {
                "run_id": row["run_id"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "run_name": row["run_name"] if "run_name" in row.keys() else "",
                "note": row["note"] if "note" in row.keys() else "",
                "config_snapshot": (
                    json.loads(row["config_snapshot_json"])
                    if "config_snapshot_json" in row.keys() and row["config_snapshot_json"]
                    else {}
                ),
                "summary": summary,
                "failures": failures,
            }
        )
    return items


def serialize_run_detail(row: object, logs: Iterable[object]) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "run_name": row["run_name"] if "run_name" in row.keys() else "",
        "note": row["note"] if "note" in row.keys() else "",
        "config_snapshot": (
            json.loads(row["config_snapshot_json"])
            if "config_snapshot_json" in row.keys() and row["config_snapshot_json"]
            else {}
        ),
        "summary": json.loads(row["summary_json"]),
        "logs": serialize_logs(logs),
    }


def serialize_logs(rows: Iterable[object]) -> list[dict[str, Any]]:
    items = []
    for row in rows:
        items.append(
            {
                "created_at": row["created_at"],
                "level": row["level"],
                "event_type": row["event_type"],
                "stage": row["stage"],
                "symbol": row["symbol"],
                "event_id": row["event_id"],
                "message": row["message"],
                "context": normalize_timestamp_fields(json.loads(row["context_json"]) if row["context_json"] else {}),
            }
        )
    return items


def serialize_error_summary(rows: Iterable[object]) -> list[dict[str, Any]]:
    return [
        {
            "count": row["count"],
            "event_type": row["event_type"],
            "stage": row["stage"],
            "symbol": row["symbol"],
            "message": row["message"],
        }
        for row in rows
    ]


def serialize_source_health(rows: Iterable[object]) -> list[dict[str, Any]]:
    return [
        {
            "source_name": row["source_name"],
            "status": row["status"],
            "detail": row["detail"],
            "latency_ms": row["latency_ms"],
            "checked_at": row["checked_at"],
        }
        for row in rows
    ]


def _is_external_connectivity_issue(detail: str) -> bool:
    normalized = (detail or "").lower()
    markers = (
        "urlerror",
        "nodename nor servname provided",
        "temporary failure in name resolution",
        "name or service not known",
        "getaddrinfo failed",
        "network is unreachable",
        "no route to host",
        "failed to establish a new connection",
    )
    return any(marker in normalized for marker in markers)


def _external_connectivity_hint(source_name: str) -> str:
    return f"{source_name}：检测到外网连通性异常，请先检查 VPN 或当前网络。"


def summarize_external_connectivity_issues(source_health: list[dict[str, Any]] | Iterable[object]) -> dict[str, Any]:
    rows = list(source_health)
    affected_sources: list[str] = []
    for row in rows:
        detail = str(getattr(row, "get", lambda _k, _d=None: _d)("detail", "") if hasattr(row, "get") else row["detail"])
        status = str(getattr(row, "get", lambda _k, _d=None: _d)("status", "") if hasattr(row, "get") else row["status"])
        source_name = str(getattr(row, "get", lambda _k, _d=None: _d)("source_name", "") if hasattr(row, "get") else row["source_name"])
        if status != "healthy" and _is_external_connectivity_issue(detail):
            affected_sources.append(source_name)
    unique_sources = sorted({item for item in affected_sources if item})
    return {
        "count": len(unique_sources),
        "sources": unique_sources,
        "has_issue": bool(unique_sources),
    }


def serialize_strategy_report(
    event_types: Iterable[object],
    source_stability: Iterable[object],
    alert_volume: Iterable[object],
    decision_outcomes_by_event_type: Iterable[object] | None = None,
    decision_outcomes_by_pool: Iterable[object] | None = None,
    outcome_data_coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_type_items = [
        {
            "event_type": row["event_type"],
            "card_count": row["card_count"],
            "avg_final_score": row["avg_final_score"],
            "high_priority_count": row["high_priority_count"],
        }
        for row in event_types
    ]
    source_stability_items = [
        {
            "source_name": row["source_name"],
            "check_count": row["check_count"],
            "healthy_count": row["healthy_count"],
            "unhealthy_count": row["unhealthy_count"],
            "avg_latency_ms": row["avg_latency_ms"],
            "last_checked_at": row["last_checked_at"],
        }
        for row in source_stability
    ]
    alert_volume_items = [
        {
            "bucket_date": row["bucket_date"],
            "total_alerts": row["total_alerts"],
            "sent_alerts": row["sent_alerts"],
            "suppressed_alerts": row["suppressed_alerts"],
            "high_priority_alerts": row["high_priority_alerts"],
            "sent_high_priority_alerts": row["sent_high_priority_alerts"] if "sent_high_priority_alerts" in row.keys() else 0,
        }
        for row in alert_volume
    ]
    outcome_event_type_items = [
        {
            "event_type": row["event_type"],
            "decision_count": row["decision_count"],
            "outcome_count": row["outcome_count"],
            "pending_count": row["pending_count"],
            "completed_count": max(int(row["outcome_count"]) - int(row["pending_count"]), 0),
            "take_profit_hits": row["take_profit_hits"],
            "invalidation_hits": row["invalidation_hits"],
            "positive_t3_count": row["positive_t3_count"],
            "t_plus_3_sample_count": row["t_plus_3_sample_count"] if "t_plus_3_sample_count" in row.keys() else row["outcome_count"],
            "max_runup_sample_count": row["max_runup_sample_count"] if "max_runup_sample_count" in row.keys() else row["outcome_count"],
            "max_drawdown_sample_count": row["max_drawdown_sample_count"] if "max_drawdown_sample_count" in row.keys() else row["outcome_count"],
            "avg_t_plus_3_return": row["avg_t_plus_3_return"],
            "avg_max_runup": row["avg_max_runup"],
            "avg_max_drawdown": row["avg_max_drawdown"],
            **_assess_outcome_row_readiness(
                {
                    "completed_count": max(int(row["outcome_count"]) - int(row["pending_count"]), 0),
                    "pending_count": row["pending_count"],
                    "avg_t_plus_3_return": row["avg_t_plus_3_return"],
                },
                axis="event",
            ),
        }
        for row in (decision_outcomes_by_event_type or [])
    ]
    outcome_pool_items = [
        {
            "pool": row["pool"],
            "decision_count": row["decision_count"],
            "outcome_count": row["outcome_count"],
            "pending_count": row["pending_count"],
            "completed_count": max(int(row["outcome_count"]) - int(row["pending_count"]), 0),
            "take_profit_hits": row["take_profit_hits"],
            "invalidation_hits": row["invalidation_hits"],
            "positive_t3_count": row["positive_t3_count"],
            "t_plus_3_sample_count": row["t_plus_3_sample_count"] if "t_plus_3_sample_count" in row.keys() else row["outcome_count"],
            "max_runup_sample_count": row["max_runup_sample_count"] if "max_runup_sample_count" in row.keys() else row["outcome_count"],
            "max_drawdown_sample_count": row["max_drawdown_sample_count"] if "max_drawdown_sample_count" in row.keys() else row["outcome_count"],
            "avg_t_plus_3_return": row["avg_t_plus_3_return"],
            "avg_max_runup": row["avg_max_runup"],
            "avg_max_drawdown": row["avg_max_drawdown"],
            **_assess_outcome_row_readiness(
                {
                    "completed_count": max(int(row["outcome_count"]) - int(row["pending_count"]), 0),
                    "pending_count": row["pending_count"],
                    "avg_t_plus_3_return": row["avg_t_plus_3_return"],
                },
                axis="pool",
            ),
        }
        for row in (decision_outcomes_by_pool or [])
    ]
    outcome_maturity = _summarize_outcome_maturity(
        outcome_event_type_items,
        outcome_pool_items,
        outcome_data_coverage or {},
    )
    completed_cohort_summary = _summarize_completed_cohorts(
        outcome_event_type_items,
        outcome_pool_items,
    )
    outcome_support = _assess_outcome_support(
        outcome_event_type_items,
        outcome_pool_items,
        outcome_data_coverage or {},
    )
    confidence_regime = _classify_confidence_regime(outcome_support)
    outcome_summary = _summarize_strategy_outcomes(
        outcome_event_type_items,
        outcome_pool_items,
        outcome_support=outcome_support,
        confidence_regime=confidence_regime,
    )
    outcome_recommendation = _recommend_strategy_outcomes(
        outcome_event_type_items,
        outcome_pool_items,
        outcome_support=outcome_support,
        confidence_regime=confidence_regime,
    )
    outcome_validation = _validate_strategy_outcomes(
        outcome_event_type_items,
        outcome_pool_items,
        outcome_data_coverage or {},
    )
    strategy_tilt = _recommend_strategy_tilt(
        event_type_items,
        alert_volume_items,
        outcome_event_type_items,
        outcome_pool_items,
        outcome_support=outcome_support,
    )
    action_regime = _build_action_regime(
        confidence_regime,
        outcome_support,
        outcome_recommendation,
        strategy_tilt,
    )
    outcome_readiness = _summarize_outcome_readiness(
        outcome_event_type_items,
        outcome_pool_items,
    )
    outcome_readiness_actions = _build_outcome_readiness_actions(
        outcome_event_type_items,
        outcome_pool_items,
    )
    outcome_headline = _build_outcome_headline(
        confidence_regime,
        action_regime,
        outcome_readiness_actions,
    )
    outcome_priority_list = _build_outcome_priority_list(
        outcome_readiness_actions,
    )
    return {
        "event_type_performance": event_type_items,
        "source_stability": source_stability_items,
        "alert_volume": alert_volume_items,
        "decision_outcomes_by_event_type": outcome_event_type_items,
        "decision_outcomes_by_pool": outcome_pool_items,
        "outcome_data_coverage": outcome_data_coverage or {},
        "outcome_summary": outcome_summary,
        "outcome_maturity": outcome_maturity,
        "completed_cohort_summary": completed_cohort_summary,
        "outcome_support": outcome_support,
        "confidence_regime": confidence_regime,
        "outcome_validation": outcome_validation,
        "outcome_recommendation": outcome_recommendation,
        "strategy_tilt": strategy_tilt,
        "action_regime": action_regime,
        "outcome_readiness": outcome_readiness,
        "outcome_readiness_actions": outcome_readiness_actions,
        "outcome_headline": outcome_headline,
        "outcome_priority_list": outcome_priority_list,
    }


def _summarize_strategy_outcomes(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
    *,
    outcome_support: dict[str, Any] | None = None,
    confidence_regime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lines: list[str] = []
    regime = confidence_regime or {}
    if regime.get("line_items"):
        lines.append(f"Confidence regime: {regime.get('title', '-')}.")
        for line in regime["line_items"]:
            lines.append(line)
    support = outcome_support or {}
    event_support = support.get("event_support") or {}
    pool_support = support.get("pool_support") or {}
    support_fragments: list[str] = []
    if event_support.get("title") in {"Thin", "Building"}:
        support_fragments.append(f"event-side evidence is {str(event_support.get('title', '')).lower()}")
    if pool_support.get("title") in {"Thin", "Building"}:
        support_fragments.append(f"pool-side evidence is {str(pool_support.get('title', '')).lower()}")
    if support_fragments:
        lines.append("Summary confidence is tempered because " + " and ".join(support_fragments) + ".")
    mature_event_rows = [
        row for row in event_type_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    mature_pool_rows = [
        row for row in pool_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    if mature_event_rows:
        strongest_event = max(
            mature_event_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )
        lines.append(
            f"Strongest event outcome so far: {strongest_event['event_type']} avg_t3={strongest_event['avg_t_plus_3_return']} with {strongest_event['outcome_count']} outcomes."
        )
        if len(mature_event_rows) > 1:
            weakest_event = min(
                mature_event_rows,
                key=lambda row: (
                    float(row.get("avg_t_plus_3_return") or float("inf")),
                    -row.get("positive_t3_count", 0),
                    -row.get("outcome_count", 0),
                ),
            )
            if weakest_event["event_type"] != strongest_event["event_type"]:
                lines.append(
                    f"Weakest event outcome so far: {weakest_event['event_type']} avg_t3={weakest_event['avg_t_plus_3_return']}."
                )
    if mature_pool_rows:
        strongest_pool = max(
            mature_pool_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )
        lines.append(
            f"Best pool so far: {strongest_pool['pool']} avg_t3={strongest_pool['avg_t_plus_3_return']} with {strongest_pool['outcome_count']} outcomes."
        )
    if pool_rows:
        most_pending_pool = max(pool_rows, key=lambda row: (row.get("pending_count", 0), row.get("decision_count", 0)))
        if most_pending_pool.get("pending_count", 0) > 0:
            lines.append(
                f"Most pending lookahead: {most_pending_pool['pool']} still has {most_pending_pool['pending_count']} decisions waiting for more bars."
            )
    if not lines:
        lines.append("No mature decision outcomes yet.")
    return {"line_items": lines}


def _classify_confidence_regime(outcome_support: dict[str, Any] | None) -> dict[str, Any]:
    support = outcome_support or {}
    support_title = support.get("title", "")
    event_support_title = (support.get("event_support") or {}).get("title", "")
    pool_support_title = (support.get("pool_support") or {}).get("title", "")

    if support_title == "Thin Evidence":
        return {
            "title": "观察期",
            "line_items": [
                "后验样本仍偏薄，当前更适合把结果当作观察信号，而不是执行信号。",
            ],
        }
    if support_title == "Building Evidence":
        return {
            "title": "积累期",
            "line_items": [
                "后验样本正在积累，但还不足以支持激进切换，适合渐进式微调。",
            ],
        }
    if event_support_title == "Strong" and pool_support_title == "Strong":
        return {
            "title": "可执行期",
            "line_items": [
                "事件侧和池子侧都已有较强 completed windows 支撑，可以把结论视作更可执行的偏好。",
            ],
        }
    return {
        "title": "验证期",
        "line_items": [
            "后验样本已具备一定支撑，但仍应继续验证跨事件和跨池子的稳定性。",
        ],
    }


def _build_action_regime(
    confidence_regime: dict[str, Any] | None,
    outcome_support: dict[str, Any] | None,
    outcome_recommendation: dict[str, Any] | None,
    strategy_tilt: dict[str, Any] | None,
) -> dict[str, Any]:
    regime_title = str((confidence_regime or {}).get("title") or "")
    support_title = str((outcome_support or {}).get("title") or "")
    recommendation_title = str((outcome_recommendation or {}).get("title") or "")
    strategy_tilt_title = str((strategy_tilt or {}).get("title") or "")

    if support_title == "Thin Evidence" or strategy_tilt_title == "Wait For Maturity" or regime_title == "观察期":
        title = "观察优先"
        lines = [
            "当前更适合把后验结果当作方向参考，先看信号是否持续，不急于放大仓位或大改阈值。",
        ]
    elif regime_title == "积累期" or support_title == "Building Evidence":
        title = "渐进确认"
        lines = [
            "当前更适合沿着成熟样本一致的方向做小步调整，把后验结果用于微调排序和提醒优先级。",
        ]
    elif regime_title == "验证期":
        title = "验证确认"
        lines = [
            "当前可以把后验结果更明确地用于确认池排序，但仍要继续验证跨事件和跨池子的稳定性。",
        ]
    else:
        title = "执行确认"
        lines = [
            "当前成熟样本已能更直接服务于确认动作，可把后验偏好用于确认池优先级和动作排序。",
        ]

    if recommendation_title:
        lines.append(
            f"后验建议主线：{_display_outcome_recommendation_title(recommendation_title)}。"
        )
    if strategy_tilt_title:
        lines.append(
            f"当前策略倾向：{_display_strategy_tilt_title(strategy_tilt_title)}。"
        )
    return {"title": title, "line_items": lines}


def _assess_outcome_row_readiness(
    row: dict[str, Any],
    *,
    axis: str,
) -> dict[str, str]:
    completed_count = int(row.get("completed_count", 0) or 0)
    pending_count = int(row.get("pending_count", 0) or 0)
    avg_t3_raw = row.get("avg_t_plus_3_return")
    avg_t3 = float(avg_t3_raw) if avg_t3_raw is not None else None

    if completed_count <= 0:
        return {
            "readiness_label": "仅供观察",
            "readiness_note": "还没有完整观察窗样本，先作为观察线索，不用于排序。",
        }
    if completed_count == 1:
        return {
            "readiness_label": "轻度参考",
            "readiness_note": "完整样本还太少，只适合观察卡和候选观察，不适合直接放大权重。",
        }
    if avg_t3 is not None and avg_t3 <= 0:
        return {
            "readiness_label": "谨慎回看",
            "readiness_note": "成熟样本已出现，但 T+3 均值仍偏弱，更适合回看阈值和排序逻辑。",
        }
    if completed_count >= 5 and avg_t3 is not None and avg_t3 > 0 and pending_count <= completed_count:
        if axis == "pool":
            return {
                "readiness_label": "可用于确认排序",
                "readiness_note": "成熟样本和收益表现都较稳定，可以更明确地服务于确认池排序。",
            }
        return {
            "readiness_label": "可用于事件排序",
            "readiness_note": "成熟样本和收益表现都较稳定，可以更明确地服务于事件优先级排序。",
        }
    return {
        "readiness_label": "渐进确认",
        "readiness_note": "已有一定成熟样本，可作为确认和排序辅助，但不宜单独决定动作。",
    }


def _summarize_outcome_readiness(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    lines: list[str] = []
    ready_events = [row for row in event_type_rows if row.get("readiness_label") in {"可用于事件排序", "渐进确认"}]
    watch_events = [row for row in event_type_rows if row.get("readiness_label") in {"仅供观察", "轻度参考", "谨慎回看"}]
    ready_pools = [row for row in pool_rows if row.get("readiness_label") in {"可用于确认排序", "渐进确认"}]
    watch_pools = [row for row in pool_rows if row.get("readiness_label") in {"仅供观察", "轻度参考", "谨慎回看"}]

    if ready_events:
        event_bits = [
            f"{row['event_type']}（{row['readiness_label']}）"
            for row in ready_events[:3]
        ]
        lines.append("事件侧较可用分组：" + "、".join(event_bits) + "。")
    if watch_events:
        event_bits = [
            f"{row['event_type']}（{row['readiness_label']}）"
            for row in watch_events[:3]
        ]
        lines.append("事件侧仍偏观察分组：" + "、".join(event_bits) + "。")
    if ready_pools:
        pool_bits = [
            f"{row['pool']}（{row['readiness_label']}）"
            for row in ready_pools[:3]
        ]
        lines.append("池子侧较可用分组：" + "、".join(pool_bits) + "。")
    if watch_pools:
        pool_bits = [
            f"{row['pool']}（{row['readiness_label']}）"
            for row in watch_pools[:3]
        ]
        lines.append("池子侧仍偏观察分组：" + "、".join(pool_bits) + "。")
    if not lines:
        lines.append("当前还没有足够成熟的分组可供 readiness 判断。")
    return {"line_items": lines}


def _build_outcome_readiness_actions(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    lines: list[str] = []
    event_sort_ready = [
        row for row in event_type_rows if row.get("readiness_label") == "可用于事件排序"
    ]
    event_confirming = [
        row for row in event_type_rows if row.get("readiness_label") == "渐进确认"
    ]
    event_review = [
        row for row in event_type_rows if row.get("readiness_label") == "谨慎回看"
    ]
    event_watch = [
        row for row in event_type_rows if row.get("readiness_label") in {"仅供观察", "轻度参考"}
    ]
    pool_sort_ready = [
        row for row in pool_rows if row.get("readiness_label") == "可用于确认排序"
    ]
    pool_confirming = [
        row for row in pool_rows if row.get("readiness_label") == "渐进确认"
    ]
    pool_review = [
        row for row in pool_rows if row.get("readiness_label") == "谨慎回看"
    ]
    pool_watch = [
        row for row in pool_rows if row.get("readiness_label") in {"仅供观察", "轻度参考"}
    ]

    if event_sort_ready:
        lines.append(
            "事件排序可优先参考："
            + "、".join(_display_event_type(str(row["event_type"])) for row in event_sort_ready[:3])
            + "。"
        )
    elif event_confirming:
        lines.append(
            "事件排序可渐进参考："
            + "、".join(_display_event_type(str(row["event_type"])) for row in event_confirming[:3])
            + "，适合小步提高排序权重。"
        )
    if pool_sort_ready:
        lines.append(
            "确认池排序可优先参考："
            + "、".join(_display_pool(str(row["pool"])) for row in pool_sort_ready[:3])
            + "。"
        )
    elif pool_confirming:
        lines.append(
            "确认池排序可渐进参考："
            + "、".join(_display_pool(str(row["pool"])) for row in pool_confirming[:3])
            + "，适合先作为辅助排序信号。"
        )
    if event_review:
        lines.append(
            "需优先回看事件阈值："
            + "、".join(_display_event_type(str(row["event_type"])) for row in event_review[:3])
            + "。"
        )
    if pool_review:
        lines.append(
            "需优先回看池子逻辑："
            + "、".join(_display_pool(str(row["pool"])) for row in pool_review[:3])
            + "。"
        )
    if event_watch:
        lines.append(
            "仍以观察为主的事件："
            + "、".join(_display_event_type(str(row["event_type"])) for row in event_watch[:3])
            + "。"
        )
    if pool_watch:
        lines.append(
            "仍以观察为主的池子："
            + "、".join(_display_pool(str(row["pool"])) for row in pool_watch[:3])
            + "。"
        )
    if not lines:
        lines.append("当前还没有可转成具体排序动作的成熟分组，先继续积累 completed windows。")
    return {"line_items": lines}


def _build_outcome_headline(
    confidence_regime: dict[str, Any] | None,
    action_regime: dict[str, Any] | None,
    outcome_readiness_actions: dict[str, Any] | None,
) -> dict[str, Any]:
    regime_title = str((confidence_regime or {}).get("title") or "")
    action_title = str((action_regime or {}).get("title") or "")
    readiness_lines = list((outcome_readiness_actions or {}).get("line_items") or [])

    if readiness_lines:
        first_line = readiness_lines[0]
        if regime_title:
            summary = f"{regime_title}下优先动作：{first_line}"
        else:
            summary = first_line
    elif action_title:
        summary = f"当前以后验{action_title}为主。"
    elif regime_title:
        summary = f"当前后验处于{regime_title}。"
    else:
        summary = "当前还没有形成明确的后验动作主线。"

    details: list[str] = []
    if action_title:
        details.append(f"动作口径：{action_title}。")
    if regime_title:
        details.append(f"信心阶段：{regime_title}。")
    for line in readiness_lines[:2]:
        if line != summary:
            details.append(line)
    return {"summary": summary, "line_items": details}


def _build_outcome_priority_list(
    outcome_readiness_actions: dict[str, Any] | None,
) -> dict[str, Any]:
    readiness_lines = list((outcome_readiness_actions or {}).get("line_items") or [])
    if not readiness_lines:
        return {
            "line_items": ["P1 继续补齐 completed windows，暂不基于薄样本调整排序。"],
        }

    priorities: list[str] = []
    for index, line in enumerate(readiness_lines[:3], start=1):
        priorities.append(f"P{index} {line}")
    return {"line_items": priorities}


def _format_percent(value: Any) -> str:
    if value is None:
        return "暂无"
    try:
        return f"{float(value):+.2f}%"
    except (TypeError, ValueError):
        return str(value)


def _render_effect_row(
    row: dict[str, Any],
    *,
    axis: str,
) -> str:
    label = _display_event_type(str(row["event_type"])) if axis == "event" else _display_pool(str(row["pool"]))
    t3_sample_count = int(row.get("t_plus_3_sample_count", 0) or 0)
    t3_text = (
        f"T+3 均值 {_format_percent(row.get('avg_t_plus_3_return'))}"
        if t3_sample_count > 0
        else "T+3 样本不足"
    )
    return (
        f"{label}：完整观察窗 {row.get('completed_count', 0)} 条，"
        f"{t3_text}，"
        f"止盈 {row.get('take_profit_hits', 0)} 条，"
        f"失效 {row.get('invalidation_hits', 0)} 条，"
        f"最大浮盈 {_format_percent(row.get('avg_max_runup'))}，"
        f"最大回撤 {_format_percent(row.get('avg_max_drawdown'))}，"
        f"当前判断 {row.get('readiness_label', '-')}"
    )


def _format_metric_value(value: Any, *, suffix: str = "") -> str:
    if value is None:
        return "暂无"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if suffix == "%":
        return f"{number:.2f}%"
    return f"{number:.2f}{suffix}"


def _format_rate_value(value: Any) -> str:
    return _format_metric_value(value, suffix="%")


def _candidate_pool_count(summary: dict[str, Any]) -> int:
    return int(summary.get("candidate_pool_count", summary.get("prewatch_candidate_count", 0)) or 0)


def _candidate_optional_alert_count(summary: dict[str, Any]) -> int:
    return int(summary.get("candidate_optional_alert_count", summary.get("prewatch_light_push_count", 0)) or 0)


def _holding_management_transition_count(summary: dict[str, Any]) -> int:
    return int(summary.get("holding_management_transition_count", summary.get("exit_pool_transition_count", 0)) or 0)


def _entry_to_holding_management_rate(summary: dict[str, Any]) -> Any:
    return summary.get("entry_to_holding_management_rate", summary.get("entry_to_exit_pool_rate"))


def _window_close_evaluation_count(summary: dict[str, Any]) -> int:
    return int(summary.get("window_close_evaluation_count", summary.get("window_complete_count", 0)) or 0)


def _candidate_pool_candidates_from_summary(summary: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = summary.get("candidate_pool_candidates")
    if isinstance(candidates, list):
        return candidates
    legacy_candidates = summary.get("prewatch_candidates", [])
    return legacy_candidates if isinstance(legacy_candidates, list) else []


def _normalized_close_reason_label(reason: str) -> str:
    return {
        "profit_protection_exit": "利润保护退出",
        "hit_take_profit": "止盈位",
        "invalidation_exit": "失效退出",
        "window_close_evaluation": "窗口结算",
    }.get(str(reason or "").strip(), reason or "-")


def _normalized_close_reason_from_row(row: dict[str, Any]) -> str:
    return normalize_close_reason(
        str(row.get("close_reason") or ""),
        exit_subreason=str(row.get("exit_subreason") or ""),
        entered=bool(row.get("entered")),
    )


def _normalized_completed_exit_breakdown(summary: dict[str, Any]) -> dict[str, int]:
    breakdown = summary.get("completed_exit_breakdown") or {}
    return {
        "profit_protection_exit": int(
            breakdown.get("profit_protection_exit", breakdown.get("exit_pool", 0)) or 0
        ),
        "hit_take_profit": int(breakdown.get("hit_take_profit", 0) or 0),
        "invalidation_exit": int(
            breakdown.get("invalidation_exit", breakdown.get("hit_invalidation", 0)) or 0
        ),
        "window_close_evaluation": int(
            breakdown.get("window_close_evaluation", breakdown.get("window_complete", 0)) or 0
        ),
    }


def _format_breakdown_section(
    title: str,
    rows: list[dict[str, Any]],
) -> list[str]:
    lines = [title]
    if not rows:
        lines.append("  暂无可比较样本。")
        return lines
    for row in rows:
        lines.append(
            "  "
            f"{row.get('label', row.get('key', '-'))}："
            f"样本 {row.get('decision_count', 0)} 条，"
        f"已成交 {row.get('entered_count', 0)} 条，"
            f"止盈 {row.get('take_profit_exit_count', 0)} 条，"
            f"失效 {row.get('invalidation_exit_count', 0)} 条，"
            f"复盘窗口结算 {_window_close_evaluation_count(row)} 条，"
            f"平均真实收益 {_format_metric_value(row.get('avg_realized_return'), suffix='%')}，"
            f"胜率 {_format_rate_value(row.get('win_rate'))}"
        )
    return lines


def _format_comparison_cell(value: Any, *, value_type: str = "metric") -> str:
    if value_type == "rate":
        return _format_rate_value(value)
    if value_type == "percent":
        return _format_metric_value(value, suffix="%")
    return _format_metric_value(value)


def _format_dual_window_comparison_lines(comparison_windows: dict[str, Any]) -> list[str]:
    primary = comparison_windows.get("primary") or {}
    baseline = comparison_windows.get("baseline") or {}
    summary = comparison_windows.get("summary") or {}
    primary_label = f"{primary.get('label', '近30天')}（{primary.get('role_label', '调参')}）"
    baseline_label = f"{baseline.get('label', '近90天')}（{baseline.get('role_label', '基准')}）"
    lines = ["核心指标摘要："]
    lines.append(f"| 指标 | {primary_label} | {baseline_label} | 状态 |")
    lines.append("| --- | --- | --- | --- |")
    for row in summary.get("metrics") or []:
        lines.append(
            "| "
            f"{row.get('label', '-')} | "
            f"{_format_comparison_cell(row.get('primary_value'), value_type=str(row.get('value_type') or 'metric'))} | "
            f"{_format_comparison_cell(row.get('baseline_value'), value_type=str(row.get('value_type') or 'metric'))} | "
            f"{row.get('status', '样本不足')} |"
        )
    if not (summary.get("metrics") or []):
        lines.append("| 暂无可比较指标 | 暂无 | 暂无 | 样本不足 |")
    for line in summary.get("summary_lines") or []:
        lines.append(f"状态判断：{line}")
    return lines


def _format_simulation_snapshot_lines(snapshot: dict[str, Any]) -> list[str]:
    role_label = str(snapshot.get("role_label") or "").strip()
    label = str(snapshot.get("label") or "-")
    if role_label:
        label = f"{label}（{role_label}）"
    funnel = snapshot.get("simulation_funnel_summary") or {}
    lines = [f"  {label}："]
    if int(funnel.get("promoted_confirmation_count", 0) or 0) <= 0:
        lines.append("    当前窗口内还没有形成可计算的模拟闭环样本。")
        return lines
    lines.append(
        "    "
        f"候选 {_candidate_pool_count(funnel)} 条 -> "
        f"观察卡 {_candidate_optional_alert_count(funnel)} 条 -> "
        f"确认机会 {funnel.get('promoted_confirmation_count', 0)} 条"
    )
    lines.append(
        "    "
        f"模拟成交 {funnel.get('simulated_entry_count', 0)} 条 "
        f"（确认机会 -> 模拟成交 {_format_rate_value(funnel.get('confirmation_to_entry_rate'))}）"
    )
    lines.append(
        "    "
        f"进入持仓管理退出 {_holding_management_transition_count(funnel)} 条 / "
        f"模拟退出完成 {funnel.get('simulated_completed_exit_count', 0)} 条"
    )
    lines.append(
        "    "
        f"另有未成交 {funnel.get('simulated_pending_entry_count', 0)} 条，"
        f"已成交未结束 {funnel.get('simulated_open_count', 0)} 条。"
    )
    return lines


def _format_path_quality_snapshot_lines(snapshot: dict[str, Any]) -> list[str]:
    role_label = str(snapshot.get("role_label") or "").strip()
    label = str(snapshot.get("label") or "-")
    if role_label:
        label = f"{label}（{role_label}）"
    summary = snapshot.get("path_quality_summary") or {}
    lines = [f"  {label}："]
    lines.append(
        "    "
        f"路径极值：平均最大浮盈 {_format_metric_value(summary.get('avg_max_runup'), suffix='%')} "
        f"（样本 {int(summary.get('max_runup_sample_count', 0) or 0)} 条） / "
        f"平均最大回撤 {_format_metric_value(summary.get('avg_max_drawdown'), suffix='%')} "
        f"（样本 {int(summary.get('max_drawdown_sample_count', 0) or 0)} 条）"
    )
    checkpoint_rows = list(summary.get("checkpoints") or [])
    if checkpoint_rows:
        lines.append("    固定观察日：")
        for row in checkpoint_rows:
            lines.append(
                "      "
                f"{row.get('label', '-')}：样本 {int(row.get('sample_count', 0) or 0)} 条，"
                f"平均 {_format_metric_value(row.get('avg_return'), suffix='%')}，"
                f"正收益占比 {_format_rate_value(row.get('positive_rate'))}"
            )
    else:
        lines.append("    固定观察日：暂无已成熟样本。")
    best_runup_samples = list(summary.get("best_runup_samples") or [])
    if best_runup_samples:
        lines.append("    最大浮盈 Top3：")
        for row in best_runup_samples:
            lines.append(
                "      "
                f"{row.get('symbol', '-')}（{row.get('action_display', row.get('action', '-'))}）"
                f" | 最大浮盈 {_format_metric_value(row.get('max_runup'), suffix='%')}"
            )
    worst_drawdown_samples = list(summary.get("worst_drawdown_samples") or [])
    if worst_drawdown_samples:
        lines.append("    最大回撤 Top3：")
        for row in worst_drawdown_samples:
            lines.append(
                "      "
                f"{row.get('symbol', '-')}（{row.get('action_display', row.get('action', '-'))}）"
                f" | 最大回撤 {_format_metric_value(row.get('max_drawdown'), suffix='%')}"
            )
    if int(summary.get("deduped_merge_count", 0) or 0) > 0:
        lines.append(
            "    "
            f"补充：已按代表样本合并 {int(summary.get('deduped_merge_count', 0) or 0)} 条重复路径记录。"
        )
    lines.append(f"    状态判断：{summary.get('observation_line') or '路径质量样本仍不足。'}")
    lines.append("    说明：截至目前路径统计，不等于完整持有周期结果。")
    return lines


def _comparison_status_for_counts(primary_value: Any, baseline_value: Any) -> str:
    if primary_value is None or baseline_value is None:
        return "样本不足"
    try:
        primary = float(primary_value)
        baseline = float(baseline_value)
    except (TypeError, ValueError):
        return "样本不足"
    if baseline <= 0:
        return "样本不足"
    tolerance = max(baseline * 0.1, 1.0)
    if abs(primary - baseline) <= tolerance:
        return "基本持平"
    return "样本增多" if primary > baseline else "样本减少"


def _comparison_status_for_quality(primary_value: Any, baseline_value: Any, *, higher_is_better: bool = True) -> str:
    if primary_value is None or baseline_value is None:
        return "样本不足"
    try:
        primary = float(primary_value)
        baseline = float(baseline_value)
    except (TypeError, ValueError):
        return "样本不足"
    if abs(primary - baseline) <= 0.05:
        return "基本持平"
    if higher_is_better:
        return "短期改善" if primary > baseline else "短期转弱"
    return "短期改善" if primary < baseline else "短期转弱"


def _build_review_glossary(review: dict[str, Any]) -> list[str]:
    terms = []
    if review.get("comparison_windows"):
        terms.extend(
            [
                "完整模拟闭环：看一笔信号有没有走完整个模拟交易流程。",
                "路径质量 / 浮盈浮亏摘要：看已模拟成交样本在持有过程中的利润空间、回撤和延续性。",
                "三池漏斗：看观察、确认、持仓管理这条筛选链路顺不顺。",
                "模拟退出完成：包含止盈退出、失效退出、窗口结算，不是只指止盈。",
            ]
        )
    return terms


def _build_core_metric_rows(review: dict[str, Any]) -> list[dict[str, Any]]:
    comparison = review.get("comparison_windows") or {}
    primary = comparison.get("primary") or {}
    baseline = comparison.get("baseline") or {}
    primary_overview = primary.get("overview") or review.get("overview") or {}
    baseline_overview = baseline.get("overview") or {}
    primary_execution = primary.get("execution_quality") or review.get("execution_quality") or {}
    baseline_execution = baseline.get("execution_quality") or {}
    primary_sim = primary.get("simulation_funnel_summary") or review.get("simulation_funnel_summary") or {}
    baseline_sim = baseline.get("simulation_funnel_summary") or {}
    if not comparison:
        return []
    return [
        {
            "label": "决策总数",
            "primary": primary_overview.get("decision_count"),
            "baseline": baseline_overview.get("decision_count"),
            "status": _comparison_status_for_counts(primary_overview.get("decision_count"), baseline_overview.get("decision_count")),
            "type": "count",
        },
        {
            "label": "模拟成交率",
            "primary": primary_execution.get("entry_hit_rate"),
            "baseline": baseline_execution.get("entry_hit_rate"),
            "status": _comparison_status_for_quality(primary_execution.get("entry_hit_rate"), baseline_execution.get("entry_hit_rate")),
            "type": "rate",
        },
        {
            "label": "模拟退出完成率",
            "primary": primary_sim.get("entry_to_completed_exit_rate"),
            "baseline": baseline_sim.get("entry_to_completed_exit_rate"),
            "status": _comparison_status_for_quality(primary_sim.get("entry_to_completed_exit_rate"), baseline_sim.get("entry_to_completed_exit_rate")),
            "type": "rate",
        },
        {
            "label": "已完成闭环样本数",
            "primary": primary_sim.get("simulated_completed_exit_count"),
            "baseline": baseline_sim.get("simulated_completed_exit_count"),
            "status": _comparison_status_for_counts(primary_sim.get("simulated_completed_exit_count"), baseline_sim.get("simulated_completed_exit_count")),
            "type": "count",
        },
        {
            "label": "平均模拟已实现收益",
            "primary": primary_overview.get("avg_realized_return"),
            "baseline": baseline_overview.get("avg_realized_return"),
            "status": _comparison_status_for_quality(primary_overview.get("avg_realized_return"), baseline_overview.get("avg_realized_return")),
            "type": "percent",
        },
        {
            "label": "模拟已实现胜率",
            "primary": primary_overview.get("win_rate"),
            "baseline": baseline_overview.get("win_rate"),
            "status": _comparison_status_for_quality(primary_overview.get("win_rate"), baseline_overview.get("win_rate")),
            "type": "rate",
        },
    ]


def _format_core_metric_summary_lines(review: dict[str, Any]) -> list[str]:
    rows = _build_core_metric_rows(review)
    if not rows:
        return []
    lines = ["核心指标摘要：", "定位：看近30天相对90天有没有偏离基准。", "| 指标 | 近 30 天（调参） | 近 90 天（基准） | 状态 |", "| --- | --- | --- | --- |"]
    for row in rows:
        lines.append(
            "| "
            f"{row['label']} | "
            f"{_format_comparison_cell(row['primary'], value_type=row['type'])} | "
            f"{_format_comparison_cell(row['baseline'], value_type=row['type'])} | "
            f"{row['status']} |"
        )
    lines.append("说明：状态表示近30天相对近90天的变化，不是绝对好坏结论。")
    return lines


def _summarize_simulation_bottleneck(primary: dict[str, Any], baseline: dict[str, Any]) -> str:
    primary_funnel = primary.get("simulation_funnel_summary") or {}
    baseline_funnel = baseline.get("simulation_funnel_summary") or {}
    rate_rows = [
        ("确认机会 -> 模拟成交", primary_funnel.get("confirmation_to_entry_rate"), baseline_funnel.get("confirmation_to_entry_rate")),
        (
            "模拟成交 -> 持仓管理退出",
            _entry_to_holding_management_rate(primary_funnel),
            _entry_to_holding_management_rate(baseline_funnel),
        ),
        ("模拟成交 -> 模拟退出完成", primary_funnel.get("entry_to_completed_exit_rate"), baseline_funnel.get("entry_to_completed_exit_rate")),
    ]
    available = []
    for label, primary_value, baseline_value in rate_rows:
        if primary_value is None:
            continue
        baseline_gap = None
        if baseline_value is not None:
            try:
                baseline_gap = float(primary_value) - float(baseline_value)
            except (TypeError, ValueError):
                baseline_gap = None
        available.append((label, float(primary_value), baseline_gap))
    if not available:
        return "当前样本还不足以判断主要瓶颈。"
    available.sort(key=lambda item: (item[1], item[2] if item[2] is not None else 0.0))
    label, primary_rate, baseline_gap = available[0]
    if baseline_gap is not None and baseline_gap < -0.05:
        return f"当前瓶颈：{label}转化率低于90天基准。"
    return f"当前瓶颈：{label}转化率是当前闭环里最弱的一段。"


def _format_main_simulation_lines(review: dict[str, Any]) -> list[str]:
    comparison = review.get("comparison_windows") or {}
    primary = comparison.get("primary") or {}
    baseline = comparison.get("baseline") or {}
    if not comparison:
        return []
    primary_funnel = primary.get("simulation_funnel_summary") or {}
    baseline_funnel = baseline.get("simulation_funnel_summary") or {}
    lines = ["完整模拟闭环：", "定位：看机会从观察到模拟退出，主要卡在哪一段。"]
    lines.append(
        "近30天（调参）："
        f" 确认机会 -> 模拟成交 {_format_rate_value(primary_funnel.get('confirmation_to_entry_rate'))}"
        f" / 模拟成交 -> 持仓管理退出 {_format_rate_value(_entry_to_holding_management_rate(primary_funnel))}"
        f" / 模拟成交 -> 模拟退出完成 {_format_rate_value(primary_funnel.get('entry_to_completed_exit_rate'))}"
    )
    lines.append(
        "近90天（基准）："
        f" 确认机会 -> 模拟成交 {_format_rate_value(baseline_funnel.get('confirmation_to_entry_rate'))}"
        f" / 模拟成交 -> 持仓管理退出 {_format_rate_value(_entry_to_holding_management_rate(baseline_funnel))}"
        f" / 模拟成交 -> 模拟退出完成 {_format_rate_value(baseline_funnel.get('entry_to_completed_exit_rate'))}"
    )
    completed_exit_breakdown = _normalized_completed_exit_breakdown(primary_funnel)
    parts = []
    for reason in (
        "profit_protection_exit",
        "hit_take_profit",
        "invalidation_exit",
        "window_close_evaluation",
    ):
        count = int(completed_exit_breakdown.get(reason, 0) or 0)
        if count > 0:
            parts.append(f"{_normalized_close_reason_label(reason)} {count}")
    lines.append(_summarize_simulation_bottleneck(primary, baseline))
    if parts:
        lines.append(f"说明：近30天模拟退出完成里包含 {' / '.join(parts)}。")
    else:
        lines.append("说明：模拟退出完成包含止盈退出、失效退出、窗口结算，不是只指止盈。")
    anomaly_count = int(primary_funnel.get("confirmation_to_entry_timing_anomaly_count", 0) or 0)
    if anomaly_count > 0:
        lines.append(f"补充：已跳过 {anomaly_count} 条成交时间早于决策时间的异常样本。")
    return lines


def _format_main_path_quality_lines(review: dict[str, Any]) -> list[str]:
    comparison = review.get("comparison_windows") or {}
    if comparison:
        primary = comparison.get("primary") or {}
        baseline = comparison.get("baseline") or {}
        lines = ["路径质量 / 浮盈浮亏摘要：", "定位：看已模拟成交样本在持有过程中的利润空间、回撤和延续性，包含已退出与未退出样本。"]
        lines.extend(_format_path_quality_snapshot_lines(primary))
        lines.extend(_format_path_quality_snapshot_lines(baseline))
        return lines
    return [
        "路径质量 / 浮盈浮亏摘要：",
        "定位：看已模拟成交样本在持有过程中的利润空间、回撤和延续性，包含已退出与未退出样本。",
        *_format_path_quality_snapshot_lines(
            {
                "label": "当前窗口",
                "role_label": "",
                "path_quality_summary": review.get("path_quality_summary") or {},
            }
        ),
    ]


def _build_action_conclusion(review: dict[str, Any]) -> tuple[str, list[str], str]:
    comparison = review.get("comparison_windows") or {}
    primary_gate = ((comparison.get("primary") or {}).get("sample_gate") or {})
    baseline_gate = ((comparison.get("baseline") or {}).get("sample_gate") or {})
    recommendations = list(review.get("recommendations") or [])
    recommendation_details = list(review.get("recommendation_details") or [])
    if not primary_gate.get("sufficient", True) or not baseline_gate.get("sufficient", True):
        reason = recommendations[0] if recommendations else "当前样本量不足，先观察。"
        return ("样本量不足，先观察，不建议直接调整参数", [], reason)
    checklist = _build_recommendation_parameter_checklist(recommendation_details, limit=3)
    has_adjustable = bool(checklist)
    current = "可开始小步调参" if has_adjustable else "优先先排查"
    reason_lines = recommendations[:2] if recommendations else ["当前样本还不足以形成明确建议，继续积累完整样本。"]
    return (current, checklist, "；".join(reason_lines))


def _format_executive_summary_lines(review: dict[str, Any]) -> list[str]:
    comparison = review.get("comparison_windows") or {}
    summary = comparison.get("summary") or {}
    current_conclusion, _, why = _build_action_conclusion(review)
    lines = ["执行摘要："]
    for line in summary.get("summary_lines") or []:
        lines.append(f"  近30天相对90天：{line}")
    primary_gate = ((comparison.get("primary") or {}).get("sample_gate") or {})
    baseline_gate = ((comparison.get("baseline") or {}).get("sample_gate") or {})
    if primary_gate:
        if primary_gate.get("sufficient"):
            lines.append("  样本状态：30d 样本已达到调参观察门槛。")
        else:
            lines.append(
                f"  样本状态：30d 样本不足（{int(primary_gate.get('decision_count', 0) or 0)} 条），先观察。"
            )
    if baseline_gate and not baseline_gate.get("sufficient", True):
        lines.append(
            f"  中期基准：90d 样本仍偏薄（{int(baseline_gate.get('decision_count', 0) or 0)} 条）。"
        )
    lines.append(f"  当前问题：{_summarize_simulation_bottleneck(comparison.get('primary') or {}, comparison.get('baseline') or {})}")
    path_summary = ((comparison.get("primary") or {}).get("path_quality_summary") or review.get("path_quality_summary") or {})
    if path_summary.get("observation_line"):
        lines.append(f"  路径质量：{path_summary.get('observation_line')}")
    lines.append(f"  当前建议：{current_conclusion}")
    if why:
        lines.append(f"  说明：{why}")
    return lines


def _format_decision_detail_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = ["决策效果明细（最近 20 条）："]
    if not rows:
        lines.append("暂无可展示明细。")
        return lines
    lines.append("| 时间 | 标的 | 事件类型 | 状态/动作 | 进场价 | 退出价 | 真实收益 | 持有天数 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        created_at = format_beijing_minute(row.get("created_at"))
        symbol = str(row.get("symbol") or "-")
        event_type = str(row.get("event_type_display") or row.get("event_type") or "-")
        action = str(row.get("action_display") or row.get("action") or "-")
        status = str(row.get("status_label") or "-")
        status_with_action = f"{status}（{action}）"
        entry_price = _format_metric_value(row.get("entry_price"))
        exit_price = _format_exit_price_cell(row)
        realized_return = _format_metric_value(row.get("realized_return"), suffix="%")
        holding_days = _format_metric_value(row.get("holding_days"), suffix=" 天")
        lines.append(
            f"| {created_at} | {symbol} | {event_type} | {status_with_action} | {entry_price} | {exit_price} | {realized_return} | {holding_days} |"
        )
    return lines


def _format_ranked_decision_table(
    title: str,
    rows: list[dict[str, Any]],
    *,
    metric_field: str = "realized_return",
    metric_label: str = "真实收益",
) -> list[str]:
    lines = [title]
    if not rows:
        lines.append("暂无可展示样本。")
        return lines
    lines.append(f"| 时间 | 标的 | 事件类型 | 状态/动作 | 真实退出 | 进场价 | 退出价 | {metric_label} | 持有天数 |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for row in rows:
        created_at = format_beijing_minute(row.get("created_at"))
        symbol = str(row.get("symbol") or "-")
        event_type = str(row.get("event_type_display") or row.get("event_type") or "-")
        action = str(row.get("action_display") or row.get("action") or "-")
        status = str(row.get("status_label") or "-")
        status_with_action = f"{status}（{action}）"
        real_exit = str(row.get("real_exit_label") or "-")
        entry_price = _format_metric_value(row.get("entry_price"))
        exit_price = _format_exit_price_cell(row)
        metric_value = _format_metric_value(row.get(metric_field), suffix="%")
        holding_days = _format_metric_value(row.get("holding_days"), suffix=" 天")
        lines.append(
            f"| {created_at} | {symbol} | {event_type} | {status_with_action} | {real_exit} | {entry_price} | {exit_price} | {metric_value} | {holding_days} |"
        )
    return lines


def _summarize_outcome_maturity(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    lines: list[str] = []
    combined = coverage.get("combined") or {}
    if combined:
        completed_count = int(combined.get("completed_count", 0) or 0)
        pending_count = int(combined.get("pending_count", 0) or 0)
        outcome_count = int(combined.get("outcome_count", 0) or 0)
    else:
        outcome_count = sum(int(row.get("outcome_count", 0) or 0) for row in event_type_rows)
        pending_count = sum(int(row.get("pending_count", 0) or 0) for row in event_type_rows)
        completed_count = sum(int(row.get("completed_count", 0) or 0) for row in event_type_rows)
    if outcome_count > 0:
        lines.append(
            f"Completed windows: {completed_count} / rolling lookahead: {pending_count} / total written outcomes: {outcome_count}."
        )
    completed_event_rows = [row for row in event_type_rows if row.get("completed_count", 0) > 0]
    if completed_event_rows:
        strongest_completed_event = max(
            completed_event_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("completed_count", 0),
                row.get("positive_t3_count", 0),
            ),
        )
        lines.append(
            "Strongest completed cohort: "
            f"{strongest_completed_event['event_type']} avg_t3={strongest_completed_event['avg_t_plus_3_return']} "
            f"across {strongest_completed_event['completed_count']} completed windows."
        )
    completed_pool_rows = [row for row in pool_rows if row.get("completed_count", 0) > 0]
    if completed_pool_rows:
        strongest_completed_pool = max(
            completed_pool_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("completed_count", 0),
                row.get("positive_t3_count", 0),
            ),
        )
        lines.append(
            "Best completed pool: "
            f"{strongest_completed_pool['pool']} avg_t3={strongest_completed_pool['avg_t_plus_3_return']} "
            f"across {strongest_completed_pool['completed_count']} completed windows."
        )
    if not lines:
        lines.append("No completed outcome windows yet.")
    return {"line_items": lines}


def _format_exit_price_cell(row: dict[str, Any]) -> str:
    if row.get("exit_price") is not None:
        return _format_metric_value(row.get("exit_price"))
    entered = bool(row.get("entered"))
    close_reason = str(row.get("close_reason") or "").strip()
    if not entered:
        if close_reason == "not_entered":
            return "未成交"
        if close_reason == "insufficient_lookahead":
            return "待更多后验"
        return "暂无"
    if close_reason == "insufficient_lookahead":
        return "仍在持有"
    if close_reason in {"exit_pool", "hit_take_profit", "hit_invalidation", "window_complete"}:
        return "待结算"
    return "暂无"


def _summarize_completed_cohorts(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    lines: list[str] = []
    completed_event_rows = [row for row in event_type_rows if row.get("completed_count", 0) > 0]
    completed_pool_rows = [row for row in pool_rows if row.get("completed_count", 0) > 0]
    if completed_event_rows:
        ranked_event_rows = sorted(
            completed_event_rows,
            key=lambda row: (
                row.get("pending_count", 0) == 0,
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("completed_count", 0),
                row.get("positive_t3_count", 0),
            ),
            reverse=True,
        )
        best_event = ranked_event_rows[0]
        lines.append(
            f"Best completed event cohort: {best_event['event_type']} avg_t3={best_event['avg_t_plus_3_return']} across {best_event['completed_count']} completed windows."
        )
        if len(ranked_event_rows) > 1:
            runner_up = ranked_event_rows[1]
            lines.append(
                f"Next completed event cohort: {runner_up['event_type']} avg_t3={runner_up['avg_t_plus_3_return']} across {runner_up['completed_count']} completed windows."
            )
    if completed_pool_rows:
        ranked_pool_rows = sorted(
            completed_pool_rows,
            key=lambda row: (
                row.get("pending_count", 0) == 0,
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("completed_count", 0),
                row.get("positive_t3_count", 0),
            ),
            reverse=True,
        )
        best_pool = ranked_pool_rows[0]
        lines.append(
            f"Best completed pool cohort: {best_pool['pool']} avg_t3={best_pool['avg_t_plus_3_return']} across {best_pool['completed_count']} completed windows."
        )
    if not lines:
        lines.append("No completed cohorts are mature enough to compare yet.")
    return {"line_items": lines}


def _assess_outcome_support(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    combined = coverage.get("combined") or {}
    completed_count = int(combined.get("completed_count", 0) or 0)
    pending_count = int(combined.get("pending_count", 0) or 0)
    if not combined:
        completed_count = sum(int(row.get("completed_count", 0) or 0) for row in event_type_rows)
        pending_count = sum(int(row.get("pending_count", 0) or 0) for row in event_type_rows)
    completed_event_rows = [row for row in event_type_rows if int(row.get("completed_count", 0) or 0) > 0]
    completed_pool_rows = [row for row in pool_rows if int(row.get("completed_count", 0) or 0) > 0]
    diversified_event_count = sum(1 for row in completed_event_rows if int(row.get("completed_count", 0) or 0) >= 2)
    diversified_pool_count = sum(1 for row in completed_pool_rows if int(row.get("completed_count", 0) or 0) >= 2)
    total_event_completed = sum(int(row.get("completed_count", 0) or 0) for row in completed_event_rows)
    total_pool_completed = sum(int(row.get("completed_count", 0) or 0) for row in completed_pool_rows)

    def assess_axis(*, completed_total: int, diversified_count: int, axis_label: str) -> tuple[str, list[str]]:
        axis_lines: list[str] = []
        if completed_total >= 8 and diversified_count >= 2:
            axis_title = "Strong"
            axis_lines.append(
                f"{axis_label} side already has {completed_total} completed windows across {diversified_count} diversified cohorts."
            )
        elif completed_total >= 3:
            axis_title = "Building"
            axis_lines.append(
                f"{axis_label} side has {completed_total} completed windows, but they are concentrated in {diversified_count} diversified cohorts."
            )
        else:
            axis_title = "Thin"
            axis_lines.append(
                f"{axis_label} side only has {completed_total} completed windows, so its signal should still be treated as tentative."
            )
        return axis_title, axis_lines

    event_support_title, event_support_lines = assess_axis(
        completed_total=total_event_completed,
        diversified_count=diversified_event_count,
        axis_label="Event",
    )
    pool_support_title, pool_support_lines = assess_axis(
        completed_total=total_pool_completed,
        diversified_count=diversified_pool_count,
        axis_label="Pool",
    )

    title = "Thin Evidence"
    lines: list[str] = []
    if completed_count >= 8 and diversified_event_count >= 2 and diversified_pool_count >= 1:
        title = "Strong Evidence"
        lines.append(
            f"Completed windows already reach {completed_count}, with {diversified_event_count} event cohorts and {diversified_pool_count} pool cohorts carrying at least 2 mature samples."
        )
    elif completed_count >= 3:
        title = "Building Evidence"
        lines.append(
            f"Completed windows have reached {completed_count}, but the evidence is still concentrated in {diversified_event_count} event cohorts and {diversified_pool_count} pool cohorts."
        )
    else:
        lines.append(
            f"Only {completed_count} completed windows are available, so current strategy conclusions should still be treated as provisional."
        )

    if pending_count > completed_count:
        lines.append(
            f"Rolling lookahead samples ({pending_count}) still exceed completed windows ({completed_count}), so avoid over-rotating on the latest tilt."
        )
    elif pending_count > 0:
        lines.append(
            f"There are still {pending_count} rolling samples waiting for completion, so let the next few windows settle before making larger policy changes."
        )

    return {
        "title": title,
        "display_title": _display_outcome_support_title(title),
        "line_items": lines,
        "event_support": {
            "title": event_support_title,
            "display_title": _display_outcome_support_title(event_support_title),
            "line_items": event_support_lines,
        },
        "pool_support": {
            "title": pool_support_title,
            "display_title": _display_outcome_support_title(pool_support_title),
            "line_items": pool_support_lines,
        },
    }


def _validate_strategy_outcomes(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    confirmations: list[str] = []

    def validate_rows(rows: list[dict[str, Any]], *, label: str, key_field: str) -> None:
        for row in rows:
            key = row.get(key_field, "-")
            decision_count = int(row.get("decision_count", 0) or 0)
            outcome_count = int(row.get("outcome_count", 0) or 0)
            pending_count = int(row.get("pending_count", 0) or 0)
            completed_count = int(row.get("completed_count", 0) or 0)
            t3_positive = int(row.get("positive_t3_count", 0) or 0)
            t3_samples = int(row.get("t_plus_3_sample_count", 0) or 0)
            tp_hits = int(row.get("take_profit_hits", 0) or 0)
            invalidation_hits = int(row.get("invalidation_hits", 0) or 0)
            if decision_count < outcome_count:
                warnings.append(f"{label} {key}: decision_count < outcome_count.")
            if outcome_count != completed_count + pending_count:
                warnings.append(f"{label} {key}: outcome_count != completed_count + pending_count.")
            if t3_positive > t3_samples:
                warnings.append(f"{label} {key}: positive_t3_count > t_plus_3_sample_count.")
            if tp_hits > outcome_count or invalidation_hits > outcome_count:
                warnings.append(f"{label} {key}: hit counts exceed outcome_count.")

    validate_rows(event_type_rows, label="event", key_field="event_type")
    validate_rows(pool_rows, label="pool", key_field="pool")

    displayed_event_summary = {
        "decision_count": sum(int(row.get("decision_count", 0) or 0) for row in event_type_rows),
        "outcome_count": sum(int(row.get("outcome_count", 0) or 0) for row in event_type_rows),
        "pending_count": sum(int(row.get("pending_count", 0) or 0) for row in event_type_rows),
        "completed_count": sum(int(row.get("completed_count", 0) or 0) for row in event_type_rows),
    }
    displayed_pool_summary = {
        "decision_count": sum(int(row.get("decision_count", 0) or 0) for row in pool_rows),
        "outcome_count": sum(int(row.get("outcome_count", 0) or 0) for row in pool_rows),
        "pending_count": sum(int(row.get("pending_count", 0) or 0) for row in pool_rows),
        "completed_count": sum(int(row.get("completed_count", 0) or 0) for row in pool_rows),
    }
    combined = coverage.get("combined") or {}
    if combined:
        combined_decisions = int(combined.get("decision_count", 0) or 0)
        combined_outcomes = int(combined.get("outcome_count", 0) or 0)
        if displayed_pool_summary["decision_count"] == combined_decisions and displayed_pool_summary["outcome_count"] == combined_outcomes:
            confirmations.append(
                f"Pool cohorts reconcile with total coverage: decisions {combined_decisions}, outcomes {combined_outcomes}."
            )
        else:
            warnings.append(
                "Displayed pool cohorts do not reconcile with total coverage: "
                f"decisions {displayed_pool_summary['decision_count']}/{combined_decisions}, "
                f"outcomes {displayed_pool_summary['outcome_count']}/{combined_outcomes}."
            )
        if displayed_event_summary["decision_count"] == combined_decisions and displayed_event_summary["outcome_count"] == combined_outcomes:
            confirmations.append(
                f"Event cohorts fully cover total sample: decisions {combined_decisions}, outcomes {combined_outcomes}."
            )
        else:
            confirmations.append(
                "Displayed event cohorts are a top-slice view: "
                f"decisions {displayed_event_summary['decision_count']}/{combined_decisions}, "
                f"outcomes {displayed_event_summary['outcome_count']}/{combined_outcomes}."
            )
    if not warnings:
        confirmations.insert(0, "Invariant check passed for all displayed cohorts.")
    return {
        "status": "warning" if warnings else "ok",
        "line_items": confirmations + warnings,
    }


def _recommend_strategy_outcomes(
    event_type_rows: list[dict[str, Any]],
    pool_rows: list[dict[str, Any]],
    *,
    outcome_support: dict[str, Any] | None = None,
    confidence_regime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mature_event_rows = [
        row for row in event_type_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    mature_pool_rows = [
        row for row in pool_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    if not mature_event_rows and not mature_pool_rows:
        return {
            "title": "Expand Outcome Coverage",
            "display_title": _display_outcome_recommendation_title("Expand Outcome Coverage"),
            "line_items": ["No mature outcomes yet; keep backfilling and wait for more bars before trusting any strategy tilt."],
        }

    lines: list[str] = []
    title = "Promote Mature Bias"
    strongest_event = None
    if mature_event_rows:
        strongest_event = max(
            mature_event_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )
        if float(strongest_event.get("avg_t_plus_3_return") or 0.0) > 0:
            lines.append(
                f"Lean more on {strongest_event['event_type']} while it keeps the best average T+3 return ({strongest_event['avg_t_plus_3_return']})."
            )
    if mature_pool_rows:
        strongest_pool = max(
            mature_pool_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )
        if float(strongest_pool.get("avg_t_plus_3_return") or 0.0) > 0:
            lines.append(
                f"Keep {strongest_pool['pool']} as the cleaner downstream pool; its average T+3 return is {strongest_pool['avg_t_plus_3_return']}."
            )
    if pool_rows:
        most_pending_pool = max(pool_rows, key=lambda row: (row.get("pending_count", 0), row.get("decision_count", 0)))
        if most_pending_pool.get("pending_count", 0) > 0:
            title = "Balance Signal And Maturity"
            lines.append(
                f"Do not overfit {most_pending_pool['pool']} yet; it still has {most_pending_pool['pending_count']} outcomes waiting for more bars."
            )
    weak_event_rows = [
        row for row in mature_event_rows if row.get("avg_t_plus_3_return") is not None and float(row["avg_t_plus_3_return"]) < 0
    ]
    if weak_event_rows:
        weakest_event = min(weak_event_rows, key=lambda row: float(row["avg_t_plus_3_return"]))
        lines.append(
            f"Review thresholds or ranking for {weakest_event['event_type']}; its average T+3 return is currently {weakest_event['avg_t_plus_3_return']}."
        )
    if not lines:
        lines.append("Current outcome sample is mixed; keep collecting more mature outcomes before shifting the policy.")
    support_title = (outcome_support or {}).get("title", "")
    if support_title == "Thin Evidence":
        title = "Provisional Bias"
        lines.insert(0, "Outcome support is still thin, so any event or pool preference should be treated as provisional.")
    elif support_title == "Building Evidence":
        title = "Measured Bias"
        lines.insert(0, "Outcome evidence is still building; lean gradually rather than making a full policy rotation.")
    regime_title = (confidence_regime or {}).get("title", "")
    if regime_title == "观察期":
        lines.insert(0, "当前更适合候选观察和观察卡，而不是把后验结果直接翻译成强执行动作。")
    elif regime_title == "积累期":
        lines.insert(0, "当前更适合渐进式加权和小步调整，而不是一次性切换整套策略偏好。")
    elif regime_title == "验证期":
        lines.insert(0, "当前可以开始把后验结果用于确认优先级，但仍需持续验证跨场景稳定性。")
    elif regime_title == "可执行期":
        lines.insert(0, "当前后验结果已更接近可执行阶段，可以更明确地服务于确认和排序动作。")
    return {
        "title": title,
        "display_title": _display_outcome_recommendation_title(title),
        "line_items": lines,
    }


def _recommend_strategy_tilt(
    event_type_rows: list[dict[str, Any]],
    alert_rows: list[dict[str, Any]],
    outcome_event_type_rows: list[dict[str, Any]],
    outcome_pool_rows: list[dict[str, Any]],
    *,
    outcome_support: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_event = None
    if event_type_rows:
        active_event = max(
            event_type_rows,
            key=lambda row: (
                row.get("high_priority_count", 0),
                row.get("card_count", 0),
                float(row.get("avg_final_score") or 0.0),
            ),
        )
    strongest_event = None
    mature_event_rows = [
        row for row in outcome_event_type_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    if mature_event_rows:
        strongest_event = max(
            mature_event_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )
    strongest_pool = None
    mature_pool_rows = [
        row for row in outcome_pool_rows if row.get("outcome_count", 0) > 0 and row.get("avg_t_plus_3_return") is not None
    ]
    if mature_pool_rows:
        strongest_pool = max(
            mature_pool_rows,
            key=lambda row: (
                float(row.get("avg_t_plus_3_return") or float("-inf")),
                row.get("positive_t3_count", 0),
                row.get("outcome_count", 0),
            ),
        )

    total_high_priority = sum(int(row.get("high_priority_alerts", 0)) for row in alert_rows)
    total_sent_high_priority = sum(int(row.get("sent_high_priority_alerts", 0)) for row in alert_rows)
    title = "Stay Balanced"
    lines: list[str] = []

    if strongest_event and active_event:
        if (
            strongest_event["event_type"] == active_event["event_type"]
            and float(strongest_event.get("avg_t_plus_3_return") or 0.0) > 0
            and total_sent_high_priority > 0
        ):
            title = "Lean Offensive"
            lines.append(
                f"Current live flow is concentrated in {active_event['event_type']}, and that also has the strongest mature T+3 outcome ({strongest_event['avg_t_plus_3_return']})."
            )
        elif strongest_event["event_type"] != active_event["event_type"]:
            title = "Prefer Selectivity"
            lines.append(
                f"Current live flow leans toward {active_event['event_type']}, but mature outcomes still favor {strongest_event['event_type']} ({strongest_event['avg_t_plus_3_return']})."
            )

    if strongest_pool:
        if strongest_pool["pool"] == "confirmation" and float(strongest_pool.get("avg_t_plus_3_return") or 0.0) > 0:
            lines.append("Use confirmation decisions as the primary execution lane while outcome quality remains strongest there.")
        elif _normalize_decision_pool(str(strongest_pool["pool"])) == "candidate_pool":
            lines.append("Candidate-pool outcomes are currently leading, but keep sizing smaller until more signals mature into confirmation.")

    if total_high_priority == 0:
        title = "Stay Defensive"
        lines.append("No high-priority cards were produced, so the current run still looks more suitable for observation than aggressive execution.")
    elif total_sent_high_priority < total_high_priority:
        lines.append(
            f"Only {total_sent_high_priority}/{total_high_priority} high-priority cards were actually sent, so keep an eye on suppression and threshold tightness."
        )

    if strongest_pool is None and strongest_event is None:
        title = "Wait For Maturity"
        lines.append("The current strategy still lacks enough mature outcomes, so avoid changing bias until more windows complete.")

    if not lines:
        lines.append("Signal mix and mature outcomes are broadly aligned; keep the current policy and collect more evidence.")
    support_title = (outcome_support or {}).get("title", "")
    if support_title == "Thin Evidence":
        title = "Wait For Maturity"
        lines.insert(0, "Completed windows are still too few to justify a strong directional tilt.")
    elif support_title == "Building Evidence":
        lines.insert(0, "Evidence is still building, so keep the current tilt measured rather than aggressive.")
    return {
        "title": title,
        "display_title": _display_strategy_tilt_title(title),
        "line_items": lines,
    }


def serialize_replay_evaluation(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
    cycle_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_pool_candidates = []
    if run_detail is not None:
        candidate_pool_candidates = _candidate_pool_candidates_from_summary(run_detail.get("summary", {}))
    return {
        "run": run_detail,
        "strategy_report": strategy_report,
        "source_health": source_health,
        "card_diagnostics": card_diagnostics or [],
        "decision_diagnostics": decision_diagnostics or [],
        "cycle_audit": cycle_audit or {},
        "prewatch_candidates": candidate_pool_candidates,
        "candidate_pool_candidates": candidate_pool_candidates,
        "alert_suppression_summary": _build_alert_suppression_summary(
            card_diagnostics or [],
            (run_detail or {}).get("candidate_evaluation_summary") or {},
        ),
    }


def serialize_llm_usage_report_payload(
    *,
    start_at: str,
    end_at: str,
    rows: dict[str, list[object]],
) -> dict[str, Any]:
    llm_input_token_price_per_k = 0.0004
    llm_output_token_price_per_k = 0.0032

    def _estimated_cost(input_tokens: int, output_tokens: int) -> float:
        return (input_tokens / 1000.0) * llm_input_token_price_per_k + (
            output_tokens / 1000.0
        ) * llm_output_token_price_per_k

    def _normalize_row(row: object) -> dict[str, Any]:
        return normalize_timestamp_fields(dict(row)) if row is not None else {}

    summary_rows = [_normalize_row(row) for row in rows.get("summary", [])]
    by_day_rows = [_normalize_row(row) for row in rows.get("by_day", [])]
    by_component_rows = [_normalize_row(row) for row in rows.get("by_component", [])]
    by_model_rows = [_normalize_row(row) for row in rows.get("by_model", [])]
    top_reason_rows = [_normalize_row(row) for row in rows.get("top_reasons", [])]
    recent_call_rows = [_normalize_row(row) for row in rows.get("recent_calls", [])]
    summary = summary_rows[0] if summary_rows else {}
    input_tokens = int(summary.get("prompt_tokens", 0) or 0)
    output_tokens = int(summary.get("completion_tokens", 0) or 0)
    total_tokens = input_tokens + output_tokens
    summary["input_tokens"] = input_tokens
    summary["output_tokens"] = output_tokens
    summary["total_tokens"] = total_tokens
    summary["estimated_cost"] = _estimated_cost(input_tokens, output_tokens)
    for row in by_day_rows:
        input_tokens = int(row.get("prompt_tokens", 0) or 0)
        output_tokens = int(row.get("completion_tokens", 0) or 0)
        row["input_tokens"] = input_tokens
        row["output_tokens"] = output_tokens
        row["total_tokens"] = input_tokens + output_tokens
        row["estimated_cost"] = _estimated_cost(input_tokens, output_tokens)
    for row in by_component_rows:
        row["component_display"] = _display_llm_component(str(row.get("component", "")))
        input_tokens = int(row.get("prompt_tokens", 0) or 0)
        output_tokens = int(row.get("completion_tokens", 0) or 0)
        row["input_tokens"] = input_tokens
        row["output_tokens"] = output_tokens
        row["total_tokens"] = input_tokens + output_tokens
        row["estimated_cost"] = _estimated_cost(input_tokens, output_tokens)
    for row in by_model_rows:
        input_tokens = int(row.get("prompt_tokens", 0) or 0)
        output_tokens = int(row.get("completion_tokens", 0) or 0)
        row["input_tokens"] = input_tokens
        row["output_tokens"] = output_tokens
        row["total_tokens"] = input_tokens + output_tokens
        row["estimated_cost"] = _estimated_cost(input_tokens, output_tokens)
    for row in top_reason_rows:
        row["component_display"] = _display_llm_component(str(row.get("component", "")))
    for row in recent_call_rows:
        row["component_display"] = _display_llm_component(str(row.get("component", "")))
        input_tokens = int(row.get("prompt_tokens_estimate", 0) or 0)
        output_tokens = int(row.get("completion_tokens_estimate", 0) or 0)
        row["input_tokens"] = input_tokens
        row["output_tokens"] = output_tokens
        row["total_tokens"] = input_tokens + output_tokens
        row["estimated_cost"] = _estimated_cost(input_tokens, output_tokens)
    return {
        "start_at": start_at,
        "end_at": end_at,
        "pricing": {
            "input_price_per_k": llm_input_token_price_per_k,
            "output_price_per_k": llm_output_token_price_per_k,
        },
        "summary": summary,
        "by_day": by_day_rows,
        "by_component": by_component_rows,
        "by_model": by_model_rows,
        "top_reasons": top_reason_rows,
        "recent_calls": recent_call_rows,
    }


def format_llm_usage_report_payload(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    by_day = payload.get("by_day") or []
    by_component = payload.get("by_component") or []
    by_model = payload.get("by_model") or []
    top_reasons = payload.get("top_reasons") or []
    recent_calls = payload.get("recent_calls") or []
    lines = [
        "LLM 用量报告",
        f"统计区间：{payload.get('start_at', '-')} ~ {payload.get('end_at', '-')}",
        "",
        "总览：",
        f"- 总记录数：{int(summary.get('total_records', 0) or 0)}",
        f"- 真实调用数：{int(summary.get('llm_calls', 0) or 0)}",
        f"- 成功调用数：{int(summary.get('success_calls', 0) or 0)}",
        f"- 失败回退数：{int(summary.get('failed_calls', 0) or 0)}",
        f"- 跳过调用数：{int(summary.get('skipped_calls', 0) or 0)}",
        f"- 输入 token：{int(summary.get('input_tokens', 0) or 0)}",
        f"- 输出 token：{int(summary.get('output_tokens', 0) or 0)}",
        f"- 总 token：{int(summary.get('total_tokens', 0) or 0)}",
        f"- 预估成本：¥{float(summary.get('estimated_cost', 0.0) or 0.0):.6f}",
        f"- 平均延迟：{round(float(summary.get('avg_latency_ms') or 0.0), 1)} ms",
        f"- 最大延迟：{int(summary.get('max_latency_ms', 0) or 0)} ms",
    ]
    lines.extend(["", "按天："])
    if by_day:
        for row in by_day:
            lines.append(
                f"- {row.get('usage_date', '-')}"
                f"：调用 {int(row.get('llm_calls', 0) or 0)}，成功 {int(row.get('success_calls', 0) or 0)}，"
                f"失败 {int(row.get('failed_calls', 0) or 0)}，跳过 {int(row.get('skipped_calls', 0) or 0)}，"
                f"输入 token {int(row.get('input_tokens', 0) or 0)}，输出 token {int(row.get('output_tokens', 0) or 0)}，"
                f"总 token {int(row.get('total_tokens', 0) or 0)}，预估成本 ¥{float(row.get('estimated_cost', 0.0) or 0.0):.6f}，"
                f"平均延迟 {round(float(row.get('avg_latency_ms') or 0.0), 1)} ms"
            )
    else:
        lines.append("- 暂无数据")
    lines.extend(["", "按环节："])
    if by_component:
        for row in by_component:
            lines.append(
                f"- {row.get('component_display', row.get('component', '-'))}"
                f"：调用 {int(row.get('llm_calls', 0) or 0)}，成功 {int(row.get('success_calls', 0) or 0)}，"
                f"失败 {int(row.get('failed_calls', 0) or 0)}，跳过 {int(row.get('skipped_calls', 0) or 0)}，"
                f"输入 token {int(row.get('input_tokens', 0) or 0)}，输出 token {int(row.get('output_tokens', 0) or 0)}，"
                f"总 token {int(row.get('total_tokens', 0) or 0)}，预估成本 ¥{float(row.get('estimated_cost', 0.0) or 0.0):.6f}，"
                f"平均延迟 {round(float(row.get('avg_latency_ms') or 0.0), 1)} ms"
            )
    else:
        lines.append("- 暂无数据")
    lines.extend(["", "按模型："])
    if by_model:
        for row in by_model:
            lines.append(
                f"- {row.get('model', '-')}"
                f"：调用 {int(row.get('llm_calls', 0) or 0)}，成功 {int(row.get('success_calls', 0) or 0)}，"
                f"失败 {int(row.get('failed_calls', 0) or 0)}，输入 token {int(row.get('input_tokens', 0) or 0)}，"
                f"输出 token {int(row.get('output_tokens', 0) or 0)}，总 token {int(row.get('total_tokens', 0) or 0)}，"
                f"预估成本 ¥{float(row.get('estimated_cost', 0.0) or 0.0):.6f}，"
                f"平均延迟 {round(float(row.get('avg_latency_ms') or 0.0), 1)} ms"
            )
    else:
        lines.append("- 暂无数据")
    lines.extend(["", "高频原因："])
    if top_reasons:
        for row in top_reasons:
            lines.append(
                f"- {row.get('component_display', row.get('component', '-'))}"
                f" / {row.get('reason', '-')}"
                f"：{int(row.get('occurrence_count', 0) or 0)} 次"
            )
    else:
        lines.append("- 暂无数据")
    lines.extend(["", "最近调用："])
    if recent_calls:
        for row in recent_calls[:10]:
            lines.append(
                f"- {format_beijing_minute(parse_datetime(str(row.get('created_at')))) if row.get('created_at') else '-'}"
                f" | {row.get('component_display', row.get('component', '-'))}"
                f" | {row.get('symbol', '-')}"
                f" | {'成功' if row.get('success') else '失败/跳过'}"
                f" | 输入 {int(row.get('input_tokens', 0) or 0)} / 输出 {int(row.get('output_tokens', 0) or 0)}"
                f" | token {int(row.get('total_tokens', 0) or 0)}"
                f" | ¥{float(row.get('estimated_cost', 0.0) or 0.0):.6f}"
                f" | {row.get('reason', '-')}"
            )
    else:
        lines.append("- 暂无数据")
    return "\n".join(lines)


def _format_llm_usage_snapshot_lines(payload: dict[str, Any] | None) -> list[str]:
    if not payload:
        return ["  最近窗口内没有 LLM 用量数据。"]
    summary = payload.get("summary") or {}
    if not summary:
        return ["  最近窗口内没有 LLM 用量数据。"]
    by_component = payload.get("by_component") or []
    lines = [
        "  "
        f"最近窗口：{payload.get('start_at', '-')} ~ {payload.get('end_at', '-')}",
        "  "
        f"真实调用 {summary.get('actual_calls', 0)} 次 / 失败回退 {summary.get('fallback_calls', 0)} 次 / "
        f"跳过 {summary.get('skipped_calls', 0)} 次 / 输入 token {summary.get('input_tokens', 0)} / "
        f"输出 token {summary.get('output_tokens', 0)} / 总 token {summary.get('total_tokens', 0)} / "
        f"预估成本 ¥{float(summary.get('estimated_cost', 0.0) or 0.0):.6f}",
    ]
    if by_component:
        parts = []
        for row in by_component[:3]:
            parts.append(
                f"{_display_llm_component(str(row.get('component') or ''))} "
                f"{row.get('actual_calls', 0)} 次"
            )
        lines.append(f"  主要环节：{'；'.join(parts)}")
    top_reasons = payload.get("top_reasons") or []
    if top_reasons:
        reason = str(top_reasons[0].get("reason") or "").strip() or "未标注"
        count = int(top_reasons[0].get("count", 0) or 0)
        lines.append(f"  高频原因：{reason}（{count} 次）")
    return lines


def serialize_run_comparison(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"runs": items}


def serialize_batch_replay(
    items: list[dict[str, Any]],
    recommendation_preferences: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = []
    for item in items:
        metrics = item.get("metrics", {})
        ranking.append(
            {
                "name": item["name"],
                "run_id": item.get("run_id", ""),
                "status": item["status"],
                "decision_count": metrics.get("decision_count", 0),
                "entered_count": metrics.get("entered_count", 0),
                "take_profit_exit_count": metrics.get("take_profit_exit_count", 0),
                "invalidation_exit_count": metrics.get("invalidation_exit_count", 0),
                "avg_realized_return": metrics.get("avg_realized_return"),
                "win_rate": metrics.get("win_rate"),
                "profit_loss_ratio": metrics.get("profit_loss_ratio"),
                "avg_t_plus_7_return": metrics.get("avg_t_plus_7_return"),
                "avg_t_plus_14_return": metrics.get("avg_t_plus_14_return"),
                "avg_t_plus_30_return": metrics.get("avg_t_plus_30_return"),
                "avg_max_drawdown": metrics.get("avg_max_drawdown"),
                "completed_outcome_count": metrics.get("completed_outcome_count", 0),
                "failures": item.get("failures", 0),
                "config_summary": item.get("config_summary", "-"),
                "db_path": item.get("db_path", ""),
            }
        )
    ranking.sort(
        key=lambda row: (
            0 if row["status"] == "success" else 1,
            -(float(row["avg_realized_return"]) if row["avg_realized_return"] is not None else float("-inf")),
            -(float(row["win_rate"]) if row["win_rate"] is not None else float("-inf")),
            float(row["avg_max_drawdown"]) if row["avg_max_drawdown"] is not None else float("inf"),
            -(float(row["profit_loss_ratio"]) if row["profit_loss_ratio"] is not None else float("-inf")),
            -(float(row["avg_t_plus_7_return"]) if row["avg_t_plus_7_return"] is not None else float("-inf")),
            row["failures"],
            row["name"],
        )
    )
    return {
        "experiments": items,
        "ranking": ranking,
        "recommendation_preferences": recommendation_preferences or {},
        "recommendation": _recommend_batch(items, recommendation_preferences),
        "summary": _summarize_batch(items, ranking, recommendation_preferences),
        "next_step": _next_step_batch(items, ranking, recommendation_preferences),
    }


def serialize_batch_comparison(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_items = {item["name"]: item for item in left.get("experiments", [])}
    right_items = {item["name"]: item for item in right.get("experiments", [])}
    names = sorted(set(left_items) | set(right_items))
    comparisons = []
    for name in names:
        left_item = left_items.get(name)
        right_item = right_items.get(name)
        left_summary = left_item.get("summary", {}) if left_item else {}
        right_summary = right_item.get("summary", {}) if right_item else {}
        comparisons.append(
            {
                "name": name,
                "left_present": left_item is not None,
                "right_present": right_item is not None,
                "left_status": left_item["status"] if left_item else "missing",
                "right_status": right_item["status"] if right_item else "missing",
                "left_config_summary": left_item.get("config_summary", "-") if left_item else "-",
                "right_config_summary": right_item.get("config_summary", "-") if right_item else "-",
                "left_alerts_sent": left_summary.get("alerts_sent", 0),
                "right_alerts_sent": right_summary.get("alerts_sent", 0),
                "left_cards_generated": left_summary.get("cards_generated", 0),
                "right_cards_generated": right_summary.get("cards_generated", 0),
                "left_events_processed": left_summary.get("events_processed", 0),
                "right_events_processed": right_summary.get("events_processed", 0),
                "alerts_delta": right_summary.get("alerts_sent", 0) - left_summary.get("alerts_sent", 0),
                "cards_delta": right_summary.get("cards_generated", 0) - left_summary.get("cards_generated", 0),
                "events_delta": right_summary.get("events_processed", 0) - left_summary.get("events_processed", 0),
                "market_margin_delta": _delta(
                    left_item.get("closest_market_margin") if left_item else None,
                    right_item.get("closest_market_margin") if right_item else None,
                ),
                "priority_margin_delta": _delta(
                    left_item.get("closest_priority_margin") if left_item else None,
                    right_item.get("closest_priority_margin") if right_item else None,
                ),
            }
        )
    return {
        "left_batch_id": left.get("batch_id", ""),
        "right_batch_id": right.get("batch_id", ""),
        "left_manifest_path": left.get("manifest_path", ""),
        "right_manifest_path": right.get("manifest_path", ""),
        "left_recommendation": left.get("recommendation"),
        "right_recommendation": right.get("recommendation"),
        "comparisons": comparisons,
        "summary": _summarize_batch_comparison(left, right, comparisons),
        "next_step": _next_step_batch_comparison(left, right, comparisons),
    }


def serialize_batch_index(payloads: list[dict[str, Any]]) -> dict[str, Any]:
    items = []
    for payload in payloads:
        recommendation = payload.get("recommendation") or {}
        next_step = payload.get("next_step") or {}
        experiments = payload.get("experiments", [])
        winner_snapshot = _winner_snapshot(payload)
        items.append(
            {
                "batch_id": payload.get("batch_id", ""),
                "generated_at": payload.get("generated_at", ""),
                "manifest_path": payload.get("manifest_path", ""),
                "report_path": payload.get("report_path", ""),
                "spec_path": payload.get("spec_path", ""),
                "experiment_count": len(experiments),
                "recommendation_name": recommendation.get("name", ""),
                "recommendation_config": recommendation.get("config_summary", "-"),
                "next_step_title": next_step.get("title", ""),
                "winner_snapshot": winner_snapshot,
            }
        )
    return {
        "batches": items,
        "summary": _summarize_batch_index(items),
    }


def _delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return round(float(right) - float(left), 2)


def _recommend_batch(
    items: list[dict[str, Any]],
    preferences: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    weights = _recommendation_weights(preferences)
    candidates = []
    for item in items:
        metrics = item.get("metrics", {})
        if item.get("status") != "success":
            continue
        if int(metrics.get("completed_outcome_count", 0) or 0) <= 0:
            continue
        avg_return = metrics.get("avg_realized_return")
        win_rate = metrics.get("win_rate")
        avg_drawdown = metrics.get("avg_max_drawdown")
        profit_loss_ratio = metrics.get("profit_loss_ratio")
        avg_t7 = metrics.get("avg_t_plus_7_return")
        score = (
            ((float(avg_return) if avg_return is not None else -999.0) * weights["avg_realized_return"])
            + ((float(win_rate) if win_rate is not None else 0.0) * weights["win_rate"])
            + ((float(profit_loss_ratio) if profit_loss_ratio is not None else 0.0) * weights["profit_loss_ratio"])
            + ((float(avg_t7) if avg_t7 is not None else 0.0) * weights["avg_t_plus_7_return"])
            - (item.get("failures", 0) * weights["failures"])
        )
        if avg_drawdown is not None:
            score -= abs(float(avg_drawdown)) * weights["max_drawdown"]
        candidates.append(
            (
                (
                    -round(score, 8),
                    -(float(avg_return) if avg_return is not None else float("-inf")),
                    -(float(win_rate) if win_rate is not None else float("-inf")),
                    abs(float(avg_drawdown)) if avg_drawdown is not None else float("inf"),
                    -(float(profit_loss_ratio) if profit_loss_ratio is not None else float("-inf")),
                    -(float(avg_t7) if avg_t7 is not None else float("-inf")),
                    item["name"],
                ),
                item,
                round(score, 4),
            )
        )
    if not candidates:
        return _recommend_batch_legacy(items, weights)
    candidates.sort(key=lambda pair: pair[0])
    chosen = candidates[0][1]
    chosen_score = candidates[0][2]
    metrics = chosen.get("metrics", {})
    reason = "在真实收益、胜率与风险控制的综合比较中表现最好。"
    return {
        "name": chosen["name"],
        "run_id": chosen.get("run_id", ""),
        "config_summary": chosen.get("config_summary", "-"),
        "reason": reason,
        "score": chosen_score,
        "weights": weights,
        "decision_count": metrics.get("decision_count", 0),
        "entered_count": metrics.get("entered_count", 0),
        "take_profit_exit_count": metrics.get("take_profit_exit_count", 0),
        "invalidation_exit_count": metrics.get("invalidation_exit_count", 0),
        "avg_realized_return": metrics.get("avg_realized_return"),
        "win_rate": metrics.get("win_rate"),
        "profit_loss_ratio": metrics.get("profit_loss_ratio"),
        "avg_t_plus_7_return": metrics.get("avg_t_plus_7_return"),
        "avg_max_drawdown": metrics.get("avg_max_drawdown"),
    }


def _recommendation_weights(preferences: dict[str, Any] | None) -> dict[str, float]:
    prefs = preferences or {}
    return {
        "avg_realized_return": float(prefs.get("avg_realized_return", 5.0)),
        "win_rate": float(prefs.get("win_rate", 2.5)),
        "max_drawdown": float(prefs.get("max_drawdown", 1.5)),
        "profit_loss_ratio": float(prefs.get("profit_loss_ratio", 1.5)),
        "avg_t_plus_7_return": float(prefs.get("avg_t_plus_7_return", 1.0)),
        "alerts_sent": float(prefs.get("alerts_sent", 3.0)),
        "cards_generated": float(prefs.get("cards_generated", 1.5)),
        "events_processed": float(prefs.get("events_processed", 1.0)),
        "strictness": float(prefs.get("strictness", 0.75)),
        "priority_proximity": float(prefs.get("priority_proximity", 0.25)),
        "failures": float(prefs.get("failures", 3.0)),
    }


def _recommend_batch_legacy(items: list[dict[str, Any]], weights: dict[str, float]) -> dict[str, Any] | None:
    candidates = []
    for item in items:
        if item.get("status") != "success":
            continue
        summary = item.get("summary", {})
        alerts_sent = int(summary.get("alerts_sent", 0) or 0)
        cards_generated = int(summary.get("cards_generated", 0) or 0)
        events_processed = int(summary.get("events_processed", 0) or 0)
        closest_market_margin = item.get("closest_market_margin")
        closest_priority_margin = item.get("closest_priority_margin")
        score = (
            alerts_sent * weights["alerts_sent"]
            + cards_generated * weights["cards_generated"]
            + events_processed * weights["events_processed"]
            - abs(float(closest_market_margin)) * weights["strictness"]
            if closest_market_margin is not None
            else -999.0
        )
        if closest_priority_margin is not None:
            score -= abs(float(closest_priority_margin)) * weights["priority_proximity"]
        score -= item.get("failures", 0) * weights["failures"]
        candidates.append(
            (
                (
                    -round(score, 8),
                    -alerts_sent,
                    -cards_generated,
                    -events_processed,
                    abs(float(closest_market_margin)) if closest_market_margin is not None else float("inf"),
                    abs(float(closest_priority_margin)) if closest_priority_margin is not None else float("inf"),
                    item["name"],
                ),
                item,
                round(score, 4),
            )
        )
    if not candidates:
        return None
    candidates.sort(key=lambda pair: pair[0])
    chosen = candidates[0][1]
    summary = chosen.get("summary", {})
    return {
        "name": chosen["name"],
        "run_id": chosen.get("run_id", ""),
        "config_summary": chosen.get("config_summary", "-"),
        "reason": "在当前样本下产出更稳定，同时更接近严格阈值控制。",
        "score": candidates[0][2],
        "weights": weights,
        "alerts_sent": int(summary.get("alerts_sent", 0) or 0),
        "cards_generated": int(summary.get("cards_generated", 0) or 0),
        "events_processed": int(summary.get("events_processed", 0) or 0),
        "closest_market_margin": chosen.get("closest_market_margin"),
        "closest_priority_margin": chosen.get("closest_priority_margin"),
    }


def _summarize_batch(
    items: list[dict[str, Any]],
    ranking: list[dict[str, Any]],
    preferences: dict[str, Any] | None,
) -> dict[str, Any]:
    successful = [item for item in items if item.get("status") == "success"]
    if not successful:
        return {
            "line_items": ["No successful experiments in this batch."],
            "has_meaningful_difference": False,
        }
    avg_returns = {item["name"]: item.get("metrics", {}).get("avg_realized_return") for item in successful}
    win_rates = {item["name"]: item.get("metrics", {}).get("win_rate") for item in successful}
    drawdowns = {item["name"]: item.get("metrics", {}).get("avg_max_drawdown") for item in successful}
    valid_returns = {name: value for name, value in avg_returns.items() if value is not None}
    recommendation = _recommend_batch(items, preferences)
    line_items = []
    top_ranked = ranking[0] if ranking else None
    if not valid_returns:
        if top_ranked:
            line_items.append(
                f"Top output: {top_ranked['name']}，已成交 {top_ranked['entered_count']}，当前还没有足够成熟样本做真实收益比较。"
            )
        output_counts = {
            item["name"]: (
                int(item.get("summary", {}).get("alerts_sent", 0) or 0),
                int(item.get("summary", {}).get("cards_generated", 0) or 0),
                int(item.get("summary", {}).get("events_processed", 0) or 0),
            )
            for item in successful
        }
        if len(set(output_counts.values())) == 1 and output_counts:
            line_items.append("各实验当前产出接近，建议扩大回放样本再比较。")
            return {"line_items": line_items, "has_meaningful_difference": False}
        if recommendation:
            line_items.append(
                f"当前更推荐：{recommendation['name']}，因为它在产出与阈值严格度之间更平衡。"
            )
        return {"line_items": line_items, "has_meaningful_difference": True}
    if top_ranked:
        line_items.append(
            f"当前领先：{top_ranked['name']}，平均真实收益 {top_ranked['avg_realized_return']}%，胜率 {top_ranked['win_rate']}%。"
        )
    if len(set(valid_returns.values())) == 1 and valid_returns:
        line_items.append("各实验的平均真实收益目前接近，建议扩大回放样本区间。")
    else:
        best_return_name, best_return = max(valid_returns.items(), key=lambda pair: pair[1]) if valid_returns else ("-", None)
        if best_return is not None:
            line_items.append(f"真实收益领先：{best_return_name}，平均真实收益 {best_return}%。")
    valid_drawdowns = {name: value for name, value in drawdowns.items() if value is not None}
    if valid_drawdowns:
        best_dd_name, best_dd = min(valid_drawdowns.items(), key=lambda pair: abs(pair[1]))
        line_items.append(f"风险控制最好：{best_dd_name}，平均最大回撤 {best_dd}%。")
    if recommendation:
        line_items.append(
            f"推荐策略：{recommendation['name']}，因为它在收益、胜率和回撤的综合平衡上最好。"
        )
    return {
        "line_items": line_items,
        "has_meaningful_difference": len(set(value for value in valid_returns.values())) > 1 if valid_returns else False,
    }


def _next_step_batch(
    items: list[dict[str, Any]],
    ranking: list[dict[str, Any]],
    preferences: dict[str, Any] | None,
) -> dict[str, Any]:
    successful = [item for item in items if item.get("status") == "success"]
    if not successful:
        return {
            "title": "Fix Batch Health",
            "line_items": ["Resolve run failures or data-source issues before comparing parameter sets."],
        }
    recommendation = _recommend_batch(items, preferences)
    summary = _summarize_batch(items, ranking, preferences)
    failure_count = sum(item.get("failures", 0) for item in items)
    lines = []
    title = "Advance Recommended Setup"
    if failure_count > 0:
        title = "Stabilize Data Path"
        lines.append("Reduce data or notification failures before trusting the batch recommendation.")
    elif recommendation and recommendation.get("alerts_sent") is not None and recommendation.get("avg_realized_return") is None:
        if not summary.get("has_meaningful_difference"):
            title = "Expand Replay Coverage"
            lines.append("当前实验只有产出差异，尚未形成足够成熟的后验样本，建议扩大回放覆盖。")
        else:
            title = "Advance Recommended Setup"
            lines.append(f"先把 {recommendation['name']} 作为下一轮赛马的临时基线。")
            lines.append("下一轮优先补充更多已成熟样本，再比较真实收益和回撤。")
        return {"title": title, "line_items": lines}
    if not summary.get("has_meaningful_difference"):
        title = "Expand Replay Coverage"
        lines.append("当前实验在收益层面的差异还不够大，建议扩大回放时间窗或加入震荡/下跌样本。")
    elif recommendation:
        lines.append(
            f"先把 {recommendation['name']} 作为下一轮赛马的临时基线。"
        )
        lines.append("下一轮建议至少加入一段风险偏好下降或震荡样本，检验策略是否只是吃到了市场 Beta。")
    if not lines and recommendation:
        lines.append(f"继续以 {recommendation['name']} 为基线，并在更长窗口上复测。")
    return {"title": title, "line_items": lines}


def _next_step_batch_comparison(
    left: dict[str, Any],
    right: dict[str, Any],
    comparisons: list[dict[str, Any]],
) -> dict[str, Any]:
    lines = []
    title = "Review Batch Drift"
    left_rec = left.get("recommendation")
    right_rec = right.get("recommendation")
    if left_rec and right_rec and left_rec.get("name") != right_rec.get("name"):
        title = "Promote New Baseline"
        lines.append(
            f"Recommended setup changed from {left_rec['name']} to {right_rec['name']}; validate the new winner on a larger replay sample."
        )
    if comparisons:
        no_output_change = all(
            item["alerts_delta"] == 0 and item["cards_delta"] == 0 and item["events_delta"] == 0
            for item in comparisons
        )
        if no_output_change:
            lines.append("Batch outputs are unchanged; increase replay coverage or widen parameter differences before another comparison.")
        else:
            biggest_alert_shift = max(comparisons, key=lambda item: abs(item["alerts_delta"]))
            if biggest_alert_shift["alerts_delta"] != 0:
                lines.append(
                    f"Largest alert shift is {biggest_alert_shift['name']} at {biggest_alert_shift['alerts_delta']:+d}; inspect that parameter delta first."
                )
    if not lines:
        lines.append("Keep comparing batch manifests over a larger replay set to confirm the current direction.")
    return {"title": title, "line_items": lines}


def _summarize_batch_comparison(
    left: dict[str, Any],
    right: dict[str, Any],
    comparisons: list[dict[str, Any]],
) -> dict[str, Any]:
    lines = []
    left_rec = left.get("recommendation") or {}
    right_rec = right.get("recommendation") or {}
    if left_rec.get("name") and right_rec.get("name"):
        if left_rec["name"] == right_rec["name"]:
            lines.append(f"两批实验的推荐参数保持一致，当前仍是 {right_rec['name']}。")
        else:
            lines.append(f"推荐参数从 {left_rec['name']} 切换到了 {right_rec['name']}。")
    elif right_rec.get("name"):
        lines.append(f"右侧批次给出了推荐参数 {right_rec['name']}，左侧批次没有可比较推荐。")
    elif left_rec.get("name"):
        lines.append(f"左侧批次推荐 {left_rec['name']}，右侧批次没有可比较推荐。")
    else:
        lines.append("两侧批次都没有完整推荐信息，只能做基础对比。")

    if comparisons:
        total_alert_delta = sum(item["alerts_delta"] for item in comparisons)
        total_cards_delta = sum(item["cards_delta"] for item in comparisons)
        total_events_delta = sum(item["events_delta"] for item in comparisons)
        lines.append(
            f"整体产出变化：提醒 {total_alert_delta:+d}，卡片 {total_cards_delta:+d}，事件 {total_events_delta:+d}。"
        )
        margin_deltas = [item["market_margin_delta"] for item in comparisons if item["market_margin_delta"] is not None]
        if margin_deltas:
            avg_margin_delta = round(sum(margin_deltas) / len(margin_deltas), 2)
            if avg_margin_delta < 0:
                lines.append(f"整体上右侧批次更贴近市场阈值，平均 market_margin 变化 {avg_margin_delta:+.2f}。")
            elif avg_margin_delta > 0:
                lines.append(f"整体上右侧批次更宽松，平均 market_margin 变化 {avg_margin_delta:+.2f}。")
            else:
                lines.append("整体上两侧批次在市场阈值距离上基本一致。")
        biggest_alert_shift = max(comparisons, key=lambda item: abs(item["alerts_delta"]))
        if biggest_alert_shift["alerts_delta"] != 0:
            lines.append(
                f"变化最大的实验是 {biggest_alert_shift['name']}，提醒数变化 {biggest_alert_shift['alerts_delta']:+d}。"
            )
    return {"line_items": lines}


def _winner_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    recommendation = payload.get("recommendation") or {}
    experiments = payload.get("experiments", [])
    chosen_name = recommendation.get("name", "")
    chosen = next((item for item in experiments if item.get("name") == chosen_name), None)
    summary = (chosen or {}).get("summary", {})
    return {
        "name": chosen_name,
        "config_summary": recommendation.get("config_summary", "-"),
        "alerts_sent": recommendation.get("alerts_sent", summary.get("alerts_sent")),
        "cards_generated": recommendation.get("cards_generated", summary.get("cards_generated")),
        "events_processed": recommendation.get("events_processed", summary.get("events_processed")),
        "closest_market_margin": recommendation.get(
            "closest_market_margin",
            (chosen or {}).get("closest_market_margin"),
        ),
        "closest_priority_margin": recommendation.get(
            "closest_priority_margin",
            (chosen or {}).get("closest_priority_margin"),
        ),
        "top_event": (chosen or {}).get("top_event", ""),
        "available": bool(chosen_name),
    }


def _summarize_batch_index(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return {
            "line_items": ["No batch manifests found in the selected directory."],
            "recommendation_changes": 0,
            "latest_recommendation": "",
        }
    recommendation_names = [item["recommendation_name"] for item in items if item.get("recommendation_name")]
    recommendation_count = len(recommendation_names)
    missing_count = len(items) - recommendation_count
    latest = items[0]
    line_items = [
        f"Latest batch is {latest['batch_id'] or '-'} with recommendation {latest['recommendation_name'] or '-'}."
    ]
    if len(items) == 1:
        line_items.append("Only one batch is on record, so recommendation drift cannot be assessed yet.")
        return {
            "line_items": line_items,
            "recommendation_changes": 0,
            "latest_recommendation": latest.get("recommendation_name", ""),
        }
    previous = items[1]
    if latest.get("recommendation_name") and previous.get("recommendation_name"):
        if latest["recommendation_name"] == previous["recommendation_name"]:
            line_items.append(
                f"Recommendation is stable across the latest two batches at {latest['recommendation_name']}."
            )
        else:
            line_items.append(
                f"Recommendation changed from {previous['recommendation_name']} to {latest['recommendation_name']} in the latest batch."
            )
    elif latest.get("recommendation_name") and not previous.get("recommendation_name"):
        line_items.append(
            "The latest batch has a recorded recommendation, but the prior batch does not, so short-term stability is not yet confirmed."
        )
    elif not latest.get("recommendation_name"):
        line_items.append(
            "The latest batch does not contain a recorded recommendation, so review its manifest before drawing conclusions."
        )
    unique_recommendations = []
    for name in recommendation_names:
        if not unique_recommendations or unique_recommendations[-1] != name:
            unique_recommendations.append(name)
    change_count = max(len(unique_recommendations) - 1, 0)
    if recommendation_count == 0:
        line_items.append("None of the listed batches contain a recorded recommendation yet.")
    elif recommendation_count == 1:
        line_items.append(
            f"Only 1 of {len(items)} listed batches contains a recorded recommendation: {recommendation_names[0]}."
        )
    elif len(set(recommendation_names)) == 1:
        if missing_count == 0:
            line_items.append(
                f"All listed batches agree on the same recommendation: {recommendation_names[0]}."
            )
        else:
            line_items.append(
                f"All batches with recorded recommendations point to {recommendation_names[0]}, but {missing_count} listed batch(es) are missing that metadata."
            )
    else:
        line_items.append(
            f"Recommendation changed {change_count} time(s) across the {recommendation_count} batch(es) that contain recorded recommendations."
        )
    next_steps = [item["next_step_title"] for item in items if item.get("next_step_title")]
    if next_steps:
        line_items.append(f"Current operational focus: {next_steps[0]}.")
    return {
        "line_items": line_items,
        "recommendation_changes": change_count,
        "latest_recommendation": latest.get("recommendation_name", ""),
    }


def format_runs(rows: Iterable[object]) -> str:
    items = serialize_runs(rows)
    if not items:
        return "No runs found."
    lines = ["run_id  status  finished_at  events  cards  alerts  failures  name"]
    for row in items:
        summary = row["summary"]
        lines.append(
            f"{row['run_id']}  {row['status']}  {format_beijing_minute(row['finished_at'])}  "
            f"{summary.get('events_processed', 0)}  {summary.get('cards_generated', 0)}  "
            f"{summary.get('alerts_sent', 0)}  {row['failures']}  {row['run_name'] or '-'}"
        )
    return "\n".join(lines)


def format_run_detail(row: object, logs: Iterable[object]) -> str:
    item = serialize_run_detail(row, logs)
    if item is None:
        return "Run not found."
    summary = item["summary"]
    lines = [
        f"Run: {item['run_id']}",
        f"Name: {item['run_name'] or '-'}",
        f"Note: {item['note'] or '-'}",
        f"Status: {item['status']}",
        f"Started: {format_beijing_minute(item['started_at'])}",
        f"Finished: {format_beijing_minute(item['finished_at'])}",
        "Summary:",
    ]
    for key in sorted(summary):
        lines.append(f"  {key}: {_format_summary_value(key, summary[key])}")
    if item["config_snapshot"]:
        lines.append("Config Snapshot:")
        for key in sorted(item["config_snapshot"]):
            lines.append(f"  {key}: {item['config_snapshot'][key]}")
    lines.append("Logs:")
    logs = item["logs"]
    if not logs:
        lines.append("  (no logs)")
    for log in logs:
        context = log["context"]
        context_part = f" | context={json.dumps(context, sort_keys=True)}" if context else ""
        symbol = f" [{log['symbol']}]" if log["symbol"] else ""
        event_id = f" ({log['event_id']})" if log["event_id"] else ""
        lines.append(
            f"  {format_beijing_minute(log['created_at'])} {log['level']} {log['stage']} {log['event_type']}{symbol}{event_id}: "
            f"{log['message']}{context_part}"
        )
    return "\n".join(lines)


def format_error_summary(rows: Iterable[object]) -> str:
    items = serialize_error_summary(rows)
    if not items:
        return "No errors found."
    lines = ["count  event_type  stage  symbol  message"]
    for row in items:
        lines.append(
            f"{row['count']}  {row['event_type']}  {row['stage']}  {row['symbol'] or '-'}  {row['message']}"
        )
    return "\n".join(lines)


def format_source_health(rows: Iterable[object]) -> str:
    items = serialize_source_health(rows)
    if not items:
        return "No source health checks found."
    lines = ["source  status  checked_at  latency_ms  detail"]
    for row in items:
        latency = row["latency_ms"] if row["latency_ms"] is not None else "-"
        detail = row["detail"]
        if row["status"] != "healthy" and _is_external_connectivity_issue(str(detail)):
            detail = "外网连通性异常，请检查 VPN/网络。"
        lines.append(
            f"{row['source_name']}  {row['status']}  {format_beijing_minute(row['checked_at'])}  {latency}  {detail}"
        )
    return "\n".join(lines)


def format_strategy_report(
    event_types: Iterable[object],
    source_stability: Iterable[object],
    alert_volume: Iterable[object],
    decision_outcomes_by_event_type: Iterable[object] | None = None,
    decision_outcomes_by_pool: Iterable[object] | None = None,
    outcome_data_coverage: dict[str, Any] | None = None,
) -> str:
    report = serialize_strategy_report(
        event_types,
        source_stability,
        alert_volume,
        decision_outcomes_by_event_type,
        decision_outcomes_by_pool,
        outcome_data_coverage,
    )
    lines = ["Event Type Performance:"]
    if not report["event_type_performance"]:
        lines.append("  (no event type data)")
    for row in report["event_type_performance"]:
        lines.append(
            f"  {row['event_type']}: cards={row['card_count']} avg_score={row['avg_final_score']} high_priority={row['high_priority_count']}"
        )
    lines.append("Source Stability:")
    if not report["source_stability"]:
        lines.append("  (no source health data)")
    for row in report["source_stability"]:
        lines.append(
            f"  {row['source_name']}: healthy={row['healthy_count']}/{row['check_count']} unhealthy={row['unhealthy_count']} avg_latency_ms={row['avg_latency_ms']}"
        )
    lines.append("Alert Volume:")
    if not report["alert_volume"]:
        lines.append("  (no alert history)")
    for row in report["alert_volume"]:
        lines.append(
            f"  {row['bucket_date']}: total={row['total_alerts']} sent={row['sent_alerts']} suppressed={row['suppressed_alerts']} high_priority={row['high_priority_alerts']}"
        )
    lines.append("Decision Outcomes By Event Type:")
    if not report["decision_outcomes_by_event_type"]:
        lines.append("  (no decision outcome data)")
    for row in report["decision_outcomes_by_event_type"]:
        lines.append(
            "  "
            f"{row['event_type']}: decisions={row['decision_count']} outcomes={row['outcome_count']} "
            f"pending={row['pending_count']} t3_positive={row['positive_t3_count']} "
            f"avg_t3={row['avg_t_plus_3_return']} tp_hits={row['take_profit_hits']} invalidations={row['invalidation_hits']} "
            f"readiness={row.get('readiness_label', '-')}"
        )
    lines.append("Decision Outcomes By Pool:")
    if not report["decision_outcomes_by_pool"]:
        lines.append("  (no decision outcome data)")
    for row in report["decision_outcomes_by_pool"]:
        lines.append(
            "  "
            f"{row['pool']}: decisions={row['decision_count']} outcomes={row['outcome_count']} "
            f"pending={row['pending_count']} t3_positive={row['positive_t3_count']} "
            f"avg_t3={row['avg_t_plus_3_return']} tp_hits={row['take_profit_hits']} invalidations={row['invalidation_hits']} "
            f"readiness={row.get('readiness_label', '-')}"
        )
    coverage = report.get("outcome_data_coverage") or {}
    if coverage:
        lines.append("Outcome Data Coverage:")
        scope = coverage.get("scope_label") or coverage.get("scope") or "main_only"
        lines.append(f"  scope={scope}")
        main_store = coverage.get("main_store") or {}
        lines.append(
            "  "
            f"main_store: decisions={main_store.get('decision_count', 0)} "
            f"outcomes={main_store.get('outcome_count', 0)} completed={main_store.get('completed_count', 0)} pending={main_store.get('pending_count', 0)}"
        )
        archive_store = coverage.get("archive_store") or {}
        if archive_store.get("present"):
            lines.append(
                "  "
                f"archive_store: decisions={archive_store.get('decision_count', 0)} "
                f"outcomes={archive_store.get('outcome_count', 0)} completed={archive_store.get('completed_count', 0)} pending={archive_store.get('pending_count', 0)}"
            )
        combined = coverage.get("combined") or {}
        lines.append(
            "  "
            f"combined: decisions={combined.get('decision_count', 0)} "
            f"outcomes={combined.get('outcome_count', 0)} completed={combined.get('completed_count', 0)} pending={combined.get('pending_count', 0)}"
        )
    headline = report.get("outcome_headline") or {}
    if headline.get("summary"):
        lines.append(f"Outcome Headline: {headline.get('summary')}")
        for line in headline.get("line_items", []):
            lines.append(f"  {line}")
    priority_list = report.get("outcome_priority_list") or {}
    if priority_list.get("line_items"):
        lines.append("Outcome Priorities:")
        for line in priority_list.get("line_items", []):
            lines.append(f"  {line}")
    lines.append("Outcome Summary:")
    for line in report.get("outcome_summary", {}).get("line_items", []):
        lines.append(f"  {line}")
    maturity = report.get("outcome_maturity", {}) or {}
    if maturity.get("line_items"):
        lines.append("Outcome Maturity:")
        for line in maturity["line_items"]:
            lines.append(f"  {line}")
    completed_cohort_summary = report.get("completed_cohort_summary", {}) or {}
    if completed_cohort_summary.get("line_items"):
        lines.append("Completed Cohorts:")
        for line in completed_cohort_summary["line_items"]:
            lines.append(f"  {line}")
    support = report.get("outcome_support", {}) or {}
    if support.get("line_items"):
        lines.append(f"Outcome Support: {support.get('display_title') or support.get('title', '-')}")
        for line in support["line_items"]:
            lines.append(f"  {line}")
        event_support = support.get("event_support") or {}
        if event_support.get("line_items"):
            lines.append(f"  Event Support: {event_support.get('display_title') or event_support.get('title', '-')}")
            for line in event_support["line_items"]:
                lines.append(f"    {line}")
        pool_support = support.get("pool_support") or {}
        if pool_support.get("line_items"):
            lines.append(f"  Pool Support: {pool_support.get('display_title') or pool_support.get('title', '-')}")
            for line in pool_support["line_items"]:
                lines.append(f"    {line}")
    confidence_regime = report.get("confidence_regime") or {}
    if confidence_regime.get("line_items"):
        lines.append(f"Confidence Regime: {confidence_regime.get('title', '-')}")
        for line in confidence_regime["line_items"]:
            lines.append(f"  {line}")
    validation = report.get("outcome_validation", {}) or {}
    if validation.get("line_items"):
        lines.append(
            f"Outcome Validation: {'OK' if validation.get('status') == 'ok' else 'Check Required'}"
        )
        for line in validation["line_items"]:
            lines.append(f"  {line}")
    recommendation = report.get("outcome_recommendation") or {}
    if recommendation.get("line_items"):
        lines.append(f"Outcome Recommendation: {recommendation.get('display_title') or recommendation.get('title', '-')}")
        for line in recommendation["line_items"]:
            lines.append(f"  {line}")
    strategy_tilt = report.get("strategy_tilt") or {}
    if strategy_tilt.get("line_items"):
        lines.append(f"Strategy Tilt: {strategy_tilt.get('display_title') or strategy_tilt.get('title', '-')}")
        for line in strategy_tilt["line_items"]:
            lines.append(f"  {line}")
    action_regime = report.get("action_regime") or {}
    if action_regime.get("line_items"):
        lines.append(f"Action Regime: {action_regime.get('title', '-')}")
        for line in action_regime["line_items"]:
            lines.append(f"  {line}")
    outcome_readiness = report.get("outcome_readiness") or {}
    if outcome_readiness.get("line_items"):
        lines.append("Outcome Readiness:")
        for line in outcome_readiness["line_items"]:
            lines.append(f"  {line}")
    readiness_actions = report.get("outcome_readiness_actions") or {}
    if readiness_actions.get("line_items"):
        lines.append("Readiness Actions:")
        for line in readiness_actions["line_items"]:
            lines.append(f"  {line}")
    return "\n".join(lines)


def format_recent_performance_review(review: dict[str, Any]) -> str:
    window = review.get("review_window") or {}
    overview = review.get("overview") or {}
    quality = review.get("execution_quality") or {}
    trade_path_summary = review.get("trade_path_summary") or {}
    pool_funnel_summary = review.get("pool_funnel_summary") or {}
    simulation_funnel_summary = review.get("simulation_funnel_summary") or {}
    candidate_evaluation_summary = review.get("candidate_evaluation_summary") or {}
    candidate_evaluation_trend_summary = review.get("candidate_evaluation_trend_summary") or {}
    appendix = review.get("appendix") or {}
    formal_readiness = review.get("formal_readiness") or {}
    adjusted_status = review.get("adjusted_price_status") or {}
    lines = [f"# {str(review.get('status_label') or '历史效果复盘')}"]
    lines.append(f"- 状态：{review.get('status', '-')}")
    lines.append(f"- 复盘口径版本：{review.get('review_version', '-')}")
    lines.append(f"- 统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}")
    lines.append(f"- 历史回补截止：{format_beijing_minute(review.get('backfill_cutoff_at'))}")
    lines.append(f"- 正式版门槛：{formal_readiness.get('status_label', '-')}")
    if adjusted_status:
        lines.append(
            f"- 复权状态：{'已满足' if review.get('adjusted_price_protection_ready') else '未满足'}"
            f"（覆盖 {len(adjusted_status.get('coverage') or [])} 个标的）"
        )
    sample_audit = review.get("sample_audit") or {}
    if sample_audit:
        lines.append(f"- 程序抽检：{sample_audit.get('status', '-')}")
        if sample_audit.get("summary_line"):
            lines.append(f"- 程序抽检说明：{sample_audit.get('summary_line')}")
    ai_review = review.get("ai_review") or review.get("manual_audit") or {}
    if ai_review:
        lines.append(f"- AI复核：{ai_review.get('status', '-')}")
        if ai_review.get("summary_line"):
            lines.append(f"- AI复核说明：{ai_review.get('summary_line')}")
    for blocker in formal_readiness.get("blockers", []):
        lines.append(f"- 阻塞项：{blocker}")
    for reason in review.get("draft_reasons", []):
        lines.append(f"- 说明：{reason}")

    glossary = _build_review_glossary(review)
    if glossary:
        lines.append("## 阅读备注：")
        for line in glossary:
            lines.append(f"- {line}")
        lines.append("")

    lines.append("## 执行摘要：")
    summary_lines = _format_executive_summary_lines(review)
    if summary_lines:
        for line in summary_lines[1:]:
            stripped = str(line).strip()
            if stripped:
                lines.append(f"- {stripped}")
    lines.append("")
    lines.append("## 核心面板")
    core_metric_lines = _format_core_metric_summary_lines(review)
    if core_metric_lines:
        lines.append("### 核心指标摘要：")
        lines.extend(core_metric_lines[1:])
        lines.append("")
    simulation_lines = _format_main_simulation_lines(review)
    if simulation_lines:
        lines.append("### 完整模拟闭环：")
        lines.extend(simulation_lines[1:])
        lines.append("")
    path_quality_lines = _format_main_path_quality_lines(review)
    if path_quality_lines:
        lines.append("### 路径质量 / 浮盈浮亏摘要：")
        lines.extend(path_quality_lines[1:])
        lines.append("")

    current_conclusion, checklist, why = _build_action_conclusion(review)
    lines.append("## 下一步建议：")
    lines.append("定位：基于30d触发、90d校验，告诉你现在该调、该查，还是先观察。")
    lines.append(f"当前结论：{current_conclusion}")
    if checklist:
        lines.append("优先检查项：")
        for index, item in enumerate(checklist, start=1):
            lines.append(f"  {index}. {item}")
    else:
        lines.append("优先检查项：当前没有建议直接动的参数，优先先观察或排查。")
    if why:
        lines.append(f"为什么是这个结论：{why}")
    lines.append("")

    lines.append("## 附录：")
    lines.append("#### 总体效果：")
    lines.append(f"  决策总数：{overview.get('decision_count', 0)}")
    lines.append(f"  已成交（试探建仓/确认做多）：{overview.get('entered_count', 0)}")
    lines.append(f"  持仓中（已成交后）：{overview.get('open_position_count', 0)}")
    lines.append(f"  止盈退出（已成交后）：{overview.get('take_profit_exit_count', 0)}")
    lines.append(f"  失效退出（已成交后）：{overview.get('invalidation_exit_count', 0)}")
    lines.append(f"  复盘窗口结算（已成交后）：{_window_close_evaluation_count(overview)}")
    lines.append(f"  未成交（试探建仓/确认做多，但未到入场区间）：{overview.get('not_entered_count', 0)}")
    lines.append(f"  平均模拟已实现收益：{_format_metric_value(overview.get('avg_realized_return'), suffix='%')}")
    lines.append(f"  中位数模拟已实现收益：{_format_metric_value(overview.get('median_realized_return'), suffix='%')}")
    lines.append(f"  模拟已实现胜率：{_format_rate_value(overview.get('win_rate'))}")
    lines.append(f"  盈亏比：{_format_metric_value(overview.get('profit_loss_ratio'))}")
    lines.append("")
    lines.append("#### 执行质量：")
    lines.append(f"  模拟成交率：{_format_rate_value(quality.get('entry_hit_rate'))}")
    lines.append(f"  止盈命中率：{_format_rate_value(quality.get('take_profit_hit_rate'))}")
    lines.append(f"  失效率：{_format_rate_value(quality.get('invalidation_hit_rate'))}")
    lines.append(f"  复盘窗口结算率：{_format_rate_value(quality.get('window_complete_rate'))}")
    lines.append(f"  平均持有天数：{_format_metric_value(quality.get('avg_holding_days'), suffix=' 天')}")
    lines.append(f"  已完成复盘样本数（已走出最终结果）：{quality.get('completed_outcome_count', 0)}")
    lines.append(f"  未结束样本数（含未成交与已成交未结束）：{quality.get('pending_outcome_count', 0)}")
    lines.append("")
    lines.append("#### 三池漏斗：")
    if (
        _candidate_pool_count(pool_funnel_summary) <= 0
        and _candidate_optional_alert_count(pool_funnel_summary) <= 0
        and int(pool_funnel_summary.get("promoted_confirmation_count", 0) or 0) <= 0
    ):
        lines.append("  当前窗口内还没有形成可计算的候选 -> 观察卡 -> 确认机会漏斗。")
    else:
        lines.append(f"  第一池候选：{_candidate_pool_count(pool_funnel_summary)} 条")
        lines.append(
            "  "
            f"观察卡：{_candidate_optional_alert_count(pool_funnel_summary)} 条"
            f"（候选 -> 观察卡转化率：{_format_rate_value(pool_funnel_summary.get('observation_to_light_push_rate'))}）"
        )
        lines.append(
            "  "
            f"确认机会：{pool_funnel_summary.get('promoted_confirmation_count', 0)} 条"
            f"（候选 -> 确认机会转化率：{_format_rate_value(pool_funnel_summary.get('observation_to_confirmation_rate'))}）"
        )
        lines.append(
            "  "
            f"观察卡后确认机会：{pool_funnel_summary.get('promoted_after_light_push_count', 0)} 条"
            f"（观察卡 -> 确认机会转化率：{_format_rate_value(pool_funnel_summary.get('light_push_to_confirmation_rate'))}）"
        )
        if int(pool_funnel_summary.get("promoted_without_light_push_count", 0) or 0) > 0:
            lines.append(
                "  "
                f"另有 {pool_funnel_summary.get('promoted_without_light_push_count', 0)} 条未经过观察卡，直接从候选进入确认机会。"
            )
        lines.append(
            "  "
            f"进入持仓管理退出：{pool_funnel_summary.get('holding_management_from_promoted_count', pool_funnel_summary.get('exit_from_promoted_count', 0))} 条"
            f"（确认机会 -> 持仓管理退出转化率：{_format_rate_value(pool_funnel_summary.get('confirmation_to_exit_rate'))}）"
        )
        if int(pool_funnel_summary.get("non_promoted_exit_count", 0) or 0) > 0:
            lines.append(
                "  "
                f"另有 {pool_funnel_summary.get('non_promoted_exit_count', 0)} 条持仓管理记录来自非候选池确认机会链路。"
            )
    lines.append("")
    lines.append("#### 完整模拟闭环：")
    if (
        _candidate_pool_count(simulation_funnel_summary) <= 0
        and _candidate_optional_alert_count(simulation_funnel_summary) <= 0
        and int(simulation_funnel_summary.get("promoted_confirmation_count", 0) or 0) <= 0
        and int(simulation_funnel_summary.get("simulated_entry_count", 0) or 0) <= 0
    ):
        lines.append("  当前窗口内还没有形成可计算的完整模拟闭环。")
    else:
        lines.append(f"  候选：{_candidate_pool_count(simulation_funnel_summary)} 条")
        lines.append(
            "  "
            f"观察卡：{_candidate_optional_alert_count(simulation_funnel_summary)} 条"
            f"（候选 -> 观察卡转化率：{_format_rate_value(simulation_funnel_summary.get('observation_to_light_push_rate'))}）"
        )
        lines.append(
            "  "
            f"确认机会：{simulation_funnel_summary.get('promoted_confirmation_count', 0)} 条"
            f"（候选 -> 确认机会转化率：{_format_rate_value(simulation_funnel_summary.get('observation_to_confirmation_rate'))}）"
        )
        lines.append(
            "  "
            f"模拟成交：{simulation_funnel_summary.get('simulated_entry_count', 0)} 条"
            f"（确认机会 -> 模拟成交转化率：{_format_rate_value(simulation_funnel_summary.get('confirmation_to_entry_rate'))}）"
        )
        lines.append(
            "  "
            f"进入持仓管理退出：{_holding_management_transition_count(simulation_funnel_summary)} 条"
            f"（模拟成交 -> 持仓管理退出转化率：{_format_rate_value(_entry_to_holding_management_rate(simulation_funnel_summary))}）"
        )
        lines.append(
            "  "
            f"模拟退出完成：{simulation_funnel_summary.get('simulated_completed_exit_count', 0)} 条"
            f"（模拟成交 -> 模拟退出完成率：{_format_rate_value(simulation_funnel_summary.get('entry_to_completed_exit_rate'))}）"
        )
        if simulation_funnel_summary.get("avg_confirmation_to_entry_days") is not None:
            lines.append(
                "  "
                f"确认机会 -> 模拟成交：平均等待 {_format_metric_value(simulation_funnel_summary.get('avg_confirmation_to_entry_days'), suffix=' 天')}，"
                f"中位数 {_format_metric_value(simulation_funnel_summary.get('median_confirmation_to_entry_days'), suffix=' 天')}"
            )
        if int(simulation_funnel_summary.get("confirmation_to_entry_timing_anomaly_count", 0) or 0) > 0:
            lines.append(
                "  "
                f"已跳过 {int(simulation_funnel_summary.get('confirmation_to_entry_timing_anomaly_count', 0) or 0)} 条成交时间早于决策时间的异常样本。"
            )
        if simulation_funnel_summary.get("avg_entry_to_completed_exit_days") is not None:
            lines.append(
                "  "
                f"模拟成交 -> 模拟退出完成：平均持有 {_format_metric_value(simulation_funnel_summary.get('avg_entry_to_completed_exit_days'), suffix=' 天')}，"
                f"中位数 {_format_metric_value(simulation_funnel_summary.get('median_entry_to_completed_exit_days'), suffix=' 天')}"
            )
        for row in simulation_funnel_summary.get("entry_timing_by_action", []):
            lines.append(
                "  "
                f"按动作看成交等待：{row.get('action_display', row.get('action', '-'))}"
                f"：样本 {row.get('sample_count', 0)} 条，平均等待 "
                f"{_format_metric_value(row.get('avg_days_to_entry'), suffix=' 天')}，"
                f"中位数 {_format_metric_value(row.get('median_days_to_entry'), suffix=' 天')}"
            )
        for row in simulation_funnel_summary.get("completed_exit_timing_by_action", []):
            lines.append(
                "  "
                f"按动作看持有时长：{row.get('action_display', row.get('action', '-'))}"
                f"：样本 {row.get('sample_count', 0)} 条，平均持有 "
                f"{_format_metric_value(row.get('avg_holding_days'), suffix=' 天')}，"
                f"中位数 {_format_metric_value(row.get('median_holding_days'), suffix=' 天')}"
            )
        if int(simulation_funnel_summary.get("simulated_pending_entry_count", 0) or 0) > 0:
            lines.append(
                f"  另有 {simulation_funnel_summary.get('simulated_pending_entry_count', 0)} 条确认机会尚未形成模拟成交。"
            )
        if int(simulation_funnel_summary.get("simulated_open_count", 0) or 0) > 0:
            lines.append(
                f"  另有 {simulation_funnel_summary.get('simulated_open_count', 0)} 条已模拟成交，但当前仍在持仓中。"
            )
        completed_exit_breakdown = _normalized_completed_exit_breakdown(simulation_funnel_summary)
        if int(simulation_funnel_summary.get("simulated_completed_exit_count", 0) or 0) > 0:
            parts = []
            for reason in (
                "profit_protection_exit",
                "hit_take_profit",
                "invalidation_exit",
                "window_close_evaluation",
            ):
                count = int(completed_exit_breakdown.get(reason, 0) or 0)
                if count > 0:
                    parts.append(f"{_normalized_close_reason_label(reason)} {count} 条")
            if parts:
                lines.append(f"  其中：{' / '.join(parts)}")
    lines.append("")
    lines.append("#### 最近新增观察样本：")
    lines.extend(_format_recent_observation_sample_lines(review.get("recent_observation_samples") or []))
    lines.append("")
    lines.extend(_format_observation_after_summary_lines(review.get("observation_after_summary") or {}))
    lines.append("")
    lines.append("#### 候选诊断：")
    lines.extend(_format_candidate_evaluation_summary_lines(candidate_evaluation_summary))
    lines.append("")
    lines.append("#### 候选诊断趋势：")
    lines.extend(_format_candidate_evaluation_trend_lines(candidate_evaluation_trend_summary))
    lines.append("")
    lines.append("#### 交易轨迹摘要：")
    transition_count = _holding_management_transition_count(trade_path_summary)
    if transition_count <= 0:
        lines.append("  当前窗口内还没有形成从进攻逻辑走到持仓管理退出的已模拟成交样本。")
    else:
        lines.append(f"  确认机会后进入持仓管理退出：{transition_count} 条")
        lines.append(
            f"  平均历时：{_format_metric_value(trade_path_summary.get('avg_days_to_holding_management', trade_path_summary.get('avg_days_to_exit_pool')), suffix=' 天')}"
        )
        lines.append(
            f"  中位数历时：{_format_metric_value(trade_path_summary.get('median_days_to_holding_management', trade_path_summary.get('median_days_to_exit_pool')), suffix=' 天')}"
        )
        for row in trade_path_summary.get("by_action", []):
            lines.append(
                "  "
                f"{row.get('action_display', row.get('action', '-'))} -> 持仓管理退出：样本 {row.get('sample_count', 0)} 条，"
                f"平均 {_format_metric_value(row.get('avg_holding_days'), suffix=' 天')}"
            )
        fastest_sample = trade_path_summary.get("fastest_sample") or {}
        if fastest_sample:
            lines.append(
                "  "
                f"最快样本：{fastest_sample.get('symbol', '-')}（{fastest_sample.get('action_display', fastest_sample.get('action', '-'))}），"
                f"{_format_metric_value(fastest_sample.get('holding_days'), suffix=' 天')}"
            )
        slowest_sample = trade_path_summary.get("slowest_sample") or {}
        if slowest_sample:
            lines.append(
                "  "
                f"最慢样本：{slowest_sample.get('symbol', '-')}（{slowest_sample.get('action_display', slowest_sample.get('action', '-'))}），"
                f"{_format_metric_value(slowest_sample.get('holding_days'), suffix=' 天')}"
            )
        recent_samples = list(trade_path_summary.get("recent_samples") or [])
        if recent_samples:
            lines.append("  代表样本（最近3条）：")
            for row in recent_samples:
                lines.append(f"  {_format_trade_path_sample_line(row)}")
        best_samples = list(trade_path_summary.get("best_samples") or [])
        if best_samples:
            lines.append("  收益最好轨迹样本：")
            for row in best_samples:
                lines.append(f"  {_format_trade_path_sample_line(row)}")
        worst_samples = list(trade_path_summary.get("worst_samples") or [])
        if worst_samples:
            lines.append("  收益最差轨迹样本：")
            for row in worst_samples:
                lines.append(f"  {_format_trade_path_sample_line(row)}")
    lines.append("")
    lines.extend(_format_breakdown_section("#### 分组效果 - 按事件类型：", list((review.get("breakdowns") or {}).get("event_type") or [])))
    lines.append("")
    lines.extend(_format_breakdown_section("#### 分组效果 - 按池子：", list((review.get("breakdowns") or {}).get("pool") or [])))
    lines.append("")
    lines.extend(_format_breakdown_section("#### 分组效果 - 按动作：", list((review.get("breakdowns") or {}).get("action") or [])))
    lines.append("")
    lines.extend(_format_breakdown_section("#### 分组效果 - 按触发模式：", list((review.get("breakdowns") or {}).get("trigger_mode") or [])))
    lines.append("")
    lines.extend(_format_breakdown_section("#### 分组效果 - 按优先级：", list((review.get("breakdowns") or {}).get("priority") or [])))
    lines.append("")
    lines.append("#### 辅助观察收益（均值=平均收益率）：")
    lines.append("  说明：其中最大浮盈 / 最大回撤已与主路径质量口径对齐，仅统计已模拟成交样本。")
    for row in review.get("auxiliary_observation", []):
        lines.append(
            f"  {row.get('label', row.get('field', '-'))}：样本 {row.get('sample_count', 0)} 条，均值 {_format_metric_value(row.get('avg_value'), suffix='%')}"
        )
    lines.append("")
    lines.extend(
        _format_ranked_decision_table(
            "#### 效果最好 Top3（已完成样本）：",
            list(review.get("best_completed_decisions") or []),
            metric_field="realized_return",
            metric_label="模拟已实现收益",
        )
    )
    lines.append("")
    lines.extend(
        _format_ranked_decision_table(
            "#### 效果最差 Top3（已完成样本）：",
            list(review.get("worst_completed_decisions") or []),
            metric_field="realized_return",
            metric_label="模拟已实现收益",
        )
    )
    lines.append("")
    lines.extend(
        _format_ranked_decision_table(
            "#### T+7 表现最好 Top3：",
            list(review.get("best_t7_decisions") or []),
            metric_field="t_plus_7_return",
            metric_label="T+7收益",
        )
    )
    lines.append("")
    lines.extend(
        _format_ranked_decision_table(
            "#### T+7 表现最差 Top3：",
            list(review.get("worst_t7_decisions") or []),
            metric_field="t_plus_7_return",
            metric_label="T+7收益",
        )
    )
    lines.append("")
    detail_lines = _format_decision_detail_table(list(review.get("decision_details") or []))
    if detail_lines:
        lines.append(f"#### {detail_lines[0]}")
        lines.extend(detail_lines[1:])
    lines.append("")
    lines.append(f"加入观察信号数：{appendix.get('observation_signal_count', 0)}")
    return "\n".join(lines)


def format_replay_evaluation(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
    cycle_audit: dict[str, Any] | None = None,
) -> str:
    lines = ["Replay Evaluation:"]
    if run_detail is None:
        lines.append("Run: (missing)")
    else:
        lines.append(f"Run: {run_detail['run_id']} ({run_detail['status']})")
        lines.append(f"Name: {run_detail.get('run_name') or '-'}")
        lines.append(f"Note: {run_detail.get('note') or '-'}")
        lines.append(f"Started: {format_beijing_minute(run_detail['started_at'])}")
        lines.append(f"Finished: {format_beijing_minute(run_detail['finished_at'])}")
        lines.append("Run Summary:")
        for key in sorted(run_detail["summary"]):
            lines.append(f"  {key}: {_format_summary_value(key, run_detail['summary'][key])}")
    lines.append(format_strategy_report(
        strategy_report.get("event_type_performance", []),
        strategy_report.get("source_stability", []),
        strategy_report.get("alert_volume", []),
    ))
    lines.append("Latest Source Health:")
    if not source_health:
        lines.append("  (no source health data)")
    for row in source_health:
        latency = row["latency_ms"] if row["latency_ms"] is not None else "-"
        lines.append(
            f"  {row['source_name']}: status={row['status']} checked_at={format_beijing_minute(row['checked_at'])} latency_ms={latency} detail={row['detail']}"
        )
    lines.append("Candidate Pool:")
    candidate_pool_candidates = (
        _candidate_pool_candidates_from_summary(run_detail.get("summary", {}))
        if run_detail
        else []
    )
    if not candidate_pool_candidates:
        lines.append("  (no candidate-pool candidates)")
    for row in candidate_pool_candidates:
        lines.append(
            "  "
            f"{row['symbol']} {row['horizon']} {_display_candidate_setup(row['setup_type'])} "
            f"score={row['score']} rv={row['relative_volume']} rsi={row['rsi_14']} trend={row['trend_state']}"
        )
    lines.append("Card Diagnostics:")
    diagnostics = card_diagnostics or []
    if not diagnostics:
        lines.append("  (no card diagnostics)")
    for row in diagnostics:
        lines.append(
            "  "
            f"{row['symbol']} {row['horizon']} {row['event_type']} priority={row['priority']} "
            f"event={row['event_score']}/{row['event_threshold']} ({row['event_margin']:+.2f}) "
            f"market={row['market_score']}/{row['market_threshold']} ({row['market_margin']:+.2f}) "
            f"final={row['final_score']}/{row['priority_threshold']} ({row['priority_margin']:+.2f})"
        )
    lines.append("Decision Diagnostics:")
    decisions = decision_diagnostics or []
    if not decisions:
        lines.append("  (no decision diagnostics)")
    for row in decisions[:20]:
        lines.append(
            "  "
            f"{row['symbol']} {row['pool_label']} action={row['action']} "
            f"priority={row['priority']} confidence={row['confidence']} "
            f"trigger={row['trigger_mode']} final={row['final_score']:.2f}"
        )
        context_line = _format_outcome_context_line(row)
        if context_line:
            lines.append(f"    Outcome Context: {context_line}")
        if row.get("t_plus_3_return") is not None or row.get("t_plus_7_return") is not None or row.get("t_plus_14_return") is not None or row.get("t_plus_30_return") is not None or row.get("max_drawdown") is not None:
            lines.append(
                "    "
                f"T+3={row.get('t_plus_3_return')} T+5={row.get('t_plus_5_return')} T+7={row.get('t_plus_7_return')} T+14={row.get('t_plus_14_return')} T+30={row.get('t_plus_30_return')} "
                f"runup={row.get('max_runup')} drawdown={row.get('max_drawdown')}"
            )
    cycle_audit_payload = cycle_audit or {}
    if cycle_audit_payload:
        summary = cycle_audit_payload.get("summary") or {}
        status_counts = summary.get("status_counts") or {}
        event_kind_counts = summary.get("event_kind_counts") or {}
        lines.append("Cycle Audit:")
        lines.append(
            "  "
            f"symbols={cycle_audit_payload.get('symbol_count', 0)} "
            f"pending={status_counts.get('pending_entry', 0)} "
            f"holding={status_counts.get('holding_active', 0)} "
            f"terminal={status_counts.get('terminal', 0)}"
        )
        lines.append(
            "  "
            f"downgraded_watch={event_kind_counts.get('formal_downgraded_to_watch_unentered', 0)} "
            f"suppressed_holding={event_kind_counts.get('suppressed_active_holding', 0)} "
            f"terminal_events={event_kind_counts.get('terminal', 0)}"
        )
        reopened_cycle_symbols = list(summary.get("reopened_cycle_symbols") or [])
        if reopened_cycle_symbols:
            lines.append("  reopened_cycles=" + ", ".join(reopened_cycle_symbols))
        terminal_reason_counts = summary.get("terminal_reason_counts") or {}
        if terminal_reason_counts:
            lines.append(
                "  terminal_reasons="
                + ", ".join(f"{key}:{value}" for key, value in terminal_reason_counts.items())
            )
        anomaly_type_counts = summary.get("anomaly_type_counts") or {}
        if anomaly_type_counts:
            lines.append(
                "  anomalies="
                + ", ".join(f"{key}:{value}" for key, value in anomaly_type_counts.items())
            )
        lines.append(
            "  "
            f"anomaly_scope current_run={summary.get('current_run_anomaly_count', 0)} "
            f"historical_carryover={summary.get('historical_carryover_anomaly_count', 0)}"
        )
        for row in (cycle_audit_payload.get("items") or [])[:10]:
            lines.append(
                "  "
                f"{row.get('symbol', '-')} status={row.get('status', '-')}"
                f" cycles={row.get('cycle_count', 0)}"
                f" last_action={row.get('previous_formal_action', '-') or '-'}"
                f" candidate72h={row.get('candidate_observation_count_72h', 0)}"
            )
            if row.get("latest_terminal"):
                lines.append(
                    "    "
                    f"terminal={((row.get('latest_terminal') or {}).get('normalized_reason') or '-')}"
                )
            if row.get("latest_downgraded_watch"):
                lines.append("    downgraded_watch=yes")
            if row.get("latest_suppression"):
                lines.append("    suppressed_holding=yes")
            for anomaly in row.get("anomalies") or []:
                lines.append(
                    "    "
                    + f"anomaly={anomaly.get('type', '-')}"
                    + f" scope={anomaly.get('scope', '-')}: {anomaly.get('message', '-')}"
                )
    return "\n".join(lines)


def summarize_run_health(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if run_detail is None:
        return {
            "status": "阻塞",
            "line_items": ["运行结果缺失，无法判断系统健康状态。"],
        }
    summary = run_detail.get("summary", {})
    diagnostics = card_diagnostics or []
    unhealthy_sources = [row for row in source_health if row.get("status") != "healthy"]
    connectivity_issues = summarize_external_connectivity_issues(source_health)
    source_health_failures = int(summary.get("source_health_failures", 0))
    extraction_failures = int(summary.get("extraction_failures", 0))
    market_data_failures = int(summary.get("market_data_failures", 0))
    scoring_failures = int(summary.get("scoring_failures", 0))
    notification_failures = int(summary.get("notification_failures", 0))
    cards_generated = int(summary.get("cards_generated", 0))
    alerts_sent = int(summary.get("alerts_sent", 0))
    candidate_optional_alerts_sent = int(summary.get("candidate_optional_alerts_sent_count", summary.get("prewatch_alerts_sent_count", 0)))
    events_processed = int(summary.get("events_processed", 0))
    candidate_pool_count = int(summary.get("candidate_pool_count", summary.get("prewatch_candidates_count", len(_candidate_pool_candidates_from_summary(summary)))))
    lines = [
        f"本轮处理 {events_processed} 个事件，生成 {cards_generated} 张卡片，发送 {alerts_sent} 条提醒，识别 {candidate_pool_count} 个第一池候选，并发出 {candidate_optional_alerts_sent} 条观察卡。"
    ]
    if diagnostics:
        closest_market = min(
            (row["market_margin"] for row in diagnostics if row.get("market_margin") is not None),
            default=None,
        )
        closest_priority = min(
            (row["priority_margin"] for row in diagnostics if row.get("priority_margin") is not None),
            default=None,
        )
        if closest_market is not None:
            lines.append(f"最接近市场阈值的卡片距离为 {closest_market:+.2f}。")
        if closest_priority is not None:
            lines.append(f"最接近高优先级阈值的卡片距离为 {closest_priority:+.2f}。")
    if connectivity_issues["has_issue"]:
        lines.append(
            "检测到外网连通性异常，建议先检查 VPN 或当前网络；相关原始报错已不在复盘正文展开。"
        )
    elif unhealthy_sources or source_health_failures:
        lines.append(
            f"数据源健康告警 {len(unhealthy_sources) + source_health_failures} 次，需要留意数据链路稳定性。"
        )
    if market_data_failures or extraction_failures or scoring_failures:
        lines.append(
            "核心链路存在失败："
            f"抽取 {extraction_failures}、行情 {market_data_failures}、评分 {scoring_failures}。"
        )
    if notification_failures:
        lines.append(f"通知发送失败 {notification_failures} 次，提醒闭环未完全打通。")
    if cards_generated > 0 and alerts_sent == 0 and notification_failures == 0:
        lines.append("有卡片但没有提醒发出，说明当前阈值或优先级压制较强。")
    alert_volume = strategy_report.get("alert_volume", [])
    if alert_volume:
        high_priority_cards = sum(int(row.get("high_priority_alerts", 0)) for row in alert_volume)
        sent_high_priority = sum(int(row.get("sent_high_priority_alerts", 0)) for row in alert_volume)
        lines.append(f"本轮高优先级卡片 {high_priority_cards} 张，其中真正发出 {sent_high_priority} 条提醒。")
    outcome_support = strategy_report.get("outcome_support") or {}
    if outcome_support.get("title") == "Thin Evidence":
        lines.append("后验支撑仍偏薄，当前复盘更适合观察方向而不是据此大幅调整策略。")
    elif outcome_support.get("title") == "Building Evidence":
        lines.append("后验支撑正在积累，但样本仍偏集中，适合渐进式调整而不是一次性切换偏好。")
    outcome_summary = _summarize_decision_outcomes(decision_diagnostics or [])
    if outcome_summary["line_items"]:
        lines.append(outcome_summary["line_items"][0])

    status = "正常"
    if (
        run_detail.get("status") != "success"
        or extraction_failures > 0
        or market_data_failures > 0
        or scoring_failures > 0
    ):
        status = "阻塞"
    elif notification_failures > 0 or unhealthy_sources or source_health_failures > 0:
        status = "需要关注"
    return {"status": status, "line_items": lines}


def _summarize_decision_outcomes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"line_items": ["本轮没有落盘决策，暂时没有后验结果可跟踪。"]}

    total = len(rows)
    with_outcomes = sum(
        1
        for row in rows
        if (
            row.get("t_plus_1_return") is not None
            or row.get("t_plus_3_return") is not None
            or row.get("t_plus_5_return") is not None
            or row.get("t_plus_7_return") is not None
            or row.get("t_plus_10_return") is not None
            or row.get("t_plus_14_return") is not None
            or row.get("t_plus_30_return") is not None
            or row.get("max_runup") is not None
            or row.get("max_drawdown") is not None
            or row.get("close_reason")
        )
    )
    completed = sum(
        1
        for row in rows
        if _normalized_close_reason_from_row(row) in {
            "window_close_evaluation",
            "hit_take_profit",
            "profit_protection_exit",
            "invalidation_exit",
        }
    )
    pending = sum(1 for row in rows if _normalized_close_reason_from_row(row) == "holding_active")
    take_profit_hits = sum(
        1
        for row in rows
        if bool(row.get("hit_take_profit"))
        or _normalized_close_reason_from_row(row) in {"hit_take_profit", "profit_protection_exit"}
    )
    invalidation_hits = sum(
        1
        for row in rows
        if bool(row.get("hit_invalidation")) or _normalized_close_reason_from_row(row) == "invalidation_exit"
    )
    t_plus_3_values = [float(row["t_plus_3_return"]) for row in rows if row.get("t_plus_3_return") is not None]
    positive_t3 = sum(1 for value in t_plus_3_values if value > 0)
    line_items = [
        f"决策后验已回写 {with_outcomes}/{total} 条，完整观察窗 {completed} 条，仍待更多 bars {pending} 条。"
    ]
    line_items.append(f"止盈命中 {take_profit_hits} 条，失效命中 {invalidation_hits} 条。")
    if t_plus_3_values:
        avg_t3 = round(sum(t_plus_3_values) / len(t_plus_3_values), 2)
        line_items.append(f"T+3 已回写 {len(t_plus_3_values)} 条，正收益 {positive_t3} 条，平均 {avg_t3:+.2f}%。")
    else:
        line_items.append("当前还没有可用的 T+3 后验收益样本。")
    return {"line_items": line_items}


def _parse_iso_datetime(value: str | None) -> datetime | None:
    parsed = parse_datetime(value)
    if parsed is None:
        return None
    return parsed


def _format_run_duration(started_at: str | None, finished_at: str | None) -> str:
    started = _parse_iso_datetime(started_at)
    finished = _parse_iso_datetime(finished_at)
    if started is None or finished is None:
        return "-"
    return f"{(finished - started).total_seconds():.2f} 秒"


def _format_trade_path_duration(started_at: str | None, finished_at: str | None) -> str:
    started = _parse_iso_datetime(started_at)
    finished = _parse_iso_datetime(finished_at)
    if started is None or finished is None or finished < started:
        return "-"
    total_seconds = int((finished - started).total_seconds())
    total_minutes = total_seconds // 60
    total_hours = total_seconds // 3600
    total_days = total_seconds // 86400
    if total_days >= 1:
        if total_hours % 24 == 0:
            return f"{total_days} 天"
        return f"{total_days} 天 {total_hours % 24} 小时"
    if total_hours >= 1:
        return f"{total_hours} 小时"
    return f"{max(total_minutes, 1)} 分钟"


def _format_trade_path_sample_line(row: dict[str, Any]) -> str:
    return (
        f"- {format_beijing_minute(row.get('created_at'))} | {row.get('symbol', '-')} | "
        f"{row.get('action_display', row.get('action', '-'))} -> {row.get('close_reason_display', row.get('close_reason', '-'))} | "
        f"历时 {_format_metric_value(row.get('holding_days'), suffix=' 天')} | "
        f"真实收益 {_format_metric_value(row.get('realized_return'), suffix='%')}"
    )


def _card_takeaway(row: dict[str, Any]) -> str:
    market_margin = row.get("market_margin")
    priority_margin = row.get("priority_margin")
    if priority_margin is not None and priority_margin >= 0:
        return "综合强度已达到高优先级，可优先关注。"
    if market_margin is not None and market_margin < 0:
        return "事件本身通过，但价格位置或技术确认不足，暂时更适合观察。"
    if priority_margin is not None and priority_margin > -3:
        return "已经接近高优先级门槛，若后续价格或事件继续强化，可升级关注。"
    if priority_margin is not None and priority_margin > -6:
        return "事件与市场确认都过线，但离高优先级还有一段距离，属于普通提醒。"
    return "当前更像候选机会，适合跟踪，不宜视作强信号。"


def _format_link_lines(source_refs: list[str] | None) -> list[str]:
    refs = [ref for ref in (source_refs or []) if ref]
    if not refs:
        return ["    原文链接：无"]
    lines = [f"    原文链接 {index}：{ref}" for index, ref in enumerate(refs[:3], start=1)]
    return lines


def _delivery_view(row: dict[str, Any]) -> dict[str, Any]:
    cached = row.get("delivery_view")
    if isinstance(cached, dict) and cached:
        return cached
    return build_delivery_view_from_record(row)


def _format_card_header(row: dict[str, Any]) -> str:
    delivery = _delivery_view(row)
    return (
        f"  {delivery['identity']} / 交易周期：{delivery['horizon_display']} / "
        f"{delivery['event_type_display']} / {delivery['priority_display']}"
    )


def _format_card_action_lines(row: dict[str, Any]) -> list[str]:
    delivery = _delivery_view(row)
    action_label = str(delivery.get("action_label_effective") or "").strip()
    confidence = str(delivery.get("confidence_label_effective") or "").strip()
    if not action_label:
        return []
    if confidence:
        return [f"    操作建议：{action_label}（置信度：{confidence}）"]
    return [f"    操作建议：{action_label}"]


def _format_card_delivery_lines(row: dict[str, Any]) -> list[str]:
    delivery = _delivery_view(row)
    theme_line = str(delivery.get("theme_reason_line") or "").strip()
    delivery_theme_tags = list(delivery.get("theme_tags") or [])
    row_theme_tags = [str(tag).strip() for tag in (row.get("theme_tags") or []) if str(tag).strip()]
    confirmed_peers = [str(symbol).strip() for symbol in (row.get("confirmed_peer_symbols") or []) if str(symbol).strip()]
    if (not delivery_theme_tags or "未标注" in theme_line) and row_theme_tags:
        theme_text = " / ".join(row_theme_tags)
        if confirmed_peers:
            theme_line = f"题材：{theme_text}；同题材已有确认标的：{'、'.join(confirmed_peers[:3])}"
        else:
            theme_line = f"题材：{theme_text}"
    lines = [
        f"    事件解读：{delivery['event_reason_line']}",
        f"    行情解读：{delivery['market_reason_line']}",
        f"    题材解读：{theme_line or delivery['theme_reason_line']}",
    ]
    valid_until_text = str(delivery.get("valid_until_text") or "").strip()
    if valid_until_text:
        lines.append(f"    有效至：{valid_until_text}")
    source_summary = str(delivery.get("source_summary") or "").strip()
    if source_summary:
        lines.append(f"    信息来源：{source_summary}")
    return lines


def _format_push_card_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有实际发送的卡片。"]
    lines: list[str] = []
    for row in rows:
        lines.append(_format_card_header(row))
        lines.extend(_format_card_action_lines(row))
        if row.get("promoted_from_prewatch"):
            candidate_setup_type = row.get("candidate_setup_type", row.get("prewatch_setup_type", ""))
            candidate_score = float(row.get("candidate_score", row.get("prewatch_score", 0.0)) or 0.0)
            lines.append(
                f"    阶段：第一池：候选池 -> 第二池：确认池（{_display_candidate_setup(candidate_setup_type)} / 候选池 {candidate_score:.2f} 分）"
            )
            observation_count = int(row.get("candidate_observation_count", row.get("prewatch_observation_count", 0)) or 0)
            alert_sent_count = int(row.get("candidate_alert_sent_count", row.get("prewatch_alert_sent_count", 0)) or 0)
            if observation_count > 0:
                lifecycle_text = f"累计观察 {observation_count} 次"
                if alert_sent_count > 0:
                    lifecycle_text += f"，观察卡 {alert_sent_count} 次"
                first_seen_at = str(row.get("candidate_first_seen_at", row.get("prewatch_first_seen_at") or "")).strip()
                last_seen_at = str(row.get("candidate_last_seen_at", row.get("prewatch_last_seen_at") or "")).strip()
                if first_seen_at or last_seen_at:
                    lifecycle_text += (
                        f"（首次 {format_beijing_minute(first_seen_at)} / 最近 {format_beijing_minute(last_seen_at)}）"
                    )
                lines.append(f"    观察轨迹：{lifecycle_text}")
            source_decision_id = row.get("candidate_source_decision_id") or row.get("prewatch_source_decision_id")
            if source_decision_id:
                lines.append(f"    候选池来源决策：{source_decision_id}")
        lines.extend(_format_card_delivery_lines(row))
        lines.append(f"    解读：{_card_takeaway(row)}")
        score_parts: list[str] = []
        if row.get("event_score") is not None:
            event_text = f"事件 {row.get('event_score')}"
            if row.get("event_threshold") is not None:
                event_text += f"/{row.get('event_threshold')}"
            if row.get("event_margin") is not None:
                event_text += f" ({float(row.get('event_margin')):+.2f})"
            score_parts.append(event_text)
        if row.get("market_score") is not None:
            market_text = f"市场 {row.get('market_score')}"
            if row.get("market_threshold") is not None:
                market_text += f"/{row.get('market_threshold')}"
            if row.get("market_margin") is not None:
                market_text += f" ({float(row.get('market_margin')):+.2f})"
            score_parts.append(market_text)
        if row.get("final_score") is not None:
            final_text = f"综合 {row.get('final_score')}"
            if row.get("priority_threshold") is not None:
                final_text += f"/{row.get('priority_threshold')}"
            if row.get("priority_margin") is not None:
                final_text += f" ({float(row.get('priority_margin')):+.2f})"
            score_parts.append(final_text)
        if score_parts:
            lines.append("    分数：" + "，".join(score_parts))
        if not row.get("market_data_complete", True):
            lines.append(f"    行情状态：{row.get('market_data_note') or '行情快照暂不可用，未自动生成价格计划。'}")
        elif row.get("entry_range") and row.get("take_profit_range"):
            entry = row["entry_range"]
            take_profit = row["take_profit_range"]
            lines.append(
                f"    价格：入场 {entry['low']}-{entry['high']}，止盈 {take_profit['low']}-{take_profit['high']}，失效价 {row.get('invalidation_level', '-')}"
            )
        if row.get("invalidation_reason"):
            lines.append(f"    失效条件：{row['invalidation_reason']}")
        if row.get("reason_to_watch"):
            lines.append(f"    关注理由：{_modernize_display_text(row['reason_to_watch'])}")
        if row.get("trend_state") or row.get("rsi_14") is not None or row.get("relative_volume") is not None:
            metrics = []
            if row.get("trend_state"):
                metrics.append(f"趋势 {row['trend_state']}")
            if row.get("rsi_14") is not None:
                metrics.append(f"RSI {row['rsi_14']}")
            if row.get("relative_volume") is not None:
                metrics.append(f"相对量能 {row['relative_volume']} 倍")
            lines.append(f"    关键指标：{' / '.join(metrics)}")
        if row.get("positioning_hint"):
            lines.append(f"    仓位提示：{row['positioning_hint']}")
        lines.extend(_format_link_lines(row.get("source_refs")))
    return lines


def _format_promoted_confirmation_lines(rows: list[dict[str, Any]]) -> list[str]:
    promoted = [row for row in rows if row.get("promoted_from_prewatch")]
    if not promoted:
        return ["  本轮没有出现从第一池：候选池进入第二池：确认池的标的。"]
    ranked = sorted(
        promoted,
        key=lambda row: (
            {"high": 0, "normal": 1, "suppressed": 2}.get(row.get("priority", ""), 3),
            -float(row.get("final_score", 0.0)),
            row.get("symbol", ""),
            row.get("horizon", ""),
        ),
    )
    lines: list[str] = []
    for row in ranked:
        lines.append(_format_card_header(row))
        candidate_setup_type = row.get("candidate_setup_type", row.get("prewatch_setup_type", ""))
        candidate_score = float(row.get("candidate_score", row.get("prewatch_score", 0.0)) or 0.0)
        lines.append(
            f"    形成原因：此前处于{_display_candidate_setup(candidate_setup_type)}候选状态（{candidate_score:.2f} 分），本轮事件达到确认条件。"
        )
        promotion_reason = row.get("candidate_promotion_reason", row.get("prewatch_promotion_reason"))
        if promotion_reason:
            lines.append(f"    确认机会链路：{_modernize_display_text(str(promotion_reason))}")
        observation_count = int(row.get("candidate_observation_count", row.get("prewatch_observation_count", 0)) or 0)
        alert_sent_count = int(row.get("candidate_alert_sent_count", row.get("prewatch_alert_sent_count", 0)) or 0)
        if observation_count > 0:
            lifecycle_text = f"累计观察 {observation_count} 次"
            if alert_sent_count > 0:
                lifecycle_text += f"，观察卡 {alert_sent_count} 次"
            first_seen_at = str(row.get("candidate_first_seen_at", row.get("prewatch_first_seen_at") or "")).strip()
            last_seen_at = str(row.get("candidate_last_seen_at", row.get("prewatch_last_seen_at") or "")).strip()
            if first_seen_at or last_seen_at:
                lifecycle_text += (
                    f"（首次 {format_beijing_minute(first_seen_at)} / 最近 {format_beijing_minute(last_seen_at)}）"
                )
            lines.append(f"    观察轨迹：{lifecycle_text}")
        source_decision_id = row.get("candidate_source_decision_id") or row.get("prewatch_source_decision_id")
        if source_decision_id:
            lines.append(f"    候选池来源决策：{source_decision_id}")
        lines.extend(_format_card_action_lines(row))
        lines.extend(_format_card_delivery_lines(row))
        if row.get("positioning_hint"):
            lines.append(f"    仓位提示：{row['positioning_hint']}")
        lines.append(
            f"    当前分数：事件 {row['event_score']:.2f} / 市场 {row['market_score']:.2f} / 综合 {row['final_score']:.2f}"
        )
    return lines


def _format_candidate_pool_lines(
    rows: list[dict[str, Any]],
    *,
    symbol_theme_map: dict[str, list[str]],
) -> list[str]:
    if not rows:
        return ["  本轮没有识别出值得提前跟踪的第一池候选。"]
    peer_map = build_candidate_pool_peer_map(rows, symbol_theme_map)
    lines: list[str] = []
    for row in rows:
        tags = theme_tags_for_symbol(str(row["symbol"]), symbol_theme_map)
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_candidate_setup(row['setup_type'])} / 候选池 {row['score']:.2f} 分"
        )
        lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(f"    入池方式：{_display_candidate_trigger_mode(str(row.get('trigger_mode', 'structure')))}")
        if tags:
            lines.append(f"    题材：{' / '.join(tags)}")
        peers = peer_map.get(str(row["symbol"]).upper(), [])
        if peers:
            lines.append(f"    同题材联动：{', '.join(peers[:4])}")
        lines.append(
            f"    状态：现价 {row['last_price']}，RSI {row['rsi_14']}，相对量能 {row['relative_volume']} 倍，趋势 {row['trend_state']}。"
        )
        lines.append(f"    建议：{_modernize_display_text(row['action_hint'])}")
        lines.append(f"    关注理由：{_modernize_display_text(row['reason_to_watch'])}")
    return lines


def _format_theme_linkage_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有形成明显的题材链路。"]
    lines: list[str] = []
    for row in rows:
        candidate_pool_only_symbols = row.get("candidate_pool_only_symbols", row.get("prewatch_only_symbols", []))
        lines.append(f"  {row['theme_name']} / 热度 {row['heat_score']}")
        if row["confirmed_symbols"]:
            lines.append(f"    确认池：{', '.join(row['confirmed_symbols'])}")
        if row["promoted_symbols"]:
            lines.append(f"    确认机会：{', '.join(row['promoted_symbols'])}")
        if candidate_pool_only_symbols:
            lines.append(f"    第一池：候选池：{', '.join(candidate_pool_only_symbols)}")
        if row["sent_symbols"]:
            lines.append(f"    已发送：{', '.join(row['sent_symbols'])}")
        lines.append(f"    链路判断：{_modernize_display_text(row['chain_note'])}")
    return lines


def _format_candidate_optional_alert_lines(rows: list[dict[str, Any]], sent_symbols: list[str]) -> list[str]:
    if not sent_symbols:
        return ["  本轮没有发送观察卡。"]
    by_symbol = {row["symbol"]: row for row in rows}
    lines: list[str] = []
    for symbol in sent_symbols:
        row = by_symbol.get(symbol)
        if row is None:
            lines.append(f"  {symbol} / 观察卡已发送")
            continue
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_candidate_setup(row['setup_type'])} / {row['score']:.2f} 分"
        )
        lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(f"    入池方式：{_display_candidate_trigger_mode(str(row.get('trigger_mode', 'structure')))}")
        lines.append(f"    建议：{_modernize_display_text(row['action_hint'])}")
        lines.append(f"    关注理由：{_modernize_display_text(row['reason_to_watch'])}")
    return lines


def _format_new_observation_lines(rows: list[dict[str, Any]], sent_symbols: list[str]) -> list[str]:
    if not rows:
        return ["  本轮没有新增观察标的。"]
    sent_symbol_set = {str(symbol).strip().upper() for symbol in sent_symbols}
    background_rows = [
        row
        for row in rows
        if str(row.get("symbol") or "").strip().upper() not in sent_symbol_set
    ]
    if not background_rows:
        return ["  本轮新增候选标的都已触发观察卡，没有仅后台观察的标的。"]
    lines = [
        f"  本轮新增观察标的 {len(rows)} 个，其中 {len(background_rows)} 个当前仍处于后台观察。"
    ]
    ranked = sorted(
        background_rows,
        key=lambda row: (
            -float(row.get("score", 0.0)),
            str(row.get("symbol") or ""),
        ),
    )
    for row in ranked:
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / "
            f"{_display_candidate_setup(row['setup_type'])} / 候选池 {row['score']:.2f} 分"
        )
        if row.get("headline_summary"):
            lines.append(f"    摘要：{row['headline_summary']}")
        lines.append("    当前状态：仅后台观察，尚未触发观察卡。")
        if row.get("reason_to_watch"):
            lines.append(f"    关注理由：{_modernize_display_text(row['reason_to_watch'])}")
    return lines


def _format_recent_observation_sample_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  当前窗口内还没有新增观察样本。"]
    lines: list[str] = []
    for row in rows:
        parts = [
            format_beijing_minute(row.get("created_at")),
            str(row.get("symbol") or "-"),
            str(row.get("observation_status") or "仅后台观察"),
            str(row.get("confirmation_status") or "仍在观察"),
        ]
        lines.append(f"  - {' | '.join(parts)}")
        detail_parts = []
        if row.get("score") is not None:
            detail_parts.append(f"候选池 {float(row.get('score') or 0.0):.2f} 分")
        if row.get("setup_type"):
            detail_parts.append(_display_candidate_setup(str(row.get("setup_type") or "")))
        if row.get("trigger_mode"):
            detail_parts.append(_display_candidate_trigger_mode(str(row.get("trigger_mode") or "")))
        if int(row.get("observation_count") or 0) > 0:
            detail_parts.append(f"累计观察 {int(row.get('observation_count') or 0)} 次")
        if int(row.get("alert_sent_count") or 0) > 0:
            detail_parts.append(f"观察卡 {int(row.get('alert_sent_count') or 0)} 次")
        if row.get("theme_ids"):
            detail_parts.append(f"题材 {' / '.join(row['theme_ids'])}")
        if detail_parts:
            lines.append(f"    {' / '.join(detail_parts)}")
    return lines


def _format_observation_after_summary_lines(summary: dict[str, Any]) -> list[str]:
    lines = ["#### 观察后表现：", "  定位：看加入观察的标的，后续值不值得继续盯。"]
    observation_count = int(summary.get("observation_count", 0) or 0)
    if observation_count <= 0:
        lines.append("  当前窗口内还没有观察样本。")
        return lines
    lines.append(f"  观察样本：{observation_count} 条")
    lines.append(
        "  "
        f"观察卡：{int(summary.get('observation_alert_count', 0) or 0)} 条"
        f"（候选 -> 观察卡转化率：{_format_rate_value(summary.get('observation_to_alert_rate'))}）"
    )
    lines.append(
        "  "
        f"确认机会：{int(summary.get('promoted_confirmation_count', 0) or 0)} 条"
        f"（候选 -> 确认机会转化率：{_format_rate_value(summary.get('observation_to_confirmation_rate'))}）"
    )
    if int(summary.get("observation_alert_count", 0) or 0) > 0:
        lines.append(
            "  "
            f"观察卡后升级：{int(summary.get('promoted_after_alert_count', 0) or 0)} 条"
            f"（观察卡 -> 确认机会转化率：{_format_rate_value(summary.get('alert_to_confirmation_rate'))}）"
        )
    if int(summary.get("promoted_without_alert_count", 0) or 0) > 0:
        lines.append(
            "  "
            f"另有 {int(summary.get('promoted_without_alert_count', 0) or 0)} 条未经过观察卡，直接升级为确认机会。"
        )
    lines.append(f"  仍在观察：{int(summary.get('still_observing_symbol_count', 0) or 0)} 个标的")
    if summary.get("avg_days_to_confirmation") is not None:
        lines.append(
            "  "
            f"观察 -> 确认：平均 {_format_metric_value(summary.get('avg_days_to_confirmation'), suffix=' 天')}，"
            f"中位数 {_format_metric_value(summary.get('median_days_to_confirmation'), suffix=' 天')}"
        )
    representative_samples = list(summary.get("representative_samples") or [])
    if representative_samples:
        lines.append("  代表样本：")
        for row in representative_samples:
            detail_parts = []
            if row.get("candidate_setup_type", row.get("prewatch_setup_type")):
                detail_parts.append(_display_candidate_setup(str(row.get("candidate_setup_type", row.get("prewatch_setup_type") or ""))))
            if row.get("candidate_score", row.get("prewatch_score")) is not None:
                detail_parts.append(f"候选池 {float(row.get('candidate_score', row.get('prewatch_score') or 0.0) or 0.0):.2f} 分")
            if int(row.get("candidate_observation_count", row.get("prewatch_observation_count", 0)) or 0) > 0:
                detail_parts.append(f"累计观察 {int(row.get('candidate_observation_count', row.get('prewatch_observation_count', 0)) or 0)} 次")
            if int(row.get("candidate_alert_sent_count", row.get("prewatch_alert_sent_count", 0)) or 0) > 0:
                detail_parts.append(f"观察卡 {int(row.get('candidate_alert_sent_count', row.get('prewatch_alert_sent_count', 0)) or 0)} 次")
            lines.append(
                "  "
                f"- {format_beijing_minute(row.get('created_at'))} | {row.get('symbol', '-')} | "
                f"{row.get('action_display', row.get('action', '-'))}"
                + (f" | {' / '.join(detail_parts)}" if detail_parts else "")
            )
    return lines


def _holding_management_reason_detail(subreason: str) -> str:
    normalized = str(subreason or "").strip()
    if normalized == "invalidation_exit":
        return "价格已跌破失效价，本次进攻逻辑结束。"
    if normalized == "window_close_evaluation":
        return "窗口到期结算只用于后验复盘提供统一截止时间，不代表真实自动卖出。"
    if normalized == "target_hit":
        return "价格已进入计划止盈区更深位置，当前更适合按计划兑现利润。"
    if normalized == "weakening_after_tp_zone":
        return "进入止盈区后连续走弱，继续持有更容易把浮盈回吐回去。"
    if normalized == "macro_protection":
        return "宏观环境转差且已有浮盈，当前更适合先做利润保护。"
    return "当前这笔交易已从进攻逻辑切换到持仓管理。"


def _exit_pool_reason_detail(subreason: str) -> str:
    return _holding_management_reason_detail(subreason)


def _holding_management_reason_key(row: dict[str, Any]) -> str:
    for key in ("holding_management_reason", "normalized_close_reason", "subreason"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    action_label = str(row.get("action_label") or row.get("action") or "").strip()
    if action_label == "失效价退出":
        return "invalidation_exit"
    if action_label == "利润保护退出":
        return "profit_protection_exit"
    if action_label == "窗口到期结算":
        return "window_close_evaluation"
    return ""


def _modernize_display_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    replacements = (
        ("进入兑现池", "利润保护退出"),
        ("兑现池管理卡", "退出卡"),
        ("预备池阶段", "候选池阶段"),
        ("预备状态", "候选状态"),
        ("预备池来源决策", "候选池来源决策"),
        ("此前已进入预备池", "此前已进入候选池"),
        ("进入预备池", "进入候选池"),
        ("预备池候选", "候选池候选"),
        ("预备池", "候选池"),
        ("观察提醒", "观察卡"),
        ("兑现池", "第三池：持仓管理"),
    )
    for old, new in replacements:
        value = value.replace(old, new)
    value = re.sub(
        r"此前已进入候选池，累计观察\s*(\d+)\s*次",
        r"此前已进入候选池，近72h进入候选池 \1 次",
        value,
    )
    value = re.sub(
        r"候选池阶段累计观察\s*(\d+)\s*次",
        r"此前已进入候选池，近72h进入候选池 \1 次",
        value,
    )
    value = value.replace("期间已观察卡", "期间已发观察卡")
    return value


def _format_holding_management_breakdown(rows: list[dict[str, Any]]) -> str:
    reason_map = {
        "profit_protection_exit": "利润保护退出",
        "invalidation_exit": "失效价退出",
        "window_close_evaluation": "窗口到期结算",
        "target_hit": "达标止盈",
        "weakening_after_tp_zone": "提前锁盈",
        "macro_protection": "宏观保护",
    }
    counts: dict[str, int] = {}
    for row in rows:
        key = reason_map.get(_holding_management_reason_key(row), "持仓管理")
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return ""
    ordered = ["利润保护退出", "失效价退出", "窗口到期结算", "达标止盈", "提前锁盈", "宏观保护", "持仓管理"]
    parts = [f"{label} {counts[label]} 个" for label in ordered if counts.get(label)]
    return "，".join(parts)


def _format_holding_management_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有标的进入持仓管理。"]
    reason_map = {
        "profit_protection_exit": "利润保护退出",
        "invalidation_exit": "失效价退出",
        "window_close_evaluation": "窗口到期结算",
        "target_hit": "达标止盈",
        "weakening_after_tp_zone": "提前锁盈",
        "macro_protection": "宏观保护",
    }
    lines: list[str] = []
    for row in rows:
        identity = row.get("display_name") or row.get("symbol")
        reason_key = _holding_management_reason_key(row)
        action_label = str(row.get("action_label") or "").strip()
        reason = reason_map.get(reason_key, action_label or "持仓管理")
        lines.append(f"  {identity} / {_display_horizon(str(row.get('horizon') or 'position'))} / {reason}")
        lines.append(f"    状态：{_modernize_display_text(row.get('reason_to_watch') or _exit_pool_reason_detail(reason_key))}")
        if row.get("chain_summary"):
            lines.append(f"    来源链路：{_modernize_display_text(row['chain_summary'])}")
        if row.get("source_decision_id"):
            lines.append(f"    来源决策：{row['source_decision_id']}")
        take_profit_range = row.get("take_profit_range") or {}
        if take_profit_range.get("low") is not None and take_profit_range.get("high") is not None:
            lines.append(
                f"    原目标区：{float(take_profit_range['low']):.2f}-{float(take_profit_range['high']):.2f}"
            )
        if row.get("positioning_hint"):
            lines.append(f"    当前处理：{row['positioning_hint']}")
        if reason_key == "invalidation_exit" or action_label == "失效价退出":
            lines.append("    使用边界：失效价退出属于风控结束，不代表新的开仓信号。")
        elif reason_key == "window_close_evaluation":
            lines.append("    使用边界：窗口到期结算只用于复盘评估，不代表真实自动卖出。")
        else:
            lines.append("    使用边界：持仓管理只面向已有仓位，不代表新的开仓信号。")
    return lines


def _format_candidate_pool_lines_legacy(
    rows: list[dict[str, Any]],
    *,
    symbol_theme_map: dict[str, list[str]],
) -> list[str]:
    return _format_candidate_pool_lines(rows, symbol_theme_map=symbol_theme_map)

def _format_prewatch_lines(
    rows: list[dict[str, Any]],
    *,
    symbol_theme_map: dict[str, list[str]],
) -> list[str]:
    return _format_candidate_pool_lines_legacy(rows, symbol_theme_map=symbol_theme_map)


def _format_candidate_optional_alert_lines_legacy(rows: list[dict[str, Any]], sent_symbols: list[str]) -> list[str]:
    return _format_candidate_optional_alert_lines(rows, sent_symbols)


def _format_prewatch_push_lines(rows: list[dict[str, Any]], sent_symbols: list[str]) -> list[str]:
    return _format_candidate_optional_alert_lines_legacy(rows, sent_symbols)


def _format_exit_pool_breakdown(rows: list[dict[str, Any]]) -> str:
    return _format_holding_management_breakdown(rows)


def _format_exit_pool_lines(rows: list[dict[str, Any]]) -> list[str]:
    return _format_holding_management_lines(rows)


def _format_decision_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有落盘的决策记录。"]
    lines: list[str] = []
    for row in rows[:20]:
        title = (
            f"  {row['symbol']} / {_modernize_display_text(str(row.get('pool_label', row['pool']) or row['pool']))} / "
            f"{_modernize_display_text(str(row.get('action', '-') or '-'))}"
        )
        lines.append(title)
        lines.append(
            "    "
            f"强度：优先级 {row.get('priority', '-')} / 置信度 {row.get('confidence', '-')} / "
            f"触发模式 {row.get('trigger_mode', '-')}"
        )
        lines.append(
            "    "
            f"分数：事件 {row.get('event_score', 0.0):.2f} / "
            f"市场 {row.get('market_score', 0.0):.2f} / "
            f"题材 {row.get('theme_score', 0.0):.2f} / "
            f"综合 {row.get('final_score', 0.0):.2f}"
        )
        if row.get("theme_ids"):
            lines.append(f"    题材：{' / '.join(row['theme_ids'])}")
        outcome_context_line = _format_outcome_context_line(row)
        if outcome_context_line:
            lines.append(f"    历史后验背景：{outcome_context_line}")
        if row.get("pool") == "exit" and row.get("source_decision_id"):
            lines.append(f"    来源决策：{row['source_decision_id']}")
        if row.get("pool") == "confirmation" and row.get("promoted_from_prewatch"):
            lines.append(
                "    "
                f"确认机会路径：第一池：候选池 / "
                f"{_display_candidate_setup(row.get('candidate_setup_type', row.get('prewatch_setup_type', '')))} / "
                f"候选池 {float(row.get('candidate_score', row.get('prewatch_score', 0.0)) or 0.0):.2f} 分"
            )
            observation_count = int(row.get("candidate_observation_count", row.get("prewatch_observation_count", 0)) or 0)
            alert_sent_count = int(row.get("candidate_alert_sent_count", row.get("prewatch_alert_sent_count", 0)) or 0)
            if observation_count > 0:
                lifecycle_text = f"累计观察 {observation_count} 次"
                if alert_sent_count > 0:
                    lifecycle_text += f"，观察卡 {alert_sent_count} 次"
                first_seen_at = str(row.get("candidate_first_seen_at", row.get("prewatch_first_seen_at") or "")).strip()
                last_seen_at = str(row.get("candidate_last_seen_at", row.get("prewatch_last_seen_at") or "")).strip()
                if first_seen_at or last_seen_at:
                    lifecycle_text += (
                        f"（首次 {format_beijing_minute(first_seen_at)} / 最近 {format_beijing_minute(last_seen_at)}）"
                    )
                lines.append(f"    观察轨迹：{lifecycle_text}")
            if row.get("source_decision_id"):
                lines.append(f"    候选池来源决策：{row['source_decision_id']}")
            promotion_reason = row.get("candidate_promotion_reason", row.get("prewatch_promotion_reason"))
            if promotion_reason:
                lines.append(f"    确认机会说明：{_modernize_display_text(str(promotion_reason))}")
        source_summary = row.get("source_decision_summary") or {}
        if row.get("pool") == "exit" and source_summary:
            lines.append(
                "    "
                f"来源动作：{_modernize_display_text(str(source_summary.get('pool_label', source_summary.get('pool', '-')) or '-'))} / "
                f"{_modernize_display_text(str(source_summary.get('action', '-') or '-'))} / "
                f"优先级 {source_summary.get('priority', '-')} / "
                f"综合 {float(source_summary.get('final_score', 0.0)):.2f}"
            )
    return lines


def _format_trade_path_lines(rows: list[dict[str, Any]]) -> list[str]:
    exit_rows = [
        row
        for row in rows
        if row.get("pool") == "exit" and row.get("source_decision_summary")
    ]
    if not exit_rows:
        return ["  本轮没有形成可追踪的确认 -> 持仓管理交易轨迹。"]
    lines: list[str] = []
    for row in exit_rows[:10]:
        source_summary = row.get("source_decision_summary") or {}
        symbol = row.get("symbol", "-")
        source_created_at = source_summary.get("created_at")
        exit_created_at = row.get("created_at")
        lines.append(
            f"  {symbol} / {_modernize_display_text(str(source_summary.get('action', '-') or '-'))} -> {_modernize_display_text(str(row.get('action', '-') or '-'))}"
        )
        lines.append(
            "    "
            f"起点：{_modernize_display_text(str(source_summary.get('pool_label', source_summary.get('pool', '-')) or '-'))} / "
            f"{_modernize_display_text(str(source_summary.get('action', '-') or '-'))} / 优先级 {source_summary.get('priority', '-')} / "
            f"综合 {float(source_summary.get('final_score', 0.0)):.2f}"
        )
        lines.append(
            "    "
            f"终点：{_modernize_display_text(str(row.get('pool_label', row.get('pool', '-')) or '-'))} / "
            f"{_modernize_display_text(str(row.get('action', '-') or '-'))} / 触发模式 {row.get('trigger_mode', '-')} / "
            f"综合 {float(row.get('final_score', 0.0)):.2f}"
        )
        if row.get("holding_days") is not None or row.get("realized_return") is not None:
            lines.append(
                "    "
                f"结果：历时 {_format_metric_value(row.get('holding_days'), suffix=' 天')} / "
                f"真实收益 {_format_metric_value(row.get('realized_return'), suffix='%')}"
            )
        if source_created_at or exit_created_at:
            lines.append(
                "    "
                f"时间：{format_beijing_minute(source_created_at)} -> {format_beijing_minute(exit_created_at)} / "
                f"历时 {_format_trade_path_duration(source_created_at, exit_created_at)}"
            )
    return lines


def _display_close_reason(value: str) -> str:
    return {
        "hit_invalidation": "触发失效位",
        "hit_take_profit": "触发止盈位",
        "exit_pool": "利润保护退出",
        "insufficient_lookahead": "仍在等待更多 bars",
        "window_complete": "窗口到期结算",
        "not_entered": "未成交结束",
    }.get(value, value)


def _format_outcome_context_line(row: dict[str, Any]) -> str | None:
    event_context = row.get("event_type_outcome_context") or {}
    pool_context = row.get("pool_outcome_context") or {}
    parts: list[str] = []
    pool_profit_hits = max(
        int(pool_context.get("take_profit_hits", 0) or 0),
        int(pool_context.get("exit_pool_hits", 0) or 0),
    )
    event_type = str(row.get("event_type") or "").strip()
    if event_context and event_type:
        parts.append(
            f"事件 {event_type} 近期待回写 {event_context.get('pending_count', 0)} 条，"
            f"T+3 均值 {event_context.get('avg_t_plus_3_return', '-')}"
            f"，已回写 {event_context.get('outcome_count', 0)} 条"
        )
    pool = str(row.get("pool") or "").strip()
    if pool_context and pool:
        parts.append(
            f"池子 {row.get('pool_label', pool)} T+3 均值 {pool_context.get('avg_t_plus_3_return', '-')}"
            f"，止盈 {pool_profit_hits} 条"
        )
    if not parts:
        return None
    return "；".join(parts) + "。"


def _format_funnel_rate(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "-"
    return f"{numerator / denominator * 100:.1f}%"


def _format_candidate_evaluation_summary_lines(summary: dict[str, Any]) -> list[str]:
    candidate_pool = _candidate_stage_summary(summary, "candidate_pool")
    confirmation = _candidate_stage_summary(summary, "confirmation")
    stage_rows = [row for row in (candidate_pool, confirmation) if row]
    if not stage_rows or all(int(row.get("total_count", 0) or 0) <= 0 for row in stage_rows):
        return ["  当前没有候选评估快照可供诊断。"]

    lines: list[str] = []
    for row in stage_rows:
        stage_label = str(row.get("stage_label") or row.get("stage") or "未分类")
        selected_label = (
            "进入观察"
            if _normalize_candidate_stage(str(row.get("stage") or "")) == "candidate_pool"
            else "形成确认机会"
        )
        lines.append(
            f"  {stage_label}：共 {int(row.get('total_count', 0) or 0)} 条，"
            f"{selected_label} {int(row.get('selected_count', 0) or 0)} 条，"
            f"未入选 {int(row.get('not_selected_count', 0) or 0)} 条，"
            f"拒绝 {int(row.get('rejected_count', 0) or 0)} 条，"
            f"异常 {int(row.get('error_count', 0) or 0)} 条。"
        )
        blockers = row.get("top_blocked_reasons") or []
        if blockers:
            parts = [
                f"{item.get('label', item.get('reason', '-'))} {int(item.get('count', 0) or 0)} 条"
                for item in blockers
            ]
            lines.append(f"    主要未通过原因：{' / '.join(parts)}")
        errors = row.get("top_error_reasons") or []
        if errors:
            parts = [
                f"{item.get('label', item.get('reason', '-'))} {int(item.get('count', 0) or 0)} 条"
                for item in errors
            ]
            lines.append(f"    主要异常：{' / '.join(parts)}")
    return lines


def _format_candidate_evaluation_trend_lines(summary: dict[str, Any]) -> list[str]:
    recent_window_days = int(summary.get("recent_window_days", 0) or 0)
    baseline_window_days = int(summary.get("baseline_window_days", 0) or 0)
    if recent_window_days <= 0 or baseline_window_days <= 0:
        return ["  当前窗口还没有足够的候选评估样本来比较近 7 天趋势。"]

    lines: list[str] = []
    has_content = False
    for stage_key in ("candidate_pool", "confirmation"):
        row = _candidate_stage_summary(summary, stage_key)
        reason_trends = list(row.get("reason_trends") or [])
        if not reason_trends:
            continue
        has_content = True
        lines.append(
            f"  {row.get('stage_label', row.get('stage', '-'))}：近 {recent_window_days} 天 {int(row.get('recent_total_count', 0) or 0)} 条，"
            f"当前窗口 {baseline_window_days} 天 {int(row.get('baseline_total_count', 0) or 0)} 条。"
        )
        for item in reason_trends:
            lines.append(
                "    "
                f"{item.get('label', item.get('reason', '-'))}：近 {recent_window_days} 天 {int(item.get('recent_count', 0) or 0)} 条，"
                f"{baseline_window_days} 天 {int(item.get('baseline_count', 0) or 0)} 条"
            )
    if not has_content:
        return ["  当前窗口还没有足够的候选评估样本来比较近 7 天趋势。"]
    return lines


def _build_recommendation_parameter_checklist(
    recommendation_details: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[str]:
    seen_keys: set[str] = set()
    items: list[str] = []
    for detail in recommendation_details:
        parameter_details = list((detail or {}).get("parameter_details") or [])
        adjustable_details = [
            item for item in parameter_details
            if str(item.get("direction") or "").strip() in {"high", "low"}
        ]
        for item in adjustable_details:
            key = str(item.get("key") or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            label = str(item.get("label") or key)
            direction_label = str(item.get("direction_label") or "先排查")
            items.append(f"{label}（{key}，{direction_label}）")
            if len(items) >= limit:
                return items
    return items


def _duration_days_between(start_value: Any, end_value: Any) -> float | None:
    start_dt = parse_datetime(start_value)
    end_dt = parse_datetime(end_value)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds() / 86400.0


def _format_pool_funnel_lines(
    *,
    prewatch_candidates: list[dict[str, Any]],
    run_detail: dict[str, Any] | None,
    decision_diagnostics: list[dict[str, Any]] | None,
    card_diagnostics: list[dict[str, Any]] | None,
) -> list[str]:
    candidate_count = len(prewatch_candidates)
    summary = run_detail.get("summary", {}) if run_detail else {}
    light_push_count = int(summary.get("prewatch_alerts_sent_count", 0) or 0)
    decision_rows = list(decision_diagnostics or [])
    promoted_confirmation_rows = [
        row
        for row in decision_rows
        if row.get("pool") == "confirmation" and row.get("promoted_from_prewatch")
    ]
    if not promoted_confirmation_rows and card_diagnostics:
        promoted_confirmation_rows = [
            row for row in card_diagnostics
            if row.get("promoted_from_prewatch")
        ]
    promoted_confirmation_count = len(promoted_confirmation_rows)
    promoted_after_light_push_count = sum(
        1
        for row in promoted_confirmation_rows
        if int(row.get("prewatch_alert_sent_count") or 0) > 0
    )
    promoted_without_light_push_count = max(
        promoted_confirmation_count - promoted_after_light_push_count,
        0,
    )
    exit_rows = [
        row for row in decision_rows
        if row.get("pool") in {"exit", "holding_management"}
    ]
    exit_from_promoted_count = sum(
        1
        for row in exit_rows
        if bool((row.get("source_decision_summary") or {}).get("promoted_from_prewatch"))
    )
    non_promoted_exit_count = max(len(exit_rows) - exit_from_promoted_count, 0)
    if (
        candidate_count <= 0
        and light_push_count <= 0
        and promoted_confirmation_count <= 0
        and exit_from_promoted_count <= 0
    ):
        return ["  本轮没有形成可计算的三池漏斗。"]
    lines = [f"  第一池候选：{candidate_count} 条"]
    lines.append(
        "  "
        f"观察卡：{light_push_count} 条 | 候选 -> 观察卡转化率 {_format_funnel_rate(light_push_count, candidate_count)}"
    )
    lines.append(
        "  "
        f"确认机会：{promoted_confirmation_count} 条 | 候选 -> 确认机会转化率 {_format_funnel_rate(promoted_confirmation_count, candidate_count)}"
    )
    lines.append(
        "  "
        f"观察卡后确认机会：{promoted_after_light_push_count} 条 | 观察卡 -> 确认机会转化率 {_format_funnel_rate(promoted_after_light_push_count, light_push_count)}"
    )
    if promoted_without_light_push_count > 0:
        lines.append(f"  另有 {promoted_without_light_push_count} 条未经过观察卡，直接从候选进入确认机会。")
    lines.append(
        "  "
        f"进入持仓管理退出：{exit_from_promoted_count} 条 | 确认机会 -> 持仓管理退出转化率 {_format_funnel_rate(exit_from_promoted_count, promoted_confirmation_count)}"
    )
    if non_promoted_exit_count > 0:
        lines.append(f"  另有 {non_promoted_exit_count} 条持仓管理记录来自非候选池确认机会链路。")
    return lines


def _format_simulation_funnel_lines(
    *,
    prewatch_candidates: list[dict[str, Any]],
    run_detail: dict[str, Any] | None,
    decision_diagnostics: list[dict[str, Any]] | None,
    card_diagnostics: list[dict[str, Any]] | None,
) -> list[str]:
    candidate_count = len(prewatch_candidates)
    summary = run_detail.get("summary", {}) if run_detail else {}
    light_push_count = int(summary.get("prewatch_alerts_sent_count", 0) or 0)
    decision_rows = list(decision_diagnostics or [])
    promoted_confirmation_rows = [
        row
        for row in decision_rows
        if row.get("pool") == "confirmation" and row.get("promoted_from_prewatch")
    ]
    if not promoted_confirmation_rows and card_diagnostics:
        promoted_confirmation_rows = [
            row for row in card_diagnostics
            if row.get("promoted_from_prewatch")
        ]
    promoted_confirmation_count = len(promoted_confirmation_rows)
    simulated_entry_rows = [
        row for row in promoted_confirmation_rows
        if bool(row.get("entered"))
    ]
    simulated_entry_count = len(simulated_entry_rows)
    pending_entry_count = max(promoted_confirmation_count - simulated_entry_count, 0)
    holding_management_transition_count = sum(
        1 for row in simulated_entry_rows
        if str(row.get("close_reason") or "").strip() == "exit_pool"
    )
    simulated_completed_exit_rows = [
        row
        for row in simulated_entry_rows
        if str(row.get("close_reason") or "").strip() not in {"", "insufficient_lookahead"}
    ]
    simulated_completed_exit_count = len(simulated_completed_exit_rows)
    simulated_open_count = sum(
        1 for row in simulated_entry_rows
        if str(row.get("close_reason") or "").strip() == "insufficient_lookahead"
    )
    confirmation_to_entry_days = [
        duration
        for duration in (
            _duration_days_between(row.get("created_at"), row.get("entered_at"))
            for row in simulated_entry_rows
        )
        if duration is not None
    ]
    entry_to_completed_exit_days = [
        float(row.get("holding_days"))
        for row in simulated_completed_exit_rows
        if row.get("holding_days") is not None
    ]
    entry_timing_by_action: list[str] = []
    completed_exit_timing_by_action: list[str] = []
    for action in ("试探建仓", "确认做多"):
        action_entry_days = [
            duration
            for duration in (
                _duration_days_between(row.get("created_at"), row.get("entered_at"))
                for row in simulated_entry_rows
                if str(row.get("action") or "").strip() == action
            )
            if duration is not None
        ]
        if action_entry_days:
            avg_days = sum(action_entry_days) / len(action_entry_days)
            sorted_days = sorted(action_entry_days)
            middle_index = len(sorted_days) // 2
            if len(sorted_days) % 2 == 0:
                median_days = (sorted_days[middle_index - 1] + sorted_days[middle_index]) / 2
            else:
                median_days = sorted_days[middle_index]
            entry_timing_by_action.append(
                f"{action}：样本 {len(action_entry_days)} 条，平均等待 {_format_metric_value(avg_days, suffix=' 天')}，"
                f"中位数 {_format_metric_value(median_days, suffix=' 天')}"
            )
        action_completed_exit_days = [
            float(row.get("holding_days"))
            for row in simulated_completed_exit_rows
            if str(row.get("action") or "").strip() == action and row.get("holding_days") is not None
        ]
        if action_completed_exit_days:
            avg_days = sum(action_completed_exit_days) / len(action_completed_exit_days)
            sorted_days = sorted(action_completed_exit_days)
            middle_index = len(sorted_days) // 2
            if len(sorted_days) % 2 == 0:
                median_days = (sorted_days[middle_index - 1] + sorted_days[middle_index]) / 2
            else:
                median_days = sorted_days[middle_index]
            completed_exit_timing_by_action.append(
                f"{action}：样本 {len(action_completed_exit_days)} 条，平均持有 {_format_metric_value(avg_days, suffix=' 天')}，"
                f"中位数 {_format_metric_value(median_days, suffix=' 天')}"
            )
    if (
        candidate_count <= 0
        and light_push_count <= 0
        and promoted_confirmation_count <= 0
        and simulated_entry_count <= 0
    ):
        return ["  本轮还没有形成可计算的完整模拟闭环。"]
    exit_pool_completed_count = sum(
        1 for row in simulated_completed_exit_rows
        if str(row.get("close_reason") or "").strip() == "exit_pool"
    )
    invalidation_completed_count = sum(
        1 for row in simulated_completed_exit_rows
        if str(row.get("close_reason") or "").strip() == "hit_invalidation"
    )
    window_completed_count = sum(
        1 for row in simulated_completed_exit_rows
        if str(row.get("close_reason") or "").strip() == "window_complete"
    )
    take_profit_completed_count = sum(
        1 for row in simulated_completed_exit_rows
        if str(row.get("close_reason") or "").strip() == "hit_take_profit"
    )
    lines = [f"  候选：{candidate_count} 条"]
    lines.append(
        "  "
        f"观察卡：{light_push_count} 条 | 候选 -> 观察卡转化率 {_format_funnel_rate(light_push_count, candidate_count)}"
    )
    lines.append(
        "  "
        f"确认机会：{promoted_confirmation_count} 条 | 候选 -> 确认机会转化率 {_format_funnel_rate(promoted_confirmation_count, candidate_count)}"
    )
    lines.append(
        "  "
        f"模拟成交：{simulated_entry_count} 条 | 确认机会 -> 模拟成交转化率 {_format_funnel_rate(simulated_entry_count, promoted_confirmation_count)}"
    )
    lines.append(
        "  "
        f"进入持仓管理退出：{holding_management_transition_count} 条 | 模拟成交 -> 持仓管理退出转化率 {_format_funnel_rate(holding_management_transition_count, simulated_entry_count)}"
    )
    lines.append(
        "  "
        f"模拟退出完成：{simulated_completed_exit_count} 条 | 模拟成交 -> 模拟退出完成率 {_format_funnel_rate(simulated_completed_exit_count, simulated_entry_count)}"
    )
    if confirmation_to_entry_days:
        avg_days_to_entry = sum(confirmation_to_entry_days) / len(confirmation_to_entry_days)
        sorted_days_to_entry = sorted(confirmation_to_entry_days)
        middle_index = len(sorted_days_to_entry) // 2
        if len(sorted_days_to_entry) % 2 == 0:
            median_days_to_entry = (sorted_days_to_entry[middle_index - 1] + sorted_days_to_entry[middle_index]) / 2
        else:
            median_days_to_entry = sorted_days_to_entry[middle_index]
        lines.append(
            "  "
            f"确认机会 -> 模拟成交：平均等待 {_format_metric_value(avg_days_to_entry, suffix=' 天')}，"
            f"中位数 {_format_metric_value(median_days_to_entry, suffix=' 天')}"
        )
    if entry_to_completed_exit_days:
        avg_days_to_completed_exit = sum(entry_to_completed_exit_days) / len(entry_to_completed_exit_days)
        sorted_days_to_completed_exit = sorted(entry_to_completed_exit_days)
        middle_index = len(sorted_days_to_completed_exit) // 2
        if len(sorted_days_to_completed_exit) % 2 == 0:
            median_days_to_completed_exit = (
                sorted_days_to_completed_exit[middle_index - 1] + sorted_days_to_completed_exit[middle_index]
            ) / 2
        else:
            median_days_to_completed_exit = sorted_days_to_completed_exit[middle_index]
        lines.append(
            "  "
            f"模拟成交 -> 模拟退出完成：平均持有 {_format_metric_value(avg_days_to_completed_exit, suffix=' 天')}，"
            f"中位数 {_format_metric_value(median_days_to_completed_exit, suffix=' 天')}"
        )
    for line in entry_timing_by_action:
        lines.append(f"  按动作看成交等待：{line}")
    for line in completed_exit_timing_by_action:
        lines.append(f"  按动作看持有时长：{line}")
    if pending_entry_count > 0:
        lines.append(f"  另有 {pending_entry_count} 条确认机会尚未形成模拟成交。")
    if simulated_open_count > 0:
        lines.append(f"  另有 {simulated_open_count} 条已模拟成交，但当前仍在持仓中。")
    if simulated_completed_exit_count > 0:
        parts = []
        if exit_pool_completed_count > 0:
            parts.append(f"利润保护退出 {exit_pool_completed_count} 条")
        if take_profit_completed_count > 0:
            parts.append(f"止盈位 {take_profit_completed_count} 条")
        if invalidation_completed_count > 0:
            parts.append(f"失效退出 {invalidation_completed_count} 条")
        if window_completed_count > 0:
            parts.append(f"窗口结算 {window_completed_count} 条")
        if parts:
            lines.append(f"  其中：{' / '.join(parts)}")
    return lines


def format_run_review(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
    llm_usage_report: dict[str, Any] | None = None,
) -> str:
    health = summarize_run_health(run_detail, strategy_report, source_health, card_diagnostics, decision_diagnostics)
    lines = ["运行复盘："]
    candidate_pool_candidates = (
        (
            run_detail.get("summary", {}).get("candidate_pool_candidates")
            or run_detail.get("summary", {}).get("prewatch_candidates", [])
        )
        if run_detail
        else []
    )
    runtime_watchlist = (
        run_detail.get("config_snapshot", {}).get("runtime_config", {}).get("watchlist", {})
        if run_detail
        else {}
    )
    symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(runtime_watchlist)
    theme_display_name_map = build_theme_display_name_map_from_watchlist_payload(runtime_watchlist)
    if run_detail is None:
        lines.append("运行：缺失")
    else:
        summary = run_detail["summary"]
        candidate_optional_alert_symbols = summary.get(
            "candidate_optional_alert_symbols",
            summary.get("prewatch_alert_symbols", []),
        )
        candidate_optional_alerts_sent = int(
            summary.get("candidate_optional_alerts_sent_count", summary.get("prewatch_alerts_sent_count", 0))
        )
        holding_management_cards = summary.get("holding_management_cards") or summary.get("exit_pool_cards", [])
        high_priority_cards = sum(int(row.get("high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        sent_high_priority = sum(int(row.get("sent_high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        lines.append("结论摘要：")
        lines.append(
            "  "
            f"本轮状态 {_display_run_status(run_detail['status'])}，健康判断为 {health['status']}；"
            f"共处理 {summary.get('events_processed', 0)} 个事件，生成 {summary.get('cards_generated', 0)} 张卡片，发送 {summary.get('alerts_sent', 0)} 条提醒，识别 {len(candidate_pool_candidates)} 个候选池标的，并发出 {candidate_optional_alerts_sent} 张观察卡。"
        )
        if holding_management_cards:
            breakdown = _format_holding_management_breakdown(holding_management_cards)
            breakdown_suffix = f"（{breakdown}）" if breakdown else ""
            lines.append(
                f"  另外有 {len(holding_management_cards)} 个标的进入持仓管理{breakdown_suffix}，建议优先处理已有仓位。"
            )
        if high_priority_cards > 0:
            lines.append(
                f"  本轮出现 {high_priority_cards} 张高优先级卡片，其中实际发送 {sent_high_priority} 条，建议优先查看对应卡片。"
            )
        else:
            lines.append("  本轮没有高优先级卡片，当前结果更适合候选观察而不是强行动信号。")
        lines.append("关键指标：")
        lines.append(f"  运行 ID：{run_detail['run_id']}")
        lines.append(
            "  "
            f"名称：{run_detail.get('run_name') or '-'} | 备注：{run_detail.get('note') or '-'} | 用时：{_format_run_duration(run_detail.get('started_at'), run_detail.get('finished_at'))}"
        )
        lines.append(
            "  "
            f"开始时间：{format_beijing_minute(run_detail.get('started_at'))} | 结束时间：{format_beijing_minute(run_detail.get('finished_at'))}"
        )
        lines.append(
            "  "
            f"失败计数：抽取 {summary.get('extraction_failures', 0)} / 行情 {summary.get('market_data_failures', 0)} / "
            f"评分 {summary.get('scoring_failures', 0)} / 通知 {summary.get('notification_failures', 0)}"
        )
        lines.append(
            "  "
            f"过滤计数：跨源去重 {summary.get('skipped_cross_source_duplicate', 0)} / 重复事件 {summary.get('skipped_duplicate_event_id', 0)} / "
            f"观察池外 {summary.get('skipped_out_of_watchlist', 0)}"
        )
    lines.append("运行健康：")
    for line in health["line_items"]:
        lines.append(f"  {line}")
    lines.append("LLM 用量摘要：")
    lines.extend(_format_llm_usage_snapshot_lines(llm_usage_report))
    lines.append("三池漏斗：")
    lines.extend(
        _format_pool_funnel_lines(
            prewatch_candidates=candidate_pool_candidates,
            run_detail=run_detail,
            decision_diagnostics=decision_diagnostics,
            card_diagnostics=card_diagnostics,
        )
    )
    lines.append("完整模拟闭环：")
    lines.extend(
        _format_simulation_funnel_lines(
            prewatch_candidates=candidate_pool_candidates,
            run_detail=run_detail,
            decision_diagnostics=decision_diagnostics,
            card_diagnostics=card_diagnostics,
        )
    )
    lines.append("候选诊断：")
    lines.extend(_format_candidate_evaluation_summary_lines(run_detail.get("candidate_evaluation_summary") or {}))
    lines.append("新增观察标的：")
    sent_symbols = candidate_optional_alert_symbols if run_detail else []
    lines.extend(_format_new_observation_lines(candidate_pool_candidates, sent_symbols))
    lines.append("机会概览：")
    event_types = strategy_report.get("event_type_performance", [])
    if not event_types:
        lines.append("  本轮没有形成可统计的事件类型。")
    for row in event_types:
        lines.append(
            f"  {_display_event_type(row['event_type'])}：生成 {row['card_count']} 张卡片，平均综合分 {row['avg_final_score']}，高优先级 {row['high_priority_count']} 张。"
        )
    lines.append("提醒结果：")
    alert_volume = strategy_report.get("alert_volume", [])
    diagnostics = card_diagnostics or []
    if not alert_volume:
        lines.append("  本轮没有提醒数据。")
    for row in alert_volume:
        lines.append(
            f"  共 {row['total_alerts']} 条，其中已发送 {row['sent_alerts']} 条，压制 {row['suppressed_alerts']} 条，高优先级卡片 {row['high_priority_alerts']} 条，高优先级已发送 {row.get('sent_high_priority_alerts', 0)} 条。"
        )
    lines.append("提醒压制诊断：")
    lines.extend(_format_alert_suppression_lines(diagnostics, run_detail.get("candidate_evaluation_summary") or {}))
    lines.append("最终推送卡片：")
    sent_cards = [row for row in diagnostics if row.get("sent")]
    lines.extend(_format_push_card_lines(sent_cards))
    lines.append("确认机会：")
    lines.extend(_format_promoted_confirmation_lines(diagnostics))
    lines.append("题材链路：")
    lines.extend(
        _format_theme_linkage_lines(
            build_theme_snapshot_rows(
                symbol_theme_map=symbol_theme_map,
                theme_display_name_map=theme_display_name_map,
                card_diagnostics=diagnostics,
                prewatch_candidates=candidate_pool_candidates,
            )
        )
    )
    lines.append("第一池：候选池：")
    lines.extend(_format_candidate_pool_lines(candidate_pool_candidates, symbol_theme_map=symbol_theme_map))
    lines.append("观察卡：")
    lines.extend(_format_candidate_optional_alert_lines(candidate_pool_candidates, sent_symbols))
    lines.append("持仓管理：")
    holding_management_cards = (
        run_detail.get("summary", {}).get("holding_management_cards")
        or run_detail.get("summary", {}).get("exit_pool_cards", [])
        if run_detail
        else []
    )
    lines.extend(_format_holding_management_lines(holding_management_cards))
    lines.append("交易轨迹：")
    lines.extend(_format_trade_path_lines(decision_diagnostics or []))
    lines.append("决策记录：")
    lines.extend(_format_decision_lines(decision_diagnostics or []))
    lines.append("数据源状态：")
    if not source_health:
        lines.append("  本轮没有数据源健康信息。")
    for row in source_health:
        latency = row["latency_ms"] if row["latency_ms"] is not None else "-"
        if row["status"] == "healthy":
            lines.append(f"  {row['source_name']}：正常，检查时间 {format_beijing_minute(row['checked_at'])}。{row['detail']}")
        elif _is_external_connectivity_issue(str(row["detail"])):
            lines.append(
                f"  {_external_connectivity_hint(row['source_name'])} 检查时间 {format_beijing_minute(row['checked_at'])}。"
            )
        else:
            lines.append(f"  {row['source_name']}：异常，检查时间 {format_beijing_minute(row['checked_at'])}，latency_ms={latency}，{row['detail']}")
    lines.append("卡片解读：")
    if not diagnostics:
        lines.append("  本轮没有生成卡片。")
    for row in diagnostics:
        lines.append(_format_card_header(row))
        lines.extend(_format_card_action_lines(row))
        lines.extend(_format_card_delivery_lines(row))
        lines.append(f"    解读：{_card_takeaway(row)}")
        score_parts: list[str] = []
        if row.get("event_score") is not None:
            event_text = f"事件 {row.get('event_score')}"
            if row.get("event_threshold") is not None:
                event_text += f"/{row.get('event_threshold')}"
            if row.get("event_margin") is not None:
                event_text += f" ({float(row.get('event_margin')):+.2f})"
            score_parts.append(event_text)
        if row.get("market_score") is not None:
            market_text = f"市场 {row.get('market_score')}"
            if row.get("market_threshold") is not None:
                market_text += f"/{row.get('market_threshold')}"
            if row.get("market_margin") is not None:
                market_text += f" ({float(row.get('market_margin')):+.2f})"
            score_parts.append(market_text)
        if row.get("final_score") is not None:
            final_text = f"综合 {row.get('final_score')}"
            if row.get("priority_threshold") is not None:
                final_text += f"/{row.get('priority_threshold')}"
            if row.get("priority_margin") is not None:
                final_text += f" ({float(row.get('priority_margin')):+.2f})"
            score_parts.append(final_text)
        if score_parts:
            lines.append("    分数：" + "，".join(score_parts))
        if not row.get("market_data_complete", True):
            lines.append(f"    行情状态：{row.get('market_data_note') or '行情快照暂不可用，未自动生成价格计划。'}")
        elif row.get("entry_range") and row.get("take_profit_range"):
            entry = row["entry_range"]
            take_profit = row["take_profit_range"]
            lines.append(
                f"    价格：入场 {entry['low']}-{entry['high']}，止盈 {take_profit['low']}-{take_profit['high']}，失效价 {row.get('invalidation_level', '-')}"
            )
        if row.get("invalidation_reason"):
            lines.append(f"    失效条件：{row['invalidation_reason']}")
        if row.get("reason_to_watch"):
            lines.append(f"    关注理由：{row['reason_to_watch']}")
        lines.extend(_format_link_lines(row.get("source_refs")))
    return "\n".join(lines)


def format_run_comparison(items: list[dict[str, Any]]) -> str:
    if not items:
        return "No runs to compare."
    lines = ["Run Comparison:"]
    for item in items:
        run = item["run"]
        summary = run["summary"] if run else {}
        strategy = item["strategy_report"]
        top_event = strategy["event_type_performance"][0]["event_type"] if strategy["event_type_performance"] else "-"
        avg_score = strategy["event_type_performance"][0]["avg_final_score"] if strategy["event_type_performance"] else "-"
        health = item["source_health"]
        unhealthy = sum(1 for row in health if row["status"] != "healthy")
        alert_total = strategy["alert_volume"][0]["total_alerts"] if strategy["alert_volume"] else 0
        run_name = "-"
        status = "-"
        config_summary = "-"
        if run:
            run_name = run.get("run_name") or "-"
            status = run["status"]
            config_summary = _config_summary(run.get("config_snapshot", {}))
        lines.append(
            f"  {item['run_id']}: name={run_name} status={status} "
            f"events={summary.get('events_processed', 0)} cards={summary.get('cards_generated', 0)} "
            f"alerts={summary.get('alerts_sent', 0)} top_event={top_event} top_avg_score={avg_score} "
            f"latest_alert_total={alert_total} unhealthy_sources={unhealthy} cfg={config_summary}"
        )
    return "\n".join(lines)


def format_batch_replay(payload: dict[str, Any]) -> str:
    items = payload.get("experiments", [])
    if not items:
        return "策略赛马结果：当前没有可比较实验。"
    lines = ["策略赛马结果："]
    if payload.get("batch_id"):
        lines.append(f"批次ID：{payload['batch_id']}")
    if payload.get("generated_at"):
        lines.append(f"生成时间：{payload['generated_at']}")
    if payload.get("manifest_path"):
        lines.append(f"清单文件：{payload['manifest_path']}")
    if payload.get("report_path"):
        lines.append(f"报告文件：{payload['report_path']}")
    lines.append("实验结果：")
    for index, item in enumerate(items, start=1):
        if item["status"] != "success":
            lines.append(
                f"  {index}. {item['name']}：状态={item['status']}，错误={item.get('error', '-')}"
            )
            continue
        metrics = item.get("metrics", {})
        lines.append(
            f"  {index}. {item['name']}：状态={item['status']}，run_id={item.get('run_id', '-')}，"
            f"决策 {metrics.get('decision_count', 0)}，已成交 {metrics.get('entered_count', 0)}，"
            f"平均真实收益 {metrics.get('avg_realized_return')}%，胜率 {metrics.get('win_rate')}%，"
            f"平均最大回撤 {metrics.get('avg_max_drawdown')}%，主导事件 {item.get('top_event', '-')}，"
            f"配置 {item.get('config_summary', '-')}，数据库 {item.get('db_path', '-')}"
        )
        for card in item.get("card_diagnostics", []):
            lines.append(
                "     "
                f"{card['symbol']} {card['horizon']} "
                f"事件阈值差={card['event_margin']:+.2f} "
                f"市场阈值差={card['market_margin']:+.2f} "
                f"优先级阈值差={card['priority_margin']:+.2f} "
                f"优先级={card['priority']}"
            )
    lines.append("排序：")
    for index, row in enumerate(payload.get("ranking", []), start=1):
        lines.append(
            f"  {index}. {row['name']}：状态={row['status']}，平均真实收益={row['avg_realized_return']}%，"
            f" 胜率={row['win_rate']}%，平均最大回撤={row['avg_max_drawdown']}%，"
            f" 盈亏比={row['profit_loss_ratio']}，T+7={row['avg_t_plus_7_return']}%，配置={row['config_summary']}"
        )
    recommendation = payload.get("recommendation")
    if recommendation:
        lines.append("推荐策略：")
        if "decision_count" in recommendation:
            lines.append(
                f"  {recommendation['name']}：决策={recommendation['decision_count']}，"
                f"已成交={recommendation['entered_count']}，止盈退出={recommendation['take_profit_exit_count']}，"
                f"失效退出={recommendation['invalidation_exit_count']}，平均真实收益={recommendation['avg_realized_return']}%，"
                f"胜率={recommendation['win_rate']}%，平均最大回撤={recommendation['avg_max_drawdown']}%，"
                f"盈亏比={recommendation['profit_loss_ratio']}，T+7={recommendation['avg_t_plus_7_return']}%，"
                f"配置={recommendation['config_summary']}"
            )
        else:
            lines.append(
                f"  {recommendation['name']}：提醒={recommendation.get('alerts_sent')}，"
                f"卡片={recommendation.get('cards_generated')}，事件={recommendation.get('events_processed')}，"
                f"最近市场阈值差={recommendation.get('closest_market_margin')}，"
                f"最近优先级阈值差={recommendation.get('closest_priority_margin')}，"
                f"配置={recommendation['config_summary']}"
            )
        lines.append(f"  推荐原因：{recommendation['reason']}")
    summary = payload.get("summary")
    if summary and summary.get("line_items"):
        lines.append("摘要：")
        for line in summary["line_items"]:
            lines.append(f"  {line}")
    recommendation = payload.get("recommendation")
    if recommendation:
        lines.append("胜者快照：")
        top_event = next(
            (
                item.get("top_event", "-")
                for item in payload.get("experiments", [])
                if item.get("name") == recommendation.get("name")
            ),
            "-",
        )
        lines.append(
            f"  {recommendation['name']}：配置={recommendation['config_summary']}，"
            f"平均真实收益={recommendation.get('avg_realized_return')}%，胜率={recommendation.get('win_rate')}%，"
            f"平均最大回撤={recommendation.get('avg_max_drawdown')}%，主导事件={top_event}"
        )
    next_step = payload.get("next_step")
    if next_step and next_step.get("line_items"):
        lines.append(f"下一步：{next_step['title']}")
        for line in next_step["line_items"]:
            lines.append(f"  {line}")
    return "\n".join(lines)


def format_batch_comparison(payload: dict[str, Any]) -> str:
    lines = ["Batch Comparison:"]
    if payload.get("left_batch_id"):
        lines.append(f"Left Batch: {payload['left_batch_id']}")
    if payload.get("right_batch_id"):
        lines.append(f"Right Batch: {payload['right_batch_id']}")
    if payload.get("report_path"):
        lines.append(f"Report: {payload['report_path']}")
    if payload.get("left_manifest_path"):
        lines.append(f"Left Manifest: {payload['left_manifest_path']}")
    if payload.get("right_manifest_path"):
        lines.append(f"Right Manifest: {payload['right_manifest_path']}")
    if payload.get("left_recommendation"):
        lines.append(
            f"Left Recommendation: {payload['left_recommendation']['name']} "
            f"({payload['left_recommendation']['config_summary']})"
        )
    if payload.get("right_recommendation"):
        lines.append(
            f"Right Recommendation: {payload['right_recommendation']['name']} "
            f"({payload['right_recommendation']['config_summary']})"
        )
    summary = payload.get("summary")
    if summary and summary.get("line_items"):
        lines.append("Summary:")
        for line in summary["line_items"]:
            lines.append(f"  {line}")
    items = payload.get("comparisons", [])
    if not items:
        lines.append("(no experiment comparisons)")
        return "\n".join(lines)
    for item in items:
        lines.append(
            f"  {item['name']}: "
            f"alerts {item['left_alerts_sent']} -> {item['right_alerts_sent']} ({item['alerts_delta']:+d}), "
            f"cards {item['left_cards_generated']} -> {item['right_cards_generated']} ({item['cards_delta']:+d}), "
            f"events {item['left_events_processed']} -> {item['right_events_processed']} ({item['events_delta']:+d}), "
            f"market_margin_delta={item['market_margin_delta']} "
            f"priority_margin_delta={item['priority_margin_delta']}"
        )
        lines.append(
            f"     left={item['left_status']} {item['left_config_summary']} | "
            f"right={item['right_status']} {item['right_config_summary']}"
        )
    next_step = payload.get("next_step")
    if next_step and next_step.get("line_items"):
        lines.append(f"Next Step: {next_step['title']}")
        for line in next_step["line_items"]:
            lines.append(f"  {line}")
    return "\n".join(lines)


def format_batch_index(payload: dict[str, Any]) -> str:
    items = payload.get("batches", [])
    if not items:
        return "No batch manifests found."
    lines = ["Batch Index:"]
    summary = payload.get("summary")
    if summary and summary.get("line_items"):
        lines.append("Summary:")
        for line in summary["line_items"]:
            lines.append(f"  {line}")
    for index, item in enumerate(items, start=1):
        lines.append(
            f"  {index}. {item['batch_id'] or '-'} generated_at={item['generated_at'] or '-'} "
            f"experiments={item['experiment_count']} recommended={item['recommendation_name'] or '-'} "
            f"cfg={item['recommendation_config']} next={item['next_step_title'] or '-'}"
        )
        winner = item.get("winner_snapshot", {})
        if winner.get("available"):
            lines.append(
                f"     winner={winner['name']} alerts={winner['alerts_sent']} cards={winner['cards_generated']} "
                f"events={winner['events_processed']} top_event={winner['top_event'] or '-'} "
                f"market_margin={winner['closest_market_margin']} priority_margin={winner['closest_priority_margin']}"
            )
        else:
            lines.append("     winner_snapshot=unavailable (legacy manifest or missing recommendation)")
        lines.append(f"     manifest={item['manifest_path'] or '-'}")
        if item.get("report_path"):
            lines.append(f"     report={item['report_path']}")
    return "\n".join(lines)
