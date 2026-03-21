from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable

from .decision_engines.mappers import build_delivery_view_from_record
from .theme_linkage import (
    build_prewatch_peer_map,
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


def _display_prewatch_setup(setup_type: str) -> str:
    return {
        "breakout_watch": "突破预热",
        "pullback_watch": "回踩蓄势",
        "relative_strength_watch": "相对强势",
    }.get(setup_type, setup_type)


def _display_prewatch_trigger_mode(trigger_mode: str) -> str:
    return {
        "event": "事件预热",
        "structure": "结构预热",
    }.get(trigger_mode, trigger_mode or "结构预热")


def _display_pool(pool: str) -> str:
    return {
        "prewatch": "预备池",
        "confirmation": "确认池",
        "exit": "兑现池",
    }.get(pool, pool)


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


def _config_summary(config_snapshot: dict[str, Any]) -> str:
    settings = config_snapshot.get("settings", {})
    horizons = settings.get("horizons", {})
    swing = horizons.get("swing", {})
    position = horizons.get("position", {})
    event_floor = settings.get("event_score_threshold")
    dedup_hours = settings.get("cross_source_dedup_hours")
    if event_floor is None and dedup_hours is None and not horizons:
        return "-"
    return (
        f"E{event_floor}/"
        f"S{swing.get('market_score_threshold', '-')}-{swing.get('priority_threshold', '-')}/"
        f"P{position.get('market_score_threshold', '-')}-{position.get('priority_threshold', '-')}/"
        f"D{dedup_hours}"
    )


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
            "readiness_note": "完整样本还太少，只适合轻推和观察，不适合直接放大权重。",
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
            f"已进场 {row.get('entered_count', 0)} 条，"
            f"止盈 {row.get('take_profit_exit_count', 0)} 条，"
            f"失效 {row.get('invalidation_exit_count', 0)} 条，"
            f"复盘窗口结算 {row.get('window_complete_count', 0)} 条，"
            f"平均真实收益 {_format_metric_value(row.get('avg_realized_return'), suffix='%')}，"
            f"胜率 {_format_rate_value(row.get('win_rate'))}"
        )
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
        exit_price = _format_metric_value(row.get("exit_price"))
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
        exit_price = _format_metric_value(row.get("exit_price"))
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
        lines.insert(0, "当前更适合观察和轻推，而不是把后验结果直接翻译成强执行动作。")
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
        elif strongest_pool["pool"] == "prewatch":
            lines.append("Prewatch outcomes are currently leading, but keep sizing smaller until more signals mature into confirmation.")

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
) -> dict[str, Any]:
    prewatch_candidates = []
    if run_detail is not None:
        prewatch_candidates = run_detail.get("summary", {}).get("prewatch_candidates", [])
    return {
        "run": run_detail,
        "strategy_report": strategy_report,
        "source_health": source_health,
        "card_diagnostics": card_diagnostics or [],
        "decision_diagnostics": decision_diagnostics or [],
        "prewatch_candidates": prewatch_candidates,
    }


def serialize_run_comparison(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"runs": items}


def serialize_batch_replay(
    items: list[dict[str, Any]],
    recommendation_preferences: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ranking = []
    for item in items:
        summary = item.get("summary", {})
        ranking.append(
            {
                "name": item["name"],
                "run_id": item.get("run_id", ""),
                "status": item["status"],
                "alerts_sent": summary.get("alerts_sent", 0),
                "cards_generated": summary.get("cards_generated", 0),
                "events_processed": summary.get("events_processed", 0),
                "failures": item.get("failures", 0),
                "config_summary": item.get("config_summary", "-"),
                "db_path": item.get("db_path", ""),
                "closest_market_margin": item.get("closest_market_margin"),
                "closest_priority_margin": item.get("closest_priority_margin"),
            }
        )
    ranking.sort(
        key=lambda row: (
            0 if row["status"] == "success" else 1,
            -row["alerts_sent"],
            -row["cards_generated"],
            -row["events_processed"],
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
        summary = item.get("summary", {})
        if item.get("status") != "success":
            continue
        if summary.get("events_processed", 0) <= 0:
            continue
        market_margin = item.get("closest_market_margin")
        priority_margin = item.get("closest_priority_margin")
        non_negative_market = market_margin is not None and market_margin >= 0
        score = (
            (summary.get("alerts_sent", 0) * weights["alerts_sent"])
            + (summary.get("cards_generated", 0) * weights["cards_generated"])
            + (summary.get("events_processed", 0) * weights["events_processed"])
            - (item.get("failures", 0) * weights["failures"])
        )
        if market_margin is not None:
            score += (
                (1.0 / (1.0 + max(market_margin, 0.0))) * weights["strictness"]
                if market_margin >= 0
                else market_margin * weights["strictness"]
            )
        if priority_margin is not None:
            score += (1.0 / (1.0 + abs(priority_margin))) * weights["priority_proximity"]
        candidates.append(
            (
                (
                    -round(score, 8),
                    0 if non_negative_market else 1,
                    market_margin if non_negative_market else float("inf"),
                    abs(priority_margin) if priority_margin is not None else float("inf"),
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
    chosen_score = candidates[0][2]
    summary = chosen.get("summary", {})
    same_output = [
        item
        for _, item, _ in candidates
        if item.get("summary", {}).get("alerts_sent", 0) == summary.get("alerts_sent", 0)
        and item.get("summary", {}).get("cards_generated", 0) == summary.get("cards_generated", 0)
        and item.get("summary", {}).get("events_processed", 0) == summary.get("events_processed", 0)
        and item.get("failures", 0) == chosen.get("failures", 0)
    ]
    reason = "Produced the strongest batch output."
    if len(same_output) > 1 and chosen.get("closest_market_margin") is not None:
        reason = (
            "Matched the top output and ranked best under the configured strictness preference."
        )
    elif chosen.get("closest_priority_margin") is not None and chosen["closest_priority_margin"] > -5:
        reason = "Produced the strongest batch output and stayed relatively close to high-priority cutoff."
    return {
        "name": chosen["name"],
        "run_id": chosen.get("run_id", ""),
        "config_summary": chosen.get("config_summary", "-"),
        "reason": reason,
        "score": chosen_score,
        "weights": weights,
        "alerts_sent": summary.get("alerts_sent", 0),
        "cards_generated": summary.get("cards_generated", 0),
        "events_processed": summary.get("events_processed", 0),
        "closest_market_margin": chosen.get("closest_market_margin"),
        "closest_priority_margin": chosen.get("closest_priority_margin"),
    }


def _recommendation_weights(preferences: dict[str, Any] | None) -> dict[str, float]:
    prefs = preferences or {}
    return {
        "alerts_sent": float(prefs.get("alerts_sent", 4.0)),
        "cards_generated": float(prefs.get("cards_generated", 2.0)),
        "events_processed": float(prefs.get("events_processed", 1.0)),
        "strictness": float(prefs.get("strictness", 0.75)),
        "priority_proximity": float(prefs.get("priority_proximity", 0.25)),
        "failures": float(prefs.get("failures", 3.0)),
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
    alerts = {item["name"]: item.get("summary", {}).get("alerts_sent", 0) for item in successful}
    cards = {item["name"]: item.get("summary", {}).get("cards_generated", 0) for item in successful}
    margins = {
        item["name"]: item.get("closest_market_margin")
        for item in successful
        if item.get("closest_market_margin") is not None
    }
    recommendation = _recommend_batch(items, preferences)
    line_items = []
    top_ranked = ranking[0] if ranking else None
    if top_ranked:
        line_items.append(
            f"Top output: {top_ranked['name']} with {top_ranked['alerts_sent']} alerts and {top_ranked['cards_generated']} cards."
        )
    if len(set(alerts.values())) == 1 and len(set(cards.values())) == 1:
        line_items.append("Output is tied across experiments on alerts and cards.")
    else:
        best_alerts = max(alerts.values())
        leaders = sorted(name for name, value in alerts.items() if value == best_alerts)
        line_items.append(f"Alert leader: {', '.join(leaders)} at {best_alerts} alerts.")
    if margins:
        strict_name, strict_margin = min(margins.items(), key=lambda pair: pair[1])
        loose_name, loose_margin = max(margins.items(), key=lambda pair: pair[1])
        line_items.append(
            f"Closest passing setup: {strict_name} with market margin {strict_margin:+.2f}; loosest is {loose_name} at {loose_margin:+.2f}."
        )
    if recommendation:
        line_items.append(
            f"Recommended setup: {recommendation['name']} because {recommendation['reason'].rstrip('.').lower()}."
        )
    return {
        "line_items": line_items,
        "has_meaningful_difference": not (len(set(alerts.values())) == 1 and len(set(cards.values())) == 1),
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
    margins = [
        item.get("closest_market_margin")
        for item in successful
        if item.get("closest_market_margin") is not None
    ]
    narrow_margin = min(margins) if margins else None
    lines = []
    title = "Advance Recommended Setup"
    if failure_count > 0:
        title = "Stabilize Data Path"
        lines.append("Reduce data or notification failures before trusting the batch recommendation.")
    if not summary.get("has_meaningful_difference"):
        title = "Expand Replay Coverage"
        lines.append("Current experiments are tied on alerts and cards; use a larger replay sample to surface real differences.")
        if narrow_margin is not None and narrow_margin > 2.0:
            lines.append("Parameter gaps are still too mild for this sample; try stricter thresholds or wider step sizes.")
        elif narrow_margin is not None:
            lines.append("The batch is already close to a threshold boundary, so a larger replay window is more useful than tighter tuning.")
    elif recommendation:
        lines.append(
            f"Carry {recommendation['name']} forward as the provisional baseline for the next batch."
        )
        if narrow_margin is not None and narrow_margin < 1.0:
            lines.append("Keep the next change small because the current winner is already close to the market threshold.")
        else:
            lines.append("Test one tighter and one looser variant around the current recommendation to map the local sensitivity.")
    if not lines and recommendation:
        lines.append(f"Use {recommendation['name']} as the next baseline and retest on a broader replay sample.")
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
        lines.append(
            f"{row['source_name']}  {row['status']}  {format_beijing_minute(row['checked_at'])}  {latency}  {row['detail']}"
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
    appendix = review.get("appendix") or {}
    formal_readiness = review.get("formal_readiness") or {}
    adjusted_status = review.get("adjusted_price_status") or {}
    lines = [str(review.get("status_label") or "历史效果复盘")]
    lines.append(f"状态：{review.get('status', '-')}")
    lines.append(f"复盘口径版本：{review.get('review_version', '-')}")
    lines.append(f"统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}")
    lines.append(f"历史回补截止：{format_beijing_minute(review.get('backfill_cutoff_at'))}")
    lines.append(f"正式版门槛：{formal_readiness.get('status_label', '-')}")
    if adjusted_status:
        lines.append(
            f"复权状态：{'已满足' if review.get('adjusted_price_protection_ready') else '未满足'}"
            f"（覆盖 {len(adjusted_status.get('coverage') or [])} 个标的）"
        )
    sample_audit = review.get("sample_audit") or {}
    if sample_audit:
        lines.append(f"程序抽检：{sample_audit.get('status', '-')}")
        if sample_audit.get("summary_line"):
            lines.append(f"程序抽检说明：{sample_audit.get('summary_line')}")
    manual_audit = review.get("manual_audit") or {}
    if manual_audit:
        lines.append(f"AI复核：{manual_audit.get('status', '-')}")
        if manual_audit.get("summary_line"):
            lines.append(f"AI复核说明：{manual_audit.get('summary_line')}")
    for blocker in formal_readiness.get("blockers", []):
        lines.append(f"阻塞项：{blocker}")
    for reason in review.get("draft_reasons", []):
        lines.append(f"说明：{reason}")

    lines.append("总体效果：")
    lines.append(f"  决策总数：{overview.get('decision_count', 0)}")
    lines.append(f"  已进场（试探建仓/确认做多）：{overview.get('entered_count', 0)}")
    lines.append(f"  观察中（已进场后）：{overview.get('open_position_count', 0)}")
    lines.append(f"  止盈退出（已进场后）：{overview.get('take_profit_exit_count', 0)}")
    lines.append(f"  失效退出（已进场后）：{overview.get('invalidation_exit_count', 0)}")
    lines.append(f"  复盘窗口结算（已进场后）：{overview.get('window_complete_count', 0)}")
    lines.append(f"  未进场（试探建仓/确认做多，但未到入场区间）：{overview.get('not_entered_count', 0)}")
    lines.append(f"  平均真实收益：{_format_metric_value(overview.get('avg_realized_return'), suffix='%')}")
    lines.append(f"  中位数真实收益：{_format_metric_value(overview.get('median_realized_return'), suffix='%')}")
    lines.append(f"  胜率：{_format_rate_value(overview.get('win_rate'))}")
    lines.append(f"  盈亏比：{_format_metric_value(overview.get('profit_loss_ratio'))}")

    lines.append("执行质量：")
    lines.append(f"  入场命中率：{_format_rate_value(quality.get('entry_hit_rate'))}")
    lines.append(f"  止盈命中率：{_format_rate_value(quality.get('take_profit_hit_rate'))}")
    lines.append(f"  失效率：{_format_rate_value(quality.get('invalidation_hit_rate'))}")
    lines.append(f"  复盘窗口结算率：{_format_rate_value(quality.get('window_complete_rate'))}")
    lines.append(f"  平均持有天数：{_format_metric_value(quality.get('avg_holding_days'), suffix=' 天')}")
    lines.append(f"  已完成复盘样本数（已走出最终结果）：{quality.get('completed_outcome_count', 0)}")
    lines.append(f"  观察中样本数（含未进场与已进场未结束）：{quality.get('pending_outcome_count', 0)}")

    lines.extend(_format_breakdown_section("分组效果 - 按事件类型：", list((review.get("breakdowns") or {}).get("event_type") or [])))
    lines.extend(_format_breakdown_section("分组效果 - 按池子：", list((review.get("breakdowns") or {}).get("pool") or [])))
    lines.extend(_format_breakdown_section("分组效果 - 按动作：", list((review.get("breakdowns") or {}).get("action") or [])))
    lines.extend(_format_breakdown_section("分组效果 - 按触发模式：", list((review.get("breakdowns") or {}).get("trigger_mode") or [])))
    lines.extend(_format_breakdown_section("分组效果 - 按优先级：", list((review.get("breakdowns") or {}).get("priority") or [])))

    lines.append("辅助观察收益（均值=平均收益率）：")
    for row in review.get("auxiliary_observation", []):
        lines.append(
            f"  {row.get('label', row.get('field', '-'))}：样本 {row.get('sample_count', 0)} 条，均值 {_format_metric_value(row.get('avg_value'), suffix='%')}"
        )

    lines.append("下一步建议：")
    recommendations = list(review.get("recommendations") or [])
    if not recommendations:
        lines.append("  1. 当前样本还不足以形成明确建议，继续积累完整样本。")
    else:
        for index, line in enumerate(recommendations, start=1):
            lines.append(f"  {index}. {line}")

    lines.extend(
        _format_ranked_decision_table(
            "效果最好 Top3（已完成样本）：",
            list(review.get("best_completed_decisions") or []),
            metric_field="realized_return",
            metric_label="真实收益",
        )
    )
    lines.extend(
        _format_ranked_decision_table(
            "效果最差 Top3（已完成样本）：",
            list(review.get("worst_completed_decisions") or []),
            metric_field="realized_return",
            metric_label="真实收益",
        )
    )
    lines.extend(
        _format_ranked_decision_table(
            "T+7 表现最好 Top3：",
            list(review.get("best_t7_decisions") or []),
            metric_field="t_plus_7_return",
            metric_label="T+7收益",
        )
    )
    lines.extend(
        _format_ranked_decision_table(
            "T+7 表现最差 Top3：",
            list(review.get("worst_t7_decisions") or []),
            metric_field="t_plus_7_return",
            metric_label="T+7收益",
        )
    )
    lines.extend(_format_decision_detail_table(list(review.get("decision_details") or [])))
    lines.append("附录：")
    lines.append(f"  加入观察信号数：{appendix.get('observation_signal_count', 0)}")
    return "\n".join(lines)


def format_replay_evaluation(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
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
    lines.append("Prewatch Candidates:")
    prewatch_candidates = run_detail.get("summary", {}).get("prewatch_candidates", []) if run_detail else []
    if not prewatch_candidates:
        lines.append("  (no prewatch candidates)")
    for row in prewatch_candidates:
        lines.append(
            "  "
            f"{row['symbol']} {row['horizon']} {_display_prewatch_setup(row['setup_type'])} "
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
    source_health_failures = int(summary.get("source_health_failures", 0))
    extraction_failures = int(summary.get("extraction_failures", 0))
    market_data_failures = int(summary.get("market_data_failures", 0))
    scoring_failures = int(summary.get("scoring_failures", 0))
    notification_failures = int(summary.get("notification_failures", 0))
    cards_generated = int(summary.get("cards_generated", 0))
    alerts_sent = int(summary.get("alerts_sent", 0))
    prewatch_alerts_sent = int(summary.get("prewatch_alerts_sent_count", 0))
    events_processed = int(summary.get("events_processed", 0))
    prewatch_candidates_count = int(summary.get("prewatch_candidates_count", len(summary.get("prewatch_candidates", []))))
    lines = [
        f"本轮处理 {events_processed} 个事件，生成 {cards_generated} 张卡片，发送 {alerts_sent} 条提醒，识别 {prewatch_candidates_count} 个预备池候选，并发出 {prewatch_alerts_sent} 条预备池轻推。"
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
    if unhealthy_sources or source_health_failures:
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
        if row.get("close_reason") in {"window_complete", "hit_take_profit", "hit_invalidation"}
    )
    pending = sum(1 for row in rows if row.get("close_reason") == "insufficient_lookahead")
    take_profit_hits = sum(1 for row in rows if row.get("hit_take_profit"))
    invalidation_hits = sum(1 for row in rows if row.get("hit_invalidation"))
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
            lines.append(
                f"    池位：预备池升级确认池（{_display_prewatch_setup(row.get('prewatch_setup_type', ''))} / 预备池 {row.get('prewatch_score', 0.0):.2f} 分）"
            )
        lines.extend(_format_card_delivery_lines(row))
        lines.append(f"    解读：{_card_takeaway(row)}")
        lines.append(
            "    "
            f"分数：事件 {row['event_score']}/{row['event_threshold']} ({row['event_margin']:+.2f})，"
            f"市场 {row['market_score']}/{row['market_threshold']} ({row['market_margin']:+.2f})，"
            f"综合 {row['final_score']}/{row['priority_threshold']} ({row['priority_margin']:+.2f})"
        )
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
        return ["  本轮没有出现从预备池升级到确认池的标的。"]
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
        lines.append(
            f"    升池原因：此前处于{_display_prewatch_setup(row.get('prewatch_setup_type', ''))}预备状态（{row.get('prewatch_score', 0.0):.2f} 分），本轮事件达到确认条件。"
        )
        lines.extend(_format_card_action_lines(row))
        lines.extend(_format_card_delivery_lines(row))
        if row.get("positioning_hint"):
            lines.append(f"    仓位提示：{row['positioning_hint']}")
        lines.append(
            f"    当前分数：事件 {row['event_score']:.2f} / 市场 {row['market_score']:.2f} / 综合 {row['final_score']:.2f}"
        )
    return lines


def _format_prewatch_lines(
    rows: list[dict[str, Any]],
    *,
    symbol_theme_map: dict[str, list[str]],
) -> list[str]:
    if not rows:
        return ["  本轮没有识别出值得提前跟踪的预备池候选。"]
    peer_map = build_prewatch_peer_map(rows, symbol_theme_map)
    lines: list[str] = []
    for row in rows:
        tags = theme_tags_for_symbol(str(row["symbol"]), symbol_theme_map)
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_prewatch_setup(row['setup_type'])} / 预备池 {row['score']:.2f} 分"
        )
        lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(f"    入池方式：{_display_prewatch_trigger_mode(str(row.get('trigger_mode', 'structure')))}")
        if tags:
            lines.append(f"    题材：{' / '.join(tags)}")
        peers = peer_map.get(str(row["symbol"]).upper(), [])
        if peers:
            lines.append(f"    同题材联动：{', '.join(peers[:4])}")
        lines.append(
            f"    状态：现价 {row['last_price']}，RSI {row['rsi_14']}，相对量能 {row['relative_volume']} 倍，趋势 {row['trend_state']}。"
        )
        lines.append(f"    建议：{row['action_hint']}")
        lines.append(f"    关注理由：{row['reason_to_watch']}")
    return lines


def _format_theme_linkage_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有形成明显的题材链路。"]
    lines: list[str] = []
    for row in rows:
        lines.append(f"  {row['theme_name']} / 热度 {row['heat_score']}")
        if row["confirmed_symbols"]:
            lines.append(f"    确认池：{', '.join(row['confirmed_symbols'])}")
        if row["promoted_symbols"]:
            lines.append(f"    升池确认：{', '.join(row['promoted_symbols'])}")
        if row["prewatch_only_symbols"]:
            lines.append(f"    预备池：{', '.join(row['prewatch_only_symbols'])}")
        if row["sent_symbols"]:
            lines.append(f"    已发送：{', '.join(row['sent_symbols'])}")
        lines.append(f"    链路判断：{row['chain_note']}")
    return lines


def _format_prewatch_push_lines(rows: list[dict[str, Any]], sent_symbols: list[str]) -> list[str]:
    if not sent_symbols:
        return ["  本轮没有发送预备池轻推。"]
    by_symbol = {row["symbol"]: row for row in rows}
    lines: list[str] = []
    for symbol in sent_symbols:
        row = by_symbol.get(symbol)
        if row is None:
            lines.append(f"  {symbol} / 预备池轻推已发送")
            continue
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_prewatch_setup(row['setup_type'])} / {row['score']:.2f} 分"
        )
        lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(f"    入池方式：{_display_prewatch_trigger_mode(str(row.get('trigger_mode', 'structure')))}")
        lines.append(f"    建议：{row['action_hint']}")
        lines.append(f"    关注理由：{row['reason_to_watch']}")
    return lines


def _format_decision_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有落盘的决策记录。"]
    lines: list[str] = []
    for row in rows[:20]:
        title = (
            f"  {row['symbol']} / {row.get('pool_label', row['pool'])} / "
            f"{row.get('action', '-')}"
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
    return lines


def _display_close_reason(value: str) -> str:
    return {
        "hit_invalidation": "触发失效位",
        "hit_take_profit": "触发止盈位",
        "insufficient_lookahead": "仍在等待更多 bars",
        "window_complete": "观察窗已完整",
    }.get(value, value)


def _format_outcome_context_line(row: dict[str, Any]) -> str | None:
    event_context = row.get("event_type_outcome_context") or {}
    pool_context = row.get("pool_outcome_context") or {}
    parts: list[str] = []
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
            f"，止盈 {pool_context.get('take_profit_hits', 0)} 条"
        )
    if not parts:
        return None
    return "；".join(parts) + "。"


def format_run_review(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
    decision_diagnostics: list[dict[str, Any]] | None = None,
) -> str:
    health = summarize_run_health(run_detail, strategy_report, source_health, card_diagnostics, decision_diagnostics)
    lines = ["运行复盘："]
    prewatch_candidates = run_detail.get("summary", {}).get("prewatch_candidates", []) if run_detail else []
    runtime_watchlist = (
        run_detail.get("config_snapshot", {}).get("runtime_config", {}).get("watchlist", {})
        if run_detail
        else {}
    )
    symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(runtime_watchlist)
    if run_detail is None:
        lines.append("运行：缺失")
    else:
        summary = run_detail["summary"]
        prewatch_alert_symbols = summary.get("prewatch_alert_symbols", [])
        high_priority_cards = sum(int(row.get("high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        sent_high_priority = sum(int(row.get("sent_high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        lines.append("结论摘要：")
        lines.append(
            "  "
            f"本轮状态 {_display_run_status(run_detail['status'])}，健康判断为 {health['status']}；"
            f"共处理 {summary.get('events_processed', 0)} 个事件，生成 {summary.get('cards_generated', 0)} 张卡片，发送 {summary.get('alerts_sent', 0)} 条提醒，识别 {len(prewatch_candidates)} 个预备池候选，并发出 {summary.get('prewatch_alerts_sent_count', 0)} 条预备池轻推。"
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
    if not alert_volume:
        lines.append("  本轮没有提醒数据。")
    for row in alert_volume:
        lines.append(
            f"  共 {row['total_alerts']} 条，其中已发送 {row['sent_alerts']} 条，压制 {row['suppressed_alerts']} 条，高优先级卡片 {row['high_priority_alerts']} 条，高优先级已发送 {row.get('sent_high_priority_alerts', 0)} 条。"
        )
    lines.append("最终推送卡片：")
    diagnostics = card_diagnostics or []
    sent_cards = [row for row in diagnostics if row.get("sent")]
    lines.extend(_format_push_card_lines(sent_cards))
    lines.append("升池确认：")
    lines.extend(_format_promoted_confirmation_lines(diagnostics))
    lines.append("题材链路：")
    lines.extend(
        _format_theme_linkage_lines(
            build_theme_snapshot_rows(
                symbol_theme_map=symbol_theme_map,
                card_diagnostics=diagnostics,
                prewatch_candidates=prewatch_candidates,
            )
        )
    )
    lines.append("预备池：")
    lines.extend(_format_prewatch_lines(prewatch_candidates, symbol_theme_map=symbol_theme_map))
    lines.append("预备池轻推：")
    sent_symbols = run_detail.get("summary", {}).get("prewatch_alert_symbols", []) if run_detail else []
    lines.extend(_format_prewatch_push_lines(prewatch_candidates, sent_symbols))
    lines.append("决策记录：")
    lines.extend(_format_decision_lines(decision_diagnostics or []))
    lines.append("数据源状态：")
    if not source_health:
        lines.append("  本轮没有数据源健康信息。")
    for row in source_health:
        latency = row["latency_ms"] if row["latency_ms"] is not None else "-"
        if row["status"] == "healthy":
            lines.append(f"  {row['source_name']}：正常，检查时间 {format_beijing_minute(row['checked_at'])}。{row['detail']}")
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
        lines.append(
            "    "
            f"分数：事件 {row['event_score']}/{row['event_threshold']} ({row['event_margin']:+.2f})，"
            f"市场 {row['market_score']}/{row['market_threshold']} ({row['market_margin']:+.2f})，"
            f"综合 {row['final_score']}/{row['priority_threshold']} ({row['priority_margin']:+.2f})"
        )
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
        return "Batch Replay: no experiments."
    lines = ["Batch Replay:"]
    if payload.get("batch_id"):
        lines.append(f"Batch ID: {payload['batch_id']}")
    if payload.get("generated_at"):
        lines.append(f"Generated At: {payload['generated_at']}")
    if payload.get("manifest_path"):
        lines.append(f"Manifest: {payload['manifest_path']}")
    if payload.get("report_path"):
        lines.append(f"Report: {payload['report_path']}")
    for index, item in enumerate(items, start=1):
        if item["status"] != "success":
            lines.append(
                f"  {index}. {item['name']}: status={item['status']} error={item.get('error', '-')}"
            )
            continue
        summary = item.get("summary", {})
        lines.append(
            f"  {index}. {item['name']}: status={item['status']} run_id={item.get('run_id', '-')} "
            f"events={summary.get('events_processed', 0)} cards={summary.get('cards_generated', 0)} "
            f"alerts={summary.get('alerts_sent', 0)} top_event={item.get('top_event', '-')} "
            f"cfg={item.get('config_summary', '-')} db={item.get('db_path', '-')}"
        )
        for card in item.get("card_diagnostics", []):
            lines.append(
                "     "
                f"{card['symbol']} {card['horizon']} "
                f"event_margin={card['event_margin']:+.2f} "
                f"market_margin={card['market_margin']:+.2f} "
                f"priority_margin={card['priority_margin']:+.2f} "
                f"priority={card['priority']}"
            )
    lines.append("Ranking:")
    for index, row in enumerate(payload.get("ranking", []), start=1):
        lines.append(
            f"  {index}. {row['name']}: status={row['status']} alerts={row['alerts_sent']} "
            f"cards={row['cards_generated']} events={row['events_processed']} cfg={row['config_summary']} "
            f"closest_market_margin={row['closest_market_margin']} closest_priority_margin={row['closest_priority_margin']}"
        )
    recommendation = payload.get("recommendation")
    if recommendation:
        lines.append("Recommendation:")
        lines.append(
            f"  {recommendation['name']}: alerts={recommendation['alerts_sent']} "
            f"cards={recommendation['cards_generated']} events={recommendation['events_processed']} "
            f"cfg={recommendation['config_summary']} "
            f"closest_market_margin={recommendation['closest_market_margin']} "
            f"closest_priority_margin={recommendation['closest_priority_margin']}"
        )
        lines.append(f"  Reason: {recommendation['reason']}")
    summary = payload.get("summary")
    if summary and summary.get("line_items"):
        lines.append("Summary:")
        for line in summary["line_items"]:
            lines.append(f"  {line}")
    recommendation = payload.get("recommendation")
    if recommendation:
        lines.append("Winner Snapshot:")
        top_event = next(
            (
                item.get("top_event", "-")
                for item in payload.get("experiments", [])
                if item.get("name") == recommendation.get("name")
            ),
            "-",
        )
        lines.append(
            f"  {recommendation['name']}: cfg={recommendation['config_summary']} "
            f"alerts={recommendation['alerts_sent']} cards={recommendation['cards_generated']} "
            f"events={recommendation['events_processed']} top_event={top_event}"
        )
        lines.append(
            f"  margins: market={recommendation['closest_market_margin']} "
            f"priority={recommendation['closest_priority_margin']}"
        )
    next_step = payload.get("next_step")
    if next_step and next_step.get("line_items"):
        lines.append(f"Next Step: {next_step['title']}")
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
