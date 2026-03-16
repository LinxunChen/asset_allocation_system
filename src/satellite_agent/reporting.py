from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable

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


def _display_priority(priority: str) -> str:
    return {"high": "高优先级", "normal": "普通", "suppressed": "压制"}.get(priority, priority)


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
) -> dict[str, Any]:
    return {
        "event_type_performance": [
            {
                "event_type": row["event_type"],
                "card_count": row["card_count"],
                "avg_final_score": row["avg_final_score"],
                "high_priority_count": row["high_priority_count"],
            }
            for row in event_types
        ],
        "source_stability": [
            {
                "source_name": row["source_name"],
                "check_count": row["check_count"],
                "healthy_count": row["healthy_count"],
                "unhealthy_count": row["unhealthy_count"],
                "avg_latency_ms": row["avg_latency_ms"],
                "last_checked_at": row["last_checked_at"],
            }
            for row in source_stability
        ],
        "alert_volume": [
            {
                "bucket_date": row["bucket_date"],
                "total_alerts": row["total_alerts"],
                "sent_alerts": row["sent_alerts"],
                "suppressed_alerts": row["suppressed_alerts"],
                "high_priority_alerts": row["high_priority_alerts"],
                "sent_high_priority_alerts": row["sent_high_priority_alerts"] if "sent_high_priority_alerts" in row.keys() else 0,
            }
            for row in alert_volume
        ],
    }


def serialize_replay_evaluation(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "run": run_detail,
        "strategy_report": strategy_report,
        "source_health": source_health,
        "card_diagnostics": card_diagnostics or [],
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
) -> str:
    report = serialize_strategy_report(event_types, source_stability, alert_volume)
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
    return "\n".join(lines)


def format_replay_evaluation(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
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
    return "\n".join(lines)


def summarize_run_health(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
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
    events_processed = int(summary.get("events_processed", 0))
    lines = [
        f"本轮处理 {events_processed} 个事件，生成 {cards_generated} 张卡片，发送 {alerts_sent} 条提醒。"
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


def _format_push_card_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["  本轮没有实际发送的卡片。"]
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_event_type(row['event_type'])} / {_display_priority(row['priority'])}"
        )
        if row.get("headline_summary"):
            lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(f"    解读：{_card_takeaway(row)}")
        lines.append(
            "    "
            f"分数：事件 {row['event_score']}/{row['event_threshold']} ({row['event_margin']:+.2f})，"
            f"市场 {row['market_score']}/{row['market_threshold']} ({row['market_margin']:+.2f})，"
            f"综合 {row['final_score']}/{row['priority_threshold']} ({row['priority_margin']:+.2f})"
        )
        if row.get("entry_range") and row.get("take_profit_range"):
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
    return lines


def format_run_review(
    run_detail: dict[str, Any] | None,
    strategy_report: dict[str, Any],
    source_health: list[dict[str, Any]],
    card_diagnostics: list[dict[str, Any]] | None = None,
) -> str:
    health = summarize_run_health(run_detail, strategy_report, source_health, card_diagnostics)
    lines = ["运行复盘："]
    if run_detail is None:
        lines.append("运行：缺失")
    else:
        summary = run_detail["summary"]
        high_priority_cards = sum(int(row.get("high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        sent_high_priority = sum(int(row.get("sent_high_priority_alerts", 0)) for row in strategy_report.get("alert_volume", []))
        lines.append("结论摘要：")
        lines.append(
            "  "
            f"本轮状态 {_display_run_status(run_detail['status'])}，健康判断为 {health['status']}；"
            f"共处理 {summary.get('events_processed', 0)} 个事件，生成 {summary.get('cards_generated', 0)} 张卡片，发送 {summary.get('alerts_sent', 0)} 条提醒。"
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
        lines.append(
            f"  {row['symbol']} / {_display_horizon(row['horizon'])} / {_display_event_type(row['event_type'])} / {_display_priority(row['priority'])}"
        )
        if row.get("headline_summary"):
            lines.append(f"    摘要：{row['headline_summary']}")
        lines.append(
            f"    解读：{_card_takeaway(row)}"
        )
        lines.append(
            "    "
            f"分数：事件 {row['event_score']}/{row['event_threshold']} ({row['event_margin']:+.2f})，"
            f"市场 {row['market_score']}/{row['market_threshold']} ({row['market_margin']:+.2f})，"
            f"综合 {row['final_score']}/{row['priority_threshold']} ({row['priority_margin']:+.2f})"
        )
        if row.get("entry_range") and row.get("take_profit_range"):
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
