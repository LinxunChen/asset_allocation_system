from __future__ import annotations

import json
from typing import Any

from .models import ensure_utc
from .store import Store
from .timefmt import parse_datetime


def _normalize_before(before: str) -> str:
    parsed = parse_datetime(before)
    if parsed is None:
        raise ValueError(f"Invalid archive cutoff: {before}")
    return ensure_utc(parsed).isoformat()


def _load_json_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def archive_decision_history(
    source_store: Store,
    archive_store: Store,
    *,
    before: str,
    limit: int = 0,
    apply: bool = False,
) -> dict[str, Any]:
    normalized_before = _normalize_before(before)
    rows = source_store.load_decision_history_for_archive(before=normalized_before, limit=limit)
    decision_ids = [str(row["decision_id"]) for row in rows]
    outcomes_present = sum(1 for row in rows if row["outcome_updated_at"])

    payload: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "before": normalized_before,
        "scanned": len(rows),
        "decision_records": len(rows),
        "decision_outcomes": outcomes_present,
        "copied_records": 0,
        "copied_outcomes": 0,
        "deleted_records": 0,
        "deleted_outcomes": 0,
    }
    if not apply or not rows:
        return payload

    copied_outcomes = 0
    for row in rows:
        archive_store.save_decision_record(
            decision_id=row["decision_id"],
            run_id=row["run_id"],
            event_id=row["event_id"],
            symbol=row["symbol"],
            event_type=row["event_type"] or "",
            pool=row["pool"],
            action=row["action"],
            priority=row["priority"],
            confidence=row["confidence"],
            event_score=row["event_score"],
            market_score=row["market_score"],
            theme_score=row["theme_score"],
            final_score=row["final_score"],
            trigger_mode=row["trigger_mode"],
            llm_used=bool(row["llm_used"]),
            theme_ids=_load_json_list(row["theme_ids_json"]),
            entry_plan=_load_json_dict(row["entry_plan_json"]),
            invalidation=_load_json_dict(row["invalidation_json"]),
            ttl=row["ttl"],
            packet=_load_json_dict(row["packet_json"]),
            created_at=row["created_at"],
        )
        if row["outcome_updated_at"]:
            archive_store.save_decision_outcome(
                decision_id=row["decision_id"],
                entered=bool(row["entered"]),
                entered_at=row["entered_at"] or "",
                entry_price=row["entry_price"],
                exit_price=row["exit_price"],
                realized_return=row["realized_return"],
                holding_days=row["holding_days"],
                gross_realized_return=row["gross_realized_return"],
                net_realized_return=row["net_realized_return"],
                slippage_bps=float(row["slippage_bps"] or 0.0),
                t_plus_1_return=row["t_plus_1_return"],
                t_plus_3_return=row["t_plus_3_return"],
                t_plus_5_return=row["t_plus_5_return"],
                t_plus_7_return=row["t_plus_7_return"],
                t_plus_10_return=row["t_plus_10_return"],
                t_plus_14_return=row["t_plus_14_return"],
                t_plus_30_return=row["t_plus_30_return"],
                max_runup=row["max_runup"],
                max_drawdown=row["max_drawdown"],
                hit_take_profit=bool(row["hit_take_profit"]),
                hit_invalidation=bool(row["hit_invalidation"]),
                close_reason=row["close_reason"] or "",
                updated_at=row["outcome_updated_at"],
            )
            copied_outcomes += 1

    deleted = source_store.delete_decision_history(decision_ids)
    payload["copied_records"] = len(rows)
    payload["copied_outcomes"] = copied_outcomes
    payload["deleted_records"] = deleted["deleted_records"]
    payload["deleted_outcomes"] = deleted["deleted_outcomes"]
    return payload
