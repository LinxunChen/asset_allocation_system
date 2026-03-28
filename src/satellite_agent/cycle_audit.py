from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import timedelta
from typing import Any, Iterable

from .outcomes import normalize_close_reason
from .store import Store
from .timefmt import format_beijing_minute, parse_datetime
from .models import utcnow

AUDIT_SINCE = "1970-01-01T00:00:00+00:00"
TERMINAL_REASONS = {
    "not_entered_price_invalidated",
    "not_entered_window_expired",
    "profit_protection_exit",
    "invalidation_exit",
    "window_close_evaluation",
}
FORMAL_ACTIONS = {"确认做多", "试探建仓"}
EVENT_KIND_ORDER = {
    "candidate_pool_selected": 0,
    "candidate_watch": 1,
    "formal_send": 2,
    "formal_downgraded_to_watch_unentered": 3,
    "suppressed_active_holding": 4,
    "entered_holding_active": 5,
    "terminal": 6,
}
ACTIVE_CYCLE_STATUSES = {"pending_entry", "holding_active"}
CYCLE_START_EVENTS = {
    "formal_send",
    "formal_downgraded_to_watch_unentered",
    "entered_holding_active",
}


def _json_object(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    try:
        payload = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_pool(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized == "prewatch":
        return "candidate_pool"
    return normalized


def _sort_key(event: dict[str, Any]) -> tuple[str, int, str, str]:
    return (
        str(event.get("created_at") or ""),
        EVENT_KIND_ORDER.get(str(event.get("event_kind") or ""), 99),
        str(event.get("decision_id") or ""),
        str(event.get("related_event_id") or ""),
    )


def _cycle_id(symbol: str, created_at: str, marker: str) -> str:
    digest = hashlib.sha1(f"{symbol}:{created_at}:{marker}".encode("utf-8")).hexdigest()
    return f"{symbol}:{digest[:12]}"


def _decision_events(row: dict[str, Any]) -> list[dict[str, Any]]:
    symbol = _normalize_symbol(str(row.get("symbol") or ""))
    if not symbol:
        return []
    pool = _normalize_pool(str(row.get("pool") or ""))
    action_label = str(row.get("action") or "").strip()
    decision_id = str(row.get("decision_id") or "")
    related_event_id = str(row.get("event_id") or "")
    created_at = str(row.get("created_at") or "")
    normalized_reason = normalize_close_reason(
        str(row.get("close_reason") or ""),
        exit_subreason=str(row.get("exit_subreason") or ""),
        entered=bool(row.get("entered")),
    )
    packet = _json_object(row.get("packet_json"))
    events: list[dict[str, Any]] = []
    if pool == "candidate_pool":
        events.append(
            {
                "symbol": symbol,
                "event_kind": "candidate_pool_selected",
                "decision_id": decision_id,
                "related_event_id": related_event_id,
                "pool": pool,
                "action_label": action_label or "加入观察",
                "normalized_reason": "",
                "created_at": created_at,
                "payload": {
                    "horizon": str(packet.get("horizon") or ""),
                    "final_score": float(row.get("final_score") or 0.0),
                    "trigger_mode": str(row.get("trigger_mode") or ""),
                },
            }
        )
        return events
    if pool == "confirmation" and action_label in FORMAL_ACTIONS:
        entered = bool(row.get("entered"))
        events.append(
            {
                "symbol": symbol,
                "event_kind": "formal_send",
                "decision_id": decision_id,
                "related_event_id": related_event_id,
                "pool": pool,
                "action_label": action_label,
                "normalized_reason": "",
                "created_at": created_at,
                "payload": {
                    "entered": entered,
                    "priority": str(row.get("priority") or ""),
                    "final_score": float(row.get("final_score") or 0.0),
                },
            }
        )
        if entered and normalized_reason not in TERMINAL_REASONS:
            entered_at = str(row.get("entered_at") or row.get("outcome_updated_at") or created_at)
            events.append(
                {
                    "symbol": symbol,
                    "event_kind": "entered_holding_active",
                    "decision_id": decision_id,
                    "related_event_id": related_event_id,
                    "pool": pool,
                    "action_label": action_label,
                    "normalized_reason": "holding_active",
                    "created_at": entered_at,
                    "payload": {
                        "entry_price": row.get("entry_price"),
                        "entered_at": str(row.get("entered_at") or ""),
                    },
                }
            )
        if normalized_reason in {"not_entered_price_invalidated", "not_entered_window_expired", "window_close_evaluation"}:
            events.append(
                {
                    "symbol": symbol,
                    "event_kind": "terminal",
                    "decision_id": decision_id,
                    "related_event_id": related_event_id,
                    "pool": pool,
                    "action_label": action_label,
                    "normalized_reason": normalized_reason,
                    "created_at": str(row.get("outcome_updated_at") or created_at),
                    "payload": {
                        "exit_subreason": str(row.get("exit_subreason") or ""),
                        "realized_return": row.get("realized_return"),
                    },
                }
            )
        return events
    if pool in {"holding_management", "exit"}:
        terminal_reason = str(packet.get("normalized_close_reason") or packet.get("holding_management_reason") or "")
        terminal_reason = terminal_reason or normalized_reason
        if terminal_reason in TERMINAL_REASONS:
            events.append(
                {
                    "symbol": symbol,
                    "event_kind": "terminal",
                    "decision_id": decision_id,
                    "related_event_id": related_event_id,
                    "pool": "holding_management",
                    "action_label": action_label or "持仓管理",
                    "normalized_reason": terminal_reason,
                    "created_at": created_at,
                    "payload": {
                        "source_decision_id": str(packet.get("source_decision_id") or ""),
                        "exit_subreason": str(packet.get("exit_subreason") or ""),
                    },
                }
            )
    return events


def _alert_events(row: dict[str, Any]) -> list[dict[str, Any]]:
    if not bool(row.get("sent")):
        return []
    symbol = _normalize_symbol(str(row.get("symbol") or ""))
    if not symbol:
        return []
    card = _json_object(row.get("card_json"))
    delivery_category = str(card.get("delivery_category") or "").strip()
    action_label = str(card.get("action_label") or "")
    if delivery_category == "candidate_optional":
        event_kind = "candidate_watch"
    elif delivery_category == "watch":
        event_kind = (
            "formal_downgraded_to_watch_unentered"
            if bool(card.get("downgraded_from_formal"))
            else "candidate_watch"
        )
    else:
        return []
    payload = {
        "dedup_key": str(row.get("dedup_key") or ""),
        "reason": str(row.get("reason") or ""),
        "delivery_category": delivery_category,
        "downgraded_from_formal": bool(card.get("downgraded_from_formal")),
        "previous_formal_action": str(card.get("previous_formal_action") or ""),
    }
    return [
        {
            "symbol": symbol,
            "event_kind": event_kind,
            "decision_id": "",
            "related_event_id": str(row.get("event_id") or ""),
            "pool": _normalize_pool(str(card.get("lifecycle_pool") or "")),
            "action_label": action_label or "加入观察",
            "normalized_reason": "",
            "created_at": str(row.get("notified_at") or ""),
            "payload": payload,
        }
    ]


def _log_events(row: dict[str, Any]) -> list[dict[str, Any]]:
    symbol = _normalize_symbol(str(row.get("symbol") or ""))
    if not symbol:
        return []
    event_type = str(row.get("event_type") or "")
    if event_type != "formal_suppressed_active_holding":
        return []
    context = _json_object(row.get("context_json"))
    return [
        {
            "symbol": symbol,
            "event_kind": "suppressed_active_holding",
            "decision_id": "",
            "related_event_id": str(row.get("event_id") or ""),
            "pool": "confirmation",
            "action_label": str(context.get("action_label") or ""),
            "normalized_reason": "holding_active",
            "created_at": str(row.get("created_at") or ""),
            "payload": {
                "previous_formal_action": str(context.get("previous_formal_action") or ""),
            },
        }
    ]


def _candidate_observation_summary(store: Store, symbols: list[str]) -> dict[str, dict[str, Any]]:
    since = (utcnow() - timedelta(hours=72)).isoformat()
    rows = store.load_candidate_evaluations_for_window(since=since, stage="candidate_pool")
    selected_runs: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        payload = dict(row)
        symbol = _normalize_symbol(str(payload.get("symbol") or ""))
        if symbols and symbol not in symbols:
            continue
        if str(payload.get("outcome") or "").strip() != "selected":
            continue
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            continue
        created_at = str(payload.get("created_at") or "")
        if run_id not in selected_runs[symbol] or created_at < selected_runs[symbol][run_id]:
            selected_runs[symbol][run_id] = created_at
    result: dict[str, dict[str, Any]] = {}
    for symbol, runs in selected_runs.items():
        ordered_runs = [
            {"run_id": run_id, "created_at": created_at, "created_at_display": format_beijing_minute(created_at)}
            for run_id, created_at in sorted(runs.items(), key=lambda item: (item[1], item[0]))
        ]
        result[symbol] = {
            "count": len(ordered_runs),
            "runs": ordered_runs,
        }
    return result


def rebuild_cycle_audit(store: Store, *, symbols: Iterable[str] | None = None) -> dict[str, Any]:
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in (symbols or []) if _normalize_symbol(symbol)})
    if normalized_symbols:
        decision_rows = store.load_decision_records_for_symbols(normalized_symbols)
        alert_rows = store.load_alert_history_for_symbols(normalized_symbols)
        log_rows = store.load_cycle_transition_logs_for_window(since=AUDIT_SINCE, symbols=normalized_symbols)
    else:
        decision_rows = store.load_decision_records_for_window(since=AUDIT_SINCE)
        alert_rows = store.load_alert_history_for_window(since=AUDIT_SINCE)
        log_rows = store.load_cycle_transition_logs_for_window(since=AUDIT_SINCE)
    events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in decision_rows:
        payload = dict(row)
        for event in _decision_events(payload):
            events_by_symbol[event["symbol"]].append(event)
    for row in alert_rows:
        payload = dict(row)
        for event in _alert_events(payload):
            events_by_symbol[event["symbol"]].append(event)
    for row in log_rows:
        payload = dict(row)
        for event in _log_events(payload):
            events_by_symbol[event["symbol"]].append(event)
    target_symbols = normalized_symbols or sorted(events_by_symbol.keys())
    observation_summary = _candidate_observation_summary(store, target_symbols)
    snapshot_items: list[dict[str, Any]] = []
    for symbol in target_symbols:
        events = sorted(events_by_symbol.get(symbol, []), key=_sort_key)
        if not events:
            store.replace_symbol_cycle_audit(symbol=symbol, snapshot=None, events=[])
            continue
        current_cycle_id = ""
        current_cycle_started_at = ""
        status = "terminal"
        previous_formal_action = ""
        last_formal_decision_id = ""
        last_related_event_id = ""
        last_event_kind = ""
        last_terminal_reason = ""
        last_transition_at = ""
        latest_formal_send: dict[str, Any] | None = None
        latest_downgraded_watch: dict[str, Any] | None = None
        latest_terminal: dict[str, Any] | None = None
        latest_suppression: dict[str, Any] | None = None
        enriched_events: list[dict[str, Any]] = []
        for event in events:
            event_kind = str(event.get("event_kind") or "")
            should_start_cycle = event_kind in CYCLE_START_EVENTS
            if (not current_cycle_id and should_start_cycle) or (
                status == "terminal" and should_start_cycle
            ):
                current_cycle_id = _cycle_id(
                    symbol,
                    str(event.get("created_at") or ""),
                    str(event.get("related_event_id") or event.get("decision_id") or event_kind),
                )
                current_cycle_started_at = str(event.get("created_at") or "")
            status_before = status
            status_after = status
            if event_kind == "formal_send":
                status_after = "holding_active" if bool((event.get("payload") or {}).get("entered")) else "pending_entry"
                previous_formal_action = str(event.get("action_label") or previous_formal_action)
                last_formal_decision_id = str(event.get("decision_id") or last_formal_decision_id)
                latest_formal_send = event
            elif event_kind == "formal_downgraded_to_watch_unentered":
                status_after = "pending_entry" if status in {"terminal", "pending_entry"} else status
                latest_downgraded_watch = event
            elif event_kind == "entered_holding_active":
                status_after = "holding_active"
            elif event_kind == "terminal":
                status_after = "terminal"
                last_terminal_reason = str(event.get("normalized_reason") or last_terminal_reason)
                latest_terminal = event
            elif event_kind == "suppressed_active_holding":
                latest_suppression = event
            if status_after != status_before or event_kind in {
                "candidate_pool_selected",
                "candidate_watch",
                "formal_downgraded_to_watch_unentered",
                "suppressed_active_holding",
                "terminal",
            }:
                last_transition_at = str(event.get("created_at") or last_transition_at)
            last_related_event_id = str(event.get("related_event_id") or event.get("decision_id") or last_related_event_id)
            last_event_kind = event_kind or last_event_kind
            status = status_after
            enriched_events.append(
                {
                    **event,
                    "cycle_id": current_cycle_id,
                    "status_before": status_before,
                    "status_after": status_after,
                }
            )
        observation = observation_summary.get(symbol) or {"count": 0, "runs": []}
        snapshot = {
            "symbol": symbol,
            "cycle_id": current_cycle_id,
            "status": status,
            "previous_formal_action": previous_formal_action,
            "last_formal_decision_id": last_formal_decision_id,
            "last_related_event_id": last_related_event_id,
            "last_event_kind": last_event_kind,
            "last_terminal_reason": last_terminal_reason,
            "current_cycle_started_at": current_cycle_started_at,
            "last_transition_at": last_transition_at,
            "updated_at": str(enriched_events[-1].get("created_at") or ""),
            "cycle_event_count": len(enriched_events),
            "latest_formal_send": latest_formal_send,
            "latest_downgraded_watch": latest_downgraded_watch,
            "latest_terminal": latest_terminal,
            "latest_suppression": latest_suppression,
            "candidate_observation_count_72h": int(observation.get("count") or 0),
            "candidate_observation_runs_72h": list(observation.get("runs") or []),
            "recent_events": [
                {
                    "created_at": str(item.get("created_at") or ""),
                    "created_at_display": format_beijing_minute(str(item.get("created_at") or "")),
                    "event_kind": str(item.get("event_kind") or ""),
                    "status_before": str(item.get("status_before") or ""),
                    "status_after": str(item.get("status_after") or ""),
                    "action_label": str(item.get("action_label") or ""),
                    "normalized_reason": str(item.get("normalized_reason") or ""),
                }
                for item in enriched_events[-8:]
            ],
        }
        store.replace_symbol_cycle_audit(symbol=symbol, snapshot=snapshot, events=enriched_events)
        snapshot_items.append(snapshot)
    snapshot_rows = store.load_symbol_cycle_snapshots(symbols=target_symbols if target_symbols else None)
    return {
        "symbols": [_normalize_symbol(str(row["symbol"] or "")) for row in snapshot_rows],
        "snapshot_count": len(snapshot_rows),
    }


def build_cycle_audit_payload(
    store: Store,
    *,
    symbols: Iterable[str] | None = None,
    status: str = "",
    limit: int = 50,
    refresh: bool = True,
    active_only: bool = False,
) -> dict[str, Any]:
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in (symbols or []) if _normalize_symbol(symbol)})
    refresh_result = {"symbols": [], "snapshot_count": 0}
    if refresh:
        refresh_result = rebuild_cycle_audit(store, symbols=normalized_symbols or None)
    rows = store.load_symbol_cycle_snapshots(status=status, symbols=normalized_symbols or None)
    if active_only and not status:
        rows = [row for row in rows if str(row["status"] or "") in ACTIVE_CYCLE_STATUSES]
    items: list[dict[str, Any]] = []
    for row in rows[: limit if limit > 0 else None]:
        row_payload = dict(row)
        snapshot = _json_object(row_payload.get("snapshot_json"))
        events = [
            {
                **dict(event_row),
                "payload": _json_object(dict(event_row).get("payload_json")),
                "created_at_display": format_beijing_minute(str(event_row["created_at"] or "")),
            }
            for event_row in store.load_symbol_cycle_events(
                symbol=str(row_payload["symbol"] or ""),
                cycle_id=str(row_payload["cycle_id"] or ""),
            )
        ]
        items.append(
            {
                "symbol": str(row_payload["symbol"] or ""),
                "cycle_id": str(row_payload["cycle_id"] or ""),
                "status": str(row_payload["status"] or "terminal"),
                "previous_formal_action": str(row_payload["previous_formal_action"] or ""),
                "last_formal_decision_id": str(row_payload["last_formal_decision_id"] or ""),
                "last_related_event_id": str(row_payload["last_related_event_id"] or ""),
                "last_event_kind": str(row_payload["last_event_kind"] or ""),
                "last_terminal_reason": str(row_payload["last_terminal_reason"] or ""),
                "current_cycle_started_at": str(row_payload["current_cycle_started_at"] or ""),
                "current_cycle_started_at_display": format_beijing_minute(str(row_payload["current_cycle_started_at"] or "")),
                "last_transition_at": str(row_payload["last_transition_at"] or ""),
                "last_transition_at_display": format_beijing_minute(str(row_payload["last_transition_at"] or "")),
                "updated_at": str(row_payload["updated_at"] or ""),
                "updated_at_display": format_beijing_minute(str(row_payload["updated_at"] or "")),
                "candidate_observation_count_72h": int(snapshot.get("candidate_observation_count_72h") or 0),
                "candidate_observation_runs_72h": list(snapshot.get("candidate_observation_runs_72h") or []),
                "latest_formal_send": snapshot.get("latest_formal_send") or {},
                "latest_downgraded_watch": snapshot.get("latest_downgraded_watch") or {},
                "latest_terminal": snapshot.get("latest_terminal") or {},
                "latest_suppression": snapshot.get("latest_suppression") or {},
                "recent_events": list(snapshot.get("recent_events") or []),
                "events": events,
            }
        )
    return {
        "generated_at": utcnow().isoformat(),
        "generated_at_display": format_beijing_minute(utcnow().isoformat()),
        "status_filter": status,
        "limit": limit,
        "refresh": refresh_result,
        "items": items,
    }


def _collect_run_touched_symbols(store: Store, run_id: str) -> list[str]:
    symbols: set[str] = set()
    for row in store.load_decision_records(run_id):
        symbol = _normalize_symbol(str(row["symbol"] or ""))
        if symbol:
            symbols.add(symbol)
    for row in store.load_opportunity_cards(run_id):
        symbol = _normalize_symbol(str(row["symbol"] or ""))
        if symbol:
            symbols.add(symbol)
    for row in store.load_alert_history(run_id):
        symbol = _normalize_symbol(str(row["symbol"] or ""))
        if symbol:
            symbols.add(symbol)
    for row in store.load_logs(run_id, limit=5000):
        symbol = _normalize_symbol(str(row["symbol"] or ""))
        if symbol:
            symbols.add(symbol)
    return sorted(symbols)


def _summarize_cycle_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = {"pending_entry": 0, "holding_active": 0, "terminal": 0}
    event_kind_counts: dict[str, int] = defaultdict(int)
    terminal_reason_counts: dict[str, int] = defaultdict(int)
    reopened_cycle_symbols: list[str] = []
    downgraded_pending_symbols: list[str] = []
    suppressed_holding_symbols: list[str] = []
    for item in items:
        status = str(item.get("status") or "terminal")
        if status in status_counts:
            status_counts[status] += 1
        cycle_count = int(item.get("cycle_count") or 0)
        if cycle_count > 1 and status != "terminal":
            reopened_cycle_symbols.append(str(item.get("symbol") or ""))
        if status == "pending_entry" and item.get("latest_downgraded_watch"):
            downgraded_pending_symbols.append(str(item.get("symbol") or ""))
        if status == "holding_active" and item.get("latest_suppression"):
            suppressed_holding_symbols.append(str(item.get("symbol") or ""))
        for event in item.get("events") or []:
            event_kind = str(event.get("event_kind") or "")
            if event_kind:
                event_kind_counts[event_kind] += 1
            if event_kind == "terminal":
                normalized_reason = str(event.get("normalized_reason") or "")
                if normalized_reason:
                    terminal_reason_counts[normalized_reason] += 1
    return {
        "status_counts": status_counts,
        "event_kind_counts": dict(sorted(event_kind_counts.items())),
        "terminal_reason_counts": dict(sorted(terminal_reason_counts.items())),
        "reopened_cycle_symbols": reopened_cycle_symbols,
        "downgraded_pending_symbols": downgraded_pending_symbols,
        "suppressed_holding_symbols": suppressed_holding_symbols,
    }


def _detect_symbol_cycle_anomalies(
    item: dict[str, Any],
    *,
    all_events: list[dict[str, Any]],
    run_started_at: Any = None,
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    run_started_dt = parse_datetime(run_started_at)

    def _classify_scope(triggered_at: Any) -> str:
        triggered_dt = parse_datetime(triggered_at)
        if run_started_dt is None or triggered_dt is None:
            return "unknown"
        return "current_run" if triggered_dt >= run_started_dt else "historical_carryover"

    duplicate_terminal_counts: dict[tuple[str, str], int] = defaultdict(int)
    duplicate_terminal_latest_at: dict[tuple[str, str], str] = {}
    for event in all_events:
        if str(event.get("event_kind") or "") != "terminal":
            continue
        payload = event.get("payload") or {}
        terminal_marker = (
            str(payload.get("source_decision_id") or "").strip()
            or str(event.get("decision_id") or "").strip()
            or str(event.get("related_event_id") or "").strip()
        )
        terminal_key = (terminal_marker, str(event.get("normalized_reason") or "").strip())
        duplicate_terminal_counts[terminal_key] += 1
        duplicate_terminal_latest_at[terminal_key] = str(event.get("created_at") or "")
    duplicate_terminal_total = sum(count - 1 for count in duplicate_terminal_counts.values() if count > 1)
    if duplicate_terminal_total > 0:
        latest_duplicate_terminal_at = max(
            (
                duplicate_terminal_latest_at[key]
                for key, count in duplicate_terminal_counts.items()
                if count > 1 and duplicate_terminal_latest_at.get(key)
            ),
            default="",
        )
        anomalies.append(
            {
                "type": "duplicate_terminal_events",
                "severity": "warning",
                "message": f"检测到 {duplicate_terminal_total} 条重复 terminal 事件。",
                "scope": _classify_scope(latest_duplicate_terminal_at),
                "triggered_at": latest_duplicate_terminal_at,
            }
        )
    current_cycle_events = list(item.get("events") or [])
    holding_formal_events = [
        event
        for event in current_cycle_events
        if str(event.get("event_kind") or "") == "formal_send"
        and str(event.get("status_before") or "") == "holding_active"
    ]
    if holding_formal_events:
        latest_holding_formal_at = str(holding_formal_events[-1].get("created_at") or "")
        anomalies.append(
            {
                "type": "formal_send_during_holding",
                "severity": "warning",
                "message": "当前周期处于 holding_active 后仍出现新的 formal_send。",
                "scope": _classify_scope(latest_holding_formal_at),
                "triggered_at": latest_holding_formal_at,
            }
        )
    downgraded_during_holding_events = [
        event
        for event in current_cycle_events
        if str(event.get("event_kind") or "") == "formal_downgraded_to_watch_unentered"
        and str(event.get("status_before") or "") == "holding_active"
    ]
    if downgraded_during_holding_events:
        latest_downgraded_holding_at = str(downgraded_during_holding_events[-1].get("created_at") or "")
        anomalies.append(
            {
                "type": "downgraded_watch_during_holding",
                "severity": "warning",
                "message": "当前周期处于 holding_active 时仍出现降级观察卡。",
                "scope": _classify_scope(latest_downgraded_holding_at),
                "triggered_at": latest_downgraded_holding_at,
            }
        )
    if str(item.get("status") or "") == "terminal" and not item.get("latest_terminal"):
        anomalies.append(
            {
                "type": "terminal_without_terminal_event",
                "severity": "warning",
                "message": "当前状态为 terminal，但缺少对应的 terminal 事件。",
                "scope": "unknown",
                "triggered_at": "",
            }
        )
    return anomalies


def build_run_cycle_audit_payload(
    store: Store,
    *,
    run_id: str,
    limit: int = 20,
) -> dict[str, Any]:
    run_row = store.load_run(run_id)
    run_started_at = str(run_row["started_at"] or "") if run_row else ""
    touched_symbols = _collect_run_touched_symbols(store, run_id)
    cycle_payload = build_cycle_audit_payload(
        store,
        symbols=touched_symbols,
        limit=0,
        refresh=True,
        active_only=False,
    )
    all_items: list[dict[str, Any]] = []
    anomaly_type_counts: dict[str, int] = defaultdict(int)
    anomaly_symbols: list[str] = []
    for item in cycle_payload.get("items") or []:
        symbol = str(item.get("symbol") or "")
        all_events = [
            {
                **dict(row),
                "payload": _json_object(dict(row).get("payload_json")),
            }
            for row in store.load_symbol_cycle_events(symbol=symbol)
        ]
        cycle_ids = {
            str(row["cycle_id"] or "")
            for row in all_events
            if str(row["cycle_id"] or "").strip()
        }
        enriched_item = dict(item)
        enriched_item["cycle_count"] = len(cycle_ids)
        enriched_item["total_cycle_event_count"] = len(all_events)
        enriched_item["has_reopened_cycle"] = len(cycle_ids) > 1 and str(item.get("status") or "") != "terminal"
        enriched_item["anomalies"] = _detect_symbol_cycle_anomalies(
            enriched_item,
            all_events=all_events,
            run_started_at=run_started_at,
        )
        if enriched_item["anomalies"]:
            anomaly_symbols.append(symbol)
            for anomaly in enriched_item["anomalies"]:
                anomaly_type = str(anomaly.get("type") or "").strip()
                if anomaly_type:
                    anomaly_type_counts[anomaly_type] += 1
        all_items.append(enriched_item)
    summary = _summarize_cycle_items(all_items)
    summary["anomaly_count"] = sum(len(item.get("anomalies") or []) for item in all_items)
    summary["anomaly_symbols"] = anomaly_symbols
    summary["anomaly_type_counts"] = dict(sorted(anomaly_type_counts.items()))
    current_run_anomalies = sum(
        1
        for item in all_items
        for anomaly in (item.get("anomalies") or [])
        if str(anomaly.get("scope") or "") == "current_run"
    )
    historical_carryover_anomalies = sum(
        1
        for item in all_items
        for anomaly in (item.get("anomalies") or [])
        if str(anomaly.get("scope") or "") == "historical_carryover"
    )
    summary["current_run_anomaly_count"] = current_run_anomalies
    summary["historical_carryover_anomaly_count"] = historical_carryover_anomalies
    items = all_items[: limit if limit > 0 else None]
    return {
        "run_id": run_id,
        "run_name": str(run_row["run_name"] or "") if run_row else "",
        "started_at": str(run_row["started_at"] or "") if run_row else "",
        "started_at_display": format_beijing_minute(str(run_row["started_at"] or "")) if run_row else "",
        "touched_symbols": touched_symbols,
        "symbol_count": len(touched_symbols),
        "display_limit": limit,
        "summary": summary,
        "items": items,
    }


def build_historical_cycle_anomaly_cleanup_payload(
    store: Store,
    *,
    run_id: str,
    anomaly_types: Iterable[str] | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    target_types = sorted({str(item).strip() for item in (anomaly_types or ["formal_send_during_holding"]) if str(item).strip()})
    run_payload = build_run_cycle_audit_payload(store, run_id=run_id, limit=0)
    run_started_at = str(run_payload.get("started_at") or "")
    run_started_dt = parse_datetime(run_started_at)
    matched_rows: list[dict[str, Any]] = []
    decision_ids: list[str] = []
    affected_symbols: set[str] = set()
    for item in run_payload.get("items") or []:
        symbol = str(item.get("symbol") or "")
        anomalies = [
            anomaly
            for anomaly in (item.get("anomalies") or [])
            if str(anomaly.get("scope") or "") == "historical_carryover"
            and str(anomaly.get("type") or "") in target_types
        ]
        if not anomalies:
            continue
        if "formal_send_during_holding" in {str(anomaly.get("type") or "") for anomaly in anomalies}:
            for event in item.get("events") or []:
                if str(event.get("event_kind") or "") != "formal_send":
                    continue
                if str(event.get("status_before") or "") != "holding_active":
                    continue
                created_at = str(event.get("created_at") or "")
                created_dt = parse_datetime(created_at)
                if run_started_dt is not None and created_dt is not None and created_dt >= run_started_dt:
                    continue
                decision_id = str(event.get("decision_id") or "").strip()
                if not decision_id:
                    continue
                payload = event.get("payload") or {}
                matched_rows.append(
                    {
                        "decision_id": decision_id,
                        "symbol": symbol,
                        "event_kind": "formal_send",
                        "created_at": created_at,
                        "created_at_display": format_beijing_minute(created_at),
                        "status_before": str(event.get("status_before") or ""),
                        "status_after": str(event.get("status_after") or ""),
                        "action_label": str(event.get("action_label") or ""),
                        "entered": bool(payload.get("entered")),
                        "final_score": payload.get("final_score"),
                        "anomaly_type": "formal_send_during_holding",
                    }
                )
                decision_ids.append(decision_id)
                affected_symbols.add(symbol)
    unique_decision_ids = sorted(set(decision_ids))
    deleted = {"deleted_records": 0, "deleted_outcomes": 0}
    if apply and unique_decision_ids:
        deleted = store.delete_decision_history(unique_decision_ids)
        rebuild_cycle_audit(store, symbols=sorted(affected_symbols))
    return {
        "mode": "apply" if apply else "dry_run",
        "run_id": run_id,
        "run_started_at": run_started_at,
        "run_started_at_display": format_beijing_minute(run_started_at),
        "target_anomaly_types": target_types,
        "matched_decisions": len(unique_decision_ids),
        "affected_symbols": sorted(affected_symbols),
        "sample_rows": matched_rows[:20],
        "deleted_records": deleted["deleted_records"],
        "deleted_outcomes": deleted["deleted_outcomes"],
    }


def format_active_cycle_report(payload: dict[str, Any]) -> str:
    items = list(payload.get("items") or [])
    lines = [
        "当前活跃周期：",
        f"生成时间：{payload.get('generated_at_display', '-')}",
        f"样本数：{len(items)}",
    ]
    if not items:
        lines.append("当前没有可用的周期快照。")
        return "\n".join(lines)
    for item in items:
        latest_terminal = item.get("latest_terminal") or {}
        latest_suppression = item.get("latest_suppression") or {}
        lines.extend(
            [
                "",
                f"{item.get('symbol', '-')} | {item.get('status', '-')}",
                f"周期起点：{item.get('current_cycle_started_at_display', '-')}",
                f"最近动作：{item.get('previous_formal_action', '-')}",
                f"最近状态变化：{item.get('last_transition_at_display', '-')}",
                f"近72h进入候选池：{item.get('candidate_observation_count_72h', 0)} 次",
                (
                    f"最近 terminal：{latest_terminal.get('normalized_reason', '-')}"
                    if latest_terminal
                    else "最近 terminal：-"
                ),
                (
                    f"最近 suppress：{(latest_suppression.get('payload') or {}).get('previous_formal_action', '-')}"
                    f" -> {latest_suppression.get('action_label', '-')}"
                    if latest_suppression
                    else "最近 suppress：-"
                ),
            ]
        )
    return "\n".join(lines)


def format_cycle_audit_report(payload: dict[str, Any]) -> str:
    items = list(payload.get("items") or [])
    lines = [
        "周期链路审计：",
        f"生成时间：{payload.get('generated_at_display', '-')}",
        f"样本数：{len(items)}",
    ]
    if not items:
        lines.append("当前没有可用的周期链路。")
        return "\n".join(lines)
    for item in items:
        lines.extend(
            [
                "",
                f"[{item.get('symbol', '-')}] 状态：{item.get('status', '-')}",
                f"cycle_id：{item.get('cycle_id', '-')}",
                f"周期起点：{item.get('current_cycle_started_at_display', '-')}",
                f"最近正式动作：{item.get('previous_formal_action', '-')}",
                f"最近正式决策：{item.get('last_formal_decision_id', '-')}",
                f"最近终止原因：{item.get('last_terminal_reason', '-') or '-'}",
                f"近72h进入候选池：{item.get('candidate_observation_count_72h', 0)} 次",
            ]
        )
        observation_runs = list(item.get("candidate_observation_runs_72h") or [])
        if observation_runs:
            lines.append(
                "近72h候选来源："
                + " / ".join(
                    f"{row.get('run_id', '-')}@{row.get('created_at_display', '-')}"
                    for row in observation_runs[:5]
                )
            )
        latest_suppression = item.get("latest_suppression") or {}
        if latest_suppression:
            payload_part = latest_suppression.get("payload") or {}
            lines.append(
                "最近 suppress："
                f"{payload_part.get('previous_formal_action', '-')}"
                f" -> {latest_suppression.get('action_label', '-')}"
                f" @ {format_beijing_minute(str(latest_suppression.get('created_at') or ''))}"
            )
        lines.append("事件链：")
        for event in item.get("events") or []:
            lines.append(
                "  "
                + f"{event.get('created_at_display', '-')} | {event.get('event_kind', '-')}"
                + f" | {event.get('status_before', '-')}"
                + f" -> {event.get('status_after', '-')}"
                + f" | {event.get('action_label', '-')}"
                + (
                    f" | {event.get('normalized_reason', '-')}"
                    if str(event.get("normalized_reason") or "").strip()
                    else ""
                )
            )
    return "\n".join(lines)


def format_run_cycle_audit_report(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    status_counts = summary.get("status_counts") or {}
    lines = [
        "Run 周期异常审计：",
        f"run_id：{payload.get('run_id', '-')}",
        f"run_name：{payload.get('run_name', '-') or '-'}",
        f"started_at：{payload.get('started_at_display', '-') or '-'}",
        f"触达标的：{payload.get('symbol_count', 0)}",
        (
            "状态分布："
            f"pending={status_counts.get('pending_entry', 0)} / "
            f"holding={status_counts.get('holding_active', 0)} / "
            f"terminal={status_counts.get('terminal', 0)}"
        ),
        f"异常总数：{summary.get('anomaly_count', 0)}",
        (
            "异常分层："
            f"current_run={summary.get('current_run_anomaly_count', 0)} / "
            f"historical_carryover={summary.get('historical_carryover_anomaly_count', 0)}"
        ),
    ]
    anomaly_type_counts = summary.get("anomaly_type_counts") or {}
    if anomaly_type_counts:
        lines.append(
            "异常类型："
            + " / ".join(f"{key}={value}" for key, value in anomaly_type_counts.items())
        )
    reopened_cycle_symbols = list(summary.get("reopened_cycle_symbols") or [])
    if reopened_cycle_symbols:
        lines.append("已开新周期：" + ", ".join(reopened_cycle_symbols))
    anomaly_symbols = list(summary.get("anomaly_symbols") or [])
    if anomaly_symbols:
        lines.append("异常标的：" + ", ".join(anomaly_symbols))
    items = list(payload.get("items") or [])
    if not items:
        lines.append("当前 run 没有可用的周期样本。")
        return "\n".join(lines)
    lines.append("")
    for item in items:
        lines.append(
            f"{item.get('symbol', '-')} | status={item.get('status', '-')} | "
            f"cycles={item.get('cycle_count', 0)} | "
            f"candidate72h={item.get('candidate_observation_count_72h', 0)}"
        )
        if item.get("latest_terminal"):
            lines.append(
                "  "
                + f"latest_terminal={((item.get('latest_terminal') or {}).get('normalized_reason') or '-')}"
            )
        if item.get("latest_downgraded_watch"):
            lines.append("  downgraded_watch=yes")
        if item.get("latest_suppression"):
            lines.append("  suppressed_holding=yes")
        for anomaly in item.get("anomalies") or []:
            lines.append(
                "  "
                + f"anomaly={anomaly.get('type', '-')}"
                + f" scope={anomaly.get('scope', '-')}: {anomaly.get('message', '-')}"
            )
    return "\n".join(lines)


def format_historical_cycle_anomaly_cleanup(payload: dict[str, Any]) -> str:
    lines = [
        "历史周期异常清理：",
        f"模式：{'正式清理' if payload.get('mode') == 'apply' else '预演'}",
        f"run_id：{payload.get('run_id', '-')}",
        f"run_started_at：{payload.get('run_started_at_display', '-') or '-'}",
        f"目标异常：{'、'.join(payload.get('target_anomaly_types') or []) or '-'}",
        f"命中决策：{payload.get('matched_decisions', 0)}",
        f"影响标的：{', '.join(payload.get('affected_symbols') or []) or '-'}",
    ]
    if payload.get("mode") == "apply":
        lines.append(
            f"已删除：记录 {payload.get('deleted_records', 0)} / 后验 {payload.get('deleted_outcomes', 0)}"
        )
    else:
        lines.append("当前仅预演统计，未修改数据库。")
    sample_rows = payload.get("sample_rows") or []
    if sample_rows:
        lines.append("样本：")
        for row in sample_rows:
            lines.append(
                "  "
                f"{row.get('created_at_display', '-')} | {row.get('symbol', '-')} | "
                f"{row.get('action_label', '-')} | entered={row.get('entered', False)} | "
                f"{row.get('status_before', '-')}"
                f" -> {row.get('status_after', '-')}"
            )
    return "\n".join(lines)
