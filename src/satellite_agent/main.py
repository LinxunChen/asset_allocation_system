from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re
from statistics import median
import sys
from typing import Any
from zoneinfo import ZoneInfo

from .config import Settings
from .archive import archive_decision_history
from .entry_exit import EntryExitEngine
from .event_normalizer import EventNormalizer
from .llm import OpenAIExtractor, OpenAINarrator
from .decision_engines.mappers import build_delivery_view_from_record
from .market_data import (
    CachedMarketDataProvider,
    MarketDataEngine,
    MultiSourceMarketDataProvider,
    StooqDailyMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from .notifier import FeishuTransport, Notifier, build_render_view
from .outcomes import backfill_decision_outcomes, backfill_recent_decision_outcomes, explain_decision_outcome
from .reporting import (
    _config_summary,
    format_batch_index,
    format_batch_comparison,
    format_batch_replay,
    format_llm_usage_report_payload,
    format_recent_performance_review,
    format_run_review,
    summarize_run_health,
    format_error_summary,
    format_replay_evaluation,
    format_run_comparison,
    format_run_detail,
    format_runs,
    format_source_health,
    format_strategy_report,
    summarize_external_connectivity_issues,
    serialize_batch_comparison,
    serialize_batch_index,
    serialize_batch_replay,
    serialize_error_summary,
    serialize_llm_usage_report_payload,
    serialize_replay_evaluation,
    serialize_run_comparison,
    serialize_run_detail,
    serialize_runs,
    serialize_source_health,
    serialize_strategy_report,
)
from .runtime_config import (
    AgentRuntimeConfig,
    DEFAULT_CONFIG,
    load_default_template_runtime_config,
    write_default_config,
)
from .scoring import SignalScorer
from .service import SatelliteAgentService
from .sources import (
    CompositeSourceAdapter,
    GoogleNewsSourceAdapter,
    JsonlReplaySourceAdapter,
    SecFilingsSourceAdapter,
    StaticSourceAdapter,
)
from .store import Store
from .models import Bar, EventInsight, PrewatchCandidate, utcnow
from .models import OpportunityCard, PriceRange
from .theme_linkage import build_symbol_theme_map_from_watchlist_payload, theme_tags_for_symbol
from .timefmt import BEIJING_TZ, format_beijing_minute


SEC_EXCLUDED_SYMBOLS = {
    "ARM",
    "NBIS",
}
RECENT_PERFORMANCE_WINDOW_DAYS = 30
BASELINE_PERFORMANCE_WINDOW_DAYS = 90
SHORT_TERM_RECOMMENDATION_MIN_SAMPLES = 3
BASELINE_RECOMMENDATION_MIN_SAMPLES = 5
OUTCOME_SAMPLE_AUDIT_LIMIT = 10
DEFAULT_ARCHIVE_DB_PATH = Path("./data/satellite_agent/archive/decision_history.db")
HISTORICAL_EFFECT_REVIEW_VERSION = "v1"
AI_OUTCOME_REVIEW_STATE_KEY = "historical_effect_ai_review"
LEGACY_MANUAL_OUTCOME_AUDIT_STATE_KEY = "historical_effect_manual_audit"
HISTORICAL_EFFECT_BASELINE_STATE_KEY = "historical_effect_review_baseline"
EXECUTABLE_DECISION_ACTIONS = ("试探建仓", "确认做多")
OBSERVATION_DECISION_ACTION = "加入观察"
US_MARKET_TZ = ZoneInfo("America/New_York")
SERVE_HISTORICAL_EFFECT_REFRESH_SECONDS = 3600
ACTIVE_MONTHLY_REVIEW_MONTHS = 3
LLM_USAGE_REPORT_WINDOW_DAYS = 7
SERVE_LLM_USAGE_REFRESH_SECONDS = 3600
RUN_ONCE_WORKSPACE_DIR = "./data/satellite_agent/run_once"
SERVE_WORKSPACE_DIR = "./data/satellite_agent/serve"
DAILY_RUN_WORKSPACE_DIR = "./data/satellite_agent/daily_run"
BATCH_RUNS_DIR = "./data/satellite_agent/experiments/batch_runs"
DEMO_FLOW_DIR = "./data/satellite_agent/experiments/demo_flow"
SATELLITE_BATCH_REPLAY_TEMPLATE_PATH = "./config/satellite_agent/batch_replay.template.json"
LEGACY_BATCH_REPLAY_TEMPLATE_PATH = "./config/batch_replay.template.json"
SATELLITE_THEME_REFERENCE_PATH = "./config/satellite_agent/theme_reference.json"
SATELLITE_FORMAL_WATCHLIST_EXCLUSIONS = {
    "SPY",
    "QQQ",
    "QQQM",
    "VOO",
    "SCHD",
    "TLT",
    "GLD",
    "BRK.B",
}


def _parse_local_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid date: {value}. Expected YYYY-MM-DD.") from exc


def _historical_effect_event_type_label(event_type: str) -> str:
    return {
        "earnings": "财报",
        "guidance": "指引",
        "sec": "公告",
        "research": "研报",
        "m&a": "并购",
        "strategic": "战略合作",
        "product": "产品",
        "news": "新闻",
        "prewatch": "预备观察",
        "uncategorized": "未分类",
    }.get(event_type, event_type)


def _historical_effect_pool_label(pool: str) -> str:
    return {
        "prewatch": "预备池",
        "confirmation": "确认池",
        "exit": "兑现池",
        "unknown": "未分类",
    }.get(pool, pool or "未分类")


def _historical_effect_action_label(action: str) -> str:
    return {
        "试探建仓": "试探建仓",
        "确认做多": "确认做多",
        "加入观察": "加入观察",
        "unknown": "未分类",
    }.get(action, action or "未分类")


def _historical_effect_trigger_mode_label(trigger_mode: str) -> str:
    return {
        "event": "事件触发",
        "structure": "结构预热成卡",
        "resonance": "共振触发",
        "direct": "直接成卡",
        "promoted": "升池触发",
        "unknown": "未分类",
    }.get(trigger_mode, trigger_mode or "未分类")


def _historical_effect_priority_label(priority: str) -> str:
    return {
        "high": "高优先级",
        "normal": "普通",
        "suppressed": "压制",
        "unknown": "未分类",
    }.get(priority, priority or "未分类")


def _candidate_evaluation_stage_label(stage: str) -> str:
    return {
        "prewatch": "预备池",
        "confirmation": "确认机会",
    }.get(stage, stage or "未分类")


def _candidate_evaluation_reason_label(reason: str) -> str:
    normalized = str(reason or "").strip()
    mapping = {
        "below_min_score": "低于观察阈值",
        "bearish_trend": "趋势偏弱",
        "overheated_without_breakout": "过热但未突破",
        "ranked_below_run_cap": "排在本轮候选上限之外",
        "threshold_not_met": "未达到确认阈值",
        "execution_ineligible": "不满足执行条件",
        "event_only_below_threshold": "事件降级分数不足",
        "confirmation_opportunity": "形成确认机会",
        "passed_scan_threshold": "通过预备池初筛",
        "ranked_in_run": "进入本轮观察",
    }
    if normalized in mapping:
        return mapping[normalized]
    if normalized.startswith("build_failed:"):
        return f"构建失败（{normalized.split(':', 1)[1] or 'Unknown'}）"
    if normalized.startswith("scoring_failed:"):
        return f"评分失败（{normalized.split(':', 1)[1] or 'Unknown'}）"
    return normalized or "未说明"


def _summarize_candidate_evaluation_rows(
    rows: list[dict[str, Any]],
    *,
    stage: str,
    limit: int = 3,
) -> dict[str, Any]:
    outcome_counts = Counter(str(row.get("outcome") or "").strip() or "unknown" for row in rows)
    blocked_reason_counts: Counter[str] = Counter()
    error_reason_counts: Counter[str] = Counter()
    for row in rows:
        outcome = str(row.get("outcome") or "").strip()
        reason = str(row.get("reason") or "").strip() or "unknown"
        if outcome in {"rejected", "not_selected"}:
            blocked_reason_counts[reason] += 1
        elif outcome == "error":
            error_reason_counts[reason] += 1

    def _serialize_reason_counts(counter: Counter[str]) -> list[dict[str, Any]]:
        ordered = sorted(
            counter.items(),
            key=lambda item: (-item[1], _candidate_evaluation_reason_label(item[0]), item[0]),
        )
        return [
            {
                "reason": reason,
                "label": _candidate_evaluation_reason_label(reason),
                "count": count,
            }
            for reason, count in ordered[:limit]
        ]

    return {
        "stage": stage,
        "stage_label": _candidate_evaluation_stage_label(stage),
        "total_count": len(rows),
        "selected_count": int(outcome_counts.get("selected", 0)),
        "not_selected_count": int(outcome_counts.get("not_selected", 0)),
        "rejected_count": int(outcome_counts.get("rejected", 0)),
        "error_count": int(outcome_counts.get("error", 0)),
        "top_blocked_reasons": _serialize_reason_counts(blocked_reason_counts),
        "top_error_reasons": _serialize_reason_counts(error_reason_counts),
    }


def _build_candidate_evaluation_summary(
    rows: list[dict[str, Any]],
    *,
    limit: int = 3,
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {
        "prewatch": [],
        "confirmation": [],
    }
    for row in rows:
        stage = str(row.get("stage") or "").strip()
        if stage in grouped:
            grouped[stage].append(row)
    return {
        "prewatch": _summarize_candidate_evaluation_rows(grouped["prewatch"], stage="prewatch", limit=limit),
        "confirmation": _summarize_candidate_evaluation_rows(grouped["confirmation"], stage="confirmation", limit=limit),
    }


def _build_candidate_reason_trend_rows(
    recent_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    recent_counts: Counter[str] = Counter()
    baseline_counts: Counter[str] = Counter()
    for row in recent_rows:
        outcome = str(row.get("outcome") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if outcome in {"rejected", "not_selected", "error"} and reason:
            recent_counts[reason] += 1
    for row in baseline_rows:
        outcome = str(row.get("outcome") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if outcome in {"rejected", "not_selected", "error"} and reason:
            baseline_counts[reason] += 1
    if not recent_counts and not baseline_counts:
        return []
    ordered_reasons = sorted(
        set(recent_counts) | set(baseline_counts),
        key=lambda reason: (
            -recent_counts.get(reason, 0),
            -baseline_counts.get(reason, 0),
            _candidate_evaluation_reason_label(reason),
            reason,
        ),
    )
    items: list[dict[str, Any]] = []
    for reason in ordered_reasons[:limit]:
        recent_count = int(recent_counts.get(reason, 0))
        baseline_count = int(baseline_counts.get(reason, 0))
        if recent_count <= 0 and baseline_count <= 0:
            continue
        items.append(
            {
                "reason": reason,
                "label": _candidate_evaluation_reason_label(reason),
                "recent_count": recent_count,
                "baseline_count": baseline_count,
            }
        )
    return items


def _build_candidate_evaluation_trend_summary(
    *,
    recent_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    recent_window_days: int,
    baseline_window_days: int,
    limit: int = 3,
) -> dict[str, Any]:
    grouped_recent: dict[str, list[dict[str, Any]]] = {
        "prewatch": [],
        "confirmation": [],
    }
    grouped_baseline: dict[str, list[dict[str, Any]]] = {
        "prewatch": [],
        "confirmation": [],
    }
    for row in recent_rows:
        stage = str(row.get("stage") or "").strip()
        if stage in grouped_recent:
            grouped_recent[stage].append(row)
    for row in baseline_rows:
        stage = str(row.get("stage") or "").strip()
        if stage in grouped_baseline:
            grouped_baseline[stage].append(row)
    result: dict[str, Any] = {
        "recent_window_days": recent_window_days,
        "baseline_window_days": baseline_window_days,
    }
    for stage in ("prewatch", "confirmation"):
        result[stage] = {
            "stage": stage,
            "stage_label": _candidate_evaluation_stage_label(stage),
            "recent_total_count": len(grouped_recent[stage]),
            "baseline_total_count": len(grouped_baseline[stage]),
            "reason_trends": _build_candidate_reason_trend_rows(
                grouped_recent[stage],
                grouped_baseline[stage],
                limit=limit,
            ),
        }
    return result


def _parameter_label(parameter_key: str) -> str:
    return {
        "prewatch_min_score": "观察门槛",
        "max_prewatch_candidates_per_run": "观察候选上限",
        "prewatch_event_min_score": "事件预热观察门槛",
        "event_score_threshold": "确认阈值",
        "confirmation_bonus": "确认加分项",
        "execution_entry_buffer_pct": "入场缓冲",
        "execution_invalidation_buffer_pct": "失效缓冲",
        "take_profit_pct": "止盈区间",
        "take_profit_extension_pct": "扩展止盈区",
        "trend_filter_strength": "趋势过滤强度",
    }.get(parameter_key, parameter_key)


def _parameter_direction_label(direction: str) -> str:
    return {
        "high": "更可能偏高",
        "low": "更可能偏低",
        "inspect": "先排查",
    }.get(direction, direction or "先排查")


def _build_recommendation_item(
    *,
    text: str,
    priority: int,
    parameter_hints: list[tuple[str, str]] | None = None,
    source: str = "",
) -> dict[str, Any]:
    parameter_details: list[dict[str, str]] = []
    seen_parameter_keys: set[str] = set()
    for item in parameter_hints or []:
        key = str(item[0] or "").strip()
        direction = str(item[1] or "").strip() or "inspect"
        if not key or key in seen_parameter_keys:
            continue
        seen_parameter_keys.add(key)
        parameter_details.append(
            {
                "key": key,
                "label": _parameter_label(key),
                "direction": direction,
                "direction_label": _parameter_direction_label(direction),
            }
        )
    return {
        "text": text,
        "priority": priority,
        "source": source,
        "parameter_keys": [row["key"] for row in parameter_details],
        "parameter_labels": [row["label"] for row in parameter_details],
        "parameter_directions": [row["direction"] for row in parameter_details],
        "parameter_direction_labels": [row["direction_label"] for row in parameter_details],
        "parameter_details": parameter_details,
    }


def _candidate_trend_recommendation_for_reason(*, stage: str, reason: str) -> dict[str, Any] | None:
    normalized = str(reason or "").strip()
    if stage == "prewatch":
        if normalized == "below_min_score":
            return _build_recommendation_item(
                text="近 7 天预备池更多卡在低于观察阈值，优先回看观察门槛是否偏高。",
                priority=10,
                parameter_hints=[("prewatch_min_score", "high")],
                source="candidate_trend",
            )
        if normalized == "ranked_below_run_cap":
            return _build_recommendation_item(
                text="近 7 天预备池更常排在候选上限之外，优先回看候选上限或预备池排序逻辑。",
                priority=10,
                parameter_hints=[("max_prewatch_candidates_per_run", "low")],
                source="candidate_trend",
            )
        if normalized in {"bearish_trend", "overheated_without_breakout"}:
            return _build_recommendation_item(
                text="近 7 天预备池更多被市场结构挡掉，优先检查趋势与过热过滤是否过严。",
                priority=10,
                parameter_hints=[("trend_filter_strength", "inspect")],
                source="candidate_trend",
            )
        if normalized.startswith("build_failed:"):
            return _build_recommendation_item(
                text="近 7 天预备池构建异常增多，优先排查预备池快照或指标生成链路。",
                priority=10,
                source="candidate_trend",
            )
    if stage == "confirmation":
        if normalized == "threshold_not_met":
            return _build_recommendation_item(
                text="近 7 天确认机会更多卡在未达到确认阈值，优先回看确认门槛或加分项设置。",
                priority=10,
                parameter_hints=[("event_score_threshold", "high"), ("confirmation_bonus", "low")],
                source="candidate_trend",
            )
        if normalized == "execution_ineligible":
            return _build_recommendation_item(
                text="近 7 天确认机会更多卡在执行条件，优先检查入场区、失效位和执行约束是否过严。",
                priority=10,
                parameter_hints=[
                    ("execution_entry_buffer_pct", "inspect"),
                    ("execution_invalidation_buffer_pct", "inspect"),
                ],
                source="candidate_trend",
            )
        if normalized == "event_only_below_threshold":
            return _build_recommendation_item(
                text="近 7 天事件降级样本更常卡在分数不足，优先回看事件降级阈值和文案降级逻辑。",
                priority=10,
                parameter_hints=[("prewatch_event_min_score", "high"), ("event_score_threshold", "high")],
                source="candidate_trend",
            )
        if normalized.startswith("scoring_failed:"):
            return _build_recommendation_item(
                text="近 7 天确认评分异常增多，优先排查确认阶段评分链路。",
                priority=10,
                source="candidate_trend",
            )
    return None


def _build_candidate_trend_recommendations(summary: dict[str, Any]) -> list[dict[str, Any]]:
    recent_window_days = int(summary.get("recent_window_days", 0) or 0)
    baseline_window_days = int(summary.get("baseline_window_days", 0) or 0)
    if recent_window_days <= 0 or baseline_window_days <= 0:
        return []
    recommendations: list[dict[str, Any]] = []
    for stage in ("prewatch", "confirmation"):
        stage_summary = summary.get(stage) or {}
        for item in stage_summary.get("reason_trends") or []:
            recent_count = int(item.get("recent_count", 0) or 0)
            baseline_count = int(item.get("baseline_count", 0) or 0)
            if recent_count < 2 or baseline_count <= 0:
                continue
            recent_rate = recent_count / max(recent_window_days, 1)
            baseline_rate = baseline_count / max(baseline_window_days, 1)
            if recent_rate < baseline_rate * 1.5:
                continue
            recommendation = _candidate_trend_recommendation_for_reason(
                stage=stage,
                reason=str(item.get("reason") or ""),
            )
            if recommendation and recommendation["text"] not in {row["text"] for row in recommendations}:
                recommendations.append(recommendation)
            if len(recommendations) >= 2:
                return recommendations
    return recommendations


def _prioritize_recommendations(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    indexed_items = list(enumerate(items))
    for _, item in sorted(indexed_items, key=lambda item: (int(item[1].get("priority", 999) or 999), item[0])):
        text = str(item.get("text") or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(item)
    return ordered


def _historical_effect_exit_reason_label(reason: str) -> str:
    return {
        "exit_pool": "兑现池退出（含达标止盈 / 提前锁盈 / 宏观保护）",
        "hit_take_profit": "兑现池退出（含达标止盈 / 提前锁盈 / 宏观保护）",
        "hit_invalidation": "失效退出",
        "window_complete": "复盘窗口结算",
        "insufficient_lookahead": "观察中",
        "not_entered": "未成交",
    }.get(reason, reason or "未回补")


def _historical_effect_status_label(*, entered: bool, close_reason: str) -> str:
    if not entered:
        return "未成交"
    return _historical_effect_exit_reason_label(close_reason)


def _is_profit_exit_reason(reason: str) -> bool:
    return str(reason or "").strip() in {"exit_pool", "hit_take_profit"}


def _close_reason_equivalent_for_audit(
    stored_reason: str,
    recomputed_reason: str,
    *,
    recomputed_subreason: str = "",
    legacy_take_profit_triggered: bool = False,
) -> bool:
    stored = str(stored_reason or "").strip()
    recomputed = str(recomputed_reason or "").strip()
    if stored == recomputed:
        return True
    return (
        stored == "hit_take_profit"
        and (
            (
                recomputed == "exit_pool"
                and str(recomputed_subreason or "").strip() == "target_hit"
            )
            or (recomputed == "insufficient_lookahead" and legacy_take_profit_triggered)
        )
    )


def _should_tolerate_legacy_take_profit_prices(
    row: dict[str, Any],
    recomputed: Any,
    *,
    legacy_take_profit_triggered: bool = False,
) -> bool:
    if not _close_reason_equivalent_for_audit(
        str(row.get("close_reason") or ""),
        str(getattr(recomputed, "close_reason", "") or ""),
        recomputed_subreason=str(getattr(recomputed, "exit_subreason", "") or ""),
        legacy_take_profit_triggered=legacy_take_profit_triggered,
    ):
        return False
    take_profit_range = (_extract_sample_price_context(dict(row)).get("take_profit_range") or {})
    take_profit_low = take_profit_range.get("low")
    stored_exit = row.get("exit_price")
    if take_profit_low is None or stored_exit is None:
        return False
    try:
        return abs(float(stored_exit) - float(take_profit_low)) <= 0.01
    except (TypeError, ValueError):
        return False


def _serialize_ranked_decision_rows(
    rows: list[dict[str, Any]],
    *,
    metric_field: str,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "created_at": str(row.get("created_at") or ""),
                "symbol": str(row.get("symbol") or ""),
                "event_type": str(row.get("event_type") or ""),
                "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
                "action": str(row.get("action") or ""),
                "action_display": _historical_effect_action_label(str(row.get("action") or "")),
                "status_label": _historical_effect_status_label(
                    entered=bool(row.get("entered")),
                    close_reason=str(row.get("close_reason") or ""),
                ),
                "entered": bool(row.get("entered")),
                "close_reason": str(row.get("close_reason") or ""),
                "real_exit_label": (
                    "已真实退出"
                if str(row.get("close_reason") or "") in {"exit_pool", "hit_take_profit", "hit_invalidation", "window_complete"}
                    and row.get("realized_return") is not None
                    else "未真实退出"
                ),
                "entry_price": row.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "realized_return": row.get("realized_return"),
                "t_plus_7_return": row.get("t_plus_7_return"),
                "t_plus_10_return": row.get("t_plus_10_return"),
                "holding_days": row.get("holding_days"),
                "rank_metric_field": metric_field,
            }
        )
    return items


def _resolve_review_window(*, days: int, start_date: str = "", end_date: str = "") -> dict[str, Any]:
    today = utcnow().astimezone(BEIJING_TZ).date()
    start_raw = str(start_date or "").strip()
    end_raw = str(end_date or "").strip()
    if end_raw and not start_raw:
        raise SystemExit("end-date requires start-date. Use --start-date YYYY-MM-DD --end-date YYYY-MM-DD, or use --days.")
    if start_raw:
        start_day = _parse_local_date(start_raw)
        end_day = _parse_local_date(end_raw) if end_raw else today
    else:
        if days <= 0:
            raise SystemExit("days must be positive.")
        end_day = today
        start_day = end_day - timedelta(days=days - 1)
    if end_day < start_day:
        raise SystemExit("end-date must be on or after start-date.")
    start_dt = datetime.combine(start_day, datetime.min.time(), tzinfo=BEIJING_TZ)
    end_exclusive_dt = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), tzinfo=BEIJING_TZ)
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "window_days": (end_day - start_day).days + 1,
        "since": start_dt.astimezone(timezone.utc).isoformat(),
        "until": end_exclusive_dt.astimezone(timezone.utc).isoformat(),
    }


def _resolve_month_review_window(month: str) -> dict[str, Any]:
    token = str(month or "").strip()
    match = re.fullmatch(r"(\d{4})-(\d{2})", token)
    if match is None:
        raise SystemExit("month must be in YYYY-MM format.")
    year = int(match.group(1))
    month_value = int(match.group(2))
    if month_value < 1 or month_value > 12:
        raise SystemExit("month must be in YYYY-MM format.")
    start_day = date(year, month_value, 1)
    if month_value == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month_value + 1, 1)
    end_day = next_month_start - timedelta(days=1)
    window = _resolve_review_window(days=1, start_date=start_day.isoformat(), end_date=end_day.isoformat())
    window["month"] = token
    return window


def _review_window_slug(window: dict[str, Any]) -> str:
    start_date = str(window.get("start_date") or "").strip()
    end_date = str(window.get("end_date") or "").strip()
    if start_date and end_date and start_date != end_date:
        return f"{start_date}_to_{end_date}"
    return start_date or end_date or "window"


def _is_default_rolling_review_request(*, days: int, start_date: str = "", end_date: str = "", month: str = "") -> bool:
    return (
        not str(start_date or "").strip()
        and not str(end_date or "").strip()
        and not str(month or "").strip()
        and int(days) == RECENT_PERFORMANCE_WINDOW_DAYS
    )


def _iter_recent_month_tokens(*, reference_day: date, count: int) -> list[str]:
    if count <= 0:
        return []
    tokens: list[str] = []
    year = reference_day.year
    month_value = reference_day.month
    for _ in range(count):
        tokens.append(f"{year:04d}-{month_value:02d}")
        month_value -= 1
        if month_value <= 0:
            month_value = 12
            year -= 1
    return tokens


def _resolve_historical_effect_output_paths(
    *,
    workspace_dir: Path,
    window: dict[str, Any],
    days: int,
    start_date: str = "",
    end_date: str = "",
    month: str = "",
) -> dict[str, Any]:
    historical_effect_dir = workspace_dir / "historical_effect"
    historical_effect_dir.mkdir(parents=True, exist_ok=True)
    month_token = str(month or "").strip()
    if month_token:
        output_dir = historical_effect_dir / "monthly" / month_token
        scope = "monthly"
        scope_label = f"活的月报 {month_token}"
        slug = month_token
    elif _is_default_rolling_review_request(days=days, start_date=start_date, end_date=end_date, month=month):
        output_dir = historical_effect_dir
        scope = "rolling"
        scope_label = f"滚动近 {RECENT_PERFORMANCE_WINDOW_DAYS} 天"
        slug = "rolling-30d"
    else:
        slug = _review_window_slug(window)
        output_dir = historical_effect_dir / "windows" / slug
        scope = "window"
        scope_label = "自定义窗口"
    output_dir.mkdir(parents=True, exist_ok=True)
    return {
        "review_scope": scope,
        "review_scope_label": scope_label,
        "review_slug": slug,
        "output_dir": output_dir,
        "performance_review_path": output_dir / "review.md",
        "payload_path": output_dir / "review_payload.json",
        "sample_audit_path": output_dir / "sample_audit.md",
        "sample_audit_payload_path": output_dir / "sample_audit_payload.json",
    }


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 2)


def _percentage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round((numerator / denominator) * 100.0, 2)


def _parse_iso_datetime_safe(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _days_since_timestamp(value: Any, *, reference: datetime) -> float | None:
    parsed = _parse_iso_datetime_safe(value)
    if parsed is None:
        return None
    return (reference - parsed).total_seconds() / 86400.0


def _rounded_float(value: Any, *, digits: int = 4) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _representative_outcome_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        str(row.get("run_id") or ""),
        str(row.get("symbol") or "").strip().upper(),
        str(row.get("action") or "").strip(),
        str(row.get("event_type") or "").strip(),
        str(row.get("pool") or "").strip(),
        str(row.get("created_at") or "").strip(),
        str(row.get("entered_at") or "").strip(),
        _rounded_float(row.get("entry_price")),
        _rounded_float(row.get("exit_price")),
        _rounded_float(row.get("max_runup")),
        _rounded_float(row.get("max_drawdown")),
        _rounded_float(row.get("realized_return")),
        str(row.get("close_reason") or "").strip(),
    )


def _dedupe_representative_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    merged_count = 0
    for row in rows:
        key = _representative_outcome_key(row)
        if key in seen:
            merged_count += 1
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, merged_count


def _select_unique_top_rows(
    rows: list[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], tuple[Any, ...]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        dedupe_key = key_fn(row)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        selected.append(row)
        if len(selected) >= limit:
            break
    return selected


def _timing_days_from_row(row: dict[str, Any]) -> tuple[float | None, bool]:
    created_at_dt = _parse_iso_datetime_safe(row.get("created_at"))
    entered_at_dt = _parse_iso_datetime_safe(row.get("entered_at"))
    if created_at_dt is None or entered_at_dt is None:
        return None, False
    days = (entered_at_dt - created_at_dt).total_seconds() / 86400.0
    if days < 0:
        return None, True
    return days, False


def _observation_to_confirmation_days(
    observation_rows: list[dict[str, Any]],
    promoted_rows: list[dict[str, Any]],
) -> list[float]:
    earliest_observation_by_symbol: dict[str, datetime] = {}
    for row in observation_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        created_at_dt = _parse_iso_datetime_safe(row.get("created_at"))
        if not symbol or created_at_dt is None:
            continue
        current = earliest_observation_by_symbol.get(symbol)
        if current is None or created_at_dt < current:
            earliest_observation_by_symbol[symbol] = created_at_dt
    deltas: list[float] = []
    for row in promoted_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        promoted_at_dt = _parse_iso_datetime_safe(row.get("created_at"))
        observed_at_dt = earliest_observation_by_symbol.get(symbol)
        if not symbol or promoted_at_dt is None or observed_at_dt is None:
            continue
        delta_days = (promoted_at_dt - observed_at_dt).total_seconds() / 86400.0
        if delta_days < 0:
            continue
        deltas.append(delta_days)
    return deltas


def _locked_checkpoint_return(
    row: dict[str, Any],
    *,
    day: int,
    review_until_dt: datetime,
) -> float | None:
    field = f"t_plus_{day}_return"
    elapsed_days = _days_since_timestamp(row.get("entered_at"), reference=review_until_dt)
    raw_value = row.get(field)
    if raw_value is not None and elapsed_days is not None and elapsed_days >= float(day):
        return float(raw_value)
    close_reason = str(row.get("close_reason") or "").strip()
    realized_return = row.get("realized_return")
    holding_days = row.get("holding_days")
    if (
        realized_return is not None
        and close_reason in {"exit_pool", "hit_take_profit", "hit_invalidation", "window_complete"}
        and holding_days is not None
        and float(holding_days) <= float(day)
    ):
        return float(realized_return)
    return None


def _build_path_quality_summary(
    rows: list[dict[str, Any]],
    *,
    review_until_dt: datetime,
) -> dict[str, Any]:
    raw_entered_rows = [row for row in rows if bool(row.get("entered"))]
    entered_rows, deduped_merge_count = _dedupe_representative_rows(raw_entered_rows)
    runup_values = [float(row["max_runup"]) for row in entered_rows if row.get("max_runup") is not None]
    drawdown_values = [float(row["max_drawdown"]) for row in entered_rows if row.get("max_drawdown") is not None]
    elapsed_days_values = [
        float(days_elapsed)
        for row in entered_rows
        for days_elapsed in [_days_since_timestamp(row.get("entered_at"), reference=review_until_dt)]
        if days_elapsed is not None and days_elapsed >= 0.0
    ]
    checkpoint_rows: list[dict[str, Any]] = []
    for day in (7, 14, 30):
        values = [
            value
            for row in entered_rows
            for value in [_locked_checkpoint_return(row, day=day, review_until_dt=review_until_dt)]
            if value is not None
        ]
        checkpoint_rows.append(
            {
                "day": day,
                "label": f"T+{day}",
                "sample_count": len(values),
                "avg_return": _mean(values),
                "median_return": _median(values),
                "positive_rate": _percentage(sum(1 for value in values if value > 0), len(values)),
            }
        )
    observation_line = "当前还没有足够的已成交样本来判断路径质量。"
    if runup_values or drawdown_values:
        observation_parts: list[str] = []
        avg_runup = _mean(runup_values)
        avg_drawdown = _mean(drawdown_values)
        if avg_runup is not None and avg_runup > 0:
            observation_parts.append("样本整体曾给出一定浮盈空间")
        if avg_drawdown is not None and avg_drawdown <= -5.0:
            observation_parts.append("平均回撤偏深")
        mature_t14 = next((row for row in checkpoint_rows if int(row.get("day", 0) or 0) == 14), {})
        mature_t30 = next((row for row in checkpoint_rows if int(row.get("day", 0) or 0) == 30), {})
        if int(mature_t14.get("sample_count", 0) or 0) > 0 and (mature_t14.get("avg_return") or 0.0) < 0:
            observation_parts.append("T+14 后续衰减明显")
        if int(mature_t30.get("sample_count", 0) or 0) > 0 and (mature_t30.get("avg_return") or 0.0) < 0:
            observation_parts.append("T+30 延续性不足")
        if observation_parts:
            observation_line = "；".join(observation_parts) + "。"
        else:
            observation_line = "路径质量整体中性，仍需继续观察更长样本。"

    top_runup_rows = _select_unique_top_rows(
        sorted(
            [row for row in entered_rows if row.get("max_runup") is not None],
            key=lambda row: (
                float(row.get("max_runup") or 0.0),
                str(row.get("created_at") or ""),
            ),
            reverse=True,
        ),
        key_fn=lambda row: (str(row.get("symbol") or "").strip().upper(),),
        limit=3,
    )
    best_runup_samples = [
        {
            "symbol": str(row.get("symbol") or ""),
            "event_type": str(row.get("event_type") or ""),
            "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
            "action": str(row.get("action") or ""),
            "action_display": _historical_effect_action_label(str(row.get("action") or "")),
            "max_runup": row.get("max_runup"),
            "max_drawdown": row.get("max_drawdown"),
            "created_at": str(row.get("created_at") or ""),
        }
        for row in top_runup_rows
    ]
    top_drawdown_rows = _select_unique_top_rows(
        sorted(
            [row for row in entered_rows if row.get("max_drawdown") is not None],
            key=lambda row: (
                float(row.get("max_drawdown") or 0.0),
                str(row.get("created_at") or ""),
            ),
        ),
        key_fn=lambda row: (str(row.get("symbol") or "").strip().upper(),),
        limit=3,
    )
    worst_drawdown_samples = [
        {
            "symbol": str(row.get("symbol") or ""),
            "event_type": str(row.get("event_type") or ""),
            "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
            "action": str(row.get("action") or ""),
            "action_display": _historical_effect_action_label(str(row.get("action") or "")),
            "max_runup": row.get("max_runup"),
            "max_drawdown": row.get("max_drawdown"),
            "created_at": str(row.get("created_at") or ""),
        }
        for row in top_drawdown_rows
    ]
    return {
        "entered_sample_count": len(entered_rows),
        "deduped_merge_count": deduped_merge_count,
        "max_runup_sample_count": len(runup_values),
        "avg_max_runup": _mean(runup_values),
        "median_max_runup": _median(runup_values),
        "max_drawdown_sample_count": len(drawdown_values),
        "avg_max_drawdown": _mean(drawdown_values),
        "median_max_drawdown": _median(drawdown_values),
        "elapsed_days_sample_count": len(elapsed_days_values),
        "avg_elapsed_days_since_entry": _mean(elapsed_days_values),
        "checkpoints": checkpoint_rows,
        "observation_line": observation_line,
        "best_runup_samples": best_runup_samples,
        "worst_drawdown_samples": worst_drawdown_samples,
    }


def _build_review_window_snapshot(
    review: dict[str, Any],
    *,
    label: str,
    role_label: str,
    min_sample_size: int,
) -> dict[str, Any]:
    overview = dict(review.get("overview") or {})
    execution_quality = dict(review.get("execution_quality") or {})
    pool_funnel = dict(review.get("pool_funnel_summary") or {})
    simulation_funnel = dict(review.get("simulation_funnel_summary") or {})
    trade_path = dict(review.get("trade_path_summary") or {})
    path_quality = dict(review.get("path_quality_summary") or {})
    decision_count = int(overview.get("decision_count", 0) or 0)
    return {
        "label": label,
        "role_label": role_label,
        "window_days": int((review.get("review_window") or {}).get("window_days") or 0),
        "overview": overview,
        "execution_quality": execution_quality,
        "pool_funnel_summary": pool_funnel,
        "simulation_funnel_summary": simulation_funnel,
        "trade_path_summary": trade_path,
        "path_quality_summary": path_quality,
        "sample_gate": {
            "decision_count": decision_count,
            "min_sample_size": min_sample_size,
            "sufficient": decision_count >= min_sample_size,
            "status_label": "样本足够" if decision_count >= min_sample_size else "样本不足",
        },
    }


def _comparison_metric_status(
    primary_value: Any,
    baseline_value: Any,
    *,
    higher_is_better: bool = True,
    tolerance: float = 0.05,
) -> str:
    if primary_value is None or baseline_value is None:
        return "样本不足"
    try:
        primary = float(primary_value)
        baseline = float(baseline_value)
    except (TypeError, ValueError):
        return "样本不足"
    if abs(primary - baseline) <= tolerance:
        return "基本持平"
    if higher_is_better:
        return "短期改善" if primary > baseline else "短期回撤"
    return "短期改善" if primary < baseline else "短期承压"


def _build_dual_window_comparison_summary(primary: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    primary_overview = primary.get("overview") or {}
    baseline_overview = baseline.get("overview") or {}
    primary_execution = primary.get("execution_quality") or {}
    baseline_execution = baseline.get("execution_quality") or {}
    primary_path = primary.get("path_quality_summary") or {}
    baseline_path = baseline.get("path_quality_summary") or {}
    metrics = [
        {
            "label": "决策总数",
            "primary_value": primary_overview.get("decision_count"),
            "baseline_value": baseline_overview.get("decision_count"),
            "value_type": "count",
            "status": _comparison_metric_status(primary_overview.get("decision_count"), baseline_overview.get("decision_count")),
        },
        {
            "label": "模拟成交率",
            "primary_value": primary_execution.get("entry_hit_rate"),
            "baseline_value": baseline_execution.get("entry_hit_rate"),
            "value_type": "rate",
            "status": _comparison_metric_status(primary_execution.get("entry_hit_rate"), baseline_execution.get("entry_hit_rate")),
        },
        {
            "label": "胜率",
            "primary_value": primary_overview.get("win_rate"),
            "baseline_value": baseline_overview.get("win_rate"),
            "value_type": "rate",
            "status": _comparison_metric_status(primary_overview.get("win_rate"), baseline_overview.get("win_rate")),
        },
        {
            "label": "平均最大浮盈",
            "primary_value": primary_path.get("avg_max_runup"),
            "baseline_value": baseline_path.get("avg_max_runup"),
            "value_type": "percent",
            "status": _comparison_metric_status(primary_path.get("avg_max_runup"), baseline_path.get("avg_max_runup")),
        },
        {
            "label": "平均最大回撤",
            "primary_value": primary_path.get("avg_max_drawdown"),
            "baseline_value": baseline_path.get("avg_max_drawdown"),
            "value_type": "percent",
            "status": _comparison_metric_status(primary_path.get("avg_max_drawdown"), baseline_path.get("avg_max_drawdown"), higher_is_better=False),
        },
    ]
    summary_lines: list[str] = []
    if not primary.get("sample_gate", {}).get("sufficient", False):
        summary_lines.append("近 30 天样本量不足，短期波动暂不支持直接调参数。")
    elif not baseline.get("sample_gate", {}).get("sufficient", False):
        summary_lines.append("近 90 天样本量仍偏少，短期变化暂时不能视为稳定结论。")
    else:
        win_rate_status = _comparison_metric_status(
            primary_overview.get("win_rate"),
            baseline_overview.get("win_rate"),
        )
        entry_rate_status = _comparison_metric_status(
            primary_execution.get("entry_hit_rate"),
            baseline_execution.get("entry_hit_rate"),
        )
        if win_rate_status == "短期回撤" and entry_rate_status == "短期回撤":
            summary_lines.append("近 30 天胜率和进场质量都弱于 90 天基准，短期表现明显转差。")
        elif win_rate_status == "短期改善" and entry_rate_status == "短期改善":
            summary_lines.append("近 30 天胜率和进场质量都好于 90 天基准，短期表现有改善。")
        else:
            summary_lines.append("近 30 天和 90 天表现存在分化，建议先看路径质量，不要只盯单一收益结果。")
    return {
        "metrics": metrics,
        "summary_lines": summary_lines,
    }


def _apply_dual_window_recommendation_guard(
    review: dict[str, Any],
    *,
    primary_snapshot: dict[str, Any],
    baseline_snapshot: dict[str, Any],
) -> None:
    primary_gate = primary_snapshot.get("sample_gate") or {}
    baseline_gate = baseline_snapshot.get("sample_gate") or {}
    if bool(primary_gate.get("sufficient")) and bool(baseline_gate.get("sufficient")):
        return
    if not bool(primary_gate.get("sufficient")):
        text = (
            f"近 30 天只有 {int(primary_gate.get('decision_count', 0) or 0)} 条可执行样本，样本量不足，"
            "近期表现即使偏弱或偏强，也不足以支持直接调整参数，不建议直接调整参数，建议先观察。"
        )
    else:
        text = (
            f"近 90 天只有 {int(baseline_gate.get('decision_count', 0) or 0)} 条可执行样本，样本量不足，"
            "当前中期基准仍偏薄，短期变化先以观察和排查为主，不建议直接调整参数。"
        )
    review["recommendations"] = [text]
    review["recommendation_details"] = [
        _build_recommendation_item(
            text=text,
            priority=5,
            source="sample_gate",
        )
    ]


def build_watchlist_config_review_payload(runtime_config: AgentRuntimeConfig, *, config_path: Path) -> dict[str, Any]:
    watchlist = runtime_config.watchlist
    symbol_theme_map = watchlist.symbol_theme_map()
    theme_display_name_map = watchlist.theme_display_name_map()
    display_name_map = watchlist.symbol_display_name_map()

    active_stock_symbols = list(watchlist.stocks)
    active_etf_symbols = list(watchlist.etfs)
    active_symbols = active_stock_symbols + active_etf_symbols
    active_symbol_set = set(active_symbols)

    excluded_symbols = sorted(symbol for symbol in active_symbols if symbol in SATELLITE_FORMAL_WATCHLIST_EXCLUSIONS)
    missing_display_names = sorted(symbol for symbol in active_symbols if not display_name_map.get(symbol))
    unthemed_symbols = sorted(symbol for symbol in active_symbols if not symbol_theme_map.get(symbol))
    disabled_symbols = sorted(
        symbol
        for symbol, item in {**watchlist.stock_items, **watchlist.etf_items}.items()
        if not item.enabled
    )
    uses_legacy_groups = bool(watchlist.stock_groups or watchlist.etf_groups)
    uses_explicit_themes = bool(watchlist.themes)

    theme_rows: list[dict[str, Any]] = []
    themes_without_active_members: list[dict[str, Any]] = []
    themes_with_off_watchlist_members: list[dict[str, Any]] = []
    for theme in watchlist.themes:
        members = list(theme.symbols) + list(theme.etfs)
        normalized_members = [str(symbol).strip().upper() for symbol in members if str(symbol).strip()]
        active_members = [symbol for symbol in normalized_members if symbol in active_symbol_set]
        off_watchlist_members = sorted(symbol for symbol in normalized_members if symbol not in active_symbol_set)
        row = {
            "theme_id": theme.theme_id,
            "display_name": theme.display_name or theme_display_name_map.get(theme.theme_id, theme.theme_id),
            "member_count": len(normalized_members),
            "active_member_count": len(active_members),
            "active_members": active_members,
            "off_watchlist_members": off_watchlist_members,
        }
        theme_rows.append(row)
        if not active_members:
            themes_without_active_members.append(row)
        if off_watchlist_members:
            themes_with_off_watchlist_members.append(row)

    symbol_rows: list[dict[str, Any]] = []
    for symbol in active_symbols:
        asset_type = "ETF" if symbol in watchlist.etfs else "股票"
        theme_ids = symbol_theme_map.get(symbol, [])
        theme_names = [
            theme_display_name_map.get(theme_id, theme_id)
            for theme_id in theme_ids
        ]
        issues: list[str] = []
        if symbol in excluded_symbols:
            issues.append("不建议保留在正式卫星观察池")
        if symbol in missing_display_names:
            issues.append("缺少展示名称")
        if symbol in unthemed_symbols:
            issues.append("未挂题材")
        symbol_rows.append(
            {
                "symbol": symbol,
                "display_name": display_name_map.get(symbol, ""),
                "asset_type": asset_type,
                "theme_ids": theme_ids,
                "theme_names": theme_names,
                "issues": issues,
            }
        )

    suggestions: list[str] = []
    if uses_legacy_groups:
        suggestions.append("当前配置仍在使用 legacy groups，建议后续只维护 watchlist 项，逐步把 groups 从用户配置里淡出。")
    if uses_explicit_themes:
        suggestions.append("当前配置仍在显式维护 themes；如果你希望只维护 watchlist，可以后续把 themes 交给系统内置题材目录托管。")
    if excluded_symbols:
        suggestions.append(
            "把不适合留在正式卫星观察池的宽基/宏观代理标的移出 watchlist，仅保留为内部环境代理。"
        )
    if missing_display_names:
        suggestions.append("给缺少展示名称的标的补齐公司名或 ETF 名称，避免推送里只显示代码。")
    if unthemed_symbols:
        suggestions.append("给未挂题材的活跃标的补齐 themes 映射，避免题材链路和扩散提示失真。")
    if themes_without_active_members:
        suggestions.append("清理没有活跃成员的空题材，或确认这些题材是否还需要保留。")
    if themes_with_off_watchlist_members:
        suggestions.append("检查题材里挂到观察池外的成员，确认是配置遗留还是故意保留的扩展映射。")
    if not suggestions:
        suggestions.append("当前卫星观察池和题材映射没有发现明显结构性问题。")

    return {
        "config_path": str(config_path),
        "active_stock_count": len(active_stock_symbols),
        "active_etf_count": len(active_etf_symbols),
        "active_symbol_count": len(active_symbols),
        "active_symbols": active_symbols,
        "excluded_symbols": excluded_symbols,
        "missing_display_names": missing_display_names,
        "unthemed_symbols": unthemed_symbols,
        "disabled_symbols": disabled_symbols,
        "uses_legacy_groups": uses_legacy_groups,
        "uses_explicit_themes": uses_explicit_themes,
        "themes_without_active_members": themes_without_active_members,
        "themes_with_off_watchlist_members": themes_with_off_watchlist_members,
        "theme_rows": theme_rows,
        "symbol_rows": symbol_rows,
        "suggestions": suggestions,
    }


def format_watchlist_config_review_payload(payload: dict[str, Any]) -> str:
    lines = [
        "卫星观察池与题材映射诊断：",
        f"配置文件：{payload.get('config_path', '-')}",
        (
            "活跃观察池："
            f" 股票 {payload.get('active_stock_count', 0)}"
            f" / ETF {payload.get('active_etf_count', 0)}"
            f" / 合计 {payload.get('active_symbol_count', 0)}"
        ),
    ]

    suggestions = list(payload.get("suggestions") or [])
    if suggestions:
        lines.append("建议优先处理：")
        for item in suggestions:
            lines.append(f"- {item}")

    lines.append(
        "配置形态："
        f" {'仍在使用 legacy groups' if payload.get('uses_legacy_groups') else '未使用 legacy groups'}"
        f" / {'显式维护 themes' if payload.get('uses_explicit_themes') else '未显式维护 themes'}"
    )

    excluded_symbols = list(payload.get("excluded_symbols") or [])
    lines.append(
        "正式卫星池内不建议保留的标的："
        + (" " + "、".join(excluded_symbols) if excluded_symbols else " 无")
    )

    missing_display_names = list(payload.get("missing_display_names") or [])
    lines.append(
        "缺少展示名称的标的："
        + (" " + "、".join(missing_display_names) if missing_display_names else " 无")
    )

    unthemed_symbols = list(payload.get("unthemed_symbols") or [])
    lines.append(
        "未挂题材的活跃标的："
        + (" " + "、".join(unthemed_symbols) if unthemed_symbols else " 无")
    )

    disabled_symbols = list(payload.get("disabled_symbols") or [])
    lines.append(
        "已禁用但仍保留在配置里的标的："
        + (" " + "、".join(disabled_symbols) if disabled_symbols else " 无")
    )

    empty_theme_rows = list(payload.get("themes_without_active_members") or [])
    lines.append("空题材：")
    if empty_theme_rows:
        for row in empty_theme_rows:
            lines.append(f"- {row.get('display_name', row.get('theme_id', '-'))}（{row.get('theme_id', '-') }）")
    else:
        lines.append("- 无")

    off_watchlist_rows = list(payload.get("themes_with_off_watchlist_members") or [])
    lines.append("题材中挂到观察池外的成员：")
    if off_watchlist_rows:
        for row in off_watchlist_rows:
            lines.append(
                f"- {row.get('display_name', row.get('theme_id', '-'))}："
                + "、".join(row.get("off_watchlist_members") or [])
            )
    else:
        lines.append("- 无")

    lines.append("当前活跃标的与题材：")
    for row in payload.get("symbol_rows") or []:
        display_name = str(row.get("display_name") or "").strip()
        identity = f"{display_name}（{row.get('symbol', '-') }）" if display_name else str(row.get("symbol", "-"))
        themes = "、".join(row.get("theme_names") or []) or "未挂题材"
        issues = "；".join(row.get("issues") or []) or "无"
        lines.append(f"- {identity} | {row.get('asset_type', '-')} | 题材：{themes} | 问题：{issues}")
    return "\n".join(lines)


def build_theme_reference_payload(runtime_config: AgentRuntimeConfig, *, config_path: Path) -> dict[str, Any]:
    watchlist = runtime_config.watchlist
    symbol_theme_map = watchlist.symbol_theme_map()
    theme_display_name_map = watchlist.theme_display_name_map()
    display_name_map = watchlist.symbol_display_name_map()

    active_symbols = list(watchlist.stocks) + list(watchlist.etfs)
    symbol_rows: list[dict[str, Any]] = []
    for symbol in active_symbols:
        theme_ids = symbol_theme_map.get(symbol, [])
        primary_theme_id = theme_ids[0] if theme_ids else ""
        symbol_rows.append(
            {
                "symbol": symbol,
                "display_name": display_name_map.get(symbol, ""),
                "asset_type": "ETF" if symbol in watchlist.etfs else "股票",
                "theme_id": primary_theme_id,
                "theme_name": theme_display_name_map.get(primary_theme_id, primary_theme_id) if primary_theme_id else "",
            }
        )

    used_theme_ids = sorted({str(row.get("theme_id") or "").strip() for row in symbol_rows if str(row.get("theme_id") or "").strip()})
    theme_catalog = [
        {
            "theme_id": theme_id,
            "display_name": theme_display_name_map.get(theme_id, theme_id),
        }
        for theme_id in used_theme_ids
    ]

    return {
        "_meta": {
            "managed_by": "satellite_agent",
            "editable": False,
            "config_path": str(config_path),
            "note": "这是系统托管的题材闭集与当前标的映射，仅供参考，不建议手工修改。",
        },
        "theme_catalog": theme_catalog,
        "symbol_theme_map": symbol_rows,
    }


def build_llm_usage_report_payload(store: Store, *, days: int) -> dict[str, Any]:
    end_at = utcnow()
    start_at = end_at - timedelta(days=max(1, days))
    rows = store.aggregate_llm_usage(
        start_at=start_at.isoformat(),
        end_at=end_at.isoformat(),
    )
    return serialize_llm_usage_report_payload(
        start_at=start_at.isoformat(),
        end_at=end_at.isoformat(),
        rows=rows,
    )


def build_write_llm_usage_report_payload(
    store: Store,
    *,
    workspace_dir: Path,
    days: int,
) -> dict[str, Any]:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    llm_usage_dir = workspace_dir / "llm_usage"
    report_path = llm_usage_dir / "report.md"
    payload_path = llm_usage_dir / "report_payload.json"
    report_payload = build_llm_usage_report_payload(store, days=days)
    report_text = format_llm_usage_report_payload(report_payload)
    _write_report(report_path, report_text)
    _write_json(payload_path, report_payload)
    return {
        "workspace_dir": str(workspace_dir),
        "days": days,
        "report_path": str(report_path),
        "payload_path": str(payload_path),
        "report": report_payload,
    }


def format_write_llm_usage_report_result(payload: dict[str, Any]) -> str:
    report = payload.get("report") or {}
    summary = report.get("summary") or {}
    return "\n".join(
        [
            "LLM 用量报告：",
            f"工作目录：{payload.get('workspace_dir', '-')}",
            f"统计窗口：最近 {payload.get('days', '-')} 天",
            f"真实调用数：{summary.get('actual_calls', 0)}",
            f"失败回退数：{summary.get('fallback_calls', 0)}",
            f"跳过调用数：{summary.get('skipped_calls', 0)}",
            f"报告文件：{payload.get('report_path', '-')}",
            f"结构化数据：{payload.get('payload_path', '-')}",
        ]
    )


def format_theme_reference_payload(payload: dict[str, Any]) -> str:
    meta = payload.get("_meta") or {}
    lines = [
        "卫星题材闭集参考：",
        f"配置文件：{meta.get('config_path', '-')}",
        f"说明：{meta.get('note', '-')}",
        "题材闭集：",
    ]
    for row in payload.get("theme_catalog") or []:
        lines.append(f"- {row.get('theme_id', '-')} = {row.get('display_name', '-')}")

    lines.append("当前标的与题材映射：")
    for row in payload.get("symbol_theme_map") or []:
        display_name = str(row.get("display_name") or "").strip()
        identity = f"{display_name}（{row.get('symbol', '-') }）" if display_name else str(row.get("symbol", "-"))
        theme_id = str(row.get("theme_id") or "").strip() or "未归类"
        theme_name = str(row.get("theme_name") or "").strip() or "未归类"
        lines.append(
            f"- {identity} | {row.get('asset_type', '-')} | theme_id={theme_id} | 主题：{theme_name}"
        )
    return "\n".join(lines)


def _merge_decision_rows(
    primary_rows: list[dict[str, Any]] | list[object],
    secondary_rows: list[dict[str, Any]] | list[object],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for rows in (secondary_rows, primary_rows):
        for raw_row in rows:
            row = dict(raw_row)
            decision_id = str(row.get("decision_id") or "")
            if not decision_id:
                continue
            merged[decision_id] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            str(row.get("created_at") or ""),
            str(row.get("symbol") or ""),
            str(row.get("pool") or ""),
        ),
    )


def _aggregate_historical_effect_rows(
    rows: list[dict[str, Any]],
    *,
    key_fn,
    label_fn,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = str(key_fn(row) or "").strip()
        if not key:
            key = "unknown"
        grouped.setdefault(key, []).append(row)
    items: list[dict[str, Any]] = []
    for key, group_rows in grouped.items():
        entered_rows = [row for row in group_rows if bool(row.get("entered"))]
        exited_rows = [
            row
            for row in entered_rows
            if str(row.get("close_reason") or "") in {"exit_pool", "hit_invalidation", "window_complete"}
            and row.get("realized_return") is not None
        ]
        realized_values = [float(row["realized_return"]) for row in exited_rows]
        take_profit_count = sum(1 for row in entered_rows if _is_profit_exit_reason(str(row.get("close_reason") or "")))
        invalidation_count = sum(1 for row in entered_rows if str(row.get("close_reason") or "") == "hit_invalidation")
        window_complete_count = sum(1 for row in entered_rows if str(row.get("close_reason") or "") == "window_complete")
        items.append(
            {
                "key": key,
                "label": label_fn(key),
                "decision_count": len(group_rows),
                "entered_count": len(entered_rows),
                "take_profit_exit_count": take_profit_count,
                "invalidation_exit_count": invalidation_count,
                "window_complete_count": window_complete_count,
                "avg_realized_return": _mean(realized_values),
                "win_rate": _percentage(sum(1 for value in realized_values if value > 0), len(realized_values)),
            }
        )
    return sorted(
        items,
        key=lambda row: (
            -(row["avg_realized_return"] if row["avg_realized_return"] is not None else float("-inf")),
            -int(row["decision_count"]),
            str(row["label"]),
        ),
    )


def _best_completed_group(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    completed = [row for row in rows if row.get("avg_realized_return") is not None]
    if not completed:
        return None
    return max(
        completed,
        key=lambda row: (
            float(row.get("avg_realized_return") or 0.0),
            int(row.get("decision_count") or 0),
        ),
    )


def _worst_completed_group(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    completed = [row for row in rows if row.get("avg_realized_return") is not None]
    if not completed:
        return None
    return min(
        completed,
        key=lambda row: (
            float(row.get("avg_realized_return") or 0.0),
            -int(row.get("decision_count") or 0),
        ),
    )


def _build_historical_effect_review_data(
    store: Store,
    *,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
    include_comparison: bool = True,
) -> dict[str, Any]:
    window = _resolve_review_window(days=days, start_date=start_date, end_date=end_date)
    archive_store, archive_db_path = _open_archive_store_if_available()
    try:
        main_rows = store.load_decision_records_for_window(
            since=window["since"],
            until=window["until"],
            actions=EXECUTABLE_DECISION_ACTIONS,
        )
        archive_rows = (
            archive_store.load_decision_records_for_window(
                since=window["since"],
                until=window["until"],
                actions=EXECUTABLE_DECISION_ACTIONS,
            )
            if archive_store is not None
            else []
        )
        observation_rows = store.load_decision_records_for_window(
            since=window["since"],
            until=window["until"],
            actions=[OBSERVATION_DECISION_ACTION],
        )
        archive_observation_rows = (
            archive_store.load_decision_records_for_window(
                since=window["since"],
                until=window["until"],
                actions=[OBSERVATION_DECISION_ACTION],
            )
            if archive_store is not None
            else []
        )
        main_candidate_evaluation_rows = [
            dict(row)
            for row in store.load_candidate_evaluations_for_window(
                since=window["since"],
                until=window["until"],
            )
        ]
        archive_candidate_evaluation_rows = (
            [
                dict(row)
                for row in archive_store.load_candidate_evaluations_for_window(
                    since=window["since"],
                    until=window["until"],
                )
            ]
            if archive_store is not None
            else []
        )
    finally:
        if archive_store is not None:
            archive_store.close()

    rows = _merge_decision_rows(list(main_rows), list(archive_rows))
    observation_rows_merged = _merge_decision_rows(list(observation_rows), list(archive_observation_rows))
    candidate_evaluation_summary = _build_candidate_evaluation_summary(
        main_candidate_evaluation_rows + archive_candidate_evaluation_rows,
        limit=limit,
    )
    review_until_dt = datetime.fromisoformat(str(window["until"]))
    review_since_dt = datetime.fromisoformat(str(window["since"]))
    recent_window_days = min(int(window.get("window_days") or days or 0), 7)
    recent_since_dt = max(review_since_dt, review_until_dt - timedelta(days=max(recent_window_days, 1)))
    recent_since = recent_since_dt.isoformat()
    recent_candidate_evaluation_rows = [
        row
        for row in (main_candidate_evaluation_rows + archive_candidate_evaluation_rows)
        if recent_since <= str(row.get("created_at") or "") < str(window["until"])
    ]
    candidate_evaluation_trend_summary = _build_candidate_evaluation_trend_summary(
        recent_rows=recent_candidate_evaluation_rows,
        baseline_rows=main_candidate_evaluation_rows + archive_candidate_evaluation_rows,
        recent_window_days=max(recent_window_days, 1),
        baseline_window_days=int(window.get("window_days") or days or 0),
        limit=limit,
    )
    historical_alert_rows = []
    for raw_row in store.load_alert_history_for_window(since=window["since"]):
        row = dict(raw_row)
        if str(row.get("notified_at") or "") >= str(window["until"]):
            continue
        historical_alert_rows.append(row)
    entered_rows = [row for row in rows if bool(row.get("entered"))]
    entered_rows, auxiliary_deduped_merge_count = _dedupe_representative_rows(entered_rows)
    not_entered_rows = [row for row in rows if not bool(row.get("entered"))]
    take_profit_rows = [row for row in entered_rows if _is_profit_exit_reason(str(row.get("close_reason") or ""))]
    invalidation_rows = [row for row in entered_rows if str(row.get("close_reason") or "") == "hit_invalidation"]
    window_complete_rows = [row for row in entered_rows if str(row.get("close_reason") or "") == "window_complete"]
    exited_rows = take_profit_rows + invalidation_rows + window_complete_rows
    open_rows = [row for row in rows if bool(row.get("entered")) and str(row.get("close_reason") or "") == "insufficient_lookahead"]
    completed_rows = [row for row in rows if str(row.get("close_reason") or "") not in {"", "insufficient_lookahead"}]
    pending_rows = [row for row in rows if str(row.get("close_reason") or "") in {"", "insufficient_lookahead"}]
    realized_values = [float(row["realized_return"]) for row in exited_rows if row.get("realized_return") is not None]
    positive_values = [value for value in realized_values if value > 0]
    negative_values = [value for value in realized_values if value < 0]
    holding_days = [int(row["holding_days"]) for row in exited_rows if row.get("holding_days") is not None]
    auxiliary_metrics = []
    for field, label, maturity_days in (
        ("t_plus_1_return", "T+1", 1),
        ("t_plus_3_return", "T+3", 3),
        ("t_plus_7_return", "T+7", 7),
        ("t_plus_14_return", "T+14", 14),
        ("t_plus_30_return", "T+30", 30),
        ("max_runup", "最大浮盈", None),
        ("max_drawdown", "最大回撤", None),
    ):
        values = []
        metric_rows = entered_rows
        for row in metric_rows:
            if maturity_days is not None:
                locked_value = _locked_checkpoint_return(row, day=maturity_days, review_until_dt=review_until_dt)
                if locked_value is None:
                    continue
                values.append(float(locked_value))
                continue
            if row.get(field) is None:
                continue
            values.append(float(row[field]))
        auxiliary_metrics.append(
            {
                "field": field,
                "label": label,
                "sample_count": len(values),
                "avg_value": _mean(values),
            }
        )
    path_quality_summary = _build_path_quality_summary(rows, review_until_dt=review_until_dt)

    event_breakdown = _aggregate_historical_effect_rows(
        rows,
        key_fn=lambda row: row.get("event_type") or "uncategorized",
        label_fn=_historical_effect_event_type_label,
    )[:limit]
    pool_breakdown = _aggregate_historical_effect_rows(
        rows,
        key_fn=lambda row: row.get("pool") or "unknown",
        label_fn=_historical_effect_pool_label,
    )[:limit]
    action_breakdown = _aggregate_historical_effect_rows(
        rows,
        key_fn=lambda row: row.get("action") or "unknown",
        label_fn=_historical_effect_action_label,
    )[:limit]
    trigger_breakdown = _aggregate_historical_effect_rows(
        rows,
        key_fn=lambda row: row.get("trigger_mode") or "unknown",
        label_fn=_historical_effect_trigger_mode_label,
    )[:limit]
    priority_breakdown = _aggregate_historical_effect_rows(
        rows,
        key_fn=lambda row: row.get("priority") or "unknown",
        label_fn=_historical_effect_priority_label,
    )[:limit]

    best_event = _best_completed_group(event_breakdown)
    worst_event = _worst_completed_group(event_breakdown)
    best_pool = _best_completed_group(pool_breakdown)
    worst_pool = _worst_completed_group(pool_breakdown)
    best_trigger = _best_completed_group(trigger_breakdown)
    worst_trigger = _worst_completed_group(trigger_breakdown)
    recommendation_items: list[dict[str, Any]] = []
    if not rows:
        recommendation_items.append(
            _build_recommendation_item(
                text="当前窗口内没有可执行建议，先继续积累样本。",
                priority=80,
                source="baseline",
            )
        )
    else:
        if best_event is not None:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{best_event['label']} 是当前窗口里表现最好的事件类型，可优先保留并继续观察后续样本。",
                    priority=50,
                    source="historical_outcome",
                )
            )
        if worst_event is not None and worst_event is not best_event:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{worst_event['label']} 表现最弱，优先回看这类事件的阈值、排序和入场时机。",
                    priority=30,
                    source="historical_outcome",
                )
            )
        if best_pool is not None:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{best_pool['label']} 当前是表现更好的池子，可优先作为后续筛选和排序参考。",
                    priority=50,
                    source="historical_outcome",
                )
            )
        if worst_pool is not None and worst_pool is not best_pool:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{worst_pool['label']} 当前表现偏弱，优先检查该池子的升池标准和价格计划。",
                    priority=30,
                    source="historical_outcome",
                )
            )
        if best_trigger is not None:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{best_trigger['label']} 当前相对更稳，可继续观察这类触发方式的后续样本。",
                    priority=55,
                    source="historical_outcome",
                )
            )
        if worst_trigger is not None and worst_trigger is not best_trigger:
            recommendation_items.append(
                _build_recommendation_item(
                    text=f"{worst_trigger['label']} 当前偏弱，值得回看触发条件是否过于宽松。",
                    priority=35,
                    source="historical_outcome",
                )
            )
        invalidation_rate = _percentage(len(invalidation_rows), len(entered_rows)) or 0.0
        window_complete_rate = _percentage(len(window_complete_rows), len(entered_rows)) or 0.0
        not_entered_rate = _percentage(len(not_entered_rows), len(rows)) or 0.0
        take_profit_rate = _percentage(len(take_profit_rows), len(entered_rows)) or 0.0
        avg_runup = next((row.get("avg_value") for row in auxiliary_metrics if row.get("field") == "max_runup"), None)
        if invalidation_rate >= 40.0:
            recommendation_items.append(
                _build_recommendation_item(
                    text="失效退出占比偏高，优先检查入场是否过早，以及失效缓冲是否过窄。",
                    priority=20,
                    parameter_hints=[
                        ("execution_entry_buffer_pct", "inspect"),
                        ("execution_invalidation_buffer_pct", "inspect"),
                    ],
                    source="historical_outcome",
                )
            )
        if window_complete_rate >= 40.0:
            recommendation_items.append(
                _build_recommendation_item(
                    text="复盘窗口结算占比偏高，说明止盈偏远或催化延续性还不够强。",
                    priority=20,
                    parameter_hints=[("take_profit_pct", "high"), ("take_profit_extension_pct", "high")],
                    source="historical_outcome",
                )
            )
        if not_entered_rate >= 40.0:
            recommendation_items.append(
                _build_recommendation_item(
                    text="未成交样本偏多，说明入场区间可能偏保守，值得回看挂单区间。",
                    priority=20,
                    parameter_hints=[("execution_entry_buffer_pct", "high")],
                    source="historical_outcome",
                )
            )
        if take_profit_rate == 0.0 and avg_runup is not None and float(avg_runup) > 0:
            recommendation_items.append(
                _build_recommendation_item(
                    text="样本中曾出现一定浮盈但没有止盈兑现，建议回看止盈区间是否偏远。",
                    priority=20,
                    parameter_hints=[("take_profit_pct", "high"), ("take_profit_extension_pct", "high")],
                    source="historical_outcome",
                )
            )
    recommendation_items.extend(_build_candidate_trend_recommendations(candidate_evaluation_trend_summary))
    if not recommendation_items:
        recommendation_items.append(
            _build_recommendation_item(
                text="当前样本表现较均衡，继续累计更多完整样本后再调整策略。",
                priority=90,
                source="baseline",
            )
        )
    prioritized_recommendations = _prioritize_recommendations(recommendation_items)
    deduped_recommendations = [str(item.get("text") or "") for item in prioritized_recommendations]

    ranked_completed_rows = [
        row
        for row in rows
        if bool(row.get("entered")) and row.get("realized_return") is not None
    ]
    best_completed_decisions = sorted(
        ranked_completed_rows,
        key=lambda row: (
            float(row.get("realized_return") or 0.0),
            str(row.get("created_at") or ""),
        ),
        reverse=True,
    )[:3]
    worst_completed_decisions = sorted(
        ranked_completed_rows,
        key=lambda row: (
            float(row.get("realized_return") or 0.0),
            str(row.get("created_at") or ""),
        ),
    )[:3]
    ranked_t7_rows = [row for row in rows if row.get("t_plus_7_return") is not None]
    best_t7_decisions = sorted(
        ranked_t7_rows,
        key=lambda row: (
            float(row.get("t_plus_7_return") or 0.0),
            str(row.get("created_at") or ""),
        ),
        reverse=True,
    )[:3]
    worst_t7_decisions = sorted(
        ranked_t7_rows,
        key=lambda row: (
            float(row.get("t_plus_7_return") or 0.0),
            str(row.get("created_at") or ""),
        ),
    )[:3]

    adjusted_price_status = _summarize_adjusted_price_readiness(store, rows)
    adjusted_price_protection_ready = bool(adjusted_price_status["ready"])
    sample_audit_payload = build_outcome_sample_payload(
        store,
        days=days,
        limit=OUTCOME_SAMPLE_AUDIT_LIMIT,
        start_date=start_date,
        end_date=end_date,
    )
    sample_audit = _summarize_outcome_sample_audit(sample_audit_payload)
    ai_review = _load_ai_outcome_review(store)
    ai_review_completed = _ai_review_matches_review(ai_review, window)
    review_baseline = _load_review_baseline(store)
    baseline_frozen = _review_baseline_matches_version(review_baseline)
    draft_reasons = [
        "当前仍处于开发校验阶段，历史结果可能因口径修正而回溯变化。",
    ]
    if not adjusted_price_protection_ready:
        if adjusted_price_status["missing_symbols"]:
            draft_reasons.append(
                f"以下标的缺少可核验的日线 bars：{'、'.join(adjusted_price_status['missing_symbols'][:5])}。"
            )
        if adjusted_price_status["unadjusted_symbols"]:
            draft_reasons.append(
                f"以下标的仍存在未复权日线：{'、'.join(adjusted_price_status['unadjusted_symbols'][:5])}。"
            )
        draft_reasons.append("复权保护未完成，正式版复盘暂不产出。")
    formal_blockers = [
    ]
    if not baseline_frozen:
        formal_blockers.append("复盘口径仍处于开发阶段，尚未冻结。")
    if not ai_review_completed:
        formal_blockers.append("AI样本复核尚未完成。")
    if sample_audit["mismatched"] > 0:
        formal_blockers.append("程序样本抽检存在不一致结果。")
    elif sample_audit["unavailable"] > 0:
        formal_blockers.append("程序样本抽检仍有无法重算的样本。")
    if not adjusted_price_protection_ready:
        formal_blockers.append("复权日线保护尚未满足正式版要求。")
    formal_ready = not formal_blockers
    detail_rows = sorted(
        rows,
        key=lambda row: (
            str(row.get("created_at") or ""),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )[:20]
    exit_pool_transition_rows = [
        row
        for row in rows
        if bool(row.get("entered")) and str(row.get("close_reason") or "") == "exit_pool"
    ]
    exit_pool_transition_holding_days = [
        float(row.get("holding_days"))
        for row in exit_pool_transition_rows
        if row.get("holding_days") is not None
    ]
    exit_pool_transition_by_action: list[dict[str, Any]] = []
    for action in EXECUTABLE_DECISION_ACTIONS:
        action_rows = [
            row for row in exit_pool_transition_rows
            if str(row.get("action") or "").strip() == action
        ]
        holding_values = [
            float(row.get("holding_days"))
            for row in action_rows
            if row.get("holding_days") is not None
        ]
        if not action_rows:
            continue
        exit_pool_transition_by_action.append(
            {
                "action": action,
                "action_display": _historical_effect_action_label(action),
                "sample_count": len(action_rows),
                "avg_holding_days": _mean(holding_values),
                "median_holding_days": _median(holding_values),
            }
        )
    fastest_exit_pool_transition = None
    slowest_exit_pool_transition = None
    transition_rows_with_days = [
        row for row in exit_pool_transition_rows
        if row.get("holding_days") is not None
    ]
    if transition_rows_with_days:
        fastest_exit_pool_transition = min(
            transition_rows_with_days,
            key=lambda row: (float(row.get("holding_days") or 0.0), str(row.get("created_at") or "")),
        )
        slowest_exit_pool_transition = max(
            transition_rows_with_days,
            key=lambda row: (float(row.get("holding_days") or 0.0), str(row.get("created_at") or "")),
        )
    recent_exit_pool_samples = [
        {
            "symbol": str(row.get("symbol") or ""),
            "event_type": str(row.get("event_type") or ""),
            "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
            "action": str(row.get("action") or ""),
            "action_display": _historical_effect_action_label(str(row.get("action") or "")),
            "close_reason": str(row.get("close_reason") or ""),
            "close_reason_display": _historical_effect_exit_reason_label(str(row.get("close_reason") or "")),
            "exit_subreason": str(row.get("exit_subreason") or ""),
            "holding_days": row.get("holding_days"),
            "realized_return": row.get("realized_return"),
            "created_at": str(row.get("created_at") or ""),
        }
        for row in sorted(
            exit_pool_transition_rows,
            key=lambda row: (
                str(row.get("created_at") or ""),
                str(row.get("symbol") or ""),
            ),
            reverse=True,
        )[:3]
    ]
    exit_pool_samples_with_return = [
        row for row in exit_pool_transition_rows
        if row.get("realized_return") is not None
    ]

    def _serialize_trade_path_sample(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": str(row.get("symbol") or ""),
            "event_type": str(row.get("event_type") or ""),
            "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
            "action": str(row.get("action") or ""),
            "action_display": _historical_effect_action_label(str(row.get("action") or "")),
            "close_reason": str(row.get("close_reason") or ""),
            "close_reason_display": _historical_effect_exit_reason_label(str(row.get("close_reason") or "")),
            "exit_subreason": str(row.get("exit_subreason") or ""),
            "holding_days": row.get("holding_days"),
            "realized_return": row.get("realized_return"),
            "created_at": str(row.get("created_at") or ""),
        }

    best_exit_pool_samples = [
        _serialize_trade_path_sample(row)
        for row in sorted(
            exit_pool_samples_with_return,
            key=lambda row: (
                float(row.get("realized_return") or 0.0),
                str(row.get("created_at") or ""),
            ),
            reverse=True,
        )[:3]
    ]
    worst_exit_pool_samples = [
        _serialize_trade_path_sample(row)
        for row in sorted(
            exit_pool_samples_with_return,
            key=lambda row: (
                float(row.get("realized_return") or 0.0),
                str(row.get("created_at") or ""),
            ),
        )[:3]
    ]
    promoted_confirmation_rows = [
        row
        for row in rows
        if bool(_load_json_dict(row.get("packet_json")).get("promoted_from_prewatch"))
        or str(row.get("trigger_mode") or "").strip() == "promoted"
    ]
    promoted_confirmation_rows, promoted_confirmation_deduped_merge_count = _dedupe_representative_rows(promoted_confirmation_rows)
    promoted_after_light_push_rows = [
        row
        for row in promoted_confirmation_rows
        if int(_load_json_dict(row.get("packet_json")).get("prewatch_alert_sent_count") or 0) > 0
    ]
    prewatch_alert_rows = [
        row
        for row in historical_alert_rows
        if bool(row.get("sent"))
        and (
            str(row.get("dedup_key") or "").startswith("prewatch-notify:")
            or str(row.get("card_id") or "").startswith("prewatch-notify:")
        )
    ]
    prewatch_alert_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in prewatch_alert_rows
        if str(row.get("symbol") or "").strip()
    }
    promoted_confirmation_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in promoted_confirmation_rows
        if str(row.get("symbol") or "").strip()
    }
    observation_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in observation_rows_merged
        if str(row.get("symbol") or "").strip()
    }
    still_observing_symbols = observation_symbols - promoted_confirmation_symbols
    observation_to_confirmation_days = _observation_to_confirmation_days(
        observation_rows_merged,
        promoted_confirmation_rows,
    )
    recent_observation_rows = _select_unique_top_rows(
        sorted(
            observation_rows_merged,
            key=lambda item: (
                str(item.get("created_at") or ""),
                str(item.get("symbol") or ""),
                float(item.get("final_score") or 0.0),
            ),
            reverse=True,
        ),
        key_fn=lambda row: (str(row.get("symbol") or "").strip().upper(),),
        limit=5,
    )
    recent_observation_samples = []
    for row in recent_observation_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        packet = _load_json_dict(row.get("packet_json"))
        lifecycle = packet.get("prewatch_lifecycle") if isinstance(packet.get("prewatch_lifecycle"), dict) else {}
        recent_observation_samples.append(
            {
                "symbol": symbol,
                "created_at": str(row.get("created_at") or ""),
                "horizon": str(row.get("horizon") or "position"),
                "setup_type": str(packet.get("setup_type") or ""),
                "score": float(row.get("final_score") or 0.0),
                "trigger_mode": str(row.get("trigger_mode") or ""),
                "theme_ids": list(row.get("theme_ids") or []),
                "observation_count": int(lifecycle.get("observation_count") or 0),
                "alert_sent_count": int(lifecycle.get("alert_sent_count") or 0),
                "observation_status": "已触发观察提醒" if symbol in prewatch_alert_symbols else "仅后台观察",
                "confirmation_status": "已形成确认机会" if symbol in promoted_confirmation_symbols else "仍在观察",
            }
        )
    observation_after_samples = []
    promoted_sample_rows = _select_unique_top_rows(
        sorted(
            promoted_confirmation_rows,
            key=lambda row: (
                str(row.get("created_at") or ""),
                str(row.get("symbol") or ""),
            ),
            reverse=True,
        ),
        key_fn=lambda row: (str(row.get("symbol") or "").strip().upper(),),
        limit=3,
    )
    for row in promoted_sample_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        packet = _load_json_dict(row.get("packet_json"))
        observation_after_samples.append(
            {
                "symbol": symbol,
                "created_at": str(row.get("created_at") or ""),
                "action": str(row.get("action") or ""),
                "action_display": _historical_effect_action_label(str(row.get("action") or "")),
                "prewatch_score": float(packet.get("prewatch_score") or 0.0),
                "prewatch_setup_type": str(packet.get("prewatch_setup_type") or ""),
                "prewatch_alert_sent_count": int(packet.get("prewatch_alert_sent_count") or 0),
                "prewatch_observation_count": int(packet.get("prewatch_observation_count") or 0),
            }
        )
    exit_from_promoted_rows = [
        row
        for row in promoted_confirmation_rows
        if bool(row.get("entered")) and str(row.get("close_reason") or "").strip() == "exit_pool"
    ]
    simulated_entry_rows = [row for row in promoted_confirmation_rows if bool(row.get("entered"))]
    simulated_pending_entry_rows = [row for row in promoted_confirmation_rows if not bool(row.get("entered"))]
    simulated_completed_exit_rows = [
        row
        for row in promoted_confirmation_rows
        if bool(row.get("entered")) and str(row.get("close_reason") or "").strip() not in {"", "insufficient_lookahead"}
    ]
    simulated_open_rows = [
        row
        for row in promoted_confirmation_rows
        if bool(row.get("entered")) and str(row.get("close_reason") or "").strip() == "insufficient_lookahead"
    ]
    simulated_exit_breakdown = {
        "exit_pool": sum(
            1 for row in simulated_completed_exit_rows
            if str(row.get("close_reason") or "").strip() == "exit_pool"
        ),
        "hit_take_profit": sum(
            1 for row in simulated_completed_exit_rows
            if str(row.get("close_reason") or "").strip() == "hit_take_profit"
        ),
        "hit_invalidation": sum(
            1 for row in simulated_completed_exit_rows
            if str(row.get("close_reason") or "").strip() == "hit_invalidation"
        ),
        "window_complete": sum(
            1 for row in simulated_completed_exit_rows
            if str(row.get("close_reason") or "").strip() == "window_complete"
        ),
    }
    confirmation_to_entry_days = []
    confirmation_to_entry_timing_anomaly_count = 0
    for row in simulated_entry_rows:
        timing_days, has_anomaly = _timing_days_from_row(row)
        if has_anomaly:
            confirmation_to_entry_timing_anomaly_count += 1
            continue
        if timing_days is not None:
            confirmation_to_entry_days.append(timing_days)
    entry_to_completed_exit_days = [
        float(row.get("holding_days"))
        for row in simulated_completed_exit_rows
        if row.get("holding_days") is not None
    ]
    entry_timing_by_action: list[dict[str, Any]] = []
    completed_exit_timing_by_action: list[dict[str, Any]] = []
    for action in EXECUTABLE_DECISION_ACTIONS:
        action_entry_days = []
        for row in simulated_entry_rows:
            if str(row.get("action") or "").strip() != action:
                continue
            timing_days, _ = _timing_days_from_row(row)
            if timing_days is not None:
                action_entry_days.append(timing_days)
        if action_entry_days:
            entry_timing_by_action.append(
                {
                    "action": action,
                    "action_display": _historical_effect_action_label(action),
                    "sample_count": len(action_entry_days),
                    "avg_days_to_entry": _mean(action_entry_days),
                    "median_days_to_entry": _median(action_entry_days),
                }
            )
        action_completed_exit_days = [
            float(row.get("holding_days"))
            for row in simulated_completed_exit_rows
            if str(row.get("action") or "").strip() == action and row.get("holding_days") is not None
        ]
        if action_completed_exit_days:
            completed_exit_timing_by_action.append(
                {
                    "action": action,
                    "action_display": _historical_effect_action_label(action),
                    "sample_count": len(action_completed_exit_days),
                    "avg_holding_days": _mean(action_completed_exit_days),
                    "median_holding_days": _median(action_completed_exit_days),
                }
            )
    pool_funnel_summary = {
        "prewatch_candidate_count": len(observation_rows_merged),
        "prewatch_light_push_count": len(prewatch_alert_rows),
        "promoted_confirmation_count": len(promoted_confirmation_rows),
        "promoted_after_light_push_count": len(promoted_after_light_push_rows),
        "promoted_without_light_push_count": max(
            len(promoted_confirmation_rows) - len(promoted_after_light_push_rows),
            0,
        ),
        "exit_from_promoted_count": len(exit_from_promoted_rows),
        "observation_to_light_push_rate": _percentage(len(prewatch_alert_rows), len(observation_rows_merged)),
        "observation_to_confirmation_rate": _percentage(len(promoted_confirmation_rows), len(observation_rows_merged)),
        "light_push_to_confirmation_rate": _percentage(len(promoted_after_light_push_rows), len(prewatch_alert_rows)),
        "confirmation_to_exit_rate": _percentage(len(exit_from_promoted_rows), len(promoted_confirmation_rows)),
    }
    simulation_funnel_summary = {
        "prewatch_candidate_count": len(observation_rows_merged),
        "prewatch_light_push_count": len(prewatch_alert_rows),
        "promoted_confirmation_count": len(promoted_confirmation_rows),
        "promoted_confirmation_deduped_merge_count": promoted_confirmation_deduped_merge_count,
        "simulated_entry_count": len(simulated_entry_rows),
        "simulated_pending_entry_count": len(simulated_pending_entry_rows),
        "exit_pool_transition_count": len(exit_from_promoted_rows),
        "simulated_completed_exit_count": len(simulated_completed_exit_rows),
        "simulated_open_count": len(simulated_open_rows),
        "observation_to_light_push_rate": _percentage(len(prewatch_alert_rows), len(observation_rows_merged)),
        "observation_to_confirmation_rate": _percentage(len(promoted_confirmation_rows), len(observation_rows_merged)),
        "confirmation_to_entry_rate": _percentage(len(simulated_entry_rows), len(promoted_confirmation_rows)),
        "entry_to_exit_pool_rate": _percentage(len(exit_from_promoted_rows), len(simulated_entry_rows)),
        "entry_to_completed_exit_rate": _percentage(len(simulated_completed_exit_rows), len(simulated_entry_rows)),
        "avg_confirmation_to_entry_days": _mean(confirmation_to_entry_days),
        "median_confirmation_to_entry_days": _median(confirmation_to_entry_days),
        "confirmation_to_entry_timing_anomaly_count": confirmation_to_entry_timing_anomaly_count,
        "avg_entry_to_completed_exit_days": _mean(entry_to_completed_exit_days),
        "median_entry_to_completed_exit_days": _median(entry_to_completed_exit_days),
        "entry_timing_by_action": entry_timing_by_action,
        "completed_exit_timing_by_action": completed_exit_timing_by_action,
        "completed_exit_breakdown": simulated_exit_breakdown,
    }
    review = {
        "status": "正式" if formal_ready else "草稿",
        "status_label": "历史效果复盘（正式）" if formal_ready else "历史效果复盘（草稿）",
        "review_version": HISTORICAL_EFFECT_REVIEW_VERSION,
        "review_window": window,
        "backfill_cutoff_at": utcnow().isoformat(),
        "adjusted_price_protection_ready": adjusted_price_protection_ready,
        "adjusted_price_status": adjusted_price_status,
        "sample_audit": sample_audit,
        "ai_review": ai_review if ai_review_completed else {},
        "review_baseline": review_baseline if baseline_frozen else {},
        "formal_readiness": {
            "ready": formal_ready,
            "status_label": "已满足" if formal_ready else "未满足",
            "blockers": formal_blockers,
        },
        "draft_reasons": [] if formal_ready else draft_reasons,
        "archive_db_path": str(archive_db_path) if archive_db_path is not None else "",
        "overview": {
            "decision_count": len(rows),
            "entered_count": len(entered_rows),
            "not_entered_count": len(not_entered_rows),
            "take_profit_exit_count": len(take_profit_rows),
            "invalidation_exit_count": len(invalidation_rows),
            "window_complete_count": len(window_complete_rows),
            "open_position_count": len(open_rows),
            "avg_realized_return": _mean(realized_values),
            "median_realized_return": _median(realized_values),
            "win_rate": _percentage(sum(1 for value in realized_values if value > 0), len(realized_values)),
            "profit_loss_ratio": (
                round((_mean(positive_values) or 0.0) / abs(_mean(negative_values) or 0.0), 2)
                if positive_values and negative_values and (_mean(negative_values) or 0.0) != 0.0
                else None
            ),
        },
        "execution_quality": {
            "entry_hit_rate": _percentage(len(entered_rows), len(rows)),
            "take_profit_hit_rate": _percentage(len(take_profit_rows), len(entered_rows)),
            "invalidation_hit_rate": _percentage(len(invalidation_rows), len(entered_rows)),
            "window_complete_rate": _percentage(len(window_complete_rows), len(entered_rows)),
            "avg_holding_days": _mean([float(value) for value in holding_days]),
            "completed_outcome_count": len(completed_rows),
            "pending_outcome_count": len(pending_rows),
        },
        "trade_path_summary": {
            "exit_pool_transition_count": len(exit_pool_transition_rows),
            "avg_days_to_exit_pool": _mean(exit_pool_transition_holding_days),
            "median_days_to_exit_pool": _median(exit_pool_transition_holding_days),
            "by_action": exit_pool_transition_by_action,
            "fastest_sample": (
                {
                    "symbol": str(fastest_exit_pool_transition.get("symbol") or ""),
                    "event_type": str(fastest_exit_pool_transition.get("event_type") or ""),
                    "action": str(fastest_exit_pool_transition.get("action") or ""),
                    "action_display": _historical_effect_action_label(str(fastest_exit_pool_transition.get("action") or "")),
                    "holding_days": fastest_exit_pool_transition.get("holding_days"),
                    "created_at": str(fastest_exit_pool_transition.get("created_at") or ""),
                }
                if fastest_exit_pool_transition is not None
                else {}
            ),
            "slowest_sample": (
                {
                    "symbol": str(slowest_exit_pool_transition.get("symbol") or ""),
                    "event_type": str(slowest_exit_pool_transition.get("event_type") or ""),
                    "action": str(slowest_exit_pool_transition.get("action") or ""),
                    "action_display": _historical_effect_action_label(str(slowest_exit_pool_transition.get("action") or "")),
                    "holding_days": slowest_exit_pool_transition.get("holding_days"),
                    "created_at": str(slowest_exit_pool_transition.get("created_at") or ""),
                }
                if slowest_exit_pool_transition is not None
                else {}
            ),
            "recent_samples": recent_exit_pool_samples,
            "best_samples": best_exit_pool_samples,
            "worst_samples": worst_exit_pool_samples,
        },
        "pool_funnel_summary": pool_funnel_summary,
        "simulation_funnel_summary": simulation_funnel_summary,
        "path_quality_summary": path_quality_summary,
        "auxiliary_path_quality_deduped_merge_count": auxiliary_deduped_merge_count,
        "candidate_evaluation_summary": candidate_evaluation_summary,
        "candidate_evaluation_trend_summary": candidate_evaluation_trend_summary,
        "recent_observation_samples": recent_observation_samples,
        "observation_after_summary": {
            "observation_count": len(observation_rows_merged),
            "observation_alert_count": len(prewatch_alert_rows),
            "promoted_confirmation_count": len(promoted_confirmation_rows),
            "promoted_after_alert_count": len(promoted_after_light_push_rows),
            "promoted_without_alert_count": max(
                len(promoted_confirmation_rows) - len(promoted_after_light_push_rows),
                0,
            ),
            "still_observing_symbol_count": len(still_observing_symbols),
            "observation_to_alert_rate": _percentage(len(prewatch_alert_rows), len(observation_rows_merged)),
            "observation_to_confirmation_rate": _percentage(len(promoted_confirmation_rows), len(observation_rows_merged)),
            "alert_to_confirmation_rate": _percentage(len(promoted_after_light_push_rows), len(prewatch_alert_rows)),
            "avg_days_to_confirmation": _mean(observation_to_confirmation_days),
            "median_days_to_confirmation": _median(observation_to_confirmation_days),
            "representative_samples": observation_after_samples,
        },
        "auxiliary_observation": auxiliary_metrics,
        "breakdowns": {
            "event_type": event_breakdown,
            "pool": pool_breakdown,
            "action": action_breakdown,
            "trigger_mode": trigger_breakdown,
            "priority": priority_breakdown,
        },
        "appendix": {
            "observation_signal_count": len(observation_rows_merged),
        },
        "decision_details": [
            {
                "created_at": str(row.get("created_at") or ""),
                "symbol": str(row.get("symbol") or ""),
                "event_type": str(row.get("event_type") or ""),
                "event_type_display": _historical_effect_event_type_label(str(row.get("event_type") or "")),
                "action": str(row.get("action") or ""),
                "action_display": _historical_effect_action_label(str(row.get("action") or "")),
                "status_label": _historical_effect_status_label(
                    entered=bool(row.get("entered")),
                    close_reason=str(row.get("close_reason") or ""),
                ),
                "entered": bool(row.get("entered")),
                "close_reason": str(row.get("close_reason") or ""),
                "entry_price": row.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "realized_return": row.get("realized_return"),
                "holding_days": row.get("holding_days"),
            }
            for row in detail_rows
        ],
        "best_completed_decisions": _serialize_ranked_decision_rows(
            best_completed_decisions,
            metric_field="realized_return",
        ),
        "worst_completed_decisions": _serialize_ranked_decision_rows(
            worst_completed_decisions,
            metric_field="realized_return",
        ),
        "best_t7_decisions": _serialize_ranked_decision_rows(
            best_t7_decisions,
            metric_field="t_plus_7_return",
        ),
        "worst_t7_decisions": _serialize_ranked_decision_rows(
            worst_t7_decisions,
            metric_field="t_plus_7_return",
        ),
        "recommendations": deduped_recommendations[:6],
        "recommendation_details": prioritized_recommendations[:6],
    }
    should_attach_comparison = include_comparison and int(days) == RECENT_PERFORMANCE_WINDOW_DAYS
    if should_attach_comparison:
        baseline_review = _build_historical_effect_review_data(
            store,
            days=BASELINE_PERFORMANCE_WINDOW_DAYS,
            limit=limit,
            include_comparison=False,
        )
        primary_snapshot = _build_review_window_snapshot(
            review,
            label=f"近 {RECENT_PERFORMANCE_WINDOW_DAYS} 天",
            role_label="调参",
            min_sample_size=SHORT_TERM_RECOMMENDATION_MIN_SAMPLES,
        )
        baseline_snapshot = _build_review_window_snapshot(
            baseline_review,
            label=f"近 {BASELINE_PERFORMANCE_WINDOW_DAYS} 天",
            role_label="基准",
            min_sample_size=BASELINE_RECOMMENDATION_MIN_SAMPLES,
        )
        review["comparison_windows"] = {
            "primary": primary_snapshot,
            "baseline": baseline_snapshot,
            "summary": _build_dual_window_comparison_summary(primary_snapshot, baseline_snapshot),
        }
        _apply_dual_window_recommendation_guard(
            review,
            primary_snapshot=primary_snapshot,
            baseline_snapshot=baseline_snapshot,
        )
    return review

def _load_json_dict(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        payload = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_sample_price_context(row: dict[str, Any]) -> dict[str, Any]:
    packet = _load_json_dict(row.get("packet_json"))
    price_plan = packet.get("price_plan") or {}
    entry_plan = _load_json_dict(row.get("entry_plan_json"))
    invalidation = _load_json_dict(row.get("invalidation_json"))
    entry_range = price_plan.get("entry_range") or entry_plan.get("entry_range") or {}
    take_profit_range = price_plan.get("take_profit_range") or entry_plan.get("take_profit_range") or {}
    invalidation_level = (
        price_plan.get("invalidation_level")
        or entry_plan.get("invalidation_level")
        or invalidation.get("level")
    )
    invalidation_reason = (
        price_plan.get("invalidation_reason")
        or entry_plan.get("invalidation_reason")
        or invalidation.get("reason")
        or ""
    )
    return {
        "entry_range": entry_range if isinstance(entry_range, dict) else {},
        "take_profit_range": take_profit_range if isinstance(take_profit_range, dict) else {},
        "invalidation_level": invalidation_level,
        "invalidation_reason": invalidation_reason,
    }


def _sample_market_session_date(value: datetime) -> date:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    normalized = normalized.astimezone(timezone.utc)
    if normalized.hour == 0 and normalized.minute == 0 and normalized.second == 0 and normalized.microsecond == 0:
        return normalized.date()
    return normalized.astimezone(US_MARKET_TZ).date()


def _sample_bar_overlaps_entry(bar: Bar, *, entry_low: float, entry_high: float) -> bool:
    return float(bar.low) <= float(entry_high) and float(bar.high) >= float(entry_low)


def _load_sample_bars(store: Store, symbol: str) -> list[Bar]:
    return store.load_price_bars(symbol.strip().upper(), "1d", 400)


def _build_sample_bar_preview(store: Store, row: dict[str, Any], price_context: dict[str, Any], *, limit: int = 6) -> list[dict[str, Any]]:
    created_at_raw = str(row.get("created_at") or "").strip()
    if not created_at_raw:
        return []
    try:
        created_at = datetime.fromisoformat(created_at_raw)
    except ValueError:
        return []
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    else:
        created_at = created_at.astimezone(timezone.utc)
    bars = _load_sample_bars(store, str(row.get("symbol") or ""))
    if not bars:
        return []
    target_date = _sample_market_session_date(created_at)
    anchor_index = None
    for index, bar in enumerate(bars):
        if _sample_market_session_date(bar.timestamp) >= target_date:
            anchor_index = index
            break
    if anchor_index is None:
        return []
    entry_range = price_context.get("entry_range") or {}
    entry_low = entry_range.get("low")
    entry_high = entry_range.get("high")
    take_profit_range = price_context.get("take_profit_range") or {}
    take_profit_level = take_profit_range.get("low")
    invalidation_level = price_context.get("invalidation_level")
    preview: list[dict[str, Any]] = []
    for offset, bar in enumerate(bars[anchor_index : anchor_index + max(limit, 0)]):
        tags: list[str] = []
        if entry_low is not None and entry_high is not None and _sample_bar_overlaps_entry(
            bar,
            entry_low=float(entry_low),
            entry_high=float(entry_high),
        ):
            tags.append("入场触碰")
        if take_profit_level is not None and float(bar.high) >= float(take_profit_level):
            tags.append("止盈触碰")
        if invalidation_level is not None and float(bar.low) <= float(invalidation_level):
            tags.append("失效触碰")
        if offset == 0:
            tags.append("锚点")
        preview.append(
            {
                "session_date": _sample_market_session_date(bar.timestamp).isoformat(),
                "open": round(float(bar.open), 4),
                "high": round(float(bar.high), 4),
                "low": round(float(bar.low), 4),
                "close": round(float(bar.close), 4),
                "adjusted": bool(getattr(bar, "adjusted", False)),
                "tags": tags,
            }
        )
    return preview


def _build_sample_recompute_check(store: Store, row: dict[str, Any]) -> dict[str, Any]:
    bars = _load_sample_bars(store, str(row.get("symbol") or ""))
    if not bars:
        return {
            "status": "无法重算",
            "issues": ["缺少 1d bars"],
        }
    computation = explain_decision_outcome(row, bars)
    recomputed = computation.outcome
    if recomputed is None:
        return {
            "status": "无法重算",
            "issues": [computation.skip_reason or "当前口径无法从样本 bars 重算结果"],
        }
    issues: list[str] = []
    price_context = _extract_sample_price_context(dict(row))
    take_profit_range = price_context.get("take_profit_range") or {}
    take_profit_low = take_profit_range.get("low")
    legacy_take_profit_triggered = False
    if str(row.get("close_reason") or "").strip() == "hit_take_profit" and take_profit_low is not None:
        try:
            tp_low_value = float(take_profit_low)
            legacy_take_profit_triggered = any(float(bar.high) >= tp_low_value for bar in bars)
        except (TypeError, ValueError):
            legacy_take_profit_triggered = False
    legacy_take_profit_equivalent = _should_tolerate_legacy_take_profit_prices(
        row,
        recomputed,
        legacy_take_profit_triggered=legacy_take_profit_triggered,
    )
    if bool(row.get("entered")) != bool(recomputed.entered):
        issues.append(f"entered: 存量={bool(row.get('entered'))} / 重算={bool(recomputed.entered)}")
    if not _close_reason_equivalent_for_audit(
        str(row.get("close_reason") or ""),
        str(recomputed.close_reason or ""),
        recomputed_subreason=str(recomputed.exit_subreason or ""),
        legacy_take_profit_triggered=legacy_take_profit_triggered,
    ):
        issues.append(
            f"close_reason: 存量={str(row.get('close_reason') or '-') } / 重算={str(recomputed.close_reason or '-')}"
        )
    for field, label in (
        ("entry_price", "entry_price"),
        ("exit_price", "exit_price"),
        ("realized_return", "realized_return"),
    ):
        if legacy_take_profit_equivalent and field in {"exit_price", "realized_return"}:
            continue
        stored_value = row.get(field)
        recomputed_value = getattr(recomputed, field)
        if stored_value is None and recomputed_value is None:
            continue
        if stored_value is None or recomputed_value is None:
            issues.append(f"{label}: 存量={stored_value} / 重算={recomputed_value}")
            continue
        if abs(float(stored_value) - float(recomputed_value)) > 0.01:
            issues.append(f"{label}: 存量={stored_value} / 重算={recomputed_value}")
    return {
        "status": "一致" if not issues else "不一致",
        "issues": issues,
        "recomputed": {
            "entered": recomputed.entered,
            "close_reason": recomputed.close_reason,
            "entry_price": recomputed.entry_price,
            "exit_price": recomputed.exit_price,
            "realized_return": recomputed.realized_return,
        },
    }


def _summarize_adjusted_price_readiness(store: Store, rows: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = sorted({str(row.get("symbol") or "").strip().upper() for row in rows if str(row.get("symbol") or "").strip()})
    adjustment_rows = store.summarize_price_bar_adjustment(symbols, "1d")
    by_symbol = {str(row["symbol"]).upper(): row for row in adjustment_rows}
    missing_symbols = [symbol for symbol in symbols if symbol not in by_symbol]
    unadjusted_symbols = [
        symbol
        for symbol, row in by_symbol.items()
        if int(row["total_bars"] or 0) <= 0 or int(row["adjusted_bars"] or 0) < int(row["total_bars"] or 0)
    ]
    return {
        "ready": bool(symbols) and not missing_symbols and not unadjusted_symbols,
        "symbols": symbols,
        "missing_symbols": missing_symbols,
        "unadjusted_symbols": unadjusted_symbols,
        "coverage": [
            {
                "symbol": symbol,
                "total_bars": int(by_symbol[symbol]["total_bars"]),
                "adjusted_bars": int(by_symbol[symbol]["adjusted_bars"]),
                "unadjusted_bars": int(by_symbol[symbol]["unadjusted_bars"]),
            }
            for symbol in symbols
            if symbol in by_symbol
        ],
    }


def _sample_status_label(row: dict[str, Any]) -> str:
    mapping = {
        "hit_take_profit": "止盈退出",
        "exit_pool": "止盈退出",
        "hit_invalidation": "失效退出",
        "window_complete": "复盘窗口结算",
        "insufficient_lookahead": "观察中",
        "not_entered": "未成交",
    }
    return mapping.get(str(row.get("close_reason") or "").strip(), "未回补")


def build_outcome_sample_payload(
    store: Store,
    *,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
) -> dict[str, Any]:
    window = _resolve_review_window(days=days, start_date=start_date, end_date=end_date)
    archive_store, _ = _open_archive_store_if_available()
    try:
        main_rows = store.load_decision_records_for_window(
            since=window["since"],
            until=window["until"],
            actions=EXECUTABLE_DECISION_ACTIONS,
        )
        archive_rows = (
            archive_store.load_decision_records_for_window(
                since=window["since"],
                until=window["until"],
                actions=EXECUTABLE_DECISION_ACTIONS,
            )
            if archive_store is not None
            else []
        )
    finally:
        if archive_store is not None:
            archive_store.close()

    merged_rows = _merge_decision_rows(list(main_rows), list(archive_rows))
    sorted_rows = sorted(
        merged_rows,
        key=lambda row: (
            0
            if str(row.get("close_reason") or "") in {"hit_take_profit", "exit_pool", "hit_invalidation", "window_complete"}
            else 1,
            str(row.get("created_at") or ""),
            str(row.get("symbol") or ""),
        ),
        reverse=True,
    )
    samples: list[dict[str, Any]] = []
    for row in sorted_rows[: max(limit, 0)]:
        price_context = _extract_sample_price_context(dict(row))
        adjustment_summary = store.summarize_price_bar_adjustment([str(row["symbol"]).upper()], "1d")
        adjustment_row = adjustment_summary[0] if adjustment_summary else None
        recompute_check = _build_sample_recompute_check(store, dict(row))
        samples.append(
            {
                "decision_id": row["decision_id"],
                "run_id": row["run_id"],
                "symbol": row["symbol"],
                "event_type": row["event_type"],
                "pool": row["pool"],
                "action": row["action"],
                "priority": row["priority"],
                "trigger_mode": row["trigger_mode"],
                "created_at": row["created_at"],
                "status_label": _sample_status_label(dict(row)),
                "entered": bool(row.get("entered")),
                "entered_at": row["entered_at"],
                "entry_price": row["entry_price"],
                "exit_price": row["exit_price"],
                "realized_return": row["realized_return"],
                "holding_days": row["holding_days"],
                "entry_range": price_context["entry_range"],
                "take_profit_range": price_context["take_profit_range"],
                "invalidation_level": price_context["invalidation_level"],
                "invalidation_reason": price_context["invalidation_reason"],
                "bar_adjustment": (
                    {
                        "total_bars": int(adjustment_row["total_bars"]),
                        "adjusted_bars": int(adjustment_row["adjusted_bars"]),
                        "unadjusted_bars": int(adjustment_row["unadjusted_bars"]),
                    }
                    if adjustment_row is not None
                    else None
                ),
                "bar_preview": _build_sample_bar_preview(store, dict(row), price_context),
                "recompute_check": recompute_check,
            }
        )
    consistency_summary = {
        "matched": sum(1 for item in samples if (item.get("recompute_check") or {}).get("status") == "一致"),
        "mismatched": sum(1 for item in samples if (item.get("recompute_check") or {}).get("status") == "不一致"),
        "unavailable": sum(1 for item in samples if (item.get("recompute_check") or {}).get("status") == "无法重算"),
    }
    return {
        "review_window": window,
        "sample_count": len(samples),
        "consistency_summary": consistency_summary,
        "samples": samples,
    }


def _summarize_outcome_sample_audit(payload: dict[str, Any]) -> dict[str, Any]:
    consistency_summary = payload.get("consistency_summary") or {}
    sample_count = int(payload.get("sample_count") or 0)
    matched = int(consistency_summary.get("matched") or 0)
    mismatched = int(consistency_summary.get("mismatched") or 0)
    unavailable = int(consistency_summary.get("unavailable") or 0)
    if sample_count <= 0:
        status = "未抽到样本"
        summary_line = "当前窗口内没有可抽检样本，暂时无法做程序一致性核对。"
    elif mismatched > 0:
        status = "程序抽检未通过"
        summary_line = f"抽检 {sample_count} 条样本，其中 {mismatched} 条与当前口径不一致。"
    elif unavailable > 0:
        status = "程序抽检部分通过"
        summary_line = f"抽检 {sample_count} 条样本，其中 {matched} 条一致，另有 {unavailable} 条暂时无法重算。"
    else:
        status = "程序抽检通过"
        summary_line = f"抽检 {sample_count} 条样本，全部与当前复盘口径一致。"
    return {
        "sample_count": sample_count,
        "matched": matched,
        "mismatched": mismatched,
        "unavailable": unavailable,
        "status": status,
        "summary_line": summary_line,
    }


def _load_ai_outcome_review(store: Store) -> dict[str, Any]:
    raw = store.get_state(AI_OUTCOME_REVIEW_STATE_KEY)
    if not raw:
        raw = store.get_state(LEGACY_MANUAL_OUTCOME_AUDIT_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _ai_review_matches_review(ai_review: dict[str, Any], review_window: dict[str, Any]) -> bool:
    if not ai_review:
        return False
    if str(ai_review.get("review_version") or "") != HISTORICAL_EFFECT_REVIEW_VERSION:
        return False
    if str(ai_review.get("status") or "") != "通过":
        return False
    audit_window = ai_review.get("review_window") or {}
    return (
        str(audit_window.get("start_date") or "") == str(review_window.get("start_date") or "")
        and str(audit_window.get("end_date") or "") == str(review_window.get("end_date") or "")
    )


def _load_review_baseline(store: Store) -> dict[str, Any]:
    raw = store.get_state(HISTORICAL_EFFECT_BASELINE_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _review_baseline_matches_version(baseline: dict[str, Any]) -> bool:
    if not baseline:
        return False
    return str(baseline.get("review_version") or "") == HISTORICAL_EFFECT_REVIEW_VERSION


def format_outcome_sample_payload(payload: dict[str, Any]) -> str:
    window = payload.get("review_window") or {}
    samples = list(payload.get("samples") or [])
    lines = [
        "后验样本抽检：",
        f"统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}",
        f"抽样数量：{len(samples)}",
    ]
    consistency_summary = payload.get("consistency_summary") or {}
    if consistency_summary:
        lines.append(
            "核对摘要："
            f" 一致 {consistency_summary.get('matched', 0)}"
            f" / 不一致 {consistency_summary.get('mismatched', 0)}"
            f" / 无法重算 {consistency_summary.get('unavailable', 0)}"
        )
    if not samples:
        lines.append("当前窗口内没有可抽检的可执行决策样本。")
        return "\n".join(lines)
    for index, row in enumerate(samples, start=1):
        entry = row.get("entry_range") or {}
        take_profit = row.get("take_profit_range") or {}
        entry_text = "-"
        if entry:
            entry_text = f"{entry.get('low', '-')}-{entry.get('high', '-')}"
        take_profit_text = "-"
        if take_profit:
            take_profit_text = f"{take_profit.get('low', '-')}-{take_profit.get('high', '-')}"
        lines.append(
            f"{index}. {row.get('symbol', '-')} | {row.get('action', '-')} | {row.get('event_type', '-')} | {row.get('status_label', '-')}"
        )
        lines.append(
            f"   决策：{row.get('decision_id', '-')} | run={row.get('run_id', '-')} | 创建时间 {row.get('created_at', '-')}"
        )
        lines.append(
            f"   价格计划：入场 {entry_text}，止盈 {take_profit_text}，失效价 {row.get('invalidation_level', '-')}"
        )
        lines.append(
            f"   执行结果：进场 {row.get('entered', False)}，进场时间 {row.get('entered_at', '-') or '-'}，进场价 {row.get('entry_price', '-') or '-'}，退出价 {row.get('exit_price', '-') or '-'}，真实收益 {row.get('realized_return', '-') or '-'}%，持有 {row.get('holding_days', '-') if row.get('holding_days') is not None else '-'} 天"
        )
        adjustment = row.get("bar_adjustment") or {}
        if adjustment:
            lines.append(
                f"   日线复权：总计 {adjustment.get('total_bars', 0)}，复权 {adjustment.get('adjusted_bars', 0)}，未复权 {adjustment.get('unadjusted_bars', 0)}"
            )
        preview = list(row.get("bar_preview") or [])
        recompute_check = row.get("recompute_check") or {}
        lines.append(f"   重算核对：{recompute_check.get('status', '-')}")
        for issue in recompute_check.get("issues", []):
            lines.append(f"     * {issue}")
        if preview:
            lines.append("   K线轨迹：")
            for bar in preview:
                tags = " / ".join(bar.get("tags") or []) or "-"
                lines.append(
                    f"     - {bar.get('session_date', '-')} O={bar.get('open', '-')} H={bar.get('high', '-')} L={bar.get('low', '-')} C={bar.get('close', '-')} | 复权={'是' if bar.get('adjusted') else '否'} | 标记={tags}"
                )
    return "\n".join(lines)


def build_ai_outcome_review_payload(
    store: Store,
    *,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
    reviewer: str = "codex",
) -> dict[str, Any]:
    sample_payload = build_outcome_sample_payload(
        store,
        days=days,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )
    audit_summary = _summarize_outcome_sample_audit(sample_payload)
    status = "通过" if audit_summary["mismatched"] == 0 and audit_summary["unavailable"] == 0 and audit_summary["sample_count"] > 0 else "未通过"
    return {
        "status": status,
        "review_version": HISTORICAL_EFFECT_REVIEW_VERSION,
        "review_window": sample_payload.get("review_window") or {},
        "reviewed_at": utcnow().isoformat(),
        "reviewer": reviewer,
        "sample_count": audit_summary["sample_count"],
        "matched": audit_summary["matched"],
        "mismatched": audit_summary["mismatched"],
        "unavailable": audit_summary["unavailable"],
        "summary_line": audit_summary["summary_line"],
        "decision_ids": [
            str(sample.get("decision_id") or "")
            for sample in sample_payload.get("samples") or []
            if str(sample.get("decision_id") or "")
        ],
        "source_sample_audit": sample_payload,
    }


def format_ai_outcome_review_payload(payload: dict[str, Any]) -> str:
    window = payload.get("review_window") or {}
    lines = [
        "AI样本复核记录：",
        f"状态：{payload.get('status', '-')}",
        f"复盘口径版本：{payload.get('review_version', '-')}",
        f"统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}",
        f"核对时间：{format_beijing_minute(payload.get('reviewed_at'))}",
        f"核对人：{payload.get('reviewer', '-')}",
        f"说明：{payload.get('summary_line', '-')}",
        (
            "样本摘要："
            f" 抽检 {payload.get('sample_count', 0)}"
            f" / 一致 {payload.get('matched', 0)}"
            f" / 不一致 {payload.get('mismatched', 0)}"
            f" / 无法重算 {payload.get('unavailable', 0)}"
        ),
    ]
    decision_ids = list(payload.get("decision_ids") or [])
    if decision_ids:
        lines.append(f"覆盖决策：{'、'.join(decision_ids)}")
    return "\n".join(lines)


def build_review_baseline_payload(*, reviewer: str = "codex", note: str = "") -> dict[str, Any]:
    return {
        "status": "已冻结",
        "review_version": HISTORICAL_EFFECT_REVIEW_VERSION,
        "frozen_at": utcnow().isoformat(),
        "reviewer": reviewer,
        "note": note,
    }


def format_review_baseline_payload(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "复盘口径冻结记录：",
            f"状态：{payload.get('status', '-')}",
            f"复盘口径版本：{payload.get('review_version', '-')}",
            f"冻结时间：{format_beijing_minute(payload.get('frozen_at'))}",
            f"冻结人：{payload.get('reviewer', '-')}",
            f"备注：{payload.get('note') or '-'}",
        ]
    )


def _build_remote_market_data_provider() -> MultiSourceMarketDataProvider:
    return MultiSourceMarketDataProvider(
        [
            YahooFinanceMarketDataProvider(),
            StooqDailyMarketDataProvider(),
        ]
    )


def build_service(
    settings: Settings,
    replay_path: str = "",
    run_name: str = "",
    note: str = "",
    runtime_config: AgentRuntimeConfig | None = None,
    apply_runtime_config: bool = True,
) -> SatelliteAgentService:
    runtime_config = runtime_config or AgentRuntimeConfig.load(settings.config_path)
    if apply_runtime_config:
        settings = runtime_config.apply(settings)
    store = Store(settings.database_path)
    store.initialize()
    _sync_watchlist_if_needed(store, settings, runtime_config)
    watchlist = store.load_watchlist()
    stock_watchlist = store.load_watchlist(asset_type="stock")
    prewatch_symbols = _resolve_prewatch_symbols(runtime_config, watchlist)
    google_feed_symbols = _limit_symbols_for_source(
        _prioritize_symbols_for_source(prewatch_symbols, watchlist),
        settings.max_google_feed_symbols_per_run,
    )
    adapters = []
    if replay_path:
        adapters.append(JsonlReplaySourceAdapter(Path(replay_path)))
    sec_watchlist = sorted(symbol for symbol in stock_watchlist if _is_sec_eligible_symbol(symbol))
    if settings.use_sec_filings_source and sec_watchlist:
        adapters.append(SecFilingsSourceAdapter(symbols=sec_watchlist, user_agent=settings.sec_user_agent))
    if settings.use_google_news_source and google_feed_symbols:
        adapters.append(GoogleNewsSourceAdapter(symbols=google_feed_symbols, mode="news"))
    if settings.use_google_research_source and google_feed_symbols:
        adapters.append(GoogleNewsSourceAdapter(symbols=google_feed_symbols, mode="research"))
    source_adapter = CompositeSourceAdapter(adapters) if adapters else StaticSourceAdapter([])
    extractor = OpenAIExtractor(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        base_url=settings.openai_base_url,
    )
    transport = FeishuTransport(settings.feishu_webhook) if settings.feishu_webhook else None
    notifier = Notifier(store=store, transport=transport, dry_run=settings.dry_run)
    provider = CachedMarketDataProvider(store=store, remote_provider=_build_remote_market_data_provider())
    return SatelliteAgentService(
        settings=settings,
        store=store,
        source_adapter=source_adapter,
        normalizer=EventNormalizer(),
        extractor=extractor,
        market_data=MarketDataEngine(provider),
        scorer=SignalScorer(settings),
        entry_exit=EntryExitEngine(),
        notifier=notifier,
        runtime_snapshot={
            "settings": settings.to_record(),
            "runtime_config": runtime_config.to_record(),
            "replay_path": replay_path,
            "active_watchlist_count": len(watchlist),
            "active_google_feed_symbol_count": len(google_feed_symbols),
        },
        run_name=run_name,
        note=note,
        prewatch_symbols=prewatch_symbols,
    )


def _sync_watchlist_if_needed(store: Store, settings: Settings, runtime_config: AgentRuntimeConfig) -> None:
    target_stocks: list[str] = []
    target_etfs: list[str] = []
    if runtime_config.has_watchlist():
        target_stocks = runtime_config.watchlist.stocks
        target_etfs = runtime_config.watchlist.etfs
    else:
        default_runtime_config = load_default_template_runtime_config()
        if default_runtime_config.has_watchlist():
            target_stocks = default_runtime_config.watchlist.stocks
            target_etfs = default_runtime_config.watchlist.etfs
    if not target_stocks and not target_etfs:
        return
    current_stocks = store.load_watchlist(asset_type="stock")
    current_etfs = store.load_watchlist(asset_type="etf")
    if current_stocks == set(target_stocks) and current_etfs == set(target_etfs):
        return
    store.replace_watchlist(target_stocks, target_etfs)


def _is_sec_eligible_symbol(symbol: str) -> bool:
    normalized = symbol.upper()
    if normalized in SEC_EXCLUDED_SYMBOLS:
        return False
    if "." in normalized or "-" in normalized:
        return False
    if len(normalized) == 5 and normalized.endswith(("Y", "F")):
        return False
    return True


def _resolve_prewatch_symbols(runtime_config: AgentRuntimeConfig, watchlist: set[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def append_symbols(symbols: list[str]) -> None:
        for symbol in symbols:
            normalized = symbol.upper()
            if normalized not in watchlist or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)

    if runtime_config.has_watchlist():
        append_symbols(runtime_config.watchlist.stocks)
        append_symbols(runtime_config.watchlist.etfs)
    else:
        default_runtime_config = load_default_template_runtime_config()
        if default_runtime_config.has_watchlist():
            append_symbols(default_runtime_config.watchlist.stocks)
            append_symbols(default_runtime_config.watchlist.etfs)

    if not ordered:
        ordered.extend(sorted(watchlist))

    return ordered


def _prioritize_symbols_for_source(preferred_symbols: list[str], watchlist: set[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for symbol in preferred_symbols + sorted(watchlist):
        normalized = symbol.upper()
        if normalized not in watchlist or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _limit_symbols_for_source(symbols: list[str], cap: int) -> list[str]:
    if cap <= 0:
        return list(symbols)
    return list(symbols[:cap])


def _add_experiment_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--name", default="", help="Optional run label")
    parser.add_argument("--note", default="", help="Optional run note")
    parser.add_argument("--event-score-threshold", type=float, default=None, help="Override global event score threshold")
    parser.add_argument(
        "--swing-market-score-threshold",
        type=float,
        default=None,
        help="Override swing market confirmation threshold",
    )
    parser.add_argument(
        "--position-market-score-threshold",
        type=float,
        default=None,
        help="Override position market confirmation threshold",
    )
    parser.add_argument(
        "--swing-priority-threshold",
        type=float,
        default=None,
        help="Override swing high-priority threshold",
    )
    parser.add_argument(
        "--position-priority-threshold",
        type=float,
        default=None,
        help="Override position high-priority threshold",
    )
    parser.add_argument(
        "--event-weight-importance",
        type=float,
        default=None,
        help="Override event score weight for importance",
    )
    parser.add_argument(
        "--event-weight-source-credibility",
        type=float,
        default=None,
        help="Override event score weight for source credibility",
    )
    parser.add_argument(
        "--event-weight-novelty",
        type=float,
        default=None,
        help="Override event score weight for novelty",
    )
    parser.add_argument(
        "--event-weight-theme-relevance",
        type=float,
        default=None,
        help="Override event score weight for theme relevance",
    )
    parser.add_argument(
        "--event-weight-sentiment",
        type=float,
        default=None,
        help="Override event score weight for sentiment strength",
    )


def _apply_cli_experiment_overrides(settings: Settings, args: argparse.Namespace) -> Settings:
    horizon_overrides = {
        "swing": {
            "market_score_threshold": getattr(args, "swing_market_score_threshold", None),
            "priority_threshold": getattr(args, "swing_priority_threshold", None),
        },
        "position": {
            "market_score_threshold": getattr(args, "position_market_score_threshold", None),
            "priority_threshold": getattr(args, "position_priority_threshold", None),
        },
    }
    return settings.with_strategy_overrides(
        event_score_threshold=getattr(args, "event_score_threshold", None),
        event_score_weights={
            "importance": getattr(args, "event_weight_importance", None),
            "source_credibility": getattr(args, "event_weight_source_credibility", None),
            "novelty": getattr(args, "event_weight_novelty", None),
            "theme_relevance": getattr(args, "event_weight_theme_relevance", None),
            "sentiment": getattr(args, "event_weight_sentiment", None),
        },
        horizons=horizon_overrides,
    )


def _apply_override_mapping(settings: Settings, overrides: dict) -> Settings:
    next_settings = settings.with_strategy_overrides(
        event_score_threshold=overrides.get("event_score_threshold"),
        event_score_weights=overrides.get("event_score_weights"),
        horizons={
            "swing": {
                "market_score_threshold": overrides.get("swing_market_score_threshold"),
                "priority_threshold": overrides.get("swing_priority_threshold"),
            },
            "position": {
                "market_score_threshold": overrides.get("position_market_score_threshold"),
                "priority_threshold": overrides.get("position_priority_threshold"),
            },
        },
    )
    toggle_overrides = {
        "use_llm_event_extraction": overrides.get("use_llm_event_extraction"),
        "use_llm_narration": overrides.get("use_llm_narration"),
        "use_llm_ranking_assist": overrides.get("use_llm_ranking_assist"),
        "use_macro_risk_overlay": overrides.get("use_macro_risk_overlay"),
    }
    toggle_overrides = {key: value for key, value in toggle_overrides.items() if value is not None}
    return next_settings.with_overrides(**toggle_overrides) if toggle_overrides else next_settings


def _resolve_replay_path(spec_path: Path, replay_path: str) -> str:
    candidate = Path(replay_path)
    if candidate.is_absolute():
        return str(candidate)
    return str((spec_path.parent / candidate).resolve())


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "experiment"


def import_bars(store: Store, path: Path, symbol: str, timeframe: str) -> int:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        bars = [
            Bar(
                timestamp=_parse_timestamp(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
            for row in reader
        ]
    store.upsert_price_bars(symbol=symbol.upper(), timeframe=timeframe, bars=bars)
    return len(bars)


def _parse_timestamp(raw: str):
    from datetime import datetime

    return datetime.fromisoformat(raw)


def _round_weighted_average(total: float, sample_count: int) -> float | None:
    if sample_count <= 0:
        return None
    return round(total / sample_count, 2)


def _merge_outcome_rows(
    primary_rows: list[dict] | list[object],
    secondary_rows: list[dict] | list[object],
    *,
    key_field: str,
) -> list[dict[str, int | float | str | None]]:
    merged: dict[str, dict[str, int | float | str | None]] = {}
    for rows in (primary_rows, secondary_rows):
        for raw_row in rows:
            row = dict(raw_row)
            key = str(row.get(key_field) or "")
            if not key:
                continue
            item = merged.setdefault(
                key,
                {
                    key_field: key,
                    "decision_count": 0,
                    "outcome_count": 0,
                    "pending_count": 0,
                    "take_profit_hits": 0,
                    "invalidation_hits": 0,
                    "positive_t3_count": 0,
                    "t_plus_3_sample_count": 0,
                    "max_runup_sample_count": 0,
                    "max_drawdown_sample_count": 0,
                    "_t3_total": 0.0,
                    "_runup_total": 0.0,
                    "_drawdown_total": 0.0,
                },
            )
            item["decision_count"] = int(item["decision_count"]) + int(row.get("decision_count") or 0)
            item["outcome_count"] = int(item["outcome_count"]) + int(row.get("outcome_count") or 0)
            item["pending_count"] = int(item["pending_count"]) + int(row.get("pending_count") or 0)
            item["take_profit_hits"] = int(item["take_profit_hits"]) + int(row.get("take_profit_hits") or 0)
            item["invalidation_hits"] = int(item["invalidation_hits"]) + int(row.get("invalidation_hits") or 0)
            item["positive_t3_count"] = int(item["positive_t3_count"]) + int(row.get("positive_t3_count") or 0)
            t3_samples = int(row.get("t_plus_3_sample_count") or 0)
            runup_samples = int(row.get("max_runup_sample_count") or 0)
            drawdown_samples = int(row.get("max_drawdown_sample_count") or 0)
            item["t_plus_3_sample_count"] = int(item["t_plus_3_sample_count"]) + t3_samples
            item["max_runup_sample_count"] = int(item["max_runup_sample_count"]) + runup_samples
            item["max_drawdown_sample_count"] = int(item["max_drawdown_sample_count"]) + drawdown_samples
            item["_t3_total"] = float(item["_t3_total"]) + float(row.get("avg_t_plus_3_return") or 0.0) * t3_samples
            item["_runup_total"] = float(item["_runup_total"]) + float(row.get("avg_max_runup") or 0.0) * runup_samples
            item["_drawdown_total"] = float(item["_drawdown_total"]) + float(row.get("avg_max_drawdown") or 0.0) * drawdown_samples
    merged_rows: list[dict[str, int | float | str | None]] = []
    for row in merged.values():
        merged_rows.append(
            {
                key_field: row[key_field],
                "decision_count": int(row["decision_count"]),
                "outcome_count": int(row["outcome_count"]),
                "pending_count": int(row["pending_count"]),
                "take_profit_hits": int(row["take_profit_hits"]),
                "invalidation_hits": int(row["invalidation_hits"]),
                "positive_t3_count": int(row["positive_t3_count"]),
                "t_plus_3_sample_count": int(row["t_plus_3_sample_count"]),
                "max_runup_sample_count": int(row["max_runup_sample_count"]),
                "max_drawdown_sample_count": int(row["max_drawdown_sample_count"]),
                "avg_t_plus_3_return": _round_weighted_average(float(row["_t3_total"]), int(row["t_plus_3_sample_count"])),
                "avg_max_runup": _round_weighted_average(float(row["_runup_total"]), int(row["max_runup_sample_count"])),
                "avg_max_drawdown": _round_weighted_average(float(row["_drawdown_total"]), int(row["max_drawdown_sample_count"])),
            }
        )
    return sorted(
        merged_rows,
        key=lambda row: (
            -int(row["outcome_count"]),
            -(float(row["avg_t_plus_3_return"]) if row["avg_t_plus_3_return"] is not None else float("-inf")),
            -int(row["decision_count"]),
            str(row[key_field]),
        ),
    )


def _summarize_outcome_rows(rows: list[dict] | list[object]) -> dict[str, int]:
    total_decisions = 0
    total_outcomes = 0
    total_pending = 0
    for raw_row in rows:
        row = dict(raw_row)
        total_decisions += int(row.get("decision_count") or 0)
        total_outcomes += int(row.get("outcome_count") or 0)
        total_pending += int(row.get("pending_count") or 0)
    total_completed = max(total_outcomes - total_pending, 0)
    return {
        "decision_count": total_decisions,
        "outcome_count": total_outcomes,
        "pending_count": total_pending,
        "completed_count": total_completed,
    }


def _row_to_outcome_summary(row: object | dict | None) -> dict[str, int]:
    if row is None:
        return {
            "decision_count": 0,
            "outcome_count": 0,
            "pending_count": 0,
            "completed_count": 0,
        }
    payload = dict(row)
    return {
        "decision_count": int(payload.get("decision_count") or 0),
        "outcome_count": int(payload.get("outcome_count") or 0),
        "pending_count": int(payload.get("pending_count") or 0),
        "completed_count": int(payload.get("completed_count") or 0),
    }


def _merge_outcome_summaries(primary: dict[str, int], secondary: dict[str, int]) -> dict[str, int]:
    return {
        "decision_count": int(primary.get("decision_count", 0)) + int(secondary.get("decision_count", 0)),
        "outcome_count": int(primary.get("outcome_count", 0)) + int(secondary.get("outcome_count", 0)),
        "pending_count": int(primary.get("pending_count", 0)) + int(secondary.get("pending_count", 0)),
        "completed_count": int(primary.get("completed_count", 0)) + int(secondary.get("completed_count", 0)),
    }


def _open_archive_store_if_available(path: Path | None = None) -> tuple[Store | None, Path | None]:
    archive_path = (path or DEFAULT_ARCHIVE_DB_PATH).resolve()
    if not archive_path.exists():
        return None, None
    archive_store = Store(archive_path)
    archive_store.initialize()
    return archive_store, archive_path


def build_strategy_report_payload(
    store: Store,
    *,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
    archive_store: Store | None = None,
    archive_db_path: str = "",
) -> dict:
    window = _resolve_review_window(days=days, start_date=start_date, end_date=end_date)
    since = window["since"]
    until = window["until"]
    event_types = store.aggregate_event_type_performance(since=since, until=until, limit=limit)
    source_stability = store.aggregate_source_stability(since=since, until=until, limit=limit)
    alert_volume = store.aggregate_alert_volume(since=since, until=until, limit=window["window_days"])
    main_event_outcomes = store.aggregate_decision_outcomes_by_event_type(
        since=since,
        until=until,
        limit=limit,
        actions=EXECUTABLE_DECISION_ACTIONS,
    )
    main_pool_outcomes = store.aggregate_decision_outcomes_by_pool(
        since=since,
        until=until,
        limit=limit,
        actions=EXECUTABLE_DECISION_ACTIONS,
    )
    main_summary = _row_to_outcome_summary(
        store.summarize_decision_outcomes(since, until, actions=EXECUTABLE_DECISION_ACTIONS)
    )
    archive_event_outcomes: list[dict] | list[object] = []
    archive_pool_outcomes: list[dict] | list[object] = []
    archive_summary = {
        "decision_count": 0,
        "outcome_count": 0,
        "pending_count": 0,
        "completed_count": 0,
    }
    if archive_store is not None:
        archive_event_outcomes = archive_store.aggregate_decision_outcomes_by_event_type(
            since=since,
            until=until,
            limit=limit,
            actions=EXECUTABLE_DECISION_ACTIONS,
        )
        archive_pool_outcomes = archive_store.aggregate_decision_outcomes_by_pool(
            since=since,
            until=until,
            limit=limit,
            actions=EXECUTABLE_DECISION_ACTIONS,
        )
        archive_summary = _row_to_outcome_summary(
            archive_store.summarize_decision_outcomes(since, until, actions=EXECUTABLE_DECISION_ACTIONS)
        )
    decision_outcomes_by_event_type = _merge_outcome_rows(
        list(main_event_outcomes),
        list(archive_event_outcomes),
        key_field="event_type",
    )[:limit]
    decision_outcomes_by_pool = _merge_outcome_rows(
        list(main_pool_outcomes),
        list(archive_pool_outcomes),
        key_field="pool",
    )[:limit]
    scope = "main_plus_archive" if archive_store is not None else "main_only"
    scope_label = "主库热数据 + 归档库" if archive_store is not None else "主库热数据"
    combined_summary = _merge_outcome_summaries(main_summary, archive_summary)
    return serialize_strategy_report(
        event_types,
        source_stability,
        alert_volume,
        decision_outcomes_by_event_type,
        decision_outcomes_by_pool,
        {
            "scope": scope,
            "scope_label": scope_label,
            "window_days": window["window_days"],
            "start_date": window["start_date"],
            "end_date": window["end_date"],
            "main_store": main_summary,
            "archive_store": {
                "present": archive_store is not None,
                **archive_summary,
            },
            "combined": combined_summary,
            "archive_db_path": archive_db_path,
            "archive_label": f"归档库（{archive_db_path}）" if archive_db_path else "归档库",
        },
    )


def _build_run_scoped_strategy_report(store: Store, *, run_id: str, limit: int) -> dict:
    event_type_rows = store.aggregate_event_type_performance_for_run(run_id, limit=limit)
    alert_rows = store.aggregate_alert_volume_for_run(run_id)
    decision_outcomes_by_event_type = store.aggregate_decision_outcomes_by_event_type_for_run(
        run_id,
        limit=limit,
        actions=EXECUTABLE_DECISION_ACTIONS,
    )
    decision_outcomes_by_pool = store.aggregate_decision_outcomes_by_pool_for_run(
        run_id,
        limit=limit,
        actions=EXECUTABLE_DECISION_ACTIONS,
    )
    source_health_rows = serialize_source_health(store.load_source_health(run_id))
    source_stability = []
    for row in source_health_rows:
        source_stability.append(
            {
                "source_name": row["source_name"],
                "check_count": 1,
                "healthy_count": 1 if row["status"] == "healthy" else 0,
                "unhealthy_count": 0 if row["status"] == "healthy" else 1,
                "avg_latency_ms": row["latency_ms"],
                "last_checked_at": row["checked_at"],
            }
        )
    return serialize_strategy_report(
        event_type_rows,
        source_stability,
        [
            {
                "bucket_date": run_id,
                "total_alerts": row["total_alerts"],
                "sent_alerts": row["sent_alerts"],
                "suppressed_alerts": row["suppressed_alerts"],
                "high_priority_alerts": row["high_priority_alerts"],
                "sent_high_priority_alerts": row["sent_high_priority_alerts"],
            }
            for row in alert_rows
        ],
        decision_outcomes_by_event_type,
        decision_outcomes_by_pool,
    )


def _build_historical_strategy_report(
    store: Store,
    *,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
) -> dict:
    archive_store, archive_db_path = _open_archive_store_if_available()
    try:
        return build_strategy_report_payload(
            store,
            days=days,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            archive_store=archive_store,
            archive_db_path=str(archive_db_path) if archive_db_path is not None else "",
        )
    finally:
        if archive_store is not None:
            archive_store.close()


def _build_historical_outcome_context(store: Store, *, days: int, limit: int) -> tuple[dict[str, dict], dict[str, dict]]:
    since = (utcnow() - timedelta(days=days)).isoformat()
    archive_store, _archive_db_path = _open_archive_store_if_available()
    try:
        event_type_context_rows = _merge_outcome_rows(
            list(store.aggregate_decision_outcomes_by_event_type(since=since, limit=limit, actions=EXECUTABLE_DECISION_ACTIONS)),
            list(archive_store.aggregate_decision_outcomes_by_event_type(since=since, limit=limit, actions=EXECUTABLE_DECISION_ACTIONS)) if archive_store is not None else [],
            key_field="event_type",
        )[:limit]
        pool_context_rows = _merge_outcome_rows(
            list(store.aggregate_decision_outcomes_by_pool(since=since, limit=limit, actions=EXECUTABLE_DECISION_ACTIONS)),
            list(archive_store.aggregate_decision_outcomes_by_pool(since=since, limit=limit, actions=EXECUTABLE_DECISION_ACTIONS)) if archive_store is not None else [],
            key_field="pool",
        )[:limit]
    finally:
        if archive_store is not None:
            archive_store.close()
    return _build_outcome_context_index(event_type_context_rows, pool_context_rows)


def build_replay_evaluation_payload(store: Store, *, run_id: str, days: int, limit: int) -> dict:
    run_row = store.load_run(run_id)
    run_detail = serialize_run_detail(run_row, store.load_logs(run_id, limit=200)) if run_row else None
    if run_detail is not None:
        run_detail["candidate_evaluation_summary"] = _build_candidate_evaluation_summary(
            [dict(row) for row in store.load_candidate_evaluations(run_id)],
            limit=limit,
        )
    strategy_report = _build_run_scoped_strategy_report(store, run_id=run_id, limit=limit)
    source_health = serialize_source_health(store.load_source_health(run_id))
    event_type_context, pool_context = _build_historical_outcome_context(store, days=days, limit=limit)
    card_diagnostics = _build_card_diagnostics(
        run_detail,
        store.load_opportunity_cards(run_id),
        store.load_alert_history(run_id),
    )
    decision_diagnostics = _build_decision_diagnostics(
        store.load_decision_records(run_id),
        event_type_context=event_type_context,
        pool_context=pool_context,
    )
    return serialize_replay_evaluation(
        run_detail,
        strategy_report,
        source_health,
        card_diagnostics,
        decision_diagnostics,
    )


def build_run_comparison_payload(store: Store, *, run_ids: list[str], limit: int) -> dict:
    items = []
    for run_id in run_ids:
        run_row = store.load_run(run_id)
        if run_row is None:
            items.append({"run_id": run_id, "run": None, "strategy_report": {"event_type_performance": [], "source_stability": [], "alert_volume": []}, "source_health": []})
            continue
        run_detail = serialize_run_detail(run_row, store.load_logs(run_id, limit=100))
        items.append(
            {
                "run_id": run_id,
                "run": run_detail,
                "strategy_report": _build_run_scoped_strategy_report(store, run_id=run_id, limit=limit),
                "source_health": serialize_source_health(store.load_source_health(run_id)),
            }
        )
    return serialize_run_comparison(items)


def _build_card_diagnostics(
    run_detail: dict | None,
    card_rows: list[object],
    alert_rows: list[object] | None = None,
) -> list[dict]:
    if run_detail is None:
        return []
    config_snapshot = run_detail.get("config_snapshot", {})
    settings = config_snapshot.get("settings", {})
    horizons = settings.get("horizons", {})
    runtime_watchlist = config_snapshot.get("runtime_config", {}).get("watchlist", {})
    symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(runtime_watchlist)
    event_threshold = settings.get("event_score_threshold")
    alert_by_card_id: dict[str, dict] = {}
    for row in alert_rows or []:
        card_id = row["card_id"]
        if card_id not in alert_by_card_id:
            alert_by_card_id[card_id] = {
                "sent": bool(row["sent"]),
                "alert_reason": row["reason"],
                "notified_at": row["notified_at"],
            }
    items = []
    for row in card_rows:
        card = json.loads(row["card_json"])
        required = {
            "card_id",
            "symbol",
            "horizon",
            "event_type",
            "priority",
            "event_score",
            "market_score",
            "final_score",
        }
        if not required.issubset(card):
            continue
        horizon = card["horizon"]
        horizon_settings = horizons.get(horizon, {})
        market_threshold = horizon_settings.get("market_score_threshold")
        priority_threshold = horizon_settings.get("priority_threshold")
        event_margin = (
            round(card["event_score"] - event_threshold, 2)
            if event_threshold is not None
            else None
        )
        market_margin = (
            round(card["market_score"] - market_threshold, 2)
            if market_threshold is not None
            else None
        )
        priority_margin = (
            round(card["final_score"] - priority_threshold, 2)
            if priority_threshold is not None
            else None
        )
        alert_state = alert_by_card_id.get(card["card_id"], {})
        delivery = build_delivery_view_from_record(card)
        items.append(
            {
                "card_id": card["card_id"],
                "symbol": card["symbol"],
                "display_name": card.get("display_name", ""),
                "horizon": horizon,
                "event_type": card["event_type"],
                "priority": card["priority"],
                "action_label": card.get("action_label", ""),
                "confidence_label": card.get("confidence_label", ""),
                "headline_summary": card.get("headline_summary", ""),
                "reason_to_watch": card.get("reason_to_watch", ""),
                "positioning_hint": card.get("positioning_hint", ""),
                "delivery_view": delivery,
                "identity": delivery["identity"],
                "event_type_display": delivery["event_type_display"],
                "priority_display": delivery["priority_display"],
                "horizon_display": delivery["horizon_display"],
                "action_label_effective": delivery["action_label_effective"],
                "confidence_label_effective": delivery["confidence_label_effective"],
                "source_summary": delivery["source_summary"],
                "event_reason_line": delivery["event_reason_line"],
                "market_reason_line": delivery["market_reason_line"],
                "theme_reason_line": delivery["theme_reason_line"],
                "valid_until_text": delivery["valid_until_text"],
                "market_data_complete": delivery["market_data_complete"],
                "promoted_from_prewatch": card.get("promoted_from_prewatch", False),
                "prewatch_score": card.get("prewatch_score", 0.0),
                "prewatch_setup_type": card.get("prewatch_setup_type", ""),
                "prewatch_observation_count": card.get("prewatch_observation_count", 0),
                "prewatch_alert_sent_count": card.get("prewatch_alert_sent_count", 0),
                "prewatch_first_seen_at": card.get("prewatch_first_seen_at", ""),
                "prewatch_last_seen_at": card.get("prewatch_last_seen_at", ""),
                "prewatch_last_alert_sent_at": card.get("prewatch_last_alert_sent_at", ""),
                "prewatch_source_decision_id": card.get("prewatch_source_decision_id", ""),
                "prewatch_promotion_reason": card.get("prewatch_promotion_reason", ""),
                "theme_tags": card.get("theme_tags") or theme_tags_for_symbol(card["symbol"], symbol_theme_map),
                "confirmed_peer_symbols": card.get("confirmed_peer_symbols", []),
                "trend_state": card.get("trend_state", ""),
                "rsi_14": card.get("rsi_14"),
                "relative_volume": card.get("relative_volume"),
                "source_refs": card.get("source_refs", []),
                "event_score": card["event_score"],
                "market_score": card["market_score"],
                "final_score": card["final_score"],
                "entry_range": card.get("entry_range"),
                "take_profit_range": card.get("take_profit_range"),
                "invalidation_level": card.get("invalidation_level"),
                "invalidation_reason": card.get("invalidation_reason", ""),
                "event_threshold": event_threshold,
                "market_threshold": market_threshold,
                "priority_threshold": priority_threshold,
                "event_margin": event_margin,
                "market_margin": market_margin,
                "priority_margin": priority_margin,
                "event_pass": event_margin is None or event_margin >= 0,
                "market_pass": market_margin is None or market_margin >= 0,
                "priority_pass": priority_margin is None or priority_margin >= 0,
                "sent": alert_state.get("sent", False),
                "alert_reason": alert_state.get("alert_reason", ""),
                "notified_at": alert_state.get("notified_at", ""),
            }
        )
    return items


def _build_outcome_context_index(
    event_type_rows: list[dict] | list[object],
    pool_rows: list[dict] | list[object],
) -> tuple[dict[str, dict], dict[str, dict]]:
    event_type_context: dict[str, dict] = {}
    pool_context: dict[str, dict] = {}
    for row in event_type_rows:
        key = str(row["event_type"] or "").strip()
        if not key:
            continue
        event_type_context[key] = {
            "event_type": key,
            "decision_count": row["decision_count"],
            "outcome_count": row["outcome_count"],
            "pending_count": row["pending_count"],
            "take_profit_hits": row["take_profit_hits"],
            "invalidation_hits": row["invalidation_hits"],
            "positive_t3_count": row["positive_t3_count"],
            "avg_t_plus_3_return": row["avg_t_plus_3_return"],
        }
    for row in pool_rows:
        key = str(row["pool"] or "").strip()
        if not key:
            continue
        pool_context[key] = {
            "pool": key,
            "decision_count": row["decision_count"],
            "outcome_count": row["outcome_count"],
            "pending_count": row["pending_count"],
            "take_profit_hits": row["take_profit_hits"],
            "invalidation_hits": row["invalidation_hits"],
            "positive_t3_count": row["positive_t3_count"],
            "avg_t_plus_3_return": row["avg_t_plus_3_return"],
        }
    return event_type_context, pool_context


def _build_decision_diagnostics(
    rows: list[object],
    *,
    event_type_context: dict[str, dict] | None = None,
    pool_context: dict[str, dict] | None = None,
) -> list[dict]:
    event_type_context = event_type_context or {}
    pool_context = pool_context or {}
    items = []
    for row in rows:
        try:
            packet = json.loads(row["packet_json"]) if row["packet_json"] else {}
        except json.JSONDecodeError:
            packet = {}
        theme_ids = []
        try:
            theme_ids = json.loads(row["theme_ids_json"]) if row["theme_ids_json"] else []
        except json.JSONDecodeError:
            theme_ids = []
        event_type = str(row["event_type"] or packet.get("event_assessment", {}).get("event_type") or "").strip()
        action = str(row["action"] or "").strip()
        executable_action = action in EXECUTABLE_DECISION_ACTIONS
        items.append(
            {
                "decision_id": row["decision_id"],
                "symbol": row["symbol"],
                "event_type": event_type,
                "pool": row["pool"],
                "pool_label": {
                    "prewatch": "预备池",
                    "confirmation": "确认池",
                    "exit": "兑现池",
                }.get(row["pool"], row["pool"]),
                "action": action,
                "priority": row["priority"],
                "confidence": row["confidence"],
                "event_score": row["event_score"],
                "market_score": row["market_score"],
                "theme_score": row["theme_score"],
                "final_score": row["final_score"],
                "trigger_mode": row["trigger_mode"],
                "llm_used": bool(row["llm_used"]),
                "theme_ids": theme_ids,
                "packet": packet,
                "source_decision_id": str(packet.get("source_decision_id") or ""),
                "promoted_from_prewatch": bool(packet.get("promoted_from_prewatch")),
                "prewatch_score": float(packet.get("prewatch_score") or 0.0),
                "prewatch_setup_type": str(packet.get("prewatch_setup_type") or ""),
                "prewatch_observation_count": int(packet.get("prewatch_observation_count") or 0),
                "prewatch_alert_sent_count": int(packet.get("prewatch_alert_sent_count") or 0),
                "prewatch_first_seen_at": str(packet.get("prewatch_first_seen_at") or ""),
                "prewatch_last_seen_at": str(packet.get("prewatch_last_seen_at") or ""),
                "prewatch_last_alert_sent_at": str(packet.get("prewatch_last_alert_sent_at") or ""),
                "prewatch_promotion_reason": str(packet.get("prewatch_promotion_reason") or ""),
                "created_at": row["created_at"],
                "entered": bool(row["entered"]) if executable_action and row["entered"] is not None else False,
                "entered_at": row["entered_at"] if executable_action else "",
                "entry_price": row["entry_price"] if executable_action else None,
                "exit_price": row["exit_price"] if executable_action else None,
                "realized_return": row["realized_return"] if executable_action else None,
                "holding_days": row["holding_days"] if executable_action else None,
                "t_plus_1_return": row["t_plus_1_return"] if executable_action else None,
                "t_plus_3_return": row["t_plus_3_return"] if executable_action else None,
                "t_plus_5_return": row["t_plus_5_return"] if executable_action else None,
                "t_plus_7_return": row["t_plus_7_return"] if executable_action else None,
                "t_plus_10_return": row["t_plus_10_return"] if executable_action else None,
                "t_plus_14_return": row["t_plus_14_return"] if executable_action else None,
                "t_plus_30_return": row["t_plus_30_return"] if executable_action else None,
                "max_runup": row["max_runup"] if executable_action else None,
                "max_drawdown": row["max_drawdown"] if executable_action else None,
                "hit_take_profit": bool(row["hit_take_profit"]) if executable_action and row["hit_take_profit"] is not None else False,
                "hit_invalidation": bool(row["hit_invalidation"]) if executable_action and row["hit_invalidation"] is not None else False,
                "close_reason": (row["close_reason"] or "") if executable_action else "",
                "event_type_outcome_context": event_type_context.get(event_type, {}),
                "pool_outcome_context": pool_context.get(str(row["pool"] or ""), {}),
            }
        )
    by_decision_id = {str(item.get("decision_id") or ""): item for item in items}
    for item in items:
        source_decision_id = str(item.get("source_decision_id") or "").strip()
        if not source_decision_id:
            continue
        source_item = by_decision_id.get(source_decision_id)
        if source_item is None:
            continue
        item["source_decision_summary"] = {
            "decision_id": source_decision_id,
            "symbol": source_item.get("symbol"),
            "pool": source_item.get("pool"),
            "pool_label": source_item.get("pool_label"),
            "action": source_item.get("action"),
            "priority": source_item.get("priority"),
            "confidence": source_item.get("confidence"),
            "final_score": source_item.get("final_score"),
            "created_at": source_item.get("created_at"),
            "promoted_from_prewatch": bool(source_item.get("promoted_from_prewatch")),
            "prewatch_score": source_item.get("prewatch_score"),
            "prewatch_setup_type": source_item.get("prewatch_setup_type"),
            "prewatch_observation_count": source_item.get("prewatch_observation_count"),
            "prewatch_alert_sent_count": source_item.get("prewatch_alert_sent_count"),
            "prewatch_first_seen_at": source_item.get("prewatch_first_seen_at"),
            "prewatch_last_seen_at": source_item.get("prewatch_last_seen_at"),
            "prewatch_last_alert_sent_at": source_item.get("prewatch_last_alert_sent_at"),
            "prewatch_promotion_reason": source_item.get("prewatch_promotion_reason"),
        }
    return items


def format_decision_outcome_backfill(payload: dict) -> str:
    lines = [
        "决策结果回写：",
        f"扫描记录：{payload.get('scanned', 0)}",
        f"成功回写：{payload.get('updated', 0)}",
        f"跳过：{payload.get('skipped', 0)}",
        f"运行范围：{payload.get('run_id') or '全部运行'}",
        (
            "结果状态："
            f"完整窗口 {payload.get('completed_window', 0)} / "
            f"等待更多 bars {payload.get('pending_lookahead', 0)}"
        ),
        (
            "命中情况："
            f"止盈 {payload.get('take_profit_hits', 0)} / "
            f"失效 {payload.get('invalidation_hits', 0)}"
        ),
    ]
    skip_reasons = payload.get("skip_reasons") or {}
    skip_symbol_samples = payload.get("skip_symbol_samples") or {}
    if any(int(value or 0) > 0 for value in skip_reasons.values()):
        lines.append(
            "跳过原因："
            f"缺少 bars {skip_reasons.get('missing_bars', 0)} / "
            f"bars 过旧 {skip_reasons.get('stale_bars', 0)} / "
            f"缺少锚点 bar {skip_reasons.get('missing_anchor_bar', 0)} / "
            f"锚点价格无效 {skip_reasons.get('invalid_anchor_price', 0)} / "
            f"创建时间异常 {skip_reasons.get('invalid_created_at', 0)}"
        )
    stale_symbols = list(skip_symbol_samples.get("stale_bars") or [])
    if stale_symbols:
        lines.append(f"bars 过旧标的：{'、'.join(stale_symbols)}")
    missing_symbols = list(skip_symbol_samples.get("missing_bars") or [])
    if missing_symbols:
        lines.append(f"缺少 bars 标的：{'、'.join(missing_symbols)}")
    attempted_symbols = list(payload.get("fetch_attempted_symbols") or [])
    if attempted_symbols:
        lines.append(f"远程补抓尝试：{'、'.join(attempted_symbols)}")
    failed_symbols = list(payload.get("fetch_failed_symbols") or [])
    if failed_symbols:
        lines.append(f"远程补抓未更新：{'、'.join(failed_symbols)}")
    failure_reasons = payload.get("fetch_failure_reasons") or {}
    if failure_reasons:
        parts = [f"{symbol}={reason}" for symbol, reason in failure_reasons.items()]
        lines.append(f"远程补抓原因：{'；'.join(parts)}")
    stale_symbol_details = payload.get("stale_symbol_details") or {}
    if stale_symbol_details:
        detail_parts: list[str] = []
        for symbol in sorted(stale_symbol_details):
            detail = stale_symbol_details.get(symbol) or {}
            target_session = str(detail.get("target_session") or "-")
            latest_local = str(detail.get("latest_local_session") or "-")
            latest_remote = str(detail.get("latest_remote_session") or "-")
            detail_parts.append(
                f"{symbol}(目标 {target_session} / 本地最新 {latest_local} / 远程最新 {latest_remote})"
            )
        lines.append(f"过旧详情：{'；'.join(detail_parts)}")
    return "\n".join(lines)


def format_decision_history_archive(payload: dict) -> str:
    lines = [
        "决策历史归档：",
        f"模式：{'正式归档' if payload.get('mode') == 'apply' else '预演'}",
        f"归档截止：{payload.get('before', '-')}",
        f"扫描记录：{payload.get('scanned', 0)}",
        f"决策记录：{payload.get('decision_records', 0)}",
        f"后验结果：{payload.get('decision_outcomes', 0)}",
    ]
    if payload.get("archive_db_path"):
        lines.append(f"归档库：{payload.get('archive_db_path')}")
    if payload.get("mode") == "apply":
        lines.append(f"已复制：记录 {payload.get('copied_records', 0)} / 后验 {payload.get('copied_outcomes', 0)}")
        lines.append(f"已删除：记录 {payload.get('deleted_records', 0)} / 后验 {payload.get('deleted_outcomes', 0)}")
    else:
        lines.append("当前仅做预演统计，未修改主库。")
    return "\n".join(lines)


def build_non_executable_outcome_cleanup_payload(
    store: Store,
    *,
    actions: tuple[str, ...] | list[str] = (OBSERVATION_DECISION_ACTION,),
    since: str = "",
    until: str = "",
    limit: int = 0,
    apply: bool = False,
) -> dict[str, Any]:
    rows = [dict(row) for row in store.load_decision_outcomes_for_actions(actions=actions, since=since, until=until, limit=limit)]
    decision_ids = [str(row.get("decision_id") or "").strip() for row in rows if str(row.get("decision_id") or "").strip()]
    deleted_count = store.delete_decision_outcomes(decision_ids) if apply and decision_ids else 0
    sample_rows = []
    for row in rows[:10]:
        sample_rows.append(
            {
                "decision_id": str(row.get("decision_id") or ""),
                "run_id": str(row.get("run_id") or ""),
                "symbol": str(row.get("symbol") or ""),
                "pool": str(row.get("pool") or ""),
                "action": str(row.get("action") or ""),
                "created_at": str(row.get("created_at") or ""),
                "entered": bool(row.get("entered")),
                "close_reason": str(row.get("close_reason") or ""),
                "realized_return": row.get("realized_return"),
                "outcome_updated_at": str(row.get("outcome_updated_at") or ""),
            }
        )
    return {
        "mode": "apply" if apply else "dry_run",
        "target_actions": [str(item) for item in actions if str(item).strip()],
        "since": since,
        "until": until,
        "matched_outcomes": len(rows),
        "deleted_outcomes": deleted_count,
        "sample_rows": sample_rows,
    }


def format_non_executable_outcome_cleanup(payload: dict[str, Any]) -> str:
    lines = [
        "历史后验清理：",
        f"模式：{'正式清理' if payload.get('mode') == 'apply' else '预演'}",
        f"目标动作：{'、'.join(payload.get('target_actions') or []) or '-'}",
        f"命中 outcome：{payload.get('matched_outcomes', 0)}",
    ]
    if payload.get("since") or payload.get("until"):
        lines.append(
            "时间范围："
            f"{payload.get('since') or '-'} ~ {payload.get('until') or '-'}"
        )
    if payload.get("mode") == "apply":
        lines.append(f"已删除 outcome：{payload.get('deleted_outcomes', 0)}")
    else:
        lines.append("当前仅预演统计，未修改数据库。")
    sample_rows = payload.get("sample_rows") or []
    if sample_rows:
        lines.append("样本：")
        for row in sample_rows:
            lines.append(
                "  "
                f"{row.get('created_at', '-')} | {row.get('symbol', '-')} | "
                f"{row.get('action', '-')} | entered={row.get('entered', False)} | "
                f"close_reason={row.get('close_reason', '-') or '-'} | "
                f"realized_return={row.get('realized_return', '-')}"
            )
    return "\n".join(lines)


def _auto_backfill_review_outcomes(store: Store) -> dict[str, Any]:
    provider = _build_remote_market_data_provider()

    def _fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
        return provider.get_bars(symbol, timeframe, limit)

    try:
        payload = backfill_recent_decision_outcomes(store, fetch_bars=_fetch_bars)
    except Exception as exc:
        return {
            "status": "failed",
            "updated": 0,
            "scanned": 0,
            "skipped": 0,
            "fetched_symbols": 0,
            "error": str(exc),
        }
    return {
        "status": "ok",
        **payload,
    }


def _execute_replay_evaluation(
    settings: Settings,
    runtime_config: AgentRuntimeConfig,
    *,
    replay_path: str,
    days: int,
    limit: int,
    run_name: str,
    note: str,
) -> dict:
    service = build_service(
        settings,
        replay_path=replay_path,
        run_name=run_name,
        note=note,
        runtime_config=runtime_config,
        apply_runtime_config=False,
    )
    try:
        service.run_once()
        _auto_backfill_review_outcomes(service.store)
        latest_run = service.store.load_latest_run()
        if latest_run is None:
            raise RuntimeError("No run generated during replay evaluation.")
        return build_replay_evaluation_payload(
            service.store,
            run_id=latest_run["run_id"],
            days=days,
            limit=limit,
        )
    finally:
        service.store.close()


def build_batch_replay_payload(
    settings: Settings,
    runtime_config: AgentRuntimeConfig,
    *,
    spec_path: Path,
    output_dir: Path,
) -> dict:
    with spec_path.open("r", encoding="utf-8") as handle:
        spec = json.load(handle)
    replay_path = _resolve_replay_path(spec_path, spec["replay_path"])
    days = int(spec.get("days", 14))
    limit = int(spec.get("limit", 10))
    experiments = spec.get("experiments", [])
    recommendation_preferences = spec.get("recommendation", {}).get("weights", {})
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    items = []
    for index, experiment in enumerate(experiments, start=1):
        name = str(experiment.get("name") or f"experiment_{index}")
        note = str(experiment.get("note") or "")
        overrides = experiment.get("overrides", {})
        experiment_settings = _apply_override_mapping(settings, overrides)
        db_path = output_dir / f"{batch_stamp}_{index:02d}_{_slugify(name)}.db"
        experiment_settings = experiment_settings.with_overrides(database_path=db_path)
        try:
            payload = _execute_replay_evaluation(
                experiment_settings,
                runtime_config,
                replay_path=replay_path,
                days=days,
                limit=limit,
                run_name=name,
                note=note,
            )
            run = payload["run"] or {}
            strategy_report = payload["strategy_report"]
            event_types = strategy_report.get("event_type_performance", [])
            summary = run.get("summary", {})
            card_diagnostics = payload.get("card_diagnostics", [])
            experiment_store = Store(db_path)
            experiment_store.initialize()
            historical_effect = _build_historical_effect_review_data(
                experiment_store,
                days=days,
                limit=limit,
            )
            experiment_store.close()
            overview = historical_effect.get("overview", {})
            execution_quality = historical_effect.get("execution_quality", {})
            auxiliary_observation = historical_effect.get("auxiliary_observation", [])
            t7_metric = next((row for row in auxiliary_observation if row.get("field") == "t_plus_7_return"), {})
            t14_metric = next((row for row in auxiliary_observation if row.get("field") == "t_plus_14_return"), {})
            t30_metric = next((row for row in auxiliary_observation if row.get("field") == "t_plus_30_return"), {})
            max_drawdown_metric = next((row for row in auxiliary_observation if row.get("field") == "max_drawdown"), {})
            failures = (
                summary.get("extraction_failures", 0)
                + summary.get("market_data_failures", 0)
                + summary.get("scoring_failures", 0)
                + summary.get("notification_failures", 0)
            )
            market_margins = [card["market_margin"] for card in card_diagnostics if card["market_margin"] is not None]
            priority_margins = [card["priority_margin"] for card in card_diagnostics if card["priority_margin"] is not None]
            items.append(
                {
                    "name": name,
                    "note": note,
                    "db_path": str(db_path),
                    "run_id": run.get("run_id", ""),
                    "status": run.get("status", "success"),
                    "summary": summary,
                    "config_summary": _config_summary(run.get("config_snapshot", {})),
                    "top_event": event_types[0]["event_type"] if event_types else "-",
                    "top_avg_score": event_types[0]["avg_final_score"] if event_types else None,
                    "card_diagnostics": card_diagnostics,
                    "closest_market_margin": round(min(market_margins), 2) if market_margins else None,
                    "closest_priority_margin": round(min(priority_margins), 2) if priority_margins else None,
                    "historical_effect": historical_effect,
                    "metrics": {
                        "decision_count": int(overview.get("decision_count", 0) or 0),
                        "entered_count": int(overview.get("entered_count", 0) or 0),
                        "take_profit_exit_count": int(overview.get("take_profit_exit_count", 0) or 0),
                        "invalidation_exit_count": int(overview.get("invalidation_exit_count", 0) or 0),
                        "avg_realized_return": overview.get("avg_realized_return"),
                        "win_rate": overview.get("win_rate"),
                        "profit_loss_ratio": overview.get("profit_loss_ratio"),
                        "avg_t_plus_7_return": t7_metric.get("avg_value"),
                        "avg_t_plus_14_return": t14_metric.get("avg_value"),
                        "avg_t_plus_30_return": t30_metric.get("avg_value"),
                        "avg_max_drawdown": max_drawdown_metric.get("avg_value"),
                        "completed_outcome_count": int(execution_quality.get("completed_outcome_count", 0) or 0),
                    },
                    "failures": failures,
                    "evaluation": payload,
                }
            )
        except Exception as exc:
            items.append(
                {
                    "name": name,
                    "note": note,
                    "db_path": str(db_path),
                    "run_id": "",
                    "status": "failed",
                    "summary": {},
                    "config_summary": _config_summary({"settings": experiment_settings.to_record()}),
                    "top_event": "-",
                    "top_avg_score": None,
                    "card_diagnostics": [],
                    "closest_market_margin": None,
                    "closest_priority_margin": None,
                    "failures": 1,
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
    payload = serialize_batch_replay(items, recommendation_preferences=recommendation_preferences)
    payload["batch_id"] = batch_stamp
    payload["generated_at"] = utcnow().isoformat()
    payload["spec_path"] = str(spec_path.resolve())
    payload["replay_path"] = replay_path
    payload["output_dir"] = str(output_dir.resolve())
    payload["manifest_path"] = str(_batch_manifest_path(output_dir, payload).resolve())
    payload["report_path"] = str(_default_batch_report_path(output_dir, payload).resolve())
    return payload


def _batch_manifest_path(output_dir: Path, payload: dict) -> Path:
    existing = payload.get("manifest_path")
    if existing:
        return Path(existing)
    return output_dir / f"{payload['batch_id']}_manifest.json"


def _default_batch_report_path(output_dir: Path, payload: dict) -> Path:
    existing = payload.get("report_path")
    if existing:
        return Path(existing)
    return output_dir / f"{payload['batch_id']}_report.md"


def _write_batch_manifest(output_dir: Path, payload: dict) -> Path:
    manifest_path = _batch_manifest_path(output_dir, payload)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest_path


def _write_report(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(content)
        if not content.endswith("\n"):
            handle.write("\n")
    return path


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
    return path


def _load_json_or_default(path: Path | None, default_payload: dict) -> dict:
    if path is None or not path.exists():
        return json.loads(json.dumps(default_payload))
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_manifest(manifest_path: Path) -> dict:
    resolved = manifest_path.resolve()
    with resolved.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["manifest_path"] = str(resolved)
    return payload


def _load_batch_index(manifest_dir: Path, limit: int) -> dict:
    manifest_paths = sorted(manifest_dir.glob("*_manifest.json"), reverse=True)
    payloads = [_load_manifest(path) for path in manifest_paths[:limit]]
    return serialize_batch_index(payloads)


def _preferred_runtime_config_path(settings: Settings) -> Path:
    recommended = settings.config_path.parent / "agent.recommended.json"
    if recommended.exists():
        return recommended.resolve()
    return settings.config_path.resolve()


def _preferred_batch_replay_template_path() -> str:
    preferred = Path(SATELLITE_BATCH_REPLAY_TEMPLATE_PATH)
    legacy = Path(LEGACY_BATCH_REPLAY_TEMPLATE_PATH)
    if preferred.exists() or not legacy.exists():
        return str(preferred)
    return str(legacy)


def _load_batch_replay_template_payload() -> dict[str, Any]:
    template_path = Path(_preferred_batch_replay_template_path()).resolve()
    with template_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    payload["_meta"] = {
        "managed_by": "satellite_agent",
        "editable": True,
        "template_path": str(template_path),
        "note": "这是策略赛马模板。你可以在副本里调整实验名称、阈值和开关，再用于 batch-replay。",
    }
    return payload


def _relativize_replay_path_for_output(payload: dict[str, Any], output_path: Path) -> dict[str, Any]:
    replay_path = payload.get("replay_path")
    template_path = payload.get("_meta", {}).get("template_path")
    if not replay_path or not template_path:
        return payload
    resolved = _resolve_replay_path(Path(template_path), str(replay_path))
    try:
        relative = os.path.relpath(resolved, start=str(output_path.parent.resolve()))
    except ValueError:
        relative = resolved
    next_payload = dict(payload)
    next_payload["replay_path"] = relative
    return next_payload


def _extract_recommended_experiment(payload: dict) -> dict:
    recommendation = payload.get("recommendation") or {}
    experiment_name = recommendation.get("name")
    if not experiment_name:
        raise ValueError("Batch manifest does not contain a recommendation.")
    for item in payload.get("experiments", []):
        if item.get("name") == experiment_name:
            return item
    raise ValueError(f"Recommended experiment '{experiment_name}' not found in manifest.")


def _strategy_field_map() -> list[tuple[str, str]]:
    return [
        ("event_score_threshold", "全局事件阈值"),
        ("use_llm_event_extraction", "LLM事件抽取"),
        ("event_score_weights.importance", "事件分权重-重要性"),
        ("event_score_weights.source_credibility", "事件分权重-来源可信度"),
        ("event_score_weights.novelty", "事件分权重-新颖度"),
        ("event_score_weights.theme_relevance", "事件分权重-题材相关度"),
        ("event_score_weights.sentiment", "事件分权重-事件情绪"),
        ("swing.ttl_days", "swing 持有有效期"),
        ("swing.market_score_threshold", "swing 市场阈值"),
        ("swing.priority_threshold", "swing 高优先级阈值"),
        ("swing.rsi_floor", "swing RSI 下限"),
        ("swing.rsi_ceiling", "swing RSI 上限"),
        ("swing.atr_percent_ceiling", "swing ATR% 上限"),
        ("position.ttl_days", "position 持有有效期"),
        ("position.market_score_threshold", "position 市场阈值"),
        ("position.priority_threshold", "position 高优先级阈值"),
        ("position.rsi_floor", "position RSI 下限"),
        ("position.rsi_ceiling", "position RSI 上限"),
        ("position.atr_percent_ceiling", "position ATR% 上限"),
    ]


def _extract_strategy_fields(payload: dict | None) -> dict[str, object]:
    payload = payload or {}
    strategy = payload.get("strategy", {})
    horizons = strategy.get("horizons", {})
    swing = horizons.get("swing", {})
    position = horizons.get("position", {})
    event_score_weights = strategy.get("event_score_weights", {})
    return {
        "event_score_threshold": strategy.get("event_score_threshold"),
        "use_llm_event_extraction": strategy.get("use_llm_event_extraction"),
        "event_score_weights.importance": event_score_weights.get("importance"),
        "event_score_weights.source_credibility": event_score_weights.get("source_credibility"),
        "event_score_weights.novelty": event_score_weights.get("novelty"),
        "event_score_weights.theme_relevance": event_score_weights.get("theme_relevance"),
        "event_score_weights.sentiment": event_score_weights.get("sentiment"),
        "swing.ttl_days": swing.get("ttl_days"),
        "swing.market_score_threshold": swing.get("market_score_threshold"),
        "swing.priority_threshold": swing.get("priority_threshold"),
        "swing.rsi_floor": swing.get("rsi_floor"),
        "swing.rsi_ceiling": swing.get("rsi_ceiling"),
        "swing.atr_percent_ceiling": swing.get("atr_percent_ceiling"),
        "position.ttl_days": position.get("ttl_days"),
        "position.market_score_threshold": position.get("market_score_threshold"),
        "position.priority_threshold": position.get("priority_threshold"),
        "position.rsi_floor": position.get("rsi_floor"),
        "position.rsi_ceiling": position.get("rsi_ceiling"),
        "position.atr_percent_ceiling": position.get("atr_percent_ceiling"),
    }


def _build_promoted_change_summary(previous_payload: dict | None, next_payload: dict) -> dict:
    previous_fields = _extract_strategy_fields(previous_payload)
    next_fields = _extract_strategy_fields(next_payload)
    changed = []
    unchanged = []
    for field_key, label in _strategy_field_map():
        before = previous_fields.get(field_key)
        after = next_fields.get(field_key)
        if before != after:
            changed.append(
                {
                    "field": field_key,
                    "label": label,
                    "before": before,
                    "after": after,
                }
            )
        else:
            unchanged.append(field_key)
    headline = (
        f"共 {len(changed)} 个策略字段发生变化。"
        if changed
        else "策略字段没有变化，本次主要更新推荐元信息。"
    )
    return {
        "headline": headline,
        "changed_count": len(changed),
        "unchanged_count": len(unchanged),
        "changed_fields": changed,
        "unchanged_field_keys": unchanged,
    }


def _backup_existing_file(path: Path) -> Path:
    stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_path = path.with_name(f"{path.stem}.{stamp}.bak{path.suffix}")
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup_path


def build_promoted_config_payload(
    manifest_payload: dict,
    *,
    base_config_payload: dict | None = None,
) -> dict:
    experiment = _extract_recommended_experiment(manifest_payload)
    evaluation = experiment.get("evaluation") or {}
    run_payload = evaluation.get("run") or {}
    config_snapshot = run_payload.get("config_snapshot") or {}
    settings_payload = config_snapshot.get("settings") or {}
    if not settings_payload:
        raise ValueError("Recommended experiment does not contain a config snapshot.")

    next_payload = json.loads(json.dumps(base_config_payload if base_config_payload is not None else DEFAULT_CONFIG))
    next_payload.setdefault("watchlist", {})
    next_payload.setdefault("sources", {})
    next_payload.setdefault("runtime", {})
    next_payload["strategy"] = {
        "event_score_threshold": settings_payload.get("event_score_threshold"),
        "horizons": {
            "swing": {
                "ttl_days": settings_payload.get("horizons", {}).get("swing", {}).get("ttl_days"),
                "market_score_threshold": settings_payload.get("horizons", {}).get("swing", {}).get("market_score_threshold"),
                "priority_threshold": settings_payload.get("horizons", {}).get("swing", {}).get("priority_threshold"),
                "rsi_floor": settings_payload.get("horizons", {}).get("swing", {}).get("rsi_floor"),
                "rsi_ceiling": settings_payload.get("horizons", {}).get("swing", {}).get("rsi_ceiling"),
                "atr_percent_ceiling": settings_payload.get("horizons", {}).get("swing", {}).get("atr_percent_ceiling"),
            },
            "position": {
                "ttl_days": settings_payload.get("horizons", {}).get("position", {}).get("ttl_days"),
                "market_score_threshold": settings_payload.get("horizons", {}).get("position", {}).get("market_score_threshold"),
                "priority_threshold": settings_payload.get("horizons", {}).get("position", {}).get("priority_threshold"),
                "rsi_floor": settings_payload.get("horizons", {}).get("position", {}).get("rsi_floor"),
                "rsi_ceiling": settings_payload.get("horizons", {}).get("position", {}).get("rsi_ceiling"),
                "atr_percent_ceiling": settings_payload.get("horizons", {}).get("position", {}).get("atr_percent_ceiling"),
            },
        },
    }
    next_payload["promotion_meta"] = {
        "source_manifest_path": manifest_payload.get("manifest_path", ""),
        "batch_id": manifest_payload.get("batch_id", ""),
        "recommended_experiment": experiment.get("name", ""),
        "recommended_config_summary": (manifest_payload.get("recommendation") or {}).get("config_summary", "-"),
    }
    return next_payload


def format_promoted_config_summary(
    *,
    manifest_path: str,
    base_config_path: str,
    comparison_source_path: str,
    output_config_path: str,
    promoted_payload: dict,
    change_summary: dict,
    backup_path: str = "",
) -> str:
    strategy = promoted_payload.get("strategy", {})
    horizons = strategy.get("horizons", {})
    swing = horizons.get("swing", {})
    position = horizons.get("position", {})
    meta = promoted_payload.get("promotion_meta", {})
    lines = [
        "推荐参数已导出：",
        f"来源 Manifest：{manifest_path}",
        f"基础配置：{base_config_path}",
        f"变更对比基线：{comparison_source_path}",
        f"输出配置：{output_config_path}",
        f"是否覆盖已有文件：{'是' if backup_path else '否'}",
        f"推荐实验：{meta.get('recommended_experiment', '-')}",
        f"参数摘要：{meta.get('recommended_config_summary', '-')}",
        f"event_score_threshold：{strategy.get('event_score_threshold', '-')}",
        f"swing：market={swing.get('market_score_threshold', '-')} priority={swing.get('priority_threshold', '-')} rsi={swing.get('rsi_floor', '-')}-{swing.get('rsi_ceiling', '-')}",
        f"position：market={position.get('market_score_threshold', '-')} priority={position.get('priority_threshold', '-')} rsi={position.get('rsi_floor', '-')}-{position.get('rsi_ceiling', '-')}",
        f"策略变化：{change_summary.get('headline', '-')}",
    ]
    if backup_path:
        lines.append(f"备份文件：{backup_path}")
    changed_fields = change_summary.get("changed_fields", [])
    if changed_fields:
        lines.append("变更明细：")
        for item in changed_fields:
            lines.append(f"  - {item['label']}：{item['before']} -> {item['after']}")
    return "\n".join(lines)


def build_daily_run_payload(
    settings: Settings,
    *,
    workspace_dir: Path,
    config_path: Path | None,
    replay_path: str,
    days: int,
    limit: int,
) -> dict:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    selected_config_path = config_path.resolve() if config_path is not None else _preferred_runtime_config_path(settings)
    resolved_replay_path = str(Path(replay_path).resolve()) if replay_path else ""
    runtime_config = AgentRuntimeConfig.load(selected_config_path)
    run_settings = settings.with_overrides(
        database_path=workspace_dir / "daily_run.db",
        config_path=selected_config_path,
    )
    service = build_service(
        run_settings,
        replay_path=resolved_replay_path,
        run_name="daily_run",
        note="日常运行",
        runtime_config=runtime_config,
        apply_runtime_config=True,
    )
    try:
        service.run_once()
        outcome_backfill = _auto_backfill_review_outcomes(service.store)
        latest_run = service.store.load_latest_run()
        if latest_run is None:
            raise RuntimeError("No run generated during daily run.")
        run_id = latest_run["run_id"]
        run_detail = serialize_run_detail(latest_run, service.store.load_logs(run_id, limit=200))
        run_detail["candidate_evaluation_summary"] = _build_candidate_evaluation_summary(
            [dict(row) for row in service.store.load_candidate_evaluations(run_id)],
            limit=limit,
        )
        strategy_report = _build_run_scoped_strategy_report(service.store, run_id=run_id, limit=limit)
        source_health = serialize_source_health(service.store.load_source_health(run_id))
        card_diagnostics = _build_card_diagnostics(
            run_detail,
            service.store.load_opportunity_cards(run_id),
            service.store.load_alert_history(run_id),
        )
        event_type_context, pool_context = _build_historical_outcome_context(
            service.store,
            days=RECENT_PERFORMANCE_WINDOW_DAYS,
            limit=limit,
        )
        decision_diagnostics = _build_decision_diagnostics(
            service.store.load_decision_records(run_id),
            event_type_context=event_type_context,
            pool_context=pool_context,
        )
        llm_usage_report = build_llm_usage_report_payload(service.store, days=LLM_USAGE_REPORT_WINDOW_DAYS)

        review_path = workspace_dir / "daily_run_review.md"
        payload_path = workspace_dir / "daily_run_payload.json"
        review_text = format_run_review(
            run_detail,
            strategy_report,
            source_health,
            card_diagnostics,
            decision_diagnostics,
            llm_usage_report,
        )
        health_summary = summarize_run_health(
            run_detail,
            strategy_report,
            source_health,
            card_diagnostics,
            decision_diagnostics,
        )
        review_payload = {
            "run": run_detail,
            "strategy_report": strategy_report,
            "source_health": source_health,
            "card_diagnostics": card_diagnostics,
            "decision_diagnostics": decision_diagnostics,
            "llm_usage_report": llm_usage_report,
            "health_summary": health_summary,
            "outcome_backfill": outcome_backfill,
        }
        _write_report(review_path, review_text)
        _write_json(payload_path, review_payload)
        historical_effect_payload = build_performance_review_payload(
            service.store,
            workspace_dir=workspace_dir,
            run_id=run_id,
            days=RECENT_PERFORMANCE_WINDOW_DAYS,
            limit=limit,
        )
        llm_usage_payload = build_write_llm_usage_report_payload(
            service.store,
            workspace_dir=workspace_dir,
            days=LLM_USAGE_REPORT_WINDOW_DAYS,
        )

        top_event = (
            strategy_report.get("event_type_performance", [{}])[0].get("event_type", "-")
            if strategy_report.get("event_type_performance")
            else "-"
        )
        summary = (run_detail or {}).get("summary", {})
        return {
            "workspace_dir": str(workspace_dir),
            "config_path": str(selected_config_path),
            "database_path": str(run_settings.database_path.resolve()),
            "replay_path": resolved_replay_path,
            "run_id": run_id,
            "status": (run_detail or {}).get("status", ""),
            "health_status": health_summary.get("status", ""),
            "events_processed": summary.get("events_processed", 0),
            "cards_generated": summary.get("cards_generated", 0),
            "alerts_sent": summary.get("alerts_sent", 0),
            "top_event": top_event,
            "outcome_backfill": outcome_backfill,
            "review_path": str(review_path),
            "historical_effect_review_path": historical_effect_payload.get("performance_review_path", ""),
            "llm_usage_report_path": llm_usage_payload.get("report_path", ""),
            "payload_path": str(payload_path),
        }
    finally:
        service.store.close()


def format_daily_run(payload: dict) -> str:
    lines = [
        "日常运行：",
        f"工作目录：{payload.get('workspace_dir', '-')}",
        f"配置文件：{payload.get('config_path', '-')}",
        f"数据库：{payload.get('database_path', '-')}",
        f"运行 ID：{payload.get('run_id', '-')}",
        f"状态：{payload.get('status', '-')}",
        f"健康判断：{payload.get('health_status', '-')}",
        f"事件数：{payload.get('events_processed', '-')}",
        f"卡片数：{payload.get('cards_generated', '-')}",
        f"提醒数：{payload.get('alerts_sent', '-')}",
        f"主事件类型：{payload.get('top_event', '-')}",
        (
            "后验回补："
                f"最近 {((payload.get('outcome_backfill') or {}).get('days', '-'))} 天，"
                f"扫描 {((payload.get('outcome_backfill') or {}).get('scanned', 0))} / "
                f"更新 {((payload.get('outcome_backfill') or {}).get('updated', 0))} / "
                f"跳过 {((payload.get('outcome_backfill') or {}).get('skipped', 0))} / "
                f"补 bars {((payload.get('outcome_backfill') or {}).get('fetched_symbols', 0))}"
            ),
        f"运行过程复盘：{payload.get('review_path', '-')}",
        f"历史效果复盘：{payload.get('historical_effect_review_path', '-')}",
        f"LLM 用量报告：{payload.get('llm_usage_report_path', '-')}",
        f"结构化数据：{payload.get('payload_path', '-')}",
    ]
    if payload.get("replay_path"):
        lines.append(f"Replay 输入：{payload.get('replay_path')}")
    return "\n".join(lines)


def build_performance_review_payload(
    store: Store,
    *,
    workspace_dir: Path,
    run_id: str,
    days: int,
    limit: int,
    start_date: str = "",
    end_date: str = "",
    month: str = "",
) -> dict:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    historical_effect_dir = workspace_dir / "historical_effect"
    historical_effect_dir.mkdir(parents=True, exist_ok=True)

    selected_run_id = run_id
    run_row = store.load_run(run_id) if run_id else store.load_latest_run()
    if run_row is not None:
        selected_run_id = str(run_row["run_id"])

    run_detail = None
    strategy_report: dict[str, Any] = serialize_strategy_report([], [], [], [], [], {})
    source_health: list[dict[str, Any]] = []
    card_diagnostics: list[dict[str, Any]] = []
    decision_diagnostics: list[dict[str, Any]] = []
    health_summary = {"status": "-", "line_items": []}

    if run_row is not None and selected_run_id:
        run_detail = serialize_run_detail(run_row, store.load_logs(selected_run_id, limit=200))
        run_detail["candidate_evaluation_summary"] = _build_candidate_evaluation_summary(
            [dict(row) for row in store.load_candidate_evaluations(selected_run_id)],
            limit=limit,
        )
        strategy_report = _build_run_scoped_strategy_report(store, run_id=selected_run_id, limit=limit)
        source_health = serialize_source_health(store.load_source_health(selected_run_id))
        card_diagnostics = _build_card_diagnostics(
            run_detail,
            store.load_opportunity_cards(selected_run_id),
            store.load_alert_history(selected_run_id),
        )
        event_type_context, pool_context = _build_historical_outcome_context(
            store,
            days=days,
            limit=limit,
        )
        decision_diagnostics = _build_decision_diagnostics(
            store.load_decision_records(selected_run_id),
            event_type_context=event_type_context,
            pool_context=pool_context,
        )
        health_summary = summarize_run_health(
            run_detail,
            strategy_report,
            source_health,
            card_diagnostics,
            decision_diagnostics,
        )
    month_token = str(month or "").strip()
    if month_token and (str(start_date or "").strip() or str(end_date or "").strip()):
        raise SystemExit("month cannot be combined with start-date/end-date.")
    if month_token and int(days) != RECENT_PERFORMANCE_WINDOW_DAYS:
        raise SystemExit("month cannot be combined with custom days. Use --month YYYY-MM only.")

    requested_window = (
        _resolve_month_review_window(month_token)
        if month_token
        else _resolve_review_window(days=days, start_date=start_date, end_date=end_date)
    )
    output_paths = _resolve_historical_effect_output_paths(
        workspace_dir=workspace_dir,
        window=requested_window,
        days=days,
        start_date=start_date,
        end_date=end_date,
        month=month_token,
    )

    def _write_review_bundle(*, window: dict[str, Any], target_paths: dict[str, Any]) -> dict[str, Any]:
        review_start = str(window.get("start_date") or "")
        review_end = str(window.get("end_date") or "")
        review_days = int(window.get("window_days") or 0)
        historical_effect_review = _build_historical_effect_review_data(
            store,
            days=review_days,
            limit=limit,
            start_date=review_start,
            end_date=review_end,
            include_comparison=target_paths["review_scope"] == "rolling",
        )
        historical_effect_review["review_scope"] = target_paths["review_scope"]
        historical_effect_review["review_scope_label"] = target_paths["review_scope_label"]
        historical_effect_review["review_slug"] = target_paths["review_slug"]
        historical_effect_review["last_generated_at"] = utcnow().astimezone(BEIJING_TZ).isoformat()
        performance_review_text = format_recent_performance_review(historical_effect_review)
        sample_audit_payload = build_outcome_sample_payload(
            store,
            days=review_days,
            limit=OUTCOME_SAMPLE_AUDIT_LIMIT,
            start_date=review_start,
            end_date=review_end,
        )
        sample_audit_text = format_outcome_sample_payload(sample_audit_payload)
        payload = {
            "workspace_dir": str(workspace_dir),
            "database_path": str(store.database_path.resolve()),
            "run_id": selected_run_id,
            "window_days": historical_effect_review["review_window"]["window_days"],
            "limit": limit,
            "review_scope": target_paths["review_scope"],
            "review_scope_label": target_paths["review_scope_label"],
            "review_slug": target_paths["review_slug"],
            "output_dir": str(target_paths["output_dir"]),
            "performance_review_path": str(target_paths["performance_review_path"]),
            "payload_path": str(target_paths["payload_path"]),
            "sample_audit_path": str(target_paths["sample_audit_path"]),
            "sample_audit_payload_path": str(target_paths["sample_audit_payload_path"]),
            "historical_effect_review": historical_effect_review,
            "sample_audit": sample_audit_payload,
            "current_run": run_detail,
            "current_strategy_report": strategy_report,
            "health_summary": health_summary,
        }
        _write_report(target_paths["performance_review_path"], performance_review_text)
        _write_json(target_paths["payload_path"], payload)
        _write_report(target_paths["sample_audit_path"], sample_audit_text)
        _write_json(target_paths["sample_audit_payload_path"], sample_audit_payload)
        return payload

    payload = _write_review_bundle(window=requested_window, target_paths=output_paths)
    monthly_review_outputs: list[dict[str, Any]] = []
    if payload["review_scope"] == "rolling":
        reference_day = _parse_local_date(payload["historical_effect_review"]["review_window"]["end_date"])
        for month_token_item in _iter_recent_month_tokens(reference_day=reference_day, count=ACTIVE_MONTHLY_REVIEW_MONTHS):
            month_window = _resolve_month_review_window(month_token_item)
            month_paths = _resolve_historical_effect_output_paths(
                workspace_dir=workspace_dir,
                window=month_window,
                days=month_window["window_days"],
                month=month_token_item,
            )
            month_payload = _write_review_bundle(window=month_window, target_paths=month_paths)
            monthly_review_outputs.append(
                {
                    "month": month_token_item,
                    "performance_review_path": month_payload["performance_review_path"],
                    "payload_path": month_payload["payload_path"],
                    "sample_audit_path": month_payload["sample_audit_path"],
                }
            )
    payload["monthly_review_outputs"] = monthly_review_outputs
    _write_json(Path(payload["payload_path"]), payload)
    return payload


def format_performance_review_result(payload: dict) -> str:
    review = payload.get("historical_effect_review") or {}
    window = review.get("review_window") or {}
    lines = [
        "历史效果复盘：",
        f"工作目录：{payload.get('workspace_dir', '-')}",
        f"数据库：{payload.get('database_path', '-')}",
        f"参考运行：{payload.get('run_id', '-') or '-'}",
        f"输出类型：{payload.get('review_scope_label', '-')}",
        f"状态：{review.get('status', '-')}",
        f"复盘口径版本：{review.get('review_version', '-')}",
        f"统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}",
        f"样本抽检：{payload.get('sample_audit_path', '-')}",
        f"报告文件：{payload.get('performance_review_path', '-')}",
        f"结构化数据：{payload.get('payload_path', '-')}",
    ]
    monthly_outputs = payload.get("monthly_review_outputs") or []
    if monthly_outputs:
        lines.append(f"同步月报：{len(monthly_outputs)} 份")
        for item in monthly_outputs:
            lines.append(f"  - {item.get('month', '-')}: {item.get('performance_review_path', '-')}")
    return "\n".join(lines)


def _should_refresh_historical_effect_review(*, workspace_dir: Path, min_interval_seconds: int) -> bool:
    if min_interval_seconds <= 0:
        return True
    review_path = workspace_dir / "historical_effect" / "review.md"
    if not review_path.exists():
        return True
    modified_at = datetime.fromtimestamp(review_path.stat().st_mtime, tz=timezone.utc)
    return (utcnow() - modified_at).total_seconds() >= float(min_interval_seconds)


def _should_refresh_llm_usage_report(*, workspace_dir: Path, min_interval_seconds: int) -> bool:
    if min_interval_seconds <= 0:
        return True
    report_path = workspace_dir / "llm_usage" / "report.md"
    if not report_path.exists():
        return True
    modified_at = datetime.fromtimestamp(report_path.stat().st_mtime, tz=timezone.utc)
    return (utcnow() - modified_at).total_seconds() >= float(min_interval_seconds)


def write_live_run_artifacts(
    store: Store,
    *,
    run_id: str,
    workspace_dir: Path,
    limit: int,
    review_filename: str,
    payload_filename: str,
    historical_effect_min_interval_seconds: int = 0,
    llm_usage_min_interval_seconds: int = 0,
) -> dict:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    outcome_backfill = _auto_backfill_review_outcomes(store)
    payload = build_replay_evaluation_payload(store, run_id=run_id, days=RECENT_PERFORMANCE_WINDOW_DAYS, limit=limit)
    review_path = workspace_dir / review_filename
    payload_path = workspace_dir / payload_filename
    review_text = format_run_review(
        payload["run"],
        payload["strategy_report"],
        payload["source_health"],
        payload["card_diagnostics"],
        payload.get("decision_diagnostics", []),
        build_llm_usage_report_payload(store, days=LLM_USAGE_REPORT_WINDOW_DAYS),
    )
    _write_report(review_path, review_text)
    _write_json(payload_path, payload)
    connectivity_issues = summarize_external_connectivity_issues(payload["source_health"])
    should_refresh_historical_effect = _should_refresh_historical_effect_review(
        workspace_dir=workspace_dir,
        min_interval_seconds=historical_effect_min_interval_seconds,
    )
    historical_effect_review_path = str((workspace_dir / "historical_effect" / "review.md").resolve())
    if should_refresh_historical_effect:
        historical_effect_payload = build_performance_review_payload(
            store,
            workspace_dir=workspace_dir,
            run_id=run_id,
            days=RECENT_PERFORMANCE_WINDOW_DAYS,
            limit=limit,
        )
        historical_effect_review_path = historical_effect_payload.get("performance_review_path", historical_effect_review_path)
    should_refresh_llm_usage = _should_refresh_llm_usage_report(
        workspace_dir=workspace_dir,
        min_interval_seconds=llm_usage_min_interval_seconds,
    )
    llm_usage_report_path = str((workspace_dir / "llm_usage" / "report.md").resolve())
    if should_refresh_llm_usage:
        llm_usage_payload = build_write_llm_usage_report_payload(
            store,
            workspace_dir=workspace_dir,
            days=LLM_USAGE_REPORT_WINDOW_DAYS,
        )
        llm_usage_report_path = llm_usage_payload.get("report_path", llm_usage_report_path)
    return {
        "run_id": run_id,
        "outcome_backfill": outcome_backfill,
        "review_path": str(review_path),
        "historical_effect_review_path": historical_effect_review_path,
        "historical_effect_review_refreshed": should_refresh_historical_effect,
        "historical_effect_monthly_outputs": (
            historical_effect_payload.get("monthly_review_outputs", []) if should_refresh_historical_effect else []
        ),
        "llm_usage_report_path": llm_usage_report_path,
        "llm_usage_report_refreshed": should_refresh_llm_usage,
        "payload_path": str(payload_path),
        "external_connectivity_issues": connectivity_issues,
    }


def format_live_run_artifacts(payload: dict) -> str:
    historical_effect_line = f"历史效果复盘：{payload.get('historical_effect_review_path', '-')}"
    if not payload.get("historical_effect_review_refreshed", True):
        historical_effect_line += "（本轮沿用上次刷新）"
    monthly_outputs = payload.get("historical_effect_monthly_outputs") or []
    llm_usage_line = f"LLM 用量报告：{payload.get('llm_usage_report_path', '-')}"
    if not payload.get("llm_usage_report_refreshed", True):
        llm_usage_line += "（本轮沿用上次刷新）"
    lines = [
        "实时运行结果已落盘：",
        f"运行 ID：{payload.get('run_id', '-')}",
        (
            "后验回补："
            f"最近 {((payload.get('outcome_backfill') or {}).get('days', '-'))} 天，"
            f"扫描 {((payload.get('outcome_backfill') or {}).get('scanned', 0))} / "
            f"更新 {((payload.get('outcome_backfill') or {}).get('updated', 0))} / "
            f"跳过 {((payload.get('outcome_backfill') or {}).get('skipped', 0))} / "
            f"补 bars {((payload.get('outcome_backfill') or {}).get('fetched_symbols', 0))}"
        ),
        f"运行过程复盘：{payload.get('review_path', '-')}",
        historical_effect_line,
    ]
    connectivity_issues = payload.get("external_connectivity_issues") or {}
    if connectivity_issues.get("has_issue"):
        sources = "、".join(connectivity_issues.get("sources") or []) or "外部数据源"
        lines.append(f"外网提醒：检测到 {sources} 连通性异常，请先检查 VPN/当前网络。")
    if monthly_outputs:
        lines.append(f"同步月报：{len(monthly_outputs)} 份")
        for item in monthly_outputs:
            lines.append(f"  - {item.get('month', '-')}: {item.get('performance_review_path', '-')}")
    lines.extend(
        [
            llm_usage_line,
            f"结构化数据：{payload.get('payload_path', '-')}",
        ]
    )
    return "\n".join(lines)


def format_live_cycle_started(*, started_at, workspace_dir: Path) -> str:
    return "\n".join(
        [
            "实时监控轮次开始：",
            f"开始时间：{format_beijing_minute(started_at)}",
            f"结果目录：{workspace_dir.resolve()}",
        ]
    )


def format_live_cycle_finished(
    *,
    started_at,
    finished_at,
    poll_seconds: int,
    run_id: str,
    summary: dict | None = None,
) -> str:
    elapsed_seconds = max((finished_at - started_at).total_seconds(), 0.0)
    next_refresh = finished_at + timedelta(seconds=poll_seconds)
    summary = summary or {}
    return "\n".join(
        [
            "实时监控轮次结束：",
            f"运行 ID：{run_id}",
            f"结束时间：{format_beijing_minute(finished_at)}",
            f"本轮耗时：{elapsed_seconds:.1f} 秒",
            (
                "本轮结果："
                f" 事件 {summary.get('events_processed', '-')}"
                f" / 卡片 {summary.get('cards_generated', '-')}"
                f" / 提醒 {summary.get('alerts_sent', '-')}"
            ),
            f"下次预计刷新：{format_beijing_minute(next_refresh)}",
        ]
    )


def format_live_cycle_paused(*, as_of, next_run_at, reason: str) -> str:
    return "\n".join(
        [
            "实时监控当前处于等待窗口：",
            f"当前时间：{format_beijing_minute(as_of)}",
            f"等待原因：{reason}",
            f"下次预计启动：{format_beijing_minute(next_run_at)}",
        ]
    )


def _runtime_window_pause(runtime_config: AgentRuntimeConfig, *, now=None) -> tuple[bool, object | None]:
    schedule = runtime_config.runtime_window
    if not schedule.is_configured():
        return False, None
    current = now or utcnow()
    if schedule.is_active_at(current):
        return False, None
    return True, schedule.next_window_start_after(current)


def _sleep_until_runtime_window(runtime_config: AgentRuntimeConfig) -> None:
    paused, next_run_at = _runtime_window_pause(runtime_config)
    if not paused or next_run_at is None:
        return
    now = utcnow()
    print(
        format_live_cycle_paused(
            as_of=now,
            next_run_at=next_run_at,
            reason="当前不在允许的运行时间窗内",
        ),
        flush=True,
    )
    sleep_seconds = max((next_run_at - now).total_seconds(), 1.0)
    time.sleep(sleep_seconds)


def _build_test_notification_card(symbol: str = "NVDA") -> OpportunityCard:
    now = utcnow()
    return OpportunityCard(
        card_id=f"test-card-{symbol.lower()}",
        event_id="test-event",
        symbol=symbol.upper(),
        horizon="swing",
        event_type="test_notification",
        headline_summary="这是一次飞书测试提醒，用于确认手机端消息链路是否正常。",
        bull_case="仅用于验证消息推送链路，不代表真实投资建议。",
        bear_case="仅用于验证消息推送链路，不代表真实投资建议。",
        event_score=88.0,
        market_score=72.0,
        final_score=81.6,
        entry_range=PriceRange(100.0, 102.0),
        take_profit_range=PriceRange(106.0, 110.0),
        invalidation_level=97.5,
        invalidation_reason="测试提醒，无真实失效条件。",
        risk_notes=["测试消息", "请确认飞书机器人关键词与权限已配置完成"],
        source_refs=["https://open.feishu.cn/"],
        created_at=now,
        ttl=now + timedelta(days=3),
        priority="high",
        dedup_key=f"test:{symbol.upper()}:feishu",
        bias="long",
        display_name=symbol.upper(),
        action_label="确认做多",
        confidence_label="高",
        confidence_score=88.0,
        reason_to_watch="如果你能在手机飞书里看到这张卡片，说明提醒链路已可用于真实监控。",
        trend_state="bullish",
        rsi_14=58.4,
        relative_volume=1.62,
        theme_tags=["测试链路"],
    )


def _preview_display_name(runtime_config: AgentRuntimeConfig, symbol: str) -> str:
    name = runtime_config.watchlist.display_name_for(symbol)
    return name or symbol.upper()


def _preview_theme_tags(runtime_config: AgentRuntimeConfig, symbol: str) -> list[str]:
    theme_map = runtime_config.watchlist.symbol_theme_map()
    theme_display_map = runtime_config.watchlist.theme_display_name_map()
    return [
        theme_display_map.get(theme_id, theme_id)
        for theme_id in theme_map.get(symbol.upper(), [])
    ]


def _build_preview_notification_card(
    runtime_config: AgentRuntimeConfig,
    *,
    symbol: str = "NVDA",
    watch_mode: bool = False,
    degraded_formal_mode: bool = False,
) -> OpportunityCard:
    now = utcnow()
    normalized_symbol = symbol.upper()
    display_name = _preview_display_name(runtime_config, normalized_symbol)
    theme_tags = _preview_theme_tags(runtime_config, normalized_symbol)
    if watch_mode:
        return OpportunityCard(
            card_id=f"preview-watch:{normalized_symbol}",
            event_id=f"preview-watch-event:{normalized_symbol}",
            symbol=normalized_symbol,
            horizon="position",
            event_type="strategic",
            headline_summary="模拟预备池卡片，用于预览当前文案与展示层效果。",
            bull_case="若题材发酵持续、价格结构继续转强，后续有机会升级为正式操作卡。",
            bear_case="若量能衰减或结构转弱，观察价值会快速下降。",
            event_score=74.0,
            market_score=61.0,
            final_score=67.2,
            entry_range=PriceRange(100.0, 102.0),
            take_profit_range=PriceRange(108.0, 112.0),
            invalidation_level=97.0,
            invalidation_reason="模拟卡片，不作为真实交易依据。",
            risk_notes=["模拟预览用卡片", "正式执行前需结合真实行情与事件确认"],
            source_refs=["https://example.com/preview-alert"],
            created_at=now,
            ttl=now + timedelta(days=10),
            priority="suppressed",
            dedup_key=f"preview-watch:{normalized_symbol}",
            bias="long",
            display_name=display_name,
            action_label="加入观察",
            confidence_label="中",
            confidence_score=64.0,
            reason_to_watch="先盯合作细节、订单金额和时间表是否继续落地，再决定是否升级。",
            trend_state="neutral",
            rsi_14=54.2,
            relative_volume=1.18,
            theme_tags=theme_tags,
            chain_summary="2天前加入观察 -> 今日继续跟踪",
            market_regime="neutral",
            rate_risk="medium",
            geopolitical_risk="low",
            macro_risk_score=35.0,
            promoted_from_prewatch=True,
            prewatch_score=78.0,
            prewatch_setup_type="pullback_watch",
            positioning_hint="当前先放入观察名单，不追价，等结构和催化进一步确认后再升级。",
        )
    if degraded_formal_mode:
        return OpportunityCard(
            card_id=f"preview-formal-degraded:{normalized_symbol}",
            event_id=f"preview-formal-degraded-event:{normalized_symbol}",
            symbol=normalized_symbol,
            horizon="swing",
            event_type="earnings",
            headline_summary="模拟正式信号降级场景，用于预览执行信号被自动降级后的展示效果。",
            bull_case="若盘后关键数据继续超预期，且次日放量脱离震荡区，后续仍可能重新升级为正式机会。",
            bear_case="若结构继续横盘且预期盈亏比不足，贸然按正式卡执行更容易陷入低质量交易。",
            event_score=84.0,
            market_score=66.0,
            final_score=77.8,
            entry_range=PriceRange(199.85, 204.28),
            take_profit_range=PriceRange(207.50, 211.40),
            invalidation_level=194.08,
            invalidation_reason="若价格跌破事件触发后的关键支撑位，做多逻辑失效。",
            risk_notes=["财报后波动往往放大，过紧止损容易被噪音触发。"],
            source_refs=[
                "https://www.sec.gov/ixviewer/doc",
                "https://www.reuters.com/world/us/example",
                "https://news.google.com/articles/preview-amd",
            ],
            created_at=now,
            ttl=now + timedelta(days=4),
            priority="high",
            dedup_key=f"preview-formal-degraded:{normalized_symbol}",
            bias="long",
            display_name=display_name,
            action_label="确认做多",
            confidence_label="高",
            confidence_score=82.0,
            reason_to_watch="事件强度已经够，但当前价格结构和预期盈亏比还不够理想，适合先看次日量价确认。",
            trend_state="neutral",
            rsi_14=49.6,
            relative_volume=0.26,
            theme_tags=theme_tags,
            chain_summary="今日试探建仓 -> 今日确认做多",
            market_regime="neutral",
            rate_risk="medium",
            geopolitical_risk="low",
            macro_risk_score=40.0,
            positioning_hint="先盯次日量价是否转强，再决定是否重新升级为正式机会。",
            execution_eligible=False,
            execution_note="按当前入场区、止盈区和失效价估算，预期盈亏比不足，先降级为观察。",
            llm_summary="AMD 财报事件本身较强，但当前仍处在震荡区，且量能明显不足。",
            llm_impact_inference="若次日放量脱离震荡区，才更像可重新升级的正式机会。",
            llm_reasoning="财报催化够强，但当前预期盈亏比不足，先观察比直接执行更稳。",
            llm_uncertainty="若管理层指引偏保守，股价可能快速回落。",
        )
    return OpportunityCard(
        card_id=f"preview-formal:{normalized_symbol}",
        event_id=f"preview-formal-event:{normalized_symbol}",
        symbol=normalized_symbol,
        horizon="swing",
        event_type="strategic" if normalized_symbol == "PLTR" else "strategic",
        headline_summary=(
            "PLTR 扩大战略合作与商业化落地，适合预览更接近真实正式卡的文案结构。"
            if normalized_symbol == "PLTR"
            else "模拟正式卡片，用于预览当前 LLM 文案和完整通知渲染效果。"
        ),
        bull_case=(
            "若合作规模与兑现路径继续明朗，软件与大模型应用叙事更容易获得资金继续跟随。"
            if normalized_symbol == "PLTR"
            else "若事件兑现顺利，叙事会继续强化，短线资金更容易沿主线加速交易。"
        ),
        bear_case=(
            "若合作细节长期模糊，市场可能把这轮上涨当成预期交易并快速回吐。"
            if normalized_symbol == "PLTR"
            else "若预期兑现不足或量能回落，强势信号容易迅速降温。"
        ),
        event_score=81.95 if normalized_symbol == "PLTR" else 82.0,
        market_score=70.75 if normalized_symbol == "PLTR" else 76.0,
        final_score=77.47 if normalized_symbol == "PLTR" else 79.6,
        entry_range=PriceRange(150.67, 155.20) if normalized_symbol == "PLTR" else PriceRange(100.0, 102.0),
        take_profit_range=PriceRange(178.65, 192.72) if normalized_symbol == "PLTR" else PriceRange(108.0, 112.0),
        invalidation_level=145.82 if normalized_symbol == "PLTR" else 97.0,
        invalidation_reason=(
            "若价格跌破事件触发后的关键支撑位，做多逻辑失效。"
            if normalized_symbol == "PLTR"
            else "模拟卡片，不作为真实交易依据。"
        ),
        risk_notes=(
            ["若后续无法披露具体合作细节，市场可能因预期落空而抛售。"]
            if normalized_symbol == "PLTR"
            else ["模拟预览用卡片", "正式执行前需结合真实行情与事件确认"]
        ),
        source_refs=(
            [
                "https://www.reuters.com/world/us/example-pltr",
                "https://news.google.com/articles/preview-pltr",
            ]
            if normalized_symbol == "PLTR"
            else ["https://example.com/preview-alert"]
        ),
        created_at=now,
        ttl=now + timedelta(days=5),
        priority="high",
        dedup_key=f"preview-formal:{normalized_symbol}",
        bias="long",
        display_name=display_name,
        action_label="确认做多",
        confidence_label="高",
        confidence_score=82.0 if normalized_symbol == "PLTR" else 84.0,
        reason_to_watch=(
            "合作规模与兑现路径已经基本过线，当前更适合按价格计划执行，而不是继续停留在观察层。"
            if normalized_symbol == "PLTR"
            else "如果当前文案读起来足够清楚，说明 Qwen 生成链路已经适合继续上线观察。"
        ),
        trend_state="neutral" if normalized_symbol == "PLTR" else "bullish",
        rsi_14=53.3 if normalized_symbol == "PLTR" else 61.5,
        relative_volume=0.52 if normalized_symbol == "PLTR" else 1.72,
        theme_tags=theme_tags or (["AI软件与大模型应用"] if normalized_symbol == "PLTR" else theme_tags),
        chain_summary="今日试探建仓 -> 今日确认做多" if normalized_symbol == "PLTR" else "昨晚试探建仓 -> 今日升级确认做多",
        market_regime="neutral" if normalized_symbol == "PLTR" else "risk_on",
        rate_risk="medium",
        geopolitical_risk="low",
        macro_risk_score=35.0 if normalized_symbol == "PLTR" else 25.0,
        positioning_hint=(
            "当前更适合按价格计划执行，不适合因为题材热度直接脱离入场区追高。"
            if normalized_symbol == "PLTR"
            else "适合按价格计划跟随，不适合追高扩仓。"
        ),
        llm_reasoning=(
            "合作规模与兑现路径已基本过线，这里更像可执行机会而不是继续观察。"
            if normalized_symbol == "PLTR"
            else "事件与市场确认共振，当前可按计划执行。"
        ),
        llm_uncertainty=(
            "若合作细节迟迟不清，市场可能先卖预期。"
            if normalized_symbol == "PLTR"
            else "若量能衰减，强势信号可能迅速降温。"
        ),
        llm_summary=(
            "PLTR 的战略合作与商业化兑现路径正在变清楚，当前更接近正式机会。"
            if normalized_symbol == "PLTR"
            else ""
        ),
        llm_impact_inference=(
            "若合作规模和兑现节奏继续明朗，软件与大模型应用主线更容易获得资金继续跟随。"
            if normalized_symbol == "PLTR"
            else ""
        ),
    )


def _build_preview_exit_pool_card(
    runtime_config: AgentRuntimeConfig,
    *,
    symbol: str = "NVDA",
) -> OpportunityCard:
    now = utcnow()
    normalized_symbol = symbol.upper()
    display_name = _preview_display_name(runtime_config, normalized_symbol)
    theme_tags = _preview_theme_tags(runtime_config, normalized_symbol)
    return OpportunityCard(
        card_id=f"preview-exit:{normalized_symbol}",
        event_id=f"preview-exit-event:{normalized_symbol}",
        symbol=normalized_symbol,
        horizon="position",
        event_type="strategic",
        headline_summary="模拟兑现池卡片，用于预览从确认到兑现的完整提示效果。",
        bull_case="前期进攻逻辑已兑现一部分，当前重点不再是找新买点，而是管好已有利润。",
        bear_case="若继续把兑现卡当成新开仓信号，容易在高位追入或回吐已有浮盈。",
        event_score=79.0,
        market_score=73.0,
        final_score=81.0,
        entry_range=PriceRange(100.0, 102.0),
        take_profit_range=PriceRange(110.0, 116.0),
        invalidation_level=97.0,
        invalidation_reason="模拟卡片，不作为真实交易依据。",
        risk_notes=["模拟预览用卡片", "兑现池卡不代表新的开仓信号"],
        source_refs=["https://example.com/preview-alert"],
        created_at=now,
        ttl=now + timedelta(days=2),
        priority="high",
        dedup_key=f"preview-exit:{normalized_symbol}",
        bias="long",
        display_name=display_name,
        action_label="进入兑现池",
        confidence_label="高",
        confidence_score=86.0,
        reason_to_watch="价格已进入目标区更深位置，当前更适合按计划兑现利润，而不是继续当作进攻卡处理。",
        trend_state="bullish",
        rsi_14=67.3,
        relative_volume=1.46,
        theme_tags=theme_tags,
        chain_summary="3天前确认做多 -> 今日进入兑现池",
        market_regime="neutral",
        rate_risk="medium",
        geopolitical_risk="low",
        macro_risk_score=42.0,
        positioning_hint="已到达目标区更深位置，优先兑现利润，不再继续追新仓。",
        execution_eligible=False,
        execution_note="兑现池卡只面向已有仓位管理。",
        exit_pool_subreason="target_hit",
        exit_pool_source_decision_id="preview-confirm-decision",
    )


def _build_preview_event_insight(symbol: str) -> EventInsight:
    now = utcnow()
    normalized_symbol = symbol.upper()
    return EventInsight(
        event_id=f"preview-insight:{normalized_symbol}",
        symbol=normalized_symbol,
        event_type="earnings" if normalized_symbol == "AMD" else "strategic",
        headline_summary=(
            "AMD 发布财报并给出后续指引，市场重点会盯核心业务与管理层表态。"
            if normalized_symbol == "AMD"
            else "公司宣布扩大与 AI 基建相关的合作与投入。"
        ),
        bull_case=(
            "若数据中心、AI 相关指引继续强化，市场更容易把这次财报当成中短期催化。"
            if normalized_symbol == "AMD"
            else "合作落地会强化市场对后续订单与资本开支扩张的预期。"
        ),
        bear_case=(
            "若管理层指引偏保守或盈利兑现不及预期，股价可能快速回落。"
            if normalized_symbol == "AMD"
            else "若合作更多停留在叙事层，市场可能快速回吐短线溢价。"
        ),
        importance=86.0 if normalized_symbol == "AMD" else 84.0,
        source_credibility=86.0,
        novelty=72.0,
        sentiment=0.70,
        theme_relevance=90.0,
        llm_confidence=80.0,
        risk_notes=(
            ["若次日仍无法放量脱离震荡区，强事件也可能先变成预期盈亏比不足的观察样本。"]
            if normalized_symbol == "AMD"
            else ["兑现节奏低于预期是主要失败点。"]
        ),
        source_refs=(
            [
                "https://www.sec.gov/ixviewer/doc",
                "https://www.reuters.com/world/us/example-amd",
            ]
            if normalized_symbol == "AMD"
            else ["https://example.com/preview-alert"]
        ),
        raw_payload={"mode": "preview"},
        created_at=now,
    )


def build_preview_alert_payload(
    settings: Settings,
    runtime_config: AgentRuntimeConfig,
    store: Store,
    *,
    symbol: str = "NVDA",
    watch_mode: bool = False,
    prewatch_light: bool = False,
    exit_pool_mode: bool = False,
    degraded_formal_mode: bool = False,
) -> dict[str, Any]:
    llm_used = False

    def _merge_preview_narrative(base_card: OpportunityCard, narrative) -> OpportunityCard:
        return OpportunityCard(
            **{
                **base_card.__dict__,
                "llm_summary": narrative.summary or base_card.llm_summary,
                "llm_impact_inference": narrative.impact_inference or base_card.llm_impact_inference,
                "llm_reasoning": narrative.reasoning or base_card.llm_reasoning,
                "llm_uncertainty": narrative.uncertainty or base_card.llm_uncertainty,
                "narrative_priority_adjustment": (
                    narrative.priority_adjustment
                    if any(
                        [
                            narrative.summary,
                            narrative.impact_inference,
                            narrative.reasoning,
                            narrative.uncertainty,
                        ]
                    )
                    else base_card.narrative_priority_adjustment
                ),
            }
        )

    if prewatch_light:
        service = build_service(
            settings,
            runtime_config=runtime_config,
            apply_runtime_config=False,
        )
        candidate = PrewatchCandidate(
            symbol=symbol.upper(),
            horizon="position",
            setup_type="pullback_watch",
            score=78.0,
            headline_summary="模拟观察提醒，用于预览当前提醒正文与 LLM 文案效果。",
            action_hint="轻仓观察",
            reason_to_watch="先盯合作细节、订单金额和时间表是否继续落地，再决定是否升级。",
            last_price=100.0,
            rsi_14=54.2,
            relative_volume=1.18,
            trend_state="neutral",
            support_20=97.0,
            resistance_20=103.0,
            trigger_mode="event",
            trigger_event_type="strategic",
            as_of=utcnow(),
        )
        card = service._build_prewatch_notification_card(  # type: ignore[attr-defined]
            candidate,
            macro_context={
                "market_regime": "neutral",
                "rate_risk": "medium",
                "geopolitical_risk": "low",
                "macro_risk_score": 35.0,
            },
            run_id="preview-prewatch-light",
        )
        llm_used = any(
            [
                card.llm_summary,
                card.llm_impact_inference,
                card.llm_reasoning,
                card.llm_uncertainty,
            ]
        )
    elif exit_pool_mode:
        card = _build_preview_exit_pool_card(runtime_config, symbol=symbol)
        insight = _build_preview_event_insight(card.symbol)
        if settings.openai_api_key and settings.use_llm_narration:
            narrator = OpenAINarrator(
                api_key=settings.openai_api_key,
                model=settings.openai_model,
                base_url=settings.openai_base_url,
            )
            narrative = narrator.narrate(
                insight=insight,
                card=card,
                market_regime=card.market_regime or "neutral",
                rate_risk=card.rate_risk or "low",
                geopolitical_risk=card.geopolitical_risk or "low",
                theme_text=" / ".join(card.theme_tags) if card.theme_tags else "未标注",
                chain_summary=card.chain_summary or "首次出现",
            )
            card = _merge_preview_narrative(card, narrative)
            llm_used = any(
                [
                    narrative.summary,
                    narrative.impact_inference,
                    narrative.reasoning,
                    narrative.uncertainty,
                ]
            )
    else:
        card = _build_preview_notification_card(
            runtime_config,
            symbol=symbol,
            watch_mode=watch_mode,
            degraded_formal_mode=degraded_formal_mode,
        )
        insight = _build_preview_event_insight(card.symbol)
        if settings.openai_api_key and settings.use_llm_narration:
            narrator = OpenAINarrator(
                api_key=settings.openai_api_key,
                model=settings.openai_model,
                base_url=settings.openai_base_url,
            )
            narrative = narrator.narrate(
                insight=insight,
                card=card,
                market_regime=card.market_regime or "neutral",
                rate_risk=card.rate_risk or "low",
                geopolitical_risk=card.geopolitical_risk or "low",
                theme_text=" / ".join(card.theme_tags) if card.theme_tags else "未标注",
                chain_summary=card.chain_summary or "首次出现",
            )
            card = _merge_preview_narrative(card, narrative)
            llm_used = any(
                [
                    narrative.summary,
                    narrative.impact_inference,
                    narrative.reasoning,
                    narrative.uncertainty,
                ]
            )
    notifier = Notifier(store=store, transport=None, dry_run=True)
    transport = FeishuTransport("https://example.com/preview-webhook")
    render_view = build_render_view(card)
    return {
        "symbol": card.symbol,
        "watch_mode": watch_mode,
        "prewatch_light": prewatch_light,
        "exit_pool_mode": exit_pool_mode,
        "degraded_formal_mode": degraded_formal_mode,
        "llm_enabled": bool(settings.openai_api_key and settings.use_llm_narration),
        "llm_used": llm_used,
        "title": f"[预备池] {notifier._title(card)}" if prewatch_light else notifier._title(card),
        "body": notifier._body(card),
        "delivery_view": build_delivery_view_from_record(card.to_record()),
        "render_view": {
            "card_type": render_view["card_type"],
            "action_label": render_view["action_label"],
            "downgraded_to_watch": render_view["downgraded_to_watch"],
            "downgrade_reason": render_view["downgrade_reason"],
            "render_warning": render_view["render_warning"],
            "chain_summary": render_view["chain_summary"],
        },
        "feishu_card": transport._build_interactive_payload(card),
    }


def _preview_mode_metadata(
    *,
    watch_mode: bool = False,
    prewatch_light: bool = False,
    exit_pool_mode: bool = False,
    degraded_formal_mode: bool = False,
) -> dict[str, str]:
    if prewatch_light:
        return {
            "mode_label": "观察提醒",
            "use_case": "预备池分数已经够高，但还没到正式执行阶段，适合用低打扰方式提醒你先盯住。",
            "not_for": "不适合拿来直接开仓，也不适合替代完整观察卡做深度判断。",
            "operator_hint": "先看催化是否继续落地，再决定是否升级成确认机会。",
        }
    if exit_pool_mode:
        return {
            "mode_label": "兑现池管理卡",
            "use_case": "已有浮盈仓位进入止盈或利润保护阶段，适合用来管理仓位和兑现节奏。",
            "not_for": "不适合当成新的开仓信号，也不适合在没有持仓时单独使用。",
            "operator_hint": "优先结合原目标区和当前强弱，决定分批止盈还是先锁定大部分利润。",
        }
    if degraded_formal_mode:
        return {
            "mode_label": "自动降级观察卡",
            "use_case": "事件强度已接近正式卡，但因为盈亏比、结构或量能条件不够，被系统自动降级为观察卡。",
            "not_for": "不适合把高事件分误当成可直接执行的正式机会，也不适合忽略降级原因盲目追单。",
            "operator_hint": "重点看降级原因和次日量价是否改善，只有条件补齐后再重新升级为正式机会。",
        }
    if watch_mode:
        return {
            "mode_label": "预备池观察卡",
            "use_case": "事件或结构开始有苗头，但确认度还不够，适合先纳入观察名单。",
            "not_for": "不适合把这张卡直接当成执行依据，也不适合在信号尚弱时追价。",
            "operator_hint": "重点盯后续催化、量能和结构确认，满足条件后再升级为正式操作卡。",
        }
    return {
        "mode_label": "正式操作卡",
        "use_case": "事件强度和市场确认已经基本过线，适合结合价格计划做执行判断。",
        "not_for": "不适合脱离价格计划盲目追高，也不适合忽略失效位单独看多。",
        "operator_hint": "把它当成可执行机会，但仍要按入场区、止盈区和失效位来处理仓位。",
    }


def send_test_notification(settings: Settings, *, symbol: str = "NVDA") -> dict:
    if not settings.feishu_webhook:
        raise ValueError(
            "飞书 webhook 未配置。请在 config/satellite_agent/agent.json 的 notifications.feishu_webhook 中填写，"
            "或设置 SATELLITE_FEISHU_WEBHOOK 环境变量。"
        )
    transport = FeishuTransport(settings.feishu_webhook)
    card = _build_test_notification_card(symbol=symbol)
    transport.send_card(card)
    return {
        "status": "sent",
        "channel": "feishu",
        "symbol": card.symbol,
        "priority": card.priority,
        "headline": card.headline_summary,
    }


def format_test_notification_result(payload: dict) -> str:
    return "\n".join(
        [
            "飞书测试提醒：",
            f"状态：{payload.get('status', '-')}",
            f"通道：{payload.get('channel', '-')}",
            f"测试标的：{payload.get('symbol', '-')}",
            f"优先级：{payload.get('priority', '-')}",
            f"说明：{payload.get('headline', '-')}",
        ]
    )


def format_preview_alert_result(payload: dict) -> str:
    mode_text = _preview_mode_metadata(
        watch_mode=bool(payload.get("watch_mode")),
        prewatch_light=bool(payload.get("prewatch_light")),
        exit_pool_mode=bool(payload.get("exit_pool_mode")),
        degraded_formal_mode=bool(payload.get("degraded_formal_mode")),
    )["mode_label"]
    render_view = payload.get("render_view") or {}
    card_type = str(render_view.get("card_type") or "-")
    action_label = str(render_view.get("action_label") or "-")
    downgraded = bool(render_view.get("downgraded_to_watch"))
    downgrade_reason = str(render_view.get("downgrade_reason") or "")
    return "\n".join(
        [
            "本地预览卡片：",
            f"模式：{mode_text}",
            f"标的：{payload.get('symbol', '-')}",
            f"最终卡型：{card_type}",
            f"最终动作：{action_label}",
            f"自动降级：{'是' if downgraded else '否'}",
            *( [f"降级原因：{downgrade_reason}"] if downgrade_reason else [] ),
            f"LLM 已启用：{'是' if payload.get('llm_enabled') else '否'}",
            f"LLM 实际参与：{'是' if payload.get('llm_used') else '否'}",
            "",
            "标题：",
            str(payload.get("title", "")),
            "",
            "正文：",
            str(payload.get("body", "")),
        ]
    )


def build_demo_flow_payload(
    settings: Settings,
    runtime_config: AgentRuntimeConfig,
    *,
    workspace_dir: Path,
    replay_path: str,
    batch_spec_path: Path,
    days: int,
    limit: int,
) -> dict:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    replay_path = str(Path(replay_path).resolve())
    batch_spec_path = batch_spec_path.resolve()
    replay_report_path = workspace_dir / "demo_replay_report.md"
    replay_json_path = workspace_dir / "demo_replay_payload.json"
    preview_report_path = workspace_dir / "demo_preview_cards.md"
    preview_json_path = workspace_dir / "demo_preview_cards.json"
    batch_spec_copy_path = workspace_dir / "demo_batch_spec.json"
    batch_index_path = workspace_dir / "batch_index.md"
    batch_output_dir = workspace_dir / "batch_runs"
    promoted_config_path = workspace_dir / "agent.recommended.json"

    replay_settings = settings.with_overrides(database_path=workspace_dir / "demo_replay.db")
    replay_payload = _execute_replay_evaluation(
        replay_settings,
        runtime_config,
        replay_path=replay_path,
        days=days,
        limit=limit,
        run_name="demo_replay",
        note="初版流程联调",
    )
    replay_report = format_replay_evaluation(
        replay_payload["run"],
        replay_payload["strategy_report"],
        replay_payload["source_health"],
        replay_payload["card_diagnostics"],
        replay_payload.get("decision_diagnostics", []),
    )
    _write_report(replay_report_path, replay_report)
    _write_json(replay_json_path, replay_payload)

    with batch_spec_path.open("r", encoding="utf-8") as handle:
        batch_spec_payload = json.load(handle)
    batch_spec_payload["replay_path"] = replay_path
    _write_json(batch_spec_copy_path, batch_spec_payload)

    batch_payload = build_batch_replay_payload(
        settings,
        runtime_config,
        spec_path=batch_spec_copy_path,
        output_dir=batch_output_dir,
    )
    batch_report = format_batch_replay(batch_payload)
    _write_batch_manifest(batch_output_dir, batch_payload)
    _write_report(Path(batch_payload["report_path"]), batch_report)

    batch_index_payload = _load_batch_index(batch_output_dir, limit=10)
    batch_index_report = format_batch_index(batch_index_payload)
    _write_report(batch_index_path, batch_index_report)
    promoted_config_payload = build_promoted_config_payload(
        batch_payload,
        base_config_payload=_load_json_or_default(settings.config_path, DEFAULT_CONFIG),
    )
    _write_json(promoted_config_path, promoted_config_payload)

    preview_store = Store(workspace_dir / "demo_preview.db")
    preview_store.initialize()
    preview_specs = [
        ("formal", {"symbol": "PLTR", "watch_mode": False, "prewatch_light": False, "exit_pool_mode": False}),
        (
            "formal_downgraded",
            {"symbol": "AMD", "watch_mode": False, "prewatch_light": False, "exit_pool_mode": False, "degraded_formal_mode": True},
        ),
        ("watch", {"symbol": "NVDA", "watch_mode": True, "prewatch_light": False, "exit_pool_mode": False}),
        ("prewatch_light", {"symbol": "NBIS", "watch_mode": False, "prewatch_light": True, "exit_pool_mode": False}),
        ("exit_pool", {"symbol": "NVDA", "watch_mode": False, "prewatch_light": False, "exit_pool_mode": True}),
    ]
    preview_payloads: list[dict[str, Any]] = []
    preview_lines = ["卡片预览联调："]
    try:
        for mode_name, spec in preview_specs:
            preview_payload = build_preview_alert_payload(
                settings,
                runtime_config,
                preview_store,
                symbol=spec["symbol"],
                watch_mode=spec["watch_mode"],
                prewatch_light=spec["prewatch_light"],
                exit_pool_mode=spec["exit_pool_mode"],
                degraded_formal_mode=spec.get("degraded_formal_mode", False),
            )
            mode_metadata = _preview_mode_metadata(
                watch_mode=spec["watch_mode"],
                prewatch_light=spec["prewatch_light"],
                exit_pool_mode=spec["exit_pool_mode"],
                degraded_formal_mode=spec.get("degraded_formal_mode", False),
            )
            preview_payload["mode_name"] = mode_name
            preview_payload.update(mode_metadata)
            preview_payloads.append(preview_payload)
            render_view = preview_payload.get("render_view") or {}
            preview_lines.extend(
                [
                    "",
                    f"[{mode_name}] {preview_payload.get('title', '')}",
                    f"模式：{mode_metadata['mode_label']}",
                    f"标的：{preview_payload.get('symbol', '-')}",
                    f"最终卡型：{render_view.get('card_type', '-')}",
                    f"最终动作：{render_view.get('action_label', '-')}",
                    f"自动降级：{'是' if render_view.get('downgraded_to_watch') else '否'}",
                    *([f"降级原因：{render_view.get('downgrade_reason', '')}"] if render_view.get("downgrade_reason") else []),
                    f"适用场景：{mode_metadata['use_case']}",
                    f"不适用场景：{mode_metadata['not_for']}",
                    f"使用提示：{mode_metadata['operator_hint']}",
                    "正文：",
                    str(preview_payload.get("body", "")),
                ]
            )
    finally:
        preview_store.close()
    _write_report(preview_report_path, "\n".join(preview_lines))
    _write_json(preview_json_path, {"items": preview_payloads})

    recommendation = batch_payload.get("recommendation") or {}
    next_step = batch_payload.get("next_step") or {}
    replay_run = replay_payload.get("run") or {}
    return {
        "workspace_dir": str(workspace_dir),
        "replay": {
            "run_id": replay_run.get("run_id", ""),
            "status": replay_run.get("status", ""),
            "report_path": str(replay_report_path),
            "payload_path": str(replay_json_path),
        },
        "preview": {
            "report_path": str(preview_report_path),
            "payload_path": str(preview_json_path),
            "mode_count": len(preview_payloads),
        },
        "batch": {
            "batch_id": batch_payload.get("batch_id", ""),
            "spec_path": str(batch_spec_copy_path),
            "manifest_path": batch_payload.get("manifest_path", ""),
            "report_path": batch_payload.get("report_path", ""),
            "index_path": str(batch_index_path),
            "promoted_config_path": str(promoted_config_path),
            "recommendation_name": recommendation.get("name", ""),
            "recommendation_config": recommendation.get("config_summary", "-"),
            "next_step_title": next_step.get("title", ""),
        },
    }


def format_demo_flow(payload: dict) -> str:
    replay = payload.get("replay", {})
    preview = payload.get("preview", {})
    batch = payload.get("batch", {})
    lines = [
        "初版流程联调：",
        f"工作目录：{payload.get('workspace_dir', '-')}",
        f"Replay 运行：run_id={replay.get('run_id', '-')} status={replay.get('status', '-')}",
        f"Replay 报告：{replay.get('report_path', '-')}",
        f"Replay 数据：{replay.get('payload_path', '-')}",
        f"卡片预览：{preview.get('report_path', '-')}",
        f"预览数据：{preview.get('payload_path', '-')}",
        f"预览模式数：{preview.get('mode_count', '-')}",
        f"Batch 批次：{batch.get('batch_id', '-')}",
        f"Batch 规格：{batch.get('spec_path', '-')}",
        f"Batch 推荐：{batch.get('recommendation_name', '-')} ({batch.get('recommendation_config', '-')})",
        f"Batch 报告：{batch.get('report_path', '-')}",
        f"Batch Manifest：{batch.get('manifest_path', '-')}",
        f"Batch 索引：{batch.get('index_path', '-')}",
        f"推荐配置：{batch.get('promoted_config_path', '-')}",
        f"当前下一步：{batch.get('next_step_title', '-')}",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Satellite opportunity agent")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db")
    subparsers.add_parser("seed-watchlist")

    run_once_parser = subparsers.add_parser("run-once")
    run_once_parser.add_argument("--replay-path", default="", help="JSONL event replay file")
    run_once_parser.add_argument(
        "--workspace-dir",
        default=RUN_ONCE_WORKSPACE_DIR,
        help=f"手动运行结果落盘目录，默认写入 {RUN_ONCE_WORKSPACE_DIR}",
    )
    run_once_parser.add_argument("--limit", type=int, default=10, help="Strategy report row limit for written artifacts")
    _add_experiment_args(run_once_parser)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--replay-path", default="", help="JSONL event replay file")
    serve_parser.add_argument(
        "--workspace-dir",
        default=SERVE_WORKSPACE_DIR,
        help=f"常驻运行结果落盘目录，默认写入 {SERVE_WORKSPACE_DIR}",
    )
    serve_parser.add_argument("--limit", type=int, default=10, help="Strategy report row limit for written artifacts")
    _add_experiment_args(serve_parser)

    import_parser = subparsers.add_parser("import-bars")
    import_parser.add_argument("--path", required=True)
    import_parser.add_argument("--symbol", required=True)
    import_parser.add_argument("--timeframe", choices=("5m", "1d"), required=True)

    sync_parser = subparsers.add_parser("sync-yahoo-bars")
    sync_parser.add_argument("--symbol", required=True)
    sync_parser.add_argument("--timeframe", choices=("5m", "1d"), required=True)
    sync_parser.add_argument("--limit", type=int, default=120)

    config_parser = subparsers.add_parser("write-default-config")
    config_parser.add_argument("--force", action="store_true")

    sync_watchlist_parser = subparsers.add_parser("sync-watchlist")
    sync_watchlist_parser.add_argument("--use-defaults", action="store_true")

    report_runs_parser = subparsers.add_parser("report-runs")
    report_runs_parser.add_argument("--limit", type=int, default=10)
    report_runs_parser.add_argument("--json", action="store_true")

    report_run_parser = subparsers.add_parser("report-run")
    report_run_parser.add_argument("--run-id", default="")
    report_run_parser.add_argument("--logs-limit", type=int, default=50)
    report_run_parser.add_argument("--json", action="store_true")

    report_errors_parser = subparsers.add_parser("report-errors")
    report_errors_parser.add_argument("--limit", type=int, default=10)
    report_errors_parser.add_argument("--json", action="store_true")

    report_sources_parser = subparsers.add_parser("report-sources")
    report_sources_parser.add_argument("--json", action="store_true")

    report_llm_usage_parser = subparsers.add_parser("report-llm-usage")
    report_llm_usage_parser.add_argument("--days", type=int, default=LLM_USAGE_REPORT_WINDOW_DAYS)
    report_llm_usage_parser.add_argument("--json", action="store_true")

    write_llm_usage_parser = subparsers.add_parser("write-llm-usage-report")
    write_llm_usage_parser.add_argument("--workspace-dir", default=SERVE_WORKSPACE_DIR)
    write_llm_usage_parser.add_argument("--days", type=int, default=LLM_USAGE_REPORT_WINDOW_DAYS)
    write_llm_usage_parser.add_argument("--json", action="store_true")

    watchlist_review_parser = subparsers.add_parser("report-watchlist-config")
    watchlist_review_parser.add_argument("--json", action="store_true")

    theme_reference_parser = subparsers.add_parser("report-theme-reference")
    theme_reference_parser.add_argument("--json", action="store_true")

    write_theme_reference_parser = subparsers.add_parser("write-theme-reference")
    write_theme_reference_parser.add_argument("--path", default=SATELLITE_THEME_REFERENCE_PATH)
    write_theme_reference_parser.add_argument("--print", action="store_true", dest="print_output")

    report_strategy_parser = subparsers.add_parser("report-strategy")
    report_strategy_parser.add_argument("--days", type=int, default=RECENT_PERFORMANCE_WINDOW_DAYS)
    report_strategy_parser.add_argument("--start-date", default="", help="历史效果复盘起始日期，格式 YYYY-MM-DD")
    report_strategy_parser.add_argument("--end-date", default="", help="历史效果复盘结束日期，格式 YYYY-MM-DD")
    report_strategy_parser.add_argument("--limit", type=int, default=10)
    report_strategy_parser.add_argument("--json", action="store_true")

    performance_report_parser = subparsers.add_parser("write-performance-review")
    performance_report_parser.add_argument("--workspace-dir", default=SERVE_WORKSPACE_DIR)
    performance_report_parser.add_argument("--run-id", default="")
    performance_report_parser.add_argument("--days", type=int, default=RECENT_PERFORMANCE_WINDOW_DAYS)
    performance_report_parser.add_argument("--start-date", default="", help="历史效果复盘起始日期，格式 YYYY-MM-DD")
    performance_report_parser.add_argument("--end-date", default="", help="历史效果复盘结束日期，格式 YYYY-MM-DD")
    performance_report_parser.add_argument("--month", default="", help="活的月报月份，格式 YYYY-MM")
    performance_report_parser.add_argument("--limit", type=int, default=10)
    performance_report_parser.add_argument("--json", action="store_true")

    outcome_sample_parser = subparsers.add_parser("report-outcome-samples")
    outcome_sample_parser.add_argument("--days", type=int, default=RECENT_PERFORMANCE_WINDOW_DAYS)
    outcome_sample_parser.add_argument("--start-date", default="", help="样本抽检起始日期，格式 YYYY-MM-DD")
    outcome_sample_parser.add_argument("--end-date", default="", help="样本抽检结束日期，格式 YYYY-MM-DD")
    outcome_sample_parser.add_argument("--limit", type=int, default=10)
    outcome_sample_parser.add_argument("--json", action="store_true")

    ai_review_parser = subparsers.add_parser("write-outcome-audit", aliases=["write-ai-review"])
    ai_review_parser.add_argument("--workspace-dir", default=SERVE_WORKSPACE_DIR)
    ai_review_parser.add_argument("--days", type=int, default=RECENT_PERFORMANCE_WINDOW_DAYS)
    ai_review_parser.add_argument("--start-date", default="", help="AI复核起始日期，格式 YYYY-MM-DD")
    ai_review_parser.add_argument("--end-date", default="", help="AI复核结束日期，格式 YYYY-MM-DD")
    ai_review_parser.add_argument("--limit", type=int, default=OUTCOME_SAMPLE_AUDIT_LIMIT)
    ai_review_parser.add_argument("--reviewer", default="codex")
    ai_review_parser.add_argument("--json", action="store_true")

    freeze_review_parser = subparsers.add_parser("freeze-review-baseline")
    freeze_review_parser.add_argument("--workspace-dir", default=SERVE_WORKSPACE_DIR)
    freeze_review_parser.add_argument("--reviewer", default="codex")
    freeze_review_parser.add_argument("--note", default="")
    freeze_review_parser.add_argument("--json", action="store_true")

    outcome_parser = subparsers.add_parser("backfill-decision-outcomes")
    outcome_parser.add_argument("--run-id", default="")
    outcome_parser.add_argument("--limit", type=int, default=0)
    outcome_parser.add_argument("--recompute-existing", action="store_true")
    outcome_parser.add_argument("--json", action="store_true")

    cleanup_outcome_parser = subparsers.add_parser("cleanup-decision-outcomes")
    cleanup_outcome_parser.add_argument("--since", default="")
    cleanup_outcome_parser.add_argument("--until", default="")
    cleanup_outcome_parser.add_argument("--limit", type=int, default=0)
    cleanup_outcome_parser.add_argument("--apply", action="store_true")
    cleanup_outcome_parser.add_argument("--json", action="store_true")

    archive_parser = subparsers.add_parser("archive-decision-history")
    archive_parser.add_argument("--before", required=True, help="Archive decision history created before this ISO timestamp/date")
    archive_parser.add_argument("--archive-db-path", default=str(DEFAULT_ARCHIVE_DB_PATH))
    archive_parser.add_argument("--limit", type=int, default=0)
    archive_parser.add_argument("--apply", action="store_true", help="Actually copy old decision history into archive DB and delete it from the main DB")
    archive_parser.add_argument("--json", action="store_true")

    replay_eval_parser = subparsers.add_parser("replay-evaluate")
    replay_eval_parser.add_argument("--replay-path", required=True)
    replay_eval_parser.add_argument("--db-path", default="")
    replay_eval_parser.add_argument("--days", type=int, default=14)
    replay_eval_parser.add_argument("--limit", type=int, default=10)
    replay_eval_parser.add_argument("--json", action="store_true")
    _add_experiment_args(replay_eval_parser)

    batch_replay_parser = subparsers.add_parser("batch-replay")
    batch_replay_parser.add_argument("--spec-path", required=True)
    batch_replay_parser.add_argument("--output-dir", default=BATCH_RUNS_DIR)
    batch_replay_parser.add_argument("--markdown-path", default="")
    batch_replay_parser.add_argument("--json", action="store_true")

    write_batch_replay_template_parser = subparsers.add_parser("write-batch-replay-template")
    write_batch_replay_template_parser.add_argument(
        "--path",
        default=_preferred_batch_replay_template_path(),
        help="输出赛马模板 spec 的路径",
    )
    write_batch_replay_template_parser.add_argument("--print-output", action="store_true")
    write_batch_replay_template_parser.add_argument("--json", action="store_true")

    report_batch_parser = subparsers.add_parser("report-batch")
    report_batch_parser.add_argument("--manifest-path", required=True)
    report_batch_parser.add_argument("--markdown-path", default="")
    report_batch_parser.add_argument("--json", action="store_true")

    compare_batches_parser = subparsers.add_parser("compare-batches")
    compare_batches_parser.add_argument("--left-manifest-path", required=True)
    compare_batches_parser.add_argument("--right-manifest-path", required=True)
    compare_batches_parser.add_argument("--markdown-path", default="")
    compare_batches_parser.add_argument("--json", action="store_true")

    list_batches_parser = subparsers.add_parser("list-batches")
    list_batches_parser.add_argument("--dir", default=BATCH_RUNS_DIR)
    list_batches_parser.add_argument("--limit", type=int, default=10)
    list_batches_parser.add_argument("--json", action="store_true")

    daily_run_parser = subparsers.add_parser("daily-run")
    daily_run_parser.add_argument("--workspace-dir", default=DAILY_RUN_WORKSPACE_DIR)
    daily_run_parser.add_argument("--config-path", default="")
    daily_run_parser.add_argument("--replay-path", default="")
    daily_run_parser.add_argument("--days", type=int, default=14)
    daily_run_parser.add_argument("--limit", type=int, default=10)
    daily_run_parser.add_argument("--json", action="store_true")

    send_test_parser = subparsers.add_parser("send-test-notification")
    send_test_parser.add_argument("--symbol", default="NVDA")
    send_test_parser.add_argument("--json", action="store_true")

    preview_alert_parser = subparsers.add_parser("preview-alert-render")
    preview_alert_parser.add_argument("--symbol", default="NVDA")
    preview_alert_parser.add_argument("--watch", action="store_true")
    preview_alert_parser.add_argument("--prewatch-light", action="store_true")
    preview_alert_parser.add_argument("--exit-pool", action="store_true")
    preview_alert_parser.add_argument("--formal-downgraded", action="store_true")
    preview_alert_parser.add_argument("--json", action="store_true")

    promote_batch_parser = subparsers.add_parser("promote-batch")
    promote_batch_parser.add_argument("--manifest-path", required=True)
    promote_batch_parser.add_argument("--output-config-path", default="./config/satellite_agent/agent.recommended.json")
    promote_batch_parser.add_argument("--base-config-path", default="")
    promote_batch_parser.add_argument("--force", action="store_true", help="Allow overwriting the target config")
    promote_batch_parser.add_argument("--json", action="store_true")

    demo_flow_parser = subparsers.add_parser("demo-flow")
    demo_flow_parser.add_argument("--workspace-dir", default=DEMO_FLOW_DIR)
    demo_flow_parser.add_argument("--replay-path", default="tests/fixtures/events.jsonl")
    demo_flow_parser.add_argument("--batch-spec-path", default=_preferred_batch_replay_template_path())
    demo_flow_parser.add_argument("--days", type=int, default=14)
    demo_flow_parser.add_argument("--limit", type=int, default=10)
    demo_flow_parser.add_argument("--json", action="store_true")

    annotate_run_parser = subparsers.add_parser("annotate-run")
    annotate_run_parser.add_argument("--run-id", required=True)
    annotate_run_parser.add_argument("--name", default=None)
    annotate_run_parser.add_argument("--note", default=None)

    compare_runs_parser = subparsers.add_parser("compare-runs")
    compare_runs_parser.add_argument("--run-id", action="append", dest="run_ids", required=True)
    compare_runs_parser.add_argument("--limit", type=int, default=10)
    compare_runs_parser.add_argument("--json", action="store_true")

    args = parser.parse_args()
    settings = Settings.from_env()
    runtime_config = AgentRuntimeConfig.load(settings.config_path)
    settings = runtime_config.apply(settings)
    settings = _apply_cli_experiment_overrides(settings, args)
    store = Store(settings.database_path)
    store.initialize()

    if args.command == "init-db":
        return
    if args.command == "write-default-config":
        write_default_config(settings.config_path, force=args.force)
        return
    if args.command == "seed-watchlist":
        default_runtime_config = load_default_template_runtime_config()
        store.seed_watchlist(default_runtime_config.watchlist.stocks, "stock")
        store.seed_watchlist(default_runtime_config.watchlist.etfs, "etf")
        return
    if args.command == "sync-watchlist":
        if runtime_config.has_watchlist():
            store.replace_watchlist(runtime_config.watchlist.stocks, runtime_config.watchlist.etfs)
            return
        if args.use_defaults:
            default_runtime_config = load_default_template_runtime_config()
            store.replace_watchlist(
                default_runtime_config.watchlist.stocks,
                default_runtime_config.watchlist.etfs,
            )
            return
        raise SystemExit("No watchlist found in config. Use write-default-config or --use-defaults.")
    if args.command == "import-bars":
        import_bars(store, Path(args.path), args.symbol, args.timeframe)
        return
    if args.command == "sync-yahoo-bars":
        provider = _build_remote_market_data_provider()
        bars = provider.get_bars(args.symbol.upper(), args.timeframe, args.limit)
        store.upsert_price_bars(args.symbol.upper(), args.timeframe, bars)
        return
    if args.command == "report-runs":
        rows = store.load_recent_runs(limit=args.limit)
        if args.json:
            print(json.dumps(serialize_runs(rows), indent=2, sort_keys=True))
            return
        print(format_runs(rows))
        return
    if args.command == "report-run":
        row = store.load_run(args.run_id) if args.run_id else store.load_latest_run()
        if row is None:
            print("Run not found.", file=sys.stderr)
            raise SystemExit(1)
        logs = store.load_logs(row["run_id"], limit=args.logs_limit)
        if args.json:
            print(json.dumps(serialize_run_detail(row, logs), indent=2, sort_keys=True))
            return
        print(format_run_detail(row, logs))
        return
    if args.command == "report-errors":
        rows = store.aggregate_errors(limit=args.limit)
        if args.json:
            print(json.dumps(serialize_error_summary(rows), indent=2, sort_keys=True))
            return
        print(format_error_summary(rows))
        return
    if args.command == "report-sources":
        rows = store.load_latest_source_health()
        if args.json:
            print(json.dumps(serialize_source_health(rows), indent=2, sort_keys=True))
            return
        print(format_source_health(rows))
        return
    if args.command == "report-llm-usage":
        payload = build_llm_usage_report_payload(store, days=args.days)
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return
        print(format_llm_usage_report_payload(payload))
        return
    if args.command == "write-llm-usage-report":
        payload = build_write_llm_usage_report_payload(
            store,
            workspace_dir=Path(args.workspace_dir),
            days=args.days,
        )
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return
        print(format_write_llm_usage_report_result(payload))
        return
    if args.command == "report-watchlist-config":
        payload = build_watchlist_config_review_payload(runtime_config, config_path=settings.config_path)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_watchlist_config_review_payload(payload))
        return
    if args.command == "report-theme-reference":
        payload = build_theme_reference_payload(runtime_config, config_path=settings.config_path)
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return
        print(format_theme_reference_payload(payload))
        return
    if args.command == "write-theme-reference":
        payload = build_theme_reference_payload(runtime_config, config_path=settings.config_path)
        output_path = Path(args.path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
        if args.print_output:
            print(format_theme_reference_payload(payload))
            return
        print(str(output_path))
        return
    if args.command == "write-batch-replay-template":
        payload = _load_batch_replay_template_payload()
        output_path = Path(args.path)
        payload = _relativize_replay_path_for_output(payload, output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        result = {
            "template_path": str(output_path.resolve()),
            "source_template_path": payload.get("_meta", {}).get("template_path", ""),
            "experiments": [
                {
                    "name": item.get("name", ""),
                    "note": item.get("note", ""),
                }
                for item in payload.get("experiments", [])
            ],
        }
        if args.json:
            print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
            return
        if args.print_output:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            return
        print(str(output_path.resolve()))
        return
    if args.command == "report-strategy":
        report = _build_historical_strategy_report(
            store,
            days=args.days,
            limit=args.limit,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if args.json:
            print(json.dumps(report, indent=2, sort_keys=True))
            return
        print(
            format_strategy_report(
                report.get("event_type_performance", []),
                report.get("source_stability", []),
                report.get("alert_volume", []),
                report.get("decision_outcomes_by_event_type", []),
                report.get("decision_outcomes_by_pool", []),
                report.get("outcome_data_coverage"),
            )
        )
        return
    if args.command == "write-performance-review":
        payload = build_performance_review_payload(
            store,
            workspace_dir=Path(args.workspace_dir),
            run_id=args.run_id,
            days=args.days,
            limit=args.limit,
            start_date=args.start_date,
            end_date=args.end_date,
            month=args.month,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_performance_review_result(payload))
        return
    if args.command == "report-outcome-samples":
        payload = build_outcome_sample_payload(
            store,
            days=args.days,
            limit=args.limit,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_outcome_sample_payload(payload))
        return
    if args.command in {"write-outcome-audit", "write-ai-review"}:
        workspace_dir = Path(args.workspace_dir).resolve()
        workspace_dir.mkdir(parents=True, exist_ok=True)
        historical_effect_dir = workspace_dir / "historical_effect"
        historical_effect_dir.mkdir(parents=True, exist_ok=True)
        payload = build_ai_outcome_review_payload(
            store,
            days=args.days,
            limit=args.limit,
            start_date=args.start_date,
            end_date=args.end_date,
            reviewer=args.reviewer,
        )
        audit_path = historical_effect_dir / "ai_review.md"
        audit_payload_path = historical_effect_dir / "ai_review_payload.json"
        store.set_state(AI_OUTCOME_REVIEW_STATE_KEY, json.dumps(payload, ensure_ascii=False, sort_keys=True))
        _write_report(audit_path, format_ai_outcome_review_payload(payload))
        _write_json(audit_payload_path, payload)
        result = {
            "workspace_dir": str(workspace_dir),
            "audit_path": str(audit_path),
            "audit_payload_path": str(audit_payload_path),
            "audit": payload,
        }
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
            return
        print(
            "\n".join(
                [
                    "AI样本复核：",
                    f"工作目录：{workspace_dir}",
                    f"状态：{payload.get('status', '-')}",
                    f"统计区间：{(payload.get('review_window') or {}).get('start_date', '-')} ~ {(payload.get('review_window') or {}).get('end_date', '-')}",
                    f"记录文件：{audit_path}",
                    f"结构化数据：{audit_payload_path}",
                ]
            )
        )
        return
    if args.command == "freeze-review-baseline":
        workspace_dir = Path(args.workspace_dir).resolve()
        workspace_dir.mkdir(parents=True, exist_ok=True)
        historical_effect_dir = workspace_dir / "historical_effect"
        historical_effect_dir.mkdir(parents=True, exist_ok=True)
        payload = build_review_baseline_payload(reviewer=args.reviewer, note=args.note)
        baseline_path = historical_effect_dir / "review_baseline.md"
        baseline_payload_path = historical_effect_dir / "review_baseline_payload.json"
        store.set_state(HISTORICAL_EFFECT_BASELINE_STATE_KEY, json.dumps(payload, ensure_ascii=False, sort_keys=True))
        _write_report(baseline_path, format_review_baseline_payload(payload))
        _write_json(baseline_payload_path, payload)
        result = {
            "workspace_dir": str(workspace_dir),
            "baseline_path": str(baseline_path),
            "baseline_payload_path": str(baseline_payload_path),
            "baseline": payload,
        }
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
            return
        print(
            "\n".join(
                [
                    "复盘口径冻结：",
                    f"工作目录：{workspace_dir}",
                    f"状态：{payload.get('status', '-')}",
                    f"复盘口径版本：{payload.get('review_version', '-')}",
                    f"记录文件：{baseline_path}",
                    f"结构化数据：{baseline_payload_path}",
                ]
            )
        )
        return
    if args.command == "backfill-decision-outcomes":
        provider = _build_remote_market_data_provider()

        def _fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
            return provider.get_bars(symbol, timeframe, limit)

        payload = backfill_decision_outcomes(
            store,
            run_id=args.run_id,
            limit=args.limit,
            recompute_existing=args.recompute_existing,
            fetch_bars=_fetch_bars,
        )
        payload["run_id"] = args.run_id
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_decision_outcome_backfill(payload))
        return
    if args.command == "cleanup-decision-outcomes":
        payload = build_non_executable_outcome_cleanup_payload(
            store,
            actions=(OBSERVATION_DECISION_ACTION,),
            since=args.since,
            until=args.until,
            limit=args.limit,
            apply=args.apply,
        )
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return
        print(format_non_executable_outcome_cleanup(payload))
        return
    if args.command == "archive-decision-history":
        archive_db_path = Path(args.archive_db_path).resolve()
        archive_store = Store(archive_db_path)
        archive_store.initialize()
        try:
            payload = archive_decision_history(
                store,
                archive_store,
                before=args.before,
                limit=args.limit,
                apply=args.apply,
            )
        finally:
            archive_store.close()
        payload["archive_db_path"] = str(archive_db_path)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_decision_history_archive(payload))
        return
    if args.command == "replay-evaluate":
        evaluation_settings = settings
        if args.db_path:
            evaluation_settings = settings.with_overrides(database_path=Path(args.db_path))
        payload = _execute_replay_evaluation(
            evaluation_settings,
            runtime_config,
            replay_path=args.replay_path,
            days=args.days,
            limit=args.limit,
            run_name=args.name,
            note=args.note,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(
            format_replay_evaluation(
                payload["run"],
                payload["strategy_report"],
                payload["source_health"],
                payload["card_diagnostics"],
                payload.get("decision_diagnostics", []),
            )
        )
        return
    if args.command == "batch-replay":
        output_dir = Path(args.output_dir)
        payload = build_batch_replay_payload(
            settings,
            runtime_config,
            spec_path=Path(args.spec_path),
            output_dir=output_dir,
        )
        payload["manifest_path"] = str(_batch_manifest_path(output_dir, payload).resolve())
        report_path = (
            Path(args.markdown_path)
            if args.markdown_path
            else _default_batch_report_path(output_dir, payload)
        )
        payload["report_path"] = str(report_path.resolve())
        report_text = format_batch_replay(payload)
        _write_batch_manifest(output_dir, payload)
        _write_report(report_path, report_text)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(report_text)
        return
    if args.command == "report-batch":
        payload = _load_manifest(Path(args.manifest_path))
        if args.markdown_path:
            payload["report_path"] = str(Path(args.markdown_path).resolve())
        report_text = format_batch_replay(payload)
        if args.markdown_path:
            _write_report(Path(args.markdown_path), report_text)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(report_text)
        return
    if args.command == "compare-batches":
        left_payload = _load_manifest(Path(args.left_manifest_path))
        right_payload = _load_manifest(Path(args.right_manifest_path))
        payload = serialize_batch_comparison(left_payload, right_payload)
        if args.markdown_path:
            payload["report_path"] = str(Path(args.markdown_path).resolve())
        report_text = format_batch_comparison(payload)
        if args.markdown_path:
            _write_report(Path(args.markdown_path), report_text)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(report_text)
        return
    if args.command == "list-batches":
        items = _load_batch_index(Path(args.dir), args.limit)
        if args.json:
            print(json.dumps(items, indent=2, sort_keys=True))
            return
        print(format_batch_index(items))
        return
    if args.command == "daily-run":
        payload = build_daily_run_payload(
            settings,
            workspace_dir=Path(args.workspace_dir),
            config_path=Path(args.config_path) if args.config_path else None,
            replay_path=args.replay_path,
            days=args.days,
            limit=args.limit,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_daily_run(payload))
        return
    if args.command == "send-test-notification":
        try:
            payload = send_test_notification(settings, symbol=args.symbol)
        except ValueError as exc:
            raise SystemExit(str(exc))
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_test_notification_result(payload))
        return
    if args.command == "preview-alert-render":
        payload = build_preview_alert_payload(
            settings,
            runtime_config,
            store,
            symbol=args.symbol,
            watch_mode=args.watch,
            prewatch_light=args.prewatch_light,
            exit_pool_mode=args.exit_pool,
            degraded_formal_mode=args.formal_downgraded,
        )
        if args.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return
        print(format_preview_alert_result(payload))
        return
    if args.command == "promote-batch":
        manifest_payload = _load_manifest(Path(args.manifest_path))
        output_config_path = Path(args.output_config_path).resolve()
        if output_config_path.exists() and not args.force:
            raise SystemExit(f"Output config already exists: {output_config_path}")
        base_path = Path(args.base_config_path).resolve() if args.base_config_path else settings.config_path.resolve()
        base_payload = _load_json_or_default(base_path, DEFAULT_CONFIG)
        existing_output_payload = _load_json_or_default(output_config_path, base_payload)
        promoted_payload = build_promoted_config_payload(
            manifest_payload,
            base_config_payload=base_payload,
        )
        comparison_source_path = str(output_config_path if output_config_path.exists() else base_path)
        change_summary = _build_promoted_change_summary(
            existing_output_payload if output_config_path.exists() else base_payload,
            promoted_payload,
        )
        backup_path = ""
        if output_config_path.exists():
            backup_path = str(_backup_existing_file(output_config_path))
        _write_json(output_config_path, promoted_payload)
        result = {
            "manifest_path": manifest_payload.get("manifest_path", ""),
            "base_config_path": str(base_path),
            "comparison_source_path": comparison_source_path,
            "output_config_path": str(output_config_path),
            "overwrote_existing": bool(backup_path),
            "backup_path": backup_path,
            "change_summary": change_summary,
            "promoted_payload": promoted_payload,
        }
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
            return
        print(
            format_promoted_config_summary(
                manifest_path=manifest_payload.get("manifest_path", ""),
                base_config_path=str(base_path),
                comparison_source_path=comparison_source_path,
                output_config_path=str(output_config_path),
                promoted_payload=promoted_payload,
                change_summary=change_summary,
                backup_path=backup_path,
            )
        )
        return
    if args.command == "demo-flow":
        payload = build_demo_flow_payload(
            settings,
            runtime_config,
            workspace_dir=Path(args.workspace_dir),
            replay_path=args.replay_path,
            batch_spec_path=Path(args.batch_spec_path).resolve(),
            days=args.days,
            limit=args.limit,
        )
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_demo_flow(payload))
        return
    if args.command == "annotate-run":
        updated = store.update_run_metadata(args.run_id, run_name=args.name, note=args.note)
        if not updated:
            print("Run not found.", file=sys.stderr)
            raise SystemExit(1)
        return
    if args.command == "compare-runs":
        payload = build_run_comparison_payload(store, run_ids=args.run_ids, limit=args.limit)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return
        print(format_run_comparison(payload["runs"]))
        return
    if args.command == "run-once":
        service = build_service(
            settings,
            replay_path=args.replay_path,
            run_name=args.name,
            note=args.note,
            runtime_config=runtime_config,
            apply_runtime_config=False,
        )
        started_at = utcnow()
        print(format_live_cycle_started(started_at=started_at, workspace_dir=Path(args.workspace_dir)), flush=True)
        try:
            summary = service.run_once()
        except KeyboardInterrupt:
            latest_run = service.store.load_latest_run()
            if latest_run is not None:
                payload = write_live_run_artifacts(
                    service.store,
                    run_id=latest_run["run_id"],
                    workspace_dir=Path(args.workspace_dir),
                    limit=args.limit,
                    review_filename="run_once_review.md",
                    payload_filename="run_once_payload.json",
                    llm_usage_min_interval_seconds=0,
                )
                print(format_live_run_artifacts(payload), flush=True)
            raise
        latest_run = service.store.load_latest_run()
        if latest_run is not None:
            payload = write_live_run_artifacts(
                service.store,
                run_id=latest_run["run_id"],
                workspace_dir=Path(args.workspace_dir),
                limit=args.limit,
                review_filename="run_once_review.md",
                payload_filename="run_once_payload.json",
                llm_usage_min_interval_seconds=0,
            )
            print(format_live_run_artifacts(payload), flush=True)
            print(
                format_live_cycle_finished(
                    started_at=started_at,
                    finished_at=utcnow(),
                    poll_seconds=service.settings.poll_seconds,
                    run_id=latest_run["run_id"],
                    summary=summary,
                ),
                flush=True,
            )
        return
    if args.command == "serve":
        service = build_service(
            settings,
            replay_path=args.replay_path,
            run_name=args.name,
            note=args.note,
            runtime_config=runtime_config,
            apply_runtime_config=False,
        )
        while True:
            _sleep_until_runtime_window(runtime_config)
            started_at = utcnow()
            print(format_live_cycle_started(started_at=started_at, workspace_dir=Path(args.workspace_dir)), flush=True)
            try:
                summary = service.run_once()
            except KeyboardInterrupt:
                latest_run = service.store.load_latest_run()
                if latest_run is not None:
                    payload = write_live_run_artifacts(
                        service.store,
                        run_id=latest_run["run_id"],
                        workspace_dir=Path(args.workspace_dir),
                        limit=args.limit,
                        review_filename="serve_review.md",
                        payload_filename="serve_payload.json",
                        historical_effect_min_interval_seconds=SERVE_HISTORICAL_EFFECT_REFRESH_SECONDS,
                        llm_usage_min_interval_seconds=SERVE_LLM_USAGE_REFRESH_SECONDS,
                    )
                    print(format_live_run_artifacts(payload), flush=True)
                raise
            latest_run = service.store.load_latest_run()
            if latest_run is not None:
                payload = write_live_run_artifacts(
                    service.store,
                    run_id=latest_run["run_id"],
                    workspace_dir=Path(args.workspace_dir),
                    limit=args.limit,
                    review_filename="serve_review.md",
                    payload_filename="serve_payload.json",
                    historical_effect_min_interval_seconds=SERVE_HISTORICAL_EFFECT_REFRESH_SECONDS,
                    llm_usage_min_interval_seconds=SERVE_LLM_USAGE_REFRESH_SECONDS,
                )
                print(format_live_run_artifacts(payload), flush=True)
                print(
                    format_live_cycle_finished(
                        started_at=started_at,
                        finished_at=utcnow(),
                        poll_seconds=service.settings.poll_seconds,
                        run_id=latest_run["run_id"],
                        summary=summary,
                    ),
                    flush=True,
                )
            time.sleep(service.settings.poll_seconds)


if __name__ == "__main__":
    main()
