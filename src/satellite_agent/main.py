from __future__ import annotations

import argparse
import csv
import json
import os
import time
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
from .notifier import FeishuTransport, Notifier
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


def _historical_effect_exit_reason_label(reason: str) -> str:
    return {
        "hit_take_profit": "止盈退出",
        "hit_invalidation": "失效退出",
        "window_complete": "复盘窗口结算",
        "insufficient_lookahead": "观察中",
        "not_entered": "未进场",
    }.get(reason, reason or "未回补")


def _historical_effect_status_label(*, entered: bool, close_reason: str) -> str:
    if not entered:
        return "未进场"
    return _historical_effect_exit_reason_label(close_reason)


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
                "real_exit_label": (
                    "已真实退出"
                    if str(row.get("close_reason") or "") in {"hit_take_profit", "hit_invalidation", "window_complete"}
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
            if str(row.get("close_reason") or "") in {"hit_take_profit", "hit_invalidation", "window_complete"}
            and row.get("realized_return") is not None
        ]
        realized_values = [float(row["realized_return"]) for row in exited_rows]
        take_profit_count = sum(1 for row in entered_rows if str(row.get("close_reason") or "") == "hit_take_profit")
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
    finally:
        if archive_store is not None:
            archive_store.close()

    rows = _merge_decision_rows(list(main_rows), list(archive_rows))
    observation_rows_merged = _merge_decision_rows(list(observation_rows), list(archive_observation_rows))
    entered_rows = [row for row in rows if bool(row.get("entered"))]
    not_entered_rows = [row for row in rows if not bool(row.get("entered"))]
    take_profit_rows = [row for row in entered_rows if str(row.get("close_reason") or "") == "hit_take_profit"]
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
    for field, label in (
        ("t_plus_1_return", "T+1"),
        ("t_plus_3_return", "T+3"),
        ("t_plus_7_return", "T+7"),
        ("t_plus_14_return", "T+14"),
        ("t_plus_30_return", "T+30"),
        ("max_runup", "最大浮盈"),
        ("max_drawdown", "最大回撤"),
    ):
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        auxiliary_metrics.append(
            {
                "field": field,
                "label": label,
                "sample_count": len(values),
                "avg_value": _mean(values),
            }
        )

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
    recommendations: list[str] = []
    if not rows:
        recommendations.append("当前窗口内没有可执行建议，先继续积累样本。")
    else:
        if best_event is not None:
            recommendations.append(
                f"{best_event['label']} 是当前窗口里表现最好的事件类型，可优先保留并继续观察后续样本。"
            )
        if worst_event is not None and worst_event is not best_event:
            recommendations.append(
                f"{worst_event['label']} 表现最弱，优先回看这类事件的阈值、排序和入场时机。"
            )
        if best_pool is not None:
            recommendations.append(
                f"{best_pool['label']} 当前是表现更好的池子，可优先作为后续筛选和排序参考。"
            )
        if worst_pool is not None and worst_pool is not best_pool:
            recommendations.append(
                f"{worst_pool['label']} 当前表现偏弱，优先检查该池子的升池标准和价格计划。"
            )
        if best_trigger is not None:
            recommendations.append(
                f"{best_trigger['label']} 当前相对更稳，可继续观察这类触发方式的后续样本。"
            )
        if worst_trigger is not None and worst_trigger is not best_trigger:
            recommendations.append(
                f"{worst_trigger['label']} 当前偏弱，值得回看触发条件是否过于宽松。"
            )
        invalidation_rate = _percentage(len(invalidation_rows), len(entered_rows)) or 0.0
        window_complete_rate = _percentage(len(window_complete_rows), len(entered_rows)) or 0.0
        not_entered_rate = _percentage(len(not_entered_rows), len(rows)) or 0.0
        take_profit_rate = _percentage(len(take_profit_rows), len(entered_rows)) or 0.0
        avg_runup = next((row.get("avg_value") for row in auxiliary_metrics if row.get("field") == "max_runup"), None)
        if invalidation_rate >= 40.0:
            recommendations.append("失效退出占比偏高，优先检查入场是否过早，以及失效缓冲是否过窄。")
        if window_complete_rate >= 40.0:
            recommendations.append("复盘窗口结算占比偏高，说明止盈偏远或催化延续性还不够强。")
        if not_entered_rate >= 40.0:
            recommendations.append("未进场样本偏多，说明入场区间可能偏保守，值得回看挂单区间。")
        if take_profit_rate == 0.0 and avg_runup is not None and float(avg_runup) > 0:
            recommendations.append("样本中曾出现一定浮盈但没有止盈兑现，建议回看止盈区间是否偏远。")
    if not recommendations:
        recommendations.append("当前样本表现较均衡，继续累计更多完整样本后再调整策略。")
    deduped_recommendations: list[str] = []
    for line in recommendations:
        if line not in deduped_recommendations:
            deduped_recommendations.append(line)

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
    return {
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
    }


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
    if bool(row.get("entered")) != bool(recomputed.entered):
        issues.append(f"entered: 存量={bool(row.get('entered'))} / 重算={bool(recomputed.entered)}")
    if str(row.get("close_reason") or "") != str(recomputed.close_reason or ""):
        issues.append(
            f"close_reason: 存量={str(row.get('close_reason') or '-') } / 重算={str(recomputed.close_reason or '-')}"
        )
    for field, label in (
        ("entry_price", "entry_price"),
        ("exit_price", "exit_price"),
        ("realized_return", "realized_return"),
    ):
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
        "hit_invalidation": "失效退出",
        "window_complete": "复盘窗口结算",
        "insufficient_lookahead": "观察中",
        "not_entered": "未进场",
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
            if str(row.get("close_reason") or "") in {"hit_take_profit", "hit_invalidation", "window_complete"}
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
    main_event_outcomes = store.aggregate_decision_outcomes_by_event_type(since=since, until=until, limit=limit)
    main_pool_outcomes = store.aggregate_decision_outcomes_by_pool(since=since, until=until, limit=limit)
    main_summary = _row_to_outcome_summary(store.summarize_decision_outcomes(since, until))
    archive_event_outcomes: list[dict] | list[object] = []
    archive_pool_outcomes: list[dict] | list[object] = []
    archive_summary = {
        "decision_count": 0,
        "outcome_count": 0,
        "pending_count": 0,
        "completed_count": 0,
    }
    if archive_store is not None:
        archive_event_outcomes = archive_store.aggregate_decision_outcomes_by_event_type(since=since, until=until, limit=limit)
        archive_pool_outcomes = archive_store.aggregate_decision_outcomes_by_pool(since=since, until=until, limit=limit)
        archive_summary = _row_to_outcome_summary(archive_store.summarize_decision_outcomes(since, until))
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
    decision_outcomes_by_event_type = store.aggregate_decision_outcomes_by_event_type_for_run(run_id, limit=limit)
    decision_outcomes_by_pool = store.aggregate_decision_outcomes_by_pool_for_run(run_id, limit=limit)
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
            list(store.aggregate_decision_outcomes_by_event_type(since=since, limit=limit)),
            list(archive_store.aggregate_decision_outcomes_by_event_type(since=since, limit=limit)) if archive_store is not None else [],
            key_field="event_type",
        )[:limit]
        pool_context_rows = _merge_outcome_rows(
            list(store.aggregate_decision_outcomes_by_pool(since=since, limit=limit)),
            list(archive_store.aggregate_decision_outcomes_by_pool(since=since, limit=limit)) if archive_store is not None else [],
            key_field="pool",
        )[:limit]
    finally:
        if archive_store is not None:
            archive_store.close()
    return _build_outcome_context_index(event_type_context_rows, pool_context_rows)


def build_replay_evaluation_payload(store: Store, *, run_id: str, days: int, limit: int) -> dict:
    run_row = store.load_run(run_id)
    run_detail = serialize_run_detail(run_row, store.load_logs(run_id, limit=200)) if run_row else None
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
                "action": row["action"],
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
                "created_at": row["created_at"],
                "t_plus_1_return": row["t_plus_1_return"],
                "t_plus_3_return": row["t_plus_3_return"],
                "t_plus_5_return": row["t_plus_5_return"],
                "t_plus_7_return": row["t_plus_7_return"],
                "t_plus_10_return": row["t_plus_10_return"],
                "t_plus_14_return": row["t_plus_14_return"],
                "t_plus_30_return": row["t_plus_30_return"],
                "max_runup": row["max_runup"],
                "max_drawdown": row["max_drawdown"],
                "hit_take_profit": bool(row["hit_take_profit"]) if row["hit_take_profit"] is not None else False,
                "hit_invalidation": bool(row["hit_invalidation"]) if row["hit_invalidation"] is not None else False,
                "close_reason": row["close_reason"] or "",
                "event_type_outcome_context": event_type_context.get(event_type, {}),
                "pool_outcome_context": pool_context.get(str(row["pool"] or ""), {}),
            }
        )
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

    historical_effect_review = _build_historical_effect_review_data(
        store,
        days=days,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )
    performance_review_text = format_recent_performance_review(historical_effect_review)
    sample_audit_payload = build_outcome_sample_payload(
        store,
        days=days,
        limit=OUTCOME_SAMPLE_AUDIT_LIMIT,
        start_date=start_date,
        end_date=end_date,
    )
    sample_audit_text = format_outcome_sample_payload(sample_audit_payload)

    performance_review_path = historical_effect_dir / "review.md"
    payload_path = historical_effect_dir / "review_payload.json"
    sample_audit_path = historical_effect_dir / "sample_audit.md"
    sample_audit_payload_path = historical_effect_dir / "sample_audit_payload.json"
    payload = {
        "workspace_dir": str(workspace_dir),
        "database_path": str(store.database_path.resolve()),
        "run_id": selected_run_id,
        "window_days": historical_effect_review["review_window"]["window_days"],
        "limit": limit,
        "performance_review_path": str(performance_review_path),
        "payload_path": str(payload_path),
        "sample_audit_path": str(sample_audit_path),
        "sample_audit_payload_path": str(sample_audit_payload_path),
        "historical_effect_review": historical_effect_review,
        "sample_audit": sample_audit_payload,
        "current_run": run_detail,
        "current_strategy_report": strategy_report,
        "health_summary": health_summary,
    }
    _write_report(performance_review_path, performance_review_text)
    _write_json(payload_path, payload)
    _write_report(sample_audit_path, sample_audit_text)
    _write_json(sample_audit_payload_path, sample_audit_payload)
    return payload


def format_performance_review_result(payload: dict) -> str:
    review = payload.get("historical_effect_review") or {}
    window = review.get("review_window") or {}
    return "\n".join(
        [
            "历史效果复盘：",
            f"工作目录：{payload.get('workspace_dir', '-')}",
            f"数据库：{payload.get('database_path', '-')}",
            f"参考运行：{payload.get('run_id', '-') or '-'}",
            f"状态：{review.get('status', '-')}",
            f"复盘口径版本：{review.get('review_version', '-')}",
            f"统计区间：{window.get('start_date', '-')} ~ {window.get('end_date', '-')}",
            f"样本抽检：{payload.get('sample_audit_path', '-')}",
            f"报告文件：{payload.get('performance_review_path', '-')}",
            f"结构化数据：{payload.get('payload_path', '-')}",
        ]
    )


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
        "llm_usage_report_path": llm_usage_report_path,
        "llm_usage_report_refreshed": should_refresh_llm_usage,
        "payload_path": str(payload_path),
    }


def format_live_run_artifacts(payload: dict) -> str:
    historical_effect_line = f"历史效果复盘：{payload.get('historical_effect_review_path', '-')}"
    if not payload.get("historical_effect_review_refreshed", True):
        historical_effect_line += "（本轮沿用上次刷新）"
    llm_usage_line = f"LLM 用量报告：{payload.get('llm_usage_report_path', '-')}"
    if not payload.get("llm_usage_report_refreshed", True):
        llm_usage_line += "（本轮沿用上次刷新）"
    return "\n".join(
        [
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
            llm_usage_line,
            f"结构化数据：{payload.get('payload_path', '-')}",
        ]
    )


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
    return OpportunityCard(
        card_id=f"preview-formal:{normalized_symbol}",
        event_id=f"preview-formal-event:{normalized_symbol}",
        symbol=normalized_symbol,
        horizon="swing",
        event_type="strategic",
        headline_summary="模拟正式卡片，用于预览当前 LLM 文案和完整通知渲染效果。",
        bull_case="若事件兑现顺利，叙事会继续强化，短线资金更容易沿主线加速交易。",
        bear_case="若预期兑现不足或量能回落，强势信号容易迅速降温。",
        event_score=82.0,
        market_score=76.0,
        final_score=79.6,
        entry_range=PriceRange(100.0, 102.0),
        take_profit_range=PriceRange(108.0, 112.0),
        invalidation_level=97.0,
        invalidation_reason="模拟卡片，不作为真实交易依据。",
        risk_notes=["模拟预览用卡片", "正式执行前需结合真实行情与事件确认"],
        source_refs=["https://example.com/preview-alert"],
        created_at=now,
        ttl=now + timedelta(days=5),
        priority="high",
        dedup_key=f"preview-formal:{normalized_symbol}",
        bias="long",
        display_name=display_name,
        action_label="确认做多",
        confidence_label="高",
        confidence_score=84.0,
        reason_to_watch="如果当前文案读起来足够清楚，说明 Qwen 生成链路已经适合继续上线观察。",
        trend_state="bullish",
        rsi_14=61.5,
        relative_volume=1.72,
        theme_tags=theme_tags,
        chain_summary="昨晚试探建仓 -> 今日升级确认做多",
        market_regime="risk_on",
        rate_risk="medium",
        geopolitical_risk="low",
        macro_risk_score=25.0,
        positioning_hint="适合按价格计划跟随，不适合追高扩仓。",
    )


def _build_preview_event_insight(symbol: str) -> EventInsight:
    now = utcnow()
    normalized_symbol = symbol.upper()
    return EventInsight(
        event_id=f"preview-insight:{normalized_symbol}",
        symbol=normalized_symbol,
        event_type="strategic",
        headline_summary="公司宣布扩大与 AI 基建相关的合作与投入。",
        bull_case="合作落地会强化市场对后续订单与资本开支扩张的预期。",
        bear_case="若合作更多停留在叙事层，市场可能快速回吐短线溢价。",
        importance=84.0,
        source_credibility=86.0,
        novelty=72.0,
        sentiment=0.70,
        theme_relevance=90.0,
        llm_confidence=80.0,
        risk_notes=["兑现节奏低于预期是主要失败点。"],
        source_refs=["https://example.com/preview-alert"],
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
) -> dict[str, Any]:
    llm_used = False
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
            headline_summary="模拟预备池轻推送，用于预览当前轻推正文与 LLM 文案效果。",
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
    else:
        card = _build_preview_notification_card(runtime_config, symbol=symbol, watch_mode=watch_mode)
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
            card = OpportunityCard(
                **{
                    **card.__dict__,
                    "llm_summary": narrative.summary,
                    "llm_impact_inference": narrative.impact_inference,
                    "llm_reasoning": narrative.reasoning,
                    "llm_uncertainty": narrative.uncertainty,
                    "narrative_priority_adjustment": narrative.priority_adjustment,
                }
            )
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
    return {
        "symbol": card.symbol,
        "watch_mode": watch_mode,
        "prewatch_light": prewatch_light,
        "llm_enabled": bool(settings.openai_api_key and settings.use_llm_narration),
        "llm_used": llm_used,
        "title": f"[预备池] {notifier._title(card)}" if prewatch_light else notifier._title(card),
        "body": notifier._body(card),
        "delivery_view": build_delivery_view_from_record(card.to_record()),
        "feishu_card": transport._build_interactive_payload(card),
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
    if payload.get("prewatch_light"):
        mode_text = "预备池轻推送"
    else:
        mode_text = "预备池观察卡" if payload.get("watch_mode") else "正式操作卡"
    return "\n".join(
        [
            "本地预览卡片：",
            f"模式：{mode_text}",
            f"标的：{payload.get('symbol', '-')}",
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
    batch = payload.get("batch", {})
    lines = [
        "初版流程联调：",
        f"工作目录：{payload.get('workspace_dir', '-')}",
        f"Replay 运行：run_id={replay.get('run_id', '-')} status={replay.get('status', '-')}",
        f"Replay 报告：{replay.get('report_path', '-')}",
        f"Replay 数据：{replay.get('payload_path', '-')}",
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
