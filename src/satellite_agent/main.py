from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import timedelta
from pathlib import Path
import re
import sys

from .config import Settings
from .entry_exit import EntryExitEngine
from .event_normalizer import EventNormalizer
from .llm import OpenAIExtractor
from .market_data import CachedMarketDataProvider, MarketDataEngine, YahooFinanceMarketDataProvider
from .notifier import FeishuTransport, Notifier
from .reporting import (
    _config_summary,
    format_batch_index,
    format_batch_comparison,
    format_batch_replay,
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
from .models import Bar, utcnow
from .models import OpportunityCard, PriceRange
from .timefmt import format_beijing_minute


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
    adapters = []
    if replay_path:
        adapters.append(JsonlReplaySourceAdapter(Path(replay_path)))
    if settings.use_sec_filings_source and stock_watchlist:
        adapters.append(SecFilingsSourceAdapter(symbols=stock_watchlist, user_agent=settings.sec_user_agent))
    if settings.use_google_news_source and watchlist:
        adapters.append(GoogleNewsSourceAdapter(symbols=watchlist, mode="news"))
    if settings.use_google_research_source and watchlist:
        adapters.append(GoogleNewsSourceAdapter(symbols=watchlist, mode="research"))
    source_adapter = CompositeSourceAdapter(adapters) if adapters else StaticSourceAdapter([])
    extractor = OpenAIExtractor(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        base_url=settings.openai_base_url,
    )
    transport = FeishuTransport(settings.feishu_webhook) if settings.feishu_webhook else None
    notifier = Notifier(store=store, transport=transport, dry_run=settings.dry_run)
    provider = CachedMarketDataProvider(store=store, remote_provider=YahooFinanceMarketDataProvider())
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
        },
        run_name=run_name,
        note=note,
    )


def _sync_watchlist_if_needed(store: Store, settings: Settings, runtime_config: AgentRuntimeConfig) -> None:
    current = store.load_watchlist()
    if current:
        return
    if runtime_config.has_watchlist():
        store.replace_watchlist(runtime_config.watchlist.stocks, runtime_config.watchlist.etfs)
        return
    default_runtime_config = load_default_template_runtime_config()
    if default_runtime_config.has_watchlist():
        store.replace_watchlist(
            default_runtime_config.watchlist.stocks,
            default_runtime_config.watchlist.etfs,
        )


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
        horizons=horizon_overrides,
    )


def _apply_override_mapping(settings: Settings, overrides: dict) -> Settings:
    return settings.with_strategy_overrides(
        event_score_threshold=overrides.get("event_score_threshold"),
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


def build_strategy_report_payload(store: Store, *, days: int, limit: int) -> dict:
    since = (utcnow() - timedelta(days=days)).isoformat()
    event_types = store.aggregate_event_type_performance(since=since, limit=limit)
    source_stability = store.aggregate_source_stability(since=since, limit=limit)
    alert_volume = store.aggregate_alert_volume(since=since, limit=days)
    return serialize_strategy_report(event_types, source_stability, alert_volume)


def _build_run_scoped_strategy_report(store: Store, *, run_id: str, limit: int) -> dict:
    event_type_rows = store.aggregate_event_type_performance_for_run(run_id, limit=limit)
    alert_rows = store.aggregate_alert_volume_for_run(run_id)
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
    return {
        "event_type_performance": [
            {
                "event_type": row["event_type"],
                "card_count": row["card_count"],
                "avg_final_score": row["avg_final_score"],
                "high_priority_count": row["high_priority_count"],
            }
            for row in event_type_rows
        ],
        "source_stability": source_stability,
        "alert_volume": [
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
    }


def build_replay_evaluation_payload(store: Store, *, run_id: str, days: int, limit: int) -> dict:
    run_row = store.load_run(run_id)
    run_detail = serialize_run_detail(run_row, store.load_logs(run_id, limit=200)) if run_row else None
    strategy_report = _build_run_scoped_strategy_report(store, run_id=run_id, limit=limit)
    source_health = serialize_source_health(store.load_source_health(run_id))
    card_diagnostics = _build_card_diagnostics(
        run_detail,
        store.load_opportunity_cards(run_id),
        store.load_alert_history(run_id),
    )
    return serialize_replay_evaluation(run_detail, strategy_report, source_health, card_diagnostics)


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
        items.append(
            {
                "card_id": card["card_id"],
                "symbol": card["symbol"],
                "horizon": horizon,
                "event_type": card["event_type"],
                "priority": card["priority"],
                "headline_summary": card.get("headline_summary", ""),
                "reason_to_watch": card.get("reason_to_watch", ""),
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
    return {
        "event_score_threshold": strategy.get("event_score_threshold"),
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

        review_path = workspace_dir / "daily_run_review.md"
        payload_path = workspace_dir / "daily_run_payload.json"
        review_text = format_run_review(run_detail, strategy_report, source_health, card_diagnostics)
        health_summary = summarize_run_health(run_detail, strategy_report, source_health, card_diagnostics)
        review_payload = {
            "run": run_detail,
            "strategy_report": strategy_report,
            "source_health": source_health,
            "card_diagnostics": card_diagnostics,
            "health_summary": health_summary,
        }
        _write_report(review_path, review_text)
        _write_json(payload_path, review_payload)

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
            "review_path": str(review_path),
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
        f"复盘报告：{payload.get('review_path', '-')}",
        f"结构化数据：{payload.get('payload_path', '-')}",
    ]
    if payload.get("replay_path"):
        lines.append(f"Replay 输入：{payload.get('replay_path')}")
    return "\n".join(lines)


def write_live_run_artifacts(store: Store, *, run_id: str, workspace_dir: Path, limit: int) -> dict:
    workspace_dir = workspace_dir.resolve()
    workspace_dir.mkdir(parents=True, exist_ok=True)
    payload = build_replay_evaluation_payload(store, run_id=run_id, days=14, limit=limit)
    review_path = workspace_dir / "latest_live_review.md"
    payload_path = workspace_dir / "latest_live_payload.json"
    review_text = format_run_review(
        payload["run"],
        payload["strategy_report"],
        payload["source_health"],
        payload["card_diagnostics"],
    )
    _write_report(review_path, review_text)
    _write_json(payload_path, payload)
    return {
        "run_id": run_id,
        "review_path": str(review_path),
        "payload_path": str(payload_path),
    }


def format_live_run_artifacts(payload: dict) -> str:
    return "\n".join(
        [
            "实时运行结果已落盘：",
            f"运行 ID：{payload.get('run_id', '-')}",
            f"复盘文档：{payload.get('review_path', '-')}",
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
        reason_to_watch="如果你能在手机飞书里看到这张卡片，说明提醒链路已可用于真实监控。",
    )


def send_test_notification(settings: Settings, *, symbol: str = "NVDA") -> dict:
    if not settings.feishu_webhook:
        raise ValueError("SATELLITE_FEISHU_WEBHOOK is not configured.")
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
    promoted_config_path = workspace_dir / "recommended_agent.json"

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
        default="./data/live_run",
        help="实时结果落盘目录，默认写入 ./data/live_run",
    )
    run_once_parser.add_argument("--limit", type=int, default=10, help="Strategy report row limit for written artifacts")
    _add_experiment_args(run_once_parser)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--replay-path", default="", help="JSONL event replay file")
    serve_parser.add_argument(
        "--workspace-dir",
        default="./data/live_run",
        help="实时结果落盘目录，默认写入 ./data/live_run",
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

    report_strategy_parser = subparsers.add_parser("report-strategy")
    report_strategy_parser.add_argument("--days", type=int, default=14)
    report_strategy_parser.add_argument("--limit", type=int, default=10)
    report_strategy_parser.add_argument("--json", action="store_true")

    replay_eval_parser = subparsers.add_parser("replay-evaluate")
    replay_eval_parser.add_argument("--replay-path", required=True)
    replay_eval_parser.add_argument("--db-path", default="")
    replay_eval_parser.add_argument("--days", type=int, default=14)
    replay_eval_parser.add_argument("--limit", type=int, default=10)
    replay_eval_parser.add_argument("--json", action="store_true")
    _add_experiment_args(replay_eval_parser)

    batch_replay_parser = subparsers.add_parser("batch-replay")
    batch_replay_parser.add_argument("--spec-path", required=True)
    batch_replay_parser.add_argument("--output-dir", default="./data/batch_runs")
    batch_replay_parser.add_argument("--markdown-path", default="")
    batch_replay_parser.add_argument("--json", action="store_true")

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
    list_batches_parser.add_argument("--dir", default="./data/batch_runs")
    list_batches_parser.add_argument("--limit", type=int, default=10)
    list_batches_parser.add_argument("--json", action="store_true")

    daily_run_parser = subparsers.add_parser("daily-run")
    daily_run_parser.add_argument("--workspace-dir", default="./data/daily_run")
    daily_run_parser.add_argument("--config-path", default="")
    daily_run_parser.add_argument("--replay-path", default="")
    daily_run_parser.add_argument("--days", type=int, default=14)
    daily_run_parser.add_argument("--limit", type=int, default=10)
    daily_run_parser.add_argument("--json", action="store_true")

    send_test_parser = subparsers.add_parser("send-test-notification")
    send_test_parser.add_argument("--symbol", default="NVDA")
    send_test_parser.add_argument("--json", action="store_true")

    promote_batch_parser = subparsers.add_parser("promote-batch")
    promote_batch_parser.add_argument("--manifest-path", required=True)
    promote_batch_parser.add_argument("--output-config-path", default="./config/agent.recommended.json")
    promote_batch_parser.add_argument("--base-config-path", default="")
    promote_batch_parser.add_argument("--force", action="store_true", help="Allow overwriting the target config")
    promote_batch_parser.add_argument("--json", action="store_true")

    demo_flow_parser = subparsers.add_parser("demo-flow")
    demo_flow_parser.add_argument("--workspace-dir", default="./data/demo_flow")
    demo_flow_parser.add_argument("--replay-path", default="tests/fixtures/events.jsonl")
    demo_flow_parser.add_argument("--batch-spec-path", default="config/batch_replay.template.json")
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
        provider = YahooFinanceMarketDataProvider()
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
    if args.command == "report-strategy":
        since = (utcnow() - timedelta(days=args.days)).isoformat()
        event_types = store.aggregate_event_type_performance(since=since, limit=args.limit)
        source_stability = store.aggregate_source_stability(since=since, limit=args.limit)
        alert_volume = store.aggregate_alert_volume(since=since, limit=args.days)
        if args.json:
            print(
                json.dumps(
                    serialize_strategy_report(event_types, source_stability, alert_volume),
                    indent=2,
                    sort_keys=True,
                )
            )
            return
        print(format_strategy_report(event_types, source_stability, alert_volume))
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
