from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Dict

from .config import Settings
from .decision_engines import (
    DecisionPacket,
    HybridEventUnderstandingEngine,
    HybridThemeUnderstandingEngine,
    RuleMarketUnderstandingEngine,
)
from .decision_engines.mappers import decision_packet_to_card
from .decision_engines.types import PricePlan, ReasonSections, SourceBundle
from .entry_exit import EntryExitEngine
from .event_normalizer import EventNormalizer
from .llm import OpenAIExtractor, RuleBasedExtractor
from .market_data import (
    MarketDataEngine,
    MultiSourceMarketDataProvider,
    StooqDailyMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from .models import utcnow
from .models import SourceHealthCheck
from .notifier import Notifier
from .observability import RunContext, StructuredLogger
from .prewatch import build_prewatch_candidate, sort_prewatch_candidates
from .scoring import SignalScorer
from .sources import SourceAdapter
from .store import Store
from .theme_linkage import (
    build_symbol_theme_map_from_watchlist_payload,
    build_theme_memberships,
    display_theme_name,
    summarize_symbol_theme_context,
    theme_tags_for_symbol,
)
from .timefmt import BEIJING_TZ

STRONG_SELECTION_TERMS: tuple[tuple[str, float], ...] = (
    ("earnings", 24.0),
    ("guidance", 22.0),
    ("forecast", 12.0),
    ("outlook", 12.0),
    ("8-k", 24.0),
    ("10-q", 22.0),
    ("10-k", 22.0),
    ("filing", 12.0),
    ("merger", 26.0),
    ("acquisition", 26.0),
    ("buyout", 26.0),
    ("strategic partnership", 26.0),
    ("partnership", 18.0),
    ("collaboration", 18.0),
    ("investment", 22.0),
    ("invests", 22.0),
    ("invested", 22.0),
    ("stake", 20.0),
    ("funding", 18.0),
    ("backed", 18.0),
    ("approval", 22.0),
    ("contract", 18.0),
    ("deal", 14.0),
    ("buyback", 16.0),
    ("dividend", 10.0),
    ("launch", 10.0),
)

LOW_SIGNAL_SELECTION_TERMS: tuple[tuple[str, float], ...] = (
    ("stock price today", -36.0),
    ("price prediction", -34.0),
    ("prediction", -20.0),
    ("top 10", -28.0),
    ("top stocks", -24.0),
    ("best stocks", -22.0),
    ("should you buy", -26.0),
    ("should investors", -22.0),
    ("to buy now", -24.0),
    ("history", -18.0),
    ("battle royale", -28.0),
    ("vs.", -14.0),
    (" versus ", -14.0),
    ("etf of the day", -20.0),
    ("everything you need to know", -24.0),
    ("what you need to know", -20.0),
)

SOURCE_TYPE_SELECTION_WEIGHTS: dict[str, float] = {
    "filing": 28.0,
    "earnings": 26.0,
    "press_release": 18.0,
    "research": 8.0,
    "news": 0.0,
}

THEME_SELECTION_BONUS_PER_PEER = 1.5
THEME_BATCH_CLUSTER_BONUS_PER_SYMBOL = 2.0
THEME_BATCH_CLUSTER_MAX_BONUS = 8.0
THEME_PREWATCH_CLUSTER_BONUS = 2.5
THEME_PREWATCH_CONFIRMED_BONUS = 4.0
THEME_CONFIRMATION_CHAIN_BONUS = 1.5
THEME_MEMORY_STATE_KEY = "theme_heat_memory"
EVENT_PREWATCH_BASE_BONUS = 4.0

EVENT_TYPE_DISPLAY_NAMES: dict[str, str] = {
    "earnings": "财报",
    "guidance": "指引",
    "sec": "公告",
    "research": "研报",
    "m&a": "并购",
    "strategic": "战略合作",
    "product": "产品发布",
    "news": "新闻",
}


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


def _display_trend_state(value: str) -> str:
    return {
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "震荡",
        "uptrend": "多头",
        "downtrend": "空头",
    }.get(value, value)


class SatelliteAgentService:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        source_adapter: SourceAdapter,
        normalizer: EventNormalizer,
        extractor: RuleBasedExtractor,
        market_data: MarketDataEngine,
        scorer: SignalScorer,
        entry_exit: EntryExitEngine,
        notifier: Notifier,
        runtime_snapshot: dict | None = None,
        run_name: str = "",
        note: str = "",
        prewatch_symbols: list[str] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.source_adapter = source_adapter
        self.normalizer = normalizer
        self.extractor = extractor
        self.market_data = market_data
        self.scorer = scorer
        self.entry_exit = entry_exit
        self.notifier = notifier
        self.runtime_snapshot = runtime_snapshot or {}
        self.run_name = run_name
        self.note = note
        self.prewatch_symbols = [symbol.upper() for symbol in (prewatch_symbols or [])]
        runtime_watchlist = self.runtime_snapshot.get("runtime_config", {}).get("watchlist", {})
        self.symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(runtime_watchlist)
        self.theme_memberships = build_theme_memberships(self.symbol_theme_map)
        self.event_engine = HybridEventUnderstandingEngine(
            rule_extractor=RuleBasedExtractor(),
            llm_extractor=extractor if isinstance(extractor, OpenAIExtractor) else None,
        )
        self.market_engine = RuleMarketUnderstandingEngine(scorer=self.scorer, entry_exit=self.entry_exit)
        self.theme_engine = HybridThemeUnderstandingEngine(
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
            store=self.store,
            settings=self.settings,
        )
        self.prewatch_market_data = MarketDataEngine(
            MultiSourceMarketDataProvider(
                [
                    YahooFinanceMarketDataProvider(),
                    StooqDailyMarketDataProvider(),
                ]
            )
        )

    def run_once(self) -> Dict[str, int]:
        run_context = self._create_run_context()
        logger = StructuredLogger(self.store, run_context.run_id)
        logger.info("run_started", "Satellite agent run started.", stage="run")
        watchlist = self.store.load_watchlist()
        now = utcnow()
        last_poll = self.store.get_state("last_event_poll_at")
        since = datetime.fromisoformat(last_poll) if last_poll else now - timedelta(days=1)
        events = self._fetch_events_from_sources(since, run_context, logger)
        events = self._deduplicate_fetched_events(events)
        raw_event_count = len(events)
        events = self._limit_fetched_events(events)
        if len(events) < raw_event_count:
            logger.info(
                "events_selected",
                "Selected a capped subset of candidate events for this run.",
                stage="source_fetch",
                context={
                    "raw_count": raw_event_count,
                    "selected_count": len(events),
                    "max_events_per_run": self.settings.max_events_per_run,
                    "max_events_per_symbol_per_run": self.settings.max_events_per_symbol_per_run,
                },
            )
        fetch_selected_count = len(events)
        events = self._limit_deep_processing_events(events)
        if len(events) < fetch_selected_count:
            logger.info(
                "events_deep_selected",
                "Selected a tighter subset of candidate events for deep processing.",
                stage="source_fetch",
                context={
                    "selected_count": fetch_selected_count,
                    "deep_selected_count": len(events),
                    "max_deep_events_per_run": self.settings.max_deep_events_per_run,
                },
            )
        run_context.metrics.events_fetched = len(events)
        logger.info(
            "events_fetched",
            "Fetched candidate events from configured sources.",
            stage="source_fetch",
            context={"count": len(events), "since": since.isoformat()},
        )
        newest_seen = since
        dedup_after = (now - timedelta(hours=self.settings.cross_source_dedup_hours)).isoformat()
        status = "success"
        notification_candidates = []
        snapshot_cache: dict[tuple[str, str], object] = {}
        saved_snapshot_keys: set[tuple[str, str]] = set()
        prewatch_candidates = []
        prewatch_alert_symbols: list[str] = []
        llm_requests_used = 0
        llm_daily_requests_used = self._llm_daily_usage_count(now)
        card_contexts: dict[str, dict] = {}
        try:
            for raw_event in events:
                event = self.normalizer.normalize(raw_event)
                newest_seen = max(newest_seen, event.published_at)
                if watchlist and event.symbol not in watchlist:
                    run_context.metrics.skipped_out_of_watchlist += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped because symbol is outside active watchlist.",
                        stage="filter",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "out_of_watchlist"},
                    )
                    continue
                if self.store.has_recent_fingerprint(event.symbol, event.event_fingerprint, dedup_after):
                    run_context.metrics.skipped_cross_source_duplicate += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped due to recent cross-source duplicate fingerprint.",
                        stage="filter",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "cross_source_duplicate"},
                    )
                    continue
                inserted = self.store.save_raw_event(event, run_id=run_context.run_id)
                if not inserted:
                    run_context.metrics.skipped_duplicate_event_id += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped because event_id already exists.",
                        stage="store",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "duplicate_event_id"},
                    )
                    continue
                run_context.metrics.events_processed += 1
                logger.info(
                    "event_accepted",
                    "Event accepted for extraction and scoring.",
                    stage="store",
                    symbol=event.symbol,
                    event_id=event.event_id,
                    context={"source": event.source, "source_type": event.source_type},
                )
                try:
                    insight, event_assessment, llm_requests_used, llm_daily_requests_used = self._extract_insight(
                        event,
                        run_context=run_context,
                        logger=logger,
                        llm_requests_used=llm_requests_used,
                        llm_daily_requests_used=llm_daily_requests_used,
                    )
                    self.store.save_event_insight(insight, run_id=run_context.run_id)
                except Exception as exc:
                    run_context.metrics.extraction_failures += 1
                    logger.error(
                        "extraction_failed",
                        "Insight extraction failed for event.",
                        stage="extract",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"error": exc.__class__.__name__},
                        )
                    continue
                prewatch_context = self._load_recent_prewatch_candidate(event.symbol)
                for horizon in self.settings.horizons:
                    try:
                        snapshot_key = (event.symbol, horizon)
                        snapshot = snapshot_cache.get(snapshot_key)
                        if snapshot is None:
                            snapshot = self.market_data.snapshot(event.symbol, horizon)
                            snapshot_cache[snapshot_key] = snapshot
                        if snapshot_key not in saved_snapshot_keys:
                            self.store.save_indicator_snapshot(snapshot)
                            saved_snapshot_keys.add(snapshot_key)
                    except Exception as exc:
                        run_context.metrics.market_data_failures += 1
                        logger.error(
                            "market_data_failed",
                            "Market data snapshot failed.",
                            stage="market_data",
                            symbol=event.symbol,
                            event_id=event.event_id,
                            context={"horizon": horizon, "error": exc.__class__.__name__},
                        )
                        degraded_card = self.scorer.score_event_only(
                            insight,
                            horizon,
                            failure_reason=exc.__class__.__name__,
                        )
                        degraded_market_assessment = self.market_engine.score_event_only(
                            insight,
                            horizon,
                            failure_reason=exc.__class__.__name__,
                        )[1]
                        if degraded_card.priority != "suppressed":
                            degraded_card = self._apply_prewatch_promotion(
                                degraded_card,
                                insight=insight,
                                prewatch_context=prewatch_context,
                            )
                            self.store.save_opportunity_card(degraded_card, run_id=run_context.run_id)
                            run_context.metrics.cards_generated += 1
                            notification_candidates.append(degraded_card)
                            card_contexts[degraded_card.card_id] = {
                                "event_assessment": event_assessment,
                                "market_assessment": degraded_market_assessment,
                            }
                            if degraded_card.promoted_from_prewatch:
                                logger.info(
                                    "prewatch_promoted",
                                    "Prewatch candidate upgraded into the confirmation pool.",
                                    stage="score",
                                    symbol=event.symbol,
                                    event_id=event.event_id,
                                    context={
                                        "horizon": horizon,
                                        "event_score": degraded_card.event_score,
                                        "prewatch_score": degraded_card.prewatch_score,
                                        "prewatch_setup_type": degraded_card.prewatch_setup_type,
                                    },
                                )
                            logger.warning(
                                "event_only_card_generated",
                                "Generated an event-only degraded card because market data was unavailable.",
                                stage="score",
                                symbol=event.symbol,
                                event_id=event.event_id,
                                context={
                                    "horizon": horizon,
                                    "event_score": degraded_card.event_score,
                                    "priority": degraded_card.priority,
                                },
                            )
                        continue
                    try:
                        card, market_assessment = self.market_engine.score_confirmation(insight, snapshot)
                        card = self._apply_prewatch_promotion(
                            card,
                            insight=insight,
                            prewatch_context=prewatch_context,
                        )
                        self.store.save_opportunity_card(card, run_id=run_context.run_id)
                        run_context.metrics.cards_generated += 1
                        notification_candidates.append(card)
                        card_contexts[card.card_id] = {
                            "event_assessment": event_assessment,
                            "market_assessment": market_assessment,
                        }
                        if card.promoted_from_prewatch:
                            logger.info(
                                "prewatch_promoted",
                                "Prewatch candidate upgraded into the confirmation pool.",
                                stage="score",
                                symbol=event.symbol,
                                event_id=event.event_id,
                                context={
                                    "horizon": horizon,
                                    "final_score": card.final_score,
                                    "priority": card.priority,
                                    "prewatch_score": card.prewatch_score,
                                    "prewatch_setup_type": card.prewatch_setup_type,
                                },
                            )
                    except Exception as exc:
                        run_context.metrics.scoring_failures += 1
                        logger.error(
                            "scoring_failed",
                            "Card scoring or enrichment failed.",
                            stage="score",
                            symbol=event.symbol,
                            event_id=event.event_id,
                            context={"horizon": horizon, "error": exc.__class__.__name__},
                        )
                        continue
            notification_candidates = self._apply_theme_linkage_to_confirmation_candidates(
                notification_candidates
            )
            confirmation_packets = self._build_confirmation_decision_packets(
                notification_candidates,
                card_contexts=card_contexts,
            )
            notification_candidates = self._apply_confirmation_packets(
                notification_candidates,
                confirmation_packets,
            )
            for card in notification_candidates:
                self.store.save_opportunity_card(card, run_id=run_context.run_id)
            self._record_decision_packets(confirmation_packets, run_id=run_context.run_id)
            self._dispatch_notifications(notification_candidates, run_context, logger)
            prewatch_candidates = self._build_prewatch_candidates(
                watchlist,
                confirmation_cards=notification_candidates,
                snapshot_cache=snapshot_cache,
                saved_snapshot_keys=saved_snapshot_keys,
                run_context=run_context,
                logger=logger,
            )
            prewatch_packets = self._build_prewatch_decision_packets(
                prewatch_candidates,
                confirmation_cards=notification_candidates,
            )
            self._record_decision_packets(prewatch_packets, run_id=run_context.run_id)
            prewatch_alert_symbols = self._dispatch_prewatch_notifications(
                prewatch_candidates,
                run_context=run_context,
                logger=logger,
            )
            self.store.set_state("last_event_poll_at", newest_seen.isoformat())
        except Exception as exc:
            status = "failed"
            logger.error(
                "run_failed",
                "Satellite agent run aborted with an unexpected error.",
                stage="run",
                context={"error": exc.__class__.__name__},
            )
            raise
        finally:
            finished_at = utcnow()
            summary = run_context.metrics.as_dict()
            summary["last_event_poll_at"] = newest_seen.isoformat()
            summary["prewatch_candidates_count"] = len(prewatch_candidates)
            summary["prewatch_candidates"] = [candidate.to_record() for candidate in prewatch_candidates]
            summary["prewatch_alerts_sent_count"] = run_context.metrics.prewatch_alerts_sent
            summary["prewatch_alert_symbols"] = list(prewatch_alert_symbols)
            if status == "success":
                self.theme_engine.persist_theme_memory(notification_candidates, prewatch_candidates)
            self.store.record_run(
                run_id=run_context.run_id,
                started_at=run_context.started_at,
                finished_at=finished_at,
                status=status,
                run_name=self.run_name,
                note=self.note,
                config_snapshot=self.runtime_snapshot,
                summary=summary,
            )
            logger.info(
                "run_finished",
                "Satellite agent run finished.",
                stage="run",
                context={"status": status, **summary},
            )
        return {
            "events_processed": run_context.metrics.events_processed,
            "cards_generated": run_context.metrics.cards_generated,
            "alerts_sent": run_context.metrics.alerts_sent,
            "prewatch_candidates": run_context.metrics.prewatch_candidates,
            "prewatch_alerts_sent": run_context.metrics.prewatch_alerts_sent,
        }

    def _extract_insight(
        self,
        event,
        *,
        run_context: RunContext,
        logger: StructuredLogger,
        llm_requests_used: int,
        llm_daily_requests_used: int,
    ) -> tuple[object, object, int, int]:
        allow_llm = isinstance(self.extractor, OpenAIExtractor) and bool(self.settings.openai_api_key)

        budget_reason = self._llm_budget_reason(
            llm_requests_used=llm_requests_used,
            llm_daily_requests_used=llm_daily_requests_used,
        )
        if budget_reason:
            run_context.metrics.llm_budget_skips += 1
            insight, assessment, _ = self.event_engine.understand(event, allow_llm=False)
            self.store.record_llm_usage(
                run_id=run_context.run_id,
                event_id=event.event_id,
                symbol=event.symbol,
                model=self.settings.openai_model,
                used_llm=False,
                success=False,
                reason=budget_reason,
                created_at=utcnow().isoformat(),
            )
            logger.info(
                "llm_skipped",
                "LLM extraction skipped due to active request budget.",
                stage="extract",
                symbol=event.symbol,
                event_id=event.event_id,
                context={"reason": budget_reason},
            )
            return insight, assessment, llm_requests_used, llm_daily_requests_used

        insight, assessment, metadata = self.event_engine.understand(event, allow_llm=allow_llm)
        used_llm = bool(metadata.get("used_llm"))
        success = bool(metadata.get("success"))
        prompt_tokens = int(metadata.get("prompt_tokens_estimate", 0))
        completion_tokens = int(metadata.get("completion_tokens_estimate", 0))
        reason = str(metadata.get("reason", ""))
        if used_llm:
            llm_requests_used += 1
            llm_daily_requests_used += 1
            run_context.metrics.llm_requests += 1
        if used_llm and not success:
            run_context.metrics.llm_fallbacks += 1
            logger.warning(
                "llm_fallback",
                "LLM extraction failed and fell back to rule-based extraction.",
                stage="extract",
                symbol=event.symbol,
                event_id=event.event_id,
                context={"reason": reason},
            )
        self.store.record_llm_usage(
            run_id=run_context.run_id,
            event_id=event.event_id,
            symbol=event.symbol,
            model=self.settings.openai_model,
            used_llm=used_llm,
            success=success,
            prompt_tokens_estimate=prompt_tokens,
            completion_tokens_estimate=completion_tokens,
            reason=reason,
            created_at=utcnow().isoformat(),
        )
        return insight, assessment, llm_requests_used, llm_daily_requests_used

    def _llm_budget_reason(self, *, llm_requests_used: int, llm_daily_requests_used: int) -> str:
        if self.settings.llm_max_requests_per_run > 0 and llm_requests_used >= self.settings.llm_max_requests_per_run:
            return "per_run_budget_exhausted"
        if self.settings.llm_max_requests_per_day > 0 and llm_daily_requests_used >= self.settings.llm_max_requests_per_day:
            return "daily_budget_exhausted"
        return ""

    def _llm_daily_usage_count(self, now: datetime) -> int:
        local_now = now.astimezone(BEIJING_TZ)
        start_of_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return self.store.count_llm_usage_since(start_of_day.astimezone(now.tzinfo).isoformat())

    def run_forever(self) -> None:
        while True:
            self.run_once()
            time.sleep(self.settings.poll_seconds)

    def _create_run_context(self) -> RunContext:
        started_at = utcnow()
        digest = hashlib.sha1(started_at.isoformat().encode("utf-8")).hexdigest()
        return RunContext(run_id=digest[:16], started_at=started_at)

    def _fetch_events_from_sources(
        self,
        since: datetime,
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        events = []
        adapters = self.source_adapter.child_adapters()
        if not adapters:
            return events
        cached_health = {
            adapter.name: self._load_cached_source_health(adapter.name)
            for adapter in adapters
        }

        with ThreadPoolExecutor(max_workers=min(len(adapters), 4)) as executor:
            futures = {
                executor.submit(self._fetch_from_adapter, adapter, since, cached_health.get(adapter.name)): adapter
                for adapter in adapters
            }
            for future in as_completed(futures):
                adapter = futures[future]
                result = future.result()
                health = result.get("health")
                health_derived_from_fetch = result.get("health_derived_from_fetch", False)
                if health is None:
                    run_context.metrics.source_health_failures += 1
                    logger.error(
                        "source_health",
                        "Source health check raised an unexpected error.",
                        stage="source_health",
                        context={
                            "source_name": adapter.name,
                            "error": result.get("health_error", "UnknownError"),
                            "detail": result.get("health_detail", ""),
                        },
                    )
                    continue
                self.store.record_source_health(run_context.run_id, health)
                if health.status == "healthy":
                    logger.info(
                        "source_health",
                        (
                            "Source fetch probe passed."
                            if health_derived_from_fetch
                            else "Source health check passed."
                        ),
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                else:
                    if not health_derived_from_fetch:
                        run_context.metrics.source_health_failures += 1
                    logger.error(
                        "source_health",
                        (
                            "Source fetch probe failed."
                            if health_derived_from_fetch
                            else "Source health check failed."
                        ),
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                    if not health_derived_from_fetch and not adapter.should_fetch_after_unhealthy_healthcheck():
                        continue
                    if not health_derived_from_fetch:
                        logger.warning(
                            "source_health_degraded_fetch",
                            "Source health check failed, but fetch will still be attempted.",
                            stage="source_health",
                            context={
                                "source_name": health.source_name,
                                "status": health.status,
                                "detail": health.detail,
                                "latency_ms": health.latency_ms,
                            },
                        )
                fetch_error = result.get("fetch_error")
                if health.status == "healthy" and not result.get("health_from_cache"):
                    self._save_cached_source_health(health)
                if fetch_error:
                    run_context.metrics.source_fetch_failures += 1
                    logger.error(
                        "source_fetch_failed",
                        (
                            "Source fetch failed during fetch probe."
                            if health_derived_from_fetch
                            else "Source fetch failed after health check."
                        ),
                        stage="source_fetch",
                        context={
                            "source_name": adapter.name,
                            "error": result.get("fetch_error_type", "UnknownError"),
                            "detail": fetch_error,
                        },
                    )
                    continue
                adapter_events = result.get("events", [])
                events.extend(adapter_events)
                logger.info(
                    "source_fetch_completed",
                    "Source fetch completed.",
                    stage="source_fetch",
                    context={"source_name": adapter.name, "event_count": len(adapter_events)},
                )
                partial_errors = result.get("partial_errors", [])
                if partial_errors:
                    logger.warning(
                        "source_fetch_partial",
                        "Source fetch completed with partial per-symbol failures.",
                        stage="source_fetch",
                        context={"source_name": adapter.name, "errors": partial_errors},
                    )
        return sorted(events, key=lambda event: event.published_at)

    def _deduplicate_fetched_events(self, events: list) -> list:
        seen_ids: set[str] = set()
        deduped = []
        for event in sorted(events, key=lambda item: item.published_at):
            normalized = self.normalizer.normalize(event)
            if normalized.event_id in seen_ids:
                continue
            seen_ids.add(normalized.event_id)
            deduped.append(normalized)
        return deduped

    def _limit_fetched_events(self, events: list) -> list:
        if self.settings.max_events_per_run <= 0:
            return events
        limited = []
        symbol_counts: dict[str, int] = {}
        ranked_events = self._rank_events_for_selection(events)
        for event in ranked_events:
            if self.settings.max_events_per_symbol_per_run > 0:
                current = symbol_counts.get(event.symbol, 0)
                if current >= self.settings.max_events_per_symbol_per_run:
                    continue
                symbol_counts[event.symbol] = current + 1
            limited.append(event)
            if len(limited) >= self.settings.max_events_per_run:
                break
        return sorted(limited, key=lambda item: item.published_at)

    def _limit_deep_processing_events(self, events: list) -> list:
        if self.settings.max_deep_events_per_run <= 0 or len(events) <= self.settings.max_deep_events_per_run:
            return events
        ranked = self._rank_events_for_selection(events)
        limit = self.settings.max_deep_events_per_run
        reserved = self._reserve_theme_representative_events(ranked, limit)
        reserved_ids = {event.event_id for event in reserved}
        trimmed = list(reserved)
        for event in ranked:
            if event.event_id in reserved_ids:
                continue
            trimmed.append(event)
            if len(trimmed) >= limit:
                break
        return sorted(trimmed, key=lambda item: item.published_at)

    def _rank_events_for_selection(self, events: list) -> list:
        theme_heat = self._build_batch_event_theme_heat(events)
        return sorted(
            events,
            key=lambda item: (
                self._event_selection_priority(item) + theme_heat.get(item.symbol.upper(), 0.0),
                item.published_at,
            ),
            reverse=True,
        )

    def _event_selection_priority(self, event) -> float:
        text = f"{event.headline} {event.summary}".lower()
        score = SOURCE_TYPE_SELECTION_WEIGHTS.get(event.source_type, 0.0)
        for term, weight in STRONG_SELECTION_TERMS:
            if term in text:
                score += weight
        for term, weight in LOW_SIGNAL_SELECTION_TERMS:
            if term in text:
                score += weight
        if any(token in text for token in ("ai", "data center", "chip", "gpu", "cloud")):
            score += 6.0
        if event.symbol in self.prewatch_symbols:
            score += 4.0
        theme_context = summarize_symbol_theme_context(
            event.symbol,
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
        )
        if theme_context["peer_count"] > 0:
            score += min(theme_context["peer_count"], 3) * THEME_SELECTION_BONUS_PER_PEER
        return score

    def _build_batch_event_theme_heat(self, events: list) -> dict[str, float]:
        theme_counts = self._build_batch_theme_symbol_counts(events)
        if not theme_counts:
            return {}

        symbol_heat: dict[str, float] = {}
        for symbol, theme_keys in self.symbol_theme_map.items():
            max_bonus = 0.0
            for theme_key in theme_keys:
                symbol_count = len(theme_counts.get(theme_key, set()))
                if symbol_count < 2:
                    continue
                bonus = min(
                    (symbol_count - 1) * THEME_BATCH_CLUSTER_BONUS_PER_SYMBOL,
                    THEME_BATCH_CLUSTER_MAX_BONUS,
                )
                if bonus > max_bonus:
                    max_bonus = bonus
            if max_bonus > 0:
                symbol_heat[symbol] = max_bonus
        return symbol_heat

    def _build_batch_theme_symbol_counts(self, events: list) -> dict[str, set[str]]:
        if not events or not self.symbol_theme_map:
            return {}
        theme_counts: dict[str, set[str]] = {}
        for event in events:
            symbol = event.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_counts.setdefault(theme_key, set()).add(symbol)
        return theme_counts

    def _reserve_theme_representative_events(self, ranked_events: list, limit: int) -> list:
        reserve_budget = min(self.settings.max_deep_theme_reservations_per_run, limit)
        if reserve_budget <= 0 or not ranked_events:
            return []
        theme_counts = self._build_batch_theme_symbol_counts(ranked_events)
        hot_themes = sorted(
            (
                theme_key
                for theme_key, symbols in theme_counts.items()
                if len(symbols) >= 2
            ),
            key=lambda theme_key: (
                len(theme_counts[theme_key]),
                theme_key,
            ),
            reverse=True,
        )
        if not hot_themes:
            return []

        reserved = []
        reserved_ids: set[str] = set()
        reserved_symbols: set[str] = set()
        for theme_key in hot_themes:
            for event in ranked_events:
                symbol = event.symbol.upper()
                if event.event_id in reserved_ids or symbol in reserved_symbols:
                    continue
                if theme_key not in self.symbol_theme_map.get(symbol, []):
                    continue
                reserved.append(event)
                reserved_ids.add(event.event_id)
                reserved_symbols.add(symbol)
                break
            if len(reserved) >= reserve_budget:
                break
        return reserved

    def _dispatch_notifications(self, cards: list, run_context: RunContext, logger: StructuredLogger) -> None:
        alerts_sent = 0
        symbol_alert_counts: dict[str, int] = {}
        for card in self._rank_notification_candidates(cards):
            if card.priority == "suppressed":
                decision = self.notifier.skip(card, reason="threshold_not_met", run_id=run_context.run_id)
            elif (
                card.priority != "high"
                and card.final_score < self.settings.normal_alert_min_final_score
            ):
                decision = self.notifier.skip(card, reason="quality_cutoff", run_id=run_context.run_id)
            elif (
                self.settings.max_alerts_per_run > 0
                and alerts_sent >= self.settings.max_alerts_per_run
            ):
                decision = self.notifier.skip(
                    card,
                    reason="run_alert_budget_exhausted",
                    run_id=run_context.run_id,
                )
            elif (
                self.settings.max_alerts_per_symbol_per_run > 0
                and symbol_alert_counts.get(card.symbol, 0) >= self.settings.max_alerts_per_symbol_per_run
            ):
                decision = self.notifier.skip(
                    card,
                    reason="symbol_alert_budget_exhausted",
                    run_id=run_context.run_id,
                )
            else:
                decision = self.notifier.send(card, run_id=run_context.run_id)
            if decision.sent:
                alerts_sent += 1
                symbol_alert_counts[card.symbol] = symbol_alert_counts.get(card.symbol, 0) + 1
                run_context.metrics.alerts_sent += 1
            self._log_notification_decision(card, decision, run_context, logger)

    def _rank_notification_candidates(self, cards: list) -> list:
        priority_rank = {"high": 0, "normal": 1, "suppressed": 2}
        theme_heat = self._build_symbol_theme_heat(cards)
        return sorted(
            cards,
            key=lambda card: (
                priority_rank.get(card.priority, 3),
                0 if getattr(card, "promoted_from_prewatch", False) else 1,
                -theme_heat.get(card.symbol.upper(), 0.0),
                -card.final_score,
                -card.event_score,
                card.symbol,
                card.horizon,
            ),
        )

    def _log_notification_decision(
        self,
        card,
        decision,
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> None:
        if decision.sent:
            logger.info(
                "alert_sent",
                "Opportunity alert delivered.",
                stage="notify",
                symbol=card.symbol,
                event_id=card.event_id,
                context={"horizon": card.horizon, "priority": decision.priority, "reason": decision.reason},
            )
            return
        if decision.reason.startswith("transport_error:"):
            run_context.metrics.notification_failures += 1
            logger.error(
                "alert_failed",
                "Opportunity alert failed to deliver.",
                stage="notify",
                symbol=card.symbol,
                event_id=card.event_id,
                context={"horizon": card.horizon, "reason": decision.reason},
            )
            return
        logger.info(
            "alert_skipped",
            "Opportunity alert not sent.",
            stage="notify",
            symbol=card.symbol,
            event_id=card.event_id,
            context={"horizon": card.horizon, "reason": decision.reason},
        )

    def _build_prewatch_candidates(
        self,
        watchlist: set[str],
        *,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        if not watchlist:
            return []
        candidates = []
        near_miss_snapshots: dict[str, object] = {}
        horizon = "position"
        horizon_settings = self.settings.horizons[horizon]
        scan_symbols = self._select_prewatch_scan_symbols(watchlist, snapshot_cache)
        max_workers = min(len(scan_symbols), max(self.settings.prewatch_concurrency, 1))
        with ThreadPoolExecutor(max_workers=max_workers or 1) as executor:
            futures = {
                executor.submit(
                    self._build_single_prewatch_candidate,
                    symbol,
                    horizon,
                    horizon_settings,
                    snapshot_cache,
                ): symbol
                for symbol in scan_symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                snapshot_key = (symbol, horizon)
                try:
                    snapshot, candidate = future.result()
                except Exception as exc:
                    self._record_prewatch_failure(symbol)
                    run_context.metrics.prewatch_failures += 1
                    logger.warning(
                        "prewatch_failed",
                        "Prewatch candidate generation failed for symbol.",
                        stage="prewatch",
                        symbol=symbol,
                        context={"error": exc.__class__.__name__},
                    )
                    continue
                self._clear_prewatch_failure(symbol)
                snapshot_cache[snapshot_key] = snapshot
                if snapshot_key not in saved_snapshot_keys:
                    self.store.save_indicator_snapshot(snapshot)
                    saved_snapshot_keys.add(snapshot_key)
                if candidate is None:
                    near_miss_snapshots[symbol] = snapshot
                    continue
                candidates.append(candidate)
        if near_miss_snapshots:
            candidates.extend(
                self._build_theme_supported_prewatch_candidates(
                    near_miss_snapshots,
                    horizon_settings=horizon_settings,
                    confirmation_cards=confirmation_cards,
                    existing_symbols={candidate.symbol.upper() for candidate in candidates},
                )
            )
        event_supported_candidates = self._build_event_supported_prewatch_candidates(
            watchlist,
            horizon_settings=horizon_settings,
            confirmation_cards=confirmation_cards,
            snapshot_cache=snapshot_cache,
            saved_snapshot_keys=saved_snapshot_keys,
            existing_symbols={candidate.symbol.upper() for candidate in candidates},
            run_context=run_context,
            logger=logger,
        )
        if event_supported_candidates:
            candidates.extend(event_supported_candidates)
        candidates = self._apply_theme_linkage_to_prewatch_candidates(
            candidates,
            confirmation_cards=confirmation_cards,
        )
        ranked = sort_prewatch_candidates(
            candidates,
            max_candidates=self.settings.max_prewatch_candidates_per_run,
        )
        run_context.metrics.prewatch_candidates = len(ranked)
        for candidate in ranked:
            self._record_prewatch_candidate(candidate)
            logger.info(
                "prewatch_selected",
                "Symbol selected into the prewatch candidate pool.",
                stage="prewatch",
                symbol=candidate.symbol,
                context={
                    "horizon": candidate.horizon,
                    "setup_type": candidate.setup_type,
                    "score": candidate.score,
                },
            )
        return ranked

    def _build_single_prewatch_candidate(
        self,
        symbol: str,
        horizon: str,
        horizon_settings,
        snapshot_cache: dict[tuple[str, str], object],
    ):
        snapshot_key = (symbol, horizon)
        snapshot = snapshot_cache.get(snapshot_key)
        if snapshot is None:
            snapshot = self.prewatch_market_data.snapshot(symbol, horizon, include_intraday=False)
        candidate = build_prewatch_candidate(
            snapshot,
            horizon_settings,
            min_score=self.settings.prewatch_min_score,
        )
        return snapshot, candidate

    def _select_prewatch_scan_symbols(
        self,
        watchlist: set[str],
        snapshot_cache: dict[tuple[str, str], object],
    ) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        current_symbols = [symbol.upper() for symbol, horizon in snapshot_cache if horizon == "position"]

        def append_symbol(symbol: str) -> None:
            normalized = symbol.upper()
            if normalized not in watchlist or normalized in seen:
                return
            if not self._is_prewatch_symbol_eligible(normalized):
                return
            if self._is_prewatch_failure_cooled_down(normalized):
                return
            seen.add(normalized)
            ordered.append(normalized)

        for symbol in current_symbols:
            append_symbol(symbol)
        for symbol in self.prewatch_symbols:
            append_symbol(symbol)
        remaining = sorted(
            (symbol for symbol in watchlist if symbol.upper() not in seen),
            key=lambda symbol: (
                self._prewatch_theme_scan_priority(symbol),
                symbol,
            ),
            reverse=True,
        )
        for symbol in remaining:
            append_symbol(symbol)
        cap = self.settings.max_prewatch_scan_symbols_per_run
        if cap > 0:
            return ordered[:cap]
        return ordered

    def _prewatch_theme_scan_priority(self, symbol: str) -> float:
        normalized = symbol.upper()
        max_score = 0
        for theme_key in self.symbol_theme_map.get(normalized, []):
            member_count = len(self.theme_memberships.get(theme_key, set()))
            max_score = max(max_score, member_count)
        return max_score + self._recent_theme_scan_bonus(normalized)

    def _is_prewatch_symbol_eligible(self, symbol: str) -> bool:
        normalized = symbol.upper()
        if "." in normalized or "-" in normalized:
            return False
        if len(normalized) == 5 and normalized.endswith(("Y", "F")):
            return False
        return True

    def _is_prewatch_failure_cooled_down(self, symbol: str) -> bool:
        raw = self.store.get_state(self._prewatch_failure_state_key(symbol))
        if not raw:
            return False
        try:
            previous = datetime.fromisoformat(raw)
        except ValueError:
            return False
        cooldown_seconds = self.settings.prewatch_failure_cooldown_minutes * 60
        return (utcnow() - previous).total_seconds() < cooldown_seconds

    def _record_prewatch_failure(self, symbol: str) -> None:
        self.store.set_state(self._prewatch_failure_state_key(symbol), utcnow().isoformat())

    def _clear_prewatch_failure(self, symbol: str) -> None:
        self.store.set_state(self._prewatch_failure_state_key(symbol), "")

    def _dispatch_prewatch_notifications(
        self,
        candidates,
        *,
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list[str]:
        if not candidates or not self.notifier.transport or self.notifier.dry_run:
            return []
        sent_symbols: list[str] = []
        for candidate in candidates:
            if candidate.score < self.settings.prewatch_alert_min_score:
                continue
            if (
                self.settings.max_prewatch_alerts_per_run > 0
                and len(sent_symbols) >= self.settings.max_prewatch_alerts_per_run
            ):
                break
            if self._is_prewatch_alert_cooled_down(candidate.symbol):
                logger.info(
                    "prewatch_alert_skipped",
                    "Prewatch light alert skipped due to cooldown.",
                    stage="prewatch_notify",
                    symbol=candidate.symbol,
                    context={"reason": "cooldown_active"},
                )
                continue
            title = f"[预备池] {candidate.symbol} {_display_horizon(candidate.horizon)}观察"
            body = "\n".join(
                [
                    f"分数：{candidate.score:.2f}",
                    f"摘要：{candidate.headline_summary}",
                    f"建议：{candidate.action_hint}",
                    f"关注理由：{candidate.reason_to_watch}",
                    f"状态：现价 {candidate.last_price:.2f} / RSI {candidate.rsi_14:.1f} / 相对量能 {candidate.relative_volume:.2f} 倍",
                ]
            )
            try:
                self.notifier.transport.send(title, body)
            except Exception as exc:
                run_context.metrics.notification_failures += 1
                logger.error(
                    "prewatch_alert_failed",
                    "Prewatch light alert failed to deliver.",
                    stage="prewatch_notify",
                    symbol=candidate.symbol,
                    context={"error": exc.__class__.__name__},
                )
                continue
            self.store.set_state(self._prewatch_alert_state_key(candidate.symbol), utcnow().isoformat())
            run_context.metrics.prewatch_alerts_sent += 1
            sent_symbols.append(candidate.symbol)
            logger.info(
                "prewatch_alert_sent",
                "Prewatch light alert delivered.",
                stage="prewatch_notify",
                symbol=candidate.symbol,
                context={"score": candidate.score, "horizon": candidate.horizon},
            )
        return sent_symbols

    def _build_theme_supported_prewatch_candidates(
        self,
        snapshots: dict[str, object],
        *,
        horizon_settings,
        confirmation_cards: list,
        existing_symbols: set[str],
    ) -> list:
        if not snapshots or not self.symbol_theme_map:
            return []
        recent_theme_memory = self._load_recent_theme_heat_memory()
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        if not confirmed_symbols and not recent_theme_memory:
            return []
        relaxed_min_score = max(
            self.settings.prewatch_min_score - self.settings.prewatch_theme_relaxed_margin,
            0.0,
        )
        candidates = []
        for symbol, snapshot in snapshots.items():
            normalized = symbol.upper()
            if normalized in existing_symbols:
                continue
            context = summarize_symbol_theme_context(
                normalized,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
                prewatch_symbols=existing_symbols,
            )
            if not context["confirmed_peer_symbols"] and not self._strongest_recent_theme_for_symbol(
                normalized,
                recent_theme_memory,
            ):
                continue
            candidate = build_prewatch_candidate(
                snapshot,
                horizon_settings,
                min_score=relaxed_min_score,
            )
            if candidate is None:
                continue
            candidates.append(candidate)
        return candidates

    def _build_event_supported_prewatch_candidates(
        self,
        watchlist: set[str],
        *,
        horizon_settings,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        existing_symbols: set[str],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        if not watchlist or not confirmation_cards or not self.symbol_theme_map:
            return []
        active_themes = self._build_event_prewatch_theme_seeds(confirmation_cards)
        if not active_themes:
            return []
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        candidates = []
        for theme_key, seed in active_themes:
            theme_members = sorted(self.theme_memberships.get(theme_key, set()))
            added_for_theme = 0
            for symbol in theme_members:
                normalized = symbol.upper()
                if normalized not in watchlist:
                    continue
                if normalized in existing_symbols or normalized in confirmed_symbols:
                    continue
                if not self._is_prewatch_symbol_eligible(normalized):
                    continue
                if self._is_prewatch_failure_cooled_down(normalized):
                    continue
                snapshot_key = (normalized, "position")
                snapshot = snapshot_cache.get(snapshot_key)
                try:
                    if snapshot is None:
                        snapshot = self.prewatch_market_data.snapshot(
                            normalized,
                            "position",
                            include_intraday=False,
                        )
                        snapshot_cache[snapshot_key] = snapshot
                    if snapshot_key not in saved_snapshot_keys:
                        self.store.save_indicator_snapshot(snapshot)
                        saved_snapshot_keys.add(snapshot_key)
                except Exception as exc:
                    self._record_prewatch_failure(normalized)
                    run_context.metrics.prewatch_failures += 1
                    logger.warning(
                        "prewatch_failed",
                        "Event-driven prewatch candidate generation failed for symbol.",
                        stage="prewatch",
                        symbol=normalized,
                        context={"error": exc.__class__.__name__, "mode": "event"},
                    )
                    continue
                self._clear_prewatch_failure(normalized)
                base_candidate = build_prewatch_candidate(
                    snapshot,
                    horizon_settings,
                    min_score=0.0,
                )
                if base_candidate is None:
                    continue
                boosted_score = round(base_candidate.score + seed["bonus"], 2)
                if boosted_score < self.settings.prewatch_event_min_score:
                    continue
                trigger_symbols = list(seed["trigger_symbols"])[:3]
                theme_name = display_theme_name(theme_key)
                event_type_label = EVENT_TYPE_DISPLAY_NAMES.get(seed["event_type"], "事件")
                headline_summary = (
                    f"{normalized} 受到{theme_name}题材事件催化带动，适合先加入观察名单，"
                    "等待个股结构和量价跟上。"
                )
                action_hint = (
                    "题材出现事件催化，可先加入观察；若后续出现回踩承接、放量突破或新增催化，"
                    "再考虑升入确认池。"
                )
                reason_note = (
                    f"事件预热：{theme_name} 题材出现{event_type_label}催化，"
                    f"触发标的 {', '.join(trigger_symbols)}。"
                )
                reason_to_watch = (
                    f"{base_candidate.reason_to_watch}；{reason_note}"
                    if base_candidate.reason_to_watch
                    else reason_note
                )
                candidates.append(
                    replace(
                        base_candidate,
                        score=boosted_score,
                        headline_summary=headline_summary,
                        action_hint=action_hint,
                        reason_to_watch=reason_to_watch,
                        trigger_mode="event",
                        trigger_event_type=seed["event_type"],
                        trigger_theme=theme_name,
                        trigger_symbols=trigger_symbols,
                    )
                )
                existing_symbols.add(normalized)
                added_for_theme += 1
                if (
                    self.settings.max_event_prewatch_symbols_per_theme_per_run > 0
                    and added_for_theme >= self.settings.max_event_prewatch_symbols_per_theme_per_run
                ):
                    break
        return candidates

    def _build_event_prewatch_theme_seeds(self, confirmation_cards: list) -> list[tuple[str, dict]]:
        seeds: dict[str, dict] = {}
        min_event_score = self.settings.prewatch_event_trigger_min_event_score
        for card in confirmation_cards:
            if card.priority == "suppressed":
                continue
            if card.event_score < min_event_score:
                continue
            symbol = card.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                seed = seeds.get(theme_key)
                if seed is None:
                    seed = {
                        "theme_key": theme_key,
                        "trigger_symbols": set(),
                        "event_type": card.event_type,
                        "max_event_score": 0.0,
                        "bonus": 0.0,
                    }
                    seeds[theme_key] = seed
                seed["trigger_symbols"].add(symbol)
                if card.event_score >= seed["max_event_score"]:
                    seed["max_event_score"] = card.event_score
                    seed["event_type"] = card.event_type
        ranked = []
        for theme_key, seed in seeds.items():
            event_bonus = min(
                EVENT_PREWATCH_BASE_BONUS
                + max(seed["max_event_score"] - min_event_score, 0.0) / 2.0,
                self.settings.prewatch_event_bonus_cap,
            )
            seed["bonus"] = round(event_bonus, 2)
            ranked.append((theme_key, seed))
        ranked.sort(
            key=lambda item: (
                -float(item[1]["max_event_score"]),
                -len(item[1]["trigger_symbols"]),
                item[0],
            )
        )
        return ranked

    def _is_prewatch_alert_cooled_down(self, symbol: str) -> bool:
        raw = self.store.get_state(self._prewatch_alert_state_key(symbol))
        if not raw:
            return False
        try:
            previous = datetime.fromisoformat(raw)
        except ValueError:
            return False
        return (utcnow() - previous).total_seconds() < self.settings.prewatch_alert_cooldown_minutes * 60

    def _prewatch_alert_state_key(self, symbol: str) -> str:
        return f"prewatch_alert:{symbol.upper()}"

    def _prewatch_failure_state_key(self, symbol: str) -> str:
        return f"prewatch_failure:{symbol.upper()}"

    def _prewatch_candidate_state_key(self, symbol: str) -> str:
        return f"prewatch_candidate:{symbol.upper()}"

    def _record_prewatch_candidate(self, candidate) -> None:
        self.store.set_state(
            self._prewatch_candidate_state_key(candidate.symbol),
            json.dumps(candidate.to_record(), sort_keys=True),
        )

    def _load_recent_prewatch_candidate(self, symbol: str) -> dict | None:
        raw = self.store.get_state(self._prewatch_candidate_state_key(symbol))
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            as_of = datetime.fromisoformat(payload["as_of"])
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return None
        age_seconds = (utcnow() - as_of).total_seconds()
        if age_seconds > max(self.settings.prewatch_promotion_window_hours, 0) * 3600:
            return None
        return payload

    def _apply_prewatch_promotion(self, card, *, insight, prewatch_context: dict | None):
        if not prewatch_context:
            return card
        if card.event_score < self.settings.prewatch_confirmation_min_event_score:
            return card
        setup_type = str(prewatch_context.get("setup_type", ""))
        setup_label = {
            "breakout_watch": "突破预热",
            "pullback_watch": "回踩蓄势",
            "relative_strength_watch": "相对强势",
        }.get(setup_type, setup_type or "预备池")
        positioning_hint = (
            f"该标的此前已进入预备池（{setup_label}，{float(prewatch_context.get('score', 0.0)):.1f} 分），"
            "本次事件触发确认，可从观察/轻仓阶段切换到正式确认。"
        )
        reason_to_watch = card.reason_to_watch
        if positioning_hint not in reason_to_watch:
            reason_to_watch = f"{reason_to_watch} {positioning_hint}".strip()
        promoted_card = replace(
            card,
            promoted_from_prewatch=True,
            prewatch_score=round(float(prewatch_context.get("score", 0.0)), 2),
            prewatch_setup_type=setup_type,
            action_label="确认做多" if card.bias == "long" else card.action_label,
            positioning_hint=positioning_hint,
            reason_to_watch=reason_to_watch,
        )
        if not card.market_data_complete:
            return promoted_card
        horizon_settings = self.settings.horizons[card.horizon]
        if promoted_card.market_score < horizon_settings.market_score_threshold:
            return promoted_card
        bonus = max(self.settings.prewatch_confirmation_bonus, 0.0)
        if bonus <= 0:
            return promoted_card
        boosted_final = round(promoted_card.final_score + bonus, 2)
        boosted_priority = promoted_card.priority
        if boosted_final >= horizon_settings.priority_threshold:
            boosted_priority = "high"
        elif boosted_final >= 60.0 and promoted_card.event_score >= self.settings.event_score_threshold:
            boosted_priority = "normal"
        return replace(
            promoted_card,
            final_score=boosted_final,
            priority=boosted_priority,
        )

    def _apply_theme_linkage_to_prewatch_candidates(self, candidates: list, *, confirmation_cards: list) -> list:
        if not candidates or not self.symbol_theme_map:
            return candidates
        recent_theme_memory = self._load_recent_theme_heat_memory()
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        prewatch_symbols = {candidate.symbol.upper() for candidate in candidates}
        themed_candidates = []
        for candidate in candidates:
            context = summarize_symbol_theme_context(
                candidate.symbol,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
                prewatch_symbols=prewatch_symbols,
            )
            bonus = 0.0
            notes: list[str] = []
            if context["confirmed_peer_symbols"]:
                bonus += THEME_PREWATCH_CONFIRMED_BONUS
                notes.append(f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}")
            elif context["prewatch_peer_symbols"]:
                bonus += THEME_PREWATCH_CLUSTER_BONUS
                notes.append(f"同题材预热共振：{', '.join(context['prewatch_peer_symbols'][:3])}")
            else:
                recent_theme = self._strongest_recent_theme_for_symbol(
                    candidate.symbol,
                    recent_theme_memory,
                )
                if recent_theme is not None:
                    theme_key, entry = recent_theme
                    heat_score = float(entry.get("heat_score", 0.0))
                    memory_bonus = min(
                        max(heat_score, 0.0),
                        self.settings.prewatch_theme_memory_bonus,
                    )
                    if memory_bonus > 0:
                        bonus += memory_bonus
                        notes.append(f"题材近期持续活跃：{display_theme_name(theme_key)}")
            if bonus <= 0:
                themed_candidates.append(candidate)
                continue
            reason_to_watch = candidate.reason_to_watch
            for note in notes:
                if note not in reason_to_watch:
                    reason_to_watch = f"{reason_to_watch}；{note}" if reason_to_watch else note
            themed_candidates.append(
                replace(
                    candidate,
                    score=round(candidate.score + bonus, 2),
                    reason_to_watch=reason_to_watch,
                )
            )
        return themed_candidates

    def _recent_theme_scan_bonus(self, symbol: str) -> float:
        recent_theme = self._strongest_recent_theme_for_symbol(
            symbol,
            self._load_recent_theme_heat_memory(),
        )
        if recent_theme is None:
            return 0.0
        _, entry = recent_theme
        return min(
            max(float(entry.get("heat_score", 0.0)), 0.0),
            self.settings.prewatch_theme_memory_scan_bonus,
        )

    def _strongest_recent_theme_for_symbol(
        self,
        symbol: str,
        recent_theme_memory: dict[str, dict] | None = None,
    ) -> tuple[str, dict] | None:
        if not self.symbol_theme_map:
            return None
        memory = recent_theme_memory or self._load_recent_theme_heat_memory()
        if not memory:
            return None
        strongest: tuple[str, dict] | None = None
        for theme_key in self.symbol_theme_map.get(symbol.upper(), []):
            entry = memory.get(theme_key)
            if entry is None:
                continue
            if float(entry.get("heat_score", 0.0)) < self.settings.prewatch_theme_memory_min_heat_score:
                continue
            if strongest is None or float(entry.get("heat_score", 0.0)) > float(
                strongest[1].get("heat_score", 0.0)
            ):
                strongest = (theme_key, entry)
        return strongest

    def _load_recent_theme_heat_memory(self) -> dict[str, dict]:
        raw = self.store.get_state(THEME_MEMORY_STATE_KEY)
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            as_of = datetime.fromisoformat(payload["as_of"])
            rows = payload.get("themes", [])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return {}
        age_seconds = (utcnow() - as_of).total_seconds()
        if age_seconds > max(self.settings.prewatch_theme_memory_window_hours, 0) * 3600:
            return {}
        memory: dict[str, dict] = {}
        for row in rows:
            theme_key = str(row.get("theme_key", "")).strip()
            if not theme_key:
                continue
            memory[theme_key] = {
                "heat_score": float(row.get("heat_score", 0.0)),
                "confirmed_symbols": list(row.get("confirmed_symbols", [])),
                "prewatch_symbols": list(row.get("prewatch_symbols", [])),
            }
        return memory

    def _persist_theme_heat_memory(self, confirmation_cards: list, prewatch_candidates: list) -> None:
        if not self.symbol_theme_map:
            self.store.set_state(
                THEME_MEMORY_STATE_KEY,
                json.dumps({"as_of": utcnow().isoformat(), "themes": []}, sort_keys=True),
            )
            return
        theme_confirmed: dict[str, set[str]] = {}
        theme_promoted: dict[str, set[str]] = {}
        theme_prewatch: dict[str, set[str]] = {}
        for card in confirmation_cards:
            if card.priority == "suppressed":
                continue
            symbol = card.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_confirmed.setdefault(theme_key, set()).add(symbol)
                if card.promoted_from_prewatch:
                    theme_promoted.setdefault(theme_key, set()).add(symbol)
        for candidate in prewatch_candidates:
            symbol = candidate.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_prewatch.setdefault(theme_key, set()).add(symbol)
        rows = []
        all_theme_keys = set(theme_confirmed) | set(theme_promoted) | set(theme_prewatch)
        for theme_key in sorted(all_theme_keys):
            confirmed_symbols = sorted(theme_confirmed.get(theme_key, set()))
            promoted_symbols = sorted(theme_promoted.get(theme_key, set()))
            prewatch_symbols = sorted(theme_prewatch.get(theme_key, set()))
            heat_score = len(promoted_symbols) * 3 + len(confirmed_symbols) * 2 + len(prewatch_symbols)
            rows.append(
                {
                    "theme_key": theme_key,
                    "heat_score": float(heat_score),
                    "confirmed_symbols": confirmed_symbols,
                    "prewatch_symbols": prewatch_symbols,
                }
            )
        self.store.set_state(
            THEME_MEMORY_STATE_KEY,
            json.dumps(
                {
                    "as_of": utcnow().isoformat(),
                    "themes": rows,
                },
                sort_keys=True,
            ),
        )

    def _apply_theme_linkage_to_confirmation_candidates(self, cards: list) -> list:
        if not cards or not self.symbol_theme_map:
            return cards
        confirmed_symbols = {
            card.symbol.upper()
            for card in cards
            if card.priority != "suppressed"
        }
        adjusted_cards = []
        for card in cards:
            context = summarize_symbol_theme_context(
                card.symbol,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
            )
            base_theme_tags = theme_tags_for_symbol(card.symbol, self.symbol_theme_map)
            if not context["confirmed_peer_symbols"]:
                adjusted_cards.append(
                    replace(
                        card,
                        theme_tags=base_theme_tags,
                        confirmed_peer_symbols=[],
                    )
                )
                continue
            peer_note = f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}"
            reason_to_watch = card.reason_to_watch
            if peer_note not in reason_to_watch:
                reason_to_watch = f"{reason_to_watch} {peer_note}".strip()
            positioning_hint = card.positioning_hint
            if card.promoted_from_prewatch:
                theme_hint = "题材联动正在形成，可按确认链条而不是单点新闻来跟踪仓位。"
                if theme_hint not in positioning_hint:
                    positioning_hint = f"{positioning_hint} {theme_hint}".strip()
            adjusted = replace(
                card,
                reason_to_watch=reason_to_watch,
                positioning_hint=positioning_hint,
                theme_tags=base_theme_tags,
                confirmed_peer_symbols=context["confirmed_peer_symbols"],
            )
            if card.promoted_from_prewatch and card.market_data_complete:
                horizon_settings = self.settings.horizons[card.horizon]
                boosted_final = round(adjusted.final_score + THEME_CONFIRMATION_CHAIN_BONUS, 2)
                boosted_priority = adjusted.priority
                if boosted_final >= horizon_settings.priority_threshold:
                    boosted_priority = "high"
                elif boosted_final >= 60.0 and adjusted.event_score >= self.settings.event_score_threshold:
                    boosted_priority = "normal"
                adjusted = replace(
                    adjusted,
                    final_score=boosted_final,
                    priority=boosted_priority,
                )
            adjusted_cards.append(adjusted)
        return adjusted_cards

    def _build_symbol_theme_heat(self, cards: list) -> dict[str, float]:
        if not cards or not self.symbol_theme_map:
            return {}
        theme_heat: dict[str, float] = {}
        theme_confirmed: dict[str, set[str]] = {}
        theme_promoted: dict[str, set[str]] = {}
        for card in cards:
            symbol = card.symbol.upper()
            if symbol not in self.symbol_theme_map or card.priority == "suppressed":
                continue
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_confirmed.setdefault(theme_key, set()).add(symbol)
                if card.promoted_from_prewatch:
                    theme_promoted.setdefault(theme_key, set()).add(symbol)
        for symbol, theme_keys in self.symbol_theme_map.items():
            score = 0.0
            for theme_key in theme_keys:
                score = max(
                    score,
                    len(theme_confirmed.get(theme_key, set())) * 2.0
                    + len(theme_promoted.get(theme_key, set())) * 2.0,
                )
            if score > 0:
                theme_heat[symbol] = score
        return theme_heat

    def _build_confirmation_decision_packets(
        self,
        cards: list,
        *,
        card_contexts: dict[str, dict],
    ) -> list[DecisionPacket]:
        confirmed_symbols = {
            card.symbol.upper()
            for card in cards
            if card.priority != "suppressed"
        }
        packets: list[DecisionPacket] = []
        for card in cards:
            context = card_contexts.get(card.card_id, {})
            event_assessment = context.get("event_assessment")
            market_assessment = context.get("market_assessment")
            if event_assessment is None or market_assessment is None:
                continue
            market_confirmation_score = market_assessment.market_confirmation_score
            if market_confirmation_score is None:
                market_confirmation_score = card.market_score or 0.0
            rsi_value = (
                market_assessment.rsi_14
                if market_assessment.rsi_14 is not None
                else card.rsi_14
            )
            relative_volume = (
                market_assessment.relative_volume
                if market_assessment.relative_volume is not None
                else card.relative_volume
            )
            theme_assessment = self.theme_engine.assess_confirmation(
                card,
                confirmed_symbols=confirmed_symbols,
            )
            theme_score = round(
                theme_assessment.theme_heat + sum(theme_assessment.theme_boosts.values()),
                2,
            )
            reason_sections = ReasonSections(
                event=event_assessment.headline_summary,
                market=market_assessment.market_data_note
                if not market_assessment.market_data_complete
                else (
                    f"市场确认 {market_confirmation_score:.1f} 分，当前为"
                    f"{_display_trend_state(card.trend_state)}结构，RSI {float(rsi_value or 0.0):.1f}，相对量能 {float(relative_volume or 0.0):.2f} 倍。"
                ),
                theme=theme_assessment.theme_chain_note or (f"题材：{' / '.join(theme_assessment.theme_ids)}" if theme_assessment.theme_ids else ""),
                positioning=(
                    f"{card.positioning_hint} 此前已进入预备池，当前事件与市场确认形成共振。"
                    if card.promoted_from_prewatch
                    else card.positioning_hint
                ),
            )
            price_plan = market_assessment.entry_plan or PricePlan(
                entry_range=card.entry_range,
                take_profit_range=card.take_profit_range,
                invalidation_level=card.invalidation_level,
                invalidation_reason=card.invalidation_reason,
                ttl_iso=card.ttl.isoformat(),
            )
            packets.append(
                DecisionPacket(
                    pool="confirmation",
                    action=card.action_label,
                    priority=card.priority,
                    confidence=card.confidence_label,
                    symbol=card.symbol,
                    horizon=card.horizon,
                    event_id=card.event_id,
                    event_assessment=event_assessment,
                    market_assessment=market_assessment,
                    theme_assessment=theme_assessment,
                    price_plan=price_plan,
                    reason_sections=reason_sections,
                    source_bundle=SourceBundle(
                        refs=list(card.source_refs),
                        quality_tier=event_assessment.source_quality_tier,
                    ),
                    final_score=card.final_score,
                    event_score=card.event_score,
                    market_score=card.market_score,
                    theme_score=theme_score,
                    trigger_mode="promoted" if card.promoted_from_prewatch else "direct",
                    llm_used=event_assessment.llm_used,
                )
            )
        return packets

    def _apply_confirmation_packets(self, cards: list, packets: list[DecisionPacket]) -> list:
        packets_by_key = {(packet.symbol, packet.horizon, packet.event_id): packet for packet in packets}
        updated = []
        for card in cards:
            packet = packets_by_key.get((card.symbol, card.horizon, card.event_id))
            if packet is None:
                updated.append(card)
                continue
            updated.append(decision_packet_to_card(packet, card))
        return updated

    def _build_prewatch_decision_packets(
        self,
        candidates: list,
        *,
        confirmation_cards: list,
    ) -> list[DecisionPacket]:
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        prewatch_symbols = {candidate.symbol.upper() for candidate in candidates}
        packets: list[DecisionPacket] = []
        for candidate in candidates:
            theme_assessment = self.theme_engine.assess_prewatch(
                candidate,
                confirmed_symbols=confirmed_symbols,
                prewatch_symbols=prewatch_symbols,
            )
            event_strength = candidate.score if candidate.trigger_mode == "event" else 0.0
            market_strength = candidate.score if candidate.trigger_mode != "event" else 0.0
            event_assessment = self._synthetic_prewatch_event_assessment(candidate)
            market_assessment = self._synthetic_prewatch_market_assessment(candidate)
            reason_sections = ReasonSections(
                event=event_assessment.headline_summary,
                market=(
                    f"结构状态：{_display_trend_state(candidate.trend_state)}，RSI {candidate.rsi_14:.1f}，"
                    f"相对量能 {candidate.relative_volume:.2f} 倍。"
                ),
                theme=theme_assessment.theme_chain_note or (f"题材：{' / '.join(theme_assessment.theme_ids)}" if theme_assessment.theme_ids else ""),
                positioning=candidate.action_hint,
            )
            packets.append(
                DecisionPacket(
                    pool="prewatch",
                    action="加入观察" if candidate.trigger_mode == "event" else "试探建仓",
                    priority="normal",
                    confidence="中" if candidate.score >= self.settings.prewatch_alert_min_score else "低",
                    symbol=candidate.symbol,
                    horizon=candidate.horizon,
                    event_id=f"prewatch:{candidate.symbol}:{candidate.as_of.isoformat()}",
                    event_assessment=event_assessment,
                    market_assessment=market_assessment,
                    theme_assessment=theme_assessment,
                    price_plan=None,
                    reason_sections=reason_sections,
                    source_bundle=SourceBundle(refs=[], quality_tier=event_assessment.source_quality_tier),
                    final_score=candidate.score,
                    event_score=event_strength,
                    market_score=market_strength,
                    theme_score=round(theme_assessment.theme_heat + sum(theme_assessment.theme_boosts.values()), 2),
                    trigger_mode=candidate.trigger_mode,
                    llm_used=False,
                )
            )
        return packets

    def _synthetic_prewatch_event_assessment(self, candidate) -> object:
        from .decision_engines.types import EventAssessment

        return EventAssessment(
            symbol=candidate.symbol,
            event_type=candidate.trigger_event_type or "prewatch",
            event_strength=round(candidate.score if candidate.trigger_mode == "event" else 0.0, 2),
            direction="bullish",
            source_quality_tier="secondary",
            credibility=75.0,
            novelty=70.0 if candidate.trigger_mode == "event" else 55.0,
            theme_candidates=[candidate.trigger_theme] if candidate.trigger_theme else [],
            reason_fragments=[candidate.headline_summary, candidate.reason_to_watch],
            llm_used=False,
            llm_confidence=0.0,
            event_prewatch_eligible=True,
            headline_summary=candidate.headline_summary,
            source_refs=[],
            risk_notes=[],
        )

    def _synthetic_prewatch_market_assessment(self, candidate) -> object:
        from .decision_engines.types import MarketAssessment

        return MarketAssessment(
            trend_state=candidate.trend_state,
            momentum_state="healthy" if 48.0 <= candidate.rsi_14 <= 65.0 else "recovering",
            volume_state="elevated" if candidate.relative_volume >= 1.2 else "normal",
            position_state=candidate.setup_type,
            volatility_state="normal",
            market_confirmation_score=round(candidate.score, 2),
            entry_plan=None,
            risk_flags=[],
            prewatch_structure_eligible=True,
            exit_signal_state="hold",
            market_data_complete=True,
            market_data_note="",
            rsi_14=round(candidate.rsi_14, 1),
            relative_volume=round(candidate.relative_volume, 2),
        )

    def _record_decision_packets(self, packets: list[DecisionPacket], *, run_id: str) -> None:
        for packet in packets:
            price_plan = packet.price_plan.to_record() if packet.price_plan is not None else {}
            invalidation = (
                {
                    "level": packet.price_plan.invalidation_level,
                    "reason": packet.price_plan.invalidation_reason,
                }
                if packet.price_plan is not None
                else {}
            )
            decision_id = hashlib.sha1(
                f"{run_id}:{packet.pool}:{packet.symbol}:{packet.horizon}:{packet.event_id}:{packet.trigger_mode}".encode("utf-8")
            ).hexdigest()
            self.store.save_decision_record(
                decision_id=decision_id,
                run_id=run_id,
                event_id=packet.event_id,
                symbol=packet.symbol,
                event_type=packet.event_assessment.event_type,
                pool=packet.pool,
                action=packet.action,
                priority=packet.priority,
                confidence=packet.confidence,
                event_score=packet.event_score,
                market_score=packet.market_score,
                theme_score=packet.theme_score,
                final_score=packet.final_score,
                trigger_mode=packet.trigger_mode,
                llm_used=packet.llm_used,
                theme_ids=list(packet.theme_assessment.theme_ids),
                entry_plan=price_plan,
                invalidation=invalidation,
                ttl=(packet.price_plan.ttl_iso if packet.price_plan is not None else ""),
                packet=packet.to_record(),
                created_at=utcnow().isoformat(),
            )

    def _fetch_from_adapter(
        self,
        adapter: SourceAdapter,
        since: datetime,
        cached_health: SourceHealthCheck | None = None,
    ) -> dict:
        if adapter.skips_pre_fetch_health_check():
            started = time.monotonic()
            try:
                adapter_events = adapter.fetch_since(since)
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "health": SourceHealthCheck(
                        source_name=adapter.name,
                        status="healthy",
                        detail="Source fetch probe succeeded.",
                        latency_ms=latency_ms,
                    ),
                    "health_from_cache": False,
                    "health_derived_from_fetch": True,
                    "events": adapter_events,
                    "partial_errors": getattr(adapter, "last_fetch_errors", []),
                }
            except Exception as exc:
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "health": SourceHealthCheck(
                        source_name=adapter.name,
                        status="unhealthy",
                        detail=f"{exc.__class__.__name__}: {exc}",
                        latency_ms=latency_ms,
                    ),
                    "health_from_cache": False,
                    "health_derived_from_fetch": True,
                    "events": [],
                    "fetch_error": str(exc),
                    "fetch_error_type": exc.__class__.__name__,
                    "partial_errors": getattr(adapter, "last_fetch_errors", []),
                }

        health = cached_health
        health_from_cache = health is not None
        if health is None:
            try:
                health = adapter.health_check()
            except Exception as exc:
                return {
                    "health": None,
                    "health_error": exc.__class__.__name__,
                    "health_detail": str(exc),
                    "events": [],
                    "partial_errors": [],
                }
        if health.status != "healthy" and not adapter.should_fetch_after_unhealthy_healthcheck():
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": [],
                "partial_errors": [],
            }
        try:
            adapter_events = adapter.fetch_since(since)
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": adapter_events,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }
        except Exception as exc:
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": [],
                "fetch_error": str(exc),
                "fetch_error_type": exc.__class__.__name__,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }

    def _source_health_state_key(self, source_name: str) -> str:
        return f"source_health:{source_name}"

    def _load_cached_source_health(self, source_name: str) -> SourceHealthCheck | None:
        ttl_seconds = max(self.settings.source_health_cache_seconds, 0)
        if ttl_seconds <= 0:
            return None
        raw = self.store.get_state(self._source_health_state_key(source_name))
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            checked_at = datetime.fromisoformat(payload["checked_at"])
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return None
        age_seconds = (utcnow() - checked_at).total_seconds()
        if age_seconds > ttl_seconds:
            return None
        return SourceHealthCheck(
            source_name=payload.get("source_name", source_name),
            status=payload.get("status", "healthy"),
            detail=f'{payload.get("detail", "")} (cached)',
            checked_at=utcnow(),
            latency_ms=0,
        )

    def _save_cached_source_health(self, health: SourceHealthCheck) -> None:
        payload = {
            "source_name": health.source_name,
            "status": health.status,
            "detail": health.detail,
            "checked_at": health.checked_at.isoformat(),
        }
        self.store.set_state(self._source_health_state_key(health.source_name), json.dumps(payload))
