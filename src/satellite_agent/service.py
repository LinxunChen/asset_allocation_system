from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import time
from datetime import datetime, timedelta
from typing import Dict

from .config import Settings
from .entry_exit import EntryExitEngine
from .event_normalizer import EventNormalizer
from .llm import RuleBasedExtractor
from .market_data import MarketDataEngine
from .models import utcnow
from .notifier import Notifier
from .observability import RunContext, StructuredLogger
from .scoring import SignalScorer
from .sources import SourceAdapter
from .store import Store


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
                    insight = self.extractor.extract(event)
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
                        continue
                    try:
                        card = self.scorer.score(insight, snapshot)
                        card = self.entry_exit.enrich(card, snapshot)
                        self.store.save_opportunity_card(card, run_id=run_context.run_id)
                        run_context.metrics.cards_generated += 1
                        notification_candidates.append(card)
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
            self._dispatch_notifications(notification_candidates, run_context, logger)
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
        }

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

        with ThreadPoolExecutor(max_workers=min(len(adapters), 4)) as executor:
            futures = {
                executor.submit(self._fetch_from_adapter, adapter, since): adapter
                for adapter in adapters
            }
            for future in as_completed(futures):
                adapter = futures[future]
                result = future.result()
                health = result.get("health")
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
                        "Source health check passed.",
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                else:
                    run_context.metrics.source_health_failures += 1
                    logger.error(
                        "source_health",
                        "Source health check failed.",
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                    continue
                fetch_error = result.get("fetch_error")
                if fetch_error:
                    run_context.metrics.source_fetch_failures += 1
                    logger.error(
                        "source_fetch_failed",
                        "Source fetch failed after health check.",
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
        for event in sorted(events, key=lambda item: item.published_at, reverse=True):
            if self.settings.max_events_per_symbol_per_run > 0:
                current = symbol_counts.get(event.symbol, 0)
                if current >= self.settings.max_events_per_symbol_per_run:
                    continue
                symbol_counts[event.symbol] = current + 1
            limited.append(event)
            if len(limited) >= self.settings.max_events_per_run:
                break
        return sorted(limited, key=lambda item: item.published_at)

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
        return sorted(
            cards,
            key=lambda card: (
                priority_rank.get(card.priority, 3),
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

    def _fetch_from_adapter(self, adapter: SourceAdapter, since: datetime) -> dict:
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
        if health.status != "healthy":
            return {
                "health": health,
                "events": [],
                "partial_errors": [],
            }
        try:
            adapter_events = adapter.fetch_since(since)
            return {
                "health": health,
                "events": adapter_events,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }
        except Exception as exc:
            return {
                "health": health,
                "events": [],
                "fetch_error": str(exc),
                "fetch_error_type": exc.__class__.__name__,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }
