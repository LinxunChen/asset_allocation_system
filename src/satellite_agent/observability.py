from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from .models import utcnow
from .store import Store
from .timefmt import format_beijing_minute, normalize_timestamp_fields


LOGGER = logging.getLogger("satellite_agent")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


def _with_legacy_run_metric_aliases(payload: dict[str, int]) -> dict[str, int]:
    merged = dict(payload)
    merged["prewatch_candidates"] = int(payload.get("candidate_pool_count", 0))
    merged["prewatch_alerts_sent"] = int(payload.get("candidate_optional_alerts_sent_count", 0))
    merged["prewatch_failures"] = int(payload.get("candidate_pool_failures", 0))
    return merged


@dataclass
class RunMetrics:
    source_health_failures: int = 0
    source_fetch_failures: int = 0
    events_fetched: int = 0
    events_processed: int = 0
    cards_generated: int = 0
    alerts_sent: int = 0
    candidate_pool_count: int = 0
    candidate_optional_alerts_sent_count: int = 0
    candidate_pool_failures: int = 0
    skipped_out_of_watchlist: int = 0
    skipped_cross_source_duplicate: int = 0
    skipped_duplicate_event_id: int = 0
    extraction_failures: int = 0
    llm_requests: int = 0
    llm_budget_skips: int = 0
    llm_fallbacks: int = 0
    market_data_failures: int = 0
    scoring_failures: int = 0
    notification_failures: int = 0

    @property
    def prewatch_candidates(self) -> int:
        return self.candidate_pool_count

    @prewatch_candidates.setter
    def prewatch_candidates(self, value: int) -> None:
        self.candidate_pool_count = int(value)

    @property
    def prewatch_alerts_sent(self) -> int:
        return self.candidate_optional_alerts_sent_count

    @prewatch_alerts_sent.setter
    def prewatch_alerts_sent(self, value: int) -> None:
        self.candidate_optional_alerts_sent_count = int(value)

    @property
    def prewatch_failures(self) -> int:
        return self.candidate_pool_failures

    @prewatch_failures.setter
    def prewatch_failures(self, value: int) -> None:
        self.candidate_pool_failures = int(value)

    def as_dict(self) -> dict[str, int]:
        return _with_legacy_run_metric_aliases({
            "source_health_failures": self.source_health_failures,
            "source_fetch_failures": self.source_fetch_failures,
            "events_fetched": self.events_fetched,
            "events_processed": self.events_processed,
            "cards_generated": self.cards_generated,
            "alerts_sent": self.alerts_sent,
            "candidate_pool_count": self.candidate_pool_count,
            "candidate_optional_alerts_sent_count": self.candidate_optional_alerts_sent_count,
            "candidate_pool_failures": self.candidate_pool_failures,
            "skipped_out_of_watchlist": self.skipped_out_of_watchlist,
            "skipped_cross_source_duplicate": self.skipped_cross_source_duplicate,
            "skipped_duplicate_event_id": self.skipped_duplicate_event_id,
            "extraction_failures": self.extraction_failures,
            "llm_requests": self.llm_requests,
            "llm_budget_skips": self.llm_budget_skips,
            "llm_fallbacks": self.llm_fallbacks,
            "market_data_failures": self.market_data_failures,
            "scoring_failures": self.scoring_failures,
            "notification_failures": self.notification_failures,
        })


@dataclass
class RunContext:
    run_id: str
    started_at: datetime = field(default_factory=utcnow)
    metrics: RunMetrics = field(default_factory=RunMetrics)


class StructuredLogger:
    def __init__(self, store: Store, run_id: str) -> None:
        self.store = store
        self.run_id = run_id

    def info(
        self,
        event_type: str,
        message: str,
        *,
        stage: str,
        symbol: str = "",
        event_id: str = "",
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        self._log("INFO", event_type, message, stage=stage, symbol=symbol, event_id=event_id, context=context)

    def warning(
        self,
        event_type: str,
        message: str,
        *,
        stage: str,
        symbol: str = "",
        event_id: str = "",
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        self._log("WARNING", event_type, message, stage=stage, symbol=symbol, event_id=event_id, context=context)

    def error(
        self,
        event_type: str,
        message: str,
        *,
        stage: str,
        symbol: str = "",
        event_id: str = "",
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        self._log("ERROR", event_type, message, stage=stage, symbol=symbol, event_id=event_id, context=context)

    def _log(
        self,
        level: str,
        event_type: str,
        message: str,
        *,
        stage: str,
        symbol: str = "",
        event_id: str = "",
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        payload = {
            "timestamp": format_beijing_minute(utcnow()),
            "run_id": self.run_id,
            "level": level,
            "event_type": event_type,
            "stage": stage,
            "symbol": symbol,
            "event_id": event_id,
            "message": message,
            "context": normalize_timestamp_fields(context or {}),
        }
        self.store.record_log(
            run_id=self.run_id,
            level=level,
            event_type=event_type,
            stage=stage,
            message=message,
            symbol=symbol,
            event_id=event_id,
            context=context or {},
        )
        LOGGER.info(json.dumps(payload, sort_keys=True))
