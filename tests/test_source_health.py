from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.config import Settings
from satellite_agent.entry_exit import EntryExitEngine
from satellite_agent.event_normalizer import EventNormalizer
from satellite_agent.llm import RuleBasedExtractor
from satellite_agent.market_data import InMemoryMarketDataProvider, MarketDataEngine
from satellite_agent.models import Bar, SourceEvent, SourceHealthCheck
from satellite_agent.notifier import Notifier
from satellite_agent.reporting import format_source_health
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import SatelliteAgentService
from satellite_agent.sources import CompositeSourceAdapter, SourceAdapter, StaticSourceAdapter
from satellite_agent.store import Store


class UnhealthySourceAdapter(SourceAdapter):
    @property
    def name(self) -> str:
        return "broken_feed"

    def health_check(self) -> SourceHealthCheck:
        return SourceHealthCheck(source_name=self.name, status="unhealthy", detail="DNS failure")

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        raise RuntimeError("should not fetch unhealthy source")


class SlowHealthySourceAdapter(SourceAdapter):
    def __init__(self, name: str, event: SourceEvent, delay_seconds: float) -> None:
        self._name = name
        self.event = event
        self.delay_seconds = delay_seconds

    @property
    def name(self) -> str:
        return self._name

    def health_check(self) -> SourceHealthCheck:
        return SourceHealthCheck(source_name=self.name, status="healthy", detail=f"{self.name} ok")

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        time.sleep(self.delay_seconds)
        return [self.event] if self.event.published_at > ts else []


class DegradedButFetchableSourceAdapter(SourceAdapter):
    def __init__(self, event: SourceEvent) -> None:
        self.event = event

    @property
    def name(self) -> str:
        return "degraded_feed"

    def health_check(self) -> SourceHealthCheck:
        return SourceHealthCheck(source_name=self.name, status="unhealthy", detail="ssl eof")

    def should_fetch_after_unhealthy_healthcheck(self) -> bool:
        return True

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        return [self.event] if self.event.published_at > ts else []


class CountingHealthySourceAdapter(SourceAdapter):
    def __init__(self) -> None:
        self.health_calls = 0
        self.fetch_calls = 0

    @property
    def name(self) -> str:
        return "counting_feed"

    def health_check(self) -> SourceHealthCheck:
        self.health_calls += 1
        return SourceHealthCheck(source_name=self.name, status="healthy", detail="counting ok")

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        self.fetch_calls += 1
        return []


class FetchProbeSourceAdapter(SourceAdapter):
    def __init__(self, should_fail: bool = False) -> None:
        self.health_calls = 0
        self.fetch_calls = 0
        self.should_fail = should_fail

    @property
    def name(self) -> str:
        return "probe_feed"

    def skips_pre_fetch_health_check(self) -> bool:
        return True

    def health_check(self) -> SourceHealthCheck:
        self.health_calls += 1
        return SourceHealthCheck(source_name=self.name, status="healthy", detail="unused")

    def fetch_since(self, ts: datetime) -> list[SourceEvent]:
        self.fetch_calls += 1
        if self.should_fail:
            raise RuntimeError("probe failed")
        return []


def _daily_bars() -> list[Bar]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        Bar(
            timestamp=base + timedelta(days=index),
            open=100 + index * 0.4,
            high=100.6 + index * 0.4,
            low=99.4 + index * 0.4,
            close=100.2 + index * 0.4,
            volume=1_000_000 + index * 7_000,
        )
        for index in range(70)
    ]


def _intraday_bars() -> list[Bar]:
    base = datetime(2026, 3, 14, 13, 30, tzinfo=timezone.utc)
    return [
        Bar(
            timestamp=base + timedelta(minutes=index * 5),
            open=128 + index * 0.1,
            high=128.2 + index * 0.1,
            low=127.8 + index * 0.1,
            close=128.05 + index * 0.1,
            volume=140_000 + index * 2_000,
        )
        for index in range(24)
    ]


class SourceHealthTests(unittest.TestCase):
    def test_unhealthy_source_is_recorded_and_other_sources_continue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                {("NVDA", "1d"): _daily_bars(), ("NVDA", "5m"): _intraday_bars()}
            )
            event = SourceEvent(
                event_id="evt-health",
                source="Replay",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia sees stronger AI demand",
                summary="Checks suggest stronger near-term demand for AI servers.",
                published_at=datetime.now(timezone.utc) - timedelta(hours=2),
                url="https://example.com/health",
            )
            service = SatelliteAgentService(
                settings=Settings(database_path=db_path, dry_run=True),
                store=store,
                source_adapter=CompositeSourceAdapter([UnhealthySourceAdapter(), StaticSourceAdapter([event])]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(Settings(database_path=db_path, dry_run=True)),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()

            self.assertEqual(result["events_processed"], 1)
            run = store.load_latest_run()
            summary = json.loads(run["summary_json"])
            self.assertEqual(summary["source_health_failures"], 1)
            health_rows = store.load_source_health(run["run_id"])
            self.assertEqual(len(health_rows), 2)
            rendered = format_source_health(store.load_latest_source_health())
            self.assertIn("broken_feed", rendered)
            self.assertIn("unhealthy", rendered)

    def test_source_fetches_run_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MSFT"], "stock")
            provider = InMemoryMarketDataProvider(
                {
                    ("NVDA", "1d"): _daily_bars(),
                    ("NVDA", "5m"): _intraday_bars(),
                    ("MSFT", "1d"): _daily_bars(),
                    ("MSFT", "5m"): _intraday_bars(),
                }
            )
            published_at = datetime.now(timezone.utc) - timedelta(hours=2)
            event_nvda = SourceEvent(
                event_id="evt-slow-1",
                source="News A",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia positive note",
                summary="Nvidia positive note",
                published_at=published_at,
                url="https://example.com/nvda",
            )
            event_msft = SourceEvent(
                event_id="evt-slow-2",
                source="News B",
                source_type="news",
                symbol="MSFT",
                headline="Microsoft positive note",
                summary="Microsoft positive note",
                published_at=published_at,
                url="https://example.com/msft",
            )
            service = SatelliteAgentService(
                settings=Settings(database_path=db_path, dry_run=True),
                store=store,
                source_adapter=CompositeSourceAdapter(
                    [
                        SlowHealthySourceAdapter("slow_a", event_nvda, 0.25),
                        SlowHealthySourceAdapter("slow_b", event_msft, 0.25),
                    ]
                ),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(Settings(database_path=db_path, dry_run=True)),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            started = time.monotonic()
            result = service.run_once()
            elapsed = time.monotonic() - started

            self.assertEqual(result["events_processed"], 2)
            self.assertLess(elapsed, 0.45)

    def test_degraded_source_can_still_fetch_when_adapter_allows_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                {("NVDA", "1d"): _daily_bars(), ("NVDA", "5m"): _intraday_bars()}
            )
            event = SourceEvent(
                event_id="evt-degraded-1",
                source="Degraded Feed",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia secures a major AI infrastructure contract",
                summary="Despite a flaky health check, the underlying feed still returns data.",
                published_at=datetime.now(timezone.utc) - timedelta(hours=2),
                url="https://example.com/degraded",
            )
            service = SatelliteAgentService(
                settings=Settings(database_path=db_path, dry_run=True),
                store=store,
                source_adapter=CompositeSourceAdapter([DegradedButFetchableSourceAdapter(event)]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(Settings(database_path=db_path, dry_run=True)),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()

            self.assertEqual(result["events_processed"], 1)
            run = store.load_latest_run()
            summary = json.loads(run["summary_json"])
            self.assertEqual(summary["source_health_failures"], 1)
            self.assertEqual(summary["source_fetch_failures"], 0)

    def test_recent_healthy_source_check_is_cached_between_runs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            adapter = CountingHealthySourceAdapter()
            settings = Settings(
                database_path=db_path,
                dry_run=True,
                source_health_cache_seconds=300,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=CompositeSourceAdapter([adapter]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider({})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            service.run_once()
            service.run_once()

            self.assertEqual(adapter.health_calls, 1)
            self.assertEqual(adapter.fetch_calls, 2)

    def test_feed_probe_source_skips_pre_fetch_health_check(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            adapter = FetchProbeSourceAdapter()
            settings = Settings(database_path=db_path, dry_run=True)
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=CompositeSourceAdapter([adapter]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider({})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            service.run_once()

            self.assertEqual(adapter.health_calls, 0)
            self.assertEqual(adapter.fetch_calls, 1)
            latest = store.load_latest_source_health()
            self.assertEqual(latest[0]["status"], "healthy")
            self.assertIn("probe", latest[0]["detail"])

    def test_feed_probe_failure_counts_as_fetch_failure_not_health_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            adapter = FetchProbeSourceAdapter(should_fail=True)
            settings = Settings(database_path=db_path, dry_run=True)
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=CompositeSourceAdapter([adapter]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider({})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            service.run_once()

            run = store.load_latest_run()
            summary = json.loads(run["summary_json"])
            self.assertEqual(summary["source_health_failures"], 0)
            self.assertEqual(summary["source_fetch_failures"], 1)


if __name__ == "__main__":
    unittest.main()
