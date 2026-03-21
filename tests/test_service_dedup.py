from __future__ import annotations

import sys
import tempfile
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
from satellite_agent.models import Bar, SourceEvent, utcnow
from satellite_agent.notifier import Notifier
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import SatelliteAgentService
from satellite_agent.sources import StaticSourceAdapter
from satellite_agent.store import Store


def _daily_bars() -> list[Bar]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = []
    for index in range(70):
        close = 100 + (index * 0.4)
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close - 0.5,
                high=close + 0.8,
                low=close - 1.0,
                close=close,
                volume=1_000_000 + (index * 10_000),
            )
        )
    return bars


def _intraday_bars() -> list[Bar]:
    base = datetime(2026, 3, 14, 13, 30, tzinfo=timezone.utc)
    bars = []
    for index in range(20):
        close = 127 + (index * 0.15)
        bars.append(
            Bar(
                timestamp=base + timedelta(minutes=5 * index),
                open=close - 0.1,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=150_000 + (index * 4_000),
            )
        )
    return bars


class ServiceDedupTests(unittest.TestCase):
    def test_limit_fetched_events_prioritizes_stronger_catalysts_over_generic_news(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NBIS", "MU", "AAPL"], "stock")
            service = SatelliteAgentService(
                settings=Settings(
                    database_path=db_path,
                    dry_run=True,
                    max_events_per_run=2,
                    max_events_per_symbol_per_run=2,
                ),
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple stock price today: everything you need to know",
                    summary="Should investors buy now after the latest move?",
                    published_at=now,
                    url="https://example.com/aapl",
                ),
                SourceEvent(
                    event_id="evt-strategic",
                    source="Google News",
                    source_type="news",
                    symbol="NBIS",
                    headline="Nebius rallies after Meta investment and strategic partnership",
                    summary="Meta invested in new AI infrastructure capacity.",
                    published_at=now - timedelta(minutes=4),
                    url="https://example.com/nbis",
                ),
                SourceEvent(
                    event_id="evt-earnings",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron earnings beat and guidance raise lift AI memory outlook",
                    summary="Management raised forecast after stronger data center demand.",
                    published_at=now - timedelta(minutes=8),
                    url="https://example.com/mu",
                ),
            ]

            selected = service._limit_fetched_events(events)

            self.assertEqual({event.symbol for event in selected}, {"NBIS", "MU"})

    def test_limit_deep_processing_events_respects_budget_and_keeps_best_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NBIS", "MU", "AAPL"], "stock")
            service = SatelliteAgentService(
                settings=Settings(
                    database_path=db_path,
                    dry_run=True,
                    max_deep_events_per_run=2,
                ),
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple stock price today: top 10 facts to know",
                    summary="What you need to know before you buy now.",
                    published_at=now,
                    url="https://example.com/aapl",
                ),
                SourceEvent(
                    event_id="evt-strategic",
                    source="Google News",
                    source_type="news",
                    symbol="NBIS",
                    headline="Nebius signs strategic partnership after fresh AI investment",
                    summary="The deal expands data center capacity for large AI customers.",
                    published_at=now - timedelta(minutes=3),
                    url="https://example.com/nbis",
                ),
                SourceEvent(
                    event_id="evt-filing",
                    source="SEC Edgar",
                    source_type="filing",
                    symbol="MU",
                    headline="8-K filing: Micron provides updated guidance",
                    summary="Micron filed an 8-K after raising outlook.",
                    published_at=now - timedelta(minutes=6),
                    url="https://example.com/mu",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 2)
            self.assertEqual({event.symbol for event in selected}, {"NBIS", "MU"})

    def test_limit_deep_processing_events_prefers_batch_theme_cluster_under_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU", "AAPL", "AMZN"], "stock")
            settings = Settings(
                database_path=db_path,
                dry_run=True,
                max_deep_events_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                                "core_platforms": ["AAPL", "AMZN"],
                            }
                        }
                    }
                },
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-aapl-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now,
                    url="https://example.com/aapl-generic",
                ),
                SourceEvent(
                    event_id="evt-nvda-generic",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=5),
                    url="https://example.com/nvda-generic",
                ),
                SourceEvent(
                    event_id="evt-mu-generic",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=10),
                    url="https://example.com/mu-generic",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 2)
            self.assertEqual({event.symbol for event in selected}, {"NVDA", "MU"})

    def test_limit_deep_processing_events_reserves_theme_representatives_before_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU", "AAPL", "AMZN", "VRTX"], "stock")
            settings = Settings(
                database_path=db_path,
                dry_run=True,
                max_deep_events_per_run=3,
                max_deep_theme_reservations_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                                "core_platforms": ["AAPL", "AMZN"],
                                "healthcare": ["VRTX"],
                            }
                        }
                    }
                },
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-aapl-1",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple strategic partnership boosts platform adoption",
                    summary="AAPL deal expands services reach.",
                    published_at=now,
                    url="https://example.com/aapl-1",
                ),
                SourceEvent(
                    event_id="evt-amzn-1",
                    source="Google News",
                    source_type="news",
                    symbol="AMZN",
                    headline="Amazon strategic partnership boosts platform adoption",
                    summary="AMZN deal expands services reach.",
                    published_at=now - timedelta(minutes=1),
                    url="https://example.com/amzn-1",
                ),
                SourceEvent(
                    event_id="evt-vrtx-1",
                    source="Google News",
                    source_type="news",
                    symbol="VRTX",
                    headline="Vertex strategic partnership boosts platform adoption",
                    summary="VRTX deal expands services reach.",
                    published_at=now - timedelta(minutes=2),
                    url="https://example.com/vrtx-1",
                ),
                SourceEvent(
                    event_id="evt-nvda-generic",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=3),
                    url="https://example.com/nvda-generic",
                ),
                SourceEvent(
                    event_id="evt-mu-generic",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=4),
                    url="https://example.com/mu-generic",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 3)
            self.assertIn("NVDA", {event.symbol for event in selected})
            self.assertIn("AAPL", {event.symbol for event in selected})

    def test_cross_source_duplicate_events_are_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                {
                    ("NVDA", "1d"): _daily_bars(),
                    ("NVDA", "5m"): _intraday_bars(),
                }
            )
            published = utcnow() - timedelta(minutes=30)
            events = [
                SourceEvent(
                    event_id="evt-news",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia jumps after upbeat AI server commentary",
                    summary="Analysts noted stronger demand and raised near-term expectations.",
                    published_at=published,
                    url="https://example.com/news",
                ),
                SourceEvent(
                    event_id="evt-research",
                    source="Research Feed",
                    source_type="research",
                    symbol="NVDA",
                    headline="Nvidia jumps after upbeat AI server commentary",
                    summary="Desk commentary pointed to stronger AI demand and better expectations.",
                    published_at=published + timedelta(minutes=10),
                    url="https://example.com/research",
                ),
            ]
            settings = Settings(database_path=db_path, dry_run=True, cross_source_dedup_hours=12)
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter(events),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            result = service.run_once()
            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)


if __name__ == "__main__":
    unittest.main()
