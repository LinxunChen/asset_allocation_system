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
