from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.config import Settings
from satellite_agent.entry_exit import EntryExitEngine
from satellite_agent.event_normalizer import EventNormalizer
from satellite_agent.llm import RuleBasedExtractor
from satellite_agent.market_data import InMemoryMarketDataProvider, MarketDataEngine
from satellite_agent.models import Bar, SourceEvent
from satellite_agent.notifier import Notifier
from satellite_agent.observability import StructuredLogger
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import SatelliteAgentService
from satellite_agent.sources import StaticSourceAdapter
from satellite_agent.store import Store


class FailingTransport:
    def send(self, title: str, body: str) -> None:
        raise RuntimeError("webhook unavailable")


def _bars_daily() -> list[Bar]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = []
    for index in range(75):
        close = 100 + (index * 0.5)
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close - 0.4,
                high=close + 0.7,
                low=close - 0.8,
                close=close,
                volume=900_000 + (index * 12_000),
            )
        )
    return bars


def _bars_intraday() -> list[Bar]:
    base = datetime(2026, 3, 14, 13, 30, tzinfo=timezone.utc)
    bars = []
    for index in range(24):
        close = 136 + (index * 0.12)
        bars.append(
            Bar(
                timestamp=base + timedelta(minutes=index * 5),
                open=close - 0.1,
                high=close + 0.15,
                low=close - 0.2,
                close=close,
                volume=160_000 + (index * 3_000),
            )
        )
    return bars


class ObservabilityTests(unittest.TestCase):
    def test_structured_logger_uses_beijing_minute_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            logger = StructuredLogger(store, "run-log")
            fixed_now = datetime(2026, 3, 15, 2, 58, 8, tzinfo=timezone.utc)
            with patch("satellite_agent.observability.utcnow", return_value=fixed_now):
                with patch("satellite_agent.observability.LOGGER.info") as mock_info:
                    logger.info(
                        "events_fetched",
                        "Fetched candidate events from configured sources.",
                        stage="source_fetch",
                        context={"since": "2026-03-14T10:58:08.700528+00:00"},
                    )
            payload = json.loads(mock_info.call_args.args[0])
            self.assertEqual(payload["timestamp"], "2026-03-15 10:58")
            self.assertEqual(payload["context"]["since"], "2026-03-14 18:58")

    def test_run_summary_and_logs_capture_notification_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                {
                    ("NVDA", "1d"): _bars_daily(),
                    ("NVDA", "5m"): _bars_intraday(),
                }
            )
            event = SourceEvent(
                event_id="evt-observe",
                source="Google News",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia rises after upbeat AI demand commentary",
                summary="Street checks pointed to stronger near-term demand and better server momentum.",
                published_at=datetime.now(timezone.utc) - timedelta(hours=2),
                url="https://example.com/observe",
            )
            settings = Settings(
                database_path=db_path,
                dry_run=False,
                normal_alert_min_final_score=0.0,
                max_alerts_per_symbol_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([event]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=FailingTransport(), dry_run=False),
            )

            result = service.run_once()

            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)
            self.assertEqual(result["alerts_sent"], 0)

            run_row = store.load_latest_run()
            self.assertIsNotNone(run_row)
            summary = json.loads(run_row["summary_json"])
            self.assertEqual(summary["notification_failures"], 2)
            self.assertEqual(summary["events_processed"], 1)

            logs = store.load_logs(run_row["run_id"])
            event_types = [row["event_type"] for row in logs]
            self.assertIn("run_started", event_types)
            self.assertIn("alert_failed", event_types)
            self.assertIn("run_finished", event_types)


if __name__ == "__main__":
    unittest.main()
