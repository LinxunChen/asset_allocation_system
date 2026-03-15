from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.event_normalizer import EventNormalizer
from satellite_agent.models import SourceEvent


class NormalizerTests(unittest.TestCase):
    def test_normalizer_cleans_fields_and_builds_stable_event_id(self) -> None:
        normalizer = EventNormalizer()
        event = SourceEvent(
            event_id="",
            source=" Reuters ",
            source_type=" news ",
            symbol=" nvda ",
            headline=" Nvidia   beats   expectations ",
            summary="  Data center revenue accelerated. ",
            published_at=datetime(2026, 3, 14, tzinfo=timezone.utc),
            url="https://example.com/test",
        )
        normalized = normalizer.normalize(event)
        self.assertEqual(normalized.symbol, "NVDA")
        self.assertEqual(normalized.headline, "Nvidia beats expectations")
        self.assertTrue(normalized.event_id)
        self.assertEqual(normalized.event_id, normalizer.normalize(event).event_id)

    def test_normalizer_strips_html_from_summary(self) -> None:
        normalizer = EventNormalizer()
        event = SourceEvent(
            event_id="evt-1",
            source="Google News",
            source_type="news",
            symbol="roku",
            headline="Roku coverage",
            summary='<a href="https://example.com">Roku expands ad platform</a>&nbsp;<font color="#6f6f6f">Example</font>',
            published_at=datetime(2026, 3, 14, tzinfo=timezone.utc),
            url="https://example.com/test",
        )

        normalized = normalizer.normalize(event)

        self.assertEqual(normalized.summary, "Roku expands ad platform Example")


if __name__ == "__main__":
    unittest.main()
