from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.llm import RuleBasedExtractor
from satellite_agent.models import SourceEvent


class ExtractorTests(unittest.TestCase):
    def test_strategic_investment_is_not_treated_as_generic_news(self) -> None:
        event = SourceEvent(
            event_id="evt-nbis",
            source="Google News",
            source_type="news",
            symbol="NBIS",
            headline="NBIS stock rallies after Meta signs $27 billion AI infrastructure partnership with Nebius",
            summary="Meta invests in AI infrastructure capacity and enters a strategic partnership with Nebius.",
            published_at=datetime.now(timezone.utc),
            url="https://example.com/nbis",
        )
        insight = RuleBasedExtractor().extract(event)
        self.assertEqual(insight.event_type, "strategic")
        self.assertEqual(insight.importance, 85.0)
        self.assertEqual(insight.novelty, 80.0)
        self.assertIn("战略合作或资本加持", insight.headline_summary)


if __name__ == "__main__":
    unittest.main()
