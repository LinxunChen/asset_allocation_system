from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.sources import SecFilingsSourceAdapter


ATOM_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>8-K - Current report</title>
    <summary>NVIDIA CORP filed an 8-K current report.</summary>
    <updated>2026-03-14T14:00:00-04:00</updated>
    <id>urn:tag:sec.gov,2008:accession-number=0000000000-26-000001</id>
    <link href="https://www.sec.gov/Archives/edgar/data/test-8k.htm" />
  </entry>
</feed>
"""


class MockHTTPResponse(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class SecSourceTests(unittest.TestCase):
    def test_sec_adapter_parses_recent_entries(self) -> None:
        adapter = SecFilingsSourceAdapter(symbols=["NVDA"], user_agent="test@example.com", forms=("8-K",))
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(ATOM_FEED)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 17, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].symbol, "NVDA")
        self.assertEqual(events[0].source_type, "filing")
        self.assertIn("8-K", events[0].headline)

    def test_sec_adapter_skips_malformed_symbol_feed_and_continues(self) -> None:
        adapter = SecFilingsSourceAdapter(symbols=["BAD", "NVDA"], user_agent="test@example.com", forms=("8-K",))
        with patch(
            "urllib.request.urlopen",
            side_effect=[MockHTTPResponse(b"<feed><broken"), MockHTTPResponse(ATOM_FEED)],
        ):
            events = adapter.fetch_since(datetime(2026, 3, 14, 17, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].symbol, "NVDA")
        self.assertEqual(adapter.last_fetch_errors[0]["symbol"], "BAD")
        self.assertIn("Malformed SEC payload", adapter.last_fetch_errors[0]["detail"])


if __name__ == "__main__":
    unittest.main()
