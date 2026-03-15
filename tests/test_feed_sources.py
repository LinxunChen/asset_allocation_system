from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.sources import GenericFeedSourceAdapter, GoogleNewsSourceAdapter


RSS_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>Nvidia jumps after upbeat AI server commentary</title>
      <description>Analysts noted stronger demand and raised near-term expectations.</description>
      <pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate>
      <guid>news-1</guid>
      <link>https://example.com/news-1</link>
    </item>
  </channel>
</rss>
"""

IRRELEVANT_COST_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>Firefly Aerospace Stock Surged 13% Following Successful Alpha Flight 7 Rocket Launch</title>
      <description>Firefly Aerospace stock surged after a successful rocket launch.</description>
      <pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate>
      <guid>cost-noise-1</guid>
      <link>https://news.google.com/rss/articles/noise</link>
    </item>
  </channel>
</rss>
"""

RELEVANT_COST_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>Costco launches same-day delivery expansion for members</title>
      <description>Costco expanded a key member delivery program.</description>
      <pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate>
      <guid>cost-hit-1</guid>
      <link>https://news.google.com/rss/articles/hit</link>
    </item>
  </channel>
</rss>
"""

IRRELEVANT_ROKU_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>SANTEN PHARMACE - SNPHY - Stock Price Today</title>
      <description>Generic stock price landing page for SNPHY.</description>
      <pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate>
      <guid>roku-noise-1</guid>
      <link>https://news.google.com/rss/articles/roku-noise</link>
    </item>
  </channel>
</rss>
"""

LOW_SIGNAL_QQQ_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>Should QQQ Investors Sell Before the Split?</title>
      <description>Generic market commentary without a fresh catalyst.</description>
      <pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate>
      <guid>qqq-noise-1</guid>
      <link>https://news.google.com/rss/articles/qqq-noise</link>
    </item>
  </channel>
</rss>
"""

MANY_RELEVANT_COST_RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item><title>Costco item 1</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:10:00 GMT</pubDate><guid>cost-1</guid><link>https://news.google.com/rss/articles/cost-1</link></item>
    <item><title>Costco item 2</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:09:00 GMT</pubDate><guid>cost-2</guid><link>https://news.google.com/rss/articles/cost-2</link></item>
    <item><title>Costco item 3</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:08:00 GMT</pubDate><guid>cost-3</guid><link>https://news.google.com/rss/articles/cost-3</link></item>
    <item><title>Costco item 4</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:07:00 GMT</pubDate><guid>cost-4</guid><link>https://news.google.com/rss/articles/cost-4</link></item>
    <item><title>Costco item 5</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:06:00 GMT</pubDate><guid>cost-5</guid><link>https://news.google.com/rss/articles/cost-5</link></item>
    <item><title>Costco item 6</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:05:00 GMT</pubDate><guid>cost-6</guid><link>https://news.google.com/rss/articles/cost-6</link></item>
    <item><title>Costco item 7</title><description>Costco expands service.</description><pubDate>Fri, 14 Mar 2026 14:04:00 GMT</pubDate><guid>cost-7</guid><link>https://news.google.com/rss/articles/cost-7</link></item>
  </channel>
</rss>
"""


class MockHTTPResponse(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class FeedSourceTests(unittest.TestCase):
    def test_generic_feed_adapter_parses_rss_items(self) -> None:
        adapter = GenericFeedSourceAdapter(
            symbols=["NVDA"],
            url_builder=lambda symbol: f"https://example.com/{symbol}.rss",
            source_name="Sample RSS",
            source_type="news",
        )
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(RSS_FEED)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].symbol, "NVDA")
        self.assertEqual(events[0].source, "Sample RSS")
        self.assertEqual(events[0].source_type, "news")

    def test_generic_feed_adapter_skips_failed_symbol_and_keeps_other_results(self) -> None:
        adapter = GenericFeedSourceAdapter(
            symbols=["BAD", "NVDA"],
            url_builder=lambda symbol: f"https://example.com/{symbol}.rss",
            source_name="Sample RSS",
            source_type="news",
        )
        def fake_urlopen(req, timeout=0):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            if "BAD.rss" in url:
                raise URLError("ssl eof")
            return MockHTTPResponse(RSS_FEED)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].symbol, "NVDA")
        self.assertEqual(adapter.last_fetch_errors[0]["symbol"], "BAD")
        self.assertIn("ssl eof", adapter.last_fetch_errors[0]["detail"])

    def test_google_news_filters_irrelevant_result_for_ambiguous_symbol(self) -> None:
        adapter = GoogleNewsSourceAdapter(symbols=["COST"])
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(IRRELEVANT_COST_RSS)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(events, [])

    def test_google_news_keeps_alias_match_for_ambiguous_symbol(self) -> None:
        adapter = GoogleNewsSourceAdapter(symbols=["COST"])
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(RELEVANT_COST_RSS)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].symbol, "COST")

    def test_google_news_filters_irrelevant_result_for_expanded_alias_symbol(self) -> None:
        adapter = GoogleNewsSourceAdapter(symbols=["ROKU"])
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(IRRELEVANT_ROKU_RSS)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(events, [])

    def test_google_news_filters_low_signal_generic_market_piece(self) -> None:
        adapter = GoogleNewsSourceAdapter(symbols=["QQQ"])
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(LOW_SIGNAL_QQQ_RSS)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(events, [])

    def test_google_news_caps_results_per_symbol_before_downstream_processing(self) -> None:
        adapter = GoogleNewsSourceAdapter(symbols=["COST"])
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(MANY_RELEVANT_COST_RSS)):
            events = adapter.fetch_since(datetime(2026, 3, 14, 13, 0, tzinfo=timezone.utc))
        self.assertEqual(len(events), adapter.MAX_RESULTS_PER_SYMBOL)
        self.assertEqual(events[0].event_id, "cost-6")
        self.assertEqual(events[-1].event_id, "cost-1")


if __name__ == "__main__":
    unittest.main()
