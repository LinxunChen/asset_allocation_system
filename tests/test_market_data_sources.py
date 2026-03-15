from __future__ import annotations

import json
import sys
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.market_data import CachedMarketDataProvider, YahooFinanceMarketDataProvider
from satellite_agent.models import Bar
from satellite_agent.store import Store


class MockHTTPResponse(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class StaticProvider:
    def __init__(self, bars: list[Bar]) -> None:
        self.bars = bars
        self.calls = 0

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        self.calls += 1
        return self.bars[-limit:]


class MarketDataProviderTests(unittest.TestCase):
    def test_yahoo_provider_parses_chart_payload(self) -> None:
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1710410400, 1710410700],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.0, 101.0],
                                    "high": [101.0, 102.0],
                                    "low": [99.5, 100.5],
                                    "close": [100.5, 101.5],
                                    "volume": [100000, 120000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(json.dumps(payload).encode("utf-8"))):
            provider = YahooFinanceMarketDataProvider()
            bars = provider.get_bars("NVDA", "5m", 10)
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[-1].close, 101.5)

    def test_cached_provider_persists_remote_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "cache.db")
            store.initialize()
            bars = [
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 14, 0, 0), open=100, high=101, low=99, close=100.5, volume=1000),
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 15, 0, 0), open=101, high=102, low=100, close=101.5, volume=1100),
            ]
            remote = StaticProvider(bars)
            provider = CachedMarketDataProvider(store=store, remote_provider=remote)
            first = provider.get_bars("NVDA", "1d", 30)
            second = provider.get_bars("NVDA", "1d", 30)
            self.assertEqual(len(first), 2)
            self.assertEqual(len(second), 2)
            self.assertEqual(remote.calls, 1)


if __name__ == "__main__":
    unittest.main()
