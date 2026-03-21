from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.market_data import CachedMarketDataProvider, MarketDataEngine, MultiSourceMarketDataError, YahooFinanceMarketDataProvider
from satellite_agent.market_data import MultiSourceMarketDataProvider, StooqDailyMarketDataProvider
from satellite_agent.models import Bar, utcnow
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


class CountingProvider:
    def __init__(self, data: dict[tuple[str, str], list[Bar]]) -> None:
        self.data = data
        self.calls: list[tuple[str, str, int]] = []

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        self.calls.append((symbol, timeframe, limit))
        return self.data[(symbol, timeframe)][-limit:]


class MarketDataProviderTests(unittest.TestCase):
    def test_yahoo_provider_adjusts_daily_bars_with_adjclose(self) -> None:
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1710410400],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.0],
                                    "high": [101.0],
                                    "low": [99.0],
                                    "close": [100.0],
                                    "volume": [100000],
                                }
                            ],
                            "adjclose": [
                                {
                                    "adjclose": [50.0],
                                }
                            ],
                        },
                    }
                ]
            }
        }

        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(json.dumps(payload).encode("utf-8"))):
            provider = YahooFinanceMarketDataProvider()
            bars = provider.get_bars("NVDA", "1d", 10)

        self.assertEqual(len(bars), 1)
        self.assertTrue(bars[0].adjusted)
        self.assertEqual(bars[0].open, 50.0)
        self.assertEqual(bars[0].high, 50.5)
        self.assertEqual(bars[0].low, 49.5)
        self.assertEqual(bars[0].close, 50.0)

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

    def test_yahoo_provider_normalizes_dot_symbol_for_request(self) -> None:
        captured_urls: list[str] = []
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1710410400],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.0],
                                    "high": [101.0],
                                    "low": [99.5],
                                    "close": [100.5],
                                    "volume": [100000],
                                }
                            ]
                        },
                    }
                ]
            }
        }

        def _fake_urlopen(req, timeout=0):
            captured_urls.append(req.full_url)
            return MockHTTPResponse(json.dumps(payload).encode("utf-8"))

        with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
            provider = YahooFinanceMarketDataProvider()
            provider.get_bars("BRK.B", "1d", 10)

        self.assertIn("/BRK-B?", captured_urls[0])

    def test_cached_provider_persists_remote_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "cache.db")
            store.initialize()
            now = utcnow()
            bars = [
                Bar(timestamp=(now - timedelta(days=1)).replace(tzinfo=None), open=100, high=101, low=99, close=100.5, volume=1000),
                Bar(timestamp=now.replace(tzinfo=None), open=101, high=102, low=100, close=101.5, volume=1100),
            ]
            remote = StaticProvider(bars)
            provider = CachedMarketDataProvider(store=store, remote_provider=remote)
            first = provider.get_bars("NVDA", "1d", 30)
            second = provider.get_bars("NVDA", "1d", 30)
            self.assertEqual(len(first), 2)
            self.assertEqual(len(second), 2)
            self.assertEqual(remote.calls, 1)

    def test_store_tracks_bar_adjustment_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "bars.db")
            store.initialize()
            bars = [
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 1, 0, 0), open=100, high=101, low=99, close=100.5, volume=1000, adjusted=True),
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 2, 0, 0), open=101, high=102, low=100, close=101.5, volume=1100, adjusted=True),
            ]
            store.upsert_price_bars("NVDA", "1d", bars)

            summary = store.summarize_price_bar_adjustment(["NVDA"], "1d")

            self.assertEqual(len(summary), 1)
            self.assertEqual(summary[0]["symbol"], "NVDA")
            self.assertEqual(summary[0]["total_bars"], 2)
            self.assertEqual(summary[0]["adjusted_bars"], 2)
            self.assertEqual(summary[0]["unadjusted_bars"], 0)

    def test_cached_provider_uses_stale_cache_when_remote_refresh_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "cache.db")
            store.initialize()
            bars = [
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 1, 0, 0), open=100, high=101, low=99, close=100.5, volume=1000),
                Bar(timestamp=__import__("datetime").datetime(2026, 3, 2, 0, 0), open=101, high=102, low=100, close=101.5, volume=1100),
            ]
            store.upsert_price_bars("NVDA", "1d", bars)

            class FailingProvider:
                def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
                    raise URLError("temporary failure")

            provider = CachedMarketDataProvider(store=store, remote_provider=FailingProvider())
            loaded = provider.get_bars("NVDA", "1d", 30)

            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded[-1].close, 101.5)

    def test_yahoo_provider_retries_temporary_network_errors(self) -> None:
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1710410400],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.0],
                                    "high": [101.0],
                                    "low": [99.5],
                                    "close": [100.5],
                                    "volume": [100000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        with patch(
            "urllib.request.urlopen",
            side_effect=[
                URLError("temporary failure"),
                MockHTTPResponse(json.dumps(payload).encode("utf-8")),
            ],
        ):
            provider = YahooFinanceMarketDataProvider(request_timeout=1.0, max_retries=1, retry_backoff_seconds=0.0)
            bars = provider.get_bars("NVDA", "5m", 10)
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[-1].close, 100.5)

    def test_multi_source_provider_falls_back_to_secondary_provider(self) -> None:
        bars = [
            Bar(timestamp=__import__("datetime").datetime(2026, 3, 14, 0, 0), open=100, high=101, low=99, close=100.5, volume=1000),
        ]

        class FailingProvider:
            def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
                raise URLError("primary down")

        secondary = StaticProvider(bars)
        provider = MultiSourceMarketDataProvider([FailingProvider(), secondary])

        loaded = provider.get_bars("NVDA", "1d", 10)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[-1].close, 100.5)
        self.assertEqual(secondary.calls, 1)

    def test_multi_source_provider_surfaces_provider_level_failures(self) -> None:
        class EmptyProvider:
            def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
                return []

        class FailingProvider:
            def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
                raise URLError("secondary down")

        provider = MultiSourceMarketDataProvider([EmptyProvider(), FailingProvider()])

        with self.assertRaises(MultiSourceMarketDataError) as cm:
            provider.get_bars("BRK.B", "1d", 10)

        self.assertEqual(cm.exception.provider_errors["EmptyProvider"], "empty_response")
        self.assertIn("URLError", cm.exception.provider_errors["FailingProvider"])
        self.assertIn("BRK.B 1d:", str(cm.exception))

    def test_stooq_provider_parses_daily_csv_payload(self) -> None:
        csv_payload = "\n".join(
            [
                "Date,Open,High,Low,Close,Volume",
                "2026-03-13,100.0,101.0,99.0,100.5,100000",
                "2026-03-14,101.0,102.0,100.0,101.5,120000",
            ]
        )

        with patch("urllib.request.urlopen", return_value=MockHTTPResponse(csv_payload.encode("utf-8"))):
            provider = StooqDailyMarketDataProvider()
            bars = provider.get_bars("NVDA", "1d", 10)

        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[-1].close, 101.5)

    def test_market_data_engine_can_skip_intraday_fetch_for_position_snapshots(self) -> None:
        bars = [
            Bar(timestamp=__import__("datetime").datetime(2026, 3, 14, 0, 0), open=100, high=101, low=99, close=100.5, volume=1000),
            Bar(timestamp=__import__("datetime").datetime(2026, 3, 15, 0, 0), open=101, high=102, low=100, close=101.5, volume=1100),
            Bar(timestamp=__import__("datetime").datetime(2026, 3, 16, 0, 0), open=102, high=103, low=101, close=102.5, volume=1200),
        ] * 35
        provider = CountingProvider({("NVDA", "1d"): bars, ("NVDA", "5m"): bars[-60:]})
        engine = MarketDataEngine(provider)

        snapshot = engine.snapshot("NVDA", "position", include_intraday=False)

        self.assertEqual(snapshot.symbol, "NVDA")
        self.assertEqual([call[1] for call in provider.calls], ["1d"])


if __name__ == "__main__":
    unittest.main()
