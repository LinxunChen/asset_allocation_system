from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.indicators import average_true_range, exponential_rsi, support_resistance
from satellite_agent.models import Bar


class IndicatorTests(unittest.TestCase):
    def test_rsi_for_rising_series_is_above_neutral(self) -> None:
        closes = [100, 101, 100.5, 102, 101.5, 103, 102.5, 104, 103.5, 105, 104.5, 106, 105.5, 107, 106.5, 108]
        self.assertGreater(exponential_rsi(closes, 14), 55.0)

    def test_atr_and_support_resistance_use_bar_ranges(self) -> None:
        base = datetime(2026, 3, 1, tzinfo=timezone.utc)
        bars = [
            Bar(timestamp=base + timedelta(days=index), open=100 + index, high=101 + index, low=99 + index, close=100.5 + index, volume=1000 + index)
            for index in range(20)
        ]
        self.assertGreater(average_true_range(bars, 14), 0.0)
        support, resistance = support_resistance(bars, 10)
        self.assertEqual(support, 109)
        self.assertEqual(resistance, 120)


if __name__ == "__main__":
    unittest.main()
