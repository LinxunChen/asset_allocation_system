from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.config import HorizonSettings
from satellite_agent.models import IndicatorSnapshot
from satellite_agent.prewatch import build_prewatch_candidate, sort_prewatch_candidates


class PrewatchTests(unittest.TestCase):
    def test_build_prewatch_candidate_accepts_bullish_breakout_setup(self) -> None:
        snapshot = IndicatorSnapshot(
            symbol="NBIS",
            horizon="position",
            as_of=datetime.now(timezone.utc),
            last_price=112.4,
            rsi_14=58.2,
            atr_14=4.8,
            sma_20=104.0,
            sma_60=98.0,
            relative_volume=1.86,
            support_20=103.0,
            resistance_20=111.5,
            support_60=96.0,
            resistance_60=114.0,
            gap_percent=1.2,
            intraday_breakout=True,
            is_pullback=False,
            trend_state="bullish",
            atr_percent=4.3,
        )

        candidate = build_prewatch_candidate(
            snapshot,
            HorizonSettings(
                name="position",
                ttl_days=20,
                market_score_threshold=55.0,
                priority_threshold=75.0,
                rsi_floor=50.0,
                rsi_ceiling=65.0,
                atr_percent_ceiling=10.0,
            ),
            min_score=60.0,
        )

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.setup_type, "breakout_watch")
        self.assertGreaterEqual(candidate.score, 60.0)
        self.assertIn("突破", candidate.headline_summary)

    def test_build_prewatch_candidate_rejects_bearish_snapshot(self) -> None:
        snapshot = IndicatorSnapshot(
            symbol="QQQ",
            horizon="position",
            as_of=datetime.now(timezone.utc),
            last_price=440.0,
            rsi_14=34.0,
            atr_14=9.0,
            sma_20=450.0,
            sma_60=465.0,
            relative_volume=0.92,
            support_20=438.0,
            resistance_20=452.0,
            support_60=432.0,
            resistance_60=470.0,
            gap_percent=-1.5,
            intraday_breakout=False,
            is_pullback=False,
            trend_state="bearish",
            atr_percent=2.0,
        )

        candidate = build_prewatch_candidate(
            snapshot,
            HorizonSettings(
                name="position",
                ttl_days=20,
                market_score_threshold=55.0,
                priority_threshold=75.0,
                rsi_floor=50.0,
                rsi_ceiling=65.0,
                atr_percent_ceiling=10.0,
            ),
            min_score=60.0,
        )

        self.assertIsNone(candidate)

    def test_sort_prewatch_candidates_prefers_higher_scores(self) -> None:
        snapshot = IndicatorSnapshot(
            symbol="AAA",
            horizon="position",
            as_of=datetime.now(timezone.utc),
            last_price=100.0,
            rsi_14=55.0,
            atr_14=2.0,
            sma_20=98.0,
            sma_60=95.0,
            relative_volume=1.5,
            support_20=96.0,
            resistance_20=101.0,
            support_60=94.0,
            resistance_60=104.0,
            gap_percent=0.0,
            intraday_breakout=False,
            is_pullback=True,
            trend_state="bullish",
            atr_percent=2.0,
        )
        settings = HorizonSettings(
            name="position",
            ttl_days=20,
            market_score_threshold=55.0,
            priority_threshold=75.0,
            rsi_floor=50.0,
            rsi_ceiling=65.0,
            atr_percent_ceiling=10.0,
        )
        a = build_prewatch_candidate(snapshot, settings, min_score=0.0)
        b = build_prewatch_candidate(
            IndicatorSnapshot(
                symbol="BBB",
                horizon=snapshot.horizon,
                as_of=snapshot.as_of,
                last_price=snapshot.last_price,
                rsi_14=snapshot.rsi_14,
                atr_14=snapshot.atr_14,
                sma_20=snapshot.sma_20,
                sma_60=snapshot.sma_60,
                relative_volume=2.1,
                support_20=snapshot.support_20,
                resistance_20=snapshot.resistance_20,
                support_60=snapshot.support_60,
                resistance_60=snapshot.resistance_60,
                gap_percent=snapshot.gap_percent,
                intraday_breakout=snapshot.intraday_breakout,
                is_pullback=snapshot.is_pullback,
                trend_state=snapshot.trend_state,
                atr_percent=snapshot.atr_percent,
            ),
            settings,
            min_score=0.0,
        )
        assert a is not None and b is not None
        ranked = sort_prewatch_candidates([a, b], max_candidates=1)
        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0].symbol, "BBB")


if __name__ == "__main__":
    unittest.main()
