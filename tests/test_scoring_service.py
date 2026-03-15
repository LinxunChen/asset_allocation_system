from __future__ import annotations

import json
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
from satellite_agent.models import Bar, SourceEvent
from satellite_agent.models import EventInsight, IndicatorSnapshot, OpportunityCard, PriceRange, utcnow
from satellite_agent.notifier import Notifier
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import SatelliteAgentService
from satellite_agent.sources import StaticSourceAdapter
from satellite_agent.store import Store


def build_daily_bars() -> list[Bar]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    closes = [
        100.0, 101.0, 100.8, 101.6, 101.4, 102.1, 101.9, 102.5, 102.3, 103.0,
        102.8, 103.4, 103.1, 103.8, 103.6, 104.1, 103.9, 104.5, 104.2, 104.8,
        104.6, 105.1, 104.9, 105.4, 105.2, 105.8, 105.6, 106.2, 105.9, 106.5,
        106.3, 106.9, 106.7, 107.3, 107.0, 107.6, 107.4, 108.0, 107.8, 108.4,
        108.2, 108.8, 108.5, 109.1, 108.9, 109.5, 109.2, 109.8, 109.6, 110.1,
        109.9, 110.4, 110.2, 110.8, 110.5, 111.1, 110.9, 111.4, 111.2, 111.8,
        111.6, 112.1, 111.9, 112.5, 112.2, 112.8, 112.6, 113.1, 112.9, 113.5,
    ]
    bars = []
    for index, close in enumerate(closes):
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close - 0.6,
                high=close + 0.8,
                low=close - 1.0,
                close=close,
                volume=1_000_000 + (index * 5_000),
            )
        )
    return bars


def build_intraday_bars() -> list[Bar]:
    base = datetime(2026, 3, 14, 13, 30, tzinfo=timezone.utc)
    closes = [
        112.2, 112.4, 112.3, 112.6, 112.5, 112.7, 112.6, 112.9, 112.8, 113.0,
        112.9, 113.1, 113.0, 113.2, 113.1, 113.3, 113.2, 113.4, 113.3, 113.5,
    ]
    bars = []
    for index, close in enumerate(closes):
        bars.append(
            Bar(
                timestamp=base + timedelta(minutes=5 * index),
                open=close - 0.15,
                high=close + 0.25,
                low=close - 0.25,
                close=close,
                volume=120_000 + (index * 2_500),
            )
        )
    return bars


class ScoringServiceTests(unittest.TestCase):
    def test_run_once_processes_event_and_generates_cards(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=Path(temp_dir) / "agent.db",
                normal_alert_min_final_score=0.0,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                }
            )
            event = SourceEvent(
                event_id="",
                source="Reuters",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia rises after analysts cite stronger AI server demand",
                summary="Several desks lifted expectations after supply checks pointed to stronger near-term data center demand.",
                published_at=utcnow() - timedelta(hours=2),
                url="https://example.com/nvda",
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
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            result = service.run_once()
            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)
            self.assertGreaterEqual(result["alerts_sent"], 1)

    def test_run_once_deduplicates_same_event_id_within_fetched_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                }
            )
            published_at = utcnow() - timedelta(hours=2)
            event_a = SourceEvent(
                event_id="same-event",
                source="Reuters",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia rises after analysts cite stronger AI server demand",
                summary="Several desks lifted expectations.",
                published_at=published_at,
                url="https://example.com/nvda-a",
            )
            event_b = SourceEvent(
                event_id="same-event",
                source="Reuters",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia rises after analysts cite stronger AI server demand",
                summary="Several desks lifted expectations.",
                published_at=published_at + timedelta(minutes=1),
                url="https://example.com/nvda-b",
            )
            service = SatelliteAgentService(
                settings=Settings(dry_run=True, database_path=Path(temp_dir) / "agent.db"),
                store=store,
                source_adapter=StaticSourceAdapter([event_a, event_b]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()
            run = store.load_latest_run()
            summary = json.loads(run["summary_json"])

            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(summary["events_fetched"], 1)
            self.assertEqual(summary["skipped_duplicate_event_id"], 0)

    def test_run_once_selects_only_a_few_high_quality_alerts(self) -> None:
        class FakeExtractor:
            def extract(self, event: SourceEvent) -> EventInsight:
                return EventInsight(
                    event_id=event.event_id,
                    symbol=event.symbol,
                    event_type="news",
                    headline_summary=event.headline,
                    bull_case="Bull case",
                    bear_case="Bear case",
                    importance=0.8,
                    source_credibility=0.8,
                    novelty=0.8,
                    sentiment=0.8,
                    theme_relevance=0.8,
                    llm_confidence=0.8,
                    risk_notes=["risk"],
                    source_refs=[event.url],
                )

        class FakeMarketData:
            def snapshot(self, symbol: str, horizon: str) -> IndicatorSnapshot:
                now = utcnow()
                return IndicatorSnapshot(
                    symbol=symbol,
                    horizon=horizon,
                    as_of=now,
                    last_price=100.0,
                    rsi_14=55.0,
                    atr_14=2.0,
                    sma_20=99.0,
                    sma_60=98.0,
                    relative_volume=1.2,
                    support_20=97.0,
                    resistance_20=103.0,
                    support_60=95.0,
                    resistance_60=105.0,
                    gap_percent=0.0,
                    intraday_breakout=False,
                    is_pullback=True,
                    trend_state="uptrend",
                    atr_percent=2.0,
                )

        class FakeScorer:
            SCORES = {
                ("AAA", "swing"): (84.0, "high"),
                ("AAA", "position"): (74.0, "normal"),
                ("BBB", "swing"): (75.0, "high"),
                ("BBB", "position"): (72.0, "normal"),
                ("CCC", "swing"): (71.0, "normal"),
                ("CCC", "position"): (70.0, "normal"),
            }

            def score(self, insight: EventInsight, snapshot: IndicatorSnapshot) -> OpportunityCard:
                final_score, priority = self.SCORES[(insight.symbol, snapshot.horizon)]
                now = utcnow()
                return OpportunityCard(
                    card_id=f"{insight.symbol}-{snapshot.horizon}",
                    event_id=insight.event_id,
                    symbol=insight.symbol,
                    horizon=snapshot.horizon,
                    event_type=insight.event_type,
                    headline_summary=insight.headline_summary,
                    bull_case=insight.bull_case,
                    bear_case=insight.bear_case,
                    event_score=80.0,
                    market_score=75.0,
                    final_score=final_score,
                    entry_range=PriceRange(99.0, 101.0),
                    take_profit_range=PriceRange(104.0, 108.0),
                    invalidation_level=96.0,
                    invalidation_reason="Break below support",
                    risk_notes=insight.risk_notes,
                    source_refs=insight.source_refs,
                    created_at=now,
                    ttl=now + timedelta(days=3),
                    priority=priority,
                    dedup_key=f"{insight.symbol}:{snapshot.horizon}",
                    bias="long",
                    reason_to_watch="watch",
                )

        class IdentityEntryExit:
            def enrich(self, card: OpportunityCard, snapshot: IndicatorSnapshot) -> OpportunityCard:
                return card

        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["AAA", "BBB", "CCC"], "stock")
            published_at = utcnow() - timedelta(hours=2)
            events = [
                SourceEvent(
                    event_id=f"evt-{symbol}",
                    source="Google News",
                    source_type="news",
                    symbol=symbol,
                    headline=f"{symbol} event",
                    summary=f"{symbol} summary",
                    published_at=published_at + timedelta(minutes=index),
                    url=f"https://example.com/{symbol.lower()}",
                )
                for index, symbol in enumerate(["AAA", "BBB", "CCC"])
            ]
            settings = Settings(
                dry_run=True,
                database_path=Path(temp_dir) / "agent.db",
                max_alerts_per_run=3,
                max_alerts_per_symbol_per_run=1,
                normal_alert_min_final_score=73.0,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter(events),
                normalizer=EventNormalizer(),
                extractor=FakeExtractor(),
                market_data=FakeMarketData(),
                scorer=FakeScorer(),
                entry_exit=IdentityEntryExit(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()
            rows = store.connection.execute(
                "SELECT symbol, horizon, sent, reason FROM alert_history ORDER BY alert_id ASC"
            ).fetchall()
            sent_rows = [row for row in rows if row["sent"] == 1]
            skipped_reasons = {row["reason"] for row in rows if row["sent"] == 0}

            self.assertEqual(result["alerts_sent"], 2)
            self.assertEqual(len(sent_rows), 2)
            self.assertEqual({row["symbol"] for row in sent_rows}, {"AAA", "BBB"})
            self.assertIn("symbol_alert_budget_exhausted", skipped_reasons)
            self.assertIn("quality_cutoff", skipped_reasons)

    def test_run_once_reuses_market_snapshot_for_same_symbol_within_run(self) -> None:
        class CountingMarketData:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def snapshot(self, symbol: str, horizon: str) -> IndicatorSnapshot:
                self.calls.append((symbol, horizon))
                now = utcnow()
                return IndicatorSnapshot(
                    symbol=symbol,
                    horizon=horizon,
                    as_of=now,
                    last_price=100.0,
                    rsi_14=55.0,
                    atr_14=2.0,
                    sma_20=99.0,
                    sma_60=98.0,
                    relative_volume=1.2,
                    support_20=97.0,
                    resistance_20=103.0,
                    support_60=95.0,
                    resistance_60=105.0,
                    gap_percent=0.0,
                    intraday_breakout=False,
                    is_pullback=True,
                    trend_state="bullish",
                    atr_percent=2.0,
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            now = utcnow()
            events = [
                SourceEvent(
                    event_id=f"evt-{index}",
                    source="Reuters",
                    source_type="news",
                    symbol="NVDA",
                    headline=f"Nvidia item {index}",
                    summary="Nvidia update",
                    published_at=now - timedelta(minutes=10 - index),
                    url=f"https://example.com/nvda-{index}",
                )
                for index in range(2)
            ]
            market_data = CountingMarketData()
            settings = Settings(dry_run=True, database_path=Path(temp_dir) / "agent.db", normal_alert_min_final_score=0.0)
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter(events),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=market_data,
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            service.run_once()

            self.assertEqual(sorted(market_data.calls), [("NVDA", "position"), ("NVDA", "swing")])


if __name__ == "__main__":
    unittest.main()
