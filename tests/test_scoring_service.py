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
from satellite_agent.llm import NarrativeOutput, OpenAIExtractor, RuleBasedExtractor
from satellite_agent.market_data import InMemoryMarketDataProvider, MarketDataEngine
from satellite_agent.models import Bar, SourceEvent
from satellite_agent.models import EventInsight, IndicatorSnapshot, OpportunityCard, PrewatchCandidate, PriceRange, utcnow
from satellite_agent.notifier import Notifier
from satellite_agent.observability import RunContext, StructuredLogger
from satellite_agent.prewatch import build_prewatch_candidate
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import (
    SATELLITE_STRATEGY_VERSION,
    SatelliteAgentService,
    THEME_CONFIRMATION_CHAIN_BONUS,
    THEME_MEMORY_STATE_KEY,
)
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


def build_daily_bars_with_room_for_targets() -> list[Bar]:
    bars = build_daily_bars()
    adjusted: list[Bar] = []
    for index, bar in enumerate(bars):
        high = bar.high
        if index >= len(bars) - 60:
            high = max(high, bar.close + 20.0)
        adjusted.append(
            Bar(
                timestamp=bar.timestamp,
                open=bar.open,
                high=high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                adjusted=bar.adjusted,
            )
        )
    return adjusted


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


def build_exit_pool_target_hit_bars() -> list[Bar]:
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    prices = [
        (104.0, 105.0, 103.5, 104.6),
        (105.0, 108.5, 104.8, 107.9),
        (108.0, 111.8, 107.5, 111.2),
        (111.3, 114.2, 110.9, 113.6),
        (113.4, 114.8, 112.8, 114.1),
    ]
    bars: list[Bar] = []
    for index, (open_, high, low, close) in enumerate(prices):
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=1_200_000 + index * 20_000,
                adjusted=True,
            )
        )
    return bars


class ScoringServiceTests(unittest.TestCase):
    def test_event_score_weights_can_be_overridden(self) -> None:
        settings = Settings().with_strategy_overrides(
            event_score_weights={
                "importance": 0.5,
                "source_credibility": 0.1,
                "novelty": 0.1,
                "theme_relevance": 0.1,
                "sentiment": 0.2,
            }
        )
        scorer = SignalScorer(settings)
        insight = EventInsight(
            event_id="evt-weights",
            symbol="NVDA",
            event_type="earnings",
            headline_summary="Positive earnings catalyst",
            bull_case="",
            bear_case="",
            importance=80.0,
            source_credibility=70.0,
            novelty=60.0,
            sentiment=0.5,
            theme_relevance=90.0,
            llm_confidence=80.0,
            risk_notes=[],
            source_refs=[],
            raw_payload={},
            created_at=utcnow(),
        )

        self.assertEqual(round(scorer._event_score(insight), 2), 72.0)

    def test_bearish_trend_does_not_directly_confirm_long(self) -> None:
        settings = Settings().with_strategy_overrides(
            event_score_threshold=50.0,
            horizons={
                "swing": {
                    "market_score_threshold": 20.0,
                    "priority_threshold": 60.0,
                }
            },
        )
        scorer = SignalScorer(settings)
        insight = EventInsight(
            event_id="evt-bearish",
            symbol="NVDA",
            event_type="earnings",
            headline_summary="Positive earnings catalyst",
            bull_case="",
            bear_case="",
            importance=90.0,
            source_credibility=90.0,
            novelty=80.0,
            sentiment=0.8,
            theme_relevance=85.0,
            llm_confidence=80.0,
            risk_notes=[],
            source_refs=[],
            raw_payload={},
            created_at=utcnow(),
        )
        snapshot = IndicatorSnapshot(
            symbol="NVDA",
            horizon="swing",
            as_of=utcnow(),
            last_price=100.0,
            rsi_14=58.0,
            atr_14=3.0,
            sma_20=101.0,
            sma_60=102.0,
            relative_volume=1.8,
            support_20=98.0,
            resistance_20=105.0,
            support_60=95.0,
            resistance_60=108.0,
            gap_percent=0.0,
            intraday_breakout=True,
            is_pullback=False,
            trend_state="bearish",
            atr_percent=3.0,
        )

        card = scorer.score(insight, snapshot)

        self.assertNotEqual(card.action_label, "确认做多")
        self.assertIn(card.action_label, {"试探建仓", "加入观察"})

    def test_entry_exit_invalidation_uses_normalized_long_entry_range(self) -> None:
        snapshot = IndicatorSnapshot(
            symbol="AAPL",
            horizon="position",
            as_of=utcnow(),
            last_price=250.0,
            rsi_14=36.0,
            atr_14=5.0,
            sma_20=260.0,
            sma_60=255.0,
            relative_volume=0.9,
            support_20=248.0,
            resistance_20=265.0,
            support_60=245.0,
            resistance_60=270.0,
            gap_percent=0.0,
            intraday_breakout=False,
            is_pullback=False,
            trend_state="bearish",
            atr_percent=2.0,
        )
        now = utcnow()
        card = OpportunityCard(
            card_id="card-aapl",
            event_id="evt-aapl",
            symbol="AAPL",
            horizon="position",
            event_type="news",
            headline_summary="AAPL sample",
            bull_case="",
            bear_case="",
            event_score=70.0,
            market_score=60.0,
            final_score=66.0,
            entry_range=PriceRange(snapshot.last_price, snapshot.last_price),
            take_profit_range=PriceRange(snapshot.last_price, snapshot.last_price),
            invalidation_level=0.0,
            invalidation_reason="",
            risk_notes=[],
            source_refs=[],
            created_at=now,
            ttl=now + timedelta(days=3),
            priority="normal",
            dedup_key="AAPL:evt-aapl:position",
            bias="long",
        )

        enriched = EntryExitEngine().enrich(card, snapshot)

        self.assertLessEqual(enriched.entry_range.low, enriched.entry_range.high)
        self.assertLess(enriched.invalidation_level, enriched.entry_range.low)

    def test_entry_exit_long_take_profit_does_not_fall_below_entry_range(self) -> None:
        snapshot = IndicatorSnapshot(
            symbol="AAPL",
            horizon="swing",
            as_of=utcnow(),
            last_price=100.0,
            rsi_14=55.0,
            atr_14=4.0,
            sma_20=98.0,
            sma_60=96.0,
            relative_volume=1.2,
            support_20=97.0,
            resistance_20=100.5,
            support_60=95.0,
            resistance_60=104.0,
            gap_percent=0.0,
            intraday_breakout=False,
            is_pullback=False,
            trend_state="bullish",
            atr_percent=4.0,
        )
        now = utcnow()
        card = OpportunityCard(
            card_id="card-aapl-tp",
            event_id="evt-aapl-tp",
            symbol="AAPL",
            horizon="swing",
            event_type="news",
            headline_summary="AAPL sample",
            bull_case="",
            bear_case="",
            event_score=70.0,
            market_score=60.0,
            final_score=66.0,
            entry_range=PriceRange(snapshot.last_price, snapshot.last_price),
            take_profit_range=PriceRange(snapshot.last_price, snapshot.last_price),
            invalidation_level=0.0,
            invalidation_reason="",
            risk_notes=[],
            source_refs=[],
            created_at=now,
            ttl=now + timedelta(days=3),
            priority="normal",
            dedup_key="AAPL:evt-aapl-tp:swing",
            bias="long",
        )

        enriched = EntryExitEngine().enrich(card, snapshot)

        self.assertGreaterEqual(enriched.take_profit_range.low, enriched.entry_range.high)
        self.assertGreaterEqual(enriched.take_profit_range.high, enriched.take_profit_range.low)

    def test_run_once_processes_event_and_generates_cards(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=Path(temp_dir) / "agent.db",
                normal_alert_min_final_score=0.0,
            ).with_strategy_overrides(
                event_score_threshold=50.0,
                horizons={
                    "swing": {
                        "market_score_threshold": 50.0,
                        "priority_threshold": 60.0,
                    },
                    "position": {
                        "market_score_threshold": 50.0,
                        "priority_threshold": 60.0,
                    },
                },
            )
            class StaticMarketData:
                def snapshot(self, symbol: str, horizon: str, include_intraday: bool = True):
                    thresholds = {
                        "swing": {
                            "rsi": 66.0,
                            "relative_volume": 1.45,
                            "atr": 3.2,
                            "support_20": 110.8,
                            "resistance_20": 126.5,
                            "support_60": 106.0,
                            "resistance_60": 132.0,
                            "intraday_breakout": True,
                            "is_pullback": False,
                        },
                        "position": {
                            "rsi": 63.0,
                            "relative_volume": 1.28,
                            "atr": 3.6,
                            "support_20": 110.8,
                            "resistance_20": 126.5,
                            "support_60": 106.0,
                            "resistance_60": 136.0,
                            "intraday_breakout": False,
                            "is_pullback": True,
                        },
                    }[horizon]
                    return IndicatorSnapshot(
                        symbol=symbol,
                        horizon=horizon,
                        as_of=utcnow(),
                        last_price=113.5,
                        rsi_14=thresholds["rsi"],
                        atr_14=thresholds["atr"],
                        sma_20=111.6,
                        sma_60=108.2,
                        relative_volume=thresholds["relative_volume"],
                        support_20=thresholds["support_20"],
                        resistance_20=thresholds["resistance_20"],
                        support_60=thresholds["support_60"],
                        resistance_60=thresholds["resistance_60"],
                        gap_percent=0.0,
                        intraday_breakout=thresholds["intraday_breakout"],
                        is_pullback=thresholds["is_pullback"],
                        trend_state="bullish",
                        atr_percent=(thresholds["atr"] / 113.5) * 100,
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
                market_data=StaticMarketData(),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            result = service.run_once()
            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)
            self.assertGreaterEqual(result["alerts_sent"], 1)
            latest_run = store.load_latest_run()
            self.assertIsNotNone(latest_run)
            config_snapshot = json.loads(latest_run["config_snapshot_json"])
            self.assertEqual(config_snapshot["strategy_version"], SATELLITE_STRATEGY_VERSION)
            decision_rows = store.load_decision_records(latest_run["run_id"])
            self.assertGreaterEqual(len(decision_rows), 1)
            first_packet = json.loads(decision_rows[0]["packet_json"])
            self.assertEqual(first_packet["strategy_version"], SATELLITE_STRATEGY_VERSION)
            confirmation_rows = store.load_candidate_evaluations(latest_run["run_id"], stage="confirmation")
            self.assertEqual(len(confirmation_rows), 2)
            self.assertTrue(all(row["outcome"] == "selected" for row in confirmation_rows))
            self.assertTrue(all(row["reason"] == "confirmation_opportunity" for row in confirmation_rows))
            self.assertTrue(all(row["strategy_version"] == SATELLITE_STRATEGY_VERSION for row in confirmation_rows))

    def test_confirmation_candidate_evaluations_capture_rejected_suppressed_cards(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=Path(temp_dir) / "agent.db",
                normal_alert_min_final_score=0.0,
            ).with_strategy_overrides(
                event_score_threshold=95.0,
                horizons={
                    "swing": {
                        "market_score_threshold": 50.0,
                        "priority_threshold": 60.0,
                    },
                    "position": {
                        "market_score_threshold": 50.0,
                        "priority_threshold": 60.0,
                    },
                },
            )

            class StaticMarketData:
                def snapshot(self, symbol: str, horizon: str, include_intraday: bool = True):
                    thresholds = {
                        "swing": {
                            "rsi": 66.0,
                            "relative_volume": 1.45,
                            "atr": 3.2,
                            "support_20": 110.8,
                            "resistance_20": 126.5,
                            "support_60": 106.0,
                            "resistance_60": 132.0,
                            "intraday_breakout": True,
                            "is_pullback": False,
                        },
                        "position": {
                            "rsi": 63.0,
                            "relative_volume": 1.28,
                            "atr": 3.6,
                            "support_20": 110.8,
                            "resistance_20": 126.5,
                            "support_60": 106.0,
                            "resistance_60": 136.0,
                            "intraday_breakout": False,
                            "is_pullback": True,
                        },
                    }[horizon]
                    return IndicatorSnapshot(
                        symbol=symbol,
                        horizon=horizon,
                        as_of=utcnow(),
                        last_price=113.5,
                        rsi_14=thresholds["rsi"],
                        atr_14=thresholds["atr"],
                        sma_20=111.6,
                        sma_60=108.2,
                        relative_volume=thresholds["relative_volume"],
                        support_20=thresholds["support_20"],
                        resistance_20=thresholds["resistance_20"],
                        support_60=thresholds["support_60"],
                        resistance_60=thresholds["resistance_60"],
                        gap_percent=0.0,
                        intraday_breakout=thresholds["intraday_breakout"],
                        is_pullback=thresholds["is_pullback"],
                        trend_state="bullish",
                        atr_percent=(thresholds["atr"] / 113.5) * 100,
                    )

            event = SourceEvent(
                event_id="evt-nvda-suppressed",
                source="Reuters",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia rises after analysts cite stronger AI server demand",
                summary="Several desks lifted expectations after supply checks pointed to stronger near-term data center demand.",
                published_at=utcnow() - timedelta(hours=2),
                url="https://example.com/nvda-suppressed",
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([event]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=StaticMarketData(),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()
            self.assertEqual(result["alerts_sent"], 0)
            latest_run = store.load_latest_run()
            self.assertIsNotNone(latest_run)
            confirmation_rows = store.load_candidate_evaluations(latest_run["run_id"], stage="confirmation")
            self.assertEqual(len(confirmation_rows), 2)
            self.assertTrue(all(row["outcome"] == "rejected" for row in confirmation_rows))
            self.assertTrue(all(row["reason"] == "threshold_not_met" for row in confirmation_rows))

    def test_run_once_generates_degraded_event_only_card_when_market_data_fails_for_strong_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NBIS"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=Path(temp_dir) / "agent.db",
                event_only_alert_min_event_score=78.0,
                normal_alert_min_final_score=73.0,
            )

            class FailingMarketData:
                def snapshot(self, symbol: str, horizon: str, include_intraday: bool = True):
                    raise TimeoutError("simulated timeout")

            event = SourceEvent(
                event_id="evt-nbis",
                source="Google News",
                source_type="news",
                symbol="NBIS",
                headline="Meta signs strategic partnership and investment deal with Nebius",
                summary="Meta invested in Nebius and expanded a strategic AI infrastructure partnership.",
                published_at=utcnow() - timedelta(hours=1),
                url="https://example.com/nbis",
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([event]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=FailingMarketData(),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            result = service.run_once()
            cards = store.connection.execute(
                "SELECT card_json FROM opportunity_cards ORDER BY created_at ASC"
            ).fetchall()

            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)
            parsed_cards = [json.loads(row["card_json"]) for row in cards]
            self.assertTrue(all(card["market_data_complete"] is False for card in parsed_cards))
            self.assertTrue(all(card["priority"] == "high" for card in parsed_cards))
            self.assertTrue(all("行情快照暂不可用" in card["market_data_note"] for card in parsed_cards))

    def test_recent_prewatch_candidate_is_promoted_into_confirmation_pool(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                prewatch_confirmation_bonus=2.0,
                prewatch_confirmation_min_event_score=74.0,
                normal_alert_min_final_score=0.0,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                }
            )
            candidate = PrewatchCandidate(
                symbol="NVDA",
                horizon="position",
                setup_type="breakout_watch",
                score=81.5,
                headline_summary="NVDA 进入预备池",
                action_hint="等待确认",
                reason_to_watch="量价结构改善",
                last_price=113.5,
                rsi_14=58.0,
                relative_volume=1.6,
                trend_state="bullish",
                support_20=108.9,
                resistance_20=114.3,
                as_of=utcnow() - timedelta(hours=6),
            )
            store.set_state(
                "prewatch_candidate:NVDA",
                json.dumps(candidate.to_record(), sort_keys=True),
            )
            event = SourceEvent(
                event_id="evt-promote",
                source="Google News",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia secures strategic AI infrastructure partnership",
                summary="Nvidia secured a strategic AI infrastructure partnership with a hyperscaler.",
                published_at=utcnow() - timedelta(hours=1),
                url="https://example.com/nvda-promote",
            )
            insight = RuleBasedExtractor().extract(EventNormalizer().normalize(event))
            baseline_snapshot = MarketDataEngine(provider).snapshot("NVDA", "position")
            baseline_card = SignalScorer(settings).score(insight, baseline_snapshot)

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

            service.run_once()
            rows = store.load_opportunity_cards(store.load_latest_run()["run_id"])
            cards = [json.loads(row["card_json"]) for row in rows]
            promoted = [card for card in cards if card["horizon"] == "position"][0]

            self.assertTrue(promoted["promoted_from_prewatch"])
            self.assertEqual(promoted["prewatch_setup_type"], "breakout_watch")
            self.assertAlmostEqual(promoted["prewatch_score"], 81.5, places=2)
            self.assertEqual(promoted["prewatch_observation_count"], 1)
            self.assertEqual(promoted["prewatch_alert_sent_count"], 0)
            self.assertIn("累计观察 1 次", promoted["prewatch_promotion_reason"])
            self.assertIn("此前已进入预备池", promoted["reason_to_watch"])
            self.assertIn("正式确认", promoted["positioning_hint"])
            self.assertAlmostEqual(
                promoted["final_score"],
                round(baseline_card.final_score + settings.prewatch_confirmation_bonus, 2),
                places=2,
            )

    def test_theme_linkage_boosts_prewatch_candidate_when_same_theme_has_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "NBIS"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                normal_alert_min_final_score=0.0,
                max_prewatch_candidates_per_run=4,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                    ("NBIS", "1d"): build_daily_bars(),
                }
            )
            event = SourceEvent(
                event_id="evt-theme-nvda",
                source="Google News",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia secures strategic AI infrastructure partnership",
                summary="Nvidia secured a strategic AI infrastructure partnership with a hyperscaler.",
                published_at=utcnow() - timedelta(hours=1),
                url="https://example.com/nvda-theme",
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
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "NBIS"],
                            }
                        }
                    }
                },
            )
            service.prewatch_market_data = MarketDataEngine(provider)

            service.run_once()
            summary = json.loads(store.load_latest_run()["summary_json"])
            candidates = summary["prewatch_candidates"]

            nbis_candidate = next(candidate for candidate in candidates if candidate["symbol"] == "NBIS")
            self.assertIn("NVDA", nbis_candidate["reason_to_watch"])
            self.assertTrue(
                (
                    "同题材已有确认标的" in nbis_candidate["reason_to_watch"]
                    or "同题材预热共振" in nbis_candidate["reason_to_watch"]
                )
            )
            self.assertGreater(nbis_candidate["score"], settings.prewatch_min_score)

    def test_prewatch_scan_records_candidate_evaluation_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["AAA", "BBB", "CCC"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_candidates_per_run=5,
                max_prewatch_scan_symbols_per_run=3,
            )

            class StaticMarketData:
                def snapshot(self, symbol: str, horizon: str, include_intraday: bool = True):
                    snapshots = {
                        "AAA": IndicatorSnapshot(
                            symbol="AAA",
                            horizon=horizon,
                            as_of=utcnow(),
                            last_price=100.0,
                            rsi_14=58.0,
                            atr_14=2.0,
                            sma_20=98.0,
                            sma_60=95.0,
                            relative_volume=1.8,
                            support_20=97.0,
                            resistance_20=103.0,
                            support_60=94.0,
                            resistance_60=108.0,
                            gap_percent=0.0,
                            intraday_breakout=True,
                            is_pullback=False,
                            trend_state="bullish",
                            atr_percent=2.0,
                        ),
                        "BBB": IndicatorSnapshot(
                            symbol="BBB",
                            horizon=horizon,
                            as_of=utcnow(),
                            last_price=100.0,
                            rsi_14=40.0,
                            atr_14=2.0,
                            sma_20=99.0,
                            sma_60=96.0,
                            relative_volume=0.9,
                            support_20=97.0,
                            resistance_20=103.0,
                            support_60=94.0,
                            resistance_60=108.0,
                            gap_percent=0.0,
                            intraday_breakout=False,
                            is_pullback=False,
                            trend_state="bullish",
                            atr_percent=2.0,
                        ),
                    }
                    return snapshots[symbol]

            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=StaticMarketData(),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                prewatch_symbols=["AAA", "BBB", "CCC"],
            )
            def fake_build_single(symbol: str, horizon: str, horizon_settings, snapshot_cache):
                snapshots = {
                    "AAA": IndicatorSnapshot(
                        symbol="AAA",
                        horizon=horizon,
                        as_of=utcnow(),
                        last_price=100.0,
                        rsi_14=58.0,
                        atr_14=2.0,
                        sma_20=98.0,
                        sma_60=95.0,
                        relative_volume=1.8,
                        support_20=97.0,
                        resistance_20=103.0,
                        support_60=94.0,
                        resistance_60=108.0,
                        gap_percent=0.0,
                        intraday_breakout=True,
                        is_pullback=False,
                        trend_state="bullish",
                        atr_percent=2.0,
                    ),
                    "BBB": IndicatorSnapshot(
                        symbol="BBB",
                        horizon=horizon,
                        as_of=utcnow(),
                        last_price=100.0,
                        rsi_14=40.0,
                        atr_14=2.0,
                        sma_20=99.0,
                        sma_60=96.0,
                        relative_volume=0.9,
                        support_20=97.0,
                        resistance_20=103.0,
                        support_60=94.0,
                        resistance_60=108.0,
                        gap_percent=0.0,
                        intraday_breakout=False,
                        is_pullback=False,
                        trend_state="bullish",
                        atr_percent=2.0,
                    ),
                }
                if symbol == "CCC":
                    raise ValueError("simulated prewatch failure")
                snapshot = snapshots[symbol]
                candidate = build_prewatch_candidate(
                    snapshot,
                    horizon_settings,
                    min_score=settings.prewatch_min_score,
                )
                return snapshot, candidate

            service._build_single_prewatch_candidate = fake_build_single  # type: ignore[method-assign]

            result = service.run_once()
            latest_run = store.load_latest_run()
            self.assertIsNotNone(latest_run)
            latest_summary = json.loads(latest_run["summary_json"])
            rows = store.load_candidate_evaluations(latest_run["run_id"], stage="prewatch")
            self.assertEqual(result["prewatch_candidates"], 1)
            self.assertEqual(latest_summary["prewatch_candidates_count"], 1)
            self.assertEqual(len(rows), 3)

            by_symbol = {row["symbol"]: row for row in rows}
            aaa_payload = json.loads(by_symbol["AAA"]["payload_json"])
            bbb_payload = json.loads(by_symbol["BBB"]["payload_json"])
            ccc_payload = json.loads(by_symbol["CCC"]["payload_json"])

            self.assertEqual(by_symbol["AAA"]["outcome"], "selected")
            self.assertEqual(by_symbol["AAA"]["reason"], "ranked_in_run")
            self.assertEqual(by_symbol["AAA"]["strategy_version"], SATELLITE_STRATEGY_VERSION)
            self.assertEqual(aaa_payload["selected_rank"], 1)
            self.assertGreater(float(aaa_payload["prewatch_score"]), settings.prewatch_min_score)

            self.assertEqual(by_symbol["BBB"]["outcome"], "rejected")
            self.assertEqual(by_symbol["BBB"]["reason"], "below_min_score")
            self.assertAlmostEqual(float(by_symbol["BBB"]["score"]), 59.0, places=2)
            self.assertEqual(bbb_payload["rejection_reason"], "below_min_score")
            self.assertEqual(bbb_payload["score_breakdown"]["trend"], 32.0)
            self.assertEqual(bbb_payload["score_breakdown"]["structure"], 14.0)

            self.assertEqual(by_symbol["CCC"]["outcome"], "error")
            self.assertEqual(by_symbol["CCC"]["reason"], "build_failed:ValueError")
            self.assertEqual(ccc_payload["error"], "ValueError")

    def test_theme_confirmed_peer_can_pull_near_threshold_symbol_into_prewatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_candidates_per_run=4,
                prewatch_theme_relaxed_margin=6.0,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                    ("MU", "1d"): build_daily_bars(),
                }
            )
            event = SourceEvent(
                event_id="evt-theme-confirm-nvda",
                source="Google News",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia secures strategic AI infrastructure partnership",
                summary="Nvidia secured a strategic AI infrastructure partnership with a hyperscaler.",
                published_at=utcnow() - timedelta(hours=1),
                url="https://example.com/nvda-theme-confirm",
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
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                            }
                        }
                    }
                },
            )

            def fake_build_single(symbol, horizon, horizon_settings, snapshot_cache):
                snapshot = IndicatorSnapshot(
                    symbol=symbol,
                    horizon=horizon,
                    as_of=utcnow(),
                    last_price=112.0,
                    rsi_14=57.0,
                    atr_14=2.0,
                    sma_20=111.0,
                    sma_60=110.0,
                    relative_volume=1.05,
                    support_20=109.5,
                    resistance_20=113.5,
                    support_60=108.0,
                    resistance_60=114.8,
                    gap_percent=0.0,
                    intraday_breakout=False,
                    is_pullback=False,
                    trend_state="bullish",
                    atr_percent=1.8,
                )
                if symbol == "MU":
                    return snapshot, None
                candidate = PrewatchCandidate(
                    symbol=symbol,
                    horizon=horizon,
                    setup_type="relative_strength_watch",
                    score=62.0,
                    headline_summary=f"{symbol} 进入预备池",
                    action_hint="等待确认",
                    reason_to_watch=f"{symbol} 量价结构改善",
                    last_price=112.0,
                    rsi_14=57.0,
                    relative_volume=1.05,
                    trend_state="bullish",
                    support_20=109.5,
                    resistance_20=113.5,
                    as_of=utcnow(),
                )
                return snapshot, candidate

            service._build_single_prewatch_candidate = fake_build_single  # type: ignore[method-assign]
            service.prewatch_market_data = MarketDataEngine(provider)

            service.run_once()
            summary = json.loads(store.load_latest_run()["summary_json"])
            candidates = summary["prewatch_candidates"]

            mu_candidate = next(candidate for candidate in candidates if candidate["symbol"] == "MU")
            self.assertIn("NVDA", mu_candidate["reason_to_watch"])
            self.assertTrue(
                (
                    "同题材已有确认标的" in mu_candidate["reason_to_watch"]
                    or "同题材预热共振" in mu_candidate["reason_to_watch"]
                )
            )

    def test_recent_theme_memory_boosts_prewatch_scan_priority(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["MU", "UNH"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_scan_symbols_per_run=1,
            )
            store.set_state(
                THEME_MEMORY_STATE_KEY,
                json.dumps(
                    {
                        "as_of": utcnow().isoformat(),
                        "themes": [
                            {
                                "theme_key": "semiconductors_and_ai",
                                "heat_score": 8.0,
                                "confirmed_symbols": ["NVDA"],
                                "prewatch_symbols": ["MU"],
                            }
                        ],
                    },
                    sort_keys=True,
                ),
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["MU"],
                                "healthcare": ["UNH"],
                            }
                        }
                    }
                },
            )

            scan_symbols = service._select_prewatch_scan_symbols({"MU", "UNH"}, {})
            self.assertEqual(scan_symbols, ["MU"])

    def test_recent_theme_memory_can_pull_near_threshold_symbol_into_prewatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["MU"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_candidates_per_run=4,
                prewatch_theme_relaxed_margin=6.0,
            )
            store.set_state(
                THEME_MEMORY_STATE_KEY,
                json.dumps(
                    {
                        "as_of": utcnow().isoformat(),
                        "themes": [
                            {
                                "theme_key": "semiconductors_and_ai",
                                "heat_score": 8.0,
                                "confirmed_symbols": ["NVDA"],
                                "prewatch_symbols": [],
                            }
                        ],
                    },
                    sort_keys=True,
                ),
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["MU"],
                            }
                        }
                    }
                },
            )

            def fake_build_single(symbol, horizon, horizon_settings, snapshot_cache):
                snapshot = IndicatorSnapshot(
                    symbol=symbol,
                    horizon=horizon,
                    as_of=utcnow(),
                    last_price=100.0,
                    rsi_14=44.0,
                    atr_14=8.0,
                    sma_20=101.0,
                    sma_60=99.0,
                    relative_volume=0.9,
                    support_20=98.5,
                    resistance_20=103.0,
                    support_60=96.0,
                    resistance_60=104.5,
                    gap_percent=0.0,
                    intraday_breakout=False,
                    is_pullback=True,
                    trend_state="bullish",
                    atr_percent=11.0,
                )
                return snapshot, None

            service._build_single_prewatch_candidate = fake_build_single  # type: ignore[method-assign]

            service.run_once()
            summary = json.loads(store.load_latest_run()["summary_json"])
            candidates = summary["prewatch_candidates"]

            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["symbol"], "MU")
            self.assertIn("题材近期持续活跃：AI芯片与半导体设备", candidates[0]["reason_to_watch"])

    def test_event_driven_theme_prewatch_can_pull_related_symbol_into_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_candidates_per_run=4,
                prewatch_event_trigger_min_event_score=76.0,
                prewatch_event_min_score=54.0,
                max_event_prewatch_symbols_per_theme_per_run=2,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars_with_room_for_targets(),
                    ("NVDA", "5m"): build_intraday_bars(),
                    ("MU", "1d"): build_daily_bars_with_room_for_targets(),
                    ("MU", "5m"): build_intraday_bars(),
                }
            )
            event = SourceEvent(
                event_id="evt-nvda-theme-seed",
                source="Google News",
                source_type="news",
                symbol="NVDA",
                headline="Nvidia secures strategic AI infrastructure partnership",
                summary="Nvidia secured a strategic AI infrastructure partnership with a hyperscaler.",
                published_at=utcnow() - timedelta(hours=1),
                url="https://example.com/nvda-theme-seed",
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
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                            }
                        }
                    }
                },
            )

            service._select_prewatch_scan_symbols = lambda watchlist, snapshot_cache: []  # type: ignore[method-assign]
            service.prewatch_market_data = MarketDataEngine(provider)

            service.run_once()
            summary = json.loads(store.load_latest_run()["summary_json"])
            candidates = summary["prewatch_candidates"]

            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["symbol"], "MU")
            self.assertEqual(candidates[0]["trigger_mode"], "event")
            self.assertEqual(candidates[0]["trigger_theme"], "AI芯片与半导体设备")
            self.assertEqual(candidates[0]["trigger_symbols"], ["NVDA"])
            self.assertIn("事件预热：AI芯片与半导体设备", candidates[0]["reason_to_watch"])
            self.assertIn("战略合作催化", candidates[0]["reason_to_watch"])
            self.assertIn("触发标的 NVDA", candidates[0]["reason_to_watch"])

    def test_run_once_generates_exit_pool_card_for_target_hit_position(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            store.upsert_price_bars("NVDA", "1d", build_exit_pool_target_hit_bars())
            macro_bars = build_daily_bars()
            for symbol in ("SPY", "QQQ", "SMH", "TLT"):
                store.upsert_price_bars(symbol, "1d", macro_bars)
            created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-confirm-nvda",
                run_id="run-seed",
                event_id="evt-confirm-nvda",
                symbol="NVDA",
                event_type="strategic",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=79.6,
                trigger_mode="direct",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 104.0, "high": 105.0},
                    "take_profit_range": {"low": 110.0, "high": 116.0},
                    "invalidation_level": 100.0,
                },
                invalidation={"level": 100.0, "reason": "跌破关键支撑"},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )

            settings = Settings(
                dry_run=True,
                database_path=db_path,
                use_macro_risk_overlay=True,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("SPY", "1d"): macro_bars,
                    ("QQQ", "1d"): macro_bars,
                    ("SMH", "1d"): macro_bars,
                    ("TLT", "1d"): macro_bars,
                }
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            service.prewatch_market_data = MarketDataEngine(provider)

            service.run_once()

            latest_run = store.load_latest_run()
            summary = json.loads(latest_run["summary_json"])
            self.assertEqual(summary["exit_pool_cards_count"], 1)
            self.assertEqual(summary["exit_pool_symbols"], ["NVDA"])
            self.assertEqual(summary["exit_pool_cards"][0]["subreason"], "target_hit")

            rows = store.load_decision_records(latest_run["run_id"])
            exit_rows = [row for row in rows if row["pool"] == "exit"]
            self.assertEqual(len(exit_rows), 1)
            self.assertEqual(exit_rows[0]["action"], "进入兑现池")
            self.assertEqual(json.loads(exit_rows[0]["packet_json"])["exit_subreason"], "target_hit")

    def test_theme_linkage_adds_chain_bonus_to_promoted_confirmation_card(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                prewatch_confirmation_bonus=2.0,
                prewatch_confirmation_min_event_score=74.0,
                normal_alert_min_final_score=0.0,
            )
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars_with_room_for_targets(),
                    ("NVDA", "5m"): build_intraday_bars(),
                    ("MU", "1d"): build_daily_bars_with_room_for_targets(),
                    ("MU", "5m"): build_intraday_bars(),
                }
            )
            candidate = PrewatchCandidate(
                symbol="NVDA",
                horizon="position",
                setup_type="breakout_watch",
                score=81.5,
                headline_summary="NVDA 进入预备池",
                action_hint="等待确认",
                reason_to_watch="量价结构改善",
                last_price=113.5,
                rsi_14=58.0,
                relative_volume=1.6,
                trend_state="bullish",
                support_20=108.9,
                resistance_20=114.3,
                as_of=utcnow() - timedelta(hours=6),
            )
            store.set_state(
                "prewatch_candidate:NVDA",
                json.dumps(candidate.to_record(), sort_keys=True),
            )
            events = [
                SourceEvent(
                    event_id="evt-promote-theme",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia secures strategic AI infrastructure partnership",
                    summary="Nvidia secured a strategic AI infrastructure partnership with a hyperscaler.",
                    published_at=utcnow() - timedelta(hours=1),
                    url="https://example.com/nvda-promote-theme",
                ),
                SourceEvent(
                    event_id="evt-mu-theme",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron expands AI memory supply partnership",
                    summary="Micron expanded an AI memory partnership with a hyperscaler.",
                    published_at=utcnow() - timedelta(minutes=30),
                    url="https://example.com/mu-theme",
                ),
            ]
            insight = RuleBasedExtractor().extract(EventNormalizer().normalize(events[0]))
            baseline_snapshot = MarketDataEngine(provider).snapshot("NVDA", "position")
            baseline_card = SignalScorer(settings).score(insight, baseline_snapshot)

            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter(events),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                            }
                        }
                    }
                },
            )

            service.run_once()
            rows = store.load_opportunity_cards(store.load_latest_run()["run_id"])
            cards = [json.loads(row["card_json"]) for row in rows]
            promoted = next(card for card in cards if card["symbol"] == "NVDA" and card["horizon"] == "position")

            self.assertTrue(promoted["promoted_from_prewatch"])
            self.assertTrue(
                (
                    "同题材已有确认标的：MU" in promoted["reason_to_watch"]
                    or "预备池" in promoted["reason_to_watch"]
                )
            )
            self.assertTrue(
                (
                    "题材联动正在形成" in promoted["positioning_hint"]
                    or "本次事件触发确认" in promoted["positioning_hint"]
                    or "切换到正式确认" in promoted["positioning_hint"]
                )
            )
            expected_bonus = settings.prewatch_confirmation_bonus
            if promoted.get("confirmed_peer_symbols"):
                expected_bonus += THEME_CONFIRMATION_CHAIN_BONUS
            self.assertAlmostEqual(
                promoted["final_score"],
                round(baseline_card.final_score + expected_bonus, 2),
                places=2,
            )

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

    def test_llm_request_budget_skips_second_event_after_cap(self) -> None:
        class BudgetedOpenAIExtractor(OpenAIExtractor):
            def __init__(self) -> None:
                super().__init__(api_key="test-key", model="test-model", base_url="https://example.com")

            def extract_with_metadata(self, event: SourceEvent):
                insight = RuleBasedExtractor.extract(self, event)
                return insight, {
                    "used_llm": True,
                    "success": True,
                    "reason": "ok",
                    "model": self.model,
                    "prompt_tokens_estimate": 120,
                    "completion_tokens_estimate": 80,
                }

        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.seed_watchlist(["NVDA", "MSFT"], "stock")
            provider = InMemoryMarketDataProvider(
                data={
                    ("NVDA", "1d"): build_daily_bars(),
                    ("NVDA", "5m"): build_intraday_bars(),
                    ("MSFT", "1d"): build_daily_bars(),
                    ("MSFT", "5m"): build_intraday_bars(),
                }
            )
            events = [
                SourceEvent(
                    event_id="evt-llm-1",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="NVDA signs strategic partnership",
                    summary="NVIDIA expands strategic AI infrastructure collaboration.",
                    published_at=utcnow() - timedelta(minutes=10),
                    url="https://example.com/nvda",
                ),
                SourceEvent(
                    event_id="evt-llm-2",
                    source="Google News",
                    source_type="news",
                    symbol="MSFT",
                    headline="MSFT expands enterprise AI deal",
                    summary="Microsoft extends a strategic AI agreement.",
                    published_at=utcnow() - timedelta(minutes=8),
                    url="https://example.com/msft",
                ),
            ]
            service = SatelliteAgentService(
                settings=Settings(
                    dry_run=True,
                    database_path=Path(temp_dir) / "agent.db",
                    openai_api_key="test-key",
                    openai_model="test-model",
                    use_llm_event_extraction=True,
                    llm_max_requests_per_run=1,
                    llm_max_requests_per_day=10,
                    normal_alert_min_final_score=100.0,
                ),
                store=store,
                source_adapter=StaticSourceAdapter(events),
                normalizer=EventNormalizer(),
                extractor=BudgetedOpenAIExtractor(),
                market_data=MarketDataEngine(provider),
                scorer=SignalScorer(Settings(dry_run=True, database_path=Path(temp_dir) / "agent.db")),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            service.run_once()

            rows = store.connection.execute(
                """
                SELECT event_id, used_llm, success, reason
                FROM llm_usage
                WHERE component = 'event_extraction'
                ORDER BY usage_id ASC
                """
            ).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["event_id"], "evt-llm-1")
            self.assertEqual(rows[0]["used_llm"], 1)
            self.assertEqual(rows[0]["reason"], "ok")
            self.assertEqual(rows[1]["event_id"], "evt-llm-2")
            self.assertEqual(rows[1]["used_llm"], 0)
            self.assertEqual(rows[1]["reason"], "per_run_budget_exhausted")

    def test_run_once_limits_prewatch_scan_and_sends_top_light_alerts(self) -> None:
        class DummyTransport:
            def __init__(self) -> None:
                self.messages: list[tuple[str, str]] = []

            def send(self, title: str, body: str) -> None:
                self.messages.append((title, body))

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["AAA", "BBB", "CCC", "DDD"], "stock")
            settings = Settings(
                dry_run=False,
                database_path=db_path,
                max_prewatch_scan_symbols_per_run=2,
                prewatch_alert_min_score=75.0,
                max_prewatch_alerts_per_run=2,
                prewatch_alert_cooldown_minutes=240,
            )
            transport = DummyTransport()
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=transport, dry_run=False),
                prewatch_symbols=["CCC", "AAA", "BBB", "DDD"],
            )

            scanned_symbols: list[str] = []

            def fake_build_single(symbol, horizon, horizon_settings, snapshot_cache):
                scanned_symbols.append(symbol)
                snapshot = IndicatorSnapshot(
                    symbol=symbol,
                    horizon=horizon,
                    as_of=utcnow(),
                    last_price=100.0,
                    rsi_14=58.0,
                    atr_14=2.0,
                    sma_20=99.0,
                    sma_60=98.0,
                    relative_volume=1.8,
                    support_20=97.0,
                    resistance_20=103.0,
                    support_60=95.0,
                    resistance_60=105.0,
                    gap_percent=0.0,
                    intraday_breakout=False,
                    is_pullback=False,
                    trend_state="bullish",
                    atr_percent=2.0,
                )
                score_map = {"CCC": 83.0, "AAA": 79.0, "BBB": 77.0, "DDD": 72.0}
                candidate = PrewatchCandidate(
                    symbol=symbol,
                    horizon=horizon,
                    setup_type="breakout_watch",
                    score=score_map[symbol],
                    headline_summary=f"{symbol} 进入预备池",
                    action_hint="轻仓观察",
                    reason_to_watch=f"{symbol} 量价结构改善",
                    last_price=100.0,
                    rsi_14=58.0,
                    relative_volume=1.8,
                    trend_state="bullish",
                    support_20=97.0,
                    resistance_20=103.0,
                    as_of=utcnow(),
                )
                return snapshot, candidate

            service._build_single_prewatch_candidate = fake_build_single  # type: ignore[method-assign]

            first = service.run_once()
            second = service.run_once()
            latest_run = store.load_latest_run()
            latest_summary = json.loads(latest_run["summary_json"])

            self.assertEqual(set(scanned_symbols), {"CCC", "AAA"})
            self.assertEqual(first["prewatch_alerts_sent"], 2)
            self.assertEqual(len(transport.messages), 2)
            self.assertTrue(transport.messages[0][0].startswith("[预备池]"))
            self.assertEqual(second["prewatch_alerts_sent"], 0)
            self.assertEqual(latest_summary["prewatch_alert_symbols"], [])

    def test_prewatch_alert_skips_repeat_when_content_unchanged_within_repeat_window(self) -> None:
        class DummyTransport:
            def __init__(self) -> None:
                self.messages: list[tuple[str, str]] = []

            def send(self, title: str, body: str) -> None:
                self.messages.append((title, body))

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            settings = Settings(
                dry_run=False,
                database_path=db_path,
                prewatch_alert_min_score=75.0,
                max_prewatch_alerts_per_run=2,
                prewatch_alert_cooldown_minutes=240,
                prewatch_alert_repeat_window_minutes=720,
                prewatch_alert_repeat_min_score_delta=4.0,
            )
            transport = DummyTransport()
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=transport, dry_run=False),
                prewatch_symbols=["NBIS"],
            )
            candidate = PrewatchCandidate(
                symbol="NBIS",
                horizon="position",
                setup_type="breakout_watch",
                score=82.0,
                headline_summary="NBIS 进入预备池",
                action_hint="轻仓观察",
                reason_to_watch="量价结构改善",
                last_price=100.0,
                rsi_14=58.0,
                relative_volume=1.8,
                trend_state="bullish",
                support_20=97.0,
                resistance_20=103.0,
                as_of=utcnow(),
            )
            logger = StructuredLogger(store, "run-prewatch-repeat")
            first_context = RunContext(run_id="run-prewatch-repeat")
            first_sent = service._dispatch_prewatch_notifications(
                [candidate],
                macro_context={"market_regime": "neutral", "rate_risk": "low", "geopolitical_risk": "low", "macro_risk_score": 35.0},
                run_context=first_context,
                logger=logger,
            )
            self.assertEqual(first_sent, ["NBIS"])
            self.assertEqual(len(transport.messages), 1)

            previous_state = json.loads(store.get_state("prewatch_alert:NBIS"))
            previous_state["sent_at"] = (utcnow() - timedelta(hours=5)).isoformat()
            store.set_state("prewatch_alert:NBIS", json.dumps(previous_state, sort_keys=True))

            second_context = RunContext(run_id="run-prewatch-repeat-2")
            second_sent = service._dispatch_prewatch_notifications(
                [candidate],
                macro_context={"market_regime": "neutral", "rate_risk": "low", "geopolitical_risk": "low", "macro_risk_score": 35.0},
                run_context=second_context,
                logger=logger,
            )
            self.assertEqual(second_sent, [])
            self.assertEqual(len(transport.messages), 1)

    def test_prewatch_alert_uses_llm_narration_in_rendered_body(self) -> None:
        class DummyTransport:
            def __init__(self) -> None:
                self.messages: list[tuple[str, str]] = []

            def send(self, title: str, body: str) -> None:
                self.messages.append((title, body))

        class FakeNarrator:
            def narrate_with_metadata(self, **kwargs):
                return (
                    NarrativeOutput(
                        summary="公司披露的新合作仍处在预热阶段，更多细节还没落地。",
                        impact_inference="短线先看合作细节和量能能否继续跟上，确认后才更容易升级。",
                        reasoning="消息先把这只票重新拉回视野，但现在更适合先观察，不适合直接追。",
                        uncertainty="如果后续没有新增细节，关注度可能很快降温。",
                        priority_adjustment=0.0,
                    ),
                    {
                        "used_llm": True,
                        "success": True,
                        "reason": "ok",
                        "model": "fake-narrator",
                        "prompt_tokens_estimate": 10,
                        "completion_tokens_estimate": 10,
                        "latency_ms": 1,
                    },
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            settings = Settings(
                dry_run=False,
                database_path=db_path,
                prewatch_alert_min_score=75.0,
                max_prewatch_alerts_per_run=1,
            )
            transport = DummyTransport()
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=transport, dry_run=False),
                prewatch_symbols=["NBIS"],
            )
            service.narrator = FakeNarrator()  # type: ignore[assignment]
            candidate = PrewatchCandidate(
                symbol="NBIS",
                horizon="position",
                setup_type="breakout_watch",
                score=82.0,
                headline_summary="NBIS 进入预备池",
                action_hint="轻仓观察",
                reason_to_watch="量价结构改善",
                last_price=100.0,
                rsi_14=58.0,
                relative_volume=1.8,
                trend_state="bullish",
                support_20=97.0,
                resistance_20=103.0,
                as_of=utcnow(),
            )
            logger = StructuredLogger(store, "run-prewatch-llm")
            sent = service._dispatch_prewatch_notifications(
                [candidate],
                macro_context={"market_regime": "neutral", "rate_risk": "low", "geopolitical_risk": "low", "macro_risk_score": 35.0},
                run_context=RunContext(run_id="run-prewatch-llm"),
                logger=logger,
            )

            self.assertEqual(sent, ["NBIS"])
            self.assertEqual(len(transport.messages), 1)
            title, body = transport.messages[0]
            self.assertTrue(title.startswith("[预备池]"))
            self.assertIn("事实摘要：公司披露的新合作仍处在预热阶段", body)
            self.assertIn("为什么现在先观察：消息先把这只票重新拉回视野", body)
            self.assertIn("升级触发：短线先看合作细节和量能能否继续跟上", body)

    def test_prewatch_scan_skips_ineligible_and_recent_failure_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["AAA", "BBB", "BRK.B", "TCEHY"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                prewatch_failure_cooldown_minutes=90,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                prewatch_symbols=["BRK.B", "BBB", "AAA", "TCEHY"],
            )

            service._record_prewatch_failure("BBB")
            symbols = service._select_prewatch_scan_symbols(store.load_watchlist(), {})

            self.assertEqual(symbols, ["AAA"])

    def test_prewatch_scan_prefers_larger_theme_groups_under_scan_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU", "AAPL", "LLY"], "stock")
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                max_prewatch_scan_symbols_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                                "core_platforms": ["AAPL"],
                                "healthcare": ["LLY"],
                            }
                        }
                    }
                },
            )

            symbols = service._select_prewatch_scan_symbols(store.load_watchlist(), {})

            self.assertEqual(symbols, ["NVDA", "MU"])

    def test_prewatch_failure_state_can_be_cleared_after_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            settings = Settings(
                dry_run=True,
                database_path=db_path,
                prewatch_failure_cooldown_minutes=90,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(settings),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )

            service._record_prewatch_failure("NVDA")
            self.assertTrue(service._is_prewatch_failure_cooled_down("NVDA"))

            service._clear_prewatch_failure("NVDA")
            self.assertFalse(service._is_prewatch_failure_cooled_down("NVDA"))


if __name__ == "__main__":
    unittest.main()
