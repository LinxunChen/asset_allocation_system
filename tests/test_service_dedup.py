from __future__ import annotations

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
from satellite_agent.models import AlertDecision, Bar, EventInsight, OpportunityCard, PriceRange, SourceEvent, utcnow
from satellite_agent.notifier import Notifier
from satellite_agent.scoring import SignalScorer
from satellite_agent.service import SatelliteAgentService
from satellite_agent.sources import StaticSourceAdapter
from satellite_agent.store import Store


def _daily_bars() -> list[Bar]:
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bars = []
    for index in range(70):
        close = 100 + (index * 0.4)
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close - 0.5,
                high=close + 0.8,
                low=close - 1.0,
                close=close,
                volume=1_000_000 + (index * 10_000),
            )
        )
    return bars


def _intraday_bars() -> list[Bar]:
    base = datetime(2026, 3, 14, 13, 30, tzinfo=timezone.utc)
    bars = []
    for index in range(20):
        close = 127 + (index * 0.15)
        bars.append(
            Bar(
                timestamp=base + timedelta(minutes=5 * index),
                open=close - 0.1,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=150_000 + (index * 4_000),
            )
        )
    return bars


class ServiceDedupTests(unittest.TestCase):
    def _make_service(self, db_path: Path, store: Store) -> SatelliteAgentService:
        return SatelliteAgentService(
            settings=Settings(
                database_path=db_path,
                dry_run=True,
            ),
            store=store,
            source_adapter=StaticSourceAdapter([]),
            normalizer=EventNormalizer(),
            extractor=RuleBasedExtractor(),
            market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
            scorer=SignalScorer(Settings()),
            entry_exit=EntryExitEngine(),
            notifier=Notifier(store=store, transport=None, dry_run=True),
        )

    def _make_card(self, *, symbol: str, event_id: str, action_label: str, created_at: datetime) -> OpportunityCard:
        return OpportunityCard(
            card_id=f"card-{event_id}",
            event_id=event_id,
            symbol=symbol,
            horizon="swing",
            event_type="earnings",
            headline_summary=f"{symbol} catalyst",
            bull_case="Bull",
            bear_case="Bear",
            event_score=80.0,
            market_score=70.0,
            final_score=78.0,
            entry_range=PriceRange(100.0, 101.0),
            take_profit_range=PriceRange(105.0, 108.0),
            invalidation_level=98.0,
            invalidation_reason="Breakdown",
            risk_notes=["Volatility can expand quickly."],
            source_refs=["https://example.com"],
            created_at=created_at,
            ttl=created_at + timedelta(days=7),
            priority="normal",
            dedup_key=f"{symbol}:{event_id}",
            bias="long",
            display_name=symbol,
            action_label=action_label,
        )

    def _make_insight(self, *, symbol: str, event_id: str) -> EventInsight:
        return EventInsight(
            event_id=event_id,
            symbol=symbol,
            event_type="earnings",
            headline_summary=f"{symbol} catalyst",
            bull_case="Bull",
            bear_case="Bear",
            importance=82.0,
            source_credibility=88.0,
            novelty=72.0,
            sentiment=0.8,
            theme_relevance=80.0,
            llm_confidence=75.0,
            risk_notes=["Volatility can expand quickly."],
            source_refs=["https://example.com"],
        )

    def test_chain_summary_resets_after_terminal_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            service = self._make_service(db_path, store)
            now = utcnow()
            old_created_at = (now - timedelta(days=5)).isoformat()
            new_created_at = (now - timedelta(days=1)).isoformat()

            store.save_decision_record(
                decision_id="decision-old",
                run_id="run-1",
                event_id="evt-old",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=78.0,
                market_score=65.0,
                theme_score=8.0,
                final_score=75.0,
                trigger_mode="direct",
                llm_used=False,
                theme_ids=["ai"],
                entry_plan={"entry_range": {"low": 100.0, "high": 101.0}},
                invalidation={"level": 98.0, "reason": "Breakdown"},
                ttl=old_created_at,
                packet={},
                created_at=old_created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-old",
                entered=True,
                entered_at=old_created_at,
                entry_price=101.0,
                exit_price=98.0,
                realized_return=-2.97,
                holding_days=1,
                hit_invalidation=True,
                close_reason="hit_invalidation",
                updated_at=old_created_at,
            )
            store.save_decision_record(
                decision_id="decision-new",
                run_id="run-2",
                event_id="evt-new",
                symbol="NVDA",
                event_type="earnings",
                pool="prewatch",
                action="加入观察",
                priority="suppressed",
                confidence="低",
                event_score=70.0,
                market_score=55.0,
                theme_score=6.0,
                final_score=62.0,
                trigger_mode="structure",
                llm_used=False,
                theme_ids=["ai"],
                entry_plan={"entry_range": {"low": 102.0, "high": 103.0}},
                invalidation={"level": 99.0, "reason": "Breakdown"},
                ttl=new_created_at,
                packet={},
                created_at=new_created_at,
            )

            summary = service._chain_summary_for_symbol("NVDA", current_action="确认做多")

            self.assertEqual(summary, "昨晚加入观察 -> 今日确认做多")

    def test_chain_summary_prefers_sent_alert_nodes_within_active_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            service = self._make_service(db_path, store)
            now = utcnow()
            created_at = now - timedelta(days=2)
            created_at_iso = created_at.isoformat()

            store.save_decision_record(
                decision_id="decision-active",
                run_id="run-1",
                event_id="evt-active",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=85.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=86.0,
                trigger_mode="direct",
                llm_used=False,
                theme_ids=["ai"],
                entry_plan={"entry_range": {"low": 100.0, "high": 101.0}},
                invalidation={"level": 98.0, "reason": "Breakdown"},
                ttl=created_at_iso,
                packet={},
                created_at=created_at_iso,
            )
            card = self._make_card(symbol="NVDA", event_id="evt-active", action_label="加入观察", created_at=created_at)
            store.save_opportunity_card(card, run_id="run-1")
            store.record_alert(
                card,
                AlertDecision(
                    sent=True,
                    priority="suppressed",
                    reason="dry_run",
                    dedup_key=card.dedup_key,
                    notified_at=created_at,
                ),
                run_id="run-1",
            )

            summary = service._chain_summary_for_symbol("NVDA", current_action="确认做多")

            self.assertEqual(summary, "2天前加入观察 -> 今日确认做多")

    def test_macro_overlay_records_penalty_and_action_downgrade(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            service = self._make_service(db_path, store)
            created_at = utcnow() - timedelta(days=1)
            card = self._make_card(symbol="NVDA", event_id="evt-macro", action_label="确认做多", created_at=created_at)
            card.final_score = 84.0
            card.priority = "high"
            decorated = service._decorate_card_with_runtime_context(
                card,
                insight=self._make_insight(symbol="NVDA", event_id="evt-macro"),
                macro_context={
                    "market_regime": "risk_off",
                    "rate_risk": "high",
                    "geopolitical_risk": "low",
                    "macro_risk_score": 90.0,
                },
            )
            self.assertEqual(decorated.action_label, "试探建仓")
            self.assertEqual(decorated.macro_action_before_overlay, "确认做多")
            self.assertEqual(decorated.macro_penalty_applied, 12.0)
            self.assertIn("综合分下调 12.0 分", decorated.macro_overlay_note)
            self.assertIn("动作由「确认做多」降为「试探建仓」", decorated.macro_overlay_note)

    def test_limit_fetched_events_prioritizes_stronger_catalysts_over_generic_news(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NBIS", "MU", "AAPL"], "stock")
            service = SatelliteAgentService(
                settings=Settings(
                    database_path=db_path,
                    dry_run=True,
                    max_events_per_run=2,
                    max_events_per_symbol_per_run=2,
                ),
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple stock price today: everything you need to know",
                    summary="Should investors buy now after the latest move?",
                    published_at=now,
                    url="https://example.com/aapl",
                ),
                SourceEvent(
                    event_id="evt-strategic",
                    source="Google News",
                    source_type="news",
                    symbol="NBIS",
                    headline="Nebius rallies after Meta investment and strategic partnership",
                    summary="Meta invested in new AI infrastructure capacity.",
                    published_at=now - timedelta(minutes=4),
                    url="https://example.com/nbis",
                ),
                SourceEvent(
                    event_id="evt-earnings",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron earnings beat and guidance raise lift AI memory outlook",
                    summary="Management raised forecast after stronger data center demand.",
                    published_at=now - timedelta(minutes=8),
                    url="https://example.com/mu",
                ),
            ]

            selected = service._limit_fetched_events(events)

            self.assertEqual({event.symbol for event in selected}, {"NBIS", "MU"})

    def test_limit_deep_processing_events_respects_budget_and_keeps_best_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NBIS", "MU", "AAPL"], "stock")
            service = SatelliteAgentService(
                settings=Settings(
                    database_path=db_path,
                    dry_run=True,
                    max_deep_events_per_run=2,
                ),
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple stock price today: top 10 facts to know",
                    summary="What you need to know before you buy now.",
                    published_at=now,
                    url="https://example.com/aapl",
                ),
                SourceEvent(
                    event_id="evt-strategic",
                    source="Google News",
                    source_type="news",
                    symbol="NBIS",
                    headline="Nebius signs strategic partnership after fresh AI investment",
                    summary="The deal expands data center capacity for large AI customers.",
                    published_at=now - timedelta(minutes=3),
                    url="https://example.com/nbis",
                ),
                SourceEvent(
                    event_id="evt-filing",
                    source="SEC Edgar",
                    source_type="filing",
                    symbol="MU",
                    headline="8-K filing: Micron provides updated guidance",
                    summary="Micron filed an 8-K after raising outlook.",
                    published_at=now - timedelta(minutes=6),
                    url="https://example.com/mu",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 2)
            self.assertEqual({event.symbol for event in selected}, {"NBIS", "MU"})

    def test_limit_deep_processing_events_prefers_batch_theme_cluster_under_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU", "AAPL", "AMZN"], "stock")
            settings = Settings(
                database_path=db_path,
                dry_run=True,
                max_deep_events_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                                "core_platforms": ["AAPL", "AMZN"],
                            }
                        }
                    }
                },
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-aapl-generic",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now,
                    url="https://example.com/aapl-generic",
                ),
                SourceEvent(
                    event_id="evt-nvda-generic",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=5),
                    url="https://example.com/nvda-generic",
                ),
                SourceEvent(
                    event_id="evt-mu-generic",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=10),
                    url="https://example.com/mu-generic",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 2)
            self.assertEqual({event.symbol for event in selected}, {"NVDA", "MU"})

    def test_limit_deep_processing_events_reserves_theme_representatives_before_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA", "MU", "AAPL", "AMZN", "VRTX"], "stock")
            settings = Settings(
                database_path=db_path,
                dry_run=True,
                max_deep_events_per_run=3,
                max_deep_theme_reservations_per_run=2,
            )
            service = SatelliteAgentService(
                settings=settings,
                store=store,
                source_adapter=StaticSourceAdapter([]),
                normalizer=EventNormalizer(),
                extractor=RuleBasedExtractor(),
                market_data=MarketDataEngine(InMemoryMarketDataProvider(data={})),
                scorer=SignalScorer(Settings()),
                entry_exit=EntryExitEngine(),
                notifier=Notifier(store=store, transport=None, dry_run=True),
                runtime_snapshot={
                    "runtime_config": {
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "MU"],
                                "core_platforms": ["AAPL", "AMZN"],
                                "healthcare": ["VRTX"],
                            }
                        }
                    }
                },
            )
            now = utcnow()
            events = [
                SourceEvent(
                    event_id="evt-aapl-1",
                    source="Google News",
                    source_type="news",
                    symbol="AAPL",
                    headline="Apple strategic partnership boosts platform adoption",
                    summary="AAPL deal expands services reach.",
                    published_at=now,
                    url="https://example.com/aapl-1",
                ),
                SourceEvent(
                    event_id="evt-amzn-1",
                    source="Google News",
                    source_type="news",
                    symbol="AMZN",
                    headline="Amazon strategic partnership boosts platform adoption",
                    summary="AMZN deal expands services reach.",
                    published_at=now - timedelta(minutes=1),
                    url="https://example.com/amzn-1",
                ),
                SourceEvent(
                    event_id="evt-vrtx-1",
                    source="Google News",
                    source_type="news",
                    symbol="VRTX",
                    headline="Vertex strategic partnership boosts platform adoption",
                    summary="VRTX deal expands services reach.",
                    published_at=now - timedelta(minutes=2),
                    url="https://example.com/vrtx-1",
                ),
                SourceEvent(
                    event_id="evt-nvda-generic",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=3),
                    url="https://example.com/nvda-generic",
                ),
                SourceEvent(
                    event_id="evt-mu-generic",
                    source="Google News",
                    source_type="news",
                    symbol="MU",
                    headline="Micron shares move after market update",
                    summary="Investors reacted to a broad market update.",
                    published_at=now - timedelta(minutes=4),
                    url="https://example.com/mu-generic",
                ),
            ]

            selected = service._limit_deep_processing_events(events)

            self.assertEqual(len(selected), 3)
            self.assertIn("NVDA", {event.symbol for event in selected})
            self.assertIn("AAPL", {event.symbol for event in selected})

    def test_cross_source_duplicate_events_are_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            store.seed_watchlist(["NVDA"], "stock")
            provider = InMemoryMarketDataProvider(
                {
                    ("NVDA", "1d"): _daily_bars(),
                    ("NVDA", "5m"): _intraday_bars(),
                }
            )
            published = utcnow() - timedelta(minutes=30)
            events = [
                SourceEvent(
                    event_id="evt-news",
                    source="Google News",
                    source_type="news",
                    symbol="NVDA",
                    headline="Nvidia jumps after upbeat AI server commentary",
                    summary="Analysts noted stronger demand and raised near-term expectations.",
                    published_at=published,
                    url="https://example.com/news",
                ),
                SourceEvent(
                    event_id="evt-research",
                    source="Research Feed",
                    source_type="research",
                    symbol="NVDA",
                    headline="Nvidia jumps after upbeat AI server commentary",
                    summary="Desk commentary pointed to stronger AI demand and better expectations.",
                    published_at=published + timedelta(minutes=10),
                    url="https://example.com/research",
                ),
            ]
            settings = Settings(database_path=db_path, dry_run=True, cross_source_dedup_hours=12)
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
            )
            result = service.run_once()
            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["cards_generated"], 2)


if __name__ == "__main__":
    unittest.main()
