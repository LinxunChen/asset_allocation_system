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

from satellite_agent.archive import archive_decision_history
from satellite_agent.main import (
    build_non_executable_outcome_cleanup_payload,
    format_decision_history_archive,
    format_decision_outcome_backfill,
    format_non_executable_outcome_cleanup,
)
from satellite_agent.market_data import MultiSourceMarketDataError
from satellite_agent.models import Bar
from satellite_agent.outcomes import backfill_decision_outcomes, compute_decision_outcome
from satellite_agent.store import Store


def build_daily_bars() -> list[Bar]:
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    closes = [100.0, 102.0, 103.0, 105.0, 104.0, 107.0, 109.0, 108.0, 110.0, 111.0, 112.0, 113.0]
    bars: list[Bar] = []
    for index, close in enumerate(closes):
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=1_000_000 + index * 1000,
            )
        )
    return bars


def build_take_profit_then_invalidation_bars() -> list[Bar]:
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    candles = [
        (100.0, 101.0, 99.0),
        (100.0, 109.0, 99.5),
        (101.0, 102.0, 95.0),
        (99.0, 100.0, 94.0),
    ]
    bars: list[Bar] = []
    for index, (close, high, low) in enumerate(candles):
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=close,
                high=high,
                low=low,
                close=close,
                volume=1_000_000 + index * 1000,
            )
        )
    return bars


def build_open_below_invalidation_then_entry_overlap_bars() -> list[Bar]:
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    candles = [
        (97.5, 101.5, 96.5),
        (100.0, 102.0, 99.0),
        (101.0, 103.0, 100.0),
        (102.0, 104.0, 101.0),
        (103.0, 105.0, 102.0),
        (104.0, 106.0, 103.0),
        (105.0, 107.0, 104.0),
        (106.0, 108.0, 105.0),
        (107.0, 109.0, 106.0),
        (108.0, 110.0, 107.0),
        (109.0, 111.0, 108.0),
    ]
    bars: list[Bar] = []
    for index, (open_price, high, low) in enumerate(candles):
        close = open_price
        bars.append(
            Bar(
                timestamp=base + timedelta(days=index),
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=1_000_000 + index * 1000,
            )
        )
    return bars


class DecisionOutcomeTests(unittest.TestCase):
    def test_compute_decision_outcome_uses_decision_timestamp_for_same_session_entry(self) -> None:
        bars = build_daily_bars()
        created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()

        outcome = compute_decision_outcome(
            {
                "decision_id": "decision-same-session",
                "created_at": created_at,
                "entry_plan_json": '{"entry_range":{"low":99.0,"high":100.5},"take_profit_range":{"low":106.0,"high":110.0},"invalidation_level":98.0}',
                "invalidation_json": '{"level":98.0}',
                "packet_json": "{}",
            },
            bars,
        )

        self.assertIsNotNone(outcome)
        assert outcome is not None
        self.assertTrue(outcome.entered)
        self.assertEqual(outcome.entered_at, created_at)

    def test_cleanup_decision_outcomes_dry_run_and_apply(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-watch-cleanup",
                run_id="run-cleanup",
                event_id="evt-watch-cleanup",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="加入观察",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_record(
                decision_id="decision-exec-cleanup",
                run_id="run-cleanup",
                event_id="evt-exec-cleanup",
                symbol="AAPL",
                event_type="product_launch",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=78.0,
                market_score=69.0,
                theme_score=7.0,
                final_score=80.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["consumer_tech"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-watch-cleanup",
                entered=True,
                entered_at=created_at,
                entry_price=100.0,
                t_plus_3_return=1.0,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-exec-cleanup",
                entered=True,
                entered_at=created_at,
                entry_price=100.0,
                t_plus_3_return=2.0,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )

            dry_run = build_non_executable_outcome_cleanup_payload(store)
            self.assertEqual(dry_run["mode"], "dry_run")
            self.assertEqual(dry_run["matched_outcomes"], 1)
            self.assertEqual(dry_run["deleted_outcomes"], 0)
            self.assertEqual(dry_run["sample_rows"][0]["decision_id"], "decision-watch-cleanup")
            formatted = format_non_executable_outcome_cleanup(dry_run)
            self.assertIn("当前仅预演统计，未修改数据库。", formatted)
            self.assertIn("加入观察", formatted)

            applied = build_non_executable_outcome_cleanup_payload(store, apply=True)
            self.assertEqual(applied["mode"], "apply")
            self.assertEqual(applied["matched_outcomes"], 1)
            self.assertEqual(applied["deleted_outcomes"], 1)

            watch_rows = store.load_decision_outcomes_for_actions(actions=("加入观察",))
            self.assertEqual(watch_rows, [])
            summary = store.summarize_decision_outcomes(
                "2026-03-01T00:00:00+00:00",
                actions=("试探建仓", "确认做多"),
            )
            self.assertEqual(summary["decision_count"], 1)

    def test_archive_decision_history_dry_run_only_reports_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_store = Store(temp_path / "source.db")
            archive_store = Store(temp_path / "archive.db")
            source_store.initialize()
            archive_store.initialize()
            old_created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()
            new_created_at = datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc).isoformat()
            source_store.save_decision_record(
                decision_id="decision-old",
                run_id="run-old",
                event_id="evt-old",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=old_created_at,
                packet={},
                created_at=old_created_at,
            )
            source_store.save_decision_outcome(
                decision_id="decision-old",
                t_plus_3_return=4.2,
                close_reason="window_complete",
                updated_at=old_created_at,
            )
            source_store.save_decision_record(
                decision_id="decision-new",
                run_id="run-new",
                event_id="evt-new",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=new_created_at,
                packet={},
                created_at=new_created_at,
            )

            payload = archive_decision_history(
                source_store,
                archive_store,
                before="2026-03-10T00:00:00+00:00",
                apply=False,
            )

            self.assertEqual(payload["mode"], "dry_run")
            self.assertEqual(payload["decision_records"], 1)
            self.assertEqual(payload["decision_outcomes"], 1)
            self.assertEqual(payload["deleted_records"], 0)
            self.assertEqual(
                len(source_store.load_decision_history_for_archive(before="2026-03-10T00:00:00+00:00")),
                payload["scanned"],
            )
            archived_rows = archive_store.load_decision_history_for_archive(before="2027-01-01T00:00:00+00:00")
            self.assertEqual(len(archived_rows), 0)
            formatted = format_decision_history_archive({**payload, "archive_db_path": str(temp_path / "archive.db")})
            self.assertIn("模式：预演", formatted)
            self.assertIn("当前仅做预演统计，未修改主库。", formatted)

            source_store.close()
            archive_store.close()

    def test_archive_decision_history_apply_moves_old_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_store = Store(temp_path / "source.db")
            archive_store = Store(temp_path / "archive.db")
            source_store.initialize()
            archive_store.initialize()
            old_created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()
            new_created_at = datetime(2026, 3, 20, 14, 0, tzinfo=timezone.utc).isoformat()
            source_store.save_decision_record(
                decision_id="decision-old",
                run_id="run-old",
                event_id="evt-old",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=old_created_at,
                packet={},
                created_at=old_created_at,
            )
            source_store.save_decision_outcome(
                decision_id="decision-old",
                t_plus_3_return=4.2,
                close_reason="window_complete",
                updated_at=old_created_at,
            )
            source_store.save_decision_record(
                decision_id="decision-new",
                run_id="run-new",
                event_id="evt-new",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=new_created_at,
                packet={},
                created_at=new_created_at,
            )

            payload = archive_decision_history(
                source_store,
                archive_store,
                before="2026-03-10T00:00:00+00:00",
                apply=True,
            )

            self.assertEqual(payload["mode"], "apply")
            self.assertEqual(payload["copied_records"], 1)
            self.assertEqual(payload["copied_outcomes"], 1)
            self.assertEqual(payload["deleted_records"], 1)
            self.assertEqual(payload["deleted_outcomes"], 1)
            source_rows = source_store.load_decision_history_for_archive(before="2027-01-01T00:00:00+00:00")
            self.assertEqual(len(source_rows), 1)
            self.assertEqual(source_rows[0]["decision_id"], "decision-new")
            archived_rows = archive_store.load_decision_history_for_archive(before="2027-01-01T00:00:00+00:00")
            self.assertEqual(len(archived_rows), 1)
            self.assertEqual(archived_rows[0]["decision_id"], "decision-old")
            self.assertEqual(archived_rows[0]["event_type"], "earnings")
            self.assertEqual(archived_rows[0]["t_plus_3_return"], 4.2)
            outcome_rows = archive_store.aggregate_decision_outcomes_by_event_type("2026-01-01T00:00:00+00:00")
            self.assertEqual(len(outcome_rows), 1)
            self.assertEqual(outcome_rows[0]["event_type"], "earnings")
            self.assertEqual(outcome_rows[0]["outcome_count"], 1)
            formatted = format_decision_history_archive({**payload, "archive_db_path": str(temp_path / "archive.db")})
            self.assertIn("模式：正式归档", formatted)
            self.assertIn("已复制：记录 1 / 后验 1", formatted)

            source_store.close()
            archive_store.close()

    def test_compute_decision_outcome_uses_daily_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.upsert_price_bars("NVDA", "1d", build_daily_bars())
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-1",
                run_id="run-1",
                event_id="evt-1",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "take_profit_range": {"low": 108.0, "high": 112.0},
                    "invalidation_level": 99.0,
                },
                invalidation={"level": 99.0, "reason": "跌破关键支撑"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "take_profit_range": {"low": 108.0, "high": 112.0},
                        "invalidation_level": 99.0,
                    }
                },
                created_at=created_at,
            )
            row = store.load_decision_records("run-1")[0]
            outcome = compute_decision_outcome(row, store.load_price_bars("NVDA", "1d", 400))
            self.assertIsNotNone(outcome)
            self.assertEqual(outcome.decision_id, "decision-1")
            self.assertTrue(outcome.hit_take_profit)
            self.assertFalse(outcome.hit_invalidation)
            self.assertIsNotNone(outcome.t_plus_3_return)
            self.assertEqual(outcome.t_plus_10_return, 10.78)
            self.assertEqual(outcome.close_reason, "exit_pool")
            self.assertEqual(outcome.exit_subreason, "target_hit")
            self.assertGreater(outcome.max_runup or 0, 0)

    def test_compute_decision_outcome_falls_back_to_recorded_price_plan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.upsert_price_bars("NVDA", "1d", build_take_profit_then_invalidation_bars())
            created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-fallback",
                run_id="run-fallback",
                event_id="evt-fallback",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={"take_profit_range": {"high": 108.0}},
                invalidation={"level": 96.0, "reason": "跌破关键支撑"},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            row = store.load_decision_records("run-fallback")[0]
            outcome = compute_decision_outcome(row, store.load_price_bars("NVDA", "1d", 400))
            self.assertIsNotNone(outcome)
            self.assertTrue(outcome.entered)
            self.assertTrue(outcome.hit_take_profit)
            self.assertFalse(outcome.hit_invalidation)
            self.assertEqual(outcome.close_reason, "exit_pool")
            self.assertEqual(outcome.exit_subreason, "target_hit")

    def test_compute_decision_outcome_does_not_enter_when_bar_opens_below_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.upsert_price_bars("NVDA", "1d", build_open_below_invalidation_then_entry_overlap_bars())
            created_at = datetime(2026, 3, 1, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-open-below-stop",
                run_id="run-open-below-stop",
                event_id="evt-open-below-stop",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 108.0, "high": 112.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破关键支撑"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 108.0, "high": 112.0},
                        "invalidation_level": 98.0,
                    }
                },
                created_at=created_at,
            )
            row = store.load_decision_records("run-open-below-stop")[0]
            outcome = compute_decision_outcome(row, store.load_price_bars("NVDA", "1d", 400))
            self.assertIsNotNone(outcome)
            self.assertFalse(outcome.entered)
            self.assertEqual(outcome.close_reason, "not_entered")
            self.assertIsNone(outcome.entry_price)
            self.assertIsNone(outcome.exit_price)
            self.assertIsNone(outcome.realized_return)

    def test_backfill_decision_outcomes_only_processes_executable_actions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.upsert_price_bars("NVDA", "1d", build_daily_bars())
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-watch",
                run_id="run-2",
                event_id="evt-watch",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="加入观察",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_record(
                decision_id="decision-exec",
                run_id="run-2",
                event_id="evt-exec",
                symbol="AAPL",
                event_type="product_launch",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=75.0,
                market_score=68.0,
                theme_score=7.0,
                final_score=78.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["consumer_tech"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.upsert_price_bars("AAPL", "1d", build_daily_bars())
            payload = backfill_decision_outcomes(store, run_id="run-2")
            rows = store.load_decision_records("run-2")
            by_id = {row["decision_id"]: row for row in rows}
            self.assertEqual(payload["updated"], 1)
            self.assertEqual(payload["scanned"], 1)
            self.assertEqual(payload["completed_window"], 1)
            self.assertEqual(payload["pending_lookahead"], 0)
            self.assertEqual(payload["take_profit_hits"], 0)
            self.assertIsNone(by_id["decision-watch"]["t_plus_3_return"])
            self.assertIsNone(by_id["decision-watch"]["entered"])
            self.assertIsNotNone(by_id["decision-exec"]["t_plus_3_return"])
            self.assertEqual(by_id["decision-exec"]["t_plus_10_return"], 10.78)
            formatted = format_decision_outcome_backfill({**payload, "run_id": "run-2"})
            self.assertIn("成功回写：1", formatted)
            self.assertIn("完整窗口 1 / 等待更多 bars 0", formatted)
            self.assertIn("命中情况：止盈 0 / 失效 0", formatted)

    def test_decision_outcome_summaries_can_filter_to_executable_actions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-watch-summary",
                run_id="run-summary",
                event_id="evt-watch-summary",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="加入观察",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_record(
                decision_id="decision-exec-summary",
                run_id="run-summary",
                event_id="evt-exec-summary",
                symbol="AAPL",
                event_type="product_launch",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=78.0,
                market_score=69.0,
                theme_score=7.0,
                final_score=80.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["consumer_tech"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-watch-summary",
                entered=True,
                entered_at=created_at,
                entry_price=100.0,
                t_plus_3_return=1.0,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-exec-summary",
                entered=True,
                entered_at=created_at,
                entry_price=100.0,
                t_plus_3_return=2.0,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )

            summary = store.summarize_decision_outcomes(
                "2026-03-01T00:00:00+00:00",
                actions=("试探建仓", "确认做多"),
            )
            event_rows = store.aggregate_decision_outcomes_by_event_type(
                "2026-03-01T00:00:00+00:00",
                actions=("试探建仓", "确认做多"),
            )
            pool_rows = store.aggregate_decision_outcomes_by_pool(
                "2026-03-01T00:00:00+00:00",
                actions=("试探建仓", "确认做多"),
            )

            self.assertEqual(summary["decision_count"], 1)
            self.assertEqual(summary["entered_count"], 1)
            self.assertEqual(len(event_rows), 1)
            self.assertEqual(event_rows[0]["decision_count"], 1)
            self.assertEqual(event_rows[0]["entered_count"], 1)
            self.assertEqual(len(pool_rows), 1)
            self.assertEqual(pool_rows[0]["pool"], "confirmation")

    def test_backfill_decision_outcomes_revisits_incomplete_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.upsert_price_bars("NVDA", "1d", build_daily_bars())
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-incomplete",
                run_id="run-incomplete",
                event_id="evt-incomplete",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={"take_profit_range": {"low": 108.0, "high": 112.0}},
                invalidation={"level": 99.0, "reason": "跌破关键支撑"},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-incomplete",
                t_plus_1_return=0.5,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )

            payload = backfill_decision_outcomes(store, run_id="run-incomplete")
            row = store.load_decision_records("run-incomplete")[0]

            self.assertEqual(payload["updated"], 1)
            self.assertEqual(payload["skipped"], 0)
            self.assertEqual(row["close_reason"], "exit_pool")
            self.assertEqual(row["exit_subreason"], "target_hit")
            self.assertEqual(row["t_plus_10_return"], 10.78)
            self.assertIsNotNone(row["max_runup"])

    def test_backfill_decision_outcomes_reports_skip_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-no-bars",
                run_id="run-no-bars",
                event_id="evt-no-bars",
                symbol="NVDA",
                event_type="product_launch",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=60.0,
                market_score=55.0,
                theme_score=5.0,
                final_score=61.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )

            payload = backfill_decision_outcomes(store, run_id="run-no-bars")
            formatted = format_decision_outcome_backfill({**payload, "run_id": "run-no-bars"})

            self.assertEqual(payload["updated"], 0)
            self.assertEqual(payload["skipped"], 1)
            self.assertEqual(payload["skip_reasons"]["missing_bars"], 1)
            self.assertIn("跳过原因：缺少 bars 1 / bars 过旧 0", formatted)
            self.assertIn("缺少 bars 标的：NVDA", formatted)

    def test_backfill_decision_outcomes_reports_stale_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-stale-bars",
                run_id="run-stale-bars",
                event_id="evt-stale-bars",
                symbol="LLY",
                event_type="earnings",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=70.0,
                market_score=65.0,
                theme_score=7.0,
                final_score=71.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["healthcare"],
                entry_plan={"entry_range": {"low": 920.0, "high": 960.0}},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.upsert_price_bars(
                "LLY",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 17, 14, 24, 45, tzinfo=timezone.utc),
                        open=978.92,
                        high=981.16,
                        low=958.0,
                        close=962.41,
                        volume=100000,
                        adjusted=False,
                    )
                ],
            )

            payload = backfill_decision_outcomes(store, run_id="run-stale-bars")
            formatted = format_decision_outcome_backfill({**payload, "run_id": "run-stale-bars"})

            self.assertEqual(payload["updated"], 0)
            self.assertEqual(payload["skipped"], 1)
            self.assertEqual(payload["skip_reasons"]["stale_bars"], 1)
            self.assertIn("bars 过旧 1", formatted)
            self.assertIn("bars 过旧标的：LLY", formatted)

    def test_backfill_decision_outcomes_fetches_missing_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 2, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-fetch-bars",
                run_id="run-fetch-bars",
                event_id="evt-fetch-bars",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )

            fetch_calls: list[tuple[str, str, int]] = []

            def fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
                fetch_calls.append((symbol, timeframe, limit))
                return build_daily_bars()

            payload = backfill_decision_outcomes(
                store,
                run_id="run-fetch-bars",
                fetch_bars=fetch_bars,
            )

            self.assertEqual(payload["updated"], 1)
            self.assertEqual(payload["fetched_symbols"], 1)
            self.assertEqual(payload["fetch_attempted_symbols"], ["NVDA"])
            self.assertEqual(payload["fetch_failed_symbols"], [])
            self.assertEqual(fetch_calls, [("NVDA", "1d", 400)])
            row = store.load_decision_records("run-fetch-bars")[0]
            self.assertIsNotNone(row["t_plus_3_return"])

    def test_backfill_decision_outcomes_fetches_stale_bars_for_missing_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-fetch-stale-bars",
                run_id="run-fetch-stale-bars",
                event_id="evt-fetch-stale-bars",
                symbol="LLY",
                event_type="earnings",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=75.0,
                market_score=67.0,
                theme_score=7.0,
                final_score=76.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["healthcare"],
                entry_plan={
                    "entry_range": {"low": 920.0, "high": 960.0},
                    "take_profit_range": {"low": 980.0, "high": 1000.0},
                    "invalidation_level": 900.0,
                },
                invalidation={"level": 900.0, "reason": "跌破失效位"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 920.0, "high": 960.0},
                        "take_profit_range": {"low": 980.0, "high": 1000.0},
                        "invalidation_level": 900.0,
                    }
                },
                created_at=created_at,
            )
            store.upsert_price_bars(
                "LLY",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 17, 14, 24, 45, tzinfo=timezone.utc),
                        open=978.92,
                        high=981.16,
                        low=958.0,
                        close=962.41,
                        volume=100000,
                        adjusted=False,
                    )
                ],
            )

            fetch_calls: list[tuple[str, str, int]] = []

            def fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
                fetch_calls.append((symbol, timeframe, limit))
                return [
                    Bar(
                        timestamp=datetime(2026, 3, 19, 20, 0, 3, tzinfo=timezone.utc),
                        open=950.0,
                        high=970.0,
                        low=940.0,
                        close=960.0,
                        volume=100000,
                        adjusted=True,
                    )
                ]

            payload = backfill_decision_outcomes(
                store,
                run_id="run-fetch-stale-bars",
                fetch_bars=fetch_bars,
                recompute_existing=True,
            )

            self.assertEqual(payload["updated"], 1)
            self.assertEqual(payload["fetched_symbols"], 1)
            self.assertEqual(payload["fetch_attempted_symbols"], ["LLY"])
            self.assertEqual(payload["fetch_failed_symbols"], [])
            self.assertEqual(fetch_calls, [("LLY", "1d", 400)])
            row = store.load_decision_records("run-fetch-stale-bars")[0]
            self.assertEqual(row["entered"], 1)
            self.assertAlmostEqual(row["entry_price"], 950.0, places=2)

    def test_backfill_decision_outcomes_replaces_unadjusted_daily_bars_when_adjusted_refresh_arrives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-replace-adjusted-bars",
                run_id="run-replace-adjusted-bars",
                event_id="evt-replace-adjusted-bars",
                symbol="AAPL",
                event_type="guidance",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=78.0,
                market_score=69.0,
                theme_score=7.0,
                final_score=80.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["consumer_tech"],
                entry_plan={"entry_range": {"low": 210.0, "high": 215.0}},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.upsert_price_bars(
                "AAPL",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 17, 0, 0, tzinfo=timezone.utc),
                        open=208.0,
                        high=209.0,
                        low=207.0,
                        close=208.5,
                        volume=100000,
                        adjusted=False,
                    )
                ],
            )

            payload = backfill_decision_outcomes(
                store,
                run_id="run-replace-adjusted-bars",
                fetch_bars=lambda symbol, timeframe, limit: [
                    Bar(
                        timestamp=datetime(2026, 3, 18, 20, 0, 0, tzinfo=timezone.utc),
                        open=211.0,
                        high=214.0,
                        low=209.5,
                        close=213.0,
                        volume=120000,
                        adjusted=True,
                    ),
                    Bar(
                        timestamp=datetime(2026, 3, 19, 20, 0, 0, tzinfo=timezone.utc),
                        open=212.0,
                        high=216.0,
                        low=210.0,
                        close=215.0,
                        volume=130000,
                        adjusted=True,
                    ),
                ],
                recompute_existing=True,
            )

            self.assertEqual(payload["updated"], 1)
            bars = store.load_price_bars("AAPL", "1d", 10)
            self.assertEqual(len(bars), 2)
            self.assertTrue(all(bar.adjusted for bar in bars))
            summary = store.summarize_price_bar_adjustment(["AAPL"], "1d")[0]
            self.assertEqual(int(summary["unadjusted_bars"]), 0)

    def test_backfill_decision_outcomes_refreshes_unadjusted_bars_even_when_not_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-refresh-unadjusted",
                run_id="run-refresh-unadjusted",
                event_id="evt-refresh-unadjusted",
                symbol="MSFT",
                event_type="guidance",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=79.0,
                market_score=70.0,
                theme_score=7.0,
                final_score=81.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["software"],
                entry_plan={"entry_range": {"low": 380.0, "high": 390.0}},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.upsert_price_bars(
                "MSFT",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 18, 0, 0, tzinfo=timezone.utc),
                        open=386.0,
                        high=389.0,
                        low=384.0,
                        close=388.0,
                        volume=100000,
                        adjusted=False,
                    ),
                    Bar(
                        timestamp=datetime(2026, 3, 19, 0, 0, tzinfo=timezone.utc),
                        open=387.0,
                        high=390.0,
                        low=385.0,
                        close=389.0,
                        volume=110000,
                        adjusted=False,
                    ),
                ],
            )
            fetch_calls: list[tuple[str, str, int]] = []

            def fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
                fetch_calls.append((symbol, timeframe, limit))
                return [
                    Bar(
                        timestamp=datetime(2026, 3, 18, 20, 0, 0, tzinfo=timezone.utc),
                        open=386.5,
                        high=389.5,
                        low=384.5,
                        close=388.5,
                        volume=100000,
                        adjusted=True,
                    ),
                    Bar(
                        timestamp=datetime(2026, 3, 19, 20, 0, 0, tzinfo=timezone.utc),
                        open=387.5,
                        high=390.5,
                        low=385.5,
                        close=389.5,
                        volume=110000,
                        adjusted=True,
                    ),
                ]

            payload = backfill_decision_outcomes(
                store,
                run_id="run-refresh-unadjusted",
                fetch_bars=fetch_bars,
                recompute_existing=True,
            )

            self.assertEqual(payload["fetch_attempted_symbols"], ["MSFT"])
            self.assertEqual(fetch_calls, [("MSFT", "1d", 400)])
            bars = store.load_price_bars("MSFT", "1d", 10)
            self.assertEqual(len(bars), 2)
            self.assertTrue(all(bar.adjusted for bar in bars))

    def test_backfill_decision_outcomes_reports_failed_remote_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-refresh-failed",
                run_id="run-refresh-failed",
                event_id="evt-refresh-failed",
                symbol="LLY",
                event_type="earnings",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=70.0,
                market_score=65.0,
                theme_score=7.0,
                final_score=71.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["healthcare"],
                entry_plan={"entry_range": {"low": 920.0, "high": 960.0}},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )
            store.upsert_price_bars(
                "LLY",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 17, 14, 24, 45, tzinfo=timezone.utc),
                        open=978.92,
                        high=981.16,
                        low=958.0,
                        close=962.41,
                        volume=100000,
                        adjusted=False,
                    )
                ],
            )

            payload = backfill_decision_outcomes(
                store,
                run_id="run-refresh-failed",
                fetch_bars=lambda symbol, timeframe, limit: [],
                recompute_existing=True,
            )
            formatted = format_decision_outcome_backfill({**payload, "run_id": "run-refresh-failed"})

            self.assertEqual(payload["fetch_attempted_symbols"], ["LLY"])
            self.assertEqual(payload["fetch_failed_symbols"], ["LLY"])
            self.assertEqual(payload["fetch_failure_reasons"]["LLY"], "empty_response")
            self.assertEqual(payload["stale_symbol_details"]["LLY"]["target_session"], "2026-03-19")
            self.assertEqual(payload["stale_symbol_details"]["LLY"]["latest_local_session"], "2026-03-17")
            self.assertIn("远程补抓尝试：LLY", formatted)
            self.assertIn("远程补抓未更新：LLY", formatted)
            self.assertIn("远程补抓原因：LLY=empty_response", formatted)
            self.assertIn("过旧详情：LLY(目标 2026-03-19 / 本地最新 2026-03-17 / 远程最新 -)", formatted)

    def test_backfill_decision_outcomes_can_recompute_existing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, 26, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-recompute",
                run_id="run-recompute",
                event_id="evt-recompute",
                symbol="NVDA",
                event_type="guidance",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=70.0,
                market_score=66.0,
                theme_score=7.0,
                final_score=71.0,
                trigger_mode="event",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 175.81, "high": 179.66},
                    "take_profit_range": {"low": 184.07, "high": 189.58},
                    "invalidation_level": 171.67,
                },
                invalidation={"level": 171.67, "reason": "跌破失效位"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 175.81, "high": 179.66},
                        "take_profit_range": {"low": 184.07, "high": 189.58},
                        "invalidation_level": 171.67,
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-recompute",
                entered=False,
                close_reason="insufficient_lookahead",
                updated_at=created_at,
            )
            store.upsert_price_bars(
                "NVDA",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 19, 0, 0, tzinfo=timezone.utc),
                        open=178.01,
                        high=179.98,
                        low=175.785,
                        close=178.56,
                        volume=100000,
                        adjusted=True,
                    )
                ],
            )

            payload = backfill_decision_outcomes(
                store,
                run_id="run-recompute",
                recompute_existing=True,
            )

            self.assertEqual(payload["updated"], 1)
            row = store.load_decision_records("run-recompute")[0]
            self.assertEqual(row["entered"], 1)
            self.assertAlmostEqual(row["entry_price"], 178.01, places=2)
            self.assertEqual(row["close_reason"], "insufficient_lookahead")

    def test_backfill_decision_outcomes_formats_provider_level_fetch_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            created_at = datetime(2026, 3, 19, 22, 37, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-provider-failure",
                run_id="run-provider-failure",
                event_id="evt-provider-failure",
                symbol="BRK.B",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=80.0,
                market_score=70.0,
                theme_score=8.0,
                final_score=82.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["financials"],
                entry_plan={"entry_range": {"low": 450.0, "high": 460.0}},
                invalidation={},
                ttl=created_at,
                packet={},
                created_at=created_at,
            )

            def fetch_bars(symbol: str, timeframe: str, limit: int) -> list[Bar]:
                raise MultiSourceMarketDataError(
                    symbol,
                    timeframe,
                    {
                        "YahooFinanceMarketDataProvider": "ValueError: no chart result",
                        "StooqDailyMarketDataProvider": "ValueError: no daily bars",
                    },
                )

            payload = backfill_decision_outcomes(
                store,
                run_id="run-provider-failure",
                fetch_bars=fetch_bars,
                recompute_existing=True,
            )
            formatted = format_decision_outcome_backfill({**payload, "run_id": "run-provider-failure"})

            self.assertEqual(
                payload["fetch_failure_reasons"]["BRK.B"],
                "YahooFinanceMarketDataProvider=ValueError: no chart result, StooqDailyMarketDataProvider=ValueError: no daily bars",
            )
            self.assertIn("远程补抓未更新：BRK.B", formatted)
            self.assertIn(
                "远程补抓原因：BRK.B=YahooFinanceMarketDataProvider=ValueError: no chart result, StooqDailyMarketDataProvider=ValueError: no daily bars",
                formatted,
            )


if __name__ == "__main__":
    unittest.main()
