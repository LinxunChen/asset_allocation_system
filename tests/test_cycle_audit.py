from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.cycle_audit import (
    build_cycle_audit_payload,
    build_historical_cycle_anomaly_cleanup_payload,
    build_run_cycle_audit_payload,
    format_active_cycle_report,
    format_historical_cycle_anomaly_cleanup,
    format_run_cycle_audit_report,
)
from satellite_agent.main import build_write_cycle_audit_payload
from satellite_agent.store import Store


class CycleAuditTests(unittest.TestCase):
    def _save_decision(
        self,
        store: Store,
        *,
        decision_id: str,
        run_id: str,
        event_id: str,
        symbol: str,
        pool: str,
        action: str,
        created_at: str,
        packet: dict | None = None,
    ) -> None:
        store.save_decision_record(
            decision_id=decision_id,
            run_id=run_id,
            event_id=event_id,
            symbol=symbol,
            event_type="news",
            pool=pool,
            action=action,
            priority="normal",
            confidence="中",
            event_score=75.0,
            market_score=70.0,
            theme_score=66.0,
            final_score=78.0,
            trigger_mode="event",
            llm_used=False,
            theme_ids=["ai"],
            entry_plan={},
            invalidation={},
            ttl="",
            packet=packet or {},
            created_at=created_at,
        )

    def _insert_alert_row(
        self,
        store: Store,
        *,
        card_id: str,
        symbol: str,
        event_id: str,
        notified_at: str,
        card_payload: dict,
    ) -> None:
        store.connection.execute(
            """
            INSERT INTO opportunity_cards
            (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_id,
                "run-alert",
                event_id,
                symbol,
                "position",
                72.0,
                "normal",
                0.0,
                json.dumps(card_payload, sort_keys=True, ensure_ascii=False),
                notified_at,
            ),
        )
        store.connection.execute(
            """
            INSERT INTO alert_history
            (run_id, dedup_key, card_id, symbol, event_id, horizon, priority, final_score, invalidation_level, sent, reason, notified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-alert",
                f"{symbol}:watch",
                card_id,
                symbol,
                event_id,
                "position",
                "normal",
                72.0,
                0.0,
                1,
                "sent",
                notified_at,
            ),
        )
        store.connection.commit()

    def _insert_log_row(
        self,
        store: Store,
        *,
        symbol: str,
        event_id: str,
        created_at: str,
        event_type: str,
        context: dict,
    ) -> None:
        store.connection.execute(
            """
            INSERT INTO system_logs
            (run_id, level, event_type, stage, symbol, event_id, message, context_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-log",
                "INFO",
                event_type,
                "route_into_pools",
                symbol,
                event_id,
                "test",
                json.dumps(context, sort_keys=True, ensure_ascii=False),
                created_at,
            ),
        )
        store.connection.commit()

    def test_cycle_audit_rebuilds_pending_entry_with_downgrade_and_suppression(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            self._save_decision(
                store,
                decision_id="candidate-1",
                run_id="run-candidate",
                event_id="candidate-event",
                symbol="NVDA",
                pool="candidate_pool",
                action="加入观察",
                created_at="2026-03-26T01:00:00+00:00",
                packet={"horizon": "position"},
            )
            store.record_candidate_evaluation(
                run_id="run-a",
                stage="candidate_pool",
                symbol="NVDA",
                horizon="position",
                outcome="selected",
                reason="ranked_in_run",
                score=81.0,
                strategy_version="v1",
                payload={},
                created_at="2026-03-26T01:00:00+00:00",
            )
            store.record_candidate_evaluation(
                run_id="run-b",
                stage="candidate_pool",
                symbol="NVDA",
                horizon="position",
                outcome="selected",
                reason="ranked_in_run",
                score=82.0,
                strategy_version="v1",
                payload={},
                created_at="2026-03-27T02:00:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="formal-1",
                run_id="run-formal",
                event_id="formal-event",
                symbol="NVDA",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-28T01:00:00+00:00",
            )
            self._insert_alert_row(
                store,
                card_id="watch-card-1",
                symbol="NVDA",
                event_id="watch-event",
                notified_at="2026-03-28T02:00:00+00:00",
                card_payload={
                    "delivery_category": "watch",
                    "lifecycle_pool": "candidate",
                    "downgraded_from_formal": True,
                    "previous_formal_action": "确认做多",
                    "action_label": "加入观察",
                },
            )
            self._insert_log_row(
                store,
                symbol="NVDA",
                event_id="suppress-event",
                created_at="2026-03-28T03:00:00+00:00",
                event_type="formal_suppressed_active_holding",
                context={"previous_formal_action": "确认做多", "action_label": "试探建仓"},
            )

            payload = build_cycle_audit_payload(store, limit=10, refresh=True)

            self.assertEqual(len(payload["items"]), 1)
            item = payload["items"][0]
            self.assertEqual(item["symbol"], "NVDA")
            self.assertEqual(item["status"], "pending_entry")
            self.assertEqual(item["candidate_observation_count_72h"], 2)
            self.assertEqual(item["latest_downgraded_watch"]["event_kind"], "formal_downgraded_to_watch_unentered")
            self.assertEqual(item["latest_suppression"]["event_kind"], "suppressed_active_holding")
            self.assertEqual(
                [event["event_kind"] for event in item["events"]],
                [
                    "formal_send",
                    "formal_downgraded_to_watch_unentered",
                    "suppressed_active_holding",
                ],
            )
            report_text = format_active_cycle_report(payload)
            self.assertIn("NVDA | pending_entry", report_text)
            self.assertIn("近72h进入候选池：2 次", report_text)

    def test_cycle_audit_starts_new_cycle_after_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            self._save_decision(
                store,
                decision_id="formal-old",
                run_id="run-old",
                event_id="event-old",
                symbol="AMD",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-26T01:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="formal-old",
                entered=True,
                entered_at="2026-03-26T02:00:00+00:00",
                entry_price=100.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-26T02:00:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="holding-exit",
                run_id="run-old",
                event_id="holding-exit-event",
                symbol="AMD",
                pool="holding_management",
                action="利润保护退出",
                created_at="2026-03-27T01:00:00+00:00",
                packet={
                    "normalized_close_reason": "profit_protection_exit",
                    "source_decision_id": "formal-old",
                },
            )
            self._save_decision(
                store,
                decision_id="formal-new",
                run_id="run-new",
                event_id="event-new",
                symbol="AMD",
                pool="confirmation",
                action="试探建仓",
                created_at="2026-03-28T01:00:00+00:00",
            )

            payload = build_cycle_audit_payload(store, limit=10, refresh=True)

            self.assertEqual(len(payload["items"]), 1)
            item = payload["items"][0]
            self.assertEqual(item["symbol"], "AMD")
            self.assertEqual(item["status"], "pending_entry")
            self.assertEqual(item["last_terminal_reason"], "profit_protection_exit")
            events = store.load_symbol_cycle_events(symbol="AMD")
            self.assertEqual(len({row["cycle_id"] for row in events}), 2)
            self.assertEqual(events[-1]["event_kind"], "formal_send")
            self.assertEqual(events[-1]["status_before"], "terminal")
            self.assertEqual(events[-1]["status_after"], "pending_entry")

    def test_write_cycle_audit_payload_writes_report_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_dir = Path(temp_dir) / "workspace"
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            self._save_decision(
                store,
                decision_id="formal-1",
                run_id="run-formal",
                event_id="formal-event",
                symbol="TSM",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-28T01:00:00+00:00",
            )

            payload = build_write_cycle_audit_payload(
                store,
                workspace_dir=workspace_dir,
                status="",
                limit=50,
            )

            report_path = Path(payload["report_path"])
            payload_path = Path(payload["payload_path"])
            self.assertTrue(report_path.exists())
            self.assertTrue(payload_path.exists())
            self.assertIn("周期链路审计", report_path.read_text(encoding="utf-8"))
            self.assertEqual(
                json.loads(payload_path.read_text(encoding="utf-8"))["items"][0]["symbol"],
                "TSM",
            )

    def test_active_cycle_payload_excludes_terminal_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            self._save_decision(
                store,
                decision_id="formal-active",
                run_id="run-active",
                event_id="event-active",
                symbol="NVDA",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-28T01:00:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="formal-terminal",
                run_id="run-terminal",
                event_id="event-terminal",
                symbol="META",
                pool="confirmation",
                action="试探建仓",
                created_at="2026-03-27T01:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="formal-terminal",
                entered=False,
                close_reason="not_entered",
                exit_subreason="price_invalidated",
                updated_at="2026-03-28T02:00:00+00:00",
            )

            active_payload = build_cycle_audit_payload(store, limit=10, refresh=True, active_only=True)
            self.assertEqual([item["symbol"] for item in active_payload["items"]], ["NVDA"])

            terminal_payload = build_cycle_audit_payload(
                store,
                limit=10,
                refresh=False,
                status="terminal",
                active_only=True,
            )
            self.assertEqual([item["symbol"] for item in terminal_payload["items"]], ["META"])

    def test_run_cycle_audit_payload_summarizes_replay_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.record_run(
                run_id="run-replay",
                started_at=datetime(2026, 3, 28, 1, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 3, 28, 1, 10, tzinfo=timezone.utc),
                status="success",
                summary={"events_processed": 2},
                run_name="replay-audit",
            )
            self._save_decision(
                store,
                decision_id="formal-1",
                run_id="run-replay",
                event_id="event-1",
                symbol="NVDA",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-28T01:01:00+00:00",
            )
            self._insert_alert_row(
                store,
                card_id="watch-card-run",
                symbol="NVDA",
                event_id="event-watch",
                notified_at="2026-03-28T01:02:00+00:00",
                card_payload={
                    "delivery_category": "watch",
                    "lifecycle_pool": "candidate",
                    "downgraded_from_formal": True,
                    "previous_formal_action": "确认做多",
                    "action_label": "加入观察",
                },
            )
            store.connection.execute(
                "UPDATE opportunity_cards SET run_id = ? WHERE card_id = ?",
                ("run-replay", "watch-card-run"),
            )
            store.connection.execute(
                "UPDATE alert_history SET run_id = ? WHERE card_id = ?",
                ("run-replay", "watch-card-run"),
            )
            store.connection.commit()
            self._insert_log_row(
                store,
                symbol="NVDA",
                event_id="event-suppress",
                created_at="2026-03-28T01:03:00+00:00",
                event_type="formal_suppressed_active_holding",
                context={"previous_formal_action": "确认做多", "action_label": "试探建仓"},
            )
            store.connection.execute(
                "UPDATE system_logs SET run_id = ? WHERE event_id = ?",
                ("run-replay", "event-suppress"),
            )
            store.connection.commit()

            payload = build_run_cycle_audit_payload(store, run_id="run-replay", limit=10)

            self.assertEqual(payload["run_id"], "run-replay")
            self.assertEqual(payload["symbol_count"], 1)
            self.assertEqual(payload["touched_symbols"], ["NVDA"])
            self.assertEqual(payload["summary"]["status_counts"]["pending_entry"], 1)
            self.assertEqual(payload["summary"]["event_kind_counts"]["formal_downgraded_to_watch_unentered"], 1)
            self.assertEqual(payload["summary"]["event_kind_counts"]["suppressed_active_holding"], 1)
            self.assertEqual(payload["summary"]["anomaly_count"], 0)

    def test_run_cycle_audit_payload_flags_duplicate_terminal_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.record_run(
                run_id="run-duplicate-terminal",
                started_at=datetime(2026, 3, 28, 2, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 3, 28, 2, 10, tzinfo=timezone.utc),
                status="success",
                summary={"events_processed": 1},
                run_name="duplicate-terminal",
            )
            self._save_decision(
                store,
                decision_id="formal-old",
                run_id="run-duplicate-terminal",
                event_id="event-old",
                symbol="AMD",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-26T01:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="formal-old",
                entered=True,
                entered_at="2026-03-26T02:00:00+00:00",
                entry_price=100.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-26T02:00:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="holding-exit-1",
                run_id="run-duplicate-terminal",
                event_id="exit-event-1",
                symbol="AMD",
                pool="holding_management",
                action="利润保护退出",
                created_at="2026-03-28T02:01:00+00:00",
                packet={
                    "normalized_close_reason": "profit_protection_exit",
                    "source_decision_id": "formal-old",
                },
            )
            self._save_decision(
                store,
                decision_id="holding-exit-2",
                run_id="run-duplicate-terminal",
                event_id="exit-event-2",
                symbol="AMD",
                pool="holding_management",
                action="利润保护退出",
                created_at="2026-03-28T02:02:00+00:00",
                packet={
                    "normalized_close_reason": "profit_protection_exit",
                    "source_decision_id": "formal-old",
                },
            )

            payload = build_run_cycle_audit_payload(store, run_id="run-duplicate-terminal", limit=10)

            self.assertEqual(payload["summary"]["anomaly_count"], 1)
            self.assertEqual(payload["summary"]["anomaly_type_counts"]["duplicate_terminal_events"], 1)
            self.assertEqual(payload["summary"]["anomaly_symbols"], ["AMD"])
            self.assertEqual(payload["summary"]["current_run_anomaly_count"], 1)
            self.assertEqual(payload["summary"]["historical_carryover_anomaly_count"], 0)
            self.assertEqual(payload["items"][0]["anomalies"][0]["type"], "duplicate_terminal_events")
            self.assertEqual(payload["items"][0]["anomalies"][0]["scope"], "current_run")
            report_text = format_run_cycle_audit_report(payload)
            self.assertIn("Run 周期异常审计", report_text)
            self.assertIn("异常类型：duplicate_terminal_events=1", report_text)

    def test_candidate_pool_only_events_do_not_create_fake_cycle_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.record_run(
                run_id="run-candidate-only",
                started_at=datetime(2026, 3, 28, 3, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 3, 28, 3, 5, tzinfo=timezone.utc),
                status="success",
                summary={"events_processed": 1},
                run_name="candidate-only",
            )
            for idx in range(3):
                self._save_decision(
                    store,
                    decision_id=f"candidate-only-{idx}",
                    run_id="run-candidate-only",
                    event_id=f"candidate-event-{idx}",
                    symbol="MRVL",
                    pool="candidate_pool",
                    action="加入观察",
                    created_at=f"2026-03-28T03:0{idx}:00+00:00",
                    packet={"horizon": "position"},
                )

            payload = build_run_cycle_audit_payload(store, run_id="run-candidate-only", limit=10)

            self.assertEqual(payload["symbol_count"], 1)
            self.assertEqual(payload["items"][0]["cycle_count"], 0)
            self.assertEqual(payload["summary"]["reopened_cycle_symbols"], [])

    def test_historical_carryover_anomaly_is_classified_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.record_run(
                run_id="run-current",
                started_at=datetime(2026, 3, 28, 4, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 3, 28, 4, 5, tzinfo=timezone.utc),
                status="success",
                summary={"events_processed": 1},
                run_name="historical-carryover",
            )
            self._save_decision(
                store,
                decision_id="old-formal-1",
                run_id="run-old-1",
                event_id="event-old-1",
                symbol="AAPL",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-27T01:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="old-formal-1",
                entered=True,
                entered_at="2026-03-27T01:10:00+00:00",
                entry_price=100.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-27T01:10:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="old-formal-2",
                run_id="run-old-2",
                event_id="event-old-2",
                symbol="AAPL",
                pool="confirmation",
                action="试探建仓",
                created_at="2026-03-27T02:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="old-formal-2",
                entered=True,
                entered_at="2026-03-27T02:05:00+00:00",
                entry_price=101.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-27T02:05:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="current-candidate",
                run_id="run-current",
                event_id="event-current",
                symbol="AAPL",
                pool="candidate_pool",
                action="加入观察",
                created_at="2026-03-28T04:01:00+00:00",
                packet={"horizon": "position"},
            )

            payload = build_run_cycle_audit_payload(store, run_id="run-current", limit=10)

            self.assertEqual(payload["summary"]["anomaly_count"], 1)
            self.assertEqual(payload["summary"]["current_run_anomaly_count"], 0)
            self.assertEqual(payload["summary"]["historical_carryover_anomaly_count"], 1)
            self.assertEqual(payload["items"][0]["anomalies"][0]["scope"], "historical_carryover")

    def test_historical_cycle_anomaly_cleanup_payload_dry_run_and_apply(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.record_run(
                run_id="run-current",
                started_at=datetime(2026, 3, 28, 4, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 3, 28, 4, 5, tzinfo=timezone.utc),
                status="success",
                summary={"events_processed": 1},
                run_name="cleanup-run",
            )
            self._save_decision(
                store,
                decision_id="old-formal-1",
                run_id="run-old-1",
                event_id="event-old-1",
                symbol="AAPL",
                pool="confirmation",
                action="确认做多",
                created_at="2026-03-27T01:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="old-formal-1",
                entered=True,
                entered_at="2026-03-27T01:10:00+00:00",
                entry_price=100.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-27T01:10:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="old-formal-2",
                run_id="run-old-2",
                event_id="event-old-2",
                symbol="AAPL",
                pool="confirmation",
                action="试探建仓",
                created_at="2026-03-27T02:00:00+00:00",
            )
            store.save_decision_outcome(
                decision_id="old-formal-2",
                entered=True,
                entered_at="2026-03-27T02:05:00+00:00",
                entry_price=101.0,
                close_reason="insufficient_lookahead",
                updated_at="2026-03-27T02:05:00+00:00",
            )
            self._save_decision(
                store,
                decision_id="current-candidate",
                run_id="run-current",
                event_id="event-current",
                symbol="AAPL",
                pool="candidate_pool",
                action="加入观察",
                created_at="2026-03-28T04:01:00+00:00",
                packet={"horizon": "position"},
            )

            dry_run_payload = build_historical_cycle_anomaly_cleanup_payload(store, run_id="run-current", apply=False)

            self.assertEqual(dry_run_payload["mode"], "dry_run")
            self.assertEqual(dry_run_payload["matched_decisions"], 1)
            self.assertEqual(dry_run_payload["affected_symbols"], ["AAPL"])
            self.assertEqual(dry_run_payload["sample_rows"][0]["decision_id"], "old-formal-2")
            self.assertIn("当前仅预演统计", format_historical_cycle_anomaly_cleanup(dry_run_payload))

            apply_payload = build_historical_cycle_anomaly_cleanup_payload(store, run_id="run-current", apply=True)

            self.assertEqual(apply_payload["mode"], "apply")
            self.assertEqual(apply_payload["deleted_records"], 1)
            self.assertEqual(apply_payload["deleted_outcomes"], 1)
            remaining = {row["decision_id"] for row in store.load_decision_records_for_symbols(["AAPL"])}
            self.assertNotIn("old-formal-2", remaining)


if __name__ == "__main__":
    unittest.main()
