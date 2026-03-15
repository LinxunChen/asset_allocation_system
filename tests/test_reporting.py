from __future__ import annotations

import json
import os
import io
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.reporting import (
    format_batch_index,
    format_batch_comparison,
    format_batch_replay,
    format_error_summary,
    format_replay_evaluation,
    format_run_comparison,
    format_run_detail,
    format_runs,
    format_source_health,
    format_strategy_report,
    serialize_batch_comparison,
    serialize_batch_index,
    serialize_batch_replay,
    serialize_error_summary,
    serialize_replay_evaluation,
    serialize_run_detail,
    serialize_runs,
    serialize_source_health,
    serialize_strategy_report,
)
from satellite_agent.main import main
from satellite_agent.main import build_batch_replay_payload
from satellite_agent.main import build_demo_flow_payload
from satellite_agent.main import build_replay_evaluation_payload
from satellite_agent.main import build_run_comparison_payload
from satellite_agent.config import Settings
from satellite_agent.models import SourceHealthCheck
from satellite_agent.runtime_config import AgentRuntimeConfig
from satellite_agent.store import Store


class ReportingTests(unittest.TestCase):
    def test_format_helpers_render_store_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            started = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
            finished = datetime(2026, 3, 14, 14, 1, tzinfo=timezone.utc)
            store.record_run(
                run_id="run-1",
                started_at=started,
                finished_at=finished,
                status="success",
                run_name="baseline",
                note="first replay",
                config_snapshot={
                    "settings": {
                        "dry_run": True,
                        "event_score_threshold": 60.0,
                        "cross_source_dedup_hours": 12,
                        "horizons": {
                            "swing": {"market_score_threshold": 55.0, "priority_threshold": 75.0},
                            "position": {"market_score_threshold": 58.0, "priority_threshold": 80.0},
                        },
                    },
                    "runtime_config": {"sources": {"google_news": True}},
                },
                summary={
                    "events_processed": 2,
                    "cards_generated": 4,
                    "alerts_sent": 1,
                    "extraction_failures": 0,
                    "market_data_failures": 1,
                    "scoring_failures": 0,
                    "notification_failures": 0,
                },
            )
            store.record_log(
                run_id="run-1",
                level="ERROR",
                event_type="market_data_failed",
                stage="market_data",
                message="Market data snapshot failed.",
                symbol="NVDA",
                event_id="evt-1",
                context={"error": "TimeoutError"},
            )
            store.record_source_health(
                "run-1",
                SourceHealthCheck(
                    source_name="sec_edgar",
                    status="healthy",
                    detail="SEC feed reachable.",
                    latency_ms=123,
                    checked_at=finished,
                ),
            )
            store.connection.execute(
                """
                INSERT INTO event_insights
                (event_id, run_id, symbol, event_type, headline_summary, bull_case, bear_case, importance, source_credibility,
                 novelty, sentiment, theme_relevance, llm_confidence, risk_notes_json, source_refs_json, raw_payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt-1",
                    "run-1",
                    "NVDA",
                    "earnings",
                    "AI demand improved.",
                    "Bull",
                    "Bear",
                    80.0,
                    80.0,
                    80.0,
                    0.8,
                    80.0,
                    80.0,
                    "[]",
                    "[]",
                    "{}",
                    finished.isoformat(),
                ),
            )
            store.connection.execute(
                """
                INSERT INTO opportunity_cards
                (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "card-1",
                    "run-1",
                    "evt-1",
                    "NVDA",
                    "swing",
                    88.5,
                    "high",
                    98.0,
                    json.dumps(
                        {
                            "card_id": "card-1",
                            "symbol": "NVDA",
                            "horizon": "swing",
                            "event_type": "earnings",
                            "priority": "high",
                            "event_score": 78.35,
                            "market_score": 58.9,
                            "final_score": 88.5,
                        }
                    ),
                    finished.isoformat(),
                ),
            )
            store.connection.execute(
                """
                INSERT INTO alert_history
                (run_id, dedup_key, card_id, symbol, event_id, horizon, priority, final_score, invalidation_level, sent, reason, notified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "run-1",
                    "dedup-1",
                    "card-1",
                    "NVDA",
                    "evt-1",
                    "swing",
                    "high",
                    88.5,
                    98.0,
                    1,
                    "sent",
                    finished.isoformat(),
                ),
            )
            store.connection.commit()
            run_text = format_runs(store.load_recent_runs())
            detail_text = format_run_detail(store.load_latest_run(), store.load_logs("run-1"))
            error_text = format_error_summary(store.aggregate_errors())
            source_text = format_source_health(store.load_latest_source_health())
            runs_json = serialize_runs(store.load_recent_runs())
            detail_json = serialize_run_detail(store.load_latest_run(), store.load_logs("run-1"))
            errors_json = serialize_error_summary(store.aggregate_errors())
            sources_json = serialize_source_health(store.load_latest_source_health())
            since = finished.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            strategy_rows = store.aggregate_event_type_performance(since)
            source_rows = store.aggregate_source_stability(since)
            alert_rows = store.aggregate_alert_volume(since)
            strategy_text = format_strategy_report(strategy_rows, source_rows, alert_rows)
            strategy_json = serialize_strategy_report(strategy_rows, source_rows, alert_rows)
            replay_payload = build_replay_evaluation_payload(store, run_id="run-1", days=14, limit=10)
            replay_text = format_replay_evaluation(
                replay_payload["run"],
                replay_payload["strategy_report"],
                replay_payload["source_health"],
                replay_payload["card_diagnostics"],
            )
            replay_json = serialize_replay_evaluation(
                replay_payload["run"],
                replay_payload["strategy_report"],
                replay_payload["source_health"],
                replay_payload["card_diagnostics"],
            )
            store.record_run(
                run_id="run-2",
                started_at=started,
                finished_at=finished,
                status="success",
                summary={
                    "events_processed": 1,
                    "cards_generated": 1,
                    "alerts_sent": 0,
                    "extraction_failures": 0,
                    "market_data_failures": 0,
                    "scoring_failures": 0,
                    "notification_failures": 0,
                },
            )
            comparison_payload = build_run_comparison_payload(store, run_ids=["run-1", "run-2"], limit=10)
            comparison_text = format_run_comparison(comparison_payload["runs"])
            self.assertIn("run-1", run_text)
            self.assertIn("cards", run_text)
            self.assertIn("2026-03-14 22:01", run_text)
            self.assertIn("market_data_failed", detail_text)
            self.assertIn("TimeoutError", detail_text)
            self.assertIn("Started: 2026-03-14 22:00", detail_text)
            self.assertIn("Finished: 2026-03-14 22:01", detail_text)
            self.assertIn("count", error_text)
            self.assertIn("NVDA", error_text)
            self.assertIn("sec_edgar", source_text)
            self.assertIn("2026-03-14 22:01", source_text)
            self.assertEqual(runs_json[0]["run_id"], "run-1")
            self.assertEqual(runs_json[0]["run_name"], "baseline")
            self.assertEqual(runs_json[0]["note"], "first replay")
            self.assertTrue(runs_json[0]["config_snapshot"]["settings"]["dry_run"])
            self.assertEqual(detail_json["run_id"], "run-1")
            self.assertEqual(detail_json["run_name"], "baseline")
            self.assertEqual(detail_json["note"], "first replay")
            self.assertTrue(detail_json["config_snapshot"]["settings"]["dry_run"])
            self.assertEqual(errors_json[0]["event_type"], "market_data_failed")
            self.assertEqual(sources_json[0]["source_name"], "sec_edgar")
            self.assertIn("Event Type Performance", strategy_text)
            self.assertEqual(strategy_json["event_type_performance"][0]["event_type"], "earnings")
            self.assertEqual(strategy_json["alert_volume"][0]["high_priority_alerts"], 1)
            self.assertIn("Replay Evaluation", replay_text)
            self.assertIn("Card Diagnostics", replay_text)
            self.assertIn("baseline", replay_text)
            self.assertIn("Started: 2026-03-14 22:00", replay_text)
            self.assertEqual(replay_json["run"]["run_id"], "run-1")
            self.assertEqual(replay_json["run"]["run_name"], "baseline")
            self.assertEqual(replay_json["card_diagnostics"][0]["horizon"], "swing")
            self.assertIn("Run Comparison", comparison_text)
            self.assertIn("name=baseline", comparison_text)
            self.assertIn("cfg=E60.0/S55.0-75.0/P58.0-80.0/D12", comparison_text)
            self.assertEqual(comparison_payload["runs"][0]["run_id"], "run-1")

    def test_annotate_run_cli_updates_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            started = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
            finished = datetime(2026, 3, 14, 14, 1, tzinfo=timezone.utc)
            store.record_run(
                run_id="run-annotate",
                started_at=started,
                finished_at=finished,
                status="success",
                summary={"events_processed": 0, "cards_generated": 0, "alerts_sent": 0},
            )
            store.close()

            argv = [
                "satellite-agent",
                "annotate-run",
                "--run-id",
                "run-annotate",
                "--name",
                "experiment-a",
                "--note",
                "tighter thresholds",
            ]
            with patch.dict(os.environ, {"SATELLITE_DB_PATH": str(db_path)}, clear=False):
                with patch.object(sys, "argv", argv):
                    main()

            verify_store = Store(db_path)
            verify_store.initialize()
            row = verify_store.load_run("run-annotate")
            self.assertIsNotNone(row)
            self.assertEqual(row["run_name"], "experiment-a")
            self.assertEqual(row["note"], "tighter thresholds")
            verify_store.close()

    def test_batch_replay_payload_and_cli(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            spec_path = Path(temp_dir) / "batch.json"
            output_dir = Path(temp_dir) / "batch_dbs"
            spec_path.write_text(
                json.dumps(
                    {
                        "replay_path": "tests/fixtures/events.jsonl",
                        "days": 14,
                        "limit": 10,
                        "experiments": [
                            {
                                "name": "baseline",
                                "note": "control",
                                "overrides": {"event_score_threshold": 60.0},
                            },
                            {
                                "name": "tuned",
                                "note": "raise thresholds",
                                "overrides": {
                                    "event_score_threshold": 66.0,
                                    "swing_market_score_threshold": 58.0,
                                    "position_priority_threshold": 82.0,
                                },
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            def fake_execute(settings, runtime_config, *, replay_path, days, limit, run_name, note):
                event_threshold = settings.event_score_threshold
                swing_threshold = settings.horizons["swing"].market_score_threshold
                swing_priority = settings.horizons["swing"].priority_threshold
                return {
                    "run": {
                        "run_id": f"run-{run_name}",
                        "status": "success",
                        "run_name": run_name,
                        "note": note,
                        "started_at": "2026-03-14T14:00:00+00:00",
                        "finished_at": "2026-03-14T14:01:00+00:00",
                        "config_snapshot": {"settings": settings.to_record()},
                        "summary": {
                            "events_processed": 2 if run_name == "baseline" else 1,
                            "cards_generated": 3 if run_name == "baseline" else 1,
                            "alerts_sent": 1 if run_name == "baseline" else 0,
                            "extraction_failures": 0,
                            "market_data_failures": 0,
                            "scoring_failures": 0,
                            "notification_failures": 0,
                        },
                        "logs": [],
                    },
                    "strategy_report": {
                        "event_type_performance": [
                            {
                                "event_type": "earnings" if run_name == "baseline" else "guidance",
                                "card_count": 1,
                                "avg_final_score": 88.5 if run_name == "baseline" else 75.0,
                                "high_priority_count": 1 if run_name == "baseline" else 0,
                            }
                        ],
                        "source_stability": [],
                        "alert_volume": [],
                    },
                    "source_health": [],
                    "card_diagnostics": [
                        {
                            "card_id": f"card-{run_name}",
                            "symbol": "NVDA",
                            "horizon": "swing",
                            "event_type": "earnings",
                            "priority": "normal",
                            "event_score": 78.5,
                            "market_score": 58.5,
                            "final_score": 70.5,
                            "event_threshold": event_threshold,
                            "market_threshold": swing_threshold,
                            "priority_threshold": swing_priority,
                            "event_margin": round(78.5 - event_threshold, 2),
                            "market_margin": round(58.5 - swing_threshold, 2),
                            "priority_margin": round(70.5 - swing_priority, 2),
                            "event_pass": True,
                            "market_pass": True,
                            "priority_pass": False,
                        }
                    ],
                }

            with patch("satellite_agent.main._execute_replay_evaluation", side_effect=fake_execute):
                payload = build_batch_replay_payload(
                    Settings(),
                    AgentRuntimeConfig(),
                    spec_path=spec_path,
                    output_dir=output_dir,
                )
                text = format_batch_replay(payload)
                data = serialize_batch_replay(payload["experiments"])

            self.assertEqual(len(payload["experiments"]), 2)
            self.assertEqual(payload["experiments"][0]["name"], "baseline")
            self.assertIn("Batch Replay", text)
            self.assertIn("baseline", text)
            self.assertIn("event_margin", text)
            self.assertIn("Recommendation:", text)
            self.assertIn("Summary:", text)
            self.assertIn("Winner Snapshot:", text)
            self.assertIn("Next Step:", text)
            self.assertIn("Report:", text)
            self.assertEqual(data["ranking"][0]["name"], "baseline")
            self.assertEqual(data["recommendation"]["name"], "baseline")
            self.assertIn("Top output:", data["summary"]["line_items"][0])
            self.assertEqual(data["next_step"]["title"], "Advance Recommended Setup")
            self.assertTrue(payload["experiments"][0]["db_path"].endswith("baseline.db"))
            self.assertEqual(payload["experiments"][0]["card_diagnostics"][0]["event_margin"], 18.5)

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "batch-replay",
                "--spec-path",
                str(spec_path),
                "--output-dir",
                str(output_dir),
                "--json",
            ]
            with patch("satellite_agent.main._execute_replay_evaluation", side_effect=fake_execute):
                with patch.dict(
                    os.environ,
                    {"SATELLITE_DB_PATH": str(Path(temp_dir) / "cli.db")},
                    clear=False,
                ):
                    with patch.object(sys, "argv", argv):
                        with patch("sys.stdout", stdout):
                            main()
            cli_payload = json.loads(stdout.getvalue())
            self.assertEqual(cli_payload["ranking"][0]["name"], "baseline")
            manifest_path = Path(cli_payload["manifest_path"])
            self.assertTrue(manifest_path.exists())
            report_path = Path(cli_payload["report_path"])
            self.assertTrue(report_path.exists())
            self.assertIn("Recommendation:", report_path.read_text(encoding="utf-8"))

            report_stdout = io.StringIO()
            report_argv = [
                "satellite-agent",
                "report-batch",
                "--manifest-path",
                str(manifest_path),
            ]
            with patch.object(sys, "argv", report_argv):
                with patch("sys.stdout", report_stdout):
                    main()
            self.assertIn("Manifest:", report_stdout.getvalue())
            self.assertIn("baseline", report_stdout.getvalue())
            self.assertIn("Recommendation:", report_stdout.getvalue())
            self.assertIn("Summary:", report_stdout.getvalue())
            self.assertIn("Winner Snapshot:", report_stdout.getvalue())
            self.assertIn("Next Step:", report_stdout.getvalue())
            self.assertIn("Report:", report_stdout.getvalue())

    def test_compare_batches_uses_manifest_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            left_manifest = Path(temp_dir) / "left_manifest.json"
            right_manifest = Path(temp_dir) / "right_manifest.json"
            compare_report = Path(temp_dir) / "compare.md"
            left_payload = {
                "batch_id": "left-batch",
                "manifest_path": str(left_manifest),
                "recommendation": {
                    "name": "baseline",
                    "config_summary": "E60.0/S55.0-75.0/P55.0-75.0/D12",
                },
                "experiments": [
                    {
                        "name": "baseline",
                        "status": "success",
                        "summary": {"alerts_sent": 1, "cards_generated": 2, "events_processed": 1},
                        "config_summary": "E60.0/S55.0-75.0/P55.0-75.0/D12",
                        "closest_market_margin": 3.5,
                        "closest_priority_margin": -4.5,
                    }
                ],
            }
            right_payload = {
                "batch_id": "right-batch",
                "manifest_path": str(right_manifest),
                "recommendation": {
                    "name": "baseline",
                    "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12",
                },
                "experiments": [
                    {
                        "name": "baseline",
                        "status": "success",
                        "summary": {"alerts_sent": 2, "cards_generated": 3, "events_processed": 1},
                        "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12",
                        "closest_market_margin": 0.9,
                        "closest_priority_margin": -7.4,
                    }
                ],
            }
            left_manifest.write_text(json.dumps(left_payload), encoding="utf-8")
            right_manifest.write_text(json.dumps(right_payload), encoding="utf-8")

            payload = serialize_batch_comparison(left_payload, right_payload)
            text = format_batch_comparison(payload)
            self.assertEqual(payload["comparisons"][0]["alerts_delta"], 1)
            self.assertEqual(payload["comparisons"][0]["cards_delta"], 1)
            self.assertEqual(payload["comparisons"][0]["market_margin_delta"], -2.6)
            self.assertIn("Batch Comparison", text)
            self.assertIn("Left Recommendation: baseline", text)
            self.assertIn("Summary:", text)
            self.assertIn("整体产出变化：提醒 +1，卡片 +1，事件 +0。", text)
            self.assertIn("alerts 1 -> 2 (+1)", text)
            self.assertIn("Next Step:", text)
            self.assertEqual(payload["next_step"]["title"], "Review Batch Drift")
            self.assertIn("两批实验的推荐参数保持一致", payload["summary"]["line_items"][0])

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "compare-batches",
                "--left-manifest-path",
                str(left_manifest),
                "--right-manifest-path",
                str(right_manifest),
                "--markdown-path",
                str(compare_report),
            ]
            with patch.object(sys, "argv", argv):
                with patch("sys.stdout", stdout):
                    main()
            self.assertIn("Left Batch: left-batch", stdout.getvalue())
            self.assertIn("Summary:", stdout.getvalue())
            self.assertIn("alerts 1 -> 2 (+1)", stdout.getvalue())
            self.assertIn("Report:", stdout.getvalue())
            self.assertIn("Next Step:", stdout.getvalue())
            self.assertTrue(compare_report.exists())
            self.assertIn("Batch Comparison", compare_report.read_text(encoding="utf-8"))

    def test_list_batches_indexes_manifest_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_dir = Path(temp_dir) / "batch_runs"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            first_manifest = manifest_dir / "20260314T100000Z_manifest.json"
            second_manifest = manifest_dir / "20260315T100000Z_manifest.json"
            first_manifest.write_text(
                json.dumps(
                    {
                        "batch_id": "20260314T100000Z",
                        "generated_at": "2026-03-14T10:00:00+00:00",
                        "manifest_path": str(first_manifest),
                        "report_path": str((manifest_dir / "20260314T100000Z_report.md").resolve()),
                        "experiments": [{"name": "baseline"}],
                    }
                ),
                encoding="utf-8",
            )
            second_manifest.write_text(
                json.dumps(
                    {
                        "batch_id": "20260315T100000Z",
                        "generated_at": "2026-03-15T10:00:00+00:00",
                        "manifest_path": str(second_manifest),
                        "report_path": str((manifest_dir / "20260315T100000Z_report.md").resolve()),
                        "experiments": [
                            {
                                "name": "baseline",
                                "summary": {"alerts_sent": 1, "cards_generated": 2, "events_processed": 1},
                                "top_event": "earnings",
                                "closest_market_margin": 3.5,
                                "closest_priority_margin": -4.5,
                            },
                            {
                                "name": "tuned",
                                "summary": {"alerts_sent": 2, "cards_generated": 2, "events_processed": 1},
                                "top_event": "guidance",
                                "closest_market_margin": 0.9,
                                "closest_priority_margin": -7.4,
                            },
                        ],
                        "recommendation": {
                            "name": "tuned",
                            "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12",
                            "alerts_sent": 2,
                            "cards_generated": 2,
                            "events_processed": 1,
                            "closest_market_margin": 0.9,
                            "closest_priority_margin": -7.4,
                        },
                        "next_step": {"title": "Expand Replay Coverage"},
                    }
                ),
                encoding="utf-8",
            )

            payloads = [json.loads(first_manifest.read_text()), json.loads(second_manifest.read_text())]
            payload = serialize_batch_index(payloads)
            text = format_batch_index(payload)

            self.assertEqual(payload["batches"][0]["batch_id"], "20260314T100000Z")
            self.assertEqual(payload["summary"]["latest_recommendation"], "")
            self.assertIn("Batch Index", text)
            self.assertIn("Summary:", text)
            self.assertIn("Only 1 of 2 listed batches contains a recorded recommendation", text)
            self.assertIn("winner_snapshot=unavailable", text)
            self.assertIn("winner=tuned alerts=2 cards=2", text)

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "list-batches",
                "--dir",
                str(manifest_dir),
                "--limit",
                "5",
            ]
            with patch.object(sys, "argv", argv):
                with patch("sys.stdout", stdout):
                    main()
            output = stdout.getvalue()
            self.assertIn("20260315T100000Z", output)
            self.assertIn("recommended=tuned", output)
            self.assertIn("report=", output)
            self.assertIn("Current operational focus", output)
            self.assertIn("winner=tuned alerts=2 cards=2", output)

    def test_batch_recommendation_prefers_stricter_equal_output(self) -> None:
        payload = serialize_batch_replay(
            [
                {
                    "name": "baseline",
                    "status": "success",
                    "summary": {"alerts_sent": 2, "cards_generated": 2, "events_processed": 1},
                    "config_summary": "E60.0/S55.0-75.0/P55.0-75.0/D12",
                    "closest_market_margin": 3.9,
                    "closest_priority_margin": -4.4,
                    "failures": 0,
                },
                {
                    "name": "tuned",
                    "status": "success",
                    "summary": {"alerts_sent": 2, "cards_generated": 2, "events_processed": 1},
                    "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12",
                    "closest_market_margin": 0.9,
                    "closest_priority_margin": -7.4,
                    "failures": 0,
                },
            ]
        )
        self.assertEqual(payload["recommendation"]["name"], "tuned")
        self.assertFalse(payload["summary"]["has_meaningful_difference"])
        self.assertEqual(payload["next_step"]["title"], "Expand Replay Coverage")

    def test_batch_recommendation_weights_can_shift_preference(self) -> None:
        items = [
            {
                "name": "busy",
                "status": "success",
                "summary": {"alerts_sent": 3, "cards_generated": 3, "events_processed": 1},
                "config_summary": "E60.0/S55.0-75.0/P55.0-75.0/D12",
                "closest_market_margin": 4.0,
                "closest_priority_margin": -2.0,
                "failures": 0,
            },
            {
                "name": "strict",
                "status": "success",
                "summary": {"alerts_sent": 2, "cards_generated": 2, "events_processed": 1},
                "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12",
                "closest_market_margin": 0.2,
                "closest_priority_margin": -6.0,
                "failures": 0,
            },
        ]
        default_payload = serialize_batch_replay(items)
        strict_payload = serialize_batch_replay(
            items,
            recommendation_preferences={
                "alerts_sent": 1.0,
                "cards_generated": 0.5,
                "events_processed": 0.5,
                "strictness": 8.0,
                "priority_proximity": 0.0,
                "failures": 3.0,
            },
        )
        self.assertEqual(default_payload["recommendation"]["name"], "busy")
        self.assertEqual(strict_payload["recommendation"]["name"], "strict")
        self.assertEqual(strict_payload["recommendation_preferences"]["strictness"], 8.0)
        self.assertTrue(default_payload["summary"]["has_meaningful_difference"])

    def test_demo_flow_writes_linked_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            workspace_dir = temp_path / "demo_flow"
            spec_path = temp_path / "batch.json"
            replay_path = temp_path / "events.jsonl"
            replay_path.write_text("", encoding="utf-8")
            spec_path.write_text(
                json.dumps(
                    {
                        "replay_path": "placeholder.jsonl",
                        "days": 14,
                        "limit": 10,
                        "experiments": [
                            {"name": "baseline", "overrides": {"event_score_threshold": 60.0}},
                            {"name": "tuned", "overrides": {"event_score_threshold": 65.0}},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            def fake_execute(settings, runtime_config, *, replay_path, days, limit, run_name, note):
                return {
                    "run": {
                        "run_id": f"run-{run_name}",
                        "status": "success",
                        "run_name": run_name,
                        "note": note,
                        "started_at": "2026-03-15T09:00:00+00:00",
                        "finished_at": "2026-03-15T09:01:00+00:00",
                        "config_snapshot": {"settings": settings.to_record()},
                        "summary": {
                            "events_processed": 1,
                            "cards_generated": 2,
                            "alerts_sent": 2 if run_name == "baseline" else 1,
                            "extraction_failures": 0,
                            "market_data_failures": 0,
                            "scoring_failures": 0,
                            "notification_failures": 0,
                        },
                        "logs": [],
                    },
                    "strategy_report": {
                        "event_type_performance": [
                            {
                                "event_type": "earnings",
                                "card_count": 2,
                                "avg_final_score": 72.0,
                                "high_priority_count": 0,
                            }
                        ],
                        "source_stability": [],
                        "alert_volume": [],
                    },
                    "source_health": [],
                    "card_diagnostics": [
                        {
                            "card_id": f"card-{run_name}",
                            "symbol": "NVDA",
                            "horizon": "swing",
                            "event_type": "earnings",
                            "priority": "normal",
                            "event_score": 78.5,
                            "market_score": 58.5,
                            "final_score": 70.5,
                            "event_threshold": settings.event_score_threshold,
                            "market_threshold": settings.horizons["swing"].market_score_threshold,
                            "priority_threshold": settings.horizons["swing"].priority_threshold,
                            "event_margin": round(78.5 - settings.event_score_threshold, 2),
                            "market_margin": round(58.5 - settings.horizons["swing"].market_score_threshold, 2),
                            "priority_margin": round(70.5 - settings.horizons["swing"].priority_threshold, 2),
                            "event_pass": True,
                            "market_pass": True,
                            "priority_pass": False,
                        }
                    ],
                }

            with patch("satellite_agent.main._execute_replay_evaluation", side_effect=fake_execute):
                payload = build_demo_flow_payload(
                    Settings(),
                    AgentRuntimeConfig(),
                    workspace_dir=workspace_dir,
                    replay_path=str(replay_path),
                    batch_spec_path=spec_path,
                    days=14,
                    limit=10,
                )

            self.assertEqual(payload["batch"]["recommendation_name"], "baseline")
            self.assertTrue(Path(payload["replay"]["report_path"]).exists())
            self.assertTrue(Path(payload["replay"]["payload_path"]).exists())
            self.assertTrue(Path(payload["batch"]["manifest_path"]).exists())
            self.assertTrue(Path(payload["batch"]["report_path"]).exists())
            self.assertTrue(Path(payload["batch"]["index_path"]).exists())
            self.assertTrue(Path(payload["batch"]["promoted_config_path"]).exists())
            copied_spec = json.loads(Path(payload["batch"]["spec_path"]).read_text(encoding="utf-8"))
            self.assertEqual(copied_spec["replay_path"], str(replay_path.resolve()))
            promoted = json.loads(Path(payload["batch"]["promoted_config_path"]).read_text(encoding="utf-8"))
            self.assertEqual(promoted["strategy"]["event_score_threshold"], 60.0)

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "demo-flow",
                "--workspace-dir",
                str(workspace_dir),
                "--replay-path",
                str(replay_path),
                "--batch-spec-path",
                str(spec_path),
            ]
            with patch("satellite_agent.main._execute_replay_evaluation", side_effect=fake_execute):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()
            output = stdout.getvalue()
            self.assertIn("初版流程联调", output)
            self.assertIn("Batch 推荐：baseline", output)
            self.assertIn("推荐配置：", output)

    def test_daily_run_writes_chinese_review(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            workspace_dir = temp_path / "daily_run"
            config_path = temp_path / "agent.recommended.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {"stocks": ["NVDA"], "etfs": ["QQQ"]},
                        "sources": {"sec_filings": False, "google_news": True, "google_research": False},
                        "runtime": {"poll_seconds": 60, "cross_source_dedup_hours": 12},
                        "strategy": {
                            "event_score_threshold": 65.0,
                            "horizons": {
                                "swing": {"market_score_threshold": 58.0, "priority_threshold": 78.0},
                                "position": {"market_score_threshold": 60.0, "priority_threshold": 80.0},
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            def fake_build_service(settings, replay_path="", run_name="", note="", runtime_config=None, apply_runtime_config=True):
                store = Store(settings.database_path)
                store.initialize()

                class FakeService:
                    def __init__(self, store, settings, runtime_config):
                        self.store = store
                        self.settings = settings
                        self.runtime_config = runtime_config

                    def run_once(self):
                        started = datetime(2026, 3, 15, 9, 0, tzinfo=timezone.utc)
                        finished = datetime(2026, 3, 15, 9, 1, tzinfo=timezone.utc)
                        self.store.record_run(
                            run_id="daily-run-1",
                            started_at=started,
                            finished_at=finished,
                            status="success",
                            run_name="daily_run",
                            note="日常运行",
                            config_snapshot={
                                "settings": self.settings.to_record(),
                                "runtime_config": self.runtime_config.to_record() if self.runtime_config else {},
                            },
                            summary={
                                "events_processed": 1,
                                "cards_generated": 2,
                                "alerts_sent": 1,
                                "extraction_failures": 0,
                                "market_data_failures": 0,
                                "scoring_failures": 0,
                                "notification_failures": 0,
                            },
                        )
                        self.store.record_log(
                            run_id="daily-run-1",
                            level="INFO",
                            event_type="event_accepted",
                            stage="store",
                            message="Daily run accepted one event.",
                            symbol="NVDA",
                            event_id="evt-daily",
                        )
                        self.store.record_source_health(
                            "daily-run-1",
                            SourceHealthCheck(
                                source_name="google_news",
                                status="healthy",
                                detail="Daily replay source healthy.",
                            ),
                        )
                        self.store.connection.execute(
                            """
                            INSERT INTO event_insights
                            (event_id, run_id, symbol, event_type, headline_summary, bull_case, bear_case, importance, source_credibility,
                             novelty, sentiment, theme_relevance, llm_confidence, risk_notes_json, source_refs_json, raw_payload_json, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "evt-daily",
                                "daily-run-1",
                                "NVDA",
                                "earnings",
                                "AI demand improved.",
                                "Bull",
                                "Bear",
                                80.0,
                                80.0,
                                80.0,
                                0.8,
                                80.0,
                                80.0,
                                "[]",
                                "[]",
                                "{}",
                                finished.isoformat(),
                            ),
                        )
                        self.store.connection.execute(
                            """
                            INSERT INTO opportunity_cards
                            (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "card-daily",
                                "daily-run-1",
                                "evt-daily",
                                "NVDA",
                                "swing",
                                72.5,
                                "normal",
                                98.0,
                                json.dumps(
                                    {
                                        "card_id": "card-daily",
                                        "symbol": "NVDA",
                                        "horizon": "swing",
                                        "event_type": "earnings",
                                        "headline_summary": "AI demand improved.",
                                        "reason_to_watch": "Earnings and demand both improved.",
                                        "source_refs": ["https://example.com/nvda-earnings"],
                                        "priority": "normal",
                                        "event_score": 78.35,
                                        "market_score": 58.9,
                                        "final_score": 72.5,
                                        "entry_range": {"low": 100.0, "high": 101.0},
                                        "take_profit_range": {"low": 105.0, "high": 108.0},
                                        "invalidation_level": 98.0,
                                        "invalidation_reason": "Breakdown through support.",
                                    }
                                ),
                                finished.isoformat(),
                            ),
                        )
                        self.store.connection.execute(
                            """
                            INSERT INTO alert_history
                            (run_id, dedup_key, card_id, symbol, event_id, horizon, priority, final_score, invalidation_level, sent, reason, notified_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "daily-run-1",
                                "dedup-daily",
                                "card-daily",
                                "NVDA",
                                "evt-daily",
                                "swing",
                                "normal",
                                72.5,
                                98.0,
                                1,
                                "dry_run",
                                finished.isoformat(),
                            ),
                        )
                        self.store.connection.commit()
                        return {"events_processed": 1, "cards_generated": 2, "alerts_sent": 1}

                return FakeService(store, settings, runtime_config)

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "daily-run",
                "--workspace-dir",
                str(workspace_dir),
                "--config-path",
                str(config_path),
            ]
            with patch("satellite_agent.main.build_service", side_effect=fake_build_service):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            review_path = workspace_dir / "daily_run_review.md"
            payload_path = workspace_dir / "daily_run_payload.json"
            self.assertIn("日常运行", output)
            self.assertIn("配置文件：", output)
            self.assertIn("健康判断：正常", output)
            self.assertTrue(review_path.exists())
            self.assertTrue(payload_path.exists())
            review_text = review_path.read_text(encoding="utf-8")
            self.assertIn("运行复盘", review_text)
            self.assertIn("健康判断为 正常", review_text)
            self.assertIn("结论摘要", review_text)
            self.assertIn("运行健康：", review_text)
            self.assertIn("机会概览", review_text)
            self.assertIn("卡片解读", review_text)
            self.assertIn("原文链接 1：https://example.com/nvda-earnings", review_text)
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["health_summary"]["status"], "正常")

    def test_promote_batch_exports_recommended_strategy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "batch_manifest.json"
            output_config_path = temp_path / "agent.recommended.json"
            base_config_path = temp_path / "agent.base.json"
            base_config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {"stocks": ["NVDA"], "etfs": ["QQQ"]},
                        "sources": {"sec_filings": False, "google_news": True, "google_research": False},
                        "runtime": {"poll_seconds": 60, "cross_source_dedup_hours": 12},
                        "strategy": {"event_score_threshold": 60.0, "horizons": {"swing": {}, "position": {}}},
                    }
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "batch_id": "batch-001",
                        "manifest_path": str(manifest_path),
                        "recommendation": {"name": "tuned", "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12"},
                        "experiments": [
                            {
                                "name": "tuned",
                                "evaluation": {
                                    "run": {
                                        "config_snapshot": {
                                            "settings": {
                                                "event_score_threshold": 65.0,
                                                "horizons": {
                                                    "swing": {
                                                        "ttl_days": 3,
                                                        "market_score_threshold": 58.0,
                                                        "priority_threshold": 78.0,
                                                        "rsi_floor": 45.0,
                                                        "rsi_ceiling": 68.0,
                                                        "atr_percent_ceiling": 8.0,
                                                    },
                                                    "position": {
                                                        "ttl_days": 20,
                                                        "market_score_threshold": 60.0,
                                                        "priority_threshold": 80.0,
                                                        "rsi_floor": 50.0,
                                                        "rsi_ceiling": 65.0,
                                                        "atr_percent_ceiling": 10.0,
                                                    },
                                                },
                                            }
                                        }
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "promote-batch",
                "--manifest-path",
                str(manifest_path),
                "--output-config-path",
                str(output_config_path),
                "--base-config-path",
                str(base_config_path),
            ]
            with patch.object(sys, "argv", argv):
                with patch("sys.stdout", stdout):
                    main()

            output = stdout.getvalue()
            promoted = json.loads(output_config_path.read_text(encoding="utf-8"))
            self.assertIn("推荐参数已导出", output)
            self.assertIn("策略变化", output)
            self.assertIn("全局事件阈值：60.0 -> 65.0", output)
            self.assertEqual(promoted["strategy"]["event_score_threshold"], 65.0)
            self.assertEqual(promoted["strategy"]["horizons"]["swing"]["market_score_threshold"], 58.0)
            self.assertEqual(promoted["watchlist"]["stocks"], ["NVDA"])

    def test_promote_batch_force_creates_backup_and_compares_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "batch_manifest.json"
            output_config_path = temp_path / "agent.recommended.json"
            base_config_path = temp_path / "agent.base.json"
            base_config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {"stocks": ["NVDA"], "etfs": ["QQQ"]},
                        "sources": {"sec_filings": False, "google_news": True, "google_research": False},
                        "runtime": {"poll_seconds": 60, "cross_source_dedup_hours": 12},
                        "strategy": {
                            "event_score_threshold": 60.0,
                            "horizons": {
                                "swing": {
                                    "ttl_days": 3,
                                    "market_score_threshold": 55.0,
                                    "priority_threshold": 75.0,
                                },
                                "position": {
                                    "ttl_days": 20,
                                    "market_score_threshold": 55.0,
                                    "priority_threshold": 75.0,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            output_config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {"stocks": ["NVDA"], "etfs": ["QQQ"]},
                        "sources": {"sec_filings": False, "google_news": True, "google_research": False},
                        "runtime": {"poll_seconds": 60, "cross_source_dedup_hours": 12},
                        "strategy": {
                            "event_score_threshold": 62.0,
                            "horizons": {
                                "swing": {
                                    "ttl_days": 3,
                                    "market_score_threshold": 56.0,
                                    "priority_threshold": 76.0,
                                },
                                "position": {
                                    "ttl_days": 20,
                                    "market_score_threshold": 57.0,
                                    "priority_threshold": 77.0,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            manifest_path.write_text(
                json.dumps(
                    {
                        "batch_id": "batch-002",
                        "manifest_path": str(manifest_path),
                        "recommendation": {"name": "tuned", "config_summary": "E65.0/S58.0-78.0/P60.0-80.0/D12"},
                        "experiments": [
                            {
                                "name": "tuned",
                                "evaluation": {
                                    "run": {
                                        "config_snapshot": {
                                            "settings": {
                                                "event_score_threshold": 65.0,
                                                "horizons": {
                                                    "swing": {
                                                        "ttl_days": 3,
                                                        "market_score_threshold": 58.0,
                                                        "priority_threshold": 78.0,
                                                        "rsi_floor": 45.0,
                                                        "rsi_ceiling": 68.0,
                                                        "atr_percent_ceiling": 8.0,
                                                    },
                                                    "position": {
                                                        "ttl_days": 20,
                                                        "market_score_threshold": 60.0,
                                                        "priority_threshold": 80.0,
                                                        "rsi_floor": 50.0,
                                                        "rsi_ceiling": 65.0,
                                                        "atr_percent_ceiling": 10.0,
                                                    },
                                                },
                                            }
                                        }
                                    }
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "promote-batch",
                "--manifest-path",
                str(manifest_path),
                "--output-config-path",
                str(output_config_path),
                "--base-config-path",
                str(base_config_path),
                "--force",
            ]
            with patch.object(sys, "argv", argv):
                with patch("sys.stdout", stdout):
                    main()

            output = stdout.getvalue()
            promoted = json.loads(output_config_path.read_text(encoding="utf-8"))
            backup_candidates = list(temp_path.glob("agent.recommended.*.bak.json"))
            self.assertEqual(len(backup_candidates), 1)
            backed_up = json.loads(backup_candidates[0].read_text(encoding="utf-8"))
            self.assertIn("是否覆盖已有文件：是", output)
            self.assertIn("备份文件：", output)
            self.assertIn("全局事件阈值：62.0 -> 65.0", output)
            self.assertEqual(backed_up["strategy"]["event_score_threshold"], 62.0)
            self.assertEqual(promoted["strategy"]["event_score_threshold"], 65.0)

    def test_send_test_notification_cli_renders_chinese_summary(self) -> None:
        stdout = io.StringIO()
        argv = [
            "satellite-agent",
            "send-test-notification",
            "--symbol",
            "TSLA",
        ]
        with patch.object(sys, "argv", argv):
            with patch("sys.stdout", stdout):
                with patch("satellite_agent.main.send_test_notification") as mocked_send:
                    mocked_send.return_value = {
                        "status": "sent",
                        "channel": "feishu",
                        "symbol": "TSLA",
                        "priority": "high",
                        "headline": "测试提醒已发送。",
                    }
                    main()

        output = stdout.getvalue()
        self.assertIn("飞书测试提醒", output)
        self.assertIn("TSLA", output)
        self.assertIn("feishu", output)

    def test_run_once_writes_latest_live_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            workspace_dir = temp_path / "live_workspace"
            db_path = temp_path / "live.db"

            def fake_build_service(settings, **kwargs):
                store = Store(db_path)
                store.initialize()

                class FakeService:
                    def __init__(self, store):
                        self.store = store
                        self.settings = settings

                    def run_once(self):
                        started = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
                        finished = datetime(2026, 3, 14, 14, 1, tzinfo=timezone.utc)
                        self.store.record_run(
                            run_id="live-run-1",
                            started_at=started,
                            finished_at=finished,
                            status="success",
                            run_name="live_run",
                            note="实时运行",
                            config_snapshot={"settings": settings.to_record()},
                            summary={
                                "events_processed": 1,
                                "cards_generated": 1,
                                "alerts_sent": 1,
                                "extraction_failures": 0,
                                "market_data_failures": 0,
                                "scoring_failures": 0,
                                "notification_failures": 0,
                            },
                        )
                        self.store.record_source_health(
                            "live-run-1",
                            SourceHealthCheck(
                                source_name="google_news",
                                status="healthy",
                                detail="Live source healthy.",
                                latency_ms=200,
                                checked_at=finished,
                            ),
                        )
                        self.store.connection.execute(
                            """
                            INSERT INTO opportunity_cards
                            (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "card-live",
                                "live-run-1",
                                "evt-live",
                                "NVDA",
                                "swing",
                                74.0,
                                "normal",
                                98.0,
                                json.dumps(
                                    {
                                        "card_id": "card-live",
                                        "symbol": "NVDA",
                                        "horizon": "swing",
                                        "event_type": "earnings",
                                        "headline_summary": "Realtime card.",
                                        "priority": "normal",
                                        "event_score": 78.35,
                                        "market_score": 58.9,
                                        "final_score": 74.0,
                                        "source_refs": ["https://example.com/live"],
                                    }
                                ),
                                finished.isoformat(),
                            ),
                        )
                        self.store.connection.commit()
                        return {"events_processed": 1, "cards_generated": 1, "alerts_sent": 1}

                return FakeService(store)

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "run-once",
                "--workspace-dir",
                str(workspace_dir),
            ]
            with patch("satellite_agent.main.build_service", side_effect=fake_build_service):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            review_path = workspace_dir / "latest_live_review.md"
            payload_path = workspace_dir / "latest_live_payload.json"
            self.assertIn("实时监控轮次开始", output)
            self.assertIn("实时运行结果已落盘", output)
            self.assertIn("实时监控轮次结束", output)
            self.assertIn("下次预计刷新", output)
            self.assertTrue(review_path.exists())
            self.assertTrue(payload_path.exists())
            self.assertIn("Realtime card.", review_path.read_text(encoding="utf-8"))

    def test_run_once_uses_default_live_workspace_when_not_provided(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "live.db"
            default_workspace = temp_path / "data" / "live_run"

            def fake_build_service(settings, **kwargs):
                store = Store(db_path)
                store.initialize()

                class FakeService:
                    def __init__(self, store):
                        self.store = store
                        self.settings = settings

                    def run_once(self):
                        started = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
                        finished = datetime(2026, 3, 14, 14, 1, tzinfo=timezone.utc)
                        self.store.record_run(
                            run_id="live-run-default",
                            started_at=started,
                            finished_at=finished,
                            status="success",
                            run_name="live_run",
                            note="实时运行",
                            config_snapshot={"settings": settings.to_record()},
                            summary={
                                "events_processed": 1,
                                "cards_generated": 1,
                                "alerts_sent": 1,
                            },
                        )
                        self.store.connection.execute(
                            """
                            INSERT INTO opportunity_cards
                            (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "card-default",
                                "live-run-default",
                                "evt-live",
                                "NVDA",
                                "swing",
                                74.0,
                                "normal",
                                98.0,
                                json.dumps(
                                    {
                                        "card_id": "card-default",
                                        "symbol": "NVDA",
                                        "horizon": "swing",
                                        "event_type": "earnings",
                                        "headline_summary": "Default workspace card.",
                                        "priority": "normal",
                                        "event_score": 78.35,
                                        "market_score": 58.9,
                                        "final_score": 74.0,
                                        "source_refs": ["https://example.com/default"],
                                    }
                                ),
                                finished.isoformat(),
                            ),
                        )
                        self.store.connection.commit()
                        return {"events_processed": 1, "cards_generated": 1, "alerts_sent": 1}

                return FakeService(store)

            stdout = io.StringIO()
            argv = ["satellite-agent", "run-once"]
            previous_cwd = Path.cwd()
            with patch("satellite_agent.main.build_service", side_effect=fake_build_service):
                try:
                    os.chdir(temp_path)
                    with patch.object(sys, "argv", argv):
                        with patch("sys.stdout", stdout):
                            main()
                finally:
                    os.chdir(previous_cwd)

            output = stdout.getvalue()
            self.assertIn("实时监控轮次开始", output)
            self.assertIn("实时运行结果已落盘", output)
            self.assertIn("实时监控轮次结束", output)
            self.assertTrue((default_workspace / "latest_live_review.md").exists())
            self.assertTrue((default_workspace / "latest_live_payload.json").exists())


if __name__ == "__main__":
    unittest.main()
