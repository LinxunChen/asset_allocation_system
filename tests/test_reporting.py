from __future__ import annotations

import json
import os
import io
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
    format_recent_performance_review,
    format_replay_evaluation,
    format_run_review,
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
from satellite_agent.main import _build_historical_effect_review_data
from satellite_agent.main import build_batch_replay_payload
from satellite_agent.main import build_demo_flow_payload
from satellite_agent.main import build_replay_evaluation_payload
from satellite_agent.main import build_run_comparison_payload
from satellite_agent.main import build_strategy_report_payload
from satellite_agent.main import write_live_run_artifacts
from satellite_agent.config import Settings
from satellite_agent.models import SourceHealthCheck, utcnow
from satellite_agent.runtime_config import AgentRuntimeConfig
from satellite_agent.store import Store


class ReportingTests(unittest.TestCase):
    def test_historical_effect_review_localizes_labels_and_counts_not_entered_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "agent.db"
            store = Store(db_path)
            store.initialize()
            created_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc).isoformat()
            later_created_at = datetime(2026, 3, 15, 14, 0, tzinfo=timezone.utc).isoformat()

            store.save_decision_record(
                decision_id="decision-entered",
                run_id="run-review",
                event_id="evt-entered",
                symbol="NVDA",
                event_type="guidance",
                pool="confirmation",
                action="确认做多",
                priority="normal",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=84.0,
                trigger_mode="direct",
                llm_used=False,
                theme_ids=["semis"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 104.0, "high": 108.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破支撑"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 104.0, "high": 108.0},
                        "invalidation_level": 98.0,
                        "invalidation_reason": "跌破支撑",
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-entered",
                entered=True,
                entered_at=created_at,
                entry_price=101.0,
                exit_price=98.0,
                realized_return=-2.97,
                t_plus_7_return=0.85,
                t_plus_10_return=1.25,
                holding_days=1,
                close_reason="hit_invalidation",
                hit_invalidation=True,
                updated_at=created_at,
            )

            store.save_decision_record(
                decision_id="decision-not-entered",
                run_id="run-review",
                event_id="evt-not-entered",
                symbol="MSFT",
                event_type="news",
                pool="prewatch",
                action="试探建仓",
                priority="normal",
                confidence="中",
                event_score=68.0,
                market_score=61.0,
                theme_score=7.0,
                final_score=70.0,
                trigger_mode="structure",
                llm_used=False,
                theme_ids=["software"],
                entry_plan={
                    "entry_range": {"low": 200.0, "high": 201.0},
                    "take_profit_range": {"low": 206.0, "high": 210.0},
                    "invalidation_level": 197.0,
                },
                invalidation={"level": 197.0, "reason": "跌破支撑"},
                ttl=later_created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 200.0, "high": 201.0},
                        "take_profit_range": {"low": 206.0, "high": 210.0},
                        "invalidation_level": 197.0,
                    }
                },
                created_at=later_created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-not-entered",
                entered=False,
                close_reason="insufficient_lookahead",
                updated_at=later_created_at,
            )

            review = _build_historical_effect_review_data(store, days=30, limit=10)
            report_text = format_recent_performance_review(review)

            self.assertEqual(review["overview"]["decision_count"], 2)
            self.assertEqual(review["overview"]["entered_count"], 1)
            self.assertEqual(review["overview"]["not_entered_count"], 1)
            self.assertEqual(review["overview"]["open_position_count"], 0)
            self.assertIn("已进场（试探建仓/确认做多）：1", report_text)
            self.assertIn("未进场（试探建仓/确认做多，但未到入场区间）：1", report_text)
            self.assertIn("观察中（已进场后）：0", report_text)
            self.assertIn("确认池：样本 1 条", report_text)
            self.assertIn("预备池：样本 1 条", report_text)
            self.assertIn("直接成卡：样本 1 条", report_text)
            self.assertIn("结构预热成卡：样本 1 条", report_text)
            self.assertIn("普通：样本 2 条", report_text)
            self.assertIn("失效退出（确认做多）", report_text)
            self.assertIn("效果最好 Top3（已完成样本）：", report_text)
            self.assertIn("效果最差 Top3（已完成样本）：", report_text)
            self.assertIn("T+7 表现最好 Top3：", report_text)
            self.assertIn("T+7 表现最差 Top3：", report_text)
            self.assertIn("T+7收益", report_text)

    def test_serialize_strategy_report_completed_cohort_summary_prefers_completed_windows(self) -> None:
        report = serialize_strategy_report(
            [],
            [],
            [],
            [
                {
                    "event_type": "earnings",
                    "decision_count": 5,
                    "outcome_count": 5,
                    "pending_count": 3,
                    "take_profit_hits": 1,
                    "invalidation_hits": 1,
                    "positive_t3_count": 2,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 5,
                    "max_drawdown_sample_count": 5,
                    "avg_t_plus_3_return": 9.0,
                    "avg_max_runup": 10.0,
                    "avg_max_drawdown": -4.0,
                },
                {
                    "event_type": "guidance",
                    "decision_count": 2,
                    "outcome_count": 2,
                    "pending_count": 0,
                    "take_profit_hits": 1,
                    "invalidation_hits": 0,
                    "positive_t3_count": 2,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 2,
                    "max_drawdown_sample_count": 2,
                    "avg_t_plus_3_return": 4.0,
                    "avg_max_runup": 6.0,
                    "avg_max_drawdown": -1.0,
                },
            ],
            [
                {
                    "pool": "confirmation",
                    "decision_count": 4,
                    "outcome_count": 4,
                    "pending_count": 2,
                    "take_profit_hits": 1,
                    "invalidation_hits": 1,
                    "positive_t3_count": 2,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 4,
                    "max_drawdown_sample_count": 4,
                    "avg_t_plus_3_return": 8.0,
                    "avg_max_runup": 9.0,
                    "avg_max_drawdown": -3.5,
                },
                {
                    "pool": "prewatch",
                    "decision_count": 2,
                    "outcome_count": 2,
                    "pending_count": 0,
                    "take_profit_hits": 1,
                    "invalidation_hits": 0,
                    "positive_t3_count": 1,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 2,
                    "max_drawdown_sample_count": 2,
                    "avg_t_plus_3_return": 3.5,
                    "avg_max_runup": 4.2,
                    "avg_max_drawdown": -1.2,
                },
                {
                    "pool": "exit",
                    "decision_count": 1,
                    "outcome_count": 1,
                    "pending_count": 1,
                    "take_profit_hits": 0,
                    "invalidation_hits": 0,
                    "positive_t3_count": 0,
                    "t_plus_3_sample_count": 0,
                    "max_runup_sample_count": 1,
                    "max_drawdown_sample_count": 1,
                    "avg_t_plus_3_return": None,
                    "avg_max_runup": 1.0,
                    "avg_max_drawdown": -0.8,
                },
            ],
            {
                "scope": "main_only",
                "scope_label": "主库热数据",
                "combined": {
                    "decision_count": 7,
                    "outcome_count": 7,
                    "pending_count": 3,
                    "completed_count": 4,
                },
            },
        )

        self.assertEqual(report["completed_cohort_summary"]["line_items"][0], "Best completed event cohort: guidance avg_t3=4.0 across 2 completed windows.")
        self.assertIn(
            "Best completed pool cohort: prewatch avg_t3=3.5 across 2 completed windows.",
            report["completed_cohort_summary"]["line_items"],
        )
        self.assertEqual(report["outcome_support"]["title"], "Building Evidence")
        self.assertEqual(report["outcome_support"]["display_title"], "支撑积累中")
        self.assertEqual(report["confidence_regime"]["title"], "积累期")
        self.assertEqual(
            report["outcome_summary"]["line_items"][0],
            "Confidence regime: 积累期.",
        )
        self.assertEqual(
            report["outcome_summary"]["line_items"][1],
            "后验样本正在积累，但还不足以支持激进切换，适合渐进式微调。",
        )
        self.assertIn(
            "Completed windows have reached 4, but the evidence is still concentrated in 2 event cohorts and 2 pool cohorts.",
            report["outcome_support"]["line_items"],
        )
        self.assertEqual(report["outcome_recommendation"]["title"], "Measured Bias")
        self.assertEqual(report["outcome_recommendation"]["display_title"], "渐进确认")
        self.assertIn(
            "当前更适合渐进式加权和小步调整，而不是一次性切换整套策略偏好。",
            report["outcome_recommendation"]["line_items"],
        )
        self.assertIn(
            "Outcome evidence is still building; lean gradually rather than making a full policy rotation.",
            report["outcome_recommendation"]["line_items"],
        )
        self.assertEqual(report["outcome_support"]["event_support"]["title"], "Building")
        self.assertEqual(report["outcome_support"]["event_support"]["display_title"], "积累中")
        self.assertEqual(report["outcome_support"]["pool_support"]["title"], "Building")
        self.assertEqual(report["outcome_support"]["pool_support"]["display_title"], "积累中")
        self.assertIn(
            "Event side has 4 completed windows, but they are concentrated in 2 diversified cohorts.",
            report["outcome_support"]["event_support"]["line_items"],
        )
        self.assertIn(
            "Pool side has 4 completed windows, but they are concentrated in 2 diversified cohorts.",
            report["outcome_support"]["pool_support"]["line_items"],
        )
        self.assertIn(
            "Completed windows: 4 / rolling lookahead: 3 / total written outcomes: 7.",
            report["outcome_maturity"]["line_items"],
        )
        self.assertEqual(report["outcome_validation"]["status"], "ok")
        self.assertEqual(report["action_regime"]["title"], "渐进确认")
        self.assertIn("后验建议主线：渐进确认。", report["action_regime"]["line_items"])
        self.assertEqual(report["decision_outcomes_by_event_type"][0]["readiness_label"], "渐进确认")
        self.assertEqual(report["decision_outcomes_by_event_type"][1]["readiness_label"], "渐进确认")
        self.assertEqual(report["decision_outcomes_by_pool"][1]["readiness_label"], "渐进确认")
        self.assertIn("事件侧较可用分组：earnings（渐进确认）、guidance（渐进确认）。", report["outcome_readiness"]["line_items"])
        self.assertIn("事件排序可渐进参考：财报、指引，适合小步提高排序权重。", report["outcome_readiness_actions"]["line_items"])
        self.assertIn("确认池排序可渐进参考：确认池、预备池，适合先作为辅助排序信号。", report["outcome_readiness_actions"]["line_items"])
        self.assertEqual(
            report["outcome_headline"]["summary"],
            "积累期下优先动作：事件排序可渐进参考：财报、指引，适合小步提高排序权重。",
        )
        self.assertIn("动作口径：渐进确认。", report["outcome_headline"]["line_items"])
        self.assertEqual(
            report["outcome_priority_list"]["line_items"][0],
            "P1 事件排序可渐进参考：财报、指引，适合小步提高排序权重。",
        )
        self.assertEqual(
            report["outcome_priority_list"]["line_items"][1],
            "P2 确认池排序可渐进参考：确认池、预备池，适合先作为辅助排序信号。",
        )

    def test_serialize_strategy_report_validation_flags_top_slice_without_warning(self) -> None:
        report = serialize_strategy_report(
            [],
            [],
            [],
            [
                {
                    "event_type": "earnings",
                    "decision_count": 2,
                    "outcome_count": 2,
                    "pending_count": 0,
                    "take_profit_hits": 1,
                    "invalidation_hits": 0,
                    "positive_t3_count": 2,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 2,
                    "max_drawdown_sample_count": 2,
                    "avg_t_plus_3_return": 3.0,
                    "avg_max_runup": 5.0,
                    "avg_max_drawdown": -1.5,
                }
            ],
            [
                {
                    "pool": "confirmation",
                    "decision_count": 3,
                    "outcome_count": 3,
                    "pending_count": 1,
                    "take_profit_hits": 1,
                    "invalidation_hits": 1,
                    "positive_t3_count": 2,
                    "t_plus_3_sample_count": 2,
                    "max_runup_sample_count": 3,
                    "max_drawdown_sample_count": 3,
                    "avg_t_plus_3_return": 2.5,
                    "avg_max_runup": 4.5,
                    "avg_max_drawdown": -2.0,
                }
            ],
            {
                "scope": "main_only",
                "scope_label": "主库热数据",
                "combined": {
                    "decision_count": 3,
                    "outcome_count": 3,
                    "pending_count": 1,
                    "completed_count": 2,
                },
            },
        )

        self.assertEqual(report["outcome_validation"]["status"], "ok")
        self.assertIn(
            "Displayed event cohorts are a top-slice view: decisions 2/3, outcomes 2/3.",
            report["outcome_validation"]["line_items"],
        )
        self.assertIn(
            "Pool cohorts reconcile with total coverage: decisions 3, outcomes 3.",
            report["outcome_validation"]["line_items"],
        )

    def test_build_strategy_report_payload_merges_archive_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            store = Store(temp_path / "agent.db")
            archive_store = Store(temp_path / "archive.db")
            store.initialize()
            archive_store.initialize()
            created_at = (utcnow() - timedelta(days=2)).isoformat()
            archived_at = (utcnow() - timedelta(days=5)).isoformat()
            store.save_decision_record(
                decision_id="decision-main",
                run_id="run-main",
                event_id="evt-main",
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
            store.save_decision_outcome(
                decision_id="decision-main",
                t_plus_3_return=4.0,
                close_reason="window_complete",
                updated_at=created_at,
            )
            archive_store.save_decision_record(
                decision_id="decision-archive",
                run_id="run-archive",
                event_id="evt-archive",
                symbol="AMD",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=78.0,
                market_score=69.0,
                theme_score=7.5,
                final_score=80.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={},
                invalidation={},
                ttl=archived_at,
                packet={},
                created_at=archived_at,
            )
            archive_store.save_decision_outcome(
                decision_id="decision-archive",
                t_plus_3_return=2.0,
                close_reason="window_complete",
                updated_at=archived_at,
            )

            report = build_strategy_report_payload(
                store,
                days=14,
                limit=10,
                archive_store=archive_store,
                archive_db_path=str((temp_path / "archive.db").resolve()),
            )

            self.assertEqual(report["decision_outcomes_by_event_type"][0]["event_type"], "earnings")
            self.assertEqual(report["decision_outcomes_by_event_type"][0]["decision_count"], 2)
            self.assertEqual(report["decision_outcomes_by_event_type"][0]["outcome_count"], 2)
            self.assertEqual(report["decision_outcomes_by_event_type"][0]["completed_count"], 2)
            self.assertEqual(report["decision_outcomes_by_event_type"][0]["avg_t_plus_3_return"], 3.0)
            self.assertEqual(report["outcome_data_coverage"]["scope"], "main_plus_archive")
            self.assertEqual(report["outcome_data_coverage"]["main_store"]["decision_count"], 1)
            self.assertEqual(report["outcome_data_coverage"]["archive_store"]["decision_count"], 1)
            self.assertEqual(report["outcome_data_coverage"]["combined"]["decision_count"], 2)
            self.assertEqual(report["outcome_data_coverage"]["combined"]["completed_count"], 2)
            self.assertEqual(report["outcome_validation"]["status"], "ok")

            archive_store.close()
            store.close()

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
                    "runtime_config": {
                        "sources": {"google_news": True},
                        "watchlist": {
                            "stock_groups": {
                                "semiconductors_and_ai": ["NVDA", "NBIS", "MU"],
                            }
                        },
                    },
                },
                summary={
                    "events_processed": 2,
                    "cards_generated": 4,
                    "alerts_sent": 1,
                    "prewatch_alerts_sent_count": 1,
                    "prewatch_alert_symbols": ["NBIS"],
                    "prewatch_candidates": [
                        {
                            "symbol": "NBIS",
                            "horizon": "position",
                            "setup_type": "breakout_watch",
                            "score": 78.4,
                            "headline_summary": "NBIS 出现放量突破预热，价格已逼近或站上阶段阻力，适合提前列入重点观察。",
                            "action_hint": "可先列入预备池，等待放量站稳阻力位后再考虑升入确认池。",
                            "reason_to_watch": "维持多头结构；相对量能放大至 1.86 倍；价格处于突破预热区；RSI 58.2，强势但未过热。",
                            "last_price": 112.4,
                            "rsi_14": 58.2,
                            "relative_volume": 1.86,
                            "trend_state": "bullish",
                            "support_20": 103.0,
                            "resistance_20": 111.5,
                            "as_of": finished.isoformat(),
                        }
                    ],
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
                            "headline_summary": "AI 需求改善带动 NVDA 进入确认阶段。",
                            "reason_to_watch": "此前已进入预备池，本轮财报与市场确认共振。",
                            "positioning_hint": "可从观察仓提升到主交易仓位，优先等待盘中回踩确认。",
                            "promoted_from_prewatch": True,
                            "prewatch_score": 81.5,
                            "prewatch_setup_type": "breakout_watch",
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
                event_score=78.35,
                market_score=58.9,
                theme_score=8.0,
                final_score=88.5,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={"entry_range": {"low": 100.0, "high": 102.0}},
                invalidation={"level": 98.0, "reason": "跌破关键支撑"},
                ttl=finished.isoformat(),
                packet={"symbol": "NVDA", "pool": "confirmation"},
                created_at=finished.isoformat(),
            )
            store.save_decision_outcome(
                decision_id="decision-1",
                t_plus_1_return=1.6,
                t_plus_3_return=4.2,
                t_plus_10_return=9.4,
                max_runup=6.8,
                max_drawdown=-1.9,
                hit_take_profit=True,
                close_reason="window_complete",
                updated_at=finished.isoformat(),
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
            outcome_event_rows = store.aggregate_decision_outcomes_by_event_type(since)
            outcome_pool_rows = store.aggregate_decision_outcomes_by_pool(since)
            strategy_text = format_strategy_report(
                strategy_rows,
                source_rows,
                alert_rows,
                outcome_event_rows,
                outcome_pool_rows,
            )
            strategy_json = serialize_strategy_report(
                strategy_rows,
                source_rows,
                alert_rows,
                outcome_event_rows,
                outcome_pool_rows,
            )
            replay_payload = build_replay_evaluation_payload(store, run_id="run-1", days=14, limit=10)
            replay_text = format_replay_evaluation(
                replay_payload["run"],
                replay_payload["strategy_report"],
                replay_payload["source_health"],
                replay_payload["card_diagnostics"],
                replay_payload.get("decision_diagnostics", []),
            )
            review_text = format_run_review(
                replay_payload["run"],
                replay_payload["strategy_report"],
                replay_payload["source_health"],
                replay_payload["card_diagnostics"],
                replay_payload.get("decision_diagnostics", []),
            )
            replay_json = serialize_replay_evaluation(
                replay_payload["run"],
                replay_payload["strategy_report"],
                replay_payload["source_health"],
                replay_payload["card_diagnostics"],
                replay_payload.get("decision_diagnostics", []),
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
            self.assertIn("Decision Outcomes By Event Type", strategy_text)
            self.assertIn("Decision Outcomes By Pool", strategy_text)
            self.assertIn("Outcome Maturity", strategy_text)
            self.assertIn("Completed Cohorts", strategy_text)
            self.assertIn("Outcome Support:", strategy_text)
            self.assertIn("Event Support:", strategy_text)
            self.assertIn("Pool Support:", strategy_text)
            self.assertIn("Confidence Regime: 观察期", strategy_text)
            self.assertIn("Outcome Validation: OK", strategy_text)
            self.assertIn("Outcome Summary", strategy_text)
            self.assertIn("Outcome Recommendation: 观察优先", strategy_text)
            self.assertIn("Strategy Tilt: 等待成熟", strategy_text)
            self.assertIn("Action Regime: 观察优先", strategy_text)
            self.assertIn("Outcome Readiness:", strategy_text)
            self.assertIn("Readiness Actions:", strategy_text)
            self.assertIn("Outcome Headline: 观察期下优先动作：仍以观察为主的事件：财报。", strategy_text)
            self.assertIn("Outcome Priorities:", strategy_text)
            self.assertIn("P1 仍以观察为主的事件：财报。", strategy_text)
            self.assertIn("earnings: decisions=1 outcomes=1", strategy_text)
            self.assertIn("readiness=轻度参考", strategy_text)
            self.assertIn("confirmation: decisions=1 outcomes=1", strategy_text)
            self.assertIn("Summary confidence is tempered because event-side evidence is thin and pool-side evidence is thin.", strategy_text)
            self.assertIn("Strongest event outcome so far: earnings avg_t3=4.2 with 1 outcomes.", strategy_text)
            self.assertIn("Best pool so far: confirmation avg_t3=4.2 with 1 outcomes.", strategy_text)
            self.assertIn("Outcome support is still thin, so any event or pool preference should be treated as provisional.", strategy_text)
            self.assertIn("当前更适合观察和轻推，而不是把后验结果直接翻译成强执行动作。", strategy_text)
            self.assertIn("Lean more on earnings while it keeps the best average T+3 return (4.2).", strategy_text)
            self.assertIn("Keep confirmation as the cleaner downstream pool; its average T+3 return is 4.2.", strategy_text)
            self.assertIn("Completed windows are still too few to justify a strong directional tilt.", strategy_text)
            self.assertIn("Current live flow is concentrated in earnings, and that also has the strongest mature T+3 outcome (4.2).", strategy_text)
            self.assertIn("Use confirmation decisions as the primary execution lane while outcome quality remains strongest there.", strategy_text)
            self.assertIn("当前更适合把后验结果当作方向参考，先看信号是否持续，不急于放大仓位或大改阈值。", strategy_text)
            self.assertIn("事件侧仍偏观察分组：earnings（轻度参考）。", strategy_text)
            self.assertIn("池子侧仍偏观察分组：confirmation（轻度参考）。", strategy_text)
            self.assertIn("仍以观察为主的事件：财报。", strategy_text)
            self.assertIn("仍以观察为主的池子：确认池。", strategy_text)
            self.assertEqual(strategy_json["event_type_performance"][0]["event_type"], "earnings")
            self.assertEqual(strategy_json["alert_volume"][0]["high_priority_alerts"], 1)
            self.assertEqual(strategy_json["decision_outcomes_by_event_type"][0]["event_type"], "earnings")
            self.assertEqual(strategy_json["decision_outcomes_by_event_type"][0]["outcome_count"], 1)
            self.assertEqual(strategy_json["decision_outcomes_by_event_type"][0]["completed_count"], 1)
            self.assertEqual(strategy_json["decision_outcomes_by_pool"][0]["pool"], "confirmation")
            self.assertEqual(strategy_json["confidence_regime"]["title"], "观察期")
            self.assertIn("Confidence regime: 观察期.", strategy_json["outcome_summary"]["line_items"][0])
            self.assertIn(
                "Strongest event outcome so far: earnings avg_t3=4.2 with 1 outcomes.",
                strategy_json["outcome_summary"]["line_items"],
            )
            self.assertIn("Completed windows: 1 / rolling lookahead: 0 / total written outcomes: 1.", strategy_json["outcome_maturity"]["line_items"][0])
            self.assertEqual(strategy_json["outcome_validation"]["status"], "ok")
            self.assertIn("Invariant check passed for all displayed cohorts.", strategy_json["outcome_validation"]["line_items"][0])
            self.assertEqual(strategy_json["outcome_recommendation"]["title"], "Provisional Bias")
            self.assertEqual(strategy_json["outcome_recommendation"]["display_title"], "观察优先")
            self.assertEqual(strategy_json["strategy_tilt"]["title"], "Wait For Maturity")
            self.assertEqual(strategy_json["strategy_tilt"]["display_title"], "等待成熟")
            self.assertEqual(strategy_json["action_regime"]["title"], "观察优先")
            self.assertEqual(strategy_json["decision_outcomes_by_event_type"][0]["readiness_label"], "轻度参考")
            self.assertEqual(strategy_json["decision_outcomes_by_pool"][0]["readiness_label"], "轻度参考")
            self.assertIn("事件侧仍偏观察分组：earnings（轻度参考）。", strategy_json["outcome_readiness"]["line_items"])
            self.assertIn("仍以观察为主的事件：财报。", strategy_json["outcome_readiness_actions"]["line_items"])
            self.assertEqual(strategy_json["outcome_headline"]["summary"], "观察期下优先动作：仍以观察为主的事件：财报。")
            self.assertEqual(strategy_json["outcome_priority_list"]["line_items"][0], "P1 仍以观察为主的事件：财报。")
            self.assertIn("Replay Evaluation", replay_text)
            self.assertIn("Prewatch Candidates", replay_text)
            self.assertIn("NBIS", replay_text)
            self.assertIn("Card Diagnostics", replay_text)
            self.assertIn("Decision Diagnostics", replay_text)
            self.assertIn("Outcome Context:", replay_text)
            self.assertIn("baseline", replay_text)
            self.assertIn("Started: 2026-03-14 22:00", replay_text)
            self.assertIn("预备池：", review_text)
            self.assertIn("NBIS / 波段 / 突破预热", review_text)
            self.assertIn("预备池轻推：", review_text)
            self.assertIn("决策记录：", review_text)
            self.assertIn("确认做多", review_text)
            self.assertIn("历史后验背景：事件 earnings 近期待回写 0 条，T+3 均值 4.2，已回写 1 条；池子 确认池 T+3 均值 4.2，止盈 1 条。", review_text)
            self.assertNotIn("后验结果概览：", review_text)
            self.assertNotIn("后验建议：", review_text)
            self.assertNotIn("策略倾向：", review_text)
            self.assertIn("升池确认：", review_text)
            self.assertIn("NVDA / 交易周期：短线 / 财报事件 / 高优先级", review_text)
            self.assertIn("升池原因：此前处于突破预热预备状态（81.50 分），本轮事件达到确认条件。", review_text)
            self.assertIn("池位：预备池升级确认池（突破预热 / 预备池 81.50 分）", review_text)
            self.assertIn("仓位提示：可从观察仓提升到主交易仓位，优先等待盘中回踩确认。", review_text)
            self.assertIn("事件解读：财报事件：AI 需求改善带动 NVDA 进入确认阶段。", review_text)
            self.assertIn("题材解读：题材：半导体与AI", review_text)
            self.assertIn("题材链路：", review_text)
            self.assertIn("半导体与AI / 热度 8", review_text)
            self.assertIn("确认池：NVDA", review_text)
            self.assertIn("预备池：NBIS", review_text)
            self.assertIn("题材：半导体与AI", review_text)
            self.assertIn("本轮状态 成功，健康判断为 阻塞；共处理 2 个事件，生成 4 张卡片，发送 1 条提醒，识别 1 个预备池候选，并发出 1 条预备池轻推。", review_text)
            self.assertEqual(replay_json["run"]["run_id"], "run-1")
            self.assertEqual(replay_json["run"]["run_name"], "baseline")
            self.assertEqual(replay_json["prewatch_candidates"][0]["symbol"], "NBIS")
            self.assertEqual(replay_json["card_diagnostics"][0]["horizon"], "swing")
            self.assertEqual(replay_json["decision_diagnostics"][0]["symbol"], "NVDA")
            self.assertEqual(replay_json["decision_diagnostics"][0]["event_type_outcome_context"]["avg_t_plus_3_return"], 4.2)
            self.assertEqual(replay_json["decision_diagnostics"][0]["pool_outcome_context"]["take_profit_hits"], 1)
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

    def test_write_performance_review_cli_writes_report_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            workspace_dir = temp_path / "reports"
            store = Store(db_path)
            store.initialize()
            started = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
            finished = datetime(2026, 3, 14, 14, 1, tzinfo=timezone.utc)
            store.record_run(
                run_id="run-performance",
                started_at=started,
                finished_at=finished,
                status="success",
                summary={"events_processed": 0, "cards_generated": 0, "alerts_sent": 0},
            )
            store.close()

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "write-performance-review",
                "--workspace-dir",
                str(workspace_dir),
                "--days",
                "14",
                "--limit",
                "5",
            ]
            with patch.dict(os.environ, {"SATELLITE_DB_PATH": str(db_path)}, clear=False):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            report_path = workspace_dir / "historical_effect" / "review.md"
            payload_path = workspace_dir / "historical_effect" / "review_payload.json"
            sample_audit_path = workspace_dir / "historical_effect" / "sample_audit.md"
            sample_audit_payload_path = workspace_dir / "historical_effect" / "sample_audit_payload.json"
            self.assertIn("历史效果复盘：", output)
            self.assertIn(str(report_path), output)
            self.assertIn(str(sample_audit_path), output)
            self.assertTrue(report_path.exists())
            self.assertTrue(payload_path.exists())
            self.assertTrue(sample_audit_path.exists())
            self.assertTrue(sample_audit_payload_path.exists())
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("历史效果复盘（草稿）", report_text)
            self.assertIn("统计区间：", report_text)
            self.assertIn("正式版门槛：未满足", report_text)
            self.assertIn("复权状态：未满足", report_text)
            self.assertIn("程序抽检：", report_text)
            self.assertIn("总体效果：", report_text)
            self.assertIn("执行质量：", report_text)
            self.assertIn("分组效果 - 按事件类型：", report_text)
            self.assertIn("辅助观察收益（均值=平均收益率）：", report_text)
            self.assertIn("决策效果明细（最近 20 条）：", report_text)
            self.assertIn("下一步建议：", report_text)
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["run_id"], "run-performance")
            self.assertEqual(payload["window_days"], 14)
            self.assertIn("sample_audit", payload)

    def test_report_outcome_samples_cli_outputs_sample_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            store = Store(db_path)
            store.initialize()
            created_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-sample-1",
                run_id="run-sample",
                event_id="evt-sample",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=84.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semis"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 104.0, "high": 108.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破支撑"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 104.0, "high": 108.0},
                        "invalidation_level": 98.0,
                        "invalidation_reason": "跌破支撑",
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-sample-1",
                entered=True,
                entered_at=created_at,
                entry_price=101.0,
                exit_price=104.0,
                realized_return=2.97,
                holding_days=2,
                close_reason="hit_take_profit",
                hit_take_profit=True,
                updated_at=created_at,
            )
            from satellite_agent.models import Bar

            store.upsert_price_bars(
                "NVDA",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc),
                        open=101.0,
                        high=105.0,
                        low=100.0,
                        close=104.0,
                        volume=100000,
                        adjusted=True,
                    ),
                    Bar(
                        timestamp=datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc),
                        open=104.5,
                        high=106.0,
                        low=103.5,
                        close=105.5,
                        volume=110000,
                        adjusted=True,
                    ),
                ],
            )
            store.close()

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "report-outcome-samples",
                "--days",
                "30",
                "--limit",
                "5",
            ]
            with patch.dict(os.environ, {"SATELLITE_DB_PATH": str(db_path)}, clear=False):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            self.assertIn("后验样本抽检：", output)
            self.assertIn("核对摘要： 一致 1 / 不一致 0 / 无法重算 0", output)
            self.assertIn("NVDA | 确认做多 | earnings | 止盈退出", output)
            self.assertIn("决策：decision-sample-1 | run=run-sample", output)
            self.assertIn("价格计划：入场 100.0-101.0，止盈 104.0-108.0，失效价 98.0", output)
            self.assertIn("执行结果：进场 True，进场时间 2026-03-14T14:00:00+00:00，进场价 101.0，退出价 104.0，真实收益 2.97%", output)
            self.assertIn("日线复权：总计 2，复权 2，未复权 0", output)
            self.assertIn("重算核对：一致", output)
            self.assertIn("K线轨迹：", output)
            self.assertIn("2026-03-14 O=101.0 H=105.0 L=100.0 C=104.0 | 复权=是 | 标记=入场触碰 / 止盈触碰 / 锚点", output)

    def test_write_outcome_audit_cli_writes_audit_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            workspace_dir = temp_path / "workspace"
            store = Store(db_path)
            store.initialize()
            created_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-audit-1",
                run_id="run-audit",
                event_id="evt-audit",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=84.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 104.0, "high": 108.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破失效位"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 104.0, "high": 108.0},
                        "invalidation_level": 98.0,
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-audit-1",
                entered=True,
                entered_at=created_at,
                entry_price=101.0,
                exit_price=104.0,
                realized_return=2.97,
                holding_days=2,
                close_reason="hit_take_profit",
                hit_take_profit=True,
                updated_at=created_at,
            )
            from satellite_agent.models import Bar

            store.upsert_price_bars(
                "NVDA",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc),
                        open=101.0,
                        high=105.0,
                        low=100.0,
                        close=104.0,
                        volume=100000,
                        adjusted=True,
                    ),
                    Bar(
                        timestamp=datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc),
                        open=104.5,
                        high=106.0,
                        low=103.5,
                        close=105.5,
                        volume=110000,
                        adjusted=True,
                    ),
                ],
            )
            store.close()

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "write-outcome-audit",
                "--workspace-dir",
                str(workspace_dir),
                "--days",
                "30",
                "--limit",
                "5",
            ]
            with patch.dict(os.environ, {"SATELLITE_DB_PATH": str(db_path)}, clear=False):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            audit_path = workspace_dir / "historical_effect" / "ai_review.md"
            audit_payload_path = workspace_dir / "historical_effect" / "ai_review_payload.json"
            self.assertIn("AI样本复核：", output)
            self.assertIn(str(audit_path), output)
            self.assertTrue(audit_path.exists())
            self.assertTrue(audit_payload_path.exists())
            audit_text = audit_path.read_text(encoding="utf-8")
            self.assertIn("AI样本复核记录：", audit_text)
            self.assertIn("状态：通过", audit_text)

    def test_performance_review_uses_completed_manual_audit_to_clear_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            store = Store(db_path)
            store.initialize()
            created_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-manual-audit-1",
                run_id="run-manual-audit",
                event_id="evt-manual-audit",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=84.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 104.0, "high": 108.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破失效位"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 104.0, "high": 108.0},
                        "invalidation_level": 98.0,
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-manual-audit-1",
                entered=True,
                entered_at=created_at,
                entry_price=101.0,
                exit_price=104.0,
                realized_return=2.97,
                holding_days=2,
                close_reason="hit_take_profit",
                hit_take_profit=True,
                updated_at=created_at,
            )
            from satellite_agent.models import Bar

            store.upsert_price_bars(
                "NVDA",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc),
                        open=101.0,
                        high=105.0,
                        low=100.0,
                        close=104.0,
                        volume=100000,
                        adjusted=True,
                    )
                ],
            )
            from satellite_agent.main import _resolve_review_window

            review_window = _resolve_review_window(days=30)
            store.set_state(
                "historical_effect_manual_audit",
                json.dumps(
                    {
                        "status": "通过",
                        "review_version": "v1",
                        "review_window": {
                            "start_date": review_window["start_date"],
                            "end_date": review_window["end_date"],
                        },
                        "reviewed_at": created_at,
                        "reviewer": "codex",
                        "summary_line": "抽检 1 条样本，全部与当前复盘口径一致。",
                    },
                    ensure_ascii=False,
                ),
            )

            from satellite_agent.main import _build_historical_effect_review_data

            review = _build_historical_effect_review_data(store, days=30, limit=5)

            self.assertEqual(review["manual_audit"]["status"], "通过")
            self.assertIn("AI复核：通过", format_recent_performance_review(review))
            self.assertNotIn("AI样本复核尚未完成。", review["formal_readiness"]["blockers"])

    def test_freeze_review_baseline_cli_writes_baseline_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            workspace_dir = temp_path / "workspace"
            store = Store(db_path)
            store.initialize()
            store.close()

            stdout = io.StringIO()
            argv = [
                "satellite-agent",
                "freeze-review-baseline",
                "--workspace-dir",
                str(workspace_dir),
                "--reviewer",
                "codex",
            ]
            with patch.dict(os.environ, {"SATELLITE_DB_PATH": str(db_path)}, clear=False):
                with patch.object(sys, "argv", argv):
                    with patch("sys.stdout", stdout):
                        main()

            output = stdout.getvalue()
            baseline_path = workspace_dir / "historical_effect" / "review_baseline.md"
            baseline_payload_path = workspace_dir / "historical_effect" / "review_baseline_payload.json"
            self.assertIn("复盘口径冻结：", output)
            self.assertTrue(baseline_path.exists())
            self.assertTrue(baseline_payload_path.exists())
            baseline_text = baseline_path.read_text(encoding="utf-8")
            self.assertIn("复盘口径冻结记录：", baseline_text)
            self.assertIn("状态：已冻结", baseline_text)

    def test_performance_review_becomes_formal_when_all_gates_are_met(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "agent.db"
            store = Store(db_path)
            store.initialize()
            created_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc).isoformat()
            store.save_decision_record(
                decision_id="decision-formal-1",
                run_id="run-formal",
                event_id="evt-formal",
                symbol="NVDA",
                event_type="earnings",
                pool="confirmation",
                action="确认做多",
                priority="high",
                confidence="高",
                event_score=82.0,
                market_score=76.0,
                theme_score=8.0,
                final_score=84.0,
                trigger_mode="resonance",
                llm_used=False,
                theme_ids=["semiconductors_and_ai"],
                entry_plan={
                    "entry_range": {"low": 100.0, "high": 101.0},
                    "take_profit_range": {"low": 104.0, "high": 108.0},
                    "invalidation_level": 98.0,
                },
                invalidation={"level": 98.0, "reason": "跌破失效位"},
                ttl=created_at,
                packet={
                    "price_plan": {
                        "entry_range": {"low": 100.0, "high": 101.0},
                        "take_profit_range": {"low": 104.0, "high": 108.0},
                        "invalidation_level": 98.0,
                    }
                },
                created_at=created_at,
            )
            store.save_decision_outcome(
                decision_id="decision-formal-1",
                entered=True,
                entered_at=created_at,
                entry_price=101.0,
                exit_price=104.0,
                realized_return=2.97,
                holding_days=2,
                close_reason="hit_take_profit",
                hit_take_profit=True,
                updated_at=created_at,
            )
            from satellite_agent.models import Bar

            store.upsert_price_bars(
                "NVDA",
                "1d",
                [
                    Bar(
                        timestamp=datetime(2026, 3, 14, 0, 0, tzinfo=timezone.utc),
                        open=101.0,
                        high=105.0,
                        low=100.0,
                        close=104.0,
                        volume=100000,
                        adjusted=True,
                    )
                ],
            )
            from satellite_agent.main import _resolve_review_window

            review_window = _resolve_review_window(days=30)
            store.set_state(
                "historical_effect_manual_audit",
                json.dumps(
                    {
                        "status": "通过",
                        "review_version": "v1",
                        "review_window": {
                            "start_date": review_window["start_date"],
                            "end_date": review_window["end_date"],
                        },
                        "reviewed_at": created_at,
                        "reviewer": "codex",
                        "summary_line": "抽检 1 条样本，全部与当前复盘口径一致。",
                    },
                    ensure_ascii=False,
                ),
            )
            store.set_state(
                "historical_effect_review_baseline",
                json.dumps(
                    {
                        "status": "已冻结",
                        "review_version": "v1",
                        "frozen_at": created_at,
                        "reviewer": "codex",
                        "note": "",
                    },
                    ensure_ascii=False,
                ),
            )

            from satellite_agent.main import _build_historical_effect_review_data

            review = _build_historical_effect_review_data(store, days=30, limit=5)

            self.assertEqual(review["status"], "正式")
            self.assertEqual(review["status_label"], "历史效果复盘（正式）")
            self.assertTrue(review["formal_readiness"]["ready"])
            self.assertEqual(review["formal_readiness"]["blockers"], [])
            self.assertEqual(review["review_baseline"]["status"], "已冻结")

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
            performance_review_path = workspace_dir / "historical_effect" / "review.md"
            self.assertIn("日常运行", output)
            self.assertIn("配置文件：", output)
            self.assertIn("健康判断：正常", output)
            self.assertIn("后验回补：最近 45 天，扫描", output)
            self.assertTrue(review_path.exists())
            self.assertTrue(payload_path.exists())
            self.assertTrue(performance_review_path.exists())
            review_text = review_path.read_text(encoding="utf-8")
            performance_text = performance_review_path.read_text(encoding="utf-8")
            self.assertIn("运行复盘", review_text)
            self.assertIn("健康判断为 正常", review_text)
            self.assertIn("结论摘要", review_text)
            self.assertIn("运行健康：", review_text)
            self.assertNotIn("后验结果概览：", review_text)
            self.assertIn("机会概览", review_text)
            self.assertIn("卡片解读", review_text)
            self.assertIn("事件解读：财报事件：AI demand improved.", review_text)
            self.assertIn("信息来源：example.com", review_text)
            self.assertIn("原文链接 1：https://example.com/nvda-earnings", review_text)
            self.assertIn("历史效果复盘（草稿）", performance_text)
            self.assertIn("总体效果：", performance_text)
            self.assertIn("执行质量：", performance_text)
            self.assertIn("下一步建议：", performance_text)
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
            review_path = workspace_dir / "run_once_review.md"
            payload_path = workspace_dir / "run_once_payload.json"
            performance_review_path = workspace_dir / "historical_effect" / "review.md"
            self.assertIn("实时监控轮次开始", output)
            self.assertIn("实时运行结果已落盘", output)
            self.assertIn("后验回补：最近 45 天，扫描", output)
            self.assertIn("实时监控轮次结束", output)
            self.assertIn("下次预计刷新", output)
            self.assertTrue(review_path.exists())
            self.assertTrue(payload_path.exists())
            self.assertTrue(performance_review_path.exists())
            self.assertIn("Realtime card.", review_path.read_text(encoding="utf-8"))
            performance_text = performance_review_path.read_text(encoding="utf-8")
            self.assertIn("历史效果复盘（草稿）", performance_text)
            self.assertIn("总体效果：", performance_text)
            self.assertIn("下一步建议：", performance_text)

    def test_run_once_uses_default_live_workspace_when_not_provided(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "live.db"
            default_workspace = temp_path / "data" / "satellite_agent" / "run_once"

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
            self.assertTrue((default_workspace / "run_once_review.md").exists())
            self.assertTrue((default_workspace / "run_once_payload.json").exists())

    def test_write_live_run_artifacts_reuses_recent_historical_effect_review_when_throttled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "live.db"
            workspace_dir = temp_path / "live_workspace"
            workspace_dir.mkdir(parents=True, exist_ok=True)
            historical_review_path = workspace_dir / "historical_effect" / "review.md"
            historical_review_path.parent.mkdir(parents=True, exist_ok=True)
            historical_review_path.write_text("existing review", encoding="utf-8")
            store = Store(db_path)
            store.initialize()

            with patch("satellite_agent.main._auto_backfill_review_outcomes", return_value={"days": 45, "scanned": 0, "updated": 0, "skipped": 0, "fetched_symbols": 0}):
                with patch(
                    "satellite_agent.main.build_replay_evaluation_payload",
                    return_value={
                        "run": {},
                        "strategy_report": {},
                        "source_health": [],
                        "card_diagnostics": [],
                        "decision_diagnostics": [],
                    },
                ):
                    with patch("satellite_agent.main.format_run_review", return_value="live review"):
                        with patch("satellite_agent.main.build_performance_review_payload") as performance_review_mock:
                            payload = write_live_run_artifacts(
                                store,
                                run_id="live-run-1",
                                workspace_dir=workspace_dir,
                                limit=10,
                                review_filename="serve_review.md",
                                payload_filename="serve_payload.json",
                                historical_effect_min_interval_seconds=3600,
                            )

            self.assertFalse(payload["historical_effect_review_refreshed"])
            self.assertEqual(payload["historical_effect_review_path"], str(historical_review_path.resolve()))
            performance_review_mock.assert_not_called()
            store.close()


if __name__ == "__main__":
    unittest.main()
