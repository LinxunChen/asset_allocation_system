from __future__ import annotations

import json
import argparse
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.config import Settings
from satellite_agent.main import (
    _apply_cli_experiment_overrides,
    _is_sec_eligible_symbol,
    _limit_symbols_for_source,
    _prioritize_symbols_for_source,
    _runtime_window_pause,
    _sync_watchlist_if_needed,
)
from satellite_agent.runtime_config import (
    AgentRuntimeConfig,
    RuntimeWindowConfig,
    WatchlistConfig,
    load_default_template_payload,
    write_default_config,
)
from satellite_agent.store import Store


class RuntimeConfigTests(unittest.TestCase):
    def test_runtime_config_applies_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {"stocks": ["nvda", "msft"], "etfs": ["qqq"]},
                        "sources": {
                            "sec_filings": True,
                            "google_news": True,
                            "google_research": False,
                        },
                        "runtime": {"poll_seconds": 120, "cross_source_dedup_hours": 6},
                        "runtime_window": {
                            "enabled": True,
                            "timezone": "Asia/Shanghai",
                            "weekdays": ["mon", "tue", "wed", "thu", "fri"],
                            "start_time": "18:00",
                            "end_time": "04:00",
                        },
                        "notifications": {
                            "feishu_webhook": "https://example.feishu.cn/webhook/test",
                            "dry_run": True,
                        },
                        "strategy": {
                            "event_score_threshold": 65,
                            "horizons": {
                                "swing": {
                                    "market_score_threshold": 58,
                                    "priority_threshold": 78,
                                    "rsi_floor": 47,
                                },
                                "position": {
                                    "market_score_threshold": 60,
                                    "priority_threshold": 80,
                                },
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            runtime_config = AgentRuntimeConfig.load(config_path)
            settings = runtime_config.apply(Settings())
            self.assertTrue(runtime_config.has_watchlist())
            self.assertEqual(runtime_config.watchlist.stocks, ["NVDA", "MSFT"])
            self.assertEqual(runtime_config.watchlist.etfs, ["QQQ"])
            self.assertEqual(runtime_config.watchlist.stock_groups, {})
            self.assertEqual(runtime_config.watchlist.etf_groups, {})
            self.assertTrue(settings.use_sec_filings_source)
            self.assertTrue(settings.use_google_news_source)
            self.assertEqual(settings.poll_seconds, 120)
            self.assertEqual(settings.cross_source_dedup_hours, 6)
            self.assertEqual(settings.feishu_webhook, "https://example.feishu.cn/webhook/test")
            self.assertTrue(settings.dry_run)
            self.assertTrue(runtime_config.runtime_window.enabled)
            self.assertEqual(runtime_config.runtime_window.timezone, "Asia/Shanghai")
            self.assertEqual(runtime_config.runtime_window.weekdays, ["mon", "tue", "wed", "thu", "fri"])
            self.assertEqual(runtime_config.runtime_window.start_time, "18:00")
            self.assertEqual(runtime_config.runtime_window.end_time, "04:00")
            self.assertEqual(settings.event_score_threshold, 65)
            self.assertEqual(settings.horizons["swing"].market_score_threshold, 58)
            self.assertEqual(settings.horizons["swing"].priority_threshold, 78)
            self.assertEqual(settings.horizons["swing"].rsi_floor, 47)
            self.assertEqual(settings.horizons["position"].market_score_threshold, 60)
            self.assertEqual(settings.horizons["position"].priority_threshold, 80)

    def test_runtime_config_flattens_grouped_watchlist_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stock_groups": {
                                "core_growth": ["nvda", "msft"],
                                "semis": ["amd", "nvda"],
                            },
                            "etf_groups": {
                                "broad_market": ["qqq", "spy"],
                                "sectors": ["smh"],
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            runtime_config = AgentRuntimeConfig.load(config_path)
            self.assertEqual(runtime_config.watchlist.stocks, ["NVDA", "MSFT", "AMD"])
            self.assertEqual(runtime_config.watchlist.etfs, ["QQQ", "SPY", "SMH"])
            self.assertEqual(
                runtime_config.watchlist.stock_groups,
                {
                    "core_growth": ["NVDA", "MSFT"],
                    "semis": ["AMD", "NVDA"],
                },
            )
            self.assertEqual(
                runtime_config.watchlist.etf_groups,
                {
                    "broad_market": ["QQQ", "SPY"],
                    "sectors": ["SMH"],
                },
            )

    def test_runtime_config_builds_symbol_theme_map_from_grouped_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stock_groups": {
                                "core_platforms": ["nvda", "msft"],
                                "semiconductors_and_ai": ["nvda", "mu"],
                            },
                            "etf_groups": {
                                "core_index": ["qqq"],
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            runtime_config = AgentRuntimeConfig.load(config_path)
            self.assertEqual(
                runtime_config.watchlist.symbol_theme_map(),
                {
                    "NVDA": ["core_platforms", "semiconductors_and_ai"],
                    "MSFT": ["core_platforms"],
                    "MU": ["semiconductors_and_ai"],
                    "QQQ": ["core_index"],
                },
            )

    def test_write_default_config_creates_expected_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            write_default_config(config_path)
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(payload, load_default_template_payload())

    def test_replace_watchlist_disables_removed_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.replace_watchlist(["NVDA", "MSFT"], ["QQQ"])
            self.assertEqual(store.load_watchlist(), {"NVDA", "MSFT", "QQQ"})
            self.assertEqual(store.load_watchlist("stock"), {"NVDA", "MSFT"})
            self.assertEqual(store.load_watchlist("etf"), {"QQQ"})
            store.replace_watchlist(["AAPL"], [])
            self.assertEqual(store.load_watchlist(), {"AAPL"})

    def test_cli_experiment_overrides_take_precedence(self) -> None:
        settings = Settings().with_strategy_overrides(
            event_score_threshold=60,
            horizons={
                "swing": {"market_score_threshold": 55, "priority_threshold": 75},
                "position": {"market_score_threshold": 55, "priority_threshold": 75},
            },
        )
        args = argparse.Namespace(
            event_score_threshold=66.0,
            swing_market_score_threshold=59.0,
            position_market_score_threshold=61.0,
            swing_priority_threshold=79.0,
            position_priority_threshold=82.0,
        )
        updated = _apply_cli_experiment_overrides(settings, args)
        self.assertEqual(updated.event_score_threshold, 66.0)
        self.assertEqual(updated.horizons["swing"].market_score_threshold, 59.0)
        self.assertEqual(updated.horizons["position"].market_score_threshold, 61.0)
        self.assertEqual(updated.horizons["swing"].priority_threshold, 79.0)
        self.assertEqual(updated.horizons["position"].priority_threshold, 82.0)

    def test_runtime_watchlist_is_resynced_when_config_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = Store(Path(temp_dir) / "agent.db")
            store.initialize()
            store.replace_watchlist(["NVDA", "MSFT"], ["QQQ"])
            runtime_config = AgentRuntimeConfig.load(Path(temp_dir) / "missing-agent.json")
            runtime_config = AgentRuntimeConfig(
                watchlist=WatchlistConfig(stocks=["AAPL"], etfs=["SPY"]),
                sources=runtime_config.sources,
                runtime=runtime_config.runtime,
                notifications=runtime_config.notifications,
                strategy=runtime_config.strategy,
            )

            _sync_watchlist_if_needed(store, Settings(), runtime_config)

            self.assertEqual(store.load_watchlist("stock"), {"AAPL"})
            self.assertEqual(store.load_watchlist("etf"), {"SPY"})

    def test_sec_eligibility_filters_known_non_edgar_symbols(self) -> None:
        self.assertTrue(_is_sec_eligible_symbol("NVDA"))
        self.assertFalse(_is_sec_eligible_symbol("BRK.B"))
        self.assertFalse(_is_sec_eligible_symbol("TCEHY"))
        self.assertFalse(_is_sec_eligible_symbol("LVMHF"))
        self.assertFalse(_is_sec_eligible_symbol("ARM"))
        self.assertFalse(_is_sec_eligible_symbol("NBIS"))

    def test_source_symbol_priority_prefers_runtime_watchlist_order(self) -> None:
        ordered = _prioritize_symbols_for_source(
            ["NBIS", "NVDA", "QQQ"],
            {"QQQ", "AAPL", "NVDA", "NBIS", "SMH"},
        )
        self.assertEqual(ordered, ["NBIS", "NVDA", "QQQ", "AAPL", "SMH"])
        self.assertEqual(_limit_symbols_for_source(ordered, 3), ["NBIS", "NVDA", "QQQ"])

    def test_runtime_window_pause_respects_beijing_weekday_overnight_window(self) -> None:
        runtime_config = AgentRuntimeConfig(runtime_window=RuntimeWindowConfig(
            enabled=True,
            timezone="Asia/Shanghai",
            weekdays=["mon", "tue", "wed", "thu", "fri"],
            start_time="18:00",
            end_time="04:00",
        ))
        friday_evening_utc = datetime(2026, 3, 20, 11, 0, tzinfo=timezone.utc)  # Fri 19:00 CST
        paused, next_run_at = _runtime_window_pause(runtime_config, now=friday_evening_utc)
        self.assertFalse(paused)
        self.assertIsNone(next_run_at)

        saturday_early_utc = datetime(2026, 3, 20, 18, 30, tzinfo=timezone.utc)  # Sat 02:30 CST
        paused, next_run_at = _runtime_window_pause(runtime_config, now=saturday_early_utc)
        self.assertFalse(paused)
        self.assertIsNone(next_run_at)

        saturday_morning_utc = datetime(2026, 3, 20, 22, 30, tzinfo=timezone.utc)  # Sat 06:30 CST
        paused, next_run_at = _runtime_window_pause(runtime_config, now=saturday_morning_utc)
        self.assertTrue(paused)
        self.assertEqual(next_run_at, datetime(2026, 3, 23, 10, 0, tzinfo=timezone.utc))  # Mon 18:00 CST


if __name__ == "__main__":
    unittest.main()
