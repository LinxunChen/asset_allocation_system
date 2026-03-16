from __future__ import annotations

import json
import argparse
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from satellite_agent.config import Settings
from satellite_agent.main import _apply_cli_experiment_overrides
from satellite_agent.runtime_config import AgentRuntimeConfig, load_default_template_payload, write_default_config
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
            self.assertTrue(settings.use_sec_filings_source)
            self.assertTrue(settings.use_google_news_source)
            self.assertEqual(settings.poll_seconds, 120)
            self.assertEqual(settings.cross_source_dedup_hours, 6)
            self.assertEqual(settings.feishu_webhook, "https://example.feishu.cn/webhook/test")
            self.assertTrue(settings.dry_run)
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


if __name__ == "__main__":
    unittest.main()
