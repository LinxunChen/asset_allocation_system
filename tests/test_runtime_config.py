from __future__ import annotations

import json
import argparse
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

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
    build_watchlist_config_review_payload,
    format_watchlist_config_review_payload,
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
    def test_settings_from_env_loads_local_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_dir = root / "config" / "satellite_agent"
            config_dir.mkdir(parents=True)
            (config_dir / ".env.local").write_text(
                "\n".join(
                    [
                        "SATELLITE_OPENAI_API_KEY=test-local-key",
                        "SATELLITE_OPENAI_MODEL=Qwen/Qwen3.5-35B-A3B",
                        "SATELLITE_OPENAI_BASE_URL=https://api.siliconflow.com/v1/chat/completions",
                        "SATELLITE_USE_LLM_NARRATION=1",
                        "SATELLITE_USE_LLM_RANKING_ASSIST=0",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            config_path = config_dir / "agent.json"
            config_path.write_text("{}", encoding="utf-8")
            with mock.patch.dict(
                "os.environ",
                {"SATELLITE_CONFIG_PATH": str(config_path)},
                clear=True,
            ):
                settings = Settings.from_env()
            self.assertEqual(settings.openai_api_key, "test-local-key")
            self.assertEqual(settings.openai_model, "Qwen/Qwen3.5-35B-A3B")
            self.assertEqual(settings.openai_base_url, "https://api.siliconflow.com/v1/chat/completions")
            self.assertTrue(settings.use_llm_narration)
            self.assertFalse(settings.use_llm_ranking_assist)

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
                        "llm": {
                            "model": "Qwen/Qwen3.5-35B-A3B",
                            "base_url": "https://api.siliconflow.com/v1/chat/completions",
                            "use_narration": True,
                            "use_ranking_assist": False,
                        },
                        "strategy": {
                            "event_score_threshold": 65,
                            "use_llm_event_extraction": False,
                            "event_score_weights": {
                                "importance": 0.35,
                                "source_credibility": 0.20,
                                "novelty": 0.15,
                                "theme_relevance": 0.20,
                                "sentiment": 0.10
                            },
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
            self.assertEqual(settings.openai_model, "Qwen/Qwen3.5-35B-A3B")
            self.assertEqual(settings.openai_base_url, "https://api.siliconflow.com/v1/chat/completions")
            self.assertTrue(settings.use_llm_narration)
            self.assertFalse(settings.use_llm_ranking_assist)
            self.assertTrue(runtime_config.runtime_window.enabled)
            self.assertEqual(runtime_config.runtime_window.timezone, "Asia/Shanghai")
            self.assertEqual(runtime_config.runtime_window.weekdays, ["mon", "tue", "wed", "thu", "fri"])
            self.assertEqual(runtime_config.runtime_window.start_time, "18:00")
            self.assertEqual(runtime_config.runtime_window.end_time, "04:00")
            self.assertEqual(settings.event_score_threshold, 65)
            self.assertFalse(settings.use_llm_event_extraction)
            self.assertEqual(settings.event_score_weights.importance, 0.35)
            self.assertEqual(settings.event_score_weights.source_credibility, 0.20)
            self.assertEqual(settings.event_score_weights.theme_relevance, 0.20)
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

    def test_runtime_config_builds_symbol_theme_map_from_defaults_when_no_groups_or_themes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stock_items": {
                                "NVDA": {"symbol": "NVDA", "name": "NVIDIA"},
                                "TSLA": {"symbol": "TSLA", "name": "Tesla"},
                                "RKLB": {"symbol": "RKLB", "name": "Rocket Lab"},
                                "NBIS": {"symbol": "NBIS", "name": "Nebius Group"},
                            },
                            "etf_items": {
                                "SMH": {"symbol": "SMH", "name": "VanEck Semiconductor ETF"},
                                "BBJP": {"symbol": "BBJP", "name": "JPMorgan BetaBuilders Japan ETF"},
                                "EMXC": {"symbol": "EMXC", "name": "iShares Emerging Markets ex China ETF"},
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            runtime_config = AgentRuntimeConfig.load(config_path)
            self.assertEqual(runtime_config.watchlist.stocks, ["NVDA", "TSLA", "RKLB", "NBIS"])
            self.assertEqual(runtime_config.watchlist.etfs, ["SMH", "BBJP", "EMXC"])
            self.assertEqual(
                runtime_config.watchlist.symbol_theme_map(),
                {
                    "NVDA": ["semiconductors_and_ai"],
                    "TSLA": ["automotive_and_mobility"],
                    "RKLB": ["space_and_defense"],
                    "NBIS": ["data_center"],
                    "SMH": ["semiconductors_and_ai"],
                    "BBJP": ["non_us_markets"],
                    "EMXC": ["non_us_markets"],
                },
            )
            self.assertEqual(
                runtime_config.watchlist.theme_display_name_map(),
                {
                    "automotive_and_mobility": "电动车与智能出行",
                    "data_center": "数据中心基建与算力网络",
                    "semiconductors_and_ai": "AI芯片与半导体设备",
                    "space_and_defense": "国防军工与航空航天",
                    "non_us_markets": "非美市场",
                },
            )

    def test_runtime_config_loads_named_items_and_independent_themes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stock_groups": {
                                "core_platforms": ["nvda"],
                            },
                            "stock_items": {
                                "NVDA": {"symbol": "NVDA", "name": "NVIDIA"},
                                "MSFT": {"symbol": "MSFT", "name": "Microsoft", "enabled": False},
                            },
                            "etf_items": {
                                "SMH": {"symbol": "SMH", "name": "VanEck Semiconductor ETF"},
                            },
                            "themes": [
                                {
                                    "theme_id": "semiconductors_and_ai",
                                    "display_name": "半导体与AI",
                                    "symbols": ["NVDA", "MSFT"],
                                    "etfs": ["SMH"],
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            runtime_config = AgentRuntimeConfig.load(config_path)
            self.assertEqual(runtime_config.watchlist.display_name_for("NVDA"), "NVIDIA")
            self.assertEqual(runtime_config.watchlist.display_name_for("SMH"), "VanEck Semiconductor ETF")
            self.assertEqual(runtime_config.watchlist.stocks, ["NVDA"])
            self.assertEqual(
                runtime_config.watchlist.symbol_theme_map(),
                {
                    "NVDA": ["semiconductors_and_ai"],
                    "SMH": ["semiconductors_and_ai"],
                },
            )
            self.assertEqual(
                runtime_config.watchlist.theme_display_name_map(),
                {"semiconductors_and_ai": "半导体与AI"},
            )

    def test_runtime_config_excludes_disabled_symbols_from_grouped_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stock_groups": {
                                "core_platforms": ["nvda", "msft", "aapl"],
                            },
                            "stock_items": {
                                "NVDA": {"symbol": "NVDA", "name": "NVIDIA"},
                                "MSFT": {"symbol": "MSFT", "name": "Microsoft", "enabled": False},
                                "AAPL": {"symbol": "AAPL", "name": "Apple"},
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            runtime_config = AgentRuntimeConfig.load(config_path)

            self.assertEqual(runtime_config.watchlist.stocks, ["NVDA", "AAPL"])
            self.assertEqual(
                runtime_config.watchlist.stock_groups,
                {"core_platforms": ["NVDA", "AAPL"]},
            )

    def test_write_default_config_creates_expected_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            write_default_config(config_path)
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(payload, load_default_template_payload())

    def test_watchlist_config_review_flags_missing_names_and_unthemed_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stocks": ["NVDA", "TSLA"],
                            "etfs": ["SMH"],
                            "stock_items": {
                                "NVDA": {"symbol": "NVDA", "name": "NVIDIA"},
                                "TSLA": {"symbol": "TSLA"},
                            },
                            "etf_items": {
                                "SMH": {"symbol": "SMH", "name": "VanEck Semiconductor ETF"},
                            },
                            "themes": [
                                {
                                    "theme_id": "semiconductors_and_ai",
                                    "display_name": "半导体与AI",
                                    "symbols": ["NVDA", "AMD"],
                                    "etfs": ["SMH"],
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            runtime_config = AgentRuntimeConfig.load(config_path)
            payload = build_watchlist_config_review_payload(runtime_config, config_path=config_path)
            report_text = format_watchlist_config_review_payload(payload)

            self.assertEqual(payload["missing_display_names"], ["TSLA"])
            self.assertEqual(payload["unthemed_symbols"], ["TSLA"])
            self.assertEqual(payload["excluded_symbols"], [])
            self.assertEqual(
                payload["themes_with_off_watchlist_members"][0]["off_watchlist_members"],
                ["AMD"],
            )
            self.assertIn("缺少展示名称的标的： TSLA", report_text)
            self.assertIn("未挂题材的活跃标的： TSLA", report_text)
            self.assertIn("题材中挂到观察池外的成员：", report_text)

    def test_watchlist_config_review_flags_formal_watchlist_exclusions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "agent.json"
            config_path.write_text(
                json.dumps(
                    {
                        "watchlist": {
                            "stocks": ["NVDA"],
                            "etfs": ["QQQ", "SMH"],
                            "stock_items": {
                                "NVDA": {"symbol": "NVDA", "name": "NVIDIA"},
                            },
                            "etf_items": {
                                "QQQ": {"symbol": "QQQ", "name": "Invesco QQQ Trust"},
                                "SMH": {"symbol": "SMH", "name": "VanEck Semiconductor ETF"},
                            },
                            "themes": [
                                {
                                    "theme_id": "semiconductors_and_ai",
                                    "display_name": "半导体与AI",
                                    "symbols": ["NVDA"],
                                    "etfs": ["SMH"],
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )

            runtime_config = AgentRuntimeConfig.load(config_path)
            payload = build_watchlist_config_review_payload(runtime_config, config_path=config_path)

            self.assertEqual(payload["excluded_symbols"], ["QQQ"])

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
