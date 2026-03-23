from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import replace
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

DEFAULT_DB_PATH = Path("./data/satellite_agent/agent.db")
LEGACY_DB_PATH = Path("./data/satellite_agent.db")
DEFAULT_CONFIG_PATH = Path("./config/satellite_agent/agent.json")
LEGACY_CONFIG_PATH = Path("./config/agent.json")
DEFAULT_LOCAL_ENV_PATH = Path("./config/satellite_agent/.env.local")
LEGACY_LOCAL_ENV_PATH = Path("./config/.env.local")


def _resolve_default_path(preferred: Path, legacy: Path) -> Path:
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


def _load_local_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True)
class HorizonSettings:
    name: str
    ttl_days: int
    market_score_threshold: float
    priority_threshold: float
    rsi_floor: float
    rsi_ceiling: float
    atr_percent_ceiling: float


@dataclass(frozen=True)
class EventScoreWeights:
    importance: float = 0.30
    source_credibility: float = 0.25
    novelty: float = 0.20
    theme_relevance: float = 0.15
    sentiment: float = 0.10

    def to_record(self) -> dict[str, float]:
        return {
            "importance": self.importance,
            "source_credibility": self.source_credibility,
            "novelty": self.novelty,
            "theme_relevance": self.theme_relevance,
            "sentiment": self.sentiment,
        }


@dataclass(frozen=True)
class Settings:
    database_path: Path = DEFAULT_DB_PATH
    config_path: Path = DEFAULT_CONFIG_PATH
    poll_seconds: int = 60
    max_events_per_run: int = 60
    max_deep_events_per_run: int = 32
    max_deep_theme_reservations_per_run: int = 4
    max_events_per_symbol_per_run: int = 2
    max_alerts_per_run: int = 4
    max_alerts_per_symbol_per_run: int = 1
    normal_alert_min_final_score: float = 73.0
    event_only_alert_min_event_score: float = 78.0
    prewatch_min_score: float = 60.0
    max_prewatch_candidates_per_run: int = 8
    max_prewatch_scan_symbols_per_run: int = 24
    prewatch_alert_min_score: float = 78.0
    max_prewatch_alerts_per_run: int = 2
    prewatch_alert_cooldown_minutes: int = 240
    prewatch_alert_repeat_window_minutes: int = 720
    prewatch_alert_repeat_min_score_delta: float = 4.0
    prewatch_failure_cooldown_minutes: int = 90
    prewatch_concurrency: int = 8
    prewatch_theme_relaxed_margin: float = 6.0
    prewatch_theme_memory_window_hours: int = 36
    prewatch_theme_memory_bonus: float = 3.0
    prewatch_theme_memory_scan_bonus: float = 4.0
    prewatch_theme_memory_min_heat_score: float = 4.0
    prewatch_event_trigger_min_event_score: float = 76.0
    prewatch_event_min_score: float = 54.0
    prewatch_event_bonus_cap: float = 10.0
    max_event_prewatch_symbols_per_theme_per_run: int = 2
    prewatch_promotion_window_hours: int = 72
    prewatch_confirmation_min_event_score: float = 74.0
    prewatch_confirmation_bonus: float = 2.0
    feishu_webhook: str = ""
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_base_url: str = "https://api.openai.com/v1/chat/completions"
    llm_max_requests_per_run: int = 6
    llm_max_requests_per_day: int = 2000
    use_llm_event_extraction: bool = False
    use_llm_narration: bool = True
    use_llm_ranking_assist: bool = True
    use_macro_risk_overlay: bool = True
    dry_run: bool = False
    sec_user_agent: str = "satellite-agent research contact@example.com"
    use_sec_filings_source: bool = False
    use_google_news_source: bool = False
    use_google_research_source: bool = False
    max_google_feed_symbols_per_run: int = 24
    cross_source_dedup_hours: int = 12
    source_health_cache_seconds: int = 300
    event_score_threshold: float = 60.0
    event_score_weights: EventScoreWeights = field(default_factory=EventScoreWeights)
    horizons: Dict[str, HorizonSettings] = field(
        default_factory=lambda: {
            "swing": HorizonSettings(
                name="swing",
                ttl_days=3,
                market_score_threshold=55.0,
                priority_threshold=75.0,
                rsi_floor=45.0,
                rsi_ceiling=68.0,
                atr_percent_ceiling=8.0,
            ),
            "position": HorizonSettings(
                name="position",
                ttl_days=20,
                market_score_threshold=55.0,
                priority_threshold=75.0,
                rsi_floor=50.0,
                rsi_ceiling=65.0,
                atr_percent_ceiling=10.0,
            ),
        }
    )

    @classmethod
    def from_env(cls) -> "Settings":
        db_path = Path(os.getenv("SATELLITE_DB_PATH", str(_resolve_default_path(DEFAULT_DB_PATH, LEGACY_DB_PATH))))
        config_path = Path(
            os.getenv("SATELLITE_CONFIG_PATH", str(_resolve_default_path(DEFAULT_CONFIG_PATH, LEGACY_CONFIG_PATH)))
        )
        _load_local_env_file(config_path.parent / ".env.local")
        _load_local_env_file(_resolve_default_path(DEFAULT_LOCAL_ENV_PATH, LEGACY_LOCAL_ENV_PATH))
        poll_seconds = int(os.getenv("SATELLITE_POLL_SECONDS", "60"))
        return cls(
            database_path=db_path,
            config_path=config_path,
            poll_seconds=poll_seconds,
            max_events_per_run=int(os.getenv("SATELLITE_MAX_EVENTS_PER_RUN", "60")),
            max_deep_events_per_run=int(os.getenv("SATELLITE_MAX_DEEP_EVENTS_PER_RUN", "32")),
            max_deep_theme_reservations_per_run=int(
                os.getenv("SATELLITE_MAX_DEEP_THEME_RESERVATIONS_PER_RUN", "4")
            ),
            max_events_per_symbol_per_run=int(
                os.getenv("SATELLITE_MAX_EVENTS_PER_SYMBOL_PER_RUN", "2")
            ),
            max_alerts_per_run=int(os.getenv("SATELLITE_MAX_ALERTS_PER_RUN", "4")),
            max_alerts_per_symbol_per_run=int(
                os.getenv("SATELLITE_MAX_ALERTS_PER_SYMBOL_PER_RUN", "1")
            ),
            normal_alert_min_final_score=float(
                os.getenv("SATELLITE_NORMAL_ALERT_MIN_FINAL_SCORE", "73")
            ),
            event_only_alert_min_event_score=float(
                os.getenv("SATELLITE_EVENT_ONLY_ALERT_MIN_EVENT_SCORE", "78")
            ),
            prewatch_min_score=float(os.getenv("SATELLITE_PREWATCH_MIN_SCORE", "60")),
            max_prewatch_candidates_per_run=int(
                os.getenv("SATELLITE_MAX_PREWATCH_CANDIDATES_PER_RUN", "8")
            ),
            max_prewatch_scan_symbols_per_run=int(
                os.getenv("SATELLITE_MAX_PREWATCH_SCAN_SYMBOLS_PER_RUN", "24")
            ),
            prewatch_alert_min_score=float(
                os.getenv("SATELLITE_PREWATCH_ALERT_MIN_SCORE", "78")
            ),
            max_prewatch_alerts_per_run=int(
                os.getenv("SATELLITE_MAX_PREWATCH_ALERTS_PER_RUN", "2")
            ),
            prewatch_alert_cooldown_minutes=int(
                os.getenv("SATELLITE_PREWATCH_ALERT_COOLDOWN_MINUTES", "240")
            ),
            prewatch_alert_repeat_window_minutes=int(
                os.getenv("SATELLITE_PREWATCH_ALERT_REPEAT_WINDOW_MINUTES", "720")
            ),
            prewatch_alert_repeat_min_score_delta=float(
                os.getenv("SATELLITE_PREWATCH_ALERT_REPEAT_MIN_SCORE_DELTA", "4")
            ),
            prewatch_failure_cooldown_minutes=int(
                os.getenv("SATELLITE_PREWATCH_FAILURE_COOLDOWN_MINUTES", "90")
            ),
            prewatch_concurrency=int(os.getenv("SATELLITE_PREWATCH_CONCURRENCY", "8")),
            prewatch_theme_relaxed_margin=float(
                os.getenv("SATELLITE_PREWATCH_THEME_RELAXED_MARGIN", "6")
            ),
            prewatch_theme_memory_window_hours=int(
                os.getenv("SATELLITE_PREWATCH_THEME_MEMORY_WINDOW_HOURS", "36")
            ),
            prewatch_theme_memory_bonus=float(
                os.getenv("SATELLITE_PREWATCH_THEME_MEMORY_BONUS", "3")
            ),
            prewatch_theme_memory_scan_bonus=float(
                os.getenv("SATELLITE_PREWATCH_THEME_MEMORY_SCAN_BONUS", "4")
            ),
            prewatch_theme_memory_min_heat_score=float(
                os.getenv("SATELLITE_PREWATCH_THEME_MEMORY_MIN_HEAT_SCORE", "4")
            ),
            prewatch_event_trigger_min_event_score=float(
                os.getenv("SATELLITE_PREWATCH_EVENT_TRIGGER_MIN_EVENT_SCORE", "76")
            ),
            prewatch_event_min_score=float(
                os.getenv("SATELLITE_PREWATCH_EVENT_MIN_SCORE", "54")
            ),
            prewatch_event_bonus_cap=float(
                os.getenv("SATELLITE_PREWATCH_EVENT_BONUS_CAP", "10")
            ),
            max_event_prewatch_symbols_per_theme_per_run=int(
                os.getenv("SATELLITE_MAX_EVENT_PREWATCH_SYMBOLS_PER_THEME_PER_RUN", "2")
            ),
            prewatch_promotion_window_hours=int(
                os.getenv("SATELLITE_PREWATCH_PROMOTION_WINDOW_HOURS", "72")
            ),
            prewatch_confirmation_min_event_score=float(
                os.getenv("SATELLITE_PREWATCH_CONFIRMATION_MIN_EVENT_SCORE", "74")
            ),
            prewatch_confirmation_bonus=float(
                os.getenv("SATELLITE_PREWATCH_CONFIRMATION_BONUS", "2")
            ),
            feishu_webhook=os.getenv("SATELLITE_FEISHU_WEBHOOK", ""),
            openai_api_key=os.getenv("SATELLITE_OPENAI_API_KEY", ""),
            openai_model=os.getenv("SATELLITE_OPENAI_MODEL", "gpt-4o-mini"),
            openai_base_url=os.getenv(
                "SATELLITE_OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions"
            ),
            llm_max_requests_per_run=int(
                os.getenv("SATELLITE_LLM_MAX_REQUESTS_PER_RUN", "6")
            ),
            llm_max_requests_per_day=int(
                os.getenv("SATELLITE_LLM_MAX_REQUESTS_PER_DAY", "2000")
            ),
            use_llm_event_extraction=os.getenv("SATELLITE_USE_LLM_EVENT_EXTRACTION", "0").lower()
            in {"1", "true", "yes"},
            use_llm_narration=os.getenv("SATELLITE_USE_LLM_NARRATION", "1").lower()
            in {"1", "true", "yes"},
            use_llm_ranking_assist=os.getenv("SATELLITE_USE_LLM_RANKING_ASSIST", "1").lower()
            in {"1", "true", "yes"},
            use_macro_risk_overlay=os.getenv("SATELLITE_USE_MACRO_RISK_OVERLAY", "1").lower()
            in {"1", "true", "yes"},
            dry_run=os.getenv("SATELLITE_DRY_RUN", "0").lower() in {"1", "true", "yes"},
            sec_user_agent=os.getenv(
                "SATELLITE_SEC_USER_AGENT", "satellite-agent research contact@example.com"
            ),
            use_sec_filings_source=os.getenv("SATELLITE_USE_SEC_FILINGS_SOURCE", "0").lower()
            in {"1", "true", "yes"},
            use_google_news_source=os.getenv("SATELLITE_USE_GOOGLE_NEWS_SOURCE", "0").lower()
            in {"1", "true", "yes"},
            use_google_research_source=os.getenv(
                "SATELLITE_USE_GOOGLE_RESEARCH_SOURCE", "0"
            ).lower()
            in {"1", "true", "yes"},
            max_google_feed_symbols_per_run=int(
                os.getenv("SATELLITE_MAX_GOOGLE_FEED_SYMBOLS_PER_RUN", "24")
            ),
            cross_source_dedup_hours=int(os.getenv("SATELLITE_CROSS_SOURCE_DEDUP_HOURS", "12")),
            source_health_cache_seconds=int(
                os.getenv("SATELLITE_SOURCE_HEALTH_CACHE_SECONDS", "300")
            ),
        )

    def with_overrides(self, **kwargs) -> "Settings":
        return replace(self, **kwargs)

    def with_horizon_overrides(self, horizon_name: str, **kwargs) -> "Settings":
        horizon = self.horizons[horizon_name]
        next_horizons = dict(self.horizons)
        next_horizons[horizon_name] = replace(
            horizon,
            **{key: value for key, value in kwargs.items() if value is not None},
        )
        return replace(self, horizons=next_horizons)

    def with_strategy_overrides(
        self,
        *,
        event_score_threshold: float | None = None,
        event_score_weights: dict[str, Any] | None = None,
        horizons: Dict[str, Dict[str, Any]] | None = None,
    ) -> "Settings":
        next_settings = self
        if event_score_threshold is not None:
            next_settings = replace(next_settings, event_score_threshold=event_score_threshold)
        if event_score_weights:
            current_weights = next_settings.event_score_weights.to_record()
            current_weights.update(
                {
                    key: float(value)
                    for key, value in event_score_weights.items()
                    if value is not None and key in current_weights
                }
            )
            next_settings = replace(
                next_settings,
                event_score_weights=EventScoreWeights(**current_weights),
            )
        for horizon_name, overrides in (horizons or {}).items():
            next_settings = next_settings.with_horizon_overrides(horizon_name, **overrides)
        return next_settings

    def to_record(self) -> dict[str, Any]:
        horizons_record = deepcopy(
            {
                name: {
                    "ttl_days": horizon.ttl_days,
                    "market_score_threshold": horizon.market_score_threshold,
                    "priority_threshold": horizon.priority_threshold,
                    "rsi_floor": horizon.rsi_floor,
                    "rsi_ceiling": horizon.rsi_ceiling,
                    "atr_percent_ceiling": horizon.atr_percent_ceiling,
                }
                for name, horizon in self.horizons.items()
            }
        )
        return {
            "database_path": str(self.database_path),
            "config_path": str(self.config_path),
            "poll_seconds": self.poll_seconds,
            "max_events_per_run": self.max_events_per_run,
            "max_deep_events_per_run": self.max_deep_events_per_run,
            "max_deep_theme_reservations_per_run": self.max_deep_theme_reservations_per_run,
            "max_events_per_symbol_per_run": self.max_events_per_symbol_per_run,
            "max_alerts_per_run": self.max_alerts_per_run,
            "max_alerts_per_symbol_per_run": self.max_alerts_per_symbol_per_run,
            "normal_alert_min_final_score": self.normal_alert_min_final_score,
            "event_only_alert_min_event_score": self.event_only_alert_min_event_score,
            "prewatch_min_score": self.prewatch_min_score,
            "max_prewatch_candidates_per_run": self.max_prewatch_candidates_per_run,
            "max_prewatch_scan_symbols_per_run": self.max_prewatch_scan_symbols_per_run,
            "prewatch_alert_min_score": self.prewatch_alert_min_score,
            "max_prewatch_alerts_per_run": self.max_prewatch_alerts_per_run,
            "prewatch_alert_cooldown_minutes": self.prewatch_alert_cooldown_minutes,
            "prewatch_alert_repeat_window_minutes": self.prewatch_alert_repeat_window_minutes,
            "prewatch_alert_repeat_min_score_delta": self.prewatch_alert_repeat_min_score_delta,
            "prewatch_failure_cooldown_minutes": self.prewatch_failure_cooldown_minutes,
            "prewatch_concurrency": self.prewatch_concurrency,
            "prewatch_theme_relaxed_margin": self.prewatch_theme_relaxed_margin,
            "prewatch_theme_memory_window_hours": self.prewatch_theme_memory_window_hours,
            "prewatch_theme_memory_bonus": self.prewatch_theme_memory_bonus,
            "prewatch_theme_memory_scan_bonus": self.prewatch_theme_memory_scan_bonus,
            "prewatch_theme_memory_min_heat_score": self.prewatch_theme_memory_min_heat_score,
            "prewatch_event_trigger_min_event_score": self.prewatch_event_trigger_min_event_score,
            "prewatch_event_min_score": self.prewatch_event_min_score,
            "prewatch_event_bonus_cap": self.prewatch_event_bonus_cap,
            "max_event_prewatch_symbols_per_theme_per_run": self.max_event_prewatch_symbols_per_theme_per_run,
            "prewatch_promotion_window_hours": self.prewatch_promotion_window_hours,
            "prewatch_confirmation_min_event_score": self.prewatch_confirmation_min_event_score,
            "prewatch_confirmation_bonus": self.prewatch_confirmation_bonus,
            "feishu_webhook_configured": bool(self.feishu_webhook),
            "openai_model": self.openai_model,
            "openai_base_url": self.openai_base_url,
            "openai_api_key_configured": bool(self.openai_api_key),
            "llm_max_requests_per_run": self.llm_max_requests_per_run,
            "llm_max_requests_per_day": self.llm_max_requests_per_day,
            "use_llm_event_extraction": self.use_llm_event_extraction,
            "use_llm_narration": self.use_llm_narration,
            "use_llm_ranking_assist": self.use_llm_ranking_assist,
            "use_macro_risk_overlay": self.use_macro_risk_overlay,
            "dry_run": self.dry_run,
            "sec_user_agent": self.sec_user_agent,
            "use_sec_filings_source": self.use_sec_filings_source,
            "use_google_news_source": self.use_google_news_source,
            "use_google_research_source": self.use_google_research_source,
            "max_google_feed_symbols_per_run": self.max_google_feed_symbols_per_run,
            "cross_source_dedup_hours": self.cross_source_dedup_hours,
            "source_health_cache_seconds": self.source_health_cache_seconds,
            "event_score_threshold": self.event_score_threshold,
            "event_score_weights": self.event_score_weights.to_record(),
            "horizons": horizons_record,
        }
