from __future__ import annotations

import os
from copy import deepcopy
from dataclasses import replace
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


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
class Settings:
    database_path: Path = Path("./data/satellite_agent.db")
    config_path: Path = Path("./config/agent.json")
    poll_seconds: int = 60
    max_events_per_run: int = 60
    max_events_per_symbol_per_run: int = 2
    max_alerts_per_run: int = 4
    max_alerts_per_symbol_per_run: int = 1
    normal_alert_min_final_score: float = 73.0
    feishu_webhook: str = ""
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    openai_base_url: str = "https://api.openai.com/v1/chat/completions"
    dry_run: bool = False
    sec_user_agent: str = "satellite-agent research contact@example.com"
    use_sec_filings_source: bool = False
    use_google_news_source: bool = False
    use_google_research_source: bool = False
    cross_source_dedup_hours: int = 12
    event_score_threshold: float = 60.0
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
        db_path = Path(os.getenv("SATELLITE_DB_PATH", "./data/satellite_agent.db"))
        config_path = Path(os.getenv("SATELLITE_CONFIG_PATH", "./config/agent.json"))
        poll_seconds = int(os.getenv("SATELLITE_POLL_SECONDS", "60"))
        return cls(
            database_path=db_path,
            config_path=config_path,
            poll_seconds=poll_seconds,
            max_events_per_run=int(os.getenv("SATELLITE_MAX_EVENTS_PER_RUN", "60")),
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
            feishu_webhook=os.getenv("SATELLITE_FEISHU_WEBHOOK", ""),
            openai_api_key=os.getenv("SATELLITE_OPENAI_API_KEY", ""),
            openai_model=os.getenv("SATELLITE_OPENAI_MODEL", "gpt-4o-mini"),
            openai_base_url=os.getenv(
                "SATELLITE_OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions"
            ),
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
            cross_source_dedup_hours=int(os.getenv("SATELLITE_CROSS_SOURCE_DEDUP_HOURS", "12")),
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
        horizons: Dict[str, Dict[str, Any]] | None = None,
    ) -> "Settings":
        next_settings = self
        if event_score_threshold is not None:
            next_settings = replace(next_settings, event_score_threshold=event_score_threshold)
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
            "max_events_per_symbol_per_run": self.max_events_per_symbol_per_run,
            "max_alerts_per_run": self.max_alerts_per_run,
            "max_alerts_per_symbol_per_run": self.max_alerts_per_symbol_per_run,
            "normal_alert_min_final_score": self.normal_alert_min_final_score,
            "feishu_webhook_configured": bool(self.feishu_webhook),
            "openai_model": self.openai_model,
            "openai_base_url": self.openai_base_url,
            "openai_api_key_configured": bool(self.openai_api_key),
            "dry_run": self.dry_run,
            "sec_user_agent": self.sec_user_agent,
            "use_sec_filings_source": self.use_sec_filings_source,
            "use_google_news_source": self.use_google_news_source,
            "use_google_research_source": self.use_google_research_source,
            "cross_source_dedup_hours": self.cross_source_dedup_hours,
            "event_score_threshold": self.event_score_threshold,
            "horizons": horizons_record,
        }
