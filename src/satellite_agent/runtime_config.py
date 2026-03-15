from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import Settings


@dataclass(frozen=True)
class WatchlistConfig:
    stocks: list[str] = field(default_factory=list)
    etfs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SourceConfig:
    sec_filings: bool | None = None
    google_news: bool | None = None
    google_research: bool | None = None


@dataclass(frozen=True)
class RuntimeOverrides:
    poll_seconds: int | None = None
    cross_source_dedup_hours: int | None = None


@dataclass(frozen=True)
class HorizonStrategyOverrides:
    ttl_days: int | None = None
    market_score_threshold: float | None = None
    priority_threshold: float | None = None
    rsi_floor: float | None = None
    rsi_ceiling: float | None = None
    atr_percent_ceiling: float | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "ttl_days": self.ttl_days,
            "market_score_threshold": self.market_score_threshold,
            "priority_threshold": self.priority_threshold,
            "rsi_floor": self.rsi_floor,
            "rsi_ceiling": self.rsi_ceiling,
            "atr_percent_ceiling": self.atr_percent_ceiling,
        }

    def as_overrides(self) -> dict[str, Any]:
        return {key: value for key, value in self.to_record().items() if value is not None}


@dataclass(frozen=True)
class StrategyConfig:
    event_score_threshold: float | None = None
    swing: HorizonStrategyOverrides = field(default_factory=HorizonStrategyOverrides)
    position: HorizonStrategyOverrides = field(default_factory=HorizonStrategyOverrides)

    def to_record(self) -> dict[str, Any]:
        return {
            "event_score_threshold": self.event_score_threshold,
            "horizons": {
                "swing": self.swing.to_record(),
                "position": self.position.to_record(),
            },
        }


@dataclass(frozen=True)
class AgentRuntimeConfig:
    watchlist: WatchlistConfig = field(default_factory=WatchlistConfig)
    sources: SourceConfig = field(default_factory=SourceConfig)
    runtime: RuntimeOverrides = field(default_factory=RuntimeOverrides)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)

    @classmethod
    def load(cls, path: Path) -> "AgentRuntimeConfig":
        if not path.exists():
            return cls()
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls(
            watchlist=WatchlistConfig(
                stocks=_load_watchlist_symbols(payload.get("watchlist", {}), flat_key="stocks", grouped_key="stock_groups"),
                etfs=_load_watchlist_symbols(payload.get("watchlist", {}), flat_key="etfs", grouped_key="etf_groups"),
            ),
            sources=SourceConfig(
                sec_filings=_optional_bool(payload.get("sources", {}).get("sec_filings")),
                google_news=_optional_bool(payload.get("sources", {}).get("google_news")),
                google_research=_optional_bool(payload.get("sources", {}).get("google_research")),
            ),
            runtime=RuntimeOverrides(
                poll_seconds=_optional_int(payload.get("runtime", {}).get("poll_seconds")),
                cross_source_dedup_hours=_optional_int(
                    payload.get("runtime", {}).get("cross_source_dedup_hours")
                ),
            ),
            strategy=StrategyConfig(
                event_score_threshold=_optional_float(payload.get("strategy", {}).get("event_score_threshold")),
                swing=_load_horizon_strategy(payload.get("strategy", {}).get("horizons", {}).get("swing", {})),
                position=_load_horizon_strategy(
                    payload.get("strategy", {}).get("horizons", {}).get("position", {})
                ),
            ),
        )

    def apply(self, settings: Settings) -> Settings:
        overrides: dict[str, Any] = {}
        if self.sources.sec_filings is not None:
            overrides["use_sec_filings_source"] = self.sources.sec_filings
        if self.sources.google_news is not None:
            overrides["use_google_news_source"] = self.sources.google_news
        if self.sources.google_research is not None:
            overrides["use_google_research_source"] = self.sources.google_research
        if self.runtime.poll_seconds is not None:
            overrides["poll_seconds"] = self.runtime.poll_seconds
        if self.runtime.cross_source_dedup_hours is not None:
            overrides["cross_source_dedup_hours"] = self.runtime.cross_source_dedup_hours
        next_settings = settings.with_overrides(**overrides) if overrides else settings
        next_settings = next_settings.with_strategy_overrides(
            event_score_threshold=self.strategy.event_score_threshold,
            horizons={
                "swing": self.strategy.swing.as_overrides(),
                "position": self.strategy.position.as_overrides(),
            },
        )
        return next_settings

    def has_watchlist(self) -> bool:
        return bool(self.watchlist.stocks or self.watchlist.etfs)

    def to_record(self) -> dict[str, Any]:
        return {
            "watchlist": {
                "stocks": list(self.watchlist.stocks),
                "etfs": list(self.watchlist.etfs),
            },
            "sources": {
                "sec_filings": self.sources.sec_filings,
                "google_news": self.sources.google_news,
                "google_research": self.sources.google_research,
            },
            "runtime": {
                "poll_seconds": self.runtime.poll_seconds,
                "cross_source_dedup_hours": self.runtime.cross_source_dedup_hours,
            },
            "strategy": self.strategy.to_record(),
        }


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_template_path() -> Path:
    return project_root() / "config" / "agent.template.json"


def load_default_template_payload() -> dict[str, Any]:
    template_path = default_template_path()
    if not template_path.exists():
        raise FileNotFoundError(f"Default runtime template not found: {template_path}")
    with template_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_default_template_runtime_config() -> AgentRuntimeConfig:
    return AgentRuntimeConfig.load(default_template_path())


def _normalize_symbols(values: list[Any]) -> list[str]:
    normalized = []
    seen = set()
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _load_watchlist_symbols(payload: dict[str, Any], *, flat_key: str, grouped_key: str) -> list[str]:
    combined: list[Any] = list(payload.get(flat_key, []))
    grouped = payload.get(grouped_key, {})
    if isinstance(grouped, dict):
        for symbols in grouped.values():
            if isinstance(symbols, list):
                combined.extend(symbols)
    return _normalize_symbols(combined)


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _load_horizon_strategy(payload: dict[str, Any]) -> HorizonStrategyOverrides:
    return HorizonStrategyOverrides(
        ttl_days=_optional_int(payload.get("ttl_days")),
        market_score_threshold=_optional_float(payload.get("market_score_threshold")),
        priority_threshold=_optional_float(payload.get("priority_threshold")),
        rsi_floor=_optional_float(payload.get("rsi_floor")),
        rsi_ceiling=_optional_float(payload.get("rsi_ceiling")),
        atr_percent_ceiling=_optional_float(payload.get("atr_percent_ceiling")),
    )


DEFAULT_CONFIG = load_default_template_payload()


def write_default_config(path: Path, force: bool = False) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"Config already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(load_default_template_payload(), handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
