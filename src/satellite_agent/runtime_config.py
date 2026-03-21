from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import Settings


@dataclass(frozen=True)
class WatchlistConfig:
    stocks: list[str] = field(default_factory=list)
    etfs: list[str] = field(default_factory=list)
    stock_groups: dict[str, list[str]] = field(default_factory=dict)
    etf_groups: dict[str, list[str]] = field(default_factory=dict)

    def symbol_theme_map(self) -> dict[str, list[str]]:
        mapping: dict[str, list[str]] = {}
        for groups in (self.stock_groups, self.etf_groups):
            for group_name, symbols in groups.items():
                for symbol in symbols:
                    bucket = mapping.setdefault(symbol, [])
                    if group_name not in bucket:
                        bucket.append(group_name)
        return mapping


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
class RuntimeWindowConfig:
    enabled: bool | None = None
    timezone: str | None = None
    weekdays: list[str] = field(default_factory=list)
    start_time: str | None = None
    end_time: str | None = None

    def is_configured(self) -> bool:
        return (
            self.enabled is not None
            or bool(self.weekdays)
            or self.start_time is not None
            or self.end_time is not None
            or self.timezone is not None
        )

    def resolved_timezone(self) -> ZoneInfo:
        return ZoneInfo((self.timezone or "Asia/Shanghai").strip() or "Asia/Shanghai")

    def normalized_weekdays(self) -> list[int]:
        return _normalize_weekdays(self.weekdays)

    def start_end_minutes(self) -> tuple[int, int]:
        start = _parse_clock_time(self.start_time or "18:00")
        end = _parse_clock_time(self.end_time or "04:00")
        return start, end

    def to_record(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "timezone": self.timezone,
            "weekdays": list(self.weekdays),
            "start_time": self.start_time,
            "end_time": self.end_time,
        }

    def is_active_at(self, dt: datetime) -> bool:
        if self.enabled is False:
            return True
        local_dt = dt.astimezone(self.resolved_timezone())
        weekdays = self.normalized_weekdays() or [0, 1, 2, 3, 4]
        start_minutes, end_minutes = self.start_end_minutes()
        current_minutes = local_dt.hour * 60 + local_dt.minute
        if start_minutes == end_minutes:
            return local_dt.weekday() in weekdays
        if start_minutes < end_minutes:
            return local_dt.weekday() in weekdays and start_minutes <= current_minutes < end_minutes
        if current_minutes >= start_minutes:
            return local_dt.weekday() in weekdays
        if current_minutes < end_minutes:
            return ((local_dt.weekday() - 1) % 7) in weekdays
        return False

    def next_window_start_after(self, dt: datetime) -> datetime:
        local_dt = dt.astimezone(self.resolved_timezone())
        weekdays = self.normalized_weekdays() or [0, 1, 2, 3, 4]
        start_minutes, end_minutes = self.start_end_minutes()
        current_minutes = local_dt.hour * 60 + local_dt.minute
        if start_minutes == end_minutes:
            start_hour, start_minute = divmod(start_minutes, 60)
            for offset in range(8):
                candidate_date = local_dt.date().fromordinal(local_dt.date().toordinal() + offset)
                candidate = datetime(
                    candidate_date.year,
                    candidate_date.month,
                    candidate_date.day,
                    start_hour,
                    start_minute,
                    tzinfo=local_dt.tzinfo,
                )
                if candidate.weekday() not in weekdays:
                    continue
                if candidate > local_dt:
                    return candidate.astimezone(dt.tzinfo)
        start_hour, start_minute = divmod(start_minutes, 60)
        for offset in range(8):
            candidate_date = local_dt.date().fromordinal(local_dt.date().toordinal() + offset)
            candidate = datetime(
                candidate_date.year,
                candidate_date.month,
                candidate_date.day,
                start_hour,
                start_minute,
                tzinfo=local_dt.tzinfo,
            )
            if candidate.weekday() not in weekdays:
                continue
            if candidate > local_dt:
                return candidate.astimezone(dt.tzinfo)
            if offset > 0:
                return candidate.astimezone(dt.tzinfo)
            if current_minutes >= end_minutes and start_minutes < end_minutes:
                continue
        return local_dt.astimezone(dt.tzinfo)


@dataclass(frozen=True)
class NotificationConfig:
    feishu_webhook: str | None = None
    dry_run: bool | None = None


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
    runtime_window: RuntimeWindowConfig = field(default_factory=RuntimeWindowConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)

    @classmethod
    def load(cls, path: Path) -> "AgentRuntimeConfig":
        if not path.exists():
            return cls()
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        stock_groups = _load_watchlist_groups(payload.get("watchlist", {}), grouped_key="stock_groups")
        etf_groups = _load_watchlist_groups(payload.get("watchlist", {}), grouped_key="etf_groups")
        return cls(
            watchlist=WatchlistConfig(
                stocks=_load_watchlist_symbols(payload.get("watchlist", {}), flat_key="stocks", grouped_key="stock_groups"),
                etfs=_load_watchlist_symbols(payload.get("watchlist", {}), flat_key="etfs", grouped_key="etf_groups"),
                stock_groups=stock_groups,
                etf_groups=etf_groups,
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
            runtime_window=RuntimeWindowConfig(
                enabled=_optional_bool(payload.get("runtime_window", {}).get("enabled")),
                timezone=_optional_str(payload.get("runtime_window", {}).get("timezone")),
                weekdays=_load_weekday_values(payload.get("runtime_window", {}).get("weekdays")),
                start_time=_optional_str(payload.get("runtime_window", {}).get("start_time")),
                end_time=_optional_str(payload.get("runtime_window", {}).get("end_time")),
            ),
            notifications=NotificationConfig(
                feishu_webhook=_optional_str(payload.get("notifications", {}).get("feishu_webhook")),
                dry_run=_optional_bool(payload.get("notifications", {}).get("dry_run")),
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
        if self.notifications.feishu_webhook is not None:
            overrides["feishu_webhook"] = self.notifications.feishu_webhook
        if self.notifications.dry_run is not None:
            overrides["dry_run"] = self.notifications.dry_run
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
                "stock_groups": {
                    group_name: list(symbols)
                    for group_name, symbols in self.watchlist.stock_groups.items()
                },
                "etf_groups": {
                    group_name: list(symbols)
                    for group_name, symbols in self.watchlist.etf_groups.items()
                },
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
            "runtime_window": self.runtime_window.to_record(),
            "notifications": {
                "feishu_webhook_configured": bool(self.notifications.feishu_webhook),
                "dry_run": self.notifications.dry_run,
            },
            "strategy": self.strategy.to_record(),
        }


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_template_path() -> Path:
    preferred = project_root() / "config" / "satellite_agent" / "agent.template.json"
    legacy = project_root() / "config" / "agent.template.json"
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


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


def _load_watchlist_groups(payload: dict[str, Any], *, grouped_key: str) -> dict[str, list[str]]:
    grouped = payload.get(grouped_key, {})
    if not isinstance(grouped, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for group_name, symbols in grouped.items():
        if not isinstance(symbols, list):
            continue
        cleaned = _normalize_symbols(symbols)
        if cleaned:
            normalized[str(group_name)] = cleaned
    return normalized


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


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip()


def _load_weekday_values(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


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


def _parse_clock_time(value: str) -> int:
    raw = (value or "").strip()
    hour_text, sep, minute_text = raw.partition(":")
    if not sep:
        raise ValueError(f"Invalid clock time: {value}")
    hour = int(hour_text)
    minute = int(minute_text)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Invalid clock time: {value}")
    return hour * 60 + minute


def _normalize_weekdays(values: list[str]) -> list[int]:
    mapping = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        key = str(value).strip().lower()
        if key not in mapping:
            continue
        weekday = mapping[key]
        if weekday in seen:
            continue
        seen.add(weekday)
        normalized.append(weekday)
    return normalized
