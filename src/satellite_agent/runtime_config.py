from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import Settings
from .theme_linkage import THEME_DISPLAY_NAMES, build_default_symbol_theme_map


@dataclass(frozen=True)
class WatchlistItem:
    symbol: str
    name: str = ""
    enabled: bool = True

    def to_record(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"symbol": self.symbol}
        if self.name:
            payload["name"] = self.name
        if not self.enabled:
            payload["enabled"] = False
        return payload


@dataclass(frozen=True)
class ThemeConfig:
    theme_id: str
    display_name: str
    symbols: list[str] = field(default_factory=list)
    etfs: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "theme_id": self.theme_id,
            "display_name": self.display_name,
            "symbols": list(self.symbols),
            "etfs": list(self.etfs),
        }


@dataclass(frozen=True)
class WatchlistConfig:
    stocks: list[str] = field(default_factory=list)
    etfs: list[str] = field(default_factory=list)
    stock_groups: dict[str, list[str]] = field(default_factory=dict)
    etf_groups: dict[str, list[str]] = field(default_factory=dict)
    stock_items: dict[str, WatchlistItem] = field(default_factory=dict)
    etf_items: dict[str, WatchlistItem] = field(default_factory=dict)
    themes: list[ThemeConfig] = field(default_factory=list)

    def symbol_theme_map(self) -> dict[str, list[str]]:
        active_symbols = set(self.stocks) | set(self.etfs)
        mapping: dict[str, list[str]] = {}
        if self.themes:
            for theme in self.themes:
                members = list(theme.symbols) + list(theme.etfs)
                for symbol in members:
                    if active_symbols and symbol not in active_symbols:
                        continue
                    bucket = mapping.setdefault(symbol, [])
                    if theme.theme_id not in bucket:
                        bucket.append(theme.theme_id)
            return mapping
        for groups in (self.stock_groups, self.etf_groups):
            for group_name, symbols in groups.items():
                for symbol in symbols:
                    if active_symbols and symbol not in active_symbols:
                        continue
                    bucket = mapping.setdefault(symbol, [])
                    if group_name not in bucket:
                        bucket.append(group_name)
        if mapping:
            return mapping
        default_mapping = build_default_symbol_theme_map(active_symbols)
        if default_mapping:
            return default_mapping
        return mapping

    def symbol_display_name_map(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for bucket in (self.stock_items, self.etf_items):
            for symbol, item in bucket.items():
                if item.name:
                    mapping[symbol] = item.name
        return mapping

    def display_name_for(self, symbol: str) -> str:
        normalized = str(symbol).strip().upper()
        if not normalized:
            return ""
        item = self.stock_items.get(normalized) or self.etf_items.get(normalized)
        return item.name if item is not None else ""

    def theme_display_name_map(self) -> dict[str, str]:
        mapping = {
            theme.theme_id: theme.display_name
            for theme in self.themes
            if theme.theme_id and theme.display_name
        }
        if mapping:
            return mapping
        theme_ids = {theme_id for theme_ids in self.symbol_theme_map().values() for theme_id in theme_ids}
        return {
            theme_id: THEME_DISPLAY_NAMES.get(theme_id, theme_id.replace("_", " ").title())
            for theme_id in sorted(theme_ids)
        }


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
class LLMConfig:
    model: str | None = None
    base_url: str | None = None
    use_narration: bool | None = None
    use_ranking_assist: bool | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "base_url": self.base_url,
            "use_narration": self.use_narration,
            "use_ranking_assist": self.use_ranking_assist,
        }


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
    use_llm_event_extraction: bool | None = None
    event_score_weights: dict[str, float] = field(default_factory=dict)
    swing: HorizonStrategyOverrides = field(default_factory=HorizonStrategyOverrides)
    position: HorizonStrategyOverrides = field(default_factory=HorizonStrategyOverrides)

    def to_record(self) -> dict[str, Any]:
        return {
            "event_score_threshold": self.event_score_threshold,
            "use_llm_event_extraction": self.use_llm_event_extraction,
            "event_score_weights": dict(self.event_score_weights),
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
    llm: LLMConfig = field(default_factory=LLMConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)

    @classmethod
    def load(cls, path: Path) -> "AgentRuntimeConfig":
        if not path.exists():
            return cls()
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        watchlist_payload = payload.get("watchlist", {})
        stock_disabled_symbols = _disabled_symbols_from_explicit_items(watchlist_payload.get("stock_items", {}))
        etf_disabled_symbols = _disabled_symbols_from_explicit_items(watchlist_payload.get("etf_items", {}))
        stock_groups = _load_watchlist_groups(
            watchlist_payload,
            grouped_key="stock_groups",
            disabled_symbols=stock_disabled_symbols,
        )
        etf_groups = _load_watchlist_groups(
            watchlist_payload,
            grouped_key="etf_groups",
            disabled_symbols=etf_disabled_symbols,
        )
        stock_items = _load_watchlist_items(
            watchlist_payload,
            flat_key="stocks",
            grouped_key="stock_groups",
        )
        etf_items = _load_watchlist_items(
            watchlist_payload,
            flat_key="etfs",
            grouped_key="etf_groups",
        )
        return cls(
            watchlist=WatchlistConfig(
                stocks=[symbol for symbol, item in stock_items.items() if item.enabled],
                etfs=[symbol for symbol, item in etf_items.items() if item.enabled],
                stock_groups=stock_groups,
                etf_groups=etf_groups,
                stock_items=stock_items,
                etf_items=etf_items,
                themes=_load_themes(watchlist_payload),
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
            llm=LLMConfig(
                model=_optional_str(payload.get("llm", {}).get("model")),
                base_url=_optional_str(payload.get("llm", {}).get("base_url")),
                use_narration=_optional_bool(payload.get("llm", {}).get("use_narration")),
                use_ranking_assist=_optional_bool(payload.get("llm", {}).get("use_ranking_assist")),
            ),
            strategy=StrategyConfig(
                event_score_threshold=_optional_float(payload.get("strategy", {}).get("event_score_threshold")),
                use_llm_event_extraction=_optional_bool(
                    payload.get("strategy", {}).get("use_llm_event_extraction")
                ),
                event_score_weights=_load_event_score_weights(
                    payload.get("strategy", {}).get("event_score_weights", {})
                ),
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
        if self.llm.model is not None:
            overrides["openai_model"] = self.llm.model
        if self.llm.base_url is not None:
            overrides["openai_base_url"] = self.llm.base_url
        if self.llm.use_narration is not None:
            overrides["use_llm_narration"] = self.llm.use_narration
        if self.llm.use_ranking_assist is not None:
            overrides["use_llm_ranking_assist"] = self.llm.use_ranking_assist
        next_settings = settings.with_overrides(**overrides) if overrides else settings
        if self.strategy.use_llm_event_extraction is not None:
            next_settings = next_settings.with_overrides(
                use_llm_event_extraction=self.strategy.use_llm_event_extraction
            )
        next_settings = next_settings.with_strategy_overrides(
            event_score_threshold=self.strategy.event_score_threshold,
            event_score_weights=self.strategy.event_score_weights,
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
                "stock_items": {
                    symbol: item.to_record()
                    for symbol, item in self.watchlist.stock_items.items()
                },
                "etf_items": {
                    symbol: item.to_record()
                    for symbol, item in self.watchlist.etf_items.items()
                },
                "themes": [theme.to_record() for theme in self.watchlist.themes],
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
            "llm": self.llm.to_record(),
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
        symbol = _extract_symbol(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _extract_symbol(value: Any) -> str:
    if isinstance(value, dict):
        raw_symbol = value.get("symbol", "")
    else:
        raw_symbol = value
    return str(raw_symbol).strip().upper()


def _extract_name(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    return str(value.get("name", "")).strip()


def _extract_enabled(value: Any) -> bool:
    if not isinstance(value, dict):
        return True
    enabled = value.get("enabled", True)
    return bool(enabled)


def _load_watchlist_symbols(payload: dict[str, Any], *, flat_key: str, grouped_key: str) -> list[str]:
    combined: list[Any] = list(payload.get(flat_key, []))
    grouped = payload.get(grouped_key, {})
    if isinstance(grouped, dict):
        for symbols in grouped.values():
            if isinstance(symbols, list):
                combined.extend(symbols)
    return _normalize_symbols(combined)


def _load_watchlist_groups(
    payload: dict[str, Any],
    *,
    grouped_key: str,
    disabled_symbols: set[str] | None = None,
) -> dict[str, list[str]]:
    grouped = payload.get(grouped_key, {})
    if not isinstance(grouped, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    blocked = {symbol.upper() for symbol in (disabled_symbols or set())}
    for group_name, symbols in grouped.items():
        if not isinstance(symbols, list):
            continue
        cleaned = [symbol for symbol in _normalize_symbols(symbols) if symbol not in blocked]
        if cleaned:
            normalized[str(group_name)] = cleaned
    return normalized


def _load_watchlist_items(
    payload: dict[str, Any],
    *,
    flat_key: str,
    grouped_key: str,
) -> dict[str, WatchlistItem]:
    combined: list[Any] = list(payload.get(flat_key, []))
    grouped = payload.get(grouped_key, {})
    if isinstance(grouped, dict):
        for symbols in grouped.values():
            if isinstance(symbols, list):
                combined.extend(symbols)
    items = _load_watchlist_item_map(combined)
    explicit_key = {"stocks": "stock_items", "etfs": "etf_items"}.get(flat_key, f"{flat_key}_items")
    explicit_items = payload.get(explicit_key, {})
    if isinstance(explicit_items, dict):
        items.update(_load_watchlist_item_dict(explicit_items))
    return items


def _load_watchlist_item_map(values: Any) -> dict[str, WatchlistItem]:
    normalized: dict[str, WatchlistItem] = {}
    if not isinstance(values, (list, tuple, set)):
        values = list(values) if values else []
    for value in values:
        symbol = _extract_symbol(value)
        if not symbol:
            continue
        item = WatchlistItem(
            symbol=symbol,
            name=_extract_name(value),
            enabled=_extract_enabled(value),
        )
        previous = normalized.get(symbol)
        if previous is None or (not previous.name and item.name):
            normalized[symbol] = item
    return normalized


def _load_watchlist_item_dict(values: dict[str, Any]) -> dict[str, WatchlistItem]:
    normalized: dict[str, WatchlistItem] = {}
    for raw_symbol, raw_value in values.items():
        value = raw_value
        if not isinstance(value, dict):
            value = {"symbol": raw_symbol, "name": str(raw_value or "").strip()}
        symbol = _extract_symbol(value) or str(raw_symbol).strip().upper()
        if not symbol:
            continue
        item = WatchlistItem(
            symbol=symbol,
            name=_extract_name(value),
            enabled=_extract_enabled(value),
        )
        previous = normalized.get(symbol)
        if previous is None or (not previous.name and item.name):
            normalized[symbol] = item
    return normalized


def _disabled_symbols_from_explicit_items(values: Any) -> set[str]:
    if not isinstance(values, dict):
        return set()
    disabled: set[str] = set()
    for raw_symbol, raw_value in values.items():
        value = raw_value
        if not isinstance(value, dict):
            value = {"symbol": raw_symbol, "name": str(raw_value or "").strip()}
        symbol = _extract_symbol(value) or str(raw_symbol).strip().upper()
        if symbol and not _extract_enabled(value):
            disabled.add(symbol)
    return disabled


def _load_themes(payload: dict[str, Any]) -> list[ThemeConfig]:
    raw_themes = payload.get("themes", [])
    if not isinstance(raw_themes, list):
        return []
    themes: list[ThemeConfig] = []
    for raw in raw_themes:
        if not isinstance(raw, dict):
            continue
        theme_id = str(raw.get("theme_id", "")).strip()
        display_name = str(raw.get("display_name", "")).strip()
        if not theme_id or not display_name:
            continue
        themes.append(
            ThemeConfig(
                theme_id=theme_id,
                display_name=display_name,
                symbols=_normalize_symbols(list(raw.get("symbols", []))),
                etfs=_normalize_symbols(list(raw.get("etfs", []))),
            )
        )
    return themes


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


def _load_event_score_weights(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    allowed = {"importance", "source_credibility", "novelty", "theme_relevance", "sentiment"}
    loaded: dict[str, float] = {}
    for key, raw in value.items():
        if key not in allowed:
            continue
        normalized = _optional_float(raw)
        if normalized is not None:
            loaded[key] = normalized
    return loaded


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
