from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Protocol
from urllib import parse, request
from urllib.error import HTTPError, URLError
from http.client import IncompleteRead
import json
import time

from .indicators import average_true_range, closes, exponential_rsi, relative_volume, simple_moving_average, support_resistance
from .models import Bar, IndicatorSnapshot, utcnow
from .store import Store


class MarketDataProvider(Protocol):
    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        ...


class MultiSourceMarketDataError(RuntimeError):
    def __init__(self, symbol: str, timeframe: str, provider_errors: dict[str, str]) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.provider_errors = provider_errors
        summary = "; ".join(f"{provider}={reason}" for provider, reason in provider_errors.items())
        super().__init__(f"{symbol} {timeframe}: {summary}")


class StoreBackedMarketDataProvider:
    def __init__(self, store: Store) -> None:
        self.store = store

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        return self.store.load_price_bars(symbol, timeframe, limit)


@dataclass
class InMemoryMarketDataProvider:
    data: Dict[tuple[str, str], list[Bar]]

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        return list(self.data.get((symbol, timeframe), []))[-limit:]


def _normalize_symbol_for_yahoo(symbol: str) -> str:
    return symbol.upper().replace(".", "-")


def _normalize_symbol_for_stooq(symbol: str) -> str:
    normalized = symbol.upper().replace(".", "-")
    if normalized.endswith(".US"):
        return normalized.lower()
    return f"{normalized.lower()}.us"


class YahooFinanceMarketDataProvider:
    INTERVALS = {"5m": "5m", "1d": "1d"}
    RANGES = {"5m": "5d", "1d": "6mo"}

    def __init__(self, request_timeout: float = 8.0, max_retries: int = 2, retry_backoff_seconds: float = 0.25) -> None:
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        interval = self.INTERVALS[timeframe]
        range_key = self.RANGES[timeframe]
        query = parse.urlencode({"interval": interval, "range": range_key, "includePrePost": "false"})
        provider_symbol = _normalize_symbol_for_yahoo(symbol)
        url = f"{self.base_url}/{parse.quote(provider_symbol)}?{query}"
        req = request.Request(url, headers={"User-Agent": "satellite-agent/0.1"})
        payload = None
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                with request.urlopen(req, timeout=self.request_timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except (URLError, HTTPError, IncompleteRead, json.JSONDecodeError, TimeoutError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_backoff_seconds * (attempt + 1))
        if payload is None:
            if last_error is not None:
                raise last_error
            raise ValueError(f"Yahoo Finance returned no payload for {symbol} {timeframe}")
        chart_payload = payload.get("chart", {})
        result_items = chart_payload.get("result") or []
        if not result_items:
            error_detail = chart_payload.get("error") or {}
            error_message = error_detail.get("description") or error_detail.get("code") or "unknown_error"
            raise ValueError(f"Yahoo Finance returned no chart result for {symbol} {timeframe}: {error_message}")
        result = result_items[0]
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]
        adjclose_items = result.get("indicators", {}).get("adjclose") or []
        adjusted_closes = adjclose_items[0].get("adjclose", []) if adjclose_items else []
        bars: list[Bar] = []
        for index, ts in enumerate(timestamps):
            values = (
                quote["open"][index],
                quote["high"][index],
                quote["low"][index],
                quote["close"][index],
                quote["volume"][index],
            )
            if any(value is None for value in values):
                continue
            open_value = float(values[0])
            high_value = float(values[1])
            low_value = float(values[2])
            close_value = float(values[3])
            adjusted = False
            if timeframe == "1d":
                adjclose_value = adjusted_closes[index] if index < len(adjusted_closes) else None
                if adjclose_value is not None and close_value:
                    factor = float(adjclose_value) / close_value
                    open_value = round(open_value * factor, 6)
                    high_value = round(high_value * factor, 6)
                    low_value = round(low_value * factor, 6)
                    close_value = round(close_value * factor, 6)
                    adjusted = True
            bars.append(
                Bar(
                    timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                    open=open_value,
                    high=high_value,
                    low=low_value,
                    close=close_value,
                    volume=float(values[4]),
                    adjusted=adjusted,
                )
            )
        return bars[-limit:]


class StooqDailyMarketDataProvider:
    def __init__(self, request_timeout: float = 6.0) -> None:
        self.base_url = "https://stooq.com/q/d/l/"
        self.request_timeout = request_timeout

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        if timeframe != "1d":
            raise ValueError(f"Stooq daily backup does not support timeframe {timeframe}")
        provider_symbol = _normalize_symbol_for_stooq(symbol)
        query = parse.urlencode({"s": provider_symbol, "i": "d"})
        url = f"{self.base_url}?{query}"
        req = request.Request(url, headers={"User-Agent": "satellite-agent/0.1"})
        with request.urlopen(req, timeout=self.request_timeout) as response:
            raw_text = response.read().decode("utf-8").strip()
        if not raw_text or raw_text.lower().startswith("no data"):
            raise ValueError(f"Stooq returned no daily bars for {symbol}")
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        if len(lines) <= 1:
            raise ValueError(f"Stooq returned insufficient daily bars for {symbol}")
        bars: list[Bar] = []
        for line in lines[1:]:
            columns = [column.strip() for column in line.split(",")]
            if len(columns) < 6:
                continue
            if any(value.lower() == "null" or value == "" for value in columns[:6]):
                continue
            bars.append(
                Bar(
                    timestamp=datetime.fromisoformat(columns[0]).replace(tzinfo=timezone.utc),
                    open=float(columns[1]),
                    high=float(columns[2]),
                    low=float(columns[3]),
                    close=float(columns[4]),
                    volume=float(columns[5]),
                    adjusted=False,
                )
            )
        if not bars:
            raise ValueError(f"Stooq returned no parsable daily bars for {symbol}")
        return bars[-limit:]


class MultiSourceMarketDataProvider:
    def __init__(self, providers: Iterable[MarketDataProvider]) -> None:
        self.providers = list(providers)

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        provider_errors: dict[str, str] = {}
        for provider in self.providers:
            provider_name = provider.__class__.__name__
            try:
                bars = provider.get_bars(symbol, timeframe, limit)
            except Exception as exc:
                provider_errors[provider_name] = f"{exc.__class__.__name__}: {exc}"
                continue
            if bars:
                return bars[-limit:]
            provider_errors[provider_name] = "empty_response"
        if provider_errors:
            raise MultiSourceMarketDataError(symbol, timeframe, provider_errors)
        return []


class CachedMarketDataProvider:
    def __init__(self, store: Store, remote_provider: MarketDataProvider) -> None:
        self.store = store
        self.remote_provider = remote_provider

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        bars = self.store.load_price_bars(symbol, timeframe, limit)
        if bars and self._is_fresh(bars[-1], timeframe):
            return bars
        try:
            remote_bars = self.remote_provider.get_bars(symbol, timeframe, limit)
        except Exception:
            if bars:
                return bars
            raise
        if remote_bars:
            self.store.upsert_price_bars(symbol, timeframe, remote_bars)
            return remote_bars[-limit:]
        return bars

    def _is_fresh(self, latest_bar: Bar, timeframe: str) -> bool:
        now = utcnow()
        latest = latest_bar.timestamp
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        else:
            latest = latest.astimezone(timezone.utc)
        age_seconds = (now - latest).total_seconds()
        freshness_window = 45 * 60 if timeframe == "5m" else 3 * 24 * 60 * 60
        return age_seconds <= freshness_window


class MarketDataEngine:
    def __init__(self, provider: MarketDataProvider) -> None:
        self.provider = provider

    def snapshot(self, symbol: str, horizon: str, include_intraday: bool = True) -> IndicatorSnapshot:
        daily_bars = self.provider.get_bars(symbol, "1d", 90)
        intraday_bars: list[Bar] = []
        if horizon == "swing" or include_intraday:
            intraday_bars = self.provider.get_bars(symbol, "5m", 60)
        if not daily_bars:
            raise ValueError(f"No daily bars available for {symbol}")
        if horizon == "swing" and not intraday_bars:
            intraday_bars = daily_bars[-20:]
        daily_closes = closes(daily_bars)
        intraday_closes = closes(intraday_bars) if intraday_bars else daily_closes
        sma20 = simple_moving_average(daily_closes, 20)
        sma60 = simple_moving_average(daily_closes, 60)
        atr14 = average_true_range(daily_bars, 14)
        rsi_input = intraday_closes if horizon == "swing" else daily_closes
        rsi14 = exponential_rsi(rsi_input, 14)
        support20, resistance20 = support_resistance(daily_bars, 20)
        support60, resistance60 = support_resistance(daily_bars, 60)
        last_price = intraday_closes[-1] if intraday_closes else daily_closes[-1]
        gap_percent = 0.0
        if intraday_bars and len(daily_bars) >= 2:
            prev_close = daily_bars[-2].close
            gap_percent = ((intraday_bars[0].open - prev_close) / prev_close) * 100 if prev_close else 0.0
        volume_series = [bar.volume for bar in intraday_bars] if horizon == "swing" and intraday_bars else [bar.volume for bar in daily_bars]
        rel_volume = relative_volume(volume_series, 20)
        trend_state = self._trend_state(last_price, sma20, sma60)
        intraday_breakout = bool(last_price > resistance20 * 1.001) if resistance20 else False
        is_pullback = bool(sma20 and (sma20 - atr14) <= last_price <= (sma20 + atr14))
        atr_percent = (atr14 / last_price) * 100 if last_price else 0.0
        return IndicatorSnapshot(
            symbol=symbol,
            horizon=horizon,
            as_of=utcnow(),
            last_price=last_price,
            rsi_14=rsi14,
            atr_14=atr14,
            sma_20=sma20,
            sma_60=sma60,
            relative_volume=rel_volume,
            support_20=support20,
            resistance_20=resistance20,
            support_60=support60,
            resistance_60=resistance60,
            gap_percent=gap_percent,
            intraday_breakout=intraday_breakout,
            is_pullback=is_pullback,
            trend_state=trend_state,
            atr_percent=atr_percent,
        )

    def _trend_state(self, last_price: float, sma20: float, sma60: float) -> str:
        if last_price >= sma20 >= sma60:
            return "bullish"
        if last_price <= sma20 <= sma60:
            return "bearish"
        return "neutral"
