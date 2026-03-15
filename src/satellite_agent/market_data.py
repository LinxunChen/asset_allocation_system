from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Protocol
from urllib import parse, request
import json

from .indicators import average_true_range, closes, exponential_rsi, relative_volume, simple_moving_average, support_resistance
from .models import Bar, IndicatorSnapshot, utcnow
from .store import Store


class MarketDataProvider(Protocol):
    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        ...


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


class YahooFinanceMarketDataProvider:
    INTERVALS = {"5m": "5m", "1d": "1d"}
    RANGES = {"5m": "5d", "1d": "6mo"}

    def __init__(self) -> None:
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart"

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        interval = self.INTERVALS[timeframe]
        range_key = self.RANGES[timeframe]
        query = parse.urlencode({"interval": interval, "range": range_key, "includePrePost": "false"})
        url = f"{self.base_url}/{parse.quote(symbol)}?{query}"
        req = request.Request(url, headers={"User-Agent": "satellite-agent/0.1"})
        with request.urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        result = payload["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]
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
            bars.append(
                Bar(
                    timestamp=datetime.fromtimestamp(ts, tz=timezone.utc),
                    open=float(values[0]),
                    high=float(values[1]),
                    low=float(values[2]),
                    close=float(values[3]),
                    volume=float(values[4]),
                )
            )
        return bars[-limit:]


class CachedMarketDataProvider:
    def __init__(self, store: Store, remote_provider: MarketDataProvider) -> None:
        self.store = store
        self.remote_provider = remote_provider

    def get_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        bars = self.store.load_price_bars(symbol, timeframe, limit)
        if bars and self._is_fresh(bars[-1], timeframe):
            return bars
        remote_bars = self.remote_provider.get_bars(symbol, timeframe, limit)
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

    def snapshot(self, symbol: str, horizon: str) -> IndicatorSnapshot:
        daily_bars = self.provider.get_bars(symbol, "1d", 90)
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
