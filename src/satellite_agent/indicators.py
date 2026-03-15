from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

from .models import Bar


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def simple_moving_average(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0
    window = list(values[-period:]) if len(values) >= period else list(values)
    return sum(window) / len(window)


def exponential_rsi(values: Sequence[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for prev, curr in zip(values[:-1], values[1:]):
        change = curr - prev
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def average_true_range(bars: Sequence[Bar], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    true_ranges: List[float] = []
    previous_close = bars[0].close
    for bar in bars[1:]:
        tr = max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close))
        true_ranges.append(tr)
        previous_close = bar.close
    if not true_ranges:
        return 0.0
    window = true_ranges[-period:] if len(true_ranges) >= period else true_ranges
    return sum(window) / len(window)


def support_resistance(bars: Sequence[Bar], lookback: int) -> Tuple[float, float]:
    if not bars:
        return 0.0, 0.0
    window = list(bars[-lookback:]) if len(bars) >= lookback else list(bars)
    support = min(bar.low for bar in window)
    resistance = max(bar.high for bar in window)
    return support, resistance


def relative_volume(volumes: Sequence[float], period: int = 20) -> float:
    if not volumes:
        return 1.0
    baseline = simple_moving_average(volumes[:-1], period) if len(volumes) > 1 else volumes[0]
    if baseline == 0:
        return 1.0
    return volumes[-1] / baseline


def closes(bars: Iterable[Bar]) -> List[float]:
    return [bar.close for bar in bars]
