from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .indicators import simple_moving_average
from .models import Bar, ensure_utc, utcnow
from .market_data import MultiSourceMarketDataError
from .store import Store

MAX_OUTCOME_LOOKAHEAD_DAYS = 10
DEFAULT_INCREMENTAL_OUTCOME_BACKFILL_DAYS = 45
US_MARKET_TZ = ZoneInfo("America/New_York")
MACRO_PROXY_SYMBOLS = ("SPY", "QQQ", "SMH", "TLT")
EXECUTABLE_OUTCOME_ACTIONS = ("试探建仓", "确认做多")


@dataclass
class DecisionOutcomeResult:
    decision_id: str
    entered: bool
    entered_at: str
    entry_price: float | None
    exit_price: float | None
    realized_return: float | None
    holding_days: int | None
    gross_realized_return: float | None
    net_realized_return: float | None
    slippage_bps: float
    t_plus_1_return: float | None
    t_plus_3_return: float | None
    t_plus_5_return: float | None
    t_plus_7_return: float | None
    t_plus_10_return: float | None
    t_plus_14_return: float | None
    t_plus_30_return: float | None
    max_runup: float | None
    max_drawdown: float | None
    hit_take_profit: bool
    hit_invalidation: bool
    close_reason: str
    exit_subreason: str = ""


@dataclass
class DecisionOutcomeComputation:
    outcome: DecisionOutcomeResult | None
    skip_reason: str = ""


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _pct_change(value: float, base: float) -> float:
    return round(((value - base) / base) * 100.0, 2)


def _market_session_date(value: datetime) -> date:
    normalized = ensure_utc(value)
    if normalized.hour == 0 and normalized.minute == 0 and normalized.second == 0 and normalized.microsecond == 0:
        return normalized.date()
    return normalized.astimezone(US_MARKET_TZ).date()


def _first_daily_bar_on_or_after(bars: list[Bar], created_at: datetime) -> int | None:
    target_date = _market_session_date(created_at)
    for index, bar in enumerate(bars):
        if _market_session_date(bar.timestamp) >= target_date:
            return index
    return None


def _bars_cover_created_at(bars: list[Bar], created_at: datetime) -> bool:
    if not bars:
        return False
    latest_session = _market_session_date(bars[-1].timestamp)
    target_session = _market_session_date(created_at)
    return latest_session >= target_session


def _latest_bar_session_date(bars: list[Bar]) -> date | None:
    if not bars:
        return None
    return _market_session_date(bars[-1].timestamp)


def _should_replace_daily_bars(existing_bars: list[Bar], remote_bars: list[Bar]) -> bool:
    if not remote_bars:
        return False
    if not all(bool(getattr(bar, "adjusted", False)) for bar in remote_bars):
        return False
    return any(not bool(getattr(bar, "adjusted", False)) for bar in existing_bars)


def _bars_need_adjusted_refresh(bars: list[Bar]) -> bool:
    if not bars:
        return False
    return any(not bool(getattr(bar, "adjusted", False)) for bar in bars)


def _load_packet_payload(row: Any) -> dict[str, Any]:
    raw = row["packet_json"]
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _load_json_payload(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_price_levels(row: Any) -> tuple[float | None, float | None, float | None]:
    packet = _load_packet_payload(row)
    packet_price_plan = packet.get("price_plan") or {}
    entry_plan = _load_json_payload(row["entry_plan_json"] if "entry_plan_json" in row.keys() else None)
    invalidation = _load_json_payload(row["invalidation_json"] if "invalidation_json" in row.keys() else None)

    take_profit_range = (
        packet_price_plan.get("take_profit_range")
        or entry_plan.get("take_profit_range")
        or {}
    )
    take_profit_low = _coerce_float(
        take_profit_range.get("low")
        if isinstance(take_profit_range, dict)
        else None
    )
    take_profit_high = _coerce_float(
        take_profit_range.get("high")
        if isinstance(take_profit_range, dict)
        else None
    )
    if take_profit_low is None and take_profit_high is not None:
        take_profit_low = take_profit_high
    if take_profit_high is None and take_profit_low is not None:
        take_profit_high = take_profit_low

    invalidation_level = _coerce_float(packet_price_plan.get("invalidation_level"))
    if invalidation_level is None:
        invalidation_level = _coerce_float(entry_plan.get("invalidation_level"))
    if invalidation_level is None:
        invalidation_level = _coerce_float(invalidation.get("level"))

    return take_profit_low, take_profit_high, invalidation_level


def _take_profit_midpoint(low: float | None, high: float | None) -> float | None:
    if low is None and high is None:
        return None
    if low is None:
        return high
    if high is None:
        return low
    return round((low + high) / 2.0, 4)


def _extract_entry_range(row: Any) -> tuple[float | None, float | None]:
    packet = _load_packet_payload(row)
    packet_price_plan = packet.get("price_plan") or {}
    entry_plan = _load_json_payload(row["entry_plan_json"] if "entry_plan_json" in row.keys() else None)
    entry_range = (
        packet_price_plan.get("entry_range")
        or entry_plan.get("entry_range")
        or {}
    )
    if not isinstance(entry_range, dict):
        return None, None
    low = _coerce_float(entry_range.get("low"))
    high = _coerce_float(entry_range.get("high"))
    if low is None or high is None:
        return None, None
    if low > high:
        low, high = high, low
    return low, high


def _entry_price_for_long(bar: Bar, *, entry_high: float) -> float:
    return round(min(float(bar.open), float(entry_high)), 4)


def _bar_overlaps_entry(bar: Bar, *, entry_low: float, entry_high: float) -> bool:
    return float(bar.low) <= float(entry_high) and float(bar.high) >= float(entry_low)


def _entry_timestamp_for_decision(bar: Bar, *, created_at: datetime) -> datetime:
    """Prevent same-session daily bars from producing pre-decision entry timestamps."""
    return max(ensure_utc(bar.timestamp), created_at)


def _resolve_long_exit(
    bar: Bar,
    *,
    take_profit_mid: float | None,
    invalidation_level: float | None,
) -> tuple[str, float | None, str]:
    hit_invalidation = invalidation_level is not None and bar.low <= invalidation_level
    hit_take_profit = take_profit_mid is not None and bar.high >= take_profit_mid
    if hit_invalidation and hit_take_profit:
        exit_price = float(bar.open) if invalidation_level is not None and float(bar.open) <= invalidation_level else invalidation_level
        return "hit_invalidation", round(float(exit_price), 4) if exit_price is not None else None, ""
    if hit_invalidation:
        exit_price = float(bar.open) if invalidation_level is not None and float(bar.open) <= invalidation_level else invalidation_level
        return "hit_invalidation", round(float(exit_price), 4) if exit_price is not None else None, ""
    if hit_take_profit:
        exit_price = float(bar.open) if take_profit_mid is not None and float(bar.open) >= take_profit_mid else take_profit_mid
        return "exit_pool", round(float(exit_price), 4) if exit_price is not None else None, "target_hit"
    return "", None, ""


def _close_reason_for_window(
    bars: list[Bar],
    *,
    take_profit_mid: float | None,
    invalidation_level: float | None,
) -> str:
    for bar in bars:
        hit_invalidation = invalidation_level is not None and bar.low <= invalidation_level
        hit_take_profit = take_profit_mid is not None and bar.high >= take_profit_mid
        if hit_invalidation and hit_take_profit:
            return "hit_invalidation"
        if hit_invalidation:
            return "hit_invalidation"
        if hit_take_profit:
            return "exit_pool"
    if len(bars) < MAX_OUTCOME_LOOKAHEAD_DAYS + 1:
        return "insufficient_lookahead"
    return "window_complete"

def _atr_slice(bars: list[Bar], end_index: int, period: int = 14) -> float:
    start_index = max(1, end_index - period + 1)
    true_ranges: list[float] = []
    for index in range(start_index, end_index + 1):
        current = bars[index]
        previous_close = float(bars[index - 1].close) if index > 0 else float(current.close)
        true_ranges.append(
            max(
                float(current.high) - float(current.low),
                abs(float(current.high) - previous_close),
                abs(float(current.low) - previous_close),
            )
        )
    if not true_ranges:
        return max(float(bars[end_index].high) - float(bars[end_index].low), 0.01)
    return max(sum(true_ranges) / len(true_ranges), 0.01)


def _trend_state_for_bars(bars: list[Bar], *, up_to_session: date) -> str:
    eligible = [float(bar.close) for bar in bars if _market_session_date(bar.timestamp) <= up_to_session]
    if len(eligible) < 60:
        return "neutral"
    last_price = eligible[-1]
    sma20 = simple_moving_average(eligible, 20)
    sma60 = simple_moving_average(eligible, 60)
    if last_price >= sma20 >= sma60:
        return "bullish"
    if last_price <= sma20 <= sma60:
        return "bearish"
    return "neutral"


def _macro_risk_score_for_session(session_day: date, proxy_bars_by_symbol: dict[str, list[Bar]]) -> float:
    scores: list[int] = []
    trends: dict[str, str] = {}
    for symbol in MACRO_PROXY_SYMBOLS:
        proxy_bars = proxy_bars_by_symbol.get(symbol, [])
        trends[symbol] = _trend_state_for_bars(proxy_bars, up_to_session=session_day) if proxy_bars else "neutral"
    for symbol in ("SPY", "QQQ", "SMH"):
        trend_state = trends.get(symbol, "neutral")
        if trend_state == "bullish":
            scores.append(1)
        elif trend_state == "bearish":
            scores.append(-1)
        else:
            scores.append(0)
    avg_score = sum(scores) / len(scores) if scores else 0.0
    if avg_score <= -0.6:
        regime = "risk_off"
    elif avg_score >= 0.6:
        regime = "risk_on"
    else:
        regime = "neutral"
    tlt_trend = trends.get("TLT", "neutral")
    if tlt_trend == "bearish":
        rate_risk = "high"
    elif tlt_trend == "neutral":
        rate_risk = "medium"
    else:
        rate_risk = "low"
    geopolitical_risk = (
        "high"
        if regime == "risk_off" and trends.get("SMH") == "bearish" and tlt_trend == "bullish"
        else "low"
    )
    score = 0.0
    score += {"risk_on": 10.0, "neutral": 35.0, "risk_off": 70.0}[regime]
    score += {"low": 0.0, "medium": 10.0, "high": 20.0}[rate_risk]
    score += {"low": 0.0, "high": 10.0}.get(geopolitical_risk, 5.0)
    return round(min(score, 100.0), 2)


def compute_decision_outcome(
    row: Any,
    bars: list[Bar],
    *,
    proxy_bars_by_symbol: dict[str, list[Bar]] | None = None,
) -> DecisionOutcomeResult | None:
    return _compute_decision_outcome(row, bars, proxy_bars_by_symbol=proxy_bars_by_symbol).outcome


def explain_decision_outcome(
    row: Any,
    bars: list[Bar],
    *,
    proxy_bars_by_symbol: dict[str, list[Bar]] | None = None,
) -> DecisionOutcomeComputation:
    return _compute_decision_outcome(row, bars, proxy_bars_by_symbol=proxy_bars_by_symbol)


def _compute_decision_outcome(
    row: Any,
    bars: list[Bar],
    *,
    proxy_bars_by_symbol: dict[str, list[Bar]] | None = None,
) -> DecisionOutcomeComputation:
    created_at = _parse_timestamp(row["created_at"])
    if created_at is None:
        return DecisionOutcomeComputation(outcome=None, skip_reason="invalid_created_at")
    if not bars:
        return DecisionOutcomeComputation(outcome=None, skip_reason="missing_bars")
    anchor_index = _first_daily_bar_on_or_after(bars, created_at)
    if anchor_index is None:
        if bars and not _bars_cover_created_at(bars, created_at):
            return DecisionOutcomeComputation(outcome=None, skip_reason="stale_bars")
        return DecisionOutcomeComputation(outcome=None, skip_reason="missing_anchor_bar")

    future_bars = bars[anchor_index:]
    if not future_bars:
        return DecisionOutcomeComputation(outcome=None, skip_reason="missing_anchor_bar")

    anchor_bar = future_bars[0]
    anchor_price = anchor_bar.close
    if anchor_price <= 0:
        return DecisionOutcomeComputation(outcome=None, skip_reason="invalid_anchor_price")

    take_profit_low, take_profit_high, invalidation_level = _extract_price_levels(row)
    take_profit_mid = _take_profit_midpoint(take_profit_low, take_profit_high)

    window = future_bars[: MAX_OUTCOME_LOOKAHEAD_DAYS + 1]
    max_runup = max((_pct_change(bar.high, anchor_price) for bar in window), default=None)
    max_drawdown = min((_pct_change(bar.low, anchor_price) for bar in window), default=None)
    hit_take_profit = bool(take_profit_mid is not None and any(bar.high >= take_profit_mid for bar in window))
    hit_invalidation = bool(invalidation_level is not None and any(bar.low <= invalidation_level for bar in window))

    def _close_return(offset: int) -> float | None:
        if len(future_bars) <= offset:
            return None
        return _pct_change(future_bars[offset].close, anchor_price)

    close_reason = _close_reason_for_window(
        window,
        take_profit_mid=take_profit_mid,
        invalidation_level=invalidation_level,
    )

    entry_low, entry_high = _extract_entry_range(row)
    entered = False
    entered_at = ""
    entry_price = None
    exit_price = None
    realized_return = None
    holding_days = None
    gross_realized_return = None
    net_realized_return = None
    exit_reason = close_reason
    exit_subreason = ""
    if entry_low is not None and entry_high is not None:
        entry_index = None
        tp_zone_entered = False
        weakening_close_count = 0
        for index, bar in enumerate(window):
            if invalidation_level is not None and float(bar.open) <= float(invalidation_level):
                exit_reason = "not_entered"
                entered = False
                entered_at = ""
                entry_price = None
                exit_price = None
                holding_days = None
                break
            if not _bar_overlaps_entry(bar, entry_low=entry_low, entry_high=entry_high):
                continue
            entered = True
            entry_index = index
            entered_at = _entry_timestamp_for_decision(bar, created_at=created_at).isoformat()
            entry_price = _entry_price_for_long(bar, entry_high=entry_high)
            same_bar_exit_reason, same_bar_exit_price, same_bar_subreason = _resolve_long_exit(
                bar,
                take_profit_mid=take_profit_mid,
                invalidation_level=invalidation_level,
            )
            if same_bar_exit_reason:
                exit_reason = same_bar_exit_reason
                exit_price = same_bar_exit_price
                exit_subreason = same_bar_subreason
                holding_days = 0
                break
            tp_zone_entered = bool(take_profit_low is not None and float(bar.high) >= take_profit_low)
            if (
                tp_zone_entered
                and take_profit_low is not None
                and float(bar.close) < take_profit_low
            ):
                weakening_close_count = 1
            if (
                proxy_bars_by_symbol
                and entry_price is not None
                and float(bar.close) >= (entry_price + _atr_slice(window, index))
                and _macro_risk_score_for_session(
                    _market_session_date(bar.timestamp),
                    proxy_bars_by_symbol,
                )
                >= 70.0
            ):
                exit_reason = "exit_pool"
                exit_subreason = "macro_protection"
                exit_price = round(float(bar.close), 4)
                holding_days = 0
                break
            for offset in range(index + 1, len(window)):
                current_bar = window[offset]
                reason, resolved_exit_price, resolved_subreason = _resolve_long_exit(
                    current_bar,
                    take_profit_mid=take_profit_mid,
                    invalidation_level=invalidation_level,
                )
                if not reason:
                    if take_profit_low is not None and float(current_bar.high) >= take_profit_low:
                        tp_zone_entered = True
                    if tp_zone_entered and take_profit_low is not None and float(current_bar.close) < take_profit_low:
                        weakening_close_count += 1
                    elif tp_zone_entered:
                        weakening_close_count = 0
                    if tp_zone_entered and weakening_close_count >= 2:
                        exit_reason = "exit_pool"
                        exit_subreason = "weakening_after_tp_zone"
                        exit_price = round(float(current_bar.close), 4)
                        holding_days = offset - index
                        break
                    if (
                        proxy_bars_by_symbol
                        and entry_price is not None
                        and float(current_bar.close) >= (entry_price + _atr_slice(window, offset))
                        and _macro_risk_score_for_session(
                            _market_session_date(current_bar.timestamp),
                            proxy_bars_by_symbol,
                        )
                        >= 70.0
                    ):
                        exit_reason = "exit_pool"
                        exit_subreason = "macro_protection"
                        exit_price = round(float(current_bar.close), 4)
                        holding_days = offset - index
                        break
                    continue
                exit_reason = reason
                exit_subreason = resolved_subreason
                exit_price = resolved_exit_price
                holding_days = offset - index
                break
            if exit_price is None:
                if len(window) >= MAX_OUTCOME_LOOKAHEAD_DAYS + 1:
                    exit_reason = "window_complete"
                    exit_price = round(float(window[-1].close), 4)
                    holding_days = (len(window) - 1) - index
                else:
                    exit_reason = "insufficient_lookahead"
            break
        if not entered and len(window) >= MAX_OUTCOME_LOOKAHEAD_DAYS + 1:
            exit_reason = "not_entered"
    else:
        if take_profit_mid is not None or invalidation_level is not None:
            entered = True
            entered_at = _entry_timestamp_for_decision(anchor_bar, created_at=created_at).isoformat()
            entry_price = round(float(anchor_bar.close), 4)
            same_bar_exit_reason, same_bar_exit_price, same_bar_subreason = _resolve_long_exit(
                anchor_bar,
                take_profit_mid=take_profit_mid,
                invalidation_level=invalidation_level,
            )
            if same_bar_exit_reason:
                exit_reason = same_bar_exit_reason
                exit_price = same_bar_exit_price
                exit_subreason = same_bar_subreason
                holding_days = 0
            else:
                tp_zone_entered = bool(take_profit_low is not None and float(anchor_bar.high) >= take_profit_low)
                weakening_close_count = (
                    1
                    if tp_zone_entered and take_profit_low is not None and float(anchor_bar.close) < take_profit_low
                    else 0
                )
                for offset in range(1, len(window)):
                    current_bar = window[offset]
                    reason, resolved_exit_price, resolved_subreason = _resolve_long_exit(
                        window[offset],
                        take_profit_mid=take_profit_mid,
                        invalidation_level=invalidation_level,
                    )
                    if not reason:
                        if take_profit_low is not None and float(current_bar.high) >= take_profit_low:
                            tp_zone_entered = True
                        if tp_zone_entered and take_profit_low is not None and float(current_bar.close) < take_profit_low:
                            weakening_close_count += 1
                        elif tp_zone_entered:
                            weakening_close_count = 0
                        if tp_zone_entered and weakening_close_count >= 2:
                            exit_reason = "exit_pool"
                            exit_subreason = "weakening_after_tp_zone"
                            exit_price = round(float(current_bar.close), 4)
                            holding_days = offset
                            break
                        if (
                            proxy_bars_by_symbol
                            and entry_price is not None
                            and float(current_bar.close) >= (entry_price + _atr_slice(window, offset))
                            and _macro_risk_score_for_session(
                                _market_session_date(current_bar.timestamp),
                                proxy_bars_by_symbol,
                            )
                            >= 70.0
                        ):
                            exit_reason = "exit_pool"
                            exit_subreason = "macro_protection"
                            exit_price = round(float(current_bar.close), 4)
                            holding_days = offset
                            break
                        continue
                    exit_reason = reason
                    exit_subreason = resolved_subreason
                    exit_price = resolved_exit_price
                    holding_days = offset
                    break
                if exit_price is None:
                    if len(window) >= MAX_OUTCOME_LOOKAHEAD_DAYS + 1:
                        exit_reason = "window_complete"
                        exit_price = round(float(window[-1].close), 4)
                        holding_days = len(window) - 1
                    else:
                        exit_reason = "insufficient_lookahead"
        else:
            entered = False
            entered_at = ""
            entry_price = None
            exit_price = None
            holding_days = None
            exit_reason = close_reason

    if entered and entry_price is not None and exit_price is not None and exit_reason in {
        "exit_pool",
        "hit_invalidation",
        "window_complete",
    }:
        realized_return = _pct_change(exit_price, entry_price)
        gross_realized_return = realized_return
        net_realized_return = realized_return

    return DecisionOutcomeComputation(
        outcome=DecisionOutcomeResult(
            decision_id=row["decision_id"],
            entered=entered,
            entered_at=entered_at,
            entry_price=entry_price,
            exit_price=exit_price,
            realized_return=realized_return,
            holding_days=holding_days,
            gross_realized_return=gross_realized_return,
            net_realized_return=net_realized_return,
            slippage_bps=0.0,
            t_plus_1_return=_close_return(1),
            t_plus_3_return=_close_return(3),
            t_plus_5_return=_close_return(5),
            t_plus_7_return=_close_return(7),
            t_plus_10_return=_close_return(10),
            t_plus_14_return=_close_return(14),
            t_plus_30_return=_close_return(30),
            max_runup=max_runup,
            max_drawdown=max_drawdown,
            hit_take_profit=exit_reason == "exit_pool" and exit_subreason == "target_hit",
            hit_invalidation=exit_reason == "hit_invalidation",
            close_reason=exit_reason,
            exit_subreason=exit_subreason,
        )
    )


def backfill_decision_outcomes(
    store: Store,
    *,
    run_id: str = "",
    since: str = "",
    limit: int = 0,
    fetch_bars: Callable[[str, str, int], list[Bar]] | None = None,
    recompute_existing: bool = False,
) -> dict[str, int]:
    rows = store.load_decision_records_without_outcomes(
        run_id=run_id,
        since=since,
        limit=limit,
        recompute_existing=recompute_existing,
        actions=EXECUTABLE_OUTCOME_ACTIONS,
    )
    updated = 0
    skipped = 0
    pending_lookahead = 0
    completed_window = 0
    exit_pool_hits = 0
    invalidation_hits = 0
    skip_reasons = {
        "missing_bars": 0,
        "stale_bars": 0,
        "missing_anchor_bar": 0,
        "invalid_anchor_price": 0,
        "invalid_created_at": 0,
    }
    skip_symbol_samples: dict[str, list[str]] = {key: [] for key in skip_reasons}
    fetched_symbols = 0
    fetch_attempted_symbols: list[str] = []
    fetch_failed_symbols: list[str] = []
    fetch_failure_reasons: dict[str, str] = {}
    stale_symbol_details: dict[str, dict[str, str]] = {}
    bar_cache: dict[str, list[Bar]] = {}
    proxy_bar_cache: dict[str, list[Bar]] = {}

    def _load_proxy_bars(symbol: str) -> list[Bar]:
        cached = proxy_bar_cache.get(symbol)
        if cached is not None:
            return cached
        bars = store.load_price_bars(symbol, "1d", 400)
        proxy_bar_cache[symbol] = bars
        return bars

    for row in rows:
        symbol = row["symbol"]
        normalized_symbol = str(symbol).strip().upper()
        bars = bar_cache.get(symbol)
        if bars is None:
            bars = store.load_price_bars(symbol, "1d", 400)
            created_at = _parse_timestamp(row["created_at"])
            needs_refresh = created_at is not None and not _bars_cover_created_at(bars, created_at)
            needs_adjusted_refresh = _bars_need_adjusted_refresh(bars)
            local_bars_before_refresh = list(bars)
            if fetch_bars is not None and (not bars or needs_refresh or needs_adjusted_refresh):
                if normalized_symbol and normalized_symbol not in fetch_attempted_symbols:
                    fetch_attempted_symbols.append(normalized_symbol)
                fetch_error: Exception | None = None
                try:
                    remote_bars = fetch_bars(symbol, "1d", 400)
                except Exception as exc:
                    fetch_error = exc
                    remote_bars = []
                if remote_bars:
                    if _should_replace_daily_bars(local_bars_before_refresh, remote_bars):
                        store.replace_price_bars(symbol, "1d", remote_bars)
                    else:
                        store.upsert_price_bars(symbol, "1d", remote_bars)
                    bars = store.load_price_bars(symbol, "1d", 400)
                    if created_at is not None and not _bars_cover_created_at(bars, created_at):
                        if normalized_symbol and normalized_symbol not in fetch_failed_symbols:
                            fetch_failed_symbols.append(normalized_symbol)
                            fetch_failure_reasons[normalized_symbol] = "stale_after_refresh"
                        if normalized_symbol and created_at is not None:
                            stale_symbol_details[normalized_symbol] = {
                                "target_session": _market_session_date(created_at).isoformat(),
                                "latest_local_session": (
                                    _latest_bar_session_date(local_bars_before_refresh)
                                    or _latest_bar_session_date(remote_bars)
                                ).isoformat()
                                if (_latest_bar_session_date(local_bars_before_refresh) or _latest_bar_session_date(remote_bars))
                                else "",
                                "latest_remote_session": _latest_bar_session_date(remote_bars).isoformat()
                                if _latest_bar_session_date(remote_bars)
                                else "",
                            }
                    else:
                        fetched_symbols += 1
                elif normalized_symbol and normalized_symbol not in fetch_failed_symbols:
                    fetch_failed_symbols.append(normalized_symbol)
                    if fetch_error is not None:
                        if isinstance(fetch_error, MultiSourceMarketDataError):
                            fetch_failure_reasons[normalized_symbol] = ", ".join(
                                f"{provider}={reason}"
                                for provider, reason in fetch_error.provider_errors.items()
                            )
                        else:
                            fetch_failure_reasons[normalized_symbol] = f"{fetch_error.__class__.__name__}: {fetch_error}"
                    else:
                        fetch_failure_reasons[normalized_symbol] = "empty_response"
                    if normalized_symbol and created_at is not None:
                        local_latest = _latest_bar_session_date(local_bars_before_refresh)
                        stale_symbol_details[normalized_symbol] = {
                            "target_session": _market_session_date(created_at).isoformat(),
                            "latest_local_session": local_latest.isoformat() if local_latest else "",
                            "latest_remote_session": "",
                        }
            bar_cache[symbol] = bars
        proxy_bars_by_symbol = {
            proxy_symbol: _load_proxy_bars(proxy_symbol)
            for proxy_symbol in MACRO_PROXY_SYMBOLS
        }
        computation = _compute_decision_outcome(
            row,
            bars,
            proxy_bars_by_symbol=proxy_bars_by_symbol,
        )
        outcome = computation.outcome
        if outcome is None:
            skipped += 1
            if computation.skip_reason in skip_reasons:
                skip_reasons[computation.skip_reason] += 1
                samples = skip_symbol_samples[computation.skip_reason]
                if normalized_symbol and normalized_symbol not in samples and len(samples) < 5:
                    samples.append(normalized_symbol)
                if computation.skip_reason == "stale_bars" and created_at is not None and normalized_symbol:
                    latest_local = _latest_bar_session_date(bars)
                    stale_symbol_details.setdefault(
                        normalized_symbol,
                        {
                            "target_session": _market_session_date(created_at).isoformat(),
                            "latest_local_session": latest_local.isoformat() if latest_local else "",
                            "latest_remote_session": "",
                        },
                    )
            continue
        store.save_decision_outcome(
            decision_id=outcome.decision_id,
            entered=outcome.entered,
            entered_at=outcome.entered_at,
            entry_price=outcome.entry_price,
            exit_price=outcome.exit_price,
            realized_return=outcome.realized_return,
            holding_days=outcome.holding_days,
            gross_realized_return=outcome.gross_realized_return,
            net_realized_return=outcome.net_realized_return,
            slippage_bps=outcome.slippage_bps,
            t_plus_1_return=outcome.t_plus_1_return,
            t_plus_3_return=outcome.t_plus_3_return,
            t_plus_5_return=outcome.t_plus_5_return,
            t_plus_7_return=outcome.t_plus_7_return,
            t_plus_10_return=outcome.t_plus_10_return,
            t_plus_14_return=outcome.t_plus_14_return,
            t_plus_30_return=outcome.t_plus_30_return,
            max_runup=outcome.max_runup,
            max_drawdown=outcome.max_drawdown,
            hit_take_profit=outcome.hit_take_profit,
            hit_invalidation=outcome.hit_invalidation,
            close_reason=outcome.close_reason,
            exit_subreason=outcome.exit_subreason,
            updated_at=utcnow().isoformat(),
        )
        if outcome.close_reason == "insufficient_lookahead":
            pending_lookahead += 1
        else:
            completed_window += 1
        if outcome.close_reason == "exit_pool":
            exit_pool_hits += 1
        if outcome.hit_invalidation:
            invalidation_hits += 1
        updated += 1
    return {
        "updated": updated,
        "skipped": skipped,
        "scanned": len(rows),
        "pending_lookahead": pending_lookahead,
        "completed_window": completed_window,
        "exit_pool_hits": exit_pool_hits,
        "take_profit_hits": exit_pool_hits,
        "invalidation_hits": invalidation_hits,
        "fetched_symbols": fetched_symbols,
        "fetch_attempted_symbols": fetch_attempted_symbols,
        "fetch_failed_symbols": fetch_failed_symbols,
        "fetch_failure_reasons": fetch_failure_reasons,
        "stale_symbol_details": stale_symbol_details,
        "skip_reasons": skip_reasons,
        "skip_symbol_samples": skip_symbol_samples,
        "recompute_existing": 1 if recompute_existing else 0,
    }


def backfill_recent_decision_outcomes(
    store: Store,
    *,
    days: int = DEFAULT_INCREMENTAL_OUTCOME_BACKFILL_DAYS,
    limit: int = 0,
    fetch_bars: Callable[[str, str, int], list[Bar]] | None = None,
    recompute_existing: bool = True,
) -> dict[str, int]:
    since = (utcnow() - timedelta(days=days)).isoformat()
    payload = backfill_decision_outcomes(
        store,
        since=since,
        limit=limit,
        fetch_bars=fetch_bars,
        recompute_existing=recompute_existing,
    )
    payload["days"] = days
    payload["since"] = since
    return payload
