from __future__ import annotations

from .config import HorizonSettings
from .models import IndicatorSnapshot, PrewatchCandidate


def evaluate_prewatch_snapshot(
    snapshot: IndicatorSnapshot,
    horizon_settings: HorizonSettings,
    *,
    min_score: float,
) -> dict[str, object]:
    trend_score = _trend_score(snapshot.trend_state, snapshot.last_price, snapshot.sma_20, snapshot.sma_60)
    volume_score = _volume_score(snapshot.relative_volume)
    structure_score = _structure_score(snapshot)
    momentum_score = _momentum_score(snapshot.rsi_14)
    volatility_score = _volatility_score(snapshot.atr_percent, horizon_settings.atr_percent_ceiling)
    total_score = round(trend_score + volume_score + structure_score + momentum_score + volatility_score, 2)
    setup_type = _setup_type(snapshot)

    rejection_reason = ""
    if snapshot.trend_state == "bearish":
        rejection_reason = "bearish_trend"
    elif snapshot.rsi_14 >= 75.0 and not snapshot.is_pullback and not (
        snapshot.intraday_breakout and snapshot.relative_volume >= 1.8
    ):
        rejection_reason = "overheated_without_breakout"
    elif total_score < min_score:
        rejection_reason = "below_min_score"

    return {
        "eligible": rejection_reason == "",
        "rejection_reason": rejection_reason,
        "setup_type": setup_type,
        "total_score": total_score,
        "trend_score": round(trend_score, 2),
        "volume_score": round(volume_score, 2),
        "structure_score": round(structure_score, 2),
        "momentum_score": round(momentum_score, 2),
        "volatility_score": round(volatility_score, 2),
    }


def build_prewatch_candidate(
    snapshot: IndicatorSnapshot,
    horizon_settings: HorizonSettings,
    *,
    min_score: float,
) -> PrewatchCandidate | None:
    evaluation = evaluate_prewatch_snapshot(
        snapshot,
        horizon_settings,
        min_score=min_score,
    )
    if not bool(evaluation.get("eligible")):
        return None

    trend_score = float(evaluation.get("trend_score") or 0.0)
    volume_score = float(evaluation.get("volume_score") or 0.0)
    structure_score = float(evaluation.get("structure_score") or 0.0)
    momentum_score = float(evaluation.get("momentum_score") or 0.0)
    volatility_score = float(evaluation.get("volatility_score") or 0.0)
    total_score = float(evaluation.get("total_score") or 0.0)
    setup_type = str(evaluation.get("setup_type") or _setup_type(snapshot))
    return PrewatchCandidate(
        symbol=snapshot.symbol,
        horizon=snapshot.horizon,
        setup_type=setup_type,
        score=total_score,
        headline_summary=_headline_summary(snapshot, setup_type),
        action_hint=_action_hint(snapshot, setup_type),
        reason_to_watch=_reason_to_watch(
            snapshot,
            trend_score=trend_score,
            volume_score=volume_score,
            structure_score=structure_score,
            momentum_score=momentum_score,
            volatility_score=volatility_score,
        ),
        last_price=round(snapshot.last_price, 2),
        rsi_14=round(snapshot.rsi_14, 1),
        relative_volume=round(snapshot.relative_volume, 2),
        trend_state=snapshot.trend_state,
        support_20=round(snapshot.support_20, 2),
        resistance_20=round(snapshot.resistance_20, 2),
        as_of=snapshot.as_of,
    )


def sort_prewatch_candidates(
    candidates: list[PrewatchCandidate],
    *,
    max_candidates: int,
) -> list[PrewatchCandidate]:
    ranked = sorted(
        candidates,
        key=lambda item: (
            -item.score,
            item.setup_type != "breakout_watch",
            item.symbol,
        ),
    )
    if max_candidates <= 0:
        return ranked
    return ranked[:max_candidates]


def _trend_score(trend_state: str, last_price: float, sma_20: float, sma_60: float) -> float:
    if trend_state == "bullish":
        if last_price >= sma_20 >= sma_60:
            return 32.0
        return 26.0
    if last_price >= sma_20:
        return 16.0
    return 8.0


def _volume_score(relative_volume: float) -> float:
    if relative_volume >= 2.0:
        return 24.0
    if relative_volume >= 1.5:
        return 18.0
    if relative_volume >= 1.2:
        return 12.0
    if relative_volume >= 1.0:
        return 6.0
    return 0.0


def _structure_score(snapshot: IndicatorSnapshot) -> float:
    if snapshot.intraday_breakout and snapshot.last_price >= snapshot.resistance_20:
        return 22.0
    if snapshot.is_pullback and snapshot.trend_state == "bullish":
        return 18.0
    if snapshot.last_price >= snapshot.sma_20 >= snapshot.sma_60:
        return 14.0
    if snapshot.last_price >= snapshot.sma_20:
        return 8.0
    return 0.0


def _momentum_score(rsi_14: float) -> float:
    if 48.0 <= rsi_14 <= 63.0:
        return 14.0
    if 42.0 <= rsi_14 <= 68.0:
        return 10.0
    if 35.0 <= rsi_14 <= 72.0:
        return 5.0
    return 0.0


def _volatility_score(atr_percent: float, atr_percent_ceiling: float) -> float:
    if atr_percent <= atr_percent_ceiling * 0.6:
        return 8.0
    if atr_percent <= atr_percent_ceiling:
        return 6.0
    if atr_percent <= atr_percent_ceiling * 1.25:
        return 3.0
    return 0.0


def _setup_type(snapshot: IndicatorSnapshot) -> str:
    if snapshot.intraday_breakout and snapshot.relative_volume >= 1.2:
        return "breakout_watch"
    if snapshot.is_pullback and snapshot.trend_state == "bullish":
        return "pullback_watch"
    return "relative_strength_watch"


def _headline_summary(snapshot: IndicatorSnapshot, setup_type: str) -> str:
    if setup_type == "breakout_watch":
        return (
            f"{snapshot.symbol} 出现放量突破预热，价格已逼近或站上阶段阻力，适合提前列入重点观察。"
        )
    if setup_type == "pullback_watch":
        return (
            f"{snapshot.symbol} 处于多头结构内的回踩观察区，若承接继续稳定，可作为预备池候选。"
        )
    return (
        f"{snapshot.symbol} 维持相对强势，量价结构正在改善，适合提前跟踪等待正式催化。"
    )


def _action_hint(snapshot: IndicatorSnapshot, setup_type: str) -> str:
    if setup_type == "breakout_watch":
        return "可先列入预备池，等待放量站稳阻力位后再考虑升入确认池。"
    if setup_type == "pullback_watch":
        return "可轻仓观察，优先等待回踩企稳或事件催化进一步确认。"
    return "当前更适合作为预热观察对象，暂不视作正式重仓信号。"


def _reason_to_watch(
    snapshot: IndicatorSnapshot,
    *,
    trend_score: float,
    volume_score: float,
    structure_score: float,
    momentum_score: float,
    volatility_score: float,
) -> str:
    reasons: list[str] = []
    if trend_score >= 26.0:
        reasons.append("维持多头结构")
    elif trend_score > 0:
        reasons.append("趋势尚未转弱")
    if volume_score >= 18.0:
        reasons.append(f"相对量能放大至 {snapshot.relative_volume:.2f} 倍")
    elif volume_score > 0:
        reasons.append(f"量能略有抬升至 {snapshot.relative_volume:.2f} 倍")
    if structure_score >= 22.0:
        reasons.append("价格处于突破预热区")
    elif structure_score >= 18.0:
        reasons.append("价格回踩关键均线附近")
    elif structure_score > 0:
        reasons.append("价格站稳短中期均线")
    if momentum_score >= 10.0:
        reasons.append(f"RSI {snapshot.rsi_14:.1f}，强势但未过热")
    elif momentum_score > 0:
        reasons.append(f"RSI {snapshot.rsi_14:.1f}，动能开始修复")
    if volatility_score >= 6.0:
        reasons.append(f"波动率可控，ATR 占比 {snapshot.atr_percent:.1f}%")
    elif volatility_score > 0:
        reasons.append(f"波动率偏高但仍可跟踪，ATR 占比 {snapshot.atr_percent:.1f}%")
    return "；".join(reasons)
