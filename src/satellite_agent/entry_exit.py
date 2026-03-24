from __future__ import annotations

from .models import IndicatorSnapshot, OpportunityCard, PriceRange


class EntryExitEngine:
    def enrich(self, card: OpportunityCard, snapshot: IndicatorSnapshot) -> OpportunityCard:
        atr = max(snapshot.atr_14, 0.01)
        last = snapshot.last_price
        buffer = self._invalidation_buffer(card.horizon, last, atr)
        if card.bias == "long":
            entry = self._long_entry(card.horizon, snapshot, last, atr).normalized()
            invalidation = round(max(0.01, entry.low - buffer), 2)
            take_profit, execution_eligible, execution_note = self._long_take_profit(
                card.horizon,
                snapshot,
                atr=atr,
                entry_reference=entry.high,
                invalidation_level=invalidation,
            )
            invalidation_reason = "若价格跌破事件触发后的关键支撑位，做多逻辑失效。"
        else:
            entry = self._short_entry(card.horizon, snapshot, last, atr).normalized()
            take_profit = self._short_take_profit(card.horizon, snapshot, last, atr)
            invalidation = round(entry.high + buffer, 2)
            invalidation_reason = "若价格重新站回触发阻力位上方，做空逻辑失效。"
            execution_eligible = True
            execution_note = ""
        card.entry_range = entry
        card.take_profit_range = take_profit.normalized()
        card.invalidation_level = invalidation
        card.invalidation_reason = invalidation_reason
        card.execution_eligible = execution_eligible
        card.execution_note = execution_note
        if card.bias == "long" and not execution_eligible:
            card.priority = "suppressed"
            card.action_label = "加入观察"
            card.confidence_label = "低"
            card.confidence_score = round(min(card.confidence_score or card.final_score, 59.0), 2)
            if execution_note:
                if execution_note not in card.risk_notes:
                    card.risk_notes.append(execution_note)
                card.positioning_hint = execution_note
        return card

    def _invalidation_buffer(self, horizon: str, last: float, atr: float) -> float:
        floor_pct = 0.015 if horizon == "swing" else 0.02
        return max(0.75 * atr, last * floor_pct)

    def _long_entry(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        anchor = snapshot.support_20 if horizon == "swing" else max(snapshot.support_60, snapshot.sma_20)
        if snapshot.intraday_breakout:
            return PriceRange(last - (0.25 * atr), last + (0.35 * atr))
        if snapshot.is_pullback:
            return PriceRange(max(anchor, last - (0.5 * atr)), last + (0.2 * atr))
        return PriceRange(max(anchor, last - atr), last + (0.15 * atr))

    def _long_take_profit(
        self,
        horizon: str,
        snapshot: IndicatorSnapshot,
        *,
        atr: float,
        entry_reference: float,
        invalidation_level: float,
    ) -> tuple[PriceRange, bool, str]:
        risk_unit = round(max(entry_reference - invalidation_level, 0.01), 4)
        if horizon == "swing":
            target_low_multiple = 2.0
            target_high_multiple = 3.0
            min_r_multiple = 1.5
            resistance = snapshot.resistance_20
        else:
            target_low_multiple = 2.5
            target_high_multiple = 4.0
            min_r_multiple = 2.0
            resistance = snapshot.resistance_60

        theoretical_low = entry_reference + (target_low_multiple * risk_unit)
        theoretical_high = entry_reference + (target_high_multiple * risk_unit)
        minimum_low = entry_reference + (min_r_multiple * risk_unit)
        minimum_width = max(0.5 * risk_unit, 0.35 * atr, 0.5)

        corrected_low = theoretical_low
        corrected_high = max(theoretical_high, theoretical_low + minimum_width)
        execution_eligible = True
        execution_note = ""

        if resistance and resistance > entry_reference:
            if resistance < minimum_low:
                execution_eligible = False
                corrected_low = minimum_low
                corrected_high = max(theoretical_high, corrected_low + minimum_width)
            elif resistance < theoretical_low:
                corrected_low = max(minimum_low, resistance)
                corrected_high = max(theoretical_high, corrected_low + minimum_width)
            elif resistance < theoretical_high:
                corrected_low = (0.8 * theoretical_low) + (0.2 * resistance)
                corrected_high = max(
                    corrected_low + minimum_width,
                    (0.45 * theoretical_high) + (0.55 * resistance),
                )

        if corrected_low < minimum_low:
            execution_eligible = False
            corrected_low = minimum_low
            corrected_high = max(corrected_high, corrected_low + minimum_width)

        corrected_low = round(max(corrected_low, entry_reference + 0.01), 2)
        corrected_high = round(max(corrected_high, corrected_low + 0.01), 2)
        if not execution_eligible:
            execution_note = (
                f"当前止盈空间折算后不足 {min_r_multiple:.1f}R，赔率不合格，先降级为观察。"
            )
        return PriceRange(corrected_low, corrected_high), execution_eligible, execution_note

    def _short_entry(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        anchor = snapshot.resistance_20 if horizon == "swing" else min(snapshot.resistance_60, snapshot.sma_20 or snapshot.resistance_20)
        return PriceRange(last - (0.2 * atr), max(anchor, last + (0.5 * atr)))

    def _short_take_profit(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        support = snapshot.support_20 if horizon == "swing" else snapshot.support_60
        return PriceRange(max(0.01, last - (2.0 * atr)), max(0.01, support - (0.5 * atr)))
