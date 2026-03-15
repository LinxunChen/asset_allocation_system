from __future__ import annotations

from .models import IndicatorSnapshot, OpportunityCard, PriceRange


class EntryExitEngine:
    def enrich(self, card: OpportunityCard, snapshot: IndicatorSnapshot) -> OpportunityCard:
        atr = max(snapshot.atr_14, 0.01)
        last = snapshot.last_price
        if card.bias == "long":
            entry = self._long_entry(card.horizon, snapshot, last, atr)
            take_profit = self._long_take_profit(card.horizon, snapshot, last, atr)
            invalidation = round(max(0.01, entry.low - (0.75 * atr)), 2)
            invalidation_reason = "若价格跌破事件触发后的关键支撑位，做多逻辑失效。"
        else:
            entry = self._short_entry(card.horizon, snapshot, last, atr)
            take_profit = self._short_take_profit(card.horizon, snapshot, last, atr)
            invalidation = round(entry.high + (0.75 * atr), 2)
            invalidation_reason = "若价格重新站回触发阻力位上方，做空逻辑失效。"
        card.entry_range = entry.normalized()
        card.take_profit_range = take_profit.normalized()
        card.invalidation_level = invalidation
        card.invalidation_reason = invalidation_reason
        return card

    def _long_entry(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        anchor = snapshot.support_20 if horizon == "swing" else max(snapshot.support_60, snapshot.sma_20)
        if snapshot.intraday_breakout:
            return PriceRange(last - (0.25 * atr), last + (0.35 * atr))
        if snapshot.is_pullback:
            return PriceRange(max(anchor, last - (0.5 * atr)), last + (0.2 * atr))
        return PriceRange(max(anchor, last - atr), last + (0.15 * atr))

    def _long_take_profit(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        if horizon == "swing":
            target_high = min(snapshot.resistance_20 + (0.5 * atr), last + (2.0 * atr))
            return PriceRange(last + (1.0 * atr), target_high)
        target_high = max(snapshot.resistance_60, last + (3.0 * atr))
        return PriceRange(last + (1.5 * atr), target_high)

    def _short_entry(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        anchor = snapshot.resistance_20 if horizon == "swing" else min(snapshot.resistance_60, snapshot.sma_20 or snapshot.resistance_20)
        return PriceRange(last - (0.2 * atr), max(anchor, last + (0.5 * atr)))

    def _short_take_profit(self, horizon: str, snapshot: IndicatorSnapshot, last: float, atr: float) -> PriceRange:
        support = snapshot.support_20 if horizon == "swing" else snapshot.support_60
        return PriceRange(max(0.01, last - (2.0 * atr)), max(0.01, support - (0.5 * atr)))
