from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from ..config import Settings
from ..models import OpportunityCard, PrewatchCandidate, utcnow
from ..store import Store
from ..theme_linkage import (
    build_theme_display_name_map_from_watchlist_payload,
    build_symbol_theme_map_from_watchlist_payload,
    build_theme_memberships,
    display_theme_name,
    summarize_symbol_theme_context,
    theme_tags_for_symbol,
)
from .types import ThemeAssessment

THEME_MEMORY_STATE_KEY = "theme_heat_memory"


class ThemeUnderstandingEngine(Protocol):
    def assess_confirmation(self, card: OpportunityCard, *, confirmed_symbols: set[str]) -> ThemeAssessment:
        ...

    def assess_prewatch(self, candidate: PrewatchCandidate, *, confirmed_symbols: set[str], prewatch_symbols: set[str]) -> ThemeAssessment:
        ...

    def load_recent_theme_memory(self) -> dict[str, dict]:
        ...

    def persist_theme_memory(self, confirmation_cards: list[OpportunityCard], prewatch_candidates: list[PrewatchCandidate]) -> None:
        ...


@dataclass
class StaticThemeUnderstandingEngine:
    symbol_theme_map: dict[str, list[str]]
    theme_memberships: dict[str, set[str]]
    theme_display_name_map: dict[str, str]

    @classmethod
    def from_watchlist_payload(cls, watchlist_payload: dict | None) -> "StaticThemeUnderstandingEngine":
        symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(watchlist_payload)
        return cls(
            symbol_theme_map=symbol_theme_map,
            theme_memberships=build_theme_memberships(symbol_theme_map),
            theme_display_name_map=build_theme_display_name_map_from_watchlist_payload(watchlist_payload),
        )

    def assess_confirmation(self, card: OpportunityCard, *, confirmed_symbols: set[str]) -> ThemeAssessment:
        context = summarize_symbol_theme_context(
            card.symbol,
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
            confirmed_symbols=confirmed_symbols,
        )
        theme_ids = list(self.symbol_theme_map.get(card.symbol.upper(), []))
        boost = 1.5 if context["confirmed_peer_symbols"] else 0.0
        if context["confirmed_peer_symbols"]:
            note = f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}"
            role = "chain_confirmed"
        elif context["peer_symbols"]:
            note = "同题材存在其他跟踪标的，暂未形成确认链。"
            role = "theme_member"
        else:
            note = ""
            role = "standalone"
        return ThemeAssessment(
            theme_ids=theme_tags_for_symbol(card.symbol, self.symbol_theme_map, self.theme_display_name_map),
            theme_heat=float(len(context["confirmed_peer_symbols"]) * 2 + len(context["peer_symbols"])),
            theme_role=role,
            confirmed_peers=list(context["confirmed_peer_symbols"]),
            prewatch_peers=[],
            dynamic_theme_detected=False,
            theme_chain_note=note,
            theme_boosts={"confirmation_chain": boost} if boost else {},
        )

    def assess_prewatch(self, candidate: PrewatchCandidate, *, confirmed_symbols: set[str], prewatch_symbols: set[str]) -> ThemeAssessment:
        context = summarize_symbol_theme_context(
            candidate.symbol,
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
            confirmed_symbols=confirmed_symbols,
            prewatch_symbols=prewatch_symbols,
        )
        note = ""
        role = "standalone"
        boosts: dict[str, float] = {}
        if context["confirmed_peer_symbols"]:
            note = f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}"
            role = "follow_on"
            boosts["confirmed_peer"] = 4.0
        elif context["prewatch_peer_symbols"]:
            note = f"同题材预热共振：{', '.join(context['prewatch_peer_symbols'][:3])}"
            role = "clustered"
            boosts["prewatch_cluster"] = 2.5
        return ThemeAssessment(
            theme_ids=theme_tags_for_symbol(candidate.symbol, self.symbol_theme_map, self.theme_display_name_map),
            theme_heat=float(len(context["confirmed_peer_symbols"]) * 2 + len(context["prewatch_peer_symbols"])),
            theme_role=role,
            confirmed_peers=list(context["confirmed_peer_symbols"]),
            prewatch_peers=list(context["prewatch_peer_symbols"]),
            dynamic_theme_detected=False,
            theme_chain_note=note,
            theme_boosts=boosts,
        )

    def load_recent_theme_memory(self) -> dict[str, dict]:
        return {}

    def persist_theme_memory(self, confirmation_cards: list[OpportunityCard], prewatch_candidates: list[PrewatchCandidate]) -> None:
        return


@dataclass
class HybridThemeUnderstandingEngine(StaticThemeUnderstandingEngine):
    store: Store | None = None
    settings: Settings | None = None

    def load_recent_theme_memory(self) -> dict[str, dict]:
        if self.store is None:
            return {}
        raw = self.store.get_state(THEME_MEMORY_STATE_KEY)
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            as_of = datetime.fromisoformat(payload["as_of"])
            rows = payload.get("themes", [])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return {}
        window_hours = max(getattr(self.settings, "prewatch_theme_memory_window_hours", 0), 0)
        if window_hours and (utcnow() - as_of).total_seconds() > window_hours * 3600:
            return {}
        memory: dict[str, dict] = {}
        for row in rows:
            theme_key = str(row.get("theme_key", "")).strip()
            if not theme_key:
                continue
            memory[theme_key] = {
                "heat_score": float(row.get("heat_score", 0.0)),
                "confirmed_symbols": list(row.get("confirmed_symbols", [])),
                "prewatch_symbols": list(row.get("prewatch_symbols", [])),
            }
        return memory

    def assess_prewatch(self, candidate: PrewatchCandidate, *, confirmed_symbols: set[str], prewatch_symbols: set[str]) -> ThemeAssessment:
        assessment = super().assess_prewatch(
            candidate,
            confirmed_symbols=confirmed_symbols,
            prewatch_symbols=prewatch_symbols,
        )
        recent = self.load_recent_theme_memory()
        strongest_key = ""
        strongest_heat = 0.0
        for theme_key in self.symbol_theme_map.get(candidate.symbol.upper(), []):
            heat_score = float(recent.get(theme_key, {}).get("heat_score", 0.0))
            if heat_score > strongest_heat:
                strongest_heat = heat_score
                strongest_key = theme_key
        if strongest_heat <= 0:
            return assessment
        boosts = dict(assessment.theme_boosts)
        boosts["recent_theme_heat"] = min(
            strongest_heat,
            getattr(self.settings, "prewatch_theme_memory_bonus", 3.0),
        )
        note = assessment.theme_chain_note or f"题材近期持续活跃：{display_theme_name(strongest_key, self.theme_display_name_map)}"
        return ThemeAssessment(
            theme_ids=assessment.theme_ids,
            theme_heat=assessment.theme_heat + strongest_heat,
            theme_role=assessment.theme_role if assessment.theme_role != "standalone" else "memory_supported",
            confirmed_peers=assessment.confirmed_peers,
            prewatch_peers=assessment.prewatch_peers,
            dynamic_theme_detected=strongest_heat >= getattr(self.settings, "prewatch_theme_memory_min_heat_score", 4.0),
            theme_chain_note=note,
            theme_boosts=boosts,
        )

    def persist_theme_memory(self, confirmation_cards: list[OpportunityCard], prewatch_candidates: list[PrewatchCandidate]) -> None:
        if self.store is None:
            return
        theme_confirmed: dict[str, set[str]] = {}
        theme_promoted: dict[str, set[str]] = {}
        theme_prewatch: dict[str, set[str]] = {}
        for card in confirmation_cards:
            if card.priority == "suppressed":
                continue
            symbol = card.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_confirmed.setdefault(theme_key, set()).add(symbol)
                if card.promoted_from_prewatch:
                    theme_promoted.setdefault(theme_key, set()).add(symbol)
        for candidate in prewatch_candidates:
            symbol = candidate.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_prewatch.setdefault(theme_key, set()).add(symbol)
        rows = []
        all_theme_keys = set(theme_confirmed) | set(theme_promoted) | set(theme_prewatch)
        for theme_key in sorted(all_theme_keys):
            confirmed_symbols = sorted(theme_confirmed.get(theme_key, set()))
            promoted_symbols = sorted(theme_promoted.get(theme_key, set()))
            prewatch_symbols = sorted(theme_prewatch.get(theme_key, set()))
            heat_score = len(promoted_symbols) * 3 + len(confirmed_symbols) * 2 + len(prewatch_symbols)
            rows.append(
                {
                    "theme_key": theme_key,
                    "heat_score": float(heat_score),
                    "confirmed_symbols": confirmed_symbols,
                    "prewatch_symbols": prewatch_symbols,
                }
            )
        self.store.set_state(
            THEME_MEMORY_STATE_KEY,
            json.dumps({"as_of": utcnow().isoformat(), "themes": rows}, sort_keys=True),
        )
