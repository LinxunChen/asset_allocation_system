from __future__ import annotations

from collections import defaultdict
from typing import Any


THEME_DISPLAY_NAMES: dict[str, str] = {
    "core_platforms": "核心平台科技",
    "semiconductors_and_ai": "半导体与AI",
    "software_and_cloud": "软件与云",
    "financials_and_fintech": "金融与金融科技",
    "healthcare": "医疗健康",
    "international_and_consumer": "国际与消费",
    "special_situations": "特殊题材",
    "core_index": "核心指数ETF",
    "leveraged_and_tactical": "杠杆与战术ETF",
    "macro_and_alternatives": "宏观与另类资产",
    "non_us_markets": "非美市场",
    "sectors_and_themes": "行业与主题ETF",
}


def display_theme_name(theme_key: str) -> str:
    if not theme_key:
        return ""
    return THEME_DISPLAY_NAMES.get(theme_key, theme_key.replace("_", " ").title())


def build_symbol_theme_map_from_watchlist_payload(watchlist_payload: dict[str, Any] | None) -> dict[str, list[str]]:
    payload = watchlist_payload or {}
    grouped_sources = [
        payload.get("stock_groups", {}),
        payload.get("etf_groups", {}),
    ]
    mapping: dict[str, list[str]] = {}
    for groups in grouped_sources:
        if not isinstance(groups, dict):
            continue
        for theme_key, symbols in groups.items():
            if not isinstance(symbols, list):
                continue
            for raw_symbol in symbols:
                symbol = str(raw_symbol).strip().upper()
                if not symbol:
                    continue
                bucket = mapping.setdefault(symbol, [])
                if theme_key not in bucket:
                    bucket.append(str(theme_key))
    return mapping


def theme_tags_for_symbol(symbol: str, symbol_theme_map: dict[str, list[str]]) -> list[str]:
    return [display_theme_name(theme_key) for theme_key in symbol_theme_map.get(symbol.upper(), [])]


def build_theme_memberships(symbol_theme_map: dict[str, list[str]]) -> dict[str, set[str]]:
    memberships: dict[str, set[str]] = defaultdict(set)
    for symbol, theme_keys in symbol_theme_map.items():
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            continue
        for theme_key in theme_keys:
            memberships[str(theme_key)].add(normalized_symbol)
    return memberships


def summarize_symbol_theme_context(
    symbol: str,
    *,
    symbol_theme_map: dict[str, list[str]],
    theme_memberships: dict[str, set[str]] | None = None,
    confirmed_symbols: set[str] | None = None,
    prewatch_symbols: set[str] | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper()
    memberships = theme_memberships or build_theme_memberships(symbol_theme_map)
    confirmed = {item.upper() for item in (confirmed_symbols or set())}
    prewatch = {item.upper() for item in (prewatch_symbols or set())}
    theme_keys = list(symbol_theme_map.get(normalized_symbol, []))
    peer_symbols: set[str] = set()
    confirmed_peer_symbols: set[str] = set()
    prewatch_peer_symbols: set[str] = set()
    for theme_key in theme_keys:
        theme_members = memberships.get(theme_key, set())
        peers = {item for item in theme_members if item != normalized_symbol}
        peer_symbols.update(peers)
        confirmed_peer_symbols.update(item for item in peers if item in confirmed)
        prewatch_peer_symbols.update(item for item in peers if item in prewatch)
    return {
        "theme_keys": theme_keys,
        "peer_symbols": sorted(peer_symbols),
        "confirmed_peer_symbols": sorted(confirmed_peer_symbols),
        "prewatch_peer_symbols": sorted(prewatch_peer_symbols),
        "peer_count": len(peer_symbols),
        "confirmed_peer_count": len(confirmed_peer_symbols),
        "prewatch_peer_count": len(prewatch_peer_symbols),
    }


def build_theme_snapshot_rows(
    *,
    symbol_theme_map: dict[str, list[str]],
    card_diagnostics: list[dict[str, Any]],
    prewatch_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    theme_rows: dict[str, dict[str, Any]] = {}
    prewatch_by_symbol = {str(row.get("symbol", "")).upper() for row in prewatch_candidates}

    def ensure_theme_row(theme_key: str) -> dict[str, Any]:
        row = theme_rows.get(theme_key)
        if row is None:
            row = {
                "theme_key": theme_key,
                "theme_name": display_theme_name(theme_key),
                "confirmed_symbols": set(),
                "promoted_symbols": set(),
                "sent_symbols": set(),
                "prewatch_symbols": set(),
                "high_priority_symbols": set(),
            }
            theme_rows[theme_key] = row
        return row

    for row in card_diagnostics:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        theme_keys = symbol_theme_map.get(symbol, [])
        if not theme_keys:
            continue
        is_confirmed = bool(row.get("priority")) and row.get("priority") != "suppressed"
        for theme_key in theme_keys:
            bucket = ensure_theme_row(theme_key)
            if is_confirmed:
                bucket["confirmed_symbols"].add(symbol)
            if row.get("promoted_from_prewatch"):
                bucket["promoted_symbols"].add(symbol)
            if row.get("sent"):
                bucket["sent_symbols"].add(symbol)
            if row.get("priority") == "high":
                bucket["high_priority_symbols"].add(symbol)

    for row in prewatch_candidates:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        for theme_key in symbol_theme_map.get(symbol, []):
            bucket = ensure_theme_row(theme_key)
            bucket["prewatch_symbols"].add(symbol)

    ranked: list[dict[str, Any]] = []
    for row in theme_rows.values():
        confirmed = sorted(row["confirmed_symbols"])
        promoted = sorted(row["promoted_symbols"])
        sent = sorted(row["sent_symbols"])
        prewatch = sorted(row["prewatch_symbols"])
        high_priority = sorted(row["high_priority_symbols"])
        heat_score = len(promoted) * 3 + len(sent) * 2 + len(confirmed) * 2 + len(prewatch)
        if confirmed and prewatch:
            chain_note = "已有确认标的，同时同题材仍有预备池候选，适合跟踪扩散。"
        elif confirmed:
            chain_note = "已有确认信号，可回看同题材是否存在补涨机会。"
        elif len(prewatch) >= 2:
            chain_note = "同题材多标的进入预备池，题材预热正在形成。"
        else:
            chain_note = "暂处于单点预热阶段，适合继续观察是否形成共振。"
        ranked.append(
            {
                "theme_key": row["theme_key"],
                "theme_name": row["theme_name"],
                "heat_score": heat_score,
                "confirmed_symbols": confirmed,
                "promoted_symbols": promoted,
                "sent_symbols": sent,
                "prewatch_symbols": prewatch,
                "high_priority_symbols": high_priority,
                "prewatch_only_symbols": [symbol for symbol in prewatch if symbol not in confirmed],
                "chain_note": chain_note,
            }
        )
    ranked.sort(
        key=lambda row: (
            -row["heat_score"],
            -len(row["confirmed_symbols"]),
            -len(row["prewatch_symbols"]),
            row["theme_name"],
        )
    )
    return ranked


def build_prewatch_peer_map(
    candidates: list[dict[str, Any]],
    symbol_theme_map: dict[str, list[str]],
) -> dict[str, list[str]]:
    theme_to_symbols: dict[str, list[str]] = defaultdict(list)
    for row in candidates:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        for theme_key in symbol_theme_map.get(symbol, []):
            if symbol not in theme_to_symbols[theme_key]:
                theme_to_symbols[theme_key].append(symbol)
    peer_map: dict[str, list[str]] = {}
    for row in candidates:
        symbol = str(row.get("symbol", "")).upper()
        peers: list[str] = []
        for theme_key in symbol_theme_map.get(symbol, []):
            for peer in theme_to_symbols.get(theme_key, []):
                if peer != symbol and peer not in peers:
                    peers.append(peer)
        if peers:
            peer_map[symbol] = peers
    return peer_map
