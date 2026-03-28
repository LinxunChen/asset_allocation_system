from __future__ import annotations

from collections import defaultdict
from typing import Any


THEME_DISPLAY_NAMES: dict[str, str] = {
    "semiconductors_and_ai": "AI芯片与半导体设备",
    "software_and_cloud": "AI软件与大模型应用",
    "data_center": "数据中心基建与算力网络",
    "consumer_and_devices": "消费电子与硬件终端",
    "automotive_and_mobility": "电动车与智能出行",
    "financials_and_fintech": "金融科技与数字支付",
    "healthcare": "医疗健康与生物制药",
    "digital_health": "数字医疗与健康消费",
    "biotech_and_pharma": "生物技术与创新药",
    "premium_consumer": "高端消费与奢侈品",
    "em_internet": "新兴市场互联网与电商",
    "international_and_consumer": "平台、社交与消费互联网",
    "digital_assets": "数字资产",
    "innovation_and_growth": "高成长科技",
    "space_and_defense": "国防军工与航空航天",
    "special_situations": "特殊题材",
    "core_index": "核心指数ETF",
    "leveraged_and_tactical": "杠杆与战术ETF",
    "macro_and_alternatives": "宏观与另类资产",
    "non_us_markets": "非美市场",
    "sectors_and_themes": "行业与主题ETF",
}

DEFAULT_PRIMARY_THEME_BY_SYMBOL: dict[str, str] = {
    "AAPL": "consumer_and_devices",
    "ADBE": "software_and_cloud",
    "AMAT": "semiconductors_and_ai",
    "AMD": "semiconductors_and_ai",
    "AMZN": "software_and_cloud",
    "ARM": "semiconductors_and_ai",
    "ARKK": "innovation_and_growth",
    "ASML": "semiconductors_and_ai",
    "AVGO": "semiconductors_and_ai",
    "AXP": "financials_and_fintech",
    "BBJP": "non_us_markets",
    "BLK": "financials_and_fintech",
    "CRM": "software_and_cloud",
    "DDOG": "software_and_cloud",
    "EMXC": "non_us_markets",
    "ETHA": "digital_assets",
    "EWY": "non_us_markets",
    "FRDM": "non_us_markets",
    "GOOG": "software_and_cloud",
    "GS": "financials_and_fintech",
    "HIMS": "digital_health",
    "HOOD": "financials_and_fintech",
    "IBIT": "digital_assets",
    "ISRG": "healthcare",
    "JPM": "financials_and_fintech",
    "KLAC": "semiconductors_and_ai",
    "LLY": "healthcare",
    "LRCX": "semiconductors_and_ai",
    "LVMHF": "premium_consumer",
    "MA": "financials_and_fintech",
    "MDB": "software_and_cloud",
    "MELI": "international_and_consumer",
    "META": "software_and_cloud",
    "MRVL": "semiconductors_and_ai",
    "MS": "financials_and_fintech",
    "MSFT": "software_and_cloud",
    "MU": "semiconductors_and_ai",
    "NBIS": "data_center",
    "NET": "software_and_cloud",
    "NOW": "software_and_cloud",
    "NVDA": "semiconductors_and_ai",
    "ORCL": "software_and_cloud",
    "PLTR": "software_and_cloud",
    "QCOM": "semiconductors_and_ai",
    "RKLB": "space_and_defense",
    "SCHW": "financials_and_fintech",
    "SE": "em_internet",
    "SMH": "semiconductors_and_ai",
    "SNOW": "software_and_cloud",
    "TCEHY": "em_internet",
    "TSLA": "automotive_and_mobility",
    "TSM": "semiconductors_and_ai",
    "UNH": "healthcare",
    "VNM": "non_us_markets",
    "VRTX": "biotech_and_pharma",
    "XBI": "healthcare",
}


def build_theme_display_name_map_from_watchlist_payload(watchlist_payload: dict[str, Any] | None) -> dict[str, str]:
    payload = watchlist_payload or {}
    themes = payload.get("themes", [])
    if not isinstance(themes, list):
        return dict(THEME_DISPLAY_NAMES)
    mapping: dict[str, str] = {}
    for raw in themes:
        if not isinstance(raw, dict):
            continue
        theme_id = str(raw.get("theme_id", "")).strip()
        display_name = str(raw.get("display_name", "")).strip()
        if theme_id and display_name:
            mapping[theme_id] = display_name
    return mapping or dict(THEME_DISPLAY_NAMES)


def _active_symbols_from_watchlist_payload(payload: dict[str, Any]) -> set[str]:
    active: set[str] = set()
    for key in ("stocks", "etfs"):
        values = payload.get(key, [])
        if isinstance(values, list):
            for raw_symbol in values:
                symbol = str(raw_symbol).strip().upper()
                if symbol:
                    active.add(symbol)
    for key in ("stock_items", "etf_items"):
        values = payload.get(key, {})
        if not isinstance(values, dict):
            continue
        for raw_symbol, raw_value in values.items():
            item = raw_value if isinstance(raw_value, dict) else {"symbol": raw_symbol}
            symbol = str(item.get("symbol", raw_symbol)).strip().upper()
            enabled = bool(item.get("enabled", True))
            if not symbol:
                continue
            if enabled:
                active.add(symbol)
            elif symbol in active:
                active.remove(symbol)
    return active


def display_theme_name(theme_key: str, theme_display_name_map: dict[str, str] | None = None) -> str:
    if not theme_key:
        return ""
    if theme_display_name_map and theme_key in theme_display_name_map:
        return theme_display_name_map[theme_key]
    return THEME_DISPLAY_NAMES.get(theme_key, theme_key.replace("_", " ").title())


def build_default_symbol_theme_map(active_symbols: set[str] | list[str]) -> dict[str, list[str]]:
    active = {str(symbol).strip().upper() for symbol in active_symbols if str(symbol).strip()}
    mapping: dict[str, list[str]] = {}
    if not active:
        return mapping
    for symbol in sorted(active):
        theme_key = DEFAULT_PRIMARY_THEME_BY_SYMBOL.get(symbol)
        if not theme_key:
            continue
        mapping[symbol] = [theme_key]
    return mapping


def build_symbol_theme_map_from_watchlist_payload(watchlist_payload: dict[str, Any] | None) -> dict[str, list[str]]:
    payload = watchlist_payload or {}
    active_symbols = _active_symbols_from_watchlist_payload(payload)
    if isinstance(payload.get("themes"), list):
        mapping: dict[str, list[str]] = {}
        for raw_theme in payload.get("themes", []):
            if not isinstance(raw_theme, dict):
                continue
            theme_key = str(raw_theme.get("theme_id", "")).strip()
            if not theme_key:
                continue
            members = list(raw_theme.get("symbols", [])) + list(raw_theme.get("etfs", []))
            for raw_symbol in members:
                symbol = str(raw_symbol).strip().upper()
                if not symbol:
                    continue
                if active_symbols and symbol not in active_symbols:
                    continue
                bucket = mapping.setdefault(symbol, [])
                if theme_key not in bucket:
                    bucket.append(theme_key)
        if mapping:
            return mapping
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
                if active_symbols and symbol not in active_symbols:
                    continue
                bucket = mapping.setdefault(symbol, [])
                if theme_key not in bucket:
                    bucket.append(str(theme_key))
    if mapping:
        return mapping
    default_mapping = build_default_symbol_theme_map(active_symbols)
    if default_mapping:
        return default_mapping
    return mapping


def theme_tags_for_symbol(
    symbol: str,
    symbol_theme_map: dict[str, list[str]],
    theme_display_name_map: dict[str, str] | None = None,
) -> list[str]:
    return [
        display_theme_name(theme_key, theme_display_name_map)
        for theme_key in symbol_theme_map.get(symbol.upper(), [])
    ]


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
    candidate_symbols: set[str] | None = None,
    prewatch_symbols: set[str] | None = None,
) -> dict[str, Any]:
    normalized_symbol = symbol.upper()
    memberships = theme_memberships or build_theme_memberships(symbol_theme_map)
    confirmed = {item.upper() for item in (confirmed_symbols or set())}
    candidate_pool = {
        item.upper() for item in (candidate_symbols if candidate_symbols is not None else (prewatch_symbols or set()))
    }
    theme_keys = list(symbol_theme_map.get(normalized_symbol, []))
    peer_symbols: set[str] = set()
    confirmed_peer_symbols: set[str] = set()
    candidate_peer_symbols: set[str] = set()
    for theme_key in theme_keys:
        theme_members = memberships.get(theme_key, set())
        peers = {item for item in theme_members if item != normalized_symbol}
        peer_symbols.update(peers)
        confirmed_peer_symbols.update(item for item in peers if item in confirmed)
        candidate_peer_symbols.update(item for item in peers if item in candidate_pool)
    return {
        "theme_keys": theme_keys,
        "peer_symbols": sorted(peer_symbols),
        "confirmed_peer_symbols": sorted(confirmed_peer_symbols),
        "candidate_peer_symbols": sorted(candidate_peer_symbols),
        "candidate_pool_peer_symbols": sorted(candidate_peer_symbols),
        "prewatch_peer_symbols": sorted(candidate_peer_symbols),
        "peer_count": len(peer_symbols),
        "confirmed_peer_count": len(confirmed_peer_symbols),
        "candidate_peer_count": len(candidate_peer_symbols),
        "candidate_pool_peer_count": len(candidate_peer_symbols),
        "prewatch_peer_count": len(candidate_peer_symbols),
    }


def build_theme_snapshot_rows(
    *,
    symbol_theme_map: dict[str, list[str]],
    theme_display_name_map: dict[str, str] | None = None,
    card_diagnostics: list[dict[str, Any]],
    prewatch_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    theme_rows: dict[str, dict[str, Any]] = {}
    candidate_pool_candidates = prewatch_candidates

    def ensure_theme_row(theme_key: str) -> dict[str, Any]:
        row = theme_rows.get(theme_key)
        if row is None:
            row = {
                "theme_key": theme_key,
                "theme_name": display_theme_name(theme_key, theme_display_name_map),
                "confirmed_symbols": set(),
                "promoted_symbols": set(),
                "sent_symbols": set(),
                "candidate_pool_symbols": set(),
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

    for row in candidate_pool_candidates:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue
        for theme_key in symbol_theme_map.get(symbol, []):
            bucket = ensure_theme_row(theme_key)
            bucket["candidate_pool_symbols"].add(symbol)
            bucket["prewatch_symbols"].add(symbol)

    ranked: list[dict[str, Any]] = []
    for row in theme_rows.values():
        confirmed = sorted(row["confirmed_symbols"])
        promoted = sorted(row["promoted_symbols"])
        sent = sorted(row["sent_symbols"])
        candidate_pool = sorted(row["candidate_pool_symbols"])
        high_priority = sorted(row["high_priority_symbols"])
        heat_score = len(promoted) * 3 + len(sent) * 2 + len(confirmed) * 2 + len(candidate_pool)
        if confirmed and candidate_pool:
            chain_note = "已有确认标的，同时同题材仍有候选池标的，适合跟踪扩散。"
        elif confirmed:
            chain_note = "已有确认信号，可回看同题材是否存在补涨机会。"
        elif len(candidate_pool) >= 2:
            chain_note = "同题材多标的进入候选池，题材预热正在形成。"
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
                "candidate_pool_symbols": candidate_pool,
                "prewatch_symbols": candidate_pool,
                "high_priority_symbols": high_priority,
                "candidate_pool_only_symbols": [symbol for symbol in candidate_pool if symbol not in confirmed],
                "prewatch_only_symbols": [symbol for symbol in candidate_pool if symbol not in confirmed],
                "chain_note": chain_note,
            }
        )
    ranked.sort(
        key=lambda row: (
            -row["heat_score"],
            -len(row["confirmed_symbols"]),
            -len(row["candidate_pool_symbols"]),
            row["theme_name"],
        )
    )
    return ranked


def build_candidate_pool_peer_map(
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


def build_prewatch_peer_map(
    candidates: list[dict[str, Any]],
    symbol_theme_map: dict[str, list[str]],
) -> dict[str, list[str]]:
    return build_candidate_pool_peer_map(candidates, symbol_theme_map)
