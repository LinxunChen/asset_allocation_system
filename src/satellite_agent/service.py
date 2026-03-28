from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from .config import Settings
from .cycle_audit import rebuild_cycle_audit
from .decision_engines import (
    DecisionPacket,
    HybridEventUnderstandingEngine,
    HybridThemeUnderstandingEngine,
    RuleMarketUnderstandingEngine,
)
from .decision_engines.mappers import decision_packet_to_card
from .decision_engines.types import PricePlan, ReasonSections, SourceBundle
from .entry_exit import EntryExitEngine
from .event_normalizer import EventNormalizer
from .llm import OpenAIExtractor, RuleBasedExtractor
from .llm import OpenAINarrator
from .market_data import (
    MarketDataEngine,
    MultiSourceMarketDataProvider,
    StooqDailyMarketDataProvider,
    YahooFinanceMarketDataProvider,
)
from .models import (
    CandidatePoolCandidate,
    EventInsight,
    OpportunityCard,
    PriceRange,
    SourceHealthCheck,
    utcnow,
)
from .notifier import Notifier
from .observability import RunContext, StructuredLogger
from .outcomes import compute_decision_outcome, normalize_close_reason
from .prewatch import (
    build_candidate_pool_candidate,
    evaluate_candidate_snapshot,
    sort_candidate_pool_candidates,
)
from .scoring import SignalScorer
from .sources import SourceAdapter
from .store import Store
from .theme_linkage import (
    build_candidate_pool_peer_map,
    build_theme_display_name_map_from_watchlist_payload,
    build_symbol_theme_map_from_watchlist_payload,
    build_theme_memberships,
    display_theme_name,
    summarize_symbol_theme_context,
    theme_tags_for_symbol,
)
from .timefmt import BEIJING_TZ, format_beijing_minute

STRONG_SELECTION_TERMS: tuple[tuple[str, float], ...] = (
    ("earnings", 24.0),
    ("guidance", 22.0),
    ("forecast", 12.0),
    ("outlook", 12.0),
    ("8-k", 24.0),
    ("10-q", 22.0),
    ("10-k", 22.0),
    ("filing", 12.0),
    ("merger", 26.0),
    ("acquisition", 26.0),
    ("buyout", 26.0),
    ("strategic partnership", 26.0),
    ("partnership", 18.0),
    ("collaboration", 18.0),
    ("investment", 22.0),
    ("invests", 22.0),
    ("invested", 22.0),
    ("stake", 20.0),
    ("funding", 18.0),
    ("backed", 18.0),
    ("approval", 22.0),
    ("contract", 18.0),
    ("deal", 14.0),
    ("buyback", 16.0),
    ("dividend", 10.0),
    ("launch", 10.0),
)

LOW_SIGNAL_SELECTION_TERMS: tuple[tuple[str, float], ...] = (
    ("stock price today", -36.0),
    ("price prediction", -34.0),
    ("prediction", -20.0),
    ("top 10", -28.0),
    ("top stocks", -24.0),
    ("best stocks", -22.0),
    ("should you buy", -26.0),
    ("should investors", -22.0),
    ("to buy now", -24.0),
    ("history", -18.0),
    ("battle royale", -28.0),
    ("vs.", -14.0),
    (" versus ", -14.0),
    ("etf of the day", -20.0),
    ("everything you need to know", -24.0),
    ("what you need to know", -20.0),
)

SATELLITE_STRATEGY_VERSION = "trade-loop-v1"

SOURCE_TYPE_SELECTION_WEIGHTS: dict[str, float] = {
    "filing": 28.0,
    "earnings": 26.0,
    "press_release": 18.0,
    "research": 8.0,
    "news": 0.0,
}

THEME_SELECTION_BONUS_PER_PEER = 1.5
THEME_BATCH_CLUSTER_BONUS_PER_SYMBOL = 2.0
THEME_BATCH_CLUSTER_MAX_BONUS = 8.0
THEME_PREWATCH_CLUSTER_BONUS = 2.5
THEME_PREWATCH_CONFIRMED_BONUS = 4.0
THEME_CONFIRMATION_CHAIN_BONUS = 1.5
THEME_MEMORY_STATE_KEY = "theme_heat_memory"
EVENT_PREWATCH_BASE_BONUS = 4.0
MACRO_PROXY_SYMBOLS = ("SPY", "QQQ", "SMH", "TLT")

EXIT_POOL_SUBREASON_DISPLAY: dict[str, str] = {
    "target_hit": "达标止盈",
    "weakening_after_tp_zone": "提前锁盈",
    "macro_protection": "宏观保护",
}

HOLDING_MANAGEMENT_REASON_DISPLAY: dict[str, str] = {
    "profit_protection_exit": "利润保护退出",
    "invalidation_exit": "失效价退出",
    "window_close_evaluation": "窗口到期结算",
    "not_entered_price_invalidated": "失效未成交",
    "not_entered_window_expired": "窗口内未成交",
    "holding_active": "持仓中",
}

FORMAL_ACTION_RANK: dict[str, int] = {
    "加入观察": 0,
    "试探建仓": 1,
    "确认做多": 2,
}

TERMINAL_CYCLE_REASONS = {
    "not_entered_price_invalidated",
    "not_entered_window_expired",
    "profit_protection_exit",
    "invalidation_exit",
    "window_close_evaluation",
}

EVENT_TYPE_DISPLAY_NAMES: dict[str, str] = {
    "earnings": "财报",
    "guidance": "指引",
    "sec": "公告",
    "research": "研报",
    "m&a": "并购",
    "strategic": "战略合作",
    "product": "产品发布",
    "news": "新闻",
}


def _display_horizon(horizon: str) -> str:
    return {"swing": "短线", "position": "波段"}.get(horizon, horizon)


def _display_trend_state(value: str) -> str:
    return {
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "震荡",
        "uptrend": "多头",
        "downtrend": "空头",
    }.get(value, value)


def _relative_volume_label(value: float | None) -> str:
    if value is None:
        return "未识别"
    if value >= 2.2:
        return "异常放量"
    if value >= 1.5:
        return "明显放量"
    if value >= 1.15:
        return "温和放量"
    return "未放量"


def _score_label(score: float | None, *, bands: tuple[float, float, float], labels: tuple[str, str, str, str]) -> str:
    if score is None:
        return "未识别"
    high, medium, low = bands
    if score >= high:
        return labels[0]
    if score >= medium:
        return labels[1]
    if score >= low:
        return labels[2]
    return labels[3]


def _market_regime_display(value: str) -> str:
    return {"risk_on": "风险偏好回升", "neutral": "中性", "risk_off": "风险偏好下降"}.get(value, value or "中性")


def _risk_level_display(value: str) -> str:
    return {"low": "低", "medium": "中", "high": "高"}.get(value, value or "中")


def _load_symbol_display_names(watchlist_payload: dict) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for bucket_key in ("stock_items", "etf_items"):
        bucket = watchlist_payload.get(bucket_key, {})
        if not isinstance(bucket, dict):
            continue
        for symbol, item in bucket.items():
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            normalized = str(symbol).strip().upper()
            if normalized and name:
                mapping[normalized] = name
    return mapping


@dataclass
class PoolRoutingInput:
    source_kind: str
    card: OpportunityCard
    insight: EventInsight
    event_assessment: Any
    market_assessment: Any
    candidate_pool_context: dict[str, Any] | None


class SatelliteAgentService:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        source_adapter: SourceAdapter,
        normalizer: EventNormalizer,
        extractor: RuleBasedExtractor,
        market_data: MarketDataEngine,
        scorer: SignalScorer,
        entry_exit: EntryExitEngine,
        notifier: Notifier,
        runtime_snapshot: dict | None = None,
        run_name: str = "",
        note: str = "",
        prewatch_symbols: list[str] | None = None,
    ) -> None:
        self.settings = settings
        self.store = store
        self.source_adapter = source_adapter
        self.normalizer = normalizer
        self.extractor = extractor
        self.market_data = market_data
        self.scorer = scorer
        self.entry_exit = entry_exit
        self.notifier = notifier
        self.runtime_snapshot = dict(runtime_snapshot or {})
        self.runtime_snapshot.setdefault("strategy_version", SATELLITE_STRATEGY_VERSION)
        self.strategy_version = str(self.runtime_snapshot.get("strategy_version") or SATELLITE_STRATEGY_VERSION)
        self.run_name = run_name
        self.note = note
        self.candidate_pool_symbols = [symbol.upper() for symbol in (prewatch_symbols or [])]
        self.prewatch_symbols = self.candidate_pool_symbols
        runtime_watchlist = self.runtime_snapshot.get("runtime_config", {}).get("watchlist", {})
        self.symbol_display_names = _load_symbol_display_names(runtime_watchlist)
        self.symbol_theme_map = build_symbol_theme_map_from_watchlist_payload(runtime_watchlist)
        self.theme_memberships = build_theme_memberships(self.symbol_theme_map)
        self.theme_display_name_map = build_theme_display_name_map_from_watchlist_payload(runtime_watchlist)
        self.event_engine = HybridEventUnderstandingEngine(
            rule_extractor=RuleBasedExtractor(),
            llm_extractor=extractor if isinstance(extractor, OpenAIExtractor) else None,
        )
        self.market_engine = RuleMarketUnderstandingEngine(scorer=self.scorer, entry_exit=self.entry_exit)
        self.theme_engine = HybridThemeUnderstandingEngine(
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
            theme_display_name_map=self.theme_display_name_map,
            store=self.store,
            settings=self.settings,
        )
        self.narrator = (
            OpenAINarrator(
                api_key=self.settings.openai_api_key,
                model=self.settings.openai_model,
                base_url=self.settings.openai_base_url,
            )
            if self.settings.openai_api_key and self.settings.use_llm_narration
            else None
        )
        self.candidate_pool_market_data = MarketDataEngine(
            MultiSourceMarketDataProvider(
                [
                    YahooFinanceMarketDataProvider(),
                    StooqDailyMarketDataProvider(),
                ]
            )
        )
        self.prewatch_market_data = self.candidate_pool_market_data

    def _display_name_for(self, symbol: str) -> str:
        normalized = str(symbol).strip().upper()
        return self.symbol_display_names.get(normalized, normalized)

    def _candidate_pool_watch_symbols(self) -> list[str]:
        legacy_symbols = getattr(self, "prewatch_symbols", None)
        if isinstance(legacy_symbols, list):
            return [str(symbol).upper() for symbol in legacy_symbols]
        return [str(symbol).upper() for symbol in getattr(self, "candidate_pool_symbols", [])]

    def _candidate_pool_market_data_engine(self) -> MarketDataEngine:
        legacy_engine = getattr(self, "prewatch_market_data", None)
        if isinstance(legacy_engine, MarketDataEngine):
            return legacy_engine
        return self.candidate_pool_market_data

    def _build_macro_context(self) -> dict[str, object]:
        if not self.settings.use_macro_risk_overlay:
            return {
                "market_regime": "neutral",
                "rate_risk": "low",
                "geopolitical_risk": "low",
                "macro_risk_score": 0.0,
            }
        snapshots: dict[str, object] = {}
        scores: list[int] = []
        for symbol in MACRO_PROXY_SYMBOLS:
            try:
                snapshot = self._candidate_pool_market_data_engine().snapshot(symbol, "position", include_intraday=False)
            except Exception:
                continue
            snapshots[symbol] = snapshot
        for symbol in ("SPY", "QQQ", "SMH"):
            snapshot = snapshots.get(symbol)
            if snapshot is None:
                continue
            trend_state = getattr(snapshot, "trend_state", "neutral")
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
        tlt_snapshot = snapshots.get("TLT")
        tlt_trend = getattr(tlt_snapshot, "trend_state", "neutral") if tlt_snapshot is not None else "neutral"
        if tlt_trend == "bearish":
            rate_risk = "high"
        elif tlt_trend == "neutral":
            rate_risk = "medium"
        else:
            rate_risk = "low"
        semis_trend = getattr(snapshots.get("SMH"), "trend_state", "neutral")
        geopolitical_risk = "high" if regime == "risk_off" and semis_trend == "bearish" and tlt_trend == "bullish" else "low"
        macro_risk_score = 0.0
        macro_risk_score += {"risk_on": 10.0, "neutral": 35.0, "risk_off": 70.0}[regime]
        macro_risk_score += {"low": 0.0, "medium": 10.0, "high": 20.0}[rate_risk]
        macro_risk_score += {"low": 0.0, "high": 10.0}.get(geopolitical_risk, 5.0)
        return {
            "market_regime": regime,
            "rate_risk": rate_risk,
            "geopolitical_risk": geopolitical_risk,
            "macro_risk_score": round(min(macro_risk_score, 100.0), 2),
        }

    def _macro_penalty(self, action_label: str, macro_risk_score: float) -> float:
        if macro_risk_score >= 70.0:
            return {"确认做多": 12.0, "试探建仓": 6.0, "加入观察": 2.0}.get(action_label, 0.0)
        if macro_risk_score >= 50.0:
            return {"确认做多": 6.0, "试探建仓": 3.0, "加入观察": 1.0}.get(action_label, 0.0)
        return 0.0

    def _parse_alert_action_label(self, row: Any) -> str:
        card_json_raw = row["card_json"] if "card_json" in row.keys() else ""
        if not card_json_raw:
            return ""
        try:
            card_payload = json.loads(str(card_json_raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ""
        action_label = str(card_payload.get("action_label") or "").strip()
        if action_label:
            return action_label
        priority = str(card_payload.get("priority") or row["priority"] or "").strip()
        market_data_complete = bool(card_payload.get("market_data_complete", True))
        if not market_data_complete:
            return "加入观察"
        return {"high": "确认做多", "normal": "试探建仓", "suppressed": "加入观察"}.get(priority, "")

    def _chain_summary_for_symbol(self, symbol: str, *, current_action: str) -> str:
        since = (utcnow() - timedelta(days=7)).isoformat()
        symbol_upper = symbol.upper()
        rows = [
            row
            for row in self.store.load_decision_records_for_window(since=since)
            if str(row["symbol"]).upper() == symbol_upper
        ]
        if not rows:
            return "首次出现"
        active_cycle: list[dict | object] = []
        for row in rows:
            if normalize_close_reason(
                str(row["close_reason"] or ""),
                exit_subreason=str(row["exit_subreason"] or "") if "exit_subreason" in row.keys() else "",
                entered=bool(row["entered"]) if "entered" in row.keys() else False,
            ) in {"profit_protection_exit", "invalidation_exit", "window_close_evaluation"}:
                active_cycle = []
                continue
            active_cycle.append(row)
        if not active_cycle:
            return "首次出现"
        cycle_start_at = datetime.fromisoformat(str(active_cycle[0]["created_at"]))
        alert_rows = [
            row
            for row in self.store.load_alert_history_for_window(since=since, symbol=symbol_upper)
            if int(row["sent"] or 0) == 1 and datetime.fromisoformat(str(row["notified_at"])) >= cycle_start_at
        ]
        action_nodes: list[str] = []
        now = utcnow()
        alert_action_nodes: list[str] = []
        for row in alert_rows[-3:]:
            action = self._parse_alert_action_label(row)
            if not action:
                continue
            notified_at = datetime.fromisoformat(str(row["notified_at"]))
            delta_days = max(0, int((now - notified_at).total_seconds() // 86400))
            prefix = "今日" if delta_days == 0 else "昨晚" if delta_days == 1 else f"{delta_days}天前"
            node = f"{prefix}{action}"
            if not alert_action_nodes or alert_action_nodes[-1] != node:
                alert_action_nodes.append(node)
        if alert_action_nodes:
            action_nodes = alert_action_nodes
        else:
            for row in active_cycle[-3:]:
                action = str(row["action"] or "").strip()
                if not action:
                    continue
                created_at = datetime.fromisoformat(str(row["created_at"]))
                delta_days = max(0, int((now - created_at).total_seconds() // 86400))
                prefix = "今日" if delta_days == 0 else "昨晚" if delta_days == 1 else f"{delta_days}天前"
                node = f"{prefix}{action}"
                if not action_nodes or action_nodes[-1] != node:
                    action_nodes.append(node)
        if not action_nodes:
            return "首次出现"
        if not action_nodes[-1].endswith(current_action):
            action_nodes.append(f"今日{current_action}")
        return " -> ".join(action_nodes)

    def _formal_action_rank(self, action_label: str) -> int:
        return FORMAL_ACTION_RANK.get(str(action_label or "").strip(), -1)

    def _load_active_cycle_state(self, symbol: str) -> dict[str, Any]:
        rows = self.store.load_latest_decision_records(limit=300)
        normalized_symbol = str(symbol or "").strip().upper()
        for row in rows:
            row_symbol = str(row["symbol"] or "").strip().upper()
            if row_symbol != normalized_symbol:
                continue
            normalized_reason = normalize_close_reason(
                str(row["close_reason"] or ""),
                exit_subreason=str(row["exit_subreason"] or ""),
                entered=bool(row["entered"]) if "entered" in row.keys() else False,
            )
            if str(row["pool"] or "").strip() in {"holding_management", "exit"} or normalized_reason in TERMINAL_CYCLE_REASONS:
                return {"status": "terminal"}
            if str(row["pool"] or "").strip() != "confirmation":
                continue
            action_label = str(row["action"] or "").strip()
            if action_label not in {"确认做多", "试探建仓"}:
                continue
            status = "holding_active" if normalized_reason == "holding_active" or bool(row["entered"]) else "pending_entry"
            return {
                "status": status,
                "previous_formal_action": action_label,
                "entered": bool(row["entered"]),
                "decision_id": str(row["decision_id"] or ""),
                "event_id": str(row["event_id"] or ""),
            }
        return {"status": "terminal"}

    def _recent_candidate_observation_count_72h(self, symbol: str) -> int:
        since = (utcnow() - timedelta(hours=72)).isoformat()
        rows = self.store.load_candidate_evaluations_for_window(since=since, stage="candidate_pool")
        run_ids = {
            str(row["run_id"] or "")
            for row in rows
            if str(row["symbol"] or "").strip().upper() == str(symbol or "").strip().upper()
            and str(row["outcome"] or "").strip() == "selected"
        }
        return len({run_id for run_id in run_ids if run_id})

    def _build_formal_downgraded_watch_card(
        self,
        card: OpportunityCard,
        *,
        previous_formal_action: str,
    ) -> OpportunityCard:
        downgrade_explainer = (
            f"前次正式机会给出「{previous_formal_action}」但尚未进场，"
            "当前结构/量能/宏观条件已转弱，本次判断下调为观察。"
        )
        reason_to_watch = f"{downgrade_explainer} 建议撤销此前设置的买入挂单，等待新结构确立。"
        return replace(
            card,
            action_label="加入观察",
            priority="normal",
            confidence_label="中",
            lifecycle_pool="candidate",
            delivery_category="watch",
            active_cycle_status="pending_entry",
            previous_formal_action=previous_formal_action,
            previous_formal_sent=True,
            downgraded_from_formal=True,
            downgrade_explainer=downgrade_explainer,
            reason_to_watch=reason_to_watch,
            positioning_hint="建议撤销此前设置的买入挂单，等待新结构确立。",
            dedup_key=f"{card.symbol.upper()}:{card.event_id}:{card.event_type}:downgraded-watch",
        )

    def _apply_formal_card_cycle_policies(
        self,
        cards: list[OpportunityCard],
        packets: list[DecisionPacket],
        *,
        logger: StructuredLogger,
    ) -> tuple[list[OpportunityCard], list[DecisionPacket]]:
        if not cards:
            return cards, packets
        allowed_packets = {(packet.symbol, packet.horizon, packet.event_id): packet for packet in packets}
        adjusted_cards: list[OpportunityCard] = []
        adjusted_packet_keys: set[tuple[str, str, str]] = set()
        virtual_cycle_state: dict[str, dict[str, Any]] = {}
        for card in self._rank_notification_candidates(cards):
            if str(getattr(card, "delivery_category", "") or "").strip() != "formal":
                adjusted_cards.append(card)
                adjusted_packet_keys.add((card.symbol, card.horizon, card.event_id))
                continue
            symbol = card.symbol.upper()
            cycle_state = dict(virtual_cycle_state.get(symbol) or self._load_active_cycle_state(symbol))
            status = str(cycle_state.get("status") or "terminal")
            current_action = self._resolved_confirmation_action(card)
            current_rank = self._formal_action_rank(current_action)
            previous_formal_action = str(cycle_state.get("previous_formal_action") or "")
            previous_rank = self._formal_action_rank(previous_formal_action)
            base_card = replace(
                card,
                active_cycle_status=status,
                previous_formal_action=previous_formal_action,
                previous_formal_sent=bool(previous_formal_action),
                recent_candidate_observation_count_72h=self._recent_candidate_observation_count_72h(symbol),
            )
            # `is_breakthrough_event` is intentionally kept as a dormant hook for
            # future add-on / pyramid logic. For the current rollout, any active
            # holding cycle still suppresses fresh entry-style cards.
            if status == "holding_active":
                logger.info(
                    "formal_suppressed_active_holding",
                    "Skipped a weaker formal entry because the symbol is already in an active holding cycle.",
                    stage="route_into_pools",
                    symbol=symbol,
                    event_id=card.event_id,
                    context={"action_label": current_action, "previous_formal_action": previous_formal_action},
                )
                continue
            if status == "pending_entry" and previous_rank > current_rank:
                downgraded_card = self._build_formal_downgraded_watch_card(
                    base_card,
                    previous_formal_action=previous_formal_action,
                )
                adjusted_cards.append(downgraded_card)
                logger.info(
                    "formal_downgraded_to_watch_unentered",
                    "Downgraded a weaker formal entry into a watch card because the previous formal order is still unfilled.",
                    stage="route_into_pools",
                    symbol=symbol,
                    event_id=card.event_id,
                    context={"action_label": current_action, "previous_formal_action": previous_formal_action},
                )
                continue
            adjusted_cards.append(base_card)
            adjusted_packet_keys.add((card.symbol, card.horizon, card.event_id))
            virtual_cycle_state[symbol] = {
                "status": "pending_entry",
                "previous_formal_action": current_action,
                "entered": False,
                "decision_id": "",
                "event_id": card.event_id,
            }
        filtered_packets = [
            packet
            for packet in packets
            if (packet.symbol, packet.horizon, packet.event_id) in adjusted_packet_keys
        ]
        return adjusted_cards, filtered_packets

    def _json_object(self, raw: Any) -> dict[str, Any]:
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            payload = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _load_local_proxy_bars(self) -> dict[str, list]:
        return {
            symbol: self.store.load_price_bars(symbol, "1d", 400)
            for symbol in MACRO_PROXY_SYMBOLS
        }

    @staticmethod
    def _with_legacy_run_summary_aliases(summary: dict[str, Any]) -> dict[str, Any]:
        merged = dict(summary)
        merged["prewatch_candidates_count"] = int(summary.get("candidate_pool_candidates_count", 0) or 0)
        merged["prewatch_candidates"] = list(summary.get("candidate_pool_candidates", []))
        merged["prewatch_alerts_sent_count"] = int(summary.get("candidate_optional_alerts_sent_count", 0) or 0)
        merged["prewatch_alert_symbols"] = list(summary.get("candidate_optional_alert_symbols", []))
        merged["exit_pool_cards_count"] = int(summary.get("holding_management_cards_count", 0) or 0)
        merged["exit_pool_symbols"] = list(summary.get("holding_management_symbols", []))
        merged["exit_pool_cards"] = list(summary.get("holding_management_cards", []))
        return merged

    def _active_confirmation_rows_for_holding_management(self) -> list[Any]:
        rows = self.store.load_latest_decision_records(limit=300)
        selected: list[Any] = []
        closed_symbols: set[str] = set()
        selected_symbols: set[str] = set()
        for row in rows:
            symbol = str(row["symbol"] or "").strip().upper()
            if not symbol or symbol in closed_symbols or symbol in selected_symbols:
                continue
            if str(row["pool"] or "").strip() in {"exit", "holding_management"}:
                closed_symbols.add(symbol)
                continue
            if normalize_close_reason(
                str(row["close_reason"] or ""),
                exit_subreason=str(row["exit_subreason"] or "") if "exit_subreason" in row.keys() else "",
                entered=bool(row["entered"]) if "entered" in row.keys() else False,
            ) in {"profit_protection_exit", "invalidation_exit", "window_close_evaluation"}:
                closed_symbols.add(symbol)
                continue
            if str(row["pool"] or "").strip() != "confirmation":
                continue
            if str(row["action"] or "").strip() not in {"确认做多", "试探建仓"}:
                continue
            selected.append(row)
            selected_symbols.add(symbol)
        return selected

    def _profit_protection_reason_text(self, subreason: str, take_profit_range: PriceRange) -> tuple[str, str]:
        if subreason == "target_hit":
            return (
                "价格已进入止盈区较深位置，计划内利润目标基本兑现。",
                f"已到达目标区更深位置（原目标区 {take_profit_range.low:.2f}-{take_profit_range.high:.2f}），优先按计划兑现利润。",
            )
        if subreason == "weakening_after_tp_zone":
            return (
                "价格进入止盈区后连续走弱，利润保护优先级上升。",
                "进入止盈区后承接连续转弱，继续持有更容易把浮盈回吐回去。",
            )
        return (
            "宏观环境转差且已有浮盈，当前更适合先保护利润。",
            "外部风险抬升时，先把已有利润锁住，比继续硬扛更重要。",
        )

    def _exit_pool_reason_text(self, subreason: str, take_profit_range: PriceRange) -> tuple[str, str]:
        return self._profit_protection_reason_text(subreason, take_profit_range)

    def _holding_management_reason_text(
        self,
        normalized_reason: str,
        *,
        take_profit_range: PriceRange,
        invalidation_level: float,
        exit_subreason: str = "",
    ) -> tuple[str, str]:
        if normalized_reason == "profit_protection_exit":
            return self._profit_protection_reason_text(exit_subreason, take_profit_range)
        if normalized_reason == "invalidation_exit":
            return (
                "价格触发失效价，本次进攻逻辑结束。",
                f"价格已跌破失效价 {invalidation_level:.2f}，优先按纪律结束这次仓位。",
            )
        return (
            "持仓在观察窗口内未触发止盈或失效，当前进入窗口结算评估。",
            "窗口到期结算只用于后验复盘提供统一截止时间，不代表真实自动卖出。",
        )

    def _holding_management_subreason_display(self, subreason: str) -> str:
        return EXIT_POOL_SUBREASON_DISPLAY.get(subreason, "持仓管理")

    def _exit_pool_subreason_display(self, subreason: str) -> str:
        return self._holding_management_subreason_display(subreason)

    def _build_holding_management_actions(
        self,
        *,
        macro_context: dict[str, object],
        run_id: str,
        logger: StructuredLogger,
    ) -> tuple[list[OpportunityCard], list[dict[str, Any]], dict[str, int]]:
        cards: list[OpportunityCard] = []
        records: list[dict[str, Any]] = []
        summary = {
            "profit_protection_exit_count": 0,
            "invalidation_exit_count": 0,
            "window_close_evaluation_count": 0,
            "holding_active_count": 0,
            "not_entered_price_invalidated_count": 0,
            "not_entered_window_expired_count": 0,
        }
        proxy_bars_by_symbol = self._load_local_proxy_bars()
        for row in self._active_confirmation_rows_for_holding_management():
            symbol = str(row["symbol"] or "").strip().upper()
            bars = self.store.load_price_bars(symbol, "1d", 400)
            if not bars:
                continue
            outcome = compute_decision_outcome(
                row,
                bars,
                proxy_bars_by_symbol=proxy_bars_by_symbol,
            )
            if outcome is None:
                continue
            normalized_reason = normalize_close_reason(
                outcome.close_reason,
                exit_subreason=outcome.exit_subreason,
                entered=bool(outcome.entered),
            )
            summary_key = f"{normalized_reason}_count"
            if summary_key in summary:
                summary[summary_key] += 1
            if normalized_reason not in {"profit_protection_exit", "invalidation_exit"}:
                continue
            exit_subreason = str(outcome.exit_subreason or "")
            packet_payload = self._json_object(row["packet_json"])
            price_plan = self._json_object(row["entry_plan_json"])
            take_profit_payload = self._json_object(price_plan.get("take_profit_range"))
            entry_payload = self._json_object(price_plan.get("entry_range"))
            take_profit_range = PriceRange(
                float(take_profit_payload.get("low") or outcome.exit_price or 0.0),
                float(take_profit_payload.get("high") or outcome.exit_price or 0.0),
            ).normalized()
            entry_range = PriceRange(
                float(entry_payload.get("low") or outcome.entry_price or 0.0),
                float(entry_payload.get("high") or outcome.entry_price or 0.0),
            ).normalized()
            invalidation_payload = self._json_object(row["invalidation_json"])
            invalidation_level = float(
                price_plan.get("invalidation_level")
                or invalidation_payload.get("level")
                or row["entry_price"]
                or 0.0
            )
            reason_summary, positioning_hint = self._holding_management_reason_text(
                normalized_reason,
                take_profit_range=take_profit_range,
                invalidation_level=invalidation_level,
                exit_subreason=exit_subreason,
            )
            action_label = HOLDING_MANAGEMENT_REASON_DISPLAY.get(normalized_reason, "持仓管理")
            if normalized_reason == "profit_protection_exit":
                dedup_suffix = exit_subreason or normalized_reason
            else:
                dedup_suffix = normalized_reason
            dedup_key = f"holding_management:{row['decision_id']}:{dedup_suffix}"
            card = OpportunityCard(
                card_id=hashlib.sha1(f"{run_id}:{dedup_key}".encode("utf-8")).hexdigest(),
                event_id=f"holding_management:{row['decision_id']}:{dedup_suffix}",
                symbol=symbol,
                horizon=str(packet_payload.get("horizon") or "position"),
                event_type=str(row["event_type"] or "news"),
                headline_summary=reason_summary,
                bull_case="",
                bear_case="",
                event_score=float(row["event_score"] or 0.0),
                market_score=float(row["market_score"] or 0.0),
                final_score=float(row["final_score"] or 0.0),
                entry_range=entry_range,
                take_profit_range=take_profit_range,
                invalidation_level=round(invalidation_level, 2),
                invalidation_reason=(
                    "本次仓位已从进攻阶段切换到持仓管理。"
                    if normalized_reason == "profit_protection_exit"
                    else "价格已跌破失效价，本次进攻逻辑结束。"
                ),
                risk_notes=[
                    (
                        f"持仓管理：{self._holding_management_subreason_display(exit_subreason)}"
                        if normalized_reason == "profit_protection_exit"
                        else "持仓管理：触发失效价后结束本次仓位。"
                    ),
                    "这是一张持仓管理卡，不再代表适合新开仓。",
                ],
                source_refs=[],
                created_at=utcnow(),
                ttl=utcnow() + timedelta(days=2),
                priority=(
                    "high"
                    if normalized_reason == "profit_protection_exit" and exit_subreason in {"target_hit", "macro_protection"}
                    else "normal"
                ),
                dedup_key=dedup_key,
                bias="long",
                display_name=self._display_name_for(symbol),
                action_label=action_label,
                confidence_label=(
                    "高"
                    if normalized_reason == "profit_protection_exit" and exit_subreason in {"target_hit", "macro_protection"}
                    else "中"
                ),
                confidence_score=min(max(float(row["final_score"] or 0.0), 72.0), 92.0),
                reason_to_watch=reason_summary,
                theme_tags=list(json.loads(str(row["theme_ids_json"] or "[]"))),
                chain_summary=self._chain_summary_for_symbol(symbol, current_action=action_label),
                market_regime=str(macro_context.get("market_regime", "")),
                rate_risk=str(macro_context.get("rate_risk", "")),
                geopolitical_risk=str(macro_context.get("geopolitical_risk", "")),
                macro_risk_score=float(macro_context.get("macro_risk_score", 0.0)),
                market_data_complete=True,
                positioning_hint=positioning_hint,
                execution_eligible=False,
                execution_note=positioning_hint,
                lifecycle_pool="holding_management",
                delivery_category="exit",
                normalized_close_reason=normalized_reason,
                holding_management_reason=normalized_reason,
                holding_management_subreason=exit_subreason,
                holding_management_source_decision_id=str(row["decision_id"] or ""),
            )
            cards.append(card)
            records.append(
                {
                    "decision_id": hashlib.sha1(
                        f"{run_id}:exit:{symbol}:{row['decision_id']}:{exit_subreason}".encode("utf-8")
                    ).hexdigest(),
                    "event_id": card.event_id,
                    "symbol": symbol,
                    "event_type": card.event_type,
                    "priority": card.priority,
                    "confidence": card.confidence_label,
                    "event_score": card.event_score,
                    "market_score": card.market_score,
                    "theme_score": float(row["theme_score"] or 0.0),
                    "final_score": card.final_score,
                    "trigger_mode": dedup_suffix,
                    "theme_ids": list(card.theme_tags),
                    "entry_plan": {
                        "entry_range": {"low": entry_range.low, "high": entry_range.high},
                        "take_profit_range": {"low": take_profit_range.low, "high": take_profit_range.high},
                        "invalidation_level": invalidation_level,
                    },
                    "invalidation": {
                        "level": invalidation_level,
                        "reason": card.invalidation_reason,
                    },
                    "packet": {
                        "pool": "holding_management",
                        "action": action_label,
                        "source_decision_id": row["decision_id"],
                        "exit_subreason": exit_subreason,
                        "close_reason": outcome.close_reason,
                        "normalized_close_reason": normalized_reason,
                        "realized_return": outcome.realized_return,
                    },
                }
            )
            logger.info(
                "holding_management_selected",
                "Position entered holding management.",
                stage="holding_management",
                symbol=symbol,
                event_id=card.event_id,
                context={
                    "normalized_reason": normalized_reason,
                    "subreason": exit_subreason,
                    "source_decision_id": row["decision_id"],
                },
            )
        return cards, records, summary

    def _record_holding_management_decisions(self, records: list[dict[str, Any]], *, run_id: str) -> None:
        for record in records:
            self.store.save_decision_record(
                decision_id=record["decision_id"],
                run_id=run_id,
                event_id=record["event_id"],
                symbol=record["symbol"],
                event_type=record["event_type"],
                pool="holding_management",
                action=record["packet"].get("action", "持仓管理"),
                priority=record["priority"],
                confidence=record["confidence"],
                event_score=record["event_score"],
                market_score=record["market_score"],
                theme_score=record["theme_score"],
                final_score=record["final_score"],
                trigger_mode=record["trigger_mode"],
                llm_used=False,
                theme_ids=record["theme_ids"],
                entry_plan=record["entry_plan"],
                invalidation=record["invalidation"],
                ttl="",
                packet=record["packet"],
                created_at=utcnow().isoformat(),
            )

    def _route_into_pools(
        self,
        *,
        watchlist: set[str],
        routing_inputs: list[PoolRoutingInput],
        confirmation_evaluation_extras: list[dict[str, Any]],
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        macro_context: dict[str, object],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> tuple[list[OpportunityCard], list[DecisionPacket], list[CandidatePoolCandidate], list[DecisionPacket]]:
        confirmation_candidates: list[OpportunityCard] = []
        rerouted_event_cards: list[OpportunityCard] = []
        card_contexts: dict[str, dict[str, Any]] = {}
        rerouted_candidate_inputs: list[CandidatePoolCandidate] = []
        for routing_input in routing_inputs:
            promoted_card = self._apply_candidate_pool_promotion(
                routing_input.card,
                insight=routing_input.insight,
                candidate_pool_context=routing_input.candidate_pool_context,
            )
            if promoted_card.promoted_from_prewatch:
                logger.info(
                    "candidate_pool_promoted",
                    "Candidate-pool opportunity upgraded into the confirmation pool.",
                    stage="route_into_pools",
                    symbol=promoted_card.symbol,
                    event_id=promoted_card.event_id,
                    context={
                        "horizon": promoted_card.horizon,
                        "final_score": promoted_card.final_score,
                        "priority": promoted_card.priority,
                        "candidate_score": promoted_card.candidate_score,
                        "candidate_setup_type": promoted_card.candidate_setup_type,
                        "source_kind": routing_input.source_kind,
                    },
                )
            decorated_card = self._decorate_card_with_runtime_context(
                replace(promoted_card, lifecycle_pool="confirmation", delivery_category="formal"),
                insight=routing_input.insight,
                macro_context=macro_context,
                run_id=run_context.run_id,
            )
            decorated_card = replace(
                decorated_card,
                dedup_key=self._formal_confirmation_dedup_key(decorated_card),
            )
            card_contexts[decorated_card.card_id] = {
                "event_assessment": routing_input.event_assessment,
                "market_assessment": routing_input.market_assessment,
                "insight": routing_input.insight,
            }
            if self._card_belongs_to_confirmation_pool(decorated_card):
                confirmation_candidates.append(decorated_card)
                continue
            rerouted_event_cards.append(decorated_card)
            rerouted_candidate_inputs.append(
                self._build_candidate_from_confirmation_card(decorated_card)
            )
        confirmation_candidates = self._apply_theme_linkage_to_confirmation_candidates(confirmation_candidates)
        confirmation_packets = self._build_confirmation_decision_packets(
            confirmation_candidates,
            card_contexts=card_contexts,
        )
        confirmation_candidates = self._apply_confirmation_packets(
            confirmation_candidates,
            confirmation_packets,
        )
        (
            confirmation_candidates,
            confirmation_packets,
        ) = self._deduplicate_confirmation_pool_candidates(
            confirmation_candidates,
            confirmation_packets,
            logger=logger,
        )
        (
            confirmation_candidates,
            confirmation_packets,
        ) = self._apply_formal_card_cycle_policies(
            confirmation_candidates,
            confirmation_packets,
            logger=logger,
        )
        active_confirmation_cards = [
            card
            for card in confirmation_candidates
            if str(getattr(card, "delivery_category", "") or "").strip() == "formal"
        ]
        scan_candidates = self._build_candidate_pool_candidates(
            watchlist,
            confirmation_cards=active_confirmation_cards,
            snapshot_cache=snapshot_cache,
            saved_snapshot_keys=saved_snapshot_keys,
            run_context=run_context,
            logger=logger,
        )
        candidate_pool_candidates = self._merge_candidate_pool_inputs(
            scan_candidates,
            rerouted_candidate_inputs,
        )
        confirmation_evaluation_cards = list(confirmation_candidates) + rerouted_event_cards
        self._record_confirmation_candidate_evaluations(
            confirmation_evaluation_cards,
            run_id=run_context.run_id,
            extras=confirmation_evaluation_extras,
        )
        candidate_pool_packets = self._build_candidate_pool_decision_packets(
            candidate_pool_candidates,
            confirmation_cards=active_confirmation_cards,
        )
        return confirmation_candidates, confirmation_packets, candidate_pool_candidates, candidate_pool_packets

    def _card_belongs_to_confirmation_pool(self, card: OpportunityCard) -> bool:
        if str(getattr(card, "delivery_category", "") or "").strip() == "candidate_optional":
            return False
        if self._resolved_confirmation_action(card) not in {"确认做多", "试探建仓"}:
            return False
        if not bool(getattr(card, "execution_eligible", True)):
            return False
        if str(getattr(card, "priority", "") or "").strip() == "suppressed":
            return False
        if self._formal_structure_incompatible_for_confirmation(card):
            return False
        return True

    def _formal_confirmation_dedup_key(self, card: OpportunityCard) -> str:
        return f"{card.symbol.upper()}:{card.event_id}:{card.event_type}:formal"

    def _formal_structure_incompatible_for_confirmation(self, card: OpportunityCard) -> bool:
        trend_state = str(getattr(card, "trend_state", "") or "").strip().lower()
        relative_volume = getattr(card, "relative_volume", None)
        return trend_state in {"neutral", "bearish", "downtrend"} and relative_volume is not None and float(relative_volume) < 1.0

    def _confirmation_candidate_rank_key(self, card: OpportunityCard) -> tuple[float, float, float, float, int]:
        action_rank = {"确认做多": 2, "试探建仓": 1}.get(self._resolved_confirmation_action(card), 0)
        horizon_rank = {"position": 1, "swing": 0}.get(str(getattr(card, "horizon", "") or "").strip(), 0)
        return (
            float(getattr(card, "final_score", 0.0) or 0.0),
            float(getattr(card, "market_score", 0.0) or 0.0),
            float(getattr(card, "event_score", 0.0) or 0.0),
            float(getattr(card, "relative_volume", 0.0) or 0.0),
            action_rank + horizon_rank,
        )

    def _deduplicate_confirmation_pool_candidates(
        self,
        cards: list[OpportunityCard],
        packets: list[DecisionPacket],
        *,
        logger: StructuredLogger,
    ) -> tuple[list[OpportunityCard], list[DecisionPacket]]:
        if len(cards) <= 1:
            return cards, packets
        grouped: dict[tuple[str, str], list[OpportunityCard]] = {}
        for card in cards:
            grouped.setdefault((card.symbol.upper(), card.event_id), []).append(card)
        selected_keys: set[tuple[str, str, str]] = set()
        selected_cards: list[OpportunityCard] = []
        for group_key, group_cards in grouped.items():
            ranked_cards = sorted(
                group_cards,
                key=self._confirmation_candidate_rank_key,
                reverse=True,
            )
            selected = ranked_cards[0]
            selected_cards.append(selected)
            selected_keys.add((selected.symbol, selected.horizon, selected.event_id))
            if len(ranked_cards) > 1:
                logger.info(
                    "confirmation_candidate_collapsed",
                    "Collapsed duplicate confirmation candidates for the same symbol and event.",
                    stage="route_into_pools",
                    symbol=selected.symbol,
                    event_id=selected.event_id,
                    context={
                        "kept_horizon": selected.horizon,
                        "kept_final_score": selected.final_score,
                        "dropped_horizons": [item.horizon for item in ranked_cards[1:]],
                    },
                )
        filtered_packets = [
            packet
            for packet in packets
            if (packet.symbol, packet.horizon, packet.event_id) in selected_keys
        ]
        selected_cards.sort(
            key=lambda card: (
                -float(getattr(card, "final_score", 0.0) or 0.0),
                card.symbol,
                card.horizon,
            )
        )
        return selected_cards, filtered_packets

    def _resolved_confirmation_action(self, card: OpportunityCard) -> str:
        action_label = str(getattr(card, "action_label", "") or "").strip()
        if action_label:
            return action_label
        if not bool(getattr(card, "market_data_complete", True)):
            return "加入观察"
        priority = str(getattr(card, "priority", "") or "").strip()
        return {"high": "确认做多", "normal": "试探建仓", "suppressed": "加入观察"}.get(
            priority,
            "加入观察",
        )

    def _build_candidate_from_confirmation_card(self, card: OpportunityCard) -> CandidatePoolCandidate:
        reason_to_watch = str(card.reason_to_watch or "").strip()
        if not reason_to_watch:
            reason_to_watch = "当前值得继续跟踪，但还不适合正式出手。"
        if not bool(getattr(card, "market_data_complete", True)):
            degraded_hint = "行情快照暂不可用，先进入候选池继续跟踪。"
            if degraded_hint not in reason_to_watch:
                reason_to_watch = f"{degraded_hint} {reason_to_watch}".strip()
        if getattr(card, "macro_overlay_note", ""):
            reason_to_watch = f"{reason_to_watch} {card.macro_overlay_note}".strip()
        return CandidatePoolCandidate(
            symbol=card.symbol.upper(),
            horizon=card.horizon,
            setup_type="event_watch",
            score=round(float(card.final_score or card.event_score or 0.0), 2),
            headline_summary=card.headline_summary,
            action_hint="先进入候选池继续跟踪，等待催化和盘面进一步确认。",
            reason_to_watch=reason_to_watch,
            last_price=float(card.entry_range.low or 0.0),
            rsi_14=float(card.rsi_14 or 0.0),
            relative_volume=float(card.relative_volume or 0.0),
            trend_state=str(card.trend_state or ""),
            support_20=float(card.invalidation_level or 0.0),
            resistance_20=float(card.take_profit_range.high or card.take_profit_range.low or 0.0),
            trigger_mode="event",
            trigger_event_type=card.event_type,
            as_of=utcnow(),
        )

    def _merge_candidate_pool_inputs(
        self,
        scan_candidates: list[CandidatePoolCandidate],
        event_candidates: list[CandidatePoolCandidate],
    ) -> list[CandidatePoolCandidate]:
        if not scan_candidates:
            return list(event_candidates)
        if not event_candidates:
            return list(scan_candidates)
        merged: dict[tuple[str, str], CandidatePoolCandidate] = {
            (candidate.symbol.upper(), candidate.horizon): candidate for candidate in scan_candidates
        }
        for candidate in event_candidates:
            key = (candidate.symbol.upper(), candidate.horizon)
            existing = merged.get(key)
            if existing is None:
                merged[key] = candidate
                continue
            if candidate.trigger_mode == "event" and existing.trigger_mode != "event":
                merged[key] = candidate
                continue
            if candidate.score > existing.score:
                merged[key] = candidate
        return list(merged.values())

    def _persist_and_review(
        self,
        *,
        confirmation_cards: list[OpportunityCard],
        confirmation_packets: list[DecisionPacket],
        candidate_pool_candidates: list[CandidatePoolCandidate],
        candidate_pool_packets: list[DecisionPacket],
        macro_context: dict[str, object],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> tuple[list[str], list[OpportunityCard], list[dict[str, Any]], dict[str, int]]:
        for card in confirmation_cards:
            self.store.save_opportunity_card(card, run_id=run_context.run_id)
        run_context.metrics.cards_generated += len(confirmation_cards)
        self._record_decision_packets(confirmation_packets, run_id=run_context.run_id)
        self._record_decision_packets(candidate_pool_packets, run_id=run_context.run_id)
        self._dispatch_notifications(confirmation_cards, run_context, logger)
        candidate_optional_alert_symbols = self._dispatch_candidate_optional_notifications(
            candidate_pool_candidates,
            macro_context=macro_context,
            run_context=run_context,
            logger=logger,
        )
        holding_management_cards, holding_management_records, holding_management_summary = (
            self._build_holding_management_actions(
                macro_context=macro_context,
                run_id=run_context.run_id,
                logger=logger,
            )
        )
        if holding_management_cards:
            for card in holding_management_cards:
                self.store.save_opportunity_card(card, run_id=run_context.run_id)
            self._record_holding_management_decisions(
                holding_management_records,
                run_id=run_context.run_id,
            )
            self._dispatch_notifications(holding_management_cards, run_context, logger)
            run_context.metrics.cards_generated += len(holding_management_cards)
        return (
            candidate_optional_alert_symbols,
            holding_management_cards,
            holding_management_records,
            holding_management_summary,
        )

    def _decorate_card_with_runtime_context(
        self,
        card: OpportunityCard,
        *,
        insight: EventInsight,
        macro_context: dict[str, object],
        run_id: str = "",
        lite_narration: bool = False,
    ) -> OpportunityCard:
        display_name = self._display_name_for(card.symbol)
        macro_risk_score = float(macro_context.get("macro_risk_score", 0.0))
        action_label = card.action_label
        original_action_label = card.action_label
        penalty = (
            self._macro_penalty(action_label, macro_risk_score)
            if card.bias == "long" and self.settings.use_macro_risk_overlay
            else 0.0
        )
        final_score = round(max(0.0, card.final_score - penalty), 2)
        priority = card.priority
        if card.bias == "long" and macro_risk_score >= 70.0:
            if action_label == "确认做多":
                action_label = "试探建仓"
                priority = "normal"
            elif action_label == "试探建仓" and card.final_score < 80.0:
                action_label = "加入观察"
                priority = "suppressed" if priority == "normal" else priority
        chain_summary = self._chain_summary_for_symbol(card.symbol, current_action=action_label)
        theme_text = " / ".join(card.theme_tags) if card.theme_tags else "未标注"
        narrative = None
        narrative_metadata: dict[str, object] = {}
        if self.narrator is not None:
            narrative, narrative_metadata = self.narrator.narrate_with_metadata(
                insight=insight,
                card=card,
                market_regime=str(macro_context.get("market_regime", "")),
                rate_risk=str(macro_context.get("rate_risk", "")),
                geopolitical_risk=str(macro_context.get("geopolitical_risk", "")),
                theme_text=theme_text,
                chain_summary=chain_summary,
                lite=lite_narration,
            )
            self.store.record_llm_usage(
                run_id=run_id,
                event_id=insight.event_id,
                symbol=card.symbol,
                component="narration",
                model=str(narrative_metadata.get("model") or self.settings.openai_model),
                used_llm=bool(narrative_metadata.get("used_llm")),
                success=bool(narrative_metadata.get("success")),
                prompt_tokens_estimate=int(narrative_metadata.get("prompt_tokens_estimate") or 0),
                completion_tokens_estimate=int(narrative_metadata.get("completion_tokens_estimate") or 0),
                latency_ms=int(narrative_metadata.get("latency_ms") or 0),
                reason=str(narrative_metadata.get("reason") or ""),
                created_at=utcnow().isoformat(),
            )
        risk_notes = list(card.risk_notes)
        macro_note = (
            f"当前环境：{_market_regime_display(str(macro_context.get('market_regime', '')))}，"
            f"利率压力{_risk_level_display(str(macro_context.get('rate_risk', '')))}。"
        )
        if penalty > 0 and card.bias == "long":
            macro_note += f" 做多优先级已下调（-{penalty:.1f}分）。"
        overlay_note = ""
        if penalty > 0 and card.bias == "long":
            overlay_note = f"宏观风险覆盖已生效：综合分下调 {penalty:.1f} 分"
        if action_label != original_action_label:
            action_note = f"动作由「{original_action_label}」降为「{action_label}」"
            overlay_note = f"{overlay_note}，{action_note}" if overlay_note else f"宏观风险覆盖已生效：{action_note}"
        if macro_note not in risk_notes:
            risk_notes.append(macro_note)
        lifecycle_pool = str(getattr(card, "lifecycle_pool", "") or "")
        delivery_category = str(getattr(card, "delivery_category", "") or "")
        if not lifecycle_pool:
            lifecycle_pool = "confirmation"
        if not delivery_category:
            delivery_category = "formal"
        return replace(
            card,
            display_name=display_name,
            action_label=action_label,
            priority=priority,
            final_score=final_score,
            chain_summary=chain_summary,
            llm_summary=(narrative.summary if narrative and narrative.summary else card.headline_summary),
            llm_impact_inference=narrative.impact_inference if narrative else "",
            llm_reasoning=narrative.reasoning if narrative else "",
            llm_uncertainty=narrative.uncertainty if narrative else "",
            market_regime=str(macro_context.get("market_regime", "")),
            rate_risk=str(macro_context.get("rate_risk", "")),
            geopolitical_risk=str(macro_context.get("geopolitical_risk", "")),
            macro_risk_score=macro_risk_score,
            macro_penalty_applied=penalty,
            macro_action_before_overlay=original_action_label,
            macro_overlay_note=overlay_note,
            narrative_priority_adjustment=(
                narrative.priority_adjustment
                if narrative and self.settings.use_llm_ranking_assist
                else 0.0
            ),
            risk_notes=risk_notes,
            lifecycle_pool=lifecycle_pool,
            delivery_category=delivery_category,
        )

    def run_once(self) -> Dict[str, int]:
        run_context = self._create_run_context()
        logger = StructuredLogger(self.store, run_context.run_id)
        logger.info("run_started", "Satellite agent run started.", stage="run")
        watchlist = self.store.load_watchlist()
        macro_context = self._build_macro_context()
        now = utcnow()
        last_poll = self.store.get_state("last_event_poll_at")
        since = datetime.fromisoformat(last_poll) if last_poll else now - timedelta(days=1)
        events = self._fetch_events_from_sources(since, run_context, logger)
        events = self._deduplicate_fetched_events(events)
        raw_event_count = len(events)
        events = self._limit_fetched_events(events)
        if len(events) < raw_event_count:
            logger.info(
                "events_selected",
                "Selected a capped subset of candidate events for this run.",
                stage="source_fetch",
                context={
                    "raw_count": raw_event_count,
                    "selected_count": len(events),
                    "max_events_per_run": self.settings.max_events_per_run,
                    "max_events_per_symbol_per_run": self.settings.max_events_per_symbol_per_run,
                },
            )
        fetch_selected_count = len(events)
        events = self._limit_deep_processing_events(events)
        if len(events) < fetch_selected_count:
            logger.info(
                "events_deep_selected",
                "Selected a tighter subset of candidate events for deep processing.",
                stage="source_fetch",
                context={
                    "selected_count": fetch_selected_count,
                    "deep_selected_count": len(events),
                    "max_deep_events_per_run": self.settings.max_deep_events_per_run,
                },
            )
        run_context.metrics.events_fetched = len(events)
        logger.info(
            "events_fetched",
            "Fetched candidate events from configured sources.",
            stage="source_fetch",
            context={"count": len(events), "since": since.isoformat()},
        )
        newest_seen = since
        dedup_after = (now - timedelta(hours=self.settings.cross_source_dedup_hours)).isoformat()
        status = "success"
        routing_inputs: list[PoolRoutingInput] = []
        snapshot_cache: dict[tuple[str, str], object] = {}
        saved_snapshot_keys: set[tuple[str, str]] = set()
        candidate_pool_candidates: list[CandidatePoolCandidate] = []
        candidate_pool_packets: list[DecisionPacket] = []
        candidate_optional_alert_symbols: list[str] = []
        holding_management_cards: list[OpportunityCard] = []
        holding_management_summary: dict[str, int] = {}
        llm_requests_used = 0
        llm_daily_requests_used = self._llm_daily_usage_count(now)
        confirmation_evaluation_extras: list[dict[str, Any]] = []
        try:
            for raw_event in events:
                event = self.normalizer.normalize(raw_event)
                newest_seen = max(newest_seen, event.published_at)
                if watchlist and event.symbol not in watchlist:
                    run_context.metrics.skipped_out_of_watchlist += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped because symbol is outside active watchlist.",
                        stage="filter",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "out_of_watchlist"},
                    )
                    continue
                if self.store.has_recent_fingerprint(event.symbol, event.event_fingerprint, dedup_after):
                    run_context.metrics.skipped_cross_source_duplicate += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped due to recent cross-source duplicate fingerprint.",
                        stage="filter",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "cross_source_duplicate"},
                    )
                    continue
                inserted = self.store.save_raw_event(event, run_id=run_context.run_id)
                if not inserted:
                    run_context.metrics.skipped_duplicate_event_id += 1
                    logger.info(
                        "event_skipped",
                        "Event skipped because event_id already exists.",
                        stage="store",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"reason": "duplicate_event_id"},
                    )
                    continue
                run_context.metrics.events_processed += 1
                logger.info(
                    "event_accepted",
                    "Event accepted for extraction and scoring.",
                    stage="store",
                    symbol=event.symbol,
                    event_id=event.event_id,
                    context={"source": event.source, "source_type": event.source_type},
                )
                try:
                    insight, event_assessment, llm_requests_used, llm_daily_requests_used = self._extract_insight(
                        event,
                        run_context=run_context,
                        logger=logger,
                        llm_requests_used=llm_requests_used,
                        llm_daily_requests_used=llm_daily_requests_used,
                    )
                    self.store.save_event_insight(insight, run_id=run_context.run_id)
                except Exception as exc:
                    run_context.metrics.extraction_failures += 1
                    logger.error(
                        "extraction_failed",
                        "Insight extraction failed for event.",
                        stage="extract",
                        symbol=event.symbol,
                        event_id=event.event_id,
                        context={"error": exc.__class__.__name__},
                        )
                    continue
                candidate_pool_context = self._load_recent_candidate_pool_context(event.symbol)
                for horizon in self.settings.horizons:
                    try:
                        snapshot_key = (event.symbol, horizon)
                        snapshot = snapshot_cache.get(snapshot_key)
                        if snapshot is None:
                            snapshot = self.market_data.snapshot(event.symbol, horizon)
                            snapshot_cache[snapshot_key] = snapshot
                        if snapshot_key not in saved_snapshot_keys:
                            self.store.save_indicator_snapshot(snapshot)
                            saved_snapshot_keys.add(snapshot_key)
                    except Exception as exc:
                        run_context.metrics.market_data_failures += 1
                        logger.error(
                            "market_data_failed",
                            "Market data snapshot failed.",
                            stage="market_data",
                            symbol=event.symbol,
                            event_id=event.event_id,
                            context={"horizon": horizon, "error": exc.__class__.__name__},
                        )
                        degraded_card = self.scorer.score_event_only(
                            insight,
                            horizon,
                            failure_reason=exc.__class__.__name__,
                        )
                        degraded_market_assessment = self.market_engine.score_event_only(
                            insight,
                            horizon,
                            failure_reason=exc.__class__.__name__,
                        )[1]
                        if degraded_card.priority != "suppressed":
                            routing_inputs.append(
                                PoolRoutingInput(
                                    source_kind="event",
                                    card=degraded_card,
                                    insight=insight,
                                    event_assessment=event_assessment,
                                    market_assessment=degraded_market_assessment,
                                    candidate_pool_context=candidate_pool_context,
                                )
                            )
                            logger.warning(
                                "event_only_card_generated",
                                "Generated an event-only degraded card because market data was unavailable.",
                                stage="score",
                                symbol=event.symbol,
                                event_id=event.event_id,
                                context={
                                    "horizon": horizon,
                                    "event_score": degraded_card.event_score,
                                    "priority": degraded_card.priority,
                                },
                            )
                        else:
                            confirmation_evaluation_extras.append(
                                self._build_confirmation_candidate_evaluation_row(
                                    symbol=event.symbol,
                                    horizon=horizon,
                                    event_id=event.event_id,
                                    outcome="rejected",
                                    reason="event_only_below_threshold",
                                    score=degraded_card.final_score,
                                    payload={
                                        "source": "event_only_degraded",
                                        "event_type": insight.event_type,
                                        "event_score": round(float(degraded_card.event_score), 2),
                                        "market_score": round(float(degraded_card.market_score), 2),
                                        "final_score": round(float(degraded_card.final_score), 2),
                                        "priority": str(degraded_card.priority),
                                        "execution_eligible": bool(getattr(degraded_card, "execution_eligible", True)),
                                        "market_data_complete": bool(getattr(degraded_card, "market_data_complete", True)),
                                        "failure_reason": exc.__class__.__name__,
                                    },
                                )
                            )
                        continue
                    try:
                        card, market_assessment = self.market_engine.score_confirmation(insight, snapshot)
                        routing_inputs.append(
                            PoolRoutingInput(
                                source_kind="event",
                                card=card,
                                insight=insight,
                                event_assessment=event_assessment,
                                market_assessment=market_assessment,
                                candidate_pool_context=candidate_pool_context,
                            )
                        )
                    except Exception as exc:
                        run_context.metrics.scoring_failures += 1
                        logger.error(
                            "scoring_failed",
                            "Card scoring or enrichment failed.",
                            stage="score",
                            symbol=event.symbol,
                            event_id=event.event_id,
                            context={"horizon": horizon, "error": exc.__class__.__name__},
                        )
                        confirmation_evaluation_extras.append(
                            self._build_confirmation_candidate_evaluation_row(
                                symbol=event.symbol,
                                horizon=horizon,
                                event_id=event.event_id,
                                outcome="error",
                                reason=f"scoring_failed:{exc.__class__.__name__}",
                                payload={
                                    "source": "confirmation_scoring",
                                    "event_type": insight.event_type,
                                    "error": exc.__class__.__name__,
                                },
                            )
                        )
                        continue
            (
                notification_candidates,
                confirmation_packets,
                candidate_pool_candidates,
                candidate_pool_packets,
            ) = self._route_into_pools(
                watchlist=watchlist,
                routing_inputs=routing_inputs,
                confirmation_evaluation_extras=confirmation_evaluation_extras,
                snapshot_cache=snapshot_cache,
                saved_snapshot_keys=saved_snapshot_keys,
                macro_context=macro_context,
                run_context=run_context,
                logger=logger,
            )
            (
                candidate_optional_alert_symbols,
                holding_management_cards,
                _holding_management_records,
                holding_management_summary,
            ) = self._persist_and_review(
                confirmation_cards=notification_candidates,
                confirmation_packets=confirmation_packets,
                candidate_pool_candidates=candidate_pool_candidates,
                candidate_pool_packets=candidate_pool_packets,
                macro_context=macro_context,
                run_context=run_context,
                logger=logger,
            )
            self.store.set_state("last_event_poll_at", newest_seen.isoformat())
        except Exception as exc:
            status = "failed"
            logger.error(
                "run_failed",
                "Satellite agent run aborted with an unexpected error.",
                stage="run",
                context={"error": exc.__class__.__name__},
            )
            raise
        finally:
            finished_at = utcnow()
            active_confirmation_cards = [
                card
                for card in notification_candidates
                if str(getattr(card, "delivery_category", "") or "").strip() == "formal"
            ]
            holding_management_card_records = [
                {
                    "symbol": card.symbol,
                    "display_name": card.display_name,
                    "horizon": card.horizon,
                    "holding_management_reason": card.holding_management_reason or card.normalized_close_reason,
                    "subreason": card.holding_management_subreason,
                    "reason_to_watch": card.reason_to_watch,
                    "positioning_hint": card.positioning_hint,
                    "chain_summary": card.chain_summary,
                    "source_decision_id": card.holding_management_source_decision_id,
                    "action_label": card.action_label,
                    "take_profit_range": {
                        "low": card.take_profit_range.low,
                        "high": card.take_profit_range.high,
                    },
                }
                for card in holding_management_cards
            ]
            summary = run_context.metrics.as_dict()
            summary["last_event_poll_at"] = newest_seen.isoformat()
            summary["candidate_pool_count"] = len(candidate_pool_candidates)
            summary["candidate_pool_candidates_count"] = len(candidate_pool_candidates)
            summary["candidate_pool_candidates"] = [candidate.to_record() for candidate in candidate_pool_candidates]
            summary["candidate_optional_alerts_sent_count"] = run_context.metrics.candidate_optional_alerts_sent_count
            summary["candidate_optional_alert_symbols"] = list(candidate_optional_alert_symbols)
            summary["confirmation_pool_count"] = len(active_confirmation_cards)
            summary["holding_management_cards_count"] = len(holding_management_cards)
            summary["holding_management_symbols"] = [card.symbol for card in holding_management_cards]
            summary["holding_management_cards"] = holding_management_card_records
            summary.update(holding_management_summary)
            summary["profit_protection_cards_count"] = len(
                [
                    card
                    for card in holding_management_cards
                    if str(card.holding_management_reason or card.normalized_close_reason or "").strip()
                    == "profit_protection_exit"
                ]
            )
            summary = self._with_legacy_run_summary_aliases(summary)
            if status == "success":
                self.theme_engine.persist_candidate_pool_memory(active_confirmation_cards, candidate_pool_candidates)
            self.store.record_run(
                run_id=run_context.run_id,
                started_at=run_context.started_at,
                finished_at=finished_at,
                status=status,
                run_name=self.run_name,
                note=self.note,
                config_snapshot=self.runtime_snapshot,
                summary=summary,
            )
            touched_symbols = sorted(
                {
                    card.symbol.upper()
                    for card in [*notification_candidates, *holding_management_cards]
                }
                | {candidate.symbol.upper() for candidate in candidate_pool_candidates}
            )
            if touched_symbols:
                try:
                    rebuild_cycle_audit(self.store, symbols=touched_symbols)
                except Exception as exc:
                    logger.warning(
                        "cycle_audit_refresh_failed",
                        "Cycle audit refresh failed after run completion.",
                        stage="run",
                        context={"error": exc.__class__.__name__, "symbol_count": len(touched_symbols)},
                    )
            logger.info(
                "run_finished",
                "Satellite agent run finished.",
                stage="run",
                context={"status": status, **summary},
            )
        return {
            "events_processed": run_context.metrics.events_processed,
            "cards_generated": run_context.metrics.cards_generated,
            "alerts_sent": run_context.metrics.alerts_sent,
            "candidate_pool_count": run_context.metrics.candidate_pool_count,
            "candidate_optional_alerts_sent_count": run_context.metrics.candidate_optional_alerts_sent_count,
            "prewatch_candidates": run_context.metrics.candidate_pool_count,
            "prewatch_alerts_sent": run_context.metrics.candidate_optional_alerts_sent_count,
        }

    def _extract_insight(
        self,
        event,
        *,
        run_context: RunContext,
        logger: StructuredLogger,
        llm_requests_used: int,
        llm_daily_requests_used: int,
    ) -> tuple[object, object, int, int]:
        allow_llm = (
            isinstance(self.extractor, OpenAIExtractor)
            and bool(self.settings.openai_api_key)
            and self.settings.use_llm_event_extraction
        )

        budget_reason = self._llm_budget_reason(
            llm_requests_used=llm_requests_used,
            llm_daily_requests_used=llm_daily_requests_used,
        )
        if budget_reason:
            run_context.metrics.llm_budget_skips += 1
            insight, assessment, _ = self.event_engine.understand(event, allow_llm=False)
            self.store.record_llm_usage(
                run_id=run_context.run_id,
                event_id=event.event_id,
                symbol=event.symbol,
                component="event_extraction",
                model=self.settings.openai_model,
                used_llm=False,
                success=False,
                reason=budget_reason,
                created_at=utcnow().isoformat(),
            )
            logger.info(
                "llm_skipped",
                "LLM extraction skipped due to active request budget.",
                stage="extract",
                symbol=event.symbol,
                event_id=event.event_id,
                context={"reason": budget_reason},
            )
            return insight, assessment, llm_requests_used, llm_daily_requests_used

        insight, assessment, metadata = self.event_engine.understand(event, allow_llm=allow_llm)
        used_llm = bool(metadata.get("used_llm"))
        success = bool(metadata.get("success"))
        prompt_tokens = int(metadata.get("prompt_tokens_estimate", 0))
        completion_tokens = int(metadata.get("completion_tokens_estimate", 0))
        reason = str(metadata.get("reason", ""))
        if used_llm:
            llm_requests_used += 1
            llm_daily_requests_used += 1
            run_context.metrics.llm_requests += 1
        if used_llm and not success:
            run_context.metrics.llm_fallbacks += 1
            logger.warning(
                "llm_fallback",
                "LLM extraction failed and fell back to rule-based extraction.",
                stage="extract",
                symbol=event.symbol,
                event_id=event.event_id,
                context={"reason": reason},
            )
        self.store.record_llm_usage(
            run_id=run_context.run_id,
            event_id=event.event_id,
            symbol=event.symbol,
            component="event_extraction",
            model=self.settings.openai_model,
            used_llm=used_llm,
            success=success,
            prompt_tokens_estimate=prompt_tokens,
            completion_tokens_estimate=completion_tokens,
            latency_ms=int(metadata.get("latency_ms", 0) or 0),
            reason=reason,
            created_at=utcnow().isoformat(),
        )
        return insight, assessment, llm_requests_used, llm_daily_requests_used

    def _llm_budget_reason(self, *, llm_requests_used: int, llm_daily_requests_used: int) -> str:
        if self.settings.llm_max_requests_per_run > 0 and llm_requests_used >= self.settings.llm_max_requests_per_run:
            return "per_run_budget_exhausted"
        if self.settings.llm_max_requests_per_day > 0 and llm_daily_requests_used >= self.settings.llm_max_requests_per_day:
            return "daily_budget_exhausted"
        return ""

    def _llm_daily_usage_count(self, now: datetime) -> int:
        local_now = now.astimezone(BEIJING_TZ)
        start_of_day = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return self.store.count_llm_usage_since(start_of_day.astimezone(now.tzinfo).isoformat())

    def run_forever(self) -> None:
        while True:
            self.run_once()
            time.sleep(self.settings.poll_seconds)

    def _create_run_context(self) -> RunContext:
        started_at = utcnow()
        digest = hashlib.sha1(started_at.isoformat().encode("utf-8")).hexdigest()
        return RunContext(run_id=digest[:16], started_at=started_at)

    def _fetch_events_from_sources(
        self,
        since: datetime,
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        events = []
        adapters = self.source_adapter.child_adapters()
        if not adapters:
            return events
        cached_health = {
            adapter.name: self._load_cached_source_health(adapter.name)
            for adapter in adapters
        }

        with ThreadPoolExecutor(max_workers=min(len(adapters), 4)) as executor:
            futures = {
                executor.submit(self._fetch_from_adapter, adapter, since, cached_health.get(adapter.name)): adapter
                for adapter in adapters
            }
            for future in as_completed(futures):
                adapter = futures[future]
                result = future.result()
                health = result.get("health")
                health_derived_from_fetch = result.get("health_derived_from_fetch", False)
                if health is None:
                    run_context.metrics.source_health_failures += 1
                    logger.error(
                        "source_health",
                        "Source health check raised an unexpected error.",
                        stage="source_health",
                        context={
                            "source_name": adapter.name,
                            "error": result.get("health_error", "UnknownError"),
                            "detail": result.get("health_detail", ""),
                        },
                    )
                    continue
                self.store.record_source_health(run_context.run_id, health)
                if health.status == "healthy":
                    logger.info(
                        "source_health",
                        (
                            "Source fetch probe passed."
                            if health_derived_from_fetch
                            else "Source health check passed."
                        ),
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                else:
                    if not health_derived_from_fetch:
                        run_context.metrics.source_health_failures += 1
                    logger.error(
                        "source_health",
                        (
                            "Source fetch probe failed."
                            if health_derived_from_fetch
                            else "Source health check failed."
                        ),
                        stage="source_health",
                        context={
                            "source_name": health.source_name,
                            "status": health.status,
                            "detail": health.detail,
                            "latency_ms": health.latency_ms,
                        },
                    )
                    if not health_derived_from_fetch and not adapter.should_fetch_after_unhealthy_healthcheck():
                        continue
                    if not health_derived_from_fetch:
                        logger.warning(
                            "source_health_degraded_fetch",
                            "Source health check failed, but fetch will still be attempted.",
                            stage="source_health",
                            context={
                                "source_name": health.source_name,
                                "status": health.status,
                                "detail": health.detail,
                                "latency_ms": health.latency_ms,
                            },
                        )
                fetch_error = result.get("fetch_error")
                if health.status == "healthy" and not result.get("health_from_cache"):
                    self._save_cached_source_health(health)
                if fetch_error:
                    run_context.metrics.source_fetch_failures += 1
                    logger.error(
                        "source_fetch_failed",
                        (
                            "Source fetch failed during fetch probe."
                            if health_derived_from_fetch
                            else "Source fetch failed after health check."
                        ),
                        stage="source_fetch",
                        context={
                            "source_name": adapter.name,
                            "error": result.get("fetch_error_type", "UnknownError"),
                            "detail": fetch_error,
                        },
                    )
                    continue
                adapter_events = result.get("events", [])
                events.extend(adapter_events)
                logger.info(
                    "source_fetch_completed",
                    "Source fetch completed.",
                    stage="source_fetch",
                    context={"source_name": adapter.name, "event_count": len(adapter_events)},
                )
                partial_errors = result.get("partial_errors", [])
                if partial_errors:
                    logger.warning(
                        "source_fetch_partial",
                        "Source fetch completed with partial per-symbol failures.",
                        stage="source_fetch",
                        context={"source_name": adapter.name, "errors": partial_errors},
                    )
        return sorted(events, key=lambda event: event.published_at)

    def _deduplicate_fetched_events(self, events: list) -> list:
        seen_ids: set[str] = set()
        deduped = []
        for event in sorted(events, key=lambda item: item.published_at):
            normalized = self.normalizer.normalize(event)
            if normalized.event_id in seen_ids:
                continue
            seen_ids.add(normalized.event_id)
            deduped.append(normalized)
        return deduped

    def _limit_fetched_events(self, events: list) -> list:
        if self.settings.max_events_per_run <= 0:
            return events
        limited = []
        symbol_counts: dict[str, int] = {}
        ranked_events = self._rank_events_for_selection(events)
        for event in ranked_events:
            if self.settings.max_events_per_symbol_per_run > 0:
                current = symbol_counts.get(event.symbol, 0)
                if current >= self.settings.max_events_per_symbol_per_run:
                    continue
                symbol_counts[event.symbol] = current + 1
            limited.append(event)
            if len(limited) >= self.settings.max_events_per_run:
                break
        return sorted(limited, key=lambda item: item.published_at)

    def _limit_deep_processing_events(self, events: list) -> list:
        if self.settings.max_deep_events_per_run <= 0 or len(events) <= self.settings.max_deep_events_per_run:
            return events
        ranked = self._rank_events_for_selection(events)
        limit = self.settings.max_deep_events_per_run
        reserved = self._reserve_theme_representative_events(ranked, limit)
        reserved_ids = {event.event_id for event in reserved}
        trimmed = list(reserved)
        for event in ranked:
            if event.event_id in reserved_ids:
                continue
            trimmed.append(event)
            if len(trimmed) >= limit:
                break
        return sorted(trimmed, key=lambda item: item.published_at)

    def _rank_events_for_selection(self, events: list) -> list:
        theme_heat = self._build_batch_event_theme_heat(events)
        return sorted(
            events,
            key=lambda item: (
                self._event_selection_priority(item) + theme_heat.get(item.symbol.upper(), 0.0),
                item.published_at,
            ),
            reverse=True,
        )

    def _event_selection_priority(self, event) -> float:
        text = f"{event.headline} {event.summary}".lower()
        score = SOURCE_TYPE_SELECTION_WEIGHTS.get(event.source_type, 0.0)
        for term, weight in STRONG_SELECTION_TERMS:
            if term in text:
                score += weight
        for term, weight in LOW_SIGNAL_SELECTION_TERMS:
            if term in text:
                score += weight
        if any(token in text for token in ("ai", "data center", "chip", "gpu", "cloud")):
            score += 6.0
        if event.symbol in self._candidate_pool_watch_symbols():
            score += 4.0
        theme_context = summarize_symbol_theme_context(
            event.symbol,
            symbol_theme_map=self.symbol_theme_map,
            theme_memberships=self.theme_memberships,
        )
        if theme_context["peer_count"] > 0:
            score += min(theme_context["peer_count"], 3) * THEME_SELECTION_BONUS_PER_PEER
        return score

    def _build_batch_event_theme_heat(self, events: list) -> dict[str, float]:
        theme_counts = self._build_batch_theme_symbol_counts(events)
        if not theme_counts:
            return {}

        symbol_heat: dict[str, float] = {}
        for symbol, theme_keys in self.symbol_theme_map.items():
            max_bonus = 0.0
            for theme_key in theme_keys:
                symbol_count = len(theme_counts.get(theme_key, set()))
                if symbol_count < 2:
                    continue
                bonus = min(
                    (symbol_count - 1) * THEME_BATCH_CLUSTER_BONUS_PER_SYMBOL,
                    THEME_BATCH_CLUSTER_MAX_BONUS,
                )
                if bonus > max_bonus:
                    max_bonus = bonus
            if max_bonus > 0:
                symbol_heat[symbol] = max_bonus
        return symbol_heat

    def _build_batch_theme_symbol_counts(self, events: list) -> dict[str, set[str]]:
        if not events or not self.symbol_theme_map:
            return {}
        theme_counts: dict[str, set[str]] = {}
        for event in events:
            symbol = event.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_counts.setdefault(theme_key, set()).add(symbol)
        return theme_counts

    def _reserve_theme_representative_events(self, ranked_events: list, limit: int) -> list:
        reserve_budget = min(self.settings.max_deep_theme_reservations_per_run, limit)
        if reserve_budget <= 0 or not ranked_events:
            return []
        theme_counts = self._build_batch_theme_symbol_counts(ranked_events)
        hot_themes = sorted(
            (
                theme_key
                for theme_key, symbols in theme_counts.items()
                if len(symbols) >= 2
            ),
            key=lambda theme_key: (
                len(theme_counts[theme_key]),
                theme_key,
            ),
            reverse=True,
        )
        if not hot_themes:
            return []

        reserved = []
        reserved_ids: set[str] = set()
        reserved_symbols: set[str] = set()
        for theme_key in hot_themes:
            for event in ranked_events:
                symbol = event.symbol.upper()
                if event.event_id in reserved_ids or symbol in reserved_symbols:
                    continue
                if theme_key not in self.symbol_theme_map.get(symbol, []):
                    continue
                reserved.append(event)
                reserved_ids.add(event.event_id)
                reserved_symbols.add(symbol)
                break
            if len(reserved) >= reserve_budget:
                break
        return reserved

    def _dispatch_notifications(self, cards: list, run_context: RunContext, logger: StructuredLogger) -> None:
        alerts_sent = 0
        symbol_alert_counts: dict[str, int] = {}
        for card in self._rank_notification_candidates(cards):
            delivery_category = str(getattr(card, "delivery_category", "") or "").strip()
            if delivery_category == "formal":
                decision = self.notifier.send(card, run_id=run_context.run_id)
            elif card.priority == "suppressed":
                decision = self.notifier.skip(card, reason="threshold_not_met", run_id=run_context.run_id)
            elif (
                card.priority != "high"
                and card.final_score < self.settings.normal_alert_min_final_score
            ):
                decision = self.notifier.skip(card, reason="quality_cutoff", run_id=run_context.run_id)
            elif (
                self.settings.max_alerts_per_run > 0
                and alerts_sent >= self.settings.max_alerts_per_run
            ):
                decision = self.notifier.skip(
                    card,
                    reason="run_alert_budget_exhausted",
                    run_id=run_context.run_id,
                )
            elif (
                self.settings.max_alerts_per_symbol_per_run > 0
                and symbol_alert_counts.get(card.symbol, 0) >= self.settings.max_alerts_per_symbol_per_run
            ):
                decision = self.notifier.skip(
                    card,
                    reason="symbol_alert_budget_exhausted",
                    run_id=run_context.run_id,
                )
            else:
                decision = self.notifier.send(card, run_id=run_context.run_id)
            if decision.sent:
                alerts_sent += 1
                symbol_alert_counts[card.symbol] = symbol_alert_counts.get(card.symbol, 0) + 1
                run_context.metrics.alerts_sent += 1
            self._log_notification_decision(card, decision, run_context, logger)

    def _rank_notification_candidates(self, cards: list) -> list:
        priority_rank = {"high": 0, "normal": 1, "suppressed": 2}
        theme_heat = self._build_symbol_theme_heat(cards)
        return sorted(
            cards,
            key=lambda card: (
                priority_rank.get(card.priority, 3),
                0 if getattr(card, "promoted_from_prewatch", False) else 1,
                -theme_heat.get(card.symbol.upper(), 0.0),
                -(card.final_score + max(min(getattr(card, "narrative_priority_adjustment", 0.0), 3.0), -3.0)),
                -card.event_score,
                card.symbol,
                card.horizon,
            ),
        )

    def _log_notification_decision(
        self,
        card,
        decision,
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> None:
        if decision.sent:
            logger.info(
                "alert_sent",
                "Opportunity alert delivered.",
                stage="notify",
                symbol=card.symbol,
                event_id=card.event_id,
                context={"horizon": card.horizon, "priority": decision.priority, "reason": decision.reason},
            )
            return
        if decision.reason.startswith("transport_error:"):
            run_context.metrics.notification_failures += 1
            logger.error(
                "alert_failed",
                "Opportunity alert failed to deliver.",
                stage="notify",
                symbol=card.symbol,
                event_id=card.event_id,
                context={"horizon": card.horizon, "reason": decision.reason},
            )
            return
        logger.info(
            "alert_skipped",
            "Opportunity alert not sent.",
            stage="notify",
            symbol=card.symbol,
            event_id=card.event_id,
            context={"horizon": card.horizon, "reason": decision.reason},
        )

    def _build_candidate_pool_candidates(
        self,
        watchlist: set[str],
        *,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list[CandidatePoolCandidate]:
        if not watchlist:
            return []
        candidates = []
        candidate_evaluations: dict[str, dict[str, Any]] = {}
        near_miss_snapshots: dict[str, object] = {}
        horizon = "position"
        horizon_settings = self.settings.horizons[horizon]
        scan_symbols = self._select_candidate_pool_scan_symbols(watchlist, snapshot_cache)
        max_workers = min(len(scan_symbols), max(self.settings.candidate_pool_concurrency, 1))
        with ThreadPoolExecutor(max_workers=max_workers or 1) as executor:
            futures = {
                executor.submit(
                    self._build_single_candidate_pool_candidate,
                    symbol,
                    horizon,
                    horizon_settings,
                    snapshot_cache,
                ): symbol
                for symbol in scan_symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                snapshot_key = (symbol, horizon)
                try:
                    snapshot, candidate = future.result()
                except Exception as exc:
                    self._record_candidate_pool_failure(symbol)
                    run_context.metrics.candidate_pool_failures += 1
                    candidate_evaluations[symbol.upper()] = {
                        "symbol": symbol.upper(),
                        "horizon": horizon,
                        "outcome": "error",
                        "reason": f"build_failed:{exc.__class__.__name__}",
                        "score": None,
                        "payload": {
                            "source": "scan",
                            "error": exc.__class__.__name__,
                            "min_score": round(float(self.settings.candidate_pool_min_score), 2),
                        },
                    }
                    logger.warning(
                        "candidate_pool_failed",
                        "Candidate-pool generation failed for symbol.",
                        stage="candidate_pool",
                        symbol=symbol,
                        context={"error": exc.__class__.__name__},
                    )
                    continue
                self._clear_candidate_pool_failure(symbol)
                snapshot_cache[snapshot_key] = snapshot
                if snapshot_key not in saved_snapshot_keys:
                    self.store.save_indicator_snapshot(snapshot)
                    saved_snapshot_keys.add(snapshot_key)
                evaluation = evaluate_candidate_snapshot(
                    snapshot,
                    horizon_settings,
                    min_score=self.settings.candidate_pool_min_score,
                )
                if candidate is None:
                    near_miss_snapshots[symbol] = snapshot
                    candidate_evaluations[symbol.upper()] = {
                        "symbol": symbol.upper(),
                        "horizon": horizon,
                        "outcome": "rejected",
                        "reason": str(evaluation.get("rejection_reason") or "rejected"),
                        "score": float(evaluation.get("total_score") or 0.0),
                        "payload": self._build_candidate_pool_evaluation_payload(
                            snapshot=snapshot,
                            evaluation=evaluation,
                            source="scan",
                        ),
                    }
                    continue
                candidates.append(candidate)
                candidate_evaluations[symbol.upper()] = {
                    "symbol": symbol.upper(),
                    "horizon": horizon,
                    "outcome": "pending",
                    "reason": "passed_scan_threshold",
                    "score": round(float(candidate.score), 2),
                    "payload": self._build_candidate_pool_evaluation_payload(
                        snapshot=snapshot,
                        candidate=candidate,
                        evaluation=evaluation,
                        source="scan",
                    ),
                }
        if near_miss_snapshots:
            candidates.extend(
                self._build_theme_supported_candidate_pool_candidates(
                    near_miss_snapshots,
                    horizon_settings=horizon_settings,
                    confirmation_cards=confirmation_cards,
                    existing_symbols={candidate.symbol.upper() for candidate in candidates},
                )
            )
        event_supported_candidates = self._build_event_supported_candidate_pool_candidates(
            watchlist,
            horizon_settings=horizon_settings,
            confirmation_cards=confirmation_cards,
            snapshot_cache=snapshot_cache,
            saved_snapshot_keys=saved_snapshot_keys,
            existing_symbols={candidate.symbol.upper() for candidate in candidates},
            run_context=run_context,
            logger=logger,
        )
        if event_supported_candidates:
            candidates.extend(event_supported_candidates)
        candidates = self._apply_theme_linkage_to_candidate_pool_candidates(
            candidates,
            confirmation_cards=confirmation_cards,
        )
        ranked = sort_candidate_pool_candidates(
            candidates,
            max_candidates=self.settings.max_candidate_pool_candidates_per_run,
        )
        selected_ranks = {
            candidate.symbol.upper(): index + 1
            for index, candidate in enumerate(ranked)
        }
        for candidate in candidates:
            symbol = candidate.symbol.upper()
            existing = candidate_evaluations.get(symbol, {})
            payload = dict(existing.get("payload") or {})
            if not payload:
                payload = self._build_candidate_pool_evaluation_payload(
                    candidate=candidate,
                    source="event_support" if candidate.trigger_mode == "event" else "candidate_pool",
                )
            if symbol in selected_ranks:
                payload["selected_rank"] = selected_ranks[symbol]
                outcome = "selected"
                reason = "ranked_in_run"
            else:
                outcome = "not_selected"
                reason = "ranked_below_run_cap"
            candidate_evaluations[symbol] = {
                "symbol": symbol,
                "horizon": candidate.horizon,
                "outcome": outcome,
                "reason": reason,
                "score": round(float(candidate.score), 2),
                "payload": payload,
            }
        run_context.metrics.candidate_pool_count = len(ranked)
        for candidate in ranked:
            self._record_candidate_pool_candidate(candidate)
            logger.info(
                "candidate_pool_selected",
                "Symbol selected into the candidate pool.",
                stage="candidate_pool",
                symbol=candidate.symbol,
                context={
                    "horizon": candidate.horizon,
                    "setup_type": candidate.setup_type,
                    "score": candidate.score,
                },
            )
        evaluation_created_at = utcnow().isoformat()
        for item in sorted(candidate_evaluations.values(), key=lambda row: (str(row.get("symbol") or ""), str(row.get("outcome") or ""))):
            self.store.record_candidate_evaluation(
                run_id=run_context.run_id,
                stage="candidate_pool",
                symbol=str(item.get("symbol") or ""),
                horizon=str(item.get("horizon") or horizon),
                outcome=str(item.get("outcome") or ""),
                reason=str(item.get("reason") or ""),
                score=item.get("score"),
                strategy_version=self.strategy_version,
                payload=dict(item.get("payload") or {}),
                created_at=evaluation_created_at,
            )
        return ranked

    def _build_prewatch_candidates(
        self,
        watchlist: set[str],
        *,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list[CandidatePoolCandidate]:
        return self._build_candidate_pool_candidates(
            watchlist,
            confirmation_cards=confirmation_cards,
            snapshot_cache=snapshot_cache,
            saved_snapshot_keys=saved_snapshot_keys,
            run_context=run_context,
            logger=logger,
        )

    def _build_candidate_pool_evaluation_payload(
        self,
        *,
        snapshot=None,
        candidate=None,
        evaluation: dict[str, object] | None = None,
        source: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source": source,
            "min_score": round(float(self.settings.candidate_pool_min_score), 2),
        }
        if snapshot is not None:
            payload.update(
                {
                    "last_price": round(float(snapshot.last_price), 2),
                    "rsi_14": round(float(snapshot.rsi_14), 2),
                    "relative_volume": round(float(snapshot.relative_volume), 2),
                    "trend_state": str(snapshot.trend_state),
                    "atr_percent": round(float(snapshot.atr_percent), 2),
                    "support_20": round(float(snapshot.support_20), 2),
                    "resistance_20": round(float(snapshot.resistance_20), 2),
                }
            )
        if evaluation:
            payload.update(
                {
                    "candidate_score": round(float(evaluation.get("total_score") or 0.0), 2),
                    "prewatch_score": round(float(evaluation.get("total_score") or 0.0), 2),
                    "setup_type": str(evaluation.get("setup_type") or ""),
                    "rejection_reason": str(evaluation.get("rejection_reason") or ""),
                    "score_breakdown": {
                        "trend": round(float(evaluation.get("trend_score") or 0.0), 2),
                        "volume": round(float(evaluation.get("volume_score") or 0.0), 2),
                        "structure": round(float(evaluation.get("structure_score") or 0.0), 2),
                        "momentum": round(float(evaluation.get("momentum_score") or 0.0), 2),
                        "volatility": round(float(evaluation.get("volatility_score") or 0.0), 2),
                    },
                }
            )
        if candidate is not None:
            payload.update(
                {
                    "candidate_score": round(float(candidate.score), 2),
                    "prewatch_score": round(float(candidate.score), 2),
                    "setup_type": str(candidate.setup_type),
                    "trigger_mode": str(candidate.trigger_mode),
                    "headline_summary": str(candidate.headline_summary),
                }
            )
        return payload

    def _build_prewatch_evaluation_payload(
        self,
        *,
        snapshot=None,
        candidate=None,
        evaluation: dict[str, object] | None = None,
        source: str = "",
    ) -> dict[str, Any]:
        return self._build_candidate_pool_evaluation_payload(
            snapshot=snapshot,
            candidate=candidate,
            evaluation=evaluation,
            source=source,
        )

    def _build_single_candidate_pool_candidate(
        self,
        symbol: str,
        horizon: str,
        horizon_settings,
        snapshot_cache: dict[tuple[str, str], object],
    ):
        return self._build_single_prewatch_candidate(
            symbol,
            horizon,
            horizon_settings,
            snapshot_cache,
        )

    def _build_single_prewatch_candidate(
        self,
        symbol: str,
        horizon: str,
        horizon_settings,
        snapshot_cache: dict[tuple[str, str], object],
    ):
        snapshot_key = (symbol, horizon)
        snapshot = snapshot_cache.get(snapshot_key)
        if snapshot is None:
            snapshot = self._candidate_pool_market_data_engine().snapshot(symbol, horizon, include_intraday=False)
        candidate = build_candidate_pool_candidate(
            snapshot,
            horizon_settings,
            min_score=self.settings.candidate_pool_min_score,
        )
        return snapshot, candidate

    def _select_candidate_pool_scan_symbols(
        self,
        watchlist: set[str],
        snapshot_cache: dict[tuple[str, str], object],
    ) -> list[str]:
        return self._select_prewatch_scan_symbols(watchlist, snapshot_cache)

    def _select_prewatch_scan_symbols(
        self,
        watchlist: set[str],
        snapshot_cache: dict[tuple[str, str], object],
    ) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        current_symbols = [symbol.upper() for symbol, horizon in snapshot_cache if horizon == "position"]

        def append_symbol(symbol: str) -> None:
            normalized = symbol.upper()
            if normalized not in watchlist or normalized in seen:
                return
            if not self._is_candidate_pool_symbol_eligible(normalized):
                return
            if self._is_candidate_pool_failure_cooled_down(normalized):
                return
            seen.add(normalized)
            ordered.append(normalized)

        for symbol in current_symbols:
            append_symbol(symbol)
        for symbol in self._candidate_pool_watch_symbols():
            append_symbol(symbol)
        remaining = sorted(
            (symbol for symbol in watchlist if symbol.upper() not in seen),
            key=lambda symbol: (
                self._candidate_pool_theme_scan_priority(symbol),
                symbol,
            ),
            reverse=True,
        )
        for symbol in remaining:
            append_symbol(symbol)
        cap = self.settings.max_candidate_pool_scan_symbols_per_run
        if cap > 0:
            return ordered[:cap]
        return ordered

    def _candidate_pool_theme_scan_priority(self, symbol: str) -> float:
        normalized = symbol.upper()
        max_score = 0
        for theme_key in self.symbol_theme_map.get(normalized, []):
            member_count = len(self.theme_memberships.get(theme_key, set()))
            max_score = max(max_score, member_count)
        return max_score + self._recent_theme_scan_bonus(normalized)

    def _prewatch_theme_scan_priority(self, symbol: str) -> float:
        return self._candidate_pool_theme_scan_priority(symbol)

    def _is_candidate_pool_symbol_eligible(self, symbol: str) -> bool:
        normalized = symbol.upper()
        if "." in normalized or "-" in normalized:
            return False
        if len(normalized) == 5 and normalized.endswith(("Y", "F")):
            return False
        return True

    def _is_prewatch_symbol_eligible(self, symbol: str) -> bool:
        return self._is_candidate_pool_symbol_eligible(symbol)

    def _is_candidate_pool_failure_cooled_down(self, symbol: str) -> bool:
        raw = self.store.get_state(self._candidate_pool_failure_state_key(symbol))
        if not raw:
            raw = self.store.get_state(self._prewatch_failure_state_key(symbol))
        if not raw:
            return False
        try:
            previous = datetime.fromisoformat(raw)
        except ValueError:
            return False
        cooldown_seconds = self.settings.candidate_pool_failure_cooldown_minutes * 60
        return (utcnow() - previous).total_seconds() < cooldown_seconds

    def _is_prewatch_failure_cooled_down(self, symbol: str) -> bool:
        return self._is_candidate_pool_failure_cooled_down(symbol)

    def _record_candidate_pool_failure(self, symbol: str) -> None:
        timestamp = utcnow().isoformat()
        self.store.set_state(self._candidate_pool_failure_state_key(symbol), timestamp)
        self.store.set_state(self._prewatch_failure_state_key(symbol), timestamp)

    def _record_prewatch_failure(self, symbol: str) -> None:
        self._record_candidate_pool_failure(symbol)

    def _clear_candidate_pool_failure(self, symbol: str) -> None:
        self.store.set_state(self._candidate_pool_failure_state_key(symbol), "")
        self.store.set_state(self._prewatch_failure_state_key(symbol), "")

    def _clear_prewatch_failure(self, symbol: str) -> None:
        self._clear_candidate_pool_failure(symbol)

    def _dispatch_candidate_optional_notifications(
        self,
        candidates,
        *,
        macro_context: dict[str, object],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list[str]:
        if not candidates or not self.notifier.transport or self.notifier.dry_run:
            return []
        sent_symbols: list[str] = []
        for candidate in candidates:
            if candidate.score < self.settings.candidate_optional_alert_min_score:
                continue
            if (
                self.settings.max_candidate_optional_alerts_per_run > 0
                and len(sent_symbols) >= self.settings.max_candidate_optional_alerts_per_run
            ):
                break
            should_skip, skip_reason = self._should_skip_candidate_optional_alert(candidate)
            if should_skip:
                logger.info(
                    "candidate_optional_alert_skipped",
                    "Candidate optional alert skipped.",
                    stage="candidate_optional_notify",
                    symbol=candidate.symbol,
                    context={"reason": skip_reason},
                )
                continue
            notification_card = self._build_candidate_optional_notification_card(
                candidate,
                macro_context=macro_context,
                run_id=run_context.run_id,
            )
            title = f"[候选池] {self.notifier._title(notification_card)}"
            body = self.notifier._body(notification_card)
            try:
                self.notifier.transport.send(title, body)
            except Exception as exc:
                run_context.metrics.notification_failures += 1
                logger.error(
                    "candidate_optional_alert_failed",
                    "Candidate optional alert failed to deliver.",
                    stage="candidate_optional_notify",
                    symbol=candidate.symbol,
                    context={"error": exc.__class__.__name__},
                )
                continue
            self._record_candidate_optional_alert_sent(candidate)
            run_context.metrics.candidate_optional_alerts_sent_count += 1
            sent_symbols.append(candidate.symbol)
            logger.info(
                "candidate_optional_alert_sent",
                "Candidate optional alert delivered.",
                stage="candidate_optional_notify",
                symbol=candidate.symbol,
                context={"score": candidate.score, "horizon": candidate.horizon},
            )
        return sent_symbols

    def _dispatch_prewatch_notifications(
        self,
        candidates,
        *,
        macro_context: dict[str, object],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list[str]:
        return self._dispatch_candidate_optional_notifications(
            candidates,
            macro_context=macro_context,
            run_context=run_context,
            logger=logger,
        )

    def _build_candidate_optional_notification_card(
        self,
        candidate,
        *,
        macro_context: dict[str, object],
        run_id: str = "",
    ) -> OpportunityCard:
        now = utcnow()
        symbol = candidate.symbol.upper()
        theme_tags = theme_tags_for_symbol(
            symbol,
            self.symbol_theme_map,
            self.theme_display_name_map,
        )
        event_type = candidate.trigger_event_type or "news"
        confidence_label = "中" if candidate.score >= self.settings.candidate_optional_alert_min_score else "低"
        base_card = OpportunityCard(
            card_id=f"candidate-notify:{symbol}:{int(now.timestamp())}",
            event_id=f"candidate-notify:{symbol}:{candidate.as_of.isoformat()}",
            symbol=symbol,
            horizon=candidate.horizon,
            event_type=event_type,
            headline_summary=candidate.headline_summary,
            bull_case="若后续催化继续兑现、价格结构转强，这条候选机会有望升级。",
            bear_case="若量能衰减或价格结构转弱，这条候选机会可能快速失去跟踪价值。",
            event_score=round(candidate.score if candidate.trigger_mode == "event" else 0.0, 2),
            market_score=round(candidate.score if candidate.trigger_mode != "event" else candidate.score * 0.6, 2),
            final_score=round(candidate.score, 2),
            entry_range=PriceRange(candidate.last_price, candidate.last_price),
            take_profit_range=PriceRange(candidate.last_price, candidate.last_price),
            invalidation_level=round(candidate.support_20, 2),
            invalidation_reason="观察卡，仅供观察，不作为正式交易依据。",
            risk_notes=["观察卡", "正式执行前需结合真实行情与事件确认"],
            source_refs=[],
            created_at=now,
            ttl=now + timedelta(days=10 if candidate.horizon == "position" else 5),
            priority="suppressed",
            dedup_key=f"candidate-notify:{symbol}:{candidate.horizon}",
            bias="long",
            display_name=self._display_name_for(symbol),
            action_label="加入观察",
            confidence_label=confidence_label,
            confidence_score=round(candidate.score, 2),
            reason_to_watch=candidate.reason_to_watch,
            trend_state=candidate.trend_state,
            rsi_14=round(candidate.rsi_14, 1),
            relative_volume=round(candidate.relative_volume, 2),
            theme_tags=theme_tags,
            market_data_complete=True,
            lifecycle_pool="candidate",
            delivery_category="candidate_optional",
            promoted_from_prewatch=True,
            candidate_score=round(candidate.score, 2),
            candidate_setup_type=candidate.setup_type,
            positioning_hint="当前先放入候选池，不追价，等结构和催化进一步确认后再升级。",
        )
        return self._decorate_card_with_runtime_context(
            base_card,
            insight=self._synthetic_candidate_pool_insight(candidate, theme_tags=theme_tags),
            macro_context=macro_context,
            run_id=run_id,
            lite_narration=True,
        )

    def _build_prewatch_notification_card(
        self,
        candidate,
        *,
        macro_context: dict[str, object],
        run_id: str = "",
    ) -> OpportunityCard:
        return self._build_candidate_optional_notification_card(
            candidate,
            macro_context=macro_context,
            run_id=run_id,
        )

    def _synthetic_candidate_pool_insight(self, candidate, *, theme_tags: list[str]) -> EventInsight:
        event_type = candidate.trigger_event_type or "news"
        return EventInsight(
            event_id=f"candidate-insight:{candidate.symbol.upper()}:{candidate.as_of.isoformat()}",
            symbol=candidate.symbol.upper(),
            event_type=event_type,
            headline_summary=candidate.headline_summary,
            bull_case="若后续催化继续兑现且价格结构转强，市场更容易把它从观察升级为可执行机会。",
            bear_case="若后续缺少新增催化、量能衰减或结构转弱，观察价值会快速下降。",
            importance=round(candidate.score if candidate.trigger_mode == "event" else 58.0, 2),
            source_credibility=72.0,
            novelty=70.0 if candidate.trigger_mode == "event" else 56.0,
            sentiment=0.45,
            theme_relevance=82.0 if theme_tags else 60.0,
            llm_confidence=0.0,
            risk_notes=["候选池阶段更重视后续确认，不宜把观察信号直接等同于正式执行信号。"],
            source_refs=[],
            raw_payload={"mode": "synthetic_candidate"},
            created_at=utcnow(),
        )

    def _synthetic_prewatch_insight(self, candidate, *, theme_tags: list[str]) -> EventInsight:
        return self._synthetic_candidate_pool_insight(candidate, theme_tags=theme_tags)

    def _build_theme_supported_candidate_pool_candidates(
        self,
        snapshots: dict[str, object],
        *,
        horizon_settings,
        confirmation_cards: list,
        existing_symbols: set[str],
    ) -> list:
        if not snapshots or not self.symbol_theme_map:
            return []
        recent_theme_memory = self._load_recent_theme_heat_memory()
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        if not confirmed_symbols and not recent_theme_memory:
            return []
        relaxed_min_score = max(
            self.settings.candidate_pool_min_score - self.settings.candidate_pool_theme_relaxed_margin,
            0.0,
        )
        candidates = []
        for symbol, snapshot in snapshots.items():
            normalized = symbol.upper()
            if normalized in existing_symbols:
                continue
            context = summarize_symbol_theme_context(
                normalized,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
                candidate_symbols=existing_symbols,
            )
            if not context["confirmed_peer_symbols"] and not self._strongest_recent_theme_for_symbol(
                normalized,
                recent_theme_memory,
            ):
                continue
            candidate = build_candidate_pool_candidate(
                snapshot,
                horizon_settings,
                min_score=relaxed_min_score,
            )
            if candidate is None:
                continue
            candidates.append(candidate)
        return candidates

    def _build_theme_supported_prewatch_candidates(
        self,
        snapshots: dict[str, object],
        *,
        horizon_settings,
        confirmation_cards: list,
        existing_symbols: set[str],
    ) -> list:
        return self._build_theme_supported_candidate_pool_candidates(
            snapshots,
            horizon_settings=horizon_settings,
            confirmation_cards=confirmation_cards,
            existing_symbols=existing_symbols,
        )

    def _build_event_supported_candidate_pool_candidates(
        self,
        watchlist: set[str],
        *,
        horizon_settings,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        existing_symbols: set[str],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        if not watchlist or not confirmation_cards or not self.symbol_theme_map:
            return []
        active_themes = self._build_event_candidate_pool_theme_seeds(confirmation_cards)
        if not active_themes:
            return []
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        candidates = []
        for theme_key, seed in active_themes:
            theme_members = sorted(self.theme_memberships.get(theme_key, set()))
            added_for_theme = 0
            for symbol in theme_members:
                normalized = symbol.upper()
                if normalized not in watchlist:
                    continue
                if normalized in seed["trigger_symbols"]:
                    continue
                if normalized in existing_symbols or normalized in confirmed_symbols:
                    continue
                if not self._is_candidate_pool_symbol_eligible(normalized):
                    continue
                if self._is_candidate_pool_failure_cooled_down(normalized):
                    continue
                snapshot_key = (normalized, "position")
                snapshot = snapshot_cache.get(snapshot_key)
                try:
                    if snapshot is None:
                        snapshot = self._candidate_pool_market_data_engine().snapshot(
                            normalized,
                            "position",
                            include_intraday=False,
                        )
                        snapshot_cache[snapshot_key] = snapshot
                    if snapshot_key not in saved_snapshot_keys:
                        self.store.save_indicator_snapshot(snapshot)
                        saved_snapshot_keys.add(snapshot_key)
                except Exception as exc:
                    self._record_candidate_pool_failure(normalized)
                    run_context.metrics.candidate_pool_failures += 1
                    logger.warning(
                        "candidate_pool_failed",
                        "Event-driven candidate-pool generation failed for symbol.",
                        stage="candidate_pool",
                        symbol=normalized,
                        context={"error": exc.__class__.__name__, "mode": "event"},
                    )
                    continue
                self._clear_candidate_pool_failure(normalized)
                base_candidate = build_candidate_pool_candidate(
                    snapshot,
                    horizon_settings,
                    min_score=0.0,
                )
                if base_candidate is None:
                    continue
                boosted_score = round(base_candidate.score + seed["bonus"], 2)
                if boosted_score < self.settings.candidate_pool_event_min_score:
                    continue
                trigger_symbols = list(seed["trigger_symbols"])[:3]
                theme_name = display_theme_name(theme_key, self.theme_display_name_map)
                event_type_label = EVENT_TYPE_DISPLAY_NAMES.get(seed["event_type"], "事件")
                headline_summary = (
                    f"{normalized} 受到{theme_name}题材事件催化带动，适合先进入候选池，"
                    "等待个股结构和量价跟上。"
                )
                action_hint = (
                    "题材出现事件催化，可先进入候选池；若后续出现回踩承接、放量突破或新增催化，"
                    "再考虑升入确认池。"
                )
                reason_note = (
                    f"事件预热：{theme_name} 题材出现{event_type_label}催化，"
                    f"触发标的 {', '.join(trigger_symbols)}。"
                )
                reason_to_watch = (
                    f"{base_candidate.reason_to_watch}；{reason_note}"
                    if base_candidate.reason_to_watch
                    else reason_note
                )
                candidates.append(
                    replace(
                        base_candidate,
                        score=boosted_score,
                        headline_summary=headline_summary,
                        action_hint=action_hint,
                        reason_to_watch=reason_to_watch,
                        trigger_mode="event",
                        trigger_event_type=seed["event_type"],
                        trigger_theme=theme_name,
                        trigger_symbols=trigger_symbols,
                    )
                )
                existing_symbols.add(normalized)
                added_for_theme += 1
                if (
                    self.settings.max_event_prewatch_symbols_per_theme_per_run > 0
                    and added_for_theme >= self.settings.max_event_prewatch_symbols_per_theme_per_run
                ):
                    break
        return candidates

    def _build_event_supported_prewatch_candidates(
        self,
        watchlist: set[str],
        *,
        horizon_settings,
        confirmation_cards: list,
        snapshot_cache: dict[tuple[str, str], object],
        saved_snapshot_keys: set[tuple[str, str]],
        existing_symbols: set[str],
        run_context: RunContext,
        logger: StructuredLogger,
    ) -> list:
        return self._build_event_supported_candidate_pool_candidates(
            watchlist,
            horizon_settings=horizon_settings,
            confirmation_cards=confirmation_cards,
            snapshot_cache=snapshot_cache,
            saved_snapshot_keys=saved_snapshot_keys,
            existing_symbols=existing_symbols,
            run_context=run_context,
            logger=logger,
        )

    def _build_event_candidate_pool_theme_seeds(self, confirmation_cards: list) -> list[tuple[str, dict]]:
        seeds: dict[str, dict] = {}
        min_event_score = self.settings.candidate_pool_event_trigger_min_event_score
        for card in confirmation_cards:
            if card.priority == "suppressed" and getattr(card, "execution_eligible", True):
                continue
            if card.event_score < min_event_score:
                continue
            symbol = card.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                seed = seeds.get(theme_key)
                if seed is None:
                    seed = {
                        "theme_key": theme_key,
                        "trigger_symbols": set(),
                        "event_type": card.event_type,
                        "max_event_score": 0.0,
                        "bonus": 0.0,
                    }
                    seeds[theme_key] = seed
                seed["trigger_symbols"].add(symbol)
                if card.event_score >= seed["max_event_score"]:
                    seed["max_event_score"] = card.event_score
                    seed["event_type"] = card.event_type
        ranked = []
        for theme_key, seed in seeds.items():
            event_bonus = min(
                EVENT_PREWATCH_BASE_BONUS
                + max(seed["max_event_score"] - min_event_score, 0.0) / 2.0,
                self.settings.candidate_pool_event_bonus_cap,
            )
            seed["bonus"] = round(event_bonus, 2)
            ranked.append((theme_key, seed))
        ranked.sort(
            key=lambda item: (
                -float(item[1]["max_event_score"]),
                -len(item[1]["trigger_symbols"]),
                item[0],
            )
        )
        return ranked

    def _build_event_prewatch_theme_seeds(self, confirmation_cards: list) -> list[tuple[str, dict]]:
        return self._build_event_candidate_pool_theme_seeds(confirmation_cards)

    def _is_candidate_optional_alert_cooled_down(self, symbol: str) -> bool:
        previous = self._load_candidate_optional_alert_state(symbol).get("sent_at")
        if previous is None:
            return False
        return (utcnow() - previous).total_seconds() < self.settings.candidate_optional_alert_cooldown_minutes * 60

    def _is_prewatch_alert_cooled_down(self, symbol: str) -> bool:
        return self._is_candidate_optional_alert_cooled_down(symbol)

    def _should_skip_candidate_optional_alert(self, candidate) -> tuple[bool, str]:
        cycle_state = self._load_active_cycle_state(candidate.symbol)
        cycle_status = str(cycle_state.get("status") or "terminal")
        if cycle_status == "holding_active":
            return True, "holding_active"
        if cycle_status == "pending_entry":
            return True, "pending_entry"
        if self._is_candidate_optional_alert_cooled_down(candidate.symbol):
            return True, "cooldown_active"
        state = self._load_candidate_optional_alert_state(candidate.symbol)
        previous = state.get("sent_at")
        if previous is None:
            return False, ""
        repeat_window_seconds = max(self.settings.candidate_optional_alert_repeat_window_minutes, 0) * 60
        if repeat_window_seconds <= 0:
            return False, ""
        if (utcnow() - previous).total_seconds() >= repeat_window_seconds:
            return False, ""
        previous_signature = str(state.get("signature") or "").strip()
        if not previous_signature:
            return False, ""
        current_signature = self._candidate_optional_alert_signature(candidate)
        score_delta = abs(float(candidate.score) - float(state.get("score") or 0.0))
        if (
            current_signature == previous_signature
            and score_delta < max(self.settings.candidate_optional_alert_repeat_min_score_delta, 0.0)
        ):
            return True, "content_unchanged_within_repeat_window"
        return False, ""

    def _should_skip_prewatch_alert(self, candidate) -> tuple[bool, str]:
        return self._should_skip_candidate_optional_alert(candidate)

    def _candidate_optional_alert_state_key(self, symbol: str) -> str:
        return f"candidate_optional_alert:{symbol.upper()}"

    def _prewatch_alert_state_key(self, symbol: str) -> str:
        return f"prewatch_alert:{symbol.upper()}"

    def _load_candidate_optional_alert_state(self, symbol: str) -> dict[str, Any]:
        raw = self.store.get_state(self._candidate_optional_alert_state_key(symbol))
        if not raw:
            raw = self.store.get_state(self._prewatch_alert_state_key(symbol))
        if not raw:
            return {}
        raw = raw.strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            try:
                sent_at = datetime.fromisoformat(raw)
            except ValueError:
                return {}
            return {"sent_at": sent_at}
        sent_at_raw = payload.get("sent_at")
        try:
            sent_at = datetime.fromisoformat(str(sent_at_raw))
        except (TypeError, ValueError):
            return {}
        return {
            "sent_at": sent_at,
            "score": float(payload.get("score") or 0.0),
            "signature": str(payload.get("signature") or ""),
        }

    def _load_prewatch_alert_state(self, symbol: str) -> dict[str, Any]:
        return self._load_candidate_optional_alert_state(symbol)

    def _candidate_optional_alert_signature(self, candidate) -> str:
        payload = {
            "symbol": candidate.symbol,
            "horizon": candidate.horizon,
            "setup_type": candidate.setup_type,
            "headline_summary": candidate.headline_summary,
            "action_hint": candidate.action_hint,
            "reason_to_watch": candidate.reason_to_watch,
        }
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha1(serialized.encode("utf-8")).hexdigest()

    def _prewatch_alert_signature(self, candidate) -> str:
        return self._candidate_optional_alert_signature(candidate)

    def _record_candidate_optional_alert_sent(self, candidate) -> None:
        sent_at = utcnow().isoformat()
        payload = json.dumps(
            {
                "sent_at": sent_at,
                "score": round(float(candidate.score), 2),
                "signature": self._candidate_optional_alert_signature(candidate),
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        self.store.set_state(self._candidate_optional_alert_state_key(candidate.symbol), payload)
        self.store.set_state(self._prewatch_alert_state_key(candidate.symbol), payload)
        state = self._load_candidate_pool_state(candidate.symbol)
        if not state:
            self._record_candidate_pool_candidate(candidate)
            state = self._load_candidate_pool_state(candidate.symbol)
        if not state:
            return
        state["alert_sent_count"] = max(int(state.get("alert_sent_count") or 0) + 1, 1)
        state["last_alert_sent_at"] = sent_at
        self._save_candidate_pool_state(candidate.symbol, state)

    def _record_prewatch_alert_sent(self, candidate) -> None:
        self._record_candidate_optional_alert_sent(candidate)

    def _candidate_pool_failure_state_key(self, symbol: str) -> str:
        return f"candidate_pool_failure:{symbol.upper()}"

    def _prewatch_failure_state_key(self, symbol: str) -> str:
        return f"prewatch_failure:{symbol.upper()}"

    def _candidate_pool_state_key(self, symbol: str) -> str:
        return f"candidate_pool:{symbol.upper()}"

    def _prewatch_candidate_state_key(self, symbol: str) -> str:
        return f"prewatch_candidate:{symbol.upper()}"

    def _load_candidate_pool_state(self, symbol: str) -> dict[str, Any]:
        raw = self.store.get_state(self._candidate_pool_state_key(symbol))
        if not raw:
            raw = self.store.get_state(self._prewatch_candidate_state_key(symbol))
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        candidate_payload: dict[str, Any]
        if isinstance(payload.get("candidate"), dict):
            candidate_payload = dict(payload.get("candidate") or {})
        elif "as_of" in payload and "setup_type" in payload:
            candidate_payload = dict(payload)
        else:
            return {}
        candidate_payload["symbol"] = str(candidate_payload.get("symbol") or symbol).upper()
        as_of_text = str(candidate_payload.get("as_of") or "")
        return {
            "candidate": candidate_payload,
            "first_seen_at": str(payload.get("first_seen_at") or as_of_text),
            "last_seen_at": str(payload.get("last_seen_at") or as_of_text),
            "observation_count": max(int(payload.get("observation_count") or 1), 1),
            "alert_sent_count": max(int(payload.get("alert_sent_count") or 0), 0),
            "last_alert_sent_at": str(payload.get("last_alert_sent_at") or ""),
            "latest_candidate_decision_id": str(
                payload.get("latest_candidate_decision_id") or payload.get("latest_prewatch_decision_id") or ""
            ),
            "latest_candidate_event_id": str(
                payload.get("latest_candidate_event_id") or payload.get("latest_prewatch_event_id") or ""
            ),
            "latest_candidate_run_id": str(
                payload.get("latest_candidate_run_id") or payload.get("latest_prewatch_run_id") or ""
            ),
            "latest_prewatch_decision_id": str(payload.get("latest_prewatch_decision_id") or ""),
            "latest_prewatch_event_id": str(payload.get("latest_prewatch_event_id") or ""),
            "latest_prewatch_run_id": str(payload.get("latest_prewatch_run_id") or ""),
        }

    def _load_prewatch_candidate_state(self, symbol: str) -> dict[str, Any]:
        return self._load_candidate_pool_state(symbol)

    def _save_candidate_pool_state(self, symbol: str, state: dict[str, Any]) -> None:
        payload = json.dumps(state, sort_keys=True, ensure_ascii=False)
        self.store.set_state(self._candidate_pool_state_key(symbol), payload)
        self.store.set_state(self._prewatch_candidate_state_key(symbol), payload)

    def _save_prewatch_candidate_state(self, symbol: str, state: dict[str, Any]) -> None:
        self._save_candidate_pool_state(symbol, state)

    def _record_candidate_pool_candidate(self, candidate) -> None:
        candidate_record = candidate.to_record()
        previous_state = self._load_candidate_pool_state(candidate.symbol)
        previous_candidate = previous_state.get("candidate") or {}
        repeated_same_observation = (
            str(previous_candidate.get("as_of") or "") == str(candidate_record.get("as_of") or "")
            and str(previous_candidate.get("setup_type") or "") == str(candidate_record.get("setup_type") or "")
            and float(previous_candidate.get("score") or 0.0) == float(candidate_record.get("score") or 0.0)
        )
        observation_count = max(int(previous_state.get("observation_count") or 0), 0)
        if not repeated_same_observation:
            observation_count += 1
        state = {
            "candidate": candidate_record,
            "first_seen_at": str(previous_state.get("first_seen_at") or candidate_record.get("as_of") or ""),
            "last_seen_at": str(candidate_record.get("as_of") or previous_state.get("last_seen_at") or ""),
            "observation_count": max(observation_count, 1),
            "alert_sent_count": max(int(previous_state.get("alert_sent_count") or 0), 0),
            "last_alert_sent_at": str(previous_state.get("last_alert_sent_at") or ""),
            "latest_candidate_decision_id": str(
                previous_state.get("latest_candidate_decision_id")
                or previous_state.get("latest_prewatch_decision_id")
                or ""
            ),
            "latest_candidate_event_id": str(
                previous_state.get("latest_candidate_event_id")
                or previous_state.get("latest_prewatch_event_id")
                or ""
            ),
            "latest_candidate_run_id": str(
                previous_state.get("latest_candidate_run_id")
                or previous_state.get("latest_prewatch_run_id")
                or ""
            ),
            "latest_prewatch_decision_id": str(previous_state.get("latest_prewatch_decision_id") or ""),
            "latest_prewatch_event_id": str(previous_state.get("latest_prewatch_event_id") or ""),
            "latest_prewatch_run_id": str(previous_state.get("latest_prewatch_run_id") or ""),
        }
        self._save_candidate_pool_state(candidate.symbol, state)

    def _record_prewatch_candidate(self, candidate) -> None:
        self._record_candidate_pool_candidate(candidate)

    def _load_recent_candidate_pool_context(self, symbol: str) -> dict | None:
        state = self._load_candidate_pool_state(symbol)
        if not state:
            return None
        payload = dict(state.get("candidate") or {})
        timestamp_text = str(state.get("last_seen_at") or payload.get("as_of") or "")
        try:
            as_of = datetime.fromisoformat(timestamp_text)
        except (ValueError, TypeError):
            return None
        age_seconds = (utcnow() - as_of).total_seconds()
        if age_seconds > max(self.settings.candidate_pool_promotion_window_hours, 0) * 3600:
            return None
        payload["observation_count"] = max(int(state.get("observation_count") or 1), 1)
        payload["first_seen_at"] = str(state.get("first_seen_at") or payload.get("as_of") or "")
        payload["last_seen_at"] = str(state.get("last_seen_at") or payload.get("as_of") or "")
        payload["alert_sent_count"] = max(int(state.get("alert_sent_count") or 0), 0)
        payload["last_alert_sent_at"] = str(state.get("last_alert_sent_at") or "")
        payload["latest_candidate_decision_id"] = str(
            state.get("latest_candidate_decision_id") or state.get("latest_prewatch_decision_id") or ""
        )
        payload["latest_candidate_event_id"] = str(
            state.get("latest_candidate_event_id") or state.get("latest_prewatch_event_id") or ""
        )
        payload["latest_candidate_run_id"] = str(
            state.get("latest_candidate_run_id") or state.get("latest_prewatch_run_id") or ""
        )
        payload["latest_prewatch_decision_id"] = str(state.get("latest_prewatch_decision_id") or "")
        payload["latest_prewatch_event_id"] = str(state.get("latest_prewatch_event_id") or "")
        payload["latest_prewatch_run_id"] = str(state.get("latest_prewatch_run_id") or "")
        return payload

    def _load_recent_prewatch_candidate(self, symbol: str) -> dict | None:
        return self._load_recent_candidate_pool_context(symbol)

    def _record_candidate_pool_decision_reference(
        self,
        *,
        symbol: str,
        decision_id: str,
        event_id: str,
        run_id: str,
    ) -> None:
        state = self._load_candidate_pool_state(symbol)
        if not state:
            return
        state["latest_candidate_decision_id"] = decision_id
        state["latest_candidate_event_id"] = event_id
        state["latest_candidate_run_id"] = run_id
        state["latest_prewatch_decision_id"] = decision_id
        state["latest_prewatch_event_id"] = event_id
        state["latest_prewatch_run_id"] = run_id
        self._save_candidate_pool_state(symbol, state)

    def _record_prewatch_decision_reference(
        self,
        *,
        symbol: str,
        decision_id: str,
        event_id: str,
        run_id: str,
    ) -> None:
        self._record_candidate_pool_decision_reference(
            symbol=symbol,
            decision_id=decision_id,
            event_id=event_id,
            run_id=run_id,
        )

    def _build_candidate_pool_promotion_reason(self, candidate_pool_context: dict[str, Any]) -> str:
        symbol = str(candidate_pool_context.get("symbol") or "").strip().upper()
        observation_count = max(self._recent_candidate_observation_count_72h(symbol), 1) if symbol else 1
        alert_sent_count = max(int(candidate_pool_context.get("alert_sent_count") or 0), 0)
        detail = f"近72h进入候选池 {observation_count} 次"
        if alert_sent_count > 0:
            last_alert_text = format_beijing_minute(candidate_pool_context.get("last_alert_sent_at"))
            detail += f"，期间已发观察卡 {alert_sent_count} 次（最近一次 {last_alert_text}）"
        return f"此前已进入候选池，{detail}，本轮事件达到确认条件。"

    def _build_prewatch_promotion_reason(self, prewatch_context: dict[str, Any]) -> str:
        return self._build_candidate_pool_promotion_reason(prewatch_context)

    def _apply_candidate_pool_promotion(self, card, *, insight, candidate_pool_context: dict | None):
        if not candidate_pool_context:
            return card
        if card.event_score < self.settings.candidate_confirmation_min_event_score:
            return card
        setup_type = str(candidate_pool_context.get("setup_type", ""))
        setup_label = {
            "breakout_watch": "突破预热",
            "pullback_watch": "回踩蓄势",
            "relative_strength_watch": "相对强势",
        }.get(setup_type, setup_type or "候选池")
        observation_count = max(int(candidate_pool_context.get("observation_count") or 1), 1)
        recent_observation_count_72h = max(self._recent_candidate_observation_count_72h(card.symbol), 1)
        alert_sent_count = max(int(candidate_pool_context.get("alert_sent_count") or 0), 0)
        lifecycle_hint = f"近72h进入候选池 {recent_observation_count_72h} 次"
        if alert_sent_count > 0:
            lifecycle_hint += f"，期间已发观察卡 {alert_sent_count} 次"
        positioning_hint = (
            f"该标的此前已进入候选池（{setup_label}，{float(candidate_pool_context.get('score', 0.0)):.1f} 分，{lifecycle_hint}），"
            "本次事件触发确认，可从观察/轻仓阶段切换到正式确认。"
        )
        promotion_reason = self._build_candidate_pool_promotion_reason(candidate_pool_context)
        reason_to_watch = card.reason_to_watch
        if promotion_reason not in reason_to_watch:
            reason_to_watch = f"{reason_to_watch} {promotion_reason}".strip()
        promoted_card = replace(
            card,
            promoted_from_prewatch=True,
            candidate_score=round(float(candidate_pool_context.get("score", 0.0)), 2),
            candidate_setup_type=setup_type,
            candidate_observation_count=observation_count,
            candidate_alert_sent_count=alert_sent_count,
            candidate_first_seen_at=str(candidate_pool_context.get("first_seen_at") or ""),
            candidate_last_seen_at=str(candidate_pool_context.get("last_seen_at") or ""),
            candidate_last_alert_sent_at=str(candidate_pool_context.get("last_alert_sent_at") or ""),
            candidate_source_decision_id=str(
                candidate_pool_context.get("latest_candidate_decision_id")
                or candidate_pool_context.get("latest_prewatch_decision_id")
                or ""
            ),
            candidate_promotion_reason=promotion_reason,
            recent_candidate_observation_count_72h=recent_observation_count_72h,
            action_label="确认做多" if card.bias == "long" else card.action_label,
            positioning_hint=positioning_hint,
            reason_to_watch=reason_to_watch,
        )
        if not card.market_data_complete:
            return promoted_card
        horizon_settings = self.settings.horizons[card.horizon]
        if promoted_card.market_score < horizon_settings.market_score_threshold:
            return promoted_card
        bonus = max(self.settings.candidate_confirmation_bonus, 0.0)
        if bonus <= 0:
            return promoted_card
        boosted_final = round(promoted_card.final_score + bonus, 2)
        boosted_priority = promoted_card.priority
        if boosted_final >= horizon_settings.priority_threshold:
            boosted_priority = "high"
        elif boosted_final >= 60.0 and promoted_card.event_score >= self.settings.event_score_threshold:
            boosted_priority = "normal"
        return replace(
            promoted_card,
            final_score=boosted_final,
            priority=boosted_priority,
        )

    def _apply_prewatch_promotion(self, card, *, insight, prewatch_context: dict | None):
        return self._apply_candidate_pool_promotion(
            card,
            insight=insight,
            candidate_pool_context=prewatch_context,
        )

    def _apply_theme_linkage_to_candidate_pool_candidates(
        self,
        candidates: list,
        *,
        confirmation_cards: list,
    ) -> list:
        if not candidates or not self.symbol_theme_map:
            return candidates
        recent_theme_memory = self._load_recent_theme_heat_memory()
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        candidate_symbols = {candidate.symbol.upper() for candidate in candidates}
        themed_candidates = []
        for candidate in candidates:
            context = summarize_symbol_theme_context(
                candidate.symbol,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
                candidate_symbols=candidate_symbols,
            )
            bonus = 0.0
            notes: list[str] = []
            if context["confirmed_peer_symbols"]:
                bonus += THEME_PREWATCH_CONFIRMED_BONUS
                notes.append(f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}")
            elif context["candidate_peer_symbols"]:
                bonus += THEME_PREWATCH_CLUSTER_BONUS
                notes.append(f"同题材预热共振：{', '.join(context['candidate_peer_symbols'][:3])}")
            else:
                recent_theme = self._strongest_recent_theme_for_symbol(
                    candidate.symbol,
                    recent_theme_memory,
                )
                if recent_theme is not None:
                    theme_key, entry = recent_theme
                    heat_score = float(entry.get("heat_score", 0.0))
                    memory_bonus = min(
                        max(heat_score, 0.0),
                        self.settings.candidate_pool_theme_memory_bonus,
                    )
                    if memory_bonus > 0:
                        bonus += memory_bonus
                        notes.append(
                            f"题材近期持续活跃：{display_theme_name(theme_key, self.theme_display_name_map)}"
                        )
            if bonus <= 0:
                themed_candidates.append(candidate)
                continue
            reason_to_watch = candidate.reason_to_watch
            for note in notes:
                if note not in reason_to_watch:
                    reason_to_watch = f"{reason_to_watch}；{note}" if reason_to_watch else note
            themed_candidates.append(
                replace(
                    candidate,
                    score=round(candidate.score + bonus, 2),
                    reason_to_watch=reason_to_watch,
                )
            )
        return themed_candidates

    def _apply_theme_linkage_to_prewatch_candidates(self, candidates: list, *, confirmation_cards: list) -> list:
        return self._apply_theme_linkage_to_candidate_pool_candidates(
            candidates,
            confirmation_cards=confirmation_cards,
        )

    def _recent_theme_scan_bonus(self, symbol: str) -> float:
        recent_theme = self._strongest_recent_theme_for_symbol(
            symbol,
            self._load_recent_theme_heat_memory(),
        )
        if recent_theme is None:
            return 0.0
        _, entry = recent_theme
        return min(
            max(float(entry.get("heat_score", 0.0)), 0.0),
            self.settings.candidate_pool_theme_memory_scan_bonus,
        )

    def _strongest_recent_theme_for_symbol(
        self,
        symbol: str,
        recent_theme_memory: dict[str, dict] | None = None,
    ) -> tuple[str, dict] | None:
        if not self.symbol_theme_map:
            return None
        memory = recent_theme_memory or self._load_recent_theme_heat_memory()
        if not memory:
            return None
        strongest: tuple[str, dict] | None = None
        for theme_key in self.symbol_theme_map.get(symbol.upper(), []):
            entry = memory.get(theme_key)
            if entry is None:
                continue
            if float(entry.get("heat_score", 0.0)) < self.settings.candidate_pool_theme_memory_min_heat_score:
                continue
            if strongest is None or float(entry.get("heat_score", 0.0)) > float(
                strongest[1].get("heat_score", 0.0)
            ):
                strongest = (theme_key, entry)
        return strongest

    def _load_recent_theme_heat_memory(self) -> dict[str, dict]:
        raw = self.store.get_state(THEME_MEMORY_STATE_KEY)
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
            as_of = datetime.fromisoformat(payload["as_of"])
            rows = payload.get("themes", [])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return {}
        age_seconds = (utcnow() - as_of).total_seconds()
        if age_seconds > max(self.settings.candidate_pool_theme_memory_window_hours, 0) * 3600:
            return {}
        memory: dict[str, dict] = {}
        for row in rows:
            theme_key = str(row.get("theme_key", "")).strip()
            if not theme_key:
                continue
            memory[theme_key] = {
                "heat_score": float(row.get("heat_score", 0.0)),
                "confirmed_symbols": list(row.get("confirmed_symbols", [])),
                "candidate_pool_symbols": list(row.get("candidate_pool_symbols", row.get("prewatch_symbols", []))),
                "prewatch_symbols": list(row.get("prewatch_symbols", row.get("candidate_pool_symbols", []))),
            }
        return memory

    def _persist_theme_heat_memory(self, confirmation_cards: list, candidate_pool_candidates: list) -> None:
        if not self.symbol_theme_map:
            self.store.set_state(
                THEME_MEMORY_STATE_KEY,
                json.dumps({"as_of": utcnow().isoformat(), "themes": []}, sort_keys=True),
            )
            return
        theme_confirmed: dict[str, set[str]] = {}
        theme_promoted: dict[str, set[str]] = {}
        theme_candidate_pool: dict[str, set[str]] = {}
        for card in confirmation_cards:
            if card.priority == "suppressed":
                continue
            symbol = card.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_confirmed.setdefault(theme_key, set()).add(symbol)
                if card.promoted_from_prewatch:
                    theme_promoted.setdefault(theme_key, set()).add(symbol)
        for candidate in candidate_pool_candidates:
            symbol = candidate.symbol.upper()
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_candidate_pool.setdefault(theme_key, set()).add(symbol)
        rows = []
        all_theme_keys = set(theme_confirmed) | set(theme_promoted) | set(theme_candidate_pool)
        for theme_key in sorted(all_theme_keys):
            confirmed_symbols = sorted(theme_confirmed.get(theme_key, set()))
            promoted_symbols = sorted(theme_promoted.get(theme_key, set()))
            candidate_pool_symbols = sorted(theme_candidate_pool.get(theme_key, set()))
            heat_score = len(promoted_symbols) * 3 + len(confirmed_symbols) * 2 + len(candidate_pool_symbols)
            rows.append(
                {
                    "theme_key": theme_key,
                    "heat_score": float(heat_score),
                    "confirmed_symbols": confirmed_symbols,
                    "candidate_pool_symbols": candidate_pool_symbols,
                    "prewatch_symbols": candidate_pool_symbols,
                }
            )
        self.store.set_state(
            THEME_MEMORY_STATE_KEY,
            json.dumps(
                {
                    "as_of": utcnow().isoformat(),
                    "themes": rows,
                },
                sort_keys=True,
            ),
        )

    def _apply_theme_linkage_to_confirmation_candidates(self, cards: list) -> list:
        if not cards or not self.symbol_theme_map:
            return cards
        confirmed_symbols = {
            card.symbol.upper()
            for card in cards
            if card.priority != "suppressed"
        }
        adjusted_cards = []
        for card in cards:
            context = summarize_symbol_theme_context(
                card.symbol,
                symbol_theme_map=self.symbol_theme_map,
                theme_memberships=self.theme_memberships,
                confirmed_symbols=confirmed_symbols,
            )
            base_theme_tags = theme_tags_for_symbol(
                card.symbol,
                self.symbol_theme_map,
                self.theme_display_name_map,
            )
            if not context["confirmed_peer_symbols"]:
                adjusted_cards.append(
                    replace(
                        card,
                        theme_tags=base_theme_tags,
                        confirmed_peer_symbols=[],
                    )
                )
                continue
            peer_note = f"同题材已有确认标的：{', '.join(context['confirmed_peer_symbols'][:3])}"
            reason_to_watch = card.reason_to_watch
            if peer_note not in reason_to_watch:
                reason_to_watch = f"{reason_to_watch} {peer_note}".strip()
            positioning_hint = card.positioning_hint
            if card.promoted_from_prewatch:
                theme_hint = "题材联动正在形成，可按确认链条而不是单点新闻来跟踪仓位。"
                if theme_hint not in positioning_hint:
                    positioning_hint = f"{positioning_hint} {theme_hint}".strip()
            adjusted = replace(
                card,
                reason_to_watch=reason_to_watch,
                positioning_hint=positioning_hint,
                theme_tags=base_theme_tags,
                confirmed_peer_symbols=context["confirmed_peer_symbols"],
            )
            if card.promoted_from_prewatch and card.market_data_complete:
                horizon_settings = self.settings.horizons[card.horizon]
                boosted_final = round(adjusted.final_score + THEME_CONFIRMATION_CHAIN_BONUS, 2)
                boosted_priority = adjusted.priority
                if boosted_final >= horizon_settings.priority_threshold:
                    boosted_priority = "high"
                elif boosted_final >= 60.0 and adjusted.event_score >= self.settings.event_score_threshold:
                    boosted_priority = "normal"
                adjusted = replace(
                    adjusted,
                    final_score=boosted_final,
                    priority=boosted_priority,
                )
            adjusted_cards.append(adjusted)
        return adjusted_cards

    def _build_symbol_theme_heat(self, cards: list) -> dict[str, float]:
        if not cards or not self.symbol_theme_map:
            return {}
        theme_heat: dict[str, float] = {}
        theme_confirmed: dict[str, set[str]] = {}
        theme_promoted: dict[str, set[str]] = {}
        for card in cards:
            symbol = card.symbol.upper()
            if symbol not in self.symbol_theme_map or card.priority == "suppressed":
                continue
            for theme_key in self.symbol_theme_map.get(symbol, []):
                theme_confirmed.setdefault(theme_key, set()).add(symbol)
                if card.promoted_from_prewatch:
                    theme_promoted.setdefault(theme_key, set()).add(symbol)
        for symbol, theme_keys in self.symbol_theme_map.items():
            score = 0.0
            for theme_key in theme_keys:
                score = max(
                    score,
                    len(theme_confirmed.get(theme_key, set())) * 2.0
                    + len(theme_promoted.get(theme_key, set())) * 2.0,
                )
            if score > 0:
                theme_heat[symbol] = score
        return theme_heat

    def _build_confirmation_decision_packets(
        self,
        cards: list,
        *,
        card_contexts: dict[str, dict],
    ) -> list[DecisionPacket]:
        confirmed_symbols = {
            card.symbol.upper()
            for card in cards
            if card.priority != "suppressed"
        }
        packets: list[DecisionPacket] = []
        for card in cards:
            context = card_contexts.get(card.card_id, {})
            event_assessment = context.get("event_assessment")
            market_assessment = context.get("market_assessment")
            if event_assessment is None or market_assessment is None:
                continue
            if not getattr(card, "execution_eligible", True):
                continue
            market_confirmation_score = market_assessment.market_confirmation_score
            if market_confirmation_score is None:
                market_confirmation_score = card.market_score or 0.0
            rsi_value = (
                market_assessment.rsi_14
                if market_assessment.rsi_14 is not None
                else card.rsi_14
            )
            relative_volume = (
                market_assessment.relative_volume
                if market_assessment.relative_volume is not None
                else card.relative_volume
            )
            theme_assessment = self.theme_engine.assess_confirmation(
                card,
                confirmed_symbols=confirmed_symbols,
            )
            theme_score = round(
                theme_assessment.theme_heat + sum(theme_assessment.theme_boosts.values()),
                2,
            )
            reason_sections = ReasonSections(
                event=event_assessment.headline_summary,
                market=market_assessment.market_data_note
                if not market_assessment.market_data_complete
                else (
                    f"市场确认 {market_confirmation_score:.1f} 分，当前为"
                    f"{_display_trend_state(card.trend_state)}结构，RSI {float(rsi_value or 0.0):.1f}，相对量能 {float(relative_volume or 0.0):.2f} 倍。"
                ),
                theme=theme_assessment.theme_chain_note or (f"题材：{' / '.join(theme_assessment.theme_ids)}" if theme_assessment.theme_ids else ""),
                positioning=(
                    f"{card.positioning_hint} 此前已进入候选池，当前事件与市场确认形成共振。"
                    if card.promoted_from_prewatch
                    else card.positioning_hint
                ),
            )
            price_plan = market_assessment.entry_plan or PricePlan(
                entry_range=card.entry_range,
                take_profit_range=card.take_profit_range,
                invalidation_level=card.invalidation_level,
                invalidation_reason=card.invalidation_reason,
                ttl_iso=card.ttl.isoformat(),
            )
            packets.append(
                DecisionPacket(
                    pool="confirmation",
                    action=self._resolved_confirmation_action(card),
                    priority=card.priority,
                    confidence=card.confidence_label,
                    symbol=card.symbol,
                    horizon=card.horizon,
                    event_id=card.event_id,
                    event_assessment=event_assessment,
                    market_assessment=market_assessment,
                    theme_assessment=theme_assessment,
                    price_plan=price_plan,
                    reason_sections=reason_sections,
                    source_bundle=SourceBundle(
                        refs=list(card.source_refs),
                        quality_tier=event_assessment.source_quality_tier,
                    ),
                    final_score=card.final_score,
                    event_score=card.event_score,
                    market_score=card.market_score,
                    theme_score=theme_score,
                    trigger_mode="promoted" if card.promoted_from_prewatch else "direct",
                    llm_used=event_assessment.llm_used,
                )
            )
        return packets

    def _apply_confirmation_packets(self, cards: list, packets: list[DecisionPacket]) -> list:
        packets_by_key = {(packet.symbol, packet.horizon, packet.event_id): packet for packet in packets}
        updated = []
        for card in cards:
            packet = packets_by_key.get((card.symbol, card.horizon, card.event_id))
            if packet is None:
                updated.append(card)
                continue
            updated.append(decision_packet_to_card(packet, card))
        return updated

    def _build_candidate_pool_decision_packets(
        self,
        candidates: list,
        *,
        confirmation_cards: list,
    ) -> list[DecisionPacket]:
        confirmed_symbols = {
            card.symbol.upper()
            for card in confirmation_cards
            if card.priority != "suppressed"
        }
        prewatch_symbols = {candidate.symbol.upper() for candidate in candidates}
        packets: list[DecisionPacket] = []
        for candidate in candidates:
            theme_assessment = self.theme_engine.assess_candidate_pool(
                candidate,
                confirmed_symbols=confirmed_symbols,
                candidate_symbols=prewatch_symbols,
            )
            event_strength = candidate.score if candidate.trigger_mode == "event" else 0.0
            market_strength = candidate.score if candidate.trigger_mode != "event" else 0.0
            event_assessment = self._synthetic_candidate_pool_event_assessment(candidate)
            market_assessment = self._synthetic_candidate_pool_market_assessment(candidate)
            reason_sections = ReasonSections(
                event=event_assessment.headline_summary,
                market=(
                    f"结构状态：{_display_trend_state(candidate.trend_state)}，RSI {candidate.rsi_14:.1f}，"
                    f"相对量能 {candidate.relative_volume:.2f} 倍。"
                ),
                theme=theme_assessment.theme_chain_note or (f"题材：{' / '.join(theme_assessment.theme_ids)}" if theme_assessment.theme_ids else ""),
                positioning=candidate.action_hint,
            )
            packets.append(
                DecisionPacket(
                    pool="candidate_pool",
                    action="加入观察",
                    priority="normal",
                    confidence="中" if candidate.score >= self.settings.candidate_optional_alert_min_score else "低",
                    symbol=candidate.symbol,
                    horizon=candidate.horizon,
                    event_id=f"candidate_pool:{candidate.symbol}:{candidate.as_of.isoformat()}",
                    event_assessment=event_assessment,
                    market_assessment=market_assessment,
                    theme_assessment=theme_assessment,
                    price_plan=None,
                    reason_sections=reason_sections,
                    source_bundle=SourceBundle(refs=[], quality_tier=event_assessment.source_quality_tier),
                    final_score=candidate.score,
                    event_score=event_strength,
                    market_score=market_strength,
                    theme_score=round(theme_assessment.theme_heat + sum(theme_assessment.theme_boosts.values()), 2),
                    trigger_mode=candidate.trigger_mode,
                    llm_used=False,
                )
            )
        return packets

    def _build_prewatch_decision_packets(
        self,
        candidates: list,
        *,
        confirmation_cards: list,
    ) -> list[DecisionPacket]:
        return self._build_candidate_pool_decision_packets(
            candidates,
            confirmation_cards=confirmation_cards,
        )

    def _synthetic_candidate_pool_event_assessment(self, candidate) -> object:
        from .decision_engines.types import EventAssessment

        return EventAssessment(
            symbol=candidate.symbol,
            event_type=candidate.trigger_event_type or "prewatch",
            event_strength=round(candidate.score if candidate.trigger_mode == "event" else 0.0, 2),
            direction="bullish",
            source_quality_tier="secondary",
            credibility=75.0,
            novelty=70.0 if candidate.trigger_mode == "event" else 55.0,
            theme_candidates=[candidate.trigger_theme] if candidate.trigger_theme else [],
            reason_fragments=[candidate.headline_summary, candidate.reason_to_watch],
            llm_used=False,
            llm_confidence=0.0,
            event_candidate_pool_eligible=True,
            headline_summary=candidate.headline_summary,
            source_refs=[],
            risk_notes=[],
        )

    def _synthetic_prewatch_event_assessment(self, candidate) -> object:
        return self._synthetic_candidate_pool_event_assessment(candidate)

    def _synthetic_candidate_pool_market_assessment(self, candidate) -> object:
        from .decision_engines.types import MarketAssessment

        return MarketAssessment(
            trend_state=candidate.trend_state,
            momentum_state="healthy" if 48.0 <= candidate.rsi_14 <= 65.0 else "recovering",
            volume_state="elevated" if candidate.relative_volume >= 1.2 else "normal",
            position_state=candidate.setup_type,
            volatility_state="normal",
            market_confirmation_score=round(candidate.score, 2),
            entry_plan=None,
            risk_flags=[],
            candidate_pool_structure_eligible=True,
            exit_signal_state="hold",
            market_data_complete=True,
            market_data_note="",
            rsi_14=round(candidate.rsi_14, 1),
            relative_volume=round(candidate.relative_volume, 2),
        )

    def _synthetic_prewatch_market_assessment(self, candidate) -> object:
        return self._synthetic_candidate_pool_market_assessment(candidate)

    def _record_decision_packets(self, packets: list[DecisionPacket], *, run_id: str) -> None:
        for packet in packets:
            packet_pool = "candidate_pool" if packet.pool in {"candidate_pool", "prewatch"} else packet.pool
            price_plan = packet.price_plan.to_record() if packet.price_plan is not None else {}
            invalidation = (
                {
                    "level": packet.price_plan.invalidation_level,
                    "reason": packet.price_plan.invalidation_reason,
                }
                if packet.price_plan is not None
                else {}
            )
            decision_id = hashlib.sha1(
                f"{run_id}:{packet_pool}:{packet.symbol}:{packet.horizon}:{packet.event_id}:{packet.trigger_mode}".encode("utf-8")
            ).hexdigest()
            packet_record = packet.to_record()
            packet_record["pool"] = packet_pool
            if packet_pool == "candidate_pool":
                candidate_pool_state = self._load_recent_candidate_pool_context(packet.symbol) or {}
                if candidate_pool_state:
                    packet_record["candidate_lifecycle"] = {
                        "observation_count": max(int(candidate_pool_state.get("observation_count") or 1), 1),
                        "first_seen_at": str(candidate_pool_state.get("first_seen_at") or ""),
                        "last_seen_at": str(candidate_pool_state.get("last_seen_at") or ""),
                        "alert_sent_count": max(int(candidate_pool_state.get("alert_sent_count") or 0), 0),
                        "last_alert_sent_at": str(candidate_pool_state.get("last_alert_sent_at") or ""),
                    }
                    packet_record["prewatch_lifecycle"] = {
                        "observation_count": max(int(candidate_pool_state.get("observation_count") or 1), 1),
                        "first_seen_at": str(candidate_pool_state.get("first_seen_at") or ""),
                        "last_seen_at": str(candidate_pool_state.get("last_seen_at") or ""),
                        "alert_sent_count": max(int(candidate_pool_state.get("alert_sent_count") or 0), 0),
                        "last_alert_sent_at": str(candidate_pool_state.get("last_alert_sent_at") or ""),
                    }
            elif packet.pool == "confirmation" and packet.trigger_mode == "promoted":
                candidate_pool_state = self._load_recent_candidate_pool_context(packet.symbol) or {}
                if candidate_pool_state:
                    packet_record["promoted_from_prewatch"] = True
                    packet_record["candidate_score"] = round(float(candidate_pool_state.get("score") or 0.0), 2)
                    packet_record["candidate_setup_type"] = str(candidate_pool_state.get("setup_type") or "")
                    packet_record["candidate_observation_count"] = max(int(candidate_pool_state.get("observation_count") or 1), 1)
                    packet_record["candidate_alert_sent_count"] = max(int(candidate_pool_state.get("alert_sent_count") or 0), 0)
                    packet_record["candidate_first_seen_at"] = str(candidate_pool_state.get("first_seen_at") or "")
                    packet_record["candidate_last_seen_at"] = str(candidate_pool_state.get("last_seen_at") or "")
                    packet_record["candidate_last_alert_sent_at"] = str(candidate_pool_state.get("last_alert_sent_at") or "")
                    packet_record["candidate_source_decision_id"] = str(
                        candidate_pool_state.get("latest_candidate_decision_id")
                        or candidate_pool_state.get("latest_prewatch_decision_id")
                        or ""
                    )
                    packet_record["candidate_promotion_reason"] = self._build_candidate_pool_promotion_reason(candidate_pool_state)
                    packet_record["prewatch_score"] = round(float(candidate_pool_state.get("score") or 0.0), 2)
                    packet_record["prewatch_setup_type"] = str(candidate_pool_state.get("setup_type") or "")
                    packet_record["prewatch_observation_count"] = max(int(candidate_pool_state.get("observation_count") or 1), 1)
                    packet_record["prewatch_alert_sent_count"] = max(int(candidate_pool_state.get("alert_sent_count") or 0), 0)
                    packet_record["prewatch_first_seen_at"] = str(candidate_pool_state.get("first_seen_at") or "")
                    packet_record["prewatch_last_seen_at"] = str(candidate_pool_state.get("last_seen_at") or "")
                    packet_record["prewatch_last_alert_sent_at"] = str(candidate_pool_state.get("last_alert_sent_at") or "")
                    packet_record["source_decision_id"] = str(
                        candidate_pool_state.get("latest_candidate_decision_id")
                        or candidate_pool_state.get("latest_prewatch_decision_id")
                        or ""
                    )
                    packet_record["prewatch_promotion_reason"] = self._build_candidate_pool_promotion_reason(candidate_pool_state)
            packet_record["strategy_version"] = self.strategy_version
            self.store.save_decision_record(
                decision_id=decision_id,
                run_id=run_id,
                event_id=packet.event_id,
                symbol=packet.symbol,
                event_type=packet.event_assessment.event_type,
                pool=packet_pool,
                action=packet.action,
                priority=packet.priority,
                confidence=packet.confidence,
                event_score=packet.event_score,
                market_score=packet.market_score,
                theme_score=packet.theme_score,
                final_score=packet.final_score,
                trigger_mode=packet.trigger_mode,
                llm_used=packet.llm_used,
                theme_ids=list(packet.theme_assessment.theme_ids),
                entry_plan=price_plan,
                invalidation=invalidation,
                ttl=(packet.price_plan.ttl_iso if packet.price_plan is not None else ""),
                packet=packet_record,
                created_at=utcnow().isoformat(),
            )
            if packet_pool == "candidate_pool":
                self._record_candidate_pool_decision_reference(
                    symbol=packet.symbol,
                    decision_id=decision_id,
                    event_id=packet.event_id,
                    run_id=run_id,
                )

    def _build_confirmation_candidate_evaluation_row(
        self,
        *,
        symbol: str,
        horizon: str,
        event_id: str,
        outcome: str,
        reason: str,
        score: float | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol.upper(),
            "horizon": horizon,
            "event_id": event_id,
            "outcome": outcome,
            "reason": reason,
            "score": score,
            "payload": dict(payload or {}),
        }

    def _record_confirmation_candidate_evaluations(
        self,
        cards: list,
        *,
        run_id: str,
        extras: list[dict[str, Any]] | None = None,
    ) -> None:
        created_at = utcnow().isoformat()
        for card in cards:
            outcome = "selected"
            reason = "confirmation_opportunity"
            if bool(getattr(card, "downgraded_from_formal", False)):
                outcome = "not_selected"
                reason = "formal_downgraded_to_watch_unentered"
            elif str(getattr(card, "delivery_category", "") or "").strip() == "watch":
                outcome = "not_selected"
                reason = "candidate_watch"
            elif not getattr(card, "execution_eligible", True):
                outcome = "rejected"
                reason = "execution_ineligible"
            elif card.priority == "suppressed":
                outcome = "rejected"
                reason = "threshold_not_met"
            payload = {
                "source": "event_only_degraded" if not getattr(card, "market_data_complete", True) else "confirmation",
                "event_type": str(getattr(card, "event_type", "") or ""),
                "action_label": str(getattr(card, "action_label", "") or ""),
                "priority": str(getattr(card, "priority", "") or ""),
                "event_score": round(float(getattr(card, "event_score", 0.0) or 0.0), 2),
                "market_score": round(float(getattr(card, "market_score", 0.0) or 0.0), 2),
                "final_score": round(float(getattr(card, "final_score", 0.0) or 0.0), 2),
                "execution_eligible": bool(getattr(card, "execution_eligible", True)),
                "market_data_complete": bool(getattr(card, "market_data_complete", True)),
                "promoted_from_prewatch": bool(getattr(card, "promoted_from_prewatch", False)),
                "trigger_mode": (
                    "promoted"
                    if bool(getattr(card, "promoted_from_prewatch", False))
                    else ("event_only" if not getattr(card, "market_data_complete", True) else "direct")
                ),
            }
            self.store.record_candidate_evaluation(
                run_id=run_id,
                stage="confirmation",
                symbol=card.symbol,
                horizon=card.horizon,
                event_id=card.event_id,
                outcome=outcome,
                reason=reason,
                score=card.final_score,
                strategy_version=self.strategy_version,
                payload=payload,
                created_at=created_at,
            )
        for item in extras or []:
            self.store.record_candidate_evaluation(
                run_id=run_id,
                stage="confirmation",
                symbol=str(item.get("symbol") or ""),
                horizon=str(item.get("horizon") or ""),
                event_id=str(item.get("event_id") or ""),
                outcome=str(item.get("outcome") or ""),
                reason=str(item.get("reason") or ""),
                score=item.get("score"),
                strategy_version=self.strategy_version,
                payload=dict(item.get("payload") or {}),
                created_at=created_at,
            )

    def _fetch_from_adapter(
        self,
        adapter: SourceAdapter,
        since: datetime,
        cached_health: SourceHealthCheck | None = None,
    ) -> dict:
        if adapter.skips_pre_fetch_health_check():
            started = time.monotonic()
            try:
                adapter_events = adapter.fetch_since(since)
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "health": SourceHealthCheck(
                        source_name=adapter.name,
                        status="healthy",
                        detail="Source fetch probe succeeded.",
                        latency_ms=latency_ms,
                    ),
                    "health_from_cache": False,
                    "health_derived_from_fetch": True,
                    "events": adapter_events,
                    "partial_errors": getattr(adapter, "last_fetch_errors", []),
                }
            except Exception as exc:
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "health": SourceHealthCheck(
                        source_name=adapter.name,
                        status="unhealthy",
                        detail=f"{exc.__class__.__name__}: {exc}",
                        latency_ms=latency_ms,
                    ),
                    "health_from_cache": False,
                    "health_derived_from_fetch": True,
                    "events": [],
                    "fetch_error": str(exc),
                    "fetch_error_type": exc.__class__.__name__,
                    "partial_errors": getattr(adapter, "last_fetch_errors", []),
                }

        health = cached_health
        health_from_cache = health is not None
        if health is None:
            try:
                health = adapter.health_check()
            except Exception as exc:
                return {
                    "health": None,
                    "health_error": exc.__class__.__name__,
                    "health_detail": str(exc),
                    "events": [],
                    "partial_errors": [],
                }
        if health.status != "healthy" and not adapter.should_fetch_after_unhealthy_healthcheck():
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": [],
                "partial_errors": [],
            }
        try:
            adapter_events = adapter.fetch_since(since)
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": adapter_events,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }
        except Exception as exc:
            return {
                "health": health,
                "health_from_cache": health_from_cache,
                "health_derived_from_fetch": False,
                "events": [],
                "fetch_error": str(exc),
                "fetch_error_type": exc.__class__.__name__,
                "partial_errors": getattr(adapter, "last_fetch_errors", []),
            }

    def _source_health_state_key(self, source_name: str) -> str:
        return f"source_health:{source_name}"

    def _load_cached_source_health(self, source_name: str) -> SourceHealthCheck | None:
        ttl_seconds = max(self.settings.source_health_cache_seconds, 0)
        if ttl_seconds <= 0:
            return None
        raw = self.store.get_state(self._source_health_state_key(source_name))
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            checked_at = datetime.fromisoformat(payload["checked_at"])
        except (KeyError, ValueError, TypeError, json.JSONDecodeError):
            return None
        age_seconds = (utcnow() - checked_at).total_seconds()
        if age_seconds > ttl_seconds:
            return None
        return SourceHealthCheck(
            source_name=payload.get("source_name", source_name),
            status=payload.get("status", "healthy"),
            detail=f'{payload.get("detail", "")} (cached)',
            checked_at=utcnow(),
            latency_ms=0,
        )

    def _save_cached_source_health(self, health: SourceHealthCheck) -> None:
        payload = {
            "source_name": health.source_name,
            "status": health.status,
            "detail": health.detail,
            "checked_at": health.checked_at.isoformat(),
        }
        self.store.set_state(self._source_health_state_key(health.source_name), json.dumps(payload))
