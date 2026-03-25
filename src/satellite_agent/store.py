from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from .models import AlertDecision, Bar, EventInsight, IndicatorSnapshot, OpportunityCard, SourceEvent, SourceHealthCheck


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


class Store:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path)
        self.connection.row_factory = sqlite3.Row

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS watchlist (
            symbol TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL,
            event_fingerprint TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL,
            source_type TEXT NOT NULL,
            headline TEXT NOT NULL,
            summary TEXT NOT NULL,
            published_at TEXT NOT NULL,
            url TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS event_insights (
            event_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL,
            headline_summary TEXT NOT NULL,
            bull_case TEXT NOT NULL,
            bear_case TEXT NOT NULL,
            importance REAL NOT NULL,
            source_credibility REAL NOT NULL,
            novelty REAL NOT NULL,
            sentiment REAL NOT NULL,
            theme_relevance REAL NOT NULL,
            llm_confidence REAL NOT NULL,
            risk_notes_json TEXT NOT NULL,
            source_refs_json TEXT NOT NULL,
            raw_payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS price_bars_5m (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            adjusted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (symbol, timestamp)
        );
        CREATE TABLE IF NOT EXISTS price_bars_1d (
            symbol TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            adjusted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (symbol, timestamp)
        );
        CREATE TABLE IF NOT EXISTS indicator_snapshots (
            symbol TEXT NOT NULL,
            horizon TEXT NOT NULL,
            as_of TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            PRIMARY KEY (symbol, horizon, as_of)
        );
        CREATE TABLE IF NOT EXISTS opportunity_cards (
            card_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL DEFAULT '',
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            horizon TEXT NOT NULL,
            final_score REAL NOT NULL,
            priority TEXT NOT NULL,
            invalidation_level REAL NOT NULL,
            card_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS alert_history (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            dedup_key TEXT NOT NULL,
            card_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_id TEXT NOT NULL,
            horizon TEXT NOT NULL,
            priority TEXT NOT NULL,
            final_score REAL NOT NULL,
            invalidation_level REAL NOT NULL,
            sent INTEGER NOT NULL,
            reason TEXT NOT NULL,
            notified_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_alert_history_dedup ON alert_history (dedup_key, notified_at DESC);
        CREATE TABLE IF NOT EXISTS agent_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            status TEXT NOT NULL,
            run_name TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            config_snapshot_json TEXT NOT NULL DEFAULT '{}',
            summary_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS system_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            stage TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_id TEXT NOT NULL,
            message TEXT NOT NULL,
            context_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_system_logs_run_id ON system_logs (run_id, created_at ASC);
        CREATE TABLE IF NOT EXISTS source_health_checks (
            check_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT NOT NULL,
            latency_ms INTEGER,
            checked_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_source_health_checks_run_id ON source_health_checks (run_id, checked_at ASC);
        CREATE TABLE IF NOT EXISTS llm_usage (
            usage_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            component TEXT NOT NULL DEFAULT 'event_extraction',
            model TEXT NOT NULL,
            used_llm INTEGER NOT NULL,
            success INTEGER NOT NULL,
            prompt_tokens_estimate INTEGER NOT NULL DEFAULT 0,
            completion_tokens_estimate INTEGER NOT NULL DEFAULT 0,
            latency_ms INTEGER NOT NULL DEFAULT 0,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_llm_usage_created_at ON llm_usage (created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_llm_usage_run_id ON llm_usage (run_id, created_at ASC);
        CREATE TABLE IF NOT EXISTS decision_records (
            decision_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            event_type TEXT NOT NULL DEFAULT '',
            pool TEXT NOT NULL,
            action TEXT NOT NULL,
            priority TEXT NOT NULL,
            confidence TEXT NOT NULL,
            event_score REAL NOT NULL,
            market_score REAL NOT NULL,
            theme_score REAL NOT NULL,
            final_score REAL NOT NULL,
            trigger_mode TEXT NOT NULL,
            llm_used INTEGER NOT NULL,
            theme_ids_json TEXT NOT NULL,
            entry_plan_json TEXT NOT NULL,
            invalidation_json TEXT NOT NULL,
            ttl TEXT NOT NULL,
            packet_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_decision_records_run_id ON decision_records (run_id, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_decision_records_symbol ON decision_records (symbol, created_at DESC);
        CREATE TABLE IF NOT EXISTS decision_outcomes (
            decision_id TEXT PRIMARY KEY,
            entered INTEGER NOT NULL DEFAULT 0,
            entered_at TEXT NOT NULL DEFAULT '',
            entry_price REAL,
            exit_price REAL,
            realized_return REAL,
            holding_days INTEGER,
            gross_realized_return REAL,
            net_realized_return REAL,
            slippage_bps REAL NOT NULL DEFAULT 0,
            t_plus_1_return REAL,
            t_plus_3_return REAL,
            t_plus_5_return REAL,
            t_plus_7_return REAL,
            t_plus_10_return REAL,
            t_plus_14_return REAL,
            t_plus_30_return REAL,
            max_runup REAL,
            max_drawdown REAL,
            hit_take_profit INTEGER NOT NULL DEFAULT 0,
            hit_invalidation INTEGER NOT NULL DEFAULT 0,
            close_reason TEXT NOT NULL DEFAULT '',
            exit_subreason TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS candidate_evaluations (
            evaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            symbol TEXT NOT NULL,
            horizon TEXT NOT NULL,
            event_id TEXT NOT NULL DEFAULT '',
            outcome TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            score REAL,
            strategy_version TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_run_stage
        ON candidate_evaluations (run_id, stage, created_at ASC);
        CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_symbol
        ON candidate_evaluations (symbol, created_at DESC);
        CREATE TABLE IF NOT EXISTS agent_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL
        );
        """
        self.connection.executescript(schema)
        self._ensure_columns()
        self._ensure_indexes()
        self.connection.commit()

    def seed_watchlist(self, symbols: Iterable[str], asset_type: str) -> None:
        rows = [(symbol, asset_type) for symbol in symbols]
        self.connection.executemany(
            "INSERT OR REPLACE INTO watchlist (symbol, asset_type, enabled) VALUES (?, ?, 1)", rows
        )
        self.connection.commit()

    def replace_watchlist(self, stocks: Iterable[str], etfs: Iterable[str]) -> None:
        self.connection.execute("UPDATE watchlist SET enabled = 0")
        rows = [(symbol, "stock") for symbol in stocks] + [(symbol, "etf") for symbol in etfs]
        self.connection.executemany(
            "INSERT OR REPLACE INTO watchlist (symbol, asset_type, enabled) VALUES (?, ?, 1)", rows
        )
        self.connection.commit()

    def load_watchlist(self, asset_type: str | None = None) -> set[str]:
        query = "SELECT symbol FROM watchlist WHERE enabled = 1"
        params: tuple[Any, ...] = ()
        if asset_type is not None:
            query += " AND asset_type = ?"
            params = (asset_type,)
        cursor = self.connection.execute(query, params)
        return {row["symbol"] for row in cursor.fetchall()}

    def save_raw_event(self, event: SourceEvent, run_id: str = "") -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO raw_events
            (event_id, run_id, symbol, event_fingerprint, source, source_type, headline, summary, published_at, url, metadata_json, raw_payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                run_id,
                event.symbol,
                event.event_fingerprint,
                event.source,
                event.source_type,
                event.headline,
                event.summary,
                event.published_at.isoformat(),
                event.url,
                json.dumps(event.metadata, sort_keys=True),
                json.dumps(event.raw_payload, sort_keys=True),
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def has_recent_fingerprint(self, symbol: str, event_fingerprint: str, published_after: str) -> bool:
        cursor = self.connection.execute(
            """
            SELECT 1
            FROM raw_events
            WHERE symbol = ? AND event_fingerprint = ? AND published_at >= ?
            LIMIT 1
            """,
            (symbol, event_fingerprint, published_after),
        )
        return cursor.fetchone() is not None

    def save_event_insight(self, insight: EventInsight, run_id: str = "") -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO event_insights
            (event_id, run_id, symbol, event_type, headline_summary, bull_case, bear_case, importance, source_credibility,
             novelty, sentiment, theme_relevance, llm_confidence, risk_notes_json, source_refs_json, raw_payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                insight.event_id,
                run_id,
                insight.symbol,
                insight.event_type,
                insight.headline_summary,
                insight.bull_case,
                insight.bear_case,
                insight.importance,
                insight.source_credibility,
                insight.novelty,
                insight.sentiment,
                insight.theme_relevance,
                insight.llm_confidence,
                json.dumps(insight.risk_notes, sort_keys=True),
                json.dumps(insight.source_refs, sort_keys=True),
                json.dumps(insight.raw_payload, sort_keys=True),
                insight.created_at.isoformat(),
            ),
        )
        self.connection.commit()

    def upsert_price_bars(self, symbol: str, timeframe: str, bars: Iterable[Bar]) -> None:
        table = self._bar_table(timeframe)
        self.connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table}
            (symbol, timestamp, open, high, low, close, volume, adjusted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    symbol,
                    bar.timestamp.isoformat(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    1 if getattr(bar, "adjusted", False) else 0,
                )
                for bar in bars
            ],
        )
        self.connection.commit()

    def replace_price_bars(self, symbol: str, timeframe: str, bars: Iterable[Bar]) -> None:
        table = self._bar_table(timeframe)
        bar_rows = list(bars)
        self.connection.execute(f"DELETE FROM {table} WHERE symbol = ?", (symbol,))
        self.connection.executemany(
            f"""
            INSERT OR REPLACE INTO {table}
            (symbol, timestamp, open, high, low, close, volume, adjusted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    symbol,
                    bar.timestamp.isoformat(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    1 if getattr(bar, "adjusted", False) else 0,
                )
                for bar in bar_rows
            ],
        )
        self.connection.commit()

    def load_price_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        table = self._bar_table(timeframe)
        cursor = self.connection.execute(
            f"""
            SELECT timestamp, open, high, low, close, volume, adjusted
            FROM {table}
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (symbol, limit),
        )
        rows = list(cursor.fetchall())
        rows.reverse()
        return [
            Bar(
                timestamp=_dt(row["timestamp"]),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
                adjusted=bool(row["adjusted"]) if "adjusted" in row.keys() else False,
            )
            for row in rows
        ]

    def summarize_price_bar_adjustment(self, symbols: Iterable[str], timeframe: str) -> list[sqlite3.Row]:
        normalized_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            return []
        table = self._bar_table(timeframe)
        placeholders = ", ".join("?" for _ in normalized_symbols)
        cursor = self.connection.execute(
            f"""
            SELECT
                symbol,
                COUNT(*) AS total_bars,
                SUM(CASE WHEN adjusted = 1 THEN 1 ELSE 0 END) AS adjusted_bars,
                SUM(CASE WHEN adjusted = 0 THEN 1 ELSE 0 END) AS unadjusted_bars
            FROM {table}
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
            ORDER BY symbol ASC
            """,
            tuple(normalized_symbols),
        )
        return cursor.fetchall()

    def save_indicator_snapshot(self, snapshot: IndicatorSnapshot) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO indicator_snapshots (symbol, horizon, as_of, snapshot_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.symbol,
                snapshot.horizon,
                snapshot.as_of.isoformat(),
                json.dumps(snapshot.to_record(), sort_keys=True),
            ),
        )
        self.connection.commit()

    def save_opportunity_card(self, card: OpportunityCard, run_id: str = "") -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO opportunity_cards
            (card_id, run_id, event_id, symbol, horizon, final_score, priority, invalidation_level, card_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card.card_id,
                run_id,
                card.event_id,
                card.symbol,
                card.horizon,
                card.final_score,
                card.priority,
                card.invalidation_level,
                json.dumps(card.to_record(), sort_keys=True),
                card.created_at.isoformat(),
            ),
        )
        self.connection.commit()

    def get_last_alert(self, dedup_key: str) -> Optional[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT * FROM alert_history
            WHERE dedup_key = ?
            ORDER BY notified_at DESC
            LIMIT 1
            """,
            (dedup_key,),
        )
        return cursor.fetchone()

    def record_alert(self, card: OpportunityCard, decision: AlertDecision, run_id: str = "") -> None:
        self.connection.execute(
            """
            INSERT INTO alert_history
            (run_id, dedup_key, card_id, symbol, event_id, horizon, priority, final_score, invalidation_level, sent, reason, notified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                decision.dedup_key,
                card.card_id,
                card.symbol,
                card.event_id,
                card.horizon,
                decision.priority,
                card.final_score,
                card.invalidation_level,
                1 if decision.sent else 0,
                decision.reason,
                decision.notified_at.isoformat(),
            ),
        )
        self.connection.commit()

    def record_log(
        self,
        *,
        run_id: str,
        level: str,
        event_type: str,
        stage: str,
        message: str,
        symbol: str = "",
        event_id: str = "",
        context: Optional[dict[str, Any]] = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO system_logs
            (run_id, level, event_type, stage, symbol, event_id, message, context_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                level,
                event_type,
                stage,
                symbol,
                event_id,
                message,
                json.dumps(context or {}, sort_keys=True),
                datetime.now().astimezone().isoformat(),
            ),
        )
        self.connection.commit()

    def record_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        status: str,
        run_name: str = "",
        note: str = "",
        config_snapshot: Optional[dict[str, Any]] = None,
        summary: dict[str, Any],
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO agent_runs
            (run_id, started_at, finished_at, status, run_name, note, config_snapshot_json, summary_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                started_at.isoformat(),
                finished_at.isoformat(),
                status,
                run_name,
                note,
                json.dumps(config_snapshot or {}, sort_keys=True),
                json.dumps(summary, sort_keys=True),
            ),
        )
        self.connection.commit()

    def update_run_metadata(
        self,
        run_id: str,
        *,
        run_name: Optional[str] = None,
        note: Optional[str] = None,
    ) -> bool:
        row = self.load_run(run_id)
        if row is None:
            return False
        updates = []
        params: list[Any] = []
        if run_name is not None:
            updates.append("run_name = ?")
            params.append(run_name)
        if note is not None:
            updates.append("note = ?")
            params.append(note)
        if not updates:
            return True
        params.append(run_id)
        self.connection.execute(
            f"UPDATE agent_runs SET {', '.join(updates)} WHERE run_id = ?",
            tuple(params),
        )
        self.connection.commit()
        return True

    def record_source_health(self, run_id: str, health: SourceHealthCheck) -> None:
        self.connection.execute(
            """
            INSERT INTO source_health_checks
            (run_id, source_name, status, detail, latency_ms, checked_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                health.source_name,
                health.status,
                health.detail,
                health.latency_ms,
                health.checked_at.isoformat(),
            ),
        )
        self.connection.commit()

    def load_run(self, run_id: str) -> Optional[sqlite3.Row]:
        cursor = self.connection.execute(
            "SELECT * FROM agent_runs WHERE run_id = ?",
            (run_id,),
        )
        return cursor.fetchone()

    def load_latest_run(self) -> Optional[sqlite3.Row]:
        cursor = self.connection.execute(
            "SELECT * FROM agent_runs ORDER BY finished_at DESC LIMIT 1"
        )
        return cursor.fetchone()

    def load_recent_runs(self, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT * FROM agent_runs
            ORDER BY finished_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cursor.fetchall()

    def load_logs(self, run_id: str, limit: Optional[int] = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM system_logs WHERE run_id = ? ORDER BY created_at ASC"
        params: tuple[Any, ...] = (run_id,)
        if limit is not None:
            query += " LIMIT ?"
            params = (run_id, limit)
        cursor = self.connection.execute(query, params)
        return cursor.fetchall()

    def load_opportunity_cards(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM opportunity_cards
            WHERE run_id = ?
            ORDER BY created_at ASC, horizon ASC
            """,
            (run_id,),
        )
        return cursor.fetchall()

    def load_alert_history(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT *
            FROM alert_history
            WHERE run_id = ?
            ORDER BY notified_at ASC, alert_id ASC
            """,
            (run_id,),
        )
        return cursor.fetchall()

    def load_alert_history_for_window(self, *, since: str, symbol: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                ah.*,
                oc.card_json
            FROM alert_history ah
            LEFT JOIN opportunity_cards oc ON ah.card_id = oc.card_id
            WHERE ah.notified_at >= ?
        """
        params: list[Any] = [since]
        if symbol:
            query += " AND ah.symbol = ?"
            params.append(symbol.upper())
        query += """
            ORDER BY ah.notified_at ASC, ah.alert_id ASC
        """
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def load_source_health(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT * FROM source_health_checks
            WHERE run_id = ?
            ORDER BY checked_at ASC
            """,
            (run_id,),
        )
        return cursor.fetchall()

    def load_decision_records(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                dr.decision_id,
                dr.run_id,
                dr.event_id,
                dr.symbol,
                COALESCE(NULLIF(dr.event_type, ''), ei.event_type, '') AS event_type,
                dr.pool,
                dr.action,
                dr.priority,
                dr.confidence,
                dr.event_score,
                dr.market_score,
                dr.theme_score,
                dr.final_score,
                dr.trigger_mode,
                dr.llm_used,
                dr.theme_ids_json,
                dr.entry_plan_json,
                dr.invalidation_json,
                dr.ttl,
                dr.packet_json,
                dr.created_at,
                do.t_plus_1_return,
                do.t_plus_3_return,
                do.t_plus_5_return,
                do.t_plus_7_return,
                do.t_plus_10_return,
                do.t_plus_14_return,
                do.t_plus_30_return,
                do.max_runup,
                do.max_drawdown,
                do.entered,
                do.entered_at,
                do.entry_price,
                do.exit_price,
                do.realized_return,
                do.holding_days,
                do.gross_realized_return,
                do.net_realized_return,
                do.slippage_bps,
                do.hit_take_profit,
                do.hit_invalidation,
                do.close_reason,
                do.exit_subreason,
                do.updated_at AS outcome_updated_at
            FROM decision_records dr
            LEFT JOIN event_insights ei ON dr.event_id = ei.event_id AND dr.run_id = ei.run_id
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.run_id = ?
            ORDER BY dr.created_at ASC, dr.symbol ASC, dr.pool ASC
            """,
            (run_id,),
        )
        return cursor.fetchall()

    def load_decision_records_without_outcomes(
        self,
        *,
        run_id: str = "",
        since: str = "",
        limit: int = 0,
        recompute_existing: bool = False,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT dr.*
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE 1 = 1
        """
        params: list[Any] = []
        if not recompute_existing:
            query += """
               AND (
                    do.decision_id IS NULL
                    OR do.close_reason = ''
                    OR do.t_plus_1_return IS NULL
                    OR do.t_plus_3_return IS NULL
                    OR do.t_plus_5_return IS NULL
                    OR do.t_plus_7_return IS NULL
                    OR do.t_plus_10_return IS NULL
                    OR do.t_plus_14_return IS NULL
                    OR do.t_plus_30_return IS NULL
                    OR do.max_runup IS NULL
                    OR do.max_drawdown IS NULL
                    OR do.entered IS NULL
                    OR (do.entered = 1 AND do.entry_price IS NULL)
                    OR (
                        do.close_reason IN ('exit_pool', 'hit_invalidation', 'window_complete')
                        AND (
                            do.exit_price IS NULL
                            OR do.realized_return IS NULL
                            OR do.holding_days IS NULL
                        )
                    )
               )
            """
        if since:
            query += " AND dr.created_at >= ?"
            params.append(since)
        if run_id:
            query += " AND dr.run_id = ?"
            params.append(run_id)
        query += " ORDER BY dr.created_at ASC"
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def load_latest_decision_records(self, limit: int = 50) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                dr.decision_id,
                dr.run_id,
                dr.event_id,
                dr.symbol,
                COALESCE(NULLIF(dr.event_type, ''), ei.event_type, '') AS event_type,
                dr.pool,
                dr.action,
                dr.priority,
                dr.confidence,
                dr.event_score,
                dr.market_score,
                dr.theme_score,
                dr.final_score,
                dr.trigger_mode,
                dr.llm_used,
                dr.theme_ids_json,
                dr.entry_plan_json,
                dr.invalidation_json,
                dr.ttl,
                dr.packet_json,
                dr.created_at,
                do.t_plus_1_return,
                do.t_plus_3_return,
                do.t_plus_5_return,
                do.t_plus_7_return,
                do.t_plus_10_return,
                do.t_plus_14_return,
                do.t_plus_30_return,
                do.max_runup,
                do.max_drawdown,
                do.entered,
                do.entered_at,
                do.entry_price,
                do.exit_price,
                do.realized_return,
                do.holding_days,
                do.gross_realized_return,
                do.net_realized_return,
                do.slippage_bps,
                do.hit_take_profit,
                do.hit_invalidation,
                do.close_reason,
                do.exit_subreason,
                do.updated_at AS outcome_updated_at
            FROM decision_records dr
            LEFT JOIN event_insights ei ON dr.event_id = ei.event_id AND dr.run_id = ei.run_id
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            ORDER BY dr.created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cursor.fetchall()

    def load_decision_records_for_window(
        self,
        *,
        since: str,
        until: str = "",
        actions: Iterable[str] | None = None,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT
                dr.decision_id,
                dr.run_id,
                dr.event_id,
                dr.symbol,
                COALESCE(NULLIF(dr.event_type, ''), ei.event_type, '') AS event_type,
                dr.pool,
                dr.action,
                dr.priority,
                dr.confidence,
                dr.event_score,
                dr.market_score,
                dr.theme_score,
                dr.final_score,
                dr.trigger_mode,
                dr.llm_used,
                dr.theme_ids_json,
                dr.entry_plan_json,
                dr.invalidation_json,
                dr.ttl,
                dr.packet_json,
                dr.created_at,
                do.entered,
                do.entered_at,
                do.entry_price,
                do.exit_price,
                do.realized_return,
                do.holding_days,
                do.gross_realized_return,
                do.net_realized_return,
                do.slippage_bps,
                do.t_plus_1_return,
                do.t_plus_3_return,
                do.t_plus_5_return,
                do.t_plus_7_return,
                do.t_plus_10_return,
                do.t_plus_14_return,
                do.t_plus_30_return,
                do.max_runup,
                do.max_drawdown,
                do.hit_take_profit,
                do.hit_invalidation,
                do.close_reason,
                do.exit_subreason,
                do.updated_at AS outcome_updated_at
            FROM decision_records dr
            LEFT JOIN event_insights ei ON dr.event_id = ei.event_id AND dr.run_id = ei.run_id
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND dr.created_at < ?"
            params.append(until)
        normalized_actions = [str(item).strip() for item in (actions or []) if str(item).strip()]
        if normalized_actions:
            placeholders = ", ".join("?" for _ in normalized_actions)
            query += f" AND dr.action IN ({placeholders})"
            params.extend(normalized_actions)
        query += " ORDER BY dr.created_at ASC, dr.symbol ASC, dr.pool ASC"
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def load_latest_source_health(self) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT sh.*
            FROM source_health_checks sh
            JOIN (
                SELECT source_name, MAX(checked_at) AS max_checked_at
                FROM source_health_checks
                GROUP BY source_name
            ) latest
              ON sh.source_name = latest.source_name
             AND sh.checked_at = latest.max_checked_at
            ORDER BY sh.source_name ASC
            """
        )
        return cursor.fetchall()

    def aggregate_errors(self, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                COUNT(*) AS count,
                event_type,
                stage,
                symbol,
                message
            FROM system_logs
            WHERE level = 'ERROR'
            GROUP BY event_type, stage, symbol, message
            ORDER BY count DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cursor.fetchall()

    def aggregate_event_type_performance(self, since: str, limit: int = 10, until: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                ei.event_type AS event_type,
                COUNT(*) AS card_count,
                ROUND(AVG(oc.final_score), 2) AS avg_final_score,
                SUM(CASE WHEN oc.priority = 'high' THEN 1 ELSE 0 END) AS high_priority_count
            FROM opportunity_cards oc
            JOIN event_insights ei ON oc.event_id = ei.event_id
            WHERE oc.created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND oc.created_at < ?"
            params.append(until)
        query += """
            GROUP BY ei.event_type
            ORDER BY avg_final_score DESC, card_count DESC
            LIMIT ?
        """
        params.append(limit)
        cursor = self.connection.execute(
            query,
            tuple(params),
        )
        return cursor.fetchall()

    def aggregate_event_type_performance_for_run(self, run_id: str, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                ei.event_type AS event_type,
                COUNT(*) AS card_count,
                ROUND(AVG(oc.final_score), 2) AS avg_final_score,
                SUM(CASE WHEN oc.priority = 'high' THEN 1 ELSE 0 END) AS high_priority_count
            FROM opportunity_cards oc
            JOIN event_insights ei ON oc.event_id = ei.event_id AND oc.run_id = ei.run_id
            WHERE oc.run_id = ?
            GROUP BY ei.event_type
            ORDER BY avg_final_score DESC, card_count DESC
            LIMIT ?
            """,
            (run_id, limit),
        )
        return cursor.fetchall()

    def aggregate_source_stability(self, since: str, limit: int = 10, until: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                source_name,
                COUNT(*) AS check_count,
                SUM(CASE WHEN status = 'healthy' THEN 1 ELSE 0 END) AS healthy_count,
                SUM(CASE WHEN status != 'healthy' THEN 1 ELSE 0 END) AS unhealthy_count,
                ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
                MAX(checked_at) AS last_checked_at
            FROM source_health_checks
            WHERE checked_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND checked_at < ?"
            params.append(until)
        query += """
            GROUP BY source_name
            ORDER BY healthy_count DESC, check_count DESC, source_name ASC
            LIMIT ?
        """
        params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def aggregate_alert_volume(self, since: str, limit: int = 14, until: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                substr(notified_at, 1, 10) AS bucket_date,
                COUNT(*) AS total_alerts,
                SUM(CASE WHEN sent = 1 THEN 1 ELSE 0 END) AS sent_alerts,
                SUM(CASE WHEN sent = 0 THEN 1 ELSE 0 END) AS suppressed_alerts,
                SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) AS high_priority_alerts,
                SUM(CASE WHEN priority = 'high' AND sent = 1 THEN 1 ELSE 0 END) AS sent_high_priority_alerts
            FROM alert_history
            WHERE notified_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND notified_at < ?"
            params.append(until)
        query += """
            GROUP BY bucket_date
            ORDER BY bucket_date DESC
            LIMIT ?
        """
        params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def aggregate_alert_volume_for_run(self, run_id: str) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                run_id,
                COUNT(*) AS total_alerts,
                SUM(CASE WHEN sent = 1 THEN 1 ELSE 0 END) AS sent_alerts,
                SUM(CASE WHEN sent = 0 THEN 1 ELSE 0 END) AS suppressed_alerts,
                SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) AS high_priority_alerts,
                SUM(CASE WHEN priority = 'high' AND sent = 1 THEN 1 ELSE 0 END) AS sent_high_priority_alerts
            FROM alert_history
            WHERE run_id = ?
            GROUP BY run_id
            """,
            (run_id,),
        )
        return cursor.fetchall()

    def aggregate_decision_outcomes_by_event_type(self, since: str, limit: int = 10, until: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                COALESCE(NULLIF(dr.event_type, ''), 'uncategorized') AS event_type,
                COUNT(dr.decision_id) AS decision_count,
                SUM(CASE WHEN do.decision_id IS NOT NULL THEN 1 ELSE 0 END) AS outcome_count,
                SUM(CASE WHEN do.close_reason = 'insufficient_lookahead' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN do.entered = 1 THEN 1 ELSE 0 END) AS entered_count,
                SUM(CASE WHEN do.close_reason = 'not_entered' THEN 1 ELSE 0 END) AS not_entered_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS exit_pool_hits,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') OR do.hit_take_profit = 1 THEN 1 ELSE 0 END) AS take_profit_hits,
                SUM(CASE WHEN do.exit_subreason = 'target_hit' THEN 1 ELSE 0 END) AS target_hit_count,
                SUM(CASE WHEN do.exit_subreason = 'weakening_after_tp_zone' THEN 1 ELSE 0 END) AS weakening_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'macro_protection' THEN 1 ELSE 0 END) AS macro_protection_count,
                SUM(CASE WHEN do.hit_invalidation = 1 THEN 1 ELSE 0 END) AS invalidation_hits,
                SUM(CASE WHEN do.close_reason = 'window_complete' THEN 1 ELSE 0 END) AS window_complete_count,
                SUM(CASE WHEN do.t_plus_3_return > 0 THEN 1 ELSE 0 END) AS positive_t3_count,
                SUM(CASE WHEN do.t_plus_3_return IS NOT NULL THEN 1 ELSE 0 END) AS t_plus_3_sample_count,
                SUM(CASE WHEN do.max_runup IS NOT NULL THEN 1 ELSE 0 END) AS max_runup_sample_count,
                SUM(CASE WHEN do.max_drawdown IS NOT NULL THEN 1 ELSE 0 END) AS max_drawdown_sample_count,
                ROUND(AVG(do.realized_return), 2) AS avg_realized_return,
                ROUND(AVG(do.t_plus_3_return), 2) AS avg_t_plus_3_return,
                ROUND(AVG(do.max_runup), 2) AS avg_max_runup,
                ROUND(AVG(do.max_drawdown), 2) AS avg_max_drawdown
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND dr.created_at < ?"
            params.append(until)
        query += """
            GROUP BY COALESCE(NULLIF(dr.event_type, ''), 'uncategorized')
            ORDER BY outcome_count DESC, avg_t_plus_3_return DESC, decision_count DESC
            LIMIT ?
        """
        params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def aggregate_decision_outcomes_by_event_type_for_run(self, run_id: str, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                COALESCE(NULLIF(dr.event_type, ''), 'uncategorized') AS event_type,
                COUNT(dr.decision_id) AS decision_count,
                SUM(CASE WHEN do.decision_id IS NOT NULL THEN 1 ELSE 0 END) AS outcome_count,
                SUM(CASE WHEN do.close_reason = 'insufficient_lookahead' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS exit_pool_hits,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') OR do.hit_take_profit = 1 THEN 1 ELSE 0 END) AS take_profit_hits,
                SUM(CASE WHEN do.exit_subreason = 'target_hit' THEN 1 ELSE 0 END) AS target_hit_count,
                SUM(CASE WHEN do.exit_subreason = 'weakening_after_tp_zone' THEN 1 ELSE 0 END) AS weakening_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'macro_protection' THEN 1 ELSE 0 END) AS macro_protection_count,
                SUM(CASE WHEN do.hit_invalidation = 1 THEN 1 ELSE 0 END) AS invalidation_hits,
                SUM(CASE WHEN do.t_plus_3_return > 0 THEN 1 ELSE 0 END) AS positive_t3_count,
                SUM(CASE WHEN do.t_plus_3_return IS NOT NULL THEN 1 ELSE 0 END) AS t_plus_3_sample_count,
                SUM(CASE WHEN do.max_runup IS NOT NULL THEN 1 ELSE 0 END) AS max_runup_sample_count,
                SUM(CASE WHEN do.max_drawdown IS NOT NULL THEN 1 ELSE 0 END) AS max_drawdown_sample_count,
                ROUND(AVG(do.t_plus_3_return), 2) AS avg_t_plus_3_return,
                ROUND(AVG(do.max_runup), 2) AS avg_max_runup,
                ROUND(AVG(do.max_drawdown), 2) AS avg_max_drawdown
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.run_id = ?
            GROUP BY COALESCE(NULLIF(dr.event_type, ''), 'uncategorized')
            ORDER BY outcome_count DESC, avg_t_plus_3_return DESC, decision_count DESC
            LIMIT ?
            """,
            (run_id, limit),
        )
        return cursor.fetchall()

    def aggregate_decision_outcomes_by_pool(self, since: str, limit: int = 10, until: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT
                dr.pool AS pool,
                COUNT(dr.decision_id) AS decision_count,
                SUM(CASE WHEN do.decision_id IS NOT NULL THEN 1 ELSE 0 END) AS outcome_count,
                SUM(CASE WHEN do.close_reason = 'insufficient_lookahead' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN do.entered = 1 THEN 1 ELSE 0 END) AS entered_count,
                SUM(CASE WHEN do.close_reason = 'not_entered' THEN 1 ELSE 0 END) AS not_entered_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS exit_pool_hits,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') OR do.hit_take_profit = 1 THEN 1 ELSE 0 END) AS take_profit_hits,
                SUM(CASE WHEN do.exit_subreason = 'target_hit' THEN 1 ELSE 0 END) AS target_hit_count,
                SUM(CASE WHEN do.exit_subreason = 'weakening_after_tp_zone' THEN 1 ELSE 0 END) AS weakening_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'macro_protection' THEN 1 ELSE 0 END) AS macro_protection_count,
                SUM(CASE WHEN do.hit_invalidation = 1 THEN 1 ELSE 0 END) AS invalidation_hits,
                SUM(CASE WHEN do.close_reason = 'window_complete' THEN 1 ELSE 0 END) AS window_complete_count,
                SUM(CASE WHEN do.t_plus_3_return > 0 THEN 1 ELSE 0 END) AS positive_t3_count,
                SUM(CASE WHEN do.t_plus_3_return IS NOT NULL THEN 1 ELSE 0 END) AS t_plus_3_sample_count,
                SUM(CASE WHEN do.max_runup IS NOT NULL THEN 1 ELSE 0 END) AS max_runup_sample_count,
                SUM(CASE WHEN do.max_drawdown IS NOT NULL THEN 1 ELSE 0 END) AS max_drawdown_sample_count,
                ROUND(AVG(do.realized_return), 2) AS avg_realized_return,
                ROUND(AVG(do.t_plus_3_return), 2) AS avg_t_plus_3_return,
                ROUND(AVG(do.max_runup), 2) AS avg_max_runup,
                ROUND(AVG(do.max_drawdown), 2) AS avg_max_drawdown
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND dr.created_at < ?"
            params.append(until)
        query += """
            GROUP BY dr.pool
            ORDER BY outcome_count DESC, avg_t_plus_3_return DESC, decision_count DESC
            LIMIT ?
        """
        params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def aggregate_decision_outcomes_by_pool_for_run(self, run_id: str, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                dr.pool AS pool,
                COUNT(dr.decision_id) AS decision_count,
                SUM(CASE WHEN do.decision_id IS NOT NULL THEN 1 ELSE 0 END) AS outcome_count,
                SUM(CASE WHEN do.close_reason = 'insufficient_lookahead' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS exit_pool_hits,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') OR do.hit_take_profit = 1 THEN 1 ELSE 0 END) AS take_profit_hits,
                SUM(CASE WHEN do.exit_subreason = 'target_hit' THEN 1 ELSE 0 END) AS target_hit_count,
                SUM(CASE WHEN do.exit_subreason = 'weakening_after_tp_zone' THEN 1 ELSE 0 END) AS weakening_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'macro_protection' THEN 1 ELSE 0 END) AS macro_protection_count,
                SUM(CASE WHEN do.hit_invalidation = 1 THEN 1 ELSE 0 END) AS invalidation_hits,
                SUM(CASE WHEN do.t_plus_3_return > 0 THEN 1 ELSE 0 END) AS positive_t3_count,
                SUM(CASE WHEN do.t_plus_3_return IS NOT NULL THEN 1 ELSE 0 END) AS t_plus_3_sample_count,
                SUM(CASE WHEN do.max_runup IS NOT NULL THEN 1 ELSE 0 END) AS max_runup_sample_count,
                SUM(CASE WHEN do.max_drawdown IS NOT NULL THEN 1 ELSE 0 END) AS max_drawdown_sample_count,
                ROUND(AVG(do.t_plus_3_return), 2) AS avg_t_plus_3_return,
                ROUND(AVG(do.max_runup), 2) AS avg_max_runup,
                ROUND(AVG(do.max_drawdown), 2) AS avg_max_drawdown
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.run_id = ?
            GROUP BY dr.pool
            ORDER BY outcome_count DESC, avg_t_plus_3_return DESC, decision_count DESC
            LIMIT ?
            """,
            (run_id, limit),
        )
        return cursor.fetchall()

    def summarize_decision_outcomes(self, since: str, until: str = "") -> sqlite3.Row:
        query = """
            SELECT
                COUNT(dr.decision_id) AS decision_count,
                SUM(CASE WHEN do.decision_id IS NOT NULL THEN 1 ELSE 0 END) AS outcome_count,
                SUM(CASE WHEN do.close_reason = 'insufficient_lookahead' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN do.entered = 1 THEN 1 ELSE 0 END) AS entered_count,
                SUM(CASE WHEN do.close_reason = 'not_entered' THEN 1 ELSE 0 END) AS not_entered_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS exit_pool_count,
                SUM(CASE WHEN do.close_reason IN ('exit_pool', 'hit_take_profit') THEN 1 ELSE 0 END) AS take_profit_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'target_hit' THEN 1 ELSE 0 END) AS target_hit_count,
                SUM(CASE WHEN do.exit_subreason = 'weakening_after_tp_zone' THEN 1 ELSE 0 END) AS weakening_exit_count,
                SUM(CASE WHEN do.exit_subreason = 'macro_protection' THEN 1 ELSE 0 END) AS macro_protection_count,
                SUM(CASE WHEN do.close_reason = 'hit_invalidation' THEN 1 ELSE 0 END) AS invalidation_exit_count,
                SUM(CASE WHEN do.close_reason = 'window_complete' THEN 1 ELSE 0 END) AS window_complete_count,
                ROUND(AVG(do.realized_return), 2) AS avg_realized_return,
                SUM(
                    CASE
                        WHEN do.decision_id IS NOT NULL
                         AND do.close_reason != ''
                         AND do.close_reason != 'insufficient_lookahead'
                        THEN 1 ELSE 0
                    END
                ) AS completed_count
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND dr.created_at < ?"
            params.append(until)
        cursor = self.connection.execute(query, tuple(params))
        row = cursor.fetchone()
        if row is None:
            cursor = self.connection.execute(
                """
                SELECT
                    0 AS decision_count,
                    0 AS outcome_count,
                    0 AS pending_count,
                    0 AS entered_count,
                    0 AS not_entered_count,
                    0 AS exit_pool_count,
                    0 AS take_profit_exit_count,
                    0 AS target_hit_count,
                    0 AS weakening_exit_count,
                    0 AS macro_protection_count,
                    0 AS invalidation_exit_count,
                    0 AS window_complete_count,
                    NULL AS avg_realized_return,
                    0 AS completed_count
                """
            )
            row = cursor.fetchone()
        return row

    def load_decision_history_for_archive(self, *, before: str, limit: int = 0) -> list[sqlite3.Row]:
        query = """
            SELECT
                dr.*,
                do.t_plus_1_return,
                do.t_plus_3_return,
                do.t_plus_5_return,
                do.t_plus_7_return,
                do.t_plus_10_return,
                do.t_plus_14_return,
                do.t_plus_30_return,
                do.max_runup,
                do.max_drawdown,
                do.entered,
                do.entered_at,
                do.entry_price,
                do.exit_price,
                do.realized_return,
                do.holding_days,
                do.gross_realized_return,
                do.net_realized_return,
                do.slippage_bps,
                do.hit_take_profit,
                do.hit_invalidation,
                do.close_reason,
                do.updated_at AS outcome_updated_at
            FROM decision_records dr
            LEFT JOIN decision_outcomes do ON dr.decision_id = do.decision_id
            WHERE dr.created_at < ?
            ORDER BY dr.created_at ASC, dr.decision_id ASC
        """
        params: list[Any] = [before]
        if limit > 0:
            query += " LIMIT ?"
            params.append(limit)
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def delete_decision_history(self, decision_ids: list[str]) -> dict[str, int]:
        if not decision_ids:
            return {"deleted_records": 0, "deleted_outcomes": 0}

        deleted_records = 0
        deleted_outcomes = 0
        chunk_size = 500
        for start in range(0, len(decision_ids), chunk_size):
            batch = decision_ids[start : start + chunk_size]
            placeholders = ", ".join("?" for _ in batch)
            outcome_cursor = self.connection.execute(
                f"DELETE FROM decision_outcomes WHERE decision_id IN ({placeholders})",
                tuple(batch),
            )
            record_cursor = self.connection.execute(
                f"DELETE FROM decision_records WHERE decision_id IN ({placeholders})",
                tuple(batch),
            )
            deleted_outcomes += max(int(outcome_cursor.rowcount), 0)
            deleted_records += max(int(record_cursor.rowcount), 0)
        self.connection.commit()
        return {"deleted_records": deleted_records, "deleted_outcomes": deleted_outcomes}

    def get_state(self, key: str) -> Optional[str]:
        cursor = self.connection.execute(
            "SELECT state_value FROM agent_state WHERE state_key = ?",
            (key,),
        )
        row = cursor.fetchone()
        return row["state_value"] if row else None

    def set_state(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO agent_state (state_key, state_value) VALUES (?, ?)",
            (key, value),
        )
        self.connection.commit()

    def record_llm_usage(
        self,
        *,
        run_id: str,
        event_id: str,
        symbol: str,
        component: str = "event_extraction",
        model: str,
        used_llm: bool,
        success: bool,
        prompt_tokens_estimate: int = 0,
        completion_tokens_estimate: int = 0,
        latency_ms: int = 0,
        reason: str = "",
        created_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO llm_usage
            (run_id, event_id, symbol, component, model, used_llm, success, prompt_tokens_estimate, completion_tokens_estimate, latency_ms, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                event_id,
                symbol,
                component,
                model,
                1 if used_llm else 0,
                1 if success else 0,
                int(prompt_tokens_estimate),
                int(completion_tokens_estimate),
                int(latency_ms),
                reason,
                created_at,
            ),
        )
        self.connection.commit()

    def count_llm_usage_since(self, since: str) -> int:
        cursor = self.connection.execute(
            """
            SELECT COUNT(*) AS usage_count
            FROM llm_usage
            WHERE used_llm = 1 AND created_at >= ?
            """,
            (since,),
        )
        row = cursor.fetchone()
        return int(row["usage_count"]) if row else 0

    def aggregate_llm_usage(self, *, start_at: str, end_at: str) -> dict[str, list[sqlite3.Row]]:
        summary_cursor = self.connection.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN used_llm = 1 THEN 1 ELSE 0 END) AS llm_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 1 THEN 1 ELSE 0 END) AS success_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 0 THEN 1 ELSE 0 END) AS failed_calls,
                SUM(CASE WHEN used_llm = 0 THEN 1 ELSE 0 END) AS skipped_calls,
                SUM(prompt_tokens_estimate) AS prompt_tokens,
                SUM(completion_tokens_estimate) AS completion_tokens,
                AVG(CASE WHEN latency_ms > 0 THEN latency_ms END) AS avg_latency_ms,
                MAX(latency_ms) AS max_latency_ms
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
            """,
            (start_at, end_at),
        )
        by_day_cursor = self.connection.execute(
            """
            SELECT
                substr(created_at, 1, 10) AS usage_date,
                COUNT(*) AS total_records,
                SUM(CASE WHEN used_llm = 1 THEN 1 ELSE 0 END) AS llm_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 1 THEN 1 ELSE 0 END) AS success_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 0 THEN 1 ELSE 0 END) AS failed_calls,
                SUM(CASE WHEN used_llm = 0 THEN 1 ELSE 0 END) AS skipped_calls,
                SUM(prompt_tokens_estimate) AS prompt_tokens,
                SUM(completion_tokens_estimate) AS completion_tokens,
                AVG(CASE WHEN latency_ms > 0 THEN latency_ms END) AS avg_latency_ms
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
            GROUP BY usage_date
            ORDER BY usage_date ASC
            """,
            (start_at, end_at),
        )
        by_component_cursor = self.connection.execute(
            """
            SELECT
                component,
                COUNT(*) AS total_records,
                SUM(CASE WHEN used_llm = 1 THEN 1 ELSE 0 END) AS llm_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 1 THEN 1 ELSE 0 END) AS success_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 0 THEN 1 ELSE 0 END) AS failed_calls,
                SUM(CASE WHEN used_llm = 0 THEN 1 ELSE 0 END) AS skipped_calls,
                SUM(prompt_tokens_estimate) AS prompt_tokens,
                SUM(completion_tokens_estimate) AS completion_tokens,
                AVG(CASE WHEN latency_ms > 0 THEN latency_ms END) AS avg_latency_ms
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
            GROUP BY component
            ORDER BY llm_calls DESC, component ASC
            """,
            (start_at, end_at),
        )
        by_model_cursor = self.connection.execute(
            """
            SELECT
                model,
                COUNT(*) AS total_records,
                SUM(CASE WHEN used_llm = 1 THEN 1 ELSE 0 END) AS llm_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 1 THEN 1 ELSE 0 END) AS success_calls,
                SUM(CASE WHEN used_llm = 1 AND success = 0 THEN 1 ELSE 0 END) AS failed_calls,
                SUM(prompt_tokens_estimate) AS prompt_tokens,
                SUM(completion_tokens_estimate) AS completion_tokens,
                AVG(CASE WHEN latency_ms > 0 THEN latency_ms END) AS avg_latency_ms
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
            GROUP BY model
            ORDER BY llm_calls DESC, model ASC
            """,
            (start_at, end_at),
        )
        top_reasons_cursor = self.connection.execute(
            """
            SELECT
                component,
                reason,
                COUNT(*) AS occurrence_count
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
              AND reason <> ''
            GROUP BY component, reason
            ORDER BY occurrence_count DESC, component ASC, reason ASC
            LIMIT 10
            """,
            (start_at, end_at),
        )
        recent_calls_cursor = self.connection.execute(
            """
            SELECT
                run_id,
                event_id,
                symbol,
                component,
                model,
                used_llm,
                success,
                prompt_tokens_estimate,
                completion_tokens_estimate,
                latency_ms,
                reason,
                created_at
            FROM llm_usage
            WHERE created_at >= ? AND created_at < ?
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (start_at, end_at),
        )
        return {
            "summary": summary_cursor.fetchall(),
            "by_day": by_day_cursor.fetchall(),
            "by_component": by_component_cursor.fetchall(),
            "by_model": by_model_cursor.fetchall(),
            "top_reasons": top_reasons_cursor.fetchall(),
            "recent_calls": recent_calls_cursor.fetchall(),
        }

    def save_decision_record(
        self,
        *,
        decision_id: str,
        run_id: str,
        event_id: str,
        symbol: str,
        event_type: str,
        pool: str,
        action: str,
        priority: str,
        confidence: str,
        event_score: float,
        market_score: float,
        theme_score: float,
        final_score: float,
        trigger_mode: str,
        llm_used: bool,
        theme_ids: list[str],
        entry_plan: dict[str, Any],
        invalidation: dict[str, Any],
        ttl: str,
        packet: dict[str, Any],
        created_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO decision_records
            (decision_id, run_id, event_id, symbol, event_type, pool, action, priority, confidence,
             event_score, market_score, theme_score, final_score, trigger_mode, llm_used,
             theme_ids_json, entry_plan_json, invalidation_json, ttl, packet_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                run_id,
                event_id,
                symbol,
                event_type,
                pool,
                action,
                priority,
                confidence,
                event_score,
                market_score,
                theme_score,
                final_score,
                trigger_mode,
                1 if llm_used else 0,
                json.dumps(theme_ids, sort_keys=True),
                json.dumps(entry_plan, sort_keys=True),
                json.dumps(invalidation, sort_keys=True),
                ttl,
                json.dumps(packet, sort_keys=True),
                created_at,
            ),
        )
        self.connection.commit()

    def save_decision_outcome(
        self,
        *,
        decision_id: str,
        entered: bool = False,
        entered_at: str = "",
        entry_price: float | None = None,
        exit_price: float | None = None,
        realized_return: float | None = None,
        holding_days: int | None = None,
        gross_realized_return: float | None = None,
        net_realized_return: float | None = None,
        slippage_bps: float = 0.0,
        t_plus_1_return: float | None = None,
        t_plus_3_return: float | None = None,
        t_plus_5_return: float | None = None,
        t_plus_7_return: float | None = None,
        t_plus_10_return: float | None = None,
        t_plus_14_return: float | None = None,
        t_plus_30_return: float | None = None,
        max_runup: float | None = None,
        max_drawdown: float | None = None,
        hit_take_profit: bool = False,
        hit_invalidation: bool = False,
        close_reason: str = "",
        exit_subreason: str = "",
        updated_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT OR REPLACE INTO decision_outcomes
            (decision_id, entered, entered_at, entry_price, exit_price, realized_return, holding_days,
             gross_realized_return, net_realized_return, slippage_bps,
             t_plus_1_return, t_plus_3_return, t_plus_5_return, t_plus_7_return, t_plus_10_return, t_plus_14_return, t_plus_30_return,
             max_runup, max_drawdown, hit_take_profit, hit_invalidation, close_reason, exit_subreason, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                1 if entered else 0,
                entered_at,
                entry_price,
                exit_price,
                realized_return,
                holding_days,
                gross_realized_return,
                net_realized_return,
                slippage_bps,
                t_plus_1_return,
                t_plus_3_return,
                t_plus_5_return,
                t_plus_7_return,
                t_plus_10_return,
                t_plus_14_return,
                t_plus_30_return,
                max_runup,
                max_drawdown,
                1 if hit_take_profit else 0,
                1 if hit_invalidation else 0,
                close_reason,
                exit_subreason,
                updated_at,
            ),
        )
        self.connection.commit()

    def record_candidate_evaluation(
        self,
        *,
        run_id: str,
        stage: str,
        symbol: str,
        horizon: str,
        outcome: str,
        reason: str = "",
        event_id: str = "",
        score: float | None = None,
        strategy_version: str = "",
        payload: dict[str, Any] | None = None,
        created_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO candidate_evaluations
            (run_id, stage, symbol, horizon, event_id, outcome, reason, score, strategy_version, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                stage,
                symbol,
                horizon,
                event_id,
                outcome,
                reason,
                score,
                strategy_version,
                json.dumps(payload or {}, sort_keys=True),
                created_at,
            ),
        )
        self.connection.commit()

    def load_candidate_evaluations(self, run_id: str, *, stage: str = "") -> list[sqlite3.Row]:
        query = """
            SELECT *
            FROM candidate_evaluations
            WHERE run_id = ?
        """
        params: list[Any] = [run_id]
        if stage:
            query += " AND stage = ?"
            params.append(stage)
        query += " ORDER BY created_at ASC, evaluation_id ASC"
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def load_candidate_evaluations_for_window(
        self,
        *,
        since: str,
        until: str = "",
        stage: str = "",
    ) -> list[sqlite3.Row]:
        query = """
            SELECT *
            FROM candidate_evaluations
            WHERE created_at >= ?
        """
        params: list[Any] = [since]
        if until:
            query += " AND created_at < ?"
            params.append(until)
        if stage:
            query += " AND stage = ?"
            params.append(stage)
        query += " ORDER BY created_at ASC, evaluation_id ASC"
        cursor = self.connection.execute(query, tuple(params))
        return cursor.fetchall()

    def _bar_table(self, timeframe: str) -> str:
        if timeframe == "5m":
            return "price_bars_5m"
        if timeframe == "1d":
            return "price_bars_1d"
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def _ensure_columns(self) -> None:
        self._ensure_table_column("raw_events", "event_fingerprint", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("raw_events", "run_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("event_insights", "run_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("opportunity_cards", "run_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("alert_history", "run_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("agent_runs", "run_name", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("agent_runs", "note", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("agent_runs", "config_snapshot_json", "TEXT NOT NULL DEFAULT '{}'")
        self._ensure_table_column("decision_records", "event_type", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("decision_outcomes", "entered", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_table_column("decision_outcomes", "entered_at", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("decision_outcomes", "entry_price", "REAL")
        self._ensure_table_column("decision_outcomes", "exit_price", "REAL")
        self._ensure_table_column("decision_outcomes", "realized_return", "REAL")
        self._ensure_table_column("decision_outcomes", "holding_days", "INTEGER")
        self._ensure_table_column("decision_outcomes", "gross_realized_return", "REAL")
        self._ensure_table_column("decision_outcomes", "net_realized_return", "REAL")
        self._ensure_table_column("decision_outcomes", "slippage_bps", "REAL NOT NULL DEFAULT 0")
        self._ensure_table_column("decision_outcomes", "t_plus_7_return", "REAL")
        self._ensure_table_column("decision_outcomes", "t_plus_14_return", "REAL")
        self._ensure_table_column("decision_outcomes", "t_plus_30_return", "REAL")
        self._ensure_table_column("decision_outcomes", "exit_subreason", "TEXT NOT NULL DEFAULT ''")
        self._ensure_table_column("price_bars_5m", "adjusted", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_table_column("price_bars_1d", "adjusted", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_table_column("llm_usage", "component", "TEXT NOT NULL DEFAULT 'event_extraction'")
        self._ensure_table_column("llm_usage", "latency_ms", "INTEGER NOT NULL DEFAULT 0")
        self._backfill_decision_record_event_types()

    def _ensure_indexes(self) -> None:
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_events_fingerprint
            ON raw_events (symbol, event_fingerprint, published_at DESC)
            """
        )

    def _ensure_table_column(self, table: str, column: str, sql_type: str) -> None:
        cursor = self.connection.execute(f"PRAGMA table_info({table})")
        columns = {row["name"] for row in cursor.fetchall()}
        if column not in columns:
            self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")

    def _backfill_decision_record_event_types(self) -> None:
        self.connection.execute(
            """
            UPDATE decision_records
            SET event_type = (
                SELECT ei.event_type
                FROM event_insights ei
                WHERE ei.event_id = decision_records.event_id
                  AND ei.run_id = decision_records.run_id
                LIMIT 1
            )
            WHERE (event_type = '' OR event_type IS NULL)
              AND EXISTS (
                SELECT 1
                FROM event_insights ei
                WHERE ei.event_id = decision_records.event_id
                  AND ei.run_id = decision_records.run_id
            )
            """
        )
        cursor = self.connection.execute(
            """
            SELECT decision_id, packet_json
            FROM decision_records
            WHERE event_type = '' OR event_type IS NULL
            """
        )
        for row in cursor.fetchall():
            event_type = self._extract_event_type_from_packet_json(row["packet_json"])
            if event_type:
                self.connection.execute(
                    "UPDATE decision_records SET event_type = ? WHERE decision_id = ?",
                    (event_type, row["decision_id"]),
                )

    def _extract_event_type_from_packet_json(self, packet_json: str | None) -> str:
        if not packet_json:
            return ""
        try:
            payload = json.loads(packet_json)
        except json.JSONDecodeError:
            return ""
        if not isinstance(payload, dict):
            return ""
        event_type = payload.get("event_type")
        if isinstance(event_type, str) and event_type:
            return event_type
        event_assessment = payload.get("event_assessment")
        if isinstance(event_assessment, dict):
            nested_event_type = event_assessment.get("event_type")
            if isinstance(nested_event_type, str):
                return nested_event_type
        return ""
