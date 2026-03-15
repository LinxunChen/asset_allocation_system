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
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
                )
                for bar in bars
            ],
        )
        self.connection.commit()

    def load_price_bars(self, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        table = self._bar_table(timeframe)
        cursor = self.connection.execute(
            f"""
            SELECT timestamp, open, high, low, close, volume
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
            )
            for row in rows
        ]

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

    def aggregate_event_type_performance(self, since: str, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                ei.event_type AS event_type,
                COUNT(*) AS card_count,
                ROUND(AVG(oc.final_score), 2) AS avg_final_score,
                SUM(CASE WHEN oc.priority = 'high' THEN 1 ELSE 0 END) AS high_priority_count
            FROM opportunity_cards oc
            JOIN event_insights ei ON oc.event_id = ei.event_id
            WHERE oc.created_at >= ?
            GROUP BY ei.event_type
            ORDER BY avg_final_score DESC, card_count DESC
            LIMIT ?
            """,
            (since, limit),
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

    def aggregate_source_stability(self, since: str, limit: int = 10) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                source_name,
                COUNT(*) AS check_count,
                SUM(CASE WHEN status = 'healthy' THEN 1 ELSE 0 END) AS healthy_count,
                SUM(CASE WHEN status != 'healthy' THEN 1 ELSE 0 END) AS unhealthy_count,
                ROUND(AVG(latency_ms), 2) AS avg_latency_ms,
                MAX(checked_at) AS last_checked_at
            FROM source_health_checks
            WHERE checked_at >= ?
            GROUP BY source_name
            ORDER BY healthy_count DESC, check_count DESC, source_name ASC
            LIMIT ?
            """,
            (since, limit),
        )
        return cursor.fetchall()

    def aggregate_alert_volume(self, since: str, limit: int = 14) -> list[sqlite3.Row]:
        cursor = self.connection.execute(
            """
            SELECT
                substr(notified_at, 1, 10) AS bucket_date,
                COUNT(*) AS total_alerts,
                SUM(CASE WHEN sent = 1 THEN 1 ELSE 0 END) AS sent_alerts,
                SUM(CASE WHEN sent = 0 THEN 1 ELSE 0 END) AS suppressed_alerts,
                SUM(CASE WHEN priority = 'high' THEN 1 ELSE 0 END) AS high_priority_alerts,
                SUM(CASE WHEN priority = 'high' AND sent = 1 THEN 1 ELSE 0 END) AS sent_high_priority_alerts
            FROM alert_history
            WHERE notified_at >= ?
            GROUP BY bucket_date
            ORDER BY bucket_date DESC
            LIMIT ?
            """,
            (since, limit),
        )
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
