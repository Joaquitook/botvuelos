"""Repositorio SQLite: histórico de precios y deduplicación de alertas."""
from __future__ import annotations
import logging
import sqlite3
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)

HISTORY_COLUMNS = [
    "timestamp_utc", "origin", "destination_iata", "destination_city",
    "destination_country", "price_usd", "departure_date", "return_date",
    "trip_type", "airline", "deep_link",
]

ALERT_COLUMNS = [
    "alert_hash", "timestamp_utc", "destination_iata", "trip_type",
    "price_usd", "avg_historical", "drop_pct",
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination_iata TEXT NOT NULL,
    destination_city TEXT,
    destination_country TEXT,
    price_usd REAL NOT NULL,
    departure_date TEXT,
    return_date TEXT,
    trip_type TEXT NOT NULL,
    airline TEXT,
    deep_link TEXT
);
CREATE INDEX IF NOT EXISTS idx_dest_type
    ON price_history(destination_iata, trip_type);

CREATE TABLE IF NOT EXISTS alerts_sent (
    alert_hash TEXT PRIMARY KEY,
    timestamp_utc TEXT NOT NULL,
    destination_iata TEXT,
    trip_type TEXT,
    price_usd REAL,
    avg_historical REAL,
    drop_pct REAL
);
"""


class DbRepository:
    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def append_history(self, records: Iterable[dict]) -> int:
        rows = [tuple(r.get(c, "") for c in HISTORY_COLUMNS) for r in records]
        if not rows:
            return 0
        placeholders = ",".join(["?"] * len(HISTORY_COLUMNS))
        cols = ",".join(HISTORY_COLUMNS)
        self._conn.executemany(
            f"INSERT INTO price_history ({cols}) VALUES ({placeholders})",
            rows,
        )
        self._conn.commit()
        return len(rows)

    def averages_by_destination(self) -> dict[tuple[str, str], dict]:
        """Retorna {(destination_iata, trip_type): {'avg': float, 'n': int}}."""
        cur = self._conn.execute(
            "SELECT destination_iata, trip_type, AVG(price_usd) AS avg, COUNT(*) AS n "
            "FROM price_history WHERE price_usd > 0 "
            "GROUP BY destination_iata, trip_type"
        )
        result: dict[tuple[str, str], dict] = {}
        for row in cur:
            result[(row["destination_iata"], row["trip_type"])] = {
                "avg": float(row["avg"]),
                "n": int(row["n"]),
            }
        return result

    def alert_already_sent(self, alert_hash: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM alerts_sent WHERE alert_hash = ? LIMIT 1", (alert_hash,)
        )
        return cur.fetchone() is not None

    def record_alert(self, record: dict) -> None:
        cols = ",".join(ALERT_COLUMNS)
        placeholders = ",".join(["?"] * len(ALERT_COLUMNS))
        values = tuple(record.get(c, "") for c in ALERT_COLUMNS)
        self._conn.execute(
            f"INSERT OR IGNORE INTO alerts_sent ({cols}) VALUES ({placeholders})",
            values,
        )
        self._conn.commit()
