"""Detección de anomalías de precio frente al promedio histórico."""
from __future__ import annotations
import hashlib
from dataclasses import dataclass


@dataclass
class Anomaly:
    record: dict
    avg_historical: float
    drop_pct: float
    alert_hash: str


def _bucket_price(price: float, width: float = 25.0) -> int:
    """Agrupa precios en buckets para evitar alertas duplicadas por variaciones mínimas."""
    return int(price // width)


def compute_alert_hash(record: dict) -> str:
    key = (
        f"{record['destination_iata']}|{record['trip_type']}|"
        f"{record['departure_date']}|{record.get('return_date', '')}|"
        f"{_bucket_price(record['price_usd'])}"
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def detect_anomalies(
    records: list[dict],
    averages: dict[tuple[str, str], dict],
    drop_threshold: float,
    min_history_points: int,
) -> list[Anomaly]:
    anomalies: list[Anomaly] = []
    for r in records:
        key = (r["destination_iata"], r["trip_type"])
        stats = averages.get(key)
        if not stats or stats["n"] < min_history_points:
            continue
        avg = stats["avg"]
        if avg <= 0:
            continue
        drop = (avg - r["price_usd"]) / avg
        if drop >= drop_threshold:
            anomalies.append(Anomaly(
                record=r,
                avg_historical=round(avg, 2),
                drop_pct=round(drop, 4),
                alert_hash=compute_alert_hash(r),
            ))
    return anomalies
