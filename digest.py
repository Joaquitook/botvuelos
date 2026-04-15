"""Resumen diario: el mejor deal detectado en las últimas 24h.

Sirve doble propósito:
  1. Darte visibilidad del mejor % de descuento del día, aunque no llegue al 30%.
  2. Heartbeat: si no te llega el digest, el bot está caído.
"""
from __future__ import annotations
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone

from config import Config
from notifier import TelegramNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("flight-digest")

# Top-N deals a mostrar (por % de descuento respecto al promedio histórico)
TOP_N = 3


def _last_24h_deals(conn: sqlite3.Connection) -> list[dict]:
    """Para cada registro insertado en las últimas 24h, devuelve precio, promedio
    de TODO el histórico de ese (destino, trip_type) y % de descuento."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """
        WITH recent AS (
            SELECT * FROM price_history WHERE timestamp_utc >= ?
        ),
        averages AS (
            SELECT destination_iata, trip_type,
                   AVG(price_usd) AS avg_price,
                   COUNT(*) AS n
            FROM price_history
            WHERE price_usd > 0
            GROUP BY destination_iata, trip_type
        )
        SELECT r.timestamp_utc, r.origin, r.destination_iata, r.destination_city,
               r.price_usd, r.departure_date, r.return_date, r.trip_type,
               r.airline, r.deep_link,
               a.avg_price, a.n
        FROM recent r
        JOIN averages a
          ON a.destination_iata = r.destination_iata
         AND a.trip_type = r.trip_type
        WHERE a.n >= 2
        """,
        (cutoff,),
    ).fetchall()

    deals: list[dict] = []
    for r in rows:
        avg = float(r["avg_price"] or 0)
        price = float(r["price_usd"])
        if avg <= 0 or price <= 0:
            continue
        drop = (avg - price) / avg
        deals.append({
            "origin": r["origin"],
            "destination_iata": r["destination_iata"],
            "destination_city": r["destination_city"],
            "price_usd": price,
            "avg_historical": round(avg, 2),
            "drop_pct": round(drop, 4),
            "departure_date": r["departure_date"],
            "return_date": r["return_date"],
            "trip_type": r["trip_type"],
            "airline": r["airline"],
            "deep_link": r["deep_link"],
            "sample_size": int(r["n"]),
        })
    # Mantener solo el mejor por ruta+tipo para no repetir
    best_per_route: dict[tuple[str, str], dict] = {}
    for d in deals:
        key = (d["destination_iata"], d["trip_type"])
        if key not in best_per_route or d["drop_pct"] > best_per_route[key]["drop_pct"]:
            best_per_route[key] = d
    return sorted(best_per_route.values(), key=lambda x: x["drop_pct"], reverse=True)


def _count_total_samples(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]


def _format_deal(d: dict, rank: int) -> str:
    trip = "🔁 Ida y vuelta" if d["trip_type"] == "roundtrip" else "🛫 Solo ida"
    dest = d["destination_city"] or d["destination_iata"]
    regreso = f" → {d['return_date']}" if d["return_date"] else ""
    sign = "🟢" if d["drop_pct"] > 0 else "🔴"
    return (
        f"<b>#{rank}</b> {trip}\n"
        f"{d['origin']} → {dest} ({d['destination_iata']})\n"
        f"USD {d['price_usd']:.2f}  (prom. {d['avg_historical']:.2f}) "
        f"{sign} {d['drop_pct'] * 100:+.1f}%\n"
        f"{d['departure_date']}{regreso}\n"
        f'<a href="{d["deep_link"]}">Ver vuelo</a>'
    )


def build_message(deals: list[dict], total_samples: int) -> str:
    today = date.today().isoformat()
    header = f"☀️ <b>Resumen diario — {today}</b>\n\n"
    footer = (
        f"\n\n<i>Histórico total: {total_samples} precios. "
        f"Alertas automáticas se disparan al detectar caídas ≥30%.</i>"
    )
    if not deals:
        return (
            header
            + "Sin datos comparables todavía (todavía estoy armando el histórico).\n"
            + f"<i>Bot activo ✅ — {total_samples} precios almacenados.</i>"
        )
    body_parts = [_format_deal(d, i + 1) for i, d in enumerate(deals[:TOP_N])]
    return header + "\n\n".join(body_parts) + footer


def run() -> int:
    cfg = Config.from_env()
    notifier = TelegramNotifier(cfg.telegram_bot_token, cfg.telegram_chat_id)

    conn = sqlite3.connect(cfg.db_path)
    conn.row_factory = sqlite3.Row
    try:
        deals = _last_24h_deals(conn)
        total = _count_total_samples(conn)
        log.info("Deals últimas 24h con histórico: %d  (total muestras: %d)", len(deals), total)
        msg = build_message(deals, total)
        notifier.send(msg)
        log.info("Digest enviado.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(run())
