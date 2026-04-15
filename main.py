"""Orquestador del pipeline: fetch → persist → analyze → notify."""
from __future__ import annotations
import logging
import sys

from config import Config
from travelpayouts_client import TravelpayoutsClient
from db_repo import DbRepository
from analyzer import detect_anomalies
from notifier import TelegramNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("flight-bot")


def run() -> int:
    cfg = Config.from_env()

    client = TravelpayoutsClient(cfg.travelpayouts_token, cfg.travelpayouts_marker)
    repo = DbRepository(cfg.db_path)
    notifier = TelegramNotifier(cfg.telegram_bot_token, cfg.telegram_chat_id)

    try:
        months = cfg.upcoming_months()
        log.info(
            "Buscando: origen=%s destinos=%d meses=%s modos=%s",
            cfg.origin, len(cfg.destinations), months, cfg.search_modes,
        )

        records = client.search_all(
            origin=cfg.origin,
            destinations=cfg.destinations,
            months=months,
            search_modes=cfg.search_modes,
            max_roundtrip_nights=cfg.max_roundtrip_nights,
            currency=cfg.currency,
        )
        log.info("Vuelos obtenidos: %d", len(records))

        averages_before = repo.averages_by_destination()

        written = repo.append_history(records)
        log.info("Filas escritas en price_history: %d", written)

        anomalies = detect_anomalies(
            records=records,
            averages=averages_before,
            drop_threshold=cfg.anomaly_drop_pct,
            min_history_points=cfg.min_history_points,
        )
        log.info("Anomalías candidatas: %d", len(anomalies))

        sent = 0
        for a in anomalies:
            if repo.alert_already_sent(a.alert_hash):
                log.info("Alerta ya enviada, skip: %s", a.alert_hash)
                continue
            notifier.send(TelegramNotifier.format_anomaly(a))
            repo.record_alert({
                "alert_hash": a.alert_hash,
                "timestamp_utc": a.record["timestamp_utc"],
                "destination_iata": a.record["destination_iata"],
                "trip_type": a.record["trip_type"],
                "price_usd": a.record["price_usd"],
                "avg_historical": a.avg_historical,
                "drop_pct": a.drop_pct,
            })
            sent += 1

        log.info("Alertas enviadas: %d", sent)
        return 0
    finally:
        repo.close()


if __name__ == "__main__":
    sys.exit(run())
