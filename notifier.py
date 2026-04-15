"""Envío de alertas vía Telegram Bot API."""
from __future__ import annotations
import logging
from datetime import date

import requests

log = logging.getLogger(__name__)
TELEGRAM_ENDPOINT = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self._url = TELEGRAM_ENDPOINT.format(token=bot_token)
        self._chat_id = chat_id

    def send(self, text: str) -> None:
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        r = requests.post(self._url, json=payload, timeout=15)
        r.raise_for_status()

    @staticmethod
    def format_anomaly(a) -> str:
        r = a.record
        trip_type = r.get("trip_type", "")
        if trip_type == "roundtrip":
            header_icon = "🔁"
            trip_label = "Ida y vuelta"
            nights = _nights(r.get("departure_date", ""), r.get("return_date", ""))
            trip_extra = f" ({nights} noches)" if nights is not None else ""
            regreso_line = f"<b>Regreso:</b> {r.get('return_date') or 'N/A'}\n"
        else:
            header_icon = "🛫"
            trip_label = "Solo ida"
            trip_extra = ""
            regreso_line = ""

        dest_parts = [r.get("destination_iata", "")]
        if r.get("destination_city"):
            dest_parts.insert(0, r["destination_city"])
        dest_str = " ".join(p for p in dest_parts if p)

        return (
            f"🚨 <b>Vuelo barato detectado</b> {header_icon}\n\n"
            f"<b>Tipo:</b> {trip_label}{trip_extra}\n"
            f"<b>Ruta:</b> {r.get('origin', 'MVD')} → {dest_str}\n"
            f"<b>Precio actual:</b> USD {r['price_usd']:.2f}\n"
            f"<b>Promedio histórico:</b> USD {a.avg_historical:.2f}\n"
            f"<b>Caída:</b> {a.drop_pct * 100:.1f}%\n"
            f"<b>Salida:</b> {r.get('departure_date', '')}\n"
            f"{regreso_line}"
            f"<b>Aerolínea:</b> {r.get('airline') or 'N/A'}\n"
            f'<a href="{r["deep_link"]}">Ver / reservar</a>'
        )


def _nights(dep: str, ret: str) -> int | None:
    try:
        return (date.fromisoformat(ret) - date.fromisoformat(dep)).days
    except (TypeError, ValueError):
        return None
