"""Cliente de Travelpayouts Data API (Aviasales) para búsquedas MVD -> destinos curados."""
from __future__ import annotations
import logging
import time
from datetime import datetime, date
from typing import Iterable

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.travelpayouts.com"
PRICES_ENDPOINT = "/aviasales/v3/prices_for_dates"
AVIASALES_HOST = "https://www.aviasales.com"
REQUEST_SLEEP_SECONDS = 0.2


class TravelpayoutsClient:
    def __init__(self, token: str, marker: str | None = None, timeout: int = 30):
        self._session = requests.Session()
        self._session.headers.update({
            "X-Access-Token": token,
            "Accept-Encoding": "gzip, deflate",
        })
        self._marker = marker or ""
        self._timeout = timeout

    def _get(self, path: str, params: dict) -> list[dict]:
        url = BASE_URL + path
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.warning("Fallo request %s params=%s err=%s", path, params, e)
            return []
        payload = resp.json()
        if not payload.get("success", True):
            log.warning("Respuesta sin success: %s", payload)
            return []
        return payload.get("data", []) or []

    def search_month(
        self,
        origin: str,
        destination: str,
        departure_month: str,
        one_way: bool,
        currency: str = "usd",
        limit: int = 30,
    ) -> list[dict]:
        """Consulta /prices_for_dates para un mes concreto (YYYY-MM)."""
        # Por defecto la API incluye vuelos directos Y con escala.
        # No mandamos el flag "direct" => se consideran ambos tipos.
        params = {
            "origin": origin,
            "destination": destination,
            "departure_at": departure_month,
            "one_way": "true" if one_way else "false",
            "currency": currency,
            "limit": limit,
            "sorting": "price",
            "unique": "false",
        }
        data = self._get(PRICES_ENDPOINT, params)
        time.sleep(REQUEST_SLEEP_SECONDS)
        return data

    def search_all(
        self,
        origin: str,
        destinations: Iterable[str],
        months: Iterable[str],
        search_modes: Iterable[str],
        max_roundtrip_nights: int,
        currency: str = "usd",
    ) -> list[dict]:
        """Itera destinos x meses x modos y devuelve registros ya normalizados."""
        records: list[dict] = []
        for dest in destinations:
            for month in months:
                for mode in search_modes:
                    one_way = mode == "oneway"
                    raw = self.search_month(
                        origin=origin,
                        destination=dest,
                        departure_month=month,
                        one_way=one_way,
                        currency=currency,
                    )
                    for r in raw:
                        normalized = self.normalize(r, mode, origin, dest, self._marker)
                        if normalized is None:
                            continue
                        if mode == "roundtrip":
                            nights = _nights_between(
                                normalized["departure_date"], normalized["return_date"]
                            )
                            if nights is None or nights <= 0 or nights > max_roundtrip_nights:
                                continue
                        records.append(normalized)
        log.info("Registros normalizados: %d", len(records))
        return records

    @staticmethod
    def normalize(
        raw: dict, trip_type: str, origin: str, destination: str, marker: str
    ) -> dict | None:
        price = raw.get("price")
        if price is None or float(price) <= 0:
            return None
        dep = (raw.get("departure_at") or "")[:10]
        ret = (raw.get("return_at") or "")[:10] if trip_type == "roundtrip" else ""
        link_path = raw.get("link") or ""
        if link_path:
            deep_link = AVIASALES_HOST + link_path
            if marker:
                sep = "&" if "?" in deep_link else "?"
                deep_link = f"{deep_link}{sep}marker={marker}"
        else:
            deep_link = _google_flights_url(origin, destination, dep, ret)
        return {
            "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "origin": raw.get("origin", origin),
            "destination_iata": raw.get("destination", destination),
            "destination_city": "",
            "destination_country": "",
            "price_usd": float(price),
            "departure_date": dep,
            "return_date": ret,
            "trip_type": trip_type,
            "airline": raw.get("airline", "") or "",
            "deep_link": deep_link,
        }


def _nights_between(dep: str, ret: str) -> int | None:
    try:
        d1 = date.fromisoformat(dep)
        d2 = date.fromisoformat(ret)
        return (d2 - d1).days
    except (TypeError, ValueError):
        return None


def _google_flights_url(origin: str, destination: str, dep: str, ret: str) -> str:
    if ret:
        q = f"Flights+from+{origin}+to+{destination}+on+{dep}+return+{ret}"
    else:
        q = f"Flights+from+{origin}+to+{destination}+on+{dep}"
    return f"https://www.google.com/travel/flights?q={q}"
