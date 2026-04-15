"""Carga de configuración desde variables de entorno."""
from __future__ import annotations
import os
from dataclasses import dataclass, field
from datetime import date


DEFAULT_DESTINATIONS = [
    # Sudamérica
    "EZE", "AEP", "GRU", "GIG", "SCL", "LIM", "BOG", "MDE", "ASU",
    "CWB", "FLN", "POA", "SSA", "FOR",
    # Norte/Centroamérica y Caribe
    "MIA", "JFK", "LAX", "MCO", "ATL", "CUN", "PUJ", "MEX", "PTY",
    # Europa
    "MAD", "BCN", "LIS", "OPO", "FCO", "MXP", "CDG", "ORY",
    "LHR", "AMS", "FRA", "MUC", "BRU",
]


@dataclass(frozen=True)
class Config:
    travelpayouts_token: str
    telegram_bot_token: str
    telegram_chat_id: str
    travelpayouts_marker: str = ""
    db_path: str = "prices.db"
    origin: str = "MVD"
    currency: str = "usd"
    destinations: tuple[str, ...] = field(default_factory=lambda: tuple(DEFAULT_DESTINATIONS))
    search_modes: tuple[str, ...] = ("oneway", "roundtrip")
    months_ahead: int = 3
    max_roundtrip_nights: int = 20
    anomaly_drop_pct: float = 0.30
    min_history_points: int = 5

    def upcoming_months(self) -> list[str]:
        today = date.today()
        months: list[str] = []
        y, m = today.year, today.month
        for i in range(1, self.months_ahead + 1):
            nm = m + i
            ny = y + (nm - 1) // 12
            nm = ((nm - 1) % 12) + 1
            months.append(f"{ny:04d}-{nm:02d}")
        return months

    @staticmethod
    def from_env() -> "Config":
        required = ["TRAVELPAYOUTS_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"Variables de entorno faltantes: {missing}")

        custom_dests = os.getenv("DESTINATIONS", "").strip()
        destinations = (
            tuple(d.strip().upper() for d in custom_dests.split(",") if d.strip())
            if custom_dests else tuple(DEFAULT_DESTINATIONS)
        )

        return Config(
            travelpayouts_token=os.environ["TRAVELPAYOUTS_TOKEN"],
            travelpayouts_marker=os.getenv("TRAVELPAYOUTS_MARKER", ""),
            telegram_bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
            telegram_chat_id=os.environ["TELEGRAM_CHAT_ID"],
            db_path=os.getenv("DB_PATH", "prices.db"),
            destinations=destinations,
            min_history_points=int(os.getenv("MIN_HISTORY_POINTS", "5")),
        )
