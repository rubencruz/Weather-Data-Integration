from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests


HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "precipitation",
]


def fetch_weather(
    base_url: str,
    city: str,
    latitude: float,
    longitude: float,
    timezone_name: str = "UTC",
    forecast_days: int = 1,
    timeout: int = 30,
) -> dict[str, Any]:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": forecast_days,
        "timezone": timezone_name,
    }
    response = requests.get(base_url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    payload["_metadata"] = {
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return payload
