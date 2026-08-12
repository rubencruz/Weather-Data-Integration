from __future__ import annotations

from typing import Mapping, Any


QUALITY_RULES = {
    "humidity_range": "0 <= humidity_pct <= 100",
    "wind_non_negative": "wind_speed_kmh >= 0",
    "precipitation_non_negative": "precipitation_mm >= 0",
}


def is_valid_weather_record(record: Mapping[str, Any]) -> bool:
    """Pure-Python quality predicate used by unit tests and documentation examples."""
    try:
        return bool(
            record.get("city")
            and record.get("observation_time")
            and record.get("temperature_c") is not None
            and 0 <= float(record["humidity_pct"]) <= 100
            and float(record["wind_speed_kmh"]) >= 0
            and float(record["precipitation_mm"]) >= 0
        )
    except (TypeError, ValueError, KeyError):
        return False


def quality_failure_reason(record: Mapping[str, Any]) -> str | None:
    if not record.get("city"):
        return "CITY_NULL"
    if not record.get("observation_time"):
        return "OBSERVATION_TIME_NULL"
    if record.get("temperature_c") is None:
        return "TEMPERATURE_NULL"
    try:
        if not 0 <= float(record["humidity_pct"]) <= 100:
            return "HUMIDITY_OUT_OF_RANGE"
        if float(record["wind_speed_kmh"]) < 0:
            return "WIND_SPEED_NEGATIVE"
        if float(record["precipitation_mm"]) < 0:
            return "PRECIPITATION_NEGATIVE"
    except (TypeError, ValueError, KeyError):
        return "INVALID_NUMERIC_VALUE"
    return None
