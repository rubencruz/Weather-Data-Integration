from src.quality.rules import is_valid_weather_record


def test_valid_weather_record_is_accepted():
    record = {
        "city": "Brasilia",
        "temperature_c": 20.0,
        "humidity_pct": 45.0,
        "wind_speed_kmh": 10.0,
        "precipitation_mm": 0.0,
        "observation_time": "2026-08-08T10:00:00",
    }
    assert is_valid_weather_record(record)


def test_invalid_humidity_is_rejected():
    record = {
        "city": "Brasilia",
        "temperature_c": 20.0,
        "humidity_pct": 120.0,
        "wind_speed_kmh": 10.0,
        "precipitation_mm": 0.0,
        "observation_time": "2026-08-08T10:00:00",
    }
    assert not is_valid_weather_record(record)
