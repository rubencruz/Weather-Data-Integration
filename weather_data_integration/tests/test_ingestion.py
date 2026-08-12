from unittest.mock import Mock, patch

from src.ingestion.open_meteo import fetch_weather


@patch("src.ingestion.open_meteo.requests.get")
def test_fetch_weather_builds_request(mock_get):
    response = Mock()
    response.json.return_value = {"hourly": {"time": [], "temperature_2m": []}}
    response.raise_for_status.return_value = None
    mock_get.return_value = response

    payload = fetch_weather(
        "https://api.open-meteo.com/v1/forecast",
        "Brasilia",
        -15.79,
        -47.88,
        "America/Sao_Paulo",
    )

    mock_get.assert_called_once()
    assert payload["_metadata"]["city"] == "Brasilia"
