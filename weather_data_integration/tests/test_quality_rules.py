from src.quality.rules import QUALITY_RULES


def test_quality_rules_are_defined():
    assert "humidity_range" in QUALITY_RULES
    assert "wind_non_negative" in QUALITY_RULES
    assert "precipitation_non_negative" in QUALITY_RULES
