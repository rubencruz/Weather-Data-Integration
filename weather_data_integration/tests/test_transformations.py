import pytest


@pytest.mark.parametrize(
    ("celsius", "fahrenheit"),
    [(0, 32), (20, 68), (100, 212)],
)
def test_temperature_conversion_reference(celsius, fahrenheit):
    assert ((celsius * 9 / 5) + 32) == fahrenheit
