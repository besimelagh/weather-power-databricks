from datetime import date
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "acquire_raw_snapshots.py"
SPEC = importlib.util.spec_from_file_location("snapshots", SCRIPT)
snapshots = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(snapshots)


def test_price_url_is_the_documented_daily_zone_shape() -> None:
    assert snapshots.price_url(date(2025, 11, 10), "NO1") == (
        "https://www.hvakosterstrommen.no/api/v1/prices/2025/11-10_NO1.json"
    )


def test_weather_url_requests_only_the_contract_columns_in_utc() -> None:
    url = snapshots.weather_url(date(2025, 11, 10), 59.9139, 10.7522)
    assert url.startswith("https://archive-api.open-meteo.com/v1/archive?")
    assert "temperature_2m%2Cprecipitation%2Cwind_speed_10m" in url
    assert "timezone=UTC" in url
    assert "start_date=2025-11-09" in url
    assert "end_date=2025-11-11" in url
