from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def hours_in_local_date(local_date: str) -> int:
    oslo = ZoneInfo("Europe/Oslo")
    start = datetime.fromisoformat(f"{local_date}T00:00:00").replace(tzinfo=oslo)
    end = start + timedelta(days=1)
    return int((end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds() / 3600)


def test_oslo_spring_dst_day_has_23_hours() -> None:
    assert hours_in_local_date("2025-03-30") == 23


def test_oslo_autumn_dst_day_has_25_hours() -> None:
    assert hours_in_local_date("2025-10-26") == 25


def test_ordinary_oslo_day_has_24_hours() -> None:
    assert hours_in_local_date("2025-11-10") == 24
