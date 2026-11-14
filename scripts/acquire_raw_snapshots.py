"""Acquire small, immutable JSON snapshots for a Databricks Free Edition demo.

Run this locally only when the Free Edition workspace cannot reach public APIs.
It deliberately writes the original JSON documents; Bronze metadata and Delta
idempotency are still applied by the Databricks ingestion notebooks.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504}


def price_url(run_date: date, zone: str) -> str:
    return (
        "https://www.hvakosterstrommen.no/api/v1/prices/"
        f"{run_date:%Y}/{run_date:%m-%d}_{zone}.json"
    )


def weather_url(run_date: date, latitude: float, longitude: float) -> str:
    query = urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            # See Silver transform: a UTC buffer is needed for a local Oslo
            # electricity-price day, including the 23/25-hour DST transitions.
            "start_date": (run_date - timedelta(days=1)).isoformat(),
            "end_date": (run_date + timedelta(days=1)).isoformat(),
            "hourly": "temperature_2m,precipitation,wind_speed_10m",
            "timezone": "UTC",
        }
    )
    return f"https://archive-api.open-meteo.com/v1/archive?{query}"


def get_json(url: str) -> Any:
    """Use bounded exponential backoff for temporary source-side conditions."""
    for attempt in range(1, 5):
        request = Request(url, headers={"User-Agent": "weather-power-demo-snapshot/1.0"})
        try:
            with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed HTTPS URLs
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in TRANSIENT_STATUSES or attempt == 4:
                raise RuntimeError(f"HTTP {exc.code} for {url}") from exc
        except URLError as exc:
            if attempt == 4:
                raise RuntimeError(f"Network error for {url}: {exc.reason}") from exc
        time.sleep(2 ** (attempt - 1) + random.uniform(0, 0.5))
    raise AssertionError("unreachable")


def write_snapshot(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Stable compact JSON makes the file easy to upload and compare without changing data.
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, dest="run_date", type=date.fromisoformat)
    parser.add_argument("--out", default="sample_data/raw_snapshots", type=Path)
    parser.add_argument("--zone", default="NO1", choices=["NO1", "NO2", "NO3", "NO4", "NO5"])
    parser.add_argument("--latitude", default=59.9139, type=float)
    parser.add_argument("--longitude", default=10.7522, type=float)
    args = parser.parse_args()

    sources = {
        "open_meteo": weather_url(args.run_date, args.latitude, args.longitude),
        "hvakosterstrommen": price_url(args.run_date, args.zone),
    }
    for source, url in sources.items():
        payload = get_json(url)
        destination = args.out / source / args.zone / f"{args.run_date.isoformat()}.json"
        write_snapshot(destination, payload)
        print(f"Wrote {source} snapshot: {destination}")


if __name__ == "__main__":
    main()
