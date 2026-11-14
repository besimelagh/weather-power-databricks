# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Ingest weather payload into Bronze
# MAGIC The source is Open-Meteo's historical endpoint.  The raw JSON is preserved
# MAGIC verbatim (as a canonical JSON string) together with enough metadata to replay it.

# COMMAND ----------

SOURCE = "open_meteo"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    # Price days are local Oslo dates while this source is intentionally read in
    # UTC. Fetch a one-day UTC buffer on each side, then assign local business
    # dates in Silver; this covers the first/last price hour and DST correctly.
    "start_date": (BUSINESS_DATE - timedelta(days=1)).isoformat(),
    "end_date": (BUSINESS_DATE + timedelta(days=1)).isoformat(),
    "hourly": "temperature_2m,precipitation,wind_speed_10m",
    # UTC removes ambiguous local timestamps around daylight-saving transitions.
    "timezone": "UTC",
}

try:
    if INGESTION_MODE == "api":
        payload, request_url, http_status = fetch_json(API_URL, PARAMS)
    else:
        payload, request_url, http_status = snapshot_json(SOURCE)

    hourly = payload.get("hourly") if isinstance(payload, dict) else None
    expected_fields = {"time", "temperature_2m", "precipitation", "wind_speed_10m"}
    if not isinstance(hourly, dict) or not expected_fields.issubset(hourly):
        raise SourceContractError(
            "Open-Meteo payload lacks expected hourly fields: "
            f"{sorted(expected_fields)}"
        )
    record_count = len(hourly["time"])
    if record_count == 0:
        raise SourceContractError("Open-Meteo returned an empty hourly time array")

    payload_json = canonical_json(payload)
    staged = spark.createDataFrame(
        [
            (
                SOURCE,
                ZONE,
                BUSINESS_DATE,
                request_url,
                int(http_status),
                payload_json,
                sha256(payload_json),
                record_count,
                utc_now(),
                RUN_ID,
                INGESTION_MODE,
            )
        ],
        """
        source string, source_zone string, business_date date, request_url string,
        http_status int, payload_json string, payload_sha256 string, record_count int,
        ingested_at timestamp, run_id string, ingestion_mode string
        """,
    )
    merge_dataframe(
        staged,
        RAW_PAYLOADS,
        ["source", "source_zone", "business_date", "payload_sha256"],
        update_matched=False,
    )
    print(
        f"Bronze weather payload stored for {RUN_DATE}: {record_count} hourly records, "
        f"hash={staged.first()['payload_sha256'][:12]}..."
    )
except Exception as exc:
    append_ingestion_error(SOURCE, type(exc).__name__, str(exc), API_URL)
    raise
