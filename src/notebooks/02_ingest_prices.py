# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Ingest electricity-price payload into Bronze
# MAGIC Hvakosterstrømmen has one small JSON document per local date and price zone.

# COMMAND ----------

SOURCE = "hvakosterstrommen"
API_URL = (
    "https://www.hvakosterstrommen.no/api/v1/prices/"
    f"{BUSINESS_DATE:%Y}/{BUSINESS_DATE:%m-%d}_{ZONE}.json"
)

try:
    effective_ingestion_mode = INGESTION_MODE
    if INGESTION_MODE == "api":
        try:
            payload, request_url, http_status = fetch_json(API_URL)
        except SourceError as api_error:
            # Free Edition can block outbound DNS/HTTPS. A date-partitioned snapshot
            # keeps the demo reproducible without masking API contract errors (4xx/
            # invalid JSON), which still fail fast in fetch_json.
            try:
                payload, request_url, http_status = snapshot_json(SOURCE)
                effective_ingestion_mode = "snapshot_fallback"
                print(
                    "Price API unavailable after bounded retries; using the uploaded "
                    f"snapshot instead. Reason: {api_error}"
                )
            except SourceError as snapshot_error:
                raise SourceError(
                    "Price API is unavailable and the snapshot fallback could not be read. "
                    f"API error: {api_error}; snapshot error: {snapshot_error}"
                ) from snapshot_error
    else:
        payload, request_url, http_status = snapshot_json(SOURCE)

    expected_fields = {"NOK_per_kWh", "EUR_per_kWh", "EXR", "time_start", "time_end"}
    if not isinstance(payload, list) or not payload:
        raise SourceContractError("Price payload is empty or not a JSON array")
    if not expected_fields.issubset(payload[0]):
        raise SourceContractError(
            "Price payload lacks expected fields: " f"{sorted(expected_fields)}"
        )

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
                len(payload),
                utc_now(),
                RUN_ID,
                effective_ingestion_mode,
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
        f"Bronze price payload stored for {RUN_DATE}: {len(payload)} hourly records, "
        f"hash={staged.first()['payload_sha256'][:12]}..."
    )
except Exception as exc:
    append_ingestion_error(SOURCE, type(exc).__name__, str(exc), API_URL)
    raise
