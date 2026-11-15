# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Set up schemas, Delta tables and the offline snapshot volume
# MAGIC All DDL is `IF NOT EXISTS`, so it is safe to run repeatedly.

# COMMAND ----------

display(spark.sql("SELECT current_catalog() AS current_catalog, current_schema() AS current_schema"))
try:
    for schema_name in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, OPS_SCHEMA]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}")
except Exception as exc:
    raise RuntimeError(
        f"Cannot create medallion schemas in catalog '{CATALOG}'. "
        "Use a catalog where you have USE CATALOG and CREATE SCHEMA, or ask the "
        "workspace owner to grant those privileges before running the demo."
    ) from exc


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RAW_PAYLOADS} (
        source STRING NOT NULL,
        source_zone STRING NOT NULL,
        business_date DATE NOT NULL,
        request_url STRING NOT NULL,
        http_status INT NOT NULL,
        payload_json STRING NOT NULL,
        payload_sha256 STRING NOT NULL,
        record_count INT NOT NULL,
        ingested_at TIMESTAMP NOT NULL,
        run_id STRING NOT NULL,
        ingestion_mode STRING NOT NULL
    ) USING DELTA
    PARTITIONED BY (source, business_date)
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {WEATHER_HOURLY} (
        zone STRING NOT NULL,
        hour_start_utc TIMESTAMP NOT NULL,
        business_date DATE NOT NULL,
        temperature_c DOUBLE,
        wind_speed_mps DOUBLE,
        precipitation_mm DOUBLE,
        source_payload_sha256 STRING NOT NULL,
        processed_at TIMESTAMP NOT NULL,
        run_id STRING NOT NULL
    ) USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {PRICE_HOURLY} (
        zone STRING NOT NULL,
        hour_start_utc TIMESTAMP NOT NULL,
        hour_end_utc TIMESTAMP,
        business_date DATE NOT NULL,
        nok_per_kwh DOUBLE NOT NULL,
        eur_per_kwh DOUBLE,
        exchange_rate DOUBLE,
        source_payload_sha256 STRING NOT NULL,
        processed_at TIMESTAMP NOT NULL,
        run_id STRING NOT NULL
    ) USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {HOURLY_FACT} (
        zone STRING NOT NULL,
        hour_start_utc TIMESTAMP NOT NULL,
        business_date DATE NOT NULL,
        nok_per_kwh DOUBLE NOT NULL,
        eur_per_kwh DOUBLE,
        exchange_rate DOUBLE,
        temperature_c DOUBLE,
        wind_speed_mps DOUBLE,
        precipitation_mm DOUBLE,
        weather_handling STRING NOT NULL,
        weather_missing BOOLEAN NOT NULL,
        join_status STRING NOT NULL,
        price_payload_sha256 STRING NOT NULL,
        weather_payload_sha256 STRING,
        processed_at TIMESTAMP NOT NULL,
        run_id STRING NOT NULL
    ) USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {INGESTION_ERRORS} (
        run_id STRING NOT NULL,
        source STRING NOT NULL,
        zone STRING NOT NULL,
        business_date DATE NOT NULL,
        error_type STRING NOT NULL,
        error_message STRING NOT NULL,
        request_url STRING,
        logged_at TIMESTAMP NOT NULL
    ) USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {QUARANTINE} (
        run_id STRING NOT NULL,
        layer STRING NOT NULL,
        source STRING NOT NULL,
        zone STRING NOT NULL,
        business_date DATE NOT NULL,
        reason STRING NOT NULL,
        raw_record_json STRING NOT NULL,
        quarantined_at TIMESTAMP NOT NULL
    ) USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {DQ_RESULTS} (
        run_id STRING NOT NULL,
        business_date DATE NOT NULL,
        layer STRING NOT NULL,
        rule_name STRING NOT NULL,
        severity STRING NOT NULL,
        status STRING NOT NULL,
        observed_value DOUBLE NOT NULL,
        expected_rule STRING NOT NULL,
        details STRING,
        checked_at TIMESTAMP NOT NULL
    ) USING DELTA
    """
)

# A UC Volume is the documented Free Edition alternative when serverless egress
# is restricted. Failure to create it does not block direct API mode.
try:
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.raw_snapshots"
    )
    print(
        "Snapshot volume is ready: "
        f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_snapshots"
    )
except Exception as exc:
    print(f"Could not create snapshot volume; API mode is still available. Details: {exc}")

print("Setup complete. Execute the two ingestion notebooks next (they are independent).")
