# Databricks notebook source
# MAGIC %md
# MAGIC # Shared helpers
# MAGIC This file is included with `%run` by the executable notebooks.  It intentionally
# MAGIC keeps configuration, identifiers and Delta merge behaviour in one visible place.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import random
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import requests
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


class SourceError(RuntimeError):
    """A source cannot currently be read; a job retry may be appropriate."""


class SourceContractError(RuntimeError):
    """The source returned a response that violates the expected contract."""


def _widget(name: str, default: str) -> str:
    """Get a Databricks widget safely in both interactive and job runs."""
    try:
        dbutils.widgets.text(name, default)  # noqa: F821 - supplied by Databricks
        value = dbutils.widgets.get(name)  # noqa: F821 - supplied by Databricks
        return value.strip() if value else default
    except Exception:
        return default


def _identifier(value: str, label: str) -> str:
    """Allow only a UC identifier, not arbitrary SQL passed through widgets."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} must be a simple SQL identifier; received {value!r}")
    return value


# UTC is deliberately the physical grain used by both parsing and joins.
spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOG = _identifier(_widget("catalog", "workspace"), "catalog")
ZONE = _widget("zone", "NO1").upper()
LATITUDE = float(_widget("latitude", "59.9139"))
LONGITUDE = float(_widget("longitude", "10.7522"))
INGESTION_MODE = _widget("ingestion_mode", "api").lower()
_run_date_value = _widget("run_date", "")
RUN_DATE = _run_date_value or (date.today() - timedelta(days=7)).isoformat()
BUSINESS_DATE = date.fromisoformat(RUN_DATE)
RUN_ID = _widget("run_id", "") or str(uuid.uuid4())

if ZONE not in {"NO1", "NO2", "NO3", "NO4", "NO5"}:
    raise ValueError(f"Unexpected Norwegian price zone: {ZONE}")
if INGESTION_MODE not in {"api", "snapshot"}:
    raise ValueError("ingestion_mode must be 'api' or 'snapshot'")

BRONZE_SCHEMA = "weather_power_bronze"
SILVER_SCHEMA = "weather_power_silver"
GOLD_SCHEMA = "weather_power_gold"
OPS_SCHEMA = "weather_power_ops"


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


RAW_PAYLOADS = table(BRONZE_SCHEMA, "api_payloads")
WEATHER_HOURLY = table(SILVER_SCHEMA, "weather_hourly")
PRICE_HOURLY = table(SILVER_SCHEMA, "price_hourly")
HOURLY_FACT = table(SILVER_SCHEMA, "hourly_weather_price")
INGESTION_ERRORS = table(OPS_SCHEMA, "ingestion_errors")
QUARANTINE = table(OPS_SCHEMA, "quarantine_hourly")
DQ_RESULTS = table(OPS_SCHEMA, "data_quality_results")


def canonical_json(payload: Any) -> str:
    """A stable payload representation makes reruns and source corrections observable."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_json(url: str, params: dict[str, Any] | None = None) -> tuple[Any, str, int]:
    """Fetch JSON with bounded retry only for errors that are normally transient."""
    transient_statuses = {408, 429, 500, 502, 503, 504}
    last_error: Exception | None = None
    for attempt in range(1, 5):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=(10, 30),
                headers={"User-Agent": "weather-power-databricks-interview-demo/1.0"},
            )
            if response.status_code in transient_statuses:
                raise SourceError(f"Transient HTTP {response.status_code} from {response.url}")
            if 400 <= response.status_code < 500:
                raise SourceContractError(
                    f"Non-retryable HTTP {response.status_code} from {response.url}: "
                    f"{response.text[:300]}"
                )
            response.raise_for_status()
            try:
                return response.json(), response.url, response.status_code
            except ValueError as exc:
                raise SourceContractError(f"Invalid JSON from {response.url}") from exc
        except SourceContractError:
            raise
        except (requests.RequestException, SourceError) as exc:
            last_error = exc
            if attempt == 4:
                break
            # Jitter prevents a retry stampede when many zones are ingested together.
            time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.5))
    raise SourceError(f"Source remained unavailable after 4 attempts: {last_error}")


def snapshot_json(source: str) -> tuple[Any, str, int]:
    """Read one-line JSON from the UC Volume used when Free Edition blocks egress."""
    path = (
        f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_snapshots/"
        f"{source}/{ZONE}/{RUN_DATE}.json"
    )
    try:
        payload_text = dbutils.fs.head(path, 2_000_000)  # noqa: F821 - Databricks runtime
        return json.loads(payload_text), f"volume://{path}", 200
    except Exception as exc:
        raise SourceError(
            f"Could not load snapshot {path}. Upload it or use ingestion_mode=api."
        ) from exc


def append_ingestion_error(
    source: str,
    error_type: str,
    error_message: str,
    request_url: str | None = None,
) -> None:
    """Persist the failure before re-raising it so job history is not the only evidence."""
    error_row = [
        (
            RUN_ID,
            source,
            ZONE,
            BUSINESS_DATE,
            error_type,
            error_message[:4000],
            request_url,
            utc_now(),
        )
    ]
    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("zone", T.StringType(), False),
            T.StructField("business_date", T.DateType(), False),
            T.StructField("error_type", T.StringType(), False),
            T.StructField("error_message", T.StringType(), False),
            T.StructField("request_url", T.StringType(), True),
            T.StructField("logged_at", T.TimestampType(), False),
        ]
    )
    spark.createDataFrame(error_row, schema).write.mode("append").saveAsTable(INGESTION_ERRORS)


def merge_dataframe(
    frame: DataFrame, target: str, keys: Iterable[str], update_matched: bool = True
) -> None:
    """MERGE all columns by a deterministic natural key.

    `UPDATE SET *` deliberately makes late source corrections visible in Silver.
    Bronze passes `update_matched=False`, so its first-seen raw metadata remains
    immutable when an identical payload is fetched again.
    """
    view_name = f"staged_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view_name)
    predicate = " AND ".join(f"t.{key} <=> s.{key}" for key in keys)
    matched_clause = "WHEN MATCHED THEN UPDATE SET *" if update_matched else ""
    spark.sql(
        f"""
        MERGE INTO {target} AS t
        USING {view_name} AS s
        ON {predicate}
        {matched_clause}
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(view_name)


def latest_raw_payload(source: str) -> DataFrame:
    """Select the most recently retrieved version of a source/day/zone payload."""
    window = Window.partitionBy("source", "source_zone", "business_date").orderBy(
        F.col("ingested_at").desc(), F.col("payload_sha256").desc()
    )
    return (
        spark.table(RAW_PAYLOADS)
        .where(
            (F.col("source") == source)
            & (F.col("source_zone") == ZONE)
            & (F.col("business_date") == F.lit(BUSINESS_DATE))
        )
        .withColumn("_rank", F.row_number().over(window))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def quarantine_rows(frame: DataFrame, source: str, reason: str, columns: list[str]) -> None:
    """Keep malformed rows for diagnosis instead of silently filtering them out."""
    if frame.limit(1).count() == 0:
        return
    to_write = frame.select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit("silver").alias("layer"),
        F.lit(source).alias("source"),
        F.lit(ZONE).alias("zone"),
        F.lit(BUSINESS_DATE).cast("date").alias("business_date"),
        F.lit(reason).alias("reason"),
        F.to_json(F.struct(*[F.col(column) for column in columns])).alias("raw_record_json"),
        F.lit(utc_now()).cast("timestamp").alias("quarantined_at"),
    )
    to_write.write.mode("append").saveAsTable(QUARANTINE)


print(
    f"Configuration: catalog={CATALOG}, zone={ZONE}, run_date={RUN_DATE}, "
    f"ingestion_mode={INGESTION_MODE}, run_id={RUN_ID}"
)
