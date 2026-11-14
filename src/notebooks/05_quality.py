# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Data-quality gate
# MAGIC
# MAGIC Results are persisted as data before critical failures are raised. This makes
# MAGIC quality observable over time instead of burying it only in a job log.

# COMMAND ----------

price = spark.table(PRICE_HOURLY).where(
    (F.col("zone") == ZONE) & (F.col("business_date") == F.lit(BUSINESS_DATE))
)
weather = spark.table(WEATHER_HOURLY).where(
    (F.col("zone") == ZONE) & (F.col("business_date") == F.lit(BUSINESS_DATE))
)
fact = spark.table(HOURLY_FACT).where(
    (F.col("zone") == ZONE) & (F.col("business_date") == F.lit(BUSINESS_DATE))
)


def count_rows(frame: DataFrame) -> int:
    return int(frame.count())


checks: list[tuple[str, str, str, float, str, str | None]] = []


def add_check(
    name: str,
    severity: str,
    observed: float,
    passed: bool,
    expected_rule: str,
    details: str | None = None,
) -> None:
    checks.append(
        (
            name,
            severity,
            "PASS" if passed else "FAIL",
            float(observed),
            expected_rule,
            details,
        )
    )


price_duplicates = count_rows(
    price.groupBy("zone", "hour_start_utc").count().where(F.col("count") > 1)
)
add_check(
    "price_natural_key_unique",
    "ERROR",
    price_duplicates,
    price_duplicates == 0,
    "0 duplicate (zone, hour_start_utc) keys",
)

price_row_count = count_rows(price)
add_check(
    "price_rows_present",
    "ERROR",
    price_row_count,
    23 <= price_row_count <= 25,
    "23 to 25 price rows for the requested zone/date",
)

price_invalid = count_rows(
    price.where(
        F.col("hour_start_utc").isNull()
        | F.col("nok_per_kwh").isNull()
        | ~F.col("nok_per_kwh").between(-10, 50)
        | (F.col("hour_end_utc") <= F.col("hour_start_utc"))
    )
)
add_check(
    "price_contract_and_range",
    "ERROR",
    price_invalid,
    price_invalid == 0,
    "non-null key/price, ordered hour end, NOK/kWh in [-10, 50]",
    "Negative prices remain valid.",
)

zone_day_count_anomalies = count_rows(
    price.groupBy("zone", "business_date")
    .agg(F.countDistinct("hour_start_utc").alias("hour_count"))
    .where(~F.col("hour_count").between(23, 25))
)
add_check(
    "price_day_hour_coverage",
    "ERROR",
    zone_day_count_anomalies,
    zone_day_count_anomalies == 0,
    "23 to 25 distinct UTC hours per requested local price day",
    "The range intentionally permits daylight-saving days.",
)

weather_invalid = count_rows(
    weather.where(
        F.col("hour_start_utc").isNull()
        | (F.col("temperature_c").isNotNull() & ~F.col("temperature_c").between(-70, 60))
        | (F.col("wind_speed_mps").isNotNull() & ~F.col("wind_speed_mps").between(0, 120))
        | (
            F.col("precipitation_mm").isNotNull()
            & ~F.col("precipitation_mm").between(0, 500)
        )
    )
)
add_check(
    "weather_physical_ranges",
    "ERROR",
    weather_invalid,
    weather_invalid == 0,
    "temperature [-70,60], wind [0,120], precipitation [0,500] when present",
)

fact_duplicates = count_rows(
    fact.groupBy("zone", "hour_start_utc").count().where(F.col("count") > 1)
)
add_check(
    "joined_fact_natural_key_unique",
    "ERROR",
    fact_duplicates,
    fact_duplicates == 0,
    "0 duplicate (zone, hour_start_utc) fact keys",
)

price_hours = count_rows(fact)
complete_weather_hours = count_rows(fact.where(~F.col("weather_missing")))
weather_coverage = complete_weather_hours / price_hours if price_hours else 0.0
add_check(
    "weather_join_coverage",
    "WARN",
    weather_coverage,
    weather_coverage >= 0.95,
    ">= 95% of price hours have complete weather",
    f"{complete_weather_hours} complete weather hours across {price_hours} price hours.",
)

errors_in_this_run = count_rows(
    spark.table(INGESTION_ERRORS).where(F.col("run_id") == RUN_ID)
)
add_check(
    "ingestion_errors_for_run",
    "ERROR",
    errors_in_this_run,
    errors_in_this_run == 0,
    "0 ingestion errors for this run",
)

quarantine_for_run = spark.table(QUARANTINE).where(F.col("run_id") == RUN_ID)
price_quarantined = count_rows(
    quarantine_for_run.where(F.col("source") == "hvakosterstrommen")
)
add_check(
    "price_source_quarantine",
    "ERROR",
    price_quarantined,
    price_quarantined == 0,
    "0 quarantined price records for this run",
)

weather_quarantined = count_rows(
    quarantine_for_run.where(F.col("source") == "open_meteo")
)
add_check(
    "weather_source_quarantine",
    "WARN",
    weather_quarantined,
    weather_quarantined == 0,
    "0 quarantined weather records for this run",
)

dq_schema = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("business_date", T.DateType(), False),
        T.StructField("layer", T.StringType(), False),
        T.StructField("rule_name", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("observed_value", T.DoubleType(), False),
        T.StructField("expected_rule", T.StringType(), False),
        T.StructField("details", T.StringType(), True),
        T.StructField("checked_at", T.TimestampType(), False),
    ]
)
dq_rows = [
    (
        RUN_ID,
        BUSINESS_DATE,
        "silver",
        name,
        severity,
        status,
        observed,
        expected,
        details,
        utc_now(),
    )
    for name, severity, status, observed, expected, details in checks
]
dq_frame = spark.createDataFrame(dq_rows, dq_schema)
dq_frame.write.mode("append").saveAsTable(DQ_RESULTS)
display(dq_frame.orderBy("severity", "rule_name"))

critical_failures = [name for name, severity, status, *_ in checks if severity == "ERROR" and status == "FAIL"]
if critical_failures:
    raise RuntimeError(
        "Data-quality gate failed; Gold was not refreshed. Failed rules: "
        + ", ".join(critical_failures)
    )

print("Data-quality gate passed (warnings, if any, were recorded).")
