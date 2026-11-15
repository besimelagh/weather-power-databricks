# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Parse, validate and join hourly weather and price data
# MAGIC
# MAGIC This notebook intentionally joins only on `hour_start_utc`. Local time is a
# MAGIC reporting attribute, not a key: a local day can have 23 or 25 hours.

# COMMAND ----------

weather_schema = T.StructType(
    [
        T.StructField(
            "hourly",
            T.StructType(
                [
                    T.StructField("time", T.ArrayType(T.StringType())),
                    T.StructField("temperature_2m", T.ArrayType(T.DoubleType())),
                    T.StructField("precipitation", T.ArrayType(T.DoubleType())),
                    T.StructField("wind_speed_10m", T.ArrayType(T.DoubleType())),
                ]
            ),
        )
    ]
)

weather_raw = latest_raw_payload("open_meteo")
if weather_raw.limit(1).count() == 0:
    raise SourceContractError(f"No Bronze weather payload found for {ZONE} / {RUN_DATE}")

weather_doc = weather_raw.select(
    "payload_sha256", F.from_json("payload_json", weather_schema).alias("document")
)
weather_rows = (
    weather_doc.select(
        F.col("payload_sha256").alias("source_payload_sha256"),
        "document",
        F.posexplode(F.col("document.hourly.time")).alias("array_index", "hour_text"),
    )
    .select(
        F.lit(ZONE).alias("zone"),
        F.to_timestamp("hour_text", "yyyy-MM-dd'T'HH:mm").alias("hour_start_utc"),
        # Use Oslo calendar date only as a reporting/partition attribute. The
        # timestamp itself remains UTC and is the sole join key.
        F.to_date(
            F.from_utc_timestamp(
                F.to_timestamp("hour_text", "yyyy-MM-dd'T'HH:mm"), "Europe/Oslo"
            )
        ).alias("business_date"),
        F.element_at(
            F.col("document.hourly.temperature_2m"), F.col("array_index") + F.lit(1)
        ).cast("double").alias("temperature_c"),
        F.element_at(
            F.col("document.hourly.wind_speed_10m"), F.col("array_index") + F.lit(1)
        ).cast("double").alias("wind_speed_mps"),
        F.element_at(
            F.col("document.hourly.precipitation"), F.col("array_index") + F.lit(1)
        ).cast("double").alias("precipitation_mm"),
        "source_payload_sha256",
    )
    .withColumn("processed_at", F.lit(utc_now()).cast("timestamp"))
    .withColumn("run_id", F.lit(RUN_ID))
)

weather_is_invalid = (
    F.col("hour_start_utc").isNull()
    | (F.col("temperature_c").isNotNull() & ~F.col("temperature_c").between(-70, 60))
    | (F.col("wind_speed_mps").isNotNull() & ~F.col("wind_speed_mps").between(0, 120))
    | (
        F.col("precipitation_mm").isNotNull()
        & ~F.col("precipitation_mm").between(0, 500)
    )
)
weather_invalid = weather_rows.where(weather_is_invalid)
quarantine_rows(
    weather_invalid,
    "open_meteo",
    "invalid_timestamp_or_physical_range",
    [
        "hour_start_utc",
        "temperature_c",
        "wind_speed_mps",
        "precipitation_mm",
        "source_payload_sha256",
    ],
)
weather_duplicate_keys = (
    weather_rows.where(F.col("hour_start_utc").isNotNull())
    .groupBy("zone", "hour_start_utc")
    .count()
    .where(F.col("count") > 1)
    .select("zone", "hour_start_utc")
)
weather_duplicate_rows = weather_rows.join(
    weather_duplicate_keys, ["zone", "hour_start_utc"], "inner"
)
quarantine_rows(
    weather_duplicate_rows,
    "open_meteo",
    "duplicate_hour_key_in_source_payload",
    ["hour_start_utc", "temperature_c", "wind_speed_mps", "precipitation_mm", "source_payload_sha256"],
)
weather_valid = weather_rows.where(~weather_is_invalid).join(
    weather_duplicate_keys, ["zone", "hour_start_utc"], "left_anti"
)
merge_dataframe(weather_valid, WEATHER_HOURLY, ["zone", "hour_start_utc"])

# COMMAND ----------

price_item_schema = T.StructType(
    [
        T.StructField("NOK_per_kWh", T.DoubleType()),
        T.StructField("EUR_per_kWh", T.DoubleType()),
        T.StructField("EXR", T.DoubleType()),
        T.StructField("time_start", T.StringType()),
        T.StructField("time_end", T.StringType()),
    ]
)

price_raw = latest_raw_payload("hvakosterstrommen")
if price_raw.limit(1).count() == 0:
    raise SourceContractError(f"No Bronze price payload found for {ZONE} / {RUN_DATE}")

price_doc = price_raw.select(
    "payload_sha256",
    F.from_json("payload_json", T.ArrayType(price_item_schema)).alias("document"),
)
price_rows = (
    price_doc.select(
        F.col("payload_sha256").alias("source_payload_sha256"),
        F.explode("document").alias("item"),
    )
    .select(
        F.lit(ZONE).alias("zone"),
        # Let Spark parse the source's ISO-8601 offset directly.  The session is
        # pinned to UTC in _shared.py, so e.g. ``00:00:00+01:00`` is stored as
        # the corresponding UTC instant.  This is more portable across the
        # Free Edition serverless runtimes than an explicit formatter pattern.
        F.to_timestamp(F.trim(F.col("item.time_start"))).alias("hour_start_utc"),
        F.to_timestamp(F.trim(F.col("item.time_end"))).alias("hour_end_utc"),
        F.lit(BUSINESS_DATE).cast("date").alias("business_date"),
        F.col("item.NOK_per_kWh").cast("double").alias("nok_per_kwh"),
        F.col("item.EUR_per_kWh").cast("double").alias("eur_per_kwh"),
        F.col("item.EXR").cast("double").alias("exchange_rate"),
        "source_payload_sha256",
    )
    .withColumn("processed_at", F.lit(utc_now()).cast("timestamp"))
    .withColumn("run_id", F.lit(RUN_ID))
)

# `explode` would otherwise turn an unparsable document into zero rows and let
# the notebook appear to succeed.  Fail with an actionable source-contract
# error instead; valid-but-bad records still reach quarantine below.
if price_rows.limit(1).count() == 0:
    raise SourceContractError(
        "Price Bronze payload could not be parsed into hourly records. "
        "Inspect api_payloads.payload_json and the source contract."
    )

price_is_invalid = (
    F.col("hour_start_utc").isNull()
    | F.col("hour_end_utc").isNull()
    | (F.col("hour_end_utc") <= F.col("hour_start_utc"))
    | F.col("nok_per_kwh").isNull()
    # Negative electricity prices are legitimate; the deliberately wide lower bound keeps them.
    | ~F.col("nok_per_kwh").between(-10, 50)
)
price_invalid = price_rows.where(price_is_invalid)
quarantine_rows(
    price_invalid,
    "hvakosterstrommen",
    "invalid_timestamp_or_price_range",
    ["hour_start_utc", "hour_end_utc", "nok_per_kwh", "source_payload_sha256"],
)
price_duplicate_keys = (
    price_rows.where(F.col("hour_start_utc").isNotNull())
    .groupBy("zone", "hour_start_utc")
    .count()
    .where(F.col("count") > 1)
    .select("zone", "hour_start_utc")
)
price_duplicate_rows = price_rows.join(
    price_duplicate_keys, ["zone", "hour_start_utc"], "inner"
)
quarantine_rows(
    price_duplicate_rows,
    "hvakosterstrommen",
    "duplicate_hour_key_in_source_payload",
    ["hour_start_utc", "hour_end_utc", "nok_per_kwh", "source_payload_sha256"],
)
price_valid = price_rows.where(~price_is_invalid).join(
    price_duplicate_keys, ["zone", "hour_start_utc"], "left_anti"
)
if price_valid.limit(1).count() == 0:
    raise SourceContractError(
        "No conforming price rows remain after validation; invalid records were "
        "written to the quarantine table."
    )
merge_dataframe(price_valid, PRICE_HOURLY, ["zone", "hour_start_utc"])

# COMMAND ----------

# Build the hourly fact from the current business date. The price feed is the
# driving side: missing prices must never be invented; missing weather remains explicit.
price_hourly = spark.table(PRICE_HOURLY).where(
    (F.col("zone") == ZONE) & (F.col("business_date") == F.lit(BUSINESS_DATE))
)
weather_hourly = spark.table(WEATHER_HOURLY).where(
    (F.col("zone") == ZONE) & (F.col("business_date") == F.lit(BUSINESS_DATE))
)

joined = (
    price_hourly.alias("p")
    .join(
        weather_hourly.alias("w"),
        (F.col("p.zone") == F.col("w.zone"))
        & (F.col("p.hour_start_utc") == F.col("w.hour_start_utc")),
        "left",
    )
    .select(
        F.col("p.zone").alias("zone"),
        F.col("p.hour_start_utc").alias("hour_start_utc"),
        F.col("p.business_date").alias("business_date"),
        F.col("p.nok_per_kwh").alias("nok_per_kwh"),
        F.col("p.eur_per_kwh").alias("eur_per_kwh"),
        F.col("p.exchange_rate").alias("exchange_rate"),
        F.col("w.temperature_c").alias("temperature_c"),
        F.col("w.wind_speed_mps").alias("wind_speed_mps"),
        F.col("w.precipitation_mm").alias("precipitation_mm"),
        F.col("p.source_payload_sha256").alias("price_payload_sha256"),
        F.col("w.source_payload_sha256").alias("weather_payload_sha256"),
    )
)

# A narrowly scoped imputation policy: interpolate temperature and wind only for
# one internal hour when both neighbouring *complete* observations exist. Never
# impute precipitation; no values are carried across a longer weather outage.
complete_weather = (
    F.col("temperature_c").isNotNull()
    & F.col("wind_speed_mps").isNotNull()
    & F.col("precipitation_mm").isNotNull()
)
previous_window = (
    Window.partitionBy("zone")
    .orderBy("hour_start_utc")
    .rowsBetween(Window.unboundedPreceding, -1)
)
next_window = (
    Window.partitionBy("zone")
    .orderBy("hour_start_utc")
    .rowsBetween(1, Window.unboundedFollowing)
)

with_neighbours = (
    joined.withColumn("_complete_weather", complete_weather)
    .withColumn(
        "_previous_hour",
        F.last(
            F.when(F.col("_complete_weather"), F.col("hour_start_utc")), True
        ).over(previous_window),
    )
    .withColumn(
        "_next_hour",
        F.first(
            F.when(F.col("_complete_weather"), F.col("hour_start_utc")), True
        ).over(next_window),
    )
    .withColumn(
        "_previous_temperature",
        F.last(
            F.when(F.col("_complete_weather"), F.col("temperature_c")), True
        ).over(previous_window),
    )
    .withColumn(
        "_next_temperature",
        F.first(
            F.when(F.col("_complete_weather"), F.col("temperature_c")), True
        ).over(next_window),
    )
    .withColumn(
        "_previous_wind",
        F.last(
            F.when(F.col("_complete_weather"), F.col("wind_speed_mps")), True
        ).over(previous_window),
    )
    .withColumn(
        "_next_wind",
        F.first(
            F.when(F.col("_complete_weather"), F.col("wind_speed_mps")), True
        ).over(next_window),
    )
)

one_hour_internal_gap = (
    ~F.col("_complete_weather")
    & F.col("_previous_hour").isNotNull()
    & F.col("_next_hour").isNotNull()
    & (
        F.unix_timestamp(F.col("hour_start_utc"))
        - F.unix_timestamp(F.col("_previous_hour"))
        == F.lit(3600)
    )
    & (
        F.unix_timestamp(F.col("_next_hour"))
        - F.unix_timestamp(F.col("hour_start_utc"))
        == F.lit(3600)
    )
)

fact_stage = (
    with_neighbours.select(
        "zone",
        "hour_start_utc",
        "business_date",
        "nok_per_kwh",
        "eur_per_kwh",
        "exchange_rate",
        F.when(
            one_hour_internal_gap,
            (F.col("_previous_temperature") + F.col("_next_temperature")) / F.lit(2.0),
        )
        .otherwise(F.col("temperature_c"))
        .alias("temperature_c"),
        F.when(
            one_hour_internal_gap,
            (F.col("_previous_wind") + F.col("_next_wind")) / F.lit(2.0),
        )
        .otherwise(F.col("wind_speed_mps"))
        .alias("wind_speed_mps"),
        F.col("precipitation_mm"),
        F.when(F.col("_complete_weather"), F.lit("observed"))
        .when(one_hour_internal_gap, F.lit("interpolated_temp_wind"))
        .when(F.col("weather_payload_sha256").isNull(), F.lit("missing_weather"))
        .otherwise(F.lit("partial_source_data"))
        .alias("weather_handling"),
        (~F.col("_complete_weather")).alias("weather_missing"),
        F.when(F.col("weather_payload_sha256").isNull(), F.lit("missing_weather"))
        .otherwise(F.lit("matched"))
        .alias("join_status"),
        "price_payload_sha256",
        "weather_payload_sha256",
        F.lit(utc_now()).cast("timestamp").alias("processed_at"),
        F.lit(RUN_ID).alias("run_id"),
    )
)

merge_dataframe(fact_stage, HOURLY_FACT, ["zone", "hour_start_utc"])

print(
    f"Silver complete for {RUN_DATE}: weather={weather_valid.count()} rows, "
    f"price={price_valid.count()} rows, hourly fact={fact_stage.count()} rows."
)
