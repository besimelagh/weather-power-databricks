# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Build Gold analyses
# MAGIC Gold is intentionally rebuilt from conforming Silver data.  It is deterministic,
# MAGIC cheap at this scale, and avoids incremental aggregation drift on reruns.

# COMMAND ----------

PRICE_BY_TEMPERATURE_BAND = table(GOLD_SCHEMA, "price_by_temperature_band")
DAILY_WEATHER_PRICE = table(GOLD_SCHEMA, "daily_weather_price")

fact = spark.table(HOURLY_FACT)

# Only a present temperature is valid for a temperature-band analysis. The daily
# table below keeps a coverage ratio so this exclusion cannot be mistaken for 100% data.
analysis_base = fact.where(
    F.col("nok_per_kwh").isNotNull() & F.col("temperature_c").isNotNull()
)

banded = (
    analysis_base.withColumn(
        "temperature_band",
        F.when(F.col("temperature_c") < -10, "< -10 °C")
        .when(F.col("temperature_c") < -5, "-10 to < -5 °C")
        .when(F.col("temperature_c") < 0, "-5 to < 0 °C")
        .when(F.col("temperature_c") < 5, "0 to < 5 °C")
        .when(F.col("temperature_c") < 10, "5 to < 10 °C")
        .otherwise("≥ 10 °C"),
    )
    .withColumn(
        "temperature_band_order",
        F.when(F.col("temperature_c") < -10, 1)
        .when(F.col("temperature_c") < -5, 2)
        .when(F.col("temperature_c") < 0, 3)
        .when(F.col("temperature_c") < 5, 4)
        .when(F.col("temperature_c") < 10, 5)
        .otherwise(6),
    )
)

temperature_summary = (
    banded.groupBy("zone", "temperature_band", "temperature_band_order")
    .agg(
        F.count("*").alias("observation_count"),
        F.round(F.avg("nok_per_kwh"), 4).alias("avg_nok_per_kwh"),
        F.round(F.expr("percentile_approx(nok_per_kwh, 0.5)"), 4).alias(
            "median_nok_per_kwh"
        ),
        F.round(F.min("nok_per_kwh"), 4).alias("min_nok_per_kwh"),
        F.round(F.max("nok_per_kwh"), 4).alias("max_nok_per_kwh"),
        F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
        F.max("processed_at").alias("refreshed_at"),
    )
)

temperature_summary.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(PRICE_BY_TEMPERATURE_BAND)

# `business_date` originates from the local price-date request. It therefore makes
# coverage interpretable even on 23/25-hour daylight-saving days.
daily_summary = (
    fact.where(F.col("nok_per_kwh").isNotNull())
    .groupBy("zone", "business_date")
    .agg(
        F.count("*").alias("price_hour_count"),
        F.sum(F.when(F.col("join_status") == "matched", 1).otherwise(0)).alias(
            "matched_weather_hours"
        ),
        F.sum(F.when(F.col("weather_handling") == "observed", 1).otherwise(0)).alias(
            "observed_weather_hours"
        ),
        F.round(F.avg("nok_per_kwh"), 4).alias("avg_nok_per_kwh"),
        F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
        F.round(F.avg("wind_speed_mps"), 2).alias("avg_wind_speed_mps"),
        F.round(F.sum("precipitation_mm"), 2).alias("precipitation_mm_sum"),
        F.max("processed_at").alias("refreshed_at"),
    )
    .withColumn(
        "matched_hour_coverage",
        F.round(F.col("matched_weather_hours") / F.col("price_hour_count"), 4),
    )
)

daily_summary.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(DAILY_WEATHER_PRICE)

print(
    f"Gold complete: {temperature_summary.count()} temperature bands and "
    f"{daily_summary.count()} daily rows."
)
display(spark.table(PRICE_BY_TEMPERATURE_BAND).orderBy("temperature_band_order"))
