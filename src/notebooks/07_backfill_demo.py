# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 — Optional small backfill for a stronger interview visualization
# MAGIC
# MAGIC A single day proves the pipeline but may populate only one or two temperature
# MAGIC bands. This driver reuses the same idempotent notebooks for a small historical
# MAGIC window (default 30 days); it is intentionally not scheduled in the daily job.

# COMMAND ----------

end_date_value = _widget("end_date", RUN_DATE)
lookback_days = int(_widget("lookback_days", "30"))
END_DATE = date.fromisoformat(end_date_value)

if not 1 <= lookback_days <= 60:
    raise ValueError("For Free Edition demo safety, lookback_days must be between 1 and 60")

common_parameters = {
    "catalog": CATALOG,
    "zone": ZONE,
    "latitude": str(LATITUDE),
    "longitude": str(LONGITUDE),
    "ingestion_mode": INGESTION_MODE,
}

# Set up once; after that, each daily slice follows exactly the same production-shaped path.
dbutils.notebook.run("./00_setup", 0, {**common_parameters, "run_date": RUN_DATE, "run_id": RUN_ID})  # noqa: F821

for offset in range(lookback_days - 1, -1, -1):
    run_date = END_DATE - timedelta(days=offset)
    child_run_id = f"{RUN_ID}-{run_date.isoformat()}"
    daily_parameters = {
        **common_parameters,
        "run_date": run_date.isoformat(),
        "run_id": child_run_id,
    }
    print(f"Backfilling {run_date.isoformat()} ({lookback_days - offset}/{lookback_days})")
    dbutils.notebook.run("./01_ingest_weather", 0, daily_parameters)  # noqa: F821
    dbutils.notebook.run("./02_ingest_prices", 0, daily_parameters)  # noqa: F821
    dbutils.notebook.run("./03_silver_transform", 0, daily_parameters)  # noqa: F821
    dbutils.notebook.run("./05_quality", 0, daily_parameters)  # noqa: F821

# Build Gold once from the complete conformed history. Run 06_visualize.py interactively
# afterwards so the chart remains visible in the notebook UI.
dbutils.notebook.run(  # noqa: F821
    "./04_gold",
    0,
    {**common_parameters, "run_date": END_DATE.isoformat(), "run_id": RUN_ID},
)
print("Backfill complete. Open 06_visualize.py to render the Gold chart.")
