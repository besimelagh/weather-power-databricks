# Databricks notebook source
# MAGIC %run ./_shared

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Visualize Gold output
# MAGIC The chart is deliberately based on a Gold table, never a raw HTTP response.

# COMMAND ----------

import matplotlib.pyplot as plt

PRICE_BY_TEMPERATURE_BAND = table(GOLD_SCHEMA, "price_by_temperature_band")
chart_data = (
    spark.table(PRICE_BY_TEMPERATURE_BAND)
    .where(F.col("zone") == ZONE)
    .orderBy("temperature_band_order")
)
display(chart_data)

pdf = chart_data.select(
    "temperature_band", "avg_nok_per_kwh", "observation_count"
).toPandas()

if pdf.empty:
    raise RuntimeError("No Gold rows found. Run 04_gold.py after a passing DQ gate.")

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(pdf["temperature_band"], pdf["avg_nok_per_kwh"], color="#1f77b4")
ax.set_title("Gjennomsnittlig strømpris per temperaturintervall (NO1)")
ax.set_xlabel("Temperaturintervall")
ax.set_ylabel("NOK per kWh")
ax.tick_params(axis="x", rotation=25)
for bar, observations in zip(bars, pdf["observation_count"]):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height(),
        f"n={observations}",
        ha="center",
        va="bottom",
        fontsize=9,
    )
fig.tight_layout()
plt.show()

# In the interview, state explicitly that this is descriptive correlation, not
# causal inference: price is also affected by time of day, season, capacity,
# hydrology and neighbouring power markets.
