from pyspark.sql.functions import (
    col,
    to_date,
    sum,
    count,
    avg
)

# =========================================================
# Config
# =========================================================
silver_table = "taxi_dev.silver.trips"
gold_bi_table = "taxi_dev.gold.daily_trip_metrics"

# =========================================================
# Read Silver
# =========================================================
df = spark.table(silver_table)

# =========================================================
# Derive trip_date
# =========================================================
df_daily = df.withColumn(
    "trip_date",
    to_date(col("tpep_pickup_datetime"))
)

# =========================================================
# Aggregate for BI
# =========================================================
df_agg = (
    df_daily
    .groupBy("trip_date")
    .agg(
        count("*").alias("total_trips"),
        sum("total_amount").alias("total_revenue"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("passenger_count").alias("avg_passenger_count")
    )
)

# =========================================================
# Write Gold BI (overwrite is OK)
# =========================================================
(
    df_agg.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_bi_table)
)

print("Gold BI table created successfully.")
