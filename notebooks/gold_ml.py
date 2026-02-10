from pyspark.sql.functions import (
    col,
    avg,
    count,
    stddev,
    sum
)

# =========================================================
# Config
# =========================================================
silver_table = "taxi_dev.silver.trips"
gold_ml_table = "taxi_dev.gold_ml.vendor_features"

# =========================================================
# Read Silver
# =========================================================
df = spark.table(silver_table)

# =========================================================
# Build ML features
# =========================================================
df_features = (
    df.groupBy("VendorID")
    .agg(
        count("*").alias("trip_count"),
        avg("trip_distance").alias("avg_trip_distance"),
        stddev("trip_distance").alias("std_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        sum("total_amount").alias("total_revenue")
    )
)

# =========================================================
# Write Gold ML
# =========================================================
(
    df_features.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_ml_table)
)

print("Gold ML feature table created successfully.")
