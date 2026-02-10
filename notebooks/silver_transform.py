from pyspark.sql.functions import col, when, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# =========================================================
# Config
# =========================================================
bronze_table = "taxi_dev.bronze.trips"
silver_table = "taxi_dev.silver.trips"
reject_table = "taxi_dev.silver_rejects.trips"

# =========================================================
# Read Bronze
# =========================================================
df = spark.table(bronze_table)

# =========================================================
# Add rejection reason
# =========================================================
df_with_reason = (
    df.withColumn(
        "reject_reason",
        when(col("tpep_pickup_datetime").isNull(), lit("pickup_datetime_null"))
        .when(col("tpep_dropoff_datetime").isNull(), lit("dropoff_datetime_null"))
        .when(col("trip_distance") <= 0, lit("invalid_trip_distance"))
        .when(col("fare_amount") <= 0, lit("invalid_fare_amount"))
        .when(col("total_amount") <= 0, lit("invalid_total_amount"))
        .when(col("passenger_count") <= 0, lit("invalid_passenger_count"))
        .otherwise(lit(None))
    )
)

# =========================================================
# Split valid vs rejected
# =========================================================
df_valid = df_with_reason.filter(col("reject_reason").isNull())
df_reject = df_with_reason.filter(col("reject_reason").isNotNull())

# =========================================================
# Deduplicate ONLY valid records
# =========================================================
window_spec = Window.partitionBy(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
).orderBy(col("ingestion_ts").desc())

df_valid_dedup = (
    df_valid
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn", "reject_reason")
)

# =========================================================
# Write Silver (clean)
# =========================================================
(
    df_valid_dedup.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# =========================================================
# Write Silver Rejects (append for audit)
# =========================================================
(
    df_reject.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(reject_table)
)

print("Silver and Silver Rejects created successfully.")
