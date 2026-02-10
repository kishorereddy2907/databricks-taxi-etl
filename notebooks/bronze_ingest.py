from pyspark.sql.functions import (
    regexp_extract,
    current_timestamp,
    col
)

# =========================================================
# Config
# =========================================================
raw_path = "/Volumes/taxi_dev/raw/taxi_files/"
bronze_table = "taxi_dev.bronze.trips"

# Control shuffle to avoid RESOURCE_EXHAUSTED
spark.conf.set("spark.sql.shuffle.partitions", "200")

# =========================================================
# Read Parquet files (Unity Catalog compliant)
# =========================================================
df = (
    spark.read
    .format("parquet")
    .load(raw_path)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("year", regexp_extract(col("source_file"), r"_(\d{4})-", 1))
    .withColumn("month", regexp_extract(col("source_file"), r"-(\d{2})", 1))
    .withColumn("ingestion_ts", current_timestamp())
)

# =========================================================
# (TEMP SAFETY) First run only – uncomment if needed
# =========================================================
# df = df.filter((col("year") == "2025") & (col("month") == "01"))

# =========================================================
# Incremental logic (month-wise)
# =========================================================
if spark.catalog.tableExists(bronze_table):
    print("Bronze table exists. Applying incremental filter.")

    existing_months = (
        spark.table(bronze_table)
        .select("year", "month")
        .distinct()
    )

    df_new = (
        df.alias("src")
        .join(
            existing_months.alias("tgt"),
            on=["year", "month"],
            how="left_anti"
        )
    )
else:
    print("Bronze table does not exist. Initial full load.")
    df_new = df

# =========================================================
# Repartition before write (critical)
# =========================================================
df_new = df_new.repartition(50, "year", "month")

# =========================================================
# Write to Bronze Delta table
# =========================================================
(
    df_new.write
    .format("delta")
    .mode("append")
    .partitionBy("year", "month")
    .option("mergeSchema", "true")
    .saveAsTable(bronze_table)
)

print("Bronze ingestion completed successfully.")
