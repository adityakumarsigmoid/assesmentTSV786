# Databricks notebook source
# MAGIC %md
# MAGIC ## 5.1 Streaming Source — Auto Loader for New Salary CSVs
# MAGIC
# MAGIC Reads new salary CSV files from the landing/salaries/ volume folder using Auto Loader.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType, DateType

# COMMAND ----------

df_salary_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/gold_salary_stream/_schema")
    .load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/landing/salaries/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Apply Watermark on effective_date (1-day threshold)

# COMMAND ----------

df_salary_stream = (
    df_salary_stream
    .withColumn("effective_date", col("effective_date").cast("timestamp"))
    .withColumn("base_salary", col("base_salary").cast(DoubleType()))
    .withColumn("bonus_pct", col("bonus_pct").cast(DoubleType()))
    .withWatermark("effective_date", "1 day")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Write Stream to gold.live_salary_updates (Append Mode with Checkpoint)

# COMMAND ----------

query = (
    df_salary_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/gold_salary_stream")
    .trigger(availableNow=True)
    .toTable("hr_catalog_adi786.gold.live_salary_updates")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_catalog_adi786.gold.live_salary_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Verify Idempotency
# MAGIC
# MAGIC Restart the stream and confirm no duplicate rows are created.

# COMMAND ----------

query2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/gold_salary_stream/_schema")
    .load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/landing/salaries/")
    .withColumn("effective_date", col("effective_date").cast("timestamp"))
    .withColumn("base_salary", col("base_salary").cast(DoubleType()))
    .withColumn("bonus_pct", col("bonus_pct").cast(DoubleType()))
    .withWatermark("effective_date", "1 day")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/gold_salary_stream")
    .trigger(availableNow=True)
    .toTable("hr_catalog_adi786.gold.live_salary_updates")
)

query2.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows FROM hr_catalog_adi786.gold.live_salary_updates;

# COMMAND ----------

print("Task 5 complete. Streaming pipeline with checkpoint is ready.")
