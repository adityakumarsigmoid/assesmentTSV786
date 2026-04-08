# Databricks notebook source
# MAGIC %md
# MAGIC ## 2.1 Read CSV Files and Display Schemas

# COMMAND ----------

df_employees = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/raw/employees.csv")
df_employees.printSchema()

# COMMAND ----------

df_salaries = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/raw/salaries.csv")
df_salaries.printSchema()

# COMMAND ----------

df_departments = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/raw/departments.csv")
df_departments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Add Metadata Columns and Write Bronze Tables

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit, col

# COMMAND ----------

df_emp_bronze = df_employees \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("employees.csv"))

df_emp_bronze.write.format("delta").mode("overwrite").saveAsTable("hr_catalog_adi786.bronze.employees")
print("bronze.employees written")

# COMMAND ----------

df_sal_bronze = df_salaries \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("salaries.csv"))

df_sal_bronze.write.format("delta").mode("overwrite").saveAsTable("hr_catalog_adi786.bronze.salaries")
print("bronze.salaries written")

# COMMAND ----------

df_dept_bronze = df_departments \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("departments.csv"))

df_dept_bronze.write.format("delta").mode("overwrite").saveAsTable("hr_catalog_adi786.bronze.departments")
print("bronze.departments written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Add CHECK Constraint on employment_type

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hr_catalog_adi786.bronze.employees
# MAGIC ADD CONSTRAINT valid_employment_type
# MAGIC CHECK (employment_type IN ('full_time', 'part_time', 'contract'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Verify Delta Log with DESCRIBE HISTORY

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY hr_catalog_adi786.bronze.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Time Travel — Query Version 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_catalog_adi786.bronze.employees VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Auto Loader — Ingest Employee Updates
# MAGIC
# MAGIC Reads new CSV files from the landing/employees/ volume folder using Auto Loader.

# COMMAND ----------

df_emp_updates = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/bronze_emp_updates/_schema")
    .load("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/landing/employees/")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df_emp_updates.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/hr_catalog_adi786/bronze/raw_landing_data/checkpoints/bronze_emp_updates")
    .trigger(availableNow=True)
    .toTable("hr_catalog_adi786.bronze.employees_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_catalog_adi786.bronze.employees_updates;

# COMMAND ----------

print("Task 2 complete. Bronze layer is ready.")
