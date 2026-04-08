# Databricks notebook source
from pyspark.sql.functions import col, datediff, current_date, round, row_number, lit, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Read Bronze Employees and Cast hire_date, Compute tenure_years

# COMMAND ----------

df_emp = spark.table("hr_catalog_adi786.bronze.employees")

df_emp = df_emp.withColumn("hire_date", col("hire_date").cast(DateType()))
df_emp = df_emp.withColumn("tenure_years", round(datediff(current_date(), col("hire_date")) / 365.25, 1))

display(df_emp.select("employee_id", "full_name", "hire_date", "tenure_years"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Join with Salaries — Get Latest Salary per Employee (Window Function)

# COMMAND ----------

df_sal = spark.table("hr_catalog_adi786.bronze.salaries")
df_sal = df_sal.withColumn("effective_date", col("effective_date").cast(DateType()))

window_spec = Window.partitionBy("employee_id").orderBy(col("effective_date").desc())
df_sal_latest = df_sal.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

df_emp_sal = df_emp.join(df_sal_latest, on="employee_id", how="inner")
display(df_emp_sal.select("employee_id", "full_name", "base_salary", "bonus_pct", "effective_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Compute total_monthly_compensation

# COMMAND ----------

df_emp_sal = df_emp_sal.withColumn(
    "total_monthly_compensation",
    round(col("base_salary") + (col("base_salary") * col("bonus_pct")), 2)
)

display(df_emp_sal.select("employee_id", "base_salary", "bonus_pct", "total_monthly_compensation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Join with Departments — Add department_name and location

# COMMAND ----------

df_dept = spark.table("hr_catalog_adi786.bronze.departments")

df_enriched = df_emp_sal.join(df_dept, on="department_id", how="left")

display(df_enriched.select("employee_id", "full_name", "department_name", "location", "total_monthly_compensation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Write silver.employees_enriched

# COMMAND ----------

df_enriched.drop("_ingested_at", "_source_file", "salary_id", "currency").write.format("delta").mode("overwrite").saveAsTable("hr_catalog_adi786.silver.employees_enriched")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 SCD Type 2 on silver.employees — Track job_title Changes
# MAGIC
# MAGIC We create the SCD table first with the current snapshot, then merge updates.

# COMMAND ----------

df_scd_initial = (
    spark.table("hr_catalog_adi786.bronze.employees")
    .select("employee_id", "full_name", "email", "department_id", "job_title", "hire_date", "employment_type", "manager_id")
    .withColumn("is_current", lit(True))
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date", lit(None).cast("date"))
)

df_scd_initial.write.format("delta").mode("overwrite").saveAsTable("hr_catalog_adi786.silver.employees")
print("silver.employees SCD2 base table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply SCD Type 2 MERGE for job_title changes from employee updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Expire existing current rows where job_title has changed
# MAGIC MERGE INTO hr_catalog_adi786.silver.employees AS target
# MAGIC USING hr_catalog_adi786.bronze.employees_updates AS source
# MAGIC ON target.employee_id = source.employee_id AND target.is_current = true
# MAGIC WHEN MATCHED AND target.job_title <> source.job_title THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.effective_end_date = current_date();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Insert new current rows for changed employees
# MAGIC INSERT INTO hr_catalog_adi786.silver.employees
# MAGIC SELECT
# MAGIC   s.employee_id,
# MAGIC   s.full_name,
# MAGIC   s.email,
# MAGIC   s.department_id,
# MAGIC   s.job_title,
# MAGIC   s.hire_date,
# MAGIC   s.employment_type,
# MAGIC   s.manager_id,
# MAGIC   true AS is_current,
# MAGIC   current_date() AS effective_start_date,
# MAGIC   NULL AS effective_end_date
# MAGIC FROM hr_catalog_adi786.bronze.employees_updates s
# MAGIC WHERE s.employee_id IN (
# MAGIC   SELECT employee_id FROM hr_catalog_adi786.silver.employees
# MAGIC   WHERE is_current = false AND effective_end_date = current_date()
# MAGIC )
# MAGIC OR s.employee_id NOT IN (
# MAGIC   SELECT employee_id FROM hr_catalog_adi786.silver.employees
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_catalog_adi786.silver.employees
# MAGIC ORDER BY employee_id, effective_start_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Enable Change Data Feed on silver.employees_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hr_catalog_adi786.silver.employees_enriched SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED hr_catalog_adi786.silver.employees_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.8 OPTIMIZE with ZORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE hr_catalog_adi786.silver.employees_enriched ZORDER BY (department_id, hire_date);

# COMMAND ----------

print("Task 3 complete. Silver layer is ready.")
