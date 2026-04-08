# Databricks notebook source
# MAGIC %md
# MAGIC ## 7.2 Verify All Delta Tables Are Registered

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN hr_catalog_adi786.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN hr_catalog_adi786.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN hr_catalog_adi786.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 Column Masking — PII Protection on full_name and email
# MAGIC
# MAGIC Users without `pii_hr_access` group membership will see NULL for PII columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hr_catalog_adi786.silver.mask_pii_string(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('pii_hr_access') THEN val
# MAGIC     ELSE NULL
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hr_catalog_adi786.silver.employees
# MAGIC ALTER COLUMN full_name SET MASK hr_catalog_adi786.silver.mask_pii_string;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hr_catalog_adi786.silver.employees
# MAGIC ALTER COLUMN email SET MASK hr_catalog_adi786.silver.mask_pii_string;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 Row-Level Security — Department Managers See Only Their Department

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION hr_catalog_adi786.silver.dept_row_filter(dept_id STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('hr_admin') THEN true
# MAGIC     ELSE dept_id = current_user()
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hr_catalog_adi786.silver.employees_enriched
# MAGIC SET ROW FILTER hr_catalog_adi786.silver.dept_row_filter ON (department_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 Grant SELECT on Gold View to hr_analytics Group
# MAGIC
# MAGIC **Pre-requisite:** Create the `hr_analytics` and `pii_hr_access` groups in Account Console → Groups if they don't exist.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON VIEW hr_catalog_adi786.gold.dept_payroll_summary TO `hr_analytics`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON VIEW hr_catalog_adi786.gold.dept_payroll_summary;

# COMMAND ----------

print("Task 7 complete. Unity Catalog governance is configured.")
