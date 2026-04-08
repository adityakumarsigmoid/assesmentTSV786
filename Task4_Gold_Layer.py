# Databricks notebook source
# MAGIC %md
# MAGIC ## 4.1 Create gold.dept_payroll_summary View
# MAGIC
# MAGIC Aggregates total_monthly_compensation by department_name and location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hr_catalog_adi786.gold.dept_payroll_summary AS
# MAGIC SELECT
# MAGIC   department_name,
# MAGIC   location,
# MAGIC   COUNT(employee_id) AS employee_count,
# MAGIC   SUM(total_monthly_compensation) AS total_payroll,
# MAGIC   ROUND(AVG(total_monthly_compensation), 2) AS avg_compensation
# MAGIC FROM hr_catalog_adi786.silver.employees_enriched
# MAGIC GROUP BY department_name, location;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hr_catalog_adi786.gold.dept_payroll_summary ORDER BY total_payroll DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Create gold.headcount_vs_budget View
# MAGIC
# MAGIC Joins departments with active employee count and compares against head_count_budget.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hr_catalog_adi786.gold.headcount_vs_budget AS
# MAGIC SELECT
# MAGIC   d.department_id,
# MAGIC   d.department_name,
# MAGIC   d.location,
# MAGIC   d.head_count_budget,
# MAGIC   COUNT(e.employee_id) AS actual_headcount,
# MAGIC   COUNT(e.employee_id) - d.head_count_budget AS over_under_budget
# MAGIC FROM hr_catalog_adi786.bronze.departments d
# MAGIC LEFT JOIN hr_catalog_adi786.silver.employees_enriched e
# MAGIC   ON d.department_id = e.department_id
# MAGIC GROUP BY d.department_id, d.department_name, d.location, d.head_count_budget;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC   CASE WHEN over_under_budget > 0 THEN 'OVER BUDGET' ELSE 'WITHIN BUDGET' END AS budget_status
# MAGIC FROM hr_catalog_adi786.gold.headcount_vs_budget
# MAGIC ORDER BY over_under_budget DESC;

# COMMAND ----------

print("Task 4 complete. Gold views created. Build the dashboard manually in SQL UI.")
