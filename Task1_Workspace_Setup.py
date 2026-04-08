# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS hr_catalog_adi786.bronze.raw_landing_data;
# MAGIC CREATE SCHEMA IF NOT EXISTS hr_catalog_adi786.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS hr_catalog_adi786.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS hr_catalog_adi786.gold;

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/raw/"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/landing/employees/"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/hr_catalog_adi786/bronze/raw_landing_data/landing/salaries/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Create Secret Scope and Store Simulated Password
# MAGIC
# MAGIC Using the Databricks Secrets REST API to create the scope and store the password directly from the notebook.

# COMMAND ----------

import requests

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

# Create scope
resp1 = requests.post(
    f"https://{workspace_url}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": "hr_scope"}
)
print(f"Create scope: {resp1.status_code} - {resp1.text}")

# Put secret
resp2 = requests.post(
    f"https://{workspace_url}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": "hr_scope", "key": "db_password", "string_value": "SimulatedPassword123"}
)
print(f"Put secret: {resp2.status_code} - {resp2.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Retrieve Databricks Secret

# COMMAND ----------

db_password = dbutils.secrets.get(scope="hr_scope", key="db_password")
print(f"Secret retrieved successfully (length={len(db_password)})")

# COMMAND ----------

print("Task 1 complete. Workspace is ready.")
