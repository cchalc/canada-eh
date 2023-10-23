-- Databricks notebook source
-- MAGIC %md Manage access to sescurable objects within the workspace
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/security-grant#syntax
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-privileges#securable-objects

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # initialize variables
-- MAGIC storage_account_name = "canadaehstorage"
-- MAGIC container = "cjc"
-- MAGIC tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc" # Microsoft Entra ID
-- MAGIC adls_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set up azure key valut backed secret scope before hand
-- MAGIC storage_account_key = dbutils.secrets.get("rgscope", "canada-eh-storage-account-key")
-- MAGIC
-- MAGIC # Configure the storage account access key
-- MAGIC spark.conf.set(
-- MAGIC   f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
-- MAGIC   storage_account_key
-- MAGIC )

-- COMMAND ----------

use catalog hive_metastore

-- COMMAND ----------

show schemas in hive_metastore

-- COMMAND ----------

show tables in source1

-- COMMAND ----------

show grants on schema source1

-- COMMAND ----------

show groups

-- COMMAND ----------

describe extended source1.table1

-- COMMAND ----------

GRANT USAGE, SELECT ON SCHEMA `source1` TO GROUP Data_Scientists
