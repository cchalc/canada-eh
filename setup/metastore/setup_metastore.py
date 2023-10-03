# Databricks notebook source
# %sql DROP SCHEMA IF EXISTS domain1 CASCADE;

# COMMAND ----------

# setup widgets
dbutils.widgets.text("domain_name", '')

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql USE CATALOG hive_metastore

# COMMAND ----------

domain_name = dbutils.widgets.get("domain_name")

# COMMAND ----------

storage_account_name = "canadaehstorage"
container_name = "cjc"
company_name = "company1"
# domain_name = "domain1" # using widget instead
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc" # Microsoft Entra ID
# database_name = f"{domain_name}"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
domain_path = f'{adls_path}/{company_name}/{domain_name}'

# COMMAND ----------

# Set up azure key valut backed secret scope before hand
storage_account_key = dbutils.secrets.get("rgscope", "canada-eh-storage-account-key")

# Configure the storage account access key
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_account_key
)

# COMMAND ----------

def get_last_dir(file_path):
    file_info = dbutils.fs.ls(file_path)
    dirs = []
    for file in file_info:
        path = file.path
        last_dir = path.split('/')[-2]
        dirs.append(last_dir)
    return dirs

# COMMAND ----------

def create_external_tables(domain_path, source_name):
    tables = get_last_dir(f"{domain_path}/{source_name}")
    for table_name in tables:
        table_script = f"""
        CREATE TABLE IF NOT EXISTS {source_name}.{table_name}
        USING DELTA
        LOCATION '{domain_path}/{source_name}/{table_name}'
        """
        spark.sql(table_script)

# COMMAND ----------

sources = get_last_dir(f"{adls_path}/{company_name}/{domain_name}")
for source_name in sources:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {source_name}")
    create_external_tables(domain_path, source_name)

# COMMAND ----------


