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
directory_path = f'{adls_path}/{company_name}/{domain_name}'

# COMMAND ----------

# Set up azure key valut backed secret scope before hand
storage_account_key = dbutils.secrets.get("rgscope", "canada-eh-storage-account-key")

# Configure the storage account access key
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_account_key
)

# COMMAND ----------

# List the files in the container
dbutils.fs.ls(f"{adls_path}/{company_name}/{domain_name}")

# COMMAND ----------

# spark.sql(f"CREATE DATABASE IF NOT EXISTS {domain_name}")

# COMMAND ----------

file_info = dbutils.fs.ls(f"{adls_path}/{company_name}/{domain_name}")
last_dirs = []
for file in file_info:
    path = file.path
    last_dir = path.split('/')[-2]
    last_dirs.append(last_dir)

print(last_dirs)

# COMMAND ----------



# COMMAND ----------

def create_external_tables(adls_path, database_name):
    for t in tables:
        table_name = t.split("/")[-1]
        table_script = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING DELTA
        LOCATION '{adls_path}/{t}'
        """
        spark.sql(table_script)
        # spark.sql("CREATE TABLE IF NOT EXISTS " + database_name + "." + table_name + " USING DELTA LOCATION '" + adls_path/t + "'")
        # spark.sql(f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} USING DELTA LOCATION {adls_path} + "/" + {t}")

# COMMAND ----------

create_external_tables(adls_path=adls_path, database_name=domain_name)

# COMMAND ----------


