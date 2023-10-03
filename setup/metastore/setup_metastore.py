# Databricks notebook source
# %sql DROP SCHEMA IF EXISTS source2_1 CASCADE;

# COMMAND ----------

# setup widgets
dbutils.widgets.text("domain", '')

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql USE CATALOG hive_metastore

# COMMAND ----------

domain = dbutils.widgets.get("domain")

# COMMAND ----------

storage_account_name = "canadaehstorage"
container = "cjc"
company = "company1"
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc" # Microsoft Entra ID
adls_path = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net"
domain_path = f'{adls_path}/{company}/{domain}'

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

def create_external_tables(domain_path, source, db):
    tables = get_last_dir(f"{domain_path}/{source}")
    for table in tables:
        table_script = f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING DELTA
        LOCATION '{domain_path}/{source}/{table}'
        """
        spark.sql(table_script)

# COMMAND ----------

sources = get_last_dir(f"{adls_path}/{company}/{domain}")
for source in sources:
    dbname = spark.sql(f"show schemas in hive_metastore like '{source}'")
    if dbname.count() == 0:
        db = source
        print("empty")
    else:
        db = f"{source}_1"
        print("not empty")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    create_external_tables(domain_path, source, db)
