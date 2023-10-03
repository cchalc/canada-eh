# Databricks notebook source
# %sql DROP SCHEMA IF EXISTS domain1 CASCADE;

# COMMAND ----------

# setup widgets
dbutils.widgets.text("domain_name", '')

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %pip install azure-storage-file-datalake azure-identity

# COMMAND ----------

dbutils.library.restartPython()

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

# Try with ADLS client
import os
import random
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential

# COMMAND ----------

def get_service_client_account_key(account_name, account_key) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=account_key)

    return service_client

# COMMAND ----------

# Get datalake service client
datalake_service_client = get_service_client_account_key(
    account_name=storage_account_name, account_key=storage_account_key
)
file_system_client = datalake_service_client.get_file_system_client(container_name)
directory_client = datalake_service_client.get_directory_client(file_system_client.file_system_name, domain_name)

# COMMAND ----------

# Get container names
file_systems = datalake_service_client.list_file_systems()
for file_system in file_systems:
    print(file_system.name)

# COMMAND ----------

# find all sources
directory_name = f"{company_name}/{domain_name}"
source_paths = []
paths = file_system_client.get_paths(path=directory_name)
for path in paths:
    if path.is_directory and "_delta_log" not in path.name:
        source_paths.append(path.name)
        unique_sources = list(set(filter(lambda x: x.count('/') == 2, source_paths)))
        sources = [s.split('/')[-1] for s in unique_sources]

# COMMAND ----------

file_info = dbutils.fs.ls(f"{adls_path}/{company_name}/{domain_name}")
last_dirs = []
for file in file_info:
    path = file.path
    last_dir = path.split('/')[-2]
    last_dirs.append(last_dir)

print(last_dirs)

# COMMAND ----------

# Find all tables
directory_name = f"{company_name}/{domain_name}"
tables = []
paths = file_system_client.get_paths(path=directory_name)
for path in paths:
    if path.is_directory and "_delta_log" not in path.name:
        tables.append(path.name)

# COMMAND ----------

print(tables)

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

# MAGIC %md cluster policy

# COMMAND ----------

from databricks_client import DatabricksAPI

# Configure your Databricks workspace and credentials
host = "YOUR_WORKSPACE_HOST"
token = "YOUR_ACCESS_TOKEN"
api = DatabricksAPI(host=host, token=token)

# Define the Single Node cluster policy JSON payload
single_node_policy = {
  "name": "Single Node Policy",
  "description": "A Single Node cluster policy",
  "spark_version": "7.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 0,
  "autoscale": {
    "min_workers": 0,
    "max_workers": 0
  }
}

# Create the Single Node cluster policy
api.cluster_policies.create(single_node_policy)
