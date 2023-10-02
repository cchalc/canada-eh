# Databricks notebook source
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
# domain_name = "domain1"
tenant_id = "9f37a392-f0ae-4280-9796-f1864a10effc" # Microsoft Entra ID
database_name = f"{domain_name}"
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
directory_path = f'{adls_path}/{company_name}/{database_name}'

# COMMAND ----------

# Set up azure key valut backed secret scope before hand
storage_account_key = dbutils.secrets.get("rgscope", "canada-eh-storage-account-key")

# COMMAND ----------

# Configure the storage account access key
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  storage_account_key
)

# COMMAND ----------

# # Define ADLS and SQL credentials
# storage_account_name = "<your-storage-account-name>"
# client_id = "<your-service-principal-client-id>"
# client_secret = "<your-service-principal-client-secret>"
# tenant_id = "<your-tenant-id>"
# file_system_name = "<your-file-system-name>"
# directory_path = "<your-directory-path>"
# server_name = "<your-server-name>"
# database_name = "<your-database-name>"
# aad_secret = "<your-aad-secret>"

# # Configure ADLS connection
# configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
#            "fs.adl.oauth2.client.id": f"{client_id}",
#            "fs.adl.oauth2.credential": f"{client_secret}",
#            "fs.adl.oauth2.refresh.url": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
#            "fs.adl.oauth2.access.token.provider": f"org.apache.hadoop.fs.adl.AdlAccessTokenProvider"}

# COMMAND ----------

# cluster configuration for SQL Warehouse
# fs.azure.account.auth.type OAuth
# fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
# fs.azure.account.oauth2.client.id {{secrets/<SCOPE NAME>/<SECRET KEY NAME>}}
# fs.azure.account.oauth2.client.secret {{secrets/<SCOPE NAME>/<SECRET KEY NAME>}}
# fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/<YOUR TENANT ID>/oauth2/token

# COMMAND ----------

# set configs
# spark.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(storage_account_name), "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(storage_account_name), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(storage_account_name), client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(storage_account_name), client_secret )
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(storage_account_name),  "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

# COMMAND ----------

# List the files in the container
# dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/company1/domain1")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {domain_name}")

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

datalake_service_client = get_service_client_account_key(
    account_name=storage_account_name, account_key=storage_account_key
)

# COMMAND ----------

# Get container names
file_systems = datalake_service_client.list_file_systems()
for file_system in file_systems:
    print(file_system.name)

# COMMAND ----------

# Get Clients from DataLakeServiceClient
file_system_client = datalake_service_client.get_file_system_client(container_name)
directory_client = datalake_service_client.get_directory_client(file_system_client.file_system_name, domain_name)

# COMMAND ----------

# Find all tables
directory_name = f"{company_name}/{domain_name}"
tables = []
paths = file_system_client.get_paths(path=directory_name)
for path in paths:
    if path.is_directory and "_delta_log" not in path.name:
        tables.append(path.name)

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


