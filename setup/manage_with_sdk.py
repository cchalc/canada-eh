# Databricks notebook source
# MAGIC %pip install -U databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient, AccountClient
import time
import os

# COMMAND ----------

w = WorkspaceClient()

for c in w.clusters.list():
  print(c.cluster_name)

# COMMAND ----------

# Let list all cluster policies & extract their names
cluster_policies = w.cluster_policies.list()
[cp.name for cp in cluster_policies]

# COMMAND ----------

# from databricks import groups
# from databricks.exceptions import DatabricksAPIException

# try:
#     client = groups.GroupsServiceClient()
#     res = client.list_groups()
#     for group in res.groups:
#         print(group.display_name)
# except DatabricksAPIException as ex:
#     print(f"Exception: {ex}")
#     # Handle the exception as required

# COMMAND ----------

groups = w.groups.list()
for group in groups:
    print(group.display_name, group.entitlements)

# COMMAND ----------

group = w.groups.create(display_name=f'sdk-{time.time_ns()}')

# COMMAND ----------

# clean up example group
w.groups.delete(id=group.id)

# COMMAND ----------


