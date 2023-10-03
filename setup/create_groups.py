import time
import os

from databricks.sdk import WorkspaceClient

dbhost = os.environ["DATABRICKS_HOST"]
db_token = os.environ["DATABRICKS_TOKEN"]
creds = {"token": db_token}

w = WorkspaceClient()

group = w.groups.create(display_name=f'sdk-{time.time_ns()}')

# cleanup
#w.groups.delete(id=group.id)
