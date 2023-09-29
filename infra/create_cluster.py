import os
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

latest = w.clusters.select_spark_version(latest=True)

cluster_name = f'cjc-sdk-{time.time_ns()}'

clstr = w.clusters.create(cluster_name=cluster_name,
                          spark_version=latest,
                          autotermination_minutes=40,
                          num_workers=1).result()

# cleanup
#w.clusters.permanent_delete(cluster_id=clstr.cluster_id)