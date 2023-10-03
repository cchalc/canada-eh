CREATE DATABASE IF NOT EXISTS my_database
LOCATION "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/all_databases/my_database";
USE my_database;
%python 
df.write.saveAsTable("my_table") 
