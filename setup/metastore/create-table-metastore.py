# Databricks notebook source
dbutils.widgets.text("adlspath",'abfs://')

# COMMAND ----------

df_database_name = dbutils.fs.ls(dbutils.widgets.get("adlspath"))

# COMMAND ----------

dbutils.widgets.text("catalog",'aterado')

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("database",'default')

# COMMAND ----------

database_name = dbutils.widgets.get("database")

# COMMAND ----------

display(df_database_name)

# COMMAND ----------

# new code

for i in df_database_name:
    #spark.sql("CREATE CATALOG IF NOT EXISTS " + catalog_name + "; USE CATALOG "+ catalog_name +";")  #uncomment this line to create catalog for UC enabled workspaces
    spark.sql("USE CATALOG "+ catalog_name +";")
    df_table_name = i[0].split("/")[3]
    
    print("CREATE DATABASE IF NOT EXISTS " + database_name + ";")
    spark.sql("CREATE DATABASE IF NOT EXISTS " + database_name + ";")
    print("CREATE TABLE IF NOT EXISTS " + database_name + "." + df_table_name + " USING delta LOCATION '" + i[0] + "'")
    spark.sql("CREATE TABLE IF NOT EXISTS " + database_name + "." + df_table_name + " USING delta LOCATION '" + i[0] + "'")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE aterado.default.`CustomerDim` using delta LOCATION 'abfss://ateradoext@ggwstdlrs.dfs.core.windows.net/customer/';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table aterado.default.customerdim

# COMMAND ----------

#%sql
#DROP table aterado.default.trips;
#drop table aterado.default.customer;


# COMMAND ----------


