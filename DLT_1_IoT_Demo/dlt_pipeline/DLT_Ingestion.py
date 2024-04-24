# Databricks notebook source
# DBTITLE 1,Enabling Import of Python modules from Databricks repos
import sys
import os

# Get the root path for the DLT Pipeline dynamically for all users by travelling one level up
repos = []
for p in sys.path:
    if "Repos" in p:
        repos.append(p)

repos.sort(reverse=True)
dlt_pipeline_root_path = "/".join(repos[0].split("/")[:-1]) + "/"

# Set current working directory to the root level to enable modules stored as python files
sys.path.append(os.path.abspath(dlt_pipeline_root_path))
print(dlt_pipeline_root_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Start - Create Bronze Table

# COMMAND ----------

# To use Delta Live Tables (DLT) we need to import the dlt library
import dlt

# Setting the path to where we generated our data
jsons_path_data = "dbfs:/landing/raw_jsons/"
jsons_path_checkpoint = "dbfs:/landing/checkpoint/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader with schema inference

# COMMAND ----------

##### DLT VERSION ##### Create a Table

# Create a table from autolader
@dlt.table(comment="Bronze iot data") 
def iot_events_bronze():
  return (
    spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", volume_path+'/inferred_schema')
        .option("cloudFiles.inferColumnTypes", "true")
        .load(jsons_path_data))
