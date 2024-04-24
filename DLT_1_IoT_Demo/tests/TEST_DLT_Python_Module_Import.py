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
# MAGIC ### Notebook Start - Test python modules

# COMMAND ----------

# Import custom python modules from Repo
from helpers.get_rules import get_rules

get_rules(spark, "iot_events_silver")

# COMMAND ----------

# Import custom python modules from Repo
from helpers.power import n_to_mth

# COMMAND ----------

n_to_mth(2,4)

# COMMAND ----------


