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
# MAGIC ### Notebook Start - Create Silver & Gold Table

# COMMAND ----------

# Imports
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# Import Custom Modules from the folder "helpers" in the Repo
from helpers.get_rules import get_rules

# Reads the "iot_events_bronze" table and writes "iot_events_silver" table
@dlt.create_table(name="iot_events_silver", comment="data cleaned")
@dlt.expect_or_drop("valid_id", "id IS NOT NULL AND id > 5") 
def iot_events_silver():
    return (
        dlt.read("iot_events_bronze").select(       # The select method is used to choose specific columns from the DataFrame
            F.col("id").cast("int"),                # Cast the "id" column to integer type
            "nestedColumn",                         # Select the "nestedColumn"
            "country",                              # Select the "country" column
            "event_ts"                              # Select the "event_ts" timestamp column
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

# Delta Live Tables definition of the Gold table
@dlt.create_table(name="iot_events_gold", comment="data adapted for use case")

# Each line represents one single constraint that is evaluated
@dlt.expect_or_drop("valid_country", "country IS NOT NULL")
@dlt.expect_or_drop("drop_if_mexico","country != 'MX'") 

def iot_events_gold():
    return (
        #dlt.read_stream("iot_events_silver")
        dlt.read("iot_events_silver")
        .withColumn(
            "exp_id", F.exp(F.col("id"))
        )
        .select(
            "id",
            "exp_id",
            "nestedColumn", 
            "country", 
            "event_ts"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Code for Round 2
# MAGIC
# MAGIC Showing how you can work with Data Quality rules (known as Expectations in Delta Live Tables)
# MAGIC
# MAGIC Links on how to work with DLT using python
# MAGIC - https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/tutorial-python
# MAGIC - https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations
# MAGIC - https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/python-ref
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Creation

# COMMAND ----------

# # Import Custom Modules from the folder "helpers" in the Repo
# from helpers.get_rules import get_rules

# # Reads the "iot_events_bronze" table and writes "iot_events_silver" table
# @dlt.create_table(name="iot_events_silver", comment="data cleaned")
# #@dlt.expect_or_drop("valid_id", "id IS NOT NULL AND id > 5") 
# @dlt.expect_all_or_drop(get_rules(spark,'iot_events_silver')) #get the rules from the UC table. 
# def iot_events_silver():
#     return (
#         dlt.read("iot_events_bronze").select(       # The select method is used to choose specific columns from the DataFrame
#             F.col("id").cast("int"),                # Cast the "id" column to integer type
#             "nestedColumn",                         # Select the "nestedColumn"
#             "country",                              # Select the "country" column
#             "event_ts"                              # Select the "event_ts" timestamp column
#         )
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Creation

# COMMAND ----------

# # Delta Live Tables definition of the Gold table
# @dlt.create_table(name="iot_events_gold", comment="data cleaned")

# # Each line represents one single constraint that is evaluated
#@dlt.expect_or_drop("valid_country", "country IS NOT NULL")
#@dlt.expect_or_drop("drop_if_mexico","country != 'MX'") 

# # This one-liner represents two (2) constraint in one single expression which is evaluated
#@dlt.expect_or_drop("valid_country", "country IS NOT NULL and country != 'MX'")

# # This one-liner represents two (2) constraint in two (2) expressions which are evaluated together with "expect_all_or_drop"
# @dlt.expect_all_or_drop({
#     "valid_country": "country IS NOT NULL", 
#     "drop_if_mexico": "country != 'MX'"
#     })

#@dlt.expect_all_or_drop(get_rules(spark, 'iot_events_gold')) Replaced by lines above

# def iot_events_gold():
#     return (
#         #dlt.read_stream("iot_events_silver")
#         dlt.read("iot_events_silver")
#         .withColumn(
#             "exp_id", F.exp(F.col("id"))
#         )
#         .select(
#             "id",
#             "exp_id",
#             "nestedColumn", 
#             "country", 
#             "event_ts"
#         )
#     )
