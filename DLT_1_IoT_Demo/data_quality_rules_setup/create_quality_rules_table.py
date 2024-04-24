# Databricks notebook source
# Store our rules as a delta table for more flexibility & reusability. 

table_name = "capgemini.mafi_demo_schema.data_quality_rules"

data = [
 # tag/table name      name              constraint
 ("iot_events_silver",  "valid_id",       "id IS NOT NULL AND id > 0"),
 ("iot_events_gold",    "valid_country",    "country IS NOT NULL")
]
#Typically only run once, this doesn't have to be part of the DLT pipeline.
df_quality_rules = spark.createDataFrame(data=data, schema=["tag", "name", "constraint"])

# Check if table exists
if spark.catalog.tableExists(table_name):
    print(f"Table {table_name} exists.")
else:
    df_quality_rules.write.saveAsTable(table_name)

# COMMAND ----------

df = spark.read.table(table_name)
df.display()

# COMMAND ----------


