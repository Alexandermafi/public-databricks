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
# MAGIC ### Notebook Start

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing DLT with Dataframes transformations

# COMMAND ----------

# Set Variables based on the volume location in Unity Catalog
volume_path = "/Volumes/capgemini/mafi_demo_schema/iot_syntehtic_data"
jsons_path_data = f"{volume_path}/data"

# Read the JSON files into a dataframe
df = spark.read.format("json").load(jsons_path_data)

# COMMAND ----------

display(df)

# COMMAND ----------

# Create a table based on the dataframe
df.write.format("delta").mode("overwrite").saveAsTable("capgemini.mafi_demo_schema.test_bronze_ingestion")

# COMMAND ----------

# Create a Dataframe based on a UC table
df_test_bronze = spark.table("capgemini.mafi_demo_schema.test_bronze_ingestion")
display(df_test_bronze)

# COMMAND ----------

# Importing functions from PySpark SQL
from pyspark.sql import functions as F

# Transforming the "df_test_bronze" DataFrame 
# The select method is used to choose specific columns from the DataFrame:
# 1. Casting the "id" column to integer data type.
# 2. Selecting the "nestedColumn", "country", and "event_ts" columns as they are.
df_test_silver = df_test_bronze.select(
    F.col("id").cast("int"),  # Cast the "id" column to integer type
    "nestedColumn",           # Select the "nestedColumn"
    "country",                # Select the "country" column
    "event_ts"                # Select the "event_ts" timestamp column
)

# Displaying the transformed DataFrame (df_test_silver) for visual inspection
# This is commonly used in notebook environments to show the DataFrame in a tabular format
df_test_silver.display()

# COMMAND ----------

# Continuing transformations on the "df_test_silver" DataFrame

# Creating a new DataFrame "df_test_gold" with further transformations:
# 1. A new column "exp_id" is added which is the exponential value of the "id" column.
# 2. Subsequent selection of specific columns.
df_test_gold = (df_test_silver
    .withColumn(
        "exp_id", F.exp(F.col("id"))   # Calculate the exponential of the "id" column and store it in the new "exp_id" column
    )
    .select(
        "id",             # Select the "id" column
        "exp_id",         # Select the newly created "exp_id" column
        "nestedColumn",   # Select the "nestedColumn"
        "country",        # Select the "country" column
        "event_ts"        # Select the "event_ts" timestamp column
    )
)

# Displaying the transformed DataFrame (df_test_gold) for visual inspection
# This is commonly used in notebook environments to show the DataFrame in a tabular format
df_test_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show catalogs, schema and tables

# COMMAND ----------

# Show all catalogs in the metastore.
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG capgemini")

# Show schemas in the catalog that was set earlier.
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Set the current schema.
spark.sql("USE SCHEMA mafi_demo_schema")

# List the available tables in the catalog that was set earlier.
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

display(spark.table("capgemini.mafi_demo_schema.test_bronze_ingestion"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capgemini.mafi_demo_schema.test_bronze_ingestion
