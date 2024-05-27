# Databricks notebook source
# MAGIC %md
# MAGIC Guide: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html

# COMMAND ----------

non_processed_data = [{"name": "John    D.", "age": 30},
  {"name": "Alice   G.", "age": 25},
  {"name": "Bob  T.", "age": 35},
  {"name": "Eve   A.", "age": 28}]

df = spark.createDataFrame(non_processed_data)
df.display()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed

transformed_df = remove_extra_spaces(df, "name")

transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1 - Using Only PySpark Built-in Test Utility Functions
# MAGIC
# MAGIC For simple ad-hoc validation cases, PySpark testing utils like assertDataFrameEqual and assertSchemaEqual can be used in a standalone context. You could easily test PySpark code in a notebook session. For example, say you want to assert equality between two DataFrames:

# COMMAND ----------

import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual

# Example 1
df1 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 1000), ("2", 3000)], schema=["id", "amount"])
assertDataFrameEqual(df1, df2)  # pass, DataFrames are identical

# COMMAND ----------

# Example 2
df1 = spark.createDataFrame(data=[("1", 0.1), ("2", 3.23)], schema=["id", "amount"])
df2 = spark.createDataFrame(data=[("1", 0.109), ("2", 3.23)], schema=["id", "amount"])

# Displays a message based on the assertion outcome 
try:
    # Assert equality
    assertDataFrameEqual(df1, df2, rtol=1e-1)  # pass, DataFrames are approx equal by rtol
    print("Test passed: DataFrames are identical.")
except AssertionError as e:
    print("Test failed:", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC You can also simply compare two DataFrame schemas:

# COMMAND ----------

from pyspark.testing.utils import assertSchemaEqual
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType

s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
s2 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])

assertSchemaEqual(s1, s2)  # pass, schemas are identical
