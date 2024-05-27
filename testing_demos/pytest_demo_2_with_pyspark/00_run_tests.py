# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Run all tests from this Notebook
# MAGIC
# MAGIC Guide: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
# MAGIC
# MAGIC Three types of files needed to get started with the testing. 
# MAGIC - This notebook "apply_and_run_tests"
# MAGIC - Files that start with "code" are python functions to be tested to ensure the transformation logic is solid
# MAGIC - Files starting with "tests" are picked up by Pytest, and each file can contain one or many tests
# MAGIC
# MAGIC For simplicity all files are in the same folder. 
# MAGIC
# MAGIC ### The Basic Idea of Testing Data Transformation Logic
# MAGIC Compare two dataframes and see if that match. If matched -> success. Else -> failure. 
# MAGIC
# MAGIC Data involved in the testing process:
# MAGIC - **non_processed_data**: Input data to your function with transformation logic
# MAGIC - **transformed_data**: Output data from your function
# MAGIC - **expected_data**: The correct data as it should look like after the *non_processed_data* has been processed by your function. 
# MAGIC
# MAGIC If your code logic is correct, then the **transformed_data** and **expected_data** should be the same. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Pytest
# MAGIC
# MAGIC We can write our tests with pytest, which is one of the most popular Python testing frameworks. For more information about pytest, see the docs here: https://docs.pytest.org/en/7.1.x/contents.html.
# MAGIC

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install pytest

# COMMAND ----------

import pytest
import os
import sys

# Disabling Bytecode Generation
# Python normally generates .pyc files as a cache, which contain the bytecode of your Python files. 
# Setting this attribute to True prevents Python from writing these .pyc files. This can be particularly 
# useful during testing to ensure that you are testing the actual .py files and not their bytecode cached versions.
sys.dont_write_bytecode = True

# Set the full path below
full_path_to_current_folder = "/Workspace/Users/alexander.mafi@capgemini.com/0. databricks-assets/testing_demo/pytest_demo_2_with_pyspark"

# Changes the current working directory of the Python interpreter to the path specified
os.chdir(full_path_to_current_folder)

# COMMAND ----------

pytest.main(["-v"])

# COMMAND ----------


# Run pytest.
pytest.main([".", "-v", "-p", "no:cacheprovider"])

# COMMAND ----------


