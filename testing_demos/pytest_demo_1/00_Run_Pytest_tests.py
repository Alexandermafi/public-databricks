# Databricks notebook source
# MAGIC %md
# MAGIC Tutorial: https://medium.com/@ssharma31/integrating-pytest-with-databricks-a9e47afecd85

# COMMAND ----------

# MAGIC %sh pip install pytest

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
full_path_to_current_folder = "/Workspace/Users/alexander.mafi@capgemini.com/0. databricks-assets/testing_demo/pytest_demo_1"

# Changes the current working directory of the Python interpreter to the path specified
os.chdir(full_path_to_current_folder)

# COMMAND ----------

# MAGIC %md
# MAGIC Right click on a file or folder to copy the full path <br> <br>
# MAGIC <img src="https://docs.databricks.com/en/_images/repos-copy-path1.png" width="500">
# MAGIC

# COMMAND ----------

pytest.main(["-v"])

# COMMAND ----------


