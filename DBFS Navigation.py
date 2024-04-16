# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS
# MAGIC This notebooks show how to navigate and explore DBFS
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - See documentation here https://docs.databricks.com/en/files/index.html and https://docs.databricks.com/en/dev-tools/databricks-utils.html
# MAGIC
# MAGIC | Tool | Example | 
# MAGIC |---|---|
# MAGIC | Databricks file system utilities | dbutils.fs.ls("file:/path") %fs ls file:/path |
# MAGIC | Bash shell commands | %sh curl http://<address>/text.zip > /dbfs/mnt/tmp/text.zip |
# MAGIC
# MAGIC

# COMMAND ----------

# Python syntax for using the tool "Databricks file system utilities"
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC # Using shell commands to check the DBFS storage. The magic command (%) at the top of the cell tells Databricks that it contains shell commands
# MAGIC ls "/dbfs/"

# COMMAND ----------


