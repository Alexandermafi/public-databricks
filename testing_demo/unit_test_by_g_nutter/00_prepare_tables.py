# Databricks notebook source
customer_table ="capgemini.mafi_demo_schema.customer"

# COMMAND ----------

def create_customer(customer_table: str) -> None:
    df = spark.createDataFrame(
    [(1, 1.0,'FI'), (1, 2.0,'NO'), (2, 3.0,'UK'), (2, 5.0,'UK'), (2, 10.0, 'UK')],
    ("customer_id", "spend", "country"))
    df.write.format("delta").mode("overwrite").saveAsTable(customer_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS capgemini.mafi_demo_schema.output

# COMMAND ----------

create_customer(customer_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM capgemini.mafi_demo_schema.customer;

# COMMAND ----------


