# Databricks notebook source
# MAGIC %run ./00_prepare_tables

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# COMMAND ----------

# Create table for finance 
def do_vouchers() -> None:
  df = sqlContext.sql(f'SELECT * FROM capgemini.mafi_demo_schema.customer')
  total_spend = df.agg({'spend': 'sum'}).head()[0]
  total_customer = df.agg({'customer_id': 'count'}).head()[0]
  total_countries = df.agg({'country': 'count'}).head()[0]

  averange_spend = total_spend / total_customer
  
  spend =df.groupBy('customer_id').sum('spend')
  spend = spend.select(col('customer_id'), col("sum(spend)").alias("total_spend"))
  
  res = spend.withColumn('discount_voucher', 
    F.when(col('total_spend') < averange_spend, col('total_spend')/6.0)
    .when(col('total_spend') < averange_spend * 1.3, col('total_spend')/5.0)
    .otherwise(col('total_spend')/2.0 ))

  customer_table ="capgemini.mafi_demo_schema.output"
  res.write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable(customer_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capgemini.mafi_demo_schema.customer

# COMMAND ----------

do_vouchers()
df = sqlContext.sql(f'SELECT * FROM capgemini.mafi_demo_schema.output')
display(df)
