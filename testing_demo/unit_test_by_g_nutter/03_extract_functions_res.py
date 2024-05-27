# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# COMMAND ----------

# Extract all that is not IO
from pyspark.sql import DataFrame

def calculate_vouchers(df: DataFrame) -> DataFrame:
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
  return res

# COMMAND ----------

# the first thing is to at least pass from to
def do_vouchers_io(customer_table : str, voucher_table : str ) -> None: #great moment to change name to the function 
  df = sqlContext.sql(f'SELECT * FROM {customer_table}')

  res = calculate_vouchers(df)
  
  res.write.option("mergeSchema", "true").format("delta").mode("overwrite").saveAsTable(voucher_table)

# COMMAND ----------

do_vouchers_io("capgemini.mafi_demo_schema.customer", "capgemini.mafi_demo_schema.output_two")
df = sqlContext.sql(f'SELECT * FROM capgemini.mafi_demo_schema.output_two')
display(df)

# COMMAND ----------


