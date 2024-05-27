# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# COMMAND ----------

# Extract all that is not IO
from pyspark.sql import DataFrame

low_spenders_ratio = 1.0/6.0
middle_spenders_ratio = 1.0/5.0
high_spenders_ratio = 0.5

middle_spenders_percertange = 1.3 

def calculate_vouchers(df: DataFrame) -> DataFrame:
  total_spend = df.agg({'spend': 'sum'}).head()[0]
  total_customer = df.agg({'customer_id': 'count'}).head()[0]
  # total_countries = df.agg({'country': 'count'}).head()[0]. <--- delete this line 
  averange_spend = total_spend / total_customer
  
  spend =df.groupBy('customer_id').sum('spend')
  spend = spend.select(col('customer_id'), col("sum(spend)").alias("total_spend"))

  res = spend.withColumn('discount_voucher', 
    F.when(col('total_spend') < averange_spend, col('total_spend') * low_spenders_ratio) # no magig cumbers, mult instead of div
    .when(col('total_spend') < averange_spend * middle_spenders_percertange, col('total_spend') * middle_spenders_ratio) # no magig cumbers, mult instead of div
    .otherwise(col('total_spend') * high_spenders_ratio )) # no magig cumbers, mult instead of div 
  return res

# COMMAND ----------


