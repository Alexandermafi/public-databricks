# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ../04_lets_clean_a_little_bit_this_function
# MAGIC

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *


class TestCustomer(NutterFixture):
  def __init__(self):
    NutterFixture.__init__(self)
    
  def assertion_calculate_vouchers(self):
    customer_df = spark.createDataFrame([(1, 1.0,'FI'), (1, 2.0,'NO'), (2, 3.0,'UK'), (2, 5.0,'UK'), (2, 10.0, 'UK')],schema="customer_id int, spend double,  country string")

    expected_df = spark.createDataFrame([(1, 3.0, 0.5), (2, 18.0, 9.0)],schema="customer_id int, total_spend double, discount_voucher double")

    result_df = calculate_vouchers(customer_df)  #easy to test no IO no RISK of someone writing to that tables 
    
    assert_df_equality(expected_df.orderBy('customer_id'), result_df.orderBy('customer_id'))

# COMMAND ----------

result = TestCustomer().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()

if is_job:
  result.exit(dbutils)

# COMMAND ----------


