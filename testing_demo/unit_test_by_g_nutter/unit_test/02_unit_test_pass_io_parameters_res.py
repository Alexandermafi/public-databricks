# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ../02_pass_io_parameters_res

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *

def create_customer_test(customer_table: str) -> None:
      df = spark.createDataFrame(
      [(1, 1.0,'FI'), (1, 2.0,'NO'), (2, 3.0,'UK'), (2, 5.0,'UK'), (2, 10.0, 'UK')],
      schema="customer_id int, spend double,  country string")
      df.write.format("delta").mode("overwrite").saveAsTable(customer_table)

class TestCustomer(NutterFixture):
  
  def __init__(self):
    self.customer_table_name = "capgemini.mafi_demo_schema.test_customer"
    self.voucher_table_name = "capgemini.mafi_demo_schema.test_voucher"
  
    sqlContext.sql(f'DROP TABLE IF EXISTS {self.customer_table_name}')
    sqlContext.sql(f'DROP TABLE IF EXISTS {self.voucher_table_name}')

    create_customer_test("capgemini.mafi_demo_schema.test_customer")
    
    NutterFixture.__init__(self)
    
  def assertion_this_is_pass(self):

    do_vouchers_io(self.customer_table_name , self.voucher_table_name)
    
    expected_df = spark.createDataFrame([(1, 3.0, 0.5), (2, 18.0, 9.0)],schema="customer_id int, total_spend double, discount_voucher double")
    # ("customer_id", "total_spend", "discount_voucher"))
    result_df = sqlContext.sql(f'SELECT * FROM {self.voucher_table_name}')
    
    assert_df_equality(expected_df.orderBy('customer_id'), result_df.orderBy('customer_id'))

# COMMAND ----------

result = TestCustomer().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()

if is_job:
  result.exit(dbutils)

# COMMAND ----------


