# Databricks notebook source
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# https://github.com/microsoft/nutter
from runtime.nutterfixture import NutterFixture, tag
# https://github.com/MrPowers/chispa
from chispa.dataframe_comparer import *


class TestCustomer(NutterFixture):
  def __init__(self):
    self.customer_table_name = "customer" #if you need to initialize anything
    NutterFixture.__init__(self)
    
  def assertion_what_I_am_testing(self):
    result = 1 # restult = STUFF
    expected_result = 1 # expected_result = STUFF 
    assert (result == expected_result)
    
  def assertion_other_thing_I_am_testing(self):
    result = 2 # restult = STUFF
    expected_result = 1 # expected_result = STUFF 
    assert (result == expected_result)


# COMMAND ----------

result = TestCustomer().execute_tests()
print(result.to_string())
is_job = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().isDefined()
if is_job:
  result.exit(dbutils)

# COMMAND ----------


