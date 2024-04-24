from pyspark.sql.functions import expr, col

def get_rules(spark, tag):
  """
    Retrieves data quality rules that match a given tag from a table. Returns the fules in a format ready for DLT annotation. 

    Parameters:
    - spark (SparkSession): The Spark session to use for reading the table.
    - tag (str): The tag used to filter the rules. Only rules matching this tag will be returned.

    Returns:
    - dict: A dictionary where each key is a rule name and each value is the corresponding constraint.
  """
  rules = {}
  df = spark.read.table("capgemini.mafi_demo_schema.data_quality_rules").where(f"tag = '{tag}'")
  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules
