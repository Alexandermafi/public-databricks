# Databricks notebook source
import json

# COMMAND ----------

# Set Variables based on the DBFS directory location
dbfs_path = "/tmp/landing/iot_syntehtic_data"
jsons_path_data = f"{dbfs_path}/data/raw.json"
print(jsons_path_data)

# COMMAND ----------

# Remove JSON in DBFS if needed
# Delete folder and its content. Correspons to "rm -r". True enables recursive mode
display(dbutils.fs.rm(dbfs_path, True))

# COMMAND ----------

contents = """
{
    "quiz": {
        "sport": {
            "q1": {
                "question": "Which one is correct team name in NBA?",
                "options": [
                    "New York Bulls",
                    "Los Angeles Kings",
                    "Golden State Warriros",
                    "Huston Rocket"
                ],
                "answer": "Huston Rocket"
            }
        },
        "maths": {
            "q1": {
                "question": "5 + 7 = ?",
                "options": [
                    "10",
                    "11",
                    "12",
                    "13"
                ],
                "answer": "12"
            },
            "q2": {
                "question": "12 - 8 = ?",
                "options": [
                    "1",
                    "2",
                    "3",
                    "4"
                ],
                "answer": "4"
            }
        }
    }
}
"""
# contents= """
# {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
# {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
# {"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
# """
overwrite = True

dbutils.fs.put(jsons_path_data, contents, overwrite)

# COMMAND ----------

# Read the JSON in DBFS
display(dbutils.fs.ls(jsons_path_data))

# COMMAND ----------

# Read the JSON file
df = spark.read \
    .option("multiline", "true") \
    .json(jsons_path_data)

display(df)
