# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate Synthetic Data with "Databricks Labs Data Generator (dbldatagen)"
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - https://github.com/databrickslabs/dbldatagen
# MAGIC - See library documentation here https://databrickslabs.github.io/dbldatagen/public_docs/index.html
# MAGIC

# COMMAND ----------

# Install the library on the compute cluster
%pip install dbldatagen

# COMMAND ----------

# Set Variables based on the volume location in Unity Catalog
volume_path = "/Volumes/dev_catalog/demos/iot_syntehtic_data"
jsons_path_data = f"{volume_path}/data"
jsons_path_checkpoint = f"{volume_path}/checkpoint/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Semistructured Data Generation - Nested Columns

# COMMAND ----------

import dbldatagen as dg
import time
from pyspark.sql.types import FloatType, LongType, IntegerType, StringType, StructType, StructField, ArrayType

shuffle_partitions_requested = 2
device_population = 100
data_rows = 10
partitions_requested = 10

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

country_codes = [
    "CN",
    "US",
    "FR",
    "CA",
    "IN",
    "JM",
    "IE",
    "PK",
    "GB",
    "IL",
    "AU",
    "SG",
    "ES",
    "GE",
    "MX",
    "ET",
    "SA",
    "LB",
    "NL",
]
country_weights = [
    1300,
    365,
    67,
    38,
    1300,
    3,
    7,
    212,
    67,
    9,
    25,
    6,
    47,
    83,
    126,
    109,
    58,
    8,
    17,
]

manufacturers = [
    "Delta corp",
    "Xyzzy Inc.",
    "Lakehouse Ltd",
    "Acme Corp",
    "Embanks Devices",
]

lines = ["delta", "xyzzy", "lakehouse", "gadget", "droid"]


# Define nested structure
nestedStructType = StructType(
    [
        StructField("subfield1", IntegerType(), True),
        StructField("subfield2", FloatType(), True),
        StructField("subfield3", StringType(), True),
    ]
)

# Define array type for nested elements
arrayType = ArrayType(IntegerType())

testDataSpec = (
    dg.DataGenerator(
        spark, name="device_data_set", rows=data_rows, partitions=partitions_requested
    )
    .withIdOutput()
    # we'll use hash of the base field to generate the ids to
    # avoid a simple incrementing sequence
    .withColumn(
        "internal_device_id",
        "long",
        minValue=0x1000000000000,
        uniqueValues=device_population,
        omit=True,
        baseColumnType="hash",
    )
    # note for format strings, we must use "%lx" not "%x" as the
    # underlying value is a long
    .withColumn(
        "device_id", "string", format="0x%013x", baseColumn="internal_device_id"
    )
    # the device / user attributes will be the same for the same device id
    # so lets use the internal device id as the base column for these attribute
    .withColumn(
        "country",
        "string",
        values=country_codes,
        weights=country_weights,
        baseColumn="internal_device_id",
    )
    .withColumn(
        "manufacturer",
        "string",
        values=manufacturers,
        baseColumn="internal_device_id",
    )
    # use omit = True if you don't want a column to appear in the final output
    # but just want to use it as part of generation of another column
    .withColumn(
        "line",
        "string",
        values=lines,
        baseColumn="manufacturer",
        baseColumnType="hash",
        omit=True,
    )
    .withColumn(
        "nestedColumn",
        nestedStructType,
        expr="struct(floor(rand() * 10) as subfield1, rand() as subfield2, concat('prefix_', floor(rand() * 100)) as subfield3)",
    )
    .withColumn(
        "arrayColumn",
        arrayType,
        expr="array(floor(rand() * 10), floor(rand() * 20), floor(rand() * 30))",
    )
    .withColumn(
        "model_ser",
        "integer",
        minValue=1,
        maxValue=11,
        baseColumn="device_id",
        baseColumnType="hash",
        omit=True,
    )
    .withColumn(
        "model_line",
        "string",
        expr="concat(line, '#', model_ser)",
        baseColumn=["line", "model_ser"],
    )
    .withColumn(
        "event_type",
        "string",
        values=[
            "activation",
            "deactivation",
            "plan change",
            "telecoms activity",
            "internet activity",
            "device error",
        ],
        random=True,
    )
    .withColumn(
        "event_ts",
        "timestamp",
        begin="2020-01-01 01:00:00",
        end="2020-12-31 23:59:00",
        interval="1 minute",
        random=True,
    )
)

# batch
#dfTestData = testDataSpec.build()
#dfTestData.write.format("json").mode("overwrite").save(jsons_path_data)

# Streaming
dfTestData = testDataSpec.build(withStreaming=True, options={'rowsPerSecond': 1})

display(dfTestData)

# COMMAND ----------

# Run cell if generating streaming data
# output jsons from streaming dataframe

(
    dfTestData.writeStream.format("json")
    .outputMode("append")
    .option("checkpointLocation", jsons_path_checkpoint)
    .option("maxRecordsPerFile", 1)
    .start(jsons_path_data)
)

# COMMAND ----------

# Show content in the Volume
display(dbutils.fs.ls(volume_path))

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # Using shell commands to check the volume storage. The magic command (%) at the top of the cell tells Databricks that it contains shell commands
# MAGIC ls "/Volumes/dev_catalog/demos/iot_syntehtic_data"

# COMMAND ----------

# Show data in the folder "jsons_path_data" where the generated JSON data is saved
display(dbutils.fs.ls(jsons_path_data))

# COMMAND ----------

# Shows checkpoint data
display(dbutils.fs.ls(jsons_path_checkpoint))

# COMMAND ----------

time_to_run = 10
time.sleep(time_to_run)

# COMMAND ----------

display(dbutils.fs.ls(jsons_path_data))

# COMMAND ----------

display(dbutils.fs.ls(jsons_path_checkpoint))

# COMMAND ----------

time.sleep(60)

# note stopping the stream may produce exceptions - these can be ignored
for x in spark.streams.active:
    try:
        x.stop()
    except RuntimeError:
        pass

# COMMAND ----------

display(dbutils.fs.ls(jsons_path_data))

# COMMAND ----------

display(dbutils.fs.ls(jsons_path_checkpoint))

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete all data in the volume

# COMMAND ----------

###### RESET ######

# Delete folder and its content. Correspons to "rm -r". True enables recursive mode
display(dbutils.fs.rm(jsons_path_data, True))
display(dbutils.fs.rm(jsons_path_checkpoint, True))

# COMMAND ----------


