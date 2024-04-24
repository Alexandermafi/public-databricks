# Demo for Delta Live Tables with Unity Catalog
<br>

- Based on generated JSON data files simulating IoT Data
- DLT Pipeline with from raw files to gold table
- Lineage visible in Unity Catalog
- DLT expectations are fetched from a created delta table for more flexibility & reusability


### Based on generated JSON data files simulating IoT Data

The folder "data_generation" contains three notebooks where you choose "JSON Batch Data Generation to Unity Catalog Volume". Thw other two in the folder "Streaming Data Generation" is for generating streaming data and depending on if you want to generated the JSON files to Unity Catalog (in a volume) or into dbfs, which is the only option if you don't have Unity Catalog access.

### DLT Pipeline with from raw files to gold table

The DLT Pipeline uses two DLT notebooks. 
#### DLT Notebooks to attach to the DLT Pipeline

The DLT Notebooks are found in the folder "dlt_pipeline" and are divdeded into two:
- DLT_Ingestion
- DLT_Transformations


#### DLT Pipeline Settings

Create a new Delta Live Tables (DLT) pipeline and in the creation wizard in the top right corner, switch from "UI" to "JSON" and paste the JSON text below. Thereafter you can switch back to "UI" and name your pipeline and do additional tweeks since you won't be able to use my configuration for the source code and catalog. 

```
{
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "num_workers": 1
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "PREVIEW",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/alexander.mafi@capgemini.com/databricks-assets/DLT_IoT_Demo/dlt_pipeline/DLT_Ingestion"
            }
        },
        {
            "notebook": {
                "path": "/Repos/alexander.mafi@capgemini.com/databricks-assets/DLT_IoT_Demo/dlt_pipeline/DLT_Transformations"
            }
        }
    ],
    "name": "Iot_Demo",
    "edition": "ADVANCED",
    "catalog": "capgemini",
    "target": "mafi_sandbox",
    "data_sampling": false
}
```


### Lineage visible in Unity Catalog

Check it out in Unity Catalog. You should see data and pipeline lineage

### DLT expectations are fetched from a delta table for more flexibility & reusability

In the folder "data_quality_rules_setup" there's a notebook "create_quality_rules_table" for creating a delta table with the data quality rules which looks like this:

| tag               | name          | constraint                |
| ----------------- | ------------- | ------------------------- |
| iot_events_silver | valid_id      | id IS NOT NULL AND id > 0 |
| iot_events_gold   | valid_country | country IS NOT NULL       |


The python script "get_rules.py" in the folders "helpers" fetches the rules by the tag with the function "get_rules". Note that you need to change "capgemini.mafi_sandbox.data_quality_rules" in the script to match your path to the created table. 

```
#Return the rules matching the tag as a format ready for DLT annotation.

from pyspark.sql.functions import expr, col

def get_rules(spark, tag):

  """
    loads data quality rules from csv file
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """

  rules = {}
  df = spark.read.table("capgemini.mafi_sandbox.data_quality_rules").where(f"tag = '{tag}'")

  for row in df.collect():
    rules[row['name']] = row['constraint']
  return rules
```


Example of how t is used 

```
@dlt.expect_all_or_drop(get_rules(spark, 'user_bronze_dlt')) #get the rules from our centralized table.
```

### Enabling Import of Python modules from Databricks repos

Sources
- https://docs.databricks.com/en/files/workspace-modules.html
- https://docs.databricks.com/en/delta-live-tables/import-workspace-files.html

This is at the top of each notebook to enable imports of your own python modules
```
import sys
import os
  

# Get the root path for the DLT Pipeline dynamically for all users by travelling one level up
repos = []

for p in sys.path:
    if "Repos" in p:
        repos.append(p)

repos.sort(reverse=True)
dlt_pipeline_root_path = "/".join(repos[0].split("/")[:-1]) + "/"

# Set current working directory to the root level to enable modules stored as python files
sys.path.append(os.path.abspath(dlt_pipeline_root_path))
print(dlt_pipeline_root_path)
```

### Furter Reading & Watching
<br>

Software Development Best-Practices for DLT
- https://www.databricks.com/blog/applying-software-development-devops-best-practices-delta-live-table-pipelines

Links on how to work with DLT using python
- https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/tutorial-python
- https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations
- https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/python-ref

Video "Delta Live Tables A to Z: Best Practices for Modern Data Pipelines"
- https://www.youtube.com/watch?v=PIFL7W3DmaY&t=2s
