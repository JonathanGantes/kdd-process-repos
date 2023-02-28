# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Cleaning Service
# MAGIC This Service lets you integrate datasets, select and clean columns and save resultant dataset as csv or parquet
# MAGIC 
# MAGIC ## Parameters 
# MAGIC 
# MAGIC  - ###  job_id
# MAGIC 	 - Description:
# MAGIC 	 - Type:
# MAGIC 	 - Example
# MAGIC  - ### notebook_params
# MAGIC 	 - #### resource_type
# MAGIC        - Description:
# MAGIC        - Type:
# MAGIC        - Example:
# MAGIC 	 - 
# MAGIC  - 

# COMMAND ----------

'''
    Import dependencies
'''
from transforms.transform_steps import TransformSteps
from src.utils.utils import *


# COMMAND ----------

'''
    This code finds one or more datasets with the route, integrate them and selects a list of columns to keep
'''

## Resource Info

resource_id = string_to_list_with_spaces(dbutils.widgets.get("resourceId"))


try:
    handler = TransformSteps(spark, dbutils)
            
    df = handler.integrate_json_or_parquet_datasets(resource_type, resource_id)

    df = df.transform(handler.create_id)
    df = df.transform(handler.get_minute_form_datetime)
    df = df.transform(handler.get_hour_form_datetime)
    df = df.transform(handler.datetime_to_shifts)
    df = df.transform(handler.steps_to_lvl)
    df = df.select("id", "dateTime", "steps", "hour", "minute", "timeShift", "stepsLvl")

    
    df.show()

except Exception:
    dbutils.notebook.exit({"message": "Something went wrong when transforming data", "status":"FAILED"})

