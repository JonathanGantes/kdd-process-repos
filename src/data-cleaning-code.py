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
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from src.handlers.handler import Handler
from src.utils.utils import string_to_list_with_spaces, string_to_list_without_spaces


# COMMAND ----------

'''
    This code finds one or more datasets with the route, integrate them and selects a list of columns to keep
'''

## Resource Info
resource_type = dbutils.widgets.get("resourceType")
resource_id = string_to_list_with_spaces(dbutils.widgets.get("resourceId"))
select_columns = string_to_list_without_spaces(dbutils.widgets.get("selectColumns"))
dupl_cols = string_to_list_without_spaces(dbutils.widgets.get("clearDuplicated"))
nan_cols = string_to_list_without_spaces(dbutils.widgets.get("clearNaN"))



if all(elem in select_columns  for elem in dupl_cols) and all(elem in select_columns  for elem in nan_cols):

    if resource_type == "json" or resource_type == "parquet" :

        handler = Handler(spark, dbutils)
        df = handler.integrate_json_or_parquet_datasets(resource_type, resource_id)
        df = handler.select_columns(df, select_columns)
        df = handler.drop_duplicated_columns(df, dupl_cols)
        df = handler.drop_nan_columns(df, nan_cols)
        df.show()
        df.printSchema()

    elif resource_type == "csv":
        pass
        
    else:
        dbutils.notebook.exit("Invalid Resource Type")
else:
    dbutils.notebook.exit("Columns inside clearDuplicated or clearNaN have to be in column as well")



