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


# COMMAND ----------

'''
    This code finds one or more datasets with the route, integrate them and selects a list of columns to keep
'''

from handlers.common_handler import CommonHandler
from src.utils.utils import *

## Resource Info
resource_type = dbutils.widgets.get("resourceType")
resource_id = string_to_list_with_spaces(dbutils.widgets.get("resourceId"))
columns_names = get_original_column_name_list(dbutils.widgets.get("selectColumns"))
columns_types = get_column_type_list(dbutils.widgets.get("selectColumns"))
columns_new_names = get_new_column_name_list(dbutils.widgets.get("selectColumns"))
dupl_cols = string_to_list_without_spaces(dbutils.widgets.get("clearDuplicated"))
nan_cols = string_to_list_without_spaces(dbutils.widgets.get("clearNaN"))
header = True if dbutils.widgets.get("header").lower() == "true" else False

delim = dbutils.widgets.get("delimiter")
delimiter = delim if (",", "") not in delim else ","


if (( all(elem in columns_names  for elem in dupl_cols) or is_empty_or_all(dupl_cols[0]))
    and (all(elem in columns_names  for elem in nan_cols) or is_empty_or_all(nan_cols[0]))):

    handler = CommonHandler(spark, dbutils)

    if resource_type == "json" or resource_type == "parquet" :
        df = handler.integrate_json_or_parquet_datasets(resource_type, resource_id)
    elif resource_type == "csv":
        df = handler.integrate_csv(resource_id, header, delimiter)
    else:
        dbutils.notebook.exit({"message": "Invalid Resource Type", "status":"FAILED"})

    df = handler.select_columns(df, columns_names, columns_new_names, columns_types)
    df = handler.drop_duplicated_columns(df, dupl_cols)
    df = handler.drop_nan_columns(df, nan_cols)
    display(df)
    handler.save_dataframe_to_csv(df, location=1)

    
else:
    dbutils.notebook.exit({"message": "Columns inside clearDuplicated or clearNaN have to be in column as well", "status":"FAILED"})



