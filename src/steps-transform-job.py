# Databricks notebook source
from transforms.transform_steps import TransformSteps
from src.utils.utils import *

resource_id = string_to_list_with_spaces(dbutils.widgets.get("resourceId"))
resource_id = [f"file:/dbfs/FileStore/tables/tesis/cleared_data/{res}" for res in resource_id]

handler = TransformSteps(spark, dbutils)

schema = TransformSteps.generate_schema()
df = handler.integrate_csv(resource_id, schema=schema)
    
df = df.transform(handler.create_id)
df = df.transform(handler.get_minute_form_datetime)
df = df.transform(handler.get_hour_form_datetime)
df = df.transform(handler.steps_to_lvl)
    
df = df.select("id", "dateTime", "steps", "hour", "minute", "stepsLvl")

handler.save_dataframe_to_csv(df, location=2)
