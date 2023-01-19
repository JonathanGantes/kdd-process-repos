from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col
import json

class Handler:

    def __init__(self, spark, dbutils):
        self.__spark = spark
        self.__dbutils = dbutils
    
    @property
    def spark(self):
        return self.__spark
    
    @property
    def dbutils(self):
        return self.__dbutils

    def select_columns(self, df: DataFrame, select_columns : list):
        new_df = None
        try:
            new_df = df.select([col(col_name).alias(col_name) for col_name in select_columns])
        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to select does not exists", "status":"FAILED"})
        return new_df

    def drop_duplicated_columns(self, df: DataFrame, columns:list):
        new_df = None
        try:
            new_df = df.drop_duplicates(columns)
        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to drop druplicateds does not exists", "status":"FAILED"})
        return new_df

    def drop_nan_columns(self, df: DataFrame, columns:list ):
        new_df = None
        try:
            new_df = df.dropna(subset=columns)
        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to drop NaN's does not exists", "status":"FAILED"})
        return new_df

    def integrate_json_or_parquet_datasets(self, resource_type: str, resource_id: list):
        df = None
        try:
            df = self.spark.read.format(resource_type).load(*resource_id)
        except AnalysisException:
            self.dbutils.notebook.exit({"message": "Invalid Resource Id or Resource Does't Exists", "status":"FAILED"})
        return df

    
    