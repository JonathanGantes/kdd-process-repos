import uuid
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col


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

    def file_exists(self, path):
        try:
            self.dbutils.fs.ls(path)
            return True
        except Exception:
            return False
    
    def save_dataframe_to_csv(self, df: DataFrame):
        uid_exists = True
        trys = 0
        while uid_exists and trys < 20:
            uid = uuid.uuid1()
            data_location = f"file:/dbfs/FileStore/tables/tesis/cleared_data/{uid}"
            uid_exists = self.file_exists(data_location)
            trys+=1

        if not uid_exists:
            
            df.coalesce(1).write.options(header='True', delimiter=',').mode("overwrite").csv(data_location)
            
            ## Move Dataset And Delete Aditional created files
            files = self.dbutils.fs.ls(data_location)

            for x in files:
                if not x.path.endswith(".csv"):
                    self.dbutils.fs.rm(f"{data_location}/{x.path}")
            print(self.dbutils.fs.ls(data_location))
            csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
            self.dbutils.fs.mv(csv_file, data_location.rstrip('/') + ".csv")
            self.dbutils.fs.rm(data_location, recurse = True)
            self.dbutils.fs.rm(f"file:/dbfs/FileStore/tables/tesis/cleared_data/.{uid}.csv.crc")
            
            self.dbutils.notebook.exit({"resultUid": f"{uid}.csv", "status":"SUCCESSFUL"})
        else:
            self.dbutils.notebook.exit({"message": "Unable to save file tryed to generate an uid 20 times", "status":"FAILED"})
        
        
        
    def integrate_json_or_parquet_datasets(self, resource_type: str, resource_id: list):
        df = None
        try:
            df = self.spark.read.format(resource_type).load(*resource_id)
        except AnalysisException:
            self.dbutils.notebook.exit({"message": "Invalid Resource Id or Resource Does't Exists", "status":"FAILED"})
        return df

    
    