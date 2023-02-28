import uuid
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col


class CommonHandler:

    def __init__(self, spark, dbutils=None):
        self.__spark = spark
        self.__dbutils = dbutils
        self.start_rows = 0
        self.end_rows = 0
        self.duplicated_dropeds = 0
        self.nan_dropeds = 0
    
    @property
    def spark(self):
        return self.__spark
    
    @property
    def dbutils(self):
        return self.__dbutils

    def select_columns(self, df: DataFrame, select_columns : list, new_col_names: list, new_column_types: list) -> DataFrame:
        new_df = None
        try:
            
            selecteds = []
            if "*" not in select_columns:
                for x in range(len(select_columns)):
                    if new_column_types[x] == "auto":
                        selecteds.append(col(select_columns[x]).alias(new_col_names[x]))
                    else:
                        selecteds.append(col(select_columns[x]).alias(new_col_names[x]).cast(new_column_types[x]))
            else:
                selecteds.append("*")

            new_df = df.select(selecteds)
            self.start_rows = new_df.count()
        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to select does not exists", "status":"FAILED"})
        return new_df

    def select_array_columns(self, df: DataFrame, select_columns : list) -> DataFrame:
        new_df = None
        try:
            new_df = df.select([col(col_name).alias(col_name) for col_name in select_columns])
            self.start_rows = new_df.count()
        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to select does not exists", "status":"FAILED"})
        return new_df
    
    def drop_duplicated_columns(self, df: DataFrame, columns:list) -> DataFrame:
        new_df = None
        old_count =df.count()
        try:
            if columns[0] == "":
                new_df = df
            elif columns[0] == "*":
                new_df = df.drop_duplicates()
                self.duplicated_dropeds = old_count - new_df.count()
            else:
                new_df = df.drop_duplicates(columns)
                self.duplicated_dropeds = old_count - new_df.count()

        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to drop druplicateds does not exists", "status":"FAILED"})
        return new_df

    def drop_nan_columns(self, df: DataFrame, columns:list ) -> DataFrame:
        new_df = None
        old_count = df.count()
        try:
            if columns[0] == "":
                new_df = df
            elif columns[0] == "*":
                new_df = df.dropna()
                self.nan_dropeds = old_count - new_df.count()
            else:
                new_df = df.dropna(subset=columns)
                self.nan_dropeds = old_count - new_df.count()

        except Exception:
            self.dbutils.notebook.exit({"message": "One o more of the columns that you are tryng to drop NaN's does not exists", "status":"FAILED"})
        return new_df

    def file_exists(self, path):
        try:
            self.dbutils.fs.ls(path)
            return True
        except Exception:
            return False
    
    def save_dataframe_to_csv(self, df: DataFrame, location = None):
        uid_exists = True
        trys = 0
        self.end_rows = df.count()

        folder = self.get_location(location)

        while uid_exists and trys < 20:
            uid = uuid.uuid1()
            data_location = f"file:/dbfs/FileStore/tables/tesis/{folder}/{uid}"
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
            self.dbutils.fs.rm(f"file:/dbfs/FileStore/tables/tesis/{folder}/.{uid}.csv.crc")
            
            self.dbutils.notebook.exit({
                                        "resultUid": f"{uid}.csv",
                                        "startRows": f"{self.start_rows}",
                                        "endRows": f"{self.end_rows}",
                                        "duplicatedDropedRows": f"{self.duplicated_dropeds}",
                                        "nanDropedRows": f"{self.nan_dropeds}",
                                        "status":"SUCCESSFUL"})
        else:
            self.dbutils.notebook.exit({"message": "Unable to save file tryed to generate an uid 20 times", "status":"FAILED"})

        
    def integrate_json_or_parquet_datasets(self, resource_type: str, resource_id: list) -> DataFrame:
        df = None
        try:
            df = self.spark.read.format(resource_type).load(resource_id)
        except AnalysisException:
            self.dbutils.notebook.exit({"message": "Invalid Resource Id or Resource Does't Exists", "status":"FAILED"})
        return df

    def integrate_csv(self, resource_id: list,  header : bool = True , delimter = ",", schema : str = None) -> DataFrame:
        df = None
        try:
            if schema is None:
                df = self.spark.read.format("csv") \
                    .option("header", header) \
                    .option("delimiter", delimter) \
                    .load(resource_id)
            else:
                df = self.spark.read.format("csv") \
                    .option("header", header) \
                    .option("delimiter", delimter) \
                    .schema(schema) \
                    .load(resource_id)
    
        except AnalysisException:
            self.dbutils.notebook.exit({"message": "Invalid Resource Id or Resource Does't Exists", "status":"FAILED"})
        return df

    def get_location(self, location: int):
        if location == 1:
            return "cleared_data"
        elif location == 2:
            return "transformed_data"
        else:
            return "others"