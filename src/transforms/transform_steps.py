
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when, lit, hour, minute, monotonically_increasing_id
from pyspark.sql.types import StructType, TimestampType, IntegerType
from handlers.common_handler import CommonHandler


class TransformSteps(CommonHandler):

    def datetime_to_shifts(self, df: DataFrame):
        return df.withColumn("timeShift", \
            when(((hour(col("dateTime"))) >= 0) & ((hour(col("dateTime"))) <= 5), lit("madrugada")) \
            .when(((hour(col("dateTime"))) >= 6) & ((hour(col("dateTime"))) <= 11), lit("manana")) \
            .when(((hour(col("dateTime"))) >= 12) & ((hour(col("dateTime"))) <= 17), lit("tarde")) \
            .otherwise(lit("noche")))

    def steps_to_lvl(self, df: DataFrame):
        return df.withColumn("stepsLvL", \
            when(col("steps") <= 10, lit("sedentario")) \
            .when((col("steps") >= 11) & (col("steps") <= 25), lit("bajo")) \
            .when((col("steps") >= 26) & (col("steps") <= 50), lit("medio")) \
            .otherwise(lit("alto")))

    def get_hour_form_datetime(self, df: DataFrame):
        return df.withColumn("hour", hour(col("dateTime")))

    def create_id(self, df: DataFrame):
        return df.withColumn("id", monotonically_increasing_id())

    def get_minute_form_datetime(self, df: DataFrame):
        return df.withColumn("minute", minute(col("dateTime")))
    
    @staticmethod
    def generate_schema():
        schema = StructType() \
            .add("dateTime",TimestampType(),True) \
            .add("steps",IntegerType(),True)
        return schema