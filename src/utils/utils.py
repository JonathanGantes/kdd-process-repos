from pyspark.sql.dataframe import DataFrame

class Utils:

    @staticmethod
    def string_to_list(value: str):
        result = __class__.clear_white_spaces(value)
        return result.split(",") if "," in result else [result]

    @staticmethod
    def clear_white_spaces(value: str):
        return value.replace(" ","")

    @staticmethod
    def drop_duplicated_columns(df: DataFrame, columns:list):
        return df.drop_duplicates(columns)

    @staticmethod
    def drop_nan_columns(df: DataFrame, columns:list ):
        return df.dropna(subset=columns)