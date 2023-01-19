class Utils:

    @staticmethod
    def string_to_list(value: str):
        result = __class__.clear_white_spaces(value)
        return result.split(",") if "," in result else [result]

    @staticmethod
    def clear_white_spaces(value: str):
        return value.replace(" ","")