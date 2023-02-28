import re

def string_to_list_without_spaces(value: str):
    result = clear_white_spaces(value)
    return result.split(",") if "," in result else [result]
    
def string_to_list_with_spaces(value: str):
    if "," in value:
        return re.split(r' , |, | ,|,', value)
    return [value]

def get_original_column_name_list(value: str):
    columns = string_to_list_without_spaces(value)
    base_list = [re.split(r':|=', column) for column in columns]
    result = []
    for org_col_name in base_list:
        result.append(org_col_name[0])
    return result

def get_column_type_list(value: str):
    columns = string_to_list_without_spaces(value)
    base_list = [column.split(":") for column in columns]
    result = []
    for new_col_name in base_list:
        result.append("auto" if len(new_col_name) == 1 else new_col_name[1].split("=")[0])
    return result

def get_new_column_name_list(value: str):
    columns = string_to_list_without_spaces(value)
    base_list = [column.split("=") for column in columns]
    result = []
    for new_col_name in base_list:
        result.append(new_col_name[0].split(":")[0] if len(new_col_name) == 1 else new_col_name[1])
    return result

def clear_white_spaces(value: str):
    return value.replace(" ","")

def is_empty_or_all(value: str):
    return value in ("","*")
