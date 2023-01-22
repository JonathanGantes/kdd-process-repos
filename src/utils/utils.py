import re

def string_to_list_without_spaces(value: str):
    result = clear_white_spaces(value)
    return result.split(",") if "," in result else [result]
    
def string_to_list_with_spaces(value: str):
    if "," in value:
        return re.split(r' , |, | ,|,', value)
    return [value]

def clear_white_spaces(value: str):
    return value.replace(" ","")

def is_empty_or_all(value: str):
    return value in ("","*")