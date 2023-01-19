
def string_to_list_without_spaces(value: str):
    result = clear_white_spaces(value)
    return result.split(",") if "," in result else [result]
    
def string_to_list_with_spaces(value: str):
    return value.split(",") if "," in value else [value]

def clear_white_spaces(value: str):
    return value.replace(" ","")
