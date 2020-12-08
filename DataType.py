ALL_DATA_TYPES = {
    "str": ["char", "varchar", "varbinary", "tinyBlob", "text", "longText","str"],
    "int": ["smallint", "int", "mediumint", "integer", "bigint"],
    "float": ["float", "double", "decimal"]
}


def getPythonBasedDataType(value):
    value = value.lower()
    for datatype in ALL_DATA_TYPES:
        for testType in ALL_DATA_TYPES[datatype]:
            if (testType in value):
                return datatype

    return None
