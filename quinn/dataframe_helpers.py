def column_to_list(df, col_name):
    return [x[col_name] for x in df.select(col_name).collect()]


def two_columns_to_dictionary(df, key_col_name, value_col_name):
    k, v = key_col_name, value_col_name
    return {x[k]: x[v] for x in df.select(k, v).collect()}


def to_list_of_dictionaries(df):
    return list(map(lambda r: r.asDict(), df.collect()))


def print_athena_create_table(df, athena_table_name, s3location):
    fields = df.schema

    print(f"CREATE EXTERNAL TABLE IF NOT EXISTS `{athena_table_name}` ( ")

    for field in fields.fieldNames()[:-1]:
        print("\t", f"`{fields[field].name}` {fields[field].dataType.simpleString()}, ")
    last = fields[fields.fieldNames()[-1]]
    print("\t", f"`{last.name}` {last.dataType.simpleString()} ")

    print(")")
    print("STORED AS PARQUET")
    print(f"LOCATION '{s3location}'\n")
