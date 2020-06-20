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


def show_output_to_df(show_output, spark):
    l = show_output.split("\n")
    ugly_column_names = l[1]
    pretty_column_names = [i.strip() for i in ugly_column_names[1:-1].split("|")]
    pretty_data = []
    ugly_data = l[3:-1]
    for row in ugly_data:
        r = [i.strip() for i in row[1:-1].split("|")]
        pretty_data.append(tuple(r))
    return spark.createDataFrame(pretty_data, pretty_column_names)
