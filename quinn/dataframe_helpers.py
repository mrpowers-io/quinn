class DataFrameHelpers:

    def column_to_list(self, df, col_name):
        return [x[col_name] for x in df.select(col_name).collect()]

    def two_columns_to_dictionary(self, df, key_col_name, value_col_name):
        k, v = key_col_name, value_col_name
        return {x[k]: x[v] for x in df.select(k, v).collect()}

    def to_list_of_dictionaries(self, df):
        return list(map(lambda r: r.asDict(), df.collect()))

