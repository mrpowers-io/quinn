class DataFrameHelpers:

    def column_to_list(self, df, col_name):
        return list(map(lambda r: getattr(r, col_name), df.collect()))

    def two_columns_to_dictionary(self, df, key_col_name, value_col_name):
        l = list(map(lambda r: (getattr(r, key_col_name), getattr(r, value_col_name)), df.collect()))
        return dict(l)

    def to_list_of_dictionaries(self, df):
        return list(map(lambda r: r.asDict(), df.collect()))

