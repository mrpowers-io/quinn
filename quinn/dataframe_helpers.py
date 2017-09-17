class DataFrameHelpers:

    def column_to_list(self, df, col_name):
        return list(map(lambda r: getattr(r, col_name), df.collect()))
