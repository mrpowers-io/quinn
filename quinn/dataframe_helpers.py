class DataFrameHelpers:

    def columnToList(self, df, colName):
        return list(map(lambda r: getattr(r, colName), df.collect()))
