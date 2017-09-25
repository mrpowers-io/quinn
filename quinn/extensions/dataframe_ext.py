from pyspark.sql.dataframe import DataFrame


def transform(self, f):
    return f(self)


DataFrame.transform = transform
