from pyspark.sql.functions import lit

class DataFrameTransformations:
    def with_greeting(self, df):
        return df.withColumn("greeting", lit("hi"))

    def with_something(self, df, something):
        return df.withColumn("something", lit(something))
