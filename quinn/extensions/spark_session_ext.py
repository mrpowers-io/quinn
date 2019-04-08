from pyspark.sql.types import StructType, StructField
from pyspark.sql import SparkSession


def create_df(self, rows_data, col_specs):
    struct_fields = list(map(lambda x: StructField(*x), col_specs))
    return self.createDataFrame(data=rows_data, schema=StructType(struct_fields))


SparkSession.create_df = create_df
