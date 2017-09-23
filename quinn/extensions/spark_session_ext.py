from quinn.spark import *

from pyspark.sql.types import StructType, StructField
from pyspark.sql import SparkSession

def createDF(self, rows_data, col_specs):
    struct_fields = list(map(lambda x: StructField(*x), col_specs))
    return spark.createDataFrame(rows_data, StructType(struct_fields))

SparkSession.createDF = createDF
