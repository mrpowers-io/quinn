from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

import quinn
from quinn import append_if_schema_identical

from tests.conftest import auto_inject_fixtures


# spark = SparkSession.builder \
#     .appName('so') \
#     .getOrCreate()
#
# sc = spark.sparkContext

@auto_inject_fixtures("spark")
def test_append_if_schema_identical(spark):
    source_data = [(1, "Alice", 25), (2, "Bob", 30)]
    target_data = [(3, "Charlie", "New York"), (4, "Dave", "Los Angeles")]

    source_df = spark.createDataFrame(source_data, schema=StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType())
    ]))

    target_df = spark.createDataFrame(target_data, schema=StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType())
    ]))

    # Call the append_if_schema_identical function
    appended_df = quinn.append_if_schema_identical(source_df, target_df)
