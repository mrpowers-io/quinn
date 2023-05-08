from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import quinn
from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def test_append_if_schema_identical(spark):
    source_data = [(1, "capetown", "Alice"), (2, "delhi", "Bob")]
    target_data = [(3, "Charlie", "New York"), (4, "Dave", "Los Angeles")]

    source_df = spark.createDataFrame(source_data, schema=StructType([
        StructField("id", IntegerType()),
        StructField("city", StringType()),
        StructField("name", StringType())
    ]))

    target_df = spark.createDataFrame(target_data, schema=StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("city", StringType())
    ]))

    # Call the append_if_schema_identical function
    quinn.append_if_schema_identical(source_df, target_df)
