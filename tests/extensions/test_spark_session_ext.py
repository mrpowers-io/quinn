import pytest

from quinn.spark import *
from quinn.extensions import *

from pyspark.sql.types import StructType, StructField, StringType, BooleanType

class TestSparkSessionExt:

    def test_create_df(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("blah", StringType(), True)]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        actual_df = spark.createDataFrame(data, schema)

        expected_df = spark.create_df(
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [("name", StringType(), True), ("blah", StringType(), True)]
        )

        assert(expected_df.collect() == actual_df.collect())
