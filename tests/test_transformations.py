import pytest

from quinn.spark import *
from quinn.spark_session_ext import *
import quinn.transformations as QT

from pyspark.sql.types import StructType, StructField, StringType, BooleanType

class TestTransformations(object):

    def test_snake_case_col_names(self):
        schema = StructType([
            StructField("I like CHEESE", StringType(), True),
            StructField("YUMMMMY stuff", StringType(), True)]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)

        actual_df = QT.snake_case_col_names(source_df)

        expected_df = spark.createDF(
            [
                ("jose", "a"),
                ("li", "b"),
                ("sam", "c")
            ],
            [
                ("i_like_cheese", StringType(), True),
                ("YUMMMMY_stuff", StringType(), True)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())
