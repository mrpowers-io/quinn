import pytest

from quinn.spark import *
import quinn.functions as QF
from quinn.spark_session_ext import *

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

class TestFunctions(object):

    def test_exists(self):
        source_df = spark.createDataFrame(
            [
                ("jose", [1, 2, 3]),
                ("li", [4, 5, 6]),
                ("luisa", [10, 11, 12]),
            ],
            StructType([
                StructField("name", StringType(), True),
                StructField("nums", ArrayType(IntegerType(), True), True),
            ])
        )

        actual_df = source_df.withColumn(
            "any_num_greater_than_5",
            QF.exists(lambda n: n > 5)(col("nums"))
        )

        expected_df = spark.createDataFrame(
            [
                ("jose", [1, 2, 3], False),
                ("li", [4, 5, 6], True),
                ("luisa", [10, 11, 12], True)
            ],
            StructType([
                StructField("name", StringType(), True),
                StructField("nums", ArrayType(IntegerType(), True), True),
                StructField("any_num_greater_than_5", BooleanType(), True)
            ])
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_forall(self):
        source_df = spark.createDataFrame(
            [
                ("jose", [1, 2, 3]),
                ("li", [4, 5, 6]),
                ("luisa", [10, 11, 12]),
            ],
            StructType([
                StructField("name", StringType(), True),
                StructField("nums", ArrayType(IntegerType(), True), True),
            ])
        )

        actual_df = source_df.withColumn(
            "all_nums_greater_than_3",
            QF.forall(lambda n: n > 3)(col("nums"))
        )

        expected_df = spark.createDataFrame(
            [
                ("jose", [1, 2, 3], False),
                ("li", [4, 5, 6], True),
                ("luisa", [10, 11, 12], True)
            ],
            StructType([
                StructField("name", StringType(), True),
                StructField("nums", ArrayType(IntegerType(), True), True),
                StructField("all_nums_greater_than_3", BooleanType(), True)
            ])
        )

        assert(expected_df.collect() == actual_df.collect())
