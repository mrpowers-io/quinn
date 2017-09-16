import pytest

from quinn.spark import *
from quinn.column_ext import *

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

class TestColumnExt(object):

    def test_is_falsy(self):
        data = [("jose", True), ("li", False), ("luisa", None)]
        source_df = spark.createDataFrame(data, ["name", "has_stuff"])

        actual_df = source_df.withColumn("is_stuff_falsy", col("has_stuff").isFalsy())

        expected_data = [("jose", True, False), ("li", False, True), ("luisa", None, True)]
        expected_df = spark.createDataFrame(expected_data, ["name", "has_stuff", "is_stuff_falsy"])

        assert(expected_df.collect() == actual_df.collect())

    def test_is_truthy(self):
        data = [("jose", True), ("li", False), ("luisa", None)]
        source_df = spark.createDataFrame(data, ["name", "has_stuff"])

        actual_df = source_df.withColumn("is_stuff_truthy", col("has_stuff").isTruthy())

        expected_data = [("jose", True, True), ("li", False, False), ("luisa", None, False)]
        expected_df = spark.createDataFrame(expected_data, ["name", "has_stuff", "is_stuff_truthy"])

        assert(expected_df.collect() == actual_df.collect())

    def test_is_null_or_blank(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("blah", StringType(), True)]
        )
        data = [("jose", ""), ("li", "   "), ("luisa", None), ("sam", "hi")]
        source_df = spark.createDataFrame(data, schema)

        actual_df = source_df.withColumn("is_blah_null_or_blank", col("blah").isNullOrBlank())

        expected_data = [("jose", "", True), ("li", "   ", True), ("luisa", None, True), ("sam", "hi", False)]
        expected_df = spark.createDataFrame(expected_data, ["name", "blah", "is_blah_null_or_blank"])

        assert(expected_df.collect() == actual_df.collect())

    def test_is_not_in(self):
        data = [("jose", "surfing"), ("li", "swimming"), ("luisa", "dancing")]
        source_df = spark.createDataFrame(data, ["name", "fun_thing"])

        bobs_hobbies = ["dancing", "snowboarding"]

        actual_df = source_df.withColumn("is_not_bobs_hobby", col("fun_thing").isNotIn(bobs_hobbies))

        expected_data = [("jose", "surfing", True), ("li", "swimming", True), ("luisa", "dancing", False)]
        expected_df = spark.createDataFrame(expected_data, ["name", "fun_thing", "is_not_bobs_hobby"])

        assert(expected_df.collect() == actual_df.collect())

