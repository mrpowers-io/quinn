import pytest

from quinn.spark import *
from quinn.column_ext import *
from quinn.spark_session_ext import *

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

class TestColumnExt(object):

    def test_is_falsy(self):
        source_df = spark.createDF(
            [
                ("jose", True),
                ("li", False),
                ("luisa", None)
            ],
            [
                ("name", StringType(), True),
                ("has_stuff", BooleanType(), True)
            ]
        )

        actual_df = source_df.withColumn("is_stuff_falsy", col("has_stuff").isFalsy())

        expected_df = spark.createDF(
            [
                ("jose", True, False),
                ("li", False, True),
                ("luisa", None, True)
            ],
            [
                ("name", StringType(), True),
                ("has_stuff", BooleanType(), True),
                ("is_stuff_falsy", BooleanType(), True)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_is_truthy(self):
        source_df = spark.createDF(
            [
                ("jose", True),
                ("li", False),
                ("luisa", None)
            ],
            [
                ("name", StringType(), True),
                ("has_stuff", BooleanType(), True)
            ]
        )

        actual_df = source_df.withColumn("is_stuff_truthy", col("has_stuff").isTruthy())

        expected_df = spark.createDF(
            [
                ("jose", True, True),
                ("li", False, False),
                ("luisa", None, False)
            ],
            [
                ("name", StringType(), True),
                ("has_stuff", BooleanType(), True),
                ("is_stuff_truthy", BooleanType(), True)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_is_null_or_blank(self):
        source_df = spark.createDF(
            [
                ("jose", ""),
                ("li", "   "),
                ("luisa", None),
                ("sam", "hi"),
            ],
            [
                ("name", StringType(), True),
                ("blah", StringType(), True),
            ]
        )

        actual_df = source_df.withColumn("is_blah_null_or_blank", col("blah").isNullOrBlank())

        expected_df = spark.createDF(
            [
                ("jose", "", True),
                ("li", "   ", True),
                ("luisa", None, True),
                ("sam", "hi", False),
            ],
            [
                ("name", StringType(), True),
                ("blah", StringType(), True),
                ("is_blah_null_or_blank", BooleanType(), True),
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_is_not_in(self):
        source_df = spark.createDF(
            [
                ("jose", "surfing"),
                ("li", "swimming"),
                ("luisa", "dancing"),
            ],
            [
                ("name", StringType(), True),
                ("fun_thing", StringType(), True),
            ]
        )

        bobs_hobbies = ["dancing", "snowboarding"]

        actual_df = source_df.withColumn("is_not_bobs_hobby", col("fun_thing").isNotIn(bobs_hobbies))

        expected_df = spark.createDF(
            [
                ("jose", "surfing", True),
                ("li", "swimming", True),
                ("luisa", "dancing", False),
            ],
            [
                ("name", StringType(), True),
                ("fun_thing", StringType(), True),
                ("is_not_bobs_hobby", BooleanType(), True),
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

