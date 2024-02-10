import pyspark.sql.functions as F
from pyspark.sql.types import StringType, BooleanType, IntegerType
import quinn
import chispa

from ..spark import spark
from quinn.extensions import *  # noqa


def test_is_falsy():
    source_df = quinn.create_df(
        spark,
        [(True, False), (False, True), (None, True)],
        [
            ("has_stuff", BooleanType(), True),
            ("expected", BooleanType(), True),
        ],
    )
    actual_df = source_df.withColumn("is_has_stuff_falsy", isFalsy(F.col("has_stuff")))
    chispa.assert_column_equality(actual_df, "is_has_stuff_falsy", "expected")


def test_is_truthy():
    source_df = quinn.create_df(
        spark,
        [(True, True), (False, False), (None, False)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn(
        "is_has_stuff_truthy", isTruthy(F.col("has_stuff"))
    )
    chispa.assert_column_equality(actual_df, "is_has_stuff_truthy", "expected")


def test_is_false():
    source_df = quinn.create_df(
        spark,
        [(True, False), (False, True), (None, None)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn("is_has_stuff_false", isFalse(F.col("has_stuff")))
    chispa.assert_column_equality(actual_df, "is_has_stuff_false", "expected")


def test_is_true():
    source_df = quinn.create_df(
        spark,
        [(True, True), (False, False), (None, None)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn("is_stuff_true", isTrue(F.col("has_stuff")))
    chispa.assert_column_equality(actual_df, "is_stuff_true", "expected")


def test_is_null_or_blank():
    source_df = quinn.create_df(
        spark,
        [
            ("", True),
            ("   ", True),
            (None, True),
            ("hi", False),
        ],
        [
            ("blah", StringType(), True),
            ("expected", BooleanType(), True),
        ],
    )
    actual_df = source_df.withColumn(
        "is_blah_null_or_blank", isNullOrBlank(F.col("blah"))
    )
    chispa.assert_column_equality(actual_df, "is_blah_null_or_blank", "expected")


def test_is_not_in():
    source_df = quinn.create_df(
        spark,
        [
            ("surfing", True),
            ("swimming", True),
            ("dancing", False),
        ],
        [
            ("fun_thing", StringType(), True),
            ("expected", BooleanType(), True),
        ],
    )
    bobs_hobbies = ["dancing", "snowboarding"]
    actual_df = source_df.withColumn(
        "is_not_bobs_hobby", isNotIn(F.col("fun_thing"), bobs_hobbies)
    )
    chispa.assert_column_equality(actual_df, "is_not_bobs_hobby", "expected")


def test_null_between():
    source_df = quinn.create_df(
        spark,
        [
            (17, None, 94, True),
            (17, None, 10, False),
            (None, 10, 5, True),
            (None, 10, 88, False),
            (10, 15, 11, True),
            (None, None, 11, False),
            (3, 5, None, False),
            (None, None, None, False),
        ],
        [
            ("lower_age", IntegerType(), True),
            ("upper_age", IntegerType(), True),
            ("age", IntegerType(), True),
            ("expected", BooleanType(), True),
        ],
    )
    actual_df = source_df.withColumn(
        "is_between", nullBetween(F.col("age"), (F.col("lower_age"), F.col("upper_age")))
    )
    chispa.assert_column_equality(actual_df, "is_between", "expected")
