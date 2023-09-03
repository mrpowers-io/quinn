import pyspark.sql.functions as F
from pyspark.sql.types import StringType, BooleanType, IntegerType
from quinn.extensions import * # noqa
import chispa

from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def test_is_falsy(spark):
    source_df = spark.create_df(
        [(True, False), (False, True), (None, True)],
        [
            ("has_stuff", BooleanType(), True),
            ("expected", BooleanType(), True),
        ],
    )
    actual_df = source_df.withColumn("is_has_stuff_falsy", F.col("has_stuff").isFalsy())
    chispa.assert_column_equality(actual_df, "is_has_stuff_falsy", "expected")


def test_is_truthy(spark):
    source_df = spark.create_df(
        [(True, True), (False, False), (None, False)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn(
        "is_has_stuff_truthy", F.col("has_stuff").isTruthy()
    )
    chispa.assert_column_equality(actual_df, "is_has_stuff_truthy", "expected")


def test_is_false(spark):
    source_df = spark.create_df(
        [(True, False), (False, True), (None, None)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn("is_has_stuff_false", F.col("has_stuff").isFalse())
    chispa.assert_column_equality(actual_df, "is_has_stuff_false", "expected")


def test_is_true(spark):
    source_df = spark.create_df(
        [(True, True), (False, False), (None, None)],
        [("has_stuff", BooleanType(), True), ("expected", BooleanType(), True)],
    )
    actual_df = source_df.withColumn("is_stuff_true", F.col("has_stuff").isTrue())
    chispa.assert_column_equality(actual_df, "is_stuff_true", "expected")


def test_is_null_or_blank(spark):
    source_df = spark.create_df(
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
        "is_blah_null_or_blank", F.col("blah").isNullOrBlank()
    )
    chispa.assert_column_equality(actual_df, "is_blah_null_or_blank", "expected")


def test_is_not_in(spark):
    source_df = spark.create_df(
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
        "is_not_bobs_hobby", F.col("fun_thing").isNotIn(bobs_hobbies)
    )
    chispa.assert_column_equality(actual_df, "is_not_bobs_hobby", "expected")


def test_null_between(spark):
    source_df = spark.create_df(
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
        "is_between", F.col("age").nullBetween(F.col("lower_age"), F.col("upper_age"))
    )
    chispa.assert_column_equality(actual_df, "is_between", "expected")
