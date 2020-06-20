import pyspark.sql.functions as F
from pyspark.sql.types import StringType, BooleanType, IntegerType
from quinn.extensions import *
import chispa

from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures('spark')


def test_is_falsy(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_stuff_falsy", F.col("has_stuff").isFalsy())
    expected_df = spark.create_df(
        [
            ("jose", True, False),
            ("li", False, True),
            ("luisa", None, True)
        ],
        [
            ("name", StringType(), True),
            ("has_stuff", BooleanType(), True),
            ("is_stuff_falsy", BooleanType(), False)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_is_truthy(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_stuff_truthy", F.col("has_stuff").isTruthy())
    expected_df = spark.create_df(
        [
            ("jose", True, True),
            ("li", False, False),
            ("luisa", None, False)
        ],
        [
            ("name", StringType(), True),
            ("has_stuff", BooleanType(), True),
            ("is_stuff_truthy", BooleanType(), False)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_is_false(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_stuff_false", F.col("has_stuff").isFalse())
    expected_df = spark.create_df(
         [
            ("jose", True, False),
            ("li", False, True),
            ("luisa", None, None)
        ],
        [
            ("name", StringType(), True),
            ("has_stuff", BooleanType(), True),
            ("is_stuff_false", BooleanType(), True)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_is_true(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_stuff_true", F.col("has_stuff").isTrue())
    expected_df = spark.create_df(
        [
            ("jose", True, True),
            ("li", False, False),
            ("luisa", None, None)
        ],
        [
            ("name", StringType(), True),
            ("has_stuff", BooleanType(), True),
            ("is_stuff_true", BooleanType(), True)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_is_null_or_blank(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_blah_null_or_blank", F.col("blah").isNullOrBlank())
    expected_df = spark.create_df(
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_is_not_in(spark):
    source_df = spark.create_df(
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
    actual_df = source_df.withColumn("is_not_bobs_hobby", F.col("fun_thing").isNotIn(bobs_hobbies))
    expected_df = spark.create_df(
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_null_between(spark):
    source_df = spark.create_df(
        [
             (17, None, 94),
             (17, None, 10),
             (None, 10, 5),
             (None, 10, 88),
             (10, 15, 11),
             (None, None, 11),
             (3, 5, None),
             (None, None, None),
        ],
        [
             ("lower_age", IntegerType(), True),
             ("upper_age", IntegerType(), True),
             ("age", IntegerType(), True)
        ]
    )
    actual_df = source_df.withColumn(
        "is_between",
        F.col("age").nullBetween(F.col("lower_age"), F.col("upper_age"))
    )
    expected_df = spark.create_df(
        [
            (17, None, 94, True),
            (17, None, 10, False),
            (None, 10, 5, True),
            (None, 10, 88, False),
            (10, 15, 11, True),
            (None, None, 11, False),
            (3, 5, None, False),
            (None, None, None, False)
        ],
        [
            ("lower_age", IntegerType(), True),
            ("upper_age", IntegerType(), True),
            ("age", IntegerType(), True),
            ("is_between", BooleanType(), True)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)

