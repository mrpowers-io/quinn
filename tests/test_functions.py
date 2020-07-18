from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

import quinn
from quinn.extensions import *
from tests.conftest import auto_inject_fixtures
import chispa


@auto_inject_fixtures('spark')

def test_single_space(spark):
    df = spark.create_df(
        [
            ("  I like     fish  ", "I like fish"),
            ("    zombies", "zombies"),
            ("simpsons   cat lady", "simpsons cat lady"),
            (None, None),
        ],
        [
            ("words", StringType(), True),
            ("expected", StringType(), True),
        ]
    )
    actual_df = df.withColumn(
        "words_single_spaced",
        quinn.single_space(col("words"))
    )
    chispa.assert_column_equality(actual_df, "words_single_spaced", "expected")


def test_remove_all_whitespace(spark):
    df = spark.create_df(
        [
            ("  I like     fish  ", "Ilikefish"),
            ("    zombies", "zombies"),
            ("simpsons   cat lady", "simpsonscatlady"),
            (None, None),
        ],
        [
            ("words", StringType(), True),
            ("expected", StringType(), True),
        ]
    )
    actual_df = df.withColumn(
        "words_without_whitespace",
        quinn.remove_all_whitespace(col("words"))
    )
    chispa.assert_column_equality(actual_df, "words_without_whitespace", "expected")


def test_remove_non_word_characters(spark):
    df = spark.create_df(
        [
            ("I?like!fish>", "Ilikefish"),
            ("%%%zombies", "zombies"),
            ("si%$#@!#$!@#mpsons", "simpsons"),
            (None, None),
        ],
        [
            ("words", StringType(), True),
            ("expected", StringType(), True),
        ]
    )
    actual_df = df.withColumn(
        "words_without_nonword_chars",
        quinn.remove_non_word_characters(col("words"))
    )
    chispa.assert_column_equality(actual_df, "words_without_nonword_chars", "expected")


def test_anti_trim(spark):
    df = spark.create_df(
        [
            ("  I like     fish  ", "  Ilikefish  "),
            ("    zombies", "    zombies"),
            ("  simpsons   cat lady   ", "  simpsonscatlady   "),
            (None, None),
        ],
        [
            ("words", StringType(), True),
            ("expected", StringType(), True),
        ]
    )
    actual_df = df.withColumn(
        "words_anti_trimmed",
        quinn.anti_trim(col("words"))
    )
    chispa.assert_column_equality(actual_df, "words_anti_trimmed", "expected")


def test_exists(spark):
    df = spark.createDataFrame(
        [
            ([1, 2, 3], False),
            ([4, 5, 6], True),
            ([10, 11, 12], True),
        ],
        StructType([
            StructField("nums", ArrayType(IntegerType(), True), True),
            StructField("expected", BooleanType(), True)
        ])
    )
    actual_df = df.withColumn(
        "any_num_greater_than_5",
        quinn.exists(lambda n: n > 5)(col("nums"))
    )
    chispa.assert_column_equality(actual_df, "any_num_greater_than_5", "expected")


def test_forall(spark):
    df = spark.createDataFrame(
        [
            ([1, 2, 3], False),
            ([4, 5, 6], True),
            ([10, 11, 12], True),
        ],
        StructType([
            StructField("nums", ArrayType(IntegerType(), True), True),
            StructField("expected", BooleanType(), True),
        ])
    )
    actual_df = df.withColumn(
        "all_nums_greater_than_3",
        quinn.forall(lambda n: n > 3)(col("nums"))
    )
    chispa.assert_column_equality(actual_df, "all_nums_greater_than_3", "expected")


def test_multi_equals(spark):
    df = spark.create_df(
        [
            ("cat", "cat", True),
            ("cat", "dog", False),
            ("pig", "pig", False),
            ("", "", False),
            (None, None, False)
        ],
        [
            ("s1", StringType(), True),
            ("s2", StringType(), True),
            ("expected", BooleanType(), True),
        ]
    )
    actual_df = df.withColumn(
        "are_s1_and_s2_cat",
        quinn.multi_equals("cat")(col("s1"), col("s2"))
    )
    chispa.assert_column_equality(actual_df, "are_s1_and_s2_cat", "expected")

