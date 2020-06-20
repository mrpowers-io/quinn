from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

import quinn
from quinn.extensions import *
from tests.conftest import auto_inject_fixtures
import chispa


@auto_inject_fixtures('spark')

def test_single_space(spark):
    source_df = spark.create_df(
        [
            ("  I like     fish  ", 1),
            ("    zombies", 2),
            ("simpsons   cat lady", 2),
            (None, 3),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
        ]
    )
    actual_df = source_df.withColumn(
        "words_single_spaced",
        quinn.single_space(col("words"))
    )
    expected_df = spark.create_df(
        [
            ("  I like     fish  ", 1, "I like fish"),
            ("    zombies", 2, "zombies"),
            ("simpsons   cat lady", 2, "simpsons cat lady"),
            (None, 3, None),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
            ("words_single_spaced", StringType(), True),
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_remove_all_whitespace(spark):
    source_df = spark.create_df(
        [
            ("  I like     fish  ", 1),
            ("    zombies", 2),
            ("simpsons   cat lady", 2),
            (None, 3),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
        ]
    )
    actual_df = source_df.withColumn(
        "words_without_whitespace",
        quinn.remove_all_whitespace(col("words"))
    )
    expected_df = spark.create_df(
        [
            ("  I like     fish  ", 1, "Ilikefish"),
            ("    zombies", 2, "zombies"),
            ("simpsons   cat lady", 2, "simpsonscatlady"),
            (None, 3, None),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
            ("words_without_whitespace", StringType(), True),
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_remove_non_word_characters(spark):
    source_df = spark.create_df(
        [
            ("I?like!fish>", 1),
            ("%%%zombies", 2),
            ("si%$#@!#$!@#mpsons", 2),
            (None, 3),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
        ]
    )
    actual_df = source_df.withColumn(
        "words_without_nonword_chars",
        quinn.remove_non_word_characters(col("words"))
    )
    expected_df = spark.create_df(
        [
            ("I?like!fish>", 1, "Ilikefish"),
            ("%%%zombies", 2, "zombies"),
            ("si%$#@!#$!@#mpsons", 2, "simpsons"),
            (None, 3, None),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
            ("words_without_nonword_chars", StringType(), True),
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_anti_trim(spark):
    source_df = spark.create_df(
        [
            ("  I like     fish  ", 1),
            ("    zombies", 2),
            ("  simpsons   cat lady   ", 2),
            (None, 3),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
        ]
    )
    actual_df = source_df.withColumn(
        "words_anti_trimmed",
        quinn.anti_trim(col("words"))
    )
    expected_df = spark.create_df(
        [
            ("  I like     fish  ", 1, "  Ilikefish  "),
            ("    zombies", 2, "    zombies"),
            ("  simpsons   cat lady   ", 2, "  simpsonscatlady   "),
            (None, 3, None),
        ],
        [
            ("words", StringType(), True),
            ("num", IntegerType(), True),
            ("words_anti_trimmed", StringType(), True),
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_exists(spark):
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
        quinn.exists(lambda n: n > 5)(col("nums"))
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_forall(spark):
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
        quinn.forall(lambda n: n > 3)(col("nums"))
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_multi_equals(spark):
    source_df = spark.create_df(
        [
            ("cat", "cat"),
            ("cat", "dog"),
            ("pig", "pig"),
            ("", ""),
            (None, None)
        ],
        [
            ("s1", StringType(), True),
            ("s2", StringType(), True)
        ]
    )
    actual_df = source_df.withColumn(
        "are_s1_and_s2_cat",
        quinn.multi_equals("cat")(col("s1"), col("s2"))
    )
    expected_df = spark.create_df(
        [
            ("cat", "cat", True),
            ("cat", "dog", False),
            ("pig", "pig", False),
            ("", "", False),
            (None, None, False),
        ],
        [
            ("s1", StringType(), True),
            ("s2", StringType(), True),
            ("are_s1_and_s2_cat", BooleanType(), True),
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)
