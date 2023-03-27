import pytest

import re

import pyspark.sql.functions as F
from pyspark.sql.types import *

import quinn
from quinn.extensions import *
from tests.conftest import auto_inject_fixtures
import chispa

import datetime


@auto_inject_fixtures("spark")
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
        ],
    )
    actual_df = df.withColumn("words_single_spaced", quinn.single_space(F.col("words")))
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
        ],
    )
    actual_df = df.withColumn(
        "words_without_whitespace", quinn.remove_all_whitespace(F.col("words"))
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
        ],
    )
    actual_df = df.withColumn(
        "words_without_nonword_chars", quinn.remove_non_word_characters(F.col("words"))
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
        ],
    )
    actual_df = df.withColumn("words_anti_trimmed", quinn.anti_trim(F.col("words")))
    chispa.assert_column_equality(actual_df, "words_anti_trimmed", "expected")


def test_exists(spark):
    df = spark.createDataFrame(
        [
            ([1, 2, 3], False),
            ([4, 5, 6], True),
            ([10, 11, 12], True),
        ],
        StructType(
            [
                StructField("nums", ArrayType(IntegerType(), True), True),
                StructField("expected", BooleanType(), True),
            ]
        ),
    )
    actual_df = df.withColumn(
        "any_num_greater_than_5", quinn.exists(lambda n: n > 5)(F.col("nums"))
    )
    chispa.assert_column_equality(actual_df, "any_num_greater_than_5", "expected")


def test_forall(spark):
    df = spark.createDataFrame(
        [
            ([1, 2, 3], False),
            ([4, 5, 6], True),
            ([10, 11, 12], True),
        ],
        StructType(
            [
                StructField("nums", ArrayType(IntegerType(), True), True),
                StructField("expected", BooleanType(), True),
            ]
        ),
    )
    actual_df = df.withColumn(
        "all_nums_greater_than_3", quinn.forall(lambda n: n > 3)(F.col("nums"))
    )
    chispa.assert_column_equality(actual_df, "all_nums_greater_than_3", "expected")


def test_multi_equals(spark):
    df = spark.create_df(
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
            ("expected", BooleanType(), True),
        ],
    )
    actual_df = df.withColumn(
        "are_s1_and_s2_cat", quinn.multi_equals("cat")(F.col("s1"), F.col("s2"))
    )
    chispa.assert_column_equality(actual_df, "are_s1_and_s2_cat", "expected")


def describe_week_start_date():
    def it_works_with_start_date_of_monday(spark):
        df = spark.create_df(
            [
                # converts a Thursday to the Monday before
                (datetime.datetime(2020, 1, 2), datetime.datetime(2019, 12, 30)),
                # converts a Wednesday to the Monday before
                (datetime.datetime(2020, 7, 15), datetime.datetime(2020, 7, 13)),
                # doesn't change if the day in a Monday
                (datetime.datetime(2020, 7, 20), datetime.datetime(2020, 7, 20)),
                (None, None),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        actual_df = df.withColumn(
            "week_start_date", quinn.week_start_date(F.col("some_date"), "Mon")
        )
        chispa.assert_column_equality(actual_df, "week_start_date", "expected")

    def it_defaults_to_sunday_start_date(spark):
        df = spark.create_df(
            [
                # converts a Tuesday to the Sunday before
                (datetime.datetime(2020, 1, 2), datetime.datetime(2019, 12, 29)),
                # converts a Wednesday to the Sunday before
                (datetime.datetime(2020, 7, 15), datetime.datetime(2020, 7, 12)),
                # doesn't change if the day is Sunday
                (datetime.datetime(2020, 7, 26), datetime.datetime(2020, 7, 26)),
                (None, None),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        actual_df = df.withColumn(
            "week_start_date", quinn.week_start_date(F.col("some_date"))
        )
        chispa.assert_column_equality(actual_df, "week_start_date", "expected")

    def it_errors_out_if_with_invalid_week_start_date(spark):
        df = spark.create_df(
            [
                (datetime.datetime(2020, 1, 2), datetime.datetime(2019, 12, 29)),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        with pytest.raises(ValueError) as excinfo:
            df.withColumn(
                "week_start_date", quinn.week_start_date(F.col("some_date"), "hello")
            )
        assert (
            excinfo.value.args[0]
            == "The day you entered 'hello' is not valid.  Here are the valid days: [Mon,Tue,Wed,Thu,Fri,Sat,Sun]"
        )


def describe_week_end_date():
    def it_works_with_end_date_of_sunday(spark):
        df = spark.create_df(
            [
                # converts a Thursday to the Sunday after
                (datetime.datetime(2020, 1, 2), datetime.datetime(2020, 1, 5)),
                # converts a Wednesday to the Sunday after
                (datetime.datetime(2020, 7, 15), datetime.datetime(2020, 7, 19)),
                # doesn't change if the day in a Sunday
                (datetime.datetime(2020, 7, 19), datetime.datetime(2020, 7, 19)),
                (None, None),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        actual_df = df.withColumn(
            "week_start_date", quinn.week_end_date(F.col("some_date"), "Sun")
        )
        chispa.assert_column_equality(actual_df, "week_start_date", "expected")

    def it_defaults_to_saturday_week_end(spark):
        df = spark.create_df(
            [
                # converts a Tuesday to the Saturday after
                (datetime.datetime(2020, 1, 2), datetime.datetime(2020, 1, 4)),
                # converts a Wednesday to the Saturday after
                (datetime.datetime(2020, 7, 15), datetime.datetime(2020, 7, 18)),
                # doesn't change if the day is Saturday
                (datetime.datetime(2020, 7, 25), datetime.datetime(2020, 7, 25)),
                (None, None),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        actual_df = df.withColumn(
            "week_start_date", quinn.week_end_date(F.col("some_date"))
        )
        chispa.assert_column_equality(actual_df, "week_start_date", "expected")

    def it_errors_out_if_with_invalid_week_end_date(spark):
        df = spark.create_df(
            [
                (datetime.datetime(2020, 1, 2), datetime.datetime(2019, 12, 29)),
            ],
            [("some_date", DateType(), True), ("expected", DateType(), True)],
        )
        with pytest.raises(ValueError) as excinfo:
            df.withColumn(
                "week_start_date", quinn.week_end_date(F.col("some_date"), "Friday")
            )
        assert (
            excinfo.value.args[0]
            == "The day you entered 'Friday' is not valid.  Here are the valid days: [Mon,Tue,Wed,Thu,Fri,Sat,Sun]"
        )


def describe_approx_equal():
    def it_works_with_floating_values(spark):
        df = spark.create_df(
            [
                (1.1, 1.05, True),
                (1.1, 11.6, False),
                (1.02, 1.09, True),
                (1.02, 1.34, False),
                (None, None, None),
            ],
            [
                ("num1", FloatType(), True),
                ("num2", FloatType(), True),
                ("expected", BooleanType(), True),
            ],
        )
        actual_df = df.withColumn(
            "are_nums_approx_equal",
            quinn.approx_equal(F.col("num1"), F.col("num2"), F.lit(0.1)),
        )
        chispa.assert_column_equality(actual_df, "are_nums_approx_equal", "expected")

    def it_works_with_integer_values(spark):
        df = spark.create_df(
            [
                (12, 14, True),
                (20, 26, False),
                (44, 41, True),
                (32, 9, False),
                (None, None, None),
            ],
            [
                ("num1", IntegerType(), True),
                ("num2", IntegerType(), True),
                ("expected", BooleanType(), True),
            ],
        )
        actual_df = df.withColumn(
            "are_nums_approx_equal",
            quinn.approx_equal(F.col("num1"), F.col("num2"), F.lit(5)),
        )
        chispa.assert_column_equality(actual_df, "are_nums_approx_equal", "expected")


def test_array_choice(spark):
    df = spark.create_df(
        [(["a", "b", "c"],), (["a", "b", "c", "d"],), (["x"],), ([None],)],
        [("letters", ArrayType(StringType(), True), True)],
    )
    actual_df = df.withColumn("random_letter", quinn.array_choice(F.col("letters")))
    # actual_df.show()
    # chispa.assert_column_equality(actual_df, "are_nums_approx_equal", "expected")

    # df = spark.createDataFrame([('a',), ('b',), ('c',)], ['letter'])
    # df.show()
    # cols = list(map(lambda c: F.lit(c), ['Retail', 'SME', 'Cor']))
    # df.withColumn('business_vertical', quinn.array_choice(F.array(cols))).show()


def test_regexp_extract_all(spark):
    df = spark.create_df(
        [("200 - 300 PA.", ["200", "300"]), ("400 PA.", ["400"]), (None, None)],
        [
            ("str", StringType(), True),
            ("expected", ArrayType(StringType(), True), True),
        ],
    )
    actual_df = df.withColumn(
        "all_numbers", quinn.regexp_extract_all(F.col("str"), F.lit(r"(\d+)"))
    )
    chispa.assert_column_equality(actual_df, "all_numbers", "expected")


def test_flatten_structs(spark):
    data = [
        (1, ("name1", "address1", 20)),
        (2, ("name2", "address2", 30)),
        (3, ("name3", "address3", 40)),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField(
                "details",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("address", StringType(), True),
                        StructField("age", IntegerType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(data, schema)
    complex_fields = {"details": StructType([StructField("name", StringType(), True), StructField("address", StringType(), True), StructField("age", IntegerType(), True)])}
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("details:name", StringType(), True),
            StructField("details:address", StringType(), True),
            StructField("details:age", IntegerType(), True),
        ]
    )
    expected_data = [
        (1, "name1", "address1", 20),
        (2, "name2", "address2", 30),
        (3, "name3", "address3", 40),
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    flattened_df = flatten_struct(df, "details")
    assert flattened_df.schema == expected_schema
    assert flattened_df.collect() == expected_df.collect()


def test_flatten_maps(spark):
    data = [
        (1, {"name": "Alice", "age": 25}),
        (2, {"name": "Bob", "age": 30}),
        (3, {"name": "Charlie", "age": 35}),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("details", MapType(StringType(), StringType()), True),
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("details_name", StringType(), True),
            StructField("details_age", StringType(), True),
        ]
    )
    expected_data = [
        (1, "Alice", "25"),
        (2, "Bob", "30"),
        (3, "Charlie", "35"),
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    flattened_df = flatten_map(df, "details")
    assert flattened_df.schema == expected_schema
    assert flattened_df.collect() == expected_df.collect()    
    
    
    

    
    
def test_flatten_dataframe(spark):

    # Define input data
    data = [
        (
            1,
            "John",
            {"age": 30, "gender": "M", "address": {"city": "New York", "state": "NY"}},
            [
                {"type": "home", "number": "555-1234"},
                {"type": "work", "number": "555-5678"},
            ],
        ),
        (
            2,
            "Jane",
            {"age": 25, "gender": "F", "address": {"city": "San Francisco", "state": "CA"}},
            [{"type": "home", "number": "555-4321"}],
        ),
    ]
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField(
                "details",
                StructType(
                    [
                        StructField("age", IntegerType(), True),
                        StructField("gender", StringType(), True),
                        StructField(
                            "address",
                            StructType(
                                [
                                    StructField("city", StringType(), True),
                                    StructField("state", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            StructField(
                "phone_numbers",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType(), True),
                            StructField("number", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    df = spark.createDataFrame(data, schema)

    # Define expected output
    expected_data = [
        (1, "John", 30, "M", "New York", "NY", "home", "555-1234"),
        (1, "John", 30, "M", "New York", "NY", "work", "555-5678"),
        (2, "Jane", 25, "F", "San Francisco", "CA", "home", "555-4321"),
    ]
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("details_age", IntegerType(), True),
            StructField("details_gender", StringType(), True),
            StructField("details_address_city", StringType(), True),
            StructField("details_address_state", StringType(), True),
            StructField("phone_numbers_type", StringType(), True),
            StructField("phone_numbers_number", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Apply function to input data
    result_df = flatten_dataframe(df)

    # Check if result matches expected output
    assert result_df.collect() == expected_df.collect()