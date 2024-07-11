import chispa
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import quinn

from .spark import spark


def describe_column_to_list():
    def it_returns_a_list():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        actual = quinn.column_to_list(source_df, "name")
        assert ["jose", "li", "luisa"] == actual


def describe_two_columns_to_dictionary():
    def it_returns_a_dictionary():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        actual = quinn.two_columns_to_dictionary(source_df, "name", "age")
        assert {"jose": 1, "li": 2, "luisa": 3} == actual


def describe_to_list_of_dictionaries():
    def returns_a_list_of_dicts():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        actual = quinn.to_list_of_dictionaries(source_df)
        expected = [
            {"name": "jose", "age": 1},
            {"name": "li", "age": 2},
            {"name": "luisa", "age": 3},
        ]
        assert expected == actual


def describe_show_output_to_df():
    def it_converts_a_show_string_to_a_dataframe():
        s = """+----+---+-----------+------+
|name|age|     stuff1|stuff2|
+----+---+-----------+------+
|jose|  1|nice person|  yoyo|
|  li|  2|nice person|  yoyo|
| liz|  3|nice person|  yoyo|
+----+---+-----------+------+"""
        actual_df = quinn.show_output_to_df(s, spark)
        expected_data = [
            ("jose", "1", "nice person", "yoyo"),
            ("li", "2", "nice person", "yoyo"),
            ("liz", "3", "nice person", "yoyo"),
        ]
        expected_df = spark.createDataFrame(
            expected_data, ["name", "age", "stuff1", "stuff2"],
        )
        chispa.assert_df_equality(expected_df, actual_df)


def test_create_df():
    rows_data = [("jose", 1), ("li", 2), ("luisa", 3)]
    col_specs = [("name", StringType()), ("age", IntegerType())]

    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ],
    )
    actual = quinn.create_df(spark, rows_data, col_specs)
    expected = spark.createDataFrame(rows_data, expected_schema)
    chispa.assert_df_equality(actual, expected)
