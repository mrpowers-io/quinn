import pytest

from quinn.spark import *
from quinn.extensions import *
import quinn

from pyspark.sql.types import StructType, StructField, StringType, BooleanType


class TestTransformations(object):

    def test_snake_case_col_names(self):
        schema = StructType([
            StructField("I like CHEESE", StringType(), True),
            StructField("YUMMMMY stuff", StringType(), True)]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)

        actual_df = quinn.snake_case_col_names(source_df)

        expected_df = spark.create_df(
            [
                ("jose", "a"),
                ("li", "b"),
                ("sam", "c")
            ],
            [
                ("i_like_cheese", StringType(), True),
                ("YUMMMMY_stuff", StringType(), True)
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_sort_columns_asc(self):
        source_df = spark.create_df(
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ]
        )

        actual_df = quinn.sort_columns(source_df, "asc")

        expected_df = spark.create_df(
            [
                ("switch", "jose", "oak"),
                ("xbox", "li", "redwood"),
                ("ps4", "luisa", "maple"),
            ],
            [
                ("gaming_system", StringType(), True),
                ("name", StringType(), True),
                ("tree", StringType(), True),
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_sort_columns_desc(self):
        source_df = spark.create_df(
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ]
        )

        actual_df = quinn.sort_columns(source_df, "desc")

        expected_df = spark.create_df(
            [
                ("oak", "jose", "switch"),
                ("redwood", "li", "xbox"),
                ("maple", "luisa", "ps4"),
            ],
            [
                ("tree", StringType(), True),
                ("name", StringType(), True),
                ("gaming_system", StringType(), True),
            ]
        )

        assert(expected_df.collect() == actual_df.collect())

    def test_sort_columns_exception(self):
        source_df = spark.create_df(
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ]
        )

        with pytest.raises(ValueError) as excinfo:
            quinn.sort_columns(source_df, "cats")
        assert excinfo.value.args[0] == "['asc', 'desc'] are the only valid sort orders and you entered a sort order of 'cats'"
