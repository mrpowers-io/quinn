import pytest
from pyspark.sql.types import StructType, StructField, StringType

import quinn
from tests.conftest import auto_inject_fixtures
import chispa


@auto_inject_fixtures('spark')


def test_modify_column_names(spark):
    def dots_to_underscores(s):
        return s.replace(".", "_")
    schema = StructType([
        StructField("i.like.cheese", StringType(), True),
        StructField("yummy.stuff", StringType(), True)]
    )
    data = [("jose", "a"), ("li", "b"), ("sam", "c")]
    source_df = spark.createDataFrame(data, schema)
    actual_df = quinn.modify_column_names(dots_to_underscores)(source_df)
    expected_df = spark.create_df(
        [
            ("jose", "a"),
            ("li", "b"),
            ("sam", "c")
        ],
        [
            ("i_like_cheese", StringType(), True),
            ("yummy_stuff", StringType(), True)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)



def test_snake_case_col_names(spark):
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
            ("yummmmy_stuff", StringType(), True)
        ]
    )
    chispa.assert_df_equality(actual_df, expected_df)


def test_sort_columns_asc(spark):
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_sort_columns_desc(spark):
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
    chispa.assert_df_equality(actual_df, expected_df)


def test_sort_columns_exception(spark):
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

