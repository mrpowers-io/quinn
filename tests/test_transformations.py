import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType

import quinn
from tests.conftest import auto_inject_fixtures
import chispa
from quinn.transformations import flatten_struct, flatten_map, flatten_dataframe


@auto_inject_fixtures("spark")
def describe_with_columns_renamed():
    def it_renames_spaces_to_underscores(spark):
        def spaces_to_underscores(s):
            return s.replace(" ", "_")

        schema = StructType(
            [
                StructField("i like cheese", StringType(), True),
                StructField("yummy stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_columns_renamed(spaces_to_underscores)(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_renames_dots_to_underscores(spark):
        def dots_to_underscores(s):
            return s.replace(".", "_")

        schema = StructType(
            [
                StructField("i.like.cheese", StringType(), True),
                StructField("yummy.stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_columns_renamed(dots_to_underscores)(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_with_some_columns_renamed():
    def it_renames_columns_based_on_a_map(spark):
        mapping = {"chips": "french_fries", "petrol": "gas"}

        def british_to_american(s):
            return mapping[s]

        def change_col_name(s):
            return s in mapping

        schema = StructType(
            [
                StructField("chips", StringType(), True),
                StructField("hi", StringType(), True),
                StructField("petrol", StringType(), True),
            ]
        )
        data = [("potato", "hola!", "disel")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_some_columns_renamed(
            british_to_american, change_col_name
        )(source_df)
        expected_df = quinn.create_df(
            spark,
            [("potato", "hola!", "disel")],
            [
                ("french_fries", StringType(), True),
                ("hi", StringType(), True),
                ("gas", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_renames_some_columns_with_dots(spark):
        def dots_to_underscores(s):
            return s.replace(".", "_")

        def change_col_name(s):
            return s.startswith("a")

        schema = StructType(
            [
                StructField("a.person", StringType(), True),
                StructField("a.thing", StringType(), True),
                StructField("b.person", StringType(), True),
            ]
        )
        data = [("frank", "hot dog", "mia")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.with_some_columns_renamed(
            dots_to_underscores, change_col_name
        )(source_df)
        expected_df = quinn.create_df(
            spark,
            [("frank", "hot dog", "mia")],
            [
                ("a_person", StringType(), True),
                ("a_thing", StringType(), True),
                ("b.person", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_snake_case_col_names():
    def it_snake_cases_col_names(spark):
        schema = StructType(
            [
                StructField("I like CHEESE", StringType(), True),
                StructField("YUMMMMY stuff", StringType(), True),
            ]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        source_df = spark.createDataFrame(data, schema)
        actual_df = quinn.snake_case_col_names(source_df)
        expected_df = quinn.create_df(
            spark,
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [
                ("i_like_cheese", StringType(), True),
                ("yummmmy_stuff", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)


def describe_sort_columns():
    def it_sorts_columns_in_asc_order(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        actual_df = quinn.sort_columns(source_df, "asc")
        expected_df = quinn.create_df(
            spark,
            [
                ("switch", "jose", "oak"),
                ("xbox", "li", "redwood"),
                ("ps4", "luisa", "maple"),
            ],
            [
                ("gaming_system", StringType(), True),
                ("name", StringType(), True),
                ("tree", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_sorts_columns_in_desc_order(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        actual_df = quinn.sort_columns(source_df, "desc")
        expected_df = quinn.create_df(
            spark,
            [
                ("oak", "jose", "switch"),
                ("redwood", "li", "xbox"),
                ("maple", "luisa", "ps4"),
            ],
            [
                ("tree", StringType(), True),
                ("name", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        chispa.assert_df_equality(actual_df, expected_df)

    def it_throws_an_error_if_the_sort_order_is_invalid(spark):
        source_df = quinn.create_df(
            spark,
            [
                ("jose", "oak", "switch"),
                ("li", "redwood", "xbox"),
                ("luisa", "maple", "ps4"),
            ],
            [
                ("name", StringType(), True),
                ("tree", StringType(), True),
                ("gaming_system", StringType(), True),
            ],
        )
        with pytest.raises(ValueError) as excinfo:
            quinn.sort_columns(source_df, "cats")
        assert (
            excinfo.value.args[0]
            == "['asc', 'desc'] are the only valid sort orders and you entered a sort order of 'cats'"
        )


def test_flatten_struct(spark):
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
    flattened_df = flatten_struct(df, "details")
    expected_data = [
        (1, "name1", "address1", 20),
        (2, "name2", "address2", 30),
        (3, "name3", "address3", 40),
    ]
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("details:name", StringType(), True),
            StructField("details:address", StringType(), True),
            StructField("details:age", IntegerType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    chispa.assert_df_equality(flattened_df, expected_df)


def test_flatten_map(spark):
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
    flattened_df = flatten_map(df, "details")
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("details:name", StringType(), True),
            StructField("details:age", StringType(), True),
        ]
    )
    expected_data = [
        (1, "Alice", "25"),
        (2, "Bob", "30"),
        (3, "Charlie", "35"),
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    chispa.assert_df_equality(flattened_df, expected_df)


def test_flatten_dataframe(spark):
    # Define input data
    data = [
        (
            1,
            "John",
            {"age": 30, "gender": "M", "address": {"city": "New York", "state": "NY"}},
        ),
        (
            2,
            "Jane",
            {"age": 25, "gender": "F", "address": {"city": "San Francisco", "state": "CA"}},
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
        ]
    )
    df = spark.createDataFrame(data, schema)
    expected_data = [
        (1, "John", 30, "M", "New York", "NY"),
        (2, "Jane", 25, "F", "San Francisco", "CA"),
    ]
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("details:age", IntegerType(), True),
            StructField("details:gender", StringType(), True),
            StructField("details:address:city", StringType(), True),
            StructField("details:address:state", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    result_df = flatten_dataframe(df)
    chispa.assert_df_equality(result_df, expected_df)
