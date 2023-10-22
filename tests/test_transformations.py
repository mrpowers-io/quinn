import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)

import quinn
from pyspark.sql import DataFrame
from tests.conftest import auto_inject_fixtures
import chispa
import chispa.schema_comparer


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


def _test_sort_struct_flat(spark, sort_order: str):
    def _get_simple_test_dataframes(sort_order) -> tuple[(DataFrame, DataFrame)]:
        col_a = 1
        col_b = 2
        col_c = 3

        unsorted_fields = StructType(
            [
                StructField("b", IntegerType()),
                StructField("c", IntegerType()),
                StructField("a", IntegerType()),
            ]
        )
        unsorted_data = [
            (col_b, col_c, col_a),
        ]
        if sort_order == "asc":
            sorted_fields = StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("c", IntegerType()),
                ]
            )

            sorted_data = [
                (col_a, col_b, col_c),
            ]
        elif sort_order == "desc":
            sorted_fields = StructType(
                [
                    StructField("c", IntegerType()),
                    StructField("b", IntegerType()),
                    StructField("a", IntegerType()),
                ]
            )

            sorted_data = [
                (col_c, col_b, col_a),
            ]
        else:
            raise ValueError(
                "['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
                    sort_order=sort_order
                )
            )

        unsorted_df = spark.createDataFrame(unsorted_data, unsorted_fields)
        expected_df = spark.createDataFrame(sorted_data, sorted_fields)

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_simple_test_dataframes(sort_order=sort_order)
    sorted_df = quinn.sort_columns(unsorted_df, sort_order)

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=True
    )


def test_sort_struct_flat(spark):
    _test_sort_struct_flat(spark, "asc")


def test_sort_struct_flat_desc(spark):
    _test_sort_struct_flat(spark, "desc")


def _get_test_dataframes_schemas() -> dict:
    elements = {
        "_id": (StructField("_id", StringType(), nullable=False)),
        "first_name": (StructField("first_name", StringType(), nullable=False)),
        "city": (StructField("city", StringType(), nullable=False)),
        "last4": (StructField("last4", IntegerType(), nullable=True)),
        "first5": (StructField("first5", IntegerType(), nullable=True)),
        "type": (StructField("type", StringType(), nullable=True)),
        "number": (StructField("number", StringType(), nullable=True)),
    }

    return elements


def _get_test_dataframes_data() -> tuple[(str, str, int, int, str)]:
    _id = "12345"
    city = "Fake City"
    zip_first5 = 54321
    zip_last4 = 12345
    first_name = "John"

    return _id, city, zip_first5, zip_last4, first_name


def _get_unsorted_nested_struct_fields(elements: dict):
    unsorted_fields = [
        elements["_id"],
        elements["first_name"],
        StructField(
            "address",
            StructType(
                [
                    StructField(
                        "zip",
                        StructType([elements["last4"], elements["first5"]]),
                        nullable=True,
                    ),
                    elements["city"],
                ]
            ),
            nullable=True,
        ),
    ]
    return unsorted_fields


def _test_sort_struct_nested(spark, ignore_nullable: bool):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        elements = _get_test_dataframes_schemas()
        unsorted_fields = _get_unsorted_nested_struct_fields(elements)
        sorted_fields = [
            elements["_id"],
            StructField(
                "address",
                StructType(
                    [
                        elements["city"],
                        StructField(
                            "zip",
                            StructType([elements["first5"], elements["last4"]]),
                            nullable=True,
                        ),
                    ]
                ),
                nullable=True,
            ),
            elements["first_name"],
        ]

        _id, city, zip_first5, zip_last4, first_name = _get_test_dataframes_data()
        unsorted_data = [
            (_id, first_name, (((zip_last4, zip_first5)), city)),
            (_id, first_name, (((None, zip_first5)), city)),
            (_id, first_name, (None)),
        ]

        sorted_data = [
            (_id, ((city, (zip_first5, zip_last4))), first_name),
            (_id, ((city, (zip_first5, None))), first_name),
            (_id, (None), first_name),
        ]

        unsorted_df = spark.createDataFrame(unsorted_data, StructType(unsorted_fields))
        expected_df = spark.createDataFrame(sorted_data, StructType(sorted_fields))

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()
    sorted_df = quinn.sort_columns(unsorted_df, "asc", sort_nested=True)

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=ignore_nullable
    )


def _test_sort_struct_nested_desc(spark, ignore_nullable: bool):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        elements = _get_test_dataframes_schemas()
        unsorted_fields = _get_unsorted_nested_struct_fields(elements)

        sorted_fields = [
            elements["first_name"],
            StructField(
                "address",
                StructType(
                    [
                        StructField(
                            "zip",
                            StructType([elements["last4"], elements["first5"]]),
                            nullable=True,
                        ),
                        elements["city"],
                    ]
                ),
                nullable=True,
            ),
            elements["_id"],
        ]

        _id, city, zip_first5, zip_last4, first_name = _get_test_dataframes_data()

        unsorted_data = [(_id, first_name, (((zip_last4, zip_first5)), city))]
        sorted_data = [
            (
                first_name,
                ((zip_first5, zip_last4), city),
                _id,
            )
        ]

        unsorted_df = spark.createDataFrame(unsorted_data, StructType(unsorted_fields))
        expected_df = spark.createDataFrame(sorted_data, StructType(sorted_fields))

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()
    sorted_df = quinn.sort_columns(unsorted_df, "desc")

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=ignore_nullable
    )


def _get_unsorted_nested_array_fields(elements: dict) -> list:
    unsorted_fields = [
        StructField(
            "address",
            StructType(
                [
                    StructField(
                        "zip",
                        StructType(
                            [
                                elements["last4"],
                                elements["first5"],
                            ]
                        ),
                        nullable=False,
                    ),
                    elements["city"],
                ]
            ),
            nullable=False,
        ),
        StructField(
            "phone_numbers",
            ArrayType(StructType([elements["type"], elements["number"]])),
            nullable=True,
        ),
        elements["_id"],
        elements["first_name"],
    ]
    return unsorted_fields


def _test_sort_struct_nested_with_arraytypes(spark, ignore_nullable: bool):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        elements = _get_test_dataframes_schemas()
        unsorted_fields = _get_unsorted_nested_array_fields(elements)

        sorted_fields = [
            elements["_id"],
            StructField(
                "address",
                StructType(
                    [
                        elements["city"],
                        StructField(
                            "zip",
                            StructType([elements["first5"], elements["last4"]]),
                            nullable=False,
                        ),
                    ]
                ),
                nullable=False,
            ),
            elements["first_name"],
            StructField(
                "phone_numbers",
                ArrayType(StructType([elements["number"], elements["type"]])),
                nullable=True,
            ),
        ]

        _id, city, zip_first5, zip_last4, first_name = _get_test_dataframes_data()
        phone_type = "home"
        phone_number = "555-555-5555"

        unsorted_data = [
            (
                ((zip_last4, zip_first5), city),
                [(phone_type, phone_number)],
                _id,
                first_name,
            ),
            (((zip_last4, zip_first5), city), [(phone_type, None)], _id, first_name),
            (((None, None), city), None, _id, first_name),
        ]
        sorted_data = [
            (
                _id,
                (city, (zip_last4, zip_first5)),
                first_name,
                [(phone_type, phone_number)],
            ),
            (_id, (city, (zip_last4, zip_first5)), first_name, [(phone_type, None)]),
            (_id, (city, (None, None)), first_name, None),
        ]
        unsorted_df = spark.createDataFrame(unsorted_data, StructType(unsorted_fields))
        expected_df = spark.createDataFrame(sorted_data, StructType(sorted_fields))

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()
    sorted_df = quinn.sort_columns(unsorted_df, "asc", sort_nested=True)

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable
    )


def _test_sort_struct_nested_with_arraytypes_desc(spark, ignore_nullable: bool):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        elements = _get_test_dataframes_schemas()
        unsorted_fields = _get_unsorted_nested_array_fields(elements)

        sorted_fields = [
            StructField(
                "phone_numbers",
                ArrayType(StructType([elements["type"], elements["number"]])),
                nullable=True,
            ),
            elements["first_name"],
            StructField(
                "address",
                StructType(
                    [
                        StructField(
                            "zip",
                            StructType([elements["last4"], elements["first5"]]),
                            nullable=False,
                        ),
                        elements["city"],
                    ]
                ),
                nullable=False,
            ),
            elements["_id"],
        ]

        _id, city, zip_first5, zip_last4, first_name = _get_test_dataframes_data()
        phone_type = "home"
        phone_number = "555-555-5555"

        unsorted_data = [
            (
                ((zip_last4, zip_first5), city),
                [(phone_type, phone_number)],
                _id,
                first_name,
            ),
        ]
        sorted_data = [
            (
                [(phone_type, phone_number)],
                first_name,
                ((zip_last4, zip_first5), city),
                _id,
            ),
        ]

        unsorted_df = spark.createDataFrame(unsorted_data, StructType(unsorted_fields))
        expected_df = spark.createDataFrame(sorted_data, StructType(sorted_fields))

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()
    sorted_df = quinn.sort_columns(unsorted_df, "desc", sort_nested=True)

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=ignore_nullable
    )


def _test_sort_struct_nested_in_arraytypes(spark, ignore_nullable: bool):
    def _get_test_dataframes() -> tuple[(DataFrame, DataFrame)]:
        elements = _get_test_dataframes_schemas()
        unsorted_fields = _get_unsorted_nested_array_fields(elements)

        # extensions = StructType(
        #     [
        #         StructField("extension_code", StringType(), nullable=True),
        #         StructField(
        #             "extension_numbers",
        #             StructType(
        #                 [
        #                     StructField("extension_number_one", IntegerType()),
        #                     StructField("extension_number_two", IntegerType()),
        #                 ]
        #             ),
        #         ),
        #     ]
        # )

        sorted_fields = [
            StructField(
                "phone_numbers",
                ArrayType(StructType([elements["type"], elements["number"]])),
            ),
            StructField(
                "extensions",
                ArrayType(
                    StructType(
                        [
                            StructField("extension_number_one", IntegerType()),
                            StructField("extension_number_two", IntegerType()),
                        ]
                    ),
                    StructField("extension_code", StringType(), nullable=True),
                ),
            ),
            elements["first_name"],
            StructField(
                "address",
                StructType(
                    [
                        StructField(
                            "zip",
                            StructType([elements["last4"], elements["first5"]]),
                            nullable=False,
                        ),
                        elements["city"],
                    ]
                ),
                nullable=False,
            ),
            elements["_id"],
        ]

        _id, city, zip_first5, zip_last4, first_name = _get_test_dataframes_data()
        phone_type = "home"
        phone_number = "555-555-5555"
        extension_code = "test"
        extension_number_one = 1
        extension_number_two = 2

        unsorted_data = [
            (
                ((zip_last4, zip_first5), city),
                [(phone_type, phone_number)],
                _id,
                first_name,
            ),
        ]
        sorted_data = [
            (
                [(phone_type, phone_number)],
                [(extension_number_one, extension_number_two), extension_code],
                first_name,
                ((zip_last4, zip_first5), city),
                _id,
            ),
        ]

        expected_df = spark.createDataFrame(sorted_data, StructType(sorted_fields))
        unsorted_df = spark.createDataFrame(unsorted_data, StructType(unsorted_fields))

        return unsorted_df, expected_df

    unsorted_df, expected_df = _get_test_dataframes()
    sorted_df = quinn.sort_columns(unsorted_df, "desc", sort_nested=True)

    chispa.schema_comparer.assert_schema_equality(
        sorted_df.schema, expected_df.schema, ignore_nullable=ignore_nullable
    )


def test_sort_struct_nested(spark):
    _test_sort_struct_nested(spark, True)


def test_sort_struct_nested_desc(spark):
    _test_sort_struct_nested_desc(spark, True)


def test_sort_struct_nested_with_arraytypes(spark):
    _test_sort_struct_nested_with_arraytypes(spark, True)


def test_sort_struct_nested_with_arraytypes_desc(spark):
    _test_sort_struct_nested_with_arraytypes_desc(spark, True)


def test_sort_struct_nested_nullable(spark):
    _test_sort_struct_nested(spark, False)


def test_sort_struct_nested_nullable_desc(spark):
    _test_sort_struct_nested_desc(spark, False)


def test_sort_struct_nested_with_arraytypes_nullable(spark):
    _test_sort_struct_nested_with_arraytypes(spark, False)


def test_sort_struct_nested_with_arraytypes_nullable_desc(spark):
    _test_sort_struct_nested_with_arraytypes_desc(spark, False)


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
_test_sort_struct_nested_with_arraytypes(spark, False)
