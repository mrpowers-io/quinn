from pyspark.sql.types import (
    StructType,
    IntegerType,
    DecimalType,
    ArrayType,
    FloatType,
    MapType,
    StringType,
    DoubleType,
    TimestampType,
    StructField,
)
import pyspark.sql.dataframe

from quinn.schema_helpers import print_schema_as_code, schema_from_csv

from chispa.schema_comparer import assert_basic_schema_equality

from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def test_print_schema_as_code(spark):
    fields = []
    fields.append(StructField("simple_int", IntegerType()))
    fields.append(StructField("decimal_with_nums", DecimalType(19, 8)))
    fields.append(StructField("array", ArrayType(FloatType())))
    fields.append(StructField("map", MapType(StringType(), ArrayType(DoubleType()))))
    fields.append(
        StructField(
            "struct",
            StructType(
                [
                    StructField("first", StringType()),
                    StructField("second", TimestampType()),
                ]
            ),
        )
    )

    schema = StructType(fields=fields)

    assert_basic_schema_equality(schema, eval(print_schema_as_code(schema)))


@auto_inject_fixtures("spark")
def test_schema_from_csv_equality(spark):
    """
    Input: tests/test_files/schema_from_csv_test_file.csv
    Tests that schema_from_csv() returns the expected schema from the provided schema configuration
    """
    expected_schema_fields: list = [
        StructField("person", StringType(), False, {"description": "The person's name"}),
        StructField("address", StringType(), True, {"description": "The person's address"}),
        StructField("phoneNumber", StringType(), True, {"description": "The person's phone number"}),
        StructField("age", IntegerType(), False, {"description": "The person's age"}),
    ]
    expected_schema = StructType(fields=expected_schema_fields)

    test_file_path = 'tests/test_files/schema_from_csv_test_file.csv'
    parsed_schema = schema_from_csv(spark, test_file_path)

    assert_basic_schema_equality(expected_schema, parsed_schema)


@auto_inject_fixtures("spark")
def test_schema_from_csv_validation(spark):
    """
    Input: tests/test_files/schema_from_csv_test_file.csv
    Tests
     1. schema_from_csv() successfully creates a dataframe for data that adheres to the parsed schema
     2. raises an exception when the data does not adhere to the parsed schema
    """

    def _check_good_df(spark, schema) -> None:
        data = [
            ('Alice', '123 Apple Lane', '111-222-3333', 27),
            ('Bob', '456 Berry Drive', '444-555-6666', 28)
        ]

        good_df = spark.createDataFrame(data, schema=schema)
        check_creation = isinstance(good_df, pyspark.sql.dataframe.DataFrame)

        if not check_creation:
            raise ValueError("Expected good dataframe to pass validation")

    def _check_bad_df(spark, schema) -> None:
        # expected to fail because of the string age values
        data = [
            ('Alice', '123 Apple Lane', '111-222-3333', '27'),
            ('Bob', None, None, '28')
        ]
        check_if_data_error_caught = False

        try:
            spark.createDataFrame(data, schema=schema)
        except TypeError:
            check_if_data_error_caught = True

        if not check_if_data_error_caught:
            raise ValueError("Expected bad dataframe to fail validation")

    schema = schema_from_csv(spark, 'tests/test_files/schema_from_csv_test_file.csv')
    _check_good_df(spark, schema)
    _check_bad_df(spark, schema)
