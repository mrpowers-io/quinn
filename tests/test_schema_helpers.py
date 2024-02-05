from __future__ import annotations

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

from quinn.schema_helpers import print_schema_as_code, schema_from_csv, complex_fields

from chispa.schema_comparer import assert_basic_schema_equality
import pytest

# from tests.conftest import auto_inject_fixtures
from .spark import *


# @auto_inject_fixtures("spark")
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


def test_schema_from_csv_good_schema1(spark):
    expected_schema = StructType([
        StructField("person", StringType(), False, {"description": "The person's name"}),
        StructField("address", StringType(), True),
        StructField("phoneNumber", StringType(), True, {"description": "The person's phone number"}),
        StructField("age", IntegerType(), False),
    ])
    path = 'tests/test_files/good_schema1.csv'
    assert_basic_schema_equality(expected_schema, schema_from_csv(spark, path))


def test_schema_from_csv_good_schema2(spark):
    expected_schema = StructType([
        StructField("person", StringType(), True),
        StructField("address", StringType(), True),
        StructField("phoneNumber", StringType(), True),
        StructField("age", IntegerType(), True),
    ])
    path = 'tests/test_files/good_schema2.csv'
    assert_basic_schema_equality(expected_schema, schema_from_csv(spark, path))


def test_schema_from_csv_equality_for_bad_csv(spark):
    path = 'tests/test_files/bad_schema.csv'
    with pytest.raises(ValueError) as excinfo:
        schema_from_csv(spark, path)
    assert (
            excinfo.value.args[0]
            == "CSV must contain columns in this order: ['name', 'type', 'nullable', 'metadata']"
    )


def test_complex_fields(spark):
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
    expected = {
        "details":
        StructType(
            [
                StructField("name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
    }
    assert complex_fields(schema) == expected
