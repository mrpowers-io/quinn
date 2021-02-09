import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType

import quinn
from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def describe_validate_presence_of_columns():
    def it_raises_if_a_required_column_is_missing(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameMissingColumnError) as excinfo:
            quinn.validate_presence_of_columns(source_df, ["name", "age", "fun"])
        assert (
            excinfo.value.args[0]
            == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"
        )

    def it_does_nothing_if_all_required_columns_are_present(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_presence_of_columns(source_df, ["name"])


def describe_validate_schema():
    def it_raises_when_struct_field_is_missing1(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("city", StringType(), True),
            ]
        )
        with pytest.raises(quinn.DataFrameMissingStructFieldError) as excinfo:
            quinn.validate_schema(source_df, required_schema)
        assert (
            excinfo.value.args[0]
            == "The [StructField(city,StringType,true)] StructFields are not included in the DataFrame with the following StructFields StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))"
        )

    def it_does_nothing_when_the_schema_matches(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", LongType(), True),
            ]
        )
        quinn.validate_schema(source_df, required_schema)


def describe_validate_absence_of_columns():
    def it_raises_when_a_unallowed_column_is_present(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameProhibitedColumnError) as excinfo:
            quinn.validate_absence_of_columns(source_df, ["age", "cool"])
        assert (
            excinfo.value.args[0]
            == "The ['age'] columns are not allowed to be included in the DataFrame with the following columns ['name', 'age']"
        )

    def it_does_nothing_when_no_unallowed_columns_are_present(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_absence_of_columns(source_df, ["favorite_color"])
