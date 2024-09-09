import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType
import semver

import quinn
from .spark import spark


def describe_validate_presence_of_columns():
    def it_raises_if_a_required_column_is_missing():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameMissingColumnError) as excinfo:
            quinn.validate_presence_of_columns(source_df, ["name", "age", "fun"])
        assert (
            excinfo.value.args[0]
            == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"
        )

    def it_does_nothing_if_all_required_columns_are_present():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_presence_of_columns(source_df, ["name"])


def describe_validate_schema():
    def it_raises_when_struct_field_is_missing1():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("city", StringType(), True),
            ]
        )
        with pytest.raises(quinn.DataFrameMissingStructFieldError) as excinfo:
            quinn.validate_schema(required_schema, df_to_be_validated=source_df)

        current_spark_version = semver.Version.parse(spark.version)
        spark_330 = semver.Version.parse("3.3.0")
        if semver.Version.compare(current_spark_version, spark_330) >= 0:  # Spark 3.3+
            expected_error_message = "The [StructField('city', StringType(), True)] StructFields are not included in the DataFrame with the following StructFields StructType([StructField('name', StringType(), True), StructField('age', LongType(), True)])"  # noqa
        else:
            expected_error_message = "The [StructField(city,StringType,true)] StructFields are not included in the DataFrame with the following StructFields StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))"  # noqa
        assert excinfo.value.args[0] == expected_error_message

    def it_does_nothing_when_the_schema_matches():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", LongType(), True),
            ]
        )
        quinn.validate_schema(required_schema, df_to_be_validated=source_df)

    def nullable_column_mismatches_are_ignored():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", LongType(), False),
            ]
        )
        quinn.validate_schema(required_schema, ignore_nullable=True, df_to_be_validated=source_df)


def describe_validate_absence_of_columns():
    def it_raises_when_a_unallowed_column_is_present():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameProhibitedColumnError) as excinfo:
            quinn.validate_absence_of_columns(source_df, ["age", "cool"])
        assert (
            excinfo.value.args[0]
            == "The ['age'] columns are not allowed to be included in the DataFrame with the following columns ['name', 'age']"  # noqa
        )

    def it_does_nothing_when_no_unallowed_columns_are_present():
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_absence_of_columns(source_df, ["favorite_color"])
