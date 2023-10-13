import pytest
from pyspark.sql.types import StructType, StructField, StringType, LongType
import semver

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

        current_spark_version = semver.Version.parse(spark.version)
        spark_330 = semver.Version.parse("3.3.0")
        if semver.Version.compare(current_spark_version, spark_330) >= 0:  # Spark 3.3+
            expected_error_message = "The [StructField('city', StringType(), True)] StructFields are not included in the DataFrame with the following StructFields StructType([StructField('name', StringType(), True), StructField('age', LongType(), True)])" # noqa
        else:
            expected_error_message = "The [StructField(city,StringType,true)] StructFields are not included in the DataFrame with the following StructFields StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))"  # noqa
        assert excinfo.value.args[0] == expected_error_message

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

    def nullable_column_mismatches_are_ignored(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", LongType(), False),
            ]
        )
        quinn.validate_schema(source_df, required_schema, ignore_nullable=True)


def describe_validate_absence_of_columns():
    def it_raises_when_a_unallowed_column_is_present(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameProhibitedColumnError) as excinfo:
            quinn.validate_absence_of_columns(source_df, ["age", "cool"])
        assert (
            excinfo.value.args[0]
            == "The ['age'] columns are not allowed to be included in the DataFrame with the following columns ['name', 'age']"  # noqa
        )

    def it_does_nothing_when_no_unallowed_columns_are_present(spark):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_absence_of_columns(source_df, ["favorite_color"])


@pytest.fixture(scope="session")
def sample_schema(spark):
    return StructType([
        StructField("col1", StringType(), False),
        StructField("col2", LongType(), False)
    ])


@pytest.fixture(scope='session')
def sample_df(spark):
    return spark.createDataFrame(
        [
            ("A", 1),
            ("B", 2)
        ], ["col1", "col2"]
    )


@pytest.fixture(scope='session')
def missing_col_df(spark):
    return spark.createDataFrame([(1,)], ["col1"])


@pytest.fixture(scope='session')
def extra_col_df(spark):
    return spark.createDataFrame([("A", 1, "C")], ["col1", "col2", "extra_col"])


def test_validate_returned_schema_positive(sample_schema, sample_df):
    @quinn.validate_returned_schema(sample_schema)
    def get_df():
        return sample_df
    get_df()


def test_validate_returned_schema_negative(sample_schema, missing_col_df):
    @quinn.validate_returned_schema(sample_schema)
    def get_wrong_df():
        return missing_col_df
    with pytest.raises(quinn.DataFrameMissingStructFieldError):
        get_wrong_df()


def test_ensure_columns_present_positive(sample_df):
    @quinn.ensure_columns_present(["col1", "col2"])
    def get_df():
        return sample_df
    get_df()


def test_ensure_columns_present_negative(missing_col_df):
    @quinn.ensure_columns_present(["col1", "col2"])
    def get_wrong_df():
        return missing_col_df
    with pytest.raises(quinn.DataFrameMissingColumnError):
        get_wrong_df()

def test_ensure_columns_absent_positive(sample_df):
    @quinn.ensure_columns_absent(["extra_col"])
    def get_df():
        return sample_df
    get_df()


def test_ensure_columns_absent_negative(extra_col_df):
    @quinn.ensure_columns_absent(["extra_col"])
    def get_wrong_df():
        return extra_col_df
    with pytest.raises(quinn.DataFrameProhibitedColumnError):
        get_wrong_df()