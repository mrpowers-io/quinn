import pytest

from quinn.spark import *
import quinn

from pyspark.sql.types import StructType, StructField, StringType, LongType

class TestDataFrameValidator:

    def test_validate_presence_of_columns_when_column_is_missing(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameMissingColumnError) as excinfo:
            quinn.validate_presence_of_columns(source_df, ["name", "age", "fun"])
        assert excinfo.value.args[0] == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"

    def test_validate_presence_of_columns_when_all_columns_are_present(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_presence_of_columns(source_df, ["name"])

    def test_validate_schema_when_struct_field_is_missing1(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType([
            StructField("name", StringType(), True),
            StructField("city", StringType(), True),
        ])
        with pytest.raises(quinn.DataFrameMissingStructFieldError) as excinfo:
            quinn.validate_schema(source_df, required_schema)
        assert excinfo.value.args[0] == "The [StructField(city,StringType,true)] StructFields are not included in the DataFrame with the following StructFields StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))"

    def test_validate_schema_when_struct_field_is_missing2(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
        ])
        quinn.validate_schema(source_df, required_schema)

    def test_validate_absence_of_columns_when_column_is_present(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(quinn.DataFrameProhibitedColumnError) as excinfo:
            quinn.validate_absence_of_columns(source_df, ["age", "cool"])
        assert excinfo.value.args[0] == "The ['age'] columns are not allowed to be included in the DataFrame with the following columns ['name', 'age']"

    def test_validate_absence_of_columns_when_column_isnt_present(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        quinn.validate_absence_of_columns(source_df, ["favorite_color"])
