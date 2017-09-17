import pytest

from quinn.spark import *
from quinn.dataframe_validator import *

from pyspark.sql.types import StructType, StructField, StringType, LongType

class TestDataFrameValidator(object):

    def test_validate_presence_of_columns_when_column_is_missing(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(DataFrameMissingColumnError) as excinfo:
            DataFrameValidator().validate_presence_of_columns(source_df, ["name", "age", "fun"])
        assert excinfo.value.args[0] == "The ['fun'] columns are not included in the DataFrame with the following columns ['name', 'age']"

    def test_validate_presence_of_columns_when_all_columns_are_present(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        DataFrameValidator().validate_presence_of_columns(source_df, ["name"])

    def test_validate_schema_when_struct_field_is_missing(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType([
            StructField("name", StringType(), True),
            StructField("city", StringType(), True),
        ])
        with pytest.raises(DataFrameMissingStructFieldError) as excinfo:
            DataFrameValidator().validate_schema(source_df, required_schema)
        assert excinfo.value.args[0] == "The [StructField(city,StringType,true)] StructFields are not included in the DataFrame with the following StructFields StructType(List(StructField(name,StringType,true),StructField(age,LongType,true)))"

    def test_validate_schema_when_struct_field_is_missing(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        required_schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
        ])
        DataFrameValidator().validate_schema(source_df, required_schema)
