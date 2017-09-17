import pytest

from quinn.spark import *
from quinn.dataframe_validator import *

class TestDataFrameValidator(object):

    def test_validate_presence_of_columns_when_column_is_missing(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        with pytest.raises(DataFrameMissingColumnError) as excinfo:
            DataFrameValidator().validate_presence_of_columns(source_df, ["name", "age", "fun"])

    def test_validate_presence_of_columns_when_all_columns_are_present(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])
        DataFrameValidator().validate_presence_of_columns(source_df, ["name"])
