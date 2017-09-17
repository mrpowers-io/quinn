import pytest

from quinn.spark import *
from quinn.dataframe_helpers import DataFrameHelpers

class TestDataFrameHelpers(object):

    def test_with_age_plus_two(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual = DataFrameHelpers().column_to_list(source_df, "name")

        assert(["jose", "li", "luisa"] == actual)

    def test_two_columns_to_dictionary(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual = DataFrameHelpers().two_columns_to_dictionary(source_df, "name", "age")

        assert({"jose": 1, "li": 2, "luisa": 3} == actual)
