import pytest

from spark import *
import quinn

class TestAssertionHelpers:

    def test_assert_column_equality_when_equal(self):
        data = [("jose", 1, 1), ("li", 2, 2), ("luisa", 3, 3)]
        source_df = spark.createDataFrame(data, ["name", "col1", "col2"])
        quinn.assert_column_equality(source_df, "col1", "col2")

    def test_assert_column_equality_when_different(self):
        data = [("jose", 1, 1), ("li", 2, 2), ("luisa", 3, 5)]
        source_df = spark.createDataFrame(data, ["name", "col1", "col2"])

        with pytest.raises(quinn.ColumnMismatchError) as excinfo:
            quinn.assert_column_equality(source_df, "col1", "col2")
        assert excinfo.value.args[0] == "The values of col1 column are different from col2"

    def test_assert_column_equality_equal_with_none(self):
        data = [("jose", None, None), ("li", 2, 2), ("luisa", 3, 3)]
        source_df = spark.createDataFrame(data, ["name", "col1", "col2"])
        quinn.assert_column_equality(source_df, "col1", "col2")

    def test_assert_column_equality_not_equal_with_none(self):
        data = [("jose", None, 0), ("li", 2, 2), ("luisa", 3, 3)]
        source_df = spark.createDataFrame(data, ["name", "col1", "col2"])

        with pytest.raises(quinn.ColumnMismatchError) as excinfo:
            quinn.assert_column_equality(source_df, "col1", "col2")
        assert excinfo.value.args[0] == "The values of col1 column are different from col2"

    def test_assert_column_equality_equal_with_text(self):
        data = [("jose", "", ""), ("li", "student", "student"), ("luisa", "teacher", "teacher")]
        source_df = spark.createDataFrame(data, ["name", "col1", "col2"])
        quinn.assert_column_equality(source_df, "col1", "col2")

