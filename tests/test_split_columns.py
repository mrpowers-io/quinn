import quinn
import chispa
import pytest
from .spark import spark


def test_split_columns():
    data = [("chrisXXmoe", 2025, "bio"),
            ("davidXXbb", 2026, "physics"),
            (None, 2025, "physics")]
    df = spark.createDataFrame(data, ["student_name", "graduation_year", "major"])
    new_df = quinn.split_col(
        df,
        col_name="student_name",
        delimiter="XX",
        new_col_names=["student_first_name", "student_last_name"],
        mode="permissive")
    data = [(2025, "bio", "chris", "moe"),
            (2026, "physics", "david", "bb"),
            (2025, "physics", None, None)]
    expected = spark.createDataFrame(data, ["graduation_year", "major", "student_first_name", "student_last_name"])
    chispa.assert_df_equality(new_df, expected)

def test_split_columns_advanced():
    data = [("chrisXXsomethingXXmoe", 2025, "bio"),
            ("davidXXbb", 2026, "physics"),
            (None, 2025, "physics")]
    df = spark.createDataFrame(data, ["student_name", "graduation_year", "major"])
    new_df = quinn.split_col(
        df,
        col_name="student_name",
        delimiter="XX",
        new_col_names=["student_first_name", "student_middle_name", "student_last_name"],
        mode="permissive")
    data = [(2025, "bio", "chris", "something", "moe"),
            (2026, "physics", "david", "bb", None),
            (2025, "physics", None, None, None)]
    expected = spark.createDataFrame(data, ["graduation_year", "major", "student_first_name", "student_middle_name", "student_last_name"])
    chispa.assert_df_equality(new_df, expected)

def test_split_columns_strict():
    data = [("chrisXXsomethingXXmoe", 2025, "bio"),
            ("davidXXbb", 2026, "physics"),
            (None, 2025, "physics")]
    df = spark.createDataFrame(data, ["student_name", "graduation_year", "major"])
    df2 = quinn.split_col(
        df,
        col_name="student_name",
        delimiter="XX",
        new_col_names=["student_first_name", "student_middle_name", "student_last_name"],
        mode="strict", default="hi")
    with pytest.raises(Exception): # there is no way to make it work for all the versions
        df2.show()
