import quinn
from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def test_split_columns(spark):
    # Create Spark DataFrame
    data = [("chrisXXmoe", 2025, "bio"),
            ("davidXXbb", 2026, "physics"),
            ("sophiaXXraul", 2022, "bio"),
            ("fredXXli", 2025, "physics"),
            ("someXXperson", 2023, "math"),
            ("liXXyao", 2025, "physics")]

    df = spark.createDataFrame(data, ["student_name", "graduation_year", "major"])
    # Define the delimiter
    delimiter = "XX"

    # New column names
    new_col_names = ["student_first_name", "student_last_name"]

    col_name = "student_name"
    mode = "strict"
    # Call split_col() function to split "student_name" column
    new_df = quinn.split_col(df, col_name, delimiter, new_col_names, mode)

    # Show the resulting DataFrame
    new_df.show()

    # Verify the resulting DataFrame has the expected columns and values
    assert set(new_df.columns) == set(["graduation_year", "major", "student_first_name", "student_last_name"])
    assert new_df.count() == 6
    assert new_df.filter("student_first_name = 'chris'").count() == 1
    assert new_df.filter("student_last_name = 'moe'").count() == 1

    col_name1 = "non_existent_column"
    # Verify that a ValueError is raised when calling split_col() with a non-existent column name
    assert quinn.split_col(df, col_name1, delimiter, new_col_names, mode) is not None, ValueError("Error: split_col "
                                                                                            "returned None")