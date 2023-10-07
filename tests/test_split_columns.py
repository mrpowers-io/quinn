import quinn
from tests.conftest import auto_inject_fixtures
import chispa


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

    data = [(2025, "bio", "chris", "moe"),
            (2026, "physics", "david", "bb"),
            (2022, "bio", "sophia", "raul"),
            (2025, "physics", "fred", "li"),
            (2023, "math", "some", "person"),
            (2025, "physics", "li", "yao")]

    expected = spark.createDataFrame(data, ["graduation_year", "major", "student_first_name", "student_last_name"])

    chispa.assert_df_equality(new_df, expected)
