from pyspark.sql.types import IntegerType, StringType

import quinn
from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures('spark')
class TestDataFrameHelpers:

    def test_with_age_plus_two(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = self.spark.createDataFrame(data, ["name", "age"])

        actual = quinn.column_to_list(source_df, "name")

        assert ["jose", "li", "luisa"] == actual

    def test_two_columns_to_dictionary(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = self.spark.createDataFrame(data, ["name", "age"])

        actual = quinn.two_columns_to_dictionary(source_df, "name", "age")

        assert {"jose": 1, "li": 2, "luisa": 3} == actual

    def test_to_list_of_dictionaries(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        source_df = self.spark.createDataFrame(data, ["name", "age"])

        actual = quinn.to_list_of_dictionaries(source_df)

        expected = [
            {"name": "jose", "age": 1},
            {"name": "li", "age": 2},
            {"name": "luisa", "age": 3},
        ]

        assert expected == actual
    
    def test_print_athena_create_table(self, capsys):
        source_df = self.spark.createDataFrame(
          [
            ("jets", "football", 45),
            ("nacional", "soccer", 10)
          ],
          [
            "team","sport", "goals_for"
          ]
        )

        quinn.print_athena_create_table(source_df, "athena_table", "s3://mock")
        out, _ = capsys.readouterr()

        assert out == "CREATE EXTERNAL TABLE IF NOT EXISTS `athena_table` ( \n\t `team` string, \n\t `sport` string, \n\t `goals_for` bigint \n)\nSTORED AS PARQUET\nLOCATION 's3://mock'\n\n"
