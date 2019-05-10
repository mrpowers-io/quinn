from pyspark.sql.types import StructType, StructField, StringType

from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures('spark')
class TestSparkSessionExt:

    def test_create_df(self):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("blah", StringType(), True)]
        )
        data = [("jose", "a"), ("li", "b"), ("sam", "c")]
        actual_df = self.spark.createDataFrame(data, schema)

        expected_df = self.spark.create_df(
            [("jose", "a"), ("li", "b"), ("sam", "c")],
            [("name", StringType(), True), ("blah", StringType(), True)]
        )

        assert expected_df.collect() == actual_df.collect()
