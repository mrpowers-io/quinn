from pyspark.sql.types import StructType, StructField, StringType

from tests.conftest import auto_inject_fixtures

import chispa


@auto_inject_fixtures('spark')


def test_create_df(spark):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("blah", StringType(), True)]
    )
    data = [("jose", "a"), ("li", "b"), ("sam", "c")]
    actual_df = spark.createDataFrame(data, schema)

    expected_df = spark.create_df(
        [("jose", "a"), ("li", "b"), ("sam", "c")],
        [("name", StringType(), True), ("blah", StringType(), True)]
    )

    chispa.assert_df_equality(expected_df, actual_df)
