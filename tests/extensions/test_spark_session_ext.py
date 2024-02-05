from pyspark.sql.types import StructType, StructField, StringType

from ..spark import spark

import chispa
import quinn


def test_create_df():
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("blah", StringType(), True),
        ]
    )
    data = [("jose", "a"), ("li", "b"), ("sam", "c")]
    actual_df = spark.createDataFrame(data, schema)

    expected_df = quinn.create_df(
        spark,
        [("jose", "a"), ("li", "b"), ("sam", "c")],
        [("name", StringType(), True), ("blah", StringType(), True)],
    )

    chispa.assert_df_equality(expected_df, actual_df)
