from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import quinn
from .spark import spark
from chispa.schema_comparer import assert_basic_schema_equality
from quinn.append_if_schema_identical import SchemaMismatchError


def test_append_if_schema_identical():
    source_data = [(1, "cape town", "Alice"), (2, "delhi", "Bob")]
    target_data = [(3, "Charlie", "New York"), (4, "Dave", "Los Angeles")]
    bad_data = [(5, "Eve", "London", "extra_column")]

    source_df = spark.createDataFrame(
        source_data,
        schema=StructType(
            [
                StructField("id", IntegerType()),
                StructField("city", StringType()),
                StructField("name", StringType()),
            ]
        ),
    )

    target_df = spark.createDataFrame(
        target_data,
        schema=StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("city", StringType()),
            ]
        ),
    )

    unidentical_df = spark.createDataFrame(
        bad_data,
        schema=StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("city", StringType()),
                StructField("extra", StringType()),
            ]
        ),
    )

    check_if_error_caught = False
    expected_names = ["Charlie", "Dave", "Alice", "Bob"]
    expected_cities = ["New York", "Los Angeles", "cape town", "delhi"]

    # Call the append_if_schema_identical function
    result = quinn.append_if_schema_identical(source_df, target_df)

    # check result content
    names = [i.name for i in result.select("name").collect()]
    cities = [i.city for i in result.select("city").collect()]

    if result.count() != 4:
        raise AssertionError("result should have 4 rows")

    if names != expected_names:
        raise AssertionError("result should have the correct names")

    if cities != expected_cities:
        raise AssertionError("result should have the correct cities")

    assert_basic_schema_equality(target_df.schema, result.schema)

    try:
        quinn.append_if_schema_identical(source_df, unidentical_df)
    except SchemaMismatchError:
        check_if_error_caught = True

    if not check_if_error_caught:
        raise AssertionError(
            "append_if_schema_identical should raise an error if the schemas are not identical"
        )
