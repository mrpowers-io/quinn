from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


class SchemaMismatchError(ValueError):
    """raise this when there's a schema mismatch between source & target schema"""


def append_if_schema_identical(source_df: DataFrame, target_df: DataFrame):
    # Retrieve the schemas of the source and target dataframes
    source_schema = source_df.schema
    target_schema = target_df.schema

    # Convert the schemas to a list of tuples
    source_schema_list = [(field.name, str(field.dataType)) for field in source_schema]
    target_schema_list = [(field.name, str(field.dataType)) for field in target_schema]

    unmatched_cols = [col for col in source_schema_list if col not in target_schema_list]

    print(source_schema_list)
    print(type(source_schema_list[0][0]))
    print(type(target_schema_list[0][0]))
    print(target_schema_list)
    error_message = f"The schemas of the source and target dataframes are not identical.From source schema column {unmatched_cols} is missing in target schema"
    expected_schema = target_schema_list
    # Compare the two schema lists
    if source_schema_list == target_schema_list:
        # Append the dataframes if the schemas are identical
        appended_df = source_df.union(target_df)
        return appended_df
    else:
        # Raise an error if the schemas are different
        raise SchemaMismatchError(error_message)
