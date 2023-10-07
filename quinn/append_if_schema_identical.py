from pyspark.sql import DataFrame


class SchemaMismatchError(ValueError):
    """raise this when there's a schema mismatch between source & target schema."""


def append_if_schema_identical(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    """Compare the schema of source & target dataframe.

    :param source_df: Input DataFrame
    :type source_df: pyspark.sql.DataFrame
    :param target_df: Input DataFrame
    :type target_df: pyspark.sql.DataFrame
    :return: dataframe
    :rtype: pyspark.sql.DataFrame
    """
    # Retrieve the schemas of the source and target dataframes
    source_schema = source_df.schema
    target_schema = target_df.schema

    # Convert the schemas to a list of tuples
    source_schema_list = [(field.name, str(field.dataType)) for field in source_schema]
    target_schema_list = [(field.name, str(field.dataType)) for field in target_schema]

    unmatched_cols = [
        col for col in source_schema_list if col not in target_schema_list
    ]
    error_message = (
        f"The schemas of the source and target dataframes are not identical."
        f"From source schema column {unmatched_cols} is missing in target schema"
    )
    # Check if the column names in the source and target schemas are the same, regardless of their order
    if set(source_schema.fieldNames()) != set(target_schema.fieldNames()):
        raise SchemaMismatchError(error_message)
    # Check if the column names and data types in the source and target schemas are the same, in the same order
    if sorted(source_schema_list) != sorted(target_schema_list):
        raise SchemaMismatchError(error_message)

    # Append the dataframes if the schemas are identical
    return target_df.unionByName(source_df)
