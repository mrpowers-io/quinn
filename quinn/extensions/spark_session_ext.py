from __future__ import annotations

import warnings

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType


def create_df(
    spark: SparkSession,
    rows_data: list[tuple],
    col_specs: list[tuple],
) -> DataFrame:
    """Creates a new DataFrame from the given data and column specs.

    The returned DataFrame is created using the StructType and StructField classes provided by PySpark.

    :param rows_data: the data used to create the DataFrame
    :type rows_data: array-like
    :param col_specs: list of tuples containing the name and type of the field
    :type col_specs: list of tuples
    :return: a new DataFrame
    :rtype: DataFrame
    """
    warnings.warn(
        "Extensions may be removed in the future versions of quinn. Please use `quinn.create_df()` instead",
        category=DeprecationWarning,
        stacklevel=2,
    )

    struct_fields = [StructField(*x) for x in col_specs]
    return spark.createDataFrame(data=rows_data, schema=StructType(struct_fields))


SparkSession.create_df = create_df
