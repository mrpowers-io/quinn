import warnings

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def _ext_function(spark: SparkSession, f: object) -> object:
    warnings.warn(
        "Extensions may be removed in the future versions of quinn. Please use explicit functions instead",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return f(spark)


DataFrame.transform = getattr(DataFrame, "transform", _ext_function)
