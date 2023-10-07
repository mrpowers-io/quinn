import warnings

from pyspark.sql.dataframe import DataFrame
from typing_extensions import Self


def _ext_function(self: Self, f: object) -> object:
    warnings.warn(
        "Extensions may be removed in the future versions of quinn. Please use explicit functions instead",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return f(self)


DataFrame.transform = getattr(DataFrame, "transform", _ext_function)
