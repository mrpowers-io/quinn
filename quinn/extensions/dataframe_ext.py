import warnings
from pyspark.sql.dataframe import DataFrame

def _ext_function(self, f):
    warnings.warn(
        "Extensions may be removed in the future versions of quinn. Please use explicit functions instead",
        category=DeprecationWarning,
        stacklevel=2
    )
    return transform(self, f)

DataFrame.transform = getattr(DataFrame, "transform", _ext_function)                                  