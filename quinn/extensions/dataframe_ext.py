from typing import Callable

from pyspark.sql.dataframe import DataFrame


def transform(self: DataFrame, f: Callable[[DataFrame], DataFrame]) -> DataFrame:
    return f(self)


DataFrame.transform = getattr(DataFrame, "transform", transform)
