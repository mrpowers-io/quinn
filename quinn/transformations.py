from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def with_columns_renamed(fun: Callable[[str], str]) -> Callable[[DataFrame], DataFrame]:
    def _(df: DataFrame) -> DataFrame:
        cols = list(
            map(
                lambda col_name: F.col("`{0}`".format(col_name)).alias(fun(col_name)),
                df.columns,
            )
        )
        return df.select(*cols)

    return _


def with_some_columns_renamed(
    fun: Callable[[str], str], change_col_name: Callable[[str], str]
) -> Callable[[DataFrame], DataFrame]:
    def _(df):
        cols = list(
            map(
                lambda col_name: F.col("`{0}`".format(col_name)).alias(fun(col_name))
                if change_col_name(col_name)
                else F.col("`{0}`".format(col_name)),
                df.columns,
            )
        )
        return df.select(*cols)

    return _


def snake_case_col_names(df: DataFrame) -> DataFrame:
    return with_columns_renamed(to_snake_case)(df)


def to_snake_case(s: str) -> str:
    return s.lower().replace(" ", "_")


def sort_columns(df: DataFrame, sort_order: str) -> DataFrame:
    sorted_col_names = None
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        raise ValueError(
            "['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
                sort_order=sort_order
            )
        )
    return df.select(*sorted_col_names)
