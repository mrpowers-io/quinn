from typing import Callable

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame


def with_columns_renamed(fun: Callable[[str], str]) -> Callable[[DataFrame], DataFrame]:
    """Ffunction designed to rename the columns of a `Spark DataFrame`.

    It takes a `Callable[[str], str]` object as an argument (``fun``) and returns a
    `Callable[[DataFrame], DataFrame]` object.

    When `_()` is called on a `DataFrame`, it creates a list of column names,
    applying the argument `fun()` to each of them, and returning a new `DataFrame`
    with the new column names.

    :param fun: Renaming function
    :returns: Function which takes DataFrame as parameter.
    """

    def _(df: DataFrame) -> DataFrame:
        cols = [F.col(f"`{col_name}`").alias(fun(col_name)) for col_name in df.columns]
        return df.select(*cols)

    return _


def with_some_columns_renamed(
    fun: Callable[[str], str],
    change_col_name: Callable[[str], str],
) -> Callable[[DataFrame], DataFrame]:
    """Function that takes a `Callable[[str], str]` and a `Callable[[str], str]` and returns a `Callable[[DataFrame], DataFrame]`.

    Which in turn takes a `DataFrame` and returns a `DataFrame` with some of its columns renamed.

    :param fun: A function that takes a column name as a string and returns a
    new name as a string.
    :type fun: `Callable[[str], str]`
    :param change_col_name: A function that takes a column name as a string and
    returns a boolean.
    :type change_col_name: `Callable[[str], str]`
    :return: A `Callable[[DataFrame], DataFrame]`, which takes a
    `DataFrame` and returns a `DataFrame` with some of its columns renamed.
    :rtype: `Callable[[DataFrame], DataFrame]`
    """

    def _(df: DataFrame) -> DataFrame:
        cols = [
            F.col(f"`{col_name}`").alias(fun(col_name))
            if change_col_name(col_name)
            else F.col(f"`{col_name}`")
            for col_name in df.columns
        ]
        return df.select(*cols)

    return _


def snake_case_col_names(df: DataFrame) -> DataFrame:
    """Function takes a ``DataFrame`` instance and returns the same ``DataFrame`` instance with all column names converted to snake case.

    (e.g. ``col_name_1``). It uses the ``to_snake_case`` function in conjunction with
    the ``with_columns_renamed`` function to achieve this.
    :param df: A ``DataFrame`` instance to process
    :type df: ``DataFrame``
    :return: A ``DataFrame`` instance with column names converted to snake case
    :rtype: ``DataFrame``.
    """
    return with_columns_renamed(to_snake_case)(df)


def to_snake_case(s: str) -> str:
    """Takes a string and converts it to snake case format.

    :param s: The string to be converted.
    :type s: str
    :return: The string in snake case format.
    :rtype: str
    """
    return s.lower().replace(" ", "_")


def sort_columns(df: DataFrame, sort_order: str) -> DataFrame:
    """Function sorts the columns of a given DataFrame based on a given sort order.

    The ``sort_order`` parameter can either be ``asc`` or ``desc``, which correspond to
    ascending and descending order, respectively. If any other value is provided for
    the ``sort_order`` parameter, a ``ValueError`` will be raised.

    :param df: A DataFrame
    :type df: pandas.DataFrame
    :param sort_order: The order in which to sort the columns in the DataFrame
    :type sort_order: str
    :return: A DataFrame with the columns sorted in the chosen order
    :rtype: pandas.DataFrame
    """
    sorted_col_names = None
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        msg = f"['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'"
        raise ValueError(
            msg,
        )
    return df.select(*sorted_col_names)
