import re
from typing import Callable

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, MapType, StructType

from quinn.schema_helpers import complex_fields


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


def flatten_struct(df: DataFrame, col_name: str, separator: str = ":") -> DataFrame:
    """Flattens the specified StructType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param col_name: The column name of the StructType to be flattened.
    :type col_name: str
    :param separator: The separator to use in the resulting flattened column names, defaults to ':'.
    :type separator: str, optional
    :return: The DataFrame with the flattened StructType column.
    :rtype: List[Column]
    """
    struct_type = complex_fields(df.schema)[col_name]
    expanded = [
        F.col(f"`{col_name}`.`{k}`").alias(col_name + separator + k)
        for k in [n.name for n in struct_type.fields]
    ]
    return df.select("*", *expanded).drop(F.col(f"`{col_name}`"))


def flatten_map(df: DataFrame, col_name: str, separator: str = ":") -> DataFrame:
    """Flattens the specified MapType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param col_name: The column name of the MapType to be flattened.
    :type col_name: str
    :param separator: The separator to use in the resulting flattened column names, defaults to ":".
    :type separator: str, optional
    :return: The DataFrame with the flattened MapType column.
    :rtype: DataFrame
    """
    keys_df = df.select(F.explode_outer(F.map_keys(F.col(f"`{col_name}`")))).distinct()
    keys = [row[0] for row in keys_df.collect()]
    key_cols = [
        F.col(f"`{col_name}`").getItem(k).alias(col_name + separator + k) for k in keys
    ]
    return df.select(
        [F.col(f"`{col}`") for col in df.columns if col != col_name] + key_cols,
    )

def flatten_dataframe(
    df: DataFrame,
    separator: str = ":",
    replace_char: str = "_",
    sanitized_columns: bool = False,  # noqa: FBT001, FBT002
) -> DataFrame:
    """Flattens the complex columns in the DataFrame.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param separator: The separator to use in the resulting flattened column names, defaults to ":".
    :type separator: str, optional
    :param replace_char: The character to replace special characters with in column names, defaults to "_".
    :type replace_char: str, optional
    :param sanitized_columns: Whether to sanitize column names, defaults to False.
    :type sanitized_columns: bool, optional
    :return: The DataFrame with all complex data types flattened.
    :rtype: DataFrame

    .. note:: This function assumes the input DataFrame has a consistent schema across all rows. If you have files with
        different schemas, process each separately instead.

    .. example:: Example usage:

        >>> data = [
                (
                    1,
                    ("Alice", 25),
                    {"A": 100, "B": 200},
                    ["apple", "banana"],
                    {"key": {"nested_key": 10}},
                    {"A#": 1000, "B@": 2000},
                ),
                (
                    2,
                    ("Bob", 30),
                    {"A": 150, "B": 250},
                    ["orange", "grape"],
                    {"key": {"nested_key": 20}},
                    {"A#": 1500, "B@": 2500},
                ),
            ]

        >>> df = spark.createDataFrame(data)
        >>> flattened_df = flatten_dataframe(df)
        >>> flattened_df.show()
        >>> flattened_df_with_hyphen = flatten_dataframe(df, replace_char="-")
        >>> flattened_df_with_hyphen.show()
    """
    def sanitize_column_name(name: str, rc: str = "_") -> str:
        """Sanitizes column names by replacing special characters with the specified character.

        :param name: The original column name.
        :type name: str
        :param rc: The character to replace special characters with, defaults to '_'.
        :type rc: str, optional
        :return: The sanitized column name.
        :rtype: str
        """
        return re.sub(r"[^a-zA-Z0-9_]", rc, name)

    def explode_array(df: DataFrame, col_name: str) -> DataFrame:
        """Explodes the specified ArrayType column in the input DataFrame and returns a new DataFrame with the exploded column.

        :param df: The input PySpark DataFrame.
        :type df: DataFrame
        :param col_name: The column name of the ArrayType to be exploded.
        :type col_name: str
        :return: The DataFrame with the exploded ArrayType column.
        :rtype: DataFrame
        """
        return df.select("*", F.explode_outer(F.col(f"`{col_name}`")).alias(col_name)).drop(
            col_name,
        )

    fields = complex_fields(df.schema)

    while len(fields) != 0:
        col_name = next(iter(fields.keys()))

        if isinstance(fields[col_name], StructType):
            df = flatten_struct(df, col_name, separator)  # noqa: PD901

        elif isinstance(fields[col_name], ArrayType):
            df = explode_array(df, col_name)  # noqa: PD901

        elif isinstance(fields[col_name], MapType):
            df = flatten_map(df, col_name, separator)  # noqa: PD901

        fields = complex_fields(df.schema)

    # Sanitize column names with the specified replace_char
    if sanitized_columns:
        sanitized_columns = [
            sanitize_column_name(col_name, replace_char) for col_name in df.columns
        ]
        df = df.toDF(*sanitized_columns)  # noqa: PD901

    return df

