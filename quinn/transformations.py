from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructField, StructType


def with_columns_renamed(fun: Callable[[str], str]) -> Callable[[DataFrame], DataFrame]:
    """This is a function designed to rename the columns of a
    `Spark DataFrame`.

    It takes a `Callable[[str], str]` object as an argument (``fun``) and returns a
    `Callable[[DataFrame], DataFrame]` object.

    When `_()` is called on a `DataFrame`, it creates a list of column names,
    applying the argument `fun()` to each of them, and returning a new `DataFrame`
    with the new column names.

    :param fun: Renaming function
    :returns: Function which takes DataFrame as parameter.
    """

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
    """A function that takes a `Callable[[str], str]` and a `Callable[[str], str]`
    and returns a `Callable[[DataFrame], DataFrame]`, which in turn takes a
    `DataFrame` and returns a `DataFrame` with some of its columns renamed.

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
    """This function takes a ``DataFrame`` instance and returns the
    same ``DataFrame`` instance with all column names converted to snake case
    (e.g. ``col_name_1``). It uses the ``to_snake_case`` function in conjunction with
    the ``with_columns_renamed`` function to achieve this.
    :param df: A ``DataFrame`` instance to process
    :type df: ``DataFrame``
    :return: A ``DataFrame`` instance with column names converted to snake case
    :rtype: ``DataFrame``
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
    """This function sorts the columns of a given DataFrame based on a given sort
    order. The ``sort_order`` parameter can either be ``asc`` or ``desc``, which correspond to
    ascending and descending order, respectively. If any other value is provided for
    the ``sort_order`` parameter, a ``ValueError`` will be raised.

    :param df: A DataFrame
    :type df: pyspark.sql.DataFrame
    :param sort_order: The order in which to sort the columns in the DataFrame
    :type sort_order: str
    :return: A DataFrame with the columns sorted in the chosen order
    :rtype: pyspark.sql.DataFrame
    """

    def parse_sort_order(sort_order: str) -> bool:
        if sort_order not in ["asc", "desc"]:
            raise ValueError(
                "['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
                    sort_order=sort_order
                )
            )
        reverse_lookup = {
            "asc": False,
            "desc": True,
        }
        return reverse_lookup[sort_order]

    def sort_top_level_cols(schema, is_reversed) -> dict:
        # sort top level columns
        top_sorted_fields: list = sorted(
            schema.fields, key=lambda x: x.name, reverse=is_reversed
        )

        is_nested: bool = any(
            [
                isinstance(i.dataType, StructType) or isinstance(i.dataType, ArrayType)
                for i in top_sorted_fields
            ]
        )

        output = {
            "schema": top_sorted_fields,
            "is_nested": is_nested,
        }

        return output

    def sort_nested_cols(schema, is_reversed, baseField="") -> list:
        # TODO: get working with ArrayType
        # recursively check nested fields and sort them
        # https://stackoverflow.com/questions/57821538/how-to-sort-columns-of-nested-structs-alphabetically-in-pyspark
        # Credits: @pault for logic

        select_cols = []
        for structField in sorted(schema, key=lambda x: x.name, reverse=is_reversed):
            if isinstance(structField.dataType, StructType):
                subFields = []
                for fld in sorted(
                    structField.jsonValue()["type"]["fields"],
                    key=lambda x: x["name"],
                    reverse=is_reversed,
                ):
                    newStruct = StructType([StructField.fromJson(fld)])
                    newBaseField = structField.name
                    if baseField:
                        newBaseField = baseField + "." + newBaseField
                    subFields.extend(
                        sort_nested_cols(newStruct, is_reversed, baseField=newBaseField)
                    )

                select_cols.append(
                    "struct(" + ",".join(subFields) + ") AS {}".format(structField.name)
                )
            else:
                if baseField:
                    select_cols.append(baseField + "." + structField.name)
                else:
                    select_cols.append(structField.name)
        return select_cols

    is_reversed: bool = parse_sort_order(sort_order)
    top_sorted_schema_results: dict = sort_top_level_cols(df.schema, is_reversed)
    if not top_sorted_schema_results["is_nested"]:
        columns: list = [i.name for i in top_sorted_schema_results["schema"]]
        return df.select(*columns)

    fully_sorted_schema = sort_nested_cols(
        top_sorted_schema_results["schema"], is_reversed
    )

    return df.selectExpr(fully_sorted_schema)
