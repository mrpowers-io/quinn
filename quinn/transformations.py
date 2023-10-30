from __future__ import annotations
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


def sort_columns(
    df: DataFrame, sort_order: str, sort_nested: bool = False
) -> DataFrame:
    """This function sorts the columns of a given DataFrame based on a given sort
    order. The ``sort_order`` parameter can either be ``asc`` or ``desc``, which correspond to
    ascending and descending order, respectively. If any other value is provided for
    the ``sort_order`` parameter, a ``ValueError`` will be raised.

    :param df: A DataFrame
    :type df: pyspark.sql.DataFrame
    :param sort_order: The order in which to sort the columns in the DataFrame
    :type sort_order: str
    :param sort_nested: Whether to sort nested structs or not. Defaults to false.
    :type sort_nested: bool
    :return: A DataFrame with the columns sorted in the chosen order
    :rtype: pyspark.sql.DataFrame
    """

    def sort_nested_cols(schema, is_reversed, base_field="") -> list[str]:
        # recursively check nested fields and sort them
        # https://stackoverflow.com/questions/57821538/how-to-sort-columns-of-nested-structs-alphabetically-in-pyspark
        # Credits: @pault for logic

        def parse_fields(
            fields_to_sort: list, parent_struct, is_reversed: bool
        ) -> list:
            sorted_fields: list = sorted(
                fields_to_sort,
                key=lambda x: x["name"],
                reverse=is_reversed,
            )

            results = []
            for field in sorted_fields:
                new_struct = StructType([StructField.fromJson(field)])
                new_base_field = parent_struct.name
                if base_field:
                    new_base_field = base_field + "." + new_base_field

                results.extend(
                    sort_nested_cols(new_struct, is_reversed, base_field=new_base_field)
                )
            return results

        select_cols = []
        for parent_struct in sorted(schema, key=lambda x: x.name, reverse=is_reversed):
            field_type = parent_struct.dataType
            if isinstance(field_type, ArrayType):
                array_parent = parent_struct.jsonValue()["type"]["elementType"]
                base_str = f"transform({parent_struct.name}"
                suffix_str = f") AS {parent_struct.name}"

                # if struct in array, create mapping to struct
                if array_parent["type"] == "struct":
                    array_parent = array_parent["fields"]
                    base_str = f"{base_str}, x -> struct("
                    suffix_str = f"){suffix_str}"

                array_elements = parse_fields(array_parent, parent_struct, is_reversed)
                element_names = [i.split(".")[-1] for i in array_elements]
                array_elements_formatted = [f"x.{i} as {i}" for i in element_names]

                # create a string representation of the sorted array
                # ex: transform(phone_numbers, x -> struct(x.number as number, x.type as type)) AS phone_numbers
                result = f"{base_str}{', '.join(array_elements_formatted)}{suffix_str}"

            elif isinstance(field_type, StructType):
                field_list = parent_struct.jsonValue()["type"]["fields"]
                sub_fields = parse_fields(field_list, parent_struct, is_reversed)

                # create a string representation of the sorted struct
                # ex: struct(address.zip.first5, address.zip.last4) AS zip
                result = f"struct({', '.join(sub_fields)}) AS {parent_struct.name}"

            else:
                if base_field:
                    result = f"{base_field}.{parent_struct.name}"
                else:
                    result = parent_struct.name
            select_cols.append(result)

        return select_cols

    def get_original_nullability(field: StructField, result_dict: dict) -> None:
        if hasattr(field, "nullable"):
            result_dict[field.name] = field.nullable
        else:
            result_dict[field.name] = True

        if not isinstance(field.dataType, StructType) and not isinstance(
            field.dataType, ArrayType
        ):
            return

        if isinstance(field.dataType, ArrayType):
            result_dict[f"{field.name}_element"] = field.dataType.containsNull
            children = field.dataType.elementType.fields
        else:
            children = field.dataType.fields
        for i in children:
            get_original_nullability(i, result_dict)

    def fix_nullability(field: StructField, result_dict: dict) -> None:
        field.nullable = result_dict[field.name]
        if not isinstance(field.dataType, StructType) and not isinstance(
            field.dataType, ArrayType
        ):
            return

        if isinstance(field.dataType, ArrayType):
            # save the containsNull property of the ArrayType
            field.dataType.containsNull = result_dict[f"{field.name}_element"]
            children = field.dataType.elementType.fields
        else:
            children = field.dataType.fields

        for i in children:
            fix_nullability(i, result_dict)

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

    is_reversed: bool = reverse_lookup[sort_order]
    top_level_sorted_df = df.select(*sorted(df.columns, reverse=is_reversed))
    if not sort_nested:
        return top_level_sorted_df

    is_nested: bool = any(
        [
            isinstance(i.dataType, StructType) or isinstance(i.dataType, ArrayType)
            for i in top_level_sorted_df.schema
        ]
    )

    if not is_nested:
        return top_level_sorted_df

    fully_sorted_schema = sort_nested_cols(top_level_sorted_df.schema, is_reversed)
    output = df.selectExpr(fully_sorted_schema)
    result_dict = {}
    for field in df.schema:
        get_original_nullability(field, result_dict)

    for field in output.schema:
        fix_nullability(field, result_dict)

    final_df = output.sparkSession.createDataFrame(output.rdd, output.schema)
    return final_df
