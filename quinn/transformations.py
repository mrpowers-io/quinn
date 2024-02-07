from __future__ import annotations

import re
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import ArrayType, MapType, StructField, StructType

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


def sort_columns(  # noqa: C901,PLR0915
    df: DataFrame,
    sort_order: str,
    sort_nested: bool = False,
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

    def sort_nested_cols(
        schema: StructType, is_reversed: bool, base_field: str="",
    ) -> list[str]:
        # recursively check nested fields and sort them
        # https://stackoverflow.com/questions/57821538/how-to-sort-columns-of-nested-structs-alphabetically-in-pyspark
        # Credits: @pault for logic

        def parse_fields(
            fields_to_sort: list,
            parent_struct: StructType,
            is_reversed: bool,
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
                    sort_nested_cols(
                        new_struct, is_reversed, base_field=new_base_field,
                    ),
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

            elif base_field:
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
            field.dataType,
            ArrayType,
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
            field.dataType,
            ArrayType,
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
        msg = f"['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'"
        raise ValueError(
            msg,
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
        isinstance(i.dataType, (StructType, ArrayType))
        for i in top_level_sorted_df.schema
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

    if not hasattr(SparkSession, "getActiveSession"):  # spark 2.4
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = SparkSession.getActiveSession()
        spark = spark if spark is not None else SparkSession.builder.getOrCreate()

    return output


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
    sanitized_columns: bool = False,
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
        return df.select(
            "*",
            F.explode_outer(F.col(f"`{col_name}`")).alias(col_name),
        ).drop(
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
