from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
      from numbers import Number

      from pyspark.sql import Column
      from pyspark.sql.functions import udf


import re
import uuid
from typing import Any, Callable

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql.types import (
      ArrayType,
      BooleanType,
      StringType,
      StructType,
      MapType,
)

from pyspark.sql import Column, DataFrame
from typing import Dict, List
from pathlib import Path

def single_space(col: Column) -> Column:
    """Function takes a column and replaces all the multiple white spaces with a single space.

    It then trims the column to make all the texts consistent.

    :param col: The column which needs to be spaced
    :type col: Column
    :returns: A trimmed column with single space
    :rtype: Column
    """
    return F.trim(F.regexp_replace(col, " +", " "))


def remove_all_whitespace(col: Column) -> Column:
    """Function takes a `Column` object as a parameter and returns a `Column` object with all white space removed.

    It does this using the regexp_replace function from F, which replaces all whitespace with an empty string.

    :param col: a `Column` object
    :type col: Column
    :returns: a `Column` object with all white space removed
    :rtype: Column
    """
    return F.regexp_replace(col, "\\s+", "")


def anti_trim(col: Column) -> Column:
    """Remove whitespace from the boundaries of ``col`` using the regexp_replace function.

    :param col: Column on which to perform the regexp_replace.
    :type col: Column
    :return: A new Column with all whitespace removed from the boundaries.
    :rtype: Column
    """
    return F.regexp_replace(col, "\\b\\s+\\b", "")


def remove_non_word_characters(col: Column) -> Column:
    r"""Removes non-word characters from a column.

    The non-word characters which will be removed are those identified by the
    regular expression ``"[^\\w\\s]+"``.  This expression represents any character
    that is not a word character (e.g. `\\w`) or whitespace (`\\s`).

    :param col: A Column object.
    :return: A Column object with non-word characters removed.

    """
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def exists(f: Callable[[Any], bool]) -> udf:
    """Create a user-defined function.

    It takes a list expressed as a column of type ``ArrayType(AnyType)`` as an argument and returns a boolean value indicating
    whether any element in the list is true according to the argument ``f`` of the ``exists()`` function.

    :param f: Callable function - A callable function that takes an element of
    type Any and returns a boolean value.
    :return: A user-defined function that takes
    a list expressed as a column of type ArrayType(AnyType) as an argument and
    returns a boolean value indicating whether any element in the list is true
    according to the argument ``f`` of the ``exists()`` function.
    :rtype: UserDefinedFunction
    """

    def temp_udf(list_: list) -> bool:
        return any(map(f, list_))

    return F.udf(temp_udf, BooleanType())


def forall(f: Callable[[Any], bool]) -> udf:
    """The **forall** function allows for mapping a given boolean function to a list of arguments and return a single boolean value.

    It does this by creating a Spark UDF which takes in a list of arguments, applying the given boolean function to
    each element of the list and returning a single boolean value if all the elements pass through the given boolean function.

    :param f: A callable function ``f`` which takes in any type and returns a boolean
    :return: A spark UDF which accepts a list of arguments and returns True if all
    elements pass through the given boolean function, False otherwise.
    :rtype: UserDefinedFunction
        """

    def temp_udf(list_: list) -> bool:
        return all(map(f, list_))

    return F.udf(temp_udf, BooleanType())


def multi_equals(value: Any) -> udf:  # noqa: ANN401
    """Create a user-defined function that checks if all the given columns have the designated value.

    :param value: The designated value.
    :type value: Any
    :return: A user-defined function of type BooleanType().
    :rtype: UserDifinedFunction
    """

    def temp_udf(*cols) -> bool:  # noqa: ANN002
        return all(map(lambda col: col == value, cols)) # noqa: C417

    return F.udf(temp_udf, BooleanType())


def week_start_date(col: Column, week_start_day: str = "Sun") -> Column:
    """Function takes a Spark `Column` and an optional `week_start_day` argument and returns a `Column` with the corresponding start of week dates.

    The "standard week" in Spark starts on Sunday, however an optional argument can be
    used to start the week from a different day, e.g. Monday. The `week_start_day`
    argument is a string corresponding to the day of the week to start the week
    from, e.g. `"Mon"`, `"Tue"`, and must be in the set: `{"Sun", "Mon", "Tue", "Wed",
    "Thu", "Fri", "Sat"}`. If the argument given is not a valid day then a `ValueError`
    will be raised.

    :param col: The column to determine start of week dates on
    :type col: Column
    :param week_start_day: The day to start the week on
    :type week_start_day: str
    :returns: A Column with start of week dates
    :rtype: Column
    """
    _raise_if_invalid_day(week_start_day)
    # the "standard week" in Spark is from Sunday to Saturday
    mapping = {
        "Sun": "Sat",
        "Mon": "Sun",
        "Tue": "Mon",
        "Wed": "Tue",
        "Thu": "Wed",
        "Fri": "Thu",
        "Sat": "Fri",
    }
    end = week_end_date(col, mapping[week_start_day])
    return F.date_add(end, -6)


def week_end_date(col: Column, week_end_day: str = "Sat") -> Column:
    """Return a date column for the end of week for a given day.

    The Spark function `dayofweek` considers Sunday as the first day of the week, and
    uses the default value of 1 to indicate Sunday. Usage of the `when` and `otherwise`
    functions allow a comparison between the end of week day indicated and the day
    of week computed, and the return of the reference date if they match or the the
    addition of one week to the reference date otherwise.

    :param col: The reference date column.
    :type col: Column
    :param week_end_day: The week end day (default: 'Sat')
    :type week_end_day: str
    :return: A Column of end of the week dates.
    :rtype: Column
    """
    _raise_if_invalid_day(week_end_day)
    # these are the default Spark mappings.  Spark considers Sunday the first day of the week.
    day_of_week_mapping = {
        "Sun": 1,
        "Mon": 2,
        "Tue": 3,
        "Wed": 4,
        "Thu": 5,
        "Fri": 6,
        "Sat": 7,
    }
    return F.when(
        F.dayofweek(col).eqNullSafe(F.lit(day_of_week_mapping[week_end_day])), col,
    ).otherwise(F.next_day(col, week_end_day))


def _raise_if_invalid_day(day: str) -> None:
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if day not in valid_days:
        message = "The day you entered '{}' is not valid.  Here are the valid days: [{}]".format(day, ",".join(valid_days))
        raise ValueError(message)


def approx_equal(col1: Column, col2: Column, threshold: Number) -> Column:
    """Compare two ``Column`` objects by checking if the difference between them is less than a specified ``threshold``.

    :param col1: the first ``Column``
    :type col1: Column
    :param col2: the second ``Column``
    :type col2: Column
    :param threshold: value to compare with
    :type threshold: Number
    :return: Boolean ``Column`` with ``True`` indicating that ``abs(col1 -
    col2)`` is less than ``threshold``
    """
    return F.abs(col1 - col2) < threshold


def array_choice(col: Column, seed: int | None = None) -> Column:
    """Returns one random element from the given column.

    :param col: Column from which element is chosen
    :type col: Column
    :return: random element from the given column
    :rtype: Column
    """
    index = (F.rand(seed) * F.size(col)).cast("int")
    return col[index]


@F.udf(returnType=ArrayType(StringType()))
def regexp_extract_all(s, regexp):
    return None if s == None else re.findall(regexp, s)


def sanitize_column_name(name: str, replace_char: str = "_") -> str:
    """
    Sanitizes column names by replacing special characters with the specified character.

    :param name: The original column name.
    :type name: str
    :param replace_char: The character to replace special characters with, defaults to '_'.
    :type replace_char: str, optional
    :return: The sanitized column name.
    :rtype: str
    """
    return re.sub(r"[^a-zA-Z0-9_]", replace_char, name)


def _get_complex_fields(df: DataFrame) -> Dict[str, object]:
    """
    Returns a dictionary of complex field names and their data types from the input DataFrame's schema.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :return: A dictionary with complex field names as keys and their respective data types as values.
    :rtype: Dict[str, object]
    """
    return {
        field.name: field.dataType
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType, MapType))
    }


def flatten_struct(df: DataFrame, col_name: str, sep: str = ":") -> DataFrame:
    """
    Flattens the specified StructType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param col_name: The column name of the StructType to be flattened.
    :type col_name: str
    :param sep: The separator to use in the resulting flattened column names, defaults to ':'.
    :type sep: str, optional
    :return: The DataFrame with the flattened StructType column.
    :rtype: List[Column]
    """
    struct_type = _get_complex_fields(df)[col_name]
    expanded = [
        F.col(f"`{col_name}`.`{k}`").alias(col_name + sep + k)
        for k in [n.name for n in struct_type.fields]
    ]
    return df.select("*", *expanded).drop(F.col(f"`{col_name}`"))

def explode_array(df: DataFrame, col_name: str) -> DataFrame:
    """
    Explodes the specified ArrayType column in the input DataFrame and returns a new DataFrame with the exploded column.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param col_name: The column name of the ArrayType to be exploded.
    :type col_name: str
    :return: The DataFrame with the exploded ArrayType column.
    :rtype: DataFrame
    """
    return df.select("*", F.explode_outer(F.col(f"`{col_name}`")).alias(col_name)).drop(col_name)

def flatten_map(df: DataFrame, col_name: str, sep: str = ":") -> DataFrame:
    """
    Flattens the specified MapType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param col_name: The column name of the MapType to be flattened.
    :type col_name: str
    :param sep: The separator to use in the resulting flattened column names, defaults to ":".
    :type sep: str, optional
    :return: The DataFrame with the flattened MapType column.
    :rtype: DataFrame
    """
    keys_df = df.select(F.explode_outer(F.map_keys(F.col(f"`{col_name}`")))).distinct()
    keys = [row[0] for row in keys_df.collect()]
    key_cols = [F.col(f"`{col_name}`").getItem(k).alias(col_name + sep + k) for k in keys]
    return df.select([F.col(f"`{col}`") for col in df.columns if col != col_name] + key_cols)

def flatten_dataframe(df: DataFrame, sep: str = ":", replace_char: str = "_", sanitized_columns: bool = False) -> DataFrame:
    """
    Flattens all complex data types (StructType, ArrayType, and MapType) in the input DataFrame and returns a
    new DataFrame with the flattened columns.

    :param df: The input PySpark DataFrame.
    :type df: DataFrame
    :param sep: The separator to use in the resulting flattened column names, defaults to ":".
    :type sep: str, optional
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
    complex_fields = _get_complex_fields(df)

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if isinstance(complex_fields[col_name], StructType):
            df = flatten_struct(df, col_name, sep)

        elif isinstance(complex_fields[col_name], ArrayType):
            df = explode_array(df, col_name)

        elif isinstance(complex_fields[col_name], MapType):
            df = flatten_map(df, col_name, sep)

        complex_fields = _get_complex_fields(df)

    # Sanitize column names with the specified replace_char
    if sanitized_columns:
        sanitized_columns = [
            sanitize_column_name(col_name, replace_char) for col_name in df.columns
        ]
        df = df.toDF(*sanitized_columns)

    return df

  
# def regexp_extract_all(s: str, regexp: str) -> list[re.Match] | None:
#     """Function uses the Python `re` library to extract regular expressions from a string (`s`) using a regex pattern (`regexp`).
#
#     It returns a list of all matches, or    `None` if `s` is `None`.
#
#     :param s: input string (`Column`)
#     :type s: str
#     :param regexp: string `re` pattern
#     :return: List of matches
#     """
#     return None if s is None else re.findall(regexp, s)


def business_days_between(start_date: Column, end_date: Column) -> Column:
    """Function takes two Spark `Columns` and returns a `Column` with the number of business days between the start and the end date.

    :param start_date: The column with the start dates
    :type start_date: Column
    :param end_date: The column with the end dates
    :type end_date: Column
    :returns: a Column with the number of business days between the start and the end date
    :rtype: Column
    """
    all_days = "sequence(start_date, end_date)"
    days_of_week = f"transform({all_days}, x -> date_format(x, 'E'))"
    filter_weekends = F.expr(f"filter({days_of_week}, x -> x NOT IN ('Sat', 'Sun'))")
    num_business_days = F.size(filter_weekends) - 1

    return F.when(num_business_days < 0, None).otherwise(num_business_days)


def uuid5(
    col: Column, namespace: uuid.UUID = uuid.NAMESPACE_DNS, extra_string: str = "",
) -> Column:
    """Function generates UUIDv5 from ``col`` and ``namespace``, optionally prepending an extra string to ``col``.

    Sets variant to RFC 4122 one.

    :param col: Column that will be hashed.
    :type col: Column
    :param namespace: Namespace to be used. (default: `uuid.NAMESPACE_DNS`)
    :type namespace: str
    :param extra_string: In case of collisions one can pass an extra string to hash on.
    :type extra_string: str
    :return: String representation of generated UUIDv5
    :rtype: Column
    """
    ns = F.lit(namespace.bytes)
    salted_col = F.concat(F.lit(extra_string), col)
    encoded = F.encode(salted_col, "utf-8")
    encoded_with_ns = F.concat(ns, encoded)
    hashed = F.sha1(encoded_with_ns)
    variant_part = F.substring(hashed, 17, 4)
    variant_part = F.conv(variant_part, 16, 2)
    variant_part = F.lpad(variant_part, 16, "0")
    variant_part = F.concat(
        F.lit("10"), F.substring(variant_part, 3, 16),
    )  # RFC 4122 variant.
    variant_part = F.lower(F.conv(variant_part, 2, 16))
    return F.concat_ws(
        "-",
        F.substring(hashed, 1, 8),
        F.substring(hashed, 9, 4),
        F.concat(F.lit("5"), F.substring(hashed, 14, 3)),  # Set version.
        variant_part,
        F.substring(hashed, 21, 12),
    )
