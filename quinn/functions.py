import re
import pyspark.sql.functions as F

from pyspark.sql.types import *

from pyspark.sql import Column, DataFrame
from typing import Dict, List
from pathlib import Path

def single_space(col):
    return F.trim(F.regexp_replace(col, " +", " "))


def remove_all_whitespace(col):
    return F.regexp_replace(col, "\\s+", "")


def anti_trim(col):
    return F.regexp_replace(col, "\\b\\s+\\b", "")


def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def exists(f):
    def temp_udf(l):
        return any(map(f, l))

    return F.udf(temp_udf, BooleanType())


def forall(f):
    def temp_udf(l):
        return all(map(f, l))

    return F.udf(temp_udf, BooleanType())


def multi_equals(value):
    def temp_udf(*cols):
        return all(map(lambda col: col == value, cols))

    return F.udf(temp_udf, BooleanType())


def week_start_date(col, week_start_day="Sun"):
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


def week_end_date(col, week_end_day="Sat"):
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
        F.dayofweek(col).eqNullSafe(F.lit(day_of_week_mapping[week_end_day])), col
    ).otherwise(F.next_day(col, week_end_day))


def _raise_if_invalid_day(day):
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if day not in valid_days:
        message = "The day you entered '{0}' is not valid.  Here are the valid days: [{1}]".format(
            day, ",".join(valid_days)
        )
        raise ValueError(message)


def approx_equal(col1, col2, threshhold):
    return F.abs(col1 - col2) < threshhold


def array_choice(col):
    index = (F.rand() * F.size(col)).cast("int")
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