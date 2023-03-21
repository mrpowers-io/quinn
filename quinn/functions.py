import re
import pyspark.sql.functions as F

from pyspark.sql.types import *


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


from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import re
from typing import Dict, List
from pathlib import Path

def sanitize_column_name(name: str, replace_char: str = "_") -> str:
    """
    Sanitizes column names by replacing special characters with the specified character.
    
    Args:
        name (str): The original column name.
        replace_char (str, optional): The character to replace special characters with. Defaults to '_'.
    
    Returns:
        str: The sanitized column name.
    """
    return re.sub(r"[^a-zA-Z0-9_]", replace_char, name)

def get_complex_fields(df: DataFrame) -> Dict[str, object]:
    """
    Returns a dictionary of complex field names and their data types from the input DataFrame's schema.
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
    
    Returns:
        Dict[str, object]: A dictionary with complex field names as keys and their respective data types as values.
    """
    complex_fields = {
        field.name: field.dataType
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType, MapType))
    }
    return complex_fields

def flatten_structs(df: DataFrame, col_name: str, complex_fields: dict, sep: str = ":", replace_char: str = "_") -> DataFrame:
    """
    Flattens the specified StructType column in the input DataFrame and returns a new DataFrame with the flattened columns.
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the StructType to be flattened.
        complex_fields (dict): A dictionary of complex field names and their data types.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to ':'.
        replace_char (str, optional): The character to replace special characters with in column names. Defaults to '_'.
    
    Returns:
        DataFrame: The DataFrame with the flattened StructType column.
    """
    expanded = [
        F.col(col_name + "." + k).alias(sanitize_column_name(col_name + sep + k, replace_char))
        for k in [n.name for n in complex_fields[col_name]]
    ]
    df = df.select("*", *expanded).drop(col_name)
    return df

def explode_arrays(df: DataFrame, col_name: str) -> DataFrame:
    """
    Explodes the specified ArrayType column in the input DataFrame and returns a new DataFrame with the exploded column.
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the ArrayType to be exploded.
    
    Returns:
        DataFrame: The DataFrame with the exploded ArrayType column.
    
    Raises:
        ValueError: If the specified column is not an ArrayType column.
    """
    if not isinstance(df.schema[col_name].dataType, ArrayType):
        raise ValueError(f"Column {col_name} is not an array column.")
    df = df.withColumn(col_name, F.explode_outer(col_name))
    return df

def flatten_maps(df: DataFrame, col_name: str, sep: str = ":", replace_char: str = "_") -> DataFrame:
    """
    Flattens the specified MapType column in the input DataFrame and returns a new DataFrame with the flattened columns.
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the MapType to be flattened.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to ':'.
    
    Returns:
        DataFrame: The DataFrame with the flattened MapType column.
    """
    keys_df = df.select(F.explode_outer(F.map_keys(F.col(col_name)))).distinct()
    keys = list(map(lambda row: row[0], keys_df.collect()))
    key_cols = list(
        map(
            lambda f: F.col(col_name).getItem(f).alias(sanitize_column_name(col_name + sep + f, replace_char)),
            keys,
        )
    )
    drop_column_list = [col_name]
    df = df.select(
        [
            col_to_keep for col_to_keep in df.columns if col_to_keep not in drop_column_list
        ]
        + key_cols
    )
    return df

def flatten_dataframe(
    df: DataFrame,
    sep: str = ":",
    sort_columns: bool = True,
    replace_char: str = "_"
) -> DataFrame:
    """
    Flattens all complex data types (StructType, ArrayType, and MapType) in the input DataFrame and returns a
    new DataFrame with the flattened columns.
    
    Args:
        df (DataFrame): The input PySpark DataFrame.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to ':'.
        sort_columns (bool, optional): Sort the columns alphabetically. Defaults to False.
        replace_char (str, optional): The character to replace special characters with in column names. Defaults to '_'.
        
    Returns:
        DataFrame: The DataFrame with all complex data types flattened.

    Note:
        This function assumes the input DataFrame has a consistent schema across all rows. If you have files with
        different schemas, use the read_and_flatten_nested_files function instead.

    Example:
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
    >>> schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name_age", StructType([
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("scores", MapType(StringType(), IntegerType()), True),
            StructField("fruits", ArrayType(StringType()), True),
            StructField("nested_key_map", MapType(StringType(), MapType(StringType(), IntegerType())), True),
            StructField("special_chars_map", MapType(StringType(), IntegerType()), True),
        ])
    >>> df = spark.createDataFrame(data, schema)
    >>> flattened_df = flatten_dataframe(df)
    >>> flattened_df.show()
    >>> flattened_df_with_hyphen = flatten_dataframe(df, replace_char="-")
    >>> flattened_df_with_hyphen.show()
    """
    complex_fields = get_complex_fields(df)

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if isinstance(complex_fields[col_name], StructType):
            df = flatten_structs(df, col_name, complex_fields, sep, replace_char)
        elif isinstance(complex_fields[col_name], ArrayType):
            df = explode_arrays(df, col_name)
        elif isinstance(complex_fields[col_name], MapType):
            df = flatten_maps(df, col_name, sep, replace_char)

        complex_fields = get_complex_fields(df)
        
    if sort_columns:
        df = df.select(sorted(df.columns))
        
    return df


def read_and_flatten_nested_files(
    data_directory: str,
    file_format: str = "json",
    sep: str = ":",
    sort_columns: bool = True,
    replace_char: str = "_",
    multiline: bool = False,
    allow_missing_columns: bool = False
) -> DataFrame:
    """
    Reads nested data files (JSON, Parquet, or Avro) in the given directory, processes each nested structure with
    flatten_dataframe, and returns a flattened DataFrame.

    Args:
        data_directory (str): The directory path containing data files.
        file_format (str, optional): The file format of the data files. Can be one of ["json", "parquet", "avro", "json_multiline"]. Defaults to "json".
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to ':'.
        sort_columns (bool, optional): Sort the columns alphabetically. Defaults to True.
        replace_char (str, optional): The character to replace special characters with in column names. Defaults to '_'.
        multiline (bool, optional): If True, file_format is "json" and the JSON files have multiline records. Defaults to False.
        allow_missing_columns (bool, optional): If True, allows missing columns when using unionByName. Defaults to False.

    Returns:
        DataFrame: The combined flattened DataFrame.
    """
    supported_formats = ["json", "parquet", "avro", "json_multiline"]
    file_format = file_format.lower()

    if file_format not in supported_formats:
        raise ValueError(f"Unsupported file format. Supported formats are {supported_formats}")

    # Get a list of files in the directory
    data_files = list(Path(data_directory).rglob(f"*.{file_format}"))

    # Read and flatten each file separately
    flattened_dfs = []
    for file in data_files:
        # Read the data file based on the file format
        if file_format == "json":
            df = spark.read.json(str(file), multiLine=multiline)
        elif file_format == "parquet":
            df = spark.read.parquet(str(file))
        elif file_format == "avro":
            df = spark.read.format("avro").load(str(file))

        # Process the nested data structure by applying the flatten_dataframe function with the provided parameters
        flattened_df = flatten_dataframe(df, sep=sep, sort_columns=sort_columns, replace_char=replace_char)
        flattened_dfs.append(flattened_df)

    # Combine the flattened DataFrames using unionByName
    combined_df = flattened_dfs[0]
    for df in flattened_dfs[1:]:
        combined_df = combined_df.unionByName(df, allowMissingColumns=allow_missing_columns)

    if sort_columns:
        combined_df = combined_df.select(sorted(combined_df.columns))

    return combined_df