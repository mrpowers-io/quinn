from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, when, length


def split_col(df: DataFrame, col_name: str, delimiter: str,
              new_col_names: List[str], mode: str = "strict") -> DataFrame:
    """
    Splits the given column based on the delimiter and creates new columns with the split values.
    :param df: The input DataFrame
    :type df: pyspark.sql.DataFrame
    :param col_name: The name of the column to split
    :type col_name: str
    :param delimiter: The delimiter to split the column on
    :type delimiter: str
    :param new_col_names: A list of two strings for the new column names
    :type new_col_names: (List[str])
    :param mode: The split mode. Can be "strict" or "permissive". Default is "strict"
    :type mode: str
    :return: dataframe: The resulting DataFrame with the split columns
    :rtype: pyspark.sql.DataFrame
    """
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found in DataFrame.")

    if not isinstance(delimiter, str):
        raise TypeError("Delimiter must be a string.")

    if not isinstance(new_col_names, list) or len(new_col_names) != 2:
        raise ValueError("New column names must be a list of two strings.")

    split_col_expr = split(col(col_name), delimiter)

    if mode == "strict":
        df = df.select("*", split_col_expr.getItem(0).alias(new_col_names[0]),
                       split_col_expr.getItem(1).alias(new_col_names[1])) \
            .drop(col_name)
    elif mode == "permissive":
        df = df.select("*", split_col_expr.getItem(0).alias(new_col_names[0]),
                       when(length(split_col_expr.getItem(1)) > 1, split_col_expr.getItem(1))
                       .alias(new_col_names[1])) \
            .filter(col(new_col_names[1]).isNotNull()) \
            .drop(col_name)
    else:
        raise ValueError(f"Invalid mode: {mode}")

    return df
