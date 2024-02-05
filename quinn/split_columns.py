from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from pyspark.sql.functions import length, split, trim, udf, when
from pyspark.sql.types import IntegerType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def split_col(  # noqa: PLR0913
    df: DataFrame,
    col_name: str,
    delimiter: str,
    new_col_names: list[str],
    mode: str = "permissive",
    default: Optional[str] = None,
) -> DataFrame:
    """Splits the given column based on the delimiter and creates new columns with the split values.

    :param df: The input DataFrame
    :type df: pyspark.sql.DataFrame
    :param col_name: The name of the column to split
    :type col_name: str
    :param delimiter: The delimiter to split the column on
    :type delimiter: str
    :param new_col_names: A list of two strings for the new column names
    :type new_col_names: (List[str])
    :param mode: The split mode. Can be "strict" or "permissive". Default is "permissive"
    :type mode: str
    :param default: If the mode is "permissive" then default value will be assigned to column
    :type mode: str
    :return: dataframe: The resulting DataFrame with the split columns
    :rtype: pyspark.sql.DataFrame.
    """
    # Check if the column to be split exists in the DataFrame
    if col_name not in df.columns:
        msg = f"Column '{col_name}' not found in DataFrame."
        raise ValueError(msg)

    # Check if the delimiter is a string
    if not isinstance(delimiter, str):
        msg = "Delimiter must be a string."
        raise TypeError(msg)

    # Check if the new column names are a list of strings
    if not isinstance(new_col_names, list):
        msg = "New column names must be a list of strings."
        raise TypeError(msg)

    # Define a UDF to check the occurrence of delimitter
    def _num_delimiter(col_value1: str) -> int:
        # Get the count of delimiter and store the result in no_of_delimiter
        no_of_delimiter = col_value1.count(delimiter)
        # Split col_value based on delimiter and store the result in split_value
        split_value = col_value1.split(delimiter)

        # Check if col_value is not None
        if col_value1 is not None:
            # Check if the no of delimiters in split_value is not as expected
            if no_of_delimiter != len(new_col_names) - 1:
                # If the length is not same, raise an IndexError with the message mentioning the expected and found length
                msg = f"Expected {len(new_col_names)} elements after splitting on delimiter, found {len(split_value)} elements"
                raise IndexError(
                    msg,
                )

            # If the length of split_value is same as new_col_names, check if any of the split values is None or empty string
            elif any(  # noqa: RET506
                x is None or x.strip() == "" for x in split_value[: len(new_col_names)]
            ):
                msg = "Null or empty values are not accepted for columns in strict mode"
                raise ValueError(
                    msg,
                )

            # If the above checks pass, return the count of delimiter
            return int(no_of_delimiter)

        # If col_value is None, return 0
        return 0

    num_udf = udf(lambda y: None if y is None else _num_delimiter(y), IntegerType())

    # Get the column expression for the column to be split
    col_expr = df[col_name]

    # Split the column by the delimiter
    split_col_expr = split(trim(col_expr), delimiter)

    # Check the split mode
    if mode == "strict":
        # Create an array of select expressions to create new columns from the split values
        select_exprs = [
            when(split_col_expr.getItem(i) != "", split_col_expr.getItem(i)).alias(
                new_col_names[i],
            )
            for i in range(len(new_col_names))
        ]

        # Select all the columns from the input DataFrame, along with the new split columns
        df = df.select("*", *select_exprs)  # noqa: PD901
        df = df.withColumn("del_length", num_udf(df[col_name]))  # noqa: PD901
        df.cache()
        # Drop the original column if the new columns were created successfully
        df = df.select( # noqa: PD901
            [c for c in df.columns if c not in {"del_length", col_name}],
        )

    elif mode == "permissive":
        # Create an array of select expressions to create new columns from the split values
        # Use the default value if a split value is missing or empty
        select_exprs = select_exprs = [
            when(length(split_col_expr.getItem(i)) > 0, split_col_expr.getItem(i))
            .otherwise(default)
            .alias(new_col_names[i])
            for i in range(len(new_col_names))
        ]

        # Select all the columns from the input DataFrame, along with the new split columns
        # Drop the original column if the new columns were created successfully
        df = df.select("*", *select_exprs).drop(col_name)  # noqa: PD901
        df.cache()

    else:
        msg = f"Invalid mode: {mode}"
        raise ValueError(msg)

    # Return the DataFrame with the split columns
    return df
