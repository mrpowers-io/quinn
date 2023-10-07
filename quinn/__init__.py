"""quinn API."""

from quinn.append_if_schema_identical import append_if_schema_identical
from quinn.split_columns import split_col
from quinn.dataframe_helpers import (
    column_to_list,
    create_df,
    print_athena_create_table,
    show_output_to_df,
    to_list_of_dictionaries,
    two_columns_to_dictionary,
)
from quinn.dataframe_validator import (
    DataFrameMissingColumnError,
    DataFrameMissingStructFieldError,
    DataFrameProhibitedColumnError,
    validate_absence_of_columns,
    validate_presence_of_columns,
    validate_schema,
)
from quinn.functions import (
    anti_trim,
    approx_equal,
    business_days_between,
    exists,
    forall,
    multi_equals,
    regexp_extract_all,
    remove_all_whitespace,
    remove_non_word_characters,
    single_space,
    uuid5,
    week_end_date,
    week_start_date,
)
from quinn.schema_helpers import print_schema_as_code
from quinn.transformations import snake_case_col_names, sort_columns, to_snake_case, with_columns_renamed, with_some_columns_renamed

# Use __all__ to let developers know what is part of the public API.
__all__ = [
    "split_col",
    "DataFrameMissingColumnError",
    "DataFrameMissingStructFieldError",
    "DataFrameProhibitedColumnError",
    "column_to_list",
    "to_list_of_dictionaries",
    "two_columns_to_dictionary",
    "print_athena_create_table",
    "show_output_to_df",
    "create_df",
    "validate_presence_of_columns",
    "validate_schema",
    "validate_absence_of_columns",
    "print_schema_as_code",
    "single_space",
    "remove_all_whitespace",
    "anti_trim",
    "remove_non_word_characters",
    "exists",
    "forall",
    "multi_equals",
    "week_start_date",
    "week_end_date",
    "approx_equal",
    "regexp_extract_all",
    "business_days_between",
    "uuid5",
    "with_columns_renamed",
    "with_some_columns_renamed",
    "snake_case_col_names",
    "to_snake_case",
    "sort_columns",
    "append_if_schema_identical",
    "flatten_dataframe",
]
