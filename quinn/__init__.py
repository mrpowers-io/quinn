# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""quinn API."""

from __future__ import annotations

from quinn.append_if_schema_identical import append_if_schema_identical
from quinn.dataframe_helpers import (
    column_to_list,
    create_df,
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
    array_choice,
    business_days_between,
    is_false,
    is_falsy,
    is_not_in,
    is_null_or_blank,
    is_true,
    is_truthy,
    multi_equals,
    null_between,
    remove_all_whitespace,
    remove_non_word_characters,
    single_space,
    uuid5,
    week_end_date,
    week_start_date,
)
from quinn.schema_helpers import print_schema_as_code
from quinn.split_columns import split_col
from quinn.transformations import (
    snake_case_col_names,
    sort_columns,
    to_snake_case,
    with_columns_renamed,
    with_some_columns_renamed,
)
