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

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from pyspark.sql.types import StructField, StructType

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def create_df(
    spark: SparkSession,
    rows_data: list[tuple],
    col_specs: list[tuple],
) -> DataFrame:
    """Creates a new DataFrame from the given data and column specifications.

    :param spark: SparkSession instance to create the DataFrame
    :type spark: SparkSession
    :param rows_data: The data used to populate the DataFrame, where each tuple represents a row.
    :type rows_data: list[tuple]
    :param col_specs: Specifications for the columns, where each tuple contains the column name and data type.
    :type col_specs: list[tuple]
    :return: A new DataFrame constructed from the provided rows and column specifications.
    :rtype: DataFrame
    """
    warnings.warn(
        "Extensions may be removed in the future versions of quinn. Please use `quinn.create_df()` instead",
        category=DeprecationWarning,
        stacklevel=2,
    )

    struct_fields = [StructField(*x) for x in col_specs]
    return spark.createDataFrame(data=rows_data, schema=StructType(struct_fields))
