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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numbers import Number

    from pyspark.sql import Column
    from pyspark.sql.functions import udf


import os
import sys
import uuid
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql.functions import lit, trim, when
from pyspark.sql.types import (
    BooleanType,
)


class UnsupportedSparkConnectFunctionError(Exception):
    """Raise this when a function that is not supported by Spark-Connect < v3.5.2 is called."""

    def __init__(self) -> None:
        """Initialize the UnsupportedSparkConnectFunction exception.

        :returns: None
        :rtype: None
        """
        self.message = "This function is not supported on Spark-Connect < 3.5.2"
        super().__init__(self.message)


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
    """Remove all inner whitespace but retain leading and trailing whitespace.

    :param col: Column on which to perform the regexp_replace.
    :type col: Column
    :return: A new Column with all inner whitespace removed but leading and trailing whitespace retained.
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


def multi_equals(value: Any) -> udf:  # noqa: ANN401
    """Create a user-defined function that checks if all the given columns have the designated value.

    :param value: The designated value.
    :type value: Any
    :return: A user-defined function of type BooleanType().
    :rtype: UserDifinedFunction
    """

    def temp_udf(*cols) -> bool:  # noqa: ANN002
        return all(map(lambda col: col == value, cols))  # noqa: C417

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
        F.dayofweek(col).eqNullSafe(F.lit(day_of_week_mapping[week_end_day])),
        col,
    ).otherwise(F.next_day(col, week_end_day))


def _raise_if_invalid_day(day: str) -> None:
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if day not in valid_days:
        message = "The day you entered '{}' is not valid.  Here are the valid days: [{}]".format(
            day,
            ",".join(valid_days),
        )
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
    if sys.modules["pyspark"].__version__ < "3.5.2" and os.getenv("SPARK_CONNECT_MODE_ENABLED"):
        raise UnsupportedSparkConnectFunctionError

    index = (F.rand(seed) * F.size(col)).cast("int")
    return col[index]


def business_days_between(
    start_date: Column,  # noqa: ARG001
    end_date: Column,  # noqa: ARG001
) -> Column:
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
    col: Column,
    namespace: uuid.UUID = uuid.NAMESPACE_DNS,
    extra_string: str = "",
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
        F.lit("10"),
        F.substring(variant_part, 3, 16),
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


def is_falsy(col: Column) -> Column:
    """Returns a Column indicating whether all values in the Column are False or NULL (**falsy**).

    Each element in the resulting column is True if all the elements in the
    Column are either NULL or False, or False otherwise. This is accomplished by
    performing a bitwise or of the ``isNull`` condition and a literal False value and
    then wrapping the result in a **when** statement.

    :param col: Column object
    :returns: Column object
    :rtype: Column
    """
    return when(col.isNull() | (col == lit(False)), True).otherwise(False)


def is_truthy(col: Column) -> Column:
    """Calculates a boolean expression that is the opposite of is_falsy for the given ``Column`` col.

    :param Column col: The ``Column`` to calculate the opposite of is_falsy for.
    :returns: A ``Column`` with the results of the calculation.
    :rtype: Column
    """
    return ~(is_falsy(col))


def is_false(col: Column) -> Column:
    """Function checks if the column is equal to False and returns the column.

    :param col: Column
    :return: Column
    :rtype: Column
    """
    return col == lit(False)


def is_true(col: Column) -> Column:
    """Function takes a column of type Column as an argument and returns a column of type Column.

    It evaluates whether each element in the column argument is equal to True, and
    if so will return True, otherwise False.

    :param col: Column object
    :returns: Column object
    :rtype: Column
    """
    return col == lit(True)


def is_null_or_blank(col: Column) -> Column:
    r"""Returns a Boolean value which expresses whether a given column is ``null`` or contains only blank characters.

    :param \*\*col: The  :class:`Column` to check.

    :returns: A `Column` containing ``True`` if the column is ``null`` or only contains
    blank characters, or ``False`` otherwise.
    :rtype: Column
    """
    return (col.isNull()) | (trim(col) == "")


def is_not_in(col: Column, _list: list[Any]) -> Column:
    """To see if a value is not in a list of values.

    :param col: Column object
    :_list: list[Any]
    :rtype: Column
    """
    return ~(col.isin(_list))


def null_between(col: Column, lower: Column, upper: Column) -> Column:
    """To see if a value is between two values in a null friendly way.

    :param col: Column object
    :lower: Column
    :upper: Column
    :rtype: Column
    """
    return when(lower.isNull() & upper.isNull(), False).otherwise(
        when(col.isNull(), False).otherwise(
            when(lower.isNull() & upper.isNotNull() & (col <= upper), True).otherwise(
                when(
                    lower.isNotNull() & upper.isNull() & (col >= lower),
                    True,
                ).otherwise(col.between(lower, upper)),
            ),
        ),
    )
