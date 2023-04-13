import re
from numbers import Number
from typing import Any, Callable, List, Optional

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import *


def single_space(col: Column) -> Column:
    """This function takes a column and replaces all the multiple white spaces with a
    single space. It then trims the column to make all the texts consistent.
    :param col: The column which needs to be spaced
    :type col: Column
    :returns: A trimmed column with single space
    :rtype: Column
    """
    return F.trim(F.regexp_replace(col, " +", " "))


def remove_all_whitespace(col: Column) -> Column:
    """This function takes a `Column` object as a parameter and returns a `Column` object
    with all white space removed. It does this using the regexp_replace function
    from F, which replaces all whitespace with an empty string.
    :param col: a `Column` object
    :type col: Column
    :returns: a `Column` object with all white space removed
    :rtype: Column
    """
    return F.regexp_replace(col, "\\s+", "")


def anti_trim(col: Column) -> Column:
    """Removes whitespace from the boundaries of ``col`` using the regexp_replace
    function.

    :param col: Column on which to perform the regexp_replace.
    :type col: Column
    :return: A new Column with all whitespace removed from the boundaries.
    :rtype: Column
    """
    return F.regexp_replace(col, "\\b\\s+\\b", "")


def remove_non_word_characters(col: Column) -> Column:
    """Removes non-word characters from a column.

    The non-word characters which will be removed are those identified by the
    regular expression ``"[^\\w\\s]+"``.  This expression represents any character
    that is not a word character (e.g. `\w`) or whitespace (`\s`).

    :param col: A Column object.
    :return: A Column object with non-word characters removed.

    """
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def exists(f: Callable[[Any], bool]):
    """
    Create a user-defined function that takes a list expressed as a column of
    type ``ArrayType(AnyType)`` as an argument and returns a boolean value indicating
    whether any element in the list is true according to the argument ``f`` of the
    ``exists()`` function.

    :param f: Callable function - A callable function that takes an element of
    type Any and returns a boolean value.
    :return: A user-defined function that takes
    a list expressed as a column of type ArrayType(AnyType) as an argument and
    returns a boolean value indicating whether any element in the list is true
    according to the argument ``f`` of the ``exists()`` function.
    :rtype: UserDefinedFunction
    """

    def temp_udf(l):
        return any(map(f, l))

    return F.udf(temp_udf, BooleanType())


def forall(f: Callable[[Any], bool]):
    """The **forall** function allows for mapping a given boolean function to a list of
    arguments and return a single boolean value as the result of applying the
    boolean function to each element of the list. It does this by creating a Spark
    UDF which takes in a list of arguments, applying the given boolean function to
    each element of the list and returning a single boolean value if all the
    elements pass through the given boolean function.

    :param f: A callable function ``f`` which takes in any type and returns a boolean
    :return: A spark UDF which accepts a list of arguments and returns True if all
    elements pass through the given boolean function, False otherwise.
    :rtype: UserDefinedFunction
        """

    def temp_udf(l):
        return all(map(f, l))

    return F.udf(temp_udf, BooleanType())


def multi_equals(value: Any):
    """Create a user-defined function that checks if all the given columns have the
    designated value.

    :param value: The designated value.
    :type value: Any
    :return: A user-defined function of type BooleanType().
    :rtype: UserDifinedFunction
    """

    def temp_udf(*cols):
        return all(map(lambda col: col == value, cols))

    return F.udf(temp_udf, BooleanType())


def week_start_date(col: Column, week_start_day: str = "Sun") -> Column:
    """This function takes a Spark `Column` and an optional `week_start_day` string
    argument and returns a `Column` with the corresponding start of week dates. The
    "standard week" in Spark starts on Sunday, however an optional argument can be
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
    """
    Returns a date column for the end of week for a given day.

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
        F.dayofweek(col).eqNullSafe(F.lit(day_of_week_mapping[week_end_day])), col
    ).otherwise(F.next_day(col, week_end_day))


def _raise_if_invalid_day(day: str) -> None:
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if day not in valid_days:
        message = "The day you entered '{0}' is not valid.  Here are the valid days: [{1}]".format(
            day, ",".join(valid_days)
        )
        raise ValueError(message)


def approx_equal(col1: Column, col2: Column, threshhold: Number) -> Column:
    """Compares two ``Column`` objects by checking if the difference between them
    is less than a specified ``threshhold``.

    :param col1: the first ``Column``
    :type col1: Column
    :param col2: the second ``Column``
    :type col2: Column
    :param threshhold: value to compare with
    :type threshhold: Number
    :return: Boolean ``Column`` with ``True`` indicating that ``abs(col1 -
    col2)`` is less than ``threshhold``
    """
    return F.abs(col1 - col2) < threshhold


def array_choice(col: Column) -> Column:
    """Returns one random element from the given column.

    :param col: Column from which element is chosen
    :type col: Column
    :return: random element from the given column
    :rtype: Column
    """
    index = (F.rand() * F.size(col)).cast("int")
    return col[index]


@F.udf(returnType=ArrayType(StringType()))
def regexp_extract_all(s: str, regexp: str) -> Optional[List[re.Match]]:
    """This function uses the Python `re` library to extract regular expressions from a
    string (`s`) using a regex pattern (`regexp`). It returns a list of all matches, or    `None` if `s` is `None`.

    :param s: input string (`Column`)
    :type s: str
    :param regexp: string `re` pattern
    :return: List of matches
    """
    return None if s == None else re.findall(regexp, s)
