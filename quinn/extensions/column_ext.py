from __future__ import annotations

from typing import Any

from pyspark.sql.column import Column
from pyspark.sql.functions import lit, trim, when


def isFalsy(self: Column) -> Column:
    """Returns a Column indicating whether all values in the Column are False or NULL (**falsy**).

    Each element in the resulting column is True if all the elements in the
    Column are either NULL or False, or False otherwise. This is accomplished by
    performing a bitwise or of the ``isNull`` condition and a literal False value and
    then wrapping the result in a **when** statement.

    :param self: Column object
    :returns: Column object
    :rtype: Column
    """
    return when(self.isNull() | (self == lit(False)), True).otherwise(False)


def isTruthy(self: Column) -> Column:
    """Calculates a boolean expression that is the opposite of isFalsy for the given ``Column`` self.

    :param Column self: The ``Column`` to calculate the opposite of isFalsy for.
    :returns: A ``Column`` with the results of the calculation.
    :rtype: Column
    """
    return ~(self.isFalsy())


def isFalse(self: Column) -> Column:
    """Function checks if the column is equal to False and returns the column.

    :param self: Column
    :return: Column
    :rtype: Column
    """
    return self == lit(False)


def isTrue(self: Column) -> Column:
    """Function takes a column of type Column as an argument and returns a column of type Column.

    It evaluates whether each element in the column argument is equal to True, and
    if so will return True, otherwise False.

    :param self: Column object
    :returns: Column object
    :rtype: Column
    """
    return self == lit(True)


def isNullOrBlank(self: Column) -> Column:
    r"""Returns a Boolean value which expresses whether a given column is ``null`` or contains only blank characters.

    :param \*\*self: The  :class:`Column` to check.

    :returns: A `Column` containing ``True`` if the column is ``null`` or only contains
    blank characters, or ``False`` otherwise.
    :rtype: Column
    """
    return (self.isNull()) | (trim(self) == "")


def isNotIn(self: Column, _list: list[Any]) -> Column:
    """To see if a value is not in a list of values.

    :param self: Column object
    :_list: list[Any]
    :rtype: Column
    """
    return ~(self.isin(_list))


def nullBetween(self: Column, lower: Column, upper: Column) -> Column:
    """To see if a value is between two values in a null friendly way.

    :param self: Column object
    :lower: Column
    :upper: Column
    :rtype: Column
    """
    return when(lower.isNull() & upper.isNull(), False).otherwise(
        when(self.isNull(), False).otherwise(
            when(lower.isNull() & upper.isNotNull() & (self <= upper), True).otherwise(
                when(
                    lower.isNotNull() & upper.isNull() & (self >= lower),
                    True,
                ).otherwise(self.between(lower, upper)),
            ),
        ),
    )


Column.isFalsy = isFalsy
Column.isTruthy = isTruthy
Column.isFalse = isFalse
Column.isTrue = isTrue
Column.isNullOrBlank = isNullOrBlank
Column.isNotIn = isNotIn
Column.nullBetween = nullBetween
