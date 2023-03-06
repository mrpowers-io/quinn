from typing import Any, List

from pyspark.sql.column import Column
from pyspark.sql.functions import lit, trim, when


def isFalsy(self: Column) -> Column:
    return when(self.isNull() | (self == lit(False)), True).otherwise(False)


def isTruthy(self: Column) -> Column:
    return ~(self.isFalsy())


def isFalse(self: Column) -> Column:
    return self == False


def isTrue(self: Column) -> Column:
    return self == True


def isNullOrBlank(self: Column) -> Column:
    return (self.isNull()) | (trim(self) == "")


def isNotIn(self: Column, list: List[Any]) -> Column:
    return ~(self.isin(list))


def nullBetween(self: Column, lower: Column, upper: Column) -> Column:
    return when(lower.isNull() & upper.isNull(), False).otherwise(
        when(self.isNull(), False).otherwise(
            when(lower.isNull() & upper.isNotNull() & (self <= upper), True).otherwise(
                when(
                    lower.isNotNull() & upper.isNull() & (self >= lower), True
                ).otherwise(self.between(lower, upper))
            )
        )
    )


Column.isFalsy = isFalsy
Column.isTruthy = isTruthy
Column.isFalse = isFalse
Column.isTrue = isTrue
Column.isNullOrBlank = isNullOrBlank
Column.isNotIn = isNotIn
Column.nullBetween = nullBetween
