from pyspark.sql.functions import when, lit, col, trim

from pyspark.sql.column import Column


def isFalsy(self):
    return when(self.isNull() | (self == lit(False)), True).otherwise(False)


def isTruthy(self):
    return ~(self.isFalsy())


def isFalse(self):
    return self == False


def isTrue(self):
    return self == True


def isNullOrBlank(self):
    return (self.isNull()) | (trim(self) == "")


def isNotIn(self, list):
    return ~(self.isin(list))


def nullBetween(self, lower, upper):
    return when(lower.isNull() & upper.isNull(), False).otherwise(
        when(self.isNull(), False).otherwise(
            when(lower.isNull() & upper.isNotNull() & (self <= upper), True).otherwise(
                when(lower.isNotNull() & upper.isNull() & (self >= lower), True).otherwise(
                    self.between(lower, upper)
                )
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
