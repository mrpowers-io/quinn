from pyspark.sql.functions import when, lit, col, trim

from pyspark.sql.column import Column

def isFalsy(self):
    return when(self.isNull() | (self == lit(False)), True).otherwise(False)

def isTruthy(self):
    return ~(self.isFalsy())

def isNullOrBlank(self):
    return (self.isNull()) | (trim(self) == "")

def isNotIn(self, list):
    return ~(self.isin(list))

Column.isFalsy = isFalsy
Column.isTruthy = isTruthy
Column.isNullOrBlank = isNullOrBlank
Column.isNotIn = isNotIn

