from quinn.spark import *

import pyspark.sql.functions as F

from pyspark.sql.types import BooleanType

def exists(f):
    def temp_udf(l):
        return any(map(f, l))
    return F.udf(temp_udf, BooleanType())

def forall(f):
    def temp_udf(l):
        return all(map(f, l))
    return F.udf(temp_udf, BooleanType())

def multi_equals(value):
    def temp_udf(*cols):
        return all(map(lambda col: col == value, cols))
    return F.udf(temp_udf, BooleanType())
