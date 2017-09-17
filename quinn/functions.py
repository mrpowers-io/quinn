from quinn.spark import *

import pyspark.sql.functions as F

from pyspark.sql.types import BooleanType

def exists(f):
    def temp_udf(l):
        return any(map(f, l))
    return F.udf(temp_udf, BooleanType())

