from quinn.spark import *

import pyspark.sql.functions as F
from pyspark.sql.column import Column

from pyspark.sql.types import BooleanType


def single_space(col):
    return F.trim(F.regexp_replace(col, " +", " "))


def remove_all_whitespace(col):
    return F.regexp_replace(col, "\\s+", "")


def anti_trim(col):
    return F.regexp_replace(col, "\\b\\s+\\b", "")


def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")


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
