import pyspark.sql.functions as F

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


def week_start_date(col, week_start_day = 'Mon'):
    raise_if_invalid_day(week_start_day)
    mapping = {'Mon': 'Sun', 'Tue': 'Mon', 'Wed': 'Tue', 'Thu': 'Wed', 'Fri': 'Thu', 'Sat': 'Fri', 'Sun': 'Sat'}
    end = week_end_date(col, mapping[week_start_day])
    return F.date_add(end, -6)


def week_end_date(col, week_end_day = 'Sun'):
    raise_if_invalid_day(week_end_day)
    return F.next_day(col, week_end_day)


def raise_if_invalid_day(day):
    valid_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    if day not in valid_days:
        message = "The day you entered '{0}' is not valid.  Here are the valid days: [{1}]".format(day, ','.join(valid_days))
        raise ValueError(message)

