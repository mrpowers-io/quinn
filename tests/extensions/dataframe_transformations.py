from pyspark.sql.functions import lit
from cytoolz import curry


def with_greeting(df):
    return df.withColumn("greeting", lit("hi"))


def with_something(df, something):
    return df.withColumn("something", lit(something))


def with_funny(word):
    def inner(df):
        return df.withColumn("funny", lit(word))
    return inner


def with_jacket(word, df):
    return df.withColumn("jacket", lit(word))


@curry
def with_stuff1(arg1, arg2, df):
    return df.withColumn("stuff1", lit(f"{arg1} {arg2}"))


@curry
def with_stuff2(arg, df):
    return df.withColumn("stuff2", lit(arg))

