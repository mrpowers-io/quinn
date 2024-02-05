from functools import partial

import chispa
from pyspark.sql.functions import col

# from tests.conftest import auto_inject_fixtures
from ..spark import *

from .dataframe_transformations import (
    with_greeting,
    with_something,
    with_funny,
    with_jacket,
)


# @auto_inject_fixtures("spark")
def test_verbose_code_without_transform(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    df1 = with_greeting(source_df)
    df2 = with_something(df1, "moo")
    expected_data = [
        ("jose", 1, "hi", "moo"),
        ("li", 2, "hi", "moo"),
        ("liz", 3, "hi", "moo"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["name", "age", "greeting", "something"]
    )
    chispa.assert_df_equality(df2, expected_df, ignore_nullable=True)


def test_transform_with_lambda(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(
        lambda df: df.withColumn("age_times_two", col("age") * 2)
    )
    expected_data = [("jose", 1, 2), ("li", 2, 4), ("liz", 3, 6)]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "age_times_two"])
    chispa.assert_df_equality(actual_df, expected_df)


def test_transform_with_no_arg_fun(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(lambda df: with_greeting(df))
    expected_data = [("jose", 1, "hi"), ("li", 2, "hi"), ("liz", 3, "hi")]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "greeting"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_transform_with_one_arg_fun(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(lambda df: with_something(df, "crazy"))
    expected_data = [("jose", 1, "crazy"), ("li", 2, "crazy"), ("liz", 3, "crazy")]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "something"])
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_chain_transforms(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(with_greeting).transform(
        lambda df: with_something(df, "crazy")
    )
    expected_data = [
        ("jose", 1, "hi", "crazy"),
        ("li", 2, "hi", "crazy"),
        ("liz", 3, "hi", "crazy"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["name", "age", "greeting", "something"]
    )
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_transform_with_closure(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(with_greeting).transform(  # no lambda required
        with_funny("haha")
    )
    expected_data = [
        ("jose", 1, "hi", "haha"),
        ("li", 2, "hi", "haha"),
        ("liz", 3, "hi", "haha"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["name", "age", "greeting", "funny"]
    )
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_transform_with_functools_partial(spark):
    data = [("jose", 1), ("li", 2), ("liz", 3)]
    source_df = spark.createDataFrame(data, ["name", "age"])
    actual_df = source_df.transform(
        partial(with_greeting)
    ).transform(  # partial is optional for transformations that only take a single DataFrame argument
        partial(with_jacket, "warm")
    )
    expected_data = [
        ("jose", 1, "hi", "warm"),
        ("li", 2, "hi", "warm"),
        ("liz", 3, "hi", "warm"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["name", "age", "greeting", "jacket"]
    )
    chispa.assert_df_equality(actual_df, expected_df, ignore_nullable=True)
