import pytest

from functools import partial

from spark import *
from quinn.extensions import *
from .dataframe_transformations import *

from cytoolz.functoolz import compose

from pyspark.sql.functions import col

class TestDataFrameExt:

    def test_verbose_code_without_transform(self):
        data = [
            ("jose", 1),
            ("li", 2),
            ("liz", 3)
        ]
        source_df = spark.createDataFrame(
            data,
            ["name", "age"]
        )

        df1 = with_greeting(source_df)
        df2 = with_something(df1, "moo")

        expected_data = [
            ("jose", 1, "hi", "moo"),
            ("li", 2, "hi", "moo"),
            ("liz", 3, "hi", "moo")
        ]
        expected_df = spark.createDataFrame(
            expected_data,
            ["name", "age", "greeting", "something"]
        )

        assert expected_df.collect() == df2.collect()

    def test_transform_with_lambda(self):
        data = [
            ("jose", 1),
            ("li", 2),
            ("liz", 3)
        ]
        source_df = spark.createDataFrame(
            data,
            ["name", "age"]
        )

        actual_df = source_df.transform(
            lambda df: df.withColumn("age_times_two", col("age") * 2)
        )

        expected_data = [
            ("jose", 1, 2),
            ("li", 2, 4),
            ("liz", 3, 6)
        ]
        expected_df = spark.createDataFrame(
            expected_data,
            ["name", "age", "age_times_two"]
        )

        assert expected_df.collect() == actual_df.collect()

    def test_transform_with_no_arg_fun(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual_df = source_df.transform(lambda df: with_greeting(df))

        expected_data = [("jose", 1, "hi"), ("li", 2, "hi"), ("liz", 3, "hi")]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "greeting"])

        assert expected_df.collect() == actual_df.collect()

    def test_transform_with_one_arg_fun(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual_df = source_df.transform(lambda df: with_something(df, "crazy"))

        expected_data = [("jose", 1, "crazy"), ("li", 2, "crazy"), ("liz", 3, "crazy")]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "something"])

        assert expected_df.collect() == actual_df.collect()

    def test_chain_transforms(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual_df = (source_df
            .transform(with_greeting)
            .transform(lambda df: with_something(df, "crazy")))

        expected_data = [("jose", 1, "hi", "crazy"), ("li", 2, "hi", "crazy"), ("liz", 3, "hi", "crazy")]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "greeting", "something"])
        assert expected_df.collect() == actual_df.collect()

    def test_transform_with_closure(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual_df = (source_df
            .transform(with_greeting)  # no lambda required
            .transform(with_funny("haha")))

        expected_data = [
            ("jose", 1, "hi", "haha"),
            ("li", 2, "hi", "haha"),
            ("liz", 3, "hi", "haha")
        ]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "greeting", "funny"])
        assert expected_df.collect() == actual_df.collect()

    def test_transform_with_functools_partial(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        actual_df = (source_df
            .transform(partial(with_greeting)) # partial is optional for transformations that only take a single DataFrame argument
            .transform(partial(with_jacket, "warm")))

        expected_data = [
            ("jose", 1, "hi", "warm"),
            ("li", 2, "hi", "warm"),
            ("liz", 3, "hi", "warm")
        ]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "greeting", "jacket"])
        assert expected_df.collect() == actual_df.collect()

    def test_currying(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        pipeline = compose(with_stuff1("nice", "person"), with_stuff2("yoyo"))
        actual_df = pipeline(source_df)

        expected_data = [
            ("jose", 1, "yoyo", "nice person"),
            ("li", 2, "yoyo", "nice person"),
            ("liz", 3, "yoyo", "nice person")
        ]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "stuff2", "stuff1"])
        assert expected_df.collect() == actual_df.collect()

    def test_reversed_currying(self):
        data = [("jose", 1), ("li", 2), ("liz", 3)]
        source_df = spark.createDataFrame(data, ["name", "age"])

        pipeline = compose(*reversed([
            with_stuff1("nice", "person"),
            with_stuff2("yoyo")
        ]))
        actual_df = pipeline(source_df)

        print("***")
        print(actual_df.show())

        expected_data = [
            ("jose", 1, "nice person", "yoyo"),
            ("li", 2, "nice person", "yoyo"),
            ("liz", 3, "nice person", "yoyo")
        ]
        expected_df = spark.createDataFrame(expected_data, ["name", "age", "stuff2", "stuff1"])
        assert expected_df.collect() == actual_df.collect()

