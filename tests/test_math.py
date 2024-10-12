import pyspark.sql.functions as F

import quinn
import math
from .spark import spark


def test_rand_laplace():
    stats = (
        spark.range(100000)
        .select(quinn.rand_laplace(0.0, 1.0, 42))
        .agg(
            F.mean("laplace_random").alias("mean"),
            F.stddev("laplace_random").alias("std_dev"),
        )
        .first()
    )

    laplace_mean = stats["mean"]
    laplace_stddev = stats["std_dev"]

    # Laplace distribution with mean=0.0 and scale=1.0 has mean=0.0 and stddev=sqrt(2.0)
    assert abs(laplace_mean) <= 0.1
    assert abs(laplace_stddev - math.sqrt(2.0)) < 0.5


def test_rand_range():
    lower_bound = 5
    upper_bound = 10
    stats = (
        spark.range(1000)
        .select(quinn.rand_range(lower_bound, upper_bound).alias("rand_uniform"))
        .agg(F.min("rand_uniform").alias("min"), F.min("rand_uniform").alias("max"))
        .first()
    )

    uniform_min = stats["min"]
    uniform_max = stats["max"]

    assert lower_bound <= uniform_min <= uniform_max <= upper_bound


def test_randn():
    mean = 1.0
    variance = 2.0
    stats = (
        spark.range(1000)
        .select(quinn.randn(mean, variance).alias("rand_normal"))
        .agg(
            F.mean("rand_normal").alias("agg_mean"),
            F.variance("rand_normal").alias("agg_variance"),
        )
        .first()
    )

    agg_mean = stats["agg_mean"]
    agg_variance = stats["agg_variance"]

    assert agg_mean - mean <= 0.1
    assert agg_variance - variance <= 0.1
