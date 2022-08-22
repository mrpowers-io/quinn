from functools import partial

import pytest
from pyspark import SparkConf

from tests.spark import TestSparkProvider
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def test_spark_conf() -> SparkConf:
    return (
        SparkConf()
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
        .set("spark.speculation", False)
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.ui.enabled", "false")
    )


@pytest.fixture(scope="session")
def spark():
    spark = TestSparkProvider.set_up_spark(
        "Testing", "local[*]", extra_dependencies=[], conf=test_spark_conf()
    )
    yield spark
    spark.stop()
