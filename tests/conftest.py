from functools import partial

import pytest
from pyspark import SparkConf

from quinn.spark import SparkProvider


# FROM: https://hackebrot.github.io/pytest-tricks/fixtures_as_class_attributes/
def _inject(cls, names):
    @pytest.fixture(autouse=True)
    def auto_injector_fixture(self, request):
        for name in names:
            setattr(self, name, request.getfixturevalue(name))

    cls.__auto_injector_fixture = auto_injector_fixture
    return cls


def auto_inject_fixtures(*names):
    return partial(_inject, names=names)


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
    spark = SparkProvider.set_up_spark(
        "Testing", "local[*]", extra_dependencies=[], conf=test_spark_conf()
    )
    yield spark
    spark.stop()
