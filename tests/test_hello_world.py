import pytest

from quinn.spark import *
from quinn.hello_world import HelloWorld

from pyspark.sql.functions import col


class TestHelloWorld(object):

    def test_hello(self):
        assert(HelloWorld().hello() == "greetings matthew")

    def test_with_age_plus_two(self):
        data = [("jose", 1), ("li", 2), ("luisa", 3)]
        sourceDF = spark.createDataFrame(data, ["name", "age"])

        actualDF = HelloWorld().with_age_plus_two(sourceDF)

        expected_data = [("jose", 1, 3), ("li", 2, 4), ("luisa", 3, 5)]
        expectedDF = spark.createDataFrame(expected_data, ["name", "age", "age_plus_two"])

        assert(expectedDF.collect() == actualDF.collect())

