from __future__ import annotations

import logging
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

STANDALONE = "local[*]"


def quiet_py4j() -> None:
    """Sets logging level of py4h."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.INFO)


class SparkProvider:
    """Class for creating and destroying SparkSession."""

    def __init__(
        self: SparkProvider,
        app_name: str,
        conf: SparkConf | None = None,
        extra_dependencies: list[str] | None = None,
        extra_files: list[str] | None = None,
    ) -> None:
        """Initialize SparkSession."""
        self.spark = self.set_up_spark(
            app_name, self.master, conf, extra_dependencies, extra_files,
        )

    @property
    def master(self: SparkProvider) -> str:  # noqa: D102
        return os.getenv("SPARK_MASTER", STANDALONE)

    @staticmethod
    def set_up_spark(  # noqa: D102
        app_name: str,
        master: str = STANDALONE,
        conf: SparkConf = None,
        extra_dependencies: list[str] | None = None,
        extra_files: list[str] | None = None,
    ) -> SparkSession:
        conf = conf if conf else SparkConf()

        if extra_dependencies:
            spark_dependencies = ",".join(extra_dependencies)
            conf.set("spark.jars.packages", spark_dependencies)

        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config(conf=conf)
            .getOrCreate()
        )

        extra_files = extra_files if extra_files else []
        for extra_file in extra_files:
            spark.sparkContext.addPyFile(extra_file)

        quiet_py4j()
        return spark

    @staticmethod
    def tear_down_spark(spark: SparkSession) -> None:  # noqa: D102
        spark.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        spark._jvm.System.clearProperty("spark.driver.port")  # noqa: SLF001
