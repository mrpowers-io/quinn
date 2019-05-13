import logging
import os
from typing import List, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

STANDALONE = 'local[*]'


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)


class SparkProvider:

    def __init__(self,
                 app_name: str,
                 conf: Optional[SparkConf] = None,
                 extra_dependencies: Optional[List[str]] = None,
                 extra_files: Optional[List[str]] = None):
        self.spark = self.set_up_spark(app_name, self.master, conf, extra_dependencies, extra_files)

    @property
    def master(self) -> str:
        return os.getenv('SPARK_MASTER', STANDALONE)

    @staticmethod
    def set_up_spark(app_name: str,
                     master: str = STANDALONE,
                     conf: SparkConf = None,
                     extra_dependencies: List[str] = None,
                     extra_files: List[str] = None) -> SparkSession:
        conf = conf if conf else SparkConf()

        if extra_dependencies:
            spark_dependencies = ','.join(extra_dependencies)
            conf.set('spark.jars.packages', spark_dependencies)

        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(master) \
            .config(conf=conf) \
            .getOrCreate()

        extra_files = extra_files if extra_files else []
        for extra_file in extra_files:
            spark.sparkContext.addPyFile(extra_file)

        quiet_py4j()
        return spark

    @staticmethod
    def tear_down_spark(spark):
        spark.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        spark._jvm.System.clearProperty('spark.driver.port')
