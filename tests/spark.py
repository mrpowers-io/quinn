import os
from pyspark.sql import SparkSession

if "SPARK_CONNECT_MODE_ENABLE" in os.environ:
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
else:
    spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()