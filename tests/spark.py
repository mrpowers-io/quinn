from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").appName("quinn").getOrCreate()