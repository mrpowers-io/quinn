import pyspark
from pyspark.sql import SparkSession

print("hi")

spark = SparkSession.builder.appName('my_app').getOrCreate()
sparkContext = spark.sparkContext
rdd=sparkContext.parallelize([1,2,3,4,5])
rddCollect = rdd.collect()
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))
print(rddCollect)

print("bye")