import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


def generate_df(spark: SparkSession, n: int) -> DataFrame:
    # TODO: make this more performant
    count_vals = [(random.randint(1, 10),) for _ in range(n)]
    output: DataFrame = (
        spark.createDataFrame(count_vals, schema=["count"])
        .withColumn("mvv", F.monotonically_increasing_id())
        .select("mvv", "count")
    )
    return output


def save_benchmark_df(spark: SparkSession, n: int, data_label: str) -> None:
    print(f"Generating benchmark df for n={n}")
    benchmark_df = generate_df(spark, n)
    benchmark_df.write.mode("overwrite").parquet(f"benchmarks/data/mvv_{data_label}")


if __name__ == "__main__":
    xsmall_n = 1_000
    small_n = 100_000
    medium_n = 10_000_000
    large_n = 100_000_000

    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.executor.memory", "10G")
        .config("spark.driver.memory", "25G")
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark = builder.getOrCreate()
    save_benchmark_df(spark, xsmall_n, "xsmall")
    save_benchmark_df(spark, small_n, "small")
    save_benchmark_df(spark, medium_n, "medium")
    save_benchmark_df(spark, large_n, "large")
