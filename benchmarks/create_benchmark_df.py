from __future__ import annotations

import random
from typing import TYPE_CHECKING, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

if TYPE_CHECKING:
    from pyspark.sql.dataframe import DataFrame


def generate_df(spark: SparkSession, n: int) -> DataFrame:
    """Generate a dataframe with a monotonically increasing id column and a random count column."""
    count_vals = [(random.randint(1, 10),) for _ in range(n)]  # noqa: S311
    output: DataFrame = (
        spark.createDataFrame(count_vals, schema=["count"])
        .withColumn("mvv", F.monotonically_increasing_id())
        .select("mvv", "count")
    )
    return output


def save_benchmark_df(
    spark: SparkSession,
    n: int,
    data_label: str,
    repartition_n: Optional[int] = None,
) -> None:
    """Save a benchmark dataframe to disk."""
    print(f"Generating benchmark df for n={n}")
    benchmark_df = generate_df(spark, n)

    if repartition_n is not None:
        benchmark_df = benchmark_df.repartition(repartition_n)

    benchmark_df.write.mode("overwrite").parquet(f"benchmarks/data/mvv_{data_label}")


if __name__ == "__main__":
    xsmall_n = 1_000
    small_n = 100_000
    medium_n = 10_000_000
    large_n = 100_000_000

    builder = (
        SparkSession.builder.appName("MyApp")
        .remote("sc://localhost")
        .config("spark.executor.memory", "20G")
        .config("spark.driver.memory", "25G")
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark = builder.getOrCreate()
    save_benchmark_df(spark, xsmall_n, "xsmall", 1)
    save_benchmark_df(spark, small_n, "small", 1)
    save_benchmark_df(spark, medium_n, "medium", 1)
    save_benchmark_df(spark, large_n, "large", 4)
