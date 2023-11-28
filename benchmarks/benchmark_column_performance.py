from __future__ import annotations

import json
import timeit
from pathlib import Path


def auto_timeit(
    stmt: str = "pass",
    setup: str = "pass",
    min_runtime_seconds: int = 2,
) -> list[float]:
    """Automatically determine the number of runs to perform to get a minimum."""
    min_runs = 5
    print(f"Running {stmt} 1 time...")
    t = timeit.repeat(stmt, setup, repeat=1, number=1)

    print(f"First run: {t[0]:.2f} seconds")
    if t[0] >= min_runtime_seconds:
        return t

    expected_runs_needed = int((min_runtime_seconds // t[0]) + 1)
    if expected_runs_needed < min_runs:
        expected_runs_needed = min_runs

    expected_runtime = t[0] * expected_runs_needed
    print(
        f"Running {stmt} {expected_runs_needed} times. Expected runtime: {expected_runtime:.2f} seconds..."
    )
    return timeit.repeat(stmt, setup, repeat=expected_runs_needed, number=1)


def get_result(
    test_name: str,
    dataset: dict,
    expr: str,
    min_runtime_seconds: int,
) -> None:
    """Run a test and save the results to a file."""
    setup = f"""import timeit
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
builder = (
    SparkSession.builder.appName("MyApp")
    .config("spark.executor.memory", "10G")
    .config("spark.driver.memory", "25G")
    .config("spark.sql.shuffle.partitions", "2")
)
spark = builder.getOrCreate()
{dataset['name']} = spark.read.parquet('benchmarks/data/mvv_{dataset['name']}')
"""
    stmt = expr.replace("df", dataset["name"])
    result = auto_timeit(stmt, setup, min_runtime_seconds)

    summary = {
        "test_name": test_name,
        "dataset": dataset["name"],
        "dataset_size": dataset["size"],
        "runtimes": result,
    }

    with Path.open(f"benchmarks/results/{test_name}_{dataset['name']}.json", "w") as f:
        json.dump(summary, f, indent=4)


config = {
    "flatmap": {"expr": "df.select('mvv').rdd.flatMap(lambda x: x).collect()"},
    "toPandas": {"expr": "list(df.select('mvv').toPandas()['mvv'])"},
    "map": {"expr": "df.select('mvv').rdd.map(lambda row : row[0]).collect()"},
    "collectlist": {"expr": "[row[0] for row in df.select('mvv').collect()]"},
    "localIterator": {"expr": "[r[0] for r in df.select('mvv').toLocalIterator()]"},
}


DATASETS = {
    "large": {"name": "large", "size": 100_000_000, "min_runtime_seconds": 1200},
    "medium": {"name": "medium", "size": 10_000_000, "min_runtime_seconds": 360},
    "small": {"name": "small", "size": 100_000, "min_runtime_seconds": 20},
    "xsmall": {"name": "xsmall", "size": 1_000, "min_runtime_seconds": 20},
}

for test_name, test_config in config.items():
    print(f"======================{test_name}======================")
    for dataset_name in DATASETS:
        dataset = DATASETS[dataset_name]
        print(f"TESTING DATASET {dataset['name']} [n={dataset['size']:,}]")
        get_result(
            test_name=test_name,
            dataset=dataset,
            expr=test_config["expr"],
            min_runtime_seconds=dataset["min_runtime_seconds"],
        )
