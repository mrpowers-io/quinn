import timeit
import json

# import pyspark.sql.functions as F
# from pyspark.sql import DataFrame, SparkSession


def auto_timeit(stmt: str = "pass", setup: str = "pass") -> float:
    min_run_time = 0.2
    runtime_multiplier = 10
    n = 1
    t = timeit.timeit(stmt, setup, number=n)

    while t < min_run_time:
        n *= runtime_multiplier
        t = timeit.timeit(stmt, setup, number=n)

    return t / n  # normalise to time-per-run


def get_result(test_name: str, dataset: str, expr: str) -> None:
    setup = f"""import timeit
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
spark = SparkSession.builder.getOrCreate()
{dataset} = spark.read.parquet('benchmarks/data/mvv_{dataset}')
"""
    stmt = f"""{dataset}.{expr}"""
    result = auto_timeit(stmt, setup)

    summary = {
        "test_name": test_name,
        "dataset": dataset,
        "avg_runtime_seconds": result,
    }

    with open(f"benchmarks/results/{test_name}_{dataset}.json", "w") as f:
        json.dump(summary, f)


config = {
    "flatmap": {
        "expr": "select('mvv').rdd.flatMap(lambda x: x).collect()",
    },
}

DATASETS = [
    ("xsmall", 1_000),
    ("small", 100_000),
    # ("medium", 10_000_000),
    # ("large", 100_000_000),
]


for dataset in DATASETS:
    dataset_name, dataset_size = dataset
    print(f"Dataset: {dataset_name} ({dataset_size})")

    for test_name, test_config in config.items():
        print(f"Test: {test_name}======================")
        get_result(test_name=test_name, dataset=dataset_name, expr=test_config["expr"])
