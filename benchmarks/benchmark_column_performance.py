import timeit
import json


def auto_timeit(stmt: str = "pass", setup: str = "pass") -> list[float]:
    min_run_time_seconds = 10
    runtime_multiplier = 5
    n = 1
    t = timeit.repeat(stmt, setup, repeat=n, number=1)

    while sum(t) < min_run_time_seconds:
        n *= runtime_multiplier
        t = timeit.repeat(stmt, setup, repeat=n, number=1)

    return t


def get_result(test_name: str, dataset: dict, expr: str) -> None:
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
    stmt = f"""{dataset['name']}.{expr}"""
    result = auto_timeit(stmt, setup)

    summary = {
        "test_name": test_name,
        "dataset": dataset["name"],
        "dataset_size": dataset["size"],
        "runtimes": result,
    }

    with open(f"benchmarks/results/{test_name}_{dataset['name']}.json", "w") as f:
        json.dump(summary, f, indent=4)


config = {
    "flatmap": {
        "expr": "select('mvv').rdd.flatMap(lambda x: x).collect()",
    },
}


DATASETS = {
    "xsmall": {"name": "xsmall", "size": 1_000},
    "small": {"name": "small", "size": 100_000},
    "medium": {"name": "medium", "size": 10_000_000},
    "large": {"name": "large", "size": 100_000_000},
}

for dataset_name in DATASETS:
    dataset = DATASETS[dataset_name]
    print(f"Dataset: {dataset['name']} ({dataset['size']})")

    for test_name, test_config in config.items():
        print(f"Test: {test_name}======================")
        get_result(test_name=test_name, dataset=dataset, expr=test_config["expr"])
