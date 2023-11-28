import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("MyApp")
    .config("spark.executor.memory", "10G")
    .config("spark.driver.memory", "25G")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

result_df = (
    spark.read.json("benchmarks/results/*.json", multiLine=True)
    .select(
        "test_name",
        "dataset",
        "dataset_size",
        F.explode("runtimes").alias("runtime"),
    )
    .withColumnRenamed("dataset", "dataset_name")
    .withColumn(
        "dataset_size_formatted",
        F.concat(F.lit("n="), F.format_number(F.col("dataset_size"), 0)),
    )
    .withColumn(
        "dataset",
        F.concat(
            F.col("dataset_name"),
            F.lit(" ("),
            F.col("dataset_size_formatted"),
            F.lit(")"),
        ),
    )
    .toPandas()
)

result_df["dataset_name"] = pd.Categorical(
    result_df["dataset_name"],
    ["xsmall", "small", "medium", "large"],
)


fig = px.box(
    result_df,
    x="dataset_size_formatted",
    y="runtime",
    color="test_name",
    facet_col="dataset_name",
    points="all",
    title="Benchmark Results<br><sup>Spark 3.5, Python 3.12, M1 Macbook Pro 32GB RAM</sup></br>",
    labels={"runtime": "Runtime (seconds)"},
    category_orders={"dataset_name": ["xsmall", "small", "medium", "large"]},
)
fig.update_yaxes(matches=None)
fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
fig.update_xaxes(matches=None, title=None)
fig.show()
