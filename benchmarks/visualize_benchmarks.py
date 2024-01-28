from __future__ import annotations

from datetime import datetime as dt
from pathlib import Path

import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F  # noqa: N812
import pytz
from pyspark.sql import SparkSession


def parse_results(spark: SparkSession) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Parse benchmark results into a Pandas DataFrame."""
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

    if not isinstance(result_df, pd.DataFrame):
        raise TypeError

    result_df["dataset_name"] = pd.Categorical(
        result_df["dataset_name"],
        ["xsmall", "small", "medium", "large"],
    )

    average_df = (
        result_df[["test_name", "dataset_size", "runtime"]]
        .groupby(["test_name", "dataset_size"], observed=False)
        .mean()
        .reset_index()
    )

    benchmark_date = get_benchmark_date(benchmark_path="benchmarks/results/")
    return result_df, average_df, benchmark_date


def save_boxplot(df: pd.DataFrame, benchmark_date: str) -> None:
    """Displays faceted boxplot of benchmark results."""
    machine_config = "Python 3.12.0, Spark 3.5, Pandas 2.1.3, M1 Macbook Pro 32GB RAM"
    subtitle = f"<sup>{benchmark_date} | {machine_config}</sup>"

    fig = px.box(
        df,
        x="dataset_size_formatted",
        y="runtime",
        color="test_name",
        facet_col="dataset_name",
        points="all",
        title=f"Column to List Benchmark Results<br>{subtitle}</br>",
        labels={"runtime": "Runtime (seconds)"},
        category_orders={
            "dataset_name": ["xsmall", "small", "medium", "large"],
            "test_name": [
                "localIterator",
                "collectlist",
                "map",
                "flatmap",
                "toPandas",
            ],
        },
        color_discrete_map={
            "collectlist": "#636EFA",
            "localIterator": "#EF553B",
            "toPandas": "#00CC96",
            "map": "#AB63FA",
            "flatmap": "#FFA15A",
        },
    )
    fig.update_yaxes(matches=None)
    fig.update_yaxes({"tickfont": {"size": 9}})
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    fig.update_xaxes(matches=None, title=None)
    fig.update_layout(legend_title_text="")

    fig.write_image(
        "benchmarks/images/column_to_list_boxplot.svg",
        width=1000,
        height=700,
    )


def save_line_plot(df: pd.DataFrame, benchmark_date: str) -> None:
    """Displays line plot of average benchmark results."""
    machine_config = "Python 3.12.0, Spark 3.5, Pandas 2.1.3, M1 Macbook Pro 32GB RAM"
    subtitle = f"<sup>{benchmark_date} | {machine_config}</sup>"
    fig = px.line(
        df,
        x="dataset_size",
        y="runtime",
        log_x=True,
        color="test_name",
        title=f"Column to List Benchmark Results<br>{subtitle}</br>",
        labels={"runtime": "Runtime (seconds)", "dataset_size": "Number of Rows"},
        category_orders={
            "test_name": [
                "localIterator",
                "collectlist",
                "map",
                "flatmap",
                "toPandas",
            ],
        },
        color_discrete_map={
            "collectlist": "#636EFA",
            "localIterator": "#EF553B",
            "toPandas": "#00CC96",
            "map": "#AB63FA",
            "flatmap": "#FFA15A",
        },
    )
    fig.update_traces(mode="markers+lines")
    fig.update_traces(marker={"size": 12})
    fig.update_layout(legend_title_text="")

    fig.write_image(
        "benchmarks/images/column_to_list_line_plot.svg",
        width=900,
        height=450,
    )


def get_benchmark_date(benchmark_path: str) -> str:
    """Returns the date of the benchmark results."""
    path = Path(benchmark_path)
    benchmark_ts = path.stat().st_mtime
    return dt.fromtimestamp(
        benchmark_ts,
        tz=pytz.timezone("US/Eastern"),
    ).strftime("%Y-%m-%d")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MyApp")  # type: ignore # noqa: PGH003
        .config("spark.executor.memory", "10G")
        .config("spark.driver.memory", "25G")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    result_df, average_df, benchmark_date = parse_results(spark)
    save_boxplot(result_df, benchmark_date)
    save_line_plot(average_df, benchmark_date)
