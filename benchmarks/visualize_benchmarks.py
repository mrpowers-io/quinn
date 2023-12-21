from __future__ import annotations

import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import SparkSession


def parse_results(spark: SparkSession) -> tuple[pd.DataFrame]:
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

    return result_df, average_df


def show_boxplot(df: pd.DataFrame) -> None:
    """Displays faceted boxplot of benchmark results."""
    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
    machine_config = "Python 3.12.0, Spark 3.5, Pandas 2.1.3, M1 Macbook Pro 32GB RAM"
    subtitle = f"<sup>{today_str} | {machine_config}</sup>"

    fig = px.box(
        df,
        x="dataset_size_formatted",
        y="runtime",
        color="test_name",
        facet_col="dataset_name",
        points="all",
        title=f"Column to List Benchmark Results<br>{subtitle}</br>",
        labels={"runtime": "Runtime (seconds)"},
        category_orders={"dataset_name": ["xsmall", "small", "medium", "large"]},
    )
    fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    fig.update_xaxes(matches=None, title=None)
    fig.update_layout(legend_title_text="")

    fig.show()


def show_line_plot(df: pd.DataFrame) -> None:
    """Displays line plot of average benchmark results."""
    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
    machine_config = "Python 3.12.0, Spark 3.5, Pandas 2.1.3, M1 Macbook Pro 32GB RAM"
    subtitle = f"<sup>{today_str} | {machine_config}</sup>"
    fig = px.line(
        df,
        x="dataset_size",
        y="runtime",
        log_x=True,
        color="test_name",
        title=f"Column to List Benchmark Results<br>{subtitle}</br>",
        labels={"runtime": "Runtime (seconds)", "dataset_size": "Number of Rows"},
    )
    fig.update_traces(mode="markers+lines")
    fig.update_traces(marker={"size": 15})
    fig.update_layout(legend_title_text="")

    fig.show()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MyApp")
        .config("spark.executor.memory", "10G")
        .config("spark.driver.memory", "25G")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    result_df, average_df = parse_results(spark)
    show_boxplot(result_df)
    show_line_plot(average_df)
