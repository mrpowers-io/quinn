from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
import sys
from typing import Any

from pyspark.sql.types import StructField, StructType


def column_to_list(df: DataFrame, col_name: str) -> list[Any]:
    """Collect column to list of values.

    :param df: Input DataFrame
    :type df: pyspark.sql.DataFrame
    :param col_name: Column to collect
    :type col_name: str
    :return: List of values
    :rtype: List[Any]
    """
    if "pyspark" not in sys.modules:
        raise ImportError

    # sparksession from df is not available in older versions of pyspark
    if sys.modules["pyspark"].__version__ < "3.3.0":
        return [row[0] for row in df.select(col_name).collect()]

    spark_session = df.sparkSession.getActiveSession()
    if spark_session is None:
        return [row[0] for row in df.select(col_name).collect()]

    pyarrow_enabled = (
        spark_session.conf.get(
            "spark.sql.execution.arrow.pyspark.enabled",
        )
        == "true"
    )

    pyarrow_valid = pyarrow_enabled and sys.modules["pyarrow"].__version__ >= "0.17.0"

    pandas_exists = "pandas" in sys.modules
    pandas_valid = pandas_exists and sys.modules["pandas"].__version__ >= "0.24.2"

    if pyarrow_valid and pandas_valid:
        return df.select(col_name).toPandas()[col_name].tolist()

    return [row[0] for row in df.select(col_name).collect()]


def two_columns_to_dictionary(
    df: DataFrame,
    key_col_name: str,
    value_col_name: str,
) -> dict[str, Any]:
    """Collect two columns as dictionary when first column is key and second is value.

    :param df: Input DataFrame
    :type df: pyspark.sql.DataFrame
    :param key_col_name: Key-column
    :type key_col_name: str
    :param value_col_name: Value-column
    :type value_col_name: str
    :return: Dictionary with values
    :rtype: Dict[str, Any]
    """
    k, v = key_col_name, value_col_name
    return {x[k]: x[v] for x in df.select(k, v).collect()}


def to_list_of_dictionaries(df: DataFrame) -> list[dict[str, Any]]:
    """Convert a Spark DataFrame to a list of dictionaries.

    :param df: The Spark DataFrame to convert.
    :type df: :py:class:`pyspark.sql.DataFrame`
    :return: A list of dictionaries representing the rows in the DataFrame.
    :rtype: List[Dict[str, Any]]
    """
    return list(map(lambda r: r.asDict(), df.collect()))  # noqa: C417


def print_athena_create_table(
    df: DataFrame,
    athena_table_name: str,
    s3location: str,
) -> None:
    """Generate the Athena create table statement for a given DataFrame.

    :param df: The pyspark.sql.DataFrame to use
    :param athena_table_name: The name of the athena table to generate
    :param s3location: The S3 location of the parquet data
    :return: None
    """
    fields = df.schema

    print(f"CREATE EXTERNAL TABLE IF NOT EXISTS `{athena_table_name}` ( ")

    for field in fields.fieldNames()[:-1]:
        print("\t", f"`{fields[field].name}` {fields[field].dataType.simpleString()}, ")
    last = fields[fields.fieldNames()[-1]]
    print("\t", f"`{last.name}` {last.dataType.simpleString()} ")

    print(")")
    print("STORED AS PARQUET")
    print(f"LOCATION '{s3location}'\n")


def show_output_to_df(show_output: str, spark: SparkSession) -> DataFrame:
    """Show output as spark DataFrame.

    :param show_output: String representing output of 'show' command in spark
    :type show_output: str
    :param spark: SparkSession object
    :type spark: SparkSession
    :return: DataFrame object containing output of a show command in spark
    :rtype: Dataframe
    """
    lines = show_output.split("\n")
    ugly_column_names = lines[1]
    pretty_column_names = [i.strip() for i in ugly_column_names[1:-1].split("|")]
    pretty_data = []
    ugly_data = lines[3:-1]
    for row in ugly_data:
        r = [i.strip() for i in row[1:-1].split("|")]
        pretty_data.append(tuple(r))
    return spark.createDataFrame(pretty_data, pretty_column_names)


def create_df(spark: SparkSession, rows_data, col_specs) -> DataFrame:  # noqa: ANN001
    """Create a new DataFrame from the given data and column specs.

    The returned DataFrame s created using the StructType and StructField classes provided by PySpark.

    :param spark: SparkSession object
    :type spark: SparkSession
    :param rows_data: the data used to create the DataFrame
    :type rows_data: array-like
    :param col_specs: list of tuples containing the name and type of the field
    :type col_specs: list of tuples
    :return: a new DataFrame
    :rtype: DataFrame
    """
    struct_fields = list(map(lambda x: StructField(*x), col_specs))  # noqa: C417
    return spark.createDataFrame(data=rows_data, schema=StructType(struct_fields))
