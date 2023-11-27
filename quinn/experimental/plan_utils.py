import contextlib
import re
from enum import Enum, auto
from io import StringIO

from pyspark.sql import DataFrame


class PlanType(Enum):
    """Enumeration with PlanTypes."""

    PARSED_LOGICAL_PLAN = auto()
    ANALYZED_LOGICAL_PLAN = auto()
    OPTIMIZED_LOGICAL_PLAN = auto()
    PHYSICAL_PLAN = auto()


def get_plan_from_df(df: DataFrame, mode: PlanType = PlanType.PHYSICAL_PLAN) -> str:
    """Get Spark plan from the given DataFrame.

    :param df: DataFrame to analyze
    :param mode: Type of plan: parsed logical, analyzed logical, optimized, physical (default)
    :returns: string representation of plan
    """
    with contextlib.redirect_stdout(StringIO()) as stdout:
        df.explain(extended=True)

    all_plan_lines = stdout.getvalue().split("\n")

    if mode == PlanType.PARSED_LOGICAL_PLAN:
        start = 1
        end = all_plan_lines.index("== Analyzed Logical Plan ==")

    if mode == PlanType.ANALYZED_LOGICAL_PLAN:
        # We need to add +1 here because Analyzed Logical Plan contains result schema as the first line
        start = all_plan_lines.index("== Analyzed Logical Plan ==") + 2
        end = all_plan_lines.index("== Optimized Logical Plan ==")

    if mode == PlanType.OPTIMIZED_LOGICAL_PLAN:
        start = all_plan_lines.index("== Optimized Logical Plan ==") + 1
        end = all_plan_lines.index("== Physical Plan ==")

    if mode == PlanType.PHYSICAL_PLAN:
        start = all_plan_lines.index("== Physical Plan ==") + 1
        end = -1

    return "\n".join(all_plan_lines[start:end])


# This function works from PySpark 3.0.0
def estimate_size_of_df(df: DataFrame) -> float:
    """Estimate the size in Bytes of the given DataFrame.

    If the size cannot be estimated return -1.0. It is possible if
    we failed to parse plan or, most probably, it is the case when statistics
    is unavailable. There is a problem that currently in the case of missing
    statistics spark return 8 (or 12) EiB. If your data size is really measured in EiB
    this function cannot help you. See https://github.com/apache/spark/pull/31817
    for details. Size is returned in Bytes!

    This function works only in PySpark 3.0.0 or higher!

    :param df: DataFrame
    :returns: size in bytes
    """
    with contextlib.redirect_stdout(StringIO()) as stdout:
        # mode argument was added in 3.0.0
        df.explain(mode="cost")

    # Get top line of Optimized Logical Plan
    # The output of df.explain(mode="cost") starts from the following line:
    # == Optimized Logical Plan ==
    # The next line after this should contain something like:
    # Statistics(sizeInBytes=3.0 MiB) (untis may be different)
    top_line = stdout.getvalue().split("\n")[1]

    # We need a pattern to parse the real size and untis
    pattern = r"^.*sizeInBytes=([0-9]+\.[0-9]+)\s(B|KiB|MiB|GiB|TiB|EiB).*$"

    _match = re.search(pattern, top_line)

    if _match:
        size = float(_match.groups()[0])
        units = _match.groups()[1]
    else:
        return -1

    if units == "KiB":
        size *= 1024

    if units == "MiB":
        size *= 1024 * 1024

    if units == "GiB":
        size *= 1024 * 1024 * 1024

    if units == "TiB":
        size *= 1024 * 1024 * 1024 * 1024

    if units == "EiB":
        # Most probably it is the case when Statistics is unavailable
        # In this case spark just returns max possible value
        # See https://github.com/apache/spark/pull/31817 for details
        return -1

    return size
