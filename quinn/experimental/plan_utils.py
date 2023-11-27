from __future__ import annotations

import contextlib
import re
import warnings
from dataclasses import dataclass
from enum import Enum, auto
from io import StringIO

from pyspark.sql import DataFrame

warnings.warn("This API is experimental and not stable!", stacklevel=2)


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


def _bytes2mb(bb: float) -> float:
    return bb / 1024 / 1024


# This function works from PySpark 3.0.0
def estimate_size_of_df(df: DataFrame, size_in_mb: bool = False) -> float:
    """Estimate the size in Bytes of the given DataFrame.

    If the size cannot be estimated return -1.0. It is possible if
    we failed to parse plan or, most probably, it is the case when statistics
    is unavailable. There is a problem that currently in the case of missing
    statistics spark return 8 (or 12) EiB. If your data size is really measured in EiB
    this function cannot help you. See https://github.com/apache/spark/pull/31817
    for details. Size is returned in Bytes!

    This function works only in PySpark 3.0.0 or higher!

    :param df: DataFrame
    :param size_in_mb: Convert output to Mb instead of B
    :returns: size in bytes (or Mb if size_in_mb)
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
        size = -1

    if size < 0:
        return size

    if size_in_mb:
        return _bytes2mb(size)  # size in Mb

    return size  # size in bytes


def _extract_alias_expressions(col: str, line: str) -> str:  # noqa: C901
    """Inner function. Extract expression before ... AS col from the line."""
    num_close_parentheses = 0
    idx = line.index(f" AS {col}")
    alias_expr = []

    if line[idx - 1] != ")":
        """It is possible that there is no expression.
        It is the case when we just make a rename of the column. In the plan
        it will look like `col#123 AS col#321`;
        """
        for j in range(idx - 1, 0, -1):
            alias_expr.append(line[j])
            if line[j - 1] == "[":
                break
            if line[j - 1] == " ":
                break
        return "".join(alias_expr)[::-1]

    """In all other cases there will be `(` at the end of the expr before AS.
    Our goal is to go symbol by symbol back until we balance all the parentheses.
    """
    for i in range(idx - 1, 0, -1):
        alias_expr.append(line[i])
        if line[i] == ")":
            # Add parenthesis
            num_close_parentheses += 1
        if line[i] == "(":
            if num_close_parentheses == 1:
                # Parentheses are balanced
                break
            # Remove parenthesis
            num_close_parentheses -= 1

    """After balancing parentheses we need to parse leading expression.
    It is always here because we checked single alias case separately."""
    for j in range(i, 0, -1):
        alias_expr.append(line[j])
        if line[j - 1] == "[":
            break
        if line[j - 1] == " ":
            break

    return "".join(alias_expr[::-1])


def _get_aliases(col: str, line: str) -> tuple[list[str], str]:
    """Inner function. Returns all the aliases from the expr and expr itself."""
    alias_exp = _extract_alias_expressions(col, line)
    # Regexp to extract columns: each column has a pattern like col_name#1234
    return (re.findall(r"[\w\d]+#[\w\d]+", alias_exp), alias_exp)


def _node2column(node: str) -> str:
    """Inner function. Transform the node from plan to column name.
    Like: col_11#1234L -> col_11.
    """
    match_ = re.match(r"([\w\d]+)#[\w\d]+", node)
    if match_:
        return match_.groups()[0]

    # This part should be unreachable! If reached may cause problems.
    # TODO: check this part # noqa: TD002, TD003, FIX002
    return ""


def _add_aggr_or_not(expr: str, line: str) -> str:
    """If the expr is aggregation we should add agg keys to the beginning."""
    # We are checking for aggregation pattern
    match_ = re.match(r"^[\s\+\-:]*Aggregate\s\[([\w\d#,\s]+)\].*$", line)
    if match_:
        agg_expr = match_.groups()[0]
        return (
            "GroupBy: " + re.sub(r"([\w\d]+)#([\w\d]+)", r"\1", agg_expr) + f"\n{expr}"
        )

    # If not just return an original expr
    return expr


@dataclass
class ColumnLineageGraph:
    """Structure to represent columnar data lineage."""

    nodes: list[int]  # list of hash values that represent nodes
    edges: list[list[int]]  # list of edges in the form of list of pairs
    node_attrs: dict[int, str]  # labels of nodes (expressions)


def _get_graph(lines: list[str], node: str):  # noqa: ANN202, PLR0915, PLR0912, C901
    """Inner function with signature for recursion."""
    nodes = []
    edges = []
    node_attrs = {}

    # TODO: Add asserts to avoid stack-overflow with endless recursion. # noqa: TD002, TD003, FIX002
    for i, l in enumerate(lines):  # noqa: E741
        """Iteration over lines of logical plan."""

        # We should use hash of line + node as a key in the graph.
        # It is not enough to use only hash of line because the same line
        # may be related to multiple nodes!
        # A good example is reading the CSV that is represented by one line!
        h = hash(l + node)

        # If the current node is not root we need to store hash of previous node.
        prev_h = None if not nodes else nodes[-1]

        if node not in l:
            # TODO: revisit this part! # noqa: TD002, TD003, FIX002
            continue
        if f"AS {node}" in l:
            """It is a hard case, when a node is an alias to some expression."""
            aliases, expr = _get_aliases(node, l)
            # For visualization we need to transform from nodes to columns
            expr = re.sub(r"([\w\d]+)#([\w\d]+)", r"\1", expr)

            # Append a new node
            nodes.append(h)
            # Append expr as an attribute of the node
            node_attrs[h] = _add_aggr_or_not(f"{expr} AS {_node2column(node)}", l)

            if len(aliases) == 1:
                # It is the case of simple alis
                # Like col1#123 AS col2#321
                # In this case we just replace an old node by new one.
                if prev_h:
                    edges.append([h, prev_h])
                node = aliases[0]
            else:
                # It is a case of complex expression.
                # Here we recursively go through all the nodes from expr.
                if prev_h:
                    edges.append([h, prev_h])
                for aa in aliases:
                    # Get graph from sub-column
                    sub_nodes, sub_edges, sub_attrs = _get_graph(lines[i:], aa)

                    # Add everything to the current graph
                    nodes.extend(sub_nodes)
                    edges.extend(sub_edges)
                    node_attrs = {**node_attrs, **sub_attrs}

                    # Add connection between top subnode and node
                    edges.append([sub_nodes[0], h])
                return (nodes, edges, node_attrs)
        else:  # noqa: PLR5501
            # Continue of the simple alias or expr case
            # In the future that may be more cases, that is the reason of nested if instead of elif
            if "Relation" in l:
                nodes.append(h)
                if prev_h:
                    edges.append([h, prev_h])

                # It is a pattern, related to data-sources (like CSV)
                match_ = re.match(r"[\s\+\-:]*Relation\s\[.*\]\s(\w+)", l)
                if match_:
                    s_ = "Read from {}: {}"
                    # Add data-source as a node
                    node_attrs[h] = s_.format(match_.groups()[0], _node2column(node))
                else:
                    # We need it to avoid empty graphs and related runtime exceptions
                    print(l)
                    node_attrs[h] = f"Relation to: {_node2column(node)}"

            elif "Join" in l:
                # TODO: this part is unreachable for now because if the line does not # noqa: TD002, TD003, FIX002
                # contain node we are skipping it.
                # It is a big question, should we parse join or not.
                nodes.append(h)
                if prev_h:
                    edges.append([h, prev_h])
                match_ = re.match(r"[\s\+\-:]*Join\s(\w+),\s\((.*)\)", l)
                if match_:
                    join_type = match_.groups()[0]
                    join_expr = match_.groups()[1]
                    join_expr_clr = re.sub(r"([\w\d]+)#([\w\d]+)", r"\1", join_expr)
                    node_attrs[h] = f"{join_type}: {join_expr_clr}"
            else:
                continue

    if not nodes:
        # Just the case of empty return. We need to avoid it.
        # I'm not sure that line is reachable.
        nodes.append(h)
        node_attrs[h] = f"Select: {_node2column(node)}"

    return (nodes, edges, node_attrs)


def get_column_lineage(df: DataFrame, column: str) -> ColumnLineageGraph:
    """Get data lineage on the level of the given column.

    Currently Union operation is not supported! API is unstable, no guarantee
    that custom spark operations or connectors won't break it!

    :param df: DataFrame
    :param column: column
    :returns: Struct with nodes, edges and attributes
    """
    lines = get_plan_from_df(df, PlanType.ANALYZED_LOGICAL_PLAN).split("\n")

    # Top line should contain plan-id of our column. We need it.
    # Regular pattern of node is column#12345L or [\w\d]+#[\w\d]+
    match_ = re.match(r".*(" + column + r"#[\w\d]+).*", lines[0])
    if match_:
        node = match_.groups()[0]
    else:
        err = f"There is no column {column} in the final schema of DF!"
        raise KeyError(err)

    nodes, edges, attrs = _get_graph(lines, node)

    return ColumnLineageGraph(nodes, edges, attrs)


def get_column_lineage_graph(
    df: DataFrame,
    column: str,
) -> "networkx.DiGraph":  # noqa: F821
    """Get data lineage one the level of the given column. Returns Graph representation.

    :param df: DataFrame
    :param column: column
    :returns: networkx.DiGraph
    """
    try:
        import networkx as nx
    except ModuleNotFoundError as e:
        err = "NetworkX is not installed. Try `pip install networkx`. "
        err += (
            "You may use `get_column_lineage` instead, that doesn't require NetworkX."
        )
        raise ModuleNotFoundError(err) from e

    lineage = get_column_lineage(df, column)
    g = nx.DiGraph()

    g.add_nodes_from(lineage.nodes)
    g.add_edges_from(lineage.edges)
    nx.set_node_attributes(g, {k: {"expr": v} for k, v in lineage.node_attrs.items()})

    return g


def plot_column_lineage_graph(
    df: DataFrame,
    column: str,
) -> "matplotlib.pyplot.Figure":  # noqa: F821
    """Plot the column lineage graph as matplotlib figure.

    :param df: DataFrame
    :param column: column
    :returns: matplotlib.pyplot.Figure
    """
    try:
        import networkx as nx
        from networkx.drawing.nx_agraph import graphviz_layout
    except ModuleNotFoundError as e:
        err = "NetworkX is not installed. Try `pip install networkx`. "
        err += (
            "You may use `get_column_lineage` instead, that doesn't require NetworkX."
        )
        raise ModuleNotFoundError(err) from e

    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as e:
        err = "You need matplotlib installed to draw the Graph"
        raise ModuleNotFoundError(err) from e

    import importlib

    if not importlib.util.find_spec("pygraphviz"):
        err = "You need to have pygraphviz installed to draw the Graph"
        raise ModuleNotFoundError(err)

    lineage = get_column_lineage(df, column)
    g = nx.DiGraph()

    g.add_nodes_from(lineage.nodes)
    g.add_edges_from(lineage.edges)

    pos = graphviz_layout(g, prog="twopi")
    pos_attrs = {}
    for node, coords in pos.items():
        pos_attrs[node] = (coords[0], coords[1] + 10)
    nx.draw(g, pos=pos)
    nx.draw_networkx_labels(g, labels=lineage.node_attrs, pos=pos_attrs, clip_on=False)

    return plt.gcf()
