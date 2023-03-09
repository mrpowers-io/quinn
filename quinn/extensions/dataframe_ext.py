from typing import Callable

from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame


def transform(self: DataFrame, f: Callable[[DataFrame], DataFrame]) -> DataFrame:
    return f(self)


def _repr_dtype(dtype: T.DataType) -> str:
    res = []
    if isinstance(dtype, T.StructType):
        res.append("StructType(\n\tfields=[")
        for field in dtype.fields:
            for line in _repr_column(field).split("\n"):
                res.append("\n\t\t")
                res.append(line)
            res.append(",")
        res.append("\n\t]\n)")

    elif isinstance(dtype, T.ArrayType):
        res.append("ArrayType(")
        res.append(_repr_dtype(dtype.elementType))
        res.append(")")

    elif isinstance(dtype, T.MapType):
        res.append("MapType(")
        res.append(f"\n\t{_repr_dtype(dtype.keyType)},")
        for line in _repr_dtype(dtype.valueType).split("\n"):
            res.append("\n\t")
            res.append(line)
        res.append(",")
        res.append(f"\n\t{dtype.valueContainsNull},")
        res.append("\n)")

    elif isinstance(dtype, T.DecimalType):
        res.append(f"DecimalType({dtype.precision}, {dtype.scale})")

    else:
        res.append(f"{dtype}()")

    return "".join(res)


def _repr_column(column: T.StructField) -> str:
    res = []

    if (
        isinstance(column.dataType, T.StructType)
        or isinstance(column.dataType, T.ArrayType)
        or isinstance(column.dataType, T.MapType)
    ):
        res.append(f'StructField(\n\t"{column.name}",')
        for line in _repr_dtype(column.dataType).split("\n"):
            res.append("\n\t")
            res.append(line)
        res.append(",")
        res.append(f"\n\t{column.nullable},")
        res.append("\n)")

    else:
        res.append(
            f'StructField("{column.name}", {_repr_dtype(column.dataType)}, {column.nullable})'
        )

    return "".join(res)


DataFrame.transform = getattr(DataFrame, "transform", transform)
DataFrame.printSchemaAsCode = getattr(
    DataFrame, "printSchemaAsCode", _repr_dtype
)
