from pyspark.sql import types as T


def print_schema_as_code(dtype: T.DataType) -> str:
    """Represent DataType (including StructType) as valid Python code.

    Parameters
    ----------
    dtype: pyspark.sql.types.DataType
        The input DataType or Schema object

    Returns
    -------
    str
        A valid python code which generate the same schema.
    """
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
        res.append(print_schema_as_code(dtype.elementType))
        res.append(")")

    elif isinstance(dtype, T.MapType):
        res.append("MapType(")
        res.append(f"\n\t{print_schema_as_code(dtype.keyType)},")
        for line in print_schema_as_code(dtype.valueType).split("\n"):
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
        for line in print_schema_as_code(column.dataType).split("\n"):
            res.append("\n\t")
            res.append(line)
        res.append(",")
        res.append(f"\n\t{column.nullable},")
        res.append("\n)")

    else:
        res.append(
            f'StructField("{column.name}", {print_schema_as_code(column.dataType)}, {column.nullable})'
        )

    return "".join(res)
