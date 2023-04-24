from pyspark.sql.types import *

from quinn.schema_helpers import print_schema_as_code

from chispa.schema_comparer import assert_basic_schema_equality

from tests.conftest import auto_inject_fixtures


@auto_inject_fixtures("spark")
def test_print_schema_as_code(spark):
    fields = []
    fields.append(StructField("simple_int", IntegerType()))
    fields.append(StructField("decimal_with_nums", DecimalType(19, 8)))
    fields.append(StructField("array", ArrayType(FloatType())))
    fields.append(StructField("map", MapType(StringType(), ArrayType(DoubleType()))))
    fields.append(
        StructField(
            "struct",
            StructType(
                [
                    StructField("first", StringType()),
                    StructField("second", TimestampType()),
                ]
            ),
        )
    )

    schema = StructType(fields=fields)


    assert_basic_schema_equality(schema, eval(print_schema_as_code(schema)))
