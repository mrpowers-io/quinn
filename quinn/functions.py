import re
import pyspark.sql.functions as F

from pyspark.sql.types import *


def single_space(col):
    return F.trim(F.regexp_replace(col, " +", " "))


def remove_all_whitespace(col):
    return F.regexp_replace(col, "\\s+", "")


def anti_trim(col):
    return F.regexp_replace(col, "\\b\\s+\\b", "")


def remove_non_word_characters(col):
    return F.regexp_replace(col, "[^\\w\\s]+", "")


def exists(f):
    def temp_udf(l):
        return any(map(f, l))

    return F.udf(temp_udf, BooleanType())


def forall(f):
    def temp_udf(l):
        return all(map(f, l))

    return F.udf(temp_udf, BooleanType())


def multi_equals(value):
    def temp_udf(*cols):
        return all(map(lambda col: col == value, cols))

    return F.udf(temp_udf, BooleanType())


def week_start_date(col, week_start_day="Sun"):
    _raise_if_invalid_day(week_start_day)
    # the "standard week" in Spark is from Sunday to Saturday
    mapping = {
        "Sun": "Sat",
        "Mon": "Sun",
        "Tue": "Mon",
        "Wed": "Tue",
        "Thu": "Wed",
        "Fri": "Thu",
        "Sat": "Fri",
    }
    end = week_end_date(col, mapping[week_start_day])
    return F.date_add(end, -6)


def week_end_date(col, week_end_day="Sat"):
    _raise_if_invalid_day(week_end_day)
    # these are the default Spark mappings.  Spark considers Sunday the first day of the week.
    day_of_week_mapping = {
        "Sun": 1,
        "Mon": 2,
        "Tue": 3,
        "Wed": 4,
        "Thu": 5,
        "Fri": 6,
        "Sat": 7,
    }
    return F.when(
        F.dayofweek(col).eqNullSafe(F.lit(day_of_week_mapping[week_end_day])), col
    ).otherwise(F.next_day(col, week_end_day))


def _raise_if_invalid_day(day):
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    if day not in valid_days:
        message = "The day you entered '{0}' is not valid.  Here are the valid days: [{1}]".format(
            day, ",".join(valid_days)
        )
        raise ValueError(message)


def approx_equal(col1, col2, threshhold):
    return F.abs(col1 - col2) < threshhold


def array_choice(col):
    index = (F.rand() * F.size(col)).cast("int")
    return col[index]


@F.udf(returnType=ArrayType(StringType()))
def regexp_extract_all(s, regexp):
    return None if s == None else re.findall(regexp, s)


from pyspark.sql.functions import *
from pyspark.sql.types import *


def flatten_test(df, sep=":"):
    """Returns a flattened dataframe.
    .. versionadded:: x.X.X

    Parameters
    ----------
    sep : str
        Delimiter for flatted columns. Default `:`

    Notes
    -----
    Don`t use `.` as `sep`
    It won't work on nested data frames with more than one level.
    And you will have to use `columns.name`.

    Flattening Map Types will have to find every key in the column.
    This can be slow.

    Examples
    --------

    data_mixed = [
        {
            "state": "Florida",
            "shortname": "FL",
            "info": {"governor": "Rick Scott"},
            "counties": [
                {"name": "Dade", "population": 12345},
                {"name": "Broward", "population": 40000},
                {"name": "Palm Beach", "population": 60000},
            ],
        },
        {
            "state": "Ohio",
            "shortname": "OH",
            "info": {"governor": "John Kasich"},
            "counties": [
                {"name": "Summit", "population": 1234},
                {"name": "Cuyahoga", "population": 1337},
            ],
        },
    ]

    data_mixed = spark.createDataFrame(data=data_mixed)

    data_mixed.printSchema()

    root
    |-- counties: array (nullable = true)
    |    |-- element: map (containsNull = true)
    |    |    |-- key: string
    |    |    |-- value: string (valueContainsNull = true)
    |-- info: map (nullable = true)
    |    |-- key: string
    |    |-- value: string (valueContainsNull = true)
    |-- shortname: string (nullable = true)
    |-- state: string (nullable = true)


    data_mixed_flat = flatten_test(df, sep=":")
    data_mixed_flat.printSchema()
    root
    |-- shortname: string (nullable = true)
    |-- state: string (nullable = true)
    |-- counties:name: string (nullable = true)
    |-- counties:population: string (nullable = true)
    |-- info:governor: string (nullable = true)




    data = [
        {
            "id": 1,
            "name": "Cole Volk",
            "fitness": {"height": 130, "weight": 60},
        },
        {"name": "Mark Reg", "fitness": {"height": 130, "weight": 60}},
        {
            "id": 2,
            "name": "Faye Raker",
            "fitness": {"height": 130, "weight": 60},
        },
    ]


    df = spark.createDataFrame(data=data)

    df.printSchema()

    root
    |-- fitness: map (nullable = true)
    |    |-- key: string
    |    |-- value: long (valueContainsNull = true)
    |-- id: long (nullable = true)
    |-- name: string (nullable = true)

    df_flat = flatten_test(df, sep=":")

    df_flat.printSchema()

    root
    |-- id: long (nullable = true)
    |-- name: string (nullable = true)
    |-- fitness:height: long (nullable = true)
    |-- fitness:weight: long (nullable = true)

    data_struct = [
            (("James",None,"Smith"),"OH","M"),
            (("Anna","Rose",""),"NY","F"),
            (("Julia","","Williams"),"OH","F"),
            (("Maria","Anne","Jones"),"NY","M"),
            (("Jen","Mary","Brown"),"NY","M"),
            (("Mike","Mary","Williams"),"OH","M")
            ]


    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
            ])),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
        ])

    df_struct = spark.createDataFrame(data = data_struct, schema = schema)

    df_struct.printSchema()

    root
    |-- name: struct (nullable = true)
    |    |-- firstname: string (nullable = true)
    |    |-- middlename: string (nullable = true)
    |    |-- lastname: string (nullable = true)
    |-- state: string (nullable = true)
    |-- gender: string (nullable = true)

    df_struct_flat = flatten_test(df_struct, sep=":")

    df_struct_flat.printSchema()

    root
    |-- state: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- name:firstname: string (nullable = true)
    |-- name:middlename: string (nullable = true)
    |-- name:lastname: string (nullable = true)
    """
    # compute Complex Fields (Arrays, Structs and Maptypes) in Schema
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
            or type(field.dataType) == StructType
            or type(field.dataType) == MapType
        ]
    )

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        # print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col(col_name + "." + k).alias(col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # if MapType then convert all sub element to columns.
        # i.e. flatten
        elif type(complex_fields[col_name]) == MapType:
            keys_df = df.select(explode_outer(map_keys(col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(
                map(
                    lambda f: col(col_name).getItem(f).alias(str(col_name + sep + f)),
                    keys,
                )
            )
            drop_column_list = [col_name]
            df = df.select(
                [
                    col_name
                    for col_name in df.columns
                    if col_name not in drop_column_list
                ]
                + key_cols
            )

        # recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == ArrayType
                or type(field.dataType) == StructType
                or type(field.dataType) == MapType
            ]
        )

    return df