## Quinn Helper Functions

```python
import quinn
```

### DataFrame Validations

**validate_presence_of_columns()**

```python
quinn.validate_presence_of_columns(source_df, ["name", "age", "fun"])
```

Raises an exception unless `source_df` contains the `name`, `age`, and `fun` column.

**validate_schema()**

```python
quinn.validate_schema(source_df, required_schema)
```

Raises an exception unless `source_df` contains all the `StructFields` defined in the `required_schema`.

**validate_absence_of_columns()**

```python
quinn.validate_absence_of_columns(source_df, ["age", "cool"])
```

Raises an exception if `source_df` contains `age` or `cool` columns.

### Functions

**single_space()**

```python
actual_df = source_df.withColumn(
    "words_single_spaced",
    quinn.single_space(col("words"))
)
```

Replaces all multispaces with single spaces (e.g. changes `"this has   some"` to `"this has some"`.

**remove_all_whitespace()**

```python
actual_df = source_df.withColumn(
    "words_without_whitespace",
    quinn.remove_all_whitespace(col("words"))
)
```

Removes all whitespace in a string (e.g. changes `"this has some"` to `"thishassome"`.

**anti_trim()**

```python
actual_df = source_df.withColumn(
    "words_anti_trimmed",
    quinn.anti_trim(col("words"))
)
```

Removes all inner whitespace, but doesn't delete leading or trailing whitespace (e.g. changes `" this has some "` to `" thishassome "`.

**remove_non_word_characters()**

```python
actual_df = source_df.withColumn(
    "words_without_nonword_chars",
    quinn.remove_non_word_characters(col("words"))
)
```

Removes all non-word characters from a string (e.g. changes `"si%$#@!#$!@#mpsons"` to `"simpsons"`.

**multi_equals()**

```python
source_df.withColumn(
    "are_s1_and_s2_cat",
    quinn.multi_equals("cat")(col("s1"), col("s2"))
)
```

`multi_equals` returns true if `s1` and `s2` are both equal to `"cat"`.

**approx_equal()**

This function takes 3 arguments which are 2 Pyspark DataFrames and one integer values as threshold, and returns the Boolean column which tells if the columns are equal in the threshold.

```
let the columns be
col1 = [1.2, 2.5, 3.1, 4.0, 5.5]
col2 = [1.3, 2.3, 3.0, 3.9, 5.6]
threshold = 0.2

result = approx_equal(col("col1"), col("col2"), threshold)
result.show()

+-----+
|value|
+-----+
| true|
|false|
| true|
| true|
| true|
+-----+
```

**array_choice()**

This function takes a Column as a parameter and returns a PySpark column that contains a random value from the input column parameter

```
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["values"])
result = df.select(array_choice(col("values")))

The output is :=
+--------------+
|array_choice()|
+--------------+
|             2|
+--------------+

```

**regexp_extract_all()**

The regexp_extract_all takes 2 parameters String `s` and `regexp` which is a regular expression. This function finds all the matches for the string which satisfies the regular expression.

```
print(regexp_extract_all("this is a example text message for testing application",r"\b\w*a\w*\b"))

The output is :=
['a', 'example', 'message', 'application']

```

Where `r"\b\w*a\w*\b"` pattern checks for words containing letter `a`

**week_start_date()**

It takes 2 parameters, column and week_start_day. It returns a Spark Dataframe column which contains the start date of the week. By default the week_start_day is set to "Sun".

For input `["2023-03-05", "2023-03-06", "2023-03-07", "2023-03-08"]` the Output is

```
result = df.select("date", week_start_date(col("date"), "Sun"))
result.show()
+----------+----------------+
|      date|week_start_date |
+----------+----------------+
|2023-03-05|      2023-03-05|
|2023-03-07|      2023-03-05|
|2023-03-08|      2023-03-05|
+----------+----------------+
```

**week_end_date()**

It also takes 2 Paramters as Column and week_end_day, and returns the dateframe column which contains the end date of the week. By default the week_end_day is set to "sat"

```
+---------+-------------+
      date|week_end_date|
+---------+-------------+
2023-03-05|   2023-03-05|
2023-03-07|   2023-03-12|
2023-03-08|   2023-03-12|
+---------+-------------+

```

**uuid5()**

This function generates UUIDv5 in string form from the passed column and optionally namespace and optional extra salt.
By default namespace is NAMESPACE_DNS UUID and no extra string used to reduce hash collisions.

```

df = spark.createDataFrame([("lorem",), ("ipsum",)], ["values"])
result = df.select(quinn.uuid5(F.col("values")).alias("uuid5"))
result.show(truncate=False)

The output is :=
+------------------------------------+
|uuid5                               |
+------------------------------------+
|35482fda-c10a-5076-8da2-dc7bf22d6be4|
|51b79c1d-d06c-5b30-a5c6-1fadcd3b2103|
+------------------------------------+

```

### Transformations

**snake_case_col_names()**

```python
quinn.snake_case_col_names(source_df)
```

Converts all the column names in a DataFrame to snake_case. It's annoying to write SQL queries when columns aren't snake cased.

**sort_columns()**

```python
quinn.sort_columns(df=source_df, sort_order="asc", sort_nested=True)
```

Sorts the DataFrame columns in alphabetical order, including nested columns if sort_nested is set to True. Wide DataFrames are easier to navigate when they're sorted alphabetically.

### DataFrame Helpers

**column_to_list()**

```python
quinn.column_to_list(source_df, "name")
```

Converts a column in a DataFrame to a list of values.

**two_columns_to_dictionary()**

```python
quinn.two_columns_to_dictionary(source_df, "name", "age")
```

Converts two columns of a DataFrame into a dictionary. In this example, `name` is the key and `age` is the value.

**to_list_of_dictionaries()**

```python
quinn.to_list_of_dictionaries(source_df)
```

Converts an entire DataFrame into a list of dictionaries.

**show_output_to_df()**

```python
quinn.show_output_to_df(output_str, spark)
```

Parses a spark DataFrame output string into a spark DataFrame. Useful for quickly pulling data from a log into a DataFrame. In this example, output_str is a string of the form:

```
+----+---+-----------+------+
|name|age|     stuff1|stuff2|
+----+---+-----------+------+
|jose|  1|nice person|  yoyo|
|  li|  2|nice person|  yoyo|
| liz|  3|nice person|  yoyo|
+----+---+-----------+------+
```

### Schema Helpers

**schema_from_csv()**

```python
quinn.schema_from_csv("schema.csv")
```

Converts a CSV file into a PySpark schema (aka `StructType`). The CSV must contain the column name and type.  The nullable and metadata columns are optional.

Here's an example CSV file:

```
name,type
person,string
address,string
phoneNumber,string
age,int
```

Here's how to convert that CSV file to a PySpark schema:

```python
schema = schema_from_csv(spark, "some_file.csv")

StructType([
    StructField("person", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phoneNumber", StringType(), True),
    StructField("age", IntegerType(), True),
])
```

Here's a more complex CSV file:

```
name,type,nullable,metadata
person,string,false,{"description":"The person's name"}
address,string
phoneNumber,string,TRUE,{"description":"The person's phone number"}
age,int,False
```

Here's how to read this CSV file into a PySpark schema:

```python
another_schema = schema_from_csv(spark, "some_file.csv")

StructType([
    StructField("person", StringType(), False, {"description": "The person's name"}),
    StructField("address", StringType(), True),
    StructField("phoneNumber", StringType(), True, {"description": "The person's phone number"}),
    StructField("age", IntegerType(), False),
])
```

**print_schema_as_code()**

```python   
fields = [
    StructField("simple_int", IntegerType()),
    StructField("decimal_with_nums", DecimalType(19, 8)),
    StructField("array", ArrayType(FloatType()))
]
schema = StructType(fields)
printable_schema: str = quinn.print_schema_as_code(schema)
```

Converts a Spark `DataType` to a string of Python code that can be evaluated as code using eval(). If the `DataType` is a `StructType`, this can be used to print an existing schema in a format that can be copy-pasted into a Python script, log to a file, etc. 

For example:
```python
print(printable_schema)
```

```
StructType(
	fields=[
		StructField("simple_int", IntegerType(), True),
		StructField("decimal_with_nums", DecimalType(19, 8), True),
		StructField(
			"array",
			ArrayType(FloatType()),
			True,
		),
	]
)
```

Once evaluated, the printable schema is a valid schema that can be used in dataframe creation, validation, etc.

```python
from chispa.schema_comparer import assert_basic_schema_equality

parsed_schema = eval(printable_schema)
assert_basic_schema_equality(parsed_schema, schema) # passes
```


`print_schema_as_code()` can also be used to print other `DataType` objects.

 `ArrayType`
```python
array_type = ArrayType(FloatType())
printable_type: str = quinn.print_schema_as_code(array_type)
print(printable_type)
 ```

 ```
ArrayType(FloatType())
 ```

`MapType`
```python
map_type = MapType(StringType(), FloatType())
printable_type: str = quinn.print_schema_as_code(map_type)
print(printable_type)
 ```

 ```
MapType(
        StringType(),
        FloatType(),
        True,
)
 ```

`IntegerType`, `StringType` etc.
```python
integer_type = IntegerType()
printable_type: str = quinn.print_schema_as_code(integer_type)
print(printable_type)
 ```

 ```
IntegerType()
 ```

## Pyspark Core Class Extensions

```
from quinn.extensions import *
```

### Column Extensions

**isFalsy()**

```python
source_df.withColumn("is_stuff_falsy", F.col("has_stuff").isFalsy())
```

Returns `True` if `has_stuff` is `None` or `False`.

**isTruthy()**

```python
source_df.withColumn("is_stuff_truthy", F.col("has_stuff").isTruthy())
```

Returns `True` unless `has_stuff` is `None` or `False`.

**isNullOrBlank()**

```python
source_df.withColumn("is_blah_null_or_blank", F.col("blah").isNullOrBlank())
```

Returns `True` if `blah` is `null` or blank (the empty string or a string that only contains whitespace).

**isNotIn()**

```python
source_df.withColumn("is_not_bobs_hobby", F.col("fun_thing").isNotIn(bobs_hobbies))
```

Returns `True` if `fun_thing` is not included in the `bobs_hobbies` list.

**nullBetween()**

```python
source_df.withColumn("is_between", F.col("age").nullBetween(F.col("lower_age"), F.col("upper_age")))
```

Returns `True` if `age` is between `lower_age` and `upper_age`. If `lower_age` is populated and `upper_age` is `null`, it will return `True` if `age` is greater than or equal to `lower_age`. If `lower_age` is `null` and `upper_age` is populate, it will return `True` if `age` is lower than or equal to `upper_age`.