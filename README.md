# Quinn

![CI](https://github.com/MrPowers/quinn/workflows/CI/badge.svg?branch=master)

Pyspark helper methods to maximize developer productivity.

Quinn validates DataFrames, extends core classes, defines DataFrame transformations, and provides SQL functions.

![quinn](https://github.com/MrPowers/quinn/blob/master/quinn.png)

## Setup

Quinn is [uploaded to PyPi](https://pypi.org/project/quinn/) and can be installed with this command:

```
pip install quinn
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

Returns `True` if `age` is between `lower_age` and `upper_age`.  If `lower_age` is populated and `upper_age` is `null`, it will return `True` if `age` is greater than or equal to `lower_age`.  If `lower_age` is `null` and `upper_age` is populate, it will return `True` if `age` is lower than or equal to `upper_age`.

### SparkSession Extensions

**create_df()**

```python
spark.create_df(
    [("jose", "a"), ("li", "b"), ("sam", "c")],
    [("name", StringType(), True), ("blah", StringType(), True)]
)
```

Creates DataFrame with a syntax that's less verbose than the built-in `createDataFrame` method.

### DataFrame Extensions

**transform()**

```python
source_df\
    .transform(lambda df: with_greeting(df))\
    .transform(lambda df: with_something(df, "crazy"))
```

Allows for multiple DataFrame transformations to be run and executed.

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

**exists()**

```python
source_df.withColumn(
    "any_num_greater_than_5",
    quinn.exists(lambda n: n > 5)(col("nums"))
)
```

`nums` contains lists of numbers and `exists()` returns `True` if any of the numbers in the list are greater than 5.  It's similar to the Python `any` function.

**forall()**

```python
source_df.withColumn(
    "all_nums_greater_than_3",
    quinn.forall(lambda n: n > 3)(col("nums"))
)
```

`nums` contains lists of numbers and `forall()` returns `True` if all of the numbers in the list are greater than 3.  It's similar to the Python `all` function.

**multi_equals()**

```python
source_df.withColumn(
    "are_s1_and_s2_cat",
    quinn.multi_equals("cat")(col("s1"), col("s2"))
)
```

`multi_equals` returns true if `s1` and `s2` are both equal to `"cat"`.

### Transformations

**snake_case_col_names()**

```python
quinn.snake_case_col_names(source_df)
```

Converts all the column names in a DataFrame to snake_case.  It's annoying to write SQL queries when columns aren't snake cased.

**sort_columns()**

```python
quinn.sort_columns(source_df, "asc")
```

Sorts the DataFrame columns in alphabetical order.  Wide DataFrames are easier to navigate when they're sorted alphabetically.

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

Converts two columns of a DataFrame into a dictionary.  In this example, `name` is the key and `age` is the value.

**to_list_of_dictionaries()**

```python
quinn.to_list_of_dictionaries(source_df)
```

Converts an entire DataFrame into a list of dictionaries.

## Contributing

We are actively looking for feature requests, pull requests, and bug fixes.

Any developer that demonstrates excellence will be invited to be a maintainer of the project.
