# Quinn

Pyspark helper methods to maximize developer productivity.

![quinn](https://github.com/MrPowers/quinn/blob/master/quinn.png)

## Setup

Quinn is [uploaded to PyPi](https://pypi.org/project/quinn/) and can be installed with this command:

```
pip install quinn
```

## API Overview

Quinn validates DataFrames, extends core classes, defines DataFrame transformations, and provides SQL functions.

### DataFrame Validations

```python
from quinn.dataframe_validator import *
```

* `DataFrameValidator().validate_presence_of_columns(source_df, ["name", "age", "fun"])`: Raises an exception unless `source_df` contains the `name`, `age`, and `fun` column.

* `DataFrameValidator().validate_schema(source_df, required_schema)`: Raises an exception unless `source_df` contains all the `StructFields` defined in the `required_schema`.

* `DataFrameValidator().validate_absence_of_columns(source_df, ["age", "cool"])`: Raises an exception if `source_df` contains `age` or `cool` columns.

### Column Extensions

```python
from quinn.column_ext import *
```

**nullBetween()**

```python
source_df.withColumn("is_between", F.col("age").nullBetween(F.col("lower_age"), F.col("upper_age")))
```

Returns `True` if `age` is between `lower_age` and `upper_age`.  If `lower_age` is populated and `upper_age` is `null`, it will return `True` if `age` is greater than or equal to `lower_age`.  If `lower_age` is `null` and `upper_age` is populate, it will return `True` if `age` is lower than or equal to `upper_age`.

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

### SparkSession Extensions

```python
from quinn.spark_session_ext import *
```

**createDF()**

```python
spark.createDF(
    [("jose", "a"), ("li", "b"), ("sam", "c")],
    [("name", StringType(), True), ("blah", StringType(), True)]
)
```

Creates DataFrame with a syntax that's less verbose than the built-in `createDataFrame` method.

### DataFrame Extensions

```python
from quinn.dataframe_ext import *
```

**transform()**

```python
source_df\
    .transform(lambda df: with_greeting(df))\
    .transform(lambda df: with_something(df, "crazy"))
```

Allows for multiple DataFrame transformations to be run and executed.

### Functions

```python
import quinn.functions as QF
```

**exists()**

```python
source_df.withColumn(
    "any_num_greater_than_5",
    QF.exists(lambda n: n > 5)(col("nums"))
)
```

`nums` contains lists of numbers and `exists()` returns `True` if any of the numbers in the list are greater than 5.  It's similar to the Python `any` function.

**forall()**

```python
source_df.withColumn(
    "all_nums_greater_than_3",
    QF.forall(lambda n: n > 3)(col("nums"))
)
```

`nums` contains lists of numbers and `forall()` returns `True` if all of the numbers in the list are greater than 3.  It's similar to the Python `all` function.

**multi_equals()**

```python
source_df.withColumn(
    "are_s1_and_s2_cat",
    QF.multi_equals("cat")(col("s1"), col("s2"))
)
```

`multi_equals` returns true if `s1` and `s2` are both equal to `"cat"`.

### Transformations

```python
import quinn.transformations as QT
```

**snake_case_col_names()**

```python
QT.snake_case_col_names(source_df)
```

Converts all the column names in a DataFrame to snake_case.  It's annoying to write SQL queries when columns aren't snake cased.

**sort_columns()**

```python
QT.sort_columns(source_df, "asc")
```

Sorts the DataFrame columns in alphabetical order.  Wide DataFrames are easier to navigate when they're sorted alphabetically.

### DataFrame Helpers

```python
from quinn.dataframe_helpers import DataFrameHelpers
```

**column_to_list()**

```python
DataFrameHelpers().column_to_list(source_df, "name")
```

Converts a column in a DataFrame to a list of values.

**two_columns_to_dictionary()**

```python
DataFrameHelpers().two_columns_to_dictionary(source_df, "name", "age")
```

Converts two columns of a DataFrame into a dictionary.  In this example, `name` is the key and `age` is the value.


**to_list_of_dictionaries()**

```python
DataFrameHelpers().to_list_of_dictionaries(source_df)
```

Converts an entire DataFrame into a list of dictionaries.

## Contributing

We are actively looking for contributors to request features, submit pull requests, or fix bugs.

Any developer that demonstrates pyspark excellence will be invited to be a maintainer of the project.
