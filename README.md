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

* `source_df.withColumn("is_stuff_truthy", F.col("has_stuff").isTruthy())`: Returns `True` unless `has_stuff` is `None` or `False`.

* `source_df.withColumn("is_blah_null_or_blank", F.col("blah").isNullOrBlank())`: Returns `True` if `blah` is `null` or blank (the empty string or a string that only contains whitespace).

* `source_df.withColumn("is_not_bobs_hobby", F.col("fun_thing").isNotIn(bobs_hobbies))`: Returns `True` if `fun_thing` is not included in the `bobs_hobbies` list.

### SparkSession Extensions

### DataFrame Extensions

### Functions

### Transformations

### DataFrame Helpers

## Contributing

We are actively looking for contributors to request features, submit pull requests, or fix bugs.

Any developer that demonstrates pyspark excellence will be invited to be a maintainer of the project.
