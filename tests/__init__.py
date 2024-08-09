import inspect
import os
import pytest
from .spark import spark
from functools import wraps


def check_spark_connect_compatibility(func):
    """
    Decorator to check Spark-Connect compatibility for a given function.

    This decorator will check the Spark version and the environment variable
    `SPARK_CONNECT_MODE_ENABLED`. If the Spark version is less than 3.5.2 and
    `SPARK_CONNECT_MODE_ENABLED` is set, it will raise an exception when the
    decorated function is called. Otherwise, it will execute the function normally.

    :param func: The function to be decorated.
    :type func: function
    :return: The wrapped function with Spark-Connect compatibility check.
    :rtype: function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        spark_version = spark.version
        if spark_version < "3.5.2" and os.getenv("SPARK_CONNECT_MODE_ENABLED"):
            # Except an exception to be raised calling the decorated function. In this context, the test case.
            with pytest.raises(Exception) as excinfo:
                func(*args, **kwargs)

            # Assert that the exception message matches the expected format
            assert str(excinfo.value) == f"This function is not supported on Spark-Connect < 3.5.2"

        else:
            # If the conditions are not met, call the function normally.
            return func(*args, **kwargs)

    return wrapper
