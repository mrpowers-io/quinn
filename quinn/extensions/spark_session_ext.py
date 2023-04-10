from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType


def create_df(self, rows_data, col_specs):
    """Creates a new DataFrame from the given data and column specs. The returned
    DataFrame is created using the StructType and StructField classes provided by
    PySpark.

    :param rows_data: the data used to create the DataFrame
    :type rows_data: array-like
    :param col_specs: list of tuples containing the name and type of the field 
    :type col_specs: list of tuples
    :return: a new DataFrame
    :rtype: DataFrame
    """
    struct_fields = list(map(lambda x: StructField(*x), col_specs))
    return self.createDataFrame(data=rows_data, schema=StructType(struct_fields))


SparkSession.create_df = create_df
