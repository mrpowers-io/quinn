class ColumnMismatchError(Exception):
    """raise this when there's a DataFrame column error"""


def assert_column_equality(df, col_name1, col_name2):
    list1 = df.select(col_name1).collect()
    list2 = df.select(col_name2).collect()

    if list1 == list2:
        return True
    else:
        error_message = "The values of {col_name1} column are different from {col_name2}".format(
            col_name1 = col_name1,
            col_name2 = col_name2
        )
        print(list1)
        print(list2)
        raise ColumnMismatchError(error_message)
