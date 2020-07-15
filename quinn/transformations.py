from functools import reduce


def modify_column_names(fun):
    def _(df):
        return reduce(
            lambda memo_df, col_name: memo_df.withColumnRenamed(col_name, fun(col_name)),
            df.columns,
            df
        )
    return _


def snake_case_col_names(df):
    return modify_column_names(to_snake_case)(df)


def to_snake_case(s):
    return s.lower().replace(" ", "_")


def sort_columns(df, sort_order):
    sorted_col_names = None
    if sort_order == "asc":
        sorted_col_names = sorted(df.columns)
    elif sort_order == "desc":
        sorted_col_names = sorted(df.columns, reverse=True)
    else:
        raise ValueError("['asc', 'desc'] are the only valid sort orders and you entered a sort order of '{sort_order}'".format(
            sort_order=sort_order
        ))
    return df.select(*sorted_col_names)
