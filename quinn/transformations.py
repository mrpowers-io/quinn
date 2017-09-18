from functools import reduce

def snake_case_col_names(df):
    return reduce(
        lambda memo_df, col_name: memo_df.withColumnRenamed(col_name, to_snake_case(col_name)),
        df.columns,
        df
    )

def to_snake_case(s):
    return s.lower().replace(" ", "_")

