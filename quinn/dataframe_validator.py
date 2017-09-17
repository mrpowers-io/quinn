class DataFrameMissingColumnError(ValueError):
    '''raise this when there's a DataFrame column error'''

class DataFrameValidator:

    def validate_presence_of_columns(self, df, required_col_names):
        all_col_names = df.columns
        missing_col_names = [x for x in required_col_names if x not in all_col_names]
        error_message = f"The {missing_col_names} columns are not included in the DataFrame with the following columns {all_col_names}"
        if missing_col_names:
            raise DataFrameMissingColumnError(error_message)
