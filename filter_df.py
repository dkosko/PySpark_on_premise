from pyspark.sql.functions import col


def filter_col(df, col_name, values: list):
    """
    Function to filter DataFrame

    Parameters
    ----------
    df: pyspark.DataFrame
        Source DataFrame
    col_name: str
        Name of the column to filter by
    values: list
        List of values that are accepted by filter

    Returns
    -------
    new_df: pyspark.DataFrame
        Filtered DataFrame
    """
    new_df = (df
              .filter(col(col_name).isin(values))
              )
    return new_df


def select_columns(df, columns: list):
    """
    Function to create new DF from source DF with selected columns

    Parameters
    ----------
    df: pyspark.DataFrame
        Source DataFrame
    columns: list[str]
        List of column names

    Returns
    -------
    new_df: pyspark.DataFrame
        New DataFrame with selected columns
    """
    new_df = (df
              .select(*columns))

    return new_df


def join_df(df_1, df_2, col_name: str):
    """
    Function to join 2 DataFrames by given common column

    Parameters
    ----------
    df_1: pyspark.DataFrame
        First Source DataFrame
    df_2: pyspark.DataFrame
        Second Source DataFrame
    col_name: str
        Name of column to join source DFs

    Returns
    -------
    new_df: pyspark.DataFrame
        New DataFrame created by joining 2 source DFs
    """
    new_df = df_1.join(df_2, col_name, 'inner')
    return new_df


def drop_column(df, col_name: str):
    """
    Function to drop column from DataFrame with given column_name

    Parameters
    ----------
    df: pyspark.DataFrame
        Source DataFrame
    col_name: str
        Name of column to drop from source DF

    Returns
    -------
    new_df: pyspark.DataFrame
        New DataFrame created by dropping given columns"""
    new_df = df.drop(col(col_name))
    return new_df


if __name__ == "__main__":
    pass
