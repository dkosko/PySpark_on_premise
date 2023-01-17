from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct


def filter_clients_by_countries(df, countries:list):
    """Filter clients data by countries from list"""
    new_df = (df
          .filter(col("country").isin(countries))
          )
    return new_df

def filter_financial_by_active(df):
    """Filter financial data for active clients"""
    new_df = (df
          .filter(col("active") == True)
          )
    return new_df

def select_columns(df, columns:list):
    """Function to select given columns from DataFrame"""
    new_df = (df
              .select(*columns))

    return new_df

def join_df(df_1, df_2, col_name):
    """Function to join 2 DataFrames by given common column"""
    new_df = df_1.join(df_2, col_name, 'inner')
    return new_df

def drop_column(df, col_name):
    """Function to drop column from DataFrame with given column_name"""
    new_df = df.drop(col(col_name))
    return new_df

if __name__ == "__main__":
    pass