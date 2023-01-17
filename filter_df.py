from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct


def filter_clients_by_countries(df, countries:list):
    """Filter clients data by countries from list
     and select  necessary columns"""
    df = (df
          .filter(col("country").isin(countries))
          .select('id', 'email'))
    return df

def filter_financial_by_active(df):
    """Filter financial data for active clients
    and select necessary columns"""
    df = (df
          .filter(df.active == True)
          .select('id', 'credit_card_type', 'account_type'))
    return df


def join_clients_fin(clients_df, finance_df, countries:list):
    """Function to merge clients personal data (email)
    with clients financial data (credit_card_type, account_type)"""
    clients_df = filter_clients_by_countries(clients_df, countries)
    finance_df = filter_financial_by_active(finance_df)

    result_df = (clients_df.join(finance_df, 'id', 'inner')
                 .drop(col('id'))
                 )

    return result_df
    


if __name__ == "__main__":
    pass