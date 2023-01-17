import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct



def read_csv(path):
    spark = SparkSession.builder.appName('spark_app').getOrCreate()
    logging.info(f'Try to read from {path}')
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )
    logging.info(f'Successfully read from {path}')
    return df


def write_csv(df, path):
    (df.coalesce(1).write
     .csv(path, header=True, mode='overwrite')
     )
    logging.info(f'Successfully writen result dataset to {path}')


def ren_columns(df, columns: dict):
    """Function to rename df columns with mapping"""
    try:
        return df.select(*[col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    except:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")


if __name__ == "__main__":
    clients_path = "data/clients.csv"
    financial_path = "data/financial.csv"

    finance_df = read_csv(financial_path)
    mapping = {'cc_t':'credit_card_type',
               'cc_n':'credit_card_number',
               'cc_mc':'credit_card_main_currency',
               'a':'active',
               'ac_t':'account_type'}
    finance_df = ren_columns(finance_df, mapping)
    finance_df.show()

    write_csv(finance_df, 'data/result.csv')

