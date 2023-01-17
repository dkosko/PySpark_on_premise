from filter_df import filter_clients_by_countries, select_columns
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max

spark = SparkSession.builder.appName('Spark_tests').getOrCreate()

def test_filter_clients_by_countries():
    source_data = [
        (1, 'France', '@1'),
        (2, 'Spain', '@2'),
        (3, 'Poland', '@3'),
        (4, 'Germany', '@4'),
        (5, 'Austria', '@5')
    ]
    source_df = (spark.createDataFrame(source_data, ["id", "country", "email"]))

    countries = ['France', 'Poland']
    actual_df = filter_clients_by_countries(source_df, countries)

    expected_data = [
        (1, 'France', '@1'),
        (3, 'Poland', '@3')
    ]
    expected_df = (spark.createDataFrame(expected_data, ["id", "country", "email"]))

    assert_df_equality(actual_df, expected_df)


def test_select_columns():
    source_data = [
        (1, 'Jack', 'Jonson', 'jack.jonson@gmail.com', 'M'),
        (2, 'Peter', 'Thomson', 'peter.thomson@gmail.com', 'M')
    ]
    source_df = (spark.createDataFrame(source_data,
                                       ["id", "first_name", "last_name", "email", "gender"]))
    columns = ["id", "email"]
    actual_df = select_columns(source_df, columns)

    expected_data = [
        (1, 'jack.jonson@gmail.com'),
        (2, 'peter.thomson@gmail.com')
    ]
    expected_df = (spark.createDataFrame(expected_data, columns))

    assert_df_equality(actual_df, expected_df)



test_filter_clients_by_countries()
test_select_columns()