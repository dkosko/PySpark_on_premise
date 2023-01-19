from filter_df import *
from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession


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


def test_filter_financial_by_active():
    source_data = [
        (1, 'jcb', 1234123412341234, 'USD', True, 'L'),
        (2, 'jcb', 1234123412341234, 'EUR', False, 'XL'),
        (3, 'jcb', 1234123412341234, 'EUR', True, 'XXL')
    ]
    columns = ["id", "credit_card_type", "credit_card_number", "currency", "active", "account_type"]
    source_df = (spark.createDataFrame(source_data, columns))
    actual_df = filter_financial_by_active(source_df)

    expected_data = [
        (1, 'jcb', 1234123412341234, 'USD', True, 'L'),
        (3, 'jcb', 1234123412341234, 'EUR', True, 'XXL')
    ]
    expected_df = (spark.createDataFrame(expected_data, columns))

    assert_df_equality(actual_df, expected_df)


def test_join_df():
    source_data_1 = [
        (1, 'Jack', 'Jonson'),
        (2, 'Peter', 'Thomson')
    ]
    columns_1 = ["id", "first_name", "last_name"]
    source_data_2 = [
        (1, True, 'L'),
        (2, False, 'XL'),
        (3, True, 'XXL')
    ]
    columns_2 = ["id", "active", "account_type"]

    source_df_1 = (spark.createDataFrame(source_data_1, columns_1))
    source_df_2 = (spark.createDataFrame(source_data_2, columns_2))

    actual_df = join_df(source_df_1, source_df_2, "id")

    expected_data = [
        (1, 'Jack', 'Jonson', True, 'L'),
        (2, 'Peter', 'Thomson', False, 'XL')
    ]
    columns_expected = columns_1 + columns_2[1:]
    expected_df = (spark.createDataFrame(expected_data, columns_expected))

    assert_df_equality(actual_df, expected_df)

def test_drop_column():
    source_data = [
        (1, 'France', '@1'),
        (2, 'Spain', '@2'),
        (3, 'Poland', '@3'),
        (4, 'Germany', '@4'),
        (5, 'Austria', '@5')
    ]
    source_df = (spark.createDataFrame(source_data, ["id", "country", "email"]))

    actual_df = drop_column(source_df, "email")

    expected_data = [
        (1, 'France'),
        (2, 'Spain'),
        (3, 'Poland'),
        (4, 'Germany'),
        (5, 'Austria')
    ]
    expected_df = (spark.createDataFrame(expected_data, ["id", "country"]))

    assert_df_equality(actual_df, expected_df)
