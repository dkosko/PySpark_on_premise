import argparse
from handle_csv import *
from filter_df import *

def create_result_dataset(client_path, finance_path, countries, mapping_finance):
    client_df = read_csv(client_path)
    finance_df = ren_columns(read_csv(finance_path), mapping_finance)

    result_df = join_clients_fin(client_df, finance_df, countries)
    result_df.show(result_df.count())
    return result_df

def get_args():
    """Function gets 3 arguments from comand line:
    clients_data_path
    financial_data_path
    list_of_countries"""

    parser = argparse.ArgumentParser()

    parser.add_argument('-clients', type=str, help='<Required> Set flag', required=True)
    parser.add_argument('-financial', type=str, help='<Required> Set flag', required=True)
    parser.add_argument('-countries', nargs='+', help='<Required> Set flag', required=True)

    args = parser.parse_args()
    clients_path = args.clients
    financial_path = args.financial
    countries = args.countries
    # clients_path = "data/clients.csv"
    # financial_path = "data/financial.csv"
    # countries = ['France', 'Poland']
    return clients_path, financial_path, countries


def start_application():
    mapping = {'cc_t': 'credit_card_type',
               'cc_n': 'credit_card_number',
               'cc_mc': 'credit_card_main_currency',
               'a': 'active',
               'ac_t': 'account_type'}

    clients_path, financial_path, countries = get_args()
    print("Startint application with args parsed:")
    print(clients_path, financial_path, countries)
    df = create_result_dataset(clients_path, financial_path, countries, mapping)
    write_csv(df, 'client_data/')

if __name__ == "__main__":
    start_application()