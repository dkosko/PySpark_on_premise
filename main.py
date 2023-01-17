import argparse
from handle_csv import *
from filter_df import *


def create_result_dataset(client_path, finance_path, countries, mapping_finance):
    client_df = read_csv(client_path)
    finance_df = read_csv(finance_path)
    renamed_finance_df = ren_columns(finance_df, mapping_finance)

    active_finance = filter_financial_by_active(renamed_finance_df)
    client_from_countries  = filter_clients_by_countries(client_df, countries)

    merged_df = join_df(client_from_countries, active_finance, "id")
    result_df = select_columns(merged_df, ["email", "credit_card_type", "account_type"])
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
    logging.info(f'Got args parsed : {clients_path}, {financial_path}, {countries}')

    df = create_result_dataset(clients_path, financial_path, countries, mapping)
    logging.info(f'Created result dataset')
    write_csv(df, 'client_data/')

if __name__ == "__main__":
    logging.basicConfig(filename='actions.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info('Start application')
    start_application()