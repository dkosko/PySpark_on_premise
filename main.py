from handle_csv import *
from filter_df import *

def create_result_dataset(client_path, finance_path, countries, mapping_finance):
    client_df = read_csv(client_path)
    finance_df = ren_columns(read_csv(finance_path), mapping_finance)

    result_df = join_clients_fin(client_df, finance_df, countries)
    result_df.show()
    return result_df


if __name__ == "__main__":
    clients_path = "data/clients.csv"
    financial_path = "data/financial.csv"
    countries = ['France', 'Poland']
    mapping = {'cc_t': 'credit_card_type',
               'cc_n': 'credit_card_number',
               'cc_mc': 'credit_card_main_currency',
               'a': 'active',
               'ac_t': 'account_type'}

    df = create_result_dataset(clients_path, financial_path, countries, mapping)
    write_csv(df, 'client_data/')