

import pandas as pd
import yaml

from sqlalchemy import create_engine
from datetime import date

# Load YAML file
with open("C:/Users/roger.lloret/Documents/creds/creds_dmbi.yml", "r") as file:
    creds = yaml.safe_load(file)  


def read_from_database(creds, query):
    """
    Returns as dataframe the result of a query to a MySQL database.

    Args:
        creds (dict): The credentials to access the database.
        query (string): The query.

    Returns:
        pandas dataframe: the output table of the query.
    """
    _db_user = creds['username']
    _db_password = creds['password']
    _db_host = creds['host']
    _db_name = creds['database']
    engine = create_engine(f"mysql://{_db_user}:{_db_password}@{_db_host}:3306/{_db_name}")
    df = pd.read_sql(query, engine)
    return df

def write_to_database(creds, df, table_name, if_exists='append'):
    """
    Returns as dataframe the result of a query to a MySQL database.

    Args:
        creds (dict): The credentials to access the database.
        query (string): The query.

    Returns:
        pandas dataframe: the output table of the query.
    """
    _db_user = creds['username']
    _db_password = creds['password']
    _db_host = creds['host']
    _db_name = creds['database']
    engine = create_engine(f"mysql://{_db_user}:{_db_password}@{_db_host}:3306/{_db_name}")
    with engine.connect() as connection:
        df.to_sql(table_name, con=connection, if_exists=if_exists, index=False) 

def classify_value(value):
    """Classifies a numeric value into three categories.
    Args:
        value (numeric): The numeric value to classify.
    Returns:
        A string representing the category: "Less than 50", "Between 50 and 100", or "Greater than 100".
        Returns "Invalid Input" if the input is not a number.
    """
    try:
        if value < 50:
            return "Less than 50"
        elif 50 <= value <= 100:
            return "Between 50 and 100"
        else:  
            return "Greater than 100"
    except TypeError:
        return "Invalid Input" 


TABLE_NAME = 'gen_kpi_ft'
with open("./sql/invoices_main.sql", "r", encoding="utf-8") as file:
    invoices_main_sql = file.read()

with open("./sql/contracts_main.sql", "r", encoding="utf-8") as file:
    contracts_main_sql = file.read()

# Input data
invoices_df = read_from_database(creds['data_warehouse'], invoices_main_sql)
contracts_df = read_from_database(creds['data_warehouse'], contracts_main_sql)

contracts_df = contracts_df.drop_duplicates()
 
#Calculate custom fields
invoices_df['category'] = invoices_df['total_import_euros'].apply(classify_value)

# Merges different sources
invoices_df['contract_id'] = pd.to_numeric(invoices_df['contract_id'])
contracts_df['contract_id'] = pd.to_numeric(contracts_df['contract_id'])
merged_df = invoices_df.merge(contracts_df, on='contract_id', how='left') 

# Create indicators
kpi_category_df = merged_df.groupby(['category', 'client_type_description'])['total_import_euros'].sum().reset_index()
kpi_category_df['kpi_name'] = 'Total amount in euros of the customers with invoices ' + kpi_category_df['category'] + ' euros and ' + kpi_category_df['client_type_description']
kpi_category_df = kpi_category_df[['kpi_name', 'total_import_euros']]
kpi_category_df.columns = ['kpi_name', 'kpi_value']

kpi_doctype_df = merged_df.groupby(['document_type_description'])['total_import_euros'].count().reset_index()
kpi_doctype_df['kpi_name'] = 'Number of invoices of invoice type ' + kpi_doctype_df['document_type_description'] 
kpi_doctype_df = kpi_doctype_df[['kpi_name', 'total_import_euros']]
kpi_doctype_df.columns = ['kpi_name', 'kpi_value']

# Aggregate indicators and write output
main_df = pd.concat([kpi_category_df, kpi_doctype_df])
main_df['kpi_date'] = date.today()
main_df = main_df[['kpi_date', 'kpi_name', 'kpi_value']]
write_to_database(creds['data_warehouse'], main_df, TABLE_NAME)