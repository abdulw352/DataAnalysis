import requests
import numpy as np 
import pandas as pd 
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "./Largest_banks.csv"

# Code for ETL operations on Exchange Rate

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    time_stamp = now.strftime(time_format)
    with open("./etl_bank_log.txt", 'a') as f:
        f.write(time_stamp + ',' + message + '\n')


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    webpage = requests.get(url).text
    data = BeautifulSoup(webpage, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[1]:
                data_dict = {
                    "Name" : col[0].a.contents[0],
                    "MC_USD_Billion" : col[1].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index = True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    fx = pd.read_csv("exchange_rate.csv")
    data_dict = fx.set_index('Currency').to_dict()['Rate']
    USD_list = df.MC_USD_Billion.tolist()
    currency_list = [float("".join(x.split(','))) for x in USD_list]
    currency_list = [np.round(x, 2) for x in currency_list]
    df['MC_USD_Billion'] = [np.round(x, 2) for x in currency_list]
    df['MC_GBP_Billion'] = [np.round(x * data_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * data_dict['EUR'],2) for x in df.MC_USD_Billion]
    df['MC_INR_Billion'] = [np.round(x * data_dict['INR'],2) for x in df.MC_USD_Billion]
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index= False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    output = pd.read_sql(query_statement, sql_connection)
    print(output)


# Initiating ETL
log_progress("Initiating ETL process.")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process.")

df = transform(df)

log_progress("Data saved to CSV")

load_to_csv(df, csv_path)

log_progress("SQL Connection initiated")

sql_connection = sqlite3.connect(db_name)

load_to_db(df, sql_connection , table_name)

log_progress("Data loaded Database as table, Running the query")

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement ,sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete")

sql_connection.close()
