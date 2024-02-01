# Code for ETL operations on Country-GDP data

from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
import sqlite3


def log_progress(message): 
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open("code_log.txt", "a") as f: 
        f.write(f"{timestamp} : {message}\n")

def extract(url, table_attribs):
    log_progress("Task 1: Extracting data from the website")

    # Send an HTTP request to the URL and get the HTML content
    page = requests.get(url).text
    # Parse the HTML content using BeautifulSoup
    data = BeautifulSoup(page, 'html.parser')

    # Extract the table based on the provided attributes
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')

    # Assuming the desired table is the third one
    if len(tables) >= 3:
        rows = tables[0].find_all('tr')
        for row in rows[1:]:  # Skip the first row
            cols = row.find_all(['td', 'th'])
            # Extract data from bank name and market cap columns
            bank_name = cols[1].text.strip() if len(cols) >= 2 else None
            market_cap = cols[2].text.strip() if len(cols) == 3 else None
            # Remove the last character from the Market Cap column contents and typecast to float
            try:
                if market_cap and '—' not in market_cap:
                    market_cap = float(market_cap[:-1].replace('.', ''))
                data_dict = {"Bank Name": bank_name, "Market Cap": market_cap}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError as e:
                print(f"Skipping row due to conversion error: Bank Name={bank_name}, Market Cap={market_cap}, Error: {e}")
    log_progress("Extraction completed successfully")

    return df


def transform(df, exchange_rate_path):
    log_progress("Task 2: Transforming data")
    # Read the exchange rate CSV file
    exchange_rate_df = pd.read_csv('exchange_rate.csv')
    # Convert the contents to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    # Add columns to the DataFrame
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market Cap']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market Cap']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market Cap']]
    log_progress( "Transformation completed successfully")
    return df

def load_to_csv(df, filename='output.csv'):
    # Save the DataFrame to a CSV file in the same directory as the script
    df.to_csv(filename, index=False)

    # Log the progress
    log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : Data saved to CSV file"
    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_message + '\n')

def load_to_db(connection, table_name, df):
    # Save the DataFrame to an SQLite database table
    df.to_sql(table_name, connection, index=False, if_exists='replace')

    # Log the progress
    log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} : Data loaded to Database as a table, Executing queries"
    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_message + '\n')



def run_queries(query, connection):
    log_progress("Task 5: Running SQL queries")

    try:
        result = connection.execute(query).fetchall()
        log_progress(f"Query statement:\n{query}\nQuery output:\n{result}\n{'='*50}")
    except Exception as e:
        log_progress(f"Error: {e}")

# Example function call
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Bank Name", "Market Cap"]
extracted_data = extract(url, table_attribs)
#print(extracted_data.head())  # Print the first few rows
exchange_rate_path = 'exchange_rate.csv'
transformed_data = transform(extracted_data, exchange_rate_path)

# Print the contents of the returning data frame
print(transformed_data)

# Save the transformed data to a CSV file
print("Market capitalization of the 5th largest bank in billion EUR:", transformed_data['MC_EUR_Billion'][4])

load_to_csv(transformed_data)

# Initiating SQLite3 connection
conn = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
load_to_db(conn, table_name, transformed_data)



# Print the contents of the entire table
query1 = "SELECT * FROM Largest_banks"
run_queries(query1, conn)

# Print the average market capitalization of all the banks in Billion USD
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query2, conn)

# Print only the names of the top 5 banks
query3 = "SELECT [Bank Name] FROM Largest_banks LIMIT 5"
run_queries(query3, conn)