## Dependendies
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import logging
import pyodbc
import urllib

## Setting up logging

logging.basicConfig(filename='data_processing.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to log errors
def log_error(error_message):
    logging.error(f"Error: {error_message}")

# Step 1: Load the CSV
def load_csv(file_path):
    try:
        df = pd.read_csv(file_path, encoding='unicode_escape')
        logging.info("CSV file loaded successfully.")
        return df
    except Exception as e:
        log_error(f"Failed to load CSV: {e}")
        raise e
    
##Step 3: Fetch data from the SQL table 

def fetch_data_from_sql(conn, table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df_sql = pd.read_sql(query, conn)
        logging.info(f"Data fetched from {table_name} successfully.")
        return df_sql
    except Exception as e:
        log_error(f"Failed to fetch data from SQL: {e}")
        raise e

## Step 4: Data cleaning   

def clean_data(df_sql):
    try:
        # Dropping the 'Unnamed' and 'Status' columns
        df_sql.drop(columns=['Unnamed', 'Status'], inplace=True, errors='ignore')

        # Renaming 'nage' to 'age_group'
        df_sql.rename(columns={'nage': 'age_group'}, inplace=True)

        # Dropping duplicates
        df_sql.drop_duplicates(inplace=True)

        # Resetting the index
        df_sql.reset_index(drop=True, inplace=True)

        logging.info("Data cleaning and transformation completed successfully.")
        return df_sql
    except Exception as e:
        log_error(f"Data cleaning failed: {e}")
        raise e
    
## Step 5: Save the updated data back to the SQL Server

def save_data_to_sql(df, engine, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data saved back to {table_name} successfully.")
    except Exception as e:
        log_error(f"Failed to save data to SQL Server: {e}")
        raise e

## step:6 Main function to execute the workflow   
def main():
    csv_file_path = r"C:\Users\LENOVO\OneDrive\Desktop\deeppython\Python_Diwali_Sales_Analysis-main\data processor\Diwali.csv"
    server = 'LAPTOP-U3795DN8'
    database = 'mydatabase'
    table_name = 'dbo.Diwali'

    # Load CSV data
    df = load_csv(csv_file_path)

    # Connect to SQL Server
    conn, engine = create_sql_connection(server, database,)

    # Fetch data from SQL
    df_sql = fetch_data_from_sql(conn, table_name)

    # Clean data
    cleaned_df = clean_data(df_sql)

    # Save the cleaned data back to SQL
    save_data_to_sql(cleaned_df, engine, table_name)

    # Close the connection
    conn.close()
    logging.info("Database connection closed.")

def create_sql_connection(server, database):
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}")
    conn = pyodbc.connect(conn_str)
    return conn, engine


if __name__ == "__main__":
    main()