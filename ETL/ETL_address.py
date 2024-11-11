import os
import pandas as pd
import numpy as np
import scipy.stats as stats
from colorama import Fore
from colorama import Style
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

def extract_data(directory_path, file_name):
    # Construct the local file path
    local_file_path = os.path.join(directory_path, file_name)
    # Read the data into a DataFrame
    data = pd.read_excel(local_file_path, engine='openpyxl')
    return data

# Directory and file names
directory_path = "C://Users/ellou/Downloads/web_scraping"
input_file_name = "address.xlsx"

# Extract data
raw = extract_data(directory_path, input_file_name)
df = raw.copy()

# Checking clean shape and nº of addresses
rows, cols = df.shape
address = df.customerid.unique().shape[0]

print(f'Dataset Shape\n\nNumber of rows : {rows}\nNumber of columns: {cols}\nNumber of different types of products : {address}')
df.info()

def convert_to_datetime(dataframe, column_name):
    try:
        dataframe[column_name] = pd.to_datetime(dataframe[column_name], errors='coerce')
    except (ValueError, TypeError):
        print(f"Conversion to datetime for column '{column_name}' failed.")

def convert_to_string(dataframe, column_name):
    try:
        dataframe[column_name]= dataframe[column_name].astype("string")
    except ValueError as ve:
        print(f"Conversion to string for column '{column_name}' failed due to ValueError: {ve}")
    except TypeError as te:
        print(f"Conversion to string for column '{column_name}' failed due to TypeError: {te}")


convert_to_datetime(df, 'created')
convert_to_datetime(df, 'updated')
convert_to_string(df, 'address1')
convert_to_string(df, 'address2')
convert_to_string(df, 'city')
convert_to_string(df, 'zip')
convert_to_string(df, 'firstname')
convert_to_string(df, 'lastname')
print("type :")
df.info()
# Missing values reporter function
def missing_value_reporter(data):
    # Replace special values with NaN
    data.replace("\\N", np.nan, inplace=True)
    # Check and drop empty columns
    empty_columns = data.columns[data.isnull().all()]
    data = data.drop(empty_columns, axis=1)
    na_count = data.isna().sum()
    na_count = na_count[na_count > 0]
    na_abs_frq = na_count.values
    na_rel_frq = na_count / na_count.sum()
    missings = pd.DataFrame({'column': na_count.index, 'Nº of missings': na_abs_frq, '% of missings': na_rel_frq})
    missings = missings.sort_values(by='Nº of missings', ascending=False)

    rows_to_drop = na_count.index.tolist()
    data = data.dropna(subset=rows_to_drop, how='any')

    return data, missings

result, missing_report = missing_value_reporter(data=df)

print("\nMissing values report:")
print(missing_report)
print("Data after dropping rows:")
print(result)


def load_data_to_postgres(final_data, table_name, database_url):
    try:
        engine = create_engine(database_url, poolclass=QueuePool)
        final_data.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Data has been loaded into the table {table_name} in PostgreSQL.')
    except SQLAlchemyError as e:
        print(f'Error: {e}')

# Replace 'your_table_name' and 'your_database_url' with your actual table name and database URL
#load_data_to_postgres(result, 'postgresql://postgres:123456127.0.0.1:5432/ETL_address')
load_data_to_postgres(result, 'Address', 'postgresql://postgres:123456@127.0.0.1:5432/ETL_address')
