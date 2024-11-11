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
    data = pd.read_excel(local_file_path)
    return data

def convert_to_numeric(dataframe, column_name):
    try:
        dataframe[column_name] = pd.to_numeric(dataframe[column_name], errors='coerce')
    except (ValueError, TypeError):
        print(f"Conversion to numeric for column '{column_name}' failed.")

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

def process_outlier(tmp, column_name, threshold=1.96):
    # discovering the specified column
    tmp_col = tmp[column_name]
    # calculating z-scores
    z = np.abs(stats.zscore(tmp_col))
    # Use boolean indexing to get the subset
    sub = z >= threshold

    # Checking review range and unique values from the instances
    min_val = tmp_col.min()
    max_val = tmp_col.max()
    if sub.sum() == 0:
        print(f'No instances found with z-score above the threshold ({threshold}).')
    else:
        for i in tmp_col[sub].index:
            print(
                f'The {Fore.LIGHTMAGENTA_EX}{column_name}{Style.RESET_ALL} column ranges from {min_val} to {max_val}\n\nUnique values from the {sub.sum()} {Fore.LIGHTMAGENTA_EX}{column_name}{Style.RESET_ALL} instances: {np.unique(tmp_col[i]).round(2)}{Style.RESET_ALL}')

    # Update the DataFrame by removing outliers
    tmp = tmp[~sub]
    return tmp

def load_data_to_postgres(filtered_data, table_name, database_url):
    try:
        engine = create_engine(database_url, poolclass=QueuePool)
        filtered_data.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Data has been loaded into the table {table_name} in PostgreSQL.')
    except SQLAlchemyError as e:
        print(f'Error: {e}')

# Directory and file names
directory_path = "C://web_scraping/"
input_file_name = "articles.xlsx"

# Extract data
raw = extract_data(directory_path, input_file_name)
df = raw.copy()

# Checking clean shape and nº of products
rows, cols = df.shape
products = df.productid.unique().shape[0]

print(f'Dataset Shape\n\nNumber of rows : {rows}\nNumber of columns: {cols}\nNumber of different types of products : {products}')
df.info()

convert_to_numeric(df, 'discountinpercent')
convert_to_numeric(df,'reducedprice')
convert_to_datetime(df, 'created')
convert_to_string(df, 'description')
convert_to_string(df, 'currentlyactive')
df.info()

result, missing_report = missing_value_reporter(data=df)

print("\nMissing values report:")
print(missing_report)
print("Data after dropping rows:")
print(result)
# Checking outliers with Z-score test

# Subseting numeric features and deleting binary for test
tmp = result.select_dtypes(include = ['float64', 'int64'])
print(tmp)

for col, rows in tmp.items():
    if col in tmp:
        if rows.value_counts().shape[0] == 2:
            del tmp[col]

# Array with all observations Z-score
z = np.abs(stats.zscore(tmp))

# Defining Z-score threshold
threshold = 1.96

# 2 arrays with the row and columns indices of  respectively
outloc = np.where(z>=threshold)

# High Z-score values count
cols = outloc[1]

for x in np.unique(cols):
    col = tmp.columns[x]
    occurrences = np.count_nonzero(cols == x)
    print(f'The {Fore.LIGHTMAGENTA_EX}{col}{Style.RESET_ALL} column has {occurrences} observations with a Z-score higher or equal to 1.96')
    # Process the outliers and update the tmp DataFrame
    tmp = process_outlier(tmp, col)
# Check if there are no outliers
if not np.any(z >= threshold):
    print("No outliers found (Z-score not above 1.96)")

filtered_data = tmp
load_data_to_postgres(filtered_data, 'Articles', 'postgresql://postgres:0000@127.0.0.1:5432/ETL')