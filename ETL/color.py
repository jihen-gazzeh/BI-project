import pandas as pd

directory_path = "C:\\Users\\user\\OneDrive\\Bureau\\webshop-data"
input_file_name = "colors.xlsx"

# Read data from Excel
df = pd.read_excel(f"{directory_path}\\{input_file_name}")

# Check for missing values
print("Missing values before transformation:")
print(df.isnull().sum())




df = df.sort_values(by='name')

print("\nModified DataFrame:")
print(df)

df.to_csv('colors.csv', index=False)
