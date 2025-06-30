import pandas as pd
import pyodbc
from datetime import datetime

# Configuration
csv_file   = "organize_data_filtered.csv"
table_name = "YourTableName"
server     = "YOUR_SERVER_NAME"
database   = "YOUR_DATABASE_NAME"
username   = "YOUR_DB_USERNAME"
password   = "YOUR_DB_PASSWORD"
driver     = "{ODBC Driver 17 for SQL Server}"

# Read & transform
df = pd.read_csv(csv_file)
df["negotiated_rates__negotiated_prices__billing_class"] = (
    df["negotiated_rates__negotiated_prices__billing_class"]
      .replace({"institutional": "I"})
)
df["load_date"] = datetime.now().date()

# Rename columns to match your DB — same mapping dict as before
column_mapping = {
    # ... (use same keys/values as above)
}
df = df.rename(columns=column_mapping)

# Build connection
conn = pyodbc.connect(
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)
cursor = conn.cursor()

# Generate an INSERT statement
cols = df.columns.tolist()
placeholders = ", ".join("?" for _ in cols)
insert_sql = (
    f"INSERT INTO {table_name} ({', '.join(cols)}) "
    f"VALUES ({placeholders})"
)

# Insert row by row (or you can batch via executemany)
cursor.fast_executemany = True
cursor.executemany(insert_sql, df.values.tolist())
conn.commit()
cursor.close()
conn.close()

print(f"Loaded {len(df)} rows into {database}.{table_name}")




































import pandas as pd
import pyodbc
from datetime import datetime

# Configuration
csv_file   = "organize_data_filtered.csv"
table_name = "YourTableName"
server     = "YOUR_SERVER_NAME"       # e.g. "localhost\\SQLEXPRESS"
database   = "YOUR_DATABASE_NAME"
driver     = "{ODBC Driver 17 for SQL Server}"

# Read & transform
df = pd.read_csv(csv_file)
# Replace "institutional" with "I"
df["negotiated_rates__negotiated_prices__billing_class"] = (
    df["negotiated_rates__negotiated_prices__billing_class"]
      .replace({"institutional": "I"})
)
# Add a load_date column (today's date)
df["load_date"] = datetime.now().date()

# Rename columns to match your DB schema
column_mapping = {
    "name":                          "db_name_col",
    "billing_code_type":             "db_code_type_col",
    "billing_code_type_version":     "db_code_type_ver_col",
    "billing_code":                  "db_code_col",
    "negotiated_rates__provider_groups__npi":            "db_npi_col",
    "negotiated_rates__provider_groups__tin_type":        "db_tin_type_col",
    "negotiated_rates__provider_groups__tin_value":       "db_tin_value_col",
    "negotiated_rates__negotiated_prices__negotiated_type":      "db_neg_type_col",
    "negotiated_rates__negotiated_prices__negotiated_rate":      "db_neg_rate_col",
    "negotiated_rates__negotiated_prices__expiration_date":      "db_exp_date_col",
    "negotiated_rates__negotiated_prices__service_code":         "db_service_code_col",
    "negotiated_rates__negotiated_prices__billing_class":        "db_bill_class_col",
    "negotiated_rates__negotiated_prices__billing_code_modifier": "db_modifier_col",
    "load_date":                    "load_date"
}
df = df.rename(columns=column_mapping)

# Build connection (Windows Authentication / Trusted Connection)
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Prepare INSERT
cols = df.columns.tolist()
placeholders = ", ".join("?" for _ in cols)
insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})"

# Bulk insert
cursor.fast_executemany = True
cursor.executemany(insert_sql, df.values.tolist())
conn.commit()

cursor.close()
conn.close()

print(f"Loaded {len(df)} rows into {database}.{table_name}")

