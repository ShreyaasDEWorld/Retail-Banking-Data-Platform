from generate_accounts import generate_accounts
from db_connection import copy_dataframe_to_table

print("Generating accounts for existing customers...")
df_accounts = generate_accounts()

print("Loading accounts into PostgreSQL...")
copy_dataframe_to_table(df_accounts, "web_banking", "accounts")

print("Accounts load complete.")