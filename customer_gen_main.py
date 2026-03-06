# main.py

from generate_customers import generate_customers
from db_connection import copy_dataframe_to_table

from generate_accounts import generate_accounts
from db_connection import copy_dataframe_to_table
def main():

    print("Generating customers...")
    df_customers = generate_customers()

    print("Loading into PostgreSQL...")
    copy_dataframe_to_table(
        df_customers,
        schema="web_banking",
        table="customers"
    )

    print("Done.")

if __name__ == "__main__":
    main()