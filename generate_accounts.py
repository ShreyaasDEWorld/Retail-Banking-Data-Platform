import random
import pandas as pd
from datetime import datetime, timedelta
from db_connection import get_connection


def fetch_customers():
    conn = get_connection()
    query = "SELECT customer_id FROM web_banking.customers"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def generate_accounts():

    customers_df = fetch_customers()
    accounts = []

    today = datetime.now().date()

    for customer_id in customers_df["customer_id"]:

        num_accounts = random.choices(
            [1, 2, 3],
            weights=[0.7, 0.25, 0.05]
        )[0]

        for _ in range(num_accounts):

            #account_type = random.choice(["SAVINGS", "CURRENT", "CREDIT_CARD"])
            account_type = random.choice(["SAVINGS", "CURRENT"])

            open_date = today - timedelta(days=random.randint(30, 730))

            status = random.choices(
                ["ACTIVE", "CLOSED"],
                weights=[0.9, 0.10]
            )[0]

            accounts.append([
                customer_id,
                account_type,
                open_date,
                status,
                datetime.now(),
                datetime.now(),
                False
            ])

    df = pd.DataFrame(accounts, columns=[
        "customer_id",
        "account_type",
        "open_date",
        "status",
        "created_at",
        "updated_at",
        "is_deleted"
    ])

    return df