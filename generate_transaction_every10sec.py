import random
import time
import pandas as pd
from datetime import datetime
from db_connection import get_connection, copy_dataframe_to_table


TXN_PER_BATCH = 10


def fetch_accounts():

    conn = get_connection()

    df = pd.read_sql(
        """
        SELECT account_id, account_type
        FROM web_banking.accounts
        WHERE status='ACTIVE'
        LIMIT 500
        """,
        conn
    )

    conn.close()

    return df


accounts_df = fetch_accounts()


def generate_transactions():

    batch = []

    for _ in range(TXN_PER_BATCH):

        row = accounts_df.sample(1).iloc[0]

        account_id = row["account_id"]
        account_type = row["account_type"]

        txn_type = random.choices(
            ["POS", "ATM", "BILL", "TRANSFER", "SALARY"],
            weights=[50, 20, 10, 10, 10]
        )[0]

        if txn_type == "SALARY":
            amount = random.randint(30000, 120000)
        elif txn_type == "ATM":
            amount = -random.randint(500, 10000)
        elif txn_type == "POS":
            amount = -random.randint(100, 5000)
        elif txn_type == "BILL":
            amount = -random.randint(500, 8000)
        else:
            amount = -random.randint(1000, 15000)

        balance = random.randint(5000, 100000)

        batch.append([
            account_id,
            datetime.now(),
            amount,
            txn_type,
            balance,
            datetime.now(),
            datetime.now(),
            False
        ])

    df = pd.DataFrame(batch, columns=[
        "account_id",
        "txn_timestamp",
        "amount",
        "txn_type",
        "balance_after_txn",
        "created_at",
        "updated_at",
        "is_deleted"
    ])

    copy_dataframe_to_table(df, "web_banking", "transactions")

    print(f"{len(df)} transactions inserted at {datetime.now()}")


def run_stream():

    print("Starting transaction stream...")

    while True:

        generate_transactions()

        time.sleep(5)


if __name__ == "__main__":
    run_stream()