import random
import pandas as pd
from datetime import datetime
from db_connection import get_connection, copy_dataframe_to_table


NUM_TRANSACTIONS = 10


def generate_transactions():

    conn = get_connection()

    accounts = pd.read_sql(
        "SELECT account_id FROM web_banking.accounts WHERE status='ACTIVE' LIMIT 100",
        conn
    )

    conn.close()

    batch = []

    for _ in range(NUM_TRANSACTIONS):

        account = random.choice(accounts["account_id"].tolist())

        amount = random.randint(100, 5000)

        batch.append([
            account,
            datetime.now(),
            -amount,
            random.choice(["POS", "ATM", "BILL", "TRANSFER"]),
            random.randint(1000, 50000),
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

    print(f"{NUM_TRANSACTIONS} transactions inserted successfully")


if __name__ == "__main__":
    generate_transactions()