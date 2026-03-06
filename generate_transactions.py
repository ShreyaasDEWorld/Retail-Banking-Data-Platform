import random
import pandas as pd
from datetime import datetime, timedelta
from db_connection import get_connection, copy_dataframe_to_table


#BATCH_SIZE = 100000
BATCH_SIZE = 100


def fetch_accounts():
    conn = get_connection()
    query = """
        SELECT account_id, account_type, open_date, status
        FROM web_banking.accounts
        WHERE status = 'ACTIVE'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def generate_transactions():

    accounts_df = fetch_accounts()
    today = datetime.now()

    batch = []
    total_inserted = 0
    

    for _, row in accounts_df.iterrows():

        account_id = row["account_id"]
        account_type = row["account_type"]
        open_date = row["open_date"]

        balance = random.randint(5000, 20000)
        # This is for 2M tranaction 
        #current_date = datetime.combine(open_date, datetime.min.time())
        # Now we are doing this for 100 tranaction for increamntal load
        current_date = datetime.now()

        salary_amount = random.randint(30000, 120000)

        while current_date < today:

            # Monthly salary (only savings)
            if account_type == "SAVINGS" and current_date.day in [1, 2, 3]:
                balance += salary_amount
                batch.append([
                    account_id,
                    current_date,
                    salary_amount,
                    "SALARY",
                    balance,
                    datetime.now(),
                    datetime.now(),
                    False
                ])

            # Random daily spend probability
            if random.random() < 0.4:
                spend = random.randint(100, 5000)
                balance -= spend
                batch.append([
                    account_id,
                    current_date,
                    -spend,
                    random.choice(["POS", "ATM", "BILL", "TRANSFER"]),
                    balance,
                    datetime.now(),
                    datetime.now(),
                    False
                ])

            current_date += timedelta(days=1)

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                df_batch = pd.DataFrame(batch, columns=[
                    "account_id",
                    "txn_timestamp",
                    "amount",
                    "txn_type",
                    "balance_after_txn",
                    "created_at",
                    "updated_at",
                    "is_deleted"
                ])

                copy_dataframe_to_table(df_batch, "web_banking", "transactions")

                total_inserted += len(df_batch)
                print(f"Inserted {total_inserted} transactions so far...")

                batch.clear()

    # Insert remaining
    if batch:
        df_batch = pd.DataFrame(batch, columns=[
            "account_id",
            "txn_timestamp",
            "amount",
            "txn_type",
            "balance_after_txn",
            "created_at",
            "updated_at",
            "is_deleted"
        ])

        copy_dataframe_to_table(df_batch, "web_banking", "transactions")

        total_inserted += len(df_batch)

    print(f"Total transactions inserted: {total_inserted}")
    print("File loaded successfully")