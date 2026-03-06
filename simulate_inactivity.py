#Step 2 — Soft Delete Their Recent Transactions

import random
from datetime import datetime, timedelta
from db_connection import get_connection


def simulate_behavioral_churn():

    conn = get_connection()
    cursor = conn.cursor()

    print("Simulating behavioral inactivity...")

    # Get currently ACTIVE customers only
    cursor.execute("""
        SELECT customer_id
        FROM analytics.customer_churn_view
        WHERE churn_flag = 0
        ORDER BY random()
        LIMIT 500
    """)
    customers = [row[0] for row in cursor.fetchall()]

    if not customers:
        print("No active customers found.")
        return

    cutoff_date = datetime.now() - timedelta(days=120)

    for cid in customers:
        cursor.execute("""
            UPDATE web_banking.transactions t
            SET is_deleted = true,
                updated_at = %s
            FROM web_banking.accounts a
            WHERE t.account_id = a.account_id
            AND a.customer_id = %s
            AND t.txn_timestamp >= %s
        """, (datetime.now(), cid, cutoff_date))

    conn.commit()
    cursor.close()
    conn.close()

    print("Behavioral churn simulation complete.")