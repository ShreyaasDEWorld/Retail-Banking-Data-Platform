import psycopg2
import random
from datetime import datetime
from db_connection import get_connection


def simulate_customer_updates():
    conn = get_connection()
    cursor = conn.cursor()

    print("Simulating customer updates...")

    # Get random customers
    cursor.execute("""
        SELECT customer_id 
        FROM web_banking.customers
        WHERE is_deleted = false
        ORDER BY random()
        LIMIT 500
    """)
    customers = [row[0] for row in cursor.fetchall()]

    if not customers:
        print("No customers found.")
        return

    # Split for different CDC actions
    income_change = customers[:300]
    soft_delete = customers[300:400]
    city_change = customers[400:500]

    now = datetime.now()

    # 1️⃣ Income band safe rotation
    for cid in income_change:
        cursor.execute("""
            UPDATE web_banking.customers
            SET income_band = CASE income_band
                WHEN 'LOW' THEN 'MID'
                WHEN 'MID' THEN 'HIGH'
                WHEN 'HIGH' THEN 'PREMIUM'
                WHEN 'PREMIUM' THEN 'HIGH'
                ELSE income_band
            END,
            updated_at = %s
            WHERE customer_id = %s
        """, (now, cid))

    # 2️⃣ Soft delete
    for cid in soft_delete:
        cursor.execute("""
            UPDATE web_banking.customers
            SET is_deleted = true,
                updated_at = %s
            WHERE customer_id = %s
        """, (now, cid))

    # 3️⃣ City update (safe text change)
    for cid in city_change:
        cursor.execute("""
            UPDATE web_banking.customers
            SET city = city || ' Updated',
                updated_at = %s
            WHERE customer_id = %s
        """, (now, cid))

    conn.commit()
    cursor.close()
    conn.close()

    print("Customer CDC simulation complete.")


def simulate_account_updates():
    conn = get_connection()
    cursor = conn.cursor()

    print("Simulating account updates...")

    cursor.execute("""
        SELECT account_id
        FROM web_banking.accounts
        WHERE is_deleted = false
        ORDER BY random()
        LIMIT 300
    """)
    accounts = [row[0] for row in cursor.fetchall()]

    if not accounts:
        print("No accounts found.")
        return

    close_accounts = accounts[:200]
    soft_delete = accounts[200:300]

    now = datetime.now()

    # 1️⃣ Close accounts (respect constraint)
    for aid in close_accounts:
        cursor.execute("""
            UPDATE web_banking.accounts
            SET status = 'CLOSED',
                updated_at = %s
            WHERE account_id = %s
        """, (now, aid))

    # 2️⃣ Soft delete
    for aid in soft_delete:
        cursor.execute("""
            UPDATE web_banking.accounts
            SET is_deleted = true,
                updated_at = %s
            WHERE account_id = %s
        """, (now, aid))

    conn.commit()
    cursor.close()
    conn.close()

    print("Account CDC simulation complete.")


def run_cdc_simulation():
    print("Running CDC simulation...")
    simulate_customer_updates()
    simulate_account_updates()
    print("CDC simulation finished successfully.")