# db_connection.py

import psycopg2
import io
from config import DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def copy_dataframe_to_table(df, schema, table):

    conn = get_connection()
    cursor = conn.cursor()

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    full_table_name = f"{schema}.{table}"

    # IMPORTANT: Explicit column list
    column_list = ",".join(df.columns)

    copy_sql = f"""
        COPY {full_table_name} ({column_list})
        FROM STDIN
        WITH CSV
    """

    try:
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()
        print(f"Loaded {len(df)} rows into {full_table_name}")

    except Exception as e:
        conn.rollback()
        print("Error during COPY:", e)

    finally:
        cursor.close()
        conn.close()