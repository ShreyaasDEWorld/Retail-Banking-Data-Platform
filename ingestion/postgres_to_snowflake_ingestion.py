import psycopg2
import snowflake.connector
import pandas as pd
import logging
import time
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import DB_CONFIG, SNOWFLAKE_CONFIG


# -------------------------------
# Logging Setup
# -------------------------------
logging.basicConfig(
    filename="ingestion_log.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# -------------------------------
# PostgreSQL Connection
# -------------------------------
pg_conn = psycopg2.connect(
    host=DB_CONFIG["host"],
    port=DB_CONFIG["port"],
    dbname=DB_CONFIG["dbname"],
    user=DB_CONFIG["user"],
    password=DB_CONFIG["password"]
)


# -------------------------------
# Snowflake Connection
# -------------------------------
sf_conn = snowflake.connector.connect(
    user=SNOWFLAKE_CONFIG["user"],
    password=SNOWFLAKE_CONFIG["password"],
    account=SNOWFLAKE_CONFIG["account"],
    warehouse=SNOWFLAKE_CONFIG["warehouse"],
    database=SNOWFLAKE_CONFIG["database"],
    schema=SNOWFLAKE_CONFIG["schema"]
)

sf_cursor = sf_conn.cursor()

"""
        THIS CODE FOR COMPLETE in one go 
        # -------------------------------
# Table Load Function
# -------------------------------
def load_table(pg_table, sf_table):

    start_time = datetime.now()
    start_timer = time.time()

    logging.info(f"Starting load for {pg_table}")
    print(f"\nLoading {pg_table} → {sf_table}")

    try:

        query = f"SELECT * FROM {pg_table}"
        
        
        df = pd.read_sql(query, pg_conn)

        # Fix column case for Snowflake
        df.columns = [col.upper() for col in df.columns]

        success, nchunks, nrows, _ = write_pandas(
            conn=sf_conn,
            df=df,
            table_name=sf_table.upper(),
            schema="BRONZE"
        )

        end_time = datetime.now()
        duration = round(time.time() - start_timer, 2)

        status = "SUCCESS" if success else "FAILED"

        logging.info(
            f"{pg_table} loaded {nrows} rows in {duration} seconds"
        )

        print(f"{nrows} rows loaded in {duration} seconds")

    except Exception as e:

        end_time = datetime.now()
        duration = round(time.time() - start_timer, 2)
        status = "FAILED"
        nrows = 0

        logging.error(f"Failed loading {pg_table}: {str(e)}")
        print(f"Error loading {pg_table}: {str(e)}")


    # -------------------------------
    # Insert Audit Log
    # -------------------------------
    sf_cursor.execute(
        ""
        INSERT INTO pipeline_audit_log
        (pipeline_name, table_name, start_time, end_time, rows_loaded, status, duration_seconds)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        "",
        (
            "postgres_to_snowflake",
            sf_table,
            start_time,
            end_time,
            nrows,
            status,
            duration
        )
    )

    sf_conn.commit()

"""


def get_last_loaded_timestamp(table_name):

    query = f"""
    SELECT MAX(end_time)
    FROM pipeline_audit_log
    WHERE table_name = '{table_name}'
    AND status = 'SUCCESS'
    """

    sf_cursor.execute(query)
    result = sf_cursor.fetchone()

    return result[0]
 
# -------------------------------
# Table Load Function (Batch)
# -------------------------------
def load_table(pg_table, sf_table):

    start_time = datetime.now()
    start_timer = time.time()

    logging.info(f"Starting load for {pg_table}")
    print(f"\nLoading {pg_table} → {sf_table}")

    total_rows = 0
    batch_number = 1
    chunksize = 100000

    try:

        # query = f"SELECT * FROM {pg_table}"
        # add below code for Incremental Load
        last_loaded = get_last_loaded_timestamp(sf_table)

        if last_loaded:
            query = f"""
            SELECT *
            FROM {pg_table}
            WHERE created_at > '{last_loaded}'
            """
        else:
            query = f"SELECT * FROM {pg_table}" 
        


        for df in pd.read_sql(query, pg_conn, chunksize=chunksize):

            batch_start = time.time()

            # Fix column case for Snowflake
            df.columns = [col.upper() for col in df.columns]

            success, nchunks, nrows, _ = write_pandas(
                conn=sf_conn,
                df=df,
                table_name=sf_table.upper(),
                schema="BRONZE"
            )

            total_rows += nrows
            batch_time = round(time.time() - batch_start, 2)

            logging.info(
                f"{pg_table} batch {batch_number} loaded {nrows} rows in {batch_time} seconds"
            )

            print(f"Batch {batch_number}: {nrows} rows loaded in {batch_time} sec")

            batch_number += 1

        end_time = datetime.now()
        duration = round(time.time() - start_timer, 2)

        status = "SUCCESS"

        logging.info(
            f"{pg_table} total rows loaded: {total_rows} in {duration} seconds"
        )

        print(f"\n{total_rows} rows loaded in {duration} seconds")

    except Exception as e:

        end_time = datetime.now()
        duration = round(time.time() - start_timer, 2)
        status = "FAILED"
        total_rows = 0

        logging.error(f"Failed loading {pg_table}: {str(e)}")
        print(f"Error loading {pg_table}: {str(e)}")


    # -------------------------------
    # Insert Audit Log
    # -------------------------------
    sf_cursor.execute(
        """
        INSERT INTO pipeline_audit_log
        (pipeline_name, table_name, start_time, end_time, rows_loaded, status, duration_seconds)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            "postgres_to_snowflake",
            sf_table,
            start_time,
            end_time,
            total_rows,
            status,
            duration
        )
    )

    sf_conn.commit()


# -------------------------------
# Pipeline Start
# -------------------------------
pipeline_start = datetime.now()

logging.info("Pipeline started")
print("Pipeline started")


# -------------------------------
# Tables to Load
# -------------------------------
tables = {
    "web_banking.accounts": "accounts_raw",
    "web_banking.customers": "customers_raw",
    "web_banking.transactions": "transactions_raw"
}




# -------------------------------
# Run Loads
# -------------------------------
for pg_table, sf_table in tables.items():
    load_table(pg_table, sf_table)


# -------------------------------
# Pipeline End
# -------------------------------
pipeline_end = datetime.now()
total_duration = pipeline_end - pipeline_start

logging.info(f"Pipeline finished. Total runtime: {total_duration}")

print(f"\nPipeline completed in {total_duration}")


# -------------------------------
# Close Connections
# -------------------------------
sf_cursor.close()
sf_conn.close()
pg_conn.close()