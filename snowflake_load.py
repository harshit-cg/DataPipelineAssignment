"""
Load transformed CSV into Snowflake and create raw table + views.
Environment variables required:
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- SNOWFLAKE_ROLE (optional)

Usage:
    python snowflake_load.py --file data/transformed/posts_transformed.csv --raw_table RAW_POSTS --transformed_view TRANSFORMED_POSTS_VIEW --final_view FINAL_POSTS_VIEW
"""

import os
import argparse
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def connect_snowflake():
    """Establish Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def ensure_db_objects(conn, raw_table):
    """Create raw table if not exists with uppercase column names."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {raw_table} (
                POST_ID INTEGER,
                USER_ID INTEGER,
                TITLE STRING,
                BODY STRING,
                TITLE_LENGTH INTEGER,
                BODY_LENGTH INTEGER,
                FETCHED_AT TIMESTAMP_NTZ
            )
        """)
        conn.commit()
    finally:
        cur.close()


def create_views(conn, raw_table, transformed_view, final_view):
    """Create transformed and final views."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE OR REPLACE VIEW {transformed_view} AS
            SELECT POST_ID, USER_ID, TITLE, BODY, TITLE_LENGTH, BODY_LENGTH, FETCHED_AT
            FROM {raw_table}
        """)

        cur.execute(f"""
            CREATE OR REPLACE VIEW {final_view} AS
            SELECT USER_ID, COUNT(*) AS POSTS_COUNT, AVG(BODY_LENGTH) AS AVG_BODY_LENGTH
            FROM {raw_table}
            GROUP BY USER_ID
        """)
        conn.commit()
    finally:
        cur.close()


def load_dataframe(conn, df, table_name):
    """Upload DataFrame to Snowflake using write_pandas."""
    # Normalize column names to uppercase for Snowflake compatibility
    df.columns = [col.upper() for col in df.columns]

    # Upload with logical type enabled for datetime columns
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name, use_logical_type=True)
    print(f"write_pandas: success={success}, nchunks={nchunks}, nrows={nrows}")
    return success


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Local transformed CSV file")
    parser.add_argument("--raw_table", default="RAW_POSTS", help="Raw table name")
    parser.add_argument("--transformed_view", default="TRANSFORMED_POSTS_VIEW", help="Transformed view name")
    parser.add_argument("--final_view", default="FINAL_POSTS_VIEW", help="Final view name")
    args = parser.parse_args()

    # Load CSV into DataFrame
    df = pd.read_csv(args.file)

    # Convert fetched_at column to datetime if present
    if "fetched_at" in df.columns:
        df["fetched_at"] = pd.to_datetime(df["fetched_at"])

    # Connect to Snowflake and execute operations
    conn = connect_snowflake()
    try:
        ensure_db_objects(conn, args.raw_table)
        load_dataframe(conn, df, args.raw_table)
        create_views(conn, args.raw_table, args.transformed_view, args.final_view)
        print("✅ Snowflake load and view creation complete.")
    finally:
        conn.close()