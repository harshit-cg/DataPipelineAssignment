"""
Extract & transform script
- Fetches data from https://jsonplaceholder.typicode.com/posts
- Saves raw JSON and CSV under data/raw/
- Saves transformed CSV under data/transformed/

Usage:
    python extract_transform.py

This script is intentionally simple and uses environment-independent public API so you can run it locally.
"""
import os
import json
from datetime import datetime
import requests
import pandas as pd

RAW_DIR = os.path.join("data", "raw")
TRANSFORM_DIR = os.path.join("data", "transformed")

API_URL = "https://jsonplaceholder.typicode.com/posts"


def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(TRANSFORM_DIR, exist_ok=True)


def fetch_data():
    """Fetch JSON data from the public API and return as Python list/dicts."""
    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    return resp.json()


def transform_records(records):
    """Transform raw records into a pandas DataFrame with some lightweight transformations."""
    df = pd.DataFrame(records)
    # normalize column names
    df = df.rename(columns={"userId": "user_id", "id": "post_id"})

    # simple cleaning: strip title/body
    df["title"] = df["title"].astype(str).str.strip()
    df["body"] = df["body"].astype(str).str.strip()

    # add some derived fields
    df["title_length"] = df["title"].str.len()
    df["body_length"] = df["body"].str.len()
    df["fetched_at"] = pd.Timestamp.utcnow()

    return df


def save_raw(records):
    path = os.path.join(RAW_DIR, "posts.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    # also write a CSV for convenience
    df = pd.DataFrame(records)
    csv_path = os.path.join(RAW_DIR, "posts.csv")
    df.to_csv(csv_path, index=False)
    return path, csv_path


def save_transformed(df):
    path = os.path.join(TRANSFORM_DIR, "posts_transformed.csv")
    df.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    ensure_dirs()
    print(f"Fetching data from {API_URL} ...")
    records = fetch_data()
    print(f"Fetched {len(records)} records")

    raw_json_path, raw_csv_path = save_raw(records)
    print(f"Saved raw JSON to {raw_json_path}")
    print(f"Saved raw CSV to {raw_csv_path}")

    df = transform_records(records)
    transformed_path = save_transformed(df)
    print(f"Saved transformed CSV to {transformed_path}")

    print("Done.")
