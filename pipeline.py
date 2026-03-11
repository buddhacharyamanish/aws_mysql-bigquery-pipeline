import json
import os
from typing import Any

import pandas as pd
import mysql.connector
from google.cloud import bigquery
from config import MYSQL_CONFIG, GCP_PROJECT_ID, BQ_DATASET, STATE_FILE, TABLES


def read_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            "sample_table": {
                "last_loaded_timestamp": "1970-01-01T00:00:00"
            },
            "sample_table2": {
                "last_loaded_id": 0
            }
        }

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def write_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_default_state(table_name: str, table_config: dict) -> dict:
    if table_config["watermark_type"] == "timestamp":
        return {"last_loaded_timestamp": "1970-01-01T00:00:00"}
    if table_config["watermark_type"] == "id":
        return {"last_loaded_id": 0}

    raise ValueError(f"Unsupported watermark_type for {table_name}")


def build_query(table_name: str, table_config: dict) -> str:
    watermark_column = table_config["watermark_column"]

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {watermark_column} > %s
        ORDER BY {watermark_column}
    """

    return query


def extract_from_mysql(table_name: str, table_config: dict, table_state: dict) -> pd.DataFrame:
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    watermark_type = table_config["watermark_type"]
    query = build_query(table_name, table_config)

    if watermark_type == "timestamp":
        watermark_value = table_state.get("last_loaded_timestamp", "1970-01-01T00:00:00")
        print(f"Processing {table_name} using timestamp watermark: {watermark_value}")
    elif watermark_type == "id":
        watermark_value = table_state.get("last_loaded_id", 0)
        print(f"Processing {table_name} using id watermark: {watermark_value}")
    else:
        raise ValueError(f"Unsupported watermark_type for {table_name}")

    cursor.execute(query, (watermark_value,))
    rows: list[dict[str, Any]] = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame(rows)
    print(f"Fetched {len(df)} rows from MySQL table {table_name}")
    return df


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    df = df.where(pd.notnull(df), None)
    return df


def ensure_table_exists(client: bigquery.Client, table_id: str, df: pd.DataFrame) -> None:
    try:
        client.get_table(table_id)
    except Exception:
        # Creating table from dataframe schema
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = client.load_table_from_dataframe(df.head(0), table_id, job_config=job_config)
        job.result()
        print(f"Created BigQuery table {table_id}")


def load_to_bigquery(table_name: str, df: pd.DataFrame) -> None:
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    df = prepare_dataframe(df)
    ensure_table_exists(client, table_id, df)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into BigQuery table {table_id}")


def update_state_for_table(table_name: str, table_config: dict, df: pd.DataFrame, state: dict) -> None:
    watermark_type = table_config["watermark_type"]
    watermark_column = table_config["watermark_column"]

    if watermark_type == "timestamp":
        latest_value = pd.to_datetime(df[watermark_column].max()).isoformat()
        state[table_name] = {"last_loaded_timestamp": latest_value}
        print(f"Updated state for {table_name}: last_loaded_timestamp = {latest_value}")

    elif watermark_type == "id":
        latest_value = int(df[watermark_column].max())
        state[table_name] = {"last_loaded_id": latest_value}
        print(f"Updated state for {table_name}: last_loaded_id = {latest_value}")

    else:
        raise ValueError(f"Unsupported watermark_type for {table_name}")


def run_pipeline() -> None:
    state = read_state()

    for table_name, table_config in TABLES.items():
        table_state = state.get(table_name, get_default_state(table_name, table_config))

        df = extract_from_mysql(table_name, table_config, table_state)

        if df.empty:
            print(f"No new rows found for {table_name}")
            continue

        load_to_bigquery(table_name, df)
        update_state_for_table(table_name, table_config, df, state)

    write_state(state)
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()