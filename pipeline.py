import json
import os
from typing import Any

import pandas as pd
import mysql.connector
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config import MYSQL_CONFIG, GCP_PROJECT_ID, BQ_DATASET, STATE_FILE, TABLES


DEFAULT_STATE_BY_WATERMARK = {
    "timestamp": {"last_loaded_timestamp": "1970-01-01T00:00:00"},
    "id": {"last_loaded_id": 0},
}

STATE_KEY_BY_WATERMARK = {
    "timestamp": "last_loaded_timestamp",
    "id": "last_loaded_id",
}

MYSQL_TO_BQ_TYPE = {
    "tinyint": "INT64",
    "smallint": "INT64",
    "mediumint": "INT64",
    "int": "INT64",
    "integer": "INT64",
    "bigint": "INT64",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "float": "FLOAT64",
    "double": "FLOAT64",
    "real": "FLOAT64",
    "char": "STRING",
    "varchar": "STRING",
    "tinytext": "STRING",
    "text": "STRING",
    "mediumtext": "STRING",
    "longtext": "STRING",
    "enum": "STRING",
    "set": "STRING",
    "date": "DATE",
    "datetime": "DATETIME",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "json": "JSON",
    "bit": "BOOL",
    "bool": "BOOL",
    "boolean": "BOOL",
    "binary": "BYTES",
    "varbinary": "BYTES",
    "blob": "BYTES",
    "tinyblob": "BYTES",
    "mediumblob": "BYTES",
    "longblob": "BYTES",
}


def read_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {
            table_name: DEFAULT_STATE_BY_WATERMARK[table_config["watermark_type"]].copy()
            for table_name, table_config in TABLES.items()
        }

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def write_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_default_state(table_config: dict) -> dict:
    watermark_type = table_config["watermark_type"]
    if watermark_type not in DEFAULT_STATE_BY_WATERMARK:
        raise ValueError(f"Unsupported watermark_type: {watermark_type}")
    return DEFAULT_STATE_BY_WATERMARK[watermark_type].copy()


def get_state_value(table_config: dict, table_state: dict) -> Any:
    watermark_type = table_config["watermark_type"]
    state_key = STATE_KEY_BY_WATERMARK[watermark_type]
    default_state = get_default_state(table_config)
    return table_state.get(state_key, default_state[state_key])


def mysql_to_bq_type(mysql_type: str) -> str:
    return MYSQL_TO_BQ_TYPE.get(mysql_type.lower(), "STRING")


def get_mysql_schema(table_name: str) -> list[bigquery.SchemaField]:
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """

    cursor.execute(query, (MYSQL_CONFIG["database"], table_name))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    schema: list[bigquery.SchemaField] = []

    for row in rows:
        column_name = row["COLUMN_NAME"]
        mysql_type = row["DATA_TYPE"]
        is_nullable = row["IS_NULLABLE"]

        bq_type = mysql_to_bq_type(mysql_type)
        mode = "NULLABLE" if is_nullable == "YES" else "REQUIRED"

        schema.append(bigquery.SchemaField(column_name, bq_type, mode=mode))

    return schema


def build_query(table_name: str, table_config: dict) -> str:
    watermark_column = table_config["watermark_column"]

    return f"""
        SELECT *
        FROM {table_name}
        WHERE {watermark_column} > %s
        ORDER BY {watermark_column}
    """


def extract_from_mysql(table_name: str, table_config: dict, table_state: dict) -> pd.DataFrame:
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    watermark_value = get_state_value(table_config, table_state)
    query = build_query(table_name, table_config)

    print(
        f"Processing {table_name} using "
        f"{table_config['watermark_type']} watermark: {watermark_value}"
    )

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
        lower_col = col.lower()
        if "date" in lower_col or "time" in lower_col:
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    return df.where(pd.notnull(df), None)


def ensure_bigquery_table(client: bigquery.Client, table_name: str) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    mysql_schema = get_mysql_schema(table_name)

    try:
        table = client.get_table(table_id)
        existing_fields = {field.name for field in table.schema}
        missing_fields = [field for field in mysql_schema if field.name not in existing_fields]

        if missing_fields:
            table.schema = list(table.schema) + missing_fields
            client.update_table(table, ["schema"])
            print(
                f"Added new columns to {table_id}: "
                f"{[field.name for field in missing_fields]}"
            )
        else:
            print(f"BigQuery table schema is up to date: {table_id}")

    except NotFound:
        table = bigquery.Table(table_id, schema=mysql_schema)
        client.create_table(table)
        print(f"Created BigQuery table: {table_id}")


def load_to_bigquery(table_name: str, df: pd.DataFrame) -> None:
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    df = prepare_dataframe(df)
    ensure_bigquery_table(client, table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {len(df)} rows into BigQuery table {table_id}")


def update_state_for_table(table_name: str, table_config: dict, df: pd.DataFrame, state: dict) -> None:
    watermark_type = table_config["watermark_type"]
    watermark_column = table_config["watermark_column"]
    state_key = STATE_KEY_BY_WATERMARK[watermark_type]

    latest_value = df[watermark_column].max()

    if watermark_type == "timestamp":
        latest_value = pd.to_datetime(latest_value).isoformat()
    else:
        latest_value = int(latest_value)

    state[table_name] = {state_key: latest_value}
    print(f"Updated state for {table_name}: {state_key} = {latest_value}")


def run_pipeline() -> None:
    state = read_state()

    for table_name, table_config in TABLES.items():
        table_state = state.get(table_name, get_default_state(table_config))

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