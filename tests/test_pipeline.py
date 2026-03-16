import sys
from pathlib import Path
import json
sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
import pipeline

from unittest.mock import MagicMock
from google.cloud import bigquery
from google.api_core.exceptions import NotFound




# TEST 1: MYSQL TO BIGQUERY DATATYPE MAPPING

def test_mysql_to_bq_type_int():
    """
    Purpose:
    Check whether MySQL datatype 'int' is correctly converted to BigQuery datatype 'INT64'.
    """
    assert pipeline.mysql_to_bq_type("int") == "INT64"


# TEST 2: VARCHAR DATATYPE MAPPING

def test_mysql_to_bq_type_varchar():
    """
    Purpose:
    Check whether MySQL datatype 'varchar' is converted to BigQuery datatype 'STRING' because Text columns in MySQL is represented as STRING in BigQuery.
    """
    assert pipeline.mysql_to_bq_type("varchar") == "STRING"


# TEST 3: DATETIME DATATYPE MAPPING

def test_mysql_to_bq_type_datetime():
    """
    Purpose:
    Verify that MySQL datatype 'datetime' maps to BigQuery datatype 'DATETIME'. Timestamp and datetime fields are important in this pipeline because wrong mapping would break watermark logic and table creation.
    """
    assert pipeline.mysql_to_bq_type("datetime") == "DATETIME"


# TEST 4: UNKNOWN DATATYPE FALLBACK

def test_mysql_to_bq_type_unknown():
    """
    Purpose:
    Check what happens when an unknown datatype is passed.

    Expected behavior:
    The pipeline should safely default to STRING instead of failing.

    Defaulting to STRING is safer than breaking the pipeline.
    """
    assert pipeline.mysql_to_bq_type("unknown_type") == "STRING"


# TEST 5: DEFAULT STATE FOR TIMESTAMP WATERMARK

def test_get_default_state_timestamp():
    """
    Purpose:
    Verify that a table using timestamp watermark gets the correct default state.

    Input:
    A table configuration saying watermark_type = 'timestamp'

    Expected output:
    {'last_loaded_timestamp': '1970-01-01T00:00:00'}

    ##### Reason for testing:
    On the first run, if no state.json exists, the pipeline needs a starting point.
    """
    table_config = {"watermark_type": "timestamp"}
    result = pipeline.get_default_state(table_config)
    assert result == {"last_loaded_timestamp": "1970-01-01T00:00:00"}


# TEST 6: DEFAULT STATE FOR ID WATERMARK

def test_get_default_state_id():
    """
    Purpose:
    Verify that a table using id watermark gets the correct default state.

    Input:
    A table configuration saying watermark_type = 'id'

    Expected output:
    {'last_loaded_id': 0}

    Reason:
    For id-based incremental loads, the pipeline starts by pulling rows where id > 0.
    """
    table_config = {"watermark_type": "id"}
    result = pipeline.get_default_state(table_config)
    assert result == {"last_loaded_id": 0}


# TEST 7: QUERY BUILDING TESTS

def test_build_query():
    """
    Purpose:
    Verify that the SQL query is built correctly for incremental extraction.

    Input:
    table_name = 'sample_table'
    watermark_column = 'created_date'

    Expected query parts:
    - FROM sample_table
    - WHERE created_date > %s
    - ORDER BY created_date

    Reson for Test:
    The pipeline must only pull new rows using the watermark column.
    """
    table_config = {"watermark_column": "created_date"}
    query = pipeline.build_query("sample_table", table_config)

    assert "FROM sample_table" in query
    assert "WHERE created_date > %s" in query
    assert "ORDER BY created_date" in query


# TEST 8: DATAFRAME DATETIME CONVERSION TEST

def test_prepare_dataframe_converts_datetime():
    """
    Purpose:
    Check whether prepare_dataframe() converts date-like string columns into pandas datetime type.

    Input:
    A dataframe where created_date is stored as string.

    Expected:
    created_date should become a datetime column.

    Reason for Testing:
    BigQuery needs proper datetime-compatible values for DATETIME/TIMESTAMP fields.
    """
    df = pd.DataFrame({
        "id": [1, 2],
        "created_date": ["2026-03-10 10:00:00", "2026-03-10 11:00:00"],
        "name": ["A", "B"]
    })

    result = pipeline.prepare_dataframe(df)

    assert pd.api.types.is_datetime64_any_dtype(result["created_date"])


# TEST 9: STATE UPDATE FOR ID WATERMARK

def test_update_state_for_table_id():
    """
    Purpose:
    Verify that the pipeline correctly updates state for an id-based table.

    Input:
    DataFrame with ids [10, 20, 30]

    Expected:
    last_loaded_id should become 30

    Reason:
    After loading data, the pipeline must record the highest id processed,
    so next time it only pulls rows greater than that.
    """
    df = pd.DataFrame({"id": [10, 20, 30]})
    state = {}
    table_config = {
        "watermark_type": "id",
        "watermark_column": "id"
    }

    pipeline.update_state_for_table("sample_table2", table_config, df, state)

    assert state["sample_table2"]["last_loaded_id"] == 30


# TEST 10: STATE UPDATE FOR TIMESTAMP WATERMARK

def test_update_state_for_table_timestamp():
    """
    Purpose:
    Verify that the pipeline correctly updates state for a timestamp-based table.

    Input:
    DataFrame with two created_date values

    Expected:
    state for sample_table should contain last_loaded_timestamp

    Reason for the test:
    After loading timestamp-based data, the pipeline must write the latest timestamp processed,
    so next run only pulls newer records.
    """
    df = pd.DataFrame({
        "created_date": pd.to_datetime(["2026-03-10 10:00:00", "2026-03-10 11:00:00"])
    })
    state = {}
    table_config = {
        "watermark_type": "timestamp",
        "watermark_column": "created_date"
    }

    pipeline.update_state_for_table("sample_table", table_config, df, state)

    assert "last_loaded_timestamp" in state["sample_table"]







                        ######################### FILE-BASED TESTS ###########################


def test_write_state(tmp_path, monkeypatch):
    """
    Purpose:
    Verify that the write_state() function correctly writes the state dictionary
    into a JSON file.

    Reason for the Test:
    Our pipeline uses a state.json file to track the latest watermark value
    (timestamp or id). This allows the pipeline to perform incremental loading
    instead of reloading all records every time.

    tmp_path:
    tmp_path is a pytest fixture that creates a temporary directory for the test.
    This ensures the test does not modify my real state.json file.

    monkeypatch purpose:
    monkeypatch temporarily replaces variables during the test. Here I replaced
    pipeline.STATE_FILE with the temporary file path.
    """

    # Create a temporary file path inside the test directory
    fake_state_file = tmp_path / "state.json"

    # Replace the pipeline STATE_FILE variable with this temporary file
    monkeypatch.setattr(pipeline, "STATE_FILE", str(fake_state_file))

    # Example state dictionary that the pipeline uses
    state = {
        "sample_table": {"last_loaded_timestamp": "2026-03-10T10:00:00"},
        "sample_table2": {"last_loaded_id": 200}
    }

    # Call the function we want to test
    pipeline.write_state(state)

    # Open the temporary file and read its content
    with open(fake_state_file, "r", encoding="utf-8") as f:
        saved = json.load(f)

    # Verify the saved JSON content matches the expected state dictionary
    assert saved == state


def test_read_state_when_file_missing(tmp_path, monkeypatch):
    """
    Purpose:
    Verify that read_state() returns default watermark values when the state file
    does not exist.

    Reason:
    On the first run of the pipeline, state.json will not exist yet. The pipeline
    must initialize default watermark values so that it knows where to start
    pulling data.

    This test ensures that the pipeline correctly generates those default values.
    """

    # Create a fake path that does not exist
    fake_state_file = tmp_path / "missing_state.json"

    # Replace pipeline.STATE_FILE with this non-existent file path
    monkeypatch.setattr(pipeline, "STATE_FILE", str(fake_state_file))

    # Replace pipeline.TABLES configuration with a simple test version
    monkeypatch.setattr(
        pipeline,
        "TABLES",
        {
            "sample_table": {"watermark_type": "timestamp"},
            "sample_table2": {"watermark_type": "id"},
        }
    )

    # Call the function
    state = pipeline.read_state()

    # Verify that default values are returned
    assert state["sample_table"]["last_loaded_timestamp"] == "1970-01-01T00:00:00"
    assert state["sample_table2"]["last_loaded_id"] == 0


                    ####################### MOCK-BASED BIGQUERY TESTS #########################

def test_ensure_bigquery_table_creates_when_missing(monkeypatch):
    """
    Purpose:
    Verify that ensure_bigquery_table() creates a BigQuery table when the table
    does not already exist.

    Reason:
    When the pipeline runs for the first time, the BigQuery table might not exist.
    The pipeline should automatically create it using the MySQL schema.

    Mechanism of Mock:
    Instead of connecting to real BigQuery, we simulate the BigQuery client.
    """

    # Create a fake BigQuery client
    mock_client = MagicMock()

    # Simulate BigQuery throwing a "table not found" error
    mock_client.get_table.side_effect = NotFound("Table not found")

    # Replace the get_mysql_schema() function so it returns a simple schema
    monkeypatch.setattr(
        pipeline,
        "get_mysql_schema",
        lambda table_name: [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
        ]
    )

    # Run the function being tested
    pipeline.ensure_bigquery_table(mock_client, "sample_table")

    # Verify that the BigQuery create_table() function was called
    assert mock_client.create_table.called


def test_ensure_bigquery_table_adds_missing_fields(monkeypatch):
    """
    Purpose:
    Verify that ensure_bigquery_table() adds missing columns to a BigQuery table
    when new columns appear in the MySQL source.

    Reason:
    Production databases often evolve. New columns may be added in MySQL.
    The pipeline must automatically update the BigQuery schema.

    This test ensures that schema evolution logic works correctly.
    """

    # Create a fake BigQuery client
    mock_client = MagicMock()

    # Simulate an existing BigQuery table with only one column
    mock_table = MagicMock()
    mock_table.schema = [bigquery.SchemaField("id", "INT64")]

    # When get_table() is called, return this fake table
    mock_client.get_table.return_value = mock_table

    # Simulate MySQL schema returning two columns
    monkeypatch.setattr(
        pipeline,
        "get_mysql_schema",
        lambda table_name: [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("name", "STRING"),
        ]
    )

    # Run the function
    pipeline.ensure_bigquery_table(mock_client, "sample_table")

    # Verify that update_table() was called to add the missing column
    assert mock_client.update_table.called


                        ###################### ORCHESTRATION TEST ################

def test_run_pipeline(monkeypatch):
    """
    Purpose:
    Verify that the main run_pipeline() function orchestrates the pipeline correctly.

    Reason:
    run_pipeline() is the central function that simply coordinates following:
    - reading state
    - extracting data from MySQL
    - loading data to BigQuery
    - updating watermark state

    This test ensures that the correct internal functions are called.
    """

    # Replace pipeline.TABLES configuration with a simplified version
    monkeypatch.setattr(
        pipeline,
        "TABLES",
        {
            "sample_table": {
                "watermark_type": "timestamp",
                "watermark_column": "created_date"
            }
        }
    )

    # Replace read_state() with a fake function returning empty state
    monkeypatch.setattr(pipeline, "read_state", lambda: {})

    # Replace extract_from_mysql() with a fake function returning a DataFrame
    monkeypatch.setattr(
        pipeline,
        "extract_from_mysql",
        lambda table_name, table_config, table_state: pd.DataFrame({
            "id": [1],
            "name": ["Test User"],
            "created_date": [pd.Timestamp("2026-03-10 10:00:00")]
        })
    )

    # Create fake functions for BigQuery load and state update
    load_mock = MagicMock()
    update_mock = MagicMock()
    write_mock = MagicMock()

    # Replace real functions with mocks
    monkeypatch.setattr(pipeline, "load_to_bigquery", load_mock)
    monkeypatch.setattr(pipeline, "update_state_for_table", update_mock)
    monkeypatch.setattr(pipeline, "write_state", write_mock)

    # Run the pipeline
    pipeline.run_pipeline()

    # Verify that the key steps of the pipeline were executed
    assert load_mock.called
    assert update_mock.called
    assert write_mock.called
