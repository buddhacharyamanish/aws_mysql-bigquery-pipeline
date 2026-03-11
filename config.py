MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "sampleuser",
    "password": "Sample@123",
    "database": "sampledb"
}

GCP_PROJECT_ID = "data-engineering-v1-489004"
BQ_DATASET = "raw_data"

STATE_FILE = "/home/ec2-user/mysql_bigquery_pipeline/state.json"

TABLES = {
    "sample_table": {
        "watermark_type": "timestamp",
        "watermark_column": "created_date",
        "columns": ["id", "name", "created_date"]
    },
    "sample_table2": {
        "watermark_type": "id",
        "watermark_column": "id",
        "columns": ["id", "email", "phoneno"]
    }
}