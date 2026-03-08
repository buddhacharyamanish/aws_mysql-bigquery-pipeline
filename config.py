MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "your_mysql_user",
    "password": "your_mysql_password",
    "database": "your_database"
}

GCP_PROJECT_ID = "your-gcp-project-id"
BQ_RAW_DATASET = "raw_mysql"
BQ_ANALYTICS_DATASET = "analytics"

TABLES = {
    "orders": {
        "watermark_column": "updated_at",
        "primary_key": "id"
    },
    "customers": {
        "watermark_column": "updated_at",
        "primary_key": "id"
    }
}
