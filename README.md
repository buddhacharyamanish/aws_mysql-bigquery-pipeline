# MySQL to BigQuery Incremental Data Pipeline

## Overview

This project implements an end-to-end data pipeline that extracts data from MySQL, processes it, and loads it into Google BigQuery using an incremental loading approach.

The pipeline is designed to reflect practical data engineering patterns such as:

* incremental ingestion using watermark logic
* automatic schema evolution
* simple batch execution using cron
* minimal and controlled infrastructure

---

## Key Features

* Incremental data loading (timestamp and ID based)
* Automatic schema evolution (new columns flow into BigQuery)
* MySQL to BigQuery data type mapping
* Lightweight Python-based pipeline
* Cron-based scheduling
* Unit testing using pytest

---

## Architecture

MySQL (AWS EC2) → Python Pipeline → BigQuery
↓
state.json

### Flow

1. Read last processed state
2. Extract only new records from MySQL
3. Detect schema changes
4. Update BigQuery schema if required
5. Load data into BigQuery
6. Update state

---

## Project Structure

```
mysql_bigquery_pipeline/
├── README.md
├── config.py
├── pipeline.py
├── generate_dummy_data.py
├── scheduler.py
├── requirements.txt
├── state.example.json
├── tests/
│   └── test_pipeline.py
├── docs/
│   └── mysql_installation.txt
```

---

## Incremental Loading Strategy

### Timestamp-based

```
WHERE created_date > last_loaded_timestamp
```

### ID-based

```
WHERE id > last_loaded_id
```

This ensures only new data is processed on each run.

---

## Automatic Schema Evolution

The pipeline automatically handles schema changes.

If a new column is added in MySQL:

* the pipeline reads the updated schema
* compares it with BigQuery
* adds missing columns in BigQuery
* continues execution without failure

This avoids manual intervention and keeps systems in sync.

---

## Execution Model

The pipeline runs as a batch job.

### Manual execution

```
python3 pipeline.py
```

### Scheduled execution (cron)

```
*/15 * * * * python3 pipeline.py >> logs/pipeline.log 2>&1
```

Runs every 15 minutes and processes only new data.

---

## Data Volume and Scalability

### Suitable for

* small to medium datasets
* thousands to millions of rows per batch
* batch processing workloads

### Performance expectation

* 10K–100K rows → seconds to minutes
* 100K–1M rows → minutes
* beyond that → requires optimization

---

## Limitations

* not real-time
* does not capture deletes automatically
* depends on reliable watermark column
* memory-bound due to Pandas
* not ideal for very large-scale data

---

## When to Use This Approach

* batch ingestion pipelines
* cost-conscious environments
* systems without CDC access
* moderate data volume

---

## When to Use Other Architectures

For larger or real-time systems:

* CDC pipelines (Debezium, Datastream)
* distributed processing (Spark, Dataflow)
* orchestration tools (Airflow, Prefect)

---

## Cost Consideration

There is no separate ingestion platform involved.

Main costs include:

* compute environment running the pipeline (e.g., EC2 instance)
* BigQuery storage and query usage

This makes the setup simple and cost-efficient for moderate workloads.

---

## Setup Instructions

### Prerequisites

* Python 3.10+
* MySQL running
* Google Cloud account with BigQuery enabled
* gcloud CLI installed

---

### 1. Clone repository

```
git clone <your-repo-url>
cd mysql_bigquery_pipeline
```

---

### 2. Create virtual environment

```
python3 -m venv datapuller
source datapuller/bin/activate
```

---

### 3. Install dependencies

```
pip install -r requirements.txt
```

---

### 4. Setup database

Create your MySQL database and tables.

Refer to:

```
docs/mysql_installation.txt
```

(Optional) Generate sample data:

```
python3 generate_dummy_data.py
```

---

### 5. Configure pipeline

Update `config.py`:

```python
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database"
}

GCP_PROJECT_ID = "your_project_id"
BQ_DATASET = "raw_data"
```

---

### 6. Initialize state file

```
cp state.example.json state.json
```

---

### 7. Authenticate with GCP

```
gcloud auth application-default login
```

---

### 8. Run pipeline

```
python3 pipeline.py
```

---

### 9. Verify output

* check logs in `logs/`
* verify tables in BigQuery
* confirm incremental loading

---

## Running Tests

```
pytest -v
```

---

## Notes

* do not commit credentials
* ignore runtime files (`state.json`, logs, virtual env)

---

## Future Work

* CDC-based ingestion
* real-time pipelines
* distributed processing
* orchestration with Airflow

---

## Author

Manish Ratna Buddhacharya
