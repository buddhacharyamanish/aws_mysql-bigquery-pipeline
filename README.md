# MySQL to BigQuery Incremental Data Pipeline

## Overview

This project implements an end-to-end data pipeline that extracts data from MySQL, processes it, and loads it into Google BigQuery using an incremental loading strategy.

The pipeline is designed to reflect real-world data engineering practices, including watermark-based ingestion, schema evolution handling, modular design, and automated scheduling.

---

## Key Features

* Incremental data loading using watermark (timestamp and ID based)
* Automatic BigQuery table creation from MySQL schema
* Schema evolution support (automatic column addition)
* MySQL to BigQuery data type mapping
* Modular and configurable pipeline design
* Unit testing using pytest
* Cron-based scheduling for automation

---

## Architecture

MySQL (Source)
→ Python ETL Pipeline
→ BigQuery (Destination)

### Pipeline Flow

1. Read watermark state from a JSON file
2. Extract only new data from MySQL
3. Prepare and transform data
4. Ensure BigQuery table exists and schema is up to date
5. Load data into BigQuery
6. Update watermark state

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
├── tests/
│   └── test_pipeline.py
├── docs/
│   └── mysql_installation.txt
```

---

## Technologies Used

* Python
* MySQL
* Google BigQuery
* Pandas
* Pytest
* Linux / AWS EC2

---

## Execution Model

### How the Pipeline Runs

The pipeline runs as a **batch process** using a Python script:

```
python3 pipeline.py
```

It is scheduled using **cron** in a Linux environment (e.g., AWS EC2).

Example:

```
*/15 * * * * /path/to/python /path/to/pipeline.py >> logs/pipeline.log 2>&1
```

This runs the pipeline every **15 minutes**, processing only new data.

---

## Incremental Loading Strategy

The pipeline uses a watermark-based approach to process only new records.

### Timestamp-based ingestion

```
WHERE created_date > last_loaded_timestamp
```

### ID-based ingestion

```
WHERE id > last_loaded_id
```

Watermark values are stored in a state file (`state.json`), enabling the pipeline to resume from the last successful run.

---

## Schema Evolution Handling

The pipeline automatically detects new columns in MySQL and updates the BigQuery table schema without manual intervention.

---

## Design Choices and Trade-offs

### Python-Based Pipeline vs Managed Cloud Services

Cloud services such as Dataflow, Datastream, and Fivetran provide managed ingestion solutions.

This project uses a Python-based approach to:

* Maintain full control over transformation logic
* Avoid vendor lock-in
* Reduce cost for smaller workloads
* Provide transparency into pipeline behavior

However, managed services are better suited for large-scale, highly automated environments.

---

### Why Watermark Instead of CDC / Binlog?

This pipeline uses a watermark-based incremental approach instead of Change Data Capture (CDC).

#### Advantages

* Simple implementation
* No dependency on binlog or replication setup
* Low infrastructure overhead
* Easy to debug and maintain

#### Trade-offs

* Not real-time
* Does not capture deletes automatically
* Requires reliable watermark columns

---

## Data Volume and Scalability

### Supported Data Volume

This pipeline is designed for:

* Small to medium datasets
* Thousands to millions of rows per batch
* Moderate ingestion frequency (5–30 minutes)

### Performance Expectations

* 10K–100K rows → seconds to minutes
* 100K–1M rows → minutes
* > 1M rows → requires optimization

### Key Bottlenecks

* MySQL query performance
* Network transfer
* Pandas in-memory processing
* BigQuery load time

---

## Limitations

This architecture has practical limits:

* Batch-based (not real-time)
* Memory-bound due to Pandas
* Not ideal for very large-scale data (hundreds of millions to billions of rows)
* Limited handling of deletes and updates without additional logic

---

## When to Use This Approach

This pipeline is suitable for:

* Learning and prototyping
* Small to medium production workloads
* Periodic batch ingestion
* Cost-sensitive environments
* Systems without CDC access

---

## When to Use Other Architectures

For larger-scale or real-time systems, alternative approaches should be considered:

### High Volume / Real-Time Pipelines

* Change Data Capture (CDC)
* Tools: Debezium, Google Datastream

### Large-Scale Processing

* Distributed processing systems
* Tools: Apache Spark, Google Dataflow

### Complex Scheduling and Orchestration

* Workflow orchestration tools
* Tools: Apache Airflow, Prefect

---

## Future Work

This project represents a foundational pipeline design.

A follow-up project will demonstrate:

* CDC-based ingestion (binlog streaming)
* Real-time data pipelines
* Distributed processing architecture
* Advanced orchestration using Airflow

---

## Setup Instructions

### 1. Clone the repository

```
git clone <repository-url>
cd mysql_bigquery_pipeline
```

### 2. Create virtual environment

```
python3 -m venv datapuller
source datapuller/bin/activate
```

### 3. Install dependencies

```
pip install -r requirements.txt
```

### 4. Configure credentials

Update `config.py` with your MySQL and GCP configuration.

---

## Running the Pipeline

```
python3 pipeline.py
```

---

## Running Unit Tests

```
pytest -v
```

Tests cover:

* Data type mapping
* Watermark logic
* Query generation
* State handling
* Schema evolution
* Pipeline orchestration

---

## Notes

* Do not commit sensitive credentials
* Use `.gitignore` to exclude:

  * virtual environments
  * logs
  * state files
  * secrets

---

## Author

Manish Ratna Buddhacharya

