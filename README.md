[etl-pipeline-framework-README.txt](https://github.com/user-attachments/files/30128415/etl-pipeline-framework-README.txt)
# etl-pipeline-framework# etl-pipeline-framework

> Production-ready Python ETL framework for extracting, transforming, and loading data from APIs, CSVs, and databases into SQL warehouses.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-D71F00?style=for-the-badge)](https://sqlalchemy.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)]()

---

## Overview

A modular, configuration-driven ETL framework built for production data workflows. It separates the concerns of extraction, transformation, and loading into clean, independently testable layers — making it easy to swap data sources, apply custom transforms, and load into any SQL warehouse without rewriting pipeline logic.

Designed for businesses that need reliable, repeatable data movement between REST APIs, legacy CSVs, operational databases, and centralized warehouses.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   SOURCES   │────▶│  EXTRACTORS │────▶│ TRANSFORMERS │────▶│    LOADERS      │
│             │     │             │     │              │     │                 │
│  REST APIs  │     │ api_extract │     │   cleaner    │     │  sql_loader     │
│  CSV Files  │     │ csv_extract │     │  normalizer  │     │  csv_loader     │
│  Databases  │     │  db_extract │     │  aggregator  │     │                 │
└─────────────┘     └─────────────┘     └──────────────┘     └────────┬────────┘
                                                                       │
                                                              ┌────────▼────────┐
                                                              │   DATA WAREHOUSE │
                                                              │  PostgreSQL /    │
                                                              │  SQL Server /    │
                                                              │  BigQuery        │
                                                              └─────────────────┘
```

All steps are orchestrated via `pipeline.py` or an **Apache Airflow DAG** (`dags/etl_dag.py`).

---

## Features

- ✅ **Modular design** — extractors, transformers, and loaders are fully decoupled
- ✅ **Config-driven** — no hardcoded values; everything lives in `config/config.yaml`
- ✅ **Structured logging** — rotating log files with configurable levels via `logging.yaml`
- ✅ **Error handling** — retries, fallback paths, and graceful failure reporting
- ✅ **Airflow-ready** — drop-in DAG for scheduled orchestration
- ✅ **Unit tested** — pytest coverage for all three pipeline layers
- ✅ **Multi-source** — REST APIs, CSV files, and database connections out of the box
- ✅ **Multi-target** — load to PostgreSQL, SQL Server, SQLite, or output CSV

---

## Project Structure

```
etl-pipeline-framework/
├── README.md
├── requirements.txt
├── .env.example
├── config/
│   ├── config.yaml          # Source/target connections, pipeline settings
│   └── logging.yaml         # Log levels, handlers, rotation policy
├── src/
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── api_extractor.py     # REST API extraction with pagination + auth
│   │   ├── csv_extractor.py     # CSV/Excel ingestion with schema validation
│   │   └── db_extractor.py      # SQLAlchemy-based database extraction
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── cleaner.py           # Null handling, type casting, deduplication
│   │   ├── normalizer.py        # Column renaming, format standardization
│   │   └── aggregator.py        # Group-by aggregations, rollups
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── sql_loader.py        # Upsert / append / replace to SQL targets
│   │   └── csv_loader.py        # Output to CSV with timestamp partitioning
│   └── pipeline.py              # Main orchestrator — runs full E→T→L chain
├── dags/
│   └── etl_dag.py               # Apache Airflow DAG definition
├── tests/
│   ├── test_extractors.py
│   ├── test_transformers.py
│   └── test_loaders.py
├── notebooks/
│   └── pipeline_exploration.ipynb
└── docs/
    └── architecture.md
```

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/jeff-rotar/etl-pipeline-framework
cd etl-pipeline-framework

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure your environment
cp .env.example .env
# Edit .env with your DB credentials and API keys

# 5. Run the pipeline
python src/pipeline.py --config config/config.yaml

# 6. Run tests
pytest tests/ -v
```

---

## Configuration

Edit `config/config.yaml` to define your sources and targets:

| Field | Description | Example |
|---|---|---|
| `source.type` | Extractor type: `api`, `csv`, `database` | `api` |
| `source.url` | API endpoint or file path | `https://api.example.com/data` |
| `source.auth` | Auth method: `bearer`, `basic`, `none` | `bearer` |
| `target.type` | Loader type: `postgresql`, `sqlserver`, `csv` | `postgresql` |
| `target.table` | Destination table name | `sales_fact` |
| `target.strategy` | Load strategy: `append`, `replace`, `upsert` | `upsert` |
| `pipeline.batch_size` | Rows per batch | `1000` |
| `pipeline.retries` | Retry attempts on failure | `3` |

Environment variables (`.env`):

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=warehouse
DB_USER=etl_user
DB_PASSWORD=your_password
API_KEY=your_api_key
```

---

## Example Use Cases

| Scenario | Source | Target | Schedule |
|---|---|---|---|
| Daily sales sync | REST API (Salesforce / HubSpot) | PostgreSQL warehouse | Airflow @ 6 AM |
| Legacy CSV migration | Shared drive CSV exports | SQL Server staging tables | On-demand |
| Operational DB snapshot | MySQL production DB | Analytics PostgreSQL DB | Nightly |
| Third-party data feed | Paginated JSON API | BigQuery | Hourly |

---

## Running with Airflow

```bash
# Copy DAG to your Airflow dags folder
cp dags/etl_dag.py ~/airflow/dags/

# Trigger manually
airflow dags trigger etl_pipeline

# Or set schedule in etl_dag.py:
# schedule_interval='0 6 * * *'  (daily at 6 AM)
```

---

## License

MIT © Jeff Rotar | [jeffrotar@hotmail.com](mailto:jeffrotar@hotmail.com)

---

<div align="center">
  <sub>📍 Fargo, ND · Available for freelance data engineering projects</sub>
</div>
