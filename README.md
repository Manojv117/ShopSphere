# ShopSphere — E-Commerce Data Engineering Pipeline

An end-to-end data engineering project built using **Apache Spark, Databricks, Delta Lake, and Apache Airflow** to process and analyze Brazilian e-commerce data.

## Project Overview

ShopSphere implements a Medallion Architecture to transform raw e-commerce data into clean, analytics-ready datasets.

The pipeline uses Apache Airflow to orchestrate Databricks Jobs, executing ingestion, transformation, analytics, and data quality validation in sequence.

## Architecture

```text
Olist E-Commerce Dataset
          |
          v
   Bronze Layer
   Raw Data Ingestion
          |
          v
   Silver Layer
   Data Cleaning & Transformation
          |
          v
   Gold Layer
   Analytical Data Models
          |
          v
   Data Quality
   Validation Checks
```

## Technology Stack

- **Python**
- **PySpark**
- **Databricks Free Edition**
- **Delta Lake**
- **Apache Airflow 3**
- **Docker Compose**
- **PostgreSQL** (Airflow metadata database)
- **Git & GitHub**

## Repository Structure

```text
ShopSphere/
├── airflow/
│   └── dags/
│       └── shopsphere_databricks_dag.py
├── databricks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_analytics.py
│   └── 04_data_quality.py
├── docs/
├── screenshots/
├── .gitignore
└── README.md
```

## Pipeline Stages

### 1. Bronze — Ingestion
- Ingest raw Olist CSV datasets.
- Store data as Delta tables.
- Add ingestion metadata.

### 2. Silver — Transformation
- Clean and standardize data.
- Handle null values and duplicates.
- Apply appropriate data types.
- Store transformed Delta tables.

### 3. Gold — Analytics
- Build analytical data models.
- Calculate daily revenue.
- Analyze product performance.
- Generate customer summaries.

### 4. Data Quality
- Validate important keys and fields.
- Check duplicates and null values.
- Reconcile Gold revenue against the expected value.

## Orchestration

Apache Airflow orchestrates the Databricks Jobs through the Databricks Jobs API.

Execution order:

`Bronze → Silver → Gold → Data Quality`

Features:
- Task dependencies
- Automatic retries
- Daily scheduling
- Execution monitoring through Airflow logs
- Pipeline status tracking

## Dataset

The project uses the Brazilian E-Commerce Public Dataset by Olist.

Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Execution

1. Import the notebooks into Databricks.
2. Configure the required Databricks Jobs.
3. Configure the `databricks_default` Airflow connection.
4. Update the Job IDs in the Airflow DAG.
5. Trigger the DAG or enable its daily schedule.

## Current Status

- [x] Bronze ingestion
- [x] Silver transformations
- [x] Gold analytics
- [x] Data quality validation
- [x] Airflow–Databricks integration
- [x] Sequential pipeline orchestration
- [x] Retry configuration
- [x] Daily scheduling
- [x] Log-based pipeline status monitoring

## Future Improvements

- External failure notifications
- Advanced pipeline observability
- Incremental data processing
- CI/CD automation

---

**Built as a hands-on Data Engineering portfolio project.**
