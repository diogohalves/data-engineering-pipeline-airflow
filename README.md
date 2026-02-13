# End-to-End Data Engineering Pipeline with Airflow

Production-style data pipeline built using Python, PostgreSQL and Apache Airflow running on Docker.

## Architecture
- Extract data from CSV source
- Transform and validate data
- Incremental load into PostgreSQL
- Orchestrated with Apache Airflow
- Multi-task DAG with dependencies
- Sensors for file arrival
- XCom for task communication
- Idempotent upsert logic

## Tech Stack
- Python
- PostgreSQL
- Apache Airflow
- Docker
- SQL
- ETL pipelines

## Features
- Incremental data loading
- Retry and failure handling
- Transaction control
- Modular pipeline design
- Production-like structure

## Project Structure
