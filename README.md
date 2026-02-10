# AI-Powered BI Platform

## Overview

The AI-Powered BI Platform is a comprehensive data engineering and machine learning solution designed to automate the lifecycle of business data. From raw ingestion to predictive analytics, the platform leverages professional-grade orchestration and data warehousing techniques to provide actionable insights.

The ecosystem is built around an automated pipeline that moves data through three primary layers: Bronze (Raw), Silver (Clean), and Gold (Warehouse), culminating in a Machine Learning layer for predictive modeling.

## System Architecture

The data pipeline follows an "A to Z" flow orchestrated by Apache Airflow:

1.  **Ingestion Layer (Bronze)**: Scripts extract data from primary sources (CSV, API, etc.) and load it into the raw database schema. This layer preserves the original data format for traceability.
2.  **Transformation Layer (Silver)**: Data is sanitized, validated, and deduplicated. It is then moved to the `bi_warehouse_clean` database, ensuring a high-quality data source for downstream processing.
3.  **Warehouse Layer (Gold)**: The cleaned data is transformed into a Star Schema (Fact and Dimension tables) within the `bi_warehouse_warehouse` database. This structure is optimized for high-performance BI queries and reporting.
4.  **Machine Learning Layer**: A dedicated pipeline fetches data from the Gold layer to train and evaluate predictive models (e.g., Sales Forecasting). The models and preprocessors are serialized for production deployment.

## Core Technologies

-   **Orchestration**: Apache Airflow
-   **Database**: PostgreSQL (Structured as Raw, Clean, and Warehouse layers)
-   **Infrastructure**: Docker and Docker Compose
-   **Data Processing**: Python, Pandas, SQLAlchemy
-   **Machine Learning**: Scikit-learn

## Directory Structure

```text
AI-Powered-BI/
├── data_ingestion/           # Airflow DAGs and raw data extraction scripts
├── data_transform/           # Logic for data cleaning and Silver layer loading
├── data_warehouse/           # Star schema implementation (Fact and Dims)
├── machine_learning_pipeline/# ML source code, model training, and artifacts
├── source/                   # Raw data sources (e.g., CSV files)
├── tests/                    # Unit and integration tests
├── docker-compose.yml        # Multi-container orchestration (Airflow, Postgres)
└── .env                      # Environment configuration and secrets
```

## Setup and Installation

### Prerequisites

-   Docker and Docker Compose installed on the host machine.
-   Python 3.10+ (for local development).

### Deployment Steps

1.  **Configure Environment**:
    Review and update the `.env` file in the root directory. Ensure database credentials and Airflow configurations are correctly set.

2.  **Launch Platform**:
    Execute the following command to build and start all services:
    ```bash
    docker-compose up --build
    ```

3.  **Access Airflow UI**:
    Once the containers are healthy, access the Airflow webserver at `http://localhost:8080`.
    Default Credentials: `airflow` / `airflow`.

4.  **Database Connection**:
    The platform exposes PostgreSQL on the following ports for local tool access:
    -   Raw/Airflow Metadata: `5433`
    -   Clean/Warehouse: `5434`

## Alerting and Notifications

The platform includes an automated alerting system to notify administrators if a pipeline fails.

### Email Alerts (Gmail)
The system is configured to send real-time email notifications upon task failure. This ensures that any breaks in the Bronze, Silver, or Gold layers are addressed immediately.

- **Configuration**: SMTP settings are managed in the root `.env` file under the `AIRFLOW__SMTP__` variables.
- **Recipients**: The recipient email address is defined within each DAG's `default_args` (e.g., `ahmedk32410@gmail.com`).

To update the notification recipient, modify the `email` field in the relevant DAG file located in `data_ingestion/dags/`.

## Component Details

### Data Ingestion
Located in `data_ingestion/`, this component manages the initial entry of data. Airflow DAGs schedule the execution of scripts that pull data into the Bronze layer of the Postgres instance.

### Data Transformation and Warehousing
The `data_transform/` and `data_warehouse/` modules handle the movement of data from Silver to Gold. The `star_schema.py` script is responsible for populating the professional BI schema, ensuring data integrity and query efficiency.

### Machine Learning Pipeline
The `machine_learning_pipeline/` fetches data from the Gold layer via a robust data fetcher. It includes a fallback mechanism for local development, allowing it to connect to either the internal Docker network or a local host mapping.

#### Real-Time Inference API
The platform now exposes a Real-Time Inference API via `ml-api` service running on port `8000`.
- **Health Check**: `GET /health`
- **Prediction**: `POST /predict`
    - Payload: JSON object with sales features.
    - Response: JSON object with `overall_sales_prediction` and `product_sales_prediction`.

## Monitoring


The entire pipeline is monitored through the Airflow UI. Automated health checks ensure that database services are ready before processing begins. Logs for each step are captured in the project's `logs/` directory for troubleshooting.
