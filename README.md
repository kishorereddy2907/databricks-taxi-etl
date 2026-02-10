# Databricks Taxi ETL

This project implements an ETL pipeline for NYC Taxi data on Databricks.

## Project Structure

```
databricks-taxi-etl/
│
├── notebooks/          # Databricks notebooks for ETL layers
│   ├── bronze_ingest.py
│   ├── silver_transform.py
│   ├── gold_bi.py
│   └── gold_ml.py
│
├── utils/              # Shared utility modules
│   ├── schema.py
│   └── validations.py
│
├── configs/            # Environment configurations
│   ├── dev.yml
│   └── prod.yml
│
└── README.md           # Project documentation
```

## Setup

1. Configure environment variables.
2. Deploy notebooks to Databricks workspace.
3. Run `bronze_ingest.py` to start ingestion.