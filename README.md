# etl-replatform

Decommission SSIS. Replatform ETL pipelines onto modern Java, PySpark, or Airflow.

## Architecture

```
etl-replatform/
│
├── ui/                  ← React (Vite) — Orchestrator UI
│   Pick source SSIS package → Choose target language → Preview → Export
│
├── orchestrator/        ← Flask API — Connects UI to generators
│   POST /generate       → Routes to the right generator
│   GET  /config/:env    → Returns environment config
│
├── pyspark_gen/         ← PySpark code generator
│   Templates for: SCD2 dim, SCD1 dim, fact load, flat file, generic
│
├── java_gen/            ← Java Spark code generator
│   Templates for: SCD2 dim, SCD1 dim, fact load, flat file, generic
│
├── airflow_gen/         ← Airflow DAG generator
│   Generates DAGs for either PySpark or Java pipelines
│
└── shared/              ← Config + SQL shared across all modules
    config/dev.yaml, qa.yaml, prod.yaml
    sql/audit_tables.sql
```

## Quick Start

```bash
# Backend
cd orchestrator && pip install -r requirements.txt && python app.py

# Frontend
cd ui && npm install && npm run dev

# Open http://localhost:5173
```
