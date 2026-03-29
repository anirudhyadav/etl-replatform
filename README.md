# etl-replatform

Decommission SSIS. Replatform ETL pipelines to **PySpark**, **Spring Batch**, or **Java Spark** — with a React UI wizard and Airflow DAG generation.

## Why

SSIS is end-of-life for modern cloud data platforms. This framework provides a structured, repeatable way to convert SSIS packages into production-ready code on the right target stack for each pipeline's needs.

| Target | Best For | SSIS Similarity |
|--------|----------|-----------------|
| **PySpark** | Any volume, data science teams, rapid development | Medium |
| **Spring Batch** | < 5M rows, Java teams, complex business logic, no cluster needed | **Highest** — same Reader/Processor/Writer model |
| **Java Spark** | 10M+ rows, distributed processing, JVM performance | Medium |

## Architecture

```
etl-replatform/
│
├── ui/                    ← React (Vite) front-end
│   5 module cards: Orchestrator, PySpark, Spring Batch, Java Spark, Airflow
│   4-step wizard: Language → Pipeline Type → Details → Preview & Download
│
├── orchestrator/          ← Flask API backend
│   app.py                    POST /api/generate → routes to correct generator
│                             GET  /api/health   → health check
│                             GET  /api/templates → list all templates
│
├── pyspark_gen/           ← PySpark code generator
│   generator.py              5 templates → .py pipeline files
│
├── springbatch_gen/       ← Spring Batch code generator
│   generator.py              5 templates → JobConfig.java files
│
├── java_gen/              ← Java Spark code generator
│   generator.py              5 templates → .java pipeline files
│
├── airflow_gen/           ← Airflow DAG + Databricks Workflow generator
│   generator.py              Supports all 3 languages
│
├── Makefile               ← make dev / make api / make ui
└── requirements.txt       ← Python dependencies (Flask)
```

## Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+

### Install

```bash
# Clone
git clone https://github.com/anirudhyadav/etl-replatform.git
cd etl-replatform

# Python backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# React frontend
cd ui && npm install && cd ..
```

### Run

Open **two terminals**:

```bash
# Terminal 1 — Flask API (port 5001)
source .venv/bin/activate
make api

# Terminal 2 — React UI (port 5173)
make ui
```

Open **http://localhost:5173** in your browser.

## How It Works

### 1. Orchestrator (Start Here)

Click the **Orchestrator** card → 4-step wizard:

1. **Pick language** — PySpark, Spring Batch, or Java Spark
2. **Pick pipeline type** — SCD2 Dimension, SCD1 Dimension, Fact Load, Flat File Ingest, or Generic
3. **Fill details** — pipeline name, SSIS package name, author, environment, etc.
4. **Preview & Download** — view generated code, download individual files or all at once

### 2. Generated Output

Each conversion produces:

| File | Description |
|------|-------------|
| `load_customer_dim.py` | PySpark pipeline (or `.java` for Java/Spring Batch) |
| `dag_load_customer_dim.py` | Airflow DAG with Databricks operator |
| `job_load_customer_dim.json` | Databricks Workflow JSON config |

### 3. Module Views

Click any module card (PySpark, Spring Batch, Java Spark, Airflow) to see:
- Generated code for that module
- SSIS → Target mapping reference table
- Module structure and build/run instructions

## SSIS Component Mapping

### PySpark

| SSIS Component | PySpark Equivalent |
|---|---|
| OLE DB Source | `get_jdbc_connection()` |
| Flat File Source | `read_csv()` |
| Derived Column | `apply_derived_columns()` |
| Lookup Transform | `apply_lookup()` + broadcast |
| Conditional Split | `apply_conditional_split()` |
| Sort (Remove Dups) | `deduplicate()` |
| Error Output | `route_errors()` |
| SCD Type 1 | `scd_type1_merge()` (Delta MERGE) |
| SCD Type 2 | `scd_type2_merge()` (Delta MERGE) |
| OLE DB Destination | `write_delta()` / `write_jdbc()` |
| SSISDB Audit | `AuditTracker` |

### Spring Batch

| SSIS Component | Spring Batch Equivalent |
|---|---|
| SSIS Package | `@Bean Job` (JobBuilder) |
| Data Flow Task | `@Bean Step` (chunk-oriented) |
| OLE DB Source | `JdbcPagingItemReader` |
| Flat File Source | `FlatFileItemReader` |
| Derived Column + Lookup | `ItemProcessor` |
| Conditional Split | `ClassifierCompositeItemWriter` |
| Error Output (Redirect) | `SkipPolicy` + error ItemWriter |
| SCD Type 1 | MERGE SQL in `JdbcBatchItemWriter` |
| SCD Type 2 | `Scd2Processor` (close/open rows) |
| OLE DB Destination | `JdbcBatchItemWriter` |
| SSISDB Audit | `JobExecutionListener` |
| Checkpoint/Restart | Built-in `ExecutionContext` |

### Java Spark

| SSIS Component | Java Spark Equivalent |
|---|---|
| OLE DB Source | `ConnectionFactory.readJdbc()` |
| Flat File Source | `ConnectionFactory.readCsv()` |
| Derived Column | `TransformationUtils.applyDerivedColumns()` |
| Lookup Transform | `TransformationUtils.applyLookup()` |
| Sort (Remove Dups) | `TransformationUtils.deduplicate()` |
| Error Output | `ErrorHandler.routeErrors()` |
| SCD Type 1 | `ScdMerge.type1Merge()` |
| SCD Type 2 | `ScdMerge.type2Merge()` |
| OLE DB Destination | `ConnectionFactory.writeDelta()` |
| SSISDB Audit | `AuditTracker` |

### Airflow (Scheduling)

| SSIS Concept | Airflow Equivalent |
|---|---|
| SQL Server Agent Job | Airflow DAG (scheduled) |
| SSIS Catalog Execution | `DatabricksSubmitRunOperator` |
| Package Parameters | `base_parameters` / `args` |
| Precedence Constraints | `>>` task dependencies |
| Event Handlers (OnError) | `trigger_rule=ONE_FAILED` |
| Agent Job Schedule | `schedule_interval` / cron |

## Pipeline Templates

Each language supports 5 pipeline types:

| Template | Description | SSIS Pattern |
|----------|-------------|--------------|
| **Dimension SCD2** | History tracking with effective/end dates | SCD Transform → Historical Attribute |
| **Dimension SCD1** | Overwrite in place, no history | SCD Transform → Changing Attribute |
| **Fact Load** | Join sources, lookup surrogate keys, append | Merge Join → Lookup → OLE DB Dest |
| **Flat File Ingest** | CSV/delimited file to database/Delta | Flat File Source → Data Conversion → Dest |
| **Generic** | Blank skeleton with all framework hooks | Empty Data Flow Task |

## API Reference

```
GET  /api/health              → { "status": "ok" }
GET  /api/templates           → { "pyspark": [...], "java": [...], "springbatch": [...] }
POST /api/generate            → { "success": true, "files": [...] }
```

### POST /api/generate

```json
{
  "language": "pyspark | java | springbatch",
  "pipelineType": "dimension_scd2 | dimension_scd1 | fact_load | flat_file_ingest | generic",
  "pipelineName": "load_customer_dim",
  "ssisPackage": "Load_Customer_Dim.dtsx",
  "description": "Customer dimension with SCD2 tracking",
  "sourceType": "jdbc | csv | delta",
  "targetType": "delta | jdbc",
  "author": "Data Engineering",
  "jiraTicket": "ETL-1234",
  "environment": "dev | qa | prod",
  "generateAirflow": true
}
```

## Choosing the Right Target

```
                    ┌─────────────────┐
                    │  SSIS Package    │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  How many rows?  │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        < 5M rows      Any volume      10M+ rows
        Java team      Python team     JVM required
              │              │              │
        ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
        │  Spring    │ │  PySpark  │ │   Java    │
        │  Batch     │ │           │ │   Spark   │
        └───────────┘ └───────────┘ └───────────┘
```

## License

MIT
