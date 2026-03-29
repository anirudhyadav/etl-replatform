"""
ETL Replatform — Flask API Backend (Orchestrator)

Routes:
  POST /api/generate   → Generate pipeline + optional Airflow DAG
  GET  /api/health     → Health check
  GET  /api/templates  → List available templates

Connects React UI to the 4 generator modules:
  - pyspark_gen
  - java_gen         (Java Spark — distributed, high-volume)
  - springbatch_gen  (Spring Batch — traditional Java ETL, < 5M rows)
  - airflow_gen
"""

import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify
from flask_cors import CORS

# Add parent dir to path so we can import sibling modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark_gen.generator import PySparkGenerator
from java_gen.generator import JavaGenerator
from springbatch_gen.generator import SpringBatchGenerator
from airflow_gen.generator import AirflowGenerator

app = Flask(__name__)
CORS(app)


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "etl-replatform"})


@app.route("/api/templates", methods=["GET"])
def list_templates():
    """Return available pipeline templates for both languages."""
    return jsonify({
        "pyspark": PySparkGenerator.list_templates(),
        "java": JavaGenerator.list_templates(),
        "springbatch": SpringBatchGenerator.list_templates(),
        "airflow": AirflowGenerator.list_templates(),
    })


@app.route("/api/generate", methods=["POST"])
def generate():
    """
    Generate pipeline code from wizard parameters.

    Expected JSON body:
    {
        "language": "pyspark" | "java" | "springbatch",
        "pipelineType": "dimension_scd2" | "dimension_scd1" | "fact_load" | "flat_file_ingest" | "generic",
        "pipelineName": "load_customer_dim",
        "ssisPackage": "Load_Customer_Dim.dtsx",
        "description": "Loads customer dimension with SCD2",
        "sourceType": "jdbc" | "csv" | "delta",
        "targetType": "delta" | "jdbc",
        "author": "Data Engineering",
        "jiraTicket": "ETL-1234",
        "environment": "dev" | "qa" | "prod",
        "generateAirflow": true | false
    }
    """
    try:
        params = request.get_json()
        if not params:
            return jsonify({"success": False, "error": "No JSON body provided"}), 400

        language = params.get("language")
        pipeline_type = params.get("pipelineType")
        pipeline_name = params.get("pipelineName", "my_pipeline")
        environment = params.get("environment", "dev")
        generate_airflow = params.get("generateAirflow", True)

        if language not in ("pyspark", "java", "springbatch"):
            return jsonify({"success": False, "error": f"Invalid language: {language}"}), 400

        if not pipeline_type:
            return jsonify({"success": False, "error": "pipelineType is required"}), 400

        # Build details dict shared by all generators
        details = {
            "pipeline_name": pipeline_name,
            "ssis_package": params.get("ssisPackage", "Unknown.dtsx"),
            "description": params.get("description", "Converted SSIS pipeline"),
            "source_type": params.get("sourceType", "jdbc"),
            "target_type": params.get("targetType", "delta"),
            "author": params.get("author", "Data Engineering"),
            "jira_ticket": params.get("jiraTicket", "N/A"),
        }

        files = []

        # ── Generate pipeline code ─────────────────────────
        if language == "pyspark":
            gen = PySparkGenerator()
            pipeline_file = gen.generate(pipeline_type, details, environment)
        elif language == "java":
            gen = JavaGenerator()
            pipeline_file = gen.generate(pipeline_type, details, environment)
        else:  # springbatch
            gen = SpringBatchGenerator()
            pipeline_file = gen.generate(pipeline_type, details, environment)

        files.append(pipeline_file)

        # ── Generate Airflow DAG (optional) ────────────────
        if generate_airflow:
            airflow_gen = AirflowGenerator()

            dag_file = airflow_gen.generate_dag(language, details, environment)
            files.append(dag_file)

            job_file = airflow_gen.generate_databricks_job(language, details, environment)
            files.append(job_file)

        return jsonify({"success": True, "files": files})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug)
