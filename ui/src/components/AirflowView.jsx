import React, { useState } from "react";

export default function AirflowView({ files }) {
  const airflowFiles = files?.filter((f) => f.type === "orchestration");
  const [activeTab, setActiveTab] = useState(0);

  if (!airflowFiles || airflowFiles.length === 0) {
    return (
      <div>
        <h2>
          <span className="dot" style={{ background: "#22c55e" }} />
          Airflow DAG Module
        </h2>
        <div className="empty-state">
          <div className="icon">🌊</div>
          <h3>No Airflow DAGs generated yet</h3>
          <p>
            Go to <strong>Orchestrator</strong>, enable "Generate Airflow DAG"
            and run the conversion.
          </p>
        </div>

        <div style={{ marginTop: 32 }}>
          <h3 style={{ fontSize: 16, marginBottom: 16 }}>Module Structure</h3>
          <div className="code-preview">
            <div className="code-content">{`airflow_gen/
├── templates/
│   ├── dag_pyspark.py.j2       ← DAG template for PySpark notebooks
│   ├── dag_java.py.j2          ← DAG template for Java JAR submissions
│   └── job_databricks.json.j2  ← Databricks Workflow JSON template
│
└── dags/                        ← Generated DAGs land here
    ├── dag_load_customer_dim.py
    └── job_load_customer_dim.json`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            SSIS → Airflow Mapping
          </h3>
          <div className="code-preview">
            <div className="code-content">{`SSIS Concept                →  Airflow Equivalent
─────────────────────────────────────────────────────
SQL Server Agent Job        →  Airflow DAG (scheduled)
SSIS Catalog Execution      →  DatabricksSubmitRunOperator
Package Parameters          →  base_parameters / args
Precedence Constraints      →  >> task dependencies
Event Handlers (OnError)    →  trigger_rule=ONE_FAILED
Email Task on Failure       →  EmailOperator
Agent Job Schedule          →  schedule_interval / cron

Supports both:
  • PySpark pipelines  →  notebook_task in DAG
  • Java pipelines     →  spark_jar_task in DAG`}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2>
        <span className="dot" style={{ background: "#22c55e" }} />
        Airflow DAG Module — Generated
      </h2>
      <div className="code-preview">
        <div className="code-tabs">
          {airflowFiles.map((f, i) => (
            <div
              key={i}
              className={`code-tab ${activeTab === i ? "active" : ""}`}
              onClick={() => setActiveTab(i)}
            >
              🌊 {f.name}
            </div>
          ))}
        </div>
        <div className="code-content">
          {airflowFiles[activeTab]?.content}
        </div>
      </div>
    </div>
  );
}
