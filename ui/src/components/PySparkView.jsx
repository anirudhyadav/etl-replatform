import React, { useState } from "react";

export default function PySparkView({ files }) {
  const pyFiles = files?.filter(
    (f) => f.language === "python" && f.type === "pipeline"
  );

  if (!pyFiles || pyFiles.length === 0) {
    return (
      <div>
        <h2>
          <span className="dot" style={{ background: "#f97316" }} />
          PySpark Module
        </h2>
        <div className="empty-state">
          <div className="icon">🐍</div>
          <h3>No PySpark pipelines generated yet</h3>
          <p>
            Go to <strong>Orchestrator</strong> and select PySpark as target
            language to generate pipeline code.
          </p>
        </div>

        <div style={{ marginTop: 32 }}>
          <h3 style={{ fontSize: 16, marginBottom: 16 }}>
            Module Structure
          </h3>
          <div className="code-preview">
            <div className="code-content">{`pyspark_gen/
├── common/
│   ├── config_loader.py      ← YAML config + secret resolution
│   ├── connections.py        ← JDBC, CSV, Delta read/write
│   ├── transformations.py    ← Derived cols, lookups, dedup, splits
│   ├── validations.py        ← Schema, null, dup, ref integrity checks
│   ├── error_handling.py     ← Error row routing (SSIS Error Output)
│   ├── logging_utils.py      ← Structured pipeline logging
│   ├── audit.py              ← Execution audit trail
│   └── scd.py                ← SCD Type 1 & Type 2 Delta MERGE
│
├── templates/
│   ├── dimension_scd2.py.j2  ← Template: SCD2 dimension load
│   ├── dimension_scd1.py.j2  ← Template: SCD1 dimension load
│   ├── fact_load.py.j2       ← Template: Fact table load
│   ├── flat_file_ingest.py.j2← Template: CSV to Delta ingestion
│   └── generic.py.j2         ← Template: Blank pipeline skeleton
│
├── schemas/
│   ├── customer_schema.py    ← StructType definitions
│   └── sales_schema.py
│
└── pipelines/                ← Generated pipelines land here
    └── (your_pipeline.py)`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            SSIS → PySpark Mapping
          </h3>
          <div className="code-preview">
            <div className="code-content">{`SSIS Component              →  PySpark Equivalent
─────────────────────────────────────────────────────
OLE DB Source               →  get_jdbc_connection()
Flat File Source            →  read_csv()
Derived Column              →  apply_derived_columns()
Conditional Split           →  apply_conditional_split()
Lookup Transform            →  apply_lookup() + broadcast
Union All                   →  union_dataframes()
Data Conversion             →  apply_type_conversions()
Sort (Remove Duplicates)    →  deduplicate()
Error Output (Redirect)     →  route_errors()
SCD Type 1                  →  scd_type1_merge()
SCD Type 2                  →  scd_type2_merge()
OLE DB Destination          →  write_delta() / write_jdbc()
SSISDB Audit                →  AuditTracker`}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2>
        <span className="dot" style={{ background: "#f97316" }} />
        PySpark Module — Generated Pipeline
      </h2>
      {pyFiles.map((f, i) => (
        <div key={i} className="code-preview" style={{ marginBottom: 16 }}>
          <div className="code-tabs">
            <div className="code-tab active">🐍 {f.name}</div>
          </div>
          <div className="code-content">{f.content}</div>
        </div>
      ))}
    </div>
  );
}
