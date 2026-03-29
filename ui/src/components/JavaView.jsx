import React from "react";

export default function JavaView({ files }) {
  const javaFiles = files?.filter(
    (f) => f.language === "java" && f.type === "pipeline"
  );

  if (!javaFiles || javaFiles.length === 0) {
    return (
      <div>
        <h2>
          <span className="dot" style={{ background: "#a855f7" }} />
          Java Spark Module
        </h2>
        <div className="empty-state">
          <div className="icon">☕</div>
          <h3>No Java pipelines generated yet</h3>
          <p>
            Go to <strong>Orchestrator</strong> and select Java Spark as target
            language to generate pipeline code.
          </p>
        </div>

        <div style={{ marginTop: 32 }}>
          <h3 style={{ fontSize: 16, marginBottom: 16 }}>Module Structure</h3>
          <div className="code-preview">
            <div className="code-content">{`java_gen/
├── pom.xml                                ← Maven build (fat JAR)
│
├── common/
│   ├── ConfigLoader.java      ← YAML config + secret resolution
│   ├── ConnectionFactory.java ← JDBC, CSV, Delta read/write
│   ├── TransformationUtils.java ← Derived cols, lookups, dedup
│   ├── ValidationUtils.java   ← Schema, null, dup checks
│   ├── ErrorHandler.java      ← Error row routing
│   ├── AuditTracker.java      ← Execution audit trail
│   └── ScdMerge.java          ← SCD Type 1 & Type 2 Delta MERGE
│
├── templates/
│   ├── DimensionScd2.java.j2  ← Template: SCD2 dimension
│   ├── DimensionScd1.java.j2  ← Template: SCD1 dimension
│   ├── FactLoad.java.j2       ← Template: Fact table
│   ├── FlatFileIngest.java.j2 ← Template: CSV ingestion
│   └── Generic.java.j2        ← Template: Blank skeleton
│
├── schemas/
│   ├── CustomerSchema.java
│   └── SalesSchema.java
│
└── pipelines/                 ← Generated pipelines land here
    └── (YourPipeline.java)`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            SSIS → Java Spark Mapping
          </h3>
          <div className="code-preview">
            <div className="code-content">{`SSIS Component              →  Java Spark Equivalent
─────────────────────────────────────────────────────────
OLE DB Source               →  ConnectionFactory.readJdbc()
Flat File Source            →  ConnectionFactory.readCsv()
Derived Column              →  TransformationUtils.applyDerivedColumns()
Conditional Split           →  TransformationUtils.applyConditionalSplit()
Lookup Transform            →  TransformationUtils.applyLookup()
Union All                   →  TransformationUtils.unionDataframes()
Data Conversion             →  TransformationUtils.applyTypeConversions()
Sort (Remove Duplicates)    →  TransformationUtils.deduplicate()
Error Output (Redirect)     →  ErrorHandler.routeErrors()
SCD Type 1                  →  ScdMerge.type1Merge()
SCD Type 2                  →  ScdMerge.type2Merge()
OLE DB Destination          →  ConnectionFactory.writeDelta()
SSISDB Audit                →  AuditTracker`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            Build & Run
          </h3>
          <div className="code-preview">
            <div className="code-content">{`# Build fat JAR
cd java_gen && mvn clean package

# Submit to Spark
spark-submit \\
  --class com.etl.pipelines.YourPipeline \\
  --master yarn \\
  target/etl-replatform-1.0.jar prod

# Or deploy to Databricks
databricks fs cp target/etl-replatform-1.0.jar dbfs:/jars/`}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2>
        <span className="dot" style={{ background: "#a855f7" }} />
        Java Spark Module — Generated Pipeline
      </h2>
      {javaFiles.map((f, i) => (
        <div key={i} className="code-preview" style={{ marginBottom: 16 }}>
          <div className="code-tabs">
            <div className="code-tab active">☕ {f.name}</div>
          </div>
          <div className="code-content">{f.content}</div>
        </div>
      ))}
    </div>
  );
}
