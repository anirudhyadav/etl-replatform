import React from "react";

export default function SpringBatchView({ files }) {
  const sbFiles = files?.filter(
    (f) => f.language === "java" && f.type === "pipeline" && f.name.includes("JobConfig")
  );

  if (!sbFiles || sbFiles.length === 0) {
    return (
      <div>
        <h2>
          <span className="dot" style={{ background: "#10b981" }} />
          Spring Batch Module
        </h2>
        <div className="empty-state">
          <div className="icon">🍃</div>
          <h3>No Spring Batch jobs generated yet</h3>
          <p>
            Go to <strong>Orchestrator</strong> and select Spring Batch as target
            language to generate pipeline code.
          </p>
        </div>

        <div style={{ marginTop: 32 }}>
          <h3 style={{ fontSize: 16, marginBottom: 16 }}>
            Why Spring Batch?
          </h3>
          <div className="code-preview">
            <div className="code-content">{`Spring Batch is the NATURAL Java replacement for SSIS.
Same mental model, same concepts:

  SSIS Package             =  Spring Batch Job
  SSIS Data Flow Task      =  Spring Batch Step
  OLE DB Source             =  ItemReader (JdbcPagingItemReader)
  Derived Column + Lookup   =  ItemProcessor
  OLE DB Destination        =  ItemWriter (JdbcBatchItemWriter)
  Error Output (Redirect)   =  SkipPolicy + error ItemWriter
  SSISDB Catalog Reports    =  JobExecutionListener + StepExecutionListener
  SQL Agent Schedule        =  Spring Scheduler / Airflow

Best for:
  ✓ < 5M rows per pipeline
  ✓ Complex business logic in transformations
  ✓ Existing Java/Spring teams
  ✓ No Spark cluster needed — runs on any JVM
  ✓ Full restart/retry built-in (like SSIS checkpoints)`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            Module Structure
          </h3>
          <div className="code-preview">
            <div className="code-content">{`springbatch_gen/
├── generator.py                     ← Code generator (5 templates)
│
└── [Generated output structure]:
    └── com/etl/batch/
        ├── jobs/
        │   ├── LoadCustomerDimJobConfig.java    ← @Configuration + @Bean Job/Step
        │   └── LoadFactSalesJobConfig.java
        │
        ├── processors/
        │   ├── Scd2Processor.java       ← SCD Type 2 logic (close/open rows)
        │   ├── Scd1Processor.java       ← SCD Type 1 logic (overwrite)
        │   └── LookupProcessor.java     ← Surrogate key lookup via JDBC
        │
        ├── listeners/
        │   ├── AuditJobListener.java    ← beforeJob() / afterJob() audit logging
        │   └── AuditStepListener.java   ← read/write/skip counts per step
        │
        ├── common/
        │   └── DataSourceConfig.java    ← source + target DataSource beans
        │
        └── application-{dev|qa|prod}.yml  ← Spring profiles per environment`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            SSIS → Spring Batch Mapping
          </h3>
          <div className="code-preview">
            <div className="code-content">{`SSIS Component              →  Spring Batch Equivalent
───────────────────────────────────────────────────────────────
SSIS Package               →  @Bean Job (JobBuilder)
Data Flow Task             →  @Bean Step (StepBuilder) chunk-oriented
OLE DB Source              →  JdbcPagingItemReader (paginated, restartable)
Flat File Source           →  FlatFileItemReader (CSV, fixed-width)
Derived Column             →  ItemProcessor (transform)
Lookup Transform           →  ItemProcessor (JdbcTemplate lookup)
Conditional Split          →  ClassifierCompositeItemWriter
Data Conversion            →  ItemProcessor (type casting)
Sort (Remove Dups)         →  SQL ORDER BY in reader query
Error Output (Redirect)    →  SkipPolicy + SkipListener → error table
Error Output (Fail)        →  noSkip() → stops on first error
SCD Type 1                 →  MERGE SQL in JdbcBatchItemWriter
SCD Type 2                 →  Scd2Processor (close + insert new version)
OLE DB Destination         →  JdbcBatchItemWriter (batch INSERT/MERGE)
SSISDB Execution Reports   →  JobExecutionListener (start/end/fail audit)
Row Count Tracking         →  StepExecution.getReadCount() / getWriteCount()
Checkpoint/Restart         →  Built-in! ExecutionContext state is persisted
Package Parameters         →  Spring @Value + application.yml profiles`}</div>
          </div>

          <h3 style={{ fontSize: 16, marginTop: 24, marginBottom: 16 }}>
            Build & Run
          </h3>
          <div className="code-preview">
            <div className="code-content">{`# Build
mvn clean package -pl springbatch-app

# Run with Spring profile
java -jar target/etl-replatform-batch-1.0.jar \\
  --spring.profiles.active=prod

# Or schedule via Airflow
# → Uses BashOperator instead of DatabricksSubmitRunOperator

# Or schedule via Spring Scheduler (cron)
@Scheduled(cron = "0 0 2 * * *")
public void runDailyLoad() {
    jobLauncher.run(loadCustomerDimJob, params);
}`}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <h2>
        <span className="dot" style={{ background: "#10b981" }} />
        Spring Batch Module — Generated Job
      </h2>
      {sbFiles.map((f, i) => (
        <div key={i} className="code-preview" style={{ marginBottom: 16 }}>
          <div className="code-tabs">
            <div className="code-tab active">🍃 {f.name}</div>
          </div>
          <div className="code-content">{f.content}</div>
        </div>
      ))}
    </div>
  );
}
