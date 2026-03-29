import React, { useState } from "react";

const PIPELINE_TYPES = [
  { id: "dimension_scd2", name: "Dimension — SCD Type 2", desc: "History tracking with effective dates" },
  { id: "dimension_scd1", name: "Dimension — SCD Type 1", desc: "Overwrite in place, no history" },
  { id: "fact_load", name: "Fact Table Load", desc: "Join sources, lookup keys, append to fact" },
  { id: "flat_file_ingest", name: "Flat File Ingestion", desc: "CSV/delimited to Delta Lake" },
  { id: "generic", name: "Generic Pipeline", desc: "Blank template with all framework hooks" },
];

const INITIAL_FORM = {
  pipelineName: "",
  ssisPackage: "",
  description: "",
  sourceType: "jdbc",
  targetType: "delta",
  author: "",
  jiraTicket: "",
  environment: "dev",
  generateAirflow: true,
};

export default function OrchestratorWizard({ onGenerate }) {
  const [step, setStep] = useState(0); // 0=language, 1=type, 2=details, 3=preview
  const [language, setLanguage] = useState(null);
  const [pipelineType, setPipelineType] = useState(null);
  const [form, setForm] = useState(INITIAL_FORM);
  const [files, setFiles] = useState(null);
  const [loading, setLoading] = useState(false);

  const updateForm = (field, value) =>
    setForm((prev) => ({ ...prev, [field]: value }));

  const handleGenerate = async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ language, pipelineType, ...form }),
      });
      const data = await res.json();
      if (data.success) {
        setFiles(data.files);
        onGenerate(data.files);
        setStep(3);
      } else {
        alert("Generation failed: " + data.error);
      }
    } catch (err) {
      alert("API error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = (file) => {
    const blob = new Blob([file.content], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = file.name;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleDownloadAll = () => {
    if (!files) return;
    files.forEach((f) => handleDownload(f));
  };

  return (
    <div>
      <h2>
        <span className="dot" style={{ background: "#3b82f6" }} />
        Orchestrator — Convert SSIS Package
      </h2>

      {/* Steps indicator */}
      <div className="steps">
        {[0, 1, 2, 3].map((s) => (
          <div
            key={s}
            className={`step ${s < step ? "done" : ""} ${s === step ? "active" : ""}`}
          />
        ))}
      </div>

      {/* ── Step 0: Pick Language ─────────────────────── */}
      {step === 0 && (
        <div>
          <p style={{ color: "var(--text-secondary)", marginBottom: 20 }}>
            Which target language for your SSIS package?
          </p>
          <div className="language-picker three-col">
            <div
              className={`lang-option ${language === "pyspark" ? "selected" : ""}`}
              onClick={() => setLanguage("pyspark")}
            >
              <div className="icon">🐍</div>
              <h4>PySpark</h4>
              <p>Python-based Spark pipelines</p>
              <div className="tags">
                <span className="tag">Rapid dev</span>
                <span className="tag">Notebooks</span>
                <span className="tag">ML ready</span>
              </div>
              <div className="volume-hint">Any volume</div>
            </div>
            <div
              className={`lang-option ${language === "springbatch" ? "selected" : ""}`}
              onClick={() => setLanguage("springbatch")}
            >
              <div className="icon">🍃</div>
              <h4>Spring Batch</h4>
              <p>Traditional Java ETL — closest to SSIS mental model</p>
              <div className="tags">
                <span className="tag">Java teams</span>
                <span className="tag">No cluster</span>
                <span className="tag">SSIS-like</span>
              </div>
              <div className="volume-hint">Best for &lt; 5M rows</div>
            </div>
            <div
              className={`lang-option ${language === "java" ? "selected" : ""}`}
              onClick={() => setLanguage("java")}
            >
              <div className="icon">☕</div>
              <h4>Java Spark</h4>
              <p>Distributed Spark on JVM — high-volume workloads</p>
              <div className="tags">
                <span className="tag">Type safe</span>
                <span className="tag">Distributed</span>
                <span className="tag">High volume</span>
              </div>
              <div className="volume-hint">Best for 10M+ rows</div>
            </div>
          </div>
          <div className="btn-row">
            <button
              className="btn btn-primary"
              disabled={!language}
              onClick={() => setStep(1)}
            >
              Next →
            </button>
          </div>
        </div>
      )}

      {/* ── Step 1: Pick Pipeline Type ────────────────── */}
      {step === 1 && (
        <div>
          <p style={{ color: "var(--text-secondary)", marginBottom: 20 }}>
            What type of SSIS package are you converting?
          </p>
          <div className="pipeline-types">
            {PIPELINE_TYPES.map((pt) => (
              <div
                key={pt.id}
                className={`pipeline-type ${pipelineType === pt.id ? "selected" : ""}`}
                onClick={() => setPipelineType(pt.id)}
              >
                <h4>{pt.name}</h4>
                <p>{pt.desc}</p>
              </div>
            ))}
          </div>
          <div className="btn-row">
            <button className="btn btn-secondary" onClick={() => setStep(0)}>
              ← Back
            </button>
            <button
              className="btn btn-primary"
              disabled={!pipelineType}
              onClick={() => setStep(2)}
            >
              Next →
            </button>
          </div>
        </div>
      )}

      {/* ── Step 2: Pipeline Details ──────────────────── */}
      {step === 2 && (
        <div>
          <p style={{ color: "var(--text-secondary)", marginBottom: 20 }}>
            Provide details about your SSIS package:
          </p>
          <div className="form-grid">
            <div className="form-group">
              <label>Pipeline Name</label>
              <input
                placeholder="e.g. load_customer_dim"
                value={form.pipelineName}
                onChange={(e) => updateForm("pipelineName", e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>SSIS Package (.dtsx)</label>
              <input
                placeholder="e.g. Load_Customer_Dim.dtsx"
                value={form.ssisPackage}
                onChange={(e) => updateForm("ssisPackage", e.target.value)}
              />
            </div>
            <div className="form-group full">
              <label>Description</label>
              <textarea
                placeholder="Brief description of what this pipeline does"
                value={form.description}
                onChange={(e) => updateForm("description", e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Source Type</label>
              <select
                value={form.sourceType}
                onChange={(e) => updateForm("sourceType", e.target.value)}
              >
                <option value="jdbc">JDBC (SQL Server / Oracle)</option>
                <option value="csv">Flat File (CSV)</option>
                <option value="delta">Delta Lake</option>
                <option value="api">REST API</option>
              </select>
            </div>
            <div className="form-group">
              <label>Target Type</label>
              <select
                value={form.targetType}
                onChange={(e) => updateForm("targetType", e.target.value)}
              >
                <option value="delta">Delta Lake</option>
                <option value="jdbc">JDBC (SQL Server)</option>
                <option value="parquet">Parquet</option>
              </select>
            </div>
            <div className="form-group">
              <label>Environment</label>
              <select
                value={form.environment}
                onChange={(e) => updateForm("environment", e.target.value)}
              >
                <option value="dev">Development</option>
                <option value="qa">QA / Staging</option>
                <option value="prod">Production</option>
              </select>
            </div>
            <div className="form-group">
              <label>Author</label>
              <input
                placeholder="Your name"
                value={form.author}
                onChange={(e) => updateForm("author", e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>JIRA Ticket</label>
              <input
                placeholder="Optional — e.g. ETL-1234"
                value={form.jiraTicket}
                onChange={(e) => updateForm("jiraTicket", e.target.value)}
              />
            </div>
            <div className="form-group">
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <input
                  type="checkbox"
                  checked={form.generateAirflow}
                  onChange={(e) =>
                    updateForm("generateAirflow", e.target.checked)
                  }
                  style={{ width: "auto" }}
                />
                Generate Airflow DAG
              </label>
            </div>
          </div>
          <div className="btn-row">
            <button className="btn btn-secondary" onClick={() => setStep(1)}>
              ← Back
            </button>
            <button
              className="btn btn-primary"
              disabled={!form.pipelineName || loading}
              onClick={handleGenerate}
            >
              {loading ? "Generating..." : "Generate Code 🚀"}
            </button>
          </div>
        </div>
      )}

      {/* ── Step 3: Preview & Download ────────────────── */}
      {step === 3 && files && <PreviewStep files={files} onDownload={handleDownload} onDownloadAll={handleDownloadAll} onBack={() => setStep(2)} language={language} />}
    </div>
  );
}

function PreviewStep({ files, onDownload, onDownloadAll, onBack, language }) {
  const [activeTab, setActiveTab] = useState(0);

  return (
    <div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
        <p style={{ color: "var(--text-secondary)" }}>
          ✅ Generated <strong>{files.length}</strong> files for{" "}
          <strong>{language === "pyspark" ? "PySpark" : language === "springbatch" ? "Spring Batch" : "Java Spark"}</strong>
        </p>
        <div style={{ display: "flex", gap: 8 }}>
          <button className="btn btn-secondary" onClick={onBack}>
            ← Edit
          </button>
          <button className="btn btn-green" onClick={onDownloadAll}>
            ⬇ Download All
          </button>
        </div>
      </div>

      <div className="code-preview">
        <div className="code-tabs">
          {files.map((f, i) => (
            <div
              key={i}
              className={`code-tab ${activeTab === i ? "active" : ""}`}
              onClick={() => setActiveTab(i)}
            >
              {f.type === "pipeline" ? "📄" : "🌊"} {f.name}
            </div>
          ))}
        </div>
        <div className="code-content">{files[activeTab]?.content}</div>
      </div>

      <div className="btn-row" style={{ marginTop: 12 }}>
        <button
          className="btn btn-secondary"
          onClick={() => onDownload(files[activeTab])}
        >
          ⬇ Download {files[activeTab]?.name}
        </button>
      </div>
    </div>
  );
}
