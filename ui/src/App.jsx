import React, { useState } from "react";
import ModuleCards from "./components/ModuleCards.jsx";
import OrchestratorWizard from "./components/OrchestratorWizard.jsx";
import PySparkView from "./components/PySparkView.jsx";
import JavaView from "./components/JavaView.jsx";
import SpringBatchView from "./components/SpringBatchView.jsx";
import AirflowView from "./components/AirflowView.jsx";

const MODULES = {
  orchestrator: {
    id: "orchestrator",
    name: "Orchestrator",
    icon: "🎯",
    desc: "Pick SSIS package, choose target language, generate code",
    badge: "Start Here",
    color: "#3b82f6",
  },
  pyspark: {
    id: "pyspark",
    name: "PySpark",
    icon: "🐍",
    desc: "Python Spark pipelines, transforms, SCD, validations",
    badge: "Generator",
    color: "#f97316",
  },
  java: {
    id: "java",
    name: "Java Spark",
    icon: "☕",
    desc: "Distributed Java Spark — high-volume, 10M+ rows",
    badge: "Big Data",
    color: "#a855f7",
  },
  springbatch: {
    id: "springbatch",
    name: "Spring Batch",
    icon: "🍃",
    desc: "Traditional Java ETL — Reader/Processor/Writer, < 5M rows",
    badge: "Enterprise",
    color: "#10b981",
  },
  airflow: {
    id: "airflow",
    name: "Airflow DAGs",
    icon: "🌊",
    desc: "DAG generation for Databricks or standalone Spark",
    badge: "Scheduling",
    color: "#22c55e",
  },
};

export default function App() {
  const [activeModule, setActiveModule] = useState(null);
  const [generatedFiles, setGeneratedFiles] = useState(null);

  const handleGenerate = (files) => {
    setGeneratedFiles(files);
  };

  const renderModule = () => {
    switch (activeModule) {
      case "orchestrator":
        return <OrchestratorWizard onGenerate={handleGenerate} />;
      case "pyspark":
        return <PySparkView files={generatedFiles} />;
      case "java":
        return <JavaView files={generatedFiles} />;
      case "springbatch":
        return <SpringBatchView files={generatedFiles} />;
      case "airflow":
        return <AirflowView files={generatedFiles} />;
      default:
        return (
          <div className="empty-state">
            <div className="icon">🏗️</div>
            <h3>ETL Replatform</h3>
            <p>
              Select <strong>Orchestrator</strong> to begin converting an SSIS
              package, or click any module to view its generated output.
            </p>
          </div>
        );
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>
          <span>etl-replatform</span>
        </h1>
        <p>Decommission SSIS — Replatform to PySpark, Java Spark, or Airflow</p>
      </header>

      <ModuleCards
        modules={MODULES}
        active={activeModule}
        onSelect={setActiveModule}
        generatedFiles={generatedFiles}
      />

      <div className="wizard-area">{renderModule()}</div>
    </div>
  );
}
