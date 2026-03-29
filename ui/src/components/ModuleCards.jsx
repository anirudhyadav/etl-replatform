import React from "react";

export default function ModuleCards({ modules, active, onSelect, generatedFiles }) {
  return (
    <div className="modules-grid">
      {Object.values(modules).map((mod) => {
        const hasOutput =
          generatedFiles &&
          generatedFiles.some(
            (f) =>
              (mod.id === "pyspark" && f.language === "python" && f.type === "pipeline") ||
              (mod.id === "java" && f.language === "java" && f.type === "pipeline" && !f.name.includes("JobConfig")) ||
            (mod.id === "springbatch" && f.language === "java" && f.type === "pipeline" && f.name.includes("JobConfig")) ||
              (mod.id === "airflow" && f.type === "orchestration")
          );

        return (
          <div
            key={mod.id}
            className={`module-card ${active === mod.id ? "active" : ""}`}
            style={{ "--module-color": mod.color }}
            onClick={() => onSelect(mod.id)}
          >
            <div className="module-icon">{mod.icon}</div>
            <h3>{mod.name}</h3>
            <p>{mod.desc}</p>
            <span className="module-badge">
              {hasOutput ? "✓ Generated" : mod.badge}
            </span>
          </div>
        );
      })}
    </div>
  );
}
