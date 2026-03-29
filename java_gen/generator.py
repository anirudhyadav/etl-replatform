"""
Java Spark Generator Module
============================
Generates Java Spark pipeline code from SSIS package metadata.

SSIS → Java Spark mapping:
  OLE DB Source             →  ConnectionFactory.readJdbc()
  Flat File Source          →  ConnectionFactory.readCsv()
  Derived Column            →  TransformationUtils.applyDerivedColumns()
  Conditional Split         →  TransformationUtils.applyConditionalSplit()
  Lookup Transform          →  TransformationUtils.applyLookup()
  Union All                 →  TransformationUtils.unionDataframes()
  Data Conversion           →  TransformationUtils.applyTypeConversions()
  Sort (Remove Duplicates)  →  TransformationUtils.deduplicate()
  Error Output (Redirect)   →  ErrorHandler.routeErrors()
  SCD Type 1                →  ScdMerge.type1Merge()
  SCD Type 2                →  ScdMerge.type2Merge()
  OLE DB Destination        →  ConnectionFactory.writeDelta()
  SSISDB Audit              →  AuditTracker
"""

from datetime import datetime


def _to_pascal_case(snake_str: str) -> str:
    return "".join(word.capitalize() for word in snake_str.split("_"))


class JavaGenerator:
    """Generates Java Spark pipeline code for various SSIS package types."""

    TEMPLATES = {
        "dimension_scd2": "Dimension — SCD Type 2 (history tracking)",
        "dimension_scd1": "Dimension — SCD Type 1 (overwrite)",
        "fact_load": "Fact Table Load (join + append)",
        "flat_file_ingest": "Flat File Ingestion (CSV → Delta)",
        "generic": "Generic Pipeline (blank template)",
    }

    @staticmethod
    def list_templates():
        return [
            {"id": k, "name": v}
            for k, v in JavaGenerator.TEMPLATES.items()
        ]

    def generate(self, pipeline_type: str, details: dict, environment: str) -> dict:
        """Generate a Java Spark pipeline file dict."""
        generators = {
            "dimension_scd2": self._dim_scd2,
            "dimension_scd1": self._dim_scd1,
            "fact_load": self._fact,
            "flat_file_ingest": self._flat_file,
            "generic": self._generic,
        }
        gen_fn = generators.get(pipeline_type, self._generic)
        code = gen_fn(details, environment)
        class_name = _to_pascal_case(details["pipeline_name"])

        return {
            "name": f"{class_name}.java",
            "language": "java",
            "content": code,
            "type": "pipeline",
        }

    # ── Header ─────────────────────────────────────────────

    def _header(self, d: dict) -> str:
        return f'''/*
 * ============================================================
 * Pipeline: {d["pipeline_name"]}
 * ============================================================
 * Converted from: {d["ssis_package"]}
 * Description:    {d["description"]}
 * Author:         {d["author"]}
 * Date:           {datetime.now().strftime("%Y-%m-%d")}
 * JIRA:           {d["jira_ticket"]}
 * ============================================================
 */
'''

    # ── Dimension SCD2 ─────────────────────────────────────

    def _dim_scd2(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.pipelines;

import com.etl.common.*;
import com.etl.common.ConfigLoader.PipelineConfig;
import com.etl.common.ErrorHandler.ErrorRule;
import org.apache.spark.sql.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class {cls} {{

    public static void main(String[] args) {{
        String env = args.length > 0 ? args[0] : "{env}";
        new {cls}().run(env);
    }}

    public void run(String environment) {{
        String pipelineName = "{d["pipeline_name"]}";
        PipelineConfig config = ConfigLoader.load(environment, pipelineName);

        SparkSession spark = SparkSession.builder()
                .appName(pipelineName)
                .getOrCreate();

        AuditTracker audit = new AuditTracker(pipelineName, environment);
        audit.start();

        try {{
            // ── READ SOURCE ─────────────────────────────
            // TODO: Replace with your source table/query
            Dataset<Row> source = ConnectionFactory.readJdbc(
                    spark, config.getConnection("source"),
                    "dbo.YourSourceTable",
                    "id", 1, 10000000, 4);
            audit.recordRead("source", source.count());

            if (source.count() == 0) {{
                audit.complete();
                return;
            }}

            // ── TRANSFORM ───────────────────────────────
            // TODO: Replace with your derived columns
            Dataset<Row> transformed = TransformationUtils.applyDerivedColumns(
                    source, Map.of(
                        "load_timestamp", current_timestamp(),
                        "source_system", lit("SOURCE_DB")));

            transformed = TransformationUtils.deduplicate(
                    transformed,
                    List.of("business_key"),
                    List.of("modified_date"), "last");

            // ── VALIDATE & ROUTE ERRORS ─────────────────
            // TODO: Add your validation rules
            List<ErrorRule> rules = List.of(
                    new ErrorRule("null_key",
                            col("business_key").isNull(),
                            "Business key cannot be null"));

            ErrorHandler.Result result = ErrorHandler.routeErrors(
                    transformed, rules);

            if (result.errorCount() > 0) {{
                ErrorHandler.writeErrorRows(result.errors(),
                        pipelineName, "validation", audit.getBatchId());
                audit.recordErrors("validation", result.errorCount());
            }}

            // ── WRITE: SCD Type 2 ───────────────────────
            // TODO: Replace with your target table and tracked columns
            ScdMerge.type2Merge(spark, result.valid(),
                    "silver.your_dimension_table",
                    List.of("business_key"),
                    List.of("col1", "col2", "col3"));

            audit.recordWrite("target", result.validCount());
            audit.complete();

        }} catch (Exception e) {{
            audit.fail(e.getMessage());
            throw new RuntimeException(e);
        }} finally {{
            spark.catalog().clearCache();
        }}
    }}
}}
'''

    # ── Dimension SCD1 ─────────────────────────────────────

    def _dim_scd1(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.pipelines;

import com.etl.common.*;
import com.etl.common.ConfigLoader.PipelineConfig;
import com.etl.common.ErrorHandler.ErrorRule;
import org.apache.spark.sql.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class {cls} {{

    public static void main(String[] args) {{
        String env = args.length > 0 ? args[0] : "{env}";
        new {cls}().run(env);
    }}

    public void run(String environment) {{
        String pipelineName = "{d["pipeline_name"]}";
        PipelineConfig config = ConfigLoader.load(environment, pipelineName);

        SparkSession spark = SparkSession.builder()
                .appName(pipelineName).getOrCreate();

        AuditTracker audit = new AuditTracker(pipelineName, environment);
        audit.start();

        try {{
            // ── READ ────────────────────────────────────
            Dataset<Row> source = ConnectionFactory.readJdbc(
                    spark, config.getConnection("source"),
                    "dbo.YourSourceTable",
                    "id", 1, 10000000, 4);
            audit.recordRead("source", source.count());

            // ── TRANSFORM ───────────────────────────────
            Dataset<Row> transformed = TransformationUtils.applyDerivedColumns(
                    source, Map.of(
                        "load_timestamp", current_timestamp(),
                        "source_system", lit("SOURCE_DB")));
            transformed = TransformationUtils.deduplicate(
                    transformed, List.of("business_key"),
                    List.of("modified_date"), "last");

            // ── VALIDATE ────────────────────────────────
            ErrorHandler.Result result = ErrorHandler.routeErrors(
                    transformed, List.of(
                        new ErrorRule("null_key",
                                col("business_key").isNull(),
                                "Business key cannot be null")));

            if (result.errorCount() > 0)
                ErrorHandler.writeErrorRows(result.errors(),
                        pipelineName, "validation", audit.getBatchId());

            // ── WRITE: SCD Type 1 ───────────────────────
            ScdMerge.type1Merge(spark, result.valid(),
                    "silver.your_dimension_table",
                    List.of("business_key"),
                    List.of("created_date"));

            audit.recordWrite("target", result.validCount());
            audit.complete();

        }} catch (Exception e) {{
            audit.fail(e.getMessage());
            throw new RuntimeException(e);
        }}
    }}
}}
'''

    # ── Fact Load ──────────────────────────────────────────

    def _fact(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.pipelines;

import com.etl.common.*;
import com.etl.common.ConfigLoader.PipelineConfig;
import org.apache.spark.sql.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class {cls} {{

    public static void main(String[] args) {{
        String env = args.length > 0 ? args[0] : "{env}";
        new {cls}().run(env);
    }}

    public void run(String environment) {{
        String pipelineName = "{d["pipeline_name"]}";
        PipelineConfig config = ConfigLoader.load(environment, pipelineName);

        SparkSession spark = SparkSession.builder()
                .appName(pipelineName).getOrCreate();

        AuditTracker audit = new AuditTracker(pipelineName, environment);
        audit.start();

        try {{
            // ── READ: Primary + Detail ──────────────────
            Dataset<Row> primary = ConnectionFactory.readJdbc(
                    spark, config.getConnection("source"),
                    "dbo.YourPrimaryTable",
                    "id", 1, 5000000, 8);
            Dataset<Row> detail = ConnectionFactory.readJdbc(
                    spark, config.getConnection("source"),
                    "dbo.YourDetailTable",
                    "detail_id", 1, 20000000, 8);
            audit.recordRead("primary", primary.count());
            audit.recordRead("detail", detail.count());

            // ── JOIN (replaces SSIS Merge Join) ─────────
            Dataset<Row> joined = detail.join(primary, "join_key");

            // ── LOOKUP: Dimension surrogate keys ────────
            // TODO: Add dimension lookups
            // Dataset<Row> dim = ConnectionFactory.readJdbc(...).cache();
            // joined = TransformationUtils.applyLookup(joined, dim, ...);

            // ── TRANSFORM ───────────────────────────────
            Dataset<Row> fact = TransformationUtils.applyDerivedColumns(
                    joined, Map.of(
                        "fact_sk", monotonically_increasing_id(),
                        "source_system", lit("OLTP"),
                        "load_timestamp", current_timestamp()));

            // ── WRITE ───────────────────────────────────
            String targetPath = config.getPath("delta_root")
                    + "/gold/your_fact_table";
            ConnectionFactory.writeDelta(fact, targetPath, "append",
                    List.of("date_sk"));

            audit.recordWrite("fact_table", fact.count());
            audit.complete();

        }} catch (Exception e) {{
            audit.fail(e.getMessage());
            throw new RuntimeException(e);
        }}
    }}
}}
'''

    # ── Flat File Ingest ───────────────────────────────────

    def _flat_file(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.pipelines;

import com.etl.common.*;
import com.etl.common.ConfigLoader.PipelineConfig;
import org.apache.spark.sql.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class {cls} {{

    public static void main(String[] args) {{
        String env = args.length > 0 ? args[0] : "{env}";
        new {cls}().run(env);
    }}

    public void run(String environment) {{
        String pipelineName = "{d["pipeline_name"]}";
        PipelineConfig config = ConfigLoader.load(environment, pipelineName);

        SparkSession spark = SparkSession.builder()
                .appName(pipelineName).getOrCreate();

        AuditTracker audit = new AuditTracker(pipelineName, environment);
        audit.start();

        try {{
            // ── READ: Flat File ─────────────────────────
            String filePath = config.getPath("incoming") + "/your_file.csv";
            Dataset<Row> source = ConnectionFactory.readCsv(
                    spark, filePath, ",", true, "UTF-8");
            audit.recordRead("file", source.count());

            // ── TRANSFORM ───────────────────────────────
            Dataset<Row> transformed = TransformationUtils.applyDerivedColumns(
                    source, Map.of(
                        "source_file", input_file_name(),
                        "load_timestamp", current_timestamp()));

            // ── WRITE ───────────────────────────────────
            String targetPath = config.getPath("delta_root")
                    + "/bronze/your_table";
            ConnectionFactory.writeDelta(transformed, targetPath,
                    "append", List.of());

            audit.recordWrite("target", transformed.count());
            audit.complete();

        }} catch (Exception e) {{
            audit.fail(e.getMessage());
            throw new RuntimeException(e);
        }}
    }}
}}
'''

    # ── Generic ────────────────────────────────────────────

    def _generic(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.pipelines;

import com.etl.common.*;
import com.etl.common.ConfigLoader.PipelineConfig;
import org.apache.spark.sql.*;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class {cls} {{

    public static void main(String[] args) {{
        String env = args.length > 0 ? args[0] : "{env}";
        new {cls}().run(env);
    }}

    public void run(String environment) {{
        String pipelineName = "{d["pipeline_name"]}";
        PipelineConfig config = ConfigLoader.load(environment, pipelineName);

        SparkSession spark = SparkSession.builder()
                .appName(pipelineName).getOrCreate();

        AuditTracker audit = new AuditTracker(pipelineName, environment);
        audit.start();

        try {{
            // TODO: 1. READ SOURCES
            // TODO: 2. TRANSFORM
            // TODO: 3. VALIDATE
            // TODO: 4. WRITE

            audit.complete();
        }} catch (Exception e) {{
            audit.fail(e.getMessage());
            throw new RuntimeException(e);
        }}
    }}
}}
'''
