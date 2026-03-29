"""
PySpark Generator Module
========================
Generates PySpark pipeline code from SSIS package metadata.

SSIS → PySpark mapping:
  OLE DB Source             →  get_jdbc_connection()
  Flat File Source          →  read_csv()
  Derived Column            →  apply_derived_columns()
  Conditional Split         →  apply_conditional_split()
  Lookup Transform          →  apply_lookup() + broadcast
  Union All                 →  union_dataframes()
  Data Conversion           →  apply_type_conversions()
  Sort (Remove Duplicates)  →  deduplicate()
  Error Output (Redirect)   →  route_errors()
  SCD Type 1                →  scd_type1_merge()
  SCD Type 2                →  scd_type2_merge()
  OLE DB Destination        →  write_delta() / write_jdbc()
  SSISDB Audit              →  AuditTracker
"""

from datetime import datetime


class PySparkGenerator:
    """Generates PySpark pipeline code for various SSIS package types."""

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
            for k, v in PySparkGenerator.TEMPLATES.items()
        ]

    def generate(self, pipeline_type: str, details: dict, environment: str) -> dict:
        """Generate a PySpark pipeline file dict."""
        generators = {
            "dimension_scd2": self._dim_scd2,
            "dimension_scd1": self._dim_scd1,
            "fact_load": self._fact,
            "flat_file_ingest": self._flat_file,
            "generic": self._generic,
        }
        gen_fn = generators.get(pipeline_type, self._generic)
        code = gen_fn(details, environment)

        return {
            "name": f"{details['pipeline_name']}.py",
            "language": "python",
            "content": code,
            "type": "pipeline",
        }

    # ── Header ─────────────────────────────────────────────

    def _header(self, d: dict) -> str:
        return f'''"""
============================================================
Pipeline: {d["pipeline_name"]}
============================================================
Converted from: {d["ssis_package"]}
Description:    {d["description"]}
Author:         {d["author"]}
Date:           {datetime.now().strftime("%Y-%m-%d")}
JIRA:           {d["jira_ticket"]}
============================================================
"""
'''

    # ── Dimension SCD2 ─────────────────────────────────────

    def _dim_scd2(self, d: dict, env: str) -> str:
        return self._header(d) + f'''
import argparse
from pyspark.sql import SparkSession, functions as F

from common.config_loader import load_config, get_spark_config
from common.connections import get_jdbc_connection, write_delta
from common.transformations import (
    apply_derived_columns, apply_lookup, apply_null_handling, deduplicate,
)
from common.validations import validate_schema, check_nulls, check_duplicates
from common.error_handling import route_errors, write_error_rows
from common.logging_utils import PipelineLogger
from common.audit import AuditTracker
from common.scd import scd_type2_merge


def main(environment: str = "{env}"):
    pipeline_name = "{d["pipeline_name"]}"
    config = load_config(environment=environment, pipeline_name=pipeline_name)
    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    log = PipelineLogger(pipeline_name, environment=environment)
    audit = AuditTracker(pipeline_name, environment=environment)
    log.log_start()
    audit.start()

    try:
        # ── READ SOURCE ──────────────────────────────────
        log.log_stage_start("read_source")
        source_conn = config.connections["source"]

        # TODO: Replace with your source table/query
        df_source = get_jdbc_connection(
            spark, source_conn,
            table_or_query="dbo.YourSourceTable",
            num_partitions=config.parameters.get("parallel_read_partitions", 4),
            partition_column="id",
            lower_bound=1, upper_bound=10000000,
        )
        source_count = df_source.count()
        log.log_read("source", source_count)
        audit.record_read("source", source_count)
        log.log_stage_end("read_source", row_count=source_count)

        if source_count == 0:
            log.log_info("No new/changed records. Exiting.")
            audit.complete()
            return

        # ── TRANSFORM ────────────────────────────────────
        log.log_stage_start("transform")

        # TODO: Replace with your derived columns
        df_transformed = apply_derived_columns(df_source, {{
            "load_timestamp": F.current_timestamp(),
            "source_system": F.lit("SOURCE_DB"),
        }})

        df_transformed = apply_null_handling(df_transformed, defaults={{}})

        df_transformed = deduplicate(
            df_transformed,
            key_columns=["business_key"],
            order_columns=["modified_date"],
            keep="last",
        )
        log.log_stage_end("transform", row_count=df_transformed.count())

        # ── VALIDATE & ROUTE ERRORS ──────────────────────
        log.log_stage_start("validate")
        df_valid, df_errors = route_errors(df_transformed, [
            # TODO: Add your validation rules
            {{"name": "null_key",
              "condition": F.col("business_key").isNull(),
              "message": "Business key cannot be null"}},
        ])
        error_count = df_errors.count()
        if error_count > 0:
            write_error_rows(df_errors, pipeline_name, "validation",
                             batch_id=audit.batch_id)
            audit.record_errors("validation", error_count)
        log.log_stage_end("validate", row_count=df_valid.count())

        # ── WRITE: SCD Type 2 ────────────────────────────
        log.log_stage_start("write_target")

        # TODO: Replace with your target table and tracked columns
        scd_type2_merge(
            spark, df_valid,
            target_table="silver.your_dimension_table",
            business_keys=["business_key"],
            tracked_columns=["col1", "col2", "col3"],
        )
        audit.record_write("target", df_valid.count())
        log.log_stage_end("write_target")

        log.log_success()
        audit.complete()

    except Exception as e:
        log.log_failure(str(e))
        audit.fail(str(e))
        raise
    finally:
        spark.catalog.clearCache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="{d["pipeline_name"]}")
    parser.add_argument("--env", default="{env}", choices=["dev", "qa", "prod"])
    args = parser.parse_args()
    main(environment=args.env)
'''

    # ── Dimension SCD1 ─────────────────────────────────────

    def _dim_scd1(self, d: dict, env: str) -> str:
        return self._header(d) + f'''
import argparse
from pyspark.sql import SparkSession, functions as F

from common.config_loader import load_config
from common.connections import get_jdbc_connection
from common.transformations import apply_derived_columns, deduplicate
from common.error_handling import route_errors, write_error_rows
from common.logging_utils import PipelineLogger
from common.audit import AuditTracker
from common.scd import scd_type1_merge


def main(environment: str = "{env}"):
    pipeline_name = "{d["pipeline_name"]}"
    config = load_config(environment=environment, pipeline_name=pipeline_name)
    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    log = PipelineLogger(pipeline_name, environment=environment)
    audit = AuditTracker(pipeline_name, environment=environment)
    log.log_start()
    audit.start()

    try:
        # ── READ ─────────────────────────────────────────
        df_source = get_jdbc_connection(
            spark, config.connections["source"],
            table_or_query="dbo.YourSourceTable",
            num_partitions=4, partition_column="id",
            lower_bound=1, upper_bound=10000000,
        )
        audit.record_read("source", df_source.count())

        # ── TRANSFORM ────────────────────────────────────
        df_transformed = apply_derived_columns(df_source, {{
            "load_timestamp": F.current_timestamp(),
            "source_system": F.lit("SOURCE_DB"),
        }})
        df_transformed = deduplicate(
            df_transformed, ["business_key"], ["modified_date"], "last"
        )

        # ── VALIDATE ─────────────────────────────────────
        df_valid, df_errors = route_errors(df_transformed, [
            {{"name": "null_key",
              "condition": F.col("business_key").isNull(),
              "message": "Business key cannot be null"}},
        ])
        if df_errors.count() > 0:
            write_error_rows(df_errors, pipeline_name, "validation",
                             batch_id=audit.batch_id)

        # ── WRITE: SCD Type 1 ────────────────────────────
        scd_type1_merge(
            spark, df_valid,
            target_table="silver.your_dimension_table",
            business_keys=["business_key"],
            exclude_columns=["created_date"],
        )
        audit.record_write("target", df_valid.count())
        audit.complete()

    except Exception as e:
        audit.fail(str(e))
        raise
    finally:
        spark.catalog.clearCache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="{env}", choices=["dev", "qa", "prod"])
    main(environment=parser.parse_args().env)
'''

    # ── Fact Load ──────────────────────────────────────────

    def _fact(self, d: dict, env: str) -> str:
        return self._header(d) + f'''
import argparse
from pyspark.sql import SparkSession, functions as F

from common.config_loader import load_config
from common.connections import get_jdbc_connection, write_delta
from common.transformations import apply_derived_columns, apply_lookup
from common.error_handling import route_errors, write_error_rows
from common.logging_utils import PipelineLogger
from common.audit import AuditTracker


def main(environment: str = "{env}"):
    pipeline_name = "{d["pipeline_name"]}"
    config = load_config(environment=environment, pipeline_name=pipeline_name)
    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    log = PipelineLogger(pipeline_name, environment=environment)
    audit = AuditTracker(pipeline_name, environment=environment)
    log.log_start()
    audit.start()

    try:
        source_conn = config.connections["source"]

        # ── READ: Primary + Detail ───────────────────────
        df_primary = get_jdbc_connection(
            spark, source_conn, "dbo.YourPrimaryTable",
            num_partitions=8, partition_column="id",
            lower_bound=1, upper_bound=5000000,
        )
        df_detail = get_jdbc_connection(
            spark, source_conn, "dbo.YourDetailTable",
            num_partitions=8, partition_column="detail_id",
            lower_bound=1, upper_bound=20000000,
        )
        audit.record_read("primary", df_primary.count())
        audit.record_read("detail", df_detail.count())

        # ── JOIN (replaces SSIS Merge Join) ──────────────
        df_joined = df_detail.join(df_primary, on="join_key", how="inner")

        # ── LOOKUP: Dimension surrogate keys ─────────────
        # TODO: Add dimension lookups
        # df_dim = get_jdbc_connection(spark, ...).cache()
        # df_joined, _ = apply_lookup(df_joined, df_dim, ...)

        # ── TRANSFORM ────────────────────────────────────
        df_fact = apply_derived_columns(df_joined, {{
            "fact_sk": F.monotonically_increasing_id(),
            "source_system": F.lit("OLTP"),
            "load_timestamp": F.current_timestamp(),
        }})

        # ── VALIDATE ─────────────────────────────────────
        df_valid, df_errors = route_errors(df_fact, [
            # TODO: Add validation rules
        ])
        if df_errors.count() > 0:
            write_error_rows(df_errors, pipeline_name, "validation",
                             batch_id=audit.batch_id)

        # ── WRITE ────────────────────────────────────────
        target_path = f"{{config.paths['delta_root']}}/gold/your_fact_table"
        write_delta(df_valid, path=target_path, mode="append",
                    partition_by=["date_sk"])
        audit.record_write("fact_table", df_valid.count())
        audit.complete()

    except Exception as e:
        audit.fail(str(e))
        raise
    finally:
        spark.catalog.clearCache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="{env}", choices=["dev", "qa", "prod"])
    main(environment=parser.parse_args().env)
'''

    # ── Flat File Ingest ───────────────────────────────────

    def _flat_file(self, d: dict, env: str) -> str:
        return self._header(d) + f'''
import argparse
from pyspark.sql import SparkSession, functions as F

from common.config_loader import load_config
from common.connections import read_csv, write_delta
from common.transformations import apply_derived_columns, apply_type_conversions
from common.error_handling import route_errors, write_error_rows
from common.logging_utils import PipelineLogger
from common.audit import AuditTracker


def main(environment: str = "{env}"):
    pipeline_name = "{d["pipeline_name"]}"
    config = load_config(environment=environment, pipeline_name=pipeline_name)
    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    log = PipelineLogger(pipeline_name, environment=environment)
    audit = AuditTracker(pipeline_name, environment=environment)
    log.log_start()
    audit.start()

    try:
        # ── READ: Flat File ──────────────────────────────
        file_path = f"{{config.paths['incoming']}}/your_file.csv"
        df_source = read_csv(
            spark, file_path, delimiter=",", header=True, encoding="UTF-8",
        )
        audit.record_read("file", df_source.count())

        # ── TRANSFORM ────────────────────────────────────
        # TODO: Add type conversions (CSV reads as String)
        df_transformed = apply_type_conversions(df_source, {{}})
        df_transformed = apply_derived_columns(df_transformed, {{
            "source_file": F.input_file_name(),
            "load_timestamp": F.current_timestamp(),
        }})

        # ── VALIDATE ─────────────────────────────────────
        df_valid, df_errors = route_errors(df_transformed, [])
        if df_errors.count() > 0:
            write_error_rows(df_errors, pipeline_name, "validation",
                             batch_id=audit.batch_id)

        # ── WRITE ────────────────────────────────────────
        target_path = f"{{config.paths['delta_root']}}/bronze/your_table"
        write_delta(df_valid, path=target_path, mode="append")
        audit.record_write("target", df_valid.count())
        audit.complete()

    except Exception as e:
        audit.fail(str(e))
        raise
    finally:
        spark.catalog.clearCache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="{env}", choices=["dev", "qa", "prod"])
    main(environment=parser.parse_args().env)
'''

    # ── Generic ────────────────────────────────────────────

    def _generic(self, d: dict, env: str) -> str:
        return self._header(d) + f'''
import argparse
from pyspark.sql import SparkSession, functions as F

from common.config_loader import load_config
from common.connections import get_jdbc_connection, write_delta, read_csv
from common.transformations import *
from common.error_handling import route_errors, write_error_rows
from common.logging_utils import PipelineLogger
from common.audit import AuditTracker


def main(environment: str = "{env}"):
    pipeline_name = "{d["pipeline_name"]}"
    config = load_config(environment=environment, pipeline_name=pipeline_name)
    spark = SparkSession.builder.appName(pipeline_name).getOrCreate()

    audit = AuditTracker(pipeline_name, environment=environment)
    audit.start()

    try:
        # ── 1. READ SOURCES ──────────────────────────────
        # TODO: Add your source reads here

        # ── 2. TRANSFORM ────────────────────────────────
        # TODO: Add your transformations here

        # ── 3. VALIDATE ─────────────────────────────────
        # TODO: Add your validations here

        # ── 4. WRITE ────────────────────────────────────
        # TODO: Add your target writes here

        audit.complete()

    except Exception as e:
        audit.fail(str(e))
        raise
    finally:
        spark.catalog.clearCache()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="{env}", choices=["dev", "qa", "prod"])
    main(environment=parser.parse_args().env)
'''
