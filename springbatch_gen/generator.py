"""
Spring Batch Generator Module
==============================
Generates Spring Batch pipeline code from SSIS package metadata.

This is the natural Java replacement for SSIS — same mental model:
  SSIS Package          →  Spring Batch Job
  SSIS Data Flow Task   →  Spring Batch Step
  OLE DB Source         →  JdbcCursorItemReader / JdbcPagingItemReader
  Flat File Source      →  FlatFileItemReader
  Derived Column        →  ItemProcessor (transform)
  Lookup Transform      →  ItemProcessor (enrich via DAO)
  Conditional Split     →  ClassifierCompositeItemWriter
  Error Output          →  SkipPolicy + ErrorItemWriter
  OLE DB Destination    →  JdbcBatchItemWriter
  Delta Lake            →  Custom ItemWriter (Spark submit or Delta API)
  SSISDB Audit          →  JobExecutionListener + StepExecutionListener
  SCD Type 1            →  ItemProcessor + JdbcBatchItemWriter (MERGE SQL)
  SCD Type 2            →  ItemProcessor (close/open rows) + JdbcBatchItemWriter

Best for: < 5M rows, complex business logic, existing Java/Spring teams
"""

from datetime import datetime


def _to_pascal_case(snake_str: str) -> str:
    return "".join(word.capitalize() for word in snake_str.split("_"))


def _to_camel_case(snake_str: str) -> str:
    parts = snake_str.split("_")
    return parts[0] + "".join(w.capitalize() for w in parts[1:])


class SpringBatchGenerator:
    """Generates Spring Batch pipeline code for various SSIS package types."""

    TEMPLATES = {
        "dimension_scd2": "Dimension — SCD Type 2 (history tracking)",
        "dimension_scd1": "Dimension — SCD Type 1 (overwrite)",
        "fact_load": "Fact Table Load (join + insert)",
        "flat_file_ingest": "Flat File Ingestion (CSV → DB/Delta)",
        "generic": "Generic Pipeline (blank template)",
    }

    @staticmethod
    def list_templates():
        return [
            {"id": k, "name": v}
            for k, v in SpringBatchGenerator.TEMPLATES.items()
        ]

    def generate(self, pipeline_type: str, details: dict, environment: str) -> dict:
        """Generate Spring Batch files. Returns a single combined file dict."""
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
            "name": f"{class_name}JobConfig.java",
            "language": "java",
            "content": code,
            "type": "pipeline",
        }

    # ── Header ─────────────────────────────────────────────

    def _header(self, d: dict) -> str:
        return f'''/*
 * ============================================================
 * Spring Batch Job: {d["pipeline_name"]}
 * ============================================================
 * Converted from: {d["ssis_package"]}
 * Description:    {d["description"]}
 * Author:         {d["author"]}
 * Date:           {datetime.now().strftime("%Y-%m-%d")}
 * JIRA:           {d["jira_ticket"]}
 *
 * SSIS → Spring Batch mapping:
 *   Package           →  @Bean Job
 *   Data Flow Task    →  @Bean Step (reader → processor → writer)
 *   OLE DB Source     →  JdbcPagingItemReader
 *   Derived Column    →  ItemProcessor
 *   OLE DB Dest       →  JdbcBatchItemWriter
 *   Error Output      →  SkipPolicy + error table writer
 *   SSISDB Audit      →  JobExecutionListener
 * ============================================================
 */
'''

    # ── Dimension SCD2 ─────────────────────────────────────

    def _dim_scd2(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        camel = _to_camel_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.batch.jobs;

import com.etl.batch.common.*;
import com.etl.batch.listeners.AuditJobListener;
import com.etl.batch.listeners.AuditStepListener;
import com.etl.batch.processors.Scd2Processor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.*;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.*;

@Configuration
@EnableBatchProcessing
public class {cls}JobConfig {{

    @Value("${{batch.chunk-size:1000}}")
    private int chunkSize;

    @Value("${{batch.skip-limit:100}}")
    private int skipLimit;

    // ── JOB (replaces SSIS Package) ─────────────────────

    @Bean
    public Job {camel}Job(
            JobRepository jobRepository,
            @Qualifier("{camel}Step") Step mainStep,
            AuditJobListener auditListener) {{

        return new JobBuilder("{d["pipeline_name"]}", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(auditListener)
                .start(mainStep)
                .build();
    }}

    // ── STEP (replaces SSIS Data Flow Task) ─────────────

    @Bean("{camel}Step")
    public Step mainStep(
            JobRepository jobRepository,
            PlatformTransactionManager txManager,
            @Qualifier("{camel}Reader") JdbcPagingItemReader<Map<String, Object>> reader,
            @Qualifier("{camel}Processor") ItemProcessor<Map<String, Object>, Map<String, Object>> processor,
            @Qualifier("{camel}Writer") JdbcBatchItemWriter<Map<String, Object>> writer,
            @Qualifier("errorWriter") JdbcBatchItemWriter<Map<String, Object>> errorWriter,
            AuditStepListener stepListener) {{

        return new StepBuilder("{d["pipeline_name"]}_step", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skipLimit(skipLimit)
                .skip(Exception.class)
                .listener(stepListener)
                .build();
    }}

    // ── READER (replaces SSIS OLE DB Source) ────────────

    @Bean("{camel}Reader")
    public JdbcPagingItemReader<Map<String, Object>> reader(
            @Qualifier("sourceDataSource") DataSource dataSource) throws Exception {{

        SqlPagingQueryProviderFactoryBean queryProvider =
                new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);

        // TODO: Replace with your source table and columns
        queryProvider.setSelectClause("SELECT *");
        queryProvider.setFromClause("FROM dbo.YourSourceTable");
        queryProvider.setWhereClause(
                "WHERE modified_date >= DATEADD(day, -3, GETDATE())");
        queryProvider.setSortKey("id");

        return new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("{camel}Reader")
                .dataSource(dataSource)
                .queryProvider(queryProvider.getObject())
                .pageSize(chunkSize)
                .rowMapper(new ColumnMapRowMapper())
                .build();
    }}

    // ── PROCESSOR (replaces SSIS Derived Column + SCD logic)

    @Bean("{camel}Processor")
    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(
            @Qualifier("targetDataSource") DataSource targetDs) {{

        // SCD2 processor: compares incoming vs existing,
        // closes old records and creates new versions
        return new Scd2Processor(
                targetDs,
                "silver.your_dimension_table",
                // TODO: Replace with your business keys
                List.of("business_key"),
                // TODO: Replace with your tracked columns
                List.of("col1", "col2", "col3")
        );
    }}

    // ── WRITER (replaces SSIS OLE DB Destination) ───────

    @Bean("{camel}Writer")
    public JdbcBatchItemWriter<Map<String, Object>> writer(
            @Qualifier("targetDataSource") DataSource dataSource) {{

        // TODO: Replace with your target table columns
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(\"\"\"
                    MERGE INTO silver.your_dimension_table AS target
                    USING (VALUES (:business_key, :col1, :col2, :col3,
                                   :effective_date, :end_date, :is_current))
                    AS source (business_key, col1, col2, col3,
                               effective_date, end_date, is_current)
                    ON target.business_key = source.business_key
                       AND target.is_current = 1
                    WHEN MATCHED AND (target.col1 <> source.col1
                                   OR target.col2 <> source.col2
                                   OR target.col3 <> source.col3)
                    THEN UPDATE SET
                        end_date = CAST(GETDATE() AS DATE),
                        is_current = 0
                    WHEN NOT MATCHED
                    THEN INSERT (business_key, col1, col2, col3,
                                 effective_date, end_date, is_current)
                         VALUES (source.business_key, source.col1,
                                 source.col2, source.col3,
                                 CAST(GETDATE() AS DATE),
                                 '9999-12-31', 1);
                    \"\"\")
                .itemSqlParameterSourceProvider(
                        new MapItemSqlParameterSourceProvider<>())
                .build();
    }}
}}
'''

    # ── Dimension SCD1 ─────────────────────────────────────

    def _dim_scd1(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        camel = _to_camel_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.batch.jobs;

import com.etl.batch.common.*;
import com.etl.batch.listeners.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.*;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.*;

@Configuration
@EnableBatchProcessing
public class {cls}JobConfig {{

    @Value("${{batch.chunk-size:1000}}")
    private int chunkSize;

    // ── JOB ─────────────────────────────────────────────

    @Bean
    public Job {camel}Job(
            JobRepository jobRepository,
            @Qualifier("{camel}Step") Step mainStep,
            AuditJobListener auditListener) {{

        return new JobBuilder("{d["pipeline_name"]}", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(auditListener)
                .start(mainStep)
                .build();
    }}

    // ── STEP ────────────────────────────────────────────

    @Bean("{camel}Step")
    public Step mainStep(
            JobRepository jobRepository,
            PlatformTransactionManager txManager,
            @Qualifier("{camel}Reader") JdbcPagingItemReader<Map<String, Object>> reader,
            @Qualifier("{camel}Processor") ItemProcessor<Map<String, Object>, Map<String, Object>> processor,
            @Qualifier("{camel}Writer") JdbcBatchItemWriter<Map<String, Object>> writer,
            AuditStepListener stepListener) {{

        return new StepBuilder("{d["pipeline_name"]}_step", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(stepListener)
                .build();
    }}

    // ── READER (replaces SSIS OLE DB Source) ────────────

    @Bean("{camel}Reader")
    public JdbcPagingItemReader<Map<String, Object>> reader(
            @Qualifier("sourceDataSource") DataSource dataSource) throws Exception {{

        SqlPagingQueryProviderFactoryBean queryProvider =
                new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);
        // TODO: Replace with your source table
        queryProvider.setSelectClause("SELECT *");
        queryProvider.setFromClause("FROM dbo.YourSourceTable");
        queryProvider.setSortKey("id");

        return new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("{camel}Reader")
                .dataSource(dataSource)
                .queryProvider(queryProvider.getObject())
                .pageSize(chunkSize)
                .rowMapper(new ColumnMapRowMapper())
                .build();
    }}

    // ── PROCESSOR (replaces SSIS Derived Column) ────────

    @Bean("{camel}Processor")
    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {{
        return item -> {{
            // TODO: Add your transformations
            item.put("load_timestamp", new java.sql.Timestamp(System.currentTimeMillis()));
            item.put("source_system", "SOURCE_DB");
            return item;
        }};
    }}

    // ── WRITER: SCD1 MERGE (replaces SSIS OLE DB Dest) ──

    @Bean("{camel}Writer")
    public JdbcBatchItemWriter<Map<String, Object>> writer(
            @Qualifier("targetDataSource") DataSource dataSource) {{

        // TODO: Replace with your target table and columns
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(\"\"\"
                    MERGE INTO silver.your_dimension_table AS target
                    USING (VALUES (:business_key, :col1, :col2, :load_timestamp))
                    AS source (business_key, col1, col2, load_timestamp)
                    ON target.business_key = source.business_key
                    WHEN MATCHED
                    THEN UPDATE SET
                        col1 = source.col1,
                        col2 = source.col2,
                        load_timestamp = source.load_timestamp
                    WHEN NOT MATCHED
                    THEN INSERT (business_key, col1, col2, load_timestamp)
                         VALUES (source.business_key, source.col1,
                                 source.col2, source.load_timestamp);
                    \"\"\")
                .itemSqlParameterSourceProvider(
                        new MapItemSqlParameterSourceProvider<>())
                .build();
    }}
}}
'''

    # ── Fact Load ──────────────────────────────────────────

    def _fact(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        camel = _to_camel_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.batch.jobs;

import com.etl.batch.common.*;
import com.etl.batch.listeners.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.*;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.*;

@Configuration
@EnableBatchProcessing
public class {cls}JobConfig {{

    @Value("${{batch.chunk-size:2000}}")
    private int chunkSize;

    // ── JOB ─────────────────────────────────────────────

    @Bean
    public Job {camel}Job(
            JobRepository jobRepository,
            @Qualifier("{camel}Step") Step mainStep,
            AuditJobListener auditListener) {{

        return new JobBuilder("{d["pipeline_name"]}", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(auditListener)
                .start(mainStep)
                .build();
    }}

    // ── STEP ────────────────────────────────────────────

    @Bean("{camel}Step")
    public Step mainStep(
            JobRepository jobRepository,
            PlatformTransactionManager txManager,
            @Qualifier("{camel}Reader") JdbcPagingItemReader<Map<String, Object>> reader,
            @Qualifier("{camel}Processor") ItemProcessor<Map<String, Object>, Map<String, Object>> processor,
            @Qualifier("{camel}Writer") JdbcBatchItemWriter<Map<String, Object>> writer,
            AuditStepListener stepListener) {{

        return new StepBuilder("{d["pipeline_name"]}_step", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(stepListener)
                .build();
    }}

    // ── READER: Joined query (replaces SSIS Merge Join) ──

    @Bean("{camel}Reader")
    public JdbcPagingItemReader<Map<String, Object>> reader(
            @Qualifier("sourceDataSource") DataSource dataSource) throws Exception {{

        SqlPagingQueryProviderFactoryBean queryProvider =
                new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(dataSource);

        // TODO: Replace — this join replaces the SSIS Merge Join component
        queryProvider.setSelectClause(\"\"\"
                SELECT d.detail_id, d.order_id, d.product_id,
                       d.quantity, d.unit_price, d.line_total,
                       p.customer_id, p.order_date, p.status
                \"\"\");
        queryProvider.setFromClause(\"\"\"
                FROM dbo.YourDetailTable d
                INNER JOIN dbo.YourPrimaryTable p ON d.order_id = p.order_id
                \"\"\");
        queryProvider.setSortKey("detail_id");

        return new JdbcPagingItemReaderBuilder<Map<String, Object>>()
                .name("{camel}Reader")
                .dataSource(dataSource)
                .queryProvider(queryProvider.getObject())
                .pageSize(chunkSize)
                .rowMapper(new ColumnMapRowMapper())
                .build();
    }}

    // ── PROCESSOR: Lookup surrogate keys + derive columns ──

    @Bean("{camel}Processor")
    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(
            @Qualifier("targetDataSource") DataSource targetDs) {{

        JdbcTemplate jdbc = new JdbcTemplate(targetDs);

        return item -> {{
            // Lookup dimension surrogate keys
            // (replaces SSIS Lookup Transform)
            // TODO: Replace with your dimension lookups
            // Long customerSk = jdbc.queryForObject(
            //     "SELECT customer_sk FROM dim_customer WHERE customer_id = ? AND is_current = 1",
            //     Long.class, item.get("customer_id"));
            // item.put("customer_sk", customerSk);

            // Derived columns (replaces SSIS Derived Column)
            item.put("source_system", "OLTP");
            item.put("load_timestamp",
                    new java.sql.Timestamp(System.currentTimeMillis()));

            return item;
        }};
    }}

    // ── WRITER: Append to fact table ────────────────────

    @Bean("{camel}Writer")
    public JdbcBatchItemWriter<Map<String, Object>> writer(
            @Qualifier("targetDataSource") DataSource dataSource) {{

        // TODO: Replace with your fact table columns
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(\"\"\"
                    INSERT INTO gold.your_fact_table
                        (detail_id, order_id, product_id, customer_id,
                         quantity, unit_price, line_total,
                         source_system, load_timestamp)
                    VALUES
                        (:detail_id, :order_id, :product_id, :customer_id,
                         :quantity, :unit_price, :line_total,
                         :source_system, :load_timestamp)
                    \"\"\")
                .itemSqlParameterSourceProvider(
                        new MapItemSqlParameterSourceProvider<>())
                .build();
    }}
}}
'''

    # ── Flat File Ingest ───────────────────────────────────

    def _flat_file(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        camel = _to_camel_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.batch.jobs;

import com.etl.batch.common.*;
import com.etl.batch.listeners.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.builder.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.*;

@Configuration
@EnableBatchProcessing
public class {cls}JobConfig {{

    @Value("${{batch.chunk-size:1000}}")
    private int chunkSize;

    @Value("${{file.input.path}}")
    private String inputFilePath;

    // ── JOB ─────────────────────────────────────────────

    @Bean
    public Job {camel}Job(
            JobRepository jobRepository,
            @Qualifier("{camel}Step") Step mainStep,
            AuditJobListener auditListener) {{

        return new JobBuilder("{d["pipeline_name"]}", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(auditListener)
                .start(mainStep)
                .build();
    }}

    // ── STEP ────────────────────────────────────────────

    @Bean("{camel}Step")
    public Step mainStep(
            JobRepository jobRepository,
            PlatformTransactionManager txManager,
            @Qualifier("{camel}Reader") FlatFileItemReader<Map<String, Object>> reader,
            @Qualifier("{camel}Processor") ItemProcessor<Map<String, Object>, Map<String, Object>> processor,
            @Qualifier("{camel}Writer") JdbcBatchItemWriter<Map<String, Object>> writer,
            AuditStepListener stepListener) {{

        return new StepBuilder("{d["pipeline_name"]}_step", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, txManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skipLimit(50)
                .skip(Exception.class)
                .listener(stepListener)
                .build();
    }}

    // ── READER (replaces SSIS Flat File Source) ─────────

    @Bean("{camel}Reader")
    public FlatFileItemReader<Map<String, Object>> reader() {{

        // TODO: Replace with your CSV column names
        String[] columnNames = {{"col1", "col2", "col3", "col4"}};

        return new FlatFileItemReaderBuilder<Map<String, Object>>()
                .name("{camel}Reader")
                .resource(new FileSystemResource(inputFilePath))
                .linesToSkip(1) // skip header
                .delimited()
                .delimiter(",")
                .names(columnNames)
                .fieldSetMapper(fieldSet -> {{
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (String col : columnNames) {{
                        map.put(col, fieldSet.readString(col));
                    }}
                    return map;
                }})
                .build();
    }}

    // ── PROCESSOR (replaces SSIS Data Conversion + Derived Column)

    @Bean("{camel}Processor")
    public ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {{
        return item -> {{
            // TODO: Add type conversions and derived columns
            item.put("source_file", inputFilePath);
            item.put("load_timestamp",
                    new java.sql.Timestamp(System.currentTimeMillis()));
            return item;
        }};
    }}

    // ── WRITER ──────────────────────────────────────────

    @Bean("{camel}Writer")
    public JdbcBatchItemWriter<Map<String, Object>> writer(
            @Qualifier("targetDataSource") DataSource dataSource) {{

        // TODO: Replace with your target table and columns
        return new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(\"\"\"
                    INSERT INTO bronze.your_table
                        (col1, col2, col3, col4, source_file, load_timestamp)
                    VALUES
                        (:col1, :col2, :col3, :col4,
                         :source_file, :load_timestamp)
                    \"\"\")
                .itemSqlParameterSourceProvider(
                        new MapItemSqlParameterSourceProvider<>())
                .build();
    }}
}}
'''

    # ── Generic ────────────────────────────────────────────

    def _generic(self, d: dict, env: str) -> str:
        cls = _to_pascal_case(d["pipeline_name"])
        camel = _to_camel_case(d["pipeline_name"])
        return self._header(d) + f'''
package com.etl.batch.jobs;

import com.etl.batch.listeners.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.*;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.*;

@Configuration
@EnableBatchProcessing
public class {cls}JobConfig {{

    @Value("${{batch.chunk-size:1000}}")
    private int chunkSize;

    // ── JOB ─────────────────────────────────────────────

    @Bean
    public Job {camel}Job(
            JobRepository jobRepository,
            @Qualifier("{camel}Step") Step mainStep,
            AuditJobListener auditListener) {{

        return new JobBuilder("{d["pipeline_name"]}", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(auditListener)
                .start(mainStep)
                .build();
    }}

    // ── STEP ────────────────────────────────────────────

    @Bean("{camel}Step")
    public Step mainStep(
            JobRepository jobRepository,
            PlatformTransactionManager txManager,
            AuditStepListener stepListener) {{

        return new StepBuilder("{d["pipeline_name"]}_step", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize, txManager)
                // TODO: 1. Add reader  (.reader(...))
                // TODO: 2. Add processor (.processor(...))
                // TODO: 3. Add writer  (.writer(...))
                .listener(stepListener)
                .build();
    }}

    // TODO: Define @Bean reader, processor, writer
}}
'''
