# Data Quest Project: Process & Architecture Documentation

## Overview

This document captures the thought process, architectural decisions, trade-offs, and lessons learned from building the BLS Productivity and DataUSA Population data pipelines.

---

## Architecture

### Medallion Architecture Choice

**Why Bronze/Silver/Gold?**

I implemented separate pipelines for each data source (BLS and DataUSA) because the timing of data updates can be different. Both pipelines follow the same medallion architecture pattern:

* **Bronze Layer**: Exact replica of source data
  - Preserves raw data in its original form
  - Schema defined explicitly to handle BLS fixed-width format quirks (preserving leading zeros in code columns)
  - Added `load_timestamp` for auditability
  - Example: `brz_pr_data_0_current`, `brz_pr_data_1_alldata`, `brz_pr_series`

* **Silver Layer**: Transformation and cleansing
  - Applied business logic transformations (series_id parsing into 6 components)
  - Handled duplicate data between BLS "current" and "alldata" files using `.dropDuplicates()`
  - Enriched with derived columns (observation_date, observation_year, observation_quarter, period_type)
  - Used **AUTO CDC** for merge capability - enables upserts based on business keys
  - Example: `slv_pr_productivity`, `slv_population_nation`

* **Gold Layer**: Modeling for reporting needs
  - Dimensional model (star schema) optimized for analytics
  - Fact tables: `gold_fact_productivity`, `gold_fact_population`
  - Dimension tables: `gold_dim_sector`, `gold_dim_class`, `gold_dim_measure`, `gold_dim_period`, etc.
  - Surrogate keys (IDENTITY columns) for dimension tables
  - Foreign key joins from fact to dimensions
  - **Current implementation**: Full reload for demo simplicity
  - **Real-world implementation**: Would use merge/SCD patterns

### SQL vs PySpark Choice

**Primary Choice: PySpark**

I chose PySpark as the primary transformation language for several reasons:

1. **Test Cases**: Easier to build unit tests for transformations
2. **Metadata-driven approaches**: Can dynamically generate transformations based on metadata
3. **Reusability**: Utility functions (like `clean_column_names`, `bls_options`) can be shared across transformations
4. **Consistency**: Uniform pattern across all layers reduces cognitive load

**When I'd use SQL**: For Gold layer transformations, SQL can be simpler for straightforward aggregations or dimension lookups. However, I kept PySpark throughout this example for uniformity.

**Trade-off accepted**: Some transformations (like simple SELECT with WHERE clauses) might be more readable in SQL, but the benefits of PySpark's testability and reusability outweigh the verbosity.

### Safe Re-running & Idempotency

**Bronze Layer**: 
- Auto Loader in streaming mode handles incremental ingestion
- Re-running on the same files is safe - Auto Loader tracks processed files

**Silver Layer**: 
- Used **AUTO CDC** (Spark Declarative Pipeline feature) to enable merge behavior
- Business keys defined: `series_id + year + period` for productivity, `nation + year` for population
- `.dropDuplicates()` applied explicitly where source files overlap (BLS current vs alldata)
- Re-running is safe - records are upserted based on business keys

**Gold Layer**:
- **Demo implementation**: Full reload (simple `spark.read.table()` and write)
- **Production consideration**: Would implement merge/upsert logic using Delta MERGE or AUTO CDC
- **Dimension tables**: IDENTITY surrogate keys regenerate on full reload - not ideal for production
  - Real-world: Would use SCD Type 1 or Type 2 patterns with business key lookups

**Key Design Principle**: Each layer can be re-run independently without corrupting downstream data, thanks to Delta Lake's ACID guarantees and merge semantics.

---

## Trade-offs: Real Client vs Demo

This demo was built for learning and showcasing capabilities. Here's what would change for a real client deployment:

### 1. Operational Monitoring & Alerting

**Demo**: Basic pipeline expectations for data quality

**Real Client**:
- **Job notifications**: Trigger alerts (email, Slack, PagerDuty) for:
  - Pipeline failures
  - Time threshold exceeded (SLA violations)
  - Data quality expectation failures
- **Logging framework**: Implement structured logging throughout the pipeline
  - Track progress at each transformation stage
  - Log row counts, data quality metrics, processing times
  - Enable troubleshooting and performance optimization
- **Monitoring dashboard**: Build operational dashboard showing:
  - Pipeline run history and success rates
  - Data quality trends (expectation pass/fail over time)
  - Processing latency and throughput
  - Cost per run

### 2. Cost Optimization

**Demo considerations**:
- Used serverless compute (simplest option)
- Full reload on Gold layer
- No optimization of run frequency

**Real Client strategies**:
- **SLA analysis**: Work with client to understand data freshness requirements
  - If hourly updates aren't needed, run less frequently (daily/weekly)
  - BLS data updates quarterly - no need for sub-daily runs
- **Compute choice**: Evaluate compute vs serverless based on workload characteristics
  - Classic compute for predictable, long-running workloads
  - Serverless for bursty, unpredictable patterns
- **Pipeline pattern**: Assess notebook workflows vs Spark Declarative Pipelines
  - Declarative pipelines optimize for incremental processing
  - Notebook workflows give more control but require manual optimization
- **Incremental processing**: Move Gold layer to merge patterns instead of full reload
  - Reduces compute time and cost
  - Enables streaming architectures for near-real-time analytics

### 3. Metadata-Driven Framework

**Demo**: Hardcoded transformations

**Real Client**:
- Implement metadata framework to make the process dynamic:
  - Configuration tables defining source-to-target mappings
  - Transformation rules stored as metadata
  - Column mappings, data types, business rules externalized
  - Enables non-engineers to modify pipelines without code changes
  - Reduces maintenance overhead as sources evolve

### 4. Schema Drift Handling

**Demo**: Explicit schemas defined, no drift detection

**Real Client recommendation**:
- **Bronze Layer**: Allow schema evolution
  - Use `mergeSchema=true` or schema inference
  - Alert when new columns appear or types change
  - Preserve all source data even if unexpected
- **Silver Layer**: DO NOT automatically carry forward new columns
  - Schema drift from Bronze triggers alert but doesn't break pipeline
  - Engineer and client review and decide on appropriate action:
    - Add to transformation logic?
    - Ignore the new column?
    - Map to existing column?
- **Gold Layer**: Strict schema enforcement
  - Only carry forward columns that have been explicitly approved
  - Business logic defines what reaches the reporting layer

**Philosophy**: Fail open in Bronze (capture everything), fail closed in Gold (only approved data).

### 5. Data Volume & Performance

**Demo scale**: ~77K productivity records, ~11 population records per year

**Real Client at 100x-1000x volume**:

**Cluster sizing**:
- Right-size worker nodes based on data volume and transformation complexity
- Monitor memory usage - heavy aggregations or joins need more RAM
- This example has minimal compute/memory requirements, but watch for:
  - Large dimension table joins (broadcast hints may help)
  - Window functions (can be memory-intensive)
  - Explode operations on nested data

**Table optimization strategies**:
- **Liquid Clustering**: Databricks' auto-optimizing clustering strategy
  - Cluster on common filter columns (e.g., `observation_year`, `sector_code`)
  - Handles evolving access patterns automatically
- **Partition strategy**: If Liquid Clustering not available
  - Partition on date columns for time-series data
  - Avoid over-partitioning (< 1GB per partition)
- **Z-Ordering**: For queries with multiple filter predicates

**Streaming with Change Data Feed (CDF)**:
- Enable CDF on Silver and Gold tables
- Downstream pipelines read only changed records
- Eliminates need for complex watermark strategies
- Enables efficient incremental processing at scale
- Example: Gold fact table reads CDF from Silver, only processes new/updated records

**Philosophy**: Start simple, optimize based on observed performance bottlenecks.

### 6. Access Control

**Demo**: Single user, single catalog/schema

**Real Client multi-team environment**:
- **Unity Catalog governance**:
  - Separate catalogs or schemas for Bronze/Silver/Gold
  - Data engineers: write to Bronze/Silver, read from all
  - Analysts: read-only on Gold, no access to Bronze/Silver
  - Executives: read-only on Gold, through curated dashboards
- **Row-level security**: Filter sensitive records by region, customer, or other attributes
- **Column masking**: PII columns (if any) masked for non-privileged users
- **Audit logging**: Track who accessed what data and when

---

## Retrospective: What Was Hardest to Get Right

### Environment Challenges

**Biggest obstacle: Databricks Free Edition reliability**

The hardest part of this project was environment-based. Dealing with the Databricks Free edition cost a significant amount of time, as services would not work for a good portion of the time allocated to the project.

**Lesson learned**: If I started over, I would use the **trial version in a trial Azure or AWS environment** instead of the free edition. The investment in a trial account with full capabilities would have saved hours of troubleshooting and workarounds.

**Impact**: 
- Pipeline runs failing due to service unavailability, not code issues
- Unable to test certain features (e.g., cluster configurations, job scheduling)
- Difficulty distinguishing between "my code is wrong" vs "the service is down"

### Code Challenges

**Most challenging: Declarative pipeline syntax**

From a code perspective, the most challenging part was the declarative nature of **Spark Declarative Pipelines (formerly Delta Live Tables)**. It had been a few years since I last wrote a declarative pipeline, and the syntax required relearning:

- `@dp.table()` decorator patterns
- `@dp.expect()` vs `@dp.expect_or_fail()` for data quality
- AUTO CDC configuration and behavior
- Understanding when transformations execute (lazy evaluation)
- Debugging pipelines (limited visibility into intermediate steps)

**What helped**:
- Reviewing Databricks documentation and examples
- Iterative testing with small datasets
- Using `display()` in notebooks to validate logic before moving to pipeline files
- Adding logging and expectations to surface issues

### Other Notable Challenges

1. **BLS data quirks**:
   - Understanding that `Q05` means annual average, not a real quarter
   - Series_id format with trailing spaces (17 characters, not 17 visible characters)
   - Overlapping data between "current" and "alldata" files requiring deduplication
   - Parsing series_id into 6 components with specific substring positions

2. **Dimension surrogate key joins**:
   - Getting the left joins right in the fact table transformation
   - Handling cases where dimension lookups fail (NULL foreign keys)
   - Understanding IDENTITY column behavior across pipeline runs

3. **Bundle configuration**:
   - Parameterizing catalog/schema for multi-environment deployment
   - Understanding path resolution in `databricks.yml`
   - Configuring the glob patterns to include all transformation files

---

## Advice for Similar Projects

### Start Here:

1. **Invest in environment setup**: Don't compromise on tooling. Use a trial account with full capabilities rather than fighting free-tier limitations.

2. **Understand the source data deeply**: Spend time with sample files before coding. BLS data has nuances (Q05, trailing spaces, overlapping files) that aren't obvious until you dig in.

3. **Start simple, iterate**: 
   - Get Bronze working first (exact replica)
   - Add one transformation at a time in Silver
   - Build Gold incrementally, one dimension/fact at a time
   - Add data quality expectations after seeing what breaks

4. **Test outside the pipeline first**: Validate transformation logic in a notebook with `display()` before moving code into pipeline `.py` files. Debugging declarative pipelines is harder than debugging notebooks.

5. **Document as you go**: Capture "why" decisions while they're fresh. This PROCESS.md would have been harder to write weeks after finishing.

### One Thing to Be Careful About:

**Business key uniqueness and duplicate handling.**

The most subtle bug in this project was discovering duplicates between BLS source files. If I hadn't explicitly checked for duplicates, they would have silently propagated through Silver to Gold, breaking:
- Dashboard metrics (double-counting)
- Dimension joins (ambiguous matches)
- Time-series analysis (multiple values per time period)

Always:
- Identify the business key upfront
- Test for duplicates in Bronze
- Add `expect_or_fail` for uniqueness in Silver
- Use `.dropDuplicates()` explicitly when sources overlap

---

## Conclusion

This project demonstrates a production-ready pattern for ingesting, transforming, and modeling external data sources using Databricks' medallion architecture. While built as a demo with simplifications (full reload, single environment, minimal monitoring), the architecture scales to real client scenarios with the trade-offs outlined above.


---

**Project**: BLS Productivity & DataUSA Population Pipelines  
**Author**: Kristian Foster  
**Date**: July 2026
**Repository**: `quest_bundle/`