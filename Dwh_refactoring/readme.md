# Cloud Data Warehouse Refactoring & Performance Engineering

This directory contains the core implementation of an enterprise-grade Data Warehouse (DWH) refactoring project engineered on **Amazon Redshift**, focusing on query speed optimization, structural simplification, and Data Product modeling.

### The Core Achievement: From >2 Minutes to ~5 Seconds
* **The Performance Bottleneck:** Core business and logistics reporting dashboards suffered from severe latency, frequently exceeding execution times of **over 2 minutes** per single refresh. This crippling slowdown was driven by a legacy architecture heavily reliant on **multiple layers of nested, complex views** that forced the query engine to reconstruct, join, and aggregate millions of raw rows on the fly at runtime.
* **The Refactored Solution:** Eradicated the nested view layers by introducing a structured three-tier physical pipeline (Layer 1 ──> Layer 1.5 ──> Layer 2). The transformation logic was shifted upstream into high-performance stored procedures, and the analytical layer was fully pre-computed and persisted.
* **The Impact:** Slashed end-user dashboard loading times down to **approximately 5 seconds** (achieving an **over 90% performance optimization**), providing the business with near-instantaneous analytical capabilities.

---

### Shift to a "Data Product" Architecture
Previously, the logistics report had to query and scan thousands of disparate tables and operational schemas simultaneously, creating heavy locks and analytical drag. 

With this refactoring, the logistics dashboard now consumes a streamlined, standalone **Data Product**. All raw operational tables, cross-system unions, and complex mapping rules are encapsulated directly within the data platform. The reporting layer no longer handles heavy computations; it simply queries fully conformed, production-ready, and business-focused analytical assets.

---

### 📂 Directory Components

* **`logistic_schema_DWH.png`**: The structural blueprint showing the data lineage and physical transformation matrix from raw, cryptic staging schemas into high-performance conformed entities.
* **`stored_procedure_upsert.sql`**: A production-ready PL/pgSQL stored procedure executing a hybrid ETL pattern. It handles real-time data deduplication (`ROW_NUMBER()`) and processes an atomic partition swap for active data alongside an incremental anti-join for historical logs.
* **`mvw_sales_orders_extended.sql`**: The Layer 2 consumption layer built as a **Redshift Materialized View**. This file represents the core optimization: it completely replaces the legacy nested views by physically caching multi-channel sales attributions, string concatenations, and time-intelligence metrics directly on cluster storage.

---

### Redshift Hardware-Level Optimization Strategies

To lock in the sub-second data rendering and guarantee the stability of the Data Product, the following infrastructure properties were engineered:

1. **Elimination of Nested Views via Materialization Caching**
   * **Mechanism:** Rather than resolving intricate business logic, text parsing, and surrogate key generation every time a user opens a report, the entire query tree is compiled and physically saved to disk via a Materialized View.
   * **Result:** Dashboard interaction times dropped entirely, as BI tools now query static, highly indexed pre-computed records instead of triggering unpredictable cascade executions of nested queries.

2. **Co-Located Data Distribution (`DISTSTYLE KEY`)**
   * **Mechanism:** Tables within the Conformed Core (Layer 1.5) and Marts (Layer 2) that share frequent join paths (e.g., Sales Lines and Customer Masterdata) were aligned on the same cluster slices using `customer_code` as the explicit `DISTKEY`.
   * **Result:** Eliminated expensive network data redistribution (shuffling) across nodes, keeping joins entirely local and parallelized inside individual slices.

3. **Compound Sorting (`COMPOUND SORTKEY`)**
   * **Mechanism:** Applied composite physical sorting prioritized by time-intelligence columns and filtering dimensions (e.g., `COMPOUND SORTKEY (record_timestamp, branch_code)`).
   * **Result:** Enabled the Redshift query planner to leverage zone maps efficiently, completely skipping storage blocks that fall outside the reporting time windows and minimizing disk I/O.
