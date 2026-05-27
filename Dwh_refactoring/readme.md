# Cloud Data Warehouse Refactoring & Performance Engineering

This directory contains the core implementation of an enterprise-grade Data Warehouse (DWH) refactoring project engineered on **Amazon Redshift**. 

### Business & Engineering Impact
* **The Challenge:** Historical analytical models and core business reporting queries suffered from severe performance bottlenecks, frequently exceeding execution latencies of **over 2 minutes** per dashboard refresh. This was primarily caused by sub-optimal data distribution (heavy network shuffles across nodes), lack of dedicated physical sorting, and un-optimized multi-layered data modeling.
* **The Solution:** Redesigned the data ingestion architecture into a decoupled, progressive three-layer layout (Layer 1 ──> Layer 1.5 ──> Layer 2), enforcing strict schema alignments, targeted data deduplication via window functions, and hardware-level distribution optimizations.
* **The Impact:** Slashed core reporting query latency down to **approximately 5 seconds**, achieving an **over 90% performance optimization** across enterprise transactional workloads.

---

### 📂 Directory Components

* **`logistic_schema_DWH.png`**: The blueprint mapping architecture. A comprehensive data lineage and structural transformation matrix detailing how raw, cryptic staging structures transition into high-performance analytical conformed entities.
* **`stored_procedure_upsert.sql`**: A robust, production-ready PL/pgSQL stored procedure executing a hybrid ETL pattern for sales orders. It implements a full atomic swap for real-time active data partitions paired with a high-performance 7-column composite natural key anti-join for incremental historical appends.
* **`mvw_sales_orders_extended.sql`**: The Layer 2 analytical consumption layer implemented as a **Redshift Materialized View**. This physically persists the denormalized data structure on storage, caching complex conditional multi-channel sales attributions, string concatenations, and time-intelligence logic. By avoiding runtime execution of these heavy calculations during dashboard refreshes, it directly enables sub-second query rendering for end-user BI tools.

---

### Redshift Hardware-Level Optimization Strategies

To achieve sub-second data processing and avoid expensive network shuffles during heavy aggregation phases, the following engine properties were applied:

1. **Co-Located Data Distribution (`DISTSTYLE KEY`)**
   * **Mechanism:** Tables within the Conformed Core (Layer 1.5) and Analytical Marts (Layer 2) that are frequently joined together (e.g., Sales Lines, Fulfillment, and Customer Masterdata) were physically co-located on the same cluster slices by designating `customer_code` or `product_code` as the explicit `DISTKEY`.
   * **Result:** Eliminated the expensive redistribution step during query compilation, enabling local, parallelized joins inside single node slices.

2. **Compound Sorting (`COMPOUND SORTKEY`)**
   * **Mechanism:** Applied composite physical sorting structures prioritized by time-intelligence columns and core filtering criteria (e.g., `COMPOUND SORTKEY (record_timestamp, branch_code)`).
   * **Result:** Allowed the Redshift query planner to leverage zone maps effectively, completely skipping storage blocks that fall outside reporting intervals and drastically reducing disk I/O scan times.

3. **Materialized View Caching & Incremental Refresh**
   * **Mechanism:** Shuffled the heavy computational weight (surrogate key generation, multi-source unions, and text parsing for omni-channel tracking) away from the BI layer by pre-computing and persisting results into a Materialized View at the Marts level.
   * **Result:** Dashboard interaction times dropped significantly, as the BI tools query static, highly indexed pre-computed data structures instead of raw operational streams.

4. **Atomic Partition Swapping**
   * **Mechanism:** Utilized transient temporary tables (`CREATE TEMP TABLE ...`) to isolate, clean, and deduplicate window-ranked records (`ROW_NUMBER()`) before running explicit transaction-wrapped `DELETE` and `INSERT` blocks.
