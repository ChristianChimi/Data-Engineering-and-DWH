# Enterprise Cloud Data Engineering & DWH Optimization (AWS, PySpark and SQL)

This repository serves as a production-ready portfolio demonstrating advanced data engineering patterns leveraging serverless Apache Spark and high-performance Data Warehouse strategies on AWS. It features robust, end-to-end ETL pipelines alongside a real-world Data Warehouse refactoring project focused on Performance Engineering, transitioning legacy legacy structures into business-ready Data Products.

---

### 📂 Repository Structure

```text
├── pipelines/
│   ├── product_catalog_ingestion.py   # Data lake ingestion & master data quality (PySpark)
│   ├── rfid_shipments_incremental.py  # High-watermark incremental ETL (RDS to Redshift)
│   └── supplier_missing_products.py   # Multi-file vendor integration & string parsing
├── dwh_refactoring/
│   ├── logistic_schema_DWH.png        # Lineage and transformation architecture blueprint
│   ├── stored_procedure_upsert.sql    # Atomic partition swapping & hybrid incremental PL/pgSQL
│   ├── mvw_sales_orders_extended.sql  # Layer 2 consumption layer engineered as a Materialized View
│   └── README.md                      # Detailed Performance Engineering documentation (>2m to ~5s)
├── requirements.txt                   # Environment dependency mappings
└── README.md                          # Global repository overview
