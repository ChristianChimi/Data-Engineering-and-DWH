# Enterprise Cloud Data Engineering & DWH Optimization (AWS, PySpark and SQL)

This repository showcases anonymized data engineering projects inspired by production environments. It includes PySpark ETL pipelines running on AWS Glue, incremental ingestion strategies, Redshift performance optimization and Data Warehouse refactoring following Medallion architecture principles.

---

### 📂 Repository Structure

```text
├── pipelines/
│   ├── product_catalog_ingestion.py   # Data lake ingestion & master data quality (PySpark)
│   ├── data_ingestion_from_API.py      # Ingestion from API to DWH
│   ├── rfid_shipments_incremental.py  # High-watermark incremental ETL (RDS to Redshift)
│   └── supplier_missing_products.py   # Multi-file vendor integration & string parsing
│   └── stored_procedure.sql           # Stored procedure to update supplier missing products historic table
├── dwh_refactoring/
│   ├── logistic_schema_DWH.png        # Lineage and transformation architecture blueprint
│   ├── stored_procedure_upsert.sql    # Atomic partition swapping & hybrid incremental PL/pgSQL
│   ├── mvw_sales_orders_extended.sql  # Layer 2 consumption layer engineered as a Materialized View
│   └── README.md                      # Detailed Performance Engineering documentation (>2m to ~5s)
├── requirements.txt                   # Environment dependency mappings
└── README.md                          # Global repository overview
