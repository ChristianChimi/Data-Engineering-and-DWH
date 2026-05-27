# Enterprise Cloud Data Engineering & DWH Optimization (AWS, PySpark and SQL)

This repository serves as a production-ready portfolio demonstrating data engineering patterns leveraging serverless Apache Spark and high-performance Data Warehouse strategies on AWS. It features end-to-end ETL pipelines alongside a real-world Data Warehouse refactoring project that achieved significant query performance optimization.

---

### 📂 Repository Structure

```text
├── Pipelines/
│   ├── product_catalog_ingestion.py   # Data lake ingestion & master data quality
│   ├── rfid_shipments_incremental.py  # High-watermark incremental ETL (RDS to Redshift)
│   └── supplier_missing_products.py   # Multi-file vendor integration & string parsing
├── Dwh_refactoring/
│   └── redshift_table_optimization.sql # SQL DDL showcasing Distribution and Sort Key strategies
├── README.md                          # Core documentation
└── requirements.txt                   # Dependency mappings
