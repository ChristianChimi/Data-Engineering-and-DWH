# Enterprise Cloud Data Pipelines (AWS Glue & PySpark)

This repository serves as a production-ready portfolio demonstrating data engineering patterns leveraging serverless Apache Spark on AWS. It features end-to-end ETL pipelines designed for high scalability, data standardization, incremental loading strategies, and automated data lake file orchestration.

---

### 📂 Repository Structure

```text
├── Pipelines/
│   ├── product_catalog_ingestion.py   # Data lake ingestion & master data quality
│   ├── rfid_shipments_incremental.py  # High-watermark incremental ETL (RDS to Redshift)
│   └── supplier_missing_products.py   # Multi-file vendor integration & string parsing
├── README.md                          # Core documentation
└── requirements.txt                   # Dependency mappings
