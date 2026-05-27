## AWS Glue & PySpark ETL Pipelines

This repository contains an enterprise-grade suite of AWS Glue jobs developed with PySpark and the AWS SDK (`boto3`). These pipelines implement robust data engineering practices, including data lake ingestion patterns, incremental loading strategies, and third-party vendor data integration.

---

### Pipelines Overview

#### 1. Product Catalog Ingestion & Standardization
* **Objective:** Developed an automated data lake ingestion pipeline to process and standardize incoming product master data from flat files.
* **Technical Details:** Leveraged AWS Glue and PySpark to dynamically list and read raw CSV files directly from the root of an Amazon S3 bronze bucket using `boto3` pagination.
* **Data Processing:** Implemented robust schema mapping to rename and align incoming fields with the corporate data warehouse schema, applied data quality checks via duplicate removal (`dropDuplicates`), and appended audit metadata (`ingestion_timestamp`).
* **Storage & Archiving:** Wrote the cleaned data into AWS Redshift using Glue DynamicFrames and handled post-processing file orchestration by archiving processed files into a designated `DONE/` S3 prefix to maintain a clean landing zone.

#### 2. Incremental RFID Cargo Shipments ETL
* **Objective:** Built a high-performance incremental ETL pipeline to synchronize real-time logistic and RFID tracking data from a relational database to an enterprise data warehouse.
* **Technical Details:** Developed a hybrid AWS Glue job connecting an operational MySQL database (RDS) to an AWS Redshift data warehouse using JDBC connections.
* **Incremental Logic:** Implemented an optimized high-watermark query (`SELECT MAX(date)`) on Redshift to dynamically identify the last ingested record. The pipeline extracts only new logs from the source database, drastically reducing network I/O and processing times.
* **Transformation & Loading:** Performed PySpark data type casting (converting IDs to `long`) and time-string standardization (`HH:mm:ss`) before loading the incremental delta into the target analytical layer using an S3 temporary directory for staging.

#### 3. Automated Third-Party Vendor Data Processing
* **Objective:** Designed an end-to-end multi-file pipeline to ingest, parse, and structure daily logistical data and depot master data provided by external wholesale suppliers.
* **Technical Details:** Orchestrated a PySpark architecture to simultaneously handle transactional data (missing items logs) and dimensional data (depot master data catalogs) from decoupled S3 paths.
* **Data Engineering Techniques:** Utilized advanced Spark SQL functions (`regexp_extract`, `element_at`, and `split`) to isolate specific date-time patterns from filenames, turning raw file metadata into partition-ready timestamp columns. Implemented string cleaning via `trim` on critical lookup keys (`depot_id`, `product_code`) to ensure referential integrity.
* **Data Warehousing & File Management:** Separately transformed and loaded both datasets into distinct target tables within AWS Redshift. Automated the post-ingestion cleanup phase by using the AWS SDK (`boto3`) to securely move raw vendor files into an analytical archive folder upon successful execution.

---

### Core Tech Stack
* **Languages:** Python, Spark SQL
* **Frameworks & Libraries:** PySpark, AWS Glue Library, Boto3
* **Cloud Infrastructure (AWS):** S3 (Bronze/Staging Layers), AWS Glue (Serverless Spark Engine), Amazon Relational Database Service (RDS MySQL), Amazon Redshift (Data Warehouse)
