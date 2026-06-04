## AWS Glue & PySpark ETL Pipelines

This repository contains an enterprise-grade suite of AWS Glue jobs developed with PySpark and the AWS SDK (`boto3`). These pipelines implement robust data engineering practices, including data lake ingestion patterns, incremental loading strategies, and third-party vendor data integration. Pipelines automatically run daily thanks to AWS Event Bridge schedules.

---

### Pipelines Overview

#### 1. Product Catalog Ingestion & Standardization
* **Objective:** Developed an automated data lake ingestion pipeline to process and standardize incoming product master data from flat files.
* **Technical Details:** Leveraged AWS Glue and PySpark to dynamically list and read raw CSV files directly from the root of an Amazon S3 bronze bucket using `boto3` pagination.
* **Data Processing:** Implemented robust schema mapping to rename and align incoming fields with the corporate data warehouse schema, applied data quality checks via duplicate removal (`dropDuplicates`), and appended audit metadata (`ingestion_timestamp`).
* **Storage & Archiving:** Wrote the cleaned data into AWS Redshift using Glue DynamicFrames and handled post-processing file orchestration by archiving processed files into a designated `DONE/` S3 prefix to maintain a clean landing zone.

#### 2. Cross-Database Incremental Telemetry Data Pipeline
* **Objective:** Developed an automated incremental ETL pipeline to synchronize high-volume IoT device and telemetry logs from an operational MySQL database into an Amazon Redshift data warehouse.
* **Technical Details:** Engineered a hybrid AWS Glue architecture utilizing PySpark and AWS Glue DynamicFrames to read concurrently across relational and analytical databases via decoupled JDBC network connections.
* **Data Engineering Techniques:** Implemented an efficient incremental loading strategy by executing push-down sample queries to fetch the maximum target timestamp, dynamically filtering incoming source transactions. Utilized advanced Spark SQL functions (`concat_ws`, `to_timestamp`, and `date_format`) to reconstruct unified datetime objects from split source attributes, ensuring schema consistency and data type alignment.
* **Data Warehousing & File Management:** Orchestrated transactional appending to Target Layer-1 analytical tables using AWS Glue’s S3-backed temporary staging directory configuration, optimizing data loading speeds while preventing duplicate record generation.
  
#### 3. Automated Vendor Product Masterdata Ingestion Pipeline
* **Objective:** Designed and implemented an automated data pipeline to ingest, clean, and structure daily product masterdata delivered by external third-party suppliers.
* **Technical Details:** Built a PySpark and AWS Glue architecture to scan decoupled S3 staging zones, dynamically detect inbound vendor files using `boto3` pagination, and isolate target CSV streams for high-throughput processing.
* **Data Engineering Techniques:** Utilized advanced Spark SQL functions (`regexp_extract`, `element_at`, and `split`) to parse and extract dynamic date-timestamp patterns directly from file metadata. Developed column-mapping layers to standardize external schemas, enforce consistent naming conventions, and guarantee structural integrity across heterogeneous product attributes.
* **Data Warehousing & File Management:** Transformed and safely loaded the cleaned data assets into core Amazon Redshift data warehouse staging tables via AWS Glue DynamicFrames. Engineered an automated post-ingestion cleanup mechanism using the AWS SDK to atomically move processed files into a secure analytical archive directory upon successful execution.
---

### Core Tech Stack
* **Languages:** Python, Spark SQL
* **Frameworks & Libraries:** PySpark, AWS Glue Library, Boto3
* **Cloud Infrastructure (AWS):** S3 (Bronze/Staging Layers), AWS Glue (Serverless Spark Engine), Amazon Relational Database Service (RDS MySQL), Amazon Redshift (Data Warehouse)
