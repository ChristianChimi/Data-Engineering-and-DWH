# Cross-Database Incremental Telemetry & RFID Data Pipeline

## Project Overview
This repository contains a high-performance, enterprise-grade incremental ETL pipeline built using **AWS Glue** and **PySpark**. The script implements a change data capture (CDC) logic over decoupled databases by synchronizing transaction and hardware scanning logs (such as RFID tag telemetry) from a relational operational MySQL instance directly into an **Amazon Redshift** data warehouse analytical layer.

The architecture solves the specific challenge of distributed temporal state management across heterogeneous database systems by extracting, parsing, and pushing down string-concatenated datetime checkpoints to optimize network I/O.

---

## Architecture Highlights
* **Distributed State Tracking:** Implements an efficient transactional sync strategy by programmatically querying the target analytical layer (`sampleQuery`) to extract the max parsed execution timestamp prior to the initialization of the upstream compute cluster.
* **Serverless Query Push-Down:** Leverages AWS Glue JDBC `whereClause` push-downs to execute a deterministic date-time string concatenation (`CONCAT(date, ' ', RIGHT(time, 8))`) directly at the transactional MySQL database engine layer, isolating historical data and minimizing overall network overhead.
* **Complex Attribute Sanitation:** Addresses specific datatype anomalies (such as dummy Unix epoch date prefixes automatically appended to standalone time strings by specific connectors) by utilizing distributed Spark SQL string parsing operations (`RIGHT(col, 8)`) to enforce data schema and type structural alignment.
* **High-Throughput Append Strategy:** Converts native PySpark DataFrames into AWS Glue DynamicFrames to leverage internal, parallelized multi-part loading drivers, routing clean analytical records directly into the Redshift target warehouse in transactional `APPEND` mode.

---

## Tech Stack
* **Language:** Python 3
* **Distributed Compute:** Apache Spark (PySpark Core & Spark SQL)
* **Serverless Orchestration:** AWS Glue (Serverless Spark Engine)
* **Databases Linked:** MySQL (Relational RDS) & Amazon Redshift (Columnar Data Warehouse via JDBC Connections)
* **Storage:** Amazon S3 (Temporary Bulk Processing Directory / Staging Layer)

---

## Production Pipeline Script

```python
import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Environment Configurations ---
SOURCE_DATABASE = "generic_operational_db"
SOURCE_TABLE_NAME = "telemetry_tag_logs"

TARGET_TABLE = "core_analytics_layer1.rfid_package_shipments"
TEMP_DIR = "s3://generic-enterprise-data-lake-raw/tmp/glue_redshift_staging/"
MYSQL_CONNECTION = "RDS_MySQL_Production_JDBC_Connection"
REDSHIFT_CONNECTION = "Redshift_Production_JDBC_Connection"

# --- STEP 1: Compute Execution Checkpoint from Analytical target Warehouse ---
print("Calculating maximum state string (MAX(date + time)) from Amazon Redshift...")
query_redshift = f"""
    SELECT CAST(MAX(CAST(date AS VARCHAR) || ' ' || CAST(time AS VARCHAR)) AS VARCHAR) as max_timestamp
    FROM {TARGET_TABLE}
"""

dyf_redshift = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": REDSHIFT_CONNECTION,
        "sampleQuery": query_redshift,
        "redshiftTmpDir": TEMP_DIR
    }
)

df_redshift = dyf_redshift.toDF()
row = df_redshift.collect()[0]
max_timestamp = row["max_timestamp"]

# --- STEP 2: Configure Push-down Query Filters for Target Source ---
connection_options = {
    "useConnectionProperties": "true",
    "connectionName": MYSQL_CONNECTION,
    "dbTable": f"{SOURCE_DATABASE}.{SOURCE_TABLE_NAME}",
    "database": SOURCE_DATABASE
}

if max_timestamp is not None:
    # Programmatically filter out ancient history using push-down queries to extract purely newer data
    where_clause = f"CONCAT(date, ' ', RIGHT(time, 8)) > '{max_timestamp}'"
    connection_options["whereClause"] = where_clause
    print(f"Incremental query push-down applied to MySQL engine: {where_clause}")
else:
    print("Zero state checkpoints identified. Initiating comprehensive table execution.")

# --- STEP 3: Ingest Incremental Payload via Distributed Compute Drivers ---
dyf_mysql = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=connection_options
)
df_filtered = dyf_mysql.toDF()

# --- STEP 4: Data Cleansing & Timestamp Structural Normalization ---
df_transformed = df_filtered.withColumn("id", col("id").cast("long"))

# Isolate clean time structures by stripping dummy system epochs added during relational conversion
df_transformed = df_transformed.withColumn("time_str", col("time").cast("string"))
df_transformed = df_transformed.withColumn("time", F.expr("RIGHT(time_str, 8)")).drop("time_str")

record_count = df_transformed.count()
print(f"Total delta records identified and staged for warehousing: {record_count}")

# --- STEP 5: Transactional Append Processing into Target Layer ---
if record_count > 0:
    print(f"Executing transactional load of {record_count} records...")
    dyf_target = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_target")

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_target,
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": REDSHIFT_CONNECTION,
            "dbtable": TARGET_TABLE,
            "redshiftTmpDir": TEMP_DIR
        },
        transformation_ctx="write_redshift_incremental"
    )
    print("Incremental warehouse target synchronization complete.")
else:
    print("Zero delta updates discovered beyond target watermark checkpoint. Ending job execution.")

job.commit()
print("Serverless state synchronization script terminated successfully.")
