# Product OOS & Supplier Depot Masterdata Ingestion Pipeline

## Project Overview
This repository contains an enterprise-grade, serverless ETL pipeline built using **AWS Glue** and **PySpark**. The script automates the dual ingestion of third-party wholesaler data: product out-of-stock (OOS/missing) records and supplier depot masterdata mapping. 

The architecture features strict schema enforcement, metadata timestamp parsing from file names using regex, string data sanitation via distributed trimming, and atomic post-processing object migration to maintain an un-cluttered S3 data lake layer.

---

## Architecture Highlights
* **Dual Ingestion Engine:** Processes two distinct business entities (OOS logs and Depot masterdata) sequentially within a single high-performance serverless Spark execution.
* **Metadata-Driven Lineage:** Parses complex file structural naming conventions using regex patterns (`\d{8}_\d{6}`) to capture snapshot execution datetimes, ensuring data lineage alongside native system timestamps.
* **Strict Type & Schema Enforcement:** Implements programmatic `StructType` compilation layers to force datatype alignment on external masterdata files, preventing silent staging failures.
* **Data Sanitation Layer:** Leverages distributed PySpark SQL functions (`trim`) to eliminate trailing spaces or ingestion noise from core transactional primary keys (`minsan`, `id_deposito`) prior to database loading.
* **Transactional Archive Strategy:** Dynamically isolates individual processed file strings via `distinct().collect()`, performing programmatic atomic loop migrations (`copy_object` and `delete_object`) into a secure storage archive path upon successful Redshift commit.

---

## Tech Stack
* **Language:** Python 3
* **Distributed Compute:** Apache Spark (PySpark Core & Spark SQL)
* **Serverless Orchestration:** AWS Glue (Serverless Spark Engine)
* **SDKs & Libraries:** Boto3 (AWS SDK)
* **Data Warehousing:** Amazon Redshift (Columnar DW)
* **Storage:** Amazon S3 (Object Storage / Data Lake)

---

## Production Pipeline Script

```python
import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import trim
from pyspark.sql.types import StructType, StructField, StringType
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

s3 = boto3.client("s3")

bucket = "corporate-vendor-data-lake"
source_path = f"s3://{bucket}/wholesalers/supplier_a/missing_products_*.csv"
source_prefix = "wholesalers/supplier_a/"
dest_prefix = "wholesalers/supplier_a/DONE/"

# --- ENTITY 1: Missing Products (OOS Logs) Processing ---

df_mancanti = (
    spark.read 
    .option("header", "true") 
    .option("delimiter", ";") 
    .csv(source_path) 
    .withColumn("filename", F.element_at(F.split(F.input_file_name(), "/"), -1))
    .withColumn("file_datetime", F.regexp_extract("filename", r"(\d{8}_\d{6})", 1))
    .withColumn("record_timestamp", F.current_timestamp())
    .select(
        "id_deposito",
        "minsan",
        "filename",
        "record_timestamp",
        "file_datetime"
    )
    .orderBy(F.col("file_datetime").desc())
)

df_mancanti = df_mancanti.withColumn("id_deposito", trim(df_mancanti.id_deposito)) \
                         .withColumn("minsan", trim(df_mancanti.minsan))
       
dyf_mancanti = DynamicFrame.fromDF(
    df_mancanti.drop("file_datetime"),
    glueContext,
    "dyf_mancanti"
)       
        
glueContext.write_dynamic_frame.from_options(
    frame=dyf_mancanti,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "aws_redshift_warehouse_connection",
        "dbtable": "supplier_layer1.supplier_a_missing_products",
        "redshiftTmpDir": f"s3://{bucket}/tmp/glue_redshift/"
    },
    transformation_ctx="write_redshift_mancanti"
)

lista_file = (
    df_mancanti.select("filename")
    .distinct()
    .collect()
)

for row in lista_file:
    filename = row["filename"]
    source_key = f"{source_prefix}{filename}"
    dest_key = f"{dest_prefix}{filename}"

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key
    )
    s3.delete_object(Bucket=bucket, Key=source_key)
    print(f"Moved to DONE: {filename}")


# --- ENTITY 2: Depot Masterdata Mapping Processing ---

schema = StructType([
    StructField("cod_ministeriale", StringType(), True),
    StructField("id_deposito_principale", StringType(), True)
])

df_anag = (
    spark.read 
    .option("header", "true") 
    .option("delimiter", ";") 
    .schema(schema)
    .csv(f"s3://{bucket}/wholesalers/supplier_a/supplier_a_depot_masterdata_*.csv") 
    .withColumn("filename", F.element_at(F.split(F.input_file_name(), "/"), -1))
    .withColumn("file_datetime", F.regexp_extract("filename", r"(\d{8}_\d{6})", 1))
    .withColumn("record_timestamp", F.current_timestamp())
    .withColumnRenamed("cod_ministeriale", "codice_ministeriale")
    .withColumnRenamed("id_deposito_principale", "id_deposito")
    .orderBy(F.col("file_datetime").desc())
)

df_anag = df_anag.withColumn("id_deposito", trim(df_anag.id_deposito)) \
                 .withColumn("codice_ministeriale", trim(df_anag.codice_ministeriale))

lista_file_anag = [
    row["filename"]
    for row in df_anag.select("filename").distinct().collect()
]

df_final = df_anag.drop("file_datetime")
dyf = DynamicFrame.fromDF(df_final, glueContext, "dyf")

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "aws_redshift_warehouse_connection",
        "dbtable": "supplier_layer1.supplier_a_depot_masterdata",
        "redshiftTmpDir": f"s3://{bucket}/tmp/glue_redshift/"
    },
    transformation_ctx="write_redshift"
)

for filename in lista_file_anag:
    source_key = f"{source_prefix}{filename}"
    dest_key = f"{dest_prefix}{filename}"

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key
    )
    s3.delete_object(Bucket=bucket, Key=source_key)
    print(f"Moved: {filename}")

job.commit()
