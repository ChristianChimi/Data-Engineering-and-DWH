import sys, json, requests, boto3
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import explode, col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_client = boto3.client('s3')

S3_BUCKET_RAW_STORICO = "generic-enterprise-data-lake-raw"
BASE_URL = "https://api.vendor-logistics-platform.internal"
headers = {
    "X-Api-Key": "AM_GENERIC_ALPHANUMERIC_API_KEY_PLACEHOLDER_NDA_COMPLIANT",
    "Content-Type": "application/json"
}

CONNECTION_NAME = "Redshift_Production_JDBC_Connection"
TARGET_SCHEMA = "core_logistics_layer1"
REDSHIFT_TMP_DIR = "s3://generic-enterprise-data-lake-raw/tmp/glue_redshift_staging/"

data_partenza = datetime.now()
timestamp_run = data_partenza.strftime('%H%M%S')
finestra_giorni = [(data_partenza - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

print(f"Starting Glue Job - 7-day rolling synchronization window: {finestra_giorni}\n")

list_shipments_raw, list_pallets_raw = [], []

for data_target in finestra_giorni:
    dt = datetime.strptime(data_target, "%Y-%m-%d")
    partition_path = f"year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"

    url_sp = f"{BASE_URL}/api/v1/export/shipments?date={data_target}"
    res_sp = requests.get(url_sp, headers=headers, timeout=30)
    if res_sp.status_code == 200:
        raw_json_sp = res_sp.json()
        items_sp = raw_json_sp.get('items', [])
        list_shipments_raw.extend(items_sp)
        print(f"Shipments Endpoint [{data_target}]: retrieved {len(items_sp)} records")
        if items_sp:
            s3_client.put_object(
                Bucket=S3_BUCKET_RAW_STORICO,
                Key=f"raw_archive/shipments/{partition_path}/shipments_{data_target}_{timestamp_run}.json",
                Body=json.dumps(raw_json_sp, ensure_ascii=False), ContentType='application/json'
            )
    else:
        print(f"Shipments Endpoint [{data_target}]: API Error (Status {res_sp.status_code})")
        
    url_ba = f"{BASE_URL}/api/v1/export/pallets?date={data_target}"
    res_ba = requests.get(url_ba, headers=headers, timeout=30)
    if res_ba.status_code == 200:
        raw_json_ba = res_ba.json()
        items_ba = raw_json_ba.get('items', [])
        list_pallets_raw.extend(items_ba)
        print(f"Pallets Endpoint   [{data_target}]: retrieved {len(items_ba)} records")
        if items_ba:
            s3_client.put_object(
                Bucket=S3_BUCKET_RAW_STORICO,
                Key=f"raw_archive/pallets/{partition_path}/pallets_{data_target}_{timestamp_run}.json",
                Body=json.dumps(raw_json_ba, ensure_ascii=False), ContentType='application/json'
            )
    else:
        print(f"Pallets Endpoint   [{data_target}]: API Error (Status {res_ba.status_code})")

print(f"\nTotal accumulated records in memory -> Shipments: {len(list_shipments_raw)} | Pallets: {len(list_pallets_raw)}")
print("\nInitializing distributed Spark DataFrame processing and schema normalization...")

df_shipments_master, df_shipments_details, df_pallets_master, df_pallets_details = None, None, None, None

if list_shipments_raw:
    rdd_sp = spark.sparkContext.parallelize([json.dumps(i) for i in list_shipments_raw])
    df_sp_raw = spark.read.json(rdd_sp)
    
    df_shipments_master = df_sp_raw.select(
        col("header.transactionId").alias("transaction_id"), col("header.documentDate").alias("document_date"),
        col("header.carrierName").alias("carrier_name"), col("header.destinationName").alias("destination_name"),
        col("header.city").alias("city"), col("header.district").alias("district"),
        col("header.deliveryStatus").alias("delivery_status"), col("header.expectedPackages").alias("expected_packages"), 
        col("header.receivedPackages").alias("received_packages")
    ).filter(col("transaction_id").isNotNull()).dropDuplicates(["transaction_id"])
    
    df_sp_details_flat = df_sp_raw.withColumn("package", explode(col("packages")))
    df_shipments_details = df_sp_details_flat.select(
        col("header.transactionId").alias("transaction_id"), col("package.packageId").alias("package_id"),
        col("package.billingCode").alias("billing_code"), col("package.isRfidActive").alias("is_rfid_active"),
        col("package.carrierOutcome").alias("carrier_outcome"), col("package.deliveryTimestamp").alias("delivery_timestamp")
    ).filter(col("package_id").isNotNull()).dropDuplicates(["package_id"])

if list_pallets_raw:
    rdd_ba = spark.sparkContext.parallelize([json.dumps(b) for b in list_pallets_raw])
    df_ba_raw = spark.read.json(rdd_ba)
    
    df_pallets_master = df_ba_raw.select(
        col("palletId").alias("pallet_id"), col("palletCode").alias("pallet_code"),
        col("status").alias("pallet_status"), col("targetDate").alias("target_date"),
        col("carrierName").alias("carrier_name"), col("packagesCount").alias("packages_count"),
        col("createdBy").alias("created_by"), col("createdAt").alias("created_at")
    ).filter(col("pallet_id").isNotNull()).dropDuplicates(["pallet_id"])
    
    df_ba_details_flat = df_ba_raw.withColumn("package", explode(col("packages")))
    df_pallets_details = df_ba_details_flat.select(
        col("palletId").alias("pallet_id"), col("package.packageId").alias("package_id"),
        col("package.transactionId").alias("transaction_id_ref"), col("package.packageNumber").alias("package_number"),
        col("package.operatorId").alias("operator_id"), col("package.sortingLine").alias("sorting_line"),
        col("package.gridPosition").alias("grid_position"), col("package.scannedAt").alias("scanned_at"),
        col("package.hasAmbiguousMatch").alias("has_ambiguous_match")
    ).filter(col("package_id").isNotNull()).dropDuplicates(["pallet_id", "package_id"])

print("Relational DataFrames normalized. Starting staging process...")

def write_staging_job(df, table_name):
    if df is None:
        print(f"Table {table_name}: DataFrame is null. Writing operation skipped.")
        return
        
    row_count = df.count()
    if row_count == 0:
        print(f"Table {table_name}: 0 rows filtered, no insertion needed.")
        return

    staging_table = f"{TARGET_SCHEMA}.{table_name}_staging"
    print(f"Executing TRUNCATE and bulk loading into {staging_table} ({row_count} rows)...")
    
    dynamic_frame_raw = DynamicFrame.fromDF(df, glueContext, "dynamic_frame_raw")
    dynamic_frame_to_write = dynamic_frame_raw.resolveChoice(choice='make_cols')
    
    pre_query = f"TRUNCATE TABLE {staging_table};"
    
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_to_write,
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true", 
            "connectionName": CONNECTION_NAME, 
            "dbtable": staging_table,
            "preactions": pre_query, 
            "redshiftTmpDir": REDSHIFT_TMP_DIR
        },
        transformation_ctx=f"write_rs_{table_name}"
    )
    print(f"Staging write completed successfully for {staging_table}!\n")

write_staging_job(df_shipments_master, "shipments_master")
write_staging_job(df_shipments_details, "shipments_details")
write_staging_job(df_pallets_master, "pallets_master")
write_staging_job(df_pallets_details, "pallets_details")

job.commit()
