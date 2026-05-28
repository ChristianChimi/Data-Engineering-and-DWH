import sys
import boto3
import fnmatch
from pyspark.context import SparkContext
from pyspark.sql import functions as F
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

bucket = "enterprise-datalake-product-masterdata"
tmp_bucket = "enterprise-glue-temporary-storage"
dest_prefix = "ARCHIVED_BATCHES/"
pattern = "*product_catalog_masterdata.csv"

s3 = boto3.client("s3")
paginator = s3.get_paginator('list_objects_v2')

pages = paginator.paginate(Bucket=bucket, Delimiter='/')

file_exists = False

for page in pages:
    if "Contents" in page:
        for obj in page["Contents"]:
            key = obj["Key"]
            if fnmatch.fnmatch(key, pattern):
                file_exists = True
                break

if file_exists:
    print("Target file detected in root directory. Initiating Spark processing pipeline...")
    source_path = f"s3://{bucket}/{pattern}"

    df_catalog = (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv(source_path)
        .select(
            "vendor_sku",
            "prod_descr",
            "prod_cat_1",
            "prod_cat_2",
            "prod_cat_3",
            "prod_cat_4",
            "prod_cat_5",
            "gender",
            "utilizer",
            "age_range"
        )
        .withColumn("filename", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )

    column_mapping = {
        'vendor_sku': 'product_code',
        'prod_descr': 'product_description',
        'prod_cat_1': 'product_category_l1',
        'prod_cat_2': 'product_category_l2',
        'prod_cat_3': 'product_category_l3',
        'prod_cat_4': 'product_category_l4',
        'prod_cat_5': 'product_category_l5'
    }

    df_catalog_renamed = df_catalog.select([
        F.col(c).alias(column_mapping[c]) if c in column_mapping else F.col(c) 
        for c in df_catalog.columns
    ])

    df_catalog_final = df_catalog_renamed.withColumn('record_date', F.current_timestamp())
    df_clean = df_catalog_final.dropDuplicates()

    file_list = [row["filename"] for row in df_clean.select("filename").distinct().collect()]

    dyf_catalog = DynamicFrame.fromDF(
        dataframe=df_clean.drop("filename"),
        glue_ctx=glueContext,
        name="dyf_catalog"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_catalog,
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": "Redshift Production JDBC Connection",
            "dbtable": "layer1_staging.stg_product_catalog_masterdata",
            "redshiftTmpDir": f"s3://{tmp_bucket}/tmp/glue_redshift_integration/",
            "preactions": "TRUNCATE TABLE layer1_staging.stg_product_catalog_masterdata;"
        },
        transformation_ctx="write_redshift_catalog"
    )

    for filename in file_list:
        source_key = filename
        dest_key = f"{dest_prefix}{filename}"

        try:
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": source_key},
                Key=dest_key
            )
            s3.delete_object(Bucket=bucket, Key=source_key)
            print(f"File successfully archived: {source_key} -> {dest_key}")
        except Exception as e:
            print(f"Failed to archive file {source_key}: {str(e)}")

else:
    print("No matching files found in the root directory. Skipping execution batch.")

job.commit()
