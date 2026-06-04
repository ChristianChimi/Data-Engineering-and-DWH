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

bucket = "company-data-lake-masterdata"
tmp_bucket = "company-sftp-temp-data-stage"
dest_prefix = "ARCHIVED_DATA/"
pattern = "*product_masterdata.csv"

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
    print("Target file found. Starting Spark processing...")

    source_path = f"s3://{bucket}/{pattern}"

    df_products = (
        spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv(source_path)
        .select(
            "product_id",
            "product_desc",
            "category_level_1",
            "category_level_2",
            "category_level_3",
            "category_level_4",
            "category_level_5",
            "gender",
            "target_user",
            "age_range"
        )
    )

    name_mapping = {
        'product_id': 'ext_product_code',
        'product_desc': 'ext_product_desc',
        'category_level_1': 'ext_product_category1',
        'category_level_2': 'ext_product_category2',
        'category_level_3': 'ext_product_category3',
        'category_level_4': 'ext_product_category4',
        'category_level_5': 'ext_product_category5'
    }

    df_products_renamed = df_products.select([
        F.col(c).alias(name_mapping[c]) if c in name_mapping else F.col(c)
         skate for c in df_products.columns
    ])

    df_products_final = df_products_renamed.withColumn(
        "filename",
        F.element_at(F.split(F.input_file_name(), "/"), -1)
    ).withColumn(
        "record_date",
        F.to_timestamp(
            F.to_date(
                F.regexp_extract("filename", r"(\d{8})", 1),
                "yyyyMMdd"
            )
        )
    )

    file_list = [row["filename"] for row in df_products_final.select("filename").distinct().collect()]

    dyf_products = DynamicFrame.fromDF(
        dataframe=df_products_final.drop("filename"),
        glue_ctx=glueContext,
        name="dyf_products"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=dyf_products,
        connection_type="redshift",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": "Redshift Production JDBC Connection",
            "dbtable": "dw_stage.product_masterdata_core",
            "redshiftTmpDir": f"s3://{tmp_bucket}/tmp/glue_redshift/"
        },
        transformation_ctx="write_redshift_products"
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
            print(f"File archived successfully: {source_key} -> {dest_key}")
        except Exception as e:
            print(f"Error moving file {source_key}: {str(e)}")

else:
    print("No target file found in root directory. Skipping execution.")

job.commit()
