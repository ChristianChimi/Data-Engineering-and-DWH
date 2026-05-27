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

bucket = "corporate-data-lake-bronze"
tmp_bucket = "glue-processing-temp"
dest_prefix = "DONE/"
pattern = "*product.csv"

s3 = boto3.client("s3")
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket)

file_paths = []

for page in pages:
    if "Contents" in page:
        for obj in page["Contents"]:
            key = obj["Key"]
            if fnmatch.fnmatch(key, pattern):
                file_paths.append(f"s3://{bucket}/{key}")

if not file_paths:
    print("No catalog files found to process.")
    job.commit()
    sys.exit(0)

df_product = (
    spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv(file_paths)
    .select(
        "code",
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

name_mapping = {
    'code': 'product_code',
    'prod_descr': 'product_desc',
    'prod_cat_1': 'product_category1',
    'prod_cat_2': 'product_category2',
    'prod_cat_3': 'product_category3',
    'prod_cat_4': 'product_category4',
    'prod_cat_5': 'product_category5'
}

df_product_renamed = df_product.select([
    F.col(c).alias(name_mapping[c]) if c in name_mapping else F.col(c) 
    for c in df_product.columns
])

df_product_final = df_product_renamed.withColumn('record_date', F.current_timestamp())
df_clean = df_product_final.dropDuplicates()

lista_file = [row["filename"] for row in df_clean.select("filename").distinct().collect()]

dyf_product = DynamicFrame.fromDF(
    dataframe=df_clean.drop("filename"),
    glue_ctx=glueContext,
    name="dyf_product"
)

glueContext.write_dynamic_frame.from_options(
    frame=dyf_product,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "aws