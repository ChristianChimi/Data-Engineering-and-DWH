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