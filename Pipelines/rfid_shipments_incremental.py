import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format
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

SOURCE_TABLE = "source_database.tag_logs"
TARGET_TABLE = "warehouse_layer1.rfid_case_shipments"
TEMP_DIR = "s3://corporate-glue-processing-temp/tmp/glue_redshift/"
MYSQL_CONNECTION = "aws_rds_mysql_jdbc_connection"
REDSHIFT_CONNECTION = "aws_redshift_warehouse_jdbc_connection"

dyf_redshift = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": REDSHIFT_CONNECTION,
        "sampleQuery": f"SELECT MAX(date) AS max_date FROM {TARGET_TABLE}",
        "redshiftTmpDir": TEMP_DIR
    }
)

df_redshift = dyf_redshift.toDF()
max_date = df_redshift.collect()[0]["max_date"]

dyf_mysql = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": MYSQL_CONNECTION,
        "dbtable": SOURCE_TABLE
    }
)

df = dyf_mysql.toDF()

if max_date is not None:
    print(f"Incremental filter active. Loading records newer than: {max_date}")
    df_filtered = df.filter(col("date") > max_date)
else:
    print("No max date found on Redshift target. Executing full load.")
    df_filtered = df

df_transformed = df_filtered.withColumn("id", col("id").cast("long"))

df_transformed = df_transformed.withColumn(
    "time",
    date_format(col("time"), "HH:mm:ss")
)

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
    transformation_ctx="write_redshift"
)

job.commit()