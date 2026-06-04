import sys
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

SOURCE_TABLE = "source_database.device_logs"
TARGET_TABLE = "dw_layer1.iot_telemetry_records"
TEMP_DIR = "s3://company-sftp-temp-data-stage/tmp/glue_redshift/"
MYSQL_CONNECTION = "RDS-MySQL-Production-Connection"
REDSHIFT_CONNECTION = "Redshift Production JDBC Connection"

dyf_redshift = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": REDSHIFT_CONNECTION,
        "sampleQuery": f"SELECT MAX(date + time) AS max_timestamp FROM {TARGET_TABLE}",
        "redshiftTmpDir": TEMP_DIR
    }
)

df_redshift = dyf_redshift.toDF()
max_timestamp = df_redshift.collect()[0]["max_timestamp"]

dyf_mysql = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": MYSQL_CONNECTION,
        "dbtable": SOURCE_TABLE
    }
)

df = dyf_mysql.toDF()

if max_timestamp is not None:
    print(f"Incremental filter active. Loading records newer than: {max_timestamp}")
    
    df_with_ts = df.withColumn(
        "source_timestamp",
        F.to_timestamp(F.concat_ws(" ", col("date"), col("time")), "yyyy-MM-dd HH:mm:ss")
    )
    
    df_filtered = df_with_ts.filter(col("source_timestamp") > max_timestamp)
    df_filtered = df_filtered.drop("source_timestamp")
else:
    print("No existing data found in target. Performing full load extraction.")
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
