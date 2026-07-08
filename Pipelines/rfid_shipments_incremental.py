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

SOURCE_DATABASE = "enterprise_source_db"
SOURCE_TABLE_NAME = "device_telemetry_logs"

TARGET_TABLE = "dw_analytics_layer1.shipping_rfid_logs"
TEMP_DIR = "s3://company-data-warehouse-prod/tmp/glue_redshift_migration/"
MYSQL_CONNECTION = "JDBC-Source-MySQL-Connection-Prod"
REDSHIFT_CONNECTION = "JDBC-Target-Redshift-Connection-Prod"

print("Calculating MAX(date + time) string from Redshift...")
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

connection_options = {
    "useConnectionProperties": "true",
    "connectionName": MYSQL_CONNECTION,
    "dbTable": f"{SOURCE_DATABASE}.{SOURCE_TABLE_NAME}",
    "database": SOURCE_DATABASE
}

if max_timestamp is not None:
    where_clause = f"CONCAT(date, ' ', RIGHT(time, 8)) > '{max_timestamp}'"
    connection_options["whereClause"] = where_clause
    print(f"Incremental filter applied on MySQL: {where_clause}")
else:
    print("No checkpoint found. Starting full data load.")

dyf_mysql = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options=connection_options
)
df_filtered = dyf_mysql.toDF()

df_transformed = df_filtered.withColumn("id", col("id").cast("long"))

df_transformed = df_transformed.withColumn("time_str", col("time").cast("string"))
df_transformed = df_transformed.withColumn("time", F.expr("RIGHT(time_str, 8)")).drop("time_str")

record_count = df_transformed.count()
print(f"Total records extracted and ready for Append: {record_count}")

if record_count > 0:
    print(f"Writing {record_count} records...")
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
    print("Incremental write completed successfully.")
else:
    print("No new records found beyond the last temporal checkpoint.")

job.commit()
print("Incremental job execution finished.")
