import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ['S3_PARQUET_PATH', 'PROCESS_DATE']
)

s3_parquet = args['S3_PARQUET_PATH'].rstrip('/')
process_date = args['PROCESS_DATE']   # yyyy-mm-dd

input_path = f"{s3_parquet}/parquet_table/"
output_path = f"{s3_parquet}/parquet_table_optimized/"

print(f"Reading from     : {input_path}")
print(f"Optimizing date  : {process_date}")
print(f"Writing to       : {output_path}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1️ Read existing parquet
df = spark.read.parquet(input_path)

# 2️ Filter only one day (VERY IMPORTANT)
df_day = df.filter(F.col("ingest_date") == process_date)

if df_day.rdd.isEmpty():
    print("No data found for given date, exiting.")
    sys.exit(0)

# 3️ Repartition → reduces small files
df_compacted = (
    df_day
    .repartition("symbol")   # repartition per symbol
)

# 4️ Write optimized parquet
(
    df_compacted.write
    .mode("append")
    .partitionBy("symbol", "ingest_date")
    .parquet(output_path)
)

print("Compaction completed successfully.")
