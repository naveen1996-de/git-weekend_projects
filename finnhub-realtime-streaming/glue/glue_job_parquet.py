import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Expect these args from Lambda / default job args
args = getResolvedOptions(sys.argv, ['S3_RAW_PATH', 'S3_PARQUET_PATH'])

s3_raw = args['S3_RAW_PATH']          # e.g. s3://<BUCKET_NAME>/te-streaming-raw-layer/
s3_parquet = args['S3_PARQUET_PATH']  # e.g. s3://<BUCKET_NAME>/te-parquet-table-layer/

print(f"RAW PATH     : {s3_raw}")
print(f"PARQUET PATH : {s3_parquet}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 1) READ RAW JSONL.GZ
# Lambda writes files like:
# s3://<BUCKET_NAME>/te-streaming-raw-layer/YYYY/MM/DD/records-...jsonl.gz
s3_input = f"{s3_raw.rstrip('/')}/2025/12/01/"

print(f"Reading from: {s3_input}")

df = spark.read.json(s3_input)

if df.rdd.isEmpty():
    print("No data found in RAW path, exiting.")
    sys.exit(0)

print("Raw schema:")
df.printSchema()

# 2) NORMALIZE / TRANSFORM

# Finnhub trade fields often come as: s (symbol), p (price), v (volume), t (timestamp)
if "s" in df.columns and "symbol" not in df.columns:
    df = df.withColumnRenamed("s", "symbol")
if "p" in df.columns and "price" not in df.columns:
    df = df.withColumnRenamed("p", "price")
if "v" in df.columns and "volume" not in df.columns:
    df = df.withColumnRenamed("v", "volume")
if "t" in df.columns and "event_ts" not in df.columns:
    df = df.withColumnRenamed("t", "event_ts")

# Cast numeric fields
if "price" in df.columns:
    df = df.withColumn("price", df["price"].cast("double"))
if "volume" in df.columns:
    df = df.withColumn("volume", df["volume"].cast("double"))

# Convert _ingest_ts (ISO string) to timestamp + date
if "_ingest_ts" in df.columns:
    df = df.withColumn("ingest_ts", F.to_timestamp(F.col("_ingest_ts")))
else:
    df = df.withColumn("ingest_ts", F.current_timestamp())

df = df.withColumn("ingest_date", F.to_date("ingest_ts"))

# Convert event_ts (epoch millis) to timestamp if present
if "event_ts" in df.columns:
    df = df.withColumn("event_time", (F.col("event_ts") / 1000).cast("timestamp"))

# 3) WRITE PARTITIONED PARQUET
output_path = s3_parquet.rstrip("/") + "/parquet_table/"

print(f"Writing parquet to: {output_path}")

(df.write
   .mode("append")
   .partitionBy("symbol", "ingest_date")  # good for querying by coin/date
   .parquet(output_path))

print("Write completed.")
