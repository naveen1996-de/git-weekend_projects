import boto3, base64, gzip, json, os
from datetime import datetime
from io import BytesIO

S3_BUCKET = os.environ.get("RAW_BUCKET")
S3_PREFIX = os.environ.get("RAW_PREFIX")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")
REGION = os.environ.get("AWS_REGION", "ap-south-2")

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

def handler(event, context):
    records = []

    # Decode Kinesis records
    for rec in event.get("Records", []):
        payload = base64.b64decode(rec["kinesis"]["data"])
        try:
            obj = json.loads(payload)
        except:
            obj = json.loads(payload.decode("utf-8"))

        records.append(obj)

    if not records:
        return {"status": "no_records"}

    # Prepare S3 file path
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    date_path = datetime.utcnow().strftime("%Y/%m/%d")
    s3_key = f"{S3_PREFIX}/{date_path}/records-{ts}.jsonl.gz"

    # Create gzipped JSON Lines file
    buf = BytesIO()
    with gzip.GzipFile(mode="w", fileobj=buf) as gz:
        for r in records:
            line = json.dumps(r) + "\n"
            gz.write(line.encode("utf-8"))

    buf.seek(0)

    # Upload to S3
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buf.read())
    print(f"Uploaded batch to S3: {s3_key}")

    # Trigger Glue job
    try:
        resp = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--S3_RAW_PATH": f"s3://{S3_BUCKET}/{S3_PREFIX}/",
                "--S3_PARQUET_PATH": os.environ.get("PARQUET_S3_PATH")
            }
        )
        print("Started Glue job:", resp.get("JobRunId"))
    except Exception as e:
        print("Error triggering Glue job:", e)

    return {"status": "ok", "records": len(records)}
