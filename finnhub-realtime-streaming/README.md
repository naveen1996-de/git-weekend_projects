\# Real-Time Streaming Pipeline on AWS

\## Overview This project demonstrates an end-to-end real-time data
streaming pipeline using AWS managed services. Live trade data is
ingested from Finnhub, processed in real time, stored in a data lake,
and queried using Athena.

\## Architecture EC2 → Kinesis → Lambda → S3 (Raw JSONL) → Glue →
Parquet → Athena

\## Components - EC2 Producer: Streams live data from Finnhub
WebSocket - Kinesis: Real-time ingestion layer - Lambda: Stream consumer
& batch writer - S3: Raw + curated data lake - Glue: ETL & Parquet
optimization - Athena: SQL analytics layer

\## Data Flow 1. EC2 producer sends trade events to Kinesis 2. Lambda
consumes Kinesis records and writes gzipped JSONL to S3 3. Lambda
triggers Glue job 4. Glue converts raw data to partitioned Parquet 5.
Athena queries Parquet data using SQL

\## Partition Strategy - symbol - ingest_date

\## Optimization A Glue compaction job is included to merge small
Parquet files into larger files for better query performance.

\## Sample Athena Query \`\`\`sql SELECT symbol, price, ingest_ts FROM
te_parquet_trade ORDER BY ingest_ts DESC LIMIT 10;
