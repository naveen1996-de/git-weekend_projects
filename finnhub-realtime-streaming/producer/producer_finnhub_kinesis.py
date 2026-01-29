#!/usr/bin/env python3
import os
import json
import time
import boto3
import traceback
from datetime import datetime
from websocket import WebSocketApp

# --------------------------------------------------------
# CONFIG
# --------------------------------------------------------
REGION = os.getenv("AWS_REGION", "ap-south-2")
KINESIS_STREAM = os.getenv("KINESIS_STREAM", "te-streaming-ds")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "d4ffbs1r01qkcvvi13u0d4ffbs1r01qkcvvi13ug")
STREAM_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# --------------------------------------------------------
# AWS CLIENT
# --------------------------------------------------------
kinesis = boto3.client("kinesis", region_name=REGION)

# --------------------------------------------------------
# SEND A RECORD TO KINESIS
# --------------------------------------------------------
def send_to_kinesis(record: dict):
    try:
        kinesis.put_record(
            StreamName=KINESIS_STREAM,
            Data=json.dumps(record),
            PartitionKey=record.get("symbol", "BTCUSDT"),
        )
    except Exception as e:
        print("ERROR sending to Kinesis:", e)
        traceback.print_exc()

# --------------------------------------------------------
# HANDLE WEB SOCKET MESSAGE
# --------------------------------------------------------
def on_message(ws, message):
    try:
        data = json.loads(message)

        if data.get("type") == "trade":
            for t in data["data"]:
                record = {
                    "symbol": t["s"],
                    "price": t["p"],
                    "volume": t["v"],
                    "timestamp": t["t"],
                    "_ingest_ts": datetime.utcnow().isoformat()
                }

                print(f"[STREAM] {record['symbol']} | {record['price']} | vol={record['volume']}")

                send_to_kinesis(record)

    except Exception as e:
        print("Error in on_message:", e)
        traceback.print_exc()

# --------------------------------------------------------
# ON OPEN
# --------------------------------------------------------
def on_open(ws):
    print("Connected. Subscribing to BTC & AAPL streams...")
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
    ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))

# --------------------------------------------------------
# ON CLOSE
# --------------------------------------------------------
def on_close(ws, code, msg):
    print("WebSocket closed. Reconnecting in 3 seconds...")
    time.sleep(3)
    start_stream()

# --------------------------------------------------------
# ON ERROR
# --------------------------------------------------------
def on_error(ws, error):
    print("WebSocket error:", error)
    time.sleep(5)
    start_stream()

# --------------------------------------------------------
# START STREAM
# --------------------------------------------------------
def start_stream():
    ws = WebSocketApp(
        STREAM_URL,
        on_message=on_message,
        on_open=on_open,
        on_close=on_close,
        on_error=on_error
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

# --------------------------------------------------------
# MAIN
# --------------------------------------------------------
if __name__ == "__main__":
    print("Starting Finnhub → Kinesis producer...")
    start_stream()
