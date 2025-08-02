import asyncio
import csv
import json
from aiokafka import AIOKafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC       = "simulated-stream"
CSV_FILE          = "project_training.csv"   # ← your dataset
STREAM_DELAY_SEC  = 0.08         # ← delay between posts
# ────────────────────────────────────────────────────────────────────────────────

async def run():
    # 1) Set up Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    await producer.start()
    
    try:
        # 2) Open the CSV file
        with open(CSV_FILE, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) < 6:
                    continue  # skip if row is too short

                # 3) Extract the needed fields
                timestamp = row[2]
                text = row[5]

                # 4) Build Kafka message
                payload = {
                    "text": text,
                    "timestamp": timestamp
                }

                # 5) Send to Kafka
                await producer.send_and_wait(KAFKA_TOPIC, json.dumps(payload).encode())
                print(f"📨 Sent: {text[:50]}…")

                # 6) Simulate real-time streaming
                await asyncio.sleep(STREAM_DELAY_SEC)

    finally:
        await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("🛑 Stopped by user")