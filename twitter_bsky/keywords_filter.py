import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC       = "simulated-stream"
GROUP_ID          = "simulated-consumer-group"
FILTER_KEYWORDS   = ["flu", "influenza", "cough", "fever", "cold", "sick", "unwell", "nausea", "sneeze", "sneezing", "chills", "vomiting", "virus", "fatigue", "allergy", "allergies"]
DESTINATION_TOPIC = "filtered_stream"
# ────────────────────────────────────────────────────────────────────────────────

async def run():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",  # read from beginning if no committed offsets
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    # 2) Set up producer to write to filtered-stream
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    await producer.start()
    
    try:
        async for msg in consumer:
            post = msg.value
            text = post.get('text', '')
            timestamp = post.get('timestamp', '')

            # Check if any filter keyword appears in the text (case-insensitive)
            if any(keyword in text.lower() for keyword in FILTER_KEYWORDS):
                #print(f"🧹 [Filtered] [{timestamp}] {text[:80]}…")
                await producer.send_and_wait(DESTINATION_TOPIC, post)
                print(f"✅ Forwarded: {text[:50]}…")
                
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("🛑 Consumer stopped by user")