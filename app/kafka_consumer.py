from kafka import KafkaConsumer, errors
import os
from dotenv import load_dotenv
from .etl_tasks import process_data

load_dotenv()

def consume_kafka():
    topic = os.getenv("KAFKA_TOPIC")
    broker = os.getenv("KAFKA_BROKER")

    if not topic or not broker:
        print("❌ Missing KAFKA_TOPIC or KAFKA_BROKER in .env file.")
        return

    print(f"🚀 Starting Kafka Consumer on topic: {topic}, broker: {broker}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="etl_group",
        )

        print("✅ Connected to Kafka. Waiting for messages...")

        for message in consumer:
            try:
                data = message.value.decode("utf-8")
                print(f"📩 Received message: {data}")
                process_data.delay(data)
            except Exception as e:
                print(f"⚠️ Error processing message: {e}")

    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available. Check your connection or .env.")
    except KeyboardInterrupt:
        print("🛑 Consumer stopped manually.")
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
    finally:
        try:
            consumer.close()
            print("🔒 Consumer closed.")
        except Exception:
            pass

if __name__ == "__main__":
    print("🚀 Starting Kafka Consumer...")
    consume_kafka()
