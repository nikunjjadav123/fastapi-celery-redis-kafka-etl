from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
from .etl_tasks import process_data

load_dotenv()

def consumer_kafka():
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='etl_group'
    )
    for message in consumer:
        data = message.value.decode('utf-8')
        print(f"Received: {data}")
        process_data.delay(data)
