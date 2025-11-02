from kafka  import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    value_serializer=lambda v: v.encode('utf-8')
)

def send_to_kafka(data:str):
    topic = os.getenv("KAFKA_TOPIC")
    producer.send(topic, data)
    producer.flush()