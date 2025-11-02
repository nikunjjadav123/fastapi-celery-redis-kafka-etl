from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

celery_app = Celery(
    'etl_worker',
    broker=os.getenv("REDIS_URL"),
    backend=os.getenv("REDIS_URL")
)
