from fastapi    import FastAPI,UploadFile, File
from .kafka_producer import send_to_kafka

app = FastAPI(title="FastAPI + Celery + Redis + Kafka ETL")
@app.post("/upload")
async def upload_data(file: UploadFile = File(...)):
    data = await file.read()
    send_to_kafka(data.decode('utf-8'))
    return {"message": "Data sent to Kafka successfully"}
