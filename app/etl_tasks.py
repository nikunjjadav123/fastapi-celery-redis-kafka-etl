from .worker import celery_app
import time

@celery_app.task
def process_data(data):
    print(f"Processing data: {data}")
    # Simulate transformation
    time.sleep(2)
    transformed = data.upper()
    print(f"Transformed Data: {transformed}")
    # Load step (save to DB or file)
    with open("output.txt", "a") as f:
        f.write(transformed + "\n")
    return transformed
