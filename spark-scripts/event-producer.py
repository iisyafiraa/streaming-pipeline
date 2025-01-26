import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()

class DataGenerator(object):
    @staticmethod
    def get_data():
        now = datetime.now()

        return {
            "transaction_id": uuid.uuid4().__str__(),
            "customer_id": faker.random_int(min=1, max=1000),
            "category": faker.random_element(
                elements=("Skincare", "Makeup", "Hair Care", "Bath & Body")
            ),
            "brand": faker.random_element(
                elements=("Maybelline", "Make Over", "Wardah", "Barenbliss", "Sea Make Up",
                          "SKIN 1004", "COSRX", "Esqa", "YOU", "Grace and Glow", "Skintific",
                          "Laneige", "Vaseline", "SK-II", "Somethinc", "Scarlett", "Avoskin",
                          "Whitelab", "Azarine", "Erha", "MS Glow", "L'Oreal", "Cetaphil")
            ) ,
            "product_name": faker.random_element(
                elements=("Cushion", "Foundation", "Loose Powder", "Lipstick", "Liptint","Lipcream",
                          "Setting Spray", "Highlighter", "Eyebrow", "Eyeliner", "Mascara", "Face Pallete",
                          "Cleansing Oil", "Face Wash", "Makeup Remover", "Serum", "Fase Mask", "Lipbalm",
                          "Toner", "Sunscreen", "Moisturizer", "Lip Serum", "Shampoo", "Hair Vitamin",
                          "Hair Oil", "Body Wash", "Body Lotion", "Deodorant", "Body Serum")
            ),
            "quantity": faker.random_int(min=1, max=10),
            "price": faker.random_int(min=10000, max=1500000),
            "payment_method": faker.random_element(
                elements=("COD", "Debit Card", "Credit Card", "Paylater")
            ),
            "timestamp": faker.unix_time(
                start_datetime=now - timedelta(minutes=60), end_datetime=now
            ),
        }

while True:
    data = DataGenerator.get_data()  
    json_data = json.dumps(data).encode("utf-8") 
    
    print(f"Sending payload: {json_data}", flush=True)
    
    try:
        response = producer.send(topic=kafka_topic, value=json_data)
        print(f"Response: {response.get()}", flush=True)
    except Exception as e:
        print(f"Error sending message: {e}", flush=True)
    
    print("=-" * 20, flush=True)
    sleep(3) 