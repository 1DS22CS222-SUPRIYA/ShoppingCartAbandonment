from kafka import KafkaProducer
import pandas as pd
import json
import time

# === CONFIG ===
DATA_PATH = "E:\ShoppingCartAbandonment\clean_cart_logs.csv"
TOPIC_NAME = "cart_logs"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📤 Starting to stream data from {DATA_PATH} to Kafka topic '{TOPIC_NAME}'...")
df = pd.read_csv(DATA_PATH)

for _, row in df.iterrows():
    producer.send(TOPIC_NAME, value=row.to_dict())
    time.sleep(0.5)  # simulate real-time data
    print("Sent:", row.to_dict())

producer.flush()
print("✅ All data sent to Kafka successfully!")
