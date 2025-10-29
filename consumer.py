from kafka import KafkaConsumer
import json
import os

# ✅ Configuration
TOPIC_NAME = "cart_logs"
LOCAL_STREAM_PATH = "E:/ShoppingCartAbandonment/cart_stream.txt"
HDFS_PATH = "hdfs://localhost:9000/user/supriya/cart_data/cart_stream.txt"

# ✅ Create local folder if not exists
os.makedirs(os.path.dirname(LOCAL_STREAM_PATH), exist_ok=True)

# ✅ Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print(f"📥 Listening to topic '{TOPIC_NAME}' and writing to HDFS...")

for message in consumer:
    event = message.value
    print("Received:", event)

    # ✅ Write locally for reference
    with open(LOCAL_STREAM_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    # ✅ Upload to HDFS (overwrite each time)
    os.system(f"hdfs dfs -put -f {LOCAL_STREAM_PATH} /user/supriya/cart_data/cart_stream.txt")
