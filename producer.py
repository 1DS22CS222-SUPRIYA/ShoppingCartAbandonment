from kafka import KafkaProducer
import json
import pandas as pd
import time
import os

# === CONFIG ===
TOPIC_NAME = "cart_logs"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DATA_PATH = r"E:\proj\clean_cart_logs.csv"

SLEEP_INTERVAL = 1.5  # delay between events

# === LOAD DATA ===
if not os.path.exists(DATA_PATH):
    raise FileNotFoundError(f"❌ CSV file not found at {DATA_PATH}")

df = pd.read_csv(DATA_PATH)

# Add action and abandonment based on InvoiceNo
df["Action"] = df["InvoiceNo"].astype(str).apply(lambda x: "Cancel" if x.startswith("C") else "Checkout")
df["is_abandoned"] = df["InvoiceNo"].astype(str).str.startswith("C").astype(int)

# === PRODUCER SETUP ===
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Kafka Producer started. Streaming real shopping cart events...\n")

try:
    for i, row in enumerate(df.itertuples(), 1):
        record = {
            "UserID": str(row.CustomerID),
            "ProductID": str(row.StockCode),
            "Action": row.Action,
            "InvoiceNo": str(row.InvoiceNo),
            "Quantity": int(row.Quantity),
            "InvoiceDate": str(row.InvoiceDate),
            "UnitPrice": float(row.UnitPrice),
            "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "is_abandoned": int(row.is_abandoned)
        }

        producer.send(TOPIC_NAME, value=record)
        print(f"➡️ Sent ({i}): {record}")
        time.sleep(SLEEP_INTERVAL)

    producer.flush()
    print("\n✅ Finished streaming all records successfully.")

except KeyboardInterrupt:
    print("\n🛑 Producer stopped manually.")
    producer.flush()
finally:
    producer.close()
