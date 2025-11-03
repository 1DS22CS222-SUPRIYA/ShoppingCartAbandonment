# =========================
# consumer.py
# =========================

from kafka import KafkaConsumer
import json, time, os

TOPIC = "cart_logs"
BOOTSTRAP_SERVERS = ["localhost:9092"]
LOCAL_PATH = r"E:\proj\cart_stream.txt"
HDFS_PATH = "/user/supriya/shopping_data/cart_stream.txt"
TIMEOUT = 180  # seconds of inactivity before abandonment

print("🛰️ Kafka Consumer started. Listening for cart events...\n")

# Track sessions
active_sessions = {}
abandoned_sessions = set()

# Make sure file + folder exist
os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
open(LOCAL_PATH, "w").close()
os.system("hdfs dfs -mkdir -p /user/supriya/shopping_data")

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def upload_to_hdfs():
    os.system(f"hdfs dfs -put -f {LOCAL_PATH} {HDFS_PATH}")
    print("☁️ Synced to HDFS")

def log_event(event):
    with open(LOCAL_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")
    print(f"📦 Processed: {event}")

try:
    while True:
        for msg in consumer:
            e = msg.value
            uid = str(e.get("UserID"))
            action = e.get("Action", "")
            ts = time.strftime("%Y-%m-%d %H:%M:%S")

            if uid not in abandoned_sessions:
                # Track activity
                active_sessions[uid] = time.time()

                # Determine status
                if action.lower() == "checkout":
                    e["is_abandoned"] = 0
                    active_sessions.pop(uid, None)

                elif action.lower() == "cancel":
                    e["is_abandoned"] = 1
                    abandoned_sessions.add(uid)
                    active_sessions.pop(uid, None)

                else:  # AddToCart or others
                    e["is_abandoned"] = 0

                e["Timestamp"] = ts
                log_event(e)

            # Check for inactive users
            now = time.time()
            for u, last in list(active_sessions.items()):
                if now - last > TIMEOUT:
                    abandoned_sessions.add(u)
                    inactive_event = {
                        "UserID": u,
                        "Action": "Timeout",
                        "Timestamp": ts,
                        "is_abandoned": 1
                    }
                    log_event(inactive_event)
                    del active_sessions[u]

            time.sleep(0.2)

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped manually.")
    upload_to_hdfs()

except Exception as e:
    print(f"⚠️ Error: {e}")
    upload_to_hdfs()
