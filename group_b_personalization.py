"""
4. CONSUMER GROUPS -> Group B: Personalization Consumer
   (page-views-topic, search-queries-topic, click-events-topic)

Writes raw behavioral events into MongoDB (5. STORAGE LAYER -> MongoDB)
for downstream recommendation / personalization models to consume.

Run:
    python consumers/group_b_personalization.py
"""
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from pymongo import MongoClient
from common import KAFKA_BOOTSTRAP_SERVERS, TOPICS, json_deserializer, MONGO_URI, MONGO_DB

GROUP_ID = "group-b-personalization"

SUBSCRIBED_TOPICS = [
    TOPICS["page_views"],
    TOPICS["search_queries"],
    TOPICS["click_events"],
]


def make_consumer():
    return KafkaConsumer(
        *SUBSCRIBED_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=json_deserializer,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    consumer = make_consumer()

    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    user_events = db["user_events"]
    user_events.create_index("session_id")
    user_events.create_index("user_id")
    user_events.create_index([("event_type", 1)])

    print(f"[{GROUP_ID}] polling {SUBSCRIBED_TOPICS} ... Ctrl+C to stop.")
    try:
        for msg in consumer:
            event = msg.value
            if not event:
                continue
            event["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            event["_source_topic"] = msg.topic
            user_events.insert_one(event)
    except KeyboardInterrupt:
        print(f"\nStopping {GROUP_ID} ...")
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
