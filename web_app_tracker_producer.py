"""
2. PRODUCERS (Data Ingestion) -> "Web / App Tracker Producer (client SDK / gateway)"

Simulates many concurrent live shopper sessions with Faker and publishes
their behavioral events to Kafka:
    page-views-topic, search-queries-topic, click-events-topic, cart-events-topic

Each simulated session probabilistically walks a funnel:
    page view(s) -> maybe search -> maybe click -> maybe add-to-cart -> maybe remove

Whenever a cart is created, a small record is appended to
shared_state/active_carts.jsonl so the separate Checkout Service Producer
(a different backend process, like in the real architecture) can decide
whether that cart converts into a purchase.

Run:
    python producers/web_app_tracker_producer.py
"""
import os
import sys
import json
import time
import random
import threading
import uuid
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from common import KAFKA_BOOTSTRAP_SERVERS, TOPICS, json_serializer
from data_catalog import random_page, random_search_term, random_product, new_user

SHARED_STATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shared_state")
os.makedirs(SHARED_STATE_DIR, exist_ok=True)
ACTIVE_CARTS_FILE = os.path.join(SHARED_STATE_DIR, "active_carts.jsonl")

NUM_CONCURRENT_SESSIONS = int(os.getenv("NUM_CONCURRENT_SESSIONS", "25"))
_file_lock = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )


def append_active_cart(record: dict):
    with _file_lock:
        with open(ACTIVE_CARTS_FILE, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")


def simulate_session(producer: KafkaProducer, stop_event: threading.Event):
    """Runs one simulated shopper session end-to-end, then loops to a new one."""
    while not stop_event.is_set():
        user = new_user()
        session_id = str(uuid.uuid4())
        user_id = user["user_id"]

        # --- 1..3 page views (browsing) ---
        num_views = random.randint(1, 3)
        for _ in range(num_views):
            page = random_page()
            event = {
                "event_type": "page_view",
                "session_id": session_id,
                "user_id": user_id,
                "page_type": page["page_type"],
                "page_ref": page["page_ref"],
                "timestamp": now_iso(),
            }
            producer.send(TOPICS["page_views"], key=session_id, value=event)
            time.sleep(random.uniform(0.2, 1.0))

        # --- 60% chance of a search ---
        if random.random() < 0.6:
            term = random_search_term()
            event = {
                "event_type": "search_query",
                "session_id": session_id,
                "user_id": user_id,
                "search_term": term,
                "results_count": random.randint(0, 200),
                "timestamp": now_iso(),
            }
            producer.send(TOPICS["search_queries"], key=session_id, value=event)
            time.sleep(random.uniform(0.2, 0.8))

        # --- 50% chance of a click (banner/nav/recommendation) ---
        if random.random() < 0.5:
            product = random_product()
            event = {
                "event_type": "click",
                "session_id": session_id,
                "user_id": user_id,
                "click_target": random.choice(["banner", "nav_menu", "recommendation", "search_result"]),
                "product_id": product["product_id"],
                "timestamp": now_iso(),
            }
            producer.send(TOPICS["click_events"], key=session_id, value=event)
            time.sleep(random.uniform(0.2, 0.8))

        # --- 35% chance of add-to-cart ---
        if random.random() < 0.35:
            product = random_product()
            quantity = random.randint(1, 3)
            cart_event = {
                "event_type": "add_to_cart",
                "session_id": session_id,
                "user_id": user_id,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "unit_price": product["unit_price"],
                "quantity": quantity,
                "timestamp": now_iso(),
            }
            producer.send(TOPICS["cart_events"], key=session_id, value=cart_event)

            # Let the (separate) checkout service know a cart now exists.
            append_active_cart({
                "session_id": session_id,
                "user_id": user_id,
                "product_id": product["product_id"],
                "product_name": product["product_name"],
                "unit_price": product["unit_price"],
                "quantity": quantity,
                "cart_created_at": now_iso(),
            })

            # --- 15% chance of then removing an item (cart edit) ---
            if random.random() < 0.15:
                time.sleep(random.uniform(0.3, 1.0))
                remove_event = {
                    "event_type": "remove_from_cart",
                    "session_id": session_id,
                    "user_id": user_id,
                    "product_id": product["product_id"],
                    "timestamp": now_iso(),
                }
                producer.send(TOPICS["cart_events"], key=session_id, value=remove_event)

        producer.flush()
        # small pause before this "slot" starts a brand new simulated user/session
        time.sleep(random.uniform(0.5, 2.0))


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = make_producer()
    print(f"Starting {NUM_CONCURRENT_SESSIONS} concurrent simulated shopper sessions. Ctrl+C to stop.")

    stop_event = threading.Event()
    threads = []
    for _ in range(NUM_CONCURRENT_SESSIONS):
        t = threading.Thread(target=simulate_session, args=(producer, stop_event), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer ...")
        stop_event.set()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
