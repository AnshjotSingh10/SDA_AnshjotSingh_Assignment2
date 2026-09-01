"""
2. PRODUCERS (Data Ingestion) -> "Checkout Service Producer (backend, payment-confirmed)"

This is an independent backend producer (separate from the client-side
Web/App Tracker Producer) that publishes only to purchases-topic.

It watches shared_state/active_carts.jsonl (written by
web_app_tracker_producer.py whenever a shopper adds something to their
cart) the way a real checkout/payments service would look up a cart in
its own datastore. For each cart it probabilistically decides -- after a
realistic delay -- whether the shopper actually completes checkout
(payment confirmed) or abandons it. This is what feeds the "Cart
Abandonment" logic downstream in Consumer Group C.

Run:
    python producers/checkout_service_producer.py
"""
import os
import sys
import json
import time
import random
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from common import KAFKA_BOOTSTRAP_SERVERS, TOPICS, json_serializer

SHARED_STATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shared_state")
os.makedirs(SHARED_STATE_DIR, exist_ok=True)
ACTIVE_CARTS_FILE = os.path.join(SHARED_STATE_DIR, "active_carts.jsonl")

CONVERSION_RATE = float(os.getenv("CART_CONVERSION_RATE", "0.55"))  # 55% of carts convert
MIN_CHECKOUT_DELAY = 2   # seconds (simulated) before a decision is made
MAX_CHECKOUT_DELAY = 8


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


def read_new_carts(last_offset: int):
    """Read any new lines appended to the active carts file since last_offset (byte offset)."""
    if not os.path.exists(ACTIVE_CARTS_FILE):
        return [], last_offset

    carts = []
    with open(ACTIVE_CARTS_FILE, "r") as f:
        f.seek(last_offset)
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                carts.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        new_offset = f.tell()
    return carts, new_offset


def process_cart(producer: KafkaProducer, cart: dict):
    """Decide whether this cart converts to a purchase, after a realistic delay."""
    delay = random.uniform(MIN_CHECKOUT_DELAY, MAX_CHECKOUT_DELAY)
    time.sleep(delay)

    if random.random() > CONVERSION_RATE:
        # Cart abandoned -- no purchase event is ever produced for it.
        # Consumer Group C detects this by timeout (no matching purchase).
        print(f"[checkout] session={cart['session_id']} -> cart abandoned (no purchase emitted)")
        return

    quantity = cart["quantity"]
    unit_price = cart["unit_price"]
    order = {
        "event_type": "purchase",
        "order_id": str(uuid.uuid4()),
        "session_id": cart["session_id"],
        "user_id": cart["user_id"],
        "product_id": cart["product_id"],
        "product_name": cart["product_name"],
        "quantity": quantity,
        "unit_price": unit_price,
        "order_value": round(quantity * unit_price, 2),
        "payment_status": "confirmed",
        "timestamp": now_iso(),
    }
    producer.send(TOPICS["purchases"], key=cart["session_id"], value=order)
    producer.flush()
    print(f"[checkout] session={cart['session_id']} -> purchase confirmed (${order['order_value']})")


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = make_producer()
    print("Checkout Service Producer watching active carts. Ctrl+C to stop.")

    last_offset = 0
    pool = ThreadPoolExecutor(max_workers=20)
    try:
        while True:
            carts, last_offset = read_new_carts(last_offset)
            for cart in carts:
                # each cart's delayed checkout decision runs concurrently,
                # so many carts can be "in flight" at once
                pool.submit(process_cart, producer, cart)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping checkout service producer ...")
        pool.shutdown(wait=False)
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
