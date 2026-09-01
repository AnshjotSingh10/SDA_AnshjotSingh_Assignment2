"""
4. CONSUMER GROUPS -> Group C: Cart Abandonment & Marketing Consumer
   (cart-events-topic, purchases-topic)

- Confirmed purchases -> MySQL `orders` (5. STORAGE LAYER -> MySQL)
- Live cart status      -> MongoDB `cart_status` (5. STORAGE LAYER -> MongoDB)
- Carts with no purchase after ABANDON_TIMEOUT_SECONDS -> MongoDB
  `abandonment_alerts`, which the dashboard's "Cart Abandonment & Discount
  Alerts" panel (6. DASHBOARDS & ALERTS) reads from.

Run:
    python consumers/group_c_cart_abandonment.py
"""
import os
import sys
import time
import threading
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from common import (
    KAFKA_BOOTSTRAP_SERVERS, TOPICS, json_deserializer,
    MONGO_URI, MONGO_DB, mysql_connection_string,
)

GROUP_ID = "group-c-cart-abandonment"

SUBSCRIBED_TOPICS = [
    TOPICS["cart_events"],
    TOPICS["purchases"],
]

ABANDON_TIMEOUT_SECONDS = int(os.getenv("ABANDON_TIMEOUT_SECONDS", "30"))


def now_dt():
    return datetime.now(timezone.utc)


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


def handle_cart_event(cart_status, event):
    session_id = event["session_id"]
    if event["event_type"] == "add_to_cart":
        cart_status.update_one(
            {"session_id": session_id},
            {
                "$set": {
                    "session_id": session_id,
                    "user_id": event.get("user_id"),
                    "product_id": event.get("product_id"),
                    "product_name": event.get("product_name"),
                    "unit_price": event.get("unit_price"),
                    "quantity": event.get("quantity"),
                    "status": "active_cart",
                    "cart_updated_at": now_dt(),
                }
            },
            upsert=True,
        )
    elif event["event_type"] == "remove_from_cart":
        cart_status.update_one(
            {"session_id": session_id},
            {"$set": {"status": "emptied", "cart_updated_at": now_dt()}},
        )


def handle_purchase_event(cart_status, orders_engine, event):
    session_id = event["session_id"]

    # mark cart as converted
    cart_status.update_one(
        {"session_id": session_id},
        {"$set": {"status": "purchased", "cart_updated_at": now_dt()}},
        upsert=True,
    )

    with orders_engine.begin() as conn:
        conn.execute(
            text("""
                INSERT IGNORE INTO orders
                    (order_id, session_id, user_id, product_id, product_name,
                     quantity, unit_price, order_value, payment_status, created_at)
                VALUES
                    (:order_id, :session_id, :user_id, :product_id, :product_name,
                     :quantity, :unit_price, :order_value, :payment_status, :created_at)
            """),
            {
                "order_id": event["order_id"],
                "session_id": session_id,
                "user_id": event.get("user_id"),
                "product_id": event.get("product_id"),
                "product_name": event.get("product_name"),
                "quantity": event.get("quantity"),
                "unit_price": event.get("unit_price"),
                "order_value": event.get("order_value"),
                "payment_status": event.get("payment_status", "confirmed"),
                "created_at": event.get("timestamp", now_dt().isoformat()),
            },
        )


def abandonment_watcher(cart_status, alerts):
    """Background loop: flags carts untouched for ABANDON_TIMEOUT_SECONDS as abandoned."""
    while True:
        cutoff = now_dt().timestamp() - ABANDON_TIMEOUT_SECONDS
        stale_carts = cart_status.find({"status": "active_cart"})
        for cart in stale_carts:
            updated_at = cart.get("cart_updated_at")
            if updated_at is None:
                continue
            if updated_at.timestamp() < cutoff:
                cart_status.update_one(
                    {"_id": cart["_id"]},
                    {"$set": {"status": "abandoned"}},
                )
                alerts.update_one(
                    {"session_id": cart["session_id"]},
                    {
                        "$set": {
                            "session_id": cart["session_id"],
                            "user_id": cart.get("user_id"),
                            "product_name": cart.get("product_name"),
                            "quantity": cart.get("quantity"),
                            "unit_price": cart.get("unit_price"),
                            "flagged_at": now_dt(),
                            "discount_suggestion_pct": 10,
                        }
                    },
                    upsert=True,
                )
                print(f"[{GROUP_ID}] ALERT: cart abandoned -> session={cart['session_id']}")
        time.sleep(5)


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    consumer = make_consumer()

    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    cart_status = db["cart_status"]
    alerts = db["abandonment_alerts"]
    cart_status.create_index("session_id", unique=True)
    alerts.create_index("session_id", unique=True)

    orders_engine = create_engine(mysql_connection_string(), pool_pre_ping=True)

    watcher_thread = threading.Thread(
        target=abandonment_watcher, args=(cart_status, alerts), daemon=True
    )
    watcher_thread.start()

    print(f"[{GROUP_ID}] polling {SUBSCRIBED_TOPICS} ... Ctrl+C to stop.")
    try:
        for msg in consumer:
            event = msg.value
            if not event:
                continue
            if msg.topic == TOPICS["cart_events"]:
                handle_cart_event(cart_status, event)
            elif msg.topic == TOPICS["purchases"]:
                handle_purchase_event(cart_status, orders_engine, event)
    except KeyboardInterrupt:
        print(f"\nStopping {GROUP_ID} ...")
    finally:
        consumer.close()
        mongo.close()


if __name__ == "__main__":
    main()
