"""
4. CONSUMER GROUPS -> Group A: Session & Funnel Consumer (all 5 topics)

Reconstructs each session's funnel progress (view -> search -> click ->
cart -> purchase) and upserts it into MySQL `session_funnel`
(5. STORAGE LAYER -> MySQL).

Run:
    python consumers/group_a_session_funnel.py
"""
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer, TopicPartition
from sqlalchemy import create_engine, text
from common import KAFKA_BOOTSTRAP_SERVERS, TOPICS, json_deserializer, mysql_connection_string

GROUP_ID = "group-a-session-funnel"

STAGE_ORDER = ["view", "search", "click", "cart", "purchase"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_consumer():
    return KafkaConsumer(
        *TOPICS.values(),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=json_deserializer,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


def upsert_funnel(engine, session_id, user_id, event_type, timestamp):
    flags = {
        "has_page_view": event_type == "page_view",
        "has_search": event_type == "search_query",
        "has_click": event_type == "click",
        "has_cart_event": event_type in ("add_to_cart", "remove_from_cart"),
        "has_purchase": event_type == "purchase",
    }
    stage_map = {
        "page_view": "view",
        "search_query": "search",
        "click": "click",
        "add_to_cart": "cart",
        "remove_from_cart": "cart",
        "purchase": "purchase",
    }
    new_stage = stage_map.get(event_type, "view")

    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT funnel_stage FROM session_funnel WHERE session_id = :sid"),
            {"sid": session_id},
        ).fetchone()

        if existing is None:
            conn.execute(
                text("""
                    INSERT INTO session_funnel
                        (session_id, user_id, has_page_view, has_search, has_click,
                         has_cart_event, has_purchase, funnel_stage, first_seen_at, last_seen_at)
                    VALUES
                        (:sid, :uid, :hpv, :hs, :hc, :hce, :hp, :stage, :ts, :ts)
                """),
                {
                    "sid": session_id, "uid": user_id,
                    "hpv": int(flags["has_page_view"]), "hs": int(flags["has_search"]),
                    "hc": int(flags["has_click"]), "hce": int(flags["has_cart_event"]),
                    "hp": int(flags["has_purchase"]), "stage": new_stage, "ts": timestamp,
                },
            )
        else:
            current_stage = existing[0]
            # only move the funnel stage forward, never backward
            final_stage = new_stage
            if STAGE_ORDER.index(current_stage) > STAGE_ORDER.index(new_stage):
                final_stage = current_stage

            conn.execute(
                text(f"""
                    UPDATE session_funnel SET
                        has_page_view = has_page_view OR :hpv,
                        has_search = has_search OR :hs,
                        has_click = has_click OR :hc,
                        has_cart_event = has_cart_event OR :hce,
                        has_purchase = has_purchase OR :hp,
                        funnel_stage = :stage,
                        last_seen_at = :ts
                    WHERE session_id = :sid
                """),
                {
                    "hpv": int(flags["has_page_view"]), "hs": int(flags["has_search"]),
                    "hc": int(flags["has_click"]), "hce": int(flags["has_cart_event"]),
                    "hp": int(flags["has_purchase"]), "stage": final_stage,
                    "ts": timestamp, "sid": session_id,
                },
            )


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    consumer = make_consumer()
    engine = create_engine(mysql_connection_string(), pool_pre_ping=True)
    print(f"[{GROUP_ID}] polling all 5 topics ... Ctrl+C to stop.")

    try:
        for msg in consumer:
            event = msg.value
            if not event:
                continue
            session_id = event.get("session_id")
            user_id = event.get("user_id")
            event_type = event.get("event_type")
            timestamp = event.get("timestamp", now_iso())
            if not session_id or not event_type:
                continue
            upsert_funnel(engine, session_id, user_id, event_type, timestamp)
    except KeyboardInterrupt:
        print(f"\nStopping {GROUP_ID} ...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
