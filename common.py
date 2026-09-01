"""
Shared configuration and helpers for producers, consumers, and the dashboard.
"""
import os
import json
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "ecommerce")
MYSQL_USER = os.getenv("MYSQL_USER", "ecommerce_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "ecommerce_pass")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")

TOPICS = {
    "page_views": "page-views-topic",
    "search_queries": "search-queries-topic",
    "click_events": "click-events-topic",
    "cart_events": "cart-events-topic",
    "purchases": "purchases-topic",
}


def json_serializer(value: dict) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


def json_deserializer(value: bytes) -> dict:
    if value is None:
        return None
    return json.loads(value.decode("utf-8"))


def mysql_connection_string() -> str:
    return (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )
