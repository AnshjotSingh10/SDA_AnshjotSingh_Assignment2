"""
Creates the 5 Kafka topics used by the pipeline, each partitioned with
replication factor 3 (matching '3. KAFKA CLUSTER' in the architecture diagram).

Run this once after the Kafka cluster (3 brokers + ZooKeeper) is up:
    python create_topics.py
"""
import os
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

TOPICS = [
    "page-views-topic",
    "search-queries-topic",
    "click-events-topic",
    "cart-events-topic",
    "purchases-topic",
]

PARTITIONS = 3
REPLICATION_FACTOR = 3


def main():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="topic-creator",
    )

    new_topics = [
        NewTopic(name=t, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for t in TOPICS
    ]

    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        print(f"Created topics: {TOPICS}")
    except TopicAlreadyExistsError:
        print("Some topics already exist — skipping those.")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
