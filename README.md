# Real-Time E-Commerce Clickstream & Conversion Pipeline

Full working implementation of the architecture diagram:
Faker-driven producers → Kafka (3 brokers + ZooKeeper) → 3 consumer groups → MySQL + MongoDB → Streamlit dashboard.

## 0. Prerequisites

- Docker + Docker Compose installed
- Python 3.9+
- ~4 GB free RAM for the containers (3 Kafka brokers + ZooKeeper + MySQL + MongoDB)

## 1. Start the infrastructure

```bash
cd ecommerce-kafka-pipeline
docker compose up -d
```

This starts:
- ZooKeeper (`localhost:2181`)
- 3 Kafka brokers (`localhost:9092`, `9093`, `9094`)
- Kafka UI at http://localhost:8080 (visually confirm topics/partitions/brokers)
- MySQL at `localhost:3306` (db `ecommerce`, auto-loads `storage/mysql_schema.sql`)
- MongoDB at `localhost:27017` (db `ecommerce`)

Check everything is healthy:
```bash
docker compose ps
```
Wait ~30-60s for Kafka brokers to fully elect a controller before continuing.

## 2. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

`.env` is already configured for the default docker-compose ports — edit it if you change anything.

## 3. Create the Kafka topics

```bash
python create_topics.py
```

Creates, each with 3 partitions / replication factor 3 (matches the diagram):
- `page-views-topic`
- `search-queries-topic`
- `click-events-topic`
- `cart-events-topic`
- `purchases-topic`

Verify in Kafka UI (http://localhost:8080) under Topics.

## 4. Run the producers (live Faker-generated traffic)

Open **two terminals** (both with the venv activated):

```bash
# Terminal 1 — client-side tracker (page views, search, clicks, cart)
python producers/web_app_tracker_producer.py
```

```bash
# Terminal 2 — backend checkout service (purchase-confirmed events)
python producers/checkout_service_producer.py
```

`web_app_tracker_producer.py` continuously simulates `NUM_CONCURRENT_SESSIONS` (default 25,
set in `.env`) concurrent shoppers with Faker, each randomly walking the funnel:
page view(s) → maybe search → maybe click → maybe add-to-cart → maybe remove.

`checkout_service_producer.py` watches carts created by the tracker (via
`shared_state/active_carts.jsonl`, standing in for a real cart microservice lookup) and,
after a realistic delay, converts ~55% of them into confirmed purchases — the rest are left
to time out, which is what feeds cart-abandonment detection downstream.

## 5. Run the consumer groups

Open **three more terminals**:

```bash
# Group A — Session & Funnel Consumer (all 5 topics -> MySQL session_funnel)
python consumers/group_a_session_funnel.py
```

```bash
# Group B — Personalization Consumer (views/search/clicks -> MongoDB user_events)
python consumers/group_b_personalization.py
```

```bash
# Group C — Cart Abandonment & Marketing Consumer (cart+purchases -> MySQL orders,
# MongoDB cart_status + abandonment_alerts)
python consumers/group_c_cart_abandonment.py
```

Each runs independently as its own Kafka consumer group, exactly like "each group
independently reads the topics it needs" in the diagram.

## 6. Launch the live dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 — it auto-refreshes every 5s and shows:
- **Live Funnel & Conversion**: bar chart of sessions per funnel stage, order count/revenue, recent orders
- **Cart Abandonment & Discount Alerts**: table of flagged abandoned carts + live cart-status breakdown

## 7. Shutting down

```bash
# Ctrl+C each Python process, then:
docker compose down          # keep volumes (data persists)
docker compose down -v       # wipe all data too
```

## Project layout

```
ecommerce-kafka-pipeline/
├── docker-compose.yml          # ZooKeeper + 3 Kafka brokers + Kafka UI + MySQL + MongoDB
├── requirements.txt
├── .env                        # shared config (bootstrap servers, DB creds, tuning)
├── common.py                   # shared config/serializers used by all scripts
├── create_topics.py            # one-time: creates the 5 Kafka topics
├── producers/
│   ├── data_catalog.py         # Faker-generated product/user catalog
│   ├── web_app_tracker_producer.py    # page views, search, clicks, cart events
│   └── checkout_service_producer.py   # purchase-confirmed events
├── consumers/
│   ├── group_a_session_funnel.py      # -> MySQL session_funnel
│   ├── group_b_personalization.py     # -> MongoDB user_events
│   └── group_c_cart_abandonment.py    # -> MySQL orders, MongoDB cart_status/alerts
├── storage/
│   └── mysql_schema.sql        # orders + session_funnel tables
├── dashboard/
│   └── app.py                  # Streamlit live dashboard
└── shared_state/
    └── active_carts.jsonl      # created at runtime; hand-off between the two producers
```

## Tuning live traffic

Edit `.env`:
- `NUM_CONCURRENT_SESSIONS` — how many simulated shoppers run in parallel (more = higher event rate)
- `CART_CONVERSION_RATE` (in `checkout_service_producer.py`, or set as env var) — % of carts that convert to purchase
- `ABANDON_TIMEOUT_SECONDS` (in `group_c_cart_abandonment.py`, or set as env var) — how long an idle cart waits before being flagged abandoned (default 30s, set low for demo purposes — use something like 900s/15min in a real system)

## Notes / production caveats

This is a **local, single-machine simulation** built to demonstrate the full architecture end-to-end:
- The "hand-off" between the two producers uses a local JSONL file rather than the two backend
  services querying a shared cart database — swap that for a real cart service/API in production.
- Kafka is running with `PLAINTEXT` listeners and no auth — add SASL/SSL for anything beyond local dev.
- `KafkaConsumer`'s default `enable_auto_commit=True` is fine for a demo; use manual commits + idempotent
  writes (or a transactional sink) for exactly-once guarantees in production.
- MySQL writes use `INSERT IGNORE` on `order_id` to stay idempotent on consumer restarts/rebalances.
