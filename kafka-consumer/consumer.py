import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "sensor-events")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://postgres:password@timescaledb:5432/sensors")
BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT   = float(os.getenv("BATCH_TIMEOUT", "2.0"))

INSERT_SQL = """
    INSERT INTO sensor_events (time, sensor_id, sensor_type, location, value, unit, quality)
    VALUES %s
    ON CONFLICT DO NOTHING
"""


def _connect_db() -> psycopg2.extensions.connection:
    for attempt in range(30):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            log.info("Connected to TimescaleDB")
            return conn
        except Exception as exc:
            log.info("Waiting for TimescaleDB… attempt %d/30: %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to TimescaleDB after 30 attempts")


def _connect_kafka() -> KafkaConsumer:
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="sensor-consumer-group",
                value_deserializer=lambda m: json.loads(m.decode()),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                # exit the for-loop after BATCH_TIMEOUT ms of silence
                consumer_timeout_ms=int(BATCH_TIMEOUT * 1000),
            )
            log.info("Connected to Kafka, topic='%s'", KAFKA_TOPIC)
            return consumer
        except Exception as exc:
            log.info("Waiting for Kafka… attempt %d/30: %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 30 attempts")


def _flush(conn: psycopg2.extensions.connection, batch: list[dict]) -> None:
    if not batch:
        return
    rows = [
        (
            e.get("timestamp", datetime.now(timezone.utc).isoformat()),
            e["sensor_id"],
            e["sensor_type"],
            e["location"],
            e["value"],
            e["unit"],
            e.get("quality", 100),
        )
        for e in batch
    ]

    # Raw insert only. "Latest reading per sensor" is served on read by the
    # sensor_latest view (DISTINCT ON + SkipScan) — no hot upsert path here.
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    log.info("Inserted %d events", len(rows))


def run() -> None:
    conn     = _connect_db()
    consumer = _connect_kafka()
    batch: list[dict] = []

    while True:
        try:
            for msg in consumer:
                batch.append(msg.value)
                if len(batch) >= BATCH_SIZE:
                    _flush(conn, batch)
                    consumer.commit()
                    batch = []

            # consumer_timeout_ms elapsed — flush partial batch
            if batch:
                _flush(conn, batch)
                consumer.commit()
                batch = []

        except psycopg2.OperationalError:
            log.warning("DB connection lost, reconnecting…")
            conn = _connect_db()
        except Exception as exc:
            log.error("Unexpected error: %s", exc, exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    run()
