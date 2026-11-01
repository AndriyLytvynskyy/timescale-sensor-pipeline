import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from pydantic import BaseModel, model_validator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor-events")

_producer: KafkaProducer | None = None


def _build_producer() -> KafkaProducer:
    for attempt in range(30):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return p
        except Exception as exc:
            log.info("Waiting for Kafka… attempt %d/30: %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 30 attempts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _producer
    _producer = _build_producer()
    yield
    if _producer:
        _producer.close()


app = FastAPI(title="Sensor Ingestion Service", lifespan=lifespan)


class SensorEvent(BaseModel):
    sensor_id: str
    sensor_type: str
    location: str
    value: float
    unit: str
    quality: int = 100
    timestamp: str | None = None

    @model_validator(mode="after")
    def set_timestamp(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc).isoformat()
        return self


@app.post("/events", status_code=202)
async def ingest_event(event: SensorEvent):
    try:
        _producer.send(KAFKA_TOPIC, event.model_dump())
    except Exception as exc:
        log.error("Kafka publish failed: %s", exc)
        raise HTTPException(status_code=503, detail="Kafka unavailable")
    return {"status": "accepted"}


@app.post("/events/batch", status_code=202)
async def ingest_batch(events: list[SensorEvent]):
    for event in events:
        try:
            _producer.send(KAFKA_TOPIC, event.model_dump())
        except Exception as exc:
            log.error("Kafka publish failed: %s", exc)
            raise HTTPException(status_code=503, detail="Kafka unavailable")
    return {"status": "accepted", "count": len(events)}


@app.get("/health")
async def health():
    return {"status": "ok"}
