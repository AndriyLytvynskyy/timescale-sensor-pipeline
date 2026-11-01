import asyncio
import random
import os
import logging
from datetime import datetime, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

INGESTION_URL = os.getenv("INGESTION_URL", "http://ingestion-service:8000/events")
SEND_INTERVAL = float(os.getenv("SEND_INTERVAL", "1.0"))

# Each sensor does a random walk within [min, max] with Gaussian steps.
SENSOR_DEFINITIONS = [
    {"id": "temp-bldg-a-01", "type": "temperature", "location": "building-a/floor-1", "unit": "C",   "base": 22.0,    "step": 0.5,  "min": 15.0,  "max": 35.0},
    {"id": "temp-bldg-a-02", "type": "temperature", "location": "building-a/floor-2", "unit": "C",   "base": 23.5,    "step": 0.5,  "min": 15.0,  "max": 35.0},
    {"id": "temp-bldg-b-01", "type": "temperature", "location": "building-b/floor-1", "unit": "C",   "base": 19.0,    "step": 0.7,  "min": 15.0,  "max": 35.0},
    {"id": "hum-bldg-a-01",  "type": "humidity",    "location": "building-a/floor-1", "unit": "%",   "base": 60.0,    "step": 1.0,  "min": 20.0,  "max": 95.0},
    {"id": "hum-bldg-b-01",  "type": "humidity",    "location": "building-b/floor-1", "unit": "%",   "base": 55.0,    "step": 1.5,  "min": 20.0,  "max": 95.0},
    {"id": "press-roof-01",  "type": "pressure",    "location": "outdoor/roof",       "unit": "hPa", "base": 1013.25, "step": 0.5,  "min": 950.0, "max": 1050.0},
    {"id": "co2-bldg-a-01",  "type": "co2",         "location": "building-a/floor-1", "unit": "ppm", "base": 450.0,   "step": 10.0, "min": 380.0, "max": 2000.0},
    {"id": "co2-bldg-b-01",  "type": "co2",         "location": "building-b/floor-1", "unit": "ppm", "base": 600.0,   "step": 15.0, "min": 380.0, "max": 2000.0},
    {"id": "vib-machine-01", "type": "vibration",   "location": "factory/machine-1",  "unit": "mm/s","base": 2.5,     "step": 0.3,  "min": 0.0,   "max": 15.0},
    {"id": "vib-machine-02", "type": "vibration",   "location": "factory/machine-2",  "unit": "mm/s","base": 1.8,     "step": 0.2,  "min": 0.0,   "max": 15.0},
]


class Sensor:
    def __init__(self, cfg: dict):
        self.id = cfg["id"]
        self.type = cfg["type"]
        self.location = cfg["location"]
        self.unit = cfg["unit"]
        self._value = cfg["base"]
        self._step = cfg["step"]
        self._min = cfg["min"]
        self._max = cfg["max"]

    def _tick(self) -> float:
        self._value = max(self._min, min(self._max, self._value + random.gauss(0, self._step)))
        return round(self._value, 2)

    def event(self) -> dict:
        return {
            "sensor_id":   self.id,
            "sensor_type": self.type,
            "location":    self.location,
            "value":       self._tick(),
            "unit":        self.unit,
            "quality":     random.choices([100, 95, 90], weights=[90, 8, 2])[0],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }


async def _wait_for_ingestion(client: httpx.AsyncClient) -> None:
    health_url = INGESTION_URL.rsplit("/events", 1)[0] + "/health"
    for attempt in range(40):
        try:
            r = await client.get(health_url, timeout=3.0)
            if r.status_code == 200:
                log.info("Ingestion service ready")
                return
        except Exception:
            pass
        log.info("Waiting for ingestion service… attempt %d/40", attempt + 1)
        await asyncio.sleep(3)
    raise RuntimeError("Ingestion service did not become ready in time")


async def simulate() -> None:
    sensors = [Sensor(cfg) for cfg in SENSOR_DEFINITIONS]
    async with httpx.AsyncClient() as client:
        await _wait_for_ingestion(client)
        log.info("Simulating %d sensors every %.1f s", len(sensors), SEND_INTERVAL)
        while True:
            for sensor in sensors:
                event = sensor.event()
                try:
                    await client.post(INGESTION_URL, json=event, timeout=5.0)
                except Exception as exc:
                    log.warning("Failed to send %s: %s", sensor.id, exc)
            await asyncio.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    asyncio.run(simulate())
