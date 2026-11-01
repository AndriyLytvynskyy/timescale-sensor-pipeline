import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.pool
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@timescaledb:5432/sensors")

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=10, dsn=DATABASE_URL)
    log.info("DB connection pool ready")
    yield
    _pool.closeall()


app = FastAPI(title="Sensor Query API", version="2.0.0", lifespan=lifespan)


def get_db():
    """Yields a pooled connection. Read-only endpoints; rollback resets state."""
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        conn.rollback()
        _pool.putconn(conn)


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


# ─── Latest readings ──────────────────────────────────────────────────────────

@app.get("/v1/sensors/latest")
def get_latest(
    sensor_type: str | None = Query(None, description="Filter by sensor type"),
    location:    str | None = Query(None, description="Filter by location prefix"),
    limit:       int        = Query(100, le=1000),
    conn                    = Depends(get_db),
):
    """Latest reading per sensor. Served by the sensor_latest view (SkipScan)."""
    where  = ["1=1"]
    params: dict = {}

    if sensor_type:
        where.append("sensor_type = %(sensor_type)s")
        params["sensor_type"] = sensor_type
    if location:
        where.append("location LIKE %(location_prefix)s")
        params["location_prefix"] = f"{location}%"

    params["limit"] = limit

    sql = f"""
        SELECT sensor_id, sensor_type, location,
               round(value::numeric, 2) AS value,
               unit, quality, last_seen
        FROM sensor_latest
        WHERE {" AND ".join(where)}
        ORDER BY sensor_type, sensor_id
        LIMIT %(limit)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return {"data": rows, "count": len(rows)}


# ─── Summary by type ──────────────────────────────────────────────────────────

@app.get("/v1/sensors/summary")
def get_summary(conn = Depends(get_db)):
    """Aggregated current state by sensor type."""
    sql = """
        SELECT
            sensor_type,
            COUNT(*)                                                         AS total,
            ROUND(AVG(value)::numeric, 2)                                    AS avg_value,
            ROUND(MIN(value)::numeric, 2)                                    AS min_value,
            ROUND(MAX(value)::numeric, 2)                                    AS max_value,
            COUNT(*) FILTER (WHERE last_seen < now() - INTERVAL '2 minutes') AS stale
        FROM sensor_latest
        GROUP BY sensor_type
        ORDER BY sensor_type
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return {"data": rows}


# ─── Alerts ───────────────────────────────────────────────────────────────────

@app.get("/v1/sensors/alerts")
def get_alerts(
    since: int = Query(120, description="Silence threshold in seconds"),
    limit: int = Query(100, le=1000),
    conn       = Depends(get_db),
):
    """Sensors that haven't reported within `since` seconds."""
    params = {"since": since, "limit": limit}
    sql = """
        SELECT
            sensor_id, sensor_type, location, last_seen,
            EXTRACT(EPOCH FROM (now() - last_seen))::int AS seconds_ago
        FROM sensor_latest
        WHERE last_seen < now() - (INTERVAL '1 second' * %(since)s)
        ORDER BY last_seen ASC
        LIMIT %(limit)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    return {"data": rows, "count": len(rows)}


# ─── History (time series) ────────────────────────────────────────────────────

@app.get("/v1/sensors/{sensor_id}/history")
def get_history(
    sensor_id: str,
    from_ts:   datetime  = Query(
                    default_factory=lambda: datetime.now(timezone.utc) - timedelta(hours=1),
                    alias="from",
                ),
    to_ts:     datetime  = Query(
                    default_factory=lambda: datetime.now(timezone.utc),
                    alias="to",
                ),
    cursor:    str | None = Query(None, description="Last bucket ISO timestamp for pagination"),
    limit:     int        = Query(100, le=1000),
    conn                  = Depends(get_db),
):
    """
    1-minute aggregates for a sensor over a time range.
    Uses cursor-based pagination — pass next_cursor from the previous response.
    """
    from_ts = from_ts.replace(tzinfo=timezone.utc) if from_ts.tzinfo is None else from_ts
    to_ts   = to_ts.replace(tzinfo=timezone.utc)   if to_ts.tzinfo is None   else to_ts

    params: dict = {"sensor_id": sensor_id, "from_ts": from_ts, "to_ts": to_ts, "limit": limit}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM sensor_latest WHERE sensor_id = %(sensor_id)s",
            {"sensor_id": sensor_id},
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Sensor not found")

    cursor_clause = ""
    if cursor:
        cursor_clause = "AND bucket > %(cursor)s"
        params["cursor"] = cursor

    sql = f"""
        SELECT bucket AS time, avg_value, min_value, max_value, stddev_value, event_count
        FROM sensor_stats_1min
        WHERE sensor_id = %(sensor_id)s
          {cursor_clause}
          AND bucket >= %(from_ts)s
          AND bucket <  %(to_ts)s
        ORDER BY bucket ASC
        LIMIT %(limit)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    next_cursor = rows[-1]["time"].isoformat() if rows else None

    return {
        "sensor_id":   sensor_id,
        "from":        from_ts.isoformat(),
        "to":          to_ts.isoformat(),
        "data":        rows,
        "count":       len(rows),
        "next_cursor": next_cursor,
    }
