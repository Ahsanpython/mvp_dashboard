# db.py
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
_ENGINE = None


def _utcnow():
    return datetime.now(timezone.utc)


def _engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is empty. Set it in Cloud Run Service + Jobs env vars.")
    _ENGINE = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
        pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
    )
    return _ENGINE


def init_db():
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS runs (
            id BIGSERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            label TEXT DEFAULT '',
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'running',
            meta JSONB
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            run_id BIGINT REFERENCES runs(id) ON DELETE SET NULL,
            source TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_source_created ON events(source, created_at DESC);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);"))


def start_run(source: str, label: str = "") -> int:
    init_db()
    eng = _engine()
    with eng.begin() as conn:
        res = conn.execute(
            text("""
                INSERT INTO runs (source, label, started_at, status)
                VALUES (:source, :label, :started_at, 'running')
                RETURNING id
            """),
            {"source": source, "label": label or "", "started_at": _utcnow()},
        )
        return int(res.scalar())


def finish_run(run_id: int, status: str, meta: Optional[Dict[str, Any]] = None):
    init_db()
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(
            text("""
                UPDATE runs
                SET status=:status, finished_at=:finished_at, meta=:meta
                WHERE id=:id
            """),
            {
                "id": int(run_id),
                "status": status,
                "finished_at": _utcnow(),
                "meta": meta if meta else None,
            },
        )


def insert_event(run_id: int, source: str, payload: Dict[str, Any]):
    init_db()
    eng = _engine()
    with eng.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO events (run_id, source, created_at, payload)
                VALUES (:run_id, :source, :created_at, :payload)
            """),
            {
                "run_id": int(run_id),
                "source": source,
                "created_at": _utcnow(),
                "payload": payload,
            },
        )


def insert_df(run_id: int, source: str, df: pd.DataFrame):
    """
    Safe for ANY columns. Stores each row as JSON in events.payload.
    """
    if df is None or df.empty:
        return

    init_db()
    eng = _engine()

    df2 = df.copy()
    df2 = df2.where(pd.notnull(df2), None)

    rows = df2.to_dict(orient="records")

    with eng.begin() as conn:
        for r in rows:
            conn.execute(
                text("""
                    INSERT INTO events (run_id, source, created_at, payload)
                    VALUES (:run_id, :source, :created_at, :payload)
                """),
                {
                    "run_id": int(run_id),
                    "source": source,
                    "created_at": _utcnow(),
                    "payload": json.loads(json.dumps(r, default=str)),
                },
            )
