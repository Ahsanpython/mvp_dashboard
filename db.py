# db.py
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB

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


def _jsonable(obj: Any) -> Any:
    """
    Convert pandas / datetime / weird objects into JSON-safe types.
    Always returns something that json.dumps can handle.
    """
    if obj is None:
        return None

    # pandas NaN / NA
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    # datetimes
    if isinstance(obj, datetime):
        return obj.isoformat()

    # basic containers: recurse
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]

    # everything else: keep if json can handle, otherwise stringify
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)


def _as_jsonb(v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Make sure we always pass a JSONB-friendly python object to SQLAlchemy.
    """
    if not v:
        return None
    return _jsonable(v)


def init_db():
    """
    Creates tables + indexes if needed.
    If your DB user is not the owner, CREATE INDEX can fail with:
    'must be owner of table ...'
    """
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

        try:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_events_source_created ON events(source, created_at DESC);"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);"
            ))
        except Exception as e:
            print(f"[db] index create skipped: {e}", flush=True)


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

    meta2 = _as_jsonb(meta)

    # Use explicit JSONB bind to avoid psycopg2 "can't adapt dict" issues
    stmt = text("""
        UPDATE runs
        SET status=:status, finished_at=:finished_at, meta=:meta
        WHERE id=:id
    """).bindparams(meta=JSONB)

    with eng.begin() as conn:
        conn.execute(
            stmt,
            {
                "id": int(run_id),
                "status": status,
                "finished_at": _utcnow(),
                "meta": meta2,
            },
        )


def insert_event(run_id: int, source: str, payload: Dict[str, Any]):
    init_db()
    eng = _engine()

    payload2 = _as_jsonb(payload) or {}

    stmt = text("""
        INSERT INTO events (run_id, source, created_at, payload)
        VALUES (:run_id, :source, :created_at, :payload)
    """).bindparams(payload=JSONB)

    with eng.begin() as conn:
        conn.execute(
            stmt,
            {
                "run_id": int(run_id),
                "source": source,
                "created_at": _utcnow(),
                "payload": payload2,
            },
        )


def insert_df(run_id: int, source: str, df: pd.DataFrame):
    if df is None or df.empty:
        return

    init_db()
    eng = _engine()

    df2 = df.copy()
    df2 = df2.where(pd.notnull(df2), None)

    rows = df2.to_dict(orient="records")

    stmt = text("""
        INSERT INTO events (run_id, source, created_at, payload)
        VALUES (:run_id, :source, :created_at, :payload)
    """).bindparams(payload=JSONB)

    with eng.begin() as conn:
        for r in rows:
            conn.execute(
                stmt,
                {
                    "run_id": int(run_id),
                    "source": source,
                    "created_at": _utcnow(),
                    "payload": _as_jsonb(r) or {},
                },
            )
