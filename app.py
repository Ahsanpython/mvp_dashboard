# app.py
import os
from datetime import datetime
from pathlib import Path

import streamlit as st

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from sqlalchemy import create_engine, text
except Exception:
    create_engine = None
    text = None

try:
    from google.cloud import storage
except Exception:
    storage = None

from jobs import trigger_job


# -----------------------------
# ENV (set in Cloud Run Service)
# -----------------------------
GCP_PROJECT = (os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
GCP_REGION = (os.getenv("GCP_REGION") or "us-central1").strip()

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_OUTPUT_PREFIX = (os.getenv("GCS_OUTPUT_PREFIX") or "outputs").strip().strip("/")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()


# -----------------------------
# Cloud Run Job names (you create these)
# -----------------------------
JOBS = {
    "Google Maps": "maps-job",
    "Yelp": "yelp-job",
    "Hunter": "hunter-job",
    "YouTube": "youtube-job",
    "TikTok Hashtags": "tiktok-hashtags-job",
    "TikTok Followers": "tiktok-followers-job",
    "Instagram Combined": "instagram-combined-job",
    "Instagram Followers": "instagram-followers-job",
}

SOURCES = {
    "Google Maps": "maps",
    "Yelp": "yelp",
    "Hunter": "hunter",
    "YouTube": "youtube",
    "TikTok Hashtags": "tiktok_hashtags",
    "TikTok Followers": "tiktok_followers",
    "Instagram Combined": "instagram_combined",
    "Instagram Followers": "instagram_followers",
}


st.set_page_config(page_title="Ops Console", layout="wide")

st.markdown(
    """
    <style>
      .block-container { max-width: 1320px; padding-top: 1.0rem; padding-bottom: 1.5rem; }
      .topbar{
        border: 1px solid rgba(255,255,255,0.09);
        background: linear-gradient(135deg, rgba(124,58,237,0.22), rgba(17,26,46,0.65));
        border-radius: 18px;
        padding: 18px 18px 14px 18px;
      }
      .topbar h1{ margin:0; font-size: 24px; font-weight: 780; letter-spacing: 0.2px; }
      .topbar p{ margin: 6px 0 0 0; opacity: 0.80; font-size: 13px; }
      .panel{
        border: 1px solid rgba(255,255,255,0.09);
        background: rgba(17,26,46,0.62);
        border-radius: 16px;
        padding: 14px;
      }
      .panel h3{ margin:0; font-size: 14px; opacity: 0.92; font-weight: 750; }
      .muted{ opacity: 0.70; font-size: 12px; margin-top: 4px; }
      .stButton>button{ border-radius: 12px !important; padding: 0.65rem 0.95rem !important; font-weight: 720 !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="topbar">
      <h1>⚡ Ops Console</h1>
      <p>Trigger Cloud Run Jobs. View results from Cloud SQL. Browse outputs from GCS.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


def _db_engine():
    if not DATABASE_URL or create_engine is None:
        return None
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def _db_read_df(sql: str, params=None):
    if pd is None or text is None:
        return None
    eng = _db_engine()
    if not eng:
        return None
    return pd.read_sql_query(text(sql), eng, params=params or {})


def _gcs_client():
    if not GCS_BUCKET or storage is None:
        return None
    return storage.Client()


def _gcs_list(prefix: str, limit: int = 80):
    c = _gcs_client()
    if not c:
        return []
    bucket = c.bucket(GCS_BUCKET)
    items = []
    for b in bucket.list_blobs(prefix=prefix):
        name = b.name
        low = name.lower()
        if low.endswith(".xlsx") or low.endswith(".csv") or low.endswith(".json"):
            items.append({"name": name, "updated": b.updated, "size": b.size or 0})
        if len(items) >= limit * 3:
            break
    items.sort(key=lambda x: (x["updated"] or datetime.min), reverse=True)
    return items[:limit]


def _gcs_download_bytes(object_name: str) -> bytes:
    c = _gcs_client()
    if not c:
        return b""
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    return blob.download_as_bytes()


def _pipe_join(vals):
    return "|".join([str(v).strip() for v in vals if str(v).strip()])


def _trigger(job_short_name: str, env: dict):
    if not GCP_PROJECT:
        raise ValueError("Set GCP_PROJECT (or GOOGLE_CLOUD_PROJECT) in the dashboard service env vars.")
    return trigger_job(job_short_name, env_overrides=env, project_id=GCP_PROJECT, region=GCP_REGION)


with st.sidebar:
    st.markdown("### Cloud Run")
    st.write(f"Project: `{GCP_PROJECT or 'NOT SET'}`")
    st.write(f"Region: `{GCP_REGION}`")

    st.markdown("### Storage")
    st.write(f"GCS bucket: `{GCS_BUCKET or 'NOT SET'}`")
    st.write(f"Prefix: `{GCS_OUTPUT_PREFIX}`")

    st.markdown("### Database")
    st.write("Cloud SQL via `DATABASE_URL`")
    st.write("Status: " + ("✅ set" if DATABASE_URL else "❌ missing"))


tab_run, tab_results, tab_outputs = st.tabs(["🚀 Run", "📡 Results", "📦 Outputs"])

with tab_run:
    st.markdown('<div class="panel"><h3>Run a scraper</h3><div class="muted">Triggers a Cloud Run Job (long runs supported)</div></div>', unsafe_allow_html=True)
    st.write("")

    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        module = st.selectbox("Module", list(JOBS.keys()), index=0)
    with c2:
        run_label = st.text_input("Run label", value="")

    keywords = st.text_input("Keywords (pipe separated)", value="Medspa|Botox|peptide")
    city = st.text_input("City (optional)", value="")
    use_progress = st.checkbox("Use progress", value=False)

    if st.button("▶ Trigger Job", use_container_width=True):
        env = {
            "RUN_LABEL": run_label,
            "DATABASE_URL": DATABASE_URL,
            "GCS_BUCKET": GCS_BUCKET,
            "GCS_OUTPUT_PREFIX": GCS_OUTPUT_PREFIX,
            "SELECTED_KEYWORDS": keywords.strip(),
            "SELECTED_HASHTAGS": keywords.strip(),
            "USE_PROGRESS": "1" if use_progress else "0",
        }
        if city.strip():
            env["SELECTED_CITY"] = city.strip()
            env["CITY"] = city.strip()

        job_name = JOBS[module]
        res = _trigger(job_name, env)
        st.success("Triggered.")
        st.code(res.get("execution", ""), language="text")
        st.caption("Open Cloud Run → Jobs → Executions to watch progress.")

with tab_results:
    st.markdown('<div class="panel"><h3>Results</h3><div class="muted">Reads from Cloud SQL tables: runs + events</div></div>', unsafe_allow_html=True)
    st.write("")

    if not DATABASE_URL:
        st.error("DATABASE_URL not set in the dashboard service.")
    else:
        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            st.subheader("Runs")
            runs_df = _db_read_df("""
                SELECT id, source, label, status, started_at, finished_at
                FROM runs
                ORDER BY id DESC
                LIMIT 200
            """)
            if runs_df is not None:
                st.dataframe(runs_df, use_container_width=True, height=420)

        with right:
            st.subheader("Events")
            src = st.selectbox("Source", ["(all)"] + list(SOURCES.values()), index=0)
            limit = st.number_input("Rows", min_value=10, max_value=2000, value=200, step=10)

            if src == "(all)":
                events_df = _db_read_df("""
                    SELECT id, run_id, source, created_at, payload
                    FROM events
                    ORDER BY id DESC
                    LIMIT :lim
                """, {"lim": int(limit)})
            else:
                events_df = _db_read_df("""
                    SELECT id, run_id, source, created_at, payload
                    FROM events
                    WHERE source = :src
                    ORDER BY id DESC
                    LIMIT :lim
                """, {"src": src, "lim": int(limit)})

            if events_df is not None:
                st.dataframe(events_df, use_container_width=True, height=420)

with tab_outputs:
    st.markdown('<div class="panel"><h3>Outputs</h3><div class="muted">Browse GCS output files</div></div>', unsafe_allow_html=True)
    st.write("")

    if not GCS_BUCKET:
        st.error("GCS_BUCKET not set.")
    else:
        prefix = f"{GCS_OUTPUT_PREFIX}/"
        items = _gcs_list(prefix=prefix, limit=120)
        if not items:
            st.caption("No files yet.")
        else:
            for it in items:
                name = it["name"]
                rel = name[len(prefix):] if name.startswith(prefix) else name
                updated = it["updated"].strftime("%Y-%m-%d %H:%M:%S") if it["updated"] else ""
                size_kb = int((it["size"] or 0) / 1024)

                a, b, c = st.columns([0.60, 0.15, 0.25], vertical_alignment="center")
                with a:
                    st.write(rel)
                    st.caption(updated)
                with b:
                    st.write(f"{size_kb} KB")
                with c:
                    data = _gcs_download_bytes(name)
                    st.download_button(
                        "Download",
                        data=data,
                        file_name=Path(name).name,
                        mime="application/octet-stream",
                        key=f"dl_{name}",
                        use_container_width=True,
                    )
