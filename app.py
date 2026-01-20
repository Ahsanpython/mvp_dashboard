# app.py
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

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
GCS_INPUT_PREFIX = (os.getenv("GCS_INPUT_PREFIX") or "inputs").strip().strip("/")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

# Optional: comma-separated list of cities to show as dropdown
CITY_LIST = [c.strip() for c in (os.getenv("CITY_LIST") or "").split(",") if c.strip()]
if not CITY_LIST:
    # sensible starter list (edit anytime)
    CITY_LIST = [
        "Miami, FL", "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
        "Dallas, TX", "Phoenix, AZ", "San Diego, CA", "San Jose, CA", "Austin, TX",
        "Seattle, WA", "Boston, MA", "Atlanta, GA", "Denver, CO", "Las Vegas, NV",
    ]

# -----------------------------
# Cloud Run Job names (match what you actually created)
# Based on your logs: namespaces/.../jobs/job-maps
# -----------------------------
JOBS = {
    "Google Maps": "job-maps",
    "Yelp": "job-yelp",
    "Hunter": "job-hunter",
    "YouTube": "job-youtube",
    "TikTok Hashtags": "job-tiktok-hashtags",
    "TikTok Followers": "job-tiktok-followers",
    "Instagram Combined": "job-instagram-combined",
    "Instagram Followers": "job-instagram-followers",
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

# Modules that need city
CITY_MODULES = {"Google Maps", "Yelp"}

# Hunter needs Yelp output input
HUNTER_NEEDS_YELP_INPUT = True

# Followers modules need special input choices
FOLLOWER_MODULES = {"TikTok Followers", "Instagram Followers"}

# -----------------------------
# UI
# -----------------------------
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
      code { font-size: 12px !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="topbar">
      <h1>⚡ Ops Console</h1>
      <p>Trigger Cloud Run Jobs. View runs/events in Cloud SQL. Download output files from GCS.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


# -----------------------------
# Helpers: DB
# -----------------------------
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


# -----------------------------
# Helpers: GCS
# -----------------------------
def _gcs_client():
    if not GCS_BUCKET or storage is None:
        return None
    return storage.Client()


def _gcs_list(prefix: str, limit: int = 120) -> List[Dict[str, Any]]:
    c = _gcs_client()
    if not c:
        return []
    bucket = c.bucket(GCS_BUCKET)
    items = []
    for b in bucket.list_blobs(prefix=prefix):
        name = b.name
        low = name.lower()
        if low.endswith(".xlsx_toggle") or low.endswith(".tmp"):
            continue
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


def _gcs_upload_bytes(object_name: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    c = _gcs_client()
    if not c:
        raise RuntimeError("GCS client not available. Set GCS_BUCKET and give service account access.")
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{GCS_BUCKET}/{object_name}"


def _pipe_join(vals: List[str]) -> str:
    cleaned = [str(v).strip() for v in vals if str(v).strip()]
    return "|".join(cleaned)


# -----------------------------
# Helpers: Job trigger payload
# -----------------------------
def _env_list(env: Dict[str, str]) -> List[Dict[str, str]]:
    out = []
    for k, v in env.items():
        if v is None:
            continue
        vv = str(v)
        out.append({"name": str(k), "value": vv})
    return out


def _build_overrides(env: Dict[str, str]) -> Dict[str, Any]:
    # Cloud Run Jobs v2 "run" expects containerOverrides
    return {"containerOverrides": [{"env": _env_list(env)}]}

def _trigger(job_short_name: str, env: dict):
    return trigger_job(job_short_name, env_overrides=env, project_id=GCP_PROJECT, region=GCP_REGION)



# -----------------------------
# Sidebar status
# -----------------------------
with st.sidebar:
    st.markdown("### Cloud Run")
    st.write(f"Project: `{GCP_PROJECT or 'NOT SET'}`")
    st.write(f"Region: `{GCP_REGION}`")

    st.markdown("### Storage")
    st.write(f"GCS bucket: `{GCS_BUCKET or 'NOT SET'}`")
    st.write(f"Outputs prefix: `{GCS_OUTPUT_PREFIX}`")
    st.write(f"Inputs prefix: `{GCS_INPUT_PREFIX}`")

    st.markdown("### Database")
    st.write("Cloud SQL via `DATABASE_URL`")
    st.write("Status: " + ("✅ set" if DATABASE_URL else "❌ missing"))

    st.markdown("### Debug")
    st.caption("If trigger says OK=false, open the response below and fix that first.")


# -----------------------------
# Tabs
# -----------------------------
tab_run, tab_results, tab_outputs = st.tabs(["🚀 Run", "📡 Results", "📦 Outputs"])

# -----------------------------
# RUN tab
# -----------------------------
with tab_run:
    st.markdown(
        '<div class="panel"><h3>Run a scraper</h3><div class="muted">Starts a Cloud Run Job with module-specific inputs</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        module = st.selectbox("Module", list(JOBS.keys()), index=0)
    with c2:
        run_label = st.text_input("Run label (optional)", value="")

    # Keyword list UX (shows all keywords and lets user pick)
    st.markdown("#### Keywords / Hashtags")
    default_keywords_text = "Medspa\nBotox\nPeptide"
    keywords_text = st.text_area(
        "Paste one per line",
        value=default_keywords_text,
        height=110,
        help="One keyword per line. You can select which ones to run below.",
    )
    all_keywords = [k.strip() for k in keywords_text.splitlines() if k.strip()]
    if not all_keywords:
        all_keywords = []

    kcol1, kcol2, kcol3 = st.columns([0.33, 0.33, 0.34])
    with kcol1:
        select_all = st.checkbox("Select all", value=True)
    with kcol2:
        use_progress = st.checkbox("Use progress", value=False)
    with kcol3:
        followers_limit_mode = None  # only used for follower modules

    if select_all:
        selected_keywords = all_keywords
    else:
        selected_keywords = st.multiselect("Select keywords", options=all_keywords, default=all_keywords[:2] if all_keywords else [])

    selected_keywords_pipe = _pipe_join(selected_keywords)

    # City only for Maps/Yelp
    selected_city = ""
    if module in CITY_MODULES:
        st.markdown("#### City")
        city_mode = st.radio("City input", ["Dropdown", "Type manually"], horizontal=True)
        if city_mode == "Dropdown":
            selected_city = st.selectbox("Select city", CITY_LIST, index=0 if CITY_LIST else 0)
        else:
            selected_city = st.text_input("City", value="")

    # Hunter needs Yelp input file
    yelp_input_gcs = ""
    if module == "Hunter" and HUNTER_NEEDS_YELP_INPUT:
        st.markdown("#### Hunter input")
        if not GCS_BUCKET:
            st.error("Set GCS_BUCKET in the dashboard service to pick Yelp output files.")
        else:
            yelp_prefix = f"{GCS_OUTPUT_PREFIX}/"
            output_items = _gcs_list(prefix=yelp_prefix, limit=200)
            # filter to likely yelp outputs
            yelp_like = [it for it in output_items if "yelp" in it["name"].lower()]
            options = ["(pick one)"] + [it["name"] for it in yelp_like]
            pick = st.selectbox("Pick a Yelp output file from GCS", options, index=0)
            if pick != "(pick one)":
                yelp_input_gcs = f"gs://{GCS_BUCKET}/{pick}"
            st.caption("Hunter will use this Yelp file as input (emails/domain enrichment).")

    # Followers modules: input choice + limit
    uploaded_gcs_path = ""
    single_username = ""
    followers_limit = ""

    if module in FOLLOWER_MODULES:
        st.markdown("#### Followers input")
        input_mode = st.radio(
            "Where do usernames come from?",
            ["Single username", "Upload file (CSV/TXT)"],
            horizontal=True,
        )

        if input_mode == "Single username":
            single_username = st.text_input("Username", value="")
        else:
            up = st.file_uploader("Upload a file with usernames (one per line)", type=["txt", "csv"])
            if up is not None:
                if not GCS_BUCKET:
                    st.error("Set GCS_BUCKET in the dashboard service to upload this file to GCS.")
                else:
                    data = up.read()
                    safe_name = up.name.replace(" ", "_")
                    object_name = f"{GCS_INPUT_PREFIX}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{safe_name}"
                    try:
                        uploaded_gcs_path = _gcs_upload_bytes(object_name, data)
                        st.success(f"Uploaded to {uploaded_gcs_path}")
                    except Exception as e:
                        st.error(str(e))

        followers_limit_mode = st.radio("How many followers?", ["All", "5", "10", "Custom"], horizontal=True)
        if followers_limit_mode == "All":
            followers_limit = "all"
        elif followers_limit_mode in {"5", "10"}:
            followers_limit = followers_limit_mode
        else:
            followers_limit = str(st.number_input("Custom limit", min_value=1, max_value=5000000, value=100, step=10))

    # Trigger button
    st.write("")
    if st.button("▶ Trigger Job", use_container_width=True):
        # Base env shared by all jobs
        env: Dict[str, str] = {
            "RUN_LABEL": run_label.strip(),
            "SOURCE": SOURCES.get(module, ""),
            "DATABASE_URL": DATABASE_URL,
            "GCS_BUCKET": GCS_BUCKET,
            "GCS_OUTPUT_PREFIX": GCS_OUTPUT_PREFIX,
            "GCS_INPUT_PREFIX": GCS_INPUT_PREFIX,
            "USE_PROGRESS": "1" if use_progress else "0",
        }

        # Keywords/hashtags
        if selected_keywords_pipe:
            env["SELECTED_KEYWORDS"] = selected_keywords_pipe
            env["KEYWORDS"] = selected_keywords_pipe
            env["SELECTED_HASHTAGS"] = selected_keywords_pipe  # for hashtag scrapers if they expect this

        # City only where relevant
        if module in CITY_MODULES and selected_city.strip():
            env["SELECTED_CITY"] = selected_city.strip()
            env["CITY"] = selected_city.strip()

        # Hunter Yelp input
        if module == "Hunter":
            if not yelp_input_gcs:
                st.error("Pick a Yelp output file for Hunter first.")
                st.stop()
            env["YELP_INPUT_GCS"] = yelp_input_gcs

        # Followers inputs
        if module in FOLLOWER_MODULES:
            env["FOLLOWERS_LIMIT"] = followers_limit
            if single_username.strip():
                env["USERNAMES_MODE"] = "single"
                env["USERNAME"] = single_username.strip()
            elif uploaded_gcs_path.strip():
                env["USERNAMES_MODE"] = "file"
                env["USERNAMES_GCS"] = uploaded_gcs_path.strip()
            else:
                st.error("Provide a username or upload a file first.")
                st.stop()

        job_name = JOBS[module]

        # Trigger and show real response
        res = _trigger(job_name, env)

        if not isinstance(res, dict):
            st.error("Trigger returned a non-dict response. Check jobs.py.")
            st.stop()

        if res.get("ok") is True:
            st.success("Triggered. Open Cloud Run → Jobs → Executions to watch progress.")
        else:
            st.error("Trigger failed. Read the response below and fix that first.")

        st.markdown("##### Trigger response")
        st.code(json.dumps(res, indent=2), language="json")

        st.markdown("##### What to check if nothing runs")
        st.write(
            "- Confirm the job exists in the same project/region.\n"
            "- Confirm the dashboard service account has `run.jobsRunner` (or `run.developer`) permission.\n"
            "- Confirm job name matches exactly (example: `job-maps`)."
        )


# -----------------------------
# RESULTS tab
# -----------------------------
with tab_results:
    st.markdown(
        '<div class="panel"><h3>Results</h3><div class="muted">Reads from Cloud SQL tables: runs + events</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    if not DATABASE_URL:
        st.error("DATABASE_URL not set in the dashboard service.")
    else:
        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            st.subheader("Runs")
            runs_df = _db_read_df(
                """
                SELECT id, run_id, source, label, status, started_at, finished_at, created_at
                FROM runs
                ORDER BY id DESC
                LIMIT 200
                """
            )
            if runs_df is not None:
                st.dataframe(runs_df, use_container_width=True, height=420)
            else:
                st.caption("No runs yet (or DB not reachable).")

        with right:
            st.subheader("Events")
            src = st.selectbox("Source", ["(all)"] + list(SOURCES.values()), index=0)
            limit = st.number_input("Rows", min_value=10, max_value=2000, value=200, step=10)

            if src == "(all)":
                events_df = _db_read_df(
                    """
                    SELECT id, run_id, source, created_at, payload
                    FROM events
                    ORDER BY id DESC
                    LIMIT :lim
                    """,
                    {"lim": int(limit)},
                )
            else:
                events_df = _db_read_df(
                    """
                    SELECT id, run_id, source, created_at, payload
                    FROM events
                    WHERE source = :src
                    ORDER BY id DESC
                    LIMIT :lim
                    """,
                    {"src": src, "lim": int(limit)},
                )

            if events_df is not None:
                st.dataframe(events_df, use_container_width=True, height=420)
            else:
                st.caption("No events yet (or DB not reachable).")


# -----------------------------
# OUTPUTS tab
# -----------------------------
with tab_outputs:
    st.markdown(
        '<div class="panel"><h3>Outputs</h3><div class="muted">Browse GCS output files</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    if not GCS_BUCKET:
        st.error("GCS_BUCKET not set.")
    else:
        prefix = f"{GCS_OUTPUT_PREFIX}/"
        items = _gcs_list(prefix=prefix, limit=160)
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
