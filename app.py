# app.py
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

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
# ENV
# -----------------------------
GCP_PROJECT = (os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
GCP_REGION = (os.getenv("GCP_REGION") or "us-central1").strip()

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_OUTPUT_PREFIX = (os.getenv("GCS_OUTPUT_PREFIX") or "outputs").strip().strip("/")
GCS_INPUT_PREFIX = (os.getenv("GCS_INPUT_PREFIX") or "inputs").strip().strip("/")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

# Cities list (dropdown)
CITY_LIST = [c.strip() for c in (os.getenv("CITY_LIST") or "").split(",") if c.strip()]
if not CITY_LIST:
    CITY_LIST = [
        "Miami, FL", "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
        "Dallas, TX", "Phoenix, AZ", "San Diego, CA", "San Jose, CA", "Austin, TX",
        "Seattle, WA", "Boston, MA", "Atlanta, GA", "Denver, CO", "Las Vegas, NV",
    ]


# -----------------------------
# Module config
# IMPORTANT: Dashboard cannot read keywords from each job's python file.
# You must provide keywords for each module via env MODULE_CONFIG_JSON (recommended).
# Supported JSON patterns:
# 1) list keywords:
#    {"YouTube": {"keywords": ["peptide","biohacking"]}}
# 2) grouped keywords:
#    {"YouTube": {"keyword_groups": {"Peptide": ["peptide","bpc157"], "Biohacking": ["biohacking"]}}}
# -----------------------------
DEFAULT_MODULES: Dict[str, Dict[str, Any]] = {
    "Google Maps": {
        "job": "job-maps",
        "source": "maps",
        "needs_city": True,
        "needs_keywords": True,
        "keywords": [],
    },
    "Yelp": {
        "job": "job-yelp",
        "source": "yelp",
        "needs_city": True,
        "needs_keywords": True,
        "keywords": [],
    },
    "Hunter": {
        "job": "job-hunter",
        "source": "hunter",
        "needs_city": False,
        "needs_keywords": False,
        "needs_yelp_input": True,
        "keywords": [],
    },
    "YouTube": {
        "job": "job-youtube",
        "source": "youtube",
        "needs_city": False,
        "needs_keywords": True,
        "keywords": [],
        "keyword_groups": {},
    },
    "TikTok Hashtags": {
        "job": "job-tiktok-hashtags",
        "source": "tiktok_hashtags",
        "needs_city": False,
        "needs_keywords": True,
        "keywords": [],
        "keyword_groups": {},
    },
    "TikTok Followers": {
        "job": "job-tiktok-followers",
        "source": "tiktok_followers",
        "needs_city": False,
        "needs_keywords": False,
        "followers_module": True,
        "keywords": [],
    },
    "Instagram Combined": {
        "job": "job-instagram-combined",
        "source": "instagram_combined",
        "needs_city": False,
        "needs_keywords": True,
        "keywords": [],
        "keyword_groups": {},
    },
    "Instagram Followers": {
        "job": "job-instagram-followers",
        "source": "instagram_followers",
        "needs_city": False,
        "needs_keywords": False,
        "followers_module": True,
        "keywords": [],
    },
}

MODULES = json.loads(json.dumps(DEFAULT_MODULES))
raw_cfg = (os.getenv("MODULE_CONFIG_JSON") or "").strip()
if raw_cfg:
    try:
        override = json.loads(raw_cfg)
        if isinstance(override, dict):
            for k, v in override.items():
                if k in MODULES and isinstance(v, dict):
                    MODULES[k].update(v)
    except Exception:
        pass


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
      .topbar h1{ margin:0; font-size: 22px; font-weight: 780; letter-spacing: 0.2px; }
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
      .small-note { opacity: 0.75; font-size: 12px; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="topbar">
      <h1>Ops Console</h1>
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
        raise RuntimeError("GCS client not available.")
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
        out.append({"name": str(k), "value": str(v)})
    return out


def _build_overrides(env: Dict[str, str]) -> Dict[str, Any]:
    return {"containerOverrides": [{"env": _env_list(env)}]}


def _trigger(job_name: str, env: Dict[str, str]) -> Dict[str, Any]:
    overrides = _build_overrides(env)
    return trigger_job(job_name, overrides=overrides)


# -----------------------------
# Sidebar (minimal)
# -----------------------------
with st.sidebar:
    st.markdown("### Status")
    st.write(f"Project: `{GCP_PROJECT or '-'}'")
    st.write(f"Region: `{GCP_REGION}`")
    st.write(f"Database: `{'set' if DATABASE_URL else 'missing'}`")
    st.write(f"Storage: `{'set' if GCS_BUCKET else 'missing'}`")


# -----------------------------
# Tabs
# -----------------------------
tab_run, tab_results, tab_outputs = st.tabs(["Run", "Results", "Outputs"])


# -----------------------------
# RUN tab
# -----------------------------
with tab_run:
    st.markdown(
        '<div class="panel"><h3>Run</h3><div class="muted">Start a job with the needed inputs</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    c1, c2 = st.columns([0.6, 0.4])
    with c1:
        module = st.selectbox("Module", list(MODULES.keys()), index=0)
    with c2:
        run_label = st.text_input("Label", value="")

    mcfg = MODULES[module]
    job_name = mcfg["job"]
    source_name = mcfg["source"]

    use_progress = st.checkbox("Use progress", value=False)

    # Keywords / Hashtags (only when needed)
    selected_keywords_pipe = ""
    selected_keyword_group = ""
    if bool(mcfg.get("needs_keywords")):
        st.markdown("#### Keywords / Hashtags")

        # Support either:
        # - keywords: ["a","b"]
        # - keyword_groups: {"Peptide":[...], "Biohacking":[...]}
        preset_keywords = mcfg.get("keywords") or []
        keyword_groups = mcfg.get("keyword_groups") or {}

        # If keywords accidentally came as dict, treat it as groups
        if isinstance(preset_keywords, dict) and not keyword_groups:
            keyword_groups = preset_keywords
            preset_keywords = []

        # Clean
        if isinstance(preset_keywords, list):
            preset_keywords = [str(x).strip() for x in preset_keywords if str(x).strip()]
        else:
            preset_keywords = []

        if isinstance(keyword_groups, dict):
            keyword_groups = {
                str(g).strip(): [str(x).strip() for x in (vals or []) if str(x).strip()]
                for g, vals in keyword_groups.items()
                if str(g).strip()
            }
        else:
            keyword_groups = {}

        # If grouped mode exists, show group selector
        if keyword_groups:
            group_names = list(keyword_groups.keys())
            selected_keyword_group = st.selectbox("Category", group_names, index=0)
            base_options = keyword_groups.get(selected_keyword_group) or []
            selected_keywords = st.multiselect("Select", options=base_options, default=base_options)
            selected_keywords_pipe = _pipe_join(selected_keywords)
        else:
            # Flat list mode
            add_text = st.text_input("Add keywords (comma-separated)", value="")
            added = [x.strip() for x in add_text.split(",") if x.strip()]
            all_keywords = sorted(list(dict.fromkeys(preset_keywords + added)))

            if not all_keywords:
                pasted = st.text_area("Paste one per line", value="", height=90)
                all_keywords = [k.strip() for k in pasted.splitlines() if k.strip()]

            selected_keywords = st.multiselect("Select", options=all_keywords, default=all_keywords)
            selected_keywords_pipe = _pipe_join(selected_keywords)

    # City (only when needed)
    selected_city = ""
    if bool(mcfg.get("needs_city")):
        st.markdown("#### City")
        selected_city = st.selectbox("Select city", CITY_LIST, index=0 if CITY_LIST else 0)

    # Hunter needs Yelp input
    yelp_input_gcs = ""
    if bool(mcfg.get("needs_yelp_input")):
        st.markdown("#### Input file")
        if not GCS_BUCKET:
            st.error("Storage not set.")
            st.stop()
        else:
            prefix = f"{GCS_OUTPUT_PREFIX}/"
            items = _gcs_list(prefix=prefix, limit=300)
            yelp_like = [it for it in items if "yelp" in it["name"].lower()]
            options = [""] + [it["name"] for it in yelp_like]
            pick = st.selectbox("Yelp output file", options, index=0)
            if pick:
                yelp_input_gcs = f"gs://{GCS_BUCKET}/{pick}"

    # Followers modules
    uploaded_gcs_path = ""
    single_username = ""
    followers_limit = ""

    if bool(mcfg.get("followers_module")):
        st.markdown("#### Usernames")
        input_mode = st.radio("Input", ["Single username", "Upload file"], horizontal=True)

        if input_mode == "Single username":
            single_username = st.text_input("Username", value="")
        else:
            up = st.file_uploader("Upload usernames file", type=["txt", "csv"])
            if up is not None:
                if not GCS_BUCKET:
                    st.error("Storage not set.")
                    st.stop()
                data = up.read()
                safe_name = up.name.replace(" ", "_")
                object_name = f"{GCS_INPUT_PREFIX}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{safe_name}"
                uploaded_gcs_path = _gcs_upload_bytes(object_name, data)

        followers_limit_mode = st.radio("Followers limit", ["All", "5", "10", "Custom"], horizontal=True)
        if followers_limit_mode == "All":
            followers_limit = "all"
        elif followers_limit_mode in {"5", "10"}:
            followers_limit = followers_limit_mode
        else:
            followers_limit = str(st.number_input("Custom limit", min_value=1, max_value=5000000, value=100, step=10))

    st.write("")
    if st.button("Run", use_container_width=True):
        env: Dict[str, str] = {
            "RUN_LABEL": run_label.strip(),
            "SOURCE": source_name,
            "DATABASE_URL": DATABASE_URL,
            "GCS_BUCKET": GCS_BUCKET,
            "GCS_OUTPUT_PREFIX": GCS_OUTPUT_PREFIX,
            "GCS_INPUT_PREFIX": GCS_INPUT_PREFIX,
            "USE_PROGRESS": "1" if use_progress else "0",
        }

        if selected_keywords_pipe:
            env["SELECTED_KEYWORDS"] = selected_keywords_pipe
            env["KEYWORDS"] = selected_keywords_pipe
            env["SELECTED_HASHTAGS"] = selected_keywords_pipe

        if selected_keyword_group:
            env["KEYWORD_GROUP"] = selected_keyword_group

        if selected_city.strip():
            env["SELECTED_CITY"] = selected_city.strip()
            env["CITY"] = selected_city.strip()

        if bool(mcfg.get("needs_yelp_input")):
            if not yelp_input_gcs:
                st.error("Select a Yelp output file.")
                st.stop()
            env["YELP_INPUT_GCS"] = yelp_input_gcs

        if bool(mcfg.get("followers_module")):
            env["FOLLOWERS_LIMIT"] = followers_limit
            if single_username.strip():
                env["USERNAMES_MODE"] = "single"
                env["USERNAME"] = single_username.strip()
            elif uploaded_gcs_path.strip():
                env["USERNAMES_MODE"] = "file"
                env["USERNAMES_GCS"] = uploaded_gcs_path.strip()
            else:
                st.error("Provide a username or upload a file.")
                st.stop()

        res = _trigger(job_name, env)
        if isinstance(res, dict) and res.get("ok") is True:
            st.success("Started")
        else:
            st.error("Failed")


# -----------------------------
# RESULTS tab
# -----------------------------
with tab_results:
    st.markdown(
        '<div class="panel"><h3>Results</h3><div class="muted">View data in table format and export</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    if not DATABASE_URL:
        st.error("Database not set.")
    else:
        left, right = st.columns([0.35, 0.65], gap="large")

        with left:
            st.subheader("Runs")
            runs_df = _db_read_df(
                """
                SELECT
                  id,
                  source,
                  label,
                  status,
                  started_at,
                  finished_at,
                  started_at AS created_at
                FROM runs
                ORDER BY id DESC
                LIMIT 200
                """
            )
            if runs_df is None or runs_df.empty:
                st.caption("No runs yet.")
            else:
                st.dataframe(runs_df, use_container_width=True, height=420)

        with right:
            st.subheader("Data")
            src = st.selectbox("Source", list({v["source"] for v in MODULES.values()}), index=0)
            limit = st.number_input("Rows", min_value=10, max_value=5000, value=200, step=10)

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

            if events_df is None or events_df.empty:
                st.caption("No data yet.")
            else:
                flat = pd.json_normalize(events_df["payload"].tolist())
                meta = events_df[["id", "run_id", "source", "created_at"]].reset_index(drop=True)
                view_df = pd.concat([meta, flat], axis=1)

                st.dataframe(view_df, use_container_width=True, height=420)

                from io import BytesIO

                cexp1, cexp2 = st.columns([0.5, 0.5])
                with cexp1:
                    csv_bytes = view_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "Download CSV",
                        data=csv_bytes,
                        file_name=f"{src}_data.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                with cexp2:
                    buf = BytesIO()
                    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                        view_df.to_excel(writer, index=False, sheet_name="data")
                    st.download_button(
                        "Download Excel",
                        data=buf.getvalue(),
                        file_name=f"{src}_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )


# -----------------------------
# OUTPUTS tab
# -----------------------------
with tab_outputs:
    st.markdown(
        '<div class="panel"><h3>Outputs</h3><div class="muted">Files saved to storage</div></div>',
        unsafe_allow_html=True,
    )
    st.write("")

    if not GCS_BUCKET:
        st.caption("Storage not set.")
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

                a, b, c = st.columns([0.62, 0.13, 0.25], vertical_alignment="center")
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
