from apify_client import ApifyClient
import pandas as pd
import os, re, csv, time, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from db import start_run, finish_run, insert_df

try:
    from google.cloud import storage
except Exception:
    storage = None


# =========================
# ENV CONFIG
# =========================

APIFY_TOKEN = (os.getenv("APIFY_TOKEN") or "").strip()
if not APIFY_TOKEN:
    raise ValueError("APIFY_TOKEN is required")

ACTOR_ID = os.getenv("IG_FOLLOWERS_ACTOR_ID", "8dqiL379xy0Ldrhdr")

MODULE_NAME = "instagram_followers"
RUN_LABEL = os.getenv("RUN_LABEL", "")

# Input handling
# - Dashboard can pass INPUT_GCS_OBJECT (path in bucket) for a selected file
# - Otherwise falls back to default influencers file object
INPUT_XLSX = os.getenv("IG_INPUT_XLSX", "/tmp/instagram_influencers.xlsx")
INPUT_GCS_OBJECT = (os.getenv("INPUT_GCS_OBJECT") or "").strip()  # set by UI if user selects a file
USERNAME_COL = os.getenv("IG_USERNAME_COL", "username")
SCORE_COL = os.getenv("IG_SCORE_COL", "final_score")

# Modes:
#   auto: if MANUAL_USERNAMES present -> manual, else use selected file if provided -> file, else default influencers file
#   manual: use MANUAL_USERNAMES
#   file: use selected file (INPUT_GCS_OBJECT) if provided, else default influencers file
MODE = os.getenv("IG_FOLLOWERS_MODE", "auto").strip().lower()

MANUAL_USERNAMES = os.getenv("MANUAL_USERNAMES", os.getenv("IG_FOLLOWERS_USERNAMES", "")).strip()

TOP_N = int(os.getenv("IG_FOLLOWERS_TOP_N", "50"))
MIN_SCORE = float(os.getenv("IG_FOLLOWERS_MIN_SCORE", "0"))

BATCH_SIZE = int(os.getenv("IG_FOLLOWERS_BATCH_SIZE", "30"))
PARALLEL_USERS = int(os.getenv("IG_FOLLOWERS_PARALLEL_USERS", "5"))

SLEEP_BETWEEN_BATCHES_SEC = int(os.getenv("IG_SLEEP_BETWEEN_BATCHES_SEC", "10"))
SLEEP_BETWEEN_USER_START_SEC = float(os.getenv("IG_SLEEP_BETWEEN_USER_START_SEC", "0.5"))

MAX_COUNT = int(os.getenv("IG_MAX_COUNT", "1000000000"))
EXCEL_LIMIT = 1048576

DB_MAX_ROWS_PER_RUN = int(os.getenv("IG_DB_MAX_ROWS_PER_RUN", "250000"))
DB_MAX_ROWS_PER_USER = int(os.getenv("IG_DB_MAX_ROWS_PER_USER", "200000"))

# IMPORTANT: this must match the module/source used in your dashboard results tab
DB_TABLE = os.getenv("IG_FOLLOWERS_DB_TABLE", "instagram_followers")

FIELDS = [
    "source_username",
    "follower_username",
    "full_name",
    "is_private",
    "is_verified",
    "profile_pic_url"
]

# Storage (must be the same bucket your dashboard uses)
GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()

# Dashboard lists objects under outputs/
GCS_OUTPUT_PREFIX = (os.getenv("GCS_OUTPUT_PREFIX") or "outputs").strip().strip("/")

# Default input object (the influencers file created by instagram_combined)
GCS_IG_INPUT_OBJECT_DEFAULT = os.getenv(
    "GCS_IG_INPUT_OBJECT",
    f"{GCS_OUTPUT_PREFIX}/social/instagram/instagram_influencers.xlsx"
)

# Followers outputs
PROGRESS_FILE = os.getenv(
    "IG_FOLLOWERS_PROGRESS_FILE",
    "/tmp/instagram_followers_progress.json"
)

GCS_IG_FOLLOWERS_PROGRESS_OBJECT = os.getenv(
    "GCS_IG_FOLLOWERS_PROGRESS_OBJECT",
    f"{GCS_OUTPUT_PREFIX}/social/instagram/followers/instagram_followers_progress.json"
)

BASE_DIR = os.getenv(
    "IG_FOLLOWERS_BASE_DIR",
    "/tmp/instagram_followers_output"
)

# Put summary under outputs/ so Ops Console can list it
GCS_IG_FOLLOWERS_SUMMARY_OBJECT = os.getenv(
    "GCS_IG_FOLLOWERS_SUMMARY_OBJECT",
    f"{GCS_OUTPUT_PREFIX}/social/instagram/followers/summary.xlsx"
)

# Folder prefix for optional per-user uploads (csv/xlsx)
GCS_IG_FOLLOWERS_FOLDER_PREFIX = os.getenv(
    "GCS_IG_FOLLOWERS_FOLDER_PREFIX",
    f"{GCS_OUTPUT_PREFIX}/social/instagram/followers/users"
).rstrip("/")

# Toggle: upload per-user followers files (can create lots of objects)
UPLOAD_PER_USER_FILES = (os.getenv("IG_UPLOAD_PER_USER_FILES", "0").strip() == "1")


# =========================
# HELPERS
# =========================

def _gcs_client():
    if not GCS_BUCKET or storage is None:
        return None
    return storage.Client()


def gcs_download_if_exists(local_path, object_name):
    c = _gcs_client()
    if not c:
        return False
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    if not blob.exists():
        return False
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)
    return True


def gcs_upload(local_path, object_name):
    c = _gcs_client()
    if not c:
        return ""
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)
    return f"gs://{GCS_BUCKET}/{object_name}"


def safe_name(s):
    s = (s or "").strip()
    s = re.sub(r"[^\w\-.]+", "_", s)
    return s[:120] if s else "unknown"


def parse_pipe_list(val):
    if not val:
        return []
    return [x.strip() for x in str(val).split("|") if x.strip()]


def load_progress():
    if GCS_BUCKET:
        gcs_download_if_exists(PROGRESS_FILE, GCS_IG_FOLLOWERS_PROGRESS_OBJECT)

    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"processed_usernames": [], "total_runs": 0, "last_run_date": None}


def save_progress(p):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(PROGRESS_FILE, GCS_IG_FOLLOWERS_PROGRESS_OBJECT)


def _resolve_input_object():
    """
    Priority:
      1) INPUT_GCS_OBJECT (selected file from UI)
      2) Default influencers file
    """
    if INPUT_GCS_OBJECT:
        return INPUT_GCS_OBJECT
    return GCS_IG_INPUT_OBJECT_DEFAULT


def ensure_input_available():
    # If file already present, ok
    if os.path.exists(INPUT_XLSX):
        return True

    # Download from GCS
    obj = _resolve_input_object()
    if GCS_BUCKET and obj:
        return gcs_download_if_exists(INPUT_XLSX, obj)

    return False


def pick_usernames_from_excel():
    if not ensure_input_available():
        raise ValueError("Instagram input Excel missing (no local file and cannot download from GCS).")

    df = pd.read_excel(INPUT_XLSX)

    if USERNAME_COL not in df.columns:
        raise ValueError(f"Column '{USERNAME_COL}' missing in input Excel")

    df[USERNAME_COL] = df[USERNAME_COL].astype(str).str.strip()
    df = df[df[USERNAME_COL] != ""]
    df = df[df[USERNAME_COL].str.lower() != "nan"]

    if SCORE_COL in df.columns:
        df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce").fillna(0)
        df = df[df[SCORE_COL] >= MIN_SCORE].sort_values(SCORE_COL, ascending=False)

    return df.head(TOP_N)[USERNAME_COL].tolist()


def get_target_usernames():
    # auto routing
    if MODE == "auto":
        if MANUAL_USERNAMES:
            return parse_pipe_list(MANUAL_USERNAMES)
        return pick_usernames_from_excel()

    if MODE in {"manual", "followers_manual"}:
        return parse_pipe_list(MANUAL_USERNAMES)

    # file mode
    return pick_usernames_from_excel()


# =========================
# SCRAPER
# =========================

def scrape_one(username):
    client = ApifyClient(APIFY_TOKEN)

    folder = os.path.join(BASE_DIR, safe_name(username))
    os.makedirs(folder, exist_ok=True)

    csv_path = os.path.join(folder, "followers.csv")
    xlsx_path = os.path.join(folder, "followers.xlsx")

    rows = 0
    status = "ok"
    dataset_id = ""
    err = ""

    try:
        run = client.actor(ACTOR_ID).call(
            run_input={"usernames": [username], "max_count": MAX_COUNT}
        )
        dataset_id = run.get("defaultDatasetId", "")

        if not dataset_id:
            status = "no_dataset"
        else:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()

                for item in client.dataset(dataset_id).iterate_items():
                    writer.writerow({
                        "source_username": username,
                        "follower_username": item.get("username", ""),
                        "full_name": item.get("full_name", ""),
                        "is_private": item.get("is_private", ""),
                        "is_verified": item.get("is_verified", ""),
                        "profile_pic_url": item.get("profile_pic_url", ""),
                    })
                    rows += 1

        if status == "ok" and 0 < rows <= EXCEL_LIMIT:
            pd.read_csv(csv_path).to_excel(xlsx_path, index=False)

    except Exception as e:
        status = "error"
        err = str(e)

    return {
        "source_username": username,
        "rows_written": rows,
        "status": status,
        "error": err,
        "folder": folder,
        "csv_path": csv_path,
        "xlsx_path": xlsx_path,
        "dataset_id": dataset_id
    }


# =========================
# MAIN
# =========================

def main():
    run_id = start_run(MODULE_NAME, RUN_LABEL)
    os.makedirs(BASE_DIR, exist_ok=True)

    try:
        progress = load_progress()
        done = set(x.lower() for x in progress.get("processed_usernames", []))

        usernames = get_target_usernames()
        usernames = [u for u in usernames if u and u.lower() not in done]

        if not usernames:
            finish_run(run_id, "ok")
            return

        summary = []
        processed_now = []
        db_rows_total = 0

        for i in range(0, len(usernames), BATCH_SIZE):
            batch = usernames[i:i + BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=PARALLEL_USERS) as ex:
                futures = []
                for u in batch:
                    futures.append(ex.submit(scrape_one, u))
                    time.sleep(SLEEP_BETWEEN_USER_START_SEC)

                for fut in as_completed(futures):
                    res = fut.result()
                    scraped_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                    summary.append({
                        "run_id": run_id,
                        "source_username": res["source_username"],
                        "rows_written": res["rows_written"],
                        "status": res["status"],
                        "error": res["error"],
                        "dataset_id": res["dataset_id"],
                        "scraped_at": scraped_at,
                    })

                    if res["status"] in {"ok", "no_dataset"}:
                        processed_now.append(res["source_username"])

                    # DB insert (limited)
                    if res["status"] == "ok" and db_rows_total < DB_MAX_ROWS_PER_RUN:
                        if os.path.exists(res["csv_path"]):
                            df = pd.read_csv(res["csv_path"])
                            if not df.empty:
                                df = df.head(DB_MAX_ROWS_PER_USER)
                                df["scraped_at"] = scraped_at
                                insert_df(run_id, DB_TABLE, df)
                                db_rows_total += len(df)

                    # Optional: upload per-user files
                    if GCS_BUCKET and UPLOAD_PER_USER_FILES:
                        user_prefix = f"{GCS_IG_FOLLOWERS_FOLDER_PREFIX}/{safe_name(res['source_username'])}"
                        if os.path.exists(res["csv_path"]):
                            gcs_upload(res["csv_path"], f"{user_prefix}/followers.csv")
                        if os.path.exists(res["xlsx_path"]):
                            gcs_upload(res["xlsx_path"], f"{user_prefix}/followers.xlsx")

            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        # Summary file
        summary_path = os.path.join(BASE_DIR, "summary.xlsx")
        pd.DataFrame(summary).to_excel(summary_path, index=False)

        if GCS_BUCKET:
            gcs_upload(summary_path, GCS_IG_FOLLOWERS_SUMMARY_OBJECT)

        progress["processed_usernames"] = sorted(set(progress["processed_usernames"] + processed_now))
        progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
        progress["last_run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        save_progress(progress)

        finish_run(run_id, "ok")

    except Exception as e:
        # keep finish_run format consistent with your db.py
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
