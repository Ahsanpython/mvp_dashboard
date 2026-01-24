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

INPUT_XLSX = os.getenv("IG_INPUT_XLSX", "/tmp/instagram_influencers.xlsx")
USERNAME_COL = os.getenv("IG_USERNAME_COL", "username")
SCORE_COL = os.getenv("IG_SCORE_COL", "final_score")

BASE_DIR = os.getenv("IG_FOLLOWERS_BASE_DIR", "/tmp/instagram_followers_output")
PROGRESS_FILE = os.getenv("IG_FOLLOWERS_PROGRESS_FILE", "/tmp/instagram_followers_progress.json")

MODE = os.getenv("IG_FOLLOWERS_MODE", "followers_from_excel")
MANUAL_USERNAMES = os.getenv("IG_FOLLOWERS_USERNAMES", "")

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

DB_TABLE = os.getenv("IG_FOLLOWERS_DB_TABLE", "instagram_followers")

FIELDS = [
    "source_username",
    "follower_username",
    "full_name",
    "is_private",
    "is_verified",
    "profile_pic_url"
]

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()

GCS_IG_INPUT_OBJECT = os.getenv(
    "GCS_IG_INPUT_OBJECT",
    "exports/social/instagram/instagram_influencers.xlsx"
)

GCS_IG_FOLLOWERS_PROGRESS_OBJECT = os.getenv(
    "GCS_IG_FOLLOWERS_PROGRESS_OBJECT",
    "exports/social/instagram/instagram_followers_progress.json"
)

GCS_IG_FOLLOWERS_SUMMARY_OBJECT = os.getenv(
    "GCS_IG_FOLLOWERS_SUMMARY_OBJECT",
    "exports/social/instagram/followers/summary.xlsx"
)

GCS_IG_FOLLOWERS_FOLDER_PREFIX = os.getenv(
    "GCS_IG_FOLLOWERS_FOLDER_PREFIX",
    "exports/social/instagram/followers"
).rstrip("/")


# Dashboard-driven inputs (NEW)
USERNAMES_MODE = (os.getenv("USERNAMES_MODE") or "").strip().lower()  # single | file
USERNAME_SINGLE = (os.getenv("USERNAME") or "").strip()
USERNAMES_GCS = (os.getenv("USERNAMES_GCS") or "").strip()


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


def ensure_input_available():
    if os.path.exists(INPUT_XLSX):
        return True
    if GCS_BUCKET:
        return gcs_download_if_exists(INPUT_XLSX, GCS_IG_INPUT_OBJECT)
    return False


def _gcs_download_to_temp(gcs_uri: str) -> str:
    """
    Downloads gs://bucket/path to /tmp and returns local path.
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError("USERNAMES_GCS must start with gs://")

    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET not set")

    # parse gs://bucket/object
    no_scheme = gcs_uri[len("gs://"):]
    parts = no_scheme.split("/", 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS uri format")
    bucket_name, object_name = parts[0], parts[1]

    if bucket_name != GCS_BUCKET:
        # allow cross-bucket but requires creds; if you want strict, raise
        pass

    local_path = f"/tmp/{safe_name(Path(object_name).name)}"
    c = _gcs_client()
    if not c:
        raise ValueError("GCS client not available")
    bucket = c.bucket(bucket_name)
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise ValueError(f"GCS file not found: {gcs_uri}")
    blob.download_to_filename(local_path)
    return local_path


def _extract_usernames_from_any_file(local_path: str) -> list:
    """
    Supports .xlsx, .csv, .txt
    - xlsx/csv: tries USERNAME_COL if exists, else uses first column
    - txt: one username per line
    """
    p = Path(local_path)
    ext = p.suffix.lower()

    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(local_path)
        if df.empty:
            return []
        if USERNAME_COL in df.columns:
            col = USERNAME_COL
        else:
            col = df.columns[0]
        vals = df[col].astype(str).tolist()
        out = [v.strip() for v in vals if v and str(v).strip()]
        return out

    if ext == ".csv":
        df = pd.read_csv(local_path)
        if df.empty:
            return []
        if USERNAME_COL in df.columns:
            col = USERNAME_COL
        else:
            col = df.columns[0]
        vals = df[col].astype(str).tolist()
        out = [v.strip() for v in vals if v and str(v).strip()]
        return out

    if ext == ".txt":
        with open(local_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines()]
        return [ln for ln in lines if ln]

    raise ValueError(f"Unsupported usernames file type: {ext} (use xlsx/csv/txt)")


def pick_usernames_from_excel():
    if not ensure_input_available():
        raise ValueError("Instagram influencers file missing")

    df = pd.read_excel(INPUT_XLSX)
    if USERNAME_COL not in df.columns:
        raise ValueError("username column missing")

    df[USERNAME_COL] = df[USERNAME_COL].astype(str).str.strip()
    df = df[df[USERNAME_COL] != ""]

    if SCORE_COL in df.columns:
        df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce").fillna(0)
        df = df[df[SCORE_COL] >= MIN_SCORE].sort_values(SCORE_COL, ascending=False)

    return df.head(TOP_N)[USERNAME_COL].tolist()


def get_target_usernames():
    # NEW: dashboard mode first
    if USERNAMES_MODE == "single" and USERNAME_SINGLE:
        return [USERNAME_SINGLE]

    if USERNAMES_MODE == "file" and USERNAMES_GCS:
        local = _gcs_download_to_temp(USERNAMES_GCS)
        return _extract_usernames_from_any_file(local)

    # existing modes
    if MODE == "followers_manual":
        return parse_pipe_list(MANUAL_USERNAMES)

    return pick_usernames_from_excel()


def upload_user_outputs_to_gcs(username: str, csv_path: str, xlsx_path: str):
    if not GCS_BUCKET:
        return

    uname = safe_name(username)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base_prefix = GCS_IG_FOLLOWERS_FOLDER_PREFIX.rstrip("/") + f"/{uname}/{ts}/"

    if os.path.exists(csv_path):
        gcs_upload(csv_path, base_prefix + "followers.csv")

    if os.path.exists(xlsx_path):
        gcs_upload(xlsx_path, base_prefix + "followers.xlsx")


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

        if status == "ok" and 0 < rows <= EXCEL_LIMIT and os.path.exists(csv_path):
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
        usernames = [str(u).strip() for u in (usernames or []) if str(u).strip()]
        # de-dup, keep order
        seen = set()
        cleaned = []
        for u in usernames:
            key = u.lower()
            if key not in seen:
                seen.add(key)
                cleaned.append(u)
        usernames = cleaned

        usernames = [u for u in usernames if u.lower() not in done]

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
                        "saved_csv": "yes" if os.path.exists(res.get("csv_path", "")) else "no",
                        "saved_excel": "yes" if os.path.exists(res.get("xlsx_path", "")) else "no",
                    })

                    if res["status"] in {"ok", "no_dataset"}:
                        processed_now.append(res["source_username"])

                    # Upload per-user outputs
                    if GCS_BUCKET:
                        upload_user_outputs_to_gcs(
                            res["source_username"],
                            res.get("csv_path", ""),
                            res.get("xlsx_path", "")
                        )

                    # DB insert (respect limits)
                    if res["status"] == "ok" and db_rows_total < DB_MAX_ROWS_PER_RUN:
                        if os.path.exists(res["csv_path"]):
                            df = pd.read_csv(res["csv_path"])
                            if not df.empty:
                                df = df.head(DB_MAX_ROWS_PER_USER)
                                df["scraped_at"] = scraped_at
                                insert_df(run_id, DB_TABLE, df)
                                db_rows_total += len(df)

            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        summary_path = os.path.join(BASE_DIR, "summary.xlsx")
        pd.DataFrame(summary).to_excel(summary_path, index=False)

        if GCS_BUCKET and os.path.exists(summary_path):
            gcs_upload(summary_path, GCS_IG_FOLLOWERS_SUMMARY_OBJECT)

        progress["processed_usernames"] = sorted(set(progress.get("processed_usernames", []) + processed_now))
        progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
        progress["last_run_date"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        save_progress(progress)

        insert_df(run_id, "runs", pd.DataFrame([{
            "module": MODULE_NAME,
            "run_label": RUN_LABEL,
            "status": "ok",
            "processed_usernames": len(processed_now),
            "db_rows_appended": db_rows_total,
            "finished_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }]))

        finish_run(run_id, "ok")

    except Exception as e:
        finish_run(run_id, "error", {"error": str(e)})
        raise


if __name__ == "__main__":
    main()
