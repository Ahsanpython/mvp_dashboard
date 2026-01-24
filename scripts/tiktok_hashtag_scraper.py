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


APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
if not APIFY_TOKEN:
    raise ValueError("Missing APIFY_TOKEN env var")

ACTOR_ID = os.getenv("TT_FOLLOWERS_ACTOR_ID", "i7JuI8WcwN94blNMb")

INPUT_XLSX = os.getenv("TT_INPUT_XLSX", "/tmp/tiktok_influencers_combined.xlsx")
USERNAME_COL = os.getenv("TT_USERNAME_COL", "username")
SCORE_COL = os.getenv("TT_SCORE_COL", "final_score")

BASE_DIR = os.getenv("TT_FOLLOWERS_BASE_DIR", "/tmp/tiktok_followers_output")
PROGRESS_FILE = os.getenv("TT_FOLLOWERS_PROGRESS_FILE", "/tmp/tiktok_followers_progress.json")

MODE = os.getenv("TT_FOLLOWERS_MODE", "followers_from_excel")  # followers_from_excel | followers_manual
FOLLOWERS_USERNAMES = os.getenv("TT_FOLLOWERS_USERNAMES", "")
TOP_N = int(os.getenv("TT_FOLLOWERS_TOP_N", "50"))
MIN_SCORE = float(os.getenv("TT_FOLLOWERS_MIN_SCORE", "0"))

BATCH_SIZE = int(os.getenv("TT_FOLLOWERS_BATCH_SIZE", "30"))
PARALLEL_USERS = int(os.getenv("TT_FOLLOWERS_PARALLEL_USERS", "5"))

SLEEP_BETWEEN_BATCHES_SEC = int(os.getenv("TT_SLEEP_BETWEEN_BATCHES_SEC", "10"))
SLEEP_BETWEEN_USER_START_SEC = float(os.getenv("TT_SLEEP_BETWEEN_USER_START_SEC", "0.5"))

EXCEL_LIMIT = 1048576
MAX_FOLLOWERS_PER_PROFILE = int(os.getenv("TT_MAX_FOLLOWERS_PER_PROFILE", "1000000000"))
MAX_FOLLOWING_PER_PROFILE = int(os.getenv("TT_MAX_FOLLOWING_PER_PROFILE", "0"))

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()

GCS_TT_INPUT_OBJECT = os.getenv(
    "GCS_TT_INPUT_OBJECT",
    "exports/social/tiktok/tiktok_influencers_combined.xlsx"
)

GCS_TT_FOLLOWERS_PREFIX = os.getenv(
    "GCS_TT_FOLLOWERS_PREFIX",
    "exports/social/tiktok/followers/"
)

GCS_TT_FOLLOWERS_PROGRESS_OBJECT = os.getenv(
    "GCS_TT_FOLLOWERS_PROGRESS_OBJECT",
    "exports/social/tiktok/followers/tiktok_followers_progress.json"
)

GCS_TT_FOLLOWERS_SUMMARY_OBJECT = os.getenv(
    "GCS_TT_FOLLOWERS_SUMMARY_OBJECT",
    "exports/social/tiktok/followers/summary.xlsx"
)

DB_TABLE = os.getenv("TT_FOLLOWERS_DB_TABLE", "tiktok_followers")


FIELDS = [
    "run_id",
    "source_username",
    "connectionType",
    "follower_id",
    "follower_username",
    "follower_nickname",
    "follower_profileUrl",
    "follower_signature",
    "follower_bioLink",
    "follower_avatar",
    "follower_privateAccount",
    "follower_verified",
    "follower_following",
    "follower_friends",
    "follower_fans",
    "follower_heart",
    "follower_video",
    "follower_digg",
    "scraped_at",
]


def _gcs_client():
    if not GCS_BUCKET or storage is None:
        return None
    return storage.Client()


def gcs_download_if_exists(local_path: str, object_name: str) -> bool:
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


def gcs_upload(local_path: str, object_name: str) -> str:
    c = _gcs_client()
    if not c:
        return ""
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)
    return f"gs://{GCS_BUCKET}/{object_name}"


def parse_pipe_list(val):
    if not val:
        return []
    s = str(val).strip()
    if not s:
        return []
    return [x.strip() for x in s.split("|") if x.strip()]


def safe_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-.]+", "_", s)
    return s[:120] if s else "unknown"


def load_progress():
    if GCS_BUCKET:
        gcs_download_if_exists(PROGRESS_FILE, GCS_TT_FOLLOWERS_PROGRESS_OBJECT)

    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"processed_usernames": [], "total_runs": 0, "last_run_date": None}


def save_progress(p):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(PROGRESS_FILE, GCS_TT_FOLLOWERS_PROGRESS_OBJECT)


def pick_usernames_from_excel():
    if GCS_BUCKET:
        gcs_download_if_exists(INPUT_XLSX, GCS_TT_INPUT_OBJECT)

    if not os.path.exists(INPUT_XLSX):
        raise ValueError(f"Missing file: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX)
    if USERNAME_COL not in df.columns:
        raise ValueError(f"Missing column: {USERNAME_COL}")

    df[USERNAME_COL] = df[USERNAME_COL].astype(str).str.strip()
    df = df[df[USERNAME_COL].notna() & (df[USERNAME_COL] != "")]

    if SCORE_COL in df.columns:
        df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce").fillna(0)
        df = df[df[SCORE_COL] >= MIN_SCORE]
        df = df.sort_values(SCORE_COL, ascending=False)

    df = df.head(TOP_N)
    return df[USERNAME_COL].tolist()


def get_target_usernames():
    if MODE == "followers_manual":
        return parse_pipe_list(FOLLOWERS_USERNAMES)
    return pick_usernames_from_excel()


def scrape_one(username: str, run_id: str):
    client = ApifyClient(APIFY_TOKEN)

    folder = os.path.join(BASE_DIR, safe_name(username))
    os.makedirs(folder, exist_ok=True)

    csv_path = os.path.join(folder, "followers.csv")
    xlsx_path = os.path.join(folder, "followers.xlsx")

    rows_written = 0
    status = "ok"
    err = ""
    dataset_id = ""
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        run = client.actor(ACTOR_ID).call(run_input={
            "profiles": [username],
            "maxFollowersPerProfile": MAX_FOLLOWERS_PER_PROFILE,
            "maxFollowingPerProfile": MAX_FOLLOWING_PER_PROFILE,
        })
        dataset_id = run.get("defaultDatasetId", "") or ""
        if not dataset_id:
            status = "no_dataset"
        else:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)
                w.writeheader()

                for item in client.dataset(dataset_id).iterate_items():
                    if str(item.get("connectionType", "")).lower() != "follower":
                        continue

                    a = item.get("authorMeta") or {}
                    w.writerow({
                        "run_id": run_id,
                        "source_username": username,
                        "connectionType": item.get("connectionType", ""),
                        "follower_id": a.get("id", ""),
                        "follower_username": a.get("name", ""),
                        "follower_nickname": a.get("nickName", ""),
                        "follower_profileUrl": a.get("profileUrl", ""),
                        "follower_signature": a.get("signature", ""),
                        "follower_bioLink": a.get("bioLink", ""),
                        "follower_avatar": a.get("avatar", "") or a.get("originalAvatarUrl", ""),
                        "follower_privateAccount": a.get("privateAccount", ""),
                        "follower_verified": a.get("verified", ""),
                        "follower_following": a.get("following", ""),
                        "follower_friends": a.get("friends", ""),
                        "follower_fans": a.get("fans", ""),
                        "follower_heart": a.get("heart", ""),
                        "follower_video": a.get("video", ""),
                        "follower_digg": a.get("digg", ""),
                        "scraped_at": scraped_at,
                    })
                    rows_written += 1

            if 0 < rows_written <= EXCEL_LIMIT:
                pd.read_csv(csv_path).to_excel(xlsx_path, index=False)

    except Exception as e:
        status = "error"
        err = str(e)

    return {
        "source_username": username,
        "rows_written": rows_written,
        "saved_excel": "yes" if os.path.exists(xlsx_path) else "no",
        "saved_csv": "yes" if os.path.exists(csv_path) else "no",
        "folder": folder,
        "dataset_id": dataset_id,
        "status": status,
        "error": err,
        "csv_path": csv_path,
        "xlsx_path": xlsx_path,
    }


def upload_user_outputs_to_gcs(username: str, csv_path: str, xlsx_path: str):
    if not GCS_BUCKET:
        return

    uname = safe_name(username)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_prefix = GCS_TT_FOLLOWERS_PREFIX.rstrip("/") + f"/{uname}/{ts}/"

    if os.path.exists(csv_path):
        gcs_upload(csv_path, base_prefix + "followers.csv")

    if os.path.exists(xlsx_path):
        gcs_upload(xlsx_path, base_prefix + "followers.xlsx")


def main():
    run_id = start_run("tiktok_followers", os.getenv("RUN_LABEL", ""))

    try:
        os.makedirs(BASE_DIR, exist_ok=True)

        progress = load_progress()
        done = set([str(u).lower() for u in progress.get("processed_usernames", [])])

        usernames = get_target_usernames()
        usernames = [u.strip() for u in usernames if u and str(u).strip()]
        usernames = [u for u in usernames if u.lower() not in done]

        if not usernames:
            print("No usernames to process (all done or empty).")
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
                    futures.append(ex.submit(scrape_one, u, run_id))
                    time.sleep(SLEEP_BETWEEN_USER_START_SEC)

                for fut in as_completed(futures):
                    res = fut.result()

                    summary.append({
                        "run_id": run_id,
                        "source_username": res.get("source_username", ""),
                        "rows_written": res.get("rows_written", 0),
                        "saved_excel": res.get("saved_excel", "no"),
                        "saved_csv": res.get("saved_csv", "no"),
                        "folder": res.get("folder", ""),
                        "dataset_id": res.get("dataset_id", ""),
                        "status": res.get("status", ""),
                        "error": res.get("error", ""),
                    })

                    if res.get("status") in {"ok", "no_dataset"}:
                        processed_now.append(res.get("source_username", ""))

                    if GCS_BUCKET:
                        upload_user_outputs_to_gcs(
                            res.get("source_username", ""),
                            res.get("csv_path", ""),
                            res.get("xlsx_path", "")
                        )

                    # if res.get("status") == "ok" and os.path.exists(res.get("csv_path", "")):
                    #     try:
                    #         df_csv = pd.read_csv(res.get("csv_path"))
                    #         if not df_csv.empty:
                    #             insert_df(run_id, DB_TABLE, df_csv)
                    #             db_rows_total += int(len(df_csv))
                    #     except Exception:
                    #         pass

                    if res.get("status") == "ok" and os.path.exists(res.get("csv_path", "")):
                        try:
                            df_csv = pd.read_csv(res.get("csv_path"))
                            if not df_csv.empty:
            # remove run_id if present (insert_df adds it)
                                if "run_id" in df_csv.columns:
                                    df_csv = df_csv.drop(columns=["run_id"])
                                insert_df(run_id, DB_TABLE, df_csv)
                                db_rows_total += int(len(df_csv))
                        except Exception:
                            pass


            time.sleep(SLEEP_BETWEEN_BATCHES_SEC)

        summary_path = os.path.join(BASE_DIR, "summary.xlsx")
        pd.DataFrame(summary).to_excel(summary_path, index=False)
        print("DONE:", BASE_DIR)

        if GCS_BUCKET and os.path.exists(summary_path):
            gcs_upload(summary_path, GCS_TT_FOLLOWERS_SUMMARY_OBJECT)

        progress["processed_usernames"] = sorted(list(set(progress.get("processed_usernames", []) + processed_now)))
        progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
        progress["last_run_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_progress(progress)

        df_runs = pd.DataFrame([{
            "module": "tiktok_followers",
            "run_label": os.getenv("RUN_LABEL", ""),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
            "processed_usernames": int(len(processed_now)),
            "db_rows_appended": int(db_rows_total),
        }])
        insert_df(run_id, "runs", df_runs)

        finish_run(run_id, "ok")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
