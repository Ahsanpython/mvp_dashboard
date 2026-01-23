from apify_client import ApifyClient
import pandas as pd
import os, json, math, time
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

client = ApifyClient(APIFY_TOKEN)

REELS_ACTOR_ID = os.getenv("IG_REELS_ACTOR_ID", "reGe1ST3OBgYZSsZJ")
PROFILE_ACTOR_ID = os.getenv("IG_PROFILE_ACTOR_ID", "dSCLg0C3YEZ83HzYX")

RESULTS_PER_HASHTAG = int(os.getenv("IG_RESULTS_PER_HASHTAG", "1000"))
BATCH_SIZE = int(os.getenv("IG_BATCH_SIZE", "30"))

OUTPUT_FILE = os.getenv("IG_OUTPUT_FILE", "/tmp/instagram_influencers.xlsx")
PROGRESS_FILE = os.getenv("IG_PROGRESS_FILE", "/tmp/instagram_progress.json")

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_IG_OUTPUT_OBJECT = "exports/social/instagram/instagram_influencers.xlsx"
GCS_IG_PROGRESS_OBJECT = "exports/social/instagram/instagram_progress.json"

SOURCE_NAME = "instagram_combined"

CATEGORIES = {
    "Peptide": [
        "peptide", "collagenpeptides", "copperpeptides", "skincare", "antiaging",
        "bpc157", "tb500", "semaglutide", "tirzepatide", "wellness", "weightloss", "prp"
    ],
    "Biohacking": [
        "biohacking", "biohacker", "longevity", "nootropics", "redlighttherapy",
        "wearables", "sleepoptimization", "coldplunge", "sauna", "hrv",
        "functionalmedicine", "recovery"
    ],
    "Regenerative Medicine": [
        "regenerativemedicine", "prp", "stemcelltherapy", "exosomes", "orthopedics",
        "sportsmedicine", "aestheticmedicine", "jointpain", "arthritis",
        "painmanagement", "hairrestoration", "celltherapy"
    ]
}


def _gcs_client():
    if not GCS_BUCKET or storage is None:
        return None
    return storage.Client()


def gcs_upload(local_path: str, object_name: str):
    c = _gcs_client()
    if not c:
        return
    bucket = c.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path)


def load_progress():
    if GCS_BUCKET:
        c = _gcs_client()
        blob = c.bucket(GCS_BUCKET).blob(GCS_IG_PROGRESS_OBJECT)
        if blob.exists():
            blob.download_to_filename(PROGRESS_FILE)

    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"processed_usernames": [], "total_runs": 0}


def save_progress(p):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(PROGRESS_FILE, GCS_IG_PROGRESS_OBJECT)


def scrape_reels(category, hashtag):
    run = client.actor(REELS_ACTOR_ID).call({
        "hashtags": [hashtag],
        "resultsType": "reels",
        "resultsLimit": RESULTS_PER_HASHTAG
    })

    rows = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        rows.append({
            "category": category,
            "keyword": hashtag,
            "username": item.get("ownerUsername", ""),
            "likes": item.get("likesCount", 0),
            "comments": item.get("commentsCount", 0),
            "views": item.get("videoPlayCount", 0),
            "post_url": item.get("url", ""),
            "video_url": item.get("videoUrl", ""),
            "reel_timestamp": item.get("timestamp", "")
        })
    return rows


def main():
    run_id = start_run(SOURCE_NAME, os.getenv("RUN_LABEL", ""))

    try:
        progress = load_progress()
        done = set(progress.get("processed_usernames", []))

        all_rows = []
        for cat, tags in CATEGORIES.items():
            for tag in tags:
                all_rows.extend(scrape_reels(cat, tag))

        df = pd.DataFrame(all_rows)
        if df.empty:
            finish_run(run_id, "ok")
            return

        df["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # SAVE FILE
        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(OUTPUT_FILE, index=False)

        if GCS_BUCKET:
            gcs_upload(OUTPUT_FILE, GCS_IG_OUTPUT_OBJECT)

        # ✅ DASHBOARD RESULTS FIX
        insert_df(run_id, SOURCE_NAME, df)

        progress["processed_usernames"] = sorted(set(done | set(df["username"])))
        progress["total_runs"] += 1
        save_progress(progress)

        finish_run(run_id, "ok")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
