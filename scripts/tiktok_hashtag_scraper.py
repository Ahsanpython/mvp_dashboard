from apify_client import ApifyClient
import pandas as pd
import os
import json
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

RESULTS_PER_PAGE = int(os.getenv("TT_RESULTS_PER_PAGE", "1000"))

OUTPUT_FILE = os.getenv("TT_OUTPUT_FILE", "/tmp/tiktok_influencers_combined.xlsx")
RUN_HISTORY_FILE = os.getenv("TT_RUN_HISTORY_FILE", "/tmp/tiktok_run_history.json")

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_TT_OUTPUT_OBJECT = os.getenv("GCS_TT_OUTPUT_OBJECT", "exports/social/tiktok/tiktok_influencers_combined.xlsx")
GCS_TT_HISTORY_OBJECT = os.getenv("GCS_TT_HISTORY_OBJECT", "exports/social/tiktok/tiktok_run_history.json")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
DB_TABLE = os.getenv("TT_DB_TABLE", "tiktok_influencers")


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





def calculate_engagement_rate(digg, share, comment, play, followers):
    if play == 0 or followers == 0:
        return 0
    return round(((digg + share + comment) / play) * 100, 2)


def normalize_followers(f):
    if f <= 1000:
        return f / 1000
    elif f <= 10000:
        return 0.1 + (f - 1000) / 9000 * 0.4
    elif f <= 100000:
        return 0.5 + (f - 10000) / 90000 * 0.4
    else:
        return 0.9 + min((f - 100000) / 1000000, 0.1)


def calculate_keyword_relevance(text, bio, keywords):
    content = (str(text) + " " + str(bio)).lower()
    matches = sum(1 for k in keywords if k in content)
    return min(matches / len(keywords), 1.0) if keywords else 0.0


def apply_scoring_formula(engagement, follower_score, keyword_rel):
    return round(((engagement / 100) * 0.4 + follower_score * 0.3 + keyword_rel * 0.3) * 100, 2)


def parse_pipe_list(val):
    if not val:
        return []
    s = str(val).strip()
    if not s:
        return []
    return [x.strip() for x in s.split("|") if x.strip()]


def cat_to_env_key(cat_name: str) -> str:
    return "TT_HASHTAGS_" + cat_name.upper().replace(" ", "_")


def get_selected_categories():
    raw = os.getenv("TT_CATEGORIES", "").strip()
    if raw:
        selected = parse_pipe_list(raw)
        selected = [c for c in selected if c in CATEGORIES]
        if selected:
            return selected
    return list(CATEGORIES.keys())


def get_selected_hashtags_for_category(cat):
    env_key = cat_to_env_key(cat)
    raw = os.getenv(env_key, "").strip()
    if raw:
        tags = parse_pipe_list(raw)
        allowed = set(CATEGORIES.get(cat, []))
        tags = [t for t in tags if t in allowed]
        if tags:
            return tags
    return CATEGORIES.get(cat, [])


def scrape_tiktok_influencers(category, hashtags):
    print(f"\n🚀 Scraping category: {category} ({len(hashtags)} hashtags)")
    run_input = {
        "hashtags": hashtags,
        "resultsPerPage": RESULTS_PER_PAGE,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadSlideshowImages": False,
    }

    run = client.actor("f1ZeP0K58iwlqG2pY").call(run_input=run_input)
    influencers = []

    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        try:
            author = item.get("authorMeta", {}) or {}
            username = author.get("name", "")
            if not username:
                continue

            followers = author.get("fans", 0)
            bio = author.get("signature", "")
            verified = author.get("verified", False)
            text = item.get("text", "")
            digg = item.get("diggCount", 0)
            share = item.get("shareCount", 0)
            comment = item.get("commentCount", 0)
            play = item.get("playCount", 0)

            if play == 0:
                continue

            engagement = calculate_engagement_rate(digg, share, comment, play, followers)
            try:
                f_int = int(followers)
            except:
                f_int = 0
            follower_score = normalize_followers(f_int)
            keyword_rel = calculate_keyword_relevance(text, bio, hashtags)
            final_score = apply_scoring_formula(engagement, follower_score, keyword_rel)

            influencers.append({
                "category": category,
                "username": username,
                "nickname": author.get("nickName", ""),
                "followers": followers,
                "engagement_rate": engagement,
                "keyword_relevance": round(keyword_rel * 100, 2),
                "final_score": final_score,
                "verified": verified,
                "bio": bio,
                "post_text": text,
                "profile_url": f"https://tiktok.com/@{username}",
                "video_url": item.get("webVideoUrl", ""),
                "timestamp": pd.Timestamp.now()
            })

        except Exception as e:
            print(f"⚠️ Error: {e}")
            continue

    influencers.sort(key=lambda x: x["final_score"], reverse=True)
    print(f"✅ Found {len(influencers)} influencers in {category}")
    return influencers


def load_history():
    if GCS_BUCKET:
        gcs_download_if_exists(RUN_HISTORY_FILE, GCS_TT_HISTORY_OBJECT)

    if os.path.exists(RUN_HISTORY_FILE):
        try:
            with open(RUN_HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


def save_history(h):
    Path(RUN_HISTORY_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(RUN_HISTORY_FILE, "w") as f:
        json.dump(h, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(RUN_HISTORY_FILE, GCS_TT_HISTORY_OBJECT)


def main():
    run_id = start_run("tiktok_hashtags", os.getenv("RUN_LABEL", ""))

    try:
        if GCS_BUCKET:
            gcs_download_if_exists(OUTPUT_FILE, GCS_TT_OUTPUT_OBJECT)

        selected_categories = get_selected_categories()
        plan = []
        for cat in selected_categories:
            tags = get_selected_hashtags_for_category(cat)
            plan.append({"category": cat, "hashtags": tags})

        print("\n🧭 Run plan:")
        for p in plan:
            print(f"  - {p['category']}: {len(p['hashtags'])} hashtags")

        all_influencers = []
        for p in plan:
            all_influencers.extend(scrape_tiktok_influencers(p["category"], p["hashtags"]))

        df_new = pd.DataFrame(all_influencers)
        if df_new.empty:
            finish_run(run_id, "ok")
            print("No new data scraped.")
            return

        if os.path.exists(OUTPUT_FILE):
            df_existing = pd.read_excel(OUTPUT_FILE)
            combined = pd.concat([df_new, df_existing], ignore_index=True)
            print(f"📂 Merging with existing {len(df_existing)} influencers...")
        else:
            combined = df_new
            print("📂 No existing file found. Creating new one.")

        combined.drop_duplicates(subset=["username"], keep="first", inplace=True)
        combined.sort_values(by=["timestamp", "final_score"], ascending=[False, False], inplace=True)

        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        combined.to_excel(OUTPUT_FILE, index=False)

        print(f"\n💾 Updated file saved as '{OUTPUT_FILE}'")
        print(f"📊 Total influencers after dedupe: {len(combined)}")

        if GCS_BUCKET:
            gcs_upload(OUTPUT_FILE, GCS_TT_OUTPUT_OBJECT)

        if DATABASE_URL and not df_new.empty:
            df_db = df_new.copy()
            insert_df(run_id, DB_TABLE, df_db)

        # df_runs = pd.DataFrame([{
        #     "run_id": run_id,
        #     "module": "tiktok_hashtags",
        #     "run_label": os.getenv("RUN_LABEL", ""),
        #     "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #     "status": "ok",
        #     "new_rows": int(len(df_new)),
        #     "total_rows": int(len(combined)),
        #     "categories": ", ".join([p["category"] for p in plan]),
        # }])
        # insert_df(run_id, "runs", df_runs)

        df_runs = pd.DataFrame([{
            "module": "tiktok_hashtags",
            "run_label": os.getenv("RUN_LABEL", ""),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
            "new_rows": int(len(df_new)),
            "total_rows": int(len(combined)),
            "categories": ", ".join([p["category"] for p in plan]),
        }])
        insert_df(run_id, "runs", df_runs)


        hist = load_history()
        hist.append({
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "run_id": run_id,
            "output_file": OUTPUT_FILE,
            "results_per_page": RESULTS_PER_PAGE,
            "categories": [p["category"] for p in plan],
            "hashtags_by_category": {p["category"]: p["hashtags"] for p in plan},
            "rows_added_this_run": int(len(df_new)),
            "total_rows_after": int(len(combined))
        })
        hist = hist[-50:]
        save_history(hist)

        finish_run(run_id, "ok")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()

