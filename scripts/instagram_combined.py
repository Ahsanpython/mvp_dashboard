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

try:
    from sqlalchemy import create_engine
except Exception:
    create_engine = None


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

SAVE_REELS_RAW = os.getenv("IG_SAVE_REELS_RAW", "0") == "1"
REELS_RAW_FILE = os.getenv("IG_REELS_RAW_FILE", "/tmp/instagram_reels_raw.xlsx")

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()

GCS_IG_OUTPUT_OBJECT = os.getenv(
    "GCS_IG_OUTPUT_OBJECT",
    "exports/social/instagram/instagram_influencers.xlsx"
)

GCS_IG_PROGRESS_OBJECT = os.getenv(
    "GCS_IG_PROGRESS_OBJECT",
    "exports/social/instagram/instagram_progress.json"
)

GCS_IG_REELS_RAW_OBJECT = os.getenv(
    "GCS_IG_REELS_RAW_OBJECT",
    "exports/social/instagram/instagram_reels_raw.xlsx"
)

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
DB_TABLE = os.getenv("IG_DB_TABLE", "instagram_influencers")


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


def cat_to_env_key(cat_name: str) -> str:
    return "IG_HASHTAGS_" + cat_name.upper().replace(" ", "_")


def get_selected_categories():
    raw = os.getenv("IG_CATEGORIES", "").strip()
    if raw:
        selected = parse_pipe_list(raw)
        selected = [c for c in selected if c in CATEGORIES]
        if selected:
            return selected
    return list(CATEGORIES.keys())


def get_selected_hashtags_for_category(cat):
    raw = os.getenv(cat_to_env_key(cat), "").strip()
    if raw:
        tags = parse_pipe_list(raw)
        allowed = set(CATEGORIES.get(cat, []))
        tags = [t for t in tags if t in allowed]
        if tags:
            return tags
    return CATEGORIES.get(cat, [])


def load_progress():
    if GCS_BUCKET:
        gcs_download_if_exists(PROGRESS_FILE, GCS_IG_PROGRESS_OBJECT)

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
        gcs_upload(PROGRESS_FILE, GCS_IG_PROGRESS_OBJECT)


def normalize_followers(f):
    try:
        f = float(f)
    except:
        f = 0.0
    if f <= 1000:
        return f / 1000
    elif f <= 10000:
        return 0.1 + (f - 1000) / 9000 * 0.4
    elif f <= 100000:
        return 0.5 + (f - 10000) / 90000 * 0.4
    else:
        return 0.9 + min((f - 100000) / 1000000, 0.1)


def scrape_reels(category, hashtag):
    print(f"🚀 Scraping Reels | Category: {category} | #{hashtag}")
    run_input = {"hashtags": [hashtag], "resultsType": "reels", "resultsLimit": RESULTS_PER_HASHTAG}
    run = client.actor(REELS_ACTOR_ID).call(run_input=run_input)

    rows = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        rows.append({
            "category": category,
            "keyword": hashtag,
            "username": item.get("ownerUsername", "") or "",
            "caption": item.get("caption", "") or "",
            "likes": item.get("likesCount", 0) or 0,
            "comments": item.get("commentsCount", 0) or 0,
            "views": item.get("videoPlayCount", 0) or 0,
            "video_url": item.get("videoUrl", "") or "",
            "post_url": item.get("url", "") or "",
            "reel_timestamp": item.get("timestamp", "") or ""
        })
    print(f"✅ Done #{hashtag} → {len(rows)} reels")
    return rows


def batch_profile_scrape(usernames):
    run_input = {"usernames": usernames, "includeAboutSection": False}
    run = client.actor(PROFILE_ACTOR_ID).call(run_input=run_input)

    profiles = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        profiles.append({
            "username": item.get("username", "") or "",
            "followers": item.get("followersCount", 0) or 0,
            "verified": item.get("isVerified", False),
            "bio": item.get("biography", "") or ""
        })
    return pd.DataFrame(profiles)


def load_existing_output():
    if GCS_BUCKET:
        gcs_download_if_exists(OUTPUT_FILE, GCS_IG_OUTPUT_OBJECT)

    if os.path.exists(OUTPUT_FILE):
        try:
            df = pd.read_excel(OUTPUT_FILE)
            if "username" in df.columns:
                df["username"] = df["username"].astype(str).str.strip()
            return df
        except:
            return pd.DataFrame()
    return pd.DataFrame()


# def save_output(df: pd.DataFrame):
#     Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
#     df.to_excel(OUTPUT_FILE, index=False)

#     if GCS_BUCKET:
#         gcs_upload(OUTPUT_FILE, GCS_IG_OUTPUT_OBJECT)

def save_output(df: pd.DataFrame):
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_FILE, index=False)

    if GCS_BUCKET:
        gcs_upload(OUTPUT_FILE, GCS_IG_OUTPUT_OBJECT)

    run_id = os.getenv("RUN_ID", "") or ""
    if DATABASE_URL:
        try:
            insert_df(run_id, DB_TABLE, df)
        except Exception:
            pass


def main():
    run_id = start_run("instagram_combined", os.getenv("RUN_LABEL", ""))
    os.environ["RUN_ID"] = run_id

    try:
        progress = load_progress()
        done = set([str(u).lower() for u in progress.get("processed_usernames", [])])

        selected_categories = get_selected_categories()
        plan = []
        for cat in selected_categories:
            tags = get_selected_hashtags_for_category(cat)
            plan.append({"category": cat, "hashtags": tags})

        print("\n🧭 Run plan:")
        for p in plan:
            print(f"  - {p['category']}: {len(p['hashtags'])} hashtags")

        all_reels = []
        for p in plan:
            for tag in p["hashtags"]:
                all_reels.extend(scrape_reels(p["category"], tag))

        df_reels = pd.DataFrame(all_reels)
        if df_reels.empty:
            print("No reels collected.")
            finish_run(run_id, "ok")
            return

        df_reels["username"] = df_reels["username"].astype(str).str.strip()
        df_reels = df_reels[df_reels["username"].notna() & (df_reels["username"] != "")]

        if SAVE_REELS_RAW:
            Path(REELS_RAW_FILE).parent.mkdir(parents=True, exist_ok=True)
            df_reels.to_excel(REELS_RAW_FILE, index=False)
            if GCS_BUCKET and os.path.exists(REELS_RAW_FILE):
                gcs_upload(REELS_RAW_FILE, GCS_IG_REELS_RAW_OBJECT)

        df_reels_dedup = df_reels.drop_duplicates(subset=["username"], keep="first")
        usernames = df_reels_dedup["username"].tolist()

        usernames_to_scrape = [u for u in usernames if u.lower() not in done]
        print(f"✅ Unique usernames from reels: {len(usernames)}")
        print(f"🆕 New usernames to profile-scrape: {len(usernames_to_scrape)} (skipping {len(usernames) - len(usernames_to_scrape)})")

        df_existing = load_existing_output()

        batch_profiles_all = []
        total_batches = math.ceil(len(usernames_to_scrape) / BATCH_SIZE) if usernames_to_scrape else 0

        for b in range(total_batches):
            batch = usernames_to_scrape[b * BATCH_SIZE:(b + 1) * BATCH_SIZE]
            if not batch:
                continue

            print(f"\n🚀 Profile Batch {b+1}/{total_batches} | {len(batch)} usernames")

            try:
                df_profiles = batch_profile_scrape(batch)
                if not df_profiles.empty:
                    batch_profiles_all.append(df_profiles)
                    for u in df_profiles["username"].astype(str).tolist():
                        if u:
                            done.add(u.lower())
                time.sleep(6)
            except Exception as e:
                print("⚠️ Batch profile error:", e)
                time.sleep(10)

        df_profiles_all = pd.concat(batch_profiles_all, ignore_index=True) if batch_profiles_all else pd.DataFrame(columns=["username", "followers", "verified", "bio"])

        df_first = df_reels_dedup.merge(df_profiles_all, on="username", how="left")

        df_first["likes"] = pd.to_numeric(df_first["likes"], errors="coerce").fillna(0)
        df_first["comments"] = pd.to_numeric(df_first["comments"], errors="coerce").fillna(0)
        df_first["views"] = pd.to_numeric(df_first["views"], errors="coerce").fillna(0)
        df_first["followers"] = pd.to_numeric(df_first["followers"], errors="coerce").fillna(0)

        df_first["engagement_rate"] = df_first.apply(
            lambda r: round(((r["likes"] + r["comments"]) / r["views"]) * 100, 2) if r["views"] > 0 else 0,
            axis=1
        )
        df_first["follower_score"] = df_first["followers"].apply(normalize_followers)
        df_first["final_score"] = df_first.apply(
            lambda r: round((r["engagement_rate"] / 100) * 0.4 + r["follower_score"] * 0.6, 2),
            axis=1
        )

        df_final = df_first[[
            "category", "keyword",
            "username", "followers", "verified", "bio",
            "likes", "comments", "views",
            "engagement_rate", "final_score",
            "post_url", "video_url",
            "reel_timestamp"
        ]].copy()

        df_final["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Insert ONLY new rows into Cloud SQL (this run)
        try:
            df_live = df_final.copy()
            insert_df(run_id, DB_TABLE, df_live)
        except Exception as e:
            print("DB insert failed:", e)

        if not df_existing.empty and "username" in df_existing.columns:
            combined = pd.concat([df_final, df_existing], ignore_index=True)
        else:
            combined = df_final

        combined["username"] = combined["username"].astype(str).str.strip()
        combined.drop_duplicates(subset=["username"], keep="first", inplace=True)

        save_output(combined)

        progress["processed_usernames"] = sorted(list(set(progress.get("processed_usernames", []) + list(done))))
        progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
        progress["last_run_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_progress(progress)

        df_runs = pd.DataFrame([{
            "module": "instagram_combined",
            "run_label": os.getenv("RUN_LABEL", ""),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
            "unique_usernames": int(len(usernames)),
            "new_profiles_scraped": int(len(usernames_to_scrape)),
            "rows_saved_total": int(len(combined)),
        }])
        try:
            insert_df(run_id, "runs", df_runs)
        except Exception:
            pass

        finish_run(run_id, "ok")

        print(f"\n✅ Saved final influencers: {OUTPUT_FILE}")
        print(f"✅ Total influencers in file: {len(combined)}")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
