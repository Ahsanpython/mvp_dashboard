from apify_client import ApifyClient
import pandas as pd
import os
import json
import time
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

VIDEOS_ACTOR_ID = os.getenv("YT_VIDEOS_ACTOR_ID", "89uTe0zmDUIatNKSd")
CHANNELS_ACTOR_ID = os.getenv("YT_CHANNELS_ACTOR_ID", "67Q6fmd8iedTVcCwY")
MAX_RESULTS = int(os.getenv("YT_MAX_RESULTS", "1000"))

OUTPUT_FILE = os.getenv("YT_OUTPUT_FILE", "/tmp/youtube_influencers_combined.xlsx")
RUN_HISTORY_FILE = os.getenv("YT_RUN_HISTORY_FILE", "/tmp/youtube_run_history.json")

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_YT_OUTPUT_OBJECT = os.getenv("GCS_YT_OUTPUT_OBJECT", "exports/social/youtube/youtube_influencers_combined.xlsx")
GCS_YT_HISTORY_OBJECT = os.getenv("GCS_YT_HISTORY_OBJECT", "exports/social/youtube/youtube_run_history.json")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
DB_TABLE = os.getenv("YT_DB_TABLE", "youtube_influencers")

CATEGORIES = {
    "Peptides": [
        "peptides", "collagenpeptides", "copperpeptides",
        "skincare", "antiaging", "bpc157", "tb500", "prp"
    ],
    "Biohacking": [
        "biohacking", "biohacker", "longevity", "nootropics",
        "redlighttherapy", "functionalmedicine", "sleepoptimization", "recovery"
    ],
    "Regenerative Medicine": [
        "regenerativemedicine", "prp", "stemcelltherapy",
        "exosomes", "sportsmedicine", "orthopedics",
        "hairrestoration", "celltherapy"
    ]
}

client = ApifyClient(APIFY_TOKEN)


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



def normalize_subscribers(subs):
    try:
        s = int(subs)
    except:
        s = 0
    if s <= 1000:
        return s / 1000
    elif s <= 10000:
        return 0.1 + (s - 1000) / 9000 * 0.4
    elif s <= 100000:
        return 0.5 + (s - 10000) / 90000 * 0.4
    else:
        return 0.9 + min((s - 100000) / 1_000_000, 0.1)


def calc_engagement(total_views, total_videos, subs):
    try:
        tv, nv, s = float(total_views), float(total_videos), float(subs)
    except:
        return 0.0
    if nv == 0 or s == 0:
        return 0.0
    return round((tv / nv / s) * 100, 2)


def calc_final_score(eng, sub_score, act_score):
    return round((eng * 0.4) + (sub_score * 100 * 0.3) + (act_score * 100 * 0.3), 2)


def load_history():
    if GCS_BUCKET:
        gcs_download_if_exists(RUN_HISTORY_FILE, GCS_YT_HISTORY_OBJECT)

    if os.path.exists(RUN_HISTORY_FILE):
        try:
            with open(RUN_HISTORY_FILE, "r") as f:
                return json.load(f)
        except:
            return []
    return []


def save_history(history):
    Path(RUN_HISTORY_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(RUN_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(RUN_HISTORY_FILE, GCS_YT_HISTORY_OBJECT)


def parse_pipe_list(val):
    if not val:
        return []
    s = str(val).strip()
    if not s:
        return []
    return [x.strip() for x in s.split("|") if x.strip()]


def cat_to_env_key(cat_name: str) -> str:
    return "YT_HASHTAGS_" + cat_name.upper().replace(" ", "_")


def get_selected_categories():
    raw = os.getenv("YT_CATEGORIES", "").strip()
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


def fetch_channels_for_hashtags(category, hashtags):
    channels = []
    for tag in hashtags:
        print(f"\n🚀 Scraping videos for #{tag} in category [{category}] ...")
        run_input = {"hashtags": [tag], "maxResults": MAX_RESULTS}
        run = client.actor(VIDEOS_ACTOR_ID).call(run_input=run_input)

        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            url = item.get("channelUrl")
            if url and url not in channels:
                channels.append(url)

        time.sleep(1)

    print(f"✅ Found {len(channels)} unique channels for [{category}]")
    return channels


def bulk_fetch_channel_details(category, urls):
    if not urls:
        return []
    print(f"🏎️ Bulk scraping {len(urls)} channels for [{category}] ...")
    run_input = {"startUrls": [{"url": u} for u in urls], "maxResults": 1}
    run = client.actor(CHANNELS_ACTOR_ID).call(run_input=run_input)

    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append({
            "category": category,
            "channel_name": item.get("channelName", ""),
            "channel_url": item.get("channelUrl", ""),
            "channel_username": item.get("channelUsername", ""),
            "subscribers": item.get("numberOfSubscribers", 0),
            "total_views": item.get("channelTotalViews", 0),
            "total_videos": item.get("channelTotalVideos", 0),
            "verified": item.get("isChannelVerified", False),
            "country": item.get("channelLocation", ""),
            "joined_date": item.get("channelJoinedDate", ""),
            "description": item.get("channelDescription", ""),
            "timestamp": pd.Timestamp.now()
        })
    print(f"📥 Received {len(results)} channels for [{category}]")
    return results


def main():
    run_id = start_run("youtube", os.getenv("RUN_LABEL", ""))

    try:
        if GCS_BUCKET:
            gcs_download_if_exists(OUTPUT_FILE, GCS_YT_OUTPUT_OBJECT)

        if os.path.exists(OUTPUT_FILE):
            df_all = pd.read_excel(OUTPUT_FILE)
            known = set(str(u) for u in df_all.get("channel_url", []) if str(u).strip())
            print(f"📂 Loaded existing file ({len(df_all)} rows, {len(known)} known channels)")
        else:
            df_all = pd.DataFrame()
            known = set()
            print("📂 No previous data file found.")

        selected_categories = get_selected_categories()

        category_plan = []
        for cat in selected_categories:
            tags = get_selected_hashtags_for_category(cat)
            category_plan.append({"category": cat, "hashtags": tags})

        print("\n🧭 Run plan:")
        for p in category_plan:
            print(f"  - {p['category']}: {len(p['hashtags'])} hashtags")

        all_new = []
        new_by_cat = {}

        for p in category_plan:
            cat = p["category"]
            tags = p["hashtags"]

            urls = fetch_channels_for_hashtags(cat, tags)
            new_urls = [u for u in urls if u not in known]
            print(f"🆕 {len(new_urls)} new channels in [{cat}] (skipping {len(urls) - len(new_urls)} duplicates)")

            new_rows = bulk_fetch_channel_details(cat, new_urls)
            all_new.extend(new_rows)
            known.update(new_urls)

            new_by_cat[cat] = len(new_rows)

        df_new = pd.DataFrame(all_new) if all_new else pd.DataFrame()

        if not df_new.empty:
            for col in ["subscribers", "total_views", "total_videos"]:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce").fillna(0).astype(int)

            df_new["engagement_rate(%)"] = df_new.apply(
                lambda r: calc_engagement(r["total_views"], r["total_videos"], r["subscribers"]), axis=1
            )
            df_new["subscriber_score"] = df_new["subscribers"].apply(normalize_subscribers).round(3)
            df_new["activity_score"] = (df_new["total_videos"] / 500).clip(0, 1).round(3)
            df_new["final_score"] = df_new.apply(
                lambda r: calc_final_score(r["engagement_rate(%)"], r["subscriber_score"], r["activity_score"]), axis=1
            )

            df_all = pd.concat([df_new, df_all], ignore_index=True)
        else:
            print("No new channels scraped this run.")

        if not df_all.empty:
            df_all.drop_duplicates(subset=["channel_url"], keep="first", inplace=True)
            if "timestamp" in df_all.columns:
                df_all.sort_values(["timestamp"], ascending=False, inplace=True)

        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        df_all.to_excel(OUTPUT_FILE, index=False)
        print(f"\n💾 Saved {len(df_all)} total influencers to '{OUTPUT_FILE}' ✅")

        if GCS_BUCKET:
            gcs_upload(OUTPUT_FILE, GCS_YT_OUTPUT_OBJECT)

        if DATABASE_URL and not df_new.empty:
            df_db = df_new.copy()
            insert_df(run_id, DB_TABLE, df_db)


        # df_runs = pd.DataFrame([{
        #     "run_id": run_id,
        #     "module": "youtube",
        #     "run_label": os.getenv("RUN_LABEL", ""),
        #     "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        #     "status": "ok",
        #     "new_rows": int(len(df_new)),
        #     "total_rows": int(len(df_all)),
        #     "categories": ", ".join(selected_categories),
        # }])
        # insert_df(run_id, "runs", df_runs)

        df_runs = pd.DataFrame([{
            "module": "youtube",
            "run_label": os.getenv("RUN_LABEL", ""),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "ok",
            "new_rows": int(len(df_new)),
            "total_rows": int(len(df_all)),
        "categories": ", ".join(selected_categories),
        }])
        insert_df(run_id, "runs", df_runs)


        history = load_history()
        history.append({
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "run_id": run_id,
            "output_file": OUTPUT_FILE,
            "max_results": MAX_RESULTS,
            "selected_categories": selected_categories,
            "hashtags_by_category": {p["category"]: p["hashtags"] for p in category_plan},
            "new_channels_total": int(len(df_new)),
            "new_channels_by_category": new_by_cat
        })
        history = history[-50:]
        save_history(history)

        finish_run(run_id, "ok")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
