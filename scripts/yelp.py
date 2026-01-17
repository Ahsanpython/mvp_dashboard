from apify_client import ApifyClient
import pandas as pd
import os
import time
from datetime import datetime
import json
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

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()

MASTER_FILE = os.getenv("YELP_MASTER_FILE", "/tmp/Yelp_Master_Data.xlsx")
PROGRESS_FILE = os.getenv("YELP_PROGRESS_FILE", "/tmp/yelp_scraping_progress.json")
SEEN_URLS_FILE = os.getenv("YELP_SEEN_URLS_FILE", "/tmp/yelp_seen_urls.json")

GCS_MASTER_OBJECT = os.getenv("GCS_YELP_MASTER_OBJECT", "exports/yelp/Yelp_Master_Data.xlsx")
GCS_PROGRESS_OBJECT = os.getenv("GCS_YELP_PROGRESS_OBJECT", "exports/yelp/yelp_scraping_progress.json")
GCS_SEEN_URLS_OBJECT = os.getenv("GCS_YELP_SEEN_URLS_OBJECT", "exports/yelp/yelp_seen_urls.json")

SEARCH_KEYWORDS = [
    "Medspa", "Aesthetic Clinic", "Cosmetic Dermatology", "Plastic Surgery",
    "Laser Clinic", "Skin Care Clinic", "Botox", "Filler", "Dysport", "Jeuveau",
    "Kybella", "PRP", "Microneedling", "RF Microneedling", "IPL", "Laser Hair Removal",
    "Tattoo Removal", "Body Contouring", "Hair Restoration", "Cryotherapy",
    "IV Therapy", "IV Hydration", "Weight Loss Clinic", "Semaglutide", "Tirzepatide",
    "GLP-1 Clinic", "Hormone Therapy", "HRT", "TRT", "Peptide Therapy",
    "Functional Medicine", "Longevity Clinic", "Wellness Center",
    "Primary Care", "Urgent Care", "OBGYN", "Chronic Care", "Urology",
    "Chronic Care Management", "Medication Management", "Medicare Clinic",
    "Solo Practice", "Small Group Practice"
]

CITIES = [
    "New York, NY", "Los Angeles, CA", "Miami, FL", "Orlando, FL", "Tampa, FL",
    "Houston, TX", "Dallas, TX", "Austin, TX", "Chicago, IL",
    "Phoenix, AZ", "Scottsdale, AZ", "Las Vegas, NV", "Denver, CO",
    "Atlanta, GA", "Boston, MA", "Seattle, WA", "San Diego, CA",
    "San Francisco, CA", "Newark, NJ", "Charlotte, NC"
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


def load_progress():
    if GCS_BUCKET:
        gcs_download_if_exists(PROGRESS_FILE, GCS_PROGRESS_OBJECT)

    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass

    return {"completed_cities": [], "total_runs": 0}


def save_progress(p):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

    if GCS_BUCKET:
        gcs_path = gcs_upload(PROGRESS_FILE, GCS_PROGRESS_OBJECT)
        if gcs_path:
            print(f"☁️ Uploaded progress: {gcs_path}")


def load_seen_urls():
    if GCS_BUCKET:
        gcs_download_if_exists(SEEN_URLS_FILE, GCS_SEEN_URLS_OBJECT)

    if os.path.exists(SEEN_URLS_FILE):
        try:
            with open(SEEN_URLS_FILE, "r") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_seen_urls(s):
    Path(SEEN_URLS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_URLS_FILE, "w") as f:
        json.dump(list(s), f, indent=2)

    if GCS_BUCKET:
        gcs_path = gcs_upload(SEEN_URLS_FILE, GCS_SEEN_URLS_OBJECT)
        if gcs_path:
            print(f"☁️ Uploaded seen-urls: {gcs_path}")


def get_next_city(progress):
    for c in CITIES:
        if c not in progress.get("completed_cities", []):
            return c
    return CITIES[0]


def load_existing_data():
    if GCS_BUCKET:
        gcs_download_if_exists(MASTER_FILE, GCS_MASTER_OBJECT)

    if os.path.exists(MASTER_FILE):
        try:
            return pd.read_excel(MASTER_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def save_to_master(existing, new, city):
    new["Scraped_Date"] = datetime.now().strftime("%Y-%m-%d")
    new["Batch_City"] = city

    combined = pd.concat([new, existing], ignore_index=True) if not existing.empty else new
    combined.drop_duplicates(subset=["Yelp_URL"], keep="first", inplace=True)

    Path(MASTER_FILE).parent.mkdir(parents=True, exist_ok=True)
    combined.to_excel(MASTER_FILE, index=False)

    if GCS_BUCKET:
        gcs_path = gcs_upload(MASTER_FILE, GCS_MASTER_OBJECT)
        if gcs_path:
            print(f"☁️ Uploaded master file: {gcs_path}")

    return combined


def scrape_city(city, keywords, seen_urls):
    print(f"\nScraping {city}")
    results = []

    for keyword in keywords:
        try:
            run_input = {
                "keywords": [keyword],
                "locations": [city],
                "maxCrawlPages": 20
            }

            run = client.actor("BxxFJax5cSD2VeXkV").call(run_input=run_input)

            for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                url = item.get("url", "")
                if not url or url in seen_urls:
                    continue

                results.append({
                    "Business_Name": item.get("name", ""),
                    "Full_Address": item.get("full_address", ""),
                    "City": item.get("city", ""),
                    "State": item.get("state", ""),
                    "Zipcode": item.get("zipcode", ""),
                    "Phone": item.get("phone_number", ""),
                    "Website": item.get("website", ""),
                    "Yelp_URL": url,
                    "Rating": item.get("rating", ""),
                    "Review_Count": item.get("review_count", ""),
                    "Search_Keyword": keyword,
                    "Search_City": city
                })
                seen_urls.add(url)

            time.sleep(float(os.getenv("SLEEP_SECONDS", "2")))

        except Exception as e:
            print("ERROR:", e)

    return results, seen_urls


def main(selected_city=None, selected_keywords=None, use_progress=False):
    run_id = start_run("yelp", os.getenv("RUN_LABEL", ""))

    try:
        print("🚀 Yelp Scraper")
        print("=" * 40)

        progress = load_progress()
        existing = load_existing_data()
        seen = load_seen_urls()

        if use_progress:
            city = get_next_city(progress)
        else:
            city = selected_city

        if not city:
            city = CITIES[0]

        keywords_to_use = selected_keywords if selected_keywords else SEARCH_KEYWORDS

        new, seen = scrape_city(city, keywords_to_use, seen)

        if new:
            df = pd.DataFrame(new)

            save_to_master(existing, df, city)

            df_live = df.copy()
            df_live["Scraped_Date"] = datetime.now().strftime("%Y-%m-%d")
            df_live["Batch_City"] = city
            df_live["Scraped_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            insert_df(run_id, "yelp", df_live)

            progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
            if city not in progress.get("completed_cities", []):
                progress.setdefault("completed_cities", []).append(city)
            save_progress(progress)

            save_seen_urls(seen)
            print(f"Added {len(df)} new records.")

            finish_run(run_id, "ok")
        else:
            print("No new results.")

            # still mark city as done if you're using progress mode
            if city not in progress.get("completed_cities", []):
                progress.setdefault("completed_cities", []).append(city)
            progress["total_runs"] = int(progress.get("total_runs", 0)) + 1

            save_seen_urls(seen)
            save_progress(progress)
            finish_run(run_id, "ok")


    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    selected_city = os.getenv("SELECTED_CITY")
    raw_keywords = os.getenv("SELECTED_KEYWORDS")
    use_progress = os.getenv("USE_PROGRESS", "0") == "1"

    if not selected_city:
        selected_city = CITIES[0]

    selected_keywords = None
    if raw_keywords and str(raw_keywords).strip():
        selected_keywords = [k.strip() for k in str(raw_keywords).split("|") if k.strip()]

    main(
        selected_city=selected_city,
        selected_keywords=selected_keywords,
        use_progress=use_progress
    )
