from apify_client import ApifyClient
import pandas as pd
import os
import time
from datetime import datetime
import json
from pathlib import Path
import sys
import traceback

# Allow importing db.py from project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

print("=== MAPS JOB STARTED ===", flush=True)
print("python:", sys.version, flush=True)
print("cwd:", os.getcwd(), flush=True)
print("argv:", sys.argv, flush=True)

# Show key envs (DON'T print secrets)
print("DATABASE_URL set:", bool(os.getenv("DATABASE_URL")), flush=True)
print("APIFY_TOKEN set:", bool(os.getenv("APIFY_TOKEN")), flush=True)
print("GCS_BUCKET:", (os.getenv("GCS_BUCKET") or "").strip(), flush=True)
print("USE_PROGRESS:", os.getenv("USE_PROGRESS", ""), flush=True)
print("CITY:", os.getenv("CITY", ""), flush=True)
print("KEYWORDS:", os.getenv("KEYWORDS", ""), flush=True)
print("SELECTED_CITY:", os.getenv("SELECTED_CITY", ""), flush=True)
print("SELECTED_KEYWORDS:", os.getenv("SELECTED_KEYWORDS", ""), flush=True)

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
GCS_MASTER_OBJECT = os.getenv("GCS_MAPS_MASTER_OBJECT", "exports/maps/Medical_Aesthetics_Master_Data.xlsx")
GCS_PROGRESS_OBJECT = os.getenv("GCS_MAPS_PROGRESS_OBJECT", "exports/maps/scraping_progress.json")

# Cloud Run writable paths
MASTER_FILE = os.getenv("MAPS_MASTER_FILE", "/tmp/Medical_Aesthetics_Master_Data.xlsx")
PROGRESS_FILE = os.getenv("MAPS_PROGRESS_FILE", "/tmp/scraping_progress.json")

SEARCH_KEYWORDS = [
    "Medspa", "Aesthetic Clinic", "Cosmetic Dermatology", "Plastic Surgery",
    "Laser Clinic", "Skin Care Clinic", "Botox", "Filler", "Dysport", "Jeuveau",
    "Kybella", "PRP", "Microneedling", "RF Microneedling", "IPL", "Laser Hair Removal",
    "Tattoo Removal", "Body Contouring", "Hair Restoration", "Cryotherapy",
    "IV Therapy", "IV Hydration", "Weight Loss Clinic", "Semaglutide", "Tirzepatide",
    "GLP-1 Clinic", "Hormone Therapy", "HRT", "TRT", "Peptide Therapy",
    "Functional Medicine", "Longevity Clinic", "Wellness Center"
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

    return {
        "last_completed_city": None,
        "completed_cities": [],
        "total_runs": 0,
        "last_run_date": None
    }


def save_progress(progress):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(PROGRESS_FILE, GCS_PROGRESS_OBJECT)


def get_next_city(progress):
    completed_cities = progress.get("completed_cities", [])
    for city in CITIES:
        if city not in completed_cities:
            return city
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


def save_to_master(existing_data, new_data, city):
    new_data["Scraped_Date"] = datetime.now().strftime("%Y-%m-%d")
    new_data["Scraped_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_data["Batch_City"] = city

    if existing_data.empty:
        combined_data = new_data
    else:
        combined_data = pd.concat([new_data, existing_data], ignore_index=True)

    combined_data = combined_data.drop_duplicates(
        subset=["Title", "Phone", "Website"],
        keep="first"
    )

    Path(MASTER_FILE).parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(MASTER_FILE, engine="openpyxl") as writer:
        combined_data.to_excel(writer, sheet_name="Master Data", index=False)

        worksheet = writer.sheets["Master Data"]
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except Exception:
                    pass
            worksheet.column_dimensions[column_letter].width = min(max_length + 2, 50)

    if GCS_BUCKET:
        gcs_upload(MASTER_FILE, GCS_MASTER_OBJECT)

    return combined_data


def scrape_city(city, keywords):
    city_results = []
    print(f"Scraping city={city} keywords_count={len(keywords)}", flush=True)

    for keyword in keywords:
        search_query = f"{keyword} {city}"
        print("Query:", search_query, flush=True)

        run_input = {
            "searchStringsArray": [search_query],
            "maxCrawledPlaces": 100,
            "language": "en",
            "maxImages": 0,
            "maxReviews": 0,
            "includeWebResults": True,
            "startUrls": [],
            "proxyConfig": {"useApifyProxy": True},
        }

        run = client.actor("WnMxbsRLNbPeYL6ge").call(run_input=run_input)
        ds_id = run.get("defaultDatasetId")
        print("Apify dataset:", ds_id, flush=True)

        count = 0
        for item in client.dataset(ds_id).iterate_items():
            count += 1
            city_results.append({
                "Title": item.get("title", ""),
                "Address": item.get("address", ""),
                "Phone": item.get("phone", ""),
                "Website": item.get("website", ""),
                "City": item.get("city", ""),
                "State": item.get("state", ""),
                "Emails": ", ".join(item.get("emails", [])) if item.get("emails") else "",
                "Search_Keyword": keyword,
                "Search_City": city,
            })

        print(f"Items fetched for '{keyword}': {count}", flush=True)
        time.sleep(float(os.getenv("SLEEP_SECONDS", "2")))

    print("Total results:", len(city_results), flush=True)
    return city_results


def _parse_keywords(s: str):
    s = (s or "").strip()
    if not s:
        return None
    # support | or , separated
    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
    elif "," in s:
        parts = [p.strip() for p in s.split(",")]
    else:
        parts = [s.strip()]
    parts = [p for p in parts if p]
    return parts or None


def main(selected_city=None, selected_keywords=None, use_progress=False):
    run_id = start_run("maps", os.getenv("RUN_LABEL", ""))

    try:
        progress = load_progress()
        existing_data = load_existing_data()

        if use_progress:
            current_city = get_next_city(progress)
        else:
            current_city = selected_city

        if not current_city:
            raise ValueError("selected_city is required when use_progress=False")

        keywords_to_use = selected_keywords if selected_keywords else SEARCH_KEYWORDS

        new_results = scrape_city(current_city, keywords_to_use)

        if new_results:
            new_df = pd.DataFrame(new_results)

            # Excel master (local /tmp + optional GCS)
            save_to_master(existing_data, new_df, current_city)

            # Live dashboard: insert ONLY new rows (this run) into DB
            df_live = new_df.copy()
            df_live["Scraped_Date"] = datetime.now().strftime("%Y-%m-%d")
            df_live["Scraped_Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_live["Batch_City"] = current_city
            insert_df(run_id, "maps", df_live)

            progress["last_completed_city"] = current_city
            if current_city not in progress.get("completed_cities", []):
                progress["completed_cities"] = progress.get("completed_cities", []) + [current_city]
            progress["total_runs"] = progress.get("total_runs", 0) + 1
            progress["last_run_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            save_progress(progress)

            finish_run(run_id, "ok", meta={"rows": int(len(new_df)), "city": current_city})
            print("=== MAPS JOB FINISHED OK (rows:", len(new_df), ") ===", flush=True)
            return

        # no results
        progress["last_completed_city"] = current_city
        if current_city not in progress.get("completed_cities", []):
            progress["completed_cities"] = progress.get("completed_cities", []) + [current_city]
        save_progress(progress)

        finish_run(run_id, "ok", meta={"rows": 0, "city": current_city})
        print("=== MAPS JOB FINISHED OK (0 rows) ===", flush=True)
        return

    except Exception as e:
        print("=== MAPS JOB ERROR ===", flush=True)
        traceback.print_exc()
        finish_run(run_id, "error", meta={"error": str(e)})
        raise


if __name__ == "__main__":
    # Support BOTH naming styles from your job env
    # Priority: SELECTED_* then CITY/KEYWORDS
    selected_city = (os.getenv("SELECTED_CITY") or os.getenv("CITY") or "").strip()
    raw_keywords = (os.getenv("SELECTED_KEYWORDS") or os.getenv("KEYWORDS") or "").strip()
    use_progress = os.getenv("USE_PROGRESS", "0").strip() == "1"

    selected_keywords = _parse_keywords(raw_keywords)

    main(
        selected_city=selected_city if selected_city else None,
        selected_keywords=selected_keywords,
        use_progress=use_progress
    )
