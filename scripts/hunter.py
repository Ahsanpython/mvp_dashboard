import re
import requests
import pandas as pd
import os
import json
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path
from db import start_run, finish_run, insert_df

try:
    from google.cloud import storage
except Exception:
    storage = None


INPUT_FILE = os.getenv("HUNTER_INPUT_FILE", "/tmp/Yelp_Master_Data.xlsx")
OUTPUT_FILE = os.getenv("HUNTER_OUTPUT_FILE", "/tmp/Yelp_Master_Data.xlsx")
WEBSITE_COL = os.getenv("HUNTER_WEBSITE_COL", "Website")
YELP_URL_COL = os.getenv("HUNTER_YELP_URL_COL", "Yelp_URL")
PROGRESS_FILE = os.getenv("HUNTER_PROGRESS_FILE", "/tmp/hunter_progress.json")

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "").strip()
if not HUNTER_API_KEY:
    raise ValueError("Missing HUNTER_API_KEY env var")

HUNTER_DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"

GCS_BUCKET = (os.getenv("GCS_BUCKET") or "").strip()
GCS_YELP_MASTER_OBJECT = os.getenv("GCS_YELP_MASTER_OBJECT", "exports/yelp/Yelp_Master_Data.xlsx")
GCS_HUNTER_PROGRESS_OBJECT = os.getenv("GCS_HUNTER_PROGRESS_OBJECT", "exports/yelp/hunter_progress.json")

SKIP_HOSTS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com",
    "tiktok.com", "youtube.com", "youtu.be",
    "goo.gl", "bit.ly", "tinyurl.com",
    "yelp.com",
    "www.facebook.com", "www.instagram.com", "www.linkedin.com", "www.twitter.com", "www.x.com",
    "www.tiktok.com", "www.youtube.com", "www.yelp.com",
}

FINAL_STATUSES = {
    "person_email_found",
    "generic_email_only",
    "no_emails_found",
    "skipped_blank_or_nonbusiness_url",
    "api_error",
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


def load_progress():
    if GCS_BUCKET:
        gcs_download_if_exists(PROGRESS_FILE, GCS_HUNTER_PROGRESS_OBJECT)

    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass

    return {
        "processed_yelp_urls": [],
        "total_runs": 0,
        "last_run_date": None,
        "total_rows_enriched": 0,
    }


def save_progress(p):
    Path(PROGRESS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2)

    if GCS_BUCKET:
        gcs_upload(PROGRESS_FILE, GCS_HUNTER_PROGRESS_OBJECT)


def to_domain(value):
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "-", "null"}:
        return None

    s = s.split()[0].strip()

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", s):
        s = "http://" + s

    try:
        p = urlparse(s)
        host = (p.netloc or "").strip().lower()
        host = host.split(":")[0]
        host = host.lstrip("www.")
        if not host:
            return None
        if host in SKIP_HOSTS:
            return None
        return host
    except Exception:
        return None


def hunter_domain_search(domain: str, limit: int = 10) -> dict:
    params = {"domain": domain, "api_key": HUNTER_API_KEY, "limit": limit}
    try:
        r = requests.get(HUNTER_DOMAIN_SEARCH_URL, params=params, timeout=30)
        try:
            payload = r.json()
        except Exception:
            payload = {"raw_text": r.text}

        if r.status_code == 429:
            return {"_error": "rate_limited_429", "_payload": payload}

        if r.status_code != 200:
            return {"_error": f"http_{r.status_code}", "_payload": payload}

        return payload
    except Exception as e:
        return {"_error": f"request_failed: {e}", "_payload": {}}


def pick_best_email(emails):
    if not emails:
        return None

    def score(e):
        has_name = 1 if (e.get("first_name") and e.get("last_name")) else 0
        conf = e.get("confidence") or 0
        return (has_name, conf)

    return sorted(emails, key=score, reverse=True)[0]


def org_name_from_payload(payload: dict) -> str:
    org = payload.get("organization")
    if isinstance(org, dict):
        return org.get("name", "") or ""
    if isinstance(org, str):
        return org
    return ""


def row_already_done(df, i, processed_urls: set) -> bool:
    yelp_url = str(df.at[i, YELP_URL_COL]) if YELP_URL_COL in df.columns else ""
    yelp_url = (yelp_url or "").strip()

    status = str(df.at[i, "hunter_status"]).strip() if "hunter_status" in df.columns else ""
    domain = str(df.at[i, "hunter_domain"]).strip() if "hunter_domain" in df.columns else ""

    if yelp_url and yelp_url in processed_urls:
        return True
    if status in FINAL_STATUSES:
        return True
    if domain and status:
        return True
    return False


def main():
    run_id = start_run("hunter", os.getenv("RUN_LABEL", ""))

    try:
        if not HUNTER_API_KEY:
            raise SystemExit("Hunter API key missing")

        if GCS_BUCKET:
            gcs_download_if_exists(INPUT_FILE, GCS_YELP_MASTER_OBJECT)

        if not os.path.exists(INPUT_FILE):
            raise SystemExit(f"Input file not found: {INPUT_FILE}")

        df = pd.read_excel(INPUT_FILE)

        if WEBSITE_COL not in df.columns:
            raise SystemExit(f"Column '{WEBSITE_COL}' not found in {INPUT_FILE}")

        if YELP_URL_COL not in df.columns:
            raise SystemExit(f"Column '{YELP_URL_COL}' not found in {INPUT_FILE}")

        out_cols = [
            "hunter_domain",
            "hunter_status",
            "hunter_error",
            "hunter_email_count",
            "hunter_generic_emails",
            "hunter_company",
            "hunter_email",
            "hunter_first_name",
            "hunter_last_name",
            "hunter_position",
            "hunter_seniority",
            "hunter_department",
            "hunter_confidence",
            "hunter_type",
            "hunter_enriched_at",
        ]
        for c in out_cols:
            if c not in df.columns:
                df[c] = ""

        progress = load_progress()
        processed_urls = set(progress.get("processed_yelp_urls", []))

        test = hunter_domain_search("hunter.io", limit=5)
        if test.get("_error"):
            print("API TEST FAILED:", test.get("_error"))
        else:
            test_emails = ((test.get("data") or {}).get("emails") or [])
            print("API TEST OK. Sample people emails:", len(test_emails))

        cache = {}
        enriched_now = 0
        skipped_done = 0

        live_rows = []

        for i, raw_site in enumerate(df[WEBSITE_COL].tolist()):
            if row_already_done(df, i, processed_urls):
                skipped_done += 1
                continue

            yelp_url = str(df.at[i, YELP_URL_COL]).strip()
            domain = to_domain(raw_site)
            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if not domain:
                df.at[i, "hunter_status"] = "skipped_blank_or_nonbusiness_url"
                df.at[i, "hunter_enriched_at"] = now_ts
                if yelp_url:
                    processed_urls.add(yelp_url)
                enriched_now += 1

                live_rows.append({
                    "yelp_url": yelp_url,
                    "website": str(raw_site),
                    "hunter_domain": "",
                    "hunter_status": "skipped_blank_or_nonbusiness_url",
                    "hunter_error": "",
                    "hunter_company": "",
                    "hunter_email": "",
                    "hunter_enriched_at": now_ts,
                })
                continue

            df.at[i, "hunter_domain"] = domain

            if domain in cache:
                data = cache[domain]
            else:
                data = hunter_domain_search(domain, limit=10)
                cache[domain] = data

            if data.get("_error"):
                df.at[i, "hunter_status"] = "api_error"
                df.at[i, "hunter_error"] = str(data.get("_error"))
                df.at[i, "hunter_enriched_at"] = now_ts
                enriched_now += 1

                live_rows.append({
                    "yelp_url": yelp_url,
                    "website": str(raw_site),
                    "hunter_domain": domain,
                    "hunter_status": "api_error",
                    "hunter_error": str(data.get("_error")),
                    "hunter_company": "",
                    "hunter_email": "",
                    "hunter_enriched_at": now_ts,
                })
                continue

            payload = data.get("data") or {}
            emails = payload.get("emails") or []
            generic_emails = payload.get("generic_emails") or []

            df.at[i, "hunter_email_count"] = len(emails)
            df.at[i, "hunter_generic_emails"] = ", ".join(generic_emails[:5])
            df.at[i, "hunter_company"] = org_name_from_payload(payload)

            best = pick_best_email(emails)
            if best:
                df.at[i, "hunter_email"] = best.get("value", "") or ""
                df.at[i, "hunter_first_name"] = best.get("first_name", "") or ""
                df.at[i, "hunter_last_name"] = best.get("last_name", "") or ""
                df.at[i, "hunter_position"] = best.get("position", "") or ""
                df.at[i, "hunter_seniority"] = best.get("seniority", "") or ""
                df.at[i, "hunter_department"] = best.get("department", "") or ""
                df.at[i, "hunter_confidence"] = best.get("confidence", "") or ""
                df.at[i, "hunter_type"] = best.get("type", "") or ""
                df.at[i, "hunter_status"] = "person_email_found"
            else:
                if generic_emails:
                    df.at[i, "hunter_email"] = generic_emails[0]
                    df.at[i, "hunter_status"] = "generic_email_only"
                else:
                    df.at[i, "hunter_status"] = "no_emails_found"

            df.at[i, "hunter_enriched_at"] = now_ts

            if yelp_url:
                processed_urls.add(yelp_url)

            enriched_now += 1

            live_rows.append({
                "yelp_url": yelp_url,
                "website": str(raw_site),
                "hunter_domain": domain,
                "hunter_status": str(df.at[i, "hunter_status"]),
                "hunter_error": str(df.at[i, "hunter_error"]) if "hunter_error" in df.columns else "",
                "hunter_company": str(df.at[i, "hunter_company"]),
                "hunter_email": str(df.at[i, "hunter_email"]),
                "hunter_enriched_at": now_ts,
            })

        Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(OUTPUT_FILE, index=False)

        if GCS_BUCKET:
            gcs_upload(OUTPUT_FILE, GCS_YELP_MASTER_OBJECT)

        if live_rows:
            df_live = pd.DataFrame(live_rows)
            insert_df(run_id, "hunter_enriched", df_live)


        progress["processed_yelp_urls"] = sorted(list(processed_urls))
        progress["total_runs"] = int(progress.get("total_runs", 0)) + 1
        progress["last_run_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        progress["total_rows_enriched"] = int(progress.get("total_rows_enriched", 0)) + int(enriched_now)
        save_progress(progress)

        print(f"Saved: {OUTPUT_FILE}")
        print(f"Rows enriched this run: {enriched_now}")
        print(f"Rows skipped (already done): {skipped_done}")

        finish_run(run_id, "ok")

    except Exception:
        finish_run(run_id, "error")
        raise


if __name__ == "__main__":
    main()
