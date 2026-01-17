import os
import subprocess
import sys

JOB_NAME = (os.getenv("JOB_NAME") or "").strip()

SCRIPTS = {
    "maps": "scripts/maps.py",
    "yelp": "scripts/yelp.py",
    "hunter": "scripts/hunter.py",
    "youtube": "scripts/youtube.py",
    "tiktok_hashtags": "scripts/tiktok_hashtag_scraper.py",
    "tiktok_followers": "scripts/tiktok_followers_scraper.py",
    "instagram_combined": "scripts/instagram_combined.py",
    "instagram_followers": "scripts/instagram_followers_scraper.py",
}

def main():
    if not JOB_NAME:
        print("Missing JOB_NAME env var. Allowed:", ", ".join(sorted(SCRIPTS.keys())))
        sys.exit(2)

    script = SCRIPTS.get(JOB_NAME)
    if not script:
        print("Invalid JOB_NAME:", JOB_NAME)
        print("Allowed:", ", ".join(sorted(SCRIPTS.keys())))
        sys.exit(2)

    print("Running:", script)
    subprocess.check_call([sys.executable, script])

if __name__ == "__main__":
    main()
