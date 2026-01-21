# jobs.py
import os
from typing import Dict, Any, Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession


def _project_id() -> str:
    return (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or os.getenv("PROJECT_ID")
        or ""
    ).strip()


def _region() -> str:
    return os.getenv("GCP_REGION", "").strip()


def trigger_job(job_name: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution via Cloud Run v2 API.

    overrides should be the *inner* object that goes under "overrides", for example:
    {
      "containerOverrides": [{
        "env": [{"name":"RUN_LABEL","value":"manual"}]
      }]
    }
    """
    project = _project_id()
    region = _region()

    if not project:
        return {"ok": False, "error": "Missing project id env var (GOOGLE_CLOUD_PROJECT or GCP_PROJECT)."}
    if not region:
        return {"ok": False, "error": "Missing GCP_REGION env var (set to your Cloud Run region, e.g. us-central1)."}
    if not job_name:
        return {"ok": False, "error": "Missing job_name."}

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job_name}:run"

    payload = {}
    if overrides:
        payload = {"overrides": overrides}   # <-- THIS is the important fix

    resp = session.post(url, json=payload, timeout=60)

    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {"ok": False, "status": resp.status_code, "error": data}

    return {"ok": True, "status": resp.status_code, "data": data}
