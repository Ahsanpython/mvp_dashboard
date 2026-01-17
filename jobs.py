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
    )


def _region() -> str:
    # Must be set in Cloud Run Service env vars, e.g. us-central1
    return os.getenv("GCP_REGION", "").strip()


def trigger_job(
    job_name: str,
    overrides: Optional[Dict[str, Any]] = None,
    env_overrides: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution.

    Supports being called from app.py using:
      trigger_job(job_name, env_overrides=..., project_id=..., region=...)

    Env vars required on Cloud Run Service:
      - GOOGLE_CLOUD_PROJECT or GCP_PROJECT
      - GCP_REGION
    """

    project = project_id or _project_id()
    reg = region or _region()

    if not project:
        return {"ok": False, "error": "Missing project id (GOOGLE_CLOUD_PROJECT / GCP_PROJECT)."}
    if not reg:
        return {"ok": False, "error": "Missing GCP_REGION (e.g. us-central1)."}
    if not job_name:
        return {"ok": False, "error": "Missing job_name."}

    # Normalize overrides
    payload = env_overrides or overrides or {}

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(creds)

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{reg}/jobs/{job_name}:run"

    resp = session.post(url, json=payload, timeout=60)

    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {
            "ok": False,
            "status": resp.status_code,
            "error": data,
        }

    return {
        "ok": True,
        "status": resp.status_code,
        "data": data,
    }
