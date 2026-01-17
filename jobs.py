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
    # Set this in Cloud Run Service env vars: GCP_REGION=us-central1
    return os.getenv("GCP_REGION", "").strip()


def trigger_job(job_name: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution.
    Requires:
      - Cloud Run Job already created with name job_name
      - Cloud Run Service account has permission: roles/run.developer (or run.admin)
      - Env var GCP_REGION set (example: us-central1)

    overrides example:
      {"containerOverrides":[{"env":[{"name":"RUN_LABEL","value":"manual"}]}]}
    """
    project = _project_id()
    region = _region()

    if not project:
        return {"ok": False, "error": "Missing project id env var (GOOGLE_CLOUD_PROJECT)."}
    if not region:
        return {"ok": False, "error": "Missing GCP_REGION env var (set it to your Cloud Run region, e.g. us-central1)."}
    if not job_name:
        return {"ok": False, "error": "Missing job_name."}

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job_name}:run"
    payload = overrides or {}

    resp = session.post(url, json=payload, timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {"ok": False, "status": resp.status_code, "error": data}

    return {"ok": True, "status": resp.status_code, "data": data}
