# jobs.py
from __future__ import annotations

import os
from typing import Dict, Any, Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession


def trigger_job(
    job_name: str,
    env_overrides: Optional[Dict[str, str]] = None,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compatible with your existing app.py call signature.

    Uses Cloud Run Jobs v2 API:
    POST https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job}:run

    Env overrides must be sent under:
    { "overrides": { "containerOverrides": [ { "env": [ {"name":"X","value":"Y"} ] } ] } }
    """

    project = (project_id or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "").strip()
    reg = (region or os.getenv("GCP_REGION") or "").strip()

    if not project:
        return {"ok": False, "status": 400, "error": "Missing project id (set GCP_PROJECT or GOOGLE_CLOUD_PROJECT)."}
    if not reg:
        return {"ok": False, "status": 400, "error": "Missing region (set GCP_REGION, e.g. us-central1)."}
    if not job_name:
        return {"ok": False, "status": 400, "error": "Missing job_name."}

    overrides: Optional[Dict[str, Any]] = None
    if env_overrides:
        env_list = [{"name": str(k), "value": str(v)} for k, v in env_overrides.items()]
        overrides = {"containerOverrides": [{"env": env_list}]}

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{reg}/jobs/{job_name}:run"
    payload: Dict[str, Any] = {}
    if overrides:
        payload["overrides"] = overrides

    resp = session.post(url, json=payload, timeout=60)

    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {"ok": False, "status": resp.status_code, "error": data}

    return {"ok": True, "status": resp.status_code, "data": data}
