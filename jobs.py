# jobs.py
import os
from typing import Dict, Any, Optional

import google.auth
from google.auth.transport.requests import AuthorizedSession


def _project_id() -> str:
    return (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or ""
    ).strip()


def _region() -> str:
    return os.getenv("GCP_REGION", "us-central1").strip()


def trigger_job(
    job_name: str,
    env_overrides: Optional[Dict[str, str]] = None,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution.
    """

    project = project_id or _project_id()
    region = region or _region()

    if not project:
        return {"ok": False, "error": "Missing project id"}
    if not region:
        return {"ok": False, "error": "Missing region"}
    if not job_name:
        return {"ok": False, "error": "Missing job name"}

    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(creds)

    url = (
        f"https://run.googleapis.com/v2/projects/{project}"
        f"/locations/{region}/jobs/{job_name}:run"
    )

    payload = {}
    if env_overrides:
        payload = {
            "overrides": {
                "containerOverrides": [
                    {
                        "env": [
                            {"name": k, "value": str(v)}
                            for k, v in env_overrides.items()
                        ]
                    }
                ]
            }
        }

    resp = session.post(url, json=payload, timeout=60)

    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {"ok": False, "status": resp.status_code, "error": data}

    return {"ok": True, "status": resp.status_code, "data": data}
