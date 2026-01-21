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
    return (os.getenv("GCP_REGION") or os.getenv("CLOUD_RUN_REGION") or "").strip()


def trigger_job(
    job_name: str,
    overrides: Optional[Dict[str, Any]] = None,
    env_overrides: Optional[Dict[str, str]] = None,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution via Cloud Run v2 API.

    Supported calling styles:
    - trigger_job("job-maps", overrides={...})
    - trigger_job("job-maps", env_overrides={"A":"1","B":"2"})
    - trigger_job("job-maps", env_overrides=..., project_id=..., region=...)

    overrides format (Cloud Run v2):
    {
      "containerOverrides": [{
        "env": [{"name":"RUN_LABEL","value":"manual"}]
      }]
    }
    """
    project = (project_id or _project_id()).strip()
    reg = (region or _region()).strip()

    if not project:
        return {"ok": False, "error": "Missing project id env var (GOOGLE_CLOUD_PROJECT or GCP_PROJECT)."}
    if not reg:
        return {"ok": False, "error": "Missing GCP_REGION env var (e.g. us-central1)."}
    if not job_name:
        return {"ok": False, "error": "Missing job_name."}

    # If caller passed env_overrides, convert to Cloud Run v2 overrides shape
    final_overrides = overrides
    if final_overrides is None and env_overrides:
        final_overrides = {
            "containerOverrides": [
                {
                    "env": [{"name": k, "value": str(v)} for k, v in env_overrides.items()]
                }
            ]
        }

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{reg}/jobs/{job_name}:run"

    payload: Dict[str, Any] = {}
    if final_overrides:
        payload["overrides"] = final_overrides

    resp = session.post(url, json=payload, timeout=60)

    try:
        data = resp.json()
    except Exception:
        data = {"text": resp.text}

    if resp.status_code >= 300:
        return {"ok": False, "status": resp.status_code, "error": data}

    return {"ok": True, "status": resp.status_code, "data": data}
