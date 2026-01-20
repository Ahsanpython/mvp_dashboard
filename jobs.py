# jobs.py
import json
import urllib.request
import urllib.error
from typing import Dict, Any


def _get_project_id() -> str:
    return (
        (os.getenv("GCP_PROJECT") or "")
        or (os.getenv("GOOGLE_CLOUD_PROJECT") or "")
        or (os.getenv("PROJECT_ID") or "")
    ).strip()


def _get_region() -> str:
    return (os.getenv("GCP_REGION") or "").strip()


# NOTE: we use urllib so we don't depend on google-auth packages in runtime.
import os


def _get_access_token() -> str:
    # Cloud Run Service/Job has metadata server available
    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read().decode("utf-8"))
        return data["access_token"]


def trigger_job(job_name: str, env_overrides: Dict[str, Any], project_id: str = "", region: str = "") -> Dict[str, Any]:
    """
    Triggers a Cloud Run Job execution (Cloud Run v2 API).

    job_name: e.g. "job-maps"
    env_overrides: dict of env vars to inject into execution container
    project_id/region: if not passed, taken from env vars in the Service
    """
    project = (project_id or _get_project_id()).strip()
    reg = (region or _get_region()).strip()

    if not project:
        return {"ok": False, "error": "Missing project id. Set GCP_PROJECT or GOOGLE_CLOUD_PROJECT on the Streamlit Service."}
    if not reg:
        return {"ok": False, "error": "Missing region. Set GCP_REGION on the Streamlit Service (example: us-central1)."}
    if not job_name:
        return {"ok": False, "error": "Missing job_name."}

    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{reg}/jobs/{job_name}:run"

    # Cloud Run v2 expects:
    # { "overrides": { "containerOverrides": [ { "env": [ {name,value} ] } ] } }
    env_list = [{"name": str(k), "value": str(v)} for k, v in (env_overrides or {}).items()]
    payload = {"overrides": {"containerOverrides": [{"env": env_list}]}}
    body = json.dumps(payload).encode("utf-8")

    token = _get_access_token()

    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            txt = r.read().decode("utf-8")
            return {"ok": True, "status": r.status, "data": json.loads(txt) if txt else {}}
    except urllib.error.HTTPError as e:
        err_txt = e.read().decode("utf-8", errors="replace")
        return {"ok": False, "status": e.code, "error": err_txt}
    except Exception as e:
        return {"ok": False, "error": str(e)}
