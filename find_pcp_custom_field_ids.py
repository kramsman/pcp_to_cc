"""
Helper script: list all custom field definitions in Planning Center People.

Use this BEFORE deploying to find the numeric ID for any custom field
(e.g. newsletter_opt_in). No deployed service needed — runs locally
using your PCP credentials from GCP Secret Manager.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python find_pcp_custom_field_ids.py

Then copy the ID for your field and set it as
PCP_NEWSLETTER_TRIGGER_FIELD_ID in .env and set-env-vars.sh.
"""

import os
import sys

import requests
from dotenv import load_dotenv
from google.api_core import retry as api_retry
from google.cloud import secretmanager
from uvbekutils.pyautobek import confirm_with_file_link
from bekgoogle import ensure_adc_auth

load_dotenv()

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"
_project_id = os.environ.get("CLOUD_PROJECT_ID", "")
_client = None
_cache: dict[str, str] = {}


def _get_secret(secret_id: str) -> str:
    global _client
    if secret_id not in _cache:
        if _client is None:
            _client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{_project_id}/secrets/{secret_id}/versions/latest"
        try:
            resp = _client.access_secret_version(
                request={"name": name},
                retry=api_retry.Retry(deadline=5.0),
            )
        except Exception as e:
            if "Reauthentication is needed" in str(e):
                ensure_adc_auth()
                _client = secretmanager.SecretManagerServiceClient()
                resp = _client.access_secret_version(request={"name": name})
            else:
                raise
        _cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _cache[secret_id]


def _emit(lines: list[str], text: str) -> None:
    lines.append(text)
    print(text)


def main():
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    url = f"{PCP_API_BASE}/field_definitions"
    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)

    lines: list[str] = []

    _emit(lines, f"PCP_APP_ID  : len={len(app_id)}  first4={app_id[:4]!r}")
    _emit(lines, f"PCP_SECRET  : len={len(secret)}  first4={secret[:4]!r}\n")
    _emit(lines, f"Fetching field definitions from {url} ...\n")

    fields = []
    next_url = url
    while next_url:
        try:
            resp = requests.get(next_url, auth=auth, timeout=10, headers={"User-Agent": "pcp_to_cc (office2@4thu.org)"})
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"ERROR: {e}")
            sys.exit(1)
        data = resp.json()
        fields.extend(data.get("data", []))
        next_url = data.get("links", {}).get("next")

    if not fields:
        _emit(lines, "No field definitions found.")
        return

    _emit(lines, f"{'ID':<12}  {'Name':<40}  Field Type")
    _emit(lines, "-" * 70)
    for f in fields:
        fid = f.get("id", "")
        attrs = f.get("attributes", {})
        name = attrs.get("name", "")
        ftype = attrs.get("field_type", "")
        _emit(lines, f"{fid:<12}  {name:<40}  {ftype}")

    _emit(lines, f"\nTotal: {len(fields)} field definitions")
    _emit(lines, "\nNext step: copy the ID for your newsletter opt-in field and set it as:")
    _emit(lines, "  PCP_NEWSLETTER_TRIGGER_FIELD_ID=<id>  in .env and in set-env-vars.sh")

    # --- Workflows ---
    _emit(lines, "\n\n=== Workflows ===\n")
    workflows = []
    next_url = f"{PCP_API_BASE}/workflows"
    while next_url:
        try:
            resp = requests.get(next_url, auth=auth, timeout=10, headers={"User-Agent": "pcp_to_cc (office2@4thu.org)"})
            resp.raise_for_status()
        except requests.RequestException as e:
            _emit(lines, f"ERROR fetching workflows: {e}")
            break
        data = resp.json()
        workflows.extend(data.get("data", []))
        next_url = data.get("links", {}).get("next")

    if not workflows:
        _emit(lines, "No workflows found.")
    else:
        _emit(lines, f"{'ID':<12}  {'Name':<50}  Campus")
        _emit(lines, "-" * 80)
        for w in workflows:
            wid = w.get("id", "")
            attrs = w.get("attributes", {})
            name = attrs.get("name", "")
            campus = attrs.get("campus_name", "") or ""
            _emit(lines, f"{wid:<12}  {name:<50}  {campus}")
        _emit(lines, f"\nTotal: {len(workflows)} workflows")

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pcp_field_and_workflow_ids.txt")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"\nSaved to: {out_path}")

    confirm_with_file_link(
        "Field definitions and workflows written.",
        out_path,
        title="PCP IDs",
        buttons=["OK"],
    )


if __name__ == "__main__":
    main()
