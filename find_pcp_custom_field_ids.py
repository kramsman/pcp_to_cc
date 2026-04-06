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
from google.cloud import secretmanager

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
        resp = _client.access_secret_version(request={"name": name})
        _cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _cache[secret_id]


def main():
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    url = f"{PCP_API_BASE}/field_definitions"
    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)

    print(f"PCP_APP_ID  : len={len(app_id)}  first4={app_id[:4]!r}")
    print(f"PCP_SECRET  : len={len(secret)}  first4={secret[:4]!r}\n")
    print(f"Fetching field definitions from {url} ...\n")

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
        print("No field definitions found.")
        return

    print(f"{'ID':<12}  {'Name':<40}  Field Type")
    print("-" * 70)
    for f in fields:
        fid = f.get("id", "")
        attrs = f.get("attributes", {})
        name = attrs.get("name", "")
        ftype = attrs.get("field_type", "")
        print(f"{fid:<12}  {name:<40}  {ftype}")

    print(f"\nTotal: {len(fields)} field definitions")
    print("\nNext step: copy the ID for your newsletter opt-in field and set it as:")
    print("  PCP_NEWSLETTER_TRIGGER_FIELD_ID=<id>  in .env and in set-env-vars.sh")


if __name__ == "__main__":
    main()
