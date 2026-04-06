"""
Helper script: list all contact lists in Constant Contact.

Use this to find the UUID for any CC list (e.g. newsletter list).
No deployed service needed — runs locally using your CC credentials
from GCP Secret Manager.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. CC_ACCESS_TOKEN is stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python find_cc_list_ids.py

Then copy the UUID for your list and set it as
CC_NEWSLETTER_LIST_ID in .env and set-env-vars.sh.
"""

import os
import sys

import requests
from dotenv import load_dotenv
from google.cloud import secretmanager

load_dotenv()

CC_API_BASE = "https://api.cc.email/v3"
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
            resp = _client.access_secret_version(request={"name": name}, timeout=10)
        except Exception as e:
            print(f"ERROR: Could not fetch secret '{secret_id}' from Secret Manager: {e}")
            print("Fix: run  gcloud auth application-default login --account=office2@4thu.org")
            sys.exit(1)
        _cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _cache[secret_id]


def _refresh_cc_token() -> str | None:
    """Exchange CC_REFRESH_TOKEN for a new access token and update Secret Manager."""
    print("CC token expired — attempting refresh...")
    try:
        try:
            cc_api_secret = _get_secret("CC_API_SECRET")
        except SystemExit:
            cc_api_secret = ""
        resp = requests.post(
            "https://authz.constantcontact.com/oauth2/default/v1/token",
            auth=(_get_secret("CC_API_KEY"), cc_api_secret),
            data={"grant_type": "refresh_token", "refresh_token": _get_secret("CC_REFRESH_TOKEN")},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        new_token = resp.json().get("access_token", "")
        if not new_token:
            print("ERROR: Token refresh response missing access_token")
            return None
        # Write new token back to Secret Manager
        parent = f"projects/{_project_id}/secrets/CC_ACCESS_TOKEN"
        payload = secretmanager.SecretPayload(data=new_token.encode("UTF-8"))
        _client.add_secret_version(request={"parent": parent, "payload": payload})
        _cache["CC_ACCESS_TOKEN"] = new_token
        print("Token refreshed and saved to Secret Manager.\n")
        return new_token
    except requests.RequestException as e:
        print(f"ERROR: Token refresh failed: {e}")
        return None


def main():
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    access_token = _get_secret("CC_ACCESS_TOKEN")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    print(f"Fetching contact lists from {CC_API_BASE}/contact_lists ...\n")

    lists = []
    next_url = f"{CC_API_BASE}/contact_lists"
    refreshed = False
    while next_url:
        try:
            resp = requests.get(next_url, headers=headers, timeout=10)
            if resp.status_code == 401 and not refreshed:
                new_token = _refresh_cc_token()
                if not new_token:
                    print("ERROR: Could not refresh CC token. Re-run the OAuth flow (see setup guide).")
                    sys.exit(1)
                headers["Authorization"] = f"Bearer {new_token}"
                refreshed = True
                continue
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"ERROR: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"Response body: {e.response.text}")
            sys.exit(1)
        data = resp.json()
        lists.extend(data.get("lists", []))
        next_url = data.get("_links", {}).get("next", {}).get("href")

    if not lists:
        print("No contact lists found.")
        return

    print(f"{'UUID':<40}  {'Name':<40}  {'Status':<8}  Members")
    print("-" * 100)
    for lst in lists:
        uuid        = lst.get("list_id", "")
        name        = lst.get("name", "")
        status      = lst.get("status", "")
        member_count = lst.get("membership_count", "")
        print(f"{uuid:<40}  {name:<40}  {status:<8}  {member_count}")

    print(f"\nTotal: {len(lists)} contact lists")
    print("\nNext step: copy the UUID for your list and set it as:")
    print("  CC_NEWSLETTER_LIST_ID=<uuid>  in .env and in set-env-vars.sh")


if __name__ == "__main__":
    main()
