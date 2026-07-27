"""
Helper script: list workflows, forms, and custom field definitions from Planning Center People.

Output order: Workflows → Forms → Custom Fields
Results are printed to the terminal and saved to find_pcp_ids.txt.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python find_pcp_ids.py
"""

import os
import sys

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # suppress gRPC noise before grpc loads

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.api_core import retry as api_retry  # noqa: E402
from google.cloud import secretmanager  # noqa: E402
from uvbekutils.pyautobek import confirm_with_file_link  # noqa: E402
from bekgoogle import ensure_adc_auth  # noqa: E402

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


def _fetch_all(endpoint: str, auth: tuple, lines: list[str]) -> list[dict]:
    """Fetch all pages from a PCP API endpoint, returning the combined data list."""
    items = []
    next_url = f"{PCP_API_BASE}/{endpoint}"
    while next_url:
        try:
            resp = requests.get(next_url, auth=auth, timeout=10,
                                headers={"User-Agent": "pco_webhook (office2@4thu.org)"})
            resp.raise_for_status()
        except requests.RequestException as e:
            _emit(lines, f"ERROR fetching {endpoint}: {e}")
            break
        data = resp.json()
        items.extend(data.get("data", []))
        next_url = data.get("links", {}).get("next")
    return items


def fetch_pcp_ids() -> dict:
    """Return {pcp_workflow, pcp_form, pcp_field} as lists of {id, name} dicts."""
    print("Fetching PCP credentials...")
    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)
    dummy: list[str] = []
    print("Fetching PCP workflows...")
    workflows = _fetch_all("workflows", auth, dummy)
    print(f"  {len(workflows)} workflows")
    print("Fetching PCP forms...")
    forms = _fetch_all("forms", auth, dummy)
    print(f"  {len(forms)} forms")
    print("Fetching PCP field definitions...")
    fields = _fetch_all("field_definitions", auth, dummy)
    print(f"  {len(fields)} field definitions")
    print("Fetching PCP field tabs...")
    tabs = _fetch_all("tabs", auth, dummy)
    print(f"  {len(tabs)} tabs")
    return {
        "pcp_workflow": [{"id": w["id"], "name": w["attributes"].get("name", "")} for w in workflows],
        "pcp_form":     [{"id": f["id"], "name": f["attributes"].get("name", "")} for f in forms],
        # data_type and tab_id let the editor offer only fields that can legally
        # fill a given slot — e.g. a durable prerequisite must be a dropdown, and
        # must not live on the workflow's own items tab.
        "pcp_field":    [{"id": f["id"], "name": f["attributes"].get("name", ""),
                          "data_type": f["attributes"].get("data_type", ""),
                          "tab_id": str(f["attributes"].get("tab_id", "") or "")}
                         for f in fields if not f["attributes"].get("deleted_at")],
        "pcp_tab":      [{"id": t["id"], "name": t["attributes"].get("name", "")} for t in tabs],
    }


def main():
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth   = (app_id, secret)
    lines: list[str] = []

    # --- Workflows ---
    _emit(lines, "\n=== Workflows ===\n")
    workflows = _fetch_all("workflows", auth, lines)
    if not workflows:
        _emit(lines, "No workflows found.")
    else:
        _emit(lines, f"{'ID':<12}  {'Name':<50}  Campus")
        _emit(lines, "-" * 80)
        for w in workflows:
            attrs = w.get("attributes", {})
            campus = attrs.get("campus_name", "") or ""
            _emit(lines, f"{w['id']:<12}  {attrs.get('name',''):<50}  {campus}")
        _emit(lines, f"\nTotal: {len(workflows)} workflows")

    # --- Forms ---
    _emit(lines, "\n\n=== Forms ===\n")
    forms = _fetch_all("forms", auth, lines)
    if not forms:
        _emit(lines, "No forms found.")
    else:
        _emit(lines, f"{'ID':<12}  {'Name':<50}  Active")
        _emit(lines, "-" * 70)
        for f in forms:
            attrs = f.get("attributes", {})
            active = "yes" if attrs.get("active") else "no"
            _emit(lines, f"{f['id']:<12}  {attrs.get('name',''):<50}  {active}")
        _emit(lines, f"\nTotal: {len(forms)} forms")

    # --- Tabs ---
    # Tabs group custom fields on a person's profile, and are how anytime-item
    # workflows find their items: the config names a tab, and the dropdown fields
    # on it are the items. Nothing in PCP links a tab to a workflow — that link
    # lives only in rules.json.
    _emit(lines, "\n\n=== Field Tabs ===\n")
    tabs = _fetch_all("tabs", auth, lines)
    tab_names = {t["id"]: t.get("attributes", {}).get("name", "") for t in tabs}
    if not tabs:
        _emit(lines, "No tabs found.")
    else:
        _emit(lines, f"{'ID':<12}  {'Name':<50}  Sequence")
        _emit(lines, "-" * 76)
        for t in sorted(tabs, key=lambda x: x.get("attributes", {}).get("sequence", 0)):
            attrs = t.get("attributes", {})
            _emit(lines, f"{t['id']:<12}  {attrs.get('name',''):<50}  {attrs.get('sequence','')}")
        _emit(lines, f"\nTotal: {len(tabs)} tabs")

    # --- Custom Fields ---
    # Grouped by tab, since that is how they appear on a profile and how the
    # anytime-item gate discovers them. Only `select` fields can gate a workflow.
    _emit(lines, "\n\n=== Custom Fields (by tab) ===\n")
    fields = _fetch_all("field_definitions", auth, lines)
    if not fields:
        _emit(lines, "No field definitions found.")
    else:
        by_tab: dict[str, list[dict]] = {}
        for f in fields:
            if f.get("attributes", {}).get("deleted_at"):
                continue
            tab_id = str(f.get("attributes", {}).get("tab_id", "") or "")
            by_tab.setdefault(tab_id, []).append(f)
        shown = 0
        for tab_id, tab_fields in sorted(by_tab.items(),
                                         key=lambda kv: tab_names.get(kv[0], "zz")):
            label = tab_names.get(tab_id, "(no tab)")
            _emit(lines, f"\n-- tab {tab_id or '?'}: {label} --")
            _emit(lines, f"{'ID':<12}  {'Name':<40}  Data Type")
            _emit(lines, "-" * 70)
            for f in tab_fields:
                attrs = f.get("attributes", {})
                dtype = attrs.get("data_type", "")
                gate = "  <- can gate" if dtype == "select" else ""
                _emit(lines, f"{f['id']:<12}  {attrs.get('name',''):<40}  {dtype}{gate}")
                shown += 1
        _emit(lines, f"\nTotal: {shown} field definitions")

    out_path = os.path.splitext(os.path.abspath(__file__))[0] + ".txt"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"\nSaved to: {out_path}")

    confirm_with_file_link(
        "Workflows, forms, and field definitions written.",
        out_path,
        title="PCP IDs",
        buttons=["OK"],
    )


if __name__ == "__main__":
    main()
