"""
Report: every active workflow card across all PCP workflows.

For each card: workflow name, step name, person, assignee, snoozed status,
overdue, last update, and a few useful timestamps. Output is a CSV that
can be opened in Excel/Numbers or further filtered with pandas.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python pcp_workflow_report.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")

import pandas as pd  # noqa: E402
import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.api_core import retry as api_retry  # noqa: E402
from google.cloud import secretmanager  # noqa: E402
from bekgoogle import ensure_adc_auth  # noqa: E402

load_dotenv()

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"
USER_AGENT = "pcp_to_cc (office2@4thu.org)"
PER_PAGE = 100

# Edit this to control workflow sort order. Lower number = higher priority.
# Workflows not listed get DEFAULT_WORKFLOW_PRIORITY and sort alphabetically after the listed ones.
WORKFLOW_PRIORITY: dict[str, int] = {
    "Membership In Process": 1,
    "Should Person go to Membership in Process": 2,
    "Membership Ceremony": 3,
    "Explorer": 4,
    "Visitor": 5,
}
DEFAULT_WORKFLOW_PRIORITY = 999

# When True, drop rows whose person last name is "test" (case-insensitive).
FILTER_TEST_PROFILES = True

_project_id = os.environ.get("CLOUD_PROJECT_ID", "")
_secret_client = None
_secret_cache: dict[str, str] = {}


def _get_secret(secret_id: str) -> str:
    global _secret_client
    if secret_id in _secret_cache:
        return _secret_cache[secret_id]
    if _secret_client is None:
        _secret_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{_project_id}/secrets/{secret_id}/versions/latest"
    try:
        resp = _secret_client.access_secret_version(
            request={"name": name},
            retry=api_retry.Retry(deadline=5.0),
        )
    except Exception as e:
        if "Reauthentication is needed" in str(e):
            ensure_adc_auth()
            _secret_client = secretmanager.SecretManagerServiceClient()
            resp = _secret_client.access_secret_version(request={"name": name})
        else:
            raise
    _secret_cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _secret_cache[secret_id]


def _get(url: str, auth: tuple, params: dict | None = None) -> dict:
    resp = requests.get(
        url, auth=auth, params=params, timeout=30,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_pages(start_url: str, auth: tuple, params: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Walk all pages of a JSON:API endpoint. Return (data, included)."""
    data: list[dict] = []
    included: list[dict] = []
    next_url: str | None = start_url
    next_params = params
    while next_url:
        body = _get(next_url, auth, next_params)
        data.extend(body.get("data", []) or [])
        included.extend(body.get("included", []) or [])
        next_url = (body.get("links") or {}).get("next")
        next_params = None  # next URL already has all params encoded
    return data, included


def _index_included(included: list[dict]) -> dict[tuple[str, str], dict]:
    """Index sideloaded resources by (type, id)."""
    return {(r.get("type", ""), r.get("id", "")): r for r in included}


def _person_name(person: dict | None) -> str:
    if not person:
        return ""
    attrs = person.get("attributes", {}) or {}
    name = attrs.get("name") or ""
    if name:
        return name
    first = attrs.get("first_name") or ""
    last = attrs.get("last_name") or ""
    return f"{first} {last}".strip()


def _person_attr(person: dict | None, key: str) -> str:
    if not person:
        return ""
    return (person.get("attributes") or {}).get(key, "") or ""


def _rel_id(card: dict, rel_name: str) -> str:
    rel = (card.get("relationships") or {}).get(rel_name) or {}
    data = rel.get("data") or {}
    return data.get("id", "") if isinstance(data, dict) else ""


def _to_date(value: str) -> str:
    """Convert ISO timestamp like '2026-04-02T00:28:40Z' to '2026-04-02'."""
    if not value:
        return ""
    return str(value)[:10]


def fetch_workflows(auth: tuple) -> list[dict]:
    print("Fetching workflows...")
    data, _ = _fetch_pages(f"{PCP_API_BASE}/workflows", auth, {"per_page": PER_PAGE})
    print(f"  {len(data)} workflows")
    return data


def fetch_active_cards(workflow_id: str, auth: tuple) -> tuple[list[dict], dict[tuple[str, str], dict]]:
    """Fetch all cards for a workflow with person, assignee, and current_step sideloaded."""
    params = {
        "include": "person,assignee,current_step",
        "per_page": PER_PAGE,
    }
    data, included = _fetch_pages(
        f"{PCP_API_BASE}/workflows/{workflow_id}/cards", auth, params
    )
    return data, _index_included(included)


def build_rows(workflows: list[dict], auth: tuple) -> list[dict]:
    rows: list[dict] = []
    for wf in workflows:
        wf_id = wf.get("id", "")
        wf_name = (wf.get("attributes") or {}).get("name", "") or ""
        cards, included_idx = fetch_active_cards(wf_id, auth)
        active = 0
        for card in cards:
            attrs = card.get("attributes") or {}
            # Defensive: skip removed/completed even though endpoint should exclude them
            if attrs.get("removed_at") or attrs.get("completed_at"):
                continue

            person_id = _rel_id(card, "person")
            assignee_id = _rel_id(card, "assignee")
            step_id = _rel_id(card, "current_step")
            person = included_idx.get(("Person", person_id)) if person_id else None
            assignee = included_idx.get(("Person", assignee_id)) if assignee_id else None
            step = included_idx.get(("WorkflowStep", step_id)) if step_id else None
            step_name = (step.get("attributes", {}).get("name", "") if step else "") or ""

            if FILTER_TEST_PROFILES and _person_attr(person, "last_name").strip().lower() == "test":
                continue
            active += 1

            snooze_until = attrs.get("snooze_until")
            rows.append({
                "workflow_name": wf_name,
                "step_name": step_name,
                "person_name": _person_name(person),
                "person_id": person_id,
                "person_status": _person_attr(person, "status"),
                "person_membership": _person_attr(person, "membership"),
                "person_child": _person_attr(person, "child"),
                "person_birthdate": _to_date(_person_attr(person, "birthdate")),
                "person_created_at": _to_date(_person_attr(person, "created_at")),
                "person_updated_at": _to_date(_person_attr(person, "updated_at")),
                "assignee_name": _person_name(assignee),
                "assignee_id": assignee_id,
                "snoozed": bool(snooze_until),
                "snooze_until": _to_date(snooze_until or ""),
                "overdue": bool(attrs.get("overdue")),
                "stage_step_id": step_id,
                "last_updated": _to_date(attrs.get("updated_at", "") or ""),
                "created_at": _to_date(attrs.get("created_at", "") or ""),
                "moved_to_step_at": _to_date(attrs.get("moved_to_step_at", "") or ""),
                "completed_at": _to_date(attrs.get("completed_at", "") or ""),
                "removed_at": _to_date(attrs.get("removed_at", "") or ""),
                "card_id": card.get("id", ""),
            })
        print(f"  [{wf_name}]: {active} active cards")
    return rows


def main() -> None:
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)

    workflows = fetch_workflows(auth)
    rows = build_rows(workflows, auth)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["_wf_priority"] = df["workflow_name"].map(
            lambda n: WORKFLOW_PRIORITY.get(n, DEFAULT_WORKFLOW_PRIORITY)
        )
        df = df.sort_values(
            by=["snoozed", "_wf_priority", "workflow_name", "step_name", "person_name"],
            ascending=[True, True, True, True, True],
            kind="stable",
        ).reset_index(drop=True)
        df = df.drop(columns=["_wf_priority"])

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_path = Path(__file__).with_name(f"pcp_workflow_report_{timestamp}_full.csv")
    out_path = Path(__file__).with_name(f"pcp_workflow_report_{timestamp}.csv")

    df.to_csv(full_path, index=False)

    output_cols = [
        "snoozed", "snooze_until", "overdue",
        "person_name", "workflow_name", "step_name", "assignee_name",
        "person_created_at", "person_updated_at",
    ]
    trimmed = df[[c for c in output_cols if c in df.columns]]
    trimmed.to_csv(out_path, index=False)

    print(f"\nWrote {len(df)} rows.")
    print(f"  Full (all fields):  {full_path}")
    print(f"  Output (trimmed):   {out_path}")


if __name__ == "__main__":
    main()
