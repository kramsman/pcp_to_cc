"""
pcp_workflow_report.py

Report all active workflow cards across all PCP workflows.
Writes three tabs to a Google Sheet (overdue / snoozed / active)
and two local debug XLSX files.
"""
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # suppress noisy gRPC logs from Secret Manager

import pandas as pd
import requests
from dotenv import load_dotenv
from google.api_core import retry as api_retry
from google.cloud import secretmanager
from bekgoogle import ensure_adc_auth
from bekgoogle.create_google_services import create_google_services
from uvbekutils.pyautobek import confirm_with_file_link

load_dotenv()

# --- Constants ---
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Hy0Qr6FDeZlmrnsWOTdrzlVMkw_zn0dCqWeyDoouGt8/edit"
TAB_OVERDUE = "overdue"
TAB_SNOOZED = "snoozed"
TAB_ACTIVE = "active"
TAB_INTERESTED = "Interested People"
INTERESTED_WORKFLOWS = {"Membership In Process", "Should Person go to Membership in Process"}
TIMESTAMP_CELL = "A3"
DATA_START_CELL = "A5"
CLEAR_RANGE_END = "Z10000"
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_CREDS_DIR = str(Path(__file__).parent / ".google_creds")

WORKFLOW_PRIORITY = {
    "Membership In Process": 1,
    "Should Person go to Membership in Process": 2,
    "Membership Ceremony": 3,
    "Get Contact Information": 4,
    "Explorer": 5,
    "Visitor": 6,
}
DEFAULT_WORKFLOW_PRIORITY = 999
FILTER_TEST_PROFILES = True

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"
_project_id = os.environ.get("CLOUD_PROJECT_ID", "")
_sm_client = None
_secret_cache: dict[str, str] = {}


def _get_secret(secret_id: str) -> str:
    """Fetch a secret value from Google Secret Manager, with in-process caching.

    Lazily initializes the Secret Manager client on first call. If the ADC
    token is expired, triggers re-authentication and retries once.

    Args:
        secret_id: The Secret Manager secret resource name (without version suffix).

    Returns:
        The decoded UTF-8 string value of the secret's latest version.

    Raises:
        google.api_core.exceptions.GoogleAPICallError: On unrecoverable API errors.
    """
    global _sm_client
    if secret_id not in _secret_cache:
        if _sm_client is None:
            _sm_client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{_project_id}/secrets/{secret_id}/versions/latest"
        try:
            resp = _sm_client.access_secret_version(
                request={"name": name},
                retry=api_retry.Retry(deadline=5.0),
            )
        except Exception as e:
            if "Reauthentication is needed" in str(e):
                # ADC token expired; refresh credentials and rebuild the client before retrying
                ensure_adc_auth()
                _sm_client = secretmanager.SecretManagerServiceClient()
                resp = _sm_client.access_secret_version(request={"name": name})
            else:
                raise
        _secret_cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _secret_cache[secret_id]


def _fetch_all_with_included(url: str, auth: tuple, params: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Fetch all paginated cards + included sideloads from a PCP endpoint.

    Follows JSON:API ``links.next`` pagination until exhausted, accumulating
    primary data and any sideloaded ``included`` records.

    Args:
        url: The initial endpoint URL.
        auth: ``(app_id, secret)`` tuple for HTTP Basic authentication.
        params: Query parameters sent only on the first request; subsequent
            pages use the URL already encoded in ``links.next``.

    Returns:
        A tuple of (data, included) where each is a list of JSON:API resource dicts.
    """
    data: list[dict] = []
    included: list[dict] = []
    next_url: str | None = url
    while next_url:
        resp = requests.get(
            next_url,
            auth=auth,
            params=params if next_url == url else None,  # params already embedded in subsequent next URLs
            timeout=15,
            headers={"User-Agent": "pcp_to_cc (office2@4thu.org)"},
        )
        resp.raise_for_status()
        body = resp.json()
        data.extend(body.get("data", []))
        included.extend(body.get("included", []))
        next_url = body.get("links", {}).get("next")  # JSON:API: None when last page reached
    return data, included


def _fetch_all(url: str, auth: tuple) -> list[dict]:
    """Fetch all pages from a PCP JSON:API endpoint, returning only primary data.

    Simpler variant of _fetch_all_with_included for endpoints that don't
    need sideloaded records.

    Args:
        url: The initial endpoint URL.
        auth: ``(app_id, secret)`` tuple for HTTP Basic authentication.

    Returns:
        Combined list of JSON:API resource dicts across all pages.
    """
    items: list[dict] = []
    next_url: str | None = url
    while next_url:
        resp = requests.get(
            next_url, auth=auth, timeout=15,
            headers={"User-Agent": "pcp_to_cc (office2@4thu.org)"},
        )
        resp.raise_for_status()
        body = resp.json()
        items.extend(body.get("data", []))
        next_url = body.get("links", {}).get("next")  # JSON:API: None when last page reached
    return items


def _fmt_date(val: str | None) -> str:
    """Truncate an ISO 8601 timestamp string to YYYY-MM-DD.

    Args:
        val: An ISO 8601 datetime string, or None.

    Returns:
        The first 10 characters (YYYY-MM-DD), or an empty string if val is falsy.
    """
    if not val:
        return ""
    return val[:10]  # slice YYYY-MM-DD; ISO timestamps include time and timezone beyond char 10


def _extract_sheet_id(url: str) -> str:
    """Parse the Google Sheets spreadsheet ID from a full sheet URL.

    Args:
        url: A Google Sheets URL containing ``/spreadsheets/d/<id>/``.

    Returns:
        The spreadsheet ID string.

    Raises:
        ValueError: If the URL does not contain a recognizable spreadsheet ID.
    """
    m = re.search(r"/spreadsheets/d/([\w-]+)", url)
    if not m:
        raise ValueError(f"Cannot extract sheet ID from URL: {url}")
    return m.group(1)


def _df_to_values(df: pd.DataFrame, headers: list[str]) -> list[list]:
    """Convert a DataFrame to a list-of-lists suitable for the Sheets values API.

    Prepends a header row. If the DataFrame is empty, returns a two-row result
    with headers and a single ``'None'`` placeholder row so the tab is not blank.

    Args:
        df: Source DataFrame; must contain all columns named in headers.
        headers: Ordered list of column names to include.

    Returns:
        List of rows (each a list of strings), with headers as the first row.
    """
    if df.empty:
        return [headers, ["None"] + [""] * (len(headers) - 1)]
    rows: list[list] = [headers]
    for _, row in df.iterrows():
        rows.append([str(row[h]) if row[h] is not None else "" for h in headers])
    return rows


def _get_tab_ids(sheet_service: Any, sheet_id: str, tab_names: list[str]) -> dict[str, int]:
    """Return {tab_name: numeric_sheet_id}, creating any missing tabs.

    Matches existing tabs case-insensitively (Google enforces case-insensitive uniqueness).

    Args:
        sheet_service: Authenticated Google Sheets API service resource.
        sheet_id: The spreadsheet ID string.
        tab_names: Exact tab names desired; missing tabs are created via batchUpdate.

    Returns:
        Mapping from each requested tab name to its numeric sheet ID.
    """
    meta = sheet_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    norm_to_id = {
        s["properties"]["title"].strip().lower(): s["properties"]["sheetId"]
        for s in meta["sheets"]
    }
    result: dict[str, int] = {}
    missing: list[str] = []
    for n in tab_names:
        key = n.strip().lower()
        if key in norm_to_id:
            result[n] = norm_to_id[key]
        else:
            missing.append(n)
    if missing:
        add_requests = [{"addSheet": {"properties": {"title": n}}} for n in missing]
        resp = sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id, body={"requests": add_requests}
        ).execute()
        for n, reply in zip(missing, resp.get("replies", [])):
            result[n] = reply["addSheet"]["properties"]["sheetId"]
        print(f"  Created tabs: {missing}")
    return result


def _write_tab(
    sheet_service: Any,
    sheet_id: str,
    tab_name: str,
    tab_numeric_id: int,
    df: pd.DataFrame,
    headers: list[str],
    timestamp_str: str,
) -> None:
    """Clear and rewrite one tab in the Google Sheet with fresh data.

    Clears rows from DATA_START_CELL downward, writes the run timestamp at
    TIMESTAMP_CELL, writes data starting at DATA_START_CELL, then freezes
    the top 5 rows and auto-resizes all data columns.

    Args:
        sheet_service: Authenticated Google Sheets API service resource.
        sheet_id: The spreadsheet ID string.
        tab_name: Display name of the tab to write.
        tab_numeric_id: Numeric sheet ID used for batchUpdate formatting requests.
        df: DataFrame containing the rows to write.
        headers: Ordered column names that determine the write order and header row.
        timestamp_str: Human-readable timestamp string written to TIMESTAMP_CELL.
    """
    ss = sheet_service.spreadsheets()
    ss.values().clear(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!{DATA_START_CELL}:{CLEAR_RANGE_END}",
    ).execute()
    ss.values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!{TIMESTAMP_CELL}",
        valueInputOption="RAW",
        body={"values": [[timestamp_str]]},
    ).execute()
    ss.values().update(
        spreadsheetId=sheet_id,
        range=f"'{tab_name}'!{DATA_START_CELL}",
        valueInputOption="RAW",
        body={"values": _df_to_values(df, headers)},
    ).execute()
    ss.batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": tab_numeric_id,
                        "gridProperties": {"frozenRowCount": 5},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": tab_numeric_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(headers),
                    }
                }
            },
        ]},
    ).execute()
    print(f"  {tab_name}: {len(df)} rows")


def main() -> None:
    """Entry point: fetch all active PCP workflow cards and publish to Google Sheets.

    Retrieves credentials from Secret Manager, pages through every workflow
    and its cards, filters out completed/removed/test records, partitions the
    results into overdue/snoozed/active/interested subsets, writes debug XLSX
    files locally, and publishes all four subsets to the configured Google Sheet.
    """
    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env")
        sys.exit(1)

    print("Fetching PCP credentials...")
    app_id = _get_secret("PCP_APP_ID")
    secret = _get_secret("PCP_SECRET")
    auth = (app_id, secret)

    print("Fetching workflows...")
    workflows = _fetch_all(f"{PCP_API_BASE}/workflows", auth)
    print(f"  {len(workflows)} workflows")

    rows: list[dict] = []
    for wf in workflows:
        wf_name = wf["attributes"].get("name", "")
        wf_id = wf["id"]
        print(f"  Cards: {wf_name}")

        cards, included = _fetch_all_with_included(
            f"{PCP_API_BASE}/workflows/{wf_id}/cards",
            auth,
            params={"include": "person,assignee,current_step", "per_page": 100},
        )

        # build an O(1) lookup index keyed by (JSON:API type, id) for sideloaded records
        included_idx: dict[tuple, dict] = {
            (item["type"], item["id"]): item.get("attributes", {})
            for item in included
        }

        for card in cards:
            attrs = card.get("attributes", {})
            rels = card.get("relationships", {})

            stage = (attrs.get("stage") or "").lower()
            if stage in ("completed", "removed"):
                continue

            person_rel = (rels.get("person") or {}).get("data") or {}
            person_id = person_rel.get("id")
            person_attrs = included_idx.get(("Person", person_id), {}) if person_id else {}
            first = person_attrs.get("first_name", "")
            last = person_attrs.get("last_name", "")
            if FILTER_TEST_PROFILES and last.lower() == "test":
                continue
            person_name = f"{first} {last}".strip()

            assignee_rel = (rels.get("assignee") or {}).get("data") or {}
            assignee_id = assignee_rel.get("id")
            assignee_attrs = included_idx.get(("Person", assignee_id), {}) if assignee_id else {}
            af = assignee_attrs.get("first_name", "")
            al = assignee_attrs.get("last_name", "")
            assignee_name = f"{af} {al}".strip() if assignee_id else ""

            step_rel = (rels.get("current_step") or {}).get("data") or {}
            step_id = step_rel.get("id")
            step_attrs = included_idx.get(("WorkflowStep", step_id), {}) if step_id else {}
            step_name = step_attrs.get("name", "")

            snooze_until_raw = attrs.get("snooze_until")
            rows.append({
                "workflow_name": wf_name,
                "step_name": step_name,
                "person_name": person_name,
                "assignee_name": assignee_name,
                "overdue": bool(attrs.get("overdue", False)),
                "snoozed": bool(snooze_until_raw),
                "snooze_until": _fmt_date(snooze_until_raw),
                "person_created_at": _fmt_date(person_attrs.get("created_at")),
                "person_updated_at": _fmt_date(person_attrs.get("updated_at")),
                "_wf_priority": WORKFLOW_PRIORITY.get(wf_name, DEFAULT_WORKFLOW_PRIORITY),
            })

    print(f"\n{len(rows)} total cards")
    if not rows:
        print("No workflow cards found.")
        return

    # ascending snoozed: False sorts before True, so non-snoozed cards appear first
    df = pd.DataFrame(rows).sort_values(
        ["snoozed", "_wf_priority", "workflow_name", "step_name", "person_name"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)

    output_cols = [
        "snoozed", "snooze_until", "overdue",
        "person_name", "workflow_name", "step_name", "assignee_name",
        "person_created_at", "person_updated_at",
    ]
    trimmed = df[output_cols].copy()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_dir = Path(__file__).parent
    df.drop(columns=["_wf_priority"]).to_excel(
        script_dir / f"pcp_workflow_report_full.xlsx", engine="openpyxl", index=False
    )
    trimmed.to_excel(
        script_dir / f"pcp_workflow_report.xlsx", engine="openpyxl", index=False
    )
    print(f"Debug XLSX written ({ts})")

    overdue_mask = trimmed["overdue"] == True
    snoozed_mask = trimmed["snoozed"] == True
    overdue_df = trimmed[overdue_mask]
    snoozed_df = trimmed[~overdue_mask & snoozed_mask]  # snoozed but not overdue
    active_df  = trimmed[~overdue_mask & ~snoozed_mask]  # neither overdue nor snoozed
    interested_df = (
        trimmed[trimmed["workflow_name"].isin(INTERESTED_WORKFLOWS)]
        .sort_values(["snoozed", "overdue", "workflow_name"], ascending=[True, False, True])
        .copy()
    )

    timestamp_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    sheet_id = _extract_sheet_id(GOOGLE_SHEET_URL)

    print("\nWriting to Google Sheet...")
    _, sheet_service = create_google_services(SHEETS_SCOPES, cred_dir=GOOGLE_CREDS_DIR)
    tab_ids = _get_tab_ids(sheet_service, sheet_id, [TAB_INTERESTED, TAB_OVERDUE, TAB_SNOOZED, TAB_ACTIVE])
    _write_tab(sheet_service, sheet_id, TAB_INTERESTED, tab_ids[TAB_INTERESTED], interested_df, output_cols, timestamp_str)
    _write_tab(sheet_service, sheet_id, TAB_OVERDUE,    tab_ids[TAB_OVERDUE],    overdue_df,    output_cols, timestamp_str)
    _write_tab(sheet_service, sheet_id, TAB_SNOOZED,    tab_ids[TAB_SNOOZED],    snoozed_df,    output_cols, timestamp_str)
    _write_tab(sheet_service, sheet_id, TAB_ACTIVE,     tab_ids[TAB_ACTIVE],     active_df,     output_cols, timestamp_str)

    print(f"\nDone. {GOOGLE_SHEET_URL}")

    confirm_with_file_link(
        f"Workflow cards report updated.\n\n"
        f"  overdue:    {len(overdue_df)}\n"
        f"  snoozed:    {len(snoozed_df)}\n"
        f"  active:     {len(active_df)}\n"
        f"  interested: {len(interested_df)}\n\n"
        f"Click the link below to open the Google Sheet.",
        GOOGLE_SHEET_URL,
        title="Workflow Cards Report",
        buttons=["OK"],
        close_on_link_click=True,
    )


if __name__ == "__main__":
    main()
