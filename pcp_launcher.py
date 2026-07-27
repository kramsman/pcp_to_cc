"""
pcp_launcher.py

Launcher menu for PCP / Realm / Constant Contact utilities.

Running this script refreshes Google credentials (re-authenticating in the
browser if they have expired), then shows a dialog listing the available tools.
Pick one and the launcher runs its underlying script — most run inline, while
"Workflow Assignment" is detached into its own process so it can open its own
editor window.

Menu items:
    Transfer Data           Transfer member data between Planning Center People
                            (PCP) and Realm — reads an exported CSV, applies a
                            column-mapping spreadsheet, and writes a reformatted
                            CSV ready for import.
    Find PCP IDs            List PCP field definitions with each field's numeric
                            ID, name, and type, for locating IDs used in config.
    Find CC IDs             List Constant Contact lists with each list's UUID,
                            name, status, and member count, for locating IDs
                            used in config.
    Workflow Cards Report   Report every active workflow card across all PCP
                            workflows to a CSV (workflow, step, person,
                            assignee, snoozed, overdue, last-updated).
    Anytime Items Report    Show who still owes the anytime items on a workflow —
                            those doable in any order but required before it
                            completes — as an always-current HTML page.
    Validate Anytime Rules  Check the Anytime WF Items rules against live PCP.
                            They fail silently when wrong, so run after changing
                            a rule, a dropdown's options, or a workflow's steps.
    Workflow Assignment     Edit workflow and CC list rules via a GUI without
                            touching Python code; changes save to rules.json
                            (run deploy.sh to apply them to Cloud Run).
    Check for Updates       Check GitHub for newer versions of the uvbekutils
                            and bekgoogle libraries and reinstall if available.
"""
import os
import sys
import time
import subprocess
from pathlib import Path

_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from uvbekutils.pyautobek import confirm

ROOT_PATH = Path(__file__).parent


def _ensure_adc_auth() -> None:
    import subprocess
    import google.auth
    import google.auth.transport.requests
    try:
        creds, _ = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
    except Exception as e:
        if not any(k in str(e).lower() for k in ("reauth", "expired", "invalid_grant", "could not be found", "credentials")):
            return
        print("\nGoogle credentials have expired. A browser window will open — sign in to continue.\n")
        subprocess.run(["gcloud", "auth", "application-default", "login"], check=True)

TOOLS = {
    "Transfer Data": {
        "script": ROOT_PATH / "pcp_and_realm_csv_transfer.py",
        "description": (
            "Transfer member data between Planning Center People (PCP) and Realm.\n"
            "Reads an exported CSV, applies a column mapping spreadsheet, and writes\n"
            "a reformatted CSV ready for import into the destination system."
        ),
        "detach": False,
    },
    "Find PCP IDs": {
        "script": ROOT_PATH / "find_pcp_ids.py",
        "description": (
            "List all selected definitions in Planning Center People,\n"
            "showing each field's numeric ID, name, and type.\n"
            "Use this to find field IDs needed for configuration."
        ),
        "detach": False,
    },
    "Find CC IDs": {
        "script": ROOT_PATH / "find_cc_ids.py",
        "description": (
            "List all selected definitions in Constant Contact,\n"
            "showing each list's UUID, name, status, and member count.\n"
            "Use this to find list IDs needed for configuration."
        ),
        "detach": False,
    },
    "Workflow Cards Report": {
        "script": ROOT_PATH / "pcp_workflow_report.py",
        "description": (
            "Report every active workflow card across all PCP workflows.\n"
            "Writes a CSV with workflow, step, person, assignee, snoozed,\n"
            "overdue, and last-updated columns."
        ),
        "detach": False,
    },
    "Anytime Items Report": {
        "script": ROOT_PATH / "wf_anytimeitems_rpt.py",
        "description": (
            "Show who still owes the anytime items on a workflow — the ones that\n"
            "can be done in any order but must be done before it completes.\n"
            "Opens an always-current HTML page, sorted by who is missing the most."
        ),
        "detach": False,
    },
    "Validate Anytime Rules": {
        "script": ROOT_PATH / "wf_anytimeitems_validate.py",
        "description": (
            "Check the Anytime WF Items rules against live Planning Center.\n"
            "These fail silently when wrong — cards just stop advancing — so run\n"
            "this after changing a rule, a dropdown's options, or a workflow's steps."
        ),
        "detach": False,
    },
    "Workflow Assignment": {
        "script": ROOT_PATH / "edit_config.py",
        "description": (
            "Edit workflow and CC list rules without modifying Python code.\n"
            "Changes are saved to rules.json — run deploy.sh to apply to Cloud Run."
        ),
        "detach": True,
    },
    "Check for Updates": {
        "script": ROOT_PATH / "run_gitupdater.py",
        "description": (
            "Check GitHub for updates to uvbekutils and bekgoogle libraries\n"
            "and reinstall if newer versions are available."
        ),
        "detach": False,
    },
}


def main() -> None:
    _ensure_adc_auth()
    msg_lines = []
    for name, info in TOOLS.items():
        msg_lines.append(f"{name}\n{info['description']}\n")
    msg = "\n".join(msg_lines)

    buttons = list(TOOLS.keys()) + ["Cancel"]
    choice = confirm(msg, title="Pick a Utility", buttons=buttons)

    if choice.lower() == "cancel" or choice is None:
        return

    for name, info in TOOLS.items():
        if choice.lower() == name.lower():
            if info.get("detach"):
                log_path = info["script"].parent / f"{info['script'].stem}.log"
                log_file = open(log_path, "w", buffering=1)
                proc = subprocess.Popen(
                    [sys.executable, "-u", str(info["script"])],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                    cwd=str(info["script"].parent),
                )
                print(f"Launched {info['script'].name} — log at {log_path}\n")
                # Relay child's log to this console until it signals the
                # window is about to open, then exit so the launcher's
                # QApplication is gone before the child creates its own.
                sentinel = "Done. Opening editor window."
                with open(log_path, "r") as log_read:
                    deadline = time.time() + 180
                    while time.time() < deadline:
                        line = log_read.readline()
                        if line:
                            sys.stdout.write(line)
                            sys.stdout.flush()
                            if sentinel in line:
                                break
                        else:
                            if proc.poll() is not None:
                                break
                            time.sleep(0.1)
            else:
                subprocess.run([sys.executable, str(info["script"])], check=False)
            return


if __name__ == "__main__":
    main()
