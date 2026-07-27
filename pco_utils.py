"""
pco_utils.py

Launcher menu for PCP / Realm / Constant Contact utilities.

Running this script refreshes Google credentials (re-authenticating in the
browser if they have expired), then shows a dialog listing the available tools.
Pick one and the launcher runs its underlying script — most run inline, while
"Edit Rules" is detached into its own process so it can open its own editor
window.

Menu items:
    Edit Rules
        Edit workflow and CC list rules via a GUI without touching Python code;
        changes save to rules.json (run deploy.sh to apply them to Cloud Run).
    Chk 'WF Anytime-Items'
        Check the Anytime WF Items rules against live PCP. They fail silently
        when wrong, so run after changing a rule, a dropdown's options, or a
        workflow's steps.
    Rpt 'WF People'
        Report every active workflow card across all PCP workflows to a CSV
        (workflow, step, person, assignee, snoozed, overdue, last-updated).
    Rpt 'WF Anytime-Items'
        Show who still owes the anytime items on a workflow — those doable in
        any order but required before it completes — as an HTML page.
    Rpt 'PCO and CC Field Ids'
        List every ID used in the rules, from both Planning Center and Constant
        Contact: workflows, forms, tabs, custom fields grouped by tab, CC lists.
    Xfer data bet PCO and Realm
        Transfer member data between Planning Center People (PCP) and Realm —
        reads an exported CSV, applies a column-mapping spreadsheet, and writes
        a reformatted CSV ready for import.
    Chk Google and BEK code updates
        Check GitHub for newer versions of the uvbekutils and bekgoogle
        libraries and reinstall if available.
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
    "Edit Rules": {
        "script": ROOT_PATH / "edit_config.py",
        "description": (
            "Edit workflow and CC list rules without modifying Python code.\n"
            "Changes are saved to rules.json — run deploy.sh to apply to Cloud Run."
        ),
        "detach": True,
    },
    "Chk 'WF Anytime-Items'": {
        "script": ROOT_PATH / "wf_anytimeitems_validate.py",
        "description": (
            "Check the Anytime WF Items rules against live Planning Center.\n"
            "These fail silently when wrong — cards just stop advancing — so run\n"
            "this after changing a rule, a dropdown's options, or a workflow's steps."
        ),
        "detach": False,
    },
    "Rpt 'WF People'": {
        "script": ROOT_PATH / "pcp_workflow_report.py",
        "description": (
            "Report every active workflow card across all PCP workflows.\n"
            "Writes a CSV with workflow, step, person, assignee, snoozed,\n"
            "overdue, and last-updated columns."
        ),
        "detach": False,
    },
    "Rpt 'WF Anytime-Items'": {
        "script": ROOT_PATH / "wf_anytimeitems_rpt.py",
        "description": (
            "Show who still owes the anytime items on a workflow — the ones that\n"
            "can be done in any order but must be done before it completes.\n"
            "Opens an always-current HTML page, sorted by who is missing the most."
        ),
        "detach": False,
    },
    "Rpt 'PCO and CC Field Ids'": {
        "script": ROOT_PATH / "find_pcp_ids.py",
        "description": (
            "List every ID used in the rules, from both Planning Center and\n"
            "Constant Contact: workflows, forms, tabs, custom fields grouped by\n"
            "tab, and CC lists. Use this to find IDs needed for configuration."
        ),
        "detach": False,
    },
    "Xfer data bet PCO and Realm": {
        "script": ROOT_PATH / "pcp_and_realm_csv_transfer.py",
        "description": (
            "Transfer member data between Planning Center People (PCP) and Realm.\n"
            "Reads an exported CSV, applies a column mapping spreadsheet, and writes\n"
            "a reformatted CSV ready for import into the destination system."
        ),
        "detach": False,
    },
    "Chk Google and BEK code updates": {
        "script": ROOT_PATH / "run_gitupdater.py",
        "description": (
            "Check GitHub for updates to uvbekutils and bekgoogle libraries\n"
            "and reinstall if newer versions are available."
        ),
        "detach": False,
    },
}


def _run_detached(info: dict) -> None:
    """Launch a tool that opens its own window, relaying its log until it does."""
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
    # Relay child's log to this console until it signals the window is about to
    # open, then let main() exit so the launcher's QApplication is gone before
    # the child creates its own.
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


def main() -> None:
    _ensure_adc_auth()
    msg = "\n".join(f"{name}\n{info['description']}\n" for name, info in TOOLS.items())
    buttons = list(TOOLS.keys()) + ["Cancel"]

    # Reopen the menu after each tool finishes, so several can be run in one
    # sitting. confirm() reuses QApplication.instance(), so showing the dialog
    # repeatedly in this process is fine.
    while True:
        choice = confirm(msg, title="Pick a Utility", buttons=buttons)

        # Empty (or None) when the dialog is closed with the window button
        # rather than a choice — test that before calling .lower() on it.
        if not choice or choice.lower() == "cancel":
            return

        info = next((i for n, i in TOOLS.items() if choice.lower() == n.lower()), None)
        if info is None:
            continue

        if info.get("detach"):
            # The one tool that cannot loop: it opens its own Qt window, and
            # this process must exit first so there is only one QApplication.
            _run_detached(info)
            return

        subprocess.run([sys.executable, str(info["script"])], check=False)
        print()  # separate this tool's output from the next menu


if __name__ == "__main__":
    main()
