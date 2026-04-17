"""
pcp_launcher.py

Launcher menu for PCP / Realm / Constant Contact utilities.
Run this script to pick and launch one of the available tools.
"""
import os
import sys
import subprocess
from pathlib import Path

sys.path.append(os.path.expanduser("~/Dropbox/Postcard Files/"))
if True:
    import gitupdater

_UTILS_ROOT = Path("/Users/Denise/Library/CloudStorage/Dropbox/PythonPrograms/uvbekutils")
if str(_UTILS_ROOT) not in sys.path:
    sys.path.insert(0, str(_UTILS_ROOT))

from uvbekutils.pyautobek import confirm

HERE = Path(__file__).parent

TOOLS = {
    "Transfer Data": {
        "script": HERE / "pcp_and_realm_csv_transfer.py",
        "description": (
            "Transfer member data between Planning Center People (PCP) and Realm.\n"
            "Reads an exported CSV, applies a column mapping spreadsheet, and writes\n"
            "a reformatted CSV ready for import into the destination system."
        ),
    },
    "Find PCP Field IDs": {
        "script": HERE / "find_pcp_custom_field_ids.py",
        "description": (
            "List all custom field definitions in Planning Center People,\n"
            "showing each field's numeric ID, name, and type.\n"
            "Use this to find field IDs needed for configuration."
        ),
    },
    "Find CC List IDs": {
        "script": HERE / "find_cc_list_ids.py",
        "description": (
            "List all contact lists in Constant Contact,\n"
            "showing each list's UUID, name, status, and member count.\n"
            "Use this to find list IDs needed for configuration."
        ),
    },
}


def main() -> None:
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
            subprocess.run([sys.executable, str(info["script"])], check=False)
            return


if __name__ == "__main__":
    main()
