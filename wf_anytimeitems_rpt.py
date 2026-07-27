"""
Build the anytime-items readiness report locally and open it in a browser.

Same data and layout the deployed service serves at /readiness/<workflow_id>,
but run from your machine — no Cloud Run deploy, no READINESS_TOKEN, no Google
auth. Read-only against Planning Center.

Writes wf_anytimeitems_rpt_<workflow_id>.html next to this script, overwriting it
each run. Nothing is timestamped or kept: an out-of-date chase list is worse than
no chase list.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python wf_anytimeitems_rpt.py                 # every configured workflow
    python wf_anytimeitems_rpt.py 730471          # one workflow
    python wf_anytimeitems_rpt.py --no-open       # write the file, don't open it
"""

import argparse
import os
import sys
import webbrowser
from pathlib import Path

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")

from dotenv import load_dotenv  # noqa: E402

_HERE = Path(__file__).parent
load_dotenv(_HERE / ".env")
sys.path.insert(0, str(_HERE / "pco_webhook"))  # main.py uses a bare `import config`


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workflow_id", nargs="?", help="defaults to every configured workflow")
    parser.add_argument("--no-open", action="store_true", help="write the file but do not open it")
    args = parser.parse_args()

    if not os.environ.get("CLOUD_PROJECT_ID"):
        print("ERROR: CLOUD_PROJECT_ID not set in .env", file=sys.stderr)
        return 1

    from pco_webhook import config
    from pco_webhook.main import build_readiness, render_readiness_html

    workflows = ([args.workflow_id] if args.workflow_id
                 else [w["workflow_id"] for w in config.ANYTIME_ITEM_WORKFLOWS])
    if not workflows:
        print("No anytime_item_workflows configured — add one in Edit Config first.")
        return 1

    written = []
    for wid in workflows:
        try:
            data = build_readiness(wid)
        except ValueError as e:
            print(f"  {e}")
            continue
        # Named after this script, one file per workflow, always overwritten —
        # never timestamped. The point is a current view, and stale copies of a
        # chase list are worse than none.
        out = _HERE / f"{Path(__file__).stem}_{wid}.html"
        out.write_text(render_readiness_html(data), encoding="utf-8")
        t = data["totals"]
        print(f"\n{data['config'].get('description', wid)}  ({wid})")
        print(f"  {t['enrolled']} enrolled · {t['ready']} ready · {t['outstanding']} outstanding")
        for row in data["rows"]:
            if row["missing"]:
                missing = [i["name"] for i, c in zip(data["items"], row["cells"]) if not c["done"]]
                print(f"    {row['name']:<28} needs {row['missing']}: {', '.join(missing)}")
        print(f"  → {out}")
        written.append(out)

    if written and not args.no_open:
        webbrowser.open(written[0].as_uri())
    return 0


if __name__ == "__main__":
    sys.exit(main())
