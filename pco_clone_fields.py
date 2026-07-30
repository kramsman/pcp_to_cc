"""
Clone a Planning Center custom field, including its dropdown options.

PCO has no "duplicate field" function, so every anytime item otherwise needs its
option list retyped by hand — tedious, and the options drift apart as you go.
This picks an existing field as a template and creates new ones with the same
type and the same options, differing only in name.

Works for any field type: a text template clones as text with no options. Target
tab may be the same tab as the template.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python pco_clone_fields.py
"""

import os
import re
import sys

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # suppress gRPC noise before grpc loads

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from loguru import logger  # noqa: E402

# pco_webhook.config logs on import, and it is imported partway through the
# prompts — which drops INFO lines into the middle of a numbered list. This is an
# interactive tool with nothing to log, so drop every sink up front.
logger.remove()

_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, ".env"))
sys.path.insert(0, _HERE)

# Deliberately NOT from find_pcp_ids: that imports uvbekutils, whose __init__
# pulls in PySide6. Loading Qt turns this terminal script into a GUI app on
# macOS, which steals focus so you cannot type at its own prompts. This module
# imports bekgoogle lazily, only inside the re-auth branch, so no Qt is loaded.
from wf_anytimeitems_validate import PCP_API_BASE, _get_secret  # noqa: E402

HEADERS = {"User-Agent": "pco_webhook (office2@4thu.org)"}

# Kept in step with pco_webhook.config.ITEM_SEPARATOR, but not imported at module
# level: config reads rules.json, and this tool must still run when that file is
# mid-edit. gating_step() imports config lazily and tolerates failure.
SEPARATOR = "!"


def _auth() -> tuple:
    return (_get_secret("PCP_APP_ID"), _get_secret("PCP_SECRET"))


def _get(path: str, auth: tuple, params: dict | None = None) -> dict:
    r = requests.get(f"{PCP_API_BASE}/{path}", params=params, auth=auth,
                     headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()


def slugify(name: str) -> str:
    """Approximate PCP's own slug derivation, for collision warnings.

    PCP turns "Bio Received" into "bio_received", so ">>_RSVP" and "RSVP" can
    collide even though the names differ. Only an approximation — it is used to
    warn, never to block.
    """
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", name.lower())).strip("_")


# ── prompts ───────────────────────────────────────────────────────────────────

def pick(prompt: str, rows: list[tuple[str, str]], default_idx: int | None = None) -> str | None:
    """Show a numbered list and return the chosen row's id, or None to cancel."""
    print(f"\n{prompt}")
    for i, (_, label) in enumerate(rows, 1):
        print(f"  {i:>3}. {label}")
    hint = f" [{default_idx + 1}]" if default_idx is not None else ""
    while True:
        raw = input(f"Number{hint} (q to quit): ").strip()
        if raw.lower() in ("q", "quit"):
            return None
        if not raw and default_idx is not None:
            return rows[default_idx][0]
        if raw.isdigit() and 1 <= int(raw) <= len(rows):
            return rows[int(raw) - 1][0]
        print("  Not a number on the list.")


def ask(prompt: str, default: str = "") -> str:
    shown = f" [{default}]" if default else ""
    return input(f"{prompt}{shown}: ").strip() or default


def yes(prompt: str, default: bool = False) -> bool:
    d = "Y/n" if default else "y/N"
    raw = input(f"{prompt} [{d}]: ").strip().lower()
    return default if not raw else raw.startswith("y")


# ── PCP reads ─────────────────────────────────────────────────────────────────

def tabs(auth) -> list[tuple[str, str]]:
    rows = _get("tabs", auth, {"per_page": 100}).get("data", [])
    rows.sort(key=lambda t: t["attributes"].get("sequence", 0))
    return [(t["id"], f"{t['attributes'].get('name','')}  (id {t['id']})") for t in rows]


def fields_on(tab_id: str, auth) -> list[dict]:
    body = _get(f"tabs/{tab_id}/field_definitions", auth,
                {"include": "field_options", "per_page": 100})
    by_id = {i["id"]: i["attributes"]["value"]
             for i in body.get("included", []) if i.get("type") == "FieldOption"}
    out = []
    for f in body.get("data", []):
        a = f["attributes"]
        if a.get("deleted_at"):
            continue
        rel = (f.get("relationships", {}).get("field_options", {}).get("data") or [])
        out.append({"id": f["id"], "name": a.get("name", ""),
                    "data_type": a.get("data_type", ""),
                    "sequence": a.get("sequence", 0),
                    "options": [by_id[r["id"]] for r in rel if r["id"] in by_id]})
    return out


def _workflow_for_tab(target_tab: str) -> str:
    """The workflow whose anytime items live on this tab, or "" if none does."""
    try:
        sys.path.insert(0, os.path.join(_HERE, "pco_webhook"))
        from pco_webhook import config
        for w in config.ANYTIME_ITEM_WORKFLOWS:
            if str(w.get("field_tab_id")) == str(target_tab):
                return str(w.get("workflow_id", ""))
    except Exception:
        pass
    return ""


def gating_step(target_tab: str, auth) -> str:
    """Ask which step the new fields should gate, and return its NAME.

    An anytime item names the step it gates inside its own name — "get bio!Raw
    Bio" — so on an items tab the question is not "what prefix?" but "which step
    does this belong to?". Returns "" for a plain data field, which is the right
    answer for bio prose and note fields sharing the tab.
    """
    workflow_id = _workflow_for_tab(target_tab)
    if not workflow_id:
        return ""
    try:
        steps = _get(f"workflows/{workflow_id}/steps", auth, {"per_page": 100}).get("data", [])
    except requests.RequestException as e:
        print(f"  (could not read steps for workflow {workflow_id}: {e})")
        return ""
    steps.sort(key=lambda s: s["attributes"].get("sequence", 0))
    rows = [("", "— none: a plain data field, not an anytime item")]
    rows += [(s["attributes"].get("name", ""),
              f"{s['attributes'].get('name', '')}") for s in steps]
    chosen = pick("\nWhich step should these gate?", rows)
    return chosen or ""


# ── the clone itself ──────────────────────────────────────────────────────────

def clone_field(template: dict, target_tab: str, new_name: str, auth) -> str | None:
    """Create one field from the template. Returns its id, or None on failure.

    Verifies by reading back: options are one API call each, so a partial failure
    would otherwise leave a field silently missing some of them.
    """
    body = {"data": {"type": "FieldDefinition",
                     "attributes": {"name": new_name,
                                    "data_type": template["data_type"]}}}
    try:
        r = requests.post(f"{PCP_API_BASE}/tabs/{target_tab}/field_definitions",
                          json=body, auth=auth, headers=HEADERS, timeout=15)
        r.raise_for_status()
        fid = r.json()["data"]["id"]
    except requests.RequestException as e:
        print(f"  FAILED to create {new_name!r}: {e}")
        if getattr(e, "response", None) is not None:
            print(f"    {e.response.text[:300]}")
        return None

    for value in template["options"]:
        try:
            requests.post(f"{PCP_API_BASE}/field_definitions/{fid}/field_options",
                          json={"data": {"type": "FieldOption",
                                         "attributes": {"value": value}}},
                          auth=auth, headers=HEADERS, timeout=15).raise_for_status()
        except requests.RequestException as e:
            print(f"  option {value!r} failed: {e}")

    got = next((f for f in fields_on(target_tab, auth) if f["id"] == fid), None)
    if got and got["options"] == template["options"]:
        print(f"  created {new_name!r}  (id {fid})"
              + (f"  options={got['options']}" if got["options"] else ""))
        return fid

    print(f"  created {new_name!r} (id {fid}) but options do not match the template:")
    print(f"    wanted {template['options']}")
    print(f"    got    {got['options'] if got else '(could not read back)'}")
    if yes("  Delete this half-made field?", default=True):
        try:
            requests.delete(f"{PCP_API_BASE}/field_definitions/{fid}",
                            auth=auth, headers=HEADERS, timeout=15).raise_for_status()
            print("  deleted.")
        except requests.RequestException as e:
            print(f"  could not delete — remove it in PCO by hand: {e}")
        return None
    return fid


def main() -> int:
    if not os.environ.get("CLOUD_PROJECT_ID"):
        print("ERROR: CLOUD_PROJECT_ID not set in .env", file=sys.stderr)
        return 1
    auth = _auth()

    print("Fetching tabs…")
    tab_rows = tabs(auth)
    if not tab_rows:
        print("No tabs found.")
        return 1

    src_tab = pick("Template — which tab is the field on?", tab_rows)
    if not src_tab:
        return 0
    src_fields = fields_on(src_tab, auth)
    if not src_fields:
        print("That tab has no fields to copy.")
        return 1
    fid = pick("Template — which field?",
               [(f["id"], f"{f['name']:<32} {f['data_type']:<10} "
                          f"{f['options'] if f['options'] else ''}") for f in src_fields])
    if not fid:
        return 0
    template = next(f for f in src_fields if f["id"] == fid)

    src_idx = next(i for i, (tid, _) in enumerate(tab_rows) if tid == src_tab)
    tgt_tab = pick("Create the new field(s) on which tab?", tab_rows, default_idx=src_idx)
    if not tgt_tab:
        return 0

    step = gating_step(tgt_tab, auth)

    print(f"\nTemplate: {template['name']!r} ({template['data_type']})"
          + (f" options={template['options']}" if template["options"] else ""))

    while True:
        raw = ask("\nNew field name(s), comma separated (blank to finish)")
        if not raw:
            break
        existing = fields_on(tgt_tab, auth)
        by_name = {f["name"].lower() for f in existing}
        by_slug = {slugify(f["name"]) for f in existing}

        for part in [p.strip() for p in raw.split(",") if p.strip()]:
            name = f"{step}{SEPARATOR}{part}" if step else part
            if name.lower() in by_name:
                print(f"  SKIPPED {name!r} — that tab already has a field with this name.")
                continue
            if slugify(name) in by_slug:
                print(f"  {name!r} would share the internal slug "
                      f"{slugify(name)!r} with an existing field on that tab.")
                if not yes("  Create it anyway?", default=False):
                    continue
            new_id = clone_field(template, tgt_tab, name, auth)
            if new_id:
                by_name.add(name.lower())
                by_slug.add(slugify(name))

        # Defaults to no: the name prompt above already loops, so Enter here
        # should finish rather than ask again.
        if not yes("\nClone more from the same template?", default=False):
            break

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
