"""
Check the "Anytime WF Items" rules in rules.json against live Planning Center.

These rules fail SILENTLY when wrong — a gate step belonging to another workflow,
a satisfying value matching no real dropdown option, or a text field listed as a
durable prerequisite all produce the same symptom: cards stop advancing, with
nothing in the PCO UI or the webhook logs to say why. Run this after any change
to an anytime rule, to a dropdown's options, or to a workflow's steps.

    validate    (default) check every configured rule.  READ-ONLY.

Also carries the one-off API probes used to design the feature. They need a
DISPOSABLE test card, and the last two change it:

    tabs        How do field definitions relate to tabs?      read-only
    activities  What strings does WorkflowCardActivity.type use?  read-only
    go-back     Does go_back work on an already-completed card?   MUTATES
    send-email  What does a card's native send_email do?          SENDS EMAIL

Results are printed, and written to wf_anytimeitems_validate.html — overwritten
each run, since the question is whether the config is valid right now.

Prerequisites:
    1. .env has CLOUD_PROJECT_ID set.
    2. PCP_APP_ID and PCP_SECRET are stored in GCP Secret Manager.
    3. gcloud auth application-default login has been run.

Usage:
    python wf_anytimeitems_validate.py
    python wf_anytimeitems_validate.py tabs --tab 263604
    python wf_anytimeitems_validate.py activities --workflow 730471 --card 48256640
    python wf_anytimeitems_validate.py go-back --workflow 730471 --card 48256640 --i-mean-it
"""

import argparse
import html
import json
import os
import sys
from datetime import datetime
from pathlib import Path

os.environ.setdefault("GRPC_VERBOSITY", "ERROR")  # suppress gRPC noise before grpc loads

import requests  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from google.api_core import retry as api_retry  # noqa: E402
from google.cloud import secretmanager  # noqa: E402

load_dotenv()

# pco_webhook.config logs when first imported, which happens partway through
# building the report and drops INFO lines into the middle of it.
from loguru import logger  # noqa: E402
logger.remove()

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"
HEADERS = {"User-Agent": "pco_webhook (office2@4thu.org)"}
OUTFILE = Path(__file__).parent / "wf_anytimeitems_validate.html"

_project_id = os.environ.get("CLOUD_PROJECT_ID", "")
_client = None
_cache: dict[str, str] = {}
_lines: list[str] = []


def _get_secret(secret_id: str) -> str:
    """Read a secret from GCP Secret Manager, re-authenticating ADC if needed."""
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
                from bekgoogle import ensure_adc_auth
                ensure_adc_auth()
                _client = secretmanager.SecretManagerServiceClient()
                resp = _client.access_secret_version(request={"name": name})
            else:
                raise
        _cache[secret_id] = resp.payload.data.decode("UTF-8")
    return _cache[secret_id]


def _emit(text: str = "") -> None:
    _lines.append(text)
    print(text)


def _auth() -> tuple:
    return (_get_secret("PCP_APP_ID"), _get_secret("PCP_SECRET"))


def _get(path: str, auth: tuple, params: dict | None = None) -> dict:
    """GET one page from the PCP API and return the parsed body ({} on error)."""
    url = path if path.startswith("http") else f"{PCP_API_BASE}/{path}"
    resp = requests.get(url, auth=auth, params=params, headers=HEADERS, timeout=15)
    if not resp.ok:
        _emit(f"  HTTP {resp.status_code}  {resp.text[:400]}")
        return {}
    return resp.json()


def _post(path: str, auth: tuple, body: dict | None = None) -> requests.Response:
    """POST to the PCP API, echoing status and body so error text is captured."""
    url = path if path.startswith("http") else f"{PCP_API_BASE}/{path}"
    resp = requests.post(url, auth=auth, json=body, headers=HEADERS, timeout=15)
    _emit(f"  → HTTP {resp.status_code}")
    if resp.text:
        _emit(f"  → body: {resp.text[:800]}")
    return resp


# ── Probe 1: tabs → field definitions (DESIGN-CRITICAL) ──────────────────────

def probe_tabs(auth: tuple, tab_id: str | None = None) -> None:
    """Establish whether field definitions can be discovered from a tab.

    The design depends on this: config names a tab, and the items are whatever
    fields live on it. If FieldDefinition exposes no usable tab link, config must
    instead list field IDs explicitly (see the plan's fallback).
    """
    _emit("=" * 78)
    _emit("PROBE 1 — tabs → field definitions   (DESIGN-CRITICAL)")
    _emit("=" * 78)

    tabs = _get("tabs", auth, {"per_page": 100}).get("data", [])
    _emit(f"\nGET /tabs  →  {len(tabs)} tabs")
    for t in tabs:
        _emit(f"  {t['id']:<10}  {t['attributes'].get('name', '')!r}"
              f"   sequence={t['attributes'].get('sequence')}")
    if not tabs:
        _emit("\n  NO TABS FOUND — design fallback required (explicit item_field_ids).")
        return

    chosen = next((t for t in tabs if t["id"] == tab_id), tabs[0])
    tab_id = chosen["id"]
    tab_name = chosen["attributes"].get("name", "")
    _emit(f"\nGET /tabs/{tab_id}/field_definitions   (tab {tab_name!r})")
    body = _get(f"tabs/{tab_id}/field_definitions", auth,
                {"include": "field_options", "per_page": 100})
    fields = body.get("data", [])
    _emit(f"  → {len(fields)} field definitions on this tab")

    options_by_id = {
        item["id"]: item["attributes"]["value"]
        for item in body.get("included", [])
        if item.get("type") == "FieldOption"
    }
    for f in fields:
        attrs = f["attributes"]
        dtype = attrs.get("data_type", "?")
        line = f"  {f['id']:<10}  {attrs.get('name', ''):<38}  {dtype}"
        if dtype in ("select", "checkboxes"):
            related = (f.get("relationships", {}).get("field_options", {}).get("data") or [])
            opts = [options_by_id[r["id"]] for r in related if r["id"] in options_by_id]
            line += f"  options={opts}"
        _emit(line)

    # The reverse direction matters just as much: given a field, can we name its
    # tab? The webhook receives a field_datum event carrying only a field id, and
    # has to decide which workflow's gate to re-evaluate.
    _emit("\nReverse lookup — does a FieldDefinition name its tab?")
    one = _get("field_definitions", auth, {"per_page": 1}).get("data", [])
    if one:
        rels = one[0].get("relationships", {})
        _emit(f"  FieldDefinition {one[0]['id']} relationships: {list(rels.keys())}")
        _emit(f"  attributes: {list(one[0]['attributes'].keys())}")
        if "tab" in rels:
            _emit(f"  tab → {json.dumps(rels['tab'].get('data'))}")
            _emit("\n  VERDICT: tab-driven config works in both directions.")
        elif "tab_id" in one[0]["attributes"]:
            _emit(f"  tab_id attribute = {one[0]['attributes']['tab_id']}")
            _emit("\n  VERDICT: tab-driven config works via the tab_id attribute.")
        else:
            _emit("\n  VERDICT: no direct tab link on FieldDefinition. Forward lookup "
                  "still works, so cache tab→fields at startup and invert the map.")


# ── Probe 2: workflow card activity types ────────────────────────────────────

def probe_activities(auth: tuple, workflow: str, card: str) -> None:
    """Print every activity on a card so completed vs skipped can be told apart.

    Complete one step and skip another in the PCO UI before running this,
    otherwise there is nothing to compare.
    """
    _emit("=" * 78)
    _emit("PROBE 2 — WorkflowCardActivity.attributes.type")
    _emit("=" * 78)
    _emit("\nExpects: one step COMPLETED and one step SKIPPED in the PCO UI first.\n")

    steps = _get(f"workflows/{workflow}/steps", auth, {"per_page": 100}).get("data", [])
    seq_by_id = {s["id"]: s["attributes"].get("sequence") for s in steps}
    name_by_id = {s["id"]: s["attributes"].get("name", "") for s in steps}
    _emit(f"Workflow {workflow} steps:")
    for s in sorted(steps, key=lambda x: x["attributes"].get("sequence", 0)):
        _emit(f"  seq {s['attributes'].get('sequence')}  id={s['id']:<10} {s['attributes'].get('name','')!r}")

    body = _get(f"workflows/{workflow}/cards/{card}/activities", auth, {"per_page": 100})
    acts = body.get("data", [])
    _emit(f"\nGET .../cards/{card}/activities  →  {len(acts)} activities\n")
    seen: set[str] = set()
    for a in acts:
        attrs = a["attributes"]
        step = (a.get("relationships", {}).get("workflow_step", {}).get("data") or {}).get("id", "")
        atype = attrs.get("type", "")
        seen.add(str(atype))
        _emit(f"  type={atype!r:<24} step={name_by_id.get(step, step)!r} "
              f"(seq {seq_by_id.get(step)})")
        if attrs.get("comment"):
            _emit(f"      comment: {attrs['comment']!r}")
        _emit(f"      all attributes: {json.dumps(attrs, default=str)[:300]}")

    _emit(f"\n  DISTINCT type VALUES: {sorted(seen)}")
    _emit("  → If completed and skipped differ here, a hard backstop is possible.")


# ── Probe 3: go_back on a completed card ─────────────────────────────────────

def probe_go_back(auth: tuple, workflow: str, card: str) -> None:
    """Attempt go_back on a completed card. MUTATES the card."""
    _emit("=" * 78)
    _emit("PROBE 3 — go_back on a COMPLETED card   (MUTATES)")
    _emit("=" * 78)

    before = _get(f"workflows/{workflow}/cards/{card}", auth).get("data", {})
    b_attrs = before.get("attributes", {})
    b_step = (before.get("relationships", {}).get("current_step", {}).get("data") or {})
    _emit(f"\nBefore:  stage={b_attrs.get('stage')!r}  "
          f"completed_at={b_attrs.get('completed_at')}  "
          f"current_step={b_step.get('id')}")

    if b_attrs.get("stage") != "completed":
        _emit("\n  Card is NOT completed — this probe only answers the question for a "
              "completed card. Complete it in the PCO UI first, then re-run.")
        return

    _emit("\nPOST .../go_back")
    _post(f"workflows/{workflow}/cards/{card}/go_back", auth)

    after = _get(f"workflows/{workflow}/cards/{card}", auth).get("data", {})
    a_attrs = after.get("attributes", {})
    a_step = (after.get("relationships", {}).get("current_step", {}).get("data") or {})
    _emit(f"\nAfter:   stage={a_attrs.get('stage')!r}  "
          f"completed_at={a_attrs.get('completed_at')}  "
          f"current_step={a_step.get('id')}")
    _emit("\n  → If stage returned to 'ready' with a current_step, the holding step MAY "
          "be last. If not, add a trivial 'Confirm & finish' step after it.")


# ── Probe 4: native send_email ───────────────────────────────────────────────

def probe_send_email(auth: tuple, workflow: str, card: str) -> None:
    """Discover what the card's native send_email action requires. SENDS EMAIL."""
    _emit("=" * 78)
    _emit("PROBE 4 — native card send_email   (MAY SEND REAL EMAIL)")
    _emit("=" * 78)

    card_body = _get(f"workflows/{workflow}/cards/{card}", auth).get("data", {})
    person = (card_body.get("relationships", {}).get("person", {}).get("data") or {})
    _emit(f"\nCard {card} is for person {person.get('id')} — make sure that is YOU.")

    _emit("\nStep A: POST with an empty body, to read the validation error.")
    resp = _post(f"workflows/{workflow}/cards/{card}/send_email", auth, {})
    if resp.ok:
        _emit("\n  Empty body ACCEPTED — an email may have been sent. "
              "Check the recipient's inbox and the card's activity log.")
        return

    _emit("\nStep B: POST with subject/body, using the params named above if they differ.")
    _post(f"workflows/{workflow}/cards/{card}/send_email", auth,
          {"data": {"attributes": {"subject": "PCO probe — please ignore",
                                   "body": "Probe of the native send_email action."}}})
    _emit("\n  → Check who actually received it: the person on the card, or the "
          "assignee. If it is assignee-only or untemplated, use Brevo.")


# ── Probe 5: validate configured anytime rules ───────────────────────────────

_tab_name_cache: dict = {}


def tab_name(tab_id, auth) -> str:
    """Tab name for an id, cached. Empty string if it cannot be read."""
    key = str(tab_id or "")
    if not key:
        return ""
    if key not in _tab_name_cache:
        body = _get(f"tabs/{key}", auth).get("data", {})
        _tab_name_cache[key] = (body.get("attributes", {}) or {}).get("name", "")
    return _tab_name_cache[key]


def _known_options() -> list:
    """The approved item-dropdown vocabulary, from pco_webhook/config.py."""
    try:
        sys.path.insert(0, str(Path(__file__).parent / "pco_webhook"))
        from pco_webhook import config
        return list(config.KNOWN_ITEM_OPTIONS)
    except Exception:
        return ["Yes", "Not Needed", "Promised", "Waiting", "Later", "No"]


def _satisfying_values() -> list:
    """The org-wide satisfying values, from pco_webhook/config.py."""
    try:
        sys.path.insert(0, str(Path(__file__).parent / "pco_webhook"))
        from pco_webhook import config
        return list(config.DEFAULT_SATISFYING_VALUES)
    except Exception:
        return ["Yes", "Not Needed"]


def _parse_item_name(field_name: str):
    """Split "step!label", via pco_webhook/config.py so both agree exactly."""
    try:
        sys.path.insert(0, str(Path(__file__).parent / "pco_webhook"))
        from pco_webhook import config
        return config.parse_item_name(field_name)
    except Exception:
        if "!" not in field_name:
            return None
        step, _, label = field_name.partition("!")
        return (step.strip(), label.strip()) if step.strip() and label.strip() else None


def validate_rules(auth: tuple | None = None) -> tuple[int, list[str]]:
    """Check every anytime_item_workflows rule against live PCP. Read-only.

    Returns (problem_count, report_lines) so callers other than the CLI — notably
    the config editor's save — can act on the result instead of parsing stdout.

    A gate step id belonging to a different workflow, or a satisfying value that
    does not match any real dropdown option, both fail silently at runtime: cards
    simply never advance. This turns those into errors you can see.
    """
    _lines.clear()
    auth = auth or _auth()
    _emit("=" * 78)
    _emit("Validate configured anytime rules")
    _emit("=" * 78)

    rules_path = Path(__file__).parent / "rules.json"
    rules = json.loads(rules_path.read_text()).get("anytime_item_workflows", [])
    if not rules:
        _emit("\nNo anytime_item_workflows configured.")
        return 0, list(_lines)

    def as_list(v):
        if not v:
            return []
        return [x.strip() for x in v.split(",")] if isinstance(v, str) else [str(x) for x in v]

    problems = 0
    for rule in rules:
        _emit(f"\n--- {rule.get('description', '(no description)')} ---")
        wf, tab = str(rule.get("workflow_id", "")), str(rule.get("field_tab_id", ""))

        wf_body = _get(f"workflows/{wf}", auth).get("data", {})
        _emit(f"  workflow {wf}: {wf_body.get('attributes', {}).get('name', 'NOT FOUND')!r}")
        if not wf_body:
            problems += 1

        steps = _get(f"workflows/{wf}/steps", auth, {"per_page": 100}).get("data", [])
        gate = str(rule.get("gate_step_id", ""))
        match = next((s for s in steps if s["id"] == gate), None)
        if match:
            # Rank among the sorted steps, not PCP's raw `sequence`: that is
            # 0-based on some workflows and 1-based on others, so printing it
            # gives "1 of 2" for the middle of three steps.
            order = sorted(steps, key=lambda x: x["attributes"].get("sequence", 0))
            pos = next(i for i, x in enumerate(order, 1) if x["id"] == gate)
            total = len(order)
            _emit(f"  holding step {gate}: {match['attributes'].get('name')!r} "
                  f"at position {pos} of {total}")
            if pos == total:
                _emit("    WARNING: the holding step is the LAST step. A staff member skipping it "
                      "completes the card outright, and recovery depends on go_back, which "
                      "is unverified. Add a trailing 'Confirm & finish' step.")
            if pos == 1:
                _emit("    WARNING: the holding step is the FIRST step, so every card waits there "
                      "immediately and the sequential steps never run.")
        else:
            _emit(f"  holding step {gate}: NOT FOUND in this workflow  <-- cards will never be released")
            _emit(f"    steps that do exist: "
                  f"{[(s['id'], s['attributes'].get('name')) for s in steps]}")
            problems += 1

        body = _get(f"tabs/{tab}/field_definitions", auth,
                    {"include": "field_options", "per_page": 100})
        by_id = {i["id"]: i["attributes"]["value"]
                 for i in body.get("included", []) if i.get("type") == "FieldOption"}
        # An item names the step it gates, so the step list is what item names are
        # checked against. Two steps sharing a name make "get bio!Raw Bio"
        # ambiguous; the card would park at one and sail through the other.
        ordered = sorted(steps, key=lambda x: x["attributes"].get("sequence", 0))
        step_names = [s["attributes"].get("name", "") for s in ordered]
        step_lc = {n.strip().lower() for n in step_names}
        last_step = step_names[-1] if step_names else ""

        seen_lc = set()
        for n in step_names:
            key = n.strip().lower()
            if key in seen_lc:
                _emit(f"    ERROR: two steps in this workflow are both named {n!r}. An item "
                      f"naming it cannot say which is meant, so the card parks at one and "
                      f"passes straight through the other. Rename one.")
                problems += 1
            seen_lc.add(key)
            if "!" in n:
                _emit(f"    ERROR: step {n!r} contains '!', which is reserved as the item "
                      f"separator. A field naming this step could not be parsed. Rename it.")
                problems += 1
        _emit()

        selects, ignored, unmarked, mismarked, legacy, all_options = [], [], [], [], [], set()
        for f in body.get("data", []):
            if f["attributes"].get("deleted_at"):
                continue
            name, dtype = f["attributes"].get("name", ""), f["attributes"].get("data_type")
            parsed = _parse_item_name(name)
            if name.startswith(">>"):
                legacy.append((f["id"], name))
            if parsed and dtype == "select":
                opts = [by_id[r["id"]] for r in
                        (f.get("relationships", {}).get("field_options", {}).get("data") or [])
                        if r["id"] in by_id]
                selects.append((f["id"], name, opts, parsed[0], parsed[1]))
                all_options.update(opts)
            elif parsed:
                mismarked.append((f["id"], name, dtype))   # names a step, but not a dropdown
            elif dtype == "select":
                unmarked.append((f["id"], name))           # dropdown gating nothing
            else:
                ignored.append((name, dtype))

        _emit(f"  Associated fields in tab/screen {tab_name(tab, auth)!r} {tab}; "
              f"{len(selects)} required, {len(ignored)} ignored")
        for fid, name, opts, step_name, label in selects:
            _emit(f"    ITEM  {fid:<10} {label:<24} before step {step_name!r}  options={opts}")
        for name, dtype in ignored:
            _emit(f"    data  {'':<10} {name:<34} ({dtype}) — not required")

        # Without a marker on the field, a typo in the step name is the failure
        # that matters: the item looks fine and silently gates nothing. These
        # checks are what make that loud.
        for fid, name, opts, step_name, label in selects:
            if step_name.strip().lower() not in step_lc:
                _emit(f"    ERROR: {name!r} ({fid}) names step {step_name!r}, which does not "
                      f"exist in this workflow, so it gates nothing. Steps: {step_names}")
                problems += 1
            elif step_name.strip().lower() == last_step.strip().lower():
                _emit(f"    WARNING: {name!r} ({fid}) gates {step_name!r}, the LAST step. "
                      f"Nothing follows it, so a card completes rather than parking. "
                      f"Add a trailing step after it.")

        for fid, name in legacy:
            _emit(f"    ERROR: {name!r} ({fid}) still uses the old '>>_' prefix. Items now "
                  f"name the step they gate, e.g. 'get bio!{name.lstrip('>_')}'. As it "
                  f"stands it gates nothing.")
            problems += 1
        for fid, name in unmarked:
            _emit(f"    NOTE: dropdown {name!r} ({fid}) names no step, so it is NOT required. "
                  f"Rename it to 'step!{name}' if it should gate one.")
        for fid, name, dtype in mismarked:
            _emit(f"    ERROR: {name!r} ({fid}) names a step but is a `{dtype}`, not a "
                  f"dropdown. Its value can never match a satisfying value, so it is being "
                  f"ignored. Make it a dropdown or take the '!' out of its name.")
            problems += 1

        # Every option is checked against one approved vocabulary. Comparing items
        # to each other instead would nag about legitimate per-item values such as
        # "Later", while missing the mistake that matters: a rule-driving option
        # typed "Latter" leaves its rule silently never firing.
        known = _known_options()
        known_lc = {k.strip().lower() for k in known}
        unknown = [(fid, name, o) for fid, name, opts, _step, _label in selects
                   for o in opts if o.strip().lower() not in known_lc]
        for fid, name, opt in unknown:
            _emit(f"    NOTE: {name!r} ({fid}) offers {opt!r}, which is not a known choice.")
            _emit(f"          Known: {', '.join(known)}.")
            _emit("          Likely a typo — a rule keyed on it would silently never fire. "
                  "If deliberate, add it to KNOWN_ITEM_OPTIONS.")
            _emit()

        if not selects:
            _emit("    ERROR: nothing on this tab is required, so the card is released "
                  "immediately. No dropdown here names a step, e.g. 'get bio!Raw Bio'.")
            problems += 1

        satisfying = _satisfying_values()
        # Compared case-insensitively, matching how the webhook decides —
        # see satisfies() in pco_webhook/main.py.
        sat_lc = {v.strip().lower() for v in satisfying}
        opts_lc = {o.strip().lower() for o in all_options}
        unmatched = [v for v in satisfying if v.strip().lower() not in opts_lc]
        _emit()
        _emit(f"  satisfying values: {satisfying}  (same for all workflows)")

        if unmatched:
            _emit(f"    NOTE: {unmatched} match no option on this tab. Harmless if "
                  f"intentional (e.g. spelling variants), but a typo here means the "
                  f"item can never be satisfied.")
        never = [(fid, n) for fid, n, o, _step, _label in selects
                 if not {x.strip().lower() for x in o} & sat_lc]
        for fid, n in never:
            _emit(f"    ERROR: item {n!r} ({fid}) has NO option that satisfies it — "
                  f"every card will park here forever.")
            problems += 1

        for label, key in [("durable", "requires_person_fields"), ("notes", "notes_field_id")]:
            for fid in as_list(rule.get(key)):
                fd = _get(f"field_definitions/{fid}", auth).get("data", {})
                attrs = fd.get("attributes", {})
                dtype = attrs.get("data_type", "?")
                t_id = attrs.get("tab_id")
                _emit(f"  {label} field {fid}: {attrs.get('name', 'NOT FOUND')!r} "
                      f"({dtype}) on tab {t_id} {tab_name(t_id, auth)!r}")
                if not fd:
                    problems += 1
                    continue
                if key != "requires_person_fields":
                    continue
                # A durable field becomes a gating item, so it has to be a
                # dropdown whose options include a satisfying value. A text field
                # here holds prose that can never equal "Done", which parks every
                # card on it permanently.
                if dtype != "select":
                    _emit(f"    ERROR: a durable field must be a `select` dropdown. "
                          f"{attrs.get('name')!r} is `{dtype}`, so its value can never "
                          f"match {satisfying} and every card will park on it forever. "
                          f"Remove it from Durable field IDs.")
                    problems += 1
                else:
                    opts = [by_id[r["id"]] for r in
                            (fd.get("relationships", {}).get("field_options", {}).get("data") or [])
                            if r["id"] in by_id]
                    if opts and not {x.strip().lower() for x in opts} & sat_lc:
                        _emit(f"    ERROR: options {opts} include no satisfying value.")
                        problems += 1
                if str(attrs.get("tab_id")) == tab:
                    _emit("    WARNING: this durable field lives on the ITEMS tab, so it "
                          "will be cleared on re-enrolment — the opposite of durable. "
                          "Move it to another tab.")

    _emit(f"\n{'=' * 78}")
    _emit(f"{problems} setup problem{'s' if problems != 1 else ''} found — fix these or "
          f"cards will never leave the holding step." if problems
          else "No setup problems found.")
    return problems, list(_lines)


def problems_only(lines: list[str]) -> list[str]:
    """Just the ERROR/WARNING/NOTE lines, with the rule heading they sit under.

    The full report is long and mostly reassurance. When something is wrong the
    only useful part is what is wrong and which rule it belongs to.
    """
    out, heading = [], ""
    for line in lines:
        if line.startswith("--- "):
            heading = line
        elif line.strip().startswith(("ERROR", "WARNING", "NOTE")):
            if heading and heading not in out:
                out.append(heading)
            out.append(line.strip())
    return out


def render_html(lines: list[str], problems: int) -> str:
    """Render the report, blocking problems first so they can't be missed."""
    esc = html.escape
    findings = problems_only(lines)

    if problems:
        banner = (f'<div class="bad"><strong>{problems} setup problem'
                  f'{"s" if problems != 1 else ""}.</strong> Fix these or cards will '
                  f'never leave the holding step, with nothing in the logs to say why.</div>')
    elif findings:
        banner = '<div class="warn"><strong>No setup problems.</strong> Notes below.</div>'
    else:
        banner = '<div class="ok"><strong>No setup problems found.</strong></div>'

    top = ""
    if findings:
        top = "<ul>" + "".join(
            f'<li class="{"e" if f.startswith("ERROR") else "w" if f.startswith("WARNING") else "n"}">'
            f"{esc(f)}</li>" for f in findings if not f.startswith("---")
        ) + "</ul>"

    body = []
    for line in lines:
        s = line.strip()
        cls = ("e" if s.startswith("ERROR") else "w" if s.startswith("WARNING")
               else "n" if s.startswith("NOTE") else "")
        body.append(f'<span class="{cls}">{esc(line)}</span>' if cls else esc(line))

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Anytime rules — validation</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; margin:0; padding:1.5rem; }}
 h1 {{ font-size:1.2rem; margin:0 0 .25rem; }}
 .sub {{ opacity:.7; font-size:.85rem; margin-bottom:1rem; }}
 div.ok, div.warn, div.bad {{ padding:.6rem .8rem; border-radius:.4rem; margin-bottom:1rem;
        border-left:4px solid currentColor; }}
 div.ok {{ color:#1b6b3a; }} div.warn {{ color:#8a6100; }} div.bad {{ color:#b3261e; }}
 ul {{ margin:0 0 1.5rem; padding-left:1.2rem; }}
 li {{ margin:.25rem 0; }}
 li.e, .e {{ color:#b3261e; font-weight:600; }}
 li.w, .w {{ color:#8a6100; }}
 li.n, .n {{ opacity:.75; }}
 pre {{ overflow-x:auto; background:rgba(128,128,128,.09); padding:1rem;
        border-radius:.4rem; font-size:12.5px; line-height:1.45; }}
 @media (prefers-color-scheme: dark) {{
   div.ok, li.e, .e {{ }} .e, li.e {{ color:#ff8a80; }} div.bad {{ color:#ff8a80; }}
   .w, li.w {{ color:#ffcc80; }} div.warn {{ color:#ffcc80; }} div.ok {{ color:#7ddc9a; }}
 }}
</style></head><body>
<h1>Anytime rules — validation</h1>
<div class="sub">rules.json checked against live Planning Center ·
 {datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
{banner}
{top}
<pre>{chr(10).join(body)}</pre>
</body></html>"""


def reorder_items_tab(auth: tuple, apply: bool = False) -> int:
    """Sequence each rule's items tab to match its workflow's step order.

    With gates spread across several steps, the items tab IS the place to see
    everything a workflow requires — it is visible on any profile and under
    People → Manage → Tabs. But PCP orders tab fields by a manual `sequence`, so
    without this they sit in creation order and the grouping is invisible.

    Items sort by their step's position, then by label. Fields that gate nothing
    (bio prose, notes) keep to the end, where they read as an appendix.

    Read-only unless `apply`, so the intended order can be checked first.
    """
    _emit("=" * 78)
    _emit("Order items tabs to match step order" + ("" if apply else "  (DRY RUN)"))
    _emit("=" * 78)

    rules_path = Path(__file__).parent / "rules.json"
    rules = json.loads(rules_path.read_text()).get("anytime_item_workflows", [])
    changed = 0

    for rule in rules:
        wf, tab = str(rule.get("workflow_id", "")), str(rule.get("field_tab_id", ""))
        _emit(f"\n--- {rule.get('description', '(no description)')} ---")

        steps = _get(f"workflows/{wf}/steps", auth, {"per_page": 100}).get("data", [])
        rank = {s["attributes"].get("name", "").strip().lower(): i
                for i, s in enumerate(sorted(steps,
                                             key=lambda x: x["attributes"].get("sequence", 0)))}

        fields = [f for f in _get(f"tabs/{tab}/field_definitions", auth,
                                  {"per_page": 100}).get("data", [])
                  if not f["attributes"].get("deleted_at")]

        def sort_key(f):
            name = f["attributes"].get("name", "")
            parsed = _parse_item_name(name)
            if not parsed:
                return (2, 0, name.lower())          # data fields last
            step, label = parsed
            pos = rank.get(step.strip().lower())
            if pos is None:
                return (1, 0, name.lower())          # names a step that is gone
            return (0, pos, label.lower())

        wanted = sorted(fields, key=sort_key)
        for i, f in enumerate(wanted):
            name = f["attributes"].get("name", "")
            was = f["attributes"].get("sequence")
            if was == i:
                continue
            _emit(f"  {name:<40} sequence {was} -> {i}")
            changed += 1
            if not apply:
                continue
            try:
                requests.patch(
                    f"{PCP_API_BASE}/tabs/{tab}/field_definitions/{f['id']}",
                    json={"data": {"type": "FieldDefinition", "id": f["id"],
                                   "attributes": {"sequence": i}}},
                    auth=auth, headers=HEADERS, timeout=15,
                ).raise_for_status()
            except requests.RequestException as e:
                _emit(f"    ERROR: could not set sequence on {name!r}: {e}")

    if not changed:
        _emit("\nAlready in step order — nothing to do.")
    elif not apply:
        _emit(f"\n{changed} field(s) would move. Re-run with --i-mean-it to apply.")
    else:
        _emit(f"\n{changed} field(s) reordered.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("probe", nargs="?", default="validate",
                        choices=["validate", "reorder", "tabs",
                                 "activities", "go-back", "send-email"],
                        help="defaults to 'validate'")
    parser.add_argument("--workflow", help="workflow id (not needed for 'tabs')")
    parser.add_argument("--card", help="DISPOSABLE test card id (not needed for 'tabs')")
    parser.add_argument("--tab", help="for 'tabs': inspect this tab instead of the first")
    parser.add_argument("--no-popup", action="store_true",
                        help="skip the results dialog — for scripted or scheduled runs")
    parser.add_argument("--i-mean-it", action="store_true",
                        help="required for go-back and send-email, which mutate or send; "
                             "for 'reorder', applies the change instead of a dry run")
    args = parser.parse_args()

    if not _project_id:
        print("ERROR: CLOUD_PROJECT_ID not set in .env", file=sys.stderr)
        return 1

    needs_card = args.probe in ("activities", "go-back", "send-email")
    if needs_card and not (args.workflow and args.card):
        print(f"ERROR: --workflow and --card are required for '{args.probe}'", file=sys.stderr)
        return 1

    if args.probe in ("go-back", "send-email") and not args.i_mean_it:
        print(f"ERROR: '{args.probe}' modifies the card or sends real email.\n"
              f"       Re-run with --i-mean-it once you are using a disposable card.",
              file=sys.stderr)
        return 1

    auth = _auth()
    problems = 0
    if args.probe == "tabs":
        probe_tabs(auth, args.tab)
    elif args.probe == "validate":
        problems, _ = validate_rules(auth)
    elif args.probe == "reorder":
        _lines.clear()
        reorder_items_tab(auth, apply=args.i_mean_it)
    elif args.probe == "activities":
        probe_activities(auth, args.workflow, args.card)
    elif args.probe == "go-back":
        probe_go_back(auth, args.workflow, args.card)
    else:
        probe_send_email(auth, args.workflow, args.card)

    # Overwritten, not appended: this answers "is my config valid right now",
    # and an old run above the current one is just something to misread.
    OUTFILE.write_text(render_html(_lines, problems), encoding="utf-8")
    print(f"\nWritten to {OUTFILE}")

    # Shown unless explicitly suppressed. isatty() was tried and is wrong here:
    # PyCharm's console reports False, so the popup silently never appeared.
    if not os.environ.get("CLOUD_RUN_JOB") and not args.no_popup:
        from uvbekutils.pyautobek import confirm_with_file_link
        headline = (f"{problems} setup problem{'s' if problems != 1 else ''} found — "
                    f"fix these or cards will never leave the holding step."
                    if problems else "No setup problems found.")
        confirm_with_file_link(
            headline + "\nClick the link below to open the full report.",
            str(OUTFILE),
            title="Chk 'WF Anytime-Items'",
            buttons=["OK"],
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
