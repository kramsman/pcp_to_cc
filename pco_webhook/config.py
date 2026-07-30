"""
Application configuration — reads all environment variables.
Import constants from here rather than reading os.environ directly in main.py.
"""

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load .env file when running locally (no-op in Cloud Run)
load_dotenv()

# ─── GCP ──────────────────────────────────────────────────────────────────────

CLOUD_PROJECT_ID = os.environ["CLOUD_PROJECT_ID"]  # required — set in .env or Cloud Run env vars

# ─── Feature flags ────────────────────────────────────────────────────────────

TEST_MODE    = os.environ.get("TEST_MODE",    "false").lower() == "true"  # true = skip CC API calls, log what would happen instead
LOG_PAYLOADS = os.environ.get("LOG_PAYLOADS", "false").lower() == "true"  # true = log raw webhook payload (contains PII — disable when stable)
logger.debug(f"TEST_MODE={TEST_MODE}  LOG_PAYLOADS={LOG_PAYLOADS}")

# ─── PCP API ──────────────────────────────────────────────────────────────────

PCP_API_BASE = "https://api.planningcenteronline.com/people/v2"

# PCP Personal Access Token credentials are stored in GCP Secret Manager.
# Create a Personal Access Token at: https://api.planningcenteronline.com/oauth/applications
# Store the Application ID and Secret as secrets named "PCP_APP_ID" and "PCP_SECRET".

# PCP custom field definition IDs are hardcoded directly in CC_LIST_RULES and
# WORKFLOW_FIELD_RULES below. Find IDs by running find_pcp_ids.py locally.

# ─── PCP payload types — pushed vs polled ─────────────────────────────────────
#
# PUSHED (use these — PCP sends them automatically to /webhook):
#   {"data": [{"type": "EventDelivery", "attributes": {"name": "...", "payload": "..."}}]}
#   The "name" field identifies the event. The "payload" field is an escaped JSON string
#   containing the actual data. Examples seen in logs:
#     people.v2.events.workflow_card.created       — person added to a workflow
#     people.v2.events.workflow_card.updated        — card moved to next step / completed
#     people.v2.events.workflow_card_activity.created — step completed, comment added, etc.
#       → check attributes.type inside payload: "card_complete" = whole workflow done
#     people.v2.events.workflow_step.updated        — step definition changed (not person-specific)
#     people.v2.events.person.created               — new person record created
#
# POLLED (not used here — requires your code to make a GET request):
#   {"data": {"type": "WorkflowCard", ...}}   single object, no EventDelivery wrapper
#   Only needed if you need data not included in the pushed payload.
#
# ─── Adding a new Pydantic model ──────────────────────────────────────────────
#
# When PCP sends a new payload type you want to parse:
#   1. Copy the raw JSON from Cloud Logging (expand the webhook_payload log entry,
#      copy the "body" field value) into a new file under tests/payloads/PCP/
#   2. Tell Claude: "add a Pydantic model for this payload" — Claude reads the file,
#      identifies data.type, creates the model named after that type, and adds it
#      to the InnerData union in main.py
#   3. Add a property to LegacyWebhookEvent / PcpWebhookEvent if you need to
#      surface a specific field from the new type

# ─── Constant Contact API ─────────────────────────────────────────────────────

CC_API_BASE = "https://api.cc.email/v3"

# CC access token is stored in GCP Secret Manager as "CC_ACCESS_TOKEN".
# Create an app and get credentials at: https://developer.constantcontact.com/

# ─── Workflow Field Rules ─────────────────────────────────────────────────────
# When a workflow card event fires, set a custom field on the person's profile.
#
# workflow_id: PCP workflow ID ("" matches any workflow) — from find_pcp_ids.py
# field_id:    PCP field definition ID — from find_pcp_ids.py
# trigger:     "created"   = person added to workflow (workflow_card.created)
# destroyed:   "completed" = workflow card marked complete (workflow_card.updated, stage=completed)
# value:       value to write to the field

_rules_env = os.environ.get("RULES_JSON")
if _rules_env:
    try:
        import base64
        _rules = json.loads(base64.b64decode(_rules_env).decode())
        logger.info(f"rules loaded from RULES_JSON env var: {len(_rules.get('cc_list_rules',[]))} cc_list_rules")
    except Exception as _e:
        logger.error(f"RULES_JSON env var decode failed ({_e}), falling back to rules.json")
        _RULES_FILE = Path(__file__).parent.parent / "rules.json"
        _rules = json.loads(_RULES_FILE.read_text())
else:
    logger.info("RULES_JSON env var not set — loading rules.json from container")
    _RULES_FILE = Path(__file__).parent.parent / "rules.json"
    _rules = json.loads(_RULES_FILE.read_text())

# ─── Workflow Field Rules ─────────────────────────────────────────────────────
# When a workflow card event fires, set a custom field on the person's profile.
# Edit via pco_utils.py → Edit Rules, or directly in rules.json.
#
# workflow_id: PCP workflow ID ("" matches any workflow) — from find_pcp_ids.py
# field_id:    PCP field definition ID — from find_pcp_ids.py
# trigger:     "entered"   = person added to workflow (workflow_card.created)
#              "completed" = workflow card marked complete (workflow_card.updated, stage=completed)
# value:       value to write to the field

WORKFLOW_FIELD_RULES    = _rules["workflow_field_rules"]
FORM_COMPLETION_RULES   = _rules.get("form_completion_rules", [])
PCP_WORKFLOW_RULES      = _rules.get("pcp_workflow_rules", [])

# ─── Workflow Chain Rules ─────────────────────────────────────────────────────
# When a workflow card event fires, automatically add the person to another workflow.
# Edit via pco_utils.py → Edit Rules, or directly in rules.json.
#
# workflow_id:        source workflow ID — must match the completed workflow
# trigger:            "completed" = workflow card marked complete
# add_to_workflow_id: destination workflow to enroll the person in

WORKFLOW_CHAIN_RULES = _rules["workflow_chain_rules"]

# ─── Anytime Item Workflows ───────────────────────────────────────────────────
# PCP workflows are strictly sequential, but some items ("buy a shirt") can be
# done at any point and must still be done before the workflow completes. Each
# such workflow gets a gate step the webhook only promotes once every item is
# satisfied. Edit via pco_utils.py → Edit Rules, or directly in rules.json.
#
# workflow_id:            PCP workflow the gate belongs to — from find_pcp_ids.py
# field_tab_id:           PCP tab whose dropdown fields ARE the items. Nothing in
#                         PCP links a tab to a workflow; this line is that link.
#                         Discover with: python wf_anytimeitems_validate.py tabs
# gate_step_id:           the "Outstanding items" step held until all items pass
# requires_person_fields: durable field IDs on some OTHER tab, never cleared
#                         (e.g. "background check on file"). Optional.
# notes_field_id:         a text/paragraph field on the tab shown as the report's
#                         final column. Optional.
#
# Which fields are items is NOT configured — it is read from their names, see
# ITEM_SEPARATOR below. Adding an item is a PCP UI action with no config change
# and no deploy.
#
# Only `select` (dropdown) fields on the tab are items. text/paragraph/boolean
# fields are data — shown on the report, never gating — so content fields such as
# "Bio Text Edited" can live on the same tab without blocking anyone.

def _as_list(value) -> list[str]:
    """Accept either a JSON list or a comma-separated string.

    The config editor stores these columns as free text, while a hand-edited
    rules.json is naturally a list. Without this, set("Done, Waived") would
    iterate characters and no item would ever be satisfied.
    """
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(v).strip() for v in value if str(v).strip()]


# An item field names the step it gates, inside its own name:
#
#     get bio!Raw Bio          gates the step called "get bio"
#     Bio: get raw!Raw Bio     gates the step called "Bio: get raw"
#
# "!" is reserved on items tabs: a field containing it IS an anytime item, and
# needs no separate marker. Anything malformed — a step that does not exist, a
# non-dropdown, a typo — is caught by wf_anytimeitems_validate.py, which is a
# better place for it than a prefix nobody can see the absence of.
#
# The step is matched by NAME, not id: step ids change whenever a step is
# recreated or a workflow duplicated, while the name reads plainly to staff.
#
# ":" is reserved in STEP names as a display grouping separator ("Bio: get raw",
# "Bio: edit"). It affects sorting and report headings only, never enforcement.
# There is no conflict between the two: the split below takes the FIRST "!", so
# any ":" in the step name passes through untouched.
ITEM_SEPARATOR = "!"

# Replaced the ">>_" name prefix on 2026-07-30. Fields still carrying it are
# flagged by the validator as needing renaming, since under "!" discovery they
# would silently gate nothing.
LEGACY_ITEM_PREFIX = ">>"


def parse_item_name(field_name: str) -> tuple[str, str] | None:
    """Split an item field name into (step_name, item_label), or None.

    Shared by the webhook, the validator and the clone tool so they can never
    disagree about what counts as an item. Returns None for ordinary data fields,
    which is every field with no separator.

    Splits on the FIRST separator, so an item label may itself contain "!" while
    a step name may not — the validator flags step names that do.
    """
    if ITEM_SEPARATOR not in field_name:
        return None
    step, _, label = field_name.partition(ITEM_SEPARATOR)
    step, label = step.strip(), label.strip()
    if not step or not label:
        return None
    return step, label

# The only values that let an item pass. One vocabulary for every workflow, so
# there is nothing per-rule to get wrong. Edit here to change it everywhere.
#   Yes         — done
#   Not Needed  — does not apply to this person (staff escape hatch)
# Any other dropdown option simply leaves the item outstanding: "Waiting",
# "Promised", and values that exist only to drive rules such as "No"/"Later".
DEFAULT_SATISFYING_VALUES = ["Yes", "Not Needed"]

# The subset meaning a person was let off this item, rather than having done it.
# A strict subset of DEFAULT_SATISFYING_VALUES — these already pass the gate, so
# this only changes how reports present them: counted and styled apart from
# genuine completions, so a card completing on excused items is never silent.
EXCUSED_VALUES = ["Not Needed"]

# Every value an item dropdown is allowed to offer. Anything else is flagged by
# the validator as a probable typo — including rule-driving values, where a
# misspelling makes the rule silently never fire (an option typed "Latter" would
# leave a rule keyed on "Later" doing nothing, with no error anywhere).
#   Yes / Not Needed   satisfy the item — see DEFAULT_SATISFYING_VALUES
#   Promised, Waiting  leave it outstanding
#   Later, No          leave it outstanding and drive WF From Field rules
KNOWN_ITEM_OPTIONS = ["Yes", "Not Needed", "Promised", "Waiting", "Later", "No"]

ANYTIME_ITEM_WORKFLOWS = [
    {**w, "requires_person_fields": _as_list(w.get("requires_person_fields"))}
    for w in _rules.get("anytime_item_workflows", [])
]


def anytime_workflow(workflow_id: str) -> dict | None:
    """Return the anytime-item config for a workflow, or None if it has no gate."""
    return next((w for w in ANYTIME_ITEM_WORKFLOWS
                 if w.get("workflow_id") == workflow_id), None)


def anytime_workflows_for_tab(tab_id: str) -> list[dict]:
    """Return every anytime-item config whose items live on the given tab.

    Used by the field_datum handler: an event carries only a field id, and the
    field names its tab, so this maps a field change back to the gates it affects.
    """
    return [w for w in ANYTIME_ITEM_WORKFLOWS if str(w.get("field_tab_id")) == str(tab_id)]

# ─── CC List Rules ────────────────────────────────────────────────────────────
# Controls which PCP profiles get added to which CC lists.
# Edit via pco_utils.py → Edit Rules, or directly in rules.json.
#
# pcp_field_id: PCP field definition ID — from find_pcp_ids.py
# pcp_value:    field value that triggers the rule
# match:        contains (default) | whole word | exact — see value_matches()
# cc_list_id:   Constant Contact list UUID (find via pco_utils.py → Rpt 'PCO and CC Field Ids')

CC_LIST_RULES = [
    {**r, "cc_lists": [r["cc_list_id"]]} for r in _rules["cc_list_rules"]
]

# ─── Server ───────────────────────────────────────────────────────────────────

PORT = int(os.environ.get("PORT", "8080"))  # Cloud Run sets this automatically

# ─── Startup validation ───────────────────────────────────────────────────────
# Warn (not error) about missing IDs since they are discovered after first deploy.

if not TEST_MODE:
    for rule in CC_LIST_RULES:
        if not any(rule["cc_lists"]):
            logger.warning(f"CC list ID empty in rule '{rule['description']}' — set the corresponding env var")
