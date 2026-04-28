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
# Edit via pcp_launcher.py → Edit Config, or directly in rules.json.
#
# workflow_id: PCP workflow ID ("" matches any workflow) — from find_pcp_ids.py
# field_id:    PCP field definition ID — from find_pcp_ids.py
# trigger:     "entered"   = person added to workflow (workflow_card.created)
#              "completed" = workflow card marked complete (workflow_card.updated, stage=completed)
# value:       value to write to the field

WORKFLOW_FIELD_RULES = _rules["workflow_field_rules"]

# ─── Workflow Chain Rules ─────────────────────────────────────────────────────
# When a workflow card event fires, automatically add the person to another workflow.
# Edit via pcp_launcher.py → Edit Config, or directly in rules.json.
#
# workflow_id:        source workflow ID — must match the completed workflow
# trigger:            "completed" = workflow card marked complete
# add_to_workflow_id: destination workflow to enroll the person in

WORKFLOW_CHAIN_RULES = _rules["workflow_chain_rules"]

# ─── CC List Rules ────────────────────────────────────────────────────────────
# Controls which PCP profiles get added to which CC lists.
# Edit via pcp_launcher.py → Edit Config, or directly in rules.json.
#
# pcp_field_id: PCP field definition ID — from find_pcp_ids.py
# pcp_value:    field value that triggers the rule (case-sensitive)
# cc_list_id:   Constant Contact list UUID (find via pcp_launcher → Find CC IDs)

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
