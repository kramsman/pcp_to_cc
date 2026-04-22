"""
Application configuration — reads all environment variables.
Import constants from here rather than reading os.environ directly in main.py.
"""

import os

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

# PCP custom field definition IDs.
# Each custom field in PCP has a numeric ID you set once here.
# How to find the ID: run python find_pcp_ids.py locally (requires PCP credentials).
# Then set the env var (e.g. PCP_NEWSLETTER_TRIGGER_FIELD_ID=12345) in .env or set-env-vars.sh.
PCP_FIELD_IDS = {
    "newsletter_opt_in":  os.environ.get("PCP_NEWSLETTER_FIELD_ID", ""),
    "temp_import_field":  os.environ.get("PCP_TEMP_IMPORT_FIELD_ID", ""),
}

# ─── Constant Contact API ─────────────────────────────────────────────────────

CC_API_BASE = "https://api.cc.email/v3"

# CC access token is stored in GCP Secret Manager as "CC_ACCESS_TOKEN".
# Create an app and get credentials at: https://developer.constantcontact.com/

# ─── Workflow Field Rules ─────────────────────────────────────────────────────
# When a person is added to a PCP workflow, set a custom field on their profile.
#
# workflow_id: PCP workflow ID that triggers this rule ("" matches any workflow)
# field_name:  key in PCP_FIELD_IDS above
# value:       value to write to that field
#
# How to find workflow IDs: run python find_pcp_ids.py locally.
# Then set the env var (e.g. PCP_CONNECT_WORKFLOW_ID=730471) in .env.

WORKFLOW_FIELD_RULES = [
    {
        "description": "Added to New Visitor workflow → set TempImportField = Visitor",
        "workflow_id": os.environ.get("PCP_NEW_VISITOR_WORKFLOW_ID", ""),
        "field_name":  "temp_import_field",
        "value":       "Visitor",
    },
]

# ─── CC List Rules ────────────────────────────────────────────────────────────
# Controls which PCP profiles get added to which CC lists.
#
# Each rule: if the person has pcp_field == pcp_value, add them to all cc_lists.
# Multiple rules can match — person is added to the union of all matching lists.
#
# pcp_field:  key in PCP_FIELD_IDS above (must match a defined PCP custom field)
# pcp_value:  the field value that triggers the rule (case-sensitive)
# cc_lists:   list of Constant Contact list UUIDs
#
# How to find CC list UUIDs:
#   Log into Constant Contact → Contacts → Lists → click a list → UUID is in the URL.
#   Then set the env var (e.g. CC_NEWSLETTER_LIST_ID=abc-123) in .env or set-env-vars.sh.
#
# To add a new list: copy one of the rule dicts below and fill in the values.
# No other code changes needed.

CC_LIST_RULES = [
    {
        "description": "Newsletter opt-in → newsletter list",
        "pcp_field":   "newsletter_opt_in",
        "pcp_value":   "true",
        "cc_lists":    [os.environ.get("CC_NEWSLETTER_LIST_ID", "")],
    },
    # Example of a second rule — uncomment and fill in to activate:
    # {
    #     "description": "Events opt-in → events list",
    #     "pcp_field":   "events_opt_in",
    #     "pcp_value":   "Yes",
    #     "cc_lists":    [os.environ.get("CC_EVENTS_LIST_ID", "")],
    # },
]

# ─── Server ───────────────────────────────────────────────────────────────────

PORT = int(os.environ.get("PORT", "8080"))  # Cloud Run sets this automatically

# ─── Startup validation ───────────────────────────────────────────────────────
# Warn (not error) about missing IDs since they are discovered after first deploy.

if not TEST_MODE:
    for name, field_id in PCP_FIELD_IDS.items():
        if not field_id:
            logger.warning(f"PCP_FIELD_IDS['{name}'] is not set — rules using this field will never match (set LOG_PAYLOADS=true to discover the field ID)")
    for rule in CC_LIST_RULES:
        if not any(rule["cc_lists"]):
            logger.warning(f"CC list ID empty in rule '{rule['description']}' — set the corresponding env var")
